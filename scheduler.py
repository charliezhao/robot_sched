from typing import Dict, List, Optional
from models import CANONICAL_STATE, Job, StationRuntime, WorkType

class FleetScheduler:
    def __init__(self, num_gpu_slots: int = 4):
        self.available_gpu_slots = num_gpu_slots
        self.queued_jobs: Dict[str, Job] = {}

    def add_job(self, job: Job):
        self.queued_jobs[job.job_id] = job

    def remove_job(self, job_id: str):
        self.queued_jobs.pop(job_id, None)

    def select_next_job(self, station: StationRuntime, storage=None, sim_time: float = 0.0) -> Optional[Job]:
        active_eval_batches = {
            j.batch_id for j in self.queued_jobs.values()
            if j.work_type == WorkType.EVAL and j.batch_id is not None
        }

        traces_to_log = []
        selected_job = None

        # 1. EVAL Gang Batches (Highest Priority)
        if self.available_gpu_slots > 0:
            for job in list(self.queued_jobs.values()):
                if job.work_type == WorkType.EVAL and job.batch_id in active_eval_batches:
                    if job.required_state_hash in (station.table_state, "ANY"):
                        selected_job = job
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "DISPATCHED", "Matched physical state and acquired GPU slot"
                        ))
                        break
                    else:
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "SKIPPED_STATE_MISMATCH", f"Station at {station.table_state}, requires {job.required_state_hash}"
                        ))

        # 2. Closed-Loop Policy Inference
        if not selected_job and self.available_gpu_slots > 0:
            for job in list(self.queued_jobs.values()):
                if job.work_type == WorkType.POLICY:
                    if job.required_state_hash in (station.table_state, "ANY"):
                        selected_job = job
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "DISPATCHED", "Matched physical state and acquired GPU slot"
                        ))
                        break
                    else:
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "SKIPPED_STATE_MISMATCH", f"Station at {station.table_state}, requires {job.required_state_hash}"
                        ))

        # 3. Trajectory Replay Dispatching
        if not selected_job:
            for job in list(self.queued_jobs.values()):
                if job.work_type == WorkType.REPLAY:
                    if job.required_state_hash == station.table_state:
                        selected_job = job
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "DISPATCHED", "Matched table configuration"
                        ))
                        break
                    else:
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "SKIPPED_STATE_MISMATCH", f"Station at {station.table_state}, requires {job.required_state_hash}"
                        ))

        if selected_job:
            self.remove_job(selected_job.job_id)

        # Bulk insert audit traces once instead of individual commits
        if storage and traces_to_log:
            storage.record_decision_traces_batch(traces_to_log)

        return selected_job