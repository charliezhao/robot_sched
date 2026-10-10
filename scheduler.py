from typing import Dict, Optional
from models import CANONICAL_STATE, Job, StationRuntime, WorkType

class FleetScheduler:
    def __init__(self, num_gpu_slots: int = 4):
        self.available_gpu_slots = num_gpu_slots
        self.queued_jobs: Dict[str, Job] = {}

    def add_job(self, job: Job):
        self.queued_jobs[job.job_id] = job

    def remove_job(self, job_id: str):
        self.queued_jobs.pop(job_id, None)

    def select_next_job(self, station: StationRuntime) -> Optional[Job]:
        """
        Decision-driven selection:
        1. Eval batch gang draining (highest score ROI).
        2. Policy inference (dual-resource acquisition).
        3. Replay work matching current table arrangement.
        """
        active_eval_batches = {
            j.batch_id for j in self.queued_jobs.values()
            if j.work_type == WorkType.EVAL and j.batch_id is not None
        }

        # 1. Drain active Eval Batches
        if self.available_gpu_slots > 0:
            for job in list(self.queued_jobs.values()):
                if job.work_type == WorkType.EVAL and job.batch_id in active_eval_batches:
                    if job.required_state_hash in (station.table_state, "ANY"):
                        self.remove_job(job.job_id)
                        return job

        # 2. Policy inference
        if self.available_gpu_slots > 0:
            for job in list(self.queued_jobs.values()):
                if job.work_type == WorkType.POLICY:
                    if job.required_state_hash in (station.table_state, "ANY"):
                        self.remove_job(job.job_id)
                        return job

        # 3. State-graph matched Replay work
        for job in list(self.queued_jobs.values()):
            if job.work_type == WorkType.REPLAY and job.required_state_hash == station.table_state:
                self.remove_job(job.job_id)
                return job

        return None