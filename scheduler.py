from typing import Dict, List, Optional
from models import CANONICAL_STATE, Job, StationRuntime, WorkType

class FleetScheduler:
    def __init__(self, num_gpu_slots: int = 4):
        self.available_gpu_slots = num_gpu_slots
        self.queued_jobs: Dict[str, Job] = {}
        # Empirical reliability stats per station (successes, failures)
        self.station_health: Dict[str, Dict[str, float]] = {}

    def add_job(self, job: Job):
        self.queued_jobs[job.job_id] = job

    def remove_job(self, job_id: str):
        self.queued_jobs.pop(job_id, None)

    def register_station_feedback(self, station_id: str, failed: bool):
        if station_id not in self.station_health:
            self.station_health[station_id] = {"successes": 2.0, "failures": 1.0}
        if failed:
            self.station_health[station_id]["failures"] += 1.0
        else:
            self.station_health[station_id]["successes"] += 1.0

    def get_station_failure_prob(self, station: StationRuntime) -> float:
        stats = self.station_health.get(station.station_id, {"successes": 2.0, "failures": 1.0})
        base_p = stats["failures"] / (stats["failures"] + stats["successes"])
        drift_p = min(0.95, base_p + (station.drift_counter * 0.03))
        return drift_p

    def is_risk_acceptable(self, job: Job, p_fail: float) -> bool:
        """
        Risk filtering:
        - Critical Eval gang jobs require p_fail < 0.40
        - Deep chain moves (depth >= 2) require p_fail < 0.35
        - Root moves (depth == 0) or 1-step exploratory runs allowed on any station (up to 0.85)
        """
        if job.work_type == WorkType.EVAL and job.batch_id is not None:
            return p_fail < 0.45
        if job.work_type == WorkType.REPLAY:
            if job.chain_depth >= 2:
                return p_fail < 0.35
            elif job.chain_depth == 1:
                return p_fail < 0.55
        return p_fail < 0.80

    def select_next_job(self, station: StationRuntime, storage=None, sim_time: float = 0.0) -> Optional[Job]:
        p_fail = self.get_station_failure_prob(station)
        traces_to_log = []
        selected_job = None

        # PRIORITY 0: Exact-state Replay Preservation
        # If the station is already in a non-canonical grid state, prioritize advancing the chain
        if station.table_state != CANONICAL_STATE:
            for job in list(self.queued_jobs.values()):
                if job.work_type == WorkType.REPLAY and job.required_state_hash == station.table_state:
                    if not self.is_risk_acceptable(job, p_fail):
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "SKIPPED_RELIABILITY_MISMATCH",
                            f"Station P_fail={p_fail:.2f} too brittle for chain depth {job.chain_depth}"
                        ))
                        continue

                    selected_job = job
                    traces_to_log.append((
                        sim_time, station.station_id, job.job_id, job.work_type.value,
                        station.table_state, job.required_state_hash, self.available_gpu_slots,
                        "DISPATCHED", f"Matched replay continuation (depth={job.chain_depth}, P_fail={p_fail:.2f})"
                    ))
                    break

        # PRIORITY 1: EVAL Gang Batches (Run on S0 or when no chain continuation exists)
        if not selected_job and self.available_gpu_slots > 0:
            active_eval_batches = {
                j.batch_id for j in self.queued_jobs.values()
                if j.work_type == WorkType.EVAL and j.batch_id is not None
            }
            for job in list(self.queued_jobs.values()):
                if job.work_type == WorkType.EVAL and job.batch_id in active_eval_batches:
                    if not self.is_risk_acceptable(job, p_fail):
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "SKIPPED_RELIABILITY_MISMATCH",
                            f"Station P_fail={p_fail:.2f} exceeds eval threshold 0.45"
                        ))
                        continue

                    if job.required_state_hash in (station.table_state, "ANY"):
                        selected_job = job
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "DISPATCHED", f"Dispatched Eval (P_fail={p_fail:.2f})"
                        ))
                        break

        # PRIORITY 2: Policy Inference
        if not selected_job and self.available_gpu_slots > 0:
            for job in list(self.queued_jobs.values()):
                if job.work_type == WorkType.POLICY:
                    if not self.is_risk_acceptable(job, p_fail):
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "SKIPPED_RELIABILITY_MISMATCH",
                            f"Station P_fail={p_fail:.2f} exceeds policy threshold"
                        ))
                        continue

                    if job.required_state_hash in (station.table_state, "ANY"):
                        selected_job = job
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "DISPATCHED", f"Dispatched Policy (P_fail={p_fail:.2f})"
                        ))
                        break

        # PRIORITY 3: Root Replay Trajectories (from S0)
        if not selected_job:
            for job in list(self.queued_jobs.values()):
                if job.work_type == WorkType.REPLAY:
                    if job.required_state_hash == station.table_state:
                        if not self.is_risk_acceptable(job, p_fail):
                            traces_to_log.append((
                                sim_time, station.station_id, job.job_id, job.work_type.value,
                                station.table_state, job.required_state_hash, self.available_gpu_slots,
                                "SKIPPED_RELIABILITY_MISMATCH",
                                f"Station P_fail={p_fail:.2f} too brittle for chain depth {job.chain_depth}"
                            ))
                            continue

                        selected_job = job
                        traces_to_log.append((
                            sim_time, station.station_id, job.job_id, job.work_type.value,
                            station.table_state, job.required_state_hash, self.available_gpu_slots,
                            "DISPATCHED", f"Dispatched root replay (depth={job.chain_depth}, P_fail={p_fail:.2f})"
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

        if storage and traces_to_log:
            storage.record_decision_traces_batch(traces_to_log)

        return selected_job        