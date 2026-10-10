"""
Honest Finish-Time Estimator with Per-Station Empirical Drift and Human Bottleneck Modeling.
"""
from typing import Dict, List
import time

class HonestFinishTimeEstimator:
    def __init__(
        self,
        num_stations: int = 10,
        num_operators: int = 2,
        num_gpus: int = 4,
        t_run: float = 31.0,
        t_reset: float = 30.0,
        t_calib: float = 40.0,
        calib_interval: int = 4
    ):
        self.num_stations = num_stations
        self.num_operators = num_operators
        self.num_gpus = num_gpus
        self.t_run = t_run
        self.t_reset = t_reset
        self.t_calib = t_calib
        self.calib_interval = calib_interval

        # Beta distribution priors for per-station failure (alpha=fails, beta=successes)
        self.station_stats: Dict[str, Dict[str, float]] = {
            f"st_{i:02d}": {"alpha": 1.0, "beta": 1.0, "drift_k": 0}
            for i in range(num_stations)
        }

        # Rolling operator wait tracking
        self.operator_wait_samples: List[float] = [5.0]  # seed with 5s baseline

    def record_move_outcome(self, station_id: str, failed: bool, drift: int):
        stats = self.station_stats[station_id]
        if failed:
            stats["alpha"] += 1.0
        else:
            stats["beta"] += 1.0
        stats["drift_k"] = drift

    def record_operator_wait_time(self, wait_seconds: float):
        self.operator_wait_samples.append(wait_seconds)
        if len(self.operator_wait_samples) > 50:
            self.operator_wait_samples.pop(0)

    def estimate_seconds_remaining(self, remaining_jobs: int) -> Dict[str, float]:
        if remaining_jobs <= 0:
            return {"eta_seconds": 0.0, "is_operator_bottleneck": False, "fleet_failure_rate": 0.0}

        avg_op_wait = sum(self.operator_wait_samples) / max(1, len(self.operator_wait_samples))

        station_rates = []
        failure_rates = []

        for sid, stats in self.station_stats.items():
            base_p_fail = stats["alpha"] / (stats["alpha"] + stats["beta"])
            drift_p_fail = min(0.95, base_p_fail + (stats["drift_k"] * 0.03))
            failure_rates.append(drift_p_fail)

            # Expected station time to produce 1 completed unit
            # E[T] = T_run + (p / (1-p)) * (T_run + W_op + T_reset) + (T_calib / K)
            expected_retries = drift_p_fail / max(0.01, 1.0 - drift_p_fail)
            wasted_cycle_time = self.t_run + avg_op_wait + self.t_reset
            calib_overhead = self.t_calib / self.calib_interval

            e_station_time = self.t_run + (expected_retries * wasted_cycle_time) + calib_overhead
            station_rates.append(1.0 / max(1.0, e_station_time))

        fleet_station_rate = sum(station_rates)  # total units/sec from arm throughput
        avg_fleet_fail = sum(failure_rates) / len(failure_rates)

        # Operator capacity bound:
        # Humans can do N_op / T_reset resets per sec. Each unit consumes (p / (1-p)) resets.
        expected_resets_per_unit = avg_fleet_fail / max(0.01, 1.0 - avg_fleet_fail)
        if expected_resets_per_unit > 0.001:
            max_units_per_sec_by_operators = (self.num_operators / self.t_reset) / expected_resets_per_unit
        else:
            max_units_per_sec_by_operators = 999.0

        effective_rate = min(fleet_station_rate, max_units_per_sec_by_operators)
        eta_seconds = remaining_jobs / max(0.0001, effective_rate)

        return {
            "eta_seconds": eta_seconds,
            "is_operator_bottleneck": max_units_per_sec_by_operators < fleet_station_rate,
            "fleet_failure_rate": avg_fleet_fail,
            "avg_operator_wait_sec": avg_op_wait
        }