from enum import Enum
from dataclasses import dataclass
from typing import Optional

CANONICAL_STATE = "STATE_CANONICAL_S0"

class WorkType(str, Enum):
    REPLAY = "REPLAY"
    POLICY = "POLICY"
    EVAL = "EVAL"
    SHADOW = "SHADOW"

class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    IN_FLIGHT = "IN_FLIGHT"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REFUSED_UNSAFE = "REFUSED_UNSAFE"

class StationStatus(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    CALIBRATING = "CALIBRATING"
    AWAITING_OPERATOR = "AWAITING_OPERATOR"
    OPERATOR_RESETTING = "OPERATOR_RESETTING"

@dataclass
class Job:
    job_id: str
    idempotency_key: str
    work_type: WorkType
    batch_id: Optional[str]
    required_state_hash: str
    target_state_hash: str
    max_duration_sec: float
    preflight_unsafe: bool
    retry_limit: int
    attempt_count: int = 0
    chain_depth: int = 0  # 0 = root move from S0, 1 = move 1, 2 = move 2, etc.
    status: JobStatus = JobStatus.QUEUED
    assigned_station_id: Optional[str] = None

@dataclass
class StationRuntime:
    station_id: str
    table_state: str
    status: StationStatus
    drift_counter: int = 0
    current_job: Optional[Job] = None