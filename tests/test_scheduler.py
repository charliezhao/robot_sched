import os
import sqlite3
import pytest
from models import CANONICAL_STATE, Job, JobStatus, StationRuntime, StationStatus, WorkType
from storage import Storage
from scheduler import FleetScheduler

TEST_DB = "test_scheduler_state.db"

@pytest.fixture
def clean_storage():
    """Provides a fresh isolated SQLite database for each test."""
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    storage = Storage(db_path=TEST_DB)
    storage.init_stations_if_empty(num_stations=2)
    yield storage
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


def test_preflight_refusal(clean_storage):
    """Unsafe jobs must be recorded as REFUSED_UNSAFE without queueing."""
    storage = clean_storage
    unsafe_job = Job(
        job_id="job_unsafe_1",
        idempotency_key="key_unsafe_1",
        work_type=WorkType.REPLAY,
        batch_id=None,
        required_state_hash=CANONICAL_STATE,
        target_state_hash="STATE_GRID_1",
        max_duration_sec=31.0,
        preflight_unsafe=True,
        retry_limit=0,
        status=JobStatus.REFUSED_UNSAFE
    )
    storage.insert_jobs([unsafe_job])

    cur = storage.conn.cursor()
    cur.execute("SELECT status FROM jobs WHERE job_id = ?", (unsafe_job.job_id,))
    row = cur.fetchone()
    assert row is not None
    assert row[0] == JobStatus.REFUSED_UNSAFE.value


def test_state_graph_replay_matching():
    """Replay jobs must only be dispatched to stations matching their required table state."""
    scheduler = FleetScheduler(num_gpu_slots=2)
    
    # Job needing non-canonical state S1
    job_s1 = Job(
        job_id="job_needs_s1",
        idempotency_key="k1",
        work_type=WorkType.REPLAY,
        batch_id=None,
        required_state_hash="STATE_GRID_S1",
        target_state_hash="STATE_GRID_S2",
        max_duration_sec=31.0,
        preflight_unsafe=False,
        retry_limit=1
    )
    # Job needing canonical state S0
    job_s0 = Job(
        job_id="job_needs_s0",
        idempotency_key="k2",
        work_type=WorkType.REPLAY,
        batch_id=None,
        required_state_hash=CANONICAL_STATE,
        target_state_hash="STATE_GRID_S1",
        max_duration_sec=31.0,
        preflight_unsafe=False,
        retry_limit=1
    )
    scheduler.add_job(job_s1)
    scheduler.add_job(job_s0)

    # Station is currently at S0
    st_s0 = StationRuntime(station_id="st_00", table_state=CANONICAL_STATE, status=StationStatus.IDLE)
    selected = scheduler.select_next_job(st_s0)
    assert selected is not None
    assert selected.job_id == "job_needs_s0"

    # Station is at an unindexed state S_UNKNOWN
    st_unk = StationRuntime(station_id="st_01", table_state="STATE_UNKNOWN", status=StationStatus.IDLE)
    selected_unk = scheduler.select_next_job(st_unk)
    assert selected_unk is None


def test_gpu_slot_contention_and_eval_gang_priority():
    """Active Eval batches get priority over Policy jobs when GPU slots are contested."""
    scheduler = FleetScheduler(num_gpu_slots=1)
    st = StationRuntime(station_id="st_00", table_state=CANONICAL_STATE, status=StationStatus.IDLE)

    policy_job = Job(
        job_id="pol_0", idempotency_key="pk0", work_type=WorkType.POLICY,
        batch_id=None, required_state_hash="ANY", target_state_hash=CANONICAL_STATE,
        max_duration_sec=30.0, preflight_unsafe=False, retry_limit=1
    )
    eval_job = Job(
        job_id="eval_0", idempotency_key="ek0", work_type=WorkType.EVAL,
        batch_id="batch_checkpoint_alpha", required_state_hash="ANY",
        target_state_hash=CANONICAL_STATE, max_duration_sec=30.0,
        preflight_unsafe=False, retry_limit=1
    )

    scheduler.add_job(policy_job)
    scheduler.add_job(eval_job)

    # Eval batch should be selected first to preserve gang completion ROI
    selected = scheduler.select_next_job(st)
    assert selected is not None
    assert selected.job_id == "eval_0"

    # If GPU slots drop to 0, no Policy or Eval jobs can be selected
    scheduler.available_gpu_slots = 0
    selected_no_gpu = scheduler.select_next_job(st)
    assert selected_no_gpu is None


def test_crash_recovery_downgrades_in_flight_stations(clean_storage):
    """Crash recovery must set in-flight stations to DIRTY and AWAITING_OPERATOR."""
    storage = clean_storage
    job = Job(
        job_id="in_flight_job", idempotency_key="if_key", work_type=WorkType.REPLAY,
        batch_id=None, required_state_hash=CANONICAL_STATE, target_state_hash="STATE_GRID_1",
        max_duration_sec=31.0, preflight_unsafe=False, retry_limit=2
    )
    storage.insert_jobs([job])
    storage.mark_job_dispatched(job.job_id, "st_00", attempt=1)

    # Check that database records in-flight state
    cur = storage.conn.cursor()
    cur.execute("SELECT status FROM stations WHERE station_id = 'st_00'")
    assert cur.fetchone()[0] == StationStatus.BUSY.value

    # Simulate restart reconciliation
    recovered_jobs, recovered_stations = storage.recover_on_startup()

    # The interrupted station must be DIRTY and waiting for a human
    assert recovered_stations["st_00"]["status"] == StationStatus.AWAITING_OPERATOR.value
    assert recovered_stations["st_00"]["table_state_hash"] == "DIRTY"
    assert recovered_stations["st_00"]["current_job_id"] is None

    # The job must be returned to QUEUED because attempt_count < retry_limit
    assert len(recovered_jobs) == 1
    assert recovered_jobs[0].job_id == "in_flight_job"
    assert recovered_jobs[0].status == JobStatus.QUEUED