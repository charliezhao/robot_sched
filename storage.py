import sqlite3
import time
from typing import Dict, List, Tuple
from models import CANONICAL_STATE, Job, JobStatus, StationStatus, WorkType

DB_PATH = "scheduler_state.db"

class Storage:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute("PRAGMA journal_mode = WAL;")
            self.conn.execute("PRAGMA synchronous = NORMAL;")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE NOT NULL,
                    work_type TEXT NOT NULL,
                    batch_id TEXT,
                    required_state_hash TEXT NOT NULL,
                    target_state_hash TEXT NOT NULL,
                    max_duration_sec REAL NOT NULL,
                    preflight_unsafe INTEGER NOT NULL,
                    retry_limit INTEGER NOT NULL,
                    attempt_count INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    assigned_station_id TEXT,
                    updated_at REAL NOT NULL
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS stations (
                    station_id TEXT PRIMARY KEY,
                    table_state_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    current_job_id TEXT,
                    drift_counter INTEGER DEFAULT 0,
                    updated_at REAL NOT NULL
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS decision_audit_ledger (
                    decision_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sim_time REAL NOT NULL,
                    station_id TEXT NOT NULL,
                    candidate_job_id TEXT NOT NULL,
                    candidate_work_type TEXT NOT NULL,
                    station_table_state TEXT NOT NULL,
                    job_required_state TEXT NOT NULL,
                    available_gpus INTEGER NOT NULL,
                    decision_verdict TEXT NOT NULL,
                    rejection_reason TEXT,
                    created_at REAL NOT NULL
                );
            """)

    def record_decision_trace(
        self,
        sim_time: float,
        station_id: str,
        job_id: str,
        work_type: str,
        st_state: str,
        req_state: str,
        avail_gpus: int,
        verdict: str,
        reason: str = ""
    ):
        with self.conn:
            self.conn.execute("""
                INSERT INTO decision_audit_ledger 
                (sim_time, station_id, candidate_job_id, candidate_work_type, station_table_state,
                 job_required_state, available_gpus, decision_verdict, rejection_reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (sim_time, station_id, job_id, work_type, st_state, req_state, avail_gpus, verdict, reason, time.time()))

    def record_decision_traces_batch(self, traces: list):
        now = time.time()
        rows = [
            (t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7], t[8], now)
            for t in traces
        ]
        with self.conn:
            self.conn.executemany("""
                INSERT INTO decision_audit_ledger 
                (sim_time, station_id, candidate_job_id, candidate_work_type, station_table_state,
                 job_required_state, available_gpus, decision_verdict, rejection_reason, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)

    def init_stations_if_empty(self, num_stations: int):
        with self.conn:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM stations")
            if cur.fetchone()[0] == 0:
                now = time.time()
                for i in range(num_stations):
                    cur.execute("""
                        INSERT INTO stations (station_id, table_state_hash, status, current_job_id, drift_counter, updated_at)
                        VALUES (?, ?, ?, NULL, 0, ?)
                    """, (f"st_{i:02d}", CANONICAL_STATE, StationStatus.IDLE.value, now))

    def insert_jobs(self, jobs: List[Job]):
        with self.conn:
            for j in jobs:
                self.conn.execute("""
                    INSERT OR IGNORE INTO jobs 
                    (job_id, idempotency_key, work_type, batch_id, required_state_hash, target_state_hash,
                     max_duration_sec, preflight_unsafe, retry_limit, attempt_count, status, assigned_station_id, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    j.job_id, j.idempotency_key, j.work_type.value, j.batch_id,
                    j.required_state_hash, j.target_state_hash, j.max_duration_sec,
                    1 if j.preflight_unsafe else 0, j.retry_limit, j.attempt_count,
                    j.status.value, j.assigned_station_id, time.time()
                ))

    def record_preflight_refusal(self, job_id: str):
        with self.conn:
            self.conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE job_id = ?",
                              (JobStatus.REFUSED_UNSAFE.value, time.time(), job_id))

    def mark_job_dispatched(self, job_id: str, station_id: str, attempt: int):
        with self.conn:
            now = time.time()
            self.conn.execute("UPDATE jobs SET status = ?, assigned_station_id = ?, attempt_count = ?, updated_at = ? WHERE job_id = ?",
                              (JobStatus.IN_FLIGHT.value, station_id, attempt, now, job_id))
            self.conn.execute("UPDATE stations SET status = ?, current_job_id = ?, updated_at = ? WHERE station_id = ?",
                              (StationStatus.BUSY.value, job_id, now, station_id))

    def mark_job_success(self, job_id: str, station_id: str, target_state: str, new_drift: int):
        with self.conn:
            now = time.time()
            self.conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE job_id = ?",
                              (JobStatus.COMPLETED.value, now, job_id))
            self.conn.execute("UPDATE stations SET status = ?, table_state_hash = ?, current_job_id = NULL, drift_counter = ?, updated_at = ? WHERE station_id = ?",
                              (StationStatus.IDLE.value, target_state, new_drift, now, station_id))

    def mark_job_failed(self, job_id: str, station_id: str, can_retry: bool):
        with self.conn:
            now = time.time()
            new_status = JobStatus.QUEUED.value if can_retry else JobStatus.FAILED.value
            self.conn.execute("UPDATE jobs SET status = ?, assigned_station_id = NULL, updated_at = ? WHERE job_id = ?",
                              (new_status, now, job_id))
            self.conn.execute("UPDATE stations SET status = ?, table_state_hash = 'DIRTY', current_job_id = NULL, updated_at = ? WHERE station_id = ?",
                              (StationStatus.AWAITING_OPERATOR.value, now, station_id))

    def update_station_state(self, station_id: str, status: StationStatus, table_state: str, drift: int):
        with self.conn:
            self.conn.execute("UPDATE stations SET status = ?, table_state_hash = ?, drift_counter = ?, updated_at = ? WHERE station_id = ?",
                              (status.value, table_state, drift, time.time(), station_id))

    def recover_on_startup(self) -> Tuple[List[Job], Dict[str, dict]]:
        with self.conn:
            self.conn.execute("""
                UPDATE stations SET status = 'AWAITING_OPERATOR', table_state_hash = 'DIRTY', current_job_id = NULL
                WHERE status IN ('BUSY', 'CALIBRATING', 'OPERATOR_RESETTING')
            """)
            self.conn.execute("""
                UPDATE jobs SET status = 'QUEUED', assigned_station_id = NULL
                WHERE status = 'IN_FLIGHT' AND attempt_count < retry_limit
            """)
            self.conn.execute("""
                UPDATE jobs SET status = 'FAILED', assigned_station_id = NULL
                WHERE status = 'IN_FLIGHT' AND attempt_count >= retry_limit
            """)

            stations = {row["station_id"]: dict(row) for row in self.conn.execute("SELECT * FROM stations")}
            jobs = [
                Job(
                    job_id=r["job_id"], idempotency_key=r["idempotency_key"], work_type=WorkType(r["work_type"]),
                    batch_id=r["batch_id"], required_state_hash=r["required_state_hash"], target_state_hash=r["target_state_hash"],
                    max_duration_sec=r["max_duration_sec"], preflight_unsafe=bool(r["preflight_unsafe"]),
                    retry_limit=r["retry_limit"], attempt_count=r["attempt_count"], status=JobStatus(r["status"])
                )
                for r in self.conn.execute("SELECT * FROM jobs WHERE status = 'QUEUED'")
            ]
            return jobs, stations