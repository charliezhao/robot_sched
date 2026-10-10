#!/usr/bin/env python3
import asyncio
import hashlib
import json
import os
import random
import sqlite3
import sys
import time
from typing import Dict, List, Optional

from models import CANONICAL_STATE, Job, JobStatus, StationRuntime, StationStatus, WorkType
from storage import DB_PATH, Storage
from scheduler import FleetScheduler

NUM_STATIONS = 10
NUM_OPERATORS = 2
NUM_GPU_SLOTS = 4
TIME_ACCELERATION = 20.0

REPLAY_DURATION_S = 31.0
CALIBRATION_DURATION_S = 40.0
OPERATOR_RESET_DURATION_S = 30.0
CALIBRATION_INTERVAL_K = 4
MOVE_FAILURE_RATE = 0.50

class FleetEngine:
    def __init__(self, storage: Storage, sim_speed: float = TIME_ACCELERATION):
        self.storage = storage
        self.sim_speed = sim_speed
        self.scheduler = FleetScheduler(num_gpu_slots=NUM_GPU_SLOTS)
        self.operator_semaphore = asyncio.Semaphore(NUM_OPERATORS)
        self.stations: Dict[str, StationRuntime] = {}
        self.running_tasks: List[asyncio.Task] = []
        self.active_shadow_streamers: int = 0
        self.running = True

    async def sim_sleep(self, seconds: float):
        await asyncio.sleep(seconds / self.sim_speed)

    def load_state(self):
        self.storage.init_stations_if_empty(NUM_STATIONS)
        uncompleted_jobs, db_stations = self.storage.recover_on_startup()
        for sid, sdata in db_stations.items():
            self.stations[sid] = StationRuntime(
                station_id=sid,
                table_state=sdata["table_state_hash"],
                status=StationStatus(sdata["status"]),
                drift_counter=sdata["drift_counter"]
            )
        for j in uncompleted_jobs:
            self.scheduler.add_job(j)

    def submit_jobs(self, jobs: List[Job]):
        clean = []
        for j in jobs:
            if j.preflight_unsafe:
                self.storage.record_preflight_refusal(j.job_id)
                print(f"[{time.strftime('%X')}] 🚫 PREFLIGHT REFUSAL: Job {j.job_id} flagged unsafe. Skipped.")
            else:
                clean.append(j)
                self.scheduler.add_job(j)
        self.storage.insert_jobs(clean)

    async def station_worker(self, station_id: str):
        st = self.stations[station_id]
        while self.running:
            if st.status == StationStatus.AWAITING_OPERATOR:
                await self.handle_operator_reset(st)
                continue

            if st.status == StationStatus.CALIBRATING:
                await self.sim_sleep(CALIBRATION_DURATION_S)
                st.drift_counter = 0
                st.status = StationStatus.IDLE
                self.storage.update_station_state(st.station_id, st.status, st.table_state, st.drift_counter)
                print(f"[{time.strftime('%X')}] 🔧 Station {st.station_id} recalibrated. State={st.table_state}")
                continue

            if st.status == StationStatus.IDLE:
                job = self.scheduler.select_next_job(st)
                if job is None:
                    has_match = any(j.required_state_hash == st.table_state for j in self.scheduler.queued_jobs.values())
                    if not has_match and len(self.scheduler.queued_jobs) > 0 and st.table_state != CANONICAL_STATE:
                        st.status = StationStatus.AWAITING_OPERATOR
                        self.storage.update_station_state(st.station_id, st.status, "DIRTY", st.drift_counter)
                        print(f"[{time.strftime('%X')}] ⚠️ Station {st.station_id} dead-ended on {st.table_state}. Requesting reset.")
                        continue
                    await self.sim_sleep(1.0)
                    continue

                await self.execute_job(st, job)

    async def execute_job(self, st: StationRuntime, job: Job):
        needs_gpu = job.work_type in (WorkType.POLICY, WorkType.EVAL)
        if needs_gpu:
            self.scheduler.available_gpu_slots -= 1

        st.status = StationStatus.BUSY
        st.current_job = job
        job.attempt_count += 1
        self.storage.mark_job_dispatched(job.job_id, st.station_id, job.attempt_count)

        # Shadow telemetry listener
        shadow_active = False
        if self.scheduler.available_gpu_slots > 0:
            self.scheduler.available_gpu_slots -= 1
            self.active_shadow_streamers += 1
            shadow_active = True

        print(f"[{time.strftime('%X')}] 🚀 Station {st.station_id} STARTED {job.work_type} ({job.job_id}) on {st.table_state}")

        duration = REPLAY_DURATION_S if job.work_type == WorkType.REPLAY else random.uniform(15.0, job.max_duration_sec)
        await self.sim_sleep(duration)

        if shadow_active:
            self.scheduler.available_gpu_slots += 1
            self.active_shadow_streamers -= 1

        drift_penalty = st.drift_counter * 0.03
        succeeded = random.random() > min(0.85, MOVE_FAILURE_RATE + drift_penalty)

        if needs_gpu:
            self.scheduler.available_gpu_slots += 1

        if succeeded:
            st.drift_counter += 1
            st.table_state = job.target_state_hash
            st.status = StationStatus.CALIBRATING if st.drift_counter >= CALIBRATION_INTERVAL_K else StationStatus.IDLE
            self.storage.mark_job_success(job.job_id, st.station_id, st.table_state, st.drift_counter)
            print(f"[{time.strftime('%X')}] ✅ Station {st.station_id} SUCCESS {job.job_id} -> {st.table_state} (drift={st.drift_counter})")
        else:
            can_retry = job.attempt_count < job.retry_limit
            st.status = StationStatus.AWAITING_OPERATOR
            st.table_state = "DIRTY"
            self.storage.mark_job_failed(job.job_id, st.station_id, can_retry)
            if can_retry:
                self.scheduler.add_job(job)
            print(f"[{time.strftime('%X')}] ❌ Station {st.station_id} FAILED {job.job_id}! Table DIRTY. Enqueueing human.")

        st.current_job = None

    async def handle_operator_reset(self, st: StationRuntime):
        async with self.operator_semaphore:
            st.status = StationStatus.OPERATOR_RESETTING
            self.storage.update_station_state(st.station_id, st.status, "DIRTY", st.drift_counter)
            print(f"[{time.strftime('%X')}] 🧑‍🔧 Operator CLAIMED Station {st.station_id}. Jig reset in progress...")
            await self.sim_sleep(OPERATOR_RESET_DURATION_S)

            st.table_state = CANONICAL_STATE
            st.drift_counter = 0
            st.status = StationStatus.IDLE
            self.storage.update_station_state(st.station_id, st.status, st.table_state, st.drift_counter)
            print(f"[{time.strftime('%X')}] ✨ Station {st.station_id} RESET to {CANONICAL_STATE}. Ready.")

    async def start(self):
        self.load_state()
        workers = [asyncio.create_task(self.station_worker(sid)) for sid in self.stations.keys()]
        self.running_tasks.extend(workers)
        await asyncio.gather(*workers)


def generate_sample_workload() -> List[Job]:
    jobs = []
    # 6 Replay chains
    for c in range(6):
        curr = CANONICAL_STATE
        for m in range(4):
            target = f"STATE_GRID_C{c}_M{m+1}"
            jid = f"replay_c{c}_m{m}"
            key = hashlib.sha256(f"{jid}:{curr}:{target}".encode()).hexdigest()
            jobs.append(Job(
                job_id=jid, idempotency_key=key, work_type=WorkType.REPLAY, batch_id=None,
                required_state_hash=curr, target_state_hash=target, max_duration_sec=REPLAY_DURATION_S,
                preflight_unsafe=False, retry_limit=2
            ))
            curr = target

    # Eval Batch (6 episodes)
    for i in range(6):
        jid = f"eval_ep_{i}"
        key = hashlib.sha256(f"eval_checkpoint_v2:{jid}".encode()).hexdigest()
        jobs.append(Job(
            job_id=jid, idempotency_key=key, work_type=WorkType.EVAL, batch_id="eval_checkpoint_v2",
            required_state_hash="ANY", target_state_hash=CANONICAL_STATE, max_duration_sec=30.0,
            preflight_unsafe=False, retry_limit=1
        ))

    # Policy inference
    for i in range(4):
        jid = f"policy_run_{i}"
        key = hashlib.sha256(jid.encode()).hexdigest()
        jobs.append(Job(
            job_id=jid, idempotency_key=key, work_type=WorkType.POLICY, batch_id=None,
            required_state_hash="ANY", target_state_hash=CANONICAL_STATE, max_duration_sec=40.0,
            preflight_unsafe=False, retry_limit=1
        ))

    # Preflight refusal test
    jobs.append(Job(
        job_id="unsafe_move_99", idempotency_key="unsafe_99_key", work_type=WorkType.REPLAY,
        batch_id=None, required_state_hash=CANONICAL_STATE, target_state_hash="STATE_ERR",
        max_duration_sec=31.0, preflight_unsafe=True, retry_limit=0
    ))
    return jobs


async def run_simulation(kill_after_sec: Optional[float] = None):
    storage = Storage()
    engine = FleetEngine(storage, sim_speed=TIME_ACCELERATION)

    with storage.conn:
        cur = storage.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM jobs")
        if cur.fetchone()[0] == 0:
            print("📦 Initializing SQLite database with initial workload...")
            engine.submit_jobs(generate_sample_workload())

    task = asyncio.create_task(engine.start())
    if kill_after_sec:
        await asyncio.sleep(kill_after_sec)
        print(f"\n⚡ SIMULATING ABRUPT CRASH (kill -9) at {kill_after_sec}s...\n")
        engine.running = False
        task.cancel()
        storage.conn.close()
    else:
        await asyncio.sleep(30.0)
        engine.running = False
        task.cancel()


def print_database_report():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    print("\n" + "=" * 65)
    print("📋 SYSTEM DATABASE AUDIT REPORT")
    print("=" * 65)
    status_counts = {r["status"]: r["cnt"] for r in conn.execute("SELECT status, count(*) as cnt FROM jobs GROUP BY status")}
    print(f"Jobs Summary: {json.dumps(status_counts, indent=2)}")
    print("\nStations Status:")
    for r in conn.execute("SELECT station_id, status, table_state_hash, drift_counter FROM stations"):
        print(f"  {r['station_id']}: status={r['status']:<18} table={r['table_state_hash']:<22} drift={r['drift_counter']}")
    conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fleet Queue & Scheduler Engine")
    parser.add_argument("--demonstrate-resume", action="store_true", help="Demonstrates mid-run crash and resume recovery")
    args = parser.parse_args()

    if args.demonstrate_resume:
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        print("=== STAGE 1: Starting run (Crashing at 4s) ===")
        try:
            asyncio.run(run_simulation(kill_after_sec=4.0))
        except asyncio.CancelledError:
            pass

        print("\n=== STAGE 2: Resuming from SQLite WAL Ledger ===")
        asyncio.run(run_simulation(kill_after_sec=None))
        print_database_report()
    else:
        asyncio.run(run_simulation(kill_after_sec=None))
        print_database_report()