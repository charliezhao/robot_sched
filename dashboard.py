#!/usr/bin/env python3
"""
Phase 3: Interactive Fleet Dashboard & Comparative Benchmark
- Live terminal TUI using 'rich'
- Station inspection & interactive operator controls (pause, abort, requeue, restore, offline)
- Side-by-side Comparative Engine: State-Graph vs. Strict Linear Chains
"""

import asyncio
import copy
import hashlib
import os
import random
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from models import CANONICAL_STATE, Job, JobStatus, StationRuntime, StationStatus, WorkType
from storage import Storage

console = Console()

# ---------------------------------------------------------------------------
# Extended State & Operator Action Enums
# ---------------------------------------------------------------------------
class InterventionType(str, Enum):
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    ABORT = "ABORT"
    REQUEUE = "REQUEUE"
    MARK_RESTORED = "MARK_RESTORED"
    TAKE_OFFLINE = "TAKE_OFFLINE"


@dataclass
class FleetMetrics:
    total_jobs: int = 0
    completed: int = 0
    failed: int = 0
    refused_unsafe: int = 0
    queued: int = 0
    in_flight: int = 0
    operator_labor_seconds: float = 0.0
    simulated_elapsed_seconds: float = 0.0
    failures_by_reason: Dict[str, int] = field(default_factory=lambda: {
        "GRIPPER_SLIP": 0,
        "TORQUE_LIMIT": 0,
        "VISION_MISALIGN": 0,
        "TIMEOUT": 0,
        "OPERATOR_ABORT": 0
    })
    eval_batch_progress: Dict[str, Tuple[int, int]] = field(default_factory=dict)  # batch_id -> (done, total)


# ---------------------------------------------------------------------------
# Engine Supporting Real-Time Interventions & Scheduling Modes
# ---------------------------------------------------------------------------
class OperableFleetEngine:
    def __init__(
        self,
        num_stations: int = 10,
        num_operators: int = 2,
        num_gpu_slots: int = 4,
        sim_speed: float = 60.0,
        mode: str = "STATE_GRAPH",  # "STATE_GRAPH" or "STRICT_CHAINS"
        seed: int = 42
    ):
        self.num_stations = num_stations
        self.num_operators = num_operators
        self.num_gpu_slots = num_gpu_slots
        self.sim_speed = sim_speed
        self.mode = mode
        self.prng = random.Random(seed)

        self.available_gpu_slots = num_gpu_slots
        self.operator_semaphore = asyncio.Semaphore(num_operators)
        self.operator_queue_len = 0
        self.station_locks = {f"st_{i:02d}": asyncio.Lock() for i in range(num_stations)}

        self.stations: Dict[str, StationRuntime] = {}
        self.station_offline: Dict[str, bool] = {f"st_{i:02d}": False for i in range(num_stations)}
        self.station_paused: Dict[str, bool] = {f"st_{i:02d}": False for i in range(num_stations)}

        self.jobs_pool: Dict[str, Job] = {}
        self.metrics = FleetMetrics()
        self.running = True
        self.start_sim_time = 0.0

        for i in range(num_stations):
            sid = f"st_{i:02d}"
            self.stations[sid] = StationRuntime(
                station_id=sid,
                table_state=CANONICAL_STATE,
                status=StationStatus.IDLE,
                drift_counter=0
            )

    async def sim_sleep(self, seconds: float):
        self.metrics.simulated_elapsed_seconds += seconds / self.num_stations
        if self.sim_speed >= 1000.0:
            await asyncio.sleep(0)  # Pure cooperative yield, instant execution
        else:
            await asyncio.sleep(seconds / self.sim_speed)

    def submit_jobs(self, jobs: List[Job]):
        for j in jobs:
            self.jobs_pool[j.job_id] = j
            if j.preflight_unsafe:
                j.status = JobStatus.REFUSED_UNSAFE
                self.metrics.refused_unsafe += 1
            else:
                self.metrics.queued += 1
                if j.work_type == WorkType.EVAL and j.batch_id:
                    done, total = self.metrics.eval_batch_progress.get(j.batch_id, (0, 0))
                    self.metrics.eval_batch_progress[j.batch_id] = (done, total + 1)
        self.metrics.total_jobs = len(self.jobs_pool)

    # -----------------------------------------------------------------------
    # Thread-Safe Operator Interventions
    # -----------------------------------------------------------------------
    async def apply_operator_intervention(self, station_id: str, action: InterventionType) -> str:
        async with self.station_locks[station_id]:
            st = self.stations[station_id]
            if action == InterventionType.PAUSE:
                self.station_paused[station_id] = True
                return f"{station_id} paused safely."

            elif action == InterventionType.RESUME:
                self.station_paused[station_id] = False
                return f"{station_id} resumed."

            elif action == InterventionType.TAKE_OFFLINE:
                self.station_offline[station_id] = True
                st.status = StationStatus.AWAITING_OPERATOR
                return f"{station_id} flagged offline for maintenance."

            elif action == InterventionType.MARK_RESTORED:
                st.table_state = CANONICAL_STATE
                st.drift_counter = 0
                st.status = StationStatus.IDLE
                self.station_paused[station_id] = False
                self.station_offline[station_id] = False
                return f"{station_id} verified via camera and restored to S0."

            elif action == InterventionType.ABORT:
                if st.status == StationStatus.BUSY and st.current_job:
                    aborted_job = st.current_job
                    aborted_job.status = JobStatus.FAILED
                    self.metrics.failed += 1
                    self.metrics.in_flight -= 1
                    self.metrics.failures_by_reason["OPERATOR_ABORT"] += 1
                    st.current_job = None
                    st.status = StationStatus.AWAITING_OPERATOR
                    st.table_state = "DIRTY"
                    if aborted_job.work_type in (WorkType.POLICY, WorkType.EVAL):
                        self.available_gpu_slots = min(self.num_gpu_slots, self.available_gpu_slots + 1)
                    return f"Aborted in-flight job {aborted_job.job_id} on {station_id}. Table marked DIRTY."
                return f"Station {station_id} is not running active work."

            elif action == InterventionType.REQUEUE:
                if st.current_job:
                    re_job = st.current_job
                    re_job.status = JobStatus.QUEUED
                    self.metrics.in_flight -= 1
                    self.metrics.queued += 1
                    st.current_job = None
                    st.status = StationStatus.AWAITING_OPERATOR
                    st.table_state = "DIRTY"
                    return f"Job {re_job.job_id} requeued. {station_id} set to DIRTY."
                return f"No active job on {station_id} to requeue."
        return "Unknown action."

    # -----------------------------------------------------------------------
    # Decision Scheduling Dispatcher
    # -----------------------------------------------------------------------
    def select_next_job(self, station: StationRuntime) -> Optional[Job]:
        if self.station_paused[station.station_id] or self.station_offline[station.station_id]:
            return None

        candidates = [j for j in self.jobs_pool.values() if j.status == JobStatus.QUEUED]
        if not candidates:
            return None

        # 1. EVAL Gang Batches (Highest priority)
        if self.available_gpu_slots > 0:
            for j in candidates:
                if j.work_type == WorkType.EVAL:
                    if j.required_state_hash in (station.table_state, "ANY"):
                        return j

        # 2. Closed-Loop Policy
        if self.available_gpu_slots > 0:
            for j in candidates:
                if j.work_type == WorkType.POLICY:
                    if j.required_state_hash in (station.table_state, "ANY"):
                        return j

        # 3. Trajectory Replay Dispatching
        if self.mode == "STATE_GRAPH":
            # Match ANY available replay job fitting the station's exact table state
            for j in candidates:
                if j.work_type == WorkType.REPLAY and j.required_state_hash == station.table_state:
                    return j
        else:
            # STRICT CHAINS: Must strictly follow original sequential order
            for j in candidates:
                if j.work_type == WorkType.REPLAY:
                    if j.required_state_hash == station.table_state:
                        return j
                    else:
                        # In strict chains, cannot jump ahead: forces an operator reset!
                        break

        return None

    # -----------------------------------------------------------------------
    # Station Lifecycle Loop
    # -----------------------------------------------------------------------
    async def station_worker(self, station_id: str):
        st = self.stations[station_id]
        while self.running:
            if self.station_paused[station_id] or self.station_offline[station_id]:
                await asyncio.sleep(0.05)
                continue

            if st.status == StationStatus.AWAITING_OPERATOR:
                self.operator_queue_len += 1
                async with self.operator_semaphore:
                    self.operator_queue_len -= 1
                    st.status = StationStatus.OPERATOR_RESETTING
                    reset_time = 30.0  # 30s jig alignment + camera preflight
                    self.metrics.operator_labor_seconds += reset_time
                    await self.sim_sleep(reset_time)
                    st.table_state = CANONICAL_STATE
                    st.drift_counter = 0
                    st.status = StationStatus.IDLE
                continue

            if st.status == StationStatus.CALIBRATING:
                await self.sim_sleep(40.0)
                st.drift_counter = 0
                st.status = StationStatus.IDLE
                continue

            if st.status == StationStatus.IDLE:
                async with self.station_locks[station_id]:
                    # Safe optimistic check
                    job = self.select_next_job(st)
                    if job:
                        job.status = JobStatus.IN_FLIGHT
                        job.assigned_station_id = station_id
                        st.current_job = job
                        st.status = StationStatus.BUSY
                        self.metrics.queued -= 1
                        self.metrics.in_flight += 1

                if not job:
                    # Check for dead-end state forcing operator reset
                    has_match = any(
                        j.required_state_hash == st.table_state and j.status == JobStatus.QUEUED
                        for j in self.jobs_pool.values()
                    )
                    if not has_match and self.metrics.queued > 0 and st.table_state != CANONICAL_STATE:
                        st.status = StationStatus.AWAITING_OPERATOR
                        st.table_state = "DIRTY"
                        continue
                    await asyncio.sleep(0.01)
                    continue

                # Execute Move
                needs_gpu = job.work_type in (WorkType.POLICY, WorkType.EVAL)
                if needs_gpu:
                    self.available_gpu_slots -= 1

                # 31s replay or variable policy duration
                dur = 31.0 if job.work_type == WorkType.REPLAY else self.prng.uniform(15.0, job.max_duration_sec)
                await self.sim_sleep(dur)

                if needs_gpu:
                    self.available_gpu_slots = min(self.num_gpu_slots, self.available_gpu_slots + 1)

                # Stochastic physical outcome
                effective_fail = 0.50 + (st.drift_counter * 0.03)
                succeeded = self.prng.random() > effective_fail

                async with self.station_locks[station_id]:
                    if st.status == StationStatus.BUSY and st.current_job == job:
                        self.metrics.in_flight -= 1
                        if succeeded:
                            job.status = JobStatus.COMPLETED
                            self.metrics.completed += 1
                            st.table_state = job.target_state_hash
                            st.drift_counter += 1
                            st.status = StationStatus.CALIBRATING if st.drift_counter >= 4 else StationStatus.IDLE
                            
                            if job.work_type == WorkType.EVAL and job.batch_id:
                                d, t = self.metrics.eval_batch_progress[job.batch_id]
                                self.metrics.eval_batch_progress[job.batch_id] = (d + 1, t)
                        else:
                            reason = self.prng.choice(["GRIPPER_SLIP", "TORQUE_LIMIT", "VISION_MISALIGN"])
                            self.metrics.failures_by_reason[reason] += 1
                            job.attempt_count += 1
                            if job.attempt_count < job.retry_limit:
                                job.status = JobStatus.QUEUED
                                self.metrics.queued += 1
                            else:
                                job.status = JobStatus.FAILED
                                self.metrics.failed += 1
                            
                            st.table_state = "DIRTY"
                            st.status = StationStatus.AWAITING_OPERATOR
                        st.current_job = None


# ---------------------------------------------------------------------------
# UI Dashboard Render Logic (Rich TUI)
# ---------------------------------------------------------------------------
def build_dashboard_view(engine: OperableFleetEngine) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", ratio=1),
        Layout(name="footer", size=8)
    )
    layout["body"].split_row(
        Layout(name="stations", ratio=3),
        Layout(name="telemetry", ratio=2)
    )

    # 1. Header
    m = engine.metrics
    pct_done = (m.completed / max(1, m.total_jobs)) * 100
    est_remaining_sec = 0.0
    if m.completed > 0:
        rate = m.completed / max(1.0, m.simulated_elapsed_seconds)
        est_remaining_sec = (m.queued + m.in_flight) / max(0.001, rate)

    header_text = Text()
    header_text.append(f" MODE: {engine.mode} ", style="bold black on green" if engine.mode == "STATE_GRAPH" else "bold black on yellow")
    header_text.append(f"  Sim Clock: {m.simulated_elapsed_seconds:.1f}s ({m.simulated_elapsed_seconds/3600:.2f} hrs) | ")
    header_text.append(f"Fleet Progress: {m.completed}/{m.total_jobs} ({pct_done:.1f}%) | ")
    header_text.append(f"Projected Finish: in {est_remaining_sec/60:.1f} sim-mins", style="bold cyan")
    layout["header"].update(Panel(header_text, style="white on blue"))

    # 2. Stations Table
    st_table = Table(title="10 Physical Stations & Current States", expand=True, box=None)
    st_table.add_column("Station", justify="center", style="bold")
    st_table.add_column("Status", justify="center")
    st_table.add_column("Physical Table State", style="yellow")
    st_table.add_column("Active Job", style="cyan")
    st_table.add_column("Drift", justify="center")

    for sid, st in engine.stations.items():
        status_style = "green"
        if st.status == StationStatus.BUSY: status_style = "bold cyan"
        elif st.status == StationStatus.AWAITING_OPERATOR: status_style = "bold red"
        elif st.status == StationStatus.OPERATOR_RESETTING: status_style = "bold magenta"
        elif st.status == StationStatus.CALIBRATING: status_style = "yellow"

        ctrl_tag = ""
        if engine.station_paused[sid]: ctrl_tag = " [PAUSED]"
        if engine.station_offline[sid]: ctrl_tag = " [OFFLINE]"

        job_info = f"{st.current_job.work_type}:{st.current_job.job_id}" if st.current_job else "-"
        st_table.add_row(
            f"{sid}{ctrl_tag}",
            f"[{status_style}]{st.status.value}[/]",
            st.table_state,
            job_info,
            str(st.drift_counter)
        )
    layout["stations"].update(Panel(st_table, border_style="blue"))

    # 3. Telemetry & Contended Resources
    side_layout = Layout()
    side_layout.split_column(
        Layout(name="resources", size=5),
        Layout(name="evals", size=6),
        Layout(name="failures", ratio=1)
    )

    # Contended Resources
    res_text = Text()
    res_text.append(f"Operators: {engine.num_operators - engine.operator_semaphore._value}/{engine.num_operators} busy ")
    res_text.append(f"({engine.operator_queue_len} stations waiting in queue)\n", style="bold red" if engine.operator_queue_len > 0 else "green")
    res_text.append(f"GPU Slots: {engine.num_gpu_slots - engine.available_gpu_slots}/{engine.num_gpu_slots} in use\n")
    res_text.append(f"Human Labor Expended: {m.operator_labor_seconds:.1f} human-sec ({m.operator_labor_seconds/3600:.2f} hrs)", style="bold yellow")
    side_layout["resources"].update(Panel(res_text, title="Contended Resources"))

    # Eval Batches (Worth Thresholds)
    eval_text = Text()
    for bid, (done, total) in m.eval_batch_progress.items():
        pct = (done / max(1, total)) * 100
        val_status = "WORTHLESS" if done < total else "100% COMPLETE (VALID)"
        style = "green" if done == total else "yellow"
        eval_text.append(f"• {bid}: {done}/{total} eps ({pct:.0f}%) -> [{style}]{val_status}[/{style}]\n")
    side_layout["evals"].update(Panel(eval_text, title="Eval Batches (Gang Value)"))

    # Failures & Preflight Refusals
    f_table = Table(title="Failure & Refusal Ledger", expand=True, box=None)
    f_table.add_column("Category", style="red")
    f_table.add_column("Count", justify="right")
    for r, cnt in m.failures_by_reason.items():
        f_table.add_row(r, str(cnt))
    f_table.add_row("PREFLIGHT REFUSAL", str(m.refused_unsafe), style="magenta")
    side_layout["failures"].update(Panel(f_table))

    layout["telemetry"].update(side_layout)

    # 4. Interactive Control Strip
    footer_text = Text()
    footer_text.append("INTERACTIVE OPERATOR KEYBOARD HOTKEYS:\n", style="bold yellow")
    footer_text.append("[P] Pause st_00   [R] Resume st_00   [A] Abort st_00   [K] Requeue st_00   [O] Mark Restored st_00   [Q] Quit")
    layout["footer"].update(Panel(footer_text, border_style="yellow"))

    return layout


# ---------------------------------------------------------------------------
# Workload Synthesis for Realism
# ---------------------------------------------------------------------------
def make_benchmark_workload() -> List[Job]:
    jobs = []
    # 12 chains of 5 moves each
    for c in range(12):
        curr = CANONICAL_STATE
        for m in range(5):
            nxt = f"STATE_G{c}_M{m+1}"
            jid = f"rep_c{c}_m{m}"
            key = hashlib.sha256(f"{jid}:{curr}:{nxt}".encode()).hexdigest()
            jobs.append(Job(
                job_id=jid, idempotency_key=key, work_type=WorkType.REPLAY, batch_id=None,
                required_state_hash=curr, target_state_hash=nxt, max_duration_sec=31.0,
                preflight_unsafe=False, retry_limit=2
            ))
            curr = nxt

    # 2 Eval Batches (Gang scheduled)
    for b in ["eval_model_alpha", "eval_model_beta"]:
        for e in range(8):
            jid = f"{b}_ep{e}"
            key = hashlib.sha256(jid.encode()).hexdigest()
            jobs.append(Job(
                job_id=jid, idempotency_key=key, work_type=WorkType.EVAL, batch_id=b,
                required_state_hash="ANY", target_state_hash=CANONICAL_STATE, max_duration_sec=25.0,
                preflight_unsafe=False, retry_limit=1
            ))

    # Policy inference jobs
    for p in range(6):
        jid = f"pol_live_{p}"
        key = hashlib.sha256(jid.encode()).hexdigest()
        jobs.append(Job(
            job_id=jid, idempotency_key=key, work_type=WorkType.POLICY, batch_id=None,
            required_state_hash="ANY", target_state_hash=CANONICAL_STATE, max_duration_sec=35.0,
            preflight_unsafe=False, retry_limit=1
        ))

    # Refusal test
    jobs.append(Job(
        job_id="unsafe_grip_01", idempotency_key="unsafe_k1", work_type=WorkType.REPLAY,
        batch_id=None, required_state_hash=CANONICAL_STATE, target_state_hash="STATE_ERR",
        max_duration_sec=31.0, preflight_unsafe=True, retry_limit=0
    ))
    return jobs


# ---------------------------------------------------------------------------
# Head-to-Head Comparative Runner
# ---------------------------------------------------------------------------
async def run_headless_simulation_old(mode: str, seed: int = 42) -> FleetMetrics:
    engine = OperableFleetEngine(mode=mode, seed=seed, sim_speed=500.0)
    engine.submit_jobs(make_benchmark_workload())
    workers = [asyncio.create_task(engine.station_worker(sid)) for sid in engine.stations.keys()]

    # Run until all queued jobs finish or reach terminal state
    while engine.metrics.queued > 0 or engine.metrics.in_flight > 0:
        await asyncio.sleep(0.01)

    engine.running = False
    for w in workers:
        w.cancel()
    return engine.metrics

async def run_headless_simulation(mode: str, seed: int = 42) -> FleetMetrics:
    engine = OperableFleetEngine(mode=mode, seed=seed, sim_speed=5000.0)  # Instant execution
    engine.submit_jobs(make_benchmark_workload())
    workers = [asyncio.create_task(engine.station_worker(sid)) for sid in engine.stations.keys()]

    # Termination guard: exit if work finishes OR fleet becomes completely deadlocked
    idle_ticks = 0
    prev_completed = -1
    
    while engine.metrics.queued > 0 or engine.metrics.in_flight > 0:
        await asyncio.sleep(0.01)
        if engine.metrics.completed == prev_completed and engine.metrics.in_flight == 0:
            idle_ticks += 1
            if idle_ticks > 50:  # No progress possible (e.g. unresolvable chain dead-end)
                break
        else:
            idle_ticks = 0
            prev_completed = engine.metrics.completed

    engine.running = False
    for w in workers:
        w.cancel()
    return engine.metrics


async def run_interactive_dashboard_old():
    engine = OperableFleetEngine(mode="STATE_GRAPH", seed=42, sim_speed=40.0)
    engine.submit_jobs(make_benchmark_workload())

    workers = [asyncio.create_task(engine.station_worker(sid)) for sid in engine.stations.keys()]

    with Live(build_dashboard_view(engine), refresh_per_second=4, console=console) as live:
        for _ in range(60):  # Run live view for 60 ticks (~15 seconds wall-clock)
            live.update(build_dashboard_view(engine))
            await asyncio.sleep(0.25)

    engine.running = False
    for w in workers:
        w.cancel()

async def run_interactive_dashboard():
    # sim_speed=6.0 means a 31s robot move takes ~5.1s wall-clock time
    # An operator 30s reset takes 5.0s wall-clock time
    engine = OperableFleetEngine(mode="STATE_GRAPH", seed=42, sim_speed=6.0)
    engine.submit_jobs(make_benchmark_workload())

    workers = [asyncio.create_task(engine.station_worker(sid)) for sid in engine.stations.keys()]

    console.clear()
    with Live(build_dashboard_view(engine), refresh_per_second=5, console=console) as live:
        try:
            # Keeps running until all jobs are either COMPLETED, FAILED, or REFUSED
            while engine.metrics.queued > 0 or engine.metrics.in_flight > 0:
                live.update(build_dashboard_view(engine))
                await asyncio.sleep(0.2)
            
            # Final refresh once everything settles
            live.update(build_dashboard_view(engine))
            await asyncio.sleep(3.0)
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass

    engine.running = False
    for w in workers:
        w.cancel()
        
def print_comparison_table(metrics_graph: FleetMetrics, metrics_strict: FleetMetrics):
    console.print("\n" + "=" * 78, style="bold green")
    console.print("⚖️  HEAD-TO-HEAD COMPARATIVE RESULTS (SAME WORKLOAD & SEED)", style="bold cyan")
    console.print("=" * 78, style="bold green")

    t = Table(expand=True)
    t.add_column("Metric Dimension", style="bold")
    t.add_column("Approach A: State-Graph Dispatch (Adopted)", style="bold green")
    t.add_column("Approach B: Strict Sequential Chains", style="bold yellow")
    t.add_column("Delta / Operator Savings", style="bold magenta")

    # Metrics
    op_graph_hr = metrics_graph.operator_labor_seconds / 3600.0
    op_strict_hr = metrics_strict.operator_labor_seconds / 3600.0
    saved_hr = op_strict_hr - op_graph_hr

    sim_graph_hr = metrics_graph.simulated_elapsed_seconds / 3600.0
    sim_strict_hr = metrics_strict.simulated_elapsed_seconds / 3600.0

    t.add_row(
        "Total Operator Labor Required",
        f"{metrics_graph.operator_labor_seconds:.1f} s ({op_graph_hr:.2f} hrs)",
        f"{metrics_strict.operator_labor_seconds:.1f} s ({op_strict_hr:.2f} hrs)",
        f"Saved {saved_hr:.2f} human-hrs ({(saved_hr/max(0.01, op_strict_hr))*100:.1f}% less work)"
    )
    t.add_row(
        "Total Wall-Clock Fleet Time",
        f"{metrics_graph.simulated_elapsed_seconds:.1f} s ({sim_graph_hr:.2f} hrs)",
        f"{metrics_strict.simulated_elapsed_seconds:.1f} s ({sim_strict_hr:.2f} hrs)",
        f"{(sim_strict_hr - sim_graph_hr):.2f} hrs faster"
    )
    t.add_row(
        "Jobs Completed Successfully",
        f"{metrics_graph.completed} / {metrics_graph.total_jobs}",
        f"{metrics_strict.completed} / {metrics_strict.total_jobs}",
        f"+{metrics_graph.completed - metrics_strict.completed} moves"
    )
    t.add_row(
        "Failed Moves",
        str(metrics_graph.failed),
        str(metrics_strict.failed),
        f"{metrics_strict.failed - metrics_graph.failed} fewer aborts"
    )
    t.add_row(
        "Preflight Refusals (Zero Station Time)",
        str(metrics_graph.refused_unsafe),
        str(metrics_strict.refused_unsafe),
        "Identical (Zero leak)"
    )

    console.print(t)
    console.print("\n[bold]Key Takeaway:[/] Strict Chains lock stations into dead-ends upon intermediate move failure, forcing humans to reset the table over and over. State-Graph Dispatch reuses existing valid states across all queued work, drastically cutting human labor on the floor.\n")


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fleet Dashboard & Comparison Engine")
    parser.add_argument("--compare", action="store_true", help="Runs head-to-head comparison between State-Graph and Strict Chains")
    args = parser.parse_args()

    if args.compare:
        console.print("[bold yellow]Running Head-to-Head Comparative Simulation (Seed=42)...[/]")
        m_graph = asyncio.run(run_headless_simulation(mode="STATE_GRAPH", seed=42))
        m_strict = asyncio.run(run_headless_simulation(mode="STRICT_CHAINS", seed=42))
        print_comparison_table(m_graph, m_strict)
    else:
        asyncio.run(run_interactive_dashboard())