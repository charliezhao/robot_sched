# Robot Fleet Queue & Scheduler

An asynchronous, crash-resilient scheduling engine managing four heterogeneous robotics workloads across 10 physical stations under tight human operator (2 operators) and GPU inference constraints.

---

## Key Features

- **Operator-Conservation Scheduling:** Addresses the fleet's primary bottleneck (2 operators vs. 10 stations, a $1.8\times$ to $7\times$ starvation ratio) by decoupling linear moves into a state-graph to reuse verified physical table arrangements.
- **Unified Two-Resource Arbitration:** Schedules 4 distinct workloads (open-loop Replay, closed-loop Policy, all-or-nothing Eval gangs, and zero-perturbation Shadow sidecars) across physical stations and shared GPU inference slots.
- **ACID Crash Resilience (`kill -9` Safe):** State transitions persist to an embedded SQLite WAL ledger before hardware moves. Interrupted in-flight stations recover cleanly as `DIRTY` / `AWAITING_OPERATOR` with zero dropped jobs and zero duplicate physical runs.
- **Preflight Refusal Accounting:** Unsafe trajectories are rejected deterministically at ingestion and logged separately from physical execution failures.
- **Interactive TUI Dashboard:** Real-time terminal view (`rich`) showing per-station progress, operator queues, GPU allocation, gang eval thresholds, failure breakdowns, and live keyboard interventions.

---

## Architecture Overview

```text
├── models.py            # Job/Station schemas, enums, and minimal metadata
├── storage.py           # SQLite WAL persistence, state transitions, crash recovery
├── scheduler.py         # Dual-resource matching, gang priority, state-graph indexing
├── main.py              # Engine lifecycle runner and crash-resume demonstrator
├── dashboard.py         # Live interactive TUI and head-to-head comparison benchmark
├── tests/               # Pytest suite validating scheduling invariants
└── DESIGN.md            # Comprehensive architecture and bottleneck analysis
```

---

## Quickstart

### 1. Requirements & Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install rich pytest
```

### 2. Run Engine & Demonstrate Crash Recovery
Runs an active workload, simulates an abrupt crash mid-run (`kill -9`), restarts from the SQLite WAL ledger, and verifies state reconciliation:
```bash
python main.py --demonstrate-resume
```

### 3. Launch the Interactive Fleet Dashboard
Starts the real-time TUI dashboard with live hotkey controls:
```bash
python dashboard.py
```
- **`0`–`9`**: Target specific station (`st_00` to `st_09`)
- **`P` / `R`**: Pause / Resume entire fleet simulation
- **`A`**: Abort active move on targeted station (marks table `DIRTY`)
- **`K`**: Requeue active job on targeted station
- **`O`**: Operator table restore to canonical baseline ($S_0$)
- **`Q`**: Exit cleanly

### 4. Run Head-to-Head Benchmark
Compares **State-Graph Dispatch** against **Strict Sequential Chains** using the identical workload and PRNG seed:
```bash
python dashboard.py --compare
```

### 5. Run Unit Tests
```bash
pytest -v
```