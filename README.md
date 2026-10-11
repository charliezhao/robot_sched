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

## Key Features Added

### 1. State-Preservation & Priority Dispatch
* **$O(1)$ Partitioned Dispatch Hierarchy**:
  1. **Exact-State Replay Continuation**: Prioritizes queued moves matching the current non-canonical physical grid state (`STATE_GRID_C*_M*`), preventing wildcards from destroying expensive multi-step progress.
  2. **EVAL Gang Batches**: Dispatches batch evaluation episodes when GPU resources are available.
  3. **Policy Workloads**: Dispatches closed-loop policy runs subject to station reliability bounds.
  4. **Root Replay Trajectories**: Launches Move 0 trajectories from canonical ground truth (`STATE_CANONICAL_S0`).
* **Wildcard (`"ANY"`) Guardrails**: Prevents closed-loop eval/policy runs from wiping intermediate table states unless no exact continuation exists.

### 2. Empirical Reliability & Drift Routing
* **Online Failure Probability Tracking ($P_{\text{fail}}$)**: Dynamically estimates station reliability with cumulative kinematic drift penalties:
  $$P_{\text{fail}} = \frac{\text{failures}}{\text{failures} + \text{successes}} + (0.03 \times \text{drift})$$
* **Chain-Depth Risk Gating**: Deep-chain trajectory moves ($m \ge 2$) require strictly lower $P_{\text{fail}}$ stations, preventing brittle stations from aborting downstream multi-step sequences.
* **Autonomous Recalibration**: Automatically triggers zeroing and clears kinematic drift when stations reach critical operational thresholds ($K \ge 4$).

### 3. Physical State & Fault Convergence Model
* **5 Station Lifecycle States**: `IDLE`, `BUSY`, `AWAITING_OPERATOR`, `OPERATOR_RESETTING`, `CALIBRATING`.
* **26 Physical Table States**:
  * 1 Canonical Datum (`STATE_CANONICAL_S0`).
  * 24 Intermediate Replay Grids ($6\text{ chains } \times 4\text{ moves}$).
  * 1 Physical Fault state (`DIRTY`).
* **Operator Labor Queue (`asyncio.Semaphore(2)`)**: Realistically simulates labor constraints where 10 automated stations share 2 human technicians for jig-based surface resets back to $S_0$.

### 4. Forensic Decision Ledger & Explainability CLI
* **`decision_audit_ledger`**: Every candidate traversal cycle logs exact audit metadata (timestamp, station state, target state, free GPUs, verdict, and forensic reasoning) to SQLite WAL.
* **`explain_job_wait.py` CLI**:
  ```bash
  python explain_job_wait.py <job_id>

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