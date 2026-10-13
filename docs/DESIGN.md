# DESIGN.md: Fleet Queue & Scheduler Architecture

## 1. Executive Summary & The Bottleneck Arithmetic

Before designing scheduling structures, we analyze the physical and labor constraints imposed by the benchmark numbers:
* **Queued workload:** 4,840 chains $\times$ ~6 moves = 29,040 moves.
* **Nominal move execution:** 31 s.
* **Failure rate:** 50% ($14,520$ failures across the run).
* **Fleet size:** 10 robot stations.
* **Human operator pool:** 2 operators.

### Human Intervention Demand
Assuming a physical intervention of **120 s (2.0 min)**—accounting for transit across the lab floor, clearing scattered objects, aligning cubes via a mechanical jig, stepping outside the light curtain, and confirming safety handoff:
$$\text{Total Human Work} = 14,520 \text{ failures} \times 120\text{ s} = 1,742,400\text{ human-seconds} \approx 484.0\text{ human-hours}$$
With 2 operators working continuously in parallel:
$$\text{Wall-Clock Time dictated by Operators} = \frac{484.0\text{ human-hours}}{2\text{ operators}} = \mathbf{242.0\text{ hours}}\;(\approx 10.1\text{ days})$$

Even if human intervention is reduced to an aggressive **30 s**:
$$\text{Total Human Work} = 14,520 \times 30\text{ s} = 435,600\text{ s} = 121.0\text{ human-hours} \implies \frac{121.0}{2} = \mathbf{60.5\text{ hours}}\;(\approx 2.5\text{ days})$$

### Robot Station Capacity Demand
* **Active execution:** $29,040\text{ moves} \times 31\text{ s} = 900,240\text{ station-seconds} = 250.1\text{ station-hours}$.
* **Recalibration & transit overhead:** Recalibrating every $K=4$ moves ($40\text{ s}$ overhead amortized to $10\text{ s/move}$):
  $$\frac{29,040}{4} \times 40\text{ s} = 290,400\text{ station-seconds} = 80.7\text{ station-hours}$$
* **Total station demand:** $250.1 + 80.7 = 330.8\text{ station-hours}$.
* **Wall-clock time dictated by stations:** 
  $$\frac{330.8\text{ station-hours}}{10\text{ stations}} = \mathbf{33.1\text{ hours}}\;(\approx 1.4\text{ days})$$

### Primary Bottleneck Conclusion
The operator pool bottlenecks the fleet by a factor of **$1.8\times$ (at 30 s)** to **$7.3\times$ (at 120 s)**. Station arm utilization and GPU slot packing are secondary concerns. **The primary purpose of the scheduler is operator conservation**:
1. Maximize autonomous runs without requiring human table intervention.
2. Decouple open-loop moves from brittle linear chains into a shared state-graph.
3. Eliminate redundant resets by treating physical table state as a shared, reusable resource.

---

## 2. Station State Machine

A station tracks mechanical actuation, drift accumulation, and verified physical table state:

```
                        ┌──────────────────┐
                        │     OFFLINE      │
                        │  (Maintenance)  │
                        └────────┬─────────┘
                                 │ Operator enables station
                                 ▼
                        ┌──────────────────┐
     ┌─────────────────►│ IDLE_CALIBRATED  │◄───────────────────────────┐
     │                  │  (Table Trusted) │                            │
     │                  └────────┬─────────┘                            │
     │                           │ Scheduler dispatches job             │
     │                           ▼                                      │
     │                  ┌──────────────────┐                            │
     │                  │       BUSY       │                            │
     │                  │ (Move Executing) │                            │
     │                  └────────┬─────────┘                            │
     │                           │                                      │
     │            ┌──────────────┴──────────────┐                       │
     │            │                             │                       │
     │      Move SUCCEEDS                 Move FAILS                    │
     │            │                             │                       │
     │     [Check Drift]                        │                       │
     │     moves < K?                           ▼                       │
     │     ┌──────┴──────┐             ┌──────────────────┐             │
     │ Yes │             │ No          │AWAITING_OPERATOR │             │
     │     │             ▼             │  (Table Dirty)   │             │
     │     │    ┌──────────────────┐   └────────┬─────────┘             │
     │     │    │   CALIBRATING    │            │                       │
     │     │    │  (Re-zero ~40s)  │            │ Operator arrives      │
     │     │    └────────┬─────────┘            ▼                       │
     │     │             │             ┌──────────────────┐             │
     │     │             │             │ OPERATOR_RESET   │             │
     │     │             │             │ (Human at Table) │             │
     │     │             │             └────────┬─────────┘             │
     │     │             │                      │                       │
     │     │             │                      │ Operator clicks       │
     │     │             │                      │ "Table Restored"      │
     │     │             │                      │ (Grid validated)      │
     └─────┴─────────────┴──────────────────────┴───────────────────────┘
```

### State Transitions
* **`IDLE_CALIBRATED` $\to$ `BUSY`:** Dispatched when a candidate job's `required_state_hash` matches `station.table_state` and compute locks are secured.
* **`BUSY` $\to$ `IDLE_CALIBRATED`:** Move completes successfully and `drift_counter < K`. Table state advances to `target_state_hash`.
* **`BUSY` $\to$ `CALIBRATING`:** Move completes successfully and `drift_counter >= K`. The arm homes and zeroes grippers (~40 s), resetting `drift_counter = 0`.
* **`BUSY` $\to$ `AWAITING_OPERATOR`:** Move aborts, slips, or trips torque limits. Station marks `table_state = DIRTY` and enters the prioritized operator queue.
* **`AWAITING_OPERATOR` $\to$ `OPERATOR_RESET` $\to$ `IDLE_CALIBRATED`:** Operator locks arm in safe compliant mode, realigns cubes via jig to canonical arrangement $S_0$, and triggers restoration. Automated camera preflight validates alignment, zeroes the gripper, and transitions station back to `IDLE_CALIBRATED`.

---

## 3. Replay Model: Strict Sequences vs. State-Graph Dispatch

### Model A: Strict Chains (Naive)
Moves are chained: $M_1 \to M_2 \dots \to M_6$. If $M_3$ fails, downstream moves $M_4 \dots M_6$ are invalidated until an operator resets the table.
* **Cost:** With $P(\text{fail}) = 0.50$, chain completion probability is $0.5^6 = 1.56\%$. Over $98.4\%$ of chains fail midway, requiring human intervention almost every run.
* **Verdict:** Unviable. Saturates the operator bottleneck immediately.

### Model B: State-Graph Dispatch (Adopted)
A move is modeled as a directed edge on a grid configuration multigraph:

$$
e = (S_{\text{from}}, \text{trajectory\_id}, S_{\text{to}})
$$

* **Advantage:** When a station reaches verified arrangement $S_k$, the scheduler looks up any job in the entire pool requiring $S_k$, regardless of which demonstration generated it.
* **Failure isolation:** A failure at move 3 on Station 1 only marks Station 1 dirty; it does not block candidate moves needing $S_3$ from running on Station 2 if Station 2 happens to be in state $S_3$.
* **Verification cost:** Open-loop moves accumulate spatial variance. To avoid blind cascades, an overhead static camera runs a sub-second ($~200\text{ ms}$) bounding-box/AprilTag occupancy verification upon move completion. If the check passes, the table transitions to $S_{\text{to}}$.

---

## 4. Failure Protocol & Trusting Human State

### Protocol on Failure
1. **Safing:** Hardware cuts motor drive, releases GPU slots, and sets `table_state = DIRTY`.
2. **Standardized Human Workflow:** Operators **never** attempt to reconstruct arbitrary intermediate states. They sweep the table and seat cubes into a **mechanical alignment jig** defining canonical baseline state $S_0$.
3. **Automated Verification Gate:** Human placement introduces $\pm 10\text{ mm}$ error, which exceeds open-loop tolerances. The station **never trusts manual placement blindly**.
   * Tapping "Table Restored" triggers an automated overhead camera capture.
   * If all cubes sit within $\pm 2\text{ mm}$ bounding tolerances of $S_0$, the station zeroes the gripper, sets `table_state = S_0`, and transitions to `IDLE_CALIBRATED`.
   * If alignment fails, the station remains blocked in `OPERATOR_RESET` with an audible warning indicating the misaligned cell.

---

## 5. Unified Queuing & Resource Contention

The four workloads contend across two independent resource dimensions:

```
                     ┌─────────────────────────────────────────────────────────────┐
                     │                 GLOBAL JOB REPOSITORY (LEDGER)              │
                     │  All submissions persist here with a unified QueueItem spec │
                     └──────────────────────────────┬──────────────────────────────┘
                                                    │
                                     Indexed by execution constraints
                                                    │
        ┌──────────────────────────────┬────────────┴─────────────────┬──────────────────────────────┐
        ▼                              ▼                              ▼                              ▼
 ┌──────────────┐               ┌──────────────┐               ┌──────────────┐               ┌──────────────┐
 │  EVAL GANG   │               │ POLICY QUEUE │               │REPLAY INDEX  │               │ SHADOW QUEUE │
 │ (deque[Job]) │               │ (deque[Job]) │               │dict[S, deque]│               │ (deque[Job]) │
 └──────┬───────┘               └──────┬───────┘               └──────┬───────┘               └──────┬───────┘
        │                              │                              │                              │
        │ Needs BOTH                   │ Needs BOTH                   │ Needs Station Only           │ Needs GPU Only
        │ (Gang Priority)              │ (Timeout Watchdog)           │ (Matches S_table)            │ (Passive Listener)
        ▼                              ▼                              ▼                              │
   ┌────────────────────────────────────────┐                         │                              │
   │       DUAL-RESOURCE COORDINATOR        │                         │                              │
   │  Acquires: 1 Station + 1 GPU Slot      │                         │                              │
   └───────────────────┬────────────────────┘                         │                              │
                       │                                              │                              │
                       └───────────────────────┐                      │                              │
                                               ▼                      ▼                              ▼
                                     ┌───────────────────────────────────┐        ┌──────────────────────────────┐
                                     │     10 PHYSICAL ROBOT STATIONS    │        │       M GPU SLOTS POOL       │
                                     │  • Station holds physical state S │        │  • Shared inference capacity │
                                     │  • Contended by Replay, Eval, Pol │        │  • Contended by Eval, Pol,   │
                                     └─────────────────┬─────────────────┘        │    and Shadow listeners      │
                                                       │                          └──────────────┬───────────────┘
                                                       │ Live telemetry stream (s_t, a_t)        │
                                                       └─────────────────────────────────────────┘
                                                          Shadow attaches if GPU slot free;
                                                          drops frame if busy (zero arm stall)
```

1. **Eval Checkpoints (All-or-Nothing Batches):** Evaluated via gang scheduling. Once an eval batch begins, the scheduler assigns it high priority across stations and GPU slots to drain all episodes to 100% completion.
2. **Policy Inference (Variable Runtime):** Two-phase atomic lock: requires both an idle station and an available GPU slot. Enforces a strict `max_duration_sec` watchdog to prevent hung policies from holding stations indefinitely.
3. **Trajectory Replay (Bulk State-Graph Work):** Needs a station only. Matched via hash table $O(1)$ lookup (`replay_by_state[station.table_state]`).
4. **Shadow World-Model (Zero Perturbation Sidecar):** Does not occupy a station. When any station runs a move, shadow work taps the telemetry feed if a GPU slot is available. If GPU slots are saturated, frames drop silently; it never stalls physical arms.

---

## 6. Drift & Recalibration Policy

* **Cost trade-off:** Recalibration takes $40\text{ s}$ of station time. Skipping it saves $40\text{ s}$ immediately but compounds mechanical drift, increasing failure probability $\overline{P}_{\text{fail}}(K) = P_0 + \frac{\alpha (K - 1)}{2}$.
* **System loss function:** Weighting operator penalty by the fleet bottleneck ratio ($\lambda = \frac{10\text{ stations}}{2\text{ ops}} = 5.0$):
  $$\text{Loss}(K) = \frac{40}{K} + \lambda \cdot \overline{P}_{\text{fail}}(K) \cdot T_{\text{op}} = \frac{40}{K} + 5.0 \cdot \left(0.50 + \frac{\alpha (K - 1)}{2}\right) \cdot 120$$
  $$\frac{d}{dK}\text{Loss}(K) = -\frac{40}{K^2} + 300\alpha = 0 \implies K^* = \sqrt{\frac{2}{15\alpha}}$$
* For drift coefficient $\alpha \in [0.005, 0.015]$ ($0.5\%\text{--}1.5\%$ added failure risk per uncalibrated move), the mathematical optimum is **$K^* = 3$ to $4$ moves**.
* **Zero-Cost Hook:** Any operator reset or safety trip automatically zeroes `drift_counter` for free.

---

## 7. Minimal Per-Item Metadata

Every field maps directly to an explicit decision branch in the scheduler:

| Field | Type | Scheduling Decision Driven by Field |
| :--- | :--- | :--- |
| `job_id` | `string (UUID)` | Idempotency deduplication, WAL tracking, and status querying. |
| `work_type` | `enum` | Resource lock routing: Station-only (`REPLAY`), Station+GPU (`POLICY`, `EVAL`), or GPU-only (`SHADOW`). |
| `batch_id` | `string / null` | Gang prioritization: groups all-or-nothing Eval episodes. |
| `required_state_hash` | `string` | Station filter: matches `station.table_state` for autonomous dispatch. |
| `target_state_hash` | `string` | State propagation: updates station state upon move success. |
| `max_duration_sec` | `float` | Watchdog timer: aborts variable-length policy inference if exceeded. |
| `preflight_unsafe` | `bool` | Safety refusal: skips dispatch and marks `REFUSED_UNSAFE` without moving arms. |
| `retry_limit` | `int` | Failure quarantine: determines whether a failed move re-queues or marks permanently failed. |

*Deliberately excluded:* Continuous trajectory coordinate arrays (stored in blob store), human descriptions, and deep neural network checkpoint binaries.

---

## 8. Deduplication, Retries, and Persistence

### Deduplication
Incoming jobs compute a deterministic payload hash:

$$
\text{idempotency\_key} = \text{SHA256}(\text{work\_type} + \text{batch\_id} + \text{required\_state} + \text{target\_state} + \text{trajectory\_id})
$$

Duplicate submissions return existing job state without re-queueing or re-running physical moves.

### Reproducible Retries
* Trajectory replays reuse identical recorded joint paths and execute only after table state is verified to match `required_state_hash`.
* Policy inference and evals use deterministic PRNG seeds derived from `SHA256(job_id + attempt_count)`.
* Jobs failing up to `retry_limit` re-enter the queue. Jobs exceeding `retry_limit` are marked `FAILED_PERMANENT` to prevent infinite operator reset loops on defective trajectories.

### Crash Recovery (WAL Persistence)
State transitions are committed to SQLite with Write-Ahead Logging (`PRAGMA journal_mode = WAL`) prior to motor actuation.

```
                           SCHEDULER RESTARTS
                                   │
                                   ▼
             Scan `stations` table for `status == 'BUSY'`
                                   │
            ┌──────────────────────┴──────────────────────┐
            ▼ Found                                       ▼ None Found
 [In-Flight Physical Work Detected]             Resume normal dispatch
  • Station was killed mid-move!                from IDLE stations
  • Mark station:
      table_state_hash = 'DIRTY'
      status = 'AWAITING_OPERATOR'
  • Lookup interrupted `current_job_id`
            │
            ▼
 [Reconcile Interrupted Job]
  • If attempt_count < retry_limit:
      Mark job QUEUED (eligible for future run)
  • If attempt_count >= retry_limit:
      Mark job FAILED
            │
            ▼
 [Rebuild In-Memory Indexes]
  • Load all `QUEUED` jobs into state-index maps
  • Stations in IDLE resume dispatch immediately
```

* **Zero Work Lost:** All submitted jobs and batch counters persist in SQLite.
* **Zero Double-Runs:** Stations interrupted mid-flight are downgraded to `DIRTY` / `AWAITING_OPERATOR`, preventing accidental blind execution on untrusted physical arrangements.

---

## 9. Scoping Boundaries: What We Deliberately Cut

1. **No Physics / Continuous Dynamics Simulators:** Moves are discrete state transitions with simulated time delays and stochastic failure models.
2. **No CUDA / GPU Memory Allocator:** GPU capacity is modeled as an integer semaphore ($M$ slots).
3. **No Distributed Microservices:** Runs as an in-process asynchronous engine backed by embedded SQLite WAL to prevent distributed consensus edge cases.
4. **No Continuous 6-DoF Computer Vision:** Table state is verified using discrete hash matching with simulated vision noise.
5. **No Dynamic Motion Planning / Inverse Kinematics:** Joint paths are pre-recorded or policy-generated black boxes.


## 10. Make it watchable, and make it operable

Here is the complete implementation for Phase 3: Make it watchable, and make it operable.This delivers:Live Interactive Dashboard (Rich / Textual TUI): Fleet-wide view showing all 10 stations, operator queue, GPU slot utilization, eval completion thresholds, failure/refusal telemetry breakdown, and dynamic projected finish times.Interactive Station Panel & Safety Interventions: Interactive operator controls (pause, abort, requeue, table_restored, take_offline).Mid-Decision Concurrency Safety: Explicit transactional fencing via lock hierarchy and optimistic SQLite versioning ensuring operator interventions never race with scheduler dispatch.Fast Clock Simulation: Runs at customizable acceleration ($50\times$ to $200\times$) so multi-day runs finish in minutes.Head-to-Head Comparative Benchmark: Runs the identical workload and PRNG seed across State-Graph Dispatch vs. Strict Sequential Chains, outputting operator labor hours, idle starvation, and total wall clock.Part 1: What Makes Human Intervention Mid-Decision Safe?When an operator reaches in to pause a station or mark a table restored while the scheduler loop is actively deciding, two race conditions emerge:The Phantom Dispatch Race: The scheduler inspects Station 3 as IDLE, selects a candidate job, but before it dispatches, the operator clicks "Take Offline" or "Pause". If the scheduler proceeds blindly, the arm commands a paused/servicing station.The Stale State Race: The scheduler reads table_state = STATE_CANONICAL_S0 and begins acquiring GPU/operator locks, but the operator bumps the table or clicks "Mark Dirty".

### The Safety Guarantees Implemented in Code:  
```
        OPERATOR ACTION THREAD / UI                SCHEDULER DISPATCH LOOP
               │                                         │
               │ (1) Acquire station_mutex[sid]          │
               ├────────────────────────────────────────►│ Blocked waiting for lock
               │                                         │
               │ (2) Atomic DB Transition:               │
               │     SET status = 'PAUSED',              │
               │     version = version + 1               │
               │                                         │
               │ (3) Release station_mutex[sid]          │
               │                                         │
               │                                         ▼
               │                            Acquires station_mutex[sid]
               │                            Validates Pre-Condition:
               │                            if station.status != IDLE:
               │                                Abort dispatch cycle!

```
1.**Two-Phase Optimistic State Verification: Before dispatching an arm command, the scheduler validates that station.status == IDLE under an atomic lock. If an operator intervened mid-decision, the dispatch cycle immediately aborts with zero side effects.
2.**Transactional Fencing in SQLite: Every status update checks row versioning (WHERE station_id = ? AND status = 'IDLE'). If an operator intervention updated the row concurrently, the scheduler's write fails with 0 rows affected.
3.**Idempotent Abort / Requeue: Aborting mid-move cuts arm commands instantly, immediately marks the table state DIRTY, releases any allocated GPU slots, and routes the aborted job back to QUEUED (incrementing attempt_count).
