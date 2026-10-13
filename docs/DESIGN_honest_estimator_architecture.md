# Part 1: Drift-Aware Honest Finish-Time Estimator Architecture

## 1. Overview & Problem Definition

In an open-loop or naive scheduler, projected time-to-completion (ETA) assumes static parameters:

$$\text{ETA}_{\text{naive}} = \frac{N_{\text{remaining}} \times T_{\text{nominal}}}{N_{\text{stations}}}$$

In an active robot workcell fleet (10 stations, 2 human operators, 4 shared inference GPUs), this projection fails for three reasons:

1. **Heterogeneous & Drifting Failure Rates:** Stations wear unevenly. Joint thermal expansion, gripper pad friction degradation, and mechanical backlash increase move failure rate as uncalibrated moves accumulate ($P_{\text{fail}}(k) = P_0 + k \cdot \alpha_{\text{drift}}$).
2. **Hidden Multiplier of Human Downtime:** A physical move failure does not merely consume $T_{\text{run}} = 31\,\text{s}$. It places the station in an `AWAITING_OPERATOR` state, triggering an $M/M/2$ or $G/G/2$ queuing bottleneck where stations sit idle waiting for an operator.
3. **Contended Operator Throughput Ceiling:** The fleet cannot complete work faster than the human operator pool can execute physical jig resets and camera alignments ($T_{\text{reset}} = 30\,\text{s}$).

---

## 2. Mathematical Formulation

### 2.1 Online Per-Station Bayesian Failure Tracking

Each station $i \in \{0, \dots, 9\}$ models its failure distribution as a Beta-Binomial conjugate model with parameters $(\alpha_i, \beta_i)$:

- Initial non-informative prior: $\alpha_i = 1.0, \; \beta_i = 1.0$
- Move outcome update $y \in \{0, 1\}$ (where $1 = \text{failure}$, $0 = \text{success}$):

$$\alpha_i \leftarrow \alpha_i + y, \quad \beta_i \leftarrow \beta_i + (1 - y)$$

The empirical base failure probability is:

$$\hat{p}_{i,\text{base}} = \frac{\alpha_i}{\alpha_i + \beta_i}$$

Accounting for uncalibrated consecutive move drift count $k_i \in [0, K-1]$:

$$\hat{p}_{i,\text{fail}} = \min\left(0.95, \; \hat{p}_{i,\text{base}} + k_i \cdot \alpha_{\text{drift}}\right)$$

---

### 2.2 Empirical Operator Wait Tracking

The engine monitors the rolling average duration $W_{\text{op}}$ that stations spend waiting in `AWAITING_OPERATOR` before being claimed by an operator:

$$W_{\text{op}} = \frac{1}{|S_{\text{wait}}|} \sum_{s \in S_{\text{wait}}} \Delta t_{\text{wait}}(s)$$

Where $S_{\text{wait}}$ is a sliding window of recent operator claims (default size: 50).

---

### 2.3 Effective Machine Cycle Time per Unit

To yield one completed move, station $i$ expects:

$$\mathbb{E}[R_i] = \frac{\hat{p}_{i,\text{fail}}}{1 - \hat{p}_{i,\text{fail}}}$$

failed attempts before a success. Each failure wastes machine run time, human queue wait time, and jig reset duration:

$$T_{\text{wasted}} = T_{\text{run}} + W_{\text{op}} + T_{\text{reset}}$$

Factoring in periodic recalibration ($T_{\text{calib}} = 40\,\text{s}$ every $K = 4$ moves), the expected machine time required on station $i$ to complete one unit of work is:

$$\mathbb{E}[T_{\text{station}, i}] = T_{\text{run}} + \left(\frac{\hat{p}_{i,\text{fail}}}{1 - \hat{p}_{i,\text{fail}}}\right) \cdot (T_{\text{run}} + W_{\text{op}} + T_{\text{reset}}) + \frac{T_{\text{calib}}}{K}$$

Summing over all operational stations gives the **Machine Throughput Capacity Rate**:

$$\mu_{\text{fleet}} = \sum_{i=1}^{N_{\text{stations}}} \frac{1}{\mathbb{E}[T_{\text{station}, i}]} \quad [\text{completed units / second}]$$

---

### 2.4 Human Operator Capacity Ceiling

The human pool ($N_{\text{operators}} = 2$) can process resets at an aggregate rate:

$$\mu_{\text{resets}} = \frac{N_{\text{operators}}}{T_{\text{reset}}} \quad [\text{resets / second}]$$

Across the fleet, the average failure rate is $\bar{p}_{\text{fail}} = \frac{1}{N_{\text{stations}}} \sum_{i=1}^{N_{\text{stations}}} \hat{p}_{i,\text{fail}}$. The expected human resets required per completed unit is:

$$\bar{R} = \frac{\bar{p}_{\text{fail}}}{1 - \bar{p}_{\text{fail}}}$$

Thus, the maximum possible system throughput bounded by operator availability is:

$$\mu_{\text{op\\_bound}} = \frac{\mu_{\text{resets}}}{\bar{R}} = \frac{N_{\text{operators}} \cdot (1 - \bar{p}_{\text{fail}})}{T_{\text{reset}} \cdot \bar{p}_{\text{fail}}} \quad [\text{completed units / second}]$$

---

### 2.5 Honest Finish-Time Estimate (Bound Integration)

The honest completion rate $\mu_{\text{effective}}$ is constrained by the tighter of machine availability and human capacity:

$$\mu_{\text{effective}} = \min\left( \mu_{\text{fleet}}, \;\; \mu_{\text{op\\_bound}} \right)$$

For $N_{\text{remaining}}$ uncompleted units in the queue:

$$\text{ETA}_{\text{honest}} = \frac{N_{\text{remaining}}}{\mu_{\text{effective}}}$$

Additionally, if $\mu_{\text{op\\_bound}} < \mu_{\text{fleet}}$, the system flags `is_operator_bottleneck = True`, alerting operations that adding stations will yield zero throughput improvement without adding human operators.