This is a crucial architectural cleanup. Currently, `cl-revenue-ops` exposes a "sprawling" API surface (13+ commands) that mixes debugging, configuration, manual overrides, and reporting.

To prepare for `cl-hive` integration while hardening security, we should move to a **"Policy-Driven" Architecture**. Instead of `cl-hive` micromanaging specific actions (e.g., "Set Fee"), it should register **Policies** (e.g., "Treat Peer X as VIP").

Here is the plan to reduce RPC count, unify logic, and secure the interface.

---

# Architecture Plan: API Unification & Hardening

## 1. The Strategy: Consolidate & Deprecate

We will reduce the public RPC interface from **13 commands** to **5 core commands**.

| Category | Current Commands (To Deprecate) | **New Unified Command** | Purpose |
| :--- | :--- | :--- | :--- |
| **Control** | `revenue-ignore`<br>`revenue-unignore`<br>`revenue-set-fee` (Manual)<br>`revenue-remanage` | **`revenue-policy`** | Sets per-peer rules (Ignore, Hive-Member, Fixed-Fee). **Primary hook for cl-hive.** |
| **Config** | `revenue-config` | **`revenue-config`** | (Unchanged) Sets global algorithm variables. |
| **Reporting** | `revenue-status`<br>`revenue-history`<br>`revenue-profitability`<br>`revenue-clboss-status`<br>`revenue-list-ignored` | **`revenue-report`** | Unified read-only data dump. `type=summary|peer|financial`. |
| **Action** | `revenue-rebalance` | **`revenue-pump`** | Manual rebalance override (Renamed to indicate active intervention). |
| **Planning** | `revenue-capacity-report`<br>`revenue-analyze` | **`revenue-plan`** | Strategic output for Splice/Close/Open decisions. |

---

## 2. The New Integration Hook: `revenue-policy`

This is the most critical change. `cl-hive` will not call "Set Fee." It will call `revenue-policy` to tag a peer. `cl-revenue-ops` then applies its own logic based on that tag.

### Schema
`revenue-policy set <peer_id> [key=value] ...`

### Supported Keys (The Interface Contract)
1.  **`strategy`**:
    *   `dynamic` (Default: Hill Climbing + Scarcity)
    *   `static` (Manual fee)
    *   `hive` (0-Fee / Low Fee for fleet)
    *   `passive` (Do not touch fees, let CLBOSS handle it)
2.  **`rebalance`**:
    *   `enabled` (Default)
    *   `disabled` (Do not rebalance to/from)
    *   `source_only` (Only drain)
    *   `sink_only` (Only fill)
3.  **`tags`**:
    *   `["hive", "friend", "toxic"]`

### Example Usage (How cl-hive talks to us)

**Scenario 1: `cl-hive` handshakes with a new fleet member.**
*   *Old Way:* `cl-hive` tries to calculate fees and call `revenue-set-fee` every 30 mins. (Race conditions).
*   *New Way:* `cl-hive` calls:
    ```bash
    revenue-policy set <peer_id> strategy=hive tags=hive
    ```
    *Result:* `cl-revenue-ops` detects `strategy=hive`, bypasses Hill Climbing, bypasses Scarcity Pricing, and applies the `config.hive_fee_ppm` setting automatically in its loop.

**Scenario 2: `cl-hive` bans a toxic peer.**
*   *New Way:*
    ```bash
    revenue-policy set <peer_id> strategy=passive rebalance=disabled tags=toxic
    ```

---

## 3. The New Reporting Hook: `revenue-report`

`cl-hive` needs data to make routing decisions. It shouldn't scrape logs.

### Schema
`revenue-report [scope] [format]`

*   **`revenue-report summary json`**: Returns Net Worth, Margin, Active Channel count.
*   **`revenue-report peer <peer_id> json`**: Returns Profitability Class (Zombie/Winner), Flow State (Source/Sink), and Volatility.

**Security Benefit:** This is a **Read-Only** method. It cannot change state. We can strictly validate inputs.

---

## 4. Security Hardening (Attack Surface Reduction)

### A. Input Sanitization
By funneling all peer modifications through `revenue-policy`, we can centralize validation:
*   Strict Pubkey format checking (`02...`/`03...`).
*   Enum validation for strategies (prevent arbitrary string injection).
*   Database transactions (Atomicity).

### B. Removal of Dangerous RPCs
We will **remove/hide** `revenue-set-fee`.
*   *Risk:* A manual fee set via RPC implies a "permanent" override, but the algorithm fights it.
*   *Fix:* Users must use `revenue-policy set <id> strategy=static fee_ppm=500`. This makes the override explicit in the database state, preventing the "Fee Drift" bug we debugged earlier.

---

## 5. Implementation Roadmap

### Step 1: Database Migration
*   Rename `ignored_peers` table to `peer_policies`.
*   Add columns: `strategy`, `min_fee_override`, `max_fee_override`, `tags`.
*   Migrate existing ignores to `strategy=passive`, `rebalance=disabled`.

### Step 2: Implement `revenue-policy`
*   Create `modules/policy_manager.py`.
*   Handle `set`, `get`, `list` logic.
*   Update `FeeController` to check `PolicyManager` instead of `ignored_peers`.

### Step 3: Implement `revenue-report`
*   Aggregate logic from `profitability_analyzer` and `database`.
*   Deprecate old reporting commands (add deprecation warnings to logs).

### Step 4: Update `cl-revenue-ops.py`
*   Register new commands.
*   Remove deprecated commands (or hide them).

This plan reduces your API surface area by **~60%** while adding the exact capabilities needed for Phase 9.