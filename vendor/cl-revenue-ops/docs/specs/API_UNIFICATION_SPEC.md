## `docs/specs/API_UNIFICATION_SPEC.md`

```markdown
# Developer Spec: API Unification & Policy Architecture

| Field | Value |
|-------|-------|
| **Target Version** | v1.4.0 |
| **Objective** | Consolidate RPC interface, implement Policy-Driven logic, prepare hooks for `cl-hive`. |
| **Status** | **Ready for Development** |

---

## 1. Architecture Overview

We are moving from an **Imperative** model ("Set fee to 500") to a **Declarative** model ("Apply 'Static' strategy to this peer").

Instead of exposing 13+ disjointed commands, we expose 5 high-level interfaces. The core logic (`FeeController`, `Rebalancer`) will no longer check ad-hoc lists like `ignored_peers`. Instead, they will query a centralized `PolicyManager` to determine behavior for a specific peer.

---

## 2. Database Schema

### 2.1 New Table: `peer_policies`
This replaces `ignored_peers`. It stores the governing rules for each channel/peer.

```sql
CREATE TABLE IF NOT EXISTS peer_policies (
    peer_id TEXT PRIMARY KEY,
    strategy TEXT NOT NULL DEFAULT 'dynamic',   -- dynamic, static, hive, passive
    rebalance_mode TEXT NOT NULL DEFAULT 'enabled', -- enabled, disabled, source_only, sink_only
    fee_ppm_target INTEGER,                     -- Used if strategy='static'
    tags TEXT,                                  -- JSON list of strings (e.g. ["hive", "toxic"])
    updated_at INTEGER NOT NULL
);
```

### 2.2 Migration Logic
On startup (`initialize`), check if `ignored_peers` exists.
1.  Read all rows from `ignored_peers`.
2.  Insert into `peer_policies`:
    *   `strategy` = `'passive'` (Do not touch fees).
    *   `rebalance_mode` = `'disabled'` (Do not rebalance).
    *   `tags` = `['migrated_ignore']`.
3.  Drop `ignored_peers` table (or rename to `_backup_ignored_peers`).

---

## 3. New Module: `modules/policy_manager.py`

Create a class `PolicyManager` to encapsulate all policy logic and input validation.

### Data Structures
```python
class FeeStrategy(Enum):
    DYNAMIC = "dynamic"   # Hill Climbing + Scarcity (Default)
    STATIC = "static"     # Fixed fee (User Override)
    HIVE = "hive"         # 0-Fee / Low Fee (Fleet Member)
    PASSIVE = "passive"   # Do nothing (allow CLBOSS/Manual control)

class RebalanceMode(Enum):
    ENABLED = "enabled"
    DISABLED = "disabled"
    SOURCE_ONLY = "source_only" # Can drain, cannot fill
    SINK_ONLY = "sink_only"     # Can fill, cannot drain
```

### Core Methods
*   `set_policy(peer_id, **kwargs)`: Validates inputs, updates DB.
*   `get_policy(peer_id)`: Returns policy dict. If no DB row exists, returns **Default Policy** (`dynamic`, `enabled`).
*   `add_tag(peer_id, tag)` / `remove_tag(peer_id, tag)`: Helper for list manipulation.

---

## 4. RPC Interface Definition

### 4.1 `revenue-policy` (The Control Plane)
**Usage:** `revenue-policy set <peer_id> [strategy=X] [fee_ppm=Y] [rebalance=Z]`

*   **Logic:**
    1.  Validate `peer_id` (Hex string, length 66).
    2.  Parse kwargs. Ensure enums match `FeeStrategy` / `RebalanceMode`.
    3.  Call `policy_manager.set_policy`.
    4.  **Critical:** If `strategy` changes to `static` or `hive`, trigger immediate fee update for that channel to apply the new rule.

### 4.2 `revenue-report` (The Data Plane)
**Usage:** `revenue-report [type] [target]`

*   `type=summary`: Returns aggregated P&L, Active Channel Count, Net Worth (Phase 8 logic).
*   `type=peer`: Returns specific metrics for a peer ID (Profitability Class, Flow State, Policy).
*   `type=hive`: Returns list of peers with `hive` strategy (Bridge for `cl-hive`).

### 4.3 `revenue-pump` (Manual Action)
*   Renaming of `revenue-rebalance`.
*   **Logic Change:** Must check `PolicyManager`.
    *   If `rebalance_mode == disabled`, reject request (unless `force=true` flag added).

### 4.4 `revenue-plan` (Strategic Output)
*   Consolidates `revenue-analyze` and `revenue-capacity-report`.
*   Output includes "Recommended Actions" (Close, Splice, Open) based on Profitability Analyzer.

---

## 5. Integration: Modifying Logic Cores

### 5.1 Update `FeeController`
Currently checks `database.is_peer_ignored(peer_id)`.
**New Logic:**
```python
policy = self.policy_manager.get_policy(peer_id)

if policy.strategy == FeeStrategy.PASSIVE:
    return # Skip
elif policy.strategy == FeeStrategy.STATIC:
    # Set fee to policy.fee_ppm_target
    # Enforce idempotency (don't spam if already set)
elif policy.strategy == FeeStrategy.HIVE:
    # Set fee to config.hive_fee_ppm (default 0 or 10)
    # Bypass Scarcity/Hill Climbing
elif policy.strategy == FeeStrategy.DYNAMIC:
    # Run existing Hill Climbing + Scarcity logic
```

### 5.2 Update `Rebalancer`
Currently checks `database.is_peer_ignored(peer_id)`.
**New Logic:**
```python
policy = self.policy_manager.get_policy(peer_id)

if policy.rebalance_mode == RebalanceMode.DISABLED:
    continue
if policy.rebalance_mode == RebalanceMode.SOURCE_ONLY and is_destination:
    continue
# ... etc
```

---

## 6. Deprecation Plan

In `cl-revenue-ops.py`:

1.  **Remove Registration** for: `revenue-ignore`, `revenue-unignore`, `revenue-list-ignored`, `revenue-remanage`.
2.  **Mapping:**
    *   `revenue-set-fee <id> <val>` -> Calls `revenue-policy set <id> strategy=static fee_ppm=<val>`.
    *   *Add Log Warning:* "Deprecated: Use revenue-policy instead."
3.  **Hiding:** Do not register deprecated commands in `init()` if possible, or mark them `hidden=True` if `pyln-client` supports it (otherwise just move them to bottom of help).

---

## 7. Security Hardening Checklist

- [ ] **Input Sanitization:** `PolicyManager` must reject non-hex peer IDs.
- [ ] **Enum Enforcement:** Reject arbitrary strings for strategies.
- [ ] **Read-Only Reporting:** `revenue-report` strictly reads DB; ensures no side effects.
- [ ] **Atomicity:** Policy updates use SQLite transactions.

---

## 8. Implementation Steps

1.  **Module:** Create `modules/policy_manager.py`.
2.  **Database:** Update `database.py` with schema and migration logic.
3.  **Refactor:** Update `fee_controller.py` to use PolicyManager.
4.  **Refactor:** Update `rebalancer.py` to use PolicyManager.
5.  **RPC:** Update `cl-revenue-ops.py` to expose new commands and map old ones.
6.  **Test:** Verify migration of existing ignored peers.

```