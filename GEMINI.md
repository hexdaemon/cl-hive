# GEMINI.md: Command & Control Manifest

## 🎯 Mission Briefing
Transition `cl-hive` from a group of independent nodes into a **deterministic, coordinated swarm**. 
**Core Goal:** Maximize fleet-wide liquidity efficiency through the "Gardner" Planner without introducing fund risk, deadlocks, or state poisoning.

---

## 🛡️ The Red Team Manifesto
*Think like an attacker. Every peer is hostile until proven otherwise.*

### Non-Negotiables (The Law of the Hive)
1.  **Identity Binding:** `sender_peer_id` MUST always match the claimed identity in the payload. No impersonation.
2.  **Unbounded Input Protection:** Every cache, list, and database table MUST have a hard cap. No OOM or Disk Exhaustion.
3.  **Fail-Closed Bias:** If a dependency (Bridge, DB, RPC) is missing or malformed, **do nothing and log**. 
4.  **No Silent Fund Actions:** Funds never move unless `governance_mode=autonomous`. All proposals MUST be auditable via `pending_actions` or `hive_planner_log`.
5.  **Anti-Stall:** RPC calls MUST use the `ThreadSafeRpcProxy` with a 10s timeout. Never block the main message-handling thread.

---

## 🚦 Phase Coordination Dashboard

| Phase | Status | Objective |
| :--- | :--- | :--- |
| **Phase 1-5** | ✅ PASS | Core Protocol, State, Intent, Bridge, and Membership. |
| **Phase 6** | ✅ PASS | Topology Optimization (Planner logic & Background loop). |
| **Phase 7** | 🏗️ IN PROGRESS | **Governance Decision Engine** (Ticket 7-01, 7-02). |
| **Phase 8** | 🏗️ IN PROGRESS | **Management RPCs** (Ticket 8-01, 8-02). |
| **Phase 9** | 📅 PLANNED | **Maintenance & Pruning** (Ticket 9-01). |

---

## 🧩 Architectural Patterns for Agents

### 1. The Thread-Safe RPC Pattern
All CLN RPC calls MUST wrap through `safe_plugin.rpc` or `safe_plugin.safe_call`.
```python
# GOOD: Thread-safe with 10s timeout
info = self.plugin.rpc.getinfo()

# BAD: Direct access to underlying RPC (race condition risk)
info = self.plugin._plugin.rpc.getinfo()
```

### 2. The Configuration Snapshot
Background loops MUST capture an immutable snapshot at the start of a cycle to prevent "torn reads" if the user updates config mid-cycle.
```python
cfg = config.snapshot()
# Use cfg.market_share_cap_pct, NOT config.market_share_cap_pct
```

### 3. Peek & Check Messaging
The `on_custommsg` hook MUST implement fast-rejection based on the 4-byte `HIVE_MAGIC` prefix before attempting heavy JSON deserialization.

---

## 🛠️ Tooling & Validation
- **Audit Tool:** `gh issue list --state open` to see current security bottlenecks.
- **Testing:** Always run `python3 -m pytest tests/test_security.py` after protocol changes.
- **Documentation:** Synchronize `IMPLEMENTATION_PLAN.md` every time a Ticket is closed.

---
*Manifest established: January 8, 2026*
