# Phase 6 Implementation Plan: Topology Optimization (The Planner)

## Overview
Phase 6 introduces the "Planner" (Gardner) algorithm to optimize the fleet's topology. It actively manages channel saturation to prevent capital overlap and proposes new expansions to underserved targets.

## Team Assignments (Tickets)

### Ticket 1: Database & Schema (Agent A)
**Scope:** Add persistence for Planner decisions.
**Files:** `modules/database.py`
**Tasks:**
1.  Add `hive_planner_log` table to `initialize()`.
    *   Schema: `id, timestamp, action_type, target, result, details`
2.  Add `log_planner_action(...)` method.
3.  Add `get_planner_logs(limit)` method.
**Constraints:** Use existing thread-local pattern.

### Ticket 2: Planner Core - Saturation & Guard (Agent B)
**Scope:** Implement the analysis engine and "Anti-Overlap" guard.
**Files:** `modules/planner.py`
**Tasks:**
1.  Create `Planner` class initialized with `state_manager`, `bridge`, `config`, `plugin`.
2.  Implement `calculate_hive_share(target)`:
    *   Sum capacity of all hive members to target.
    *   Divide by total public capacity to target (from `listchannels`).
3.  Implement `get_saturated_targets()`: Return peers where share > `market_share_cap` (20%).
4.  Implement `enforce_saturation_limits()`:
    *   Call `clboss_bridge.ignore_peer()` for saturated targets on *other* nodes (logic check required: we only ignore if WE don't have a channel? Or do we ignore to prevent *further* channels?).
    *   Ref logic: "Issue clboss-ignore to all fleet nodes EXCEPT those already connected."
5.  Implement `release_saturation_limits()`: Un-ignore if share drops < 15%.

### Ticket 3: Planner Expansion - Capital Allocation (Agent C)
**Scope:** Implement the expansion logic to propose new channels.
**Files:** `modules/planner.py`
**Tasks:**
1.  Implement `get_underserved_targets()`: High value, low Hive share.
2.  Implement `get_idle_capital()`: Find members with `onchain_balance > min_channel_size`.
3.  Implement `propose_expansion()`:
    *   Select best opener (idle capital, high uptime, no pending intents).
    *   If *we* are the selected opener:
        *   Call `intent_mgr.announce_intent('channel_open', target)`.
        *   (Actually, `intent_mgr` needs a way to broadcast. We might need to expose a helper or return the intent for the main loop to broadcast).
    *   If *remote* is selected: Do nothing (they will calculate it themselves? Or do we coordinate? Phase 6 logic usually implies each node runs the planner independently and comes to the same conclusion, OR one node acts. The "Intent Lock" resolves conflicts if multiple try. Let's assume independent execution).

### Ticket 4: Integration - The Loop (Agent D)
**Scope:** Wire the Planner into the main plugin loop.
**Files:** `cl-hive.py`
**Tasks:**
1.  Initialize `Planner` in `init()`.
2.  Implement `planner_loop()` background thread.
    *   Run every hour (3600s).
    *   Jitter execution to avoid network spikes.
    *   Call `planner.run_cycle()`.
3.  Register `hive-topology` and `hive-planner-log` RPC commands.

### Ticket 5: Quality Assurance (Agent E)
**Scope:** Verify the Planner logic.
**Files:** `tests/test_planner.py`
**Tasks:**
1.  Test `calculate_hive_share` with mocked topology.
2.  Test `enforce_saturation_limits` calls `clboss-ignore`.
3.  Test `propose_expansion` respects rate limits and pending intents.

## Risk Register
1.  **Risk:** Runaway Ignoring.
    *   *Mitigation:* `enforce_saturation_limits` must be idempotent and log heavily. Limit number of ignores per cycle.
2.  **Risk:** False Positive Saturation.
    *   *Mitigation:* Ensure `calculate_hive_share` handles missing public data gracefully (fail open/allow expansion).
3.  **Risk:** Intent Storms.
    *   *Mitigation:* One expansion per cycle (1 hour). Intent Lock prevents simultaneous opens.

## Gate Checklist
*   [x] `hive_planner_log` table exists.
*   [x] Planner loop running with `shutdown_event`.
*   [x] Rate limit: Max 1 intent/hour.
*   [x] Tests pass.
