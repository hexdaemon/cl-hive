# cl-hive Full Plugin Audit — 2026-02-10

**Auditor:** Claude Opus 4.6 (7 parallel audit agents)
**Scope:** All 39 modules, 3 tools, MCP server, 1,432 tests
**Codebase:** commit `2a47949` (main)

---

## Executive Summary

cl-hive demonstrates strong security fundamentals: parameterized SQL throughout, HSM-delegated crypto, consistent identity binding, bounded caches, and rate limiting on all message types. No critical vulnerabilities were found. The main areas needing attention are:

- **2 HIGH thread safety bugs** — unprotected shared dicts that can crash under concurrent access
- **Unbounded data growth** — 8+ database tables and 2 in-memory structures lack cleanup
- **Settlement auto-execution** — moves real funds without human approval gate
- **Missing test coverage** — 6 modules untested, key new features (rejection reason, expansion pause) not tested

**Finding Totals:** 0 Critical, 9 High, 28 Medium, 40+ Low, 30+ Info/Positive

---

## Critical & High Severity Findings

### H-1. `routing_intelligence._path_stats` has no lock protection
- **File:** `modules/routing_intelligence.py:107`
- **Severity:** HIGH (thread safety)
- **Description:** `_path_stats` dict is read/written from message handler threads (`process_route_probe`), RPC handlers (`get_best_routes`, `get_stats`), and the fee_intelligence_loop (`cleanup_stale_data`) with no lock. Concurrent dict mutation during iteration will raise `RuntimeError` and crash the loop.
- **Fix:** Add `threading.Lock()` to `HiveRoutingMap.__init__` and acquire in all methods touching `_path_stats`.

### H-2. Direct write to `state_manager._local_state` without lock
- **File:** `cl-hive.py:13491`
- **Severity:** HIGH (thread safety)
- **Description:** `hive-set-version` RPC directly assigns `state_manager._local_state[our_pubkey] = new_state` bypassing `state_manager._lock`. Background loops iterating this dict will crash with `RuntimeError: dictionary changed size during iteration`.
- **Fix:** Use `StateManager` public API or acquire `state_manager._lock`.

### H-3. `pending_actions` table has no indexes
- **File:** `modules/database.py:388-398`
- **Severity:** HIGH (performance)
- **Description:** Planner queries filter on `status`, `proposed_at`, `action_type`, and `payload LIKE '%target%'` — all full table scans. This table grows with every proposal/rejection cycle.
- **Fix:** Add `CREATE INDEX idx_pending_actions_status ON pending_actions(status, proposed_at)` and `CREATE INDEX idx_pending_actions_type ON pending_actions(action_type, proposed_at)`.

### H-4. `peer_events` prune function defined but never called
- **File:** `modules/database.py` — `prune_peer_events()` at line 2972
- **Severity:** HIGH (data growth)
- **Description:** 180+ days of peer events accumulate without pruning. Function exists but is never wired into any maintenance loop.
- **Fix:** Call `prune_peer_events()` from `membership_maintenance_loop`.

### H-5. `budget_tracking` table has no cleanup
- **File:** `modules/database.py:484-493`
- **Severity:** HIGH (data growth)
- **Description:** One row per budget expenditure per day. No prune function exists. Grows unboundedly over months/years.
- **Fix:** Add and wire a `prune_budget_tracking(days=90)` function.

### H-6. `advisor_db.cleanup_old_data()` is never called
- **File:** `tools/advisor_db.py:912-940`
- **Severity:** HIGH (data growth)
- **Description:** `channel_history`, `fleet_snapshots` (with full JSON blobs), `alert_history`, and `action_outcomes` grow without bound. Hourly snapshots with 100KB+ reports will reach gigabytes within months.
- **Fix:** Call `cleanup_old_data()` from the advisor cycle or a scheduled task.

### H-7. Settlement auto-execution without human approval
- **File:** `tools/proactive_advisor.py:556-562`
- **Severity:** HIGH (fund safety)
- **Description:** `_check_weekly_settlement` calls `settlement_execute` with `dry_run=False` automatically. BOLT12 payments are irreversible. Only guards are day-of-week (Mon-Wed) and once-per-period.
- **Fix:** Queue settlement execution as a `pending_action` requiring AI/human approval instead of auto-executing.

### H-8. `prune_old_settlement_data()` runs without transaction
- **File:** `modules/database.py:5963-6009`
- **Severity:** HIGH (data integrity)
- **Description:** Performs 4 sequential DELETEs (proposals → executions → votes → proposals) in autocommit mode. Crash mid-sequence leaves orphaned rows.
- **Fix:** Wrap in `self.transaction()`.

### H-9. N+1 query pattern in `sync_uptime_from_presence()`
- **File:** `modules/database.py:1939-1998`
- **Severity:** HIGH (performance)
- **Description:** For each member: SELECT presence, then UPDATE member. O(2N+1) queries. With 50 members = 101 queries per maintenance cycle.
- **Fix:** Use a single JOIN-based UPDATE.

---

## Medium Severity Findings

### Thread Safety (3)

| ID | File | Line | Description |
|----|------|------|-------------|
| M-1 | `cl-hive.py` | 13465,13494 | `gossip_mgr._last_broadcast_state.version` accessed without lock in `hive-set-version` |
| M-2 | `modules/contribution.py` | 93-119 | `_channel_map` and `_last_refresh` not lock-protected; concurrent map rebuild + iteration race |
| M-3 | `modules/liquidity_coordinator.py` | 184-214 | `_need_rate` and `_snapshot_rate` dicts modified without lock |

### Protocol (3)

| ID | File | Line | Description |
|----|------|------|-------------|
| M-4 | `cl-hive.py` | 3446,3496,3513 | `serialize()` returns `None` on overflow; callers call `.hex()` on None → `AttributeError` instead of clean error |
| M-5 | `cl-hive.py` | 4521-4536 | Settlement gaming ban uses reversed voting — non-participation = approval. Exploitable during low fleet activity |
| M-6 | `modules/membership.py` | 367-381 | Active member window (24h) can shrink quorum to dangerously low levels in larger hives |

### Database (8)

| ID | File | Line | Description |
|----|------|------|-------------|
| M-7 | `modules/database.py` | 279-296 | `ban_proposals` table missing indexes on `target_peer_id` and `status` |
| M-8 | `modules/database.py` | 483-493 | `budget_tracking` missing composite index for `GROUP BY action_type` queries |
| M-9 | `modules/database.py` | 298-306,1042-1068 | Missing FK constraints: `ban_votes→ban_proposals`, `settlement_ready_votes→settlement_proposals`, `settlement_executions→settlement_proposals`. Orphan risk on partial deletes |
| M-10 | `modules/database.py` | 131-1189 | All migrations/table creations run without wrapping transaction. Crash mid-init = partial schema |
| M-11 | `modules/database.py` | 1889-1921 | `update_presence()` has TOCTOU race: concurrent INSERT attempts on same peer_id, no `ON CONFLICT` |
| M-12 | `modules/database.py` | 2482-2519 | `log_planner_action()` ring-buffer: concurrent COUNT + DELETE + INSERT without transaction can double-prune |
| M-13 | `modules/database.py` | 84 | `PRAGMA foreign_keys=ON` set but zero FK constraints defined. Inert and misleading |
| M-14 | `modules/database.py` | 82 | No WAL checkpoint scheduled. `-wal` file can grow large between SQLite auto-checkpoints |

### Resource Management (4)

| ID | File | Line | Description |
|----|------|------|-------------|
| M-15 | `modules/routing_intelligence.py` | 107 | `_path_stats` entries and `PathStats.reporters` sets grow unboundedly between hourly cleanups |
| M-16 | `cl-hive.py` | 8497-8502 | Intent committed to DB but execute failure leaves intent stuck in `committed` state with no recovery |
| M-17 | Multiple | N/A | ~150 `except Exception: pass/continue` clauses silently swallow errors. Most are defensive around `sendcustommsg` (acceptable), but some mask genuine bugs in settlement and protocol parsing |
| M-18 | `cl-hive.py` | 249 | RPC calls have no timeout on the call itself (only 10s on lock acquisition). Stuck CLN RPC blocks all threads |

### Tools & MCP (7)

| ID | File | Line | Description |
|----|------|------|-------------|
| M-19 | `mcp-hive-server.py` | 3627-3648 | No authentication/authorization on MCP tool calls. Transport-level security only |
| M-20 | `mcp-hive-server.py` | 286-330 | Docker command arguments from RPC params passed to `lightning-cli` without sanitization (mitigated by `create_subprocess_exec`) |
| M-21 | `mcp-hive-server.py` | 4438-5132 | Destructive tools (`hive_approve_action`, `hive_splice`, `revenue_set_fee`, `revenue_rebalance`) have no confirmation gate |
| M-22 | `mcp-hive-server.py` | 90,228-238 | `HIVE_ALLOW_INSECURE_TLS=true` disables cert verification globally; rune sent over unverified connection |
| M-23 | `tools/external_peer_intel.py` | 399-401 | 1ML API TLS verification unconditionally disabled (`CERT_NONE`). MITM can inject false reputation data |
| M-24 | `tools/proactive_advisor.py` | 126-129,966-974 | After 200 outcomes at 95%+ success, auto-execute threshold drops to 0.55 confidence |
| M-25 | `tools/hive-monitor.py` | 173,200 | `FleetMonitor.alerts` list grows without bound in daemon mode |

### Security (1)

| ID | File | Line | Description |
|----|------|------|-------------|
| M-26 | `modules/rpc_commands.py` | 2879 | `create_close_actions()` creates `pending_actions` entries without `check_permission()` call |

---

## Low Severity Findings (Summary)

| Category | Count | Key Items |
|----------|-------|-----------|
| Input validation | 3 | VPN port parsing ValueError; no peer_id format validation on read-only RPCs; planner_log limit type not checked |
| Thread safety | 5 | Bridge rate-limiter TOCTOU; function attribute mutation; config snapshot not atomic; cooperative_expansion cooldown dicts unlocked; state_manager cached hash torn read |
| Protocol | 4 | Documented message type range stale (32845 vs actual 32881); remote intent 24h acceptance window vs 1h cleanup; outbox retry success/failure branches identical; relay path entries not validated for pubkey format |
| Database | 12 | 8 unbounded query patterns missing LIMIT; redundant `conn.commit()` in autocommit mode (9 instances); delegation_attempts/task_requests cleanup never called; contribution_rate_limits cleanup never called |
| Resource mgmt | 6 | Bridge init `time.sleep()`; fee_coordination closed-channel orphans; gossip `_peer_gossip_times` partial cleanup; thread-local SQLite connections never explicitly closed; error logs lack stack traces |
| Tools | 6 | No MCP rate limiting; rune in memory; error messages leak paths; hardcoded 100sat rebalance fee estimate; advisor_db query params unbounded; bump_version no validation |
| Identity | 2 | FEE_INTELLIGENCE_SNAPSHOT handler identity binding not explicit; challenge nonce not bound to expected peer |

---

## Test Coverage Gaps

### Modules with NO test file
| Module | Risk |
|--------|------|
| `quality_scorer.py` | Medium — influences membership decisions |
| `task_manager.py` | Medium — background task coordination |
| `splice_coordinator.py` | Medium — high-level splice coordination |
| `clboss_bridge.py` | Low — optional integration |
| `config.py` | Medium — hot-reload behavior untested |
| `rpc_commands.py` | **High** — handler functions never tested directly (only DB layer) |

### Critical untested paths
1. `reject_action()` with `reason` parameter — new feature, zero tests
2. `_reject_all_actions()` with `reason` — zero tests
3. `update_action_status()` with `reason` — parameter not verified stored/retrievable
4. Expansion pause at `MAX_CONSECUTIVE_REJECTIONS` threshold — not functionally tested
5. Database migrations — zero migration tests across entire suite
6. `fees_earned_sats` in learning engine measurement — new feature, zero tests
7. Budget enforcement under concurrent access — no concurrent hold stress test
8. Several `test_feerate_gate.py` test classes have empty `pass` bodies

---

## Positive Findings

The audit confirmed many strong practices:

1. **Zero SQL injection risk** — all queries use parameterized `?` placeholders. Dynamic column names filtered through whitelist sets
2. **HSM-delegated crypto** — no external crypto libraries, all signatures via CLN `signmessage`/`checkmessage`
3. **Strong identity binding** — cryptographic signature verification on all state-changing messages with pubkey match
4. **Consistent shutdown** — all 8 background loops use `shutdown_event.wait()`, all threads are daemon, zero `time.sleep()` in loops
5. **Bounded caches** — `MAX_REMOTE_INTENTS=200`, `MAX_PENDING_CHALLENGES=1000`, `MAX_SEEN_MESSAGES=50000`, `MAX_TRACKED_PEERS=1000`, `MAX_POLICY_CACHE=500` all with LRU eviction
6. **Fund safety layers** — governance modes, budget holds, daily caps, rate limits, per-channel max percentages
7. **Protocol validation** — comprehensive schema validation on every message type with string length caps, numeric bounds, pubkey format checks
8. **DoS protection** — per-type rate limits, per-peer throttling, message size enforcement at serialize and deserialize
9. **Fail-closed** — invalid input consistently rejected with no state changes
10. **Config snapshot pattern** — frozen dataclass prevents mid-cycle mutation

---

## Recommended Fix Priority

### Immediate (next deploy)
1. **H-1** Add lock to `routing_intelligence._path_stats` — prevents crash
2. **H-2** Fix `hive-set-version` state_manager access — prevents crash
3. **H-7** Gate settlement auto-execution behind pending_action approval
4. **H-3** Add indexes on `pending_actions` — improves planner performance

### Short-term (this week)
5. **H-4,H-5,H-6** Wire up uncalled cleanup functions: `prune_peer_events()`, add `prune_budget_tracking()`, call `advisor_db.cleanup_old_data()`
6. **H-8** Wrap `prune_old_settlement_data()` in transaction
7. **M-4** Guard `serialize()` None return before `.hex()` calls
8. **M-16** Add intent recovery for stuck `committed` state
9. **M-23** Fix 1ML TLS bypass or make it opt-in

### Medium-term (this month)
10. **M-2,M-3** Add lock protection to contribution `_channel_map` and liquidity rate dicts
11. **M-11,M-12** Add `ON CONFLICT` to `update_presence()`, wrap `log_planner_action()` in transaction
12. **H-9** Rewrite `sync_uptime_from_presence()` as single JOIN-based UPDATE
13. Write tests for `reject_action` with reason, expansion pause cap, fees_earned_sats measurement
14. Add dedicated test files for `rpc_commands.py`, `quality_scorer.py`, `task_manager.py`

### Low-priority (backlog)
15. Add FK constraints or remove misleading `PRAGMA foreign_keys=ON`
16. Schedule periodic WAL checkpoint
17. Add LIMIT clauses to 8 unbounded queries
18. Remove 9 no-op `conn.commit()` calls in autocommit mode
19. Add stack traces to top-level loop error logs
