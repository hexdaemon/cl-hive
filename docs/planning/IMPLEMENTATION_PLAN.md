# cl-hive Implementation Plan

| Field | Value |
|-------|-------|
| **Version** | v0.1.0 (MVP) → v1.0.0 (Full Swarm) |
| **Base Dependency** | `cl-revenue-ops` v1.4.0+ |
| **Target Runtime** | Core Lightning Plugin (Python) |
| **Status** | **APPROVED FOR DEVELOPMENT** (Red Team Hardened) |

---

## Executive Summary

This document outlines the phased implementation plan for `cl-hive`, a distributed swarm intelligence layer for Lightning node fleets. The architecture leverages the existing `cl-revenue-ops` infrastructure (PolicyManager, Database, Config patterns) while adding BOLT 8 custom messaging for peer-to-peer coordination.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         cl-hive Plugin                          │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Protocol  │  │   State     │  │      Planner            │  │
│  │   Manager   │  │   Manager   │  │   (Topology Logic)      │  │
│  │  (BOLT 8)   │  │  (HiveMap)  │  │                         │  │
│  └──────┬──────┘  └──────┬──────┘  └───────────┬─────────────┘  │
│         │                │                     │                │
│         └────────────────┴─────────────────────┘                │
│                          │                                      │
│  ┌───────────────────────┴───────────────────────────────────┐  │
│  │             Integration Bridge (Paranoid)                  │  │
│  │   (Calls cl-revenue-ops PolicyManager & Rebalancer APIs)   │  │
│  └────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    cl-revenue-ops Plugin                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Policy    │  │  Rebalancer │  │    Fee Controller       │  │
│  │   Manager   │  │  (EV-Based) │  │   (Hill Climbing)       │  │
│  │  [HIVE]     │  │  [Exemption]│  │   [HIVE Fee: 0 PPM]     │  │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Phase 0: Foundation (Pre-MVP) ✅ AUDITED

**Objective:** Establish plugin skeleton and database schema.

**Audit Status:** ✅ **PASSED** (Red Team Review: 2026-01-05)
- Thread Safety: `RPC_LOCK`, `ThreadSafeRpcProxy`, `threading.local()` + WAL mode
- Graceful Shutdown: `shutdown_event` + `SIGTERM` handler
- Input Validation: `CONFIG_FIELD_TYPES` + `CONFIG_FIELD_RANGES`
- Dependency Isolation: RPC-based loose coupling with `cl-revenue-ops`

### 0.1 Plugin Skeleton
**File:** `cl-hive.py`
**Tasks:**
- [x] Create `cl-hive.py` with pyln-client plugin boilerplate
- [x] Create `modules/` directory structure
- [x] Add `requirements.txt` (pyln-client)
- [x] Implement thread-safe RPC proxy & graceful shutdown (copy from cl-revenue-ops)

### 0.2 Database Schema
**File:** `modules/database.py`
**Tables:** `hive_members`, `intent_locks`, `hive_state`, `contribution_ledger`, `hive_bans`
**Tasks:**
- [x] Implement schema initialization
- [x] Implement thread-local connection pattern

### 0.3 Configuration
**File:** `modules/config.py`
**Tasks:**
- [x] Create `HiveConfig` dataclass
- [x] Implement `ConfigSnapshot` pattern

---

## Phase 1: Protocol Layer (MVP Core) ✅ AUDITED

**Objective:** Implement BOLT 8 custom messaging and the cryptographic handshake.

**Audit Status:** ✅ **PASSED (With Commendation)** (Red Team Review: 2026-01-05)
- Magic Prefix Enforcement: Peek & Check pattern correctly implemented
- Crypto Safety: HSM-based `signmessage`/`checkmessage` - no keys in Python memory
- Ticket Integrity: 3-layer validation (Expiry + Signature + Admin Status)
- State Machine: HELLO→CHALLENGE→ATTEST→WELCOME flow correctly bound to session

### 1.1 Message Types
**File:** `modules/protocol.py`
**Range:** 32769 (Odd) to avoid conflicts.
**Magic Prefix:** `0x48495645` (ASCII "HIVE") - 4 bytes prepended to all messages.

**Tasks:**
- [x] Define IntEnum for MVP message types:
    - `HELLO` (32769)
    - `CHALLENGE` (32771)
    - `ATTEST` (32773)
    - `WELCOME` (32775)
    - *Deferred to Phase 2:* `GOSSIP`
    - *Deferred to Phase 3:* `INTENT`
    - *Deferred to Phase 5:* `VOUCH`, `BAN`, `PROMOTION`, `PROMOTION_REQUEST`
- [x] Implement `serialize(msg_type, payload) -> bytes` (JSON + Magic Prefix)
- [x] Implement `deserialize(bytes) -> (msg_type, payload)` with Magic check

### 1.2 Handshake Protocol & Crypto
**File:** `modules/handshake.py`
**Crypto Strategy:** Use CLN RPC `signmessage` and `checkmessage`. Do not import external crypto libs.

**Tasks:**
- [x] **Genesis:** Implement `hive-genesis` RPC.
    - Creates self-signed "Genesis Ticket" using `signmessage`.
    - Stores as Admin in DB.
- [x] **Ticket Logic:** 
    - `generate_invite_ticket(params)`: Returns base64 encoded JSON + Sig.
    - `verify_ticket(ticket)`: Validates Sig against Admin Pubkey.
- [x] **Manifest Logic:**
    - `create_manifest(nonce)`: JSON of capabilities + `signmessage(nonce)`.
    - `verify_manifest(manifest)`: Validates `checkmessage(sig, nonce)`.
- [x] **Active Probe:** (Optional/Post-MVP) Deferred - rely on signature verification.

### 1.3 Custom Message Hook
**File:** `cl-hive.py`

**Tasks:**
- [x] Register `custommsg` hook.
- [x] **Security:** Implement "Peek & Check". Read first 4 bytes. If `!= HIVE_MAGIC`, return `continue` immediately.
- [x] Dispatch to protocol handlers (HELLO, CHALLENGE, ATTEST, WELCOME).
- [x] Implement `hive-invite` and `hive-join` RPC commands.

### 1.4 Phase 1 Testing
**File:** `tests/test_protocol.py`

**Tasks:**
- [x] **Magic Byte Test:** Verify non-HIVE messages are ignored.
- [x] **Round Trip Test:** Serialize -> Deserialize preserves data.
- [ ] **Crypto Test:** Verify `signmessage` output from one node verifies on another. (Requires integration test)
- [x] **Expiry Test:** Verify tickets are rejected after `valid_hours`.

---

## Phase 2: State Management (Anti-Entropy) ✅ IMPLEMENTED

**Objective:** Build the HiveMap and ensure consistency after network partitions using Gossip and Anti-Entropy.

**Implementation Status:** ✅ **COMPLETE** (Awaiting Red Team Audit)

### 2.1 HiveMap & State Hashing
**File:** `modules/state_manager.py`

**State Hash Algorithm:** 
To ensure deterministic comparison, the State Hash is calculated as:
`SHA256( SortedJSON( [ {peer_id, version, timestamp}, ... ] ) )`
*   Only essential metadata is hashed to detect drift.
*   List must be sorted by `peer_id`.

**Tasks:**
- [x] Implement `HivePeerState` dataclass.
- [x] Implement `update_peer_state(peer_id, gossip_data)`: Updates local DB if gossip version > local version.
- [x] Implement `calculate_fleet_hash()`: Computes the global checksum of the local Hive view.
- [x] Implement `get_missing_peers(remote_hash)`: Identifies divergence (naive full sync for MVP).
- [x] Database Integration: Persist state to `hive_state` table.

### 2.2 Gossip Protocol (Thresholds)
**File:** `modules/gossip.py`

**Threshold Rules:**
1.  **Capacity:** Change > 10% from last broadcast.
2.  **Fee:** Any change in `fee_policy`.
3.  **Status:** Ban/Unban events.
4.  **Heartbeat:** Force broadcast every `heartbeat_interval` (300s) if no other updates.

**Tasks:**
- [x] Implement `should_broadcast(old_state, new_state)` logic.
- [x] Implement `create_gossip_payload()`: Bundles local state for transmission.
- [x] Implement `process_gossip(payload)`: Validates and passes to StateManager.

### 2.3 Protocol Integration (cl-hive.py)
**Context:** Wire up the message types defined in Phase 1 to the logic in Phase 2.

**New Handlers:**
1.  `HIVE_GOSSIP` (32777): Passive state update.
2.  `HIVE_STATE_HASH` (32779): Active Anti-Entropy check (sent on reconnection).
3.  `HIVE_FULL_SYNC` (32781): Response to hash mismatch.

**Tasks:**
- [x] Register new message handlers in `on_custommsg`.
- [x] Implement `handle_gossip`: Update StateManager.
- [x] Implement `handle_state_hash`: Compare local vs remote hash. If mismatch -> Send `FULL_SYNC`.
- [x] Implement `handle_full_sync`: Bulk update StateManager.
- [x] Hook `peer_connected` event: Trigger `send_state_hash` on connection.

### 2.4 Phase 2 Testing
**File:** `tests/test_state.py`

**Tasks:**
- [x] **Determinism Test:** Verify `calculate_fleet_hash` produces identical hashes for identical (but scrambled) inputs.
- [x] **Threshold Test:** Verify 9% capacity change returns `False` for broadcast, 11% returns `True`.
- [x] **Anti-Entropy Test:** Simulate two nodes with divergent state; verify `FULL_SYNC` restores consistency.
- [x] **Persistence Test:** Verify state survives plugin restart via SQLite.

---

## Phase 3: Intent Lock Protocol ✅ AUDITED

**Objective:** Implement deterministic conflict resolution for coordinated actions to prevent "Thundering Herd" race conditions.

**Audit Status:** ✅ **PASSED (With Commendation)** (Red Team Review: 2026-01-05)
- Deterministic Tie-Breaker: Lowest lexicographical pubkey wins - both nodes reach same conclusion independently
- State Consistency: Monitor loop checks status='pending' AND timestamp <= cutoff
- Message Handling: Correct passive-aggressive protocol design

### 3.1 Intent Manager Logic
**File:** `modules/intent_manager.py`

**Supported Intent Types:**
1.  `channel_open`: Opening a channel to an external peer.
2.  `rebalance`: Large circular rebalance affecting fleet liquidity.
3.  `ban_peer`: Proposing a ban (requires consensus).

**Tasks:**
- [x] Implement `Intent` dataclass (type, target, initiator, timestamp).
- [x] Implement `announce_intent(type, target)`:
    - Insert into `intent_locks` table (status='pending').
    - Broadcast `HIVE_INTENT` message.
- [x] Implement `handle_conflict(remote_intent)`:
    - Query DB for local pending intents matching target.
    - If conflict found: Execute **Tie-Breaker** (Lowest Lexicographical Pubkey wins).
    - If we lose: Update DB status to 'aborted', broadcast `HIVE_INTENT_ABORT`, return False.
    - If we win: Log conflict, keep waiting.

### 3.2 Protocol Integration (Messaging)
**Context:** Wire up the intent message flow in `cl-hive.py`.

**New Handlers:**
1.  `HIVE_INTENT` (32783): Remote node requesting a lock.
2.  `HIVE_INTENT_ABORT` (32787): Remote node yielding the lock.

**Tasks:**
- [x] Register handlers in `on_custommsg`.
- [x] `handle_intent`:
    - Record remote intent in DB (for visibility).
    - Check for local conflicts via `intent_manager.check_conflicts`.
    - If conflict & we win: Do nothing (let them abort).
    - If conflict & we lose: Call `intent_manager.abort_local()`.
- [x] `handle_intent_abort`:
    - Update remote intent status in DB to 'aborted'.

### 3.3 Timer Management (The Commit Loop)
**Context:** We need a background task to finalize locks after the hold period.

**Tasks:**
- [x] Add `intent_monitor_loop` to `cl-hive.py` threads.
- [x] Logic (Run every 5s):
    - Query DB for `status='pending'` intents where `now > timestamp + hold_seconds`.
    - If no abort signal received/generated:
        - Update status to 'committed'.
        - Trigger the actual action (e.g., call `bridge.open_channel`).
    - Clean up expired/stale intents (> 1 hour).

### 3.4 Phase 3 Testing
**File:** `tests/test_intent.py`

**Tasks:**
- [x] **Tie-Breaker Test:** Verify `min(pubkey_A, pubkey_B)` logic allows the correct node to proceed 100% of the time.
- [x] **Race Condition Test:** Simulate receiving a conflicting `HIVE_INTENT` 1 second before local timer expires. Verify local abort.
- [x] **Silence Test:** Verify commit executes if no conflict messages are received during hold period.
- [x] **Cleanup Test:** Verify DB does not grow indefinitely with old locks.

---

## Phase 4: Integration Bridge (Hardened)

**Objective:** Connect cl-hive decisions to external plugins (`cl-revenue-ops`, `clboss`) with "Paranoid" error handling.

### 4.1 The "Paranoid" Bridge (Circuit Breaker)
**File:** `modules/bridge.py`

**Circuit Breaker Logic:**
To prevent cascading failures if a dependency hangs or crashes.
*   **States:** `CLOSED` (Normal), `OPEN` (Fail Fast), `HALF_OPEN` (Probe).
*   **Thresholds:**
    *   `MAX_FAILURES`: 3 consecutive RPC errors.
    *   `RESET_TIMEOUT`: 60 seconds (time to wait before probing).
    *   `RPC_TIMEOUT`: 5 seconds (strict timeout for calls).

**Tasks:**
- [x] Implement `CircuitBreaker` class.
- [x] Implement `feature_detection()` on startup:
    *   Call `plugin.rpc.plugin("list")`.
    *   Verify `cl-revenue-ops` is `active`.
    *   Verify version >= 1.4.0 via `revenue-status`.
    *   If failed: Set status to `DISABLED`, log warning, skip all future calls.
- [x] Implement generic `safe_call(method, payload)` wrapper:
    *   Checks Circuit Breaker state.
    *   Wraps RPC in try/except.
    *   Updates failure counters on `RpcError` or `Timeout`.

### 4.2 Revenue-Ops Integration
**File:** `modules/bridge.py`

**Methods:**
- [x] `set_hive_policy(peer_id, is_member: bool)`:
    *   **Member:** `revenue-policy set <id> strategy=hive rebalance=enabled`.
    *   **Non-Member:** `revenue-policy set <id> strategy=dynamic` (Revert to default).
    *   *Validation:* Check result `{"status": "success"}`.
- [x] `trigger_rebalance(target_peer, amount_sats)`:
    *   Call: `revenue-rebalance from=auto to=<target> amount=<sats>`.
    *   *Note:* Relies on `cl-revenue-ops` v1.4 "Strategic Exemption" to bypass profitability checks for Hive peers.

### 4.3 CLBoss Conflict Prevention (The Gateway Pattern)
**File:** `modules/clboss_bridge.py`

**Constraint:** `cl-hive` manages **Topology** (New Channels). `cl-revenue-ops` manages **Fees/Balancing** (Existing Channels).

**Tasks:**
- [x] `detect_clboss()`: Check if `clboss` plugin is registered.
- [x] `ignore_peer(peer_id)`:
    *   Call `clboss-ignore <peer_id>`.
    *   *Purpose:* Prevent CLBoss from opening redundant channels to saturated targets.
- [x] `unignore_peer(peer_id)`:
    *   Call `clboss-unignore <peer_id>` (if command exists/supported).
    *   *Note:* Do **NOT** call `clboss-manage` or `clboss-unmanage` (fee tags). Leave that to `cl-revenue-ops`.

### 4.4 Phase 4 Testing
**File:** `tests/test_bridge.py`

**Tasks:**
- [x] **Circuit Breaker Test:** Simulate 3 RPC failures -> Verify 4th call raises immediate "Circuit Open" exception without network IO.
- [x] **Recovery Test:** Simulate time passing -> Verify Circuit moves to HALF_OPEN -> Success closes it.
- [x] **Version Mismatch:** Mock `revenue-status` returning v1.3.0 -> Verify Bridge disables itself.
- [x] **Method Signature:** Verify `set_hive_policy` constructs the exact JSON expected by `revenue-policy`.

---

## Phase 5: Governance & Membership

**Objective:** Implement the two-tier membership system (Neophyte/Member) and the algorithmic promotion protocol.

**Implemented artifacts:**
*   New modules: `modules/membership.py`, `modules/contribution.py`
*   New DB tables: `promotion_vouches`, `promotion_requests`, `peer_presence`, `leech_flags`
*   New config flags: `membership_enabled`, `auto_vouch_enabled`, `auto_promote_enabled`, `ban_autotrigger_enabled`
*   New background job: membership maintenance (prune vouches/contributions/presence)

### 5.1 Membership Tiers
**File:** `modules/membership.py`

**Tier Definitions:**
| Tier | Fees | Rebalancing | Data Access | Governance |
|------|------|-------------|-------------|------------|
| **Neophyte** | Discounted (50% of public) | Pull Only | Read-Only | None |
| **Member** | Zero (0 PPM) or Floor (10 PPM) | Push & Pull | Read-Write | Voting Power |

**Database Schema Update:**
*   Add `tier` column to `hive_members` table: `ENUM('neophyte', 'member')`.
*   Add `joined_at` timestamp for probation tracking.

**Tasks:**
- [x] Implement `MembershipTier` enum.
- [x] Implement `get_tier(peer_id)` -> Returns current tier.
- [x] Implement `set_tier(peer_id, tier)` -> Updates DB + triggers Bridge policy update.
- [x] Implement `is_probation_complete(peer_id)` -> `joined_at + 30 days < now`.

### 5.2 The Value-Add Equation (Promotion Criteria)
**File:** `modules/membership.py`

**Promotion Requirements (ALL must be satisfied):**
1.  **Reliability:** Uptime > 99.5% over 30-day probation.
    *   *Metric:* `(seconds_online / total_seconds) * 100`.
    *   *Source:* Track via `peer_connected`/`peer_disconnected` events.
2.  **Contribution Ratio:** Ratio >= 1.0.
    *   *Formula:* `sats_forwarded_for_hive / sats_received_from_hive`.
    *   *Interpretation:* Neophyte must route MORE for the fleet than they consume.
3.  **Topological Uniqueness:** Connects to >= 1 peer the Hive doesn't already have.
    *   *Check:* `neophyte_peers - union(all_member_peers) != empty`.

**Tasks:**
- [x] Implement `calculate_uptime(peer_id)` -> float (0.0 to 100.0).
- [x] Implement `calculate_contribution_ratio(peer_id)` -> float.
- [x] Implement `get_unique_peers(peer_id)` -> list of pubkeys.
- [x] Implement `evaluate_promotion(peer_id)` -> `{eligible: bool, reasons: []}`.

### 5.3 Promotion Protocol (Consensus Vouching)
**File:** `modules/membership.py`

**Message Flow:**
1.  Neophyte calls `hive-request-promotion` RPC.
2.  Plugin broadcasts `HIVE_PROMOTION_REQUEST` (32795) to all Members.
3.  Each Member runs `evaluate_promotion()` locally.
4.  If passed: Member broadcasts `HIVE_VOUCH` (32789) with signature.
5.  Neophyte collects vouches. When threshold met: broadcasts `HIVE_PROMOTION` (32793).
6.  All nodes update local DB tier to 'member'.

**Consensus Threshold:**
*   **Quorum:** `max(3, ceil(active_members * 0.51))`.
*   *Example:* 5 members → need 3 vouches. 10 members → need 6 vouches.

**Tasks:**
- [x] Implement `request_promotion()` -> Broadcasts request.
- [x] Implement `handle_promotion_request(peer_id)` -> Auto-evaluate and vouch if passed.
- [x] Implement `handle_vouch(vouch)` -> Collect and count.
- [x] Implement `handle_promotion(proof)` -> Validate vouches, update tier.
- [x] Implement `calculate_quorum()` -> int.

### 5.4 Contribution Tracking
**File:** `modules/contribution.py`

**Tracking Logic:**
*   Hook `forward_event` notification.
*   For each forward, check if `in_channel` or `out_channel` belongs to a Hive member.
*   Update `contribution_ledger` table.

**Ledger Schema:**
```sql
CREATE TABLE contribution_ledger (
    id INTEGER PRIMARY KEY,
    peer_id TEXT NOT NULL,
    direction TEXT NOT NULL,  -- 'forwarded' or 'received'
    amount_sats INTEGER NOT NULL,
    timestamp INTEGER NOT NULL
);
```

**Anti-Leech Throttling:**
*   If `Ratio < 0.5` for a Member: Signal Bridge to reduce push rebalancing priority.
*   If `Ratio < 0.4` for 7 consecutive days: Auto-trigger `HIVE_BAN` proposal (guarded by config).

**Tasks:**
- [x] Register `forward_event` subscription.
- [x] Implement `record_forward(in_peer, out_peer, amount)`.
- [x] Implement `get_contribution_stats(peer_id)` -> `{forwarded, received, ratio}`.
- [x] Implement `check_leech_status(peer_id)` -> `{is_leech: bool, ratio: float}`.

### 5.5 Phase 5 Testing
**File:** `tests/test_membership.py`

**Tasks:**
- [x] **Uptime Test:** Simulate 30 days with 99.6% uptime -> eligible. 99.4% -> rejected.
- [x] **Ratio Test:** Forward 100k, receive 90k -> ratio 1.11 -> eligible. Forward 80k, receive 100k -> ratio 0.8 -> rejected.
- [x] **Uniqueness Test:** Neophyte with peer not in Hive -> unique. All peers overlap -> not unique.
- [x] **Quorum Test:** 5 members, 3 vouches -> promoted. 2 vouches -> not promoted.
- [x] **Leech Test:** Ratio 0.4 for 7 days -> ban proposal triggered.

---

## Phase 6: Hive Planner (Topology Optimization) ✅ IMPLEMENTED

**Objective:** Implement the "Gardner" algorithm for fleet-wide graph optimization.

### 6.1 Saturation Analysis
**File:** `modules/planner.py`

**Saturation Metric:**
*   `Hive_Share(target) = sum(hive_capacity_to_target) / total_network_capacity_to_target`.
*   **Threshold:** 20% (from PHASE9_3 spec).

**Data Sources:**
*   Local channels: `listpeerchannels`.
*   Gossip state: `HiveMap` from Phase 2.
*   Network capacity: Estimate from `listchannels` (cached, updated hourly).

**Tasks:**
- [x] Implement `calculate_hive_share(target_pubkey)` -> float (0.0 to 1.0).
- [x] Implement `get_saturated_targets()` -> list of pubkeys where share > 0.20.
- [x] Implement `get_underserved_targets()` -> list of high-value peers with share < 0.05.

### 6.2 Anti-Overlap (The Guard)
**File:** `modules/planner.py`

**Logic:**
*   For each saturated target: Issue `clboss-ignore` to all fleet nodes EXCEPT those already connected.
*   Prevents capital duplication on already-covered targets.

**Tasks:**
- [x] Implement `enforce_saturation_limits()`:
    *   Get saturated targets.
    *   For each: Broadcast `HIVE_IGNORE_TARGET` (internal, not a wire message).
    *   Call `clboss_bridge.ignore_peer()` for each.
- [x] Implement `release_saturation_limits()`:
    *   If share drops below 15%, call `clboss_bridge.unignore_peer()`.

### 6.3 Expansion (Capital Allocation)
**File:** `modules/planner.py`

**Logic:**
*   Identify underserved targets (high-value, low Hive coverage).
*   Select the node with the most idle on-chain funds.
*   Trigger Intent Lock for `channel_open`.

**Node Selection Criteria:**
1.  `onchain_balance > min_channel_size * 2` (safety margin).
2.  `pending_intents == 0` (not already busy).
3.  `uptime > 99%` (reliable).

**Tasks:**
- [x] Implement `get_idle_capital()` -> dict `{peer_id: onchain_sats}`.
- [x] Implement `select_opener(target_pubkey)` -> peer_id or None.
- [x] Implement `propose_expansion(target_pubkey)`:
    *   Select opener.
    *   Call `intent_manager.announce_intent('channel_open', target)`.

### 6.4 Planner Schedule
**File:** `cl-hive.py`

**Execution:**
*   Run `planner_loop` every **3600 seconds** (1 hour).
*   On each run:
    1.  Refresh network capacity cache.
    2.  Calculate saturation for top 100 targets.
    3.  Enforce/release ignore rules.
    4.  Propose up to 1 expansion per cycle (rate limit).

**Tasks:**
- [x] Add `planner_loop` to background threads.
- [x] Implement rate limiting: max 1 `channel_open` intent per hour.
- [x] Log all planner decisions to `hive_planner_log` table.

### 6.5 Phase 6 Testing
**File:** `tests/test_planner.py`

**Tasks:**
- [x] **Saturation Test:** Mock Hive with 25% share to target X -> verify `clboss-ignore` called.
- [x] **Release Test:** Share drops to 14% -> verify `clboss-unignore` called.
- [x] **Expansion Test:** Underserved target + idle node -> verify Intent announced.
- [x] **Rate Limit Test:** 2 expansions in 1 hour -> verify second is queued, not executed.

---

## Phase 7: Governance Modes

**Objective:** Implement the configurable Decision Engine for action execution.

### 7.1 Mode Definitions
**File:** `modules/governance.py`

**Modes:**
| Mode | Behavior | Use Case |
|------|----------|----------|
| `ADVISOR` | Log + Notify, no execution | Cautious operators, learning phase |
| `AUTONOMOUS` | Execute within safety limits | Trusted fleet, hands-off operation |
| `ORACLE` | Delegate to external API | AI/ML integration, quant strategies |

**Configuration:**
*   `governance_mode`: enum in `HiveConfig`.
*   Runtime switchable via `hive-set-mode` RPC.

### 7.2 ADVISOR Mode (Human in the Loop)
**File:** `modules/governance.py`

**Flow:**
1.  Planner/Intent proposes action.
2.  Action saved to `pending_actions` table with `status='pending'`.
3.  Notification sent (webhook or log).
4.  Operator reviews via `hive-pending` RPC.
5.  Operator approves via `hive-approve <action_id>` or rejects via `hive-reject <action_id>`.

**Pending Actions Schema:**
```sql
CREATE TABLE pending_actions (
    id INTEGER PRIMARY KEY,
    action_type TEXT NOT NULL,  -- 'channel_open', 'rebalance', 'ban'
    target TEXT NOT NULL,
    proposed_by TEXT NOT NULL,
    proposed_at INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',  -- 'pending', 'approved', 'rejected', 'expired'
    expires_at INTEGER NOT NULL
);
```

**Tasks:**
- [ ] Implement `propose_action(action_type, target)` -> Saves to DB, sends notification.
- [ ] Implement `get_pending_actions()` -> list.
- [ ] Implement `approve_action(action_id)` -> Execute + update status.
- [ ] Implement `reject_action(action_id)` -> Update status only.
- [ ] Implement expiry: Actions older than 24h auto-expire.

### 7.3 AUTONOMOUS Mode (Algorithmic Execution)
**File:** `modules/governance.py`

**Safety Constraints:**
*   **Budget Cap:** Max `budget_per_day` sats for channel opens (default: 10M sats).
*   **Rate Limit:** Max `actions_per_hour` (default: 2).
*   **Confidence Threshold:** Only execute if `evaluate_promotion().confidence > 0.8`.

**Tasks:**
- [ ] Implement `check_budget(amount)` -> bool (within daily limit).
- [ ] Implement `check_rate_limit()` -> bool (within hourly limit).
- [ ] Implement `execute_if_safe(action)` -> Runs all checks, executes or rejects.
- [ ] Track daily spend in memory, reset at midnight UTC.

### 7.4 ORACLE Mode (External API)
**File:** `modules/governance.py`

**Flow:**
1.  Planner proposes action.
2.  Build `DecisionPacket` JSON.
3.  POST to configured `oracle_url` with timeout (5s).
4.  Parse response: `{"decision": "APPROVE"}` or `{"decision": "DENY", "reason": "..."}`.
5.  Execute or reject based on response.

**DecisionPacket Schema:**
```json
{
    "action_type": "channel_open",
    "target": "02abc...",
    "context": {
        "hive_share": 0.12,
        "target_capacity": 50000000,
        "opener_balance": 10000000
    },
    "timestamp": 1736100000
}
```

**Fallback:** If API unreachable or timeout, fall back to `ADVISOR` mode.

**Tasks:**
- [ ] Implement `query_oracle(decision_packet)` -> `{"decision": str, "reason": str}`.
- [ ] Implement timeout + retry (1 retry after 2s).
- [ ] Implement fallback to ADVISOR on failure.
- [ ] Log all oracle queries and responses.

### 7.5 Phase 7 Testing
**File:** `tests/test_governance.py`

**Tasks:**
- [ ] **Advisor Test:** Propose action -> verify saved to DB, not executed.
- [ ] **Approve Test:** Approve pending action -> verify executed.
- [ ] **Budget Test:** Exceed daily budget -> verify action rejected.
- [ ] **Rate Limit Test:** 3 actions in 1 hour (limit=2) -> verify 3rd rejected.
- [ ] **Oracle Test:** Mock API returns APPROVE -> verify executed. Returns DENY -> verify rejected.
- [ ] **Oracle Timeout Test:** API hangs -> verify fallback to ADVISOR.

---

## Phase 8: RPC Commands

**Objective:** Expose Hive functionality via CLI with consistent interface.

### 8.1 Core Commands
**File:** `cl-hive.py`

| Command | Parameters | Returns | Description |
|---------|------------|---------|-------------|
| `hive-genesis` | `--force` (optional) | `{hive_id, admin_pubkey}` | Initialize as Hive admin |
| `hive-invite` | `--valid-hours=24` | `{ticket: base64}` | Generate invite ticket |
| `hive-join` | `ticket=<base64>` | `{status, hive_id}` | Join Hive with ticket |
| `hive-status` | *(none)* | `{hive_id, tier, members, mode}` | Current Hive status |
| `hive-members` | `--tier=<filter>` | `[{pubkey, tier, uptime, ratio}]` | List members |

### 8.2 Governance Commands
**File:** `cl-hive.py`

| Command | Parameters | Returns | Description |
|---------|------------|---------|-------------|
| `hive-pending` | *(none)* | `[{id, type, target, proposed_at}]` | List pending actions |
| `hive-approve` | `action_id=<int>` | `{status, result}` | Approve pending action |
| `hive-reject` | `action_id=<int>` | `{status}` | Reject pending action |
| `hive-set-mode` | `mode=<advisor\|autonomous\|oracle>` | `{old_mode, new_mode}` | Change governance mode |

### 8.3 Membership Commands
**File:** `cl-hive.py`

| Command | Parameters | Returns | Description |
|---------|------------|---------|-------------|
| `hive-request-promotion` | *(none)* | `{status, vouches_needed}` | Request promotion to Member |
| `hive-vouch` | `peer_id=<pubkey>` | `{status}` | Manually vouch for a Neophyte |
| `hive-ban` | `peer_id=<pubkey>`, `reason=<str>` | `{status, intent_id}` | Propose ban (starts Intent) |
| `hive-contribution` | `peer_id=<pubkey>` (optional) | `{forwarded, received, ratio}` | View contribution stats |

### 8.4 Topology Commands
**File:** `cl-hive.py`

| Command | Parameters | Returns | Description |
|---------|------------|---------|-------------|
| `hive-topology` | *(none)* | `{saturated: [], underserved: []}` | View topology analysis |
| `hive-planner-log` | `--limit=10` | `[{timestamp, action, target, result}]` | View planner history |

### 8.5 Permission Model
**File:** `cl-hive.py`

**Rules:**
*   **Admin Only:** `hive-genesis`, `hive-invite`, `hive-ban`, `hive-set-mode`.
*   **Member Only:** `hive-vouch`, `hive-approve`, `hive-reject`.
*   **Any Tier:** `hive-status`, `hive-members`, `hive-contribution`, `hive-topology`.
*   **Neophyte Only:** `hive-request-promotion`.

**Implementation:**
*   Check `get_tier(local_pubkey)` before executing.
*   Return `{"error": "permission_denied", "required_tier": "member"}` if unauthorized.

### 8.6 Phase 8 Testing
**File:** `tests/test_rpc.py`

**Tasks:**
- [ ] **Genesis Test:** Call `hive-genesis` -> verify DB initialized, returns hive_id.
- [ ] **Invite/Join Test:** Generate ticket on A, join on B -> verify B in members list.
- [ ] **Status Test:** Verify all fields returned with correct types.
- [ ] **Permission Test:** Neophyte calls `hive-ban` -> verify permission denied.
- [ ] **Approve Flow:** Create pending action, approve -> verify executed.

---

## Testing Strategy

### Unit Tests
- Message serialization/deserialization.
- Intent conflict resolution (deterministic comparison).
- Contribution ratio logic.

### Integration Tests
- **Genesis Flow:** Start Node A -> Generate Ticket -> Join Node B.
- **Conflict:** Force simultaneous Intent from A and B -> Verify only one executes.
- **Failover:** Kill `cl-revenue-ops` on Node A -> Verify `cl-hive` logs error but stays up.

---

## Next Steps

1.  **Immediate:** Create plugin skeleton (Phase 0).
2.  **Week 1:** Complete Protocol Layer + Genesis (Phase 1).
3.  **Week 2:** Complete State + Anti-Entropy (Phase 2).

---
*Plan Updated: January 5, 2026*
