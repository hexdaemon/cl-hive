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
    - *Deferred to Phase 5:* `VOUCH`, `BAN`, `PROMOTION`
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

## Phase 2: State Management (Anti-Entropy)

**Objective:** Build the HiveMap and ensure consistency after network partitions using Gossip and Anti-Entropy.

### 2.1 HiveMap & State Hashing
**File:** `modules/state_manager.py`

**State Hash Algorithm:** 
To ensure deterministic comparison, the State Hash is calculated as:
`SHA256( SortedJSON( [ {peer_id, version, timestamp}, ... ] ) )`
*   Only essential metadata is hashed to detect drift.
*   List must be sorted by `peer_id`.

**Tasks:**
- [ ] Implement `HivePeerState` dataclass.
- [ ] Implement `update_peer_state(peer_id, gossip_data)`: Updates local DB if gossip version > local version.
- [ ] Implement `calculate_fleet_hash()`: Computes the global checksum of the local Hive view.
- [ ] Implement `get_missing_peers(remote_hash)`: Identifies divergence (naive full sync for MVP).
- [ ] Database Integration: Persist state to `hive_state` table.

### 2.2 Gossip Protocol (Thresholds)
**File:** `modules/gossip.py`

**Threshold Rules:**
1.  **Capacity:** Change > 10% from last broadcast.
2.  **Fee:** Any change in `fee_policy`.
3.  **Status:** Ban/Unban events.
4.  **Heartbeat:** Force broadcast every `heartbeat_interval` (300s) if no other updates.

**Tasks:**
- [ ] Implement `should_broadcast(old_state, new_state)` logic.
- [ ] Implement `create_gossip_payload()`: Bundles local state for transmission.
- [ ] Implement `process_gossip(payload)`: Validates and passes to StateManager.

### 2.3 Protocol Integration (cl-hive.py)
**Context:** Wire up the message types defined in Phase 1 to the logic in Phase 2.

**New Handlers:**
1.  `HIVE_GOSSIP` (32777): Passive state update.
2.  `HIVE_STATE_HASH` (32779): Active Anti-Entropy check (sent on reconnection).
3.  `HIVE_FULL_SYNC` (32781): Response to hash mismatch.

**Tasks:**
- [ ] Register new message handlers in `on_custommsg`.
- [ ] Implement `handle_gossip`: Update StateManager.
- [ ] Implement `handle_state_hash`: Compare local vs remote hash. If mismatch -> Send `FULL_SYNC`.
- [ ] Implement `handle_full_sync`: Bulk update StateManager.
- [ ] Hook `peer_connected` event: Trigger `send_state_hash` on connection.

### 2.4 Phase 2 Testing
**File:** `tests/test_state.py`

**Tasks:**
- [ ] **Determinism Test:** Verify `calculate_fleet_hash` produces identical hashes for identical (but scrambled) inputs.
- [ ] **Threshold Test:** Verify 9% capacity change returns `False` for broadcast, 11% returns `True`.
- [ ] **Anti-Entropy Test:** Simulate two nodes with divergent state; verify `FULL_SYNC` restores consistency.
- [ ] **Persistence Test:** Verify state survives plugin restart via SQLite.

---

## Phase 3: Intent Lock Protocol

**Objective:** Implement deterministic conflict resolution for coordinated actions to prevent "Thundering Herd" race conditions.

### 3.1 Intent Manager Logic
**File:** `modules/intent_manager.py`

**Supported Intent Types:**
1.  `channel_open`: Opening a channel to an external peer.
2.  `rebalance`: Large circular rebalance affecting fleet liquidity.
3.  `ban_peer`: Proposing a ban (requires consensus).

**Tasks:**
- [ ] Implement `Intent` dataclass (type, target, initiator, timestamp).
- [ ] Implement `announce_intent(type, target)`:
    - Insert into `intent_locks` table (status='pending').
    - Broadcast `HIVE_INTENT` message.
- [ ] Implement `handle_conflict(remote_intent)`:
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
- [ ] Register handlers in `on_custommsg`.
- [ ] `handle_intent`:
    - Record remote intent in DB (for visibility).
    - Check for local conflicts via `intent_manager.check_conflicts`.
    - If conflict & we win: Do nothing (let them abort).
    - If conflict & we lose: Call `intent_manager.abort_local()`.
- [ ] `handle_intent_abort`:
    - Update remote intent status in DB to 'aborted'.

### 3.3 Timer Management (The Commit Loop)
**Context:** We need a background task to finalize locks after the hold period.

**Tasks:**
- [ ] Add `intent_monitor_loop` to `cl-hive.py` threads.
- [ ] Logic (Run every 5s):
    - Query DB for `status='pending'` intents where `now > timestamp + hold_seconds`.
    - If no abort signal received/generated:
        - Update status to 'committed'.
        - Trigger the actual action (e.g., call `bridge.open_channel`).
    - Clean up expired/stale intents (> 1 hour).

### 3.4 Phase 3 Testing
**File:** `tests/test_intent.py`

**Tasks:**
- [ ] **Tie-Breaker Test:** Verify `min(pubkey_A, pubkey_B)` logic allows the correct node to proceed 100% of the time.
- [ ] **Race Condition Test:** Simulate receiving a conflicting `HIVE_INTENT` 1 second before local timer expires. Verify local abort.
- [ ] **Silence Test:** Verify commit executes if no conflict messages are received during hold period.
- [ ] **Cleanup Test:** Verify DB does not grow indefinitely with old locks.

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
- [ ] Implement `CircuitBreaker` class.
- [ ] Implement `feature_detection()` on startup:
    *   Call `plugin.rpc.plugin("list")`.
    *   Verify `cl-revenue-ops` is `active`.
    *   Verify version >= 1.4.0 via `revenue-status`.
    *   If failed: Set status to `DISABLED`, log warning, skip all future calls.
- [ ] Implement generic `safe_call(method, payload)` wrapper:
    *   Checks Circuit Breaker state.
    *   Wraps RPC in try/except.
    *   Updates failure counters on `RpcError` or `Timeout`.

### 4.2 Revenue-Ops Integration
**File:** `modules/bridge.py`

**Methods:**
- [ ] `set_hive_policy(peer_id, is_member: bool)`:
    *   **Member:** `revenue-policy set <id> strategy=hive rebalance=enabled`.
    *   **Non-Member:** `revenue-policy set <id> strategy=dynamic` (Revert to default).
    *   *Validation:* Check result `{"status": "success"}`.
- [ ] `trigger_rebalance(target_peer, amount_sats)`:
    *   Call: `revenue-rebalance from=auto to=<target> amount=<sats>`.
    *   *Note:* Relies on `cl-revenue-ops` v1.4 "Strategic Exemption" to bypass profitability checks for Hive peers.

### 4.3 CLBoss Conflict Prevention (The Gateway Pattern)
**File:** `modules/clboss_bridge.py`

**Constraint:** `cl-hive` manages **Topology** (New Channels). `cl-revenue-ops` manages **Fees/Balancing** (Existing Channels).

**Tasks:**
- [ ] `detect_clboss()`: Check if `clboss` plugin is registered.
- [ ] `ignore_peer(peer_id)`:
    *   Call `clboss-ignore <peer_id>`.
    *   *Purpose:* Prevent CLBoss from opening redundant channels to saturated targets.
- [ ] `unignore_peer(peer_id)`:
    *   Call `clboss-unignore <peer_id>` (if command exists/supported).
    *   *Note:* Do **NOT** call `clboss-manage` or `clboss-unmanage` (fee tags). Leave that to `cl-revenue-ops`.

### 4.4 Phase 4 Testing
**File:** `tests/test_bridge.py`

**Tasks:**
- [ ] **Circuit Breaker Test:** Simulate 3 RPC failures -> Verify 4th call raises immediate "Circuit Open" exception without network IO.
- [ ] **Recovery Test:** Simulate time passing -> Verify Circuit moves to HALF_OPEN -> Success closes it.
- [ ] **Version Mismatch:** Mock `revenue-status` returning v1.3.0 -> Verify Bridge disables itself.
- [ ] **Method Signature:** Verify `set_hive_policy` constructs the exact JSON expected by `revenue-policy`.

---

## Phase 5: Governance & Membership

**Objective:** Implement the two-tier system and promotion protocol.

### 5.1 Membership Manager
**File:** `modules/membership.py`
**Tasks:**
- [ ] Implement Value-Add Equation (Reliability + Contribution + Topology).
- [ ] Implement `HIVE_PROMOTION_REQUEST` and consensus vouching (51%).

### 5.2 Contribution Tracking
**File:** `modules/contribution.py`
**Tasks:**
- [ ] Hook into `forward_event`.
- [ ] Calculate `Ratio = Forwarded / Received`.
- [ ] Implement throttling signal for Bridge if Ratio < 0.5.

---

## Phase 6: Hive Planner (Topology Optimization)

**Objective:** Implement the "Gardner" algorithm.

### 6.1 Planner
**File:** `modules/planner.py`
**Tasks:**
- [ ] **Saturation Analysis:** Calculate fleet-wide capacity per target.
- [ ] **Anti-Overlap:** Issue `clboss-ignore` via `ClbossBridge` if saturation met.
- [ ] **Expansion:** Assign channel open task to node with most idle on-chain funds.

---

## Phase 7: Governance Modes

**Objective:** Implement the Decision Engine.

### 7.1 Decision Engine
**File:** `modules/governance.py`
**Tasks:**
- [ ] Implement modes: `ADVISOR` (Notify), `AUTONOMOUS` (Execute), `ORACLE` (API).
- [ ] Implement `pending_actions` table for Advisor mode.

---

## Phase 8: RPC Commands

**Objective:** Expose Hive functionality via CLI.

### 8.1 Command List
**File:** `cl-hive.py`
**Tasks:**
- [ ] Implement: `hive-status`, `hive-members`, `hive-invite`, `hive-join`, `hive-genesis`.
- [ ] Implement: `hive-ban`, `hive-vouch`.
- [ ] Implement: `hive-topology`.

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
