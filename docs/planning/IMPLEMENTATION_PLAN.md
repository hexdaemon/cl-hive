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

## Phase 0: Foundation (Pre-MVP) ✅

**Objective:** Establish plugin skeleton and database schema.

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

## Phase 1: Protocol Layer (MVP Core)

**Objective:** Implement BOLT 8 custom messaging for Hive communication.

### 1.1 Message Types
**File:** `modules/protocol.py`
**Range:** 32769 (Odd) to avoid conflicts.
**Magic Prefix:** `0x48495645` (ASCII "HIVE") - 4 bytes prepended to all messages.
**Tasks:**
- [ ] Define IntEnum for message types (HELLO, CHALLENGE, ATTEST, GOSSIP, INTENT, BAN).
- [ ] Implement serialization/deserialization.
- [ ] Implement Magic Byte Wrapping: All outgoing messages MUST be prefixed with `0x48495645`.

### 1.2 Handshake Protocol & Genesis
**File:** `modules/handshake.py`
**Tasks:**
- [ ] Implement `revenue-hive-genesis`: Initializes local DB as Admin, creates self-signed ticket.
- [ ] Implement `generate_invite_ticket` / `verify_ticket`.
- [ ] Implement `create_manifest` / `verify_manifest`.

### 1.3 Custom Message Hook
**Tasks:**
- [ ] Register `custommsg` hook in `cl-hive.py`.
- [ ] Implement Magic Byte Verification (Peek & Check): Read first 4 bytes, verify `0x48495645`. If mismatch, return `{"result": "continue"}` to pass message to other plugins.
- [ ] Dispatch to protocol handler (only after magic verification passes).

---

## Phase 2: State Management (Anti-Entropy)

**Objective:** Build the HiveMap and ensure consistency after network partitions.

### 2.1 HiveMap
**File:** `modules/state_manager.py`
**Tasks:**
- [ ] Implement `HivePeerState` dataclass.
- [ ] Implement fleet aggregation methods.

### 2.2 Gossip Protocol (Threshold & Sync)
**File:** `modules/gossip.py`
**Updates from Red Team:**
- [ ] **Threshold Gossiping:** Only broadcast if capacity changes >10% or fee policy changes.
- [ ] **Anti-Entropy:** On reconnection, peers exchange `State_Hash`. If mismatch, trigger `FULL_STATE_SYNC` to catch up on missed messages.

---

## Phase 3: Intent Lock Protocol

**Objective:** Implement deterministic conflict resolution for coordinated actions.

### 3.1 Intent Manager
**File:** `modules/intent_manager.py`
**Tasks:**
- [ ] Implement `HIVE_INTENT` message broadcasting (Announce).
- [ ] Implement Hold Period (60s Wait).
- [ ] Implement **Tie-Breaker:** Lowest lexicographic pubkey wins (Commit/Abort).

---

## Phase 4: Integration Bridge (Hardened)

**Objective:** Connect cl-hive decisions to cl-revenue-ops execution safely.

### 4.1 The "Paranoid" Bridge
**File:** `modules/bridge.py`
**Updates from Red Team:**
- [ ] **Circuit Breaker:** Wrap RPC calls to `cl-revenue-ops` with timeout/retry logic. If `cl-revenue-ops` hangs, `cl-hive` must not crash.
- [ ] **Feature Detection:** On startup, call `revenue-status` to verify `cl-revenue-ops` is installed and version >= 1.4.0.

### 4.2 Revenue-Ops Integration
**Tasks:**
- [ ] `set_hive_policy`: Calls `revenue-policy set <id> strategy=hive`.
- [ ] `trigger_rebalance`: Calls `revenue-rebalance` (relying on Strategic Exemption).

### 4.3 CLBoss Conflict Prevention (The Gateway Pattern)
**File:** `modules/clboss_bridge.py`
**Objective:** Prevent race conditions between plugins.
**Rules to Implement:**
1.  **Fees:** `cl-hive` NEVER calls `clboss-unmanage` for fees. It uses `revenue-policy`.
2.  **Topology:** `cl-hive` has exclusive rights to `clboss-ignore` (for blocking new channels).
3.  **Cleanup:** When releasing a peer, set `strategy=passive` rather than deleting policy.

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
