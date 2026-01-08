# CL-HIVE RED TEAM SECURITY AUDIT (V2)

**Date:** 2026-01-08
**Auditor:** Red Team AI
**Scope:** Phases 1-6 (Protocol, State, Intent, Bridge, Membership, Planner)

---

## EXECUTIVE RISK SUMMARY (TOP 5)

| Rank | Finding | Severity | Component | Risk |
|------|---------|----------|-----------|------|
| **1** | **FULL_SYNC STATE POISONING** | **HIGH** | `cl-hive.py`, `state_manager.py` | `handle_full_sync` allows non-members to push 2000 state entries, polluting the DB and HiveMap. |
| **2** | **UNBOUNDED TICKET DESERIALIZATION** | **HIGH** | `handshake.py` | `Ticket.from_base64` decodes arbitrary length input; attacker can crash node with multi-MB payload. |
| **3** | **CIRCUIT BREAKER FLAPPING** | **MEDIUM** | `bridge.py` | Circuit resets to CLOSED after a *single* success in HALF_OPEN, allowing rapid flapping against a failing dependency. |
| **4** | **HANDSHAKE CHALLENGE EVICTION** | **MEDIUM** | `handshake.py` | LRU eviction for pending challenges allows an attacker to flood 1000 HELLOs and evict legitimate candidates. |
| **5** | **GOVERNANCE STATE INCONSISTENCY** | **MEDIUM** | `cl-hive.py` | Intents are marked `COMMITTED` in DB *before* checking governance mode, causing "phantom" commits that never execute. |

---

## FINDINGS TABLE

| ID | Severity | Component | Vulnerability | Exploit Path | Impact |
|----|----------|-----------|---------------|--------------|--------|
| **P1-04** | **High** | `handshake.py:82` | No size limit in `Ticket.from_base64` | Send 50MB string in `ticket` field of HELLO | Memory exhaustion / Crash |
| **P2-01** | **High** | `cl-hive.py:1268` | `handle_full_sync` lacks member check | Non-member sends `FULL_SYNC` with 2000 valid-looking states | State cache pollution, DB bloat |
| **P4-01** | **Medium** | `bridge.py:117` | Circuit breaker resets too easily | Wait `RESET_TIMEOUT`, succeed once, full reset | Bypass of protection, log spam |
| **P1-03** | **Medium** | `handshake.py:440` | LRU eviction for pending challenges | Flood 1001 HELLO messages | DoS of legitimate joiners |
| **P3-03** | **Medium** | `cl-hive.py:1570` | Commit status set before governance check | Win intent lock -> Commit DB -> Blocked by Governance | Intent stuck in `COMMITTED` but not done |
| **P2-02** | **Low** | `gossip.py:96` | `_peer_gossip_times` never pruned | Gossip to unique peer IDs continuously | Slow memory leak |

---

## EXPLOIT SKETCHES

### Exploit 1: TICKET BOMB (P1-04)
**Target:** `handshake.py`
**Attack:**
1. Attacker connects to victim node.
2. Attacker generates a 50MB string of random characters, base64 encoded.
3. Attacker sends `HIVE_HELLO` message with this string as the `ticket`.
4. `handle_hello` calls `verify_ticket` -> `Ticket.from_base64`.
5. `base64.b64decode` attempts to decode 50MB in memory, followed by `json.loads`.
**Result:** Python process OOMs or hangs significantly.

### Exploit 2: FULL SYNC POISONING (P2-01)
**Target:** `cl-hive.py` / `state_manager.py`
**Attack:**
1. Attacker (non-member) connects.
2. Attacker sends `HIVE_FULL_SYNC` message.
3. Payload contains 2000 state entries with random valid-looking `peer_id`s.
4. `handle_full_sync` DOES NOT check `database.get_member(sender)`.
5. `gossip_mgr` passes it to `state_manager`.
6. `state_manager` validates types but not membership, inserting 2000 rows into `hive_state`.
**Result:** `hive_state` table fills with junk; fleet hash becomes garbage; legitimate syncs fail or are slow.

### Exploit 3: GOVERNANCE PHANTOM COMMIT (P3-03)
**Target:** `cl-hive.py` (Intent Monitor)
**Attack:**
1. Node is in `governance-mode=advisor`.
2. `IntentManager` wins a lock for `channel_open`.
3. `process_ready_intents` runs.
4. Calls `intent_mgr.commit_intent(id)` -> DB updates to `STATUS_COMMITTED`.
5. Checks `config.governance_mode` -> it is `advisor`.
6. Code `continue`s (skips execution).
**Result:** The intent is permanently `COMMITTED` in the DB but the channel was never opened. The UI/CLI will report it as committed.

---

## MITIGATION GUIDANCE

### Fix P1-04: Ticket Size Limit
In `modules/handshake.py`:
```python
MAX_TICKET_SIZE = 10240  # 10KB

@classmethod
def from_base64(cls, encoded: str) -> 'Ticket':
    if len(encoded) > MAX_TICKET_SIZE:
        raise ValueError("Ticket too large")
    # ... existing decode ...
```

### Fix P2-01: Member Check in Full Sync
In `cl-hive.py`, `handle_full_sync`:
```python
def handle_full_sync(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    if not gossip_mgr:
        return {"result": "continue"}
        
    # ADDED: Verify sender is member
    member = database.get_member(peer_id)
    if not member:
        plugin.log(f"Ignoring FULL_SYNC from non-member {peer_id[:16]}...", level='warn')
        return {"result": "continue"}
        
    updated = gossip_mgr.process_full_sync(peer_id, payload)
    # ...
```

### Fix P4-01: Circuit Breaker Hysteresis
In `modules/bridge.py`:
```python
def record_success(self) -> None:
    self._last_success_time = int(time.time())
    
    if self._state == CircuitState.HALF_OPEN:
        self._half_open_successes += 1
        # Require 3 consecutive successes to close
        if self._half_open_successes >= 3:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._half_open_successes = 0
    else:
        self._failure_count = max(0, self._failure_count - 1)
```

### Fix P1-03: Challenge Rate Limiting
In `modules/handshake.py`, implement a per-peer leaky bucket or strict rate limit before creating a challenge entry.

---

## TESTS REQUIRED

1.  **Unit Test (Ticket Size):** Call `Ticket.from_base64` with 20KB string, assert `ValueError`.
2.  **Integration Test (Full Sync):** Connect as non-member, send `FULL_SYNC`. Assert DB `hive_state` table count does not increase.
3.  **Unit Test (Circuit Breaker):**
    - Set state to `HALF_OPEN`.
    - Call `record_success()` once. Assert state is still `HALF_OPEN`.
    - Call `record_success()` 2 more times. Assert state is `CLOSED`.
4.  **Integration Test (Governance):**
    - Set `governance-mode=advisor`.
    - Create and expire an intent.
    - Run `process_ready_intents`.
    - Assert intent status in DB is `pending` or `proposed`, NOT `committed` (requires logic change to not commit if mode!=autonomous).
