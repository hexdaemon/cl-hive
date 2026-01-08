# Phase 5 Review Template (Governance & Membership)

Use this template to audit Phase 5 in the same **PASS / PARTIAL / FAIL** style as phases 1–4.  
Phase 5 is **PASS** only when every mandatory item below is satisfied and tests pass.

---

## Phase 5 findings
**Status:** ☐ PASS ☐ PARTIAL ☐ FAIL

**What I verified:**
- `modules/protocol.py`
- `cl-hive.py`
- `modules/membership.py` (new)
- `modules/contribution.py` (new or expanded)
- `modules/database.py`
- `modules/state_manager.py` (for uniqueness inputs)
- `tests/test_membership.py` (new)
- `docs/planning/IMPLEMENTATION_PLAN.md` Phase 5 section

---

## Security findings (must check)

### 5.1 Protocol completeness and validation
**Must be true:**
- ☐ `HiveMessageType` includes **`PROMOTION_REQUEST = 32795`**
- ☐ Payload schemas are enforced for:
  - PROMOTION_REQUEST (`target_pubkey`, `request_id`, `timestamp`)
  - VOUCH (`target_pubkey`, `request_id`, `timestamp`, `voucher_pubkey`, `sig`)
  - PROMOTION (`target_pubkey`, `request_id`, `vouches[]`)
  - BAN/VOTE messages (if Phase 5 touches them)
- ☐ **Hard size caps** exist and are enforced:
  - `MAX_VOUCHES_IN_PROMOTION` (e.g., 50)
  - `MAX_REQUEST_ID_LEN` (e.g., 64)
  - Message size limit (confirm Phase 1 inbound cap still applies)
- ☐ All inbound handlers are fail-safe: malformed payloads are ignored; plugin does not crash

**FAIL conditions:**
- Missing PROMOTION_REQUEST type
- Any handler trusts arbitrary dicts/lists without type checks
- Promotion messages can include unbounded lists/dicts

---

### 5.2 Identity binding and signature correctness
**Must be true:**
- ☐ `voucher_pubkey` must equal the **actual sender peer_id** (prevents spoofing)
- ☐ Signature verification uses a **canonical string** (exact format documented and stable)
- ☐ Signatures are verified with CLN `checkmessage` (no external crypto; no key material in Python)
- ☐ Timestamp and request_id are included in the signed content (prevents replay across requests)
- ☐ VOUCHes expire (`VOUCH_TTL_SECONDS`) and expired vouches are rejected

**FAIL conditions:**
- Voucher identity not bound to sender
- Signature doesn’t cover request_id or timestamp
- Non-canonical/ambiguous serialization

---

### 5.3 Membership rules and enforcement
**Must be true:**
- ☐ Only **members** can emit VOUCH and PROMOTION (and BAN/VOTE if applicable)
- ☐ Neophytes can emit PROMOTION_REQUEST but cannot vouch/ban
- ☐ Promotion requires quorum: `max(3, ceil(active_members * 0.51))`
- ☐ “Active member” definition is explicit and bounded (e.g., last_seen ≤ 24h, not banned)
- ☐ Promotion is idempotent:
  - `(target_pubkey, request_id)` accepted once
  - Repeats ignored safely

**FAIL conditions:**
- Neophytes can vouch/promote others
- Quorum can be satisfied by duplicate vouchers
- Promotion can be replayed to re-trigger side effects

---

### 5.4 Contribution tracking (anti-leech) safety
**Must be true:**
- ☐ Contribution ledger writes are **bounded** (rate limited, size limited, windowed)
- ☐ Ledger is pruned (e.g., keep 30–45 days max)
- ☐ Contribution ratio calculation uses a fixed window and tolerates missing data
- ☐ “Leech” detection does not auto-ban unless explicitly enabled; default is **log-only** or **proposal-only**
- ☐ No untrusted input can cause unbounded DB growth

**FAIL conditions:**
- Unlimited forward_event writes
- No pruning
- Auto-ban enabled by default

---

### 5.5 Uptime tracking safety
**Must be true:**
- ☐ Presence/uptime is computed via a rolling accumulator (not infinite logs)
- ☐ On restart, uptime does not reset incorrectly or produce negative time
- ☐ Peer flapping cannot produce huge DB churn (rate limit state transitions)

**FAIL conditions:**
- Uptime stored as raw events without pruning
- Arithmetic bugs allow >100% uptime or negative uptime

---

### 5.6 Integration safety (Phase 4 bridge hooks)
**Must be true:**
- ☐ Promotion triggers Bridge policy changes safely:
  - member → `set_hive_policy(peer, True)`
  - neophyte/demoted → `set_hive_policy(peer, False)`
- ☐ Bridge calls use `safe_call` + circuit breaker (no direct RPC bypass)
- ☐ If bridge is disabled, promotion still updates local tier without crashing

**FAIL conditions:**
- Promotion causes direct RPC calls bypassing bridge hardening
- Promotion blocks on bridge failures

---

## Functional acceptance checks (must check)

### 5.7 Promotion flow works end-to-end
**Must be true:**
- ☐ Neophyte sends PROMOTION_REQUEST
- ☐ Members evaluate eligibility and emit VOUCH
- ☐ Target collects vouches, verifies them, reaches quorum, emits PROMOTION
- ☐ All nodes accept PROMOTION and set tier to member
- ☐ Duplicate vouches ignored; duplicate promotions ignored

---

## Concrete fixes (fill in during review)
For each issue:
- File + function
- Impact (DoS / spoof / replay / privilege escalation / DB bloat)
- Minimal patch (diff)
- Test to cover it

---

## New/updated tests (Phase 5)
**Required in `tests/test_membership.py`:**
- ☐ Uptime threshold test (99.6% pass, 99.4% fail over 30 days)
- ☐ Contribution ratio test (>=1 pass, <1 fail)
- ☐ Unique peers test (>=5 pass, <5 fail)
- ☐ Quorum test (5 members → quorum 3)
- ☐ Leech test (ratio 0.4 for 7 days → ban proposal triggered / flag set)

**Security tests (strongly recommended):**
- ☐ Invalid vouch signature rejected
- ☐ Voucher not a member rejected
- ☐ Voucher identity mismatch (sender != voucher_pubkey) rejected
- ☐ Expired vouch rejected
- ☐ PROMOTION with >MAX_VOUCHES rejected
- ☐ Neophyte attempting to vouch rejected
- ☐ Replay of PROMOTION (same request_id) is a no-op

---

## Final Phase 5 decision rubric
- **PASS**: all “Must be true” items checked, all tests passing, pruning present, identity binding & replay prevention correct
- **PARTIAL**: core promotion works but missing one or more security bounds/pruning/identity checks, or tests incomplete
- **FAIL**: any spoofing/replay path, any unbounded list/DB growth path, or promotion criteria unenforced
