# Security Review: cl-hive Branch Changes

**Date:** 2026-01-13
**Commits Analyzed:** ce0e6d1..d6e154f (5 commits ahead of origin/main)
**Reviewer:** Claude Opus 4.5

## Executive Summary

This review analyzed 6,504 lines of additions across the cl-hive plugin for Core Lightning. The changes implement cooperative expansion features, peer quality scoring, intelligent channel sizing, and hot-reload configuration support.

**Overall Assessment:** No HIGH-SEVERITY vulnerabilities found. The codebase follows good security practices with proper input validation, parameterized SQL queries, and authorization checks.

---

## Files Reviewed

| File | Lines Changed | Risk Area |
|------|---------------|-----------|
| `cl-hive.py` | +1918 | RPC handlers, message processing |
| `modules/cooperative_expansion.py` | +885 | State coordination, elections |
| `modules/quality_scorer.py` | +554 | Scoring algorithms |
| `modules/database.py` | +492 | Data persistence, SQL |
| `modules/planner.py` | +567 | Channel planning |
| `modules/protocol.py` | +346 | Message validation |
| `modules/config.py` | +27 | Configuration |

---

## Security Analysis

### 1. Input Validation - GOOD

**Finding:** All incoming protocol messages have proper validation.

The protocol module (`modules/protocol.py`) includes validators for all new message types:
- `validate_peer_available()` - Lines 417-470
- `validate_expansion_nominate()` - Lines 628-667
- `validate_expansion_elect()` - Lines 670-705

**Positive Observations:**
- Public keys validated via `_valid_pubkey()` (66 hex characters)
- Event types restricted to an explicit allowlist
- Numeric fields type-checked
- Quality scores bounded to 0-1 range

```python
# Example from protocol.py:339
def _valid_pubkey(pubkey: Any) -> bool:
    """Check if value is a valid 66-char hex pubkey."""
    if not isinstance(pubkey, str) or len(pubkey) != 66:
        return False
    return all(c in "0123456789abcdef" for c in pubkey)
```

---

### 2. SQL Injection Prevention - GOOD

**Finding:** All SQL queries use parameterized statements.

**Review of `modules/database.py`:**
- All `INSERT`, `UPDATE`, `DELETE`, and `SELECT` statements use `?` placeholders
- User-supplied values never concatenated into query strings
- The `update_member()` method constructs column names from an allowlist

```python
# database.py:446 - Safe dynamic update
allowed = {'tier', 'contribution_ratio', 'uptime_pct', 'vouch_count',
           'last_seen', 'promoted_at', 'metadata'}
updates = {k: v for k, v in kwargs.items() if k in allowed}
set_clause = ", ".join(f"{k} = ?" for k in updates.keys())  # Only allowed keys
```

**Note:** While `set_clause` is constructed dynamically, keys are strictly validated against `allowed` set, preventing injection.

---

### 3. Authorization and Authentication - GOOD

**Finding:** All RPC commands have appropriate permission checks.

The `_check_permission()` function (cl-hive.py:216) enforces a tier-based permission model:
- **Admin Only:** `hive-genesis`, `hive-invite`, `hive-ban`, expansion management
- **Member Only:** `hive-vouch`, `hive-approve-action`
- **Any Tier:** `hive-status`, `hive-topology`, query-only commands

**Protocol Message Handling:**
All incoming gossip messages verify sender membership:
```python
# cl-hive.py:2251-2253
sender = database.get_member(peer_id)
if not sender or database.is_banned(peer_id):
    return {"result": "continue"}  # Silently drop
```

---

### 4. Race Condition Protection - GOOD

**Finding:** The cooperative expansion module uses proper locking.

`CooperativeExpansionManager` uses `threading.Lock()` to protect:
- Round state transitions
- Nomination additions
- Election processing

```python
# cooperative_expansion.py:495
def add_nomination(self, round_id: str, nomination: Nomination) -> bool:
    with self._lock:
        round_obj = self._rounds.get(round_id)
        if not round_obj:
            return False
        if round_obj.state != ExpansionRoundState.NOMINATING:
            return False
        # ... safe modification
```

**Round Merging:** Deterministic merge protocol uses lexicographic round ID comparison to prevent split-brain scenarios (lines 557-580).

---

### 5. Resource Exhaustion - LOW RISK

**Finding:** Reasonable limits are in place but could be more explicit.

**Current Limits:**
- `MAX_ACTIVE_ROUNDS = 5` (cooperative_expansion.py:128)
- `limit = min(max(1, limit), 500)` for queries (cl-hive.py:3472)
- Round expiration: `ROUND_EXPIRE_SECONDS = 120`
- Target cooldown: `COOLDOWN_SECONDS = 300`

**Recommendation (LOW):** Consider adding explicit rate limiting for incoming `PEER_AVAILABLE` messages to prevent gossip flooding from a compromised hive member.

---

### 6. Budget Controls - GOOD

**Finding:** Financial safety mechanisms are well-implemented.

Budget constraints (`modules/cooperative_expansion.py:202-249`):
1. Reserve percentage (default 20%) kept on-chain
2. Daily budget cap (default 10M sats)
3. Per-channel maximum (50% of daily budget)

```python
# cooperative_expansion.py:237
available = min(after_reserve, daily_budget, max_per_channel)
```

Channel opens via pending actions require explicit approval in advisor mode.

---

### 7. Code Injection Prevention - GOOD

**Finding:** No dangerous patterns found.

Searched for dangerous dynamic code patterns - none present in the diff:
- No dynamic code execution functions
- No shell command execution through strings
- No dangerous compile operations

The `subprocess` usage in `modules/bridge.py` is for `lightning-cli` calls with properly constructed command arrays (not shell=True).

---

### 8. Hot-Reload Configuration - ADEQUATE

**Finding:** Hot-reload is implemented safely but has a minor concern.

The `setconfig` handler (cl-hive.py:325-415) properly:
- Validates new values before applying
- Reverts changes on validation failure
- Uses version tracking for snapshots

**Minor Note:** Immutable options (`hive-db-path`) are checked but not explicitly blocked by CLN's dynamic option system - they rely on runtime logging warnings.

---

## Informational Findings

### 1. No Cryptographic Signature Verification on Elections

**Classification:** Informational (by design)

Election results are broadcast via `EXPANSION_ELECT` without cryptographic proof. A malicious hive member could broadcast false elections.

**Mitigation:** This is acceptable because:
1. Only existing hive members can send messages
2. Channels require on-chain action (funds commitment)
3. The worst case is a confused state, not fund loss

### 2. Quality Score Manipulation

**Classification:** Informational

Hive members report their own channel performance data. A malicious member could report inflated scores for certain peers.

**Mitigation:** The `consistency_score` component (15% weight) penalizes scores that disagree with other reporters. Multiple data points are aggregated.

---

## Recommendations

All recommendations from the initial review have been implemented:

1. ~~**OPTIONAL:** Add explicit rate limiting for `PEER_AVAILABLE` messages per sender (e.g., max 10/minute).~~
   - **IMPLEMENTED**: `RateLimiter` class added (cl-hive.py:211-307), applied in `handle_peer_available()` (cl-hive.py:2368-2374)

2. ~~**OPTIONAL:** Consider signing `EXPANSION_ELECT` messages with the coordinator's key for stronger authenticity.~~
   - **IMPLEMENTED**: Cryptographic signatures added to both `EXPANSION_NOMINATE` and `EXPANSION_ELECT` messages
   - Signing: `_broadcast_expansion_nomination()` and `_broadcast_expansion_elect()` now sign payloads
   - Verification: `handle_expansion_nominate()` and `handle_expansion_elect()` verify signatures

3. ~~**DOCUMENTATION:** Add a threat model document describing trust assumptions between hive members.~~
   - **IMPLEMENTED**: See `docs/security/THREAT_MODEL.md`

---

## Conclusion

The cl-hive cooperative expansion implementation demonstrates good security practices:
- Input validation at protocol boundaries
- Parameterized SQL throughout
- Proper authorization checks
- Thread-safe state management
- Budget controls preventing overspending

No blocking security issues were found. The codebase is suitable for continued development and testing.
