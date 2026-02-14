"""
Idempotency module for cl-hive (Phase C hardening).

Provides deterministic event ID generation and check-and-record helpers
for state-changing protocol messages. Ensures that restarted nodes do not
re-process messages already handled before the restart.

Design:
- Each message family declares its identity fields in EVENT_ID_FIELDS.
- generate_event_id() produces a stable SHA-256 hash from those fields.
- check_and_record() wraps the DB insert with INSERT OR IGNORE semantics.
- Gossip/state-hash/intelligence snapshots are excluded (overwrite-based).
"""

import hashlib
import json
from typing import Any, Dict, Optional, Tuple


# Maps message type name -> list of payload fields that form the stable
# event identity.  Order matters for deterministic hashing.
EVENT_ID_FIELDS: Dict[str, list] = {
    # Phase 5: Membership
    "PROMOTION_REQUEST": ["target_pubkey", "request_id"],
    "VOUCH": ["target_pubkey", "request_id", "voucher_pubkey"],
    "PROMOTION": ["target_pubkey", "request_id"],
    "MEMBER_LEFT": ["peer_id", "timestamp"],
    # Phase 5: Ban governance
    "BAN_PROPOSAL": ["proposal_id"],
    "BAN_VOTE": ["proposal_id", "voter_peer_id"],
    # Phase 9: Fee reports
    "FEE_REPORT": ["peer_id", "period_start"],
    # Phase 12: Settlement
    "SETTLEMENT_PROPOSE": ["proposal_id"],
    "SETTLEMENT_READY": ["proposal_id", "voter_peer_id"],
    "SETTLEMENT_EXECUTED": ["proposal_id", "executor_peer_id"],
    # Phase 10: Task delegation
    "TASK_REQUEST": ["request_id"],
    "TASK_RESPONSE": ["request_id", "responder_id"],
    # Phase 11: Splice coordination
    "SPLICE_INIT_RESPONSE": ["session_id", "responder_id"],
    "SPLICE_INIT_REQUEST": ["session_id"],
    "SPLICE_UPDATE": ["session_id", "update_seq"],
    "SPLICE_SIGNED": ["session_id"],
    "SPLICE_ABORT": ["session_id"],
}


def generate_event_id(event_type: str, payload: Dict[str, Any]) -> Optional[str]:
    """
    Build a deterministic event ID from the message type and identity fields.

    Returns:
        32-char hex string, or None if no rules exist for event_type or
        required fields are missing.
    """
    fields = EVENT_ID_FIELDS.get(event_type)
    if not fields:
        return None

    identity = {"_type": event_type}
    for f in fields:
        val = payload.get(f)
        if val is None:
            return None
        identity[f] = val

    canonical = json.dumps(identity, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:32]


def check_and_record(db, event_type: str, payload: Dict[str, Any],
                     actor_id: str) -> Tuple[bool, Optional[str]]:
    """
    Generate event ID and atomically record it via the database.

    Returns:
        (is_new, event_id) where:
        - (True, event_id)  -- first time seeing this event
        - (False, event_id) -- duplicate, already recorded
        - (True, None)      -- untracked message type (no rules)
    """
    event_id = generate_event_id(event_type, payload)
    if event_id is None:
        return (True, None)

    is_new = db.record_proto_event(event_id, event_type, actor_id)
    return (is_new, event_id)
