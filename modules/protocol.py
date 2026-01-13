"""
Protocol module for cl-hive

Implements BOLT 8 custom message types for Hive communication.

Wire Format:
    All messages use a 4-byte magic prefix (0x48495645 = "HIVE") to avoid
    collisions with other plugins using the experimental message range.

    ┌────────────────────┬────────────────────────────────────┐
    │  Magic Bytes (4)   │           Payload (N)              │
    ├────────────────────┼────────────────────────────────────┤
    │     0x48495645     │  [Message-Type-Specific Content]   │
    │     ("HIVE")       │                                    │
    └────────────────────┴────────────────────────────────────┘

Message ID Range: 32769 - 33000 (Odd numbers for safe ignoring by non-Hive peers)
"""

import hashlib
import json
from enum import IntEnum
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass


# =============================================================================
# CONSTANTS
# =============================================================================

# 4-byte magic prefix: ASCII "HIVE" = 0x48 0x49 0x56 0x45
HIVE_MAGIC = b'HIVE'
HIVE_MAGIC_HEX = 0x48495645

# Protocol version for compatibility checks
PROTOCOL_VERSION = 1

# Maximum message size in bytes (post-hex decode)
MAX_MESSAGE_BYTES = 65535

# =============================================================================
# MESSAGE TYPES
# =============================================================================

class HiveMessageType(IntEnum):
    """
    BOLT 8 custom message IDs for Hive protocol.
    
    Uses odd numbers in experimental range (32768+) so non-Hive nodes
    can safely ignore unknown messages per BOLT 1.
    
    MVP Messages (Phase 1):
        HELLO, CHALLENGE, ATTEST, WELCOME
    
    Deferred Messages:
        GOSSIP (Phase 2), INTENT (Phase 3), VOUCH/BAN/PROMOTION (Phase 5)
    """
    # Phase 1: Handshake
    HELLO = 32769       # Ticket presentation
    CHALLENGE = 32771   # Nonce for proof-of-identity
    ATTEST = 32773      # Signed manifest + nonce response
    WELCOME = 32775     # Session established, HiveID assigned
    
    # Phase 2: State Sync (deferred)
    GOSSIP = 32777      # State update broadcast
    STATE_HASH = 32779  # Anti-entropy hash exchange
    FULL_SYNC = 32781   # Full state sync request/response
    
    # Phase 3: Coordination (deferred)
    INTENT = 32783      # Intent lock announcement
    INTENT_ACK = 32785  # Intent acknowledgment
    INTENT_ABORT = 32787  # Intent abort notification
    
    # Phase 5: Governance (deferred)
    VOUCH = 32789       # Member vouching for promotion
    BAN = 32791         # Ban announcement (executed ban)
    PROMOTION = 32793   # Promotion confirmation
    PROMOTION_REQUEST = 32795  # Neophyte requesting promotion
    MEMBER_LEFT = 32797  # Member voluntarily leaving hive
    BAN_PROPOSAL = 32799  # Propose banning a member (requires vote)
    BAN_VOTE = 32801     # Vote on a pending ban proposal

    # Phase 6: Channel Coordination
    PEER_AVAILABLE = 32803  # Notify hive that a peer is available for channels
    EXPANSION_NOMINATE = 32805  # Nominate self to open channel (Phase 6.4)
    EXPANSION_ELECT = 32807     # Announce elected member for expansion (Phase 6.4)


# =============================================================================
# PHASE 5 VALIDATION CONSTANTS
# =============================================================================

# Maximum number of vouches allowed in a promotion message
MAX_VOUCHES_IN_PROMOTION = 50

# Maximum length of request_id
MAX_REQUEST_ID_LEN = 64

# Vouch validity window (7 days)
VOUCH_TTL_SECONDS = 7 * 24 * 3600


# =============================================================================
# PAYLOAD STRUCTURES
# =============================================================================

@dataclass
class HelloPayload:
    """HIVE_HELLO message payload - Ticket presentation."""
    ticket: str         # Base64-encoded signed ticket
    protocol_version: int = PROTOCOL_VERSION


@dataclass
class ChallengePayload:
    """HIVE_CHALLENGE message payload - Nonce for authentication."""
    nonce: str          # 32-byte random hex string
    hive_id: str        # Hive identifier (for multi-hive future)


@dataclass  
class AttestPayload:
    """HIVE_ATTEST message payload - Signed manifest + nonce response."""
    pubkey: str         # Node public key (66 hex chars)
    version: str        # Plugin version string
    features: list      # Supported features ["splice", "dual-fund", ...]
    nonce_signature: str  # signmessage(nonce) result
    manifest_signature: str  # signmessage(manifest_json) result


@dataclass
class WelcomePayload:
    """HIVE_WELCOME message payload - Session established."""
    hive_id: str        # Assigned Hive identifier
    tier: str           # 'neophyte', 'member', or 'admin'
    member_count: int   # Current Hive size
    state_hash: str     # Current state hash for anti-entropy


# =============================================================================
# SERIALIZATION
# =============================================================================

def serialize(msg_type: HiveMessageType, payload: Dict[str, Any]) -> bytes:
    """
    Serialize a Hive message for transmission via sendcustommsg.
    
    Format: MAGIC (4 bytes) + JSON payload
    
    Args:
        msg_type: HiveMessageType enum value
        payload: Dictionary to serialize as JSON
        
    Returns:
        bytes: Wire-ready message with magic prefix
        
    Example:
        >>> data = serialize(HiveMessageType.HELLO, {"ticket": "abc123..."})
        >>> data[:4]
        b'HIVE'
    """
    # Add message type to payload for deserialization
    envelope = {
        "type": int(msg_type),
        "version": PROTOCOL_VERSION,
        "payload": payload
    }
    
    # JSON encode
    json_bytes = json.dumps(envelope, separators=(',', ':')).encode('utf-8')
    
    # Prepend magic
    return HIVE_MAGIC + json_bytes


def deserialize(data: bytes) -> Tuple[Optional[HiveMessageType], Optional[Dict[str, Any]]]:
    """
    Deserialize a Hive message received via custommsg hook.
    
    Performs magic byte verification before attempting JSON parse.
    
    Args:
        data: Raw bytes from custommsg event
        
    Returns:
        Tuple of (message_type, payload) if valid Hive message
        Tuple of (None, None) if magic check fails or parse error
        
    Example:
        >>> msg_type, payload = deserialize(data)
        >>> if msg_type is None:
        ...     return {"result": "continue"}  # Not our message
    """
    # Peek & Check: Verify magic prefix
    if len(data) < 4 or len(data) > MAX_MESSAGE_BYTES:
        return (None, None)
    
    if data[:4] != HIVE_MAGIC:
        return (None, None)
    
    # Strip magic and parse JSON
    try:
        json_data = data[4:].decode('utf-8')
        envelope = json.loads(json_data)
        
        if envelope.get('version') != PROTOCOL_VERSION:
            return (None, None)

        msg_type = HiveMessageType(envelope['type'])
        payload = envelope.get('payload', {})
        if not isinstance(payload, dict):
            return (None, None)
        
        return (msg_type, payload)
        
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        # Malformed message - log would go here in production
        return (None, None)


def is_hive_message(data: bytes) -> bool:
    """
    Quick check if data is a Hive message (magic prefix only).
    
    Use this for fast rejection in custommsg hook before full deserialization.
    
    Args:
        data: Raw bytes from custommsg event
        
    Returns:
        True if magic prefix matches, False otherwise
    """
    return len(data) >= 4 and data[:4] == HIVE_MAGIC


# =============================================================================
# PHASE 5 PAYLOAD VALIDATION
# =============================================================================

def _valid_request_id(request_id: Any) -> bool:
    if not isinstance(request_id, str):
        return False
    if not request_id or len(request_id) > MAX_REQUEST_ID_LEN:
        return False
    return all(c in "0123456789abcdef" for c in request_id)


def validate_promotion_request(payload: Dict[str, Any]) -> bool:
    """Validate PROMOTION_REQUEST payload schema."""
    if not isinstance(payload, dict):
        return False
    target_pubkey = payload.get("target_pubkey")
    request_id = payload.get("request_id")
    timestamp = payload.get("timestamp")
    if not isinstance(target_pubkey, str) or not target_pubkey:
        return False
    if not _valid_request_id(request_id):
        return False
    if not isinstance(timestamp, int) or timestamp < 0:
        return False
    return True


def validate_vouch(payload: Dict[str, Any]) -> bool:
    """Validate VOUCH payload schema."""
    if not isinstance(payload, dict):
        return False
    required = ["target_pubkey", "request_id", "timestamp", "voucher_pubkey", "sig"]
    for key in required:
        if key not in payload:
            return False
    if not isinstance(payload["target_pubkey"], str) or not payload["target_pubkey"]:
        return False
    if not _valid_request_id(payload["request_id"]):
        return False
    if not isinstance(payload["timestamp"], int) or payload["timestamp"] < 0:
        return False
    if not isinstance(payload["voucher_pubkey"], str) or not payload["voucher_pubkey"]:
        return False
    if not isinstance(payload["sig"], str) or not payload["sig"]:
        return False
    return True


def validate_promotion(payload: Dict[str, Any]) -> bool:
    """Validate PROMOTION payload schema and vouch list caps."""
    if not isinstance(payload, dict):
        return False
    target_pubkey = payload.get("target_pubkey")
    request_id = payload.get("request_id")
    vouches = payload.get("vouches")
    if not isinstance(target_pubkey, str) or not target_pubkey:
        return False
    if not _valid_request_id(request_id):
        return False
    if not isinstance(vouches, list):
        return False
    if len(vouches) > MAX_VOUCHES_IN_PROMOTION:
        return False
    for vouch in vouches:
        if not validate_vouch(vouch):
            return False
        if vouch.get("target_pubkey") != target_pubkey:
            return False
        if vouch.get("request_id") != request_id:
            return False
    return True


def validate_member_left(payload: Dict[str, Any]) -> bool:
    """Validate MEMBER_LEFT payload schema."""
    if not isinstance(payload, dict):
        return False
    peer_id = payload.get("peer_id")
    timestamp = payload.get("timestamp")
    reason = payload.get("reason")
    signature = payload.get("signature")

    # peer_id must be valid pubkey (66 hex chars)
    if not isinstance(peer_id, str) or len(peer_id) != 66:
        return False
    if not all(c in "0123456789abcdef" for c in peer_id):
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # reason must be a non-empty string
    if not isinstance(reason, str) or not reason:
        return False

    # signature must be present (zbase encoded)
    if not isinstance(signature, str) or not signature:
        return False

    return True


def _valid_pubkey(pubkey: Any) -> bool:
    """Check if value is a valid 66-char hex pubkey."""
    if not isinstance(pubkey, str) or len(pubkey) != 66:
        return False
    return all(c in "0123456789abcdef" for c in pubkey)


def validate_ban_proposal(payload: Dict[str, Any]) -> bool:
    """Validate BAN_PROPOSAL payload schema."""
    if not isinstance(payload, dict):
        return False

    target_peer_id = payload.get("target_peer_id")
    proposer_peer_id = payload.get("proposer_peer_id")
    proposal_id = payload.get("proposal_id")
    reason = payload.get("reason")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # Validate pubkeys
    if not _valid_pubkey(target_peer_id):
        return False
    if not _valid_pubkey(proposer_peer_id):
        return False

    # proposal_id must be valid hex string
    if not _valid_request_id(proposal_id):
        return False

    # reason must be non-empty string
    if not isinstance(reason, str) or not reason or len(reason) > 500:
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # signature must be present
    if not isinstance(signature, str) or not signature:
        return False

    return True


def validate_ban_vote(payload: Dict[str, Any]) -> bool:
    """Validate BAN_VOTE payload schema."""
    if not isinstance(payload, dict):
        return False

    proposal_id = payload.get("proposal_id")
    voter_peer_id = payload.get("voter_peer_id")
    vote = payload.get("vote")  # "approve" or "reject"
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # proposal_id must be valid hex string
    if not _valid_request_id(proposal_id):
        return False

    # voter must be valid pubkey
    if not _valid_pubkey(voter_peer_id):
        return False

    # vote must be "approve" or "reject"
    if vote not in ("approve", "reject"):
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # signature must be present
    if not isinstance(signature, str) or not signature:
        return False

    return True


def validate_peer_available(payload: Dict[str, Any]) -> bool:
    """
    Validate PEER_AVAILABLE payload schema.

    SECURITY: Requires cryptographic signature from the reporter.
    """
    if not isinstance(payload, dict):
        return False

    target_peer_id = payload.get("target_peer_id")
    reporter_peer_id = payload.get("reporter_peer_id")
    event_type = payload.get("event_type")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # target_peer_id must be valid pubkey (the external peer)
    if not _valid_pubkey(target_peer_id):
        return False

    # reporter_peer_id must be valid pubkey (the hive member reporting)
    if not _valid_pubkey(reporter_peer_id):
        return False

    # event_type must be a valid string
    valid_event_types = (
        'channel_open',      # New channel opened
        'channel_close',     # Channel closed (any type)
        'remote_close',      # Remote peer initiated close
        'local_close',       # Local node initiated close
        'mutual_close',      # Mutual close
        'channel_expired',   # Channel expired/timeout
        'peer_quality'       # Periodic quality report
    )
    if event_type not in valid_event_types:
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present (zbase encoded)
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Optional numeric fields - validate if present
    optional_int_fields = [
        'capacity_sats', 'channel_id', 'duration_days',
        'total_revenue_sats', 'total_rebalance_cost_sats', 'net_pnl_sats',
        'forward_count', 'forward_volume_sats', 'our_fee_ppm', 'their_fee_ppm',
        'our_funding_sats', 'their_funding_sats'
    ]
    for field in optional_int_fields:
        val = payload.get(field)
        if val is not None and not isinstance(val, int):
            return False

    optional_float_fields = ['routing_score', 'profitability_score']
    for field in optional_float_fields:
        val = payload.get(field)
        if val is not None and not isinstance(val, (int, float)):
            return False

    return True


def get_peer_available_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing PEER_AVAILABLE messages.

    The signature covers core fields that identify the event, in sorted order.
    """
    signing_fields = {
        "target_peer_id": payload.get("target_peer_id", ""),
        "reporter_peer_id": payload.get("reporter_peer_id", ""),
        "event_type": payload.get("event_type", ""),
        "timestamp": payload.get("timestamp", 0),
        "capacity_sats": payload.get("capacity_sats", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


# =============================================================================
# PHASE 2: STATE MANAGEMENT MESSAGE VALIDATION
# =============================================================================

def validate_gossip(payload: Dict[str, Any]) -> bool:
    """
    Validate GOSSIP payload schema.

    SECURITY: Requires cryptographic signature from the sender.
    """
    if not isinstance(payload, dict):
        return False

    sender_id = payload.get("sender_id")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # sender_id must be valid pubkey
    if not _valid_pubkey(sender_id):
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # version must be a positive integer if present
    version = payload.get("version")
    if version is not None and (not isinstance(version, int) or version < 0):
        return False

    return True


def compute_gossip_data_hash(payload: Dict[str, Any]) -> str:
    """
    Compute a hash of the GOSSIP data fields.

    SECURITY: This hash is included in the signature to prevent
    data tampering while keeping the signing payload small.
    """
    data_fields = {
        "capacity_sats": payload.get("capacity_sats", 0),
        "available_sats": payload.get("available_sats", 0),
        "fee_policy": payload.get("fee_policy", {}),
        "topology": sorted(payload.get("topology", [])),  # Sort for determinism
    }
    json_str = json.dumps(data_fields, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()


def get_gossip_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing GOSSIP messages.

    SECURITY: The signature covers:
    - sender_id: Identity of sender
    - timestamp: Replay protection
    - version: State version for conflict resolution
    - fleet_hash: Overall fleet state hash
    - data_hash: Hash of actual gossip data (fee_policy, topology, capacity)

    This prevents data tampering attacks where an attacker modifies
    the fee policies or topology while keeping the signature valid.
    """
    data_hash = compute_gossip_data_hash(payload)

    signing_fields = {
        "sender_id": payload.get("sender_id", ""),
        "timestamp": payload.get("timestamp", 0),
        "version": payload.get("version", 0),
        "fleet_hash": payload.get("fleet_hash", ""),
        "data_hash": data_hash,
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def validate_state_hash(payload: Dict[str, Any]) -> bool:
    """
    Validate STATE_HASH payload schema.

    SECURITY: Requires cryptographic signature from the sender.
    """
    if not isinstance(payload, dict):
        return False

    sender_id = payload.get("sender_id")
    fleet_hash = payload.get("fleet_hash")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # sender_id must be valid pubkey
    if not _valid_pubkey(sender_id):
        return False

    # fleet_hash must be a string
    if not isinstance(fleet_hash, str) or not fleet_hash:
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    return True


def get_state_hash_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing STATE_HASH messages.

    The signature covers core fields in sorted order.
    """
    signing_fields = {
        "sender_id": payload.get("sender_id", ""),
        "fleet_hash": payload.get("fleet_hash", ""),
        "timestamp": payload.get("timestamp", 0),
        "peer_count": payload.get("peer_count", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def validate_full_sync(payload: Dict[str, Any]) -> bool:
    """
    Validate FULL_SYNC payload schema.

    SECURITY: Requires cryptographic signature from the sender.
    This is critical as FULL_SYNC contains membership lists.
    """
    if not isinstance(payload, dict):
        return False

    sender_id = payload.get("sender_id")
    fleet_hash = payload.get("fleet_hash")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")
    states = payload.get("states")

    # sender_id must be valid pubkey
    if not _valid_pubkey(sender_id):
        return False

    # fleet_hash must be a string
    if not isinstance(fleet_hash, str):
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # states must be a list (can be empty)
    if not isinstance(states, list):
        return False

    # Limit states to prevent DoS
    if len(states) > 500:
        return False

    return True


def compute_members_hash(members: list) -> str:
    """
    Compute a deterministic hash of the members list.

    SECURITY: This hash is included in the FULL_SYNC signature to prevent
    membership injection attacks. Without this, an attacker could modify
    the members array while keeping the signature valid.

    Args:
        members: List of member dicts with peer_id, tier, joined_at

    Returns:
        Hex-encoded SHA256 hash of the sorted members array
    """
    if not members:
        return ""

    # Extract minimal fields and sort by peer_id for determinism
    member_tuples = [
        {
            "peer_id": m.get("peer_id", ""),
            "tier": m.get("tier", ""),
            "joined_at": m.get("joined_at", 0),
        }
        for m in members
    ]
    member_tuples.sort(key=lambda x: x["peer_id"])

    json_str = json.dumps(member_tuples, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()


def compute_states_hash(states: list) -> str:
    """
    Compute a deterministic hash of the states list.

    SECURITY: This allows receivers to verify that received states
    match the signed fleet_hash, preventing state injection attacks.

    Algorithm matches StateManager.calculate_fleet_hash():
    1. Extract minimal tuples: (peer_id, version, timestamp)
    2. Sort by peer_id (lexicographic)
    3. Serialize to JSON with sorted keys
    4. SHA256 hash the result

    Args:
        states: List of state dicts from FULL_SYNC

    Returns:
        Hex-encoded SHA256 hash of the sorted state tuples
    """
    if not states:
        return ""

    # Extract minimal state tuples (matching StateManager algorithm)
    state_tuples = [
        {
            "peer_id": s.get("peer_id", ""),
            "version": s.get("version", 0),
            "timestamp": s.get("last_update", s.get("timestamp", 0)),
        }
        for s in states
    ]

    # Sort by peer_id for determinism
    state_tuples.sort(key=lambda x: x["peer_id"])

    # Serialize and hash
    json_str = json.dumps(state_tuples, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(json_str.encode('utf-8')).hexdigest()


def get_full_sync_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing FULL_SYNC messages.

    SECURITY: The signature covers:
    - sender_id: Identity of sender
    - fleet_hash: Cryptographic digest of states (verified separately)
    - members_hash: Cryptographic digest of members list
    - timestamp: Replay protection

    This prevents both state tampering AND membership injection attacks.
    """
    members = payload.get("members", [])
    members_hash = compute_members_hash(members)

    signing_fields = {
        "sender_id": payload.get("sender_id", ""),
        "fleet_hash": payload.get("fleet_hash", ""),
        "members_hash": members_hash,
        "timestamp": payload.get("timestamp", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


# =============================================================================
# PHASE 3: INTENT MESSAGE VALIDATION
# =============================================================================

def validate_intent_abort(payload: Dict[str, Any]) -> bool:
    """
    Validate INTENT_ABORT payload schema.

    SECURITY: Requires cryptographic signature from the initiator.
    Only the intent owner can abort their own intent.
    """
    if not isinstance(payload, dict):
        return False

    intent_type = payload.get("intent_type")
    target = payload.get("target")
    initiator = payload.get("initiator")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # intent_type must be a valid string
    valid_intent_types = ('channel_open', 'channel_close', 'rebalance')
    if intent_type not in valid_intent_types:
        return False

    # target must be valid pubkey
    if not _valid_pubkey(target):
        return False

    # initiator must be valid pubkey (the one aborting their intent)
    if not _valid_pubkey(initiator):
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    return True


def get_intent_abort_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing INTENT_ABORT messages.

    The signature proves the initiator is voluntarily aborting their intent.
    """
    signing_fields = {
        "intent_type": payload.get("intent_type", ""),
        "target": payload.get("target", ""),
        "initiator": payload.get("initiator", ""),
        "timestamp": payload.get("timestamp", 0),
        "reason": payload.get("reason", ""),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def create_hello(ticket: str) -> bytes:
    """Create a HIVE_HELLO message."""
    return serialize(HiveMessageType.HELLO, {
        "ticket": ticket,
        "protocol_version": PROTOCOL_VERSION
    })


def create_challenge(nonce: str, hive_id: str) -> bytes:
    """Create a HIVE_CHALLENGE message."""
    return serialize(HiveMessageType.CHALLENGE, {
        "nonce": nonce,
        "hive_id": hive_id
    })


def create_attest(pubkey: str, version: str, features: list,
                  nonce_signature: str, manifest_signature: str,
                  manifest: Dict[str, Any]) -> bytes:
    """Create a HIVE_ATTEST message."""
    return serialize(HiveMessageType.ATTEST, {
        "pubkey": pubkey,
        "version": version,
        "features": features,
        "nonce_signature": nonce_signature,
        "manifest_signature": manifest_signature,
        "manifest": manifest
    })


def create_welcome(hive_id: str, tier: str, member_count: int,
                   state_hash: str) -> bytes:
    """Create a HIVE_WELCOME message."""
    return serialize(HiveMessageType.WELCOME, {
        "hive_id": hive_id,
        "tier": tier,
        "member_count": member_count,
        "state_hash": state_hash
    })


def create_peer_available(target_peer_id: str, reporter_peer_id: str,
                          event_type: str, timestamp: int,
                          signature: str = "",
                          channel_id: str = "",
                          capacity_sats: int = 0,
                          routing_score: float = 0.0,
                          profitability_score: float = 0.0,
                          reason: str = "",
                          # Profitability data from cl-revenue-ops
                          duration_days: int = 0,
                          total_revenue_sats: int = 0,
                          total_rebalance_cost_sats: int = 0,
                          net_pnl_sats: int = 0,
                          forward_count: int = 0,
                          forward_volume_sats: int = 0,
                          our_fee_ppm: int = 0,
                          their_fee_ppm: int = 0,
                          # Channel funding info (for opens)
                          our_funding_sats: int = 0,
                          their_funding_sats: int = 0,
                          opener: str = "") -> bytes:
    """
    Create a PEER_AVAILABLE message.

    Used to notify hive members about channel events for topology awareness.
    Sent when:
    - A channel opens (local or remote initiated)
    - A channel closes (any type)
    - A peer's routing quality is exceptional

    Args:
        target_peer_id: The external peer involved
        reporter_peer_id: The hive member reporting (our pubkey)
        event_type: 'channel_open', 'channel_close', 'remote_close', 'local_close',
                    'mutual_close', 'channel_expired', or 'peer_quality'
        timestamp: Unix timestamp
        channel_id: The channel short ID
        capacity_sats: Channel capacity
        routing_score: Peer's routing quality score (0-1)
        profitability_score: Overall profitability score (0-1)
        reason: Human-readable reason

        # Profitability data (for closures):
        duration_days: How long the channel was open
        total_revenue_sats: Total routing fees earned
        total_rebalance_cost_sats: Total rebalancing costs
        net_pnl_sats: Net profit/loss
        forward_count: Number of forwards routed
        forward_volume_sats: Total volume routed
        our_fee_ppm: Fee rate we charged
        their_fee_ppm: Fee rate they charged us

        # Funding info (for opens):
        our_funding_sats: Amount we funded
        their_funding_sats: Amount they funded
        opener: Who opened: 'local' or 'remote'

    Returns:
        Serialized PEER_AVAILABLE message
    """
    payload = {
        "target_peer_id": target_peer_id,
        "reporter_peer_id": reporter_peer_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "reason": reason
    }

    # Add non-zero optional fields to reduce message size
    if channel_id:
        payload["channel_id"] = channel_id
    if capacity_sats:
        payload["capacity_sats"] = capacity_sats
    if routing_score:
        payload["routing_score"] = routing_score
    if profitability_score:
        payload["profitability_score"] = profitability_score

    # Profitability data
    if duration_days:
        payload["duration_days"] = duration_days
    if total_revenue_sats:
        payload["total_revenue_sats"] = total_revenue_sats
    if total_rebalance_cost_sats:
        payload["total_rebalance_cost_sats"] = total_rebalance_cost_sats
    if net_pnl_sats:
        payload["net_pnl_sats"] = net_pnl_sats
    if forward_count:
        payload["forward_count"] = forward_count
    if forward_volume_sats:
        payload["forward_volume_sats"] = forward_volume_sats
    if our_fee_ppm:
        payload["our_fee_ppm"] = our_fee_ppm
    if their_fee_ppm:
        payload["their_fee_ppm"] = their_fee_ppm

    # Funding info
    if our_funding_sats:
        payload["our_funding_sats"] = our_funding_sats
    if their_funding_sats:
        payload["their_funding_sats"] = their_funding_sats
    if opener:
        payload["opener"] = opener

    # SECURITY: Signature is required
    if signature:
        payload["signature"] = signature

    return serialize(HiveMessageType.PEER_AVAILABLE, payload)


# =============================================================================
# PHASE 6.4: COOPERATIVE EXPANSION PROTOCOL
# =============================================================================

def get_expansion_nominate_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing EXPANSION_NOMINATE messages.

    The signature covers all fields except the signature itself, in sorted order.
    """
    signing_fields = {
        "round_id": payload.get("round_id", ""),
        "target_peer_id": payload.get("target_peer_id", ""),
        "nominator_id": payload.get("nominator_id", ""),
        "timestamp": payload.get("timestamp", 0),
        "available_liquidity_sats": payload.get("available_liquidity_sats", 0),
        "quality_score": payload.get("quality_score", 0.5),
        "has_existing_channel": payload.get("has_existing_channel", False),
        "channel_count": payload.get("channel_count", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def validate_expansion_nominate(payload: Dict[str, Any]) -> bool:
    """
    Validate EXPANSION_NOMINATE payload schema.

    This message is sent by hive members to express interest in opening
    a channel to a target peer during a cooperative expansion round.

    SECURITY: Requires a valid cryptographic signature from the nominator.
    """
    if not isinstance(payload, dict):
        return False

    # Required fields
    round_id = payload.get("round_id")
    target_peer_id = payload.get("target_peer_id")
    nominator_id = payload.get("nominator_id")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # round_id must be a non-empty string
    if not isinstance(round_id, str) or len(round_id) < 8:
        return False

    # Pubkeys must be valid
    if not _valid_pubkey(target_peer_id):
        return False
    if not _valid_pubkey(nominator_id):
        return False

    # Timestamp must be valid
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # Signature must be present (zbase encoded string)
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Optional: Check numeric fields
    available_liquidity = payload.get("available_liquidity_sats", 0)
    if not isinstance(available_liquidity, int) or available_liquidity < 0:
        return False

    quality_score = payload.get("quality_score", 0.5)
    if not isinstance(quality_score, (int, float)) or not (0 <= quality_score <= 1):
        return False

    return True


def get_expansion_elect_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing EXPANSION_ELECT messages.

    The signature covers all fields except the signature itself, in sorted order.
    """
    signing_fields = {
        "round_id": payload.get("round_id", ""),
        "target_peer_id": payload.get("target_peer_id", ""),
        "elected_id": payload.get("elected_id", ""),
        "coordinator_id": payload.get("coordinator_id", ""),
        "timestamp": payload.get("timestamp", 0),
        "channel_size_sats": payload.get("channel_size_sats", 0),
        "quality_score": payload.get("quality_score", 0.5),
        "nomination_count": payload.get("nomination_count", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def validate_expansion_elect(payload: Dict[str, Any]) -> bool:
    """
    Validate EXPANSION_ELECT payload schema.

    This message announces which hive member has been elected to open
    a channel to the target peer.

    SECURITY: Requires a valid cryptographic signature from the coordinator
    who ran the election.
    """
    if not isinstance(payload, dict):
        return False

    # Required fields
    round_id = payload.get("round_id")
    target_peer_id = payload.get("target_peer_id")
    elected_id = payload.get("elected_id")
    coordinator_id = payload.get("coordinator_id")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # round_id must be a non-empty string
    if not isinstance(round_id, str) or len(round_id) < 8:
        return False

    # Pubkeys must be valid
    if not _valid_pubkey(target_peer_id):
        return False
    if not _valid_pubkey(elected_id):
        return False
    if not _valid_pubkey(coordinator_id):
        return False

    # Timestamp must be valid
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # Signature must be present (zbase encoded string)
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # channel_size_sats must be positive if present
    channel_size = payload.get("channel_size_sats", 0)
    if not isinstance(channel_size, int) or channel_size < 0:
        return False

    return True


def create_expansion_nominate(
    round_id: str,
    target_peer_id: str,
    nominator_id: str,
    timestamp: int,
    signature: str,
    available_liquidity_sats: int = 0,
    quality_score: float = 0.5,
    has_existing_channel: bool = False,
    channel_count: int = 0,
    reason: str = ""
) -> bytes:
    """
    Create an EXPANSION_NOMINATE message.

    Sent by hive members to express interest in opening a channel to a target
    during a cooperative expansion round.

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_expansion_nominate_signing_payload().

    Args:
        round_id: Unique identifier for this expansion round
        target_peer_id: The external peer to potentially open a channel to
        nominator_id: The hive member nominating themselves
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        available_liquidity_sats: Nominator's available onchain balance
        quality_score: Nominator's calculated quality score for the target
        has_existing_channel: Whether nominator already has a channel to target
        channel_count: Total number of channels the nominator has
        reason: Optional reason for nomination

    Returns:
        Serialized EXPANSION_NOMINATE message
    """
    payload = {
        "round_id": round_id,
        "target_peer_id": target_peer_id,
        "nominator_id": nominator_id,
        "timestamp": timestamp,
        "signature": signature,
        "available_liquidity_sats": available_liquidity_sats,
        "quality_score": quality_score,
        "has_existing_channel": has_existing_channel,
        "channel_count": channel_count,
    }

    if reason:
        payload["reason"] = reason

    return serialize(HiveMessageType.EXPANSION_NOMINATE, payload)


def create_expansion_elect(
    round_id: str,
    target_peer_id: str,
    elected_id: str,
    coordinator_id: str,
    timestamp: int,
    signature: str,
    channel_size_sats: int = 0,
    quality_score: float = 0.5,
    nomination_count: int = 0,
    reason: str = ""
) -> bytes:
    """
    Create an EXPANSION_ELECT message.

    Broadcast to announce which hive member has been elected to open
    a channel to the target peer.

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_expansion_elect_signing_payload().
    The coordinator_id identifies who ran the election and signed the message.

    Args:
        round_id: Unique identifier for this expansion round
        target_peer_id: The external peer to open a channel to
        elected_id: The hive member elected to open the channel
        coordinator_id: The hive member who ran the election and signed
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        channel_size_sats: Recommended channel size
        quality_score: Target's quality score
        nomination_count: Number of nominations received
        reason: Reason for election

    Returns:
        Serialized EXPANSION_ELECT message
    """
    payload = {
        "round_id": round_id,
        "target_peer_id": target_peer_id,
        "elected_id": elected_id,
        "coordinator_id": coordinator_id,
        "timestamp": timestamp,
        "signature": signature,
        "channel_size_sats": channel_size_sats,
        "quality_score": quality_score,
        "nomination_count": nomination_count,
    }

    if reason:
        payload["reason"] = reason

    return serialize(HiveMessageType.EXPANSION_ELECT, payload)
