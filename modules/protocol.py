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

    # Phase 7: Cooperative Fee Coordination
    FEE_INTELLIGENCE = 32809    # Share fee observations with hive
    LIQUIDITY_NEED = 32811      # Broadcast rebalancing needs
    HEALTH_REPORT = 32813       # NNLB health status report


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
# PHASE 7: FEE INTELLIGENCE PAYLOADS
# =============================================================================

@dataclass
class FeeIntelligencePayload:
    """
    FEE_INTELLIGENCE message payload - Share fee observations with hive.

    Enables cooperative fee setting by sharing observations about
    external peers' fee elasticity and routing performance.
    """
    reporter_id: str              # Who observed this (must match sender)
    target_peer_id: str           # External peer being reported on
    timestamp: int                # Unix timestamp of observation
    signature: str                # Required signature over payload

    # Current fee configuration
    our_fee_ppm: int              # Fee we charge to this peer
    their_fee_ppm: int            # Fee they charge us (if known)

    # Performance metrics (observation period)
    forward_count: int            # Number of forwards through this peer
    forward_volume_sats: int      # Total volume routed
    revenue_sats: int             # Fees earned from this peer

    # Flow analysis
    flow_direction: str           # 'source', 'sink', 'balanced'
    utilization_pct: float        # Channel utilization (0.0-1.0)

    # Elasticity observation (optional)
    last_fee_change_ppm: int = 0  # Previous fee rate (for elasticity calc)
    volume_delta_pct: float = 0.0 # Volume change after fee change

    # Confidence
    days_observed: int = 1        # How long we've observed this peer


@dataclass
class LiquidityNeedPayload:
    """
    LIQUIDITY_NEED message payload - Broadcast rebalancing needs.

    Enables cooperative rebalancing by sharing liquidity requirements.
    """
    reporter_id: str              # Who needs liquidity
    timestamp: int
    signature: str

    # What we need
    need_type: str                # 'inbound', 'outbound', 'rebalance'
    target_peer_id: str           # External peer (or hive member)
    amount_sats: int              # How much we need
    urgency: str                  # 'critical', 'high', 'medium', 'low'
    max_fee_ppm: int              # Maximum fee we'll pay

    # Why we need it
    reason: str                   # 'channel_depleted', 'opportunity', 'nnlb_assist'
    current_balance_pct: float    # Current local balance percentage

    # Reciprocity - what we can offer
    can_provide_inbound: int = 0  # Sats of inbound we can provide
    can_provide_outbound: int = 0 # Sats of outbound we can provide


@dataclass
class HealthReportPayload:
    """
    HEALTH_REPORT message payload - NNLB health status.

    Periodic health report for No Node Left Behind coordination.
    Allows hive to identify who needs help.
    """
    reporter_id: str
    timestamp: int
    signature: str

    # Self-reported health scores (0-100)
    overall_health: int
    capacity_score: int
    revenue_score: int
    connectivity_score: int

    # Specific needs (optional flags)
    needs_inbound: bool = False
    needs_outbound: bool = False
    needs_channels: bool = False

    # Willingness to help others
    can_provide_assistance: bool = False
    assistance_budget_sats: int = 0


# =============================================================================
# PHASE 7 VALIDATION CONSTANTS
# =============================================================================

# Fee intelligence bounds
MAX_FEE_PPM = 10000              # Maximum fee rate (1%)
MAX_VOLUME_SATS = 1_000_000_000_000  # 10k BTC max volume
MAX_DAYS_OBSERVED = 365          # Maximum observation period
FEE_INTELLIGENCE_MAX_AGE = 3600  # 1 hour max message age

# Liquidity need bounds
MAX_LIQUIDITY_AMOUNT = 100_000_000_000  # 1000 BTC max
VALID_NEED_TYPES = {'inbound', 'outbound', 'rebalance'}
VALID_URGENCY_LEVELS = {'critical', 'high', 'medium', 'low'}
VALID_FLOW_DIRECTIONS = {'source', 'sink', 'balanced'}

# Health report bounds
MAX_HEALTH_SCORE = 100
MIN_HEALTH_SCORE = 0

# Rate limits (count, period_seconds)
FEE_INTELLIGENCE_RATE_LIMIT = (10, 3600)    # 10 per hour per sender
LIQUIDITY_NEED_RATE_LIMIT = (5, 3600)       # 5 per hour per sender
HEALTH_REPORT_RATE_LIMIT = (1, 3600)        # 1 per hour per sender


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
    """Validate PEER_AVAILABLE payload schema."""
    if not isinstance(payload, dict):
        return False

    target_peer_id = payload.get("target_peer_id")
    reporter_peer_id = payload.get("reporter_peer_id")
    event_type = payload.get("event_type")
    timestamp = payload.get("timestamp")

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


# =============================================================================
# PHASE 7: FEE INTELLIGENCE SIGNING & VALIDATION
# =============================================================================

def get_fee_intelligence_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for FEE_INTELLIGENCE messages.

    Includes all critical fields to prevent tampering:
    - reporter_id, target_peer_id, timestamp
    - Fee and performance metrics
    - Flow analysis data

    Args:
        payload: FEE_INTELLIGENCE message payload

    Returns:
        Canonical string for signmessage()
    """
    return (
        f"FEE_INTELLIGENCE:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('target_peer_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{payload.get('our_fee_ppm', 0)}:"
        f"{payload.get('their_fee_ppm', 0)}:"
        f"{payload.get('forward_count', 0)}:"
        f"{payload.get('forward_volume_sats', 0)}:"
        f"{payload.get('revenue_sats', 0)}:"
        f"{payload.get('flow_direction', '')}:"
        f"{payload.get('utilization_pct', 0.0):.4f}"
    )


def validate_fee_intelligence_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a FEE_INTELLIGENCE payload.

    SECURITY: Bounds all values to prevent manipulation and overflow.

    Args:
        payload: FEE_INTELLIGENCE message payload

    Returns:
        True if valid, False otherwise
    """
    import time as time_module

    # Required string fields
    reporter_id = payload.get("reporter_id")
    target_peer_id = payload.get("target_peer_id")
    signature = payload.get("signature")

    if not isinstance(reporter_id, str) or not reporter_id:
        return False
    if not isinstance(target_peer_id, str) or not target_peer_id:
        return False
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Timestamp freshness
    timestamp = payload.get("timestamp", 0)
    if not isinstance(timestamp, int) or timestamp < 0:
        return False
    if abs(time_module.time() - timestamp) > FEE_INTELLIGENCE_MAX_AGE:
        return False

    # Fee bounds
    our_fee_ppm = payload.get("our_fee_ppm", 0)
    their_fee_ppm = payload.get("their_fee_ppm", 0)
    if not isinstance(our_fee_ppm, int) or not (0 <= our_fee_ppm <= MAX_FEE_PPM):
        return False
    if not isinstance(their_fee_ppm, int) or not (0 <= their_fee_ppm <= MAX_FEE_PPM):
        return False

    # Volume bounds (prevent overflow)
    forward_count = payload.get("forward_count", 0)
    forward_volume_sats = payload.get("forward_volume_sats", 0)
    revenue_sats = payload.get("revenue_sats", 0)

    if not isinstance(forward_count, int) or forward_count < 0:
        return False
    if not isinstance(forward_volume_sats, int) or not (0 <= forward_volume_sats <= MAX_VOLUME_SATS):
        return False
    if not isinstance(revenue_sats, int) or not (0 <= revenue_sats <= MAX_VOLUME_SATS):
        return False

    # Flow direction
    flow_direction = payload.get("flow_direction", "")
    if flow_direction and flow_direction not in VALID_FLOW_DIRECTIONS:
        return False

    # Utilization bounds
    utilization_pct = payload.get("utilization_pct", 0.0)
    if not isinstance(utilization_pct, (int, float)) or not (0 <= utilization_pct <= 1):
        return False

    # Days observed bounds
    days_observed = payload.get("days_observed", 1)
    if not isinstance(days_observed, int) or not (1 <= days_observed <= MAX_DAYS_OBSERVED):
        return False

    return True


def get_liquidity_need_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for LIQUIDITY_NEED messages.

    Args:
        payload: LIQUIDITY_NEED message payload

    Returns:
        Canonical string for signmessage()
    """
    return (
        f"LIQUIDITY_NEED:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{payload.get('need_type', '')}:"
        f"{payload.get('target_peer_id', '')}:"
        f"{payload.get('amount_sats', 0)}:"
        f"{payload.get('urgency', '')}:"
        f"{payload.get('max_fee_ppm', 0)}"
    )


def validate_liquidity_need_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a LIQUIDITY_NEED payload.

    Args:
        payload: LIQUIDITY_NEED message payload

    Returns:
        True if valid, False otherwise
    """
    # Required string fields
    reporter_id = payload.get("reporter_id")
    target_peer_id = payload.get("target_peer_id")
    signature = payload.get("signature")

    if not isinstance(reporter_id, str) or not reporter_id:
        return False
    if not isinstance(target_peer_id, str) or not target_peer_id:
        return False
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Timestamp
    timestamp = payload.get("timestamp", 0)
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # Need type validation
    need_type = payload.get("need_type")
    if need_type not in VALID_NEED_TYPES:
        return False

    # Urgency validation
    urgency = payload.get("urgency")
    if urgency not in VALID_URGENCY_LEVELS:
        return False

    # Amount bounds
    amount_sats = payload.get("amount_sats", 0)
    if not isinstance(amount_sats, int) or not (0 < amount_sats <= MAX_LIQUIDITY_AMOUNT):
        return False

    # Fee bounds
    max_fee_ppm = payload.get("max_fee_ppm", 0)
    if not isinstance(max_fee_ppm, int) or not (0 <= max_fee_ppm <= MAX_FEE_PPM):
        return False

    # Balance percentage
    current_balance_pct = payload.get("current_balance_pct", 0.0)
    if not isinstance(current_balance_pct, (int, float)) or not (0 <= current_balance_pct <= 1):
        return False

    return True


def get_health_report_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for HEALTH_REPORT messages.

    Args:
        payload: HEALTH_REPORT message payload

    Returns:
        Canonical string for signmessage()
    """
    return (
        f"HEALTH_REPORT:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{payload.get('overall_health', 0)}:"
        f"{payload.get('capacity_score', 0)}:"
        f"{payload.get('revenue_score', 0)}:"
        f"{payload.get('connectivity_score', 0)}"
    )


def validate_health_report_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a HEALTH_REPORT payload.

    Args:
        payload: HEALTH_REPORT message payload

    Returns:
        True if valid, False otherwise
    """
    # Required string fields
    reporter_id = payload.get("reporter_id")
    signature = payload.get("signature")

    if not isinstance(reporter_id, str) or not reporter_id:
        return False
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Timestamp
    timestamp = payload.get("timestamp", 0)
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # Health scores (0-100)
    for score_field in ['overall_health', 'capacity_score', 'revenue_score', 'connectivity_score']:
        score = payload.get(score_field, 0)
        if not isinstance(score, int) or not (MIN_HEALTH_SCORE <= score <= MAX_HEALTH_SCORE):
            return False

    # Assistance budget bounds
    assistance_budget = payload.get("assistance_budget_sats", 0)
    if not isinstance(assistance_budget, int) or assistance_budget < 0:
        return False

    return True


def create_fee_intelligence(
    reporter_id: str,
    target_peer_id: str,
    timestamp: int,
    signature: str,
    our_fee_ppm: int,
    their_fee_ppm: int,
    forward_count: int,
    forward_volume_sats: int,
    revenue_sats: int,
    flow_direction: str,
    utilization_pct: float,
    last_fee_change_ppm: int = 0,
    volume_delta_pct: float = 0.0,
    days_observed: int = 1
) -> bytes:
    """
    Create a FEE_INTELLIGENCE message.

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_fee_intelligence_signing_payload().

    Args:
        reporter_id: Hive member reporting this observation
        target_peer_id: External peer being reported on
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        our_fee_ppm: Fee we charge to this peer
        their_fee_ppm: Fee they charge us
        forward_count: Number of forwards
        forward_volume_sats: Total volume routed
        revenue_sats: Fees earned
        flow_direction: 'source', 'sink', or 'balanced'
        utilization_pct: Channel utilization (0.0-1.0)
        last_fee_change_ppm: Previous fee rate (for elasticity)
        volume_delta_pct: Volume change after fee change
        days_observed: How long this peer has been observed

    Returns:
        Serialized FEE_INTELLIGENCE message
    """
    payload = {
        "reporter_id": reporter_id,
        "target_peer_id": target_peer_id,
        "timestamp": timestamp,
        "signature": signature,
        "our_fee_ppm": our_fee_ppm,
        "their_fee_ppm": their_fee_ppm,
        "forward_count": forward_count,
        "forward_volume_sats": forward_volume_sats,
        "revenue_sats": revenue_sats,
        "flow_direction": flow_direction,
        "utilization_pct": utilization_pct,
        "last_fee_change_ppm": last_fee_change_ppm,
        "volume_delta_pct": volume_delta_pct,
        "days_observed": days_observed,
    }

    return serialize(HiveMessageType.FEE_INTELLIGENCE, payload)


def create_liquidity_need(
    reporter_id: str,
    timestamp: int,
    signature: str,
    need_type: str,
    target_peer_id: str,
    amount_sats: int,
    urgency: str,
    max_fee_ppm: int,
    reason: str,
    current_balance_pct: float,
    can_provide_inbound: int = 0,
    can_provide_outbound: int = 0
) -> bytes:
    """
    Create a LIQUIDITY_NEED message.

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_liquidity_need_signing_payload().

    Args:
        reporter_id: Hive member needing liquidity
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        need_type: 'inbound', 'outbound', or 'rebalance'
        target_peer_id: External peer (or hive member)
        amount_sats: How much liquidity needed
        urgency: 'critical', 'high', 'medium', or 'low'
        max_fee_ppm: Maximum fee willing to pay
        reason: Why liquidity is needed
        current_balance_pct: Current local balance percentage
        can_provide_inbound: Sats of inbound we can provide
        can_provide_outbound: Sats of outbound we can provide

    Returns:
        Serialized LIQUIDITY_NEED message
    """
    payload = {
        "reporter_id": reporter_id,
        "timestamp": timestamp,
        "signature": signature,
        "need_type": need_type,
        "target_peer_id": target_peer_id,
        "amount_sats": amount_sats,
        "urgency": urgency,
        "max_fee_ppm": max_fee_ppm,
        "reason": reason,
        "current_balance_pct": current_balance_pct,
        "can_provide_inbound": can_provide_inbound,
        "can_provide_outbound": can_provide_outbound,
    }

    return serialize(HiveMessageType.LIQUIDITY_NEED, payload)


def create_health_report(
    reporter_id: str,
    timestamp: int,
    signature: str,
    overall_health: int,
    capacity_score: int,
    revenue_score: int,
    connectivity_score: int,
    needs_inbound: bool = False,
    needs_outbound: bool = False,
    needs_channels: bool = False,
    can_provide_assistance: bool = False,
    assistance_budget_sats: int = 0
) -> bytes:
    """
    Create a HEALTH_REPORT message.

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_health_report_signing_payload().

    Args:
        reporter_id: Hive member reporting their health
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        overall_health: Overall health score (0-100)
        capacity_score: Capacity score (0-100)
        revenue_score: Revenue score (0-100)
        connectivity_score: Connectivity score (0-100)
        needs_inbound: Whether node needs inbound liquidity
        needs_outbound: Whether node needs outbound liquidity
        needs_channels: Whether node needs more channels
        can_provide_assistance: Whether node can help others
        assistance_budget_sats: How much node can spend helping

    Returns:
        Serialized HEALTH_REPORT message
    """
    payload = {
        "reporter_id": reporter_id,
        "timestamp": timestamp,
        "signature": signature,
        "overall_health": overall_health,
        "capacity_score": capacity_score,
        "revenue_score": revenue_score,
        "connectivity_score": connectivity_score,
        "needs_inbound": needs_inbound,
        "needs_outbound": needs_outbound,
        "needs_channels": needs_channels,
        "can_provide_assistance": can_provide_assistance,
        "assistance_budget_sats": assistance_budget_sats,
    }

    return serialize(HiveMessageType.HEALTH_REPORT, payload)
