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
import time
from enum import IntEnum
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field


# =============================================================================
# CONSTANTS
# =============================================================================

# 4-byte magic prefix: ASCII "HIVE" = 0x48 0x49 0x56 0x45
HIVE_MAGIC = b'HIVE'
HIVE_MAGIC_HEX = 0x48495645

# Protocol version for compatibility checks
PROTOCOL_VERSION = 1

# Version tolerance: accept messages from this range of protocol versions.
# Prevents fleet partition during rolling upgrades (Phase B hardening).
MIN_SUPPORTED_VERSION = 1
MAX_SUPPORTED_VERSION = 2
SUPPORTED_VERSIONS = set(range(MIN_SUPPORTED_VERSION, MAX_SUPPORTED_VERSION + 1))

# Maximum message size in bytes (post-hex decode)
MAX_MESSAGE_BYTES = 65535

# Maximum peer_id length (hex-encoded pubkey should be 66 chars, allow some margin)
MAX_PEER_ID_LEN = 128

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

    # Phase 8: Hive-wide Affordability
    EXPANSION_DECLINE = 32819   # Elected member declines, trigger fallback (Phase 8)

    # Phase 9: Settlement
    SETTLEMENT_OFFER = 32821    # Broadcast BOLT12 offer for settlement
    FEE_REPORT = 32823          # Real-time fee earnings report for settlement

    # Phase 7: Cooperative Fee Coordination
    FEE_INTELLIGENCE_SNAPSHOT = 32825  # Batch fee observations for all peers
    PEER_REPUTATION_SNAPSHOT = 32827   # Batch peer reputation for all peers
    ROUTE_PROBE_BATCH = 32829          # Batch route probe observations
    LIQUIDITY_SNAPSHOT = 32831         # Batch liquidity needs
    LIQUIDITY_NEED = 32811      # Broadcast rebalancing needs
    HEALTH_REPORT = 32813       # NNLB health status report
    ROUTE_PROBE = 32815         # Share routing observations (Phase 4)

    # Phase 10: Task Delegation
    TASK_REQUEST = 32833        # Request another member to perform a task
    TASK_RESPONSE = 32835       # Response to task request (accept/reject/complete)

    # Phase 11: Hive-Splice Coordination
    SPLICE_INIT_REQUEST = 32837   # Request peer to participate in splice
    SPLICE_INIT_RESPONSE = 32839  # Accept/reject splice with PSBT
    SPLICE_UPDATE = 32841         # Exchange updated PSBT during splice
    SPLICE_SIGNED = 32843         # Final signed PSBT/txid
    SPLICE_ABORT = 32845          # Abort splice operation

    # Phase 12: Distributed Settlement
    SETTLEMENT_PROPOSE = 32847    # Propose settlement for a period
    SETTLEMENT_READY = 32849      # Vote that data hash matches (quorum)
    SETTLEMENT_EXECUTED = 32851   # Confirm payment execution

    # Phase 13: Stigmergic Marker Sharing
    STIGMERGIC_MARKER_BATCH = 32853  # Batch of route markers for fleet learning

    # Phase 13: Pheromone Sharing
    PHEROMONE_BATCH = 32855  # Batch of fee pheromone levels for fleet learning

    # Phase 14: Fleet-Wide Intelligence Sharing
    YIELD_METRICS_BATCH = 32857    # Per-channel ROI and profitability metrics
    CIRCULAR_FLOW_ALERT = 32859    # Detected wasteful circular rebalancing patterns
    TEMPORAL_PATTERN_BATCH = 32861 # Hour/day flow patterns and predictions

    # Phase 14.2: Strategic Positioning & Rationalization
    CORRIDOR_VALUE_BATCH = 32863   # High-value routing corridors discovered
    POSITIONING_PROPOSAL = 32865   # Channel open recommendation for fleet coordination
    PHYSARUM_RECOMMENDATION = 32867 # Flow-based channel lifecycle (strengthen/atrophy/stimulate)
    COVERAGE_ANALYSIS_BATCH = 32869 # Peer coverage and ownership analysis
    CLOSE_PROPOSAL = 32871         # Channel close recommendation for redundancy

    # Phase 15: Min-Cost Max-Flow (MCF) Rebalance Optimization
    MCF_NEEDS_BATCH = 32873        # Batch of rebalance needs for MCF optimization
    MCF_SOLUTION_BROADCAST = 32875 # Computed MCF solution from coordinator
    MCF_ASSIGNMENT_ACK = 32877     # Acknowledge receipt of MCF assignment
    MCF_COMPLETION_REPORT = 32879  # Report completion of MCF assignment


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
    """
    HIVE_HELLO message payload - Introduction to hive.

    Channel existence serves as proof of stake - no ticket needed.
    If sender has a channel with a hive member, they can join as neophyte.
    """
    pubkey: str         # Sender's public key (66 hex chars)
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
    tier: str           # 'neophyte' or 'member'
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


@dataclass
class RouteProbePayload:
    """
    ROUTE_PROBE message payload - Routing intelligence.

    Share payment path quality observations to build collective
    routing intelligence across the hive.
    """
    reporter_id: str
    timestamp: int
    signature: str

    # Route definition
    destination: str           # Final destination pubkey
    path: List[str]            # Intermediate hops (pubkeys)

    # Probe results
    success: bool              # Did the probe succeed
    latency_ms: int            # Round-trip time in milliseconds
    failure_reason: str = ""   # If failed: 'temporary', 'permanent', 'capacity'
    failure_hop: int = -1      # Which hop failed (0-indexed, -1 if success)

    # Capacity observations
    estimated_capacity_sats: int = 0  # Max amount that would succeed

    # Fee observations
    total_fee_ppm: int = 0     # Total fees for this route
    per_hop_fees: List[int] = field(default_factory=list)  # Fee at each hop

    # Amount probed
    amount_probed_sats: int = 0


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
FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT = (2, 3600)  # 2 snapshots per hour per sender
MAX_PEERS_IN_SNAPSHOT = 200                 # Maximum peers in one snapshot message
LIQUIDITY_NEED_RATE_LIMIT = (5, 3600)       # 5 per hour per sender
LIQUIDITY_SNAPSHOT_RATE_LIMIT = (2, 3600)  # 2 snapshots per hour per sender
MAX_NEEDS_IN_SNAPSHOT = 50                 # Maximum liquidity needs in one snapshot message
HEALTH_REPORT_RATE_LIMIT = (1, 3600)        # 1 per hour per sender
ROUTE_PROBE_RATE_LIMIT = (20, 3600)         # 20 per hour per sender
ROUTE_PROBE_BATCH_RATE_LIMIT = (2, 3600)   # 2 batches per hour per sender
MAX_PROBES_IN_BATCH = 100                  # Maximum route probes in one batch message
PEER_REPUTATION_SNAPSHOT_RATE_LIMIT = (2, 86400)  # 2 snapshots per day per sender
MAX_PEERS_IN_REPUTATION_SNAPSHOT = 200      # Maximum peers in one reputation snapshot

# Stigmergic marker sharing constants
STIGMERGIC_MARKER_BATCH_RATE_LIMIT = (1, 3600)  # 1 batch per hour per sender
MAX_MARKERS_IN_BATCH = 50                   # Maximum markers in one batch message
MIN_MARKER_STRENGTH = 0.1                   # Minimum strength to share (after decay)
MAX_MARKER_AGE_HOURS = 24                   # Don't share markers older than this

# Pheromone sharing constants
PHEROMONE_BATCH_RATE_LIMIT = (1, 3600)      # 1 batch per hour per sender
MAX_PHEROMONES_IN_BATCH = 100               # Maximum pheromone entries in one batch
MIN_PHEROMONE_LEVEL = 0.5                   # Minimum level to share (meaningful signal)
PHEROMONE_WEIGHTING_FACTOR = 0.3            # How much to weight remote pheromones vs local

# Yield metrics sharing constants (Phase 14)
YIELD_METRICS_BATCH_RATE_LIMIT = (1, 86400)  # 1 batch per day per sender
MAX_YIELD_METRICS_IN_BATCH = 200             # Maximum channels in one batch
MIN_YIELD_ROI_TO_SHARE = -100.0              # Share even underwater channels (negative ROI)
YIELD_WEIGHTING_FACTOR = 0.4                 # How much to weight remote yield data

# Circular flow alert constants (Phase 14)
CIRCULAR_FLOW_ALERT_RATE_LIMIT = (10, 3600)  # Up to 10 alerts per hour (event-driven)
MIN_CIRCULAR_FLOW_SATS = 10000               # Minimum amount to report (10k sats)
MIN_CIRCULAR_FLOW_COST_SATS = 100            # Minimum cost to report (100 sats)
MAX_CIRCULAR_FLOW_MEMBERS = 10               # Maximum members in one circular flow

# Temporal pattern sharing constants (Phase 14)
TEMPORAL_PATTERN_BATCH_RATE_LIMIT = (4, 86400)  # 4 batches per day (every 6 hours)
MAX_PATTERNS_IN_BATCH = 500                     # Maximum patterns in one batch
MAX_TEMPORAL_PATTERNS_IN_BATCH = MAX_PATTERNS_IN_BATCH  # Alias for consistency
MIN_PATTERN_CONFIDENCE = 0.6                    # Minimum confidence to share
MIN_PATTERN_SAMPLES = 10                        # Minimum samples for pattern validity
MIN_TEMPORAL_PATTERN_CONFIDENCE = MIN_PATTERN_CONFIDENCE  # Alias
MIN_TEMPORAL_PATTERN_SAMPLES = MIN_PATTERN_SAMPLES        # Alias

# Strategic positioning sharing constants (Phase 14.2)
CORRIDOR_VALUE_BATCH_RATE_LIMIT = (2, 86400)    # 2 batches per day (every 12 hours)
MAX_CORRIDORS_IN_BATCH = 100                    # Maximum corridors in one batch
MIN_CORRIDOR_VALUE_SCORE = 0.05                 # Minimum value score to share
POSITIONING_PROPOSAL_RATE_LIMIT = (5, 86400)    # 5 proposals per day
MAX_POSITIONING_PROPOSALS_PER_CYCLE = 5         # Alias for broadcast function
PHYSARUM_RECOMMENDATION_RATE_LIMIT = (10, 86400) # 10 recommendations per day
MAX_PHYSARUM_RECOMMENDATIONS_PER_CYCLE = 10     # Alias for broadcast function
VALID_PHYSARUM_ACTIONS = {"strengthen", "atrophy", "stimulate", "hold"}
VALID_PRIORITY_TIERS = {"critical", "high", "medium", "low"}

# Channel rationalization sharing constants (Phase 14.2)
COVERAGE_ANALYSIS_BATCH_RATE_LIMIT = (2, 86400) # 2 batches per day
MAX_COVERAGE_ENTRIES_IN_BATCH = 200             # Maximum coverage entries
MIN_COVERAGE_OWNERSHIP_CONFIDENCE = 0.5         # Minimum confidence to share ownership
MIN_OWNERSHIP_CONFIDENCE = MIN_COVERAGE_OWNERSHIP_CONFIDENCE  # Alias
CLOSE_PROPOSAL_RATE_LIMIT = (5, 86400)          # 5 close proposals per day
MAX_CLOSE_PROPOSALS_PER_CYCLE = 5               # Alias for broadcast function

# MCF (Min-Cost Max-Flow) optimization constants (Phase 15)
MCF_NEEDS_BATCH_RATE_LIMIT = (2, 3600)          # 2 batches per hour per sender
MCF_SOLUTION_BROADCAST_RATE_LIMIT = (6, 3600)   # 6 solutions per hour (every 10 min)
MCF_ASSIGNMENT_ACK_RATE_LIMIT = (30, 3600)      # 30 acks per hour
MCF_COMPLETION_REPORT_RATE_LIMIT = (30, 3600)   # 30 completions per hour
MAX_MCF_NEEDS_IN_BATCH = 100                    # Maximum needs in one batch
MAX_MCF_ASSIGNMENTS_IN_SOLUTION = 200           # Maximum assignments in solution
MCF_SOLUTION_MAX_AGE = 1200                     # 20 minutes max solution age
MCF_MIN_AMOUNT_SATS = 10000                     # Minimum amount for MCF assignment
MCF_MAX_AMOUNT_SATS = 100_000_000_000           # 1000 BTC max per assignment
VALID_MCF_NEED_TYPES = {'inbound', 'outbound'}  # Valid need types
VALID_MCF_URGENCY_LEVELS = {'critical', 'high', 'medium', 'low'}

# Route probe constants
MAX_PATH_LENGTH = 20                        # Maximum hops in a path
MAX_LATENCY_MS = 60000                      # 60 seconds max latency
MAX_CAPACITY_SATS = 1_000_000_000           # 1 BTC max capacity per route
VALID_FAILURE_REASONS = {"", "temporary", "permanent", "capacity", "unknown"}

# Peer reputation constants
MAX_RESPONSE_TIME_MS = 60000                # 60 seconds max response time
MAX_FORCE_CLOSE_COUNT = 100                 # Reasonable max for tracking
MAX_CHANNEL_AGE_DAYS = 3650                 # 10 years max
MAX_OBSERVATION_DAYS = 365                  # 1 year max observation period
MAX_WARNINGS_COUNT = 10                     # Max warnings per report
MAX_WARNING_LENGTH = 200                    # Max length of each warning
VALID_WARNINGS = {
    "fee_spike",           # Sudden fee increase
    "force_close",         # Initiated force close
    "htlc_timeout",        # HTLC timeouts
    "offline_frequent",    # Frequently offline
    "channel_reject",      # Rejected channel opens
    "routing_failure",     # High routing failure rate
    "slow_response",       # Slow HTLC processing
    "fee_manipulation",    # Suspected fee manipulation
    "capacity_drain",      # Draining liquidity
    "other",               # Other issues
}


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
        
        if envelope.get('version') not in SUPPORTED_VERSIONS:
            return (None, None)

        msg_type = HiveMessageType(envelope['type'])
        payload = envelope.get('payload', {})
        if not isinstance(payload, dict):
            return (None, None)

        # Inject envelope version so handlers can check it without
        # changing the function signature (Phase B hardening).
        payload['_envelope_version'] = envelope.get('version')

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

    # Budget fields (Phase 8 - optional, backward compatible)
    # Validate only if present, must be non-negative integers
    budget_available = payload.get("budget_available_sats")
    if budget_available is not None:
        if not isinstance(budget_available, int) or budget_available < 0:
            return False

    budget_reserved = payload.get("budget_reserved_until")
    if budget_reserved is not None:
        if not isinstance(budget_reserved, int) or budget_reserved < 0:
            return False

    budget_update = payload.get("budget_last_update")
    if budget_update is not None:
        if not isinstance(budget_update, int) or budget_update < 0:
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

def create_hello(pubkey: str) -> bytes:
    """
    Create a HIVE_HELLO message.

    Args:
        pubkey: Sender's public key (66 hex chars)

    Channel existence serves as proof of stake - no ticket needed.
    """
    return serialize(HiveMessageType.HELLO, {
        "pubkey": pubkey,
        "protocol_version": PROTOCOL_VERSION,
        "supported_versions": sorted(SUPPORTED_VERSIONS)
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


# =============================================================================
# PHASE 8: EXPANSION DECLINE SIGNING & VALIDATION
# =============================================================================

def get_expansion_decline_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for EXPANSION_DECLINE messages.

    Args:
        payload: EXPANSION_DECLINE message payload

    Returns:
        Canonical string for signmessage()
    """
    return (
        f"EXPANSION_DECLINE:"
        f"{payload.get('round_id', '')}:"
        f"{payload.get('decliner_id', '')}:"
        f"{payload.get('reason', '')}:"
        f"{payload.get('timestamp', 0)}"
    )


def validate_expansion_decline(payload: Dict[str, Any]) -> bool:
    """
    Validate EXPANSION_DECLINE payload schema.

    SECURITY: Requires cryptographic signature from the decliner.

    Args:
        payload: EXPANSION_DECLINE message payload

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(payload, dict):
        return False

    # Required fields
    round_id = payload.get("round_id")
    decliner_id = payload.get("decliner_id")
    reason = payload.get("reason")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # round_id must be at least 8 characters
    if not isinstance(round_id, str) or len(round_id) < 8:
        return False

    # decliner_id must be valid pubkey
    if not _valid_pubkey(decliner_id):
        return False

    # reason must be a non-empty string
    if not isinstance(reason, str) or not reason:
        return False

    # Valid reasons
    valid_reasons = {
        'insufficient_funds', 'budget_consumed', 'feerate_high',
        'channel_exists', 'peer_unavailable', 'config_changed', 'manual'
    }
    if reason not in valid_reasons:
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp <= 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    return True


def create_expansion_decline(
    round_id: str,
    decliner_id: str,
    reason: str,
    timestamp: int,
    signature: str
) -> bytes:
    """
    Create an EXPANSION_DECLINE message.

    Sent by an elected member who cannot fulfill the channel open.
    Triggers fallback election to next candidate.

    Args:
        round_id: The expansion round ID
        decliner_id: Our pubkey (the elected member declining)
        reason: Reason for declining
        timestamp: Current Unix timestamp
        signature: Signature over the signing payload

    Returns:
        Serialized EXPANSION_DECLINE message
    """
    payload = {
        "round_id": round_id,
        "decliner_id": decliner_id,
        "reason": reason,
        "timestamp": timestamp,
        "signature": signature,
    }

    return serialize(HiveMessageType.EXPANSION_DECLINE, payload)


# =============================================================================
# PHASE 7: FEE INTELLIGENCE SIGNING & VALIDATION
# =============================================================================

def get_fee_intelligence_snapshot_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for FEE_INTELLIGENCE_SNAPSHOT messages.

    Signs over: reporter_id, timestamp, and a hash of the sorted peer data.
    This ensures the entire snapshot is authenticated without making the
    signing string excessively long.

    Args:
        payload: FEE_INTELLIGENCE_SNAPSHOT message payload

    Returns:
        Canonical string for signmessage()
    """
    import hashlib
    import json

    # Create deterministic hash of peers data
    peers = payload.get("peers", [])
    # Sort by peer_id for deterministic ordering
    sorted_peers = sorted(peers, key=lambda p: p.get("peer_id", ""))
    peers_json = json.dumps(sorted_peers, sort_keys=True, separators=(',', ':'))
    peers_hash = hashlib.sha256(peers_json.encode()).hexdigest()[:16]

    return (
        f"FEE_INTELLIGENCE_SNAPSHOT:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(peers)}:"
        f"{peers_hash}"
    )


def validate_fee_intelligence_snapshot_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a FEE_INTELLIGENCE_SNAPSHOT payload.

    SECURITY: Bounds all values to prevent manipulation and overflow.

    Args:
        payload: FEE_INTELLIGENCE_SNAPSHOT message payload

    Returns:
        True if valid, False otherwise
    """
    import time as time_module

    # Required string fields
    reporter_id = payload.get("reporter_id")
    signature = payload.get("signature")

    if not isinstance(reporter_id, str) or not reporter_id:
        return False
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Timestamp freshness
    timestamp = payload.get("timestamp", 0)
    if not isinstance(timestamp, int) or timestamp < 0:
        return False
    if abs(time_module.time() - timestamp) > FEE_INTELLIGENCE_MAX_AGE:
        return False

    # Peers array
    peers = payload.get("peers")
    if not isinstance(peers, list):
        return False
    if len(peers) > MAX_PEERS_IN_SNAPSHOT:
        return False

    # Validate each peer entry
    for peer in peers:
        if not isinstance(peer, dict):
            return False

        peer_id = peer.get("peer_id")
        if not isinstance(peer_id, str) or not peer_id:
            return False

        # Fee bounds
        our_fee_ppm = peer.get("our_fee_ppm", 0)
        their_fee_ppm = peer.get("their_fee_ppm", 0)
        if not isinstance(our_fee_ppm, int) or not (0 <= our_fee_ppm <= MAX_FEE_PPM):
            return False
        if not isinstance(their_fee_ppm, int) or not (0 <= their_fee_ppm <= MAX_FEE_PPM):
            return False

        # Volume bounds
        forward_count = peer.get("forward_count", 0)
        forward_volume_sats = peer.get("forward_volume_sats", 0)
        revenue_sats = peer.get("revenue_sats", 0)

        if not isinstance(forward_count, int) or forward_count < 0:
            return False
        if not isinstance(forward_volume_sats, int) or not (0 <= forward_volume_sats <= MAX_VOLUME_SATS):
            return False
        if not isinstance(revenue_sats, int) or not (0 <= revenue_sats <= MAX_VOLUME_SATS):
            return False

        # Flow direction
        flow_direction = peer.get("flow_direction", "")
        if flow_direction and flow_direction not in VALID_FLOW_DIRECTIONS:
            return False

        # Utilization bounds
        utilization_pct = peer.get("utilization_pct", 0.0)
        if not isinstance(utilization_pct, (int, float)) or not (0 <= utilization_pct <= 1):
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


def get_liquidity_snapshot_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for LIQUIDITY_SNAPSHOT messages.

    Signs over: reporter_id, timestamp, and a hash of the sorted needs data.
    This ensures the entire snapshot is authenticated without making the
    signing string excessively long.

    Args:
        payload: LIQUIDITY_SNAPSHOT message payload

    Returns:
        Canonical string for signmessage()
    """
    import hashlib
    import json

    # Create deterministic hash of needs data
    needs = payload.get("needs", [])
    # Sort by target_peer_id for deterministic ordering
    sorted_needs = sorted(needs, key=lambda n: (n.get("target_peer_id", ""), n.get("need_type", "")))
    needs_json = json.dumps(sorted_needs, sort_keys=True, separators=(',', ':'))
    needs_hash = hashlib.sha256(needs_json.encode()).hexdigest()[:16]

    return (
        f"LIQUIDITY_SNAPSHOT:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(needs)}:"
        f"{needs_hash}"
    )


def validate_liquidity_snapshot_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a LIQUIDITY_SNAPSHOT payload.

    SECURITY: Bounds all values to prevent manipulation and overflow.

    Args:
        payload: LIQUIDITY_SNAPSHOT message payload

    Returns:
        True if valid, False otherwise
    """
    import time as time_module

    # Required string fields
    reporter_id = payload.get("reporter_id")
    signature = payload.get("signature")

    if not isinstance(reporter_id, str) or not reporter_id:
        return False
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Timestamp freshness (allow 1 hour for snapshot messages)
    timestamp = payload.get("timestamp", 0)
    if not isinstance(timestamp, int) or timestamp < 0:
        return False
    if abs(time_module.time() - timestamp) > 3600:
        return False

    # Needs array
    needs = payload.get("needs")
    if not isinstance(needs, list):
        return False
    if len(needs) > MAX_NEEDS_IN_SNAPSHOT:
        return False

    # Validate each need entry
    for need in needs:
        if not isinstance(need, dict):
            return False

        # Target peer required
        target_peer_id = need.get("target_peer_id")
        if not isinstance(target_peer_id, str) or not target_peer_id:
            return False

        # Need type validation
        need_type = need.get("need_type")
        if need_type not in VALID_NEED_TYPES:
            return False

        # Urgency validation
        urgency = need.get("urgency")
        if urgency not in VALID_URGENCY_LEVELS:
            return False

        # Amount bounds
        amount_sats = need.get("amount_sats", 0)
        if not isinstance(amount_sats, int) or not (0 < amount_sats <= MAX_LIQUIDITY_AMOUNT):
            return False

        # Fee bounds
        max_fee_ppm = need.get("max_fee_ppm", 0)
        if not isinstance(max_fee_ppm, int) or not (0 <= max_fee_ppm <= MAX_FEE_PPM):
            return False

        # Balance percentage
        current_balance_pct = need.get("current_balance_pct", 0.0)
        if not isinstance(current_balance_pct, (int, float)) or not (0 <= current_balance_pct <= 1):
            return False

    return True


def create_liquidity_snapshot(
    reporter_id: str,
    timestamp: int,
    signature: str,
    needs: list
) -> bytes:
    """
    Create a LIQUIDITY_SNAPSHOT message.

    This is the preferred method for sharing liquidity needs, replacing
    individual LIQUIDITY_NEED messages. Send one snapshot with all needs
    instead of N individual messages.

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_liquidity_snapshot_signing_payload().

    Args:
        reporter_id: Hive member reporting these needs
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        needs: List of liquidity needs, each containing:
            - target_peer_id: External peer or hive member
            - need_type: 'inbound', 'outbound', 'rebalance'
            - amount_sats: How much is needed
            - urgency: 'critical', 'high', 'medium', 'low'
            - max_fee_ppm: Maximum fee willing to pay
            - reason: Why this liquidity is needed
            - current_balance_pct: Current local balance percentage
            - can_provide_inbound: Sats of inbound that can be provided
            - can_provide_outbound: Sats of outbound that can be provided

    Returns:
        Serialized LIQUIDITY_SNAPSHOT message
    """
    payload = {
        "reporter_id": reporter_id,
        "timestamp": timestamp,
        "signature": signature,
        "needs": needs,
    }

    return serialize(HiveMessageType.LIQUIDITY_SNAPSHOT, payload)


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


def get_route_probe_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for ROUTE_PROBE messages.

    Args:
        payload: ROUTE_PROBE message payload

    Returns:
        Canonical string for signmessage()
    """
    # Sort path to make signing deterministic
    path = payload.get("path", [])
    path_str = ",".join(sorted(path)) if path else ""

    return (
        f"ROUTE_PROBE:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('destination', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{path_str}:"
        f"{payload.get('success', False)}:"
        f"{payload.get('latency_ms', 0)}:"
        f"{payload.get('total_fee_ppm', 0)}"
    )


def validate_route_probe_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a ROUTE_PROBE payload.

    Args:
        payload: ROUTE_PROBE message payload

    Returns:
        True if valid, False otherwise
    """
    # Required string fields
    reporter_id = payload.get("reporter_id")
    destination = payload.get("destination")
    signature = payload.get("signature")

    if not isinstance(reporter_id, str) or not reporter_id:
        return False
    if not isinstance(destination, str) or not destination:
        return False
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Timestamp
    timestamp = payload.get("timestamp", 0)
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # Path validation
    path = payload.get("path", [])
    if not isinstance(path, list):
        return False
    if len(path) > MAX_PATH_LENGTH:
        return False
    for hop in path:
        if not isinstance(hop, str):
            return False

    # Success must be boolean
    success = payload.get("success")
    if not isinstance(success, bool):
        return False

    # Latency bounds
    latency_ms = payload.get("latency_ms", 0)
    if not isinstance(latency_ms, int) or not (0 <= latency_ms <= MAX_LATENCY_MS):
        return False

    # Failure reason validation
    failure_reason = payload.get("failure_reason", "")
    if failure_reason not in VALID_FAILURE_REASONS:
        return False

    # Failure hop must be valid index or -1
    failure_hop = payload.get("failure_hop", -1)
    if not isinstance(failure_hop, int):
        return False
    if failure_hop != -1 and (failure_hop < 0 or failure_hop >= len(path)):
        return False

    # Capacity bounds
    estimated_capacity = payload.get("estimated_capacity_sats", 0)
    if not isinstance(estimated_capacity, int) or not (0 <= estimated_capacity <= MAX_CAPACITY_SATS):
        return False

    # Fee bounds
    total_fee_ppm = payload.get("total_fee_ppm", 0)
    if not isinstance(total_fee_ppm, int) or not (0 <= total_fee_ppm <= MAX_FEE_PPM * MAX_PATH_LENGTH):
        return False

    # Per-hop fees validation
    per_hop_fees = payload.get("per_hop_fees", [])
    if not isinstance(per_hop_fees, list):
        return False
    for fee in per_hop_fees:
        if not isinstance(fee, int) or fee < 0:
            return False

    # Amount probed bounds
    amount_probed = payload.get("amount_probed_sats", 0)
    if not isinstance(amount_probed, int) or amount_probed < 0:
        return False

    return True


def create_route_probe(
    reporter_id: str,
    destination: str,
    path: List[str],
    success: bool,
    latency_ms: int,
    rpc,
    failure_reason: str = "",
    failure_hop: int = -1,
    estimated_capacity_sats: int = 0,
    total_fee_ppm: int = 0,
    per_hop_fees: List[int] = None,
    amount_probed_sats: int = 0
) -> Optional[bytes]:
    """
    Create a signed ROUTE_PROBE message.

    Args:
        reporter_id: Hive member reporting this probe
        destination: Final destination pubkey
        path: List of intermediate hop pubkeys
        success: Whether probe succeeded
        latency_ms: Round-trip time in milliseconds
        rpc: RPC interface for signing
        failure_reason: Reason for failure (if any)
        failure_hop: Index of failing hop (if any)
        estimated_capacity_sats: Estimated route capacity
        total_fee_ppm: Total fees for route
        per_hop_fees: Fee at each hop
        amount_probed_sats: Amount that was probed

    Returns:
        Serialized and signed ROUTE_PROBE message, or None on error
    """
    timestamp = int(time.time())

    payload = {
        "reporter_id": reporter_id,
        "destination": destination,
        "timestamp": timestamp,
        "path": path,
        "success": success,
        "latency_ms": latency_ms,
        "failure_reason": failure_reason,
        "failure_hop": failure_hop,
        "estimated_capacity_sats": estimated_capacity_sats,
        "total_fee_ppm": total_fee_ppm,
        "per_hop_fees": per_hop_fees or [],
        "amount_probed_sats": amount_probed_sats,
    }

    # Sign the payload
    signing_message = get_route_probe_signing_payload(payload)
    try:
        sig_result = rpc.signmessage(signing_message)
        payload["signature"] = sig_result["zbase"]
    except Exception:
        return None

    return serialize(HiveMessageType.ROUTE_PROBE, payload)


def get_route_probe_batch_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for ROUTE_PROBE_BATCH messages.

    Signs over: reporter_id, timestamp, and a hash of the sorted probes data.
    This ensures the entire batch is authenticated without making the
    signing string excessively long.

    Args:
        payload: ROUTE_PROBE_BATCH message payload

    Returns:
        Canonical string for signmessage()
    """
    import hashlib
    import json

    # Create deterministic hash of probes data
    probes = payload.get("probes", [])
    # Sort by destination for deterministic ordering
    sorted_probes = sorted(probes, key=lambda p: (p.get("destination", ""), p.get("timestamp", 0)))
    probes_json = json.dumps(sorted_probes, sort_keys=True, separators=(',', ':'))
    probes_hash = hashlib.sha256(probes_json.encode()).hexdigest()[:16]

    return (
        f"ROUTE_PROBE_BATCH:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(probes)}:"
        f"{probes_hash}"
    )


def validate_route_probe_batch_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a ROUTE_PROBE_BATCH payload.

    SECURITY: Bounds all values to prevent manipulation and overflow.

    Args:
        payload: ROUTE_PROBE_BATCH message payload

    Returns:
        True if valid, False otherwise
    """
    import time as time_module

    # Required string fields
    reporter_id = payload.get("reporter_id")
    signature = payload.get("signature")

    if not isinstance(reporter_id, str) or not reporter_id:
        return False
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Timestamp freshness (allow 1 hour for batch messages)
    timestamp = payload.get("timestamp", 0)
    if not isinstance(timestamp, int) or timestamp < 0:
        return False
    if abs(time_module.time() - timestamp) > 3600:
        return False

    # Probes array
    probes = payload.get("probes")
    if not isinstance(probes, list):
        return False
    if len(probes) > MAX_PROBES_IN_BATCH:
        return False

    # Validate each probe entry
    for probe in probes:
        if not isinstance(probe, dict):
            return False

        # Destination required
        destination = probe.get("destination")
        if not isinstance(destination, str) or not destination:
            return False

        # Path validation
        path = probe.get("path", [])
        if not isinstance(path, list):
            return False
        if len(path) > MAX_PATH_LENGTH:
            return False
        for hop in path:
            if not isinstance(hop, str):
                return False

        # Success must be boolean
        success = probe.get("success")
        if not isinstance(success, bool):
            return False

        # Latency bounds
        latency_ms = probe.get("latency_ms", 0)
        if not isinstance(latency_ms, int) or not (0 <= latency_ms <= MAX_LATENCY_MS):
            return False

        # Failure reason validation
        failure_reason = probe.get("failure_reason", "")
        if failure_reason not in VALID_FAILURE_REASONS:
            return False

        # Failure hop must be valid index or -1
        failure_hop = probe.get("failure_hop", -1)
        if not isinstance(failure_hop, int):
            return False
        if failure_hop != -1 and (failure_hop < 0 or failure_hop >= len(path)):
            return False

        # Capacity bounds
        estimated_capacity = probe.get("estimated_capacity_sats", 0)
        if not isinstance(estimated_capacity, int) or not (0 <= estimated_capacity <= MAX_CAPACITY_SATS):
            return False

        # Fee bounds
        total_fee_ppm = probe.get("total_fee_ppm", 0)
        if not isinstance(total_fee_ppm, int) or not (0 <= total_fee_ppm <= MAX_FEE_PPM * MAX_PATH_LENGTH):
            return False

        # Per-hop fees validation
        per_hop_fees = probe.get("per_hop_fees", [])
        if not isinstance(per_hop_fees, list):
            return False
        for fee in per_hop_fees:
            if not isinstance(fee, int) or fee < 0:
                return False

        # Amount probed bounds
        amount_probed = probe.get("amount_probed_sats", 0)
        if not isinstance(amount_probed, int) or amount_probed < 0:
            return False

    return True


def create_route_probe_batch(
    reporter_id: str,
    timestamp: int,
    signature: str,
    probes: list
) -> bytes:
    """
    Create a ROUTE_PROBE_BATCH message.

    This is the preferred method for sharing route probes, replacing
    individual ROUTE_PROBE messages. Send one batch with all probe
    observations instead of N individual messages.

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_route_probe_batch_signing_payload().

    Args:
        reporter_id: Hive member reporting these observations
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        probes: List of probe observations, each containing:
            - destination: Final destination pubkey
            - path: List of intermediate hop pubkeys
            - success: Whether probe succeeded
            - latency_ms: Round-trip time in milliseconds
            - failure_reason: Reason for failure (if any)
            - failure_hop: Index of failing hop (if any)
            - estimated_capacity_sats: Estimated route capacity
            - total_fee_ppm: Total fees for route
            - per_hop_fees: Fee at each hop
            - amount_probed_sats: Amount that was probed

    Returns:
        Serialized ROUTE_PROBE_BATCH message
    """
    payload = {
        "reporter_id": reporter_id,
        "timestamp": timestamp,
        "signature": signature,
        "probes": probes,
    }

    return serialize(HiveMessageType.ROUTE_PROBE_BATCH, payload)


def get_peer_reputation_snapshot_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for PEER_REPUTATION_SNAPSHOT messages.

    Signs over: reporter_id, timestamp, and a hash of the sorted peer data.
    This ensures the entire snapshot is authenticated without making the
    signing string excessively long.

    Args:
        payload: PEER_REPUTATION_SNAPSHOT message payload

    Returns:
        Canonical string for signmessage()
    """
    import hashlib
    import json

    # Create deterministic hash of peers data
    peers = payload.get("peers", [])
    # Sort by peer_id for deterministic ordering
    sorted_peers = sorted(peers, key=lambda p: p.get("peer_id", ""))
    peers_json = json.dumps(sorted_peers, sort_keys=True, separators=(',', ':'))
    peers_hash = hashlib.sha256(peers_json.encode()).hexdigest()[:16]

    return (
        f"PEER_REPUTATION_SNAPSHOT:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(peers)}:"
        f"{peers_hash}"
    )


def validate_peer_reputation_snapshot_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a PEER_REPUTATION_SNAPSHOT payload.

    SECURITY: Bounds all values to prevent manipulation and overflow.

    Args:
        payload: PEER_REPUTATION_SNAPSHOT message payload

    Returns:
        True if valid, False otherwise
    """
    import time as time_module

    # Required string fields
    reporter_id = payload.get("reporter_id")
    signature = payload.get("signature")

    if not isinstance(reporter_id, str) or not reporter_id:
        return False
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    # Timestamp freshness (allow 1 hour for reputation snapshots)
    timestamp = payload.get("timestamp", 0)
    if not isinstance(timestamp, int) or timestamp < 0:
        return False
    if abs(time_module.time() - timestamp) > 3600:
        return False

    # Peers array
    peers = payload.get("peers")
    if not isinstance(peers, list):
        return False
    if len(peers) > MAX_PEERS_IN_REPUTATION_SNAPSHOT:
        return False

    # Validate each peer entry
    for peer in peers:
        if not isinstance(peer, dict):
            return False

        peer_id = peer.get("peer_id")
        if not isinstance(peer_id, str) or not peer_id:
            return False

        # Uptime percentage bounds (0-1)
        uptime_pct = peer.get("uptime_pct", 1.0)
        if not isinstance(uptime_pct, (int, float)) or not (0 <= uptime_pct <= 1):
            return False

        # Response time bounds
        response_time_ms = peer.get("response_time_ms", 0)
        if not isinstance(response_time_ms, int) or not (0 <= response_time_ms <= MAX_RESPONSE_TIME_MS):
            return False

        # Force close count bounds
        force_close_count = peer.get("force_close_count", 0)
        if not isinstance(force_close_count, int) or not (0 <= force_close_count <= MAX_FORCE_CLOSE_COUNT):
            return False

        # Fee stability bounds (0-1)
        fee_stability = peer.get("fee_stability", 1.0)
        if not isinstance(fee_stability, (int, float)) or not (0 <= fee_stability <= 1):
            return False

        # HTLC success rate bounds (0-1)
        htlc_success_rate = peer.get("htlc_success_rate", 1.0)
        if not isinstance(htlc_success_rate, (int, float)) or not (0 <= htlc_success_rate <= 1):
            return False

        # Channel age bounds
        channel_age_days = peer.get("channel_age_days", 0)
        if not isinstance(channel_age_days, int) or not (0 <= channel_age_days <= MAX_CHANNEL_AGE_DAYS):
            return False

        # Total routed bounds
        total_routed_sats = peer.get("total_routed_sats", 0)
        if not isinstance(total_routed_sats, int) or total_routed_sats < 0:
            return False

        # Warnings validation
        warnings = peer.get("warnings", [])
        if not isinstance(warnings, list):
            return False
        if len(warnings) > MAX_WARNINGS_COUNT:
            return False
        for warning in warnings:
            if not isinstance(warning, str):
                return False
            if warning and warning not in VALID_WARNINGS:
                return False

    return True


def create_peer_reputation_snapshot(
    reporter_id: str,
    timestamp: int,
    signature: str,
    peers: list
) -> bytes:
    """
    Create a PEER_REPUTATION_SNAPSHOT message.

    This is the preferred method for sharing peer reputation, replacing
    individual PEER_REPUTATION messages. Send one snapshot with all peer
    observations instead of N individual messages.

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_peer_reputation_snapshot_signing_payload().

    Args:
        reporter_id: Hive member reporting these observations
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        peers: List of peer observations, each containing:
            - peer_id: External peer being reported on
            - uptime_pct: Peer uptime (0-1)
            - response_time_ms: Average HTLC response time
            - force_close_count: Force closes by peer
            - fee_stability: Fee stability (0-1)
            - htlc_success_rate: HTLC success rate (0-1)
            - channel_age_days: Channel age
            - total_routed_sats: Total volume routed
            - warnings: Warning codes list
            - observation_days: Days covered

    Returns:
        Serialized PEER_REPUTATION_SNAPSHOT message
    """
    payload = {
        "reporter_id": reporter_id,
        "timestamp": timestamp,
        "signature": signature,
        "peers": peers,
    }

    return serialize(HiveMessageType.PEER_REPUTATION_SNAPSHOT, payload)


def create_fee_intelligence_snapshot(
    reporter_id: str,
    timestamp: int,
    signature: str,
    peers: list
) -> bytes:
    """
    Create a FEE_INTELLIGENCE_SNAPSHOT message.

    This is the preferred method for sharing fee intelligence, replacing
    individual FEE_INTELLIGENCE messages. Send one snapshot with all peer
    observations instead of N individual messages.

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_fee_intelligence_snapshot_signing_payload().

    Args:
        reporter_id: Hive member reporting these observations
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        peers: List of peer observations, each containing:
            - peer_id: External peer being reported on
            - our_fee_ppm: Fee we charge to this peer
            - their_fee_ppm: Fee they charge us
            - forward_count: Number of forwards
            - forward_volume_sats: Total volume routed
            - revenue_sats: Fees earned
            - flow_direction: 'source', 'sink', or 'balanced'
            - utilization_pct: Channel utilization (0.0-1.0)

    Returns:
        Serialized FEE_INTELLIGENCE_SNAPSHOT message
    """
    payload = {
        "reporter_id": reporter_id,
        "timestamp": timestamp,
        "signature": signature,
        "peers": peers,
    }

    return serialize(HiveMessageType.FEE_INTELLIGENCE_SNAPSHOT, payload)


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


def create_settlement_offer(
    peer_id: str,
    bolt12_offer: str,
    timestamp: int,
    signature: str
) -> bytes:
    """
    Create a SETTLEMENT_OFFER message to broadcast BOLT12 offer for settlement.

    This message is broadcast when a member registers a settlement offer so
    all hive members can record the offer for future settlement calculations.

    Args:
        peer_id: Member's node public key
        bolt12_offer: BOLT12 offer string (lno1...)
        timestamp: Unix timestamp of registration
        signature: zbase-encoded signature from signmessage(peer_id + bolt12_offer)

    Returns:
        Serialized SETTLEMENT_OFFER message
    """
    payload = {
        "peer_id": peer_id,
        "bolt12_offer": bolt12_offer,
        "timestamp": timestamp,
        "signature": signature,
    }

    return serialize(HiveMessageType.SETTLEMENT_OFFER, payload)


def get_settlement_offer_signing_payload(peer_id: str, bolt12_offer: str) -> str:
    """
    Get the canonical payload for signing a settlement offer announcement.

    Args:
        peer_id: Member's node public key
        bolt12_offer: BOLT12 offer string

    Returns:
        String to be signed with signmessage()
    """
    return f"settlement_offer:{peer_id}:{bolt12_offer}"


# =============================================================================
# FEE REPORT MESSAGES (Real-time fee earnings for settlement)
# =============================================================================

def create_fee_report(
    peer_id: str,
    fees_earned_sats: int,
    period_start: int,
    period_end: int,
    forward_count: int,
    signature: str,
    rebalance_costs_sats: int = 0
) -> bytes:
    """
    Create a FEE_REPORT message to broadcast fee earnings.

    This message is broadcast when a node earns routing fees to keep
    fleet settlement calculations accurate in near real-time.

    Args:
        peer_id: Member's node public key
        fees_earned_sats: Cumulative fees earned in sats for the period
        period_start: Unix timestamp of period start
        period_end: Unix timestamp of period end (current time)
        forward_count: Number of forwards completed
        signature: zbase-encoded signature of the fee report payload
        rebalance_costs_sats: Rebalancing costs in the period (for net profit settlement)

    Returns:
        Serialized FEE_REPORT message
    """
    payload = {
        "peer_id": peer_id,
        "fees_earned_sats": fees_earned_sats,
        "rebalance_costs_sats": rebalance_costs_sats,
        "period_start": period_start,
        "period_end": period_end,
        "forward_count": forward_count,
        "signature": signature,
    }

    return serialize(HiveMessageType.FEE_REPORT, payload)


def get_fee_report_signing_payload(
    peer_id: str,
    fees_earned_sats: int,
    period_start: int,
    period_end: int,
    forward_count: int,
    rebalance_costs_sats: int = 0
) -> str:
    """
    Get the canonical payload for signing a fee report.

    Args:
        peer_id: Member's node public key
        fees_earned_sats: Cumulative fees earned
        period_start: Period start timestamp
        period_end: Period end timestamp
        forward_count: Number of forwards
        rebalance_costs_sats: Rebalancing costs in the period

    Returns:
        String to be signed with signmessage()
    """
    # Include costs in signed payload for verification
    return f"fee_report:{peer_id}:{fees_earned_sats}:{rebalance_costs_sats}:{period_start}:{period_end}:{forward_count}"


def get_fee_report_signing_payload_legacy(
    peer_id: str,
    fees_earned_sats: int,
    period_start: int,
    period_end: int,
    forward_count: int
) -> str:
    """
    Get the legacy signing payload (without costs) for backward compatibility.

    Used to verify signatures from old nodes that don't include rebalance_costs.

    Args:
        peer_id: Member's node public key
        fees_earned_sats: Cumulative fees earned
        period_start: Period start timestamp
        period_end: Period end timestamp
        forward_count: Number of forwards

    Returns:
        String in legacy format for signature verification
    """
    return f"fee_report:{peer_id}:{fees_earned_sats}:{period_start}:{period_end}:{forward_count}"


def validate_fee_report(payload: Dict[str, Any]) -> bool:
    """
    Validate FEE_REPORT payload schema.

    Args:
        payload: Decoded FEE_REPORT payload

    Returns:
        True if valid, False otherwise
    """
    required = ["peer_id", "fees_earned_sats", "period_start", "period_end",
                "forward_count", "signature"]

    for field in required:
        if field not in payload:
            return False

    # Type checks
    if not isinstance(payload["peer_id"], str):
        return False
    if not isinstance(payload["fees_earned_sats"], int):
        return False
    if not isinstance(payload["period_start"], int):
        return False
    if not isinstance(payload["period_end"], int):
        return False
    if not isinstance(payload["forward_count"], int):
        return False
    if not isinstance(payload["signature"], str):
        return False

    # Optional field validation: rebalance_costs_sats (backward compat - defaults to 0)
    if "rebalance_costs_sats" in payload:
        if not isinstance(payload["rebalance_costs_sats"], int):
            return False
        if payload["rebalance_costs_sats"] < 0:
            return False

    # Bounds checks
    if len(payload["peer_id"]) > MAX_PEER_ID_LEN:
        return False
    if payload["fees_earned_sats"] < 0:
        return False
    if payload["forward_count"] < 0:
        return False
    if payload["period_end"] < payload["period_start"]:
        return False

    return True


# =============================================================================
# PHASE 10: TASK DELEGATION PROTOCOL
# =============================================================================
#
# Enables hive members to delegate tasks to each other when they can't
# complete them directly (e.g., peer rejects connection from node A,
# so A asks node B to try opening the channel instead).
#

# Task types supported
TASK_TYPE_EXPAND_TO = "expand_to"           # Open channel to a target peer
TASK_TYPE_REBALANCE_THROUGH = "rebalance"   # Coordinate rebalancing (future)

VALID_TASK_TYPES = {TASK_TYPE_EXPAND_TO, TASK_TYPE_REBALANCE_THROUGH}

# Task priorities
TASK_PRIORITY_LOW = "low"
TASK_PRIORITY_NORMAL = "normal"
TASK_PRIORITY_HIGH = "high"
TASK_PRIORITY_URGENT = "urgent"

VALID_TASK_PRIORITIES = {
    TASK_PRIORITY_LOW,
    TASK_PRIORITY_NORMAL,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_URGENT
}

# Task response statuses
TASK_STATUS_ACCEPTED = "accepted"       # Will attempt the task
TASK_STATUS_REJECTED = "rejected"       # Cannot/won't do the task
TASK_STATUS_COMPLETED = "completed"     # Task finished successfully
TASK_STATUS_FAILED = "failed"           # Task attempted but failed

VALID_TASK_STATUSES = {
    TASK_STATUS_ACCEPTED,
    TASK_STATUS_REJECTED,
    TASK_STATUS_COMPLETED,
    TASK_STATUS_FAILED
}

# Rejection reasons
TASK_REJECT_BUSY = "busy"                   # Too many pending tasks
TASK_REJECT_NO_FUNDS = "insufficient_funds" # Not enough on-chain/channel funds
TASK_REJECT_NO_CONNECTION = "no_connection" # Can't connect to target either
TASK_REJECT_POLICY = "policy"               # Policy prevents this task
TASK_REJECT_INVALID = "invalid_request"     # Malformed request

# Compensation types
COMPENSATION_NONE = "none"              # No compensation expected
COMPENSATION_RECIPROCAL = "reciprocal"  # Requester will do a favor in return
COMPENSATION_FEE = "fee"                # Pay a fee for the service

VALID_COMPENSATION_TYPES = {COMPENSATION_NONE, COMPENSATION_RECIPROCAL, COMPENSATION_FEE}

# Rate limits
TASK_REQUEST_RATE_LIMIT = (5, 3600)     # 5 requests per hour per sender
TASK_RESPONSE_RATE_LIMIT = (10, 3600)   # 10 responses per hour per sender

# Limits
MAX_PENDING_TASKS = 10                  # Max tasks a node will accept at once
MAX_REQUEST_ID_LENGTH = 128             # Max length of request_id
TASK_REQUEST_MAX_AGE = 300              # 5 minute freshness window
TASK_DEFAULT_DEADLINE_HOURS = 1         # Default deadline if not specified


def get_task_request_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for TASK_REQUEST messages.

    Args:
        payload: TASK_REQUEST message payload

    Returns:
        Canonical string for signmessage()
    """
    # Include key fields that must not be tampered with
    task_params = payload.get("task_params", {})
    return (
        f"TASK_REQUEST:"
        f"{payload.get('requester_id', '')}:"
        f"{payload.get('request_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{payload.get('task_type', '')}:"
        f"{task_params.get('target', '')}:"
        f"{task_params.get('amount_sats', 0)}:"
        f"{payload.get('deadline_timestamp', 0)}"
    )


def get_task_response_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for TASK_RESPONSE messages.

    Args:
        payload: TASK_RESPONSE message payload

    Returns:
        Canonical string for signmessage()
    """
    return (
        f"TASK_RESPONSE:"
        f"{payload.get('responder_id', '')}:"
        f"{payload.get('request_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{payload.get('status', '')}"
    )


def validate_task_request_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a TASK_REQUEST payload.

    SECURITY: Bounds all values to prevent manipulation.

    Args:
        payload: TASK_REQUEST message payload

    Returns:
        True if valid, False otherwise
    """
    import time as time_module

    # Required fields
    required = ["requester_id", "request_id", "timestamp", "task_type",
                "task_params", "priority", "deadline_timestamp", "signature"]

    for field in required:
        if field not in payload:
            return False

    # Type checks
    if not isinstance(payload["requester_id"], str):
        return False
    if not isinstance(payload["request_id"], str):
        return False
    if not isinstance(payload["timestamp"], int):
        return False
    if not isinstance(payload["task_type"], str):
        return False
    if not isinstance(payload["task_params"], dict):
        return False
    if not isinstance(payload["priority"], str):
        return False
    if not isinstance(payload["deadline_timestamp"], int):
        return False
    if not isinstance(payload["signature"], str):
        return False

    # Validate task type
    if payload["task_type"] not in VALID_TASK_TYPES:
        return False

    # Validate priority
    if payload["priority"] not in VALID_TASK_PRIORITIES:
        return False

    # Bounds checks
    if len(payload["requester_id"]) > MAX_PEER_ID_LEN:
        return False
    if len(payload["request_id"]) > MAX_REQUEST_ID_LENGTH:
        return False
    if len(payload["signature"]) < 10:
        return False

    # Timestamp freshness
    now = int(time_module.time())
    if abs(now - payload["timestamp"]) > TASK_REQUEST_MAX_AGE:
        return False

    # Deadline must be in the future
    if payload["deadline_timestamp"] <= now:
        return False

    # Validate task_params based on task_type
    task_params = payload["task_params"]

    if payload["task_type"] == TASK_TYPE_EXPAND_TO:
        # expand_to requires target and amount_sats
        if "target" not in task_params or not isinstance(task_params["target"], str):
            return False
        if "amount_sats" not in task_params or not isinstance(task_params["amount_sats"], int):
            return False
        if len(task_params["target"]) > MAX_PEER_ID_LEN:
            return False
        if task_params["amount_sats"] < 100000 or task_params["amount_sats"] > 10_000_000_000:
            return False

    # Validate compensation if present
    compensation = payload.get("compensation", {})
    if compensation:
        comp_type = compensation.get("type", COMPENSATION_NONE)
        if comp_type not in VALID_COMPENSATION_TYPES:
            return False

    return True


def validate_task_response_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate a TASK_RESPONSE payload.

    Args:
        payload: TASK_RESPONSE message payload

    Returns:
        True if valid, False otherwise
    """
    import time as time_module

    # Required fields
    required = ["responder_id", "request_id", "timestamp", "status", "signature"]

    for field in required:
        if field not in payload:
            return False

    # Type checks
    if not isinstance(payload["responder_id"], str):
        return False
    if not isinstance(payload["request_id"], str):
        return False
    if not isinstance(payload["timestamp"], int):
        return False
    if not isinstance(payload["status"], str):
        return False
    if not isinstance(payload["signature"], str):
        return False

    # Validate status
    if payload["status"] not in VALID_TASK_STATUSES:
        return False

    # Bounds checks
    if len(payload["responder_id"]) > MAX_PEER_ID_LEN:
        return False
    if len(payload["request_id"]) > MAX_REQUEST_ID_LENGTH:
        return False
    if len(payload["signature"]) < 10:
        return False

    # Timestamp freshness (responses can be slightly older due to task execution time)
    now = int(time_module.time())
    if abs(now - payload["timestamp"]) > 3600:  # 1 hour tolerance for responses
        return False

    # If rejected, reason should be present
    if payload["status"] == TASK_STATUS_REJECTED:
        reason = payload.get("reason", "")
        if not reason or not isinstance(reason, str):
            return False

    # If completed, result should be present
    if payload["status"] == TASK_STATUS_COMPLETED:
        result = payload.get("result", {})
        if not isinstance(result, dict):
            return False

    return True


def create_task_request(
    requester_id: str,
    request_id: str,
    timestamp: int,
    task_type: str,
    task_params: Dict[str, Any],
    priority: str,
    deadline_timestamp: int,
    rpc,
    compensation: Optional[Dict[str, Any]] = None,
    failure_context: Optional[Dict[str, Any]] = None
) -> Optional[bytes]:
    """
    Create a signed TASK_REQUEST message.

    Args:
        requester_id: Our node pubkey
        request_id: Unique request identifier
        timestamp: Unix timestamp
        task_type: Type of task (expand_to, rebalance, etc.)
        task_params: Task-specific parameters
        priority: Task priority level
        deadline_timestamp: When the task should be completed by
        rpc: RPC proxy for signmessage
        compensation: Optional compensation offer
        failure_context: Optional context about why we're delegating

    Returns:
        Serialized message bytes, or None on error
    """
    payload = {
        "requester_id": requester_id,
        "request_id": request_id,
        "timestamp": timestamp,
        "task_type": task_type,
        "task_params": task_params,
        "priority": priority,
        "deadline_timestamp": deadline_timestamp,
        "compensation": compensation or {"type": COMPENSATION_RECIPROCAL},
        "signature": ""  # Placeholder for validation
    }

    # Add failure context if provided (helps responder understand why)
    if failure_context:
        payload["failure_context"] = failure_context

    # Validate before signing
    if not validate_task_request_payload(payload):
        return None

    # Sign the message
    signing_payload = get_task_request_signing_payload(payload)
    try:
        sign_result = rpc.signmessage(signing_payload)
        payload["signature"] = sign_result.get("zbase", "")
    except Exception:
        return None

    return serialize(HiveMessageType.TASK_REQUEST, payload)


def create_task_response(
    responder_id: str,
    request_id: str,
    timestamp: int,
    status: str,
    rpc,
    reason: Optional[str] = None,
    result: Optional[Dict[str, Any]] = None
) -> Optional[bytes]:
    """
    Create a signed TASK_RESPONSE message.

    Args:
        responder_id: Our node pubkey
        request_id: Original request ID we're responding to
        timestamp: Unix timestamp
        status: Response status (accepted/rejected/completed/failed)
        rpc: RPC proxy for signmessage
        reason: Reason for rejection/failure (required if rejected/failed)
        result: Task result (required if completed)

    Returns:
        Serialized message bytes, or None on error
    """
    payload = {
        "responder_id": responder_id,
        "request_id": request_id,
        "timestamp": timestamp,
        "status": status,
        "signature": ""  # Placeholder
    }

    if reason:
        payload["reason"] = reason

    if result:
        payload["result"] = result

    # Validate before signing
    if not validate_task_response_payload(payload):
        return None

    # Sign the message
    signing_payload = get_task_response_signing_payload(payload)
    try:
        sign_result = rpc.signmessage(signing_payload)
        payload["signature"] = sign_result.get("zbase", "")
    except Exception:
        return None

    return serialize(HiveMessageType.TASK_RESPONSE, payload)


# =============================================================================
# PHASE 11: SPLICE COORDINATION CONSTANTS
# =============================================================================

# Splice session timeout (5 minutes)
SPLICE_SESSION_TIMEOUT_SECONDS = 300

# Valid splice types
SPLICE_TYPE_IN = "splice_in"
SPLICE_TYPE_OUT = "splice_out"
VALID_SPLICE_TYPES = {SPLICE_TYPE_IN, SPLICE_TYPE_OUT}

# Splice session statuses
SPLICE_STATUS_PENDING = "pending"
SPLICE_STATUS_INIT_SENT = "init_sent"
SPLICE_STATUS_INIT_RECEIVED = "init_received"
SPLICE_STATUS_UPDATING = "updating"
SPLICE_STATUS_SIGNING = "signing"
SPLICE_STATUS_COMPLETED = "completed"
SPLICE_STATUS_ABORTED = "aborted"
SPLICE_STATUS_FAILED = "failed"
VALID_SPLICE_STATUSES = {
    SPLICE_STATUS_PENDING, SPLICE_STATUS_INIT_SENT, SPLICE_STATUS_INIT_RECEIVED,
    SPLICE_STATUS_UPDATING, SPLICE_STATUS_SIGNING, SPLICE_STATUS_COMPLETED,
    SPLICE_STATUS_ABORTED, SPLICE_STATUS_FAILED
}

# Splice rejection reasons
SPLICE_REJECT_NOT_MEMBER = "not_member"
SPLICE_REJECT_NO_CHANNEL = "no_channel"
SPLICE_REJECT_CHANNEL_BUSY = "channel_busy"
SPLICE_REJECT_SAFETY_BLOCKED = "safety_blocked"
SPLICE_REJECT_NO_SPLICING = "no_splicing_enabled"
SPLICE_REJECT_INSUFFICIENT_FUNDS = "insufficient_funds"
SPLICE_REJECT_INVALID_AMOUNT = "invalid_amount"
SPLICE_REJECT_SESSION_EXISTS = "session_exists"
SPLICE_REJECT_DECLINED = "declined"

# Splice abort reasons
SPLICE_ABORT_TIMEOUT = "timeout"
SPLICE_ABORT_USER_CANCELLED = "user_cancelled"
SPLICE_ABORT_RPC_ERROR = "rpc_error"
SPLICE_ABORT_INVALID_PSBT = "invalid_psbt"
SPLICE_ABORT_SIGNATURE_FAILED = "signature_failed"

# Rate limits
SPLICE_INIT_REQUEST_RATE_LIMIT = (5, 3600)  # 5 per hour per sender
SPLICE_MESSAGE_RATE_LIMIT = (20, 3600)  # 20 per hour per session

# Maximum PSBT size (500KB base64 encoded)
MAX_PSBT_SIZE = 500_000


# =============================================================================
# PHASE 11: SPLICE VALIDATION FUNCTIONS
# =============================================================================

def validate_splice_init_request_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate SPLICE_INIT_REQUEST payload schema.

    SECURITY: Requires cryptographic signature from the initiator.
    """
    if not isinstance(payload, dict):
        return False

    initiator_id = payload.get("initiator_id")
    session_id = payload.get("session_id")
    channel_id = payload.get("channel_id")
    splice_type = payload.get("splice_type")
    amount_sats = payload.get("amount_sats")
    feerate_perkw = payload.get("feerate_perkw")
    psbt = payload.get("psbt")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # initiator_id must be valid pubkey
    if not _valid_pubkey(initiator_id):
        return False

    # session_id must be valid hex string
    if not isinstance(session_id, str) or not session_id or len(session_id) > MAX_REQUEST_ID_LEN:
        return False

    # channel_id must be present
    if not isinstance(channel_id, str) or not channel_id:
        return False

    # splice_type must be valid
    if splice_type not in VALID_SPLICE_TYPES:
        return False

    # amount_sats must be positive integer
    if not isinstance(amount_sats, int) or amount_sats <= 0:
        return False

    # feerate_perkw is optional but must be positive if present
    if feerate_perkw is not None:
        if not isinstance(feerate_perkw, int) or feerate_perkw <= 0:
            return False

    # psbt must be present and within size limit
    if not isinstance(psbt, str) or not psbt or len(psbt) > MAX_PSBT_SIZE:
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    return True


def get_splice_init_request_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing SPLICE_INIT_REQUEST messages.
    """
    signing_fields = {
        "initiator_id": payload.get("initiator_id", ""),
        "session_id": payload.get("session_id", ""),
        "channel_id": payload.get("channel_id", ""),
        "splice_type": payload.get("splice_type", ""),
        "amount_sats": payload.get("amount_sats", 0),
        "timestamp": payload.get("timestamp", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def validate_splice_init_response_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate SPLICE_INIT_RESPONSE payload schema.

    SECURITY: Requires cryptographic signature from the responder.
    """
    if not isinstance(payload, dict):
        return False

    responder_id = payload.get("responder_id")
    session_id = payload.get("session_id")
    accepted = payload.get("accepted")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # responder_id must be valid pubkey
    if not _valid_pubkey(responder_id):
        return False

    # session_id must be present
    if not isinstance(session_id, str) or not session_id:
        return False

    # accepted must be boolean
    if not isinstance(accepted, bool):
        return False

    # If accepted, psbt is optional (CLN handles PSBT exchange internally)
    if accepted:
        psbt = payload.get("psbt")
        if psbt is not None and (not isinstance(psbt, str) or len(psbt) > MAX_PSBT_SIZE):
            return False

    # If rejected, reason should be present
    if not accepted:
        reason = payload.get("reason")
        if reason is not None and (not isinstance(reason, str) or len(reason) > 200):
            return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    return True


def get_splice_init_response_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing SPLICE_INIT_RESPONSE messages.
    """
    signing_fields = {
        "responder_id": payload.get("responder_id", ""),
        "session_id": payload.get("session_id", ""),
        "accepted": payload.get("accepted", False),
        "timestamp": payload.get("timestamp", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def validate_splice_update_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate SPLICE_UPDATE payload schema.

    SECURITY: Requires cryptographic signature.
    """
    if not isinstance(payload, dict):
        return False

    sender_id = payload.get("sender_id")
    session_id = payload.get("session_id")
    psbt = payload.get("psbt")
    commitments_secured = payload.get("commitments_secured")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # sender_id must be valid pubkey
    if not _valid_pubkey(sender_id):
        return False

    # session_id must be present
    if not isinstance(session_id, str) or not session_id:
        return False

    # psbt must be present and within size limit
    if not isinstance(psbt, str) or not psbt or len(psbt) > MAX_PSBT_SIZE:
        return False

    # commitments_secured must be boolean
    if not isinstance(commitments_secured, bool):
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    return True


def get_splice_update_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing SPLICE_UPDATE messages.
    """
    signing_fields = {
        "sender_id": payload.get("sender_id", ""),
        "session_id": payload.get("session_id", ""),
        "commitments_secured": payload.get("commitments_secured", False),
        "timestamp": payload.get("timestamp", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def validate_splice_signed_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate SPLICE_SIGNED payload schema.

    SECURITY: Requires cryptographic signature.
    """
    if not isinstance(payload, dict):
        return False

    sender_id = payload.get("sender_id")
    session_id = payload.get("session_id")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # sender_id must be valid pubkey
    if not _valid_pubkey(sender_id):
        return False

    # session_id must be present
    if not isinstance(session_id, str) or not session_id:
        return False

    # Either signed_psbt or txid must be present
    signed_psbt = payload.get("signed_psbt")
    txid = payload.get("txid")

    if signed_psbt is not None:
        if not isinstance(signed_psbt, str) or len(signed_psbt) > MAX_PSBT_SIZE:
            return False
    elif txid is not None:
        if not isinstance(txid, str) or len(txid) != 64:
            return False
    else:
        return False  # At least one must be present

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    return True


def get_splice_signed_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing SPLICE_SIGNED messages.
    """
    signing_fields = {
        "sender_id": payload.get("sender_id", ""),
        "session_id": payload.get("session_id", ""),
        "timestamp": payload.get("timestamp", 0),
        "has_txid": payload.get("txid") is not None,
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def validate_splice_abort_payload(payload: Dict[str, Any]) -> bool:
    """
    Validate SPLICE_ABORT payload schema.

    SECURITY: Requires cryptographic signature.
    """
    if not isinstance(payload, dict):
        return False

    sender_id = payload.get("sender_id")
    session_id = payload.get("session_id")
    reason = payload.get("reason")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # sender_id must be valid pubkey
    if not _valid_pubkey(sender_id):
        return False

    # session_id must be present
    if not isinstance(session_id, str) or not session_id:
        return False

    # reason must be a string
    if not isinstance(reason, str) or len(reason) > 500:
        return False

    # timestamp must be positive integer
    if not isinstance(timestamp, int) or timestamp < 0:
        return False

    # SECURITY: Signature must be present
    if not isinstance(signature, str) or len(signature) < 10:
        return False

    return True


def get_splice_abort_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing SPLICE_ABORT messages.
    """
    signing_fields = {
        "sender_id": payload.get("sender_id", ""),
        "session_id": payload.get("session_id", ""),
        "reason": payload.get("reason", ""),
        "timestamp": payload.get("timestamp", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


# =============================================================================
# PHASE 11: SPLICE MESSAGE CREATION FUNCTIONS
# =============================================================================

def create_splice_init_request(
    initiator_id: str,
    session_id: str,
    channel_id: str,
    splice_type: str,
    amount_sats: int,
    psbt: str,
    timestamp: int,
    rpc,
    feerate_perkw: Optional[int] = None
) -> Optional[bytes]:
    """
    Create a signed SPLICE_INIT_REQUEST message.

    Args:
        initiator_id: Our node pubkey
        session_id: Unique session identifier
        channel_id: Channel to splice
        splice_type: 'splice_in' or 'splice_out'
        amount_sats: Amount to splice (positive)
        psbt: Initial PSBT from splice_init
        timestamp: Unix timestamp
        rpc: RPC proxy for signmessage
        feerate_perkw: Optional feerate

    Returns:
        Serialized message bytes, or None on error
    """
    payload = {
        "initiator_id": initiator_id,
        "session_id": session_id,
        "channel_id": channel_id,
        "splice_type": splice_type,
        "amount_sats": amount_sats,
        "psbt": psbt,
        "timestamp": timestamp,
        "signature": ""  # Placeholder
    }

    if feerate_perkw is not None:
        payload["feerate_perkw"] = feerate_perkw

    # Sign the message (validation happens on receipt)
    signing_payload = get_splice_init_request_signing_payload(payload)
    try:
        sign_result = rpc.signmessage(signing_payload)
        payload["signature"] = sign_result.get("zbase", "")
    except Exception:
        return None

    return serialize(HiveMessageType.SPLICE_INIT_REQUEST, payload)


def create_splice_init_response(
    responder_id: str,
    session_id: str,
    accepted: bool,
    timestamp: int,
    rpc,
    psbt: Optional[str] = None,
    reason: Optional[str] = None
) -> Optional[bytes]:
    """
    Create a signed SPLICE_INIT_RESPONSE message.

    Args:
        responder_id: Our node pubkey
        session_id: Session we're responding to
        accepted: Whether we accept the splice
        timestamp: Unix timestamp
        rpc: RPC proxy for signmessage
        psbt: Updated PSBT (required if accepted)
        reason: Rejection reason (if not accepted)

    Returns:
        Serialized message bytes, or None on error
    """
    payload = {
        "responder_id": responder_id,
        "session_id": session_id,
        "accepted": accepted,
        "timestamp": timestamp,
        "signature": ""  # Placeholder
    }

    if accepted and psbt:
        payload["psbt"] = psbt
    if not accepted and reason:
        payload["reason"] = reason

    # Sign the message (validation happens on receipt)
    signing_payload = get_splice_init_response_signing_payload(payload)
    try:
        sign_result = rpc.signmessage(signing_payload)
        payload["signature"] = sign_result.get("zbase", "")
    except Exception:
        return None

    return serialize(HiveMessageType.SPLICE_INIT_RESPONSE, payload)


def create_splice_update(
    sender_id: str,
    session_id: str,
    psbt: str,
    commitments_secured: bool,
    timestamp: int,
    rpc
) -> Optional[bytes]:
    """
    Create a signed SPLICE_UPDATE message.

    Args:
        sender_id: Our node pubkey
        session_id: Session ID
        psbt: Updated PSBT
        commitments_secured: Whether commitments are secured
        timestamp: Unix timestamp
        rpc: RPC proxy for signmessage

    Returns:
        Serialized message bytes, or None on error
    """
    payload = {
        "sender_id": sender_id,
        "session_id": session_id,
        "psbt": psbt,
        "commitments_secured": commitments_secured,
        "timestamp": timestamp,
        "signature": ""  # Placeholder
    }

    # Sign the message (validation happens on receipt)
    signing_payload = get_splice_update_signing_payload(payload)
    try:
        sign_result = rpc.signmessage(signing_payload)
        payload["signature"] = sign_result.get("zbase", "")
    except Exception:
        return None

    return serialize(HiveMessageType.SPLICE_UPDATE, payload)


def create_splice_signed(
    sender_id: str,
    session_id: str,
    timestamp: int,
    rpc,
    signed_psbt: Optional[str] = None,
    txid: Optional[str] = None
) -> Optional[bytes]:
    """
    Create a signed SPLICE_SIGNED message.

    Args:
        sender_id: Our node pubkey
        session_id: Session ID
        timestamp: Unix timestamp
        rpc: RPC proxy for signmessage
        signed_psbt: Final signed PSBT
        txid: Transaction ID if already broadcast

    Returns:
        Serialized message bytes, or None on error
    """
    payload = {
        "sender_id": sender_id,
        "session_id": session_id,
        "timestamp": timestamp,
        "signature": ""  # Placeholder
    }

    if signed_psbt:
        payload["signed_psbt"] = signed_psbt
    if txid:
        payload["txid"] = txid

    # Sign the message (validation happens on receipt)
    signing_payload = get_splice_signed_signing_payload(payload)
    try:
        sign_result = rpc.signmessage(signing_payload)
        payload["signature"] = sign_result.get("zbase", "")
    except Exception:
        return None

    return serialize(HiveMessageType.SPLICE_SIGNED, payload)


def create_splice_abort(
    sender_id: str,
    session_id: str,
    reason: str,
    timestamp: int,
    rpc
) -> Optional[bytes]:
    """
    Create a signed SPLICE_ABORT message.

    Args:
        sender_id: Our node pubkey
        session_id: Session to abort
        reason: Abort reason
        timestamp: Unix timestamp
        rpc: RPC proxy for signmessage

    Returns:
        Serialized message bytes, or None on error
    """
    payload = {
        "sender_id": sender_id,
        "session_id": session_id,
        "reason": reason,
        "timestamp": timestamp,
        "signature": ""  # Placeholder
    }

    # Sign the message (validation happens on receipt)
    signing_payload = get_splice_abort_signing_payload(payload)
    try:
        sign_result = rpc.signmessage(signing_payload)
        payload["signature"] = sign_result.get("zbase", "")
    except Exception:
        return None

    return serialize(HiveMessageType.SPLICE_ABORT, payload)


# =============================================================================
# PHASE 12: DISTRIBUTED SETTLEMENT MESSAGES
# =============================================================================

def validate_settlement_propose(payload: Dict[str, Any]) -> bool:
    """
    Validate SETTLEMENT_PROPOSE payload schema.

    Args:
        payload: Decoded SETTLEMENT_PROPOSE payload

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(payload, dict):
        return False

    required = ["proposal_id", "period", "proposer_peer_id", "timestamp",
                "data_hash", "plan_hash", "total_fees_sats", "member_count",
                "contributions", "signature"]

    for field in required:
        if field not in payload:
            return False

    # Validate types
    if not isinstance(payload["proposal_id"], str) or len(payload["proposal_id"]) > 64:
        return False
    if not isinstance(payload["period"], str) or len(payload["period"]) > 10:
        return False
    if not _valid_pubkey(payload["proposer_peer_id"]):
        return False
    if not isinstance(payload["timestamp"], int) or payload["timestamp"] < 0:
        return False
    if not isinstance(payload["data_hash"], str) or len(payload["data_hash"]) != 64:
        return False
    if not isinstance(payload["plan_hash"], str) or len(payload["plan_hash"]) != 64:
        return False
    if not isinstance(payload["total_fees_sats"], int) or payload["total_fees_sats"] < 0:
        return False
    if not isinstance(payload["member_count"], int) or payload["member_count"] < 1:
        return False
    if not isinstance(payload["contributions"], list):
        return False
    if not isinstance(payload["signature"], str) or len(payload["signature"]) < 10:
        return False

    # Validate contributions list (limit to prevent DoS)
    if len(payload["contributions"]) > 100:
        return False

    for contrib in payload["contributions"]:
        if not isinstance(contrib, dict):
            return False
        if not _valid_pubkey(contrib.get("peer_id", "")):
            return False
        fees_earned = contrib.get("fees_earned", 0)
        if not isinstance(fees_earned, int) or fees_earned < 0:
            return False
        rebalance_costs = contrib.get("rebalance_costs", 0)
        if not isinstance(rebalance_costs, int) or rebalance_costs < 0:
            return False
        forward_count = contrib.get("forward_count", 0)
        if not isinstance(forward_count, int) or forward_count < 0:
            return False
        uptime = contrib.get("uptime", 100)
        if not isinstance(uptime, int) or not (0 <= uptime <= 100):
            return False
        capacity = contrib.get("capacity", 0)
        if not isinstance(capacity, int) or capacity < 0:
            return False

    return True


def validate_settlement_ready(payload: Dict[str, Any]) -> bool:
    """
    Validate SETTLEMENT_READY payload schema.

    Args:
        payload: Decoded SETTLEMENT_READY payload

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(payload, dict):
        return False

    required = ["proposal_id", "voter_peer_id", "data_hash", "timestamp", "signature"]

    for field in required:
        if field not in payload:
            return False

    if not isinstance(payload["proposal_id"], str) or len(payload["proposal_id"]) > 64:
        return False
    if not _valid_pubkey(payload["voter_peer_id"]):
        return False
    if not isinstance(payload["data_hash"], str) or len(payload["data_hash"]) != 64:
        return False
    if not isinstance(payload["timestamp"], int) or payload["timestamp"] < 0:
        return False
    if not isinstance(payload["signature"], str) or len(payload["signature"]) < 10:
        return False

    return True


def validate_settlement_executed(payload: Dict[str, Any]) -> bool:
    """
    Validate SETTLEMENT_EXECUTED payload schema.

    Args:
        payload: Decoded SETTLEMENT_EXECUTED payload

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(payload, dict):
        return False

    required = ["proposal_id", "executor_peer_id", "timestamp", "signature"]

    for field in required:
        if field not in payload:
            return False

    if not isinstance(payload["proposal_id"], str) or len(payload["proposal_id"]) > 64:
        return False
    if not _valid_pubkey(payload["executor_peer_id"]):
        return False
    if not isinstance(payload["timestamp"], int) or payload["timestamp"] < 0:
        return False
    if not isinstance(payload["signature"], str) or len(payload["signature"]) < 10:
        return False

    # Optional fields
    if "plan_hash" in payload:
        if not isinstance(payload["plan_hash"], str) or (payload["plan_hash"] and len(payload["plan_hash"]) != 64):
            return False
    if "total_sent_sats" in payload:
        if not isinstance(payload["total_sent_sats"], int) or payload["total_sent_sats"] < 0:
            return False
    if "payment_hash" in payload:
        if not isinstance(payload["payment_hash"], str):
            return False
    if "amount_paid_sats" in payload:
        if not isinstance(payload["amount_paid_sats"], int):
            return False

    return True


def get_settlement_propose_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing SETTLEMENT_PROPOSE messages.

    The signature covers the core fields that define the proposal.
    """
    signing_fields = {
        "proposal_id": payload.get("proposal_id", ""),
        "period": payload.get("period", ""),
        "proposer_peer_id": payload.get("proposer_peer_id", ""),
        "data_hash": payload.get("data_hash", ""),
        "plan_hash": payload.get("plan_hash", ""),
        "total_fees_sats": payload.get("total_fees_sats", 0),
        "member_count": payload.get("member_count", 0),
        "timestamp": payload.get("timestamp", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def get_settlement_ready_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing SETTLEMENT_READY messages.

    The signature covers the voter's hash confirmation.
    """
    signing_fields = {
        "proposal_id": payload.get("proposal_id", ""),
        "voter_peer_id": payload.get("voter_peer_id", ""),
        "data_hash": payload.get("data_hash", ""),
        "timestamp": payload.get("timestamp", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def get_settlement_executed_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical payload string for signing SETTLEMENT_EXECUTED messages.

    The signature covers the execution confirmation.
    """
    signing_fields = {
        "proposal_id": payload.get("proposal_id", ""),
        "executor_peer_id": payload.get("executor_peer_id", ""),
        "plan_hash": payload.get("plan_hash", ""),
        "total_sent_sats": payload.get("total_sent_sats", 0),
        "payment_hash": payload.get("payment_hash", ""),
        "amount_paid_sats": payload.get("amount_paid_sats", 0),
        "timestamp": payload.get("timestamp", 0),
    }
    return json.dumps(signing_fields, sort_keys=True, separators=(',', ':'))


def create_settlement_propose(
    proposal_id: str,
    period: str,
    proposer_peer_id: str,
    data_hash: str,
    plan_hash: str,
    total_fees_sats: int,
    member_count: int,
    contributions: List[Dict[str, Any]],
    timestamp: int,
    signature: str
) -> bytes:
    """
    Create a SETTLEMENT_PROPOSE message.

    This message proposes a settlement for a given period using canonical
    fee data from gossiped FEE_REPORT messages.

    Args:
        proposal_id: Unique identifier for this proposal
        period: Settlement period (YYYY-WW format)
        proposer_peer_id: Node proposing the settlement
        data_hash: Canonical hash of contribution data for verification
        total_fees_sats: Total fees to distribute
        member_count: Number of participating members
        contributions: List of member contribution dicts
        timestamp: Unix timestamp of proposal
        signature: Proposer's signature

    Returns:
        Serialized SETTLEMENT_PROPOSE message
    """
    payload = {
        "proposal_id": proposal_id,
        "period": period,
        "proposer_peer_id": proposer_peer_id,
        "data_hash": data_hash,
        "plan_hash": plan_hash,
        "total_fees_sats": total_fees_sats,
        "member_count": member_count,
        "contributions": contributions,
        "timestamp": timestamp,
        "signature": signature
    }
    return serialize(HiveMessageType.SETTLEMENT_PROPOSE, payload)


def create_settlement_ready(
    proposal_id: str,
    voter_peer_id: str,
    data_hash: str,
    timestamp: int,
    signature: str
) -> bytes:
    """
    Create a SETTLEMENT_READY message.

    This message votes that the sender has verified the data_hash matches
    their own calculation from gossiped FEE_REPORT data.

    Args:
        proposal_id: Proposal being voted on
        voter_peer_id: Node casting the vote
        data_hash: Hash the voter calculated (must match proposal)
        timestamp: Unix timestamp of vote
        signature: Voter's signature

    Returns:
        Serialized SETTLEMENT_READY message
    """
    payload = {
        "proposal_id": proposal_id,
        "voter_peer_id": voter_peer_id,
        "data_hash": data_hash,
        "timestamp": timestamp,
        "signature": signature
    }
    return serialize(HiveMessageType.SETTLEMENT_READY, payload)


def create_settlement_executed(
    proposal_id: str,
    executor_peer_id: str,
    timestamp: int,
    signature: str,
    plan_hash: Optional[str] = None,
    total_sent_sats: Optional[int] = None,
    payment_hash: Optional[str] = None,
    amount_paid_sats: Optional[int] = None
) -> bytes:
    """
    Create a SETTLEMENT_EXECUTED message.

    This message confirms that the sender has executed their settlement
    payment (if they owed money).

    Args:
        proposal_id: Proposal being executed
        executor_peer_id: Node that executed payment
        timestamp: Unix timestamp of execution
        signature: Executor's signature
        payment_hash: Payment hash (if payment was made)
        amount_paid_sats: Amount paid (if payment was made)

    Returns:
        Serialized SETTLEMENT_EXECUTED message
    """
    payload = {
        "proposal_id": proposal_id,
        "executor_peer_id": executor_peer_id,
        "timestamp": timestamp,
        "signature": signature
    }
    if plan_hash is not None:
        payload["plan_hash"] = plan_hash
    if total_sent_sats is not None:
        payload["total_sent_sats"] = total_sent_sats
    if payment_hash is not None:
        payload["payment_hash"] = payment_hash
    if amount_paid_sats is not None:
        payload["amount_paid_sats"] = amount_paid_sats

    return serialize(HiveMessageType.SETTLEMENT_EXECUTED, payload)


# =============================================================================
# PHASE 13: STIGMERGIC MARKER SHARING
# =============================================================================

def get_stigmergic_marker_batch_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for STIGMERGIC_MARKER_BATCH messages.

    Signs over: reporter_id, timestamp, and a hash of the sorted marker data.
    This ensures the entire batch is authenticated without making the
    signing string excessively long.

    Args:
        payload: STIGMERGIC_MARKER_BATCH message payload

    Returns:
        Canonical string for signmessage()
    """
    import hashlib

    markers = payload.get("markers", [])

    # Create deterministic hash of markers
    # Sort by (source, destination, timestamp) for consistency
    sorted_markers = sorted(
        markers,
        key=lambda m: (
            m.get("source_peer_id", ""),
            m.get("destination_peer_id", ""),
            m.get("timestamp", 0)
        )
    )

    # Hash the sorted marker data
    markers_str = json.dumps(sorted_markers, sort_keys=True, separators=(',', ':'))
    markers_hash = hashlib.sha256(markers_str.encode()).hexdigest()[:16]

    return (
        f"STIGMERGIC_MARKER_BATCH:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(markers)}:"
        f"{markers_hash}"
    )


def validate_stigmergic_marker_batch(payload: Dict[str, Any]) -> bool:
    """
    Validate a STIGMERGIC_MARKER_BATCH payload.

    SECURITY: Bounds all values to prevent manipulation and overflow.

    Args:
        payload: STIGMERGIC_MARKER_BATCH message payload

    Returns:
        True if valid, False otherwise
    """
    import time as time_module

    # Required fields
    if not payload.get("reporter_id"):
        return False
    if not payload.get("timestamp"):
        return False
    if not payload.get("signature"):
        return False
    if "markers" not in payload:
        return False

    # Reporter ID must be valid pubkey format
    reporter_id = payload.get("reporter_id", "")
    if not (len(reporter_id) == 66 and reporter_id[:2] in ("02", "03")):
        return False

    # Timestamp must be recent (within 1 hour) and not in future
    now = int(time_module.time())
    timestamp = payload.get("timestamp", 0)
    if not isinstance(timestamp, int):
        return False
    if timestamp > now + 60:  # Allow 60s clock skew
        return False
    if timestamp < now - 3600:  # Not older than 1 hour
        return False

    # Markers list bounds
    markers = payload.get("markers", [])
    if not isinstance(markers, list):
        return False
    if len(markers) > MAX_MARKERS_IN_BATCH:
        return False

    # Validate each marker
    for marker in markers:
        if not isinstance(marker, dict):
            return False

        # Required marker fields
        if not marker.get("source_peer_id"):
            return False
        if not marker.get("destination_peer_id"):
            return False

        # Validate peer ID formats
        src = marker.get("source_peer_id", "")
        dst = marker.get("destination_peer_id", "")
        if not (len(src) == 66 and src[:2] in ("02", "03")):
            return False
        if not (len(dst) == 66 and dst[:2] in ("02", "03")):
            return False

        # Validate fee_ppm bounds
        fee_ppm = marker.get("fee_ppm", 0)
        if not isinstance(fee_ppm, int) or fee_ppm < 0 or fee_ppm > 100000:
            return False

        # Validate volume bounds
        volume_sats = marker.get("volume_sats", 0)
        if not isinstance(volume_sats, int) or volume_sats < 0 or volume_sats > 1_000_000_000:
            return False

        # Validate strength bounds
        strength = marker.get("strength", 0)
        if not isinstance(strength, (int, float)) or strength < 0 or strength > 10:
            return False

        # Validate timestamp
        marker_ts = marker.get("timestamp", 0)
        if not isinstance(marker_ts, (int, float)):
            return False
        # Marker shouldn't be older than 24 hours
        if marker_ts < now - (MAX_MARKER_AGE_HOURS * 3600):
            return False

    return True


def create_stigmergic_marker_batch(
    reporter_id: str,
    timestamp: int,
    signature: str,
    markers: List[Dict[str, Any]]
) -> bytes:
    """
    Create a STIGMERGIC_MARKER_BATCH message.

    This message shares successful/failed routing markers with the fleet,
    enabling indirect coordination on fee levels. Other members read these
    markers and adjust their fees accordingly (stigmergic coordination).

    SECURITY: The signature must be created using signmessage() over the
    canonical payload returned by get_stigmergic_marker_batch_signing_payload().

    Args:
        reporter_id: Hive member sharing markers
        timestamp: Unix timestamp
        signature: zbase-encoded signature from signmessage()
        markers: List of route marker dicts, each containing:
            - source_peer_id: Source of the route
            - destination_peer_id: Destination of the route
            - fee_ppm: Fee charged for this route
            - success: Whether routing succeeded
            - volume_sats: Volume routed
            - timestamp: When the routing occurred
            - strength: Current marker strength (after decay)
            - channel_id: Optional - channel used for routing

    Returns:
        Serialized STIGMERGIC_MARKER_BATCH message
    """
    payload = {
        "reporter_id": reporter_id,
        "timestamp": timestamp,
        "signature": signature,
        "markers": markers,
    }

    return serialize(HiveMessageType.STIGMERGIC_MARKER_BATCH, payload)


# =============================================================================
# PHEROMONE BATCH FUNCTIONS
# =============================================================================

def get_pheromone_batch_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for PHEROMONE_BATCH messages.

    Signs over: reporter_id, timestamp, and a hash of the sorted pheromone data.
    This ensures the entire batch is authenticated without making the
    signing string excessively long.

    Args:
        payload: PHEROMONE_BATCH message payload

    Returns:
        Canonical string for signmessage()
    """
    pheromones = payload.get("pheromones", [])

    # Sort pheromones by peer_id for deterministic ordering
    sorted_pheromones = sorted(pheromones, key=lambda p: p.get("peer_id", ""))

    # Create a condensed hash of the pheromone data
    pheromones_str = json.dumps(sorted_pheromones, sort_keys=True, separators=(',', ':'))
    pheromones_hash = hashlib.sha256(pheromones_str.encode()).hexdigest()[:16]

    return (
        f"PHEROMONE_BATCH:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(pheromones)}:"
        f"{pheromones_hash}"
    )


def validate_pheromone_batch(payload: Dict[str, Any]) -> bool:
    """
    Validate a PHEROMONE_BATCH payload.

    SECURITY: Bounds all values to prevent manipulation and overflow.

    Args:
        payload: PHEROMONE_BATCH message payload

    Returns:
        True if valid, False otherwise
    """
    # Required fields
    reporter_id = payload.get("reporter_id")
    if not reporter_id or not isinstance(reporter_id, str):
        return False
    if len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False
    # Timestamp should be within reasonable range (not in future, not too old)
    now = time.time()
    if timestamp > now + 300:  # 5 min future tolerance
        return False
    if timestamp < now - (48 * 3600):  # 48 hour max age
        return False

    signature = payload.get("signature")
    if not signature or not isinstance(signature, str):
        return False

    pheromones = payload.get("pheromones")
    if not isinstance(pheromones, list):
        return False
    if len(pheromones) > MAX_PHEROMONES_IN_BATCH:
        return False

    # Validate each pheromone entry
    for p in pheromones:
        if not isinstance(p, dict):
            return False

        peer_id = p.get("peer_id")
        if not peer_id or not isinstance(peer_id, str):
            return False
        if len(peer_id) > MAX_PEER_ID_LEN:
            return False

        level = p.get("level")
        if not isinstance(level, (int, float)):
            return False
        if level < 0 or level > 10000:  # Reasonable bounds
            return False

        fee_ppm = p.get("fee_ppm")
        if not isinstance(fee_ppm, int):
            return False
        if fee_ppm < 0 or fee_ppm > 100000:  # Reasonable fee bounds
            return False

        # Optional fields
        if "channel_id" in p:
            channel_id = p.get("channel_id")
            if not isinstance(channel_id, str) or len(channel_id) > 50:
                return False

    return True


def create_pheromone_batch(
    pheromones: List[Dict[str, Any]],
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create a PHEROMONE_BATCH message.

    This message shares pheromone levels (fee memory from successful routing)
    with the fleet, enabling collective learning about what fees work for
    specific external peers.

    Args:
        pheromones: List of pheromone entries, each containing:
            - peer_id: External peer pubkey
            - level: Pheromone level (strength of fee memory)
            - fee_ppm: Fee that earned this pheromone
            - channel_id: Optional - channel ID
        rpc: CLN RPC interface for signing
        our_pubkey: Our node's public key

    Returns:
        Serialized PHEROMONE_BATCH message, or None on error
    """
    timestamp = int(time.time())
    reporter_id = our_pubkey

    # Create payload for signing
    payload = {
        "reporter_id": reporter_id,
        "timestamp": timestamp,
        "signature": "",  # Placeholder
        "pheromones": pheromones,
    }

    # Sign the payload
    try:
        signing_payload = get_pheromone_batch_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.PHEROMONE_BATCH, payload)


# =============================================================================
# YIELD METRICS BATCH FUNCTIONS (Phase 14)
# =============================================================================

def get_yield_metrics_batch_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for YIELD_METRICS_BATCH messages.

    Signs over: reporter_id, timestamp, and a hash of the sorted yield data.
    """
    metrics = payload.get("metrics", [])

    # Sort metrics by peer_id for deterministic ordering
    sorted_metrics = sorted(metrics, key=lambda m: m.get("peer_id", ""))

    # Create a condensed hash of the metrics data
    metrics_str = json.dumps(sorted_metrics, sort_keys=True, separators=(',', ':'))
    metrics_hash = hashlib.sha256(metrics_str.encode()).hexdigest()[:16]

    return (
        f"YIELD_METRICS_BATCH:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(metrics)}:"
        f"{metrics_hash}"
    )


def validate_yield_metrics_batch(payload: Dict[str, Any]) -> bool:
    """
    Validate a YIELD_METRICS_BATCH payload.

    SECURITY: Bounds all values to prevent manipulation and overflow.
    """
    # Required fields
    reporter_id = payload.get("reporter_id")
    if not reporter_id or not isinstance(reporter_id, str):
        return False
    if len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False
    now = time.time()
    if timestamp > now + 300:  # 5 min future tolerance
        return False
    if timestamp < now - (48 * 3600):  # 48 hour max age
        return False

    signature = payload.get("signature")
    if not signature or not isinstance(signature, str):
        return False

    metrics = payload.get("metrics")
    if not isinstance(metrics, list):
        return False
    if len(metrics) > MAX_YIELD_METRICS_IN_BATCH:
        return False

    # Validate each metrics entry
    for m in metrics:
        if not isinstance(m, dict):
            return False

        peer_id = m.get("peer_id")
        if not peer_id or not isinstance(peer_id, str):
            return False
        if len(peer_id) > MAX_PEER_ID_LEN:
            return False

        # ROI can be negative (underwater channels)
        roi_pct = m.get("roi_pct")
        if not isinstance(roi_pct, (int, float)):
            return False
        if roi_pct < -1000 or roi_pct > 10000:  # Reasonable bounds
            return False

        # Capital efficiency should be small positive
        capital_efficiency = m.get("capital_efficiency")
        if not isinstance(capital_efficiency, (int, float)):
            return False
        if capital_efficiency < -1 or capital_efficiency > 1:  # Per-sat efficiency
            return False

        # Flow intensity 0-1
        flow_intensity = m.get("flow_intensity")
        if not isinstance(flow_intensity, (int, float)):
            return False
        if flow_intensity < 0 or flow_intensity > 1:
            return False

        # Profitability tier
        tier = m.get("profitability_tier")
        if tier not in ("profitable", "underwater", "zombie", "stagnant", "unknown"):
            return False

    return True


def create_yield_metrics_batch(
    metrics: List[Dict[str, Any]],
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create a YIELD_METRICS_BATCH message.

    This message shares per-channel profitability metrics with the fleet,
    enabling collective learning about which external peers are profitable.

    Args:
        metrics: List of yield metric entries, each containing:
            - peer_id: External peer pubkey
            - channel_id: Channel short ID
            - roi_pct: Return on investment percentage
            - capital_efficiency: Revenue per sat of capacity
            - flow_intensity: Volume / capacity ratio
            - profitability_tier: profitable/underwater/zombie/stagnant
            - period_days: Analysis period
        rpc: CLN RPC interface for signing
        our_pubkey: Our node's public key

    Returns:
        Serialized YIELD_METRICS_BATCH message, or None on error
    """
    timestamp = int(time.time())
    reporter_id = our_pubkey

    payload = {
        "reporter_id": reporter_id,
        "timestamp": timestamp,
        "signature": "",
        "metrics": metrics,
    }

    try:
        signing_payload = get_yield_metrics_batch_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.YIELD_METRICS_BATCH, payload)


# =============================================================================
# CIRCULAR FLOW ALERT FUNCTIONS (Phase 14)
# =============================================================================

def get_circular_flow_alert_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for CIRCULAR_FLOW_ALERT messages.
    """
    members = payload.get("members_involved", [])
    members_str = ",".join(sorted(members))

    return (
        f"CIRCULAR_FLOW_ALERT:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{payload.get('total_amount_sats', 0)}:"
        f"{payload.get('total_cost_sats', 0)}:"
        f"{members_str}"
    )


def validate_circular_flow_alert(payload: Dict[str, Any]) -> bool:
    """
    Validate a CIRCULAR_FLOW_ALERT payload.
    """
    reporter_id = payload.get("reporter_id")
    if not reporter_id or not isinstance(reporter_id, str):
        return False
    if len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False
    now = time.time()
    if timestamp > now + 300:
        return False
    if timestamp < now - (24 * 3600):  # 24 hour max age for alerts
        return False

    signature = payload.get("signature")
    if not signature or not isinstance(signature, str):
        return False

    members = payload.get("members_involved")
    if not isinstance(members, list):
        return False
    if len(members) < 2 or len(members) > MAX_CIRCULAR_FLOW_MEMBERS:
        return False
    for m in members:
        if not isinstance(m, str) or len(m) > MAX_PEER_ID_LEN:
            return False

    total_amount = payload.get("total_amount_sats")
    if not isinstance(total_amount, int) or total_amount < 0:
        return False
    if total_amount > 100_000_000_000:  # 1000 BTC max
        return False

    total_cost = payload.get("total_cost_sats")
    if not isinstance(total_cost, int) or total_cost < 0:
        return False

    cycle_count = payload.get("cycle_count")
    if not isinstance(cycle_count, int) or cycle_count < 1:
        return False

    return True


def create_circular_flow_alert(
    members_involved: List[str],
    total_amount_sats: int,
    total_cost_sats: int,
    cycle_count: int,
    detection_window_hours: float,
    recommendation: str,
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create a CIRCULAR_FLOW_ALERT message.

    This message alerts the fleet about detected wasteful circular rebalancing
    patterns (A→B→C→A) so members can adjust their behavior.

    Args:
        members_involved: List of member pubkeys in the circular flow
        total_amount_sats: Total amount flowing in the cycle
        total_cost_sats: Total fees wasted on the cycle
        cycle_count: Number of times cycle was detected
        detection_window_hours: Time window for detection
        recommendation: Human-readable recommendation
        rpc: CLN RPC interface for signing
        our_pubkey: Our node's public key

    Returns:
        Serialized CIRCULAR_FLOW_ALERT message, or None on error
    """
    timestamp = int(time.time())

    payload = {
        "reporter_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "members_involved": members_involved,
        "total_amount_sats": total_amount_sats,
        "total_cost_sats": total_cost_sats,
        "cycle_count": cycle_count,
        "detection_window_hours": detection_window_hours,
        "recommendation": recommendation[:500],  # Limit recommendation length
    }

    try:
        signing_payload = get_circular_flow_alert_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.CIRCULAR_FLOW_ALERT, payload)


# =============================================================================
# TEMPORAL PATTERN BATCH FUNCTIONS (Phase 14)
# =============================================================================

def get_temporal_pattern_batch_signing_payload(payload: Dict[str, Any]) -> str:
    """
    Get the canonical string to sign for TEMPORAL_PATTERN_BATCH messages.
    """
    patterns = payload.get("patterns", [])
    sorted_patterns = sorted(patterns, key=lambda p: (p.get("peer_id", ""), p.get("hour_of_day", 0)))
    patterns_str = json.dumps(sorted_patterns, sort_keys=True, separators=(',', ':'))
    patterns_hash = hashlib.sha256(patterns_str.encode()).hexdigest()[:16]

    return (
        f"TEMPORAL_PATTERN_BATCH:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(patterns)}:"
        f"{patterns_hash}"
    )


def validate_temporal_pattern_batch(payload: Dict[str, Any]) -> bool:
    """
    Validate a TEMPORAL_PATTERN_BATCH payload.
    """
    reporter_id = payload.get("reporter_id")
    if not reporter_id or not isinstance(reporter_id, str):
        return False
    if len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False
    now = time.time()
    if timestamp > now + 300:
        return False
    if timestamp < now - (48 * 3600):
        return False

    signature = payload.get("signature")
    if not signature or not isinstance(signature, str):
        return False

    patterns = payload.get("patterns")
    if not isinstance(patterns, list):
        return False
    if len(patterns) > MAX_PATTERNS_IN_BATCH:
        return False

    for p in patterns:
        if not isinstance(p, dict):
            return False

        peer_id = p.get("peer_id")
        if not peer_id or not isinstance(peer_id, str):
            return False
        if len(peer_id) > MAX_PEER_ID_LEN:
            return False

        hour = p.get("hour_of_day")
        if not isinstance(hour, int) or hour < 0 or hour > 23:
            return False

        day = p.get("day_of_week")
        if not isinstance(day, int) or day < -1 or day > 6:  # -1 = every day
            return False

        direction = p.get("direction")
        if direction not in ("inbound", "outbound", "bidirectional"):
            return False

        intensity = p.get("intensity")
        if not isinstance(intensity, (int, float)) or intensity < 0 or intensity > 1:
            return False

        confidence = p.get("confidence")
        if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
            return False

    return True


def create_temporal_pattern_batch(
    patterns: List[Dict[str, Any]],
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create a TEMPORAL_PATTERN_BATCH message.

    This message shares detected temporal flow patterns with the fleet,
    enabling coordinated liquidity positioning and fee optimization.

    Args:
        patterns: List of pattern entries, each containing:
            - peer_id: External peer pubkey
            - channel_id: Channel short ID
            - hour_of_day: 0-23 hour when pattern occurs
            - day_of_week: 0-6 (Mon-Sun) or -1 for every day
            - direction: inbound/outbound/bidirectional
            - intensity: Flow intensity 0-1
            - confidence: Pattern confidence 0-1
            - samples: Number of samples used to detect pattern
        rpc: CLN RPC interface for signing
        our_pubkey: Our node's public key

    Returns:
        Serialized TEMPORAL_PATTERN_BATCH message, or None on error
    """
    timestamp = int(time.time())

    payload = {
        "reporter_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "patterns": patterns,
    }

    try:
        signing_payload = get_temporal_pattern_batch_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.TEMPORAL_PATTERN_BATCH, payload)


# =============================================================================
# CORRIDOR VALUE BATCH FUNCTIONS (Phase 14.2)
# =============================================================================

def get_corridor_value_batch_signing_payload(payload: Dict[str, Any]) -> str:
    """Get the canonical string to sign for CORRIDOR_VALUE_BATCH messages."""
    corridors = payload.get("corridors", [])
    sorted_corridors = sorted(corridors, key=lambda c: (c.get("source_peer_id", ""), c.get("destination_peer_id", "")))
    corridors_str = json.dumps(sorted_corridors, sort_keys=True, separators=(',', ':'))
    corridors_hash = hashlib.sha256(corridors_str.encode()).hexdigest()[:16]

    return (
        f"CORRIDOR_VALUE_BATCH:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(corridors)}:"
        f"{corridors_hash}"
    )


def validate_corridor_value_batch(payload: Dict[str, Any]) -> bool:
    """Validate a CORRIDOR_VALUE_BATCH payload."""
    reporter_id = payload.get("reporter_id")
    if not reporter_id or not isinstance(reporter_id, str) or len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False
    now = time.time()
    if timestamp > now + 300 or timestamp < now - (48 * 3600):
        return False

    signature = payload.get("signature")
    if not signature or not isinstance(signature, str):
        return False

    corridors = payload.get("corridors")
    if not isinstance(corridors, list) or len(corridors) > MAX_CORRIDORS_IN_BATCH:
        return False

    for c in corridors:
        if not isinstance(c, dict):
            return False
        for field in ["source_peer_id", "destination_peer_id"]:
            val = c.get(field)
            if not val or not isinstance(val, str) or len(val) > MAX_PEER_ID_LEN:
                return False
        value_score = c.get("value_score")
        if not isinstance(value_score, (int, float)) or value_score < 0 or value_score > 100:
            return False
        daily_volume = c.get("daily_volume_sats")
        if not isinstance(daily_volume, int) or daily_volume < 0:
            return False

    return True


def create_corridor_value_batch(
    corridors: List[Dict[str, Any]],
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create a CORRIDOR_VALUE_BATCH message.

    Shares discovered high-value routing corridors with the fleet.
    """
    timestamp = int(time.time())
    payload = {
        "reporter_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "corridors": corridors,
    }

    try:
        signing_payload = get_corridor_value_batch_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.CORRIDOR_VALUE_BATCH, payload)


# =============================================================================
# POSITIONING PROPOSAL FUNCTIONS (Phase 14.2)
# =============================================================================

def get_positioning_proposal_signing_payload(payload: Dict[str, Any]) -> str:
    """Get the canonical string to sign for POSITIONING_PROPOSAL messages."""
    return (
        f"POSITIONING_PROPOSAL:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{payload.get('target_peer_id', '')}:"
        f"{payload.get('recommended_member', '')}:"
        f"{payload.get('target_capacity_sats', 0)}"
    )


def validate_positioning_proposal(payload: Dict[str, Any]) -> bool:
    """Validate a POSITIONING_PROPOSAL payload."""
    reporter_id = payload.get("reporter_id")
    if not reporter_id or not isinstance(reporter_id, str) or len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False
    now = time.time()
    if timestamp > now + 300 or timestamp < now - (48 * 3600):
        return False

    signature = payload.get("signature")
    if not signature or not isinstance(signature, str):
        return False

    target_peer_id = payload.get("target_peer_id")
    if not target_peer_id or not isinstance(target_peer_id, str) or len(target_peer_id) > MAX_PEER_ID_LEN:
        return False

    recommended_member = payload.get("recommended_member")
    if not recommended_member or not isinstance(recommended_member, str) or len(recommended_member) > MAX_PEER_ID_LEN:
        return False

    priority_tier = payload.get("priority_tier")
    if priority_tier not in VALID_PRIORITY_TIERS:
        return False

    target_capacity = payload.get("target_capacity_sats")
    if not isinstance(target_capacity, int) or target_capacity < 0 or target_capacity > 100_000_000_000:
        return False

    return True


def create_positioning_proposal(
    target_peer_id: str,
    recommended_member: str,
    priority_tier: str,
    target_capacity_sats: int,
    reason: str,
    value_score: float,
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create a POSITIONING_PROPOSAL message.

    Proposes which fleet member should open a channel to a target peer.
    """
    timestamp = int(time.time())
    payload = {
        "reporter_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "target_peer_id": target_peer_id,
        "recommended_member": recommended_member,
        "priority_tier": priority_tier,
        "target_capacity_sats": target_capacity_sats,
        "reason": reason[:500],
        "value_score": round(value_score, 4),
    }

    try:
        signing_payload = get_positioning_proposal_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.POSITIONING_PROPOSAL, payload)


# =============================================================================
# PHYSARUM RECOMMENDATION FUNCTIONS (Phase 14.2)
# =============================================================================

def get_physarum_recommendation_signing_payload(payload: Dict[str, Any]) -> str:
    """Get the canonical string to sign for PHYSARUM_RECOMMENDATION messages."""
    return (
        f"PHYSARUM_RECOMMENDATION:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{payload.get('channel_id', '')}:"
        f"{payload.get('action', '')}:"
        f"{payload.get('flow_intensity', 0)}"
    )


def validate_physarum_recommendation(payload: Dict[str, Any]) -> bool:
    """Validate a PHYSARUM_RECOMMENDATION payload."""
    reporter_id = payload.get("reporter_id")
    if not reporter_id or not isinstance(reporter_id, str) or len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False
    now = time.time()
    if timestamp > now + 300 or timestamp < now - (48 * 3600):
        return False

    signature = payload.get("signature")
    if not signature or not isinstance(signature, str):
        return False

    channel_id = payload.get("channel_id")
    if not channel_id or not isinstance(channel_id, str) or len(channel_id) > 50:
        return False

    peer_id = payload.get("peer_id")
    if not peer_id or not isinstance(peer_id, str) or len(peer_id) > MAX_PEER_ID_LEN:
        return False

    action = payload.get("action")
    if action not in VALID_PHYSARUM_ACTIONS:
        return False

    flow_intensity = payload.get("flow_intensity")
    if not isinstance(flow_intensity, (int, float)) or flow_intensity < 0 or flow_intensity > 1:
        return False

    return True


def create_physarum_recommendation(
    channel_id: str,
    peer_id: str,
    action: str,
    flow_intensity: float,
    reason: str,
    expected_yield_change_pct: float,
    rpc: Any,
    our_pubkey: str,
    splice_amount_sats: int = 0
) -> Optional[bytes]:
    """
    Create a PHYSARUM_RECOMMENDATION message.

    Shares flow-based channel lifecycle recommendations (strengthen/atrophy/stimulate).
    """
    timestamp = int(time.time())
    payload = {
        "reporter_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "channel_id": channel_id,
        "peer_id": peer_id,
        "action": action,
        "flow_intensity": round(flow_intensity, 4),
        "reason": reason[:500],
        "expected_yield_change_pct": round(expected_yield_change_pct, 2),
        "splice_amount_sats": splice_amount_sats,
    }

    try:
        signing_payload = get_physarum_recommendation_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.PHYSARUM_RECOMMENDATION, payload)


# =============================================================================
# COVERAGE ANALYSIS BATCH FUNCTIONS (Phase 14.2)
# =============================================================================

def get_coverage_analysis_batch_signing_payload(payload: Dict[str, Any]) -> str:
    """Get the canonical string to sign for COVERAGE_ANALYSIS_BATCH messages."""
    entries = payload.get("coverage_entries", [])
    sorted_entries = sorted(entries, key=lambda e: e.get("peer_id", ""))
    entries_str = json.dumps(sorted_entries, sort_keys=True, separators=(',', ':'))
    entries_hash = hashlib.sha256(entries_str.encode()).hexdigest()[:16]

    return (
        f"COVERAGE_ANALYSIS_BATCH:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{len(entries)}:"
        f"{entries_hash}"
    )


def validate_coverage_analysis_batch(payload: Dict[str, Any]) -> bool:
    """Validate a COVERAGE_ANALYSIS_BATCH payload."""
    reporter_id = payload.get("reporter_id")
    if not reporter_id or not isinstance(reporter_id, str) or len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False
    now = time.time()
    if timestamp > now + 300 or timestamp < now - (48 * 3600):
        return False

    signature = payload.get("signature")
    if not signature or not isinstance(signature, str):
        return False

    entries = payload.get("coverage_entries")
    if not isinstance(entries, list) or len(entries) > MAX_COVERAGE_ENTRIES_IN_BATCH:
        return False

    for e in entries:
        if not isinstance(e, dict):
            return False
        peer_id = e.get("peer_id")
        if not peer_id or not isinstance(peer_id, str) or len(peer_id) > MAX_PEER_ID_LEN:
            return False
        members = e.get("members_with_channels")
        if not isinstance(members, list):
            return False
        owner_confidence = e.get("ownership_confidence")
        if not isinstance(owner_confidence, (int, float)) or owner_confidence < 0 or owner_confidence > 1:
            return False

    return True


def create_coverage_analysis_batch(
    coverage_entries: List[Dict[str, Any]],
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create a COVERAGE_ANALYSIS_BATCH message.

    Shares peer coverage analysis showing which members have channels to each peer.
    """
    timestamp = int(time.time())
    payload = {
        "reporter_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "coverage_entries": coverage_entries,
    }

    try:
        signing_payload = get_coverage_analysis_batch_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.COVERAGE_ANALYSIS_BATCH, payload)


# =============================================================================
# CLOSE PROPOSAL FUNCTIONS (Phase 14.2)
# =============================================================================

def get_close_proposal_signing_payload(payload: Dict[str, Any]) -> str:
    """Get the canonical string to sign for CLOSE_PROPOSAL messages."""
    return (
        f"CLOSE_PROPOSAL:"
        f"{payload.get('reporter_id', '')}:"
        f"{payload.get('timestamp', 0)}:"
        f"{payload.get('member_id', '')}:"
        f"{payload.get('peer_id', '')}:"
        f"{payload.get('channel_id', '')}"
    )


def validate_close_proposal(payload: Dict[str, Any]) -> bool:
    """Validate a CLOSE_PROPOSAL payload."""
    reporter_id = payload.get("reporter_id")
    if not reporter_id or not isinstance(reporter_id, str) or len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, (int, float)):
        return False
    now = time.time()
    if timestamp > now + 300 or timestamp < now - (48 * 3600):
        return False

    signature = payload.get("signature")
    if not signature or not isinstance(signature, str):
        return False

    member_id = payload.get("member_id")
    if not member_id or not isinstance(member_id, str) or len(member_id) > MAX_PEER_ID_LEN:
        return False

    peer_id = payload.get("peer_id")
    if not peer_id or not isinstance(peer_id, str) or len(peer_id) > MAX_PEER_ID_LEN:
        return False

    channel_id = payload.get("channel_id")
    if not channel_id or not isinstance(channel_id, str) or len(channel_id) > 50:
        return False

    owner_id = payload.get("owner_id")
    if owner_id and (not isinstance(owner_id, str) or len(owner_id) > MAX_PEER_ID_LEN):
        return False

    freed_capacity = payload.get("freed_capacity_sats")
    if not isinstance(freed_capacity, int) or freed_capacity < 0:
        return False

    return True


def create_close_proposal(
    member_id: str,
    peer_id: str,
    channel_id: str,
    owner_id: str,
    reason: str,
    freed_capacity_sats: int,
    member_marker_strength: float,
    owner_marker_strength: float,
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create a CLOSE_PROPOSAL message.

    Proposes that a fleet member close a redundant/underperforming channel.
    """
    timestamp = int(time.time())
    payload = {
        "reporter_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "member_id": member_id,
        "peer_id": peer_id,
        "channel_id": channel_id,
        "owner_id": owner_id,
        "reason": reason[:500],
        "freed_capacity_sats": freed_capacity_sats,
        "member_marker_strength": round(member_marker_strength, 3),
        "owner_marker_strength": round(owner_marker_strength, 3),
    }

    try:
        signing_payload = get_close_proposal_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.CLOSE_PROPOSAL, payload)


# =============================================================================
# PHASE 15: MCF (MIN-COST MAX-FLOW) MESSAGE FUNCTIONS
# =============================================================================

def validate_mcf_needs_batch(payload: Dict[str, Any]) -> bool:
    """
    Validate an MCF_NEEDS_BATCH message payload.

    Args:
        payload: Message payload dict

    Returns:
        True if valid
    """
    if not isinstance(payload, dict):
        return False

    reporter_id = payload.get("reporter_id")
    if not isinstance(reporter_id, str) or len(reporter_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, int) or timestamp <= 0:
        return False

    signature = payload.get("signature")
    if not isinstance(signature, str):
        return False

    needs = payload.get("needs")
    if not isinstance(needs, list) or len(needs) > MAX_MCF_NEEDS_IN_BATCH:
        return False

    for need in needs:
        if not isinstance(need, dict):
            return False

        need_type = need.get("need_type")
        if need_type not in VALID_MCF_NEED_TYPES:
            return False

        amount_sats = need.get("amount_sats")
        if not isinstance(amount_sats, int) or amount_sats < MCF_MIN_AMOUNT_SATS:
            return False
        if amount_sats > MCF_MAX_AMOUNT_SATS:
            return False

        urgency = need.get("urgency", "medium")
        if urgency not in VALID_MCF_URGENCY_LEVELS:
            return False

    return True


def validate_mcf_solution_broadcast(payload: Dict[str, Any]) -> bool:
    """
    Validate an MCF_SOLUTION_BROADCAST message payload.

    Args:
        payload: Message payload dict

    Returns:
        True if valid
    """
    if not isinstance(payload, dict):
        return False

    coordinator_id = payload.get("coordinator_id")
    if not isinstance(coordinator_id, str) or len(coordinator_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, int) or timestamp <= 0:
        return False

    signature = payload.get("signature")
    if not isinstance(signature, str):
        return False

    assignments = payload.get("assignments")
    if not isinstance(assignments, list) or len(assignments) > MAX_MCF_ASSIGNMENTS_IN_SOLUTION:
        return False

    total_flow_sats = payload.get("total_flow_sats", 0)
    if not isinstance(total_flow_sats, int) or total_flow_sats < 0:
        return False

    total_cost_sats = payload.get("total_cost_sats", 0)
    if not isinstance(total_cost_sats, int) or total_cost_sats < 0:
        return False

    return True


def validate_mcf_assignment_ack(payload: Dict[str, Any]) -> bool:
    """
    Validate an MCF_ASSIGNMENT_ACK message payload.

    Args:
        payload: Message payload dict

    Returns:
        True if valid
    """
    if not isinstance(payload, dict):
        return False

    member_id = payload.get("member_id")
    if not isinstance(member_id, str) or len(member_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, int) or timestamp <= 0:
        return False

    signature = payload.get("signature")
    if not isinstance(signature, str):
        return False

    solution_timestamp = payload.get("solution_timestamp")
    if not isinstance(solution_timestamp, int):
        return False

    assignment_count = payload.get("assignment_count")
    if not isinstance(assignment_count, int) or assignment_count < 0:
        return False

    return True


def validate_mcf_completion_report(payload: Dict[str, Any]) -> bool:
    """
    Validate an MCF_COMPLETION_REPORT message payload.

    Args:
        payload: Message payload dict

    Returns:
        True if valid
    """
    if not isinstance(payload, dict):
        return False

    member_id = payload.get("member_id")
    if not isinstance(member_id, str) or len(member_id) > MAX_PEER_ID_LEN:
        return False

    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, int) or timestamp <= 0:
        return False

    signature = payload.get("signature")
    if not isinstance(signature, str):
        return False

    assignment_id = payload.get("assignment_id")
    if not isinstance(assignment_id, str):
        return False

    success = payload.get("success")
    if not isinstance(success, bool):
        return False

    actual_amount_sats = payload.get("actual_amount_sats", 0)
    if not isinstance(actual_amount_sats, int) or actual_amount_sats < 0:
        return False

    actual_cost_sats = payload.get("actual_cost_sats", 0)
    if not isinstance(actual_cost_sats, int) or actual_cost_sats < 0:
        return False

    return True


def get_mcf_needs_batch_signing_payload(payload: Dict[str, Any]) -> str:
    """Get signing payload for MCF_NEEDS_BATCH message."""
    reporter_id = payload.get("reporter_id", "")
    timestamp = payload.get("timestamp", 0)
    needs = payload.get("needs", [])
    needs_hash = hashlib.sha256(json.dumps(needs, sort_keys=True).encode()).hexdigest()[:16]
    return f"mcf_needs_batch:{reporter_id}:{timestamp}:{needs_hash}"


def get_mcf_solution_signing_payload(payload: Dict[str, Any]) -> str:
    """Get signing payload for MCF_SOLUTION_BROADCAST message."""
    coordinator_id = payload.get("coordinator_id", "")
    timestamp = payload.get("timestamp", 0)
    total_flow = payload.get("total_flow_sats", 0)
    total_cost = payload.get("total_cost_sats", 0)
    assignments = payload.get("assignments", [])
    assign_hash = hashlib.sha256(json.dumps(assignments, sort_keys=True).encode()).hexdigest()[:16]
    return f"mcf_solution:{coordinator_id}:{timestamp}:{total_flow}:{total_cost}:{assign_hash}"


def get_mcf_assignment_ack_signing_payload(payload: Dict[str, Any]) -> str:
    """Get signing payload for MCF_ASSIGNMENT_ACK message."""
    member_id = payload.get("member_id", "")
    timestamp = payload.get("timestamp", 0)
    solution_timestamp = payload.get("solution_timestamp", 0)
    assignment_count = payload.get("assignment_count", 0)
    return f"mcf_ack:{member_id}:{timestamp}:{solution_timestamp}:{assignment_count}"


def get_mcf_completion_signing_payload(payload: Dict[str, Any]) -> str:
    """Get signing payload for MCF_COMPLETION_REPORT message."""
    member_id = payload.get("member_id", "")
    timestamp = payload.get("timestamp", 0)
    assignment_id = payload.get("assignment_id", "")
    success = "1" if payload.get("success", False) else "0"
    amount = payload.get("actual_amount_sats", 0)
    return f"mcf_complete:{member_id}:{timestamp}:{assignment_id}:{success}:{amount}"


def create_mcf_needs_batch(
    needs: List[Dict[str, Any]],
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create an MCF_NEEDS_BATCH message.

    Args:
        needs: List of rebalance needs (need_type, target_peer, amount_sats, urgency)
        rpc: RPC interface for signing
        our_pubkey: Our pubkey

    Returns:
        Serialized message or None on error
    """
    timestamp = int(time.time())

    # Enforce bounds
    if len(needs) > MAX_MCF_NEEDS_IN_BATCH:
        needs = needs[:MAX_MCF_NEEDS_IN_BATCH]

    payload = {
        "reporter_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "needs": needs,
    }

    try:
        signing_payload = get_mcf_needs_batch_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.MCF_NEEDS_BATCH, payload)


def create_mcf_solution_broadcast(
    assignments: List[Dict[str, Any]],
    total_flow_sats: int,
    total_cost_sats: int,
    unmet_demand_sats: int,
    iterations: int,
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create an MCF_SOLUTION_BROADCAST message.

    Args:
        assignments: List of rebalance assignments
        total_flow_sats: Total flow achieved
        total_cost_sats: Total cost
        unmet_demand_sats: Demand that couldn't be met
        iterations: Solver iterations
        rpc: RPC interface for signing
        our_pubkey: Our pubkey (coordinator)

    Returns:
        Serialized message or None on error
    """
    timestamp = int(time.time())

    # Enforce bounds
    if len(assignments) > MAX_MCF_ASSIGNMENTS_IN_SOLUTION:
        assignments = assignments[:MAX_MCF_ASSIGNMENTS_IN_SOLUTION]

    payload = {
        "coordinator_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "assignments": assignments,
        "total_flow_sats": total_flow_sats,
        "total_cost_sats": total_cost_sats,
        "unmet_demand_sats": unmet_demand_sats,
        "iterations": iterations,
    }

    try:
        signing_payload = get_mcf_solution_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.MCF_SOLUTION_BROADCAST, payload)


def create_mcf_assignment_ack(
    solution_timestamp: int,
    assignment_count: int,
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create an MCF_ASSIGNMENT_ACK message.

    Args:
        solution_timestamp: Timestamp of the solution being acknowledged
        assignment_count: Number of assignments received for us
        rpc: RPC interface for signing
        our_pubkey: Our pubkey

    Returns:
        Serialized message or None on error
    """
    timestamp = int(time.time())

    payload = {
        "member_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "solution_timestamp": solution_timestamp,
        "assignment_count": assignment_count,
    }

    try:
        signing_payload = get_mcf_assignment_ack_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.MCF_ASSIGNMENT_ACK, payload)


def create_mcf_completion_report(
    assignment_id: str,
    success: bool,
    actual_amount_sats: int,
    actual_cost_sats: int,
    error_message: str,
    rpc: Any,
    our_pubkey: str
) -> Optional[bytes]:
    """
    Create an MCF_COMPLETION_REPORT message.

    Args:
        assignment_id: ID of the completed assignment
        success: Whether the assignment succeeded
        actual_amount_sats: Actual amount rebalanced
        actual_cost_sats: Actual cost paid
        error_message: Error message if failed
        rpc: RPC interface for signing
        our_pubkey: Our pubkey

    Returns:
        Serialized message or None on error
    """
    timestamp = int(time.time())

    payload = {
        "member_id": our_pubkey,
        "timestamp": timestamp,
        "signature": "",
        "assignment_id": assignment_id,
        "success": success,
        "actual_amount_sats": actual_amount_sats,
        "actual_cost_sats": actual_cost_sats,
        "error_message": error_message[:500] if error_message else "",
    }

    try:
        signing_payload = get_mcf_completion_signing_payload(payload)
        sign_result = rpc.signmessage(signing_payload)
        signature = sign_result.get("signature", sign_result.get("zbase", ""))
        payload["signature"] = signature
    except Exception:
        return None

    return serialize(HiveMessageType.MCF_COMPLETION_REPORT, payload)
