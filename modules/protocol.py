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
    BAN = 32791         # Ban announcement
    PROMOTION = 32793   # Promotion confirmation


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
