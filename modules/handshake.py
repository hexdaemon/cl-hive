"""
Handshake module for cl-hive

Implements the PKI-based authentication protocol:
- Genesis: Create a new Hive as the founding Member
- Manifest: Create and verify capability attestations
- Challenge-Response: Prove identity via HSM signatures

Crypto Strategy:
    Uses Core Lightning's signmessage/checkmessage RPCs.
    Keys never leave the HSM. No external crypto libraries required.

Join Flow (Channel-as-Proof-of-Stake):
    A node with a channel to any hive member can join:
    1. A -> B (HELLO): Candidate announces pubkey
    2. B checks for existing channel with A
    3. B -> A (CHALLENGE): Member sends random Nonce
    4. A -> B (ATTEST): Candidate sends signed Manifest + Nonce
    5. B -> A (WELCOME): New member joins as neophyte

Note: Tickets are deprecated. Channel existence serves as proof of stake.
Having a channel demonstrates economic commitment to the network.
"""

import os
import json
import threading
import time
import base64
import hashlib
import secrets
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict


# =============================================================================
# CONSTANTS
# =============================================================================

# Default ticket validity period
DEFAULT_TICKET_HOURS = 24

# Nonce size in bytes (32 bytes = 64 hex chars)
NONCE_SIZE = 32

# Challenge time-to-live in seconds
CHALLENGE_TTL_SECONDS = 300

# Cap to prevent unbounded pending challenge growth
MAX_PENDING_CHALLENGES = 1000

# SECURITY (Issue #11): Per-peer rate limit for challenge generation
CHALLENGE_RATE_LIMIT_SECONDS = 10  # Minimum seconds between challenges per peer

# Plugin version for manifest
PLUGIN_VERSION = "cl-hive v2.2.6"


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class Ticket:
    """
    Invitation ticket structure.

    DEPRECATED: Tickets are no longer required for joining. Channel existence
    serves as proof of stake. This class is retained for backward compatibility
    with genesis bootstrapping and legacy flows.

    A Member generates this to authorize new members to join.
    The ticket is signed by the Member's node key.
    """
    admin_pubkey: str       # 66-char hex pubkey of issuing admin
    hive_id: str            # Unique Hive identifier
    requirements: int       # Bitmask of required features
    issued_at: int          # Unix timestamp
    expires_at: int         # Unix timestamp
    signature: str          # signmessage result
    initial_tier: str = 'neophyte'  # Starting tier: always 'neophyte' (member tier via promotion only)
    
    def to_json(self) -> str:
        """Serialize to JSON (excluding signature for signing)."""
        data = asdict(self)
        del data['signature']
        return json.dumps(data, sort_keys=True, separators=(',', ':'))
    
    def to_base64(self) -> str:
        """Encode full ticket (including signature) as base64."""
        return base64.b64encode(json.dumps(asdict(self)).encode()).decode()
    
    # SECURITY: Maximum ticket size to prevent memory exhaustion DoS (Issue #9)
    MAX_TICKET_SIZE = 10 * 1024  # 10KB

    @classmethod
    def from_base64(cls, encoded: str) -> 'Ticket':
        """
        Decode ticket from base64.

        SECURITY: Enforces size limit to prevent memory exhaustion DoS.
        """
        # Check size before decoding
        if len(encoded) > cls.MAX_TICKET_SIZE:
            raise ValueError(
                f"Ticket too large: {len(encoded)} bytes exceeds "
                f"{cls.MAX_TICKET_SIZE} byte limit"
            )
        data = json.loads(base64.b64decode(encoded))
        return cls(**data)
    
    def is_expired(self) -> bool:
        """Check if ticket has expired."""
        return time.time() > self.expires_at


@dataclass
class Manifest:
    """
    Capability manifest structure.
    
    A node creates this to prove its identity and capabilities
    during the handshake process.
    """
    pubkey: str             # Node's public key
    version: str            # Plugin version
    features: list          # Supported features
    timestamp: int          # Creation timestamp
    nonce: str              # Challenge nonce being responded to
    
    def to_json(self) -> str:
        """Serialize to JSON for signing."""
        return json.dumps(asdict(self), sort_keys=True, separators=(',', ':'))


# =============================================================================
# REQUIREMENT FLAGS (Bitmask)
# =============================================================================

class Requirements:
    """Feature requirement bitmask values."""
    NONE = 0
    SPLICE = 1 << 0         # Node must support splicing
    DUAL_FUND = 1 << 1      # Node must support dual-funded channels
    ANCHOR = 1 << 2         # Node must support anchor outputs
    ONION_MSG = 1 << 3      # Node must support onion messages


# =============================================================================
# HANDSHAKE MANAGER
# =============================================================================

class HandshakeManager:
    """
    Manages Hive authentication and session establishment.

    Handles:
    - Genesis (creating a new Hive as founding member)
    - Manifest creation and verification
    - Challenge-response protocol for identity proof
    - Legacy ticket operations (deprecated)

    Join Flow:
    Nodes join by having a channel with any existing member.
    Channel existence serves as proof of stake - no ticket needed.
    """
    
    def __init__(self, rpc_proxy, db, plugin, min_vouch_count: int = 3):
        """
        Initialize the handshake manager.

        Args:
            rpc_proxy: ThreadSafeRpcProxy for CLN RPC calls
            db: HiveDatabase instance
            plugin: Plugin reference for logging
            min_vouch_count: Minimum vouches for promotion (for bootstrap check)
        """
        self.rpc = rpc_proxy
        self.db = db
        self.plugin = plugin
        self.min_vouch_count = min_vouch_count
        self._our_pubkey: Optional[str] = None
        self._challenge_lock = threading.Lock()
        self._pending_challenges: Dict[str, Dict[str, Any]] = {}
    
    # =========================================================================
    # IDENTITY
    # =========================================================================
    
    def get_our_pubkey(self) -> str:
        """Get our node's public key (cached)."""
        if self._our_pubkey is None:
            info = self.rpc.getinfo()
            self._our_pubkey = info['id']
        return self._our_pubkey
    
    # =========================================================================
    # GENESIS
    # =========================================================================
    
    def genesis(self, hive_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a new Hive with this node as the founding Member.

        This bootstraps a new Hive. The founding node starts as a full
        member and can invite others. Other nodes join by opening a
        channel to any existing member (no tickets required).

        Args:
            hive_id: Optional custom Hive ID (auto-generated if not provided)

        Returns:
            Dict with genesis info (hive_id, member_pubkey, etc.)

        Raises:
            ValueError: If this node is already part of a Hive
        """
        our_pubkey = self.get_our_pubkey()
        
        # Check if we're already in a Hive
        existing = self.db.get_member(our_pubkey)
        if existing:
            raise ValueError(f"Already member of Hive (tier: {existing['tier']})")
        
        # Generate Hive ID if not provided
        if hive_id is None:
            hive_id = f"hive_{secrets.token_hex(8)}"
        
        # Create genesis ticket (self-signed)
        now = int(time.time())
        ticket_data = {
            "admin_pubkey": our_pubkey,
            "hive_id": hive_id,
            "requirements": Requirements.NONE,
            "issued_at": now,
            "expires_at": now + (365 * 24 * 3600),  # 1 year validity
            "initial_tier": "neophyte",  # Must be in signed data for verification
        }
        
        # Sign the ticket
        ticket_json = json.dumps(ticket_data, sort_keys=True, separators=(',', ':'))
        sig_result = self.rpc.signmessage(ticket_json)
        signature = sig_result['zbase']
        
        # Create full ticket
        genesis_ticket = Ticket(
            **ticket_data,
            signature=signature
        )

        # Store ourselves as founding member
        # NOTE: Admin tier removed - genesis creates a member directly
        self.db.add_member(
            peer_id=our_pubkey,
            tier='member',
            joined_at=now,
            promoted_at=now
        )
        
        # Store genesis ticket in metadata
        self.db.update_member(
            our_pubkey,
            metadata=json.dumps({
                "genesis_ticket": genesis_ticket.to_base64(),
                "hive_id": hive_id
            })
        )
        
        self.plugin.log(f"Genesis complete: Hive '{hive_id}' created")
        
        return {
            "status": "genesis_complete",
            "hive_id": hive_id,
            "admin_pubkey": our_pubkey,
            "genesis_ticket": genesis_ticket.to_base64()
        }
    
    # =========================================================================
    # TICKET OPERATIONS
    # =========================================================================
    
    def generate_invite_ticket(self,
                                valid_hours: int = DEFAULT_TICKET_HOURS,
                                requirements: int = Requirements.NONE,
                                initial_tier: str = 'neophyte') -> str:
        """
        Generate an invitation ticket for a new member.

        DEPRECATED: Prefer JOIN_INTENT flow where nodes join by opening a
        channel to any hive member and broadcasting intent.

        Only Members can generate invite tickets. All new members start as neophytes.

        Args:
            valid_hours: Hours until ticket expires
            requirements: Bitmask of required features
            initial_tier: Starting tier (always 'neophyte')

        Returns:
            Base64-encoded signed ticket

        Raises:
            PermissionError: If caller is not a Member
            ValueError: If invalid initial_tier requested
        """
        our_pubkey = self.get_our_pubkey()

        # Verify we're a Member
        member = self.db.get_member(our_pubkey)
        if not member or member['tier'] != 'member':
            raise PermissionError("Only Members can generate invite tickets")

        # All new members start as neophytes - no more bootstrap/admin tier
        if initial_tier != 'neophyte':
            raise ValueError(f"Invalid initial_tier: {initial_tier}. All new members start as 'neophyte'")

        # Get Hive ID from metadata
        metadata = json.loads(member.get('metadata', '{}'))
        hive_id = metadata.get('hive_id', 'unknown')

        # Create ticket
        now = int(time.time())
        ticket_data = {
            "admin_pubkey": our_pubkey,
            "hive_id": hive_id,
            "requirements": requirements,
            "issued_at": now,
            "expires_at": now + (valid_hours * 3600),
            "initial_tier": initial_tier,
        }

        # Sign
        ticket_json = json.dumps(ticket_data, sort_keys=True, separators=(',', ':'))
        sig_result = self.rpc.signmessage(ticket_json)

        ticket = Ticket(**ticket_data, signature=sig_result['zbase'])

        tier_desc = " (BOOTSTRAP)" if initial_tier == 'member' else ""
        self.plugin.log(f"Generated invite ticket{tier_desc} (expires in {valid_hours}h)")

        return ticket.to_base64()
    
    def verify_ticket(self, ticket_b64: str) -> Tuple[bool, Optional[Ticket], str]:
        """
        Verify an invitation ticket.
        
        Checks:
        1. Signature is valid (signed by admin_pubkey)
        2. Ticket has not expired
        3. Admin is a known member of the Hive
        
        Args:
            ticket_b64: Base64-encoded ticket
            
        Returns:
            Tuple of (is_valid, ticket_obj, error_message)
        """
        try:
            ticket = Ticket.from_base64(ticket_b64)
        except Exception as e:
            return (False, None, f"Invalid ticket format: {e}")
        
        # Check expiry
        if ticket.is_expired():
            return (False, ticket, "Ticket has expired")
        
        # Verify signature
        ticket_json = ticket.to_json()
        try:
            result = self.rpc.checkmessage(ticket_json, ticket.signature)
            if not result.get('verified', False):
                return (False, ticket, "Invalid signature")
            
            # Verify signer matches admin_pubkey
            if result.get('pubkey') != ticket.admin_pubkey:
                return (False, ticket, "Signature pubkey mismatch")
                
        except Exception as e:
            return (False, ticket, f"Signature verification failed: {e}")
        
        # Verify issuer is a known member
        issuer = self.db.get_member(ticket.admin_pubkey)
        if not issuer or issuer['tier'] != 'member':
            return (False, ticket, "Unknown or non-member issuer")
        
        return (True, ticket, "")
    
    # =========================================================================
    # MANIFEST OPERATIONS
    # =========================================================================
    
    def create_manifest(self, nonce: str, features: Optional[list] = None) -> Dict[str, Any]:
        """
        Create a signed manifest for attestation.
        
        Args:
            nonce: Challenge nonce to include
            features: List of supported features (auto-detected if None)
            
        Returns:
            Dict with manifest data and signatures
        """
        our_pubkey = self.get_our_pubkey()
        
        if features is None:
            features = self._detect_features()
        
        manifest = Manifest(
            pubkey=our_pubkey,
            version=PLUGIN_VERSION,
            features=features,
            timestamp=int(time.time()),
            nonce=nonce
        )
        
        manifest_json = manifest.to_json()
        
        # Sign both the nonce and the full manifest
        nonce_sig = self.rpc.signmessage(nonce)['zbase']
        manifest_sig = self.rpc.signmessage(manifest_json)['zbase']
        
        return {
            "manifest": asdict(manifest),
            "nonce_signature": nonce_sig,
            "manifest_signature": manifest_sig
        }
    
    def verify_manifest(self, manifest_data: Dict, nonce_sig: str, 
                        manifest_sig: str, expected_nonce: str) -> Tuple[bool, str]:
        """
        Verify a manifest attestation.
        
        Args:
            manifest_data: Manifest dictionary
            nonce_sig: Signature of the nonce
            manifest_sig: Signature of the manifest JSON
            expected_nonce: The nonce we challenged with
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        pubkey = manifest_data.get('pubkey')
        
        # Verify nonce matches
        if manifest_data.get('nonce') != expected_nonce:
            return (False, "Nonce mismatch")
        
        # Verify nonce signature (pass pubkey for nodes not in gossip graph)
        try:
            result = self.rpc.checkmessage(expected_nonce, nonce_sig, pubkey)
            if not result.get('verified') or result.get('pubkey') != pubkey:
                return (False, "Invalid nonce signature")
        except Exception as e:
            return (False, f"Nonce verification failed: {e}")

        # Verify manifest signature (pass pubkey for nodes not in gossip graph)
        manifest_json = json.dumps(manifest_data, sort_keys=True, separators=(',', ':'))
        try:
            result = self.rpc.checkmessage(manifest_json, manifest_sig, pubkey)
            if not result.get('verified') or result.get('pubkey') != pubkey:
                return (False, "Invalid manifest signature")
        except Exception as e:
            return (False, f"Manifest verification failed: {e}")
        
        return (True, "")
    
    # =========================================================================
    # CHALLENGE-RESPONSE
    # =========================================================================
    
    def generate_challenge(self, peer_id: str, requirements: int,
                            initial_tier: str = 'neophyte') -> str:
        """
        Generate a challenge nonce for a peer.

        Args:
            peer_id: Peer's public key
            requirements: Bitmask requirements from the invite ticket
            initial_tier: Starting tier for new member ('neophyte' or 'member')

        Returns:
            Hex-encoded random nonce

        Raises:
            ValueError: If rate limit exceeded for this peer

        SECURITY (Issue #11): Per-peer rate limiting to prevent DoS via
        challenge flooding that would evict legitimate pending challenges.
        """
        now = int(time.time())

        with self._challenge_lock:
            # Check per-peer rate limit
            existing = self._pending_challenges.get(peer_id)
            if existing:
                time_since_last = now - existing["issued_at"]
                if time_since_last < CHALLENGE_RATE_LIMIT_SECONDS:
                    raise ValueError(
                        f"Rate limit exceeded: wait {CHALLENGE_RATE_LIMIT_SECONDS - time_since_last}s"
                    )

            nonce = secrets.token_hex(NONCE_SIZE)
            self._pending_challenges[peer_id] = {
                "nonce": nonce,
                "issued_at": now,
                "requirements": requirements,
                "initial_tier": initial_tier
            }

            # LRU eviction if over limit
            if len(self._pending_challenges) > MAX_PENDING_CHALLENGES:
                oldest = sorted(
                    self._pending_challenges.items(),
                    key=lambda item: item[1]["issued_at"]
                )
                for key, _ in oldest[: len(self._pending_challenges) - MAX_PENDING_CHALLENGES]:
                    self._pending_challenges.pop(key, None)

        return nonce

    def get_pending_challenge(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """Get the pending challenge nonce for a peer."""
        with self._challenge_lock:
            challenge = self._pending_challenges.get(peer_id)
            if challenge is None:
                return None
            # Enforce TTL - expire stale challenges
            now = int(time.time())
            if now - challenge["issued_at"] > CHALLENGE_TTL_SECONDS:
                self._pending_challenges.pop(peer_id, None)
                return None
            return challenge

    def clear_challenge(self, peer_id: str) -> None:
        """Clear the pending challenge for a peer."""
        with self._challenge_lock:
            self._pending_challenges.pop(peer_id, None)
    
    # =========================================================================
    # FEATURE DETECTION
    # =========================================================================
    
    def _detect_features(self) -> list:
        """
        Detect supported features on this node.
        
        Returns:
            List of feature strings
        """
        features = []
        
        try:
            # Check for splice support
            config = self.rpc.listconfigs()
            if config.get('experimental-splicing'):
                features.append('splice')
            if config.get('experimental-dual-fund'):
                features.append('dual-fund')
            if config.get('experimental-onion-messages'):
                features.append('onion-msg')
        except Exception:
            pass

        # Advertise max supported protocol version (Phase B hardening)
        from modules.protocol import SUPPORTED_VERSIONS
        features.append(f'proto-v{max(SUPPORTED_VERSIONS)}')

        return features
    
    def check_requirements(self, requirements: int, features: list) -> Tuple[bool, list]:
        """
        Check if features satisfy requirements bitmask.
        
        Args:
            requirements: Bitmask of required features
            features: List of available features
            
        Returns:
            Tuple of (satisfied, missing_features)
        """
        missing = []
        
        if requirements & Requirements.SPLICE and 'splice' not in features:
            missing.append('splice')
        if requirements & Requirements.DUAL_FUND and 'dual-fund' not in features:
            missing.append('dual-fund')
        if requirements & Requirements.ANCHOR and 'anchor' not in features:
            missing.append('anchor')
        if requirements & Requirements.ONION_MSG and 'onion-msg' not in features:
            missing.append('onion-msg')
        
        return (len(missing) == 0, missing)
