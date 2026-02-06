"""
Gossip Relay Module for cl-hive

Implements TTL-based message relay with deduplication to enable
gossip propagation in non-fully-connected mesh topologies.

When A↔B↔C but A is not connected to C:
- B receives message from A
- B relays to C (with decremented TTL)
- C receives message via relay

Key features:
- TTL-based hop limiting (default: 3)
- Message deduplication via hash
- Relay path tracking to prevent echo
- Automatic expiry of seen messages
"""

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any, Callable
from enum import Enum


# =============================================================================
# CONSTANTS
# =============================================================================

DEFAULT_TTL = 3                    # Maximum hops for relay
DEDUP_EXPIRY_SECONDS = 300         # 5 minutes - how long to remember seen messages
CLEANUP_INTERVAL_SECONDS = 60      # How often to clean expired entries
MAX_RELAY_PATH_LENGTH = 10         # Maximum nodes in relay path (safety limit)
MAX_SEEN_MESSAGES = 10000          # Maximum cached message hashes


# =============================================================================
# RELAY METADATA
# =============================================================================

@dataclass
class RelayMetadata:
    """
    Metadata added to messages for relay tracking.

    Attributes:
        msg_id: Unique message identifier (hash of original content)
        ttl: Time-to-live (decremented on each hop)
        relay_path: List of node pubkeys that have seen this message
        origin: Original sender's pubkey
        origin_ts: Original timestamp
    """
    msg_id: str
    ttl: int = DEFAULT_TTL
    relay_path: List[str] = field(default_factory=list)
    origin: str = ""
    origin_ts: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "msg_id": self.msg_id,
            "ttl": self.ttl,
            "relay_path": self.relay_path,
            "origin": self.origin,
            "origin_ts": self.origin_ts
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RelayMetadata":
        return cls(
            msg_id=data.get("msg_id", ""),
            ttl=data.get("ttl", DEFAULT_TTL),
            relay_path=data.get("relay_path", []),
            origin=data.get("origin", ""),
            origin_ts=data.get("origin_ts", 0)
        )


# =============================================================================
# MESSAGE DEDUPLICATION
# =============================================================================

class MessageDeduplicator:
    """
    Thread-safe message deduplication cache.

    Tracks seen message hashes with automatic expiry to prevent
    infinite relay loops while allowing legitimate re-broadcasts.
    """

    def __init__(self, expiry_seconds: int = DEDUP_EXPIRY_SECONDS):
        self._seen: Dict[str, int] = {}  # msg_id -> timestamp
        self._lock = threading.Lock()
        self._expiry = expiry_seconds
        self._last_cleanup = time.time()

    def is_duplicate(self, msg_id: str) -> bool:
        """Check if message was already seen (returns True if duplicate)."""
        with self._lock:
            self._maybe_cleanup()
            return msg_id in self._seen

    def mark_seen(self, msg_id: str) -> None:
        """Mark a message as seen."""
        with self._lock:
            self._seen[msg_id] = int(time.time())
            # Enforce size limit
            if len(self._seen) > MAX_SEEN_MESSAGES:
                self._cleanup_oldest()

    def check_and_mark(self, msg_id: str) -> bool:
        """
        Atomic check-and-mark operation.

        Returns:
            True if this is the first time seeing this message (should process)
            False if duplicate (should skip)
        """
        with self._lock:
            self._maybe_cleanup()
            if msg_id in self._seen:
                return False
            self._seen[msg_id] = int(time.time())
            return True

    def _maybe_cleanup(self) -> None:
        """Clean expired entries if enough time has passed."""
        now = time.time()
        if now - self._last_cleanup < CLEANUP_INTERVAL_SECONDS:
            return
        self._last_cleanup = now
        cutoff = int(now) - self._expiry
        self._seen = {k: v for k, v in self._seen.items() if v > cutoff}

    def _cleanup_oldest(self) -> None:
        """Remove oldest entries when cache is full."""
        if len(self._seen) <= MAX_SEEN_MESSAGES // 2:
            return
        sorted_items = sorted(self._seen.items(), key=lambda x: x[1])
        keep_count = MAX_SEEN_MESSAGES // 2
        self._seen = dict(sorted_items[-keep_count:])

    def stats(self) -> Dict[str, Any]:
        """Return cache statistics."""
        with self._lock:
            return {
                "cached_messages": len(self._seen),
                "expiry_seconds": self._expiry,
                "max_size": MAX_SEEN_MESSAGES
            }


# =============================================================================
# RELAY MANAGER
# =============================================================================

class RelayManager:
    """
    Manages gossip relay for non-mesh topologies.

    Usage:
        relay_mgr = RelayManager(our_pubkey, send_func, get_members_func, log_func)

        # When receiving a message:
        if relay_mgr.should_process(msg_payload):
            # Process locally
            handle_message(msg_payload)
            # Relay to other members
            relay_mgr.relay(msg_payload, sender_peer_id)
    """

    def __init__(
        self,
        our_pubkey: str,
        send_message: Callable[[str, bytes], bool],  # (peer_id, message_bytes) -> success
        get_members: Callable[[], List[str]],        # () -> list of member pubkeys
        log: Callable[[str, str], None] = None       # (msg, level) -> None
    ):
        """
        Initialize relay manager.

        Args:
            our_pubkey: Our node's public key
            send_message: Function to send raw message bytes to a peer
            get_members: Function to get list of hive member pubkeys
            log: Optional logging function
        """
        self.our_pubkey = our_pubkey
        self.send_message = send_message
        self.get_members = get_members
        self.log = log or (lambda msg, level: None)
        self.dedup = MessageDeduplicator()

        # Statistics
        self._stats_lock = threading.Lock()
        self._stats = {
            "messages_processed": 0,
            "messages_relayed": 0,
            "messages_deduplicated": 0,
            "relay_failures": 0
        }

    def generate_msg_id(self, payload: Dict[str, Any]) -> str:
        """
        Generate unique message ID from payload content.

        Uses hash of key identifying fields to detect duplicates
        even if relay metadata differs.

        Phase C hardening: if the payload carries a deterministic
        ``_event_id`` (injected by idempotency layer), use it directly
        instead of hashing the full payload.
        """
        # Prefer deterministic event ID when available
        eid = payload.get("_event_id")
        if isinstance(eid, str) and len(eid) == 32:
            return eid

        # Fallback: hash core content (exclude relay + internal metadata)
        core = {k: v for k, v in payload.items()
                if k not in ("_relay", "msg_id", "ttl", "relay_path",
                             "_envelope_version", "_event_id")}
        content = json.dumps(core, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    def prepare_for_broadcast(
        self,
        payload: Dict[str, Any],
        ttl: int = DEFAULT_TTL
    ) -> Dict[str, Any]:
        """
        Prepare a new message for broadcast with relay metadata.

        Call this when originating a new message (not relaying).
        """
        msg_id = self.generate_msg_id(payload)
        relay_meta = RelayMetadata(
            msg_id=msg_id,
            ttl=ttl,
            relay_path=[self.our_pubkey],
            origin=self.our_pubkey,
            origin_ts=int(time.time())
        )
        payload["_relay"] = relay_meta.to_dict()

        # Mark as seen so we don't process our own broadcast
        self.dedup.mark_seen(msg_id)

        return payload

    def should_process(self, payload: Dict[str, Any]) -> bool:
        """
        Check if we should process this message.

        Returns:
            True if message is new and should be processed
            False if duplicate (already seen)
        """
        # Extract or generate msg_id
        relay_data = payload.get("_relay", {})
        msg_id = relay_data.get("msg_id") or self.generate_msg_id(payload)

        # Check for duplicate
        if not self.dedup.check_and_mark(msg_id):
            with self._stats_lock:
                self._stats["messages_deduplicated"] += 1
            return False

        with self._stats_lock:
            self._stats["messages_processed"] += 1

        return True

    def should_relay(self, payload: Dict[str, Any]) -> bool:
        """
        Check if message should be relayed (TTL > 0 and we're not in path already).
        """
        relay_data = payload.get("_relay", {})
        ttl = relay_data.get("ttl", DEFAULT_TTL)
        relay_path = relay_data.get("relay_path", [])

        # Don't relay if TTL exhausted
        if ttl <= 0:
            return False

        # Don't relay if path is too long (safety)
        if len(relay_path) >= MAX_RELAY_PATH_LENGTH:
            return False

        return True

    def prepare_for_relay(
        self,
        payload: Dict[str, Any],
        sender_peer_id: str
    ) -> Dict[str, Any]:
        """
        Prepare message for relay by decrementing TTL and adding us to path.

        Args:
            payload: Message payload to relay
            sender_peer_id: Who sent us this message

        Returns:
            Updated payload ready for relay (or None if shouldn't relay)
        """
        relay_data = payload.get("_relay", {})

        # Get or create relay metadata
        msg_id = relay_data.get("msg_id") or self.generate_msg_id(payload)
        ttl = relay_data.get("ttl", DEFAULT_TTL)
        relay_path = relay_data.get("relay_path", [])
        origin = relay_data.get("origin", sender_peer_id)
        origin_ts = relay_data.get("origin_ts", int(time.time()))

        # Decrement TTL
        new_ttl = ttl - 1
        if new_ttl <= 0:
            return None

        # Add ourselves to path
        new_path = relay_path + [self.our_pubkey]
        if len(new_path) > MAX_RELAY_PATH_LENGTH:
            return None

        # Update relay metadata
        new_relay = RelayMetadata(
            msg_id=msg_id,
            ttl=new_ttl,
            relay_path=new_path,
            origin=origin,
            origin_ts=origin_ts
        )

        # Create new payload with updated relay info
        new_payload = dict(payload)
        new_payload["_relay"] = new_relay.to_dict()

        return new_payload

    def relay(
        self,
        payload: Dict[str, Any],
        sender_peer_id: str,
        encode_message: Callable[[Dict[str, Any]], bytes]
    ) -> int:
        """
        Relay message to other hive members.

        Args:
            payload: Message payload to relay
            sender_peer_id: Who sent us this message (to exclude from relay)
            encode_message: Function to encode payload to wire format

        Returns:
            Number of members message was relayed to
        """
        if not self.should_relay(payload):
            return 0

        # Prepare relay payload
        relay_payload = self.prepare_for_relay(payload, sender_peer_id)
        if not relay_payload:
            return 0

        relay_path = relay_payload.get("_relay", {}).get("relay_path", [])

        # Get members to relay to
        members = self.get_members()

        # Encode message
        try:
            message_bytes = encode_message(relay_payload)
        except Exception as e:
            self.log(f"Failed to encode relay message: {e}", "error")
            return 0

        # Relay to members not in path and not sender
        sent_count = 0
        for member_id in members:
            # Skip ourselves
            if member_id == self.our_pubkey:
                continue
            # Skip sender
            if member_id == sender_peer_id:
                continue
            # Skip nodes already in relay path
            if member_id in relay_path:
                continue

            try:
                if self.send_message(member_id, message_bytes):
                    sent_count += 1
            except Exception as e:
                self.log(f"Failed to relay to {member_id[:16]}...: {e}", "debug")
                with self._stats_lock:
                    self._stats["relay_failures"] += 1

        if sent_count > 0:
            with self._stats_lock:
                self._stats["messages_relayed"] += 1
            self.log(
                f"Relayed message to {sent_count} members (TTL={relay_payload['_relay']['ttl']})",
                "debug"
            )

        return sent_count

    def stats(self) -> Dict[str, Any]:
        """Return relay statistics."""
        with self._stats_lock:
            stats = dict(self._stats)
        stats["dedup"] = self.dedup.stats()
        return stats


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def extract_relay_metadata(payload: Dict[str, Any]) -> Optional[RelayMetadata]:
    """Extract relay metadata from message payload."""
    relay_data = payload.get("_relay")
    if not relay_data:
        return None
    return RelayMetadata.from_dict(relay_data)


def is_relayed_message(payload: Dict[str, Any]) -> bool:
    """Check if message was relayed (not direct from origin)."""
    relay_data = payload.get("_relay", {})
    relay_path = relay_data.get("relay_path", [])
    return len(relay_path) > 1


def get_message_origin(payload: Dict[str, Any]) -> Optional[str]:
    """Get original sender of message (may differ from peer_id for relayed messages)."""
    relay_data = payload.get("_relay", {})
    return relay_data.get("origin")
