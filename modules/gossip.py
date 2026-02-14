"""
Gossip Module for cl-hive.

Implements threshold-based gossip broadcasting to minimize network
overhead while ensuring timely propagation of important state changes.

Threshold Rules:
1. Capacity: Change > 10% from last broadcast
2. Fee: Any change in fee_policy
3. Status: Ban/Unban events (immediate)
4. Heartbeat: Force broadcast every heartbeat_interval if no other updates

Author: Lightning Goats Team
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

# =============================================================================
# CONSTANTS
# =============================================================================

# Threshold for capacity change triggering gossip (10%)
CAPACITY_CHANGE_THRESHOLD = 0.10

# Default heartbeat interval in seconds (5 minutes)
DEFAULT_HEARTBEAT_INTERVAL = 300

# Minimum interval between gossip broadcasts to same peer (seconds)
MIN_GOSSIP_INTERVAL = 10

# Bounds to prevent unbounded payload growth
MAX_TOPOLOGY_ENTRIES = 200
MAX_FULL_SYNC_STATES = 2000
MAX_FEE_POLICY_KEYS = 20
MAX_GOSSIP_STRING_LEN = 256  # Pubkeys are 66 chars, channel IDs ~18

# Rate limit for FULL_SYNC processing (seconds per peer)
FULL_SYNC_COOLDOWN = 60


# =============================================================================
# DATA CLASSES
# =============================================================================

# Capability constants for version-aware feature negotiation
CAPABILITY_MCF = "mcf"  # Min-Cost Max-Flow optimization support


@dataclass
class GossipState:
    """
    Tracks the last broadcast state for comparison.

    Used to determine if a new gossip is warranted based on
    threshold rules.
    """
    capacity_sats: int = 0
    available_sats: int = 0
    fee_policy: Dict[str, Any] = field(default_factory=dict)
    topology: List[str] = field(default_factory=list)
    last_broadcast: int = 0
    version: int = 0
    budget_available_sats: int = 0
    budget_reserved_until: int = 0
    capabilities: List[str] = field(default_factory=list)


# =============================================================================
# GOSSIP MANAGER CLASS
# =============================================================================

class GossipManager:
    """
    Manages gossip broadcasting with threshold-based filtering.

    Responsibilities:
    - Track last broadcast state to detect meaningful changes
    - Determine if current state warrants a broadcast
    - Create gossip payloads for transmission
    - Process incoming gossip and delegate to StateManager

    Thread Safety:
    - _last_broadcast_state, _peer_gossip_times, and _active_peers protected by _lock
    - Multiple background loops (gossip_loop, custommsg handler) access this manager
    """

    def __init__(self, state_manager, plugin=None,
                 heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
                 get_membership_hash: Optional[Callable[[], str]] = None):
        """
        Initialize the GossipManager.

        Args:
            state_manager: StateManager instance for state updates
            plugin: Optional plugin reference for logging
            heartbeat_interval: Seconds between forced heartbeat broadcasts
            get_membership_hash: Optional callback to get membership hash for sync
        """
        self.state_manager = state_manager
        self.plugin = plugin
        self.heartbeat_interval = heartbeat_interval
        self.get_membership_hash = get_membership_hash

        # Lock protecting _last_broadcast_state, _peer_gossip_times, _active_peers
        self._lock = threading.Lock()

        # Track our last broadcast state
        self._last_broadcast_state = GossipState()

        # Track when we last sent gossip to each peer
        self._peer_gossip_times: Dict[str, int] = {}

        # Set of peers we've received gossip from (for connectivity tracking)
        self._active_peers: Set[str] = set()

        # Per-peer rate limit for FULL_SYNC processing
        self._full_sync_times: Dict[str, float] = {}

    def sync_version_from_state_manager(self, our_pubkey: str) -> None:
        """
        Sync the broadcast version from persisted state manager data.

        Call this after initialization once our_pubkey is known, to restore
        the version number after a restart.
        """
        our_state = self.state_manager.get_peer_state(our_pubkey)
        with self._lock:
            if our_state and our_state.version > self._last_broadcast_state.version:
                old_version = self._last_broadcast_state.version
                self._last_broadcast_state.version = our_state.version
                self._log(f"Synced version from state manager: v{old_version} -> v{our_state.version}")
    
    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[GossipManager] {msg}", level=level)
    
    # =========================================================================
    # THRESHOLD CHECKING
    # =========================================================================
    
    def should_broadcast(self, new_capacity: int, new_available: int,
                         new_fee_policy: Dict[str, Any],
                         new_topology: List[str],
                         force_status: bool = False) -> bool:
        """
        Determine if current state warrants a gossip broadcast.

        Threshold Rules (OR logic - any trigger broadcasts):
        1. Capacity change > 10% from last broadcast
        2. Any change in fee_policy (base_fee, fee_rate, etc.)
        3. force_status=True (ban/unban events)
        4. Heartbeat timeout exceeded

        Args:
            new_capacity: Current total capacity in sats
            new_available: Current available liquidity in sats
            new_fee_policy: Current fee policy dict
            new_topology: Current external peer list
            force_status: True to force broadcast (status change)

        Returns:
            True if broadcast should be sent, False otherwise
        """
        now = int(time.time())

        # Rule 3: Force on status change (bans, promotions)
        if force_status:
            self._log("Broadcast triggered: Status change (forced)")
            return True

        with self._lock:
            old = self._last_broadcast_state

            # Rule 4: Heartbeat timeout
            time_since_last = now - old.last_broadcast
            if time_since_last >= self.heartbeat_interval:
                self._log(f"Broadcast triggered: Heartbeat ({time_since_last}s elapsed)")
                return True

            # Rule 1: Capacity change > 10%
            if old.capacity_sats > 0:
                capacity_delta = abs(new_capacity - old.capacity_sats)
                capacity_ratio = capacity_delta / old.capacity_sats
                if capacity_ratio > CAPACITY_CHANGE_THRESHOLD:
                    self._log(f"Broadcast triggered: Capacity change ({capacity_ratio:.1%})")
                    return True
            elif new_capacity > 0:
                # First time having capacity
                self._log("Broadcast triggered: First capacity")
                return True

            # Rule 2: Any fee policy change
            if self._fee_policy_changed(old.fee_policy, new_fee_policy):
                self._log("Broadcast triggered: Fee policy change")
                return True

            # Rule 2b: Topology change (new external connections)
            old_topo_set = set(old.topology)
            new_topo_set = set(new_topology)
            if old_topo_set != new_topo_set:
                added = new_topo_set - old_topo_set
                removed = old_topo_set - new_topo_set
                self._log(f"Broadcast triggered: Topology change (+{len(added)}/-{len(removed)})")
                return True

        return False
    
    def _fee_policy_changed(self, old_policy: Dict, new_policy: Dict) -> bool:
        """
        Compare fee policies for meaningful changes.
        
        Checks: base_fee, fee_rate, min_htlc, max_htlc, cltv_delta
        """
        keys_to_check = ['base_fee', 'fee_rate', 'min_htlc', 'max_htlc', 'cltv_delta']
        
        for key in keys_to_check:
            old_val = old_policy.get(key)
            new_val = new_policy.get(key)
            if old_val != new_val:
                return True
        
        return False
    
    # =========================================================================
    # GOSSIP CREATION
    # =========================================================================
    
    def create_gossip_payload(self, our_pubkey: str, capacity_sats: int,
                               available_sats: int, fee_policy: Dict[str, Any],
                               topology: List[str],
                               budget_available_sats: int = 0,
                               budget_reserved_until: int = 0,
                               addresses: List[str] = None,
                               capabilities: List[str] = None) -> Dict[str, Any]:
        """
        Create a gossip payload for broadcast.

        Updates internal tracking state after creating payload.

        Args:
            our_pubkey: Our node's public key
            capacity_sats: Total Hive channel capacity
            available_sats: Available outbound liquidity
            fee_policy: Current fee policy
            topology: List of external peer connections
            budget_available_sats: Budget-constrained spendable liquidity
            budget_reserved_until: Timestamp when budget hold expires (0 if none)
            addresses: List of our node's connection addresses (e.g., ["1.2.3.4:9735", "xyz.onion:9735"])
            capabilities: List of supported capabilities (e.g., ["mcf"] for MCF optimization)

        Returns:
            Dict payload ready for GOSSIP message serialization
        """
        now = int(time.time())

        # Default capabilities include MCF support (this node has it)
        if capabilities is None:
            capabilities = [CAPABILITY_MCF]

        with self._lock:
            new_version = self._last_broadcast_state.version + 1

            # Update our tracking state
            self._last_broadcast_state = GossipState(
                capacity_sats=capacity_sats,
                available_sats=available_sats,
                fee_policy=fee_policy.copy(),
                topology=topology.copy(),
                last_broadcast=now,
                version=new_version,
                budget_available_sats=budget_available_sats,
                budget_reserved_until=budget_reserved_until,
                capabilities=capabilities.copy(),
            )

        # Also update the state manager with our local state
        # Pass the gossip version to ensure it gets persisted for restart recovery
        self.state_manager.update_local_state(
            capacity_sats=capacity_sats,
            available_sats=available_sats,
            fee_policy=fee_policy,
            topology=topology,
            our_pubkey=our_pubkey,
            force_version=new_version
        )

        return {
            "peer_id": our_pubkey,
            "capacity_sats": capacity_sats,
            "available_sats": available_sats,
            "fee_policy": fee_policy.copy() if fee_policy else {},
            "topology": topology.copy() if topology else [],
            "version": new_version,
            "timestamp": now,
            "state_hash": self.state_manager.calculate_fleet_hash(),
            # Budget fields (Phase 8 - Hive-wide Affordability)
            "budget_available_sats": budget_available_sats,
            "budget_reserved_until": budget_reserved_until,
            "budget_last_update": now,
            # Connection addresses for auto-connect (Issue #38)
            "addresses": addresses or [],
            # Capabilities for version-aware feature negotiation (Phase 15)
            "capabilities": capabilities,
        }
    
    # =========================================================================
    # GOSSIP PROCESSING
    # =========================================================================
    
    def process_gossip(self, sender_id: str, payload: Dict[str, Any]) -> bool:
        """
        Process an incoming GOSSIP message.
        
        Validates payload and delegates to StateManager for storage.
        
        Args:
            sender_id: Public key of the sending node
            payload: Gossip payload from message
            
        Returns:
            True if gossip was accepted, False if rejected
        """
        # Validate required fields
        required_fields = ['peer_id', 'version', 'timestamp']
        for field in required_fields:
            if field not in payload:
                self._log(f"Rejected gossip from {sender_id[:16]}...: missing {field}")
                return False
        
        # Verify sender matches payload peer_id (prevent spoofing)
        if payload['peer_id'] != sender_id:
            self._log(f"Rejected gossip: sender mismatch "
                     f"({sender_id[:16]}... != {payload['peer_id'][:16]}...)")
            return False

        # Timestamp freshness check - reject messages too old or too far in the future
        now = int(time.time())
        msg_timestamp = payload.get('timestamp', 0)
        MAX_GOSSIP_AGE = 3600  # 1 hour
        MAX_CLOCK_SKEW = 300   # 5 minutes
        if msg_timestamp < (now - MAX_GOSSIP_AGE):
            self._log(f"Rejected stale gossip from {sender_id[:16]}...: "
                     f"timestamp {now - msg_timestamp}s old")
            return False
        if msg_timestamp > (now + MAX_CLOCK_SKEW):
            self._log(f"Rejected future gossip from {sender_id[:16]}...: "
                     f"timestamp {msg_timestamp - now}s ahead")
            return False
        
        fee_policy = payload.get("fee_policy", {})
        topology = payload.get("topology", [])
        if not isinstance(fee_policy, dict) or len(fee_policy) > MAX_FEE_POLICY_KEYS:
            self._log(f"Rejected gossip from {sender_id[:16]}...: invalid fee_policy")
            return False
        if not isinstance(topology, list) or len(topology) > MAX_TOPOLOGY_ENTRIES:
            self._log(f"Rejected gossip from {sender_id[:16]}...: invalid topology")
            return False

        # Validate individual string lengths (pubkeys are 66 chars, channel IDs ~18)
        if any(not isinstance(t, str) or len(t) > MAX_GOSSIP_STRING_LEN for t in topology):
            self._log(f"Rejected gossip from {sender_id[:16]}...: topology entry too long")
            return False
        if any(not isinstance(k, str) or len(k) > MAX_GOSSIP_STRING_LEN for k in fee_policy):
            self._log(f"Rejected gossip from {sender_id[:16]}...: fee_policy key too long")
            return False

        MAX_FEE_VALUE = 10_000_000
        for k, v in fee_policy.items():
            if not isinstance(v, (int, float)) or v < 0 or v > MAX_FEE_VALUE:
                self._log(f"Rejected gossip from {sender_id[:16]}...: invalid fee_policy value", level="warn")
                return False

        # Track active peer
        with self._lock:
            self._active_peers.add(sender_id)

        # Delegate to state manager
        return self.state_manager.update_peer_state(sender_id, payload)
    
    # =========================================================================
    # STATE HASH OPERATIONS
    # =========================================================================
    
    def create_state_hash_payload(self) -> Dict[str, Any]:
        """
        Create a STATE_HASH message payload for anti-entropy check.

        Sent on peer connection to detect state divergence.
        Includes both fleet hash (gossip state) and membership hash (tiers).

        Returns:
            Dict with fleet_hash, membership_hash and metadata
        """
        fleet_hash = self.state_manager.calculate_fleet_hash()
        stats = self.state_manager.get_fleet_stats()

        payload = {
            "fleet_hash": fleet_hash,
            "peer_count": stats['peer_count'],
            "timestamp": int(time.time())
        }

        # Include membership hash if available
        if self.get_membership_hash:
            try:
                payload["membership_hash"] = self.get_membership_hash()
            except Exception as e:
                self._log(f"Failed to get membership hash: {e}", "warn")

        return payload
    
    def process_state_hash(self, sender_id: str, payload: Dict[str, Any]) -> bool:
        """
        Process an incoming STATE_HASH message.

        Compares remote hashes against local and determines if
        FULL_SYNC is needed. Checks both fleet hash (gossip state)
        and membership hash (tiers).

        Args:
            sender_id: Public key of the sending node
            payload: STATE_HASH payload

        Returns:
            True if all hashes match (no sync needed), False if any diverged
        """
        remote_fleet_hash = payload.get('fleet_hash', '')
        remote_membership_hash = payload.get('membership_hash', '')
        remote_count = payload.get('peer_count', 0)

        local_fleet_hash = self.state_manager.calculate_fleet_hash()
        local_stats = self.state_manager.get_fleet_stats()

        # Get local membership hash if available
        local_membership_hash = ''
        if self.get_membership_hash:
            try:
                local_membership_hash = self.get_membership_hash()
            except Exception as e:
                self._log(f"Failed to get membership hash: {e}", "warn")

        # Check fleet hash
        fleet_match = (local_fleet_hash == remote_fleet_hash)

        # Check membership hash (only if both sides have it)
        membership_match = True
        if local_membership_hash and remote_membership_hash:
            membership_match = (local_membership_hash == remote_membership_hash)

        if fleet_match and membership_match:
            self._log(f"State hash match with {sender_id[:16]}... "
                     f"({local_stats['peer_count']} peers)")
            return True
        else:
            mismatch_type = []
            if not fleet_match:
                mismatch_type.append("fleet")
            if not membership_match:
                mismatch_type.append("membership")
            self._log(f"State hash MISMATCH ({', '.join(mismatch_type)}) with {sender_id[:16]}...: "
                     f"local_fleet={local_fleet_hash[:16]}..., "
                     f"remote_fleet={remote_fleet_hash[:16]}... ({remote_count} peers)")
            return False
    
    # =========================================================================
    # FULL SYNC OPERATIONS
    # =========================================================================
    
    def create_full_sync_payload(self) -> Dict[str, Any]:
        """
        Create a FULL_SYNC message payload.
        
        Contains complete state for all known peers.
        
        Returns:
            Dict with states array and metadata
        """
        states = self.state_manager.get_full_state_for_sync()
        
        return {
            "states": states,
            "fleet_hash": self.state_manager.calculate_fleet_hash(),
            "timestamp": int(time.time())
        }
    
    def process_full_sync(self, sender_id: str, payload: Dict[str, Any]) -> int:
        """
        Process an incoming FULL_SYNC message.

        Merges remote state into local state manager.

        Args:
            sender_id: Public key of the sending node
            payload: FULL_SYNC payload with states array

        Returns:
            Number of states that were updated
        """
        # Per-peer rate limit to prevent DoS via repeated FULL_SYNC
        now = time.time()
        with self._lock:
            last_sync = self._full_sync_times.get(sender_id, 0)
            if now - last_sync < FULL_SYNC_COOLDOWN:
                self._log(f"Rate-limited FULL_SYNC from {sender_id[:16]}...", level="warn")
                return 0
            self._full_sync_times[sender_id] = now

            cutoff = now - FULL_SYNC_COOLDOWN * 2
            stale_sync_keys = [k for k, t in self._full_sync_times.items() if t < cutoff]
            for k in stale_sync_keys:
                del self._full_sync_times[k]

        states = payload.get('states', [])

        if not states:
            self._log(f"Empty FULL_SYNC from {sender_id[:16]}...")
            return 0
        if not isinstance(states, list) or len(states) > MAX_FULL_SYNC_STATES:
            self._log(f"Rejected FULL_SYNC from {sender_id[:16]}...: too many states")
            return 0
        
        updated = self.state_manager.apply_full_sync(states)
        
        self._log(f"FULL_SYNC from {sender_id[:16]}...: "
                 f"{len(states)} states received, {updated} updated")
        
        return updated
    
    # =========================================================================
    # PEER MANAGEMENT
    # =========================================================================
    
    def get_active_peers(self) -> List[str]:
        """Get list of peers we've received gossip from."""
        with self._lock:
            return list(self._active_peers)
    
    def mark_peer_inactive(self, peer_id: str) -> None:
        """Mark a peer as inactive (disconnected) and cleanup tracking data."""
        with self._lock:
            self._active_peers.discard(peer_id)
            self._peer_gossip_times.pop(peer_id, None)
    
    def can_send_gossip_to(self, peer_id: str) -> bool:
        """
        Check if we can send gossip to a peer (rate limiting).

        Enforces minimum interval between gossip to same peer.

        Args:
            peer_id: Target peer's public key

        Returns:
            True if enough time has passed since last gossip
        """
        now = int(time.time())
        with self._lock:
            last_time = self._peer_gossip_times.get(peer_id, 0)
        return (now - last_time) >= MIN_GOSSIP_INTERVAL
    
    def record_gossip_sent(self, peer_id: str) -> None:
        """Record that we sent gossip to a peer."""
        with self._lock:
            self._peer_gossip_times[peer_id] = int(time.time())
    
    # =========================================================================
    # STATISTICS
    # =========================================================================
    
    def get_gossip_stats(self) -> Dict[str, Any]:
        """
        Get gossip statistics.

        Returns:
            Dict with gossip metrics
        """
        now = int(time.time())
        with self._lock:
            last_broadcast = self._last_broadcast_state.last_broadcast
            version = self._last_broadcast_state.version
            active_peers_count = len(self._active_peers)
            tracked_peers_count = len(self._peer_gossip_times)

        return {
            "version": version,
            "last_broadcast_ago": now - last_broadcast if last_broadcast else None,
            "heartbeat_interval": self.heartbeat_interval,
            "active_peers": active_peers_count,
            "tracked_peers": tracked_peers_count
        }
