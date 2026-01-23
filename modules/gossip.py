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

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

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


# =============================================================================
# DATA CLASSES
# =============================================================================

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
    - All state is local to this manager instance
    - No shared mutable state between threads
    """
    
    def __init__(self, state_manager, plugin=None,
                 heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL):
        """
        Initialize the GossipManager.
        
        Args:
            state_manager: StateManager instance for state updates
            plugin: Optional plugin reference for logging
            heartbeat_interval: Seconds between forced heartbeat broadcasts
        """
        self.state_manager = state_manager
        self.plugin = plugin
        self.heartbeat_interval = heartbeat_interval

        # Track our last broadcast state
        self._last_broadcast_state = GossipState()

        # Track when we last sent gossip to each peer
        self._peer_gossip_times: Dict[str, int] = {}

        # Set of peers we've received gossip from (for connectivity tracking)
        self._active_peers: Set[str] = set()

    def sync_version_from_state_manager(self, our_pubkey: str) -> None:
        """
        Sync the broadcast version from persisted state manager data.

        Call this after initialization once our_pubkey is known, to restore
        the version number after a restart.
        """
        our_state = self.state_manager.get_peer_state(our_pubkey)
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
        old = self._last_broadcast_state
        now = int(time.time())
        
        # Rule 3: Force on status change (bans, promotions)
        if force_status:
            self._log("Broadcast triggered: Status change (forced)")
            return True
        
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
                               budget_reserved_until: int = 0) -> Dict[str, Any]:
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

        Returns:
            Dict payload ready for GOSSIP message serialization
        """
        now = int(time.time())
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
            "fee_policy": fee_policy,
            "topology": topology,
            "version": new_version,
            "timestamp": now,
            "state_hash": self.state_manager.calculate_fleet_hash(),
            # Budget fields (Phase 8 - Hive-wide Affordability)
            "budget_available_sats": budget_available_sats,
            "budget_reserved_until": budget_reserved_until,
            "budget_last_update": now,
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
        
        fee_policy = payload.get("fee_policy", {})
        topology = payload.get("topology", [])
        if not isinstance(fee_policy, dict) or len(fee_policy) > MAX_FEE_POLICY_KEYS:
            self._log(f"Rejected gossip from {sender_id[:16]}...: invalid fee_policy")
            return False
        if not isinstance(topology, list) or len(topology) > MAX_TOPOLOGY_ENTRIES:
            self._log(f"Rejected gossip from {sender_id[:16]}...: invalid topology")
            return False

        # Track active peer
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
        
        Returns:
            Dict with fleet_hash and metadata
        """
        fleet_hash = self.state_manager.calculate_fleet_hash()
        stats = self.state_manager.get_fleet_stats()
        
        return {
            "fleet_hash": fleet_hash,
            "peer_count": stats['peer_count'],
            "timestamp": int(time.time())
        }
    
    def process_state_hash(self, sender_id: str, payload: Dict[str, Any]) -> bool:
        """
        Process an incoming STATE_HASH message.
        
        Compares remote hash against local and determines if
        FULL_SYNC is needed.
        
        Args:
            sender_id: Public key of the sending node
            payload: STATE_HASH payload
            
        Returns:
            True if hashes match (no sync needed), False if diverged
        """
        remote_hash = payload.get('fleet_hash', '')
        remote_count = payload.get('peer_count', 0)
        
        local_hash = self.state_manager.calculate_fleet_hash()
        local_stats = self.state_manager.get_fleet_stats()
        
        if local_hash == remote_hash:
            self._log(f"State hash match with {sender_id[:16]}... "
                     f"({local_stats['peer_count']} peers)")
            return True
        else:
            self._log(f"State hash MISMATCH with {sender_id[:16]}...: "
                     f"local={local_hash[:16]}... ({local_stats['peer_count']} peers), "
                     f"remote={remote_hash[:16]}... ({remote_count} peers)")
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
        return list(self._active_peers)
    
    def mark_peer_inactive(self, peer_id: str) -> None:
        """Mark a peer as inactive (disconnected)."""
        self._active_peers.discard(peer_id)
    
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
        last_time = self._peer_gossip_times.get(peer_id, 0)
        return (now - last_time) >= MIN_GOSSIP_INTERVAL
    
    def record_gossip_sent(self, peer_id: str) -> None:
        """Record that we sent gossip to a peer."""
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
        last_broadcast = self._last_broadcast_state.last_broadcast
        
        return {
            "version": self._last_broadcast_state.version,
            "last_broadcast_ago": now - last_broadcast if last_broadcast else None,
            "heartbeat_interval": self.heartbeat_interval,
            "active_peers": len(self._active_peers),
            "tracked_peers": len(self._peer_gossip_times)
        }
