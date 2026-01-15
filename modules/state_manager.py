"""
State Manager Module for cl-hive.

Implements the HiveMap data structure and state synchronization
using Anti-Entropy (gossip + hash comparison) to ensure consistency
after network partitions.

State Hash Algorithm:
    SHA256( SortedJSON( [ {peer_id, version, timestamp}, ... ] ) )
    - Only essential metadata is hashed to detect drift.
    - List must be sorted by peer_id for determinism.

Author: Lightning Goats Team
"""

import hashlib
import json
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# CONSTANTS
# =============================================================================

# Minimum interval between state hash checks (seconds)
STATE_CHECK_INTERVAL = 60

# Maximum age for stale state entries (seconds) - 1 hour
STALE_STATE_THRESHOLD = 3600

# Bounds to prevent unbounded state growth
MAX_TOPOLOGY_ENTRIES = 200
MAX_FEE_POLICY_KEYS = 20
MAX_STATE_HASH_LEN = 128
MAX_PEER_ID_LEN = 128


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class HivePeerState:
    """
    Represents the cached state of a Hive peer.

    This is what we know about a peer's current liquidity and policy,
    updated via GOSSIP messages or FULL_SYNC responses.

    Attributes:
        peer_id: Node public key (33 bytes hex)
        capacity_sats: Total channel capacity to this peer
        available_sats: Available outbound liquidity
        fee_policy: Dict with base_fee, fee_rate, min_htlc, etc.
        topology: List of external peer_ids this node is connected to
        version: Monotonically increasing version number
        last_update: Unix timestamp of last gossip received
        state_hash: Hash of this peer's local state view
        budget_available_sats: Current budget-constrained spendable liquidity
        budget_reserved_until: Unix timestamp when any active budget hold expires
        budget_last_update: Unix timestamp when budget was last calculated
    """
    peer_id: str
    capacity_sats: int
    available_sats: int
    fee_policy: Dict[str, Any]
    topology: List[str]
    version: int
    last_update: int
    state_hash: str = ""
    budget_available_sats: int = 0
    budget_reserved_until: int = 0
    budget_last_update: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HivePeerState':
        """
        Create from dictionary with backward compatibility.

        Handles old nodes that don't send budget fields by using defaults.
        """
        # Required fields
        peer_id = data.get("peer_id", "")
        capacity_sats = data.get("capacity_sats", 0)
        available_sats = data.get("available_sats", 0)
        fee_policy = data.get("fee_policy", {})
        topology = data.get("topology", [])
        version = data.get("version", 0)
        last_update = data.get("last_update", data.get("timestamp", 0))
        state_hash = data.get("state_hash", "")

        # Budget fields (optional, backward compatible defaults)
        budget_available_sats = data.get("budget_available_sats", 0)
        budget_reserved_until = data.get("budget_reserved_until", 0)
        budget_last_update = data.get("budget_last_update", 0)

        return cls(
            peer_id=peer_id,
            capacity_sats=capacity_sats,
            available_sats=available_sats,
            fee_policy=fee_policy,
            topology=topology,
            version=version,
            last_update=last_update,
            state_hash=state_hash,
            budget_available_sats=budget_available_sats,
            budget_reserved_until=budget_reserved_until,
            budget_last_update=budget_last_update,
        )
    
    def to_hash_tuple(self) -> Dict[str, Any]:
        """
        Extract minimal tuple for state hash calculation.
        
        Only includes peer_id, version, and timestamp to detect drift
        without exposing full state in the hash comparison.
        """
        return {
            "peer_id": self.peer_id,
            "version": self.version,
            "timestamp": self.last_update
        }


# =============================================================================
# STATE MANAGER CLASS
# =============================================================================

class StateManager:
    """
    Manages the local view of Hive fleet state (the "HiveMap").
    
    Responsibilities:
    - Cache peer state received via GOSSIP
    - Calculate deterministic fleet hash for anti-entropy
    - Detect state divergence and trigger FULL_SYNC
    - Persist state to database
    
    Thread Safety:
    - All database operations use thread-local connections
    - State hash calculation is read-only (safe for concurrent access)
    """
    
    def __init__(self, database, plugin=None):
        """
        Initialize the StateManager.
        
        Args:
            database: HiveDatabase instance for persistence
            plugin: Optional plugin reference for logging
        """
        self.db = database
        self.plugin = plugin
        self._local_state: Dict[str, HivePeerState] = {}
        self._last_hash: str = ""
        self._last_hash_time: int = 0
    
    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[StateManager] {msg}", level=level)

    def _validate_state_entry(self, data: Dict[str, Any]) -> bool:
        """Validate a state entry before using it or writing to DB."""
        peer_id = data.get("peer_id")
        if not isinstance(peer_id, str) or not peer_id or len(peer_id) > MAX_PEER_ID_LEN:
            return False

        capacity_sats = data.get("capacity_sats", 0)
        available_sats = data.get("available_sats", 0)
        version = data.get("version", 0)
        timestamp = data.get("timestamp", data.get("last_update", 0))
        state_hash = data.get("state_hash", "")

        if not isinstance(capacity_sats, int) or capacity_sats < 0:
            return False
        if not isinstance(available_sats, int) or available_sats < 0:
            return False
        if not isinstance(version, int) or version < 0:
            return False
        if not isinstance(timestamp, int) or timestamp < 0:
            return False
        if not isinstance(state_hash, str) or len(state_hash) > MAX_STATE_HASH_LEN:
            return False

        fee_policy = data.get("fee_policy", {})
        if not isinstance(fee_policy, dict) or len(fee_policy) > MAX_FEE_POLICY_KEYS:
            return False

        topology = data.get("topology", [])
        if not isinstance(topology, list) or len(topology) > MAX_TOPOLOGY_ENTRIES:
            return False
        for entry in topology:
            if not isinstance(entry, str) or not entry or len(entry) > MAX_PEER_ID_LEN:
                return False

        return True
    
    # =========================================================================
    # STATE UPDATES
    # =========================================================================
    
    def update_peer_state(self, peer_id: str, gossip_data: Dict[str, Any]) -> bool:
        """
        Update local cache with received gossip data.
        
        Only updates if the incoming version is newer than what we have.
        Persists to database after update.
        
        Args:
            peer_id: The peer's public key
            gossip_data: Dict containing state fields from GOSSIP message
            
        Returns:
            True if state was updated, False if rejected (stale)
        """
        data = dict(gossip_data)
        data["peer_id"] = peer_id
        if not self._validate_state_entry(data):
            self._log(f"Rejected invalid gossip state from {peer_id[:16]}...", level="warn")
            return False

        remote_version = gossip_data.get('version', 0)
        
        # Check if we have existing state
        existing = self._local_state.get(peer_id)
        if existing and existing.version >= remote_version:
            self._log(f"Rejected stale gossip from {peer_id[:16]}... "
                     f"(local v{existing.version} >= remote v{remote_version})")
            return False
        
        # Create new state entry
        now = int(time.time())
        new_state = HivePeerState(
            peer_id=peer_id,
            capacity_sats=gossip_data.get('capacity_sats', 0),
            available_sats=gossip_data.get('available_sats', 0),
            fee_policy=gossip_data.get('fee_policy', {}),
            topology=gossip_data.get('topology', []),
            version=remote_version,
            last_update=gossip_data.get('timestamp', now),
            state_hash=gossip_data.get('state_hash', ""),
            # Budget fields (Phase 8 - backward compatible, defaults to 0)
            budget_available_sats=gossip_data.get('budget_available_sats', 0),
            budget_reserved_until=gossip_data.get('budget_reserved_until', 0),
            budget_last_update=gossip_data.get('budget_last_update', 0),
        )
        
        # Update in-memory cache
        self._local_state[peer_id] = new_state
        
        # Persist to database
        self.db.update_hive_state(
            peer_id=peer_id,
            capacity_sats=new_state.capacity_sats,
            available_sats=new_state.available_sats,
            fee_policy=new_state.fee_policy,
            topology=new_state.topology,
            state_hash=new_state.state_hash
        )
        
        self._log(f"Updated state for {peer_id[:16]}... to v{remote_version}")
        return True
    
    def update_local_state(self, capacity_sats: int, available_sats: int,
                           fee_policy: Dict[str, Any], topology: List[str],
                           our_pubkey: str) -> HivePeerState:
        """
        Update our own node's state in the HiveMap.
        
        Called after local changes (fee updates, channel opens, etc.)
        to prepare for outbound gossip.
        
        Args:
            capacity_sats: Our total Hive channel capacity
            available_sats: Our available outbound liquidity
            fee_policy: Our current fee policy dict
            topology: List of our external peer connections
            our_pubkey: Our node's public key
            
        Returns:
            The updated HivePeerState for our node
        """
        now = int(time.time())
        existing = self._local_state.get(our_pubkey)
        new_version = (existing.version + 1) if existing else 1
        
        our_state = HivePeerState(
            peer_id=our_pubkey,
            capacity_sats=capacity_sats,
            available_sats=available_sats,
            fee_policy=fee_policy,
            topology=topology,
            version=new_version,
            last_update=now,
            state_hash=""  # Will be calculated on demand
        )
        
        self._local_state[our_pubkey] = our_state
        return our_state
    
    def get_peer_state(self, peer_id: str) -> Optional[HivePeerState]:
        """Get cached state for a specific peer."""
        return self._local_state.get(peer_id)
    
    def get_all_peer_states(self) -> List[HivePeerState]:
        """Get all cached peer states."""
        return list(self._local_state.values())

    def get_fleet_budget_summary(self, min_channel_sats: int = 0,
                                  stale_threshold_sec: int = 600) -> Dict[str, Any]:
        """
        Get aggregated budget information across the hive fleet.

        Used for pre-flight affordability checks before starting expansion rounds.

        Args:
            min_channel_sats: Minimum channel size to consider "affordable"
            stale_threshold_sec: Seconds after which budget data is considered stale

        Returns:
            Dict with:
                - total_available_sats: Sum of all member budgets
                - members_with_budget: Count of members with budget > 0
                - affordable_members: List of peer_ids that can afford min_channel_sats
                - stale_count: Number of members with stale budget data
                - freshness_avg_sec: Average age of budget data
                - can_afford: True if any member can afford min_channel_sats
        """
        now = int(time.time())

        total_available = 0
        members_with_budget = 0
        affordable_members = []
        stale_count = 0
        budget_ages = []

        for state in self._local_state.values():
            budget = state.budget_available_sats
            budget_time = state.budget_last_update

            # Track budget totals
            if budget > 0:
                total_available += budget
                members_with_budget += 1

            # Check affordability
            if min_channel_sats > 0 and budget >= min_channel_sats:
                affordable_members.append(state.peer_id)

            # Track staleness
            if budget_time > 0:
                age = now - budget_time
                budget_ages.append(age)
                if age > stale_threshold_sec:
                    stale_count += 1

        avg_age = sum(budget_ages) / len(budget_ages) if budget_ages else 0

        return {
            "total_available_sats": total_available,
            "members_with_budget": members_with_budget,
            "affordable_members": affordable_members,
            "stale_count": stale_count,
            "freshness_avg_sec": int(avg_age),
            "can_afford": len(affordable_members) > 0 or min_channel_sats == 0,
        }

    def remove_peer_state(self, peer_id: str) -> bool:
        """Remove a peer from the state cache (e.g., after ban)."""
        if peer_id in self._local_state:
            del self._local_state[peer_id]
            return True
        return False
    
    # =========================================================================
    # STATE HASH CALCULATION
    # =========================================================================
    
    def calculate_fleet_hash(self) -> str:
        """
        Calculate deterministic hash of the entire fleet state.
        
        Algorithm:
            1. Extract minimal tuples: (peer_id, version, timestamp)
            2. Sort by peer_id (lexicographic)
            3. Serialize to JSON with sorted keys, compact separators
            4. SHA256 hash the result
        
        Returns:
            Hex-encoded SHA256 hash of the sorted state array
        """
        # Extract minimal state tuples
        state_tuples = [
            state.to_hash_tuple() 
            for state in self._local_state.values()
        ]
        
        # Sort by peer_id for determinism
        state_tuples.sort(key=lambda x: x['peer_id'])
        
        # Serialize to canonical JSON
        json_str = json.dumps(state_tuples, sort_keys=True, separators=(',', ':'))
        
        # Calculate SHA256
        hash_bytes = hashlib.sha256(json_str.encode('utf-8')).digest()
        hash_hex = hash_bytes.hex()
        
        # Cache the result
        self._last_hash = hash_hex
        self._last_hash_time = int(time.time())
        
        return hash_hex
    
    def get_cached_hash(self) -> Tuple[str, int]:
        """
        Get the cached fleet hash if still fresh.
        
        Returns:
            Tuple of (hash_hex, age_seconds)
        """
        age = int(time.time()) - self._last_hash_time
        return (self._last_hash, age)
    
    # =========================================================================
    # ANTI-ENTROPY (DIVERGENCE DETECTION)
    # =========================================================================
    
    def compare_hash(self, remote_hash: str) -> bool:
        """
        Compare remote hash against local state.
        
        Args:
            remote_hash: Fleet hash received from another node
            
        Returns:
            True if hashes match (no divergence), False otherwise
        """
        local_hash = self.calculate_fleet_hash()
        return local_hash == remote_hash
    
    def get_full_state_for_sync(self) -> List[Dict[str, Any]]:
        """
        Get complete state data for FULL_SYNC response.
        
        Returns:
            List of peer state dictionaries
        """
        return [state.to_dict() for state in self._local_state.values()]
    
    def apply_full_sync(self, remote_states: List[Dict[str, Any]]) -> int:
        """
        Apply a FULL_SYNC payload to update local state.
        
        Merges remote state, preferring higher versions.
        
        Args:
            remote_states: List of peer state dictionaries
            
        Returns:
            Number of states that were updated
        """
        updated_count = 0
        
        for state_dict in remote_states:
            peer_id = state_dict.get('peer_id')
            if not peer_id:
                continue
            if not self._validate_state_entry(state_dict):
                self._log(f"Rejected invalid FULL_SYNC entry for {peer_id[:16]}...", level="warn")
                continue
            
            remote_version = state_dict.get('version', 0)
            local_state = self._local_state.get(peer_id)
            
            # Only update if remote is newer
            if not local_state or local_state.version < remote_version:
                new_state = HivePeerState.from_dict(state_dict)
                self._local_state[peer_id] = new_state
                
                # Persist to database
                self.db.update_hive_state(
                    peer_id=peer_id,
                    capacity_sats=new_state.capacity_sats,
                    available_sats=new_state.available_sats,
                    fee_policy=new_state.fee_policy,
                    topology=new_state.topology,
                    state_hash=new_state.state_hash
                )
                
                updated_count += 1
        
        self._log(f"FULL_SYNC applied: {updated_count} states updated")
        return updated_count
    
    # =========================================================================
    # INITIALIZATION & PERSISTENCE
    # =========================================================================
    
    def load_from_database(self) -> int:
        """
        Load cached state from database on startup.
        
        Returns:
            Number of states loaded
        """
        db_states = self.db.get_all_hive_states()
        
        for state_dict in db_states:
            peer_id = state_dict.get('peer_id')
            if peer_id:
                self._local_state[peer_id] = HivePeerState(
                    peer_id=peer_id,
                    capacity_sats=state_dict.get('capacity_sats', 0),
                    available_sats=state_dict.get('available_sats', 0),
                    fee_policy=state_dict.get('fee_policy', {}),
                    topology=state_dict.get('topology', []),
                    version=state_dict.get('version', 0),
                    last_update=state_dict.get('last_gossip', 0),
                    state_hash=state_dict.get('state_hash', "")
                )
        
        self._log(f"Loaded {len(self._local_state)} peer states from database")
        return len(self._local_state)
    
    def cleanup_stale_states(self, max_age_seconds: int = STALE_STATE_THRESHOLD) -> int:
        """
        Remove states that haven't been updated recently.
        
        Args:
            max_age_seconds: Maximum age before state is considered stale
            
        Returns:
            Number of states removed
        """
        now = int(time.time())
        cutoff = now - max_age_seconds
        
        stale_peers = [
            peer_id for peer_id, state in self._local_state.items()
            if state.last_update < cutoff
        ]
        
        for peer_id in stale_peers:
            del self._local_state[peer_id]
        
        if stale_peers:
            self._log(f"Cleaned up {len(stale_peers)} stale states")
        
        return len(stale_peers)
    
    # =========================================================================
    # STATISTICS
    # =========================================================================
    
    def get_fleet_stats(self) -> Dict[str, Any]:
        """
        Calculate aggregate statistics for the fleet.
        
        Returns:
            Dict with fleet-wide metrics
        """
        states = list(self._local_state.values())
        
        if not states:
            return {
                "peer_count": 0,
                "total_capacity_sats": 0,
                "total_available_sats": 0,
                "unique_external_peers": 0,
                "fleet_hash": ""
            }
        
        total_capacity = sum(s.capacity_sats for s in states)
        total_available = sum(s.available_sats for s in states)
        
        # Deduplicate external peers across fleet
        all_external = set()
        for state in states:
            all_external.update(state.topology)
        
        return {
            "peer_count": len(states),
            "total_capacity_sats": total_capacity,
            "total_available_sats": total_available,
            "unique_external_peers": len(all_external),
            "fleet_hash": self.calculate_fleet_hash()
        }
