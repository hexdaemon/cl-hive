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
import threading
import time
from dataclasses import dataclass, asdict, field
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
        fees_earned_sats: Routing fees earned in current settlement period
        fees_forward_count: Number of forwards in current period
        fees_period_start: Start of current fee reporting period
        fees_last_report: Timestamp of last fee report received
        capabilities: List of supported capabilities (e.g., ["mcf"] for MCF optimization)
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
    # Fee reporting fields for settlement
    fees_earned_sats: int = 0
    fees_forward_count: int = 0
    fees_period_start: int = 0
    fees_last_report: int = 0
    fees_costs_sats: int = 0  # Rebalance costs in period (for net profit settlement)
    # Capabilities for version-aware feature negotiation (e.g., ["mcf"])
    capabilities: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Optional['HivePeerState']:
        """
        Create from dictionary with backward compatibility.

        Handles old nodes that don't send budget fields by using defaults.
        Returns None if peer_id is missing or empty.
        """
        # Required fields
        peer_id = data.get("peer_id", "")
        if not peer_id:
            return None
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

        # Fee reporting fields (optional, backward compatible defaults)
        fees_earned_sats = data.get("fees_earned_sats", 0)
        fees_forward_count = data.get("fees_forward_count", 0)
        fees_period_start = data.get("fees_period_start", 0)
        fees_last_report = data.get("fees_last_report", 0)
        fees_costs_sats = data.get("fees_costs_sats", 0)

        # Capabilities (optional, backward compatible - old nodes have no capabilities)
        capabilities = data.get("capabilities", [])

        return cls(
            peer_id=peer_id,
            capacity_sats=capacity_sats,
            available_sats=available_sats,
            fee_policy=dict(fee_policy),       # defensive copy
            topology=list(topology),           # defensive copy
            version=version,
            last_update=last_update,
            state_hash=state_hash,
            budget_available_sats=budget_available_sats,
            budget_reserved_until=budget_reserved_until,
            budget_last_update=budget_last_update,
            fees_earned_sats=fees_earned_sats,
            fees_forward_count=fees_forward_count,
            fees_period_start=fees_period_start,
            fees_last_report=fees_last_report,
            fees_costs_sats=fees_costs_sats,
            capabilities=list(capabilities),   # defensive copy
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
    - _local_state dict protected by _lock for thread-safe access
    - State hash calculation acquires lock for consistent snapshot
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
        self._lock = threading.Lock()  # Protects _local_state access
        self._local_state: Dict[str, HivePeerState] = {}
        self._last_hash: str = ""
        self._last_hash_time: int = 0

        # Load persisted state from database on startup
        self._load_state_from_db()
    
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

        # Validate capabilities field (prevent unbounded arrays or non-string entries)
        capabilities = data.get("capabilities", [])
        if not isinstance(capabilities, list) or len(capabilities) > 20:
            return False
        for cap in capabilities:
            if not isinstance(cap, str) or len(cap) > 32:
                return False

        # Cap available at capacity (don't mutate caller's dict — caller handles it)
        if data.get('available_sats', 0) > data.get('capacity_sats', 0):
            return False

        return True

    def _load_state_from_db(self) -> int:
        """
        Load persisted state from database into memory on startup.

        This ensures state_manager has data immediately available after
        plugin restart, rather than waiting for gossip messages.

        Returns:
            Number of peer states loaded
        """
        try:
            states = self.db.get_all_hive_states()
            loaded = 0

            with self._lock:
                for state_data in states:
                    peer_id = state_data.get('peer_id')
                    if not peer_id:
                        continue

                    # Create HivePeerState from DB data using from_dict for
                    # defensive copies and consistent field handling
                    state_data['last_update'] = state_data.get('last_gossip', 0)
                    peer_state = HivePeerState.from_dict(state_data)
                    if peer_state:
                        self._local_state[peer_id] = peer_state
                        loaded += 1

            if loaded > 0:
                self._log(f"Loaded {loaded} peer states from database")
            return loaded
        except Exception as e:
            self._log(f"Failed to load state from DB: {e}", level="warn")
            return 0

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

        with self._lock:
            # Check if we have existing state
            existing = self._local_state.get(peer_id)
            if existing and existing.version >= remote_version:
                self._log(f"Rejected stale gossip from {peer_id[:16]}... "
                         f"(local v{existing.version} >= remote v{remote_version})")
                return False

            # Create new state entry (use from_dict for defensive copies and field defaults)
            now = int(time.time())
            # Cap available_sats at capacity_sats
            avail = gossip_data.get('available_sats', 0)
            cap = gossip_data.get('capacity_sats', 0)
            if avail > cap:
                avail = cap

            new_state = HivePeerState(
                peer_id=peer_id,
                capacity_sats=cap,
                available_sats=avail,
                fee_policy=dict(gossip_data.get('fee_policy', {})),  # defensive copy
                topology=list(gossip_data.get('topology', [])),      # defensive copy
                version=remote_version,
                last_update=gossip_data.get('timestamp', now),
                state_hash=gossip_data.get('state_hash', ""),
                # Budget fields (Phase 8 - backward compatible, defaults to 0)
                budget_available_sats=gossip_data.get('budget_available_sats', 0),
                budget_reserved_until=gossip_data.get('budget_reserved_until', 0),
                budget_last_update=gossip_data.get('budget_last_update', 0),
                # Capabilities (MCF support, etc. - backward compatible, defaults to empty)
                capabilities=list(gossip_data.get('capabilities', [])),  # defensive copy
            )

            # Update in-memory cache
            self._local_state[peer_id] = new_state

        # Persist to database outside lock (DB has its own thread-local connections)
        self.db.update_hive_state(
            peer_id=peer_id,
            capacity_sats=new_state.capacity_sats,
            available_sats=new_state.available_sats,
            fee_policy=new_state.fee_policy,
            topology=new_state.topology,
            state_hash=new_state.state_hash,
            version=remote_version
        )

        self._log(f"Updated state for {peer_id[:16]}... to v{remote_version}")
        return True

    def update_peer_fees(self, peer_id: str, fees_earned_sats: int,
                         forward_count: int, period_start: int,
                         period_end: int, rebalance_costs_sats: int = 0) -> bool:
        """
        Update fee reporting data for a peer from FEE_REPORT message.

        This is called when we receive a FEE_REPORT gossip message from
        another hive member, allowing settlement calculations to use
        accurate fee data from all members.

        Args:
            peer_id: The peer's public key
            fees_earned_sats: Cumulative fees earned in the period
            forward_count: Number of forwards in the period
            period_start: Period start timestamp
            period_end: Period end timestamp (report time)
            rebalance_costs_sats: Rebalancing costs in the period

        Returns:
            True if fee data was updated, False if rejected
        """
        now = int(time.time())

        # Basic validation (before lock)
        if not peer_id or fees_earned_sats < 0 or forward_count < 0:
            return False
        if rebalance_costs_sats < 0:
            rebalance_costs_sats = 0

        with self._lock:
            # Get or create peer state
            existing = self._local_state.get(peer_id)

            if existing:
                # Only update if this report is newer
                if existing.fees_last_report >= period_end:
                    self._log(f"Rejected stale fee report from {peer_id[:16]}...")
                    return False

                # Update fee fields while preserving other state
                existing.fees_earned_sats = fees_earned_sats
                existing.fees_forward_count = forward_count
                existing.fees_period_start = period_start
                existing.fees_last_report = period_end
                existing.fees_costs_sats = rebalance_costs_sats
            else:
                # Create minimal state entry with just fee data
                new_state = HivePeerState(
                    peer_id=peer_id,
                    capacity_sats=0,
                    available_sats=0,
                    fee_policy={},
                    topology=[],
                    version=0,
                    last_update=now,
                    fees_earned_sats=fees_earned_sats,
                    fees_forward_count=forward_count,
                    fees_period_start=period_start,
                    fees_last_report=period_end,
                    fees_costs_sats=rebalance_costs_sats
                )
                self._local_state[peer_id] = new_state

        self._log(f"Updated fees for {peer_id[:16]}...: {fees_earned_sats} sats, "
                 f"{forward_count} forwards, costs={rebalance_costs_sats}")
        return True

    def get_peer_fees(self, peer_id: str) -> Dict[str, int]:
        """
        Get fee reporting data for a peer.

        Args:
            peer_id: The peer's public key

        Returns:
            Dict with fees_earned_sats, forward_count, period_start, last_report, rebalance_costs_sats
        """
        with self._lock:
            state = self._local_state.get(peer_id)
            if not state:
                return {
                    "fees_earned_sats": 0,
                    "forward_count": 0,
                    "period_start": 0,
                    "last_report": 0,
                    "rebalance_costs_sats": 0
                }

            return {
                "fees_earned_sats": state.fees_earned_sats,
                "forward_count": state.fees_forward_count,
                "period_start": state.fees_period_start,
                "last_report": state.fees_last_report,
                "rebalance_costs_sats": state.fees_costs_sats
            }

    def get_all_peer_fees(self) -> Dict[str, Dict[str, int]]:
        """
        Get fee reporting data for all peers.

        Returns:
            Dict mapping peer_id to fee data dict
        """
        with self._lock:
            result = {}
            for peer_id, state in self._local_state.items():
                result[peer_id] = {
                    "fees_earned_sats": state.fees_earned_sats,
                    "forward_count": state.fees_forward_count,
                    "period_start": state.fees_period_start,
                    "last_report": state.fees_last_report,
                    "rebalance_costs_sats": state.fees_costs_sats
                }
            return result

    def update_local_state(self, capacity_sats: int, available_sats: int,
                           fee_policy: Dict[str, Any], topology: List[str],
                           our_pubkey: str, force_version: Optional[int] = None) -> HivePeerState:
        """
        Update our own node's state in the HiveMap.

        Called after local changes (fee updates, channel opens, etc.)
        to prepare for outbound gossip.

        Only increments version if state has actually changed to avoid
        unnecessary gossip traffic and version inflation, unless force_version
        is provided (used by GossipManager to ensure persisted version matches
        the version sent in gossip messages, critical for restart recovery).

        Args:
            capacity_sats: Our total Hive channel capacity
            available_sats: Our available outbound liquidity
            fee_policy: Our current fee policy dict
            topology: List of our external peer connections
            our_pubkey: Our node's public key
            force_version: If provided, use this version instead of calculating.
                           Used by GossipManager to sync gossip version to DB.

        Returns:
            The updated HivePeerState for our node
        """
        now = int(time.time())

        with self._lock:
            existing = self._local_state.get(our_pubkey)

            # Check if state has actually changed
            state_changed = True
            if existing:
                # Compare relevant fields (not version, timestamp, or state_hash)
                state_changed = (
                    existing.capacity_sats != capacity_sats or
                    existing.available_sats != available_sats or
                    existing.fee_policy != fee_policy or
                    set(existing.topology) != set(topology)
                )

            # Use forced version from GossipManager if provided (ensures persistence matches gossip)
            # Otherwise, only increment version if state changed
            if force_version is not None:
                new_version = force_version
            elif state_changed:
                new_version = (existing.version + 1) if existing else 1
                self._log(f"State changed for {our_pubkey[:16]}..., incrementing to v{new_version}")
            else:
                # No change - keep existing version
                new_version = existing.version if existing else 1

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

        # Persist to database outside lock (DB has its own thread-local connections)
        if state_changed or force_version is not None:
            self.db.update_hive_state(
                peer_id=our_pubkey,
                capacity_sats=capacity_sats,
                available_sats=available_sats,
                fee_policy=fee_policy,
                topology=topology,
                state_hash="",
                version=new_version  # Persist the version we calculated
            )

        return our_state
    
    def get_peer_state(self, peer_id: str) -> Optional[HivePeerState]:
        """Get cached state for a specific peer (returns a defensive copy)."""
        with self._lock:
            state = self._local_state.get(peer_id)
            if state is None:
                return None
            return HivePeerState.from_dict(state.to_dict())

    def get_all_peer_states(self) -> List[HivePeerState]:
        """Get all cached peer states (returns defensive copies for thread safety)."""
        with self._lock:
            return [
                HivePeerState.from_dict(state.to_dict())
                for state in self._local_state.values()
            ]

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

        with self._lock:
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
        with self._lock:
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
        with self._lock:
            # Extract minimal state tuples (snapshot while holding lock)
            state_tuples = [
                state.to_hash_tuple()
                for state in self._local_state.values()
            ]

        # Sort by peer_id for determinism (can release lock for CPU work)
        state_tuples.sort(key=lambda x: x['peer_id'])

        # Serialize to canonical JSON
        json_str = json.dumps(state_tuples, sort_keys=True, separators=(',', ':'))

        # Calculate SHA256
        hash_bytes = hashlib.sha256(json_str.encode('utf-8')).digest()
        hash_hex = hash_bytes.hex()

        with self._lock:
            self._last_hash = hash_hex
            self._last_hash_time = int(time.time())

        return hash_hex
    
    def get_cached_hash(self) -> Tuple[str, int]:
        """
        Get the cached fleet hash if still fresh.

        Returns:
            Tuple of (hash_hex, age_seconds)
        """
        with self._lock:
            last_hash = self._last_hash
            last_hash_time = self._last_hash_time
        age = int(time.time()) - last_hash_time
        return (last_hash, age)
    
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
        with self._lock:
            return [state.to_dict() for state in self._local_state.values()]

    def apply_full_sync(self, remote_states: List[Dict[str, Any]]) -> int:
        """
        Apply a FULL_SYNC payload to update local state.

        Merges remote state, preferring higher versions.
        The entire batch is applied atomically under a single lock
        to prevent concurrent hash calculations from seeing partial state.

        Args:
            remote_states: List of peer state dictionaries

        Returns:
            Number of states that were updated
        """
        # Validate all entries before acquiring lock
        validated = []
        for state_dict in remote_states:
            peer_id = state_dict.get('peer_id')
            if not peer_id:
                continue
            if not self._validate_state_entry(state_dict):
                self._log(f"Rejected invalid FULL_SYNC entry for {peer_id[:16]}...", level="warn")
                continue
            new_state = HivePeerState.from_dict(state_dict)
            if new_state is None:
                continue
            validated.append((peer_id, new_state, state_dict.get('version', 0)))

        # Apply all updates atomically under a single lock
        updated_count = 0
        states_to_persist = []

        with self._lock:
            for peer_id, new_state, remote_version in validated:
                local_state = self._local_state.get(peer_id)

                # Only update if remote is newer
                if not local_state or local_state.version < remote_version:
                    self._local_state[peer_id] = new_state
                    states_to_persist.append((peer_id, new_state, remote_version))
                    updated_count += 1

        # Persist to database outside lock (DB has version guard)
        for peer_id, new_state, remote_version in states_to_persist:
            self.db.update_hive_state(
                peer_id=peer_id,
                capacity_sats=new_state.capacity_sats,
                available_sats=new_state.available_sats,
                fee_policy=new_state.fee_policy,
                topology=new_state.topology,
                state_hash=new_state.state_hash,
                version=remote_version
            )

        self._log(f"FULL_SYNC applied: {updated_count} states updated")
        return updated_count
    
    # =========================================================================
    # INITIALIZATION & PERSISTENCE
    # =========================================================================
    
    def load_from_database(self) -> int:
        """
        Load cached state from database.

        Only loads entries that are newer than what's already in memory,
        so this is safe to call after gossip has already been received.

        Returns:
            Number of states actually loaded or updated
        """
        db_states = self.db.get_all_hive_states()
        loaded = 0

        with self._lock:
            for state_dict in db_states:
                peer_id = state_dict.get('peer_id')
                if not peer_id:
                    continue
                # DB uses 'last_gossip', HivePeerState uses 'last_update'
                state_dict['last_update'] = state_dict.get('last_gossip', 0)
                peer_state = HivePeerState.from_dict(state_dict)
                if not peer_state:
                    continue

                # Only load if we don't have a newer version in memory
                existing = self._local_state.get(peer_id)
                if not existing or existing.version < peer_state.version:
                    self._local_state[peer_id] = peer_state
                    loaded += 1

        if loaded > 0:
            self._log(f"Loaded {loaded} peer states from database")
        return loaded
    
    def cleanup_stale_states(self, max_age_seconds: int = STALE_STATE_THRESHOLD) -> int:
        """
        Remove states that haven't been updated recently.

        Removes from both in-memory cache and database to prevent
        stale entries from reappearing after restart.

        Args:
            max_age_seconds: Maximum age before state is considered stale

        Returns:
            Number of states removed
        """
        now = int(time.time())
        cutoff = now - max_age_seconds

        with self._lock:
            stale_peers = [
                peer_id for peer_id, state in self._local_state.items()
                if state.last_update < cutoff
            ]

            for peer_id in stale_peers:
                del self._local_state[peer_id]

        # Also remove from database outside lock
        for peer_id in stale_peers:
            try:
                self.db.delete_hive_state(peer_id)
            except Exception as e:
                self._log(f"Failed to delete stale state from DB for {peer_id[:16]}...: {e}",
                         level="warn")

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
        with self._lock:
            states = [
                HivePeerState.from_dict(state.to_dict())
                for state in self._local_state.values()
            ]

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
