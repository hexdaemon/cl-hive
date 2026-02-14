"""
Network Metrics Module for cl-hive.

Provides centralized calculation and caching of network position metrics
for hive members. These metrics are used across multiple modules:

- Routing Pool: Fair share calculation for revenue distribution
- Membership: Promotion eligibility evaluation
- Planner: Channel open target prioritization
- Rebalancing: Identify optimal rebalance paths through central nodes
- Fee Coordination: Adjust strategies based on network position
- Rationalization: Decide which redundant channels to close

Key Metrics:
- Centrality: Approximated betweenness centrality (routing importance)
- Unique Peers: External peers only this member connects to
- Bridge Score: Ratio indicating bridge function (connecting clusters)
- Hive Centrality: Internal fleet connectivity (rebalance hub potential)

Author: Lightning Goats Team
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


# =============================================================================
# CONSTANTS
# =============================================================================

# Cache TTL in seconds (metrics don't change rapidly)
METRICS_CACHE_TTL = 300  # 5 minutes

# Normalization constants
MAX_EXTERNAL_CENTRALITY = 0.1   # Typical max betweenness centrality
MAX_UNIQUE_PEERS = 50           # Normalize unique peer count
MAX_HIVE_CENTRALITY = 1.0       # Already normalized 0-1

# Minimum topology size to be considered "well connected"
MIN_WELL_CONNECTED_PEERS = 5


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class MemberPositionMetrics:
    """
    Network position metrics for a hive member.

    Attributes:
        member_id: Node public key

        # External network metrics (connections outside hive)
        external_centrality: Betweenness centrality approximation (0-0.1 typical)
        external_peer_count: Total external peer connections
        unique_peers: Peers only this member connects to
        bridge_score: Ratio of unique to total (0-1, higher = more bridge-like)

        # Hive internal metrics (connections within fleet)
        hive_centrality: Internal fleet connectivity score (0-1)
        hive_peer_count: Number of direct hive member connections
        hive_reachability: Fraction of fleet reachable in 1-2 hops

        # Computed scores
        overall_position_score: Weighted combination for pool share
        rebalance_hub_score: Suitability as rebalance intermediary

        # Metadata
        calculated_at: Timestamp of calculation
    """
    member_id: str

    # External metrics
    external_centrality: float = 0.0
    external_peer_count: int = 0
    unique_peers: int = 0
    unique_peer_list: List[str] = field(default_factory=list)
    bridge_score: float = 0.0

    # Hive internal metrics
    hive_centrality: float = 0.0
    hive_peer_count: int = 0
    hive_reachability: float = 0.0

    # Computed scores
    overall_position_score: float = 0.0
    rebalance_hub_score: float = 0.0

    # Metadata
    calculated_at: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "member_id": self.member_id,
            "external_centrality": round(self.external_centrality, 6),
            "external_peer_count": self.external_peer_count,
            "unique_peers": self.unique_peers,
            "bridge_score": round(self.bridge_score, 4),
            "hive_centrality": round(self.hive_centrality, 4),
            "hive_peer_count": self.hive_peer_count,
            "hive_reachability": round(self.hive_reachability, 4),
            "overall_position_score": round(self.overall_position_score, 4),
            "rebalance_hub_score": round(self.rebalance_hub_score, 4),
            "calculated_at": self.calculated_at,
        }


@dataclass
class FleetTopologySnapshot:
    """
    Snapshot of fleet topology for metric calculations.

    Captures the state of all member connections at a point in time
    to enable consistent calculations across multiple members.
    """
    # Member ID -> set of external peer pubkeys
    member_topologies: Dict[str, Set[str]] = field(default_factory=dict)

    # Member ID -> set of hive member pubkeys they're connected to
    member_hive_connections: Dict[str, Set[str]] = field(default_factory=dict)

    # All external peers across fleet
    all_external_peers: Set[str] = field(default_factory=set)

    # All hive member pubkeys
    all_members: Set[str] = field(default_factory=set)

    # Statistics
    avg_topology_size: float = 0.0
    total_unique_coverage: int = 0

    # Timestamp
    captured_at: int = 0


# =============================================================================
# NETWORK METRICS CALCULATOR
# =============================================================================

class NetworkMetricsCalculator:
    """
    Calculates and caches network position metrics for hive members.

    Thread-safe with TTL-based caching to avoid redundant calculations.

    Usage:
        calculator = NetworkMetricsCalculator(state_manager, database)

        # Get metrics for a single member
        metrics = calculator.get_member_metrics("03abc...")

        # Get metrics for all members
        all_metrics = calculator.get_all_metrics()

        # Get best rebalance hubs
        hubs = calculator.get_rebalance_hubs(top_n=3)
    """

    def __init__(
        self,
        state_manager=None,
        database=None,
        plugin=None,
        cache_ttl: int = METRICS_CACHE_TTL
    ):
        """
        Initialize the calculator.

        Args:
            state_manager: StateManager for peer state (topology data)
            database: HiveDatabase for member list
            plugin: Plugin for logging
            cache_ttl: Cache lifetime in seconds
        """
        self.state_manager = state_manager
        self.db = database
        self.plugin = plugin
        self.cache_ttl = cache_ttl

        # Cache
        self._cache: Dict[str, MemberPositionMetrics] = {}
        self._cache_time: int = 0
        self._topology_snapshot: Optional[FleetTopologySnapshot] = None
        self._lock = threading.RLock()

    def _log(self, msg: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[NetworkMetrics] {msg}", level=level)

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def get_member_metrics(
        self,
        member_id: str,
        force_refresh: bool = False
    ) -> Optional[MemberPositionMetrics]:
        """
        Get position metrics for a specific member.

        Args:
            member_id: Node public key
            force_refresh: Bypass cache and recalculate

        Returns:
            MemberPositionMetrics or None if member not found
        """
        with self._lock:
            # Check cache
            if not force_refresh and self._is_cache_valid():
                if member_id in self._cache:
                    return self._cache[member_id]

            # Refresh all metrics (more efficient than single calculation)
            self._refresh_all_metrics()

            return self._cache.get(member_id)

    def get_all_metrics(
        self,
        force_refresh: bool = False
    ) -> Dict[str, MemberPositionMetrics]:
        """
        Get position metrics for all hive members.

        Args:
            force_refresh: Bypass cache and recalculate

        Returns:
            Dict mapping member_id to MemberPositionMetrics
        """
        with self._lock:
            if not force_refresh and self._is_cache_valid():
                return dict(self._cache)

            self._refresh_all_metrics()
            return dict(self._cache)

    def get_rebalance_hubs(
        self,
        top_n: int = 3,
        exclude_members: List[str] = None
    ) -> List[MemberPositionMetrics]:
        """
        Get the best members to use as rebalance intermediaries.

        High hive_centrality nodes make good rebalance hubs because:
        - They have connections to many fleet members
        - They can route rebalances between otherwise disconnected members
        - Zero-fee hive channels make them cost-effective paths

        Args:
            top_n: Number of top hubs to return
            exclude_members: Member IDs to exclude (e.g., source/dest of rebalance)

        Returns:
            List of MemberPositionMetrics sorted by rebalance_hub_score descending
        """
        exclude = set(exclude_members or [])

        all_metrics = self.get_all_metrics()

        candidates = [
            m for m in all_metrics.values()
            if m.member_id not in exclude and m.hive_peer_count > 0
        ]

        # Sort by rebalance hub score (higher is better)
        candidates.sort(key=lambda m: m.rebalance_hub_score, reverse=True)

        return candidates[:top_n]

    def get_unique_peers(self, member_id: str) -> List[str]:
        """
        Get list of external peers only this member connects to.

        Convenience method for backward compatibility with membership.py.

        Args:
            member_id: Node public key

        Returns:
            List of unique peer pubkeys
        """
        metrics = self.get_member_metrics(member_id)
        if metrics:
            return metrics.unique_peer_list
        return []

    def get_topology_snapshot(self) -> Optional[FleetTopologySnapshot]:
        """
        Get the current topology snapshot (refreshes if stale).

        Returns:
            FleetTopologySnapshot or None if no data available
        """
        with self._lock:
            if not self._is_cache_valid():
                self._refresh_all_metrics()
            return self._topology_snapshot

    def invalidate_cache(self) -> None:
        """Force cache invalidation (call after topology changes)."""
        with self._lock:
            self._cache_time = 0

    # =========================================================================
    # INTERNAL CALCULATION
    # =========================================================================

    def _is_cache_valid(self) -> bool:
        """Check if cache is still valid."""
        return (
            self._cache_time > 0 and
            (int(time.time()) - self._cache_time) < self.cache_ttl
        )

    def _refresh_all_metrics(self) -> None:
        """Recalculate metrics for all members."""
        now = int(time.time())

        # Build topology snapshot
        snapshot = self._build_topology_snapshot()
        if not snapshot or not snapshot.all_members:
            self._log("No members found for metrics calculation")
            return

        self._topology_snapshot = snapshot

        # Calculate metrics for each member
        new_cache = {}
        for member_id in snapshot.all_members:
            metrics = self._calculate_member_metrics(member_id, snapshot)
            if metrics:
                new_cache[member_id] = metrics

        self._cache = new_cache
        self._cache_time = now

        self._log(f"Refreshed metrics for {len(new_cache)} members")

    def _build_topology_snapshot(self) -> Optional[FleetTopologySnapshot]:
        """Build a snapshot of current fleet topology."""
        if not self.db or not self.state_manager:
            return None

        snapshot = FleetTopologySnapshot(captured_at=int(time.time()))

        # Get all members
        members = self.db.get_all_members()
        if not members:
            return None

        member_ids = {m['peer_id'] for m in members if m.get('peer_id')}
        snapshot.all_members = member_ids

        topology_sizes = []

        for member_id in member_ids:
            state = self.state_manager.get_peer_state(member_id)
            if not state:
                snapshot.member_topologies[member_id] = set()
                snapshot.member_hive_connections[member_id] = set()
                continue

            # External topology (non-hive peers)
            external_topology = set(getattr(state, 'topology', []) or [])
            snapshot.member_topologies[member_id] = external_topology
            snapshot.all_external_peers.update(external_topology)
            topology_sizes.append(len(external_topology))

            # Hive connections (other fleet members this node is connected to)
            # This would come from channel data - for now approximate from who we see
            hive_connections = set()
            for other_id in member_ids:
                if other_id != member_id:
                    other_state = self.state_manager.get_peer_state(other_id)
                    if other_state:
                        # If we can see their state, assume connectivity
                        hive_connections.add(other_id)
            snapshot.member_hive_connections[member_id] = hive_connections

        # Calculate statistics
        if topology_sizes:
            snapshot.avg_topology_size = sum(topology_sizes) / len(topology_sizes)

        # Count unique coverage
        all_peers = set()
        for topo in snapshot.member_topologies.values():
            all_peers.update(topo)
        snapshot.total_unique_coverage = len(all_peers)

        return snapshot

    def _calculate_member_metrics(
        self,
        member_id: str,
        snapshot: FleetTopologySnapshot
    ) -> Optional[MemberPositionMetrics]:
        """Calculate metrics for a single member using the snapshot."""
        if member_id not in snapshot.all_members:
            return None

        metrics = MemberPositionMetrics(
            member_id=member_id,
            calculated_at=snapshot.captured_at
        )

        member_topology = snapshot.member_topologies.get(member_id, set())
        hive_connections = snapshot.member_hive_connections.get(member_id, set())

        # -----------------------------------------------------------------
        # External Network Metrics
        # -----------------------------------------------------------------

        metrics.external_peer_count = len(member_topology)

        # Find unique peers (only this member connects to)
        other_members_peers = set()
        for other_id, other_topo in snapshot.member_topologies.items():
            if other_id != member_id:
                other_members_peers.update(other_topo)

        unique_peer_set = member_topology - other_members_peers
        metrics.unique_peers = len(unique_peer_set)
        metrics.unique_peer_list = list(unique_peer_set)

        # Bridge score
        if len(member_topology) > 0:
            metrics.bridge_score = min(1.0, metrics.unique_peers / len(member_topology))

        # External centrality approximation
        if snapshot.avg_topology_size > 0:
            relative_connectivity = len(member_topology) / snapshot.avg_topology_size
            bridge_boost = 1.0 + (metrics.bridge_score * 0.5)
            metrics.external_centrality = min(
                MAX_EXTERNAL_CENTRALITY,
                0.01 * relative_connectivity * bridge_boost
            )

        # -----------------------------------------------------------------
        # Hive Internal Metrics
        # -----------------------------------------------------------------

        metrics.hive_peer_count = len(hive_connections)

        # Hive centrality: fraction of fleet directly connected
        fleet_size = len(snapshot.all_members)
        if fleet_size > 1:
            metrics.hive_centrality = len(hive_connections) / (fleet_size - 1)

        # Hive reachability: what fraction of fleet can be reached in 1-2 hops
        reachable = set(hive_connections)  # 1-hop
        for connected_id in hive_connections:
            # 2-hop: peers of our peers
            connected_peers = snapshot.member_hive_connections.get(connected_id, set())
            reachable.update(connected_peers)
        reachable.discard(member_id)  # Don't count self

        if fleet_size > 1:
            metrics.hive_reachability = len(reachable) / (fleet_size - 1)

        # -----------------------------------------------------------------
        # Computed Scores
        # -----------------------------------------------------------------

        # Overall position score (for pool share calculation)
        # Weighted: 40% centrality, 30% unique peers, 30% bridge
        centrality_norm = min(1.0, metrics.external_centrality / MAX_EXTERNAL_CENTRALITY)
        unique_norm = min(1.0, metrics.unique_peers / MAX_UNIQUE_PEERS)

        metrics.overall_position_score = (
            centrality_norm * 0.4 +
            unique_norm * 0.3 +
            metrics.bridge_score * 0.3
        )

        # Rebalance hub score
        # High score = good choice for routing internal rebalances
        # Factors: hive centrality (most important), reachability, some external connectivity
        metrics.rebalance_hub_score = (
            metrics.hive_centrality * 0.5 +      # Direct fleet connections
            metrics.hive_reachability * 0.3 +    # Can reach rest of fleet
            min(1.0, metrics.external_peer_count / 10) * 0.2  # Some external presence
        )

        return metrics

    # =========================================================================
    # REBALANCING UTILITIES
    # =========================================================================

    def find_best_rebalance_path(
        self,
        source_member: str,
        dest_member: str,
        max_hops: int = 2
    ) -> Optional[List[str]]:
        """
        Find the best path for an internal hive rebalance.

        For zero-fee hive rebalances, we want to route through
        high-centrality members when direct path isn't available.

        Args:
            source_member: Starting member pubkey
            dest_member: Ending member pubkey
            max_hops: Maximum intermediaries (default 2)

        Returns:
            List of member pubkeys forming path, or None if no path found
        """
        snapshot = self.get_topology_snapshot()
        if not snapshot:
            return None

        source_conns = snapshot.member_hive_connections.get(source_member, set())
        dest_conns = snapshot.member_hive_connections.get(dest_member, set())

        # Direct connection?
        if dest_member in source_conns:
            return [source_member, dest_member]

        # 1-hop intermediary: find common connections
        common = source_conns & dest_conns
        if common:
            # Pick the one with highest hive centrality
            all_metrics = self.get_all_metrics()
            best_hub = max(
                common,
                key=lambda m: all_metrics.get(m, MemberPositionMetrics(m)).rebalance_hub_score
            )
            return [source_member, best_hub, dest_member]

        # 2-hop: find path through any two intermediaries
        if max_hops >= 2:
            for mid1 in source_conns:
                mid1_conns = snapshot.member_hive_connections.get(mid1, set())
                for mid2 in mid1_conns:
                    if mid2 in dest_conns and mid2 != source_member:
                        return [source_member, mid1, mid2, dest_member]

        return None

    def get_rebalance_recommendations(
        self,
        amount_sats: int = 100000
    ) -> List[Dict[str, Any]]:
        """
        Get recommendations for using high-centrality nodes in rebalances.

        Returns insights about which members could serve as efficient
        rebalance hubs for the fleet.

        Args:
            amount_sats: Typical rebalance amount for context

        Returns:
            List of recommendation dicts with hub info and rationale
        """
        hubs = self.get_rebalance_hubs(top_n=5)

        recommendations = []
        for hub in hubs:
            rec = {
                "member_id": hub.member_id,
                "member_id_short": hub.member_id[:16] + "...",
                "rebalance_hub_score": hub.rebalance_hub_score,
                "hive_centrality": hub.hive_centrality,
                "hive_peer_count": hub.hive_peer_count,
                "hive_reachability": hub.hive_reachability,
                "rationale": self._generate_hub_rationale(hub),
                "suggested_use": "zero_fee_intermediary" if hub.hive_centrality > 0.5 else "backup_path"
            }
            recommendations.append(rec)

        return recommendations

    def _generate_hub_rationale(self, hub: MemberPositionMetrics) -> str:
        """Generate human-readable rationale for hub recommendation."""
        parts = []

        if hub.hive_centrality >= 0.8:
            parts.append("Excellent fleet connectivity")
        elif hub.hive_centrality >= 0.5:
            parts.append("Good fleet connectivity")
        else:
            parts.append("Moderate fleet connectivity")

        if hub.hive_reachability >= 0.9:
            parts.append("can reach entire fleet in 2 hops")
        elif hub.hive_reachability >= 0.7:
            parts.append(f"can reach {hub.hive_reachability:.0%} of fleet")

        if hub.external_peer_count >= 10:
            parts.append(f"{hub.external_peer_count} external peers")

        return "; ".join(parts)

    # =========================================================================
    # FLEET HEALTH MONITORING
    # =========================================================================

    def get_fleet_health(self) -> Dict[str, Any]:
        """
        Get overall fleet connectivity health metrics.

        Returns aggregated metrics showing how well-connected the fleet is
        internally. Used for monitoring and alerting.

        Returns:
            Dict with fleet health metrics including:
            - member_count: Total members analyzed
            - avg_hive_centrality: Average internal connectivity (0-1)
            - avg_hive_reachability: Average fleet reachability (0-1)
            - fully_connected_count: Members with 100% reachability
            - isolated_count: Members with <50% reachability
            - hub_count: Members suitable as rebalance hubs
            - health_score: Overall health score (0-100)
            - health_grade: Letter grade (A-F)
        """
        all_metrics = self.get_all_metrics()

        if not all_metrics:
            return {
                "member_count": 0,
                "health_score": 0,
                "health_grade": "F",
                "error": "No members found"
            }

        members = list(all_metrics.values())
        member_count = len(members)

        # Calculate averages
        avg_centrality = sum(m.hive_centrality for m in members) / member_count
        avg_reachability = sum(m.hive_reachability for m in members) / member_count
        avg_hub_score = sum(m.rebalance_hub_score for m in members) / member_count

        # Count categories
        fully_connected = sum(1 for m in members if m.hive_reachability >= 0.99)
        isolated = sum(1 for m in members if m.hive_reachability < 0.5)
        hubs = sum(1 for m in members if m.rebalance_hub_score >= 0.5)

        # Members with no hive connections
        disconnected = sum(1 for m in members if m.hive_peer_count == 0)

        # Calculate health score (0-100)
        # Weighted: 40% avg centrality, 40% avg reachability, 20% hub availability
        health_score = (
            avg_centrality * 40 +
            avg_reachability * 40 +
            (hubs / max(member_count, 1)) * 20
        )

        # Penalize for isolated/disconnected members
        if isolated > 0:
            health_score -= (isolated / member_count) * 20
        if disconnected > 0:
            health_score -= (disconnected / member_count) * 30

        health_score = max(0, min(100, health_score))

        # Assign grade
        if health_score >= 90:
            grade = "A"
        elif health_score >= 80:
            grade = "B"
        elif health_score >= 70:
            grade = "C"
        elif health_score >= 60:
            grade = "D"
        else:
            grade = "F"

        return {
            "member_count": member_count,
            "avg_hive_centrality": round(avg_centrality, 3),
            "avg_hive_reachability": round(avg_reachability, 3),
            "avg_hub_score": round(avg_hub_score, 3),
            "fully_connected_count": fully_connected,
            "isolated_count": isolated,
            "disconnected_count": disconnected,
            "hub_count": hubs,
            "health_score": round(health_score, 1),
            "health_grade": grade,
            "calculated_at": int(time.time())
        }

    def check_connectivity_alerts(self) -> List[Dict[str, Any]]:
        """
        Check for fleet connectivity issues that need attention.

        Returns a list of alerts for:
        - Isolated members (reachability < 50%)
        - Disconnected members (no hive connections)
        - Low hub availability (< 2 good hubs)
        - Declining centrality (if historical data available)

        Returns:
            List of alert dicts with severity, member_id, message
        """
        alerts = []
        all_metrics = self.get_all_metrics()

        if not all_metrics:
            return [{
                "severity": "warning",
                "alert_type": "no_data",
                "message": "No fleet members found for connectivity analysis"
            }]

        members = list(all_metrics.values())
        member_count = len(members)

        # Check for disconnected members (critical)
        for m in members:
            if m.hive_peer_count == 0:
                alerts.append({
                    "severity": "critical",
                    "alert_type": "disconnected",
                    "member_id": m.member_id,
                    "member_id_short": m.member_id[:16] + "...",
                    "message": f"Member has no channels to other fleet members",
                    "recommendation": "Open channel to a high-centrality hub"
                })

        # Check for isolated members (warning)
        for m in members:
            if m.hive_peer_count > 0 and m.hive_reachability < 0.5:
                alerts.append({
                    "severity": "warning",
                    "alert_type": "isolated",
                    "member_id": m.member_id,
                    "member_id_short": m.member_id[:16] + "...",
                    "hive_reachability": round(m.hive_reachability, 2),
                    "message": f"Member can only reach {m.hive_reachability:.0%} of fleet",
                    "recommendation": "Open additional channels to improve connectivity"
                })

        # Check hub availability
        hubs = [m for m in members if m.rebalance_hub_score >= 0.5]
        if len(hubs) < 2 and member_count >= 3:
            alerts.append({
                "severity": "warning",
                "alert_type": "low_hub_availability",
                "hub_count": len(hubs),
                "member_count": member_count,
                "message": f"Only {len(hubs)} rebalance hub(s) available for {member_count} members",
                "recommendation": "Encourage members to open more internal channels"
            })

        # Check for members with very low centrality
        for m in members:
            if m.hive_centrality < 0.2 and m.hive_peer_count > 0:
                alerts.append({
                    "severity": "info",
                    "alert_type": "low_centrality",
                    "member_id": m.member_id,
                    "member_id_short": m.member_id[:16] + "...",
                    "hive_centrality": round(m.hive_centrality, 3),
                    "message": f"Member has low fleet centrality ({m.hive_centrality:.1%})",
                    "recommendation": "Consider opening channel to this member to improve their connectivity"
                })

        # Sort by severity
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        alerts.sort(key=lambda a: severity_order.get(a.get("severity", "info"), 3))

        return alerts

    def get_member_connectivity_report(self, member_id: str) -> Dict[str, Any]:
        """
        Get detailed connectivity report for a specific member.

        Shows how well-connected this member is within the fleet and
        recommendations for improvement.

        Args:
            member_id: Member's public key

        Returns:
            Dict with connectivity details and recommendations
        """
        metrics = self.get_member_metrics(member_id)
        if not metrics:
            return {"error": f"No metrics found for member {member_id[:16]}..."}

        all_metrics = self.get_all_metrics()
        fleet_health = self.get_fleet_health()

        # Find members this node is NOT connected to
        topology = self.get_topology_snapshot()
        if not topology:
            return {"error": "Could not get fleet topology"}

        hive_connections = topology.member_hive_connections.get(member_id, set())
        all_members = set(all_metrics.keys())
        not_connected_to = all_members - hive_connections - {member_id}

        # Find best connection targets (highest centrality nodes we're not connected to)
        connection_targets = []
        for target_id in not_connected_to:
            target_metrics = all_metrics.get(target_id)
            if target_metrics:
                connection_targets.append({
                    "member_id": target_id,
                    "member_id_short": target_id[:16] + "...",
                    "hive_centrality": target_metrics.hive_centrality,
                    "rebalance_hub_score": target_metrics.rebalance_hub_score,
                    "reason": "high_centrality" if target_metrics.hive_centrality > 0.5 else "fleet_member"
                })

        # Sort by centrality
        connection_targets.sort(key=lambda x: x["hive_centrality"], reverse=True)

        # Determine status
        if metrics.hive_peer_count == 0:
            status = "disconnected"
            status_message = "No channels to fleet members - urgent action needed"
        elif metrics.hive_reachability < 0.5:
            status = "isolated"
            status_message = "Limited fleet connectivity - improvement recommended"
        elif metrics.hive_reachability < 0.8:
            status = "partial"
            status_message = "Good connectivity but room for improvement"
        else:
            status = "well_connected"
            status_message = "Excellent fleet connectivity"

        return {
            "member_id": member_id,
            "member_id_short": member_id[:16] + "...",
            "status": status,
            "status_message": status_message,
            "metrics": metrics.to_dict(),
            "fleet_comparison": {
                "your_centrality": metrics.hive_centrality,
                "fleet_avg_centrality": fleet_health.get("avg_hive_centrality", 0),
                "your_reachability": metrics.hive_reachability,
                "fleet_avg_reachability": fleet_health.get("avg_hive_reachability", 0),
                "above_average": metrics.hive_centrality > fleet_health.get("avg_hive_centrality", 0)
            },
            "connections": {
                "connected_to": metrics.hive_peer_count,
                "not_connected_to": len(not_connected_to),
                "total_fleet_members": len(all_members)
            },
            "recommended_connections": connection_targets[:3],
            "calculated_at": int(time.time())
        }


# =============================================================================
# MODULE-LEVEL SINGLETON
# =============================================================================

_calculator: Optional[NetworkMetricsCalculator] = None


def get_calculator() -> Optional[NetworkMetricsCalculator]:
    """Get the global NetworkMetricsCalculator instance."""
    return _calculator


def init_calculator(
    state_manager=None,
    database=None,
    plugin=None,
    cache_ttl: int = METRICS_CACHE_TTL
) -> NetworkMetricsCalculator:
    """
    Initialize the global NetworkMetricsCalculator.

    Call this once during plugin startup.
    """
    global _calculator
    _calculator = NetworkMetricsCalculator(
        state_manager=state_manager,
        database=database,
        plugin=plugin,
        cache_ttl=cache_ttl
    )
    return _calculator
