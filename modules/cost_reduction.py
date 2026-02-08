"""
Phase 3: Cost Reduction Module for Yield Optimization.

This module reduces rebalancing costs by up to 50% through:

1. Predictive Rebalancing: Rebalance BEFORE depletion when urgency is low
2. Fleet Rebalance Routing: Use fleet members as cheaper rebalance hops
3. Circular Flow Detection: Identify and prevent wasteful circular flows

The goal is to move liquidity proactively when fees are low rather than
reactively when desperate and fees are high.

Author: Lightning Goats Team
"""

import time
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict, deque

from . import network_metrics
from .mcf_solver import (
    MCFCoordinator,
    MCFNetworkBuilder,
    SSPSolver,
    RebalanceNeed,
    RebalanceAssignment,
    MCFSolution,
    MIN_MCF_DEMAND,
    MAX_SOLUTION_AGE,
)


# =============================================================================
# CONSTANTS
# =============================================================================

# Predictive rebalancing thresholds
DEPLETION_RISK_THRESHOLD = 0.7      # Trigger preemptive rebalance at 70% risk
SATURATION_RISK_THRESHOLD = 0.7     # Trigger preemptive rebalance at 70% risk
PREEMPTIVE_MAX_FEE_PPM = 500        # Max fee for non-urgent rebalances
URGENT_MAX_FEE_PPM = 2000           # Max fee for urgent rebalances

# Velocity thresholds (% of capacity per hour)
CRITICAL_VELOCITY_PCT = 0.02        # 2% per hour is critical
HIGH_VELOCITY_PCT = 0.01            # 1% per hour is high
NORMAL_VELOCITY_PCT = 0.005         # 0.5% per hour is normal

# Fleet routing thresholds
FLEET_PATH_SAVINGS_THRESHOLD = 0.20  # 20% savings to prefer fleet path
FLEET_FEE_DISCOUNT_PCT = 0.50       # Fleet members charge 50% less internally

# Circular flow detection
CIRCULAR_FLOW_WINDOW_HOURS = 24     # Look back 24 hours
MIN_CIRCULAR_AMOUNT_SATS = 100000   # Minimum amount to flag circular flow
CIRCULAR_FLOW_RATIO_THRESHOLD = 0.8  # 80% flow ratio indicates circular

# Rebalance outcome tracking
REBALANCE_HISTORY_HOURS = 72        # Track rebalances for 72 hours

# Rebalance hub scoring (Use Case 5)
HIGH_HUB_SCORE_THRESHOLD = 0.6      # Score above this is "high" hub potential
PREFER_HUB_SCORE_BONUS = 1.2        # 20% preference bonus for high-hub peers
HUB_SCORE_WEIGHT_IN_PATH = 0.3      # 30% weight for hub score in path selection


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class RebalanceRecommendation:
    """
    Recommendation for a rebalance operation.

    Can be preemptive (low urgency, low max fee) or reactive (high urgency).
    """
    channel_id: str
    peer_id: str
    direction: str  # "inbound" (need more local) or "outbound" (need less local)

    # Risk assessment
    depletion_risk: float = 0.0
    saturation_risk: float = 0.0
    hours_to_critical: Optional[float] = None

    # Recommendation details
    recommended_amount_sats: int = 0
    max_fee_ppm: int = PREEMPTIVE_MAX_FEE_PPM
    urgency: str = "low"  # "low", "medium", "high", "critical"
    reason: str = ""

    # Fleet path info (if available)
    fleet_path_available: bool = False
    fleet_path: List[str] = field(default_factory=list)
    estimated_fleet_cost_sats: int = 0
    estimated_external_cost_sats: int = 0

    # Rebalance hub info (Use Case 5)
    peer_hub_score: float = 0.0
    is_high_hub: bool = False
    preferred_hub_member: Optional[str] = None
    hub_member_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "direction": self.direction,
            "depletion_risk": round(self.depletion_risk, 3),
            "saturation_risk": round(self.saturation_risk, 3),
            "hours_to_critical": round(self.hours_to_critical, 1) if self.hours_to_critical else None,
            "recommended_amount_sats": self.recommended_amount_sats,
            "max_fee_ppm": self.max_fee_ppm,
            "urgency": self.urgency,
            "reason": self.reason,
            "fleet_path_available": self.fleet_path_available,
            "fleet_path": self.fleet_path,
            "estimated_fleet_cost_sats": self.estimated_fleet_cost_sats,
            "estimated_external_cost_sats": self.estimated_external_cost_sats
        }
        # Include hub info if relevant
        if self.is_high_hub or self.preferred_hub_member:
            result["peer_hub_score"] = round(self.peer_hub_score, 3)
            result["is_high_hub"] = self.is_high_hub
            if self.preferred_hub_member:
                result["preferred_hub_member"] = self.preferred_hub_member
                result["hub_member_score"] = round(self.hub_member_score, 3)
        return result


@dataclass
class RebalanceOutcome:
    """
    Record of a completed rebalance operation.

    Used for circular flow detection and cost tracking.
    """
    timestamp: float
    from_channel: str
    to_channel: str
    from_peer: str
    to_peer: str
    amount_sats: int
    cost_sats: int
    success: bool
    via_fleet: bool = False
    member_id: str = ""  # Which fleet member performed this

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "from_channel": self.from_channel,
            "to_channel": self.to_channel,
            "from_peer": self.from_peer,
            "to_peer": self.to_peer,
            "amount_sats": self.amount_sats,
            "cost_sats": self.cost_sats,
            "success": self.success,
            "via_fleet": self.via_fleet,
            "member_id": self.member_id
        }


@dataclass
class CircularFlow:
    """
    Detected circular flow pattern.

    Example: A→B→C→A where A, B, C are fleet members
    This is pure cost with no benefit.
    """
    members: List[str]  # Members involved in the cycle
    total_amount_sats: int
    total_cost_sats: int
    cycle_count: int
    detection_window_hours: float
    recommendation: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "members": self.members,
            "total_amount_sats": self.total_amount_sats,
            "total_cost_sats": self.total_cost_sats,
            "cycle_count": self.cycle_count,
            "detection_window_hours": self.detection_window_hours,
            "recommendation": self.recommendation
        }


@dataclass
class FleetPath:
    """
    A rebalance path through fleet members.
    """
    path: List[str]  # List of member pubkeys
    hops: int
    estimated_cost_sats: int
    estimated_time_seconds: int
    reliability_score: float  # 0-1, based on member health

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "hops": self.hops,
            "estimated_cost_sats": self.estimated_cost_sats,
            "estimated_time_seconds": self.estimated_time_seconds,
            "reliability_score": round(self.reliability_score, 3)
        }


# =============================================================================
# PREDICTIVE REBALANCER
# =============================================================================

class PredictiveRebalancer:
    """
    Rebalance based on predictions, not current state.

    Benefits:
    - Lower urgency = lower fees paid
    - Better timing = more route options
    - Proactive = never desperate

    Uses velocity predictions from yield_metrics.py to determine
    when channels will deplete or saturate.
    """

    def __init__(self, plugin, yield_metrics_mgr=None, state_manager=None):
        """
        Initialize the predictive rebalancer.

        Args:
            plugin: Plugin reference for RPC calls
            yield_metrics_mgr: YieldMetricsManager for velocity predictions
            state_manager: StateManager for fleet state
        """
        self.plugin = plugin
        self.yield_metrics = yield_metrics_mgr
        self.state_manager = state_manager
        self._our_pubkey: Optional[str] = None

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"PREDICTIVE_REBAL: {message}", level=level)

    def get_preemptive_recommendations(
        self,
        prediction_hours: int = 24
    ) -> List[RebalanceRecommendation]:
        """
        Get preemptive rebalance recommendations based on predictions.

        Analyzes all channels and returns recommendations for those
        predicted to deplete or saturate within the time window.

        Args:
            prediction_hours: Hours to look ahead (default: 24)

        Returns:
            List of RebalanceRecommendation sorted by urgency
        """
        recommendations = []

        if not self.yield_metrics:
            self._log("Yield metrics not available", level="warn")
            return recommendations

        try:
            # Get critical velocity channels
            critical_channels = self.yield_metrics.get_critical_velocity_channels(
                threshold_hours=prediction_hours
            )

            for pred in critical_channels:
                rec = self._create_recommendation_from_prediction(pred)
                if rec:
                    recommendations.append(rec)

            # Sort by urgency (critical first, then by hours to critical)
            urgency_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
            recommendations.sort(
                key=lambda r: (
                    urgency_order.get(r.urgency, 4),
                    r.hours_to_critical or 999
                )
            )

            return recommendations

        except Exception as e:
            self._log(f"Error getting preemptive recommendations: {e}", level="warn")
            return recommendations

    def _create_recommendation_from_prediction(self, prediction) -> Optional[RebalanceRecommendation]:
        """
        Create a rebalance recommendation from a velocity prediction.

        Args:
            prediction: ChannelVelocityPrediction from yield_metrics

        Returns:
            RebalanceRecommendation or None if no action needed
        """
        # Determine if we need to act
        depletion_risk = prediction.depletion_risk
        saturation_risk = prediction.saturation_risk

        # No action needed if risks are low
        if depletion_risk < DEPLETION_RISK_THRESHOLD and saturation_risk < SATURATION_RISK_THRESHOLD:
            return None

        # Determine direction and urgency
        if depletion_risk >= DEPLETION_RISK_THRESHOLD:
            direction = "inbound"  # Need more local balance
            risk = depletion_risk
            hours_to_critical = prediction.hours_to_depletion
        else:
            direction = "outbound"  # Need less local balance
            risk = saturation_risk
            hours_to_critical = prediction.hours_to_saturation

        # Calculate urgency based on time remaining
        if hours_to_critical is not None:
            if hours_to_critical < 6:
                urgency = "critical"
                max_fee = URGENT_MAX_FEE_PPM
            elif hours_to_critical < 12:
                urgency = "high"
                max_fee = int(URGENT_MAX_FEE_PPM * 0.75)
            elif hours_to_critical < 24:
                urgency = "medium"
                max_fee = int(PREEMPTIVE_MAX_FEE_PPM * 1.5)
            else:
                urgency = "low"
                max_fee = PREEMPTIVE_MAX_FEE_PPM
        else:
            urgency = "low"
            max_fee = PREEMPTIVE_MAX_FEE_PPM

        # Calculate recommended amount (restore to 50% balance)
        capacity = prediction.capacity_sats
        current_local_pct = prediction.current_local_pct
        target_pct = 0.5  # Target 50% local balance

        if direction == "inbound":
            # Need to increase local balance
            amount = int((target_pct - current_local_pct) * capacity)
        else:
            # Need to decrease local balance
            amount = int((current_local_pct - target_pct) * capacity)

        # Minimum amount check
        if amount < 50000:  # Less than 50k sats not worth it
            return None

        return RebalanceRecommendation(
            channel_id=prediction.channel_id,
            peer_id=prediction.peer_id,
            direction=direction,
            depletion_risk=depletion_risk,
            saturation_risk=saturation_risk,
            hours_to_critical=hours_to_critical,
            recommended_amount_sats=amount,
            max_fee_ppm=max_fee,
            urgency=urgency,
            reason=prediction.recommended_action or f"predicted_{direction}_need"
        )

    def should_preemptive_rebalance(
        self,
        channel_id: str,
        hours: int = 12
    ) -> Optional[RebalanceRecommendation]:
        """
        Check if a specific channel should be preemptively rebalanced.

        Args:
            channel_id: Channel to check
            hours: Prediction window

        Returns:
            RebalanceRecommendation or None if no action needed
        """
        if not self.yield_metrics:
            return None

        try:
            prediction = self.yield_metrics.predict_channel_state(channel_id, hours)
            if prediction:
                return self._create_recommendation_from_prediction(prediction)
            return None

        except Exception as e:
            self._log(f"Error checking channel {channel_id}: {e}", level="debug")
            return None


# =============================================================================
# FLEET REBALANCE ROUTER
# =============================================================================

class FleetRebalanceRouter:
    """
    Find rebalance paths through fleet members when cheaper.

    Fleet members have coordinated fees and can offer internal
    "friendship" rates, making internal paths often cheaper than
    external paths through random network nodes.
    """

    def __init__(self, plugin, state_manager=None, liquidity_coordinator=None):
        """
        Initialize the fleet rebalance router.

        Args:
            plugin: Plugin reference for RPC calls
            state_manager: StateManager for fleet topology
            liquidity_coordinator: LiquidityCoordinator for liquidity state
        """
        self.plugin = plugin
        self.state_manager = state_manager
        self.liquidity_coordinator = liquidity_coordinator
        self._our_pubkey: Optional[str] = None

        # Cache for fleet topology
        self._topology_cache: Dict[str, Set[str]] = {}  # member -> connected peers
        self._topology_cache_time: float = 0
        self._topology_cache_ttl: float = 300  # 5 minutes

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"FLEET_ROUTER: {message}", level=level)

    def _get_fleet_topology(self) -> Dict[str, Set[str]]:
        """
        Get fleet member topology (who is connected to whom).

        Returns cached topology if fresh, otherwise rebuilds from state.
        """
        now = time.time()

        # Return cached if fresh
        if (self._topology_cache and
            now - self._topology_cache_time < self._topology_cache_ttl):
            return self._topology_cache

        # Rebuild from state manager
        topology = {}

        if self.state_manager:
            try:
                all_states = self.state_manager.get_all_peer_states()
                for state in all_states:
                    member_id = state.peer_id
                    # Get peers this member has channels with
                    peers = set(getattr(state, 'topology', []) or [])
                    topology[member_id] = peers
            except Exception as e:
                self._log(f"Error getting fleet topology: {e}", level="debug")

        self._topology_cache = topology
        self._topology_cache_time = now
        return topology

    def _get_fleet_members(self) -> List[str]:
        """Get list of fleet member pubkeys."""
        if not self.state_manager:
            return []

        try:
            return list(self._get_fleet_topology().keys())
        except Exception:
            return []

    def find_fleet_path(
        self,
        from_peer: str,
        to_peer: str,
        amount_sats: int
    ) -> Optional[FleetPath]:
        """
        Find a rebalance path through fleet members.

        Args:
            from_peer: Source peer (where we have excess liquidity)
            to_peer: Destination peer (where we need liquidity)
            amount_sats: Amount to rebalance

        Returns:
            FleetPath if found, None otherwise
        """
        topology = self._get_fleet_topology()
        members = set(topology.keys())

        if not members:
            return None

        # BFS to find shortest path through fleet members
        # Start: our node connected to from_peer
        # End: any fleet member connected to to_peer

        # Find members connected to from_peer
        start_members = []
        for member, peers in topology.items():
            if from_peer in peers:
                start_members.append(member)

        if not start_members:
            return None

        # Find members connected to to_peer
        end_members = set()
        for member, peers in topology.items():
            if to_peer in peers:
                end_members.add(member)

        if not end_members:
            return None

        # If same member connects both peers, direct path
        direct = set(start_members) & end_members
        if direct:
            member = list(direct)[0]
            return FleetPath(
                path=[member],
                hops=1,
                estimated_cost_sats=self._estimate_fleet_cost(amount_sats, 1),
                estimated_time_seconds=30,
                reliability_score=0.9
            )

        # BFS for shortest path
        visited = set()
        queue = deque([(m, [m]) for m in start_members])

        while queue:
            current, path = queue.popleft()

            if current in visited:
                continue
            visited.add(current)

            # Check if we reached an end member
            if current in end_members:
                return FleetPath(
                    path=path,
                    hops=len(path),
                    estimated_cost_sats=self._estimate_fleet_cost(amount_sats, len(path)),
                    estimated_time_seconds=30 * len(path),
                    reliability_score=max(0.5, 1.0 - 0.1 * len(path))
                )

            # Add neighbors (other fleet members this member has a direct channel to)
            current_peers = topology.get(current, set())
            for member in topology:
                if member not in visited and member != current:
                    # Check if current has a direct channel to member
                    # (member appears in current's peer set, or current in member's)
                    member_peers = topology.get(member, set())
                    if member in current_peers or current in member_peers:
                        queue.append((member, path + [member]))

        return None

    def _estimate_fleet_cost(self, amount_sats: int, hops: int) -> int:
        """
        Estimate cost for a fleet rebalance path.

        Fleet members use discounted internal rates.
        """
        # Base fee: 100 ppm * discount
        base_ppm = 100
        discounted_ppm = int(base_ppm * (1 - FLEET_FEE_DISCOUNT_PCT))

        # Cost per hop
        cost_per_hop = (amount_sats * discounted_ppm) // 1_000_000

        return cost_per_hop * hops

    def _estimate_external_cost(self, amount_sats: int) -> int:
        """
        Estimate cost for an external rebalance path.

        Based on typical network fees.
        """
        # Assume average 500 ppm for external routes
        avg_ppm = 500
        avg_hops = 3

        cost_per_hop = (amount_sats * avg_ppm) // 1_000_000
        return cost_per_hop * avg_hops

    def get_best_rebalance_path(
        self,
        from_channel: str,
        to_channel: str,
        amount_sats: int
    ) -> Dict[str, Any]:
        """
        Get the best rebalance path (fleet or external).

        Args:
            from_channel: Source channel SCID
            to_channel: Destination channel SCID
            amount_sats: Amount to rebalance

        Returns:
            Dict with path recommendation
        """
        result = {
            "fleet_path_available": False,
            "fleet_path": [],
            "estimated_fleet_cost_sats": 0,
            "estimated_external_cost_sats": self._estimate_external_cost(amount_sats),
            "savings_pct": 0,
            "recommendation": "use_external_path"
        }

        # Get peers for the channels
        from_peer = self._get_peer_for_channel(from_channel)
        to_peer = self._get_peer_for_channel(to_channel)

        if not from_peer or not to_peer:
            return result

        # Find fleet path
        fleet_path = self.find_fleet_path(from_peer, to_peer, amount_sats)

        if fleet_path:
            result["fleet_path_available"] = True
            result["fleet_path"] = fleet_path.path
            result["estimated_fleet_cost_sats"] = fleet_path.estimated_cost_sats

            # Calculate savings
            external_cost = result["estimated_external_cost_sats"]
            fleet_cost = fleet_path.estimated_cost_sats

            if external_cost > 0:
                savings = (external_cost - fleet_cost) / external_cost
                result["savings_pct"] = round(savings * 100, 1)

                if savings >= FLEET_PATH_SAVINGS_THRESHOLD:
                    result["recommendation"] = "use_fleet_path"

        return result

    def _get_peer_for_channel(self, channel_id: str) -> Optional[str]:
        """Get peer pubkey for a channel ID."""
        if not self.plugin:
            return None

        try:
            channels = self.plugin.rpc.listpeerchannels()
            for ch in channels.get("channels", []):
                scid = ch.get("short_channel_id", "").replace(":", "x")
                if scid == channel_id.replace(":", "x"):
                    return ch.get("peer_id")
            return None
        except Exception:
            return None

    # =========================================================================
    # REBALANCE HUB SCORING (Use Case 5)
    # =========================================================================

    def get_member_hub_scores(self) -> Dict[str, float]:
        """
        Get rebalance hub scores for all fleet members.

        High hub scores indicate members that are well-connected
        within the hive and optimal for routing liquidity.

        Returns:
            Dict mapping member_id to hub_score (0.0-1.0)
        """
        calculator = network_metrics.get_calculator()
        if not calculator:
            return {}

        hub_scores = {}
        members = self._get_fleet_members()

        for member_id in members:
            metrics = calculator.get_member_metrics(member_id)
            if metrics:
                hub_scores[member_id] = metrics.rebalance_hub_score
            else:
                hub_scores[member_id] = 0.0

        return hub_scores

    def get_optimal_rebalance_hubs(self, min_score: float = HIGH_HUB_SCORE_THRESHOLD) -> List[Dict[str, Any]]:
        """
        Find members with high rebalance hub scores.

        These members are optimal intermediaries for fleet-internal
        rebalancing due to their central position in the hive topology.

        Args:
            min_score: Minimum hub score to include (default: 0.6)

        Returns:
            List of hub members with scores, sorted by score descending
        """
        hub_scores = self.get_member_hub_scores()

        hubs = []
        for member_id, score in hub_scores.items():
            if score >= min_score:
                hubs.append({
                    "member_id": member_id,
                    "hub_score": round(score, 3),
                    "is_high_hub": score >= HIGH_HUB_SCORE_THRESHOLD
                })

        # Sort by score descending
        hubs.sort(key=lambda h: h["hub_score"], reverse=True)
        return hubs

    def _score_path_with_hub_bonus(self, path: List[str], amount_sats: int) -> float:
        """
        Score a fleet path considering hub scores of members.

        Higher hub scores along the path indicate better routing efficiency.

        Args:
            path: List of member pubkeys in the path
            amount_sats: Amount being routed

        Returns:
            Combined score (lower is better for routing)
        """
        if not path:
            return float('inf')

        hub_scores = self.get_member_hub_scores()

        # Base cost component
        cost = self._estimate_fleet_cost(amount_sats, len(path))
        cost_score = cost / max(1, amount_sats)  # Normalize to 0-1ish

        # Hub score component (average hub score along path)
        path_hub_scores = [hub_scores.get(m, 0.0) for m in path]
        avg_hub_score = sum(path_hub_scores) / len(path_hub_scores) if path_hub_scores else 0.0

        # Higher hub score = better, so invert for "lower is better" scoring
        hub_penalty = 1.0 - avg_hub_score

        # Combined score: weighted average
        combined = (1 - HUB_SCORE_WEIGHT_IN_PATH) * cost_score + HUB_SCORE_WEIGHT_IN_PATH * hub_penalty

        return combined

    def find_hub_aware_fleet_path(
        self,
        from_peer: str,
        to_peer: str,
        amount_sats: int
    ) -> Optional[FleetPath]:
        """
        Find a fleet path that prefers high-hub-score members.

        This is an enhanced version of find_fleet_path that considers
        rebalance hub scores when selecting the path.

        Args:
            from_peer: Source peer
            to_peer: Destination peer
            amount_sats: Amount to rebalance

        Returns:
            FleetPath optimized for hub routing, or None
        """
        topology = self._get_fleet_topology()
        members = set(topology.keys())

        if not members:
            return None

        # Find all candidate paths (limit search to reasonable depth)
        all_paths = self._find_all_fleet_paths(from_peer, to_peer, max_depth=4)

        if not all_paths:
            # Fall back to regular path finding
            return self.find_fleet_path(from_peer, to_peer, amount_sats)

        # Score each path with hub bonus
        scored_paths = []
        for path in all_paths:
            score = self._score_path_with_hub_bonus(path, amount_sats)
            scored_paths.append((path, score))

        # Sort by score (lower is better)
        scored_paths.sort(key=lambda x: x[1])

        # Return best path
        best_path = scored_paths[0][0]
        hub_scores = self.get_member_hub_scores()
        avg_hub = sum(hub_scores.get(m, 0.0) for m in best_path) / len(best_path)

        return FleetPath(
            path=best_path,
            hops=len(best_path),
            estimated_cost_sats=self._estimate_fleet_cost(amount_sats, len(best_path)),
            estimated_time_seconds=30 * len(best_path),
            reliability_score=max(0.5, min(0.95, 0.8 + avg_hub * 0.2))  # Hub score boosts reliability
        )

    def _find_all_fleet_paths(
        self,
        from_peer: str,
        to_peer: str,
        max_depth: int = 4
    ) -> List[List[str]]:
        """
        Find all fleet paths between peers up to max_depth.

        Returns multiple paths for hub-aware selection.
        """
        topology = self._get_fleet_topology()
        all_paths = []

        # Find members connected to from_peer
        start_members = []
        for member, peers in topology.items():
            if from_peer in peers:
                start_members.append(member)

        if not start_members:
            return []

        # Find members connected to to_peer
        end_members = set()
        for member, peers in topology.items():
            if to_peer in peers:
                end_members.add(member)

        if not end_members:
            return []

        # DFS to find all paths
        def dfs(current: str, path: List[str], visited: Set[str]):
            if len(path) > max_depth:
                return

            if current in end_members:
                all_paths.append(list(path))
                return

            current_peers = topology.get(current, set())
            for member in topology:
                if member not in visited and member != current:
                    # Check if current has a direct channel to member
                    member_peers = topology.get(member, set())
                    if member in current_peers or current in member_peers:
                        visited.add(member)
                        path.append(member)
                        dfs(member, path, visited)
                        path.pop()
                        visited.discard(member)

        # Search from each start member
        for start in start_members:
            dfs(start, [start], {start})

        return all_paths

    def get_hub_enhanced_rebalance_path(
        self,
        from_channel: str,
        to_channel: str,
        amount_sats: int
    ) -> Dict[str, Any]:
        """
        Get rebalance path recommendation with hub optimization.

        Enhanced version of get_best_rebalance_path that considers
        rebalance hub scores for optimal liquidity routing.

        Args:
            from_channel: Source channel SCID
            to_channel: Destination channel SCID
            amount_sats: Amount to rebalance

        Returns:
            Dict with path recommendation including hub info
        """
        result = {
            "fleet_path_available": False,
            "fleet_path": [],
            "estimated_fleet_cost_sats": 0,
            "estimated_external_cost_sats": self._estimate_external_cost(amount_sats),
            "savings_pct": 0,
            "recommendation": "use_external_path",
            # Hub info (Use Case 5)
            "hub_optimized": False,
            "path_avg_hub_score": 0.0,
            "preferred_hub_member": None
        }

        from_peer = self._get_peer_for_channel(from_channel)
        to_peer = self._get_peer_for_channel(to_channel)

        if not from_peer or not to_peer:
            return result

        # Try hub-aware path finding
        fleet_path = self.find_hub_aware_fleet_path(from_peer, to_peer, amount_sats)

        if fleet_path:
            result["fleet_path_available"] = True
            result["fleet_path"] = fleet_path.path
            result["estimated_fleet_cost_sats"] = fleet_path.estimated_cost_sats
            result["hub_optimized"] = True

            # Calculate path hub score
            hub_scores = self.get_member_hub_scores()
            path_hub_scores = [hub_scores.get(m, 0.0) for m in fleet_path.path]
            avg_hub = sum(path_hub_scores) / len(path_hub_scores) if path_hub_scores else 0.0
            result["path_avg_hub_score"] = round(avg_hub, 3)

            # Identify best hub in path
            if path_hub_scores:
                best_hub_idx = path_hub_scores.index(max(path_hub_scores))
                result["preferred_hub_member"] = fleet_path.path[best_hub_idx]

            # Calculate savings
            external_cost = result["estimated_external_cost_sats"]
            fleet_cost = fleet_path.estimated_cost_sats

            if external_cost > 0:
                savings = (external_cost - fleet_cost) / external_cost
                result["savings_pct"] = round(savings * 100, 1)

                if savings >= FLEET_PATH_SAVINGS_THRESHOLD:
                    result["recommendation"] = "use_fleet_path"

        return result


# =============================================================================
# CIRCULAR FLOW DETECTOR
# =============================================================================

class CircularFlowDetector:
    """
    Detect when fleet pays fees to move liquidity in circles.

    Example: A→B→C→A where A, B, C are all fleet members
    This is pure cost with no benefit.

    Detection:
    1. Track all rebalances across fleet (from state gossip)
    2. Build flow graph
    3. Detect cycles
    4. Alert if cycle cost exceeds threshold
    """

    def __init__(self, plugin, state_manager=None):
        """
        Initialize the circular flow detector.

        Args:
            plugin: Plugin reference
            state_manager: StateManager for fleet state
        """
        self.plugin = plugin
        self.state_manager = state_manager

        # Track rebalance outcomes
        self._rebalance_history: List[RebalanceOutcome] = []
        self._max_history_size = 1000

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"CIRCULAR_FLOW: {message}", level=level)

    def record_rebalance_outcome(
        self,
        from_channel: str,
        to_channel: str,
        from_peer: str,
        to_peer: str,
        amount_sats: int,
        cost_sats: int,
        success: bool,
        via_fleet: bool = False,
        member_id: str = ""
    ) -> None:
        """
        Record a rebalance outcome for circular flow detection.

        Args:
            from_channel: Source channel
            to_channel: Destination channel
            from_peer: Source peer
            to_peer: Destination peer
            amount_sats: Amount rebalanced
            cost_sats: Cost paid
            success: Whether rebalance succeeded
            via_fleet: Whether routed through fleet
            member_id: Which fleet member performed this
        """
        if not success:
            return  # Only track successful rebalances

        outcome = RebalanceOutcome(
            timestamp=time.time(),
            from_channel=from_channel,
            to_channel=to_channel,
            from_peer=from_peer,
            to_peer=to_peer,
            amount_sats=amount_sats,
            cost_sats=cost_sats,
            success=success,
            via_fleet=via_fleet,
            member_id=member_id
        )

        self._rebalance_history.append(outcome)

        # Trim history if too large
        if len(self._rebalance_history) > self._max_history_size:
            self._rebalance_history = self._rebalance_history[-self._max_history_size:]

    def detect_circular_flows(
        self,
        window_hours: float = CIRCULAR_FLOW_WINDOW_HOURS
    ) -> List[CircularFlow]:
        """
        Detect circular flow patterns in recent rebalances.

        Args:
            window_hours: How far back to look

        Returns:
            List of detected circular flows
        """
        circular_flows = []

        # Filter to recent rebalances
        cutoff = time.time() - (window_hours * 3600)
        recent = [r for r in self._rebalance_history if r.timestamp >= cutoff]

        if len(recent) < 2:
            return circular_flows

        # Build flow graph: peer -> peer -> amount
        flow_graph: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        cost_graph: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        for outcome in recent:
            flow_graph[outcome.from_peer][outcome.to_peer] += outcome.amount_sats
            cost_graph[outcome.from_peer][outcome.to_peer] += outcome.cost_sats

        # Detect cycles using DFS
        visited_cycles = set()

        for start_peer in flow_graph:
            cycles = self._find_cycles(flow_graph, start_peer, [], set())

            for cycle in cycles:
                cycle_key = tuple(sorted(cycle))
                if cycle_key in visited_cycles:
                    continue
                visited_cycles.add(cycle_key)

                # Calculate cycle metrics
                total_amount = 0
                total_cost = 0
                cycle_count = 0

                for i in range(len(cycle)):
                    from_p = cycle[i]
                    to_p = cycle[(i + 1) % len(cycle)]

                    amount = flow_graph[from_p][to_p]
                    cost = cost_graph[from_p][to_p]

                    if amount > 0:
                        total_amount += amount
                        total_cost += cost
                        cycle_count += 1

                # Only report significant circular flows
                if total_amount >= MIN_CIRCULAR_AMOUNT_SATS:
                    circular_flows.append(CircularFlow(
                        members=cycle,
                        total_amount_sats=total_amount,
                        total_cost_sats=total_cost,
                        cycle_count=cycle_count,
                        detection_window_hours=window_hours,
                        recommendation=self._get_circular_flow_recommendation(
                            cycle, total_amount, total_cost
                        )
                    ))

        return circular_flows

    def _find_cycles(
        self,
        graph: Dict[str, Dict[str, int]],
        current: str,
        path: List[str],
        visited: Set[str]
    ) -> List[List[str]]:
        """
        Find cycles in the flow graph using DFS.
        """
        cycles = []

        if current in path:
            # Found a cycle
            cycle_start = path.index(current)
            cycle = path[cycle_start:] + [current]
            if len(cycle) >= 3:  # At least 3 nodes for meaningful cycle
                cycles.append(cycle[:-1])  # Remove duplicate end node
            return cycles

        if current in visited:
            return cycles

        visited.add(current)
        path.append(current)

        for neighbor in graph.get(current, {}):
            if graph[current][neighbor] > 0:  # Has flow
                cycles.extend(self._find_cycles(graph, neighbor, path.copy(), visited.copy()))

        return cycles

    def _get_circular_flow_recommendation(
        self,
        cycle: List[str],
        total_amount: int,
        total_cost: int
    ) -> str:
        """
        Get recommendation for handling a circular flow.
        """
        cost_pct = (total_cost / total_amount * 100) if total_amount > 0 else 0

        if cost_pct > 2:
            return f"URGENT: Stop rebalancing between {cycle[0][:8]}... and {cycle[1][:8]}... - circular flow wasting {cost_pct:.1f}% in fees"
        elif cost_pct > 1:
            return f"WARNING: Coordinate rebalancing between {len(cycle)} members to avoid circular flow"
        else:
            return f"MONITOR: Minor circular flow detected between {len(cycle)} members"

    def get_circular_flow_status(self) -> Dict[str, Any]:
        """
        Get overall circular flow detection status.

        Returns:
            Dict with detection status and any active circular flows
        """
        circular_flows = self.detect_circular_flows()

        total_waste = sum(cf.total_cost_sats for cf in circular_flows)

        return {
            "detection_enabled": True,
            "history_entries": len(self._rebalance_history),
            "circular_flows_detected": len(circular_flows),
            "total_wasted_sats": total_waste,
            "circular_flows": [cf.to_dict() for cf in circular_flows]
        }

    # =========================================================================
    # FLEET INTELLIGENCE SHARING (Phase 14)
    # =========================================================================

    def get_shareable_circular_flows(
        self,
        min_cost_sats: int = 100,
        min_amount_sats: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Get detected circular flows suitable for sharing with fleet.

        Only shares flows that meet minimum thresholds for significance.

        Args:
            min_cost_sats: Minimum total cost to report
            min_amount_sats: Minimum total amount to report

        Returns:
            List of circular flow dicts ready for fleet broadcast
        """
        shareable = []

        try:
            flows = self.detect_circular_flows()

            for cf in flows:
                if cf.total_cost_sats < min_cost_sats:
                    continue
                if cf.total_amount_sats < min_amount_sats:
                    continue

                recommendation = self._get_circular_flow_recommendation(
                    cf.members, cf.total_amount_sats, cf.total_cost_sats
                )

                shareable.append({
                    "members_involved": cf.members,
                    "total_amount_sats": cf.total_amount_sats,
                    "total_cost_sats": cf.total_cost_sats,
                    "cycle_count": cf.members_count,
                    "detection_window_hours": cf.detection_window_hours,
                    "recommendation": recommendation
                })

        except Exception as e:
            if self.plugin:
                self.plugin.log(f"cl-hive: Error collecting shareable circular flows: {e}", level="debug")

        return shareable

    def receive_circular_flow_alert(
        self,
        reporter_id: str,
        alert_data: Dict[str, Any]
    ) -> bool:
        """
        Receive a circular flow alert from another fleet member.

        Stores remote alerts for coordination and prevention.

        Args:
            reporter_id: The fleet member who detected this
            alert_data: Dict with members_involved, costs, etc.

        Returns:
            True if stored successfully
        """
        members = alert_data.get("members_involved", [])
        if len(members) < 2:
            return False

        # Initialize remote alerts storage if needed
        if not hasattr(self, "_remote_circular_alerts"):
            self._remote_circular_alerts: List[Dict[str, Any]] = []

        entry = {
            "reporter_id": reporter_id,
            "members_involved": members,
            "total_amount_sats": alert_data.get("total_amount_sats", 0),
            "total_cost_sats": alert_data.get("total_cost_sats", 0),
            "cycle_count": alert_data.get("cycle_count", 1),
            "recommendation": alert_data.get("recommendation", ""),
            "timestamp": time.time()
        }

        self._remote_circular_alerts.append(entry)

        # Keep only last 100 alerts
        if len(self._remote_circular_alerts) > 100:
            self._remote_circular_alerts = self._remote_circular_alerts[-100:]

        return True

    def get_all_circular_flow_alerts(self, include_remote: bool = True) -> List[Dict[str, Any]]:
        """
        Get all circular flow alerts (local and remote).

        Args:
            include_remote: Whether to include alerts from fleet

        Returns:
            List of all circular flow alerts
        """
        alerts = []

        # Local flows
        try:
            local_flows = self.detect_circular_flows()
            for cf in local_flows:
                alerts.append({
                    "source": "local",
                    "members_involved": cf.members,
                    "total_amount_sats": cf.total_amount_sats,
                    "total_cost_sats": cf.total_cost_sats,
                    "cycle_count": cf.members_count,
                    "recommendation": self._get_circular_flow_recommendation(
                        cf.members, cf.total_amount_sats, cf.total_cost_sats
                    )
                })
        except Exception:
            pass

        # Remote alerts
        if include_remote and hasattr(self, "_remote_circular_alerts"):
            now = time.time()
            for alert in self._remote_circular_alerts:
                # Only include recent alerts (last 24 hours)
                if now - alert.get("timestamp", 0) < 86400:
                    alert_copy = alert.copy()
                    alert_copy["source"] = "fleet"
                    alerts.append(alert_copy)

        return alerts

    def is_member_in_circular_flow(self, member_id: str) -> bool:
        """
        Check if a member is involved in any detected circular flow.

        Args:
            member_id: Member pubkey to check

        Returns:
            True if member is in an active circular flow
        """
        all_alerts = self.get_all_circular_flow_alerts(include_remote=True)

        for alert in all_alerts:
            if member_id in alert.get("members_involved", []):
                return True

        return False

    def cleanup_old_remote_alerts(self, max_age_hours: float = 24) -> int:
        """Remove old remote circular flow alerts."""
        if not hasattr(self, "_remote_circular_alerts"):
            return 0

        cutoff = time.time() - (max_age_hours * 3600)
        before = len(self._remote_circular_alerts)
        self._remote_circular_alerts = [
            a for a in self._remote_circular_alerts
            if a.get("timestamp", 0) > cutoff
        ]
        return before - len(self._remote_circular_alerts)


# =============================================================================
# COST REDUCTION MANAGER
# =============================================================================

class CostReductionManager:
    """
    Main interface for Phase 3 cost reduction features.

    Coordinates:
    - Predictive rebalancing
    - Fleet rebalance routing
    - Circular flow detection
    - MCF (Min-Cost Max-Flow) optimization (Phase 15)
    """

    def __init__(
        self,
        plugin,
        database=None,
        state_manager=None,
        yield_metrics_mgr=None,
        liquidity_coordinator=None
    ):
        """
        Initialize the cost reduction manager.

        Args:
            plugin: Plugin reference
            database: Database instance
            state_manager: StateManager for fleet state
            yield_metrics_mgr: YieldMetricsManager for velocity predictions
            liquidity_coordinator: LiquidityCoordinator for liquidity state
        """
        self.plugin = plugin
        self.database = database
        self.state_manager = state_manager
        self.liquidity_coordinator = liquidity_coordinator

        # Initialize components
        self.predictive_rebalancer = PredictiveRebalancer(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics_mgr,
            state_manager=state_manager
        )

        self.fleet_router = FleetRebalanceRouter(
            plugin=plugin,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator
        )

        self.circular_detector = CircularFlowDetector(
            plugin=plugin,
            state_manager=state_manager
        )

        # MCF coordinator (initialized when pubkey is set)
        self._mcf_coordinator: Optional[MCFCoordinator] = None
        self._mcf_enabled: bool = True  # Can be disabled via config

        self._our_pubkey: Optional[str] = None

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey
        self.predictive_rebalancer.set_our_pubkey(pubkey)
        self.fleet_router.set_our_pubkey(pubkey)

        # Initialize MCF coordinator
        if self.database and self.state_manager:
            self._mcf_coordinator = MCFCoordinator(
                plugin=self.plugin,
                database=self.database,
                state_manager=self.state_manager,
                liquidity_coordinator=self.liquidity_coordinator,
                our_pubkey=pubkey
            )

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"COST_REDUCTION: {message}", level=level)

    def get_rebalance_recommendations(
        self,
        prediction_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get comprehensive rebalance recommendations.

        Combines predictive analysis with fleet routing optimization.

        Args:
            prediction_hours: How far ahead to predict

        Returns:
            List of rebalance recommendations with fleet path info
        """
        recommendations = []

        # Get predictive recommendations
        preemptive = self.predictive_rebalancer.get_preemptive_recommendations(
            prediction_hours=prediction_hours
        )

        for rec in preemptive:
            rec_dict = rec.to_dict()

            # Check for fleet path
            if rec.channel_id:
                # Find a source channel for rebalancing
                # For now, use the channel itself for outbound, find another for inbound
                if rec.direction == "outbound":
                    from_channel = rec.channel_id
                    to_channel = self._find_sink_channel(rec.recommended_amount_sats)
                else:
                    from_channel = self._find_source_channel(rec.recommended_amount_sats)
                    to_channel = rec.channel_id

                if from_channel and to_channel:
                    fleet_info = self.fleet_router.get_best_rebalance_path(
                        from_channel=from_channel,
                        to_channel=to_channel,
                        amount_sats=rec.recommended_amount_sats
                    )
                    rec_dict.update(fleet_info)

            recommendations.append(rec_dict)

        return recommendations

    def _find_source_channel(self, amount_sats: int) -> Optional[str]:
        """Find a channel with excess local balance to use as source."""
        if not self.plugin:
            return None

        try:
            channels = self.plugin.rpc.listpeerchannels()
            for ch in channels.get("channels", []):
                if ch.get("state") != "CHANNELD_NORMAL":
                    continue

                local = ch.get("to_us_msat", 0)
                if isinstance(local, str):
                    local = int(local.replace("msat", ""))
                local_sats = local // 1000

                capacity = ch.get("total_msat", 0)
                if isinstance(capacity, str):
                    capacity = int(capacity.replace("msat", ""))
                capacity_sats = capacity // 1000

                # Check if has excess local balance
                local_pct = local_sats / capacity_sats if capacity_sats > 0 else 0
                if local_pct > 0.6 and local_sats > amount_sats:
                    return ch.get("short_channel_id", "").replace(":", "x")

            return None
        except Exception:
            return None

    def _find_sink_channel(self, amount_sats: int) -> Optional[str]:
        """Find a channel with low local balance to use as sink."""
        if not self.plugin:
            return None

        try:
            channels = self.plugin.rpc.listpeerchannels()
            for ch in channels.get("channels", []):
                if ch.get("state") != "CHANNELD_NORMAL":
                    continue

                local = ch.get("to_us_msat", 0)
                if isinstance(local, str):
                    local = int(local.replace("msat", ""))
                local_sats = local // 1000

                capacity = ch.get("total_msat", 0)
                if isinstance(capacity, str):
                    capacity = int(capacity.replace("msat", ""))
                capacity_sats = capacity // 1000

                remote_sats = capacity_sats - local_sats

                # Check if has room for inbound
                local_pct = local_sats / capacity_sats if capacity_sats > 0 else 0
                if local_pct < 0.4 and remote_sats > amount_sats:
                    return ch.get("short_channel_id", "").replace(":", "x")

            return None
        except Exception:
            return None

    def record_rebalance_outcome(
        self,
        from_channel: str,
        to_channel: str,
        amount_sats: int,
        cost_sats: int,
        success: bool,
        via_fleet: bool = False
    ) -> Dict[str, Any]:
        """
        Record a rebalance outcome for tracking and analysis.

        Args:
            from_channel: Source channel SCID
            to_channel: Destination channel SCID
            amount_sats: Amount rebalanced
            cost_sats: Cost paid
            success: Whether rebalance succeeded
            via_fleet: Whether routed through fleet

        Returns:
            Dict with recording result and any circular flow warnings
        """
        # Get peer IDs (skip circular flow recording if peers unknown)
        from_peer = self.fleet_router._get_peer_for_channel(from_channel)
        to_peer = self.fleet_router._get_peer_for_channel(to_channel)

        if not from_peer or not to_peer:
            return {
                "status": "recorded",
                "warning": "Could not resolve peers for circular flow tracking"
            }

        # Record for circular flow detection
        self.circular_detector.record_rebalance_outcome(
            from_channel=from_channel,
            to_channel=to_channel,
            from_peer=from_peer,
            to_peer=to_peer,
            amount_sats=amount_sats,
            cost_sats=cost_sats,
            success=success,
            via_fleet=via_fleet,
            member_id=self._our_pubkey or ""
        )

        # Check for circular flows
        circular_flows = self.circular_detector.detect_circular_flows()

        result = {
            "recorded": True,
            "circular_flows_detected": len(circular_flows)
        }

        if circular_flows:
            result["warnings"] = [cf.recommendation for cf in circular_flows]

        return result

    def get_fleet_rebalance_path(
        self,
        from_channel: str,
        to_channel: str,
        amount_sats: int
    ) -> Dict[str, Any]:
        """
        Get fleet rebalance path recommendation.

        Args:
            from_channel: Source channel SCID
            to_channel: Destination channel SCID
            amount_sats: Amount to rebalance

        Returns:
            Dict with path recommendation
        """
        return self.fleet_router.get_best_rebalance_path(
            from_channel=from_channel,
            to_channel=to_channel,
            amount_sats=amount_sats
        )

    def get_cost_reduction_status(self) -> Dict[str, Any]:
        """
        Get overall cost reduction status.

        Returns:
            Dict with status of all cost reduction features
        """
        circular_status = self.circular_detector.get_circular_flow_status()

        # Get MCF status if available
        mcf_status = None
        if self._mcf_coordinator and self._mcf_enabled:
            mcf_status = self._mcf_coordinator.get_status()

        return {
            "predictive_rebalancing_enabled": True,
            "fleet_routing_enabled": True,
            "circular_flow_detection_enabled": True,
            "mcf_optimization_enabled": self._mcf_enabled and self._mcf_coordinator is not None,
            "circular_flow_status": circular_status,
            "mcf_status": mcf_status,
            "constants": {
                "depletion_risk_threshold": DEPLETION_RISK_THRESHOLD,
                "saturation_risk_threshold": SATURATION_RISK_THRESHOLD,
                "preemptive_max_fee_ppm": PREEMPTIVE_MAX_FEE_PPM,
                "urgent_max_fee_ppm": URGENT_MAX_FEE_PPM,
                "fleet_path_savings_threshold": FLEET_PATH_SAVINGS_THRESHOLD,
                "mcf_min_demand_sats": MIN_MCF_DEMAND,
                "mcf_max_solution_age_seconds": MAX_SOLUTION_AGE
            }
        }

    # =========================================================================
    # MCF (MIN-COST MAX-FLOW) OPTIMIZATION (Phase 15)
    # =========================================================================

    def run_mcf_optimization(self) -> Optional[Dict[str, Any]]:
        """
        Run MCF optimization cycle.

        Only runs if we are the coordinator. Returns solution if successful.

        Returns:
            MCF solution dict or None
        """
        if not self._mcf_enabled or not self._mcf_coordinator:
            return None

        solution = self._mcf_coordinator.run_optimization_cycle()
        if solution:
            return solution.to_dict()
        return None

    def get_mcf_status(self) -> Dict[str, Any]:
        """
        Get MCF coordinator status.

        Returns:
            Status dict with coordinator info, solution, and assignments
        """
        if not self._mcf_coordinator:
            return {
                "enabled": False,
                "reason": "MCF coordinator not initialized"
            }

        if not self._mcf_enabled:
            return {
                "enabled": False,
                "reason": "MCF optimization disabled"
            }

        return self._mcf_coordinator.get_status()

    def get_mcf_assignments(self) -> List[Dict[str, Any]]:
        """
        Get our pending MCF assignments.

        Returns:
            List of assignment dicts
        """
        if not self._mcf_coordinator:
            return []

        assignments = self._mcf_coordinator.get_our_assignments()
        return [a.to_dict() for a in assignments]

    def receive_mcf_solution(self, solution_data: Dict[str, Any]) -> bool:
        """
        Receive and validate an MCF solution from the coordinator.

        Args:
            solution_data: Serialized MCF solution

        Returns:
            True if accepted
        """
        if not self._mcf_coordinator:
            return False

        return self._mcf_coordinator.receive_solution(solution_data)

    def set_mcf_enabled(self, enabled: bool) -> None:
        """Enable or disable MCF optimization."""
        self._mcf_enabled = enabled
        self._log(f"MCF optimization {'enabled' if enabled else 'disabled'}")

    def get_mcf_optimized_path(
        self,
        from_channel: str,
        to_channel: str,
        amount_sats: int
    ) -> Dict[str, Any]:
        """
        Get rebalance path using MCF optimization when available.

        Falls back to BFS-based fleet routing if MCF is not available.

        Args:
            from_channel: Source channel SCID
            to_channel: Destination channel SCID
            amount_sats: Amount to rebalance

        Returns:
            Path recommendation dict
        """
        # Try MCF-based path first
        if self._mcf_enabled and self._mcf_coordinator:
            mcf_status = self._mcf_coordinator.get_status()
            if mcf_status.get("solution_valid"):
                # Check if there's an assignment that matches
                assignments = self._mcf_coordinator.get_our_assignments()
                for assignment in assignments:
                    if (assignment.from_channel == from_channel and
                        assignment.amount_sats >= amount_sats):
                        return {
                            "source": "mcf",
                            "fleet_path_available": True,
                            "fleet_path": assignment.path,
                            "estimated_fleet_cost_sats": assignment.expected_cost_sats,
                            "via_fleet": assignment.via_fleet,
                            "assignment": assignment.to_dict(),
                            "recommendation": "use_mcf_assignment"
                        }

        # Fall back to BFS-based routing
        bfs_result = self.fleet_router.get_best_rebalance_path(
            from_channel=from_channel,
            to_channel=to_channel,
            amount_sats=amount_sats
        )
        bfs_result["source"] = "bfs"
        return bfs_result

    def get_current_mcf_coordinator(self) -> Optional[str]:
        """
        Get the pubkey of the current MCF coordinator.

        Returns:
            Coordinator pubkey or None if MCF not available
        """
        if not self._mcf_coordinator:
            return None
        return self._mcf_coordinator.elect_coordinator()

    def record_mcf_ack(
        self,
        member_id: str,
        solution_timestamp: int,
        assignment_count: int
    ) -> None:
        """
        Record an MCF assignment acknowledgment from a member.

        Only tracks if we are the coordinator.

        Args:
            member_id: Pubkey of the member sending ACK
            solution_timestamp: Timestamp of the solution being acknowledged
            assignment_count: Number of assignments the member accepted
        """
        if not self._mcf_coordinator:
            return

        # Track ACK for monitoring
        ack_key = f"{member_id}:{solution_timestamp}"
        if not hasattr(self, "_mcf_acks"):
            self._mcf_acks: Dict[str, Dict[str, Any]] = {}

        self._mcf_acks[ack_key] = {
            "member_id": member_id,
            "solution_timestamp": solution_timestamp,
            "assignment_count": assignment_count,
            "received_at": int(time.time())
        }

        # Limit cache size
        if len(self._mcf_acks) > 500:
            # Remove oldest entries
            sorted_acks = sorted(
                self._mcf_acks.items(),
                key=lambda x: x[1].get("received_at", 0)
            )
            for k, _ in sorted_acks[:100]:
                del self._mcf_acks[k]

        self._log(f"Recorded MCF ACK from {member_id[:16]}... for solution {solution_timestamp}")

    def record_mcf_completion(
        self,
        member_id: str,
        assignment_id: str,
        success: bool,
        actual_amount_sats: int,
        actual_cost_sats: int,
        failure_reason: str = ""
    ) -> None:
        """
        Record the outcome of an MCF assignment execution.

        Args:
            member_id: Pubkey of the member reporting
            assignment_id: ID of the completed assignment
            success: Whether execution succeeded
            actual_amount_sats: Actual amount rebalanced
            actual_cost_sats: Actual cost incurred
            failure_reason: Reason for failure if not successful
        """
        if not hasattr(self, "_mcf_completions"):
            self._mcf_completions: Dict[str, Dict[str, Any]] = {}

        self._mcf_completions[assignment_id] = {
            "member_id": member_id,
            "assignment_id": assignment_id,
            "success": success,
            "actual_amount_sats": actual_amount_sats,
            "actual_cost_sats": actual_cost_sats,
            "failure_reason": failure_reason,
            "completed_at": int(time.time())
        }

        # Limit cache size
        if len(self._mcf_completions) > 1000:
            sorted_completions = sorted(
                self._mcf_completions.items(),
                key=lambda x: x[1].get("completed_at", 0)
            )
            for k, _ in sorted_completions[:200]:
                del self._mcf_completions[k]

        status = "succeeded" if success else f"failed: {failure_reason}"
        self._log(f"MCF assignment {assignment_id[:20]}... {status} ({actual_amount_sats} sats)")

    def get_mcf_acks(self) -> List[Dict[str, Any]]:
        """Get all recorded MCF acknowledgments."""
        if not hasattr(self, "_mcf_acks"):
            return []
        return list(self._mcf_acks.values())

    def get_mcf_completions(self) -> List[Dict[str, Any]]:
        """Get all recorded MCF completion reports."""
        if not hasattr(self, "_mcf_completions"):
            return []
        return list(self._mcf_completions.values())

    def execute_hive_circular_rebalance(
        self,
        from_channel: str,
        to_channel: str,
        amount_sats: int,
        via_members: Optional[List[str]] = None,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a circular rebalance through the hive using explicit sendpay route.

        This bypasses sling's automatic route finding and uses an explicit route
        through hive members, ensuring zero-fee internal routing.

        Args:
            from_channel: Source channel SCID (where we have outbound liquidity)
            to_channel: Destination channel SCID (where we want more local balance)
            amount_sats: Amount to rebalance in satoshis
            via_members: Optional list of intermediate member pubkeys. If not provided,
                        will attempt to find a path automatically.
            dry_run: If True, just show the route without executing (default: True)

        Returns:
            Dict with route details and execution result (or preview if dry_run)
        """
        if not self.plugin or not self.plugin.rpc:
            return {"error": "RPC not available"}

        rpc = self.plugin.rpc

        amount_msat = amount_sats * 1000

        try:
            # Get our own node info
            info = rpc.getinfo()
            our_id = info['id']

            # Get channel info for from_channel and to_channel
            channels = rpc.listpeerchannels()['channels']

            from_chan = None
            to_chan = None
            for ch in channels:
                scid = ch.get('short_channel_id')
                if scid == from_channel:
                    from_chan = ch
                elif scid == to_channel:
                    to_chan = ch

            if not from_chan:
                return {"error": f"Source channel {from_channel} not found"}
            if not to_chan:
                return {"error": f"Destination channel {to_channel} not found"}

            # Verify source has enough outbound liquidity
            from_local = int(from_chan.get('to_us_msat', 0))
            if from_local < amount_msat:
                return {
                    "error": f"Insufficient outbound liquidity in {from_channel}",
                    "available_msat": from_local,
                    "requested_msat": amount_msat
                }

            # Get the peer IDs
            from_peer = from_chan['peer_id']
            to_peer = to_chan['peer_id']

            # If no via_members specified, try to find a path through hive
            if not via_members:
                # For a triangle rebalance, we need to find a path:
                # us -> from_peer -> ??? -> to_peer -> us
                # The intermediate node must have channels to both from_peer and to_peer

                # Get hive members directly from database (avoid RPC deadlock)
                if not self.database:
                    return {"error": "Database not available"}

                members = self.database.get_all_members()
                member_ids = {m['peer_id'] for m in members if m.get('peer_id')}

                # Check if from_peer and to_peer are both hive members
                if from_peer not in member_ids:
                    return {
                        "error": f"Source peer {from_peer[:16]}... is not a hive member",
                        "hint": "Hive circular rebalance only works through hive member channels"
                    }
                if to_peer not in member_ids:
                    return {
                        "error": f"Destination peer {to_peer[:16]}... is not a hive member",
                        "hint": "Hive circular rebalance only works through hive member channels"
                    }

                # For direct triangle: us -> from_peer -> to_peer -> us
                # Check if from_peer has a channel to to_peer
                # We need to query from_peer's channels (if we have that info via gossip)
                # For now, assume direct path: from_peer can route to to_peer

                via_members = []  # Direct path through from_peer to to_peer

            # Build the explicit route
            # Route format: array of {id, channel, amount_msat, delay}
            # For zero-fee hive channels, amount stays the same at each hop

            # Calculate delays (CLTV)
            # Each hop needs its delay, working backwards from destination
            # Typically: final_cltv_delta + (hop_count * per_hop_cltv)
            final_cltv = 9  # Standard final CLTV
            per_hop_cltv = 34  # Our cltv_expiry_delta

            route = []

            # For a simple triangle: us -> from_peer -> to_peer -> us
            # Hop 1: from_peer (via from_channel)
            # Hop 2: to_peer (via from_peer's channel to to_peer - need to find this)
            # Hop 3: us (via to_channel - but reversed!)

            # Actually for circular rebalance, we pay ourselves:
            # - We send out on from_channel
            # - Payment routes through the network
            # - We receive on to_channel

            # We need to find the channel from from_peer to to_peer
            # This requires gossip data or hive state

            # Get the channel between from_peer and to_peer
            try:
                # Try to find channel via listchannels
                listchannels = rpc.listchannels(source=from_peer)
                intermediate_channel = None
                for lc in listchannels.get('channels', []):
                    if lc.get('destination') == to_peer:
                        intermediate_channel = lc.get('short_channel_id')
                        break

                if not intermediate_channel:
                    return {
                        "error": f"No channel found from {from_peer[:16]}... to {to_peer[:16]}...",
                        "hint": "The intermediate hive member needs a channel to the destination peer"
                    }
            except Exception as e:
                return {"error": f"Failed to find intermediate channel: {e}"}

            # Build route: 3 hops for triangle
            # Hop 1: to from_peer via from_channel
            # Hop 2: to to_peer via intermediate_channel
            # Hop 3: to us via to_channel

            # Calculate delays (backwards from final)
            hop3_delay = final_cltv  # Final hop
            hop2_delay = hop3_delay + per_hop_cltv
            hop1_delay = hop2_delay + per_hop_cltv

            route = [
                {
                    "id": from_peer,
                    "channel": from_channel,
                    "amount_msat": amount_msat,  # Zero fees
                    "delay": hop1_delay
                },
                {
                    "id": to_peer,
                    "channel": intermediate_channel,
                    "amount_msat": amount_msat,  # Zero fees
                    "delay": hop2_delay
                },
                {
                    "id": our_id,
                    "channel": to_channel,
                    "amount_msat": amount_msat,  # Final amount
                    "delay": hop3_delay
                }
            ]

            result = {
                "route": route,
                "amount_sats": amount_sats,
                "amount_msat": amount_msat,
                "expected_fee_sats": 0,  # Zero fees through hive
                "hop_count": len(route),
                "path_description": f"{our_id[:8]}... -> {from_peer[:8]}... -> {to_peer[:8]}... -> {our_id[:8]}...",
                "from_channel": from_channel,
                "to_channel": to_channel,
                "intermediate_channel": intermediate_channel,
                "dry_run": dry_run
            }

            if dry_run:
                result["status"] = "preview"
                result["message"] = "Dry run - route preview only. Set dry_run=false to execute."
                return result

            # Execute the rebalance
            # 1. Create invoice for ourselves
            import secrets
            label = f"hive-rebalance-{int(time.time())}-{secrets.token_hex(4)}"
            invoice = rpc.invoice(
                amount_msat=amount_msat,
                label=label,
                description="Hive circular rebalance"
            )
            payment_hash = invoice['payment_hash']
            payment_secret = invoice.get('payment_secret')

            result["invoice_label"] = label
            result["payment_hash"] = payment_hash

            # 2. Send via explicit route
            try:
                sendpay_result = rpc.sendpay(
                    route=route,
                    payment_hash=payment_hash,
                    payment_secret=payment_secret,
                    amount_msat=amount_msat
                )
                result["sendpay_result"] = sendpay_result

                # 3. Wait for completion using short polling to avoid RPC lock starvation
                # Use short timeouts (2s) with retries to allow other RPC calls
                max_attempts = 30  # 30 * 2s = 60s total
                waitsendpay_result = None
                for attempt in range(max_attempts):
                    try:
                        waitsendpay_result = rpc.waitsendpay(
                            payment_hash=payment_hash,
                            timeout=2  # Short timeout to release RPC lock frequently
                        )
                        # Success - payment completed
                        break
                    except Exception as wait_err:
                        err_str = str(wait_err)
                        # Check if it's just a timeout (payment still in progress)
                        if "Timed out" in err_str or "timeout" in err_str.lower():
                            # Payment still in progress, continue polling
                            continue
                        # Real error - payment failed
                        raise

                if waitsendpay_result:
                    result["status"] = "success"
                    result["waitsendpay_result"] = waitsendpay_result
                    result["message"] = f"Successfully rebalanced {amount_sats} sats through hive at zero fees!"
                else:
                    result["status"] = "timeout"
                    result["error"] = "Payment timed out after 60 seconds"

            except Exception as e:
                error_str = str(e)
                result["status"] = "failed"
                result["error"] = error_str

                # Clean up the invoice
                try:
                    rpc.delinvoice(label=label, status="unpaid")
                except Exception:
                    pass

            return result

        except Exception as e:
            return {"error": f"Circular rebalance failed: {e}"}
