"""
Phase 5: Strategic Positioning Module for Yield Optimization.

Positions the fleet on critical network paths to maximize routing opportunities:

1. RouteValueAnalyzer: Identify high-value corridors with volume and limited competition
2. FleetPositioningStrategy: Coordinate channel opens without duplication
3. ExchangeConnectivity: Prioritize connections to major Lightning exchanges
4. PhysarumChannelManager: Flow-based channel lifecycle (strengthen/atrophy)

The goal is strategic capital deployment - position on high-value routes where
the fleet can capture significant routing fees.

Author: Lightning Goats Team
"""

import time
import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from . import network_metrics

# =============================================================================
# CONSTANTS
# =============================================================================

# Route value thresholds
HIGH_VALUE_VOLUME_SATS_DAILY = 10_000_000   # 10M sats/day = high value
MEDIUM_VALUE_VOLUME_SATS_DAILY = 1_000_000  # 1M sats/day = medium value
LOW_COMPETITION_THRESHOLD = 5               # <5 competitors = low competition
MEDIUM_COMPETITION_THRESHOLD = 15           # <15 competitors = medium

# Physarum flow thresholds
STRENGTHEN_FLOW_THRESHOLD = 0.02            # 2% daily turn rate → splice in
ATROPHY_FLOW_THRESHOLD = 0.001              # 0.1% daily turn rate → close
STIMULATE_GRACE_DAYS = 90                   # Young channels get fee reduction
MIN_CHANNEL_AGE_FOR_ATROPHY_DAYS = 180      # Must be >6 months to recommend close

# Physarum auto-trigger configuration (Phase 7.2)
AUTO_STRENGTHEN_ENABLED = True              # Enable auto splice-in for high-flow channels
AUTO_ATROPHY_ENABLED = False                # Atrophy always requires human approval
AUTO_STIMULATE_ENABLED = True               # Enable auto fee reduction for young channels

# Auto-trigger thresholds
MIN_AUTO_STRENGTHEN_FLOW = 0.025            # 2.5% flow for auto-strengthen (above base)
MIN_SUSTAIN_PERIODS = 3                     # Flow must be sustained for 3 periods
AUTO_STRENGTHEN_MIN_SATS = 1_000_000        # Minimum 1M sats for auto splice-in
AUTO_STRENGTHEN_MAX_SATS = 5_000_000        # Maximum 5M sats for auto splice-in

# Rate limits for auto-triggers
MAX_AUTO_STRENGTHEN_PER_DAY = 2             # Max 2 splice-in recommendations per day
MAX_AUTO_ATROPHY_PER_WEEK = 1               # Max 1 atrophy recommendation per week
MIN_STRENGTHEN_INTERVAL_HOURS = 24          # Minimum 24h between strengthen for same channel

# Safety constraints
AUTO_TRIGGER_MIN_ON_CHAIN_SATS = 500_000    # Minimum 500k sats on-chain reserve
AUTO_TRIGGER_MAX_PCT_OF_CAPACITY = 0.10     # Max 10% of total capacity per action

# Positioning priorities
EXCHANGE_PRIORITY_BONUS = 1.5               # 50% bonus for exchange channels
BRIDGE_PRIORITY_BONUS = 1.3                 # 30% bonus for bridge positions
UNDERSERVED_PRIORITY_BONUS = 1.2            # 20% bonus for underserved targets

# Centrality-aware targeting (Use Case 4)
CENTRALITY_IMPROVEMENT_BONUS = 1.25         # 25% bonus for centrality improvements
LOW_CENTRALITY_MEMBER_BONUS = 1.15          # 15% bonus when member has low centrality
LOW_CENTRALITY_THRESHOLD = 0.3              # Members below this are "low centrality"
MIN_CENTRALITY_IMPROVEMENT = 0.05           # Minimum improvement to apply bonus

# Fleet coordination
MAX_MEMBERS_PER_TARGET = 2                  # Max 2 members per target (healthy redundancy)
POSITION_RECOMMENDATION_COOLDOWN_HOURS = 24

# Known high-value exchanges (pubkey prefixes or aliases)
PRIORITY_EXCHANGES = {
    "ACINQ": {"alias_patterns": ["ACINQ", "acinq"], "priority": 1.0},
    "Kraken": {"alias_patterns": ["Kraken", "kraken"], "priority": 0.95},
    "Bitfinex": {"alias_patterns": ["Bitfinex", "bitfinex", "bfx"], "priority": 0.9},
    "River": {"alias_patterns": ["River", "river"], "priority": 0.85},
    "CashApp": {"alias_patterns": ["Cash App", "CashApp", "Block"], "priority": 0.85},
    "Strike": {"alias_patterns": ["Strike", "strike"], "priority": 0.85},
    "Coinbase": {"alias_patterns": ["Coinbase", "coinbase"], "priority": 0.8},
    "WalletOfSatoshi": {"alias_patterns": ["WoS", "Wallet of Satoshi"], "priority": 0.75},
    "Muun": {"alias_patterns": ["Muun", "muun"], "priority": 0.7},
    "Breez": {"alias_patterns": ["Breez", "breez"], "priority": 0.7},
}


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class CorridorValue:
    """
    Value assessment for a routing corridor.
    """
    source_peer_id: str
    destination_peer_id: str
    source_alias: Optional[str] = None
    destination_alias: Optional[str] = None

    # Volume metrics
    daily_volume_sats: int = 0
    monthly_volume_sats: int = 0

    # Competition metrics
    competitor_count: int = 0
    fleet_members_present: int = 0

    # Value score
    value_score: float = 0.0
    margin_estimate_ppm: int = 0

    # Classification
    value_tier: str = "unknown"  # "high", "medium", "low"
    competition_level: str = "unknown"  # "low", "medium", "high"

    # Accessibility
    accessible: bool = True
    accessibility_reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_peer_id": self.source_peer_id,
            "destination_peer_id": self.destination_peer_id,
            "source_alias": self.source_alias,
            "destination_alias": self.destination_alias,
            "daily_volume_sats": self.daily_volume_sats,
            "monthly_volume_sats": self.monthly_volume_sats,
            "competitor_count": self.competitor_count,
            "fleet_members_present": self.fleet_members_present,
            "value_score": round(self.value_score, 3),
            "margin_estimate_ppm": self.margin_estimate_ppm,
            "value_tier": self.value_tier,
            "competition_level": self.competition_level,
            "accessible": self.accessible,
            "accessibility_reason": self.accessibility_reason
        }


@dataclass
class PositionRecommendation:
    """
    Recommendation to open a channel for strategic positioning.
    """
    target_peer_id: str
    target_alias: Optional[str] = None

    # Recommended member to open
    recommended_member: Optional[str] = None
    recommended_member_alias: Optional[str] = None

    # Channel parameters
    recommended_capacity_sats: int = 0
    max_fee_rate_ppm: int = 0

    # Reasoning
    reason: str = ""
    priority_score: float = 0.0
    priority_tier: str = "low"  # "critical", "high", "medium", "low"

    # Value sources
    is_exchange: bool = False
    is_bridge_node: bool = False
    is_underserved: bool = False
    corridor_value: Optional[float] = None

    # Current state
    current_fleet_channels: int = 0

    # Centrality impact (Use Case 4)
    member_current_centrality: float = 0.0
    estimated_centrality_improvement: float = 0.0
    improves_network_position: bool = False

    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "target_peer_id": self.target_peer_id,
            "target_alias": self.target_alias,
            "recommended_member": self.recommended_member,
            "recommended_member_alias": self.recommended_member_alias,
            "recommended_capacity_sats": self.recommended_capacity_sats,
            "max_fee_rate_ppm": self.max_fee_rate_ppm,
            "reason": self.reason,
            "priority_score": round(self.priority_score, 3),
            "priority_tier": self.priority_tier,
            "is_exchange": self.is_exchange,
            "is_bridge_node": self.is_bridge_node,
            "is_underserved": self.is_underserved,
            "corridor_value": round(self.corridor_value, 3) if self.corridor_value else None,
            "current_fleet_channels": self.current_fleet_channels,
            "timestamp": self.timestamp
        }
        # Include centrality info if there's a network position improvement
        if self.improves_network_position:
            result["member_current_centrality"] = round(self.member_current_centrality, 3)
            result["estimated_centrality_improvement"] = round(self.estimated_centrality_improvement, 3)
            result["improves_network_position"] = True
        return result


@dataclass
class FlowRecommendation:
    """
    Physarum-inspired recommendation for channel lifecycle.
    """
    channel_id: str
    peer_id: str
    peer_alias: Optional[str] = None

    # Flow metrics
    flow_intensity: float = 0.0  # Volume / Capacity per day
    turn_rate: float = 0.0       # How many times capacity turns over

    # Channel state
    capacity_sats: int = 0
    age_days: int = 0
    revenue_sats: int = 0

    # Recommendation
    action: str = "hold"  # "strengthen", "stimulate", "atrophy", "hold"
    method: str = ""      # "splice_in", "fee_reduction", "cooperative_close"
    reason: str = ""

    # For strengthen
    splice_amount_sats: int = 0

    # For atrophy
    capital_to_redeploy_sats: int = 0

    # Expected impact
    expected_yield_change_pct: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "peer_alias": self.peer_alias,
            "flow_intensity": round(self.flow_intensity, 4),
            "turn_rate": round(self.turn_rate, 4),
            "capacity_sats": self.capacity_sats,
            "age_days": self.age_days,
            "revenue_sats": self.revenue_sats,
            "action": self.action,
            "method": self.method,
            "reason": self.reason,
            "splice_amount_sats": self.splice_amount_sats,
            "capital_to_redeploy_sats": self.capital_to_redeploy_sats,
            "expected_yield_change_pct": round(self.expected_yield_change_pct, 2)
        }


@dataclass
class PositioningSummary:
    """
    Summary of fleet strategic positioning.
    """
    total_targets_analyzed: int = 0
    high_value_corridors: int = 0
    exchange_coverage_pct: float = 0.0
    bridge_positions: int = 0

    # Recommendations
    open_recommendations: int = 0
    strengthen_recommendations: int = 0
    atrophy_recommendations: int = 0

    # Fleet coverage
    well_positioned_targets: int = 0
    underserved_targets: int = 0
    over_positioned_targets: int = 0

    # Capital allocation
    capital_to_redeploy_sats: int = 0
    recommended_new_capacity_sats: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_targets_analyzed": self.total_targets_analyzed,
            "high_value_corridors": self.high_value_corridors,
            "exchange_coverage_pct": round(self.exchange_coverage_pct, 1),
            "bridge_positions": self.bridge_positions,
            "open_recommendations": self.open_recommendations,
            "strengthen_recommendations": self.strengthen_recommendations,
            "atrophy_recommendations": self.atrophy_recommendations,
            "well_positioned_targets": self.well_positioned_targets,
            "underserved_targets": self.underserved_targets,
            "over_positioned_targets": self.over_positioned_targets,
            "capital_to_redeploy_sats": self.capital_to_redeploy_sats,
            "recommended_new_capacity_sats": self.recommended_new_capacity_sats
        }


# =============================================================================
# ROUTE VALUE ANALYZER
# =============================================================================

class RouteValueAnalyzer:
    """
    Identify routes with high volume and limited competition.

    Value = f(volume, margin, accessibility)
    """

    def __init__(self, plugin, state_manager=None, fee_coordination_mgr=None):
        """
        Initialize the route value analyzer.

        Args:
            plugin: Plugin reference for RPC calls
            state_manager: StateManager for fleet topology
            fee_coordination_mgr: FeeCoordinationManager for corridor data
        """
        self.plugin = plugin
        self.state_manager = state_manager
        self.fee_coordination_mgr = fee_coordination_mgr
        self._our_pubkey: Optional[str] = None

        # Cache for corridor values
        self._corridor_cache: Dict[Tuple[str, str], CorridorValue] = {}
        self._cache_time: float = 0
        self._cache_ttl: float = 3600  # 1 hour

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"ROUTE_VALUE: {message}", level=level)

    def _get_corridor_data(self) -> List[Any]:
        """Get corridor assignment data from fee coordination."""
        if not self.fee_coordination_mgr:
            return []

        try:
            corridor_mgr = self.fee_coordination_mgr.corridor_manager
            if corridor_mgr:
                return corridor_mgr.get_all_assignments()
            return []
        except Exception as e:
            self._log(f"Error getting corridor data: {e}", level="debug")
            return []

    def _get_fleet_topology(self) -> Dict[str, Set[str]]:
        """Get fleet member topology (who has channels to whom)."""
        if not self.state_manager:
            return {}

        topology = {}
        try:
            all_states = self.state_manager.get_all_peer_states()
            for state in all_states:
                member_id = state.peer_id
                peers = set(getattr(state, 'topology', []) or [])
                topology[member_id] = peers
        except Exception as e:
            self._log(f"Error getting fleet topology: {e}", level="debug")

        return topology

    def _estimate_competitor_count(self, target_peer_id: str) -> int:
        """
        Estimate number of competitors for routing to a target.

        This is a rough estimate based on known network data.
        """
        # In a real implementation, this would query network gossip
        # For now, return a conservative estimate
        return 10

    def _is_exchange(self, alias: str) -> Tuple[bool, float]:
        """
        Check if a node is a known exchange.

        Returns (is_exchange, priority_score)
        """
        if not alias:
            return False, 0.0

        alias_lower = alias.lower()
        for exchange, data in PRIORITY_EXCHANGES.items():
            for pattern in data["alias_patterns"]:
                if pattern.lower() in alias_lower:
                    return True, data["priority"]

        return False, 0.0

    def analyze_corridor(
        self,
        source_peer_id: str,
        destination_peer_id: str,
        volume_sats: int = 0,
        source_alias: str = None,
        destination_alias: str = None
    ) -> CorridorValue:
        """
        Analyze a single corridor's value.

        Args:
            source_peer_id: Source of payments
            destination_peer_id: Destination of payments
            volume_sats: Known volume (monthly)
            source_alias: Source node alias
            destination_alias: Destination node alias

        Returns:
            CorridorValue with full analysis
        """
        corridor = CorridorValue(
            source_peer_id=source_peer_id,
            destination_peer_id=destination_peer_id,
            source_alias=source_alias,
            destination_alias=destination_alias
        )

        # Set volume
        corridor.monthly_volume_sats = volume_sats
        corridor.daily_volume_sats = volume_sats // 30

        # Classify volume tier
        if corridor.daily_volume_sats >= HIGH_VALUE_VOLUME_SATS_DAILY:
            corridor.value_tier = "high"
        elif corridor.daily_volume_sats >= MEDIUM_VALUE_VOLUME_SATS_DAILY:
            corridor.value_tier = "medium"
        else:
            corridor.value_tier = "low"

        # Estimate competition
        corridor.competitor_count = self._estimate_competitor_count(destination_peer_id)

        if corridor.competitor_count < LOW_COMPETITION_THRESHOLD:
            corridor.competition_level = "low"
        elif corridor.competitor_count < MEDIUM_COMPETITION_THRESHOLD:
            corridor.competition_level = "medium"
        else:
            corridor.competition_level = "high"

        # Count fleet members present
        topology = self._get_fleet_topology()
        corridor.fleet_members_present = sum(
            1 for peers in topology.values()
            if destination_peer_id in peers
        )

        # Estimate margin (higher with less competition)
        base_margin = 500  # Base 500 ppm
        competition_factor = max(0.2, 1.0 - (corridor.competitor_count * 0.05))
        corridor.margin_estimate_ppm = int(base_margin * competition_factor)

        # Calculate value score
        # Score = Volume * Margin * (1 / Competition)
        volume_factor = min(1.0, corridor.daily_volume_sats / HIGH_VALUE_VOLUME_SATS_DAILY)
        margin_factor = corridor.margin_estimate_ppm / 1000
        competition_penalty = 1.0 / max(1, corridor.competitor_count ** 0.5)

        corridor.value_score = volume_factor * margin_factor * competition_penalty

        # Check accessibility (can we get a channel?)
        # For now, always accessible
        corridor.accessible = True

        return corridor

    def find_valuable_corridors(self, min_score: float = 0.1) -> List[CorridorValue]:
        """
        Find corridors with high value and limited competition.

        Args:
            min_score: Minimum value score to include

        Returns:
            List of CorridorValue sorted by value score
        """
        corridors = []

        # Get corridor data from fee coordination
        assignments = self._get_corridor_data()

        for assignment in assignments:
            try:
                corridor_data = assignment.corridor if hasattr(assignment, 'corridor') else assignment
                corridor = self.analyze_corridor(
                    source_peer_id=corridor_data.source_peer_id,
                    destination_peer_id=corridor_data.destination_peer_id,
                    volume_sats=corridor_data.total_volume_sats,
                    source_alias=corridor_data.source_alias,
                    destination_alias=corridor_data.destination_alias
                )

                if corridor.value_score >= min_score:
                    corridors.append(corridor)

            except Exception as e:
                self._log(f"Error analyzing corridor: {e}", level="debug")

        # Sort by value score
        corridors.sort(key=lambda c: c.value_score, reverse=True)

        return corridors

    def find_exchange_targets(self) -> List[Dict[str, Any]]:
        """
        Find exchanges that the fleet should connect to.

        Returns:
            List of exchange targets with connection status
        """
        targets = []
        topology = self._get_fleet_topology()

        # Collect all known peer aliases
        # In a real implementation, this would query listchannels
        known_aliases = {}

        for exchange_name, data in PRIORITY_EXCHANGES.items():
            # Check if any fleet member has this exchange
            has_connection = False
            connected_members = []

            for member_id, peers in topology.items():
                for peer_id in peers:
                    alias = known_aliases.get(peer_id, "")
                    is_exchange, _ = self._is_exchange(alias)
                    if is_exchange:
                        # Check if this specific exchange
                        for pattern in data["alias_patterns"]:
                            if pattern.lower() in alias.lower():
                                has_connection = True
                                connected_members.append(member_id)
                                break

            targets.append({
                "exchange": exchange_name,
                "priority": data["priority"],
                "has_connection": has_connection,
                "connected_members": connected_members,
                "needs_channel": not has_connection
            })

        # Sort by priority (uncovered first)
        targets.sort(key=lambda t: (not t["needs_channel"], -t["priority"]))

        return targets


# =============================================================================
# FLEET POSITIONING STRATEGY
# =============================================================================

class FleetPositioningStrategy:
    """
    Coordinate channel opens to maximize fleet coverage.

    Principles:
    1. Don't duplicate - one member per target (max 2 for redundancy)
    2. Complementary positions - cover different regions
    3. Bridge priority - control chokepoints
    """

    def __init__(
        self,
        plugin,
        state_manager=None,
        route_analyzer: RouteValueAnalyzer = None,
        planner=None
    ):
        """
        Initialize the fleet positioning strategy.

        Args:
            plugin: Plugin reference
            state_manager: StateManager for fleet state
            route_analyzer: RouteValueAnalyzer for value assessment
            planner: Planner for underserved targets
        """
        self.plugin = plugin
        self.state_manager = state_manager
        self.route_analyzer = route_analyzer
        self.planner = planner
        self._our_pubkey: Optional[str] = None

        # Track recent recommendations
        self._recent_recommendations: Dict[str, float] = {}

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey
        if self.route_analyzer:
            self.route_analyzer.set_our_pubkey(pubkey)

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"POSITIONING: {message}", level=level)

    def _get_fleet_members(self) -> List[str]:
        """Get list of fleet member pubkeys."""
        if not self.state_manager:
            return []

        try:
            all_states = self.state_manager.get_all_peer_states()
            return [s.peer_id for s in all_states]
        except Exception:
            return []

    def _count_fleet_channels_to_target(self, target_peer_id: str) -> int:
        """Count how many fleet members have channels to a target."""
        if not self.state_manager:
            return 0

        count = 0
        try:
            all_states = self.state_manager.get_all_peer_states()
            for state in all_states:
                topology = set(getattr(state, 'topology', []) or [])
                if target_peer_id in topology:
                    count += 1
        except Exception:
            pass

        return count

    def _get_member_centrality(self, member_id: str) -> float:
        """Get hive centrality for a member."""
        calculator = network_metrics.get_calculator()
        if not calculator:
            return 0.5  # Default to middle value

        metrics = calculator.get_member_metrics(member_id)
        if not metrics:
            return 0.5

        return metrics.hive_centrality

    def _estimate_centrality_improvement(
        self,
        member_id: str,
        target_peer_id: str
    ) -> float:
        """
        Estimate how much opening a channel to target would improve member's centrality.

        Higher values indicate the target would provide better network position.
        """
        calculator = network_metrics.get_calculator()
        if not calculator:
            return 0.0

        # Get current member metrics
        member_metrics = calculator.get_member_metrics(member_id)
        if not member_metrics:
            return 0.0

        current_centrality = member_metrics.hive_centrality
        hive_peer_count = member_metrics.hive_peer_count

        # Check if target is a hive member (internal channel)
        topology = calculator._get_topology_snapshot()
        if not topology:
            return 0.0

        is_hive_target = target_peer_id in topology.member_topologies

        if is_hive_target:
            # Opening to hive member improves internal connectivity
            # Improvement is inversely proportional to current hive connections
            if hive_peer_count == 0:
                # First hive connection is a big improvement
                return 0.3
            elif hive_peer_count == 1:
                # Second hive connection still significant
                return 0.15
            else:
                # Diminishing returns
                return max(0.02, 0.1 / hive_peer_count)
        else:
            # Opening to external target - check if it improves bridge position
            # (bridge bonus if target is well-connected externally)
            # This is a heuristic - real bridge detection would need network graph
            return 0.02  # Minimal centrality boost for external targets

    def _select_best_member_for_target(self, target_peer_id: str) -> Optional[str]:
        """
        Select the best fleet member to open a channel to target.

        Criteria:
        - Doesn't already have a channel to target
        - Has available on-chain funds
        - Has capacity for another channel
        - Complements existing positions
        - Considers hive centrality (Use Case 4):
          - Members with lower centrality get priority for strategic targets
          - Targets that improve centrality get higher scores
        """
        members = self._get_fleet_members()
        if not members:
            return None

        candidates = []

        for member_id in members:
            if not self.state_manager:
                continue

            state = self.state_manager.get_peer_state(member_id)
            if not state:
                continue

            topology = set(getattr(state, 'topology', []) or [])

            # Skip if already has channel to target
            if target_peer_id in topology:
                continue

            # Score based on position complementarity
            # (member with fewer channels to similar targets is better)
            score = 1.0

            # Prefer members with fewer total channels (more focused)
            channel_count = len(topology)
            if channel_count < 20:
                score += 0.2
            elif channel_count > 50:
                score -= 0.2

            # Use Case 4: Centrality-aware scoring
            member_centrality = self._get_member_centrality(member_id)

            # Bonus for members with low centrality (they need connections more)
            if member_centrality < LOW_CENTRALITY_THRESHOLD:
                score *= LOW_CENTRALITY_MEMBER_BONUS
                self._log(
                    f"Member {member_id[:16]}... has low centrality ({member_centrality:.2f}), "
                    f"applying {LOW_CENTRALITY_MEMBER_BONUS}x bonus",
                    level="debug"
                )

            # Bonus if this target would significantly improve centrality
            centrality_improvement = self._estimate_centrality_improvement(member_id, target_peer_id)
            if centrality_improvement >= MIN_CENTRALITY_IMPROVEMENT:
                score *= CENTRALITY_IMPROVEMENT_BONUS
                self._log(
                    f"Target {target_peer_id[:16]}... would improve {member_id[:16]}...'s "
                    f"centrality by ~{centrality_improvement:.2f}",
                    level="debug"
                )

            candidates.append((member_id, score, member_centrality, centrality_improvement))

        if not candidates:
            return None

        # Return highest scoring candidate
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates[0][0]

    def _select_best_member_with_metrics(
        self,
        target_peer_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Select best member and return selection metrics for recommendations.

        Returns:
            Dict with member_id, centrality, and improvement estimate
        """
        members = self._get_fleet_members()
        if not members:
            return None

        candidates = []

        for member_id in members:
            if not self.state_manager:
                continue

            state = self.state_manager.get_peer_state(member_id)
            if not state:
                continue

            topology = set(getattr(state, 'topology', []) or [])

            if target_peer_id in topology:
                continue

            score = 1.0
            channel_count = len(topology)
            if channel_count < 20:
                score += 0.2
            elif channel_count > 50:
                score -= 0.2

            member_centrality = self._get_member_centrality(member_id)
            centrality_improvement = self._estimate_centrality_improvement(member_id, target_peer_id)

            if member_centrality < LOW_CENTRALITY_THRESHOLD:
                score *= LOW_CENTRALITY_MEMBER_BONUS

            if centrality_improvement >= MIN_CENTRALITY_IMPROVEMENT:
                score *= CENTRALITY_IMPROVEMENT_BONUS

            candidates.append({
                "member_id": member_id,
                "score": score,
                "centrality": member_centrality,
                "centrality_improvement": centrality_improvement
            })

        if not candidates:
            return None

        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[0]

    def recommend_next_open(
        self,
        member_id: Optional[str] = None
    ) -> Optional[PositionRecommendation]:
        """
        Recommend next channel open for optimal positioning.

        Args:
            member_id: Specific member to recommend for, or None for any

        Returns:
            PositionRecommendation or None
        """
        # Check cooldown
        cooldown_key = member_id or "fleet"
        last_rec = self._recent_recommendations.get(cooldown_key, 0)
        if time.time() - last_rec < POSITION_RECOMMENDATION_COOLDOWN_HOURS * 3600:
            return None

        # Get valuable corridors
        if self.route_analyzer:
            corridors = self.route_analyzer.find_valuable_corridors(min_score=0.05)
        else:
            corridors = []

        # Find best target
        best_target = None
        best_score = 0.0

        for corridor in corridors:
            target = corridor.destination_peer_id

            # Check fleet coverage
            fleet_channels = self._count_fleet_channels_to_target(target)
            if fleet_channels >= MAX_MEMBERS_PER_TARGET:
                continue  # Already covered

            # Calculate priority score
            priority = corridor.value_score

            # Apply bonuses
            is_exchange, exchange_priority = self.route_analyzer._is_exchange(
                corridor.destination_alias
            ) if self.route_analyzer else (False, 0)

            if is_exchange:
                priority *= EXCHANGE_PRIORITY_BONUS

            if fleet_channels == 0:
                priority *= UNDERSERVED_PRIORITY_BONUS

            if priority > best_score:
                best_score = priority
                best_target = corridor

        if not best_target:
            return None

        # Select member to open
        if member_id:
            recommended_member = member_id
        else:
            recommended_member = self._select_best_member_for_target(
                best_target.destination_peer_id
            )

        if not recommended_member:
            return None

        # Create recommendation
        is_exchange, _ = self.route_analyzer._is_exchange(
            best_target.destination_alias
        ) if self.route_analyzer else (False, 0)

        fleet_channels = self._count_fleet_channels_to_target(best_target.destination_peer_id)

        # Determine priority tier
        if best_score >= 0.5:
            priority_tier = "critical"
        elif best_score >= 0.3:
            priority_tier = "high"
        elif best_score >= 0.15:
            priority_tier = "medium"
        else:
            priority_tier = "low"

        rec = PositionRecommendation(
            target_peer_id=best_target.destination_peer_id,
            target_alias=best_target.destination_alias,
            recommended_member=recommended_member,
            recommended_capacity_sats=5_000_000,  # Default 5M sats
            max_fee_rate_ppm=1000,  # Max 1000 ppm opening fee
            reason=f"High-value corridor ({best_target.value_tier} volume, "
                   f"{best_target.competition_level} competition)",
            priority_score=best_score,
            priority_tier=priority_tier,
            is_exchange=is_exchange,
            is_bridge_node=False,  # Would require network analysis
            is_underserved=fleet_channels == 0,
            corridor_value=best_target.value_score,
            current_fleet_channels=fleet_channels
        )

        # Record recommendation time
        self._recent_recommendations[cooldown_key] = time.time()

        return rec

    def get_positioning_recommendations(self, count: int = 5) -> List[PositionRecommendation]:
        """
        Get top positioning recommendations for the fleet.

        Now includes hive centrality analysis (Use Case 4):
        - Selects members who would benefit most from new connections
        - Estimates centrality improvement from target connection
        - Applies bonuses for network position improvements

        Args:
            count: Number of recommendations to return

        Returns:
            List of PositionRecommendation
        """
        recommendations = []

        # Get valuable corridors
        if self.route_analyzer:
            corridors = self.route_analyzer.find_valuable_corridors(min_score=0.03)
        else:
            corridors = []

        seen_targets = set()

        for corridor in corridors:
            if len(recommendations) >= count:
                break

            target = corridor.destination_peer_id
            if target in seen_targets:
                continue

            # Check fleet coverage
            fleet_channels = self._count_fleet_channels_to_target(target)
            if fleet_channels >= MAX_MEMBERS_PER_TARGET:
                continue

            # Select member with centrality metrics (Use Case 4)
            member_selection = self._select_best_member_with_metrics(target)
            if not member_selection:
                continue

            recommended_member = member_selection["member_id"]
            member_centrality = member_selection["centrality"]
            centrality_improvement = member_selection["centrality_improvement"]

            # Calculate priority
            priority = corridor.value_score

            is_exchange, _ = self.route_analyzer._is_exchange(
                corridor.destination_alias
            ) if self.route_analyzer else (False, 0)

            if is_exchange:
                priority *= EXCHANGE_PRIORITY_BONUS
            if fleet_channels == 0:
                priority *= UNDERSERVED_PRIORITY_BONUS

            # Use Case 4: Apply centrality improvement bonus
            improves_network_position = centrality_improvement >= MIN_CENTRALITY_IMPROVEMENT
            if improves_network_position:
                priority *= CENTRALITY_IMPROVEMENT_BONUS

            # Determine priority tier
            if priority >= 0.5:
                priority_tier = "critical"
            elif priority >= 0.3:
                priority_tier = "high"
            elif priority >= 0.15:
                priority_tier = "medium"
            else:
                priority_tier = "low"

            # Build reason with centrality context
            reason_parts = [f"{corridor.value_tier} value corridor"]
            if improves_network_position:
                reason_parts.append(f"improves hive centrality by ~{centrality_improvement:.0%}")
            if member_centrality < LOW_CENTRALITY_THRESHOLD:
                reason_parts.append(f"member needs better connectivity")

            rec = PositionRecommendation(
                target_peer_id=target,
                target_alias=corridor.destination_alias,
                recommended_member=recommended_member,
                recommended_capacity_sats=5_000_000,
                max_fee_rate_ppm=1000,
                reason="; ".join(reason_parts),
                priority_score=priority,
                priority_tier=priority_tier,
                is_exchange=is_exchange,
                is_underserved=fleet_channels == 0,
                corridor_value=corridor.value_score,
                current_fleet_channels=fleet_channels,
                # Centrality fields (Use Case 4)
                member_current_centrality=member_centrality,
                estimated_centrality_improvement=centrality_improvement,
                improves_network_position=improves_network_position
            )

            recommendations.append(rec)
            seen_targets.add(target)

        # Sort by priority
        recommendations.sort(key=lambda r: r.priority_score, reverse=True)

        return recommendations


# =============================================================================
# PHYSARUM CHANNEL MANAGER
# =============================================================================

class PhysarumChannelManager:
    """
    Channels evolve based on flow, like slime mold tubes.

    High flow → strengthen (splice in capacity)
    Low flow → atrophy (reduce capacity or close)

    This naturally optimizes capital allocation without central planning.
    """

    def __init__(self, plugin, yield_metrics_mgr=None):
        """
        Initialize the Physarum channel manager.

        Args:
            plugin: Plugin reference
            yield_metrics_mgr: YieldMetricsManager for flow data
        """
        self.plugin = plugin
        self.yield_metrics = yield_metrics_mgr
        self._our_pubkey: Optional[str] = None

        # Channel flow history
        self._flow_history: Dict[str, List[Tuple[float, float]]] = defaultdict(list)

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"PHYSARUM: {message}", level=level)

    def _get_channel_data(self, channel_id: str = None) -> List[Dict]:
        """Get channel data from RPC."""
        if not self.plugin:
            return []

        try:
            channels = self.plugin.rpc.listpeerchannels()
            all_channels = channels.get("channels", [])

            if channel_id:
                # Normalize channel ID format
                normalized = channel_id.replace(":", "x")
                return [
                    ch for ch in all_channels
                    if ch.get("short_channel_id", "").replace(":", "x") == normalized
                ]
            return all_channels
        except Exception as e:
            self._log(f"Error getting channel data: {e}", level="debug")
            return []

    def calculate_flow_intensity(self, channel_id: str) -> float:
        """
        Calculate flow intensity for a channel.

        Flow intensity = Daily volume / Capacity

        Uses actual channel age from SCID block height for accurate
        daily volume estimation instead of assuming 30 days.
        """
        channels = self._get_channel_data(channel_id)
        if not channels:
            return 0.0

        channel = channels[0]

        # Get capacity
        capacity_msat = channel.get("total_msat", 0)
        if isinstance(capacity_msat, str):
            capacity_msat = int(capacity_msat.replace("msat", ""))
        capacity_sats = capacity_msat // 1000

        if capacity_sats == 0:
            return 0.0

        # Get volume from in/out fulfilled
        in_fulfilled_msat = channel.get("in_fulfilled_msat", 0)
        out_fulfilled_msat = channel.get("out_fulfilled_msat", 0)

        if isinstance(in_fulfilled_msat, str):
            in_fulfilled_msat = int(in_fulfilled_msat.replace("msat", ""))
        if isinstance(out_fulfilled_msat, str):
            out_fulfilled_msat = int(out_fulfilled_msat.replace("msat", ""))

        total_volume_sats = (in_fulfilled_msat + out_fulfilled_msat) // 1000

        # Get actual channel age for accurate daily volume calculation
        age_days = self._get_channel_age_days(channel_id)
        if age_days <= 0:
            age_days = 30  # Fallback to 30 days if age unknown

        # Calculate daily volume using actual channel lifetime
        daily_volume_sats = total_volume_sats / age_days

        # Flow intensity = daily volume / capacity
        flow_intensity = daily_volume_sats / capacity_sats

        return flow_intensity

    def _get_channel_age_days(self, channel_id: str) -> int:
        """
        Get channel age in days from funding block height.

        Extracts block height from SCID (format: block_height x tx_index x output_index)
        and compares to current block height to get age.

        Returns:
            Channel age in days, or 0 if unable to determine
        """
        try:
            # Normalize SCID format
            normalized = channel_id.replace(":", "x")
            parts = normalized.split("x")
            if len(parts) != 3:
                return 0

            funding_block = int(parts[0])

            # Get current block height
            if self.plugin:
                info = self.plugin.rpc.getinfo()
                current_block = info.get("blockheight", 0)
                if current_block > funding_block:
                    # Approximate: 144 blocks per day on average
                    blocks_elapsed = current_block - funding_block
                    age_days = max(1, blocks_elapsed // 144)
                    return age_days

            return 0
        except Exception:
            return 0

    def _get_channel_revenue(self, channel_id: str) -> int:
        """Get channel revenue in sats."""
        if not self.yield_metrics:
            return 0

        try:
            metrics = self.yield_metrics.get_channel_yield_metrics(channel_id=channel_id)
            if metrics:
                return metrics[0].routing_revenue_sats
            return 0
        except Exception:
            return 0

    def _calculate_splice_amount(self, channel_id: str, flow: float) -> int:
        """Calculate recommended splice-in amount based on flow."""
        channels = self._get_channel_data(channel_id)
        if not channels:
            return 0

        channel = channels[0]
        capacity_msat = channel.get("total_msat", 0)
        if isinstance(capacity_msat, str):
            capacity_msat = int(capacity_msat.replace("msat", ""))
        capacity_sats = capacity_msat // 1000

        # Splice amount proportional to flow intensity
        # High flow → bigger splice
        base_splice_pct = 0.25  # Base: 25% capacity increase
        flow_multiplier = min(3.0, flow / STRENGTHEN_FLOW_THRESHOLD)

        splice_amount = int(capacity_sats * base_splice_pct * flow_multiplier)

        # Clamp to reasonable range
        splice_amount = max(500_000, min(splice_amount, 10_000_000))

        return splice_amount

    def get_channel_recommendation(self, channel_id: str) -> FlowRecommendation:
        """
        Get Physarum-inspired recommendation for a channel.

        Args:
            channel_id: Channel to analyze

        Returns:
            FlowRecommendation with action and reasoning
        """
        channels = self._get_channel_data(channel_id)
        if not channels:
            return FlowRecommendation(
                channel_id=channel_id,
                peer_id="",
                action="hold",
                reason="Channel not found"
            )

        channel = channels[0]
        peer_id = channel.get("peer_id", "")

        # Get metrics
        flow = self.calculate_flow_intensity(channel_id)
        age_days = self._get_channel_age_days(channel_id)
        revenue = self._get_channel_revenue(channel_id)

        capacity_msat = channel.get("total_msat", 0)
        if isinstance(capacity_msat, str):
            capacity_msat = int(capacity_msat.replace("msat", ""))
        capacity_sats = capacity_msat // 1000

        # Calculate turn rate
        turn_rate = flow  # They're equivalent in our model

        # Create base recommendation
        rec = FlowRecommendation(
            channel_id=channel_id,
            peer_id=peer_id,
            flow_intensity=flow,
            turn_rate=turn_rate,
            capacity_sats=capacity_sats,
            age_days=age_days,
            revenue_sats=revenue
        )

        # Physarum decision logic
        if flow >= STRENGTHEN_FLOW_THRESHOLD:
            # High flow - this tube should grow
            splice_amount = self._calculate_splice_amount(channel_id, flow)
            rec.action = "strengthen"
            rec.method = "splice_in"
            rec.splice_amount_sats = splice_amount
            rec.reason = f"Flow intensity {flow:.3f} exceeds threshold {STRENGTHEN_FLOW_THRESHOLD}"
            rec.expected_yield_change_pct = flow * 0.5  # Rough estimate

        elif flow < ATROPHY_FLOW_THRESHOLD:
            # Low flow - potential atrophy candidate
            if age_days < STIMULATE_GRACE_DAYS:
                # Young channel - try fee reduction to stimulate
                rec.action = "stimulate"
                rec.method = "fee_reduction"
                rec.reason = f"Young channel ({age_days} days) with low flow, attempting stimulation"
                rec.expected_yield_change_pct = 0.1

            elif age_days >= MIN_CHANNEL_AGE_FOR_ATROPHY_DAYS:
                # Mature channel with no flow - let it go
                rec.action = "atrophy"
                rec.method = "cooperative_close"
                rec.capital_to_redeploy_sats = capacity_sats
                rec.reason = f"Mature channel ({age_days} days) with flow {flow:.4f} below threshold"
                rec.expected_yield_change_pct = -0.1  # Short term loss

            else:
                # Middle-aged, low flow - hold and monitor
                rec.action = "hold"
                rec.reason = f"Low flow but not yet mature enough for atrophy"

        else:
            # Normal flow - hold
            rec.action = "hold"
            rec.reason = f"Flow intensity {flow:.4f} is within normal range"

        return rec

    def get_all_recommendations(self) -> List[FlowRecommendation]:
        """
        Get flow recommendations for all channels.

        Returns:
            List of FlowRecommendation sorted by action priority
        """
        recommendations = []

        channels = self._get_channel_data()

        for channel in channels:
            if channel.get("state") != "CHANNELD_NORMAL":
                continue

            channel_id = channel.get("short_channel_id", "").replace(":", "x")
            if not channel_id:
                continue

            rec = self.get_channel_recommendation(channel_id)
            if rec.action != "hold":
                recommendations.append(rec)

        # Sort by action priority: strengthen > stimulate > atrophy
        action_priority = {"strengthen": 0, "stimulate": 1, "atrophy": 2, "hold": 3}
        recommendations.sort(key=lambda r: action_priority.get(r.action, 4))

        return recommendations

    # =========================================================================
    # AUTO-TRIGGER METHODS (Phase 7.2)
    # =========================================================================

    def set_database(self, database) -> None:
        """Set database reference for pending_actions."""
        self._database = database

    def set_decision_engine(self, decision_engine) -> None:
        """Set decision engine reference for governance checks."""
        self._decision_engine = decision_engine

    def execute_physarum_cycle(self) -> Dict[str, Any]:
        """
        Execute one Physarum optimization cycle.

        Evaluates all channels and creates pending_actions for:
        - High-flow channels that should be strengthened (splice-in)
        - Old low-flow channels that should atrophy (close recommendation)
        - Young low-flow channels that need stimulation (fee reduction)

        All actions go through governance approval (pending_actions) - nothing
        is executed directly.

        Returns:
            Dict with cycle results:
            {
                "evaluated_channels": 25,
                "strengthen_proposals": 1,
                "atrophy_proposals": 0,
                "stimulate_proposals": 2,
                "skipped_rate_limit": 0,
                "actions_created": [...]
            }
        """
        result = {
            "evaluated_channels": 0,
            "strengthen_proposals": 0,
            "atrophy_proposals": 0,
            "stimulate_proposals": 0,
            "skipped_rate_limit": 0,
            "actions_created": []
        }

        # Check if we have required dependencies
        if not hasattr(self, '_database') or not self._database:
            self._log("Physarum cycle skipped: no database", level="debug")
            return result

        # Get all recommendations
        recommendations = self.get_all_recommendations()
        result["evaluated_channels"] = len(self._get_channel_data())

        now = int(time.time())

        for rec in recommendations:
            action_created = None

            if rec.action == "strengthen" and AUTO_STRENGTHEN_ENABLED:
                # Check if meets auto-strengthen criteria
                if rec.flow_intensity >= MIN_AUTO_STRENGTHEN_FLOW:
                    action_created = self._create_strengthen_action(rec, now)
                    if action_created:
                        result["strengthen_proposals"] += 1
                    else:
                        result["skipped_rate_limit"] += 1

            elif rec.action == "atrophy":
                # Atrophy always creates action for human review (never auto)
                action_created = self._create_atrophy_action(rec, now)
                if action_created:
                    result["atrophy_proposals"] += 1

            elif rec.action == "stimulate" and AUTO_STIMULATE_ENABLED:
                action_created = self._create_stimulate_action(rec, now)
                if action_created:
                    result["stimulate_proposals"] += 1

            if action_created:
                result["actions_created"].append(action_created)

        self._log(
            f"Physarum cycle: {result['evaluated_channels']} channels, "
            f"{result['strengthen_proposals']} strengthen, "
            f"{result['atrophy_proposals']} atrophy, "
            f"{result['stimulate_proposals']} stimulate",
            level="info"
        )

        return result

    def _create_strengthen_action(
        self,
        rec: 'FlowRecommendation',
        now: int
    ) -> Optional[Dict[str, Any]]:
        """
        Create pending_action for splice-in (strengthen).

        Safety checks:
        - Rate limit not exceeded
        - On-chain balance sufficient
        - Channel hasn't been strengthened recently
        - Splice amount within bounds

        Returns:
            Action dict if created, None if skipped
        """
        if not self._check_strengthen_rate_limit(now):
            self._log(f"Strengthen rate limit reached", level="debug")
            return None

        if not self._check_on_chain_reserve():
            self._log(f"Insufficient on-chain reserve for strengthen", level="debug")
            return None

        # Check channel cooldown
        if self._check_recent_strengthen(rec.channel_id, now):
            self._log(f"Channel {rec.channel_id[:12]}... strengthened recently", level="debug")
            return None

        # Clamp splice amount to safe range
        splice_amount = max(AUTO_STRENGTHEN_MIN_SATS, min(
            rec.splice_amount_sats or AUTO_STRENGTHEN_MIN_SATS,
            AUTO_STRENGTHEN_MAX_SATS
        ))

        action = {
            "action_type": "physarum_strengthen",
            "channel_id": rec.channel_id,
            "peer_id": rec.peer_id,
            "splice_amount_sats": splice_amount,
            "flow_intensity": rec.flow_intensity,
            "turn_rate": rec.turn_rate,
            "reason": rec.reason,
            "method": "splice_in",
            "timestamp": now
        }

        # Create pending action via database
        action_id = self._create_pending_action(
            action_type="physarum_strengthen",
            payload=action,
            expires_hours=72  # 3 days to approve
        )

        if action_id:
            action["action_id"] = action_id
            self._log(
                f"Created strengthen action for {rec.channel_id[:12]}... "
                f"({splice_amount:,} sats, flow={rec.flow_intensity:.3f})",
                level="info"
            )
            return action

        return None

    def _create_atrophy_action(
        self,
        rec: 'FlowRecommendation',
        now: int
    ) -> Optional[Dict[str, Any]]:
        """
        Create pending_action for close recommendation (atrophy).

        Always creates action (never auto-executes closes).
        Rate limit checked to avoid spam.

        Returns:
            Action dict if created, None if skipped
        """
        if not self._check_atrophy_rate_limit(now):
            self._log(f"Atrophy rate limit reached", level="debug")
            return None

        action = {
            "action_type": "physarum_atrophy",
            "channel_id": rec.channel_id,
            "peer_id": rec.peer_id,
            "capacity_sats": rec.capacity_sats,
            "capital_to_redeploy_sats": rec.capital_to_redeploy_sats,
            "flow_intensity": rec.flow_intensity,
            "age_days": rec.age_days,
            "revenue_sats": rec.revenue_sats,
            "reason": rec.reason,
            "method": "cooperative_close",
            "timestamp": now,
            "requires_human_approval": True  # Always
        }

        # Create pending action
        action_id = self._create_pending_action(
            action_type="physarum_atrophy",
            payload=action,
            expires_hours=168  # 7 days to review
        )

        if action_id:
            action["action_id"] = action_id
            self._log(
                f"Created atrophy action for {rec.channel_id[:12]}... "
                f"({rec.capacity_sats:,} sats, age={rec.age_days}d, flow={rec.flow_intensity:.4f})",
                level="info"
            )
            return action

        return None

    def _create_stimulate_action(
        self,
        rec: 'FlowRecommendation',
        now: int
    ) -> Optional[Dict[str, Any]]:
        """
        Create pending_action for fee reduction (stimulate young channel).

        Returns:
            Action dict if created, None if skipped
        """
        action = {
            "action_type": "physarum_stimulate",
            "channel_id": rec.channel_id,
            "peer_id": rec.peer_id,
            "capacity_sats": rec.capacity_sats,
            "flow_intensity": rec.flow_intensity,
            "age_days": rec.age_days,
            "reason": rec.reason,
            "method": "fee_reduction",
            "recommended_fee_ppm": 50,  # Stimulate with low fee
            "timestamp": now
        }

        # Create pending action
        action_id = self._create_pending_action(
            action_type="physarum_stimulate",
            payload=action,
            expires_hours=48  # 2 days to approve
        )

        if action_id:
            action["action_id"] = action_id
            self._log(
                f"Created stimulate action for {rec.channel_id[:12]}... "
                f"(young channel, age={rec.age_days}d)",
                level="info"
            )
            return action

        return None

    def _create_pending_action(
        self,
        action_type: str,
        payload: Dict,
        expires_hours: int = 72
    ) -> Optional[int]:
        """Create a pending action in the database."""
        import json

        if not hasattr(self, '_database') or not self._database:
            return None

        try:
            now = int(time.time())
            expires_at = now + (expires_hours * 3600)

            return self._database.create_pending_action(
                action_type=action_type,
                payload=json.dumps(payload),
                proposed_at=now,
                expires_at=expires_at
            )
        except Exception as e:
            self._log(f"Failed to create pending action: {e}", level="debug")
            return None

    def _check_strengthen_rate_limit(self, now: int) -> bool:
        """Check if we can create another strengthen action today."""
        if not hasattr(self, '_database') or not self._database:
            return True

        try:
            # Count today's strengthen actions
            day_start = now - (now % 86400)
            count = self._database.count_pending_actions_since(
                action_type="physarum_strengthen",
                since_timestamp=day_start
            )
            return count < MAX_AUTO_STRENGTHEN_PER_DAY
        except Exception:
            return True  # Allow on error

    def _check_atrophy_rate_limit(self, now: int) -> bool:
        """Check if we can create another atrophy action this week."""
        if not hasattr(self, '_database') or not self._database:
            return True

        try:
            week_start = now - (7 * 86400)
            count = self._database.count_pending_actions_since(
                action_type="physarum_atrophy",
                since_timestamp=week_start
            )
            return count < MAX_AUTO_ATROPHY_PER_WEEK
        except Exception:
            return True

    def _check_recent_strengthen(self, channel_id: str, now: int) -> bool:
        """Check if channel was strengthened recently."""
        if not hasattr(self, '_database') or not self._database:
            return False

        try:
            cutoff = now - (MIN_STRENGTHEN_INTERVAL_HOURS * 3600)
            return self._database.has_recent_action_for_channel(
                channel_id=channel_id,
                action_type="physarum_strengthen",
                since_timestamp=cutoff
            )
        except Exception:
            return False

    def _check_on_chain_reserve(self) -> bool:
        """Check if on-chain balance is sufficient for splice-in."""
        if not self.plugin:
            return False

        try:
            funds = self.plugin.rpc.listfunds()
            confirmed = sum(
                o.get("amount_msat", 0)
                for o in funds.get("outputs", [])
                if o.get("status") == "confirmed"
            )
            if isinstance(confirmed, str):
                confirmed = int(confirmed.replace("msat", ""))
            confirmed_sats = confirmed // 1000

            return confirmed_sats >= (AUTO_TRIGGER_MIN_ON_CHAIN_SATS + AUTO_STRENGTHEN_MIN_SATS)
        except Exception:
            return False

    def get_auto_trigger_status(self) -> Dict[str, Any]:
        """
        Get status of auto-trigger configuration and limits.

        Returns:
            Dict with auto-trigger status
        """
        now = int(time.time())

        status = {
            "auto_strengthen_enabled": AUTO_STRENGTHEN_ENABLED,
            "auto_atrophy_enabled": AUTO_ATROPHY_ENABLED,
            "auto_stimulate_enabled": AUTO_STIMULATE_ENABLED,
            "thresholds": {
                "strengthen_flow": STRENGTHEN_FLOW_THRESHOLD,
                "auto_strengthen_flow": MIN_AUTO_STRENGTHEN_FLOW,
                "atrophy_flow": ATROPHY_FLOW_THRESHOLD,
                "min_channel_age_days": MIN_CHANNEL_AGE_FOR_ATROPHY_DAYS
            },
            "limits": {
                "max_strengthen_per_day": MAX_AUTO_STRENGTHEN_PER_DAY,
                "max_atrophy_per_week": MAX_AUTO_ATROPHY_PER_WEEK,
                "min_strengthen_interval_hours": MIN_STRENGTHEN_INTERVAL_HOURS
            },
            "safety": {
                "min_on_chain_reserve_sats": AUTO_TRIGGER_MIN_ON_CHAIN_SATS,
                "splice_min_sats": AUTO_STRENGTHEN_MIN_SATS,
                "splice_max_sats": AUTO_STRENGTHEN_MAX_SATS
            }
        }

        # Add current usage if database available
        if hasattr(self, '_database') and self._database:
            try:
                day_start = now - (now % 86400)
                week_start = now - (7 * 86400)

                status["current_usage"] = {
                    "strengthen_today": self._database.count_pending_actions_since(
                        "physarum_strengthen", day_start
                    ),
                    "atrophy_this_week": self._database.count_pending_actions_since(
                        "physarum_atrophy", week_start
                    )
                }
            except Exception:
                pass

        return status


# =============================================================================
# STRATEGIC POSITIONING MANAGER
# =============================================================================

class StrategicPositioningManager:
    """
    Main interface for Phase 5 strategic positioning.

    Coordinates:
    - Route value analysis
    - Fleet positioning strategy
    - Physarum-based channel lifecycle
    """

    def __init__(
        self,
        plugin,
        database=None,
        state_manager=None,
        fee_coordination_mgr=None,
        yield_metrics_mgr=None,
        planner=None
    ):
        """
        Initialize the strategic positioning manager.

        Args:
            plugin: Plugin reference
            database: Database for persistence
            state_manager: StateManager for fleet state
            fee_coordination_mgr: FeeCoordinationManager for corridor data
            yield_metrics_mgr: YieldMetricsManager for flow data
            planner: Planner for underserved targets
        """
        self.plugin = plugin
        self.database = database

        # Initialize components
        self.route_analyzer = RouteValueAnalyzer(
            plugin=plugin,
            state_manager=state_manager,
            fee_coordination_mgr=fee_coordination_mgr
        )

        self.positioning_strategy = FleetPositioningStrategy(
            plugin=plugin,
            state_manager=state_manager,
            route_analyzer=self.route_analyzer,
            planner=planner
        )

        self.physarum_mgr = PhysarumChannelManager(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics_mgr
        )
        # Pass database reference for pending_actions
        self.physarum_mgr.set_database(database)

        self._our_pubkey: Optional[str] = None

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey
        self.route_analyzer.set_our_pubkey(pubkey)
        self.positioning_strategy.set_our_pubkey(pubkey)
        self.physarum_mgr.set_our_pubkey(pubkey)

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"STRATEGIC_POS: {message}", level=level)

    def get_valuable_corridors(self, min_score: float = 0.05) -> List[Dict[str, Any]]:
        """
        Get high-value corridors for potential positioning.

        Args:
            min_score: Minimum value score

        Returns:
            List of corridor value dicts
        """
        corridors = self.route_analyzer.find_valuable_corridors(min_score=min_score)
        return [c.to_dict() for c in corridors]

    def get_exchange_coverage(self) -> Dict[str, Any]:
        """
        Get exchange connectivity status.

        Returns:
            Dict with exchange coverage analysis
        """
        targets = self.route_analyzer.find_exchange_targets()

        covered = sum(1 for t in targets if t["has_connection"])
        total = len(targets)

        return {
            "total_priority_exchanges": total,
            "covered_exchanges": covered,
            "coverage_pct": round(covered / total * 100, 1) if total > 0 else 0,
            "exchanges": targets
        }

    def get_positioning_recommendations(self, count: int = 5) -> List[Dict[str, Any]]:
        """
        Get channel open recommendations for strategic positioning.

        Args:
            count: Number of recommendations

        Returns:
            List of recommendation dicts
        """
        recs = self.positioning_strategy.get_positioning_recommendations(count=count)
        return [r.to_dict() for r in recs]

    def get_flow_recommendations(
        self,
        channel_id: str = None
    ) -> List[Dict[str, Any]]:
        """
        Get Physarum-inspired flow recommendations.

        Args:
            channel_id: Specific channel, or None for all

        Returns:
            List of flow recommendation dicts
        """
        if channel_id:
            rec = self.physarum_mgr.get_channel_recommendation(channel_id)
            return [rec.to_dict()]
        else:
            recs = self.physarum_mgr.get_all_recommendations()
            return [r.to_dict() for r in recs]

    def report_flow_intensity(
        self,
        channel_id: str,
        peer_id: str,
        intensity: float
    ) -> Dict[str, Any]:
        """
        Report flow intensity for a channel.

        This updates the Physarum model with observed flow.

        Args:
            channel_id: Channel ID
            peer_id: Peer ID
            intensity: Observed flow intensity

        Returns:
            Dict with acknowledgment
        """
        # Store in flow history
        self.physarum_mgr._flow_history[channel_id].append((time.time(), intensity))

        # Trim old entries
        cutoff = time.time() - (7 * 24 * 3600)  # Keep 7 days
        self.physarum_mgr._flow_history[channel_id] = [
            (t, i) for t, i in self.physarum_mgr._flow_history[channel_id]
            if t >= cutoff
        ]

        return {
            "recorded": True,
            "channel_id": channel_id,
            "intensity": intensity,
            "history_entries": len(self.physarum_mgr._flow_history[channel_id])
        }

    def get_positioning_summary(self) -> Dict[str, Any]:
        """
        Get summary of strategic positioning.

        Returns:
            PositioningSummary dict
        """
        summary = PositioningSummary()

        # Get corridor data
        corridors = self.route_analyzer.find_valuable_corridors(min_score=0.01)
        summary.total_targets_analyzed = len(corridors)
        summary.high_value_corridors = sum(1 for c in corridors if c.value_tier == "high")

        # Get exchange coverage
        exchange_data = self.get_exchange_coverage()
        summary.exchange_coverage_pct = exchange_data["coverage_pct"]

        # Get recommendations
        position_recs = self.positioning_strategy.get_positioning_recommendations(count=20)
        summary.open_recommendations = len(position_recs)
        summary.underserved_targets = sum(1 for r in position_recs if r.is_underserved)

        # Get flow recommendations
        flow_recs = self.physarum_mgr.get_all_recommendations()
        summary.strengthen_recommendations = sum(1 for r in flow_recs if r.action == "strengthen")
        summary.atrophy_recommendations = sum(1 for r in flow_recs if r.action == "atrophy")
        summary.capital_to_redeploy_sats = sum(
            r.capital_to_redeploy_sats for r in flow_recs if r.action == "atrophy"
        )

        return summary.to_dict()

    def get_status(self) -> Dict[str, Any]:
        """
        Get overall strategic positioning status.

        Returns:
            Status dict
        """
        summary = self.get_positioning_summary()

        return {
            "enabled": True,
            "summary": summary,
            "thresholds": {
                "strengthen_flow_threshold": STRENGTHEN_FLOW_THRESHOLD,
                "atrophy_flow_threshold": ATROPHY_FLOW_THRESHOLD,
                "high_value_volume_daily": HIGH_VALUE_VOLUME_SATS_DAILY,
                "max_members_per_target": MAX_MEMBERS_PER_TARGET
            },
            "priority_exchanges": list(PRIORITY_EXCHANGES.keys())
        }

    # =========================================================================
    # FLEET INTELLIGENCE SHARING (Phase 14.2)
    # =========================================================================

    def get_shareable_corridors(
        self,
        min_value_score: float = 0.05,
        max_corridors: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get valuable corridors suitable for sharing with fleet.

        Args:
            min_value_score: Minimum value score to share
            max_corridors: Maximum number of corridors

        Returns:
            List of corridor dicts ready for serialization
        """
        shareable = []

        try:
            corridors = self.route_analyzer.find_valuable_corridors(min_score=min_value_score)

            for c in corridors:
                shareable.append({
                    "source_peer_id": c.source_peer_id,
                    "destination_peer_id": c.destination_peer_id,
                    "source_alias": c.source_alias,
                    "destination_alias": c.destination_alias,
                    "daily_volume_sats": c.daily_volume_sats,
                    "value_score": round(c.value_score, 4),
                    "competition_level": c.competition_level,
                    "competitor_count": c.competitor_count,
                    "margin_estimate_ppm": c.margin_estimate_ppm,
                    "fleet_coverage": c.fleet_coverage
                })

        except Exception as e:
            self._log(f"Error collecting shareable corridors: {e}", level="debug")

        return shareable[:max_corridors]

    def get_shareable_positioning_recommendations(
        self,
        max_recommendations: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get positioning recommendations suitable for sharing.

        Args:
            max_recommendations: Maximum number of recommendations

        Returns:
            List of positioning proposal dicts
        """
        shareable = []

        try:
            recs = self.positioning_strategy.get_positioning_recommendations(count=max_recommendations)

            for r in recs:
                shareable.append({
                    "target_peer_id": r.target_peer_id,
                    "recommended_member": r.recommended_member or "",
                    "priority_tier": r.priority_tier,
                    "target_capacity_sats": r.target_capacity_sats,
                    "reason": r.reason,
                    "value_score": round(r.value_score, 4),
                    "is_exchange": r.is_exchange,
                    "is_underserved": r.is_underserved
                })

        except Exception as e:
            self._log(f"Error collecting positioning recommendations: {e}", level="debug")

        return shareable

    def get_shareable_physarum_recommendations(
        self,
        exclude_hold: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get Physarum flow recommendations suitable for sharing.

        Args:
            exclude_hold: Whether to exclude "hold" recommendations

        Returns:
            List of Physarum recommendation dicts
        """
        shareable = []

        try:
            recs = self.physarum_mgr.get_all_recommendations()

            for r in recs:
                if exclude_hold and r.action == "hold":
                    continue

                shareable.append({
                    "channel_id": r.channel_id,
                    "peer_id": r.peer_id,
                    "action": r.action,
                    "flow_intensity": round(r.flow_intensity, 4),
                    "reason": r.reason,
                    "expected_yield_change_pct": round(r.expected_yield_change_pct, 2),
                    "splice_amount_sats": r.splice_amount_sats,
                    "capital_to_redeploy_sats": r.capital_to_redeploy_sats
                })

        except Exception as e:
            self._log(f"Error collecting Physarum recommendations: {e}", level="debug")

        return shareable

    def receive_corridor_from_fleet(
        self,
        reporter_id: str,
        corridor_data: Dict[str, Any]
    ) -> bool:
        """
        Receive a corridor value report from another fleet member.

        Args:
            reporter_id: The fleet member who reported this
            corridor_data: Dict with corridor details

        Returns:
            True if stored successfully
        """
        source = corridor_data.get("source_peer_id")
        dest = corridor_data.get("destination_peer_id")
        if not source or not dest:
            return False

        # Initialize remote corridors storage
        if not hasattr(self, "_remote_corridors"):
            self._remote_corridors: Dict[str, List[Dict[str, Any]]] = {}

        key = f"{source}:{dest}"
        entry = {
            "reporter_id": reporter_id,
            "daily_volume_sats": corridor_data.get("daily_volume_sats", 0),
            "value_score": corridor_data.get("value_score", 0),
            "competition_level": corridor_data.get("competition_level", "unknown"),
            "timestamp": time.time()
        }

        if key not in self._remote_corridors:
            self._remote_corridors[key] = []

        self._remote_corridors[key].append(entry)

        # Keep only last 5 reports per corridor
        if len(self._remote_corridors[key]) > 5:
            self._remote_corridors[key] = self._remote_corridors[key][-5:]

        return True

    def receive_positioning_proposal_from_fleet(
        self,
        reporter_id: str,
        proposal_data: Dict[str, Any]
    ) -> bool:
        """
        Receive a positioning proposal from another fleet member.

        Args:
            reporter_id: The fleet member who proposed this
            proposal_data: Dict with proposal details

        Returns:
            True if stored successfully
        """
        target = proposal_data.get("target_peer_id")
        if not target:
            return False

        # Initialize remote proposals storage
        if not hasattr(self, "_remote_proposals"):
            self._remote_proposals: List[Dict[str, Any]] = []

        entry = {
            "reporter_id": reporter_id,
            "target_peer_id": target,
            "recommended_member": proposal_data.get("recommended_member", ""),
            "priority_tier": proposal_data.get("priority_tier", "low"),
            "target_capacity_sats": proposal_data.get("target_capacity_sats", 0),
            "reason": proposal_data.get("reason", ""),
            "value_score": proposal_data.get("value_score", 0),
            "timestamp": time.time()
        }

        self._remote_proposals.append(entry)

        # Keep only last 50 proposals
        if len(self._remote_proposals) > 50:
            self._remote_proposals = self._remote_proposals[-50:]

        return True

    def receive_physarum_recommendation_from_fleet(
        self,
        reporter_id: str,
        recommendation_data: Dict[str, Any]
    ) -> bool:
        """
        Receive a Physarum recommendation from another fleet member.

        Args:
            reporter_id: The fleet member who recommended this
            recommendation_data: Dict with recommendation details

        Returns:
            True if stored successfully
        """
        channel_id = recommendation_data.get("channel_id")
        peer_id = recommendation_data.get("peer_id")
        if not channel_id or not peer_id:
            return False

        # Initialize remote recommendations storage
        if not hasattr(self, "_remote_physarum"):
            self._remote_physarum: Dict[str, List[Dict[str, Any]]] = {}

        entry = {
            "reporter_id": reporter_id,
            "action": recommendation_data.get("action", "hold"),
            "flow_intensity": recommendation_data.get("flow_intensity", 0),
            "reason": recommendation_data.get("reason", ""),
            "expected_yield_change_pct": recommendation_data.get("expected_yield_change_pct", 0),
            "timestamp": time.time()
        }

        if peer_id not in self._remote_physarum:
            self._remote_physarum[peer_id] = []

        self._remote_physarum[peer_id].append(entry)

        # Keep only last 10 recommendations per peer
        if len(self._remote_physarum[peer_id]) > 10:
            self._remote_physarum[peer_id] = self._remote_physarum[peer_id][-10:]

        return True

    def get_fleet_corridor_consensus(self, source: str, dest: str) -> Optional[Dict[str, Any]]:
        """Get consensus corridor value from fleet reports."""
        if not hasattr(self, "_remote_corridors"):
            return None

        key = f"{source}:{dest}"
        reports = self._remote_corridors.get(key, [])
        if not reports:
            return None

        now = time.time()
        recent = [r for r in reports if now - r.get("timestamp", 0) < 7 * 86400]
        if not recent:
            return None

        avg_volume = sum(r.get("daily_volume_sats", 0) for r in recent) / len(recent)
        avg_score = sum(r.get("value_score", 0) for r in recent) / len(recent)

        return {
            "source": source,
            "destination": dest,
            "avg_daily_volume_sats": int(avg_volume),
            "avg_value_score": round(avg_score, 4),
            "reporter_count": len(recent)
        }

    def cleanup_old_remote_data(self, max_age_days: float = 7) -> int:
        """Remove old remote positioning data."""
        cutoff = time.time() - (max_age_days * 86400)
        cleaned = 0

        # Cleanup corridors
        if hasattr(self, "_remote_corridors"):
            for key in list(self._remote_corridors.keys()):
                before = len(self._remote_corridors[key])
                self._remote_corridors[key] = [
                    r for r in self._remote_corridors[key]
                    if r.get("timestamp", 0) > cutoff
                ]
                cleaned += before - len(self._remote_corridors[key])
                if not self._remote_corridors[key]:
                    del self._remote_corridors[key]

        # Cleanup proposals
        if hasattr(self, "_remote_proposals"):
            before = len(self._remote_proposals)
            self._remote_proposals = [
                p for p in self._remote_proposals
                if p.get("timestamp", 0) > cutoff
            ]
            cleaned += before - len(self._remote_proposals)

        # Cleanup physarum
        if hasattr(self, "_remote_physarum"):
            for peer_id in list(self._remote_physarum.keys()):
                before = len(self._remote_physarum[peer_id])
                self._remote_physarum[peer_id] = [
                    r for r in self._remote_physarum[peer_id]
                    if r.get("timestamp", 0) > cutoff
                ]
                cleaned += before - len(self._remote_physarum[peer_id])
                if not self._remote_physarum[peer_id]:
                    del self._remote_physarum[peer_id]

        return cleaned
