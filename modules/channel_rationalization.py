"""
Channel Rationalization Module - Swarm Intelligence for Fleet Efficiency

When multiple fleet members have channels to the same peer (redundant coverage),
this module determines which member(s) "own" those routes based on stigmergic
markers (routing success patterns) and recommends channel closes for
non-performing members.

Part of the Hive covenant: members follow swarm intelligence recommendations
to maximize collective efficiency.

Key concepts:
- Ownership: Determined by cumulative marker strength (successful routing)
- Redundancy: Multiple members → same peer without proportional routing
- Rationalization: Recommending closes for underperforming redundant channels

Author: Lightning Goats Team
"""

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from . import network_metrics

# =============================================================================
# CONSTANTS
# =============================================================================

# Connectivity impact thresholds
CONNECTIVITY_CRITICAL_DROP = 0.3   # >30% drop in centrality = critical
CONNECTIVITY_WARNING_DROP = 0.15   # >15% drop = warning, reduce confidence

# Ownership thresholds
OWNERSHIP_DOMINANT_RATIO = 0.6      # Member with >60% markers owns the route
OWNERSHIP_MIN_MARKERS = 3           # Need at least 3 markers to claim ownership
OWNERSHIP_MIN_STRENGTH = 1.0        # Minimum total marker strength to claim

# Redundancy thresholds
REDUNDANCY_MIN_MEMBERS = 2          # At least 2 members = potential redundancy
MAX_HEALTHY_REDUNDANCY = 2          # Up to 2 members per peer is healthy

# Performance thresholds for close recommendations
UNDERPERFORMER_MARKER_RATIO = 0.1   # <10% of leader's markers = underperformer
UNDERPERFORMER_MIN_AGE_DAYS = 30    # Channel must be >30 days old to recommend close
UNDERPERFORMER_MIN_CAPACITY = 1_000_000  # Only consider channels >1M sats

# Grace periods
NEW_CHANNEL_GRACE_DAYS = 14         # Don't recommend close for channels <14 days
CLOSE_RECOMMENDATION_COOLDOWN_HOURS = 72  # Don't repeat recommendation within 72h


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class PeerCoverage:
    """
    Coverage analysis for a single external peer.

    Shows which fleet members have channels to this peer and their
    routing performance (marker strength).
    """
    peer_id: str
    peer_alias: Optional[str] = None

    # Fleet members with channels to this peer
    members_with_channels: List[str] = field(default_factory=list)

    # Marker strength by member (sum of all routing markers)
    member_marker_strength: Dict[str, float] = field(default_factory=dict)

    # Marker count by member
    member_marker_count: Dict[str, int] = field(default_factory=dict)

    # Channel capacity by member
    member_capacity_sats: Dict[str, int] = field(default_factory=dict)

    # Determined owner (None if no clear owner)
    owner_member: Optional[str] = None
    ownership_confidence: float = 0.0

    # Redundancy level
    redundancy_count: int = 0
    is_over_redundant: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "peer_id": self.peer_id,
            "peer_alias": self.peer_alias,
            "members_with_channels": self.members_with_channels,
            "member_marker_strength": {k: round(v, 3) for k, v in self.member_marker_strength.items()},
            "member_marker_count": self.member_marker_count,
            "member_capacity_sats": self.member_capacity_sats,
            "owner_member": self.owner_member,
            "ownership_confidence": round(self.ownership_confidence, 2),
            "redundancy_count": self.redundancy_count,
            "is_over_redundant": self.is_over_redundant
        }


@dataclass
class CloseRecommendation:
    """
    Recommendation to close an underperforming redundant channel.
    """
    member_id: str
    peer_id: str
    channel_id: str

    # Context
    peer_alias: Optional[str] = None
    member_alias: Optional[str] = None

    # Performance metrics
    member_marker_strength: float = 0.0
    owner_marker_strength: float = 0.0
    owner_member: str = ""

    # Channel details
    capacity_sats: int = 0
    channel_age_days: int = 0
    local_balance_pct: float = 0.0

    # Recommendation details
    reason: str = ""
    confidence: float = 0.0
    urgency: str = "low"  # "low", "medium", "high"

    # Estimated impact
    freed_capital_sats: int = 0

    # Connectivity impact (Use Case 3)
    connectivity_impact: str = "none"  # "none", "low", "warning", "critical"
    current_hive_centrality: float = 0.0
    projected_hive_centrality: float = 0.0
    connectivity_warning: Optional[str] = None

    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "member_id": self.member_id,
            "peer_id": self.peer_id,
            "channel_id": self.channel_id,
            "peer_alias": self.peer_alias,
            "member_alias": self.member_alias,
            "member_marker_strength": round(self.member_marker_strength, 3),
            "owner_marker_strength": round(self.owner_marker_strength, 3),
            "owner_member": self.owner_member,
            "capacity_sats": self.capacity_sats,
            "channel_age_days": self.channel_age_days,
            "local_balance_pct": round(self.local_balance_pct, 2),
            "reason": self.reason,
            "confidence": round(self.confidence, 2),
            "urgency": self.urgency,
            "freed_capital_sats": self.freed_capital_sats,
            "connectivity_impact": self.connectivity_impact,
            "timestamp": self.timestamp
        }
        # Only include connectivity details if there's an impact
        if self.connectivity_impact != "none":
            result["current_hive_centrality"] = round(self.current_hive_centrality, 3)
            result["projected_hive_centrality"] = round(self.projected_hive_centrality, 3)
            if self.connectivity_warning:
                result["connectivity_warning"] = self.connectivity_warning
        return result


@dataclass
class RationalizationSummary:
    """
    Summary of channel rationalization analysis.
    """
    total_peers_analyzed: int = 0
    redundant_peers: int = 0
    over_redundant_peers: int = 0
    close_recommendations: int = 0
    potential_freed_capital_sats: int = 0

    # Top recommendations
    top_recommendations: List[Dict] = field(default_factory=list)

    # Coverage health
    well_owned_peers: int = 0      # Clear owner with strong markers
    contested_peers: int = 0       # Multiple members, no clear owner
    orphan_peers: int = 0          # No routing activity despite channels

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_peers_analyzed": self.total_peers_analyzed,
            "redundant_peers": self.redundant_peers,
            "over_redundant_peers": self.over_redundant_peers,
            "close_recommendations": self.close_recommendations,
            "potential_freed_capital_sats": self.potential_freed_capital_sats,
            "top_recommendations": self.top_recommendations,
            "well_owned_peers": self.well_owned_peers,
            "contested_peers": self.contested_peers,
            "orphan_peers": self.orphan_peers
        }


# =============================================================================
# REDUNDANCY ANALYZER
# =============================================================================

class RedundancyAnalyzer:
    """
    Analyzes fleet coverage redundancy.

    Identifies peers that multiple fleet members have channels to
    and calculates the distribution of routing activity.
    """

    def __init__(self, plugin, state_manager=None, fee_coordination_mgr=None):
        """
        Initialize the redundancy analyzer.

        Args:
            plugin: Plugin reference for RPC calls
            state_manager: StateManager for fleet topology
            fee_coordination_mgr: FeeCoordinationManager for marker access
        """
        self.plugin = plugin
        self.state_manager = state_manager
        self.fee_coordination_mgr = fee_coordination_mgr
        self._our_pubkey: Optional[str] = None

        # Cache
        self._coverage_cache: Dict[str, PeerCoverage] = {}
        self._cache_time: float = 0
        self._cache_ttl: float = 300  # 5 minutes

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"REDUNDANCY: {message}", level=level)

    def _get_fleet_members(self) -> List[str]:
        """Get list of fleet member pubkeys."""
        if not self.state_manager:
            return []

        try:
            all_states = self.state_manager.get_all_peer_states()
            return [s.peer_id for s in all_states]
        except Exception:
            return []

    def _get_member_topology(self, member_id: str) -> Set[str]:
        """Get the set of peers a member has channels with."""
        if not self.state_manager:
            return set()

        try:
            state = self.state_manager.get_peer_state(member_id)
            if state:
                return set(getattr(state, 'topology', []) or [])
            return set()
        except Exception:
            return set()

    def _get_markers_for_peer(self, peer_id: str) -> List[Any]:
        """
        Get all stigmergic markers involving this peer.

        Markers where peer is either source or destination.
        """
        if not self.fee_coordination_mgr:
            return []

        try:
            stigmergy = self.fee_coordination_mgr.stigmergy
            if not stigmergy:
                return []

            all_markers = stigmergy.get_all_markers()
            return [
                m for m in all_markers
                if m.source_peer_id == peer_id or m.destination_peer_id == peer_id
            ]
        except Exception as e:
            self._log(f"Error getting markers for peer: {e}", level="debug")
            return []

    def analyze_peer_coverage(self, peer_id: str) -> PeerCoverage:
        """
        Analyze coverage for a single external peer.

        Args:
            peer_id: External peer to analyze

        Returns:
            PeerCoverage with ownership and redundancy analysis
        """
        coverage = PeerCoverage(peer_id=peer_id)

        fleet_members = self._get_fleet_members()
        if not fleet_members:
            return coverage

        # Find which members have channels to this peer
        for member_id in fleet_members:
            topology = self._get_member_topology(member_id)
            if peer_id in topology:
                coverage.members_with_channels.append(member_id)
                coverage.member_marker_strength[member_id] = 0.0
                coverage.member_marker_count[member_id] = 0

        coverage.redundancy_count = len(coverage.members_with_channels)
        coverage.is_over_redundant = coverage.redundancy_count > MAX_HEALTHY_REDUNDANCY

        if not coverage.members_with_channels:
            return coverage

        # Analyze markers to determine ownership
        markers = self._get_markers_for_peer(peer_id)

        for marker in markers:
            depositor = marker.depositor
            if depositor in coverage.member_marker_strength:
                coverage.member_marker_strength[depositor] += marker.strength
                coverage.member_marker_count[depositor] += 1

        # Determine owner
        self._determine_ownership(coverage)

        return coverage

    def _determine_ownership(self, coverage: PeerCoverage) -> None:
        """
        Determine which member owns this peer relationship.

        Owner is the member with dominant marker strength.
        """
        if not coverage.member_marker_strength:
            return

        total_strength = sum(coverage.member_marker_strength.values())
        if total_strength < OWNERSHIP_MIN_STRENGTH:
            # Not enough routing activity to determine ownership
            return

        # Find strongest member
        strongest_member = max(
            coverage.member_marker_strength.items(),
            key=lambda x: x[1]
        )
        member_id, strength = strongest_member

        # Check if dominant
        strength_ratio = strength / total_strength if total_strength > 0 else 0
        marker_count = coverage.member_marker_count.get(member_id, 0)

        if strength_ratio >= OWNERSHIP_DOMINANT_RATIO and marker_count >= OWNERSHIP_MIN_MARKERS:
            coverage.owner_member = member_id
            coverage.ownership_confidence = min(0.95, strength_ratio)
        elif strength_ratio >= 0.4:  # Plurality but not dominant
            coverage.owner_member = member_id
            coverage.ownership_confidence = strength_ratio * 0.7  # Lower confidence

    def analyze_all_coverage(self) -> Dict[str, PeerCoverage]:
        """
        Analyze coverage for all external peers with fleet channels.

        Returns:
            Dict mapping peer_id -> PeerCoverage
        """
        now = time.time()

        # Return cached if fresh
        if self._coverage_cache and now - self._cache_time < self._cache_ttl:
            return self._coverage_cache

        coverage_map = {}

        fleet_members = self._get_fleet_members()
        if not fleet_members:
            return coverage_map

        # Collect all external peers
        all_peers: Set[str] = set()
        for member_id in fleet_members:
            topology = self._get_member_topology(member_id)
            # Exclude other fleet members
            external_peers = topology - set(fleet_members)
            all_peers.update(external_peers)

        # Analyze each peer
        for peer_id in all_peers:
            coverage = self.analyze_peer_coverage(peer_id)
            if coverage.redundancy_count >= REDUNDANCY_MIN_MEMBERS:
                coverage_map[peer_id] = coverage

        self._coverage_cache = coverage_map
        self._cache_time = now

        return coverage_map

    def get_redundant_peers(self) -> List[PeerCoverage]:
        """
        Get list of peers with redundant coverage (multiple members).
        """
        all_coverage = self.analyze_all_coverage()
        return list(all_coverage.values())

    def get_over_redundant_peers(self) -> List[PeerCoverage]:
        """
        Get list of peers with excessive redundancy (>2 members).
        """
        all_coverage = self.analyze_all_coverage()
        return [c for c in all_coverage.values() if c.is_over_redundant]


# =============================================================================
# CHANNEL RATIONALIZER
# =============================================================================

class ChannelRationalizer:
    """
    Generates close recommendations for underperforming redundant channels.

    Uses stigmergic markers to determine ownership and identifies
    channels that should be closed to free capital.
    """

    def __init__(
        self,
        plugin,
        database=None,
        state_manager=None,
        fee_coordination_mgr=None,
        governance=None
    ):
        """
        Initialize the channel rationalizer.

        Args:
            plugin: Plugin reference
            database: Database for persistence
            state_manager: StateManager for fleet state
            fee_coordination_mgr: FeeCoordinationManager for markers
            governance: Governance module for pending_actions
        """
        self.plugin = plugin
        self.database = database
        self.state_manager = state_manager
        self.governance = governance

        # Initialize redundancy analyzer
        self.redundancy_analyzer = RedundancyAnalyzer(
            plugin=plugin,
            state_manager=state_manager,
            fee_coordination_mgr=fee_coordination_mgr
        )

        self._our_pubkey: Optional[str] = None

        # Track recent recommendations to avoid spam
        self._recent_recommendations: Dict[str, float] = {}

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey
        self.redundancy_analyzer.set_our_pubkey(pubkey)

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"RATIONALIZE: {message}", level=level)

    def _get_channel_info(self, member_id: str, peer_id: str) -> Optional[Dict]:
        """
        Get channel information for a member's channel to a peer.

        Returns channel details if this is our node, otherwise returns
        estimated data from state.
        """
        if member_id == self._our_pubkey and self.plugin:
            try:
                channels = self.plugin.rpc.listpeerchannels()
                for ch in channels.get("channels", []):
                    if ch.get("peer_id") == peer_id:
                        return {
                            "channel_id": ch.get("short_channel_id", "").replace(":", "x"),
                            "capacity_sats": ch.get("total_msat", 0) // 1000,
                            "local_balance_sats": ch.get("to_us_msat", 0) // 1000,
                            "state": ch.get("state"),
                            "funding_tx": ch.get("funding_txid"),
                            # Estimate age from funding blockheight if available
                            "opened_at": ch.get("open_confirm_time")
                        }
            except Exception:
                pass

        # For remote members, use state manager data
        if self.state_manager:
            try:
                state = self.state_manager.get_peer_state(member_id)
                if state:
                    # Return estimated data
                    return {
                        "channel_id": "unknown",
                        "capacity_sats": getattr(state, 'capacity_sats', 0) // max(1, len(getattr(state, 'topology', [1]) or [1])),
                        "local_balance_sats": 0,
                        "state": "CHANNELD_NORMAL"
                    }
            except Exception:
                pass

        return None

    def _assess_connectivity_impact(
        self,
        member_id: str,
        peer_id: str
    ) -> Dict[str, Any]:
        """
        Assess the connectivity impact of closing a member's channel to a peer.

        Returns impact assessment including:
        - current_centrality: Current hive centrality
        - projected_centrality: Estimated centrality after closing channel
        - impact_level: "none", "low", "warning", "critical"
        - warning: Optional warning message
        """
        calculator = network_metrics.get_calculator()
        if not calculator:
            return {"impact_level": "none", "warning": None}

        # Get current metrics
        metrics = calculator.get_member_metrics(member_id)
        if not metrics:
            return {"impact_level": "none", "warning": None}

        current_centrality = metrics.hive_centrality
        hive_peer_count = metrics.hive_peer_count

        # Check if the peer being closed is a hive member
        topology = calculator.get_topology_snapshot()
        if not topology:
            return {
                "impact_level": "none",
                "current_centrality": current_centrality,
                "projected_centrality": current_centrality,
                "warning": None
            }

        # Check if this peer is a hive member
        is_hive_peer = peer_id in topology.member_topologies

        if not is_hive_peer:
            # Closing external channel doesn't affect hive centrality significantly
            return {
                "impact_level": "none",
                "current_centrality": current_centrality,
                "projected_centrality": current_centrality,
                "warning": None
            }

        # Estimate impact of losing this hive connection
        # Simple heuristic: centrality drop proportional to 1 / (hive_peer_count)
        if hive_peer_count <= 1:
            # Closing only hive connection = critical
            projected_centrality = 0.0
            impact_level = "critical"
            warning = "CRITICAL: This is the member's only hive connection!"
        elif hive_peer_count == 2:
            # Losing 1 of 2 connections
            projected_centrality = current_centrality * 0.5
            centrality_drop = current_centrality - projected_centrality
            if centrality_drop / max(current_centrality, 0.01) > CONNECTIVITY_CRITICAL_DROP:
                impact_level = "critical"
                warning = "Closing would significantly reduce hive connectivity"
            else:
                impact_level = "warning"
                warning = "Member would have only 1 hive connection remaining"
        else:
            # Estimate proportional drop
            drop_factor = 1.0 / hive_peer_count
            projected_centrality = current_centrality * (1 - drop_factor)
            centrality_drop_pct = (current_centrality - projected_centrality) / max(current_centrality, 0.01)

            if centrality_drop_pct > CONNECTIVITY_CRITICAL_DROP:
                impact_level = "critical"
                warning = f"Closing would drop hive centrality by {centrality_drop_pct:.0%}"
            elif centrality_drop_pct > CONNECTIVITY_WARNING_DROP:
                impact_level = "warning"
                warning = f"Closing would reduce hive centrality by {centrality_drop_pct:.0%}"
            else:
                impact_level = "low"
                warning = None

        return {
            "impact_level": impact_level,
            "current_centrality": current_centrality,
            "projected_centrality": projected_centrality,
            "warning": warning
        }

    def _should_recommend_close(
        self,
        coverage: PeerCoverage,
        member_id: str
    ) -> Tuple[bool, str, float, str, Dict]:
        """
        Determine if we should recommend closing member's channel to this peer.

        Considers both routing performance and connectivity impact.

        Returns:
            (should_close, reason, confidence, urgency, connectivity_impact)
        """
        empty_impact = {"impact_level": "none"}

        # Skip if this member is the owner
        if member_id == coverage.owner_member:
            return False, "", 0.0, "none", empty_impact

        # Skip if no clear owner
        if not coverage.owner_member:
            return False, "no_clear_owner", 0.0, "none", empty_impact

        # Check cooldown
        cooldown_key = f"{member_id}:{coverage.peer_id}"
        last_rec = self._recent_recommendations.get(cooldown_key, 0)
        if time.time() - last_rec < CLOSE_RECOMMENDATION_COOLDOWN_HOURS * 3600:
            return False, "cooldown", 0.0, "none", empty_impact

        # Get member's marker strength
        member_strength = coverage.member_marker_strength.get(member_id, 0)
        owner_strength = coverage.member_marker_strength.get(coverage.owner_member, 0)

        # Calculate performance ratio
        if owner_strength > 0:
            performance_ratio = member_strength / owner_strength
        else:
            performance_ratio = 0.0

        # Check if underperformer
        if performance_ratio < UNDERPERFORMER_MARKER_RATIO:
            # Member has <10% of owner's routing activity

            # Assess connectivity impact before recommending close
            connectivity_impact = self._assess_connectivity_impact(member_id, coverage.peer_id)
            impact_level = connectivity_impact.get("impact_level", "none")

            # Block critical connectivity impacts
            if impact_level == "critical":
                self._log(
                    f"Blocked close recommendation for {member_id[:16]}... -> {coverage.peer_id[:16]}...: "
                    f"critical connectivity impact",
                    level="info"
                )
                return False, "critical_connectivity_impact", 0.0, "none", connectivity_impact

            # Determine urgency based on redundancy level and performance
            if coverage.redundancy_count > 3 and performance_ratio < 0.05:
                urgency = "high"
                confidence = min(0.9, coverage.ownership_confidence)
            elif coverage.is_over_redundant:
                urgency = "medium"
                confidence = min(0.8, coverage.ownership_confidence * 0.9)
            else:
                urgency = "low"
                confidence = min(0.7, coverage.ownership_confidence * 0.8)

            # Reduce confidence for warning-level connectivity impacts
            if impact_level == "warning":
                confidence = confidence * 0.7  # Reduce confidence by 30%
                urgency = "low" if urgency != "high" else "medium"  # Reduce urgency

            reason = (
                f"Underperforming: {performance_ratio:.1%} of owner's routing activity; "
                f"{coverage.redundancy_count} members serve this peer"
            )

            return True, reason, confidence, urgency, connectivity_impact

        return False, "", 0.0, "none", empty_impact

    def generate_close_recommendations(self) -> List[CloseRecommendation]:
        """
        Generate close recommendations for underperforming redundant channels.

        Returns:
            List of CloseRecommendation
        """
        recommendations = []

        # Get all redundant peer coverage
        redundant_peers = self.redundancy_analyzer.get_redundant_peers()

        for coverage in redundant_peers:
            # Skip if no clear owner
            if not coverage.owner_member:
                continue

            # Check each non-owner member
            for member_id in coverage.members_with_channels:
                should_close, reason, confidence, urgency, connectivity_impact = self._should_recommend_close(
                    coverage, member_id
                )

                if not should_close:
                    continue

                # Get channel info
                channel_info = self._get_channel_info(member_id, coverage.peer_id)

                # Create recommendation with connectivity impact
                rec = CloseRecommendation(
                    member_id=member_id,
                    peer_id=coverage.peer_id,
                    channel_id=channel_info.get("channel_id", "unknown") if channel_info else "unknown",
                    peer_alias=coverage.peer_alias,
                    member_marker_strength=coverage.member_marker_strength.get(member_id, 0),
                    owner_marker_strength=coverage.member_marker_strength.get(coverage.owner_member, 0),
                    owner_member=coverage.owner_member,
                    capacity_sats=channel_info.get("capacity_sats", 0) if channel_info else 0,
                    local_balance_pct=(
                        channel_info.get("local_balance_sats", 0) /
                        channel_info.get("capacity_sats", 1)
                        if channel_info and channel_info.get("capacity_sats", 0) > 0
                        else 0
                    ),
                    reason=reason,
                    confidence=confidence,
                    urgency=urgency,
                    freed_capital_sats=channel_info.get("capacity_sats", 0) if channel_info else 0,
                    # Connectivity impact (Use Case 3)
                    connectivity_impact=connectivity_impact.get("impact_level", "none"),
                    current_hive_centrality=connectivity_impact.get("current_centrality", 0.0),
                    projected_hive_centrality=connectivity_impact.get("projected_centrality", 0.0),
                    connectivity_warning=connectivity_impact.get("warning")
                )

                recommendations.append(rec)

                # Record recommendation time
                cooldown_key = f"{member_id}:{coverage.peer_id}"
                self._recent_recommendations[cooldown_key] = time.time()

        # Sort by urgency then confidence
        urgency_order = {"high": 0, "medium": 1, "low": 2}
        recommendations.sort(
            key=lambda r: (urgency_order.get(r.urgency, 3), -r.confidence)
        )

        return recommendations

    def get_my_close_recommendations(self) -> List[CloseRecommendation]:
        """
        Get close recommendations specifically for our node.

        Returns:
            List of recommendations where we should close channels
        """
        all_recs = self.generate_close_recommendations()
        return [r for r in all_recs if r.member_id == self._our_pubkey]

    def create_pending_actions(self, recommendations: List[CloseRecommendation]) -> int:
        """
        Create pending_actions for close recommendations.

        Args:
            recommendations: List of close recommendations

        Returns:
            Number of pending_actions created
        """
        if not self.governance:
            self._log("Governance not available, cannot create pending_actions", level="warn")
            return 0

        created = 0

        for rec in recommendations:
            # Only create actions for high confidence recommendations
            if rec.confidence < 0.5:
                continue

            try:
                action_data = {
                    "action_type": "close_channel",
                    "member_id": rec.member_id,
                    "peer_id": rec.peer_id,
                    "channel_id": rec.channel_id,
                    "reason": rec.reason,
                    "urgency": rec.urgency,
                    "confidence": rec.confidence,
                    "freed_capital_sats": rec.freed_capital_sats,
                    "owner_member": rec.owner_member,
                    "recommendation_type": "rationalization"
                }

                self.governance.create_pending_action(
                    action_type="close_recommendation",
                    data=action_data,
                    source="channel_rationalization"
                )
                created += 1

                self._log(
                    f"Created pending action: close {rec.channel_id} "
                    f"({rec.member_id[:8]}... → {rec.peer_id[:8]}...)",
                    level="info"
                )

            except Exception as e:
                self._log(f"Error creating pending action: {e}", level="warn")

        return created

    def get_rationalization_summary(self) -> RationalizationSummary:
        """
        Get summary of channel rationalization analysis.

        Returns:
            RationalizationSummary with coverage health and recommendations
        """
        summary = RationalizationSummary()

        # Analyze all coverage
        all_coverage = self.redundancy_analyzer.analyze_all_coverage()

        summary.total_peers_analyzed = len(all_coverage)

        for coverage in all_coverage.values():
            if coverage.redundancy_count >= REDUNDANCY_MIN_MEMBERS:
                summary.redundant_peers += 1

            if coverage.is_over_redundant:
                summary.over_redundant_peers += 1

            # Categorize coverage health
            total_strength = sum(coverage.member_marker_strength.values())

            if coverage.owner_member and coverage.ownership_confidence >= 0.6:
                summary.well_owned_peers += 1
            elif total_strength > 0 and not coverage.owner_member:
                summary.contested_peers += 1
            elif total_strength == 0 and coverage.redundancy_count > 0:
                summary.orphan_peers += 1

        # Get recommendations
        recommendations = self.generate_close_recommendations()
        summary.close_recommendations = len(recommendations)

        # Calculate potential freed capital
        summary.potential_freed_capital_sats = sum(
            r.freed_capital_sats for r in recommendations
        )

        # Top 5 recommendations
        summary.top_recommendations = [
            r.to_dict() for r in recommendations[:5]
        ]

        return summary


# =============================================================================
# RATIONALIZATION MANAGER
# =============================================================================

class RationalizationManager:
    """
    Main interface for channel rationalization.

    Coordinates redundancy analysis, ownership determination,
    and close recommendations.
    """

    def __init__(
        self,
        plugin,
        database=None,
        state_manager=None,
        fee_coordination_mgr=None,
        governance=None
    ):
        """
        Initialize the rationalization manager.

        Args:
            plugin: Plugin reference
            database: Database for persistence
            state_manager: StateManager for fleet state
            fee_coordination_mgr: FeeCoordinationManager for markers
            governance: Governance module for pending_actions
        """
        self.plugin = plugin
        self.database = database

        # Initialize rationalizer
        self.rationalizer = ChannelRationalizer(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            fee_coordination_mgr=fee_coordination_mgr,
            governance=governance
        )

        self._our_pubkey: Optional[str] = None
        self._remote_coverage: Dict[str, List[Dict[str, Any]]] = {}
        self._remote_close_proposals: List[Dict[str, Any]] = []

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey."""
        self._our_pubkey = pubkey
        self.rationalizer.set_our_pubkey(pubkey)

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"RATIONALIZATION_MGR: {message}", level=level)

    def analyze_coverage(self, peer_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze fleet coverage for a peer or all redundant peers.

        Args:
            peer_id: Specific peer to analyze, or None for all

        Returns:
            Coverage analysis results
        """
        if peer_id:
            coverage = self.rationalizer.redundancy_analyzer.analyze_peer_coverage(peer_id)
            return {
                "peer_id": peer_id,
                "coverage": coverage.to_dict()
            }
        else:
            redundant = self.rationalizer.redundancy_analyzer.get_redundant_peers()
            return {
                "redundant_peers": len(redundant),
                "peers": [c.to_dict() for c in redundant]
            }

    def get_close_recommendations(
        self,
        for_our_node_only: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get channel close recommendations.

        Args:
            for_our_node_only: If True, only return recommendations for our node

        Returns:
            List of close recommendations
        """
        if for_our_node_only:
            recs = self.rationalizer.get_my_close_recommendations()
        else:
            recs = self.rationalizer.generate_close_recommendations()

        return [r.to_dict() for r in recs]

    def create_close_actions(self) -> Dict[str, Any]:
        """
        Create pending_actions for close recommendations.

        Returns:
            Dict with creation results
        """
        recommendations = self.rationalizer.generate_close_recommendations()
        created = self.rationalizer.create_pending_actions(recommendations)

        return {
            "recommendations_analyzed": len(recommendations),
            "pending_actions_created": created
        }

    def get_summary(self) -> Dict[str, Any]:
        """
        Get rationalization summary.

        Returns:
            Summary dict
        """
        summary = self.rationalizer.get_rationalization_summary()
        return summary.to_dict()

    def get_status(self) -> Dict[str, Any]:
        """
        Get overall rationalization status.

        Returns:
            Status dict with health metrics
        """
        summary = self.rationalizer.get_rationalization_summary()

        return {
            "enabled": True,
            "summary": summary.to_dict(),
            "health": {
                "well_owned_ratio": (
                    summary.well_owned_peers / summary.total_peers_analyzed
                    if summary.total_peers_analyzed > 0 else 0
                ),
                "redundancy_ratio": (
                    summary.redundant_peers / summary.total_peers_analyzed
                    if summary.total_peers_analyzed > 0 else 0
                ),
                "orphan_ratio": (
                    summary.orphan_peers / summary.total_peers_analyzed
                    if summary.total_peers_analyzed > 0 else 0
                )
            },
            "thresholds": {
                "ownership_dominant_ratio": OWNERSHIP_DOMINANT_RATIO,
                "max_healthy_redundancy": MAX_HEALTHY_REDUNDANCY,
                "underperformer_marker_ratio": UNDERPERFORMER_MARKER_RATIO
            }
        }

    # =========================================================================
    # FLEET INTELLIGENCE SHARING (Phase 14.2)
    # =========================================================================

    def get_shareable_coverage_analysis(
        self,
        min_ownership_confidence: float = 0.5,
        max_entries: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Get peer coverage analysis suitable for sharing with fleet.

        Args:
            min_ownership_confidence: Minimum confidence to share ownership
            max_entries: Maximum number of entries

        Returns:
            List of coverage entry dicts ready for serialization
        """
        shareable = []

        try:
            all_coverage = self.rationalizer.redundancy_analyzer.analyze_all_coverage()

            for peer_id, coverage in all_coverage.items():
                # Only share if we have meaningful ownership data
                if coverage.ownership_confidence < min_ownership_confidence and not coverage.is_over_redundant:
                    continue

                shareable.append({
                    "peer_id": peer_id,
                    "peer_alias": coverage.peer_alias,
                    "members_with_channels": coverage.members_with_channels,
                    "member_marker_strength": {
                        k: round(v, 3) for k, v in coverage.member_marker_strength.items()
                    },
                    "owner_member": coverage.owner_member,
                    "ownership_confidence": round(coverage.ownership_confidence, 3),
                    "redundancy_count": coverage.redundancy_count,
                    "is_over_redundant": coverage.is_over_redundant
                })

        except Exception as e:
            self._log(f"Error collecting shareable coverage: {e}", level="debug")

        # Sort by redundancy (most redundant first - more important to coordinate)
        shareable.sort(key=lambda x: (-x["redundancy_count"], -x["ownership_confidence"]))

        return shareable[:max_entries]

    def get_shareable_close_recommendations(
        self,
        max_recommendations: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Get close recommendations suitable for sharing with fleet.

        Args:
            max_recommendations: Maximum number of recommendations

        Returns:
            List of close recommendation dicts
        """
        shareable = []

        try:
            recs = self.rationalizer.generate_close_recommendations()

            for r in recs:
                shareable.append({
                    "member_id": r.member_id,
                    "peer_id": r.peer_id,
                    "channel_id": r.channel_id,
                    "owner_id": r.owner_member,
                    "reason": r.reason,
                    "freed_capacity_sats": r.freed_capital_sats,
                    "member_marker_strength": round(r.member_marker_strength, 3),
                    "owner_marker_strength": round(r.owner_marker_strength, 3)
                })

        except Exception as e:
            self._log(f"Error collecting close recommendations: {e}", level="debug")

        return shareable[:max_recommendations]

    def receive_coverage_from_fleet(
        self,
        reporter_id: str,
        coverage_data: Dict[str, Any]
    ) -> bool:
        """
        Receive a coverage analysis from another fleet member.

        Args:
            reporter_id: The fleet member who reported this
            coverage_data: Dict with coverage details

        Returns:
            True if stored successfully
        """
        peer_id = coverage_data.get("peer_id")
        if not peer_id:
            return False

        # Initialize remote coverage storage
        if not hasattr(self, "_remote_coverage"):
            self._remote_coverage: Dict[str, List[Dict[str, Any]]] = {}

        entry = {
            "reporter_id": reporter_id,
            "members_with_channels": coverage_data.get("members_with_channels", []),
            "owner_member": coverage_data.get("owner_member"),
            "ownership_confidence": coverage_data.get("ownership_confidence", 0),
            "redundancy_count": coverage_data.get("redundancy_count", 0),
            "is_over_redundant": coverage_data.get("is_over_redundant", False),
            "timestamp": time.time()
        }

        if peer_id not in self._remote_coverage:
            self._remote_coverage[peer_id] = []

        self._remote_coverage[peer_id].append(entry)

        # Keep only last 5 reports per peer
        if len(self._remote_coverage[peer_id]) > 5:
            self._remote_coverage[peer_id] = self._remote_coverage[peer_id][-5:]

        return True

    def receive_close_proposal_from_fleet(
        self,
        reporter_id: str,
        proposal_data: Dict[str, Any]
    ) -> bool:
        """
        Receive a close proposal from another fleet member.

        Args:
            reporter_id: The fleet member who proposed this
            proposal_data: Dict with proposal details

        Returns:
            True if stored successfully
        """
        member_id = proposal_data.get("member_id")
        peer_id = proposal_data.get("peer_id")
        channel_id = proposal_data.get("channel_id")
        if not member_id or not peer_id or not channel_id:
            return False

        # Initialize remote proposals storage
        if not hasattr(self, "_remote_close_proposals"):
            self._remote_close_proposals: List[Dict[str, Any]] = []

        entry = {
            "reporter_id": reporter_id,
            "member_id": member_id,
            "peer_id": peer_id,
            "channel_id": channel_id,
            "owner_id": proposal_data.get("owner_id"),
            "reason": proposal_data.get("reason", ""),
            "freed_capacity_sats": proposal_data.get("freed_capacity_sats", 0),
            "timestamp": time.time()
        }

        self._remote_close_proposals.append(entry)

        # Keep only last 50 proposals
        if len(self._remote_close_proposals) > 50:
            self._remote_close_proposals = self._remote_close_proposals[-50:]

        return True

    def get_fleet_coverage_consensus(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get consensus coverage analysis from fleet reports.

        Args:
            peer_id: Peer to get consensus for

        Returns:
            Consensus coverage data or None
        """
        if not hasattr(self, "_remote_coverage"):
            return None

        reports = self._remote_coverage.get(peer_id, [])
        if not reports:
            return None

        now = time.time()
        recent = [r for r in reports if now - r.get("timestamp", 0) < 7 * 86400]
        if not recent:
            return None

        # Find consensus owner (most commonly reported)
        owner_counts: Dict[str, int] = {}
        for r in recent:
            owner = r.get("owner_member")
            if owner:
                owner_counts[owner] = owner_counts.get(owner, 0) + 1

        consensus_owner = max(owner_counts, key=owner_counts.get) if owner_counts else None

        # Average confidence
        avg_confidence = sum(r.get("ownership_confidence", 0) for r in recent) / len(recent)

        # Check for over-redundancy consensus
        over_redundant_count = sum(1 for r in recent if r.get("is_over_redundant"))

        return {
            "peer_id": peer_id,
            "consensus_owner": consensus_owner,
            "avg_ownership_confidence": round(avg_confidence, 3),
            "is_over_redundant": over_redundant_count > len(recent) // 2,
            "reporter_count": len(recent)
        }

    def get_pending_close_proposals_for_us(self) -> List[Dict[str, Any]]:
        """
        Get close proposals that target us (our node).

        Returns:
            List of close proposals where we are the recommended member to close
        """
        if not hasattr(self, "_remote_close_proposals"):
            return []

        our_proposals = []
        now = time.time()

        for p in self._remote_close_proposals:
            # Only recent proposals
            if now - p.get("timestamp", 0) > 7 * 86400:
                continue
            # Only proposals for us
            if p.get("member_id") == self._our_pubkey:
                our_proposals.append(p)

        return our_proposals

    def cleanup_old_remote_data(self, max_age_days: float = 7) -> int:
        """Remove old remote rationalization data."""
        cutoff = time.time() - (max_age_days * 86400)
        cleaned = 0

        # Cleanup coverage
        if hasattr(self, "_remote_coverage"):
            for peer_id in list(self._remote_coverage.keys()):
                before = len(self._remote_coverage[peer_id])
                self._remote_coverage[peer_id] = [
                    r for r in self._remote_coverage[peer_id]
                    if r.get("timestamp", 0) > cutoff
                ]
                cleaned += before - len(self._remote_coverage[peer_id])
                if not self._remote_coverage[peer_id]:
                    del self._remote_coverage[peer_id]

        # Cleanup close proposals
        if hasattr(self, "_remote_close_proposals"):
            before = len(self._remote_close_proposals)
            self._remote_close_proposals = [
                p for p in self._remote_close_proposals
                if p.get("timestamp", 0) > cutoff
            ]
            cleaned += before - len(self._remote_close_proposals)

        return cleaned
