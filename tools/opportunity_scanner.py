"""
Opportunity Scanner for Proactive AI Advisor

Scans all available data sources to identify optimization opportunities:
- Phase 7.1: Anticipatory Liquidity predictions
- Phase 7.2: Physarum channel lifecycle
- Phase 7.4: Time-based fee optimization
- Revenue-ops: Profitability analysis
- Velocity alerts: Critical depletion/saturation
- Planner analysis: Topology improvements

Usage:
    from opportunity_scanner import OpportunityScanner

    scanner = OpportunityScanner(mcp_client, db)
    opportunities = await scanner.scan_all(node_name, state)
"""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# Enums and Constants
# =============================================================================

class OpportunityType(Enum):
    """Types of optimization opportunities."""
    # Fee-related
    PEAK_HOUR_FEE = "peak_hour_fee"
    LOW_HOUR_FEE = "low_hour_fee"
    COMPETITOR_UNDERCUT = "competitor_undercut"
    BLEEDER_FIX = "bleeder_fix"
    STAGNANT_CHANNEL = "stagnant_channel"

    # Balance-related
    CRITICAL_DEPLETION = "critical_depletion"
    CRITICAL_SATURATION = "critical_saturation"
    PREEMPTIVE_REBALANCE = "preemptive_rebalance"
    IMBALANCED_CHANNEL = "imbalanced_channel"

    # Config-related
    CONFIG_TUNING = "config_tuning"
    POLICY_CHANGE = "policy_change"

    # Topology
    CHANNEL_OPEN = "channel_open"
    CHANNEL_CLOSE = "channel_close"
    UNDERSERVED_TARGET = "underserved_target"

    # New member onboarding
    NEW_MEMBER_CHANNEL = "new_member_channel"

    # Routing intelligence (pheromones + stigmergic markers)
    PHEROMONE_FEE_ADJUST = "pheromone_fee_adjust"
    STIGMERGIC_COORDINATION = "stigmergic_coordination"
    ROUTING_INTELLIGENCE = "routing_intelligence"

    # Fleet consensus (Phase 14.2) - Multiple fleet members agree
    FLEET_CONSENSUS_FEE = "fleet_consensus_fee"          # Fleet agrees on fee direction
    FLEET_CONSENSUS_CLOSE = "fleet_consensus_close"      # Fleet agrees we should close
    FLEET_CONSENSUS_CORRIDOR = "fleet_consensus_corridor" # Fleet identifies valuable corridor
    FLEET_DEFENSIVE_ACTION = "fleet_defensive_action"    # Fleet warns about peer


class ActionType(Enum):
    """Types of actions the advisor can take."""
    FEE_CHANGE = "fee_change"
    REBALANCE = "rebalance"
    CONFIG_CHANGE = "config_change"
    POLICY_CHANGE = "policy_change"
    CHANNEL_OPEN = "channel_open"
    CHANNEL_CLOSE = "channel_close"
    FLAG_FOR_REVIEW = "flag_for_review"


class ActionClassification(Enum):
    """How an action should be handled."""
    AUTO_EXECUTE = "auto_execute"      # Safe to execute automatically
    QUEUE_FOR_REVIEW = "queue_review"  # Queue for human review
    REQUIRE_APPROVAL = "require_approval"  # Must be explicitly approved


# Safety constraints
SAFETY_CONSTRAINTS = {
    # Channel operations ALWAYS require approval
    "channel_open_always_approve": True,
    "channel_close_always_approve": True,

    # Fee bounds
    "absolute_min_fee_ppm": 25,
    "absolute_max_fee_ppm": 5000,
    "max_fee_change_pct_per_cycle": 25,  # Max 25% change per 3h cycle

    # Rebalancing bounds
    "max_rebalance_cost_pct": 2.0,      # Never pay >2% for rebalance
    "max_single_rebalance_sats": 500_000,  # 500k sat cap per operation
    "max_daily_rebalance_spend_sats": 50_000,  # 50k sat daily fee cap

    # On-chain reserve
    "min_onchain_sats": 600_000,        # Always keep 600k on-chain

    # Rate limits per cycle
    "fee_changes_per_cycle": 10,         # Max 10 fee changes per 3h
    "rebalances_per_cycle": 5,           # Max 5 rebalances per 3h

    # Confidence requirements
    "min_confidence_auto_execute": 0.8,
    "min_confidence_for_queue": 0.5,
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class Opportunity:
    """A detected optimization opportunity."""
    opportunity_type: OpportunityType
    action_type: ActionType
    channel_id: Optional[str]
    peer_id: Optional[str]
    node_name: str

    # Scoring
    priority_score: float  # 0-1, higher = more important
    confidence_score: float  # 0-1, higher = more certain
    roi_estimate: float  # Expected return on investment

    # Details
    description: str
    reasoning: str
    recommended_action: str
    predicted_benefit: int  # Estimated benefit in sats

    # Classification
    classification: ActionClassification
    auto_execute_safe: bool

    # Context
    current_state: Dict[str, Any] = field(default_factory=dict)
    detected_at: int = 0

    # Final adjusted scores (set by learning engine)
    final_score: float = 0.0
    adjusted_confidence: float = 0.0
    goal_alignment_bonus: float = 0.0

    def __post_init__(self):
        if self.detected_at == 0:
            self.detected_at = int(time.time())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "opportunity_type": self.opportunity_type.value,
            "action_type": self.action_type.value,
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "node_name": self.node_name,
            "priority_score": round(self.priority_score, 4),
            "confidence_score": round(self.confidence_score, 4),
            "roi_estimate": round(self.roi_estimate, 4),
            "description": self.description,
            "reasoning": self.reasoning,
            "recommended_action": self.recommended_action,
            "predicted_benefit": self.predicted_benefit,
            "classification": self.classification.value,
            "auto_execute_safe": self.auto_execute_safe,
            "final_score": round(self.final_score, 4),
            "adjusted_confidence": round(self.adjusted_confidence, 4),
            "goal_alignment_bonus": round(self.goal_alignment_bonus, 4),
            "detected_at": self.detected_at
        }


# =============================================================================
# Opportunity Scanner
# =============================================================================

class OpportunityScanner:
    """
    Scans all data sources for optimization opportunities.

    Integrates with:
    - Phase 7.1: Anticipatory liquidity predictions
    - Phase 7.2: Physarum channel lifecycle
    - Phase 7.4: Time-based fee optimization
    - Revenue-ops: Profitability analysis
    - Advisor DB: Velocity tracking
    """

    def __init__(self, mcp_client, db):
        """
        Initialize opportunity scanner.

        Args:
            mcp_client: Client for calling MCP tools
            db: AdvisorDB instance
        """
        self.mcp = mcp_client
        self.db = db

    async def scan_all(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """
        Scan all data sources and return prioritized opportunities.

        Integrates with full intelligence suite:
        - Core: velocity, profitability, time-based fees, imbalanced channels
        - Fleet coordination: defense warnings, internal competition
        - Cost reduction: circular flows, rebalance recommendations
        - Strategic: positioning, competitor analysis, rationalization
        - Collective warnings: ban candidates

        Args:
            node_name: Node to scan
            state: Current node state from analyze_node_state()

        Returns:
            List of Opportunity objects, sorted by priority
        """
        opportunities = []

        # Scan each data source in parallel
        results = await asyncio.gather(
            # Core scanners
            self._scan_velocity_alerts(node_name, state),
            self._scan_profitability(node_name, state),
            self._scan_time_based_fees(node_name, state),
            self._scan_anticipatory_liquidity(node_name, state),
            self._scan_imbalanced_channels(node_name, state),
            self._scan_config_opportunities(node_name, state),
            # Fleet coordination scanners (Phase 2)
            self._scan_defense_warnings(node_name, state),
            self._scan_internal_competition(node_name, state),
            # Cost reduction scanners (Phase 3)
            self._scan_circular_flows(node_name, state),
            self._scan_rebalance_recommendations(node_name, state),
            # Strategic positioning scanners (Phase 4)
            self._scan_positioning_opportunities(node_name, state),
            self._scan_competitor_opportunities(node_name, state),
            self._scan_rationalization(node_name, state),
            # Collective warning scanners
            self._scan_ban_candidates(node_name, state),
            # New member onboarding scanner
            self._scan_new_member_opportunities(node_name, state),
            # Routing intelligence scanner (pheromones + stigmergic markers)
            self._scan_routing_intelligence(node_name, state),
            # Fleet consensus scanner (Phase 14.2 - shared intelligence)
            self._scan_fleet_consensus(node_name, state),
            return_exceptions=True
        )

        # Collect all opportunities
        for result in results:
            if isinstance(result, Exception):
                # Log but don't fail
                continue
            if result:
                opportunities.extend(result)

        # Sort by priority
        opportunities.sort(key=lambda x: x.priority_score, reverse=True)

        return opportunities

    async def _scan_velocity_alerts(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for critical velocity (depletion/saturation) issues."""
        opportunities = []

        velocities = state.get("velocities", {})
        critical_channels = velocities.get("channels", [])

        for ch in critical_channels:
            channel_id = ch.get("channel_id")
            trend = ch.get("trend")
            hours_until = ch.get("hours_until_depleted") or ch.get("hours_until_full")
            urgency = ch.get("urgency", "low")

            if not hours_until or hours_until > 48:
                continue

            # Critical depletion
            if trend == "depleting" and hours_until < 24:
                priority = 0.95 if hours_until < 6 else 0.85 if hours_until < 12 else 0.7
                opp = Opportunity(
                    opportunity_type=OpportunityType.CRITICAL_DEPLETION,
                    action_type=ActionType.REBALANCE,
                    channel_id=channel_id,
                    peer_id=None,
                    node_name=node_name,
                    priority_score=priority,
                    confidence_score=ch.get("confidence", 0.7),
                    roi_estimate=0.8,  # High ROI - prevents lost routing
                    description=f"Channel {channel_id} depleting in {hours_until:.0f}h",
                    reasoning=f"Velocity {ch.get('velocity_pct_per_hour', 0):.2f}%/h, "
                              f"current balance {ch.get('current_balance_ratio', 0):.0%}",
                    recommended_action="Rebalance to restore outbound liquidity",
                    predicted_benefit=5000,  # Estimated routing saved
                    classification=ActionClassification.AUTO_EXECUTE if hours_until < 12
                                   else ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=hours_until < 12 and priority > 0.8,
                    current_state=ch
                )
                opportunities.append(opp)

            # Critical saturation
            elif trend == "filling" and hours_until < 24:
                priority = 0.9 if hours_until < 6 else 0.75 if hours_until < 12 else 0.6
                opp = Opportunity(
                    opportunity_type=OpportunityType.CRITICAL_SATURATION,
                    action_type=ActionType.FEE_CHANGE,
                    channel_id=channel_id,
                    peer_id=None,
                    node_name=node_name,
                    priority_score=priority,
                    confidence_score=ch.get("confidence", 0.7),
                    roi_estimate=0.6,
                    description=f"Channel {channel_id} saturating in {hours_until:.0f}h",
                    reasoning=f"Inbound velocity {abs(ch.get('velocity_pct_per_hour', 0)):.2f}%/h",
                    recommended_action="Reduce fees to encourage outbound flow",
                    predicted_benefit=2000,
                    classification=ActionClassification.AUTO_EXECUTE if hours_until < 12
                                   else ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=hours_until < 12,
                    current_state=ch
                )
                opportunities.append(opp)

        return opportunities

    async def _scan_profitability(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for profitability-related opportunities."""
        opportunities = []

        # Extract channel list from profitability dict structure (Issue #45)
        prof_data = state.get("profitability", {})
        if isinstance(prof_data, dict):
            channels_by_class = prof_data.get("channels_by_class", {})
            prof_list = []
            for channels in channels_by_class.values():
                if isinstance(channels, list):
                    prof_list.extend(channels)
        else:
            prof_list = prof_data  # Legacy list format

        # Use channels data for balance info (prof_list may not have it)
        channels = state.get("channels", [])
        channel_map = {ch.get("channel_id") or ch.get("scid"): ch for ch in channels}

        dashboard = state.get("dashboard", {})
        bleeders = dashboard.get("bleeder_warnings", [])

        # Bleeder channels need attention
        for bleeder in bleeders:
            channel_id = bleeder.get("channel_id") or bleeder.get("scid")
            if not channel_id:
                continue

            opp = Opportunity(
                opportunity_type=OpportunityType.BLEEDER_FIX,
                action_type=ActionType.POLICY_CHANGE,
                channel_id=channel_id,
                peer_id=bleeder.get("peer_id"),
                node_name=node_name,
                priority_score=0.85,
                confidence_score=0.8,
                roi_estimate=0.9,  # High ROI - stops bleeding
                description=f"Bleeder channel {channel_id} losing money",
                reasoning=f"Consistently negative ROI, needs intervention",
                recommended_action="Apply static fee policy or flag for closure review",
                predicted_benefit=bleeder.get("estimated_loss_sats", 1000),
                classification=ActionClassification.QUEUE_FOR_REVIEW,
                auto_execute_safe=False,
                current_state=bleeder
            )
            opportunities.append(opp)

        # Stagnant channels (100% local, no flow)
        # Use channels data for balance info, merge with profitability
        for ch in channels:
            channel_id = ch.get("channel_id") or ch.get("scid")
            balance_ratio = ch.get("balance_ratio", 0)
            forward_count = ch.get("forward_count", 0)

            if balance_ratio > 0.95 and forward_count == 0:
                # Merge with profitability data if available
                current_state = dict(ch)
                if channel_id in channel_map:
                    current_state.update(channel_map.get(channel_id, {}))

                opp = Opportunity(
                    opportunity_type=OpportunityType.STAGNANT_CHANNEL,
                    action_type=ActionType.FEE_CHANGE,
                    channel_id=channel_id,
                    peer_id=ch.get("peer_id"),
                    node_name=node_name,
                    priority_score=0.6,
                    confidence_score=0.82,  # Raised for auto-execute (Issue #45)
                    roi_estimate=0.5,
                    description=f"Stagnant channel {channel_id} - 100% local, no flow",
                    reasoning="Channel is fully local with no routing activity",
                    recommended_action="Lower fees to attract outbound flow",
                    predicted_benefit=500,
                    classification=ActionClassification.AUTO_EXECUTE,
                    auto_execute_safe=True,
                    current_state=current_state
                )
                opportunities.append(opp)

        return opportunities

    async def _scan_time_based_fees(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for time-based fee optimization opportunities (Phase 7.4)."""
        opportunities = []

        # Get current hour and check for patterns
        current_hour = datetime.now().hour

        channels = state.get("channels", [])

        for ch in channels:
            channel_id = ch.get("short_channel_id") or ch.get("channel_id")
            if not channel_id:
                continue

            # Get channel history to detect patterns
            history = self.db.get_channel_history(node_name, channel_id, hours=168)  # 1 week

            if len(history) < 24:  # Need at least 24 data points
                continue

            # Simple pattern detection - look for consistent flow at certain hours
            hour_flows = {}
            for h in history:
                ts = h.get("timestamp", 0)
                if ts:
                    hour = datetime.fromtimestamp(ts).hour
                    if hour not in hour_flows:
                        hour_flows[hour] = []
                    hour_flows[hour].append(h.get("forward_count", 0))

            # Check if current hour is typically high or low activity
            if current_hour in hour_flows and len(hour_flows[current_hour]) >= 3:
                avg_current = sum(hour_flows[current_hour]) / len(hour_flows[current_hour])
                all_averages = [sum(v) / len(v) for v in hour_flows.values() if len(v) >= 3]

                if all_averages:
                    overall_avg = sum(all_averages) / len(all_averages)

                    # Peak hour: higher than average
                    if avg_current > overall_avg * 1.3:
                        opp = Opportunity(
                            opportunity_type=OpportunityType.PEAK_HOUR_FEE,
                            action_type=ActionType.FEE_CHANGE,
                            channel_id=channel_id,
                            peer_id=ch.get("peer_id"),
                            node_name=node_name,
                            priority_score=0.65,
                            confidence_score=min(0.9, len(hour_flows[current_hour]) / 10),
                            roi_estimate=0.7,
                            description=f"Peak hour detected for channel {channel_id}",
                            reasoning=f"Hour {current_hour} activity {avg_current:.0f} vs avg {overall_avg:.0f}",
                            recommended_action="Temporarily increase fees (+15-25%)",
                            predicted_benefit=int(avg_current * 0.2),
                            classification=ActionClassification.AUTO_EXECUTE,
                            auto_execute_safe=True,
                            current_state={"hour": current_hour, "avg_flow": avg_current}
                        )
                        opportunities.append(opp)

                    # Low hour: lower than average
                    elif avg_current < overall_avg * 0.5:
                        opp = Opportunity(
                            opportunity_type=OpportunityType.LOW_HOUR_FEE,
                            action_type=ActionType.FEE_CHANGE,
                            channel_id=channel_id,
                            peer_id=ch.get("peer_id"),
                            node_name=node_name,
                            priority_score=0.5,
                            confidence_score=min(0.85, len(hour_flows[current_hour]) / 10),
                            roi_estimate=0.4,
                            description=f"Low activity hour for channel {channel_id}",
                            reasoning=f"Hour {current_hour} activity {avg_current:.0f} vs avg {overall_avg:.0f}",
                            recommended_action="Temporarily decrease fees (-10-15%) to attract flow",
                            predicted_benefit=int(overall_avg * 0.1),
                            classification=ActionClassification.AUTO_EXECUTE,
                            auto_execute_safe=True,
                            current_state={"hour": current_hour, "avg_flow": avg_current}
                        )
                        opportunities.append(opp)

        return opportunities

    async def _scan_anticipatory_liquidity(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for anticipatory liquidity opportunities (Phase 7.1)."""
        opportunities = []

        # Get predictions from context if available
        context = state.get("context", {})
        predictions = context.get("liquidity_predictions", [])

        for pred in predictions:
            channel_id = pred.get("channel_id")
            hours_ahead = pred.get("hours_ahead", 24)
            depletion_risk = pred.get("depletion_risk", 0)
            saturation_risk = pred.get("saturation_risk", 0)
            recommended_action = pred.get("recommended_action", "monitor")

            if recommended_action == "preemptive_rebalance" and depletion_risk > 0.5:
                opp = Opportunity(
                    opportunity_type=OpportunityType.PREEMPTIVE_REBALANCE,
                    action_type=ActionType.REBALANCE,
                    channel_id=channel_id,
                    peer_id=pred.get("peer_id"),
                    node_name=node_name,
                    priority_score=0.6 + depletion_risk * 0.3,
                    confidence_score=pred.get("confidence", 0.6),
                    roi_estimate=0.65,
                    description=f"Preemptive rebalance recommended for {channel_id}",
                    reasoning=f"Predicted depletion in {hours_ahead}h with {depletion_risk:.0%} risk",
                    recommended_action="Rebalance before predicted depletion",
                    predicted_benefit=3000,
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state=pred
                )
                opportunities.append(opp)

        return opportunities

    async def _scan_imbalanced_channels(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for imbalanced channels needing attention."""
        opportunities = []

        channels = state.get("channels", [])

        for ch in channels:
            channel_id = ch.get("short_channel_id") or ch.get("channel_id")
            if not channel_id:
                continue

            local_msat = ch.get("to_us_msat", 0)
            if isinstance(local_msat, str):
                local_msat = int(local_msat.replace("msat", ""))
            capacity_msat = ch.get("total_msat", 0)
            if isinstance(capacity_msat, str):
                capacity_msat = int(capacity_msat.replace("msat", ""))

            if capacity_msat == 0:
                continue

            balance_ratio = local_msat / capacity_msat

            # Very imbalanced (< 15% or > 85%)
            if balance_ratio < 0.15 or balance_ratio > 0.85:
                direction = "depleted" if balance_ratio < 0.15 else "saturated"
                opp = Opportunity(
                    opportunity_type=OpportunityType.IMBALANCED_CHANNEL,
                    action_type=ActionType.REBALANCE if balance_ratio < 0.3 else ActionType.FEE_CHANGE,
                    channel_id=channel_id,
                    peer_id=ch.get("peer_id"),
                    node_name=node_name,
                    priority_score=0.55 if 0.15 <= balance_ratio <= 0.85 else 0.7,
                    confidence_score=0.85,
                    roi_estimate=0.5,
                    description=f"Channel {channel_id} is {direction} ({balance_ratio:.0%} local)",
                    reasoning=f"Balance {balance_ratio:.0%} is far from ideal 50%",
                    recommended_action="Rebalance" if balance_ratio < 0.3 else "Adjust fees to attract outflow",
                    predicted_benefit=1500,
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state={"balance_ratio": balance_ratio}
                )
                opportunities.append(opp)

        return opportunities

    async def _scan_config_opportunities(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for configuration tuning opportunities."""
        opportunities = []

        summary = state.get("summary", {})
        dashboard = state.get("dashboard", {})

        # If many underwater channels, suggest config changes
        underwater_pct = summary.get("underwater_pct", 0)
        if underwater_pct > 40:
            opp = Opportunity(
                opportunity_type=OpportunityType.CONFIG_TUNING,
                action_type=ActionType.CONFIG_CHANGE,
                channel_id=None,
                peer_id=None,
                node_name=node_name,
                priority_score=0.7,
                confidence_score=0.75,
                roi_estimate=0.6,
                description=f"High underwater channel rate ({underwater_pct:.0f}%)",
                reasoning="Many channels unprofitable - review fee controller settings",
                recommended_action="Consider increasing hill_climbing_aggression or min_fee_ppm",
                predicted_benefit=5000,
                classification=ActionClassification.QUEUE_FOR_REVIEW,
                auto_execute_safe=False,
                current_state={"underwater_pct": underwater_pct}
            )
            opportunities.append(opp)

        # If ROC is very low, suggest config review
        roc_pct = summary.get("roc_pct", 0)
        if roc_pct < 0.1:
            opp = Opportunity(
                opportunity_type=OpportunityType.CONFIG_TUNING,
                action_type=ActionType.CONFIG_CHANGE,
                channel_id=None,
                peer_id=None,
                node_name=node_name,
                priority_score=0.65,
                confidence_score=0.7,
                roi_estimate=0.7,
                description=f"Very low ROC ({roc_pct:.2f}%)",
                reasoning="Return on capital below sustainable threshold",
                recommended_action="Review overall fee strategy and channel selection",
                predicted_benefit=10000,
                classification=ActionClassification.QUEUE_FOR_REVIEW,
                auto_execute_safe=False,
                current_state={"roc_pct": roc_pct}
            )
            opportunities.append(opp)

        return opportunities

    def classify_opportunity(self, opp: Opportunity) -> ActionClassification:
        """
        Classify an opportunity for appropriate handling.

        Args:
            opp: Opportunity to classify

        Returns:
            ActionClassification indicating how to handle
        """
        # Channel operations always require approval
        if opp.action_type in [ActionType.CHANNEL_OPEN, ActionType.CHANNEL_CLOSE]:
            return ActionClassification.REQUIRE_APPROVAL

        # High confidence + safe action type = auto execute
        if (opp.confidence_score >= SAFETY_CONSTRAINTS["min_confidence_auto_execute"]
            and opp.auto_execute_safe):
            return ActionClassification.AUTO_EXECUTE

        # Medium confidence = queue for review
        if opp.confidence_score >= SAFETY_CONSTRAINTS["min_confidence_for_queue"]:
            return ActionClassification.QUEUE_FOR_REVIEW

        # Low confidence = require explicit approval
        return ActionClassification.REQUIRE_APPROVAL

    async def _scan_defense_warnings(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan mycelium defense system for peer warnings."""
        opportunities = []

        defense = state.get("defense_status", {})
        warnings = defense.get("warnings", [])

        for warning in warnings:
            peer_id = warning.get("peer_id")
            warning_type = warning.get("type", "unknown")
            severity = warning.get("severity", "low")

            if not peer_id:
                continue

            # High severity warnings should trigger action
            if severity in ["high", "critical"]:
                priority = 0.9 if severity == "critical" else 0.75
                opp = Opportunity(
                    opportunity_type=OpportunityType.POLICY_CHANGE,
                    action_type=ActionType.POLICY_CHANGE,
                    channel_id=None,
                    peer_id=peer_id,
                    node_name=node_name,
                    priority_score=priority,
                    confidence_score=0.85,
                    roi_estimate=0.8,
                    description=f"Defense warning: {warning_type} for peer {peer_id[:16]}...",
                    reasoning=f"Mycelium defense flagged peer with {severity} severity: {warning.get('reason', 'N/A')}",
                    recommended_action="Apply defensive fee policy or consider channel closure",
                    predicted_benefit=5000,
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state=warning
                )
                opportunities.append(opp)

        return opportunities

    async def _scan_ban_candidates(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for peers that should be considered for ban proposals."""
        opportunities = []

        ban_data = state.get("ban_candidates", {})
        candidates = ban_data.get("candidates", [])

        for candidate in candidates:
            peer_id = candidate.get("peer_id")
            warning_count = candidate.get("warning_count", 0)
            severity = candidate.get("severity", "low")
            reasons = candidate.get("reasons", [])

            if not peer_id or warning_count < 2:
                continue

            priority = 0.95 if severity == "critical" else 0.85 if severity == "high" else 0.7
            opp = Opportunity(
                opportunity_type=OpportunityType.CHANNEL_CLOSE,
                action_type=ActionType.FLAG_FOR_REVIEW,
                channel_id=None,
                peer_id=peer_id,
                node_name=node_name,
                priority_score=priority,
                confidence_score=min(0.95, 0.5 + warning_count * 0.1),
                roi_estimate=0.9,
                description=f"Ban candidate: {peer_id[:16]}... ({warning_count} warnings)",
                reasoning=f"Collective warning system flagged peer: {', '.join(reasons[:3])}",
                recommended_action="Propose ban or close channel to this peer",
                predicted_benefit=10000,
                classification=ActionClassification.REQUIRE_APPROVAL,
                auto_execute_safe=False,
                current_state=candidate
            )
            opportunities.append(opp)

        return opportunities

    async def _scan_circular_flows(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for wasteful circular flow patterns (Phase 3)."""
        opportunities = []

        circular = state.get("circular_flows", {})
        detected_flows = circular.get("detected_flows", [])

        for flow in detected_flows:
            flow_id = flow.get("flow_id", "unknown")
            cost_sats = flow.get("cost_sats", 0)
            frequency = flow.get("frequency", 0)

            if cost_sats < 100:  # Ignore tiny flows
                continue

            priority = min(0.9, 0.5 + cost_sats / 10000)
            opp = Opportunity(
                opportunity_type=OpportunityType.CONFIG_TUNING,
                action_type=ActionType.FEE_CHANGE,
                channel_id=flow.get("entry_channel"),
                peer_id=None,
                node_name=node_name,
                priority_score=priority,
                confidence_score=0.8,
                roi_estimate=cost_sats / 1000,
                description=f"Circular flow detected: {cost_sats} sats wasted",
                reasoning=f"Flow pattern {flow_id} burns fees without net movement",
                recommended_action="Adjust fees to break circular pattern",
                predicted_benefit=cost_sats,
                classification=ActionClassification.QUEUE_FOR_REVIEW,
                auto_execute_safe=False,
                current_state=flow
            )
            opportunities.append(opp)

        return opportunities

    async def _scan_rationalization(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for redundant channels that should be closed."""
        opportunities = []

        close_recs = state.get("close_recommendations", {})
        recommendations = close_recs.get("recommendations", [])

        for rec in recommendations:
            channel_id = rec.get("channel_id")
            peer_id = rec.get("peer_id")
            reason = rec.get("reason", "redundant")
            our_activity_pct = rec.get("our_activity_pct", 0)

            if not channel_id:
                continue

            # Only recommend if we have significantly less activity than owner
            if our_activity_pct > 20:  # We're actively using this channel
                continue

            opp = Opportunity(
                opportunity_type=OpportunityType.CHANNEL_CLOSE,
                action_type=ActionType.CHANNEL_CLOSE,
                channel_id=channel_id,
                peer_id=peer_id,
                node_name=node_name,
                priority_score=0.6,
                confidence_score=0.75,
                roi_estimate=0.5,
                description=f"Redundant channel {channel_id} - another member owns this route",
                reasoning=f"Our routing activity is {our_activity_pct:.0f}% of fleet owner. {reason}",
                recommended_action="Close channel to free capital for better uses",
                predicted_benefit=rec.get("capacity_sats", 0) // 100,
                classification=ActionClassification.REQUIRE_APPROVAL,
                auto_execute_safe=False,
                current_state=rec
            )
            opportunities.append(opp)

        return opportunities

    async def _scan_positioning_opportunities(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for strategic positioning opportunities (Phase 4)."""
        opportunities = []

        positioning = state.get("positioning", {})

        # High-value corridors we're not serving
        corridors = positioning.get("valuable_corridors", [])
        for corridor in corridors[:5]:  # Top 5 corridors
            if corridor.get("we_serve"):
                continue

            score = corridor.get("value_score", 0)
            if score < 0.3:
                continue

            opp = Opportunity(
                opportunity_type=OpportunityType.CHANNEL_OPEN,
                action_type=ActionType.CHANNEL_OPEN,
                channel_id=None,
                peer_id=corridor.get("target_peer"),
                node_name=node_name,
                priority_score=min(0.8, score),
                confidence_score=0.7,
                roi_estimate=score,
                description=f"High-value corridor opportunity: {corridor.get('description', 'N/A')}",
                reasoning=f"Value score {score:.2f} based on volume, margin, and competition",
                recommended_action=f"Open channel to {corridor.get('target_peer', 'target')[:16]}...",
                predicted_benefit=int(score * 50000),
                classification=ActionClassification.REQUIRE_APPROVAL,
                auto_execute_safe=False,
                current_state=corridor
            )
            opportunities.append(opp)

        # Exchange coverage gaps
        exchanges = positioning.get("exchange_gaps", [])
        for exchange in exchanges[:3]:  # Top 3 missing exchanges
            opp = Opportunity(
                opportunity_type=OpportunityType.CHANNEL_OPEN,
                action_type=ActionType.CHANNEL_OPEN,
                channel_id=None,
                peer_id=exchange.get("pubkey"),
                node_name=node_name,
                priority_score=0.75,
                confidence_score=0.8,
                roi_estimate=0.7,
                description=f"Missing exchange connection: {exchange.get('name', 'Unknown')}",
                reasoning="Priority exchanges provide critical liquidity paths",
                recommended_action=f"Open channel to {exchange.get('name', 'exchange')}",
                predicted_benefit=20000,
                classification=ActionClassification.REQUIRE_APPROVAL,
                auto_execute_safe=False,
                current_state=exchange
            )
            opportunities.append(opp)

        # Physarum flow recommendations
        flow_recs = state.get("flow_recommendations", {})
        for rec in flow_recs.get("recommendations", []):
            action = rec.get("action")
            channel_id = rec.get("channel_id")

            if action == "atrophy":
                opp = Opportunity(
                    opportunity_type=OpportunityType.CHANNEL_CLOSE,
                    action_type=ActionType.CHANNEL_CLOSE,
                    channel_id=channel_id,
                    peer_id=rec.get("peer_id"),
                    node_name=node_name,
                    priority_score=0.55,
                    confidence_score=0.7,
                    roi_estimate=0.4,
                    description=f"Low-flow channel {channel_id} - atrophy recommended",
                    reasoning=f"Flow intensity {rec.get('flow_intensity', 0):.4f} below threshold",
                    recommended_action="Close channel to free capital",
                    predicted_benefit=rec.get("capacity_sats", 0) // 100,
                    classification=ActionClassification.REQUIRE_APPROVAL,
                    auto_execute_safe=False,
                    current_state=rec
                )
                opportunities.append(opp)

            elif action == "stimulate":
                opp = Opportunity(
                    opportunity_type=OpportunityType.STAGNANT_CHANNEL,
                    action_type=ActionType.FEE_CHANGE,
                    channel_id=channel_id,
                    peer_id=rec.get("peer_id"),
                    node_name=node_name,
                    priority_score=0.5,
                    confidence_score=0.82,  # Raised for auto-execute (Issue #45)
                    roi_estimate=0.5,
                    description=f"Young channel {channel_id} needs stimulation",
                    reasoning="New channel with low flow - reduce fees to attract traffic",
                    recommended_action="Lower fees to stimulate flow",
                    predicted_benefit=1000,
                    classification=ActionClassification.AUTO_EXECUTE,
                    auto_execute_safe=True,
                    current_state=rec
                )
                opportunities.append(opp)

        return opportunities

    async def _scan_competitor_opportunities(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for competitive positioning opportunities."""
        opportunities = []

        competitor_data = state.get("competitor_analysis", {})
        analysis = competitor_data.get("analysis", [])

        for peer_analysis in analysis:
            peer_id = peer_analysis.get("peer_id")
            our_fee = peer_analysis.get("our_fee_ppm", 0)
            competitor_fee = peer_analysis.get("competitor_median_fee", 0)
            recommendation = peer_analysis.get("recommendation", "hold")

            if not peer_id or not our_fee:
                continue

            if recommendation == "undercut" and competitor_fee > our_fee:
                # We could raise fees closer to competitors
                fee_gap = competitor_fee - our_fee
                if fee_gap > 50:
                    opp = Opportunity(
                        opportunity_type=OpportunityType.COMPETITOR_UNDERCUT,
                        action_type=ActionType.FEE_CHANGE,
                        channel_id=peer_analysis.get("channel_id"),
                        peer_id=peer_id,
                        node_name=node_name,
                        priority_score=0.6,
                        confidence_score=0.7,
                        roi_estimate=fee_gap / our_fee if our_fee > 0 else 0.5,
                        description=f"Fee increase opportunity: {fee_gap} ppm gap to competitors",
                        reasoning=f"Our fee {our_fee} ppm vs competitor median {competitor_fee} ppm",
                        recommended_action=f"Increase fee to capture more margin (suggest: {min(our_fee + fee_gap // 2, competitor_fee - 20)} ppm)",
                        predicted_benefit=int(fee_gap * 10),
                        classification=ActionClassification.QUEUE_FOR_REVIEW,
                        auto_execute_safe=False,
                        current_state=peer_analysis
                    )
                    opportunities.append(opp)

            elif recommendation == "premium" and our_fee > competitor_fee * 1.5:
                # We're charging too much relative to competitors
                opp = Opportunity(
                    opportunity_type=OpportunityType.COMPETITOR_UNDERCUT,
                    action_type=ActionType.FEE_CHANGE,
                    channel_id=peer_analysis.get("channel_id"),
                    peer_id=peer_id,
                    node_name=node_name,
                    priority_score=0.55,
                    confidence_score=0.65,
                    roi_estimate=0.4,
                    description=f"Fee potentially too high: {our_fee} vs market {competitor_fee}",
                    reasoning="May be pricing ourselves out of routes",
                    recommended_action=f"Consider reducing fee closer to {competitor_fee} ppm",
                    predicted_benefit=500,
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state=peer_analysis
                )
                opportunities.append(opp)

        return opportunities

    async def _scan_internal_competition(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan for internal fleet competition issues."""
        opportunities = []

        competition = state.get("internal_competition", {})
        conflicts = competition.get("conflicts", [])

        for conflict in conflicts:
            channel_id = conflict.get("our_channel_id")
            peer_id = conflict.get("peer_id")
            competing_member = conflict.get("competing_member")

            if not channel_id:
                continue

            opp = Opportunity(
                opportunity_type=OpportunityType.CONFIG_TUNING,
                action_type=ActionType.FEE_CHANGE,
                channel_id=channel_id,
                peer_id=peer_id,
                node_name=node_name,
                priority_score=0.5,
                confidence_score=0.7,
                roi_estimate=0.3,
                description=f"Internal competition with {competing_member[:16] if competing_member else 'fleet member'}...",
                reasoning="Multiple hive members competing for same route wastes collective resources",
                recommended_action="Coordinate fees with fleet member or defer to corridor owner",
                predicted_benefit=1000,
                classification=ActionClassification.QUEUE_FOR_REVIEW,
                auto_execute_safe=False,
                current_state=conflict
            )
            opportunities.append(opp)

        return opportunities

    async def _scan_rebalance_recommendations(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Scan predictive rebalance recommendations (Phase 3)."""
        opportunities = []

        rebal_data = state.get("rebalance_recommendations", {})
        recommendations = rebal_data.get("recommendations", [])

        for rec in recommendations:
            from_channel = rec.get("from_channel")
            to_channel = rec.get("to_channel")
            amount_sats = rec.get("amount_sats", 0)
            urgency = rec.get("urgency", "low")
            reason = rec.get("reason", "")

            if not from_channel or not to_channel or amount_sats < 10000:
                continue

            priority = 0.8 if urgency == "high" else 0.65 if urgency == "medium" else 0.5
            opp = Opportunity(
                opportunity_type=OpportunityType.PREEMPTIVE_REBALANCE,
                action_type=ActionType.REBALANCE,
                channel_id=to_channel,
                peer_id=None,
                node_name=node_name,
                priority_score=priority,
                confidence_score=rec.get("confidence", 0.7),
                roi_estimate=rec.get("expected_roi", 0.5),
                description=f"Proactive rebalance: {amount_sats:,} sats to {to_channel}",
                reasoning=reason or f"Predictive analysis suggests rebalancing with {urgency} urgency",
                recommended_action=f"Rebalance {amount_sats:,} sats from {from_channel} to {to_channel}",
                predicted_benefit=int(amount_sats * 0.01),
                classification=ActionClassification.QUEUE_FOR_REVIEW,
                auto_execute_safe=False,
                current_state=rec
            )
            opportunities.append(opp)

        return opportunities

    async def _scan_new_member_opportunities(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """
        Scan for new hive member onboarding opportunities.

        When new members (neophytes) join the hive, this scanner:
        1. Identifies members who haven't been "onboarded" yet
        2. Analyzes their topology and connectivity
        3. Suggests strategic channel openings:
           - Existing members opening channels TO the new member
           - New member opening channels to strategic targets

        This ensures new members integrate well into the fleet topology.
        """
        opportunities = []

        # Get hive members from state
        hive_members = state.get("hive_members", {})
        members_list = hive_members.get("members", [])

        if not members_list:
            return opportunities

        # Get our node's pubkey
        node_info = state.get("node_info", {})
        our_pubkey = node_info.get("id", "")

        # Get existing channels to understand current topology
        channels = state.get("channels", [])
        our_peers = set()
        for ch in channels:
            peer_id = ch.get("peer_id")
            if peer_id:
                our_peers.add(peer_id)

        # Get positioning data for strategic targets
        positioning = state.get("positioning", {})
        valuable_corridors = positioning.get("valuable_corridors", [])
        exchange_gaps = positioning.get("exchange_gaps", [])

        # Check for recently joined members (neophytes or recently promoted)
        for member in members_list:
            member_pubkey = member.get("pubkey") or member.get("peer_id")
            member_alias = member.get("alias", "")
            tier = member.get("tier", "unknown")
            joined_at = member.get("joined_at", 0)

            if not member_pubkey:
                continue

            # Skip ourselves
            if member_pubkey == our_pubkey:
                continue

            # Check if this is a new member (neophyte or recently joined)
            is_neophyte = tier == "neophyte"
            is_recent = False
            if joined_at:
                age_days = (time.time() - joined_at) / 86400
                is_recent = age_days < 30  # Joined in last 30 days

            # Skip if not new
            if not is_neophyte and not is_recent:
                continue

            # Check if already onboarded (using advisor DB)
            onboard_key = f"onboarded_{member_pubkey[:16]}"
            if self.db.get_metadata(onboard_key):
                continue

            # Check if we already have a channel to this member
            if member_pubkey in our_peers:
                # We have a channel - no need to suggest one from us
                # But still suggest strategic openings FOR them
                pass
            else:
                # We don't have a channel to this new member
                # Suggest opening one if we have capacity

                # Get member's current connectivity
                member_topology = member.get("topology", [])
                member_channel_count = len(member_topology)

                # Higher priority for members with fewer connections
                connectivity_factor = max(0.3, 1.0 - (member_channel_count / 20))

                opp = Opportunity(
                    opportunity_type=OpportunityType.NEW_MEMBER_CHANNEL,
                    action_type=ActionType.CHANNEL_OPEN,
                    channel_id=None,
                    peer_id=member_pubkey,
                    node_name=node_name,
                    priority_score=0.7 * connectivity_factor,
                    confidence_score=0.8,
                    roi_estimate=0.6,
                    description=f"Open channel to new {tier} member: {member_alias or member_pubkey[:16]}...",
                    reasoning=f"New member joined hive with {member_channel_count} existing channels. "
                              f"Opening a channel strengthens fleet connectivity.",
                    recommended_action=f"Open 2-5M sat channel to {member_alias or member_pubkey[:16]}...",
                    predicted_benefit=5000,
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state={
                        "member_pubkey": member_pubkey,
                        "member_alias": member_alias,
                        "tier": tier,
                        "member_channel_count": member_channel_count,
                        "is_neophyte": is_neophyte,
                        "onboarding": True
                    }
                )
                opportunities.append(opp)

            # Suggest strategic targets for the new member to open channels to
            # (if they have few channels and there are high-value gaps)
            member_topology = member.get("topology", [])
            member_peers = set(member_topology) if member_topology else set()

            # Check valuable corridors the new member could serve
            for corridor in valuable_corridors[:3]:
                target_peer = corridor.get("target_peer") or corridor.get("destination_peer_id")
                if not target_peer:
                    continue

                # Skip if new member already has this peer
                if target_peer in member_peers:
                    continue

                score = corridor.get("value_score", 0)
                if score < 0.2:
                    continue

                opp = Opportunity(
                    opportunity_type=OpportunityType.NEW_MEMBER_CHANNEL,
                    action_type=ActionType.FLAG_FOR_REVIEW,
                    channel_id=None,
                    peer_id=target_peer,
                    node_name=node_name,
                    priority_score=0.6 * score,
                    confidence_score=0.7,
                    roi_estimate=score,
                    description=f"Suggest {member_alias or member_pubkey[:16]}... open to valuable target",
                    reasoning=f"New member could strengthen fleet coverage of high-value corridor "
                              f"(score: {score:.2f})",
                    recommended_action=f"Recommend {member_alias or 'new member'} open channel to target "
                                       f"{target_peer[:16]}...",
                    predicted_benefit=int(score * 10000),
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state={
                        "new_member_pubkey": member_pubkey,
                        "new_member_alias": member_alias,
                        "suggested_target": target_peer,
                        "corridor_value_score": score,
                        "suggestion_for_new_member": True
                    }
                )
                opportunities.append(opp)

            # Check exchange coverage gaps the new member could fill
            for exchange in exchange_gaps[:2]:
                exchange_pubkey = exchange.get("pubkey")
                exchange_name = exchange.get("name", "Unknown Exchange")

                if not exchange_pubkey:
                    continue

                # Skip if new member already connected to this exchange
                if exchange_pubkey in member_peers:
                    continue

                opp = Opportunity(
                    opportunity_type=OpportunityType.NEW_MEMBER_CHANNEL,
                    action_type=ActionType.FLAG_FOR_REVIEW,
                    channel_id=None,
                    peer_id=exchange_pubkey,
                    node_name=node_name,
                    priority_score=0.65,
                    confidence_score=0.75,
                    roi_estimate=0.7,
                    description=f"Suggest {member_alias or member_pubkey[:16]}... connect to {exchange_name}",
                    reasoning=f"Fleet lacks connection to {exchange_name}. "
                              f"New member could fill this gap.",
                    recommended_action=f"Recommend {member_alias or 'new member'} open channel to {exchange_name}",
                    predicted_benefit=15000,
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state={
                        "new_member_pubkey": member_pubkey,
                        "new_member_alias": member_alias,
                        "suggested_exchange": exchange_name,
                        "exchange_pubkey": exchange_pubkey,
                        "suggestion_for_new_member": True
                    }
                )
                opportunities.append(opp)

        return opportunities

    async def _scan_routing_intelligence(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """
        Scan routing intelligence (pheromones + stigmergic markers) for fee optimization.

        Pheromones indicate successful fee levels - channels with strong pheromones
        have proven routing at their current fee. Stigmergic markers provide fleet-wide
        coordination signals about route performance.

        This scanner generates opportunities based on:
        1. Strong pheromones = fee can potentially be raised (proven demand)
        2. No pheromones = fee may be too high, needs stimulation
        3. Stigmergic markers indicating successful routes = follow the signals
        4. Markers indicating failed routes = avoid or adjust fees
        """
        opportunities = []

        pheromones = state.get("pheromone_levels", {})
        markers = state.get("stigmergic_markers", {})
        routing_intel = state.get("routing_intelligence", {})
        channels = state.get("channels", [])

        # Build channel lookup by ID
        channel_lookup = {}
        for ch in channels:
            ch_id = ch.get("short_channel_id") or ch.get("channel_id")
            if ch_id:
                channel_lookup[ch_id] = ch

        # === Pheromone-based opportunities ===
        # pheromone_levels returns {"levels": [{"channel_id": x, "level": y}, ...]}
        pheromone_list = pheromones.get("levels", [])

        for ph in pheromone_list:
            channel_id = ph.get("channel_id")
            pheromone_level = ph.get("level", 0)  # "level" not "pheromone_level"

            if not channel_id:
                continue

            ch_info = channel_lookup.get(channel_id, {})
            current_fee = ch_info.get("fee_per_millionth", 0)
            peer_id = ch_info.get("peer_id")

            # Strong pheromone (>0.1 after our scaling) = channel has proven routing success
            # Consider raising fee if there's headroom
            if pheromone_level > 0.1:
                if current_fee > 0 and current_fee < 1500:
                    suggested_fee = min(current_fee + int(current_fee * 0.1), 1500)

                    opp = Opportunity(
                        opportunity_type=OpportunityType.PHEROMONE_FEE_ADJUST,
                        action_type=ActionType.FEE_CHANGE,
                        channel_id=channel_id,
                        peer_id=peer_id,
                        node_name=node_name,
                        priority_score=min(0.7, 0.4 + pheromone_level),
                        confidence_score=min(0.85, 0.5 + pheromone_level * 2),
                        roi_estimate=0.6,
                        description=f"Pheromone signal: {channel_id} has proven routing demand",
                        reasoning=f"Pheromone level {pheromone_level:.3f} indicates successful routing. "
                                  f"Current fee: {current_fee} ppm.",
                        recommended_action=f"Consider raising fee to ~{suggested_fee} ppm to capture more margin",
                        predicted_benefit=int(pheromone_level * 500),
                        classification=ActionClassification.QUEUE_FOR_REVIEW,
                        auto_execute_safe=False,
                        current_state={
                            "pheromone_level": pheromone_level,
                            "current_fee": current_fee,
                            "suggested_fee": suggested_fee
                        }
                    )
                    opportunities.append(opp)

        # Find channels with NO pheromone data (not routing)
        channels_with_pheromones = {ph.get("channel_id") for ph in pheromone_list if ph.get("channel_id")}

        for ch in channels:
            ch_id = ch.get("short_channel_id") or ch.get("channel_id")
            if not ch_id or ch_id in channels_with_pheromones:
                continue

            # Check if channel has significant capacity but no routing history
            capacity = ch.get("capacity_sats", 0)
            if not capacity:
                total_msat = ch.get("total_msat", 0)
                if isinstance(total_msat, str):
                    total_msat = int(total_msat.replace("msat", ""))
                capacity = total_msat // 1000

            current_fee = ch.get("fee_per_millionth", 0)
            peer_id = ch.get("peer_id")

            if capacity > 1_000_000 and current_fee > 100:  # 1M+ sat channel with >100 ppm
                opp = Opportunity(
                    opportunity_type=OpportunityType.PHEROMONE_FEE_ADJUST,
                    action_type=ActionType.FEE_CHANGE,
                    channel_id=ch_id,
                    peer_id=peer_id,
                    node_name=node_name,
                    priority_score=0.5,
                    confidence_score=0.6,
                    roi_estimate=0.4,
                    description=f"No routing history: {ch_id} may need fee reduction",
                    reasoning=f"No pheromone data (zero routing) with {capacity:,} sat capacity. "
                              f"Current fee {current_fee} ppm may be too high.",
                    recommended_action=f"Consider lowering fee from {current_fee} to ~{max(50, current_fee // 2)} ppm",
                    predicted_benefit=500,
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state={
                        "pheromone_level": 0,
                        "capacity": capacity,
                        "current_fee": current_fee
                    }
                )
                opportunities.append(opp)

        # === Stigmergic marker-based opportunities ===
        # Markers are keyed by source/destination peer IDs, not channel IDs
        marker_list = markers.get("markers", [])

        # Group markers by destination peer (routes TO that peer)
        peer_markers = {}
        for marker in marker_list:
            dest_peer = marker.get("destination_peer_id")
            if dest_peer:
                if dest_peer not in peer_markers:
                    peer_markers[dest_peer] = []
                peer_markers[dest_peer].append(marker)

        # Build reverse lookup: peer_id -> channel_id
        peer_to_channel = {}
        for ch in channels:
            peer_id = ch.get("peer_id")
            ch_id = ch.get("short_channel_id") or ch.get("channel_id")
            if peer_id and ch_id:
                peer_to_channel[peer_id] = ch_id

        for dest_peer, peer_mk_list in peer_markers.items():
            if not peer_mk_list:
                continue

            # Find the channel to this peer
            channel_id = peer_to_channel.get(dest_peer)
            if not channel_id:
                continue

            ch_info = channel_lookup.get(channel_id, {})
            current_fee = ch_info.get("fee_per_millionth", 0)

            # Analyze marker patterns
            success_markers = [m for m in peer_mk_list if m.get("success")]
            fail_markers = [m for m in peer_mk_list if not m.get("success")]

            # Strong success signal from markers
            if len(success_markers) >= 2 and len(fail_markers) == 0:
                avg_fee = sum(m.get("fee_ppm", 0) for m in success_markers) / len(success_markers)

                opp = Opportunity(
                    opportunity_type=OpportunityType.STIGMERGIC_COORDINATION,
                    action_type=ActionType.FEE_CHANGE,
                    channel_id=channel_id,
                    peer_id=dest_peer,
                    node_name=node_name,
                    priority_score=0.6,
                    confidence_score=min(0.8, 0.5 + len(success_markers) * 0.1),
                    roi_estimate=0.5,
                    description=f"Stigmergic signal: routes to {dest_peer[:16]}... have {len(success_markers)} success markers",
                    reasoning=f"Fleet markers indicate successful routing at avg fee {avg_fee:.0f} ppm. "
                              f"Current fee: {current_fee} ppm. Follow the successful signals.",
                    recommended_action=f"Align fee closer to marker signal (~{int(avg_fee)} ppm)",
                    predicted_benefit=int(len(success_markers) * 50),
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state={
                        "success_markers": len(success_markers),
                        "fail_markers": len(fail_markers),
                        "avg_successful_fee": avg_fee,
                        "current_fee": current_fee,
                        "destination_peer": dest_peer
                    }
                )
                opportunities.append(opp)

            # High failure signal from markers
            elif len(fail_markers) >= 2 and len(success_markers) == 0:
                avg_fail_fee = sum(m.get("fee_ppm", 0) for m in fail_markers) / len(fail_markers)

                # Current fee is similar to failing fee - consider adjustment
                if current_fee > 0 and abs(current_fee - avg_fail_fee) < 100:
                    opp = Opportunity(
                        opportunity_type=OpportunityType.STIGMERGIC_COORDINATION,
                        action_type=ActionType.FEE_CHANGE,
                        channel_id=channel_id,
                        peer_id=dest_peer,
                        node_name=node_name,
                        priority_score=0.55,
                        confidence_score=0.6,
                        roi_estimate=0.4,
                        description=f"Stigmergic warning: routes to {dest_peer[:16]}... have {len(fail_markers)} fail markers",
                        reasoning=f"Fleet markers indicate routing failures at avg fee {avg_fail_fee:.0f} ppm. "
                                  f"Current fee {current_fee} ppm is in the failing range.",
                        recommended_action=f"Consider significant fee adjustment (lower to attract flow or raise if oversupplied)",
                        predicted_benefit=200,
                        classification=ActionClassification.QUEUE_FOR_REVIEW,
                        auto_execute_safe=False,
                        current_state={
                            "success_markers": len(success_markers),
                            "fail_markers": len(fail_markers),
                            "avg_fail_fee": avg_fail_fee,
                            "current_fee": current_fee,
                            "destination_peer": dest_peer
                        }
                    )
                    opportunities.append(opp)

        # === Overall routing intelligence status ===
        if routing_intel:
            total_forwards = routing_intel.get("total_forwards_tracked", 0)
            channels_with_data = routing_intel.get("channels_with_pheromones", 0)
            total_channels = len(channels)

            # Low coverage warning - not enough routing data being collected
            if total_channels > 5 and channels_with_data < total_channels * 0.2 and total_forwards < 10:
                opp = Opportunity(
                    opportunity_type=OpportunityType.ROUTING_INTELLIGENCE,
                    action_type=ActionType.FLAG_FOR_REVIEW,
                    channel_id=None,
                    peer_id=None,
                    node_name=node_name,
                    priority_score=0.4,
                    confidence_score=0.7,
                    roi_estimate=0.3,
                    description=f"Low routing intelligence coverage: {channels_with_data}/{total_channels} channels",
                    reasoning=f"Only {channels_with_data} of {total_channels} channels have routing data. "
                              f"Total forwards tracked: {total_forwards}. Consider running backfill or "
                              f"investigating why routing is low.",
                    recommended_action="Run hive-backfill-routing-intelligence or review channel selection",
                    predicted_benefit=0,
                    classification=ActionClassification.QUEUE_FOR_REVIEW,
                    auto_execute_safe=False,
                    current_state={
                        "total_forwards": total_forwards,
                        "channels_with_data": channels_with_data,
                        "total_channels": total_channels
                    }
                )
                opportunities.append(opp)

        return opportunities

    async def _scan_fleet_consensus(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """
        Scan for fleet consensus opportunities (Phase 14.2).

        When multiple fleet members agree on an action, we can safely automate:
        - Close recommendations with consensus -> auto-execute safe if we're clear underperformer
        - Corridor value consensus -> boost confidence for channel opens
        - Fleet defensive warnings -> auto-execute fee increases

        Key insight: Fleet consensus significantly reduces risk of bad decisions.
        If 2+ independent nodes agree, confidence is boosted and auto-execution enabled.
        """
        opportunities = []

        # Get fleet data from state
        fleet_corridors = state.get("fleet_corridor_consensus", {})
        fleet_close_proposals = state.get("fleet_close_proposals", [])
        defense_status = state.get("defense_status", {})
        channels = state.get("channels", [])

        # Build peer-to-channel lookup
        peer_to_channel = {}
        for ch in channels:
            peer_id = ch.get("peer_id")
            ch_id = ch.get("short_channel_id") or ch.get("channel_id")
            if peer_id and ch_id:
                peer_to_channel[peer_id] = ch

        # === Fleet Close Proposals (Rationalization) ===
        # If the fleet says we should close a redundant channel, check consensus
        our_pubkey = state.get("our_pubkey", "")
        for proposal in fleet_close_proposals:
            target_member = proposal.get("target_member", "")
            target_peer = proposal.get("target_peer", "")
            our_share = proposal.get("their_routing_share", 0)  # Our share in their perspective
            their_share = proposal.get("our_routing_share", 0)  # Owner's share
            reporters = proposal.get("reporters", [])

            # Only care about proposals targeting us
            if target_member != our_pubkey:
                continue

            # Check if we have a channel to this peer
            ch = peer_to_channel.get(target_peer)
            if not ch:
                continue

            ch_id = ch.get("short_channel_id") or ch.get("channel_id")
            reporter_count = len(reporters) if isinstance(reporters, list) else 1

            # Consensus: 2+ reporters and we're clearly the underperformer (<10% of owner's share)
            is_consensus = reporter_count >= 2
            is_clear_underperformer = our_share < (their_share * 0.1)

            # Safe to auto-execute if consensus AND clear underperformer AND low capacity
            capacity = ch.get("capacity_sats", 0)
            if not capacity:
                total_msat = ch.get("total_msat", 0)
                if isinstance(total_msat, str):
                    total_msat = int(total_msat.replace("msat", ""))
                capacity = total_msat // 1000

            # Only auto-execute for small channels (<3M sats) with strong consensus
            auto_safe = is_consensus and is_clear_underperformer and capacity < 3_000_000

            confidence = 0.6 + (0.1 * reporter_count)  # Boost confidence with consensus
            if is_clear_underperformer:
                confidence += 0.1

            opp = Opportunity(
                opportunity_type=OpportunityType.FLEET_CONSENSUS_CLOSE,
                action_type=ActionType.CHANNEL_CLOSE,
                channel_id=ch_id,
                peer_id=target_peer,
                node_name=node_name,
                priority_score=0.7 if is_consensus else 0.5,
                confidence_score=min(0.95, confidence),
                roi_estimate=0.5,
                description=f"Fleet consensus: close redundant channel to {target_peer[:16]}...",
                reasoning=f"{reporter_count} fleet members report we're underperformer "
                          f"({our_share:.1%} vs owner's {their_share:.1%}). "
                          f"Channel capacity: {capacity:,} sats.",
                recommended_action="Close channel to free capital for better placement",
                predicted_benefit=int(capacity * 0.001),  # Freed capital benefit
                classification=ActionClassification.AUTO_EXECUTE if auto_safe
                              else ActionClassification.QUEUE_FOR_REVIEW,
                auto_execute_safe=auto_safe,
                current_state={
                    "reporter_count": reporter_count,
                    "our_routing_share": our_share,
                    "owner_routing_share": their_share,
                    "capacity": capacity
                }
            )
            opportunities.append(opp)

        # === Fleet Defensive Warnings ===
        # When fleet warns about a peer we have a channel with, raise fees defensively
        warnings = defense_status.get("warnings", [])
        for warning in warnings:
            peer_id = warning.get("peer_id", "")
            severity = warning.get("severity", "info")
            warning_type = warning.get("type", "")
            sources = warning.get("sources", [])

            ch = peer_to_channel.get(peer_id)
            if not ch:
                continue

            ch_id = ch.get("short_channel_id") or ch.get("channel_id")
            current_fee = ch.get("fee_per_millionth", 0)
            source_count = len(sources) if isinstance(sources, list) else 1

            # Fleet consensus on threat: 2+ sources reporting the same warning
            is_consensus = source_count >= 2
            is_serious = severity in ("high", "critical")

            # Safe to auto-execute defensive fee increase if consensus AND serious
            auto_safe = is_consensus and is_serious and current_fee < 1500

            # Suggested defensive fee based on severity
            if severity == "critical":
                suggested_fee = min(2000, max(1000, current_fee * 2))
            elif severity == "high":
                suggested_fee = min(1500, max(500, int(current_fee * 1.5)))
            else:
                suggested_fee = min(1000, max(300, int(current_fee * 1.25)))

            confidence = 0.7 + (0.1 * source_count)
            if is_serious:
                confidence += 0.1

            opp = Opportunity(
                opportunity_type=OpportunityType.FLEET_DEFENSIVE_ACTION,
                action_type=ActionType.FEE_CHANGE,
                channel_id=ch_id,
                peer_id=peer_id,
                node_name=node_name,
                priority_score=0.8 if is_serious else 0.6,
                confidence_score=min(0.95, confidence),
                roi_estimate=0.3,
                description=f"Fleet defense: {severity} {warning_type} warning for {peer_id[:16]}...",
                reasoning=f"{source_count} fleet sources report {warning_type} threat. "
                          f"Current fee: {current_fee} ppm.",
                recommended_action=f"Raise fee defensively to {suggested_fee} ppm",
                predicted_benefit=500,
                classification=ActionClassification.AUTO_EXECUTE if auto_safe
                              else ActionClassification.QUEUE_FOR_REVIEW,
                auto_execute_safe=auto_safe,
                current_state={
                    "source_count": source_count,
                    "severity": severity,
                    "warning_type": warning_type,
                    "current_fee": current_fee,
                    "suggested_fee": suggested_fee
                }
            )
            opportunities.append(opp)

        # === Fleet Corridor Value Consensus ===
        # When fleet agrees on high-value corridors, suggest channel opens
        for corridor_key, corridor_data in fleet_corridors.items():
            reporters = corridor_data.get("reporters", [])
            value_scores = corridor_data.get("value_scores", [])
            source = corridor_data.get("source", "")
            dest = corridor_data.get("dest", "")

            if len(reporters) < 2:  # Need consensus
                continue

            # Calculate consensus value score
            if value_scores:
                avg_score = sum(value_scores) / len(value_scores)
            else:
                avg_score = 0.5

            if avg_score < 0.3:  # Not valuable enough
                continue

            # Check if we already have connectivity to either endpoint
            have_source = source in peer_to_channel
            have_dest = dest in peer_to_channel

            if have_source and have_dest:
                continue  # Already covered

            # Suggest opening channel to missing endpoint
            target = dest if have_source else source
            confidence = 0.6 + (0.1 * len(reporters)) + (avg_score * 0.2)

            opp = Opportunity(
                opportunity_type=OpportunityType.FLEET_CONSENSUS_CORRIDOR,
                action_type=ActionType.CHANNEL_OPEN,
                channel_id=None,
                peer_id=target,
                node_name=node_name,
                priority_score=avg_score,
                confidence_score=min(0.9, confidence),
                roi_estimate=avg_score,
                description=f"Fleet identifies valuable corridor: {source[:8]}...→{dest[:8]}...",
                reasoning=f"{len(reporters)} fleet members report high-value corridor "
                          f"(avg score: {avg_score:.2f}). We're missing connection to {target[:16]}...",
                recommended_action=f"Open channel to {target[:16]}... to capture corridor value",
                predicted_benefit=int(avg_score * 2000),
                classification=ActionClassification.QUEUE_FOR_REVIEW,  # Channel opens need review
                auto_execute_safe=False,  # Never auto-execute channel opens
                current_state={
                    "reporter_count": len(reporters),
                    "avg_value_score": avg_score,
                    "source": source,
                    "dest": dest,
                    "have_source": have_source,
                    "have_dest": have_dest
                }
            )
            opportunities.append(opp)

        return opportunities

    def filter_safe_opportunities(
        self,
        opportunities: List[Opportunity]
    ) -> Tuple[List[Opportunity], List[Opportunity], List[Opportunity]]:
        """
        Separate opportunities by safety classification.

        Returns:
            Tuple of (auto_execute, queue_review, require_approval) lists
        """
        auto_execute = []
        queue_review = []
        require_approval = []

        for opp in opportunities:
            classification = self.classify_opportunity(opp)
            opp.classification = classification

            if classification == ActionClassification.AUTO_EXECUTE:
                auto_execute.append(opp)
            elif classification == ActionClassification.QUEUE_FOR_REVIEW:
                queue_review.append(opp)
            else:
                require_approval.append(opp)

        return auto_execute, queue_review, require_approval
