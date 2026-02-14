"""
Learning Engine for Proactive AI Advisor

Tracks action outcomes and adapts advisor behavior through:
- Confidence calibration based on prediction accuracy
- Action type effectiveness tracking
- Pattern recognition for opportunity types
- Goal strategy mapping

Usage:
    from learning_engine import LearningEngine

    engine = LearningEngine(db)
    outcomes = engine.measure_outcomes(hours_ago_min=6, hours_ago_max=24)
    confidence = engine.get_adjusted_confidence(0.7, "fee_change", "peak_hour_fee")
"""

import json
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ActionOutcome:
    """Tracked outcome of an advisor action."""
    action_id: int
    action_type: str        # "fee_change", "rebalance", "config_change", etc.
    opportunity_type: str   # "peak_hour_fee", "critical_depletion", "bleeder_fix", etc.
    channel_id: Optional[str]
    node_name: str

    # Context at decision time
    decision_confidence: float
    predicted_benefit: int

    # Outcome (measured 6-24 hours later)
    actual_benefit: int
    success: bool
    outcome_measured_at: int

    # Learning metrics
    prediction_error: float  # (actual - predicted) / predicted if predicted != 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "action_id": self.action_id,
            "action_type": self.action_type,
            "opportunity_type": self.opportunity_type,
            "channel_id": self.channel_id,
            "node_name": self.node_name,
            "decision_confidence": self.decision_confidence,
            "predicted_benefit": self.predicted_benefit,
            "actual_benefit": self.actual_benefit,
            "success": self.success,
            "outcome_measured_at": self.outcome_measured_at,
            "prediction_error": self.prediction_error
        }


@dataclass
class LearnedParameters:
    """Learned parameters that adjust advisor behavior."""
    # Action type multipliers (1.0 = neutral, >1 = more confident, <1 = less confident)
    action_type_confidence: Dict[str, float] = field(default_factory=lambda: {
        "fee_change": 1.0,
        "rebalance": 1.0,
        "config_change": 1.0,
        "channel_open": 1.0,
        "policy_change": 1.0
    })

    # Opportunity type success rates (0.5 = baseline)
    opportunity_success_rates: Dict[str, float] = field(default_factory=dict)

    # Statistics
    total_outcomes_measured: int = 0
    overall_success_rate: float = 0.5
    last_updated: int = 0


# =============================================================================
# Learning Engine
# =============================================================================

class LearningEngine:
    """
    Tracks action outcomes and adjusts advisor behavior.

    Key learning mechanisms:
    1. Confidence calibration - adjust confidence based on accuracy
    2. Action type effectiveness - track which actions actually help
    3. Pattern recognition - learn which opportunities are real
    4. Goal strategy mapping - learn which actions help which goals
    """

    # Minimum samples before adjusting confidence
    MIN_SAMPLES_FOR_ADJUSTMENT = 5

    # Learning rate (how much to adjust toward new observations)
    LEARNING_RATE = 0.1

    # Default success rate for new opportunity types
    # Optimistic default for untracked opportunity types (Issue #45)
    # No reason to assume 50% failure when system success is 99%+
    DEFAULT_SUCCESS_RATE = 0.9

    def __init__(self, db):
        """
        Initialize learning engine.

        Args:
            db: AdvisorDB instance for persistence
        """
        self.db = db
        self._params = self._load_parameters()

    def _load_parameters(self) -> LearnedParameters:
        """Load learned parameters from database."""
        data = self.db.get_learning_params()
        if data:
            params = LearnedParameters()
            params.action_type_confidence = data.get(
                "action_type_confidence",
                params.action_type_confidence
            )
            params.opportunity_success_rates = data.get(
                "opportunity_success_rates", {}
            )
            params.total_outcomes_measured = data.get("total_outcomes_measured", 0)
            params.overall_success_rate = data.get("overall_success_rate", 0.5)
            params.last_updated = data.get("last_updated", 0)
            return params
        return LearnedParameters()

    def _save_parameters(self) -> None:
        """Save learned parameters to database."""
        self._params.last_updated = int(time.time())
        data = {
            "action_type_confidence": self._params.action_type_confidence,
            "opportunity_success_rates": self._params.opportunity_success_rates,
            "total_outcomes_measured": self._params.total_outcomes_measured,
            "overall_success_rate": self._params.overall_success_rate,
            "last_updated": self._params.last_updated
        }
        self.db.save_learning_params(data)

    def measure_outcomes(
        self,
        hours_ago_min: int = 6,
        hours_ago_max: int = 24
    ) -> List[ActionOutcome]:
        """
        Measure outcomes of past decisions.

        Called each cycle to evaluate decisions from the specified time window.
        This window allows actions to have effect but is recent enough for learning.

        Args:
            hours_ago_min: Minimum hours since decision (default 6)
            hours_ago_max: Maximum hours since decision (default 24)

        Returns:
            List of measured ActionOutcome objects
        """
        outcomes = []

        # Get decisions from the time window
        decisions = self.db.get_decisions_in_window(hours_ago_min, hours_ago_max)

        for decision in decisions:
            if decision.get("outcome_measured"):
                continue  # Already measured

            outcome = self._measure_single_outcome(decision)
            if outcome:
                outcomes.append(outcome)
                # Record outcome in database
                self.db.record_action_outcome(outcome.to_dict())

        # Update learned parameters
        if outcomes:
            self._update_learned_parameters(outcomes)

        return outcomes

    def _measure_single_outcome(self, decision: Dict) -> Optional[ActionOutcome]:
        """
        Measure outcome for a single decision.

        Args:
            decision: Decision record from database

        Returns:
            ActionOutcome or None if cannot measure
        """
        action_type = decision.get("decision_type", "unknown")
        node_name = decision.get("node_name", "unknown")
        channel_id = decision.get("channel_id")
        decision_time = decision.get("timestamp", 0)

        # Get context at decision time
        snapshot_metrics = decision.get("snapshot_metrics")
        if snapshot_metrics and isinstance(snapshot_metrics, str):
            try:
                snapshot_metrics = json.loads(snapshot_metrics)
            except json.JSONDecodeError:
                snapshot_metrics = {}
        snapshot_metrics = snapshot_metrics or {}

        # Enrich decision with data from snapshot_metrics if not already present
        if not decision.get("predicted_benefit") and snapshot_metrics:
            decision["predicted_benefit"] = snapshot_metrics.get("predicted_benefit", 0)
        if not decision.get("opportunity_type") and snapshot_metrics.get("opportunity_type"):
            decision["opportunity_type"] = snapshot_metrics["opportunity_type"]

        # Get current state for comparison
        current_state = self._get_current_channel_state(node_name, channel_id)

        if not current_state and action_type in ["fee_change", "rebalance"]:
            # No recent history - skip measurement rather than assume failure (Issue #45)
            # Missing history != channel closed; could be data gap or slow sync
            return None

        # Calculate outcome based on action type
        if action_type == "fee_change":
            outcome = self._measure_fee_change_outcome(
                decision, snapshot_metrics, current_state
            )
        elif action_type == "rebalance":
            outcome = self._measure_rebalance_outcome(
                decision, snapshot_metrics, current_state
            )
        elif action_type == "config_change":
            outcome = self._measure_config_change_outcome(
                decision, snapshot_metrics
            )
        elif action_type == "policy_change":
            outcome = self._measure_policy_change_outcome(
                decision, snapshot_metrics, current_state
            )
        else:
            # Generic outcome - just mark as measured with neutral result
            outcome = ActionOutcome(
                action_id=decision.get("id", 0),
                action_type=action_type,
                opportunity_type=decision.get("opportunity_type", "unknown"),
                channel_id=channel_id,
                node_name=node_name,
                decision_confidence=decision.get("confidence", 0.5),
                predicted_benefit=0,
                actual_benefit=0,
                success=True,  # Neutral
                outcome_measured_at=int(time.time()),
                prediction_error=0
            )

        return outcome

    def _get_current_channel_state(
        self,
        node_name: str,
        channel_id: str,
        hours: int = 24
    ) -> Optional[Dict]:
        """Get current state of a channel from database."""
        if not channel_id:
            return None

        # Get most recent history record within window
        # Use longer window (default 24h) to avoid false "channel closed" (Issue #45)
        history = self.db.get_channel_history(node_name, channel_id, hours=hours)
        if history:
            return history[-1]  # Most recent
        return None

    def _measure_fee_change_outcome(
        self,
        decision: Dict,
        before: Dict,
        after: Optional[Dict]
    ) -> ActionOutcome:
        """
        Measure outcome of a fee change decision using revenue-based comparison.

        Primary metric: fees_earned_sats delta (direct revenue measurement).
        Secondary metric: forward_count delta (volume proxy).
        When both are 0 (no activity), outcome is neutral rather than failed.
        """
        if not after:
            after = {}

        before_revenue = before.get("fees_earned_sats", 0)
        after_revenue = after.get("fees_earned_sats", 0)
        before_flow = before.get("forward_count", 0)
        after_flow = after.get("forward_count", 0)
        after_fee = after.get("fee_ppm", 0)

        # Primary metric: revenue change (direct measurement)
        revenue_delta = after_revenue - before_revenue

        # Secondary metric: flow count change (volume proxy)
        flow_delta = after_flow - before_flow

        # Success criteria:
        # 1. Revenue increased or maintained with fee change
        # 2. Or flow increased significantly even if revenue flat
        # 3. No activity = neutral (don't penalize inactive channels)
        if revenue_delta > 0:
            success = True
            actual_benefit = revenue_delta
        elif revenue_delta == 0 and flow_delta > 0:
            success = True
            actual_benefit = flow_delta * after_fee // 1_000_000  # estimate from count
        elif revenue_delta == 0 and flow_delta == 0:
            # No data yet — neutral (don't penalize for no activity)
            success = True
            actual_benefit = 0
        else:
            success = False
            actual_benefit = revenue_delta  # negative

        predicted_benefit = decision.get("predicted_benefit", 0)
        if predicted_benefit != 0:
            prediction_error = (actual_benefit - predicted_benefit) / abs(predicted_benefit)
        else:
            prediction_error = 0

        return ActionOutcome(
            action_id=decision.get("id", 0),
            action_type="fee_change",
            opportunity_type=decision.get("opportunity_type", "unknown"),
            channel_id=decision.get("channel_id"),
            node_name=decision.get("node_name"),
            decision_confidence=decision.get("confidence", 0.5),
            predicted_benefit=predicted_benefit,
            actual_benefit=actual_benefit,
            success=success,
            outcome_measured_at=int(time.time()),
            prediction_error=prediction_error
        )

    def _measure_rebalance_outcome(
        self,
        decision: Dict,
        before: Dict,
        after: Optional[Dict]
    ) -> ActionOutcome:
        """Measure outcome of a rebalance decision."""
        if not after:
            after = {}

        # Success: channel balance improved toward 0.5
        before_ratio = before.get("balance_ratio", 0.5)
        after_ratio = after.get("balance_ratio", 0.5)

        # Distance from ideal (0.5)
        before_distance = abs(before_ratio - 0.5)
        after_distance = abs(after_ratio - 0.5)

        # Improvement in percentage points
        improvement = (before_distance - after_distance) * 100

        success = after_distance < before_distance - 0.02  # At least 2% improvement
        actual_benefit = int(improvement * 100)  # Scale for comparison

        predicted_benefit = decision.get("predicted_benefit", 0)
        if predicted_benefit != 0:
            prediction_error = (actual_benefit - predicted_benefit) / abs(predicted_benefit)
        else:
            prediction_error = 0

        return ActionOutcome(
            action_id=decision.get("id", 0),
            action_type="rebalance",
            opportunity_type=decision.get("opportunity_type", "unknown"),
            channel_id=decision.get("channel_id"),
            node_name=decision.get("node_name"),
            decision_confidence=decision.get("confidence", 0.5),
            predicted_benefit=predicted_benefit,
            actual_benefit=actual_benefit,
            success=success,
            outcome_measured_at=int(time.time()),
            prediction_error=prediction_error
        )

    def _measure_config_change_outcome(
        self,
        decision: Dict,
        before: Dict
    ) -> ActionOutcome:
        """Measure outcome of a config change decision."""
        # Config changes are harder to measure directly
        # Mark as success if no errors occurred (neutral outcome)
        return ActionOutcome(
            action_id=decision.get("id", 0),
            action_type="config_change",
            opportunity_type=decision.get("opportunity_type", "unknown"),
            channel_id=decision.get("channel_id"),
            node_name=decision.get("node_name"),
            decision_confidence=decision.get("confidence", 0.5),
            predicted_benefit=decision.get("predicted_benefit", 0),
            actual_benefit=0,  # Cannot measure directly
            success=True,  # Assume success if no errors
            outcome_measured_at=int(time.time()),
            prediction_error=0
        )

    def _measure_policy_change_outcome(
        self,
        decision: Dict,
        before: Dict,
        after: Optional[Dict]
    ) -> ActionOutcome:
        """Measure outcome of a policy change (static fees, rebalance mode)."""
        if not after:
            after = {}

        # For static policies, check if the channel stopped bleeding
        before_flow_state = before.get("flow_state", "unknown")
        after_flow_state = after.get("flow_state", "unknown")

        # Success: improved classification or maintained stable
        success = (
            after_flow_state in ["profitable", "stable", "unknown"]
            or after_flow_state != "underwater"
        )

        return ActionOutcome(
            action_id=decision.get("id", 0),
            action_type="policy_change",
            opportunity_type=decision.get("opportunity_type", "unknown"),
            channel_id=decision.get("channel_id"),
            node_name=decision.get("node_name"),
            decision_confidence=decision.get("confidence", 0.5),
            predicted_benefit=decision.get("predicted_benefit", 0),
            actual_benefit=1 if success else -1,
            success=success,
            outcome_measured_at=int(time.time()),
            prediction_error=0
        )

    def _update_learned_parameters(self, outcomes: List[ActionOutcome]) -> None:
        """Update learned parameters based on outcomes."""

        # Group outcomes by action type
        by_action_type: Dict[str, List[ActionOutcome]] = {}
        for outcome in outcomes:
            at = outcome.action_type
            if at not in by_action_type:
                by_action_type[at] = []
            by_action_type[at].append(outcome)

        # Update confidence multipliers
        for action_type, type_outcomes in by_action_type.items():
            if len(type_outcomes) < self.MIN_SAMPLES_FOR_ADJUSTMENT:
                continue

            success_rate = sum(1 for o in type_outcomes if o.success) / len(type_outcomes)

            # Get current multiplier
            current = self._params.action_type_confidence.get(action_type, 1.0)

            # Move toward actual success rate (exponential moving average)
            new_value = current * (1 - self.LEARNING_RATE) + success_rate * self.LEARNING_RATE

            # Clamp to reasonable range [0.5, 1.5]
            new_value = max(0.5, min(1.5, new_value))

            self._params.action_type_confidence[action_type] = new_value

        # Group by opportunity type
        by_opp_type: Dict[str, List[ActionOutcome]] = {}
        for outcome in outcomes:
            ot = outcome.opportunity_type
            if ot not in by_opp_type:
                by_opp_type[ot] = []
            by_opp_type[ot].append(outcome)

        # Update opportunity success rates
        for opp_type, opp_outcomes in by_opp_type.items():
            success_rate = sum(1 for o in opp_outcomes if o.success) / len(opp_outcomes)

            # Get current rate
            current = self._params.opportunity_success_rates.get(
                opp_type, self.DEFAULT_SUCCESS_RATE
            )

            # Exponential moving average
            new_rate = current * (1 - self.LEARNING_RATE * 2) + success_rate * self.LEARNING_RATE * 2

            # Clamp to [0.1, 0.9]
            new_rate = max(0.1, min(0.9, new_rate))

            self._params.opportunity_success_rates[opp_type] = new_rate

        # Update overall statistics
        self._params.total_outcomes_measured += len(outcomes)
        total_success = sum(1 for o in outcomes if o.success)
        current_rate = self._params.overall_success_rate
        new_rate = (
            current_rate * (1 - self.LEARNING_RATE)
            + (total_success / len(outcomes)) * self.LEARNING_RATE
        )
        self._params.overall_success_rate = new_rate

        # Save updated parameters
        self._save_parameters()

    def get_adjusted_confidence(
        self,
        base_confidence: float,
        action_type: str,
        opportunity_type: str
    ) -> float:
        """
        Get confidence adjusted by learning.

        Combines base confidence with learned multipliers.

        Args:
            base_confidence: Initial confidence score (0-1)
            action_type: Type of action being considered
            opportunity_type: Type of opportunity

        Returns:
            Adjusted confidence score (0.1-0.99)
        """
        # Action type multiplier
        action_mult = self._params.action_type_confidence.get(action_type, 1.0)

        # Opportunity success rate (use as additional multiplier)
        opp_rate = self._params.opportunity_success_rates.get(
            opportunity_type, self.DEFAULT_SUCCESS_RATE
        )

        # Use sqrt of action_mult to avoid over-penalizing (Issue #45)
        # sqrt makes the learned penalty less severe as confidence grows
        # Example: 0.59 -> sqrt(0.59) = 0.77, so 0.85 * 0.77 = 0.65 (passable)
        # opp_rate: 0.5 = neutral, 1.0 = 50% boost, 0 = 50% reduction
        adjusted = base_confidence * math.sqrt(action_mult) * (0.5 + opp_rate * 0.5)

        # Clamp to valid range
        return min(0.99, max(0.1, adjusted))

    def get_learning_summary(self) -> Dict[str, Any]:
        """Get summary of learned parameters for display."""
        return {
            "action_type_confidence": dict(self._params.action_type_confidence),
            "opportunity_success_rates": dict(self._params.opportunity_success_rates),
            "total_outcomes_measured": self._params.total_outcomes_measured,
            "overall_success_rate": round(self._params.overall_success_rate, 4),
            "last_updated": datetime.fromtimestamp(
                self._params.last_updated
            ).isoformat() if self._params.last_updated else None
        }

    def should_skip_action(
        self,
        action_type: str,
        opportunity_type: str,
        base_confidence: float
    ) -> Tuple[bool, str]:
        """
        Check if an action should be skipped based on learning.

        Args:
            action_type: Type of action
            opportunity_type: Type of opportunity
            base_confidence: Base confidence score

        Returns:
            Tuple of (should_skip, reason)
        """
        adjusted = self.get_adjusted_confidence(
            base_confidence, action_type, opportunity_type
        )

        # Skip if adjusted confidence is very low
        if adjusted < 0.3:
            opp_rate = self._params.opportunity_success_rates.get(opportunity_type, 0.5)
            return True, f"Low success rate for {opportunity_type} ({opp_rate:.0%})"

        # Skip if action type has been very unsuccessful
        action_conf = self._params.action_type_confidence.get(action_type, 1.0)
        if action_conf < 0.6:
            return True, f"Action type {action_type} has low success (mult={action_conf:.2f})"

        return False, ""

    def reset_learning(self) -> None:
        """Reset all learned parameters to defaults."""
        self._params = LearnedParameters()
        self._save_parameters()

    def get_action_type_recommendations(self) -> List[Dict[str, Any]]:
        """Get recommendations based on action type performance."""
        recommendations = []

        for action_type, confidence in self._params.action_type_confidence.items():
            if confidence < 0.7:
                recommendations.append({
                    "action_type": action_type,
                    "confidence": confidence,
                    "recommendation": f"Review {action_type} strategy - low success rate",
                    "severity": "warning" if confidence > 0.5 else "critical"
                })
            elif confidence > 1.2:
                recommendations.append({
                    "action_type": action_type,
                    "confidence": confidence,
                    "recommendation": f"Consider more aggressive {action_type} actions",
                    "severity": "info"
                })

        return recommendations
