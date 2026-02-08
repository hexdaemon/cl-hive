"""
Peer Quality Scoring Module for cl-hive (Phase 6.2)

Calculates quality scores for external peers based on historical channel
event data collected from hive members. These scores inform topology
decisions in the Planner.

Quality Score Components:
1. Reliability Score (0-1): Based on closure behavior and channel duration
2. Profitability Score (0-1): Based on P&L and revenue data
3. Routing Score (0-1): Based on forward activity
4. Consistency Score (0-1): Based on agreement across multiple reporters

The overall quality score is a weighted combination of these components.
"""

import math
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .database import HiveDatabase


@dataclass
class PeerQualityResult:
    """Result of peer quality scoring calculation."""
    peer_id: str
    overall_score: float  # 0.0 to 1.0
    reliability_score: float
    profitability_score: float
    routing_score: float
    consistency_score: float
    confidence: float  # 0.0 to 1.0 based on data quantity
    recommendation: str  # 'excellent', 'good', 'neutral', 'caution', 'avoid'
    factors: Dict[str, Any]  # Detailed breakdown

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "peer_id": self.peer_id,
            "overall_score": round(self.overall_score, 3),
            "reliability_score": round(self.reliability_score, 3),
            "profitability_score": round(self.profitability_score, 3),
            "routing_score": round(self.routing_score, 3),
            "consistency_score": round(self.consistency_score, 3),
            "confidence": round(self.confidence, 3),
            "recommendation": self.recommendation,
            "factors": self.factors,
        }


class PeerQualityScorer:
    """
    Calculates quality scores for external peers based on hive event data.

    The scoring system uses historical channel events to predict future
    behavior. Peers with a pattern of remote-initiated closures, short
    durations, or poor routing activity receive lower scores.

    Score Weights:
        - Reliability: 35% (most important - will they stay?)
        - Profitability: 25% (are they profitable?)
        - Routing: 25% (do they route traffic?)
        - Consistency: 15% (do hive members agree?)
    """

    # Component weights (sum to 1.0)
    WEIGHT_RELIABILITY = 0.35
    WEIGHT_PROFITABILITY = 0.25
    WEIGHT_ROUTING = 0.25
    WEIGHT_CONSISTENCY = 0.15

    # Reliability scoring parameters
    REMOTE_CLOSE_PENALTY = 0.3  # Penalty per remote close (up to max)
    MAX_REMOTE_CLOSE_PENALTY = 0.6  # Maximum penalty from remote closes
    MUTUAL_CLOSE_BONUS = 0.05  # Small bonus for cooperative closures
    SHORT_DURATION_THRESHOLD_DAYS = 30  # Channels under 30 days = short
    LONG_DURATION_THRESHOLD_DAYS = 180  # Channels over 180 days = long-lived
    DURATION_BONUS_PER_MONTH = 0.05  # Bonus per month of avg duration

    # Profitability scoring parameters
    BREAK_EVEN_DAILY_SATS = 10  # 10 sats/day = break even
    GOOD_DAILY_SATS = 100  # 100 sats/day = good profitability
    EXCELLENT_DAILY_SATS = 500  # 500+ sats/day = excellent

    # Routing scoring parameters
    LOW_FORWARD_COUNT = 10  # Under 10 forwards = low activity
    MEDIUM_FORWARD_COUNT = 100  # 100+ forwards = medium activity
    HIGH_FORWARD_COUNT = 1000  # 1000+ forwards = high activity

    # Confidence thresholds
    MIN_EVENTS_FOR_CONFIDENCE = 3  # Need at least 3 events for any confidence
    GOOD_CONFIDENCE_EVENTS = 10  # 10+ events = good confidence
    HIGH_CONFIDENCE_EVENTS = 25  # 25+ events = high confidence

    # Recommendation thresholds
    EXCELLENT_THRESHOLD = 0.80
    GOOD_THRESHOLD = 0.65
    NEUTRAL_THRESHOLD = 0.45
    CAUTION_THRESHOLD = 0.30
    # Below CAUTION_THRESHOLD = avoid

    def __init__(self, database: 'HiveDatabase', plugin=None):
        """
        Initialize the PeerQualityScorer.

        Args:
            database: HiveDatabase instance for querying events
            plugin: Plugin instance for logging (optional)
        """
        self.database = database
        self.plugin = plugin

    def _log(self, msg: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[QualityScorer] {msg}", level=level)

    def calculate_score(self, peer_id: str, days: int = 90) -> PeerQualityResult:
        """
        Calculate quality score for a peer.

        Args:
            peer_id: The external peer's pubkey
            days: Number of days of history to consider

        Returns:
            PeerQualityResult with scores and recommendation
        """
        # Get aggregated event summary
        summary = self.database.get_peer_event_summary(peer_id, days=days)

        factors = {
            "days_analyzed": days,
            "event_count": summary["event_count"],
            "data_source": "peer_events",
        }

        # Handle no-data case
        if summary["event_count"] == 0:
            return PeerQualityResult(
                peer_id=peer_id,
                overall_score=0.5,  # Neutral for unknown peers
                reliability_score=0.5,
                profitability_score=0.5,
                routing_score=0.5,
                consistency_score=0.5,
                confidence=0.0,
                recommendation="neutral",
                factors={
                    **factors,
                    "note": "No historical data - using neutral defaults",
                }
            )

        # Calculate component scores
        reliability = self._calculate_reliability_score(summary, factors)
        profitability = self._calculate_profitability_score(summary, factors)
        routing = self._calculate_routing_score(summary, factors)
        consistency = self._calculate_consistency_score(summary, factors)

        # Calculate confidence based on data quantity
        confidence = self._calculate_confidence(summary, factors)

        # Calculate weighted overall score
        overall = (
            self.WEIGHT_RELIABILITY * reliability +
            self.WEIGHT_PROFITABILITY * profitability +
            self.WEIGHT_ROUTING * routing +
            self.WEIGHT_CONSISTENCY * consistency
        )

        # Determine recommendation
        recommendation = self._get_recommendation(overall, confidence)

        factors["weight_breakdown"] = {
            "reliability": f"{self.WEIGHT_RELIABILITY:.0%}",
            "profitability": f"{self.WEIGHT_PROFITABILITY:.0%}",
            "routing": f"{self.WEIGHT_ROUTING:.0%}",
            "consistency": f"{self.WEIGHT_CONSISTENCY:.0%}",
        }

        self._log(
            f"Quality score for {peer_id[:16]}...: "
            f"overall={overall:.3f} confidence={confidence:.3f} "
            f"rec={recommendation}"
        )

        return PeerQualityResult(
            peer_id=peer_id,
            overall_score=overall,
            reliability_score=reliability,
            profitability_score=profitability,
            routing_score=routing,
            consistency_score=consistency,
            confidence=confidence,
            recommendation=recommendation,
            factors=factors,
        )

    def _calculate_reliability_score(
        self, summary: Dict[str, Any], factors: Dict
    ) -> float:
        """
        Calculate reliability score based on closure behavior.

        High remote close rate = unreliable
        Long durations = reliable
        Mutual closes = cooperative
        """
        score = 0.5  # Start neutral
        reliability_factors = {}

        close_count = summary["close_count"]
        if close_count > 0:
            # Penalize remote closes
            remote_close_ratio = summary["remote_close_count"] / close_count
            remote_penalty = min(
                self.MAX_REMOTE_CLOSE_PENALTY,
                remote_close_ratio * self.REMOTE_CLOSE_PENALTY * close_count
            )
            score -= remote_penalty
            reliability_factors["remote_close_ratio"] = round(remote_close_ratio, 3)
            reliability_factors["remote_penalty"] = round(remote_penalty, 3)

            # Bonus for mutual closes (cooperative behavior)
            mutual_close_ratio = summary["mutual_close_count"] / close_count
            mutual_bonus = mutual_close_ratio * self.MUTUAL_CLOSE_BONUS * close_count
            mutual_bonus = min(0.15, mutual_bonus)  # Cap bonus
            score += mutual_bonus
            reliability_factors["mutual_close_ratio"] = round(mutual_close_ratio, 3)
            reliability_factors["mutual_bonus"] = round(mutual_bonus, 3)

        # Duration factor
        avg_duration = summary.get("avg_duration_days", 0)
        if avg_duration > 0:
            if avg_duration < self.SHORT_DURATION_THRESHOLD_DAYS:
                # Penalty for short durations
                duration_penalty = 0.15 * (1 - avg_duration / self.SHORT_DURATION_THRESHOLD_DAYS)
                score -= duration_penalty
                reliability_factors["duration_penalty"] = round(duration_penalty, 3)
            elif avg_duration > self.LONG_DURATION_THRESHOLD_DAYS:
                # Bonus for long durations
                months_over = (avg_duration - self.LONG_DURATION_THRESHOLD_DAYS) / 30
                duration_bonus = min(0.2, months_over * self.DURATION_BONUS_PER_MONTH)
                score += duration_bonus
                reliability_factors["duration_bonus"] = round(duration_bonus, 3)

        reliability_factors["avg_duration_days"] = round(avg_duration, 1)

        # Clamp to valid range
        score = max(0.0, min(1.0, score))
        factors["reliability"] = reliability_factors

        return score

    def _calculate_profitability_score(
        self, summary: Dict[str, Any], factors: Dict
    ) -> float:
        """
        Calculate profitability score based on P&L data.

        Uses net P&L normalized by duration to get daily profitability.
        """
        score = 0.5  # Start neutral
        profit_factors = {}

        total_pnl = summary.get("total_net_pnl_sats", 0)
        total_revenue = summary.get("total_revenue_sats", 0)
        total_rebalance = summary.get("total_rebalance_cost_sats", 0)
        avg_duration = summary.get("avg_duration_days", 0)
        close_count = summary.get("close_count", 0)

        profit_factors["total_pnl_sats"] = total_pnl
        profit_factors["total_revenue_sats"] = total_revenue
        profit_factors["total_rebalance_cost_sats"] = total_rebalance

        if close_count > 0 and avg_duration > 0:
            # Calculate average daily P&L across all reported channels
            total_channel_days = avg_duration * close_count
            daily_pnl = total_pnl / total_channel_days if total_channel_days > 0 else 0
            profit_factors["avg_daily_pnl_sats"] = round(daily_pnl, 2)

            # Score based on daily profitability
            if daily_pnl < 0:
                # Negative P&L - penalty
                penalty = min(0.4, abs(daily_pnl) / self.BREAK_EVEN_DAILY_SATS * 0.2)
                score -= penalty
                profit_factors["negative_penalty"] = round(penalty, 3)
            elif daily_pnl < self.BREAK_EVEN_DAILY_SATS:
                # Below break-even but positive
                score += 0.05
            elif daily_pnl < self.GOOD_DAILY_SATS:
                # Good profitability
                ratio = daily_pnl / self.GOOD_DAILY_SATS
                score += 0.1 + (ratio * 0.1)
            elif daily_pnl < self.EXCELLENT_DAILY_SATS:
                # Very good
                score += 0.25
            else:
                # Excellent profitability
                score += 0.35

        # Also consider reported profitability scores from events
        avg_profit_score = summary.get("avg_profitability_score", 0.5)
        if avg_profit_score != 0.5:
            # Blend with reported scores (20% weight)
            score = score * 0.8 + avg_profit_score * 0.2
            profit_factors["reported_avg_score"] = round(avg_profit_score, 3)

        score = max(0.0, min(1.0, score))
        factors["profitability"] = profit_factors

        return score

    def _calculate_routing_score(
        self, summary: Dict[str, Any], factors: Dict
    ) -> float:
        """
        Calculate routing score based on forward activity.

        More forwards = better routing node.
        """
        score = 0.5  # Start neutral
        routing_factors = {}

        total_forwards = summary.get("total_forward_count", 0)
        avg_routing_score = summary.get("avg_routing_score", 0.5)
        close_count = summary.get("close_count", 0)

        routing_factors["total_forwards"] = total_forwards
        routing_factors["reported_avg_score"] = round(avg_routing_score, 3)

        # Score based on forward count
        if total_forwards > 0:
            if total_forwards < self.LOW_FORWARD_COUNT:
                # Low activity
                score = 0.3 + (total_forwards / self.LOW_FORWARD_COUNT) * 0.2
                routing_factors["activity_level"] = "low"
            elif total_forwards < self.MEDIUM_FORWARD_COUNT:
                # Medium activity
                ratio = (total_forwards - self.LOW_FORWARD_COUNT) / (
                    self.MEDIUM_FORWARD_COUNT - self.LOW_FORWARD_COUNT
                )
                score = 0.5 + ratio * 0.15
                routing_factors["activity_level"] = "medium"
            elif total_forwards < self.HIGH_FORWARD_COUNT:
                # High activity
                ratio = (total_forwards - self.MEDIUM_FORWARD_COUNT) / (
                    self.HIGH_FORWARD_COUNT - self.MEDIUM_FORWARD_COUNT
                )
                score = 0.65 + ratio * 0.15
                routing_factors["activity_level"] = "high"
            else:
                # Very high activity
                score = 0.85
                routing_factors["activity_level"] = "very_high"

        # Blend with reported routing scores (30% weight)
        if avg_routing_score != 0.5:
            score = score * 0.7 + avg_routing_score * 0.3

        # Normalize by number of channels if we have close data
        if close_count > 0:
            avg_forwards_per_channel = total_forwards / close_count
            routing_factors["avg_forwards_per_channel"] = round(avg_forwards_per_channel, 1)

        score = max(0.0, min(1.0, score))
        factors["routing"] = routing_factors

        return score

    def _calculate_consistency_score(
        self, summary: Dict[str, Any], factors: Dict
    ) -> float:
        """
        Calculate consistency score based on agreement across reporters.

        Multiple hive members reporting similar experiences = high confidence.
        Conflicting reports = lower consistency.
        """
        score = 0.5  # Start neutral
        consistency_factors = {}

        reporters = summary.get("reporters", [])
        reporter_count = len(reporters)
        consistency_factors["reporter_count"] = reporter_count

        if reporter_count == 0:
            consistency_factors["note"] = "No reporters"
            factors["consistency"] = consistency_factors
            return score

        if reporter_count == 1:
            # Single reporter - neutral consistency
            score = 0.5
            consistency_factors["note"] = "Single reporter - limited data"
        elif reporter_count == 2:
            # Two reporters - slightly better
            score = 0.6
            consistency_factors["note"] = "Two reporters - moderate confidence"
        else:
            # Multiple reporters - high consistency potential
            # Base score increases with more reporters
            score = min(0.85, 0.6 + (reporter_count - 2) * 0.05)
            consistency_factors["note"] = f"{reporter_count} reporters - good sample"

        # Detect disagreement between reporters and reduce consistency score
        reporter_scores = summary.get("reporter_scores", {})
        if len(reporter_scores) >= 2:
            # Calculate variance of profitability scores across reporters
            profit_scores = [rs.get("avg_profitability_score", 0.5)
                           for rs in reporter_scores.values()]
            routing_scores = [rs.get("avg_routing_score", 0.5)
                            for rs in reporter_scores.values()]

            # Calculate standard deviation (measure of disagreement)
            if len(profit_scores) >= 2:
                profit_mean = sum(profit_scores) / len(profit_scores)
                profit_variance = sum((x - profit_mean) ** 2 for x in profit_scores) / len(profit_scores)
                profit_std = math.sqrt(profit_variance)

                routing_mean = sum(routing_scores) / len(routing_scores)
                routing_variance = sum((x - routing_mean) ** 2 for x in routing_scores) / len(routing_scores)
                routing_std = math.sqrt(routing_variance)

                # Average disagreement (0 = perfect agreement, 0.5 = max disagreement)
                avg_std = (profit_std + routing_std) / 2
                consistency_factors["score_std_dev"] = round(avg_std, 3)

                # Penalize high disagreement: std > 0.2 starts reducing score
                if avg_std > 0.2:
                    disagreement_penalty = min(0.3, (avg_std - 0.2) * 1.5)
                    score = max(0.3, score - disagreement_penalty)
                    consistency_factors["disagreement_penalty"] = round(disagreement_penalty, 3)
                    consistency_factors["note"] += " (score reduced due to reporter disagreement)"

        factors["consistency"] = consistency_factors
        return score

    def _calculate_confidence(
        self, summary: Dict[str, Any], factors: Dict
    ) -> float:
        """
        Calculate confidence level based on data quantity.

        More events = higher confidence in the score.
        """
        event_count = summary.get("event_count", 0)
        reporter_count = len(summary.get("reporters", []))

        # Base confidence from event count
        if event_count < self.MIN_EVENTS_FOR_CONFIDENCE:
            confidence = event_count / self.MIN_EVENTS_FOR_CONFIDENCE * 0.3
        elif event_count < self.GOOD_CONFIDENCE_EVENTS:
            ratio = (event_count - self.MIN_EVENTS_FOR_CONFIDENCE) / (
                self.GOOD_CONFIDENCE_EVENTS - self.MIN_EVENTS_FOR_CONFIDENCE
            )
            confidence = 0.3 + ratio * 0.3
        elif event_count < self.HIGH_CONFIDENCE_EVENTS:
            ratio = (event_count - self.GOOD_CONFIDENCE_EVENTS) / (
                self.HIGH_CONFIDENCE_EVENTS - self.GOOD_CONFIDENCE_EVENTS
            )
            confidence = 0.6 + ratio * 0.25
        else:
            confidence = 0.85

        # Boost confidence if multiple reporters
        if reporter_count > 1:
            confidence = min(1.0, confidence + 0.1 * (reporter_count - 1))

        factors["confidence_factors"] = {
            "event_count": event_count,
            "reporter_count": reporter_count,
            "thresholds": {
                "min": self.MIN_EVENTS_FOR_CONFIDENCE,
                "good": self.GOOD_CONFIDENCE_EVENTS,
                "high": self.HIGH_CONFIDENCE_EVENTS,
            }
        }

        return min(1.0, confidence)

    def _get_recommendation(self, score: float, confidence: float) -> str:
        """
        Get recommendation string based on score and confidence.

        Lower confidence shifts recommendation toward neutral.
        """
        # Adjust score toward neutral (0.5) based on confidence
        if confidence < 0.5:
            # Low confidence - regress toward neutral
            adjusted_score = score * confidence + 0.5 * (1 - confidence)
        else:
            adjusted_score = score

        if adjusted_score >= self.EXCELLENT_THRESHOLD:
            return "excellent"
        elif adjusted_score >= self.GOOD_THRESHOLD:
            return "good"
        elif adjusted_score >= self.NEUTRAL_THRESHOLD:
            return "neutral"
        elif adjusted_score >= self.CAUTION_THRESHOLD:
            return "caution"
        else:
            return "avoid"

    def calculate_scores_batch(
        self, peer_ids: List[str], days: int = 90
    ) -> List[PeerQualityResult]:
        """
        Calculate quality scores for multiple peers.

        Args:
            peer_ids: List of peer pubkeys to score
            days: Number of days of history to consider

        Returns:
            List of PeerQualityResult, sorted by overall_score descending
        """
        results = []
        for peer_id in peer_ids:
            result = self.calculate_score(peer_id, days=days)
            results.append(result)

        # Sort by overall score descending
        results.sort(key=lambda r: r.overall_score, reverse=True)
        return results

    def get_scored_peers(
        self, days: int = 90, min_confidence: float = 0.0
    ) -> List[PeerQualityResult]:
        """
        Get quality scores for all peers with event data.

        Args:
            days: Number of days of history to consider
            min_confidence: Minimum confidence threshold (0-1)

        Returns:
            List of PeerQualityResult for all peers with data
        """
        peer_ids = self.database.get_peers_with_events(days=days)
        results = self.calculate_scores_batch(peer_ids, days=days)

        # Filter by confidence if requested
        if min_confidence > 0:
            results = [r for r in results if r.confidence >= min_confidence]

        return results

    def should_open_channel(
        self, peer_id: str, days: int = 90, min_score: float = 0.45
    ) -> Tuple[bool, str]:
        """
        Quick check if we should consider opening a channel to a peer.

        Args:
            peer_id: Peer to evaluate
            days: Days of history to consider
            min_score: Minimum quality score required (default: 0.45)

        Returns:
            Tuple of (should_open: bool, reason: str)
        """
        result = self.calculate_score(peer_id, days=days)

        if result.confidence < 0.3:
            return (True, f"Insufficient data (confidence={result.confidence:.2f}), allow exploration")

        if result.overall_score < min_score:
            return (False, f"Quality score too low: {result.overall_score:.2f} < {min_score:.2f} ({result.recommendation})")

        if result.recommendation == "avoid":
            return (False, f"Recommendation is 'avoid' - high remote close rate or poor performance")

        if result.recommendation == "caution":
            return (True, f"Proceed with caution - score={result.overall_score:.2f}")

        return (True, f"Quality score acceptable: {result.overall_score:.2f} ({result.recommendation})")
