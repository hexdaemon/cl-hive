"""
Health Score Aggregator for NNLB prioritization.

Aggregates health data from fleet members for INFORMATION SHARING.
No fund movement - each node uses this to optimize its own operations.

The health tier system affects how aggressively a node manages its own channels:
- Struggling: Accept higher rebalance costs to recover own channels
- Vulnerable: Elevated priority for self-recovery
- Stable: Normal operation
- Thriving: Be selective, save on routing fees

Author: Lightning Goats Team
"""

from enum import Enum
from typing import Any, Dict, Optional, Tuple


# =============================================================================
# HEALTH TIERS
# =============================================================================

class HealthTier(Enum):
    """
    NNLB health tiers for budget multiplier calculation.

    Each tier affects how the node manages its OWN operations.
    No fund transfers between nodes.
    """
    STRUGGLING = "struggling"    # 0-30: Accept higher costs to recover
    VULNERABLE = "vulnerable"    # 31-50: Elevated priority for self
    STABLE = "stable"            # 51-70: Normal operation
    THRIVING = "thriving"        # 71-100: Be selective, save fees


# Budget multipliers for OWN rebalancing operations
NNLB_BUDGET_MULTIPLIERS = {
    HealthTier.STRUGGLING: 2.0,   # Accept higher costs to recover own channels
    HealthTier.VULNERABLE: 1.5,   # Elevated priority for own recovery
    HealthTier.STABLE: 1.0,       # Normal operation
    HealthTier.THRIVING: 0.75     # Be selective, save on routing fees
}


# =============================================================================
# HEALTH SCORE AGGREGATOR
# =============================================================================

class HealthScoreAggregator:
    """
    Aggregates and calculates NNLB health scores.

    Health scores are used for INFORMATION SHARING only.
    Each node uses this data to optimize its own operations.
    """

    def __init__(self, database: Any, plugin: Any = None):
        """
        Initialize the health score aggregator.

        Args:
            database: HiveDatabase instance
            plugin: Plugin instance for logging (optional)
        """
        self.database = database
        self.plugin = plugin

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"HEALTH_AGGREGATOR: {message}", level=level)

    def calculate_health_score(
        self,
        profitable_pct: float,
        underwater_pct: float,
        liquidity_score: float,
        revenue_trend: str
    ) -> Tuple[int, HealthTier]:
        """
        Calculate overall health score from components.

        Components and weights:
        - Profitable channels % (40% weight)
        - Inverse underwater % (30% weight)
        - Liquidity balance score (20% weight)
        - Revenue trend bonus (10% weight)

        Args:
            profitable_pct: Percentage of channels that are profitable (0.0-1.0)
            underwater_pct: Percentage of channels that are underwater (0.0-1.0)
            liquidity_score: Liquidity balance score (0-100)
            revenue_trend: "improving", "stable", or "declining"

        Returns:
            (score, tier) tuple where score is 0-100
        """
        # Clamp inputs
        profitable_pct = max(0.0, min(1.0, profitable_pct))
        underwater_pct = max(0.0, min(1.0, underwater_pct))
        liquidity_score = max(0, min(100, liquidity_score))

        # Profitable channels contribution (0-40 points)
        profitable_score = profitable_pct * 40

        # Underwater penalty (0-30 points, inverted - fewer underwater = more points)
        underwater_score = (1.0 - underwater_pct) * 30

        # Liquidity score (0-20 points)
        liquidity_contribution = (liquidity_score / 100) * 20

        # Revenue trend (0-10 points)
        trend_bonus = {
            "improving": 10,
            "stable": 5,
            "declining": 0
        }.get(revenue_trend, 5)

        # Calculate total
        total = round(profitable_score + underwater_score +
                     liquidity_contribution + trend_bonus)
        total = max(0, min(100, total))

        # Determine tier
        tier = self._score_to_tier(total)

        return total, tier

    def _score_to_tier(self, score: int) -> HealthTier:
        """Convert health score to tier."""
        if score <= 30:
            return HealthTier.STRUGGLING
        elif score <= 50:
            return HealthTier.VULNERABLE
        elif score <= 70:
            return HealthTier.STABLE
        else:
            return HealthTier.THRIVING

    def get_budget_multiplier(self, tier: HealthTier) -> float:
        """
        Get rebalance budget multiplier for node's OWN operations.

        This affects how aggressively the node rebalances its own channels.
        Higher multiplier = accept higher costs for rebalancing.

        Args:
            tier: Health tier

        Returns:
            Budget multiplier (0.75 - 2.0)
        """
        return NNLB_BUDGET_MULTIPLIERS.get(tier, 1.0)

    def get_budget_multiplier_from_score(self, score: int) -> float:
        """
        Get budget multiplier directly from health score.

        Args:
            score: Health score (0-100)

        Returns:
            Budget multiplier
        """
        tier = self._score_to_tier(score)
        return self.get_budget_multiplier(tier)

    def calculate_liquidity_score(
        self,
        channels: list
    ) -> int:
        """
        Calculate liquidity balance score from channel data.

        A well-balanced node has channels near 50% local balance.
        Depleted (<20%) or saturated (>80%) channels hurt the score.

        Args:
            channels: List of channel dicts with 'local_balance_pct' key

        Returns:
            Liquidity score (0-100)
        """
        if not channels:
            return 50  # Default to neutral

        total_penalty = 0
        for ch in channels:
            local_pct = ch.get("local_balance_pct", 0.5)

            # Calculate distance from ideal (50%)
            distance = abs(local_pct - 0.5)

            # Penalty increases with distance from 50%
            # 0% or 100% local = 50 penalty points (worst)
            # 50% local = 0 penalty points (ideal)
            penalty = distance * 100
            total_penalty += penalty

        # Average penalty across channels
        avg_penalty = total_penalty / len(channels)

        # Convert to score (0-100, higher is better)
        score = int(max(0, min(100, 100 - avg_penalty)))
        return score

    def update_our_health(
        self,
        profitable_channels: int,
        underwater_channels: int,
        stagnant_channels: int,
        total_channels: int,
        revenue_trend: str,
        liquidity_score: int,
        our_pubkey: str
    ) -> Dict[str, Any]:
        """
        Update our own health record in the database.

        Args:
            profitable_channels: Number of profitable channels
            underwater_channels: Number of underwater channels
            stagnant_channels: Number of stagnant channels
            total_channels: Total number of channels
            revenue_trend: "improving", "stable", or "declining"
            liquidity_score: Liquidity balance score (0-100)
            our_pubkey: Our node's pubkey

        Returns:
            Health record dict with score and tier
        """
        # Calculate percentages
        if total_channels > 0:
            profitable_pct = profitable_channels / total_channels
            underwater_pct = underwater_channels / total_channels
        else:
            profitable_pct = 0.5
            underwater_pct = 0.0

        # Calculate score and tier
        score, tier = self.calculate_health_score(
            profitable_pct=profitable_pct,
            underwater_pct=underwater_pct,
            liquidity_score=liquidity_score,
            revenue_trend=revenue_trend
        )

        # Get budget multiplier
        multiplier = self.get_budget_multiplier(tier)

        # Determine needs flags based on health
        needs_help = tier in [HealthTier.STRUGGLING, HealthTier.VULNERABLE]
        can_help = tier == HealthTier.THRIVING

        # Calculate component scores (for detailed reporting)
        capacity_score = int((1.0 - underwater_pct) * 100)
        revenue_score = int(profitable_pct * 100)
        connectivity_score = liquidity_score

        # Update database
        self.database.update_member_health(
            peer_id=our_pubkey,
            overall_health=score,
            capacity_score=capacity_score,
            revenue_score=revenue_score,
            connectivity_score=connectivity_score,
            tier=tier.value,
            needs_help=needs_help,
            can_help_others=can_help,
            needs_inbound=False,  # Could be calculated from liquidity_score
            needs_outbound=False,
            needs_channels=False,
            assistance_budget_sats=0  # Not used - no fund transfers
        )

        self._log(
            f"Updated our health: score={score}, tier={tier.value}, "
            f"multiplier={multiplier:.2f}"
        )

        return {
            "peer_id": our_pubkey,
            "health_score": score,
            "health_tier": tier.value,
            "budget_multiplier": multiplier,
            "profitable_channels": profitable_channels,
            "underwater_channels": underwater_channels,
            "stagnant_channels": stagnant_channels,
            "total_channels": total_channels,
            "revenue_trend": revenue_trend,
            "liquidity_score": liquidity_score,
            "capacity_score": capacity_score,
            "revenue_score": revenue_score
        }

    def get_our_health(self, our_pubkey: str) -> Optional[Dict[str, Any]]:
        """
        Get our current health record with budget multiplier.

        Args:
            our_pubkey: Our node's pubkey

        Returns:
            Health record with budget_multiplier or None
        """
        health = self.database.get_member_health(our_pubkey)
        if not health:
            return None

        # Add budget multiplier
        score = health.get("overall_health", 50)
        tier = self._score_to_tier(score)
        health["health_tier"] = tier.value
        health["budget_multiplier"] = self.get_budget_multiplier(tier)

        return health

    def get_fleet_health_summary(self) -> Dict[str, Any]:
        """
        Get aggregated fleet health summary.

        Returns:
            Summary dict with tier counts and average health
        """
        all_health = self.database.get_all_member_health()

        if not all_health:
            return {
                "fleet_health": 50,
                "member_count": 0,
                "struggling_count": 0,
                "vulnerable_count": 0,
                "stable_count": 0,
                "thriving_count": 0,
                "members": []
            }

        # Count by tier
        tier_counts = {
            "struggling": 0,
            "vulnerable": 0,
            "stable": 0,
            "thriving": 0
        }

        total_health = 0
        members = []

        for health in all_health:
            score = health.get("overall_health", 50)
            tier = self._score_to_tier(score)
            tier_counts[tier.value] = tier_counts.get(tier.value, 0) + 1
            total_health += score

            members.append({
                "peer_id": health.get("peer_id"),
                "health_score": score,
                "health_tier": tier.value,
                "budget_multiplier": self.get_budget_multiplier(tier)
            })

        avg_health = total_health // len(all_health) if all_health else 50

        return {
            "fleet_health": avg_health,
            "member_count": len(all_health),
            "struggling_count": tier_counts["struggling"],
            "vulnerable_count": tier_counts["vulnerable"],
            "stable_count": tier_counts["stable"],
            "thriving_count": tier_counts["thriving"],
            "members": members
        }
