"""
Tests for HealthScoreAggregator module.

Tests the HealthScoreAggregator class for:
- Health score calculation with tier boundaries
- Budget multiplier mapping
- Liquidity score calculation
- Update/query of health records
- Fleet summary aggregation

Author: Lightning Goats Team
"""

import pytest
from unittest.mock import MagicMock

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.health_aggregator import (
    HealthScoreAggregator, HealthTier, NNLB_BUDGET_MULTIPLIERS
)


# =============================================================================
# FIXTURES
# =============================================================================

OUR_PUBKEY = "03" + "b2" * 32


@pytest.fixture
def mock_database():
    """Create a mock database with health methods."""
    db = MagicMock()
    db.update_member_health = MagicMock()
    db.get_member_health = MagicMock(return_value=None)
    db.get_all_member_health = MagicMock(return_value=[])
    return db


@pytest.fixture
def aggregator(mock_database):
    """Create a HealthScoreAggregator instance."""
    return HealthScoreAggregator(database=mock_database)


# =============================================================================
# SCORE CALCULATION TESTS
# =============================================================================

class TestScoreCalculation:
    """Tests for health score calculation."""

    def test_struggling_scenario(self, aggregator):
        """Low profitable, high underwater → STRUGGLING tier (0-30)."""
        score, tier = aggregator.calculate_health_score(
            profitable_pct=0.1,   # 10% profitable → 4 points
            underwater_pct=0.8,   # 80% underwater → 6 points
            liquidity_score=20,   # → 4 points
            revenue_trend="declining"  # → 0 points
        )
        assert tier == HealthTier.STRUGGLING
        assert score <= 30

    def test_thriving_scenario(self, aggregator):
        """High profitable, low underwater → THRIVING tier (71-100)."""
        score, tier = aggregator.calculate_health_score(
            profitable_pct=0.9,   # 90% profitable → 36 points
            underwater_pct=0.05,  # 5% underwater → 28.5 points
            liquidity_score=80,   # → 16 points
            revenue_trend="improving"  # → 10 points
        )
        assert tier == HealthTier.THRIVING
        assert score > 70

    def test_stable_scenario(self, aggregator):
        """Moderate values → STABLE tier (51-70)."""
        score, tier = aggregator.calculate_health_score(
            profitable_pct=0.5,
            underwater_pct=0.3,
            liquidity_score=50,
            revenue_trend="stable"
        )
        assert tier == HealthTier.STABLE
        assert 51 <= score <= 70

    def test_vulnerable_scenario(self, aggregator):
        """Below average → VULNERABLE tier (31-50)."""
        score, tier = aggregator.calculate_health_score(
            profitable_pct=0.3,   # → 12 points
            underwater_pct=0.5,   # → 15 points
            liquidity_score=30,   # → 6 points
            revenue_trend="declining"  # → 0 points
        )
        assert tier == HealthTier.VULNERABLE
        assert 31 <= score <= 50

    def test_input_clamping(self, aggregator):
        """Out-of-range inputs are clamped."""
        score, tier = aggregator.calculate_health_score(
            profitable_pct=2.0,     # Clamped to 1.0
            underwater_pct=-0.5,    # Clamped to 0.0
            liquidity_score=200,    # Clamped to 100
            revenue_trend="improving"
        )
        # All maxed out: 40 + 30 + 20 + 10 = 100
        assert score == 100
        assert tier == HealthTier.THRIVING

    def test_score_clamped_to_0_100(self, aggregator):
        """Score is always between 0 and 100."""
        score, _ = aggregator.calculate_health_score(
            profitable_pct=0.0,
            underwater_pct=1.0,
            liquidity_score=0,
            revenue_trend="declining"
        )
        assert 0 <= score <= 100

    def test_tier_boundaries(self, aggregator):
        """Verify exact tier boundary values."""
        assert aggregator._score_to_tier(0) == HealthTier.STRUGGLING
        assert aggregator._score_to_tier(20) == HealthTier.STRUGGLING
        assert aggregator._score_to_tier(21) == HealthTier.VULNERABLE
        assert aggregator._score_to_tier(40) == HealthTier.VULNERABLE
        assert aggregator._score_to_tier(41) == HealthTier.STABLE
        assert aggregator._score_to_tier(65) == HealthTier.STABLE
        assert aggregator._score_to_tier(66) == HealthTier.THRIVING
        assert aggregator._score_to_tier(100) == HealthTier.THRIVING


# =============================================================================
# BUDGET MULTIPLIER TESTS
# =============================================================================

class TestBudgetMultiplier:
    """Tests for budget multiplier mapping."""

    def test_struggling_multiplier(self, aggregator):
        """STRUGGLING tier gets 2.0x multiplier."""
        mult = aggregator.get_budget_multiplier(HealthTier.STRUGGLING)
        assert mult == 2.0

    def test_thriving_multiplier(self, aggregator):
        """THRIVING tier gets 0.75x multiplier."""
        mult = aggregator.get_budget_multiplier(HealthTier.THRIVING)
        assert mult == 0.75

    def test_stable_multiplier(self, aggregator):
        """STABLE tier gets 1.0x multiplier."""
        mult = aggregator.get_budget_multiplier(HealthTier.STABLE)
        assert mult == 1.0

    def test_multiplier_from_score(self, aggregator):
        """get_budget_multiplier_from_score maps score→tier→multiplier."""
        # Score 20 → STRUGGLING → 2.0
        assert aggregator.get_budget_multiplier_from_score(20) == 2.0
        # Score 80 → THRIVING → 0.75
        assert aggregator.get_budget_multiplier_from_score(80) == 0.75


# =============================================================================
# LIQUIDITY SCORE TESTS
# =============================================================================

class TestLiquidityScore:
    """Tests for liquidity score calculation."""

    def test_balanced_channels_high_score(self, aggregator):
        """All channels near 50% → high score."""
        channels = [
            {"local_balance_pct": 0.5},
            {"local_balance_pct": 0.48},
            {"local_balance_pct": 0.52},
        ]
        score = aggregator.calculate_liquidity_score(channels)
        assert score >= 90

    def test_depleted_channels_low_score(self, aggregator):
        """Channels near 0% → low score."""
        channels = [
            {"local_balance_pct": 0.05},
            {"local_balance_pct": 0.1},
            {"local_balance_pct": 0.02},
        ]
        score = aggregator.calculate_liquidity_score(channels)
        assert score < 60

    def test_empty_channels_default(self, aggregator):
        """Empty channel list → default score of 50."""
        score = aggregator.calculate_liquidity_score([])
        assert score == 50

    def test_saturated_channels_low_score(self, aggregator):
        """Channels near 100% → low score."""
        channels = [
            {"local_balance_pct": 0.95},
            {"local_balance_pct": 0.9},
            {"local_balance_pct": 0.98},
        ]
        score = aggregator.calculate_liquidity_score(channels)
        assert score < 60


# =============================================================================
# UPDATE/QUERY TESTS
# =============================================================================

class TestUpdateQuery:
    """Tests for health record updates and queries."""

    def test_update_our_health_writes_correctly(self, aggregator, mock_database):
        """update_our_health writes to database and returns correct record."""
        result = aggregator.update_our_health(
            profitable_channels=8,
            underwater_channels=1,
            stagnant_channels=1,
            total_channels=10,
            revenue_trend="improving",
            liquidity_score=75,
            our_pubkey=OUR_PUBKEY
        )

        assert result["peer_id"] == OUR_PUBKEY
        assert result["health_score"] > 0
        assert result["health_tier"] in ["struggling", "vulnerable", "stable", "thriving"]
        assert result["budget_multiplier"] > 0
        mock_database.update_member_health.assert_called_once()

    def test_get_our_health_parses(self, aggregator, mock_database):
        """get_our_health fetches and enriches from database."""
        mock_database.get_member_health.return_value = {
            "peer_id": OUR_PUBKEY,
            "overall_health": 75,
        }

        result = aggregator.get_our_health(OUR_PUBKEY)
        assert result is not None
        assert result["health_tier"] == "thriving"
        assert result["budget_multiplier"] == 0.75

    def test_get_our_health_missing(self, aggregator, mock_database):
        """get_our_health returns None when no record exists."""
        mock_database.get_member_health.return_value = None
        result = aggregator.get_our_health(OUR_PUBKEY)
        assert result is None

    def test_fleet_summary_aggregation(self, aggregator, mock_database):
        """get_fleet_health_summary aggregates all members."""
        mock_database.get_all_member_health.return_value = [
            {"peer_id": "peer1", "overall_health": 80},  # thriving
            {"peer_id": "peer2", "overall_health": 15},  # struggling (≤20)
            {"peer_id": "peer3", "overall_health": 60},  # stable
        ]

        summary = aggregator.get_fleet_health_summary()
        assert summary["member_count"] == 3
        assert summary["thriving_count"] == 1
        assert summary["struggling_count"] == 1
        assert summary["stable_count"] == 1
        assert summary["fleet_health"] == 51  # (80+15+60)//3
        assert len(summary["members"]) == 3

    def test_fleet_summary_empty(self, aggregator, mock_database):
        """Fleet summary with no members returns defaults."""
        mock_database.get_all_member_health.return_value = []

        summary = aggregator.get_fleet_health_summary()
        assert summary["member_count"] == 0
        assert summary["fleet_health"] == 50

    def test_update_zero_channels(self, aggregator, mock_database):
        """update_our_health handles zero channels gracefully."""
        result = aggregator.update_our_health(
            profitable_channels=0,
            underwater_channels=0,
            stagnant_channels=0,
            total_channels=0,
            revenue_trend="stable",
            liquidity_score=50,
            our_pubkey=OUR_PUBKEY
        )
        assert result["health_score"] >= 0
        assert result["total_channels"] == 0
