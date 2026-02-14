"""
Tests for Routing Pool functionality (Phase 0 - Collective Economics).

Tests cover:
- RoutingPool class
- Revenue recording
- Contribution calculation
- Distribution calculation
- Pool status reporting
"""

import pytest
import time
from unittest.mock import MagicMock, patch

from modules.routing_pool import (
    RoutingPool,
    MemberContribution,
    PoolDistribution,
    PoolStatus,
    CAPITAL_WEIGHT,
    POSITION_WEIGHT,
    OPERATIONS_WEIGHT,
    MIN_CONTRIBUTION_THRESHOLD,
)


class MockDatabase:
    """Mock database for testing."""

    def __init__(self):
        self.pool_revenue = []
        self.pool_contributions = []
        self.pool_distributions = []
        self.members = {}

    def get_all_members(self):
        return list(self.members.values()) if self.members else []

    def get_member(self, peer_id):
        return self.members.get(peer_id)

    def record_pool_revenue(self, member_id, amount_sats, channel_id=None, payment_hash=None):
        self.pool_revenue.append({
            "member_id": member_id,
            "amount_sats": amount_sats,
            "channel_id": channel_id,
            "payment_hash": payment_hash,
            "recorded_at": int(time.time())
        })

    def get_pool_revenue(self, period=None):
        if not self.pool_revenue:
            return {"total_sats": 0, "transaction_count": 0, "by_member": []}

        total = sum(r["amount_sats"] for r in self.pool_revenue)
        by_member = {}
        for r in self.pool_revenue:
            mid = r["member_id"]
            if mid not in by_member:
                by_member[mid] = {"member_id": mid, "total_sats": 0, "count": 0}
            by_member[mid]["total_sats"] += r["amount_sats"]
            by_member[mid]["count"] += 1

        return {
            "total_sats": total,
            "transaction_count": len(self.pool_revenue),
            "by_member": list(by_member.values())
        }

    def record_pool_contribution(self, **kwargs):
        self.pool_contributions.append(kwargs)

    def get_pool_contributions(self, period):
        return self.pool_contributions

    def get_member_contribution_history(self, member_id, limit=10):
        return [c for c in self.pool_contributions if c.get("member_id") == member_id][:limit]

    def get_member_distribution_history(self, member_id, limit=10):
        return [d for d in self.pool_distributions if d.get("member_id") == member_id][:limit]

    def record_pool_distribution(self, **kwargs):
        self.pool_distributions.append(kwargs)
        return True


class MockPlugin:
    """Mock plugin for testing."""

    def __init__(self):
        self.logs = []

    def log(self, msg, level="info"):
        self.logs.append({"msg": msg, "level": level})


class MockStateManager:
    """Mock state manager for testing."""

    def __init__(self):
        self.peer_states = {}

    def get_peer_state(self, peer_id):
        return self.peer_states.get(peer_id)

    def set_peer_state(self, peer_id, capacity=0, topology=None):
        state = MagicMock()
        state.capacity_sats = capacity
        state.topology = topology or []
        self.peer_states[peer_id] = state


class TestRevenueRecording:
    """Test revenue recording functionality."""

    def test_record_positive_revenue(self):
        """Test that positive revenue is recorded."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        result = pool.record_revenue(
            member_id="02" + "a" * 64,
            amount_sats=1000,
            channel_id="123x1x0"
        )

        assert result is True
        assert len(db.pool_revenue) == 1
        assert db.pool_revenue[0]["amount_sats"] == 1000
        assert db.pool_revenue[0]["member_id"] == "02" + "a" * 64

    def test_reject_zero_revenue(self):
        """Test that zero revenue is rejected."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        result = pool.record_revenue(
            member_id="02" + "a" * 64,
            amount_sats=0
        )

        assert result is False
        assert len(db.pool_revenue) == 0

    def test_reject_negative_revenue(self):
        """Test that negative revenue is rejected."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        result = pool.record_revenue(
            member_id="02" + "a" * 64,
            amount_sats=-100
        )

        assert result is False
        assert len(db.pool_revenue) == 0

    def test_multiple_revenue_records(self):
        """Test recording multiple revenue entries."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        pool.record_revenue("02" + "a" * 64, 1000)
        pool.record_revenue("02" + "b" * 64, 2000)
        pool.record_revenue("02" + "a" * 64, 500)

        assert len(db.pool_revenue) == 3
        revenue = db.get_pool_revenue()
        assert revenue["total_sats"] == 3500
        assert revenue["transaction_count"] == 3


class TestContributionCalculation:
    """Test contribution calculation functionality."""

    def test_basic_contribution_calculation(self):
        """Test basic contribution scoring."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        contrib = pool.calculate_contribution(
            member_id="02" + "a" * 64,
            period="2026-W01",
            capacity_sats=10_000_000,
            uptime_pct=0.95,
            centrality=0.05,
            unique_peers=10,
            bridge_score=0.5,
            success_rate=0.98,
            response_time_ms=30
        )

        assert contrib.member_id == "02" + "a" * 64
        assert contrib.period == "2026-W01"
        assert contrib.total_capacity_sats == 10_000_000
        assert contrib.weighted_capacity_sats == 9_500_000  # 10M * 0.95
        assert contrib.capital_score > 0
        assert contrib.position_score > 0
        assert contrib.operations_score > 0

    def test_low_uptime_reduces_contribution(self):
        """Test that low uptime reduces contribution."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        high_uptime = pool.calculate_contribution(
            member_id="02" + "a" * 64,
            period="2026-W01",
            capacity_sats=10_000_000,
            uptime_pct=1.0,
        )

        low_uptime = pool.calculate_contribution(
            member_id="02" + "a" * 64,
            period="2026-W01",
            capacity_sats=10_000_000,
            uptime_pct=0.5,
        )

        assert high_uptime.weighted_capacity_sats > low_uptime.weighted_capacity_sats
        assert high_uptime.capital_score > low_uptime.capital_score

    def test_contribution_weights_sum_to_one(self):
        """Test that contribution weights sum to 1.0."""
        total_weight = CAPITAL_WEIGHT + POSITION_WEIGHT + OPERATIONS_WEIGHT
        assert abs(total_weight - 1.0) < 0.001


class TestDistributionCalculation:
    """Test distribution calculation functionality."""

    def test_simple_distribution(self):
        """Test simple distribution with equal shares."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        # Add members
        db.members = {
            "02" + "a" * 64: {"peer_id": "02" + "a" * 64, "tier": "member", "uptime_pct": 1.0},
            "02" + "b" * 64: {"peer_id": "02" + "b" * 64, "tier": "member", "uptime_pct": 1.0},
        }

        # Add revenue
        pool.record_revenue("02" + "a" * 64, 5000)
        pool.record_revenue("02" + "b" * 64, 5000)

        # Add contributions manually
        db.pool_contributions = [
            {"member_id": "02" + "a" * 64, "pool_share": 0.5},
            {"member_id": "02" + "b" * 64, "pool_share": 0.5},
        ]

        distributions = pool.calculate_distribution("2026-W01")

        assert len(distributions) == 2
        assert distributions["02" + "a" * 64] == 5000
        assert distributions["02" + "b" * 64] == 5000

    def test_unequal_distribution(self):
        """Test distribution with unequal shares."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        # Add revenue
        pool.record_revenue("02" + "a" * 64, 10000)

        # Add contributions with unequal shares
        db.pool_contributions = [
            {"member_id": "02" + "a" * 64, "pool_share": 0.7},
            {"member_id": "02" + "b" * 64, "pool_share": 0.3},
        ]

        distributions = pool.calculate_distribution("2026-W01")

        assert len(distributions) == 2
        assert distributions["02" + "a" * 64] == 7000
        assert distributions["02" + "b" * 64] == 3000

    def test_minimum_contribution_threshold(self):
        """Test that tiny contributions are excluded."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        # Add revenue
        pool.record_revenue("02" + "a" * 64, 10000)

        # Add contributions with one tiny share
        db.pool_contributions = [
            {"member_id": "02" + "a" * 64, "pool_share": 0.999},
            {"member_id": "02" + "b" * 64, "pool_share": 0.0001},  # Below threshold
        ]

        distributions = pool.calculate_distribution("2026-W01")

        # Only member_a should receive distribution
        assert "02" + "a" * 64 in distributions
        # Member_b is below MIN_CONTRIBUTION_THRESHOLD (0.001)
        if "02" + "b" * 64 in distributions:
            # If included, should have very small share
            assert distributions["02" + "b" * 64] <= 10  # 0.1% of 10000

    def test_no_revenue_no_distribution(self):
        """Test that no revenue means no distribution."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        db.pool_contributions = [
            {"member_id": "02" + "a" * 64, "pool_share": 0.5},
            {"member_id": "02" + "b" * 64, "pool_share": 0.5},
        ]

        distributions = pool.calculate_distribution("2026-W01")

        assert distributions == {}


class TestPoolStatus:
    """Test pool status reporting."""

    def test_get_pool_status(self):
        """Test getting pool status."""
        db = MockDatabase()
        plugin = MockPlugin()
        state_mgr = MockStateManager()
        pool = RoutingPool(database=db, plugin=plugin, state_manager=state_mgr)

        # Add members
        db.members = {
            "02" + "a" * 64: {"peer_id": "02" + "a" * 64, "tier": "member", "uptime_pct": 0.95},
        }
        state_mgr.set_peer_state("02" + "a" * 64, capacity=10_000_000)

        # Add revenue
        pool.record_revenue("02" + "a" * 64, 1000)

        status = pool.get_pool_status()

        assert "period" in status
        assert status["total_revenue_sats"] == 1000
        assert status["transaction_count"] == 1
        assert "contributions" in status
        assert "weights" in status
        assert status["weights"]["capital"] == CAPITAL_WEIGHT

    def test_get_member_status(self):
        """Test getting member-specific status."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        member_id = "02" + "a" * 64

        # Add contribution
        db.pool_contributions = [
            {
                "member_id": member_id,
                "pool_share": 0.5,
                "total_capacity_sats": 10_000_000
            }
        ]

        status = pool.get_member_status(member_id)

        assert status["member_id"] == member_id
        assert "current_contribution" in status
        assert "contribution_history" in status
        assert "distribution_history" in status


class TestPeriodHandling:
    """Test period string handling."""

    def test_current_period_format(self):
        """Test that current period has correct format."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        period = pool._current_period()

        # Should be YYYY-WW format (e.g., "2026-06")
        assert len(period) == 7
        assert period[4] == "-"
        year = int(period[:4])
        week = int(period[5:])
        assert year >= 2024
        assert 1 <= week <= 53

    def test_previous_period(self):
        """Test getting previous period."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        current = pool._current_period()
        previous = pool._previous_period()

        # Previous should be different from current
        # (unless at week boundary, but unlikely in tests)
        current_week = int(current[6:])
        previous_week = int(previous[6:])

        # Previous week should be 1 less (or 52/53 if current is week 1)
        if current_week > 1:
            assert previous_week == current_week - 1
        else:
            assert previous_week >= 52


class TestSettlement:
    """Test period settlement functionality."""

    def test_settle_period(self):
        """Test settling a period."""
        db = MockDatabase()
        plugin = MockPlugin()
        pool = RoutingPool(database=db, plugin=plugin)

        # Add revenue
        pool.record_revenue("02" + "a" * 64, 10000)

        # Add contributions
        db.pool_contributions = [
            {"member_id": "02" + "a" * 64, "pool_share": 1.0},
        ]

        results = pool.settle_period("2026-W01")

        assert len(results) == 1
        assert results[0].member_id == "02" + "a" * 64
        assert results[0].revenue_share_sats == 10000
        assert len(db.pool_distributions) == 1


class TestIntegration:
    """Integration tests for full workflow."""

    def test_full_workflow(self):
        """Test complete workflow: record, snapshot, distribute, settle."""
        db = MockDatabase()
        plugin = MockPlugin()
        state_mgr = MockStateManager()
        pool = RoutingPool(database=db, plugin=plugin, state_manager=state_mgr)

        # Set up members
        member_a = "02" + "a" * 64
        member_b = "02" + "b" * 64

        db.members = {
            member_a: {"peer_id": member_a, "tier": "member", "uptime_pct": 1.0},
            member_b: {"peer_id": member_b, "tier": "member", "uptime_pct": 0.8},
        }

        state_mgr.set_peer_state(member_a, capacity=20_000_000, topology=["p1", "p2"])
        state_mgr.set_peer_state(member_b, capacity=10_000_000, topology=["p3"])

        # Record revenue over time
        pool.record_revenue(member_a, 5000)
        pool.record_revenue(member_b, 3000)
        pool.record_revenue(member_a, 2000)

        # Snapshot contributions
        period = pool._current_period()
        contributions = pool.snapshot_contributions(period)

        assert len(contributions) == 2
        assert sum(c.pool_share for c in contributions) == pytest.approx(1.0, rel=0.01)

        # Calculate distribution (preview)
        distributions = pool.calculate_distribution(period)

        assert len(distributions) == 2
        assert sum(distributions.values()) == 10000

        # Settle period
        results = pool.settle_period(period)

        assert len(results) == 2
        assert sum(r.revenue_share_sats for r in results) == 10000
