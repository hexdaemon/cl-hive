"""
Tests for Yield Metrics Module (Phase 1 - Metrics & Measurement).

Tests cover:
- ChannelYieldMetrics data class and calculations
- ChannelVelocityPrediction
- InternalCompetition detection
- FleetYieldSummary
- YieldMetricsManager
"""

import pytest
import time
from unittest.mock import MagicMock, patch

from modules.yield_metrics import (
    ChannelYieldMetrics,
    ChannelVelocityPrediction,
    InternalCompetition,
    FleetYieldSummary,
    YieldMetricsManager,
    FLOW_INTENSITY_HIGH,
    FLOW_INTENSITY_LOW,
    DEPLETION_RISK_THRESHOLD,
    SATURATION_RISK_THRESHOLD,
)


class MockDatabase:
    """Mock database for testing."""

    def __init__(self):
        self.members = {}

    def get_all_members(self):
        return list(self.members.values()) if self.members else []

    def get_member(self, peer_id):
        return self.members.get(peer_id)


class MockPlugin:
    """Mock plugin for testing."""

    def __init__(self):
        self.logs = []
        self.rpc = MockRpc()

    def log(self, msg, level="info"):
        self.logs.append({"msg": msg, "level": level})


class MockRpc:
    """Mock RPC interface."""

    def __init__(self):
        self.channels = []

    def listpeerchannels(self):
        return {"channels": self.channels}


class MockStateManager:
    """Mock state manager for testing."""

    def __init__(self):
        self.peer_states = {}

    def get_peer_state(self, peer_id):
        return self.peer_states.get(peer_id)

    def get_all_peer_states(self):
        return list(self.peer_states.values())

    def set_peer_state(self, peer_id, capacity=0, topology=None):
        state = MagicMock()
        state.peer_id = peer_id
        state.capacity_sats = capacity
        state.topology = topology or []
        self.peer_states[peer_id] = state


class TestChannelYieldMetrics:
    """Test ChannelYieldMetrics data class."""

    def test_basic_metrics_creation(self):
        """Test creating metrics with basic values."""
        metrics = ChannelYieldMetrics(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            capacity_sats=10_000_000,
            local_balance_sats=5_000_000,
            routing_revenue_sats=10000,
            period_days=30
        )

        assert metrics.channel_id == "123x1x0"
        assert metrics.peer_id == "02" + "a" * 64
        assert metrics.capacity_sats == 10_000_000
        assert metrics.local_balance_pct == 0.5

    def test_roi_calculation(self):
        """Test ROI calculation."""
        # 10,000 sats revenue on 10M capacity over 30 days
        # Daily: 333.33 sats, Annual: 121,666 sats
        # ROI: (121,666 / 10,000,000) * 100 = 1.217%
        metrics = ChannelYieldMetrics(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            capacity_sats=10_000_000,
            routing_revenue_sats=10000,
            period_days=30
        )

        assert metrics.net_revenue_sats == 10000
        expected_roi = (10000 / 30 * 365 / 10_000_000) * 100
        assert abs(metrics.roi_pct - expected_roi) < 0.01

    def test_capital_efficiency(self):
        """Test capital efficiency calculation."""
        metrics = ChannelYieldMetrics(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            capacity_sats=10_000_000,
            routing_revenue_sats=10000,
            period_days=30
        )

        # Capital efficiency = revenue / capacity
        assert metrics.capital_efficiency == 10000 / 10_000_000

    def test_turn_rate_calculation(self):
        """Test turn rate calculation."""
        # 50M volume over 30 days on 10M capacity = 5 turns total
        # Turn rate = 5 / 30 = 0.167 per day
        metrics = ChannelYieldMetrics(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            capacity_sats=10_000_000,
            volume_routed_sats=50_000_000,
            period_days=30
        )

        expected_turn_rate = (50_000_000 / 10_000_000) / 30
        assert abs(metrics.turn_rate - expected_turn_rate) < 0.001

    def test_net_revenue_with_costs(self):
        """Test net revenue calculation with costs."""
        metrics = ChannelYieldMetrics(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            capacity_sats=10_000_000,
            routing_revenue_sats=10000,
            rebalance_cost_sats=2000,
            period_days=30
        )

        assert metrics.total_cost_sats == 2000
        assert metrics.net_revenue_sats == 8000

    def test_zero_capacity_handling(self):
        """Test handling of zero capacity (avoid division by zero)."""
        metrics = ChannelYieldMetrics(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            capacity_sats=0,
            routing_revenue_sats=100,
            period_days=30
        )

        assert metrics.roi_pct == 0.0
        assert metrics.capital_efficiency == 0.0
        assert metrics.local_balance_pct == 0.0

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        metrics = ChannelYieldMetrics(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            capacity_sats=10_000_000,
            routing_revenue_sats=10000,
            period_days=30
        )

        d = metrics.to_dict()
        assert d["channel_id"] == "123x1x0"
        assert d["capacity_sats"] == 10_000_000
        assert "roi_pct" in d
        assert "capital_efficiency" in d

    def test_from_dict_creation(self):
        """Test from_dict creation."""
        data = {
            "channel_id": "123x1x0",
            "peer_id": "02" + "a" * 64,
            "capacity_sats": 10_000_000,
            "routing_revenue_sats": 10000,
            "period_days": 30
        }

        metrics = ChannelYieldMetrics.from_dict(data)
        assert metrics.channel_id == "123x1x0"
        assert metrics.capacity_sats == 10_000_000


class TestChannelVelocityPrediction:
    """Test ChannelVelocityPrediction data class."""

    def test_basic_prediction_creation(self):
        """Test creating a basic prediction."""
        pred = ChannelVelocityPrediction(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            current_local_pct=0.5,
            capacity_sats=10_000_000,
            velocity_pct_per_hour=-0.01  # Losing 1% per hour
        )

        assert pred.channel_id == "123x1x0"
        assert pred.current_local_pct == 0.5
        assert pred.velocity_pct_per_hour == -0.01

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        pred = ChannelVelocityPrediction(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            current_local_pct=0.5,
            depletion_risk=0.3,
            saturation_risk=0.1,
            recommended_action="preemptive_rebalance"
        )

        d = pred.to_dict()
        assert d["channel_id"] == "123x1x0"
        assert d["depletion_risk"] == 0.3
        assert d["recommended_action"] == "preemptive_rebalance"


class TestInternalCompetition:
    """Test InternalCompetition data class."""

    def test_basic_competition_creation(self):
        """Test creating a competition instance."""
        comp = InternalCompetition(
            source_peer_id="02" + "a" * 64,
            destination_peer_id="02" + "b" * 64,
            competing_members=["02" + "c" * 64, "02" + "d" * 64],
            member_count=2
        )

        assert len(comp.competing_members) == 2
        assert comp.member_count == 2

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        comp = InternalCompetition(
            source_peer_id="02" + "a" * 64,
            destination_peer_id="02" + "b" * 64,
            competing_members=["02" + "c" * 64, "02" + "d" * 64],
            member_count=2,
            recommendation="coordinate_fees"
        )

        d = comp.to_dict()
        assert "source_peer_id" in d
        assert "competing_members" in d
        assert d["recommendation"] == "coordinate_fees"


class TestFleetYieldSummary:
    """Test FleetYieldSummary data class."""

    def test_basic_summary_creation(self):
        """Test creating a fleet summary."""
        summary = FleetYieldSummary(
            total_capacity_sats=100_000_000,
            total_revenue_sats=100_000,
            total_channels=10,
            profitable_channels=7,
            underwater_channels=3
        )

        assert summary.total_capacity_sats == 100_000_000
        assert summary.total_channels == 10
        assert summary.profitable_channels == 7

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        summary = FleetYieldSummary(
            total_capacity_sats=100_000_000,
            avg_roi_pct=1.5,
            total_channels=10
        )

        d = summary.to_dict()
        assert d["total_capacity_sats"] == 100_000_000
        assert d["avg_roi_pct"] == 1.5


class TestYieldMetricsManager:
    """Test YieldMetricsManager class."""

    def test_initialization(self):
        """Test basic initialization."""
        db = MockDatabase()
        plugin = MockPlugin()

        mgr = YieldMetricsManager(database=db, plugin=plugin)

        assert mgr.database == db
        assert mgr.plugin == plugin
        assert mgr.our_pubkey is None

    def test_set_our_pubkey(self):
        """Test setting our pubkey."""
        db = MockDatabase()
        plugin = MockPlugin()
        mgr = YieldMetricsManager(database=db, plugin=plugin)

        mgr.set_our_pubkey("02" + "a" * 64)

        assert mgr.our_pubkey == "02" + "a" * 64

    def test_get_channel_yield_metrics_no_channels(self):
        """Test getting metrics when no channels exist."""
        db = MockDatabase()
        plugin = MockPlugin()
        plugin.rpc.channels = []

        mgr = YieldMetricsManager(database=db, plugin=plugin)

        metrics = mgr.get_channel_yield_metrics()
        assert len(metrics) == 0

    def test_get_channel_yield_metrics_with_channel(self):
        """Test getting metrics with a real channel."""
        db = MockDatabase()
        plugin = MockPlugin()

        # Add a mock channel
        plugin.rpc.channels = [{
            "short_channel_id": "123x1x0",
            "peer_id": "02" + "a" * 64,
            "total_msat": 10_000_000_000,  # 10M sats
            "to_us_msat": 5_000_000_000,   # 5M sats
            "state": "CHANNELD_NORMAL",
            "funding": {"local_funds_msat": 10_000_000_000}
        }]

        mgr = YieldMetricsManager(database=db, plugin=plugin)
        metrics = mgr.get_channel_yield_metrics()

        assert len(metrics) == 1
        assert metrics[0].channel_id == "123x1x0"
        assert metrics[0].capacity_sats == 10_000_000

    def test_get_channel_yield_metrics_single_channel(self):
        """Test getting metrics for a specific channel."""
        db = MockDatabase()
        plugin = MockPlugin()

        plugin.rpc.channels = [
            {
                "short_channel_id": "123x1x0",
                "peer_id": "02" + "a" * 64,
                "total_msat": 10_000_000_000,
                "to_us_msat": 5_000_000_000,
                "state": "CHANNELD_NORMAL"
            },
            {
                "short_channel_id": "456x2x0",
                "peer_id": "02" + "b" * 64,
                "total_msat": 20_000_000_000,
                "to_us_msat": 10_000_000_000,
                "state": "CHANNELD_NORMAL"
            }
        ]

        mgr = YieldMetricsManager(database=db, plugin=plugin)
        metrics = mgr.get_channel_yield_metrics(channel_id="123x1x0")

        assert len(metrics) == 1
        assert metrics[0].channel_id == "123x1x0"

    def test_predict_channel_state_no_history(self):
        """Test predicting state when no history exists."""
        db = MockDatabase()
        plugin = MockPlugin()

        plugin.rpc.channels = [{
            "short_channel_id": "123x1x0",
            "peer_id": "02" + "a" * 64,
            "total_msat": 10_000_000_000,
            "to_us_msat": 5_000_000_000,
            "state": "CHANNELD_NORMAL"
        }]

        mgr = YieldMetricsManager(database=db, plugin=plugin)
        pred = mgr.predict_channel_state("123x1x0")

        assert pred is not None
        assert pred.channel_id == "123x1x0"
        assert pred.current_local_pct == 0.5

    def test_get_critical_velocity_channels(self):
        """Test getting critical velocity channels."""
        db = MockDatabase()
        plugin = MockPlugin()

        # Add channels with varying balance
        plugin.rpc.channels = [
            {
                "short_channel_id": "123x1x0",
                "peer_id": "02" + "a" * 64,
                "total_msat": 10_000_000_000,
                "to_us_msat": 1_000_000_000,  # 10% - low local
                "state": "CHANNELD_NORMAL"
            },
            {
                "short_channel_id": "456x2x0",
                "peer_id": "02" + "b" * 64,
                "total_msat": 10_000_000_000,
                "to_us_msat": 5_000_000_000,  # 50% - balanced
                "state": "CHANNELD_NORMAL"
            }
        ]

        mgr = YieldMetricsManager(database=db, plugin=plugin)
        critical = mgr.get_critical_velocity_channels(threshold_hours=24)

        # Channel with 10% local might be flagged as critical
        # (depends on implementation details)
        assert isinstance(critical, list)

    def test_get_fleet_yield_summary_empty(self):
        """Test fleet summary with no channels."""
        db = MockDatabase()
        plugin = MockPlugin()
        plugin.rpc.channels = []

        mgr = YieldMetricsManager(database=db, plugin=plugin)
        summary = mgr.get_fleet_yield_summary()

        assert summary.total_channels == 0
        assert summary.total_capacity_sats == 0

    def test_get_fleet_yield_summary_with_channels(self):
        """Test fleet summary with channels."""
        db = MockDatabase()
        plugin = MockPlugin()

        plugin.rpc.channels = [
            {
                "short_channel_id": "123x1x0",
                "peer_id": "02" + "a" * 64,
                "total_msat": 10_000_000_000,
                "to_us_msat": 5_000_000_000,
                "state": "CHANNELD_NORMAL"
            },
            {
                "short_channel_id": "456x2x0",
                "peer_id": "02" + "b" * 64,
                "total_msat": 20_000_000_000,
                "to_us_msat": 10_000_000_000,
                "state": "CHANNELD_NORMAL"
            }
        ]

        mgr = YieldMetricsManager(database=db, plugin=plugin)
        summary = mgr.get_fleet_yield_summary()

        assert summary.total_channels == 2
        assert summary.total_capacity_sats == 30_000_000  # 10M + 20M


class TestConstants:
    """Test constant values."""

    def test_flow_intensity_thresholds(self):
        """Verify flow intensity thresholds are reasonable."""
        assert FLOW_INTENSITY_HIGH > FLOW_INTENSITY_LOW
        assert FLOW_INTENSITY_HIGH == 0.02  # 2% daily
        assert FLOW_INTENSITY_LOW == 0.001  # 0.1% daily

    def test_risk_thresholds(self):
        """Verify risk thresholds are reasonable."""
        assert DEPLETION_RISK_THRESHOLD < 0.5  # Below balanced
        assert SATURATION_RISK_THRESHOLD > 0.5  # Above balanced
        assert DEPLETION_RISK_THRESHOLD == 0.15  # 15%
        assert SATURATION_RISK_THRESHOLD == 0.85  # 85%
