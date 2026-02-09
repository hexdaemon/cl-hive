"""
Tests for Cost Reduction Module (Phase 3 - Cost Reduction).

Tests cover:
- RebalanceRecommendation data class
- RebalanceOutcome data class
- CircularFlow data class
- FleetPath data class
- PredictiveRebalancer
- FleetRebalanceRouter
- CircularFlowDetector
- CostReductionManager
"""

import pytest
import time
from unittest.mock import MagicMock, patch

from modules.cost_reduction import (
    RebalanceRecommendation,
    RebalanceOutcome,
    CircularFlow,
    FleetPath,
    PredictiveRebalancer,
    FleetRebalanceRouter,
    CircularFlowDetector,
    CostReductionManager,
    DEPLETION_RISK_THRESHOLD,
    SATURATION_RISK_THRESHOLD,
    PREEMPTIVE_MAX_FEE_PPM,
    URGENT_MAX_FEE_PPM,
    FLEET_PATH_SAVINGS_THRESHOLD,
    FLEET_FEE_DISCOUNT_PCT,
    MIN_CIRCULAR_AMOUNT_SATS,
)


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


class MockYieldMetrics:
    """Mock yield metrics manager for testing."""

    def __init__(self):
        self.critical_channels = []
        self.predictions = {}

    def get_critical_velocity_channels(self, threshold_hours=24):
        return self.critical_channels

    def predict_channel_state(self, channel_id, hours=12):
        return self.predictions.get(channel_id)

    def add_critical_channel(self, channel_id, peer_id, local_pct,
                             depletion_risk=0, saturation_risk=0,
                             hours_to_depletion=None, hours_to_saturation=None):
        pred = MagicMock()
        pred.channel_id = channel_id
        pred.peer_id = peer_id
        pred.current_local_pct = local_pct
        pred.capacity_sats = 10_000_000
        pred.depletion_risk = depletion_risk
        pred.saturation_risk = saturation_risk
        pred.hours_to_depletion = hours_to_depletion
        pred.hours_to_saturation = hours_to_saturation
        pred.recommended_action = "preemptive_rebalance"
        self.critical_channels.append(pred)
        self.predictions[channel_id] = pred


# =============================================================================
# DATA CLASS TESTS
# =============================================================================

class TestRebalanceRecommendation:
    """Test RebalanceRecommendation data class."""

    def test_basic_creation(self):
        """Test creating a basic recommendation."""
        rec = RebalanceRecommendation(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            direction="inbound"
        )

        assert rec.channel_id == "123x1x0"
        assert rec.peer_id == "02" + "a" * 64
        assert rec.direction == "inbound"
        assert rec.urgency == "low"

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        rec = RebalanceRecommendation(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            direction="inbound",
            depletion_risk=0.75,
            hours_to_critical=8.5,
            recommended_amount_sats=500000,
            max_fee_ppm=500,
            urgency="high",
            reason="predicted_depletion"
        )

        d = rec.to_dict()

        assert d["channel_id"] == "123x1x0"
        assert d["direction"] == "inbound"
        assert d["depletion_risk"] == 0.75
        assert d["hours_to_critical"] == 8.5
        assert d["urgency"] == "high"
        assert d["recommended_amount_sats"] == 500000

    def test_fleet_path_fields(self):
        """Test fleet path related fields."""
        rec = RebalanceRecommendation(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            direction="inbound",
            fleet_path_available=True,
            fleet_path=["02" + "b" * 64, "02" + "c" * 64],
            estimated_fleet_cost_sats=100,
            estimated_external_cost_sats=500
        )

        d = rec.to_dict()

        assert d["fleet_path_available"] is True
        assert len(d["fleet_path"]) == 2
        assert d["estimated_fleet_cost_sats"] == 100
        assert d["estimated_external_cost_sats"] == 500


class TestRebalanceOutcome:
    """Test RebalanceOutcome data class."""

    def test_basic_creation(self):
        """Test creating a basic outcome."""
        outcome = RebalanceOutcome(
            timestamp=time.time(),
            from_channel="123x1x0",
            to_channel="456x2x0",
            from_peer="02" + "a" * 64,
            to_peer="02" + "b" * 64,
            amount_sats=100000,
            cost_sats=100,
            success=True
        )

        assert outcome.from_channel == "123x1x0"
        assert outcome.to_channel == "456x2x0"
        assert outcome.amount_sats == 100000
        assert outcome.success is True

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        now = time.time()
        outcome = RebalanceOutcome(
            timestamp=now,
            from_channel="123x1x0",
            to_channel="456x2x0",
            from_peer="02" + "a" * 64,
            to_peer="02" + "b" * 64,
            amount_sats=100000,
            cost_sats=100,
            success=True,
            via_fleet=True,
            member_id="02" + "c" * 64
        )

        d = outcome.to_dict()

        assert d["timestamp"] == now
        assert d["via_fleet"] is True
        assert d["member_id"] == "02" + "c" * 64


class TestCircularFlow:
    """Test CircularFlow data class."""

    def test_basic_creation(self):
        """Test creating a circular flow."""
        cf = CircularFlow(
            members=["02" + "a" * 64, "02" + "b" * 64, "02" + "c" * 64],
            total_amount_sats=1000000,
            total_cost_sats=5000,
            cycle_count=3,
            detection_window_hours=24,
            recommendation="WARNING: Coordinate rebalancing"
        )

        assert len(cf.members) == 3
        assert cf.total_amount_sats == 1000000
        assert cf.total_cost_sats == 5000
        assert cf.cycle_count == 3

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        cf = CircularFlow(
            members=["02" + "a" * 64, "02" + "b" * 64],
            total_amount_sats=500000,
            total_cost_sats=2500,
            cycle_count=2,
            detection_window_hours=24,
            recommendation="MONITOR"
        )

        d = cf.to_dict()

        assert "members" in d
        assert d["total_amount_sats"] == 500000
        assert d["recommendation"] == "MONITOR"


class TestFleetPath:
    """Test FleetPath data class."""

    def test_basic_creation(self):
        """Test creating a fleet path."""
        path = FleetPath(
            path=["02" + "a" * 64, "02" + "b" * 64],
            hops=2,
            estimated_cost_sats=50,
            estimated_time_seconds=60,
            reliability_score=0.85
        )

        assert len(path.path) == 2
        assert path.hops == 2
        assert path.estimated_cost_sats == 50
        assert path.reliability_score == 0.85

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        path = FleetPath(
            path=["02" + "a" * 64],
            hops=1,
            estimated_cost_sats=25,
            estimated_time_seconds=30,
            reliability_score=0.9
        )

        d = path.to_dict()

        assert d["hops"] == 1
        assert d["reliability_score"] == 0.9


# =============================================================================
# PREDICTIVE REBALANCER TESTS
# =============================================================================

class TestPredictiveRebalancer:
    """Test PredictiveRebalancer class."""

    def test_initialization(self):
        """Test basic initialization."""
        plugin = MockPlugin()
        yield_metrics = MockYieldMetrics()

        rebalancer = PredictiveRebalancer(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics
        )

        assert rebalancer.plugin == plugin
        assert rebalancer.yield_metrics == yield_metrics

    def test_set_our_pubkey(self):
        """Test setting our pubkey."""
        plugin = MockPlugin()
        rebalancer = PredictiveRebalancer(plugin=plugin)

        rebalancer.set_our_pubkey("02" + "a" * 64)

        assert rebalancer._our_pubkey == "02" + "a" * 64

    def test_get_preemptive_recommendations_no_metrics(self):
        """Test getting recommendations without yield metrics."""
        plugin = MockPlugin()
        rebalancer = PredictiveRebalancer(plugin=plugin)

        recs = rebalancer.get_preemptive_recommendations()

        assert len(recs) == 0

    def test_get_preemptive_recommendations_no_critical(self):
        """Test getting recommendations with no critical channels."""
        plugin = MockPlugin()
        yield_metrics = MockYieldMetrics()

        rebalancer = PredictiveRebalancer(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics
        )

        recs = rebalancer.get_preemptive_recommendations()

        assert len(recs) == 0

    def test_get_preemptive_recommendations_with_depletion(self):
        """Test getting recommendations for depleting channel."""
        plugin = MockPlugin()
        yield_metrics = MockYieldMetrics()

        # Add a channel with high depletion risk
        yield_metrics.add_critical_channel(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            local_pct=0.15,
            depletion_risk=0.85,
            saturation_risk=0.0,
            hours_to_depletion=8.0
        )

        rebalancer = PredictiveRebalancer(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics
        )

        recs = rebalancer.get_preemptive_recommendations()

        assert len(recs) == 1
        assert recs[0].direction == "inbound"
        assert recs[0].urgency == "high"

    def test_get_preemptive_recommendations_with_saturation(self):
        """Test getting recommendations for saturating channel."""
        plugin = MockPlugin()
        yield_metrics = MockYieldMetrics()

        # Add a channel with high saturation risk
        yield_metrics.add_critical_channel(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            local_pct=0.85,
            depletion_risk=0.0,
            saturation_risk=0.85,
            hours_to_saturation=20.0
        )

        rebalancer = PredictiveRebalancer(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics
        )

        recs = rebalancer.get_preemptive_recommendations()

        assert len(recs) == 1
        assert recs[0].direction == "outbound"
        assert recs[0].urgency == "medium"

    def test_urgency_critical_threshold(self):
        """Test critical urgency threshold (<6 hours)."""
        plugin = MockPlugin()
        yield_metrics = MockYieldMetrics()

        yield_metrics.add_critical_channel(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            local_pct=0.05,
            depletion_risk=0.95,
            hours_to_depletion=3.0
        )

        rebalancer = PredictiveRebalancer(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics
        )

        recs = rebalancer.get_preemptive_recommendations()

        assert len(recs) == 1
        assert recs[0].urgency == "critical"
        assert recs[0].max_fee_ppm == URGENT_MAX_FEE_PPM

    def test_urgency_low_threshold(self):
        """Test low urgency threshold (>24 hours)."""
        plugin = MockPlugin()
        yield_metrics = MockYieldMetrics()

        yield_metrics.add_critical_channel(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            local_pct=0.20,
            depletion_risk=0.75,
            hours_to_depletion=48.0
        )

        rebalancer = PredictiveRebalancer(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics
        )

        recs = rebalancer.get_preemptive_recommendations()

        assert len(recs) == 1
        assert recs[0].urgency == "low"
        assert recs[0].max_fee_ppm == PREEMPTIVE_MAX_FEE_PPM

    def test_should_preemptive_rebalance_channel(self):
        """Test checking specific channel for preemptive rebalance."""
        plugin = MockPlugin()
        yield_metrics = MockYieldMetrics()

        yield_metrics.add_critical_channel(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            local_pct=0.10,
            depletion_risk=0.80,
            hours_to_depletion=10.0
        )

        rebalancer = PredictiveRebalancer(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics
        )

        rec = rebalancer.should_preemptive_rebalance("123x1x0", hours=12)

        assert rec is not None
        assert rec.channel_id == "123x1x0"


# =============================================================================
# FLEET REBALANCE ROUTER TESTS
# =============================================================================

class TestFleetRebalanceRouter:
    """Test FleetRebalanceRouter class."""

    def test_initialization(self):
        """Test basic initialization."""
        plugin = MockPlugin()
        state_manager = MockStateManager()

        router = FleetRebalanceRouter(
            plugin=plugin,
            state_manager=state_manager
        )

        assert router.plugin == plugin
        assert router.state_manager == state_manager

    def test_set_our_pubkey(self):
        """Test setting our pubkey."""
        plugin = MockPlugin()
        router = FleetRebalanceRouter(plugin=plugin)

        router.set_our_pubkey("02" + "a" * 64)

        assert router._our_pubkey == "02" + "a" * 64

    def test_get_fleet_topology_empty(self):
        """Test getting topology with no members."""
        plugin = MockPlugin()
        state_manager = MockStateManager()

        router = FleetRebalanceRouter(
            plugin=plugin,
            state_manager=state_manager
        )

        topology = router._get_fleet_topology()

        assert len(topology) == 0

    def test_get_fleet_topology_with_members(self):
        """Test getting topology with members."""
        plugin = MockPlugin()
        state_manager = MockStateManager()

        # Add fleet members with topology
        state_manager.set_peer_state(
            "02" + "a" * 64,
            topology=["02" + "x" * 64, "02" + "y" * 64]
        )
        state_manager.set_peer_state(
            "02" + "b" * 64,
            topology=["02" + "y" * 64, "02" + "z" * 64]
        )

        router = FleetRebalanceRouter(
            plugin=plugin,
            state_manager=state_manager
        )

        topology = router._get_fleet_topology()

        assert len(topology) == 2
        assert "02" + "x" * 64 in topology["02" + "a" * 64]
        assert "02" + "y" * 64 in topology["02" + "b" * 64]

    def test_topology_cache(self):
        """Test topology caching."""
        plugin = MockPlugin()
        state_manager = MockStateManager()

        state_manager.set_peer_state(
            "02" + "a" * 64,
            topology=["02" + "x" * 64]
        )

        router = FleetRebalanceRouter(
            plugin=plugin,
            state_manager=state_manager
        )

        # First call populates cache
        topology1 = router._get_fleet_topology()

        # Add another member (shouldn't affect cached result)
        state_manager.set_peer_state(
            "02" + "b" * 64,
            topology=["02" + "z" * 64]
        )

        # Second call should return cached
        topology2 = router._get_fleet_topology()

        # Cache should still have only 1 member
        assert len(topology2) == 1

    def test_find_fleet_path_no_topology(self):
        """Test finding path with no topology."""
        plugin = MockPlugin()

        router = FleetRebalanceRouter(plugin=plugin)

        path = router.find_fleet_path(
            from_peer="02" + "a" * 64,
            to_peer="02" + "b" * 64,
            amount_sats=100000
        )

        assert path is None

    def test_find_fleet_path_direct(self):
        """Test finding direct path through single member."""
        plugin = MockPlugin()
        state_manager = MockStateManager()

        from_peer = "02" + "x" * 64
        to_peer = "02" + "y" * 64
        member = "02" + "a" * 64

        # Member has channels to both peers
        state_manager.set_peer_state(
            member,
            topology=[from_peer, to_peer]
        )

        router = FleetRebalanceRouter(
            plugin=plugin,
            state_manager=state_manager
        )

        path = router.find_fleet_path(
            from_peer=from_peer,
            to_peer=to_peer,
            amount_sats=100000
        )

        assert path is not None
        assert path.hops == 1
        assert member in path.path

    def test_estimate_fleet_cost(self):
        """Test fleet cost estimation."""
        plugin = MockPlugin()
        router = FleetRebalanceRouter(plugin=plugin)

        # 1M sats, 2 hops, 50 ppm (discounted from 100)
        cost = router._estimate_fleet_cost(1_000_000, 2)

        # Expected: (1M * 50) / 1M * 2 = 100 sats
        expected = (1_000_000 * int(100 * (1 - FLEET_FEE_DISCOUNT_PCT))) // 1_000_000 * 2
        assert cost == expected

    def test_estimate_external_cost(self):
        """Test external cost estimation."""
        plugin = MockPlugin()
        router = FleetRebalanceRouter(plugin=plugin)

        # 1M sats, assumed 500 ppm avg, 3 hops
        cost = router._estimate_external_cost(1_000_000)

        # Expected: (1M * 500) / 1M * 3 = 1500 sats
        expected = (1_000_000 * 500) // 1_000_000 * 3
        assert cost == expected

    def test_get_best_rebalance_path_no_fleet_path(self):
        """Test best path when no fleet path available."""
        plugin = MockPlugin()

        router = FleetRebalanceRouter(plugin=plugin)

        result = router.get_best_rebalance_path(
            from_channel="123x1x0",
            to_channel="456x2x0",
            amount_sats=100000
        )

        assert result["fleet_path_available"] is False
        assert result["recommendation"] == "use_external_path"
        assert result["estimated_external_cost_sats"] > 0

    def test_source_eligible_members_returned(self):
        """When fleet path exists, source_eligible_members should list our peers connected to to_peer."""
        plugin = MagicMock()
        to_peer = "02" + "bb" * 32
        fleet_member = "02" + "cc" * 32  # hive member connected to to_peer AND our peer

        # Mock listpeerchannels: we have channels with from_peer, to_peer, and fleet_member
        plugin.rpc.listpeerchannels.return_value = {
            "channels": [
                {"short_channel_id": "100x1x0", "peer_id": "02" + "aa" * 32},
                {"short_channel_id": "200x2x0", "peer_id": to_peer},
                {"short_channel_id": "300x3x0", "peer_id": fleet_member},
            ]
        }
        plugin.rpc.listchannels.return_value = {"channels": []}

        # Mock state_manager: fleet_member is connected to to_peer in topology
        state_manager = MockStateManager()
        state_manager.set_peer_state(fleet_member, capacity=1_000_000)
        state_manager.peer_states[fleet_member].topology = [to_peer]

        router = FleetRebalanceRouter(plugin=plugin, state_manager=state_manager)

        # Patch _get_peer_for_channel to return the right peers
        with patch.object(router, '_get_peer_for_channel', side_effect=lambda ch: {
            "100x1x0": "02" + "aa" * 32,
            "200x2x0": to_peer,
        }.get(ch)):
            result = router.get_best_rebalance_path(
                from_channel="100x1x0",
                to_channel="200x2x0",
                amount_sats=100000
            )

        if result["fleet_path_available"]:
            assert fleet_member in result.get("source_eligible_members", [])


# =============================================================================
# CIRCULAR FLOW DETECTOR TESTS
# =============================================================================

class TestCircularFlowDetector:
    """Test CircularFlowDetector class."""

    def test_initialization(self):
        """Test basic initialization."""
        plugin = MockPlugin()

        detector = CircularFlowDetector(plugin=plugin)

        assert detector.plugin == plugin
        assert len(detector._rebalance_history) == 0

    def test_record_rebalance_outcome_success(self):
        """Test recording successful rebalance."""
        plugin = MockPlugin()
        detector = CircularFlowDetector(plugin=plugin)

        detector.record_rebalance_outcome(
            from_channel="123x1x0",
            to_channel="456x2x0",
            from_peer="02" + "a" * 64,
            to_peer="02" + "b" * 64,
            amount_sats=100000,
            cost_sats=100,
            success=True
        )

        assert len(detector._rebalance_history) == 1

    def test_record_rebalance_outcome_failure_ignored(self):
        """Test that failed rebalances are ignored."""
        plugin = MockPlugin()
        detector = CircularFlowDetector(plugin=plugin)

        detector.record_rebalance_outcome(
            from_channel="123x1x0",
            to_channel="456x2x0",
            from_peer="02" + "a" * 64,
            to_peer="02" + "b" * 64,
            amount_sats=100000,
            cost_sats=0,
            success=False
        )

        assert len(detector._rebalance_history) == 0

    def test_history_trimming(self):
        """Test that history is trimmed when too large."""
        plugin = MockPlugin()
        detector = CircularFlowDetector(plugin=plugin)
        detector._max_history_size = 5

        # Add more than max
        for i in range(10):
            detector.record_rebalance_outcome(
                from_channel=f"{i}x1x0",
                to_channel="456x2x0",
                from_peer="02" + "a" * 64,
                to_peer="02" + "b" * 64,
                amount_sats=100000,
                cost_sats=100,
                success=True
            )

        assert len(detector._rebalance_history) == 5

    def test_detect_circular_flows_empty(self):
        """Test detection with no history."""
        plugin = MockPlugin()
        detector = CircularFlowDetector(plugin=plugin)

        flows = detector.detect_circular_flows()

        assert len(flows) == 0

    def test_detect_circular_flows_no_cycle(self):
        """Test detection with non-circular flows."""
        plugin = MockPlugin()
        detector = CircularFlowDetector(plugin=plugin)

        # A -> B (no cycle)
        detector.record_rebalance_outcome(
            from_channel="123x1x0",
            to_channel="456x2x0",
            from_peer="02" + "a" * 64,
            to_peer="02" + "b" * 64,
            amount_sats=200000,
            cost_sats=200,
            success=True
        )

        flows = detector.detect_circular_flows()

        assert len(flows) == 0

    def test_detect_circular_flows_with_cycle(self):
        """Test detection with actual circular flow."""
        plugin = MockPlugin()
        detector = CircularFlowDetector(plugin=plugin)

        peer_a = "02" + "a" * 64
        peer_b = "02" + "b" * 64
        peer_c = "02" + "c" * 64

        # Create a cycle: A -> B -> C -> A
        # Record A -> B
        detector.record_rebalance_outcome(
            from_channel="1x1x0",
            to_channel="2x1x0",
            from_peer=peer_a,
            to_peer=peer_b,
            amount_sats=200000,
            cost_sats=200,
            success=True
        )

        # Record B -> C
        detector.record_rebalance_outcome(
            from_channel="2x1x0",
            to_channel="3x1x0",
            from_peer=peer_b,
            to_peer=peer_c,
            amount_sats=200000,
            cost_sats=200,
            success=True
        )

        # Record C -> A (completes cycle)
        detector.record_rebalance_outcome(
            from_channel="3x1x0",
            to_channel="1x1x0",
            from_peer=peer_c,
            to_peer=peer_a,
            amount_sats=200000,
            cost_sats=200,
            success=True
        )

        flows = detector.detect_circular_flows()

        assert len(flows) >= 1
        # Check total cost is tracked
        assert flows[0].total_cost_sats > 0

    def test_get_circular_flow_status(self):
        """Test getting overall status."""
        plugin = MockPlugin()
        detector = CircularFlowDetector(plugin=plugin)

        status = detector.get_circular_flow_status()

        assert status["detection_enabled"] is True
        assert status["history_entries"] == 0
        assert status["circular_flows_detected"] == 0


# =============================================================================
# COST REDUCTION MANAGER TESTS
# =============================================================================

class TestCostReductionManager:
    """Test CostReductionManager class."""

    def test_initialization(self):
        """Test basic initialization."""
        plugin = MockPlugin()

        manager = CostReductionManager(plugin=plugin)

        assert manager.plugin == plugin
        assert manager.predictive_rebalancer is not None
        assert manager.fleet_router is not None
        assert manager.circular_detector is not None

    def test_set_our_pubkey(self):
        """Test setting our pubkey propagates to components."""
        plugin = MockPlugin()
        manager = CostReductionManager(plugin=plugin)

        pubkey = "02" + "a" * 64
        manager.set_our_pubkey(pubkey)

        assert manager._our_pubkey == pubkey
        assert manager.predictive_rebalancer._our_pubkey == pubkey
        assert manager.fleet_router._our_pubkey == pubkey

    def test_get_rebalance_recommendations_empty(self):
        """Test getting recommendations with no critical channels."""
        plugin = MockPlugin()
        yield_metrics = MockYieldMetrics()

        manager = CostReductionManager(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics
        )

        recs = manager.get_rebalance_recommendations()

        assert len(recs) == 0

    def test_get_rebalance_recommendations_with_channel(self):
        """Test getting recommendations with critical channel."""
        plugin = MockPlugin()
        yield_metrics = MockYieldMetrics()

        plugin.rpc.channels = [
            {
                "short_channel_id": "123x1x0",
                "peer_id": "02" + "a" * 64,
                "total_msat": 10_000_000_000,
                "to_us_msat": 1_000_000_000,
                "state": "CHANNELD_NORMAL"
            }
        ]

        yield_metrics.add_critical_channel(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            local_pct=0.10,
            depletion_risk=0.85,
            hours_to_depletion=10.0
        )

        manager = CostReductionManager(
            plugin=plugin,
            yield_metrics_mgr=yield_metrics
        )

        recs = manager.get_rebalance_recommendations()

        assert len(recs) == 1
        assert recs[0]["direction"] == "inbound"

    def test_record_rebalance_outcome(self):
        """Test recording rebalance outcome."""
        plugin = MockPlugin()
        manager = CostReductionManager(plugin=plugin)

        plugin.rpc.channels = [
            {
                "short_channel_id": "123x1x0",
                "peer_id": "02" + "a" * 64,
                "state": "CHANNELD_NORMAL"
            },
            {
                "short_channel_id": "456x2x0",
                "peer_id": "02" + "b" * 64,
                "state": "CHANNELD_NORMAL"
            }
        ]

        result = manager.record_rebalance_outcome(
            from_channel="123x1x0",
            to_channel="456x2x0",
            amount_sats=100000,
            cost_sats=100,
            success=True
        )

        assert result["recorded"] is True

    def test_get_fleet_rebalance_path(self):
        """Test getting fleet rebalance path."""
        plugin = MockPlugin()
        manager = CostReductionManager(plugin=plugin)

        result = manager.get_fleet_rebalance_path(
            from_channel="123x1x0",
            to_channel="456x2x0",
            amount_sats=100000
        )

        assert "fleet_path_available" in result
        assert "estimated_external_cost_sats" in result

    def test_get_cost_reduction_status(self):
        """Test getting overall status."""
        plugin = MockPlugin()
        manager = CostReductionManager(plugin=plugin)

        status = manager.get_cost_reduction_status()

        assert status["predictive_rebalancing_enabled"] is True
        assert status["fleet_routing_enabled"] is True
        assert status["circular_flow_detection_enabled"] is True
        assert "circular_flow_status" in status
        assert "constants" in status


# =============================================================================
# CONSTANTS TESTS
# =============================================================================

class TestConstants:
    """Test constant values."""

    def test_risk_thresholds(self):
        """Verify risk thresholds are reasonable."""
        assert 0 < DEPLETION_RISK_THRESHOLD < 1
        assert 0 < SATURATION_RISK_THRESHOLD < 1

    def test_fee_thresholds(self):
        """Verify fee thresholds are reasonable."""
        assert PREEMPTIVE_MAX_FEE_PPM < URGENT_MAX_FEE_PPM
        assert PREEMPTIVE_MAX_FEE_PPM > 0
        assert URGENT_MAX_FEE_PPM <= 5000  # Reasonable cap

    def test_fleet_savings_threshold(self):
        """Verify fleet savings threshold is reasonable."""
        assert 0 < FLEET_PATH_SAVINGS_THRESHOLD < 1
        assert FLEET_PATH_SAVINGS_THRESHOLD == 0.20  # 20%

    def test_fleet_discount(self):
        """Verify fleet discount is reasonable."""
        assert 0 < FLEET_FEE_DISCOUNT_PCT < 1
        assert FLEET_FEE_DISCOUNT_PCT == 0.50  # 50%

    def test_circular_flow_minimum(self):
        """Verify circular flow minimum is reasonable."""
        assert MIN_CIRCULAR_AMOUNT_SATS >= 10000
        assert MIN_CIRCULAR_AMOUNT_SATS == 100000  # 100k sats


class TestHiveCircularDelegation:
    """Tests for circular rebalance delegation to bridge/sling."""

    def _make_manager(self):
        """Create a CostReductionManager with mocks for circular rebalance testing."""
        plugin = MagicMock()
        plugin.rpc.getinfo.return_value = {"id": "02" + "aa" * 32}
        plugin.rpc.listpeerchannels.return_value = {
            "channels": [
                {
                    "short_channel_id": "100x1x0",
                    "peer_id": "02" + "bb" * 32,
                    "to_us_msat": 5_000_000_000,  # 5M sats outbound
                    "state": "CHANNELD_NORMAL",
                },
                {
                    "short_channel_id": "200x2x0",
                    "peer_id": "02" + "cc" * 32,
                    "to_us_msat": 500_000_000,  # 500k sats outbound
                    "state": "CHANNELD_NORMAL",
                },
            ]
        }
        plugin.rpc.listchannels.return_value = {
            "channels": [
                {
                    "source": "02" + "bb" * 32,
                    "destination": "02" + "cc" * 32,
                    "short_channel_id": "300x3x0",
                }
            ]
        }

        db = MagicMock()
        db.get_all_members.return_value = [
            {"peer_id": "02" + "bb" * 32},
            {"peer_id": "02" + "cc" * 32},
        ]

        mgr = CostReductionManager(plugin, db)
        return mgr

    def test_execute_delegates_to_bridge(self):
        """Execution should delegate to bridge.safe_call with revenue-rebalance."""
        mgr = self._make_manager()
        bridge = MagicMock()
        bridge.safe_call.return_value = {"status": "initiated", "rebalance_id": 42}

        result = mgr.execute_hive_circular_rebalance(
            from_channel="100x1x0",
            to_channel="200x2x0",
            amount_sats=50000,
            dry_run=False,
            bridge=bridge,
        )

        assert result["status"] == "initiated"
        bridge.safe_call.assert_called_once()
        call_args = bridge.safe_call.call_args
        assert call_args[0][0] == "revenue-rebalance"
        payload = call_args[0][1]
        assert payload["from_channel"] == "100x1x0"
        assert payload["to_channel"] == "200x2x0"
        assert payload["amount_sats"] == 50000
        assert payload["max_fee_sats"] == 10

    def test_dry_run_still_returns_preview(self):
        """dry_run=True should return route preview without calling bridge."""
        mgr = self._make_manager()
        bridge = MagicMock()

        result = mgr.execute_hive_circular_rebalance(
            from_channel="100x1x0",
            to_channel="200x2x0",
            amount_sats=50000,
            dry_run=True,
            bridge=bridge,
        )

        assert result["status"] == "preview"
        assert result["dry_run"] is True
        assert len(result["route"]) > 0
        bridge.safe_call.assert_not_called()
