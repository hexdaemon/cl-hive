"""
Tests for Phase 5 Strategic Positioning Module.

Tests cover:
- CorridorValue data class
- PositionRecommendation data class
- FlowRecommendation data class
- PositioningSummary data class
- RouteValueAnalyzer
- FleetPositioningStrategy
- PhysarumChannelManager
- StrategicPositioningManager
"""

import pytest
import time
from unittest.mock import MagicMock, patch

from modules.strategic_positioning import (
    CorridorValue,
    PositionRecommendation,
    FlowRecommendation,
    PositioningSummary,
    RouteValueAnalyzer,
    FleetPositioningStrategy,
    PhysarumChannelManager,
    StrategicPositioningManager,
    HIGH_VALUE_VOLUME_SATS_DAILY,
    MEDIUM_VALUE_VOLUME_SATS_DAILY,
    LOW_COMPETITION_THRESHOLD,
    MEDIUM_COMPETITION_THRESHOLD,
    STRENGTHEN_FLOW_THRESHOLD,
    ATROPHY_FLOW_THRESHOLD,
    STIMULATE_GRACE_DAYS,
    MIN_CHANNEL_AGE_FOR_ATROPHY_DAYS,
    EXCHANGE_PRIORITY_BONUS,
    MAX_MEMBERS_PER_TARGET,
    PRIORITY_EXCHANGES,
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


class MockFeeCoordinationManager:
    """Mock fee coordination manager for testing."""

    def __init__(self):
        self.corridor_manager = MockCorridorManager()


class MockCorridorManager:
    """Mock corridor manager for testing."""

    def __init__(self):
        self.assignments = []

    def get_all_assignments(self):
        return self.assignments


class MockYieldMetricsManager:
    """Mock yield metrics manager for testing."""

    def __init__(self):
        self.channel_metrics = {}

    def get_channel_yield_metrics(self, channel_id=None):
        if channel_id:
            return self.channel_metrics.get(channel_id, [])
        return list(self.channel_metrics.values())


# =============================================================================
# DATA CLASS TESTS
# =============================================================================

class TestCorridorValue:
    """Tests for CorridorValue data class."""

    def test_corridor_value_defaults(self):
        """Test CorridorValue has correct defaults."""
        corridor = CorridorValue(
            source_peer_id="source123",
            destination_peer_id="dest456"
        )
        assert corridor.source_peer_id == "source123"
        assert corridor.destination_peer_id == "dest456"
        assert corridor.daily_volume_sats == 0
        assert corridor.monthly_volume_sats == 0
        assert corridor.competitor_count == 0
        assert corridor.fleet_members_present == 0
        assert corridor.value_score == 0.0
        assert corridor.value_tier == "unknown"
        assert corridor.competition_level == "unknown"
        assert corridor.accessible is True

    def test_corridor_value_to_dict(self):
        """Test CorridorValue to_dict method."""
        corridor = CorridorValue(
            source_peer_id="source123",
            destination_peer_id="dest456",
            source_alias="Source Node",
            destination_alias="Dest Node",
            daily_volume_sats=5_000_000,
            monthly_volume_sats=150_000_000,
            competitor_count=8,
            fleet_members_present=2,
            value_score=0.456,
            margin_estimate_ppm=350,
            value_tier="medium",
            competition_level="medium"
        )
        result = corridor.to_dict()
        assert result["source_peer_id"] == "source123"
        assert result["destination_alias"] == "Dest Node"
        assert result["value_score"] == 0.456
        assert result["value_tier"] == "medium"


class TestPositionRecommendation:
    """Tests for PositionRecommendation data class."""

    def test_position_recommendation_defaults(self):
        """Test PositionRecommendation has correct defaults."""
        rec = PositionRecommendation(target_peer_id="target123")
        assert rec.target_peer_id == "target123"
        assert rec.recommended_member is None
        assert rec.recommended_capacity_sats == 0
        assert rec.priority_score == 0.0
        assert rec.priority_tier == "low"
        assert rec.is_exchange is False
        assert rec.is_underserved is False

    def test_position_recommendation_to_dict(self):
        """Test PositionRecommendation to_dict method."""
        rec = PositionRecommendation(
            target_peer_id="target123",
            target_alias="ACINQ",
            recommended_member="member456",
            recommended_capacity_sats=5_000_000,
            priority_score=0.75,
            priority_tier="high",
            is_exchange=True,
            corridor_value=0.5
        )
        result = rec.to_dict()
        assert result["target_peer_id"] == "target123"
        assert result["is_exchange"] is True
        assert result["priority_tier"] == "high"
        assert result["corridor_value"] == 0.5


class TestFlowRecommendation:
    """Tests for FlowRecommendation data class."""

    def test_flow_recommendation_defaults(self):
        """Test FlowRecommendation has correct defaults."""
        rec = FlowRecommendation(
            channel_id="123x456x0",
            peer_id="peer123"
        )
        assert rec.channel_id == "123x456x0"
        assert rec.peer_id == "peer123"
        assert rec.flow_intensity == 0.0
        assert rec.turn_rate == 0.0
        assert rec.action == "hold"
        assert rec.method == ""

    def test_flow_recommendation_strengthen(self):
        """Test FlowRecommendation for strengthen action."""
        rec = FlowRecommendation(
            channel_id="123x456x0",
            peer_id="peer123",
            flow_intensity=0.05,
            action="strengthen",
            method="splice_in",
            splice_amount_sats=1_000_000
        )
        result = rec.to_dict()
        assert result["action"] == "strengthen"
        assert result["method"] == "splice_in"
        assert result["splice_amount_sats"] == 1_000_000

    def test_flow_recommendation_atrophy(self):
        """Test FlowRecommendation for atrophy action."""
        rec = FlowRecommendation(
            channel_id="123x456x0",
            peer_id="peer123",
            flow_intensity=0.0005,
            age_days=200,
            action="atrophy",
            method="cooperative_close",
            capital_to_redeploy_sats=5_000_000
        )
        result = rec.to_dict()
        assert result["action"] == "atrophy"
        assert result["method"] == "cooperative_close"
        assert result["capital_to_redeploy_sats"] == 5_000_000


class TestPositioningSummary:
    """Tests for PositioningSummary data class."""

    def test_positioning_summary_defaults(self):
        """Test PositioningSummary has correct defaults."""
        summary = PositioningSummary()
        assert summary.total_targets_analyzed == 0
        assert summary.high_value_corridors == 0
        assert summary.exchange_coverage_pct == 0.0
        assert summary.open_recommendations == 0
        assert summary.strengthen_recommendations == 0
        assert summary.atrophy_recommendations == 0

    def test_positioning_summary_to_dict(self):
        """Test PositioningSummary to_dict method."""
        summary = PositioningSummary(
            total_targets_analyzed=50,
            high_value_corridors=5,
            exchange_coverage_pct=60.0,
            open_recommendations=3,
            strengthen_recommendations=2,
            atrophy_recommendations=1,
            capital_to_redeploy_sats=10_000_000
        )
        result = summary.to_dict()
        assert result["total_targets_analyzed"] == 50
        assert result["high_value_corridors"] == 5
        assert result["exchange_coverage_pct"] == 60.0


# =============================================================================
# ROUTE VALUE ANALYZER TESTS
# =============================================================================

class TestRouteValueAnalyzer:
    """Tests for RouteValueAnalyzer."""

    def test_initialization(self):
        """Test RouteValueAnalyzer initializes correctly."""
        plugin = MockPlugin()
        analyzer = RouteValueAnalyzer(plugin=plugin)
        assert analyzer.plugin == plugin
        assert analyzer._our_pubkey is None

    def test_set_our_pubkey(self):
        """Test setting our pubkey."""
        plugin = MockPlugin()
        analyzer = RouteValueAnalyzer(plugin=plugin)
        analyzer.set_our_pubkey("our123")
        assert analyzer._our_pubkey == "our123"

    def test_analyze_corridor_high_value(self):
        """Test analyzing a high-value corridor."""
        plugin = MockPlugin()
        analyzer = RouteValueAnalyzer(plugin=plugin)

        corridor = analyzer.analyze_corridor(
            source_peer_id="source123",
            destination_peer_id="dest456",
            volume_sats=300_000_000,  # High volume (10M/day)
            source_alias="Source",
            destination_alias="ACINQ"
        )

        assert corridor.value_tier == "high"
        assert corridor.daily_volume_sats == 10_000_000
        assert corridor.accessible is True

    def test_analyze_corridor_medium_value(self):
        """Test analyzing a medium-value corridor."""
        plugin = MockPlugin()
        analyzer = RouteValueAnalyzer(plugin=plugin)

        corridor = analyzer.analyze_corridor(
            source_peer_id="source123",
            destination_peer_id="dest456",
            volume_sats=60_000_000  # Medium volume (2M/day)
        )

        assert corridor.value_tier == "medium"
        assert corridor.daily_volume_sats == 2_000_000

    def test_analyze_corridor_low_value(self):
        """Test analyzing a low-value corridor."""
        plugin = MockPlugin()
        analyzer = RouteValueAnalyzer(plugin=plugin)

        corridor = analyzer.analyze_corridor(
            source_peer_id="source123",
            destination_peer_id="dest456",
            volume_sats=15_000_000  # Low volume (500k/day)
        )

        assert corridor.value_tier == "low"
        assert corridor.daily_volume_sats == 500_000

    def test_is_exchange(self):
        """Test exchange detection."""
        plugin = MockPlugin()
        analyzer = RouteValueAnalyzer(plugin=plugin)

        # Known exchanges
        is_exchange, priority = analyzer._is_exchange("ACINQ")
        assert is_exchange is True
        assert priority == 1.0

        is_exchange, priority = analyzer._is_exchange("Kraken Lightning")
        assert is_exchange is True
        assert priority == 0.95

        # Not an exchange
        is_exchange, priority = analyzer._is_exchange("Random Node")
        assert is_exchange is False
        assert priority == 0.0

    def test_find_valuable_corridors_empty(self):
        """Test finding valuable corridors with no data."""
        plugin = MockPlugin()
        analyzer = RouteValueAnalyzer(plugin=plugin)

        corridors = analyzer.find_valuable_corridors()
        assert corridors == []

    def test_find_exchange_targets(self):
        """Test finding exchange targets."""
        plugin = MockPlugin()
        state_manager = MockStateManager()
        analyzer = RouteValueAnalyzer(
            plugin=plugin,
            state_manager=state_manager
        )

        targets = analyzer.find_exchange_targets()
        # Should list all priority exchanges
        assert len(targets) == len(PRIORITY_EXCHANGES)


# =============================================================================
# FLEET POSITIONING STRATEGY TESTS
# =============================================================================

class TestFleetPositioningStrategy:
    """Tests for FleetPositioningStrategy."""

    def test_initialization(self):
        """Test FleetPositioningStrategy initializes correctly."""
        plugin = MockPlugin()
        strategy = FleetPositioningStrategy(plugin=plugin)
        assert strategy.plugin == plugin
        assert strategy._recent_recommendations == {}

    def test_set_our_pubkey(self):
        """Test setting our pubkey propagates to route analyzer."""
        plugin = MockPlugin()
        route_analyzer = RouteValueAnalyzer(plugin=plugin)
        strategy = FleetPositioningStrategy(
            plugin=plugin,
            route_analyzer=route_analyzer
        )

        strategy.set_our_pubkey("our123")
        assert strategy._our_pubkey == "our123"
        assert route_analyzer._our_pubkey == "our123"

    def test_count_fleet_channels_to_target_no_state_manager(self):
        """Test counting fleet channels without state manager."""
        plugin = MockPlugin()
        strategy = FleetPositioningStrategy(plugin=plugin)

        count = strategy._count_fleet_channels_to_target("target123")
        assert count == 0

    def test_count_fleet_channels_to_target_with_coverage(self):
        """Test counting fleet channels with coverage."""
        plugin = MockPlugin()
        state_manager = MockStateManager()

        # Set up two members with channels to target
        state_manager.set_peer_state("member1", topology=["target123", "other"])
        state_manager.set_peer_state("member2", topology=["target123"])
        state_manager.set_peer_state("member3", topology=["other"])

        strategy = FleetPositioningStrategy(
            plugin=plugin,
            state_manager=state_manager
        )

        count = strategy._count_fleet_channels_to_target("target123")
        assert count == 2

    def test_select_best_member_for_target(self):
        """Test selecting best member for a target."""
        plugin = MockPlugin()
        state_manager = MockStateManager()

        # Member 1 already has channel to target
        state_manager.set_peer_state("member1", topology=["target123"])
        # Member 2 doesn't have channel, fewer existing channels
        state_manager.set_peer_state("member2", topology=["other1", "other2"])
        # Member 3 doesn't have channel, more existing channels
        state_manager.set_peer_state("member3", topology=["a", "b", "c", "d", "e"] * 10)

        strategy = FleetPositioningStrategy(
            plugin=plugin,
            state_manager=state_manager
        )

        best = strategy._select_best_member_for_target("target123")
        # Member 2 should be selected (doesn't have target, fewer channels)
        assert best == "member2"

    def test_recommend_next_open_cooldown(self):
        """Test recommendation cooldown."""
        plugin = MockPlugin()
        strategy = FleetPositioningStrategy(plugin=plugin)

        # Set recent recommendation
        strategy._recent_recommendations["fleet"] = time.time()

        # Should return None due to cooldown
        rec = strategy.recommend_next_open()
        assert rec is None

    def test_get_positioning_recommendations_empty(self):
        """Test getting recommendations with no corridors."""
        plugin = MockPlugin()
        route_analyzer = RouteValueAnalyzer(plugin=plugin)
        strategy = FleetPositioningStrategy(
            plugin=plugin,
            route_analyzer=route_analyzer
        )

        recs = strategy.get_positioning_recommendations()
        assert recs == []


# =============================================================================
# PHYSARUM CHANNEL MANAGER TESTS
# =============================================================================

class TestPhysarumChannelManager:
    """Tests for PhysarumChannelManager."""

    def test_initialization(self):
        """Test PhysarumChannelManager initializes correctly."""
        plugin = MockPlugin()
        manager = PhysarumChannelManager(plugin=plugin)
        assert manager.plugin == plugin
        assert manager._flow_history == {}

    def test_set_our_pubkey(self):
        """Test setting our pubkey."""
        plugin = MockPlugin()
        manager = PhysarumChannelManager(plugin=plugin)
        manager.set_our_pubkey("our123")
        assert manager._our_pubkey == "our123"

    def test_calculate_flow_intensity_no_channel(self):
        """Test flow intensity for non-existent channel."""
        plugin = MockPlugin()
        manager = PhysarumChannelManager(plugin=plugin)

        flow = manager.calculate_flow_intensity("nonexistent")
        assert flow == 0.0

    def test_calculate_flow_intensity(self):
        """Test flow intensity calculation."""
        plugin = MockPlugin()
        plugin.rpc.channels = [{
            "short_channel_id": "123x456x0",
            "total_msat": "10000000000msat",  # 10M sats
            "in_fulfilled_msat": "100000000000msat",  # 100M sats in
            "out_fulfilled_msat": "50000000000msat",  # 50M sats out
            "state": "CHANNELD_NORMAL",
            "peer_id": "peer123"
        }]

        manager = PhysarumChannelManager(plugin=plugin)
        flow = manager.calculate_flow_intensity("123x456x0")
        # (100M + 50M) / 30 days / 10M capacity = 0.5
        assert flow == 0.5

    def test_get_channel_recommendation_not_found(self):
        """Test recommendation for non-existent channel."""
        plugin = MockPlugin()
        manager = PhysarumChannelManager(plugin=plugin)

        rec = manager.get_channel_recommendation("nonexistent")
        assert rec.action == "hold"
        assert "not found" in rec.reason.lower()

    def test_get_channel_recommendation_strengthen(self):
        """Test strengthen recommendation for high-flow channel."""
        plugin = MockPlugin()
        # High flow channel - above strengthen threshold
        plugin.rpc.channels = [{
            "short_channel_id": "123x456x0",
            "total_msat": "10000000000msat",  # 10M sats
            "in_fulfilled_msat": "200000000000msat",  # High volume
            "out_fulfilled_msat": "200000000000msat",
            "state": "CHANNELD_NORMAL",
            "peer_id": "peer123"
        }]

        manager = PhysarumChannelManager(plugin=plugin)
        rec = manager.get_channel_recommendation("123x456x0")
        assert rec.action == "strengthen"
        assert rec.method == "splice_in"
        assert rec.splice_amount_sats > 0

    def test_get_channel_recommendation_hold(self):
        """Test hold recommendation for normal flow channel."""
        plugin = MockPlugin()
        # Normal flow channel - flow between atrophy (0.001) and strengthen (0.02)
        # With 10M sats capacity and monthly volume of 3M, flow = 3M/30/10M = 0.01
        plugin.rpc.channels = [{
            "short_channel_id": "123x456x0",
            "total_msat": "10000000000msat",  # 10M sats
            "in_fulfilled_msat": "2000000000msat",  # 2M sats in
            "out_fulfilled_msat": "1000000000msat",  # 1M sats out (3M total monthly)
            "state": "CHANNELD_NORMAL",
            "peer_id": "peer123"
        }]

        manager = PhysarumChannelManager(plugin=plugin)
        rec = manager.get_channel_recommendation("123x456x0")
        assert rec.action == "hold"

    def test_get_all_recommendations_filters_holds(self):
        """Test that get_all_recommendations filters out hold actions."""
        plugin = MockPlugin()
        plugin.rpc.channels = [
            {
                "short_channel_id": "123x456x0",
                "total_msat": "10000000000msat",
                "in_fulfilled_msat": "5000000000msat",  # Normal flow
                "out_fulfilled_msat": "5000000000msat",
                "state": "CHANNELD_NORMAL",
                "peer_id": "peer1"
            },
            {
                "short_channel_id": "789x012x0",
                "total_msat": "10000000000msat",
                "in_fulfilled_msat": "200000000000msat",  # High flow
                "out_fulfilled_msat": "200000000000msat",
                "state": "CHANNELD_NORMAL",
                "peer_id": "peer2"
            }
        ]

        manager = PhysarumChannelManager(plugin=plugin)
        recs = manager.get_all_recommendations()

        # Should only include non-hold recommendations
        for rec in recs:
            assert rec.action != "hold"


# =============================================================================
# STRATEGIC POSITIONING MANAGER TESTS
# =============================================================================

class TestStrategicPositioningManager:
    """Tests for StrategicPositioningManager."""

    def test_initialization(self):
        """Test StrategicPositioningManager initializes correctly."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        assert manager.plugin == plugin
        assert manager.route_analyzer is not None
        assert manager.positioning_strategy is not None
        assert manager.physarum_mgr is not None

    def test_set_our_pubkey(self):
        """Test setting our pubkey propagates to all components."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        manager.set_our_pubkey("our123")

        assert manager._our_pubkey == "our123"
        assert manager.route_analyzer._our_pubkey == "our123"
        assert manager.positioning_strategy._our_pubkey == "our123"
        assert manager.physarum_mgr._our_pubkey == "our123"

    def test_get_valuable_corridors(self):
        """Test getting valuable corridors."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        # Empty corridors
        corridors = manager.get_valuable_corridors()
        assert corridors == []

    def test_get_exchange_coverage(self):
        """Test getting exchange coverage."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        coverage = manager.get_exchange_coverage()

        assert "total_priority_exchanges" in coverage
        assert "covered_exchanges" in coverage
        assert "coverage_pct" in coverage
        assert "exchanges" in coverage

    def test_get_positioning_recommendations(self):
        """Test getting positioning recommendations."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        recs = manager.get_positioning_recommendations()
        assert isinstance(recs, list)

    def test_get_flow_recommendations_all(self):
        """Test getting flow recommendations for all channels."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        recs = manager.get_flow_recommendations()
        assert isinstance(recs, list)

    def test_get_flow_recommendations_specific_channel(self):
        """Test getting flow recommendation for specific channel."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        recs = manager.get_flow_recommendations(channel_id="123x456x0")
        assert isinstance(recs, list)
        # Should return one recommendation (even if channel not found)
        assert len(recs) == 1

    def test_report_flow_intensity(self):
        """Test reporting flow intensity."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        result = manager.report_flow_intensity(
            channel_id="123x456x0",
            peer_id="peer123",
            intensity=0.05
        )

        assert result["recorded"] is True
        assert result["channel_id"] == "123x456x0"
        assert result["intensity"] == 0.05
        assert result["history_entries"] == 1

    def test_report_flow_intensity_multiple(self):
        """Test reporting multiple flow intensities."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        # Report multiple times
        for i in range(5):
            result = manager.report_flow_intensity(
                channel_id="123x456x0",
                peer_id="peer123",
                intensity=0.01 * (i + 1)
            )

        assert result["history_entries"] == 5

    def test_get_positioning_summary(self):
        """Test getting positioning summary."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        summary = manager.get_positioning_summary()

        assert "total_targets_analyzed" in summary
        assert "high_value_corridors" in summary
        assert "exchange_coverage_pct" in summary
        assert "open_recommendations" in summary
        assert "strengthen_recommendations" in summary
        assert "atrophy_recommendations" in summary

    def test_get_status(self):
        """Test getting positioning status."""
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        status = manager.get_status()

        assert status["enabled"] is True
        assert "summary" in status
        assert "thresholds" in status
        assert "priority_exchanges" in status

        # Check thresholds
        thresholds = status["thresholds"]
        assert thresholds["strengthen_flow_threshold"] == STRENGTHEN_FLOW_THRESHOLD
        assert thresholds["atrophy_flow_threshold"] == ATROPHY_FLOW_THRESHOLD
        assert thresholds["max_members_per_target"] == MAX_MEMBERS_PER_TARGET


# =============================================================================
# CONSTANT VALIDATION TESTS
# =============================================================================

class TestConstants:
    """Tests for module constants."""

    def test_volume_thresholds_ordered(self):
        """Test volume thresholds are properly ordered."""
        assert HIGH_VALUE_VOLUME_SATS_DAILY > MEDIUM_VALUE_VOLUME_SATS_DAILY

    def test_competition_thresholds_ordered(self):
        """Test competition thresholds are properly ordered."""
        assert MEDIUM_COMPETITION_THRESHOLD > LOW_COMPETITION_THRESHOLD

    def test_flow_thresholds_ordered(self):
        """Test flow thresholds are properly ordered."""
        assert STRENGTHEN_FLOW_THRESHOLD > ATROPHY_FLOW_THRESHOLD

    def test_age_thresholds_ordered(self):
        """Test age thresholds are properly ordered."""
        assert MIN_CHANNEL_AGE_FOR_ATROPHY_DAYS > STIMULATE_GRACE_DAYS

    def test_priority_exchanges_valid(self):
        """Test priority exchanges have required fields."""
        for name, data in PRIORITY_EXCHANGES.items():
            assert "alias_patterns" in data
            assert "priority" in data
            assert isinstance(data["alias_patterns"], list)
            assert 0 <= data["priority"] <= 1.0

    def test_bonuses_positive(self):
        """Test bonus multipliers are positive."""
        assert EXCHANGE_PRIORITY_BONUS > 1.0
        assert MAX_MEMBERS_PER_TARGET >= 1


# =============================================================================
# RPC COMMAND HANDLER TESTS
# =============================================================================

class TestRpcCommandHandlers:
    """Tests for RPC command handlers in rpc_commands.py."""

    def test_valuable_corridors_handler(self):
        """Test valuable_corridors RPC handler."""
        from modules.rpc_commands import valuable_corridors, HiveContext

        # Create mock context
        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        ctx = MagicMock(spec=HiveContext)
        ctx.strategic_positioning_mgr = manager

        result = valuable_corridors(ctx, min_score=0.05)

        assert "corridors" in result
        assert "total_count" in result
        assert "by_value_tier" in result

    def test_valuable_corridors_handler_not_initialized(self):
        """Test valuable_corridors RPC handler when not initialized."""
        from modules.rpc_commands import valuable_corridors, HiveContext

        ctx = MagicMock(spec=HiveContext)
        ctx.strategic_positioning_mgr = None

        result = valuable_corridors(ctx)
        assert "error" in result

    def test_exchange_coverage_handler(self):
        """Test exchange_coverage RPC handler."""
        from modules.rpc_commands import exchange_coverage, HiveContext

        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        ctx = MagicMock(spec=HiveContext)
        ctx.strategic_positioning_mgr = manager

        result = exchange_coverage(ctx)

        assert "total_priority_exchanges" in result
        assert "covered_exchanges" in result

    def test_positioning_recommendations_handler(self):
        """Test positioning_recommendations RPC handler."""
        from modules.rpc_commands import positioning_recommendations, HiveContext

        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        ctx = MagicMock(spec=HiveContext)
        ctx.strategic_positioning_mgr = manager

        result = positioning_recommendations(ctx, count=3)

        assert "recommendations" in result
        assert "count" in result
        assert "by_priority" in result

    def test_flow_recommendations_handler(self):
        """Test flow_recommendations RPC handler."""
        from modules.rpc_commands import flow_recommendations, HiveContext

        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        ctx = MagicMock(spec=HiveContext)
        ctx.strategic_positioning_mgr = manager

        result = flow_recommendations(ctx)

        assert "recommendations" in result
        assert "by_action" in result

    def test_report_flow_intensity_handler(self):
        """Test report_flow_intensity RPC handler."""
        from modules.rpc_commands import report_flow_intensity, HiveContext

        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        mock_db = MagicMock()
        mock_db.get_member.return_value = {"tier": "member", "peer_id": "our_pubkey_123"}

        ctx = MagicMock(spec=HiveContext)
        ctx.strategic_positioning_mgr = manager
        ctx.our_pubkey = "our_pubkey_123"
        ctx.database = mock_db

        result = report_flow_intensity(
            ctx,
            channel_id="123x456x0",
            peer_id="peer123",
            intensity=0.05
        )

        assert result["recorded"] is True

    def test_positioning_summary_handler(self):
        """Test positioning_summary RPC handler."""
        from modules.rpc_commands import positioning_summary, HiveContext

        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        ctx = MagicMock(spec=HiveContext)
        ctx.strategic_positioning_mgr = manager

        result = positioning_summary(ctx)

        assert "total_targets_analyzed" in result

    def test_positioning_status_handler(self):
        """Test positioning_status RPC handler."""
        from modules.rpc_commands import positioning_status, HiveContext

        plugin = MockPlugin()
        manager = StrategicPositioningManager(plugin=plugin)

        ctx = MagicMock(spec=HiveContext)
        ctx.strategic_positioning_mgr = manager

        result = positioning_status(ctx)

        assert result["enabled"] is True
        assert "thresholds" in result
        assert "priority_exchanges" in result
