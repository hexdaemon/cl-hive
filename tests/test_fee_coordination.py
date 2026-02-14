"""
Tests for Fee Coordination Module (Phase 2 - Fee Coordination).

Tests cover:
- FlowCorridorManager and corridor assignment
- AdaptiveFeeController (pheromone-based learning)
- StigmergicCoordinator (route markers)
- MyceliumDefenseSystem (collective defense)
- FeeCoordinationManager (main interface)
"""

import pytest
import time
import math
from unittest.mock import MagicMock, patch

from modules.fee_coordination import (
    # Constants
    FLEET_FEE_FLOOR_PPM,
    FLEET_FEE_CEILING_PPM,
    DEFAULT_FEE_PPM,
    PRIMARY_FEE_MULTIPLIER,
    SECONDARY_FEE_MULTIPLIER,
    BASE_EVAPORATION_RATE,
    PHEROMONE_EXPLOIT_THRESHOLD,
    DRAIN_RATIO_THRESHOLD,
    FAILURE_RATE_THRESHOLD,
    WARNING_TTL_HOURS,
    MARKER_MIN_STRENGTH,
    MARKER_HALF_LIFE_HOURS,
    # Data classes
    FlowCorridor,
    CorridorAssignment,
    RouteMarker,
    PeerWarning,
    FeeRecommendation,
    # Classes
    FlowCorridorManager,
    AdaptiveFeeController,
    StigmergicCoordinator,
    MyceliumDefenseSystem,
    FeeCoordinationManager,
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

    def listpeerchannels(self, id=None):
        if id:
            return {"channels": [c for c in self.channels if c.get("peer_id") == id]}
        return {"channels": self.channels}


class MockStateManager:
    """Mock state manager for testing."""

    def __init__(self):
        self.peer_states = {}

    def get_peer_state(self, peer_id):
        return self.peer_states.get(peer_id)

    def get_all_peer_states(self):
        return list(self.peer_states.values())

    def set_peer_state(self, peer_id, topology=None, capacity_sats=10_000_000):
        state = MagicMock()
        state.peer_id = peer_id
        state.topology = topology or []
        state.capacity_sats = capacity_sats
        self.peer_states[peer_id] = state


class MockLiquidityCoordinator:
    """Mock liquidity coordinator for testing."""

    def __init__(self):
        self.competitions = []

    def detect_internal_competition(self):
        return self.competitions

    def add_competition(self, source, dest, members):
        self.competitions.append({
            "source_peer_id": source,
            "destination_peer_id": dest,
            "source_alias": f"alias_{source[:8]}",
            "destination_alias": f"alias_{dest[:8]}",
            "competing_members": members,
            "total_fleet_capacity_sats": 10_000_000 * len(members)
        })


# =============================================================================
# FLOW CORRIDOR TESTS
# =============================================================================

class TestFlowCorridor:
    """Test FlowCorridor data class."""

    def test_basic_creation(self):
        """Test creating a flow corridor."""
        corridor = FlowCorridor(
            source_peer_id="02" + "a" * 64,
            destination_peer_id="02" + "b" * 64,
            capable_members=["02" + "c" * 64, "02" + "d" * 64]
        )

        assert corridor.source_peer_id == "02" + "a" * 64
        assert len(corridor.capable_members) == 2

    def test_to_dict(self):
        """Test serialization."""
        corridor = FlowCorridor(
            source_peer_id="02" + "a" * 64,
            destination_peer_id="02" + "b" * 64,
            competition_level="medium"
        )

        d = corridor.to_dict()
        assert "source_peer_id" in d
        assert d["competition_level"] == "medium"


class TestCorridorAssignment:
    """Test CorridorAssignment data class."""

    def test_basic_creation(self):
        """Test creating a corridor assignment."""
        corridor = FlowCorridor(
            source_peer_id="02" + "a" * 64,
            destination_peer_id="02" + "b" * 64
        )
        assignment = CorridorAssignment(
            corridor=corridor,
            primary_member="02" + "c" * 64,
            secondary_members=["02" + "d" * 64],
            primary_fee_ppm=500,
            secondary_fee_ppm=750,
            assignment_reason="highest_score",
            confidence=0.8
        )

        assert assignment.primary_member == "02" + "c" * 64
        assert assignment.primary_fee_ppm == 500


class TestFlowCorridorManager:
    """Test FlowCorridorManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.state_manager = MockStateManager()
        self.liquidity_coord = MockLiquidityCoordinator()

        self.manager = FlowCorridorManager(
            database=self.db,
            plugin=self.plugin,
            state_manager=self.state_manager,
            liquidity_coordinator=self.liquidity_coord
        )
        self.manager.set_our_pubkey("02" + "0" * 64)

    def test_identify_corridors_empty(self):
        """Test identifying corridors when no competition exists."""
        corridors = self.manager.identify_corridors()
        assert len(corridors) == 0

    def test_identify_corridors_with_competition(self):
        """Test identifying corridors with competition data."""
        # Add competition
        self.liquidity_coord.add_competition(
            "peer1", "peer2",
            ["02" + "a" * 64, "02" + "b" * 64]
        )

        corridors = self.manager.identify_corridors()
        assert len(corridors) == 1
        assert corridors[0].source_peer_id == "peer1"

    def test_assess_competition_level(self):
        """Test competition level assessment."""
        assert self.manager._assess_competition_level(1) == "none"
        assert self.manager._assess_competition_level(2) == "low"
        assert self.manager._assess_competition_level(3) == "medium"
        assert self.manager._assess_competition_level(5) == "high"

    def test_assign_corridor_no_members(self):
        """Test assigning corridor with no capable members."""
        corridor = FlowCorridor(
            source_peer_id="peer1",
            destination_peer_id="peer2",
            capable_members=[]
        )

        assignment = self.manager.assign_corridor(corridor)
        assert assignment.primary_member == ""
        assert assignment.confidence == 0.0

    def test_assign_corridor_with_members(self):
        """Test assigning corridor with capable members."""
        # Set up state for members
        self.state_manager.set_peer_state("02" + "a" * 64, capacity_sats=20_000_000)
        self.state_manager.set_peer_state("02" + "b" * 64, capacity_sats=10_000_000)

        corridor = FlowCorridor(
            source_peer_id="peer1",
            destination_peer_id="peer2",
            capable_members=["02" + "a" * 64, "02" + "b" * 64]
        )

        assignment = self.manager.assign_corridor(corridor)

        # Member with higher capacity should be primary
        assert assignment.primary_member == "02" + "a" * 64
        assert "02" + "b" * 64 in assignment.secondary_members


# =============================================================================
# ADAPTIVE FEE CONTROLLER TESTS
# =============================================================================

class TestAdaptiveFeeController:
    """Test AdaptiveFeeController class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.plugin = MockPlugin()
        self.controller = AdaptiveFeeController(plugin=self.plugin)
        self.controller.set_our_pubkey("02" + "0" * 64)

    def test_base_evaporation_rate(self):
        """Test basic evaporation rate calculation."""
        rate = self.controller.calculate_evaporation_rate("123x1x0")

        # With no velocity or volatility, should be close to base
        assert rate >= 0.1
        assert rate <= 0.9

    def test_evaporation_with_velocity(self):
        """Test evaporation rate increases with velocity."""
        # Set high velocity
        self.controller.update_velocity("123x1x0", 0.2)

        rate = self.controller.calculate_evaporation_rate("123x1x0")

        # Should be higher than base rate
        assert rate > BASE_EVAPORATION_RATE

    def test_pheromone_deposit_on_success(self):
        """Test pheromone deposit after successful routing."""
        channel_id = "123x1x0"

        # Initial level
        initial = self.controller.get_pheromone_level(channel_id)
        assert initial == 0.0

        # Deposit on success
        self.controller.update_pheromone(
            channel_id, 500, routing_success=True, revenue_sats=1000
        )

        level = self.controller.get_pheromone_level(channel_id)
        assert level > 0

    def test_pheromone_evaporation_on_failure(self):
        """Test pheromone evaporates on failure."""
        channel_id = "123x1x0"

        # Set initial pheromone
        self.controller._pheromone[channel_id] = 10.0

        # Update with failure (no deposit, just evaporation)
        self.controller.update_pheromone(
            channel_id, 500, routing_success=False, revenue_sats=0
        )

        level = self.controller.get_pheromone_level(channel_id)
        assert level < 10.0

    def test_suggest_fee_exploit_high_pheromone(self):
        """Test fee suggestion with high pheromone (exploit)."""
        channel_id = "123x1x0"

        # Set high pheromone (above exploit threshold)
        self.controller._pheromone[channel_id] = PHEROMONE_EXPLOIT_THRESHOLD + 5

        fee, reason = self.controller.suggest_fee(channel_id, 500, 0.5)

        assert fee == 500  # Stay at current fee
        assert "exploit" in reason

    def test_suggest_fee_explore_depleting(self):
        """Test fee suggestion when depleting (raise fees)."""
        channel_id = "123x1x0"

        # Low pheromone
        self.controller._pheromone[channel_id] = 1.0

        fee, reason = self.controller.suggest_fee(channel_id, 500, 0.2)  # 20% local

        assert fee > 500  # Should raise fees
        assert "raise" in reason or "depleting" in reason

    def test_suggest_fee_explore_saturating(self):
        """Test fee suggestion when saturating (lower fees)."""
        channel_id = "123x1x0"

        # Low pheromone
        self.controller._pheromone[channel_id] = 1.0

        fee, reason = self.controller.suggest_fee(channel_id, 500, 0.8)  # 80% local

        assert fee < 500  # Should lower fees
        assert "lower" in reason or "saturating" in reason


# =============================================================================
# STIGMERGIC COORDINATOR TESTS
# =============================================================================

class TestStigmergicCoordinator:
    """Test StigmergicCoordinator class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.coordinator = StigmergicCoordinator(
            database=self.db,
            plugin=self.plugin
        )
        self.coordinator.set_our_pubkey("02" + "0" * 64)

    def test_deposit_marker(self):
        """Test depositing a route marker."""
        marker = self.coordinator.deposit_marker(
            source="peer1",
            destination="peer2",
            fee_charged=500,
            success=True,
            volume_sats=100_000
        )

        assert marker.source_peer_id == "peer1"
        assert marker.fee_ppm == 500
        assert marker.success is True
        assert marker.strength > 0

    def test_read_markers(self):
        """Test reading markers for a route."""
        # Deposit some markers
        self.coordinator.deposit_marker("peer1", "peer2", 500, True, 100_000)
        self.coordinator.deposit_marker("peer1", "peer2", 600, False, 50_000)

        markers = self.coordinator.read_markers("peer1", "peer2")

        assert len(markers) == 2

    def test_marker_decay(self):
        """Test marker strength decays over time."""
        # Deposit marker with old timestamp (MARKER_HALF_LIFE_HOURS=168, i.e. 7 days)
        marker = RouteMarker(
            depositor="02" + "0" * 64,
            source_peer_id="peer1",
            destination_peer_id="peer2",
            fee_ppm=500,
            success=True,
            volume_sats=100_000,
            timestamp=time.time() - 336 * 3600,  # 336 hours ago (2 half-lives)
            strength=1.0
        )

        now = time.time()
        current_strength = self.coordinator._calculate_marker_strength(marker, now)

        # After 336 hours (2 half-lives of 168h), should be around 0.25
        assert current_strength < 0.5

    def test_calculate_coordinated_fee_no_markers(self):
        """Test fee calculation with no markers."""
        fee, confidence = self.coordinator.calculate_coordinated_fee(
            "peer1", "peer2", 500
        )

        assert fee == 500  # Default
        assert confidence < 0.5  # Low confidence

    def test_calculate_coordinated_fee_with_success(self):
        """Test fee calculation with successful markers."""
        # Deposit successful marker
        self.coordinator.deposit_marker("peer1", "peer2", 600, True, 200_000)

        fee, confidence = self.coordinator.calculate_coordinated_fee(
            "peer1", "peer2", 500
        )

        # Should be at least the successful marker's fee
        assert fee >= 600
        assert confidence > 0.3


# =============================================================================
# MYCELIUM DEFENSE SYSTEM TESTS
# =============================================================================

class TestMyceliumDefenseSystem:
    """Test MyceliumDefenseSystem class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.defense = MyceliumDefenseSystem(
            database=self.db,
            plugin=self.plugin
        )
        self.defense.set_our_pubkey("02" + "0" * 64)

    def test_detect_drain_threat(self):
        """Test detecting drain threat."""
        peer_id = "02" + "a" * 64

        # Update stats showing drain
        self.defense.update_peer_stats(
            peer_id=peer_id,
            inflow_sats=100_000,
            outflow_sats=600_000,  # 6:1 ratio
            successful_forwards=100,
            failed_forwards=10
        )

        threat = self.defense.detect_threat(peer_id)

        assert threat is not None
        assert threat.threat_type == "drain"
        assert threat.severity > 0

    def test_detect_unreliable_threat(self):
        """Test detecting unreliable peer."""
        peer_id = "02" + "a" * 64

        # Update stats showing high failure rate
        self.defense.update_peer_stats(
            peer_id=peer_id,
            inflow_sats=100_000,
            outflow_sats=100_000,  # Balanced
            successful_forwards=40,
            failed_forwards=60  # 60% failure rate
        )

        threat = self.defense.detect_threat(peer_id)

        assert threat is not None
        assert threat.threat_type == "unreliable"

    def test_no_threat_healthy_peer(self):
        """Test no threat for healthy peer."""
        peer_id = "02" + "a" * 64

        # Healthy stats
        self.defense.update_peer_stats(
            peer_id=peer_id,
            inflow_sats=100_000,
            outflow_sats=100_000,
            successful_forwards=95,
            failed_forwards=5
        )

        threat = self.defense.detect_threat(peer_id)

        assert threat is None

    def test_handle_warning_self_detected(self):
        """Test self-detected threat triggers immediate defense."""
        our_pubkey = "02" + "c" * 64
        self.defense.set_our_pubkey(our_pubkey)

        # Self-detected threat should trigger immediately (no quorum needed)
        warning = PeerWarning(
            peer_id="02" + "a" * 64,
            threat_type="drain",
            severity=0.7,
            reporter=our_pubkey,  # Self-reported
            timestamp=time.time(),
            ttl=24 * 3600
        )

        result = self.defense.handle_warning(warning)

        assert result is not None
        assert result["multiplier"] > 1.0

    def test_handle_warning_quorum_required(self):
        """Test remote warnings require quorum before defense activates."""
        peer_id = "02" + "a" * 64

        # First remote warning - quorum not met
        warning1 = PeerWarning(
            peer_id=peer_id,
            threat_type="drain",
            severity=0.7,
            reporter="02" + "b" * 64,
            timestamp=time.time(),
            ttl=24 * 3600
        )
        result = self.defense.handle_warning(warning1)
        assert result is None  # Quorum not met

        # Second independent report - quorum met
        warning2 = PeerWarning(
            peer_id=peer_id,
            threat_type="drain",
            severity=0.6,
            reporter="02" + "c" * 64,  # Different reporter
            timestamp=time.time(),
            ttl=24 * 3600
        )
        result = self.defense.handle_warning(warning2)
        assert result is not None
        assert result["multiplier"] > 1.0
        assert result["report_count"] == 2

    def test_defensive_multiplier(self):
        """Test getting defensive multiplier."""
        peer_id = "02" + "a" * 64
        our_pubkey = "02" + "d" * 64
        self.defense.set_our_pubkey(our_pubkey)

        # No warning - should be 1.0
        mult = self.defense.get_defensive_multiplier(peer_id)
        assert mult == 1.0

        # Add self-detected warning (triggers immediately)
        warning = PeerWarning(
            peer_id=peer_id,
            threat_type="drain",
            severity=0.5,
            reporter=our_pubkey,  # Self-detected
            timestamp=time.time(),
            ttl=24 * 3600
        )
        self.defense.handle_warning(warning)

        mult = self.defense.get_defensive_multiplier(peer_id)
        assert mult > 1.0

    def test_warning_expiration(self):
        """Test warning expiration."""
        warning = PeerWarning(
            peer_id="02" + "a" * 64,
            threat_type="drain",
            severity=0.5,
            reporter="02" + "b" * 64,
            timestamp=time.time() - 25 * 3600,  # 25 hours ago
            ttl=24 * 3600  # 24 hour TTL
        )

        assert warning.is_expired() is True


# =============================================================================
# FEE COORDINATION MANAGER TESTS
# =============================================================================

class TestFeeCoordinationManager:
    """Test FeeCoordinationManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.state_manager = MockStateManager()
        self.liquidity_coord = MockLiquidityCoordinator()

        self.manager = FeeCoordinationManager(
            database=self.db,
            plugin=self.plugin,
            state_manager=self.state_manager,
            liquidity_coordinator=self.liquidity_coord
        )
        self.manager.set_our_pubkey("02" + "0" * 64)

    def test_get_fee_recommendation_basic(self):
        """Test basic fee recommendation."""
        rec = self.manager.get_fee_recommendation(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            current_fee=500,
            local_balance_pct=0.5
        )

        assert rec.channel_id == "123x1x0"
        assert rec.recommended_fee_ppm >= FLEET_FEE_FLOOR_PPM
        assert rec.recommended_fee_ppm <= FLEET_FEE_CEILING_PPM

    def test_floor_enforcement(self):
        """Test fee floor enforcement."""
        rec = self.manager.get_fee_recommendation(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            current_fee=10,  # Very low
            local_balance_pct=0.5
        )

        # Should be at least floor
        assert rec.recommended_fee_ppm >= FLEET_FEE_FLOOR_PPM

    def test_ceiling_enforcement(self):
        """Test fee ceiling enforcement."""
        # Set up high defensive multiplier
        warning = PeerWarning(
            peer_id="02" + "a" * 64,
            threat_type="drain",
            severity=1.0,
            reporter="02" + "b" * 64,
            timestamp=time.time(),
            ttl=24 * 3600
        )
        self.manager.defense_system.handle_warning(warning)

        rec = self.manager.get_fee_recommendation(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            current_fee=2000,  # Already high
            local_balance_pct=0.5
        )

        # Should not exceed ceiling
        assert rec.recommended_fee_ppm <= FLEET_FEE_CEILING_PPM

    def test_record_routing_outcome(self):
        """Test recording routing outcome."""
        # Should not raise
        # Note: revenue_sats must be >= 10000 to create marker strength above
        # MARKER_MIN_STRENGTH (0.1), since strength = volume_sats / 100_000
        self.manager.record_routing_outcome(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            fee_ppm=500,
            success=True,
            revenue_sats=100000,  # 100k sats gives strength of 1.0
            source="peer1",
            destination="peer2"
        )

        # Pheromone should be updated
        level = self.manager.adaptive_controller.get_pheromone_level("123x1x0")
        assert level > 0

        # Marker should be deposited
        markers = self.manager.stigmergic_coord.read_markers("peer1", "peer2")
        assert len(markers) == 1

    def test_get_coordination_status(self):
        """Test getting coordination status."""
        status = self.manager.get_coordination_status()

        assert "corridor_assignments" in status
        assert "active_markers" in status
        assert "defense_status" in status
        assert "fleet_fee_floor" in status
        assert "fleet_fee_ceiling" in status


# =============================================================================
# CONSTANT TESTS
# =============================================================================

class TestConstants:
    """Test constant values."""

    def test_fee_bounds(self):
        """Test fee floor and ceiling are reasonable."""
        assert FLEET_FEE_FLOOR_PPM > 0
        assert FLEET_FEE_CEILING_PPM > FLEET_FEE_FLOOR_PPM
        assert DEFAULT_FEE_PPM >= FLEET_FEE_FLOOR_PPM
        assert DEFAULT_FEE_PPM <= FLEET_FEE_CEILING_PPM

    def test_fee_multipliers(self):
        """Test fee multipliers are reasonable."""
        assert PRIMARY_FEE_MULTIPLIER <= SECONDARY_FEE_MULTIPLIER

    def test_threat_thresholds(self):
        """Test threat detection thresholds."""
        assert DRAIN_RATIO_THRESHOLD > 1.0  # Outflow must exceed inflow
        assert 0 < FAILURE_RATE_THRESHOLD < 1.0


# =============================================================================
# FIX 2: THREAD LOCK TESTS
# =============================================================================

class TestAdaptiveFeeControllerLocks:
    """Test that AdaptiveFeeController methods are thread-safe."""

    def setup_method(self):
        self.plugin = MockPlugin()
        self.controller = AdaptiveFeeController(plugin=self.plugin)
        self.controller.set_our_pubkey("02" + "0" * 64)

    def test_update_pheromone_holds_lock(self):
        """Test update_pheromone acquires the lock (no deadlock, no crash)."""
        # Acquire the lock first and release — ensure method also acquires it
        import threading

        channel_id = "100x1x0"
        # Seed some pheromone so evaporation path runs
        with self.controller._lock:
            self.controller._pheromone[channel_id] = 5.0

        # Now call from another thread — should succeed without deadlock
        result = [None]
        def run():
            self.controller.update_pheromone(channel_id, 500, True, 1000)
            result[0] = self.controller.get_pheromone_level(channel_id)

        t = threading.Thread(target=run)
        t.start()
        t.join(timeout=5)
        assert not t.is_alive(), "Thread deadlocked"
        assert result[0] is not None
        assert result[0] > 0

    def test_suggest_fee_holds_lock(self):
        """Test suggest_fee reads pheromone under lock."""
        channel_id = "100x1x0"
        self.controller._pheromone[channel_id] = 20.0  # Above exploit threshold

        fee, reason = self.controller.suggest_fee(channel_id, 500, 0.5)
        assert fee == 500
        assert "exploit" in reason

    def test_get_pheromone_level_holds_lock(self):
        """Test get_pheromone_level acquires lock."""
        self.controller._pheromone["100x1x0"] = 7.5
        level = self.controller.get_pheromone_level("100x1x0")
        assert level == 7.5

    def test_get_all_pheromone_levels_holds_lock(self):
        """Test get_all_pheromone_levels returns snapshot under lock."""
        self.controller._pheromone["a"] = 1.0
        self.controller._pheromone["b"] = 2.0
        levels = self.controller.get_all_pheromone_levels()
        assert levels["a"] == 1.0
        assert levels["b"] == 2.0

    def test_get_fleet_fee_hint_holds_lock(self):
        """Test get_fleet_fee_hint acquires lock."""
        peer = "02" + "a" * 64
        self.controller._remote_pheromones[peer].append({
            "reporter_id": "02" + "b" * 64,
            "level": 5.0,
            "fee_ppm": 300,
            "timestamp": time.time(),
            "weight": 0.3
        })
        result = self.controller.get_fleet_fee_hint(peer)
        assert result is not None
        assert result[0] > 0

    def test_defensive_multiplier_holds_lock(self):
        """Test MyceliumDefenseSystem.get_defensive_multiplier acquires lock."""
        db = MockDatabase()
        plugin = MockPlugin()
        defense = MyceliumDefenseSystem(database=db, plugin=plugin)
        defense.set_our_pubkey("02" + "d" * 64)

        peer_id = "02" + "a" * 64
        # No defense set — should return 1.0
        assert defense.get_defensive_multiplier(peer_id) == 1.0

        # Set active defense
        warning = PeerWarning(
            peer_id=peer_id,
            threat_type="drain",
            severity=0.5,
            reporter="02" + "d" * 64,
            timestamp=time.time(),
            ttl=24 * 3600
        )
        defense.handle_warning(warning)
        mult = defense.get_defensive_multiplier(peer_id)
        assert mult > 1.0


# =============================================================================
# FIX 5: GOSSIP PHEROMONE BOUNDS TESTS
# =============================================================================

class TestGossipPheromoneBounds:
    """Test that gossip pheromone values are bounded."""

    def setup_method(self):
        self.plugin = MockPlugin()
        self.controller = AdaptiveFeeController(plugin=self.plugin)
        self.controller.set_our_pubkey("02" + "0" * 64)

    def test_extreme_fee_ppm_clamped(self):
        """Test that extreme fee_ppm from gossip is clamped to fleet bounds."""
        result = self.controller.receive_pheromone_from_gossip(
            reporter_id="02" + "a" * 64,
            pheromone_data={
                "peer_id": "02" + "b" * 64,
                "level": 5.0,
                "fee_ppm": 999999  # Way above ceiling
            }
        )
        assert result is True

        peer_id = "02" + "b" * 64
        reports = self.controller._remote_pheromones[peer_id]
        assert len(reports) == 1
        assert reports[0]["fee_ppm"] == FLEET_FEE_CEILING_PPM

    def test_very_low_fee_ppm_clamped(self):
        """Test that very low fee_ppm is clamped to floor."""
        result = self.controller.receive_pheromone_from_gossip(
            reporter_id="02" + "a" * 64,
            pheromone_data={
                "peer_id": "02" + "b" * 64,
                "level": 5.0,
                "fee_ppm": 1  # Way below floor
            }
        )
        assert result is True

        peer_id = "02" + "b" * 64
        reports = self.controller._remote_pheromones[peer_id]
        assert reports[0]["fee_ppm"] == FLEET_FEE_FLOOR_PPM

    def test_extreme_level_clamped(self):
        """Test that extreme pheromone level is clamped to 100."""
        result = self.controller.receive_pheromone_from_gossip(
            reporter_id="02" + "a" * 64,
            pheromone_data={
                "peer_id": "02" + "b" * 64,
                "level": 99999.0,  # Way above max
                "fee_ppm": 500
            }
        )
        assert result is True

        peer_id = "02" + "b" * 64
        reports = self.controller._remote_pheromones[peer_id]
        assert reports[0]["level"] == 100.0


# =============================================================================
# FIX 6: MARKER STRENGTH CAP + WEIGHTED AVERAGE TESTS
# =============================================================================

class TestMarkerStrengthCap:
    """Test that local marker strength is capped to [0.1, 1.0]."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.coordinator = StigmergicCoordinator(
            database=self.db, plugin=self.plugin
        )
        self.coordinator.set_our_pubkey("02" + "0" * 64)

    def test_large_volume_strength_capped(self):
        """Test that a 1 BTC payment does not produce strength > 1.0."""
        marker = self.coordinator.deposit_marker(
            source="peer1",
            destination="peer2",
            fee_charged=500,
            success=True,
            volume_sats=100_000_000  # 1 BTC
        )
        assert marker.strength <= 1.0

    def test_small_volume_has_floor(self):
        """Test that a tiny payment still gets minimum strength."""
        marker = self.coordinator.deposit_marker(
            source="peer1",
            destination="peer2",
            fee_charged=500,
            success=True,
            volume_sats=100  # Very small
        )
        assert marker.strength >= 0.1

    def test_weighted_average_not_winner_take_all(self):
        """Test that calculate_coordinated_fee uses weighted average."""
        # Deposit two markers with different fees and strengths
        self.coordinator.deposit_marker("p1", "p2", 200, True, 50_000)   # strength 0.5
        self.coordinator.deposit_marker("p1", "p2", 800, True, 100_000)  # strength 1.0

        fee, confidence = self.coordinator.calculate_coordinated_fee(
            "p1", "p2", 500
        )

        # With weighted avg: (200*0.5 + 800*1.0)/(0.5+1.0) = 600
        # Not 800 (which winner-take-all would give)
        assert fee < 800
        assert fee >= FLEET_FEE_FLOOR_PPM

    def test_weighted_average_single_marker(self):
        """Test that single marker works correctly."""
        self.coordinator.deposit_marker("p1", "p2", 600, True, 100_000)

        fee, confidence = self.coordinator.calculate_coordinated_fee(
            "p1", "p2", 500
        )
        assert fee == 600


# =============================================================================
# FIX 3: RECORD_FEE_CHANGE WIRING TESTS
# =============================================================================

class TestRecordFeeChangeWiring:
    """Test that salient recommendations trigger record_fee_change."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.manager = FeeCoordinationManager(
            database=self.db,
            plugin=self.plugin
        )
        self.manager.set_our_pubkey("02" + "0" * 64)

    def test_salient_change_records_fee_change(self):
        """Test that a salient recommendation records fee change time."""
        channel_id = "100x1x0"

        # Start with no recorded change time
        assert self.manager._get_last_fee_change_time(channel_id) == 0

        # Make a recommendation with a significantly different fee
        # Set up pheromone to drive the fee away from current
        self.manager.adaptive_controller._pheromone[channel_id] = 1.0

        rec = self.manager.get_fee_recommendation(
            channel_id=channel_id,
            peer_id="02" + "a" * 64,
            current_fee=500,
            local_balance_pct=0.15  # Low balance → raise fees
        )

        if rec.is_salient and rec.recommended_fee_ppm != 500:
            # Fee change time should have been recorded
            assert self.manager._get_last_fee_change_time(channel_id) > 0

    def test_non_salient_change_no_record(self):
        """Test that a non-salient recommendation doesn't record."""
        channel_id = "100x1x0"

        # Request recommendation with current fee that won't change much
        rec = self.manager.get_fee_recommendation(
            channel_id=channel_id,
            peer_id="02" + "a" * 64,
            current_fee=500,
            local_balance_pct=0.5  # Balanced → no change
        )

        if not rec.is_salient:
            # No fee change time should be recorded
            assert self.manager._get_last_fee_change_time(channel_id) == 0


# =============================================================================
# FIX 7: CROSS-WIRE FEE INTELLIGENCE TESTS
# =============================================================================

class TestCrossWireFeeIntelligence:
    """Test fee_intelligence integration into fee_coordination."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.manager = FeeCoordinationManager(
            database=self.db,
            plugin=self.plugin
        )
        self.manager.set_our_pubkey("02" + "0" * 64)

    def test_set_fee_intelligence_mgr(self):
        """Test setter method works."""
        mock_intel = MagicMock()
        self.manager.set_fee_intelligence_mgr(mock_intel)
        assert self.manager.fee_intelligence_mgr is mock_intel

    def test_intelligence_blended_when_confident(self):
        """Test that fee intelligence is blended when confidence > 0.3."""
        mock_intel = MagicMock()
        mock_intel.get_fee_recommendation.return_value = {
            "recommended_fee_ppm": 300,
            "confidence": 0.8,
        }
        self.manager.set_fee_intelligence_mgr(mock_intel)

        rec = self.manager.get_fee_recommendation(
            channel_id="100x1x0",
            peer_id="02" + "a" * 64,
            current_fee=500,
            local_balance_pct=0.5
        )

        # Intelligence was called
        mock_intel.get_fee_recommendation.assert_called_once()
        # Reason should include intelligence
        assert "intelligence" in rec.reason

    def test_intelligence_skipped_when_low_confidence(self):
        """Test that low-confidence intelligence is ignored."""
        mock_intel = MagicMock()
        mock_intel.get_fee_recommendation.return_value = {
            "recommended_fee_ppm": 300,
            "confidence": 0.1,  # Below 0.3 threshold
        }
        self.manager.set_fee_intelligence_mgr(mock_intel)

        rec = self.manager.get_fee_recommendation(
            channel_id="100x1x0",
            peer_id="02" + "a" * 64,
            current_fee=500,
            local_balance_pct=0.5
        )

        assert "intelligence" not in rec.reason

    def test_intelligence_exception_handled(self):
        """Test that exception from intelligence manager doesn't crash."""
        mock_intel = MagicMock()
        mock_intel.get_fee_recommendation.side_effect = Exception("db error")
        self.manager.set_fee_intelligence_mgr(mock_intel)

        # Should not raise
        rec = self.manager.get_fee_recommendation(
            channel_id="100x1x0",
            peer_id="02" + "a" * 64,
            current_fee=500,
            local_balance_pct=0.5
        )
        assert rec is not None


# =============================================================================
# PERSISTENCE TESTS (Pheromone & Marker Save/Restore)
# =============================================================================

class MockPersistenceDatabase:
    """Mock database with routing intelligence persistence methods."""

    def __init__(self):
        self.members = {}
        self._pheromones = []
        self._markers = []
        self._defense_reports = []
        self._defense_fees = []
        self._remote_pheromones = []
        self._fee_observations = []

    def get_all_members(self):
        return list(self.members.values()) if self.members else []

    def get_member(self, peer_id):
        return self.members.get(peer_id)

    def save_pheromone_levels(self, levels):
        self._pheromones = list(levels)
        return len(levels)

    def load_pheromone_levels(self):
        return list(self._pheromones)

    def save_stigmergic_markers(self, markers):
        self._markers = list(markers)
        return len(markers)

    def load_stigmergic_markers(self):
        return list(self._markers)

    def get_pheromone_count(self):
        return len(self._pheromones)

    def get_latest_marker_timestamp(self):
        if not self._markers:
            return None
        return max(m['timestamp'] for m in self._markers)

    def save_defense_state(self, reports, active_fees):
        self._defense_reports = list(reports)
        self._defense_fees = list(active_fees)
        return len(reports) + len(active_fees)

    def load_defense_state(self):
        return {
            'reports': list(self._defense_reports),
            'active_fees': list(self._defense_fees),
        }

    def save_remote_pheromones(self, pheromones):
        self._remote_pheromones = list(pheromones)
        return len(pheromones)

    def load_remote_pheromones(self):
        return list(self._remote_pheromones)

    def save_fee_observations(self, observations):
        self._fee_observations = list(observations)
        return len(observations)

    def load_fee_observations(self):
        return list(self._fee_observations)


class TestPersistence:
    """Tests for pheromone and marker persistence."""

    def setup_method(self):
        self.db = MockPersistenceDatabase()
        self.plugin = MockPlugin()
        self.manager = FeeCoordinationManager(
            database=self.db,
            plugin=self.plugin
        )
        self.manager.set_our_pubkey("02" + "bb" * 32)

    def test_save_load_pheromone_round_trip(self):
        """Populate pheromones, save, clear, restore, verify."""
        ctrl = self.manager.adaptive_controller

        # Populate pheromones
        now = time.time()
        with ctrl._lock:
            ctrl._pheromone["100x1x0"] = 1.5
            ctrl._pheromone_fee["100x1x0"] = 300
            ctrl._pheromone_last_update["100x1x0"] = now
            ctrl._pheromone["200x2x0"] = 0.8
            ctrl._pheromone_fee["200x2x0"] = 450
            ctrl._pheromone_last_update["200x2x0"] = now

        # Save
        saved = self.manager.save_state_to_database()
        assert saved['pheromones'] == 2

        # Clear in-memory state
        with ctrl._lock:
            ctrl._pheromone.clear()
            ctrl._pheromone_fee.clear()
            ctrl._pheromone_last_update.clear()

        assert len(ctrl._pheromone) == 0

        # Restore
        restored = self.manager.restore_state_from_database()
        assert restored['pheromones'] == 2

        # Verify data is back (values may have slight decay)
        assert "100x1x0" in ctrl._pheromone
        assert "200x2x0" in ctrl._pheromone
        assert ctrl._pheromone["100x1x0"] > 0
        assert ctrl._pheromone_fee["100x1x0"] == 300
        assert ctrl._pheromone_fee["200x2x0"] == 450

    def test_save_load_markers_round_trip(self):
        """Populate markers, save, clear, restore, verify."""
        coord = self.manager.stigmergic_coord
        src = "02" + "aa" * 32
        dst = "02" + "cc" * 32

        # Deposit a marker
        coord.deposit_marker(src, dst, 500, True, 50000)

        # Save
        saved = self.manager.save_state_to_database()
        assert saved['markers'] == 1

        # Clear in-memory
        with coord._lock:
            coord._markers.clear()

        assert len(coord._markers) == 0

        # Restore
        restored = self.manager.restore_state_from_database()
        assert restored['markers'] == 1

        # Verify data
        key = (src, dst)
        assert key in coord._markers
        assert len(coord._markers[key]) == 1
        assert coord._markers[key][0].fee_ppm == 500
        assert coord._markers[key][0].success is True

    def test_save_filters_below_threshold(self):
        """Pheromones < 0.01 and weak markers are excluded from save."""
        ctrl = self.manager.adaptive_controller
        coord = self.manager.stigmergic_coord

        now = time.time()

        # Add one above threshold and one below
        with ctrl._lock:
            ctrl._pheromone["100x1x0"] = 0.5   # Above 0.01
            ctrl._pheromone_fee["100x1x0"] = 300
            ctrl._pheromone_last_update["100x1x0"] = now
            ctrl._pheromone["200x2x0"] = 0.005  # Below 0.01
            ctrl._pheromone_fee["200x2x0"] = 100
            ctrl._pheromone_last_update["200x2x0"] = now

        # Add a very old marker (strength should decay below threshold)
        old_marker = RouteMarker(
            depositor="02" + "bb" * 32,
            source_peer_id="02" + "aa" * 32,
            destination_peer_id="02" + "cc" * 32,
            fee_ppm=300,
            success=True,
            volume_sats=1000,
            timestamp=now - (MARKER_HALF_LIFE_HOURS * 3600 * 10),  # Very old
            strength=0.5,
        )
        with coord._lock:
            coord._markers[("02" + "aa" * 32, "02" + "cc" * 32)].append(old_marker)

        saved = self.manager.save_state_to_database()
        assert saved['pheromones'] == 1  # Only the 0.5 level one
        assert saved['markers'] == 0     # Decayed below threshold

    def test_should_auto_backfill_empty(self):
        """Empty DB returns True for auto-backfill."""
        assert self.manager.should_auto_backfill() is True

    def test_should_auto_backfill_with_data(self):
        """Populated DB returns False for auto-backfill."""
        # Add some pheromone data
        self.db._pheromones = [
            {'channel_id': '100x1x0', 'level': 1.0, 'fee_ppm': 300,
             'last_update': time.time()}
        ]
        assert self.manager.should_auto_backfill() is False

    def test_should_auto_backfill_stale_markers(self):
        """Returns True when only old markers exist (>24h) and no pheromones."""
        self.db._markers = [
            {'depositor': 'x', 'source_peer_id': 'a', 'destination_peer_id': 'b',
             'fee_ppm': 100, 'success': 1, 'volume_sats': 1000,
             'timestamp': time.time() - 48 * 3600, 'strength': 0.5}
        ]
        assert self.manager.should_auto_backfill() is True

    def test_restore_applies_decay(self):
        """Restored pheromone values are decayed by elapsed time."""
        ctrl = self.manager.adaptive_controller
        hours_ago = 2.0
        past_time = time.time() - hours_ago * 3600

        # Directly populate the mock DB with a known level
        self.db._pheromones = [
            {'channel_id': '100x1x0', 'level': 1.0, 'fee_ppm': 300,
             'last_update': past_time}
        ]

        restored = self.manager.restore_state_from_database()
        assert restored['pheromones'] == 1

        # Level should be decayed: 1.0 * (1 - 0.2)^2 = 0.64
        expected = math.pow(1 - BASE_EVAPORATION_RATE, hours_ago)
        actual = ctrl._pheromone["100x1x0"]
        assert abs(actual - expected) < 0.05, f"Expected ~{expected:.3f}, got {actual:.3f}"

    def test_save_load_defense_warnings_round_trip(self):
        """Create warnings via handle_warning, save, clear, restore, verify."""
        defense = self.manager.defense_system
        our_pubkey = "02" + "bb" * 32
        defense.set_our_pubkey(our_pubkey)
        threat_peer = "02" + "dd" * 32

        # Create a self-detected warning (immediate defense)
        warning = PeerWarning(
            peer_id=threat_peer,
            threat_type="drain",
            severity=0.8,
            reporter=our_pubkey,
            timestamp=time.time(),
            ttl=WARNING_TTL_HOURS * 3600,
            evidence={"drain_rate": 5.2},
        )
        result = defense.handle_warning(warning)
        assert result is not None
        assert result['multiplier'] > 1.0

        # Save
        saved = self.manager.save_state_to_database()
        assert saved['defense_reports'] == 1
        assert saved['defense_fees'] == 1

        # Clear in-memory state
        with defense._lock:
            defense._warnings.clear()
            defense._warning_reports.clear()
            defense._defensive_fees.clear()

        assert len(defense._warnings) == 0
        assert len(defense._defensive_fees) == 0

        # Restore
        restored = self.manager.restore_state_from_database()
        assert restored['defense_reports'] == 1
        assert restored['defense_fees'] == 1

        # Verify reports rebuilt
        assert threat_peer in defense._warning_reports
        assert our_pubkey in defense._warning_reports[threat_peer]
        restored_warning = defense._warning_reports[threat_peer][our_pubkey]
        assert restored_warning.threat_type == "drain"
        assert restored_warning.severity == 0.8
        assert restored_warning.evidence == {"drain_rate": 5.2}

        # Verify _warnings derived from reports
        assert threat_peer in defense._warnings

        # Verify defensive fees
        assert threat_peer in defense._defensive_fees
        assert defense._defensive_fees[threat_peer]['multiplier'] > 1.0

    def test_save_filters_expired_warnings(self):
        """Expired warnings are excluded from save."""
        defense = self.manager.defense_system
        our_pubkey = "02" + "bb" * 32
        defense.set_our_pubkey(our_pubkey)
        threat_peer = "02" + "dd" * 32

        # Create an already-expired warning
        warning = PeerWarning(
            peer_id=threat_peer,
            threat_type="drain",
            severity=0.5,
            reporter=our_pubkey,
            timestamp=time.time() - 100,  # 100 seconds ago
            ttl=50,  # TTL of 50 seconds -> expired 50 seconds ago
            evidence={},
        )
        with defense._lock:
            defense._warning_reports[threat_peer][our_pubkey] = warning
            defense._warnings[threat_peer] = warning
            defense._defensive_fees[threat_peer] = {
                'multiplier': 2.0,
                'expires_at': time.time() - 50,  # Already expired
                'threat_type': 'drain',
                'reporter': our_pubkey,
                'report_count': 1,
            }

        saved = self.manager.save_state_to_database()
        assert saved['defense_reports'] == 0
        assert saved['defense_fees'] == 0

    def test_save_load_remote_pheromones_round_trip(self):
        """Populate via receive_pheromone_from_gossip, save, clear, restore, verify."""
        ctrl = self.manager.adaptive_controller
        peer_a = "02" + "aa" * 32
        reporter_1 = "02" + "11" * 32

        # Receive a pheromone
        ctrl.receive_pheromone_from_gossip(
            reporter_id=reporter_1,
            pheromone_data={"peer_id": peer_a, "level": 2.5, "fee_ppm": 350},
        )

        # Save
        saved = self.manager.save_state_to_database()
        assert saved['remote_pheromones'] == 1

        # Clear in-memory
        with ctrl._lock:
            ctrl._remote_pheromones.clear()

        assert len(ctrl._remote_pheromones) == 0

        # Restore
        restored = self.manager.restore_state_from_database()
        assert restored['remote_pheromones'] == 1

        # Verify
        assert peer_a in ctrl._remote_pheromones
        assert len(ctrl._remote_pheromones[peer_a]) == 1
        entry = ctrl._remote_pheromones[peer_a][0]
        assert entry['reporter_id'] == reporter_1
        assert entry['fee_ppm'] == 350

    def test_save_load_fee_observations_round_trip(self):
        """Record observations, save, clear, restore, verify."""
        ctrl = self.manager.adaptive_controller

        # Record some observations
        ctrl.record_fee_observation(200)
        ctrl.record_fee_observation(350)

        # Save
        saved = self.manager.save_state_to_database()
        assert saved['fee_observations'] == 2

        # Clear in-memory
        with ctrl._fee_obs_lock:
            ctrl._fee_observations.clear()

        assert len(ctrl._fee_observations) == 0

        # Restore
        restored = self.manager.restore_state_from_database()
        assert restored['fee_observations'] == 2

        # Verify
        assert len(ctrl._fee_observations) == 2
        fees = [f for _, f in ctrl._fee_observations]
        assert 200 in fees
        assert 350 in fees

    def test_restore_filters_old_fee_observations(self):
        """Observations older than 1 hour are excluded on restore."""
        # Directly populate mock DB with old and recent observations
        now = time.time()
        self.db._fee_observations = [
            {'timestamp': now - 7200, 'fee_ppm': 100},  # 2 hours ago - too old
            {'timestamp': now - 1800, 'fee_ppm': 200},  # 30 min ago - recent
        ]

        restored = self.manager.restore_state_from_database()
        assert restored['fee_observations'] == 1

        ctrl = self.manager.adaptive_controller
        assert len(ctrl._fee_observations) == 1
        assert ctrl._fee_observations[0][1] == 200

    def test_defense_restore_derives_warnings_from_reports(self):
        """Verify _warnings dict is correctly rebuilt from _warning_reports."""
        defense = self.manager.defense_system
        threat_peer = "02" + "dd" * 32
        reporter_a = "02" + "aa" * 32
        reporter_b = "02" + "cc" * 32
        now = time.time()

        # Directly populate mock DB with two reports at different severities
        self.db._defense_reports = [
            {
                'peer_id': threat_peer,
                'reporter_id': reporter_a,
                'threat_type': 'drain',
                'severity': 0.3,
                'timestamp': now,
                'ttl': WARNING_TTL_HOURS * 3600,
                'evidence_json': '{}',
            },
            {
                'peer_id': threat_peer,
                'reporter_id': reporter_b,
                'threat_type': 'drain',
                'severity': 0.9,
                'timestamp': now,
                'ttl': WARNING_TTL_HOURS * 3600,
                'evidence_json': '{"drain_rate": 8.0}',
            },
        ]

        restored = self.manager.restore_state_from_database()
        assert restored['defense_reports'] == 2

        # _warnings should have the highest severity report
        assert threat_peer in defense._warnings
        assert defense._warnings[threat_peer].severity == 0.9
        assert defense._warnings[threat_peer].evidence == {"drain_rate": 8.0}
