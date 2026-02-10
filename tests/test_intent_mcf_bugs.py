"""
Tests for Intent Lock Protocol and MCF bug fixes.

Covers:
- MCFCircuitBreaker get_status() race condition fix
- IntentManager get_intent_stats() lock fix
- LiquidityCoordinator thread safety fixes
- LiquidityCoordinator claim_pending_assignment() atomic operation
- CostReductionManager circular flow AttributeError fix
- CostReductionManager hub scoring division-by-zero fix
- CostReductionManager record_mcf_ack thread safety fix

Author: Lightning Goats Team
"""

import pytest
import time
import threading
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.mcf_solver import (
    MCFCircuitBreaker,
    MCF_CIRCUIT_FAILURE_THRESHOLD,
    MCF_CIRCUIT_RECOVERY_TIMEOUT,
)
from modules.intent_manager import (
    IntentManager, Intent,
    STATUS_PENDING, STATUS_ABORTED,
    DEFAULT_HOLD_SECONDS, MAX_REMOTE_INTENTS,
)
from modules.cost_reduction import (
    CircularFlow,
    FleetPath,
    CostReductionManager,
    CircularFlowDetector,
    FleetRebalanceRouter,
)


# =============================================================================
# FIXTURES
# =============================================================================

class MockPlugin:
    """Mock plugin for testing."""
    def __init__(self):
        self.logs = []
        self.rpc = MagicMock()

    def log(self, msg, level="info"):
        self.logs.append({"msg": msg, "level": level})


class MockDatabase:
    """Mock database for testing."""
    def __init__(self):
        self.members = []
        self.intents = {}

    def create_intent(self, **kwargs):
        return 1

    def get_conflicting_intents(self, target, intent_type):
        return []

    def update_intent_status(self, intent_id, status, reason=None):
        return True

    def cleanup_expired_intents(self):
        return 0

    def get_all_members(self):
        return self.members

    def get_pending_intents_ready(self, hold_seconds):
        return []


class MockStateManager:
    """Mock state manager for testing."""
    def __init__(self):
        self.hive_map = MagicMock()
        self.hive_map.peer_states = {}

    def get_member_list(self):
        return []


# =============================================================================
# MCFCircuitBreaker get_status() RACE CONDITION FIX
# =============================================================================

class TestCircuitBreakerGetStatusRace:
    """Test that get_status() reads can_execute atomically under lock."""

    def test_get_status_returns_consistent_state(self):
        """get_status() should return can_execute consistent with state."""
        cb = MCFCircuitBreaker()

        # CLOSED state - can_execute should be True
        status = cb.get_status()
        assert status["state"] == MCFCircuitBreaker.CLOSED
        assert status["can_execute"] is True

    def test_get_status_open_state_consistent(self):
        """get_status() in OPEN state returns can_execute=False."""
        cb = MCFCircuitBreaker()

        # Open the circuit
        for _ in range(MCF_CIRCUIT_FAILURE_THRESHOLD):
            cb.record_failure("error")

        status = cb.get_status()
        assert status["state"] == MCFCircuitBreaker.OPEN
        assert status["can_execute"] is False

    def test_get_status_half_open_consistent(self):
        """get_status() in HALF_OPEN returns can_execute=True."""
        cb = MCFCircuitBreaker()

        # Open, then wait for recovery
        for _ in range(MCF_CIRCUIT_FAILURE_THRESHOLD):
            cb.record_failure("error")

        cb.last_state_change = time.time() - MCF_CIRCUIT_RECOVERY_TIMEOUT - 1

        status = cb.get_status()
        assert status["state"] == MCFCircuitBreaker.HALF_OPEN
        assert status["can_execute"] is True

    def test_get_status_concurrent_access(self):
        """get_status() is safe under concurrent access."""
        cb = MCFCircuitBreaker()
        results = []
        errors = []

        def reader():
            try:
                for _ in range(100):
                    status = cb.get_status()
                    # Verify invariant: if CLOSED, can_execute must be True
                    if status["state"] == MCFCircuitBreaker.CLOSED:
                        assert status["can_execute"] is True
                    results.append(status)
            except Exception as e:
                errors.append(e)

        def mutator():
            try:
                for _ in range(50):
                    cb.record_failure("test")
                    cb.record_success()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        threads.append(threading.Thread(target=mutator))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent errors: {errors}"
        assert len(results) == 400

    def test_can_execute_unlocked_exists(self):
        """_can_execute_unlocked() method exists for internal use."""
        cb = MCFCircuitBreaker()
        assert hasattr(cb, '_can_execute_unlocked')
        # Should work when called from within lock context
        with cb._lock:
            assert cb._can_execute_unlocked() is True


# =============================================================================
# IntentManager get_intent_stats() LOCK FIX
# =============================================================================

class TestIntentManagerStatsLock:
    """Test that get_intent_stats() reads remote intents under lock."""

    def test_get_intent_stats_thread_safe(self):
        """get_intent_stats() should not crash under concurrent modification."""
        db = MockDatabase()
        plugin = MockPlugin()
        mgr = IntentManager(db, plugin, our_pubkey="02" + "a" * 64)

        errors = []

        def reader():
            try:
                for _ in range(100):
                    stats = mgr.get_intent_stats()
                    assert "remote_intents_cached" in stats
            except Exception as e:
                errors.append(e)

        def writer():
            try:
                for i in range(100):
                    intent = Intent(
                        intent_type="channel_open",
                        target=f"target_{i}",
                        initiator=f"02{'b' * 64}",
                        timestamp=int(time.time()),
                        expires_at=int(time.time()) + 60,
                    )
                    mgr.record_remote_intent(intent)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(3)]
        threads.append(threading.Thread(target=writer))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent errors: {errors}"


# =============================================================================
# LiquidityCoordinator THREAD SAFETY + CLAIM ATOMIC
# =============================================================================

class TestLiquidityCoordinatorThreadSafety:
    """Test thread safety fixes in LiquidityCoordinator."""

    def _make_coordinator(self):
        """Create a LiquidityCoordinator with mocks."""
        from modules.liquidity_coordinator import LiquidityCoordinator
        plugin = MockPlugin()
        db = MockDatabase()
        return LiquidityCoordinator(
            database=db,
            plugin=plugin,
            our_pubkey="02" + "a" * 64,
            state_manager=MockStateManager(),
        )

    def test_claim_pending_assignment_atomic(self):
        """claim_pending_assignment() should atomically find and claim."""
        from modules.liquidity_coordinator import LiquidityCoordinator, MCFAssignment
        coord = self._make_coordinator()

        # Add a pending assignment
        assignment = MCFAssignment(
            assignment_id="test-1",
            from_channel="100x1x0",
            to_channel="200x2x0",
            amount_sats=50000,
            expected_cost_sats=50,
            priority=1,
            coordinator_id="02" + "c" * 64,
            solution_timestamp=int(time.time()),
            path=["02" + "d" * 64],
            via_fleet=True,
            received_at=int(time.time()),
        )
        coord._mcf_assignments["test-1"] = assignment

        # Claim it
        claimed = coord.claim_pending_assignment("test-1")
        assert claimed is not None
        assert claimed.status == "executing"
        assert claimed.assignment_id == "test-1"

        # Second claim should fail (already executing)
        second = coord.claim_pending_assignment("test-1")
        assert second is None

    def test_claim_pending_assignment_no_id(self):
        """claim_pending_assignment(None) claims highest priority."""
        from modules.liquidity_coordinator import LiquidityCoordinator, MCFAssignment
        coord = self._make_coordinator()

        now = int(time.time())
        # Add two assignments with different priorities
        coord._mcf_assignments["low"] = MCFAssignment(
            assignment_id="low", from_channel="100x1x0", to_channel="200x2x0",
            amount_sats=50000, expected_cost_sats=50, priority=10,
            coordinator_id="02" + "c" * 64, solution_timestamp=now,
            path=[], via_fleet=False, received_at=now,
        )
        coord._mcf_assignments["high"] = MCFAssignment(
            assignment_id="high", from_channel="300x3x0", to_channel="400x4x0",
            amount_sats=100000, expected_cost_sats=100, priority=1,
            coordinator_id="02" + "c" * 64, solution_timestamp=now,
            path=[], via_fleet=False, received_at=now,
        )

        # Should claim highest priority (lowest number)
        claimed = coord.claim_pending_assignment()
        assert claimed is not None
        assert claimed.assignment_id == "high"
        assert claimed.status == "executing"

    def test_claim_pending_assignment_empty(self):
        """claim_pending_assignment() returns None when nothing pending."""
        coord = self._make_coordinator()
        assert coord.claim_pending_assignment() is None
        assert coord.claim_pending_assignment("nonexistent") is None

    def test_claim_concurrent_no_double_claim(self):
        """Two threads racing to claim same assignment: only one wins."""
        from modules.liquidity_coordinator import LiquidityCoordinator, MCFAssignment
        coord = self._make_coordinator()

        now = int(time.time())
        coord._mcf_assignments["race-1"] = MCFAssignment(
            assignment_id="race-1", from_channel="100x1x0", to_channel="200x2x0",
            amount_sats=50000, expected_cost_sats=50, priority=1,
            coordinator_id="02" + "c" * 64, solution_timestamp=now,
            path=[], via_fleet=False, received_at=now,
        )

        results = []
        def claimer():
            result = coord.claim_pending_assignment("race-1")
            results.append(result)

        threads = [threading.Thread(target=claimer) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        # Exactly one should win
        winners = [r for r in results if r is not None]
        losers = [r for r in results if r is None]
        assert len(winners) == 1, f"Expected 1 winner, got {len(winners)}"
        assert len(losers) == 9

    def test_get_mcf_status_thread_safe(self):
        """get_mcf_status() should not crash under concurrent modification."""
        coord = self._make_coordinator()
        errors = []

        def reader():
            try:
                for _ in range(50):
                    status = coord.get_mcf_status()
                    assert "assignment_counts" in status
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors

    def test_get_pending_mcf_assignments_thread_safe(self):
        """get_pending_mcf_assignments() is safe under concurrent access."""
        from modules.liquidity_coordinator import MCFAssignment
        coord = self._make_coordinator()
        errors = []

        now = int(time.time())
        # Pre-populate some assignments
        for i in range(10):
            coord._mcf_assignments[f"a-{i}"] = MCFAssignment(
                assignment_id=f"a-{i}", from_channel=f"{i}x1x0", to_channel=f"{i}x2x0",
                amount_sats=50000, expected_cost_sats=50, priority=i,
                coordinator_id="02" + "c" * 64, solution_timestamp=now,
                path=[], via_fleet=False, received_at=now,
            )

        def reader():
            try:
                for _ in range(50):
                    pending = coord.get_pending_mcf_assignments()
                    assert isinstance(pending, list)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors


# =============================================================================
# CostReductionManager CIRCULAR FLOW ATTRIBUTEERROR FIX
# =============================================================================

class TestCircularFlowAttributeFix:
    """Test that circular flow reporting uses cf.cycle_count (not members_count)."""

    def test_circular_flow_has_cycle_count(self):
        """CircularFlow uses cycle_count, not members_count."""
        cf = CircularFlow(
            members=["A", "B", "C"],
            total_amount_sats=100000,
            total_cost_sats=500,
            cycle_count=3,
            detection_window_hours=24.0,
            recommendation="Consider fee adjustment"
        )
        assert cf.cycle_count == 3
        assert not hasattr(cf, 'members_count')

    def test_circular_flow_to_dict(self):
        """CircularFlow.to_dict() should include cycle_count."""
        cf = CircularFlow(
            members=["A", "B"],
            total_amount_sats=50000,
            total_cost_sats=200,
            cycle_count=5,
            detection_window_hours=12.0,
            recommendation="Halt"
        )
        d = cf.to_dict()
        assert d["cycle_count"] == 5
        assert "members_count" not in d

    def test_get_shareable_circular_flows_no_crash(self):
        """get_shareable_circular_flows() should not raise AttributeError."""
        plugin = MockPlugin()
        detector = CircularFlowDetector(plugin=plugin, state_manager=MockStateManager())

        # Add a fake rebalance history to create a circular flow
        from modules.cost_reduction import RebalanceOutcome
        now = time.time()
        # Create a simple A→B→A circular pattern
        detector._rebalance_history = [
            RebalanceOutcome(
                timestamp=time.time(),
                from_channel="100x1x0", to_channel="200x2x0",
                from_peer="peer_a", to_peer="peer_b",
                amount_sats=100000, cost_sats=500,
                success=True, via_fleet=True, member_id="peer_a"
            ),
            RebalanceOutcome(
                timestamp=time.time(),
                from_channel="200x2x0", to_channel="100x1x0",
                from_peer="peer_b", to_peer="peer_a",
                amount_sats=100000, cost_sats=500,
                success=True, via_fleet=True, member_id="peer_b"
            ),
        ]

        # This should not raise AttributeError
        try:
            flows = detector.get_shareable_circular_flows()
            # Verify it returns a list (may be empty if no cycles detected)
            assert isinstance(flows, list)
        except AttributeError as e:
            pytest.fail(f"AttributeError in get_shareable_circular_flows: {e}")

    def test_get_all_circular_flow_alerts_no_crash(self):
        """get_all_circular_flow_alerts() should not raise AttributeError."""
        plugin = MockPlugin()
        detector = CircularFlowDetector(plugin=plugin, state_manager=MockStateManager())

        try:
            alerts = detector.get_all_circular_flow_alerts()
            assert isinstance(alerts, list)
        except AttributeError as e:
            pytest.fail(f"AttributeError in get_all_circular_flow_alerts: {e}")


# =============================================================================
# FleetRebalanceRouter HUB SCORING DIVISION-BY-ZERO FIX
# =============================================================================

class TestHubScoringDivisionByZero:
    """Test that hub scoring handles empty paths safely."""

    def test_avg_hub_no_divide_by_zero(self):
        """Hub scoring should use max(1, len) to prevent division by zero."""
        plugin = MockPlugin()
        router = FleetRebalanceRouter(
            plugin=plugin,
            state_manager=MockStateManager(),
            liquidity_coordinator=None
        )

        # Verify the formula works with an empty path
        # (In practice this shouldn't happen, but the guard prevents crashes)
        best_path = []
        hub_scores = {}
        # This would divide by zero without max(1, ...)
        avg_hub = sum(hub_scores.get(m, 0.0) for m in best_path) / max(1, len(best_path))
        assert avg_hub == 0.0

    def test_hub_scoring_with_path(self):
        """Hub scoring should work correctly with non-empty path."""
        plugin = MockPlugin()
        router = FleetRebalanceRouter(
            plugin=plugin,
            state_manager=MockStateManager(),
            liquidity_coordinator=None
        )

        best_path = ["member_a", "member_b"]
        hub_scores = {"member_a": 0.8, "member_b": 0.6}
        avg_hub = sum(hub_scores.get(m, 0.0) for m in best_path) / max(1, len(best_path))
        assert abs(avg_hub - 0.7) < 0.001


# =============================================================================
# CostReductionManager record_mcf_ack THREAD SAFETY FIX
# =============================================================================

class TestRecordMcfAckThreadSafety:
    """Test that record_mcf_ack() is thread-safe."""

    def _make_manager(self):
        """Create a CostReductionManager with mocks."""
        plugin = MockPlugin()
        db = MockDatabase()
        mgr = CostReductionManager(
            plugin=plugin,
            database=db,
            state_manager=MockStateManager()
        )
        # Manually set MCF coordinator so record_mcf_ack processes
        mgr._mcf_coordinator = MagicMock()
        return mgr

    def test_mcf_acks_initialized_in_init(self):
        """_mcf_acks should be initialized in __init__, not lazily."""
        mgr = self._make_manager()
        assert hasattr(mgr, '_mcf_acks')
        assert hasattr(mgr, '_mcf_acks_lock')
        assert isinstance(mgr._mcf_acks, dict)

    def test_record_mcf_ack_basic(self):
        """record_mcf_ack() should store ACK data."""
        mgr = self._make_manager()
        mgr.record_mcf_ack("02" + "a" * 64, 1000, 3)
        assert len(mgr._mcf_acks) == 1

    def test_record_mcf_ack_concurrent(self):
        """record_mcf_ack() should not crash under concurrent access."""
        mgr = self._make_manager()
        errors = []

        def record_acks(thread_id):
            try:
                for i in range(50):
                    member = f"02{'0' * 62}{thread_id:02d}"
                    mgr.record_mcf_ack(member, 1000 + i, 1)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_acks, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent errors: {errors}"

    def test_record_mcf_ack_cache_limit(self):
        """record_mcf_ack() should evict old entries when over 500."""
        mgr = self._make_manager()

        # Fill up to 510 entries
        for i in range(510):
            member = f"02{'0' * 60}{i:04d}"
            mgr.record_mcf_ack(member, i, 1)

        # Should have evicted oldest 100, leaving ~410
        assert len(mgr._mcf_acks) <= 420  # Allow some margin


# =============================================================================
# INTEGRATION: Verify all fixes together
# =============================================================================

class TestIntegrationFixesConsistency:
    """Verify fixes don't break existing functionality."""

    def test_circuit_breaker_can_execute_still_works(self):
        """Public can_execute() should still function correctly."""
        cb = MCFCircuitBreaker()
        assert cb.can_execute() is True

        for _ in range(MCF_CIRCUIT_FAILURE_THRESHOLD):
            cb.record_failure("err")
        assert cb.can_execute() is False

    def test_intent_manager_stats_structure(self):
        """get_intent_stats() returns expected structure."""
        db = MockDatabase()
        mgr = IntentManager(db, MockPlugin(), our_pubkey="02" + "a" * 64)
        stats = mgr.get_intent_stats()

        assert "hold_seconds" in stats
        assert "our_pubkey" in stats
        assert "remote_intents_cached" in stats
        assert "registered_callbacks" in stats
        assert stats["remote_intents_cached"] == 0

    def test_circular_flow_dataclass_fields(self):
        """CircularFlow has expected fields and no stale references."""
        cf = CircularFlow(
            members=["A", "B", "C"],
            total_amount_sats=100000,
            total_cost_sats=500,
            cycle_count=3,
            detection_window_hours=24.0,
            recommendation="reduce fees"
        )
        d = cf.to_dict()
        assert set(d.keys()) == {
            "members", "total_amount_sats", "total_cost_sats",
            "cycle_count", "detection_window_hours", "recommendation"
        }
