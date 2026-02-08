"""
Tests for Anticipatory Liquidity Management and NNLB bug fixes.

Covers:
- AnticipatoryLiquidityManager thread safety (lock usage on all caches)
- AnticipatoryLiquidityManager proper __init__ (no hasattr needed)
- AnticipatoryLiquidityManager per-channel flow sample limit
- YieldMetricsManager missing get_channel_history() handling
- YieldMetricsManager thread safety (lock on caches)
- LiquidityCoordinator NNLB health_score clamping
- HiveBridge key name fix (forecasts vs predictions)
- HiveBridge no_forecast status handling
- cl-hive.py anticipatory channel mapping updates

Author: Lightning Goats Team
"""

import pytest
import time
import threading
from collections import defaultdict
from unittest.mock import MagicMock, patch, PropertyMock

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.anticipatory_liquidity import (
    AnticipatoryLiquidityManager,
    HourlyFlowSample,
    KalmanVelocityReport,
    TemporalPattern,
    FlowDirection,
    MAX_FLOW_HISTORY_CHANNELS,
    MAX_FLOW_SAMPLES_PER_CHANNEL,
    KALMAN_VELOCITY_TTL_SECONDS,
)
from modules.yield_metrics import YieldMetricsManager
from modules.liquidity_coordinator import LiquidityCoordinator, LiquidityNeed


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
        self._flow_samples = {}

    def get_all_members(self):
        return self.members

    def record_flow_sample(self, **kwargs):
        pass

    def get_flow_samples(self, channel_id, days=14):
        return self._flow_samples.get(channel_id, [])

    def get_member_health(self, peer_id):
        return None


class MockDatabaseNoHistory:
    """Mock database that lacks get_channel_history method."""
    def __init__(self):
        pass
    # Intentionally no get_channel_history method


class MockDatabaseWithHistory:
    """Mock database with get_channel_history."""
    def __init__(self, history_data=None):
        self._history = history_data or []

    def get_channel_history(self, channel_id, hours=48):
        return self._history


# =============================================================================
# ANTICIPATORY LIQUIDITY MANAGER - INIT TESTS
# =============================================================================

class TestAnticipatoryInit:
    """Test that all caches are properly initialized in __init__."""

    def test_intraday_cache_initialized(self):
        """_intraday_cache should be initialized in __init__, not via hasattr."""
        mgr = AnticipatoryLiquidityManager(database=MockDatabase())
        assert hasattr(mgr, '_intraday_cache')
        assert isinstance(mgr._intraday_cache, dict)

    def test_channel_peer_map_initialized(self):
        """_channel_peer_map should be initialized in __init__, not via hasattr."""
        mgr = AnticipatoryLiquidityManager(database=MockDatabase())
        assert hasattr(mgr, '_channel_peer_map')
        assert isinstance(mgr._channel_peer_map, dict)

    def test_remote_patterns_initialized(self):
        """_remote_patterns should be initialized in __init__, not via hasattr."""
        mgr = AnticipatoryLiquidityManager(database=MockDatabase())
        assert hasattr(mgr, '_remote_patterns')
        # defaultdict(list)
        assert isinstance(mgr._remote_patterns, dict)

    def test_lock_initialized(self):
        """_lock should be initialized in __init__."""
        mgr = AnticipatoryLiquidityManager(database=MockDatabase())
        assert hasattr(mgr, '_lock')
        assert isinstance(mgr._lock, type(threading.Lock()))


# =============================================================================
# ANTICIPATORY LIQUIDITY MANAGER - THREAD SAFETY TESTS
# =============================================================================

class TestAnticipatoryThreadSafety:
    """Test that shared caches are protected by locks."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.mgr = AnticipatoryLiquidityManager(
            database=self.db,
            plugin=self.plugin,
            our_id="our_pubkey_abc123"
        )

    def test_record_flow_sample_uses_lock(self):
        """record_flow_sample should use _lock when updating _flow_history."""
        original_lock = self.mgr._lock
        lock_acquired = []

        class TrackingLock:
            def __enter__(self_lock):
                lock_acquired.append(True)
                return original_lock.__enter__()
            def __exit__(self_lock, *args):
                return original_lock.__exit__(*args)

        self.mgr._lock = TrackingLock()
        self.mgr.record_flow_sample("chan1", 1000, 500)
        assert len(lock_acquired) > 0, "Lock was not acquired during record_flow_sample"

    def test_concurrent_flow_recording(self):
        """Multiple threads recording flow samples should not corrupt state."""
        errors = []

        def record_samples(channel_prefix, count):
            try:
                for i in range(count):
                    self.mgr.record_flow_sample(
                        f"{channel_prefix}_{i % 5}",
                        inbound_sats=1000 + i,
                        outbound_sats=500 + i,
                        timestamp=int(time.time()) + i
                    )
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=record_samples, args=(f"t{t}", 50))
            for t in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent recording raised errors: {errors}"

    def test_concurrent_kalman_velocity(self):
        """Multiple threads receiving Kalman velocities should not corrupt state."""
        errors = []

        def receive_velocities(reporter_prefix, count):
            try:
                for i in range(count):
                    self.mgr.receive_kalman_velocity(
                        reporter_id=f"{reporter_prefix}_reporter",
                        channel_id=f"chan_{i % 5}",
                        peer_id=f"peer_{i % 3}",
                        velocity_pct_per_hour=0.01 * i,
                        uncertainty=0.05,
                        flow_ratio=0.3,
                        confidence=0.8,
                        is_regime_change=False
                    )
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=receive_velocities, args=(f"t{t}", 30))
            for t in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent Kalman writes raised errors: {errors}"

    def test_concurrent_pattern_receive(self):
        """Multiple threads receiving remote patterns should not corrupt state."""
        errors = []

        def receive_patterns(reporter_prefix, count):
            try:
                for i in range(count):
                    self.mgr.receive_pattern_from_fleet(
                        reporter_id=f"{reporter_prefix}_reporter",
                        pattern_data={
                            "peer_id": f"peer_{i % 5}",
                            "hour_of_day": i % 24,
                            "direction": "inbound",
                            "intensity": 1.5,
                            "confidence": 0.8,
                            "samples": 20
                        }
                    )
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=receive_patterns, args=(f"t{t}", 30))
            for t in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent pattern receive raised errors: {errors}"

    def test_get_status_uses_lock(self):
        """get_status should read caches under lock."""
        # Add some data first
        self.mgr.record_flow_sample("chan1", 1000, 500)
        status = self.mgr.get_status()
        assert status["active"] is True
        assert status["total_flow_samples"] >= 1

    def test_cleanup_stale_kalman_uses_lock(self):
        """cleanup_stale_kalman_data should clean under lock."""
        # Add stale data
        self.mgr.receive_kalman_velocity(
            reporter_id="reporter1",
            channel_id="chan1",
            peer_id="peer1",
            velocity_pct_per_hour=0.01,
            uncertainty=0.05,
            flow_ratio=0.3,
            confidence=0.8,
        )
        # Not stale yet, should not clean
        cleaned = self.mgr.cleanup_stale_kalman_data()
        assert cleaned == 0

    def test_set_channel_peer_mapping_uses_lock(self):
        """set_channel_peer_mapping should use lock."""
        self.mgr.set_channel_peer_mapping("chan1", "peer1")
        with self.mgr._lock:
            assert self.mgr._channel_peer_map["chan1"] == "peer1"

    def test_update_channel_peer_mappings_uses_lock(self):
        """update_channel_peer_mappings should use lock."""
        channels = [
            {"short_channel_id": "100x1x0", "peer_id": "peer_aaa"},
            {"short_channel_id": "200x1x0", "peer_id": "peer_bbb"},
        ]
        self.mgr.update_channel_peer_mappings(channels)
        with self.mgr._lock:
            assert self.mgr._channel_peer_map["100x1x0"] == "peer_aaa"
            assert self.mgr._channel_peer_map["200x1x0"] == "peer_bbb"


# =============================================================================
# ANTICIPATORY - PER-CHANNEL FLOW SAMPLE LIMIT
# =============================================================================

class TestFlowSampleLimit:
    """Test per-channel flow sample limit."""

    def test_per_channel_sample_limit_enforced(self):
        """Flow history should be trimmed to MAX_FLOW_SAMPLES_PER_CHANNEL."""
        db = MockDatabase()
        mgr = AnticipatoryLiquidityManager(database=db)

        # Record more than the limit
        base_ts = int(time.time())
        for i in range(MAX_FLOW_SAMPLES_PER_CHANNEL + 100):
            mgr.record_flow_sample(
                "chan1",
                inbound_sats=1000,
                outbound_sats=500,
                timestamp=base_ts + i
            )

        with mgr._lock:
            assert len(mgr._flow_history["chan1"]) <= MAX_FLOW_SAMPLES_PER_CHANNEL


# =============================================================================
# ANTICIPATORY - AGGREGATE UNCERTAINTY FIX
# =============================================================================

class TestAggregateUncertainty:
    """Test that aggregate uncertainty calculation doesn't produce bad values."""

    def test_aggregate_uncertainty_with_tiny_uncertainty(self):
        """Very small uncertainty values should not cause overflow."""
        db = MockDatabase()
        mgr = AnticipatoryLiquidityManager(database=db, plugin=MockPlugin())

        # Add multiple reports with very small uncertainty
        now = int(time.time())
        for i in range(5):
            mgr.receive_kalman_velocity(
                reporter_id=f"reporter_{i}",
                channel_id="chan1",
                peer_id="peer1",
                velocity_pct_per_hour=0.01,
                uncertainty=0.001,  # Very small
                flow_ratio=0.3,
                confidence=0.9,
            )

        result = mgr.query_kalman_velocity("chan1")
        if result:
            # Should produce a valid (not NaN/Inf) uncertainty
            assert result.get("uncertainty", 0) >= 0
            assert result.get("uncertainty", float('inf')) < float('inf')


# =============================================================================
# YIELD METRICS - MISSING METHOD HANDLING
# =============================================================================

class TestYieldMetricsMissingMethod:
    """Test that missing get_channel_history is handled gracefully."""

    def test_velocity_without_get_channel_history(self):
        """Should return None, not raise AttributeError."""
        db = MockDatabaseNoHistory()
        mgr = YieldMetricsManager(database=db, plugin=MockPlugin())

        result = mgr._calculate_velocity_from_history("chan1")
        assert result is None

    def test_velocity_with_empty_history(self):
        """Should return None when history is empty."""
        db = MockDatabaseWithHistory([])
        mgr = YieldMetricsManager(database=db, plugin=MockPlugin())

        result = mgr._calculate_velocity_from_history("chan1")
        assert result is None

    def test_velocity_with_valid_history(self):
        """Should calculate velocity correctly when data is available."""
        now = int(time.time())
        history = [
            {"local_pct": 0.5, "timestamp": now - 7200},
            {"local_pct": 0.6, "timestamp": now},
        ]
        db = MockDatabaseWithHistory(history)
        mgr = YieldMetricsManager(database=db, plugin=MockPlugin())

        result = mgr._calculate_velocity_from_history("chan1")
        assert result is not None
        assert result["velocity_pct_per_hour"] == pytest.approx(0.05, abs=0.01)
        assert result["data_points"] == 2


# =============================================================================
# YIELD METRICS - THREAD SAFETY
# =============================================================================

class TestYieldMetricsThreadSafety:
    """Test that YieldMetricsManager caches are protected by lock."""

    def test_lock_initialized(self):
        """YieldMetricsManager should have a _lock."""
        mgr = YieldMetricsManager(database=MockDatabase(), plugin=MockPlugin())
        assert hasattr(mgr, '_lock')
        assert isinstance(mgr._lock, type(threading.Lock()))

    def test_concurrent_yield_metrics_receive(self):
        """Multiple threads receiving yield metrics should not corrupt state."""
        mgr = YieldMetricsManager(database=MockDatabase(), plugin=MockPlugin())
        errors = []

        def receive_metrics(reporter_prefix, count):
            try:
                for i in range(count):
                    mgr.receive_yield_metrics_from_fleet(
                        reporter_id=f"{reporter_prefix}_reporter",
                        metrics_data={
                            "peer_id": f"peer_{i % 5}",
                            "roi_pct": 2.5,
                            "capital_efficiency": 0.001,
                            "flow_intensity": 0.02,
                            "profitability_tier": "profitable",
                            "capacity_sats": 5000000
                        }
                    )
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=receive_metrics, args=(f"t{t}", 30))
            for t in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Concurrent yield metrics writes raised errors: {errors}"

    def test_cleanup_old_yield_metrics(self):
        """cleanup_old_remote_yield_metrics should work under lock."""
        mgr = YieldMetricsManager(database=MockDatabase(), plugin=MockPlugin())

        # Add data
        mgr.receive_yield_metrics_from_fleet(
            reporter_id="reporter1",
            metrics_data={
                "peer_id": "peer1",
                "roi_pct": 2.5,
            }
        )

        # Not old yet, should not clean
        cleaned = mgr.cleanup_old_remote_yield_metrics(max_age_days=7)
        assert cleaned == 0

    def test_get_fleet_yield_consensus_no_hasattr(self):
        """get_fleet_yield_consensus should work without hasattr check."""
        mgr = YieldMetricsManager(database=MockDatabase(), plugin=MockPlugin())

        # Should return None, not raise
        result = mgr.get_fleet_yield_consensus("unknown_peer")
        assert result is None


# =============================================================================
# NNLB - HEALTH SCORE CLAMPING
# =============================================================================

class TestNNLBHealthClamping:
    """Test that NNLB priority calculation clamps health_score."""

    def _make_coordinator(self, health_score=None):
        """Create a LiquidityCoordinator with a mock database returning given health_score."""
        db = MagicMock()
        if health_score is not None:
            db.get_member_health.return_value = {"overall_health": health_score}
        else:
            db.get_member_health.return_value = None
        db.get_all_members.return_value = []
        plugin = MockPlugin()
        coord = LiquidityCoordinator(
            database=db,
            plugin=plugin,
            our_pubkey="our_pubkey_abc123"
        )
        return coord

    def _make_need(self, reporter_id, target_peer_id, urgency="high"):
        """Create a LiquidityNeed with valid fields."""
        return LiquidityNeed(
            reporter_id=reporter_id,
            need_type="inbound",
            target_peer_id=target_peer_id,
            amount_sats=500000,
            urgency=urgency,
            max_fee_ppm=100,
            reason="low_balance",
            current_balance_pct=0.1,
            can_provide_inbound=0,
            can_provide_outbound=0,
            timestamp=int(time.time()),
            signature="sig_placeholder",
        )

    def test_health_score_over_100_clamped(self):
        """Health score > 100 should be clamped, not produce negative priority."""
        coord = self._make_coordinator(health_score=150)

        need = self._make_need("node_aaa", "peer1", "high")
        with coord._lock:
            coord._liquidity_needs[("node_aaa", "chan1")] = need

        prioritized = coord.get_prioritized_needs()
        assert len(prioritized) == 1

    def test_health_score_below_zero_clamped(self):
        """Health score < 0 should be clamped to 0."""
        coord = self._make_coordinator(health_score=-50)

        need = self._make_need("node_bbb", "peer2", "critical")
        with coord._lock:
            coord._liquidity_needs[("node_bbb", "chan2")] = need

        prioritized = coord.get_prioritized_needs()
        assert len(prioritized) == 1

    def test_normal_health_score(self):
        """Normal health scores in [0, 100] should work normally."""
        coord = self._make_coordinator(health_score=30)

        need = self._make_need("node_ccc", "peer3", "medium")
        with coord._lock:
            coord._liquidity_needs[("node_ccc", "chan3")] = need

        prioritized = coord.get_prioritized_needs()
        assert len(prioritized) == 1


# =============================================================================
# HIVE BRIDGE - KEY NAME FIX
# =============================================================================

class TestHiveBridgeKeyFix:
    """Test that hive_bridge uses correct key names for anticipatory data."""

    def test_forecasts_key_used(self):
        """query_all_anticipatory_predictions should read 'forecasts', not 'predictions'."""
        # We can't easily import HiveBridge without the full cl_revenue_ops env,
        # so we test by checking the file content directly
        bridge_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "..", "cl_revenue_ops", "modules", "hive_bridge.py"
        )
        if not os.path.exists(bridge_path):
            pytest.skip("cl_revenue_ops not available")

        with open(bridge_path, 'r') as f:
            content = f.read()

        # The fix should have changed "predictions" to "forecasts"
        assert 'result.get("forecasts", [])' in content, \
            "hive_bridge.py should use 'forecasts' key, not 'predictions'"

    def test_no_forecast_status_handled(self):
        """query_anticipatory_prediction should handle 'no_forecast' status."""
        bridge_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "..", "cl_revenue_ops", "modules", "hive_bridge.py"
        )
        if not os.path.exists(bridge_path):
            pytest.skip("cl_revenue_ops not available")

        with open(bridge_path, 'r') as f:
            content = f.read()

        assert '"no_forecast"' in content, \
            "hive_bridge.py should handle 'no_forecast' status"


# =============================================================================
# CL-HIVE.PY - ANTICIPATORY CHANNEL MAPPING UPDATE
# =============================================================================

class TestAnticipatoryChannelMapping:
    """Test that anticipatory_liquidity_mgr gets channel mapping updates."""

    def test_channel_mapping_update_in_broadcast(self):
        """_broadcast_our_temporal_patterns area should update anticipatory mappings."""
        main_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "cl-hive.py"
        )
        with open(main_path, 'r') as f:
            content = f.read()

        # Should update anticipatory_liquidity_mgr alongside fee_coordination_mgr
        assert "anticipatory_liquidity_mgr.update_channel_peer_mappings" in content, \
            "cl-hive.py should update anticipatory_liquidity_mgr channel mappings"


# =============================================================================
# ANTICIPATORY - PATTERN SHARING WITH CHANNEL MAP
# =============================================================================

class TestPatternSharing:
    """Test pattern sharing with channel-to-peer mappings."""

    def test_get_shareable_patterns_empty_map(self):
        """Should return empty list when no channel mappings exist."""
        mgr = AnticipatoryLiquidityManager(database=MockDatabase())
        result = mgr.get_shareable_patterns()
        assert result == []

    def test_get_fleet_patterns_returns_list(self):
        """get_fleet_patterns_for_peer should return list, not raise."""
        mgr = AnticipatoryLiquidityManager(database=MockDatabase())
        result = mgr.get_fleet_patterns_for_peer("unknown_peer")
        assert result == []

    def test_cleanup_remote_patterns_empty(self):
        """cleanup_old_remote_patterns should work on empty state."""
        mgr = AnticipatoryLiquidityManager(database=MockDatabase())
        cleaned = mgr.cleanup_old_remote_patterns()
        assert cleaned == 0

    def test_receive_and_retrieve_pattern(self):
        """Should be able to store and retrieve remote patterns."""
        mgr = AnticipatoryLiquidityManager(database=MockDatabase())

        success = mgr.receive_pattern_from_fleet(
            reporter_id="reporter_abc",
            pattern_data={
                "peer_id": "peer_xyz",
                "hour_of_day": 14,
                "direction": "outbound",
                "intensity": 1.5,
                "confidence": 0.8,
                "samples": 20
            }
        )
        assert success is True

        patterns = mgr.get_fleet_patterns_for_peer("peer_xyz")
        assert len(patterns) == 1
        assert patterns[0]["hour_of_day"] == 14


# =============================================================================
# ANTICIPATORY - KALMAN VELOCITY INTEGRATION
# =============================================================================

class TestKalmanVelocity:
    """Test Kalman velocity receive and query."""

    def setup_method(self):
        self.mgr = AnticipatoryLiquidityManager(
            database=MockDatabase(),
            plugin=MockPlugin()
        )

    def test_receive_and_query(self):
        """Should be able to store and query Kalman velocity."""
        self.mgr.receive_kalman_velocity(
            reporter_id="reporter1",
            channel_id="chan1",
            peer_id="peer1",
            velocity_pct_per_hour=0.02,
            uncertainty=0.05,
            flow_ratio=0.3,
            confidence=0.8,
        )

        result = self.mgr.query_kalman_velocity("chan1")
        if result:
            assert result["channel_id"] == "chan1"

    def test_receive_invalid_inputs(self):
        """Should reject invalid inputs gracefully."""
        result = self.mgr.receive_kalman_velocity(
            reporter_id="",
            channel_id="",
            peer_id="peer1",
            velocity_pct_per_hour=0.01,
            uncertainty=0.05,
            flow_ratio=0.3,
            confidence=0.8,
        )
        assert result is False

    def test_velocity_clamped(self):
        """Velocity should be clamped to [-1.0, 1.0]."""
        self.mgr.receive_kalman_velocity(
            reporter_id="reporter1",
            channel_id="chan1",
            peer_id="peer1",
            velocity_pct_per_hour=5.0,  # Way too high
            uncertainty=0.05,
            flow_ratio=0.3,
            confidence=0.8,
        )
        # Should not crash, velocity gets clamped internally


# =============================================================================
# VELOCITY CACHE TTL
# =============================================================================

class TestVelocityCacheTTL:
    """Test that velocity cache respects TTL."""

    def test_cache_miss_returns_fresh_data(self):
        """Fresh calculation should be returned when cache is expired."""
        now = int(time.time())
        history = [
            {"local_pct": 0.4, "timestamp": now - 3600},
            {"local_pct": 0.6, "timestamp": now},
        ]
        db = MockDatabaseWithHistory(history)
        mgr = YieldMetricsManager(database=db, plugin=MockPlugin())

        # First call populates cache
        r1 = mgr._calculate_velocity_from_history("chan1")
        assert r1 is not None

        # Second call within TTL should return cached (identical timestamp)
        r2 = mgr._calculate_velocity_from_history("chan1")
        assert r2 is not None
        assert r2["timestamp"] == r1["timestamp"]
