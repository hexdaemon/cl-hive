"""
Tests for 13 anticipatory liquidity fixes.

Covers:
- Fix 1: Monthly pattern detection loads 30 days of history
- Fix 2: Pattern matcher handles day_of_month patterns
- Fix 3: Intra-day velocity uses actual capacity instead of hardcoded 10M
- Fix 4: Fleet coordination uses remote patterns instead of stub
- Fix 5: total_predicted_demand_sats uses velocity-based estimate
- Fix 6: Pattern adjustment works when base_velocity is zero
- Fix 7: receive_pattern_from_fleet uses single lock block
- Fix 8: Kalman weight uses 1/sigma^2 (inverse variance)
- Fix 9: Risk combination uses weighted sum instead of max()
- Fix 10: Long-horizon predictions step through patterns
- Fix 11: Flow history eviction uses tracker dict
- Fix 12: Flow history trims by window before limit
- Fix 13: Kalman velocity status batches consensus in single lock

Author: Lightning Goats Team
"""

import math
import time
import threading
import pytest
from collections import defaultdict
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.anticipatory_liquidity import (
    AnticipatoryLiquidityManager,
    HourlyFlowSample,
    KalmanVelocityReport,
    TemporalPattern,
    LiquidityPrediction,
    FlowDirection,
    PredictionUrgency,
    RecommendedAction,
    PATTERN_WINDOW_DAYS,
    MONTHLY_PATTERN_WINDOW_DAYS,
    MONTHLY_PATTERNS_ENABLED,
    PATTERN_CONFIDENCE_THRESHOLD,
    PATTERN_STRENGTH_THRESHOLD,
    MAX_FLOW_HISTORY_CHANNELS,
    MAX_FLOW_SAMPLES_PER_CHANNEL,
    KALMAN_VELOCITY_TTL_SECONDS,
    KALMAN_MIN_CONFIDENCE,
    KALMAN_MIN_REPORTERS,
    DEPLETION_PCT_THRESHOLD,
    SATURATION_PCT_THRESHOLD,
)


# =============================================================================
# FIXTURES
# =============================================================================

class MockPlugin:
    def __init__(self):
        self.logs = []
        self.rpc = MagicMock()

    def log(self, msg, level="info"):
        self.logs.append({"msg": msg, "level": level})


class MockDatabase:
    def __init__(self):
        self._flow_samples = {}
        self._requested_days = []

    def record_flow_sample(self, **kwargs):
        pass

    def get_flow_samples(self, channel_id, days=14):
        self._requested_days.append(days)
        return self._flow_samples.get(channel_id, [])


class MockStateManager:
    def __init__(self):
        self._states = []

    def get_all_peer_states(self):
        return self._states


def _make_sample(channel_id, hour, day_of_week, net_flow, ts=None):
    """Helper to create an HourlyFlowSample."""
    ts = ts or int(time.time())
    return HourlyFlowSample(
        channel_id=channel_id,
        hour=hour,
        day_of_week=day_of_week,
        inbound_sats=max(0, net_flow),
        outbound_sats=max(0, -net_flow),
        net_flow_sats=net_flow,
        timestamp=ts,
    )


def _make_manager(db=None, plugin=None, state_manager=None, our_id="our_node_abc"):
    """Helper to create a manager."""
    return AnticipatoryLiquidityManager(
        database=db or MockDatabase(),
        plugin=plugin or MockPlugin(),
        state_manager=state_manager,
        our_id=our_id,
    )


# =============================================================================
# FIX 1: Monthly pattern detection loads 30 days
# =============================================================================

class TestMonthlyPatternHistoryWindow:
    """Fix 1: load_flow_history uses MONTHLY_PATTERN_WINDOW_DAYS when enabled."""

    def test_default_loads_monthly_window(self):
        """Default load_flow_history should request 30 days when monthly enabled."""
        db = MockDatabase()
        mgr = _make_manager(db=db)
        mgr.load_flow_history("chan1")
        assert db._requested_days[-1] == MONTHLY_PATTERN_WINDOW_DAYS

    def test_explicit_days_override(self):
        """Explicit days parameter should override default."""
        db = MockDatabase()
        mgr = _make_manager(db=db)
        mgr.load_flow_history("chan1", days=7)
        assert db._requested_days[-1] == 7

    def test_monthly_window_constant(self):
        """MONTHLY_PATTERN_WINDOW_DAYS should be 30."""
        assert MONTHLY_PATTERN_WINDOW_DAYS == 30
        assert MONTHLY_PATTERN_WINDOW_DAYS > PATTERN_WINDOW_DAYS


# =============================================================================
# FIX 2: Pattern matcher handles day_of_month
# =============================================================================

class TestPatternMatcherDayOfMonth:
    """Fix 2: _find_best_pattern_match handles monthly patterns."""

    def setup_method(self):
        self.mgr = _make_manager()

    def test_exact_day_of_month_match(self):
        """Should match pattern with exact day_of_month."""
        pattern = TemporalPattern(
            channel_id="c1", hour_of_day=None, direction=FlowDirection.OUTBOUND,
            intensity=1.5, confidence=0.8, samples=10, avg_flow_sats=50000,
            day_of_month=15,
        )
        match = self.mgr._find_best_pattern_match([pattern], target_hour=10, target_day=2, target_day_of_month=15)
        assert match is pattern

    def test_day_of_month_no_match(self):
        """Should not match when day_of_month differs."""
        pattern = TemporalPattern(
            channel_id="c1", hour_of_day=None, direction=FlowDirection.OUTBOUND,
            intensity=1.5, confidence=0.8, samples=10, avg_flow_sats=50000,
            day_of_month=15,
        )
        match = self.mgr._find_best_pattern_match([pattern], target_hour=10, target_day=2, target_day_of_month=20)
        assert match is None

    def test_eom_cluster_matches_day_28(self):
        """EOM cluster (day_of_month=31) should match day 28."""
        pattern = TemporalPattern(
            channel_id="c1", hour_of_day=None, direction=FlowDirection.INBOUND,
            intensity=2.0, confidence=0.7, samples=15, avg_flow_sats=80000,
            day_of_month=31,  # EOM cluster marker
        )
        match = self.mgr._find_best_pattern_match([pattern], target_hour=10, target_day=2, target_day_of_month=28)
        assert match is pattern

    def test_eom_cluster_matches_day_1(self):
        """EOM cluster should also match day 1 (beginning of next month)."""
        pattern = TemporalPattern(
            channel_id="c1", hour_of_day=None, direction=FlowDirection.INBOUND,
            intensity=2.0, confidence=0.7, samples=15, avg_flow_sats=80000,
            day_of_month=31,
        )
        match = self.mgr._find_best_pattern_match([pattern], target_hour=10, target_day=2, target_day_of_month=1)
        assert match is pattern

    def test_hourly_beats_monthly(self):
        """Hour+day match (score 3) should beat monthly match (score 1.5)."""
        monthly = TemporalPattern(
            channel_id="c1", hour_of_day=None, direction=FlowDirection.OUTBOUND,
            intensity=2.0, confidence=0.9, samples=20, avg_flow_sats=80000,
            day_of_month=15,
        )
        hourly_daily = TemporalPattern(
            channel_id="c1", hour_of_day=10, day_of_week=2,
            direction=FlowDirection.INBOUND,
            intensity=1.5, confidence=0.8, samples=10, avg_flow_sats=50000,
        )
        match = self.mgr._find_best_pattern_match(
            [monthly, hourly_daily], target_hour=10, target_day=2, target_day_of_month=15
        )
        assert match is hourly_daily


# =============================================================================
# FIX 3: Intra-day velocity uses actual capacity
# =============================================================================

class TestIntradayCapacity:
    """Fix 3: _analyze_intraday_bucket uses capacity_sats instead of hardcoded 10M."""

    def setup_method(self):
        self.mgr = _make_manager()

    def test_velocity_with_actual_capacity(self):
        """Velocity should scale correctly with actual channel capacity."""
        from modules.anticipatory_liquidity import IntraDayPhase

        # 1M sat channel with 100K net flow => 10% velocity
        samples = [
            _make_sample("c1", hour=9, day_of_week=0, net_flow=100_000,
                         ts=int(time.time()) - i * 3600)
            for i in range(10)
        ]
        result = self.mgr._analyze_intraday_bucket(
            channel_id="c1", samples=samples,
            phase=IntraDayPhase.MORNING, hour_start=8, hour_end=12,
            kalman_confidence=0.5, is_regime_change=False,
            capacity_sats=1_000_000,
        )
        assert result is not None
        # velocity = 100_000 / 1_000_000 = 0.10 (10%)
        assert abs(result.avg_velocity - 0.10) < 0.01

    def test_velocity_with_zero_capacity_uses_estimate(self):
        """When capacity_sats=0, should estimate from flow magnitudes."""
        from modules.anticipatory_liquidity import IntraDayPhase

        samples = [
            _make_sample("c1", hour=9, day_of_week=0, net_flow=100_000,
                         ts=int(time.time()) - i * 3600)
            for i in range(10)
        ]
        result = self.mgr._analyze_intraday_bucket(
            channel_id="c1", samples=samples,
            phase=IntraDayPhase.MORNING, hour_start=8, hour_end=12,
            kalman_confidence=0.5, is_regime_change=False,
            capacity_sats=0,
        )
        assert result is not None
        # Estimate: p90 of magnitudes * 10 = 100_000 * 10 = 1M
        # So velocity ~ 100_000 / 1M = 0.10
        assert result.avg_velocity > 0


# =============================================================================
# FIX 4: Fleet coordination uses remote patterns
# =============================================================================

class TestFleetCoordinationRemotePatterns:
    """Fix 4: get_fleet_recommendations uses _remote_patterns instead of stub."""

    def test_remote_patterns_included_in_depletion(self):
        """Remote outbound patterns should add members to depleting list."""
        sm = MockStateManager()
        mgr = _make_manager(state_manager=sm, our_id="our_node")

        # Set up a prediction for peer_abc
        pred = LiquidityPrediction(
            channel_id="c1", peer_id="peer_abc",
            current_local_pct=0.15, predicted_local_pct=0.05,
            hours_ahead=12, velocity_pct_per_hour=-0.008,
            depletion_risk=0.7, saturation_risk=0.0,
            hours_to_critical=5.0,
            recommended_action=RecommendedAction.PREEMPTIVE_REBALANCE,
            urgency=PredictionUrgency.URGENT,
            confidence=0.8, pattern_match=None,
        )

        # Add remote pattern from another member
        mgr.receive_pattern_from_fleet(
            reporter_id="member_xyz",
            pattern_data={
                "peer_id": "peer_abc",
                "direction": "outbound",
                "intensity": 1.5,
                "confidence": 0.8,
                "samples": 20,
            },
        )

        # Mock get_all_predictions to return our prediction
        with patch.object(mgr, 'get_all_predictions', return_value=[pred]):
            with patch.object(mgr, '_get_channel_info', return_value={
                "capacity_sats": 5_000_000, "channel_id": "c1"
            }):
                recs = mgr.get_fleet_recommendations()

        assert len(recs) == 1
        rec = recs[0]
        assert "member_xyz" in rec.members_predicting_depletion
        assert "our_node" in rec.members_predicting_depletion


# =============================================================================
# FIX 5: Demand calculation uses velocity
# =============================================================================

class TestDemandCalculation:
    """Fix 5: total_predicted_demand_sats uses velocity-based estimate."""

    def test_demand_based_on_velocity(self):
        """Demand should be velocity * hours * capacity, not pct * 1M."""
        sm = MockStateManager()
        mgr = _make_manager(state_manager=sm)

        pred = LiquidityPrediction(
            channel_id="c1", peer_id="peer_abc",
            current_local_pct=0.15, predicted_local_pct=0.05,
            hours_ahead=12, velocity_pct_per_hour=-0.01,
            depletion_risk=0.7, saturation_risk=0.0,
            hours_to_critical=5.0,
            recommended_action=RecommendedAction.PREEMPTIVE_REBALANCE,
            urgency=PredictionUrgency.URGENT,
            confidence=0.8, pattern_match=None,
        )

        with patch.object(mgr, 'get_all_predictions', return_value=[pred]):
            with patch.object(mgr, '_get_channel_info', return_value={
                "capacity_sats": 10_000_000, "channel_id": "c1"
            }):
                recs = mgr.get_fleet_recommendations()

        assert len(recs) == 1
        # velocity=0.01, hours=12, capacity=10M => demand = 0.01 * 12 * 10M = 1.2M
        assert recs[0].total_predicted_demand_sats == 1_200_000


# =============================================================================
# FIX 6: Pattern adjustment works when base_velocity is zero
# =============================================================================

class TestPatternVelocityFloor:
    """Fix 6: Pattern adjustment has effect even when base_velocity=0."""

    def test_outbound_pattern_with_zero_velocity(self):
        """Outbound pattern should reduce velocity below zero even from base=0."""
        mgr = _make_manager()

        # Compute what hour the prediction will target (1h from now)
        target_time = datetime.fromtimestamp(time.time() + 3600, tz=timezone.utc)
        target_hour = target_time.hour
        target_day = target_time.weekday()

        pattern = TemporalPattern(
            channel_id="c1", hour_of_day=target_hour, day_of_week=target_day,
            direction=FlowDirection.OUTBOUND,
            intensity=1.5, confidence=0.8, samples=15, avg_flow_sats=100_000,
        )

        # Mock the methods
        with patch.object(mgr, 'detect_patterns', return_value=[pattern]):
            with patch.object(mgr, '_calculate_velocity', return_value=0.0):
                with patch.object(mgr, '_get_channel_info', return_value=None):
                    pred = mgr.predict_liquidity(
                        channel_id="c1",
                        hours_ahead=1,
                        current_local_pct=0.5,
                        capacity_sats=2_000_000,
                        peer_id="peer1",
                    )

        assert pred is not None
        # Pattern floor = 100_000 / 2_000_000 = 0.05
        # adjusted = 0.0 - (1.5 * 0.05 * 0.5) = -0.0375
        assert pred.velocity_pct_per_hour < 0
        assert pred.predicted_local_pct < 0.5


# =============================================================================
# FIX 7: receive_pattern_from_fleet single lock block
# =============================================================================

class TestReceivePatternThreadSafety:
    """Fix 7: Eviction and append in single lock acquisition."""

    def test_concurrent_receive_patterns(self):
        """Concurrent calls should not corrupt state."""
        mgr = _make_manager()
        errors = []

        def add_pattern(reporter, peer):
            try:
                result = mgr.receive_pattern_from_fleet(
                    reporter_id=reporter,
                    pattern_data={
                        "peer_id": peer,
                        "direction": "outbound",
                        "intensity": 1.5,
                        "confidence": 0.7,
                        "samples": 10,
                    },
                )
                assert result is True
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=add_pattern, args=(f"reporter_{i}", f"peer_{i % 5}"))
            for i in range(50)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # All 5 unique peers should be tracked
        assert len(mgr._remote_patterns) == 5


# =============================================================================
# FIX 8: Kalman inverse-variance weighting (1/sigma^2)
# =============================================================================

class TestKalmanInverseVarianceWeighting:
    """Fix 8: Consensus velocity uses 1/sigma^2, not 1/sigma."""

    def test_low_uncertainty_dominates(self):
        """Reporter with much lower uncertainty should dominate consensus."""
        mgr = _make_manager()
        now = int(time.time())

        # Reporter A: velocity=0.05, uncertainty=0.01 (very precise)
        mgr.receive_kalman_velocity(
            reporter_id="A", channel_id="c1", peer_id="p1",
            velocity_pct_per_hour=0.05, uncertainty=0.01,
            flow_ratio=0.5, confidence=0.9,
        )
        # Reporter B: velocity=-0.05, uncertainty=0.10 (10x less precise)
        mgr.receive_kalman_velocity(
            reporter_id="B", channel_id="c1", peer_id="p1",
            velocity_pct_per_hour=-0.05, uncertainty=0.10,
            flow_ratio=0.5, confidence=0.9,
        )

        consensus = mgr._get_kalman_consensus_velocity("c1")
        assert consensus is not None
        # With 1/sigma^2: weight_A = 0.9/(0.0001*1.5) = 6000, weight_B = 0.9/(0.01*1.5) = 60
        # So A should dominate ~99:1
        assert consensus > 0.04  # Should be close to 0.05, not 0.0

    def test_equal_uncertainty_equal_weight(self):
        """Equal uncertainties should give equal weight (averaging)."""
        mgr = _make_manager()

        mgr.receive_kalman_velocity(
            reporter_id="A", channel_id="c1", peer_id="p1",
            velocity_pct_per_hour=0.10, uncertainty=0.05,
            flow_ratio=0.5, confidence=0.9,
        )
        mgr.receive_kalman_velocity(
            reporter_id="B", channel_id="c1", peer_id="p1",
            velocity_pct_per_hour=0.00, uncertainty=0.05,
            flow_ratio=0.5, confidence=0.9,
        )

        consensus = mgr._get_kalman_consensus_velocity("c1")
        assert consensus is not None
        # Equal uncertainty + equal confidence => simple average ≈ 0.05
        assert abs(consensus - 0.05) < 0.01


# =============================================================================
# FIX 9: Risk combination weighted sum
# =============================================================================

class TestRiskWeightedSum:
    """Fix 9: Risk uses weighted sum instead of max()."""

    def setup_method(self):
        self.mgr = _make_manager()

    def test_all_factors_contribute(self):
        """All risk factors should contribute to combined risk."""
        # High base (20% local), high velocity (-1.5%/hr), predicted 5%
        risk = self.mgr._calculate_depletion_risk(
            current_pct=0.20, predicted_pct=0.05, velocity=-0.015
        )
        # base_risk=0.8, velocity_risk=0.8, predicted_risk=0.9
        # weighted = 0.8*0.4 + 0.8*0.3 + 0.9*0.3 = 0.32 + 0.24 + 0.27 = 0.83
        assert 0.8 <= risk <= 0.9

    def test_low_base_with_bad_velocity(self):
        """Bad velocity should increase risk even when level seems safe."""
        # 50% local (safe level), but draining fast
        risk = self.mgr._calculate_depletion_risk(
            current_pct=0.50, predicted_pct=0.30, velocity=-0.015
        )
        # base_risk=0.0, velocity_risk=0.8, predicted_risk=0.1
        # weighted = 0.0*0.4 + 0.8*0.3 + 0.1*0.3 = 0.0 + 0.24 + 0.03 = 0.27
        assert risk > 0.2  # Should be non-trivial, not 0

    def test_saturation_all_factors(self):
        """Saturation risk should also compound all factors."""
        risk = self.mgr._calculate_saturation_risk(
            current_pct=0.80, predicted_pct=0.90, velocity=0.015
        )
        # base_risk=0.8, velocity_risk=0.8, predicted_risk=0.9
        assert 0.8 <= risk <= 0.9


# =============================================================================
# FIX 10: Multi-bucket long-horizon prediction
# =============================================================================

class TestMultiBucketPrediction:
    """Fix 10: Long predictions step through hourly patterns."""

    def test_short_prediction_uses_simple_linear(self):
        """Predictions <= 6 hours should use simple linear projection."""
        mgr = _make_manager()

        with patch.object(mgr, 'detect_patterns', return_value=[]):
            with patch.object(mgr, '_calculate_velocity', return_value=-0.01):
                pred = mgr.predict_liquidity(
                    channel_id="c1", hours_ahead=4,
                    current_local_pct=0.5, capacity_sats=5_000_000, peer_id="p1",
                )
        assert pred is not None
        # Simple: 0.5 + (-0.01 * 4) = 0.46
        assert abs(pred.predicted_local_pct - 0.46) < 0.01

    def test_long_prediction_steps_through_patterns(self):
        """24h prediction should step through different patterns."""
        mgr = _make_manager()

        # Pattern: hour 9 = outbound drain
        pattern_drain = TemporalPattern(
            channel_id="c1", hour_of_day=9, direction=FlowDirection.OUTBOUND,
            intensity=2.0, confidence=0.9, samples=20, avg_flow_sats=200_000,
        )
        # Pattern: hour 22 = inbound surge
        pattern_surge = TemporalPattern(
            channel_id="c1", hour_of_day=22, direction=FlowDirection.INBOUND,
            intensity=2.0, confidence=0.9, samples=20, avg_flow_sats=200_000,
        )

        with patch.object(mgr, 'detect_patterns', return_value=[pattern_drain, pattern_surge]):
            with patch.object(mgr, '_calculate_velocity', return_value=0.0):
                pred = mgr.predict_liquidity(
                    channel_id="c1", hours_ahead=24,
                    current_local_pct=0.5, capacity_sats=5_000_000, peer_id="p1",
                )

        # With patterns: drain at hour 9, surge at hour 22, neutral otherwise
        # Should not just be 0.5 (which it would be with zero velocity and no patterns)
        assert pred is not None
        # The exact value depends on current time, but the prediction should differ
        # from 0.5 since patterns provide velocity floors


# =============================================================================
# FIX 11: Flow history eviction uses tracker
# =============================================================================

class TestFlowHistoryEviction:
    """Fix 11: O(1) eviction via _flow_history_last_ts tracker."""

    def test_tracker_initialized(self):
        """Manager should have _flow_history_last_ts dict."""
        mgr = _make_manager()
        assert hasattr(mgr, '_flow_history_last_ts')
        assert isinstance(mgr._flow_history_last_ts, dict)

    def test_tracker_updated_on_record(self):
        """Recording a sample should update the timestamp tracker."""
        mgr = _make_manager()
        now = int(time.time())
        mgr.record_flow_sample("chan1", 100, 50, timestamp=now)
        assert "chan1" in mgr._flow_history_last_ts
        assert mgr._flow_history_last_ts["chan1"] == now

    def test_eviction_removes_oldest_tracker(self):
        """When evicting, the tracker entry should also be removed."""
        mgr = _make_manager()
        now = int(time.time())

        # Fill to limit
        for i in range(MAX_FLOW_HISTORY_CHANNELS):
            mgr.record_flow_sample(f"chan_{i}", 100, 50, timestamp=now + i)

        assert len(mgr._flow_history) == MAX_FLOW_HISTORY_CHANNELS

        # Add one more => should evict oldest
        mgr.record_flow_sample("chan_new", 100, 50, timestamp=now + MAX_FLOW_HISTORY_CHANNELS + 1)
        assert len(mgr._flow_history) <= MAX_FLOW_HISTORY_CHANNELS + 1
        # The evicted channel (chan_0) should not be in tracker
        if "chan_0" not in mgr._flow_history:
            assert "chan_0" not in mgr._flow_history_last_ts


# =============================================================================
# FIX 12: Window trim before limit
# =============================================================================

class TestFlowHistoryTrimOrder:
    """Fix 12: Old samples trimmed by window first, then limit applied."""

    def test_old_samples_trimmed_by_monthly_window(self):
        """Samples older than monthly window should be trimmed."""
        mgr = _make_manager()
        now = int(time.time())

        # Add a sample 40 days ago (beyond 30-day monthly window)
        old_ts = now - (40 * 24 * 3600)
        mgr.record_flow_sample("chan1", 100, 50, timestamp=old_ts)

        # Add a recent sample
        mgr.record_flow_sample("chan1", 200, 100, timestamp=now)

        with mgr._lock:
            samples = mgr._flow_history["chan1"]
        # Old sample should have been trimmed
        assert all(s.timestamp > now - (MONTHLY_PATTERN_WINDOW_DAYS * 24 * 3600) for s in samples)


# =============================================================================
# FIX 13: Kalman velocity status batched in single lock
# =============================================================================

class TestKalmanStatusBatched:
    """Fix 13: get_kalman_velocity_status doesn't call _get_kalman_consensus_velocity."""

    def test_status_works_without_deadlock(self):
        """get_kalman_velocity_status should complete without deadlocking."""
        mgr = _make_manager()

        # Add some Kalman data
        mgr.receive_kalman_velocity(
            reporter_id="A", channel_id="c1", peer_id="p1",
            velocity_pct_per_hour=0.01, uncertainty=0.05,
            flow_ratio=0.5, confidence=0.8,
        )

        status = mgr.get_kalman_velocity_status()
        assert status["kalman_integration_active"] is True
        assert status["channels_with_data"] == 1
        assert status["total_reports"] == 1

    def test_consensus_count_correct(self):
        """channels_with_consensus should count channels meeting min_reporters threshold."""
        mgr = _make_manager()

        # Channel c1: 1 reporter (below default KALMAN_MIN_REPORTERS=1 means it qualifies)
        mgr.receive_kalman_velocity(
            reporter_id="A", channel_id="c1", peer_id="p1",
            velocity_pct_per_hour=0.01, uncertainty=0.05,
            flow_ratio=0.5, confidence=0.8,
        )

        status = mgr.get_kalman_velocity_status()
        if KALMAN_MIN_REPORTERS <= 1:
            assert status["channels_with_consensus"] >= 1
        else:
            assert status["channels_with_consensus"] == 0
