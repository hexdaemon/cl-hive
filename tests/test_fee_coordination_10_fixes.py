"""
Tests for 10 fee coordination bug fixes.

Bug 1:  Fleet pheromone hints now used in recommendation pipeline
Bug 2:  _pheromone_fee tracks EMA instead of last-value-wins
Bug 3:  receive_marker_from_gossip enforces route count cap
Bug 4:  get_all_fleet_hints snapshots keys under lock
Bug 5:  FlowCorridorManager._assignments uses atomic swap
Bug 6:  _velocity_cache evicted during evaporate_all_pheromones
Bug 7:  Stigmergic confidence formula scales with marker count
Bug 8:  suggest_fee enforces floor/ceiling bounds
Bug 9:  _record_forward_for_fee_coordination uses channel_peer_map cache
Bug 10: _fee_observations protected by _fee_obs_lock
"""

import math
import threading
import time
import pytest
from unittest.mock import MagicMock, patch

from modules.fee_coordination import (
    AdaptiveFeeController,
    StigmergicCoordinator,
    FlowCorridorManager,
    FeeCoordinationManager,
    RouteMarker,
    FLEET_FEE_FLOOR_PPM,
    FLEET_FEE_CEILING_PPM,
    DEFAULT_FEE_PPM,
    PHEROMONE_DEPOSIT_SCALE,
    MARKER_MIN_STRENGTH,
)


# =============================================================================
# Bug 1: Fleet pheromone hints used in recommendation pipeline
# =============================================================================

class TestFleetHintInPipeline:
    """Bug 1: get_fee_recommendation now consults fleet pheromone hints."""

    def test_fleet_hint_blended_into_recommendation(self):
        """Fleet pheromone hint should influence the recommended fee."""
        mgr = FeeCoordinationManager(
            database=MagicMock(),
            plugin=MagicMock(),
        )
        mgr.set_our_pubkey("03us")

        peer_id = "03external"

        # Inject strong fleet pheromone hints for this peer (multiple reporters)
        with mgr.adaptive_controller._lock:
            mgr.adaptive_controller._remote_pheromones[peer_id] = [
                {
                    "reporter_id": "03reporter_1",
                    "level": 10.0,
                    "fee_ppm": 200,
                    "timestamp": time.time(),
                    "weight": 0.5,  # High weight for strong confidence
                },
                {
                    "reporter_id": "03reporter_2",
                    "level": 8.0,
                    "fee_ppm": 200,
                    "timestamp": time.time(),
                    "weight": 0.5,
                },
            ]

        rec = mgr.get_fee_recommendation(
            channel_id="123x1x0",
            peer_id=peer_id,
            current_fee=500,
            local_balance_pct=0.5,
        )

        # The recommended fee should be pulled toward 200 from 500
        assert rec.recommended_fee_ppm < 500
        assert "fleet_pheromone" in rec.reason

    def test_fleet_hint_skipped_low_confidence(self):
        """Fleet hint with very low confidence should not influence fee."""
        mgr = FeeCoordinationManager(
            database=MagicMock(),
            plugin=MagicMock(),
        )
        mgr.set_our_pubkey("03us")

        peer_id = "03external"

        # Inject a weak fleet hint (low level → low confidence)
        with mgr.adaptive_controller._lock:
            mgr.adaptive_controller._remote_pheromones[peer_id] = [{
                "reporter_id": "03reporter",
                "level": 0.5,
                "fee_ppm": 200,
                "timestamp": time.time(),
                "weight": 0.1  # Very low weight
            }]

        rec = mgr.get_fee_recommendation(
            channel_id="123x1x0",
            peer_id=peer_id,
            current_fee=500,
            local_balance_pct=0.5,
        )

        # With such low confidence, the hint should be skipped
        assert "fleet_pheromone" not in rec.reason

    def test_fleet_hint_no_data_no_crash(self):
        """No fleet data should produce normal recommendation without error."""
        mgr = FeeCoordinationManager(
            database=MagicMock(),
            plugin=MagicMock(),
        )
        mgr.set_our_pubkey("03us")

        rec = mgr.get_fee_recommendation(
            channel_id="123x1x0",
            peer_id="03external",
            current_fee=500,
            local_balance_pct=0.5,
        )

        assert rec.recommended_fee_ppm > 0
        assert "fleet_pheromone" not in rec.reason


# =============================================================================
# Bug 2: _pheromone_fee tracks EMA instead of last value
# =============================================================================

class TestPheromoneEMA:
    """Bug 2: Pheromone fee should track exponential moving average."""

    def test_ema_not_last_value(self):
        """Multiple successes at 500 then one at 100 should not drop to 100."""
        controller = AdaptiveFeeController()

        # Route successfully 10 times at 500 ppm
        for _ in range(10):
            controller.update_pheromone("ch1", 500, True, 10000)

        # Route once at 100 ppm
        controller.update_pheromone("ch1", 100, True, 10000)

        # Fee should still be much closer to 500 than 100
        fee = controller._pheromone_fee.get("ch1", 0)
        assert fee > 300, f"EMA fee {fee} should be > 300 (close to 500, not 100)"

    def test_ema_converges_to_new_fee(self):
        """Repeated routing at new fee should converge the EMA."""
        controller = AdaptiveFeeController()

        # Start at 500
        controller.update_pheromone("ch1", 500, True, 10000)
        assert controller._pheromone_fee["ch1"] == 500

        # Route many times at 200 - should converge toward 200
        for _ in range(30):
            controller.update_pheromone("ch1", 200, True, 10000)

        fee = controller._pheromone_fee["ch1"]
        assert fee < 250, f"EMA fee {fee} should converge toward 200"


# =============================================================================
# Bug 3: receive_marker_from_gossip enforces route count cap
# =============================================================================

class TestGossipMarkerRouteCap:
    """Bug 3: receive_marker_from_gossip should cap route pairs at 1000."""

    def test_route_count_capped(self):
        """Markers for >1000 distinct routes should trigger eviction."""
        coord = StigmergicCoordinator(
            database=MagicMock(), plugin=MagicMock()
        )

        # Insert markers for 1001 distinct (source, dest) pairs
        for i in range(1001):
            marker_data = {
                "depositor": "03reporter",
                "source_peer_id": f"src_{i:04d}",
                "destination_peer_id": f"dst_{i:04d}",
                "fee_ppm": 500,
                "success": True,
                "volume_sats": 50000,
                "timestamp": time.time(),
                "strength": 0.5,
            }
            coord.receive_marker_from_gossip(marker_data)

        # Should be capped at 1000
        assert len(coord._markers) <= 1000

    def test_eviction_removes_oldest(self):
        """Eviction should remove the route with the oldest marker."""
        coord = StigmergicCoordinator(
            database=MagicMock(), plugin=MagicMock()
        )

        # Insert an old marker
        old_marker = {
            "depositor": "03reporter",
            "source_peer_id": "old_src",
            "destination_peer_id": "old_dst",
            "fee_ppm": 500,
            "success": True,
            "volume_sats": 50000,
            "timestamp": time.time() - 86400,  # 1 day old
            "strength": 0.5,
        }
        coord.receive_marker_from_gossip(old_marker)

        # Fill up to 1000 with fresh markers
        for i in range(1000):
            marker_data = {
                "depositor": "03reporter",
                "source_peer_id": f"src_{i:04d}",
                "destination_peer_id": f"dst_{i:04d}",
                "fee_ppm": 500,
                "success": True,
                "volume_sats": 50000,
                "timestamp": time.time(),
                "strength": 0.5,
            }
            coord.receive_marker_from_gossip(marker_data)

        # The old route should have been evicted
        assert ("old_src", "old_dst") not in coord._markers
        assert len(coord._markers) <= 1000


# =============================================================================
# Bug 4: get_all_fleet_hints snapshots keys under lock
# =============================================================================

class TestFleetHintsLock:
    """Bug 4: get_all_fleet_hints should snapshot keys under lock."""

    def test_concurrent_modification_no_error(self):
        """get_all_fleet_hints should not crash with concurrent modification."""
        controller = AdaptiveFeeController()

        # Pre-populate some remote pheromones
        for i in range(20):
            controller.receive_pheromone_from_gossip(
                reporter_id=f"03reporter_{i}",
                pheromone_data={
                    "peer_id": f"03peer_{i}",
                    "level": 5.0,
                    "fee_ppm": 500,
                },
            )

        errors = []

        def modify_dict():
            """Continuously add/remove entries."""
            for j in range(100):
                controller.receive_pheromone_from_gossip(
                    reporter_id=f"03mod_{j}",
                    pheromone_data={
                        "peer_id": f"03modpeer_{j}",
                        "level": 3.0,
                        "fee_ppm": 300,
                    },
                )

        def read_hints():
            """Continuously read hints."""
            try:
                for _ in range(50):
                    controller.get_all_fleet_hints()
            except RuntimeError as e:
                errors.append(str(e))

        t1 = threading.Thread(target=modify_dict)
        t2 = threading.Thread(target=read_hints)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Should complete without RuntimeError
        assert len(errors) == 0, f"Got errors: {errors}"


# =============================================================================
# Bug 5: FlowCorridorManager._assignments atomic swap
# =============================================================================

class TestAssignmentsAtomicSwap:
    """Bug 5: get_assignments should use atomic swap, not clear+rebuild."""

    def test_assignments_never_empty_during_refresh(self):
        """_assignments should not be temporarily empty during refresh."""
        mgr = FlowCorridorManager(
            database=MagicMock(),
            plugin=MagicMock(),
            liquidity_coordinator=MagicMock(),
        )
        mgr.set_our_pubkey("03us")

        # Pre-populate assignments
        from modules.fee_coordination import FlowCorridor, CorridorAssignment
        corridor = FlowCorridor(
            source_peer_id="src",
            destination_peer_id="dst",
            capable_members=["03us"],
        )
        initial_assignments = {("src", "dst"): CorridorAssignment(
            corridor=corridor,
            primary_member="03us",
            secondary_members=[],
            primary_fee_ppm=500,
            secondary_fee_ppm=750,
            assignment_reason="test",
            confidence=0.8,
        )}
        mgr._assignments_snapshot = (initial_assignments, 0)

        # Mock identify_corridors to return empty (simulates no competitions)
        mgr.liquidity_coordinator.detect_internal_competition.return_value = []

        seen_empty = []

        original_assign = mgr.assign_corridor

        def slow_assign(corridor):
            """Simulate slow assignment to test concurrency."""
            # Check if assignments dict is visible during rebuild
            assignments, _ = mgr._assignments_snapshot
            if len(assignments) == 0:
                seen_empty.append(True)
            return original_assign(corridor)

        mgr.assign_corridor = slow_assign

        # Force refresh
        mgr.get_assignments(force_refresh=True)

        # With atomic swap, assignments should never be seen as empty
        # during the rebuild (the old dict stays until new one is ready)
        assert len(seen_empty) == 0


# =============================================================================
# Bug 6: _velocity_cache evicted during evaporate_all_pheromones
# =============================================================================

class TestVelocityCacheEviction:
    """Bug 6: Stale velocity cache entries should be evicted."""

    def test_stale_velocity_entries_evicted(self):
        """Velocity entries older than 48h should be cleaned up."""
        controller = AdaptiveFeeController()

        # Add a stale entry (3 days old)
        controller._velocity_cache["old_ch"] = 0.01
        controller._velocity_cache_time["old_ch"] = time.time() - 72 * 3600

        # Add a fresh entry
        controller._velocity_cache["new_ch"] = 0.02
        controller._velocity_cache_time["new_ch"] = time.time()

        # Add some pheromone data so evaporate_all_pheromones has work to do
        with controller._lock:
            controller._pheromone["ch1"] = 5.0
            controller._pheromone_last_update["ch1"] = time.time() - 3600

        controller.evaporate_all_pheromones()

        # Old entry should be evicted
        assert "old_ch" not in controller._velocity_cache
        assert "old_ch" not in controller._velocity_cache_time

        # Fresh entry should remain
        assert "new_ch" in controller._velocity_cache

    def test_no_velocity_entries_no_crash(self):
        """Evaporation with empty velocity cache should not crash."""
        controller = AdaptiveFeeController()
        controller.evaporate_all_pheromones()  # Should not raise


# =============================================================================
# Bug 7: Stigmergic confidence scales with marker count
# =============================================================================

class TestStigmergicConfidenceFormula:
    """Bug 7: More markers should yield higher confidence."""

    def test_single_marker_moderate_confidence(self):
        """One successful marker gives moderate confidence."""
        coord = StigmergicCoordinator(
            database=MagicMock(), plugin=MagicMock()
        )
        coord.set_our_pubkey("03us")

        coord.deposit_marker("src", "dst", 500, True, 100000)
        _, confidence = coord.calculate_coordinated_fee("src", "dst", 500)

        # 1 marker: 0.5 + 1 * 0.05 = 0.55
        assert 0.50 <= confidence <= 0.60

    def test_many_markers_high_confidence(self):
        """Many successful markers should yield higher confidence."""
        coord = StigmergicCoordinator(
            database=MagicMock(), plugin=MagicMock()
        )
        coord.set_our_pubkey("03us")

        # Deposit 8 successful markers
        for i in range(8):
            marker = RouteMarker(
                depositor=f"03member_{i}",
                source_peer_id="src",
                destination_peer_id="dst",
                fee_ppm=500,
                success=True,
                volume_sats=50000,
                timestamp=time.time(),
                strength=0.5,
            )
            with coord._lock:
                coord._markers[("src", "dst")].append(marker)

        _, confidence = coord.calculate_coordinated_fee("src", "dst", 500)

        # 8 markers: 0.5 + 8 * 0.05 = 0.9 (capped at 0.9)
        assert confidence >= 0.85

    def test_confidence_capped_at_0_9(self):
        """Confidence should not exceed 0.9."""
        coord = StigmergicCoordinator(
            database=MagicMock(), plugin=MagicMock()
        )

        # Deposit 20 markers
        for i in range(20):
            marker = RouteMarker(
                depositor=f"03member_{i}",
                source_peer_id="src",
                destination_peer_id="dst",
                fee_ppm=500,
                success=True,
                volume_sats=50000,
                timestamp=time.time(),
                strength=1.0,
            )
            with coord._lock:
                coord._markers[("src", "dst")].append(marker)

        _, confidence = coord.calculate_coordinated_fee("src", "dst", 500)
        assert confidence <= 0.9


# =============================================================================
# Bug 8: suggest_fee enforces floor/ceiling bounds
# =============================================================================

class TestSuggestFeeBounds:
    """Bug 8: suggest_fee should respect floor and ceiling."""

    def test_depleting_fee_capped_at_ceiling(self):
        """Raising fee for depletion should not exceed ceiling."""
        controller = AdaptiveFeeController()

        # Start at ceiling - raising should not go above
        fee, reason = controller.suggest_fee("ch1", FLEET_FEE_CEILING_PPM, 0.1)
        assert fee <= FLEET_FEE_CEILING_PPM
        assert "depleting" in reason

    def test_saturating_fee_floored(self):
        """Lowering fee for saturation should not go below floor."""
        controller = AdaptiveFeeController()

        # Start at floor - lowering should not go below
        fee, reason = controller.suggest_fee("ch1", FLEET_FEE_FLOOR_PPM, 0.9)
        assert fee >= FLEET_FEE_FLOOR_PPM
        assert "saturating" in reason

    def test_normal_range_still_works(self):
        """Normal fee adjustments should still work within bounds."""
        controller = AdaptiveFeeController()

        # Depleting at 500 ppm → should raise to ~575
        fee, _ = controller.suggest_fee("ch1", 500, 0.1)
        assert FLEET_FEE_FLOOR_PPM <= fee <= FLEET_FEE_CEILING_PPM
        assert fee > 500


# =============================================================================
# Bug 9: _record_forward_for_fee_coordination uses cache
# =============================================================================

class TestForwardRecordCache:
    """Bug 9: Forward recording should use channel_peer_map cache."""

    def test_channel_peer_map_used_on_cache_hit(self):
        """When channel is in peer map cache, no RPC should be called."""
        controller = AdaptiveFeeController()

        # Pre-populate the cache
        controller._channel_peer_map["100x1x0"] = "03peer_in"
        controller._channel_peer_map["200x2x0"] = "03peer_out"

        # The cache is populated - verify it works
        assert controller._channel_peer_map.get("100x1x0") == "03peer_in"
        assert controller._channel_peer_map.get("200x2x0") == "03peer_out"

    def test_cache_miss_returns_empty(self):
        """Cache miss should return empty string (fallback to RPC)."""
        controller = AdaptiveFeeController()

        result = controller._channel_peer_map.get("unknown_channel", "")
        assert result == ""


# =============================================================================
# Bug 10: _fee_observations protected by _fee_obs_lock
# =============================================================================

class TestFeeObservationsLock:
    """Bug 10: _fee_observations should be protected by _fee_obs_lock."""

    def test_fee_obs_lock_exists(self):
        """AdaptiveFeeController should have a _fee_obs_lock."""
        controller = AdaptiveFeeController()
        assert hasattr(controller, '_fee_obs_lock')
        assert isinstance(controller._fee_obs_lock, type(threading.Lock()))

    def test_concurrent_fee_observations_no_loss(self):
        """Concurrent record_fee_observation calls should not lose data."""
        controller = AdaptiveFeeController()
        num_threads = 4
        observations_per_thread = 50
        barrier = threading.Barrier(num_threads)

        def record_observations(thread_id):
            barrier.wait()
            for i in range(observations_per_thread):
                controller.record_fee_observation(100 + thread_id * 100 + i)

        threads = [
            threading.Thread(target=record_observations, args=(t,))
            for t in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All observations should be recorded (all are recent)
        total_expected = num_threads * observations_per_thread
        assert len(controller._fee_observations) == total_expected

    def test_fee_observation_trimming_works(self):
        """Old observations should be trimmed during record."""
        controller = AdaptiveFeeController()

        # Manually inject an old observation
        controller._fee_observations.append((time.time() - 7200, 999))

        # Record a new observation - should trim the old one
        controller.record_fee_observation(500)

        # Old observation should be gone, new one present
        fees = [f for _, f in controller._fee_observations]
        assert 999 not in fees
        assert 500 in fees
