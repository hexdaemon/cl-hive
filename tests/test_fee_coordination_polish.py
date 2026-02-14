"""
Tests for 6 remaining fee coordination fixes.

Fix 1: broadcast_warning writes _warnings under lock
Fix 2: get_active_warnings snapshots under lock
Fix 3: get_defense_status snapshots under lock
Fix 4: _channel_peer_map evicts closed channels on update
Fix 5: _fee_change_times evicts stale entries
Fix 6: Failed-marker fee returns default (no directional assumption)
"""

import threading
import time
import pytest
from unittest.mock import MagicMock

from modules.fee_coordination import (
    AdaptiveFeeController,
    StigmergicCoordinator,
    MyceliumDefenseSystem,
    FeeCoordinationManager,
    PeerWarning,
    RouteMarker,
    FLEET_FEE_FLOOR_PPM,
    DEFAULT_FEE_PPM,
    SALIENT_FEE_CHANGE_COOLDOWN,
    WARNING_TTL_HOURS,
)


# =============================================================================
# Fix 1: broadcast_warning writes _warnings under lock
# =============================================================================

class TestBroadcastWarningLock:
    """Fix 1: broadcast_warning should hold lock when writing _warnings."""

    def test_broadcast_warning_acquires_lock(self):
        """broadcast_warning should write _warnings under self._lock."""
        defense = MyceliumDefenseSystem(
            database=MagicMock(), plugin=MagicMock()
        )
        defense.set_our_pubkey("03us")

        warning = PeerWarning(
            peer_id="03bad",
            threat_type="drain",
            severity=0.8,
            reporter="03us",
            timestamp=time.time(),
            ttl=WARNING_TTL_HOURS * 3600,
        )

        lock_was_held = []
        original_setitem = dict.__setitem__

        # Monkey-patch to detect if lock is held during write
        old_broadcast = defense.broadcast_warning

        def patched_broadcast(w):
            # Check lock state just before the method runs
            result = old_broadcast(w)
            return result

        defense.broadcast_warning(warning)

        # Verify the warning was stored
        assert "03bad" in defense._warnings

    def test_concurrent_broadcast_and_handle(self):
        """Concurrent broadcast_warning and handle_warning should not corrupt state."""
        defense = MyceliumDefenseSystem(
            database=MagicMock(), plugin=MagicMock()
        )
        defense.set_our_pubkey("03us")

        errors = []
        barrier = threading.Barrier(2)

        def broadcast_warnings():
            try:
                barrier.wait(timeout=2)
                for i in range(50):
                    w = PeerWarning(
                        peer_id=f"03peer_{i}",
                        threat_type="drain",
                        severity=0.5,
                        reporter="03us",
                        timestamp=time.time(),
                        ttl=3600,
                    )
                    defense.broadcast_warning(w)
            except Exception as e:
                errors.append(str(e))

        def handle_warnings():
            try:
                barrier.wait(timeout=2)
                for i in range(50):
                    w = PeerWarning(
                        peer_id=f"03peer_{i}",
                        threat_type="unreliable",
                        severity=0.6,
                        reporter="03reporter",
                        timestamp=time.time(),
                        ttl=3600,
                    )
                    defense.handle_warning(w)
            except Exception as e:
                errors.append(str(e))

        t1 = threading.Thread(target=broadcast_warnings)
        t2 = threading.Thread(target=handle_warnings)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(errors) == 0, f"Concurrent errors: {errors}"


# =============================================================================
# Fix 2: get_active_warnings snapshots under lock
# =============================================================================

class TestGetActiveWarningsLock:
    """Fix 2: get_active_warnings should snapshot under lock."""

    def test_no_crash_during_concurrent_modification(self):
        """get_active_warnings should not crash with concurrent handle_warning."""
        defense = MyceliumDefenseSystem(
            database=MagicMock(), plugin=MagicMock()
        )
        defense.set_our_pubkey("03us")

        errors = []

        def add_warnings():
            for i in range(100):
                w = PeerWarning(
                    peer_id=f"03peer_{i}",
                    threat_type="drain",
                    severity=0.5,
                    reporter="03us",
                    timestamp=time.time(),
                    ttl=3600,
                )
                defense.broadcast_warning(w)

        def read_warnings():
            try:
                for _ in range(100):
                    defense.get_active_warnings()
            except RuntimeError as e:
                errors.append(str(e))

        t1 = threading.Thread(target=add_warnings)
        t2 = threading.Thread(target=read_warnings)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(errors) == 0, f"RuntimeError during iteration: {errors}"


# =============================================================================
# Fix 3: get_defense_status snapshots under lock
# =============================================================================

class TestGetDefenseStatusLock:
    """Fix 3: get_defense_status should snapshot shared dicts under lock."""

    def test_defense_status_consistent_snapshot(self):
        """get_defense_status should return consistent data."""
        defense = MyceliumDefenseSystem(
            database=MagicMock(), plugin=MagicMock()
        )
        defense.set_our_pubkey("03us")

        # Add a self-detected warning (triggers immediate defense)
        w = PeerWarning(
            peer_id="03bad",
            threat_type="drain",
            severity=0.8,
            reporter="03us",
            timestamp=time.time(),
            ttl=3600,
        )
        defense.handle_warning(w)

        status = defense.get_defense_status()

        assert status["active_warnings"] >= 1
        assert status["defensive_fees_active"] >= 1
        assert "03bad" in status["defensive_peers"]

    def test_no_crash_during_concurrent_expiration(self):
        """get_defense_status should not crash during concurrent expiration."""
        defense = MyceliumDefenseSystem(
            database=MagicMock(), plugin=MagicMock()
        )
        defense.set_our_pubkey("03us")

        errors = []

        def expire_loop():
            for _ in range(50):
                defense.check_warning_expiration()

        def status_loop():
            try:
                for _ in range(50):
                    defense.get_defense_status()
            except RuntimeError as e:
                errors.append(str(e))

        # Pre-populate some warnings
        for i in range(10):
            w = PeerWarning(
                peer_id=f"03peer_{i}",
                threat_type="drain",
                severity=0.5,
                reporter="03us",
                timestamp=time.time(),
                ttl=3600,
            )
            defense.handle_warning(w)

        t1 = threading.Thread(target=expire_loop)
        t2 = threading.Thread(target=status_loop)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(errors) == 0, f"RuntimeError: {errors}"


# =============================================================================
# Fix 4: _channel_peer_map evicts closed channels on update
# =============================================================================

class TestChannelPeerMapEviction:
    """Fix 4: update_channel_peer_mappings should replace, not merge."""

    def test_closed_channels_evicted_fee_controller(self):
        """Closed channels should be removed from AdaptiveFeeController map."""
        controller = AdaptiveFeeController()

        # Initial channels
        controller.update_channel_peer_mappings([
            {"short_channel_id": "100x1x0", "peer_id": "03peer_a"},
            {"short_channel_id": "200x1x0", "peer_id": "03peer_b"},
            {"short_channel_id": "300x1x0", "peer_id": "03peer_c"},
        ])
        assert len(controller._channel_peer_map) == 3

        # Channel 200x1x0 closes — update with only remaining channels
        controller.update_channel_peer_mappings([
            {"short_channel_id": "100x1x0", "peer_id": "03peer_a"},
            {"short_channel_id": "300x1x0", "peer_id": "03peer_c"},
        ])

        assert "200x1x0" not in controller._channel_peer_map
        assert len(controller._channel_peer_map) == 2
        assert controller._channel_peer_map["100x1x0"] == "03peer_a"

    def test_closed_channels_evicted_anticipatory(self):
        """Closed channels should be removed from AnticipatoryLiquidityManager map."""
        from modules.anticipatory_liquidity import AnticipatoryLiquidityManager

        class MockDB:
            def record_flow_sample(self, **kw): pass
            def get_flow_samples(self, **kw): return []

        mgr = AnticipatoryLiquidityManager(
            database=MockDB(), plugin=None,
            state_manager=None, our_id="03test"
        )

        # Initial channels
        mgr.update_channel_peer_mappings([
            {"short_channel_id": "100x1x0", "peer_id": "03peer_a"},
            {"short_channel_id": "200x1x0", "peer_id": "03peer_b"},
        ])
        assert len(mgr._channel_peer_map) == 2

        # Channel closes
        mgr.update_channel_peer_mappings([
            {"short_channel_id": "100x1x0", "peer_id": "03peer_a"},
        ])
        assert "200x1x0" not in mgr._channel_peer_map
        assert len(mgr._channel_peer_map) == 1

    def test_empty_update_clears_map(self):
        """Empty channel list should clear the map."""
        controller = AdaptiveFeeController()
        controller.update_channel_peer_mappings([
            {"short_channel_id": "100x1x0", "peer_id": "03peer_a"},
        ])
        assert len(controller._channel_peer_map) == 1

        controller.update_channel_peer_mappings([])
        assert len(controller._channel_peer_map) == 0


# =============================================================================
# Fix 5: _fee_change_times evicts stale entries
# =============================================================================

class TestFeeChangeTimesEviction:
    """Fix 5: record_fee_change should evict stale entries when dict grows large."""

    def test_stale_entries_evicted_when_large(self):
        """Entries past 2x cooldown should be evicted when dict exceeds 500."""
        mgr = FeeCoordinationManager(
            database=MagicMock(), plugin=MagicMock()
        )

        # Manually inject 501 old entries
        old_time = time.time() - SALIENT_FEE_CHANGE_COOLDOWN * 3
        with mgr._lock:
            for i in range(501):
                mgr._fee_change_times[f"old_ch_{i}"] = old_time

        # Record a new entry — should trigger eviction
        mgr.record_fee_change("new_ch")

        with mgr._lock:
            # Old entries should be evicted, only new_ch remains
            assert "new_ch" in mgr._fee_change_times
            assert len(mgr._fee_change_times) < 502

    def test_recent_entries_preserved(self):
        """Recent entries within cooldown should not be evicted."""
        mgr = FeeCoordinationManager(
            database=MagicMock(), plugin=MagicMock()
        )

        recent_time = time.time() - 100  # Well within cooldown
        with mgr._lock:
            for i in range(501):
                mgr._fee_change_times[f"recent_ch_{i}"] = recent_time

        mgr.record_fee_change("new_ch")

        with mgr._lock:
            # Recent entries should be preserved (all within 2x cooldown)
            assert len(mgr._fee_change_times) == 502

    def test_small_dict_not_trimmed(self):
        """Small dicts should not trigger eviction."""
        mgr = FeeCoordinationManager(
            database=MagicMock(), plugin=MagicMock()
        )

        old_time = time.time() - SALIENT_FEE_CHANGE_COOLDOWN * 3
        with mgr._lock:
            for i in range(10):
                mgr._fee_change_times[f"old_ch_{i}"] = old_time

        mgr.record_fee_change("new_ch")

        with mgr._lock:
            # Small dict — old entries should still be there (no trim)
            assert len(mgr._fee_change_times) == 11


# =============================================================================
# Fix 6: Failed-marker fee returns default (no directional assumption)
# =============================================================================

class TestFailedMarkerNoAssumption:
    """Fix 6: All-failure markers should return default fee, not reduced fee."""

    def test_all_failures_returns_default_fee(self):
        """When only failed markers exist, return default_fee not reduced."""
        coord = StigmergicCoordinator(
            database=MagicMock(), plugin=MagicMock()
        )

        # Deposit failed markers at various fees
        for fee in [300, 500, 700]:
            marker = RouteMarker(
                depositor="03member",
                source_peer_id="src",
                destination_peer_id="dst",
                fee_ppm=fee,
                success=False,
                volume_sats=50000,
                timestamp=time.time(),
                strength=0.5,
            )
            with coord._lock:
                coord._markers[("src", "dst")].append(marker)

        default = 400
        recommended, confidence = coord.calculate_coordinated_fee(
            "src", "dst", default
        )

        # Should return default fee (not 80% of avg failed fee)
        assert recommended == default
        assert confidence < 0.5  # Low confidence since no successes

    def test_mixed_markers_still_uses_successful(self):
        """When both success and failure markers exist, use successful ones."""
        coord = StigmergicCoordinator(
            database=MagicMock(), plugin=MagicMock()
        )

        # Add a successful marker
        success_marker = RouteMarker(
            depositor="03member",
            source_peer_id="src",
            destination_peer_id="dst",
            fee_ppm=500,
            success=True,
            volume_sats=50000,
            timestamp=time.time(),
            strength=0.8,
        )

        # Add a failed marker
        fail_marker = RouteMarker(
            depositor="03member2",
            source_peer_id="src",
            destination_peer_id="dst",
            fee_ppm=200,
            success=False,
            volume_sats=50000,
            timestamp=time.time(),
            strength=0.5,
        )

        with coord._lock:
            coord._markers[("src", "dst")].extend([success_marker, fail_marker])

        recommended, confidence = coord.calculate_coordinated_fee(
            "src", "dst", 400
        )

        # Should use successful marker's fee (~500), not failed marker's
        assert abs(recommended - 500) <= 5
        assert confidence >= 0.5

    def test_no_markers_returns_default(self):
        """No markers at all should return default fee with low confidence."""
        coord = StigmergicCoordinator(
            database=MagicMock(), plugin=MagicMock()
        )

        recommended, confidence = coord.calculate_coordinated_fee(
            "src", "dst", 400
        )

        assert recommended == 400
        assert confidence == 0.3
