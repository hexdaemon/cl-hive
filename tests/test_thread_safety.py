"""
Tests for thread safety fixes from audit 2026-02-10.

Tests cover:
- H-1: HiveRoutingMap._path_stats lock under concurrent access
- M-3: LiquidityCoordinator rate dict lock under concurrent access
"""

import threading
import time
import pytest
from unittest.mock import MagicMock

from modules.routing_intelligence import HiveRoutingMap, PathStats


class TestRoutingMapThreadSafety:
    """Test that HiveRoutingMap operations don't crash under concurrent access."""

    def _make_routing_map(self):
        db = MagicMock()
        db.get_all_route_probes.return_value = []
        plugin = MagicMock()
        return HiveRoutingMap(database=db, plugin=plugin, our_pubkey="02" + "aa" * 32)

    def test_concurrent_update_and_read(self):
        """Hammer _update_path_stats and get_routing_stats simultaneously."""
        routing_map = self._make_routing_map()
        errors = []
        stop = threading.Event()

        def writer():
            i = 0
            while not stop.is_set():
                try:
                    dest = f"02{'bb' * 32}"
                    path = (f"02{'cc' * 32}", f"02{'dd' * 32}")
                    routing_map._update_path_stats(
                        destination=dest,
                        path=path,
                        success=True,
                        latency_ms=100 + i,
                        fee_ppm=50,
                        capacity_sats=1000000,
                        reporter_id=f"02{'ee' * 32}",
                        failure_reason="",
                        timestamp=int(time.time())
                    )
                    i += 1
                except Exception as e:
                    errors.append(f"writer: {e}")

        def reader():
            while not stop.is_set():
                try:
                    routing_map.get_routing_stats()
                    routing_map.get_path_success_rate([f"02{'cc' * 32}", f"02{'dd' * 32}"])
                    routing_map.get_path_confidence([f"02{'cc' * 32}", f"02{'dd' * 32}"])
                except Exception as e:
                    errors.append(f"reader: {e}")

        threads = []
        for _ in range(3):
            t = threading.Thread(target=writer, daemon=True)
            threads.append(t)
            t.start()
        for _ in range(3):
            t = threading.Thread(target=reader, daemon=True)
            threads.append(t)
            t.start()

        time.sleep(0.5)
        stop.set()
        for t in threads:
            t.join(timeout=2)

        assert errors == [], f"Thread safety errors: {errors}"

    def test_concurrent_cleanup_and_update(self):
        """Test cleanup_stale_data concurrent with updates."""
        routing_map = self._make_routing_map()
        errors = []
        stop = threading.Event()

        # Seed some data
        for i in range(20):
            routing_map._update_path_stats(
                destination=f"02{'bb' * 32}",
                path=(f"02{i:02d}" + "cc" * 31,),
                success=True,
                latency_ms=100,
                fee_ppm=50,
                capacity_sats=1000000,
                reporter_id=f"02{'ee' * 32}",
                failure_reason="",
                timestamp=1  # Old timestamp to be cleaned up
            )

        def cleaner():
            while not stop.is_set():
                try:
                    routing_map.cleanup_stale_data()
                except Exception as e:
                    errors.append(f"cleaner: {e}")

        def writer():
            while not stop.is_set():
                try:
                    routing_map._update_path_stats(
                        destination=f"02{'bb' * 32}",
                        path=(f"02{'ff' * 32}",),
                        success=True,
                        latency_ms=100,
                        fee_ppm=50,
                        capacity_sats=1000000,
                        reporter_id=f"02{'ee' * 32}",
                        failure_reason="",
                        timestamp=int(time.time())
                    )
                except Exception as e:
                    errors.append(f"writer: {e}")

        t1 = threading.Thread(target=cleaner, daemon=True)
        t2 = threading.Thread(target=writer, daemon=True)
        t1.start()
        t2.start()

        time.sleep(0.3)
        stop.set()
        t1.join(timeout=2)
        t2.join(timeout=2)

        assert errors == [], f"Thread safety errors: {errors}"

    def test_has_lock_attribute(self):
        """Verify the lock was added."""
        routing_map = self._make_routing_map()
        assert hasattr(routing_map, '_lock')
        assert isinstance(routing_map._lock, type(threading.Lock()))


class TestLiquidityCoordinatorRateLock:
    """Test that LiquidityCoordinator rate limiting is thread-safe."""

    def test_has_rate_lock(self):
        """Verify the rate lock was added."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        db = MagicMock()
        plugin = MagicMock()
        lc = LiquidityCoordinator(database=db, plugin=plugin, our_pubkey="02" + "aa" * 32)
        assert hasattr(lc, '_rate_lock')
        assert isinstance(lc._rate_lock, type(threading.Lock()))

    def test_concurrent_rate_limiting(self):
        """Test rate limiting under concurrent access."""
        from modules.liquidity_coordinator import LiquidityCoordinator
        from modules.protocol import LIQUIDITY_NEED_RATE_LIMIT

        db = MagicMock()
        plugin = MagicMock()
        lc = LiquidityCoordinator(database=db, plugin=plugin, our_pubkey="02" + "aa" * 32)
        errors = []
        stop = threading.Event()

        def check_rates():
            while not stop.is_set():
                try:
                    sender = f"02{'bb' * 32}"
                    lc._check_rate_limit(sender, lc._need_rate, LIQUIDITY_NEED_RATE_LIMIT)
                    lc._record_message(sender, lc._need_rate)
                except Exception as e:
                    errors.append(str(e))

        threads = [threading.Thread(target=check_rates, daemon=True) for _ in range(4)]
        for t in threads:
            t.start()

        time.sleep(0.3)
        stop.set()
        for t in threads:
            t.join(timeout=2)

        assert errors == [], f"Rate limit thread safety errors: {errors}"
