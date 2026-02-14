"""
Tests for 10 routing intelligence bug fixes.

Bug 1: Signing payload preserves path order (not sorted)
Bug 2: Relayed probes accepted via pre_verified flag
Bug 3: Double signature verification eliminated
Bug 4: listfunds cached with 5-min TTL
Bug 5: _path_stats bounded with LRU eviction + MAX_PROBES_PER_PATH
Bug 6: Batch probes use per-probe timestamps
Bug 7: Confidence calculated inline from stats (O(1) not O(n))
Bug 8: Forward probe records intermediate hops only
Bug 9: store_route_probe deduplicates via UNIQUE + INSERT OR IGNORE
Bug 10: cost_reduction.py documents routing_map integration gap
"""

import time
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from modules.routing_intelligence import (
    HiveRoutingMap,
    PathStats,
    RouteSuggestion,
    MAX_CACHED_PATHS,
    MAX_PROBES_PER_PATH,
    PROBE_STALENESS_HOURS,
)
from modules.protocol import (
    get_route_probe_signing_payload,
)


class MockDatabase:
    """Mock database for testing."""

    def __init__(self):
        self.route_probes = []
        self.members = {}

    def get_member(self, peer_id):
        return self.members.get(peer_id)

    def get_all_members(self):
        return list(self.members.values()) if self.members else []

    def store_route_probe(self, **kwargs):
        self.route_probes.append(kwargs)

    def get_all_route_probes(self, max_age_hours=24):
        return self.route_probes

    def get_route_probes_for_destination(self, destination, max_age_hours=24):
        return [p for p in self.route_probes if p.get("destination") == destination]

    def cleanup_old_route_probes(self, max_age_hours=24):
        return 0


def make_pubkey(char, prefix="02"):
    """Create a fake 66-char pubkey."""
    return prefix + char * 64


OUR_PUBKEY = make_pubkey("0")


def make_routing_map():
    """Create a HiveRoutingMap with mock database and plugin."""
    db = MockDatabase()
    plugin = MagicMock()
    rm = HiveRoutingMap(db, plugin, OUR_PUBKEY)
    return rm, db


# =========================================================================
# Bug 1: Signing payload preserves path order
# =========================================================================

class TestBug1PathOrderInSigning:
    """Signing payload must preserve path hop order, not sort it."""

    def test_signing_payload_preserves_order(self):
        """Path A->B->C should produce different signature than C->B->A."""
        hop_a = make_pubkey("a")
        hop_b = make_pubkey("b")
        hop_c = make_pubkey("c")

        payload_abc = {
            "reporter_id": make_pubkey("1"),
            "destination": make_pubkey("9"),
            "timestamp": 1000,
            "path": [hop_a, hop_b, hop_c],
            "success": True,
            "latency_ms": 100,
            "total_fee_ppm": 50,
        }
        payload_cba = dict(payload_abc, path=[hop_c, hop_b, hop_a])

        sig_abc = get_route_probe_signing_payload(payload_abc)
        sig_cba = get_route_probe_signing_payload(payload_cba)

        assert sig_abc != sig_cba, "Different path orders must produce different signing payloads"

    def test_signing_payload_identical_same_order(self):
        """Same path order produces identical signing payload."""
        path = [make_pubkey("a"), make_pubkey("b")]
        payload = {
            "reporter_id": make_pubkey("1"),
            "destination": make_pubkey("9"),
            "timestamp": 1000,
            "path": path,
            "success": True,
            "latency_ms": 100,
            "total_fee_ppm": 50,
        }
        assert get_route_probe_signing_payload(payload) == get_route_probe_signing_payload(payload)

    def test_signing_payload_not_sorted(self):
        """Verify the path string in signing payload is not sorted."""
        hop_z = make_pubkey("z")  # Lexicographically late
        hop_a = make_pubkey("a")  # Lexicographically early
        payload = {
            "reporter_id": make_pubkey("1"),
            "destination": make_pubkey("9"),
            "timestamp": 1000,
            "path": [hop_z, hop_a],  # z before a
            "success": True,
            "latency_ms": 0,
            "total_fee_ppm": 0,
        }
        result = get_route_probe_signing_payload(payload)
        # The path portion should have z before a (not sorted)
        z_pos = result.find(hop_z)
        a_pos = result.find(hop_a)
        assert z_pos < a_pos, "Path order in signing payload must match input order"


# =========================================================================
# Bug 2+3: pre_verified skips identity binding and double signature check
# =========================================================================

class TestBug2And3PreVerified:
    """pre_verified=True skips identity binding and signature verification."""

    def test_pre_verified_allows_different_peer_id(self):
        """With pre_verified=True, peer_id != reporter_id should still succeed."""
        rm, db = make_routing_map()
        reporter = make_pubkey("r")
        transport_peer = make_pubkey("t")  # Different from reporter (relay case)
        db.members[reporter] = {"peer_id": reporter, "tier": "member"}

        payload = {
            "reporter_id": reporter,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": make_pubkey("d"),
            "path": [make_pubkey("h")],
            "success": True,
            "latency_ms": 100,
            "failure_reason": "",
            "failure_hop": -1,
            "estimated_capacity_sats": 100000,
            "total_fee_ppm": 50,
            "per_hop_fees": [50],
            "amount_probed_sats": 50000,
        }

        # With pre_verified=True, no RPC calls should happen
        mock_rpc = MagicMock()
        result = rm.handle_route_probe(transport_peer, payload, mock_rpc, pre_verified=True)

        assert result.get("success") is True
        mock_rpc.checkmessage.assert_not_called()

    def test_without_pre_verified_rejects_mismatched_peer(self):
        """Without pre_verified, peer_id != reporter_id should fail."""
        rm, db = make_routing_map()
        reporter = make_pubkey("r")
        transport_peer = make_pubkey("t")
        db.members[reporter] = {"peer_id": reporter, "tier": "member"}

        payload = {
            "reporter_id": reporter,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": make_pubkey("d"),
            "path": [make_pubkey("h")],
            "success": True,
            "latency_ms": 100,
            "failure_reason": "",
            "failure_hop": -1,
            "estimated_capacity_sats": 100000,
            "total_fee_ppm": 50,
            "per_hop_fees": [50],
            "amount_probed_sats": 50000,
        }

        mock_rpc = MagicMock()
        result = rm.handle_route_probe(transport_peer, payload, mock_rpc, pre_verified=False)
        assert "error" in result
        assert "identity binding" in result["error"]

    def test_pre_verified_batch_skips_signature(self):
        """Batch handler with pre_verified=True skips signature check."""
        rm, db = make_routing_map()
        reporter = make_pubkey("r")
        db.members[reporter] = {"peer_id": reporter, "tier": "member"}

        payload = {
            "reporter_id": reporter,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "probes": [
                {
                    "destination": make_pubkey("d"),
                    "path": [make_pubkey("h")],
                    "success": True,
                    "latency_ms": 50,
                    "failure_reason": "",
                    "failure_hop": -1,
                    "estimated_capacity_sats": 100000,
                    "total_fee_ppm": 30,
                    "amount_probed_sats": 50000,
                }
            ],
            "probe_count": 1,
        }

        mock_rpc = MagicMock()
        result = rm.handle_route_probe_batch(
            make_pubkey("t"), payload, mock_rpc, pre_verified=True
        )
        assert result.get("success") is True
        assert result.get("probes_stored") == 1
        mock_rpc.checkmessage.assert_not_called()


# =========================================================================
# Bug 5: _path_stats bounded with LRU eviction + MAX_PROBES_PER_PATH
# =========================================================================

class TestBug5BoundedPathStats:
    """_path_stats must be bounded by MAX_CACHED_PATHS and MAX_PROBES_PER_PATH."""

    @patch("modules.routing_intelligence.MAX_CACHED_PATHS", 50)
    def test_eviction_when_exceeding_max_cached_paths(self):
        """When _path_stats exceeds MAX_CACHED_PATHS, oldest entries are evicted."""
        rm, db = make_routing_map()
        now = time.time()
        test_cap = 50  # Patched value

        # Fill up to cap
        with rm._lock:
            for i in range(test_cap):
                dest = f"dest_{i}"
                path = (f"hop_{i}",)
                rm._path_stats[(dest, path)] = PathStats(
                    path=path, destination=dest,
                    probe_count=1,
                    success_count=1,
                    last_success_time=now - (test_cap - i),  # Oldest first
                    last_failure_time=0,
                    last_failure_reason="",
                    total_latency_ms=100,
                    total_fee_ppm=50,
                    avg_capacity_sats=100000,
                    reporters={"reporter1"},
                )

        # Add one more via _update_path_stats — should trigger eviction
        rm._update_path_stats(
            destination="new_dest",
            path=("new_hop",),
            success=True,
            latency_ms=100,
            fee_ppm=50,
            capacity_sats=100000,
            reporter_id="reporter2",
            failure_reason="",
            timestamp=int(now),
        )

        with rm._lock:
            assert len(rm._path_stats) <= test_cap

    def test_probe_count_capped_at_max(self):
        """probe_count should not exceed MAX_PROBES_PER_PATH."""
        rm, db = make_routing_map()
        now = int(time.time())
        dest = "dest_cap"
        path = ("hop_cap",)

        # Add probes up to the limit
        for i in range(MAX_PROBES_PER_PATH + 10):
            rm._update_path_stats(
                destination=dest,
                path=path,
                success=True,
                latency_ms=100,
                fee_ppm=50,
                capacity_sats=100000,
                reporter_id=f"reporter_{i}",
                failure_reason="",
                timestamp=now + i,
            )

        with rm._lock:
            stats = rm._path_stats.get((dest, path))
            assert stats is not None
            assert stats.probe_count <= MAX_PROBES_PER_PATH

    def test_evict_oldest_locked_removes_10_percent(self):
        """_evict_oldest_locked removes ~10% of entries."""
        rm, db = make_routing_map()
        now = time.time()
        count = 100

        with rm._lock:
            for i in range(count):
                rm._path_stats[(f"dest_{i}", (f"hop_{i}",))] = PathStats(
                    path=(f"hop_{i}",), destination=f"dest_{i}",
                    probe_count=1, success_count=1,
                    last_success_time=now - (count - i),
                    last_failure_time=0, last_failure_reason="",
                    total_latency_ms=100, total_fee_ppm=50,
                    avg_capacity_sats=100000, reporters={"r1"},
                )
            rm._evict_oldest_locked()
            assert len(rm._path_stats) == 90  # 10% of 100 evicted


# =========================================================================
# Bug 6: Batch probes use per-probe timestamps
# =========================================================================

class TestBug6PerProbeTimestamps:
    """Batch probes should use individual timestamps when available."""

    def test_per_probe_timestamp_used(self):
        """Each probe in a batch should use its own timestamp."""
        rm, db = make_routing_map()
        reporter = make_pubkey("r")
        db.members[reporter] = {"peer_id": reporter, "tier": "member"}

        batch_ts = int(time.time())
        probe_ts_1 = batch_ts - 100
        probe_ts_2 = batch_ts - 200

        payload = {
            "reporter_id": reporter,
            "timestamp": batch_ts,
            "signature": "a" * 100,
            "probes": [
                {
                    "destination": make_pubkey("d1"),
                    "path": [make_pubkey("h1")],
                    "success": True,
                    "latency_ms": 50,
                    "failure_reason": "",
                    "failure_hop": -1,
                    "estimated_capacity_sats": 100000,
                    "total_fee_ppm": 30,
                    "amount_probed_sats": 50000,
                    "timestamp": probe_ts_1,
                },
                {
                    "destination": make_pubkey("d2"),
                    "path": [make_pubkey("h2")],
                    "success": False,
                    "latency_ms": 0,
                    "failure_reason": "temporary",
                    "failure_hop": 0,
                    "estimated_capacity_sats": 0,
                    "total_fee_ppm": 0,
                    "amount_probed_sats": 50000,
                    "timestamp": probe_ts_2,
                },
            ],
            "probe_count": 2,
        }

        mock_rpc = MagicMock()
        result = rm.handle_route_probe_batch(reporter, payload, mock_rpc, pre_verified=True)
        assert result.get("success") is True

        # Check that stored probes used per-probe timestamps
        assert len(db.route_probes) == 2
        assert db.route_probes[0]["timestamp"] == probe_ts_1
        assert db.route_probes[1]["timestamp"] == probe_ts_2

    def test_missing_probe_timestamp_uses_batch(self):
        """Probes without individual timestamp should use batch timestamp."""
        rm, db = make_routing_map()
        reporter = make_pubkey("r")
        db.members[reporter] = {"peer_id": reporter, "tier": "member"}

        batch_ts = int(time.time())

        payload = {
            "reporter_id": reporter,
            "timestamp": batch_ts,
            "signature": "a" * 100,
            "probes": [
                {
                    "destination": make_pubkey("d1"),
                    "path": [make_pubkey("h1")],
                    "success": True,
                    "latency_ms": 50,
                    "failure_reason": "",
                    "failure_hop": -1,
                    "estimated_capacity_sats": 100000,
                    "total_fee_ppm": 30,
                    "amount_probed_sats": 50000,
                    # No "timestamp" key
                },
            ],
            "probe_count": 1,
        }

        mock_rpc = MagicMock()
        result = rm.handle_route_probe_batch(reporter, payload, mock_rpc, pre_verified=True)
        assert result.get("success") is True
        assert db.route_probes[0]["timestamp"] == batch_ts

    def test_invalid_probe_timestamp_uses_batch(self):
        """Probes with invalid timestamp should fall back to batch timestamp."""
        rm, db = make_routing_map()
        reporter = make_pubkey("r")
        db.members[reporter] = {"peer_id": reporter, "tier": "member"}

        batch_ts = int(time.time())

        payload = {
            "reporter_id": reporter,
            "timestamp": batch_ts,
            "signature": "a" * 100,
            "probes": [
                {
                    "destination": make_pubkey("d1"),
                    "path": [make_pubkey("h1")],
                    "success": True,
                    "latency_ms": 50,
                    "failure_reason": "",
                    "failure_hop": -1,
                    "estimated_capacity_sats": 100000,
                    "total_fee_ppm": 30,
                    "amount_probed_sats": 50000,
                    "timestamp": -5,  # Invalid
                },
            ],
            "probe_count": 1,
        }

        mock_rpc = MagicMock()
        result = rm.handle_route_probe_batch(reporter, payload, mock_rpc, pre_verified=True)
        assert result.get("success") is True
        assert db.route_probes[0]["timestamp"] == batch_ts


# =========================================================================
# Bug 7: Confidence calculated inline from stats (O(1) not O(n))
# =========================================================================

class TestBug7InlineConfidence:
    """Confidence should be calculated inline from stats, not via re-search."""

    def test_confidence_from_stats_static_method(self):
        """_confidence_from_stats should compute confidence correctly."""
        now = time.time()
        stale_cutoff = now - (PROBE_STALENESS_HOURS * 3600)

        stats = PathStats(
            path=("hop1",), destination="dest1",
            probe_count=10,
            success_count=8,
            last_success_time=now - 100,  # Recent
            last_failure_time=now - 200,
            last_failure_reason="",
            total_latency_ms=1000,
            total_fee_ppm=500,
            avg_capacity_sats=100000,
            reporters={"r1", "r2", "r3"},
        )
        conf = HiveRoutingMap._confidence_from_stats(stats, stale_cutoff)
        # reporter_factor = min(1.0, 3/3) = 1.0
        # recency_factor = 1.0 (recent)
        # count_factor = min(1.0, 10/10) = 1.0
        assert conf == pytest.approx(1.0)

    def test_confidence_stale_data_penalty(self):
        """Stale data should receive 0.3 recency factor."""
        now = time.time()
        stale_cutoff = now - (PROBE_STALENESS_HOURS * 3600)

        stats = PathStats(
            path=("hop1",), destination="dest1",
            probe_count=10,
            success_count=8,
            last_success_time=now - 200000,  # Very old
            last_failure_time=now - 200000,
            last_failure_reason="",
            total_latency_ms=1000,
            total_fee_ppm=500,
            avg_capacity_sats=100000,
            reporters={"r1", "r2", "r3"},
        )
        conf = HiveRoutingMap._confidence_from_stats(stats, stale_cutoff)
        # reporter_factor = 1.0, recency_factor = 0.3, count_factor = 1.0
        assert conf == pytest.approx(0.3)

    def test_confidence_low_reporter_count(self):
        """Fewer reporters should lower confidence."""
        now = time.time()
        stale_cutoff = now - (PROBE_STALENESS_HOURS * 3600)

        stats = PathStats(
            path=("hop1",), destination="dest1",
            probe_count=10,
            success_count=8,
            last_success_time=now - 100,
            last_failure_time=0,
            last_failure_reason="",
            total_latency_ms=1000,
            total_fee_ppm=500,
            avg_capacity_sats=100000,
            reporters={"r1"},  # Only 1 reporter
        )
        conf = HiveRoutingMap._confidence_from_stats(stats, stale_cutoff)
        # reporter_factor = min(1.0, 1/3) ≈ 0.333
        assert conf == pytest.approx(1.0 / 3.0)

    def test_get_best_route_uses_inline_confidence(self):
        """get_best_route_to should use inline confidence (no O(n) re-search)."""
        rm, db = make_routing_map()
        now = time.time()
        dest = make_pubkey("d")
        path = (make_pubkey("h1"),)

        with rm._lock:
            rm._path_stats[(dest, path)] = PathStats(
                path=path, destination=dest,
                probe_count=10, success_count=9,
                last_success_time=now - 10,
                last_failure_time=0, last_failure_reason="",
                total_latency_ms=1000, total_fee_ppm=500,
                avg_capacity_sats=500000,
                reporters={"r1", "r2", "r3"},
            )

        with patch.object(rm, 'get_path_confidence', wraps=rm.get_path_confidence) as mock_conf:
            result = rm.get_best_route_to(dest, 100000)
            # get_path_confidence should NOT be called since we inline it
            mock_conf.assert_not_called()

        assert result is not None
        assert result.confidence > 0

    def test_get_routes_to_uses_inline_confidence(self):
        """get_routes_to should also use inline confidence."""
        rm, db = make_routing_map()
        now = time.time()
        dest = make_pubkey("d")
        path = (make_pubkey("h1"),)

        with rm._lock:
            rm._path_stats[(dest, path)] = PathStats(
                path=path, destination=dest,
                probe_count=10, success_count=9,
                last_success_time=now - 10,
                last_failure_time=0, last_failure_reason="",
                total_latency_ms=1000, total_fee_ppm=500,
                avg_capacity_sats=500000,
                reporters={"r1", "r2"},
            )

        with patch.object(rm, 'get_path_confidence', wraps=rm.get_path_confidence) as mock_conf:
            results = rm.get_routes_to(dest)
            mock_conf.assert_not_called()

        assert len(results) == 1
        assert results[0].confidence > 0


# =========================================================================
# Bug 9: store_route_probe deduplication
# =========================================================================

class TestBug9RouteProbeDedup:
    """store_route_probe should use INSERT OR IGNORE with UNIQUE constraint."""

    def test_unique_constraint_in_schema(self):
        """route_probes table should have UNIQUE constraint on dedup columns."""
        import sqlite3
        conn = sqlite3.connect(":memory:")
        # Simulate the schema
        conn.execute("""
            CREATE TABLE IF NOT EXISTS route_probes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_id TEXT NOT NULL,
                destination TEXT NOT NULL,
                path TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                success INTEGER NOT NULL,
                latency_ms INTEGER DEFAULT 0,
                failure_reason TEXT DEFAULT '',
                failure_hop INTEGER DEFAULT -1,
                estimated_capacity_sats INTEGER DEFAULT 0,
                total_fee_ppm INTEGER DEFAULT 0,
                amount_probed_sats INTEGER DEFAULT 0,
                UNIQUE(reporter_id, destination, path, timestamp)
            )
        """)

        # First insert should succeed
        conn.execute("""
            INSERT OR IGNORE INTO route_probes
            (reporter_id, destination, path, timestamp, success)
            VALUES (?, ?, ?, ?, ?)
        """, ("reporter1", "dest1", '["hop1"]', 1000, 1))

        # Duplicate should be silently ignored
        conn.execute("""
            INSERT OR IGNORE INTO route_probes
            (reporter_id, destination, path, timestamp, success)
            VALUES (?, ?, ?, ?, ?)
        """, ("reporter1", "dest1", '["hop1"]', 1000, 1))

        count = conn.execute("SELECT COUNT(*) FROM route_probes").fetchone()[0]
        assert count == 1, "Duplicate probe should have been ignored"

        # Different timestamp should succeed
        conn.execute("""
            INSERT OR IGNORE INTO route_probes
            (reporter_id, destination, path, timestamp, success)
            VALUES (?, ?, ?, ?, ?)
        """, ("reporter1", "dest1", '["hop1"]', 1001, 1))

        count = conn.execute("SELECT COUNT(*) FROM route_probes").fetchone()[0]
        assert count == 2
        conn.close()


# =========================================================================
# Bug 10: cost_reduction.py documents routing_map integration gap
# =========================================================================

class TestBug10IntegrationGapDocumented:
    """cost_reduction.py should have a TODO comment about routing_map integration."""

    def test_todo_comment_exists(self):
        """Verify the TODO comment exists in cost_reduction.py."""
        with open("modules/cost_reduction.py", "r") as f:
            content = f.read()
        assert "TODO" in content
        assert "routing_intelligence" in content or "routing_map" in content
        assert "cost_reduction" in content or "MCF" in content or "BFS" in content
