"""
Tests for Routing Intelligence functionality (Phase 7.4).

Tests cover:
- HiveRoutingMap class
- ROUTE_PROBE payload validation
- Path success rate tracking
- Route suggestions
- Rate limiting
- Database integration
"""

import pytest
import time
from unittest.mock import MagicMock, patch

from modules.routing_intelligence import (
    HiveRoutingMap,
    PathStats,
    RouteSuggestion,
    HIGH_SUCCESS_RATE,
    LOW_SUCCESS_RATE,
    PROBE_STALENESS_HOURS,
)
from modules.protocol import (
    validate_route_probe_payload,
    validate_route_probe_batch_payload,
    get_route_probe_signing_payload,
    get_route_probe_batch_signing_payload,
    create_route_probe,
    create_route_probe_batch,
    ROUTE_PROBE_RATE_LIMIT,
    ROUTE_PROBE_BATCH_RATE_LIMIT,
    MAX_PATH_LENGTH,
    MAX_PROBES_IN_BATCH,
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

    def get_route_probe_stats(self, destination):
        probes = self.get_route_probes_for_destination(destination)
        if not probes:
            return None
        success = sum(1 for p in probes if p.get("success"))
        return {
            "destination": destination,
            "total_probes": len(probes),
            "success_count": success,
            "success_rate": success / len(probes) if probes else 0,
        }

    def cleanup_old_route_probes(self, max_age_hours=24):
        return 0


class TestRouteProbePayload:
    """Test ROUTE_PROBE payload validation."""

    def test_valid_payload(self):
        """Test that valid payload passes validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": "03" + "b" * 64,
            "path": ["02" + "c" * 64, "02" + "d" * 64],
            "success": True,
            "latency_ms": 500,
            "failure_reason": "",
            "failure_hop": -1,
            "estimated_capacity_sats": 1000000,
            "total_fee_ppm": 150,
            "per_hop_fees": [100, 50],
            "amount_probed_sats": 500000,
        }
        assert validate_route_probe_payload(payload) is True

    def test_missing_reporter(self):
        """Test that missing reporter fails validation."""
        payload = {
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": "03" + "b" * 64,
            "path": ["02" + "c" * 64],
            "success": True,
        }
        assert validate_route_probe_payload(payload) is False

    def test_invalid_pubkey_format(self):
        """Test that short/invalid reporter passes basic validation.

        Note: Pubkey format validation is done at the signature verification
        stage, not during basic payload validation.
        """
        payload = {
            "reporter_id": "invalid",
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": "03" + "b" * 64,
            "path": ["02" + "c" * 64],
            "success": True,
        }
        # Basic validation passes - actual pubkey verification happens at signature check
        assert validate_route_probe_payload(payload) is True

    def test_path_too_long(self):
        """Test that paths exceeding max length fail validation."""
        long_path = ["02" + "a" * 64] * (MAX_PATH_LENGTH + 1)
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": "03" + "b" * 64,
            "path": long_path,
            "success": True,
        }
        assert validate_route_probe_payload(payload) is False

    def test_invalid_failure_reason(self):
        """Test that invalid failure reason fails validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": "03" + "b" * 64,
            "path": ["02" + "c" * 64],
            "success": False,
            "failure_reason": "invalid_reason",
        }
        assert validate_route_probe_payload(payload) is False

    def test_negative_latency(self):
        """Test that negative latency fails validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": "03" + "b" * 64,
            "path": ["02" + "c" * 64],
            "success": True,
            "latency_ms": -100,
        }
        assert validate_route_probe_payload(payload) is False

    def test_empty_path_valid(self):
        """Test that empty path (direct connection) is valid."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": "03" + "b" * 64,
            "path": [],
            "success": True,
        }
        assert validate_route_probe_payload(payload) is True

    def test_failed_probe_with_reason(self):
        """Test that failed probe with valid reason passes."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "destination": "03" + "b" * 64,
            "path": ["02" + "c" * 64],
            "success": False,
            "failure_reason": "capacity",
            "failure_hop": 0,
        }
        assert validate_route_probe_payload(payload) is True


class TestRouteProbeSigningPayload:
    """Test route probe signing payload generation."""

    def test_signing_payload_deterministic(self):
        """Test that signing payload is deterministic."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": 1700000000,
            "destination": "03" + "b" * 64,
            "path": ["02" + "c" * 64],
            "success": True,
            "latency_ms": 500,
        }
        sig1 = get_route_probe_signing_payload(payload)
        sig2 = get_route_probe_signing_payload(payload)
        assert sig1 == sig2

    def test_signing_payload_contains_essential_fields(self):
        """Test that signing payload contains essential fields."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": 1700000000,
            "destination": "03" + "b" * 64,
            "path": ["02" + "c" * 64],
            "success": True,
        }
        sig = get_route_probe_signing_payload(payload)
        assert payload["reporter_id"] in sig
        assert payload["destination"] in sig
        assert "1700000000" in sig


class TestHiveRoutingMap:
    """Test HiveRoutingMap class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db = MockDatabase()
        self.mock_plugin = MagicMock()
        self.our_pubkey = "02" + "0" * 64
        self.routing_map = HiveRoutingMap(
            database=self.mock_db,
            plugin=self.mock_plugin,
            our_pubkey=self.our_pubkey
        )

        # Add members
        self.member1 = "02" + "a" * 64
        self.member2 = "02" + "b" * 64
        self.mock_db.members[self.member1] = {
            "peer_id": self.member1,
            "tier": "member"
        }
        self.mock_db.members[self.member2] = {
            "peer_id": self.member2,
            "tier": "member"
        }

    def test_handle_route_probe_valid(self):
        """Test handling a valid route probe."""
        mock_rpc = MagicMock()
        mock_rpc.checkmessage.return_value = {
            "verified": True,
            "pubkey": self.member1
        }

        destination = "03" + "d" * 64
        path = ["02" + "e" * 64]

        payload = {
            "reporter_id": self.member1,
            "timestamp": int(time.time()),
            "signature": "valid_signature_here",  # Must be >= 10 chars
            "destination": destination,
            "path": path,
            "success": True,
            "latency_ms": 300,
            "total_fee_ppm": 100,
            "estimated_capacity_sats": 1000000,
        }

        result = self.routing_map.handle_route_probe(
            self.member1, payload, mock_rpc
        )

        assert result.get("success") is True
        assert result.get("stored") is True
        assert len(self.mock_db.route_probes) == 1

    def test_handle_route_probe_non_member(self):
        """Test rejecting probe from non-member."""
        mock_rpc = MagicMock()
        non_member = "02" + "z" * 64
        mock_rpc.checkmessage.return_value = {"verified": True, "pubkey": non_member}

        payload = {
            "reporter_id": non_member,
            "timestamp": int(time.time()),
            "signature": "valid_signature_here",  # Must be >= 10 chars
            "destination": "03" + "d" * 64,
            "path": ["02" + "e" * 64],
            "success": True,
        }

        result = self.routing_map.handle_route_probe(
            non_member, payload, mock_rpc
        )

        assert result.get("error") == "reporter not a member"
        assert len(self.mock_db.route_probes) == 0

    def test_handle_route_probe_invalid_signature(self):
        """Test rejecting probe with invalid signature."""
        mock_rpc = MagicMock()
        mock_rpc.checkmessage.return_value = {"verified": False}

        payload = {
            "reporter_id": self.member1,
            "timestamp": int(time.time()),
            "signature": "invalid_signature",  # Must be >= 10 chars
            "destination": "03" + "d" * 64,
            "path": ["02" + "e" * 64],
            "success": True,
        }

        result = self.routing_map.handle_route_probe(
            self.member1, payload, mock_rpc
        )

        assert "signature" in result.get("error", "").lower()
        assert len(self.mock_db.route_probes) == 0

    def test_handle_route_probe_rate_limited(self):
        """Test rate limiting of route probes."""
        mock_rpc = MagicMock()
        mock_rpc.checkmessage.return_value = {
            "verified": True,
            "pubkey": self.member1
        }

        destination = "03" + "d" * 64

        # Send probes up to rate limit
        max_probes, period = ROUTE_PROBE_RATE_LIMIT

        for i in range(max_probes):
            payload = {
                "reporter_id": self.member1,
                "timestamp": int(time.time()),
                "signature": f"signature_for_probe_{i:02d}",  # Must be >= 10 chars
                "destination": destination,
                "path": ["02" + "e" * 64],
                "success": True,
            }
            result = self.routing_map.handle_route_probe(
                self.member1, payload, mock_rpc
            )
            assert result.get("success") is True

        # Next probe should be rate limited
        payload = {
            "reporter_id": self.member1,
            "timestamp": int(time.time()),
            "signature": "extra_signature_here",  # Must be >= 10 chars
            "destination": destination,
            "path": ["02" + "e" * 64],
            "success": True,
        }
        result = self.routing_map.handle_route_probe(
            self.member1, payload, mock_rpc
        )

        assert result.get("error") == "rate limited"

    def test_get_path_success_rate(self):
        """Test path success rate calculation."""
        destination = "03" + "d" * 64
        path = ("02" + "e" * 64,)

        # Add mixed results
        self.routing_map._update_path_stats(
            destination=destination,
            path=path,
            success=True,
            latency_ms=100,
            fee_ppm=50,
            capacity_sats=1000000,
            reporter_id=self.member1,
            failure_reason="",
            timestamp=int(time.time())
        )
        self.routing_map._update_path_stats(
            destination=destination,
            path=path,
            success=True,
            latency_ms=150,
            fee_ppm=50,
            capacity_sats=1000000,
            reporter_id=self.member1,
            failure_reason="",
            timestamp=int(time.time())
        )
        self.routing_map._update_path_stats(
            destination=destination,
            path=path,
            success=False,
            latency_ms=0,
            fee_ppm=0,
            capacity_sats=0,
            reporter_id=self.member1,
            failure_reason="capacity",
            timestamp=int(time.time())
        )

        rate = self.routing_map.get_path_success_rate(list(path))
        # 2 successes, 1 failure = 66.67%
        assert 0.65 < rate < 0.68

    def test_get_best_route_to(self):
        """Test getting best route to a destination."""
        destination = "03" + "d" * 64

        # Path 1: 90% success, low fee
        path1 = ("02" + "e" * 64,)
        for _ in range(9):
            self.routing_map._update_path_stats(
                destination=destination,
                path=path1,
                success=True,
                latency_ms=100,
                fee_ppm=50,
                capacity_sats=1000000,
                reporter_id=self.member1,
                failure_reason="",
                timestamp=int(time.time())
            )
        self.routing_map._update_path_stats(
            destination=destination,
            path=path1,
            success=False,
            latency_ms=0,
            fee_ppm=0,
            capacity_sats=0,
            reporter_id=self.member1,
            failure_reason="capacity",
            timestamp=int(time.time())
        )

        # Path 2: 60% success, lower fee
        path2 = ("02" + "f" * 64,)
        for _ in range(6):
            self.routing_map._update_path_stats(
                destination=destination,
                path=path2,
                success=True,
                latency_ms=80,
                fee_ppm=30,
                capacity_sats=500000,
                reporter_id=self.member2,
                failure_reason="",
                timestamp=int(time.time())
            )
        for _ in range(4):
            self.routing_map._update_path_stats(
                destination=destination,
                path=path2,
                success=False,
                latency_ms=0,
                fee_ppm=0,
                capacity_sats=0,
                reporter_id=self.member2,
                failure_reason="temporary",
                timestamp=int(time.time())
            )

        best = self.routing_map.get_best_route_to(destination, 100000)

        assert best is not None
        assert best.success_rate >= 0.6
        # Path 1 should be preferred due to higher success rate
        assert best.path == list(path1)

    def test_get_routes_to(self):
        """Test getting multiple routes to a destination."""
        destination = "03" + "d" * 64

        # Add two paths
        path1 = ("02" + "e" * 64,)
        path2 = ("02" + "f" * 64,)

        for _ in range(5):
            self.routing_map._update_path_stats(
                destination=destination,
                path=path1,
                success=True,
                latency_ms=100,
                fee_ppm=50,
                capacity_sats=1000000,
                reporter_id=self.member1,
                failure_reason="",
                timestamp=int(time.time())
            )

        for _ in range(3):
            self.routing_map._update_path_stats(
                destination=destination,
                path=path2,
                success=True,
                latency_ms=200,
                fee_ppm=100,
                capacity_sats=500000,
                reporter_id=self.member2,
                failure_reason="",
                timestamp=int(time.time())
            )

        routes = self.routing_map.get_routes_to(destination, limit=5)

        assert len(routes) == 2
        # Should be sorted by success rate (both 100%, so order depends on iteration)
        assert routes[0].success_rate == 1.0
        assert routes[1].success_rate == 1.0

    def test_get_routing_stats(self):
        """Test getting overall routing statistics."""
        destination1 = "03" + "d" * 64
        destination2 = "03" + "e" * 64
        path = ("02" + "f" * 64,)

        # Add probes for two destinations
        self.routing_map._update_path_stats(
            destination=destination1,
            path=path,
            success=True,
            latency_ms=100,
            fee_ppm=50,
            capacity_sats=1000000,
            reporter_id=self.member1,
            failure_reason="",
            timestamp=int(time.time())
        )
        self.routing_map._update_path_stats(
            destination=destination2,
            path=path,
            success=True,
            latency_ms=100,
            fee_ppm=50,
            capacity_sats=1000000,
            reporter_id=self.member1,
            failure_reason="",
            timestamp=int(time.time())
        )

        stats = self.routing_map.get_routing_stats()

        assert stats["total_paths"] == 2
        assert stats["total_probes"] == 2
        assert stats["total_successes"] == 2
        assert stats["overall_success_rate"] == 1.0
        assert stats["unique_destinations"] == 2

    def test_aggregate_from_database(self):
        """Test rebuilding stats from database probes."""
        destination = "03" + "d" * 64
        path = ["02" + "e" * 64]

        # Add probes to mock database
        self.mock_db.route_probes = [
            {
                "reporter_id": self.member1,
                "destination": destination,
                "path": path,
                "success": True,
                "latency_ms": 100,
                "total_fee_ppm": 50,
                "estimated_capacity_sats": 1000000,
                "failure_reason": "",
                "timestamp": int(time.time())
            },
            {
                "reporter_id": self.member2,
                "destination": destination,
                "path": path,
                "success": False,
                "latency_ms": 0,
                "total_fee_ppm": 0,
                "estimated_capacity_sats": 0,
                "failure_reason": "capacity",
                "timestamp": int(time.time())
            }
        ]

        # Create fresh routing map and aggregate
        new_map = HiveRoutingMap(
            database=self.mock_db,
            plugin=self.mock_plugin,
            our_pubkey=self.our_pubkey
        )
        new_map.aggregate_from_database()

        # Should have stats for the path
        stats = new_map.get_routing_stats()
        assert stats["total_paths"] == 1
        assert stats["total_probes"] == 2
        assert stats["total_successes"] == 1

    def test_cleanup_stale_data(self):
        """Test cleanup of stale path statistics."""
        destination = "03" + "d" * 64
        path = ("02" + "e" * 64,)

        # Add old probe (older than staleness hours)
        old_timestamp = int(time.time()) - (PROBE_STALENESS_HOURS + 1) * 3600

        self.routing_map._update_path_stats(
            destination=destination,
            path=path,
            success=True,
            latency_ms=100,
            fee_ppm=50,
            capacity_sats=1000000,
            reporter_id=self.member1,
            failure_reason="",
            timestamp=old_timestamp
        )

        assert len(self.routing_map._path_stats) == 1

        cleaned = self.routing_map.cleanup_stale_data()

        assert cleaned == 1
        assert len(self.routing_map._path_stats) == 0

    def test_path_confidence_calculation(self):
        """Test path confidence based on reporters and recency."""
        destination = "03" + "d" * 64
        path = ("02" + "e" * 64,)

        # Add probes from multiple reporters - need 10+ probes for high count_factor
        reporters = [self.member1, self.member2, self.our_pubkey]
        for _ in range(4):  # 4 rounds x 3 reporters = 12 probes
            for reporter in reporters:
                self.routing_map._update_path_stats(
                    destination=destination,
                    path=path,
                    success=True,
                    latency_ms=100,
                    fee_ppm=50,
                    capacity_sats=1000000,
                    reporter_id=reporter,
                    failure_reason="",
                    timestamp=int(time.time())
                )

        confidence = self.routing_map.get_path_confidence(list(path))

        # With 3 reporters, recent probes, and 12 probe count, confidence should be high
        # reporter_factor=1.0, recency_factor=1.0, count_factor=1.0
        assert confidence > 0.9

    def test_unknown_path_neutral_success_rate(self):
        """Test that unknown paths return neutral success rate."""
        unknown_path = ["02" + "z" * 64]
        rate = self.routing_map.get_path_success_rate(unknown_path)

        # Should return 0.5 (neutral)
        assert rate == 0.5

    def test_hive_hop_bonus(self):
        """Test that paths through hive members get bonus."""
        destination = "03" + "d" * 64

        # Path through hive member
        hive_path = (self.member1, self.member2)
        # Path through non-members
        non_hive_path = ("02" + "x" * 64, "02" + "y" * 64)

        # Add same success rate for both
        for _ in range(10):
            self.routing_map._update_path_stats(
                destination=destination,
                path=hive_path,
                success=True,
                latency_ms=100,
                fee_ppm=100,
                capacity_sats=1000000,
                reporter_id=self.member1,
                failure_reason="",
                timestamp=int(time.time())
            )
            self.routing_map._update_path_stats(
                destination=destination,
                path=non_hive_path,
                success=True,
                latency_ms=100,
                fee_ppm=100,
                capacity_sats=1000000,
                reporter_id=self.member1,
                failure_reason="",
                timestamp=int(time.time())
            )

        hive_members = {self.member1, self.member2}
        best = self.routing_map.get_best_route_to(
            destination, 100000, hive_members=hive_members
        )

        # Hive path should be preferred due to hop bonus
        assert best is not None
        assert best.path == list(hive_path)
        assert best.hive_hop_count == 2


class TestCreateRouteProbe:
    """Test route probe message creation."""

    def test_create_route_probe(self):
        """Test creating a signed route probe message."""
        mock_rpc = MagicMock()
        mock_rpc.signmessage.return_value = {"signature": "base64sig", "zbase": "zbasesig"}

        reporter_id = "02" + "a" * 64
        destination = "03" + "b" * 64
        path = ["02" + "c" * 64]

        msg = create_route_probe(
            reporter_id=reporter_id,
            destination=destination,
            path=path,
            success=True,
            latency_ms=500,
            rpc=mock_rpc,
            total_fee_ppm=100,
            estimated_capacity_sats=1000000
        )

        assert msg is not None
        assert isinstance(msg, bytes)
        # Should have called signmessage
        assert mock_rpc.signmessage.called


class TestPathStats:
    """Test PathStats dataclass."""

    def test_path_stats_defaults(self):
        """Test PathStats default values."""
        stats = PathStats(
            path=("02" + "a" * 64,),
            destination="03" + "b" * 64
        )

        assert stats.probe_count == 0
        assert stats.success_count == 0
        assert stats.total_latency_ms == 0
        assert stats.total_fee_ppm == 0
        assert stats.last_success_time == 0
        assert stats.last_failure_time == 0
        assert stats.last_failure_reason == ""
        assert stats.avg_capacity_sats == 0
        assert len(stats.reporters) == 0


class TestRouteSuggestion:
    """Test RouteSuggestion dataclass."""

    def test_route_suggestion_creation(self):
        """Test RouteSuggestion creation with all fields."""
        suggestion = RouteSuggestion(
            destination="03" + "a" * 64,
            path=["02" + "b" * 64, "02" + "c" * 64],
            expected_fee_ppm=150,
            expected_latency_ms=300,
            success_rate=0.95,
            confidence=0.8,
            last_successful_probe=int(time.time()),
            hive_hop_count=1
        )

        assert suggestion.destination == "03" + "a" * 64
        assert len(suggestion.path) == 2
        assert suggestion.expected_fee_ppm == 150
        assert suggestion.expected_latency_ms == 300
        assert suggestion.success_rate == 0.95
        assert suggestion.confidence == 0.8
        assert suggestion.hive_hop_count == 1


class TestRouteProbeBatch:
    """Test ROUTE_PROBE_BATCH message handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db = MockDatabase()
        self.mock_plugin = MagicMock()
        self.our_pubkey = "02" + "0" * 64
        self.routing_map = HiveRoutingMap(
            database=self.mock_db,
            plugin=self.mock_plugin,
            our_pubkey=self.our_pubkey
        )

        # Add member
        self.member1 = "02" + "a" * 64
        self.mock_db.members[self.member1] = {
            "peer_id": self.member1,
            "tier": "member"
        }

        # Destination
        self.destination = "03" + "x" * 64

    def test_batch_payload_validation(self):
        """Test ROUTE_PROBE_BATCH payload validation."""
        now = int(time.time())
        valid_payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "signature": "testsig12345",
            "probes": [
                {
                    "destination": "03" + "b" * 64,
                    "path": ["02" + "c" * 64],
                    "success": True,
                    "latency_ms": 500,
                    "failure_reason": "",
                    "failure_hop": -1,
                    "estimated_capacity_sats": 1000000,
                    "total_fee_ppm": 100,
                    "per_hop_fees": [100],
                    "amount_probed_sats": 100000
                }
            ]
        }

        assert validate_route_probe_batch_payload(valid_payload) is True

    def test_batch_rejects_invalid_probes(self):
        """Test that invalid probe entries are rejected."""
        now = int(time.time())
        invalid_payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "signature": "testsig12345",
            "probes": [
                {
                    "destination": "",  # Empty destination
                    "path": [],
                    "success": True,
                    "latency_ms": 500,
                }
            ]
        }

        assert validate_route_probe_batch_payload(invalid_payload) is False

    def test_batch_rejects_too_many_probes(self):
        """Test that batches with too many probes are rejected."""
        now = int(time.time())
        too_many_probes = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "signature": "testsig12345",
            "probes": [
                {
                    "destination": f"03{'x' * 63}{i:x}",
                    "path": [],
                    "success": True,
                    "latency_ms": 100,
                    "failure_reason": "",
                    "failure_hop": -1,
                }
                for i in range(MAX_PROBES_IN_BATCH + 1)
            ]
        }

        assert validate_route_probe_batch_payload(too_many_probes) is False

    def test_batch_signing_deterministic(self):
        """Test that batch signing payload is deterministic."""
        now = int(time.time())
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "probes": [
                {"destination": "03" + "b" * 64, "path": [], "success": True, "latency_ms": 100},
                {"destination": "03" + "c" * 64, "path": [], "success": False, "latency_ms": 200},
            ]
        }

        # Different order should produce same signing payload (sorted by destination)
        payload_reordered = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "probes": [
                {"destination": "03" + "c" * 64, "path": [], "success": False, "latency_ms": 200},
                {"destination": "03" + "b" * 64, "path": [], "success": True, "latency_ms": 100},
            ]
        }

        sig1 = get_route_probe_batch_signing_payload(payload)
        sig2 = get_route_probe_batch_signing_payload(payload_reordered)

        assert sig1 == sig2

    def test_batch_rate_limiting(self):
        """Test batch rate limiting."""
        sender_id = "02" + "b" * 64
        self.mock_db.members[sender_id] = {"peer_id": sender_id, "tier": "member"}

        # Should allow first few batches
        for i in range(ROUTE_PROBE_BATCH_RATE_LIMIT[0]):
            allowed = self.routing_map._check_rate_limit(
                sender_id,
                self.routing_map._batch_rate,
                ROUTE_PROBE_BATCH_RATE_LIMIT
            )
            self.routing_map._record_message(sender_id, self.routing_map._batch_rate)
            assert allowed is True

        # Should reject the next one
        allowed = self.routing_map._check_rate_limit(
            sender_id,
            self.routing_map._batch_rate,
            ROUTE_PROBE_BATCH_RATE_LIMIT
        )
        assert allowed is False

    def test_handle_batch_valid(self):
        """Test handling a valid route probe batch."""
        mock_rpc = MagicMock()
        mock_rpc.checkmessage.return_value = {
            "verified": True,
            "pubkey": self.member1
        }

        now = int(time.time())
        payload = {
            "reporter_id": self.member1,
            "timestamp": now,
            "signature": "valid_signature_here",
            "probes": [
                {
                    "destination": self.destination,
                    "path": ["02" + "c" * 64],
                    "success": True,
                    "latency_ms": 500,
                    "failure_reason": "",
                    "failure_hop": -1,
                    "estimated_capacity_sats": 1000000,
                    "total_fee_ppm": 100,
                    "per_hop_fees": [100],
                    "amount_probed_sats": 100000
                },
                {
                    "destination": "03" + "y" * 64,
                    "path": [],
                    "success": False,
                    "latency_ms": 200,
                    "failure_reason": "temporary",
                    "failure_hop": -1,
                }
            ]
        }

        result = self.routing_map.handle_route_probe_batch(
            self.member1, payload, mock_rpc
        )

        assert result.get("success") is True
        assert result.get("probes_stored") == 2
        assert len(self.mock_db.route_probes) == 2

    def test_handle_batch_non_member(self):
        """Test rejecting batch from non-member."""
        mock_rpc = MagicMock()
        non_member = "02" + "z" * 64
        mock_rpc.checkmessage.return_value = {"verified": True, "pubkey": non_member}

        now = int(time.time())
        payload = {
            "reporter_id": non_member,
            "timestamp": now,
            "signature": "valid_signature_here",
            "probes": []
        }

        result = self.routing_map.handle_route_probe_batch(
            non_member, payload, mock_rpc
        )

        assert result.get("error") == "reporter not a member"

    def test_create_batch_message(self):
        """Test creating a signed route probe batch message."""
        mock_rpc = MagicMock()
        mock_rpc.signmessage.return_value = {"signature": "base64sig", "zbase": "zbasesig"}

        probes = [
            {
                "destination": "03" + "b" * 64,
                "path": ["02" + "c" * 64],
                "success": True,
                "latency_ms": 500,
                "failure_reason": "",
                "failure_hop": -1,
                "estimated_capacity_sats": 1000000,
                "total_fee_ppm": 100,
                "per_hop_fees": [100],
                "amount_probed_sats": 100000
            }
        ]

        msg = self.routing_map.create_route_probe_batch_message(
            probes=probes,
            rpc=mock_rpc
        )

        assert msg is not None
        assert isinstance(msg, bytes)
        assert mock_rpc.signmessage.called
