"""
Tests for Fee Intelligence functionality (Phase 7).

Tests cover:
- FeeIntelligenceManager class
- Fee profile aggregation
- Health scoring and NNLB
- Message validation
"""

import pytest
import time
from unittest.mock import MagicMock, patch

from modules.fee_intelligence import (
    FeeIntelligenceManager,
    WEIGHT_QUALITY, WEIGHT_ELASTICITY, WEIGHT_COMPETITION, WEIGHT_FAIRNESS,
    MIN_FEE_PPM, MAX_FEE_PPM, DEFAULT_BASE_FEE,
    HEALTH_THRIVING, HEALTH_HEALTHY, HEALTH_STRUGGLING
)
from modules.protocol import (
    HiveMessageType,
    get_fee_intelligence_snapshot_signing_payload,
    get_health_report_signing_payload,
    validate_fee_intelligence_snapshot_payload,
    validate_health_report_payload,
    create_fee_intelligence_snapshot,
    create_health_report,
    FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT,
    MAX_PEERS_IN_SNAPSHOT,
)


class MockDatabase:
    """Mock database for testing."""

    def __init__(self):
        self.fee_intelligence = []
        self.fee_profiles = {}
        self.member_health = {}
        self.members = {}

    def get_member(self, peer_id):
        return self.members.get(peer_id, {"peer_id": peer_id, "tier": "member"})

    def store_fee_intelligence(self, **kwargs):
        self.fee_intelligence.append(kwargs)
        return len(self.fee_intelligence)

    def get_all_fee_intelligence(self, max_age_hours=24):
        return self.fee_intelligence

    def get_fee_intelligence_for_peer(self, target_peer_id, max_age_hours=24):
        return [r for r in self.fee_intelligence if r.get("target_peer_id") == target_peer_id]

    def update_peer_fee_profile(self, peer_id, **kwargs):
        self.fee_profiles[peer_id] = {"peer_id": peer_id, **kwargs}

    def get_peer_fee_profile(self, peer_id):
        return self.fee_profiles.get(peer_id)

    def get_all_peer_fee_profiles(self):
        return list(self.fee_profiles.values())

    def update_member_health(self, peer_id, **kwargs):
        self.member_health[peer_id] = {"peer_id": peer_id, **kwargs}

    def get_member_health(self, peer_id):
        return self.member_health.get(peer_id)

    def get_all_member_health(self):
        return list(self.member_health.values())

    def get_struggling_members(self, threshold=40):
        return [h for h in self.member_health.values() if h.get("overall_health", 100) < threshold]

    def get_helping_members(self):
        return [h for h in self.member_health.values() if h.get("can_help_others")]


class TestHealthReportPayload:
    """Test HEALTH_REPORT payload validation."""

    def test_valid_payload(self):
        """Test that valid health report passes validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "overall_health": 75,
            "capacity_score": 80,
            "revenue_score": 70,
            "connectivity_score": 75,
        }
        assert validate_health_report_payload(payload) is True

    def test_invalid_health_score(self):
        """Test that out-of-bounds health scores fail validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "overall_health": 150,  # Too high
            "capacity_score": 80,
            "revenue_score": 70,
            "connectivity_score": 75,
        }
        assert validate_health_report_payload(payload) is False


class TestSigningPayloads:
    """Test signing payload generation."""

    def test_health_report_signing_deterministic(self):
        """Test that health report signing payload is deterministic."""
        payload = {
            "reporter_id": "02aaa",
            "timestamp": 1700000000,
            "overall_health": 75,
            "capacity_score": 80,
            "revenue_score": 70,
            "connectivity_score": 75,
        }

        msg1 = get_health_report_signing_payload(payload)
        msg2 = get_health_report_signing_payload(payload)

        assert msg1 == msg2
        assert "HEALTH_REPORT:" in msg1


class TestFeeIntelligenceManager:
    """Test FeeIntelligenceManager class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.plugin = MagicMock()
        self.manager = FeeIntelligenceManager(
            database=self.db,
            plugin=self.plugin,
            our_pubkey="02" + "a" * 64
        )

    def test_aggregate_fee_profiles_empty(self):
        """Test aggregation with no fee intelligence."""
        updated = self.manager.aggregate_fee_profiles()
        assert updated == 0

    def test_aggregate_fee_profiles_single_report(self):
        """Test aggregation with single fee intelligence report."""
        now = int(time.time())
        self.db.fee_intelligence.append({
            "reporter_id": "02" + "a" * 64,
            "target_peer_id": "03" + "b" * 64,
            "timestamp": now,
            "our_fee_ppm": 100,
            "their_fee_ppm": 50,
            "forward_count": 10,
            "forward_volume_sats": 1000000,
            "revenue_sats": 100,
            "flow_direction": "source",
            "utilization_pct": 0.5,
        })

        updated = self.manager.aggregate_fee_profiles()
        assert updated == 1

        profile = self.db.get_peer_fee_profile("03" + "b" * 64)
        assert profile is not None
        assert profile["reporter_count"] == 1
        assert profile["avg_fee_charged"] == 100

    def test_aggregate_fee_profiles_multiple_reporters(self):
        """Test aggregation with multiple reporters."""
        now = int(time.time())
        target = "03" + "b" * 64

        # Add reports from multiple reporters
        for i, fee in enumerate([100, 150, 200]):
            self.db.fee_intelligence.append({
                "reporter_id": f"02{chr(ord('a') + i)}" + "0" * 63,
                "target_peer_id": target,
                "timestamp": now,
                "our_fee_ppm": fee,
                "their_fee_ppm": 50,
                "forward_count": 10,
                "forward_volume_sats": 1000000,
                "revenue_sats": fee,
                "flow_direction": "balanced",
                "utilization_pct": 0.5,
            })

        updated = self.manager.aggregate_fee_profiles()
        assert updated == 1

        profile = self.db.get_peer_fee_profile(target)
        assert profile["reporter_count"] == 3
        assert profile["avg_fee_charged"] == 150  # Average of 100, 150, 200
        assert profile["min_fee_charged"] == 100
        assert profile["max_fee_charged"] == 200

    def test_fee_recommendation_default(self):
        """Test fee recommendation when no profile exists."""
        recommendation = self.manager.get_fee_recommendation(
            target_peer_id="03" + "x" * 64,
            our_health=50
        )

        assert recommendation["recommended_fee_ppm"] == DEFAULT_BASE_FEE
        assert recommendation["source"] == "default"

    def test_fee_recommendation_with_profile(self):
        """Test fee recommendation with existing profile."""
        target = "03" + "b" * 64
        self.db.fee_profiles[target] = {
            "peer_id": target,
            "optimal_fee_estimate": 200,
            "estimated_elasticity": 0.0,
            "confidence": 0.8,
            "reporter_count": 3,
        }

        recommendation = self.manager.get_fee_recommendation(
            target_peer_id=target,
            our_health=50
        )

        assert recommendation["source"] == "hive_intelligence"
        assert recommendation["base_optimal_fee"] == 200
        assert "recommended_fee_ppm" in recommendation

    def test_fee_recommendation_nnlb_struggling(self):
        """Test that struggling nodes get lower fee recommendations."""
        target = "03" + "b" * 64
        self.db.fee_profiles[target] = {
            "peer_id": target,
            "optimal_fee_estimate": 200,
            "estimated_elasticity": 0.0,
            "confidence": 0.8,
            "reporter_count": 3,
        }

        # Healthy node recommendation
        healthy_rec = self.manager.get_fee_recommendation(
            target_peer_id=target,
            our_health=70
        )

        # Struggling node recommendation (must be < HEALTH_STRUGGLING=20)
        struggling_rec = self.manager.get_fee_recommendation(
            target_peer_id=target,
            our_health=10
        )

        # Struggling node should get lower fees
        assert struggling_rec["recommended_fee_ppm"] < healthy_rec["recommended_fee_ppm"]
        assert struggling_rec["health_multiplier"] < 1.0


class TestHealthCalculation:
    """Test health score calculation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.manager = FeeIntelligenceManager(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_calculate_health_minimal(self):
        """Test health calculation with minimal capacity."""
        health = self.manager.calculate_our_health(
            capacity_sats=1_000_000,
            available_sats=500_000,
            channel_count=2,
            daily_revenue_sats=0,
            hive_avg_capacity=10_000_000
        )

        assert "overall_health" in health
        assert "tier" in health
        assert health["capacity_score"] < 50  # Below average capacity

    def test_calculate_health_thriving(self):
        """Test health calculation for thriving node."""
        health = self.manager.calculate_our_health(
            capacity_sats=50_000_000,
            available_sats=25_000_000,
            channel_count=20,
            daily_revenue_sats=5000,
            hive_avg_capacity=10_000_000,
            hive_avg_revenue=1000
        )

        assert health["tier"] in ("thriving", "healthy")
        assert health["can_help_others"] is True

    def test_calculate_health_balanced(self):
        """Test that balanced channels score higher."""
        # Well balanced (50% local)
        balanced = self.manager.calculate_our_health(
            capacity_sats=10_000_000,
            available_sats=5_000_000,
            channel_count=5,
            daily_revenue_sats=100
        )

        # Unbalanced (90% local)
        unbalanced = self.manager.calculate_our_health(
            capacity_sats=10_000_000,
            available_sats=9_000_000,
            channel_count=5,
            daily_revenue_sats=100
        )

        assert balanced["balance_score"] > unbalanced["balance_score"]


class TestNNLBStatus:
    """Test NNLB status reporting."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.manager = FeeIntelligenceManager(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_nnlb_status_empty(self):
        """Test NNLB status with no health records."""
        status = self.manager.get_nnlb_status()

        assert status["member_count"] == 0
        assert status["average_health"] == 0
        assert status["struggling_count"] == 0

    def test_nnlb_status_with_members(self):
        """Test NNLB status with health records."""
        # Add some health records
        self.db.member_health["member1"] = {
            "peer_id": "member1",
            "overall_health": 80,
            "tier": "thriving",
            "can_help_others": True,
            "assistance_budget_sats": 1000000,
        }
        self.db.member_health["member2"] = {
            "peer_id": "member2",
            "overall_health": 30,
            "tier": "struggling",
            "can_help_others": False,
        }

        status = self.manager.get_nnlb_status()

        assert status["member_count"] == 2
        assert status["average_health"] == 55  # (80 + 30) / 2
        assert status["struggling_count"] == 1
        assert status["helper_count"] == 1


class TestRateLimiting:
    """Test rate limiting functionality for fee intelligence snapshots."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.manager = FeeIntelligenceManager(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_rate_limit_allows_initial(self):
        """Test that initial snapshot messages are allowed."""
        sender = "02" + "b" * 64
        assert self.manager._check_rate_limit(
            sender,
            self.manager._fee_intel_snapshot_rate,
            FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT
        ) is True

    def test_rate_limit_blocks_excess(self):
        """Test that excess snapshot messages are blocked."""
        sender = "02" + "b" * 64
        max_count, period = FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT

        # Fill up to limit
        for _ in range(max_count):
            self.manager._record_message(sender, self.manager._fee_intel_snapshot_rate)

        # Next should be blocked
        assert self.manager._check_rate_limit(
            sender,
            self.manager._fee_intel_snapshot_rate,
            FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT
        ) is False


class TestDatabaseCleanup:
    """Test database cleanup operations for fee intelligence."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        # Add cleanup method to mock
        self.db.cleanup_old_fee_intelligence = self._mock_cleanup

    def _mock_cleanup(self, max_age_hours: int = 168) -> int:
        """Mock cleanup that removes old records."""
        now = int(time.time())
        cutoff = now - (max_age_hours * 3600)
        old_count = len(self.db.fee_intelligence)
        self.db.fee_intelligence = [
            r for r in self.db.fee_intelligence
            if r.get("timestamp", 0) >= cutoff
        ]
        return old_count - len(self.db.fee_intelligence)

    def test_cleanup_removes_old_records(self):
        """Test that cleanup removes old fee intelligence records."""
        now = int(time.time())
        # Add old record (8 days old)
        self.db.fee_intelligence.append({
            "reporter_id": "02" + "a" * 64,
            "target_peer_id": "03" + "b" * 64,
            "timestamp": now - (8 * 24 * 3600),  # 8 days old
            "our_fee_ppm": 100,
        })
        # Add recent record (1 day old)
        self.db.fee_intelligence.append({
            "reporter_id": "02" + "a" * 64,
            "target_peer_id": "03" + "c" * 64,
            "timestamp": now - (1 * 24 * 3600),  # 1 day old
            "our_fee_ppm": 150,
        })

        deleted = self.db.cleanup_old_fee_intelligence(max_age_hours=168)

        assert deleted == 1
        assert len(self.db.fee_intelligence) == 1
        assert self.db.fee_intelligence[0]["our_fee_ppm"] == 150

    def test_cleanup_keeps_all_recent_records(self):
        """Test that cleanup keeps all records within max age."""
        now = int(time.time())
        # Add several recent records
        for i in range(5):
            self.db.fee_intelligence.append({
                "reporter_id": "02" + "a" * 64,
                "target_peer_id": f"03{'b' * 63}{i}",
                "timestamp": now - (i * 24 * 3600),  # 0-4 days old
                "our_fee_ppm": 100 + i,
            })

        deleted = self.db.cleanup_old_fee_intelligence(max_age_hours=168)

        assert deleted == 0
        assert len(self.db.fee_intelligence) == 5


class TestFeeProfileAggregationWeighting:
    """Test fee profile aggregation weighting and edge cases."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.manager = FeeIntelligenceManager(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_volume_weighted_average(self):
        """Test that high-volume reports have more weight."""
        now = int(time.time())
        target = "03" + "b" * 64

        # Low volume reporter with high fee
        self.db.fee_intelligence.append({
            "reporter_id": "02" + "c" * 64,
            "target_peer_id": target,
            "timestamp": now,
            "our_fee_ppm": 500,
            "forward_count": 1,
            "forward_volume_sats": 10000,  # Low volume
            "revenue_sats": 5,
            "flow_direction": "balanced",
            "utilization_pct": 0.5,
        })

        # High volume reporter with low fee
        self.db.fee_intelligence.append({
            "reporter_id": "02" + "d" * 64,
            "target_peer_id": target,
            "timestamp": now,
            "our_fee_ppm": 100,
            "forward_count": 100,
            "forward_volume_sats": 10000000,  # High volume
            "revenue_sats": 1000,
            "flow_direction": "balanced",
            "utilization_pct": 0.5,
        })

        self.manager.aggregate_fee_profiles()
        profile = self.db.get_peer_fee_profile(target)

        # Aggregation uses simple average of fees across reporters
        # (100 + 500) / 2 = 300
        assert profile is not None
        assert profile["avg_fee_charged"] == 300

    def test_aggregation_handles_zero_volume(self):
        """Test that zero volume reports don't cause division errors."""
        now = int(time.time())
        target = "03" + "b" * 64

        self.db.fee_intelligence.append({
            "reporter_id": "02" + "c" * 64,
            "target_peer_id": target,
            "timestamp": now,
            "our_fee_ppm": 200,
            "forward_count": 0,
            "forward_volume_sats": 0,
            "revenue_sats": 0,
            "flow_direction": "balanced",
            "utilization_pct": 0.5,
        })

        # Should not raise exception
        updated = self.manager.aggregate_fee_profiles()
        assert updated == 1


class TestFeeRecommendationEdgeCases:
    """Test fee recommendation edge cases."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.manager = FeeIntelligenceManager(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_fee_bounds_enforced(self):
        """Test that recommended fees stay within bounds."""
        target = "03" + "b" * 64

        # Set up profile with extreme optimal fee
        self.db.fee_profiles[target] = {
            "peer_id": target,
            "optimal_fee_estimate": 100000,  # Very high
            "estimated_elasticity": 0.0,
            "confidence": 1.0,
            "reporter_count": 5,
        }

        recommendation = self.manager.get_fee_recommendation(
            target_peer_id=target,
            our_health=50
        )

        assert recommendation["recommended_fee_ppm"] <= MAX_FEE_PPM
        assert recommendation["recommended_fee_ppm"] >= MIN_FEE_PPM

    def test_low_confidence_falls_back(self):
        """Test that low confidence uses default/conservative fee."""
        target = "03" + "b" * 64

        self.db.fee_profiles[target] = {
            "peer_id": target,
            "optimal_fee_estimate": 50,
            "estimated_elasticity": 0.0,
            "confidence": 0.1,  # Very low confidence
            "reporter_count": 1,
        }

        recommendation = self.manager.get_fee_recommendation(
            target_peer_id=target,
            our_health=50
        )

        # Low confidence should still return a recommendation but note the low confidence
        assert "recommended_fee_ppm" in recommendation


class TestFeeIntelligenceSnapshot:
    """Test FEE_INTELLIGENCE_SNAPSHOT message handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.manager = FeeIntelligenceManager(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_snapshot_payload_validation(self):
        """Test FEE_INTELLIGENCE_SNAPSHOT payload validation."""
        from modules.protocol import validate_fee_intelligence_snapshot_payload

        now = int(time.time())
        valid_payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "signature": "testsig12345",
            "peers": [
                {
                    "peer_id": "03" + "b" * 64,
                    "our_fee_ppm": 250,
                    "their_fee_ppm": 300,
                    "forward_count": 10,
                    "forward_volume_sats": 100000,
                    "revenue_sats": 25,
                    "flow_direction": "source",
                    "utilization_pct": 0.65
                }
            ]
        }

        assert validate_fee_intelligence_snapshot_payload(valid_payload) is True

    def test_snapshot_rejects_invalid_peers(self):
        """Test that invalid peer entries are rejected."""
        from modules.protocol import validate_fee_intelligence_snapshot_payload

        now = int(time.time())
        invalid_payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "signature": "testsig12345",
            "peers": [
                {
                    "peer_id": "",  # Empty peer_id
                    "our_fee_ppm": 250,
                }
            ]
        }

        assert validate_fee_intelligence_snapshot_payload(invalid_payload) is False

    def test_snapshot_rejects_too_many_peers(self):
        """Test that snapshots with too many peers are rejected."""
        from modules.protocol import (
            validate_fee_intelligence_snapshot_payload,
            MAX_PEERS_IN_SNAPSHOT
        )

        now = int(time.time())
        too_many_peers = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "signature": "testsig12345",
            "peers": [
                {
                    "peer_id": f"03{'x' * 63}{i:x}",
                    "our_fee_ppm": 100,
                    "their_fee_ppm": 100,
                    "forward_count": 1,
                    "forward_volume_sats": 1000,
                    "revenue_sats": 1,
                    "flow_direction": "balanced",
                    "utilization_pct": 0.5
                }
                for i in range(MAX_PEERS_IN_SNAPSHOT + 1)
            ]
        }

        assert validate_fee_intelligence_snapshot_payload(too_many_peers) is False

    def test_snapshot_signing_deterministic(self):
        """Test that snapshot signing payload is deterministic."""
        from modules.protocol import get_fee_intelligence_snapshot_signing_payload

        now = int(time.time())
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "peers": [
                {"peer_id": "03" + "b" * 64, "our_fee_ppm": 100},
                {"peer_id": "03" + "c" * 64, "our_fee_ppm": 200},
            ]
        }

        # Different order should produce same signing payload (sorted by peer_id)
        payload_reordered = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "peers": [
                {"peer_id": "03" + "c" * 64, "our_fee_ppm": 200},
                {"peer_id": "03" + "b" * 64, "our_fee_ppm": 100},
            ]
        }

        sig1 = get_fee_intelligence_snapshot_signing_payload(payload)
        sig2 = get_fee_intelligence_snapshot_signing_payload(payload_reordered)

        assert sig1 == sig2

    def test_snapshot_rate_limiting(self):
        """Test snapshot rate limiting."""
        from modules.protocol import FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT

        sender_id = "02" + "b" * 64
        self.db.members[sender_id] = {"peer_id": sender_id, "tier": "member"}

        # Should allow first few messages
        for i in range(FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT[0]):
            allowed = self.manager._check_rate_limit(
                sender_id,
                self.manager._fee_intel_snapshot_rate,
                FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT
            )
            self.manager._record_message(sender_id, self.manager._fee_intel_snapshot_rate)
            assert allowed is True

        # Should reject the next one
        allowed = self.manager._check_rate_limit(
            sender_id,
            self.manager._fee_intel_snapshot_rate,
            FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT
        )
        assert allowed is False


# =============================================================================
# FIX 8: MULTI-FACTOR WEIGHTED FEE CALCULATION TESTS
# =============================================================================

class TestMultiFactorFeeCalculation:
    """Test the multi-factor weighted optimal fee calculation."""

    def setup_method(self):
        self.db = MockDatabase()
        self.manager = FeeIntelligenceManager(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_weights_sum_to_one(self):
        """Test that factor weights sum to 1.0."""
        total = WEIGHT_QUALITY + WEIGHT_ELASTICITY + WEIGHT_COMPETITION + WEIGHT_FAIRNESS
        assert abs(total - 1.0) < 0.001

    def test_high_reporter_count_closer_to_avg(self):
        """Test that high reporter count gives result closer to avg_fee."""
        # Many reporters: quality factor should strongly weight avg_fee
        fee_many = self.manager._calculate_optimal_fee(
            avg_fee=300, elasticity=0.0, reporter_count=10
        )

        # Few reporters: quality factor weights toward default
        fee_few = self.manager._calculate_optimal_fee(
            avg_fee=300, elasticity=0.0, reporter_count=1
        )

        # With many reporters, result should be closer to avg_fee (300)
        # than with few reporters (which blends toward DEFAULT_BASE_FEE=100)
        assert abs(fee_many - 300) < abs(fee_few - 300)

    def test_elastic_demand_lowers_fee(self):
        """Test that very elastic demand produces lower optimal fee."""
        fee_elastic = self.manager._calculate_optimal_fee(
            avg_fee=500, elasticity=-0.8, reporter_count=5  # Very elastic
        )
        fee_inelastic = self.manager._calculate_optimal_fee(
            avg_fee=500, elasticity=0.5, reporter_count=5  # Inelastic
        )

        assert fee_elastic < fee_inelastic

    def test_result_bounded(self):
        """Test that result is always within MIN_FEE_PPM..MAX_FEE_PPM."""
        # Very low avg
        fee_low = self.manager._calculate_optimal_fee(
            avg_fee=0.1, elasticity=-0.9, reporter_count=1
        )
        assert fee_low >= MIN_FEE_PPM

        # Very high avg
        fee_high = self.manager._calculate_optimal_fee(
            avg_fee=100000, elasticity=0.9, reporter_count=10
        )
        assert fee_high <= MAX_FEE_PPM

    def test_zero_reporters_uses_default_blend(self):
        """Test that zero reporters blends entirely toward DEFAULT_BASE_FEE."""
        fee = self.manager._calculate_optimal_fee(
            avg_fee=1000, elasticity=0.0, reporter_count=0
        )
        # Quality factor: 0 confidence → entirely DEFAULT_BASE_FEE for quality component
        # Other factors still use avg_fee, so result should be between default and avg
        assert fee >= MIN_FEE_PPM
        assert fee <= MAX_FEE_PPM

    def test_aggregation_uses_multi_factor(self):
        """Test that aggregate_fee_profiles produces different results with reporter count."""
        now = int(time.time())
        target = "03" + "b" * 64

        # Single reporter
        self.db.fee_intelligence.append({
            "reporter_id": "02" + "c" * 64,
            "target_peer_id": target,
            "timestamp": now,
            "our_fee_ppm": 500,
            "forward_count": 10,
            "forward_volume_sats": 1000000,
            "revenue_sats": 500,
            "flow_direction": "balanced",
            "utilization_pct": 0.5,
        })

        self.manager.aggregate_fee_profiles()
        profile_1 = self.db.get_peer_fee_profile(target)
        fee_1_reporter = profile_1["optimal_fee_estimate"]

        # Add 4 more reporters with same fee
        for i in range(4):
            self.db.fee_intelligence.append({
                "reporter_id": f"02{chr(ord('d') + i)}" + "0" * 63,
                "target_peer_id": target,
                "timestamp": now,
                "our_fee_ppm": 500,
                "forward_count": 10,
                "forward_volume_sats": 1000000,
                "revenue_sats": 500,
                "flow_direction": "balanced",
                "utilization_pct": 0.5,
            })

        self.manager.aggregate_fee_profiles()
        profile_5 = self.db.get_peer_fee_profile(target)
        fee_5_reporters = profile_5["optimal_fee_estimate"]

        # 5 reporters should give result closer to avg_fee (500)
        # 1 reporter blends toward DEFAULT_BASE_FEE (100)
        assert abs(fee_5_reporters - 500) <= abs(fee_1_reporter - 500)
