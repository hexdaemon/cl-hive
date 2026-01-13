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
    get_fee_intelligence_signing_payload,
    get_health_report_signing_payload,
    validate_fee_intelligence_payload,
    validate_health_report_payload,
    create_fee_intelligence,
    create_health_report,
    FEE_INTELLIGENCE_RATE_LIMIT,
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


class TestFeeIntelligencePayload:
    """Test FEE_INTELLIGENCE payload validation."""

    def test_valid_payload(self):
        """Test that valid payload passes validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "target_peer_id": "03" + "b" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "our_fee_ppm": 100,
            "their_fee_ppm": 50,
            "forward_count": 10,
            "forward_volume_sats": 1000000,
            "revenue_sats": 100,
            "flow_direction": "source",
            "utilization_pct": 0.5,
            "days_observed": 7
        }
        assert validate_fee_intelligence_payload(payload) is True

    def test_missing_reporter(self):
        """Test that missing reporter fails validation."""
        payload = {
            "target_peer_id": "03" + "b" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "our_fee_ppm": 100,
        }
        assert validate_fee_intelligence_payload(payload) is False

    def test_invalid_fee_bounds(self):
        """Test that out-of-bounds fees fail validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "target_peer_id": "03" + "b" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "our_fee_ppm": 100000,  # Too high
            "their_fee_ppm": 50,
        }
        assert validate_fee_intelligence_payload(payload) is False

    def test_invalid_utilization(self):
        """Test that out-of-bounds utilization fails validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "target_peer_id": "03" + "b" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "our_fee_ppm": 100,
            "utilization_pct": 1.5,  # Too high
        }
        assert validate_fee_intelligence_payload(payload) is False


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

    def test_fee_intelligence_signing_deterministic(self):
        """Test that fee intelligence signing payload is deterministic."""
        payload = {
            "reporter_id": "02aaa",
            "target_peer_id": "03bbb",
            "timestamp": 1700000000,
            "our_fee_ppm": 100,
            "their_fee_ppm": 50,
            "forward_count": 10,
            "forward_volume_sats": 1000000,
            "revenue_sats": 100,
            "flow_direction": "source",
            "utilization_pct": 0.5,
        }

        msg1 = get_fee_intelligence_signing_payload(payload)
        msg2 = get_fee_intelligence_signing_payload(payload)

        assert msg1 == msg2
        assert "FEE_INTELLIGENCE:" in msg1
        assert "02aaa" in msg1

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
            our_health=60
        )

        # Struggling node recommendation
        struggling_rec = self.manager.get_fee_recommendation(
            target_peer_id=target,
            our_health=20
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
    """Test rate limiting functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.manager = FeeIntelligenceManager(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_rate_limit_allows_initial(self):
        """Test that initial messages are allowed."""
        sender = "02" + "b" * 64
        assert self.manager._check_rate_limit(
            sender,
            self.manager._fee_intel_rate,
            FEE_INTELLIGENCE_RATE_LIMIT
        ) is True

    def test_rate_limit_blocks_excess(self):
        """Test that excess messages are blocked."""
        sender = "02" + "b" * 64
        max_count, period = FEE_INTELLIGENCE_RATE_LIMIT

        # Fill up to limit
        for _ in range(max_count):
            self.manager._record_message(sender, self.manager._fee_intel_rate)

        # Next should be blocked
        assert self.manager._check_rate_limit(
            sender,
            self.manager._fee_intel_rate,
            FEE_INTELLIGENCE_RATE_LIMIT
        ) is False
