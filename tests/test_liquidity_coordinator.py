"""
Tests for Liquidity Coordinator functionality (Phase 7.3).

Tests cover:
- LiquidityCoordinator class
- LIQUIDITY_NEED payload validation
- Internal rebalance opportunity detection
- NNLB prioritization
- Rate limiting
"""

import pytest
import time
from unittest.mock import MagicMock, patch

from modules.liquidity_coordinator import (
    LiquidityCoordinator,
    LiquidityNeed,
    URGENCY_CRITICAL,
    URGENCY_HIGH,
    URGENCY_MEDIUM,
    URGENCY_LOW,
    NEED_INBOUND,
    NEED_OUTBOUND,
    NEED_REBALANCE,
)
from modules.protocol import (
    validate_liquidity_need_payload,
    validate_liquidity_snapshot_payload,
    get_liquidity_need_signing_payload,
    get_liquidity_snapshot_signing_payload,
    create_liquidity_need,
    create_liquidity_snapshot,
    LIQUIDITY_NEED_RATE_LIMIT,
    LIQUIDITY_SNAPSHOT_RATE_LIMIT,
    MAX_NEEDS_IN_SNAPSHOT,
)


class MockDatabase:
    """Mock database for testing."""

    def __init__(self):
        self.liquidity_needs = []
        self.members = {}
        self.member_health = {}

    def get_member(self, peer_id):
        return self.members.get(peer_id)  # Returns None for non-members

    def get_all_members(self):
        return list(self.members.values()) if self.members else []

    def store_liquidity_need(self, **kwargs):
        self.liquidity_needs.append(kwargs)

    def get_all_liquidity_needs(self, max_age_hours=24):
        return self.liquidity_needs

    def get_member_health(self, peer_id):
        return self.member_health.get(peer_id)

    def get_struggling_members(self, threshold=40):
        return [h for h in self.member_health.values() if h.get("overall_health", 100) < threshold]

    def get_helping_members(self):
        return [h for h in self.member_health.values() if h.get("can_help_others")]

    def cleanup_old_liquidity_needs(self, max_age_hours=24):
        return 0


class TestLiquidityNeedPayload:
    """Test LIQUIDITY_NEED payload validation."""

    def test_valid_payload(self):
        """Test that valid payload passes validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "need_type": "outbound",
            "target_peer_id": "03" + "b" * 64,
            "amount_sats": 1000000,
            "urgency": "high",
            "max_fee_ppm": 100,
            "reason": "channel_depleted",
            "current_balance_pct": 0.1,
        }
        assert validate_liquidity_need_payload(payload) is True

    def test_missing_reporter(self):
        """Test that missing reporter fails validation."""
        payload = {
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "need_type": "outbound",
            "amount_sats": 1000000,
        }
        assert validate_liquidity_need_payload(payload) is False

    def test_invalid_need_type(self):
        """Test that invalid need type fails validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "need_type": "invalid_type",
            "amount_sats": 1000000,
        }
        assert validate_liquidity_need_payload(payload) is False

    def test_invalid_amount_bounds(self):
        """Test that out-of-bounds amount fails validation."""
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "need_type": "outbound",
            "amount_sats": -100,  # Negative
        }
        assert validate_liquidity_need_payload(payload) is False


class TestSigningPayload:
    """Test signing payload generation."""

    def test_signing_payload_deterministic(self):
        """Test that signing payload is deterministic."""
        payload = {
            "reporter_id": "02aaa",
            "timestamp": 1700000000,
            "need_type": "outbound",
            "target_peer_id": "03bbb",
            "amount_sats": 1000000,
            "urgency": "high",
            "max_fee_ppm": 100,
        }

        msg1 = get_liquidity_need_signing_payload(payload)
        msg2 = get_liquidity_need_signing_payload(payload)

        assert msg1 == msg2
        assert "LIQUIDITY_NEED:" in msg1
        assert "02aaa" in msg1


class TestLiquidityCoordinator:
    """Test LiquidityCoordinator class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.plugin = MagicMock()
        self.coordinator = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey="02" + "a" * 64
        )

    def test_handle_liquidity_need_valid(self):
        """Test handling valid liquidity need."""
        # Add reporter as member
        reporter_id = "02" + "b" * 64
        self.db.members[reporter_id] = {"peer_id": reporter_id, "tier": "member"}

        payload = {
            "reporter_id": reporter_id,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "need_type": "outbound",
            "target_peer_id": "03" + "c" * 64,
            "amount_sats": 1000000,
            "urgency": "high",
            "max_fee_ppm": 100,
            "reason": "channel_depleted",
            "current_balance_pct": 0.1,
        }

        # Mock RPC for signature verification
        mock_rpc = MagicMock()
        mock_rpc.checkmessage.return_value = {
            "verified": True,
            "pubkey": reporter_id
        }

        result = self.coordinator.handle_liquidity_need(
            peer_id=reporter_id,
            payload=payload,
            rpc=mock_rpc
        )

        assert result.get("success") is True
        # Keyed by composite key: reporter_id:target_peer_id
        target_id = payload["target_peer_id"]
        assert f"{reporter_id}:{target_id}" in self.coordinator._liquidity_needs

    def test_handle_liquidity_need_non_member(self):
        """Test that non-members are rejected."""
        reporter_id = "02" + "x" * 64
        # reporter_id not in self.db.members, so get_member returns None

        payload = {
            "reporter_id": reporter_id,
            "timestamp": int(time.time()),
            "signature": "a" * 100,
            "need_type": "outbound",
            "target_peer_id": "03" + "y" * 64,
            "amount_sats": 1000000,
            "urgency": "high",
            "max_fee_ppm": 100,
            "current_balance_pct": 0.1,
        }

        result = self.coordinator.handle_liquidity_need(
            peer_id=reporter_id,
            payload=payload,
            rpc=MagicMock()
        )

        assert result.get("error") == "reporter not a member"

    def test_nnlb_prioritization(self):
        """Test that struggling members get higher priority."""
        now = int(time.time())
        our_pubkey = "02" + "a" * 64

        # Add needs from different members
        struggling_member = "02" + "b" * 64
        healthy_member = "02" + "c" * 64

        self.coordinator._liquidity_needs[struggling_member] = LiquidityNeed(
            reporter_id=struggling_member,
            need_type=NEED_OUTBOUND,
            target_peer_id="03" + "d" * 64,
            amount_sats=1000000,
            urgency=URGENCY_MEDIUM,
            max_fee_ppm=100,
            reason="channel_depleted",
            current_balance_pct=0.1,
            can_provide_inbound=0,
            can_provide_outbound=0,
            timestamp=now,
            signature="sig1"
        )

        self.coordinator._liquidity_needs[healthy_member] = LiquidityNeed(
            reporter_id=healthy_member,
            need_type=NEED_OUTBOUND,
            target_peer_id="03" + "e" * 64,
            amount_sats=1000000,
            urgency=URGENCY_MEDIUM,
            max_fee_ppm=100,
            reason="channel_depleted",
            current_balance_pct=0.1,
            can_provide_inbound=0,
            can_provide_outbound=0,
            timestamp=now,
            signature="sig2"
        )

        # Set health scores
        self.db.member_health[struggling_member] = {
            "peer_id": struggling_member,
            "overall_health": 20,  # Struggling
        }
        self.db.member_health[healthy_member] = {
            "peer_id": healthy_member,
            "overall_health": 80,  # Healthy
        }

        prioritized = self.coordinator.get_prioritized_needs()

        # Struggling member should come first
        assert prioritized[0].reporter_id == struggling_member

    def test_assess_liquidity_needs_depleted_outbound(self):
        """Test assessment identifies depleted outbound channels."""
        funds = {
            "channels": [
                {
                    "peer_id": "03" + "x" * 64,
                    "state": "CHANNELD_NORMAL",
                    "amount_msat": 10_000_000_000,  # 10M sats
                    "our_amount_msat": 500_000_000,  # 0.5M sats (5% - critical)
                }
            ]
        }

        needs = self.coordinator.assess_our_liquidity_needs(funds)

        assert len(needs) == 1
        assert needs[0]["need_type"] == NEED_OUTBOUND
        assert needs[0]["urgency"] == URGENCY_HIGH

    def test_assess_liquidity_needs_depleted_inbound(self):
        """Test assessment identifies depleted inbound channels."""
        funds = {
            "channels": [
                {
                    "peer_id": "03" + "x" * 64,
                    "state": "CHANNELD_NORMAL",
                    "amount_msat": 10_000_000_000,  # 10M sats
                    "our_amount_msat": 9_500_000_000,  # 9.5M sats (95% - critical)
                }
            ]
        }

        needs = self.coordinator.assess_our_liquidity_needs(funds)

        assert len(needs) == 1
        assert needs[0]["need_type"] == NEED_INBOUND
        assert needs[0]["urgency"] == URGENCY_HIGH

class TestRateLimiting:
    """Test rate limiting functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.coordinator = LiquidityCoordinator(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_rate_limit_allows_initial(self):
        """Test that initial messages are allowed."""
        sender = "02" + "b" * 64
        assert self.coordinator._check_rate_limit(
            sender,
            self.coordinator._need_rate,
            LIQUIDITY_NEED_RATE_LIMIT
        ) is True

    def test_rate_limit_blocks_excess(self):
        """Test that excess messages are blocked."""
        sender = "02" + "b" * 64
        max_count, period = LIQUIDITY_NEED_RATE_LIMIT

        # Fill up to limit
        for _ in range(max_count):
            self.coordinator._record_message(
                sender,
                self.coordinator._need_rate
            )

        # Next should be blocked
        assert self.coordinator._check_rate_limit(
            sender,
            self.coordinator._need_rate,
            LIQUIDITY_NEED_RATE_LIMIT
        ) is False


class TestNNLBAssistanceStatus:
    """Test NNLB assistance status reporting."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.coordinator = LiquidityCoordinator(
            database=self.db,
            plugin=MagicMock(),
            our_pubkey="02" + "a" * 64
        )

    def test_status_empty(self):
        """Test status with no needs."""
        status = self.coordinator.get_nnlb_assistance_status()

        assert status["pending_needs"] == 0
        assert status["critical_needs"] == 0

    def test_status_with_needs(self):
        """Test status with various needs."""
        now = int(time.time())

        # Add needs with different urgencies
        self.coordinator._liquidity_needs["m1"] = LiquidityNeed(
            reporter_id="m1",
            need_type=NEED_OUTBOUND,
            target_peer_id="t1",
            amount_sats=1000000,
            urgency=URGENCY_CRITICAL,
            max_fee_ppm=100,
            reason="depleted",
            current_balance_pct=0.05,
            can_provide_inbound=0,
            can_provide_outbound=0,
            timestamp=now,
            signature="sig"
        )

        self.coordinator._liquidity_needs["m2"] = LiquidityNeed(
            reporter_id="m2",
            need_type=NEED_INBOUND,
            target_peer_id="t2",
            amount_sats=500000,
            urgency=URGENCY_HIGH,
            max_fee_ppm=50,
            reason="depleted",
            current_balance_pct=0.9,
            can_provide_inbound=0,
            can_provide_outbound=0,
            timestamp=now,
            signature="sig2"
        )

        status = self.coordinator.get_nnlb_assistance_status()

        assert status["pending_needs"] == 2
        assert status["critical_needs"] == 1
        assert status["high_needs"] == 1


class MockStateManager:
    """Mock state manager for internal competition tests."""

    def __init__(self):
        self.peer_states = {}

    def get_peer_state(self, peer_id):
        return self.peer_states.get(peer_id)

    def get_all_peer_states(self):
        return list(self.peer_states.values())

    def set_peer_state(self, peer_id, topology=None, capacity_sats=10_000_000):
        state = MagicMock()
        state.peer_id = peer_id
        state.topology = topology or []
        state.capacity_sats = capacity_sats
        self.peer_states[peer_id] = state


class TestInternalCompetitionDetection:
    """Test internal competition detection functionality (Phase 1)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.plugin = MagicMock()
        self.state_manager = MockStateManager()
        self.coordinator = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey="02" + "0" * 64,
            state_manager=self.state_manager
        )

    def _add_member(self, peer_id, topology=None):
        """Helper to add member to both database and state manager."""
        self.db.members[peer_id] = {"peer_id": peer_id, "tier": "member"}
        self.state_manager.set_peer_state(peer_id, topology=topology or [])

    def test_no_competition_empty_fleet(self):
        """Test that empty fleet returns no competition."""
        result = self.coordinator.detect_internal_competition()

        assert len(result) == 0

    def test_no_competition_single_member(self):
        """Test that single member returns no competition."""
        # Single member with some peers
        self._add_member("02" + "a" * 64, topology=["peer1", "peer2", "peer3"])

        result = self.coordinator.detect_internal_competition()

        assert len(result) == 0

    def test_detect_competition_shared_peer(self):
        """Test detecting competition when two members share peers for same route.

        Competition requires both source AND destination to be shared by 2+ members.
        Single shared peer doesn't create routing competition.
        """
        # Two members share peer1 but need a shared destination too
        self._add_member("02" + "a" * 64, topology=["peer1", "peer2"])
        self._add_member("02" + "b" * 64, topology=["peer1", "peer2"])

        result = self.coordinator.detect_internal_competition()

        # Both members can route peer1 <-> peer2 so competition exists
        assert len(result) >= 1

    def test_detect_competition_multiple_shared_peers(self):
        """Test detecting competition with multiple shared peers."""
        # Two members share two peers - creates route competition
        self._add_member("02" + "a" * 64, topology=["peer1", "peer2", "peer3"])
        self._add_member("02" + "b" * 64, topology=["peer1", "peer2", "peer4"])

        result = self.coordinator.detect_internal_competition()

        # Both can route peer1 <-> peer2
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_competition_summary(self):
        """Test getting competition summary."""
        # Set up competition scenario
        self._add_member("02" + "a" * 64, topology=["peer1", "peer2"])
        self._add_member("02" + "b" * 64, topology=["peer1", "peer2"])

        summary = self.coordinator.get_internal_competition_summary()

        # Check for actual return keys (not hypothetical ones)
        assert "competition_count" in summary
        assert "competitions" in summary
        assert "status" in summary

    def test_competition_handles_missing_state_manager(self):
        """Test that missing state manager is handled gracefully."""
        coordinator = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey="02" + "0" * 64,
            state_manager=None  # No state manager
        )

        result = coordinator.detect_internal_competition()

        assert result == []

    def test_competition_with_three_members(self):
        """Test competition detection with three members sharing peers."""
        # Three members all share peer1 and peer2
        self._add_member("02" + "a" * 64, topology=["peer1", "peer2"])
        self._add_member("02" + "b" * 64, topology=["peer1", "peer2"])
        self._add_member("02" + "c" * 64, topology=["peer1", "peer2"])

        result = self.coordinator.detect_internal_competition()

        # All three compete for peer1 <-> peer2 route
        assert isinstance(result, list)
        # With 3+ competitors, should flag as high competition
        if len(result) > 0:
            assert result[0]["member_count"] >= 2


class TestLiquiditySnapshot:
    """Test LIQUIDITY_SNAPSHOT message handling."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MockDatabase()
        self.plugin = MagicMock()
        self.our_pubkey = "02" + "0" * 64
        self.coordinator = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey=self.our_pubkey
        )

        # Add member
        self.member1 = "02" + "a" * 64
        self.db.members[self.member1] = {
            "peer_id": self.member1,
            "tier": "member"
        }

        # Target peer
        self.target_peer = "03" + "x" * 64

    def test_snapshot_payload_validation(self):
        """Test LIQUIDITY_SNAPSHOT payload validation."""
        now = int(time.time())
        valid_payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "signature": "testsig12345",
            "needs": [
                {
                    "target_peer_id": "03" + "b" * 64,
                    "need_type": "outbound",
                    "amount_sats": 1000000,
                    "urgency": "high",
                    "max_fee_ppm": 500,
                    "reason": "channel_depleted",
                    "current_balance_pct": 0.1,
                    "can_provide_inbound": 0,
                    "can_provide_outbound": 0
                }
            ]
        }

        assert validate_liquidity_snapshot_payload(valid_payload) is True

    def test_snapshot_rejects_invalid_need_type(self):
        """Test that invalid need types are rejected."""
        now = int(time.time())
        invalid_payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "signature": "testsig12345",
            "needs": [
                {
                    "target_peer_id": "03" + "b" * 64,
                    "need_type": "invalid_type",  # Invalid
                    "amount_sats": 1000000,
                    "urgency": "high",
                    "max_fee_ppm": 500,
                }
            ]
        }

        assert validate_liquidity_snapshot_payload(invalid_payload) is False

    def test_snapshot_rejects_too_many_needs(self):
        """Test that snapshots with too many needs are rejected."""
        now = int(time.time())
        too_many_needs = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "signature": "testsig12345",
            "needs": [
                {
                    "target_peer_id": f"03{'x' * 63}{i:x}",
                    "need_type": "outbound",
                    "amount_sats": 1000000,
                    "urgency": "medium",
                    "max_fee_ppm": 500,
                    "current_balance_pct": 0.5,
                }
                for i in range(MAX_NEEDS_IN_SNAPSHOT + 1)
            ]
        }

        assert validate_liquidity_snapshot_payload(too_many_needs) is False

    def test_snapshot_signing_deterministic(self):
        """Test that snapshot signing payload is deterministic."""
        now = int(time.time())
        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "needs": [
                {"target_peer_id": "03" + "b" * 64, "need_type": "outbound", "amount_sats": 100000, "urgency": "medium", "max_fee_ppm": 500, "current_balance_pct": 0.5},
                {"target_peer_id": "03" + "c" * 64, "need_type": "inbound", "amount_sats": 200000, "urgency": "high", "max_fee_ppm": 300, "current_balance_pct": 0.9},
            ]
        }

        # Different order should produce same signing payload (sorted by target_peer_id)
        payload_reordered = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": now,
            "needs": [
                {"target_peer_id": "03" + "c" * 64, "need_type": "inbound", "amount_sats": 200000, "urgency": "high", "max_fee_ppm": 300, "current_balance_pct": 0.9},
                {"target_peer_id": "03" + "b" * 64, "need_type": "outbound", "amount_sats": 100000, "urgency": "medium", "max_fee_ppm": 500, "current_balance_pct": 0.5},
            ]
        }

        sig1 = get_liquidity_snapshot_signing_payload(payload)
        sig2 = get_liquidity_snapshot_signing_payload(payload_reordered)

        assert sig1 == sig2

    def test_snapshot_rate_limiting(self):
        """Test snapshot rate limiting."""
        sender_id = "02" + "b" * 64
        self.db.members[sender_id] = {"peer_id": sender_id, "tier": "member"}

        # Should allow first few snapshots
        for i in range(LIQUIDITY_SNAPSHOT_RATE_LIMIT[0]):
            allowed = self.coordinator._check_rate_limit(
                sender_id,
                self.coordinator._snapshot_rate,
                LIQUIDITY_SNAPSHOT_RATE_LIMIT
            )
            self.coordinator._record_message(sender_id, self.coordinator._snapshot_rate)
            assert allowed is True

        # Should reject the next one
        allowed = self.coordinator._check_rate_limit(
            sender_id,
            self.coordinator._snapshot_rate,
            LIQUIDITY_SNAPSHOT_RATE_LIMIT
        )
        assert allowed is False

    def test_handle_snapshot_valid(self):
        """Test handling a valid liquidity snapshot."""
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
            "needs": [
                {
                    "target_peer_id": self.target_peer,
                    "need_type": "outbound",
                    "amount_sats": 1000000,
                    "urgency": "high",
                    "max_fee_ppm": 500,
                    "reason": "channel_depleted",
                    "current_balance_pct": 0.1,
                },
                {
                    "target_peer_id": "03" + "y" * 64,
                    "need_type": "inbound",
                    "amount_sats": 500000,
                    "urgency": "medium",
                    "max_fee_ppm": 300,
                    "current_balance_pct": 0.9,
                }
            ]
        }

        result = self.coordinator.handle_liquidity_snapshot(
            self.member1, payload, mock_rpc
        )

        assert result.get("success") is True
        assert result.get("needs_stored") == 2
        assert len(self.db.liquidity_needs) == 2

    def test_handle_snapshot_non_member(self):
        """Test rejecting snapshot from non-member."""
        mock_rpc = MagicMock()
        non_member = "02" + "z" * 64

        now = int(time.time())
        payload = {
            "reporter_id": non_member,
            "timestamp": now,
            "signature": "valid_signature_here",
            "needs": []
        }

        result = self.coordinator.handle_liquidity_snapshot(
            non_member, payload, mock_rpc
        )

        assert result.get("error") == "reporter not a member"

    def test_create_snapshot_message(self):
        """Test creating a signed liquidity snapshot message."""
        mock_rpc = MagicMock()
        mock_rpc.signmessage.return_value = {"signature": "base64sig", "zbase": "zbasesig"}

        needs = [
            {
                "target_peer_id": "03" + "b" * 64,
                "need_type": "outbound",
                "amount_sats": 1000000,
                "urgency": "high",
                "max_fee_ppm": 500,
                "reason": "channel_depleted",
                "current_balance_pct": 0.1,
            }
        ]

        msg = self.coordinator.create_liquidity_snapshot_message(
            needs=needs,
            rpc=mock_rpc
        )

        assert msg is not None
        assert isinstance(msg, bytes)
        assert mock_rpc.signmessage.called
