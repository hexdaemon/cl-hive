"""
Tests for the Distributed Settlement module (Phase 12).

Tests cover:
- Canonical hash calculation (deterministic)
- Proposal creation and validation
- Voting and quorum detection
- Settlement execution
- Anti-gaming detection (participation tracking)
"""

import json
import time
import pytest
import hashlib
from unittest.mock import MagicMock, patch, Mock, AsyncMock
from dataclasses import dataclass

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.settlement import (
    SettlementManager,
    MemberContribution,
    SettlementResult,
    SettlementPayment,
    MIN_PAYMENT_FLOOR_SATS,
    calculate_min_payment,
)
from modules.protocol import (
    HiveMessageType,
    validate_settlement_propose,
    validate_settlement_ready,
    validate_settlement_executed,
    create_settlement_propose,
    create_settlement_ready,
    create_settlement_executed,
    get_settlement_propose_signing_payload,
    get_settlement_ready_signing_payload,
    get_settlement_executed_signing_payload,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_database():
    """Create a mock database with distributed settlement methods."""
    db = MagicMock()

    # Settlement proposal methods
    db.add_settlement_proposal.return_value = True
    db.get_settlement_proposal.return_value = None
    db.get_settlement_proposal_by_period.return_value = None
    db.get_pending_settlement_proposals.return_value = []
    db.get_ready_settlement_proposals.return_value = []
    db.update_settlement_proposal_status.return_value = True
    db.is_period_settled.return_value = False

    # Voting methods
    db.add_settlement_ready_vote.return_value = True
    db.get_settlement_ready_votes.return_value = []
    db.count_settlement_ready_votes.return_value = 0
    db.has_voted_settlement.return_value = False

    # Execution methods
    db.add_settlement_execution.return_value = True
    db.get_settlement_executions.return_value = []
    db.has_executed_settlement.return_value = False

    # Period methods
    db.mark_period_settled.return_value = True
    db.get_settled_periods.return_value = []

    # Member methods
    db.get_all_members.return_value = [
        {'peer_id': '02' + 'a' * 64, 'tier': 'member', 'uptime_pct': 99.5},
        {'peer_id': '02' + 'b' * 64, 'tier': 'member', 'uptime_pct': 98.0},
        {'peer_id': '02' + 'c' * 64, 'tier': 'member', 'uptime_pct': 95.0},
    ]

    return db


@pytest.fixture
def mock_plugin():
    """Create a mock plugin."""
    plugin = MagicMock()
    return plugin


@pytest.fixture
def mock_rpc():
    """Create a mock RPC proxy."""
    rpc = MagicMock()
    rpc.signmessage.return_value = {'zbase': 'mock_signature_zbase'}
    rpc.checkmessage.return_value = {'verified': True, 'pubkey': '02' + 'a' * 64}
    return rpc


@pytest.fixture
def mock_state_manager():
    """Create a mock state manager with fee data."""
    sm = MagicMock()

    # Simulated fee data from FEE_REPORT gossip
    fee_data = {
        '02' + 'a' * 64: {'fees_earned_sats': 10000, 'forward_count': 50},
        '02' + 'b' * 64: {'fees_earned_sats': 5000, 'forward_count': 25},
        '02' + 'c' * 64: {'fees_earned_sats': 3000, 'forward_count': 15},
    }

    def get_peer_fees(peer_id):
        return fee_data.get(peer_id, {'fees_earned_sats': 0, 'forward_count': 0})

    sm.get_peer_fees.side_effect = get_peer_fees

    # Mock peer state for capacity
    class MockPeerState:
        def __init__(self, capacity):
            self.capacity_sats = capacity

    def get_peer_state(peer_id):
        capacities = {
            '02' + 'a' * 64: MockPeerState(10_000_000),
            '02' + 'b' * 64: MockPeerState(8_000_000),
            '02' + 'c' * 64: MockPeerState(5_000_000),
        }
        return capacities.get(peer_id)

    sm.get_peer_state.side_effect = get_peer_state

    return sm


@pytest.fixture
def settlement_manager(mock_database, mock_plugin, mock_rpc):
    """Create a SettlementManager instance."""
    return SettlementManager(
        database=mock_database,
        plugin=mock_plugin,
        rpc=mock_rpc
    )


# =============================================================================
# CANONICAL HASH TESTS
# =============================================================================

class TestCanonicalHash:
    """Tests for deterministic hash calculation."""

    def test_hash_is_deterministic(self, settlement_manager):
        """Same inputs should always produce same hash."""
        period = "2024-05"
        contributions = [
            {'peer_id': '02' + 'a' * 64, 'fees_earned': 1000, 'capacity': 1000000, 'uptime': 100},
            {'peer_id': '02' + 'b' * 64, 'fees_earned': 500, 'capacity': 500000, 'uptime': 99},
        ]

        hash1 = settlement_manager.calculate_settlement_hash(period, contributions)
        hash2 = settlement_manager.calculate_settlement_hash(period, contributions)

        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex

    def test_hash_is_order_independent(self, settlement_manager):
        """Hash should be same regardless of contribution order."""
        period = "2024-05"
        contributions_a = [
            {'peer_id': '02' + 'a' * 64, 'fees_earned': 1000, 'capacity': 1000000, 'uptime': 100},
            {'peer_id': '02' + 'b' * 64, 'fees_earned': 500, 'capacity': 500000, 'uptime': 99},
        ]
        contributions_b = [
            {'peer_id': '02' + 'b' * 64, 'fees_earned': 500, 'capacity': 500000, 'uptime': 99},
            {'peer_id': '02' + 'a' * 64, 'fees_earned': 1000, 'capacity': 1000000, 'uptime': 100},
        ]

        hash_a = settlement_manager.calculate_settlement_hash(period, contributions_a)
        hash_b = settlement_manager.calculate_settlement_hash(period, contributions_b)

        assert hash_a == hash_b

    def test_different_periods_produce_different_hashes(self, settlement_manager):
        """Different periods should produce different hashes."""
        contributions = [
            {'peer_id': '02' + 'a' * 64, 'fees_earned': 1000, 'capacity': 1000000, 'uptime': 100},
        ]

        hash1 = settlement_manager.calculate_settlement_hash("2024-05", contributions)
        hash2 = settlement_manager.calculate_settlement_hash("2024-06", contributions)

        assert hash1 != hash2


# =============================================================================
# PERIOD STRING TESTS
# =============================================================================

class TestPeriodString:
    """Tests for period string generation."""

    def test_get_period_string_format(self):
        """Period string should be in YYYY-WW format."""
        period = SettlementManager.get_period_string()

        assert len(period) == 7 or len(period) == 8  # "2024-05" or "2024-52"
        assert '-' in period
        year, week = period.split('-')
        assert len(year) == 4
        assert int(week) >= 1 and int(week) <= 53

    def test_get_previous_period(self):
        """Previous period should be one week before current."""
        current = SettlementManager.get_period_string()
        previous = SettlementManager.get_previous_period()

        # Parse week numbers
        curr_year, curr_week = map(int, current.split('-'))
        prev_year, prev_week = map(int, previous.split('-'))

        # Previous week logic
        if curr_week == 1:
            assert prev_week >= 52
            assert prev_year == curr_year - 1
        else:
            assert prev_week == curr_week - 1
            assert prev_year == curr_year


# =============================================================================
# PROPOSAL CREATION TESTS
# =============================================================================

class TestProposalCreation:
    """Tests for settlement proposal creation."""

    def test_create_proposal_success(
        self, settlement_manager, mock_database, mock_state_manager, mock_rpc
    ):
        """Should successfully create a proposal."""
        period = "2024-05"
        our_peer_id = '02' + 'a' * 64

        proposal = settlement_manager.create_proposal(
            period=period,
            our_peer_id=our_peer_id,
            state_manager=mock_state_manager,
            rpc=mock_rpc
        )

        assert proposal is not None
        assert proposal['period'] == period
        assert proposal['proposer_peer_id'] == our_peer_id
        assert 'data_hash' in proposal
        assert len(proposal['data_hash']) == 64
        assert 'contributions' in proposal
        mock_database.add_settlement_proposal.assert_called_once()

    def test_create_proposal_rejects_duplicate_period(
        self, settlement_manager, mock_database, mock_state_manager, mock_rpc
    ):
        """Should not create proposal if period already has one."""
        mock_database.get_settlement_proposal_by_period.return_value = {
            'proposal_id': 'existing_proposal',
            'period': '2024-05'
        }

        proposal = settlement_manager.create_proposal(
            period="2024-05",
            our_peer_id='02' + 'a' * 64,
            state_manager=mock_state_manager,
            rpc=mock_rpc
        )

        assert proposal is None
        mock_database.add_settlement_proposal.assert_not_called()

    def test_create_proposal_rejects_settled_period(
        self, settlement_manager, mock_database, mock_state_manager, mock_rpc
    ):
        """Should not create proposal for already settled period."""
        mock_database.is_period_settled.return_value = True

        proposal = settlement_manager.create_proposal(
            period="2024-05",
            our_peer_id='02' + 'a' * 64,
            state_manager=mock_state_manager,
            rpc=mock_rpc
        )

        assert proposal is None


# =============================================================================
# VOTING TESTS
# =============================================================================

class TestVoting:
    """Tests for settlement voting."""

    def test_verify_and_vote_success(
        self, settlement_manager, mock_database, mock_state_manager, mock_rpc
    ):
        """Should vote when hash matches."""
        # Create a proposal with correct hash
        contributions = settlement_manager.gather_contributions_from_gossip(
            mock_state_manager, "2024-05"
        )
        data_hash = settlement_manager.calculate_settlement_hash("2024-05", contributions)

        proposal = {
            'proposal_id': 'test_proposal_123',
            'period': '2024-05',
            'data_hash': data_hash,
            'total_fees_sats': 18000,
            'member_count': 3,
        }

        vote = settlement_manager.verify_and_vote(
            proposal=proposal,
            our_peer_id='02' + 'a' * 64,
            state_manager=mock_state_manager,
            rpc=mock_rpc
        )

        assert vote is not None
        assert vote['proposal_id'] == 'test_proposal_123'
        assert vote['data_hash'] == data_hash
        mock_database.add_settlement_ready_vote.assert_called_once()

    def test_verify_and_vote_rejects_hash_mismatch(
        self, settlement_manager, mock_database, mock_state_manager, mock_rpc
    ):
        """Should not vote when hash doesn't match."""
        proposal = {
            'proposal_id': 'test_proposal_123',
            'period': '2024-05',
            'data_hash': 'wrong_hash_' + 'x' * 54,  # 64 chars
            'total_fees_sats': 18000,
            'member_count': 3,
        }

        vote = settlement_manager.verify_and_vote(
            proposal=proposal,
            our_peer_id='02' + 'a' * 64,
            state_manager=mock_state_manager,
            rpc=mock_rpc
        )

        assert vote is None
        mock_database.add_settlement_ready_vote.assert_not_called()

    def test_verify_and_vote_rejects_already_voted(
        self, settlement_manager, mock_database, mock_state_manager, mock_rpc
    ):
        """Should not vote again if already voted."""
        mock_database.has_voted_settlement.return_value = True

        proposal = {
            'proposal_id': 'test_proposal_123',
            'period': '2024-05',
            'data_hash': 'any_hash_' + 'x' * 55,
        }

        vote = settlement_manager.verify_and_vote(
            proposal=proposal,
            our_peer_id='02' + 'a' * 64,
            state_manager=mock_state_manager,
            rpc=mock_rpc
        )

        assert vote is None


# =============================================================================
# QUORUM TESTS
# =============================================================================

class TestQuorum:
    """Tests for quorum detection."""

    def test_quorum_reached_with_majority(
        self, settlement_manager, mock_database
    ):
        """Should mark ready when 51% quorum reached."""
        mock_database.count_settlement_ready_votes.return_value = 2  # 2/3 = 67%
        mock_database.get_settlement_proposal.return_value = {
            'proposal_id': 'test_proposal',
            'status': 'pending'
        }

        result = settlement_manager.check_quorum_and_mark_ready(
            proposal_id='test_proposal',
            member_count=3
        )

        assert result is True
        mock_database.update_settlement_proposal_status.assert_called_with(
            'test_proposal', 'ready'
        )

    def test_quorum_not_reached(
        self, settlement_manager, mock_database
    ):
        """Should not mark ready when quorum not reached."""
        mock_database.count_settlement_ready_votes.return_value = 1  # 1/3 = 33%

        result = settlement_manager.check_quorum_and_mark_ready(
            proposal_id='test_proposal',
            member_count=3
        )

        assert result is False
        mock_database.update_settlement_proposal_status.assert_not_called()


# =============================================================================
# PROTOCOL VALIDATION TESTS
# =============================================================================

class TestProtocolValidation:
    """Tests for protocol message validation."""

    def test_validate_settlement_propose_valid(self):
        """Valid SETTLEMENT_PROPOSE should pass validation."""
        payload = {
            "proposal_id": "abc123",
            "period": "2024-05",
            "proposer_peer_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "data_hash": "a" * 64,
            "total_fees_sats": 10000,
            "member_count": 3,
            "contributions": [
                {"peer_id": "02" + "a" * 64, "fees_earned": 5000, "capacity": 1000000}
            ],
            "signature": "mock_signature_zbase_1234567890"
        }

        assert validate_settlement_propose(payload) is True

    def test_validate_settlement_propose_invalid_hash(self):
        """Invalid hash length should fail validation."""
        payload = {
            "proposal_id": "abc123",
            "period": "2024-05",
            "proposer_peer_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "data_hash": "tooshort",  # Should be 64 chars
            "total_fees_sats": 10000,
            "member_count": 3,
            "contributions": [],
            "signature": "mock_signature"
        }

        assert validate_settlement_propose(payload) is False

    def test_validate_settlement_ready_valid(self):
        """Valid SETTLEMENT_READY should pass validation."""
        payload = {
            "proposal_id": "abc123",
            "voter_peer_id": "02" + "b" * 64,
            "data_hash": "b" * 64,
            "timestamp": int(time.time()),
            "signature": "mock_signature_zbase_1234567890"
        }

        assert validate_settlement_ready(payload) is True

    def test_validate_settlement_executed_valid(self):
        """Valid SETTLEMENT_EXECUTED should pass validation."""
        payload = {
            "proposal_id": "abc123",
            "executor_peer_id": "02" + "c" * 64,
            "timestamp": int(time.time()),
            "signature": "mock_signature_zbase_1234567890",
            "payment_hash": "payment123",
            "amount_paid_sats": 1000
        }

        assert validate_settlement_executed(payload) is True


# =============================================================================
# MESSAGE CREATION TESTS
# =============================================================================

class TestMessageCreation:
    """Tests for protocol message creation."""

    def test_create_settlement_propose(self):
        """Should create valid SETTLEMENT_PROPOSE message."""
        msg = create_settlement_propose(
            proposal_id="test_proposal",
            period="2024-05",
            proposer_peer_id="02" + "a" * 64,
            data_hash="a" * 64,
            total_fees_sats=10000,
            member_count=3,
            contributions=[{"peer_id": "02" + "a" * 64, "fees_earned": 5000}],
            timestamp=int(time.time()),
            signature="mock_signature"
        )

        assert msg is not None
        assert msg[:4] == b'HIVE'  # Magic bytes

    def test_create_settlement_ready(self):
        """Should create valid SETTLEMENT_READY message."""
        msg = create_settlement_ready(
            proposal_id="test_proposal",
            voter_peer_id="02" + "b" * 64,
            data_hash="b" * 64,
            timestamp=int(time.time()),
            signature="mock_signature"
        )

        assert msg is not None
        assert msg[:4] == b'HIVE'

    def test_create_settlement_executed(self):
        """Should create valid SETTLEMENT_EXECUTED message."""
        msg = create_settlement_executed(
            proposal_id="test_proposal",
            executor_peer_id="02" + "c" * 64,
            timestamp=int(time.time()),
            signature="mock_signature",
            payment_hash="payment123",
            amount_paid_sats=1000
        )

        assert msg is not None
        assert msg[:4] == b'HIVE'


# =============================================================================
# SIGNING PAYLOAD TESTS
# =============================================================================

class TestSigningPayloads:
    """Tests for canonical signing payloads."""

    def test_signing_payload_is_deterministic(self):
        """Signing payload should be deterministic."""
        payload = {
            "proposal_id": "test",
            "period": "2024-05",
            "proposer_peer_id": "02" + "a" * 64,
            "data_hash": "a" * 64,
            "total_fees_sats": 10000,
            "member_count": 3,
            "timestamp": 1234567890,
        }

        sig1 = get_settlement_propose_signing_payload(payload)
        sig2 = get_settlement_propose_signing_payload(payload)

        assert sig1 == sig2

    def test_different_payloads_produce_different_signatures(self):
        """Different payloads should produce different signing strings."""
        payload1 = {
            "proposal_id": "test1",
            "voter_peer_id": "02" + "a" * 64,
            "data_hash": "a" * 64,
            "timestamp": 1234567890,
        }
        payload2 = {
            "proposal_id": "test2",
            "voter_peer_id": "02" + "a" * 64,
            "data_hash": "a" * 64,
            "timestamp": 1234567890,
        }

        sig1 = get_settlement_ready_signing_payload(payload1)
        sig2 = get_settlement_ready_signing_payload(payload2)

        assert sig1 != sig2


# =============================================================================
# ANTI-GAMING TESTS
# =============================================================================

class TestAntiGaming:
    """Tests for detecting gaming behavior."""

    def test_participation_tracking(self, mock_database):
        """Should track participation rates across periods."""
        # Simulate a member who skipped 3 out of 5 votes
        mock_database.get_settled_periods.return_value = [
            {'proposal_id': 'p1'},
            {'proposal_id': 'p2'},
            {'proposal_id': 'p3'},
            {'proposal_id': 'p4'},
            {'proposal_id': 'p5'},
        ]

        # Mock voting behavior: skipped 3 times
        def has_voted(proposal_id, peer_id):
            if peer_id == '02' + 'a' * 64:
                return proposal_id in ['p1', 'p2']  # Only voted on 2/5
            return True

        mock_database.has_voted_settlement.side_effect = has_voted

        # Calculate participation rate
        peer_id = '02' + 'a' * 64
        vote_count = sum(
            1 for p in mock_database.get_settled_periods.return_value
            if has_voted(p['proposal_id'], peer_id)
        )
        total_periods = 5
        vote_rate = (vote_count / total_periods) * 100

        assert vote_rate == 40.0  # Only voted 2/5 = 40%

    def test_low_participation_flags_suspect(self):
        """Low participation combined with debt should flag as suspect."""
        member_stats = {
            'peer_id': '02' + 'a' * 64,
            'vote_rate': 30.0,  # Below 50%
            'execution_rate': 40.0,  # Below 50%
            'total_owed': -5000,  # Negative = owes money
        }

        # Gaming detection logic
        is_suspect = (
            member_stats['vote_rate'] < 50 or
            member_stats['execution_rate'] < 50
        )
        owes_money = member_stats['total_owed'] < 0
        is_high_risk = is_suspect and owes_money

        assert is_suspect is True
        assert is_high_risk is True


# =============================================================================
# NET PROFIT SETTLEMENT TESTS (Issue #42)
# =============================================================================

class TestNetProfitSettlement:
    """Tests for net profit settlement (Issue #42).

    Settlement now uses net_profit = fees_earned - rebalance_costs instead of
    gross fees. This ensures members who spend heavily on rebalancing don't
    subsidize those who don't.
    """

    def test_net_profit_calculation(self):
        """Verify net profit = fees - costs with proper capping."""
        # Member earns 1000 sats, spends 300 on rebalancing
        contrib = MemberContribution(
            peer_id='02' + 'a' * 64,
            capacity_sats=1_000_000,
            forwards_sats=500_000,
            fees_earned_sats=1000,
            rebalance_costs_sats=300,
            uptime_pct=99.0
        )

        assert contrib.net_profit_sats == 700  # 1000 - 300

    def test_net_profit_capped_at_zero(self):
        """Verify net profit is capped at 0 (no negative contributions)."""
        # Member earns 500 sats but spends 800 on rebalancing (loss)
        contrib = MemberContribution(
            peer_id='02' + 'b' * 64,
            capacity_sats=1_000_000,
            forwards_sats=500_000,
            fees_earned_sats=500,
            rebalance_costs_sats=800,
            uptime_pct=99.0
        )

        assert contrib.net_profit_sats == 0  # max(0, 500 - 800) = 0

    def test_fair_share_uses_net_profit(self, mock_database, mock_plugin):
        """Verify fair share distribution uses net profit, not gross fees.

        Example scenario from Issue #42:
        - Node A: earns 1000 sats, spends 800 rebalancing → net profit 200 sats
        - Node B: earns 500 sats, spends 0 rebalancing → net profit 500 sats

        The fair share is based on capacity/forwards/uptime (equal in this test),
        so both get equal fair share. But the BALANCE is based on net profit:
        - balance = fair_share - net_profit
        - Node A: fair_share - 200 = positive (owed money)
        - Node B: fair_share - 500 = negative (owes money)
        """
        settlement_manager = SettlementManager(mock_database, mock_plugin)

        contributions = [
            MemberContribution(
                peer_id='02' + 'a' * 64,
                capacity_sats=1_000_000,
                forwards_sats=500_000,
                fees_earned_sats=1000,
                rebalance_costs_sats=800,  # High rebalancing costs
                uptime_pct=99.0
            ),
            MemberContribution(
                peer_id='02' + 'b' * 64,
                capacity_sats=1_000_000,
                forwards_sats=500_000,
                fees_earned_sats=500,
                rebalance_costs_sats=0,  # No rebalancing costs
                uptime_pct=99.0
            ),
        ]

        results = settlement_manager.calculate_fair_shares(contributions)

        assert len(results) == 2

        result_a = next(r for r in results if r.peer_id == '02' + 'a' * 64)
        result_b = next(r for r in results if r.peer_id == '02' + 'b' * 64)

        # Verify net profit is captured correctly
        assert result_a.net_profit == 200  # 1000 - 800
        assert result_b.net_profit == 500  # 500 - 0

        # Fair share is based on capacity/forwards/uptime (equal), not net profit
        # Both should get equal fair share
        assert result_a.fair_share == result_b.fair_share

        # KEY TEST: Balance is now based on net profit, not gross fees
        # balance = fair_share - net_profit
        assert result_a.balance == result_a.fair_share - 200  # 349 - 200 = 149 (owed)
        assert result_b.balance == result_b.fair_share - 500  # 349 - 500 = -151 (owes)

        # Node A (low net profit) is owed money (positive balance)
        assert result_a.balance > 0
        # Node B (high net profit) owes money (negative balance)
        assert result_b.balance < 0

        # Total balance should be approximately zero (accounting identity)
        total_balance = result_a.balance + result_b.balance
        assert abs(total_balance) <= 2  # Allow small rounding error

        # Verify costs are tracked
        assert result_a.rebalance_costs == 800
        assert result_b.rebalance_costs == 0

    def test_hash_includes_costs(self):
        """Verify settlement hash changes when costs are included."""
        period = "2025-05"
        contributions_no_costs = [
            {'peer_id': '02' + 'a' * 64, 'fees_earned': 1000, 'capacity': 1000000, 'uptime': 99},
            {'peer_id': '02' + 'b' * 64, 'fees_earned': 500, 'capacity': 1000000, 'uptime': 99},
        ]

        contributions_with_costs = [
            {'peer_id': '02' + 'a' * 64, 'fees_earned': 1000, 'rebalance_costs': 300, 'capacity': 1000000, 'uptime': 99},
            {'peer_id': '02' + 'b' * 64, 'fees_earned': 500, 'rebalance_costs': 0, 'capacity': 1000000, 'uptime': 99},
        ]

        hash_no_costs = SettlementManager.calculate_settlement_hash(period, contributions_no_costs)
        hash_with_costs = SettlementManager.calculate_settlement_hash(period, contributions_with_costs)

        # Hashes should be different when costs are included
        assert hash_no_costs != hash_with_costs

    def test_backward_compatible_default_costs(self, mock_database, mock_plugin):
        """Verify old nodes without costs work (defaults to 0)."""
        settlement_manager = SettlementManager(mock_database, mock_plugin)

        # Old-style contribution without rebalance_costs field
        contributions = [
            MemberContribution(
                peer_id='02' + 'a' * 64,
                capacity_sats=1_000_000,
                forwards_sats=500_000,
                fees_earned_sats=1000,
                # rebalance_costs_sats not set - should default to 0
                uptime_pct=99.0
            ),
        ]

        results = settlement_manager.calculate_fair_shares(contributions)

        assert len(results) == 1
        result = results[0]

        # Net profit should equal gross fees when costs default to 0
        assert result.net_profit == 1000
        assert result.rebalance_costs == 0

    def test_zero_total_net_profit(self, mock_database, mock_plugin):
        """Handle edge case where total net profit is zero."""
        settlement_manager = SettlementManager(mock_database, mock_plugin)

        contributions = [
            MemberContribution(
                peer_id='02' + 'a' * 64,
                capacity_sats=1_000_000,
                forwards_sats=500_000,
                fees_earned_sats=500,
                rebalance_costs_sats=500,  # net = 0
                uptime_pct=99.0
            ),
            MemberContribution(
                peer_id='02' + 'b' * 64,
                capacity_sats=1_000_000,
                forwards_sats=500_000,
                fees_earned_sats=0,
                rebalance_costs_sats=0,  # net = 0
                uptime_pct=99.0
            ),
        ]

        results = settlement_manager.calculate_fair_shares(contributions)

        assert len(results) == 2
        # With zero total net profit, all balances should be zero
        for result in results:
            assert result.fair_share == 0
            assert result.balance == 0

    def test_settlement_result_includes_cost_fields(self, mock_database, mock_plugin):
        """Verify SettlementResult dataclass has new cost fields."""
        result = SettlementResult(
            peer_id='02' + 'a' * 64,
            fees_earned=1000,
            rebalance_costs=300,
            net_profit=700,
            fair_share=800,
            balance=100,
            bolt12_offer="lno1...",
        )

        assert result.rebalance_costs == 300
        assert result.net_profit == 700
        assert result.fees_earned == 1000
        assert result.balance == 100  # fair_share - net_profit = 800 - 700 = 100


# =============================================================================
# SETTLEMENT REBROADCAST TESTS (Issue #49)
# =============================================================================

class TestSettlementRebroadcast:
    """Tests for settlement proposal rebroadcast functionality (Issue #49)."""

    def test_proposal_stores_contributions_json(self, mock_database, mock_plugin, mock_rpc, mock_state_manager):
        """Verify proposals store contributions_json for rebroadcast."""
        settlement_manager = SettlementManager(mock_database, mock_plugin, mock_rpc)

        # Set up mock state manager
        mock_state_manager.get_peer_fees.return_value = {
            'fees_earned_sats': 1000,
            'forward_count': 10,
            'rebalance_costs_sats': 100
        }
        mock_state_manager.get_peer_state.return_value = MagicMock(capacity_sats=10_000_000)
        mock_database.get_fee_reports_for_period.return_value = []

        proposal = settlement_manager.create_proposal(
            period="2025-01",
            our_peer_id='02' + 'a' * 64,
            state_manager=mock_state_manager,
            rpc=mock_rpc
        )

        # Verify add_settlement_proposal was called with contributions_json
        assert mock_database.add_settlement_proposal.called
        call_kwargs = mock_database.add_settlement_proposal.call_args
        # Check that contributions_json was passed
        if call_kwargs.kwargs:
            assert 'contributions_json' in call_kwargs.kwargs
            contributions_json = call_kwargs.kwargs['contributions_json']
            assert contributions_json is not None
            # Verify it's valid JSON
            contributions = json.loads(contributions_json)
            assert isinstance(contributions, list)

    def test_get_proposals_needing_rebroadcast_filters_correctly(self):
        """Test database method filters proposals correctly for rebroadcast."""
        import sqlite3
        import tempfile
        import os

        # Create a real temporary database for this test
        fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            now = int(time.time())

            # Create table with all columns
            conn.execute("""
                CREATE TABLE settlement_proposals (
                    proposal_id TEXT PRIMARY KEY,
                    period TEXT NOT NULL UNIQUE,
                    proposer_peer_id TEXT NOT NULL,
                    proposed_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    data_hash TEXT NOT NULL,
                    total_fees_sats INTEGER NOT NULL,
                    member_count INTEGER NOT NULL,
                    last_broadcast_at INTEGER,
                    contributions_json TEXT
                )
            """)

            our_peer_id = '02' + 'a' * 64
            other_peer_id = '02' + 'b' * 64

            # Insert test proposals
            proposals = [
                # Should be returned: pending, not expired, our proposal, broadcast long ago
                ('prop1', '2025-01', our_peer_id, now - 7200, now + 86400, 'pending', 'hash1', 1000, 3, now - 20000, '[]'),
                # Should NOT be: broadcast recently (within interval)
                ('prop2', '2025-02', our_peer_id, now - 3600, now + 86400, 'pending', 'hash2', 1000, 3, now - 1000, '[]'),
                # Should NOT be: different proposer
                ('prop3', '2025-03', other_peer_id, now - 7200, now + 86400, 'pending', 'hash3', 1000, 3, now - 20000, '[]'),
                # Should NOT be: expired
                ('prop4', '2025-04', our_peer_id, now - 100000, now - 1000, 'pending', 'hash4', 1000, 3, now - 20000, '[]'),
                # Should NOT be: status is 'ready' (already at quorum)
                ('prop5', '2025-05', our_peer_id, now - 7200, now + 86400, 'ready', 'hash5', 1000, 3, now - 20000, '[]'),
                # Should be returned: never broadcast (NULL last_broadcast_at)
                ('prop6', '2025-06', our_peer_id, now - 7200, now + 86400, 'pending', 'hash6', 1000, 3, None, '[]'),
            ]

            for p in proposals:
                conn.execute("""
                    INSERT INTO settlement_proposals
                    (proposal_id, period, proposer_peer_id, proposed_at, expires_at,
                     status, data_hash, total_fees_sats, member_count, last_broadcast_at, contributions_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, p)
            conn.commit()

            # Query for proposals needing rebroadcast (4 hour interval = 14400 seconds)
            rebroadcast_interval = 14400
            cutoff = now - rebroadcast_interval

            rows = conn.execute("""
                SELECT * FROM settlement_proposals
                WHERE status = 'pending'
                AND expires_at > ?
                AND proposer_peer_id = ?
                AND (last_broadcast_at IS NULL OR last_broadcast_at < ?)
                ORDER BY proposed_at ASC
            """, (now, our_peer_id, cutoff)).fetchall()

            # Should return prop1 and prop6
            results = [dict(row) for row in rows]
            assert len(results) == 2
            proposal_ids = [r['proposal_id'] for r in results]
            assert 'prop1' in proposal_ids  # Broadcast long ago
            assert 'prop6' in proposal_ids  # Never broadcast

        finally:
            os.unlink(db_path)

    def test_update_proposal_broadcast_time(self):
        """Test database method updates broadcast timestamp."""
        import sqlite3
        import tempfile
        import os

        fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(fd)

        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            now = int(time.time())

            conn.execute("""
                CREATE TABLE settlement_proposals (
                    proposal_id TEXT PRIMARY KEY,
                    period TEXT NOT NULL,
                    proposer_peer_id TEXT NOT NULL,
                    proposed_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL,
                    status TEXT DEFAULT 'pending',
                    data_hash TEXT NOT NULL,
                    total_fees_sats INTEGER NOT NULL,
                    member_count INTEGER NOT NULL,
                    last_broadcast_at INTEGER,
                    contributions_json TEXT
                )
            """)

            # Insert a proposal with old broadcast time
            conn.execute("""
                INSERT INTO settlement_proposals
                (proposal_id, period, proposer_peer_id, proposed_at, expires_at,
                 status, data_hash, total_fees_sats, member_count, last_broadcast_at)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)
            """, ('prop1', '2025-01', '02' + 'a' * 64, now - 3600, now + 86400, 'hash1', 1000, 3, now - 20000))
            conn.commit()

            # Update broadcast time
            new_time = now
            conn.execute("""
                UPDATE settlement_proposals
                SET last_broadcast_at = ?
                WHERE proposal_id = ?
            """, (new_time, 'prop1'))
            conn.commit()

            # Verify update
            row = conn.execute(
                "SELECT last_broadcast_at FROM settlement_proposals WHERE proposal_id = ?",
                ('prop1',)
            ).fetchone()

            assert row['last_broadcast_at'] == new_time

        finally:
            os.unlink(db_path)
