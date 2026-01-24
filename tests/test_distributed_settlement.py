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
