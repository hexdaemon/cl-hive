"""
Unit tests for cl-hive splice manager.

Tests:
1. Protocol validation - Splice message payload validation
2. Message creation - Signed message generation
3. Session management - Create, update, cleanup sessions
4. Splice coordination - Initiate and handle splice workflow

Run with: pytest tests/test_splice_manager.py -v
"""

import pytest
import time
import json
from unittest.mock import Mock, MagicMock, patch

# Import modules under test
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.protocol import (
    HIVE_MAGIC,
    HiveMessageType,
    serialize,
    deserialize,
    # Splice constants
    SPLICE_SESSION_TIMEOUT_SECONDS,
    SPLICE_TYPE_IN, SPLICE_TYPE_OUT,
    SPLICE_STATUS_PENDING, SPLICE_STATUS_INIT_SENT, SPLICE_STATUS_COMPLETED,
    SPLICE_STATUS_FAILED, SPLICE_STATUS_ABORTED,
    SPLICE_REJECT_NOT_MEMBER, SPLICE_REJECT_NO_CHANNEL,
    # Validation functions
    validate_splice_init_request_payload,
    validate_splice_init_response_payload,
    validate_splice_update_payload,
    validate_splice_signed_payload,
    validate_splice_abort_payload,
    # Signing payload functions
    get_splice_init_request_signing_payload,
    get_splice_init_response_signing_payload,
    get_splice_update_signing_payload,
    get_splice_signed_signing_payload,
    get_splice_abort_signing_payload,
    # Message creation functions
    create_splice_init_request,
    create_splice_init_response,
    create_splice_update,
    create_splice_signed,
    create_splice_abort,
)


# =============================================================================
# TEST FIXTURES
# =============================================================================

@pytest.fixture
def mock_plugin():
    """Create a mock plugin for testing."""
    plugin = Mock()
    plugin.log = Mock()
    return plugin


@pytest.fixture
def mock_rpc():
    """Create a mock RPC interface."""
    rpc = Mock()
    rpc.signmessage = Mock(return_value={"signature": "test_signature_abc123"})
    rpc.checkmessage = Mock(return_value={"verified": True, "pubkey": "02" + "a" * 64})
    rpc.listpeerchannels = Mock(return_value={"channels": []})
    rpc.feerates = Mock(return_value={"perkw": {"urgent": 10000}})
    rpc.call = Mock()
    return rpc


@pytest.fixture
def mock_database():
    """Create a mock database for testing."""
    db = Mock()
    db.get_member = Mock(return_value={"peer_id": "02" + "a" * 64, "tier": "member"})
    db.is_banned = Mock(return_value=False)
    db.create_splice_session = Mock(return_value=True)
    db.get_splice_session = Mock(return_value=None)
    db.get_active_splice_for_channel = Mock(return_value=None)
    db.get_active_splice_for_peer = Mock(return_value=None)
    db.update_splice_session = Mock(return_value=True)
    db.cleanup_expired_splice_sessions = Mock(return_value=0)
    db.get_pending_splice_sessions = Mock(return_value=[])
    db.delete_splice_session = Mock(return_value=True)
    return db


@pytest.fixture
def mock_splice_coordinator():
    """Create a mock splice coordinator."""
    coord = Mock()
    coord.check_splice_out_safety = Mock(return_value={
        "safety": "safe",
        "can_proceed": True,
        "reason": "Safe to splice"
    })
    return coord


@pytest.fixture
def sample_pubkey():
    """Sample 66-char hex pubkey."""
    return "02" + "a" * 64


@pytest.fixture
def sample_session_id():
    """Sample session ID."""
    return "splice_02aaaaaa_1234567890_abcd1234"


@pytest.fixture
def sample_channel_id():
    """Sample channel ID."""
    return "123x456x0"


@pytest.fixture
def sample_psbt():
    """Sample PSBT string."""
    return "cHNidP8B" + "A" * 100


# =============================================================================
# PAYLOAD VALIDATION TESTS
# =============================================================================

class TestSpliceInitRequestValidation:
    """Test SPLICE_INIT_REQUEST payload validation."""

    def test_valid_payload(self, sample_pubkey, sample_session_id, sample_channel_id, sample_psbt):
        """Valid payload should pass validation."""
        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": sample_channel_id,
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 1000000,
            "psbt": sample_psbt,
            "timestamp": int(time.time()),
            "signature": "valid_signature_here"
        }
        assert validate_splice_init_request_payload(payload) is True

    def test_invalid_initiator_id(self, sample_session_id, sample_channel_id, sample_psbt):
        """Invalid initiator_id should fail validation."""
        payload = {
            "initiator_id": "invalid",
            "session_id": sample_session_id,
            "channel_id": sample_channel_id,
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 1000000,
            "psbt": sample_psbt,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_init_request_payload(payload) is False

    def test_invalid_splice_type(self, sample_pubkey, sample_session_id, sample_channel_id, sample_psbt):
        """Invalid splice_type should fail validation."""
        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": sample_channel_id,
            "splice_type": "invalid_type",
            "amount_sats": 1000000,
            "psbt": sample_psbt,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_init_request_payload(payload) is False

    def test_zero_amount(self, sample_pubkey, sample_session_id, sample_channel_id, sample_psbt):
        """Zero amount should fail validation."""
        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": sample_channel_id,
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 0,
            "psbt": sample_psbt,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_init_request_payload(payload) is False

    def test_negative_amount(self, sample_pubkey, sample_session_id, sample_channel_id, sample_psbt):
        """Negative amount should fail validation."""
        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": sample_channel_id,
            "splice_type": SPLICE_TYPE_OUT,
            "amount_sats": -1000000,
            "psbt": sample_psbt,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_init_request_payload(payload) is False

    def test_missing_psbt(self, sample_pubkey, sample_session_id, sample_channel_id):
        """Missing PSBT should fail validation."""
        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": sample_channel_id,
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 1000000,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_init_request_payload(payload) is False

    def test_missing_signature(self, sample_pubkey, sample_session_id, sample_channel_id, sample_psbt):
        """Missing signature should fail validation."""
        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": sample_channel_id,
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 1000000,
            "psbt": sample_psbt,
            "timestamp": int(time.time()),
        }
        assert validate_splice_init_request_payload(payload) is False


class TestSpliceInitResponseValidation:
    """Test SPLICE_INIT_RESPONSE payload validation."""

    def test_valid_accepted_response(self, sample_pubkey, sample_session_id, sample_psbt):
        """Valid accepted response should pass validation."""
        payload = {
            "responder_id": sample_pubkey,
            "session_id": sample_session_id,
            "accepted": True,
            "psbt": sample_psbt,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_init_response_payload(payload) is True

    def test_valid_rejected_response(self, sample_pubkey, sample_session_id):
        """Valid rejected response should pass validation."""
        payload = {
            "responder_id": sample_pubkey,
            "session_id": sample_session_id,
            "accepted": False,
            "reason": "channel_busy",
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_init_response_payload(payload) is True

    def test_accepted_without_psbt(self, sample_pubkey, sample_session_id):
        """Accepted response without PSBT should pass validation (PSBT is optional, CLN handles exchange)."""
        payload = {
            "responder_id": sample_pubkey,
            "session_id": sample_session_id,
            "accepted": True,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        # PSBT is optional - CLN handles PSBT exchange internally via Lightning protocol
        assert validate_splice_init_response_payload(payload) is True


class TestSpliceUpdateValidation:
    """Test SPLICE_UPDATE payload validation."""

    def test_valid_update(self, sample_pubkey, sample_session_id, sample_psbt):
        """Valid update should pass validation."""
        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "psbt": sample_psbt,
            "commitments_secured": False,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_update_payload(payload) is True

    def test_commitments_secured_true(self, sample_pubkey, sample_session_id, sample_psbt):
        """Update with commitments_secured=True should pass."""
        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "psbt": sample_psbt,
            "commitments_secured": True,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_update_payload(payload) is True

    def test_missing_commitments_secured(self, sample_pubkey, sample_session_id, sample_psbt):
        """Missing commitments_secured should fail validation."""
        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "psbt": sample_psbt,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_update_payload(payload) is False


class TestSpliceSignedValidation:
    """Test SPLICE_SIGNED payload validation."""

    def test_valid_with_txid(self, sample_pubkey, sample_session_id):
        """Valid payload with txid should pass."""
        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "txid": "a" * 64,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_signed_payload(payload) is True

    def test_valid_with_signed_psbt(self, sample_pubkey, sample_session_id, sample_psbt):
        """Valid payload with signed_psbt should pass."""
        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "signed_psbt": sample_psbt,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_signed_payload(payload) is True

    def test_missing_both_txid_and_psbt(self, sample_pubkey, sample_session_id):
        """Missing both txid and signed_psbt should fail."""
        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_signed_payload(payload) is False

    def test_invalid_txid_length(self, sample_pubkey, sample_session_id):
        """Invalid txid length should fail."""
        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "txid": "a" * 32,  # Too short
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_signed_payload(payload) is False


class TestSpliceAbortValidation:
    """Test SPLICE_ABORT payload validation."""

    def test_valid_abort(self, sample_pubkey, sample_session_id):
        """Valid abort should pass validation."""
        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "reason": "user_cancelled",
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_abort_payload(payload) is True

    def test_reason_too_long(self, sample_pubkey, sample_session_id):
        """Reason exceeding max length should fail."""
        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "reason": "x" * 600,  # Too long
            "timestamp": int(time.time()),
            "signature": "valid_signature"
        }
        assert validate_splice_abort_payload(payload) is False


# =============================================================================
# SIGNING PAYLOAD TESTS
# =============================================================================

class TestSigningPayloads:
    """Test signing payload generation."""

    def test_splice_init_request_signing_payload(self, sample_pubkey, sample_session_id, sample_channel_id):
        """Signing payload should be deterministic."""
        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": sample_channel_id,
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 1000000,
            "timestamp": 1234567890,
        }

        signing_payload = get_splice_init_request_signing_payload(payload)
        parsed = json.loads(signing_payload)

        assert parsed["initiator_id"] == sample_pubkey
        assert parsed["session_id"] == sample_session_id
        assert parsed["channel_id"] == sample_channel_id
        assert parsed["splice_type"] == SPLICE_TYPE_IN
        assert parsed["amount_sats"] == 1000000
        assert parsed["timestamp"] == 1234567890

    def test_signing_payload_is_deterministic(self, sample_pubkey, sample_session_id):
        """Same payload should always produce same signing string."""
        payload = {
            "responder_id": sample_pubkey,
            "session_id": sample_session_id,
            "accepted": True,
            "timestamp": 1234567890,
        }

        result1 = get_splice_init_response_signing_payload(payload)
        result2 = get_splice_init_response_signing_payload(payload)

        assert result1 == result2


# =============================================================================
# MESSAGE CREATION TESTS
# =============================================================================

class TestMessageCreation:
    """Test message creation functions."""

    def test_create_splice_init_request(
        self, mock_rpc, sample_pubkey, sample_session_id, sample_channel_id, sample_psbt
    ):
        """Should create valid SPLICE_INIT_REQUEST message."""
        msg = create_splice_init_request(
            initiator_id=sample_pubkey,
            session_id=sample_session_id,
            channel_id=sample_channel_id,
            splice_type=SPLICE_TYPE_IN,
            amount_sats=1000000,
            psbt=sample_psbt,
            timestamp=int(time.time()),
            rpc=mock_rpc
        )

        assert msg is not None
        assert msg[:4] == HIVE_MAGIC

        msg_type, payload = deserialize(msg)
        assert msg_type == HiveMessageType.SPLICE_INIT_REQUEST
        assert payload["initiator_id"] == sample_pubkey
        assert payload["splice_type"] == SPLICE_TYPE_IN
        assert payload["amount_sats"] == 1000000

    def test_create_splice_init_response_accepted(
        self, mock_rpc, sample_pubkey, sample_session_id, sample_psbt
    ):
        """Should create valid accepted SPLICE_INIT_RESPONSE message."""
        msg = create_splice_init_response(
            responder_id=sample_pubkey,
            session_id=sample_session_id,
            accepted=True,
            timestamp=int(time.time()),
            rpc=mock_rpc,
            psbt=sample_psbt
        )

        assert msg is not None
        msg_type, payload = deserialize(msg)
        assert msg_type == HiveMessageType.SPLICE_INIT_RESPONSE
        assert payload["accepted"] is True
        assert payload["psbt"] == sample_psbt

    def test_create_splice_init_response_rejected(
        self, mock_rpc, sample_pubkey, sample_session_id
    ):
        """Should create valid rejected SPLICE_INIT_RESPONSE message."""
        msg = create_splice_init_response(
            responder_id=sample_pubkey,
            session_id=sample_session_id,
            accepted=False,
            timestamp=int(time.time()),
            rpc=mock_rpc,
            reason=SPLICE_REJECT_NO_CHANNEL
        )

        assert msg is not None
        msg_type, payload = deserialize(msg)
        assert msg_type == HiveMessageType.SPLICE_INIT_RESPONSE
        assert payload["accepted"] is False
        assert payload["reason"] == SPLICE_REJECT_NO_CHANNEL

    def test_create_splice_update(
        self, mock_rpc, sample_pubkey, sample_session_id, sample_psbt
    ):
        """Should create valid SPLICE_UPDATE message."""
        msg = create_splice_update(
            sender_id=sample_pubkey,
            session_id=sample_session_id,
            psbt=sample_psbt,
            commitments_secured=False,
            timestamp=int(time.time()),
            rpc=mock_rpc
        )

        assert msg is not None
        msg_type, payload = deserialize(msg)
        assert msg_type == HiveMessageType.SPLICE_UPDATE
        assert payload["commitments_secured"] is False

    def test_create_splice_signed_with_txid(self, mock_rpc, sample_pubkey, sample_session_id):
        """Should create valid SPLICE_SIGNED message with txid."""
        txid = "a" * 64
        msg = create_splice_signed(
            sender_id=sample_pubkey,
            session_id=sample_session_id,
            timestamp=int(time.time()),
            rpc=mock_rpc,
            txid=txid
        )

        assert msg is not None
        msg_type, payload = deserialize(msg)
        assert msg_type == HiveMessageType.SPLICE_SIGNED
        assert payload["txid"] == txid

    def test_create_splice_abort(self, mock_rpc, sample_pubkey, sample_session_id):
        """Should create valid SPLICE_ABORT message."""
        msg = create_splice_abort(
            sender_id=sample_pubkey,
            session_id=sample_session_id,
            reason="user_cancelled",
            timestamp=int(time.time()),
            rpc=mock_rpc
        )

        assert msg is not None
        msg_type, payload = deserialize(msg)
        assert msg_type == HiveMessageType.SPLICE_ABORT
        assert payload["reason"] == "user_cancelled"


# =============================================================================
# SPLICE MANAGER TESTS
# =============================================================================

class TestSpliceManager:
    """Test SpliceManager class."""

    @pytest.fixture
    def splice_manager(self, mock_database, mock_plugin, mock_splice_coordinator, sample_pubkey):
        """Create a SpliceManager instance for testing."""
        from modules.splice_manager import SpliceManager
        return SpliceManager(
            database=mock_database,
            plugin=mock_plugin,
            splice_coordinator=mock_splice_coordinator,
            our_pubkey=sample_pubkey
        )

    def test_initiate_splice_non_member(
        self, splice_manager, mock_rpc, mock_database, sample_pubkey, sample_channel_id
    ):
        """Should fail if peer is not a hive member."""
        mock_database.get_member.return_value = None

        result = splice_manager.initiate_splice(
            peer_id=sample_pubkey,
            channel_id=sample_channel_id,
            relative_amount=1000000,
            rpc=mock_rpc
        )

        assert result.get("error") == "not_member"

    def test_initiate_splice_no_channel(
        self, splice_manager, mock_rpc, mock_database, sample_pubkey, sample_channel_id
    ):
        """Should fail if no channel exists with peer."""
        mock_database.get_member.return_value = {"peer_id": sample_pubkey, "tier": "member"}
        mock_rpc.call.return_value = {"channels": []}

        result = splice_manager.initiate_splice(
            peer_id=sample_pubkey,
            channel_id=sample_channel_id,
            relative_amount=1000000,
            rpc=mock_rpc
        )

        assert result.get("error") == "no_channel"

    def test_initiate_splice_zero_amount(
        self, splice_manager, mock_rpc, sample_pubkey, sample_channel_id
    ):
        """Should fail with zero amount."""
        result = splice_manager.initiate_splice(
            peer_id=sample_pubkey,
            channel_id=sample_channel_id,
            relative_amount=0,
            rpc=mock_rpc
        )

        assert result.get("error") == "invalid_amount"

    def test_initiate_splice_dry_run(
        self, splice_manager, mock_rpc, mock_database, sample_pubkey, sample_channel_id
    ):
        """Dry run should return preview without executing."""
        mock_database.get_member.return_value = {"peer_id": sample_pubkey, "tier": "member"}
        mock_rpc.call.return_value = {
            "channels": [{
                "peer_id": sample_pubkey,
                "short_channel_id": sample_channel_id,
                "channel_id": "abc123def456",
                "state": "CHANNELD_NORMAL"
            }]
        }

        result = splice_manager.initiate_splice(
            peer_id=sample_pubkey,
            channel_id=sample_channel_id,
            relative_amount=1000000,
            rpc=mock_rpc,
            dry_run=True
        )

        assert result.get("dry_run") is True
        assert result.get("splice_type") == SPLICE_TYPE_IN
        assert result.get("amount_sats") == 1000000

    def test_initiate_splice_out_safety_blocked(
        self, splice_manager, mock_rpc, mock_database, mock_splice_coordinator,
        sample_pubkey, sample_channel_id
    ):
        """Splice-out should be blocked if safety check fails."""
        mock_database.get_member.return_value = {"peer_id": sample_pubkey, "tier": "member"}
        mock_rpc.call.return_value = {
            "channels": [{
                "peer_id": sample_pubkey,
                "short_channel_id": sample_channel_id,
                "channel_id": "abc123def456",
                "state": "CHANNELD_NORMAL"
            }]
        }
        mock_splice_coordinator.check_splice_out_safety.return_value = {
            "safety": "blocked",
            "can_proceed": False,
            "reason": "Would eliminate fleet connectivity"
        }

        result = splice_manager.initiate_splice(
            peer_id=sample_pubkey,
            channel_id=sample_channel_id,
            relative_amount=-500000,  # Negative = splice-out
            rpc=mock_rpc
        )

        assert result.get("error") == "safety_blocked"

    def test_cleanup_expired_sessions(self, splice_manager, mock_database):
        """Should cleanup expired sessions."""
        mock_database.cleanup_expired_splice_sessions.return_value = 3

        count = splice_manager.cleanup_expired_sessions()

        assert count == 3
        mock_database.cleanup_expired_splice_sessions.assert_called_once()

    def test_get_active_sessions(self, splice_manager, mock_database):
        """Should return active sessions."""
        expected_sessions = [
            {"session_id": "session1", "status": "pending"},
            {"session_id": "session2", "status": "updating"},
        ]
        mock_database.get_pending_splice_sessions.return_value = expected_sessions

        sessions = splice_manager.get_active_sessions()

        assert sessions == expected_sessions

    def test_abort_session(self, splice_manager, mock_rpc, mock_database, sample_session_id, sample_pubkey):
        """Should abort an active session."""
        mock_database.get_splice_session.return_value = {
            "session_id": sample_session_id,
            "peer_id": sample_pubkey,
            "status": "updating"
        }

        result = splice_manager.abort_session(sample_session_id, mock_rpc)

        assert result.get("success") is True
        assert result.get("status") == SPLICE_STATUS_ABORTED

    def test_abort_completed_session(
        self, splice_manager, mock_rpc, mock_database, sample_session_id, sample_pubkey
    ):
        """Should fail to abort already completed session."""
        mock_database.get_splice_session.return_value = {
            "session_id": sample_session_id,
            "peer_id": sample_pubkey,
            "status": SPLICE_STATUS_COMPLETED
        }

        result = splice_manager.abort_session(sample_session_id, mock_rpc)

        assert result.get("error") == "session_already_ended"


# =============================================================================
# ROUND TRIP TESTS
# =============================================================================

class TestMessageRoundTrip:
    """Test full message serialize/deserialize round trips."""

    def test_splice_init_request_round_trip(
        self, mock_rpc, sample_pubkey, sample_session_id, sample_channel_id, sample_psbt
    ):
        """SPLICE_INIT_REQUEST should survive round trip."""
        original = create_splice_init_request(
            initiator_id=sample_pubkey,
            session_id=sample_session_id,
            channel_id=sample_channel_id,
            splice_type=SPLICE_TYPE_OUT,
            amount_sats=500000,
            psbt=sample_psbt,
            timestamp=1234567890,
            rpc=mock_rpc
        )

        msg_type, payload = deserialize(original)

        assert msg_type == HiveMessageType.SPLICE_INIT_REQUEST
        assert payload["initiator_id"] == sample_pubkey
        assert payload["session_id"] == sample_session_id
        assert payload["channel_id"] == sample_channel_id
        assert payload["splice_type"] == SPLICE_TYPE_OUT
        assert payload["amount_sats"] == 500000
        assert payload["psbt"] == sample_psbt

    def test_all_splice_messages_have_hive_magic(self, mock_rpc, sample_pubkey, sample_session_id, sample_psbt):
        """All splice messages should have HIVE magic prefix."""
        messages = [
            create_splice_init_request(
                sample_pubkey, sample_session_id, "123x456x0",
                SPLICE_TYPE_IN, 1000000, sample_psbt, int(time.time()), mock_rpc
            ),
            create_splice_init_response(
                sample_pubkey, sample_session_id, True, int(time.time()), mock_rpc, psbt=sample_psbt
            ),
            create_splice_update(
                sample_pubkey, sample_session_id, sample_psbt, False, int(time.time()), mock_rpc
            ),
            create_splice_signed(
                sample_pubkey, sample_session_id, int(time.time()), mock_rpc, txid="a"*64
            ),
            create_splice_abort(
                sample_pubkey, sample_session_id, "test", int(time.time()), mock_rpc
            ),
        ]

        for msg in messages:
            assert msg is not None
            assert msg[:4] == HIVE_MAGIC
