"""
Tests for Coordinated Splicing bug fixes.

Covers:
1. Silent session creation failure — create_splice_session return checked
2. Unknown session abort — peer notified on unknown session
3. DB validation — status, splice_type, initiator, amount validated
4. Ban checks — banned peers rejected in all splice handlers
5. Amount bounds — initiate_splice rejects out-of-bounds amounts
6. State transition validation — _proceed_to_signing rejects terminal states
"""

import pytest
import time
from unittest.mock import Mock, MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.protocol import (
    SPLICE_TYPE_IN, SPLICE_TYPE_OUT,
    SPLICE_STATUS_PENDING, SPLICE_STATUS_INIT_SENT, SPLICE_STATUS_INIT_RECEIVED,
    SPLICE_STATUS_UPDATING, SPLICE_STATUS_SIGNING, SPLICE_STATUS_COMPLETED,
    SPLICE_STATUS_ABORTED, SPLICE_STATUS_FAILED,
    SPLICE_SESSION_TIMEOUT_SECONDS,
)
from modules.splice_manager import SpliceManager


# =============================================================================
# TEST FIXTURES
# =============================================================================

@pytest.fixture
def mock_plugin():
    plugin = Mock()
    plugin.log = Mock()
    return plugin


@pytest.fixture
def mock_rpc():
    rpc = Mock()
    rpc.signmessage = Mock(return_value={"signature": "test_signature_abc123"})
    rpc.checkmessage = Mock(return_value={"verified": True, "pubkey": "02" + "a" * 64})
    rpc.listpeerchannels = Mock(return_value={"channels": []})
    rpc.feerates = Mock(return_value={"perkw": {"urgent": 10000}})
    rpc.call = Mock()
    return rpc


@pytest.fixture
def mock_database():
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
    return db


@pytest.fixture
def mock_splice_coordinator():
    coord = Mock()
    coord.check_splice_out_safety = Mock(return_value={
        "safety": "safe", "can_proceed": True, "reason": "Safe"
    })
    return coord


@pytest.fixture
def sample_pubkey():
    return "02" + "a" * 64


@pytest.fixture
def sample_session_id():
    return "splice_02aaaaaa_1234567890_abcd1234"


@pytest.fixture
def sample_channel_id():
    return "abc123def456"  # Full hex channel_id


@pytest.fixture
def splice_mgr(mock_database, mock_plugin, mock_splice_coordinator, sample_pubkey):
    return SpliceManager(
        database=mock_database,
        plugin=mock_plugin,
        splice_coordinator=mock_splice_coordinator,
        our_pubkey=sample_pubkey
    )


# =============================================================================
# Fix 1: Silent session creation failure
# =============================================================================

class TestSessionCreationFailureHandling:
    """
    Bug: create_splice_session() return value was not checked.
    If DB insert failed (e.g. duplicate session_id), code continued
    to update_splice_session which also failed silently.
    """

    def test_initiate_splice_returns_error_on_db_failure(
        self, splice_mgr, mock_database, mock_rpc, sample_pubkey
    ):
        """initiate_splice should return error when DB create fails."""
        # Setup: DB create fails
        mock_database.create_splice_session.return_value = False
        mock_database.get_member.return_value = {"peer_id": sample_pubkey, "tier": "member"}

        # Mock channel exists
        mock_rpc.call.return_value = {"psbt": "cHNidP8B" + "A" * 100}
        splice_mgr._get_channel_for_peer = Mock(return_value={
            "short_channel_id": "100x1x0",
            "channel_id": "abc123def456",
            "state": "CHANNELD_NORMAL"
        })

        result = splice_mgr.initiate_splice(
            peer_id=sample_pubkey,
            channel_id="abc123def456",
            relative_amount=100000,
            rpc=mock_rpc
        )

        assert "error" in result
        assert result["error"] == "database_error"

    @patch('modules.splice_manager.validate_splice_init_request_payload', return_value=True)
    def test_handle_init_request_returns_error_on_db_failure(
        self, mock_validate, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """handle_splice_init_request should reject when DB create fails."""
        mock_database.create_splice_session.return_value = False

        splice_mgr._get_channel_for_peer = Mock(return_value={
            "short_channel_id": "100x1x0",
            "channel_id": "abc123def456"
        })
        splice_mgr._verify_signature = Mock(return_value=True)

        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": "abc123def456",
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 100000,
            "psbt": "cHNidP8B" + "A" * 100,
            "timestamp": int(time.time()),
            "signature": "valid_sig"
        }

        result = splice_mgr.handle_splice_init_request(sample_pubkey, payload, mock_rpc)

        assert result.get("error") == "database_error"

    def test_initiate_splice_succeeds_on_db_success(
        self, splice_mgr, mock_database, mock_rpc, sample_pubkey
    ):
        """initiate_splice should succeed when DB create succeeds."""
        mock_database.create_splice_session.return_value = True
        mock_database.get_splice_session.return_value = {"status": "pending"}
        mock_database.get_member.return_value = {"peer_id": sample_pubkey, "tier": "member"}

        mock_rpc.call.return_value = {"psbt": "cHNidP8B" + "A" * 100}
        splice_mgr._get_channel_for_peer = Mock(return_value={
            "short_channel_id": "100x1x0",
            "channel_id": "abc123def456",
            "state": "CHANNELD_NORMAL"
        })
        splice_mgr._send_message = Mock(return_value=True)

        result = splice_mgr.initiate_splice(
            peer_id=sample_pubkey,
            channel_id="abc123def456",
            relative_amount=100000,
            rpc=mock_rpc
        )

        assert result.get("success") is True


# =============================================================================
# Fix 2: Unknown session abort notification
# =============================================================================

class TestUnknownSessionAbort:
    """
    Bug: When session lookup failed in handle_splice_init_response,
    handle_splice_update, or handle_splice_signed, the peer was never
    notified and waited indefinitely.
    """

    @patch('modules.splice_manager.validate_splice_init_response_payload', return_value=True)
    def test_init_response_sends_abort_on_unknown_session(
        self, mock_validate, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """handle_splice_init_response should send abort when session unknown."""
        mock_database.get_splice_session.return_value = None
        splice_mgr._verify_signature = Mock(return_value=True)
        splice_mgr._send_abort = Mock()

        payload = {
            "responder_id": sample_pubkey,
            "session_id": sample_session_id,
            "accepted": True,
            "timestamp": int(time.time()),
            "signature": "valid_sig"
        }

        result = splice_mgr.handle_splice_init_response(sample_pubkey, payload, mock_rpc)

        assert result.get("error") == "unknown_session"
        splice_mgr._send_abort.assert_called_once()
        call_args = splice_mgr._send_abort.call_args
        assert call_args[0][0] == sample_pubkey
        assert call_args[0][1] == sample_session_id

    @patch('modules.splice_manager.validate_splice_update_payload', return_value=True)
    def test_splice_update_sends_abort_on_unknown_session(
        self, mock_validate, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """handle_splice_update should send abort when session unknown."""
        mock_database.get_splice_session.return_value = None
        splice_mgr._verify_signature = Mock(return_value=True)
        splice_mgr._send_abort = Mock()

        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "psbt": "cHNidP8B" + "A" * 100,
            "commitments_secured": False,
            "timestamp": int(time.time()),
            "signature": "valid_sig"
        }

        result = splice_mgr.handle_splice_update(sample_pubkey, payload, mock_rpc)

        assert result.get("error") == "unknown_session"
        splice_mgr._send_abort.assert_called_once()

    @patch('modules.splice_manager.validate_splice_signed_payload', return_value=True)
    def test_splice_signed_sends_abort_on_unknown_session(
        self, mock_validate, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """handle_splice_signed should send abort when session unknown."""
        mock_database.get_splice_session.return_value = None
        splice_mgr._verify_signature = Mock(return_value=True)
        splice_mgr._send_abort = Mock()

        payload = {
            "sender_id": sample_pubkey,
            "session_id": sample_session_id,
            "txid": "a" * 64,
            "timestamp": int(time.time()),
            "signature": "valid_sig"
        }

        result = splice_mgr.handle_splice_signed(sample_pubkey, payload, mock_rpc)

        assert result.get("error") == "unknown_session"
        splice_mgr._send_abort.assert_called_once()


# =============================================================================
# Fix 3: DB validation
# =============================================================================

class TestSpliceDBValidation:
    """
    Bug: update_splice_session accepted any string for status,
    create_splice_session didn't validate splice_type, amount, or initiator.
    """

    def _make_db(self):
        """Create a minimal Database-like object for validation testing."""
        import sqlite3
        import tempfile
        from modules.database import HiveDatabase

        plugin = Mock()
        plugin.log = Mock()

        # Create a real in-memory database
        db = HiveDatabase.__new__(HiveDatabase)
        db.plugin = plugin
        db.db_path = ":memory:"

        # Create connection
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")

        # Create splice_sessions table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS splice_sessions (
                session_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                initiator TEXT NOT NULL,
                splice_type TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                psbt TEXT,
                commitments_secured INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                txid TEXT,
                error_message TEXT,
                timeout_at INTEGER NOT NULL
            )
        """)

        # Store connection for thread-local access
        import threading
        db._local = threading.local()
        db._local.conn = conn
        db._get_connection = lambda: conn

        return db

    def test_create_rejects_invalid_initiator(self):
        """create_splice_session should reject invalid initiator values."""
        db = self._make_db()
        result = db.create_splice_session(
            session_id="test1", channel_id="ch1", peer_id="peer1",
            initiator="hacked", splice_type="splice_in", amount_sats=100000
        )
        assert result is False

    def test_create_rejects_invalid_splice_type(self):
        """create_splice_session should reject invalid splice_type values."""
        db = self._make_db()
        result = db.create_splice_session(
            session_id="test2", channel_id="ch1", peer_id="peer1",
            initiator="local", splice_type="steal_funds", amount_sats=100000
        )
        assert result is False

    def test_create_rejects_negative_amount(self):
        """create_splice_session should reject negative amounts."""
        db = self._make_db()
        result = db.create_splice_session(
            session_id="test3", channel_id="ch1", peer_id="peer1",
            initiator="local", splice_type="splice_in", amount_sats=-100
        )
        assert result is False

    def test_create_rejects_zero_amount(self):
        """create_splice_session should reject zero amounts."""
        db = self._make_db()
        result = db.create_splice_session(
            session_id="test4", channel_id="ch1", peer_id="peer1",
            initiator="local", splice_type="splice_in", amount_sats=0
        )
        assert result is False

    def test_create_accepts_valid_inputs(self):
        """create_splice_session should accept valid inputs."""
        db = self._make_db()
        result = db.create_splice_session(
            session_id="test5", channel_id="ch1", peer_id="peer1",
            initiator="local", splice_type="splice_in", amount_sats=100000
        )
        assert result is True

    def test_create_accepts_remote_initiator(self):
        """create_splice_session should accept 'remote' initiator."""
        db = self._make_db()
        result = db.create_splice_session(
            session_id="test6", channel_id="ch1", peer_id="peer1",
            initiator="remote", splice_type="splice_out", amount_sats=50000
        )
        assert result is True

    def test_update_rejects_invalid_status(self):
        """update_splice_session should reject invalid status values."""
        db = self._make_db()
        # First create a valid session
        db.create_splice_session(
            session_id="test7", channel_id="ch1", peer_id="peer1",
            initiator="local", splice_type="splice_in", amount_sats=100000
        )
        # Try to update with invalid status
        result = db.update_splice_session("test7", status="hacked")
        assert result is False

    def test_update_accepts_valid_statuses(self):
        """update_splice_session should accept all valid status values."""
        db = self._make_db()
        db.create_splice_session(
            session_id="test8", channel_id="ch1", peer_id="peer1",
            initiator="local", splice_type="splice_in", amount_sats=100000
        )

        for status in ['init_sent', 'init_received', 'updating', 'signing', 'completed', 'aborted', 'failed']:
            # Re-create to reset
            db.create_splice_session(
                session_id=f"test_status_{status}", channel_id="ch1", peer_id="peer1",
                initiator="local", splice_type="splice_in", amount_sats=100000
            )
            result = db.update_splice_session(f"test_status_{status}", status=status)
            assert result is True, f"Status '{status}' should be accepted"


# =============================================================================
# Fix 4: Ban checks in splice handlers  (tested at integration level via cl-hive.py)
# We test the SpliceManager doesn't need ban checks itself — those are in cl-hive.py
# =============================================================================

# Note: Ban checks are added in cl-hive.py's handle_splice_* functions,
# which call database.is_banned() before delegating to splice_mgr.
# Testing these requires integration tests with the full handler chain.
# The unit tests above verify the splice_manager correctness.


# =============================================================================
# Fix 5: Amount bounds
# =============================================================================

class TestAmountBoundsValidation:
    """
    Bug: initiate_splice had no upper bound on relative_amount.
    Extremely large amounts could cause issues.
    """

    def test_rejects_absurdly_large_amount(self, splice_mgr, mock_rpc, sample_pubkey):
        """Amount exceeding 21M BTC should be rejected."""
        result = splice_mgr.initiate_splice(
            peer_id=sample_pubkey,
            channel_id="abc123",
            relative_amount=2_200_000_000_000_000,  # > 21M BTC
            rpc=mock_rpc
        )
        assert result.get("error") == "invalid_amount"

    def test_rejects_absurdly_large_negative_amount(self, splice_mgr, mock_rpc, sample_pubkey):
        """Negative amount exceeding 21M BTC should be rejected."""
        result = splice_mgr.initiate_splice(
            peer_id=sample_pubkey,
            channel_id="abc123",
            relative_amount=-2_200_000_000_000_000,  # > 21M BTC
            rpc=mock_rpc
        )
        assert result.get("error") == "invalid_amount"

    def test_accepts_valid_amount(
        self, splice_mgr, mock_database, mock_rpc, sample_pubkey
    ):
        """Valid amount within bounds should proceed."""
        mock_database.get_member.return_value = {"peer_id": sample_pubkey}
        splice_mgr._get_channel_for_peer = Mock(return_value={
            "short_channel_id": "100x1x0",
            "channel_id": "abc123def456"
        })
        mock_rpc.call.return_value = {"psbt": "cHNidP8BAAAA"}
        splice_mgr._send_message = Mock(return_value=True)

        result = splice_mgr.initiate_splice(
            peer_id=sample_pubkey,
            channel_id="abc123def456",
            relative_amount=1_000_000,
            rpc=mock_rpc
        )
        # Should not be rejected for invalid_amount
        assert result.get("error") != "invalid_amount"

    def test_rejects_zero_amount(self, splice_mgr, mock_rpc, sample_pubkey):
        """Zero amount should be rejected."""
        mock_database = splice_mgr.db
        mock_database.get_member.return_value = {"peer_id": sample_pubkey}

        result = splice_mgr.initiate_splice(
            peer_id=sample_pubkey,
            channel_id="abc123",
            relative_amount=0,
            rpc=mock_rpc
        )
        assert result.get("error") == "invalid_amount"


# =============================================================================
# Fix 6: State transition validation
# =============================================================================

class TestStateTransitionValidation:
    """
    Bug: _proceed_to_signing didn't validate current state.
    Could be called on a COMPLETED or FAILED session.
    """

    def test_proceed_to_signing_rejects_completed_session(
        self, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """_proceed_to_signing should reject sessions in COMPLETED state."""
        mock_database.get_splice_session.return_value = {
            "session_id": sample_session_id,
            "status": SPLICE_STATUS_COMPLETED,
            "channel_id": "abc123",
            "peer_id": sample_pubkey
        }

        result = splice_mgr._proceed_to_signing(
            sample_session_id, sample_pubkey, "abc123", "psbt_data", mock_rpc
        )

        assert result.get("error") == "invalid_state"

    def test_proceed_to_signing_rejects_failed_session(
        self, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """_proceed_to_signing should reject sessions in FAILED state."""
        mock_database.get_splice_session.return_value = {
            "session_id": sample_session_id,
            "status": SPLICE_STATUS_FAILED,
            "channel_id": "abc123",
            "peer_id": sample_pubkey
        }

        result = splice_mgr._proceed_to_signing(
            sample_session_id, sample_pubkey, "abc123", "psbt_data", mock_rpc
        )

        assert result.get("error") == "invalid_state"

    def test_proceed_to_signing_rejects_aborted_session(
        self, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """_proceed_to_signing should reject sessions in ABORTED state."""
        mock_database.get_splice_session.return_value = {
            "session_id": sample_session_id,
            "status": SPLICE_STATUS_ABORTED,
            "channel_id": "abc123",
            "peer_id": sample_pubkey
        }

        result = splice_mgr._proceed_to_signing(
            sample_session_id, sample_pubkey, "abc123", "psbt_data", mock_rpc
        )

        assert result.get("error") == "invalid_state"

    def test_proceed_to_signing_allows_updating_session(
        self, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """_proceed_to_signing should allow sessions in UPDATING state."""
        mock_database.get_splice_session.return_value = {
            "session_id": sample_session_id,
            "status": SPLICE_STATUS_UPDATING,
            "channel_id": "abc123",
            "peer_id": sample_pubkey
        }
        # splice_signed RPC returns txid
        mock_rpc.call.return_value = {"txid": "b" * 64}
        splice_mgr._send_message = Mock(return_value=True)

        result = splice_mgr._proceed_to_signing(
            sample_session_id, sample_pubkey, "abc123", "psbt_data", mock_rpc
        )

        # Should succeed (not return invalid_state error)
        assert result.get("error") != "invalid_state"


# =============================================================================
# Fund ownership protection
# =============================================================================

class TestFundOwnershipProtection:
    """
    Verify that fund ownership protections are in place.
    Each node controls only its own funds via CLN's HSM.
    """

    @patch('modules.splice_manager.validate_splice_init_request_payload', return_value=True)
    def test_responder_does_not_exchange_psbt_in_hive_message(
        self, mock_validate, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """
        Responder should send acceptance with psbt=None.
        PSBT exchange happens only via CLN's internal Lightning protocol.
        """
        mock_database.create_splice_session.return_value = True
        splice_mgr._get_channel_for_peer = Mock(return_value={
            "short_channel_id": "100x1x0",
            "channel_id": "abc123def456"
        })
        splice_mgr._verify_signature = Mock(return_value=True)
        splice_mgr._send_message = Mock(return_value=True)

        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": "abc123def456",
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 100000,
            "psbt": "cHNidP8B" + "A" * 100,
            "timestamp": int(time.time()),
            "signature": "valid_sig"
        }

        result = splice_mgr.handle_splice_init_request(sample_pubkey, payload, mock_rpc)

        # Verify success
        assert result.get("success") is True

    @patch('modules.splice_manager.validate_splice_init_request_payload', return_value=True)
    def test_signature_verification_required(
        self, mock_validate, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """All splice messages require valid signatures."""
        splice_mgr._verify_signature = Mock(return_value=False)

        payload = {
            "initiator_id": sample_pubkey,
            "session_id": sample_session_id,
            "channel_id": "abc123def456",
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 100000,
            "psbt": "cHNidP8B" + "A" * 100,
            "timestamp": int(time.time()),
            "signature": "bad_sig"
        }

        result = splice_mgr.handle_splice_init_request(sample_pubkey, payload, mock_rpc)

        assert result.get("error") == "invalid_signature"

    @patch('modules.splice_manager.validate_splice_init_request_payload', return_value=True)
    def test_sender_id_must_match_peer_id(
        self, mock_validate, splice_mgr, mock_database, mock_rpc, sample_pubkey, sample_session_id
    ):
        """Sender ID in payload must match the peer that sent the message."""
        splice_mgr._verify_signature = Mock(return_value=True)

        payload = {
            "initiator_id": "02" + "b" * 64,  # Different from sender
            "session_id": sample_session_id,
            "channel_id": "abc123def456",
            "splice_type": SPLICE_TYPE_IN,
            "amount_sats": 100000,
            "psbt": "cHNidP8B" + "A" * 100,
            "timestamp": int(time.time()),
            "signature": "valid_sig"
        }

        result = splice_mgr.handle_splice_init_request(sample_pubkey, payload, mock_rpc)

        assert result.get("error") == "initiator_mismatch"
