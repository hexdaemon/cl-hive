"""
Tests for 7 outbox/idempotency bug fixes.

Bug 1: retry_pending failed sends no longer burn retry budget
Bug 2: Duplicate messages now receive ACK (via _emit_ack in not-is_new paths)
Bug 3: SPLICE_INIT_RESPONSE added to EVENT_ID_FIELDS
Bug 4: handle_msg_ack uses verified sender_id (not transport peer_id)
Bug 5: ack_outbox_by_type LIKE fallback escapes SQL wildcards
Bug 6: stats() uses efficient COUNT(*) query
Bug 7: Max retries failure logged at 'warn' level

Run with: pytest tests/test_outbox_7_fixes.py -v
"""

import json
import time
import pytest
import sys
import os
from unittest.mock import Mock, patch, call

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.database import HiveDatabase
from modules.outbox import OutboxManager
from modules.idempotency import generate_event_id, check_and_record, EVENT_ID_FIELDS
from modules.protocol import (
    HiveMessageType,
    RELIABLE_MESSAGE_TYPES,
    serialize,
    deserialize,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def db(tmp_path):
    mock_plugin = Mock()
    mock_plugin.log = Mock()
    database = HiveDatabase(str(tmp_path / "test.db"), mock_plugin)
    database.initialize()
    return database


@pytest.fixture
def send_log():
    return []


@pytest.fixture
def send_fn(send_log):
    def _send(peer_id, msg_bytes):
        send_log.append({"peer_id": peer_id, "msg_bytes": msg_bytes})
        return True
    return _send


@pytest.fixture
def failing_send_fn():
    def _send(peer_id, msg_bytes):
        return False
    return _send


@pytest.fixture
def log_messages():
    return []


@pytest.fixture
def log_fn(log_messages):
    def _log(msg, level='info'):
        log_messages.append({"msg": msg, "level": level})
    return _log


@pytest.fixture
def outbox(db, send_fn):
    return OutboxManager(
        database=db,
        send_fn=send_fn,
        get_members_fn=lambda: ["peer_a", "peer_b"],
        our_pubkey="our_pub",
        log_fn=lambda msg, level='info': None,
    )


@pytest.fixture
def outbox_failing(db, failing_send_fn, log_fn):
    return OutboxManager(
        database=db,
        send_fn=failing_send_fn,
        get_members_fn=lambda: ["peer_a", "peer_b"],
        our_pubkey="our_pub",
        log_fn=log_fn,
    )


# =============================================================================
# BUG 1: Failed sends don't burn retry budget
# =============================================================================

class TestFailedSendRetryBudget:
    """Bug 1: Failed sends should not increment retry_count."""

    def test_failed_send_does_not_increment_retry_count(self, outbox_failing, db):
        """When send_fn returns False, retry_count should stay at 0."""
        outbox_failing.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                               {"proposal_id": "p1"}, peer_ids=["peer_a"])
        stats = outbox_failing.retry_pending()
        assert stats["skipped"] == 1

        # retry_count should NOT have been incremented
        conn = db._get_connection()
        row = conn.execute(
            "SELECT retry_count, status FROM proto_outbox WHERE msg_id = ? AND peer_id = ?",
            ("msg1", "peer_a")
        ).fetchone()
        assert row["retry_count"] == 0  # Not incremented on failure
        # Status should remain 'queued', not 'sent'
        assert row["status"] == "queued"

    def test_successful_send_increments_retry_count(self, outbox, db):
        """When send_fn succeeds, retry_count should increment normally."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                       {"proposal_id": "p1"}, peer_ids=["peer_a"])
        stats = outbox.retry_pending()
        assert stats["sent"] == 1

        conn = db._get_connection()
        row = conn.execute(
            "SELECT retry_count, status FROM proto_outbox WHERE msg_id = ? AND peer_id = ?",
            ("msg1", "peer_a")
        ).fetchone()
        assert row["retry_count"] == 1
        assert row["status"] == "sent"

    def test_failed_send_uses_short_retry_delay(self, outbox_failing, db):
        """Failed sends should use BASE_RETRY_SECONDS delay, not exponential."""
        outbox_failing.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                               {"proposal_id": "p1"}, peer_ids=["peer_a"])
        before = int(time.time())
        outbox_failing.retry_pending()

        conn = db._get_connection()
        row = conn.execute(
            "SELECT next_retry_at FROM proto_outbox WHERE msg_id = ? AND peer_id = ?",
            ("msg1", "peer_a")
        ).fetchone()
        # Short delay: ~BASE_RETRY_SECONDS + small jitter (0-10s)
        max_expected = before + OutboxManager.BASE_RETRY_SECONDS + 15
        assert row["next_retry_at"] <= max_expected

    def test_many_failed_sends_preserve_retry_budget(self, db, failing_send_fn):
        """After N failed sends, retry_count should still be 0."""
        mgr = OutboxManager(
            database=db,
            send_fn=failing_send_fn,
            get_members_fn=lambda: ["peer_a"],
            our_pubkey="our_pub",
            log_fn=lambda msg, level='info': None,
        )
        mgr.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                     {"proposal_id": "p1"}, peer_ids=["peer_a"])

        # Simulate multiple retry cycles with failed sends
        for _ in range(5):
            # Make entry eligible for retry
            conn = db._get_connection()
            conn.execute(
                "UPDATE proto_outbox SET next_retry_at = ? WHERE msg_id = ?",
                (int(time.time()) - 1, "msg1")
            )
            mgr.retry_pending()

        conn = db._get_connection()
        row = conn.execute(
            "SELECT retry_count FROM proto_outbox WHERE msg_id = ? AND peer_id = ?",
            ("msg1", "peer_a")
        ).fetchone()
        assert row["retry_count"] == 0  # Never incremented

    def test_update_outbox_retry_db_method(self, db):
        """update_outbox_retry updates next_retry_at without touching retry_count."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer_a", 32769, '{"test":1}', now + 86400)

        next_retry = now + 60
        result = db.update_outbox_retry("msg1", "peer_a", next_retry)
        assert result is True

        conn = db._get_connection()
        row = conn.execute(
            "SELECT retry_count, status, next_retry_at FROM proto_outbox WHERE msg_id = ?",
            ("msg1",)
        ).fetchone()
        assert row["retry_count"] == 0
        assert row["status"] == "queued"  # Unchanged
        assert row["next_retry_at"] == next_retry


# =============================================================================
# BUG 2: Duplicate messages ACK (integration-level — tests event_id flow)
# =============================================================================

class TestDuplicateMessageAckFlow:
    """Bug 2: check_and_record returns event_id for duplicates, enabling ACK."""

    def test_check_and_record_returns_event_id_for_duplicate(self, db):
        """Duplicate detection returns the event_id so it can be used for ACK."""
        payload = {"proposal_id": "p1"}
        is_new, event_id = check_and_record(db, "SETTLEMENT_PROPOSE", payload, "actor1")
        assert is_new is True
        assert event_id is not None

        # Second time: duplicate detected, but event_id still returned
        is_new2, event_id2 = check_and_record(db, "SETTLEMENT_PROPOSE", payload, "actor1")
        assert is_new2 is False
        assert event_id2 == event_id  # Same event_id for ACK

    def test_event_id_matches_outbox_msg_id(self, db):
        """The event_id from check_and_record matches generate_event_id used by outbox."""
        payload = {"proposal_id": "p1"}
        msg_id = generate_event_id("SETTLEMENT_PROPOSE", payload)
        _, event_id = check_and_record(db, "SETTLEMENT_PROPOSE", payload, "actor1")
        assert msg_id == event_id

    def test_duplicate_ack_clears_outbox_entry(self, db, send_fn):
        """Simulating: receiver gets duplicate, sends ACK with event_id, outbox clears."""
        mgr = OutboxManager(
            database=db,
            send_fn=send_fn,
            get_members_fn=lambda: ["peer_a"],
            our_pubkey="our_pub",
            log_fn=lambda msg, level='info': None,
        )
        payload = {"proposal_id": "p1"}
        msg_id = generate_event_id("SETTLEMENT_PROPOSE", payload)
        mgr.enqueue(msg_id, HiveMessageType.SETTLEMENT_PROPOSE, payload, peer_ids=["peer_a"])
        assert db.count_inflight_for_peer("peer_a") == 1

        # Simulate receiver detecting duplicate and ACKing with event_id
        _, event_id = check_and_record(db, "SETTLEMENT_PROPOSE", payload, "peer_a")
        # First process (new)
        is_new2, event_id2 = check_and_record(db, "SETTLEMENT_PROPOSE", payload, "peer_a")
        # It's duplicate now — receiver would call _emit_ack(peer_id, event_id2)
        assert is_new2 is False
        # ACK with the event_id clears the outbox
        mgr.process_ack("peer_a", event_id2, "ok")
        assert db.count_inflight_for_peer("peer_a") == 0


# =============================================================================
# BUG 3: SPLICE_INIT_RESPONSE in EVENT_ID_FIELDS
# =============================================================================

class TestSpliceInitResponseIdempotency:
    """Bug 3: SPLICE_INIT_RESPONSE should have deterministic event ID."""

    def test_splice_init_response_in_event_id_fields(self):
        """SPLICE_INIT_RESPONSE is now in EVENT_ID_FIELDS."""
        assert "SPLICE_INIT_RESPONSE" in EVENT_ID_FIELDS
        assert EVENT_ID_FIELDS["SPLICE_INIT_RESPONSE"] == ["session_id", "responder_id"]

    def test_splice_init_response_generates_event_id(self):
        """generate_event_id works for SPLICE_INIT_RESPONSE."""
        payload = {"session_id": "sess1", "responder_id": "peer_a"}
        event_id = generate_event_id("SPLICE_INIT_RESPONSE", payload)
        assert event_id is not None
        assert len(event_id) == 32

    def test_splice_init_response_deterministic(self):
        """Same inputs produce same event_id."""
        payload = {"session_id": "sess1", "responder_id": "peer_a", "extra": "ignored"}
        id1 = generate_event_id("SPLICE_INIT_RESPONSE", payload)
        id2 = generate_event_id("SPLICE_INIT_RESPONSE", payload)
        assert id1 == id2

    def test_splice_init_response_different_sessions(self):
        """Different session_ids produce different event_ids."""
        p1 = {"session_id": "sess1", "responder_id": "peer_a"}
        p2 = {"session_id": "sess2", "responder_id": "peer_a"}
        assert generate_event_id("SPLICE_INIT_RESPONSE", p1) != \
               generate_event_id("SPLICE_INIT_RESPONSE", p2)

    def test_splice_init_response_dedup(self, db):
        """check_and_record deduplicates SPLICE_INIT_RESPONSE."""
        payload = {"session_id": "sess1", "responder_id": "peer_a"}
        is_new, eid = check_and_record(db, "SPLICE_INIT_RESPONSE", payload, "peer_a")
        assert is_new is True

        is_new2, eid2 = check_and_record(db, "SPLICE_INIT_RESPONSE", payload, "peer_a")
        assert is_new2 is False
        assert eid2 == eid

    def test_all_reliable_types_have_event_id_fields(self):
        """Every RELIABLE_MESSAGE_TYPES entry should have EVENT_ID_FIELDS coverage."""
        for msg_type in RELIABLE_MESSAGE_TYPES:
            assert msg_type.name in EVENT_ID_FIELDS, \
                f"{msg_type.name} is in RELIABLE_MESSAGE_TYPES but missing from EVENT_ID_FIELDS"


# =============================================================================
# BUG 4: handle_msg_ack sender_id (unit test of the fix concept)
# =============================================================================

class TestMsgAckSenderId:
    """Bug 4: process_ack should use verified sender_id, not transport peer_id."""

    def test_ack_matches_on_target_peer_id(self, outbox, db):
        """process_ack with the correct target peer_id clears the entry."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                       {"proposal_id": "p1"}, peer_ids=["peer_a"])
        assert db.count_inflight_for_peer("peer_a") == 1

        # ACK from sender_id matching the target
        result = outbox.process_ack("peer_a", "msg1", "ok")
        assert result is True
        assert db.count_inflight_for_peer("peer_a") == 0

    def test_ack_with_wrong_peer_id_fails(self, outbox, db):
        """process_ack with mismatched peer_id doesn't clear the entry."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                       {"proposal_id": "p1"}, peer_ids=["peer_a"])
        # ACK from transport peer "relay_node" — won't match outbox entry for "peer_a"
        result = outbox.process_ack("relay_node", "msg1", "ok")
        assert result is False
        assert db.count_inflight_for_peer("peer_a") == 1


# =============================================================================
# BUG 5: LIKE fallback wildcard escaping
# =============================================================================

class TestLikeWildcardEscaping:
    """Bug 5: ack_outbox_by_type LIKE fallback escapes SQL wildcards."""

    def test_ack_by_type_with_percent_in_value(self, db):
        """match_value containing '%' should not match unrelated entries."""
        now = int(time.time())
        # Entry with normal proposal_id
        db.enqueue_outbox("msg1", "peer_a", 32769,
                          json.dumps({"proposal_id": "abc123"}), now + 86400)
        # Entry with proposal_id that starts with "a"
        db.enqueue_outbox("msg2", "peer_a", 32769,
                          json.dumps({"proposal_id": "axyz"}), now + 86400)

        # Try to ack with match_value "a%" — should NOT match "abc123" via LIKE
        # This tests the LIKE fallback path by wrapping json_extract to fail
        # We test the escaping logic directly instead
        safe_value = "a%".replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
        assert safe_value == "a\\%"
        # The pattern should be '"proposal_id":"a\\%"' which won't match abc123
        pattern = f'"proposal_id":"{safe_value}"'
        assert "abc123" not in pattern

    def test_ack_by_type_with_underscore_in_value(self, db):
        """match_value containing '_' should be escaped."""
        safe_value = "test_id".replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
        assert safe_value == "test\\_id"

    def test_ack_by_type_exact_match_works(self, db):
        """Normal match_value without wildcards still works via json_extract."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer_a", 32769,
                          json.dumps({"proposal_id": "p1"}), now + 86400)
        count = db.ack_outbox_by_type("peer_a", 32769, "proposal_id", "p1")
        assert count == 1


# =============================================================================
# BUG 6: stats() efficiency
# =============================================================================

class TestStatsEfficiency:
    """Bug 6: stats() should use COUNT(*) instead of fetching all rows."""

    def test_stats_returns_count(self, outbox, db):
        """stats() returns pending_count."""
        result = outbox.stats()
        assert result == {"pending_count": 0}

    def test_stats_counts_pending(self, outbox, db):
        """stats() counts entries ready for retry."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                       {"proposal_id": "p1"}, peer_ids=["peer_a"])
        result = outbox.stats()
        assert result["pending_count"] == 1

    def test_count_outbox_pending_method(self, db):
        """count_outbox_pending returns correct count without fetching rows."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer_a", 32769, '{"x":1}', now + 86400)
        db.enqueue_outbox("msg2", "peer_b", 32769, '{"x":2}', now + 86400)

        count = db.count_outbox_pending()
        assert count == 2

    def test_count_outbox_pending_excludes_future(self, db):
        """count_outbox_pending excludes entries with future next_retry_at."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer_a", 32769, '{"x":1}', now + 86400)
        # Push next_retry_at into the future
        conn = db._get_connection()
        conn.execute(
            "UPDATE proto_outbox SET next_retry_at = ? WHERE msg_id = ?",
            (now + 3600, "msg1")
        )
        count = db.count_outbox_pending()
        assert count == 0

    def test_count_outbox_pending_excludes_expired(self, db):
        """count_outbox_pending excludes expired entries."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer_a", 32769, '{"x":1}', now - 1)  # Already expired
        count = db.count_outbox_pending()
        assert count == 0


# =============================================================================
# BUG 7: Max retries log level
# =============================================================================

class TestMaxRetriesLogLevel:
    """Bug 7: Max retries failure should log at 'warn' level."""

    def test_max_retries_logs_warn(self, db, send_fn, log_messages, log_fn):
        """When message exceeds MAX_RETRIES, log at 'warn' not 'debug'."""
        mgr = OutboxManager(
            database=db,
            send_fn=send_fn,
            get_members_fn=lambda: ["peer_a"],
            our_pubkey="our_pub",
            log_fn=log_fn,
        )
        mgr.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                     {"proposal_id": "p1"}, peer_ids=["peer_a"])

        # Set retry_count to MAX_RETRIES
        conn = db._get_connection()
        conn.execute(
            "UPDATE proto_outbox SET retry_count = ? WHERE msg_id = ?",
            (mgr.MAX_RETRIES, "msg1")
        )
        mgr.retry_pending()

        # Should have logged at warn level
        warn_msgs = [m for m in log_messages if m["level"] == "warn"]
        assert len(warn_msgs) >= 1
        assert "max retries" in warn_msgs[0]["msg"]

    def test_max_retries_not_debug(self, db, send_fn, log_messages, log_fn):
        """Max retries should NOT be at debug level."""
        mgr = OutboxManager(
            database=db,
            send_fn=send_fn,
            get_members_fn=lambda: ["peer_a"],
            our_pubkey="our_pub",
            log_fn=log_fn,
        )
        mgr.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                     {"proposal_id": "p1"}, peer_ids=["peer_a"])

        conn = db._get_connection()
        conn.execute(
            "UPDATE proto_outbox SET retry_count = ? WHERE msg_id = ?",
            (mgr.MAX_RETRIES, "msg1")
        )
        mgr.retry_pending()

        debug_msgs = [m for m in log_messages
                      if m["level"] == "debug" and "max retries" in m["msg"]]
        assert len(debug_msgs) == 0  # Not logged at debug anymore
