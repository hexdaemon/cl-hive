"""
Tests for Phase D: Reliable Delivery (Outbox + MSG_ACK).

Covers:
- OutboxManager: enqueue, retry, ack, implicit ack, expiry, cleanup, backpressure
- Database outbox methods: CRUD operations on proto_outbox table
- MSG_ACK protocol: create, validate, serialize/deserialize round-trip
- Exponential backoff calculation

Run with: pytest tests/test_outbox.py -v
"""

import json
import time
import pytest
import sys
import os
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.database import HiveDatabase
from modules.outbox import OutboxManager
from modules.protocol import (
    HiveMessageType,
    RELIABLE_MESSAGE_TYPES,
    IMPLICIT_ACK_MAP,
    IMPLICIT_ACK_MATCH_FIELD,
    VALID_ACK_STATUSES,
    create_msg_ack,
    validate_msg_ack,
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
    """Track send calls."""
    return []


@pytest.fixture
def send_fn(send_log):
    """Mock send function that records calls and succeeds."""
    def _send(peer_id, msg_bytes):
        send_log.append({"peer_id": peer_id, "msg_bytes": msg_bytes})
        return True
    return _send


@pytest.fixture
def failing_send_fn():
    """Mock send function that always fails."""
    def _send(peer_id, msg_bytes):
        return False
    return _send


@pytest.fixture
def members():
    """Mock member list."""
    return ["peer_a" * 4, "peer_b" * 4, "peer_c" * 4]


@pytest.fixture
def get_members_fn(members):
    return lambda: members


@pytest.fixture
def outbox(db, send_fn, get_members_fn):
    return OutboxManager(
        database=db,
        send_fn=send_fn,
        get_members_fn=get_members_fn,
        our_pubkey="our_pub" * 4,
        log_fn=lambda msg, level='info': None,
    )


@pytest.fixture
def outbox_failing(db, failing_send_fn, get_members_fn):
    return OutboxManager(
        database=db,
        send_fn=failing_send_fn,
        get_members_fn=get_members_fn,
        our_pubkey="our_pub" * 4,
        log_fn=lambda msg, level='info': None,
    )


# =============================================================================
# MSG_ACK PROTOCOL TESTS
# =============================================================================

class TestMsgAckProtocol:

    def test_create_msg_ack(self):
        """create_msg_ack returns valid serialized bytes."""
        ack = create_msg_ack("abc123", "ok", "sender_pub")
        assert ack[:4] == b'HIVE'
        msg_type, payload = deserialize(ack)
        assert msg_type == HiveMessageType.MSG_ACK
        assert payload["ack_msg_id"] == "abc123"
        assert payload["status"] == "ok"
        assert payload["sender_id"] == "sender_pub"
        assert "timestamp" in payload

    def test_create_msg_ack_statuses(self):
        """All valid statuses are accepted."""
        for status in VALID_ACK_STATUSES:
            ack = create_msg_ack("msg1", status, "pub1")
            _, payload = deserialize(ack)
            assert payload["status"] == status

    def test_create_msg_ack_invalid_status_defaults_ok(self):
        """Invalid status defaults to 'ok'."""
        ack = create_msg_ack("msg1", "bogus", "pub1")
        _, payload = deserialize(ack)
        assert payload["status"] == "ok"

    def test_validate_msg_ack_valid(self):
        """Valid MSG_ACK payload passes validation."""
        payload = {
            "ack_msg_id": "abcdef1234567890",
            "status": "ok",
            "sender_id": "03" + "aa" * 32,
            "timestamp": int(time.time()),
        }
        assert validate_msg_ack(payload) is True

    def test_validate_msg_ack_missing_fields(self):
        """Missing required fields fail validation."""
        assert validate_msg_ack({}) is False
        assert validate_msg_ack({"ack_msg_id": "abc"}) is False
        assert validate_msg_ack({"ack_msg_id": "abc", "status": "ok"}) is False

    def test_validate_msg_ack_invalid_status(self):
        """Invalid status fails validation."""
        payload = {
            "ack_msg_id": "abc",
            "status": "bogus",
            "sender_id": "pub1",
            "timestamp": 123,
        }
        assert validate_msg_ack(payload) is False

    def test_validate_msg_ack_empty_ack_id(self):
        """Empty ack_msg_id fails validation."""
        payload = {
            "ack_msg_id": "",
            "status": "ok",
            "sender_id": "pub1",
            "timestamp": 123,
        }
        assert validate_msg_ack(payload) is False

    def test_msg_ack_round_trip(self):
        """MSG_ACK serialization round-trip."""
        original_ack_id = "deadbeef" * 4
        ack_bytes = create_msg_ack(original_ack_id, "ok", "sender123")
        msg_type, payload = deserialize(ack_bytes)
        assert msg_type == HiveMessageType.MSG_ACK
        assert payload["ack_msg_id"] == original_ack_id
        assert payload["status"] == "ok"

    def test_msg_ack_odd_number(self):
        """MSG_ACK uses odd message number (safe ignore by non-Hive nodes)."""
        assert HiveMessageType.MSG_ACK % 2 == 1

    def test_msg_ack_in_valid_range(self):
        """MSG_ACK is in the experimental message range."""
        assert HiveMessageType.MSG_ACK >= 32768


# =============================================================================
# RELIABLE MESSAGE TYPES TESTS
# =============================================================================

class TestReliableMessageTypes:

    def test_reliable_types_are_frozenset(self):
        assert isinstance(RELIABLE_MESSAGE_TYPES, frozenset)

    def test_settlement_types_included(self):
        assert HiveMessageType.SETTLEMENT_PROPOSE in RELIABLE_MESSAGE_TYPES
        assert HiveMessageType.SETTLEMENT_READY in RELIABLE_MESSAGE_TYPES
        assert HiveMessageType.SETTLEMENT_EXECUTED in RELIABLE_MESSAGE_TYPES

    def test_governance_types_included(self):
        assert HiveMessageType.BAN_PROPOSAL in RELIABLE_MESSAGE_TYPES
        assert HiveMessageType.BAN_VOTE in RELIABLE_MESSAGE_TYPES
        assert HiveMessageType.MEMBER_LEFT in RELIABLE_MESSAGE_TYPES

    def test_splice_types_included(self):
        assert HiveMessageType.SPLICE_INIT_REQUEST in RELIABLE_MESSAGE_TYPES
        assert HiveMessageType.SPLICE_ABORT in RELIABLE_MESSAGE_TYPES

    def test_gossip_not_included(self):
        """Gossip messages are NOT reliable (overwrite-based)."""
        assert HiveMessageType.GOSSIP not in RELIABLE_MESSAGE_TYPES
        assert HiveMessageType.STATE_HASH not in RELIABLE_MESSAGE_TYPES

    def test_implicit_ack_map_keys_are_valid(self):
        """All implicit ack response types should be valid message types."""
        for response_type, request_type in IMPLICIT_ACK_MAP.items():
            assert isinstance(response_type, HiveMessageType)
            assert isinstance(request_type, HiveMessageType)

    def test_implicit_ack_match_fields_exist(self):
        """Every IMPLICIT_ACK_MAP entry has a corresponding match field."""
        for response_type in IMPLICIT_ACK_MAP:
            assert response_type in IMPLICIT_ACK_MATCH_FIELD


# =============================================================================
# DATABASE OUTBOX TESTS
# =============================================================================

class TestDatabaseOutbox:

    def test_enqueue_outbox(self, db):
        """Basic enqueue creates a row."""
        now = int(time.time())
        result = db.enqueue_outbox("msg1", "peer1", 32847, '{"key":"val"}', now + 3600)
        assert result is True

    def test_enqueue_outbox_idempotent(self, db):
        """Same msg_id+peer_id is silently ignored (INSERT OR IGNORE)."""
        now = int(time.time())
        assert db.enqueue_outbox("msg1", "peer1", 32847, '{"k":"v"}', now + 3600) is True
        assert db.enqueue_outbox("msg1", "peer1", 32847, '{"k":"v2"}', now + 7200) is False

    def test_enqueue_different_peers(self, db):
        """Same msg_id can be enqueued for different peers."""
        now = int(time.time())
        assert db.enqueue_outbox("msg1", "peer1", 32847, '{}', now + 3600) is True
        assert db.enqueue_outbox("msg1", "peer2", 32847, '{}', now + 3600) is True

    def test_get_outbox_pending(self, db):
        """get_outbox_pending returns queued entries ready for retry."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer1", 32847, '{"test":1}', now + 3600)
        pending = db.get_outbox_pending()
        assert len(pending) == 1
        assert pending[0]["msg_id"] == "msg1"
        assert pending[0]["status"] == "queued"
        assert pending[0]["retry_count"] == 0

    def test_get_outbox_pending_respects_next_retry(self, db):
        """Entries with future next_retry_at are not returned."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer1", 32847, '{}', now + 3600)
        # Update to have future retry
        conn = db._get_connection()
        conn.execute(
            "UPDATE proto_outbox SET next_retry_at = ? WHERE msg_id = ?",
            (now + 9999, "msg1")
        )
        pending = db.get_outbox_pending()
        assert len(pending) == 0

    def test_get_outbox_pending_excludes_expired(self, db):
        """Expired entries are not returned."""
        past = int(time.time()) - 100
        db.enqueue_outbox("msg1", "peer1", 32847, '{}', past)
        pending = db.get_outbox_pending()
        assert len(pending) == 0

    def test_update_outbox_sent(self, db):
        """update_outbox_sent updates status and retry_count."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer1", 32847, '{}', now + 3600)
        result = db.update_outbox_sent("msg1", "peer1", now + 60)
        assert result is True

        # Verify state
        pending = db.get_outbox_pending()
        # Should not be returned yet (next_retry_at is in the future)
        assert len(pending) == 0

    def test_ack_outbox(self, db):
        """ack_outbox marks entry as acked."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer1", 32847, '{}', now + 3600)
        result = db.ack_outbox("msg1", "peer1")
        assert result is True

        # Should no longer appear in pending
        pending = db.get_outbox_pending()
        assert len(pending) == 0

    def test_ack_outbox_nonexistent(self, db):
        """Acking nonexistent entry returns False."""
        result = db.ack_outbox("nonexistent", "peer1")
        assert result is False

    def test_ack_outbox_by_type(self, db):
        """ack_outbox_by_type clears entries matching payload field."""
        now = int(time.time())
        payload = json.dumps({"proposal_id": "prop123", "data_hash": "abc"})
        db.enqueue_outbox("msg1", "peer1", int(HiveMessageType.SETTLEMENT_PROPOSE),
                         payload, now + 3600)
        db.enqueue_outbox("msg2", "peer1", int(HiveMessageType.SETTLEMENT_PROPOSE),
                         json.dumps({"proposal_id": "prop456"}), now + 3600)

        # Ack by type for proposal_id=prop123
        count = db.ack_outbox_by_type(
            "peer1",
            int(HiveMessageType.SETTLEMENT_PROPOSE),
            "proposal_id",
            "prop123"
        )
        assert count == 1

        # prop456 should still be pending
        pending = db.get_outbox_pending()
        assert len(pending) == 1
        assert pending[0]["msg_id"] == "msg2"

    def test_fail_outbox(self, db):
        """fail_outbox marks entry as failed."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer1", 32847, '{}', now + 3600)
        result = db.fail_outbox("msg1", "peer1", "test error")
        assert result is True
        pending = db.get_outbox_pending()
        assert len(pending) == 0

    def test_expire_outbox(self, db):
        """expire_outbox marks expired entries."""
        past = int(time.time()) - 100
        # Manually insert an entry with past expiry but still queued
        conn = db._get_connection()
        conn.execute(
            """INSERT INTO proto_outbox
               (msg_id, peer_id, msg_type, payload_json, status,
                created_at, next_retry_at, expires_at)
               VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)""",
            ("msg1", "peer1", 32847, '{}', past - 1000, past - 500, past)
        )
        count = db.expire_outbox()
        assert count == 1

    def test_cleanup_outbox(self, db):
        """cleanup_outbox removes old terminal entries."""
        now = int(time.time())
        old = now - 8 * 86400  # 8 days ago
        conn = db._get_connection()
        # Insert old acked entry
        conn.execute(
            """INSERT INTO proto_outbox
               (msg_id, peer_id, msg_type, payload_json, status,
                created_at, next_retry_at, expires_at, acked_at)
               VALUES (?, ?, ?, ?, 'acked', ?, ?, ?, ?)""",
            ("msg1", "peer1", 32847, '{}', old, old, old + 3600, old + 100)
        )
        count = db.cleanup_outbox()
        assert count == 1

    def test_cleanup_outbox_keeps_recent(self, db):
        """cleanup_outbox keeps recent terminal entries."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer1", 32847, '{}', now + 3600)
        db.ack_outbox("msg1", "peer1")
        count = db.cleanup_outbox()
        assert count == 0  # Too recent to clean

    def test_count_inflight_for_peer(self, db):
        """count_inflight_for_peer counts queued+sent entries."""
        now = int(time.time())
        db.enqueue_outbox("msg1", "peer1", 32847, '{}', now + 3600)
        db.enqueue_outbox("msg2", "peer1", 32847, '{}', now + 3600)
        db.enqueue_outbox("msg3", "peer2", 32847, '{}', now + 3600)
        assert db.count_inflight_for_peer("peer1") == 2
        assert db.count_inflight_for_peer("peer2") == 1
        assert db.count_inflight_for_peer("peer3") == 0


# =============================================================================
# OUTBOX MANAGER TESTS
# =============================================================================

class TestOutboxManagerEnqueue:

    def test_enqueue_broadcast(self, outbox, db, members):
        """Broadcast enqueue creates per-peer rows."""
        payload = {"proposal_id": "prop1", "data_hash": "abc"}
        count = outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE, payload)
        assert count == 3  # 3 members

    def test_enqueue_excludes_self(self, db, send_fn):
        """Enqueue excludes our own pubkey."""
        our_pub = "peer_a" * 4
        mgr = OutboxManager(
            database=db,
            send_fn=send_fn,
            get_members_fn=lambda: [our_pub, "peer_b" * 4],
            our_pubkey=our_pub,
            log_fn=lambda msg, level='info': None,
        )
        count = mgr.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE, {"k": "v"})
        assert count == 1  # Only peer_b

    def test_enqueue_unicast(self, outbox, db):
        """Unicast enqueue creates single row."""
        count = outbox.enqueue("msg1", HiveMessageType.TASK_REQUEST,
                              {"request_id": "r1"}, peer_ids=["target_peer"])
        assert count == 1

    def test_enqueue_idempotent(self, outbox, db, members):
        """Duplicate enqueue (same msg_id+peer_id) is idempotent."""
        payload = {"proposal_id": "prop1"}
        count1 = outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE, payload)
        count2 = outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE, payload)
        assert count1 == 3
        assert count2 == 0  # All duplicates

    def test_enqueue_backpressure(self, db, send_fn):
        """Enqueue rejects when peer has MAX_INFLIGHT_PER_PEER entries."""
        mgr = OutboxManager(
            database=db,
            send_fn=send_fn,
            get_members_fn=lambda: ["target"],
            our_pubkey="self",
            log_fn=lambda msg, level='info': None,
        )
        # Fill up the inflight limit
        for i in range(mgr.MAX_INFLIGHT_PER_PEER):
            mgr.enqueue(f"msg{i}", HiveMessageType.SETTLEMENT_PROPOSE,
                       {"proposal_id": f"p{i}"}, peer_ids=["target"])

        # Next enqueue should be dropped
        count = mgr.enqueue("msg_overflow", HiveMessageType.SETTLEMENT_PROPOSE,
                           {"proposal_id": "overflow"}, peer_ids=["target"])
        assert count == 0

    def test_enqueue_empty_members(self, db, send_fn):
        """Enqueue with no members returns 0."""
        mgr = OutboxManager(
            database=db,
            send_fn=send_fn,
            get_members_fn=lambda: [],
            our_pubkey="self",
            log_fn=lambda msg, level='info': None,
        )
        count = mgr.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE, {"k": "v"})
        assert count == 0


class TestOutboxManagerRetry:

    def test_retry_pending_sends_messages(self, outbox, db, send_log, members):
        """retry_pending sends queued messages."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "p1"})
        stats = outbox.retry_pending()
        assert stats["sent"] == 3  # One per member
        assert len(send_log) == 3

    def test_retry_pending_updates_retry_count(self, outbox, db, members):
        """After retry, retry_count increments."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "p1"})
        outbox.retry_pending()

        # Entries should now have next_retry_at in the future
        pending = db.get_outbox_pending()
        assert len(pending) == 0  # All have future next_retry_at

    def test_retry_max_retries_fails(self, db, send_fn):
        """Messages exceeding MAX_RETRIES are marked failed."""
        mgr = OutboxManager(
            database=db,
            send_fn=send_fn,
            get_members_fn=lambda: ["target"],
            our_pubkey="self",
            log_fn=lambda msg, level='info': None,
        )
        mgr.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                    {"proposal_id": "p1"}, peer_ids=["target"])

        # Manually set retry_count to MAX_RETRIES
        conn = db._get_connection()
        conn.execute(
            "UPDATE proto_outbox SET retry_count = ? WHERE msg_id = ?",
            (mgr.MAX_RETRIES, "msg1")
        )
        stats = mgr.retry_pending()
        assert stats["failed"] == 1

    def test_retry_with_failing_send(self, outbox_failing, db, members):
        """Failed sends still update next_retry_at (don't get stuck)."""
        outbox_failing.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                              {"proposal_id": "p1"})
        stats = outbox_failing.retry_pending()
        assert stats["skipped"] == 3  # All failed sends


class TestOutboxManagerAck:

    def test_process_ack_ok(self, outbox, db, members):
        """process_ack with 'ok' marks entry as acked."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "p1"})
        result = outbox.process_ack(members[0], "msg1", "ok")
        assert result is True
        assert db.count_inflight_for_peer(members[0]) == 0

    def test_process_ack_invalid(self, outbox, db, members):
        """process_ack with 'invalid' marks entry as failed."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "p1"})
        result = outbox.process_ack(members[0], "msg1", "invalid")
        assert result is True
        assert db.count_inflight_for_peer(members[0]) == 0

    def test_process_ack_retry_later(self, outbox, db, members):
        """process_ack with 'retry_later' leaves entry as-is."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "p1"})
        result = outbox.process_ack(members[0], "msg1", "retry_later")
        assert result is False  # No state change
        assert db.count_inflight_for_peer(members[0]) == 1  # Still inflight


class TestOutboxManagerImplicitAck:

    def test_implicit_ack_settlement_ready(self, outbox, db, members):
        """SETTLEMENT_READY implicitly acks SETTLEMENT_PROPOSE."""
        # Enqueue a SETTLEMENT_PROPOSE
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "prop123"})

        # Simulate receiving SETTLEMENT_READY from a peer
        response_payload = {"proposal_id": "prop123", "voter_peer_id": members[0]}
        count = outbox.process_implicit_ack(
            members[0], HiveMessageType.SETTLEMENT_READY, response_payload
        )
        assert count == 1
        assert db.count_inflight_for_peer(members[0]) == 0

    def test_implicit_ack_task_response(self, outbox, db):
        """TASK_RESPONSE implicitly acks TASK_REQUEST."""
        outbox.enqueue("msg1", HiveMessageType.TASK_REQUEST,
                      {"request_id": "req1"}, peer_ids=["target"])

        count = outbox.process_implicit_ack(
            "target", HiveMessageType.TASK_RESPONSE,
            {"request_id": "req1", "responder_id": "target"}
        )
        assert count == 1

    def test_implicit_ack_ban_vote(self, outbox, db, members):
        """BAN_VOTE implicitly acks BAN_PROPOSAL."""
        outbox.enqueue("msg1", HiveMessageType.BAN_PROPOSAL,
                      {"proposal_id": "ban123"})

        count = outbox.process_implicit_ack(
            members[0], HiveMessageType.BAN_VOTE,
            {"proposal_id": "ban123", "voter_peer_id": members[0]}
        )
        assert count == 1

    def test_implicit_ack_vouch(self, outbox, db, members):
        """VOUCH implicitly acks PROMOTION_REQUEST."""
        outbox.enqueue("msg1", HiveMessageType.PROMOTION_REQUEST,
                      {"request_id": "prom1", "target_pubkey": "neo1"})

        count = outbox.process_implicit_ack(
            members[0], HiveMessageType.VOUCH,
            {"request_id": "prom1", "voucher_pubkey": members[0]}
        )
        assert count == 1

    def test_implicit_ack_splice_init_response(self, outbox, db):
        """SPLICE_INIT_RESPONSE implicitly acks SPLICE_INIT_REQUEST."""
        outbox.enqueue("msg1", HiveMessageType.SPLICE_INIT_REQUEST,
                      {"session_id": "splice1"}, peer_ids=["target"])

        count = outbox.process_implicit_ack(
            "target", HiveMessageType.SPLICE_INIT_RESPONSE,
            {"session_id": "splice1", "accepted": True}
        )
        assert count == 1

    def test_implicit_ack_no_match(self, outbox, db, members):
        """Implicit ack with non-matching field has no effect."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "prop123"})

        count = outbox.process_implicit_ack(
            members[0], HiveMessageType.SETTLEMENT_READY,
            {"proposal_id": "other_prop"}
        )
        assert count == 0
        assert db.count_inflight_for_peer(members[0]) == 1

    def test_implicit_ack_unrelated_type(self, outbox, db, members):
        """Unrelated message type has no implicit ack mapping."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "prop123"})

        count = outbox.process_implicit_ack(
            members[0], HiveMessageType.GOSSIP, {"whatever": "value"}
        )
        assert count == 0


class TestOutboxManagerExpiry:

    def test_expire_and_cleanup(self, outbox, db):
        """expire_and_cleanup processes expired entries."""
        now = int(time.time())
        # Insert an entry that's already expired
        conn = db._get_connection()
        conn.execute(
            """INSERT INTO proto_outbox
               (msg_id, peer_id, msg_type, payload_json, status,
                created_at, next_retry_at, expires_at)
               VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)""",
            ("msg1", "peer1", 32847, '{}', now - 1000, now - 500, now - 1)
        )

        stats = outbox.expire_and_cleanup()
        assert stats["expired"] == 1


class TestOutboxManagerBackoff:

    def test_backoff_increases(self, outbox):
        """Backoff delay increases with retry count."""
        delays = []
        for i in range(5):
            next_retry = outbox._calculate_next_retry(i)
            delays.append(next_retry - int(time.time()))

        # Each delay should be roughly double the previous (with jitter)
        for i in range(1, len(delays)):
            # Allow for jitter but check the general trend
            assert delays[i] >= delays[i - 1] * 0.8  # At least 80% of doubling

    def test_backoff_cap(self, outbox):
        """Backoff is capped at MAX_RETRY_SECONDS."""
        next_retry = outbox._calculate_next_retry(100)
        delay = next_retry - int(time.time())
        # Should be at most MAX_RETRY_SECONDS + 25% jitter
        assert delay <= outbox.MAX_RETRY_SECONDS * 1.26

    def test_backoff_base(self, outbox):
        """First retry is approximately BASE_RETRY_SECONDS."""
        next_retry = outbox._calculate_next_retry(0)
        delay = next_retry - int(time.time())
        assert delay >= outbox.BASE_RETRY_SECONDS
        assert delay <= outbox.BASE_RETRY_SECONDS * 1.30  # 25% jitter + int() rounding


class TestOutboxManagerStats:

    def test_stats_empty(self, outbox):
        """Stats on empty outbox."""
        stats = outbox.stats()
        assert stats["pending_count"] == 0

    def test_stats_with_pending(self, outbox, members):
        """Stats reflect pending messages."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "p1"})
        stats = outbox.stats()
        assert stats["pending_count"] == 3


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestOutboxIntegration:

    def test_full_lifecycle(self, outbox, db, send_log, members):
        """Test complete lifecycle: enqueue -> retry -> ack."""
        # 1. Enqueue
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "p1"})
        assert db.count_inflight_for_peer(members[0]) == 1

        # 2. Retry (sends the message)
        stats = outbox.retry_pending()
        assert stats["sent"] == 3

        # 3. Ack from one peer
        outbox.process_ack(members[0], "msg1", "ok")
        assert db.count_inflight_for_peer(members[0]) == 0
        assert db.count_inflight_for_peer(members[1]) == 1  # Still inflight

        # 4. Ack from remaining peers
        outbox.process_ack(members[1], "msg1", "ok")
        outbox.process_ack(members[2], "msg1", "ok")
        assert db.count_inflight_for_peer(members[1]) == 0
        assert db.count_inflight_for_peer(members[2]) == 0

    def test_implicit_ack_lifecycle(self, outbox, db, send_log, members):
        """Enqueue SETTLEMENT_PROPOSE, implicitly acked by SETTLEMENT_READY."""
        outbox.enqueue("msg1", HiveMessageType.SETTLEMENT_PROPOSE,
                      {"proposal_id": "p1"})

        # Retry to send
        outbox.retry_pending()

        # Peer responds with SETTLEMENT_READY (implicit ack)
        outbox.process_implicit_ack(
            members[0], HiveMessageType.SETTLEMENT_READY,
            {"proposal_id": "p1", "voter_peer_id": members[0]}
        )
        assert db.count_inflight_for_peer(members[0]) == 0
        # Others still pending
        assert db.count_inflight_for_peer(members[1]) == 1

    def test_old_node_ignores_msg_ack(self):
        """MSG_ACK (32881) is odd -> unknown types are safely ignored by old nodes.
        This test verifies that deserialize handles MSG_ACK correctly for new nodes
        while old nodes would see it as an unknown type and skip in their dispatch."""
        ack_bytes = create_msg_ack("test_id", "ok", "sender")
        msg_type, payload = deserialize(ack_bytes)
        # New node can parse it
        assert msg_type == HiveMessageType.MSG_ACK
        # Old node dispatch would hit the 'else' branch and log "Unhandled message type"
