"""
Tests for Phase C: Deterministic idempotency.

Covers:
- generate_event_id() determinism and edge cases
- check_and_record() new vs duplicate detection
- Proto events cleanup
- Relay generate_msg_id() _event_id preference

Run with: pytest tests/test_idempotency.py -v
"""

import json
import time
import pytest
import sys
import os
from unittest.mock import Mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.idempotency import (
    EVENT_ID_FIELDS,
    generate_event_id,
    check_and_record,
)
from modules.database import HiveDatabase
from modules.relay import RelayManager


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


# =============================================================================
# generate_event_id() TESTS
# =============================================================================

class TestGenerateEventId:

    def test_deterministic_same_inputs(self):
        """Same inputs always produce the same event ID."""
        payload = {"peer_id": "abc", "timestamp": 1234}
        id1 = generate_event_id("MEMBER_LEFT", payload)
        id2 = generate_event_id("MEMBER_LEFT", payload)
        assert id1 == id2
        assert len(id1) == 32

    def test_different_types_different_ids(self):
        """Different message types with overlapping fields produce different IDs."""
        payload = {"target_pubkey": "abc", "request_id": "req1"}
        id_promo_req = generate_event_id("PROMOTION_REQUEST", payload)
        id_promo = generate_event_id("PROMOTION", payload)
        assert id_promo_req != id_promo

    def test_returns_none_for_untracked_type(self):
        """Returns None for message types not in EVENT_ID_FIELDS."""
        assert generate_event_id("GOSSIP", {"data": "test"}) is None
        assert generate_event_id("STATE_HASH", {"hash": "abc"}) is None

    def test_returns_none_when_fields_missing(self):
        """Returns None when required fields are absent."""
        # MEMBER_LEFT requires peer_id and timestamp
        assert generate_event_id("MEMBER_LEFT", {"peer_id": "abc"}) is None
        assert generate_event_id("MEMBER_LEFT", {"timestamp": 123}) is None
        assert generate_event_id("MEMBER_LEFT", {}) is None

    def test_extra_fields_ignored(self):
        """Extra payload fields don't affect the event ID."""
        base = {"peer_id": "abc", "timestamp": 1234}
        extra = {"peer_id": "abc", "timestamp": 1234, "reason": "goodbye", "sig": "xyz"}
        assert generate_event_id("MEMBER_LEFT", base) == generate_event_id("MEMBER_LEFT", extra)

    def test_all_tracked_types_have_fields(self):
        """Every entry in EVENT_ID_FIELDS has at least one field."""
        for event_type, fields in EVENT_ID_FIELDS.items():
            assert len(fields) > 0, f"{event_type} has no identity fields"

    def test_ban_proposal_uses_proposal_id(self):
        """BAN_PROPOSAL event ID is based on proposal_id."""
        id1 = generate_event_id("BAN_PROPOSAL", {"proposal_id": "prop1"})
        id2 = generate_event_id("BAN_PROPOSAL", {"proposal_id": "prop2"})
        assert id1 != id2

    def test_ban_vote_includes_voter(self):
        """BAN_VOTE event ID includes voter_peer_id for uniqueness."""
        id1 = generate_event_id("BAN_VOTE", {"proposal_id": "p1", "voter_peer_id": "v1"})
        id2 = generate_event_id("BAN_VOTE", {"proposal_id": "p1", "voter_peer_id": "v2"})
        assert id1 != id2

    def test_vouch_includes_all_three_fields(self):
        """VOUCH needs target, request_id, and voucher."""
        payload = {"target_pubkey": "t", "request_id": "r", "voucher_pubkey": "v"}
        eid = generate_event_id("VOUCH", payload)
        assert eid is not None
        # Missing one field
        assert generate_event_id("VOUCH", {"target_pubkey": "t", "request_id": "r"}) is None

    def test_settlement_types(self):
        """Settlement message types produce valid event IDs."""
        assert generate_event_id("SETTLEMENT_PROPOSE", {"proposal_id": "sp1"}) is not None
        assert generate_event_id("SETTLEMENT_READY", {"proposal_id": "sp1", "voter_peer_id": "v1"}) is not None
        assert generate_event_id("SETTLEMENT_EXECUTED", {"proposal_id": "sp1", "executor_peer_id": "e1"}) is not None


# =============================================================================
# check_and_record() TESTS
# =============================================================================

class TestCheckAndRecord:

    def test_new_event_returns_true(self, db):
        """First occurrence returns (True, event_id)."""
        payload = {"peer_id": "abc", "timestamp": 1234}
        is_new, event_id = check_and_record(db, "MEMBER_LEFT", payload, "abc")
        assert is_new is True
        assert event_id is not None
        assert len(event_id) == 32

    def test_duplicate_returns_false(self, db):
        """Second occurrence of same event returns (False, event_id)."""
        payload = {"peer_id": "abc", "timestamp": 1234}
        is_new1, eid1 = check_and_record(db, "MEMBER_LEFT", payload, "abc")
        is_new2, eid2 = check_and_record(db, "MEMBER_LEFT", payload, "abc")
        assert is_new1 is True
        assert is_new2 is False
        assert eid1 == eid2

    def test_untracked_type_returns_true_none(self, db):
        """Untracked message type returns (True, None)."""
        is_new, event_id = check_and_record(db, "GOSSIP", {"data": "test"}, "xyz")
        assert is_new is True
        assert event_id is None

    def test_different_events_both_new(self, db):
        """Different events are independently tracked."""
        p1 = {"peer_id": "abc", "timestamp": 1000}
        p2 = {"peer_id": "abc", "timestamp": 2000}
        is_new1, _ = check_and_record(db, "MEMBER_LEFT", p1, "abc")
        is_new2, _ = check_and_record(db, "MEMBER_LEFT", p2, "abc")
        assert is_new1 is True
        assert is_new2 is True

    def test_has_proto_event(self, db):
        """has_proto_event returns True after recording."""
        payload = {"proposal_id": "ban1"}
        _, event_id = check_and_record(db, "BAN_PROPOSAL", payload, "peer1")
        assert db.has_proto_event(event_id) is True
        assert db.has_proto_event("nonexistent") is False


# =============================================================================
# CLEANUP TESTS
# =============================================================================

class TestCleanup:

    def test_cleanup_prunes_old_events(self, db):
        """cleanup_proto_events removes entries older than threshold."""
        # Insert an event manually with old timestamp
        conn = db._get_connection()
        old_time = int(time.time()) - 60 * 86400  # 60 days ago
        conn.execute(
            "INSERT INTO proto_events (event_id, event_type, actor_id, created_at, received_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("old_event", "MEMBER_LEFT", "peer1", old_time, old_time)
        )
        # Insert a recent event
        recent_time = int(time.time())
        conn.execute(
            "INSERT INTO proto_events (event_id, event_type, actor_id, created_at, received_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("recent_event", "MEMBER_LEFT", "peer2", recent_time, recent_time)
        )

        pruned = db.cleanup_proto_events(max_age_seconds=30 * 86400)
        assert pruned == 1  # Only old event pruned

        assert db.has_proto_event("old_event") is False
        assert db.has_proto_event("recent_event") is True

    def test_cleanup_returns_zero_when_nothing_to_prune(self, db):
        """cleanup_proto_events returns 0 when no old events exist."""
        payload = {"peer_id": "abc", "timestamp": 1234}
        check_and_record(db, "MEMBER_LEFT", payload, "abc")
        pruned = db.cleanup_proto_events(max_age_seconds=30 * 86400)
        assert pruned == 0


# =============================================================================
# RELAY generate_msg_id() TESTS
# =============================================================================

class TestRelayEventIdPreference:

    @pytest.fixture
    def relay_mgr(self):
        return RelayManager(
            our_pubkey="our_pk",
            send_message=Mock(),
            get_members=Mock(return_value=[]),
        )

    def test_uses_event_id_when_present(self, relay_mgr):
        """generate_msg_id returns _event_id directly when present."""
        payload = {
            "peer_id": "abc",
            "timestamp": 1234,
            "_event_id": "a" * 32,
        }
        assert relay_mgr.generate_msg_id(payload) == "a" * 32

    def test_fallback_when_no_event_id(self, relay_mgr):
        """generate_msg_id hashes payload when _event_id absent."""
        payload = {"peer_id": "abc", "timestamp": 1234}
        msg_id = relay_mgr.generate_msg_id(payload)
        assert len(msg_id) == 32
        assert msg_id != "a" * 32

    def test_ignores_short_event_id(self, relay_mgr):
        """generate_msg_id ignores _event_id that's too short."""
        payload = {
            "peer_id": "abc",
            "timestamp": 1234,
            "_event_id": "tooshort",
        }
        msg_id = relay_mgr.generate_msg_id(payload)
        assert msg_id != "tooshort"

    def test_excludes_internal_fields_from_hash(self, relay_mgr):
        """_envelope_version and _event_id don't affect hash fallback."""
        payload1 = {"peer_id": "abc", "timestamp": 1234}
        payload2 = {
            "peer_id": "abc",
            "timestamp": 1234,
            "_envelope_version": 2,
            "_event_id": "not32chars",  # won't be used (too short)
        }
        # Both should produce the same hash because internal fields are excluded
        assert relay_mgr.generate_msg_id(payload1) == relay_mgr.generate_msg_id(payload2)
