"""
Tests for RPC command fixes from audit 2026-02-10.

Tests cover:
- M-26: create_close_actions() permission check
- reject_action() with reason parameter
- _reject_all_actions() with reason parameter
"""

import pytest
import time
import json
from unittest.mock import MagicMock
from dataclasses import dataclass

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.database import HiveDatabase
from modules.rpc_commands import (
    HiveContext,
    check_permission,
    create_close_actions,
    reject_action,
    _reject_all_actions,
)


@pytest.fixture
def mock_plugin():
    plugin = MagicMock()
    plugin.log = MagicMock()
    return plugin


@pytest.fixture
def database(mock_plugin, tmp_path):
    db_path = str(tmp_path / "test_rpc_audit.db")
    db = HiveDatabase(db_path, mock_plugin)
    db.initialize()
    return db


def _make_ctx(database, pubkey, tier='member', rationalization_mgr=None):
    """Create HiveContext with a member of the given tier."""
    now = int(time.time())
    conn = database._get_connection()

    # Ensure the member exists
    existing = conn.execute(
        "SELECT peer_id FROM hive_members WHERE peer_id = ?", (pubkey,)
    ).fetchone()
    if not existing:
        conn.execute(
            "INSERT INTO hive_members (peer_id, tier, joined_at) VALUES (?, ?, ?)",
            (pubkey, tier, now)
        )

    return HiveContext(
        database=database,
        config=MagicMock(),
        safe_plugin=MagicMock(),
        our_pubkey=pubkey,
        rationalization_mgr=rationalization_mgr,
        log=MagicMock(),
    )


class TestCreateCloseActionsPermission:
    """M-26: Test permission check on create_close_actions."""

    def test_neophyte_denied(self, database):
        """Neophytes should be denied."""
        pubkey = "02" + "aa" * 32
        ctx = _make_ctx(database, pubkey, tier='neophyte')

        result = create_close_actions(ctx)
        assert 'error' in result
        assert result['error'] == 'permission_denied'

    def test_member_allowed(self, database):
        """Members should be allowed (even if rationalization_mgr is missing)."""
        pubkey = "02" + "bb" * 32
        ctx = _make_ctx(database, pubkey, tier='member')

        result = create_close_actions(ctx)
        # Should pass permission check and hit rationalization_mgr check
        assert result == {"error": "Rationalization not initialized"}

    def test_member_with_rationalization_mgr(self, database):
        """Members with rationalization_mgr should succeed."""
        pubkey = "02" + "cc" * 32
        mock_mgr = MagicMock()
        mock_mgr.create_close_actions.return_value = {"actions_created": 2}
        ctx = _make_ctx(database, pubkey, tier='member', rationalization_mgr=mock_mgr)

        result = create_close_actions(ctx)
        assert result == {"actions_created": 2}
        mock_mgr.create_close_actions.assert_called_once()


class TestRejectActionWithReason:
    """Test reject_action with reason parameter."""

    def _insert_pending_action(self, database, action_type="channel_open"):
        """Helper to insert a pending action."""
        conn = database._get_connection()
        now = int(time.time())
        payload = json.dumps({"target": "peer_x", "amount_sats": 500000})
        conn.execute(
            "INSERT INTO pending_actions (action_type, payload, proposed_at, expires_at, status) "
            "VALUES (?, ?, ?, ?, ?)",
            (action_type, payload, now, now + 3600, 'pending')
        )
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def test_reject_with_reason(self, database):
        """Rejection reason should be stored."""
        pubkey = "02" + "dd" * 32
        ctx = _make_ctx(database, pubkey, tier='member')
        action_id = self._insert_pending_action(database)

        result = reject_action(ctx, action_id, reason="Too expensive")
        assert result['status'] == 'rejected'
        assert result['reason'] == 'Too expensive'

        # Verify in DB
        action = database.get_pending_action_by_id(action_id)
        assert action['status'] == 'rejected'
        assert action['rejection_reason'] == 'Too expensive'

    def test_reject_without_reason(self, database):
        """Rejection without reason should also work."""
        pubkey = "02" + "ee" * 32
        ctx = _make_ctx(database, pubkey, tier='member')
        action_id = self._insert_pending_action(database)

        result = reject_action(ctx, action_id)
        assert result['status'] == 'rejected'
        assert 'reason' not in result

    def test_reject_neophyte_denied(self, database):
        """Neophytes can't reject actions."""
        pubkey = "02" + "ff" * 32
        ctx = _make_ctx(database, pubkey, tier='neophyte')
        action_id = self._insert_pending_action(database)

        result = reject_action(ctx, action_id, reason="test")
        assert result['error'] == 'permission_denied'


class TestRejectAllActionsWithReason:
    """Test _reject_all_actions with reason parameter."""

    def _insert_pending_actions(self, database, count=3):
        """Helper to insert multiple pending actions."""
        conn = database._get_connection()
        now = int(time.time())
        for i in range(count):
            payload = json.dumps({"target": f"peer_{i}", "amount_sats": 500000})
            conn.execute(
                "INSERT INTO pending_actions (action_type, payload, proposed_at, expires_at, status) "
                "VALUES (?, ?, ?, ?, ?)",
                ("channel_open", payload, now, now + 3600, 'pending')
            )

    def test_reject_all_with_reason(self, database):
        """All actions should be rejected with the given reason."""
        pubkey = "02" + "11" * 32
        ctx = _make_ctx(database, pubkey, tier='member')
        self._insert_pending_actions(database, count=3)

        result = _reject_all_actions(ctx, reason="Market conditions unfavorable")
        assert result['rejected_count'] == 3

        # Verify all have the reason
        conn = database._get_connection()
        rows = conn.execute(
            "SELECT rejection_reason FROM pending_actions WHERE status = 'rejected'"
        ).fetchall()
        for row in rows:
            assert row['rejection_reason'] == "Market conditions unfavorable"

    def test_reject_all_empty(self, database):
        """No pending actions should return appropriate status."""
        pubkey = "02" + "22" * 32
        ctx = _make_ctx(database, pubkey, tier='member')

        result = _reject_all_actions(ctx)
        assert result['status'] == 'no_actions'
