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
    defense_status,
    record_rebalance_outcome,
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


# =========================================================================
# Tests for defense_status and record_rebalance_outcome
# =========================================================================

def _make_defense_ctx(database, pubkey, fee_coordination_mgr=None,
                      cost_reduction_mgr=None, safe_plugin=None):
    """Create HiveContext with fee coordination and cost reduction managers."""
    now = int(time.time())
    conn = database._get_connection()
    existing = conn.execute(
        "SELECT peer_id FROM hive_members WHERE peer_id = ?", (pubkey,)
    ).fetchone()
    if not existing:
        conn.execute(
            "INSERT INTO hive_members (peer_id, tier, joined_at) VALUES (?, ?, ?)",
            (pubkey, 'member', now)
        )
    return HiveContext(
        database=database,
        config=MagicMock(),
        safe_plugin=safe_plugin or MagicMock(),
        our_pubkey=pubkey,
        fee_coordination_mgr=fee_coordination_mgr,
        cost_reduction_mgr=cost_reduction_mgr,
        log=MagicMock(),
    )


class TestDefenseStatus:
    """Tests for hive-defense-status RPC handler."""

    def _make_warning(self, peer_id, threat_type="drain", severity=0.8, ttl=3600):
        """Create a mock PeerWarning-like object."""
        warn = MagicMock()
        warn.peer_id = peer_id
        warn.threat_type = threat_type
        warn.severity = severity
        warn.timestamp = time.time()
        warn.ttl = ttl
        warn.to_dict.return_value = {
            "peer_id": peer_id,
            "threat_type": threat_type,
            "severity": severity,
            "reporter": "02" + "aa" * 32,
            "timestamp": warn.timestamp,
            "ttl": ttl,
            "is_expired": False,
        }
        warn.is_expired.return_value = False
        return warn

    def test_defense_status_returns_active_warnings(self, database):
        """Active warnings should be returned as a list with enriched fields."""
        pubkey = "02" + "33" * 32
        threat_peer = "02" + "dd" * 32

        mock_fcm = MagicMock()
        warning = self._make_warning(threat_peer, severity=0.8)
        mock_fcm.defense_system.get_active_warnings.return_value = [warning]
        mock_fcm.defense_system.get_defensive_multiplier.return_value = 2.5
        mock_fcm.defense_system._defensive_fees = {threat_peer: {}}

        ctx = _make_defense_ctx(database, pubkey, fee_coordination_mgr=mock_fcm)
        result = defense_status(ctx)

        assert "error" not in result
        assert isinstance(result["active_warnings"], list)
        assert len(result["active_warnings"]) == 1
        assert result["warning_count"] == 1

        w = result["active_warnings"][0]
        assert w["peer_id"] == threat_peer
        assert "expires_at" in w
        assert w["defensive_multiplier"] == 2.5

    def test_defense_status_empty(self, database):
        """No warnings should return empty list."""
        pubkey = "02" + "44" * 32

        mock_fcm = MagicMock()
        mock_fcm.defense_system.get_active_warnings.return_value = []
        mock_fcm.defense_system._defensive_fees = {}

        ctx = _make_defense_ctx(database, pubkey, fee_coordination_mgr=mock_fcm)
        result = defense_status(ctx)

        assert result["active_warnings"] == []
        assert result["warning_count"] == 0

    def test_defense_status_peer_filter(self, database):
        """peer_id param should populate peer_threat field."""
        pubkey = "02" + "55" * 32
        threat_peer = "02" + "ee" * 32

        mock_fcm = MagicMock()
        warning = self._make_warning(threat_peer, severity=0.9, threat_type="drain")
        mock_fcm.defense_system.get_active_warnings.return_value = [warning]
        mock_fcm.defense_system.get_defensive_multiplier.return_value = 3.0
        mock_fcm.defense_system._defensive_fees = {}

        ctx = _make_defense_ctx(database, pubkey, fee_coordination_mgr=mock_fcm)
        result = defense_status(ctx, peer_id=threat_peer)

        assert "peer_threat" in result
        pt = result["peer_threat"]
        assert pt["is_threat"] is True
        assert pt["threat_type"] == "drain"
        assert pt["severity"] == 0.9
        assert pt["defensive_multiplier"] == 3.0

    def test_defense_status_peer_filter_no_threat(self, database):
        """peer_id with no matching warning should return is_threat=False."""
        pubkey = "02" + "66" * 32
        safe_peer = "02" + "ff" * 32

        mock_fcm = MagicMock()
        mock_fcm.defense_system.get_active_warnings.return_value = []
        mock_fcm.defense_system._defensive_fees = {}

        ctx = _make_defense_ctx(database, pubkey, fee_coordination_mgr=mock_fcm)
        result = defense_status(ctx, peer_id=safe_peer)

        assert result["peer_threat"]["is_threat"] is False
        assert result["peer_threat"]["defensive_multiplier"] == 1.0

    def test_defense_status_not_initialized(self, database):
        """Missing fee_coordination_mgr should return error."""
        pubkey = "02" + "77" * 32
        ctx = _make_defense_ctx(database, pubkey, fee_coordination_mgr=None)
        result = defense_status(ctx)
        assert "error" in result


class TestRecordRebalanceOutcome:
    """Tests for hive-report-rebalance-outcome RPC handler."""

    def test_report_outcome_deposits_marker(self, database):
        """Successful rebalance should deposit stigmergic marker."""
        pubkey = "02" + "88" * 32
        from_peer = "02" + "aa" * 32
        to_peer = "02" + "bb" * 32

        mock_crm = MagicMock()
        mock_crm.record_rebalance_outcome.return_value = {"status": "recorded"}

        mock_fcm = MagicMock()
        mock_safe = MagicMock()
        mock_safe.rpc.listpeerchannels.return_value = {
            "channels": [
                {"short_channel_id": "100x1x0", "peer_id": from_peer},
                {"short_channel_id": "200x2x0", "peer_id": to_peer},
            ]
        }

        ctx = _make_defense_ctx(
            database, pubkey,
            fee_coordination_mgr=mock_fcm,
            cost_reduction_mgr=mock_crm,
            safe_plugin=mock_safe,
        )

        result = record_rebalance_outcome(
            ctx, from_channel="100x1x0", to_channel="200x2x0",
            amount_sats=500000, cost_sats=150, success=True,
        )

        assert "error" not in result
        assert result["marker_deposited"] is True
        mock_fcm.stigmergic_coord.deposit_marker.assert_called_once()

        # Verify marker params
        call_kwargs = mock_fcm.stigmergic_coord.deposit_marker.call_args
        assert call_kwargs[1]["source"] == from_peer
        assert call_kwargs[1]["destination"] == to_peer
        assert call_kwargs[1]["success"] is True

    def test_report_outcome_failure_deposits_marker(self, database):
        """Failed rebalance should also deposit stigmergic marker."""
        pubkey = "02" + "99" * 32
        from_peer = "02" + "cc" * 32
        to_peer = "02" + "dd" * 32

        mock_crm = MagicMock()
        mock_crm.record_rebalance_outcome.return_value = {"status": "recorded"}

        mock_fcm = MagicMock()
        mock_safe = MagicMock()
        mock_safe.rpc.listpeerchannels.return_value = {
            "channels": [
                {"short_channel_id": "300x1x0", "peer_id": from_peer},
                {"short_channel_id": "400x2x0", "peer_id": to_peer},
            ]
        }

        ctx = _make_defense_ctx(
            database, pubkey,
            fee_coordination_mgr=mock_fcm,
            cost_reduction_mgr=mock_crm,
            safe_plugin=mock_safe,
        )

        result = record_rebalance_outcome(
            ctx, from_channel="300x1x0", to_channel="400x2x0",
            amount_sats=500000, cost_sats=0, success=False,
            failure_reason="no_route",
        )

        assert "error" not in result
        assert result["marker_deposited"] is True
        assert result["failure_reason"] == "no_route"

        call_kwargs = mock_fcm.stigmergic_coord.deposit_marker.call_args
        assert call_kwargs[1]["success"] is False
        assert call_kwargs[1]["volume_sats"] == 0  # 0 on failure

    def test_report_outcome_unknown_channel(self, database):
        """Unresolvable SCID should still record but not deposit marker."""
        pubkey = "02" + "ab" * 32

        mock_crm = MagicMock()
        mock_crm.record_rebalance_outcome.return_value = {"status": "recorded"}

        mock_fcm = MagicMock()
        mock_safe = MagicMock()
        mock_safe.rpc.listpeerchannels.return_value = {"channels": []}

        ctx = _make_defense_ctx(
            database, pubkey,
            fee_coordination_mgr=mock_fcm,
            cost_reduction_mgr=mock_crm,
            safe_plugin=mock_safe,
        )

        result = record_rebalance_outcome(
            ctx, from_channel="999x1x0", to_channel="999x2x0",
            amount_sats=100000, cost_sats=50, success=True,
        )

        assert "error" not in result
        assert result["marker_deposited"] is False
        mock_fcm.stigmergic_coord.deposit_marker.assert_not_called()
