"""
Tests for database integrity fixes from audit 2026-02-10.

Tests cover:
- H-3: pending_actions indexes exist
- H-5: prune_budget_tracking works
- H-8: prune_old_settlement_data atomicity
- H-9: sync_uptime_from_presence JOIN-based query
- M-11: update_presence TOCTOU prevention
- M-12: log_planner_action transaction atomicity
"""

import pytest
import time
import threading
from unittest.mock import MagicMock

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.database import HiveDatabase


@pytest.fixture
def mock_plugin():
    plugin = MagicMock()
    plugin.log = MagicMock()
    return plugin


@pytest.fixture
def database(mock_plugin, tmp_path):
    db_path = str(tmp_path / "test_audit.db")
    db = HiveDatabase(db_path, mock_plugin)
    db.initialize()
    return db


class TestPendingActionsIndexes:
    """H-3: Verify indexes exist on pending_actions table."""

    def test_status_expires_index_exists(self, database):
        conn = database._get_connection()
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='pending_actions'"
        ).fetchall()
        index_names = [row['name'] for row in rows]
        assert 'idx_pending_actions_status_expires' in index_names

    def test_type_proposed_index_exists(self, database):
        conn = database._get_connection()
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='pending_actions'"
        ).fetchall()
        index_names = [row['name'] for row in rows]
        assert 'idx_pending_actions_type_proposed' in index_names


class TestPruneBudgetTracking:
    """H-5: Test prune_budget_tracking works correctly."""

    def test_prune_old_records(self, database):
        """Insert rows, prune, verify count."""
        conn = database._get_connection()
        now = int(time.time())
        old_ts = now - (100 * 86400)  # 100 days ago
        recent_ts = now - (10 * 86400)  # 10 days ago

        # Insert old records
        for i in range(5):
            conn.execute(
                "INSERT INTO budget_tracking (date_key, action_type, amount_sats, target, action_id, timestamp) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (f"2025-10-{i+1:02d}", "rebalance", 1000, "target_a", i, old_ts + i)
            )

        # Insert recent records
        for i in range(3):
            conn.execute(
                "INSERT INTO budget_tracking (date_key, action_type, amount_sats, target, action_id, timestamp) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (f"2026-01-{i+1:02d}", "rebalance", 2000, "target_b", 100 + i, recent_ts + i)
            )

        # Prune with 90-day threshold
        deleted = database.prune_budget_tracking(older_than_days=90)
        assert deleted == 5

        # Verify recent records remain
        remaining = conn.execute("SELECT COUNT(*) as cnt FROM budget_tracking").fetchone()
        assert remaining['cnt'] == 3

    def test_prune_no_old_records(self, database):
        """No records to prune returns 0."""
        deleted = database.prune_budget_tracking(older_than_days=90)
        assert deleted == 0


class TestUpdatePresenceTransaction:
    """M-11: Test update_presence TOCTOU prevention."""

    def test_insert_new_presence(self, database):
        """First call should insert."""
        now = int(time.time())
        database.update_presence("peer_a", True, now, 86400)
        result = database.get_presence("peer_a")
        assert result is not None
        assert result['peer_id'] == 'peer_a'
        assert result['is_online'] == 1

    def test_update_existing_presence(self, database):
        """Second call should update, not duplicate."""
        now = int(time.time())
        database.update_presence("peer_a", True, now, 86400)
        database.update_presence("peer_a", False, now + 100, 86400)

        result = database.get_presence("peer_a")
        assert result['is_online'] == 0
        assert result['online_seconds_rolling'] == 100

        # Verify no duplicate rows
        conn = database._get_connection()
        count = conn.execute(
            "SELECT COUNT(*) as cnt FROM peer_presence WHERE peer_id = ?",
            ("peer_a",)
        ).fetchone()
        assert count['cnt'] == 1

    def test_concurrent_presence_inserts(self, database):
        """No duplicate rows under concurrent inserts."""
        now = int(time.time())
        errors = []

        def insert_presence(peer_id):
            try:
                database.update_presence(peer_id, True, now, 86400)
            except Exception as e:
                errors.append(str(e))

        # Concurrent inserts for different peers should be fine
        threads = [
            threading.Thread(target=insert_presence, args=(f"peer_{i}",))
            for i in range(10)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert errors == []

        # Verify exactly 10 rows
        conn = database._get_connection()
        count = conn.execute("SELECT COUNT(*) as cnt FROM peer_presence").fetchone()
        assert count['cnt'] == 10


class TestLogPlannerActionTransaction:
    """M-12: Test log_planner_action transaction."""

    def test_ring_buffer_cap(self, database):
        """Verify ring buffer cap holds."""
        # Set a small cap for testing
        original_cap = database.MAX_PLANNER_LOG_ROWS
        database.MAX_PLANNER_LOG_ROWS = 20

        try:
            # Insert more than cap
            for i in range(25):
                database.log_planner_action(
                    action_type="test",
                    result="success",
                    target=f"target_{i}",
                    details={"iteration": i}
                )

            conn = database._get_connection()
            count = conn.execute("SELECT COUNT(*) as cnt FROM hive_planner_log").fetchone()
            # After 20 rows, 10% (2) are pruned before inserting next
            # So we should have <= 20 rows
            assert count['cnt'] <= 20
        finally:
            database.MAX_PLANNER_LOG_ROWS = original_cap

    def test_basic_logging(self, database):
        """Test basic planner log insertion."""
        database.log_planner_action(
            action_type="expansion",
            result="proposed",
            target="02" + "aa" * 32,
            details={"reason": "underserved"}
        )
        logs = database.get_planner_logs(limit=1)
        assert len(logs) == 1
        assert logs[0]['action_type'] == 'expansion'
        assert logs[0]['result'] == 'proposed'


class TestSyncUptimeFromPresence:
    """H-9: Test JOIN-based uptime calculation."""

    def test_correct_uptime_calculation(self, database):
        """Verify correct uptime from presence data."""
        now = int(time.time())
        conn = database._get_connection()

        # Add a member
        conn.execute(
            "INSERT INTO hive_members (peer_id, tier, joined_at) VALUES (?, ?, ?)",
            ("peer_a", "member", now - 86400)
        )

        # Add presence: online for 50% of window
        window = 1000
        conn.execute(
            "INSERT INTO peer_presence (peer_id, last_change_ts, is_online, "
            "online_seconds_rolling, window_start_ts) VALUES (?, ?, ?, ?, ?)",
            ("peer_a", now - 100, 0, 500, now - window)
        )

        updated = database.sync_uptime_from_presence(window_seconds=window)
        assert updated == 1

        # Check uptime
        member = conn.execute(
            "SELECT uptime_pct FROM hive_members WHERE peer_id = ?",
            ("peer_a",)
        ).fetchone()
        assert member['uptime_pct'] == pytest.approx(0.5, abs=0.05)

    def test_online_member_gets_credit(self, database):
        """Currently online members get credit for time since last change."""
        now = int(time.time())
        conn = database._get_connection()

        conn.execute(
            "INSERT INTO hive_members (peer_id, tier, joined_at) VALUES (?, ?, ?)",
            ("peer_b", "member", now - 86400)
        )

        # Online since window start
        window = 1000
        conn.execute(
            "INSERT INTO peer_presence (peer_id, last_change_ts, is_online, "
            "online_seconds_rolling, window_start_ts) VALUES (?, ?, ?, ?, ?)",
            ("peer_b", now - window, 1, 0, now - window)
        )

        updated = database.sync_uptime_from_presence(window_seconds=window)
        assert updated == 1

        member = conn.execute(
            "SELECT uptime_pct FROM hive_members WHERE peer_id = ?",
            ("peer_b",)
        ).fetchone()
        # Should be ~100% since online for the entire window
        assert member['uptime_pct'] == pytest.approx(1.0, abs=0.05)

    def test_no_presence_data_skipped(self, database):
        """Members without presence data are skipped."""
        now = int(time.time())
        conn = database._get_connection()

        conn.execute(
            "INSERT INTO hive_members (peer_id, tier, joined_at) VALUES (?, ?, ?)",
            ("peer_c", "member", now - 86400)
        )

        updated = database.sync_uptime_from_presence()
        assert updated == 0


class TestPruneSettlementData:
    """H-8: Test prune_old_settlement_data atomicity."""

    def _insert_proposal(self, conn, proposal_id, proposed_at):
        """Helper to insert a settlement proposal with correct schema."""
        conn.execute(
            "INSERT INTO settlement_proposals "
            "(proposal_id, period, proposer_peer_id, proposed_at, expires_at, "
            "status, data_hash, total_fees_sats, member_count) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (proposal_id, f"2025-W{proposal_id}", "peer_a", proposed_at,
             proposed_at + 3600, "completed", "hash123", 10000, 3)
        )

    def test_prune_deletes_related_data(self, database):
        """Verify all related data (proposals, votes, executions) is deleted."""
        conn = database._get_connection()
        old_ts = int(time.time()) - (100 * 86400)

        # Insert old proposal
        self._insert_proposal(conn, "prop_1", old_ts)

        # Insert related vote
        conn.execute(
            "INSERT INTO settlement_ready_votes "
            "(proposal_id, voter_peer_id, data_hash, voted_at, signature) "
            "VALUES (?, ?, ?, ?, ?)",
            ("prop_1", "peer_b", "hash123", old_ts, "sig_vote")
        )

        # Insert related execution
        conn.execute(
            "INSERT INTO settlement_executions "
            "(proposal_id, executor_peer_id, amount_paid_sats, executed_at, signature) "
            "VALUES (?, ?, ?, ?, ?)",
            ("prop_1", "peer_a", 10000, old_ts, "sig_exec")
        )

        total = database.prune_old_settlement_data(older_than_days=90)
        assert total == 3  # 1 execution + 1 vote + 1 proposal

        # Verify all gone
        assert conn.execute("SELECT COUNT(*) FROM settlement_proposals").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM settlement_ready_votes").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM settlement_executions").fetchone()[0] == 0

    def test_prune_preserves_recent(self, database):
        """Recent data should not be pruned."""
        conn = database._get_connection()
        now = int(time.time())

        self._insert_proposal(conn, "prop_recent", now)

        total = database.prune_old_settlement_data(older_than_days=90)
        assert total == 0
        assert conn.execute("SELECT COUNT(*) FROM settlement_proposals").fetchone()[0] == 1
