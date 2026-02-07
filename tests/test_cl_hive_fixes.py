"""
Tests for cl-hive bug fixes (Phase B/C hardening).

Tests for:
1. contribution.py: ban_autotrigger else branch correctness
2. intent_manager.py: None pubkey guard in tie-breaker and create_intent
3. gossip.py: per-string length validation in gossip payloads
4. gossip.py: FULL_SYNC per-peer rate limiting
5. liquidity_coordinator.py: consistent composite dict keying
6. state_manager.py: defensive copy of topology/fee_policy
7. config.py: cross-field validation (min > max channel size)
8. database.py: close_connection method for thread-local cleanup
"""

import pytest
import time
import threading
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.intent_manager import (
    IntentManager, Intent, IntentType,
    STATUS_PENDING, STATUS_ABORTED,
)
from modules.gossip import (
    GossipManager, MAX_GOSSIP_STRING_LEN, FULL_SYNC_COOLDOWN,
)
from modules.state_manager import HivePeerState
from modules.config import HiveConfig
from modules.contribution import ContributionManager, LEECH_WINDOW_DAYS


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_plugin():
    plugin = MagicMock()
    plugin.log = MagicMock()
    return plugin


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.create_intent.return_value = 1
    db.get_conflicting_intents.return_value = []
    db.update_intent_status.return_value = True
    db.cleanup_expired_intents.return_value = 0
    db.get_pending_intents_ready.return_value = []
    return db


@pytest.fixture
def mock_state_manager():
    sm = MagicMock()
    sm.update_peer_state.return_value = True
    sm.apply_full_sync.return_value = 5
    sm.get_peer_state.return_value = None
    return sm


# =============================================================================
# FIX 1: contribution.py — ban_autotrigger else branch
# =============================================================================

class TestBanAutotriggerFix:
    """Fix 1: ban_autotrigger_enabled=False should NOT set ban_triggered=True."""

    def _make_manager(self, mock_plugin, mock_db, autotrigger_enabled):
        config = MagicMock()
        config.ban_autotrigger_enabled = autotrigger_enabled
        mgr = ContributionManager(rpc=MagicMock(), db=mock_db, plugin=mock_plugin, config=config)
        return mgr

    def test_autotrigger_disabled_sets_false(self, mock_plugin, mock_db):
        """When autotrigger is disabled, ban_triggered should be False."""
        mock_db.get_contribution_stats.return_value = {"forwarded": 10, "received": 100}
        # Ratio = 10/100 = 0.1 < LEECH_BAN_RATIO (0.4) -> leech
        mock_db.get_leech_flag.return_value = {
            "low_since_ts": int(time.time()) - (LEECH_WINDOW_DAYS * 86400) - 1,
            "ban_triggered": False,
        }

        mgr = self._make_manager(mock_plugin, mock_db, autotrigger_enabled=False)
        result = mgr.check_leech_status("02" + "a" * 64)

        assert result["is_leech"] is True
        # The critical check: set_leech_flag must be called with ban_triggered=False
        mock_db.set_leech_flag.assert_called_once()
        call_args = mock_db.set_leech_flag.call_args
        assert call_args[0][2] is False, "ban_triggered should be False when autotrigger disabled"

    def test_autotrigger_enabled_sets_true(self, mock_plugin, mock_db):
        """When autotrigger is enabled, ban_triggered should be True."""
        mock_db.get_contribution_stats.return_value = {"forwarded": 10, "received": 100}
        mock_db.get_leech_flag.return_value = {
            "low_since_ts": int(time.time()) - (LEECH_WINDOW_DAYS * 86400) - 1,
            "ban_triggered": False,
        }

        mgr = self._make_manager(mock_plugin, mock_db, autotrigger_enabled=True)
        result = mgr.check_leech_status("02" + "a" * 64)

        assert result["is_leech"] is True
        mock_db.set_leech_flag.assert_called_once()
        call_args = mock_db.set_leech_flag.call_args
        assert call_args[0][2] is True, "ban_triggered should be True when autotrigger enabled"


# =============================================================================
# FIX 2: intent_manager.py — None pubkey guard
# =============================================================================

class TestNonePubkeyGuard:
    """Fix 2: None our_pubkey should not crash, should return safe defaults."""

    def test_check_conflicts_none_pubkey_no_crash(self, mock_db, mock_plugin):
        """check_conflicts with None pubkey returns (True, False) without TypeError."""
        mgr = IntentManager(mock_db, mock_plugin, our_pubkey=None)

        remote = Intent(
            intent_type="channel_open",
            target="02" + "b" * 64,
            initiator="02" + "c" * 64,
            timestamp=int(time.time()),
            expires_at=int(time.time()) + 60,
        )
        has_conflict, we_win = mgr.check_conflicts(remote)
        assert has_conflict is True
        assert we_win is False

    def test_create_intent_none_pubkey_returns_none(self, mock_db, mock_plugin):
        """create_intent with None pubkey returns None instead of crashing."""
        mgr = IntentManager(mock_db, mock_plugin, our_pubkey=None)
        result = mgr.create_intent("channel_open", "02" + "b" * 64)
        assert result is None

    def test_check_conflicts_with_pubkey_works(self, mock_db, mock_plugin):
        """Normal tie-breaker works when pubkey is set."""
        our_pk = "02" + "a" * 64  # 'a' < 'c' -> we win
        mgr = IntentManager(mock_db, mock_plugin, our_pubkey=our_pk)

        # Return a local conflict so tie-breaker runs
        mock_db.get_conflicting_intents.return_value = [{"id": 1, "intent_type": "channel_open"}]

        remote = Intent(
            intent_type="channel_open",
            target="02" + "b" * 64,
            initiator="02" + "c" * 64,
            timestamp=int(time.time()),
            expires_at=int(time.time()) + 60,
        )
        has_conflict, we_win = mgr.check_conflicts(remote)
        assert has_conflict is True
        assert we_win is True  # 'a' < 'c'


# =============================================================================
# FIX 3: gossip.py — String length validation
# =============================================================================

class TestGossipStringLengthValidation:
    """Fix 3: Oversized strings in topology/fee_policy should be rejected."""

    def _make_gossip_mgr(self, mock_state_manager, mock_plugin):
        return GossipManager(mock_state_manager, mock_plugin)

    def test_oversized_topology_entry_rejected(self, mock_state_manager, mock_plugin):
        """Topology with an entry exceeding MAX_GOSSIP_STRING_LEN is rejected."""
        mgr = self._make_gossip_mgr(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64

        payload = {
            "peer_id": sender,
            "version": 1,
            "timestamp": int(time.time()),
            "topology": ["02" + "b" * 64, "X" * (MAX_GOSSIP_STRING_LEN + 1)],
            "fee_policy": {},
        }
        result = mgr.process_gossip(sender, payload)
        assert result is False

    def test_oversized_fee_policy_key_rejected(self, mock_state_manager, mock_plugin):
        """fee_policy with an oversized key is rejected."""
        mgr = self._make_gossip_mgr(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64

        payload = {
            "peer_id": sender,
            "version": 1,
            "timestamp": int(time.time()),
            "topology": [],
            "fee_policy": {"K" * (MAX_GOSSIP_STRING_LEN + 1): 100},
        }
        result = mgr.process_gossip(sender, payload)
        assert result is False

    def test_non_string_topology_entry_rejected(self, mock_state_manager, mock_plugin):
        """Non-string entries in topology are rejected."""
        mgr = self._make_gossip_mgr(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64

        payload = {
            "peer_id": sender,
            "version": 1,
            "timestamp": int(time.time()),
            "topology": ["02" + "b" * 64, 12345],
            "fee_policy": {},
        }
        result = mgr.process_gossip(sender, payload)
        assert result is False

    def test_valid_gossip_accepted(self, mock_state_manager, mock_plugin):
        """Normal-sized gossip passes validation."""
        mgr = self._make_gossip_mgr(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64

        payload = {
            "peer_id": sender,
            "version": 1,
            "timestamp": int(time.time()),
            "topology": ["02" + "b" * 64, "02" + "c" * 64],
            "fee_policy": {"base_fee": 1000},
        }
        result = mgr.process_gossip(sender, payload)
        assert result is True


# =============================================================================
# FIX 4: gossip.py — FULL_SYNC rate limit
# =============================================================================

class TestFullSyncRateLimit:
    """Fix 4: Rapid FULL_SYNC from same peer should be rate-limited."""

    def test_first_full_sync_accepted(self, mock_state_manager, mock_plugin):
        """First FULL_SYNC from a peer should be processed."""
        mgr = GossipManager(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64

        payload = {"states": [{"peer_id": "02" + "b" * 64, "version": 1}]}
        result = mgr.process_full_sync(sender, payload)
        assert result == 5  # mock returns 5

    def test_rapid_full_sync_rate_limited(self, mock_state_manager, mock_plugin):
        """Second FULL_SYNC within cooldown from same peer should return 0."""
        mgr = GossipManager(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64

        payload = {"states": [{"peer_id": "02" + "b" * 64, "version": 1}]}
        mgr.process_full_sync(sender, payload)

        # Second attempt within cooldown
        result = mgr.process_full_sync(sender, payload)
        assert result == 0

    def test_different_peer_not_rate_limited(self, mock_state_manager, mock_plugin):
        """FULL_SYNC from a different peer should not be affected by first peer's limit."""
        mgr = GossipManager(mock_state_manager, mock_plugin)
        sender1 = "02" + "a" * 64
        sender2 = "02" + "b" * 64

        payload = {"states": [{"peer_id": "02" + "c" * 64, "version": 1}]}
        mgr.process_full_sync(sender1, payload)

        result = mgr.process_full_sync(sender2, payload)
        assert result == 5  # Not rate limited

    def test_full_sync_after_cooldown(self, mock_state_manager, mock_plugin):
        """FULL_SYNC after cooldown period should be accepted."""
        mgr = GossipManager(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64

        payload = {"states": [{"peer_id": "02" + "b" * 64, "version": 1}]}
        mgr.process_full_sync(sender, payload)

        # Simulate cooldown expiry
        mgr._full_sync_times[sender] = time.time() - FULL_SYNC_COOLDOWN - 1

        result = mgr.process_full_sync(sender, payload)
        assert result == 5


# =============================================================================
# FIX 5: liquidity_coordinator.py — Consistent dict keying
# =============================================================================

class TestLiquidityDictKeying:
    """Fix 5: Single + batch needs from same reporter use consistent composite keys."""

    def _make_coordinator(self, mock_plugin):
        from modules.liquidity_coordinator import LiquidityCoordinator

        db = MagicMock()
        db.store_liquidity_need = MagicMock()
        db.get_member.return_value = {"tier": "member"}

        rpc = MagicMock()

        coord = LiquidityCoordinator(
            database=db,
            plugin=mock_plugin,
            our_pubkey="02" + "a" * 64,
        )
        return coord, rpc

    @patch('modules.liquidity_coordinator.validate_liquidity_need_payload', return_value=True)
    @patch('modules.liquidity_coordinator.get_liquidity_need_signing_payload', return_value="signing_msg")
    def test_single_need_uses_composite_key(self, mock_sign, mock_validate, mock_plugin):
        """Single need should use composite key reporter_id:target_peer_id."""
        coord, rpc = self._make_coordinator(mock_plugin)

        reporter = "02" + "b" * 64
        target = "02" + "c" * 64
        rpc.checkmessage.return_value = {"verified": True, "pubkey": reporter}

        payload = {
            "reporter_id": reporter,
            "need_type": "rebalance",
            "target_peer_id": target,
            "amount_sats": 100000,
            "urgency": "medium",
            "max_fee_ppm": 100,
            "reason": "test",
            "current_balance_pct": 0.2,
            "timestamp": int(time.time()),
            "signature": "fakesignature00",
        }

        coord.handle_liquidity_need(reporter, payload, rpc)

        expected_key = f"{reporter}:{target}"
        assert expected_key in coord._liquidity_needs
        # The bare reporter_id should NOT be a key
        assert reporter not in coord._liquidity_needs

    @patch('modules.liquidity_coordinator.validate_liquidity_snapshot_payload', return_value=True)
    @patch('modules.liquidity_coordinator.validate_liquidity_need_payload', return_value=True)
    @patch('modules.liquidity_coordinator.get_liquidity_need_signing_payload', return_value="signing_msg")
    @patch('modules.liquidity_coordinator.get_liquidity_snapshot_signing_payload', return_value="signing_msg")
    def test_single_then_batch_no_stale_entries(self, mock_snap_sign, mock_need_sign,
                                                 mock_need_val, mock_snap_val, mock_plugin):
        """A reporter submitting single then batch needs shouldn't accumulate stale entries."""
        coord, rpc = self._make_coordinator(mock_plugin)

        reporter = "02" + "b" * 64
        target1 = "02" + "c" * 64
        target2 = "02" + "d" * 64
        rpc.checkmessage.return_value = {"verified": True, "pubkey": reporter}

        # Single need for target1
        payload = {
            "reporter_id": reporter,
            "need_type": "rebalance",
            "target_peer_id": target1,
            "amount_sats": 100000,
            "urgency": "medium",
            "max_fee_ppm": 100,
            "reason": "test",
            "current_balance_pct": 0.2,
            "timestamp": int(time.time()),
            "signature": "fakesignature00",
        }
        coord.handle_liquidity_need(reporter, payload, rpc)

        # Batch need for target2
        batch_payload = {
            "reporter_id": reporter,
            "needs": [
                {
                    "need_type": "rebalance",
                    "target_peer_id": target2,
                    "amount_sats": 200000,
                    "urgency": "high",
                    "max_fee_ppm": 200,
                    "reason": "batch",
                    "current_balance_pct": 0.3,
                }
            ],
            "timestamp": int(time.time()),
            "signature": "fakesignature00",
        }
        coord.handle_liquidity_snapshot(reporter, batch_payload, rpc)

        # Both should use composite keys
        assert f"{reporter}:{target1}" in coord._liquidity_needs
        assert f"{reporter}:{target2}" in coord._liquidity_needs
        assert len(coord._liquidity_needs) == 2


# =============================================================================
# FIX 6: state_manager.py — Defensive copy
# =============================================================================

class TestStateManagerDefensiveCopy:
    """Fix 6: External mutation of topology/fee_policy shouldn't corrupt state."""

    def test_topology_mutation_doesnt_corrupt_state(self):
        """Mutating the original topology list after from_dict shouldn't affect state."""
        original_topology = ["02" + "a" * 64, "02" + "b" * 64]
        data = {
            "peer_id": "02" + "c" * 64,
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {"base_fee": 1000},
            "topology": original_topology,
            "version": 1,
            "last_update": int(time.time()),
        }

        state = HivePeerState.from_dict(data)

        # Mutate the original list
        original_topology.append("02" + "x" * 64)

        # State should not be affected
        assert len(state.topology) == 2
        assert "02" + "x" * 64 not in state.topology

    def test_fee_policy_mutation_doesnt_corrupt_state(self):
        """Mutating the original fee_policy dict after from_dict shouldn't affect state."""
        original_policy = {"base_fee": 1000, "fee_rate": 100}
        data = {
            "peer_id": "02" + "c" * 64,
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": original_policy,
            "topology": [],
            "version": 1,
            "last_update": int(time.time()),
        }

        state = HivePeerState.from_dict(data)

        # Mutate the original dict
        original_policy["base_fee"] = 9999
        original_policy["new_key"] = "injected"

        # State should not be affected
        assert state.fee_policy["base_fee"] == 1000
        assert "new_key" not in state.fee_policy


# =============================================================================
# FIX 7: config.py — Cross-field validation
# =============================================================================

class TestConfigCrossFieldValidation:
    """Fix 7: min_channel_size > max_channel_size should fail validation."""

    def test_min_greater_than_max_fails(self):
        """Config where min > max channel sats should return an error."""
        config = HiveConfig(
            planner_min_channel_sats=50_000_000,
            planner_max_channel_sats=10_000_000,
            planner_default_channel_sats=5_000_000,
        )
        error = config.validate()
        assert error is not None
        assert "planner_min_channel_sats" in error

    def test_default_outside_range_fails(self):
        """Config where default channel sats is outside min/max range should fail."""
        config = HiveConfig(
            planner_min_channel_sats=10_000_000,
            planner_max_channel_sats=50_000_000,
            planner_default_channel_sats=5_000_000,  # Below min
        )
        error = config.validate()
        assert error is not None
        assert "planner_default_channel_sats" in error

    def test_valid_config_passes(self):
        """Valid config with correct ordering should pass."""
        config = HiveConfig(
            planner_min_channel_sats=1_000_000,
            planner_max_channel_sats=50_000_000,
            planner_default_channel_sats=5_000_000,
        )
        error = config.validate()
        assert error is None


# =============================================================================
# FIX 8: database.py — close_connection method
# =============================================================================

class TestDatabaseCloseConnection:
    """Fix 8: close_connection should clean up thread-local connections."""

    def test_close_connection_exists(self):
        """HiveDatabase should have a close_connection method."""
        from modules.database import HiveDatabase
        assert hasattr(HiveDatabase, 'close_connection')

    def test_close_connection_with_no_conn(self, mock_plugin):
        """close_connection should not crash when no connection exists."""
        from modules.database import HiveDatabase
        db = HiveDatabase.__new__(HiveDatabase)
        db._local = threading.local()
        db.plugin = mock_plugin
        db.db_path = "/tmp/test_nonexistent.db"

        # Should not raise
        db.close_connection()

    def test_close_connection_closes_and_clears(self, mock_plugin):
        """close_connection should close conn and set to None."""
        from modules.database import HiveDatabase
        db = HiveDatabase.__new__(HiveDatabase)
        db._local = threading.local()
        db.plugin = mock_plugin
        db.db_path = "/tmp/test_nonexistent.db"

        # Simulate an existing connection
        mock_conn = MagicMock()
        db._local.conn = mock_conn

        db.close_connection()

        mock_conn.close.assert_called_once()
        assert db._local.conn is None
