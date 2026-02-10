"""
Tests for HiveMap (state_manager) and Topology Planner bug fixes.

Covers:
- Bug: _validate_state_entry() silently mutated input dict (available > capacity)
- Bug: update_peer_state() missing defensive copies for fee_policy/topology
- Bug: load_from_database() not using from_dict(), missing defensive copies
- Bug: Gossip process_gossip() missing timestamp freshness check
- Bug: Planner _propose_expansion() missing feerate gate
- Bug: Planner cfg.market_share_cap_pct crash (direct attribute access)
- Bug: Planner cfg.governance_mode crash (direct attribute access)
"""

import pytest
import time
from unittest.mock import MagicMock, patch, PropertyMock
from dataclasses import dataclass

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.state_manager import StateManager, HivePeerState
from modules.gossip import GossipManager, GossipState


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_database():
    db = MagicMock()
    db.get_all_hive_states.return_value = []
    db.update_hive_state.return_value = None
    db.log_planner_action.return_value = None
    return db


@pytest.fixture
def mock_plugin():
    plugin = MagicMock()
    plugin.log = MagicMock()
    return plugin


@pytest.fixture
def state_manager(mock_database, mock_plugin):
    return StateManager(mock_database, mock_plugin)


@pytest.fixture
def gossip_manager(state_manager, mock_plugin):
    return GossipManager(state_manager, mock_plugin, heartbeat_interval=300)


# =============================================================================
# STATE MANAGER: _validate_state_entry() MUTATION FIX
# =============================================================================

class TestValidateStateEntryNoMutation:
    """Verify _validate_state_entry no longer mutates the input dict."""

    def test_available_gt_capacity_rejected(self, state_manager):
        """available_sats > capacity_sats should be rejected, not silently capped."""
        data = {
            "peer_id": "02" + "a" * 64,
            "capacity_sats": 1000000,
            "available_sats": 2000000,  # More than capacity
            "version": 1,
            "timestamp": int(time.time()),
        }
        original_available = data["available_sats"]

        result = state_manager._validate_state_entry(data)

        # Should reject invalid data
        assert result is False
        # Input dict must NOT be mutated
        assert data["available_sats"] == original_available

    def test_available_eq_capacity_accepted(self, state_manager):
        """available_sats == capacity_sats should be accepted."""
        data = {
            "peer_id": "02" + "b" * 64,
            "capacity_sats": 1000000,
            "available_sats": 1000000,
            "version": 1,
            "timestamp": int(time.time()),
        }
        assert state_manager._validate_state_entry(data) is True

    def test_available_lt_capacity_accepted(self, state_manager):
        """available_sats < capacity_sats should be accepted."""
        data = {
            "peer_id": "02" + "c" * 64,
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "version": 1,
            "timestamp": int(time.time()),
        }
        assert state_manager._validate_state_entry(data) is True


# =============================================================================
# STATE MANAGER: update_peer_state() DEFENSIVE COPIES
# =============================================================================

class TestUpdatePeerStateDefensiveCopies:
    """Verify update_peer_state makes defensive copies of mutable fields."""

    def test_fee_policy_is_defensive_copy(self, state_manager):
        """Modifying original fee_policy dict should not affect stored state."""
        fee_policy = {"base_fee": 1000, "fee_rate": 100}
        gossip_data = {
            "peer_id": "02" + "d" * 64,
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": fee_policy,
            "topology": ["peer1"],
            "version": 1,
            "timestamp": int(time.time()),
        }

        state_manager.update_peer_state("02" + "d" * 64, gossip_data)

        # Mutate the original fee_policy
        fee_policy["base_fee"] = 9999

        # Stored state should not be affected
        stored = state_manager.get_peer_state("02" + "d" * 64)
        assert stored.fee_policy["base_fee"] == 1000

    def test_topology_is_defensive_copy(self, state_manager):
        """Modifying original topology list should not affect stored state."""
        topology = ["peer1", "peer2"]
        gossip_data = {
            "peer_id": "02" + "e" * 64,
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {},
            "topology": topology,
            "version": 1,
            "timestamp": int(time.time()),
        }

        state_manager.update_peer_state("02" + "e" * 64, gossip_data)

        # Mutate the original topology
        topology.append("INJECTED")

        # Stored state should not be affected
        stored = state_manager.get_peer_state("02" + "e" * 64)
        assert "INJECTED" not in stored.topology
        assert len(stored.topology) == 2

    def test_available_capped_at_capacity(self, state_manager):
        """update_peer_state should cap available_sats at capacity_sats."""
        gossip_data = {
            "peer_id": "02" + "f" * 64,
            "capacity_sats": 1000000,
            "available_sats": 1500000,  # Invalid: more than capacity
            "fee_policy": {},
            "topology": [],
            "version": 1,
            "timestamp": int(time.time()),
        }

        # With the new validation, this should be rejected
        result = state_manager.update_peer_state("02" + "f" * 64, gossip_data)
        assert result is False


# =============================================================================
# STATE MANAGER: load_from_database() USES from_dict()
# =============================================================================

class TestLoadFromDatabaseUsesFromDict:
    """Verify load_from_database uses from_dict() for consistent field handling."""

    def test_load_creates_defensive_copies(self, mock_database, mock_plugin):
        """Loaded state should have defensive copies of mutable fields."""
        fee_policy = {"base_fee": 500}
        topology = ["external1"]
        mock_database.get_all_hive_states.return_value = [
            {
                "peer_id": "02" + "a" * 64,
                "capacity_sats": 2000000,
                "available_sats": 1000000,
                "fee_policy": fee_policy,
                "topology": topology,
                "version": 5,
                "last_gossip": 1700000000,
                "state_hash": "abc123",
            }
        ]

        sm = StateManager(mock_database, mock_plugin)
        sm.load_from_database()

        # Mutate originals
        fee_policy["base_fee"] = 9999
        topology.append("INJECTED")

        state = sm.get_peer_state("02" + "a" * 64)
        assert state is not None
        assert state.fee_policy["base_fee"] == 500
        assert "INJECTED" not in state.topology

    def test_load_handles_last_gossip_field(self, mock_database, mock_plugin):
        """DB uses 'last_gossip' but HivePeerState uses 'last_update'."""
        mock_database.get_all_hive_states.return_value = [
            {
                "peer_id": "02" + "b" * 64,
                "capacity_sats": 1000000,
                "available_sats": 500000,
                "fee_policy": {},
                "topology": [],
                "version": 3,
                "last_gossip": 1700000000,
                "state_hash": "",
            }
        ]

        sm = StateManager(mock_database, mock_plugin)
        sm.load_from_database()

        state = sm.get_peer_state("02" + "b" * 64)
        assert state is not None
        assert state.last_update == 1700000000

    def test_load_skips_invalid_entries(self, mock_database, mock_plugin):
        """Entries with empty peer_id should be skipped."""
        mock_database.get_all_hive_states.return_value = [
            {
                "peer_id": "",
                "capacity_sats": 1000000,
                "available_sats": 500000,
                "fee_policy": {},
                "topology": [],
                "version": 1,
                "last_gossip": 0,
            },
            {
                "peer_id": "02" + "c" * 64,
                "capacity_sats": 2000000,
                "available_sats": 1000000,
                "fee_policy": {},
                "topology": [],
                "version": 2,
                "last_gossip": 0,
            },
        ]

        sm = StateManager(mock_database, mock_plugin)

        # Valid entry loaded by __init__, invalid entry skipped
        assert "02" + "c" * 64 in sm._local_state
        assert "" not in sm._local_state

        # Calling load_from_database again returns 0 (same versions)
        loaded = sm.load_from_database()
        assert loaded == 0


# =============================================================================
# GOSSIP: TIMESTAMP FRESHNESS CHECK
# =============================================================================

class TestGossipTimestampFreshness:
    """Verify process_gossip rejects stale and future-dated messages."""

    def test_rejects_stale_gossip(self, gossip_manager):
        """Gossip with timestamp > 1 hour old should be rejected."""
        now = int(time.time())
        payload = {
            "peer_id": "02" + "a" * 64,
            "version": 1,
            "timestamp": now - 7200,  # 2 hours old
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {},
            "topology": [],
        }

        result = gossip_manager.process_gossip("02" + "a" * 64, payload)
        assert result is False

    def test_rejects_future_gossip(self, gossip_manager):
        """Gossip with timestamp > 5 minutes in future should be rejected."""
        now = int(time.time())
        payload = {
            "peer_id": "02" + "b" * 64,
            "version": 1,
            "timestamp": now + 600,  # 10 minutes in the future
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {},
            "topology": [],
        }

        result = gossip_manager.process_gossip("02" + "b" * 64, payload)
        assert result is False

    def test_accepts_recent_gossip(self, gossip_manager):
        """Gossip with recent timestamp should be accepted."""
        now = int(time.time())
        payload = {
            "peer_id": "02" + "c" * 64,
            "version": 1,
            "timestamp": now - 30,  # 30 seconds ago - fresh
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {},
            "topology": [],
        }

        result = gossip_manager.process_gossip("02" + "c" * 64, payload)
        assert result is True

    def test_accepts_slight_clock_skew(self, gossip_manager):
        """Gossip with slight clock skew (< 5 min) should be accepted."""
        now = int(time.time())
        payload = {
            "peer_id": "02" + "d" * 64,
            "version": 1,
            "timestamp": now + 120,  # 2 minutes ahead - within tolerance
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {},
            "topology": [],
        }

        result = gossip_manager.process_gossip("02" + "d" * 64, payload)
        assert result is True

    def test_rejects_sender_mismatch(self, gossip_manager):
        """Gossip with sender != payload peer_id should be rejected."""
        now = int(time.time())
        payload = {
            "peer_id": "02" + "e" * 64,
            "version": 1,
            "timestamp": now,
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {},
            "topology": [],
        }

        result = gossip_manager.process_gossip("02" + "f" * 64, payload)
        assert result is False


# =============================================================================
# PLANNER: FEERATE GATE
# =============================================================================

class TestPlannerFeerateGate:
    """Verify planner blocks expansion when feerates are too high."""

    def _make_planner(self, mock_plugin, mock_database, feerate_response=None):
        """Create a planner with mocked RPC."""
        from modules.planner import Planner
        from modules.state_manager import StateManager

        mock_plugin.rpc = MagicMock()
        if feerate_response is not None:
            mock_plugin.rpc.feerates.return_value = feerate_response

        mock_state_mgr = MagicMock(spec=StateManager)
        mock_bridge = MagicMock()
        mock_clboss = MagicMock()

        planner = Planner(
            state_manager=mock_state_mgr,
            database=mock_database,
            bridge=mock_bridge,
            clboss_bridge=mock_clboss,
            plugin=mock_plugin,
            intent_manager=MagicMock(),
        )
        return planner

    def _make_cfg(self, **overrides):
        """Create a minimal config snapshot for planner."""
        @dataclass
        class FakeCfg:
            max_expansion_feerate_perkb: int = 5000
            market_share_cap_pct: float = 0.20
            governance_mode: str = 'advisor'
            planner_enable_expansions: bool = True
            planner_min_channel_sats: int = 1000000
            planner_safety_reserve_sats: int = 500000
            planner_fee_buffer_sats: int = 100000
            rejection_cooldown_seconds: int = 86400
            planner_max_expansion_rate: int = 1
            planner_expansion_cooldown: int = 3600

        cfg = FakeCfg()
        for k, v in overrides.items():
            setattr(cfg, k, v)
        return cfg

    def test_feerate_too_high_blocks_expansion(self, mock_plugin, mock_database):
        """Expansion should be blocked when opening feerate > max threshold."""
        planner = self._make_planner(mock_plugin, mock_database, feerate_response={
            "perkb": {"opening": 10000}  # 10000 > 5000 default max
        })

        cfg = self._make_cfg(max_expansion_feerate_perkb=5000)

        # Mock out methods that would be called before feerate gate
        planner._should_pause_expansions_globally = MagicMock(return_value=(False, ""))

        decisions = planner._propose_expansion(cfg, run_id="test-1")

        # Should have no expansion decisions
        assert decisions == []
        # Should have logged a planner action
        mock_database.log_planner_action.assert_called()
        call_args = mock_database.log_planner_action.call_args
        assert call_args[1]['result'] == 'skipped'
        assert call_args[1]['details']['reason'] == 'feerate_too_high'

    def test_feerate_acceptable_allows_expansion(self, mock_plugin, mock_database):
        """Expansion should proceed when opening feerate <= max threshold."""
        planner = self._make_planner(mock_plugin, mock_database, feerate_response={
            "perkb": {"opening": 3000}  # 3000 < 5000 max
        })

        cfg = self._make_cfg(max_expansion_feerate_perkb=5000)

        planner._should_pause_expansions_globally = MagicMock(return_value=(False, ""))
        # It will proceed to the onchain balance check - mock it to return low funds
        # to exit early (we're only testing feerate gate)
        planner._get_local_onchain_balance = MagicMock(return_value=0)

        decisions = planner._propose_expansion(cfg, run_id="test-2")

        # Should reach the balance check (feerate passed), then exit due to low funds
        call_args = mock_database.log_planner_action.call_args
        assert call_args[1]['details']['reason'] == 'insufficient_funds'

    def test_feerate_zero_disables_check(self, mock_plugin, mock_database):
        """max_expansion_feerate_perkb=0 should disable the feerate gate."""
        planner = self._make_planner(mock_plugin, mock_database)

        cfg = self._make_cfg(max_expansion_feerate_perkb=0)

        planner._should_pause_expansions_globally = MagicMock(return_value=(False, ""))
        planner._get_local_onchain_balance = MagicMock(return_value=0)

        decisions = planner._propose_expansion(cfg, run_id="test-3")

        # Should NOT have called feerates RPC
        mock_plugin.rpc.feerates.assert_not_called()
        # Should have reached the balance check
        call_args = mock_database.log_planner_action.call_args
        assert call_args[1]['details']['reason'] == 'insufficient_funds'

    def test_feerate_rpc_failure_allows_expansion(self, mock_plugin, mock_database):
        """If feerate RPC fails, expansion should proceed (fail-open for non-critical)."""
        planner = self._make_planner(mock_plugin, mock_database)
        mock_plugin.rpc.feerates.side_effect = Exception("RPC timeout")

        cfg = self._make_cfg(max_expansion_feerate_perkb=5000)

        planner._should_pause_expansions_globally = MagicMock(return_value=(False, ""))
        planner._get_local_onchain_balance = MagicMock(return_value=0)

        decisions = planner._propose_expansion(cfg, run_id="test-4")

        # Should have proceeded past feerate check to balance check
        call_args = mock_database.log_planner_action.call_args
        assert call_args[1]['details']['reason'] == 'insufficient_funds'


# =============================================================================
# PLANNER: CONFIG ATTRIBUTE SAFETY
# =============================================================================

class TestPlannerConfigSafety:
    """Verify planner uses getattr for config access."""

    def test_market_share_cap_uses_getattr(self):
        """market_share_cap_pct should use getattr with default 0.20 in source."""
        import inspect
        from modules.planner import Planner

        source = inspect.getsource(Planner)
        # Verify the source uses getattr for market_share_cap_pct
        assert "getattr(cfg, 'market_share_cap_pct'" in source
        # Should NOT have direct access pattern
        lines = source.split('\n')
        for line in lines:
            stripped = line.strip()
            if 'cfg.market_share_cap_pct' in stripped and 'getattr' not in stripped:
                pytest.fail(f"Direct cfg.market_share_cap_pct access: {stripped}")

    def test_governance_mode_uses_getattr(self):
        """governance_mode should use getattr with default 'advisor' in source."""
        import inspect
        from modules.planner import Planner

        source = inspect.getsource(Planner)
        # Verify the source uses getattr for governance_mode
        assert "getattr(cfg, 'governance_mode'" in source
        # Check no direct access
        lines = source.split('\n')
        for line in lines:
            stripped = line.strip()
            if 'cfg.governance_mode' in stripped and 'getattr' not in stripped:
                pytest.fail(f"Direct cfg.governance_mode access: {stripped}")

    def test_feerate_config_uses_getattr(self):
        """max_expansion_feerate_perkb should use getattr in source."""
        import inspect
        from modules.planner import Planner

        source = inspect.getsource(Planner)
        assert "getattr(cfg, 'max_expansion_feerate_perkb'" in source


# =============================================================================
# FULL_SYNC: VALIDATION INTEGRATION
# =============================================================================

class TestApplyFullSyncValidation:
    """Verify apply_full_sync validates entries correctly."""

    def test_rejects_available_gt_capacity(self, state_manager):
        """FULL_SYNC entries with available > capacity should be rejected."""
        remote_states = [
            {
                "peer_id": "02" + "a" * 64,
                "capacity_sats": 1000000,
                "available_sats": 2000000,  # Invalid
                "fee_policy": {},
                "topology": [],
                "version": 5,
                "timestamp": int(time.time()),
            }
        ]

        updated = state_manager.apply_full_sync(remote_states)
        assert updated == 0

    def test_accepts_valid_entries(self, state_manager):
        """FULL_SYNC with valid entries should be applied."""
        now = int(time.time())
        remote_states = [
            {
                "peer_id": "02" + "b" * 64,
                "capacity_sats": 2000000,
                "available_sats": 1000000,
                "fee_policy": {"base_fee": 100},
                "topology": ["peer1"],
                "version": 3,
                "timestamp": now,
            }
        ]

        updated = state_manager.apply_full_sync(remote_states)
        assert updated == 1

        state = state_manager.get_peer_state("02" + "b" * 64)
        assert state is not None
        assert state.capacity_sats == 2000000
        assert state.version == 3
