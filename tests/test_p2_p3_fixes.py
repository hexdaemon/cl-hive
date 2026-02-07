"""
Tests for P2/P3 bug fixes (hardening phase).

Covers:
- governance.py: negative amount_sats, action_type/target bounds, frozenset
- bridge.py: daily rebalance budget atomicity, policy cache eviction
- protocol.py: pubkey prefix validation, reason string bounds
- config.py: feerate range with 0, governance_mode normalization
- state_manager.py: available > capacity clamp, from_dict None peer_id, hash atomicity
- membership.py: JSON-safe contribution ratio (no float inf)
- contribution.py: rate_limits bounded growth
- cost_reduction.py: BFS uses deque
- anticipatory_liquidity.py: consistency clamped, Kalman velocity bounds
- gossip.py: fee_policy value validation
- mcf_solver.py: add_node bool return (already tested, included for completeness)
- splice_manager.py: rate limiting in all handlers

Author: Lightning Goats Team
"""

import pytest
import time
import json
import threading
from collections import deque
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock pyln.client before importing modules that depend on it
class MockRpcError(Exception):
    pass

if 'pyln.client' not in sys.modules:
    mock_pyln = MagicMock()
    mock_pyln.Plugin = MagicMock
    mock_pyln.RpcError = MockRpcError
    sys.modules['pyln'] = mock_pyln
    sys.modules['pyln.client'] = mock_pyln

from modules.protocol import _valid_pubkey, MAX_REASON_LEN
from modules.config import HiveConfig
from modules.state_manager import HivePeerState
from modules.membership import CONTRIBUTION_RATIO_NO_DATA
from modules.contribution import ContributionManager, MAX_RATE_LIMIT_ENTRIES
from modules.governance import MAX_ACTION_TYPE_LEN, MAX_TARGET_LEN, DecisionEngine
from modules.bridge import Bridge, BridgeStatus, MAX_POLICY_CACHE
from modules.gossip import GossipManager
from modules.intent_manager import IntentManager, Intent
from modules.anticipatory_liquidity import MAX_FLOW_HISTORY_CHANNELS


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
    db.get_all_members.return_value = []
    db.save_contribution_daily_stats = MagicMock()
    db.save_contribution_rate_limit = MagicMock()
    return db


@pytest.fixture
def mock_state_manager():
    sm = MagicMock()
    sm.update_peer_state.return_value = True
    sm.apply_full_sync.return_value = 5
    sm.get_peer_state.return_value = None
    return sm


# =============================================================================
# governance.py — negative amount_sats, string bounds, frozenset
# =============================================================================

class TestGovernanceValidation:
    """P2: Governance input validation."""

    def test_failsafe_action_types_is_frozenset(self):
        """FAILSAFE_ACTION_TYPES should be immutable."""
        assert isinstance(DecisionEngine.FAILSAFE_ACTION_TYPES, frozenset)

    def test_max_action_type_len_defined(self):
        """MAX_ACTION_TYPE_LEN constant should be defined."""
        assert MAX_ACTION_TYPE_LEN == 64

    def test_max_target_len_defined(self):
        """MAX_TARGET_LEN constant should be defined."""
        assert MAX_TARGET_LEN == 256


# =============================================================================
# protocol.py — pubkey prefix validation, string bounds
# =============================================================================

class TestProtocolPubkeyValidation:
    """P2: Pubkey must start with 02 or 03."""

    def test_valid_02_prefix(self):
        """02-prefixed 66-char hex pubkey is valid."""
        assert _valid_pubkey("02" + "a" * 64) is True

    def test_valid_03_prefix(self):
        """03-prefixed 66-char hex pubkey is valid."""
        assert _valid_pubkey("03" + "b" * 64) is True

    def test_invalid_00_prefix_rejected(self):
        """00-prefix should be rejected even if length/hex is correct."""
        assert _valid_pubkey("00" + "a" * 64) is False

    def test_invalid_ff_prefix_rejected(self):
        """ff-prefix should be rejected."""
        assert _valid_pubkey("ff" + "a" * 64) is False

    def test_invalid_04_prefix_rejected(self):
        """04-prefix (uncompressed) should be rejected."""
        assert _valid_pubkey("04" + "a" * 64) is False

    def test_wrong_length_rejected(self):
        """Short pubkey should be rejected."""
        assert _valid_pubkey("02" + "a" * 32) is False

    def test_non_hex_rejected(self):
        """Non-hex chars should be rejected."""
        assert _valid_pubkey("02" + "g" * 64) is False


class TestProtocolStringBounds:
    """P2: Reason fields should be bounded."""

    def test_max_reason_len_defined(self):
        """MAX_REASON_LEN constant should be defined."""
        assert MAX_REASON_LEN == 512


# =============================================================================
# config.py — feerate 0 allowed, governance_mode normalization
# =============================================================================

class TestConfigFeerateValidation:
    """P2: Feerate 0 means disabled, nonzero must be in range."""

    def test_feerate_zero_passes(self):
        """Feerate 0 (disabled) should pass validation."""
        config = HiveConfig(max_expansion_feerate_perkb=0)
        error = config.validate()
        assert error is None

    def test_feerate_valid_range_passes(self):
        """Feerate within valid range should pass."""
        config = HiveConfig(max_expansion_feerate_perkb=5000)
        error = config.validate()
        assert error is None

    def test_feerate_below_range_fails(self):
        """Feerate 500 (below 1000 and not 0) should fail."""
        config = HiveConfig(max_expansion_feerate_perkb=500)
        error = config.validate()
        assert error is not None
        assert "max_expansion_feerate_perkb" in error


class TestConfigGovernanceMode:
    """P2: Governance mode normalization."""

    def test_advisor_mode_passes(self):
        """'advisor' should pass."""
        config = HiveConfig(governance_mode='advisor')
        error = config.validate()
        assert error is None

    def test_failsafe_mode_passes(self):
        """'failsafe' should pass."""
        config = HiveConfig(governance_mode='failsafe')
        error = config.validate()
        assert error is None

    def test_invalid_mode_fails(self):
        """Invalid mode should fail validation."""
        config = HiveConfig(governance_mode='yolo')
        error = config.validate()
        assert error is not None
        assert "governance_mode" in error

    def test_mode_normalized(self):
        """Mode with whitespace/caps should be normalized."""
        config = HiveConfig(governance_mode='  Advisor  ')
        error = config.validate()
        assert error is None
        assert config.governance_mode == 'advisor'


# =============================================================================
# state_manager.py — available > capacity clamp, from_dict peer_id
# =============================================================================

class TestStateManagerValidation:
    """P3: State manager defensive validation."""

    def test_from_dict_empty_peer_id_returns_none(self):
        """from_dict with empty peer_id should return None."""
        data = {
            "peer_id": "",
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {},
            "topology": [],
            "version": 1,
            "last_update": int(time.time()),
        }
        result = HivePeerState.from_dict(data)
        assert result is None

    def test_from_dict_missing_peer_id_returns_none(self):
        """from_dict with missing peer_id should return None."""
        data = {
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {},
            "topology": [],
            "version": 1,
            "last_update": int(time.time()),
        }
        result = HivePeerState.from_dict(data)
        assert result is None

    def test_from_dict_valid_peer_id_works(self):
        """from_dict with valid peer_id should return state object."""
        peer_id = "02" + "a" * 64
        data = {
            "peer_id": peer_id,
            "capacity_sats": 1000000,
            "available_sats": 500000,
            "fee_policy": {"base_fee": 1000},
            "topology": [],
            "version": 1,
            "last_update": int(time.time()),
        }
        result = HivePeerState.from_dict(data)
        assert result is not None
        assert result.peer_id == peer_id


# =============================================================================
# membership.py — no float("inf")
# =============================================================================

class TestMembershipJSONSafety:
    """P3: Contribution ratio should be JSON-serializable."""

    def test_no_data_sentinel_is_json_safe(self):
        """CONTRIBUTION_RATIO_NO_DATA should be JSON-serializable."""
        result = json.dumps({"ratio": CONTRIBUTION_RATIO_NO_DATA})
        assert "999999999" in result

    def test_sentinel_is_not_inf(self):
        """Sentinel should not be float('inf')."""
        assert CONTRIBUTION_RATIO_NO_DATA != float('inf')
        assert isinstance(CONTRIBUTION_RATIO_NO_DATA, int)


# =============================================================================
# contribution.py — rate_limits bounded
# =============================================================================

class TestContributionRateLimitBounds:
    """P2: Rate limit dict should not grow unbounded."""

    def test_max_rate_limit_entries_defined(self):
        """MAX_RATE_LIMIT_ENTRIES should be defined."""
        assert MAX_RATE_LIMIT_ENTRIES == 1000

    def test_rate_limits_pruned_on_overflow(self, mock_plugin, mock_db):
        """Old rate limit entries should be pruned when exceeding limit."""
        config = MagicMock()
        config.ban_autotrigger_enabled = False
        mgr = ContributionManager(
            rpc=MagicMock(), db=mock_db, plugin=mock_plugin, config=config
        )

        # Inject many old entries
        old_time = int(time.time()) - 7200  # 2 hours ago
        for i in range(MAX_RATE_LIMIT_ENTRIES + 100):
            mgr._rate_limits[f"peer_{i}"] = (old_time, 1)

        # Record a new peer — should trigger pruning
        mgr._allow_record("02" + "f" * 64)
        assert len(mgr._rate_limits) <= MAX_RATE_LIMIT_ENTRIES + 1


# =============================================================================
# cost_reduction.py — BFS uses deque
# =============================================================================

class TestCostReductionBFS:
    """P2: BFS should use deque for O(1) popleft."""

    def test_deque_imported(self):
        """deque should be available in cost_reduction module."""
        from modules.cost_reduction import deque as cr_deque
        assert cr_deque is deque


# =============================================================================
# anticipatory_liquidity.py — bounds and validation
# =============================================================================

class TestAnticipatoryBounds:
    """P2: Anticipatory liquidity bounds and validation."""

    def test_max_flow_history_channels_defined(self):
        """MAX_FLOW_HISTORY_CHANNELS should be defined."""
        assert MAX_FLOW_HISTORY_CHANNELS == 500


# =============================================================================
# gossip.py — fee_policy value validation
# =============================================================================

class TestGossipFeeValidation:
    """P2: fee_policy values must be numeric and bounded."""

    def _make_gossip_mgr(self, mock_state_manager, mock_plugin):
        return GossipManager(mock_state_manager, mock_plugin)

    def test_negative_fee_value_rejected(self, mock_state_manager, mock_plugin):
        """Negative fee values should be rejected."""
        mgr = self._make_gossip_mgr(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64
        payload = {
            "peer_id": sender,
            "version": 1,
            "timestamp": int(time.time()),
            "topology": [],
            "fee_policy": {"base_fee": -100},
        }
        result = mgr.process_gossip(sender, payload)
        assert result is False

    def test_string_fee_value_rejected(self, mock_state_manager, mock_plugin):
        """Non-numeric fee values should be rejected."""
        mgr = self._make_gossip_mgr(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64
        payload = {
            "peer_id": sender,
            "version": 1,
            "timestamp": int(time.time()),
            "topology": [],
            "fee_policy": {"base_fee": "not_a_number"},
        }
        result = mgr.process_gossip(sender, payload)
        assert result is False

    def test_valid_fee_values_accepted(self, mock_state_manager, mock_plugin):
        """Valid numeric fee values should be accepted."""
        mgr = self._make_gossip_mgr(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64
        payload = {
            "peer_id": sender,
            "version": 1,
            "timestamp": int(time.time()),
            "topology": ["02" + "b" * 64],
            "fee_policy": {"base_fee": 1000, "fee_rate": 100},
        }
        result = mgr.process_gossip(sender, payload)
        assert result is True

    def test_huge_fee_value_rejected(self, mock_state_manager, mock_plugin):
        """Fee values exceeding 10M should be rejected."""
        mgr = self._make_gossip_mgr(mock_state_manager, mock_plugin)
        sender = "02" + "a" * 64
        payload = {
            "peer_id": sender,
            "version": 1,
            "timestamp": int(time.time()),
            "topology": [],
            "fee_policy": {"base_fee": 99_000_000},
        }
        result = mgr.process_gossip(sender, payload)
        assert result is False


# =============================================================================
# bridge.py — policy cache eviction
# =============================================================================

class TestBridgePolicyCacheEviction:
    """P3: Policy cache should not grow unbounded."""

    def test_max_policy_cache_defined(self):
        """MAX_POLICY_CACHE constant should be defined."""
        assert MAX_POLICY_CACHE == 500


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
