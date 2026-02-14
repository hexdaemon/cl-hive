"""
Tests for Phase 6: Planner Module (Ticket 6-01)

Tests the Planner class for:
- Network cache refresh and directional dedup
- Saturation calculation with gossip clamping
- Guard mechanism with max ignores/cycle limit
- Governance mode behavior
- Fail-closed on RPC errors

Author: Lightning Goats Team
"""

import pytest
import time
from unittest.mock import MagicMock, patch, PropertyMock
from dataclasses import dataclass

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.planner import (
    Planner, ChannelInfo, SaturationResult, RpcError, ExpansionRecommendation,
    ChannelSizer, ChannelSizeResult,
    MAX_IGNORES_PER_CYCLE, SATURATION_RELEASE_THRESHOLD_PCT,
    MIN_TARGET_CAPACITY_SATS, NETWORK_CACHE_TTL_SECONDS,
    MIN_QUALITY_SCORE,
    # Cooperation module constants (Phase 7)
    HIVE_COVERAGE_MAJORITY_PCT, LOW_COMPETITION_CHANNELS,
    MEDIUM_COMPETITION_CHANNELS, HIGH_COMPETITION_CHANNELS,
    COMPETITION_DISCOUNT_LOW, COMPETITION_DISCOUNT_MEDIUM,
    COMPETITION_DISCOUNT_HIGH, BOTTLENECK_BONUS_MULTIPLIER
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_state_manager():
    """Create a mock StateManager."""
    sm = MagicMock()
    sm.get_all_peer_states.return_value = []
    return sm


@pytest.fixture
def mock_database():
    """Create a mock database."""
    db = MagicMock()
    db.get_all_members.return_value = []
    db.log_planner_action = MagicMock()
    # Mock pending action tracking methods (rejection tracking)
    db.has_pending_action_for_target.return_value = False
    db.was_recently_rejected.return_value = False
    db.get_rejection_count.return_value = 0
    # Mock global constraint tracking (BUG-001 fix)
    db.count_consecutive_expansion_rejections.return_value = 0
    db.get_recent_expansion_rejections.return_value = []
    # Mock budget tracking
    db.get_available_budget.return_value = 2_000_000  # Matches failsafe_budget_per_day
    # Mock ignored peers (planner ignore feature)
    db.is_peer_ignored.return_value = False
    # Mock peer event summary for quality scorer (neutral values)
    db.get_peer_event_summary.return_value = {
        "peer_id": "",
        "event_count": 0,
        "open_count": 0,
        "close_count": 0,
        "remote_close_count": 0,
        "local_close_count": 0,
        "mutual_close_count": 0,
        "total_revenue_sats": 0,
        "total_rebalance_cost_sats": 0,
        "total_net_pnl_sats": 0,
        "total_forward_count": 0,
        "avg_routing_score": 0.5,
        "avg_profitability_score": 0.5,
        "avg_duration_days": 0,
        "reporters": []
    }
    return db


@pytest.fixture
def mock_bridge():
    """Create a mock Bridge."""
    return MagicMock()


@pytest.fixture
def mock_clboss_bridge():
    """Create a mock CLBossBridge."""
    clboss = MagicMock()
    clboss._available = True
    # Modern API methods
    clboss.unmanage_open.return_value = True
    clboss.manage_open.return_value = True
    # Legacy aliases (deprecated but may still be used in tests)
    clboss.ignore_peer.return_value = True
    clboss.unignore_peer.return_value = True
    return clboss


@pytest.fixture
def mock_plugin():
    """Create a mock plugin."""
    plugin = MagicMock()
    plugin.log = MagicMock()
    plugin.rpc = MagicMock()
    return plugin


@pytest.fixture
def mock_config():
    """Create a mock config snapshot."""
    cfg = MagicMock()
    cfg.market_share_cap_pct = 0.20  # 20%
    cfg.governance_mode = 'advisor'
    # Channel size options (new)
    cfg.planner_min_channel_sats = 1_000_000  # 1M sats
    cfg.planner_max_channel_sats = 50_000_000  # 50M sats
    cfg.planner_default_channel_sats = 5_000_000  # 5M sats
    # Global constraint tracking (BUG-001 fix)
    cfg.expansion_pause_threshold = 3  # Pause after 3 consecutive rejections
    cfg.planner_safety_reserve_sats = 500_000  # 500k sats safety reserve
    cfg.planner_fee_buffer_sats = 100_000  # 100k sats for on-chain fees
    # Budget constraints (ensures proposals are within executable limits)
    cfg.failsafe_budget_per_day = 2_000_000  # 2M sats daily budget
    cfg.budget_reserve_pct = 0.20  # 20% reserve
    cfg.budget_max_per_channel_pct = 0.50  # 50% of daily budget per channel (= 1M)
    return cfg


@pytest.fixture
def planner(mock_state_manager, mock_database, mock_bridge, mock_clboss_bridge, mock_plugin):
    """Create a Planner instance with mocked dependencies."""
    return Planner(
        state_manager=mock_state_manager,
        database=mock_database,
        bridge=mock_bridge,
        clboss_bridge=mock_clboss_bridge,
        plugin=mock_plugin
    )


# =============================================================================
# NETWORK CACHE TESTS (Directional Dedup)
# =============================================================================

class TestNetworkCache:
    """Test network cache refresh and deduplication."""

    def test_refresh_network_cache_success(self, planner, mock_plugin):
        """_refresh_network_cache should populate cache from listchannels."""
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'a' * 64,
                    'destination': '02' + 'b' * 64,
                    'short_channel_id': '123x1x0',
                    'satoshis': 1000000,
                    'active': True
                }
            ]
        }

        result = planner._refresh_network_cache(force=True)

        assert result is True
        assert len(planner._network_cache) > 0

    def test_refresh_network_cache_rpc_failure(self, planner, mock_plugin):
        """_refresh_network_cache should return False on RPC error."""
        mock_plugin.rpc.listchannels.side_effect = RpcError('listchannels', {}, 'timeout')

        result = planner._refresh_network_cache(force=True)

        assert result is False

    def test_directional_dedup(self, planner, mock_plugin):
        """Should deduplicate bidirectional channels (A->B and B->A counted once)."""
        # Same channel, both directions
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'a' * 64,
                    'destination': '02' + 'b' * 64,
                    'short_channel_id': '123x1x0',
                    'satoshis': 1000000,
                    'active': True
                },
                {
                    'source': '02' + 'b' * 64,
                    'destination': '02' + 'a' * 64,
                    'short_channel_id': '123x1x0',  # Same channel
                    'satoshis': 1000000,
                    'active': True
                }
            ]
        }

        planner._refresh_network_cache(force=True)

        # Should not double-count
        target_a = '02' + 'a' * 64
        target_b = '02' + 'b' * 64

        # Each target should have exactly 1 channel entry (the deduplicated one)
        channels_to_a = planner._network_cache.get(target_a, [])
        channels_to_b = planner._network_cache.get(target_b, [])

        # The dedup logic should result in consistent counts
        assert len(channels_to_a) == len(channels_to_b)

    def test_cache_ttl_respected(self, planner, mock_plugin):
        """Should not refresh if cache is fresh."""
        mock_plugin.rpc.listchannels.return_value = {'channels': []}

        # First refresh
        planner._refresh_network_cache(force=True)
        call_count_1 = mock_plugin.rpc.listchannels.call_count

        # Second refresh without force (should use cache)
        planner._refresh_network_cache(force=False)
        call_count_2 = mock_plugin.rpc.listchannels.call_count

        assert call_count_2 == call_count_1  # No additional call


# =============================================================================
# SATURATION CALCULATION TESTS
# =============================================================================

class TestSaturationCalculation:
    """Test saturation calculation with gossip clamping."""

    def test_calculate_hive_share_basic(self, planner, mock_database, mock_state_manager, mock_plugin, mock_config):
        """Basic saturation calculation."""
        target = '02' + 'c' * 64
        member1 = '02' + 'a' * 64

        # Setup Hive member
        mock_database.get_all_members.return_value = [
            {'peer_id': member1, 'tier': 'member'}
        ]

        # Setup member state with target in topology
        mock_state = MagicMock()
        mock_state.peer_id = member1
        mock_state.topology = [target]
        mock_state.capacity_sats = 500000  # 500k sats
        mock_state_manager.get_all_peer_states.return_value = [mock_state]

        # Setup network cache with public channels
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': member1,
                    'destination': target,
                    'short_channel_id': '100x1x0',
                    'satoshis': 500000,
                    'active': True
                },
                {
                    'source': '02' + 'd' * 64,  # Non-hive node
                    'destination': target,
                    'short_channel_id': '200x1x0',
                    'satoshis': 2000000,  # 2M sats
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        result = planner._calculate_hive_share(target, mock_config)

        # Hive has 500k out of 2.5M total = 20%
        assert result.hive_capacity_sats == 500000
        assert result.public_capacity_sats == 2500000
        assert abs(result.hive_share_pct - 0.20) < 0.01

    def test_gossip_clamping_to_public_reality(self, planner, mock_database, mock_state_manager, mock_plugin, mock_config):
        """Gossip capacity should be clamped to public listchannels maximum."""
        target = '02' + 'c' * 64
        member1 = '02' + 'a' * 64

        # Setup Hive member
        mock_database.get_all_members.return_value = [
            {'peer_id': member1, 'tier': 'member'}
        ]

        # Gossip claims 10 BTC (inflated!)
        mock_state = MagicMock()
        mock_state.peer_id = member1
        mock_state.topology = [target]
        mock_state.capacity_sats = 1_000_000_000  # 10 BTC - INFLATED
        mock_state_manager.get_all_peer_states.return_value = [mock_state]

        # But public reality shows only 500k sats
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': member1,
                    'destination': target,
                    'short_channel_id': '100x1x0',
                    'satoshis': 500000,  # Only 500k in reality
                    'active': True
                },
                {
                    'source': '02' + 'd' * 64,
                    'destination': target,
                    'short_channel_id': '200x1x0',
                    'satoshis': 2000000,
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        result = planner._calculate_hive_share(target, mock_config)

        # Should be clamped to 500k, not 10 BTC
        assert result.hive_capacity_sats == 500000
        # Share should be 500k / 2.5M = 20%, not 10BTC / (10BTC + 2M) = ~83%
        assert result.hive_share_pct < 0.25

    def test_no_public_channel_ignores_gossip(self, planner, mock_database, mock_state_manager, mock_plugin, mock_config):
        """If no public channel exists, gossip capacity should be ignored."""
        target = '02' + 'c' * 64
        member1 = '02' + 'a' * 64

        mock_database.get_all_members.return_value = [
            {'peer_id': member1, 'tier': 'member'}
        ]

        # Gossip claims capacity to target
        mock_state = MagicMock()
        mock_state.peer_id = member1
        mock_state.topology = [target]
        mock_state.capacity_sats = 5_000_000
        mock_state_manager.get_all_peer_states.return_value = [mock_state]

        # But no public channel exists between member and target
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'd' * 64,  # Different source
                    'destination': target,
                    'short_channel_id': '200x1x0',
                    'satoshis': 2000000,
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        result = planner._calculate_hive_share(target, mock_config)

        # No verified public channel = 0 hive capacity
        assert result.hive_capacity_sats == 0


# =============================================================================
# GUARD MECHANISM TESTS
# =============================================================================

class TestGuardMechanism:
    """Test saturation enforcement (clboss-ignore)."""

    def test_ignore_saturated_target(self, planner, mock_clboss_bridge, mock_database, mock_plugin, mock_config):
        """Should issue clboss-ignore for saturated targets."""
        target = '02' + 'x' * 64

        # Setup network cache with saturated target
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'a' * 64,
                    'destination': target,
                    'short_channel_id': '100x1x0',
                    'satoshis': MIN_TARGET_CAPACITY_SATS,  # Meets minimum
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        # Mock get_saturated_targets to return our target
        with patch.object(planner, 'get_saturated_targets') as mock_get_sat:
            mock_get_sat.return_value = [
                SaturationResult(
                    target=target,
                    hive_capacity_sats=25_000_000,
                    public_capacity_sats=100_000_000,
                    hive_share_pct=0.25,  # 25% > 20% threshold
                    is_saturated=True,
                    should_release=False
                )
            ]

            decisions = planner._enforce_saturation(mock_config, 'test-run-1')

        # Should have called unmanage_open (modern API)
        mock_clboss_bridge.unmanage_open.assert_called_once_with(target)
        assert target in planner._ignored_peers

    def test_max_ignores_per_cycle_limit(self, planner, mock_clboss_bridge, mock_database, mock_plugin, mock_config):
        """Should abort if more than MAX_IGNORES_PER_CYCLE ignores needed."""
        # Setup network cache
        mock_plugin.rpc.listchannels.return_value = {'channels': []}
        planner._refresh_network_cache(force=True)

        # Create more saturated targets than allowed
        too_many_targets = [
            SaturationResult(
                target=f'02{i:064x}'[:66],
                hive_capacity_sats=25_000_000,
                public_capacity_sats=100_000_000,
                hive_share_pct=0.25,
                is_saturated=True,
                should_release=False
            )
            for i in range(MAX_IGNORES_PER_CYCLE + 5)
        ]

        with patch.object(planner, 'get_saturated_targets') as mock_get_sat:
            mock_get_sat.return_value = too_many_targets

            decisions = planner._enforce_saturation(mock_config, 'test-run-2')

        # Should have aborted
        assert any(d.get('action') == 'abort' for d in decisions)
        assert any(d.get('reason') == 'mass_saturation_detected' for d in decisions)

        # Should NOT have called ignore_peer
        mock_clboss_bridge.ignore_peer.assert_not_called()

        # Should have logged the abort
        mock_database.log_planner_action.assert_any_call(
            action_type='saturation_check',
            result='aborted',
            details={
                'reason': 'mass_saturation_detected',
                'targets_count': MAX_IGNORES_PER_CYCLE + 5,
                'max_allowed': MAX_IGNORES_PER_CYCLE,
                'run_id': 'test-run-2'
            }
        )

    def test_idempotent_ignore(self, planner, mock_clboss_bridge, mock_database, mock_plugin, mock_config):
        """Should not re-ignore already-ignored peers."""
        target = '02' + 'y' * 64

        # Mark as already ignored
        planner._ignored_peers.add(target)

        mock_plugin.rpc.listchannels.return_value = {'channels': []}
        planner._refresh_network_cache(force=True)

        with patch.object(planner, 'get_saturated_targets') as mock_get_sat:
            mock_get_sat.return_value = [
                SaturationResult(
                    target=target,
                    hive_capacity_sats=25_000_000,
                    public_capacity_sats=100_000_000,
                    hive_share_pct=0.25,
                    is_saturated=True,
                    should_release=False
                )
            ]

            planner._enforce_saturation(mock_config, 'test-run-3')

        # Should NOT have called ignore_peer (already ignored)
        mock_clboss_bridge.ignore_peer.assert_not_called()

    def test_clboss_unavailable_records_saturation(self, planner, mock_clboss_bridge, mock_database, mock_plugin, mock_config):
        """Should record saturation detection when CLBoss is unavailable (CLBoss is optional)."""
        target = '02' + 'z' * 64
        mock_clboss_bridge._available = False

        mock_plugin.rpc.listchannels.return_value = {'channels': []}
        planner._refresh_network_cache(force=True)

        with patch.object(planner, 'get_saturated_targets') as mock_get_sat:
            mock_get_sat.return_value = [
                SaturationResult(
                    target=target,
                    hive_capacity_sats=25_000_000,
                    public_capacity_sats=100_000_000,
                    hive_share_pct=0.25,
                    is_saturated=True,
                    should_release=False
                )
            ]

            decisions = planner._enforce_saturation(mock_config, 'test-run-4')

        # Should record saturation_detected (CLBoss is optional, so this is informational)
        assert any(d.get('action') == 'saturation_detected' for d in decisions)


# =============================================================================
# FAIL-CLOSED BEHAVIOR TESTS
# =============================================================================

class TestFailClosed:
    """Test fail-closed behavior on errors."""

    def test_rpc_failure_aborts_cycle(self, planner, mock_plugin, mock_config):
        """Should abort cycle if network cache refresh fails."""
        mock_plugin.rpc.listchannels.side_effect = RpcError('listchannels', {}, 'timeout')

        decisions = planner.run_cycle(mock_config, run_id='test-fail')

        # Should return empty (no actions taken)
        assert decisions == []

    def test_no_intents_on_cache_failure(self, planner, mock_plugin, mock_config, mock_database):
        """Should not issue any ignores if cache refresh fails."""
        mock_plugin.rpc.listchannels.side_effect = RpcError('listchannels', {}, 'timeout')

        # Even with mocked saturated targets, should not act
        planner.run_cycle(mock_config, run_id='test-no-action')

        # Verify logged failure
        mock_database.log_planner_action.assert_any_call(
            action_type='cycle',
            result='failed',
            details={'reason': 'cache_refresh_failed', 'run_id': 'test-no-action'}
        )


# =============================================================================
# GOVERNANCE MODE TESTS
# =============================================================================

class TestGovernanceMode:
    """Test governance mode behavior."""

    def test_advisor_mode_queues_only(self, planner, mock_config):
        """In advisor mode, actions should be logged but not necessarily blocked."""
        mock_config.governance_mode = 'advisor'

        # The current implementation still performs ignores in advisor mode
        # (ignoring is defensive, not fund-moving)
        # This test documents the expected behavior
        stats = planner.get_planner_stats()
        assert 'ignored_peers_count' in stats


# =============================================================================
# RUN CYCLE INTEGRATION TESTS
# =============================================================================

class TestRunCycle:
    """Test the main run_cycle method."""

    def test_run_cycle_returns_decisions(self, planner, mock_plugin, mock_config, mock_database):
        """run_cycle should return decision records."""
        mock_plugin.rpc.listchannels.return_value = {'channels': []}

        decisions = planner.run_cycle(mock_config, run_id='test-cycle')

        # Should return a list (may be empty)
        assert isinstance(decisions, list)

        # Should log cycle completion
        mock_database.log_planner_action.assert_called()

    def test_run_cycle_respects_shutdown(self, planner, mock_config):
        """run_cycle should exit early if shutdown_event is set."""
        import threading
        shutdown = threading.Event()
        shutdown.set()

        decisions = planner.run_cycle(mock_config, shutdown_event=shutdown, run_id='test-shutdown')

        assert decisions == []


# =============================================================================
# SATURATION RELEASE TESTS
# =============================================================================

class TestSaturationRelease:
    """Test release of ignores when saturation drops."""

    def test_release_when_below_threshold(self, planner, mock_clboss_bridge, mock_config, mock_plugin, mock_database):
        """Should unignore when share drops below release threshold."""
        target = '02' + 'r' * 64

        # Mark as ignored
        planner._ignored_peers.add(target)

        mock_plugin.rpc.listchannels.return_value = {'channels': []}
        planner._refresh_network_cache(force=True)

        # Mock share calculation to show it's now below threshold
        with patch.object(planner, '_calculate_hive_share') as mock_calc:
            mock_calc.return_value = SaturationResult(
                target=target,
                hive_capacity_sats=10_000_000,
                public_capacity_sats=100_000_000,
                hive_share_pct=0.10,  # 10% < 15% release threshold
                is_saturated=False,
                should_release=True
            )

            decisions = planner._release_saturation(mock_config, 'test-release')

        # Should have called manage_open (modern API)
        mock_clboss_bridge.manage_open.assert_called_once_with(target)
        assert target not in planner._ignored_peers


# =============================================================================
# EXPANSION LOGIC TESTS (Ticket 6-02)
# =============================================================================

class TestExpansionLogic:
    """Test expansion proposal logic."""

    def test_expansion_disabled_by_default(self, planner, mock_config):
        """Expansion should not run when planner_enable_expansions is False."""
        mock_config.planner_enable_expansions = False

        decisions = planner._propose_expansion(mock_config, 'test-disabled')

        assert decisions == []

    def test_expansion_requires_intent_manager(self, planner, mock_config):
        """Expansion should skip if intent_manager is not available."""
        mock_config.planner_enable_expansions = True
        planner.intent_manager = None

        decisions = planner._propose_expansion(mock_config, 'test-no-intent-mgr')

        assert decisions == []

    def test_expansion_requires_sufficient_funds(self, planner, mock_config, mock_plugin):
        """Expansion should skip if onchain balance is insufficient."""
        mock_config.planner_enable_expansions = True

        # Setup mock intent manager
        mock_intent_mgr = MagicMock()
        planner.intent_manager = mock_intent_mgr

        # Mock insufficient funds (50k sats < 2M required with 1M min channel size)
        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [
                {'status': 'confirmed', 'amount_msat': 50000000}  # 50k sats
            ]
        }

        decisions = planner._propose_expansion(mock_config, 'test-low-funds')

        assert decisions == []
        mock_intent_mgr.create_intent.assert_not_called()

    def test_expansion_proposes_to_underserved_target(self, planner, mock_config, mock_plugin, mock_database):
        """Should propose expansion to underserved target when all conditions are met."""
        mock_config.planner_enable_expansions = True
        mock_config.governance_mode = 'advisor'

        target = '02' + 'u' * 64

        # Setup mock intent manager
        mock_intent_mgr = MagicMock()
        mock_intent = MagicMock()
        mock_intent.intent_id = 123
        mock_intent_mgr.create_intent.return_value = mock_intent
        mock_intent_mgr.our_pubkey = '02' + 'a' * 64
        mock_intent_mgr.create_intent_message.return_value = {'intent_type': 'channel_open', 'target': target}
        planner.intent_manager = mock_intent_mgr

        # Mock sufficient funds (10M sats > 2M required with 1M min channel size)
        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [
                {'status': 'confirmed', 'amount_msat': 10000000000}  # 10M sats
            ]
        }

        # Mock underserved targets
        from modules.planner import UnderservedResult
        with patch.object(planner, 'get_underserved_targets') as mock_get_underserved:
            mock_get_underserved.return_value = [
                UnderservedResult(
                    target=target,
                    public_capacity_sats=200_000_000,
                    hive_share_pct=0.02,
                    score=2.0
                )
            ]

            # Mock no pending intents
            mock_database.get_pending_intents.return_value = []

            # Mock members for broadcast
            mock_database.get_all_members.return_value = [
                {'peer_id': '02' + 'b' * 64}
            ]

            decisions = planner._propose_expansion(mock_config, 'test-propose')

        assert len(decisions) == 1
        assert decisions[0]['action'] == 'expansion_proposed'
        assert decisions[0]['target'] == target
        mock_intent_mgr.create_intent.assert_called_once()

    def test_expansion_skips_target_with_pending_intent(self, planner, mock_config, mock_plugin, mock_database):
        """Should skip targets that already have pending intents."""
        mock_config.planner_enable_expansions = True

        target = '02' + 'v' * 64

        # Setup mock intent manager
        mock_intent_mgr = MagicMock()
        planner.intent_manager = mock_intent_mgr

        # Mock sufficient funds
        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [{'status': 'confirmed', 'amount_msat': 10000000000}]
        }

        # Mock underserved targets
        from modules.planner import UnderservedResult
        with patch.object(planner, 'get_underserved_targets') as mock_get_underserved:
            mock_get_underserved.return_value = [
                UnderservedResult(
                    target=target,
                    public_capacity_sats=200_000_000,
                    hive_share_pct=0.02,
                    score=2.0
                )
            ]

            # Mock existing pending intent for target
            mock_database.get_pending_intents.return_value = [
                {'target': target, 'status': 'pending'}
            ]

            decisions = planner._propose_expansion(mock_config, 'test-pending')

        assert decisions == []
        mock_intent_mgr.create_intent.assert_not_called()

    def test_expansion_skips_recently_rejected_target(self, planner, mock_config, mock_plugin, mock_database):
        """Should skip targets that were recently rejected."""
        mock_config.planner_enable_expansions = True
        target = '02' + 'r' * 64

        # Setup mock intent manager
        mock_intent_mgr = MagicMock()
        planner.intent_manager = mock_intent_mgr

        # Mock sufficient funds
        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [{'status': 'confirmed', 'amount_msat': 10000000000}]
        }

        # Mock underserved targets
        from modules.planner import UnderservedResult
        with patch.object(planner, 'get_underserved_targets') as mock_get_underserved:
            mock_get_underserved.return_value = [
                UnderservedResult(
                    target=target,
                    public_capacity_sats=200_000_000,
                    hive_share_pct=0.02,
                    score=2.0
                )
            ]

            # Mock no pending intents
            mock_database.get_pending_intents.return_value = []

            # But the target was recently rejected
            mock_database.was_recently_rejected.return_value = True

            decisions = planner._propose_expansion(mock_config, 'test-rejected')

        # Should skip due to recent rejection
        assert decisions == []
        mock_intent_mgr.create_intent.assert_not_called()

    def test_expansion_skips_target_with_pending_action(self, planner, mock_config, mock_plugin, mock_database):
        """Should skip targets that have a pending action awaiting approval."""
        mock_config.planner_enable_expansions = True
        target = '02' + 'p' * 64

        # Setup mock intent manager
        mock_intent_mgr = MagicMock()
        planner.intent_manager = mock_intent_mgr

        # Mock sufficient funds
        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [{'status': 'confirmed', 'amount_msat': 10000000000}]
        }

        # Mock underserved targets
        from modules.planner import UnderservedResult
        with patch.object(planner, 'get_underserved_targets') as mock_get_underserved:
            mock_get_underserved.return_value = [
                UnderservedResult(
                    target=target,
                    public_capacity_sats=200_000_000,
                    hive_share_pct=0.02,
                    score=2.0
                )
            ]

            # Mock no pending intents
            mock_database.get_pending_intents.return_value = []
            mock_database.was_recently_rejected.return_value = False

            # But target has a pending action awaiting approval
            mock_database.has_pending_action_for_target.return_value = True

            decisions = planner._propose_expansion(mock_config, 'test-pending-action')

        # Should skip due to pending action
        assert decisions == []
        mock_intent_mgr.create_intent.assert_not_called()

    def test_expansion_rate_limit(self, planner, mock_config, mock_plugin, mock_database):
        """Should respect max expansions per cycle limit."""
        mock_config.planner_enable_expansions = True

        # Simulate already at rate limit
        planner._expansions_this_cycle = 1  # MAX_EXPANSIONS_PER_CYCLE is 1

        mock_intent_mgr = MagicMock()
        planner.intent_manager = mock_intent_mgr

        decisions = planner._propose_expansion(mock_config, 'test-rate-limit')

        assert decisions == []
        mock_intent_mgr.create_intent.assert_not_called()

    def test_expansion_advisor_mode_no_broadcast(self, planner, mock_config, mock_plugin, mock_database):
        """In advisor mode, intent should be queued to pending_actions but not broadcast."""
        mock_config.planner_enable_expansions = True
        mock_config.governance_mode = 'advisor'

        target = '02' + 'w' * 64

        # Setup mock intent manager
        mock_intent_mgr = MagicMock()
        mock_intent = MagicMock()
        mock_intent.intent_id = 456
        mock_intent_mgr.create_intent.return_value = mock_intent
        planner.intent_manager = mock_intent_mgr

        # Mock sufficient funds
        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [{'status': 'confirmed', 'amount_msat': 10000000000}]
        }

        # Mock add_pending_action to return an action ID
        mock_database.add_pending_action.return_value = 99

        from modules.planner import UnderservedResult
        with patch.object(planner, 'get_underserved_targets') as mock_get_underserved:
            mock_get_underserved.return_value = [
                UnderservedResult(
                    target=target,
                    public_capacity_sats=200_000_000,
                    hive_share_pct=0.02,
                    score=2.0
                )
            ]

            mock_database.get_pending_intents.return_value = []

            decisions = planner._propose_expansion(mock_config, 'test-advisor')

        assert len(decisions) == 1
        assert decisions[0]['broadcast'] is False
        assert decisions[0]['pending_action_id'] == 99

        # Verify add_pending_action was called with correct args
        mock_database.add_pending_action.assert_called_once()
        call_args = mock_database.add_pending_action.call_args
        assert call_args[1]['action_type'] == 'channel_open'
        assert call_args[1]['payload']['intent_id'] == 456
        assert call_args[1]['payload']['target'] == target


class TestPlannerGovernanceIntegration:
    """Test Planner-Governance integration (Issue #14)."""

    def test_expansion_with_decision_engine_advisor(self, mock_config, mock_plugin, mock_database, mock_state_manager, mock_clboss_bridge):
        """Planner should use DecisionEngine for governance in advisor mode."""
        from modules.governance import DecisionEngine, DecisionResult

        mock_config.planner_enable_expansions = True
        mock_config.governance_mode = 'advisor'

        # Create mock decision engine
        mock_decision_engine = MagicMock(spec=DecisionEngine)
        mock_response = MagicMock()
        mock_response.result = DecisionResult.APPROVED
        mock_response.action_id = None
        mock_response.reason = "Within limits"
        mock_decision_engine.propose_action.return_value = mock_response

        # Create planner with decision engine
        planner = Planner(
            state_manager=mock_state_manager,
            database=mock_database,
            bridge=MagicMock(),
            clboss_bridge=mock_clboss_bridge,
            plugin=mock_plugin,
            intent_manager=MagicMock(),
            decision_engine=mock_decision_engine
        )

        target = '02' + 'g' * 64

        # Setup mocks
        mock_intent = MagicMock()
        mock_intent.intent_id = 789
        planner.intent_manager.create_intent.return_value = mock_intent

        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [{'status': 'confirmed', 'amount_msat': 10000000000}]
        }
        # Mock budget check to allow channel opens (needs to be > min_channel_sats after per-channel limit)
        # With default budget_max_per_channel_pct=0.5, we need daily budget of 2M to get 1M per-channel limit
        mock_database.get_available_budget.return_value = 2_000_000
        mock_config.failsafe_budget_per_day = 2_000_000  # Results in max_per_channel = 1M

        from modules.planner import UnderservedResult
        with patch.object(planner, 'get_underserved_targets') as mock_get_underserved:
            mock_get_underserved.return_value = [
                UnderservedResult(
                    target=target,
                    public_capacity_sats=200_000_000,
                    hive_share_pct=0.02,
                    score=2.0
                )
            ]
            mock_database.get_pending_intents.return_value = []

            decisions = planner._propose_expansion(mock_config, 'test-gov-integration')

        assert len(decisions) == 1
        assert decisions[0]['broadcast'] is True
        assert decisions[0]['governance_result'] == 'approved'

        # Verify DecisionEngine was called
        mock_decision_engine.propose_action.assert_called_once()

    def test_expansion_with_decision_engine_queued(self, mock_config, mock_plugin, mock_database, mock_state_manager, mock_clboss_bridge):
        """Planner should handle QUEUED result from DecisionEngine."""
        from modules.governance import DecisionEngine, DecisionResult

        mock_config.planner_enable_expansions = True
        mock_config.governance_mode = 'advisor'

        # Create mock decision engine that returns QUEUED
        mock_decision_engine = MagicMock(spec=DecisionEngine)
        mock_response = MagicMock()
        mock_response.result = DecisionResult.QUEUED
        mock_response.action_id = 42
        mock_response.reason = "Queued for approval"
        mock_decision_engine.propose_action.return_value = mock_response

        # Create planner with decision engine
        planner = Planner(
            state_manager=mock_state_manager,
            database=mock_database,
            bridge=MagicMock(),
            clboss_bridge=mock_clboss_bridge,
            plugin=mock_plugin,
            intent_manager=MagicMock(),
            decision_engine=mock_decision_engine
        )

        target = '02' + 'h' * 64

        # Setup mocks
        mock_intent = MagicMock()
        mock_intent.intent_id = 999
        planner.intent_manager.create_intent.return_value = mock_intent

        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [{'status': 'confirmed', 'amount_msat': 10000000000}]
        }
        # Mock budget check to allow channel opens (needs to be > min_channel_sats after per-channel limit)
        # With default budget_max_per_channel_pct=0.5, we need daily budget of 2M to get 1M per-channel limit
        mock_database.get_available_budget.return_value = 2_000_000
        mock_config.failsafe_budget_per_day = 2_000_000  # Results in max_per_channel = 1M

        from modules.planner import UnderservedResult
        with patch.object(planner, 'get_underserved_targets') as mock_get_underserved:
            mock_get_underserved.return_value = [
                UnderservedResult(
                    target=target,
                    public_capacity_sats=200_000_000,
                    hive_share_pct=0.03,
                    score=1.9
                )
            ]
            mock_database.get_pending_intents.return_value = []

            decisions = planner._propose_expansion(mock_config, 'test-gov-queued')

        assert len(decisions) == 1
        assert decisions[0]['broadcast'] is False
        assert decisions[0]['governance_result'] == 'queued'
        assert decisions[0]['pending_action_id'] == 42


class TestUnderservedTargets:
    """Test underserved target identification."""

    def test_get_underserved_targets_basic(self, planner, mock_config, mock_plugin, mock_database, mock_state_manager):
        """Should identify targets with low Hive share."""
        target = '02' + 'x' * 64

        # Setup network cache with high-capacity target
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'd' * 64,
                    'destination': target,
                    'short_channel_id': '100x1x0',
                    'satoshis': 200_000_000,  # 2 BTC
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        # No Hive members to calculate share
        mock_database.get_all_members.return_value = []
        mock_state_manager.get_all_peer_states.return_value = []

        underserved = planner.get_underserved_targets(mock_config)

        # Should find targets since Hive share is 0%
        # Both source and destination are indexed, so we may get both
        assert len(underserved) >= 1
        # Our specific target should be in the results
        target_results = [u for u in underserved if u.target == target]
        assert len(target_results) == 1
        assert target_results[0].hive_share_pct == 0.0

    def test_get_underserved_skips_small_targets(self, planner, mock_config, mock_plugin):
        """Should skip targets below minimum capacity."""
        small_target = '02' + 'y' * 64

        # Setup network cache with small target
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'd' * 64,
                    'destination': small_target,
                    'short_channel_id': '100x1x0',
                    'satoshis': 50_000_000,  # 0.5 BTC < 1 BTC threshold
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        underserved = planner.get_underserved_targets(mock_config)

        # Should not find the target (too small)
        assert len(underserved) == 0


# =============================================================================
# COOPERATION MODULE INTEGRATION TESTS (Phase 7)
# =============================================================================

class TestCooperationModuleIntegration:
    """Test cooperation module integration for smarter topology decisions."""

    def test_count_hive_members_with_target_basic(self, planner, mock_database, mock_state_manager):
        """Should count how many hive members have channels to a target."""
        target = '02' + 't' * 64
        member1 = '02' + 'a' * 64
        member2 = '02' + 'b' * 64
        member3 = '02' + 'c' * 64

        # Setup 3 hive members (all members, admin tier removed)
        mock_database.get_all_members.return_value = [
            {'peer_id': member1, 'tier': 'member'},
            {'peer_id': member2, 'tier': 'member'},
            {'peer_id': member3, 'tier': 'member'}
        ]

        # Only 2 of them have the target in their topology
        mock_state1 = MagicMock()
        mock_state1.peer_id = member1
        mock_state1.topology = [target, '02' + 'd' * 64]

        mock_state2 = MagicMock()
        mock_state2.peer_id = member2
        mock_state2.topology = [target]

        mock_state3 = MagicMock()
        mock_state3.peer_id = member3
        mock_state3.topology = ['02' + 'e' * 64]  # Different peer

        mock_state_manager.get_all_peer_states.return_value = [
            mock_state1, mock_state2, mock_state3
        ]

        members_with, total = planner._count_hive_members_with_target(target)

        assert members_with == 2
        assert total == 3

    def test_count_hive_members_no_state_manager(self, planner, mock_database):
        """Should return 0,0 if state_manager is not available."""
        planner.state_manager = None
        target = '02' + 't' * 64

        members_with, total = planner._count_hive_members_with_target(target)

        assert members_with == 0
        assert total == 0

    def test_competition_score_low(self, planner, mock_plugin):
        """Low competition peers should get no discount."""
        target = '02' + 'l' * 64

        # Setup network cache with 20 channels (< LOW_COMPETITION_CHANNELS)
        channels = []
        for i in range(20):
            channels.append({
                'source': target,
                'destination': f'02{i:064x}'[:66],
                'short_channel_id': f'{100+i}x1x0',
                'satoshis': 1000000,
                'active': True
            })
        mock_plugin.rpc.listchannels.return_value = {'channels': channels}
        planner._refresh_network_cache(force=True)

        discount, level = planner._calculate_competition_score(target)

        assert discount == COMPETITION_DISCOUNT_LOW
        assert level == "low"

    def test_competition_score_medium(self, planner, mock_plugin):
        """Medium competition peers should get 15% discount."""
        target = '02' + 'm' * 64

        # Setup network cache with 50 channels
        channels = []
        for i in range(50):
            channels.append({
                'source': target,
                'destination': f'02{i:064x}'[:66],
                'short_channel_id': f'{100+i}x1x0',
                'satoshis': 1000000,
                'active': True
            })
        mock_plugin.rpc.listchannels.return_value = {'channels': channels}
        planner._refresh_network_cache(force=True)

        discount, level = planner._calculate_competition_score(target)

        assert discount == COMPETITION_DISCOUNT_MEDIUM
        assert level == "medium"

    def test_competition_score_high(self, planner, mock_plugin):
        """High competition peers should get 35% discount."""
        target = '02' + 'h' * 64

        # Setup network cache with 150 channels
        channels = []
        for i in range(150):
            channels.append({
                'source': target,
                'destination': f'02{i:064x}'[:66],
                'short_channel_id': f'{100+i}x1x0',
                'satoshis': 1000000,
                'active': True
            })
        mock_plugin.rpc.listchannels.return_value = {'channels': channels}
        planner._refresh_network_cache(force=True)

        discount, level = planner._calculate_competition_score(target)

        assert discount == COMPETITION_DISCOUNT_HIGH
        assert level == "high"

    def test_competition_score_very_high(self, planner, mock_plugin):
        """Very high competition peers should get 50% discount."""
        target = '02' + 'v' * 64

        # Setup network cache with 300 channels (> HIGH_COMPETITION_CHANNELS)
        channels = []
        for i in range(300):
            channels.append({
                'source': target,
                'destination': f'02{i:064x}'[:66],
                'short_channel_id': f'{100+i}x1x0',
                'satoshis': 1000000,
                'active': True
            })
        mock_plugin.rpc.listchannels.return_value = {'channels': channels}
        planner._refresh_network_cache(force=True)

        discount, level = planner._calculate_competition_score(target)

        assert discount == 0.50
        assert level == "very_high"

    def test_bottleneck_peer_detection(self, planner):
        """Should detect bottleneck peers from liquidity_coordinator."""
        target = '02' + 'b' * 64

        # Setup mock liquidity coordinator
        mock_liq_coord = MagicMock()
        mock_liq_coord._get_common_bottleneck_peers.return_value = [
            target, '02' + 'x' * 64
        ]
        planner.liquidity_coordinator = mock_liq_coord

        is_bottleneck = planner._is_bottleneck_peer(target)

        assert is_bottleneck is True

    def test_bottleneck_peer_not_in_list(self, planner):
        """Should return False if peer is not in bottleneck list."""
        target = '02' + 'n' * 64

        # Setup mock liquidity coordinator with different bottlenecks
        mock_liq_coord = MagicMock()
        mock_liq_coord._get_common_bottleneck_peers.return_value = [
            '02' + 'x' * 64, '02' + 'y' * 64
        ]
        planner.liquidity_coordinator = mock_liq_coord

        is_bottleneck = planner._is_bottleneck_peer(target)

        assert is_bottleneck is False

    def test_bottleneck_peer_no_coordinator(self, planner):
        """Should return False if liquidity_coordinator is not available."""
        target = '02' + 'n' * 64
        planner.liquidity_coordinator = None

        is_bottleneck = planner._is_bottleneck_peer(target)

        assert is_bottleneck is False

    def test_majority_coverage_filters_targets(
        self, planner, mock_config, mock_plugin, mock_database, mock_state_manager
    ):
        """Should filter out targets where majority of hive already has channels."""
        target = '02' + 'f' * 64
        member1 = '02' + 'a' * 64
        member2 = '02' + 'b' * 64

        # Setup 2 hive members, both with channels to target (100% coverage)
        mock_database.get_all_members.return_value = [
            {'peer_id': member1, 'tier': 'member'},
            {'peer_id': member2, 'tier': 'member'}
        ]

        mock_state1 = MagicMock()
        mock_state1.peer_id = member1
        mock_state1.topology = [target]
        mock_state1.capacity_sats = 5000000

        mock_state2 = MagicMock()
        mock_state2.peer_id = member2
        mock_state2.topology = [target]
        mock_state2.capacity_sats = 5000000

        mock_state_manager.get_all_peer_states.return_value = [mock_state1, mock_state2]

        # Setup network cache with target having >1 BTC capacity
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'd' * 64,
                    'destination': target,
                    'short_channel_id': '100x1x0',
                    'satoshis': 200_000_000,  # 2 BTC
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        underserved = planner.get_underserved_targets(mock_config)

        # Target should be filtered out due to majority coverage
        target_results = [u for u in underserved if u.target == target]
        assert len(target_results) == 0

    def test_expansion_recommendation_open_channel(
        self, planner, mock_config, mock_plugin, mock_database, mock_state_manager
    ):
        """Should recommend open_channel for targets with low hive coverage."""
        target = '02' + 'o' * 64
        member1 = '02' + 'a' * 64

        # Setup 3 hive members, only 1 with channel to target (33% coverage)
        mock_database.get_all_members.return_value = [
            {'peer_id': member1, 'tier': 'member'},
            {'peer_id': '02' + 'b' * 64, 'tier': 'member'},
            {'peer_id': '02' + 'c' * 64, 'tier': 'member'}
        ]

        mock_state1 = MagicMock()
        mock_state1.peer_id = member1
        mock_state1.topology = [target]
        mock_state1.capacity_sats = 5000000

        mock_state_manager.get_all_peer_states.return_value = [mock_state1]

        # Setup network cache
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'd' * 64,
                    'destination': target,
                    'short_channel_id': '100x1x0',
                    'satoshis': 200_000_000,
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        rec = planner.get_expansion_recommendation(target, mock_config)

        assert rec.recommendation_type == "open_channel"
        assert rec.hive_coverage_pct < HIVE_COVERAGE_MAJORITY_PCT

    def test_expansion_recommendation_no_action_majority_coverage(
        self, planner, mock_config, mock_plugin, mock_database, mock_state_manager
    ):
        """Should recommend no_action when majority has channels."""
        target = '02' + 'n' * 64
        member1 = '02' + 'a' * 64
        member2 = '02' + 'b' * 64

        # Setup 2 hive members, both with channels (100% > 50%)
        mock_database.get_all_members.return_value = [
            {'peer_id': member1, 'tier': 'member'},
            {'peer_id': member2, 'tier': 'member'}
        ]

        mock_state1 = MagicMock()
        mock_state1.peer_id = member1
        mock_state1.topology = [target]
        mock_state1.capacity_sats = 5000000

        mock_state2 = MagicMock()
        mock_state2.peer_id = member2
        mock_state2.topology = [target]
        mock_state2.capacity_sats = 5000000

        mock_state_manager.get_all_peer_states.return_value = [mock_state1, mock_state2]

        # Setup network cache
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'd' * 64,
                    'destination': target,
                    'short_channel_id': '100x1x0',
                    'satoshis': 200_000_000,
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        rec = planner.get_expansion_recommendation(target, mock_config)

        assert rec.recommendation_type == "no_action"
        assert rec.hive_coverage_pct >= HIVE_COVERAGE_MAJORITY_PCT

    def test_expansion_recommendation_bottleneck_bonus(
        self, planner, mock_config, mock_plugin, mock_database, mock_state_manager
    ):
        """Bottleneck peers should get score boost."""
        target = '02' + 'b' * 64

        # Setup hive with no members having channels to target
        mock_database.get_all_members.return_value = [
            {'peer_id': '02' + 'a' * 64, 'tier': 'member'}
        ]
        mock_state_manager.get_all_peer_states.return_value = []

        # Setup network cache
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'd' * 64,
                    'destination': target,
                    'short_channel_id': '100x1x0',
                    'satoshis': 200_000_000,
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        # Setup mock liquidity coordinator with bottleneck
        mock_liq_coord = MagicMock()
        mock_liq_coord._get_common_bottleneck_peers.return_value = [target]
        planner.liquidity_coordinator = mock_liq_coord

        rec = planner.get_expansion_recommendation(target, mock_config)

        assert rec.is_bottleneck is True
        assert "bottleneck" in rec.reasoning.lower()
        assert rec.details['bottleneck_bonus'] == BOTTLENECK_BONUS_MULTIPLIER

    def test_set_cooperation_modules(self, planner):
        """Should set cooperation modules via setter."""
        mock_liq = MagicMock()
        mock_splice = MagicMock()
        mock_health = MagicMock()

        planner.set_cooperation_modules(
            liquidity_coordinator=mock_liq,
            splice_coordinator=mock_splice,
            health_aggregator=mock_health
        )

        assert planner.liquidity_coordinator == mock_liq
        assert planner.splice_coordinator == mock_splice
        assert planner.health_aggregator == mock_health


# =============================================================================
# CHANNEL SIZER TESTS (Phase 6.3)
# =============================================================================

class TestChannelSizer:
    """Tests for the ChannelSizer intelligent sizing engine."""

    def _default_params(self, **overrides):
        """Return default params for ChannelSizer.calculate_size()."""
        params = dict(
            target='02' + 'a' * 64,
            target_capacity_sats=5_000_000_000,  # 50 BTC (mid-size)
            target_channel_count=50,
            hive_share_pct=0.01,
            target_share_cap=0.10,
            onchain_balance_sats=100_000_000,  # 1 BTC
            min_channel_sats=1_000_000,
            max_channel_sats=50_000_000,
            default_channel_sats=5_000_000,
            avg_fee_rate_ppm=500,
            quality_score=0.5,
            quality_confidence=0.5,
            quality_recommendation='neutral',
        )
        params.update(overrides)
        return params

    def test_default_baseline_within_bounds(self):
        """Default sizing should produce result between min and max."""
        sizer = ChannelSizer()
        result = sizer.calculate_size(**self._default_params())
        assert result.recommended_size_sats >= 1_000_000
        assert result.recommended_size_sats <= 50_000_000

    def test_mid_size_node_preferred(self):
        """Mid-size node (50 BTC) should score higher than very large (5000 BTC)."""
        sizer = ChannelSizer()
        mid = sizer.calculate_size(**self._default_params(
            target_capacity_sats=50_00_000_000,  # 50 BTC
            target_channel_count=50
        ))
        large = sizer.calculate_size(**self._default_params(
            target_capacity_sats=500_000_000_000,  # 5000 BTC
            target_channel_count=500
        ))
        assert mid.recommended_size_sats >= large.recommended_size_sats

    def test_excellent_quality_bonus(self):
        """Excellent quality (0.9) should size larger than neutral (0.5)."""
        sizer = ChannelSizer()
        excellent = sizer.calculate_size(**self._default_params(
            quality_score=0.9, quality_confidence=0.8, quality_recommendation='excellent'
        ))
        neutral = sizer.calculate_size(**self._default_params(
            quality_score=0.5, quality_confidence=0.8, quality_recommendation='neutral'
        ))
        assert excellent.recommended_size_sats > neutral.recommended_size_sats

    def test_caution_quality_reduction(self):
        """Caution quality (0.2) should size smaller than neutral (0.5)."""
        sizer = ChannelSizer()
        caution = sizer.calculate_size(**self._default_params(
            quality_score=0.2, quality_confidence=0.8, quality_recommendation='caution'
        ))
        neutral = sizer.calculate_size(**self._default_params(
            quality_score=0.5, quality_confidence=0.8, quality_recommendation='neutral'
        ))
        assert caution.recommended_size_sats < neutral.recommended_size_sats

    def test_budget_limited_sizing(self):
        """Channel size should be capped at available budget."""
        sizer = ChannelSizer()
        result = sizer.calculate_size(**self._default_params(
            available_budget_sats=2_000_000
        ))
        assert result.recommended_size_sats <= 2_000_000

    def test_liquidity_constrained_sizing(self):
        """Low balance should produce smaller channel size."""
        sizer = ChannelSizer()
        low_balance = sizer.calculate_size(**self._default_params(
            onchain_balance_sats=3_000_000  # Very tight
        ))
        high_balance = sizer.calculate_size(**self._default_params(
            onchain_balance_sats=500_000_000  # Flush
        ))
        assert low_balance.recommended_size_sats <= high_balance.recommended_size_sats

    def test_zero_capacity_target(self):
        """Zero capacity target should produce a low capacity score."""
        sizer = ChannelSizer()
        result = sizer.calculate_size(**self._default_params(
            target_capacity_sats=0
        ))
        assert result.factors['capacity_score'] == 0.5
        assert result.factors['target_capacity_btc'] == 0.0

    def test_zero_channels_low_routing(self):
        """Target with zero channels should have low routing score."""
        sizer = ChannelSizer()
        result = sizer.calculate_size(**self._default_params(
            target_channel_count=0
        ))
        assert result.factors['routing_score'] < 1.0

    def test_low_confidence_quality_neutral(self):
        """Low confidence quality should use neutral factor (1.0)."""
        sizer = ChannelSizer()
        result = sizer.calculate_size(**self._default_params(
            quality_score=0.9, quality_confidence=0.1
        ))
        assert result.factors['quality_factor'] == 1.0
        assert result.factors.get('quality_note') == 'low_confidence_neutral'

    def test_insufficient_budget_flagged(self):
        """Budget below minimum should be flagged in factors."""
        sizer = ChannelSizer()
        result = sizer.calculate_size(**self._default_params(
            available_budget_sats=500_000,  # Below min_channel_sats of 1M
            min_channel_sats=1_000_000
        ))
        assert result.factors.get('insufficient_budget') is True

    def test_share_gap_influences_size(self):
        """Larger share gap (more underserved) should produce larger channel."""
        sizer = ChannelSizer()
        underserved = sizer.calculate_size(**self._default_params(
            hive_share_pct=0.0, target_share_cap=0.10
        ))
        well_served = sizer.calculate_size(**self._default_params(
            hive_share_pct=0.09, target_share_cap=0.10
        ))
        assert underserved.recommended_size_sats >= well_served.recommended_size_sats


# =============================================================================
# QUALITY SCORE VARIATION TESTS (Phase 6.2)
# =============================================================================

class TestQualityScoreVariation:
    """Tests for quality score filtering in get_underserved_targets()."""

    def _setup_planner_with_target(self, planner, mock_plugin, mock_database,
                                    mock_state_manager, target, capacity_sats=200_000_000):
        """Setup a planner with a target in the network cache."""
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [{
                'source': '02' + 'd' * 64,
                'destination': target,
                'short_channel_id': '100x1x0',
                'satoshis': capacity_sats,
                'active': True
            }]
        }
        planner._refresh_network_cache(force=True)

        # No existing channels
        mock_plugin.rpc.listpeerchannels.return_value = {'channels': []}

        # No hive members with channels to target (underserved)
        mock_database.get_all_members.return_value = [
            {'peer_id': '02' + 'a' * 64, 'tier': 'member'}
        ]
        mock_state_manager.get_all_peer_states.return_value = []

    @staticmethod
    def _filter_target(results, target):
        """Filter results for a specific target pubkey."""
        return [r for r in results if r.target == target]

    def _make_quality_result(self, score, confidence, recommendation):
        """Create a mock quality result."""
        result = MagicMock()
        result.overall_score = score
        result.confidence = confidence
        result.recommendation = recommendation
        return result

    def test_high_quality_scores_higher(self, planner, mock_config, mock_plugin,
                                         mock_database, mock_state_manager):
        """High quality target should score higher than neutral."""
        target = '02' + 'e' * 64
        self._setup_planner_with_target(planner, mock_plugin, mock_database,
                                         mock_state_manager, target)

        # Mock quality scorer returning high quality
        mock_scorer = MagicMock()
        mock_scorer.calculate_score.return_value = self._make_quality_result(0.85, 0.8, 'excellent')
        planner.quality_scorer = mock_scorer

        results_high = self._filter_target(planner.get_underserved_targets(mock_config), target)

        # Now test with neutral quality
        mock_scorer.calculate_score.return_value = self._make_quality_result(0.5, 0.8, 'neutral')
        results_neutral = self._filter_target(planner.get_underserved_targets(mock_config), target)

        assert len(results_high) == 1
        assert len(results_neutral) == 1
        # High quality should produce a higher combined score
        assert results_high[0].score > results_neutral[0].score

    def test_avoid_recommendation_filtered(self, planner, mock_config, mock_plugin,
                                            mock_database, mock_state_manager):
        """Target with 'avoid' recommendation should be filtered out."""
        target = '02' + 'e' * 64
        self._setup_planner_with_target(planner, mock_plugin, mock_database,
                                         mock_state_manager, target)

        mock_scorer = MagicMock()
        mock_scorer.calculate_score.return_value = self._make_quality_result(0.2, 0.8, 'avoid')
        planner.quality_scorer = mock_scorer

        results = self._filter_target(planner.get_underserved_targets(mock_config), target)
        assert len(results) == 0

    def test_low_quality_included_when_flag_set(self, planner, mock_config, mock_plugin,
                                                 mock_database, mock_state_manager):
        """Low quality target should be included when include_low_quality=True."""
        target = '02' + 'e' * 64
        self._setup_planner_with_target(planner, mock_plugin, mock_database,
                                         mock_state_manager, target)

        mock_scorer = MagicMock()
        mock_scorer.calculate_score.return_value = self._make_quality_result(0.2, 0.8, 'avoid')
        planner.quality_scorer = mock_scorer

        results = self._filter_target(
            planner.get_underserved_targets(mock_config, include_low_quality=True), target
        )
        assert len(results) == 1

    def test_below_min_quality_with_high_confidence_filtered(self, planner, mock_config,
                                                              mock_plugin, mock_database,
                                                              mock_state_manager):
        """Below MIN_QUALITY_SCORE with sufficient confidence should be filtered."""
        target = '02' + 'e' * 64
        self._setup_planner_with_target(planner, mock_plugin, mock_database,
                                         mock_state_manager, target)

        mock_scorer = MagicMock()
        # Score below MIN_QUALITY_SCORE (0.45), high confidence, not 'avoid'
        mock_scorer.calculate_score.return_value = self._make_quality_result(0.3, 0.8, 'caution')
        planner.quality_scorer = mock_scorer

        results = self._filter_target(planner.get_underserved_targets(mock_config), target)
        assert len(results) == 0

    def test_below_min_quality_with_low_confidence_passes(self, planner, mock_config,
                                                           mock_plugin, mock_database,
                                                           mock_state_manager):
        """Below MIN_QUALITY_SCORE with low confidence should pass (neutral treatment)."""
        target = '02' + 'e' * 64
        self._setup_planner_with_target(planner, mock_plugin, mock_database,
                                         mock_state_manager, target)

        mock_scorer = MagicMock()
        # Score below threshold but LOW confidence - should not filter
        mock_scorer.calculate_score.return_value = self._make_quality_result(0.3, 0.1, 'caution')
        planner.quality_scorer = mock_scorer

        results = self._filter_target(planner.get_underserved_targets(mock_config), target)
        assert len(results) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
