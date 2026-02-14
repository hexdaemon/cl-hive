"""
Simulation & Game Theory Tests for the Planner (Ticket 6-05)

Tests multi-node scenarios and edge cases:
1. The Stalemate: Two nodes propose the same target simultaneously
2. The Flap: Hysteresis verification at saturation threshold

Author: Lightning Goats Team
"""

import sys
import time
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from modules.planner import (
    Planner,
    SaturationResult,
    UnderservedResult,
    SATURATION_RELEASE_THRESHOLD_PCT,
    MAX_IGNORES_PER_CYCLE,
)
from modules.intent_manager import IntentManager, Intent, IntentType


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_rpc():
    """Mock RPC interface."""
    rpc = MagicMock()
    rpc.listchannels.return_value = {'channels': []}
    # Return enough funds for expansion (10M sats confirmed)
    rpc.listfunds.return_value = {
        'outputs': [{'status': 'confirmed', 'amount_msat': 10_000_000_000}]
    }
    return rpc


@pytest.fixture
def mock_plugin(mock_rpc):
    """Mock plugin with RPC."""
    plugin = MagicMock()
    plugin.rpc = mock_rpc
    return plugin


@pytest.fixture
def mock_database():
    """Mock database."""
    db = MagicMock()
    db.get_all_members.return_value = []
    db.get_pending_intents.return_value = []
    db.create_intent.return_value = 1
    # Mock pending action tracking methods (rejection tracking)
    db.has_pending_action_for_target.return_value = False
    db.was_recently_rejected.return_value = False
    db.get_rejection_count.return_value = 0
    # Mock global constraint tracking (BUG-001 fix)
    db.count_consecutive_expansion_rejections.return_value = 0
    db.get_recent_expansion_rejections.return_value = []
    # Mock budget tracking
    db.get_available_budget.return_value = 2_000_000
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
def mock_state_manager():
    """Mock state manager."""
    sm = MagicMock()
    sm.get_all_peer_states.return_value = []
    return sm


@pytest.fixture
def mock_bridge():
    """Mock integration bridge."""
    bridge = MagicMock()
    bridge._state = 'ENABLED'
    return bridge


@pytest.fixture
def mock_clboss_bridge():
    """Mock CLBoss bridge."""
    cb = MagicMock()
    cb._available = True
    cb.ignore_peer.return_value = True
    cb.unignore_peer.return_value = True
    return cb


@pytest.fixture
def mock_config():
    """Mock config snapshot."""
    cfg = MagicMock()
    cfg.market_share_cap_pct = 0.20
    cfg.governance_mode = 'advisor'
    cfg.planner_enable_expansions = True
    # Channel size options
    cfg.planner_min_channel_sats = 1_000_000  # 1M sats
    cfg.planner_max_channel_sats = 50_000_000  # 50M sats
    cfg.planner_default_channel_sats = 5_000_000  # 5M sats
    # Global constraint tracking (BUG-001 fix)
    cfg.expansion_pause_threshold = 3  # Pause after 3 consecutive rejections
    cfg.planner_safety_reserve_sats = 500_000  # 500k sats safety reserve
    cfg.planner_fee_buffer_sats = 100_000  # 100k sats for on-chain fees
    # Budget constraints (needed for pre-intent budget validation)
    cfg.failsafe_budget_per_day = 10_000_000  # 10M sats daily budget
    cfg.budget_reserve_pct = 0.20  # 20% reserve
    cfg.budget_max_per_channel_pct = 0.50  # 50% of daily budget per channel
    return cfg


# =============================================================================
# THE STALEMATE TEST
# =============================================================================

class TestTheStalemate:
    """
    Test simultaneous intent proposals from multiple nodes.

    Scenario: Two nodes (Alice and Bob) have funds and both identify
    the same Underserved target. Both propose expansion at the exact
    same second. The IntentManager tie-breaker must resolve this
    without a crash or double-spend.
    """

    def test_intent_tiebreaker_deterministic(self, mock_database):
        """
        Two nodes create conflicting intents - tie-breaker should be deterministic.

        The node with the lexicographically LOWEST pubkey wins.
        check_conflicts returns (has_conflict, we_win).
        """
        # Alice has a "lower" pubkey (will win)
        alice_pubkey = '02' + 'a' * 64
        # Bob has a "higher" pubkey (will lose)
        bob_pubkey = '02' + 'b' * 64

        target = '02' + 't' * 64
        now = int(time.time())

        # Create intents from both nodes
        alice_intent = Intent(
            intent_type=IntentType.CHANNEL_OPEN.value,
            target=target,
            initiator=alice_pubkey,
            timestamp=now,
            expires_at=now + 60,
            status='pending'
        )

        bob_intent = Intent(
            intent_type=IntentType.CHANNEL_OPEN.value,
            target=target,
            initiator=bob_pubkey,
            timestamp=now,  # Same timestamp
            expires_at=now + 60,
            status='pending'
        )

        # Setup Alice's IntentManager - need to mock conflicting intents in DB
        mock_database.get_conflicting_intents.return_value = [alice_intent.to_dict()]

        alice_mgr = IntentManager(
            database=mock_database,
            plugin=None,
            our_pubkey=alice_pubkey,
            hold_seconds=60
        )

        # Check conflict - Alice should win (lower pubkey)
        # check_conflicts returns (has_conflict, we_win)
        has_conflict, we_win = alice_mgr.check_conflicts(bob_intent)
        assert has_conflict is True
        assert we_win is True, "Alice (lower pubkey) should win"

        # Setup Bob's IntentManager
        mock_database.get_conflicting_intents.return_value = [bob_intent.to_dict()]

        bob_mgr = IntentManager(
            database=mock_database,
            plugin=None,
            our_pubkey=bob_pubkey,
            hold_seconds=60
        )

        # Check conflict from Bob's perspective - Bob should lose
        has_conflict, we_win = bob_mgr.check_conflicts(alice_intent)
        assert has_conflict is True
        assert we_win is False, "Bob (higher pubkey) should lose"

    def test_concurrent_intent_creation(self, mock_database, mock_plugin, mock_state_manager,
                                         mock_bridge, mock_clboss_bridge):
        """
        Simulate two planners creating intents for the same target concurrently.

        Both should be able to create intents, but only one should eventually
        be committed (determined by tie-breaker). This test verifies no crashes
        or race conditions occur.
        """
        alice_pubkey = '02' + 'a' * 64
        bob_pubkey = '02' + 'b' * 64
        target = '02' + 't' * 64

        # Setup two planners with their own intent managers
        alice_intent_mgr = IntentManager(
            database=mock_database,
            plugin=mock_plugin,
            our_pubkey=alice_pubkey,
            hold_seconds=60
        )

        bob_intent_mgr = IntentManager(
            database=mock_database,
            plugin=mock_plugin,
            our_pubkey=bob_pubkey,
            hold_seconds=60
        )

        # Create intents concurrently
        results = {}
        errors = []

        def create_intent(mgr, name):
            try:
                intent = mgr.create_intent(
                    intent_type=IntentType.CHANNEL_OPEN.value,
                    target=target
                )
                results[name] = intent
            except Exception as e:
                errors.append((name, e))

        # Start threads
        alice_thread = threading.Thread(target=create_intent, args=(alice_intent_mgr, 'alice'))
        bob_thread = threading.Thread(target=create_intent, args=(bob_intent_mgr, 'bob'))

        alice_thread.start()
        bob_thread.start()

        alice_thread.join(timeout=5)
        bob_thread.join(timeout=5)

        # Verify no errors
        assert len(errors) == 0, f"Errors during concurrent creation: {errors}"

        # Both should have created intents
        assert 'alice' in results
        assert 'bob' in results

        # Both intents should target the same thing
        assert results['alice'].target == target
        assert results['bob'].target == target

        # Mock that Alice has local pending intent for conflict check
        mock_database.get_conflicting_intents.return_value = [results['alice'].to_dict()]

        # Verify conflict detection works (check_conflicts returns has_conflict, we_win)
        has_conflict, alice_wins = alice_intent_mgr.check_conflicts(results['bob'])
        assert has_conflict is True
        assert alice_wins is True  # Alice wins (lower pubkey)

    def test_stalemate_no_double_action(self, mock_database, mock_plugin, mock_state_manager,
                                         mock_bridge, mock_clboss_bridge, mock_config):
        """
        When two planners identify the same target, only one should proceed.

        After tie-breaker resolution, only the winner should take action.
        """
        alice_pubkey = '02' + 'a' * 64
        bob_pubkey = '02' + 'b' * 64
        target = '02' + 't' * 64

        # Track actions taken
        actions_taken = []

        # Setup Alice's planner
        alice_intent_mgr = IntentManager(
            database=mock_database,
            plugin=mock_plugin,
            our_pubkey=alice_pubkey,
            hold_seconds=60
        )

        alice_planner = Planner(
            state_manager=mock_state_manager,
            database=mock_database,
            bridge=mock_bridge,
            clboss_bridge=mock_clboss_bridge,
            plugin=mock_plugin,
            intent_manager=alice_intent_mgr
        )

        # Setup Bob's planner
        bob_intent_mgr = IntentManager(
            database=mock_database,
            plugin=mock_plugin,
            our_pubkey=bob_pubkey,
            hold_seconds=60
        )

        bob_planner = Planner(
            state_manager=mock_state_manager,
            database=mock_database,
            bridge=mock_bridge,
            clboss_bridge=mock_clboss_bridge,
            plugin=mock_plugin,
            intent_manager=bob_intent_mgr
        )

        # Both planners identify the same underserved target
        # Mock listfunds for sufficient balance (need 2x min_channel_sats = 2M sats)
        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [{'status': 'confirmed', 'amount_msat': 5_000_000_000}]  # 5M sats
        }

        # Mock get_underserved_targets for both
        underserved_result = UnderservedResult(
            target=target,
            public_capacity_sats=200_000_000,
            hive_share_pct=0.02,
            score=2.0
        )

        with patch.object(alice_planner, 'get_underserved_targets', return_value=[underserved_result]):
            with patch.object(bob_planner, 'get_underserved_targets', return_value=[underserved_result]):
                # Alice proposes first
                alice_decisions = alice_planner._propose_expansion(mock_config, 'alice-run')

                # Now Bob checks - should find Alice's pending intent
                mock_database.get_pending_intents.return_value = [
                    {'target': target, 'status': 'pending', 'initiator': alice_pubkey}
                ]

                bob_decisions = bob_planner._propose_expansion(mock_config, 'bob-run')

        # Alice should have proposed
        assert len(alice_decisions) == 1
        assert alice_decisions[0]['action'] == 'expansion_proposed'

        # Bob should NOT have proposed (existing pending intent)
        assert len(bob_decisions) == 0


# =============================================================================
# THE FLAP TEST (HYSTERESIS)
# =============================================================================

class TestTheFlap:
    """
    Test hysteresis logic to prevent ignore/unignore flapping.

    Scenario: A target hovers exactly at 20% saturation (the cap).
    The Planner should not toggle ignore/unignore every cycle due
    to the hysteresis gap between thresholds (20% to ignore, 15% to release).
    """

    def test_hysteresis_prevents_flapping(self, mock_state_manager, mock_database,
                                           mock_bridge, mock_clboss_bridge, mock_plugin,
                                           mock_config):
        """
        Target at exactly 20% should be ignored, but once ignored,
        it should NOT be released until it drops to 15%.
        """
        target = '02' + 'f' * 64

        planner = Planner(
            state_manager=mock_state_manager,
            database=mock_database,
            bridge=mock_bridge,
            clboss_bridge=mock_clboss_bridge,
            plugin=mock_plugin
        )

        # Initialize network cache
        mock_plugin.rpc.listchannels.return_value = {
            'channels': [
                {
                    'source': '02' + 'x' * 64,
                    'destination': target,
                    'short_channel_id': '100x1x0',
                    'satoshis': 100_000_000,
                    'active': True
                }
            ]
        }
        planner._refresh_network_cache(force=True)

        # Cycle 1: Target at 21% - should be ignored
        # Mock get_saturated_targets to return the saturated target
        with patch.object(planner, 'get_saturated_targets') as mock_saturated:
            mock_saturated.return_value = [
                SaturationResult(
                    target=target,
                    hive_capacity_sats=21_000_000,
                    public_capacity_sats=100_000_000,
                    hive_share_pct=0.21,  # Above 20% cap
                    is_saturated=True,
                    should_release=False
                )
            ]

            decisions_1 = planner._enforce_saturation(mock_config, 'cycle-1')

        # Should have issued ignore
        assert len(decisions_1) == 1
        assert decisions_1[0]['action'] == 'ignore'
        assert target in planner._ignored_peers

        # Reset mock
        mock_clboss_bridge.ignore_peer.reset_mock()

        # Cycle 2: Target drops to 18% - should NOT release (still above 15%)
        with patch.object(planner, '_calculate_hive_share') as mock_calc:
            mock_calc.return_value = SaturationResult(
                target=target,
                hive_capacity_sats=18_000_000,
                public_capacity_sats=100_000_000,
                hive_share_pct=0.18,  # Below 20% but above 15%
                is_saturated=False,
                should_release=False
            )

            decisions_2 = planner._release_saturation(mock_config, 'cycle-2')

        # Should NOT have released (hysteresis)
        assert len(decisions_2) == 0
        assert target in planner._ignored_peers  # Still ignored
        mock_clboss_bridge.unignore_peer.assert_not_called()

        # Cycle 3: Target drops to 14% - NOW should release
        with patch.object(planner, '_calculate_hive_share') as mock_calc:
            mock_calc.return_value = SaturationResult(
                target=target,
                hive_capacity_sats=14_000_000,
                public_capacity_sats=100_000_000,
                hive_share_pct=0.14,  # Below 15% release threshold
                is_saturated=False,
                should_release=True
            )

            decisions_3 = planner._release_saturation(mock_config, 'cycle-3')

        # Should have released
        assert len(decisions_3) == 1
        assert decisions_3[0]['action'] == 'unignore'
        assert target not in planner._ignored_peers

    def test_hovering_at_threshold_no_flap(self, mock_state_manager, mock_database,
                                            mock_bridge, mock_clboss_bridge, mock_plugin,
                                            mock_config):
        """
        Target oscillating between 19% and 21% should not cause flapping.

        Once ignored at 21%, should stay ignored even when dropping to 19%.
        """
        target = '02' + 'h' * 64

        planner = Planner(
            state_manager=mock_state_manager,
            database=mock_database,
            bridge=mock_bridge,
            clboss_bridge=mock_clboss_bridge,
            plugin=mock_plugin
        )

        # Initialize network cache
        mock_plugin.rpc.listchannels.return_value = {'channels': []}
        planner._refresh_network_cache(force=True)
        planner._network_cache[target] = []

        ignore_count = 0
        unignore_count = 0

        def count_ignore(*args, **kwargs):
            nonlocal ignore_count
            ignore_count += 1
            return True

        def count_unignore(*args, **kwargs):
            nonlocal unignore_count
            unignore_count += 1
            return True

        # Modern API methods (unmanage_open/manage_open)
        mock_clboss_bridge.unmanage_open.side_effect = count_ignore
        mock_clboss_bridge.manage_open.side_effect = count_unignore

        # Simulate 10 cycles oscillating between 19% and 21%
        for i in range(10):
            share = 0.21 if i % 2 == 0 else 0.19

            # Mock get_saturated_targets for enforcement cycles
            with patch.object(planner, 'get_saturated_targets') as mock_saturated:
                if share > mock_config.market_share_cap_pct:
                    mock_saturated.return_value = [
                        SaturationResult(
                            target=target,
                            hive_capacity_sats=int(share * 100_000_000),
                            public_capacity_sats=100_000_000,
                            hive_share_pct=share,
                            is_saturated=True,
                            should_release=False
                        )
                    ]
                else:
                    mock_saturated.return_value = []

                planner._enforce_saturation(mock_config, f'cycle-{i}')

            # Mock _calculate_hive_share for release cycles
            with patch.object(planner, '_calculate_hive_share') as mock_calc:
                mock_calc.return_value = SaturationResult(
                    target=target,
                    hive_capacity_sats=int(share * 100_000_000),
                    public_capacity_sats=100_000_000,
                    hive_share_pct=share,
                    is_saturated=share > mock_config.market_share_cap_pct,
                    should_release=share < SATURATION_RELEASE_THRESHOLD_PCT
                )

                planner._release_saturation(mock_config, f'cycle-{i}')

        # Should have only ONE ignore (on first 21% cycle)
        # and ZERO unignores (never drops below 15%)
        assert ignore_count == 1, f"Expected 1 ignore, got {ignore_count}"
        assert unignore_count == 0, f"Expected 0 unignores, got {unignore_count}"

    def test_exact_threshold_boundary(self, mock_state_manager, mock_database,
                                       mock_bridge, mock_clboss_bridge, mock_plugin,
                                       mock_config):
        """
        Test behavior at exact threshold boundaries (20% and 15%).
        """
        target = '02' + 'e' * 64

        planner = Planner(
            state_manager=mock_state_manager,
            database=mock_database,
            bridge=mock_bridge,
            clboss_bridge=mock_clboss_bridge,
            plugin=mock_plugin
        )

        mock_plugin.rpc.listchannels.return_value = {'channels': []}
        planner._refresh_network_cache(force=True)
        planner._network_cache[target] = []

        # At exactly 20% - should NOT be ignored (not strictly greater)
        with patch.object(planner, 'get_saturated_targets') as mock_saturated:
            # At exactly 20%, is_saturated should be False (not > 0.20)
            mock_saturated.return_value = []  # No saturated targets

            decisions = planner._enforce_saturation(mock_config, 'test-exact')

        # Should NOT have issued ignore (0.20 is not > 0.20)
        assert len(decisions) == 0

        # Now at 20.01% - should be ignored
        with patch.object(planner, 'get_saturated_targets') as mock_saturated:
            mock_saturated.return_value = [
                SaturationResult(
                    target=target,
                    hive_capacity_sats=20_010_000,
                    public_capacity_sats=100_000_000,
                    hive_share_pct=0.2001,  # Just over cap
                    is_saturated=True,
                    should_release=False
                )
            ]

            decisions = planner._enforce_saturation(mock_config, 'test-over')

        assert len(decisions) == 1
        assert decisions[0]['action'] == 'ignore'


# =============================================================================
# ADDITIONAL GAME THEORY TESTS
# =============================================================================

class TestGameTheory:
    """Additional game theory scenarios."""

    def test_three_way_conflict(self, mock_database):
        """
        Three nodes propose the same target - lowest pubkey wins.

        check_conflicts returns (has_conflict, we_win).
        """
        alice_pubkey = '02' + 'a' * 64  # Winner (lowest)
        bob_pubkey = '02' + 'b' * 64    # Middle
        carol_pubkey = '02' + 'c' * 64  # Loser (highest)

        target = '02' + 't' * 64
        now = int(time.time())

        # Create intents
        alice_intent = Intent(
            intent_type=IntentType.CHANNEL_OPEN.value,
            target=target,
            initiator=alice_pubkey,
            timestamp=now,
            expires_at=now + 60,
            status='pending'
        )

        bob_intent = Intent(
            intent_type=IntentType.CHANNEL_OPEN.value,
            target=target,
            initiator=bob_pubkey,
            timestamp=now,
            expires_at=now + 60,
            status='pending'
        )

        carol_intent = Intent(
            intent_type=IntentType.CHANNEL_OPEN.value,
            target=target,
            initiator=carol_pubkey,
            timestamp=now,
            expires_at=now + 60,
            status='pending'
        )

        # Bob checks against Alice and Carol
        # Need to mock local pending intent for conflict detection
        mock_database.get_conflicting_intents.return_value = [bob_intent.to_dict()]

        bob_mgr = IntentManager(mock_database, None, bob_pubkey, 60)

        # Bob vs Alice - Bob loses (alice < bob)
        # check_conflicts returns (has_conflict, we_win)
        has_conflict, bob_wins_vs_alice = bob_mgr.check_conflicts(alice_intent)
        assert has_conflict is True
        assert bob_wins_vs_alice is False  # Bob loses to Alice

        # Bob vs Carol - Bob wins (bob < carol)
        has_conflict, bob_wins_vs_carol = bob_mgr.check_conflicts(carol_intent)
        assert has_conflict is True
        assert bob_wins_vs_carol is True  # Bob wins against Carol

    def test_expansion_respects_max_per_cycle(self, mock_state_manager, mock_database,
                                               mock_bridge, mock_clboss_bridge, mock_plugin,
                                               mock_config):
        """
        Even with multiple underserved targets, only MAX_EXPANSIONS_PER_CYCLE should be proposed.
        """
        target1 = '02' + '1' * 64
        target2 = '02' + '2' * 64
        target3 = '02' + '3' * 64

        mock_intent_mgr = MagicMock()
        mock_intent = MagicMock()
        mock_intent.intent_id = 1
        mock_intent_mgr.create_intent.return_value = mock_intent

        planner = Planner(
            state_manager=mock_state_manager,
            database=mock_database,
            bridge=mock_bridge,
            clboss_bridge=mock_clboss_bridge,
            plugin=mock_plugin,
            intent_manager=mock_intent_mgr
        )

        mock_plugin.rpc.listfunds.return_value = {
            'outputs': [{'status': 'confirmed', 'amount_msat': 10000000000}]
        }

        # Multiple underserved targets
        with patch.object(planner, 'get_underserved_targets') as mock_get:
            mock_get.return_value = [
                UnderservedResult(target1, 200_000_000, 0.01, 2.0),
                UnderservedResult(target2, 200_000_000, 0.02, 1.9),
                UnderservedResult(target3, 200_000_000, 0.03, 1.8),
            ]

            mock_database.get_pending_intents.return_value = []

            decisions = planner._propose_expansion(mock_config, 'test-multi')

        # Should only have proposed 1 (MAX_EXPANSIONS_PER_CYCLE = 1)
        assert len(decisions) == 1
        assert mock_intent_mgr.create_intent.call_count == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
