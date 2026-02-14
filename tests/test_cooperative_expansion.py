"""
Tests for CooperativeExpansion module (Phase 6.4).

Tests the CooperativeExpansionManager class for:
- Round lifecycle (start, complete, cancel, expire)
- Nomination handling
- Election winner selection with weighted scoring
- Decline/fallback handling (Phase 8)
- Affordability checks and cleanup

Author: Lightning Goats Team
"""

import pytest
import time
import math
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.cooperative_expansion import (
    CooperativeExpansionManager, ExpansionRound, ExpansionRoundState,
    Nomination
)


# =============================================================================
# FIXTURES
# =============================================================================

OUR_PUBKEY = "03" + "a1" * 32
PEER_B = "03" + "b2" * 32
PEER_C = "03" + "c3" * 32
TARGET_PEER = "03" + "d4" * 32
TARGET_PEER_2 = "03" + "e5" * 32


@pytest.fixture
def mock_database():
    """Create a mock database."""
    db = MagicMock()
    return db


@pytest.fixture
def mock_quality_scorer():
    """Create a mock quality scorer."""
    scorer = MagicMock()
    result = MagicMock()
    result.overall_score = 0.7
    scorer.calculate_score.return_value = result
    return scorer


@pytest.fixture
def mock_plugin():
    """Create a mock plugin."""
    plugin = MagicMock()
    plugin.log = MagicMock()
    plugin.rpc.getinfo.return_value = {"id": OUR_PUBKEY}
    plugin.rpc.listfunds.return_value = {
        "outputs": [{"amount_msat": 5_000_000_000, "status": "confirmed"}]
    }
    plugin.rpc.listpeerchannels.return_value = {"channels": []}
    return plugin


@pytest.fixture
def manager(mock_database, mock_quality_scorer, mock_plugin):
    """Create a CooperativeExpansionManager.

    Auto-nomination is disabled by default (plugin=None).
    Tests that need auto-nominate can set manager.plugin and manager.our_id.
    """
    mgr = CooperativeExpansionManager(
        database=mock_database,
        quality_scorer=mock_quality_scorer,
        plugin=None,
        our_id=None,
    )
    return mgr


# =============================================================================
# ROUND LIFECYCLE TESTS
# =============================================================================

class TestRoundLifecycle:
    """Tests for expansion round lifecycle."""

    def test_start_round(self, manager):
        """Start a new expansion round."""
        round_id = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="remote_close",
            trigger_reporter=PEER_B,
            quality_score=0.7,
        )
        assert round_id is not None

        round_obj = manager.get_round(round_id)
        assert round_obj is not None
        assert round_obj.state == ExpansionRoundState.NOMINATING
        assert round_obj.target_peer_id == TARGET_PEER

    def test_max_active_rounds(self, manager):
        """Cannot exceed MAX_ACTIVE_ROUNDS."""
        # Disable auto-nominate to not interfere


        for i in range(manager.MAX_ACTIVE_ROUNDS):
            rid = manager.start_round(
                target_peer_id=f"03{'%02x' % i}" + "ff" * 31,
                trigger_event="manual",
                trigger_reporter=PEER_B,
            )
            assert rid is not None

        # Verify we have MAX_ACTIVE_ROUNDS active
        active = manager.get_active_rounds()
        assert len(active) == manager.MAX_ACTIVE_ROUNDS

    def test_cooldown_rejection(self, manager):
        """Cannot start a round for a target on cooldown."""
        manager.our_id = None  # Disable auto-nominate
        # First round
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )
        assert rid is not None

        # Election sets cooldown
        nom = Nomination(
            nominator_id=PEER_B,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=5_000_000,
            quality_score=0.7,
            has_existing_channel=False,
            channel_count=10,
        )
        manager.add_nomination(rid, nom)
        manager.elect_winner(rid)

        # Try evaluate_expansion for same target → rejected by cooldown
        result = manager.evaluate_expansion(
            target_peer_id=TARGET_PEER,
            event_type="remote_close",
            reporter_id=PEER_C,
            quality_score=0.7,
        )
        assert result is None

    def test_complete_round(self, manager):
        """Complete a round successfully."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        manager.complete_round(rid, success=True, result="channel_opened")

        round_obj = manager.get_round(rid)
        assert round_obj.state == ExpansionRoundState.COMPLETED
        assert round_obj.result == "channel_opened"

    def test_cancel_round(self, manager):
        """Cancel an active round."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        manager.cancel_round(rid, reason="test_cancel")

        round_obj = manager.get_round(rid)
        assert round_obj.state == ExpansionRoundState.CANCELLED


# =============================================================================
# NOMINATION TESTS
# =============================================================================

class TestNominations:
    """Tests for nomination handling."""

    def test_add_nomination(self, manager):
        """Add a valid nomination to a round."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        nom = Nomination(
            nominator_id=PEER_B,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=5_000_000,
            quality_score=0.7,
            has_existing_channel=False,
            channel_count=10,
        )

        result = manager.add_nomination(rid, nom)
        assert result is True

    def test_handle_nomination_payload(self, manager):
        """Handle an incoming EXPANSION_NOMINATE message."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        payload = {
            "round_id": rid,
            "target_peer_id": TARGET_PEER,
            "nominator_id": PEER_C,
            "available_liquidity_sats": 3_000_000,
            "quality_score": 0.6,
            "has_existing_channel": False,
            "channel_count": 5,
        }

        result = manager.handle_nomination(PEER_C, payload)
        assert result["success"] is True

    def test_duplicate_nomination_overwrites(self, manager):
        """Same nominator can update their nomination."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        nom1 = Nomination(
            nominator_id=PEER_B,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=5_000_000,
            quality_score=0.7,
            has_existing_channel=False,
            channel_count=10,
        )
        nom2 = Nomination(
            nominator_id=PEER_B,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=8_000_000,
            quality_score=0.8,
            has_existing_channel=False,
            channel_count=10,
        )

        manager.add_nomination(rid, nom1)
        manager.add_nomination(rid, nom2)

        round_obj = manager.get_round(rid)
        # Should have 1 nomination (overwritten)
        assert len(round_obj.nominations) == 1
        assert round_obj.nominations[PEER_B].available_liquidity_sats == 8_000_000

    def test_nomination_after_window_rejected(self, manager):
        """Nominations rejected after round leaves NOMINATING state."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        # Add one nomination and elect
        nom = Nomination(
            nominator_id=PEER_B,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=5_000_000,
            quality_score=0.7,
            has_existing_channel=False,
            channel_count=10,
        )
        manager.add_nomination(rid, nom)
        manager.elect_winner(rid)

        # Late nomination rejected
        late_nom = Nomination(
            nominator_id=PEER_C,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=3_000_000,
            quality_score=0.6,
            has_existing_channel=False,
            channel_count=5,
        )
        result = manager.add_nomination(rid, late_nom)
        assert result is False

    def test_nomination_with_existing_channel_rejected(self, manager):
        """Nominations from members with existing channel are rejected."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        nom = Nomination(
            nominator_id=PEER_B,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=5_000_000,
            quality_score=0.7,
            has_existing_channel=True,  # Already has channel
            channel_count=10,
        )

        result = manager.add_nomination(rid, nom)
        assert result is False


# =============================================================================
# ELECTION TESTS
# =============================================================================

class TestElection:
    """Tests for election winner selection."""

    def test_winner_by_weight(self, manager):
        """Higher-scored nomination wins the election."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        # PEER_B: higher liquidity, fewer channels, higher quality
        nom_b = Nomination(
            nominator_id=PEER_B,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=10_000_000,
            quality_score=0.9,
            has_existing_channel=False,
            channel_count=5,
        )
        # PEER_C: lower liquidity, more channels, lower quality
        nom_c = Nomination(
            nominator_id=PEER_C,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=1_000_000,
            quality_score=0.5,
            has_existing_channel=False,
            channel_count=40,
        )

        manager.add_nomination(rid, nom_b)
        manager.add_nomination(rid, nom_c)

        winner = manager.elect_winner(rid)
        assert winner == PEER_B

    def test_min_nominations_required(self, manager):
        """Election fails with insufficient nominations."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        # No nominations added (MIN_NOMINATIONS_FOR_ELECTION = 1)
        # Since we added 0 nominations, election should fail
        winner = manager.elect_winner(rid)
        assert winner is None

        round_obj = manager.get_round(rid)
        assert round_obj.state == ExpansionRoundState.CANCELLED

    def test_recent_opens_penalized(self, manager):
        """Members who recently opened channels get lower score."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        # Mark PEER_B as having recently opened (within the hour)
        manager._recent_opens[PEER_B] = int(time.time()) - 60

        # Equal stats otherwise
        nom_b = Nomination(
            nominator_id=PEER_B,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=5_000_000,
            quality_score=0.7,
            has_existing_channel=False,
            channel_count=10,
        )
        nom_c = Nomination(
            nominator_id=PEER_C,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=5_000_000,
            quality_score=0.7,
            has_existing_channel=False,
            channel_count=10,
        )

        manager.add_nomination(rid, nom_b)
        manager.add_nomination(rid, nom_c)

        winner = manager.elect_winner(rid)
        assert winner == PEER_C  # PEER_C wins because no recent opens

    def test_elect_payload_handled(self, manager):
        """handle_elect correctly identifies if we're the elected member."""
        manager.our_id = OUR_PUBKEY

        # Create round locally
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )
        manager.our_id = OUR_PUBKEY

        payload = {
            "round_id": rid,
            "elected_id": OUR_PUBKEY,
            "target_peer_id": TARGET_PEER,
            "channel_size_sats": 2_000_000,
        }

        result = manager.handle_elect(PEER_B, payload)
        assert result["action"] == "open_channel"
        assert result["target_peer_id"] == TARGET_PEER

    def test_elect_payload_not_us(self, manager):
        """handle_elect when we're NOT the elected member."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )
        manager.our_id = OUR_PUBKEY

        payload = {
            "round_id": rid,
            "elected_id": PEER_B,  # Not us
            "target_peer_id": TARGET_PEER,
            "channel_size_sats": 2_000_000,
        }

        result = manager.handle_elect(PEER_B, payload)
        assert result["action"] == "none"


# =============================================================================
# DECLINE / FALLBACK TESTS (Phase 8)
# =============================================================================

class TestDeclineHandling:
    """Tests for decline and fallback handling."""

    def _setup_round_with_election(self, manager):
        """Helper: create round, add nominations, elect winner."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        nom_b = Nomination(
            nominator_id=PEER_B,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=10_000_000,
            quality_score=0.9,
            has_existing_channel=False,
            channel_count=5,
        )
        nom_c = Nomination(
            nominator_id=PEER_C,
            target_peer_id=TARGET_PEER,
            timestamp=int(time.time()),
            available_liquidity_sats=5_000_000,
            quality_score=0.7,
            has_existing_channel=False,
            channel_count=10,
        )

        manager.add_nomination(rid, nom_b)
        manager.add_nomination(rid, nom_c)
        winner = manager.elect_winner(rid)
        return rid, winner

    def test_decline_fallback_to_next(self, manager):
        """Decline from winner triggers fallback to next candidate."""
        rid, winner = self._setup_round_with_election(manager)
        assert winner == PEER_B  # B should win (higher score)

        result = manager.handle_decline(PEER_B, {
            "round_id": rid,
            "decliner_id": PEER_B,
            "reason": "insufficient_funds",
        })

        assert result["action"] == "fallback_elected"
        assert result["elected_id"] == PEER_C

    def test_max_fallbacks_cancel(self, manager):
        """After MAX_FALLBACK_ATTEMPTS, round is cancelled."""
        rid, winner = self._setup_round_with_election(manager)

        # Decline from B → fallback to C
        manager.handle_decline(PEER_B, {
            "round_id": rid,
            "decliner_id": PEER_B,
            "reason": "test",
        })

        # Decline from C → max declines reached (MAX_FALLBACK_ATTEMPTS=2)
        result = manager.handle_decline(PEER_C, {
            "round_id": rid,
            "decliner_id": PEER_C,
            "reason": "test",
        })

        assert result["action"] == "cancelled"
        assert "no_fallback_candidates" in result["reason"] or "max_fallbacks" in result["reason"]

    def test_decline_invalid_round(self, manager):
        """Decline for non-existent round returns error."""
        result = manager.handle_decline(PEER_B, {
            "round_id": "nonexistent",
            "decliner_id": PEER_B,
            "reason": "test",
        })
        assert "error" in result


# =============================================================================
# AFFORDABILITY / CLEANUP TESTS
# =============================================================================

class TestAffordabilityAndCleanup:
    """Tests for affordability checks and round cleanup."""

    def test_fleet_affordability_local_only(self, manager, mock_plugin):
        """Fleet affordability check without state_manager uses local balance."""
        manager.plugin = mock_plugin
        manager.our_id = OUR_PUBKEY
        manager.state_manager = None
        result = manager.check_fleet_affordability(min_channel_sats=100_000)
        assert result["can_afford"] is True
        assert result["source"] == "local_only"

    def test_evaluate_expansion_low_quality(self, manager):
        """evaluate_expansion rejects low quality targets."""
        result = manager.evaluate_expansion(
            target_peer_id=TARGET_PEER,
            event_type="remote_close",
            reporter_id=PEER_B,
            quality_score=0.1,  # Below MIN_QUALITY_SCORE (0.45)
        )
        assert result is None

    def test_expired_round_cleanup(self, manager):
        """Expired rounds are cleaned up."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        # Force expiration
        round_obj = manager.get_round(rid)
        round_obj.expires_at = int(time.time()) - 10

        cleaned = manager.cleanup_expired_rounds()
        assert cleaned == 1

        round_obj = manager.get_round(rid)
        assert round_obj.state == ExpansionRoundState.EXPIRED

    def test_get_active_rounds(self, manager):
        """get_active_rounds returns only NOMINATING/ELECTING rounds."""

        rid1 = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )
        rid2 = manager.start_round(
            target_peer_id=TARGET_PEER_2,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )
        manager.complete_round(rid2, success=True)

        active = manager.get_active_rounds()
        assert len(active) == 1
        assert active[0].round_id == rid1

    def test_get_status(self, manager):
        """get_status returns correct counts."""
        rid = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        status = manager.get_status()
        assert status["active_rounds"] >= 1
        assert status["total_rounds"] >= 1
        assert "max_active_rounds" in status

    def test_rounds_for_target(self, manager):
        """get_rounds_for_target filters by target."""

        rid1 = manager.start_round(
            target_peer_id=TARGET_PEER,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )
        rid2 = manager.start_round(
            target_peer_id=TARGET_PEER_2,
            trigger_event="manual",
            trigger_reporter=PEER_B,
        )

        rounds = manager.get_rounds_for_target(TARGET_PEER)
        assert len(rounds) == 1
        assert rounds[0].target_peer_id == TARGET_PEER
