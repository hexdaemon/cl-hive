"""
Tests for BudgetManager module.

Tests the BudgetHoldManager class for:
- Hold creation with concurrent limits and duration caps
- Hold release and idempotency
- Hold consumption lifecycle
- Available budget calculation
- Expiry cleanup and DB persistence

Author: Lightning Goats Team
"""

import pytest
import time
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.budget_manager import (
    BudgetHoldManager, BudgetHold, MAX_HOLD_DURATION_SECONDS,
    MAX_CONCURRENT_HOLDS, CLEANUP_INTERVAL_SECONDS
)


# =============================================================================
# FIXTURES
# =============================================================================

OUR_PUBKEY = "03" + "a1" * 32

@pytest.fixture
def mock_database():
    """Create a mock database with budget hold methods."""
    db = MagicMock()
    db.create_budget_hold = MagicMock()
    db.release_budget_hold = MagicMock()
    db.consume_budget_hold = MagicMock()
    db.expire_budget_hold = MagicMock()
    db.get_budget_hold = MagicMock(return_value=None)
    db.get_holds_for_round = MagicMock(return_value=[])
    db.get_active_holds_for_peer = MagicMock(return_value=[])
    return db


@pytest.fixture
def manager(mock_database):
    """Create a BudgetHoldManager instance."""
    mgr = BudgetHoldManager(database=mock_database, our_pubkey=OUR_PUBKEY)
    # Bypass cleanup rate limiting for tests
    mgr._last_cleanup = 0
    return mgr


# =============================================================================
# HOLD CREATION TESTS
# =============================================================================

class TestHoldCreation:
    """Tests for creating budget holds."""

    def test_basic_create_hold(self, manager, mock_database):
        """Create a simple budget hold and verify it's stored."""
        hold_id = manager.create_hold(round_id="round_001", amount_sats=500_000)

        assert hold_id is not None
        assert hold_id.startswith("hold_")
        mock_database.create_budget_hold.assert_called_once()

    def test_hold_stored_in_memory(self, manager):
        """Verify hold is accessible from in-memory cache."""
        hold_id = manager.create_hold(round_id="round_002", amount_sats=300_000)

        hold = manager.get_hold(hold_id)
        assert hold is not None
        assert hold.amount_sats == 300_000
        assert hold.round_id == "round_002"
        assert hold.peer_id == OUR_PUBKEY
        assert hold.status == "active"

    def test_max_concurrent_holds_enforced(self, manager):
        """Cannot create more than MAX_CONCURRENT_HOLDS active holds."""
        created = []
        for i in range(MAX_CONCURRENT_HOLDS):
            hold_id = manager.create_hold(round_id=f"round_{i}", amount_sats=100_000)
            assert hold_id is not None
            created.append(hold_id)

        # Next one should fail
        result = manager.create_hold(round_id="round_extra", amount_sats=100_000)
        assert result is None

    def test_duplicate_round_returns_existing(self, manager):
        """Creating a hold for the same round returns existing hold_id."""
        hold_id1 = manager.create_hold(round_id="round_dup", amount_sats=500_000)
        hold_id2 = manager.create_hold(round_id="round_dup", amount_sats=500_000)

        assert hold_id1 == hold_id2

    def test_duration_cap(self, manager):
        """Duration is capped at MAX_HOLD_DURATION_SECONDS."""
        hold_id = manager.create_hold(
            round_id="round_long", amount_sats=100_000,
            duration_seconds=99999
        )
        hold = manager.get_hold(hold_id)
        assert hold is not None
        assert (hold.expires_at - hold.created_at) <= MAX_HOLD_DURATION_SECONDS

    def test_db_persistence_called(self, manager, mock_database):
        """Verify database persistence is called on creation."""
        hold_id = manager.create_hold(round_id="round_db", amount_sats=250_000)

        call_kwargs = mock_database.create_budget_hold.call_args
        assert call_kwargs is not None
        # Verify the call was made with correct params
        _, kwargs = call_kwargs
        assert kwargs["round_id"] == "round_db"
        assert kwargs["amount_sats"] == 250_000
        assert kwargs["peer_id"] == OUR_PUBKEY


# =============================================================================
# HOLD RELEASE TESTS
# =============================================================================

class TestHoldRelease:
    """Tests for releasing budget holds."""

    def test_release_active_hold(self, manager):
        """Release an active hold successfully."""
        hold_id = manager.create_hold(round_id="round_rel", amount_sats=200_000)

        result = manager.release_hold(hold_id)
        assert result is True

        hold = manager.get_hold(hold_id)
        assert hold.status == "released"

    def test_release_nonexistent_hold(self, manager):
        """Releasing a non-existent hold returns False."""
        result = manager.release_hold("hold_does_not_exist")
        assert result is False

    def test_release_already_released_hold(self, manager):
        """Releasing an already released hold returns False."""
        hold_id = manager.create_hold(round_id="round_rr", amount_sats=100_000)
        manager.release_hold(hold_id)

        result = manager.release_hold(hold_id)
        assert result is False

    def test_release_holds_for_round(self, manager, mock_database):
        """Release all holds for a given round."""
        hold_id1 = manager.create_hold(round_id="round_batch", amount_sats=100_000)
        hold_id2 = manager.create_hold(round_id="round_other", amount_sats=100_000)

        released = manager.release_holds_for_round("round_batch")
        assert released == 1

        # The other round's hold should still be active
        hold2 = manager.get_hold(hold_id2)
        assert hold2.status == "active"


# =============================================================================
# HOLD CONSUMPTION TESTS
# =============================================================================

class TestHoldConsumption:
    """Tests for consuming budget holds."""

    def test_consume_active_hold(self, manager):
        """Consume an active hold successfully."""
        hold_id = manager.create_hold(round_id="round_con", amount_sats=500_000)

        result = manager.consume_hold(hold_id, consumed_by="channel_abc123")
        assert result is True

        hold = manager.get_hold(hold_id)
        assert hold.status == "consumed"
        assert hold.consumed_by == "channel_abc123"
        assert hold.consumed_at is not None

    def test_consume_released_hold_fails(self, manager):
        """Cannot consume a released hold."""
        hold_id = manager.create_hold(round_id="round_cr", amount_sats=500_000)
        manager.release_hold(hold_id)

        result = manager.consume_hold(hold_id, consumed_by="channel_xyz")
        assert result is False

    def test_consume_nonexistent_hold_fails(self, manager):
        """Cannot consume a non-existent hold."""
        result = manager.consume_hold("hold_nonexistent", consumed_by="channel_xyz")
        assert result is False

    def test_consume_expired_hold_fails(self, manager):
        """Cannot consume an expired hold."""
        hold_id = manager.create_hold(
            round_id="round_exp_con", amount_sats=100_000, duration_seconds=1
        )
        # Force expiration
        hold = manager.get_hold(hold_id)
        hold.expires_at = int(time.time()) - 10
        hold.status = "expired"

        result = manager.consume_hold(hold_id, consumed_by="channel_xyz")
        assert result is False


# =============================================================================
# BUDGET CALCULATION TESTS
# =============================================================================

class TestBudgetCalculation:
    """Tests for available budget calculation."""

    def test_available_budget_no_holds(self, manager):
        """Available budget with no holds = total * (1 - reserve)."""
        available = manager.get_available_budget(
            total_onchain_sats=1_000_000, reserve_pct=0.20
        )
        assert available == 800_000

    def test_available_budget_with_holds(self, manager):
        """Available budget subtracts active holds."""
        manager.create_hold(round_id="round_b1", amount_sats=200_000)

        available = manager.get_available_budget(
            total_onchain_sats=1_000_000, reserve_pct=0.20
        )
        # 800_000 spendable - 200_000 held = 600_000
        assert available == 600_000

    def test_total_held_sum(self, manager):
        """Total held sums all active holds."""
        manager.create_hold(round_id="round_h1", amount_sats=100_000)
        manager.create_hold(round_id="round_h2", amount_sats=250_000)

        total = manager.get_total_held()
        assert total == 350_000

    def test_available_budget_floors_at_zero(self, manager):
        """Available budget cannot go negative."""
        manager.create_hold(round_id="round_neg", amount_sats=900_000)

        available = manager.get_available_budget(
            total_onchain_sats=500_000, reserve_pct=0.20
        )
        assert available == 0


# =============================================================================
# CLEANUP AND EXPIRY TESTS
# =============================================================================

class TestCleanupExpiry:
    """Tests for hold expiry and cleanup."""

    def test_expired_holds_cleaned(self, manager, mock_database):
        """Expired holds are marked as expired during cleanup."""
        hold_id = manager.create_hold(
            round_id="round_expire", amount_sats=100_000, duration_seconds=1
        )

        # Force the hold to be expired
        manager._holds[hold_id].expires_at = int(time.time()) - 10
        # Reset cleanup timer so cleanup runs
        manager._last_cleanup = 0

        expired_count = manager.cleanup_expired_holds()
        assert expired_count == 1

        # After cleanup, expired holds are evicted from memory and persisted to DB.
        # Verify the DB was notified of expiry.
        mock_database.expire_budget_hold.assert_called_once_with(hold_id)
        # Hold should no longer be in memory (evicted)
        assert hold_id not in manager._holds

    def test_load_from_database(self, manager, mock_database):
        """Load active holds from database on init."""
        future = int(time.time()) + 300
        mock_database.get_active_holds_for_peer.return_value = [
            {
                "hold_id": "hold_db1",
                "round_id": "round_db1",
                "peer_id": OUR_PUBKEY,
                "amount_sats": 500_000,
                "created_at": int(time.time()),
                "expires_at": future,
                "status": "active",
            }
        ]

        loaded = manager.load_from_database()
        assert loaded == 1

        hold = manager.get_hold("hold_db1")
        assert hold is not None
        assert hold.amount_sats == 500_000


# =============================================================================
# BUDGET HOLD DATACLASS TESTS
# =============================================================================

class TestBudgetHoldDataclass:
    """Tests for BudgetHold dataclass methods."""

    def test_to_dict(self):
        """Verify to_dict serialization."""
        hold = BudgetHold(
            hold_id="hold_test",
            round_id="round_test",
            peer_id=OUR_PUBKEY,
            amount_sats=100_000,
            created_at=1000,
            expires_at=2000,
        )
        d = hold.to_dict()
        assert d["hold_id"] == "hold_test"
        assert d["amount_sats"] == 100_000

    def test_from_dict(self):
        """Verify from_dict deserialization."""
        data = {
            "hold_id": "hold_fd",
            "round_id": "round_fd",
            "peer_id": OUR_PUBKEY,
            "amount_sats": 250_000,
            "created_at": 1000,
            "expires_at": 2000,
            "status": "active",
        }
        hold = BudgetHold.from_dict(data)
        assert hold.hold_id == "hold_fd"
        assert hold.amount_sats == 250_000

    def test_is_active_true(self):
        """Active hold with future expiry returns True."""
        hold = BudgetHold(
            hold_id="h", round_id="r", peer_id="p",
            amount_sats=100, created_at=int(time.time()),
            expires_at=int(time.time()) + 300, status="active"
        )
        assert hold.is_active() is True

    def test_is_active_false_expired(self):
        """Hold past expiry returns False."""
        hold = BudgetHold(
            hold_id="h", round_id="r", peer_id="p",
            amount_sats=100, created_at=int(time.time()) - 600,
            expires_at=int(time.time()) - 1, status="active"
        )
        assert hold.is_active() is False

    def test_is_active_false_released(self):
        """Released hold returns False."""
        hold = BudgetHold(
            hold_id="h", round_id="r", peer_id="p",
            amount_sats=100, created_at=int(time.time()),
            expires_at=int(time.time()) + 300, status="released"
        )
        assert hold.is_active() is False
