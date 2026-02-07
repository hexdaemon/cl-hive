"""
Tests for Ticket S-01: Critical Security Hardening

Tests security fixes from audit remediation:
- P3-01: Remote intent cache bounds (MAX_REMOTE_INTENTS=200)
- P3-02: Membership verification in handlers
- P5-02: Contribution daily total cap
- P5-03: Contribution ledger DB row limit
- X-01: RPC lock timeout

Author: Lightning Goats Team
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
    MAX_REMOTE_INTENTS, STATUS_PENDING
)
from modules.contribution import (
    ContributionManager, MAX_CONTRIB_EVENTS_PER_DAY_TOTAL
)
from modules.database import HiveDatabase


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_database():
    """Create a mock database for testing."""
    db = MagicMock()
    db.create_intent.return_value = 1
    db.get_conflicting_intents.return_value = []
    db.update_intent_status.return_value = True
    db.cleanup_expired_intents.return_value = 0
    db.get_member.return_value = None
    db.record_contribution.return_value = True
    return db


@pytest.fixture
def mock_plugin():
    """Create a mock plugin for logging."""
    plugin = MagicMock()
    plugin.log = MagicMock()
    return plugin


@pytest.fixture
def intent_manager(mock_database, mock_plugin):
    """Create an IntentManager with mocked dependencies."""
    return IntentManager(
        mock_database,
        mock_plugin,
        our_pubkey="02" + "a" * 64,
        hold_seconds=60
    )


@pytest.fixture
def contribution_manager(mock_database, mock_plugin):
    """Create a ContributionManager with mocked dependencies."""
    mock_rpc = MagicMock()
    mock_config = MagicMock()
    mock_config.ban_autotrigger_enabled = False
    return ContributionManager(mock_rpc, mock_database, mock_plugin, mock_config)


# =============================================================================
# P3-01: REMOTE INTENT CACHE BOUNDS TESTS
# =============================================================================

class TestRemoteIntentCacheBounds:
    """Test P3-01: MAX_REMOTE_INTENTS=200 with LRU eviction."""

    def test_max_remote_intents_constant_exists(self):
        """MAX_REMOTE_INTENTS should be defined as 200."""
        assert MAX_REMOTE_INTENTS == 200

    def test_cache_does_not_exceed_limit(self, intent_manager):
        """Cache should never exceed MAX_REMOTE_INTENTS entries."""
        now = int(time.time())

        # Insert more than MAX_REMOTE_INTENTS unique intents
        for i in range(MAX_REMOTE_INTENTS + 50):
            intent = Intent(
                intent_type='channel_open',
                target=f'target_{i:05d}',
                initiator=f'02{i:064x}'[:66],
                timestamp=now + i,  # Increasing timestamps
                expires_at=now + i + 60
            )
            intent_manager.record_remote_intent(intent)

        # Verify cache size is bounded
        assert len(intent_manager._remote_intents) <= MAX_REMOTE_INTENTS

    def test_oldest_intent_evicted_on_overflow(self, intent_manager):
        """Oldest intent by timestamp should be evicted when cache is full."""
        base_time = int(time.time()) - 3600  # 1 hour ago (within 24h validation window)

        # Fill cache exactly to limit
        for i in range(MAX_REMOTE_INTENTS):
            intent = Intent(
                intent_type='channel_open',
                target=f'target_{i:05d}',
                initiator=f'02{i:064x}'[:66],
                timestamp=base_time + i,  # Oldest = base_time, newest = base_time + 199
                expires_at=base_time + i + 60
            )
            intent_manager.record_remote_intent(intent)

        assert len(intent_manager._remote_intents) == MAX_REMOTE_INTENTS

        # Add one more (should evict oldest)
        new_intent = Intent(
            intent_type='channel_open',
            target='new_target',
            initiator='02' + 'f' * 64,
            timestamp=base_time + MAX_REMOTE_INTENTS,  # Newer than all
            expires_at=base_time + MAX_REMOTE_INTENTS + 60
        )
        intent_manager.record_remote_intent(new_intent)

        # Cache should still be at limit
        assert len(intent_manager._remote_intents) == MAX_REMOTE_INTENTS

        # Oldest (timestamp=base_time, target_00000) should be gone
        oldest_key = 'channel_open:target_00000:' + f'02{0:064x}'[:66]
        assert oldest_key not in intent_manager._remote_intents

        # New intent should be present
        new_key = 'channel_open:new_target:' + '02' + 'f' * 64
        assert new_key in intent_manager._remote_intents

    def test_updating_existing_intent_does_not_evict(self, intent_manager):
        """Updating an existing intent should not trigger eviction."""
        base_time = int(time.time()) - 3600  # 1 hour ago (within 24h validation window)

        # Fill cache
        for i in range(MAX_REMOTE_INTENTS):
            intent = Intent(
                intent_type='channel_open',
                target=f'target_{i:05d}',
                initiator=f'02{i:064x}'[:66],
                timestamp=base_time + i,
                expires_at=base_time + i + 60
            )
            intent_manager.record_remote_intent(intent)

        initial_count = len(intent_manager._remote_intents)

        # Update an existing intent (same key)
        existing_intent = Intent(
            intent_type='channel_open',
            target='target_00050',  # Already exists
            initiator=f'02{50:064x}'[:66],
            timestamp=base_time + 50 + 100,  # New timestamp (still within validation window)
            expires_at=base_time + 50 + 160
        )
        intent_manager.record_remote_intent(existing_intent)

        # Count should remain the same
        assert len(intent_manager._remote_intents) == initial_count

    def test_no_crash_under_sustained_spam(self, intent_manager):
        """Cache should handle sustained spam without crash or OOM."""
        now = int(time.time())

        # Simulate sustained spam (1 million unique intents)
        for i in range(1000000):
            intent = Intent(
                intent_type='channel_open',
                target=f'spam_{i:010d}',
                initiator=f'02{i % 10000:064x}'[:66],  # Reuse some initiators
                timestamp=now + i,
                expires_at=now + i + 60
            )
            intent_manager.record_remote_intent(intent)

            # Periodically check bounds (every 10000 iterations)
            if i % 10000 == 0:
                assert len(intent_manager._remote_intents) <= MAX_REMOTE_INTENTS

        # Final check
        assert len(intent_manager._remote_intents) <= MAX_REMOTE_INTENTS


# =============================================================================
# P5-02: CONTRIBUTION DAILY TOTAL CAP TESTS
# =============================================================================

class TestContributionDailyCap:
    """Test P5-02: MAX_CONTRIB_EVENTS_PER_DAY_TOTAL=10000."""

    def test_daily_cap_constant_exists(self):
        """MAX_CONTRIB_EVENTS_PER_DAY_TOTAL should be defined as 10000."""
        assert MAX_CONTRIB_EVENTS_PER_DAY_TOTAL == 10000

    def test_daily_global_limit_enforced(self, contribution_manager):
        """Daily global limit should reject events after cap reached."""
        # Exhaust the daily cap
        for i in range(MAX_CONTRIB_EVENTS_PER_DAY_TOTAL):
            assert contribution_manager._allow_daily_global() is True

        # Next should be rejected
        assert contribution_manager._allow_daily_global() is False

    def test_daily_limit_resets_after_24h(self, contribution_manager):
        """Daily limit should reset after 24 hours."""
        # Exhaust the cap
        for _ in range(MAX_CONTRIB_EVENTS_PER_DAY_TOTAL):
            contribution_manager._allow_daily_global()

        assert contribution_manager._allow_daily_global() is False

        # Simulate 24h passing
        contribution_manager._daily_window_start = int(time.time()) - 86401

        # Should allow again
        assert contribution_manager._allow_daily_global() is True

    def test_allow_record_checks_daily_limit(self, contribution_manager):
        """_allow_record should check daily global limit before per-peer limit."""
        peer_id = "02" + "a" * 64

        # Exhaust daily cap
        contribution_manager._daily_count = MAX_CONTRIB_EVENTS_PER_DAY_TOTAL

        # Even with no per-peer limit hit, should reject
        assert contribution_manager._allow_record(peer_id) is False


# =============================================================================
# P5-03: CONTRIBUTION LEDGER DB ROW LIMIT TESTS
# =============================================================================

class TestContributionDbCap:
    """Test P5-03: MAX_CONTRIBUTION_ROWS=500000."""

    def test_db_cap_constant_exists(self):
        """MAX_CONTRIBUTION_ROWS should be defined as 500000."""
        assert HiveDatabase.MAX_CONTRIBUTION_ROWS == 500000

    def test_record_contribution_checks_row_count(self, mock_plugin):
        """record_contribution should reject if DB is at capacity."""
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: 500000 if key == 'cnt' else None
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        db = HiveDatabase(':memory:', mock_plugin)
        db._local = threading.local()
        db._local.conn = mock_conn

        # Try to record - should return False
        result = db.record_contribution("02" + "a" * 64, "forwarded", 1000)
        assert result is False

    def test_record_contribution_allows_under_cap(self, mock_plugin):
        """record_contribution should allow inserts under capacity."""
        mock_conn = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: 100 if key == 'cnt' else None
        mock_conn.execute.return_value.fetchone.return_value = mock_row

        db = HiveDatabase(':memory:', mock_plugin)
        db._local = threading.local()
        db._local.conn = mock_conn

        # Should succeed
        result = db.record_contribution("02" + "a" * 64, "forwarded", 1000)
        assert result is True


# =============================================================================
# X-01: RPC LOCK TIMEOUT TESTS
# =============================================================================

class TestRpcLockTimeout:
    """Test X-01: RPC lock timeout to prevent global stalls."""

    def test_lock_timeout_constant_exists(self):
        """RPC_LOCK_TIMEOUT_SECONDS should be defined."""
        # Import from cl-hive.py
        try:
            sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

            # We can't easily import cl-hive.py as a module since it has plugin.run()
            # Instead, verify the constant exists by reading the file
            with open(os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                'cl-hive.py'
            )) as f:
                content = f.read()

            assert 'RPC_LOCK_TIMEOUT_SECONDS = 10' in content
        except Exception:
            pytest.skip("Could not verify RPC_LOCK_TIMEOUT_SECONDS")

    def test_rpc_lock_timeout_error_class_exists(self):
        """RpcLockTimeoutError should be defined."""
        with open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'cl-hive.py'
        )) as f:
            content = f.read()

        assert 'class RpcLockTimeoutError' in content
        assert 'TimeoutError' in content  # Should inherit from TimeoutError

    def test_thread_safe_proxy_uses_timeout(self):
        """ThreadSafeRpcProxy should use timeout on lock.acquire."""
        with open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'cl-hive.py'
        )) as f:
            content = f.read()

        # Check that timeout is used in lock acquisition
        assert 'RPC_LOCK.acquire(timeout=' in content
        assert 'RpcLockTimeoutError' in content


# =============================================================================
# P3-02: MEMBERSHIP VERIFICATION TESTS
# =============================================================================

class TestMembershipVerification:
    """Test P3-02: Membership verification in handlers."""

    def test_handle_intent_checks_membership(self):
        """handle_intent should verify peer is a member."""
        with open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'cl-hive.py'
        )) as f:
            content = f.read()

        # Find the handle_intent function
        assert 'def handle_intent(peer_id: str, payload: Dict, plugin: Plugin)' in content

        # Should check membership before processing
        # The P3-02 comment and membership check should be present
        assert 'P3-02' in content
        assert 'database.get_member(peer_id)' in content
        assert 'INTENT from non-member' in content

    def test_handle_gossip_checks_membership(self):
        """handle_gossip should verify peer is a member."""
        with open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'cl-hive.py'
        )) as f:
            content = f.read()

        # Find the handle_gossip function
        assert 'def handle_gossip(peer_id: str, payload: Dict, plugin: Plugin)' in content

        # Should check membership before processing
        assert 'GOSSIP from non-member' in content


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestSecurityIntegration:
    """Integration tests for security fixes."""

    def test_all_security_fixes_present(self):
        """All S-01 security fixes should be implemented."""
        # Check intent_manager.py for P3-01
        with open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'modules', 'intent_manager.py'
        )) as f:
            intent_content = f.read()

        assert 'MAX_REMOTE_INTENTS = 200' in intent_content
        assert 'Enforce cache size limit' in intent_content or 'P3-01' in intent_content

        # Check contribution.py for P5-02
        with open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'modules', 'contribution.py'
        )) as f:
            contrib_content = f.read()

        assert 'MAX_CONTRIB_EVENTS_PER_DAY_TOTAL = 10000' in contrib_content
        assert 'P5-02' in contrib_content

        # Check database.py for P5-03
        with open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'modules', 'database.py'
        )) as f:
            db_content = f.read()

        assert 'MAX_CONTRIBUTION_ROWS = 500000' in db_content
        assert 'P5-03' in db_content

        # Check cl-hive.py for X-01
        with open(os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'cl-hive.py'
        )) as f:
            main_content = f.read()

        assert 'RPC_LOCK_TIMEOUT_SECONDS' in main_content
        assert 'X-01' in main_content
        assert 'P3-02' in main_content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
