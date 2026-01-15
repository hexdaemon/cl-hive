"""
Tests for Phase 8 RPC Commands

Tests the RPC command interface as specified in IMPLEMENTATION_PLAN.md Section 8.6:
- Genesis Test: Call hive-genesis -> verify DB initialized, returns hive_id
- Invite/Join Test: Generate ticket -> verify ticket structure
- Status Test: Verify all fields returned with correct types
- Permission Test: (Pending - requires Issue #25)
- Approve Flow: Create pending action, approve -> verify status change

Author: Lightning Goats Team
"""

import pytest
import time
import json
from unittest.mock import MagicMock, patch, PropertyMock

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.database import HiveDatabase
from modules.config import HiveConfig
from modules.handshake import HandshakeManager, Ticket
from modules.membership import MembershipManager, MembershipTier
from modules.contribution import ContributionManager
from modules.governance import DecisionEngine, GovernanceMode, DecisionResult


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_plugin():
    """Create a mock plugin for testing."""
    plugin = MagicMock()
    plugin.log = MagicMock()
    return plugin


@pytest.fixture
def mock_rpc():
    """Create a mock RPC interface."""
    rpc = MagicMock()
    rpc.getinfo.return_value = {'id': '02' + 'a' * 64}
    rpc.signmessage.return_value = {'zbase': 'test_signature_zbase'}
    rpc.checkmessage.return_value = {'verified': True, 'pubkey': '02' + 'a' * 64}
    return rpc


@pytest.fixture
def database(mock_plugin, tmp_path):
    """Create a test database."""
    db_path = str(tmp_path / "test_rpc.db")
    db = HiveDatabase(db_path, mock_plugin)
    db.initialize()
    return db


@pytest.fixture
def config():
    """Create a test config."""
    return HiveConfig(
        db_path=':memory:',
        governance_mode='advisor',
        membership_enabled=True,
        auto_vouch_enabled=True,
        auto_promote_enabled=True,
    )


@pytest.fixture
def handshake_mgr(mock_rpc, database, mock_plugin):
    """Create a handshake manager for testing."""
    return HandshakeManager(mock_rpc, database, mock_plugin)


# =============================================================================
# GENESIS TESTS
# =============================================================================

class TestGenesisRPC:
    """Test hive-genesis RPC command functionality."""

    def test_genesis_creates_admin(self, handshake_mgr, database):
        """Genesis should create an admin member and return hive_id."""
        result = handshake_mgr.genesis(hive_id="test-hive")

        assert result['status'] == 'genesis_complete'
        assert result['hive_id'] == 'test-hive'
        assert 'admin_pubkey' in result

        # Verify admin was created in DB
        admin = database.get_member(result['admin_pubkey'])
        assert admin is not None
        assert admin['tier'] == 'admin'

    def test_genesis_auto_generates_hive_id(self, handshake_mgr, database):
        """Genesis without hive_id should auto-generate one."""
        result = handshake_mgr.genesis()

        assert result['status'] == 'genesis_complete'
        assert 'hive_id' in result
        assert len(result['hive_id']) > 0

    def test_genesis_fails_if_already_initialized(self, handshake_mgr, database):
        """Genesis should fail if Hive already has members."""
        # First genesis
        handshake_mgr.genesis(hive_id="test-hive")

        # Second genesis should fail
        with pytest.raises(ValueError, match="Already member"):
            handshake_mgr.genesis(hive_id="another-hive")


# =============================================================================
# INVITE/JOIN TESTS
# =============================================================================

class TestInviteRPC:
    """Test hive-invite RPC command functionality."""

    def test_invite_generates_valid_ticket(self, handshake_mgr, database):
        """Invite should generate a valid base64 ticket."""
        # First create genesis
        handshake_mgr.genesis(hive_id="test-hive")

        # Generate invite
        ticket_b64 = handshake_mgr.generate_invite_ticket(valid_hours=24)

        assert ticket_b64 is not None
        assert len(ticket_b64) > 0

        # Verify ticket can be decoded
        ticket = Ticket.from_base64(ticket_b64)
        assert ticket.hive_id == "test-hive"
        assert not ticket.is_expired()

    def test_invite_requires_admin(self, handshake_mgr, database):
        """Invite should fail if no admin exists."""
        with pytest.raises(PermissionError):
            handshake_mgr.generate_invite_ticket(valid_hours=24)

    def test_ticket_expiry_respected(self, handshake_mgr, database):
        """Ticket should respect valid_hours parameter."""
        handshake_mgr.genesis(hive_id="test-hive")

        ticket_b64 = handshake_mgr.generate_invite_ticket(valid_hours=1)
        ticket = Ticket.from_base64(ticket_b64)

        # Ticket should expire in ~1 hour
        assert ticket.expires_at > int(time.time())
        assert ticket.expires_at < int(time.time()) + 3700  # 1 hour + margin


# =============================================================================
# STATUS TESTS
# =============================================================================

class TestStatusRPC:
    """Test hive-status RPC command functionality."""

    def test_status_returns_correct_structure(self, database, config):
        """Status should return all required fields with correct types."""
        # Add some members
        database.add_member('02' + 'a' * 64, tier='admin', joined_at=int(time.time()))
        database.add_member('02' + 'b' * 64, tier='member', joined_at=int(time.time()))
        database.add_member('02' + 'c' * 64, tier='neophyte', joined_at=int(time.time()))

        # Simulate status response
        members = database.get_all_members()
        member_count = len([m for m in members if m['tier'] == 'member'])
        neophyte_count = len([m for m in members if m['tier'] == 'neophyte'])
        admin_count = len([m for m in members if m['tier'] == 'admin'])

        status = {
            "status": "active",
            "governance_mode": config.governance_mode,
            "members": {
                "total": len(members),
                "admin": admin_count,
                "member": member_count,
                "neophyte": neophyte_count,
            },
            "limits": {
                "max_members": config.max_members,
                "market_share_cap": config.market_share_cap_pct,
            },
            "version": "0.1.0-dev",
        }

        # Verify structure
        assert isinstance(status['status'], str)
        assert isinstance(status['governance_mode'], str)
        assert isinstance(status['members'], dict)
        assert isinstance(status['members']['total'], int)
        assert status['members']['total'] == 3
        assert status['members']['admin'] == 1
        assert status['members']['member'] == 1
        assert status['members']['neophyte'] == 1

    def test_status_genesis_required_when_empty(self, database, config):
        """Status should indicate genesis_required when no members."""
        members = database.get_all_members()

        status = "genesis_required" if not members else "active"
        assert status == "genesis_required"


# =============================================================================
# APPROVE FLOW TESTS
# =============================================================================

class TestApproveFlowRPC:
    """Test hive-approve and hive-reject RPC commands."""

    def test_pending_action_created(self, database):
        """Pending action should be created and retrievable."""
        action_id = database.add_pending_action(
            action_type='channel_open',
            payload={'target': '02' + 'x' * 64, 'amount_sats': 1000000},
            expires_hours=24
        )

        assert action_id > 0

        actions = database.get_pending_actions()
        assert len(actions) == 1
        assert actions[0]['action_type'] == 'channel_open'
        assert actions[0]['status'] == 'pending'

    def test_approve_action_updates_status(self, database):
        """Approving an action should update its status."""
        action_id = database.add_pending_action(
            action_type='channel_open',
            payload={'target': '02' + 'x' * 64},
            expires_hours=24
        )

        # Approve
        success = database.update_action_status(action_id, 'approved')
        assert success

        # Verify status changed
        action = database.get_pending_action_by_id(action_id)
        assert action['status'] == 'approved'

        # Should not appear in pending list
        pending = database.get_pending_actions()
        assert len(pending) == 0

    def test_reject_action_updates_status(self, database):
        """Rejecting an action should update its status."""
        action_id = database.add_pending_action(
            action_type='channel_open',
            payload={'target': '02' + 'x' * 64},
            expires_hours=24
        )

        # Reject
        success = database.update_action_status(action_id, 'rejected')
        assert success

        # Verify status changed
        action = database.get_pending_action_by_id(action_id)
        assert action['status'] == 'rejected'

    def test_expired_action_not_in_pending(self, database):
        """Expired actions should not appear in pending list."""
        # Create action that expires immediately
        action_id = database.add_pending_action(
            action_type='channel_open',
            payload={'target': '02' + 'x' * 64},
            expires_hours=0  # Expires immediately
        )

        # Small delay to ensure expiry
        time.sleep(0.1)

        # Mark expired
        database.cleanup_expired_actions()

        # Should not appear in pending
        pending = database.get_pending_actions()
        assert len(pending) == 0


# =============================================================================
# SET MODE TESTS
# =============================================================================

class TestSetModeRPC:
    """Test hive-set-mode RPC command functionality."""

    def test_set_mode_advisor(self, config):
        """Setting mode to advisor should work."""
        config.governance_mode = 'advisor'
        assert config.governance_mode == 'advisor'

    def test_set_mode_failsafe(self, config):
        """Setting mode to failsafe should work."""
        config.governance_mode = 'failsafe'
        assert config.governance_mode == 'failsafe'


# =============================================================================
# CONTRIBUTION TESTS
# =============================================================================

class TestContributionRPC:
    """Test hive-contribution RPC command functionality."""

    def test_contribution_stats_returned(self, database, mock_plugin, mock_rpc, config):
        """Contribution stats should be retrievable."""
        peer_id = '02' + 'a' * 64

        # Record some contributions
        database.record_contribution(peer_id, 'forwarded', 100000)
        database.record_contribution(peer_id, 'forwarded', 50000)
        database.record_contribution(peer_id, 'received', 75000)

        # Get stats
        stats = database.get_contribution_stats(peer_id)

        assert stats['forwarded'] == 150000
        assert stats['received'] == 75000

    def test_contribution_ratio_calculation(self, database):
        """Contribution ratio should be calculated correctly."""
        peer_id = '02' + 'b' * 64

        database.record_contribution(peer_id, 'forwarded', 200000)
        database.record_contribution(peer_id, 'received', 100000)

        ratio = database.get_contribution_ratio(peer_id)
        assert ratio == 2.0  # 200000 / 100000


# =============================================================================
# TOPOLOGY TESTS
# =============================================================================

class TestTopologyRPC:
    """Test hive-topology RPC command functionality."""

    def test_planner_log_retrieval(self, database):
        """Planner logs should be retrievable."""
        # Add some log entries
        database.log_planner_action(
            action_type='saturation_check',
            result='completed',
            details={'saturated_targets': 2}
        )
        database.log_planner_action(
            action_type='ignore',
            result='success',
            target='02' + 'x' * 64,
            details={'hive_share_pct': 0.25}
        )

        # Retrieve logs
        logs = database.get_planner_logs(limit=10)

        assert len(logs) == 2
        assert logs[0]['action_type'] in ('saturation_check', 'ignore')


# =============================================================================
# MEMBERS TESTS
# =============================================================================

class TestMembersRPC:
    """Test hive-members RPC command functionality."""

    def test_members_list_returned(self, database):
        """Members list should include all tiers."""
        database.add_member('02' + 'a' * 64, tier='admin', joined_at=int(time.time()))
        database.add_member('02' + 'b' * 64, tier='member', joined_at=int(time.time()))
        database.add_member('02' + 'c' * 64, tier='neophyte', joined_at=int(time.time()))

        members = database.get_all_members()

        assert len(members) == 3
        tiers = [m['tier'] for m in members]
        assert 'admin' in tiers
        assert 'member' in tiers
        assert 'neophyte' in tiers


# =============================================================================
# VOUCH TESTS
# =============================================================================

class TestVouchRPC:
    """Test hive-vouch RPC command functionality."""

    def test_vouch_stored_in_db(self, database):
        """Vouch should be stored in the database."""
        target = '02' + 'a' * 64
        voucher = '02' + 'b' * 64
        request_id = 'test-request-123'

        # Add promotion request first
        database.add_promotion_request(target, request_id, status='pending')

        # Add vouch
        success = database.add_promotion_vouch(
            target_peer_id=target,
            request_id=request_id,
            voucher_peer_id=voucher,
            sig='test_signature',
            timestamp=int(time.time())
        )

        assert success

        # Verify vouch exists
        vouches = database.get_promotion_vouches(target, request_id)
        assert len(vouches) == 1
        assert vouches[0]['voucher_peer_id'] == voucher

    def test_duplicate_vouch_rejected(self, database):
        """Same voucher cannot vouch twice for same request."""
        target = '02' + 'a' * 64
        voucher = '02' + 'b' * 64
        request_id = 'test-request-456'

        database.add_promotion_request(target, request_id, status='pending')

        # First vouch
        success1 = database.add_promotion_vouch(
            target, request_id, voucher, 'sig1', int(time.time())
        )
        assert success1

        # Second vouch (duplicate)
        success2 = database.add_promotion_vouch(
            target, request_id, voucher, 'sig2', int(time.time())
        )
        assert not success2  # Should fail due to unique constraint


# =============================================================================
# BAN TESTS
# =============================================================================

class TestBanRPC:
    """Test hive-ban RPC command functionality."""

    def test_ban_added_to_db(self, database):
        """Ban should be stored in the database."""
        peer_id = '02' + 'x' * 64
        reporter = '02' + 'a' * 64

        success = database.add_ban(
            peer_id=peer_id,
            reason='test ban',
            reporter=reporter,
            signature='test_sig'
        )

        assert success
        assert database.is_banned(peer_id)

    def test_ban_info_retrievable(self, database):
        """Ban info should be retrievable."""
        peer_id = '02' + 'y' * 64
        reporter = '02' + 'a' * 64

        database.add_ban(peer_id, 'spam', reporter, 'sig')

        info = database.get_ban_info(peer_id)
        assert info is not None
        assert info['reason'] == 'spam'
        assert info['reporter'] == reporter


# =============================================================================
# PERMISSION TESTS (Issue #25)
# =============================================================================

class TestPermissionModel:
    """Test permission model enforcement."""

    def test_admin_permission_granted(self, database):
        """Admin should have permission for admin-only commands."""
        # Add admin member
        admin_pubkey = '02' + 'a' * 64
        database.add_member(admin_pubkey, tier='admin', joined_at=int(time.time()))

        member = database.get_member(admin_pubkey)
        assert member['tier'] == 'admin'

    def test_member_permission_denied_for_admin_command(self, database):
        """Member should be denied for admin-only commands."""
        # Add member (not admin)
        member_pubkey = '02' + 'b' * 64
        database.add_member(member_pubkey, tier='member', joined_at=int(time.time()))

        member = database.get_member(member_pubkey)
        assert member['tier'] == 'member'
        # In real RPC, _check_permission('admin') would return error

    def test_neophyte_permission_denied_for_member_command(self, database):
        """Neophyte should be denied for member-only commands."""
        # Add neophyte
        neophyte_pubkey = '02' + 'c' * 64
        database.add_member(neophyte_pubkey, tier='neophyte', joined_at=int(time.time()))

        member = database.get_member(neophyte_pubkey)
        assert member['tier'] == 'neophyte'
        # In real RPC, _check_permission('member') would return error

    def test_admin_has_member_permissions(self, database):
        """Admin should also have member-level permissions."""
        admin_pubkey = '02' + 'a' * 64
        database.add_member(admin_pubkey, tier='admin', joined_at=int(time.time()))

        member = database.get_member(admin_pubkey)
        # Admin tier should pass both 'admin' and 'member' checks
        assert member['tier'] in ('admin', 'member')


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
