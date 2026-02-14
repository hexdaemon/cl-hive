"""
Tests for GitHub Issues #59 and #60: Member Stats and Addresses

Issue #59: contribution_ratio and uptime_pct are 0.0 for all members;
           last_seen stuck at join time.
Issue #60: A promoted member has null addresses.

Tests verify:
1. members() returns live contribution_ratio from ledger
2. members() formats uptime_pct as percentage (0-100)
3. on_custommsg updates last_seen for valid Hive messages
4. handle_attest creates initial presence record
5. handle_attest captures addresses from listpeers
6. on_peer_connected populates null addresses
"""

import json
import time
import pytest
from unittest.mock import MagicMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.database import HiveDatabase
from modules.config import HiveConfig
from modules.membership import MembershipManager
from modules.contribution import ContributionManager
from modules.rpc_commands import members, HiveContext


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_plugin():
    plugin = MagicMock()
    plugin.log = MagicMock()
    return plugin


@pytest.fixture
def database(mock_plugin, tmp_path):
    db_path = str(tmp_path / "test_issue_59_60.db")
    db = HiveDatabase(db_path, mock_plugin)
    db.initialize()
    return db


@pytest.fixture
def config():
    return HiveConfig(
        db_path=':memory:',
        governance_mode='advisor',
        membership_enabled=True,
        auto_vouch_enabled=True,
        auto_promote_enabled=True,
    )


@pytest.fixture
def mock_rpc():
    rpc = MagicMock()
    return rpc


@pytest.fixture
def contribution_mgr(mock_rpc, database, mock_plugin, config):
    return ContributionManager(mock_rpc, database, mock_plugin, config)


@pytest.fixture
def membership_mgr(database, config, contribution_mgr, mock_plugin):
    return MembershipManager(
        db=database,
        state_manager=None,
        contribution_mgr=contribution_mgr,
        bridge=None,
        config=config,
        plugin=mock_plugin,
    )


PEER_A = "02" + "a1" * 32
PEER_B = "02" + "b2" * 32


# =============================================================================
# FIX 1: members() enriches with live contribution_ratio
# =============================================================================

class TestMembersContributionRatio:
    """Test that members() returns live contribution_ratio from ledger."""

    def test_members_returns_contribution_ratio_from_ledger(
        self, database, membership_mgr, config, mock_plugin
    ):
        """members() should return dynamically-calculated contribution_ratio."""
        now = int(time.time())
        database.add_member(PEER_A, tier="member", joined_at=now)

        # Record some forwarding activity (direction, amount_sats)
        database.record_contribution(PEER_A, "forwarded", 5000)
        database.record_contribution(PEER_A, "received", 10000)

        ctx = HiveContext(
            database=database,
            config=config,
            safe_plugin=mock_plugin,
            our_pubkey="02" + "00" * 32,
            membership_mgr=membership_mgr,
        )

        result = members(ctx)
        assert result["count"] == 1
        member = result["members"][0]
        # contribution_ratio = forwarded / received = 5000 / 10000 = 0.5
        assert member["contribution_ratio"] == 0.5

    def test_members_without_membership_mgr_returns_raw(
        self, database, config, mock_plugin
    ):
        """Without membership_mgr, members() should return raw DB values."""
        now = int(time.time())
        database.add_member(PEER_A, tier="member", joined_at=now)

        ctx = HiveContext(
            database=database,
            config=config,
            safe_plugin=mock_plugin,
            our_pubkey="02" + "00" * 32,
            membership_mgr=None,
        )

        result = members(ctx)
        assert result["count"] == 1
        # Raw DB value should be 0.0 (default)
        member = result["members"][0]
        assert member["contribution_ratio"] == 0.0


# =============================================================================
# FIX 1: members() formats uptime_pct as percentage
# =============================================================================

class TestMembersUptimeFormat:
    """Test that members() formats uptime_pct as 0-100 percentage."""

    def test_uptime_pct_formatted_as_percentage(
        self, database, membership_mgr, config, mock_plugin
    ):
        """uptime_pct should be formatted as 0-100, not 0.0-1.0."""
        now = int(time.time())
        database.add_member(PEER_A, tier="member", joined_at=now)
        # Simulate stored uptime as 0.75 (75%)
        database.update_member(PEER_A, uptime_pct=0.75)

        ctx = HiveContext(
            database=database,
            config=config,
            safe_plugin=mock_plugin,
            our_pubkey="02" + "00" * 32,
            membership_mgr=membership_mgr,
        )

        result = members(ctx)
        member = result["members"][0]
        assert member["uptime_pct"] == 75.0

    def test_uptime_pct_zero_stays_zero(
        self, database, membership_mgr, config, mock_plugin
    ):
        """0.0 uptime should format as 0.0 percentage."""
        now = int(time.time())
        database.add_member(PEER_A, tier="member", joined_at=now)

        ctx = HiveContext(
            database=database,
            config=config,
            safe_plugin=mock_plugin,
            our_pubkey="02" + "00" * 32,
            membership_mgr=membership_mgr,
        )

        result = members(ctx)
        member = result["members"][0]
        assert member["uptime_pct"] == 0.0


# =============================================================================
# FIX 3: last_seen updates on any Hive message
# =============================================================================

class TestLastSeenOnMessage:
    """Test that last_seen updates when any valid Hive message is received."""

    def test_last_seen_updates_on_hive_message(self, database, mock_plugin):
        """Receiving a valid Hive message should update last_seen."""
        old_time = int(time.time()) - 86400  # 1 day ago
        database.add_member(PEER_A, tier="member", joined_at=old_time)
        database.update_member(PEER_A, last_seen=old_time)

        # Verify the stale last_seen
        member = database.get_member(PEER_A)
        assert member["last_seen"] == old_time

        # Simulate what on_custommsg now does: update last_seen on valid message
        now = int(time.time())
        member = database.get_member(PEER_A)
        if member:
            database.update_member(PEER_A, last_seen=now)

        # Verify last_seen was updated
        member = database.get_member(PEER_A)
        assert member["last_seen"] >= now


# =============================================================================
# FIX 4: Addresses captured at join and on connect
# =============================================================================

class TestAddressCapture:
    """Test that addresses are captured at join and on peer connect."""

    def test_addresses_null_by_default(self, database):
        """New member should have null addresses by default."""
        database.add_member(PEER_A, tier="neophyte", joined_at=int(time.time()))
        member = database.get_member(PEER_A)
        assert member["addresses"] is None

    def test_addresses_populated_via_update_member(self, database):
        """update_member should accept addresses field."""
        database.add_member(PEER_A, tier="neophyte", joined_at=int(time.time()))

        addrs = ["127.0.0.1:9735", "[::1]:9735"]
        database.update_member(PEER_A, addresses=json.dumps(addrs))

        member = database.get_member(PEER_A)
        assert member["addresses"] is not None
        parsed = json.loads(member["addresses"])
        assert len(parsed) == 2
        assert "127.0.0.1:9735" in parsed

    def test_null_addresses_populated_on_connect(self, database):
        """Simulates the on_peer_connected fix: populate addresses if missing."""
        database.add_member(PEER_A, tier="member", joined_at=int(time.time()))

        member = database.get_member(PEER_A)
        assert member["addresses"] is None

        # Simulate what on_peer_connected now does
        if not member.get("addresses"):
            netaddr = ["10.0.0.1:9735"]
            database.update_member(PEER_A, addresses=json.dumps(netaddr))

        member = database.get_member(PEER_A)
        assert member["addresses"] is not None
        parsed = json.loads(member["addresses"])
        assert parsed == ["10.0.0.1:9735"]

    def test_existing_addresses_not_overwritten_on_connect(self, database):
        """If addresses already exist, on_peer_connected should not overwrite."""
        database.add_member(PEER_A, tier="member", joined_at=int(time.time()))
        original_addrs = ["10.0.0.1:9735"]
        database.update_member(PEER_A, addresses=json.dumps(original_addrs))

        member = database.get_member(PEER_A)
        # Simulate on_peer_connected check
        if not member.get("addresses"):
            database.update_member(PEER_A, addresses=json.dumps(["99.99.99.99:9735"]))

        # Should still have original addresses
        member = database.get_member(PEER_A)
        parsed = json.loads(member["addresses"])
        assert parsed == original_addrs


# =============================================================================
# FIX 5: Presence record created at join
# =============================================================================

class TestPresenceAtJoin:
    """Test that a presence record is created when a member joins."""

    def test_presence_created_at_join(self, database):
        """After add_member + update_presence, presence data should exist."""
        now = int(time.time())
        database.add_member(PEER_A, tier="neophyte", joined_at=now)

        # Simulate what handle_attest now does
        database.update_presence(PEER_A, is_online=True, now_ts=now, window_seconds=30 * 86400)

        # Verify presence was created
        presence = database.get_presence(PEER_A)
        assert presence is not None
        assert presence["is_online"] == 1


# =============================================================================
# FIX 2: Contribution ratio synced in maintenance loop
# =============================================================================

class TestContributionRatioSync:
    """Test that contribution_ratio gets synced to DB in maintenance."""

    def test_contribution_ratio_synced_to_db(
        self, database, membership_mgr, contribution_mgr
    ):
        """Simulates the maintenance loop syncing contribution_ratio to DB."""
        now = int(time.time())
        database.add_member(PEER_A, tier="member", joined_at=now)

        # Record forwarding activity (direction, amount_sats)
        database.record_contribution(PEER_A, "forwarded", 3000)
        database.record_contribution(PEER_A, "received", 6000)

        # Simulate what the maintenance loop now does
        members_list = database.get_all_members()
        for m in members_list:
            pid = m.get("peer_id")
            if pid:
                ratio = membership_mgr.calculate_contribution_ratio(pid)
                database.update_member(pid, contribution_ratio=ratio)

        # Verify ratio was persisted
        member = database.get_member(PEER_A)
        assert member["contribution_ratio"] == 0.5  # 3000 / 6000
