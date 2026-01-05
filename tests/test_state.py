"""
Tests for Phase 2: State Management

Tests the StateManager and GossipManager modules for:
- Deterministic state hash calculation
- Gossip threshold logic
- Anti-entropy (FULL_SYNC) state merging
- Database persistence

Author: Lightning Goats Team
"""

import hashlib
import json
import pytest
import time
from unittest.mock import MagicMock, patch

# Import modules under test
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.state_manager import StateManager, HivePeerState
from modules.gossip import GossipManager, GossipState, CAPACITY_CHANGE_THRESHOLD


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_database():
    """Create a mock database for testing."""
    db = MagicMock()
    db.get_all_hive_states.return_value = []
    db.update_hive_state.return_value = None
    return db


@pytest.fixture
def mock_plugin():
    """Create a mock plugin for logging."""
    plugin = MagicMock()
    plugin.log = MagicMock()
    return plugin


@pytest.fixture
def state_manager(mock_database, mock_plugin):
    """Create a StateManager with mocked dependencies."""
    return StateManager(mock_database, mock_plugin)


@pytest.fixture
def gossip_manager(state_manager, mock_plugin):
    """Create a GossipManager with mocked dependencies."""
    return GossipManager(state_manager, mock_plugin, heartbeat_interval=300)


# =============================================================================
# STATE MANAGER TESTS
# =============================================================================

class TestHivePeerState:
    """Test the HivePeerState dataclass."""
    
    def test_to_dict_round_trip(self):
        """HivePeerState should survive dict conversion."""
        original = HivePeerState(
            peer_id="02" + "a" * 64,
            capacity_sats=1000000,
            available_sats=500000,
            fee_policy={"base_fee": 1000, "fee_rate": 100},
            topology=["peer1", "peer2"],
            version=5,
            last_update=int(time.time()),
            state_hash="abc123"
        )
        
        as_dict = original.to_dict()
        restored = HivePeerState.from_dict(as_dict)
        
        assert restored.peer_id == original.peer_id
        assert restored.capacity_sats == original.capacity_sats
        assert restored.version == original.version
    
    def test_to_hash_tuple(self):
        """Hash tuple should only contain minimal fields."""
        state = HivePeerState(
            peer_id="02" + "b" * 64,
            capacity_sats=2000000,
            available_sats=1000000,
            fee_policy={"base_fee": 500},
            topology=["external1"],
            version=10,
            last_update=1234567890,
            state_hash="xyz"
        )
        
        hash_tuple = state.to_hash_tuple()
        
        # Should only have these 3 fields
        assert set(hash_tuple.keys()) == {'peer_id', 'version', 'timestamp'}
        assert hash_tuple['peer_id'] == state.peer_id
        assert hash_tuple['version'] == 10
        assert hash_tuple['timestamp'] == 1234567890
        
        # Should NOT include sensitive data
        assert 'capacity_sats' not in hash_tuple
        assert 'fee_policy' not in hash_tuple


class TestStateManagerHashCalculation:
    """Test deterministic state hash calculation."""
    
    def test_empty_state_produces_hash(self, state_manager):
        """Empty state should produce a valid hash."""
        hash_result = state_manager.calculate_fleet_hash()
        
        # Should be valid hex SHA256 (64 chars)
        assert len(hash_result) == 64
        assert all(c in '0123456789abcdef' for c in hash_result)
    
    def test_identical_states_produce_identical_hashes(self, state_manager):
        """Same states should produce same hash."""
        # Add some states
        state_manager._local_state["peer_a"] = HivePeerState(
            peer_id="peer_a", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=1, last_update=1000
        )
        state_manager._local_state["peer_b"] = HivePeerState(
            peer_id="peer_b", capacity_sats=2000, available_sats=1000,
            fee_policy={}, topology=[], version=2, last_update=2000
        )
        
        hash1 = state_manager.calculate_fleet_hash()
        hash2 = state_manager.calculate_fleet_hash()
        
        assert hash1 == hash2
    
    def test_scrambled_order_same_hash(self, mock_database, mock_plugin):
        """Adding states in different order should produce same hash."""
        # Create two managers
        sm1 = StateManager(mock_database, mock_plugin)
        sm2 = StateManager(mock_database, mock_plugin)
        
        state_a = HivePeerState(
            peer_id="aaa", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=1, last_update=1000
        )
        state_b = HivePeerState(
            peer_id="bbb", capacity_sats=2000, available_sats=1000,
            fee_policy={}, topology=[], version=2, last_update=2000
        )
        state_c = HivePeerState(
            peer_id="ccc", capacity_sats=3000, available_sats=1500,
            fee_policy={}, topology=[], version=3, last_update=3000
        )
        
        # Add in different orders
        sm1._local_state["aaa"] = state_a
        sm1._local_state["bbb"] = state_b
        sm1._local_state["ccc"] = state_c
        
        sm2._local_state["ccc"] = state_c
        sm2._local_state["aaa"] = state_a
        sm2._local_state["bbb"] = state_b
        
        # Hashes should match (sorted by peer_id)
        assert sm1.calculate_fleet_hash() == sm2.calculate_fleet_hash()
    
    def test_different_versions_different_hash(self, state_manager):
        """Different version numbers should produce different hash."""
        state_manager._local_state["peer_x"] = HivePeerState(
            peer_id="peer_x", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=1, last_update=1000
        )
        hash1 = state_manager.calculate_fleet_hash()
        
        # Update version
        state_manager._local_state["peer_x"] = HivePeerState(
            peer_id="peer_x", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=2, last_update=1000  # version changed
        )
        hash2 = state_manager.calculate_fleet_hash()
        
        assert hash1 != hash2


class TestStateManagerUpdates:
    """Test state update logic."""
    
    def test_update_peer_state_new_peer(self, state_manager):
        """New peer state should be accepted."""
        gossip_data = {
            "capacity_sats": 5000000,
            "available_sats": 2500000,
            "fee_policy": {"base_fee": 1000},
            "topology": ["external_peer"],
            "version": 1,
            "timestamp": int(time.time())
        }
        
        result = state_manager.update_peer_state("new_peer", gossip_data)
        
        assert result is True
        assert "new_peer" in state_manager._local_state
    
    def test_update_peer_state_newer_version(self, state_manager):
        """Newer version should update existing state."""
        # Add initial state
        state_manager._local_state["peer_1"] = HivePeerState(
            peer_id="peer_1", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=5, last_update=1000
        )
        
        # Update with higher version
        gossip_data = {
            "capacity_sats": 2000,
            "version": 6,
            "timestamp": 2000
        }
        
        result = state_manager.update_peer_state("peer_1", gossip_data)
        
        assert result is True
        assert state_manager._local_state["peer_1"].version == 6
        assert state_manager._local_state["peer_1"].capacity_sats == 2000
    
    def test_update_peer_state_stale_rejected(self, state_manager):
        """Stale (lower version) gossip should be rejected."""
        # Add current state
        state_manager._local_state["peer_1"] = HivePeerState(
            peer_id="peer_1", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=10, last_update=1000
        )
        
        # Try to update with older version
        gossip_data = {
            "capacity_sats": 999,
            "version": 5,  # Older than 10
            "timestamp": 500
        }
        
        result = state_manager.update_peer_state("peer_1", gossip_data)
        
        assert result is False
        # Original should remain unchanged
        assert state_manager._local_state["peer_1"].version == 10
        assert state_manager._local_state["peer_1"].capacity_sats == 1000


class TestStateManagerFullSync:
    """Test FULL_SYNC state merging."""
    
    def test_apply_full_sync_empty(self, state_manager):
        """Empty FULL_SYNC should update nothing."""
        result = state_manager.apply_full_sync([])
        assert result == 0
    
    def test_apply_full_sync_new_peers(self, state_manager):
        """FULL_SYNC should add new peers."""
        remote_states = [
            {
                "peer_id": "remote_1",
                "capacity_sats": 1000,
                "available_sats": 500,
                "fee_policy": {},
                "topology": [],
                "version": 1,
                "last_update": 1000,
                "state_hash": ""
            },
            {
                "peer_id": "remote_2",
                "capacity_sats": 2000,
                "available_sats": 1000,
                "fee_policy": {},
                "topology": [],
                "version": 1,
                "last_update": 1000,
                "state_hash": ""
            }
        ]
        
        result = state_manager.apply_full_sync(remote_states)
        
        assert result == 2
        assert "remote_1" in state_manager._local_state
        assert "remote_2" in state_manager._local_state
    
    def test_apply_full_sync_prefers_higher_version(self, state_manager):
        """FULL_SYNC should only update if remote version is higher."""
        # Add local state with version 5
        state_manager._local_state["peer_x"] = HivePeerState(
            peer_id="peer_x", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=5, last_update=1000
        )
        
        remote_states = [
            {
                "peer_id": "peer_x",
                "capacity_sats": 9999,
                "available_sats": 4999,
                "fee_policy": {},
                "topology": [],
                "version": 3,  # Lower than 5
                "last_update": 500,
                "state_hash": ""
            }
        ]
        
        result = state_manager.apply_full_sync(remote_states)
        
        # Should not update
        assert result == 0
        assert state_manager._local_state["peer_x"].version == 5
        assert state_manager._local_state["peer_x"].capacity_sats == 1000


# =============================================================================
# GOSSIP MANAGER TESTS
# =============================================================================

class TestGossipThresholds:
    """Test gossip threshold logic."""
    
    def test_should_broadcast_first_time(self, gossip_manager):
        """First broadcast should always trigger (heartbeat)."""
        result = gossip_manager.should_broadcast(
            new_capacity=1000000,
            new_available=500000,
            new_fee_policy={"base_fee": 1000},
            new_topology=[]
        )
        
        # Heartbeat exceeded (last_broadcast = 0)
        assert result is True
    
    def test_should_broadcast_forced_status(self, gossip_manager):
        """force_status=True should always broadcast."""
        # Set recent broadcast
        gossip_manager._last_broadcast_state.last_broadcast = int(time.time())
        
        result = gossip_manager.should_broadcast(
            new_capacity=0,
            new_available=0,
            new_fee_policy={},
            new_topology=[],
            force_status=True
        )
        
        assert result is True
    
    def test_should_broadcast_capacity_change_above_threshold(self, gossip_manager):
        """Capacity change > 10% should trigger broadcast."""
        now = int(time.time())
        gossip_manager._last_broadcast_state = GossipState(
            capacity_sats=1000000,
            last_broadcast=now  # Recent
        )
        
        # 15% increase (above 10% threshold)
        new_capacity = 1150000
        
        result = gossip_manager.should_broadcast(
            new_capacity=new_capacity,
            new_available=500000,
            new_fee_policy={},
            new_topology=[]
        )
        
        assert result is True
    
    def test_should_broadcast_capacity_change_below_threshold(self, gossip_manager):
        """Capacity change < 10% should NOT trigger broadcast."""
        now = int(time.time())
        gossip_manager._last_broadcast_state = GossipState(
            capacity_sats=1000000,
            last_broadcast=now  # Recent
        )
        
        # 9% increase (below 10% threshold)
        new_capacity = 1090000
        
        result = gossip_manager.should_broadcast(
            new_capacity=new_capacity,
            new_available=500000,
            new_fee_policy={},
            new_topology=[]
        )
        
        assert result is False
    
    def test_should_broadcast_fee_change(self, gossip_manager):
        """Any fee policy change should trigger broadcast."""
        now = int(time.time())
        gossip_manager._last_broadcast_state = GossipState(
            capacity_sats=1000000,
            fee_policy={"base_fee": 1000, "fee_rate": 100},
            last_broadcast=now
        )
        
        # Change fee_rate
        new_fee_policy = {"base_fee": 1000, "fee_rate": 200}  # fee_rate changed
        
        result = gossip_manager.should_broadcast(
            new_capacity=1000000,  # Same
            new_available=500000,
            new_fee_policy=new_fee_policy,
            new_topology=[]
        )
        
        assert result is True
    
    def test_should_broadcast_topology_change(self, gossip_manager):
        """Topology change should trigger broadcast."""
        now = int(time.time())
        gossip_manager._last_broadcast_state = GossipState(
            capacity_sats=1000000,
            topology=["peer_a", "peer_b"],
            last_broadcast=now
        )
        
        # Add new external peer
        new_topology = ["peer_a", "peer_b", "peer_c"]
        
        result = gossip_manager.should_broadcast(
            new_capacity=1000000,
            new_available=500000,
            new_fee_policy={},
            new_topology=new_topology
        )
        
        assert result is True
    
    def test_should_broadcast_heartbeat_timeout(self, gossip_manager):
        """Heartbeat timeout should trigger broadcast."""
        old_time = int(time.time()) - 400  # 400s ago (> 300s heartbeat)
        gossip_manager._last_broadcast_state = GossipState(
            capacity_sats=1000000,
            fee_policy={"base_fee": 1000},
            topology=["peer_a"],
            last_broadcast=old_time
        )
        
        # No changes, but heartbeat expired
        result = gossip_manager.should_broadcast(
            new_capacity=1000000,  # Same
            new_available=500000,
            new_fee_policy={"base_fee": 1000},  # Same
            new_topology=["peer_a"]  # Same
        )
        
        assert result is True


class TestGossipProcessing:
    """Test gossip message processing."""
    
    def test_process_gossip_valid(self, gossip_manager):
        """Valid gossip should be accepted."""
        peer_id = "02" + "a" * 64
        payload = {
            "peer_id": peer_id,
            "capacity_sats": 1000000,
            "version": 1,
            "timestamp": int(time.time())
        }
        
        result = gossip_manager.process_gossip(peer_id, payload)
        
        assert result is True
        assert peer_id in gossip_manager._active_peers
    
    def test_process_gossip_sender_mismatch(self, gossip_manager):
        """Gossip with mismatched sender should be rejected."""
        sender = "02" + "a" * 64
        payload = {
            "peer_id": "02" + "b" * 64,  # Different!
            "version": 1,
            "timestamp": int(time.time())
        }
        
        result = gossip_manager.process_gossip(sender, payload)
        
        assert result is False
    
    def test_process_gossip_missing_fields(self, gossip_manager):
        """Gossip with missing required fields should be rejected."""
        sender = "02" + "c" * 64
        payload = {
            "peer_id": sender,
            # Missing version and timestamp
        }
        
        result = gossip_manager.process_gossip(sender, payload)
        
        assert result is False


class TestStateHashExchange:
    """Test STATE_HASH anti-entropy logic."""
    
    def test_process_state_hash_match(self, gossip_manager, state_manager):
        """Matching hashes should return True."""
        # Get our local hash
        local_hash = state_manager.calculate_fleet_hash()
        
        payload = {
            "fleet_hash": local_hash,
            "peer_count": 0,
            "timestamp": int(time.time())
        }
        
        result = gossip_manager.process_state_hash("some_peer", payload)
        
        assert result is True
    
    def test_process_state_hash_mismatch(self, gossip_manager, state_manager):
        """Mismatched hashes should return False."""
        payload = {
            "fleet_hash": "0" * 64,  # Definitely different
            "peer_count": 5,
            "timestamp": int(time.time())
        }
        
        # Add a state so our hash is different from all-zeros
        state_manager._local_state["test_peer"] = HivePeerState(
            peer_id="test_peer", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=1, last_update=1000
        )
        
        result = gossip_manager.process_state_hash("some_peer", payload)
        
        assert result is False


class TestFullSyncPayload:
    """Test FULL_SYNC payload creation."""
    
    def test_create_full_sync_payload(self, gossip_manager, state_manager):
        """FULL_SYNC payload should contain all states."""
        # Add some states
        state_manager._local_state["peer_1"] = HivePeerState(
            peer_id="peer_1", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=1, last_update=1000
        )
        state_manager._local_state["peer_2"] = HivePeerState(
            peer_id="peer_2", capacity_sats=2000, available_sats=1000,
            fee_policy={}, topology=[], version=2, last_update=2000
        )
        
        payload = gossip_manager.create_full_sync_payload()
        
        assert 'states' in payload
        assert 'fleet_hash' in payload
        assert 'timestamp' in payload
        assert len(payload['states']) == 2


class TestAntiEntropyFull:
    """Integration test for anti-entropy flow."""
    
    def test_divergence_detection_and_sync(self, mock_database, mock_plugin):
        """Simulate two nodes detecting and resolving state divergence."""
        # Create two independent state managers
        sm1 = StateManager(mock_database, mock_plugin)
        sm2 = StateManager(mock_database, mock_plugin)
        
        gm1 = GossipManager(sm1, mock_plugin)
        gm2 = GossipManager(sm2, mock_plugin)
        
        # Node 1 has state A
        sm1._local_state["peer_a"] = HivePeerState(
            peer_id="peer_a", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=1, last_update=1000
        )
        
        # Node 2 has state A + B
        sm2._local_state["peer_a"] = HivePeerState(
            peer_id="peer_a", capacity_sats=1000, available_sats=500,
            fee_policy={}, topology=[], version=1, last_update=1000
        )
        sm2._local_state["peer_b"] = HivePeerState(
            peer_id="peer_b", capacity_sats=2000, available_sats=1000,
            fee_policy={}, topology=[], version=1, last_update=1000
        )
        
        # Hashes should differ
        hash1 = sm1.calculate_fleet_hash()
        hash2 = sm2.calculate_fleet_hash()
        assert hash1 != hash2
        
        # Node 1 receives STATE_HASH from Node 2
        state_hash_payload = gm2.create_state_hash_payload()
        diverged = not gm1.process_state_hash("node_2", state_hash_payload)
        
        assert diverged is True
        
        # Node 2 sends FULL_SYNC
        full_sync_payload = gm2.create_full_sync_payload()
        
        # Node 1 applies FULL_SYNC
        updated = gm1.process_full_sync("node_2", full_sync_payload)
        
        assert updated == 1  # peer_b was added
        
        # Now hashes should match
        hash1_after = sm1.calculate_fleet_hash()
        hash2_after = sm2.calculate_fleet_hash()
        
        assert hash1_after == hash2_after


# =============================================================================
# PERSISTENCE TESTS
# =============================================================================

class TestDatabasePersistence:
    """Test state persistence via database."""
    
    def test_load_from_database(self, mock_plugin):
        """StateManager should load states from database on init."""
        mock_db = MagicMock()
        mock_db.get_all_hive_states.return_value = [
            {
                "peer_id": "db_peer_1",
                "capacity_sats": 5000000,
                "available_sats": 2500000,
                "fee_policy": {"base_fee": 1000},
                "topology": ["ext_1"],
                "version": 3,
                "last_gossip": 9999,
                "state_hash": "abc"
            }
        ]
        
        sm = StateManager(mock_db, mock_plugin)
        loaded = sm.load_from_database()
        
        assert loaded == 1
        assert "db_peer_1" in sm._local_state
        assert sm._local_state["db_peer_1"].version == 3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
