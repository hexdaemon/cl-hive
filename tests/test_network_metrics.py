"""
Tests for NetworkMetrics module.

Tests the NetworkMetricsCalculator class for:
- Topology snapshot building
- Member metrics calculation (unique peers, bridge score, centrality)
- Cache validity and invalidation
- Rebalance hub ranking

Author: Lightning Goats Team
"""

import pytest
import time
from unittest.mock import MagicMock, PropertyMock

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.network_metrics import (
    NetworkMetricsCalculator, MemberPositionMetrics, FleetTopologySnapshot,
    METRICS_CACHE_TTL, MAX_EXTERNAL_CENTRALITY, MAX_UNIQUE_PEERS
)


# =============================================================================
# HELPERS
# =============================================================================

def make_peer_state(topology=None):
    """Create a mock peer state with a topology attribute."""
    state = MagicMock()
    state.topology = topology or []
    return state


def make_member(peer_id):
    """Create a member dict."""
    return {"peer_id": peer_id}


# Member IDs
MEMBER_A = "03" + "aa" * 32
MEMBER_B = "03" + "bb" * 32
MEMBER_C = "03" + "cc" * 32
EXTERNAL_1 = "03" + "e1" * 32
EXTERNAL_2 = "03" + "e2" * 32
EXTERNAL_3 = "03" + "e3" * 32
EXTERNAL_4 = "03" + "e4" * 32


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_database():
    """Create a mock database."""
    db = MagicMock()
    db.get_all_members.return_value = []
    return db


@pytest.fixture
def mock_state_manager():
    """Create a mock state manager."""
    sm = MagicMock()
    sm.get_peer_state.return_value = None
    return sm


@pytest.fixture
def calculator(mock_state_manager, mock_database):
    """Create a NetworkMetricsCalculator."""
    return NetworkMetricsCalculator(
        state_manager=mock_state_manager,
        database=mock_database,
        cache_ttl=300
    )


# =============================================================================
# TOPOLOGY SNAPSHOT TESTS
# =============================================================================

class TestTopologySnapshot:
    """Tests for building topology snapshots."""

    def test_basic_build(self, calculator, mock_database, mock_state_manager):
        """Build a basic topology snapshot with 2 members."""
        mock_database.get_all_members.return_value = [
            make_member(MEMBER_A),
            make_member(MEMBER_B),
        ]
        mock_state_manager.get_peer_state.side_effect = lambda pid: {
            MEMBER_A: make_peer_state([EXTERNAL_1, EXTERNAL_2]),
            MEMBER_B: make_peer_state([EXTERNAL_2, EXTERNAL_3]),
        }.get(pid)

        snapshot = calculator._build_topology_snapshot()
        assert snapshot is not None
        assert MEMBER_A in snapshot.all_members
        assert MEMBER_B in snapshot.all_members
        assert EXTERNAL_1 in snapshot.all_external_peers
        assert EXTERNAL_2 in snapshot.all_external_peers
        assert EXTERNAL_3 in snapshot.all_external_peers
        assert snapshot.total_unique_coverage == 3

    def test_empty_members(self, calculator, mock_database):
        """No members → returns None."""
        mock_database.get_all_members.return_value = []
        snapshot = calculator._build_topology_snapshot()
        assert snapshot is None

    def test_missing_state(self, calculator, mock_database, mock_state_manager):
        """Members with no state get empty topologies."""
        mock_database.get_all_members.return_value = [make_member(MEMBER_A)]
        mock_state_manager.get_peer_state.return_value = None

        snapshot = calculator._build_topology_snapshot()
        assert snapshot is not None
        assert MEMBER_A in snapshot.all_members
        assert snapshot.member_topologies[MEMBER_A] == set()


# =============================================================================
# MEMBER METRICS TESTS
# =============================================================================

class TestMemberMetrics:
    """Tests for individual member metric calculation."""

    def _setup_fleet(self, calculator, mock_database, mock_state_manager,
                     member_topologies):
        """Setup a fleet with specific topologies.

        member_topologies: dict of member_id -> list of external peer ids
        """
        members = [make_member(mid) for mid in member_topologies]
        mock_database.get_all_members.return_value = members

        def get_state(pid):
            if pid in member_topologies:
                return make_peer_state(member_topologies[pid])
            return make_peer_state([])

        mock_state_manager.get_peer_state.side_effect = get_state

    def test_unique_peers(self, calculator, mock_database, mock_state_manager):
        """Unique peers = peers only this member connects to."""
        self._setup_fleet(calculator, mock_database, mock_state_manager, {
            MEMBER_A: [EXTERNAL_1, EXTERNAL_2, EXTERNAL_3],
            MEMBER_B: [EXTERNAL_2, EXTERNAL_3],
        })

        metrics = calculator.get_member_metrics(MEMBER_A)
        assert metrics is not None
        assert metrics.unique_peers == 1  # EXTERNAL_1
        assert EXTERNAL_1 in metrics.unique_peer_list

    def test_bridge_score(self, calculator, mock_database, mock_state_manager):
        """Bridge score = unique_peers / total_peers."""
        self._setup_fleet(calculator, mock_database, mock_state_manager, {
            MEMBER_A: [EXTERNAL_1, EXTERNAL_2],  # 1 unique of 2 → 0.5
            MEMBER_B: [EXTERNAL_2],
        })

        metrics = calculator.get_member_metrics(MEMBER_A)
        assert metrics is not None
        assert metrics.bridge_score == pytest.approx(0.5, abs=0.01)

    def test_external_centrality(self, calculator, mock_database, mock_state_manager):
        """External centrality scales with relative connectivity."""
        self._setup_fleet(calculator, mock_database, mock_state_manager, {
            MEMBER_A: [EXTERNAL_1, EXTERNAL_2, EXTERNAL_3, EXTERNAL_4],
            MEMBER_B: [EXTERNAL_1],
        })

        metrics_a = calculator.get_member_metrics(MEMBER_A)
        metrics_b = calculator.get_member_metrics(MEMBER_B)
        assert metrics_a.external_centrality > metrics_b.external_centrality

    def test_hive_centrality(self, calculator, mock_database, mock_state_manager):
        """Hive centrality = fraction of fleet directly connected."""
        self._setup_fleet(calculator, mock_database, mock_state_manager, {
            MEMBER_A: [EXTERNAL_1],
            MEMBER_B: [EXTERNAL_2],
            MEMBER_C: [EXTERNAL_3],
        })

        metrics_a = calculator.get_member_metrics(MEMBER_A)
        # A can see B and C (they have state), so 2/(3-1) = 1.0
        assert metrics_a is not None
        assert metrics_a.hive_centrality > 0

    def test_reachability(self, calculator, mock_database, mock_state_manager):
        """Hive reachability counts members reachable in 1-2 hops."""
        self._setup_fleet(calculator, mock_database, mock_state_manager, {
            MEMBER_A: [EXTERNAL_1],
            MEMBER_B: [EXTERNAL_2],
            MEMBER_C: [EXTERNAL_3],
        })

        metrics_a = calculator.get_member_metrics(MEMBER_A)
        assert metrics_a is not None
        assert metrics_a.hive_reachability > 0

    def test_overall_position_score(self, calculator, mock_database, mock_state_manager):
        """Overall position score combines centrality, unique peers, bridge."""
        self._setup_fleet(calculator, mock_database, mock_state_manager, {
            MEMBER_A: [EXTERNAL_1, EXTERNAL_2, EXTERNAL_3],
            MEMBER_B: [EXTERNAL_2],
        })

        metrics = calculator.get_member_metrics(MEMBER_A)
        assert metrics is not None
        assert metrics.overall_position_score > 0
        assert metrics.overall_position_score <= 1.0


# =============================================================================
# CACHING TESTS
# =============================================================================

class TestCaching:
    """Tests for cache validity and invalidation."""

    def test_cache_valid_within_ttl(self, calculator, mock_database, mock_state_manager):
        """Cache is valid within TTL window."""
        mock_database.get_all_members.return_value = [make_member(MEMBER_A)]
        mock_state_manager.get_peer_state.return_value = make_peer_state([EXTERNAL_1])

        # First call populates cache
        calculator.get_all_metrics()
        call_count_1 = mock_database.get_all_members.call_count

        # Second call uses cache
        calculator.get_all_metrics()
        call_count_2 = mock_database.get_all_members.call_count

        assert call_count_2 == call_count_1

    def test_cache_expired_recalculates(self, calculator, mock_database, mock_state_manager):
        """Expired cache triggers recalculation."""
        mock_database.get_all_members.return_value = [make_member(MEMBER_A)]
        mock_state_manager.get_peer_state.return_value = make_peer_state([EXTERNAL_1])

        calculator.get_all_metrics()
        call_count_1 = mock_database.get_all_members.call_count

        # Expire cache
        calculator._cache_time = int(time.time()) - calculator.cache_ttl - 1

        calculator.get_all_metrics()
        call_count_2 = mock_database.get_all_members.call_count

        assert call_count_2 > call_count_1

    def test_invalidate_cache_forces_recalc(self, calculator, mock_database, mock_state_manager):
        """invalidate_cache() forces recalculation on next call."""
        mock_database.get_all_members.return_value = [make_member(MEMBER_A)]
        mock_state_manager.get_peer_state.return_value = make_peer_state([EXTERNAL_1])

        calculator.get_all_metrics()
        call_count_1 = mock_database.get_all_members.call_count

        calculator.invalidate_cache()

        calculator.get_all_metrics()
        call_count_2 = mock_database.get_all_members.call_count

        assert call_count_2 > call_count_1

    def test_force_refresh_bypasses_cache(self, calculator, mock_database, mock_state_manager):
        """force_refresh=True bypasses cache."""
        mock_database.get_all_members.return_value = [make_member(MEMBER_A)]
        mock_state_manager.get_peer_state.return_value = make_peer_state([EXTERNAL_1])

        calculator.get_all_metrics()
        call_count_1 = mock_database.get_all_members.call_count

        calculator.get_all_metrics(force_refresh=True)
        call_count_2 = mock_database.get_all_members.call_count

        assert call_count_2 > call_count_1


# =============================================================================
# REBALANCE HUB TESTS
# =============================================================================

class TestRebalanceHubs:
    """Tests for rebalance hub ranking."""

    def test_hub_ordering(self, calculator, mock_database, mock_state_manager):
        """Hubs sorted by rebalance_hub_score descending."""
        mock_database.get_all_members.return_value = [
            make_member(MEMBER_A),
            make_member(MEMBER_B),
            make_member(MEMBER_C),
        ]

        def get_state(pid):
            topologies = {
                MEMBER_A: [EXTERNAL_1, EXTERNAL_2, EXTERNAL_3, EXTERNAL_4],
                MEMBER_B: [EXTERNAL_1],
                MEMBER_C: [EXTERNAL_1, EXTERNAL_2],
            }
            return make_peer_state(topologies.get(pid, []))

        mock_state_manager.get_peer_state.side_effect = get_state

        hubs = calculator.get_rebalance_hubs(top_n=3)
        assert len(hubs) > 0
        # Should be ordered by hub score descending
        scores = [h.rebalance_hub_score for h in hubs]
        assert scores == sorted(scores, reverse=True)

    def test_empty_fleet_no_hubs(self, calculator, mock_database):
        """Empty fleet returns no hubs."""
        mock_database.get_all_members.return_value = []
        hubs = calculator.get_rebalance_hubs()
        assert len(hubs) == 0

    def test_exclude_members(self, calculator, mock_database, mock_state_manager):
        """Excluded members don't appear in hub results."""
        mock_database.get_all_members.return_value = [
            make_member(MEMBER_A),
            make_member(MEMBER_B),
        ]
        mock_state_manager.get_peer_state.side_effect = lambda pid: make_peer_state([EXTERNAL_1])

        hubs = calculator.get_rebalance_hubs(exclude_members=[MEMBER_A])
        hub_ids = [h.member_id for h in hubs]
        assert MEMBER_A not in hub_ids


# =============================================================================
# FLEET HEALTH TESTS
# =============================================================================

class TestFleetHealth:
    """Tests for fleet health monitoring."""

    def test_fleet_health_empty(self, calculator, mock_database):
        """Empty fleet returns F grade."""
        mock_database.get_all_members.return_value = []
        health = calculator.get_fleet_health()
        assert health["health_grade"] == "F"
        assert health["member_count"] == 0

    def test_fleet_health_with_members(self, calculator, mock_database, mock_state_manager):
        """Fleet health computed from member metrics."""
        mock_database.get_all_members.return_value = [
            make_member(MEMBER_A),
            make_member(MEMBER_B),
        ]
        mock_state_manager.get_peer_state.side_effect = lambda pid: make_peer_state([EXTERNAL_1])

        health = calculator.get_fleet_health()
        assert health["member_count"] == 2
        assert "health_grade" in health
        assert health["health_score"] >= 0


# =============================================================================
# DATA CLASS TESTS
# =============================================================================

class TestMemberPositionMetricsDataclass:
    """Tests for MemberPositionMetrics dataclass."""

    def test_to_dict(self):
        """Verify to_dict serialization."""
        metrics = MemberPositionMetrics(
            member_id=MEMBER_A,
            external_centrality=0.05,
            unique_peers=3,
            bridge_score=0.6,
        )
        d = metrics.to_dict()
        assert d["member_id"] == MEMBER_A
        assert d["unique_peers"] == 3
        assert d["bridge_score"] == 0.6

    def test_default_values(self):
        """Default values are sensible zeros."""
        metrics = MemberPositionMetrics(member_id="test")
        assert metrics.external_centrality == 0.0
        assert metrics.unique_peers == 0
        assert metrics.hive_centrality == 0.0
        assert metrics.overall_position_score == 0.0
