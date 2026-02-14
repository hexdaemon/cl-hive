"""
Tests for 17 bug fixes across high-priority modules:
- cost_reduction.py (7 fixes)
- liquidity_coordinator.py (6 fixes)
- splice_coordinator.py (1 fix)
- budget_manager.py (3 fixes)

Tests cover thread safety, bounded data structures, cache eviction,
and correctness improvements.
"""

import threading
import time
import pytest
from unittest.mock import MagicMock, patch
from collections import defaultdict

from modules.cost_reduction import (
    CircularFlowDetector,
    CostReductionManager,
    FleetRebalanceRouter,
)
from modules.liquidity_coordinator import (
    LiquidityCoordinator,
    LiquidityNeed,
    URGENCY_HIGH,
    URGENCY_MEDIUM,
    NEED_INBOUND,
    NEED_OUTBOUND,
)
from modules.splice_coordinator import (
    SpliceCoordinator,
    CHANNEL_CACHE_TTL,
    MAX_CHANNEL_CACHE_SIZE,
)
from modules.budget_manager import (
    BudgetHoldManager,
    BudgetHold,
    MAX_CONCURRENT_HOLDS,
    CLEANUP_INTERVAL_SECONDS,
)


# =============================================================================
# FIXTURES
# =============================================================================

OUR_PUBKEY = "03" + "a1" * 32
MEMBER_A = "02" + "bb" * 32
MEMBER_B = "02" + "cc" * 32
MEMBER_C = "02" + "dd" * 32


class MockPlugin:
    def __init__(self):
        self.logs = []
        self.rpc = MockRpc()

    def log(self, msg, level="info"):
        self.logs.append({"msg": msg, "level": level})


class MockRpc:
    def __init__(self):
        self.channels = []

    def listpeerchannels(self, **kwargs):
        peer_id = kwargs.get("id")
        if peer_id:
            return {"channels": [c for c in self.channels if c.get("peer_id") == peer_id]}
        return {"channels": self.channels}

    def listchannels(self, **kwargs):
        return {"channels": []}

    def listfunds(self):
        return {"channels": []}


class MockStateManager:
    def __init__(self):
        self.peer_states = {}

    def get_peer_state(self, peer_id):
        return self.peer_states.get(peer_id)

    def get_all_peer_states(self):
        return list(self.peer_states.values())

    def set_peer_state(self, peer_id, capacity=0, topology=None):
        state = MagicMock()
        state.peer_id = peer_id
        state.capacity_sats = capacity
        state.topology = topology or []
        self.peer_states[peer_id] = state


class MockDatabase:
    def __init__(self):
        self.members = {}
        self.member_health = {}
        self.liquidity_needs = []
        self.member_liquidity_state = {}

    def get_member(self, peer_id):
        return self.members.get(peer_id)

    def get_all_members(self):
        return list(self.members.values())

    def get_member_health(self, peer_id):
        return self.member_health.get(peer_id)

    def get_struggling_members(self, threshold=40):
        return []

    def store_liquidity_need(self, **kwargs):
        self.liquidity_needs.append(kwargs)

    def update_member_liquidity_state(self, **kwargs):
        pass


@pytest.fixture
def mock_plugin():
    return MockPlugin()


@pytest.fixture
def mock_db():
    return MockDatabase()


@pytest.fixture
def mock_state():
    return MockStateManager()


@pytest.fixture
def mock_budget_db():
    db = MagicMock()
    db.create_budget_hold = MagicMock()
    db.release_budget_hold = MagicMock()
    db.consume_budget_hold = MagicMock()
    db.expire_budget_hold = MagicMock()
    db.get_budget_hold = MagicMock(return_value=None)
    db.get_holds_for_round = MagicMock(return_value=[])
    db.get_active_holds_for_peer = MagicMock(return_value=[])
    return db


# =============================================================================
# COST REDUCTION BUG FIXES (Bugs 1-7)
# =============================================================================


class TestBug1RemoteCircularAlertsInit:
    """Bug 1: _remote_circular_alerts should be initialized in __init__."""

    def test_attr_exists_at_init(self, mock_plugin, mock_state):
        """Verify _remote_circular_alerts exists immediately after construction."""
        detector = CircularFlowDetector(plugin=mock_plugin, state_manager=mock_state)
        assert hasattr(detector, "_remote_circular_alerts")
        assert isinstance(detector._remote_circular_alerts, list)
        assert len(detector._remote_circular_alerts) == 0

    def test_receive_alert_without_hasattr_check(self, mock_plugin, mock_state):
        """Verify alerts can be received without lazy init."""
        detector = CircularFlowDetector(plugin=mock_plugin, state_manager=mock_state)
        result = detector.receive_circular_flow_alert(
            reporter_id=MEMBER_A,
            alert_data={
                "members_involved": [MEMBER_A, MEMBER_B],
                "total_amount_sats": 50000,
                "total_cost_sats": 100,
            }
        )
        assert result is True
        assert len(detector._remote_circular_alerts) == 1

    def test_get_all_alerts_without_hasattr(self, mock_plugin, mock_state):
        """get_all_circular_flow_alerts should work without hasattr guard."""
        detector = CircularFlowDetector(plugin=mock_plugin, state_manager=mock_state)
        alerts = detector.get_all_circular_flow_alerts(include_remote=True)
        assert isinstance(alerts, list)

    def test_cleanup_without_hasattr(self, mock_plugin, mock_state):
        """cleanup_old_remote_alerts should work without hasattr guard."""
        detector = CircularFlowDetector(plugin=mock_plugin, state_manager=mock_state)
        removed = detector.cleanup_old_remote_alerts()
        assert removed == 0


class TestBug2McfCompletionsInit:
    """Bug 2: _mcf_completions should be initialized in __init__."""

    def test_attr_exists_at_init(self, mock_plugin, mock_db, mock_state):
        mgr = CostReductionManager(
            plugin=mock_plugin, database=mock_db, state_manager=mock_state
        )
        assert hasattr(mgr, "_mcf_completions")
        assert isinstance(mgr._mcf_completions, dict)

    def test_get_completions_returns_empty_list(self, mock_plugin, mock_db, mock_state):
        mgr = CostReductionManager(
            plugin=mock_plugin, database=mock_db, state_manager=mock_state
        )
        assert mgr.get_mcf_completions() == []


class TestBug3GetMcfAcksLock:
    """Bug 3: get_mcf_acks should use _mcf_acks_lock."""

    def test_get_acks_uses_lock(self, mock_plugin, mock_db, mock_state):
        mgr = CostReductionManager(
            plugin=mock_plugin, database=mock_db, state_manager=mock_state
        )
        # record_mcf_ack requires _mcf_coordinator to be set
        mgr._mcf_coordinator = MagicMock()

        # Record an ack
        mgr.record_mcf_ack(
            member_id=MEMBER_A,
            solution_timestamp=1000,
            assignment_count=2
        )
        # get_mcf_acks should safely return under lock
        acks = mgr.get_mcf_acks()
        assert len(acks) == 1
        assert acks[0]["member_id"] == MEMBER_A

    def test_concurrent_ack_access(self, mock_plugin, mock_db, mock_state):
        """Verify thread-safe concurrent access to MCF acks."""
        mgr = CostReductionManager(
            plugin=mock_plugin, database=mock_db, state_manager=mock_state
        )
        mgr._mcf_coordinator = MagicMock()
        errors = []

        def writer():
            try:
                for i in range(50):
                    mgr.record_mcf_ack(f"member_{i}", i, 1)
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(50):
                    mgr.get_mcf_acks()
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert errors == []


class TestBug4McfCompletionsThreadSafety:
    """Bug 4: _mcf_completions should be protected by lock."""

    def test_record_and_get_completions(self, mock_plugin, mock_db, mock_state):
        mgr = CostReductionManager(
            plugin=mock_plugin, database=mock_db, state_manager=mock_state
        )
        mgr.record_mcf_completion(
            member_id=MEMBER_A,
            assignment_id="assign_1",
            success=True,
            actual_amount_sats=50000,
            actual_cost_sats=10,
        )
        completions = mgr.get_mcf_completions()
        assert len(completions) == 1
        assert completions[0]["success"] is True

    def test_concurrent_completion_access(self, mock_plugin, mock_db, mock_state):
        """Verify thread-safe concurrent access to MCF completions."""
        mgr = CostReductionManager(
            plugin=mock_plugin, database=mock_db, state_manager=mock_state
        )
        errors = []

        def writer():
            try:
                for i in range(50):
                    mgr.record_mcf_completion(
                        member_id=f"member_{i}",
                        assignment_id=f"assign_{i}",
                        success=True,
                        actual_amount_sats=1000,
                        actual_cost_sats=1,
                    )
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(50):
                    mgr.get_mcf_completions()
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert errors == []


class TestBug5BoundedFleetPaths:
    """Bug 5: _find_all_fleet_paths should be bounded."""

    def test_path_count_bounded(self, mock_plugin, mock_state):
        """Verify path count never exceeds _MAX_CANDIDATE_PATHS."""
        router = FleetRebalanceRouter(
            plugin=mock_plugin, state_manager=mock_state
        )

        # Create a densely connected mesh topology
        # 20 members all connected to each other + from_peer + to_peer
        from_peer = "from_" + "00" * 30
        to_peer = "to_" + "00" * 31
        members = [f"member_{i:02d}" + "x" * 56 for i in range(20)]

        topology = {}
        for m in members:
            # Each member connected to from_peer, to_peer, and all other members
            peers = {from_peer, to_peer} | (set(members) - {m})
            topology[m] = peers

        router._topology_snapshot = (topology, time.time())

        paths = router._find_all_fleet_paths(from_peer, to_peer, max_depth=4)
        assert len(paths) <= router._MAX_CANDIDATE_PATHS

    def test_max_candidate_paths_constant(self):
        """Verify the bound constant exists."""
        assert FleetRebalanceRouter._MAX_CANDIDATE_PATHS == 100


class TestBug6SingleRpcForOutcome:
    """Bug 6: record_rebalance_outcome should use a single RPC call."""

    def test_single_listpeerchannels_call(self, mock_plugin, mock_db, mock_state):
        """Verify only one listpeerchannels call is made."""
        mgr = CostReductionManager(
            plugin=mock_plugin, database=mock_db, state_manager=mock_state
        )
        mgr._our_pubkey = OUR_PUBKEY

        # Set up channels
        mock_plugin.rpc.channels = [
            {
                "short_channel_id": "100x1x0",
                "peer_id": MEMBER_A,
                "state": "CHANNELD_NORMAL",
            },
            {
                "short_channel_id": "200x1x0",
                "peer_id": MEMBER_B,
                "state": "CHANNELD_NORMAL",
            },
        ]

        call_count = [0]
        orig_listpeerchannels = mock_plugin.rpc.listpeerchannels

        def counting_listpeerchannels(**kwargs):
            call_count[0] += 1
            return orig_listpeerchannels(**kwargs)

        mock_plugin.rpc.listpeerchannels = counting_listpeerchannels

        mgr.record_rebalance_outcome(
            from_channel="100x1x0",
            to_channel="200x1x0",
            amount_sats=50000,
            cost_sats=10,
            success=True,
        )

        # Should be exactly 1 call, not 2
        assert call_count[0] == 1


class TestBug7HubScoresCached:
    """Bug 7: Hub scores should be fetched once, not per-path."""

    def test_score_path_accepts_precomputed_scores(self, mock_plugin, mock_state):
        """_score_path_with_hub_bonus should accept hub_scores parameter."""
        router = FleetRebalanceRouter(
            plugin=mock_plugin, state_manager=mock_state
        )
        precomputed = {MEMBER_A: 0.8, MEMBER_B: 0.6}
        score = router._score_path_with_hub_bonus(
            [MEMBER_A, MEMBER_B], 100000, hub_scores=precomputed
        )
        assert isinstance(score, float)
        assert score < float('inf')

    def test_score_path_without_precomputed_still_works(self, mock_plugin, mock_state):
        """_score_path_with_hub_bonus should still work without hub_scores."""
        router = FleetRebalanceRouter(
            plugin=mock_plugin, state_manager=mock_state
        )
        with patch("modules.cost_reduction.network_metrics") as mock_nm:
            mock_nm.get_calculator.return_value = None
            score = router._score_path_with_hub_bonus(
                [MEMBER_A], 100000
            )
            assert isinstance(score, float)


# =============================================================================
# LIQUIDITY COORDINATOR BUG FIXES (Bugs 8-13)
# =============================================================================


class TestBug8And9LiquidityNeedsMcfLock:
    """Bugs 8-9: get_all_liquidity_needs_for_mcf should snapshot under lock."""

    def _make_coordinator(self, mock_plugin, mock_db):
        return LiquidityCoordinator(
            database=mock_db, plugin=mock_plugin, our_pubkey=OUR_PUBKEY,
            state_manager=None
        )

    def test_mcf_needs_snapshots_under_lock(self, mock_plugin, mock_db):
        """Verify concurrent writes don't crash MCF needs reader."""
        coord = self._make_coordinator(mock_plugin, mock_db)
        errors = []

        def writer():
            try:
                for i in range(100):
                    key = f"{MEMBER_A}:peer_{i}"
                    need = LiquidityNeed(
                        reporter_id=MEMBER_A,
                        need_type="inbound",
                        target_peer_id=f"peer_{i}",
                        amount_sats=10000,
                        urgency="medium",
                        max_fee_ppm=500,
                        reason="test",
                        current_balance_pct=0.3,
                        can_provide_inbound=0,
                        can_provide_outbound=0,
                        timestamp=int(time.time()),
                        signature="sig",
                    )
                    with coord._lock:
                        coord._liquidity_needs[key] = need
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(100):
                    coord.get_all_liquidity_needs_for_mcf()
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert errors == []

    def test_remote_mcf_needs_snapshots_under_lock(self, mock_plugin, mock_db):
        """Verify remote MCF needs are also snapshotted under lock."""
        coord = self._make_coordinator(mock_plugin, mock_db)

        # Store a remote need
        coord.store_remote_mcf_need({
            "reporter_id": MEMBER_B,
            "need_type": "inbound",
            "target_peer": "some_peer",
            "amount_sats": 50000,
            "urgency": "high",
            "received_at": int(time.time()),
        })

        needs = coord.get_all_liquidity_needs_for_mcf()
        remote_needs = [n for n in needs if n["member_id"] == MEMBER_B]
        assert len(remote_needs) == 1


class TestBug10FleetLiquidityNeedsLock:
    """Bug 10: get_fleet_liquidity_needs should snapshot under lock."""

    def test_concurrent_state_access(self, mock_plugin, mock_db):
        mock_db.members = {
            MEMBER_A: {"peer_id": MEMBER_A},
            MEMBER_B: {"peer_id": MEMBER_B},
        }
        coord = LiquidityCoordinator(
            database=mock_db, plugin=mock_plugin, our_pubkey=OUR_PUBKEY,
        )
        errors = []

        def writer():
            try:
                for i in range(50):
                    coord.record_member_liquidity_report(
                        member_id=MEMBER_A,
                        depleted_channels=[{"peer_id": f"ext_{i}", "local_pct": 0.05}],
                        saturated_channels=[],
                    )
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(50):
                    coord.get_fleet_liquidity_needs()
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert errors == []


class TestBug11FleetLiquidityStateLock:
    """Bug 11: get_fleet_liquidity_state should snapshot under lock."""

    def test_fleet_state_snapshots(self, mock_plugin, mock_db):
        mock_db.members = {
            MEMBER_A: {"peer_id": MEMBER_A},
        }
        coord = LiquidityCoordinator(
            database=mock_db, plugin=mock_plugin, our_pubkey=OUR_PUBKEY,
        )

        # Write some state
        coord.record_member_liquidity_report(
            member_id=MEMBER_A,
            depleted_channels=[{"peer_id": "ext_1", "local_pct": 0.05}],
            saturated_channels=[],
            rebalancing_active=True,
            rebalancing_peers=["ext_1"],
        )

        state = coord.get_fleet_liquidity_state()
        assert state["fleet_summary"]["members_rebalancing"] == 1


class TestBug12BottleneckPeersLock:
    """Bug 12: _get_common_bottleneck_peers should snapshot under lock."""

    def test_bottleneck_peers_with_data(self, mock_plugin, mock_db):
        mock_db.members = {
            MEMBER_A: {"peer_id": MEMBER_A},
            MEMBER_B: {"peer_id": MEMBER_B},
        }
        coord = LiquidityCoordinator(
            database=mock_db, plugin=mock_plugin, our_pubkey=OUR_PUBKEY,
        )

        # Both members report issues with same external peer
        ext_peer = "03" + "ff" * 32
        coord.record_member_liquidity_report(
            member_id=MEMBER_A,
            depleted_channels=[{"peer_id": ext_peer, "local_pct": 0.05}],
            saturated_channels=[],
        )
        coord.record_member_liquidity_report(
            member_id=MEMBER_B,
            depleted_channels=[{"peer_id": ext_peer, "local_pct": 0.08}],
            saturated_channels=[],
        )

        bottlenecks = coord._get_common_bottleneck_peers()
        assert ext_peer in bottlenecks


class TestBug13ClearStaleRemoteNeedsLock:
    """Bug 13: clear_stale_remote_needs should use lock."""

    def test_concurrent_clear_and_store(self, mock_plugin, mock_db):
        coord = LiquidityCoordinator(
            database=mock_db, plugin=mock_plugin, our_pubkey=OUR_PUBKEY,
        )
        errors = []

        def writer():
            try:
                for i in range(50):
                    coord.store_remote_mcf_need({
                        "reporter_id": f"member_{i}" + "x" * 50,
                        "need_type": "inbound",
                        "target_peer": "some_peer",
                        "amount_sats": 1000,
                        "received_at": int(time.time()) - 3600,  # Stale
                    })
            except Exception as e:
                errors.append(e)

        def cleaner():
            try:
                for _ in range(50):
                    coord.clear_stale_remote_needs(max_age_seconds=1)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=cleaner)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert errors == []


# =============================================================================
# SPLICE COORDINATOR BUG FIX (Bug 14)
# =============================================================================


class TestBug14BoundedChannelCache:
    """Bug 14: _channel_cache should be bounded with eviction."""

    def test_cache_bounded(self, mock_plugin):
        coord = SpliceCoordinator(database=MagicMock(), plugin=mock_plugin)

        # Fill cache beyond max
        overfill = MAX_CHANNEL_CACHE_SIZE + 100
        for i in range(overfill):
            coord._channel_cache[f"key_{i}"] = (i, time.time())

        assert len(coord._channel_cache) == overfill

        # Add one more via _cache_put — should trigger eviction
        coord._cache_put("new_key", 999)

        # Eviction should have reduced the cache (10% of entries removed)
        assert len(coord._channel_cache) < overfill
        assert "new_key" in coord._channel_cache

    def test_stale_entries_evicted_first(self, mock_plugin):
        coord = SpliceCoordinator(database=MagicMock(), plugin=mock_plugin)

        # Fill cache with stale entries
        stale_time = time.time() - CHANNEL_CACHE_TTL - 10
        for i in range(MAX_CHANNEL_CACHE_SIZE):
            coord._channel_cache[f"stale_{i}"] = (i, stale_time)

        # Add new entry — stale entries should be evicted
        coord._cache_put("fresh_key", 42)

        assert "fresh_key" in coord._channel_cache
        # All stale entries should be gone
        assert len(coord._channel_cache) < MAX_CHANNEL_CACHE_SIZE

    def test_cache_put_stores_value(self, mock_plugin):
        coord = SpliceCoordinator(database=MagicMock(), plugin=mock_plugin)
        coord._cache_put("test_key", 123)

        data, ts = coord._channel_cache["test_key"]
        assert data == 123
        assert time.time() - ts < 2


# =============================================================================
# BUDGET MANAGER BUG FIXES (Bugs 15-17)
# =============================================================================


class TestBug15BudgetManagerThreadSafety:
    """Bug 15: BudgetHoldManager should have thread-safe _holds."""

    def test_has_lock(self, mock_budget_db):
        mgr = BudgetHoldManager(database=mock_budget_db, our_pubkey=OUR_PUBKEY)
        assert hasattr(mgr, "_lock")
        assert isinstance(mgr._lock, type(threading.Lock()))

    def test_concurrent_create_and_read(self, mock_budget_db):
        mgr = BudgetHoldManager(database=mock_budget_db, our_pubkey=OUR_PUBKEY)
        mgr._last_cleanup = 0
        errors = []

        def creator():
            try:
                for i in range(20):
                    # Force cleanup so rate limit doesn't block
                    mgr._last_cleanup = 0
                    mgr.create_hold(round_id=f"round_{i}", amount_sats=1000)
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                for _ in range(50):
                    mgr.get_active_holds()
                    mgr.get_total_held()
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=creator)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert errors == []


class TestBug16ConsumeHoldChecksExpiry:
    """Bug 16: consume_hold should check is_active() (includes expiry)."""

    def test_cannot_consume_expired_hold(self, mock_budget_db):
        mgr = BudgetHoldManager(database=mock_budget_db, our_pubkey=OUR_PUBKEY)
        mgr._last_cleanup = 0

        # Create hold with very short duration
        hold_id = mgr.create_hold(round_id="round_exp", amount_sats=5000, duration_seconds=1)
        assert hold_id is not None

        # Wait for it to expire
        time.sleep(1.1)

        # Try to consume — should fail because hold is expired
        result = mgr.consume_hold(hold_id, consumed_by="test_action")
        assert result is False

    def test_can_consume_active_hold(self, mock_budget_db):
        mgr = BudgetHoldManager(database=mock_budget_db, our_pubkey=OUR_PUBKEY)
        mgr._last_cleanup = 0

        hold_id = mgr.create_hold(round_id="round_ok", amount_sats=5000, duration_seconds=120)
        assert hold_id is not None

        result = mgr.consume_hold(hold_id, consumed_by="test_action")
        assert result is True


class TestBug17HoldsEviction:
    """Bug 17: Non-active holds should be evicted from _holds dict."""

    def test_expired_holds_evicted_on_cleanup(self, mock_budget_db):
        mgr = BudgetHoldManager(database=mock_budget_db, our_pubkey=OUR_PUBKEY)

        # Create hold that expires immediately
        now = int(time.time())
        hold = BudgetHold(
            hold_id="hold_old",
            round_id="round_old",
            peer_id=OUR_PUBKEY,
            amount_sats=1000,
            created_at=now - 200,
            expires_at=now - 100,  # Already expired
            status="active",
        )
        mgr._holds["hold_old"] = hold
        mgr._last_cleanup = 0  # Allow cleanup to run

        count = mgr.cleanup_expired_holds()

        # Should be expired and evicted
        assert count == 1
        assert "hold_old" not in mgr._holds

    def test_released_holds_evicted_on_cleanup(self, mock_budget_db):
        mgr = BudgetHoldManager(database=mock_budget_db, our_pubkey=OUR_PUBKEY)

        now = int(time.time())
        hold = BudgetHold(
            hold_id="hold_rel",
            round_id="round_rel",
            peer_id=OUR_PUBKEY,
            amount_sats=1000,
            created_at=now,
            expires_at=now + 120,
            status="released",  # Already released
        )
        mgr._holds["hold_rel"] = hold
        mgr._last_cleanup = 0

        mgr.cleanup_expired_holds()

        # Released hold should be evicted from memory
        assert "hold_rel" not in mgr._holds

    def test_consumed_holds_evicted_on_cleanup(self, mock_budget_db):
        mgr = BudgetHoldManager(database=mock_budget_db, our_pubkey=OUR_PUBKEY)

        now = int(time.time())
        hold = BudgetHold(
            hold_id="hold_con",
            round_id="round_con",
            peer_id=OUR_PUBKEY,
            amount_sats=1000,
            created_at=now,
            expires_at=now + 120,
            status="consumed",
        )
        mgr._holds["hold_con"] = hold
        mgr._last_cleanup = 0

        mgr.cleanup_expired_holds()

        assert "hold_con" not in mgr._holds

    def test_active_holds_not_evicted(self, mock_budget_db):
        mgr = BudgetHoldManager(database=mock_budget_db, our_pubkey=OUR_PUBKEY)

        now = int(time.time())
        hold = BudgetHold(
            hold_id="hold_active",
            round_id="round_active",
            peer_id=OUR_PUBKEY,
            amount_sats=1000,
            created_at=now,
            expires_at=now + 120,
            status="active",
        )
        mgr._holds["hold_active"] = hold
        mgr._last_cleanup = 0

        mgr.cleanup_expired_holds()

        # Active hold should remain
        assert "hold_active" in mgr._holds
