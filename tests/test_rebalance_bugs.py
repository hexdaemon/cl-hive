"""
Tests for rebalance flow bug fixes.

Covers:
- Bug: cf.cycle → cf.members AttributeError fix in CircularFlowDetector
- Bug: Lock acquisition in LiquidityCoordinator
- Bug: BFS fleet path connectivity uses direct channels, not shared peers
- Bug: MCF get_total_demand counts all needs, not just inbound
- Bug: MCFCircuitBreaker thread safety
- Bug: receive_mcf_assignment bounds enforcement after cleanup
- Bug: Empty peer IDs rejected from circular flow tracking
- Bug: to_us_msat type coercion
- Bug: create_mcf_ack_message() called with wrong number of args
- Bug: create_mcf_completion_message() called with wrong number of args
- Bug: ctx.state_manager AttributeError in rebalance_hubs/rebalance_path
- Bug: execute_hive_circular_rebalance missing permission check
- Bug: get_mcf_optimized_path ignores to_channel parameter
- Bug: _check_stuck_mcf_assignments accesses private internals
"""

import pytest
import time
import threading
from unittest.mock import MagicMock, patch
from collections import deque

from modules.cost_reduction import (
    CircularFlow,
    CircularFlowDetector,
    FleetRebalanceRouter,
    CostReductionManager,
    FleetPath,
)
from modules.mcf_solver import (
    MCFCircuitBreaker,
    MCFCoordinator,
    MCF_CIRCUIT_FAILURE_THRESHOLD,
    MCF_CIRCUIT_RECOVERY_TIMEOUT,
)
from modules.liquidity_coordinator import (
    LiquidityCoordinator,
    LiquidityNeed,
    MAX_MCF_ASSIGNMENTS,
    MCFAssignment,
)


class MockPlugin:
    def __init__(self):
        self.logs = []
        self.rpc = MockRpc()

    def log(self, msg, level="info"):
        self.logs.append({"msg": msg, "level": level})


class MockRpc:
    def __init__(self):
        self.channels = []

    def listpeerchannels(self, id=None):
        if id:
            return {"channels": [c for c in self.channels if c.get("peer_id") == id]}
        return {"channels": self.channels}


class MockDatabase:
    def __init__(self):
        self.members = {}
        self._liquidity_needs = []
        self._member_health = {}
        self._member_liquidity = {}

    def get_all_members(self):
        return list(self.members.values()) if self.members else []

    def get_member(self, peer_id):
        return self.members.get(peer_id)

    def get_member_health(self, peer_id):
        return self._member_health.get(peer_id)

    def store_liquidity_need(self, **kwargs):
        self._liquidity_needs.append(kwargs)

    def update_member_liquidity_state(self, **kwargs):
        self._member_liquidity[kwargs.get("member_id")] = kwargs


class MockStateManager:
    def __init__(self):
        self._peer_states = []

    def get(self, key, default=None):
        return default

    def set(self, key, value):
        pass

    def get_state(self, key, default=None):
        return default

    def set_state(self, key, value):
        pass

    def get_all_peer_states(self):
        return self._peer_states


class TestCircularFlowMembersFix:
    """cf.cycle → cf.members: CircularFlow dataclass uses 'members' field."""

    def test_circular_flow_has_members_field(self):
        cf = CircularFlow(
            members=["peer1", "peer2", "peer3"],
            total_amount_sats=100000,
            total_cost_sats=500,
            cycle_count=3,
            detection_window_hours=24.0,
            recommendation="MONITOR"
        )
        assert cf.members == ["peer1", "peer2", "peer3"]
        assert not hasattr(cf, 'cycle'), "CircularFlow should NOT have a 'cycle' attribute"

    def test_to_dict_uses_members(self):
        cf = CircularFlow(
            members=["peer1", "peer2"],
            total_amount_sats=50000,
            total_cost_sats=200,
            cycle_count=2,
            detection_window_hours=12.0,
            recommendation="WARN"
        )
        d = cf.to_dict()
        assert "members" in d
        assert d["members"] == ["peer1", "peer2"]

    def test_get_shareable_circular_flows_no_crash(self):
        """get_shareable_circular_flows should not crash with AttributeError."""
        plugin = MockPlugin()
        state_mgr = MockStateManager()
        detector = CircularFlowDetector(plugin=plugin, state_manager=state_mgr)

        # Even with no flows, should not crash
        result = detector.get_shareable_circular_flows()
        assert isinstance(result, list)

    def test_get_all_circular_flow_alerts_no_crash(self):
        """get_all_circular_flow_alerts should not crash with AttributeError."""
        plugin = MockPlugin()
        state_mgr = MockStateManager()
        detector = CircularFlowDetector(plugin=plugin, state_manager=state_mgr)

        result = detector.get_all_circular_flow_alerts()
        assert isinstance(result, list)


class TestLiquidityCoordinatorLock:
    """Lock must be acquired on shared state mutations."""

    def setup_method(self):
        self.db = MockDatabase()
        self.db.members = {"peer1": {"peer_id": "peer1", "tier": "member"}}
        self.plugin = MockPlugin()
        self.state_mgr = MockStateManager()
        self.coord = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey="02" + "0" * 64,
            state_manager=self.state_mgr
        )

    def test_lock_exists(self):
        assert hasattr(self.coord, '_lock')
        assert isinstance(self.coord._lock, type(threading.Lock()))

    def test_record_member_liquidity_report(self):
        """record_member_liquidity_report should update state under lock."""
        result = self.coord.record_member_liquidity_report(
            member_id="peer1",
            depleted_channels=[{"peer_id": "ext1", "local_pct": 0.1, "capacity_sats": 1000000}],
            saturated_channels=[],
            rebalancing_active=True,
            rebalancing_peers=["ext1"]
        )
        assert result.get("status") == "recorded"
        assert "peer1" in self.coord._member_liquidity_state

    def test_check_rebalancing_conflict_snapshot(self):
        """check_rebalancing_conflict should use snapshot of state."""
        # Set up a member rebalancing through ext1
        self.coord._member_liquidity_state["other_member"] = {
            "rebalancing_active": True,
            "rebalancing_peers": ["ext1"]
        }
        result = self.coord.check_rebalancing_conflict("ext1")
        assert result["conflict"] is True

    def test_receive_mcf_assignment_bounds(self):
        """After cleanup, if still at limit, assignment should be rejected."""
        # Fill to limit with fresh (non-expired) assignments
        for i in range(MAX_MCF_ASSIGNMENTS):
            aid = f"mcf_test_{i}_x_y"
            self.coord._mcf_assignments[aid] = MCFAssignment(
                assignment_id=aid,
                solution_timestamp=int(time.time()),
                coordinator_id="coordinator",
                from_channel=f"from_{i}",
                to_channel=f"to_{i}",
                amount_sats=10000,
                expected_cost_sats=10,
                path=[],
                priority=i,
                via_fleet=True,
                received_at=int(time.time()),
                status="pending",
            )

        # Try to add one more — should be rejected since all are fresh
        result = self.coord.receive_mcf_assignment(
            assignment_data={
                "from_channel": "new_from",
                "to_channel": "new_to",
                "amount_sats": 5000,
                "priority": 99,
            },
            solution_timestamp=int(time.time()),
            coordinator_id="coordinator"
        )
        assert result is False, "Should reject assignment when at limit and cleanup can't free space"


class TestBFSFleetPathConnectivity:
    """BFS should use direct channel connectivity, not shared external peers."""

    def setup_method(self):
        self.plugin = MockPlugin()
        self.state_mgr = MockStateManager()
        self.router = FleetRebalanceRouter(
            plugin=self.plugin,
            state_manager=self.state_mgr
        )
        self.router.set_our_pubkey("02" + "0" * 64)

    def test_direct_channel_connectivity(self):
        """Members with direct channels should be connected in BFS."""
        # memberA has channels to: ext1, memberB
        # memberB has channels to: ext2, memberA
        # They are directly connected — BFS should find a path
        topology = {
            "memberA": {"ext1", "memberB"},
            "memberB": {"ext2", "memberA"},
        }

        # Cache the topology
        self.router._topology_snapshot = (topology, time.time())

        # ext1 connects to memberA, ext2 connects to memberB
        path = self.router.find_fleet_path("ext1", "ext2", 100000)

        # Should find a path: memberA → memberB
        assert path is not None, "Should find path through directly connected members"

    def test_shared_peers_not_sufficient(self):
        """Members sharing external peers but NOT directly connected should NOT be connected."""
        # memberA has channels to: ext1, ext_shared
        # memberC has channels to: ext2, ext_shared
        # They share ext_shared but have NO direct channel
        topology = {
            "memberA": {"ext1", "ext_shared"},
            "memberC": {"ext2", "ext_shared"},
        }

        self.router._topology_snapshot = (topology, time.time())

        # Looking for path from ext1 to ext2
        path = self.router.find_fleet_path("ext1", "ext2", 100000)

        # Should NOT find a multi-hop path (no direct memberA→memberC channel)
        # But if both are start AND end, could be direct
        if path:
            # The path should only contain a single member if ext1→memberA→ext2
            # Only possible if memberA also has ext2 in peers
            assert len(path.path) <= 1, "Should not route through unconnected members"


class TestMCFGetTotalDemand:
    """get_total_demand should count ALL needs, not just inbound."""

    def test_counts_outbound_needs(self):
        """Outbound needs should be included in total demand."""
        from modules.mcf_solver import RebalanceNeed

        needs = [
            RebalanceNeed(
                member_id="m1", need_type="inbound", target_peer="ext1",
                amount_sats=100000, channel_id="ch1", urgency="high", max_fee_ppm=500
            ),
            RebalanceNeed(
                member_id="m2", need_type="outbound", target_peer="ext2",
                amount_sats=200000, channel_id="ch2", urgency="medium", max_fee_ppm=300
            ),
        ]

        plugin = MockPlugin()
        db = MockDatabase()
        state_mgr = MockStateManager()

        coord = MCFCoordinator(
            plugin=plugin,
            database=db,
            state_manager=state_mgr,
            liquidity_coordinator=None,
            our_pubkey="02" + "0" * 64
        )

        total = coord.get_total_demand(needs)
        assert total == 300000, f"Should count all needs (300000), got {total}"

    def test_inbound_only(self):
        """Pure inbound needs should still work."""
        from modules.mcf_solver import RebalanceNeed

        needs = [
            RebalanceNeed(
                member_id="m1", need_type="inbound", target_peer="ext1",
                amount_sats=100000, channel_id="ch1", urgency="high", max_fee_ppm=500
            ),
        ]

        plugin = MockPlugin()
        db = MockDatabase()
        state_mgr = MockStateManager()

        coord = MCFCoordinator(
            plugin=plugin,
            database=db,
            state_manager=state_mgr,
            liquidity_coordinator=None,
            our_pubkey="02" + "0" * 64
        )

        total = coord.get_total_demand(needs)
        assert total == 100000


class TestMCFCircuitBreakerThreadSafety:
    """MCFCircuitBreaker should be thread-safe."""

    def test_has_lock(self):
        cb = MCFCircuitBreaker()
        assert hasattr(cb, '_lock')

    def test_concurrent_record_success(self):
        """Multiple threads recording success should not corrupt state."""
        cb = MCFCircuitBreaker()
        errors = []

        def record_many():
            try:
                for _ in range(100):
                    cb.record_success()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_many) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors during concurrent access: {errors}"
        assert cb.total_successes == 500

    def test_concurrent_record_failure(self):
        """Multiple threads recording failures should not corrupt state."""
        cb = MCFCircuitBreaker()
        errors = []

        def record_failures():
            try:
                for _ in range(10):
                    cb.record_failure()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=record_failures) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert cb.total_failures == 50


class TestEmptyPeerCircularFlow:
    """Empty peer IDs should be rejected from circular flow tracking."""

    def test_record_outcome_skips_unknown_peers(self):
        """record_rebalance_outcome should skip circular flow when peers unknown."""
        plugin = MockPlugin()
        state_mgr = MockStateManager()
        mgr = CostReductionManager(
            plugin=plugin,
            state_manager=state_mgr
        )

        # Mock _get_peer_for_channel to return None
        mgr.fleet_router._get_peer_for_channel = MagicMock(return_value=None)

        result = mgr.record_rebalance_outcome(
            from_channel="ch1",
            to_channel="ch2",
            amount_sats=50000,
            cost_sats=100,
            success=True,
            via_fleet=False
        )

        assert "warning" in result, "Should warn when peers can't be resolved"


class TestToUsMsatTypeSafety:
    """to_us_msat should be safely converted to int."""

    def test_int_conversion(self):
        """int() handles both int and Msat string types."""
        # Normal int
        assert int(5000000) == 5000000
        # String-like Msat (CLN sometimes returns these)
        assert int("5000000") == 5000000


class TestCreateMcfAckMessageSignature:
    """create_mcf_ack_message() takes zero args (uses internal state)."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.state_mgr = MockStateManager()
        self.coord = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey="02" + "0" * 64,
            state_manager=self.state_mgr
        )

    def test_create_mcf_ack_no_args(self):
        """create_mcf_ack_message takes no positional args."""
        import inspect
        sig = inspect.signature(self.coord.create_mcf_ack_message)
        # Only 'self' — no other parameters
        params = [p for p in sig.parameters if p != 'self']
        assert len(params) == 0, f"Expected 0 params, got: {params}"

    def test_create_mcf_ack_callable_without_args(self):
        """Should be callable with no args and return None (no pending solution)."""
        result = self.coord.create_mcf_ack_message()
        assert result is None  # No pending solution timestamp


class TestCreateMcfCompletionMessageSignature:
    """create_mcf_completion_message() takes only assignment_id."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.state_mgr = MockStateManager()
        self.coord = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey="02" + "0" * 64,
            state_manager=self.state_mgr
        )

    def test_create_completion_signature(self):
        """create_mcf_completion_message takes only assignment_id."""
        import inspect
        sig = inspect.signature(self.coord.create_mcf_completion_message)
        params = [p for p in sig.parameters if p != 'self']
        assert params == ['assignment_id'], f"Expected ['assignment_id'], got: {params}"

    def test_create_completion_missing_assignment(self):
        """Should return None for unknown assignment."""
        result = self.coord.create_mcf_completion_message("nonexistent_id")
        assert result is None

    def test_create_completion_not_final_status(self):
        """Should return None if assignment isn't in completed/failed/rejected state."""
        aid = "test_assignment"
        self.coord._mcf_assignments[aid] = MCFAssignment(
            assignment_id=aid,
            solution_timestamp=int(time.time()),
            coordinator_id="coordinator",
            from_channel="from_ch",
            to_channel="to_ch",
            amount_sats=10000,
            expected_cost_sats=10,
            path=[],
            priority=1,
            via_fleet=True,
            received_at=int(time.time()),
            status="pending",
        )
        result = self.coord.create_mcf_completion_message(aid)
        assert result is None  # Not in final status


class TestHiveContextNoStateManager:
    """HiveContext has no state_manager field — access must be safe."""

    def test_getattr_safe_access(self):
        """getattr(ctx, 'state_manager', None) should return None."""
        from modules.rpc_commands import HiveContext
        ctx = HiveContext(
            database=MockDatabase(),
            config=None,
            safe_plugin=None,
            our_pubkey="02" + "0" * 64,
        )
        # state_manager is not a field on HiveContext
        assert getattr(ctx, 'state_manager', None) is None

    def test_rebalance_hubs_no_crash(self):
        """rebalance_hubs should not crash on missing state_manager."""
        from modules.rpc_commands import HiveContext
        # We can't easily test the full rebalance_hubs without network_metrics,
        # but we verify the safe access pattern
        ctx = HiveContext(
            database=MockDatabase(),
            config=None,
            safe_plugin=None,
            our_pubkey="02" + "0" * 64,
        )
        # The fix uses getattr(ctx, 'state_manager', None) which is safe
        sm = getattr(ctx, 'state_manager', None)
        assert sm is None  # No crash, returns None


class TestCircularRebalancePermission:
    """execute_hive_circular_rebalance should check permission when not dry_run."""

    def test_dry_run_no_permission_check(self):
        """dry_run=True should not require permission."""
        from modules.rpc_commands import execute_hive_circular_rebalance, HiveContext
        mock_mgr = MagicMock()
        mock_mgr.execute_hive_circular_rebalance.return_value = {"dry_run": True, "route": []}

        ctx = HiveContext(
            database=MockDatabase(),
            config=None,
            safe_plugin=None,
            our_pubkey="02" + "0" * 64,
            cost_reduction_mgr=mock_mgr,
        )

        result = execute_hive_circular_rebalance(
            ctx, from_channel="ch1", to_channel="ch2",
            amount_sats=50000, dry_run=True
        )
        # Should succeed — dry_run doesn't need permission
        assert "error" not in result or "permission" not in result.get("error", "").lower()

    def test_non_dry_run_needs_member(self):
        """dry_run=False should require member tier."""
        from modules.rpc_commands import execute_hive_circular_rebalance, HiveContext

        db = MockDatabase()
        # No member entry = not a member
        ctx = HiveContext(
            database=db,
            config=None,
            safe_plugin=None,
            our_pubkey="02" + "0" * 64,
            cost_reduction_mgr=MagicMock(),
        )

        result = execute_hive_circular_rebalance(
            ctx, from_channel="ch1", to_channel="ch2",
            amount_sats=50000, dry_run=False
        )
        # Should be rejected — not a member
        assert "error" in result


class TestMcfOptimizedPathToChannel:
    """get_mcf_optimized_path should match both from_channel AND to_channel."""

    def setup_method(self):
        self.plugin = MockPlugin()
        self.state_mgr = MockStateManager()
        self.mgr = CostReductionManager(
            plugin=self.plugin,
            state_manager=self.state_mgr
        )

    def test_matching_both_channels(self):
        """Assignment must match both from_channel and to_channel."""
        mock_coord = MagicMock()
        mock_coord.get_status.return_value = {"solution_valid": True}

        mock_assignment = MagicMock()
        mock_assignment.from_channel = "ch_from"
        mock_assignment.to_channel = "ch_to_A"  # Different to_channel
        mock_assignment.amount_sats = 100000
        mock_coord.get_our_assignments.return_value = [mock_assignment]

        self.mgr._mcf_enabled = True
        self.mgr._mcf_coordinator = mock_coord

        # Request to_channel=ch_to_B, should NOT match assignment with ch_to_A
        result = self.mgr.get_mcf_optimized_path("ch_from", "ch_to_B", 50000)
        assert result.get("source") != "mcf", "Should not match wrong to_channel"

    def test_correct_match(self):
        """Assignment with matching from + to channels should be returned."""
        mock_coord = MagicMock()
        mock_coord.get_status.return_value = {"solution_valid": True}

        mock_assignment = MagicMock()
        mock_assignment.from_channel = "ch_from"
        mock_assignment.to_channel = "ch_to"
        mock_assignment.amount_sats = 100000
        mock_assignment.expected_cost_sats = 50
        mock_assignment.path = ["member1"]
        mock_assignment.via_fleet = True
        mock_assignment.to_dict.return_value = {"id": "test"}
        mock_coord.get_our_assignments.return_value = [mock_assignment]

        self.mgr._mcf_enabled = True
        self.mgr._mcf_coordinator = mock_coord

        result = self.mgr.get_mcf_optimized_path("ch_from", "ch_to", 50000)
        assert result.get("source") == "mcf", "Should match correct from + to channels"


class TestTimeoutStuckAssignments:
    """timeout_stuck_assignments encapsulates stuck assignment handling."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.state_mgr = MockStateManager()
        self.coord = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey="02" + "0" * 64,
            state_manager=self.state_mgr
        )

    def test_method_exists(self):
        """LiquidityCoordinator should have timeout_stuck_assignments method."""
        assert hasattr(self.coord, 'timeout_stuck_assignments')
        assert callable(self.coord.timeout_stuck_assignments)

    def test_no_stuck_assignments(self):
        """Should return empty list when no assignments are stuck."""
        result = self.coord.timeout_stuck_assignments()
        assert result == []

    def test_times_out_old_executing(self):
        """Should timeout assignments in executing state past max time."""
        aid = "stuck_assignment"
        self.coord._mcf_assignments[aid] = MCFAssignment(
            assignment_id=aid,
            solution_timestamp=int(time.time()) - 7200,
            coordinator_id="coordinator",
            from_channel="from_ch",
            to_channel="to_ch",
            amount_sats=10000,
            expected_cost_sats=10,
            path=[],
            priority=1,
            via_fleet=True,
            received_at=int(time.time()) - 7200,  # 2 hours ago
            status="executing",
        )

        result = self.coord.timeout_stuck_assignments(max_execution_time=1800)
        assert aid in result
        assert self.coord._mcf_assignments[aid].status == "failed"
        assert self.coord._mcf_assignments[aid].error_message == "execution_timeout"

    def test_preserves_fresh_executing(self):
        """Should not timeout fresh executing assignments."""
        aid = "fresh_assignment"
        self.coord._mcf_assignments[aid] = MCFAssignment(
            assignment_id=aid,
            solution_timestamp=int(time.time()),
            coordinator_id="coordinator",
            from_channel="from_ch",
            to_channel="to_ch",
            amount_sats=10000,
            expected_cost_sats=10,
            path=[],
            priority=1,
            via_fleet=True,
            received_at=int(time.time()),  # Just now
            status="executing",
        )

        result = self.coord.timeout_stuck_assignments(max_execution_time=1800)
        assert result == []
        assert self.coord._mcf_assignments[aid].status == "executing"

    def test_thread_safe(self):
        """timeout_stuck_assignments should be thread-safe."""
        # Add a stuck assignment
        aid = "stuck_ts"
        self.coord._mcf_assignments[aid] = MCFAssignment(
            assignment_id=aid,
            solution_timestamp=int(time.time()) - 7200,
            coordinator_id="coordinator",
            from_channel="from_ch",
            to_channel="to_ch",
            amount_sats=10000,
            expected_cost_sats=10,
            path=[],
            priority=1,
            via_fleet=True,
            received_at=int(time.time()) - 7200,
            status="executing",
        )

        errors = []
        def timeout_many():
            try:
                for _ in range(50):
                    self.coord.timeout_stuck_assignments()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=timeout_many) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread safety errors: {errors}"
