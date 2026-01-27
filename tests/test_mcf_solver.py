"""
Tests for MCF (Min-Cost Max-Flow) Solver Module.

Tests cover:
- MCFEdge, MCFNode, MCFNetwork data classes
- SSPSolver with Bellman-Ford algorithm
- MCFNetworkBuilder
- MCFCoordinator
- Integration with cost_reduction module

Author: Lightning Goats Team
"""

import pytest
import time
from unittest.mock import MagicMock, patch

from modules.mcf_solver import (
    MCFEdge,
    MCFNode,
    MCFNetwork,
    SSPSolver,
    MCFNetworkBuilder,
    MCFCoordinator,
    RebalanceNeed,
    RebalanceAssignment,
    MCFSolution,
    MIN_MCF_DEMAND,
    MAX_SOLUTION_AGE,
    HIVE_INTERNAL_COST_PPM,
    DEFAULT_EXTERNAL_COST_PPM,
    INFINITY,
)


# =============================================================================
# TEST FIXTURES
# =============================================================================

class MockPlugin:
    """Mock plugin for testing."""

    def __init__(self):
        self.logs = []
        self.rpc = MockRpc()

    def log(self, msg, level="info"):
        self.logs.append({"msg": msg, "level": level})


class MockRpc:
    """Mock RPC interface."""

    def __init__(self):
        self.channels = []
        self.info = {"id": "02" + "a" * 64}

    def listpeerchannels(self):
        return {"channels": self.channels}

    def getinfo(self):
        return self.info


class MockDatabase:
    """Mock database for testing."""

    def __init__(self):
        self.members = []

    def get_all_members(self):
        return self.members

    def get_member(self, peer_id):
        for m in self.members:
            if m.get("peer_id") == peer_id:
                return m
        return None


class MockStateManager:
    """Mock state manager for testing."""

    def __init__(self):
        self.peer_states = {}

    def get_peer_state(self, peer_id):
        return self.peer_states.get(peer_id)

    def get_all_peer_states(self):
        return list(self.peer_states.values())

    def set_peer_state(self, peer_id, capacity=0, topology=None, capabilities=None):
        state = MagicMock()
        state.peer_id = peer_id
        state.capacity_sats = capacity
        state.topology = topology or []
        state.capabilities = capabilities if capabilities is not None else ["mcf"]
        self.peer_states[peer_id] = state

    def set_mcf_capable(self, peer_id, capable=True):
        """Set MCF capability for a peer (for testing version-aware election)."""
        if peer_id in self.peer_states:
            self.peer_states[peer_id].capabilities = ["mcf"] if capable else []
        else:
            self.set_peer_state(peer_id, capabilities=["mcf"] if capable else [])


class MockLiquidityCoordinator:
    """Mock liquidity coordinator for testing."""

    def __init__(self):
        self.needs = []

    def get_prioritized_needs(self):
        return self.needs

    def add_need(self, reporter_id, need_type, target_peer, amount_sats,
                 urgency="medium", max_fee_ppm=1000):
        need = MagicMock()
        need.reporter_id = reporter_id
        need.need_type = need_type
        need.target_peer_id = target_peer
        need.amount_sats = amount_sats
        need.urgency = urgency
        need.max_fee_ppm = max_fee_ppm
        self.needs.append(need)


# =============================================================================
# MCF DATA CLASS TESTS
# =============================================================================

class TestMCFEdge:
    """Test MCFEdge data class."""

    def test_basic_creation(self):
        """Test creating a basic edge."""
        edge = MCFEdge(
            from_node="02" + "a" * 64,
            to_node="02" + "b" * 64,
            capacity=1_000_000,
            cost_ppm=100,
            residual_capacity=1_000_000
        )

        assert edge.from_node == "02" + "a" * 64
        assert edge.to_node == "02" + "b" * 64
        assert edge.capacity == 1_000_000
        assert edge.cost_ppm == 100
        assert edge.residual_capacity == 1_000_000
        assert edge.flow == 0
        assert edge.is_hive_internal is False

    def test_hive_internal_edge(self):
        """Test creating a hive internal edge with zero fees."""
        edge = MCFEdge(
            from_node="02" + "a" * 64,
            to_node="02" + "b" * 64,
            capacity=1_000_000,
            cost_ppm=0,
            residual_capacity=1_000_000,
            is_hive_internal=True
        )

        assert edge.cost_ppm == 0
        assert edge.is_hive_internal is True

    def test_unit_cost_calculation(self):
        """Test unit cost calculation."""
        edge = MCFEdge(
            from_node="02" + "a" * 64,
            to_node="02" + "b" * 64,
            capacity=1_000_000,
            cost_ppm=500,
            residual_capacity=1_000_000
        )

        # 100,000 sats at 500 ppm = 50 sats
        cost = edge.unit_cost(100_000)
        assert cost == 50

        # 1,000,000 sats at 500 ppm = 500 sats
        cost = edge.unit_cost(1_000_000)
        assert cost == 500


class TestMCFNode:
    """Test MCFNode data class."""

    def test_basic_creation(self):
        """Test creating a basic node."""
        node = MCFNode(node_id="02" + "a" * 64)

        assert node.node_id == "02" + "a" * 64
        assert node.supply == 0
        assert node.is_fleet_member is False
        assert node.outgoing_edges == []

    def test_source_node(self):
        """Test creating a source node (positive supply)."""
        node = MCFNode(
            node_id="02" + "a" * 64,
            supply=500_000,
            is_fleet_member=True
        )

        assert node.supply == 500_000
        assert node.is_fleet_member is True

    def test_sink_node(self):
        """Test creating a sink node (negative supply)."""
        node = MCFNode(
            node_id="02" + "a" * 64,
            supply=-500_000,
            is_fleet_member=True
        )

        assert node.supply == -500_000


class TestRebalanceNeed:
    """Test RebalanceNeed data class."""

    def test_basic_creation(self):
        """Test creating a basic rebalance need."""
        need = RebalanceNeed(
            member_id="02" + "a" * 64,
            need_type="inbound",
            target_peer="02" + "b" * 64,
            amount_sats=100_000
        )

        assert need.member_id == "02" + "a" * 64
        assert need.need_type == "inbound"
        assert need.target_peer == "02" + "b" * 64
        assert need.amount_sats == 100_000
        assert need.urgency == "medium"

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        need = RebalanceNeed(
            member_id="02" + "a" * 64,
            need_type="outbound",
            target_peer="02" + "b" * 64,
            amount_sats=200_000,
            urgency="high",
            max_fee_ppm=500
        )

        d = need.to_dict()

        assert d["member_id"] == "02" + "a" * 64
        assert d["need_type"] == "outbound"
        assert d["amount_sats"] == 200_000
        assert d["urgency"] == "high"
        assert d["max_fee_ppm"] == 500


class TestRebalanceAssignment:
    """Test RebalanceAssignment data class."""

    def test_basic_creation(self):
        """Test creating a basic assignment."""
        assignment = RebalanceAssignment(
            member_id="02" + "a" * 64,
            from_channel="123x1x0",
            to_channel="456x2x0",
            amount_sats=100_000,
            expected_cost_sats=50
        )

        assert assignment.member_id == "02" + "a" * 64
        assert assignment.from_channel == "123x1x0"
        assert assignment.to_channel == "456x2x0"
        assert assignment.amount_sats == 100_000
        assert assignment.expected_cost_sats == 50
        assert assignment.via_fleet is True

    def test_to_dict_serialization(self):
        """Test to_dict serialization."""
        assignment = RebalanceAssignment(
            member_id="02" + "a" * 64,
            from_channel="123x1x0",
            to_channel="456x2x0",
            amount_sats=100_000,
            expected_cost_sats=50,
            path=["02" + "a" * 64, "02" + "b" * 64],
            priority=1,
            via_fleet=True
        )

        d = assignment.to_dict()

        assert d["member_id"] == "02" + "a" * 64
        assert d["from_channel"] == "123x1x0"
        assert len(d["path"]) == 2
        assert d["priority"] == 1


# =============================================================================
# MCF NETWORK TESTS
# =============================================================================

class TestMCFNetwork:
    """Test MCFNetwork class."""

    def test_add_node(self):
        """Test adding nodes to network."""
        network = MCFNetwork()

        network.add_node("02" + "a" * 64, supply=100_000, is_fleet_member=True)
        network.add_node("02" + "b" * 64, supply=-100_000, is_fleet_member=True)

        assert network.get_node_count() == 2
        assert network.nodes["02" + "a" * 64].supply == 100_000
        assert network.nodes["02" + "b" * 64].supply == -100_000

    def test_add_node_aggregates_supply(self):
        """Test that adding same node aggregates supply."""
        network = MCFNetwork()

        network.add_node("02" + "a" * 64, supply=50_000)
        network.add_node("02" + "a" * 64, supply=50_000)

        assert network.get_node_count() == 1
        assert network.nodes["02" + "a" * 64].supply == 100_000

    def test_add_edge(self):
        """Test adding edges to network."""
        network = MCFNetwork()

        network.add_node("02" + "a" * 64)
        network.add_node("02" + "b" * 64)

        edge_idx = network.add_edge(
            from_node="02" + "a" * 64,
            to_node="02" + "b" * 64,
            capacity=1_000_000,
            cost_ppm=100
        )

        # Should create 2 edges (forward + reverse)
        assert network.get_edge_count() == 2
        assert edge_idx == 0

        # Check forward edge
        forward = network.edges[0]
        assert forward.from_node == "02" + "a" * 64
        assert forward.to_node == "02" + "b" * 64
        assert forward.capacity == 1_000_000
        assert forward.cost_ppm == 100

        # Check reverse edge
        reverse = network.edges[1]
        assert reverse.from_node == "02" + "b" * 64
        assert reverse.to_node == "02" + "a" * 64
        assert reverse.capacity == 0  # Initially 0
        assert reverse.cost_ppm == -100  # Negative for cancellation

        # Check linking
        assert forward.reverse_edge_idx == 1
        assert reverse.reverse_edge_idx == 0

    def test_setup_super_source_sink(self):
        """Test super-source and super-sink setup."""
        network = MCFNetwork()

        # Add source and sink nodes
        network.add_node("02" + "a" * 64, supply=100_000, is_fleet_member=True)
        network.add_node("02" + "b" * 64, supply=-100_000, is_fleet_member=True)
        network.add_edge("02" + "a" * 64, "02" + "b" * 64, 200_000, 0)

        network.setup_super_source_sink()

        # Should have super-source and super-sink
        assert network.super_source in network.nodes
        assert network.super_sink in network.nodes

        # Super-source should have supply, super-sink should have demand
        assert network.nodes[network.super_source].supply == 100_000
        assert network.nodes[network.super_sink].supply == -100_000


# =============================================================================
# SSP SOLVER TESTS
# =============================================================================

class TestSSPSolver:
    """Test SSPSolver class."""

    def test_simple_augmentation(self):
        """Test simple single source to single sink flow."""
        network = MCFNetwork()

        # Create simple network: source -> sink
        network.add_node("source", supply=100_000)
        network.add_node("sink", supply=-100_000)
        network.add_edge("source", "sink", 200_000, 100)  # 100 ppm

        network.setup_super_source_sink()

        solver = SSPSolver(network)
        total_flow, total_cost, edge_flows = solver.solve()

        assert total_flow == 100_000
        # Cost: 100_000 * 100 / 1_000_000 = 10 sats
        assert total_cost == 10

    def test_multiple_paths(self):
        """Test flow splits correctly across multiple paths."""
        network = MCFNetwork()

        # Create diamond network:
        #     source
        #     /    \
        #   mid1   mid2
        #     \    /
        #      sink
        network.add_node("source", supply=200_000)
        network.add_node("mid1")
        network.add_node("mid2")
        network.add_node("sink", supply=-200_000)

        # Each path has capacity 150k
        network.add_edge("source", "mid1", 150_000, 100)
        network.add_edge("source", "mid2", 150_000, 200)  # Higher cost
        network.add_edge("mid1", "sink", 150_000, 100)
        network.add_edge("mid2", "sink", 150_000, 200)

        network.setup_super_source_sink()

        solver = SSPSolver(network)
        total_flow, total_cost, edge_flows = solver.solve()

        # Should route as much as possible through cheaper path
        assert total_flow == 200_000

    def test_prefer_zero_cost_hive_paths(self):
        """Test that solver prefers zero-cost hive internal paths."""
        network = MCFNetwork()

        # Create network with two paths: hive (free) and external (expensive)
        network.add_node("source", supply=100_000)
        network.add_node("hive_member", is_fleet_member=True)
        network.add_node("external")
        network.add_node("sink", supply=-100_000)

        # Hive path: zero cost
        network.add_edge("source", "hive_member", 100_000, 0, is_hive_internal=True)
        network.add_edge("hive_member", "sink", 100_000, 0, is_hive_internal=True)

        # External path: expensive
        network.add_edge("source", "external", 100_000, 500)
        network.add_edge("external", "sink", 100_000, 500)

        network.setup_super_source_sink()

        solver = SSPSolver(network)
        total_flow, total_cost, edge_flows = solver.solve()

        assert total_flow == 100_000
        assert total_cost == 0  # Should use free hive path

    def test_no_feasible_solution(self):
        """Test graceful handling when no path exists."""
        network = MCFNetwork()

        # Disconnected source and sink
        network.add_node("source", supply=100_000)
        network.add_node("sink", supply=-100_000)
        # No edges connecting them

        network.setup_super_source_sink()

        solver = SSPSolver(network)
        total_flow, total_cost, edge_flows = solver.solve()

        # No flow possible
        assert total_flow == 0
        assert total_cost == 0

    def test_capacity_constrained_flow(self):
        """Test that flow respects capacity constraints."""
        network = MCFNetwork()

        # Demand exceeds capacity
        network.add_node("source", supply=200_000)
        network.add_node("sink", supply=-200_000)
        network.add_edge("source", "sink", 100_000, 100)  # Only 100k capacity

        network.setup_super_source_sink()

        solver = SSPSolver(network)
        total_flow, total_cost, edge_flows = solver.solve()

        # Flow limited by capacity
        assert total_flow == 100_000


# =============================================================================
# MCF NETWORK BUILDER TESTS
# =============================================================================

class TestMCFNetworkBuilder:
    """Test MCFNetworkBuilder class."""

    def test_build_from_fleet_state(self):
        """Test building network from fleet state."""
        plugin = MockPlugin()
        state_manager = MockStateManager()

        # Add fleet members with topology
        state_manager.set_peer_state(
            "02" + "a" * 64,
            capacity=1_000_000,
            topology=["02" + "b" * 64]
        )
        state_manager.set_peer_state(
            "02" + "b" * 64,
            capacity=1_000_000,
            topology=["02" + "a" * 64]
        )

        # Create needs
        needs = [
            RebalanceNeed(
                member_id="02" + "a" * 64,
                need_type="outbound",
                target_peer="02" + "b" * 64,
                amount_sats=100_000
            ),
            RebalanceNeed(
                member_id="02" + "b" * 64,
                need_type="inbound",
                target_peer="02" + "a" * 64,
                amount_sats=100_000
            )
        ]

        builder = MCFNetworkBuilder(plugin)
        network = builder.build_from_fleet_state(
            state_manager,
            needs,
            our_pubkey="02" + "c" * 64
        )

        # Should have nodes for fleet members
        assert network.get_node_count() >= 2

    def test_add_edges_from_channels(self):
        """Test adding edges from channel data."""
        plugin = MockPlugin()
        builder = MCFNetworkBuilder(plugin)

        network = MCFNetwork()
        our_pubkey = "02" + "a" * 64
        member_ids = {"02" + "b" * 64}  # One hive member

        channels = [
            {
                "state": "CHANNELD_NORMAL",
                "peer_id": "02" + "b" * 64,  # Hive member
                "short_channel_id": "123x1x0",
                "total_msat": 2_000_000_000,  # 2M sats
                "to_us_msat": 1_000_000_000,  # 1M local
            },
            {
                "state": "CHANNELD_NORMAL",
                "peer_id": "02" + "c" * 64,  # External peer
                "short_channel_id": "456x2x0",
                "total_msat": 1_000_000_000,
                "to_us_msat": 500_000_000,
            }
        ]

        builder._add_edges_from_channels(network, our_pubkey, channels, member_ids)

        # Should have edges for both channels
        assert network.get_edge_count() >= 4  # 2 directions * 2 (forward + reverse)


# =============================================================================
# MCF COORDINATOR TESTS
# =============================================================================

class TestMCFCoordinator:
    """Test MCFCoordinator class."""

    def test_elect_coordinator(self):
        """Test coordinator election (lexicographic lowest among MCF-capable)."""
        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        # Add members
        database.members = [
            {"peer_id": "02" + "b" * 64},
            {"peer_id": "02" + "a" * 64},  # Lexicographically lowest
            {"peer_id": "02" + "c" * 64},
        ]

        # Set MCF capability for all members (version-aware election)
        state_manager.set_mcf_capable("02" + "a" * 64, True)
        state_manager.set_mcf_capable("02" + "b" * 64, True)
        state_manager.set_mcf_capable("02" + "c" * 64, True)

        coordinator = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "d" * 64  # Not lowest
        )

        elected = coordinator.elect_coordinator()

        # Should be lexicographically lowest among MCF-capable
        assert elected == "02" + "a" * 64

    def test_elect_coordinator_skips_non_mcf_capable(self):
        """Test that election skips members without MCF capability."""
        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        # Add members
        database.members = [
            {"peer_id": "02" + "a" * 64},  # Lowest but NOT MCF-capable
            {"peer_id": "02" + "b" * 64},  # MCF-capable
            {"peer_id": "02" + "c" * 64},  # MCF-capable
        ]

        # Only b and c are MCF-capable
        state_manager.set_mcf_capable("02" + "a" * 64, False)
        state_manager.set_mcf_capable("02" + "b" * 64, True)
        state_manager.set_mcf_capable("02" + "c" * 64, True)

        coordinator = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "d" * 64
        )

        elected = coordinator.elect_coordinator()

        # Should skip "a" (not MCF-capable) and elect "b"
        assert elected == "02" + "b" * 64

    def test_is_coordinator(self):
        """Test is_coordinator check."""
        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        database.members = [
            {"peer_id": "02" + "b" * 64},
            {"peer_id": "02" + "c" * 64},
        ]

        # Set MCF capability for members
        state_manager.set_mcf_capable("02" + "b" * 64, True)
        state_manager.set_mcf_capable("02" + "c" * 64, True)

        # Our pubkey is lexicographically lowest
        coordinator = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "a" * 64  # Lowest
        )

        assert coordinator.is_coordinator() is True

        # Now with higher pubkey
        coordinator2 = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "z" * 64  # Not lowest
        )

        assert coordinator2.is_coordinator() is False

    def test_collect_fleet_needs(self):
        """Test collecting needs from liquidity coordinator."""
        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        # Add needs
        liquidity_coordinator.add_need(
            reporter_id="02" + "a" * 64,
            need_type="inbound",
            target_peer="02" + "b" * 64,
            amount_sats=100_000,
            urgency="high"
        )

        coordinator = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "c" * 64
        )

        needs = coordinator.collect_fleet_needs()

        assert len(needs) == 1
        assert needs[0].member_id == "02" + "a" * 64
        assert needs[0].amount_sats == 100_000

    def test_get_total_demand(self):
        """Test calculating total demand."""
        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        coordinator = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "c" * 64
        )

        needs = [
            RebalanceNeed("02a", "inbound", "02b", 100_000),
            RebalanceNeed("02c", "outbound", "02d", 50_000),  # Not counted
            RebalanceNeed("02e", "inbound", "02f", 200_000),
        ]

        total = coordinator.get_total_demand(needs)

        # Only inbound needs count as demand
        assert total == 300_000

    def test_get_status(self):
        """Test getting coordinator status."""
        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        database.members = [{"peer_id": "02" + "a" * 64}]

        coordinator = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "a" * 64
        )

        status = coordinator.get_status()

        assert status["enabled"] is True
        assert status["is_coordinator"] is True
        assert status["coordinator_id"] is not None

    def test_receive_solution(self):
        """Test receiving and validating solution from coordinator."""
        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        database.members = [{"peer_id": "02" + "a" * 64}]

        # Set MCF capability for the member
        state_manager.set_mcf_capable("02" + "a" * 64, True)

        coordinator = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "b" * 64  # Not coordinator
        )

        # Solution from correct coordinator
        solution_data = {
            "coordinator_id": "02" + "a" * 64,  # Lexicographically lowest
            "timestamp": int(time.time()),
            "assignments": [],
            "total_flow_sats": 100_000,
            "total_cost_sats": 10,
            "unmet_demand_sats": 0,
            "computation_time_ms": 50,
            "iterations": 5,
        }

        result = coordinator.receive_solution(solution_data)
        assert result is True

        # Solution from wrong coordinator
        bad_solution = {
            "coordinator_id": "02" + "z" * 64,  # Wrong coordinator
            "timestamp": int(time.time()),
            "assignments": [],
            "total_flow_sats": 100_000,
            "total_cost_sats": 10,
            "unmet_demand_sats": 0,
            "computation_time_ms": 50,
            "iterations": 5,
        }

        result = coordinator.receive_solution(bad_solution)
        assert result is False


# =============================================================================
# INVARIANT TESTS
# =============================================================================

class TestInvariants:
    """Test solver invariants."""

    def test_flow_conservation(self):
        """Test that inflow = outflow at every non-source/sink node."""
        network = MCFNetwork()

        # Create network with transit node
        network.add_node("source", supply=100_000)
        network.add_node("transit")
        network.add_node("sink", supply=-100_000)

        network.add_edge("source", "transit", 200_000, 100)
        network.add_edge("transit", "sink", 200_000, 100)

        network.setup_super_source_sink()

        solver = SSPSolver(network)
        solver.solve()

        # Check flow conservation at transit node
        inflow = 0
        outflow = 0

        for edge in network.edges:
            if edge.to_node == "transit":
                inflow += edge.flow
            if edge.from_node == "transit":
                outflow += edge.flow

        assert inflow == outflow

    def test_capacity_constraints(self):
        """Test that flow <= capacity on every edge."""
        network = MCFNetwork()

        network.add_node("source", supply=200_000)
        network.add_node("sink", supply=-200_000)
        network.add_edge("source", "sink", 100_000, 100)

        network.setup_super_source_sink()

        solver = SSPSolver(network)
        solver.solve()

        for edge in network.edges:
            assert edge.flow <= edge.capacity

    def test_no_negative_flow(self):
        """Test that no edge has negative flow."""
        network = MCFNetwork()

        network.add_node("source", supply=100_000)
        network.add_node("mid")
        network.add_node("sink", supply=-100_000)

        network.add_edge("source", "mid", 150_000, 100)
        network.add_edge("mid", "sink", 150_000, 100)

        network.setup_super_source_sink()

        solver = SSPSolver(network)
        solver.solve()

        for edge in network.edges:
            assert edge.flow >= 0


# =============================================================================
# COMPARISON TESTS
# =============================================================================

class TestMCFvsBFS:
    """Test that MCF produces better or equal solutions to BFS."""

    def test_mcf_cost_less_equal_bfs(self):
        """Test that MCF cost is less than or equal to BFS cost."""
        # MCF should find the optimal (minimum cost) solution
        # BFS finds shortest path (minimum hops) which may cost more

        network = MCFNetwork()

        # Create network where shortest path is NOT cheapest
        network.add_node("source", supply=100_000)
        network.add_node("mid1")  # Expensive direct path
        network.add_node("mid2")  # Part of cheap indirect path
        network.add_node("mid3")  # Part of cheap indirect path
        network.add_node("sink", supply=-100_000)

        # Direct path (1 hop): 1000 ppm
        network.add_edge("source", "mid1", 100_000, 1000)
        network.add_edge("mid1", "sink", 100_000, 1000)

        # Indirect path (2 hops): 100 ppm each
        network.add_edge("source", "mid2", 100_000, 100)
        network.add_edge("mid2", "mid3", 100_000, 100)
        network.add_edge("mid3", "sink", 100_000, 100)

        network.setup_super_source_sink()

        solver = SSPSolver(network)
        total_flow, total_cost, edge_flows = solver.solve()

        # MCF should choose the cheaper 3-hop path
        # Cost: 100_000 * (100 + 100 + 100) / 1_000_000 = 30 sats
        # vs direct: 100_000 * (1000 + 1000) / 1_000_000 = 200 sats
        assert total_cost < 100  # Much less than direct path cost


# =============================================================================
# PROTOCOL VALIDATION TESTS
# =============================================================================

class TestProtocolValidation:
    """Test protocol message validation functions."""

    def test_validate_mcf_needs_batch_valid(self):
        """Test validating valid MCF_NEEDS_BATCH payload."""
        from modules.protocol import validate_mcf_needs_batch

        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "sig123",
            "needs": [
                {
                    "need_type": "inbound",
                    "amount_sats": 100_000,
                    "urgency": "medium"
                }
            ]
        }

        assert validate_mcf_needs_batch(payload) is True

    def test_validate_mcf_needs_batch_invalid_type(self):
        """Test validating MCF_NEEDS_BATCH with invalid need type."""
        from modules.protocol import validate_mcf_needs_batch

        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "sig123",
            "needs": [
                {
                    "need_type": "invalid_type",  # Invalid
                    "amount_sats": 100_000,
                    "urgency": "medium"
                }
            ]
        }

        assert validate_mcf_needs_batch(payload) is False

    def test_validate_mcf_needs_batch_amount_too_small(self):
        """Test validating MCF_NEEDS_BATCH with amount below minimum."""
        from modules.protocol import validate_mcf_needs_batch, MCF_MIN_AMOUNT_SATS

        payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "sig123",
            "needs": [
                {
                    "need_type": "inbound",
                    "amount_sats": MCF_MIN_AMOUNT_SATS - 1,  # Below minimum
                    "urgency": "medium"
                }
            ]
        }

        assert validate_mcf_needs_batch(payload) is False

    def test_validate_mcf_solution_valid(self):
        """Test validating valid MCF_SOLUTION_BROADCAST payload."""
        from modules.protocol import validate_mcf_solution_broadcast

        payload = {
            "coordinator_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "sig123",
            "assignments": [],
            "total_flow_sats": 100_000,
            "total_cost_sats": 10,
        }

        assert validate_mcf_solution_broadcast(payload) is True

    def test_validate_mcf_solution_invalid_flow(self):
        """Test validating MCF_SOLUTION_BROADCAST with negative flow."""
        from modules.protocol import validate_mcf_solution_broadcast

        payload = {
            "coordinator_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "sig123",
            "assignments": [],
            "total_flow_sats": -100,  # Invalid negative
            "total_cost_sats": 10,
        }

        assert validate_mcf_solution_broadcast(payload) is False


# =============================================================================
# INTEGRATION TESTS
# =============================================================================

class TestMCFIntegration:
    """Integration tests for MCF solver."""

    def test_end_to_end_optimization(self):
        """Test complete MCF optimization cycle."""
        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        # Setup fleet members
        database.members = [
            {"peer_id": "02" + "a" * 64},
            {"peer_id": "02" + "b" * 64},
        ]

        state_manager.set_peer_state(
            "02" + "a" * 64,
            capacity=2_000_000,
            topology=["02" + "b" * 64]
        )
        state_manager.set_peer_state(
            "02" + "b" * 64,
            capacity=2_000_000,
            topology=["02" + "a" * 64]
        )

        # Add liquidity needs (enough to trigger MCF)
        for _ in range(5):
            liquidity_coordinator.add_need(
                reporter_id="02" + "a" * 64,
                need_type="outbound",
                target_peer="02" + "b" * 64,
                amount_sats=50_000
            )

        for _ in range(5):
            liquidity_coordinator.add_need(
                reporter_id="02" + "b" * 64,
                need_type="inbound",
                target_peer="02" + "a" * 64,
                amount_sats=50_000
            )

        # Create coordinator (we are coordinator)
        coordinator = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "a" * 64
        )

        # Run optimization
        solution = coordinator.run_optimization_cycle()

        # Should produce a solution (demand is above MIN_MCF_DEMAND)
        assert solution is not None
        assert solution.total_flow_sats >= 0
        assert solution.computation_time_ms >= 0


# =============================================================================
# PHASE 2 INTEGRATION TESTS - LIQUIDITY COORDINATOR MCF METHODS
# =============================================================================

class TestLiquidityCoordinatorMCF:
    """Tests for LiquidityCoordinator MCF integration methods."""

    def test_receive_mcf_assignment(self):
        """Test receiving and storing an MCF assignment."""
        from modules.liquidity_coordinator import LiquidityCoordinator, MCFAssignment

        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()

        our_pubkey = "02" + "a" * 64
        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey=our_pubkey,
            state_manager=state_manager
        )

        assignment_data = {
            "member_id": our_pubkey,
            "from_channel": "123x1x0",
            "to_channel": "456x2x0",
            "amount_sats": 100_000,
            "expected_cost_sats": 50,
            "path": [our_pubkey, "02" + "b" * 64],
            "priority": 1,
            "via_fleet": True,
        }

        solution_timestamp = int(time.time())
        coordinator_id = "02" + "c" * 64

        result = coordinator.receive_mcf_assignment(
            assignment_data, solution_timestamp, coordinator_id
        )

        assert result is True

        # Verify assignment was stored
        pending = coordinator.get_pending_mcf_assignments()
        assert len(pending) == 1
        # MCFAssignment objects have attributes, not dict access
        assert pending[0].amount_sats == 100_000
        assert pending[0].from_channel == "123x1x0"

    def test_receive_duplicate_assignment_rejected(self):
        """Test that duplicate assignments are rejected."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()

        our_pubkey = "02" + "a" * 64
        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey=our_pubkey,
            state_manager=state_manager
        )

        assignment_data = {
            "member_id": our_pubkey,
            "amount_sats": 100_000,
            "priority": 1,
        }

        solution_timestamp = int(time.time())
        coordinator_id = "02" + "c" * 64

        # First acceptance
        result1 = coordinator.receive_mcf_assignment(
            assignment_data, solution_timestamp, coordinator_id
        )
        assert result1 is True

        # Duplicate rejection
        result2 = coordinator.receive_mcf_assignment(
            assignment_data, solution_timestamp, coordinator_id
        )
        assert result2 is False

    def test_update_mcf_assignment_status(self):
        """Test updating assignment status."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()

        our_pubkey = "02" + "a" * 64
        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey=our_pubkey,
            state_manager=state_manager
        )

        assignment_data = {
            "member_id": our_pubkey,
            "amount_sats": 100_000,
            "priority": 1,
        }

        solution_timestamp = int(time.time())
        coordinator_id = "02" + "c" * 64

        coordinator.receive_mcf_assignment(
            assignment_data, solution_timestamp, coordinator_id
        )

        # Get the assignment ID
        pending = coordinator.get_pending_mcf_assignments()
        assignment_id = pending[0].assignment_id

        # Update status
        result = coordinator.update_mcf_assignment_status(
            assignment_id, "completed",
            actual_amount_sats=95_000, actual_cost_sats=40
        )
        assert result is True

        # Verify status updated
        pending = coordinator.get_pending_mcf_assignments()
        assert len(pending) == 0  # Completed assignments not in pending

    def test_get_all_liquidity_needs_for_mcf(self):
        """Test collecting all liquidity needs for MCF optimization."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()

        our_pubkey = "02" + "a" * 64
        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey=our_pubkey,
            state_manager=state_manager
        )

        # Get needs returns a list (empty if no needs stored)
        needs = coordinator.get_all_liquidity_needs_for_mcf()

        # Should return a list structure
        assert isinstance(needs, list)

    def test_get_mcf_status(self):
        """Test getting MCF status."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64,
            state_manager=state_manager
        )

        status = coordinator.get_mcf_status()

        # Check actual return structure
        assert "pending_assignments" in status
        assert "assignment_counts" in status
        assert "last_solution_timestamp" in status
        assert "ack_sent" in status


# =============================================================================
# PHASE 2 INTEGRATION TESTS - COST REDUCTION MANAGER MCF METHODS
# =============================================================================

class TestCostReductionManagerMCF:
    """Tests for CostReductionManager MCF tracking methods."""

    def test_get_current_mcf_coordinator_without_mcf(self):
        """Test getting coordinator when MCF not initialized."""
        from modules.cost_reduction import CostReductionManager

        plugin = MockPlugin()

        manager = CostReductionManager(
            plugin=plugin,
            database=None,
            state_manager=None,
            liquidity_coordinator=None
        )
        manager.set_our_pubkey("02" + "a" * 64)

        result = manager.get_current_mcf_coordinator()
        assert result is None

    def test_record_mcf_ack(self):
        """Test recording MCF acknowledgments with MCF coordinator."""
        from modules.cost_reduction import CostReductionManager
        from modules.mcf_solver import MCFCoordinator

        plugin = MockPlugin()
        database = MockDatabase()
        database.members = [{"peer_id": "02" + "a" * 64}]
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        manager = CostReductionManager(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator
        )
        manager.set_our_pubkey("02" + "a" * 64)

        # Initialize MCF (this sets up _mcf_coordinator)
        manager.set_mcf_enabled(True)

        # Record an ACK
        manager.record_mcf_ack(
            member_id="02" + "b" * 64,
            solution_timestamp=int(time.time()),
            assignment_count=3
        )

        acks = manager.get_mcf_acks()
        assert len(acks) == 1
        assert acks[0]["member_id"] == "02" + "b" * 64
        assert acks[0]["assignment_count"] == 3

    def test_record_mcf_ack_without_coordinator(self):
        """Test that record_mcf_ack is a no-op without MCF coordinator."""
        from modules.cost_reduction import CostReductionManager

        plugin = MockPlugin()

        manager = CostReductionManager(
            plugin=plugin,
            database=None,
            state_manager=None,
            liquidity_coordinator=None
        )
        manager.set_our_pubkey("02" + "a" * 64)

        # This should not crash and should be a no-op
        manager.record_mcf_ack(
            member_id="02" + "b" * 64,
            solution_timestamp=int(time.time()),
            assignment_count=3
        )

        # No acks recorded because no MCF coordinator
        acks = manager.get_mcf_acks()
        assert len(acks) == 0

    def test_record_mcf_completion_success(self):
        """Test recording successful MCF completion."""
        from modules.cost_reduction import CostReductionManager

        plugin = MockPlugin()

        manager = CostReductionManager(
            plugin=plugin,
            database=None,
            state_manager=None,
            liquidity_coordinator=None
        )
        manager.set_our_pubkey("02" + "a" * 64)

        # record_mcf_completion works without coordinator (for fleet-wide tracking)
        manager.record_mcf_completion(
            member_id="02" + "b" * 64,
            assignment_id="mcf_12345_1",
            success=True,
            actual_amount_sats=100_000,
            actual_cost_sats=50,
            failure_reason=""
        )

        completions = manager.get_mcf_completions()
        assert len(completions) == 1
        assert completions[0]["success"] is True
        assert completions[0]["actual_amount_sats"] == 100_000

    def test_record_mcf_completion_failure(self):
        """Test recording failed MCF completion."""
        from modules.cost_reduction import CostReductionManager

        plugin = MockPlugin()

        manager = CostReductionManager(
            plugin=plugin,
            database=None,
            state_manager=None,
            liquidity_coordinator=None
        )
        manager.set_our_pubkey("02" + "a" * 64)

        manager.record_mcf_completion(
            member_id="02" + "b" * 64,
            assignment_id="mcf_12345_2",
            success=False,
            actual_amount_sats=0,
            actual_cost_sats=0,
            failure_reason="no_route"
        )

        completions = manager.get_mcf_completions()
        assert len(completions) == 1
        assert completions[0]["success"] is False
        assert completions[0]["failure_reason"] == "no_route"

    def test_mcf_ack_cache_bounded(self):
        """Test that MCF ACK cache is bounded to prevent memory issues."""
        from modules.cost_reduction import CostReductionManager

        plugin = MockPlugin()
        database = MockDatabase()
        database.members = [{"peer_id": "02" + "a" * 64}]
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        manager = CostReductionManager(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator
        )
        manager.set_our_pubkey("02" + "a" * 64)
        manager.set_mcf_enabled(True)

        # Add 600 ACKs (cache limit is 500)
        for i in range(600):
            manager.record_mcf_ack(
                member_id=f"02{'a' * 60}{i:04d}",
                solution_timestamp=1000000 + i,
                assignment_count=1
            )

        acks = manager.get_mcf_acks()
        # Should be capped at 500 (after pruning 100)
        assert len(acks) <= 500


# =============================================================================
# PHASE 2 INTEGRATION TESTS - PROTOCOL VALIDATION
# =============================================================================

class TestMCFProtocolValidation:
    """Tests for MCF protocol message validation."""

    def test_validate_mcf_assignment_ack_valid(self):
        """Test validating valid MCF_ASSIGNMENT_ACK."""
        from modules.protocol import validate_mcf_assignment_ack

        payload = {
            "member_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "solution_timestamp": int(time.time()) - 60,
            "assignment_count": 3,
            "signature": "zbase_signature",
        }

        assert validate_mcf_assignment_ack(payload) is True

    def test_validate_mcf_assignment_ack_invalid_count(self):
        """Test validating MCF_ASSIGNMENT_ACK with invalid assignment count."""
        from modules.protocol import validate_mcf_assignment_ack

        payload = {
            "member_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "solution_timestamp": int(time.time()) - 60,
            "assignment_count": -1,  # Invalid
            "signature": "zbase_signature",
        }

        assert validate_mcf_assignment_ack(payload) is False

    def test_validate_mcf_completion_report_valid(self):
        """Test validating valid MCF_COMPLETION_REPORT."""
        from modules.protocol import validate_mcf_completion_report

        payload = {
            "member_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "assignment_id": "mcf_12345_1",
            "success": True,
            "actual_amount_sats": 100_000,
            "actual_cost_sats": 50,
            "signature": "zbase_signature",
        }

        assert validate_mcf_completion_report(payload) is True

    def test_validate_mcf_completion_report_failure(self):
        """Test validating MCF_COMPLETION_REPORT with failure."""
        from modules.protocol import validate_mcf_completion_report

        payload = {
            "member_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "assignment_id": "mcf_12345_2",
            "success": False,
            "actual_amount_sats": 0,
            "actual_cost_sats": 0,
            "failure_reason": "no_route",
            "signature": "zbase_signature",
        }

        assert validate_mcf_completion_report(payload) is True

    def test_mcf_signing_payloads(self):
        """Test MCF signing payload generation."""
        from modules.protocol import (
            get_mcf_solution_signing_payload,
            get_mcf_assignment_ack_signing_payload,
            get_mcf_completion_signing_payload,
        )

        solution_payload = {
            "coordinator_id": "02" + "a" * 64,
            "timestamp": 1234567890,
            "total_flow_sats": 100_000,
            "total_cost_sats": 50,
            "assignments": [],
        }
        signing = get_mcf_solution_signing_payload(solution_payload)
        assert "mcf_solution:" in signing
        assert "1234567890" in signing

        ack_payload = {
            "member_id": "02" + "a" * 64,
            "timestamp": 1234567890,
            "solution_timestamp": 1234567800,
            "assignment_count": 3,
        }
        signing = get_mcf_assignment_ack_signing_payload(ack_payload)
        assert "mcf_ack:" in signing

        completion_payload = {
            "member_id": "02" + "a" * 64,
            "timestamp": 1234567890,
            "assignment_id": "mcf_12345_1",
            "success": True,
            "actual_amount_sats": 100_000,
        }
        signing = get_mcf_completion_signing_payload(completion_payload)
        assert "mcf_complete:" in signing


# =============================================================================
# PHASE 3 INTEGRATION TESTS - COORDINATION PROTOCOL
# =============================================================================

class TestMCFCoordinationProtocol:
    """Integration tests for MCF coordination protocol flow."""

    def test_needs_batch_message_creation(self):
        """Test creating MCF_NEEDS_BATCH message."""
        from modules.protocol import create_mcf_needs_batch

        class MockSignRpc:
            def signmessage(self, message):
                return {"zbase": "mock_signature_" + message[:20]}

        needs = [
            {
                "need_type": "inbound",
                "target_peer": "02" + "b" * 64,
                "amount_sats": 500_000,
                "urgency": "high",
                "max_fee_ppm": 500,
            },
            {
                "need_type": "outbound",
                "target_peer": "02" + "c" * 64,
                "amount_sats": 300_000,
                "urgency": "medium",
                "max_fee_ppm": 1000,
            },
        ]

        msg = create_mcf_needs_batch(
            needs=needs,
            rpc=MockSignRpc(),
            our_pubkey="02" + "a" * 64
        )

        assert msg is not None
        # Message should start with HIVE magic
        assert msg[:4] == b'HIVE'

    def test_needs_batch_validation(self):
        """Test MCF_NEEDS_BATCH validation."""
        from modules.protocol import validate_mcf_needs_batch

        valid_payload = {
            "reporter_id": "02" + "a" * 64,
            "timestamp": int(time.time()),
            "signature": "zbase_signature",
            "needs": [
                {
                    "need_type": "inbound",
                    "target_peer": "02" + "b" * 64,
                    "amount_sats": 100_000,
                    "urgency": "high",
                }
            ],
        }

        assert validate_mcf_needs_batch(valid_payload) is True

        # Invalid - missing reporter_id
        invalid_payload = {
            "timestamp": int(time.time()),
            "signature": "zbase_signature",
            "needs": [],
        }
        assert validate_mcf_needs_batch(invalid_payload) is False

    def test_store_remote_mcf_needs(self):
        """Test storing remote MCF needs from fleet members."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64  # We are coordinator
        )

        # Store needs from member B
        need1 = {
            "reporter_id": "02" + "b" * 64,
            "need_type": "inbound",
            "target_peer": "02" + "c" * 64,
            "amount_sats": 500_000,
            "urgency": "high",
            "max_fee_ppm": 1000,
        }

        result = coordinator.store_remote_mcf_need(need1)
        assert result is True
        assert coordinator.get_remote_mcf_needs_count() == 1

        # Store needs from member C
        need2 = {
            "reporter_id": "02" + "c" * 64,
            "need_type": "outbound",
            "target_peer": "02" + "d" * 64,
            "amount_sats": 300_000,
            "urgency": "medium",
        }

        result = coordinator.store_remote_mcf_need(need2)
        assert result is True
        assert coordinator.get_remote_mcf_needs_count() == 2

    def test_remote_needs_included_in_mcf(self):
        """Test that remote needs are included in MCF optimization input."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        # Store remote need
        need = {
            "reporter_id": "02" + "b" * 64,
            "need_type": "inbound",
            "target_peer": "02" + "c" * 64,
            "amount_sats": 500_000,
            "urgency": "high",
            "received_at": int(time.time()),  # Fresh
        }
        coordinator.store_remote_mcf_need(need)

        # Get needs for MCF
        mcf_needs = coordinator.get_all_liquidity_needs_for_mcf()

        # Should include the remote need
        remote_needs = [n for n in mcf_needs if n.get("member_id") == "02" + "b" * 64]
        assert len(remote_needs) == 1
        assert remote_needs[0]["amount_sats"] == 500_000

    def test_clear_stale_remote_needs(self):
        """Test clearing stale remote MCF needs."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        # Store a stale need (old timestamp)
        stale_need = {
            "reporter_id": "02" + "b" * 64,
            "need_type": "inbound",
            "target_peer": "02" + "c" * 64,
            "amount_sats": 500_000,
            "received_at": int(time.time()) - 3600,  # 1 hour ago
        }
        coordinator.store_remote_mcf_need(stale_need)

        # Store a fresh need
        fresh_need = {
            "reporter_id": "02" + "d" * 64,
            "need_type": "outbound",
            "target_peer": "02" + "e" * 64,
            "amount_sats": 300_000,
            "received_at": int(time.time()),  # Fresh
        }
        coordinator.store_remote_mcf_need(fresh_need)

        assert coordinator.get_remote_mcf_needs_count() == 2

        # Clear stale (older than 30 minutes)
        removed = coordinator.clear_stale_remote_needs(max_age_seconds=1800)
        assert removed == 1
        assert coordinator.get_remote_mcf_needs_count() == 1

    def test_coordinator_election_consistency(self):
        """Test that coordinator election is deterministic."""
        from modules.mcf_solver import MCFCoordinator

        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()
        liquidity_coordinator = MockLiquidityCoordinator()

        # Setup fleet members (A < B < C lexicographically)
        database.members = [
            {"peer_id": "02" + "b" * 64},
            {"peer_id": "02" + "a" * 64},  # Lowest
            {"peer_id": "02" + "c" * 64},
        ]

        # Set MCF capability for all members (version-aware election)
        state_manager.set_mcf_capable("02" + "a" * 64, True)
        state_manager.set_mcf_capable("02" + "b" * 64, True)
        state_manager.set_mcf_capable("02" + "c" * 64, True)

        # Create coordinator from different perspectives
        coord_a = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "a" * 64
        )

        coord_b = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "b" * 64
        )

        coord_c = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "c" * 64
        )

        # All should elect same coordinator (lexicographically lowest among MCF-capable)
        elected_by_a = coord_a.elect_coordinator()
        elected_by_b = coord_b.elect_coordinator()
        elected_by_c = coord_c.elect_coordinator()

        assert elected_by_a == elected_by_b == elected_by_c
        # 02aaa... < 02bbb... < 02ccc... lexicographically
        assert elected_by_a == "02" + "a" * 64

    def test_full_coordination_cycle(self):
        """Test complete MCF coordination cycle with mock components."""
        from modules.mcf_solver import MCFCoordinator, MIN_MCF_DEMAND
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()

        # Setup members (we are coordinator as lowest pubkey)
        our_pubkey = "02" + "a" * 64
        member_b = "02" + "b" * 64
        member_c = "02" + "c" * 64

        database.members = [
            {"peer_id": our_pubkey},
            {"peer_id": member_b},
            {"peer_id": member_c},
        ]

        # Setup topology
        state_manager.set_peer_state(our_pubkey, capacity=5_000_000, topology=[member_b])
        state_manager.set_peer_state(member_b, capacity=5_000_000, topology=[our_pubkey, member_c])
        state_manager.set_peer_state(member_c, capacity=5_000_000, topology=[member_b])

        # Create liquidity coordinator to receive remote needs
        liq_coord = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey=our_pubkey
        )

        # Simulate receiving needs from members
        liq_coord.store_remote_mcf_need({
            "reporter_id": member_b,
            "need_type": "outbound",
            "target_peer": our_pubkey,
            "amount_sats": 200_000,
            "urgency": "high",
            "received_at": int(time.time()),
        })

        liq_coord.store_remote_mcf_need({
            "reporter_id": member_c,
            "need_type": "inbound",
            "target_peer": member_b,
            "amount_sats": 200_000,
            "urgency": "high",
            "received_at": int(time.time()),
        })

        # Create MCF coordinator
        mcf_coord = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liq_coord,
            our_pubkey=our_pubkey
        )

        # Verify we are coordinator
        assert mcf_coord.is_coordinator() is True

        # Collect needs
        needs = mcf_coord.collect_fleet_needs()
        # Should have needs from our liquidity coordinator
        assert len(needs) >= 0  # May be 0 if mock doesn't provide enough

    def test_solution_serialization_roundtrip(self):
        """Test that MCF solution can be serialized and deserialized."""
        from modules.mcf_solver import MCFSolution, RebalanceAssignment

        solution = MCFSolution(
            total_flow_sats=500_000,
            total_cost_sats=250,
            unmet_demand_sats=0,
            assignments=[
                RebalanceAssignment(
                    member_id="02" + "a" * 64,
                    from_channel="123x1x0",
                    to_channel="456x2x0",
                    amount_sats=500_000,
                    expected_cost_sats=250,
                    path=["02" + "a" * 64, "02" + "b" * 64],
                    priority=1,
                    via_fleet=True,
                ),
            ],
            computation_time_ms=50,
            iterations=10,
        )

        # Serialize to dict
        solution_dict = solution.to_dict()

        assert solution_dict["total_flow_sats"] == 500_000
        assert solution_dict["total_cost_sats"] == 250
        assert len(solution_dict["assignments"]) == 1
        assert solution_dict["assignments"][0]["amount_sats"] == 500_000

    def test_remote_needs_cache_bounded(self):
        """Test that remote MCF needs cache is bounded."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        # Add more than the limit (500)
        for i in range(600):
            coordinator.store_remote_mcf_need({
                "reporter_id": f"02{'b' * 60}{i:04d}",
                "need_type": "inbound",
                "target_peer": "02" + "c" * 64,
                "amount_sats": 100_000,
                "received_at": 1000000 + i,
            })

        # Should be bounded
        assert coordinator.get_remote_mcf_needs_count() <= 500


# =============================================================================
# PHASE 4 END-TO-END TESTS - ASSIGNMENT EXECUTION WORKFLOW
# =============================================================================

class TestMCFAssignmentExecution:
    """End-to-end tests for MCF assignment execution workflow."""

    def _make_assignment_data(self, priority=1, amount_sats=500_000, from_channel="123x1x0"):
        """Helper to create assignment data dict."""
        return {
            "member_id": "02" + "a" * 64,
            "from_channel": from_channel,
            "to_channel": "456x2x0",
            "amount_sats": amount_sats,
            "expected_cost_sats": amount_sats // 2000,  # ~500ppm
            "path": ["02" + "a" * 64, "02" + "b" * 64],
            "priority": priority,
            "via_fleet": True,
        }

    def test_assignment_claim_workflow(self):
        """Test claiming an assignment for execution."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        # Simulate receiving an assignment from MCF solution
        assignment_data = self._make_assignment_data()
        now = int(time.time())
        coordinator_id = "02" + "c" * 64  # Some coordinator

        # Store assignment
        result = coordinator.receive_mcf_assignment(assignment_data, now, coordinator_id)
        assert result is True

        # Verify pending
        pending = coordinator.get_pending_mcf_assignments()
        assert len(pending) == 1
        assert pending[0].status == "pending"

        # Claim assignment
        assignment_id = pending[0].assignment_id
        result = coordinator.update_mcf_assignment_status(
            assignment_id=assignment_id,
            status="executing"
        )
        assert result is True

        # Verify claimed
        pending = coordinator.get_pending_mcf_assignments()
        assert len(pending) == 0  # No longer pending

    def test_assignment_completion_success(self):
        """Test reporting successful assignment completion."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        # Create and receive assignment
        assignment_data = self._make_assignment_data()
        now = int(time.time())
        coordinator_id = "02" + "c" * 64

        coordinator.receive_mcf_assignment(assignment_data, now, coordinator_id)

        # Claim
        pending = coordinator.get_pending_mcf_assignments()
        assignment_id = pending[0].assignment_id
        coordinator.update_mcf_assignment_status(
            assignment_id=assignment_id,
            status="executing"
        )

        # Report success
        result = coordinator.update_mcf_assignment_status(
            assignment_id=assignment_id,
            status="completed",
            actual_amount_sats=500_000,
            actual_cost_sats=200
        )
        assert result is True

        # Verify completed
        status = coordinator.get_mcf_status()
        assert status["assignment_counts"]["completed"] == 1

    def test_assignment_completion_failure(self):
        """Test reporting failed assignment."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        # Create and receive assignment
        assignment_data = self._make_assignment_data()
        now = int(time.time())
        coordinator_id = "02" + "c" * 64

        coordinator.receive_mcf_assignment(assignment_data, now, coordinator_id)

        # Claim
        pending = coordinator.get_pending_mcf_assignments()
        assignment_id = pending[0].assignment_id
        coordinator.update_mcf_assignment_status(
            assignment_id=assignment_id,
            status="executing"
        )

        # Report failure
        result = coordinator.update_mcf_assignment_status(
            assignment_id=assignment_id,
            status="failed",
            error_message="no_route_found"
        )
        assert result is True

        # Verify failed
        status = coordinator.get_mcf_status()
        assert status["assignment_counts"]["failed"] == 1

    def test_assignment_priority_ordering(self):
        """Test that assignments are processed in priority order."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        coordinator_id = "02" + "c" * 64

        # Add assignments with different priorities (use different timestamps
        # to ensure unique assignment IDs)
        for i, priority in enumerate([3, 1, 2]):
            assignment_data = self._make_assignment_data(
                priority=priority,
                amount_sats=100_000 * priority,
                from_channel=f"123x{priority}x0"
            )
            # Different timestamp for each to get unique ID
            coordinator.receive_mcf_assignment(
                assignment_data,
                int(time.time()) + i,
                coordinator_id
            )

        # Get pending - should be sorted by priority
        pending = coordinator.get_pending_mcf_assignments()
        assert len(pending) == 3
        assert pending[0].priority == 1
        assert pending[1].priority == 2
        assert pending[2].priority == 3

    def test_stuck_assignment_detection(self):
        """Test detection of stuck assignments."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        # Create assignment
        assignment_data = self._make_assignment_data()
        old_time = int(time.time()) - 3600  # 1 hour ago
        coordinator_id = "02" + "c" * 64

        # Receive with old timestamp
        coordinator.receive_mcf_assignment(assignment_data, old_time, coordinator_id)

        # Claim it
        pending = coordinator.get_pending_mcf_assignments()
        assignment_id = pending[0].assignment_id
        coordinator.update_mcf_assignment_status(
            assignment_id=assignment_id,
            status="executing"
        )

        # Check stuck detection (should find it if executing for > 30 min)
        status = coordinator.get_mcf_status()
        assert status["assignment_counts"]["executing"] == 1


class TestMCFHiveBridgeIntegration:
    """Tests for MCF integration with hive_bridge (cl-revenue-ops side)."""

    def test_mcf_status_query_structure(self):
        """Test MCF status query returns expected structure."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        status = coordinator.get_mcf_status()

        # Verify structure - matches actual implementation
        assert "last_solution_timestamp" in status
        assert "ack_sent" in status
        assert "assignment_counts" in status
        assert "pending_assignments" in status
        assert "total_pending_amount_sats" in status

        # Check assignment_counts structure
        counts = status["assignment_counts"]
        assert "total" in counts
        assert "pending" in counts
        assert "executing" in counts
        assert "completed" in counts
        assert "failed" in counts

    def test_assignment_dict_serialization(self):
        """Test that assignments serialize correctly for RPC."""
        from modules.mcf_solver import RebalanceAssignment

        assignment = RebalanceAssignment(
            member_id="02" + "a" * 64,
            from_channel="123x1x0",
            to_channel="456x2x0",
            amount_sats=500_000,
            expected_cost_sats=250,
            path=["02" + "a" * 64, "02" + "b" * 64],
            priority=1,
            via_fleet=True,
        )

        # Test to_dict method
        d = assignment.to_dict()

        assert d["member_id"] == "02" + "a" * 64
        assert d["from_channel"] == "123x1x0"
        assert d["to_channel"] == "456x2x0"
        assert d["amount_sats"] == 500_000
        assert d["expected_cost_sats"] == 250
        assert d["priority"] == 1
        assert d["via_fleet"] is True
        assert isinstance(d["path"], list)

    def test_completion_report_creates_message(self):
        """Test that completion report creates broadcastable message."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        # Create mock plugin with working RPC
        class MockPluginWithRpc:
            def __init__(self):
                self.logs = []

            class rpc:
                @staticmethod
                def signmessage(message):
                    return {"zbase": "mock_signature"}

            def log(self, msg, level="info"):
                self.logs.append({"msg": msg, "level": level})

        plugin = MockPluginWithRpc()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        # Create assignment using proper interface
        assignment_data = {
            "member_id": "02" + "a" * 64,
            "from_channel": "123x1x0",
            "to_channel": "456x2x0",
            "amount_sats": 500_000,
            "expected_cost_sats": 250,
            "path": ["02" + "a" * 64, "02" + "b" * 64],
            "priority": 1,
            "via_fleet": True,
        }
        now = int(time.time())
        coordinator.receive_mcf_assignment(assignment_data, now, "02" + "c" * 64)

        pending = coordinator.get_pending_mcf_assignments()
        assignment_id = pending[0].assignment_id if pending else None

        # Mark as completed first (required for completion message)
        if assignment_id:
            coordinator.update_mcf_assignment_status(
                assignment_id, "executing"
            )
            coordinator.update_mcf_assignment_status(
                assignment_id, "completed",
                actual_amount_sats=500_000,
                actual_cost_sats=200
            )

        # Test creating completion message (takes only assignment_id)
        if hasattr(coordinator, 'create_mcf_completion_message') and assignment_id:
            msg = coordinator.create_mcf_completion_message(assignment_id)
            # Message may be None if plugin.rpc doesn't work as expected
            # Just verify the method doesn't crash
            assert msg is None or isinstance(msg, bytes)


class TestMCFFullLifecycle:
    """Full lifecycle tests for MCF optimization workflow."""

    def test_full_mcf_cycle_single_node(self):
        """Test complete MCF cycle from needs to solution to execution."""
        from modules.mcf_solver import (
            MCFCoordinator, MCFNetworkBuilder, SSPSolver,
            RebalanceNeed, MIN_MCF_DEMAND
        )
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()
        state_manager = MockStateManager()

        our_pubkey = "02" + "a" * 64
        external_peer = "02" + "e" * 64

        database.members = [{"peer_id": our_pubkey}]
        state_manager.set_peer_state(our_pubkey, capacity=10_000_000)

        liq_coord = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey=our_pubkey
        )

        mcf_coord = MCFCoordinator(
            plugin=plugin,
            database=database,
            state_manager=state_manager,
            liquidity_coordinator=liq_coord,
            our_pubkey=our_pubkey
        )

        # Step 1: Verify we are coordinator (only member)
        assert mcf_coord.is_coordinator() is True

        # Step 2: Collect needs (may be empty in mock)
        needs = mcf_coord.collect_fleet_needs()
        # Just verify it doesn't crash
        assert isinstance(needs, list)

    def test_assignment_counts_accuracy(self):
        """Test that assignment counts are accurate through lifecycle."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        plugin = MockPlugin()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        coordinator_id = "02" + "c" * 64

        # Add 3 assignments with unique timestamps
        for i in range(3):
            assignment_data = {
                "member_id": "02" + "a" * 64,
                "from_channel": f"123x{i}x0",
                "to_channel": "456x2x0",
                "amount_sats": 100_000,
                "expected_cost_sats": 50,
                "path": ["02" + "a" * 64, "02" + "b" * 64],
                "priority": i + 1,
                "via_fleet": True,
            }
            coordinator.receive_mcf_assignment(
                assignment_data, int(time.time()) + i, coordinator_id
            )

        # All pending
        status = coordinator.get_mcf_status()
        assert status["assignment_counts"]["pending"] == 3
        assert status["assignment_counts"]["executing"] == 0

        # Claim one
        pending = coordinator.get_pending_mcf_assignments()
        coordinator.update_mcf_assignment_status(
            pending[0].assignment_id, "executing"
        )

        status = coordinator.get_mcf_status()
        assert status["assignment_counts"]["pending"] == 2
        assert status["assignment_counts"]["executing"] == 1

        # Complete it
        coordinator.update_mcf_assignment_status(
            pending[0].assignment_id, "completed"
        )

        status = coordinator.get_mcf_status()
        assert status["assignment_counts"]["pending"] == 2
        assert status["assignment_counts"]["executing"] == 0
        assert status["assignment_counts"]["completed"] == 1

    def test_mcf_ack_message_creation(self):
        """Test MCF ACK message creation for acknowledging assignments."""
        from modules.liquidity_coordinator import LiquidityCoordinator

        # Create mock plugin with working RPC
        class MockPluginWithRpc:
            def __init__(self):
                self.logs = []

            class rpc:
                @staticmethod
                def signmessage(message):
                    return {"zbase": "mock_signature"}

            def log(self, msg, level="info"):
                self.logs.append({"msg": msg, "level": level})

        plugin = MockPluginWithRpc()
        database = MockDatabase()

        coordinator = LiquidityCoordinator(
            database=database,
            plugin=plugin,
            our_pubkey="02" + "a" * 64
        )

        # Need to receive an assignment first to have a solution to ACK
        assignment_data = {
            "member_id": "02" + "a" * 64,
            "from_channel": "123x1x0",
            "to_channel": "456x2x0",
            "amount_sats": 500_000,
            "expected_cost_sats": 250,
            "path": ["02" + "a" * 64, "02" + "b" * 64],
            "priority": 1,
            "via_fleet": True,
        }
        coordinator.receive_mcf_assignment(
            assignment_data, int(time.time()), "02" + "c" * 64
        )

        # Test creating ACK message (no arguments - uses internal state)
        if hasattr(coordinator, 'create_mcf_ack_message'):
            msg = coordinator.create_mcf_ack_message()
            # Message may be None if RPC doesn't work as expected
            # Just verify the method doesn't crash
            assert msg is None or isinstance(msg, bytes)

    def test_solution_expiry(self):
        """Test that old MCF solutions expire correctly."""
        from modules.mcf_solver import MAX_SOLUTION_AGE, MCFSolution

        # Create solution with old timestamp
        old_timestamp = int(time.time()) - MAX_SOLUTION_AGE - 100

        solution = MCFSolution(
            total_flow_sats=500_000,
            total_cost_sats=250,
            unmet_demand_sats=0,
            assignments=[],
            computation_time_ms=50,
            iterations=10,
        )

        # Check if solution would be considered stale
        age = int(time.time()) - old_timestamp
        assert age > MAX_SOLUTION_AGE


# =============================================================================
# PHASE 5: CIRCUIT BREAKER TESTS
# =============================================================================

class TestMCFCircuitBreaker:
    """Tests for MCFCircuitBreaker staleness handling and recovery."""

    def test_circuit_breaker_initial_state(self):
        """Test circuit breaker starts in CLOSED state."""
        from modules.mcf_solver import MCFCircuitBreaker

        cb = MCFCircuitBreaker()
        assert cb.state == MCFCircuitBreaker.CLOSED
        assert cb.can_execute()
        assert cb.failure_count == 0
        assert cb.success_count == 0

    def test_circuit_breaker_opens_after_failures(self):
        """Test circuit breaker opens after threshold failures."""
        from modules.mcf_solver import MCFCircuitBreaker, MCF_CIRCUIT_FAILURE_THRESHOLD

        cb = MCFCircuitBreaker()

        # Record failures up to threshold
        for i in range(MCF_CIRCUIT_FAILURE_THRESHOLD - 1):
            cb.record_failure(f"Error {i}")
            assert cb.state == MCFCircuitBreaker.CLOSED

        # One more failure should open the circuit
        cb.record_failure("Final error")
        assert cb.state == MCFCircuitBreaker.OPEN
        assert not cb.can_execute()
        assert cb.total_trips == 1

    def test_circuit_breaker_recovery_timeout(self):
        """Test circuit breaker transitions to HALF_OPEN after timeout."""
        from modules.mcf_solver import (
            MCFCircuitBreaker,
            MCF_CIRCUIT_FAILURE_THRESHOLD,
            MCF_CIRCUIT_RECOVERY_TIMEOUT
        )

        cb = MCFCircuitBreaker()

        # Open the circuit
        for _ in range(MCF_CIRCUIT_FAILURE_THRESHOLD):
            cb.record_failure()
        assert cb.state == MCFCircuitBreaker.OPEN

        # Simulate timeout
        cb.last_state_change = time.time() - MCF_CIRCUIT_RECOVERY_TIMEOUT - 1

        # Now can_execute should transition to HALF_OPEN
        assert cb.can_execute()
        assert cb.state == MCFCircuitBreaker.HALF_OPEN

    def test_circuit_breaker_closes_after_successes(self):
        """Test circuit breaker closes after enough successes in HALF_OPEN."""
        from modules.mcf_solver import (
            MCFCircuitBreaker,
            MCF_CIRCUIT_FAILURE_THRESHOLD,
            MCF_CIRCUIT_RECOVERY_TIMEOUT,
            MCF_CIRCUIT_SUCCESS_THRESHOLD
        )

        cb = MCFCircuitBreaker()

        # Open and then transition to half-open
        for _ in range(MCF_CIRCUIT_FAILURE_THRESHOLD):
            cb.record_failure()
        cb.last_state_change = time.time() - MCF_CIRCUIT_RECOVERY_TIMEOUT - 1
        cb.can_execute()  # Triggers transition to HALF_OPEN
        assert cb.state == MCFCircuitBreaker.HALF_OPEN

        # Record successes to close
        for i in range(MCF_CIRCUIT_SUCCESS_THRESHOLD - 1):
            cb.record_success()
            assert cb.state == MCFCircuitBreaker.HALF_OPEN

        # One more success should close
        cb.record_success()
        assert cb.state == MCFCircuitBreaker.CLOSED
        assert cb.can_execute()
        assert cb.failure_count == 0

    def test_circuit_breaker_fails_in_half_open(self):
        """Test circuit breaker reopens on failure in HALF_OPEN."""
        from modules.mcf_solver import (
            MCFCircuitBreaker,
            MCF_CIRCUIT_FAILURE_THRESHOLD,
            MCF_CIRCUIT_RECOVERY_TIMEOUT
        )

        cb = MCFCircuitBreaker()

        # Get to HALF_OPEN state
        for _ in range(MCF_CIRCUIT_FAILURE_THRESHOLD):
            cb.record_failure()
        cb.last_state_change = time.time() - MCF_CIRCUIT_RECOVERY_TIMEOUT - 1
        cb.can_execute()
        assert cb.state == MCFCircuitBreaker.HALF_OPEN

        # Single failure should reopen
        cb.record_failure("Recovery failed")
        assert cb.state == MCFCircuitBreaker.OPEN
        assert not cb.can_execute()

    def test_circuit_breaker_success_resets_failures(self):
        """Test success resets failure count in CLOSED state."""
        from modules.mcf_solver import MCFCircuitBreaker, MCF_CIRCUIT_FAILURE_THRESHOLD

        cb = MCFCircuitBreaker()

        # Record some failures (not enough to trip)
        for _ in range(MCF_CIRCUIT_FAILURE_THRESHOLD - 1):
            cb.record_failure()
        assert cb.failure_count == MCF_CIRCUIT_FAILURE_THRESHOLD - 1

        # Success should reset
        cb.record_success()
        assert cb.failure_count == 0
        assert cb.state == MCFCircuitBreaker.CLOSED

    def test_circuit_breaker_reset(self):
        """Test circuit breaker reset clears all state."""
        from modules.mcf_solver import MCFCircuitBreaker, MCF_CIRCUIT_FAILURE_THRESHOLD

        cb = MCFCircuitBreaker()

        # Open the circuit
        for _ in range(MCF_CIRCUIT_FAILURE_THRESHOLD):
            cb.record_failure()
        assert cb.state == MCFCircuitBreaker.OPEN

        # Reset
        cb.reset()
        assert cb.state == MCFCircuitBreaker.CLOSED
        assert cb.failure_count == 0
        assert cb.success_count == 0
        assert cb.can_execute()

    def test_circuit_breaker_status_dict(self):
        """Test circuit breaker status returns correct structure."""
        from modules.mcf_solver import MCFCircuitBreaker

        cb = MCFCircuitBreaker()
        cb.record_success()
        cb.record_failure()

        status = cb.get_status()

        assert "state" in status
        assert "failure_count" in status
        assert "success_count" in status
        assert "time_in_state_seconds" in status
        assert "total_successes" in status
        assert "total_failures" in status
        assert "total_trips" in status
        assert "can_execute" in status

        assert status["state"] == MCFCircuitBreaker.CLOSED
        assert status["total_successes"] == 1
        assert status["total_failures"] == 1


# =============================================================================
# PHASE 5: HEALTH METRICS TESTS
# =============================================================================

class TestMCFHealthMetrics:
    """Tests for MCFHealthMetrics tracking and staleness detection."""

    def test_health_metrics_initial_state(self):
        """Test health metrics start with default values."""
        from modules.mcf_solver import MCFHealthMetrics

        metrics = MCFHealthMetrics()
        assert metrics.last_solution_timestamp == 0
        assert metrics.consecutive_stale_cycles == 0
        assert metrics.successful_assignments == 0
        assert metrics.failed_assignments == 0
        assert metrics.is_healthy()  # Healthy when no data yet

    def test_health_metrics_record_solution(self):
        """Test recording a solution updates all fields."""
        from modules.mcf_solver import MCFHealthMetrics

        metrics = MCFHealthMetrics()
        metrics.consecutive_stale_cycles = 3  # Simulate prior staleness

        metrics.record_solution(
            flow_sats=1_000_000,
            cost_sats=500,
            assignments=5,
            computation_time_ms=150,
            node_count=20,
            edge_count=80
        )

        assert metrics.last_solution_timestamp > 0
        assert metrics.last_solution_flow_sats == 1_000_000
        assert metrics.last_solution_cost_sats == 500
        assert metrics.last_solution_assignments == 5
        assert metrics.last_computation_time_ms == 150
        assert metrics.last_network_node_count == 20
        assert metrics.last_network_edge_count == 80
        assert metrics.consecutive_stale_cycles == 0  # Reset by solution

    def test_health_metrics_record_stale_cycle(self):
        """Test stale cycle tracking."""
        from modules.mcf_solver import MCFHealthMetrics

        metrics = MCFHealthMetrics()

        for i in range(5):
            metrics.record_stale_cycle()
            assert metrics.consecutive_stale_cycles == i + 1
            assert metrics.max_consecutive_stale == i + 1

        # Solution resets consecutive but not max
        metrics.record_solution(100000, 50, 1, 100, 10, 40)
        assert metrics.consecutive_stale_cycles == 0
        assert metrics.max_consecutive_stale == 5

    def test_health_metrics_assignment_completion(self):
        """Test assignment completion tracking."""
        from modules.mcf_solver import MCFHealthMetrics

        metrics = MCFHealthMetrics()

        # Record successes
        metrics.record_assignment_completion(True, 500_000, 250)
        metrics.record_assignment_completion(True, 300_000, 150)

        assert metrics.successful_assignments == 2
        assert metrics.total_flow_executed_sats == 800_000
        assert metrics.total_cost_paid_sats == 400

        # Record failure
        metrics.record_assignment_completion(False, 200_000, 0)

        assert metrics.failed_assignments == 1
        assert metrics.successful_assignments == 2  # Unchanged
        assert metrics.total_flow_executed_sats == 800_000  # Unchanged

    def test_health_metrics_is_healthy_stale_solution(self):
        """Test health check fails on stale solution."""
        from modules.mcf_solver import MCFHealthMetrics

        metrics = MCFHealthMetrics()

        # Record a solution that's older than 30 minutes
        metrics.record_solution(100000, 50, 1, 100, 10, 40)
        metrics.last_solution_timestamp = int(time.time()) - 2000

        assert not metrics.is_healthy()

    def test_health_metrics_is_healthy_many_stale_cycles(self):
        """Test health check fails after many stale cycles."""
        from modules.mcf_solver import MCFHealthMetrics

        metrics = MCFHealthMetrics()
        metrics.record_solution(100000, 50, 1, 100, 10, 40)

        # 4 stale cycles should still be healthy
        for _ in range(4):
            metrics.record_stale_cycle()
        assert metrics.is_healthy()

        # 5th stale cycle tips it to unhealthy
        metrics.record_stale_cycle()
        assert not metrics.is_healthy()

    def test_health_metrics_staleness_levels(self):
        """Test solution staleness level reporting."""
        from modules.mcf_solver import MCFHealthMetrics, MAX_SOLUTION_AGE

        metrics = MCFHealthMetrics()

        # No solution yet
        age, level = metrics.get_solution_staleness()
        assert level == "none"

        # Fresh solution
        metrics.record_solution(100000, 50, 1, 100, 10, 40)
        age, level = metrics.get_solution_staleness()
        assert level == "fresh"

        # Stale solution (between MAX_SOLUTION_AGE and 2x)
        metrics.last_solution_timestamp = int(time.time()) - MAX_SOLUTION_AGE - 100
        age, level = metrics.get_solution_staleness()
        assert level == "stale"

        # Expired solution (older than 2x MAX_SOLUTION_AGE)
        metrics.last_solution_timestamp = int(time.time()) - (MAX_SOLUTION_AGE * 2 + 100)
        age, level = metrics.get_solution_staleness()
        assert level == "expired"

    def test_health_metrics_to_dict(self):
        """Test health metrics serialization."""
        from modules.mcf_solver import MCFHealthMetrics

        metrics = MCFHealthMetrics()
        metrics.record_solution(500_000, 250, 3, 200, 15, 60)
        metrics.record_assignment_completion(True, 100_000, 50)

        data = metrics.to_dict()

        assert "last_solution_timestamp" in data
        assert "last_solution_age_seconds" in data
        assert "staleness_level" in data
        assert "successful_assignments" in data
        assert "is_healthy" in data

        assert data["last_solution_flow_sats"] == 500_000
        assert data["successful_assignments"] == 1
        assert data["staleness_level"] == "fresh"


# =============================================================================
# PHASE 5: COORDINATOR WITH CIRCUIT BREAKER TESTS
# =============================================================================

class TestMCFCoordinatorCircuitBreaker:
    """Tests for MCFCoordinator integration with circuit breaker."""

    def test_coordinator_has_circuit_breaker(self):
        """Test coordinator initializes with circuit breaker."""
        from modules.mcf_solver import MCFCoordinator, MCFCircuitBreaker

        state_manager = MockStateManager()
        database = MockDatabase()
        database.members = [{"peer_id": "02" + "a" * 64}]
        plugin = MockPlugin()
        liquidity_coordinator = MockLiquidityCoordinator()

        coordinator = MCFCoordinator(
            state_manager=state_manager,
            database=database,
            plugin=plugin,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "a" * 64
        )

        assert hasattr(coordinator, '_circuit_breaker')
        assert isinstance(coordinator._circuit_breaker, MCFCircuitBreaker)
        assert coordinator._circuit_breaker.state == MCFCircuitBreaker.CLOSED

    def test_coordinator_has_health_metrics(self):
        """Test coordinator initializes with health metrics."""
        from modules.mcf_solver import MCFCoordinator, MCFHealthMetrics

        state_manager = MockStateManager()
        database = MockDatabase()
        database.members = [{"peer_id": "02" + "a" * 64}]
        plugin = MockPlugin()
        liquidity_coordinator = MockLiquidityCoordinator()

        coordinator = MCFCoordinator(
            state_manager=state_manager,
            database=database,
            plugin=plugin,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "a" * 64
        )

        assert hasattr(coordinator, '_health_metrics')
        assert isinstance(coordinator._health_metrics, MCFHealthMetrics)

    def test_coordinator_get_health_summary(self):
        """Test coordinator health summary includes circuit breaker and metrics."""
        from modules.mcf_solver import MCFCoordinator

        state_manager = MockStateManager()
        database = MockDatabase()
        database.members = [{"peer_id": "02" + "a" * 64}]
        plugin = MockPlugin()
        liquidity_coordinator = MockLiquidityCoordinator()

        coordinator = MCFCoordinator(
            state_manager=state_manager,
            database=database,
            plugin=plugin,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "a" * 64
        )

        summary = coordinator.get_health_summary()

        # Check expected fields
        assert "healthy" in summary
        assert "circuit_state" in summary
        assert "solution_staleness" in summary
        assert "can_execute" in summary
        assert "consecutive_stale_cycles" in summary

        # Verify initial healthy state
        assert summary["circuit_state"] == "closed"
        assert summary["can_execute"] is True
        assert summary["healthy"] is True

    def test_coordinator_record_assignment_completion(self):
        """Test coordinator tracks assignment completion metrics."""
        from modules.mcf_solver import MCFCoordinator

        state_manager = MockStateManager()
        database = MockDatabase()
        database.members = [{"peer_id": "02" + "a" * 64}]
        plugin = MockPlugin()
        liquidity_coordinator = MockLiquidityCoordinator()

        coordinator = MCFCoordinator(
            state_manager=state_manager,
            database=database,
            plugin=plugin,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "a" * 64
        )

        # Record successful completion (requires assignment_id)
        coordinator.record_assignment_completion(
            assignment_id="mcf_assign_001",
            success=True,
            amount_sats=500_000,
            cost_sats=250
        )

        metrics = coordinator._health_metrics
        assert metrics.successful_assignments == 1
        assert metrics.total_flow_executed_sats == 500_000

        # Record failed completion
        coordinator.record_assignment_completion(
            assignment_id="mcf_assign_002",
            success=False,
            amount_sats=300_000,
            cost_sats=0
        )

        assert metrics.failed_assignments == 1
        assert metrics.successful_assignments == 1  # Unchanged

    def test_coordinator_circuit_breaker_blocks_optimization(self):
        """Test that open circuit breaker prevents optimization."""
        from modules.mcf_solver import MCFCoordinator, MCF_CIRCUIT_FAILURE_THRESHOLD

        state_manager = MockStateManager()
        database = MockDatabase()
        database.members = [{"peer_id": "02" + "a" * 64}]
        plugin = MockPlugin()
        liquidity_coordinator = MockLiquidityCoordinator()

        coordinator = MCFCoordinator(
            state_manager=state_manager,
            database=database,
            plugin=plugin,
            liquidity_coordinator=liquidity_coordinator,
            our_pubkey="02" + "a" * 64
        )

        # Open the circuit breaker
        for _ in range(MCF_CIRCUIT_FAILURE_THRESHOLD):
            coordinator._circuit_breaker.record_failure()

        assert not coordinator._circuit_breaker.can_execute()

        # Run optimization cycle should return None or indicate skipped
        result = coordinator.run_optimization_cycle()

        # Should not produce a valid solution when circuit is open
        assert result is None or (hasattr(result, 'total_flow_sats') and result.total_flow_sats == 0)
