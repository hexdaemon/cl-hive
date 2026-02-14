"""
Min-Cost Max-Flow (MCF) Solver for Global Fleet Rebalance Optimization.

This module implements a Successive Shortest Paths (SSP) algorithm with
Dijkstra+Johnson potentials for finding optimal fleet-wide rebalancing
assignments.

Key Benefits:
- Global optimization vs local decisions
- Prefers zero-fee hive internal channels automatically
- Prevents circular flows at planning stage
- Coordinates simultaneous rebalances across fleet

Algorithm: Successive Shortest Paths (SSP) with Dijkstra+Johnson Potentials

The first shortest-path query uses Bellman-Ford (O(V*E)) to handle negative
residual costs and establish Johnson potentials. All subsequent queries use
Dijkstra (O(E log V)) with reduced costs guaranteed non-negative.

Why SSP:
1. Handles asymmetric channel capacities and per-direction fees
2. Bellman-Ford bootstrap handles negative reduced costs in residual networks
3. Dijkstra acceleration keeps per-path queries fast after first iteration
4. Fleet sizes (5-50 members, ~500 edges) are well within bounds
5. Can warm-start from previous solutions

Complexity: O(E log V * flow) after first iteration - under 1 second for typical fleets

Author: Lightning Goats Team
"""

import heapq
import time
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict


# =============================================================================
# CONSTANTS
# =============================================================================

# MCF solver configuration
MCF_CYCLE_INTERVAL = 600           # 10 minutes between optimization cycles
MAX_GOSSIP_AGE_FOR_MCF = 900       # 15 minutes max gossip age for fresh data
MAX_SOLUTION_AGE = 1200            # 20 minutes max solution validity
MIN_MCF_DEMAND = 100000            # 100k sats minimum to trigger MCF

# Algorithm limits
MAX_MCF_ITERATIONS = 1000          # Maximum augmentation iterations
MAX_BELLMAN_FORD_ITERATIONS = 500  # Maximum BF iterations (for cycle detection)
INFINITY = float('inf')

# Network size limits (prevent unbounded memory)
MAX_MCF_NODES = 200                # Maximum nodes in network
# INVARIANT: MAX_BELLMAN_FORD_ITERATIONS must be >= MAX_MCF_NODES
assert MAX_BELLMAN_FORD_ITERATIONS >= MAX_MCF_NODES, "BF iterations must be >= node count"
MAX_MCF_EDGES = 2000               # Maximum edges in network

# Cost scaling
HIVE_INTERNAL_COST_PPM = 0         # Zero fees for hive internal channels
DEFAULT_EXTERNAL_COST_PPM = 500    # Default external route cost estimate

# Assignment validation
MAX_ASSIGNMENT_AMOUNT_SATS = 50_000_000  # 0.5 BTC max per assignment
MAX_TOTAL_SOLUTION_SATS = 500_000_000    # 5 BTC max total solution flow

# Circuit breaker configuration
MCF_CIRCUIT_FAILURE_THRESHOLD = 3  # Failures before opening circuit
MCF_CIRCUIT_RECOVERY_TIMEOUT = 300 # 5 minutes before half-open
MCF_CIRCUIT_SUCCESS_THRESHOLD = 2  # Successes needed to close from half-open


# =============================================================================
# CIRCUIT BREAKER FOR MCF OPERATIONS
# =============================================================================

class MCFCircuitBreaker:
    """
    Circuit breaker pattern for MCF optimization.

    Prevents cascading failures when MCF is having problems.

    States:
    - CLOSED: Normal operation, MCF runs normally
    - OPEN: MCF disabled due to failures, use fallback
    - HALF_OPEN: Testing if MCF has recovered
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self):
        self._lock = threading.Lock()
        self.state = self.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0
        self.last_state_change = time.time()

        # Metrics
        self.total_successes = 0
        self.total_failures = 0
        self.total_trips = 0  # Times circuit opened

    def record_success(self) -> None:
        """Record a successful MCF operation."""
        with self._lock:
            self.total_successes += 1
            self.failure_count = 0

            if self.state == self.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= MCF_CIRCUIT_SUCCESS_THRESHOLD:
                    self._transition_to(self.CLOSED)
            elif self.state == self.OPEN:
                # Shouldn't happen, but reset just in case
                self._transition_to(self.CLOSED)

    def record_failure(self, error: str = "") -> None:
        """Record a failed MCF operation."""
        with self._lock:
            self.total_failures += 1
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == self.CLOSED:
                if self.failure_count >= MCF_CIRCUIT_FAILURE_THRESHOLD:
                    self._transition_to(self.OPEN)
                    self.total_trips += 1
            elif self.state == self.HALF_OPEN:
                # Single failure in half-open goes back to open
                self._transition_to(self.OPEN)

    def can_execute(self) -> bool:
        """Check if MCF operation should be attempted."""
        with self._lock:
            return self._can_execute_unlocked()

    def _can_execute_unlocked(self) -> bool:
        """Check if MCF operation should be attempted. Caller must hold self._lock."""
        if self.state == self.CLOSED:
            return True

        if self.state == self.OPEN:
            # Check if recovery timeout has passed
            elapsed = time.time() - self.last_state_change
            if elapsed >= MCF_CIRCUIT_RECOVERY_TIMEOUT:
                self._transition_to(self.HALF_OPEN)
                return True
            return False

        # HALF_OPEN - allow one attempt
        return True

    def _transition_to(self, new_state: str) -> None:
        """Transition to a new state. Caller must hold self._lock."""
        self.state = new_state
        self.last_state_change = time.time()
        if new_state == self.CLOSED:
            self.failure_count = 0
            self.success_count = 0
        elif new_state == self.HALF_OPEN:
            self.success_count = 0

    def get_status(self) -> Dict[str, Any]:
        """Get circuit breaker status."""
        with self._lock:
            can_exec = self._can_execute_unlocked()
            now = time.time()
            return {
                "state": self.state,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "time_in_state_seconds": int(now - self.last_state_change),
                "total_successes": self.total_successes,
                "total_failures": self.total_failures,
                "total_trips": self.total_trips,
                "can_execute": can_exec,
            }

    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        with self._lock:
            self.state = self.CLOSED
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = 0
            self.last_state_change = time.time()


# =============================================================================
# MCF HEALTH METRICS
# =============================================================================

@dataclass
class MCFHealthMetrics:
    """
    Tracks MCF solver health and performance metrics.

    Used for monitoring and alerting. Thread-safe via _metrics_lock.
    """
    # Solution metrics
    last_solution_timestamp: int = 0
    last_solution_flow_sats: int = 0
    last_solution_cost_sats: int = 0
    last_solution_assignments: int = 0
    last_computation_time_ms: int = 0

    # Staleness tracking
    consecutive_stale_cycles: int = 0
    max_consecutive_stale: int = 0

    # Execution metrics
    successful_assignments: int = 0
    failed_assignments: int = 0
    total_flow_executed_sats: int = 0
    total_cost_paid_sats: int = 0

    # Network health
    last_network_node_count: int = 0
    last_network_edge_count: int = 0

    def __post_init__(self):
        self._metrics_lock = threading.Lock()

    def record_solution(
        self,
        flow_sats: int,
        cost_sats: int,
        assignments: int,
        computation_time_ms: int,
        node_count: int,
        edge_count: int
    ) -> None:
        """Record metrics from a successful solution."""
        with self._metrics_lock:
            self.last_solution_timestamp = int(time.time())
            self.last_solution_flow_sats = flow_sats
            self.last_solution_cost_sats = cost_sats
            self.last_solution_assignments = assignments
            self.last_computation_time_ms = computation_time_ms
            self.last_network_node_count = node_count
            self.last_network_edge_count = edge_count
            self.consecutive_stale_cycles = 0

    def record_stale_cycle(self) -> None:
        """Record that a cycle had stale/insufficient data."""
        with self._metrics_lock:
            self.consecutive_stale_cycles += 1
            self.max_consecutive_stale = max(
                self.max_consecutive_stale,
                self.consecutive_stale_cycles
            )

    def record_assignment_completion(
        self,
        success: bool,
        amount_sats: int,
        cost_sats: int
    ) -> None:
        """Record completion of an assignment."""
        with self._metrics_lock:
            if success:
                self.successful_assignments += 1
                self.total_flow_executed_sats += amount_sats
                self.total_cost_paid_sats += cost_sats
            else:
                self.failed_assignments += 1

    def is_healthy(self) -> bool:
        """Check if MCF is operating healthily."""
        now = time.time()
        solution_age = now - self.last_solution_timestamp

        # Unhealthy if no solution in 30 minutes
        if self.last_solution_timestamp > 0 and solution_age > 1800:
            return False

        # Unhealthy if 5+ consecutive stale cycles
        if self.consecutive_stale_cycles >= 5:
            return False

        return True

    def get_solution_staleness(self) -> Tuple[int, str]:
        """
        Get solution age and staleness level.

        Returns:
            (age_seconds, staleness_level)
            staleness_level: "fresh", "stale", "expired", "none"
        """
        if self.last_solution_timestamp == 0:
            return (0, "none")

        age = int(time.time()) - self.last_solution_timestamp

        if age < MAX_SOLUTION_AGE:
            return (age, "fresh")
        elif age < MAX_SOLUTION_AGE * 2:
            return (age, "stale")
        else:
            return (age, "expired")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for RPC/serialization."""
        age, staleness = self.get_solution_staleness()
        return {
            "last_solution_timestamp": self.last_solution_timestamp,
            "last_solution_age_seconds": age,
            "staleness_level": staleness,
            "last_solution_flow_sats": self.last_solution_flow_sats,
            "last_solution_cost_sats": self.last_solution_cost_sats,
            "last_solution_assignments": self.last_solution_assignments,
            "last_computation_time_ms": self.last_computation_time_ms,
            "consecutive_stale_cycles": self.consecutive_stale_cycles,
            "max_consecutive_stale": self.max_consecutive_stale,
            "successful_assignments": self.successful_assignments,
            "failed_assignments": self.failed_assignments,
            "total_flow_executed_sats": self.total_flow_executed_sats,
            "total_cost_paid_sats": self.total_cost_paid_sats,
            "network_node_count": self.last_network_node_count,
            "network_edge_count": self.last_network_edge_count,
            "is_healthy": self.is_healthy(),
        }


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class MCFEdge:
    """
    Edge in the MCF network representing a channel direction.

    In the residual network, each channel creates two edges:
    - Forward edge with capacity and cost
    - Reverse edge with 0 initial capacity (filled by flow)
    """
    from_node: str              # Source node pubkey
    to_node: str                # Destination node pubkey
    capacity: int               # Max flow in sats
    cost_ppm: int               # Cost per million (0 for hive internal)
    residual_capacity: int      # Current residual capacity
    flow: int = 0               # Current flow on this edge
    reverse_edge_idx: int = -1  # Index of reverse edge in adjacency list
    channel_id: str = ""        # SCID for identification
    is_hive_internal: bool = False  # True if between hive members
    is_reverse: bool = False        # True if this is a reverse (residual) edge

    def unit_cost(self, amount: int) -> int:
        """Calculate cost for flowing `amount` sats."""
        return (amount * self.cost_ppm + 500_000) // 1_000_000


@dataclass
class MCFNode:
    """
    Node in the MCF network representing a node in the fleet topology.

    Supply/demand semantics:
    - supply > 0: Source node (has excess liquidity to give)
    - supply < 0: Sink node (needs liquidity)
    - supply == 0: Transit node
    """
    node_id: str                # Pubkey
    supply: int = 0             # Positive=source, negative=sink (in sats)
    is_fleet_member: bool = False
    outgoing_edges: List[int] = field(default_factory=list)  # Edge indices


@dataclass
class RebalanceNeed:
    """
    A single rebalancing need from a fleet member.

    Used as input to the MCF solver.
    """
    member_id: str              # Which fleet member has this need
    need_type: str              # 'inbound' or 'outbound'
    target_peer: str            # External peer or hive member
    amount_sats: int            # Amount needed
    urgency: str = "medium"     # 'critical', 'high', 'medium', 'low'
    max_fee_ppm: int = 1000     # Maximum fee willing to pay
    channel_id: str = ""        # Specific channel if known

    def to_dict(self) -> Dict[str, Any]:
        return {
            "member_id": self.member_id,
            "need_type": self.need_type,
            "target_peer": self.target_peer,
            "amount_sats": self.amount_sats,
            "urgency": self.urgency,
            "max_fee_ppm": self.max_fee_ppm,
            "channel_id": self.channel_id,
        }


@dataclass
class RebalanceAssignment:
    """
    Assignment for a specific fleet member to execute a rebalance.

    This is the output of the MCF solver - actionable instructions
    for each member involved in the optimal flow.
    """
    member_id: str              # Which fleet member executes
    from_channel: str           # Source channel SCID
    to_channel: str             # Destination channel SCID
    amount_sats: int            # Amount to rebalance
    expected_cost_sats: int     # Expected routing cost
    path: List[str] = field(default_factory=list)  # Routing path (pubkeys)
    priority: int = 0           # Execution order (lower = sooner)
    via_fleet: bool = True      # True if routed through hive
    need_id: str = ""           # Reference to original need

    def to_dict(self) -> Dict[str, Any]:
        return {
            "member_id": self.member_id,
            "from_channel": self.from_channel,
            "to_channel": self.to_channel,
            "amount_sats": self.amount_sats,
            "expected_cost_sats": self.expected_cost_sats,
            "path": self.path,
            "priority": self.priority,
            "via_fleet": self.via_fleet,
            "need_id": self.need_id,
        }


@dataclass
class MCFSolution:
    """
    Complete solution from MCF solver.

    Contains all assignments, total flow/cost, and metadata.
    """
    assignments: List[RebalanceAssignment] = field(default_factory=list)
    total_flow_sats: int = 0
    total_cost_sats: int = 0
    unmet_demand_sats: int = 0  # Demand that couldn't be satisfied
    computation_time_ms: int = 0
    iterations: int = 0
    timestamp: int = 0
    coordinator_id: str = ""
    signature: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "assignments": [a.to_dict() for a in self.assignments],
            "total_flow_sats": self.total_flow_sats,
            "total_cost_sats": self.total_cost_sats,
            "unmet_demand_sats": self.unmet_demand_sats,
            "computation_time_ms": self.computation_time_ms,
            "iterations": self.iterations,
            "timestamp": self.timestamp,
            "coordinator_id": self.coordinator_id,
        }


# =============================================================================
# MCF NETWORK
# =============================================================================

class MCFNetwork:
    """
    Graph representation for the MCF problem.

    Nodes represent Lightning nodes (fleet members and external peers).
    Edges represent channel directions with capacity and cost.

    The network includes a super-source and super-sink for multi-commodity flow.
    """

    def __init__(self):
        """Initialize empty network."""
        self.nodes: Dict[str, MCFNode] = {}
        self.edges: List[MCFEdge] = []
        self._node_indices: Dict[str, int] = {}  # For efficient lookup

        # Super-source and super-sink for multiple sources/sinks
        self.super_source = "__SUPER_SOURCE__"
        self.super_sink = "__SUPER_SINK__"

    def add_node(
        self,
        node_id: str,
        supply: int = 0,
        is_fleet_member: bool = False
    ) -> bool:
        """
        Add a node to the network.

        Args:
            node_id: Node pubkey
            supply: Positive for source, negative for sink
            is_fleet_member: True if this is a hive member

        Returns:
            True if the node was added or already exists, False if at capacity
        """
        if len(self.nodes) >= MAX_MCF_NODES:
            return node_id in self.nodes  # False if new node rejected, True if already present

        if node_id not in self.nodes:
            self.nodes[node_id] = MCFNode(
                node_id=node_id,
                supply=supply,
                is_fleet_member=is_fleet_member,
                outgoing_edges=[]
            )
            self._node_indices[node_id] = len(self._node_indices)
        else:
            # Update supply (aggregate from multiple needs)
            self.nodes[node_id].supply += supply
            if is_fleet_member:
                self.nodes[node_id].is_fleet_member = True

        return True

    def add_edge(
        self,
        from_node: str,
        to_node: str,
        capacity: int,
        cost_ppm: int,
        channel_id: str = "",
        is_hive_internal: bool = False
    ) -> int:
        """
        Add a directed edge (channel direction) to the network.

        Also creates the reverse edge for the residual network.

        Args:
            from_node: Source node pubkey
            to_node: Destination node pubkey
            capacity: Maximum flow in sats
            cost_ppm: Cost per million sats
            channel_id: Channel SCID
            is_hive_internal: True if between hive members

        Returns:
            Index of the forward edge
        """
        if len(self.edges) >= MAX_MCF_EDGES - 2:  # -2 for reverse edge
            return -1

        # Ensure nodes exist
        if from_node not in self.nodes:
            if not self.add_node(from_node):
                return -1  # Node limit reached
        if to_node not in self.nodes:
            if not self.add_node(to_node):
                return -1  # Node limit reached

        # Forward edge
        forward_idx = len(self.edges)
        forward_edge = MCFEdge(
            from_node=from_node,
            to_node=to_node,
            capacity=capacity,
            cost_ppm=cost_ppm,
            residual_capacity=capacity,
            channel_id=channel_id,
            is_hive_internal=is_hive_internal,
        )
        self.edges.append(forward_edge)
        self.nodes[from_node].outgoing_edges.append(forward_idx)

        # Reverse edge (for residual network)
        reverse_idx = len(self.edges)
        reverse_edge = MCFEdge(
            from_node=to_node,
            to_node=from_node,
            capacity=0,  # Initially 0, filled as flow is pushed
            cost_ppm=-cost_ppm,  # Negative cost for cancellation
            residual_capacity=0,
            channel_id=channel_id,
            is_hive_internal=is_hive_internal,
            is_reverse=True,
        )
        self.edges.append(reverse_edge)
        self.nodes[to_node].outgoing_edges.append(reverse_idx)

        # Link forward and reverse
        self.edges[forward_idx].reverse_edge_idx = reverse_idx
        self.edges[reverse_idx].reverse_edge_idx = forward_idx

        return forward_idx

    def setup_super_source_sink(self) -> None:
        """
        Add super-source and super-sink for multi-commodity flow.

        Connects all source nodes to super-source and all sink nodes to super-sink.
        """
        # Add super nodes
        self.add_node(self.super_source, supply=0, is_fleet_member=False)
        self.add_node(self.super_sink, supply=0, is_fleet_member=False)

        total_supply = 0
        total_demand = 0

        for node_id, node in self.nodes.items():
            if node_id in (self.super_source, self.super_sink):
                continue

            if node.supply > 0:
                # Source node: connect from super-source
                self.add_edge(
                    self.super_source,
                    node_id,
                    capacity=node.supply,
                    cost_ppm=0,  # No cost from super-source
                    is_hive_internal=True
                )
                total_supply += node.supply

            elif node.supply < 0:
                # Sink node: connect to super-sink
                self.add_edge(
                    node_id,
                    self.super_sink,
                    capacity=-node.supply,  # Capacity is positive
                    cost_ppm=0,  # No cost to super-sink
                    is_hive_internal=True
                )
                total_demand -= node.supply  # Convert negative to positive

        # Set super-source supply and super-sink demand
        self.nodes[self.super_source].supply = total_supply
        self.nodes[self.super_sink].supply = -total_demand

    def get_node_count(self) -> int:
        """Get number of nodes in network."""
        return len(self.nodes)

    def get_edge_count(self) -> int:
        """Get number of edges in network (including reverse edges)."""
        return len(self.edges)


# =============================================================================
# SUCCESSIVE SHORTEST PATHS SOLVER
# =============================================================================

class SSPSolver:
    """
    Successive Shortest Paths (SSP) algorithm for Min-Cost Max-Flow.

    Algorithm overview:
    1. While there exists an augmenting path from source to sink:
       a. Find shortest (min-cost) path using Bellman-Ford
       b. Determine bottleneck capacity
       c. Augment flow along path
       d. Update residual capacities
    2. Return total flow and cost

    Bellman-Ford is used because the residual network can have
    negative-cost edges (from flow cancellation).
    """

    def __init__(self, network: MCFNetwork):
        """
        Initialize solver with network.

        Args:
            network: MCFNetwork instance with nodes, edges, and super-source/sink
        """
        self.network = network
        self.iterations = 0
        self.warnings: List[str] = []
        self._potentials: Dict[str, float] = {}
        self._first_iteration = True

    def solve(self) -> Tuple[int, int, List[Tuple[int, int]]]:
        """
        Find min-cost max-flow in the network.

        Returns:
            Tuple of (total_flow, total_cost, edge_flows)
            where edge_flows is list of (edge_idx, flow_amount)
        """
        total_flow = 0
        total_cost = 0
        self.iterations = 0

        source = self.network.super_source
        sink = self.network.super_sink

        while self.iterations < MAX_MCF_ITERATIONS:
            self.iterations += 1

            # First iteration: Bellman-Ford (handles negative costs, sets potentials)
            # Subsequent: Dijkstra with Johnson potentials (O(E log V) vs O(V*E))
            if self._first_iteration:
                path, path_cost = self._bellman_ford_shortest_path(source, sink)
                self._first_iteration = False
            else:
                path, path_cost = self._dijkstra_shortest_path(source, sink)

            if not path:
                # No more augmenting paths
                break

            # Find bottleneck capacity along path
            bottleneck = self._find_bottleneck(path)

            if bottleneck <= 0:
                break

            # Augment flow along path
            self._augment_flow(path, bottleneck)

            total_flow += bottleneck
            total_cost += (bottleneck * path_cost + 500_000) // 1_000_000

        # Collect edge flows
        edge_flows = []
        for i, edge in enumerate(self.network.edges):
            if edge.flow > 0:
                edge_flows.append((i, edge.flow))

        return total_flow, total_cost, edge_flows

    def _bellman_ford_shortest_path(
        self,
        source: str,
        sink: str
    ) -> Tuple[List[int], int]:
        """
        Find shortest (min-cost) path from source to sink using Bellman-Ford.

        Works with negative edge costs (from residual network).

        Args:
            source: Source node ID
            sink: Sink node ID

        Returns:
            Tuple of (path_edge_indices, total_cost_ppm)
            Empty path if no augmenting path exists
        """
        nodes = list(self.network.nodes.keys())
        n = len(nodes)
        node_to_idx = {node: i for i, node in enumerate(nodes)}

        # Distance to each node (cost in ppm)
        dist = [INFINITY] * n
        # Predecessor edge for path reconstruction
        pred_edge = [-1] * n

        source_idx = node_to_idx.get(source)
        sink_idx = node_to_idx.get(sink)

        if source_idx is None or sink_idx is None:
            return [], 0

        dist[source_idx] = 0

        # Bellman-Ford relaxation (capped for safety)
        bf_limit = min(n, MAX_BELLMAN_FORD_ITERATIONS)
        for iteration in range(bf_limit):
            updated = False

            for edge_idx, edge in enumerate(self.network.edges):
                if edge.residual_capacity <= 0:
                    continue

                from_idx = node_to_idx.get(edge.from_node)
                to_idx = node_to_idx.get(edge.to_node)

                if from_idx is None or to_idx is None:
                    continue

                if dist[from_idx] == INFINITY:
                    continue

                new_dist = dist[from_idx] + edge.cost_ppm

                if new_dist < dist[to_idx]:
                    dist[to_idx] = new_dist
                    pred_edge[to_idx] = edge_idx
                    updated = True

            if not updated:
                break

            # Detect negative cycle (shouldn't happen with proper setup)
            if iteration == bf_limit - 1 and updated:
                # Negative cycle detected - stop to prevent infinite loop
                self.warnings.append(
                    f"Negative cycle detected in residual network "
                    f"({n} nodes, {len(self.network.edges)} edges)"
                )
                return [], 0

        # Check if sink is reachable
        if dist[sink_idx] == INFINITY:
            return [], 0

        # Initialize Johnson potentials from Bellman-Ford distances
        for i, node_id in enumerate(nodes):
            if dist[i] < INFINITY:
                self._potentials[node_id] = dist[i]

        # Reconstruct path
        path = []
        current_idx = sink_idx

        while current_idx != source_idx:
            edge_idx = pred_edge[current_idx]
            if edge_idx == -1:
                return [], 0  # Path broken
            path.append(edge_idx)

            edge = self.network.edges[edge_idx]
            current_idx = node_to_idx.get(edge.from_node)

            if current_idx is None:
                return [], 0

            # Safety check to prevent infinite loops
            if len(path) > n:
                return [], 0

        path.reverse()
        return path, dist[sink_idx]

    def _find_bottleneck(self, path: List[int]) -> int:
        """
        Find the bottleneck (minimum residual capacity) along a path.

        Args:
            path: List of edge indices

        Returns:
            Minimum residual capacity
        """
        if not path:
            return 0

        return min(
            self.network.edges[edge_idx].residual_capacity
            for edge_idx in path
        )

    def _augment_flow(self, path: List[int], amount: int) -> None:
        """
        Augment flow along a path by the given amount.

        Updates residual capacities and flow values.

        Args:
            path: List of edge indices
            amount: Flow amount to push
        """
        for edge_idx in path:
            edge = self.network.edges[edge_idx]
            reverse_idx = edge.reverse_edge_idx

            # Push flow on forward edge
            edge.residual_capacity -= amount
            edge.flow += amount

            # Update reverse edge (allow flow cancellation)
            if reverse_idx >= 0:
                reverse_edge = self.network.edges[reverse_idx]
                reverse_edge.residual_capacity += amount

    def _dijkstra_shortest_path(
        self,
        source: str,
        sink: str
    ) -> Tuple[List[int], int]:
        """
        Find shortest (min-cost) path using Dijkstra with Johnson potentials.

        Uses reduced costs c'(u,v) = cost(u,v) + h[u] - h[v] which are
        guaranteed non-negative after Bellman-Ford initialization.

        Args:
            source: Source node ID
            sink: Sink node ID

        Returns:
            Tuple of (path_edge_indices, original_total_cost_ppm)
            Empty path if no augmenting path exists
        """
        h = self._potentials
        dist: Dict[str, float] = {}
        pred_edge: Dict[str, int] = {}
        visited: Set[str] = set()

        dist[source] = 0
        pq: List[Tuple[float, str]] = [(0, source)]

        while pq:
            d_u, u = heapq.heappop(pq)
            if u in visited:
                continue
            visited.add(u)
            if u == sink:
                break

            node = self.network.nodes.get(u)
            if not node:
                continue

            h_u = h.get(u, 0)
            for edge_idx in node.outgoing_edges:
                edge = self.network.edges[edge_idx]
                if edge.residual_capacity <= 0:
                    continue

                v = edge.to_node
                if v in visited:
                    continue

                # Reduced cost (clamp to 0 for floating point safety)
                reduced_cost = max(0, edge.cost_ppm + h_u - h.get(v, 0))
                new_dist = d_u + reduced_cost

                if v not in dist or new_dist < dist[v]:
                    dist[v] = new_dist
                    pred_edge[v] = edge_idx
                    heapq.heappush(pq, (new_dist, v))

        if sink not in dist:
            return [], 0

        # Update potentials: h[v] += dist_reduced[v]
        for node_id, d in dist.items():
            h[node_id] = h.get(node_id, 0) + d

        # Reconstruct path and compute original cost
        path: List[int] = []
        current = sink

        while current != source:
            if current not in pred_edge:
                return [], 0
            idx = pred_edge[current]
            path.append(idx)
            current = self.network.edges[idx].from_node

            # Safety check to prevent infinite loops
            if len(path) > len(self.network.nodes):
                return [], 0

        path.reverse()

        # Return original cost (sum of actual edge costs, not reduced)
        original_cost = sum(self.network.edges[i].cost_ppm for i in path)
        return path, original_cost


# =============================================================================
# MCF NETWORK BUILDER
# =============================================================================

class MCFNetworkBuilder:
    """
    Builds MCF network from fleet state and rebalancing needs.

    Transforms the distributed hive topology and liquidity needs
    into a flow network suitable for the SSP solver.
    """

    def __init__(self, plugin=None):
        """
        Initialize network builder.

        Args:
            plugin: Plugin reference for logging
        """
        self.plugin = plugin

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"MCF_BUILDER: {message}", level=level)

    def build_from_fleet_state(
        self,
        state_manager,
        needs: List[RebalanceNeed],
        our_pubkey: str,
        our_channels: List[Dict[str, Any]] = None
    ) -> MCFNetwork:
        """
        Build MCF network from fleet state and rebalancing needs.

        Args:
            state_manager: StateManager with fleet topology
            needs: List of RebalanceNeed from all fleet members
            our_pubkey: Our node's pubkey
            our_channels: Our channel list from listpeerchannels

        Returns:
            MCFNetwork ready for solving
        """
        network = MCFNetwork()

        # Get all fleet member states
        all_states = state_manager.get_all_peer_states() if state_manager else []
        member_ids = {s.peer_id for s in all_states}
        member_ids.add(our_pubkey)  # Include ourselves

        # Add fleet members as nodes
        for member_id in member_ids:
            network.add_node(member_id, supply=0, is_fleet_member=True)

        # Add supply/demand from needs
        for need in needs:
            if need.need_type == "outbound":
                # Needs outbound = has excess local = source
                network.add_node(need.member_id, supply=need.amount_sats)
            elif need.need_type == "inbound":
                # Needs inbound = has excess remote = sink
                network.add_node(need.member_id, supply=-need.amount_sats)

        # Add edges from our channels first (precise data takes priority)
        channel_edge_pairs: Set[Tuple[str, str]] = set()
        if our_channels:
            channel_edge_pairs = self._add_edges_from_channels(
                network, our_pubkey, our_channels, member_ids
            )

        # Add inferred edges from fleet topology, skipping pairs with precise data
        self._add_edges_from_topology(network, all_states, member_ids, channel_edge_pairs)

        # Setup super-source and super-sink
        network.setup_super_source_sink()

        self._log(
            f"Built MCF network: {network.get_node_count()} nodes, "
            f"{network.get_edge_count()} edges"
        )

        return network

    def _add_edges_from_topology(
        self,
        network: MCFNetwork,
        all_states: List,
        member_ids: Set[str],
        skip_pairs: Set[Tuple[str, str]] = None
    ) -> None:
        """
        Add edges between fleet members based on gossip state.

        Since gossip provides each member's available_sats (hive outbound
        liquidity) but not per-channel breakdown, we infer connectivity
        by distributing available_sats across edges to all other known
        hive members (conservative full-mesh assumption).

        Pairs already covered by precise channel data (skip_pairs) are excluded
        to prevent duplicate edges that would overstate capacity.
        """
        MAX_ESTIMATED_EDGE_CAPACITY = 16_777_215  # standard channel cap
        if skip_pairs is None:
            skip_pairs = set()
        state_by_id = {s.peer_id: s for s in all_states}
        member_list = sorted(member_ids)

        for from_node in member_list:
            state = state_by_id.get(from_node)
            if not state:
                continue
            available = getattr(state, 'available_sats', 0) or 0
            if available <= 0:
                continue
            other_members = [m for m in member_list if m != from_node]
            if not other_members:
                continue
            per_edge = min(available // len(other_members), MAX_ESTIMATED_EDGE_CAPACITY)
            if per_edge <= 0:
                continue
            for to_node in other_members:
                if (from_node, to_node) in skip_pairs:
                    continue
                network.add_edge(
                    from_node=from_node,
                    to_node=to_node,
                    capacity=per_edge,
                    cost_ppm=HIVE_INTERNAL_COST_PPM,
                    is_hive_internal=True
                )

    def _add_edges_from_channels(
        self,
        network: MCFNetwork,
        our_pubkey: str,
        channels: List[Dict[str, Any]],
        member_ids: Set[str]
    ) -> Set[Tuple[str, str]]:
        """
        Add edges from our channel data.

        Returns:
            Set of (from_node, to_node) pairs that were added, so the
            topology builder can skip them to avoid duplicate edges.
        """
        added_pairs: Set[Tuple[str, str]] = set()
        for ch in channels:
            if ch.get("state") != "CHANNELD_NORMAL":
                continue

            peer_id = ch.get("peer_id")
            if not peer_id:
                continue

            channel_id = ch.get("short_channel_id", "")

            # Get capacities
            total_msat = ch.get("total_msat", 0)
            if isinstance(total_msat, str):
                total_msat = int(total_msat.replace("msat", ""))

            local_msat = ch.get("to_us_msat", 0)
            if isinstance(local_msat, str):
                local_msat = int(local_msat.replace("msat", ""))

            remote_msat = total_msat - local_msat

            local_sats = local_msat // 1000
            remote_sats = remote_msat // 1000

            is_hive_internal = peer_id in member_ids

            # Cost: zero for hive, estimate for external
            cost_ppm = HIVE_INTERNAL_COST_PPM if is_hive_internal else DEFAULT_EXTERNAL_COST_PPM

            # Edge from us to peer (outbound capacity = local balance)
            if local_sats > 0:
                network.add_edge(
                    from_node=our_pubkey,
                    to_node=peer_id,
                    capacity=local_sats,
                    cost_ppm=cost_ppm,
                    channel_id=channel_id,
                    is_hive_internal=is_hive_internal
                )
                added_pairs.add((our_pubkey, peer_id))

            # Edge from peer to us (inbound capacity = remote balance)
            if remote_sats > 0:
                network.add_edge(
                    from_node=peer_id,
                    to_node=our_pubkey,
                    capacity=remote_sats,
                    cost_ppm=cost_ppm,
                    channel_id=channel_id,
                    is_hive_internal=is_hive_internal
                )
                added_pairs.add((peer_id, our_pubkey))

        return added_pairs


# =============================================================================
# MCF COORDINATOR
# =============================================================================

class MCFCoordinator:
    """
    Coordinates MCF optimization across the fleet.

    Responsibilities:
    - Collect needs from all fleet members
    - Elect coordinator (lexicographic lowest pubkey)
    - Run optimization cycles
    - Distribute assignments
    - Track completion
    """

    def __init__(
        self,
        plugin,
        database,
        state_manager,
        liquidity_coordinator,
        our_pubkey: str
    ):
        """
        Initialize MCF coordinator.

        Args:
            plugin: Plugin reference for RPC/logging
            database: HiveDatabase for persistence
            state_manager: StateManager for fleet topology
            liquidity_coordinator: LiquidityCoordinator for needs
            our_pubkey: Our node's pubkey
        """
        self.plugin = plugin
        self.database = database
        self.state_manager = state_manager
        self.liquidity_coordinator = liquidity_coordinator
        self.our_pubkey = our_pubkey

        # Builder and solution cache
        self._builder = MCFNetworkBuilder(plugin)
        self._solution_lock = threading.Lock()
        self._last_solution: Optional[MCFSolution] = None
        self._last_solution_time: float = 0

        # Pending assignments for us
        self._our_assignments: List[RebalanceAssignment] = []

        # Election cache
        self._cached_coordinator: Optional[str] = None
        self._election_cache_time: float = 0
        self._election_cache_ttl: float = 60  # seconds

        # Completion tracking
        self._completed_assignments: Dict[str, Dict[str, Any]] = {}

        # Circuit breaker and health metrics (Phase 5)
        self._circuit_breaker = MCFCircuitBreaker()
        self._health_metrics = MCFHealthMetrics()

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"MCF_COORD: {message}", level=level)

    def elect_coordinator(self) -> str:
        """
        Elect coordinator using lexicographic lowest pubkey among MCF-capable members.

        Only considers members that:
        1. Have advertised MCF capability via gossip
        2. Have fresh gossip (not stale/offline)

        Falls back to our own node if no MCF-capable members found.

        Returns:
            Pubkey of elected coordinator (MCF-capable member with lowest pubkey)
        """
        members = self.database.get_all_members() if self.database else []
        member_ids = [m.get("peer_id") for m in members if m.get("peer_id")]

        if not member_ids:
            return self.our_pubkey

        # Include ourselves (we always have MCF capability since we're running this code)
        if self.our_pubkey not in member_ids:
            member_ids.append(self.our_pubkey)

        # Filter to only MCF-capable members with fresh gossip
        now = time.time()
        mcf_capable_ids = []
        stale_count = 0

        for member_id in member_ids:
            if member_id == self.our_pubkey:
                # We have MCF capability (running this code proves it)
                mcf_capable_ids.append(member_id)
            elif self.state_manager:
                # Check peer's advertised capabilities from gossip
                peer_state = self.state_manager.get_peer_state(member_id)
                if peer_state:
                    # Check if gossip is fresh enough (not stale/offline)
                    last_update = getattr(peer_state, 'last_update', 0)
                    state_age = now - last_update
                    if state_age > MAX_GOSSIP_AGE_FOR_MCF:
                        # Peer gossip is stale - likely offline, exclude from election
                        stale_count += 1
                        continue

                    capabilities = getattr(peer_state, 'capabilities', [])
                    if 'mcf' in capabilities:
                        mcf_capable_ids.append(member_id)
                # If no peer state, member hasn't gossiped recently - exclude from election
            # If no state_manager, can't check capabilities - exclude

        if not mcf_capable_ids:
            # No MCF-capable members found, fall back to ourselves
            self._log("No MCF-capable members found, using self as coordinator", level="debug")
            return self.our_pubkey

        # Lexicographic lowest among MCF-capable members wins
        elected = min(mcf_capable_ids)

        if len(mcf_capable_ids) < len(member_ids) or stale_count > 0:
            self._log(
                f"MCF coordinator election: {len(mcf_capable_ids)}/{len(member_ids)} "
                f"members are MCF-capable ({stale_count} stale), elected {elected[:16]}...",
                level="debug"
            )

        return elected

    def is_coordinator(self) -> bool:
        """Check if we are the elected coordinator (uses cached result)."""
        now = time.time()
        with self._solution_lock:
            if (self._cached_coordinator is not None
                    and (now - self._election_cache_time) < self._election_cache_ttl):
                return self._cached_coordinator == self.our_pubkey
        result = self.elect_coordinator()
        with self._solution_lock:
            self._cached_coordinator = result
            self._election_cache_time = now
        return result == self.our_pubkey

    def invalidate_election_cache(self) -> None:
        """Invalidate the coordinator election cache (e.g. on membership change)."""
        with self._solution_lock:
            self._cached_coordinator = None
            self._election_cache_time = 0

    def collect_fleet_needs(self) -> List[RebalanceNeed]:
        """
        Collect rebalancing needs from all fleet members.

        Returns:
            List of RebalanceNeed from fleet
        """
        needs = []

        if not self.liquidity_coordinator:
            return needs

        # Get needs from liquidity coordinator
        liq_needs = self.liquidity_coordinator.get_prioritized_needs()

        for need in liq_needs:
            needs.append(RebalanceNeed(
                member_id=need.reporter_id,
                need_type=need.need_type,
                target_peer=need.target_peer_id,
                amount_sats=need.amount_sats,
                urgency=need.urgency,
                max_fee_ppm=need.max_fee_ppm,
            ))

        return needs

    def get_total_demand(self, needs: List[RebalanceNeed]) -> int:
        """Get total demand (inbound + outbound needs) in sats."""
        return sum(n.amount_sats for n in needs)

    def run_optimization_cycle(self) -> Optional[MCFSolution]:
        """
        Run a full MCF optimization cycle.

        Only runs if we are the coordinator and circuit breaker allows.

        Returns:
            MCFSolution if successful, None otherwise
        """
        if not self.is_coordinator():
            self._log("Not coordinator, skipping optimization")
            return None

        # Check circuit breaker
        if not self._circuit_breaker.can_execute():
            self._log(
                f"Circuit breaker OPEN, skipping MCF "
                f"(state={self._circuit_breaker.state}, failures={self._circuit_breaker.failure_count})",
                level="warn"
            )
            return None

        needs = self.collect_fleet_needs()
        total_demand = self.get_total_demand(needs)

        if total_demand < MIN_MCF_DEMAND:
            self._log(f"Total demand {total_demand} < {MIN_MCF_DEMAND}, skipping")
            self._health_metrics.record_stale_cycle()
            return None

        self._log(f"Running MCF optimization with {len(needs)} needs, {total_demand} sats demand")

        start_time = time.time()

        try:
            # Build network
            our_channels = []
            if self.plugin and self.plugin.rpc:
                try:
                    result = self.plugin.rpc.listpeerchannels()
                    our_channels = result.get("channels", [])
                except Exception as e:
                    self._log(f"Failed to get channels: {e}", level="warn")
                    self._circuit_breaker.record_failure(f"listpeerchannels: {e}")
                    return None

            network = self._builder.build_from_fleet_state(
                self.state_manager,
                needs,
                self.our_pubkey,
                our_channels
            )

            # Check for stale/insufficient network
            if len(network.nodes) < 2:
                self._log("Insufficient network topology for MCF", level="warn")
                self._health_metrics.record_stale_cycle()
                return None

            # Solve
            solver = SSPSolver(network)
            total_flow, total_cost, edge_flows = solver.solve()

            # Log any solver warnings
            for warning in solver.warnings:
                self._log(f"Solver warning: {warning}", level="warn")

            computation_time = int((time.time() - start_time) * 1000)

            # Extract assignments
            assignments = self._extract_assignments(network, edge_flows, needs)

            # Calculate unmet demand
            unmet_demand = total_demand - total_flow

            solution = MCFSolution(
                assignments=assignments,
                total_flow_sats=total_flow,
                total_cost_sats=total_cost,
                unmet_demand_sats=max(0, unmet_demand),
                computation_time_ms=computation_time,
                iterations=solver.iterations,
                timestamp=int(time.time()),
                coordinator_id=self.our_pubkey,
            )

            with self._solution_lock:
                self._last_solution = solution
                self._last_solution_time = time.time()

            # Record success to circuit breaker and metrics
            self._circuit_breaker.record_success()
            self._health_metrics.record_solution(
                flow_sats=total_flow,
                cost_sats=total_cost,
                assignments=len(assignments),
                computation_time_ms=computation_time,
                node_count=len(network.nodes),
                edge_count=len(network.edges)
            )

            self._log(
                f"MCF solution: flow={total_flow} sats, cost={total_cost} sats, "
                f"assignments={len(assignments)}, iterations={solver.iterations}, "
                f"time={computation_time}ms"
            )

            return solution

        except Exception as e:
            self._log(f"MCF optimization failed: {e}", level="warn")
            self._circuit_breaker.record_failure(str(e))
            return None

    def _extract_assignments(
        self,
        network: MCFNetwork,
        edge_flows: List[Tuple[int, int]],
        needs: List[RebalanceNeed]
    ) -> List[RebalanceAssignment]:
        """
        Extract rebalance assignments from solved network.

        Analyzes flow on edges to determine what each member should do.
        """
        assignments = []
        priority = 0

        # Group flows by member
        member_flows: Dict[str, List[Tuple[MCFEdge, int]]] = defaultdict(list)

        for edge_idx, flow in edge_flows:
            edge = network.edges[edge_idx]

            # Skip super-source/sink edges
            if edge.from_node in (network.super_source, network.super_sink):
                continue
            if edge.to_node in (network.super_source, network.super_sink):
                continue

            # Skip reverse edges (negative or zero-cost reverse edges)
            if edge.cost_ppm < 0 or edge.is_reverse:
                continue

            # Determine which member executes this
            node = network.nodes.get(edge.from_node)
            if node and node.is_fleet_member:
                member_flows[edge.from_node].append((edge, flow))

        # Create assignments from flows
        for member_id, flows in member_flows.items():
            for edge, flow in flows:
                if flow <= 0:
                    continue

                expected_cost = edge.unit_cost(flow)

                assignments.append(RebalanceAssignment(
                    member_id=member_id,
                    from_channel=edge.channel_id,
                    to_channel="",  # Filled during execution
                    amount_sats=flow,
                    expected_cost_sats=expected_cost,
                    path=[edge.from_node, edge.to_node],
                    priority=priority,
                    via_fleet=edge.is_hive_internal,
                ))

                priority += 1

        return assignments

    def get_our_assignments(self) -> List[RebalanceAssignment]:
        """Get assignments for our node from the latest solution."""
        with self._solution_lock:
            return self._get_our_assignments_unlocked()

    def _get_our_assignments_unlocked(self) -> List[RebalanceAssignment]:
        """Get assignments without acquiring lock. Caller must hold _solution_lock."""
        if not self._last_solution:
            return []

        return [
            a for a in self._last_solution.assignments
            if a.member_id == self.our_pubkey
        ]

    def get_status(self) -> Dict[str, Any]:
        """Get MCF coordinator status including circuit breaker and health."""
        is_coord = self.is_coordinator()  # populates _cached_coordinator
        coordinator_id = self._cached_coordinator or self.elect_coordinator()

        with self._solution_lock:
            solution_age = 0
            if self._last_solution:
                solution_age = int(time.time() - self._last_solution_time)

            our_assignments = self._get_our_assignments_unlocked()

            return {
                "enabled": True,
                "is_coordinator": is_coord,
                "coordinator_id": coordinator_id[:16] + "..." if coordinator_id else None,
                "last_solution": self._last_solution.to_dict() if self._last_solution else None,
                "solution_age_seconds": solution_age,
                "solution_valid": self._last_solution is not None and solution_age < MAX_SOLUTION_AGE,
                "our_assignments": [a.to_dict() for a in our_assignments],
                "pending_count": len(our_assignments),
                # Phase 5: Circuit breaker and health metrics
                "circuit_breaker": self._circuit_breaker.get_status(),
                "health_metrics": self._health_metrics.to_dict(),
            }

    def get_health_summary(self) -> Dict[str, Any]:
        """
        Get a brief health summary for monitoring.

        Returns:
            Dict with key health indicators
        """
        age, staleness = self._health_metrics.get_solution_staleness()
        return {
            "healthy": self._health_metrics.is_healthy() and self._circuit_breaker.state == MCFCircuitBreaker.CLOSED,
            "circuit_state": self._circuit_breaker.state,
            "solution_staleness": staleness,
            "solution_age_seconds": age,
            "consecutive_stale_cycles": self._health_metrics.consecutive_stale_cycles,
            "can_execute": self._circuit_breaker.can_execute(),
        }

    def reset_circuit_breaker(self) -> None:
        """Reset circuit breaker to closed state."""
        self._circuit_breaker.reset()
        self._log("Circuit breaker reset to CLOSED", level="info")

    def record_assignment_completion(
        self,
        assignment_id: str,
        success: bool,
        amount_sats: int = 0,
        cost_sats: int = 0
    ) -> None:
        """
        Record completion of an MCF assignment.

        Updates health metrics for monitoring.

        Args:
            assignment_id: Assignment that was completed
            success: Whether it succeeded
            amount_sats: Actual amount transferred
            cost_sats: Actual cost paid
        """
        self._health_metrics.record_assignment_completion(success, amount_sats, cost_sats)
        self._completed_assignments[assignment_id] = {
            "success": success,
            "amount_sats": amount_sats,
            "cost_sats": cost_sats,
            "completed_at": int(time.time()),
        }

        # Keep completions cache bounded
        if len(self._completed_assignments) > 100:
            # Remove oldest
            oldest_key = min(
                self._completed_assignments.keys(),
                key=lambda k: self._completed_assignments[k].get("completed_at", 0)
            )
            del self._completed_assignments[oldest_key]

    def receive_solution(self, solution_data: Dict[str, Any]) -> bool:
        """
        Receive and validate a solution from the coordinator.

        Args:
            solution_data: Serialized MCFSolution

        Returns:
            True if accepted
        """
        # Parse solution
        assignments = []
        for a_data in solution_data.get("assignments", []):
            assignments.append(RebalanceAssignment(
                member_id=a_data.get("member_id", ""),
                from_channel=a_data.get("from_channel", ""),
                to_channel=a_data.get("to_channel", ""),
                amount_sats=a_data.get("amount_sats", 0),
                expected_cost_sats=a_data.get("expected_cost_sats", 0),
                path=a_data.get("path", []),
                priority=a_data.get("priority", 0),
                via_fleet=a_data.get("via_fleet", True),
            ))

        solution = MCFSolution(
            assignments=assignments,
            total_flow_sats=solution_data.get("total_flow_sats", 0),
            total_cost_sats=solution_data.get("total_cost_sats", 0),
            unmet_demand_sats=solution_data.get("unmet_demand_sats", 0),
            computation_time_ms=solution_data.get("computation_time_ms", 0),
            iterations=solution_data.get("iterations", 0),
            timestamp=solution_data.get("timestamp", 0),
            coordinator_id=solution_data.get("coordinator_id", ""),
        )

        # Validate timestamp freshness
        now = int(time.time())
        if solution.timestamp > 0 and abs(now - solution.timestamp) > MAX_SOLUTION_AGE:
            self._log(
                f"Solution timestamp too old or too far in future: "
                f"age={now - solution.timestamp}s, max={MAX_SOLUTION_AGE}s",
                level="warn"
            )
            return False

        # Validate coordinator
        expected_coordinator = self.elect_coordinator()
        if solution.coordinator_id != expected_coordinator:
            self._log(
                f"Solution from wrong coordinator: {solution.coordinator_id[:16]}... "
                f"expected {expected_coordinator[:16]}...",
                level="warn"
            )
            return False

        # Validate assignment amounts (L-11: prevent data poisoning)
        for a in assignments:
            if a.amount_sats <= 0 or a.amount_sats > MAX_ASSIGNMENT_AMOUNT_SATS:
                self._log(
                    f"Rejecting solution: assignment amount {a.amount_sats} sats "
                    f"out of bounds (0, {MAX_ASSIGNMENT_AMOUNT_SATS}]",
                    level="warn"
                )
                return False
        if solution.total_flow_sats > MAX_TOTAL_SOLUTION_SATS:
            self._log(
                f"Rejecting solution: total flow {solution.total_flow_sats} sats "
                f"exceeds max {MAX_TOTAL_SOLUTION_SATS}",
                level="warn"
            )
            return False

        # Accept solution
        with self._solution_lock:
            self._last_solution = solution
            self._last_solution_time = time.time()

        self._log(f"Accepted MCF solution with {len(assignments)} assignments")
        return True
