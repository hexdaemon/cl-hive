"""
Routing Pool Module (Phase 0 - Collective Economics)

Implements collective profit sharing for the hive:
- All routing revenue goes to a shared pool
- Distribution proportional to member contributions
- Contributions weighted by capital (70%), position (20%), operations (10%)

This is the economic foundation that enables all other coordination:
- Eliminates internal competition (your peer's success = your success)
- Aligns incentives for fee coordination, positioning, etc.
- Mirrors mining pools but for Lightning routing

Author: Lightning Goats Team
"""

import time
import datetime
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from . import network_metrics


# =============================================================================
# CONSTANTS
# =============================================================================

# Contribution weights (must sum to 1.0)
CAPITAL_WEIGHT = 0.70      # Capacity, weighted capacity, uptime
POSITION_WEIGHT = 0.20     # Centrality, unique peers, bridge score
OPERATIONS_WEIGHT = 0.10   # Success rate, response time

# Settlement period
DEFAULT_SETTLEMENT_DAYS = 7

# Minimum contribution to receive distribution
MIN_CONTRIBUTION_THRESHOLD = 0.001  # 0.1% of pool

# Position scoring normalization
MAX_CENTRALITY = 0.1       # Normalize centrality scores (typical max ~0.1)
MAX_UNIQUE_PEERS = 50      # Normalize unique peer count
MAX_BRIDGE_SCORE = 1.0     # Already normalized

# Operations scoring
TARGET_SUCCESS_RATE = 0.95  # Target for full score
TARGET_RESPONSE_MS = 100    # Target for full score (ms)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class MemberContribution:
    """Contribution metrics for a pool member."""
    member_id: str
    period: str

    # Capital metrics
    total_capacity_sats: int
    weighted_capacity_sats: int
    uptime_pct: float

    # Position metrics
    betweenness_centrality: float
    unique_peers: int
    bridge_score: float

    # Operations metrics
    routing_success_rate: float
    avg_response_time_ms: float

    # Computed
    capital_score: float = 0.0
    position_score: float = 0.0
    operations_score: float = 0.0
    pool_share: float = 0.0


@dataclass
class PoolDistribution:
    """Distribution record for a member."""
    member_id: str
    period: str
    contribution_share: float
    revenue_share_sats: int
    total_pool_revenue_sats: int


@dataclass
class PoolStatus:
    """Current pool status."""
    period: str
    total_revenue_sats: int
    member_count: int
    total_capacity_sats: int
    contributions: List[Dict[str, Any]]
    projected_distribution: Dict[str, int]


# =============================================================================
# ROUTING POOL CLASS
# =============================================================================

class RoutingPool:
    """
    Collective profit sharing for the hive.

    All routing revenue goes to the pool, distributed proportionally
    based on capital, position, and operational contributions.

    This class coordinates:
    - Revenue recording (from forward events)
    - Contribution snapshots (periodic assessment)
    - Distribution calculation (settlement)
    - Pool status reporting (for MCP/display)
    """

    def __init__(
        self,
        database,
        plugin,
        state_manager=None,
        health_aggregator=None,
        metrics_calculator=None
    ):
        """
        Initialize the routing pool.

        Args:
            database: HiveDatabase instance for persistence
            plugin: Plugin instance for RPC/logging
            state_manager: StateManager for member state (optional)
            health_aggregator: HealthScoreAggregator for health data (optional)
            metrics_calculator: NetworkMetricsCalculator for position metrics (optional)
        """
        self.db = database
        self.plugin = plugin
        self.state_manager = state_manager
        self.health_aggregator = health_aggregator
        self.metrics_calculator = metrics_calculator

        # Our pubkey (set later)
        self.our_pubkey: Optional[str] = None

    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[RoutingPool] {msg}", level=level)

    # =========================================================================
    # REVENUE RECORDING
    # =========================================================================

    def record_revenue(
        self,
        member_id: str,
        amount_sats: int,
        channel_id: str = None,
        payment_hash: str = None
    ) -> bool:
        """
        Record routing revenue for the pool.

        Called when any fleet member earns routing fees.
        Revenue goes to collective pool, not individual.

        Args:
            member_id: Pubkey of member who routed
            amount_sats: Fee revenue in satoshis
            channel_id: Channel that earned the fee
            payment_hash: Payment hash for deduplication

        Returns:
            True if recorded successfully
        """
        if amount_sats <= 0:
            return False

        try:
            self.db.record_pool_revenue(
                member_id=member_id,
                amount_sats=amount_sats,
                channel_id=channel_id,
                payment_hash=payment_hash
            )
            self._log(
                f"Recorded revenue: {amount_sats} sats from {member_id[:12]}...",
                level='debug'
            )
            return True
        except Exception as e:
            self._log(f"Error recording revenue: {e}", level='error')
            return False

    def get_period_revenue(self, period: str = None) -> Dict[str, Any]:
        """
        Get revenue statistics for a period.

        Args:
            period: Period string (default: current week)

        Returns:
            Revenue stats including total, by_member breakdown
        """
        if period is None:
            period = self._current_period()

        return self.db.get_pool_revenue(period=period)

    # =========================================================================
    # CONTRIBUTION CALCULATION
    # =========================================================================

    def calculate_contribution(
        self,
        member_id: str,
        period: str,
        capacity_sats: int,
        uptime_pct: float,
        centrality: float = 0.0,
        unique_peers: int = 0,
        bridge_score: float = 0.0,
        success_rate: float = 1.0,
        response_time_ms: float = 50.0
    ) -> MemberContribution:
        """
        Calculate a member's contribution scores.

        Args:
            member_id: Member pubkey
            period: Period string
            capacity_sats: Total channel capacity
            uptime_pct: Uptime percentage (0-1)
            centrality: Betweenness centrality (0-1)
            unique_peers: Peers only this member connects to
            bridge_score: Score for connecting clusters (0-1)
            success_rate: HTLC success rate (0-1)
            response_time_ms: Average forwarding time in ms

        Returns:
            MemberContribution with computed scores
        """
        # Capital score (70% weight)
        # - Higher capacity = higher score
        # - Weighted by uptime (offline capacity doesn't help)
        weighted_capacity = int(capacity_sats * uptime_pct)
        capital_score = weighted_capacity  # Actual weighted capacity, used in pool share calc

        # Position score (20% weight)
        # - Higher centrality = more important position
        # - More unique peers = more network coverage
        # - Higher bridge score = connects clusters
        centrality_norm = min(1.0, centrality / MAX_CENTRALITY)
        unique_peers_norm = min(1.0, unique_peers / MAX_UNIQUE_PEERS)
        bridge_norm = min(1.0, bridge_score / MAX_BRIDGE_SCORE)

        position_score = (
            centrality_norm * 0.4 +
            unique_peers_norm * 0.3 +
            bridge_norm * 0.3
        )

        # Operations score (10% weight)
        # - Higher success rate = more reliable
        # - Lower response time = faster routing
        success_score = min(1.0, success_rate / TARGET_SUCCESS_RATE)
        response_score = max(0.0, 1.0 - (response_time_ms / (TARGET_RESPONSE_MS * 10)))

        operations_score = (
            success_score * 0.7 +
            response_score * 0.3
        )

        # Combined score (will be normalized to pool_share later)
        # This is raw score, pool_share is relative to other members

        return MemberContribution(
            member_id=member_id,
            period=period,
            total_capacity_sats=capacity_sats,
            weighted_capacity_sats=weighted_capacity,
            uptime_pct=uptime_pct,
            betweenness_centrality=centrality,
            unique_peers=unique_peers,
            bridge_score=bridge_score,
            routing_success_rate=success_rate,
            avg_response_time_ms=response_time_ms,
            capital_score=capital_score,
            position_score=position_score,
            operations_score=operations_score,
            pool_share=0.0  # Calculated after all members assessed
        )

    def snapshot_contributions(self, period: str = None) -> List[MemberContribution]:
        """
        Snapshot all member contributions for a period.

        Queries state from state_manager and health_aggregator,
        calculates scores, and stores in database.

        Args:
            period: Period string (default: current week)

        Returns:
            List of MemberContribution for all members
        """
        if period is None:
            period = self._current_period()

        contributions = []
        total_weighted_capacity = 0

        # Get all members
        members = self.db.get_all_members()
        if not members:
            self._log("No members found for contribution snapshot")
            return []

        # First pass: calculate raw contributions
        for member in members:
            member_id = member['peer_id']

            # Get capacity and uptime
            capacity = self._get_member_capacity(member_id)
            uptime = member.get('uptime_pct', 1.0)

            # Get position metrics (from state_manager if available)
            centrality, unique_peers, bridge_score = self._get_position_metrics(member_id)

            # Get operations metrics
            success_rate, response_time = self._get_operations_metrics(member_id)

            contrib = self.calculate_contribution(
                member_id=member_id,
                period=period,
                capacity_sats=capacity,
                uptime_pct=uptime,
                centrality=centrality,
                unique_peers=unique_peers,
                bridge_score=bridge_score,
                success_rate=success_rate,
                response_time_ms=response_time
            )

            contributions.append(contrib)
            total_weighted_capacity += contrib.weighted_capacity_sats

        # Second pass: calculate pool shares
        total_raw_score = sum(
            c.weighted_capacity_sats * CAPITAL_WEIGHT +
            c.position_score * POSITION_WEIGHT +
            c.operations_score * OPERATIONS_WEIGHT
            for c in contributions
        )

        if total_raw_score == 0:
            self._log("Total raw score is 0, cannot calculate shares")
            return contributions

        for contrib in contributions:
            raw_score = (
                (contrib.weighted_capacity_sats / max(1, total_weighted_capacity)) * CAPITAL_WEIGHT +
                contrib.position_score * POSITION_WEIGHT +
                contrib.operations_score * OPERATIONS_WEIGHT
            )
            contrib.pool_share = raw_score / (CAPITAL_WEIGHT + POSITION_WEIGHT + OPERATIONS_WEIGHT)

        # Normalize shares to sum to 1.0
        total_shares = sum(c.pool_share for c in contributions)
        if total_shares > 0:
            for contrib in contributions:
                contrib.pool_share /= total_shares

        # Store in database
        for contrib in contributions:
            self.db.record_pool_contribution(
                member_id=contrib.member_id,
                period=period,
                total_capacity_sats=contrib.total_capacity_sats,
                weighted_capacity_sats=contrib.weighted_capacity_sats,
                uptime_pct=contrib.uptime_pct,
                betweenness_centrality=contrib.betweenness_centrality,
                unique_peers=contrib.unique_peers,
                bridge_score=contrib.bridge_score,
                routing_success_rate=contrib.routing_success_rate,
                avg_response_time_ms=contrib.avg_response_time_ms,
                pool_share=contrib.pool_share
            )

        self._log(
            f"Snapshot complete for {period}: {len(contributions)} members, "
            f"total capacity {total_weighted_capacity:,} sats"
        )

        return contributions

    # =========================================================================
    # DISTRIBUTION CALCULATION
    # =========================================================================

    def calculate_distribution(self, period: str = None) -> Dict[str, int]:
        """
        Calculate distribution amounts for a period.

        Args:
            period: Period string (default: current week)

        Returns:
            Dict mapping member_id to distribution amount in sats
        """
        if period is None:
            period = self._current_period()

        # Get revenue for period
        revenue = self.db.get_pool_revenue(period=period)
        total_revenue = revenue.get('total_sats', 0)

        if total_revenue == 0:
            self._log(f"No revenue for period {period}")
            return {}

        # Get contributions for period (read-only — snapshot must be triggered separately)
        contributions = self.db.get_pool_contributions(period)
        if not contributions:
            self._log(f"No contributions recorded for {period}")
            return {}

        # Calculate total shares
        total_shares = sum(c['pool_share'] for c in contributions)
        if total_shares == 0:
            self._log("Total shares is 0")
            return {}

        # Calculate distributions
        distributions = {}
        distributed_total = 0

        for contrib in contributions:
            share_pct = contrib['pool_share'] / total_shares

            # Skip if below minimum threshold
            if share_pct < MIN_CONTRIBUTION_THRESHOLD:
                continue

            amount = int(total_revenue * share_pct)
            distributions[contrib['member_id']] = amount
            distributed_total += amount

        # Handle rounding remainder (give to largest contributor)
        remainder = total_revenue - distributed_total
        if remainder > 0 and distributions:
            largest = max(distributions.keys(), key=lambda k: distributions[k])
            distributions[largest] += remainder

        self._log(
            f"Distribution for {period}: {total_revenue:,} sats to "
            f"{len(distributions)} members"
        )

        return distributions

    def settle_period(self, period: str = None) -> List[PoolDistribution]:
        """
        Settle distributions for a period.

        Records distribution records in database.

        Args:
            period: Period string (default: previous week)

        Returns:
            List of PoolDistribution records
        """
        if period is None:
            # Settle previous period, not current
            period = self._previous_period()

        distributions = self.calculate_distribution(period)
        if not distributions:
            return []

        # Get total revenue for records
        revenue = self.db.get_pool_revenue(period=period)
        total_revenue = revenue.get('total_sats', 0)

        # Get contributions for share percentages
        contributions = {
            c['member_id']: c['pool_share']
            for c in self.db.get_pool_contributions(period)
        }

        def _record_all() -> List[PoolDistribution]:
            results: List[PoolDistribution] = []
            for member_id, amount in distributions.items():
                share = contributions.get(member_id, 0)

                ok = self.db.record_pool_distribution(
                    period=period,
                    member_id=member_id,
                    contribution_share=share,
                    revenue_share_sats=amount,
                    total_pool_revenue_sats=total_revenue
                )
                if not ok:
                    raise RuntimeError(f"record_pool_distribution failed for {member_id}")

                results.append(PoolDistribution(
                    member_id=member_id,
                    period=period,
                    contribution_share=share,
                    revenue_share_sats=amount,
                    total_pool_revenue_sats=total_revenue
                ))
            return results

        # Make the settlement atomic when using a real HiveDatabase.
        if hasattr(self.db, "transaction"):
            with self.db.transaction():
                results = _record_all()
        else:
            results = _record_all()

        self._log(f"Settled {len(results)} distributions for {period}")
        return results

    # =========================================================================
    # STATUS AND REPORTING
    # =========================================================================

    def get_pool_status(self, period: str = None) -> Dict[str, Any]:
        """
        Get current pool status for display/MCP.

        Args:
            period: Optional period to query (format: YYYY-WW, defaults to current week)

        Returns:
            Dict with period, revenue, contributions, projections
        """
        if period is None:
            period = self._current_period()

        # Get revenue
        revenue = self.db.get_pool_revenue(period=period)

        # Get contributions (read-only — snapshot must be triggered separately)
        contributions = self.db.get_pool_contributions(period)

        # Calculate projected distribution
        projected = self.calculate_distribution(period)

        # Total capacity
        total_capacity = sum(c.get('total_capacity_sats', 0) for c in contributions)

        return {
            "period": period,
            "total_revenue_sats": revenue.get('total_sats', 0),
            "transaction_count": revenue.get('transaction_count', 0),
            "member_count": len(contributions),
            "total_capacity_sats": total_capacity,
            "contributions": [
                {
                    "member_id": c['member_id'][:16] + "...",
                    "member_id_full": c['member_id'],
                    "capacity_sats": c.get('total_capacity_sats', 0),
                    "weighted_capacity_sats": c.get('weighted_capacity_sats', 0),
                    "uptime_pct": round(c.get('uptime_pct', 0) * 100, 1),
                    "pool_share_pct": round(c.get('pool_share', 0) * 100, 2),
                    "projected_distribution_sats": projected.get(c['member_id'], 0)
                }
                for c in contributions
            ],
            "revenue_by_member": revenue.get('by_member', []),
            "weights": {
                "capital": CAPITAL_WEIGHT,
                "position": POSITION_WEIGHT,
                "operations": OPERATIONS_WEIGHT
            }
        }

    def get_member_status(self, member_id: str) -> Dict[str, Any]:
        """
        Get pool status for a specific member.

        Args:
            member_id: Member pubkey

        Returns:
            Dict with member's contribution and distribution history
        """
        period = self._current_period()

        # Get current contribution
        contributions = self.db.get_pool_contributions(period)
        current = next(
            (c for c in contributions if c['member_id'] == member_id),
            None
        )

        # Get history
        contribution_history = self.db.get_member_contribution_history(member_id)
        distribution_history = self.db.get_member_distribution_history(member_id)

        # Calculate projected distribution
        projected = self.calculate_distribution(period)

        return {
            "member_id": member_id,
            "period": period,
            "current_contribution": current,
            "projected_distribution_sats": projected.get(member_id, 0),
            "contribution_history": contribution_history,
            "distribution_history": distribution_history
        }

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _current_period(self) -> str:
        """Get current ISO week period string (UTC).

        Format: YYYY-WW (e.g., "2026-06") to match SettlementManager.get_period_string().
        """
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        year, week, _ = now.isocalendar()
        return f"{year}-{week:02d}"

    def _previous_period(self) -> str:
        """Get previous ISO week period string (UTC).

        Format: YYYY-WW (e.g., "2026-05") to match SettlementManager.get_previous_period().
        """
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        last_week = now - datetime.timedelta(days=7)
        year, week, _ = last_week.isocalendar()
        return f"{year}-{week:02d}"

    def _get_member_capacity(self, member_id: str) -> int:
        """Get total channel capacity for a member."""
        if self.state_manager:
            state = self.state_manager.get_peer_state(member_id)
            if state:
                return getattr(state, 'capacity_sats', 0) or 0
        return 0

    def _get_position_metrics(self, member_id: str) -> Tuple[float, int, float]:
        """
        Get position metrics for a member.

        Uses the shared NetworkMetricsCalculator if available for cached,
        fleet-wide consistent calculations. Falls back to local calculation
        if calculator not initialized.

        Returns:
            (centrality, unique_peers, bridge_score)
        """
        # Try shared calculator first (preferred - cached and consistent)
        calculator = self.metrics_calculator or network_metrics.get_calculator()
        if calculator:
            metrics = calculator.get_member_metrics(member_id)
            if metrics:
                return (
                    metrics.external_centrality,
                    metrics.unique_peers,
                    metrics.bridge_score
                )

        # Fallback: local calculation if calculator unavailable
        return self._calculate_position_metrics_local(member_id)

    def _calculate_position_metrics_local(self, member_id: str) -> Tuple[float, int, float]:
        """
        Local fallback calculation for position metrics.

        Used when NetworkMetricsCalculator is not available.
        """
        centrality = 0.01  # Default low centrality
        unique_peers = 0
        bridge_score = 0.0

        if not self.state_manager:
            return (centrality, unique_peers, bridge_score)

        # Get this member's topology
        state = self.state_manager.get_peer_state(member_id)
        if not state:
            return (centrality, unique_peers, bridge_score)

        member_topology = set(getattr(state, 'topology', []) or [])
        if not member_topology:
            return (centrality, unique_peers, bridge_score)

        # Collect all other members' topologies to find unique peers
        all_members = self.db.get_all_members() if self.db else []
        other_members_peers = set()
        topology_sizes = []

        for member in all_members:
            other_id = member.get('peer_id')
            if not other_id:
                continue

            other_state = self.state_manager.get_peer_state(other_id)
            if not other_state:
                continue

            other_topology = set(getattr(other_state, 'topology', []) or [])
            topology_sizes.append(len(other_topology))

            if other_id != member_id:
                other_members_peers.update(other_topology)

        # Calculate unique peers (peers only this member connects to)
        unique_peer_set = member_topology - other_members_peers
        unique_peers = len(unique_peer_set)

        # Calculate bridge score (ratio of unique to total connections)
        if len(member_topology) > 0:
            bridge_score = min(1.0, unique_peers / len(member_topology))

        # Approximate betweenness centrality
        if topology_sizes:
            avg_topology_size = sum(topology_sizes) / len(topology_sizes)
            if avg_topology_size > 0:
                relative_connectivity = len(member_topology) / avg_topology_size
                bridge_boost = 1.0 + (bridge_score * 0.5)
                centrality = min(MAX_CENTRALITY, 0.01 * relative_connectivity * bridge_boost)

        return (centrality, unique_peers, bridge_score)

    def _get_operations_metrics(self, member_id: str) -> Tuple[float, float]:
        """
        Get operations metrics for a member.

        Calculates:
        - success_rate: Estimated from contribution ratio and uptime
                       (nodes that forward more with high uptime are reliable)
        - response_time_ms: Estimated from uptime (high uptime = likely responsive)

        Note: Without explicit HTLC success/failure tracking, we approximate
        using available proxy metrics. Future enhancement could add actual
        timing data from forward events.

        Returns:
            (success_rate, response_time_ms)
        """
        # Default values (good baseline)
        success_rate = 0.95
        response_time_ms = 50.0

        # Get member data for uptime
        member = self.db.get_member(member_id) if self.db else None
        uptime_pct = 1.0
        if member:
            uptime_pct = member.get('uptime_pct', 1.0)
            # Handle percentage stored as 0-100 vs 0-1
            if uptime_pct > 1.0:
                uptime_pct = uptime_pct / 100.0

        # Get contribution stats as proxy for routing reliability
        contribution_ratio = 1.0
        total_forwarded = 0
        if self.db:
            try:
                stats = self.db.get_contribution_stats(member_id, window_days=30)
                forwarded = stats.get('forwarded', 0)
                received = stats.get('received', 0)
                total_forwarded = forwarded
                if received > 0:
                    contribution_ratio = forwarded / received
            except Exception:
                pass

        # Estimate success rate:
        # - Base rate from uptime (offline nodes can't succeed)
        # - Boost for good contribution ratio (active routing = working node)
        # - Cap at TARGET_SUCCESS_RATE (0.95)
        base_success = uptime_pct * 0.9  # Uptime contributes 90% of base

        # Contribution bonus: nodes that forward a lot are likely reliable
        # Scale: ratio of 1.0+ is good, higher is better, cap bonus at 10%
        contrib_bonus = min(0.10, (min(contribution_ratio, 2.0) - 0.5) * 0.1)

        success_rate = min(TARGET_SUCCESS_RATE, base_success + contrib_bonus)
        success_rate = max(0.5, success_rate)  # Floor at 50%

        # Estimate response time:
        # - High uptime suggests well-maintained, fast node
        # - Active routing (high forwarded volume) suggests responsive
        # - Scale from 20ms (best) to 200ms (worst) based on metrics
        if uptime_pct >= 0.99:
            # Excellent uptime = likely fast
            response_time_ms = 30.0
        elif uptime_pct >= 0.95:
            # Good uptime
            response_time_ms = 50.0
        elif uptime_pct >= 0.90:
            # Acceptable uptime
            response_time_ms = 80.0
        else:
            # Lower uptime = assume slower
            response_time_ms = 120.0

        # Adjust for routing activity (active nodes are tuned)
        if total_forwarded > 1000000:  # >1M sats forwarded
            response_time_ms = max(20.0, response_time_ms - 20.0)
        elif total_forwarded > 100000:  # >100k sats
            response_time_ms = max(20.0, response_time_ms - 10.0)

        # Check health aggregator for additional signals
        if self.health_aggregator:
            try:
                health = self.health_aggregator.get_member_health(member_id)
                if health:
                    # If health data includes success metrics, use them
                    if 'success_rate' in health:
                        success_rate = health['success_rate']
                    if 'avg_response_ms' in health:
                        response_time_ms = health['avg_response_ms']
            except Exception:
                pass

        return (success_rate, response_time_ms)

    def set_our_pubkey(self, pubkey: str):
        """Set our node's pubkey."""
        self.our_pubkey = pubkey
