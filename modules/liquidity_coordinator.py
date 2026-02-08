"""
Liquidity Coordinator Module

Coordinates INFORMATION SHARING about liquidity state between hive members.
Each node manages its own funds independently - no sats transfer between nodes.

Information shared:
- Which channels are depleted/saturated (liquidity needs)
- Which peers need more capacity
- Rebalancing activity (to avoid route conflicts)

How this helps without fund transfer:
- Fee coordination: Adjust fees to direct public flow toward peers that help struggling members
- Conflict avoidance: Don't compete for same rebalance routes
- Topology planning: Open channels that benefit the fleet

Security: All operations use cryptographic signatures.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from .protocol import (
    HiveMessageType,
    serialize,
    create_liquidity_need,
    create_liquidity_snapshot,
    validate_liquidity_need_payload,
    validate_liquidity_snapshot_payload,
    get_liquidity_need_signing_payload,
    get_liquidity_snapshot_signing_payload,
    LIQUIDITY_NEED_RATE_LIMIT,
    LIQUIDITY_SNAPSHOT_RATE_LIMIT,
    MAX_NEEDS_IN_SNAPSHOT,
    # MCF message functions (Phase 15)
    create_mcf_assignment_ack,
    create_mcf_completion_report,
)


# Urgency levels for liquidity needs
URGENCY_CRITICAL = "critical"
URGENCY_HIGH = "high"
URGENCY_MEDIUM = "medium"
URGENCY_LOW = "low"

# Need types
NEED_INBOUND = "inbound"
NEED_OUTBOUND = "outbound"
NEED_REBALANCE = "rebalance"

# Reasons for liquidity need
REASON_CHANNEL_DEPLETED = "channel_depleted"
REASON_OPPORTUNITY = "opportunity"
REASON_NNLB_ASSIST = "nnlb_assist"

# Limits
MAX_PENDING_NEEDS = 100  # Max liquidity needs to track
MAX_MCF_ASSIGNMENTS = 50  # Max MCF assignments to track
MCF_ASSIGNMENT_TTL = 3600  # 1 hour TTL for assignments


@dataclass
class LiquidityNeed:
    """Tracked liquidity need from a hive member."""
    reporter_id: str
    need_type: str
    target_peer_id: str
    amount_sats: int
    urgency: str
    max_fee_ppm: int
    reason: str
    current_balance_pct: float
    can_provide_inbound: int
    can_provide_outbound: int
    timestamp: int
    signature: str


@dataclass
class MCFAssignment:
    """
    MCF rebalance assignment received from coordinator.

    Represents an action we should execute as part of the fleet-wide
    optimization computed by the MCF solver.
    """
    assignment_id: str          # Unique ID for tracking
    solution_timestamp: int     # Which solution this belongs to
    coordinator_id: str         # Who computed the solution
    from_channel: str           # Source channel SCID
    to_channel: str             # Destination channel SCID
    amount_sats: int            # Amount to rebalance
    expected_cost_sats: int     # Expected routing cost
    path: List[str]             # Routing path (pubkeys)
    priority: int               # Execution order (lower = sooner)
    via_fleet: bool             # True if routed through hive
    received_at: int            # When we received this
    status: str = "pending"     # pending, executing, completed, failed, rejected
    actual_amount_sats: int = 0
    actual_cost_sats: int = 0
    error_message: str = ""
    completed_at: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "assignment_id": self.assignment_id,
            "solution_timestamp": self.solution_timestamp,
            "coordinator_id": self.coordinator_id[:16] + "..." if self.coordinator_id else "",
            "from_channel": self.from_channel,
            "to_channel": self.to_channel,
            "amount_sats": self.amount_sats,
            "expected_cost_sats": self.expected_cost_sats,
            "path": self.path,
            "priority": self.priority,
            "via_fleet": self.via_fleet,
            "status": self.status,
            "actual_amount_sats": self.actual_amount_sats,
            "actual_cost_sats": self.actual_cost_sats,
            "error_message": self.error_message,
        }


class LiquidityCoordinator:
    """
    Coordinates liquidity INFORMATION between hive members.

    Shares information about liquidity state (depleted/saturated channels)
    so nodes can make better independent decisions about fees and rebalancing.

    No fund transfers between nodes - each node manages its own funds.
    """

    def __init__(
        self,
        database: Any,
        plugin: Any,
        our_pubkey: str,
        fee_intel_mgr: Any = None,
        state_manager: Any = None
    ):
        """
        Initialize the liquidity coordinator.

        Args:
            database: HiveDatabase instance
            plugin: Plugin instance for RPC/logging
            our_pubkey: Our node's pubkey
            fee_intel_mgr: FeeIntelligenceManager for health data
            state_manager: StateManager for member topology (Phase 1)
        """
        self.database = database
        self.plugin = plugin
        self.our_pubkey = our_pubkey
        self.fee_intel_mgr = fee_intel_mgr
        self.state_manager = state_manager

        # Lock protecting in-memory state
        self._lock = threading.Lock()

        # In-memory tracking
        self._liquidity_needs: Dict[str, LiquidityNeed] = {}  # reporter_id -> need

        # Liquidity state tracking (information only)
        # Stores latest liquidity reports from cl-revenue-ops instances
        self._member_liquidity_state: Dict[str, Dict[str, Any]] = {}

        # Rate limiting
        self._need_rate: Dict[str, List[float]] = defaultdict(list)
        self._snapshot_rate: Dict[str, List[float]] = defaultdict(list)

        # MCF assignment tracking (Phase 15)
        self._mcf_assignments: Dict[str, MCFAssignment] = {}  # assignment_id -> assignment
        self._last_mcf_solution_timestamp: int = 0
        self._mcf_ack_sent: bool = False  # Track if we've ACKed current solution

        # Remote MCF needs (received from fleet members, coordinator only)
        self._remote_mcf_needs: Dict[str, Dict[str, Any]] = {}  # reporter_id -> need
        self._max_remote_needs = 500  # Bound cache size

    def _check_rate_limit(
        self,
        sender: str,
        rate_tracker: Dict[str, List[float]],
        limit: Tuple[int, int]
    ) -> bool:
        """Check if sender is within rate limit."""
        max_count, period = limit
        now = time.time()

        # Clean old entries for this sender
        rate_tracker[sender] = [
            ts for ts in rate_tracker[sender]
            if now - ts < period
        ]

        # Evict empty/stale keys to prevent unbounded dict growth
        if len(rate_tracker) > 200:
            stale = [k for k, v in rate_tracker.items() if not v]
            for k in stale:
                del rate_tracker[k]

        return len(rate_tracker[sender]) < max_count

    def _record_message(
        self,
        sender: str,
        rate_tracker: Dict[str, List[float]]
    ):
        """Record a message for rate limiting."""
        rate_tracker[sender].append(time.time())

    def create_liquidity_need_message(
        self,
        need_type: str,
        target_peer_id: str,
        amount_sats: int,
        urgency: str,
        max_fee_ppm: int,
        reason: str,
        current_balance_pct: float,
        can_provide_inbound: int,
        can_provide_outbound: int,
        rpc: Any
    ) -> Optional[bytes]:
        """
        Create a signed LIQUIDITY_NEED message.

        Args:
            need_type: Type of need (inbound/outbound/rebalance)
            target_peer_id: External peer involved
            amount_sats: Amount needed
            urgency: Urgency level
            max_fee_ppm: Maximum fee willing to pay
            reason: Why we need this
            current_balance_pct: Current local balance percentage
            can_provide_inbound: Sats of inbound we can provide
            can_provide_outbound: Sats of outbound we can provide
            rpc: RPC interface for signing

        Returns:
            Serialized and signed message bytes, or None on error
        """
        try:
            timestamp = int(time.time())

            # Build payload for signing
            payload = {
                "reporter_id": self.our_pubkey,
                "timestamp": timestamp,
                "need_type": need_type,
                "target_peer_id": target_peer_id,
                "amount_sats": amount_sats,
                "urgency": urgency,
                "max_fee_ppm": max_fee_ppm,
            }

            # Sign the payload
            signing_msg = get_liquidity_need_signing_payload(payload)
            sig_result = rpc.signmessage(signing_msg)
            signature = sig_result['zbase']

            return create_liquidity_need(
                reporter_id=self.our_pubkey,
                timestamp=timestamp,
                signature=signature,
                need_type=need_type,
                target_peer_id=target_peer_id,
                amount_sats=amount_sats,
                urgency=urgency,
                max_fee_ppm=max_fee_ppm,
                reason=reason,
                current_balance_pct=current_balance_pct,
                can_provide_inbound=can_provide_inbound,
                can_provide_outbound=can_provide_outbound,
            )
        except Exception as e:
            if self.plugin:
                self.plugin.log(
                    f"cl-hive: Failed to create liquidity need message: {e}",
                    level='warn'
                )
            return None

    def handle_liquidity_need(
        self,
        peer_id: str,
        payload: Dict[str, Any],
        rpc: Any
    ) -> Dict[str, Any]:
        """
        Handle incoming LIQUIDITY_NEED message.

        Args:
            peer_id: Sender peer ID
            payload: Message payload
            rpc: RPC interface for signature verification

        Returns:
            Result dict with success/error
        """
        # Validate payload structure
        if not validate_liquidity_need_payload(payload):
            return {"error": "invalid payload"}

        reporter_id = payload.get("reporter_id")

        # Identity binding: sender must match reporter (prevent relay attacks)
        if peer_id != reporter_id:
            return {"error": "identity binding failed"}

        # Verify sender is a hive member
        member = self.database.get_member(reporter_id)
        if not member:
            return {"error": "reporter not a member"}

        # Rate limit check
        if not self._check_rate_limit(
            reporter_id,
            self._need_rate,
            LIQUIDITY_NEED_RATE_LIMIT
        ):
            return {"error": "rate limited"}

        # Verify signature
        signature = payload.get("signature")
        if not signature:
            return {"error": "missing signature"}

        signing_message = get_liquidity_need_signing_payload(payload)

        try:
            verify_result = rpc.checkmessage(signing_message, signature)
            if not verify_result.get("verified"):
                return {"error": "signature verification failed"}

            if verify_result.get("pubkey") != reporter_id:
                return {"error": "signature pubkey mismatch"}
        except Exception as e:
            return {"error": f"signature check failed: {e}"}

        # Record rate limit
        self._record_message(reporter_id, self._need_rate)

        # Store the liquidity need
        need = LiquidityNeed(
            reporter_id=reporter_id,
            need_type=payload.get("need_type", NEED_REBALANCE),
            target_peer_id=payload.get("target_peer_id", ""),
            amount_sats=payload.get("amount_sats", 0),
            urgency=payload.get("urgency", URGENCY_LOW),
            max_fee_ppm=payload.get("max_fee_ppm", 0),
            reason=payload.get("reason", ""),
            current_balance_pct=payload.get("current_balance_pct", 0.5),
            can_provide_inbound=payload.get("can_provide_inbound", 0),
            can_provide_outbound=payload.get("can_provide_outbound", 0),
            timestamp=payload.get("timestamp", int(time.time())),
            signature=signature
        )

        # Store in memory using composite key (consistent with batch path)
        key = f"{reporter_id}:{need.target_peer_id}"
        with self._lock:
            self._liquidity_needs[key] = need

        # Prune old needs if over limit
        self._prune_old_needs()

        # Store in database
        self.database.store_liquidity_need(
            reporter_id=need.reporter_id,
            need_type=need.need_type,
            target_peer_id=need.target_peer_id,
            amount_sats=need.amount_sats,
            urgency=need.urgency,
            max_fee_ppm=need.max_fee_ppm,
            reason=need.reason,
            current_balance_pct=need.current_balance_pct,
            timestamp=need.timestamp
        )

        if self.plugin:
            self.plugin.log(
                f"cl-hive: Received liquidity need from {reporter_id[:16]}...: "
                f"{need.need_type} {need.amount_sats} sats ({need.urgency})",
                level='debug'
            )

        return {"success": True, "stored": True}

    def handle_liquidity_snapshot(
        self,
        peer_id: str,
        payload: Dict[str, Any],
        rpc: Any
    ) -> Dict[str, Any]:
        """
        Handle incoming LIQUIDITY_SNAPSHOT message.

        This is the preferred method for receiving liquidity needs - one message
        contains multiple needs instead of N individual messages.

        Args:
            peer_id: Sender peer ID
            payload: Message payload
            rpc: RPC interface for signature verification

        Returns:
            Result dict with success/error
        """
        # Validate payload structure
        if not validate_liquidity_snapshot_payload(payload):
            return {"error": "invalid payload"}

        reporter_id = payload.get("reporter_id")

        # Identity binding: sender must match reporter (prevent relay attacks)
        if peer_id != reporter_id:
            return {"error": "identity binding failed"}

        # Verify sender is a hive member
        member = self.database.get_member(reporter_id)
        if not member:
            return {"error": "reporter not a member"}

        # Rate limit check for snapshot messages
        if not self._check_rate_limit(
            reporter_id,
            self._snapshot_rate,
            LIQUIDITY_SNAPSHOT_RATE_LIMIT
        ):
            return {"error": "rate limited"}

        # Verify signature
        signature = payload.get("signature")
        if not signature:
            return {"error": "missing signature"}

        signing_message = get_liquidity_snapshot_signing_payload(payload)

        try:
            verify_result = rpc.checkmessage(signing_message, signature)
            if not verify_result.get("verified"):
                return {"error": "signature verification failed"}

            if verify_result.get("pubkey") != reporter_id:
                return {"error": "signature pubkey mismatch"}
        except Exception as e:
            return {"error": f"signature check failed: {e}"}

        # Record rate limit
        self._record_message(reporter_id, self._snapshot_rate)

        # Process each need in the snapshot
        needs = payload.get("needs", [])
        stored_count = 0
        batch_timestamp = payload.get("timestamp", int(time.time()))

        for need_data in needs:
            # Store the liquidity need
            need = LiquidityNeed(
                reporter_id=reporter_id,
                need_type=need_data.get("need_type", NEED_REBALANCE),
                target_peer_id=need_data.get("target_peer_id", ""),
                amount_sats=need_data.get("amount_sats", 0),
                urgency=need_data.get("urgency", URGENCY_LOW),
                max_fee_ppm=need_data.get("max_fee_ppm", 0),
                reason=need_data.get("reason", ""),
                current_balance_pct=need_data.get("current_balance_pct", 0.5),
                can_provide_inbound=need_data.get("can_provide_inbound", 0),
                can_provide_outbound=need_data.get("can_provide_outbound", 0),
                timestamp=batch_timestamp,
                signature=signature
            )

            # Use composite key for multiple needs from same reporter
            key = f"{reporter_id}:{need.target_peer_id}"
            with self._lock:
                self._liquidity_needs[key] = need

            # Store in database
            self.database.store_liquidity_need(
                reporter_id=need.reporter_id,
                need_type=need.need_type,
                target_peer_id=need.target_peer_id,
                amount_sats=need.amount_sats,
                urgency=need.urgency,
                max_fee_ppm=need.max_fee_ppm,
                reason=need.reason,
                current_balance_pct=need.current_balance_pct,
                timestamp=need.timestamp
            )

            stored_count += 1

        # Prune old needs if over limit
        self._prune_old_needs()

        if self.plugin:
            self.plugin.log(
                f"cl-hive: Received liquidity snapshot from {reporter_id[:16]}... "
                f"with {stored_count} needs",
                level='debug'
            )

        return {"success": True, "needs_stored": stored_count}

    def create_liquidity_snapshot_message(
        self,
        needs: List[Dict[str, Any]],
        rpc: Any
    ) -> Optional[bytes]:
        """
        Create a signed LIQUIDITY_SNAPSHOT message.

        This is the preferred method for sharing liquidity needs. Instead of
        sending N individual messages for N needs, send one snapshot with all
        liquidity needs.

        Args:
            needs: List of liquidity needs, each containing:
                - target_peer_id: External peer or hive member
                - need_type: 'inbound', 'outbound', 'rebalance'
                - amount_sats: How much is needed
                - urgency: 'critical', 'high', 'medium', 'low'
                - max_fee_ppm: Maximum fee willing to pay
                - reason: Why this liquidity is needed
                - current_balance_pct: Current local balance percentage
                - can_provide_inbound: Sats of inbound that can be provided
                - can_provide_outbound: Sats of outbound that can be provided
            rpc: RPC interface for signing

        Returns:
            Serialized message bytes, or None on error
        """
        # Enforce bounds
        if len(needs) > MAX_NEEDS_IN_SNAPSHOT:
            if self.plugin:
                self.plugin.log(
                    f"cl-hive: Liquidity snapshot too large ({len(needs)} > {MAX_NEEDS_IN_SNAPSHOT})",
                    level='warn'
                )
            return None

        timestamp = int(time.time())

        # Build payload for signing
        payload = {
            "reporter_id": self.our_pubkey,
            "timestamp": timestamp,
            "needs": needs,
        }

        # Sign the payload
        signing_message = get_liquidity_snapshot_signing_payload(payload)

        try:
            sig_result = rpc.signmessage(signing_message)
            signature = sig_result["zbase"]
        except Exception as e:
            if self.plugin:
                self.plugin.log(
                    f"cl-hive: Failed to sign liquidity snapshot: {e}",
                    level='warn'
                )
            return None

        return create_liquidity_snapshot(
            reporter_id=self.our_pubkey,
            timestamp=timestamp,
            signature=signature,
            needs=needs
        )

    def _prune_old_needs(self):
        """Remove old liquidity needs to stay under limit."""
        with self._lock:
            if len(self._liquidity_needs) <= MAX_PENDING_NEEDS:
                return

            # Sort by timestamp, remove oldest
            sorted_needs = sorted(
                self._liquidity_needs.items(),
                key=lambda x: x[1].timestamp
            )

            to_remove = len(sorted_needs) - MAX_PENDING_NEEDS
            for key, _ in sorted_needs[:to_remove]:
                del self._liquidity_needs[key]

    def get_prioritized_needs(self) -> List[LiquidityNeed]:
        """
        Get liquidity needs sorted by NNLB priority.

        Struggling nodes get higher priority.

        Returns:
            List of needs sorted by priority (highest first)
        """
        with self._lock:
            needs = list(self._liquidity_needs.values())

        def nnlb_priority(need: LiquidityNeed) -> float:
            """Calculate NNLB priority score."""
            # Get member health
            member_health = self.database.get_member_health(need.reporter_id)
            if member_health:
                health_score = member_health.get("overall_health", 50)
            else:
                health_score = 50

            # Clamp health_score to valid range before priority calc
            health_score = max(0, min(100, health_score))
            # Lower health = higher priority (inverted)
            health_priority = 1.0 - (health_score / 100.0)

            # Urgency multiplier
            urgency_mult = {
                URGENCY_CRITICAL: 2.0,
                URGENCY_HIGH: 1.5,
                URGENCY_MEDIUM: 1.0,
                URGENCY_LOW: 0.5
            }.get(need.urgency, 1.0)

            return health_priority * urgency_mult

        return sorted(needs, key=nnlb_priority, reverse=True)

    def assess_our_liquidity_needs(
        self,
        funds: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Assess what liquidity we currently need.

        Args:
            funds: Result of listfunds() call

        Returns:
            List of liquidity needs
        """
        channels = funds.get("channels", [])
        needs = []

        # Get hive members
        members = self.database.get_all_members()
        member_ids = {m.get("peer_id") for m in members}

        for ch in channels:
            if ch.get("state") != "CHANNELD_NORMAL":
                continue

            peer_id = ch.get("peer_id")
            if peer_id in member_ids:
                continue  # Skip hive channels, focus on external

            capacity = ch.get("amount_msat", 0) // 1000
            local = ch.get("our_amount_msat", 0) // 1000
            local_pct = local / capacity if capacity > 0 else 0.5

            # Determine if we need liquidity
            if local_pct < 0.2:
                # Depleted outbound - need outbound to this peer
                amount_needed = int(capacity * 0.5 - local)
                needs.append({
                    "need_type": NEED_OUTBOUND,
                    "target_peer_id": peer_id,
                    "amount_sats": amount_needed,
                    "urgency": URGENCY_HIGH if local_pct < 0.1 else URGENCY_MEDIUM,
                    "reason": REASON_CHANNEL_DEPLETED,
                    "current_balance_pct": local_pct
                })
            elif local_pct > 0.8:
                # Depleted inbound - need inbound from this peer
                amount_needed = int(local - capacity * 0.5)
                needs.append({
                    "need_type": NEED_INBOUND,
                    "target_peer_id": peer_id,
                    "amount_sats": amount_needed,
                    "urgency": URGENCY_HIGH if local_pct > 0.9 else URGENCY_MEDIUM,
                    "reason": REASON_CHANNEL_DEPLETED,
                    "current_balance_pct": local_pct
                })

        return needs

    def get_nnlb_assistance_status(self) -> Dict[str, Any]:
        """
        Get status of NNLB liquidity assistance.

        Returns:
            Dict with assistance statistics
        """
        needs = self.get_prioritized_needs()

        # Count by urgency
        urgency_counts = defaultdict(int)
        for need in needs:
            urgency_counts[need.urgency] += 1

        # Get struggling members (for informational purposes)
        struggling = self.database.get_struggling_members(threshold=40)

        return {
            "pending_needs": len(needs),
            "critical_needs": urgency_counts.get(URGENCY_CRITICAL, 0),
            "high_needs": urgency_counts.get(URGENCY_HIGH, 0),
            "medium_needs": urgency_counts.get(URGENCY_MEDIUM, 0),
            "low_needs": urgency_counts.get(URGENCY_LOW, 0),
            "struggling_members": len(struggling)
        }

    def cleanup_expired_data(self):
        """Clean up old liquidity needs."""
        now = time.time()

        with self._lock:
            # Remove old needs (older than 1 hour)
            old_needs = [
                rid for rid, need in self._liquidity_needs.items()
                if now - need.timestamp > 3600
            ]
            for rid in old_needs:
                del self._liquidity_needs[rid]

    def get_status(self) -> Dict[str, Any]:
        """
        Get overall liquidity coordination status.

        Returns:
            Dict with coordination status and statistics
        """
        nnlb_status = self.get_nnlb_assistance_status()

        # Count need types under lock to prevent RuntimeError during iteration
        with self._lock:
            inbound_needs = sum(
                1 for n in self._liquidity_needs.values()
                if n.need_type == NEED_INBOUND
            )
            outbound_needs = sum(
                1 for n in self._liquidity_needs.values()
                if n.need_type == NEED_OUTBOUND
            )
            pending_count = len(self._liquidity_needs)

        return {
            "status": "active",
            "pending_needs": pending_count,
            "inbound_needs": inbound_needs,
            "outbound_needs": outbound_needs,
            "nnlb_status": nnlb_status
        }

    # =========================================================================
    # Liquidity Intelligence Sharing (Information Only)
    # =========================================================================
    # These methods coordinate INFORMATION about liquidity state.
    # No fund transfers between nodes - each node manages its own funds.
    # Knowing fleet state helps nodes make better independent decisions about:
    # - Fee adjustments to direct public flow helpfully
    # - Rebalancing timing to avoid route conflicts
    # - Topology planning to prioritize helpful channel opens
    # =========================================================================

    def record_member_liquidity_report(
        self,
        member_id: str,
        depleted_channels: List[Dict[str, Any]],
        saturated_channels: List[Dict[str, Any]],
        rebalancing_active: bool = False,
        rebalancing_peers: List[str] = None
    ) -> Dict[str, Any]:
        """
        Record a liquidity state report from a cl-revenue-ops instance.

        INFORMATION SHARING - enables coordinated fee/rebalance decisions.
        No sats transfer between nodes.

        Args:
            member_id: Reporting member's pubkey
            depleted_channels: List of {peer_id, local_pct, capacity_sats}
            saturated_channels: List of {peer_id, local_pct, capacity_sats}
            rebalancing_active: Whether member is currently rebalancing
            rebalancing_peers: Which peers they're rebalancing through

        Returns:
            {"status": "recorded", ...}
        """
        # Verify member exists
        member = self.database.get_member(member_id)
        if not member:
            return {"error": "member_not_found"}

        timestamp = int(time.time())

        # Store in database
        self.database.update_member_liquidity_state(
            member_id=member_id,
            depleted_count=len(depleted_channels),
            saturated_count=len(saturated_channels),
            rebalancing_active=rebalancing_active,
            rebalancing_peers=rebalancing_peers or [],
            timestamp=timestamp
        )

        # Update in-memory tracking for fast access
        with self._lock:
            self._member_liquidity_state[member_id] = {
                "depleted_channels": depleted_channels,
                "saturated_channels": saturated_channels,
                "rebalancing_active": rebalancing_active,
                "rebalancing_peers": rebalancing_peers or [],
                "timestamp": timestamp
            }

        if self.plugin:
            self.plugin.log(
                f"cl-hive: Recorded liquidity state from {member_id[:16]}...: "
                f"depleted={len(depleted_channels)}, saturated={len(saturated_channels)}, "
                f"rebalancing={rebalancing_active}",
                level='debug'
            )

        return {
            "status": "recorded",
            "depleted_count": len(depleted_channels),
            "saturated_count": len(saturated_channels)
        }

    def get_fleet_liquidity_state(self) -> Dict[str, Any]:
        """
        Get fleet-wide liquidity state overview.

        INFORMATION ONLY - helps nodes understand fleet situation
        to make better independent decisions.

        Returns:
            Fleet liquidity summary for coordination
        """
        members = self.database.get_all_members()

        members_with_depleted = 0
        members_with_saturated = 0
        members_rebalancing = 0
        all_rebalancing_peers = set()

        # Get our own state
        our_state = self._member_liquidity_state.get(self.our_pubkey, {})

        for member in members:
            member_id = member.get("peer_id")
            state = self._member_liquidity_state.get(member_id)

            if state:
                if state.get("depleted_channels"):
                    members_with_depleted += 1
                if state.get("saturated_channels"):
                    members_with_saturated += 1
                if state.get("rebalancing_active"):
                    members_rebalancing += 1
                    all_rebalancing_peers.update(state.get("rebalancing_peers", []))

        # Identify common bottleneck peers (multiple members have issues)
        bottleneck_peers = self._get_common_bottleneck_peers()

        return {
            "active": True,
            "fleet_summary": {
                "total_members": len(members),
                "members_with_depleted_channels": members_with_depleted,
                "members_with_saturated_channels": members_with_saturated,
                "members_rebalancing": members_rebalancing,
                "common_bottleneck_peers": bottleneck_peers
            },
            "our_state": {
                "depleted_channels": len(our_state.get("depleted_channels", [])),
                "saturated_channels": len(our_state.get("saturated_channels", [])),
                "rebalancing_active": our_state.get("rebalancing_active", False)
            },
            "rebalancing_activity": {
                "active_count": members_rebalancing,
                "peers_in_use": list(all_rebalancing_peers)
            }
        }

    def get_fleet_liquidity_needs(self) -> List[Dict[str, Any]]:
        """
        Get fleet liquidity needs for coordination.

        INFORMATION ONLY - helps nodes understand what others need
        so they can make coordinated fee/rebalance decisions.

        Returns:
            List of needs with relevance scores
        """
        needs = []

        for member_id, state in self._member_liquidity_state.items():
            if member_id == self.our_pubkey:
                continue  # Skip ourselves

            # Get member health for priority
            member_health = self.database.get_member_health(member_id)
            health_score = member_health.get("overall_health", 50) if member_health else 50
            health_tier = member_health.get("health_tier", "stable") if member_health else "stable"

            # Process depleted channels (they need outbound)
            for ch in state.get("depleted_channels", []):
                peer_id = ch.get("peer_id")
                if not peer_id:
                    continue

                # Calculate relevance: how much we could help via fee adjustment
                relevance = self._calculate_relevance_score(peer_id)

                needs.append({
                    "member_id": member_id,
                    "need_type": "outbound",
                    "peer_id": peer_id,
                    "local_pct": ch.get("local_pct", 0),
                    "capacity_sats": ch.get("capacity_sats", 0),
                    "severity": "high" if ch.get("local_pct", 0) < 0.1 else "medium",
                    "member_health_tier": health_tier,
                    "our_relevance": relevance
                })

            # Process saturated channels (they need inbound)
            for ch in state.get("saturated_channels", []):
                peer_id = ch.get("peer_id")
                if not peer_id:
                    continue

                relevance = self._calculate_relevance_score(peer_id)

                needs.append({
                    "member_id": member_id,
                    "need_type": "inbound",
                    "peer_id": peer_id,
                    "local_pct": ch.get("local_pct", 1.0),
                    "capacity_sats": ch.get("capacity_sats", 0),
                    "severity": "high" if ch.get("local_pct", 1.0) > 0.9 else "medium",
                    "member_health_tier": health_tier,
                    "our_relevance": relevance
                })

        # Sort by severity and health tier (struggling members first)
        tier_priority = {"struggling": 0, "vulnerable": 1, "stable": 2, "thriving": 3}
        severity_priority = {"high": 0, "medium": 1, "low": 2}

        needs.sort(key=lambda n: (
            tier_priority.get(n["member_health_tier"], 2),
            severity_priority.get(n["severity"], 1),
            -n["our_relevance"]  # Higher relevance first
        ))

        return needs

    def _calculate_relevance_score(self, peer_id: str) -> float:
        """
        Calculate how relevant we are to helping with a peer.

        Based on whether we have a channel to this peer and our balance state.
        Higher score = we're better positioned to influence flow via fees.
        """
        try:
            channels = self.plugin.rpc.listpeerchannels(id=peer_id)
            our_channels = channels.get("channels", [])

            if not our_channels:
                return 0.0  # No direct connection

            # Sum capacity to this peer
            total_capacity = 0
            total_local = 0

            for ch in our_channels:
                if ch.get("state") != "CHANNELD_NORMAL":
                    continue
                capacity = ch.get("total_msat", 0) // 1000
                local = ch.get("to_us_msat", 0) // 1000
                total_capacity += capacity
                total_local += local

            if total_capacity == 0:
                return 0.0

            # Balanced channels are more useful for flow influence
            balance_ratio = total_local / total_capacity
            # Score peaks at 50% balance
            balance_score = 1.0 - abs(0.5 - balance_ratio) * 2

            # Larger capacity = more influence
            capacity_score = min(1.0, total_capacity / 10_000_000)  # Cap at 10M

            return (balance_score * 0.6 + capacity_score * 0.4)

        except Exception:
            return 0.0

    def _get_common_bottleneck_peers(self) -> List[str]:
        """
        Identify peers that multiple hive members have liquidity issues with.

        These are priority targets for fee coordination or new channel opens.
        """
        peer_issue_count: Dict[str, int] = defaultdict(int)

        for state in self._member_liquidity_state.values():
            for ch in state.get("depleted_channels", []):
                peer_id = ch.get("peer_id")
                if peer_id:
                    peer_issue_count[peer_id] += 1

            for ch in state.get("saturated_channels", []):
                peer_id = ch.get("peer_id")
                if peer_id:
                    peer_issue_count[peer_id] += 1

        # Return peers with issues from 2+ members
        bottlenecks = [
            peer_id for peer_id, count in peer_issue_count.items()
            if count >= 2
        ]

        return sorted(bottlenecks, key=lambda p: peer_issue_count[p], reverse=True)[:10]

    def check_rebalancing_conflict(self, peer_id: str) -> Dict[str, Any]:
        """
        Check if another fleet member is actively rebalancing through a peer.

        INFORMATION ONLY - helps avoid competing for the same routes.

        Args:
            peer_id: The peer to check

        Returns:
            Conflict info if found
        """
        with self._lock:
            state_snapshot = dict(self._member_liquidity_state)

        for member_id, state in state_snapshot.items():
            if member_id == self.our_pubkey:
                continue

            if not state.get("rebalancing_active"):
                continue

            if peer_id in state.get("rebalancing_peers", []):
                return {
                    "conflict": True,
                    "member_id": member_id,
                    "peer_id": peer_id,
                    "recommendation": "delay_rebalance",
                    "reason": f"Member {member_id[:12]}... is actively rebalancing through this peer"
                }

        return {"conflict": False}

    # =========================================================================
    # Internal Competition Detection (Phase 1 - Yield Optimization)
    # =========================================================================

    def detect_internal_competition(self) -> List[Dict[str, Any]]:
        """
        Detect when fleet members compete for the same routes.

        Internal competition occurs when multiple members have channels
        to the same (source, destination) pair. This leads to:
        - Fee undercutting between members
        - Wasted capacity
        - Lower overall fleet yield

        Returns:
            List of competition instances with recommendations
        """
        if not self.database:
            return []

        # Get all hive members and their topologies
        members = self.database.get_all_members()
        if len(members) < 2:
            return []

        member_ids = {m.get("peer_id") for m in members}

        # Build topology map: peer_id -> set of connected peers
        member_topologies: Dict[str, set] = {}

        for member in members:
            member_id = member.get("peer_id")
            if not member_id:
                continue

            # Get member's topology from state manager
            topology = set()
            if self.state_manager:
                state = self.state_manager.get_peer_state(member_id)
                if state and hasattr(state, 'topology'):
                    topology = set(state.topology or [])

            # Also check local channels if this is us
            if member_id == self.our_pubkey:
                try:
                    channels = self.plugin.rpc.listpeerchannels()
                    for ch in channels.get("channels", []):
                        if ch.get("state") == "CHANNELD_NORMAL":
                            peer_id = ch.get("peer_id")
                            if peer_id and peer_id not in member_ids:
                                topology.add(peer_id)
                except Exception:
                    pass

            member_topologies[member_id] = topology

        # Find overlapping peers (peers connected to 2+ members)
        peer_to_members: Dict[str, List[str]] = defaultdict(list)

        for member_id, topology in member_topologies.items():
            for peer_id in topology:
                if peer_id not in member_ids:  # External peer
                    peer_to_members[peer_id].append(member_id)

        # Identify competition: peers connected to multiple members
        competitions = []

        # For each pair of external peers, check if multiple members
        # can route between them (source -> member -> destination)
        external_peers = list(peer_to_members.keys())

        for i, source_peer in enumerate(external_peers):
            source_members = set(peer_to_members[source_peer])
            if len(source_members) < 2:
                continue

            for dest_peer in external_peers[i+1:]:
                if dest_peer == source_peer:
                    continue

                dest_members = set(peer_to_members[dest_peer])
                if len(dest_members) < 2:
                    continue

                # Members that can route source -> dest
                competing_members = list(source_members & dest_members)

                if len(competing_members) >= 2:
                    # Get peer aliases if possible
                    source_alias = self._get_peer_alias(source_peer)
                    dest_alias = self._get_peer_alias(dest_peer)

                    # Estimate capacity
                    total_capacity = self._estimate_route_capacity(
                        competing_members, source_peer, dest_peer
                    )

                    # Determine recommended primary (member with best position)
                    recommended_primary = self._select_primary_member(
                        competing_members, source_peer, dest_peer
                    )

                    # Estimate fee loss from competition
                    fee_loss_pct = self._estimate_competition_fee_loss(
                        len(competing_members)
                    )

                    competitions.append({
                        "source_peer_id": source_peer,
                        "destination_peer_id": dest_peer,
                        "source_alias": source_alias,
                        "destination_alias": dest_alias,
                        "competing_members": competing_members,
                        "member_count": len(competing_members),
                        "total_fleet_capacity_sats": total_capacity,
                        "estimated_fee_loss_pct": fee_loss_pct,
                        "recommendation": "coordinate_fees",
                        "recommended_primary": recommended_primary
                    })

        # Sort by number of competing members (most competition first)
        competitions.sort(key=lambda c: c["member_count"], reverse=True)

        # Limit results
        return competitions[:50]

    def _get_peer_alias(self, peer_id: str) -> Optional[str]:
        """Get peer alias from node list."""
        try:
            nodes = self.plugin.rpc.listnodes(id=peer_id)
            if nodes.get("nodes"):
                return nodes["nodes"][0].get("alias")
        except Exception:
            pass
        return None

    def _estimate_route_capacity(
        self,
        members: List[str],
        source_peer: str,
        dest_peer: str
    ) -> int:
        """Estimate total fleet capacity on a route."""
        total = 0

        for member_id in members:
            if member_id == self.our_pubkey:
                # Get our capacity to these peers
                try:
                    for peer in [source_peer, dest_peer]:
                        channels = self.plugin.rpc.listpeerchannels(id=peer)
                        for ch in channels.get("channels", []):
                            if ch.get("state") == "CHANNELD_NORMAL":
                                total += ch.get("total_msat", 0) // 1000
                except Exception:
                    pass
            else:
                # Estimate from state manager
                if self.state_manager:
                    state = self.state_manager.get_peer_state(member_id)
                    if state and hasattr(state, 'capacity_sats'):
                        # Rough estimate: assume average channel size
                        total += state.capacity_sats // 10  # Rough per-channel estimate

        return total

    def _select_primary_member(
        self,
        members: List[str],
        source_peer: str,
        dest_peer: str
    ) -> Optional[str]:
        """
        Select the member who should be primary for a route.

        Criteria:
        - Most capacity to source/dest
        - Best balance (can actually route)
        - Highest uptime
        """
        best_member = None
        best_score = 0

        for member_id in members:
            score = 0

            if member_id == self.our_pubkey:
                # Calculate our score based on actual channel data
                try:
                    for peer in [source_peer, dest_peer]:
                        channels = self.plugin.rpc.listpeerchannels(id=peer)
                        for ch in channels.get("channels", []):
                            if ch.get("state") == "CHANNELD_NORMAL":
                                capacity = ch.get("total_msat", 0) // 1000
                                local = ch.get("to_us_msat", 0) // 1000
                                # Score: capacity + balance score
                                balance_pct = local / capacity if capacity > 0 else 0.5
                                balance_score = 1.0 - abs(0.5 - balance_pct) * 2
                                score += capacity * balance_score / 1_000_000
                except Exception:
                    pass
            else:
                # Score from state manager
                if self.state_manager:
                    state = self.state_manager.get_peer_state(member_id)
                    if state:
                        # Use capacity and uptime as proxies
                        capacity = getattr(state, 'capacity_sats', 0)
                        score = capacity / 10_000_000  # Normalize

            if score > best_score:
                best_score = score
                best_member = member_id

        return best_member

    def _estimate_competition_fee_loss(self, num_competitors: int) -> float:
        """
        Estimate fee loss percentage due to competition.

        More competitors = more undercutting = lower fees.
        """
        if num_competitors <= 1:
            return 0.0

        # Simple model: each additional competitor reduces fees by ~15%
        # 2 competitors: 15% loss
        # 3 competitors: 27% loss
        # 4+ competitors: 35-40% loss
        loss_per_competitor = 0.15
        max_loss = 0.40

        loss = (num_competitors - 1) * loss_per_competitor
        return min(loss, max_loss) * 100  # Return as percentage

    def get_internal_competition_summary(self) -> Dict[str, Any]:
        """
        Get summary of internal competition in the fleet.

        Returns:
            Summary with statistics and recommendations
        """
        competitions = self.detect_internal_competition()

        if not competitions:
            return {
                "status": "ok",
                "competition_count": 0,
                "message": "No internal competition detected",
                "competitions": []
            }

        # Calculate totals
        total_capacity_at_risk = sum(c["total_fleet_capacity_sats"] for c in competitions)
        avg_fee_loss = sum(c["estimated_fee_loss_pct"] for c in competitions) / len(competitions)

        # Count by severity
        high_competition = len([c for c in competitions if c["member_count"] >= 3])
        medium_competition = len([c for c in competitions if c["member_count"] == 2])

        return {
            "status": "warning" if competitions else "ok",
            "competition_count": len(competitions),
            "high_competition_routes": high_competition,
            "medium_competition_routes": medium_competition,
            "total_capacity_at_risk_sats": total_capacity_at_risk,
            "avg_estimated_fee_loss_pct": round(avg_fee_loss, 1),
            "recommendation": "Implement coordinated fee strategy to eliminate undercutting",
            "competitions": competitions[:10]  # Top 10 for display
        }

    # =========================================================================
    # MCF (MIN-COST MAX-FLOW) INTEGRATION (Phase 15)
    # =========================================================================

    def get_all_liquidity_needs_for_mcf(self) -> List[Dict[str, Any]]:
        """
        Get all liquidity needs in MCF-compatible format.

        Combines our own needs with fleet needs for the MCF solver.
        Used by MCFCoordinator to build the optimization network.

        Returns:
            List of needs formatted for MCF solver with:
            - member_id: Who has the need
            - need_type: 'inbound' or 'outbound'
            - target_peer: Target peer pubkey
            - amount_sats: Amount needed
            - urgency: Priority level
            - max_fee_ppm: Maximum acceptable fee
        """
        mcf_needs = []

        # Add needs from _liquidity_needs (received via gossip)
        for need in self._liquidity_needs.values():
            # Skip stale needs (older than 30 minutes)
            if time.time() - need.timestamp > 1800:
                continue

            mcf_needs.append({
                "member_id": need.reporter_id,
                "need_type": need.need_type,
                "target_peer": need.target_peer_id,
                "amount_sats": need.amount_sats,
                "urgency": need.urgency,
                "max_fee_ppm": need.max_fee_ppm,
                "channel_id": "",  # Not always known from gossip
            })

        # Add our own needs (if we have fresh data)
        try:
            if self.plugin and self.plugin.rpc:
                funds = self.plugin.rpc.listfunds()
                our_needs = self.assess_our_liquidity_needs(funds)

                for need in our_needs:
                    mcf_needs.append({
                        "member_id": self.our_pubkey,
                        "need_type": need["need_type"],
                        "target_peer": need["target_peer_id"],
                        "amount_sats": need["amount_sats"],
                        "urgency": need["urgency"],
                        "max_fee_ppm": 1000,  # Default max fee
                        "channel_id": "",
                    })
        except Exception as e:
            self._log(f"Error assessing our needs for MCF: {e}", "debug")

        # Add remote MCF needs (received from other fleet members)
        for reporter_id, need in self._remote_mcf_needs.items():
            # Skip stale needs (older than 30 minutes)
            received_at = need.get("received_at", 0)
            if time.time() - received_at > 1800:
                continue

            mcf_needs.append({
                "member_id": need.get("reporter_id", reporter_id),
                "need_type": need.get("need_type", "inbound"),
                "target_peer": need.get("target_peer", ""),
                "amount_sats": need.get("amount_sats", 0),
                "urgency": need.get("urgency", "medium"),
                "max_fee_ppm": need.get("max_fee_ppm", 1000),
                "channel_id": need.get("channel_id", ""),
            })

        return mcf_needs

    def store_remote_mcf_need(self, need: Dict[str, Any]) -> bool:
        """
        Store a remote MCF need received from another fleet member.

        Called by the coordinator when receiving MCF_NEEDS_BATCH messages.

        Args:
            need: Need dict containing reporter_id, need_type, target_peer, etc.

        Returns:
            True if stored successfully
        """
        reporter_id = need.get("reporter_id", "")
        if not reporter_id:
            return False

        # Validate basic structure
        need_type = need.get("need_type", "")
        if need_type not in ("inbound", "outbound"):
            return False

        amount_sats = need.get("amount_sats", 0)
        if amount_sats <= 0:
            return False

        # Store by reporter_id (latest need per member)
        with self._lock:
            self._remote_mcf_needs[reporter_id] = {
                "reporter_id": reporter_id,
                "need_type": need_type,
                "target_peer": need.get("target_peer", ""),
                "amount_sats": amount_sats,
                "urgency": need.get("urgency", "medium"),
                "max_fee_ppm": need.get("max_fee_ppm", 1000),
                "channel_id": need.get("channel_id", ""),
                "received_at": need.get("received_at", int(time.time())),
            }

            # Enforce size limit
            if len(self._remote_mcf_needs) > self._max_remote_needs:
                # Remove oldest entries
                sorted_needs = sorted(
                    self._remote_mcf_needs.items(),
                    key=lambda x: x[1].get("received_at", 0)
                )
                for k, _ in sorted_needs[:100]:
                    del self._remote_mcf_needs[k]

        return True

    def get_remote_mcf_needs_count(self) -> int:
        """Get count of stored remote MCF needs."""
        return len(self._remote_mcf_needs)

    def clear_stale_remote_needs(self, max_age_seconds: int = 1800) -> int:
        """
        Clear remote MCF needs older than max_age_seconds.

        Args:
            max_age_seconds: Maximum age in seconds (default 30 minutes)

        Returns:
            Number of needs removed
        """
        now = time.time()
        stale_keys = [
            k for k, v in self._remote_mcf_needs.items()
            if now - v.get("received_at", 0) > max_age_seconds
        ]
        for k in stale_keys:
            del self._remote_mcf_needs[k]
        return len(stale_keys)

    def receive_mcf_assignment(
        self,
        assignment_data: Dict[str, Any],
        solution_timestamp: int,
        coordinator_id: str
    ) -> bool:
        """
        Receive and store an MCF assignment from the coordinator.

        Called when we receive an MCF_SOLUTION_BROADCAST with assignments for us.

        Args:
            assignment_data: Assignment dict from solution
            solution_timestamp: Timestamp of the MCF solution
            coordinator_id: Pubkey of the coordinator

        Returns:
            True if assignment was accepted
        """
        # Generate assignment ID
        from_ch = assignment_data.get("from_channel", "")[-8:]
        to_ch = assignment_data.get("to_channel", "")[-8:]
        assignment_id = f"mcf_{solution_timestamp}_{assignment_data.get('priority', 0)}_{from_ch}_{to_ch}"

        # Check for duplicate
        if assignment_id in self._mcf_assignments:
            return False

        # Validate basic fields
        amount_sats = assignment_data.get("amount_sats", 0)
        if amount_sats <= 0:
            return False

        # Create assignment
        assignment = MCFAssignment(
            assignment_id=assignment_id,
            solution_timestamp=solution_timestamp,
            coordinator_id=coordinator_id,
            from_channel=assignment_data.get("from_channel", ""),
            to_channel=assignment_data.get("to_channel", ""),
            amount_sats=amount_sats,
            expected_cost_sats=assignment_data.get("expected_cost_sats", 0),
            path=assignment_data.get("path", []),
            priority=assignment_data.get("priority", 0),
            via_fleet=assignment_data.get("via_fleet", True),
            received_at=int(time.time()),
            status="pending",
        )

        # Enforce limits
        with self._lock:
            if len(self._mcf_assignments) >= MAX_MCF_ASSIGNMENTS:
                self._cleanup_old_mcf_assignments_unlocked()
                # If still at limit after cleanup, reject
                if len(self._mcf_assignments) >= MAX_MCF_ASSIGNMENTS:
                    return False

            self._mcf_assignments[assignment_id] = assignment
            self._last_mcf_solution_timestamp = solution_timestamp
            self._mcf_ack_sent = False

        self._log(
            f"Received MCF assignment {assignment_id}: "
            f"{amount_sats} sats, priority={assignment.priority}",
            "info"
        )

        return True

    def get_pending_mcf_assignments(self) -> List[MCFAssignment]:
        """
        Get pending MCF assignments sorted by priority.

        Returns:
            List of pending assignments (status='pending'), sorted by priority
        """
        with self._lock:
            self._cleanup_old_mcf_assignments_unlocked()
            pending = [
                a for a in self._mcf_assignments.values()
                if a.status == "pending"
            ]

        return sorted(pending, key=lambda a: a.priority)

    def get_mcf_assignment(self, assignment_id: str) -> Optional[MCFAssignment]:
        """Get a specific MCF assignment by ID."""
        with self._lock:
            return self._mcf_assignments.get(assignment_id)

    def update_mcf_assignment_status(
        self,
        assignment_id: str,
        status: str,
        actual_amount_sats: int = 0,
        actual_cost_sats: int = 0,
        error_message: str = ""
    ) -> bool:
        """
        Update the status of an MCF assignment.

        Args:
            assignment_id: Assignment to update
            status: New status (executing, completed, failed, rejected)
            actual_amount_sats: Actual amount rebalanced (for completed)
            actual_cost_sats: Actual cost paid (for completed)
            error_message: Error message (for failed)

        Returns:
            True if assignment was found and updated
        """
        with self._lock:
            assignment = self._mcf_assignments.get(assignment_id)
            if not assignment:
                return False

            assignment.status = status
            assignment.actual_amount_sats = actual_amount_sats
            assignment.actual_cost_sats = actual_cost_sats
            assignment.error_message = error_message

            if status in ("completed", "failed", "rejected"):
                assignment.completed_at = int(time.time())

        self._log(
            f"MCF assignment {assignment_id} status updated to {status}",
            "info"
        )

        return True

    def claim_pending_assignment(self, assignment_id: str = None) -> Optional[MCFAssignment]:
        """
        Atomically find and claim a pending MCF assignment.

        Prevents TOCTOU race by doing lookup + status update in a single lock.

        Args:
            assignment_id: Specific assignment to claim, or None for highest priority

        Returns:
            The claimed MCFAssignment (now status='executing'), or None
        """
        with self._lock:
            self._cleanup_old_mcf_assignments_unlocked()

            if assignment_id:
                # Claim specific assignment
                assignment = self._mcf_assignments.get(assignment_id)
                if not assignment or assignment.status != "pending":
                    return None
            else:
                # Claim highest priority pending assignment
                pending = [
                    a for a in self._mcf_assignments.values()
                    if a.status == "pending"
                ]
                if not pending:
                    return None
                assignment = min(pending, key=lambda a: a.priority)

            # Atomically mark as executing
            assignment.status = "executing"

        self._log(
            f"MCF assignment {assignment.assignment_id} claimed (executing)",
            "info"
        )
        return assignment

    def create_mcf_ack_message(self) -> Optional[bytes]:
        """
        Create MCF_ASSIGNMENT_ACK message for current solution.

        Returns:
            Serialized message or None if no pending solution
        """
        with self._lock:
            if self._mcf_ack_sent:
                return None
            if not self._last_mcf_solution_timestamp:
                return None
            solution_ts = self._last_mcf_solution_timestamp

        pending = self.get_pending_mcf_assignments()
        assignment_count = len(pending)

        try:
            msg = create_mcf_assignment_ack(
                solution_timestamp=solution_ts,
                assignment_count=assignment_count,
                rpc=self.plugin.rpc,
                our_pubkey=self.our_pubkey
            )
            if msg:
                with self._lock:
                    self._mcf_ack_sent = True
            return msg
        except Exception as e:
            self._log(f"Error creating MCF ACK: {e}", "warn")
            return None

    def create_mcf_completion_message(
        self,
        assignment_id: str
    ) -> Optional[bytes]:
        """
        Create MCF_COMPLETION_REPORT message for a completed assignment.

        Args:
            assignment_id: Assignment that was completed

        Returns:
            Serialized message or None on error
        """
        with self._lock:
            assignment = self._mcf_assignments.get(assignment_id)
            if not assignment:
                return None
            if assignment.status not in ("completed", "failed", "rejected"):
                return None
            # Snapshot fields under lock
            success = (assignment.status == "completed")
            actual_amount = assignment.actual_amount_sats
            actual_cost = assignment.actual_cost_sats
            error_msg = assignment.error_message

        try:
            return create_mcf_completion_report(
                assignment_id=assignment_id,
                success=success,
                actual_amount_sats=actual_amount,
                actual_cost_sats=actual_cost,
                error_message=error_msg,
                rpc=self.plugin.rpc,
                our_pubkey=self.our_pubkey
            )
        except Exception as e:
            self._log(f"Error creating MCF completion report: {e}", "warn")
            return None

    def get_mcf_status(self) -> Dict[str, Any]:
        """
        Get MCF assignment status for this node.

        Returns:
            Dict with assignment counts and details
        """
        with self._lock:
            self._cleanup_old_mcf_assignments_unlocked()

            all_assignments = list(self._mcf_assignments.values())
            solution_ts = self._last_mcf_solution_timestamp
            ack_sent = self._mcf_ack_sent

        pending = [a for a in all_assignments if a.status == "pending"]
        executing = [a for a in all_assignments if a.status == "executing"]
        completed = [a for a in all_assignments if a.status == "completed"]
        failed = [a for a in all_assignments if a.status in ("failed", "rejected")]

        return {
            "last_solution_timestamp": solution_ts,
            "ack_sent": ack_sent,
            "assignment_counts": {
                "total": len(all_assignments),
                "pending": len(pending),
                "executing": len(executing),
                "completed": len(completed),
                "failed": len(failed),
            },
            "pending_assignments": [a.to_dict() for a in pending[:10]],
            "total_pending_amount_sats": sum(a.amount_sats for a in pending),
        }

    def _cleanup_old_mcf_assignments_unlocked(self) -> None:
        """Remove old/expired MCF assignments. Caller MUST hold self._lock."""
        now = time.time()
        expired = []

        for assignment_id, assignment in self._mcf_assignments.items():
            age = now - assignment.received_at

            # Remove completed/failed assignments older than 1 hour
            if assignment.status in ("completed", "failed", "rejected"):
                if age > MCF_ASSIGNMENT_TTL:
                    expired.append(assignment_id)

            # Remove pending assignments older than solution TTL (20 min)
            elif assignment.status == "pending":
                if age > 1200:  # MAX_SOLUTION_AGE
                    expired.append(assignment_id)

        for assignment_id in expired:
            del self._mcf_assignments[assignment_id]

        if expired:
            self._log(f"Cleaned up {len(expired)} old MCF assignments", "debug")

    def _cleanup_old_mcf_assignments(self) -> None:
        """Remove old/expired MCF assignments (acquires lock)."""
        with self._lock:
            self._cleanup_old_mcf_assignments_unlocked()

    def get_all_assignments(self) -> List:
        """Return a snapshot of all MCF assignments (thread-safe)."""
        with self._lock:
            return list(self._mcf_assignments.values())

    def timeout_stuck_assignments(self, max_execution_time: int = 1800) -> List[str]:
        """
        Check for and timeout assignments stuck in 'executing' state.

        Args:
            max_execution_time: Max seconds in executing state (default: 30 min)

        Returns:
            List of assignment IDs that were timed out
        """
        now = int(time.time())
        timed_out = []

        with self._lock:
            for assignment in list(self._mcf_assignments.values()):
                if assignment.status == "executing":
                    age = now - assignment.received_at
                    if age > max_execution_time:
                        assignment.status = "failed"
                        assignment.error_message = "execution_timeout"
                        assignment.completed_at = now
                        timed_out.append(assignment.assignment_id)

        for aid in timed_out:
            self._log(f"MCF assignment {aid[:20]}... timed out after {max_execution_time}s", "warn")

        return timed_out

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"LIQUIDITY_COORD: {message}", level=level)
