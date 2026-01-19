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

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from .protocol import (
    HiveMessageType,
    serialize,
    create_liquidity_need,
    validate_liquidity_need_payload,
    get_liquidity_need_signing_payload,
    LIQUIDITY_NEED_RATE_LIMIT,
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
        fee_intel_mgr: Any = None
    ):
        """
        Initialize the liquidity coordinator.

        Args:
            database: HiveDatabase instance
            plugin: Plugin instance for RPC/logging
            our_pubkey: Our node's pubkey
            fee_intel_mgr: FeeIntelligenceManager for health data
        """
        self.database = database
        self.plugin = plugin
        self.our_pubkey = our_pubkey
        self.fee_intel_mgr = fee_intel_mgr

        # In-memory tracking
        self._liquidity_needs: Dict[str, LiquidityNeed] = {}  # reporter_id -> need

        # Liquidity state tracking (information only)
        # Stores latest liquidity reports from cl-revenue-ops instances
        self._member_liquidity_state: Dict[str, Dict[str, Any]] = {}

        # Rate limiting
        self._need_rate: Dict[str, List[float]] = defaultdict(list)

    def _check_rate_limit(
        self,
        sender: str,
        rate_tracker: Dict[str, List[float]],
        limit: Tuple[int, int]
    ) -> bool:
        """Check if sender is within rate limit."""
        max_count, period = limit
        now = time.time()

        # Clean old entries
        rate_tracker[sender] = [
            ts for ts in rate_tracker[sender]
            if now - ts < period
        ]

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

        # Store in memory (replace older need from same reporter)
        self._liquidity_needs[reporter_id] = need

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

    def _prune_old_needs(self):
        """Remove old liquidity needs to stay under limit."""
        if len(self._liquidity_needs) <= MAX_PENDING_NEEDS:
            return

        # Sort by timestamp, remove oldest
        sorted_needs = sorted(
            self._liquidity_needs.items(),
            key=lambda x: x[1].timestamp
        )

        to_remove = len(sorted_needs) - MAX_PENDING_NEEDS
        for reporter_id, _ in sorted_needs[:to_remove]:
            del self._liquidity_needs[reporter_id]

    def get_prioritized_needs(self) -> List[LiquidityNeed]:
        """
        Get liquidity needs sorted by NNLB priority.

        Struggling nodes get higher priority.

        Returns:
            List of needs sorted by priority (highest first)
        """
        needs = list(self._liquidity_needs.values())

        def nnlb_priority(need: LiquidityNeed) -> float:
            """Calculate NNLB priority score."""
            # Get member health
            member_health = self.database.get_member_health(need.reporter_id)
            if member_health:
                health_score = member_health.get("overall_health", 50)
            else:
                health_score = 50

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

        # Count need types
        inbound_needs = sum(
            1 for n in self._liquidity_needs.values()
            if n.need_type == NEED_INBOUND
        )
        outbound_needs = sum(
            1 for n in self._liquidity_needs.values()
            if n.need_type == NEED_OUTBOUND
        )

        return {
            "status": "active",
            "pending_needs": len(self._liquidity_needs),
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
        for member_id, state in self._member_liquidity_state.items():
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
