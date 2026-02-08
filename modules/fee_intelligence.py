"""
Fee Intelligence Manager for cl-hive.

Implements cooperative fee coordination through:
1. Fee intelligence sharing between hive members
2. Aggregated fee profile generation
3. NNLB-aware fee recommendations

Author: Lightning Goats Team
"""

import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from modules.protocol import (
    HiveMessageType,
    get_fee_intelligence_snapshot_signing_payload,
    validate_fee_intelligence_snapshot_payload,
    get_health_report_signing_payload,
    validate_health_report_payload,
    create_fee_intelligence_snapshot,
    create_health_report,
    FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT,
    HEALTH_REPORT_RATE_LIMIT,
    MAX_PEERS_IN_SNAPSHOT,
)


# =============================================================================
# CONSTANTS
# =============================================================================

# Weight factors for fee recommendation (from design doc)
WEIGHT_QUALITY = 0.25
WEIGHT_ELASTICITY = 0.30
WEIGHT_COMPETITION = 0.20
WEIGHT_FAIRNESS = 0.25

# Fee bounds
MIN_FEE_PPM = 1
MAX_FEE_PPM = 5000
DEFAULT_BASE_FEE = 100

# Health tier thresholds
HEALTH_THRIVING = 75
HEALTH_HEALTHY = 50
HEALTH_STRUGGLING = 25

# Elasticity thresholds
ELASTICITY_VERY_ELASTIC = -0.5
ELASTICITY_SOMEWHAT_ELASTIC = 0.0


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class PeerFeeProfile:
    """Aggregated fee intelligence for an external peer."""
    peer_id: str
    reporters: List[str]
    avg_fee_charged: float
    min_fee_charged: int
    max_fee_charged: int
    total_hive_volume: int
    total_hive_revenue: int
    avg_utilization: float
    estimated_elasticity: float
    optimal_fee_estimate: int
    last_update: int
    confidence: float


@dataclass
class MemberHealth:
    """Health assessment for NNLB."""
    peer_id: str
    timestamp: int
    overall_health: int
    capacity_score: int
    revenue_score: int
    connectivity_score: int
    tier: str
    needs_help: bool
    can_help_others: bool


# =============================================================================
# FEE INTELLIGENCE MANAGER
# =============================================================================

class FeeIntelligenceManager:
    """
    Manages fee intelligence sharing and aggregation.

    Responsibilities:
    - Create signed fee intelligence messages
    - Process incoming fee intelligence from peers
    - Aggregate intelligence into peer fee profiles
    - Calculate optimal fee recommendations using NNLB principles
    - Track member health for cooperative assistance

    Thread Safety:
    - All database operations use thread-local connections
    - Rate limiting uses in-memory tracking per sender
    """

    def __init__(
        self,
        database,
        plugin=None,
        our_pubkey: str = "",
    ):
        """
        Initialize the FeeIntelligenceManager.

        Args:
            database: HiveDatabase instance
            plugin: Optional plugin reference for logging and RPC
            our_pubkey: Our node's public key
        """
        self.db = database
        self.plugin = plugin
        self.our_pubkey = our_pubkey

        # Rate limiting: {sender_id: [timestamp, ...]}
        self._fee_intel_snapshot_rate: Dict[str, List[int]] = {}
        self._health_report_rate: Dict[str, List[int]] = {}

    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[FeeIntelligenceManager] {msg}", level=level)

    # =========================================================================
    # RATE LIMITING
    # =========================================================================

    def _check_rate_limit(
        self,
        sender_id: str,
        rate_dict: Dict[str, List[int]],
        limit: Tuple[int, int]
    ) -> bool:
        """
        Check if sender is within rate limit.

        Args:
            sender_id: Sender's peer ID
            rate_dict: Rate tracking dictionary
            limit: (max_count, period_seconds) tuple

        Returns:
            True if within limit, False if rate limited
        """
        max_count, period = limit
        now = int(time.time())
        cutoff = now - period

        # Get sender's history, filter old entries
        history = rate_dict.get(sender_id, [])
        history = [t for t in history if t > cutoff]
        rate_dict[sender_id] = history

        if len(history) >= max_count:
            return False

        return True

    def _record_message(
        self,
        sender_id: str,
        rate_dict: Dict[str, List[int]]
    ) -> None:
        """Record a message for rate limiting."""
        now = int(time.time())
        if sender_id not in rate_dict:
            rate_dict[sender_id] = []
        rate_dict[sender_id].append(now)

    # =========================================================================
    # FEE INTELLIGENCE CREATION
    # =========================================================================

    def create_fee_intelligence_snapshot_message(
        self,
        peers: List[Dict[str, Any]],
        rpc
    ) -> Optional[bytes]:
        """
        Create a signed FEE_INTELLIGENCE_SNAPSHOT message.

        This is the preferred method for sharing fee intelligence. Instead of
        sending N individual messages for N peers, send one snapshot with all
        peer observations.

        Args:
            peers: List of peer observations, each containing:
                - peer_id: External peer being reported on
                - our_fee_ppm: Fee we charge to this peer
                - their_fee_ppm: Fee they charge us (optional)
                - forward_count: Number of forwards
                - forward_volume_sats: Total volume routed
                - revenue_sats: Fees earned
                - flow_direction: 'source', 'sink', or 'balanced'
                - utilization_pct: Channel utilization (0.0-1.0)
            rpc: RPC proxy for signmessage

        Returns:
            Serialized message bytes or None on error
        """
        if not self.our_pubkey:
            self._log("Cannot create fee intelligence snapshot: no pubkey set", level='warn')
            return None

        if not peers:
            self._log("Cannot create fee intelligence snapshot: no peers", level='warn')
            return None

        if len(peers) > MAX_PEERS_IN_SNAPSHOT:
            self._log(
                f"Too many peers in snapshot ({len(peers)} > {MAX_PEERS_IN_SNAPSHOT}), truncating",
                level='warn'
            )
            peers = peers[:MAX_PEERS_IN_SNAPSHOT]

        timestamp = int(time.time())

        # Build payload for signing
        payload = {
            "reporter_id": self.our_pubkey,
            "timestamp": timestamp,
            "peers": peers,
        }

        # Sign the payload
        signing_msg = get_fee_intelligence_snapshot_signing_payload(payload)
        try:
            sig_result = rpc.signmessage(signing_msg)
            signature = sig_result['zbase']
        except Exception as e:
            self._log(f"Failed to sign fee intelligence snapshot: {e}", level='error')
            return None

        return create_fee_intelligence_snapshot(
            reporter_id=self.our_pubkey,
            timestamp=timestamp,
            signature=signature,
            peers=peers
        )

    # =========================================================================
    # FEE INTELLIGENCE PROCESSING
    # =========================================================================

    def handle_fee_intelligence_snapshot(
        self,
        sender_id: str,
        payload: Dict[str, Any],
        rpc
    ) -> Dict[str, Any]:
        """
        Handle incoming FEE_INTELLIGENCE_SNAPSHOT message.

        Validates signature and stores intelligence for all peers in the snapshot.
        This is the preferred method - one message contains all peer observations.

        Args:
            sender_id: Peer who sent the message
            payload: Message payload containing peers list
            rpc: RPC proxy for checkmessage

        Returns:
            Dict with result status
        """
        # Rate limit check
        if not self._check_rate_limit(
            sender_id, self._fee_intel_snapshot_rate, FEE_INTELLIGENCE_SNAPSHOT_RATE_LIMIT
        ):
            self._log(f"Rate limited fee intelligence snapshot from {sender_id[:16]}...")
            return {"error": "rate_limited"}

        # Validate payload structure
        if not validate_fee_intelligence_snapshot_payload(payload):
            self._log(f"Invalid fee intelligence snapshot payload from {sender_id[:16]}...")
            return {"error": "invalid_payload"}

        # Verify reporter matches sender
        reporter_id = payload.get("reporter_id")
        if reporter_id != sender_id:
            self._log(
                f"Fee intelligence snapshot reporter mismatch: {reporter_id[:16]}... != {sender_id[:16]}..."
            )
            return {"error": "reporter_mismatch"}

        # Verify reporter is a hive member
        member = self.db.get_member(reporter_id)
        if not member:
            self._log(f"Fee intelligence snapshot from non-member {reporter_id[:16]}...")
            return {"error": "not_a_member"}

        # Verify signature
        signature = payload.get("signature")
        signing_msg = get_fee_intelligence_snapshot_signing_payload(payload)

        try:
            verify_result = rpc.checkmessage(signing_msg, signature)
            if not verify_result.get("verified"):
                self._log(f"Fee intelligence snapshot signature verification failed")
                return {"error": "invalid_signature"}
            if verify_result.get("pubkey") != reporter_id:
                self._log(f"Fee intelligence snapshot signature pubkey mismatch")
                return {"error": "signature_mismatch"}
        except Exception as e:
            self._log(f"Signature verification error: {e}", level='error')
            return {"error": "verification_failed"}

        # Record for rate limiting
        self._record_message(sender_id, self._fee_intel_snapshot_rate)

        # Store intelligence for each peer
        peers = payload.get("peers", [])
        timestamp = payload.get("timestamp")
        stored_count = 0

        for peer in peers:
            peer_id = peer.get("peer_id")
            if not peer_id:
                continue

            self.db.store_fee_intelligence(
                reporter_id=reporter_id,
                target_peer_id=peer_id,
                timestamp=timestamp,
                our_fee_ppm=peer.get("our_fee_ppm", 0),
                their_fee_ppm=peer.get("their_fee_ppm", 0),
                forward_count=peer.get("forward_count", 0),
                forward_volume_sats=peer.get("forward_volume_sats", 0),
                revenue_sats=peer.get("revenue_sats", 0),
                flow_direction=peer.get("flow_direction", "balanced"),
                utilization_pct=peer.get("utilization_pct", 0.0),
                signature=signature,  # Same signature for all peers in snapshot
                last_fee_change_ppm=peer.get("last_fee_change_ppm", 0),
                volume_delta_pct=peer.get("volume_delta_pct", 0.0),
                days_observed=peer.get("days_observed", 1)
            )
            stored_count += 1

        self._log(
            f"Stored fee intelligence snapshot from {reporter_id[:16]}... "
            f"with {stored_count} peer observations"
        )

        return {"success": True, "peers_stored": stored_count}

    # =========================================================================
    # FEE PROFILE AGGREGATION
    # =========================================================================

    def aggregate_fee_profiles(self) -> int:
        """
        Aggregate all fee intelligence into peer fee profiles.

        Calculates averages, estimates elasticity, and determines
        optimal fee recommendations.

        Returns:
            Number of profiles updated
        """
        # Get all recent fee intelligence
        intel_reports = self.db.get_all_fee_intelligence(max_age_hours=24)

        # Group by target peer
        by_peer: Dict[str, List[Dict[str, Any]]] = {}
        for report in intel_reports:
            peer_id = report.get("target_peer_id")
            if peer_id not in by_peer:
                by_peer[peer_id] = []
            by_peer[peer_id].append(report)

        updated = 0
        for peer_id, reports in by_peer.items():
            if not reports:
                continue

            # Get unique reporters
            reporters = list(set(r.get("reporter_id") for r in reports if r.get("reporter_id")))

            # Calculate fee statistics
            fees = [r.get("our_fee_ppm", 0) for r in reports if r.get("our_fee_ppm", 0) > 0]
            if fees:
                avg_fee = sum(fees) / len(fees)
                min_fee = min(fees)
                max_fee = max(fees)
            else:
                avg_fee = DEFAULT_BASE_FEE
                min_fee = 0
                max_fee = 0

            # Calculate volume and revenue totals
            total_volume = sum(r.get("forward_volume_sats", 0) for r in reports)
            total_revenue = sum(r.get("revenue_sats", 0) for r in reports)

            # Calculate average utilization
            utils = [r.get("utilization_pct", 0) for r in reports]
            avg_util = sum(utils) / len(utils) if utils else 0

            # Estimate elasticity from volume delta observations
            elasticity = self._estimate_elasticity(reports)

            # Calculate optimal fee estimate
            optimal_fee = self._calculate_optimal_fee(
                avg_fee=avg_fee,
                elasticity=elasticity,
                reporter_count=len(reporters)
            )

            # Calculate confidence based on reporter count and data freshness
            confidence = self._calculate_confidence(reports, len(reporters))

            # Update the profile in database
            self.db.update_peer_fee_profile(
                peer_id=peer_id,
                reporter_count=len(reporters),
                avg_fee_charged=avg_fee,
                min_fee_charged=min_fee,
                max_fee_charged=max_fee,
                total_hive_volume=total_volume,
                total_hive_revenue=total_revenue,
                avg_utilization=avg_util,
                estimated_elasticity=elasticity,
                optimal_fee_estimate=optimal_fee,
                confidence=confidence
            )
            updated += 1

        self._log(f"Aggregated {updated} peer fee profiles from {len(intel_reports)} reports")
        return updated

    def _estimate_elasticity(self, reports: List[Dict[str, Any]]) -> float:
        """
        Estimate price elasticity from volume delta observations.

        Elasticity interpretation:
        - Negative: Volume decreases when fees increase (elastic demand)
        - Positive: Volume increases when fees increase (inelastic)
        - Zero: No clear relationship

        Args:
            reports: List of fee intelligence reports

        Returns:
            Estimated elasticity (-1 to 1)
        """
        # Look for reports with volume delta observations
        deltas = []
        for report in reports:
            fee_change = report.get("last_fee_change_ppm", 0)
            volume_delta = report.get("volume_delta_pct", 0.0)

            # Only consider reports with fee changes
            if fee_change != 0 and report.get("our_fee_ppm", 0) > 0:
                # Normalize: if fee went up and volume went down, that's elastic
                fee_pct_change = fee_change / report.get("our_fee_ppm", 1)
                if fee_pct_change != 0:
                    elasticity_point = -volume_delta / fee_pct_change
                    deltas.append(elasticity_point)

        if not deltas:
            return 0.0  # No data, assume neutral

        # Average the elasticity estimates, bounded
        avg_elasticity = sum(deltas) / len(deltas)
        return max(-1.0, min(1.0, avg_elasticity))

    def _calculate_optimal_fee(
        self,
        avg_fee: float,
        elasticity: float,
        reporter_count: int
    ) -> int:
        """
        Calculate optimal fee recommendation.

        Uses elasticity to adjust from average:
        - High elasticity (negative): Lower fees to maximize volume
        - Low elasticity (positive): Higher fees for more revenue

        Args:
            avg_fee: Average fee charged by hive members
            elasticity: Estimated price elasticity
            reporter_count: Number of reporters (confidence proxy)

        Returns:
            Recommended optimal fee in ppm
        """
        base = avg_fee

        # Elasticity adjustment
        if elasticity < ELASTICITY_VERY_ELASTIC:
            # Very elastic: 70% of average
            elasticity_mult = 0.7
        elif elasticity < ELASTICITY_SOMEWHAT_ELASTIC:
            # Somewhat elastic: 85% of average
            elasticity_mult = 0.85
        else:
            # Inelastic: can go slightly above average
            elasticity_mult = 1.1

        optimal = int(base * elasticity_mult)

        # Bound the result
        return max(MIN_FEE_PPM, min(MAX_FEE_PPM, optimal))

    def _calculate_confidence(
        self,
        reports: List[Dict[str, Any]],
        reporter_count: int
    ) -> float:
        """
        Calculate confidence score for fee profile.

        Based on:
        - Number of reporters (more = higher confidence)
        - Data freshness (newer = higher confidence)
        - Volume of observations

        Args:
            reports: List of fee intelligence reports
            reporter_count: Number of unique reporters

        Returns:
            Confidence score (0-1)
        """
        if not reports:
            return 0.0

        # Reporter count factor (3+ reporters = full confidence from this factor)
        reporter_factor = min(1.0, reporter_count / 3.0)

        # Freshness factor (average age, newer is better)
        now = int(time.time())
        ages = [(now - r.get("timestamp", now)) / 3600 for r in reports]  # hours
        avg_age_hours = sum(ages) / len(ages) if ages else 24
        freshness_factor = max(0.0, 1.0 - (avg_age_hours / 24))  # Decay over 24h

        # Volume factor (more observations = higher confidence)
        total_forwards = sum(r.get("forward_count", 0) for r in reports)
        volume_factor = min(1.0, total_forwards / 100)  # 100+ forwards = full

        # Weighted average
        confidence = (
            reporter_factor * 0.4 +
            freshness_factor * 0.3 +
            volume_factor * 0.3
        )

        return round(confidence, 3)

    # =========================================================================
    # FEE RECOMMENDATIONS
    # =========================================================================

    def get_fee_recommendation(
        self,
        target_peer_id: str,
        our_channel_size: int = 0,
        our_health: int = 50
    ) -> Dict[str, Any]:
        """
        Get fee recommendation for an external peer.

        Incorporates:
        - Collective fee intelligence
        - NNLB health-based adjustments
        - Elasticity estimates

        Args:
            target_peer_id: External peer to get recommendation for
            our_channel_size: Our channel size to this peer (for context)
            our_health: Our health score (0-100) for NNLB adjustment

        Returns:
            Dict with recommendation and reasoning
        """
        profile = self.db.get_peer_fee_profile(target_peer_id)

        if not profile:
            return {
                "peer_id": target_peer_id,
                "recommended_fee_ppm": DEFAULT_BASE_FEE,
                "confidence": 0.0,
                "reasoning": "No fee intelligence available, using default",
                "source": "default"
            }

        base_fee = profile.get("optimal_fee_estimate", DEFAULT_BASE_FEE)
        elasticity = profile.get("estimated_elasticity", 0.0)
        confidence = profile.get("confidence", 0.0)

        # NNLB health adjustment
        if our_health < HEALTH_STRUGGLING:
            # Critical/struggling: lower fees to attract traffic
            health_mult = 0.7 + (our_health / 100 * 0.3)  # 0.7x to 0.85x
            health_reason = "lowered for NNLB (struggling node)"
        elif our_health > HEALTH_THRIVING:
            # Thriving: can yield to others
            health_mult = 1.0 + ((our_health - 75) / 100 * 0.15)  # 1.0x to 1.04x
            health_reason = "slightly raised (thriving, yielding to others)"
        else:
            health_mult = 1.0
            health_reason = "normal (healthy node)"

        recommended = int(base_fee * health_mult)
        recommended = max(MIN_FEE_PPM, min(MAX_FEE_PPM, recommended))

        return {
            "peer_id": target_peer_id,
            "recommended_fee_ppm": recommended,
            "base_optimal_fee": base_fee,
            "elasticity": elasticity,
            "confidence": confidence,
            "health_multiplier": round(health_mult, 3),
            "our_health": our_health,
            "reporter_count": profile.get("reporter_count", 0),
            "reasoning": f"Based on {profile.get('reporter_count', 0)} reporters, "
                        f"elasticity {elasticity:.2f}, {health_reason}",
            "source": "hive_intelligence"
        }

    # =========================================================================
    # HEALTH REPORTING
    # =========================================================================

    def create_health_report_message(
        self,
        overall_health: int,
        capacity_score: int,
        revenue_score: int,
        connectivity_score: int,
        rpc,
        needs_inbound: bool = False,
        needs_outbound: bool = False,
        needs_channels: bool = False,
        can_provide_assistance: bool = False,
        assistance_budget_sats: int = 0
    ) -> Optional[bytes]:
        """
        Create a signed HEALTH_REPORT message.

        Args:
            overall_health: Overall health score (0-100)
            capacity_score: Capacity score (0-100)
            revenue_score: Revenue score (0-100)
            connectivity_score: Connectivity score (0-100)
            rpc: RPC proxy for signmessage
            needs_inbound: Whether we need inbound liquidity
            needs_outbound: Whether we need outbound liquidity
            needs_channels: Whether we need more channels
            can_provide_assistance: Whether we can help others
            assistance_budget_sats: How much we can spend helping

        Returns:
            Serialized message bytes or None on error
        """
        if not self.our_pubkey:
            self._log("Cannot create health report: no pubkey set", level='warn')
            return None

        timestamp = int(time.time())

        # Build payload for signing
        payload = {
            "reporter_id": self.our_pubkey,
            "timestamp": timestamp,
            "overall_health": overall_health,
            "capacity_score": capacity_score,
            "revenue_score": revenue_score,
            "connectivity_score": connectivity_score,
        }

        # Sign the payload
        signing_msg = get_health_report_signing_payload(payload)
        try:
            sig_result = rpc.signmessage(signing_msg)
            signature = sig_result['zbase']
        except Exception as e:
            self._log(f"Failed to sign health report: {e}", level='error')
            return None

        return create_health_report(
            reporter_id=self.our_pubkey,
            timestamp=timestamp,
            signature=signature,
            overall_health=overall_health,
            capacity_score=capacity_score,
            revenue_score=revenue_score,
            connectivity_score=connectivity_score,
            needs_inbound=needs_inbound,
            needs_outbound=needs_outbound,
            needs_channels=needs_channels,
            can_provide_assistance=can_provide_assistance,
            assistance_budget_sats=assistance_budget_sats
        )

    def handle_health_report(
        self,
        sender_id: str,
        payload: Dict[str, Any],
        rpc
    ) -> Dict[str, Any]:
        """
        Handle incoming HEALTH_REPORT message.

        Args:
            sender_id: Peer who sent the message
            payload: Message payload
            rpc: RPC proxy for checkmessage

        Returns:
            Dict with result status
        """
        # Rate limit check
        if not self._check_rate_limit(
            sender_id, self._health_report_rate, HEALTH_REPORT_RATE_LIMIT
        ):
            self._log(f"Rate limited health report from {sender_id[:16]}...")
            return {"error": "rate_limited"}

        # Validate payload structure
        if not validate_health_report_payload(payload):
            self._log(f"Invalid health report payload from {sender_id[:16]}...")
            return {"error": "invalid_payload"}

        # Verify reporter matches sender
        reporter_id = payload.get("reporter_id")
        if reporter_id != sender_id:
            self._log(
                f"Health report reporter mismatch: {reporter_id[:16]}... != {sender_id[:16]}..."
            )
            return {"error": "reporter_mismatch"}

        # Verify reporter is a hive member
        member = self.db.get_member(reporter_id)
        if not member:
            self._log(f"Health report from non-member {reporter_id[:16]}...")
            return {"error": "not_a_member"}

        # Verify signature
        signature = payload.get("signature")
        signing_msg = get_health_report_signing_payload(payload)

        try:
            verify_result = rpc.checkmessage(signing_msg, signature)
            if not verify_result.get("verified"):
                self._log(f"Health report signature verification failed")
                return {"error": "invalid_signature"}
            if verify_result.get("pubkey") != reporter_id:
                self._log(f"Health report signature pubkey mismatch")
                return {"error": "signature_mismatch"}
        except Exception as e:
            self._log(f"Signature verification error: {e}", level='error')
            return {"error": "verification_failed"}

        # Record for rate limiting
        self._record_message(sender_id, self._health_report_rate)

        # Determine tier from health score
        overall_health = payload.get("overall_health", 50)
        if overall_health >= HEALTH_THRIVING:
            tier = "thriving"
            needs_help = False
            can_help = True
        elif overall_health >= HEALTH_HEALTHY:
            tier = "healthy"
            needs_help = False
            can_help = True
        elif overall_health >= HEALTH_STRUGGLING:
            tier = "struggling"
            needs_help = True
            can_help = False
        else:
            tier = "critical"
            needs_help = True
            can_help = False

        # Override with explicit flags if provided
        if payload.get("can_provide_assistance"):
            can_help = True

        # Store the health report
        self.db.update_member_health(
            peer_id=reporter_id,
            overall_health=overall_health,
            capacity_score=payload.get("capacity_score", 50),
            revenue_score=payload.get("revenue_score", 50),
            connectivity_score=payload.get("connectivity_score", 50),
            tier=tier,
            needs_help=needs_help or payload.get("needs_inbound") or
                       payload.get("needs_outbound") or payload.get("needs_channels"),
            can_help_others=can_help,
            needs_inbound=payload.get("needs_inbound", False),
            needs_outbound=payload.get("needs_outbound", False),
            needs_channels=payload.get("needs_channels", False),
            assistance_budget_sats=payload.get("assistance_budget_sats", 0)
        )

        self._log(
            f"Stored health report from {reporter_id[:16]}...: "
            f"health={overall_health}, tier={tier}"
        )

        return {"success": True, "tier": tier}

    # =========================================================================
    # NNLB UTILITIES
    # =========================================================================

    def get_nnlb_status(self) -> Dict[str, Any]:
        """
        Get NNLB (No Node Left Behind) status summary.

        Returns:
            Dict with NNLB statistics and member lists
        """
        all_health = self.db.get_all_member_health()
        struggling = self.db.get_struggling_members()
        helpers = self.db.get_helping_members()

        # Calculate tier distribution
        tier_counts = {"thriving": 0, "healthy": 0, "struggling": 0, "critical": 0}
        for h in all_health:
            tier = h.get("tier", "unknown")
            if tier in tier_counts:
                tier_counts[tier] += 1

        # Calculate average health
        if all_health:
            avg_health = sum(h.get("overall_health", 0) for h in all_health) / len(all_health)
        else:
            avg_health = 0

        return {
            "member_count": len(all_health),
            "average_health": round(avg_health, 1),
            "tier_distribution": tier_counts,
            "struggling_count": len(struggling),
            "helper_count": len(helpers),
            "struggling_members": [
                {"peer_id": s.get("peer_id"), "health": s.get("overall_health")}
                for s in struggling[:5]  # Top 5 most struggling
            ],
            "helper_members": [
                {"peer_id": h.get("peer_id"), "budget_sats": h.get("assistance_budget_sats", 0)}
                for h in helpers[:5]  # Top 5 helpers
            ]
        }

    def calculate_our_health(
        self,
        capacity_sats: int,
        available_sats: int,
        channel_count: int,
        daily_revenue_sats: int,
        hive_avg_capacity: int = 10_000_000,
        hive_avg_revenue: int = 1000
    ) -> Dict[str, Any]:
        """
        Calculate our node's health score.

        Args:
            capacity_sats: Our total channel capacity
            available_sats: Our available outbound liquidity
            channel_count: Number of our channels
            daily_revenue_sats: Our daily routing revenue
            hive_avg_capacity: Hive average capacity (for comparison)
            hive_avg_revenue: Hive average daily revenue

        Returns:
            Dict with health scores and tier
        """
        # Capacity score: compare to hive average
        if hive_avg_capacity > 0:
            capacity_score = min(100, int((capacity_sats / hive_avg_capacity) * 50))
        else:
            capacity_score = 50

        # Revenue score: compare to hive average
        if hive_avg_revenue > 0:
            revenue_score = min(100, int((daily_revenue_sats / hive_avg_revenue) * 50))
        else:
            revenue_score = 50

        # Connectivity score: based on channel count
        connectivity_score = min(100, channel_count * 10)

        # Balance score: how well-balanced are we
        if capacity_sats > 0:
            balance_ratio = available_sats / capacity_sats
            # Optimal is around 50%
            balance_deviation = abs(0.5 - balance_ratio)
            balance_score = max(0, int(100 - (balance_deviation * 200)))
        else:
            balance_score = 50

        # Overall weighted average
        overall = int(
            capacity_score * 0.25 +
            revenue_score * 0.30 +
            connectivity_score * 0.25 +
            balance_score * 0.20
        )

        # Determine tier
        if overall >= HEALTH_THRIVING:
            tier = "thriving"
            needs_help = False
            can_help = True
        elif overall >= HEALTH_HEALTHY:
            tier = "healthy"
            needs_help = False
            can_help = True
        elif overall >= HEALTH_STRUGGLING:
            tier = "struggling"
            needs_help = True
            can_help = False
        else:
            tier = "critical"
            needs_help = True
            can_help = False

        return {
            "overall_health": overall,
            "capacity_score": capacity_score,
            "revenue_score": revenue_score,
            "connectivity_score": connectivity_score,
            "balance_score": balance_score,
            "tier": tier,
            "needs_help": needs_help,
            "can_help_others": can_help
        }

    # =========================================================================
    # COMPETITOR FEE INTELLIGENCE (Phase 1: Query Integration)
    # =========================================================================
    # These methods expose aggregated fee profiles for cl-revenue-ops

    def get_aggregated_profile(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get aggregated fee profile for external query by cl-revenue-ops.

        Combines:
        - Aggregated fee data from peer_fee_profiles table
        - Market share calculation based on hive capacity to this peer
        - Fee volatility estimate from recent observations
        - Confidence weighting based on data freshness

        Args:
            peer_id: External peer to get profile for

        Returns:
            Dict with profile data or None if no data available:
            {
                "peer_id": "02abc...",
                "avg_fee_charged": 250,
                "min_fee": 100,
                "max_fee": 500,
                "fee_volatility": 0.15,
                "estimated_elasticity": -0.8,
                "optimal_fee_estimate": 180,
                "confidence": 0.75,
                "market_share": 0.12,
                "hive_capacity_sats": 6000000,
                "hive_reporters": 3,
                "last_updated": 1705000000
            }
        """
        profile = self.db.get_peer_fee_profile(peer_id)
        if not profile:
            return None

        # Get recent observations to calculate volatility
        reports = self.db.get_fee_intelligence_for_peer(peer_id, max_age_hours=72)
        volatility = self._calculate_fee_volatility(reports)

        # Calculate hive capacity to this peer from recent observations
        hive_capacity = sum(
            r.get("forward_volume_sats", 0)
            for r in reports
            if r.get("forward_volume_sats", 0) > 0
        )

        # Apply freshness decay to confidence
        now = int(time.time())
        last_update = profile.get("last_update", now)
        age_hours = (now - last_update) / 3600
        freshness_factor = max(0.1, 1.0 - (age_hours / 48))  # Decay over 48h

        return {
            "peer_id": peer_id,
            "avg_fee_charged": int(profile.get("avg_fee_charged", 0)),
            "min_fee": profile.get("min_fee_charged", 0),
            "max_fee": profile.get("max_fee_charged", 0),
            "fee_volatility": round(volatility, 3),
            "estimated_elasticity": round(profile.get("estimated_elasticity", 0), 3),
            "optimal_fee_estimate": profile.get("optimal_fee_estimate", 0),
            "confidence": round(profile.get("confidence", 0) * freshness_factor, 3),
            "market_share": 0.0,  # Calculated by caller with their data
            "hive_capacity_sats": hive_capacity,
            "hive_reporters": profile.get("reporter_count", 0),
            "last_updated": last_update
        }

    def get_all_profiles(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get all known peer profiles for batch query.

        Args:
            limit: Maximum number of profiles to return

        Returns:
            List of profile dicts (see get_aggregated_profile for format)
        """
        profiles = self.db.get_all_peer_fee_profiles(limit=limit)
        result = []

        for profile in profiles:
            peer_id = profile.get("peer_id")
            if not peer_id:
                continue

            # Skip profiles with no useful data
            if profile.get("reporter_count", 0) == 0:
                continue

            # Get recent observations for volatility
            reports = self.db.get_fee_intelligence_for_peer(peer_id, max_age_hours=72)
            volatility = self._calculate_fee_volatility(reports)

            # Calculate hive capacity
            hive_capacity = sum(
                r.get("forward_volume_sats", 0)
                for r in reports
                if r.get("forward_volume_sats", 0) > 0
            )

            # Apply freshness decay
            now = int(time.time())
            last_update = profile.get("last_update", now)
            age_hours = (now - last_update) / 3600
            freshness_factor = max(0.1, 1.0 - (age_hours / 48))

            result.append({
                "peer_id": peer_id,
                "avg_fee_charged": int(profile.get("avg_fee_charged", 0)),
                "min_fee": profile.get("min_fee_charged", 0),
                "max_fee": profile.get("max_fee_charged", 0),
                "fee_volatility": round(volatility, 3),
                "estimated_elasticity": round(profile.get("estimated_elasticity", 0), 3),
                "optimal_fee_estimate": profile.get("optimal_fee_estimate", 0),
                "confidence": round(profile.get("confidence", 0) * freshness_factor, 3),
                "market_share": 0.0,
                "hive_capacity_sats": hive_capacity,
                "hive_reporters": profile.get("reporter_count", 0),
                "last_updated": last_update
            })

        return result

    def _calculate_fee_volatility(self, reports: List[Dict[str, Any]]) -> float:
        """
        Calculate fee volatility from recent observations.

        Volatility is the standard deviation of fees divided by mean.
        Higher volatility indicates the peer changes fees frequently.

        Args:
            reports: List of fee intelligence reports

        Returns:
            Volatility coefficient (0.0 = stable, 1.0+ = high volatility)
        """
        if len(reports) < 2:
            return 0.0

        fees = [r.get("our_fee_ppm", 0) for r in reports if r.get("our_fee_ppm", 0) > 0]
        if len(fees) < 2:
            return 0.0

        mean_fee = sum(fees) / len(fees)
        if mean_fee == 0:
            return 0.0

        variance = sum((f - mean_fee) ** 2 for f in fees) / len(fees)
        std_dev = variance ** 0.5

        return min(1.0, std_dev / mean_fee)  # Cap at 1.0

    # =========================================================================
    # LOCAL OBSERVATION STORAGE (Phase 2: Bidirectional Sharing)
    # =========================================================================
    # These methods handle observations reported by local cl-revenue-ops

    def store_local_observation(
        self,
        target_peer_id: str,
        our_fee_ppm: int,
        their_fee_ppm: Optional[int] = None,
        forward_count: int = 0,
        forward_volume_sats: int = 0,
        revenue_rate: float = 0.0,
        flow_direction: str = "balanced",
        utilization_pct: float = 0.0,
        timestamp: Optional[int] = None
    ) -> int:
        """
        Store a local fee observation from cl-revenue-ops.

        Unlike handle_fee_intelligence() which processes signed messages from
        remote hive members, this method stores local observations directly.
        These observations are used for local aggregation and can optionally
        be broadcast to hive members.

        Args:
            target_peer_id: External peer being observed
            our_fee_ppm: Our fee toward this peer
            their_fee_ppm: Their fee toward us (optional)
            forward_count: Number of forwards in period
            forward_volume_sats: Volume routed in period
            revenue_rate: Calculated revenue rate (sats/hour)
            flow_direction: 'source', 'sink', or 'balanced'
            utilization_pct: Channel utilization (0.0-1.0)
            timestamp: Observation timestamp (defaults to now)

        Returns:
            Observation ID from database
        """
        if timestamp is None:
            timestamp = int(time.time())

        # Calculate revenue from volume if not provided
        revenue_sats = int(revenue_rate * 1.0)  # Assuming 1 hour period
        if forward_volume_sats > 0 and our_fee_ppm > 0:
            revenue_sats = (forward_volume_sats * our_fee_ppm) // 1_000_000

        # Store using our own pubkey as reporter
        # Use "local" signature to distinguish from remote reports
        observation_id = self.db.store_fee_intelligence(
            reporter_id=self.our_pubkey or "local",
            target_peer_id=target_peer_id,
            timestamp=timestamp,
            our_fee_ppm=our_fee_ppm,
            their_fee_ppm=their_fee_ppm or 0,
            forward_count=forward_count,
            forward_volume_sats=forward_volume_sats,
            revenue_sats=revenue_sats,
            flow_direction=flow_direction,
            utilization_pct=utilization_pct,
            signature="local",  # Mark as local observation
            last_fee_change_ppm=0,
            volume_delta_pct=0.0,
            days_observed=1
        )

        self._log(
            f"Stored local observation for {target_peer_id[:16]}... "
            f"(fee={our_fee_ppm}, volume={forward_volume_sats}, forwards={forward_count})",
            level='debug'
        )

        return observation_id
