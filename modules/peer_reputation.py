"""
Peer Reputation Module (Phase 5 - Advanced Cooperation)

Implements collective reputation tracking for external peers:
- Aggregation of reputation reports from multiple hive members
- Outlier detection to prevent manipulation
- Reputation scoring with confidence levels
- Warning propagation and tracking

Security: All reputation reports require cryptographic signatures.
Skepticism: No single reporter can significantly impact aggregated scores.
"""

import time
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from collections import defaultdict

from .protocol import (
    HiveMessageType,
    serialize,
    create_peer_reputation_snapshot,
    validate_peer_reputation_snapshot_payload,
    get_peer_reputation_snapshot_signing_payload,
    PEER_REPUTATION_SNAPSHOT_RATE_LIMIT,
    MAX_PEERS_IN_REPUTATION_SNAPSHOT,
    MAX_WARNINGS_COUNT,
    VALID_WARNINGS,
)


# Aggregation thresholds
MIN_REPORTERS_FOR_CONFIDENCE = 3    # Minimum reporters for high confidence
OUTLIER_DEVIATION_THRESHOLD = 0.2   # 20% deviation from median is outlier
REPUTATION_STALENESS_HOURS = 168    # 7 days staleness window
OUR_DATA_WEIGHT = 2                 # Weight our own data 2x vs others


@dataclass
class AggregatedReputation:
    """Aggregated reputation for an external peer."""
    peer_id: str

    # Aggregated metrics (from multiple reporters)
    avg_uptime: float = 1.0
    avg_htlc_success: float = 1.0
    avg_fee_stability: float = 1.0
    avg_response_time_ms: int = 0
    total_force_closes: int = 0

    # Reporter information
    reporters: Set[str] = field(default_factory=set)
    report_count: int = 0

    # Aggregated warnings
    warnings: Dict[str, int] = field(default_factory=dict)  # warning -> count

    # Confidence and timestamps
    confidence: str = "low"  # 'low', 'medium', 'high'
    last_update: int = 0
    oldest_report: int = 0

    # Overall score (0-100)
    reputation_score: int = 50


@dataclass
class ReputationReport:
    """Single reputation report from a hive member."""
    reporter_id: str
    peer_id: str
    timestamp: int
    uptime_pct: float
    response_time_ms: int
    force_close_count: int
    fee_stability: float
    htlc_success_rate: float
    channel_age_days: int
    total_routed_sats: int
    warnings: List[str]
    observation_days: int


class PeerReputationManager:
    """
    Manage collective reputation data for external peers.

    Aggregates reputation reports from hive members while
    applying skepticism to prevent manipulation.
    """

    def __init__(
        self,
        database: Any,
        plugin: Any,
        our_pubkey: str
    ):
        """
        Initialize the reputation manager.

        Args:
            database: HiveDatabase instance
            plugin: Plugin instance for RPC/logging
            our_pubkey: Our node's pubkey
        """
        self.database = database
        self.plugin = plugin
        self.our_pubkey = our_pubkey

        # In-memory aggregated reputations
        # Key: peer_id
        self._aggregated: Dict[str, AggregatedReputation] = {}

        # Rate limiting for snapshots
        self._snapshot_rate: Dict[str, List[float]] = defaultdict(list)

    def _check_rate_limit(
        self,
        sender: str,
        rate_tracker: Dict[str, List[float]],
        limit: tuple
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

    def create_reputation_snapshot_message(
        self,
        peers: List[Dict[str, Any]],
        rpc: Any
    ) -> Optional[bytes]:
        """
        Create a signed PEER_REPUTATION_SNAPSHOT message.

        This is the preferred method for sharing peer reputation. Instead of
        sending N individual messages for N peers, send one snapshot with all
        peer observations.

        Args:
            peers: List of peer observations, each containing:
                - peer_id: External peer being reported on
                - uptime_pct: Peer uptime (0-1)
                - response_time_ms: Average HTLC response time
                - force_close_count: Force closes by peer
                - fee_stability: Fee stability (0-1)
                - htlc_success_rate: HTLC success rate (0-1)
                - channel_age_days: Channel age
                - total_routed_sats: Total volume routed
                - warnings: Warning codes list
                - observation_days: Days covered
            rpc: RPC interface for signing

        Returns:
            Serialized message bytes, or None on error
        """
        if not self.our_pubkey:
            if self.plugin:
                self.plugin.log(
                    "cl-hive: Cannot create reputation snapshot: no pubkey set",
                    level='warn'
                )
            return None

        if not peers:
            if self.plugin:
                self.plugin.log(
                    "cl-hive: Cannot create reputation snapshot: no peers",
                    level='warn'
                )
            return None

        if len(peers) > MAX_PEERS_IN_REPUTATION_SNAPSHOT:
            if self.plugin:
                self.plugin.log(
                    f"cl-hive: Too many peers in snapshot ({len(peers)} > {MAX_PEERS_IN_REPUTATION_SNAPSHOT}), truncating",
                    level='warn'
                )
            peers = peers[:MAX_PEERS_IN_REPUTATION_SNAPSHOT]

        timestamp = int(time.time())

        # Build payload for signing
        payload = {
            "reporter_id": self.our_pubkey,
            "timestamp": timestamp,
            "peers": peers,
        }

        # Sign the payload
        signing_msg = get_peer_reputation_snapshot_signing_payload(payload)
        try:
            sig_result = rpc.signmessage(signing_msg)
            signature = sig_result['zbase']
        except Exception as e:
            if self.plugin:
                self.plugin.log(
                    f"cl-hive: Failed to sign peer reputation snapshot: {e}",
                    level='error'
                )
            return None

        return create_peer_reputation_snapshot(
            reporter_id=self.our_pubkey,
            timestamp=timestamp,
            signature=signature,
            peers=peers
        )

    def handle_peer_reputation_snapshot(
        self,
        peer_id: str,
        payload: Dict[str, Any],
        rpc: Any
    ) -> Dict[str, Any]:
        """
        Handle incoming PEER_REPUTATION_SNAPSHOT message.

        Validates signature and stores reputation for all peers in the snapshot.
        This is the preferred method - one message contains all peer observations.

        Args:
            peer_id: Sender peer ID
            payload: Message payload containing peers list
            rpc: RPC interface for signature verification

        Returns:
            Result dict with success/error
        """
        # Validate payload structure
        if not validate_peer_reputation_snapshot_payload(payload):
            return {"error": "invalid payload"}

        reporter_id = payload.get("reporter_id")

        # Identity binding: sender must match reporter
        if peer_id != reporter_id:
            return {"error": "identity binding failed"}

        # Verify sender is a hive member
        member = self.database.get_member(reporter_id)
        if not member:
            return {"error": "reporter not a member"}

        # Rate limit check
        if not self._check_rate_limit(
            reporter_id,
            self._snapshot_rate,
            PEER_REPUTATION_SNAPSHOT_RATE_LIMIT
        ):
            return {"error": "rate limited"}

        # Verify signature
        signature = payload.get("signature")
        if not signature:
            return {"error": "missing signature"}

        signing_message = get_peer_reputation_snapshot_signing_payload(payload)

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

        # Store reputation for each peer
        peers = payload.get("peers", [])
        timestamp = payload.get("timestamp", int(time.time()))
        stored_count = 0

        for peer_data in peers:
            target_peer_id = peer_data.get("peer_id")
            if not target_peer_id:
                continue

            self.database.store_peer_reputation(
                reporter_id=reporter_id,
                peer_id=target_peer_id,
                timestamp=timestamp,
                uptime_pct=peer_data.get("uptime_pct", 1.0),
                response_time_ms=peer_data.get("response_time_ms", 0),
                force_close_count=peer_data.get("force_close_count", 0),
                fee_stability=peer_data.get("fee_stability", 1.0),
                htlc_success_rate=peer_data.get("htlc_success_rate", 1.0),
                channel_age_days=peer_data.get("channel_age_days", 0),
                total_routed_sats=peer_data.get("total_routed_sats", 0),
                warnings=peer_data.get("warnings", []),
                observation_days=peer_data.get("observation_days", 7)
            )

            # Update aggregation for this peer
            self._update_aggregation(target_peer_id)
            stored_count += 1

        if self.plugin:
            self.plugin.log(
                f"cl-hive: Peer reputation snapshot from {reporter_id[:16]}... "
                f"with {stored_count} peer observations",
                level='debug'
            )

        return {"success": True, "peers_stored": stored_count}

    def _update_aggregation(self, peer_id: str):
        """Update aggregated reputation for a peer."""
        reports = self.database.get_peer_reputation_reports(
            peer_id,
            max_age_hours=REPUTATION_STALENESS_HOURS
        )

        if not reports:
            if peer_id in self._aggregated:
                del self._aggregated[peer_id]
            return

        # Apply skepticism: filter outliers
        filtered = self._filter_outliers(reports)

        if not filtered:
            return

        # Weight our own data higher
        weighted_reports = []
        for r in filtered:
            weighted_reports.append(r)
            if r.get("reporter_id") == self.our_pubkey:
                # Add our data twice for 2x weight
                weighted_reports.append(r)

        # Calculate aggregates
        uptimes = [r.get("uptime_pct", 1.0) for r in weighted_reports]
        htlc_rates = [r.get("htlc_success_rate", 1.0) for r in weighted_reports]
        fee_stabilities = [r.get("fee_stability", 1.0) for r in weighted_reports]
        response_times = [r.get("response_time_ms", 0) for r in weighted_reports]
        force_closes = max((r.get("force_close_count", 0) for r in filtered), default=0)

        # Aggregate warnings
        warnings_count: Dict[str, int] = defaultdict(int)
        for r in filtered:
            for warning in r.get("warnings", []):
                if warning in VALID_WARNINGS:
                    warnings_count[warning] += 1

        # Determine confidence
        unique_reporters = set(r.get("reporter_id") for r in filtered if r.get("reporter_id"))
        if len(unique_reporters) >= MIN_REPORTERS_FOR_CONFIDENCE:
            confidence = "high"
        elif len(unique_reporters) >= 2:
            confidence = "medium"
        else:
            confidence = "low"

        # Calculate overall score (0-100)
        avg_uptime = statistics.mean(uptimes) if uptimes else 1.0
        avg_htlc = statistics.mean(htlc_rates) if htlc_rates else 1.0
        avg_fee_stability = statistics.mean(fee_stabilities) if fee_stabilities else 1.0

        # Score components
        uptime_score = avg_uptime * 30
        htlc_score = avg_htlc * 30
        fee_score = avg_fee_stability * 20

        # Penalty for force closes (max 20 points penalty)
        force_close_penalty = min(20, force_closes * 5)

        # Penalty for warnings (max 10 points)
        warning_penalty = min(10, len(warnings_count) * 2)

        reputation_score = int(
            uptime_score + htlc_score + fee_score -
            force_close_penalty - warning_penalty
        )
        reputation_score = max(0, min(100, reputation_score))

        timestamps = [r.get("timestamp", 0) for r in filtered]

        self._aggregated[peer_id] = AggregatedReputation(
            peer_id=peer_id,
            avg_uptime=avg_uptime,
            avg_htlc_success=avg_htlc,
            avg_fee_stability=avg_fee_stability,
            avg_response_time_ms=int(statistics.mean(response_times)) if response_times else 0,
            total_force_closes=force_closes,
            reporters=unique_reporters,
            report_count=len(filtered),
            warnings=dict(warnings_count),
            confidence=confidence,
            last_update=max(timestamps) if timestamps else 0,
            oldest_report=min(timestamps) if timestamps else 0,
            reputation_score=reputation_score
        )

    def _filter_outliers(
        self,
        reports: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Filter outlier reports to prevent manipulation.

        Uses median-based outlier detection.
        """
        if len(reports) < 3:
            return reports  # Not enough data for outlier detection

        # Calculate median uptime
        uptimes = [r.get("uptime_pct", 1.0) for r in reports]
        median_uptime = statistics.median(uptimes)

        # Calculate median HTLC success
        htlcs = [r.get("htlc_success_rate", 1.0) for r in reports]
        median_htlc = statistics.median(htlcs)

        # Filter reports that deviate significantly from median
        filtered = []
        for r in reports:
            uptime_dev = abs(r.get("uptime_pct", 1.0) - median_uptime)
            htlc_dev = abs(r.get("htlc_success_rate", 1.0) - median_htlc)

            # Keep if within threshold or it's our own data
            if (uptime_dev <= OUTLIER_DEVIATION_THRESHOLD and
                htlc_dev <= OUTLIER_DEVIATION_THRESHOLD):
                filtered.append(r)
            elif r.get("reporter_id") == self.our_pubkey:
                # Always trust our own data
                filtered.append(r)

        return filtered if filtered else reports

    def get_reputation(self, peer_id: str) -> Optional[AggregatedReputation]:
        """
        Get aggregated reputation for a peer.

        Args:
            peer_id: External peer pubkey

        Returns:
            AggregatedReputation if available, None otherwise
        """
        return self._aggregated.get(peer_id)

    def get_all_reputations(self) -> Dict[str, AggregatedReputation]:
        """Get all aggregated reputations."""
        return dict(self._aggregated)

    def get_peers_with_warnings(self) -> List[AggregatedReputation]:
        """Get peers that have active warnings."""
        return [
            rep for rep in self._aggregated.values()
            if rep.warnings
        ]

    def get_low_reputation_peers(
        self,
        threshold: int = 40
    ) -> List[AggregatedReputation]:
        """
        Get peers with reputation below threshold.

        Args:
            threshold: Minimum reputation score

        Returns:
            List of low-reputation peers
        """
        return [
            rep for rep in self._aggregated.values()
            if rep.reputation_score < threshold
        ]

    def get_reputation_stats(self) -> Dict[str, Any]:
        """
        Get overall reputation tracking statistics.

        Returns:
            Dict with reputation statistics
        """
        total_peers = len(self._aggregated)

        if not self._aggregated:
            return {
                "total_peers_tracked": 0,
                "high_confidence_count": 0,
                "low_reputation_count": 0,
                "peers_with_warnings": 0,
                "avg_reputation_score": 0,
            }

        high_confidence = sum(
            1 for r in self._aggregated.values()
            if r.confidence == "high"
        )

        low_reputation = len(self.get_low_reputation_peers())

        with_warnings = len(self.get_peers_with_warnings())

        avg_score = statistics.mean(
            r.reputation_score for r in self._aggregated.values()
        )

        return {
            "total_peers_tracked": total_peers,
            "high_confidence_count": high_confidence,
            "low_reputation_count": low_reputation,
            "peers_with_warnings": with_warnings,
            "avg_reputation_score": round(avg_score, 1),
        }

    def aggregate_from_database(self):
        """
        Rebuild aggregations from database reports.

        Used on startup or after clearing in-memory data.
        """
        # Get all unique peers with reports
        all_reports = self.database.get_all_peer_reputation_reports(
            max_age_hours=REPUTATION_STALENESS_HOURS
        )

        # Group by peer_id
        peers = set(r.get("peer_id") for r in all_reports if r.get("peer_id"))

        for peer_id in peers:
            self._update_aggregation(peer_id)

    def cleanup_stale_data(self) -> int:
        """
        Remove stale aggregations.

        Returns:
            Number of cleaned entries
        """
        now = time.time()
        stale_cutoff = now - (REPUTATION_STALENESS_HOURS * 3600)

        stale_peers = [
            peer_id for peer_id, rep in self._aggregated.items()
            if rep.last_update < stale_cutoff
        ]

        for peer_id in stale_peers:
            del self._aggregated[peer_id]

        return len(stale_peers)
