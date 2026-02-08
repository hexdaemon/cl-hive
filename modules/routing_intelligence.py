"""
Routing Intelligence Module (Phase 4 - Cooperative Routing)

Implements collective routing intelligence for the hive:
- Route probe aggregation and analysis
- Best route suggestions based on collective observations
- Path success rate tracking
- Hive-aware route optimization

Security: All route probes require cryptographic signatures.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from .protocol import (
    HiveMessageType,
    serialize,
    create_route_probe,
    create_route_probe_batch,
    validate_route_probe_payload,
    validate_route_probe_batch_payload,
    get_route_probe_signing_payload,
    get_route_probe_batch_signing_payload,
    ROUTE_PROBE_RATE_LIMIT,
    ROUTE_PROBE_BATCH_RATE_LIMIT,
    MAX_PATH_LENGTH,
    MAX_PROBES_IN_BATCH,
)
from . import network_metrics


# Route quality thresholds
HIGH_SUCCESS_RATE = 0.9     # 90% success rate considered high
LOW_SUCCESS_RATE = 0.5      # Below 50% considered unreliable
MAX_PROBES_PER_PATH = 100   # Max probes to track per path
PROBE_STALENESS_HOURS = 24  # Probes older than this are stale

# Centrality-aware routing (Use Case 7)
CENTRALITY_WEIGHT_IN_ROUTING = 0.15  # 15% weight for centrality in route score
HIGH_CENTRALITY_ROUTING_BONUS = 1.2  # 20% bonus for paths with high-centrality members
MIN_CENTRALITY_FOR_FALLBACK = 0.4    # Minimum centrality to consider for fallback paths


@dataclass
class RouteSuggestion:
    """A suggested route to a destination."""
    destination: str
    path: List[str]
    expected_fee_ppm: int
    expected_latency_ms: int
    success_rate: float
    confidence: float
    last_successful_probe: int
    hive_hop_count: int  # Number of hive members in path
    # Centrality-aware routing (Use Case 7)
    path_centrality_score: float = 0.0  # Average centrality of hive members in path
    is_high_centrality_path: bool = False  # True if path includes high-centrality members


@dataclass
class PathStats:
    """Aggregated statistics for a specific path."""
    path: Tuple[str, ...]  # Immutable path tuple
    destination: str
    probe_count: int = 0
    success_count: int = 0
    total_latency_ms: int = 0
    total_fee_ppm: int = 0
    last_success_time: int = 0
    last_failure_time: int = 0
    last_failure_reason: str = ""
    avg_capacity_sats: int = 0
    reporters: set = field(default_factory=set)


class HiveRoutingMap:
    """
    Collective routing intelligence from all hive members.

    Each member contributes route probe observations; all benefit
    from the aggregated routing knowledge.
    """

    def __init__(
        self,
        database: Any,
        plugin: Any,
        our_pubkey: str
    ):
        """
        Initialize the routing map.

        Args:
            database: HiveDatabase instance
            plugin: Plugin instance for RPC/logging
            our_pubkey: Our node's pubkey
        """
        self.database = database
        self.plugin = plugin
        self.our_pubkey = our_pubkey

        # In-memory path statistics
        # Key: (destination, path_tuple)
        self._path_stats: Dict[Tuple[str, Tuple[str, ...]], PathStats] = {}

        # Rate limiting
        self._probe_rate: Dict[str, List[float]] = defaultdict(list)
        self._batch_rate: Dict[str, List[float]] = defaultdict(list)

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

    def create_route_probe_message(
        self,
        destination: str,
        path: List[str],
        success: bool,
        latency_ms: int,
        rpc: Any,
        failure_reason: str = "",
        failure_hop: int = -1,
        estimated_capacity_sats: int = 0,
        total_fee_ppm: int = 0,
        per_hop_fees: List[int] = None,
        amount_probed_sats: int = 0
    ) -> Optional[bytes]:
        """
        Create a signed ROUTE_PROBE message.

        Args:
            destination: Final destination pubkey
            path: List of intermediate hop pubkeys
            success: Whether probe succeeded
            latency_ms: Round-trip time
            rpc: RPC interface for signing
            failure_reason: Reason for failure
            failure_hop: Index of failing hop
            estimated_capacity_sats: Route capacity estimate
            total_fee_ppm: Total route fees
            per_hop_fees: Fee at each hop
            amount_probed_sats: Amount probed

        Returns:
            Serialized message bytes, or None on error
        """
        try:
            return create_route_probe(
                reporter_id=self.our_pubkey,
                destination=destination,
                path=path,
                success=success,
                latency_ms=latency_ms,
                rpc=rpc,
                failure_reason=failure_reason,
                failure_hop=failure_hop,
                estimated_capacity_sats=estimated_capacity_sats,
                total_fee_ppm=total_fee_ppm,
                per_hop_fees=per_hop_fees,
                amount_probed_sats=amount_probed_sats
            )
        except Exception as e:
            if self.plugin:
                self.plugin.log(
                    f"cl-hive: Failed to create route probe message: {e}",
                    level='warn'
                )
            return None

    def handle_route_probe(
        self,
        peer_id: str,
        payload: Dict[str, Any],
        rpc: Any
    ) -> Dict[str, Any]:
        """
        Handle incoming ROUTE_PROBE message.

        Args:
            peer_id: Sender peer ID
            payload: Message payload
            rpc: RPC interface for signature verification

        Returns:
            Result dict with success/error
        """
        # Validate payload structure
        if not validate_route_probe_payload(payload):
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
            self._probe_rate,
            ROUTE_PROBE_RATE_LIMIT
        ):
            return {"error": "rate limited"}

        # Verify signature
        signature = payload.get("signature")
        if not signature:
            return {"error": "missing signature"}

        signing_message = get_route_probe_signing_payload(payload)

        try:
            verify_result = rpc.checkmessage(signing_message, signature)
            if not verify_result.get("verified"):
                return {"error": "signature verification failed"}

            if verify_result.get("pubkey") != reporter_id:
                return {"error": "signature pubkey mismatch"}
        except Exception as e:
            return {"error": f"signature check failed: {e}"}

        # Record rate limit
        self._record_message(reporter_id, self._probe_rate)

        # Extract probe data
        destination = payload.get("destination", "")
        path = tuple(payload.get("path", []))
        success = payload.get("success", False)
        latency_ms = payload.get("latency_ms", 0)
        failure_reason = payload.get("failure_reason", "")
        total_fee_ppm = payload.get("total_fee_ppm", 0)
        estimated_capacity = payload.get("estimated_capacity_sats", 0)
        timestamp = payload.get("timestamp", int(time.time()))

        # Update path statistics
        self._update_path_stats(
            destination=destination,
            path=path,
            success=success,
            latency_ms=latency_ms,
            fee_ppm=total_fee_ppm,
            capacity_sats=estimated_capacity,
            reporter_id=reporter_id,
            failure_reason=failure_reason,
            timestamp=timestamp
        )

        # Store in database
        self.database.store_route_probe(
            reporter_id=reporter_id,
            destination=destination,
            path=list(path),
            success=success,
            latency_ms=latency_ms,
            failure_reason=failure_reason,
            failure_hop=payload.get("failure_hop", -1),
            estimated_capacity_sats=estimated_capacity,
            total_fee_ppm=total_fee_ppm,
            amount_probed_sats=payload.get("amount_probed_sats", 0),
            timestamp=timestamp
        )

        if self.plugin:
            result_str = "success" if success else f"failed ({failure_reason})"
            self.plugin.log(
                f"cl-hive: Route probe from {reporter_id[:16]}... to {destination[:16]}...: {result_str}",
                level='debug'
            )

        return {"success": True, "stored": True}

    def handle_route_probe_batch(
        self,
        peer_id: str,
        payload: Dict[str, Any],
        rpc: Any
    ) -> Dict[str, Any]:
        """
        Handle incoming ROUTE_PROBE_BATCH message.

        This is the preferred method for receiving route probes - one message
        contains multiple probe observations instead of N individual messages.

        Args:
            peer_id: Sender peer ID
            payload: Message payload
            rpc: RPC interface for signature verification

        Returns:
            Result dict with success/error
        """
        # Validate payload structure
        if not validate_route_probe_batch_payload(payload):
            return {"error": "invalid payload"}

        reporter_id = payload.get("reporter_id")

        # Identity binding: sender must match reporter (prevent relay attacks)
        if peer_id != reporter_id:
            return {"error": "identity binding failed"}

        # Verify sender is a hive member
        member = self.database.get_member(reporter_id)
        if not member:
            return {"error": "reporter not a member"}

        # Rate limit check for batch messages
        if not self._check_rate_limit(
            reporter_id,
            self._batch_rate,
            ROUTE_PROBE_BATCH_RATE_LIMIT
        ):
            return {"error": "rate limited"}

        # Verify signature
        signature = payload.get("signature")
        if not signature:
            return {"error": "missing signature"}

        signing_message = get_route_probe_batch_signing_payload(payload)

        try:
            verify_result = rpc.checkmessage(signing_message, signature)
            if not verify_result.get("verified"):
                return {"error": "signature verification failed"}

            if verify_result.get("pubkey") != reporter_id:
                return {"error": "signature pubkey mismatch"}
        except Exception as e:
            return {"error": f"signature check failed: {e}"}

        # Record rate limit
        self._record_message(reporter_id, self._batch_rate)

        # Process each probe in the batch
        probes = payload.get("probes", [])
        stored_count = 0
        batch_timestamp = payload.get("timestamp", int(time.time()))

        for probe_data in probes:
            destination = probe_data.get("destination", "")
            path = tuple(probe_data.get("path", []))
            success = probe_data.get("success", False)
            latency_ms = probe_data.get("latency_ms", 0)
            failure_reason = probe_data.get("failure_reason", "")
            total_fee_ppm = probe_data.get("total_fee_ppm", 0)
            estimated_capacity = probe_data.get("estimated_capacity_sats", 0)

            # Update path statistics
            self._update_path_stats(
                destination=destination,
                path=path,
                success=success,
                latency_ms=latency_ms,
                fee_ppm=total_fee_ppm,
                capacity_sats=estimated_capacity,
                reporter_id=reporter_id,
                failure_reason=failure_reason,
                timestamp=batch_timestamp
            )

            # Store in database
            self.database.store_route_probe(
                reporter_id=reporter_id,
                destination=destination,
                path=list(path),
                success=success,
                latency_ms=latency_ms,
                failure_reason=failure_reason,
                failure_hop=probe_data.get("failure_hop", -1),
                estimated_capacity_sats=estimated_capacity,
                total_fee_ppm=total_fee_ppm,
                amount_probed_sats=probe_data.get("amount_probed_sats", 0),
                timestamp=batch_timestamp
            )

            stored_count += 1

        if self.plugin:
            self.plugin.log(
                f"cl-hive: Route probe batch from {reporter_id[:16]}... "
                f"with {stored_count} probe observations",
                level='debug'
            )

        return {"success": True, "probes_stored": stored_count}

    def create_route_probe_batch_message(
        self,
        probes: List[Dict[str, Any]],
        rpc: Any
    ) -> Optional[bytes]:
        """
        Create a signed ROUTE_PROBE_BATCH message.

        This is the preferred method for sharing route probes. Instead of
        sending N individual messages for N probes, send one batch with all
        probe observations.

        Args:
            probes: List of probe observations, each containing:
                - destination: Final destination pubkey
                - path: List of intermediate hop pubkeys
                - success: Whether probe succeeded
                - latency_ms: Round-trip time in milliseconds
                - failure_reason: Reason for failure (if any)
                - failure_hop: Index of failing hop (if any)
                - estimated_capacity_sats: Estimated route capacity
                - total_fee_ppm: Total fees for route
                - per_hop_fees: Fee at each hop
                - amount_probed_sats: Amount that was probed
            rpc: RPC interface for signing

        Returns:
            Serialized message bytes, or None on error
        """
        # Enforce bounds
        if len(probes) > MAX_PROBES_IN_BATCH:
            if self.plugin:
                self.plugin.log(
                    f"cl-hive: Route probe batch too large ({len(probes)} > {MAX_PROBES_IN_BATCH})",
                    level='warn'
                )
            return None

        timestamp = int(time.time())

        # Build payload for signing
        payload = {
            "reporter_id": self.our_pubkey,
            "timestamp": timestamp,
            "probes": probes,
        }

        # Sign the payload
        signing_message = get_route_probe_batch_signing_payload(payload)

        try:
            sig_result = rpc.signmessage(signing_message)
            signature = sig_result["zbase"]
        except Exception as e:
            if self.plugin:
                self.plugin.log(
                    f"cl-hive: Failed to sign route probe batch: {e}",
                    level='warn'
                )
            return None

        return create_route_probe_batch(
            reporter_id=self.our_pubkey,
            timestamp=timestamp,
            signature=signature,
            probes=probes
        )

    def _update_path_stats(
        self,
        destination: str,
        path: Tuple[str, ...],
        success: bool,
        latency_ms: int,
        fee_ppm: int,
        capacity_sats: int,
        reporter_id: str,
        failure_reason: str,
        timestamp: int
    ):
        """Update aggregated statistics for a path."""
        key = (destination, path)

        if key not in self._path_stats:
            self._path_stats[key] = PathStats(
                path=path,
                destination=destination
            )

        stats = self._path_stats[key]
        stats.probe_count += 1
        stats.reporters.add(reporter_id)

        if success:
            stats.success_count += 1
            stats.total_latency_ms += latency_ms
            stats.total_fee_ppm += fee_ppm
            stats.last_success_time = timestamp

            # Update capacity (weighted average)
            if capacity_sats > 0:
                if stats.avg_capacity_sats == 0:
                    stats.avg_capacity_sats = capacity_sats
                else:
                    stats.avg_capacity_sats = int(
                        stats.avg_capacity_sats * 0.7 + capacity_sats * 0.3
                    )
        else:
            stats.last_failure_time = timestamp
            stats.last_failure_reason = failure_reason

    def get_path_success_rate(self, path: List[str]) -> float:
        """
        Get the success rate for a specific path.

        Args:
            path: List of hop pubkeys

        Returns:
            Success rate (0.0 to 1.0)
        """
        path_tuple = tuple(path)

        # Look for this path to any destination
        for (dest, p), stats in self._path_stats.items():
            if p == path_tuple and stats.probe_count > 0:
                return stats.success_count / stats.probe_count

        return 0.5  # Unknown path, return neutral

    def get_path_confidence(self, path: List[str]) -> float:
        """
        Get confidence level for path data based on reporter count and recency.

        Args:
            path: List of hop pubkeys

        Returns:
            Confidence score (0.0 to 1.0)
        """
        path_tuple = tuple(path)
        now = time.time()
        stale_cutoff = now - (PROBE_STALENESS_HOURS * 3600)

        for (dest, p), stats in self._path_stats.items():
            if p == path_tuple:
                # Base confidence on reporter diversity
                reporter_factor = min(1.0, len(stats.reporters) / 3.0)

                # Recency factor
                last_probe = max(stats.last_success_time, stats.last_failure_time)
                if last_probe < stale_cutoff:
                    recency_factor = 0.3  # Stale data
                else:
                    recency_factor = 1.0

                # Probe count factor
                count_factor = min(1.0, stats.probe_count / 10.0)

                return reporter_factor * recency_factor * count_factor

        return 0.0  # No data

    def _get_path_centrality_score(
        self,
        path: List[str],
        hive_members: set
    ) -> Tuple[float, bool]:
        """
        Calculate centrality score for a routing path.

        Returns:
            Tuple of (average_centrality, is_high_centrality)
        """
        calculator = network_metrics.get_calculator()
        if not calculator:
            return 0.0, False

        centrality_scores = []
        for hop in path:
            if hop in hive_members:
                metrics = calculator.get_member_metrics(hop)
                if metrics:
                    centrality_scores.append(metrics.hive_centrality)

        if not centrality_scores:
            return 0.0, False

        avg_centrality = sum(centrality_scores) / len(centrality_scores)
        is_high = max(centrality_scores) >= 0.6  # High if any hop has centrality >= 0.6

        return avg_centrality, is_high

    def get_best_route_to(
        self,
        destination: str,
        amount_sats: int,
        hive_members: set = None,
        use_centrality_scoring: bool = True
    ) -> Optional[RouteSuggestion]:
        """
        Get best known route to destination based on collective probes.

        Now includes centrality-aware scoring (Use Case 7) to prefer
        paths through well-connected hive members.

        Args:
            destination: Target node pubkey
            amount_sats: Amount to route
            hive_members: Set of hive member pubkeys (for bonus calculation)
            use_centrality_scoring: If True, include centrality in scoring

        Returns:
            RouteSuggestion if found, None otherwise
        """
        if hive_members is None:
            hive_members = set()

        # Collect all paths to this destination
        candidates = []

        for (dest, path), stats in self._path_stats.items():
            if dest != destination:
                continue

            if stats.probe_count == 0:
                continue

            # Calculate success rate
            success_rate = stats.success_count / stats.probe_count

            # Skip unreliable paths
            if success_rate < LOW_SUCCESS_RATE:
                continue

            # Check capacity
            if stats.avg_capacity_sats > 0 and stats.avg_capacity_sats < amount_sats:
                continue

            # Calculate averages
            if stats.success_count > 0:
                avg_latency = stats.total_latency_ms // stats.success_count
                avg_fee = stats.total_fee_ppm // stats.success_count
            else:
                avg_latency = 0
                avg_fee = 0

            # Calculate hive hop bonus
            hive_hop_count = sum(1 for hop in path if hop in hive_members)

            # Calculate confidence
            confidence = self.get_path_confidence(list(path))

            # Calculate path centrality (Use Case 7)
            path_centrality, is_high_centrality = self._get_path_centrality_score(
                list(path), hive_members
            )

            candidates.append(RouteSuggestion(
                destination=destination,
                path=list(path),
                expected_fee_ppm=avg_fee,
                expected_latency_ms=avg_latency,
                success_rate=success_rate,
                confidence=confidence,
                last_successful_probe=stats.last_success_time,
                hive_hop_count=hive_hop_count,
                path_centrality_score=path_centrality,
                is_high_centrality_path=is_high_centrality
            ))

        if not candidates:
            return None

        # Score candidates
        def score_route(route: RouteSuggestion) -> float:
            # Higher success rate is better
            success_score = route.success_rate

            # Lower fees are better
            fee_score = 1.0 / (1 + route.expected_fee_ppm / 1000)

            # Prefer paths through hive members (0 fee hops)
            hive_bonus = 0.1 * route.hive_hop_count

            # Centrality bonus (Use Case 7)
            centrality_bonus = 0.0
            if use_centrality_scoring and route.path_centrality_score > 0:
                centrality_bonus = route.path_centrality_score * CENTRALITY_WEIGHT_IN_ROUTING
                if route.is_high_centrality_path:
                    centrality_bonus *= HIGH_CENTRALITY_ROUTING_BONUS

            # Confidence multiplier
            confidence_mult = 0.5 + (route.confidence * 0.5)

            # Adjust weights to include centrality
            if use_centrality_scoring:
                return (
                    success_score * 0.35 +
                    fee_score * 0.35 +
                    hive_bonus * 0.15 +
                    centrality_bonus
                ) * confidence_mult
            else:
                return (
                    success_score * 0.4 +
                    fee_score * 0.4 +
                    hive_bonus * 0.2
                ) * confidence_mult

        return max(candidates, key=score_route)

    def get_fallback_routes(
        self,
        destination: str,
        failed_path: List[str],
        amount_sats: int,
        hive_members: set = None,
        limit: int = 3
    ) -> List[RouteSuggestion]:
        """
        Get fallback routes when a primary path fails.

        Prioritizes paths through high-centrality hive members that
        don't include the failed path's hops.

        Args:
            destination: Target node pubkey
            failed_path: The path that failed
            amount_sats: Amount to route
            hive_members: Set of hive member pubkeys
            limit: Maximum number of fallback routes to return

        Returns:
            List of alternative RouteSuggestion sorted by quality
        """
        if hive_members is None:
            hive_members = set()

        failed_set = set(failed_path)
        candidates = []

        for (dest, path), stats in self._path_stats.items():
            if dest != destination:
                continue

            if stats.probe_count == 0:
                continue

            # Skip paths that overlap with failed path (except destination)
            path_set = set(path)
            if path_set & failed_set:  # Any overlap
                continue

            success_rate = stats.success_count / stats.probe_count

            # For fallbacks, we might accept slightly lower success rates
            if success_rate < LOW_SUCCESS_RATE * 0.8:  # 40% threshold for fallbacks
                continue

            if stats.avg_capacity_sats > 0 and stats.avg_capacity_sats < amount_sats:
                continue

            if stats.success_count > 0:
                avg_latency = stats.total_latency_ms // stats.success_count
                avg_fee = stats.total_fee_ppm // stats.success_count
            else:
                avg_latency = 0
                avg_fee = 0

            hive_hop_count = sum(1 for hop in path if hop in hive_members)
            path_centrality, is_high_centrality = self._get_path_centrality_score(
                list(path), hive_members
            )

            # For fallbacks, strongly prefer high-centrality paths
            if path_centrality < MIN_CENTRALITY_FOR_FALLBACK and hive_hop_count > 0:
                continue

            candidates.append(RouteSuggestion(
                destination=destination,
                path=list(path),
                expected_fee_ppm=avg_fee,
                expected_latency_ms=avg_latency,
                success_rate=success_rate,
                confidence=self.get_path_confidence(list(path)),
                last_successful_probe=stats.last_success_time,
                hive_hop_count=hive_hop_count,
                path_centrality_score=path_centrality,
                is_high_centrality_path=is_high_centrality
            ))

        # Score with heavy emphasis on centrality for fallbacks
        def score_fallback(route: RouteSuggestion) -> float:
            success_score = route.success_rate
            fee_score = 1.0 / (1 + route.expected_fee_ppm / 1000)

            # Heavily weight centrality for fallback routes
            centrality_score = route.path_centrality_score
            if route.is_high_centrality_path:
                centrality_score *= 1.5

            return (
                success_score * 0.3 +
                fee_score * 0.2 +
                centrality_score * 0.5  # 50% weight for centrality in fallbacks
            )

        candidates.sort(key=score_fallback, reverse=True)
        return candidates[:limit]

    def get_routes_to(
        self,
        destination: str,
        amount_sats: int = 0,
        limit: int = 5
    ) -> List[RouteSuggestion]:
        """
        Get all known routes to a destination, sorted by quality.

        Args:
            destination: Target node pubkey
            amount_sats: Minimum capacity required (0 for any)
            limit: Maximum routes to return

        Returns:
            List of route suggestions
        """
        candidates = []

        for (dest, path), stats in self._path_stats.items():
            if dest != destination:
                continue

            if stats.probe_count == 0:
                continue

            success_rate = stats.success_count / stats.probe_count

            # Check capacity if specified
            if amount_sats > 0 and stats.avg_capacity_sats > 0:
                if stats.avg_capacity_sats < amount_sats:
                    continue

            if stats.success_count > 0:
                avg_latency = stats.total_latency_ms // stats.success_count
                avg_fee = stats.total_fee_ppm // stats.success_count
            else:
                avg_latency = 0
                avg_fee = 0

            candidates.append(RouteSuggestion(
                destination=destination,
                path=list(path),
                expected_fee_ppm=avg_fee,
                expected_latency_ms=avg_latency,
                success_rate=success_rate,
                confidence=self.get_path_confidence(list(path)),
                last_successful_probe=stats.last_success_time,
                hive_hop_count=0
            ))

        # Sort by success rate
        candidates.sort(key=lambda r: r.success_rate, reverse=True)

        return candidates[:limit]

    def get_routing_stats(self) -> Dict[str, Any]:
        """
        Get overall routing intelligence statistics.

        Returns:
            Dict with routing statistics
        """
        total_paths = len(self._path_stats)
        total_probes = sum(s.probe_count for s in self._path_stats.values())
        total_successes = sum(s.success_count for s in self._path_stats.values())

        # Unique destinations
        destinations = set(dest for dest, _ in self._path_stats.keys())

        # High quality paths (>90% success)
        high_quality = sum(
            1 for s in self._path_stats.values()
            if s.probe_count > 0 and s.success_count / s.probe_count >= HIGH_SUCCESS_RATE
        )

        # Recent activity
        now = time.time()
        recent_cutoff = now - (24 * 3600)
        recent_probes = sum(
            1 for s in self._path_stats.values()
            if max(s.last_success_time, s.last_failure_time) > recent_cutoff
        )

        return {
            "total_paths": total_paths,
            "total_probes": total_probes,
            "total_successes": total_successes,
            "overall_success_rate": total_successes / total_probes if total_probes > 0 else 0,
            "unique_destinations": len(destinations),
            "high_quality_paths": high_quality,
            "recent_activity_count": recent_probes,
        }

    def aggregate_from_database(self):
        """
        Rebuild path statistics from database probes.

        Used on startup or after clearing in-memory data.
        """
        probes = self.database.get_all_route_probes(max_age_hours=PROBE_STALENESS_HOURS)

        for probe in probes:
            path = tuple(probe.get("path", []))
            if not path:
                continue

            self._update_path_stats(
                destination=probe.get("destination", ""),
                path=path,
                success=probe.get("success", False),
                latency_ms=probe.get("latency_ms", 0),
                fee_ppm=probe.get("total_fee_ppm", 0),
                capacity_sats=probe.get("estimated_capacity_sats", 0),
                reporter_id=probe.get("reporter_id", ""),
                failure_reason=probe.get("failure_reason", ""),
                timestamp=probe.get("timestamp", 0)
            )

    def cleanup_stale_data(self):
        """Remove stale path statistics."""
        now = time.time()
        stale_cutoff = now - (PROBE_STALENESS_HOURS * 3600)

        stale_keys = [
            key for key, stats in self._path_stats.items()
            if max(stats.last_success_time, stats.last_failure_time) < stale_cutoff
        ]

        for key in stale_keys:
            del self._path_stats[key]

        return len(stale_keys)
