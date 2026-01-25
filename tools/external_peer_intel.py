"""
External Peer Intelligence - Network graph analysis and optional API integration.

This module provides comprehensive peer evaluation for channel open decisions by:
1. Querying local network graph (listnodes, listchannels) for connectivity data
2. Optionally fetching from external APIs (1ML, Amboss) for reputation data
3. Combining with local experience data for final recommendations

Usage:
    from external_peer_intel import ExternalPeerIntelligence

    intel = ExternalPeerIntelligence(rpc)
    result = intel.get_peer_profile("02abc...")

    # Check if peer meets channel open criteria
    if result.meets_channel_open_criteria():
        print("Safe to open channel")
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import json
import ssl

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class NetworkGraphData:
    """Data from local network graph analysis."""
    pubkey: str
    alias: str = ""

    # Connectivity
    channel_count: int = 0
    total_capacity_sats: int = 0
    avg_channel_size_sats: int = 0

    # Fee profile
    median_fee_ppm: int = 0
    min_fee_ppm: int = 0
    max_fee_ppm: int = 0

    # Network position
    connected_to_exchanges: List[str] = field(default_factory=list)
    is_well_connected: bool = False  # >15 channels

    # Timestamps
    last_update: int = 0
    oldest_channel_block: int = 0  # Approximate node age


@dataclass
class ExternalReputationData:
    """Data from external APIs (1ML, Amboss)."""
    pubkey: str
    source: str = ""  # "1ml", "amboss", "none"

    # Reputation scores
    reputation_score: float = 0.0  # 0-1 normalized
    rank: int = 0  # Network rank if available

    # Historical data
    capacity_rank: int = 0
    channel_count_rank: int = 0

    # Risk indicators
    force_close_count: int = 0
    avg_channel_age_days: float = 0.0

    # Flags
    is_verified: bool = False
    has_warnings: bool = False
    warning_reasons: List[str] = field(default_factory=list)

    # Fetch status
    fetched_at: int = 0
    fetch_error: str = ""


@dataclass
class ComprehensivePeerProfile:
    """Combined peer profile from all sources."""
    pubkey: str
    alias: str = ""

    # Graph data
    graph: Optional[NetworkGraphData] = None

    # External reputation
    external: Optional[ExternalReputationData] = None

    # Local experience (from advisor_db)
    local_channels_opened: int = 0
    local_force_closes: int = 0
    local_reliability_score: float = 0.0
    local_profitability_score: float = 0.0
    local_recommendation: str = "unknown"

    # Combined assessment
    overall_score: float = 0.0  # 0-100
    risk_level: str = "unknown"  # low, medium, high, critical
    recommendation: str = "unknown"  # excellent, good, neutral, caution, avoid
    recommendation_reasons: List[str] = field(default_factory=list)

    # Channel open criteria check
    meets_min_channels: bool = False  # >15 channels
    meets_fee_criteria: bool = False  # median fee <500 ppm
    has_force_close_history: bool = False
    is_existing_peer: bool = False

    def meets_channel_open_criteria(self) -> bool:
        """Check if peer meets all criteria for channel open approval."""
        if self.is_existing_peer:
            return False
        if self.has_force_close_history and self.local_force_closes > 0:
            return False
        if not self.meets_min_channels:
            return False
        if self.risk_level in ("high", "critical"):
            return False
        if self.recommendation == "avoid":
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pubkey": self.pubkey,
            "alias": self.alias,
            "graph": {
                "channel_count": self.graph.channel_count if self.graph else 0,
                "total_capacity_sats": self.graph.total_capacity_sats if self.graph else 0,
                "median_fee_ppm": self.graph.median_fee_ppm if self.graph else 0,
                "is_well_connected": self.graph.is_well_connected if self.graph else False,
                "connected_to_exchanges": self.graph.connected_to_exchanges if self.graph else [],
            } if self.graph else None,
            "external": {
                "source": self.external.source if self.external else "none",
                "reputation_score": self.external.reputation_score if self.external else 0,
                "force_close_count": self.external.force_close_count if self.external else 0,
                "is_verified": self.external.is_verified if self.external else False,
                "has_warnings": self.external.has_warnings if self.external else False,
            } if self.external else None,
            "local": {
                "channels_opened": self.local_channels_opened,
                "force_closes": self.local_force_closes,
                "reliability_score": self.local_reliability_score,
                "profitability_score": self.local_profitability_score,
                "recommendation": self.local_recommendation,
            },
            "assessment": {
                "overall_score": self.overall_score,
                "risk_level": self.risk_level,
                "recommendation": self.recommendation,
                "reasons": self.recommendation_reasons,
            },
            "channel_open_criteria": {
                "meets_min_channels": self.meets_min_channels,
                "meets_fee_criteria": self.meets_fee_criteria,
                "has_force_close_history": self.has_force_close_history,
                "is_existing_peer": self.is_existing_peer,
                "approved": self.meets_channel_open_criteria(),
            }
        }


# =============================================================================
# Known Exchange Pubkeys (for connectivity scoring)
# =============================================================================

KNOWN_EXCHANGES = {
    # ACINQ (Phoenix)
    "03864ef025fde8fb587d989186ce6a4a186895ee44a926bfc370e2c366597a3f8f": "ACINQ",
    # Kraken
    "02f1a8c87607f415c8f22c00593002775941dea48869ce23096af27b0cfdcc0b69": "Kraken",
    # Bitfinex
    "033d8656219478701227199cbd6f670335c8d408a92ae88b962c49d4dc0e83e025": "Bitfinex",
    # River Financial
    "03037dc08e9ac63b82581f79b662a4d0ceca8a8ca162b1af3551595b452a26db11": "River",
    # Wallet of Satoshi
    "035e4ff418fc8b5554c5d9eea66396c227bd429a3251c8cbc711002ba215bfc226": "WoS",
    # Muun
    "038f8f113c580048d847d6949371726653e02b928196bad310e3edd39d40a1ce84": "Muun",
    # OpenNode
    "03abf6f44c355dec0d5aa155bdbdd6e0c8fefe318eff402de65c6eb2e1be55dc3e": "OpenNode",
    # CoinGate
    "0242a4ae0c5bef18048fbecf995094b74bfb0f7391418d71ed394784373f41e4f3": "CoinGate",
    # Fold
    "02816caed43171d3c9854e3b0ab2cf0c42be086ff1bd4005acc2a5f7db70d83774": "Fold",
    # Strike
    "0326a480c969a2c56eeaea8c0e6afe7eb2a2f30e13f6b3f0b0f8e3a4a7a9a2e2a2": "Strike",
}


# =============================================================================
# External Peer Intelligence Class
# =============================================================================

class ExternalPeerIntelligence:
    """
    Fetches and analyzes peer data from network graph and external APIs.
    """

    # Minimum channels for "well connected" status
    MIN_CHANNELS_WELL_CONNECTED = 15

    # Maximum acceptable median fee for quality routing partner
    MAX_MEDIAN_FEE_PPM = 500

    # Cache TTL in seconds
    GRAPH_CACHE_TTL = 300  # 5 minutes
    EXTERNAL_CACHE_TTL = 3600  # 1 hour

    def __init__(self, rpc, enable_external_apis: bool = False):
        """
        Initialize external peer intelligence.

        Args:
            rpc: CLN RPC interface
            enable_external_apis: Whether to fetch from 1ML (free but rate limited).
                                  Disabled by default - local graph analysis is sufficient
                                  for most use cases and doesn't depend on external services.
        """
        self.rpc = rpc
        self.enable_external_apis = enable_external_apis  # Default OFF - use local graph only

        # Caches
        self._graph_cache: Dict[str, Tuple[NetworkGraphData, int]] = {}
        self._external_cache: Dict[str, Tuple[ExternalReputationData, int]] = {}
        self._our_peers: Optional[set] = None
        self._our_peers_fetched: int = 0

    def get_peer_profile(
        self,
        pubkey: str,
        local_intel: Optional[Dict] = None
    ) -> ComprehensivePeerProfile:
        """
        Get comprehensive peer profile combining all data sources.

        Args:
            pubkey: Peer's public key
            local_intel: Optional local intelligence data from advisor_db

        Returns:
            ComprehensivePeerProfile with combined assessment
        """
        profile = ComprehensivePeerProfile(pubkey=pubkey)

        # 1. Get network graph data
        profile.graph = self._get_graph_data(pubkey)
        if profile.graph:
            profile.alias = profile.graph.alias

        # 2. Get external reputation (if enabled)
        if self.enable_external_apis:
            profile.external = self._get_external_reputation(pubkey)

        # 3. Apply local intelligence data
        if local_intel:
            profile.local_channels_opened = local_intel.get("channels_opened", 0)
            profile.local_force_closes = local_intel.get("force_closes", 0)
            profile.local_reliability_score = local_intel.get("reliability_score", 0.0)
            profile.local_profitability_score = local_intel.get("profitability_score", 0.0)
            profile.local_recommendation = local_intel.get("recommendation", "unknown")

        # 4. Check if existing peer
        profile.is_existing_peer = self._is_existing_peer(pubkey)

        # 5. Calculate combined assessment
        self._calculate_assessment(profile)

        return profile

    def _get_graph_data(self, pubkey: str) -> Optional[NetworkGraphData]:
        """Get peer data from local network graph."""
        now = int(time.time())

        # Check cache
        if pubkey in self._graph_cache:
            cached, cached_at = self._graph_cache[pubkey]
            if now - cached_at < self.GRAPH_CACHE_TTL:
                return cached

        try:
            # Get node info
            nodes = self.rpc.listnodes(pubkey)
            if not nodes or not nodes.get("nodes"):
                return None

            node = nodes["nodes"][0]

            data = NetworkGraphData(
                pubkey=pubkey,
                alias=node.get("alias", ""),
                last_update=node.get("last_timestamp", 0)
            )

            # Get channels for this node
            channels = self.rpc.listchannels(source=pubkey)
            channel_list = channels.get("channels", [])

            data.channel_count = len(channel_list)

            if channel_list:
                capacities = []
                fees = []
                destinations = set()

                for ch in channel_list:
                    cap = ch.get("amount_msat", 0)
                    if isinstance(cap, str):
                        cap = int(cap.replace("msat", ""))
                    capacities.append(cap // 1000)  # Convert to sats

                    fee_ppm = ch.get("fee_per_millionth", 0)
                    fees.append(fee_ppm)

                    dest = ch.get("destination", "")
                    destinations.add(dest)

                data.total_capacity_sats = sum(capacities)
                data.avg_channel_size_sats = data.total_capacity_sats // len(capacities) if capacities else 0

                if fees:
                    sorted_fees = sorted(fees)
                    data.median_fee_ppm = sorted_fees[len(sorted_fees) // 2]
                    data.min_fee_ppm = sorted_fees[0]
                    data.max_fee_ppm = sorted_fees[-1]

                # Check exchange connectivity
                for dest in destinations:
                    if dest in KNOWN_EXCHANGES:
                        data.connected_to_exchanges.append(KNOWN_EXCHANGES[dest])

                data.is_well_connected = data.channel_count >= self.MIN_CHANNELS_WELL_CONNECTED

            # Cache result
            self._graph_cache[pubkey] = (data, now)

            return data

        except Exception as e:
            logger.warning(f"Error fetching graph data for {pubkey[:16]}...: {e}")
            return None

    def _get_external_reputation(self, pubkey: str) -> Optional[ExternalReputationData]:
        """Fetch reputation from external APIs (1ML, Amboss)."""
        now = int(time.time())

        # Check cache
        if pubkey in self._external_cache:
            cached, cached_at = self._external_cache[pubkey]
            if now - cached_at < self.EXTERNAL_CACHE_TTL:
                return cached

        data = ExternalReputationData(pubkey=pubkey, fetched_at=now)

        # Try 1ML first
        try:
            data = self._fetch_1ml_data(pubkey)
            if data.source == "1ml":
                self._external_cache[pubkey] = (data, now)
                return data
        except Exception as e:
            logger.debug(f"1ML fetch failed for {pubkey[:16]}...: {e}")

        # Fallback to Amboss (TODO: implement when API available)
        # try:
        #     data = self._fetch_amboss_data(pubkey)
        # except Exception as e:
        #     logger.debug(f"Amboss fetch failed: {e}")

        data.source = "none"
        data.fetch_error = "No external data available"
        self._external_cache[pubkey] = (data, now)

        return data

    def _fetch_1ml_data(self, pubkey: str) -> ExternalReputationData:
        """Fetch data from 1ML API."""
        data = ExternalReputationData(
            pubkey=pubkey,
            source="1ml",
            fetched_at=int(time.time())
        )

        url = f"https://1ml.com/node/{pubkey}/json"

        # Create SSL context that doesn't verify (1ML has cert issues sometimes)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        req = Request(url, headers={"User-Agent": "cl-hive/1.0"})

        try:
            with urlopen(req, timeout=10, context=ctx) as response:
                result = json.loads(response.read().decode())

                # Extract relevant fields
                data.rank = result.get("noderank", {}).get("rank", 0)
                data.capacity_rank = result.get("noderank", {}).get("capacity", 0)
                data.channel_count_rank = result.get("noderank", {}).get("channelcount", 0)

                # Calculate normalized reputation score (lower rank = higher score)
                if data.rank > 0:
                    # Assume ~15000 nodes, normalize to 0-1
                    data.reputation_score = max(0, 1 - (data.rank / 15000))

                # Check for force closes in history (if available)
                # 1ML doesn't directly expose this, but we can infer from metrics

                return data

        except (URLError, HTTPError) as e:
            data.fetch_error = str(e)
            raise

    def _is_existing_peer(self, pubkey: str) -> bool:
        """Check if we already have a channel with this peer."""
        now = int(time.time())

        # Refresh peer list every 60 seconds
        if self._our_peers is None or now - self._our_peers_fetched > 60:
            try:
                peers = self.rpc.listpeers()
                self._our_peers = {
                    p.get("id") for p in peers.get("peers", [])
                    if p.get("channels")  # Only peers with channels
                }
                self._our_peers_fetched = now
            except Exception as e:
                logger.warning(f"Error fetching peers: {e}")
                return False

        return pubkey in self._our_peers

    def _calculate_assessment(self, profile: ComprehensivePeerProfile) -> None:
        """Calculate combined assessment from all data sources."""
        reasons = []
        score = 50.0  # Start neutral

        # Graph-based scoring
        if profile.graph:
            # Channel count scoring
            if profile.graph.channel_count >= 50:
                score += 15
                reasons.append(f"Well connected ({profile.graph.channel_count} channels)")
            elif profile.graph.channel_count >= self.MIN_CHANNELS_WELL_CONNECTED:
                score += 10
                reasons.append(f"Adequately connected ({profile.graph.channel_count} channels)")
                profile.meets_min_channels = True
            elif profile.graph.channel_count >= 10:
                score += 5
                reasons.append(f"Moderately connected ({profile.graph.channel_count} channels)")
            else:
                score -= 10
                reasons.append(f"Poorly connected ({profile.graph.channel_count} channels)")

            if profile.graph.channel_count >= self.MIN_CHANNELS_WELL_CONNECTED:
                profile.meets_min_channels = True

            # Fee scoring
            if profile.graph.median_fee_ppm <= self.MAX_MEDIAN_FEE_PPM:
                score += 10
                profile.meets_fee_criteria = True
                reasons.append(f"Reasonable fees (median {profile.graph.median_fee_ppm} ppm)")
            elif profile.graph.median_fee_ppm <= 1000:
                score += 5
                reasons.append(f"Moderate fees (median {profile.graph.median_fee_ppm} ppm)")
            else:
                score -= 5
                reasons.append(f"High fees (median {profile.graph.median_fee_ppm} ppm)")

            # Exchange connectivity bonus
            if profile.graph.connected_to_exchanges:
                score += 5 * min(len(profile.graph.connected_to_exchanges), 3)
                reasons.append(f"Connected to: {', '.join(profile.graph.connected_to_exchanges[:3])}")

        # External reputation scoring
        if profile.external and profile.external.source != "none":
            if profile.external.reputation_score >= 0.8:
                score += 15
                reasons.append(f"Excellent network reputation (rank ~{profile.external.rank})")
            elif profile.external.reputation_score >= 0.5:
                score += 10
                reasons.append(f"Good network reputation (rank ~{profile.external.rank})")
            elif profile.external.reputation_score >= 0.2:
                score += 5
                reasons.append(f"Average network reputation")

            if profile.external.force_close_count > 0:
                score -= 10 * min(profile.external.force_close_count, 3)
                profile.has_force_close_history = True
                reasons.append(f"Has {profile.external.force_close_count} force closes in history")

            if profile.external.has_warnings:
                score -= 15
                reasons.extend(profile.external.warning_reasons)

        # Local experience scoring (weighted heavily)
        if profile.local_recommendation != "unknown":
            if profile.local_recommendation == "excellent":
                score += 20
                reasons.append("Excellent local experience")
            elif profile.local_recommendation == "good":
                score += 15
                reasons.append("Good local experience")
            elif profile.local_recommendation == "neutral":
                score += 5
            elif profile.local_recommendation == "caution":
                score -= 10
                reasons.append("Previous issues (caution)")
            elif profile.local_recommendation == "avoid":
                score -= 25
                reasons.append("Previous severe issues (avoid)")

        if profile.local_force_closes > 0:
            score -= 15 * min(profile.local_force_closes, 3)
            profile.has_force_close_history = True
            reasons.append(f"Had {profile.local_force_closes} force closes with us")

        # Existing peer check
        if profile.is_existing_peer:
            reasons.append("Already have channel with this peer")

        # Normalize score to 0-100
        profile.overall_score = max(0, min(100, score))

        # Determine risk level
        if profile.overall_score >= 80:
            profile.risk_level = "low"
        elif profile.overall_score >= 60:
            profile.risk_level = "medium"
        elif profile.overall_score >= 40:
            profile.risk_level = "high"
        else:
            profile.risk_level = "critical"

        # Determine recommendation
        if profile.overall_score >= 80:
            profile.recommendation = "excellent"
        elif profile.overall_score >= 65:
            profile.recommendation = "good"
        elif profile.overall_score >= 50:
            profile.recommendation = "neutral"
        elif profile.overall_score >= 35:
            profile.recommendation = "caution"
        else:
            profile.recommendation = "avoid"

        profile.recommendation_reasons = reasons

    def clear_cache(self) -> None:
        """Clear all caches."""
        self._graph_cache.clear()
        self._external_cache.clear()
        self._our_peers = None
