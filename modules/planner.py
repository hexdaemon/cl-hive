"""
Planner Module for cl-hive (Phase 6: Topology Optimization)

Implements the "Gardner" algorithm for automated topology management:
- Saturation Analysis: Calculate Hive market share per target
- Guard Mechanism: Prevent redundant channel opens to saturated targets
- Expansion Proposals: Cooperative expansion with feerate gate

CLBoss Integration (Optional):
CLBoss is NOT required. If installed (ksedgwic/clboss fork):
- Uses clboss-unmanage with 'open' tag to prevent CLBoss channel opens to saturated targets
- Uses clboss-manage to re-enable opens when saturation drops
- Fee/balance tags are managed by cl-revenue-ops (not this module)

If CLBoss is NOT installed:
- Saturation detection still runs for analytics
- Hive uses native cooperative expansion instead

Security Constraints (Red Team - PHASE6_THREAT_MODEL):
- Gossip capacity is CLAMPED to public listchannels data
- Max 5 new unmanages per cycle (abort if exceeded)
- All decisions logged to hive_planner_log table

This ticket (6-01) implements ONLY saturation detection and guard mechanism.
Expansion logic will be added in later tickets.

Author: Lightning Goats Team
"""

import time
import secrets
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from pyln.client import RpcError
except ImportError:
    # For testing without pyln installed
    class RpcError(Exception):
        """Stub RpcError for testing."""
        pass

try:
    from modules.intent_manager import IntentType
    from modules.protocol import serialize, HiveMessageType
except ImportError:
    # For testing - define stubs
    class IntentType:
        CHANNEL_OPEN = 'channel_open'
    class HiveMessageType:
        INTENT = 'intent'
    def serialize(msg_type, payload):
        return b''

try:
    from modules.quality_scorer import PeerQualityScorer
except ImportError:
    # For testing without quality_scorer
    PeerQualityScorer = None


# =============================================================================
# CONSTANTS
# =============================================================================

# Cache refresh interval (seconds) - avoid hammering listchannels
NETWORK_CACHE_TTL_SECONDS = 300

# Maximum ignores per cycle (Red Team mitigation)
MAX_IGNORES_PER_CYCLE = 5

# Saturation release threshold (hysteresis to avoid flip-flopping)
SATURATION_RELEASE_THRESHOLD_PCT = 0.15  # Release ignore at 15%

# Minimum public capacity to consider a target (anti-Sybil)
MIN_TARGET_CAPACITY_SATS = 100_000_000  # 1 BTC

# Underserved threshold (targets with low Hive share)
UNDERSERVED_THRESHOLD_PCT = 0.05  # < 5% Hive share = underserved

# Legacy minimum channel size (now configurable via planner_min_channel_sats)
# Kept for backwards compatibility in case config is not available
MIN_CHANNEL_SIZE_SATS_FALLBACK = 1_000_000  # 1M sats fallback

# Maximum expansion proposals per cycle (rate limiting)
MAX_EXPANSIONS_PER_CYCLE = 1

# Quality scoring thresholds (Phase 6.2)
MIN_QUALITY_SCORE = 0.45  # Minimum quality score for expansion
QUALITY_SCORE_DAYS = 90   # Days of history to consider for quality scoring


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ChannelInfo:
    """Represents a channel from listchannels."""
    source: str
    destination: str
    short_channel_id: str
    capacity_sats: int
    active: bool


@dataclass
class SaturationResult:
    """Result of saturation calculation for a target."""
    target: str
    hive_capacity_sats: int
    public_capacity_sats: int
    hive_share_pct: float
    is_saturated: bool
    should_release: bool


@dataclass
class UnderservedResult:
    """Result identifying an underserved target for expansion."""
    target: str
    public_capacity_sats: int
    hive_share_pct: float
    score: float  # Higher = more attractive for expansion
    quality_score: float = 0.5  # Peer quality score (Phase 6.2)
    quality_confidence: float = 0.0  # Confidence in quality score
    quality_recommendation: str = "neutral"  # Quality recommendation


@dataclass
class ChannelSizeResult:
    """Result of intelligent channel sizing calculation."""
    recommended_size_sats: int
    factors: Dict[str, Any]
    reasoning: str


# =============================================================================
# INTELLIGENT CHANNEL SIZING
# =============================================================================

class ChannelSizer:
    """
    Intelligent channel sizing engine for Hive expansion proposals.

    Factors considered:
    1. Target capacity - larger nodes warrant larger channels (credibility)
    2. Hive share gap - lower share → larger channel to reach target share
    3. Routing potential - nodes with high connectivity get larger channels
    4. Available liquidity - don't overcommit, leave operational reserve
    5. Economics - expected fee revenue vs capital lockup cost
    6. Quality score - peer reliability based on historical hive data (Phase 6.3)

    The algorithm produces a weighted score that determines channel size
    within the configured min/max bounds.
    """

    # Weight factors for each sizing component (sum to 1.0)
    # Phase 6.3: Redistributed to include quality factor
    WEIGHT_TARGET_CAPACITY = 0.15  # 15% - larger targets get larger channels
    WEIGHT_SHARE_GAP = 0.20        # 20% - underserved targets get priority
    WEIGHT_ROUTING_POTENTIAL = 0.20  # 20% - high-connectivity nodes
    WEIGHT_LIQUIDITY = 0.15        # 15% - available balance consideration
    WEIGHT_ECONOMICS = 0.10        # 10% - expected ROI
    WEIGHT_QUALITY = 0.20          # 20% - peer quality score (Phase 6.3)

    # Routing potential thresholds
    HIGH_CONNECTIVITY_CHANNELS = 50   # Node with 50+ channels = high connectivity
    VERY_HIGH_CONNECTIVITY_CHANNELS = 200  # 200+ = major routing node

    # Economic assumptions
    EXPECTED_ANNUAL_FEE_RATE = 0.001  # 0.1% annual return on channel capacity
    OPPORTUNITY_COST_RATE = 0.05      # 5% opportunity cost of locked capital

    # Quality score thresholds (Phase 6.3)
    QUALITY_EXCELLENT_THRESHOLD = 0.75  # Excellent quality - bonus sizing
    QUALITY_GOOD_THRESHOLD = 0.55       # Good quality - normal sizing
    QUALITY_NEUTRAL_THRESHOLD = 0.40    # Neutral - slightly reduced
    # Below NEUTRAL = caution - significantly reduced

    def __init__(self, plugin=None, quality_scorer=None):
        """
        Initialize the ChannelSizer.

        Args:
            plugin: Plugin instance for logging
            quality_scorer: PeerQualityScorer instance for quality lookups (Phase 6.3)
        """
        self.plugin = plugin
        self.quality_scorer = quality_scorer

    def _log(self, msg: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[ChannelSizer] {msg}", level=level)

    def calculate_size(
        self,
        target: str,
        target_capacity_sats: int,
        target_channel_count: int,
        hive_share_pct: float,
        target_share_cap: float,
        onchain_balance_sats: int,
        min_channel_sats: int,
        max_channel_sats: int,
        default_channel_sats: int,
        hive_total_capacity_sats: int = 0,
        avg_fee_rate_ppm: int = 500,
        quality_score: float = None,
        quality_confidence: float = 0.0,
        quality_recommendation: str = "neutral",
        available_budget_sats: int = None,
    ) -> ChannelSizeResult:
        """
        Calculate the optimal channel size for a target.

        Args:
            target: Target node pubkey
            target_capacity_sats: Target's total public capacity
            target_channel_count: Number of channels the target has
            hive_share_pct: Current hive share to this target (0.0-1.0)
            target_share_cap: Maximum share we want (e.g., 0.20 for 20%)
            onchain_balance_sats: Available onchain balance
            min_channel_sats: Minimum allowed channel size
            max_channel_sats: Maximum allowed channel size
            default_channel_sats: Default channel size (baseline)
            hive_total_capacity_sats: Total hive capacity (for liquidity calc)
            avg_fee_rate_ppm: Average fee rate in ppm for economic calc
            quality_score: Peer quality score 0-1 (Phase 6.3, optional)
            quality_confidence: Confidence in quality score 0-1 (Phase 6.3)
            quality_recommendation: Quality recommendation string (Phase 6.3)
            available_budget_sats: Available budget for channel opens (optional)
                If provided, caps the channel size to stay within budget.

        Returns:
            ChannelSizeResult with recommended size and reasoning
        """
        factors = {}

        # Phase 6.3: Lookup quality if not provided and scorer is available
        if quality_score is None and self.quality_scorer:
            quality_result = self.quality_scorer.calculate_score(target)
            quality_score = quality_result.overall_score
            quality_confidence = quality_result.confidence
            quality_recommendation = quality_result.recommendation
        elif quality_score is None:
            # Default to neutral if no quality data
            quality_score = 0.5
            quality_confidence = 0.0
            quality_recommendation = "neutral"

        # =================================================================
        # Factor 1: Target Capacity Score (0.0 to 2.0)
        # Larger nodes warrant larger channels for credibility
        # =================================================================
        # Baseline: 1 BTC capacity = 1.0 score
        # Scale logarithmically: 10 BTC = 1.5, 100 BTC = 2.0
        import math
        btc_capacity = target_capacity_sats / 100_000_000
        if btc_capacity <= 0:
            capacity_score = 0.5
        else:
            capacity_score = min(2.0, 0.5 + 0.5 * math.log10(max(1, btc_capacity)))
        factors['capacity_score'] = round(capacity_score, 3)
        factors['target_capacity_btc'] = round(btc_capacity, 2)

        # =================================================================
        # Factor 2: Share Gap Score (0.0 to 2.0)
        # Lower current share = higher score (need to catch up)
        # =================================================================
        share_gap = target_share_cap - hive_share_pct
        if share_gap <= 0:
            # Already at or above target share
            share_score = 0.5
        else:
            # Scale: 0% share = 2.0, target_share = 1.0
            share_score = 1.0 + (share_gap / target_share_cap)
        share_score = min(2.0, max(0.5, share_score))
        factors['share_score'] = round(share_score, 3)
        factors['share_gap_pct'] = round(share_gap * 100, 2)

        # =================================================================
        # Factor 3: Routing Potential Score (0.0 to 2.0)
        # More connected nodes = better routing potential
        # =================================================================
        if target_channel_count >= self.VERY_HIGH_CONNECTIVITY_CHANNELS:
            routing_score = 2.0  # Major routing hub
        elif target_channel_count >= self.HIGH_CONNECTIVITY_CHANNELS:
            routing_score = 1.5  # Well-connected node
        elif target_channel_count >= 20:
            routing_score = 1.2  # Moderately connected
        elif target_channel_count >= 10:
            routing_score = 1.0  # Average
        else:
            routing_score = 0.7  # Low connectivity (risky)
        factors['routing_score'] = routing_score
        factors['target_channel_count'] = target_channel_count

        # =================================================================
        # Factor 4: Liquidity Score (0.5 to 1.5)
        # Don't overcommit - leave operational reserve
        # =================================================================
        # Reserve: keep at least 20% of balance for other operations
        available_for_channel = onchain_balance_sats * 0.8

        # Score based on how comfortable we are with the allocation
        if available_for_channel >= max_channel_sats * 3:
            liquidity_score = 1.5  # Very comfortable
        elif available_for_channel >= max_channel_sats * 2:
            liquidity_score = 1.3  # Comfortable
        elif available_for_channel >= max_channel_sats:
            liquidity_score = 1.0  # Adequate
        elif available_for_channel >= min_channel_sats * 2:
            liquidity_score = 0.8  # Tight
        else:
            liquidity_score = 0.5  # Very tight - use minimum
        factors['liquidity_score'] = liquidity_score
        factors['available_sats'] = int(available_for_channel)

        # =================================================================
        # Factor 5: Economics Score (0.5 to 1.5)
        # Expected fee revenue vs capital lockup cost
        # =================================================================
        # Simple model: (expected_annual_fees / locked_capital) vs threshold
        # Higher fee rate environments = larger channels make more sense

        # Expected annual fee revenue per sat locked
        fee_multiplier = avg_fee_rate_ppm / 1_000_000
        annual_turns = 12  # Assume capital turns over ~12x per year
        expected_annual_return = fee_multiplier * annual_turns

        # ROI score
        if expected_annual_return >= 0.02:  # 2%+ return
            economics_score = 1.5
        elif expected_annual_return >= 0.01:  # 1%+ return
            economics_score = 1.2
        elif expected_annual_return >= 0.005:  # 0.5%+ return
            economics_score = 1.0
        else:
            economics_score = 0.7  # Low return environment
        factors['economics_score'] = economics_score
        factors['expected_annual_return_pct'] = round(expected_annual_return * 100, 2)
        factors['avg_fee_rate_ppm'] = avg_fee_rate_ppm

        # =================================================================
        # Factor 6: Quality Score (0.5 to 2.0) - Phase 6.3
        # Higher quality peers get larger channels
        # =================================================================
        # Quality score ranges from 0 to 1, we map to 0.5 to 2.0
        # - Excellent (>0.75): 1.5 to 2.0 - larger channels
        # - Good (0.55-0.75): 1.0 to 1.5 - normal to bonus
        # - Neutral (0.40-0.55): 0.8 to 1.0 - slightly reduced
        # - Caution (<0.40): 0.5 to 0.8 - significantly reduced

        if quality_confidence < 0.3:
            # Low confidence - use neutral scoring
            quality_factor = 1.0
            factors['quality_note'] = 'low_confidence_neutral'
        elif quality_score >= self.QUALITY_EXCELLENT_THRESHOLD:
            # Excellent quality - bonus sizing
            excess = quality_score - self.QUALITY_EXCELLENT_THRESHOLD
            quality_factor = 1.5 + (excess / 0.25) * 0.5  # 1.5 to 2.0
            quality_factor = min(2.0, quality_factor)
        elif quality_score >= self.QUALITY_GOOD_THRESHOLD:
            # Good quality - normal to bonus
            ratio = (quality_score - self.QUALITY_GOOD_THRESHOLD) / (
                self.QUALITY_EXCELLENT_THRESHOLD - self.QUALITY_GOOD_THRESHOLD
            )
            quality_factor = 1.0 + ratio * 0.5  # 1.0 to 1.5
        elif quality_score >= self.QUALITY_NEUTRAL_THRESHOLD:
            # Neutral - slightly reduced
            ratio = (quality_score - self.QUALITY_NEUTRAL_THRESHOLD) / (
                self.QUALITY_GOOD_THRESHOLD - self.QUALITY_NEUTRAL_THRESHOLD
            )
            quality_factor = 0.8 + ratio * 0.2  # 0.8 to 1.0
        else:
            # Caution - significantly reduced
            ratio = quality_score / self.QUALITY_NEUTRAL_THRESHOLD
            quality_factor = 0.5 + ratio * 0.3  # 0.5 to 0.8

        factors['quality_factor'] = round(quality_factor, 3)
        factors['quality_score'] = round(quality_score, 3)
        factors['quality_confidence'] = round(quality_confidence, 3)
        factors['quality_recommendation'] = quality_recommendation

        # =================================================================
        # Calculate Weighted Score
        # =================================================================
        weighted_score = (
            capacity_score * self.WEIGHT_TARGET_CAPACITY +
            share_score * self.WEIGHT_SHARE_GAP +
            routing_score * self.WEIGHT_ROUTING_POTENTIAL +
            liquidity_score * self.WEIGHT_LIQUIDITY +
            economics_score * self.WEIGHT_ECONOMICS +
            quality_factor * self.WEIGHT_QUALITY
        )
        factors['weighted_score'] = round(weighted_score, 3)

        # =================================================================
        # Convert Score to Channel Size
        # =================================================================
        # Score range: ~0.5 to ~2.0
        # Map to channel size range: min to max
        # Score of 1.0 = default size
        # Score of 0.5 = min size
        # Score of 2.0 = max size

        if weighted_score <= 1.0:
            # Below average: scale between min and default
            ratio = (weighted_score - 0.5) / 0.5  # 0.0 to 1.0
            size_range = default_channel_sats - min_channel_sats
            recommended_size = min_channel_sats + int(size_range * ratio)
        else:
            # Above average: scale between default and max
            ratio = (weighted_score - 1.0) / 1.0  # 0.0 to 1.0
            size_range = max_channel_sats - default_channel_sats
            recommended_size = default_channel_sats + int(size_range * ratio)

        # =================================================================
        # Apply Hard Limits
        # =================================================================
        size_before_limits = recommended_size

        # Never exceed available liquidity (with reserve)
        max_from_liquidity = int(available_for_channel * 0.5)  # Max 50% of available
        recommended_size = min(recommended_size, max_from_liquidity)

        # Never exceed available budget (if provided)
        budget_limited = False
        if available_budget_sats is not None and available_budget_sats > 0:
            if recommended_size > available_budget_sats:
                recommended_size = available_budget_sats
                budget_limited = True
            factors['available_budget_sats'] = available_budget_sats
            factors['budget_limited'] = budget_limited

        # Ensure within config bounds
        recommended_size = max(min_channel_sats, min(recommended_size, max_channel_sats))

        # If budget is less than minimum, we can't open this channel
        if available_budget_sats is not None and available_budget_sats < min_channel_sats:
            factors['insufficient_budget'] = True
            factors['budget_shortfall_sats'] = min_channel_sats - available_budget_sats

        factors['size_before_limits'] = size_before_limits
        factors['max_from_liquidity'] = max_from_liquidity

        # =================================================================
        # Generate Reasoning
        # =================================================================
        reasoning_parts = []

        if capacity_score >= 1.5:
            reasoning_parts.append(f"large target ({btc_capacity:.1f} BTC)")
        elif capacity_score <= 0.7:
            reasoning_parts.append(f"small target ({btc_capacity:.1f} BTC)")

        if share_score >= 1.5:
            reasoning_parts.append(f"underserved ({share_gap*100:.1f}% gap)")

        if routing_score >= 1.5:
            reasoning_parts.append(f"high routing potential ({target_channel_count} channels)")
        elif routing_score <= 0.8:
            reasoning_parts.append(f"low connectivity ({target_channel_count} channels)")

        if liquidity_score <= 0.7:
            reasoning_parts.append("liquidity constrained")

        if economics_score >= 1.3:
            reasoning_parts.append(f"favorable economics ({expected_annual_return*100:.1f}% expected)")

        # Phase 6.3: Add quality to reasoning
        if quality_confidence >= 0.3:
            if quality_factor >= 1.5:
                reasoning_parts.append(f"excellent quality ({quality_score:.2f})")
            elif quality_factor >= 1.0:
                reasoning_parts.append(f"good quality ({quality_score:.2f})")
            elif quality_factor < 0.8:
                reasoning_parts.append(f"quality concern ({quality_score:.2f}/{quality_recommendation})")

        # Add budget constraints to reasoning
        if budget_limited:
            reasoning_parts.append(f"budget-limited to {available_budget_sats:,} sats")
        if factors.get('insufficient_budget'):
            reasoning_parts.append(f"INSUFFICIENT BUDGET (need {min_channel_sats:,}, have {available_budget_sats:,})")

        if reasoning_parts:
            reasoning = f"Size factors: {', '.join(reasoning_parts)}"
        else:
            reasoning = "Standard sizing applied"

        self._log(
            f"Sizing for {target[:16]}...: {recommended_size:,} sats "
            f"(score={weighted_score:.2f}, quality={quality_score:.2f}, {reasoning})"
        )

        return ChannelSizeResult(
            recommended_size_sats=recommended_size,
            factors=factors,
            reasoning=reasoning
        )


# =============================================================================
# PLANNER CLASS
# =============================================================================

class Planner:
    """
    Topology optimization engine for the Hive swarm.

    Analyzes network topology to:
    1. Detect targets where Hive has excessive market share (saturation)
    2. Issue clboss-ignore to prevent further capital accumulation
    3. Release ignores when saturation drops below threshold

    Thread Safety:
    - Uses config snapshot pattern (cfg passed to run_cycle)
    - Network cache is refreshed per-cycle
    - No sleeping inside run_cycle
    """

    def __init__(self, state_manager, database, bridge, clboss_bridge, plugin=None,
                 intent_manager=None, decision_engine=None):
        """
        Initialize the Planner.

        Args:
            state_manager: StateManager for accessing Hive peer states
            database: HiveDatabase for logging and membership data
            bridge: Integration Bridge for cl-revenue-ops
            clboss_bridge: CLBossBridge for ignore/unignore operations
            plugin: Plugin reference for RPC and logging
            intent_manager: IntentManager for coordinated channel opens
            decision_engine: DecisionEngine for governance decisions (Phase 7)
        """
        self.state_manager = state_manager
        self.db = database
        self.bridge = bridge
        self.clboss = clboss_bridge
        self.plugin = plugin
        self.intent_manager = intent_manager
        self.decision_engine = decision_engine

        # Quality scorer for peer evaluation (Phase 6.2)
        if PeerQualityScorer and database:
            self.quality_scorer = PeerQualityScorer(database, plugin)
        else:
            self.quality_scorer = None

        # Network cache (refreshed each cycle)
        self._network_cache: Dict[str, List[ChannelInfo]] = {}
        self._network_cache_time: int = 0

        # Track currently ignored peers (to avoid duplicate ignores)
        self._ignored_peers: Set[str] = set()

        # Track expansion proposals this cycle (rate limiting)
        self._expansions_this_cycle: int = 0

    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[Planner] {msg}", level=level)

    # =========================================================================
    # NETWORK CACHE
    # =========================================================================

    def _refresh_network_cache(self, force: bool = False) -> bool:
        """
        Refresh the network channel cache from listchannels.

        Implements efficient caching to minimize RPC load.
        Deduplicates bidirectional channels (A->B and B->A counted once).

        Args:
            force: Force refresh even if cache is fresh

        Returns:
            True if cache was refreshed successfully, False on error
        """
        now = int(time.time())

        # Use cached data if still fresh
        if not force and (now - self._network_cache_time) < NETWORK_CACHE_TTL_SECONDS:
            return True

        if not self.plugin:
            self._log("Cannot refresh network cache: no plugin reference", level='warn')
            return False

        try:
            # Fetch all public channels
            result = self.plugin.rpc.listchannels()
            channels_raw = result.get('channels', [])

            # Build capacity map: target -> list of channels TO that target
            # Deduplicate: for bidirectional channels, count capacity once
            capacity_map: Dict[str, List[ChannelInfo]] = {}
            seen_pairs: Set[str] = set()

            for ch in channels_raw:
                source = ch.get('source', '')
                dest = ch.get('destination', '')
                scid = ch.get('short_channel_id', '')

                if not source or not dest or not scid:
                    continue

                # Parse capacity (may be int or dict with msat)
                capacity_raw = ch.get('amount_msat') or ch.get('satoshis', 0)
                if isinstance(capacity_raw, dict):
                    capacity_sats = capacity_raw.get('msat', 0) // 1000
                elif isinstance(capacity_raw, str) and capacity_raw.endswith('msat'):
                    capacity_sats = int(capacity_raw[:-4]) // 1000
                elif isinstance(capacity_raw, int):
                    # Could be msat or sats depending on field
                    if capacity_raw > 10_000_000_000:  # Likely msat
                        capacity_sats = capacity_raw // 1000
                    else:
                        capacity_sats = capacity_raw
                else:
                    capacity_sats = 0

                active = ch.get('active', True)

                # Create normalized pair key for dedup (smaller pubkey first)
                pair_key = tuple(sorted([source, dest])) + (scid,)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                info = ChannelInfo(
                    source=source,
                    destination=dest,
                    short_channel_id=scid,
                    capacity_sats=capacity_sats,
                    active=active
                )

                # Index by destination (target)
                if dest not in capacity_map:
                    capacity_map[dest] = []
                capacity_map[dest].append(info)

                # Also index by source (for bidirectional lookup)
                if source not in capacity_map:
                    capacity_map[source] = []
                capacity_map[source].append(info)

            self._network_cache = capacity_map
            self._network_cache_time = now

            self._log(f"Network cache refreshed: {len(seen_pairs)} channels, "
                     f"{len(capacity_map)} targets", level='debug')
            return True

        except RpcError as e:
            self._log(f"listchannels RPC failed: {e}", level='warn')
            return False
        except Exception as e:
            self._log(f"Network cache refresh error: {e}", level='warn')
            return False

    def _get_public_capacity_to_target(self, target: str) -> int:
        """
        Get total public network capacity to a target.

        Args:
            target: Target node pubkey

        Returns:
            Total capacity in satoshis (0 if not found)
        """
        channels = self._network_cache.get(target, [])
        return sum(ch.capacity_sats for ch in channels if ch.active)

    # =========================================================================
    # SATURATION LOGIC
    # =========================================================================

    def _get_hive_members(self) -> List[str]:
        """Get list of Hive member pubkeys."""
        if not self.db:
            return []
        members = self.db.get_all_members()
        return [m['peer_id'] for m in members if m.get('tier') in ('member', 'admin')]

    def _get_hive_capacity_to_target(self, target: str, hive_members: List[str]) -> int:
        """
        Calculate total Hive capacity to a target.

        SECURITY: Clamps gossip-reported capacity to public listchannels maximum.
        This prevents attackers from inflating saturation via fake gossip.

        Args:
            target: Target node pubkey
            hive_members: List of Hive member pubkeys

        Returns:
            Total Hive capacity in satoshis (clamped to public reality)
        """
        if not self.state_manager:
            return 0

        # Get all known Hive peer states (list -> dict for lookup)
        all_states_list = self.state_manager.get_all_peer_states()
        all_states = {s.peer_id: s for s in all_states_list}

        # Get public capacity for reality check
        public_channels = self._network_cache.get(target, [])

        # Build map: (source, dest) -> max public capacity
        public_capacity_map: Dict[Tuple[str, str], int] = {}
        for ch in public_channels:
            key = (ch.source, ch.destination)
            public_capacity_map[key] = max(
                public_capacity_map.get(key, 0),
                ch.capacity_sats
            )
            # Also check reverse direction
            key_rev = (ch.destination, ch.source)
            public_capacity_map[key_rev] = max(
                public_capacity_map.get(key_rev, 0),
                ch.capacity_sats
            )

        total_hive_capacity = 0

        for member_pubkey in hive_members:
            state = all_states.get(member_pubkey)
            if not state:
                continue

            # Check if this member's topology includes the target
            topology = getattr(state, 'topology', []) or []
            if target not in topology:
                continue

            # Get claimed capacity from gossip
            claimed_capacity = getattr(state, 'capacity_sats', 0)

            # SECURITY: Clamp to public reality
            # Look up the actual public capacity for this (member, target) pair
            public_max = public_capacity_map.get((member_pubkey, target), 0)
            if public_max == 0:
                # Also try reverse
                public_max = public_capacity_map.get((target, member_pubkey), 0)

            if public_max > 0:
                clamped_capacity = min(claimed_capacity, public_max)
            else:
                # No public channel found - don't trust gossip at all
                clamped_capacity = 0

            total_hive_capacity += clamped_capacity

        return total_hive_capacity

    def _calculate_hive_share(self, target: str, cfg) -> SaturationResult:
        """
        Calculate Hive's market share for a target.

        Args:
            target: Target node pubkey
            cfg: Config snapshot for thresholds

        Returns:
            SaturationResult with share calculation
        """
        hive_members = self._get_hive_members()

        # Get public capacity (denominator)
        public_capacity = self._get_public_capacity_to_target(target)

        # Get Hive capacity (numerator, clamped)
        hive_capacity = self._get_hive_capacity_to_target(target, hive_members)

        # Calculate share
        if public_capacity <= 0:
            hive_share = 0.0
        else:
            hive_share = hive_capacity / public_capacity

        # Check saturation threshold
        is_saturated = hive_share >= cfg.market_share_cap_pct

        # Check release threshold (hysteresis)
        should_release = hive_share < SATURATION_RELEASE_THRESHOLD_PCT

        return SaturationResult(
            target=target,
            hive_capacity_sats=hive_capacity,
            public_capacity_sats=public_capacity,
            hive_share_pct=hive_share,
            is_saturated=is_saturated,
            should_release=should_release
        )

    def get_saturated_targets(self, cfg) -> List[SaturationResult]:
        """
        Get all targets where Hive exceeds market share cap.

        Args:
            cfg: Config snapshot

        Returns:
            List of SaturationResult for saturated targets
        """
        saturated = []

        # Check all known targets in network cache
        for target in self._network_cache.keys():
            # Skip targets below minimum capacity (anti-Sybil)
            public_capacity = self._get_public_capacity_to_target(target)
            if public_capacity < MIN_TARGET_CAPACITY_SATS:
                continue

            result = self._calculate_hive_share(target, cfg)
            if result.is_saturated:
                saturated.append(result)

        return saturated

    # =========================================================================
    # GUARD MECHANISM
    # =========================================================================

    def _enforce_saturation(self, cfg, run_id: str) -> List[Dict[str, Any]]:
        """
        Enforce saturation limits by issuing clboss-ignore.

        SECURITY CONSTRAINTS:
        - Max 5 new ignores per cycle (abort if exceeded)
        - Idempotent: skip already-ignored peers
        - Log all decisions to hive_planner_log

        Args:
            cfg: Config snapshot
            run_id: Unique identifier for this cycle

        Returns:
            List of decision records for testing
        """
        decisions = []

        # Refresh network cache
        if not self._refresh_network_cache():
            self._log("Failed to refresh network cache, aborting saturation enforcement", level='warn')
            self.db.log_planner_action(
                action_type='saturation_check',
                result='failed',
                details={'reason': 'network_cache_refresh_failed', 'run_id': run_id}
            )
            return decisions

        # Get saturated targets
        saturated_targets = self.get_saturated_targets(cfg)

        # Count new ignores needed
        new_ignores_needed = []
        for result in saturated_targets:
            if result.target not in self._ignored_peers:
                new_ignores_needed.append(result)

        # SECURITY: Check rate limit
        if len(new_ignores_needed) > MAX_IGNORES_PER_CYCLE:
            self._log(
                f"Mass Saturation Detected: {len(new_ignores_needed)} targets exceed cap. "
                f"Aborting cycle (max {MAX_IGNORES_PER_CYCLE}/cycle).",
                level='warn'
            )
            self.db.log_planner_action(
                action_type='saturation_check',
                result='aborted',
                details={
                    'reason': 'mass_saturation_detected',
                    'targets_count': len(new_ignores_needed),
                    'max_allowed': MAX_IGNORES_PER_CYCLE,
                    'run_id': run_id
                }
            )
            decisions.append({
                'action': 'abort',
                'reason': 'mass_saturation_detected',
                'targets_count': len(new_ignores_needed)
            })
            return decisions

        # Issue ignores for new saturated targets
        ignores_issued = 0
        for result in new_ignores_needed:
            if ignores_issued >= MAX_IGNORES_PER_CYCLE:
                break

            # Check if CLBoss is available (optional integration)
            if not self.clboss or not self.clboss._available:
                # CLBoss not installed - this is fine, hive uses native expansion control
                # Still log for saturation analytics
                self.db.log_planner_action(
                    action_type='saturation_detected',
                    result='info',
                    target=result.target,
                    details={
                        'note': 'clboss_not_installed',
                        'hive_share_pct': round(result.hive_share_pct, 4),
                        'run_id': run_id
                    }
                )
                decisions.append({
                    'action': 'saturation_detected',
                    'target': result.target,
                    'hive_share_pct': round(result.hive_share_pct, 4),
                    'note': 'clboss_not_installed'
                })
                continue

            # Issue clboss-unmanage for 'open' tag (prevent channel opens)
            success = self.clboss.unmanage_open(result.target)
            if success:
                self._ignored_peers.add(result.target)
                ignores_issued += 1

                self._log(
                    f"Ignored saturated target {result.target[:16]}... "
                    f"(share={result.hive_share_pct:.1%})"
                )
                self.db.log_planner_action(
                    action_type='ignore',
                    result='success',
                    target=result.target,
                    details={
                        'hive_share_pct': round(result.hive_share_pct, 4),
                        'hive_capacity_sats': result.hive_capacity_sats,
                        'public_capacity_sats': result.public_capacity_sats,
                        'run_id': run_id
                    }
                )
                decisions.append({
                    'action': 'ignore',
                    'target': result.target,
                    'result': 'success',
                    'hive_share_pct': result.hive_share_pct
                })
            else:
                self._log(f"Failed to ignore {result.target[:16]}...", level='warn')
                self.db.log_planner_action(
                    action_type='ignore',
                    result='failed',
                    target=result.target,
                    details={
                        'hive_share_pct': round(result.hive_share_pct, 4),
                        'run_id': run_id
                    }
                )
                decisions.append({
                    'action': 'ignore',
                    'target': result.target,
                    'result': 'failed'
                })

        # Log summary
        self.db.log_planner_action(
            action_type='saturation_check',
            result='completed',
            details={
                'saturated_targets': len(saturated_targets),
                'new_ignores_issued': ignores_issued,
                'run_id': run_id
            }
        )

        return decisions

    def _release_saturation(self, cfg, run_id: str) -> List[Dict[str, Any]]:
        """
        Release ignores for targets that are no longer saturated.

        Uses hysteresis (15% threshold) to prevent flip-flopping.

        Args:
            cfg: Config snapshot
            run_id: Unique identifier for this cycle

        Returns:
            List of decision records
        """
        decisions = []

        # Check currently ignored peers
        peers_to_release = []
        for peer in list(self._ignored_peers):
            result = self._calculate_hive_share(peer, cfg)
            if result.should_release:
                peers_to_release.append((peer, result))

        # Issue unignores
        for peer, result in peers_to_release:
            if not self.clboss or not self.clboss._available:
                continue

            success = self.clboss.manage_open(peer)
            if success:
                self._ignored_peers.discard(peer)

                self._log(
                    f"Released ignore on {peer[:16]}... "
                    f"(share={result.hive_share_pct:.1%} < {SATURATION_RELEASE_THRESHOLD_PCT:.0%})"
                )
                self.db.log_planner_action(
                    action_type='unignore',
                    result='success',
                    target=peer,
                    details={
                        'hive_share_pct': round(result.hive_share_pct, 4),
                        'run_id': run_id
                    }
                )
                decisions.append({
                    'action': 'unignore',
                    'target': peer,
                    'result': 'success'
                })

        return decisions

    # =========================================================================
    # EXPANSION LOGIC (Ticket 6-02)
    # =========================================================================

    def get_underserved_targets(self, cfg, include_low_quality: bool = False) -> List[UnderservedResult]:
        """
        Get targets with low Hive coverage that are candidates for expansion.

        Criteria:
        - Public capacity > MIN_TARGET_CAPACITY_SATS (1 BTC)
        - Hive share < UNDERSERVED_THRESHOLD_PCT (5%)
        - Target exists in public graph (verified via network cache)
        - Quality score >= MIN_QUALITY_SCORE (Phase 6.2) or insufficient data

        Args:
            cfg: Config snapshot
            include_low_quality: If True, include targets with low quality scores
                                 (they will be flagged but not filtered)

        Returns:
            List of UnderservedResult sorted by combined score (highest first)
        """
        underserved = []

        for target in self._network_cache.keys():
            # Check minimum capacity (anti-Sybil)
            public_capacity = self._get_public_capacity_to_target(target)
            if public_capacity < MIN_TARGET_CAPACITY_SATS:
                continue

            # Calculate Hive share
            result = self._calculate_hive_share(target, cfg)

            # Check if underserved (< 5% Hive share)
            if result.hive_share_pct >= UNDERSERVED_THRESHOLD_PCT:
                continue

            # Phase 6.2: Get quality score for the target
            quality_score = 0.5  # Default neutral
            quality_confidence = 0.0
            quality_recommendation = "neutral"

            if self.quality_scorer:
                quality_result = self.quality_scorer.calculate_score(
                    target, days=QUALITY_SCORE_DAYS
                )
                quality_score = quality_result.overall_score
                quality_confidence = quality_result.confidence
                quality_recommendation = quality_result.recommendation

                # Filter out low-quality targets unless explicitly included
                if not include_low_quality:
                    # Skip targets with 'avoid' recommendation
                    if quality_recommendation == "avoid":
                        self._log(
                            f"Skipping {target[:16]}... - quality='avoid' "
                            f"(score={quality_score:.2f}, confidence={quality_confidence:.2f})",
                            level='debug'
                        )
                        continue

                    # Skip targets below minimum score with sufficient confidence
                    if quality_confidence >= 0.3 and quality_score < MIN_QUALITY_SCORE:
                        self._log(
                            f"Skipping {target[:16]}... - low quality score "
                            f"({quality_score:.2f} < {MIN_QUALITY_SCORE})",
                            level='debug'
                        )
                        continue

            # Calculate base score: higher capacity + lower Hive share = more attractive
            # Score = capacity_btc * (1 - hive_share)
            capacity_btc = public_capacity / 100_000_000
            base_score = capacity_btc * (1 - result.hive_share_pct)

            # Phase 6.2: Factor in quality score
            # Combined score = base_score * quality_multiplier
            # Quality multiplier ranges from 0.5 (avoid) to 1.5 (excellent)
            if quality_confidence > 0.3:
                # Quality data is meaningful - apply multiplier
                quality_multiplier = 0.5 + quality_score  # 0.5 to 1.5
            else:
                # Low confidence - use neutral multiplier
                quality_multiplier = 1.0

            combined_score = base_score * quality_multiplier

            underserved.append(UnderservedResult(
                target=target,
                public_capacity_sats=public_capacity,
                hive_share_pct=result.hive_share_pct,
                score=combined_score,
                quality_score=quality_score,
                quality_confidence=quality_confidence,
                quality_recommendation=quality_recommendation
            ))

        # Sort by combined score (highest first)
        underserved.sort(key=lambda r: r.score, reverse=True)
        return underserved

    def _get_local_onchain_balance(self) -> int:
        """
        Get local confirmed onchain balance.

        Returns:
            Confirmed balance in satoshis, or 0 on error
        """
        if not self.plugin:
            return 0

        try:
            funds = self.plugin.rpc.listfunds()
            outputs = funds.get('outputs', [])

            confirmed_sats = 0
            for output in outputs:
                # Only count confirmed outputs
                if output.get('status') == 'confirmed':
                    # Handle both msat and satoshi formats
                    amount = output.get('amount_msat')
                    if amount is not None:
                        if isinstance(amount, int):
                            confirmed_sats += amount // 1000
                        elif isinstance(amount, str) and amount.endswith('msat'):
                            confirmed_sats += int(amount[:-4]) // 1000
                    else:
                        # Fallback to value field
                        confirmed_sats += output.get('value', 0)

            return confirmed_sats
        except RpcError as e:
            self._log(f"listfunds RPC failed: {e}", level='warn')
            return 0
        except Exception as e:
            self._log(f"Error getting onchain balance: {e}", level='warn')
            return 0

    def _get_target_channel_count(self, target: str) -> int:
        """
        Get the number of channels a target node has.

        Uses the network cache to count unique channel partners.

        Args:
            target: Target node pubkey

        Returns:
            Number of channels the target has
        """
        if target not in self._network_cache:
            return 0

        # Count unique partners (each channel has source/destination)
        partners = set()
        for channel in self._network_cache.get(target, []):
            # Add both ends of each channel
            if channel.source == target:
                partners.add(channel.destination)
            else:
                partners.add(channel.source)

        return len(partners)

    def _get_avg_fee_rate(self) -> int:
        """
        Get average fee rate from cl-revenue-ops if available.

        Returns:
            Average fee rate in ppm, or 500 as default
        """
        if not self.bridge:
            return 500

        try:
            # Try to get policy info from bridge
            policy = self.bridge.get_current_policy()
            if policy and 'avg_fee_ppm' in policy:
                return policy['avg_fee_ppm']
        except Exception:
            pass

        return 500  # Default if unavailable

    def _has_pending_intent(self, target: str) -> bool:
        """
        Check if there's already a pending intent for this target.

        Args:
            target: Target pubkey to check

        Returns:
            True if a pending intent exists, False otherwise
        """
        if not self.db:
            return False

        pending = self.db.get_pending_intents()
        for intent in pending:
            if intent.get('target') == target and intent.get('status') == 'pending':
                return True
        return False

    def _should_skip_target(self, target: str, cooldown_seconds: int = 86400) -> tuple[bool, str]:
        """
        Check if a target should be skipped due to existing proposals or rejections.

        This consolidates all the checks for whether we should skip proposing
        a channel to this target.

        Args:
            target: Target pubkey to check
            cooldown_seconds: Cooldown period after rejection (default: 24 hours)

        Returns:
            Tuple of (should_skip, reason)
        """
        if not self.db:
            return False, ""

        # Check for pending intent
        if self._has_pending_intent(target):
            return True, "pending_intent"

        # Check for pending action in pending_actions table
        if self.db.has_pending_action_for_target(target):
            return True, "pending_action"

        # Check for recent rejection
        if self.db.was_recently_rejected(target, cooldown_seconds):
            return True, "recently_rejected"

        return False, ""

    def _propose_expansion(self, cfg, run_id: str) -> List[Dict[str, Any]]:
        """
        Propose channel expansions to underserved targets.

        Security constraints:
        - Only when planner_enable_expansions is True
        - Max 1 expansion per cycle
        - Must have sufficient onchain funds (> 2 * MIN_CHANNEL_SIZE)
        - Target must exist in public graph
        - No existing pending intent for target
        - In advisor mode, actions are queued for AI/human approval

        Args:
            cfg: Config snapshot
            run_id: Unique identifier for this cycle

        Returns:
            List of decision records
        """
        decisions = []

        # Check if expansions are enabled
        if not cfg.planner_enable_expansions:
            return decisions

        # Check rate limit
        if self._expansions_this_cycle >= MAX_EXPANSIONS_PER_CYCLE:
            self._log("Expansion rate limit reached for this cycle", level='debug')
            return decisions

        # Check if we have an intent manager
        if not self.intent_manager:
            self._log("IntentManager not available, skipping expansions", level='debug')
            return decisions

        # Check onchain balance
        onchain_balance = self._get_local_onchain_balance()
        min_channel_size = getattr(cfg, 'planner_min_channel_sats', MIN_CHANNEL_SIZE_SATS_FALLBACK)
        min_required = min_channel_size * 2  # Need at least 2x min for fees/reserve

        if onchain_balance < min_required:
            self._log(
                f"Insufficient onchain funds for expansion: "
                f"{onchain_balance} < {min_required} sats",
                level='debug'
            )
            self.db.log_planner_action(
                action_type='expansion',
                result='skipped',
                details={
                    'reason': 'insufficient_funds',
                    'onchain_balance': onchain_balance,
                    'min_required': min_required,
                    'run_id': run_id
                }
            )
            return decisions

        # Get underserved targets
        underserved = self.get_underserved_targets(cfg)
        if not underserved:
            self._log("No underserved targets found", level='debug')
            return decisions

        # Get rejection cooldown from config (default 24 hours)
        rejection_cooldown = getattr(cfg, 'rejection_cooldown_seconds', 86400)

        # Find a target without pending intent, pending action, or recent rejection
        selected_target = None
        skipped_reasons = {}
        for candidate in underserved:
            should_skip, reason = self._should_skip_target(candidate.target, rejection_cooldown)
            if not should_skip:
                selected_target = candidate
                break
            else:
                skipped_reasons[candidate.target[:16]] = reason

        if not selected_target:
            if skipped_reasons:
                self._log(
                    f"All underserved targets skipped: {skipped_reasons}",
                    level='debug'
                )
            else:
                self._log("All underserved targets have pending intents", level='debug')
            return decisions

        # Create intent and potentially broadcast
        # Phase 6.2: Include quality information in log
        self._log(
            f"Proposing expansion to {selected_target.target[:16]}... "
            f"(share={selected_target.hive_share_pct:.1%}, "
            f"capacity={selected_target.public_capacity_sats} sats, "
            f"quality={selected_target.quality_score:.2f}/{selected_target.quality_recommendation})"
        )

        try:
            # Create the intent
            intent = self.intent_manager.create_intent(
                intent_type=IntentType.CHANNEL_OPEN.value if hasattr(IntentType.CHANNEL_OPEN, 'value') else IntentType.CHANNEL_OPEN,
                target=selected_target.target
            )

            self._expansions_this_cycle += 1

            # Log the decision with quality information (Phase 6.2)
            self.db.log_planner_action(
                action_type='expansion',
                result='proposed',
                target=selected_target.target,
                details={
                    'intent_id': intent.intent_id,
                    'public_capacity_sats': selected_target.public_capacity_sats,
                    'hive_share_pct': round(selected_target.hive_share_pct, 4),
                    'score': round(selected_target.score, 4),
                    'quality_score': round(selected_target.quality_score, 3),
                    'quality_confidence': round(selected_target.quality_confidence, 3),
                    'quality_recommendation': selected_target.quality_recommendation,
                    'onchain_balance': onchain_balance,
                    'run_id': run_id
                }
            )

            decisions.append({
                'action': 'expansion_proposed',
                'target': selected_target.target,
                'intent_id': intent.intent_id,
                'hive_share_pct': selected_target.hive_share_pct
            })

            # Use DecisionEngine for governance decision if available
            if self.decision_engine:
                # Calculate proposed channel size using intelligent sizing algorithm
                default_size = getattr(cfg, 'planner_default_channel_sats', 5_000_000)
                max_size = getattr(cfg, 'planner_max_channel_sats', 50_000_000)
                market_share_cap = getattr(cfg, 'market_share_cap_pct', 0.20)

                # Get target's channel count for routing potential calculation
                target_channel_count = self._get_target_channel_count(selected_target.target)
                avg_fee_rate = self._get_avg_fee_rate()

                # Phase 6.3: Use intelligent channel sizer with quality scoring
                sizer = ChannelSizer(plugin=self.plugin, quality_scorer=self.quality_scorer)
                sizing_result = sizer.calculate_size(
                    target=selected_target.target,
                    target_capacity_sats=selected_target.public_capacity_sats,
                    target_channel_count=target_channel_count,
                    hive_share_pct=selected_target.hive_share_pct,
                    target_share_cap=market_share_cap * 0.5,  # Aim for half of cap
                    onchain_balance_sats=onchain_balance,
                    min_channel_sats=min_channel_size,
                    max_channel_sats=max_size,
                    default_channel_sats=default_size,
                    avg_fee_rate_ppm=avg_fee_rate,
                    # Phase 6.3: Pass quality data from UnderservedResult
                    quality_score=selected_target.quality_score,
                    quality_confidence=selected_target.quality_confidence,
                    quality_recommendation=selected_target.quality_recommendation,
                )

                proposed_size = sizing_result.recommended_size_sats

                # Build context for governance decision (includes sizing factors)
                context = {
                    'intent_id': intent.intent_id,
                    'public_capacity_sats': selected_target.public_capacity_sats,
                    'hive_share_pct': round(selected_target.hive_share_pct, 4),
                    'onchain_balance': onchain_balance,
                    'amount_sats': proposed_size,  # For budget tracking
                    'channel_size_sats': proposed_size,  # Recommended channel size
                    'min_channel_sats': min_channel_size,
                    'max_channel_sats': max_size,
                    'sizing_factors': sizing_result.factors,
                    'sizing_reasoning': sizing_result.reasoning,
                    'target_channel_count': target_channel_count,
                    # Phase 6.3: Include quality information
                    'quality_score': round(selected_target.quality_score, 3),
                    'quality_confidence': round(selected_target.quality_confidence, 3),
                    'quality_recommendation': selected_target.quality_recommendation,
                }

                # Define executor for channel_open (broadcasts intent)
                def channel_open_executor(target, ctx):
                    self._broadcast_intent(intent)

                self.decision_engine.register_executor('channel_open', channel_open_executor)

                # Propose action through governance
                gov_response = self.decision_engine.propose_action(
                    action_type='channel_open',
                    target=selected_target.target,
                    context=context,
                    cfg=cfg
                )

                # Record governance decision in decisions list
                from modules.governance import DecisionResult
                if gov_response.result == DecisionResult.APPROVED:
                    decisions[-1]['broadcast'] = True
                    decisions[-1]['governance_result'] = 'approved'
                elif gov_response.result == DecisionResult.QUEUED:
                    decisions[-1]['broadcast'] = False
                    decisions[-1]['pending_action_id'] = gov_response.action_id
                    decisions[-1]['governance_result'] = 'queued'
                elif gov_response.result == DecisionResult.DENIED:
                    decisions[-1]['broadcast'] = False
                    decisions[-1]['governance_result'] = 'denied'
                    decisions[-1]['governance_reason'] = gov_response.reason
                else:
                    decisions[-1]['broadcast'] = False
                    decisions[-1]['governance_result'] = 'error'
            else:
                # Fallback: Manual governance handling (backwards compatibility)
                if cfg.governance_mode == 'failsafe':
                    self._broadcast_intent(intent)
                    decisions[-1]['broadcast'] = True
                else:
                    # In advisor mode, queue to pending_actions for AI/human approval
                    action_id = self.db.add_pending_action(
                        action_type='channel_open',
                        payload={
                            'intent_id': intent.intent_id,
                            'target': selected_target.target,
                            'public_capacity_sats': selected_target.public_capacity_sats,
                            'hive_share_pct': round(selected_target.hive_share_pct, 4),
                            'onchain_balance': onchain_balance,
                        },
                        expires_hours=24
                    )
                    self._log(
                        f"Action queued for approval (id={action_id}, mode={cfg.governance_mode})",
                        level='info'
                    )
                    decisions[-1]['broadcast'] = False
                    decisions[-1]['pending_action_id'] = action_id

        except Exception as e:
            self._log(f"Failed to create expansion intent: {e}", level='warn')
            self.db.log_planner_action(
                action_type='expansion',
                result='failed',
                target=selected_target.target,
                details={
                    'error': str(e),
                    'run_id': run_id
                }
            )
            decisions.append({
                'action': 'expansion_failed',
                'target': selected_target.target,
                'error': str(e)
            })

        return decisions

    def _broadcast_intent(self, intent) -> bool:
        """
        Broadcast an intent to all Hive members.

        Args:
            intent: Intent object to broadcast

        Returns:
            True if broadcast was successful, False otherwise
        """
        if not self.plugin or not self.db or not self.intent_manager:
            return False

        try:
            # Create intent message payload
            payload = self.intent_manager.create_intent_message(intent)
            msg_bytes = serialize(HiveMessageType.INTENT, payload)

            # Get all Hive members
            members = self.db.get_all_members()
            our_pubkey = self.intent_manager.our_pubkey

            broadcast_count = 0
            for member in members:
                member_id = member.get('peer_id')
                if not member_id or member_id == our_pubkey:
                    continue

                try:
                    self.plugin.rpc.call("sendcustommsg", {
                        "node_id": member_id,
                        "msg": msg_bytes.hex()
                    })
                    broadcast_count += 1
                except Exception as e:
                    self._log(
                        f"Failed to send INTENT to {member_id[:16]}...: {e}",
                        level='debug'
                    )

            self._log(f"Broadcast INTENT to {broadcast_count} peers")
            return broadcast_count > 0

        except Exception as e:
            self._log(f"Broadcast failed: {e}", level='warn')
            return False

    # =========================================================================
    # RUN CYCLE
    # =========================================================================

    def run_cycle(self, cfg, *, shutdown_event=None, now=None, run_id=None) -> List[Dict]:
        """
        Execute one planning cycle.

        This is the main entry point called by the planner_loop thread.
        No sleeping inside this method - caller handles timing.

        Args:
            cfg: Config snapshot (use config.snapshot() at cycle start)
            shutdown_event: Threading event to check for shutdown
            now: Current timestamp (for testing)
            run_id: Unique identifier for this cycle

        Returns:
            List of decision records for testing
        """
        if shutdown_event and shutdown_event.is_set():
            return []

        if now is None:
            now = int(time.time())
        if run_id is None:
            run_id = secrets.token_hex(8)

        self._log(f"Starting planner cycle (run_id={run_id})")
        decisions = []

        # Reset per-cycle counters
        self._expansions_this_cycle = 0

        try:
            # Refresh network cache first
            if not self._refresh_network_cache(force=True):
                self._log("Network cache refresh failed, skipping cycle", level='warn')
                self.db.log_planner_action(
                    action_type='cycle',
                    result='failed',
                    details={'reason': 'cache_refresh_failed', 'run_id': run_id}
                )
                return []

            # Enforce saturation limits (Guard mechanism)
            saturation_decisions = self._enforce_saturation(cfg, run_id)
            decisions.extend(saturation_decisions)

            # Release over-ignored peers (best effort)
            release_decisions = self._release_saturation(cfg, run_id)
            decisions.extend(release_decisions)

            # Propose channel expansions (Ticket 6-02)
            expansion_decisions = self._propose_expansion(cfg, run_id)
            decisions.extend(expansion_decisions)

            self._log(f"Planner cycle complete (run_id={run_id}): {len(decisions)} decisions")
            self.db.log_planner_action(
                action_type='cycle',
                result='completed',
                details={
                    'decisions_count': len(decisions),
                    'run_id': run_id
                }
            )

        except Exception as e:
            self._log(f"Planner cycle error: {e}", level='warn')
            self.db.log_planner_action(
                action_type='cycle',
                result='error',
                details={'error': str(e), 'run_id': run_id}
            )

        return decisions

    # =========================================================================
    # STATISTICS
    # =========================================================================

    def get_planner_stats(self) -> Dict[str, Any]:
        """Get current planner statistics."""
        return {
            'network_cache_size': len(self._network_cache),
            'network_cache_age_seconds': int(time.time()) - self._network_cache_time,
            'ignored_peers_count': len(self._ignored_peers),
            'ignored_peers': list(self._ignored_peers)[:10],  # Limit for display
            'max_ignores_per_cycle': MAX_IGNORES_PER_CYCLE,
            'saturation_release_threshold_pct': SATURATION_RELEASE_THRESHOLD_PCT,
            'min_target_capacity_sats': MIN_TARGET_CAPACITY_SATS,
        }
