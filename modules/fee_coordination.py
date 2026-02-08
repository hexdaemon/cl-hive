"""
Fee Coordination Module (Phase 2 - Fee Coordination)

Provides coordinated fee management for the hive fleet:
- Flow corridor assignment (route ownership)
- Adaptive fee controller (pheromone-based learning)
- Fleet fee floor/ceiling enforcement
- Stigmergic fee coordination (indirect coordination via markers)
- Mycelium defense system (collective defense against draining peers)

This module integrates with cl-revenue-ops for fee execution while
maintaining coordination at the cl-hive layer.
"""

import math
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from . import network_metrics

# =============================================================================
# CONSTANTS
# =============================================================================

# Fleet fee bounds
FLEET_FEE_FLOOR_PPM = 50          # Never go below this (avoid drain)
FLEET_FEE_CEILING_PPM = 2500      # Don't price out flow
DEFAULT_FEE_PPM = 500             # Default when no signals

# Primary/secondary fee multipliers
PRIMARY_FEE_MULTIPLIER = 1.0      # Primary: competitive fee
SECONDARY_FEE_MULTIPLIER = 1.5    # Secondary: premium for overflow

# Adaptive fee controller (pheromone-based)
BASE_EVAPORATION_RATE = 0.2       # 20% base evaporation per cycle
MIN_EVAPORATION_RATE = 0.1        # Minimum evaporation
MAX_EVAPORATION_RATE = 0.9        # Maximum evaporation
PHEROMONE_EXPLOIT_THRESHOLD = 10.0  # Above this: exploit current fee
PHEROMONE_DEPOSIT_SCALE = 0.001   # Scale factor for deposits

# Stigmergic markers
MARKER_HALF_LIFE_HOURS = 24       # Markers decay with 24-hour half-life
MARKER_MIN_STRENGTH = 0.1         # Below this, markers are ignored

# Mycelium defense
DRAIN_RATIO_THRESHOLD = 5.0       # 5:1 outflow ratio = drain attack
DEFENSE_QUORUM_THRESHOLD = 2      # Min independent reports before fleet defense activates
FAILURE_RATE_THRESHOLD = 0.5      # >50% failures = unreliable peer
WARNING_TTL_HOURS = 24            # Warnings expire after 24 hours
DEFENSIVE_FEE_MAX_MULTIPLIER = 3.0  # Max 3x fee increase for defense

# =============================================================================
# TIME-BASED FEE ADJUSTMENT (Phase 7.4)
# =============================================================================

# Enable time-based fee adjustments
TIME_FEE_ADJUSTMENT_ENABLED = True   # Config: hive-time-fee-enabled

# Maximum adjustment bounds (applied to base fee)
TIME_FEE_MAX_INCREASE_PCT = 0.25     # +25% during peak hours
TIME_FEE_MAX_DECREASE_PCT = 0.15     # -15% during low-activity hours

# Intensity thresholds for triggering adjustments
TIME_FEE_PEAK_INTENSITY = 0.7        # Flow intensity > 70% = peak
TIME_FEE_LOW_INTENSITY = 0.3         # Flow intensity < 30% = low activity

# Minimum pattern confidence to apply adjustment
TIME_FEE_MIN_CONFIDENCE = 0.5        # Require 50% confidence

# Transition smoothing (avoid sudden jumps)
TIME_FEE_TRANSITION_PERIODS = 2      # Smooth over 2 hours
TIME_FEE_CACHE_TTL_HOURS = 1         # Cache adjustments for 1 hour

# =============================================================================
# SALIENCE DETECTION (Noise Filtering)
# =============================================================================
# These thresholds determine what constitutes a "meaningful" change worth acting on.
# Changes below these thresholds are considered noise and should be ignored.

# Fee change salience
SALIENT_FEE_CHANGE_PCT = 0.05        # 5% fee change minimum to be salient
SALIENT_FEE_CHANGE_MIN_PPM = 10      # At least 10 ppm absolute change
SALIENT_FEE_CHANGE_COOLDOWN = 3600   # 1 hour between fee changes per channel

# Balance change salience
SALIENT_BALANCE_CHANGE_PCT = 0.05    # 5% balance shift to be salient
SALIENT_VELOCITY_CHANGE_PCT = 0.10   # 10% velocity change to be salient

# Routing stats salience
SALIENT_SUCCESS_RATE_CHANGE = 0.10   # 10% success rate change to be salient
SALIENT_LATENCY_CHANGE_MS = 100      # 100ms latency change to be salient

# =============================================================================
# CENTRALITY-BASED FEE ADJUSTMENT (Use Case 8)
# =============================================================================
# High-centrality members can charge premium fees (better positioned)
# Low-centrality members may discount to attract traffic

CENTRALITY_FEE_ADJUSTMENT_ENABLED = True    # Config: hive-centrality-fee-enabled
CENTRALITY_FEE_HIGH_THRESHOLD = 0.7         # Above this = premium positioning
CENTRALITY_FEE_LOW_THRESHOLD = 0.3          # Below this = discount positioning
CENTRALITY_FEE_MAX_PREMIUM_PCT = 0.15       # +15% max for high centrality
CENTRALITY_FEE_MAX_DISCOUNT_PCT = 0.10      # -10% max for low centrality


def is_fee_change_salient(
    current_fee: int,
    new_fee: int,
    last_change_time: float = 0
) -> Tuple[bool, str]:
    """
    Determine if a fee change is significant enough to warrant action.

    Returns:
        Tuple of (is_salient, reason)
    """
    # Check cooldown
    now = time.time()
    if last_change_time > 0 and (now - last_change_time) < SALIENT_FEE_CHANGE_COOLDOWN:
        return False, "cooldown_active"

    if current_fee == new_fee:
        return False, "no_change"

    # Calculate absolute and percentage change
    abs_change = abs(new_fee - current_fee)
    pct_change = abs_change / max(current_fee, 1)

    # Must meet BOTH minimum thresholds
    if abs_change < SALIENT_FEE_CHANGE_MIN_PPM:
        return False, f"abs_change_too_small ({abs_change} < {SALIENT_FEE_CHANGE_MIN_PPM} ppm)"

    if pct_change < SALIENT_FEE_CHANGE_PCT:
        return False, f"pct_change_too_small ({pct_change * 100:.1f}% < {SALIENT_FEE_CHANGE_PCT * 100}%)"

    return True, "salient"


def is_balance_change_salient(
    old_balance_pct: float,
    new_balance_pct: float
) -> Tuple[bool, str]:
    """
    Determine if a balance change is significant enough to warrant action.

    Args:
        old_balance_pct: Previous local balance as 0-1 ratio
        new_balance_pct: Current local balance as 0-1 ratio

    Returns:
        Tuple of (is_salient, reason)
    """
    change = abs(new_balance_pct - old_balance_pct)

    if change < SALIENT_BALANCE_CHANGE_PCT:
        return False, f"balance_change_too_small ({change * 100:.1f}% < {SALIENT_BALANCE_CHANGE_PCT * 100}%)"

    return True, "salient"


def is_velocity_change_salient(
    old_velocity: float,
    new_velocity: float
) -> Tuple[bool, str]:
    """
    Determine if a velocity change is significant enough to warrant action.

    Args:
        old_velocity: Previous velocity (sats/hour)
        new_velocity: Current velocity (sats/hour)

    Returns:
        Tuple of (is_salient, reason)
    """
    if old_velocity == 0:
        # Any non-zero velocity from zero is salient
        return new_velocity != 0, "from_zero" if new_velocity != 0 else "no_change"

    pct_change = abs(new_velocity - old_velocity) / abs(old_velocity)

    if pct_change < SALIENT_VELOCITY_CHANGE_PCT:
        return False, f"velocity_change_too_small ({pct_change * 100:.1f}% < {SALIENT_VELOCITY_CHANGE_PCT * 100}%)"

    return True, "salient"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class FlowCorridor:
    """
    A flow corridor represents a (source, destination) pair that the fleet serves.
    """
    source_peer_id: str
    destination_peer_id: str
    source_alias: Optional[str] = None
    destination_alias: Optional[str] = None

    # Members that can serve this corridor
    capable_members: List[str] = field(default_factory=list)

    # Assigned primary member (gets competitive fee)
    primary_member: Optional[str] = None

    # Metrics for assignment decisions
    total_volume_sats: int = 0
    avg_fee_earned_ppm: int = 0
    competition_level: str = "none"  # "none", "low", "medium", "high"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_peer_id": self.source_peer_id,
            "destination_peer_id": self.destination_peer_id,
            "source_alias": self.source_alias,
            "destination_alias": self.destination_alias,
            "capable_members": self.capable_members,
            "primary_member": self.primary_member,
            "total_volume_sats": self.total_volume_sats,
            "avg_fee_earned_ppm": self.avg_fee_earned_ppm,
            "competition_level": self.competition_level
        }


@dataclass
class CorridorAssignment:
    """
    Assignment of a flow corridor to a primary member.
    """
    corridor: FlowCorridor
    primary_member: str
    secondary_members: List[str]

    # Fee recommendations
    primary_fee_ppm: int
    secondary_fee_ppm: int

    # Assignment reasoning
    assignment_reason: str
    confidence: float  # 0.0 to 1.0

    timestamp: int = 0

    def __post_init__(self):
        self.timestamp = int(time.time())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "corridor": self.corridor.to_dict(),
            "primary_member": self.primary_member,
            "secondary_members": self.secondary_members,
            "primary_fee_ppm": self.primary_fee_ppm,
            "secondary_fee_ppm": self.secondary_fee_ppm,
            "assignment_reason": self.assignment_reason,
            "confidence": round(self.confidence, 2),
            "timestamp": self.timestamp
        }


@dataclass
class RouteMarker:
    """
    Stigmergic marker left after routing attempt.

    Other fleet members read these markers and adjust fees accordingly.
    This enables indirect coordination without explicit messaging.
    """
    depositor: str           # Member who left the marker
    source_peer_id: str
    destination_peer_id: str
    fee_ppm: int
    success: bool
    volume_sats: int
    timestamp: float
    strength: float = 1.0    # Decays over time

    def to_dict(self) -> Dict[str, Any]:
        return {
            "depositor": self.depositor,
            "source_peer_id": self.source_peer_id,
            "destination_peer_id": self.destination_peer_id,
            "fee_ppm": self.fee_ppm,
            "success": self.success,
            "volume_sats": self.volume_sats,
            "timestamp": self.timestamp,
            "strength": round(self.strength, 3)
        }


@dataclass
class PeerWarning:
    """
    Warning about a threatening peer, broadcast to fleet.
    """
    peer_id: str
    threat_type: str         # "drain", "unreliable", "force_close"
    severity: float          # 0.0 to 1.0
    reporter: str            # Who reported the threat
    timestamp: float
    ttl: float               # Time-to-live in seconds
    evidence: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        return time.time() > self.timestamp + self.ttl

    def to_dict(self) -> Dict[str, Any]:
        return {
            "peer_id": self.peer_id,
            "threat_type": self.threat_type,
            "severity": round(self.severity, 2),
            "reporter": self.reporter,
            "timestamp": self.timestamp,
            "ttl": self.ttl,
            "evidence": self.evidence,
            "is_expired": self.is_expired()
        }


@dataclass
class FeeRecommendation:
    """
    Coordinated fee recommendation for a channel.
    """
    channel_id: str
    peer_id: str
    recommended_fee_ppm: int

    # Context
    is_primary: bool
    corridor_source: Optional[str] = None
    corridor_destination: Optional[str] = None

    # Factors that influenced recommendation
    floor_applied: bool = False
    ceiling_applied: bool = False
    stigmergic_influence: float = 0.0
    defensive_multiplier: float = 1.0
    time_adjustment_pct: float = 0.0    # Phase 7.4: Time-based adjustment
    centrality_adjustment_pct: float = 0.0  # Use Case 8: Centrality-based adjustment
    our_hive_centrality: float = 0.0       # Current node's centrality

    # Salience detection (noise filtering)
    current_fee_ppm: int = 0            # Current fee for comparison
    is_salient: bool = True             # Whether change is significant
    salience_reason: str = ""           # Why change is/isn't salient

    # Confidence
    confidence: float = 0.5
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "current_fee_ppm": self.current_fee_ppm,
            "recommended_fee_ppm": self.recommended_fee_ppm,
            "is_primary": self.is_primary,
            "corridor_source": self.corridor_source,
            "corridor_destination": self.corridor_destination,
            "floor_applied": self.floor_applied,
            "ceiling_applied": self.ceiling_applied,
            "stigmergic_influence": round(self.stigmergic_influence, 2),
            "defensive_multiplier": round(self.defensive_multiplier, 2),
            "time_adjustment_pct": round(self.time_adjustment_pct * 100, 1),
            "is_salient": self.is_salient,
            "salience_reason": self.salience_reason,
            "confidence": round(self.confidence, 2),
            "reason": self.reason
        }
        # Include centrality info if adjustment was applied
        if self.centrality_adjustment_pct != 0.0:
            result["centrality_adjustment_pct"] = round(self.centrality_adjustment_pct * 100, 1)
            result["our_hive_centrality"] = round(self.our_hive_centrality, 3)
        return result


# =============================================================================
# FLOW CORRIDOR ASSIGNMENT
# =============================================================================

class FlowCorridorManager:
    """
    Manages flow corridor assignment to eliminate internal competition.

    Assigns a "primary" member for each (source, destination) pair.
    Primary member gets competitive fees, others get premium fees.
    """

    def __init__(
        self,
        database: Any,
        plugin: Any,
        state_manager: Any = None,
        liquidity_coordinator: Any = None
    ):
        self.database = database
        self.plugin = plugin
        self.state_manager = state_manager
        self.liquidity_coordinator = liquidity_coordinator
        self.our_pubkey: Optional[str] = None

        # Cache of assignments
        self._assignments: Dict[Tuple[str, str], CorridorAssignment] = {}
        self._assignments_timestamp: float = 0
        self._assignments_ttl: float = 3600  # 1 hour cache

    def set_our_pubkey(self, pubkey: str) -> None:
        self.our_pubkey = pubkey

    def _log(self, msg: str, level: str = "info") -> None:
        if self.plugin:
            self.plugin.log(f"cl-hive: [FlowCorridor] {msg}", level=level)

    def identify_corridors(self) -> List[FlowCorridor]:
        """
        Identify all flow corridors the fleet can serve.

        A corridor exists when 2+ members can route between same (source, dest).
        """
        if not self.liquidity_coordinator:
            return []

        # Use internal competition detection to find overlapping routes
        competitions = self.liquidity_coordinator.detect_internal_competition()

        corridors = []
        for comp in competitions:
            corridor = FlowCorridor(
                source_peer_id=comp.get("source_peer_id", ""),
                destination_peer_id=comp.get("destination_peer_id", ""),
                source_alias=comp.get("source_alias"),
                destination_alias=comp.get("destination_alias"),
                capable_members=comp.get("competing_members", []),
                total_volume_sats=comp.get("total_fleet_capacity_sats", 0),
                competition_level=self._assess_competition_level(
                    len(comp.get("competing_members", []))
                )
            )
            corridors.append(corridor)

        return corridors

    def _assess_competition_level(self, member_count: int) -> str:
        """Assess competition level based on number of competing members."""
        if member_count <= 1:
            return "none"
        elif member_count == 2:
            return "low"
        elif member_count <= 4:
            return "medium"
        else:
            return "high"

    def assign_corridor(self, corridor: FlowCorridor) -> CorridorAssignment:
        """
        Assign a corridor to a primary member.

        Selection criteria:
        1. Position (shortest path to both source and dest)
        2. Capacity (more liquidity available)
        3. Historical performance (higher success rate)
        """
        if not corridor.capable_members:
            return CorridorAssignment(
                corridor=corridor,
                primary_member="",
                secondary_members=[],
                primary_fee_ppm=DEFAULT_FEE_PPM,
                secondary_fee_ppm=int(DEFAULT_FEE_PPM * SECONDARY_FEE_MULTIPLIER),
                assignment_reason="no_capable_members",
                confidence=0.0
            )

        # Score each member
        member_scores: Dict[str, float] = {}
        for member_id in corridor.capable_members:
            score = self._score_member_for_corridor(
                member_id, corridor.source_peer_id, corridor.destination_peer_id
            )
            member_scores[member_id] = score

        # Select primary (highest score)
        sorted_members = sorted(
            member_scores.items(),
            key=lambda x: x[1],
            reverse=True
        )

        primary_member = sorted_members[0][0]
        primary_score = sorted_members[0][1]
        secondary_members = [m[0] for m in sorted_members[1:]]

        # Calculate fees
        base_fee = self._estimate_corridor_base_fee(corridor)
        primary_fee = max(FLEET_FEE_FLOOR_PPM, int(base_fee * PRIMARY_FEE_MULTIPLIER))
        secondary_fee = max(
            FLEET_FEE_FLOOR_PPM * 2,
            int(base_fee * SECONDARY_FEE_MULTIPLIER)
        )

        # Calculate confidence
        confidence = min(1.0, primary_score / 10.0) if primary_score > 0 else 0.3

        return CorridorAssignment(
            corridor=corridor,
            primary_member=primary_member,
            secondary_members=secondary_members,
            primary_fee_ppm=primary_fee,
            secondary_fee_ppm=secondary_fee,
            assignment_reason=f"highest_score_{primary_score:.2f}",
            confidence=confidence
        )

    def _score_member_for_corridor(
        self,
        member_id: str,
        source: str,
        destination: str
    ) -> float:
        """
        Score a member's suitability for a corridor.
        """
        score = 0.0

        # Get member state
        if self.state_manager:
            state = self.state_manager.get_peer_state(member_id)
            if state:
                # Capacity score (normalized)
                capacity = getattr(state, 'capacity_sats', 0)
                score += capacity / 10_000_000  # 10M sats = 1 point

                # Check if this is us - we can get more detailed info
                if member_id == self.our_pubkey and self.plugin:
                    try:
                        for peer_id in [source, destination]:
                            channels = self.plugin.rpc.listpeerchannels(id=peer_id)
                            for ch in channels.get("channels", []):
                                if ch.get("state") == "CHANNELD_NORMAL":
                                    cap = ch.get("total_msat", 0) // 1000
                                    local = ch.get("to_us_msat", 0) // 1000
                                    # Balanced channels are better
                                    if cap > 0:
                                        balance_pct = local / cap
                                        balance_score = 1.0 - abs(0.5 - balance_pct) * 2
                                        score += balance_score
                    except Exception:
                        pass

        return score

    def _estimate_corridor_base_fee(self, corridor: FlowCorridor) -> int:
        """Estimate base fee for a corridor based on competition and volume."""
        # Higher competition = lower fees needed
        competition_factor = {
            "none": 1.5,
            "low": 1.2,
            "medium": 1.0,
            "high": 0.8
        }.get(corridor.competition_level, 1.0)

        return int(DEFAULT_FEE_PPM * competition_factor)

    def get_assignments(self, force_refresh: bool = False) -> List[CorridorAssignment]:
        """Get all corridor assignments, refreshing if needed."""
        now = time.time()

        if (not force_refresh and
            self._assignments and
            now - self._assignments_timestamp < self._assignments_ttl):
            return list(self._assignments.values())

        # Refresh assignments
        corridors = self.identify_corridors()
        self._assignments = {}

        for corridor in corridors:
            assignment = self.assign_corridor(corridor)
            key = (corridor.source_peer_id, corridor.destination_peer_id)
            self._assignments[key] = assignment

        self._assignments_timestamp = now
        self._log(f"Refreshed {len(self._assignments)} corridor assignments")

        return list(self._assignments.values())

    def is_primary_for_corridor(
        self,
        member_id: str,
        source: str,
        destination: str
    ) -> bool:
        """Check if member is primary for a specific corridor."""
        key = (source, destination)
        assignment = self._assignments.get(key)
        if assignment:
            return assignment.primary_member == member_id
        return False

    def get_fee_for_member(
        self,
        member_id: str,
        source: str,
        destination: str
    ) -> Tuple[int, bool]:
        """
        Get recommended fee for member on a corridor.

        Returns (fee_ppm, is_primary)
        """
        key = (source, destination)
        assignment = self._assignments.get(key)

        if not assignment:
            return DEFAULT_FEE_PPM, False

        if assignment.primary_member == member_id:
            return assignment.primary_fee_ppm, True
        else:
            return assignment.secondary_fee_ppm, False


# =============================================================================
# ADAPTIVE FEE CONTROLLER (PHEROMONE-BASED)
# =============================================================================

class AdaptiveFeeController:
    """
    Fee adjustment inspired by ant colony pheromone dynamics.

    Pheromone = "memory" of what worked
    Evaporation = forgetting rate (adaptive based on environment)
    Deposit = reinforcement from success
    """

    def __init__(self, plugin: Any = None):
        self.plugin = plugin
        self.our_pubkey: Optional[str] = None

        # Pheromone levels per channel (fee memory)
        self._pheromone: Dict[str, float] = defaultdict(float)

        # Track the fee that earned each pheromone level
        self._pheromone_fee: Dict[str, int] = {}

        # Track when each pheromone was last updated (for time-based decay)
        self._pheromone_last_update: Dict[str, float] = {}

        # Map channel_id to peer_id for sharing
        self._channel_peer_map: Dict[str, str] = {}

        # Remote pheromones received from fleet (keyed by peer_id)
        self._remote_pheromones: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

        # Velocity cache for evaporation rate calculation
        self._velocity_cache: Dict[str, float] = {}
        self._velocity_cache_time: Dict[str, float] = {}

        # Network fee volatility tracking
        self._fee_observations: List[Tuple[float, int]] = []  # (timestamp, fee)

    def set_our_pubkey(self, pubkey: str) -> None:
        self.our_pubkey = pubkey

    def _log(self, msg: str, level: str = "info") -> None:
        if self.plugin:
            self.plugin.log(f"cl-hive: [AdaptiveFee] {msg}", level=level)

    def calculate_evaporation_rate(self, channel_id: str) -> float:
        """
        Dynamic evaporation based on environment stability.

        Stable environment: Low evaporation (exploit known good fees)
        Dynamic environment: High evaporation (explore new fee points)
        """
        # Get balance velocity (if available)
        velocity = self._velocity_cache.get(channel_id, 0.0)

        # Get network fee volatility
        fee_volatility = self._calculate_fee_volatility()

        # Base evaporation
        base = BASE_EVAPORATION_RATE

        # Velocity factor: faster drain = faster adaptation
        velocity_factor = min(0.4, abs(velocity) * 4)

        # Volatility factor: market moving = faster adaptation
        volatility_factor = min(0.3, fee_volatility / 200)

        evap_rate = base + velocity_factor + volatility_factor

        return max(MIN_EVAPORATION_RATE, min(MAX_EVAPORATION_RATE, evap_rate))

    def _calculate_fee_volatility(self) -> float:
        """Calculate recent fee volatility in the network."""
        if len(self._fee_observations) < 2:
            return 0.0

        # Filter to recent observations (last hour)
        now = time.time()
        recent = [f for t, f in self._fee_observations if now - t < 3600]

        if len(recent) < 2:
            return 0.0

        mean_fee = sum(recent) / len(recent)
        variance = sum((f - mean_fee) ** 2 for f in recent) / len(recent)

        return math.sqrt(variance)

    def update_velocity(self, channel_id: str, velocity_pct_per_hour: float) -> None:
        """Update cached velocity for a channel."""
        self._velocity_cache[channel_id] = velocity_pct_per_hour
        self._velocity_cache_time[channel_id] = time.time()

    def record_fee_observation(self, fee_ppm: int) -> None:
        """Record a network fee observation for volatility calculation."""
        self._fee_observations.append((time.time(), fee_ppm))

        # Keep only recent observations
        cutoff = time.time() - 3600
        self._fee_observations = [
            (t, f) for t, f in self._fee_observations if t > cutoff
        ]

    def update_pheromone(
        self,
        channel_id: str,
        current_fee: int,
        routing_success: bool,
        revenue_sats: int = 0
    ) -> None:
        """
        Update fee "pheromone" based on routing outcomes.

        Success → deposit pheromone (reinforce this fee)
        Failure → no deposit (let it evaporate)
        High revenue → stronger deposit

        Time-based decay: Pheromone decays exponentially based on time since
        last update, not just per-call. This ensures idle channels properly
        lose their pheromone over time.
        """
        now = time.time()
        evap_rate = self.calculate_evaporation_rate(channel_id)

        # Apply time-based exponential decay (half-life model)
        # If no timestamp exists, apply at least one cycle of decay
        if channel_id in self._pheromone_last_update:
            last_update = self._pheromone_last_update[channel_id]
            hours_elapsed = (now - last_update) / 3600.0
            if hours_elapsed > 0 and self._pheromone[channel_id] > 0:
                # Convert per-cycle evaporation to continuous decay
                # If evap_rate = 0.2 means 20% loss per hour, apply proportionally
                decay_factor = math.pow(1 - evap_rate, hours_elapsed)
                self._pheromone[channel_id] *= decay_factor
        elif self._pheromone[channel_id] > 0:
            # No timestamp but has pheromone - apply one cycle of decay
            # This handles legacy data and ensures evaporation on failure
            self._pheromone[channel_id] *= (1 - evap_rate)

        # Update timestamp
        self._pheromone_last_update[channel_id] = now

        if routing_success:
            # Deposit proportional to revenue
            deposit = revenue_sats * PHEROMONE_DEPOSIT_SCALE
            self._pheromone[channel_id] += deposit

            # Track the fee that earned this pheromone
            self._pheromone_fee[channel_id] = current_fee

            self._log(
                f"Channel {channel_id[:8]}: pheromone deposit {deposit:.2f}, "
                f"total now {self._pheromone[channel_id]:.2f}",
                level="debug"
            )

    def suggest_fee(
        self,
        channel_id: str,
        current_fee: int,
        local_balance_pct: float
    ) -> Tuple[int, str]:
        """
        Suggest fee based on pheromone trails.

        Returns (suggested_fee, reason)
        """
        pheromone = self._pheromone.get(channel_id, 0)

        if pheromone > PHEROMONE_EXPLOIT_THRESHOLD:
            # Strong signal - exploit current fee
            return current_fee, "exploit_strong_pheromone"
        else:
            # Weak signal - explore
            if local_balance_pct < 0.3:
                # Depleting - raise fees to slow outflow
                new_fee = int(current_fee * 1.15)
                return new_fee, "explore_raise_depleting"
            elif local_balance_pct > 0.7:
                # Saturating - lower fees to attract flow
                new_fee = int(current_fee * 0.85)
                return new_fee, "explore_lower_saturating"
            else:
                # Balanced - small exploration
                return current_fee, "exploit_balanced"

    def get_pheromone_level(self, channel_id: str) -> float:
        """Get current pheromone level for a channel."""
        return self._pheromone.get(channel_id, 0.0)

    def get_all_pheromone_levels(self) -> Dict[str, float]:
        """Get all pheromone levels."""
        return dict(self._pheromone)

    def set_channel_peer_mapping(self, channel_id: str, peer_id: str) -> None:
        """
        Set the mapping from channel_id to peer_id.

        This is needed for sharing pheromones - we share by peer_id
        so other members with channels to the same peer can learn.
        """
        self._channel_peer_map[channel_id] = peer_id

    def update_channel_peer_mappings(self, channels: List[Dict[str, Any]]) -> None:
        """
        Update channel-to-peer mappings from a list of channel info.

        Args:
            channels: List of channel dicts with 'short_channel_id' and 'peer_id'
        """
        for ch in channels:
            channel_id = ch.get("short_channel_id")
            peer_id = ch.get("peer_id")
            if channel_id and peer_id:
                self._channel_peer_map[channel_id] = peer_id

    def get_shareable_pheromones(
        self,
        min_level: float = 0.5,
        max_pheromones: int = 100,
        exclude_peer_ids: Optional[set] = None
    ) -> List[Dict[str, Any]]:
        """
        Get pheromones suitable for sharing with fleet.

        Only shares pheromones that:
        - Have sufficient level (meaningful signal)
        - Have a known fee associated
        - Have a known peer mapping
        - Are not for hive members (exclude_peer_ids)

        Args:
            min_level: Minimum pheromone level to share
            max_pheromones: Maximum number of entries to return
            exclude_peer_ids: Set of peer IDs to exclude (e.g., hive members)

        Returns:
            List of pheromone dicts ready for serialization
        """
        exclude_peer_ids = exclude_peer_ids or set()
        shareable = []

        for channel_id, level in self._pheromone.items():
            # Check level threshold
            if level < min_level:
                continue

            # Get the fee that earned this pheromone
            fee_ppm = self._pheromone_fee.get(channel_id)
            if fee_ppm is None:
                continue

            # Get peer_id for this channel
            peer_id = self._channel_peer_map.get(channel_id)
            if not peer_id:
                continue

            # Skip hive members
            if peer_id in exclude_peer_ids:
                continue

            shareable.append({
                "peer_id": peer_id,
                "level": round(level, 3),
                "fee_ppm": fee_ppm,
                "channel_id": channel_id
            })

        # Sort by level descending (strongest signals first)
        shareable.sort(key=lambda x: -x["level"])

        return shareable[:max_pheromones]

    def receive_pheromone_from_gossip(
        self,
        reporter_id: str,
        pheromone_data: Dict[str, Any],
        weighting_factor: float = 0.3
    ) -> bool:
        """
        Receive a pheromone report from another fleet member.

        Remote pheromones are stored separately and can be used to
        influence local fee decisions. We don't directly modify our
        local pheromone levels, but blend them when making decisions.

        Args:
            reporter_id: The fleet member who reported this
            pheromone_data: Dict with peer_id, level, fee_ppm
            weighting_factor: How much weight to give remote data (0-1)

        Returns:
            True if stored successfully
        """
        peer_id = pheromone_data.get("peer_id")
        if not peer_id:
            return False

        level = pheromone_data.get("level", 0)
        fee_ppm = pheromone_data.get("fee_ppm", 0)

        if level <= 0 or fee_ppm <= 0:
            return False

        # Store remote pheromone, keyed by the external peer
        entry = {
            "reporter_id": reporter_id,
            "level": level,
            "fee_ppm": fee_ppm,
            "timestamp": time.time(),
            "weight": weighting_factor
        }

        # Keep only recent reports per peer (last 10)
        self._remote_pheromones[peer_id].append(entry)
        if len(self._remote_pheromones[peer_id]) > 10:
            self._remote_pheromones[peer_id] = self._remote_pheromones[peer_id][-10:]

        return True

    def get_fleet_fee_hint(self, peer_id: str) -> Optional[Tuple[int, float]]:
        """
        Get fee hint from fleet pheromones for a specific peer.

        Aggregates remote pheromone reports to suggest a fee.

        Args:
            peer_id: The external peer to get hints for

        Returns:
            Tuple of (suggested_fee_ppm, confidence) or None if no data
        """
        reports = self._remote_pheromones.get(peer_id, [])
        if not reports:
            return None

        # Filter to recent reports (last 24 hours)
        now = time.time()
        recent = [r for r in reports if now - r.get("timestamp", 0) < 86400]

        if not recent:
            return None

        # Weight by level and recency
        total_weight = 0
        weighted_fee = 0

        for r in recent:
            age_hours = (now - r.get("timestamp", now)) / 3600
            recency_weight = max(0.1, 1.0 - (age_hours / 24))
            level_weight = r.get("level", 0) / 10  # Normalize level
            weight = recency_weight * level_weight * r.get("weight", 0.3)

            weighted_fee += r.get("fee_ppm", 0) * weight
            total_weight += weight

        if total_weight < 0.1:
            return None

        suggested_fee = int(weighted_fee / total_weight)
        confidence = min(1.0, total_weight)

        return (suggested_fee, confidence)

    def get_all_fleet_hints(self) -> Dict[str, Tuple[int, float]]:
        """Get fee hints for all peers with remote pheromone data."""
        hints = {}
        for peer_id in self._remote_pheromones:
            hint = self.get_fleet_fee_hint(peer_id)
            if hint:
                hints[peer_id] = hint
        return hints

    def cleanup_old_remote_pheromones(self, max_age_hours: float = 48) -> int:
        """Remove old remote pheromone data."""
        cutoff = time.time() - (max_age_hours * 3600)
        cleaned = 0

        for peer_id in list(self._remote_pheromones.keys()):
            before = len(self._remote_pheromones[peer_id])
            self._remote_pheromones[peer_id] = [
                r for r in self._remote_pheromones[peer_id]
                if r.get("timestamp", 0) > cutoff
            ]
            cleaned += before - len(self._remote_pheromones[peer_id])

            # Remove empty entries
            if not self._remote_pheromones[peer_id]:
                del self._remote_pheromones[peer_id]

        return cleaned

    def evaporate_all_pheromones(self) -> int:
        """
        Apply time-based decay to ALL local pheromones.

        Called periodically from maintenance loop to ensure idle channels
        properly lose their pheromone over time, even if they haven't routed.

        Returns:
            Number of channels that had pheromone evaporated
        """
        now = time.time()
        evaporated = 0
        min_pheromone = 0.01  # Below this, remove entirely

        for channel_id in list(self._pheromone.keys()):
            if self._pheromone[channel_id] <= 0:
                continue

            last_update = self._pheromone_last_update.get(channel_id, now)
            hours_elapsed = (now - last_update) / 3600.0

            if hours_elapsed > 0:
                evap_rate = self.calculate_evaporation_rate(channel_id)
                decay_factor = math.pow(1 - evap_rate, hours_elapsed)
                old_level = self._pheromone[channel_id]
                self._pheromone[channel_id] *= decay_factor
                self._pheromone_last_update[channel_id] = now

                if old_level > min_pheromone and self._pheromone[channel_id] <= min_pheromone:
                    # Pheromone dropped below threshold, clean up
                    del self._pheromone[channel_id]
                    self._pheromone_fee.pop(channel_id, None)
                    self._pheromone_last_update.pop(channel_id, None)

                evaporated += 1

        return evaporated


# =============================================================================
# STIGMERGIC FEE COORDINATION
# =============================================================================

class StigmergicCoordinator:
    """
    Fleet members coordinate fees by observing each other's
    routing outcomes, not through direct messaging.

    The "environment" is the shared routing intelligence map.
    """

    def __init__(self, database: Any, plugin: Any, state_manager: Any = None):
        self.database = database
        self.plugin = plugin
        self.state_manager = state_manager
        self.our_pubkey: Optional[str] = None

        # Route markers (in-memory, also persisted via gossip)
        self._markers: Dict[Tuple[str, str], List[RouteMarker]] = defaultdict(list)

    def set_our_pubkey(self, pubkey: str) -> None:
        self.our_pubkey = pubkey

    def _log(self, msg: str, level: str = "info") -> None:
        if self.plugin:
            self.plugin.log(f"cl-hive: [Stigmergy] {msg}", level=level)

    def deposit_marker(
        self,
        source: str,
        destination: str,
        fee_charged: int,
        success: bool,
        volume_sats: int
    ) -> RouteMarker:
        """
        Leave a marker in shared routing map after routing attempt.

        Other fleet members will see this and adjust their fees
        for the same route accordingly.
        """
        marker = RouteMarker(
            depositor=self.our_pubkey or "",
            source_peer_id=source,
            destination_peer_id=destination,
            fee_ppm=fee_charged,
            success=success,
            volume_sats=volume_sats,
            timestamp=time.time(),
            strength=max(0.1, volume_sats / 100_000)  # Larger payments = stronger signal, min floor preserves signal
        )

        key = (source, destination)
        self._markers[key].append(marker)

        # Prune old markers
        self._prune_markers(key)

        self._log(
            f"Deposited marker: {source[:8]}->{destination[:8]} "
            f"fee={fee_charged} success={success} strength={marker.strength:.2f}",
            level="debug"
        )

        return marker

    def _prune_markers(self, key: Tuple[str, str]) -> None:
        """Remove expired markers."""
        now = time.time()
        self._markers[key] = [
            m for m in self._markers[key]
            if self._calculate_marker_strength(m, now) > MARKER_MIN_STRENGTH
        ]

    def _calculate_marker_strength(self, marker: RouteMarker, now: float) -> float:
        """Calculate current strength of a marker (decays over time)."""
        age_hours = (now - marker.timestamp) / 3600
        decay = math.exp(-age_hours * math.log(2) / MARKER_HALF_LIFE_HOURS)
        return marker.strength * decay

    def read_markers(self, source: str, destination: str) -> List[RouteMarker]:
        """
        Read markers left by other fleet members for this route.
        """
        key = (source, destination)
        markers = self._markers.get(key, [])

        now = time.time()
        result = []

        for m in markers:
            # Update strength based on decay
            current_strength = self._calculate_marker_strength(m, now)
            if current_strength > MARKER_MIN_STRENGTH:
                m.strength = current_strength
                result.append(m)

        return result

    def calculate_coordinated_fee(
        self,
        source: str,
        destination: str,
        default_fee: int
    ) -> Tuple[int, float]:
        """
        Set fee based on stigmergic signals from fleet.

        Returns (recommended_fee, confidence)
        """
        markers = self.read_markers(source, destination)

        if not markers:
            return default_fee, 0.3  # No signals, low confidence

        # Separate successful and failed markers
        successful = [m for m in markers if m.success]
        failed = [m for m in markers if not m.success]

        if successful:
            # Find strongest successful marker
            best = max(successful, key=lambda m: m.strength)

            # Don't undercut successful fleet member
            recommended = max(FLEET_FEE_FLOOR_PPM, best.fee_ppm)
            confidence = min(0.9, 0.5 + best.strength * 0.1)

            return recommended, confidence

        if failed:
            # All failures - try lower or avoid
            avg_failed_fee = sum(m.fee_ppm for m in failed) / len(failed)
            recommended = max(FLEET_FEE_FLOOR_PPM, int(avg_failed_fee * 0.8))
            confidence = 0.4

            return recommended, confidence

        return default_fee, 0.3

    def receive_marker_from_gossip(self, marker_data: Dict) -> Optional[RouteMarker]:
        """Process a marker received from fleet gossip."""
        try:
            marker = RouteMarker(
                depositor=marker_data["depositor"],
                source_peer_id=marker_data["source_peer_id"],
                destination_peer_id=marker_data["destination_peer_id"],
                fee_ppm=marker_data["fee_ppm"],
                success=marker_data["success"],
                volume_sats=marker_data["volume_sats"],
                timestamp=marker_data["timestamp"],
                strength=marker_data.get("strength", 1.0)
            )

            key = (marker.source_peer_id, marker.destination_peer_id)
            self._markers[key].append(marker)
            self._prune_markers(key)

            return marker
        except (KeyError, TypeError) as e:
            self._log(f"Invalid marker data: {e}", level="debug")
            return None

    def get_all_markers(self) -> List[RouteMarker]:
        """Get all active markers."""
        result = []
        now = time.time()

        for markers in self._markers.values():
            for m in markers:
                current_strength = self._calculate_marker_strength(m, now)
                if current_strength > MARKER_MIN_STRENGTH:
                    m.strength = current_strength
                    result.append(m)

        return result

    def get_shareable_markers(
        self,
        our_pubkey: str,
        min_strength: float = 0.1,
        max_age_hours: float = 24.0,
        max_markers: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get markers suitable for sharing with fleet.

        Only shares markers that:
        - Were deposited by us (our_pubkey)
        - Have sufficient strength after decay
        - Are not too old
        - Are successful routes (prioritized) or recent failures

        Args:
            our_pubkey: Our node's public key
            min_strength: Minimum marker strength to share
            max_age_hours: Maximum age in hours
            max_markers: Maximum number of markers to return

        Returns:
            List of marker dicts ready for serialization
        """
        now = time.time()
        max_age_secs = max_age_hours * 3600
        shareable = []

        for markers in self._markers.values():
            for m in markers:
                # Only share our own markers
                if m.depositor != our_pubkey:
                    continue

                # Check age
                age = now - m.timestamp
                if age > max_age_secs:
                    continue

                # Check strength
                current_strength = self._calculate_marker_strength(m, now)
                if current_strength < min_strength:
                    continue

                shareable.append({
                    "source_peer_id": m.source_peer_id,
                    "destination_peer_id": m.destination_peer_id,
                    "fee_ppm": m.fee_ppm,
                    "success": m.success,
                    "volume_sats": m.volume_sats,
                    "timestamp": m.timestamp,
                    "strength": round(current_strength, 3)
                })

        # Sort by: success first, then by strength (descending), then by timestamp (newest)
        shareable.sort(
            key=lambda x: (not x["success"], -x["strength"], -x["timestamp"])
        )

        # Limit to max_markers
        return shareable[:max_markers]


# =============================================================================
# MYCELIUM DEFENSE SYSTEM
# =============================================================================

class MyceliumDefenseSystem:
    """
    Fleet-wide defense against draining/malicious peers.

    When one member detects a threat, all members respond.
    Like chemical signals through mycelium network.

    Quorum Requirement (anti-false-positive):
    - Local self-detected threats: immediate defense (self-preservation)
    - Remote warnings: require DEFENSE_QUORUM_THRESHOLD independent reports
      before activating fleet-wide defense (prevents manipulation)
    """

    def __init__(self, database: Any, plugin: Any, gossip_mgr: Any = None):
        self.database = database
        self.plugin = plugin
        self.gossip_mgr = gossip_mgr
        self.our_pubkey: Optional[str] = None

        # Active warnings (most recent per peer)
        self._warnings: Dict[str, PeerWarning] = {}

        # Track all reports per peer for quorum (peer_id -> {reporter_id: warning})
        self._warning_reports: Dict[str, Dict[str, PeerWarning]] = defaultdict(dict)

        # Temporary defensive fees
        self._defensive_fees: Dict[str, Dict] = {}

        # Peer statistics cache
        self._peer_stats: Dict[str, Dict] = {}

    def set_our_pubkey(self, pubkey: str) -> None:
        self.our_pubkey = pubkey

    def _log(self, msg: str, level: str = "info") -> None:
        if self.plugin:
            self.plugin.log(f"cl-hive: [MyceliumDefense] {msg}", level=level)

    def update_peer_stats(
        self,
        peer_id: str,
        inflow_sats: int,
        outflow_sats: int,
        successful_forwards: int,
        failed_forwards: int
    ) -> None:
        """Update statistics for a peer."""
        self._peer_stats[peer_id] = {
            "inflow": inflow_sats,
            "outflow": outflow_sats,
            "successful": successful_forwards,
            "failed": failed_forwards,
            "updated_at": time.time()
        }

    def detect_threat(self, peer_id: str) -> Optional[PeerWarning]:
        """
        Detect peers that are draining us or behaving badly.
        """
        stats = self._peer_stats.get(peer_id)
        if not stats:
            return None

        # Calculate threat indicators
        inflow = max(stats.get("inflow", 0), 1)
        outflow = stats.get("outflow", 0)
        drain_rate = outflow / inflow

        successful = stats.get("successful", 0)
        failed = stats.get("failed", 0)
        total = successful + failed
        failure_rate = failed / total if total > 0 else 0

        # Check for drain attack
        if drain_rate > DRAIN_RATIO_THRESHOLD:
            return PeerWarning(
                peer_id=peer_id,
                threat_type="drain",
                severity=min(1.0, drain_rate / 10),
                reporter=self.our_pubkey or "",
                timestamp=time.time(),
                ttl=WARNING_TTL_HOURS * 3600,
                evidence={"drain_rate": round(drain_rate, 2)}
            )

        # Check for unreliable peer
        if failure_rate > FAILURE_RATE_THRESHOLD:
            return PeerWarning(
                peer_id=peer_id,
                threat_type="unreliable",
                severity=failure_rate,
                reporter=self.our_pubkey or "",
                timestamp=time.time(),
                ttl=WARNING_TTL_HOURS * 3600,
                evidence={"failure_rate": round(failure_rate, 2)}
            )

        return None

    def broadcast_warning(self, warning: PeerWarning) -> bool:
        """
        Send warning to fleet (like chemical signal through mycelium).
        """
        # Store locally
        self._warnings[warning.peer_id] = warning

        # Broadcast via gossip if available
        if self.gossip_mgr:
            try:
                # This would integrate with existing gossip infrastructure
                self._log(
                    f"Broadcasting warning for {warning.peer_id[:12]}: "
                    f"{warning.threat_type} (severity={warning.severity:.2f})"
                )
                return True
            except Exception as e:
                self._log(f"Failed to broadcast warning: {e}", level="error")
                return False

        return True

    def handle_warning(self, warning: PeerWarning) -> Optional[Dict]:
        """
        Respond to warning from another fleet member.

        Implements quorum requirement for remote warnings:
        - Self-detected threats (reporter == our_pubkey): immediate defense
        - Remote warnings: require DEFENSE_QUORUM_THRESHOLD independent reports

        Returns defensive fee adjustment if applicable, None if quorum not met.
        """
        peer_id = warning.peer_id
        reporter = warning.reporter

        # Store warning in reports tracker
        self._warning_reports[peer_id][reporter] = warning

        # Clean expired reports for this peer
        now = time.time()
        self._warning_reports[peer_id] = {
            r: w for r, w in self._warning_reports[peer_id].items()
            if now < (w.timestamp + w.ttl)
        }

        # Store most recent warning
        self._warnings[peer_id] = warning

        # Check if this is a self-detected threat (immediate defense)
        is_self_detected = (reporter == self.our_pubkey)

        # Count independent reports (excluding self if also reported by others)
        report_count = len(self._warning_reports[peer_id])

        # Quorum check: self-detected OR enough independent reports
        quorum_met = is_self_detected or (report_count >= DEFENSE_QUORUM_THRESHOLD)

        if not quorum_met:
            self._log(
                f"Warning for {peer_id[:12]} from {reporter[:12]} "
                f"(reports: {report_count}/{DEFENSE_QUORUM_THRESHOLD}, awaiting quorum)",
                level="debug"
            )
            return None

        # Calculate defensive fee increase (average severity from all reporters)
        total_severity = sum(w.severity for w in self._warning_reports[peer_id].values())
        avg_severity = total_severity / report_count
        multiplier = 1 + (avg_severity * (DEFENSIVE_FEE_MAX_MULTIPLIER - 1))

        self._defensive_fees[peer_id] = {
            "multiplier": multiplier,
            "expires_at": warning.timestamp + warning.ttl,
            "threat_type": warning.threat_type,
            "reporter": reporter,
            "report_count": report_count
        }

        self._log(
            f"Defensive fee multiplier {multiplier:.2f}x applied to "
            f"{peer_id[:12]} (quorum: {report_count} reports, "
            f"{'self-detected' if is_self_detected else 'fleet consensus'})"
        )

        return {
            "peer_id": peer_id,
            "multiplier": multiplier,
            "expires_at": warning.timestamp + warning.ttl,
            "report_count": report_count
        }

    def get_defensive_multiplier(self, peer_id: str) -> float:
        """Get current defensive fee multiplier for a peer."""
        defense = self._defensive_fees.get(peer_id)
        if not defense:
            return 1.0

        # Check if expired
        if time.time() > defense["expires_at"]:
            del self._defensive_fees[peer_id]
            return 1.0

        return defense["multiplier"]

    def check_warning_expiration(self) -> List[str]:
        """
        Check and clean up expired warnings.

        Returns list of peer_ids whose warnings expired.
        """
        now = time.time()
        expired = []

        for peer_id, warning in list(self._warnings.items()):
            if warning.is_expired():
                del self._warnings[peer_id]
                expired.append(peer_id)

        for peer_id in list(self._defensive_fees.keys()):
            if now > self._defensive_fees[peer_id]["expires_at"]:
                del self._defensive_fees[peer_id]
                if peer_id not in expired:
                    expired.append(peer_id)

        # Clean up expired reports from quorum tracking
        for peer_id in list(self._warning_reports.keys()):
            self._warning_reports[peer_id] = {
                r: w for r, w in self._warning_reports[peer_id].items()
                if now < (w.timestamp + w.ttl)
            }
            # Remove peer entry if no reports left
            if not self._warning_reports[peer_id]:
                del self._warning_reports[peer_id]

        if expired:
            self._log(f"Expired warnings for {len(expired)} peers")

        return expired

    def get_active_warnings(self) -> List[PeerWarning]:
        """Get all active (non-expired) warnings."""
        return [w for w in self._warnings.values() if not w.is_expired()]

    def get_defense_status(self) -> Dict:
        """Get current defense system status."""
        self.check_warning_expiration()

        return {
            "active_warnings": len(self._warnings),
            "defensive_fees_active": len(self._defensive_fees),
            "warnings": [w.to_dict() for w in self._warnings.values()],
            "defensive_peers": list(self._defensive_fees.keys()),
            "ban_candidates": self.get_ban_candidates()
        }

    def set_peer_reputation_manager(self, peer_rep_mgr: Any) -> None:
        """Set reference to peer reputation manager for warning broadcast."""
        self._peer_rep_mgr = peer_rep_mgr

    def broadcast_warning_via_reputation(
        self,
        warning: PeerWarning,
        rpc: Any
    ) -> bool:
        """
        Broadcast a warning through the PEER_REPUTATION protocol.

        Uses the peer reputation system to propagate threat information,
        encoding the threat as a warning in the reputation report.

        Args:
            warning: The PeerWarning to broadcast
            rpc: RPC interface for signing

        Returns:
            True if broadcast succeeded
        """
        if not hasattr(self, '_peer_rep_mgr') or not self._peer_rep_mgr:
            self._log("No peer reputation manager - cannot broadcast warning", level='warn')
            return False

        # Map threat types to warning codes
        warning_code_map = {
            "drain": "drain_attack",
            "unreliable": "unreliable",
            "force_close": "force_close_risk"
        }
        warning_code = warning_code_map.get(warning.threat_type, warning.threat_type)

        try:
            # Create a peer reputation message with the warning
            msg = self._peer_rep_mgr.create_reputation_message(
                peer_id=warning.peer_id,
                rpc=rpc,
                uptime_pct=1.0 - warning.severity,  # Lower uptime = worse reputation
                htlc_success_rate=1.0 - warning.severity if warning.threat_type == "unreliable" else 1.0,
                warnings=[warning_code],
                observation_days=7
            )

            if msg:
                self._log(
                    f"Warning broadcast prepared for {warning.peer_id[:12]}: "
                    f"{warning.threat_type} (severity={warning.severity:.2f})"
                )
                return True

        except Exception as e:
            self._log(f"Failed to broadcast warning: {e}", level='error')

        return False

    def get_accumulated_warnings(self, peer_id: str) -> Dict[str, Any]:
        """
        Get accumulated warning information for a peer.

        Combines local warnings with aggregated peer reputation data.

        Args:
            peer_id: Peer to check

        Returns:
            Dict with warning summary including count from all reporters
        """
        result = {
            "peer_id": peer_id,
            "local_warning": None,
            "reputation_warnings": {},
            "total_reporters": 0,
            "severity_weighted": 0.0,
            "recommend_ban": False
        }

        # Local warning
        local = self._warnings.get(peer_id)
        if local and not local.is_expired():
            result["local_warning"] = local.to_dict()

        # Aggregated reputation warnings
        if hasattr(self, '_peer_rep_mgr') and self._peer_rep_mgr:
            rep = self._peer_rep_mgr.get_reputation(peer_id)
            if rep:
                result["reputation_warnings"] = rep.warnings
                result["total_reporters"] = len(rep.reporters)
                result["reputation_score"] = rep.reputation_score

                # Calculate severity-weighted score
                # Multiple reporters with same warning = more severe
                for warning_code, count in rep.warnings.items():
                    result["severity_weighted"] += count * 0.2  # Each reporter adds 0.2

        # Add local warning severity
        if local:
            result["severity_weighted"] += local.severity

        # Recommend ban if severity is high or multiple reporters
        # Threshold: severity >= 2.0 (e.g., 2+ reporters or very severe local detection)
        result["recommend_ban"] = result["severity_weighted"] >= 2.0

        return result

    def get_ban_candidates(self) -> List[Dict[str, Any]]:
        """
        Get peers that should be considered for ban proposals.

        Combines local threat detection with aggregated reputation data
        to identify peers that warrant community action.

        Returns:
            List of peers with recommendation to ban
        """
        candidates = []

        # Check all peers with active warnings
        checked_peers = set(self._warnings.keys())

        # Also check peers in reputation system with warnings
        if hasattr(self, '_peer_rep_mgr') and self._peer_rep_mgr:
            for peer_id, rep in self._peer_rep_mgr.get_all_reputations().items():
                if rep.warnings or rep.reputation_score < 30:
                    checked_peers.add(peer_id)

        for peer_id in checked_peers:
            accumulated = self.get_accumulated_warnings(peer_id)
            if accumulated["recommend_ban"]:
                candidates.append({
                    "peer_id": peer_id,
                    "severity_weighted": accumulated["severity_weighted"],
                    "total_reporters": accumulated["total_reporters"],
                    "warnings": accumulated.get("reputation_warnings", {}),
                    "local_threat": accumulated.get("local_warning", {}).get("threat_type")
                })

        # Sort by severity (most severe first)
        candidates.sort(key=lambda x: x["severity_weighted"], reverse=True)

        return candidates

    def should_auto_propose_ban(self, peer_id: str) -> Optional[str]:
        """
        Check if a peer should have an automatic ban proposal created.

        Returns the reason for ban if yes, None otherwise.

        Criteria:
        - Severity weighted score >= 3.0 (very severe)
        - OR 3+ unique reporters with same warning type
        - OR local force_close threat with severity > 0.8

        Args:
            peer_id: Peer to check

        Returns:
            Ban reason string if should propose, None otherwise
        """
        accumulated = self.get_accumulated_warnings(peer_id)

        # Very high severity from multiple sources
        if accumulated["severity_weighted"] >= 3.0:
            return f"Multiple reports of malicious behavior (severity={accumulated['severity_weighted']:.1f})"

        # Check for consensus among reporters
        if accumulated["total_reporters"] >= 3:
            for warning_code, count in accumulated.get("reputation_warnings", {}).items():
                if count >= 3:
                    return f"Consensus warning: {warning_code} reported by {count} members"

        # Severe local detection
        local = accumulated.get("local_warning")
        if local:
            if local.get("threat_type") == "force_close" and local.get("severity", 0) > 0.8:
                return "Force close threat detected with high severity"
            if local.get("threat_type") == "drain" and local.get("severity", 0) > 0.9:
                return "Severe drain attack detected"

        return None


# =============================================================================
# TIME-BASED FEE ADJUSTER (Phase 7.4)
# =============================================================================

@dataclass
class TimeFeeAdjustment:
    """Result of time-based fee calculation."""
    channel_id: str
    base_fee_ppm: int
    adjusted_fee_ppm: int
    adjustment_pct: float
    adjustment_type: str        # "peak_increase" | "low_decrease" | "none"
    current_hour: int           # 0-23
    current_day: int            # 0-6 (Mon-Sun)
    pattern_intensity: float    # Detected flow intensity 0.0-1.0
    confidence: float           # Pattern confidence
    reason: str                 # Human-readable explanation

    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "base_fee_ppm": self.base_fee_ppm,
            "adjusted_fee_ppm": self.adjusted_fee_ppm,
            "adjustment_pct": round(self.adjustment_pct * 100, 1),
            "adjustment_type": self.adjustment_type,
            "current_hour": self.current_hour,
            "current_day": self.current_day,
            "pattern_intensity": round(self.pattern_intensity, 2),
            "confidence": round(self.confidence, 2),
            "reason": self.reason
        }


class TimeBasedFeeAdjuster:
    """
    Adjusts fees based on detected temporal patterns.

    Like circadian rhythms in nature - different behavior at different times.
    Uses anticipatory liquidity patterns to:
    - Increase fees during detected peak hours (capture premium)
    - Decrease fees during low-activity periods (attract flow)

    Integrates with AnticipatoryLiquidityManager for pattern data.
    """

    # Day name mapping for logging
    DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    def __init__(self, plugin: Any, anticipatory_mgr: Any = None):
        """
        Initialize the time-based fee adjuster.

        Args:
            plugin: CLN plugin for logging
            anticipatory_mgr: AnticipatoryLiquidityManager for pattern data
        """
        self.plugin = plugin
        self.anticipatory_mgr = anticipatory_mgr
        self.our_pubkey: Optional[str] = None

        # Cache: channel_id -> (adjustment, timestamp)
        self._adjustment_cache: Dict[str, Tuple[TimeFeeAdjustment, float]] = {}

        # Enabled flag (can be toggled via config)
        self.enabled = TIME_FEE_ADJUSTMENT_ENABLED

    def set_our_pubkey(self, pubkey: str) -> None:
        self.our_pubkey = pubkey

    def set_anticipatory_manager(self, mgr: Any) -> None:
        """Set or update the anticipatory liquidity manager."""
        self.anticipatory_mgr = mgr

    def _log(self, msg: str, level: str = "info") -> None:
        if self.plugin:
            self.plugin.log(f"cl-hive: [TimeFee] {msg}", level=level)

    def _get_current_time_context(self) -> Tuple[int, int]:
        """Get current hour (0-23) and day of week (0-6, Mon=0)."""
        import datetime
        try:
            # Python 3.11+
            now = datetime.datetime.now(datetime.UTC)
        except AttributeError:
            # Python 3.9-3.10
            now = datetime.datetime.now(datetime.timezone.utc)
        return now.hour, now.weekday()

    def _get_cached_adjustment(self, channel_id: str) -> Optional[TimeFeeAdjustment]:
        """Get cached adjustment if still valid."""
        if channel_id not in self._adjustment_cache:
            return None

        adjustment, cached_at = self._adjustment_cache[channel_id]
        ttl_seconds = TIME_FEE_CACHE_TTL_HOURS * 3600

        if time.time() - cached_at > ttl_seconds:
            del self._adjustment_cache[channel_id]
            return None

        # Also check if hour changed (invalidate on hour boundary)
        current_hour, _ = self._get_current_time_context()
        if adjustment.current_hour != current_hour:
            del self._adjustment_cache[channel_id]
            return None

        return adjustment

    def get_time_adjustment(
        self,
        channel_id: str,
        base_fee: int,
        use_cache: bool = True
    ) -> TimeFeeAdjustment:
        """
        Get time-adjusted fee for a channel.

        Analyzes temporal patterns to determine if current time is:
        - Peak hours: Increase fee to capture premium
        - Low-activity: Decrease fee to attract flow
        - Normal: No adjustment

        Args:
            channel_id: Channel short ID
            base_fee: Current/base fee in ppm
            use_cache: Whether to use cached adjustments

        Returns:
            TimeFeeAdjustment with recommended fee and reasoning
        """
        # Check cache
        if use_cache:
            cached = self._get_cached_adjustment(channel_id)
            if cached and cached.base_fee_ppm == base_fee:
                return cached

        current_hour, current_day = self._get_current_time_context()

        # Default no-adjustment result
        no_adjustment = TimeFeeAdjustment(
            channel_id=channel_id,
            base_fee_ppm=base_fee,
            adjusted_fee_ppm=base_fee,
            adjustment_pct=0.0,
            adjustment_type="none",
            current_hour=current_hour,
            current_day=current_day,
            pattern_intensity=0.5,
            confidence=0.0,
            reason="No time adjustment"
        )

        # Check if enabled
        if not self.enabled:
            return no_adjustment

        # Check if anticipatory manager is available
        if not self.anticipatory_mgr:
            return no_adjustment

        # Get patterns for this channel
        try:
            patterns = self.anticipatory_mgr.detect_patterns(channel_id)
        except Exception as e:
            self._log(f"Error detecting patterns for {channel_id}: {e}", level="debug")
            return no_adjustment

        if not patterns:
            return no_adjustment

        # Find pattern matching current time
        matching_pattern = None
        best_confidence = 0.0

        for pattern in patterns:
            # Check hour match (allow ±1 hour tolerance)
            if pattern.hour_of_day is None:
                hour_match = True  # None means any hour
            else:
                hour_match = abs(pattern.hour_of_day - current_hour) <= 1
                if pattern.hour_of_day == 23 and current_hour == 0:
                    hour_match = True
                if pattern.hour_of_day == 0 and current_hour == 23:
                    hour_match = True

            # Check day match (if pattern is day-specific)
            day_match = pattern.day_of_week is None or pattern.day_of_week == current_day

            if hour_match and day_match and pattern.confidence > best_confidence:
                matching_pattern = pattern
                best_confidence = pattern.confidence

        if not matching_pattern or best_confidence < TIME_FEE_MIN_CONFIDENCE:
            return no_adjustment

        # Determine adjustment based on pattern intensity
        intensity = matching_pattern.intensity
        adjustment_pct = 0.0
        adjustment_type = "none"
        reason_parts = []

        if intensity >= TIME_FEE_PEAK_INTENSITY:
            # Peak hours - increase fee to capture premium
            # Scale adjustment: 70% intensity = 0%, 100% intensity = max increase
            scale = (intensity - TIME_FEE_PEAK_INTENSITY) / (1.0 - TIME_FEE_PEAK_INTENSITY)
            adjustment_pct = scale * TIME_FEE_MAX_INCREASE_PCT
            adjustment_type = "peak_increase"
            reason_parts.append(
                f"Peak {matching_pattern.direction} hour "
                f"({intensity:.0%} intensity, +{adjustment_pct:.1%})"
            )
        elif intensity <= TIME_FEE_LOW_INTENSITY:
            # Low activity - decrease fee to attract flow
            # Scale adjustment: 30% intensity = 0%, 0% intensity = max decrease
            scale = (TIME_FEE_LOW_INTENSITY - intensity) / TIME_FEE_LOW_INTENSITY
            adjustment_pct = -scale * TIME_FEE_MAX_DECREASE_PCT
            adjustment_type = "low_decrease"
            reason_parts.append(
                f"Low-activity period "
                f"({intensity:.0%} intensity, {adjustment_pct:.1%})"
            )

        # Calculate adjusted fee
        adjusted_fee = int(base_fee * (1 + adjustment_pct))

        # Enforce bounds
        adjusted_fee = max(adjusted_fee, FLEET_FEE_FLOOR_PPM)
        adjusted_fee = min(adjusted_fee, FLEET_FEE_CEILING_PPM)

        # Add time context to reason
        day_name = self.DAY_NAMES[current_day]
        time_str = f"{current_hour:02d}:00 UTC {day_name}"
        reason_parts.append(f"at {time_str}")

        result = TimeFeeAdjustment(
            channel_id=channel_id,
            base_fee_ppm=base_fee,
            adjusted_fee_ppm=adjusted_fee,
            adjustment_pct=adjustment_pct,
            adjustment_type=adjustment_type,
            current_hour=current_hour,
            current_day=current_day,
            pattern_intensity=intensity,
            confidence=best_confidence,
            reason="; ".join(reason_parts) if reason_parts else "No time adjustment"
        )

        # Cache the result
        self._adjustment_cache[channel_id] = (result, time.time())

        if adjustment_type != "none":
            self._log(
                f"Time adjustment for {channel_id}: "
                f"{base_fee} → {adjusted_fee} ppm ({result.reason})",
                level="debug"
            )

        return result

    def detect_peak_hours(self, channel_id: str) -> List[Dict[str, Any]]:
        """
        Detect peak routing hours for a channel.

        Returns list of peak hours with their characteristics.

        Args:
            channel_id: Channel short ID

        Returns:
            List of dicts with hour info:
            [
                {"hour": 14, "day": -1, "intensity": 0.85, "direction": "outbound"},
                {"hour": 15, "day": 0, "intensity": 0.78, "direction": "inbound"},
                ...
            ]
        """
        if not self.anticipatory_mgr:
            return []

        try:
            patterns = self.anticipatory_mgr.detect_patterns(channel_id)
        except Exception:
            return []

        peak_hours = []
        for pattern in patterns:
            if pattern.intensity >= TIME_FEE_PEAK_INTENSITY and \
               pattern.confidence >= TIME_FEE_MIN_CONFIDENCE:
                peak_hours.append({
                    "hour": pattern.hour_of_day,
                    "day": pattern.day_of_week,
                    "day_name": self.DAY_NAMES[pattern.day_of_week]
                        if pattern.day_of_week is not None and pattern.day_of_week >= 0 else "Any",
                    "intensity": round(pattern.intensity, 2),
                    "direction": pattern.direction,
                    "confidence": round(pattern.confidence, 2),
                    "samples": pattern.samples
                })

        # Sort by intensity descending
        peak_hours.sort(key=lambda x: x["intensity"], reverse=True)
        return peak_hours

    def detect_low_hours(self, channel_id: str) -> List[Dict[str, Any]]:
        """
        Detect low-activity hours for a channel.

        Returns list of low-activity hours where fee reduction may help.
        """
        if not self.anticipatory_mgr:
            return []

        try:
            patterns = self.anticipatory_mgr.detect_patterns(channel_id)
        except Exception:
            return []

        low_hours = []
        for pattern in patterns:
            if pattern.intensity <= TIME_FEE_LOW_INTENSITY and \
               pattern.confidence >= TIME_FEE_MIN_CONFIDENCE:
                low_hours.append({
                    "hour": pattern.hour_of_day,
                    "day": pattern.day_of_week,
                    "day_name": self.DAY_NAMES[pattern.day_of_week]
                        if pattern.day_of_week is not None and pattern.day_of_week >= 0 else "Any",
                    "intensity": round(pattern.intensity, 2),
                    "direction": pattern.direction,
                    "confidence": round(pattern.confidence, 2),
                    "samples": pattern.samples
                })

        # Sort by intensity ascending (lowest first)
        low_hours.sort(key=lambda x: x["intensity"])
        return low_hours

    def get_all_adjustments(self) -> Dict[str, Any]:
        """
        Get current time-based adjustments for all cached channels.

        Returns summary of active time-based fee adjustments.
        """
        current_hour, current_day = self._get_current_time_context()

        active = []
        for channel_id, (adjustment, _) in self._adjustment_cache.items():
            if adjustment.adjustment_type != "none":
                active.append(adjustment.to_dict())

        return {
            "enabled": self.enabled,
            "current_hour": current_hour,
            "current_day": current_day,
            "current_day_name": self.DAY_NAMES[current_day],
            "active_adjustments": len(active),
            "adjustments": active,
            "config": {
                "max_increase_pct": TIME_FEE_MAX_INCREASE_PCT * 100,
                "max_decrease_pct": TIME_FEE_MAX_DECREASE_PCT * 100,
                "peak_threshold": TIME_FEE_PEAK_INTENSITY,
                "low_threshold": TIME_FEE_LOW_INTENSITY,
                "min_confidence": TIME_FEE_MIN_CONFIDENCE
            }
        }

    def clear_cache(self) -> int:
        """Clear adjustment cache. Returns number of entries cleared."""
        count = len(self._adjustment_cache)
        self._adjustment_cache.clear()
        return count


# =============================================================================
# FEE COORDINATION MANAGER (Main Interface)
# =============================================================================

class FeeCoordinationManager:
    """
    Main interface for Phase 2 fee coordination.

    Integrates:
    - Flow corridor assignment
    - Adaptive fee controller
    - Stigmergic coordination
    - Mycelium defense
    - Time-based fee adjustments (Phase 7.4)
    """

    def __init__(
        self,
        database: Any,
        plugin: Any,
        state_manager: Any = None,
        liquidity_coordinator: Any = None,
        gossip_mgr: Any = None,
        anticipatory_mgr: Any = None
    ):
        self.database = database
        self.plugin = plugin
        self.our_pubkey: Optional[str] = None

        # Initialize components
        self.corridor_mgr = FlowCorridorManager(
            database, plugin, state_manager, liquidity_coordinator
        )
        self.adaptive_controller = AdaptiveFeeController(plugin)
        self.stigmergic_coord = StigmergicCoordinator(
            database, plugin, state_manager
        )
        self.defense_system = MyceliumDefenseSystem(
            database, plugin, gossip_mgr
        )
        # Phase 7.4: Time-based fee adjuster
        self.time_adjuster = TimeBasedFeeAdjuster(plugin, anticipatory_mgr)

        # Salience detection: Track last fee change times per channel
        self._fee_change_times: Dict[str, float] = {}

    def set_our_pubkey(self, pubkey: str) -> None:
        self.our_pubkey = pubkey
        self.corridor_mgr.set_our_pubkey(pubkey)
        self.adaptive_controller.set_our_pubkey(pubkey)
        self.stigmergic_coord.set_our_pubkey(pubkey)
        self.defense_system.set_our_pubkey(pubkey)
        self.time_adjuster.set_our_pubkey(pubkey)

    def set_anticipatory_manager(self, mgr: Any) -> None:
        """Set or update the anticipatory liquidity manager for time-based fees."""
        self.time_adjuster.set_anticipatory_manager(mgr)

    def _log(self, msg: str, level: str = "info") -> None:
        if self.plugin:
            self.plugin.log(f"cl-hive: [FeeCoord] {msg}", level=level)

    def _get_last_fee_change_time(self, channel_id: str) -> float:
        """Get the timestamp of the last fee change for a channel."""
        return self._fee_change_times.get(channel_id, 0)

    def record_fee_change(self, channel_id: str) -> None:
        """Record that a fee change was made for a channel."""
        self._fee_change_times[channel_id] = time.time()
        self._log(f"Recorded fee change for {channel_id}")

    def _get_centrality_fee_adjustment(self) -> Tuple[float, float]:
        """
        Calculate fee adjustment based on our node's hive centrality.

        Returns:
            Tuple of (adjustment_pct, our_centrality)
            - adjustment_pct: positive = premium, negative = discount
            - our_centrality: current centrality score
        """
        if not CENTRALITY_FEE_ADJUSTMENT_ENABLED:
            return 0.0, 0.0

        if not self.our_pubkey:
            return 0.0, 0.0

        calculator = network_metrics.get_calculator()
        if not calculator:
            return 0.0, 0.0

        metrics = calculator.get_member_metrics(self.our_pubkey)
        if not metrics:
            return 0.0, 0.0

        centrality = metrics.hive_centrality

        # High centrality = premium (can charge more)
        if centrality >= CENTRALITY_FEE_HIGH_THRESHOLD:
            # Scale from 0 to max premium based on how far above threshold
            excess = centrality - CENTRALITY_FEE_HIGH_THRESHOLD
            max_excess = 1.0 - CENTRALITY_FEE_HIGH_THRESHOLD
            adjustment = CENTRALITY_FEE_MAX_PREMIUM_PCT * (excess / max_excess) if max_excess > 0 else 0
            return adjustment, centrality

        # Low centrality = discount (attract traffic)
        if centrality <= CENTRALITY_FEE_LOW_THRESHOLD:
            # Scale from 0 to max discount based on how far below threshold
            deficit = CENTRALITY_FEE_LOW_THRESHOLD - centrality
            max_deficit = CENTRALITY_FEE_LOW_THRESHOLD
            adjustment = -CENTRALITY_FEE_MAX_DISCOUNT_PCT * (deficit / max_deficit) if max_deficit > 0 else 0
            return adjustment, centrality

        # Middle range = no adjustment
        return 0.0, centrality

    def get_fee_recommendation(
        self,
        channel_id: str,
        peer_id: str,
        current_fee: int,
        local_balance_pct: float,
        source_hint: str = None,
        destination_hint: str = None,
        use_centrality_adjustment: bool = True
    ) -> FeeRecommendation:
        """
        Get coordinated fee recommendation for a channel.

        Combines all coordination signals:
        1. Corridor assignment (primary vs secondary)
        2. Pheromone-based adaptive suggestion
        3. Stigmergic markers from fleet
        4. Defensive adjustments
        5. Time-based adjustment (Phase 7.4)
        6. Centrality-based adjustment (Use Case 8)
        """
        # Start with current fee
        recommended_fee = current_fee
        is_primary = False
        floor_applied = False
        ceiling_applied = False
        stigmergic_influence = 0.0
        defensive_multiplier = 1.0
        centrality_adjustment_pct = 0.0
        our_hive_centrality = 0.0
        reasons = []

        # 1. Check corridor assignment
        if source_hint and destination_hint:
            corridor_fee, is_primary = self.corridor_mgr.get_fee_for_member(
                self.our_pubkey or "", source_hint, destination_hint
            )
            if corridor_fee != DEFAULT_FEE_PPM:
                recommended_fee = corridor_fee
                reasons.append(f"corridor_{'primary' if is_primary else 'secondary'}")

        # 2. Get adaptive controller suggestion
        adaptive_fee, adaptive_reason = self.adaptive_controller.suggest_fee(
            channel_id, recommended_fee, local_balance_pct
        )
        if adaptive_fee != recommended_fee:
            recommended_fee = adaptive_fee
            reasons.append(adaptive_reason)

        # 3. Check stigmergic markers
        if source_hint and destination_hint:
            stig_fee, stig_confidence = self.stigmergic_coord.calculate_coordinated_fee(
                source_hint, destination_hint, recommended_fee
            )
            if stig_confidence > 0.5 and stig_fee != recommended_fee:
                # Blend stigmergic signal with current recommendation
                blend_weight = stig_confidence * 0.5
                recommended_fee = int(
                    recommended_fee * (1 - blend_weight) +
                    stig_fee * blend_weight
                )
                stigmergic_influence = stig_confidence
                reasons.append(f"stigmergic_{stig_confidence:.2f}")

        # 4. Apply defensive multiplier
        defensive_multiplier = self.defense_system.get_defensive_multiplier(peer_id)
        if defensive_multiplier > 1.0:
            recommended_fee = int(recommended_fee * defensive_multiplier)
            reasons.append(f"defensive_{defensive_multiplier:.2f}x")

        # 5. Apply time-based adjustment (Phase 7.4)
        time_adjustment_pct = 0.0
        if self.time_adjuster.enabled:
            time_adj = self.time_adjuster.get_time_adjustment(
                channel_id, recommended_fee
            )
            if time_adj.adjustment_type != "none":
                recommended_fee = time_adj.adjusted_fee_ppm
                time_adjustment_pct = time_adj.adjustment_pct
                reasons.append(f"time_{time_adj.adjustment_type}")

        # 6. Apply centrality-based adjustment (Use Case 8)
        if use_centrality_adjustment:
            centrality_adjustment_pct, our_hive_centrality = self._get_centrality_fee_adjustment()
            if centrality_adjustment_pct != 0.0:
                adjustment = int(recommended_fee * centrality_adjustment_pct)
                recommended_fee += adjustment
                if centrality_adjustment_pct > 0:
                    reasons.append(f"centrality_premium_{centrality_adjustment_pct*100:.1f}%")
                else:
                    reasons.append(f"centrality_discount_{abs(centrality_adjustment_pct)*100:.1f}%")

        # 7. Apply floor and ceiling
        if recommended_fee < FLEET_FEE_FLOOR_PPM:
            recommended_fee = FLEET_FEE_FLOOR_PPM
            floor_applied = True
            reasons.append("floor_applied")

        if recommended_fee > FLEET_FEE_CEILING_PPM:
            recommended_fee = FLEET_FEE_CEILING_PPM
            ceiling_applied = True
            reasons.append("ceiling_applied")

        # Calculate confidence
        confidence = 0.5
        if is_primary:
            confidence += 0.2
        if stigmergic_influence > 0:
            confidence += stigmergic_influence * 0.2
        # Centrality boost to confidence (Use Case 8)
        if our_hive_centrality >= CENTRALITY_FEE_HIGH_THRESHOLD:
            confidence += 0.1
        confidence = min(0.95, confidence)

        # 8. Check salience (is this change worth making?)
        is_salient, salience_reason = is_fee_change_salient(
            current_fee=current_fee,
            new_fee=recommended_fee,
            last_change_time=self._get_last_fee_change_time(channel_id)
        )

        # If not salient, recommend keeping current fee
        if not is_salient:
            reasons.append(f"not_salient:{salience_reason}")

        return FeeRecommendation(
            channel_id=channel_id,
            peer_id=peer_id,
            current_fee_ppm=current_fee,
            recommended_fee_ppm=recommended_fee,
            is_primary=is_primary,
            corridor_source=source_hint,
            corridor_destination=destination_hint,
            floor_applied=floor_applied,
            ceiling_applied=ceiling_applied,
            stigmergic_influence=stigmergic_influence,
            defensive_multiplier=defensive_multiplier,
            time_adjustment_pct=time_adjustment_pct,
            centrality_adjustment_pct=centrality_adjustment_pct,
            our_hive_centrality=our_hive_centrality,
            is_salient=is_salient,
            salience_reason=salience_reason,
            confidence=confidence,
            reason="; ".join(reasons) if reasons else "default"
        )

    def record_routing_outcome(
        self,
        channel_id: str,
        peer_id: str,
        fee_ppm: int,
        success: bool,
        revenue_sats: int,
        source: str = None,
        destination: str = None
    ) -> None:
        """
        Record a routing outcome to update all coordination systems.
        """
        # Update pheromone
        self.adaptive_controller.update_pheromone(
            channel_id, fee_ppm, success, revenue_sats
        )

        # Record fee observation
        self.adaptive_controller.record_fee_observation(fee_ppm)

        # Deposit stigmergic marker
        if source and destination:
            self.stigmergic_coord.deposit_marker(
                source, destination, fee_ppm, success, revenue_sats if success else 0
            )

    def get_coordination_status(self) -> Dict:
        """Get overall fee coordination status."""
        assignments = self.corridor_mgr.get_assignments()
        markers = self.stigmergic_coord.get_all_markers()
        defense_status = self.defense_system.get_defense_status()
        pheromone_levels = self.adaptive_controller.get_all_pheromone_levels()
        time_status = self.time_adjuster.get_all_adjustments()

        return {
            "corridor_assignments": len(assignments),
            "active_markers": len(markers),
            "defense_status": defense_status,
            "pheromone_channels": len(pheromone_levels),
            "fleet_fee_floor": FLEET_FEE_FLOOR_PPM,
            "fleet_fee_ceiling": FLEET_FEE_CEILING_PPM,
            "time_based_fees": time_status,
            "assignments": [a.to_dict() for a in assignments[:10]],  # Limit output
            "recent_markers": [m.to_dict() for m in markers[:10]]
        }

    def get_time_fee_adjustment(
        self,
        channel_id: str,
        base_fee: int
    ) -> Dict[str, Any]:
        """
        Get time-based fee adjustment for a specific channel.

        Args:
            channel_id: Channel short ID
            base_fee: Current base fee in ppm

        Returns:
            Dict with adjustment details
        """
        adjustment = self.time_adjuster.get_time_adjustment(channel_id, base_fee)
        return adjustment.to_dict()

    def get_time_fee_status(self) -> Dict[str, Any]:
        """
        Get time-based fee system status.

        Returns overview of time-based fee adjustments and configuration.
        """
        return self.time_adjuster.get_all_adjustments()

    def get_channel_peak_hours(self, channel_id: str) -> List[Dict[str, Any]]:
        """Get detected peak hours for a channel."""
        return self.time_adjuster.detect_peak_hours(channel_id)

    def get_channel_low_hours(self, channel_id: str) -> List[Dict[str, Any]]:
        """Get detected low-activity hours for a channel."""
        return self.time_adjuster.detect_low_hours(channel_id)
