"""
Yield Metrics Module (Phase 1 - Metrics & Measurement)

Provides capital efficiency metrics and yield tracking for the hive.
These metrics enable:
- ROI per channel calculation
- Capital efficiency analysis
- Turn rate tracking
- Flow velocity prediction
- Internal competition detection

This module bridges cl-hive coordination with cl-revenue-ops profitability data.
"""

import math
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# CONSTANTS
# =============================================================================

# Flow intensity thresholds (from Physarum research)
FLOW_INTENSITY_HIGH = 0.02      # 2% daily turn rate = high flow
FLOW_INTENSITY_LOW = 0.001     # 0.1% daily turn rate = low flow

# Velocity prediction thresholds
DEPLETION_RISK_THRESHOLD = 0.15     # <15% local = depletion risk
SATURATION_RISK_THRESHOLD = 0.85    # >85% local = saturation risk

# Competition detection
MIN_CHANNELS_FOR_COMPETITION = 2    # Need at least 2 members with channels


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ChannelYieldMetrics:
    """
    Comprehensive yield metrics for a single channel.

    Tracks capital efficiency, ROI, and flow characteristics.
    """
    channel_id: str
    peer_id: str
    peer_alias: Optional[str] = None

    # Capacity
    capacity_sats: int = 0
    local_balance_sats: int = 0
    local_balance_pct: float = 0.0

    # Revenue (from routing)
    routing_revenue_sats: int = 0
    forward_count: int = 0
    period_days: int = 30

    # Costs
    open_cost_sats: int = 0
    rebalance_cost_sats: int = 0
    total_cost_sats: int = 0

    # Computed metrics
    net_revenue_sats: int = 0
    roi_pct: float = 0.0                 # (net_revenue / capacity) * 365 / period_days
    capital_efficiency: float = 0.0      # revenue / capacity
    turn_rate: float = 0.0               # volume / capacity per day
    annualized_yield_pct: float = 0.0    # Projected annual yield

    # Flow characteristics
    flow_direction: str = "balanced"      # "source", "sink", "balanced"
    flow_intensity: float = 0.0           # Daily turn rate
    volume_routed_sats: int = 0

    # Timestamps
    channel_age_days: int = 0
    last_forward_timestamp: int = 0
    metrics_timestamp: int = 0

    def __post_init__(self):
        """Calculate derived metrics."""
        self.metrics_timestamp = int(time.time())
        self._calculate_derived_metrics()

    def _calculate_derived_metrics(self):
        """Calculate all derived metrics from base values."""
        # Net revenue
        self.total_cost_sats = self.open_cost_sats + self.rebalance_cost_sats
        self.net_revenue_sats = self.routing_revenue_sats - self.total_cost_sats

        # ROI calculation
        if self.capacity_sats > 0 and self.period_days > 0:
            # Capital efficiency (revenue per sat of capacity)
            self.capital_efficiency = self.routing_revenue_sats / self.capacity_sats

            # Daily turn rate
            if self.volume_routed_sats > 0:
                self.turn_rate = (self.volume_routed_sats / self.capacity_sats) / self.period_days
                self.flow_intensity = self.turn_rate
            else:
                self.turn_rate = 0.0
                self.flow_intensity = 0.0

            # Annualized ROI
            daily_net = self.net_revenue_sats / self.period_days
            annual_net = daily_net * 365
            self.roi_pct = (annual_net / self.capacity_sats) * 100
            self.annualized_yield_pct = self.roi_pct

        # Local balance percentage
        if self.capacity_sats > 0:
            self.local_balance_pct = self.local_balance_sats / self.capacity_sats

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "peer_alias": self.peer_alias,
            "capacity_sats": self.capacity_sats,
            "local_balance_sats": self.local_balance_sats,
            "local_balance_pct": round(self.local_balance_pct, 4),
            "routing_revenue_sats": self.routing_revenue_sats,
            "forward_count": self.forward_count,
            "period_days": self.period_days,
            "open_cost_sats": self.open_cost_sats,
            "rebalance_cost_sats": self.rebalance_cost_sats,
            "total_cost_sats": self.total_cost_sats,
            "net_revenue_sats": self.net_revenue_sats,
            "roi_pct": round(self.roi_pct, 2),
            "capital_efficiency": round(self.capital_efficiency, 6),
            "turn_rate": round(self.turn_rate, 4),
            "annualized_yield_pct": round(self.annualized_yield_pct, 2),
            "flow_direction": self.flow_direction,
            "flow_intensity": round(self.flow_intensity, 4),
            "volume_routed_sats": self.volume_routed_sats,
            "channel_age_days": self.channel_age_days,
            "last_forward_timestamp": self.last_forward_timestamp,
            "metrics_timestamp": self.metrics_timestamp
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ChannelYieldMetrics":
        """Create from dictionary."""
        return cls(
            channel_id=data.get("channel_id", ""),
            peer_id=data.get("peer_id", ""),
            peer_alias=data.get("peer_alias"),
            capacity_sats=data.get("capacity_sats", 0),
            local_balance_sats=data.get("local_balance_sats", 0),
            routing_revenue_sats=data.get("routing_revenue_sats", 0),
            forward_count=data.get("forward_count", 0),
            period_days=data.get("period_days", 30),
            open_cost_sats=data.get("open_cost_sats", 0),
            rebalance_cost_sats=data.get("rebalance_cost_sats", 0),
            volume_routed_sats=data.get("volume_routed_sats", 0),
            channel_age_days=data.get("channel_age_days", 0),
            last_forward_timestamp=data.get("last_forward_timestamp", 0),
            flow_direction=data.get("flow_direction", "balanced")
        )


@dataclass
class ChannelVelocityPrediction:
    """
    Prediction of future channel state based on flow velocity.

    Used for predictive rebalancing and fee adjustments.
    """
    channel_id: str
    peer_id: str

    # Current state
    current_local_pct: float = 0.5
    current_local_sats: int = 0
    capacity_sats: int = 0

    # Velocity (change per hour)
    velocity_pct_per_hour: float = 0.0
    velocity_sats_per_hour: int = 0

    # Predictions
    predicted_local_pct_24h: float = 0.5
    predicted_local_pct_48h: float = 0.5
    hours_to_depletion: Optional[float] = None    # Hours until 0% local
    hours_to_saturation: Optional[float] = None   # Hours until 100% local

    # Risk scores (0.0 to 1.0)
    depletion_risk: float = 0.0
    saturation_risk: float = 0.0

    # Recommendation
    recommended_action: str = "none"  # "none", "preemptive_rebalance", "raise_fees", "lower_fees"
    urgency: str = "low"              # "low", "medium", "high", "critical"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "current_local_pct": round(self.current_local_pct, 4),
            "current_local_sats": self.current_local_sats,
            "capacity_sats": self.capacity_sats,
            "velocity_pct_per_hour": round(self.velocity_pct_per_hour, 6),
            "velocity_sats_per_hour": self.velocity_sats_per_hour,
            "predicted_local_pct_24h": round(self.predicted_local_pct_24h, 4),
            "predicted_local_pct_48h": round(self.predicted_local_pct_48h, 4),
            "hours_to_depletion": round(self.hours_to_depletion, 1) if self.hours_to_depletion else None,
            "hours_to_saturation": round(self.hours_to_saturation, 1) if self.hours_to_saturation else None,
            "depletion_risk": round(self.depletion_risk, 3),
            "saturation_risk": round(self.saturation_risk, 3),
            "recommended_action": self.recommended_action,
            "urgency": self.urgency
        }


@dataclass
class InternalCompetition:
    """
    Detected internal competition between fleet members.

    Occurs when multiple members have channels to the same source/destination pair.
    """
    source_peer_id: str
    destination_peer_id: str
    source_alias: Optional[str] = None
    destination_alias: Optional[str] = None

    # Competing members
    competing_members: List[str] = field(default_factory=list)
    member_count: int = 0

    # Competition metrics
    total_fleet_capacity_sats: int = 0
    estimated_fee_loss_pct: float = 0.0     # Estimated loss from undercutting

    # Recommendation
    recommendation: str = "coordinate_fees"  # "coordinate_fees", "specialize", "no_action"
    recommended_primary: Optional[str] = None  # Member who should be primary

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "source_peer_id": self.source_peer_id,
            "destination_peer_id": self.destination_peer_id,
            "source_alias": self.source_alias,
            "destination_alias": self.destination_alias,
            "competing_members": self.competing_members,
            "member_count": self.member_count,
            "total_fleet_capacity_sats": self.total_fleet_capacity_sats,
            "estimated_fee_loss_pct": round(self.estimated_fee_loss_pct, 2),
            "recommendation": self.recommendation,
            "recommended_primary": self.recommended_primary
        }


@dataclass
class FleetYieldSummary:
    """
    Aggregated yield metrics for the entire fleet.
    """
    # Fleet totals
    total_capacity_sats: int = 0
    total_revenue_sats: int = 0
    total_costs_sats: int = 0
    total_net_revenue_sats: int = 0

    # Averages
    avg_roi_pct: float = 0.0
    avg_turn_rate: float = 0.0
    avg_capital_efficiency: float = 0.0

    # Channel counts
    total_channels: int = 0
    profitable_channels: int = 0
    underwater_channels: int = 0
    high_flow_channels: int = 0
    low_flow_channels: int = 0

    # Competition
    internal_competition_count: int = 0
    estimated_competition_loss_sats: int = 0

    # Period
    period_days: int = 30
    timestamp: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "total_capacity_sats": self.total_capacity_sats,
            "total_revenue_sats": self.total_revenue_sats,
            "total_costs_sats": self.total_costs_sats,
            "total_net_revenue_sats": self.total_net_revenue_sats,
            "avg_roi_pct": round(self.avg_roi_pct, 2),
            "avg_turn_rate": round(self.avg_turn_rate, 4),
            "avg_capital_efficiency": round(self.avg_capital_efficiency, 6),
            "total_channels": self.total_channels,
            "profitable_channels": self.profitable_channels,
            "underwater_channels": self.underwater_channels,
            "high_flow_channels": self.high_flow_channels,
            "low_flow_channels": self.low_flow_channels,
            "internal_competition_count": self.internal_competition_count,
            "estimated_competition_loss_sats": self.estimated_competition_loss_sats,
            "period_days": self.period_days,
            "timestamp": self.timestamp
        }


# =============================================================================
# YIELD METRICS MANAGER
# =============================================================================

class YieldMetricsManager:
    """
    Manages yield metrics collection and analysis for the hive.

    Integrates with:
    - cl-revenue-ops for profitability data (via bridge)
    - State manager for member topology
    - Routing pool for revenue data
    """

    def __init__(
        self,
        database: Any,
        plugin: Any,
        state_manager: Any = None,
        routing_pool: Any = None,
        bridge: Any = None
    ):
        """
        Initialize the yield metrics manager.

        Args:
            database: HiveDatabase instance
            plugin: Plugin instance for RPC/logging
            state_manager: StateManager for member topology
            routing_pool: RoutingPool for revenue data
            bridge: Bridge to cl-revenue-ops for profitability data
        """
        self.database = database
        self.plugin = plugin
        self.state_manager = state_manager
        self.routing_pool = routing_pool
        self.bridge = bridge
        self.our_pubkey: Optional[str] = None

        # Cache for velocity calculations
        self._velocity_cache: Dict[str, Dict] = {}
        self._velocity_cache_ttl = 300  # 5 minutes

        # Remote yield metrics from fleet members
        self._remote_yield_metrics: Dict[str, List[Dict[str, Any]]] = {}

    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's pubkey after initialization."""
        self.our_pubkey = pubkey

    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"cl-hive: [YieldMetrics] {msg}", level=level)

    # =========================================================================
    # CHANNEL YIELD METRICS
    # =========================================================================

    def get_channel_yield_metrics(
        self,
        channel_id: str = None,
        period_days: int = 30
    ) -> List[ChannelYieldMetrics]:
        """
        Get yield metrics for channels.

        Args:
            channel_id: Optional specific channel (None for all)
            period_days: Analysis period in days

        Returns:
            List of ChannelYieldMetrics
        """
        metrics = []

        try:
            # Get channel list
            if channel_id:
                channels_resp = self.plugin.rpc.listpeerchannels()
                channels = [
                    ch for ch in channels_resp.get("channels", [])
                    if ch.get("short_channel_id") == channel_id
                ]
            else:
                channels_resp = self.plugin.rpc.listpeerchannels()
                channels = channels_resp.get("channels", [])

            # Get profitability data from cl-revenue-ops if available
            profitability_data = {}
            if self.bridge and hasattr(self.bridge, 'get_profitability'):
                try:
                    prof_result = self.bridge.get_profitability()
                    if prof_result:
                        for ch_prof in prof_result.get("channels", []):
                            profitability_data[ch_prof.get("channel_id")] = ch_prof
                except Exception:
                    pass

            for ch in channels:
                if ch.get("state") != "CHANNELD_NORMAL":
                    continue

                scid = ch.get("short_channel_id", "")
                peer_id = ch.get("peer_id", "")

                # Get capacity and balance
                capacity_msat = ch.get("total_msat", 0)
                local_msat = ch.get("to_us_msat", 0)
                capacity_sats = capacity_msat // 1000
                local_sats = local_msat // 1000

                # Get profitability data if available
                prof = profitability_data.get(scid, {})

                # Get peer alias
                peer_alias = None
                try:
                    nodes = self.plugin.rpc.listnodes(id=peer_id)
                    if nodes.get("nodes"):
                        peer_alias = nodes["nodes"][0].get("alias")
                except Exception:
                    pass

                # Calculate channel age
                funding_txid = ch.get("funding_txid")
                channel_age_days = self._get_channel_age_days(funding_txid)

                # Determine flow direction
                flow_direction = "balanced"
                in_sats = prof.get("in_sats", 0)
                out_sats = prof.get("out_sats", 0)
                if in_sats > out_sats * 1.5:
                    flow_direction = "sink"
                elif out_sats > in_sats * 1.5:
                    flow_direction = "source"

                yield_metric = ChannelYieldMetrics(
                    channel_id=scid,
                    peer_id=peer_id,
                    peer_alias=peer_alias,
                    capacity_sats=capacity_sats,
                    local_balance_sats=local_sats,
                    routing_revenue_sats=prof.get("fees_earned_sats", 0),
                    forward_count=prof.get("forward_count", 0),
                    period_days=period_days,
                    open_cost_sats=prof.get("open_cost_sats", 0),
                    rebalance_cost_sats=prof.get("rebalance_cost_sats", 0),
                    volume_routed_sats=prof.get("volume_routed_sats", 0),
                    channel_age_days=channel_age_days,
                    last_forward_timestamp=prof.get("last_forward_timestamp", 0),
                    flow_direction=flow_direction
                )

                metrics.append(yield_metric)

        except Exception as e:
            self._log(f"Error getting channel yield metrics: {e}", level="debug")

        return metrics

    def _get_channel_age_days(self, funding_txid: str) -> int:
        """Get channel age in days from funding transaction."""
        if not funding_txid:
            return 0

        try:
            # Try to get block height from transaction
            tx = self.plugin.rpc.gettxout(txid=funding_txid, vout=0)
            if tx and tx.get("height"):
                current_height = self.plugin.rpc.getinfo().get("blockheight", 0)
                blocks_old = current_height - tx.get("height")
                return blocks_old // 144  # ~144 blocks per day
        except Exception:
            pass

        return 0

    # =========================================================================
    # VELOCITY PREDICTION
    # =========================================================================

    def predict_channel_state(
        self,
        channel_id: str,
        hours: int = 24
    ) -> Optional[ChannelVelocityPrediction]:
        """
        Predict channel balance at future time based on flow velocity.

        Uses historical balance changes to extrapolate future state.

        Args:
            channel_id: Channel to predict
            hours: Hours into the future to predict

        Returns:
            ChannelVelocityPrediction or None if insufficient data
        """
        try:
            # Get current channel state
            channels_resp = self.plugin.rpc.listpeerchannels()
            channel = None
            for ch in channels_resp.get("channels", []):
                if ch.get("short_channel_id") == channel_id:
                    channel = ch
                    break

            if not channel or channel.get("state") != "CHANNELD_NORMAL":
                return None

            peer_id = channel.get("peer_id", "")
            capacity_msat = channel.get("total_msat", 0)
            local_msat = channel.get("to_us_msat", 0)
            capacity_sats = capacity_msat // 1000
            local_sats = local_msat // 1000
            local_pct = local_sats / capacity_sats if capacity_sats > 0 else 0.5

            # Get velocity from advisor database if available
            velocity_pct_per_hour = 0.0
            if self.database:
                # Query historical balance data
                velocity_data = self._calculate_velocity_from_history(channel_id)
                if velocity_data:
                    velocity_pct_per_hour = velocity_data.get("velocity_pct_per_hour", 0.0)

            # Calculate predictions
            velocity_sats_per_hour = int(velocity_pct_per_hour * capacity_sats)

            predicted_24h = max(0.0, min(1.0, local_pct + velocity_pct_per_hour * 24))
            predicted_48h = max(0.0, min(1.0, local_pct + velocity_pct_per_hour * 48))

            # Calculate hours to depletion/saturation
            hours_to_depletion = None
            hours_to_saturation = None

            if velocity_pct_per_hour < 0:  # Draining
                hours_to_depletion = local_pct / abs(velocity_pct_per_hour) if velocity_pct_per_hour != 0 else None
            elif velocity_pct_per_hour > 0:  # Filling
                remaining = 1.0 - local_pct
                hours_to_saturation = remaining / velocity_pct_per_hour if velocity_pct_per_hour != 0 else None

            # Calculate risk scores
            depletion_risk = 0.0
            saturation_risk = 0.0

            if hours_to_depletion is not None and hours_to_depletion < 48:
                # Risk increases as depletion approaches
                depletion_risk = max(0.0, min(1.0, 1.0 - hours_to_depletion / 48))
            elif local_pct < DEPLETION_RISK_THRESHOLD:
                depletion_risk = min(1.0, 0.5 + (DEPLETION_RISK_THRESHOLD - local_pct) * 2)

            if hours_to_saturation is not None and hours_to_saturation < 48:
                saturation_risk = max(0.0, min(1.0, 1.0 - hours_to_saturation / 48))
            elif local_pct > SATURATION_RISK_THRESHOLD:
                saturation_risk = min(1.0, 0.5 + (local_pct - SATURATION_RISK_THRESHOLD) * 2)

            # Determine recommended action
            recommended_action = "none"
            urgency = "low"

            if depletion_risk > 0.7:
                recommended_action = "raise_fees"
                urgency = "critical" if depletion_risk > 0.9 else "high"
            elif depletion_risk > 0.3:
                recommended_action = "preemptive_rebalance"
                urgency = "medium"
            elif saturation_risk > 0.7:
                recommended_action = "lower_fees"
                urgency = "critical" if saturation_risk > 0.9 else "high"
            elif saturation_risk > 0.3:
                recommended_action = "preemptive_rebalance"
                urgency = "medium"

            return ChannelVelocityPrediction(
                channel_id=channel_id,
                peer_id=peer_id,
                current_local_pct=local_pct,
                current_local_sats=local_sats,
                capacity_sats=capacity_sats,
                velocity_pct_per_hour=velocity_pct_per_hour,
                velocity_sats_per_hour=velocity_sats_per_hour,
                predicted_local_pct_24h=predicted_24h,
                predicted_local_pct_48h=predicted_48h,
                hours_to_depletion=hours_to_depletion,
                hours_to_saturation=hours_to_saturation,
                depletion_risk=depletion_risk,
                saturation_risk=saturation_risk,
                recommended_action=recommended_action,
                urgency=urgency
            )

        except Exception as e:
            self._log(f"Error predicting channel state: {e}", level="debug")
            return None

    def _calculate_velocity_from_history(self, channel_id: str) -> Optional[Dict]:
        """
        Calculate balance velocity from historical snapshots.

        Uses advisor database snapshots if available.
        """
        # Check cache first
        now = time.time()
        cached = self._velocity_cache.get(channel_id)
        if cached and now - cached.get("timestamp", 0) < self._velocity_cache_ttl:
            return cached

        try:
            # Query channel history from advisor database
            history = self.database.get_channel_history(channel_id, hours=48)

            if not history or len(history) < 2:
                return None

            # Calculate velocity from balance changes
            first = history[0]
            last = history[-1]

            first_pct = first.get("local_pct", 0.5)
            last_pct = last.get("local_pct", 0.5)
            first_ts = first.get("timestamp", 0)
            last_ts = last.get("timestamp", 0)

            hours_elapsed = (last_ts - first_ts) / 3600
            if hours_elapsed < 1:
                return None

            velocity_pct_per_hour = (last_pct - first_pct) / hours_elapsed

            result = {
                "velocity_pct_per_hour": velocity_pct_per_hour,
                "data_points": len(history),
                "hours_analyzed": hours_elapsed,
                "timestamp": now
            }

            # Cache result
            self._velocity_cache[channel_id] = result

            return result

        except Exception:
            return None

    def get_critical_velocity_channels(
        self,
        threshold_hours: int = 24
    ) -> List[ChannelVelocityPrediction]:
        """
        Get channels with critical velocity (depleting/filling rapidly).

        Args:
            threshold_hours: Alert if depletion/saturation within this time

        Returns:
            List of channels with critical velocity
        """
        critical = []

        try:
            channels_resp = self.plugin.rpc.listpeerchannels()

            for ch in channels_resp.get("channels", []):
                if ch.get("state") != "CHANNELD_NORMAL":
                    continue

                scid = ch.get("short_channel_id")
                if not scid:
                    continue

                prediction = self.predict_channel_state(scid, hours=threshold_hours)
                if not prediction:
                    continue

                # Include if critical velocity
                is_critical = (
                    (prediction.hours_to_depletion is not None and
                     prediction.hours_to_depletion < threshold_hours) or
                    (prediction.hours_to_saturation is not None and
                     prediction.hours_to_saturation < threshold_hours) or
                    prediction.depletion_risk > 0.5 or
                    prediction.saturation_risk > 0.5
                )

                if is_critical:
                    critical.append(prediction)

        except Exception as e:
            self._log(f"Error getting critical velocity channels: {e}", level="debug")

        # Sort by urgency
        urgency_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
        critical.sort(key=lambda p: urgency_order.get(p.urgency, 3))

        return critical

    # =========================================================================
    # FLEET SUMMARY
    # =========================================================================

    def get_fleet_yield_summary(self, period_days: int = 30) -> FleetYieldSummary:
        """
        Get aggregated yield metrics for the entire fleet.

        Args:
            period_days: Analysis period

        Returns:
            FleetYieldSummary with aggregated metrics
        """
        summary = FleetYieldSummary(
            period_days=period_days,
            timestamp=int(time.time())
        )

        try:
            # Get yield metrics for all channels
            metrics = self.get_channel_yield_metrics(period_days=period_days)

            if not metrics:
                return summary

            summary.total_channels = len(metrics)

            # Aggregate metrics
            total_roi = 0.0
            total_turn_rate = 0.0
            total_efficiency = 0.0

            for m in metrics:
                summary.total_capacity_sats += m.capacity_sats
                summary.total_revenue_sats += m.routing_revenue_sats
                summary.total_costs_sats += m.total_cost_sats

                total_roi += m.roi_pct
                total_turn_rate += m.turn_rate
                total_efficiency += m.capital_efficiency

                # Count by category
                if m.roi_pct > 0:
                    summary.profitable_channels += 1
                elif m.roi_pct < -10:
                    summary.underwater_channels += 1

                if m.flow_intensity > FLOW_INTENSITY_HIGH:
                    summary.high_flow_channels += 1
                elif m.flow_intensity < FLOW_INTENSITY_LOW:
                    summary.low_flow_channels += 1

            # Calculate averages
            if summary.total_channels > 0:
                summary.avg_roi_pct = total_roi / summary.total_channels
                summary.avg_turn_rate = total_turn_rate / summary.total_channels
                summary.avg_capital_efficiency = total_efficiency / summary.total_channels

            summary.total_net_revenue_sats = (
                summary.total_revenue_sats - summary.total_costs_sats
            )

        except Exception as e:
            self._log(f"Error calculating fleet yield summary: {e}", level="debug")

        return summary

    # =========================================================================
    # FLEET INTELLIGENCE SHARING (Phase 14)
    # =========================================================================

    def get_shareable_yield_metrics(
        self,
        period_days: int = 30,
        exclude_peer_ids: Optional[set] = None,
        max_metrics: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Get yield metrics suitable for sharing with fleet.

        Only shares metrics for external peers (not hive members).

        Args:
            period_days: Analysis period
            exclude_peer_ids: Set of peer IDs to exclude (e.g., hive members)
            max_metrics: Maximum number of entries to return

        Returns:
            List of yield metric dicts ready for serialization
        """
        exclude_peer_ids = exclude_peer_ids or set()
        shareable = []

        try:
            metrics = self.get_channel_yield_metrics(period_days=period_days)

            for m in metrics:
                # Skip hive members
                if m.peer_id in exclude_peer_ids:
                    continue

                # Determine profitability tier
                if m.flow_intensity < FLOW_INTENSITY_LOW and m.roi_pct < 0:
                    tier = "zombie"
                elif m.roi_pct < -10:
                    tier = "underwater"
                elif m.roi_pct > 0:
                    tier = "profitable"
                elif m.flow_intensity < FLOW_INTENSITY_LOW:
                    tier = "stagnant"
                else:
                    tier = "unknown"

                shareable.append({
                    "peer_id": m.peer_id,
                    "channel_id": m.channel_id,
                    "roi_pct": round(m.roi_pct, 2),
                    "capital_efficiency": round(m.capital_efficiency, 8),
                    "flow_intensity": round(m.flow_intensity, 4),
                    "profitability_tier": tier,
                    "period_days": period_days,
                    "capacity_sats": m.capacity_sats,
                    "net_revenue_sats": m.net_revenue_sats
                })

        except Exception as e:
            self._log(f"Error collecting shareable yield metrics: {e}", level="debug")

        # Sort by absolute ROI (most informative first)
        shareable.sort(key=lambda x: -abs(x["roi_pct"]))

        return shareable[:max_metrics]

    def receive_yield_metrics_from_fleet(
        self,
        reporter_id: str,
        metrics_data: Dict[str, Any]
    ) -> bool:
        """
        Receive yield metrics from another fleet member.

        Stores remote metrics for use in positioning decisions.

        Args:
            reporter_id: The fleet member who reported this
            metrics_data: Dict with peer_id, roi_pct, etc.

        Returns:
            True if stored successfully
        """
        peer_id = metrics_data.get("peer_id")
        if not peer_id:
            return False

        # Initialize remote metrics storage if needed
        if not hasattr(self, "_remote_yield_metrics"):
            self._remote_yield_metrics = {}

        entry = {
            "reporter_id": reporter_id,
            "roi_pct": metrics_data.get("roi_pct", 0),
            "capital_efficiency": metrics_data.get("capital_efficiency", 0),
            "flow_intensity": metrics_data.get("flow_intensity", 0),
            "profitability_tier": metrics_data.get("profitability_tier", "unknown"),
            "capacity_sats": metrics_data.get("capacity_sats", 0),
            "timestamp": time.time()
        }

        if peer_id not in self._remote_yield_metrics:
            self._remote_yield_metrics[peer_id] = []

        # Keep only recent reports per peer (last 5 reporters)
        self._remote_yield_metrics[peer_id].append(entry)
        if len(self._remote_yield_metrics[peer_id]) > 5:
            self._remote_yield_metrics[peer_id] = self._remote_yield_metrics[peer_id][-5:]

        # Evict least-recently-updated peer if dict exceeds limit
        max_peers = 200
        if len(self._remote_yield_metrics) > max_peers:
            oldest_pid = min(
                (p for p in self._remote_yield_metrics if p != peer_id),
                key=lambda p: max(
                    (e.get("timestamp", 0) for e in self._remote_yield_metrics[p]),
                    default=0
                ),
                default=None
            )
            if oldest_pid:
                del self._remote_yield_metrics[oldest_pid]

        return True

    def get_fleet_yield_consensus(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get consensus yield metrics for a peer from fleet reports.

        Aggregates reports from multiple members to get consensus view.

        Args:
            peer_id: External peer to get consensus for

        Returns:
            Dict with consensus metrics or None if no data
        """
        if not hasattr(self, "_remote_yield_metrics"):
            return None

        reports = self._remote_yield_metrics.get(peer_id, [])
        if not reports:
            return None

        # Filter to recent reports (last 7 days)
        now = time.time()
        recent = [r for r in reports if now - r.get("timestamp", 0) < 7 * 86400]
        if not recent:
            return None

        # Calculate averages
        avg_roi = sum(r.get("roi_pct", 0) for r in recent) / len(recent)
        avg_efficiency = sum(r.get("capital_efficiency", 0) for r in recent) / len(recent)
        avg_flow = sum(r.get("flow_intensity", 0) for r in recent) / len(recent)

        # Consensus tier (majority vote)
        tier_counts: Dict[str, int] = {}
        for r in recent:
            tier = r.get("profitability_tier", "unknown")
            tier_counts[tier] = tier_counts.get(tier, 0) + 1
        consensus_tier = max(tier_counts, key=tier_counts.get) if tier_counts else "unknown"

        return {
            "peer_id": peer_id,
            "avg_roi_pct": round(avg_roi, 2),
            "avg_capital_efficiency": round(avg_efficiency, 8),
            "avg_flow_intensity": round(avg_flow, 4),
            "consensus_tier": consensus_tier,
            "reporter_count": len(recent),
            "confidence": min(1.0, len(recent) / 3)  # 3+ reporters = high confidence
        }

    def get_all_fleet_yield_consensus(self) -> Dict[str, Dict[str, Any]]:
        """Get consensus yield metrics for all peers with fleet data."""
        if not hasattr(self, "_remote_yield_metrics"):
            return {}

        consensus = {}
        for peer_id in self._remote_yield_metrics:
            result = self.get_fleet_yield_consensus(peer_id)
            if result:
                consensus[peer_id] = result
        return consensus

    def cleanup_old_remote_yield_metrics(self, max_age_days: float = 7) -> int:
        """Remove old remote yield data."""
        if not hasattr(self, "_remote_yield_metrics"):
            return 0

        cutoff = time.time() - (max_age_days * 86400)
        cleaned = 0

        for peer_id in list(self._remote_yield_metrics.keys()):
            before = len(self._remote_yield_metrics[peer_id])
            self._remote_yield_metrics[peer_id] = [
                r for r in self._remote_yield_metrics[peer_id]
                if r.get("timestamp", 0) > cutoff
            ]
            cleaned += before - len(self._remote_yield_metrics[peer_id])

            if not self._remote_yield_metrics[peer_id]:
                del self._remote_yield_metrics[peer_id]

        return cleaned
