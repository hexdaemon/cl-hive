#!/usr/bin/env python3
"""
cl-hive: Distributed Swarm Intelligence for Lightning Node Fleets

This plugin implements "The Hive" protocol, enabling independent Lightning nodes
to function as a coordinated swarm. It provides:
- Zero-cost capital teleportation between fleet members
- Coordinated topology optimization (anti-overlap)
- Distributed immunity via shared ban lists
- Intent Lock protocol for conflict-free coordination

ARCHITECTURE:
-------------
cl-hive is a COORDINATION layer that sits ABOVE cl-revenue-ops.
It uses cl-revenue-ops PolicyManager for fee control (strategy=hive)
and the Strategic Rebalance Exemption for load balancing.

    cl-hive (Coordination)
         │
         ▼
    cl-revenue-ops (Execution)
         │
         ▼
    Core Lightning

DEPENDENCIES:
- cl-revenue-ops v1.4.0+ (PolicyManager with HIVE strategy)
- pyln-client: Core Lightning plugin framework

Author: Lightning Goats Team
License: MIT
"""

import json
import os
import signal
import threading
import time
import secrets
from typing import Dict, Optional, Any

from pyln.client import Plugin, RpcError

# Import our modules
from modules.config import HiveConfig
from modules.database import HiveDatabase
from modules.protocol import (
    HIVE_MAGIC, HiveMessageType,
    MAX_MESSAGE_BYTES, is_hive_message, deserialize, serialize,
    validate_promotion_request, validate_vouch, validate_promotion,
    validate_member_left, validate_ban_proposal, validate_ban_vote,
    validate_peer_available, create_peer_available,
    validate_expansion_nominate, validate_expansion_elect, validate_expansion_decline,
    create_expansion_nominate, create_expansion_elect, create_expansion_decline,
    get_expansion_nominate_signing_payload, get_expansion_elect_signing_payload,
    get_expansion_decline_signing_payload,
    VOUCH_TTL_SECONDS, MAX_VOUCHES_IN_PROMOTION,
    create_challenge, create_welcome,
    # Signed message validation (security hardening)
    validate_gossip, validate_state_hash, validate_full_sync, validate_intent_abort,
    get_gossip_signing_payload, get_state_hash_signing_payload,
    get_full_sync_signing_payload, get_intent_abort_signing_payload,
    get_peer_available_signing_payload, compute_states_hash,
    # Settlement offer broadcast
    create_settlement_offer, get_settlement_offer_signing_payload,
)
from modules.handshake import HandshakeManager, Ticket, CHALLENGE_TTL_SECONDS
from modules.state_manager import StateManager
from modules.gossip import GossipManager
from modules.intent_manager import IntentManager, Intent, IntentType
from modules.bridge import Bridge, BridgeStatus, CircuitOpenError
from modules.contribution import ContributionManager
from modules.membership import MembershipManager, MembershipTier
from modules.planner import Planner, ChannelSizer
from modules.quality_scorer import PeerQualityScorer
from modules.cooperative_expansion import CooperativeExpansionManager
from modules.clboss_bridge import CLBossBridge
from modules.governance import DecisionEngine
from modules.vpn_transport import VPNTransportManager
from modules.fee_intelligence import FeeIntelligenceManager
from modules.liquidity_coordinator import LiquidityCoordinator
from modules.splice_coordinator import SpliceCoordinator
from modules.health_aggregator import HealthScoreAggregator, HealthTier
from modules.routing_intelligence import HiveRoutingMap
from modules.peer_reputation import PeerReputationManager
from modules.routing_pool import RoutingPool
from modules.settlement import SettlementManager
from modules.yield_metrics import YieldMetricsManager
from modules.fee_coordination import FeeCoordinationManager
from modules.cost_reduction import CostReductionManager
from modules.channel_rationalization import RationalizationManager
from modules.strategic_positioning import StrategicPositioningManager
from modules.anticipatory_liquidity import AnticipatoryLiquidityManager
from modules.rpc_commands import (
    HiveContext,
    status as rpc_status,
    get_config as rpc_get_config,
    members as rpc_members,
    vpn_status as rpc_vpn_status,
    expansion_recommendations as rpc_expansion_recommendations,
    vpn_add_peer as rpc_vpn_add_peer,
    vpn_remove_peer as rpc_vpn_remove_peer,
    pending_actions as rpc_pending_actions,
    approve_action as rpc_approve_action,
    reject_action as rpc_reject_action,
    budget_summary as rpc_budget_summary,
    set_mode as rpc_set_mode,
    enable_expansions as rpc_enable_expansions,
    pending_bans as rpc_pending_bans,
    # Phase 4: Topology, Planner, and Query Commands
    reinit_bridge as rpc_reinit_bridge,
    topology as rpc_topology,
    planner_log as rpc_planner_log,
    intent_status as rpc_intent_status,
    contribution as rpc_contribution,
    expansion_status as rpc_expansion_status,
    # Phase 0: Routing Pool (Collective Economics)
    pool_status as rpc_pool_status,
    pool_member_status as rpc_pool_member_status,
    pool_snapshot as rpc_pool_snapshot,
    pool_distribution as rpc_pool_distribution,
    pool_settle as rpc_pool_settle,
    pool_record_revenue as rpc_pool_record_revenue,
    # Phase 1: Yield Metrics & Measurement
    yield_metrics as rpc_yield_metrics,
    yield_summary as rpc_yield_summary,
    velocity_prediction as rpc_velocity_prediction,
    critical_velocity_channels as rpc_critical_velocity_channels,
    internal_competition as rpc_internal_competition,
    # Phase 2: Fee Coordination
    fee_recommendation as rpc_fee_recommendation,
    corridor_assignments as rpc_corridor_assignments,
    stigmergic_markers as rpc_stigmergic_markers,
    deposit_marker as rpc_deposit_marker,
    defense_status as rpc_defense_status,
    broadcast_warning as rpc_broadcast_warning,
    pheromone_levels as rpc_pheromone_levels,
    fee_coordination_status as rpc_fee_coordination_status,
    # Phase 3 - Cost Reduction
    rebalance_recommendations as rpc_rebalance_recommendations,
    fleet_rebalance_path as rpc_fleet_rebalance_path,
    record_rebalance_outcome as rpc_record_rebalance_outcome,
    circular_flow_status as rpc_circular_flow_status,
    cost_reduction_status as rpc_cost_reduction_status,
    # Channel Rationalization
    coverage_analysis as rpc_coverage_analysis,
    close_recommendations as rpc_close_recommendations,
    create_close_actions as rpc_create_close_actions,
    rationalization_summary as rpc_rationalization_summary,
    rationalization_status as rpc_rationalization_status,
    # Phase 5 - Strategic Positioning
    valuable_corridors as rpc_valuable_corridors,
    exchange_coverage as rpc_exchange_coverage,
    positioning_recommendations as rpc_positioning_recommendations,
    flow_recommendations as rpc_flow_recommendations,
    report_flow_intensity as rpc_report_flow_intensity,
    positioning_summary as rpc_positioning_summary,
    positioning_status as rpc_positioning_status,
)

# Initialize the plugin
plugin = Plugin()

# =============================================================================
# GRACEFUL SHUTDOWN SUPPORT
# =============================================================================
# This event signals all background threads to exit cleanly.
# When `lightning-cli plugin stop cl-hive` is called, CLN sends SIGTERM.

shutdown_event = threading.Event()

# =============================================================================
# THREAD-SAFE RPC WRAPPER
# =============================================================================
# pyln-client's RPC is not inherently thread-safe for concurrent calls.
# This lock serializes all RPC calls to prevent race conditions.

RPC_LOCK = threading.Lock()

# X-01: Timeout for RPC lock acquisition to prevent global stalls
RPC_LOCK_TIMEOUT_SECONDS = 10


class RpcLockTimeoutError(TimeoutError):
    """Raised when RPC lock cannot be acquired within timeout."""
    pass


class ThreadSafeRpcProxy:
    """
    A thread-safe proxy for the plugin's RPC interface.

    Ensures all RPC calls are serialized through a lock, preventing
    race conditions when multiple background threads make concurrent
    calls to lightningd.

    X-01: Uses timeout on lock acquisition to prevent global stalls.
    """

    def __init__(self, rpc):
        """Wrap the original RPC object."""
        self._rpc = rpc

    def __getattr__(self, name):
        """Intercept attribute access to wrap RPC method calls."""
        original_method = getattr(self._rpc, name)

        if callable(original_method):
            def thread_safe_method(*args, **kwargs):
                # X-01: Use timeout to prevent indefinite blocking
                acquired = RPC_LOCK.acquire(timeout=RPC_LOCK_TIMEOUT_SECONDS)
                if not acquired:
                    raise RpcLockTimeoutError(
                        f"RPC lock acquisition timed out after {RPC_LOCK_TIMEOUT_SECONDS}s"
                    )
                try:
                    return original_method(*args, **kwargs)
                finally:
                    RPC_LOCK.release()
            return thread_safe_method
        else:
            return original_method

    def call(self, method_name, payload=None, **kwargs):
        """Thread-safe wrapper for the generic RPC call method.

        Supports both positional payload dict and keyword arguments.
        If kwargs are provided, they are merged with payload (kwargs take precedence).
        """
        # X-01: Use timeout to prevent indefinite blocking
        acquired = RPC_LOCK.acquire(timeout=RPC_LOCK_TIMEOUT_SECONDS)
        if not acquired:
            raise RpcLockTimeoutError(
                f"RPC lock acquisition timed out after {RPC_LOCK_TIMEOUT_SECONDS}s"
            )
        try:
            # Merge payload dict with kwargs
            if kwargs:
                merged = {**(payload or {}), **kwargs}
                return self._rpc.call(method_name, merged)
            elif payload:
                return self._rpc.call(method_name, payload)
            return self._rpc.call(method_name)
        finally:
            RPC_LOCK.release()

    def get_socket_path(self) -> Optional[str]:
        """Expose the underlying Lightning RPC socket path if available."""
        return getattr(self._rpc, "socket_path", None)


class ThreadSafePluginProxy:
    """
    A proxy for the Plugin object that provides thread-safe RPC access.
    
    Allows modules to use the same interface (self.plugin.rpc.method())
    while ensuring all RPC calls are serialized through the lock.
    """
    
    def __init__(self, plugin):
        """Wrap the original plugin with a thread-safe RPC proxy."""
        self._plugin = plugin
        self.rpc = ThreadSafeRpcProxy(plugin.rpc)
    
    def log(self, message, level='info'):
        """Delegate logging to the original plugin."""
        self._plugin.log(message, level=level)
    
    def __getattr__(self, name):
        """Delegate all other attribute access to the original plugin."""
        return getattr(self._plugin, name)


# =============================================================================
# GLOBAL INSTANCES (initialized in init)
# =============================================================================

database: Optional[HiveDatabase] = None
config: Optional[HiveConfig] = None
safe_plugin: Optional[ThreadSafePluginProxy] = None
handshake_mgr: Optional[HandshakeManager] = None
state_manager: Optional[StateManager] = None
gossip_mgr: Optional[GossipManager] = None
intent_mgr: Optional[IntentManager] = None
bridge: Optional[Bridge] = None
membership_mgr: Optional[MembershipManager] = None
contribution_mgr: Optional[ContributionManager] = None
planner: Optional[Planner] = None
clboss_bridge: Optional[CLBossBridge] = None
decision_engine: Optional[DecisionEngine] = None
vpn_transport: Optional[VPNTransportManager] = None
coop_expansion: Optional[CooperativeExpansionManager] = None
fee_intel_mgr: Optional[FeeIntelligenceManager] = None
health_aggregator: Optional[HealthScoreAggregator] = None
liquidity_coord: Optional[LiquidityCoordinator] = None
splice_coord: Optional[SpliceCoordinator] = None
routing_map: Optional[HiveRoutingMap] = None
peer_reputation_mgr: Optional[PeerReputationManager] = None
routing_pool: Optional[RoutingPool] = None
settlement_mgr: Optional[SettlementManager] = None
yield_metrics_mgr: Optional[YieldMetricsManager] = None
fee_coordination_mgr: Optional[FeeCoordinationManager] = None
cost_reduction_mgr: Optional[CostReductionManager] = None
rationalization_mgr: Optional[RationalizationManager] = None
strategic_positioning_mgr: Optional[StrategicPositioningManager] = None
anticipatory_liquidity_mgr: Optional[AnticipatoryLiquidityManager] = None
our_pubkey: Optional[str] = None


# =============================================================================
# RATE LIMITER (Security Enhancement)
# =============================================================================

class RateLimiter:
    """
    Token bucket rate limiter for gossip message flooding prevention.

    Tracks message rates per sender and rejects messages that exceed
    the configured rate. Uses a sliding window approach.
    """

    def __init__(self, max_per_minute: int = 10, window_seconds: int = 60):
        """
        Initialize the rate limiter.

        Args:
            max_per_minute: Maximum messages allowed per window
            window_seconds: Size of the sliding window in seconds
        """
        self._max_messages = max_per_minute
        self._window = window_seconds
        self._timestamps: Dict[str, list] = {}  # peer_id -> list of timestamps
        self._lock = threading.Lock()

    def is_allowed(self, peer_id: str) -> bool:
        """
        Check if a message from this peer is allowed.

        Args:
            peer_id: The sender's pubkey

        Returns:
            True if allowed, False if rate limited
        """
        now = time.time()
        cutoff = now - self._window

        with self._lock:
            # Get or create timestamp list for this peer
            if peer_id not in self._timestamps:
                self._timestamps[peer_id] = []

            # Remove old timestamps outside the window
            self._timestamps[peer_id] = [
                ts for ts in self._timestamps[peer_id] if ts > cutoff
            ]

            # Check if under limit
            if len(self._timestamps[peer_id]) >= self._max_messages:
                return False

            # Record this message
            self._timestamps[peer_id].append(now)
            return True

    def get_stats(self, peer_id: str = None) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        now = time.time()
        cutoff = now - self._window

        with self._lock:
            if peer_id:
                timestamps = self._timestamps.get(peer_id, [])
                recent = [ts for ts in timestamps if ts > cutoff]
                return {
                    "peer_id": peer_id,
                    "messages_in_window": len(recent),
                    "max_per_window": self._max_messages,
                    "window_seconds": self._window,
                }

            # Overall stats
            total_peers = len(self._timestamps)
            total_messages = sum(
                len([ts for ts in timestamps if ts > cutoff])
                for timestamps in self._timestamps.values()
            )
            return {
                "tracked_peers": total_peers,
                "total_messages_in_window": total_messages,
                "max_per_peer": self._max_messages,
                "window_seconds": self._window,
            }

    def cleanup(self) -> int:
        """Remove stale entries. Returns number of peers cleaned."""
        now = time.time()
        cutoff = now - self._window
        cleaned = 0

        with self._lock:
            stale_peers = [
                peer_id for peer_id, timestamps in self._timestamps.items()
                if not any(ts > cutoff for ts in timestamps)
            ]
            for peer_id in stale_peers:
                del self._timestamps[peer_id]
                cleaned += 1

        return cleaned


# Global rate limiter for PEER_AVAILABLE messages
peer_available_limiter: Optional[RateLimiter] = None


def _parse_bool(value: Any, default: bool = False) -> bool:
    """Parse a boolean-ish option value safely."""
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _check_permission(required_tier: str) -> Optional[Dict[str, Any]]:
    """
    Check if the local node has the required tier for an RPC command.

    Permission model (from IMPLEMENTATION_PLAN.md Section 8.5):
    - Admin Only: hive-genesis, hive-invite, hive-ban, hive-set-mode
    - Member Only: hive-vouch, hive-approve, hive-reject
    - Any Tier: hive-status, hive-members, hive-contribution, hive-topology

    Args:
        required_tier: 'member' (full member) or 'neophyte' (any member)

    Returns:
        None if permission granted, or error dict if denied
    """
    if not our_pubkey or not database:
        return {"error": "Not initialized"}

    member = database.get_member(our_pubkey)
    if not member:
        return {"error": "Not a Hive member", "required_tier": required_tier}

    current_tier = member.get('tier', 'neophyte')

    if required_tier == 'member':
        if current_tier != 'member':
            return {
                "error": "permission_denied",
                "message": "This command requires full member privileges",
                "current_tier": current_tier,
                "required_tier": "member"
            }
    # 'neophyte' tier means any member (including neophytes) can use the command

    return None  # Permission granted


def _get_hive_context() -> HiveContext:
    """
    Create a HiveContext with all current global dependencies.

    This bundles the global state for RPC command handlers in modules/rpc_commands.py.
    Note: Some globals may not be initialized yet if init() hasn't completed.
    """
    # These globals are always defined (may be None before init())
    _database = database if 'database' in globals() else None
    _config = config if 'config' in globals() else None
    _safe_plugin = safe_plugin if 'safe_plugin' in globals() else None
    _our_pubkey = our_pubkey if 'our_pubkey' in globals() else None
    _vpn_transport = vpn_transport if 'vpn_transport' in globals() else None
    _planner = planner if 'planner' in globals() else None
    _bridge = bridge if 'bridge' in globals() else None
    _intent_mgr = intent_mgr if 'intent_mgr' in globals() else None
    _membership_mgr = membership_mgr if 'membership_mgr' in globals() else None
    # coop_expansion is the global name, not coop_expansion_mgr
    _coop_expansion = coop_expansion if 'coop_expansion' in globals() else None
    _contribution_mgr = contribution_mgr if 'contribution_mgr' in globals() else None
    _routing_pool = routing_pool if 'routing_pool' in globals() else None
    _yield_metrics_mgr = yield_metrics_mgr if 'yield_metrics_mgr' in globals() else None
    _liquidity_coord = liquidity_coord if 'liquidity_coord' in globals() else None
    _fee_coordination_mgr = fee_coordination_mgr if 'fee_coordination_mgr' in globals() else None
    _cost_reduction_mgr = cost_reduction_mgr if 'cost_reduction_mgr' in globals() else None
    _rationalization_mgr = rationalization_mgr if 'rationalization_mgr' in globals() else None
    _strategic_positioning_mgr = strategic_positioning_mgr if 'strategic_positioning_mgr' in globals() else None

    # Create a log wrapper that calls plugin.log
    def _log(msg: str, level: str = 'info'):
        plugin.log(msg, level=level)

    return HiveContext(
        database=_database,
        config=_config,
        safe_plugin=_safe_plugin,
        our_pubkey=_our_pubkey,
        vpn_transport=_vpn_transport,
        planner=_planner,
        quality_scorer=None,  # Local to init(), not needed for current commands
        bridge=_bridge,
        intent_mgr=_intent_mgr,
        membership_mgr=_membership_mgr,
        coop_expansion_mgr=_coop_expansion,
        contribution_mgr=_contribution_mgr,
        routing_pool=_routing_pool,
        yield_metrics_mgr=_yield_metrics_mgr,
        liquidity_coordinator=_liquidity_coord,
        fee_coordination_mgr=_fee_coordination_mgr,
        cost_reduction_mgr=_cost_reduction_mgr,
        rationalization_mgr=_rationalization_mgr,
        strategic_positioning_mgr=_strategic_positioning_mgr,
        log=_log,
    )


# =============================================================================
# PLUGIN OPTIONS
# =============================================================================

# Database path is NOT dynamic (immutable after init)
plugin.add_option(
    name='hive-db-path',
    default='~/.lightning/cl_hive.db',
    description='Path to the SQLite database for Hive state (immutable)'
)

# All other options are dynamic (hot-reloadable via `lightning-cli setconfig`)
plugin.add_option(
    name='hive-governance-mode',
    default='advisor',
    description='Governance mode: advisor (AI/human approval), failsafe (emergency auto-execute)',
    dynamic=True
)

plugin.add_option(
    name='hive-neophyte-fee-discount',
    default='0.5',
    description='Fee discount for Neophyte members (0.5 = 50% of public rate)',
    dynamic=True
)

plugin.add_option(
    name='hive-member-fee-ppm',
    default='0',
    description='Fee charged to full Hive members (default: 0 = free)',
    dynamic=True
)

plugin.add_option(
    name='hive-probation-days',
    default='30',
    description='Minimum days as Neophyte before promotion eligibility',
    dynamic=True
)

plugin.add_option(
    name='hive-vouch-threshold',
    default='0.51',
    description='Percentage of member vouches required for promotion (0.51 = 51%)',
    dynamic=True
)

plugin.add_option(
    name='hive-min-vouch-count',
    default='3',
    description='Minimum number of vouches required for promotion',
    dynamic=True
)

plugin.add_option(
    name='hive-max-members',
    default='50',
    description='Maximum Hive members (Dunbar cap for gossip efficiency)',
    dynamic=True
)

plugin.add_option(
    name='hive-market-share-cap',
    default='0.20',
    description='Maximum market share per target (0.20 = 20%, anti-monopoly)',
    dynamic=True
)

plugin.add_option(
    name='hive-membership-enabled',
    default='true',
    description='Enable membership & promotion protocol (default: true)',
    dynamic=True
)

plugin.add_option(
    name='hive-auto-vouch',
    default='true',
    description='Auto-vouch for eligible neophytes (default: true)',
    dynamic=True
)

plugin.add_option(
    name='hive-auto-promote',
    default='true',
    description='Auto-promote when quorum reached (default: true)',
    dynamic=True
)

plugin.add_option(
    name='hive-ban-autotrigger',
    default='false',
    description='Auto-trigger ban proposal on sustained leeching (default: false)',
    dynamic=True
)

plugin.add_option(
    name='hive-intent-hold-seconds',
    default='60',
    description='Hold period before committing an Intent (conflict resolution)',
    dynamic=True
)

plugin.add_option(
    name='hive-gossip-threshold',
    default='0.10',
    description='Capacity change threshold to trigger gossip (0.10 = 10%)',
    dynamic=True
)

plugin.add_option(
    name='hive-heartbeat-interval',
    default='300',
    description='Heartbeat broadcast interval in seconds (default: 5 min)',
    dynamic=True
)

plugin.add_option(
    name='hive-planner-interval',
    default='3600',
    description='Planner cycle interval in seconds (default: 1 hour, minimum: 300)',
    dynamic=True
)

plugin.add_option(
    name='hive-planner-enable-expansions',
    default='false',
    description='Enable expansion proposals (new channel openings) in Planner',
    dynamic=True
)

plugin.add_option(
    name='hive-planner-min-channel-sats',
    default='1000000',
    description='Minimum channel size for expansion proposals (default: 1M sats)',
    dynamic=True
)

plugin.add_option(
    name='hive-planner-max-channel-sats',
    default='50000000',
    description='Maximum channel size for expansion proposals (default: 50M sats)',
    dynamic=True
)

plugin.add_option(
    name='hive-planner-default-channel-sats',
    default='5000000',
    description='Default channel size for expansion proposals (default: 5M sats)',
    dynamic=True
)

# Budget Options (Phase 7 - Governance)
plugin.add_option(
    name='hive-failsafe-budget-per-day',
    default='1000000',
    description='Daily budget for failsafe mode actions in sats (default: 1M)',
    dynamic=True
)

plugin.add_option(
    name='hive-budget-reserve-pct',
    default='0.20',
    description='Reserve percentage of onchain balance for future expansion (default: 20%)',
    dynamic=True
)

plugin.add_option(
    name='hive-budget-max-per-channel-pct',
    default='0.50',
    description='Maximum per-channel spend as percentage of daily budget (default: 50%)',
    dynamic=True
)

plugin.add_option(
    name='hive-max-expansion-feerate',
    default='5000',
    description='Max on-chain feerate (sat/kB) to allow expansion proposals (default: 5000 = ~1.25 sat/vB). Set to 0 to disable check.',
    dynamic=True
)

# VPN Transport Options (all dynamic)
plugin.add_option(
    name='hive-transport-mode',
    default='any',
    description='Hive transport mode: any, vpn-only, vpn-preferred',
    dynamic=True
)

plugin.add_option(
    name='hive-vpn-subnets',
    default='',
    description='VPN subnets for hive peers (CIDR, comma-separated). Example: 10.8.0.0/24',
    dynamic=True
)

plugin.add_option(
    name='hive-vpn-bind',
    default='',
    description='VPN bind address for hive traffic (ip:port)',
    dynamic=True
)

plugin.add_option(
    name='hive-vpn-peers',
    default='',
    description='VPN peer mappings (pubkey@ip:port, comma-separated)',
    dynamic=True
)

plugin.add_option(
    name='hive-vpn-required-messages',
    default='all',
    description='Message types requiring VPN: all, gossip, intent, sync, none',
    dynamic=True
)


# =============================================================================
# CONFIG RELOAD SUPPORT
# =============================================================================
# Note: CLN's setconfig command updates option values, but there's no
# notification mechanism for plugins. Use `hive-reload-config` RPC to
# sync the internal config object after using `lightning-cli setconfig`.

# Mapping from plugin option names to config attribute names and types
OPTION_TO_CONFIG_MAP: Dict[str, tuple] = {
    'hive-governance-mode': ('governance_mode', str),
    'hive-neophyte-fee-discount': ('neophyte_fee_discount_pct', float),
    'hive-member-fee-ppm': ('member_fee_ppm', int),
    'hive-probation-days': ('probation_days', int),
    'hive-max-members': ('max_members', int),
    'hive-market-share-cap': ('market_share_cap_pct', float),
    'hive-membership-enabled': ('membership_enabled', bool),
    'hive-auto-vouch': ('auto_vouch_enabled', bool),
    'hive-auto-promote': ('auto_promote_enabled', bool),
    'hive-ban-autotrigger': ('ban_autotrigger_enabled', bool),
    'hive-intent-hold-seconds': ('intent_hold_seconds', int),
    'hive-gossip-threshold': ('gossip_threshold_pct', float),
    'hive-heartbeat-interval': ('heartbeat_interval', int),
    'hive-planner-interval': ('planner_interval', int),
    'hive-planner-enable-expansions': ('planner_enable_expansions', bool),
    'hive-planner-min-channel-sats': ('planner_min_channel_sats', int),
    'hive-planner-max-channel-sats': ('planner_max_channel_sats', int),
    'hive-planner-default-channel-sats': ('planner_default_channel_sats', int),
    # Budget options (failsafe mode)
    'hive-failsafe-budget-per-day': ('failsafe_budget_per_day', int),
    'hive-budget-reserve-pct': ('budget_reserve_pct', float),
    'hive-budget-max-per-channel-pct': ('budget_max_per_channel_pct', float),
    # Feerate gate
    'hive-max-expansion-feerate': ('max_expansion_feerate_perkb', int),
}

# VPN options require special handling (reconfigure VPN transport)
VPN_OPTIONS = {
    'hive-transport-mode',
    'hive-vpn-subnets',
    'hive-vpn-bind',
    'hive-vpn-peers',
    'hive-vpn-required-messages',
}


def _parse_setconfig_value(value: Any, target_type: type) -> Any:
    """Parse a setconfig value to the target type."""
    if target_type == bool:
        if isinstance(value, bool):
            return value
        return str(value).lower() in ('true', '1', 'yes', 'on')
    elif target_type == int:
        return int(value)
    elif target_type == float:
        return float(value)
    else:
        return str(value)


def _reload_config_from_cln(plugin_obj: Plugin) -> Dict[str, Any]:
    """
    Reload all hive config options from CLN's current values.

    Call this after using `lightning-cli setconfig` to sync the internal
    config object with CLN's option values.

    Returns dict with list of updated options and any errors.
    """
    global config, vpn_transport

    results = {"updated": [], "errors": [], "vpn_reconfigured": False}

    # Reload standard config options
    for option_name, (attr_name, attr_type) in OPTION_TO_CONFIG_MAP.items():
        try:
            val = plugin_obj.get_option(option_name)
            if val is None:
                continue

            parsed_value = _parse_setconfig_value(val, attr_type)
            old_value = getattr(config, attr_name, None)

            if old_value != parsed_value:
                setattr(config, attr_name, parsed_value)
                results["updated"].append({
                    "option": option_name,
                    "attr": attr_name,
                    "old": old_value,
                    "new": parsed_value
                })

        except (ValueError, TypeError) as e:
            results["errors"].append({"option": option_name, "error": str(e)})

    # Increment config version if anything changed
    if results["updated"]:
        config._version += 1

        # Validate the new config
        validation_error = config.validate()
        if validation_error:
            results["errors"].append({"validation": validation_error})

    # Reload VPN options if VPN transport is active
    if vpn_transport is not None:
        try:
            vpn_result = vpn_transport.configure(
                mode=plugin_obj.get_option('hive-transport-mode'),
                vpn_subnets=plugin_obj.get_option('hive-vpn-subnets'),
                vpn_bind=plugin_obj.get_option('hive-vpn-bind'),
                vpn_peers=plugin_obj.get_option('hive-vpn-peers'),
                required_messages=plugin_obj.get_option('hive-vpn-required-messages')
            )
            results["vpn_reconfigured"] = True
            results["vpn_mode"] = vpn_result.get('mode', 'unknown')
        except Exception as e:
            results["errors"].append({"vpn": str(e)})

    return results


# =============================================================================
# INITIALIZATION
# =============================================================================

@plugin.init()
def init(options: Dict[str, Any], configuration: Dict[str, Any], plugin: Plugin, **kwargs):
    """
    Initialize the cl-hive plugin.
    
    Steps:
    1. Parse and validate options
    2. Initialize database
    3. Create thread-safe plugin proxy
    4. Initialize handshake manager
    5. Verify cl-revenue-ops dependency
    6. Set up signal handlers for graceful shutdown
    """
    global database, config, safe_plugin, handshake_mgr, state_manager, gossip_mgr, intent_mgr, our_pubkey, bridge, vpn_transport
    
    plugin.log("cl-hive: Initializing Swarm Intelligence layer...")
    
    # Create thread-safe plugin proxy
    safe_plugin = ThreadSafePluginProxy(plugin)
    
    # Build configuration from options
    config = HiveConfig(
        db_path=options.get('hive-db-path', '~/.lightning/cl_hive.db'),
        governance_mode=options.get('hive-governance-mode', 'advisor'),
        membership_enabled=_parse_bool(options.get('hive-membership-enabled', 'true')),
        auto_vouch_enabled=_parse_bool(options.get('hive-auto-vouch', 'true')),
        auto_promote_enabled=_parse_bool(options.get('hive-auto-promote', 'true')),
        ban_autotrigger_enabled=_parse_bool(options.get('hive-ban-autotrigger', 'false')),
        neophyte_fee_discount_pct=float(options.get('hive-neophyte-fee-discount', '0.5')),
        member_fee_ppm=int(options.get('hive-member-fee-ppm', '0')),
        probation_days=int(options.get('hive-probation-days', '90')),
        max_members=int(options.get('hive-max-members', '50')),
        market_share_cap_pct=float(options.get('hive-market-share-cap', '0.20')),
        intent_hold_seconds=int(options.get('hive-intent-hold-seconds', '60')),
        gossip_threshold_pct=float(options.get('hive-gossip-threshold', '0.10')),
        heartbeat_interval=int(options.get('hive-heartbeat-interval', '300')),
        planner_interval=int(options.get('hive-planner-interval', '3600')),
        planner_enable_expansions=_parse_bool(options.get('hive-planner-enable-expansions', 'false')),
        planner_min_channel_sats=int(options.get('hive-planner-min-channel-sats', '1000000')),
        planner_max_channel_sats=int(options.get('hive-planner-max-channel-sats', '50000000')),
        planner_default_channel_sats=int(options.get('hive-planner-default-channel-sats', '5000000')),
        # Budget options (failsafe mode)
        failsafe_budget_per_day=int(options.get('hive-failsafe-budget-per-day', '1000000')),
        budget_reserve_pct=float(options.get('hive-budget-reserve-pct', '0.20')),
        budget_max_per_channel_pct=float(options.get('hive-budget-max-per-channel-pct', '0.50')),
        max_expansion_feerate_perkb=int(options.get('hive-max-expansion-feerate', '5000')),
    )
    
    # Initialize database
    database = HiveDatabase(config.db_path, safe_plugin)
    database.initialize()
    plugin.log(f"cl-hive: Database initialized at {config.db_path}")
    
    # Initialize handshake manager
    handshake_mgr = HandshakeManager(
        safe_plugin.rpc, database, safe_plugin
    )
    plugin.log("cl-hive: Handshake manager initialized")
    
    # Initialize state manager (Phase 2)
    state_manager = StateManager(database, safe_plugin)
    state_manager.load_from_database()
    plugin.log(f"cl-hive: State manager initialized ({len(state_manager.get_all_peer_states())} peers cached)")
    
    # Initialize gossip manager (Phase 2)
    gossip_mgr = GossipManager(
        state_manager, 
        safe_plugin, 
        heartbeat_interval=config.heartbeat_interval
    )
    plugin.log("cl-hive: Gossip manager initialized")
    
    # Initialize intent manager (Phase 3)
    # Get our pubkey for tie-breaker logic
    our_pubkey = safe_plugin.rpc.getinfo()['id']
    intent_mgr = IntentManager(
        database,
        safe_plugin,
        our_pubkey=our_pubkey,
        hold_seconds=config.intent_hold_seconds
    )
    plugin.log("cl-hive: Intent manager initialized")
    
    # Start background threads (Phase 3)
    intent_thread = threading.Thread(
        target=intent_monitor_loop,
        name="cl-hive-intent-monitor",
        daemon=True
    )
    intent_thread.start()
    plugin.log("cl-hive: Intent monitor thread started")
    
    # Initialize Integration Bridge (Phase 4)
    # Uses Circuit Breaker pattern for resilient cl-revenue-ops integration
    bridge = Bridge(safe_plugin.rpc, safe_plugin)
    bridge_status = bridge.initialize()
    
    if bridge_status == BridgeStatus.ENABLED:
        plugin.log(f"cl-hive: Bridge ENABLED - cl-revenue-ops {bridge._revenue_ops_version}")
        if bridge._clboss_available:
            plugin.log("cl-hive: CLBoss detected - saturation control via Gateway Pattern")
        else:
            plugin.log("cl-hive: CLBoss not detected (optional) - using native expansion control")
    elif bridge_status == BridgeStatus.DEGRADED:
        plugin.log("cl-hive: Bridge DEGRADED - some features unavailable", level='warn')
    else:
        plugin.log(
            "cl-hive: Bridge DISABLED - cl-revenue-ops not detected or incompatible. "
            "Hive policy integration will be unavailable. Recommended: v1.4.0+",
            level='warn'
        )

    # Initialize contribution and membership managers (Phase 5)
    global contribution_mgr, membership_mgr
    contribution_mgr = ContributionManager(safe_plugin.rpc, database, safe_plugin, config)
    membership_mgr = MembershipManager(
        database,
        state_manager,
        contribution_mgr,
        bridge,
        config,
        safe_plugin
    )
    plugin.log("cl-hive: Membership and contribution managers initialized")

    # Start membership maintenance thread (Phase 5)
    membership_thread = threading.Thread(
        target=membership_maintenance_loop,
        name="cl-hive-membership-maintenance",
        daemon=True
    )
    membership_thread.start()
    plugin.log("cl-hive: Membership maintenance thread started")

    # Initialize DecisionEngine (Phase 7)
    global decision_engine
    decision_engine = DecisionEngine(database=database, plugin=safe_plugin)
    plugin.log("cl-hive: DecisionEngine initialized")

    # Initialize VPN Transport Manager
    vpn_transport = VPNTransportManager(plugin=safe_plugin)
    vpn_result = vpn_transport.configure(
        mode=options.get('hive-transport-mode', 'any'),
        vpn_subnets=options.get('hive-vpn-subnets', ''),
        vpn_bind=options.get('hive-vpn-bind', ''),
        vpn_peers=options.get('hive-vpn-peers', ''),
        required_messages=options.get('hive-vpn-required-messages', 'all')
    )
    if vpn_transport.is_enabled():
        plugin.log(f"cl-hive: VPN transport ENABLED - mode={vpn_result['mode']}, subnets={len(vpn_result['subnets'])}")
    else:
        plugin.log("cl-hive: VPN transport configured (mode=any, not enforcing)")

    # Initialize Planner (Phase 6)
    global planner, clboss_bridge
    clboss_bridge = CLBossBridge(safe_plugin.rpc, safe_plugin)
    planner = Planner(
        state_manager=state_manager,
        database=database,
        bridge=bridge,
        clboss_bridge=clboss_bridge,
        plugin=safe_plugin,
        intent_manager=intent_mgr,
        decision_engine=decision_engine
    )
    plugin.log("cl-hive: Planner initialized")

    # Start planner loop thread (Phase 6)
    planner_thread = threading.Thread(
        target=planner_loop,
        name="cl-hive-planner",
        daemon=True
    )
    planner_thread.start()
    plugin.log("cl-hive: Planner thread started")

    # Initialize Cooperative Expansion Manager (Phase 6.4)
    global coop_expansion
    quality_scorer = PeerQualityScorer(database, safe_plugin)
    coop_expansion = CooperativeExpansionManager(
        database=database,
        quality_scorer=quality_scorer,
        plugin=safe_plugin,
        our_id=our_pubkey,
        config_getter=lambda: config  # Provides access to budget settings
    )
    plugin.log("cl-hive: Cooperative expansion manager initialized")

    # Initialize Fee Intelligence Manager (Phase 7 - Cooperative Fee Coordination)
    global fee_intel_mgr
    fee_intel_mgr = FeeIntelligenceManager(
        database=database,
        plugin=safe_plugin,
        our_pubkey=our_pubkey
    )
    plugin.log("cl-hive: Fee intelligence manager initialized")

    # Initialize Health Score Aggregator (Phase 7 - NNLB)
    global health_aggregator
    health_aggregator = HealthScoreAggregator(
        database=database,
        plugin=safe_plugin
    )
    plugin.log("cl-hive: Health aggregator initialized")

    # Start fee intelligence background thread (Phase 7)
    fee_intel_thread = threading.Thread(
        target=fee_intelligence_loop,
        name="cl-hive-fee-intelligence",
        daemon=True
    )
    fee_intel_thread.start()
    plugin.log("cl-hive: Fee intelligence thread started")

    # Initialize Liquidity Coordinator (Phase 7.3 - Cooperative Rebalancing)
    global liquidity_coord
    liquidity_coord = LiquidityCoordinator(
        database=database,
        plugin=safe_plugin,
        our_pubkey=our_pubkey,
        fee_intel_mgr=fee_intel_mgr,
        state_manager=state_manager
    )
    plugin.log("cl-hive: Liquidity coordinator initialized")

    # Initialize Splice Coordinator (Phase 3 - Splice Coordination)
    global splice_coord
    splice_coord = SpliceCoordinator(
        database=database,
        plugin=safe_plugin,
        state_manager=state_manager
    )
    plugin.log("cl-hive: Splice coordinator initialized")

    # Link cooperation modules to Planner (Phase 7 - Cooperation Module Synergies)
    # These modules were initialized after the planner, so we set them via setter
    planner.set_cooperation_modules(
        liquidity_coordinator=liquidity_coord,
        splice_coordinator=splice_coord,
        health_aggregator=health_aggregator
    )
    plugin.log("cl-hive: Planner linked to cooperation modules")

    # Initialize Routing Map (Phase 7.4 - Routing Intelligence)
    global routing_map
    routing_map = HiveRoutingMap(
        database=database,
        plugin=safe_plugin,
        our_pubkey=our_pubkey
    )
    # Load existing probes from database
    routing_map.aggregate_from_database()
    plugin.log("cl-hive: Routing map initialized")

    # Initialize Peer Reputation Manager (Phase 5 - Advanced Cooperation)
    global peer_reputation_mgr
    peer_reputation_mgr = PeerReputationManager(
        database=database,
        plugin=safe_plugin,
        our_pubkey=our_pubkey
    )
    # Load existing reputation data from database
    peer_reputation_mgr.aggregate_from_database()
    plugin.log("cl-hive: Peer reputation manager initialized")

    # Initialize Routing Pool (Phase 0 - Collective Economics)
    global routing_pool
    routing_pool = RoutingPool(
        database=database,
        plugin=safe_plugin,
        state_manager=state_manager
    )
    routing_pool.set_our_pubkey(our_pubkey)
    plugin.log("cl-hive: Routing pool initialized (collective economics)")

    # Initialize Settlement Manager (BOLT12 revenue distribution)
    global settlement_mgr
    settlement_mgr = SettlementManager(
        database=database,
        plugin=safe_plugin,
        rpc=safe_plugin.rpc
    )
    settlement_mgr.initialize_tables()
    plugin.log("cl-hive: Settlement manager initialized (BOLT12 payouts)")

    # Initialize Yield Metrics Manager (Phase 1 - Metrics & Measurement)
    global yield_metrics_mgr
    yield_metrics_mgr = YieldMetricsManager(
        database=database,
        plugin=safe_plugin,
        state_manager=state_manager
    )
    yield_metrics_mgr.set_our_pubkey(our_pubkey)
    plugin.log("cl-hive: Yield metrics manager initialized (Phase 1)")

    # Initialize Fee Coordination Manager (Phase 2 - Fee Coordination)
    global fee_coordination_mgr
    fee_coordination_mgr = FeeCoordinationManager(
        database=database,
        plugin=safe_plugin,
        state_manager=state_manager,
        liquidity_coordinator=liquidity_coord,
        gossip_mgr=gossip_mgr
    )
    fee_coordination_mgr.set_our_pubkey(our_pubkey)
    plugin.log("cl-hive: Fee coordination manager initialized (Phase 2)")

    # Initialize Cost Reduction Manager (Phase 3 - Cost Reduction)
    global cost_reduction_mgr
    cost_reduction_mgr = CostReductionManager(
        plugin=safe_plugin,
        database=database,
        state_manager=state_manager,
        yield_metrics_mgr=yield_metrics_mgr,
        liquidity_coordinator=liquidity_coord
    )
    cost_reduction_mgr.set_our_pubkey(our_pubkey)
    plugin.log("cl-hive: Cost reduction manager initialized (Phase 3)")

    # Initialize Rationalization Manager (Channel Rationalization)
    global rationalization_mgr
    rationalization_mgr = RationalizationManager(
        plugin=safe_plugin,
        database=database,
        state_manager=state_manager,
        fee_coordination_mgr=fee_coordination_mgr,
        governance=decision_engine
    )
    rationalization_mgr.set_our_pubkey(our_pubkey)
    plugin.log("cl-hive: Rationalization manager initialized")

    # Wire rationalization manager to cooperative expansion (slime mold coordination)
    if coop_expansion:
        coop_expansion.set_rationalization_manager(rationalization_mgr)
        plugin.log("cl-hive: Cooperative expansion linked to rationalization (redundancy checks enabled)")

    # Initialize Strategic Positioning Manager (Phase 5 - Strategic Positioning)
    global strategic_positioning_mgr
    strategic_positioning_mgr = StrategicPositioningManager(
        plugin=safe_plugin,
        database=database,
        state_manager=state_manager,
        fee_coordination_mgr=fee_coordination_mgr,
        yield_metrics_mgr=yield_metrics_mgr,
        planner=planner
    )
    strategic_positioning_mgr.set_our_pubkey(our_pubkey)
    plugin.log("cl-hive: Strategic positioning manager initialized (Phase 5)")

    # Initialize Anticipatory Liquidity Manager (Phase 7.1 - Anticipatory Liquidity)
    global anticipatory_liquidity_mgr
    anticipatory_liquidity_mgr = AnticipatoryLiquidityManager(
        database=database,
        plugin=safe_plugin,
        state_manager=state_manager,
        our_id=our_pubkey
    )
    plugin.log("cl-hive: Anticipatory liquidity manager initialized (Phase 7.1)")

    # Link anticipatory manager to fee coordination for time-based fees (Phase 7.4)
    if fee_coordination_mgr:
        fee_coordination_mgr.set_anticipatory_manager(anticipatory_liquidity_mgr)
        plugin.log("cl-hive: Time-based fee adjustment enabled (Phase 7.4)")

    # Link yield optimization modules to Planner (Slime mold coordination)
    # These enable the planner to avoid redundant opens and prioritize high-value corridors
    planner.set_cooperation_modules(
        rationalization_mgr=rationalization_mgr,
        strategic_positioning_mgr=strategic_positioning_mgr
    )
    plugin.log("cl-hive: Planner linked to yield optimization modules (slime mold mode)")

    # Initialize rate limiter for PEER_AVAILABLE messages (Security Enhancement)
    global peer_available_limiter
    peer_available_limiter = RateLimiter(max_per_minute=10, window_seconds=60)
    plugin.log("cl-hive: Rate limiter initialized (10 msg/min per peer)")

    # Sync fee policies for existing members (Phase 4 integration)
    if bridge and bridge.status == BridgeStatus.ENABLED:
        _sync_member_policies(plugin)

    # Broadcast membership to peers for consistency (Phase 5 enhancement)
    _sync_membership_on_startup(plugin)

    # Set up graceful shutdown handler
    def handle_shutdown_signal(signum, frame):
        plugin.log("cl-hive: Received shutdown signal, cleaning up...")
        shutdown_event.set()
    
    try:
        signal.signal(signal.SIGTERM, handle_shutdown_signal)
        signal.signal(signal.SIGINT, handle_shutdown_signal)
    except Exception as e:
        plugin.log(f"cl-hive: Could not set signal handlers: {e}", level='debug')
    
    plugin.log("cl-hive: Initialization complete. Swarm Intelligence ready.")


# =============================================================================
# PEER CONNECTED HOOK (Autodiscovery)
# =============================================================================

@plugin.hook("peer_connected")
def on_peer_connected(peer: dict, plugin: Plugin, **kwargs):
    """
    Handle peer connection - trigger autodiscovery if enabled.

    When a peer connects and we're not a hive member yet, send HIVE_HELLO
    to discover if they're part of a hive we can join.
    """
    global config, database, handshake_mgr

    # Extract peer_id from the peer dict
    peer_id = peer.get("id") if isinstance(peer, dict) else None
    if not peer_id:
        return {"result": "continue"}

    # Check if auto-join is enabled
    if not config or not config.auto_join_enabled:
        return {"result": "continue"}

    # Check if we're already a member
    if not handshake_mgr or not database:
        return {"result": "continue"}

    our_pubkey = handshake_mgr.get_our_pubkey()
    our_member = database.get_member(our_pubkey)

    # If we're already a member, no need to autodiscover
    if our_member:
        return {"result": "continue"}

    # Check if this peer is already known to us as a member
    peer_member = database.get_member(peer_id)
    if peer_member:
        # Peer is known, but we're not a member - this shouldn't happen normally
        return {"result": "continue"}

    # Send HIVE_HELLO to discover if peer is a hive member
    try:
        from modules.protocol import create_hello
        hello_msg = create_hello(our_pubkey)

        safe_plugin.rpc.call("sendcustommsg", {
            "node_id": peer_id,
            "msg": hello_msg.hex()
        })
        plugin.log(f"cl-hive: Sent HELLO to {peer_id[:16]}... (autodiscovery)")
    except Exception as e:
        plugin.log(f"cl-hive: Failed to send autodiscovery HELLO: {e}", level='debug')

    return {"result": "continue"}


# =============================================================================
# CUSTOM MESSAGE HOOK (BOLT 8 Protocol Layer)
# =============================================================================

@plugin.hook("custommsg")
def on_custommsg(peer_id: str, payload: str, plugin: Plugin, **kwargs):
    """
    Handle incoming custom BOLT 8 messages.
    
    Security: Implements "Peek & Check" pattern.
    - Read first 4 bytes of payload
    - If != HIVE_MAGIC (0x48495645), return continue immediately
    - Only process messages with valid Hive magic prefix
    
    This ensures cl-hive coexists peacefully with other plugins
    using the experimental message range (32768+).
    """
    if not database or not handshake_mgr:
        return {"result": "continue"}
    
    # Reject oversized payloads before hex decode
    if len(payload) > MAX_MESSAGE_BYTES * 2:
        return {"result": "continue"}

    # Decode hex payload to bytes
    try:
        data = bytes.fromhex(payload)
    except ValueError:
        return {"result": "continue"}
    
    # SECURITY: Peek & Check - Fast rejection of non-Hive messages
    if not is_hive_message(data):
        # Not our message, let other plugins handle it
        return {"result": "continue"}
    
    # Deserialize the Hive message
    msg_type, msg_payload = deserialize(data)
    
    if msg_type is None:
        # Malformed Hive message (magic matched but parse failed)
        plugin.log(f"cl-hive: Malformed message from {peer_id[:16]}...", level='warn')
        return {"result": "continue"}

    # VPN Transport Policy Check
    if vpn_transport and vpn_transport.is_enabled():
        accept, reason = vpn_transport.should_accept_hive_message(
            peer_id=peer_id,
            message_type=msg_type.name if msg_type else ""
        )
        if not accept:
            plugin.log(
                f"cl-hive: VPN policy rejected {msg_type.name} from {peer_id[:16]}...: {reason}",
                level='info'
            )
            return {"result": "continue"}

    # Dispatch based on message type
    try:
        if msg_type == HiveMessageType.HELLO:
            return handle_hello(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.CHALLENGE:
            return handle_challenge(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.ATTEST:
            return handle_attest(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.WELCOME:
            return handle_welcome(peer_id, msg_payload, plugin)
        # Phase 2: State Management
        elif msg_type == HiveMessageType.GOSSIP:
            return handle_gossip(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.STATE_HASH:
            return handle_state_hash(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.FULL_SYNC:
            return handle_full_sync(peer_id, msg_payload, plugin)
        # Phase 3: Intent Lock Protocol
        elif msg_type == HiveMessageType.INTENT:
            return handle_intent(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.INTENT_ABORT:
            return handle_intent_abort(peer_id, msg_payload, plugin)
        # Phase 5: Membership Promotion
        elif msg_type == HiveMessageType.PROMOTION_REQUEST:
            return handle_promotion_request(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.VOUCH:
            return handle_vouch(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.PROMOTION:
            return handle_promotion(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.MEMBER_LEFT:
            return handle_member_left(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.BAN_PROPOSAL:
            return handle_ban_proposal(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.BAN_VOTE:
            return handle_ban_vote(peer_id, msg_payload, plugin)
        # Phase 6: Channel Coordination
        elif msg_type == HiveMessageType.PEER_AVAILABLE:
            return handle_peer_available(peer_id, msg_payload, plugin)
        # Phase 6.4: Cooperative Expansion
        elif msg_type == HiveMessageType.EXPANSION_NOMINATE:
            return handle_expansion_nominate(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.EXPANSION_ELECT:
            return handle_expansion_elect(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.EXPANSION_DECLINE:
            return handle_expansion_decline(peer_id, msg_payload, plugin)
        # Phase 7: Cooperative Fee Coordination
        elif msg_type == HiveMessageType.FEE_INTELLIGENCE:
            return handle_fee_intelligence(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.HEALTH_REPORT:
            return handle_health_report(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.LIQUIDITY_NEED:
            return handle_liquidity_need(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.ROUTE_PROBE:
            return handle_route_probe(peer_id, msg_payload, plugin)
        elif msg_type == HiveMessageType.PEER_REPUTATION:
            return handle_peer_reputation(peer_id, msg_payload, plugin)
        # Phase 9: Settlement
        elif msg_type == HiveMessageType.SETTLEMENT_OFFER:
            return handle_settlement_offer(peer_id, msg_payload, plugin)
        else:
            # Known but unimplemented message type
            plugin.log(f"cl-hive: Unhandled message type {msg_type.name} from {peer_id[:16]}...", level='debug')
            return {"result": "continue"}
            
    except Exception as e:
        plugin.log(f"cl-hive: Error handling {msg_type.name}: {e}", level='warn')
        return {"result": "continue"}


def handle_hello(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_HELLO message (autodiscovery join request).

    A node is requesting to join the hive. Channel existence serves as
    proof of stake - no ticket required.

    Flow:
    1. Check if we're a hive member (only members can accept new nodes)
    2. Check if peer has a channel with us (proof of stake)
    3. Check if peer is already a member
    4. Send CHALLENGE if all conditions met
    """
    sender_pubkey = payload.get('pubkey')
    if not sender_pubkey:
        plugin.log(f"cl-hive: HELLO from {peer_id[:16]}... missing pubkey", level='warn')
        return {"result": "continue"}

    # Verify pubkey matches peer_id (identity binding)
    if sender_pubkey != peer_id:
        plugin.log(f"cl-hive: HELLO from {peer_id[:16]}... pubkey mismatch", level='warn')
        return {"result": "continue"}

    # Check if we're a member (only members can accept new nodes)
    our_pubkey = handshake_mgr.get_our_pubkey()
    our_member = database.get_member(our_pubkey)
    if not our_member or our_member.get('tier') != 'member':
        plugin.log(f"cl-hive: HELLO from {peer_id[:16]}... but we're not a member", level='debug')
        return {"result": "continue"}

    # Check if peer is already a member
    existing_member = database.get_member(peer_id)
    if existing_member:
        plugin.log(f"cl-hive: HELLO from {peer_id[:16]}... already a {existing_member.get('tier')}", level='debug')
        return {"result": "continue"}

    # Check if peer has a channel with us (proof of stake)
    try:
        channels = safe_plugin.rpc.call("listpeerchannels", {"id": peer_id})
        peer_channels = channels.get('channels', [])
        # Look for any active channel
        has_channel = any(
            ch.get('state') in ('CHANNELD_NORMAL', 'CHANNELD_AWAITING_LOCKIN')
            for ch in peer_channels
        )
        if not has_channel:
            plugin.log(f"cl-hive: HELLO from {peer_id[:16]}... no channel (proof of stake required)", level='debug')
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: HELLO from {peer_id[:16]}... channel check failed: {e}", level='warn')
        return {"result": "continue"}

    # All checks passed - generate challenge
    # No requirements for autodiscovery join, tier is always neophyte
    nonce = handshake_mgr.generate_challenge(peer_id, requirements=0, initial_tier='neophyte')

    # Get Hive ID from metadata
    members = database.get_all_members()
    hive_id = "hive"
    for m in members:
        if m.get('metadata'):
            try:
                metadata = json.loads(m['metadata'])
                hive_id = metadata.get('hive_id', 'hive')
                break
            except (json.JSONDecodeError, TypeError):
                continue

    # Send CHALLENGE response
    challenge_msg = create_challenge(nonce, hive_id)

    try:
        safe_plugin.rpc.call("sendcustommsg", {
            "node_id": peer_id,
            "msg": challenge_msg.hex()
        })
        plugin.log(f"cl-hive: Sent CHALLENGE to {peer_id[:16]}... (autodiscovery join)")
    except Exception as e:
        plugin.log(f"cl-hive: Failed to send CHALLENGE: {e}", level='warn')

    return {"result": "continue"}


def handle_challenge(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_CHALLENGE message (nonce received).
    
    We received a challenge nonce - create and send attestation.
    """
    nonce = payload.get('nonce')
    hive_id = payload.get('hive_id')
    
    if not nonce:
        plugin.log(f"cl-hive: CHALLENGE from {peer_id[:16]}... missing nonce", level='warn')
        return {"result": "continue"}
    
    # Create attestation manifest
    try:
        attest_data = handshake_mgr.create_manifest(nonce)
        
        # Build ATTEST message
        from modules.protocol import create_attest
        attest_msg = create_attest(
            pubkey=attest_data['manifest']['pubkey'],
            version=attest_data['manifest']['version'],
            features=attest_data['manifest']['features'],
            nonce_signature=attest_data['nonce_signature'],
            manifest_signature=attest_data['manifest_signature'],
            manifest=attest_data['manifest']
        )
        
        safe_plugin.rpc.call("sendcustommsg", {
            "node_id": peer_id,
            "msg": attest_msg.hex()
        })
        plugin.log(f"cl-hive: Sent ATTEST to {peer_id[:16]}...")
        
    except Exception as e:
        plugin.log(f"cl-hive: Failed to create/send ATTEST: {e}", level='warn')
    
    return {"result": "continue"}


def handle_attest(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_ATTEST message (manifest verification).
    
    Verify the candidate's attestation and send WELCOME if valid.
    """
    # Get the challenge we sent
    pending = handshake_mgr.get_pending_challenge(peer_id)
    if not pending:
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... but no pending challenge", level='warn')
        return {"result": "continue"}

    now = int(time.time())
    if now - pending["issued_at"] > CHALLENGE_TTL_SECONDS:
        handshake_mgr.clear_challenge(peer_id)
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... challenge expired", level='warn')
        return {"result": "continue"}

    expected_nonce = pending["nonce"]
    
    manifest_data = payload.get('manifest')
    if not isinstance(manifest_data, dict):
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... missing manifest", level='warn')
        handshake_mgr.clear_challenge(peer_id)
        return {"result": "continue"}

    required_fields = ["pubkey", "version", "features", "timestamp", "nonce"]
    for field in required_fields:
        if field not in manifest_data:
            plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... missing {field}", level='warn')
            handshake_mgr.clear_challenge(peer_id)
            return {"result": "continue"}

    if payload.get('pubkey') and payload.get('pubkey') != manifest_data.get('pubkey'):
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... pubkey mismatch", level='warn')
        handshake_mgr.clear_challenge(peer_id)
        return {"result": "continue"}
    if payload.get('version') and payload.get('version') != manifest_data.get('version'):
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... version mismatch", level='warn')
        handshake_mgr.clear_challenge(peer_id)
        return {"result": "continue"}
    if payload.get('features') and payload.get('features') != manifest_data.get('features'):
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... features mismatch", level='warn')
        handshake_mgr.clear_challenge(peer_id)
        return {"result": "continue"}

    if manifest_data.get('pubkey') != peer_id:
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... pubkey not bound to peer", level='warn')
        handshake_mgr.clear_challenge(peer_id)
        return {"result": "continue"}

    if not isinstance(manifest_data.get('features'), list):
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... invalid features", level='warn')
        handshake_mgr.clear_challenge(peer_id)
        return {"result": "continue"}
    
    nonce_sig = payload.get('nonce_signature')
    manifest_sig = payload.get('manifest_signature')
    
    if not nonce_sig or not manifest_sig:
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... missing signatures", level='warn')
        return {"result": "continue"}
    
    # Verify manifest
    is_valid, error = handshake_mgr.verify_manifest(
        manifest_data, nonce_sig, manifest_sig, expected_nonce
    )
    
    if not is_valid:
        plugin.log(f"cl-hive: Invalid ATTEST from {peer_id[:16]}...: {error}", level='warn')
        handshake_mgr.clear_challenge(peer_id)
        return {"result": "continue"}
    
    satisfied, missing = handshake_mgr.check_requirements(
        pending["requirements"], manifest_data.get("features", [])
    )
    if not satisfied:
        plugin.log(
            f"cl-hive: ATTEST from {peer_id[:16]}... missing requirements: {missing}",
            level='warn'
        )
        handshake_mgr.clear_challenge(peer_id)
        return {"result": "continue"}

    # Get initial tier from pending challenge (always neophyte for autodiscovery)
    initial_tier = pending.get('initial_tier', 'neophyte')

    # Verification passed! Add member as neophyte
    database.add_member(
        peer_id=peer_id,
        tier=initial_tier,
        joined_at=int(time.time())
    )

    handshake_mgr.clear_challenge(peer_id)

    # Set hive fee policy for new member (0 fee to all hive members)
    if bridge and bridge.status == BridgeStatus.ENABLED:
        bridge.set_hive_policy(peer_id, is_member=True)

    # Get Hive info for WELCOME
    members = database.get_all_members()
    hive_id = "hive"
    for m in members:
        if m.get('metadata'):
            try:
                metadata = json.loads(m['metadata'])
                hive_id = metadata.get('hive_id', 'hive')
                break
            except (json.JSONDecodeError, TypeError):
                continue

    # Calculate real state hash via StateManager
    if state_manager:
        state_hash = state_manager.calculate_fleet_hash()
    else:
        state_hash = "0" * 64

    # Send WELCOME with actual tier
    welcome_msg = create_welcome(hive_id, initial_tier, len(members), state_hash)

    try:
        safe_plugin.rpc.call("sendcustommsg", {
            "node_id": peer_id,
            "msg": welcome_msg.hex()
        })
        plugin.log(f"cl-hive: Sent WELCOME to {peer_id[:16]}... (new {initial_tier})")
    except Exception as e:
        plugin.log(f"cl-hive: Failed to send WELCOME: {e}", level='warn')

    # Broadcast membership update to all existing members
    _broadcast_full_sync_to_members(plugin)

    return {"result": "continue"}


def handle_welcome(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_WELCOME message (session established).

    We've been accepted into the Hive!
    """
    hive_id = payload.get('hive_id')
    tier = payload.get('tier')
    member_count = payload.get('member_count')

    plugin.log(
        f"cl-hive: WELCOME received! Joined '{hive_id}' as {tier} "
        f"(Hive has {member_count} members)"
    )

    # Phase 4: Apply Hive fee policy to this peer
    if bridge and bridge.status == BridgeStatus.ENABLED:
        bridge.set_hive_policy(peer_id, is_member=True)
        # Also tell CLBoss about this peer (Gateway Pattern)
        if bridge._clboss_available:
            bridge.ignore_peer(peer_id)

    # Store Hive membership info for ourselves
    if database and our_pubkey:
        now = int(time.time())
        # Add ourselves as a member with the tier assigned by the admin
        database.add_member(our_pubkey, tier=tier or 'neophyte', joined_at=now)
        # Store hive_id in metadata
        database.update_member(our_pubkey, metadata=json.dumps({"hive_id": hive_id}))
        plugin.log(f"cl-hive: Stored membership (tier={tier}, hive_id={hive_id})")

        # Also add the peer that welcomed us (they're an existing member)
        database.add_member(peer_id, tier='member', joined_at=now)

        # Auto-generate and register BOLT12 offer for settlement
        if settlement_mgr:
            offer_result = settlement_mgr.generate_and_register_offer(our_pubkey)
            if "error" in offer_result:
                plugin.log(f"cl-hive: Failed to auto-register settlement offer: {offer_result['error']}", level='warn')
            else:
                plugin.log(f"cl-hive: Settlement offer auto-registered: {offer_result.get('status')}")
                # Broadcast to hive members
                bolt12_offer = settlement_mgr.get_offer(our_pubkey)
                if bolt12_offer:
                    broadcast_count = _broadcast_settlement_offer(our_pubkey, bolt12_offer)
                    plugin.log(f"cl-hive: Broadcast settlement offer to {broadcast_count} member(s)")

    # Initiate state sync with the peer that welcomed us
    if gossip_mgr and safe_plugin:
        state_hash_msg = _create_signed_state_hash_msg()
        if state_hash_msg:
            try:
                safe_plugin.rpc.call("sendcustommsg", {
                    "node_id": peer_id,
                    "msg": state_hash_msg.hex()
                })
                plugin.log(f"cl-hive: STATE_HASH sent to {peer_id[:16]}... for anti-entropy sync")
            except Exception as e:
                plugin.log(f"cl-hive: Failed to send STATE_HASH to {peer_id[:16]}...: {e}", level='warn')

    return {"result": "continue"}


# =============================================================================
# PHASE 2: STATE MANAGEMENT HANDLERS
# =============================================================================

def handle_gossip(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_GOSSIP message (state update from peer).

    Process incoming gossip and update our local state cache.
    The GossipManager handles version validation and StateManager updates.

    SECURITY: Requires cryptographic signature verification.
    """
    if not gossip_mgr:
        return {"result": "continue"}

    # SECURITY: Validate payload structure including signature field
    if not validate_gossip(payload):
        plugin.log(
            f"cl-hive: GOSSIP rejected from {peer_id[:16]}...: invalid payload",
            level='warn'
        )
        return {"result": "continue"}

    # SECURITY: Verify cryptographic signature
    sender_id = payload.get("sender_id")
    signature = payload.get("signature")
    signing_payload = get_gossip_signing_payload(payload)

    try:
        result = safe_plugin.rpc.checkmessage(signing_payload, signature)
        if not result.get("verified") or result.get("pubkey") != sender_id:
            plugin.log(
                f"cl-hive: GOSSIP signature invalid from {peer_id[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: GOSSIP signature check failed: {e}", level='warn')
        return {"result": "continue"}

    # SECURITY: Verify sender identity matches peer_id
    if sender_id != peer_id:
        plugin.log(
            f"cl-hive: GOSSIP sender mismatch: claimed {sender_id[:16]}... but peer is {peer_id[:16]}...",
            level='warn'
        )
        return {"result": "continue"}

    # Verify sender is a Hive member before processing
    if not database:
        return {"result": "continue"}
    member = database.get_member(peer_id)
    if not member:
        plugin.log(f"cl-hive: GOSSIP from non-member {peer_id[:16]}..., ignoring", level='warn')
        return {"result": "continue"}

    accepted = gossip_mgr.process_gossip(peer_id, payload)

    if accepted:
        plugin.log(f"cl-hive: GOSSIP accepted from {peer_id[:16]}... "
                   f"(v{payload.get('version', '?')})", level='debug')

    return {"result": "continue"}


def handle_state_hash(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_STATE_HASH message (anti-entropy check).

    Compare remote hash against our local state. If mismatch,
    send a FULL_SYNC with our complete state including membership.

    SECURITY: Requires cryptographic signature verification.
    """
    if not gossip_mgr or not state_manager:
        return {"result": "continue"}

    # SECURITY: Validate payload structure including signature field
    if not validate_state_hash(payload):
        plugin.log(
            f"cl-hive: STATE_HASH rejected from {peer_id[:16]}...: invalid payload",
            level='warn'
        )
        return {"result": "continue"}

    # SECURITY: Verify cryptographic signature
    sender_id = payload.get("sender_id")
    signature = payload.get("signature")
    signing_payload = get_state_hash_signing_payload(payload)

    try:
        result = safe_plugin.rpc.checkmessage(signing_payload, signature)
        if not result.get("verified") or result.get("pubkey") != sender_id:
            plugin.log(
                f"cl-hive: STATE_HASH signature invalid from {peer_id[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: STATE_HASH signature check failed: {e}", level='warn')
        return {"result": "continue"}

    # SECURITY: Verify sender identity matches peer_id
    if sender_id != peer_id:
        plugin.log(
            f"cl-hive: STATE_HASH sender mismatch: claimed {sender_id[:16]}... but peer is {peer_id[:16]}...",
            level='warn'
        )
        return {"result": "continue"}

    hashes_match = gossip_mgr.process_state_hash(peer_id, payload)

    if not hashes_match:
        # State divergence detected - send signed FULL_SYNC with membership
        plugin.log(f"cl-hive: State divergence with {peer_id[:16]}..., sending FULL_SYNC")

        full_sync_msg = _create_signed_full_sync_msg()
        if full_sync_msg:
            try:
                safe_plugin.rpc.call("sendcustommsg", {
                    "node_id": peer_id,
                    "msg": full_sync_msg.hex()
                })
            except Exception as e:
                plugin.log(f"cl-hive: Failed to send FULL_SYNC: {e}", level='warn')

    return {"result": "continue"}


def handle_full_sync(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_FULL_SYNC message (complete state transfer).

    Merge the received state with our local state, preferring
    higher version numbers for each peer.

    SECURITY: Requires cryptographic signature verification.
    Only accept FULL_SYNC from authenticated Hive members.
    """
    if not gossip_mgr:
        return {"result": "continue"}

    # SECURITY: Validate payload structure including signature field
    if not validate_full_sync(payload):
        plugin.log(
            f"cl-hive: FULL_SYNC rejected from {peer_id[:16]}...: invalid payload structure",
            level='warn'
        )
        return {"result": "continue"}

    # SECURITY: Verify cryptographic signature
    sender_id = payload.get("sender_id")
    signature = payload.get("signature")
    signing_payload = get_full_sync_signing_payload(payload)

    try:
        result = safe_plugin.rpc.checkmessage(signing_payload, signature)
        if not result.get("verified") or result.get("pubkey") != sender_id:
            plugin.log(
                f"cl-hive: FULL_SYNC signature invalid from {peer_id[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: FULL_SYNC signature check failed: {e}", level='warn')
        return {"result": "continue"}

    # SECURITY: Verify sender identity matches peer_id (prevent relay attacks)
    if sender_id != peer_id:
        plugin.log(
            f"cl-hive: FULL_SYNC sender mismatch: claimed {sender_id[:16]}... but peer is {peer_id[:16]}...",
            level='warn'
        )
        return {"result": "continue"}

    # SECURITY: Verify states match the signed fleet_hash (prevent state injection)
    states = payload.get("states", [])
    fleet_hash = payload.get("fleet_hash", "")
    if states and fleet_hash:
        computed_hash = compute_states_hash(states)
        if computed_hash != fleet_hash:
            plugin.log(
                f"cl-hive: FULL_SYNC states hash mismatch from {peer_id[:16]}...: "
                f"computed={computed_hash[:16]}... expected={fleet_hash[:16]}...",
                level='warn'
            )
            return {"result": "continue"}

    # SECURITY: Membership check to prevent state poisoning
    if database:
        member = database.get_member(peer_id)
        if not member:
            plugin.log(
                f"cl-hive: FULL_SYNC rejected from non-member {peer_id[:16]}...",
                level='warn'
            )
            return {"result": "continue"}

    updated = gossip_mgr.process_full_sync(peer_id, payload)

    # Process membership list if included (Phase 5 enhancement)
    members_synced = 0
    if database and "members" in payload:
        members_synced = _apply_membership_sync(payload["members"], peer_id, plugin)

    plugin.log(f"cl-hive: FULL_SYNC from {peer_id[:16]}...: {updated} states, {members_synced} members synced")

    return {"result": "continue"}


def _apply_membership_sync(members_list: list, sender_id: str, plugin: Plugin) -> int:
    """
    Apply membership list from FULL_SYNC payload.

    Only adds members we don't already know about. Does not demote
    or remove members (membership changes require proper protocol).

    Args:
        members_list: List of member dicts with peer_id, tier, joined_at
        sender_id: ID of the peer who sent this sync
        plugin: Plugin for logging

    Returns:
        Number of new members added
    """
    if not database or not isinstance(members_list, list):
        return 0

    added = 0
    for member_info in members_list:
        if not isinstance(member_info, dict):
            continue

        member_peer_id = member_info.get("peer_id")
        if not member_peer_id or not isinstance(member_peer_id, str):
            continue

        # Check if we already know this member
        existing = database.get_member(member_peer_id)
        if existing:
            continue  # Already have this member

        tier = member_info.get("tier", "neophyte")
        joined_at = member_info.get("joined_at", int(time.time()))

        # Validate tier value (2-tier system: member or neophyte)
        if tier not in ("member", "neophyte"):
            tier = "neophyte"

        try:
            database.add_member(
                peer_id=member_peer_id,
                tier=tier,
                joined_at=joined_at
            )
            added += 1
            plugin.log(f"cl-hive: Added member {member_peer_id[:16]}... ({tier}) from sync")
        except Exception as e:
            plugin.log(f"cl-hive: Failed to add synced member: {e}", level='warn')

    return added


def _create_membership_payload() -> list:
    """
    Create membership list for inclusion in FULL_SYNC.

    Returns:
        List of member dicts with peer_id, tier, joined_at
    """
    if not database:
        return []

    members = database.get_all_members()
    return [
        {
            "peer_id": m["peer_id"],
            "tier": m.get("tier", "neophyte"),
            "joined_at": m.get("joined_at", 0)
        }
        for m in members
    ]


def _create_signed_full_sync_msg() -> Optional[bytes]:
    """
    Create a signed FULL_SYNC message with membership.

    SECURITY: All FULL_SYNC messages must be cryptographically signed
    to prevent state poisoning attacks.

    Returns:
        Serialized and signed FULL_SYNC message, or None if signing fails
    """
    if not gossip_mgr or not safe_plugin or not our_pubkey:
        return None

    # Create base payload
    full_sync_payload = gossip_mgr.create_full_sync_payload()
    full_sync_payload["members"] = _create_membership_payload()

    # Add sender identification
    full_sync_payload["sender_id"] = our_pubkey
    full_sync_payload["timestamp"] = int(time.time())

    # Sign the payload
    signing_payload = get_full_sync_signing_payload(full_sync_payload)
    try:
        sig_result = safe_plugin.rpc.signmessage(signing_payload)
        full_sync_payload["signature"] = sig_result["zbase"]
    except Exception as e:
        plugin.log(f"cl-hive: Failed to sign FULL_SYNC: {e}", level='error')
        return None

    return serialize(HiveMessageType.FULL_SYNC, full_sync_payload)


def _create_signed_state_hash_msg() -> Optional[bytes]:
    """
    Create a signed STATE_HASH message for anti-entropy sync.

    SECURITY: All STATE_HASH messages must be cryptographically signed
    to prevent hash manipulation attacks.

    Returns:
        Serialized and signed STATE_HASH message, or None if signing fails
    """
    if not gossip_mgr or not safe_plugin or not our_pubkey:
        return None

    # Create base payload
    state_hash_payload = gossip_mgr.create_state_hash_payload()

    # Add sender identification and timestamp
    state_hash_payload["sender_id"] = our_pubkey
    state_hash_payload["timestamp"] = int(time.time())

    # Sign the payload
    signing_payload = get_state_hash_signing_payload(state_hash_payload)
    try:
        sig_result = safe_plugin.rpc.signmessage(signing_payload)
        state_hash_payload["signature"] = sig_result["zbase"]
    except Exception as e:
        plugin.log(f"cl-hive: Failed to sign STATE_HASH: {e}", level='error')
        return None

    return serialize(HiveMessageType.STATE_HASH, state_hash_payload)


def _create_signed_gossip_msg(capacity_sats: int, available_sats: int,
                               fee_policy: Dict, topology: list) -> Optional[bytes]:
    """
    Create a signed GOSSIP message for broadcast.

    SECURITY: All GOSSIP messages must be cryptographically signed
    to prevent data tampering attacks where attackers modify fee
    policies, topology, or capacity data.

    Args:
        capacity_sats: Total Hive channel capacity
        available_sats: Available outbound liquidity
        fee_policy: Current fee policy dict
        topology: List of external peer connections

    Returns:
        Serialized and signed GOSSIP message, or None if signing fails
    """
    if not gossip_mgr or not safe_plugin or not our_pubkey:
        return None

    # Create gossip payload using GossipManager
    gossip_payload = gossip_mgr.create_gossip_payload(
        our_pubkey=our_pubkey,
        capacity_sats=capacity_sats,
        available_sats=available_sats,
        fee_policy=fee_policy,
        topology=topology
    )

    # Add sender identification for signature verification
    gossip_payload["sender_id"] = our_pubkey

    # Sign the payload (includes data hash for integrity)
    signing_payload = get_gossip_signing_payload(gossip_payload)
    try:
        sig_result = safe_plugin.rpc.signmessage(signing_payload)
        gossip_payload["signature"] = sig_result["zbase"]
    except Exception as e:
        plugin.log(f"cl-hive: Failed to sign GOSSIP: {e}", level='error')
        return None

    return serialize(HiveMessageType.GOSSIP, gossip_payload)


def _broadcast_full_sync_to_members(plugin: Plugin) -> None:
    """
    Broadcast signed FULL_SYNC with membership to all existing members.

    Called after adding a new member to ensure all nodes sync.
    SECURITY: All FULL_SYNC messages are cryptographically signed.
    """
    if not database or not gossip_mgr or not safe_plugin:
        plugin.log(f"cl-hive: _broadcast_full_sync_to_members: missing deps", level='debug')
        return

    members = database.get_all_members()
    plugin.log(f"cl-hive: Broadcasting membership to {len(members)} known members")

    # Create signed FULL_SYNC payload with membership
    full_sync_msg = _create_signed_full_sync_msg()
    if not full_sync_msg:
        plugin.log("cl-hive: Failed to create signed FULL_SYNC", level='error')
        return

    sent_count = 0
    for member in members:
        member_id = member["peer_id"]
        if member_id == our_pubkey:
            continue

        try:
            safe_plugin.rpc.call("sendcustommsg", {
                "node_id": member_id,
                "msg": full_sync_msg.hex()
            })
            sent_count += 1
            plugin.log(f"cl-hive: Sent FULL_SYNC to {member_id[:16]}...", level='debug')
        except Exception as e:
            plugin.log(f"cl-hive: Failed to send FULL_SYNC to {member_id[:16]}...: {e}", level='info')

    plugin.log(f"cl-hive: Membership broadcast complete: {sent_count} messages sent")


# =============================================================================
# PEER CONNECTION HOOK (State Hash Exchange)
# =============================================================================

@plugin.subscribe("connect")
def on_peer_connected(**kwargs):
    """
    Hook called when a peer connects.

    If the peer is a Hive member, send a STATE_HASH message to
    initiate anti-entropy check and detect state divergence.
    """
    # CLN v25+ sends 'id' in the notification payload
    peer_id = kwargs.get('id')
    if not peer_id or not database or not gossip_mgr:
        return

    # Check if this peer is a Hive member
    member = database.get_member(peer_id)
    if not member:
        return  # Not a Hive member, ignore

    now = int(time.time())
    database.update_member(peer_id, last_seen=now)
    database.update_presence(peer_id, is_online=True, now_ts=now, window_seconds=30 * 86400)

    # Track VPN connection status
    peer_address = None
    if vpn_transport and safe_plugin:
        try:
            peers = safe_plugin.rpc.listpeers(id=peer_id)
            if peers and peers.get('peers') and peers['peers'][0].get('netaddr'):
                peer_address = peers['peers'][0]['netaddr'][0]
                vpn_transport.on_peer_connected(peer_id, peer_address)
        except Exception:
            pass

    if safe_plugin:
        safe_plugin.log(f"cl-hive: Hive member {peer_id[:16]}... connected, sending STATE_HASH")

    # Send signed STATE_HASH for anti-entropy check
    state_hash_msg = _create_signed_state_hash_msg()
    if state_hash_msg:
        try:
            safe_plugin.rpc.call("sendcustommsg", {
                "node_id": peer_id,
                "msg": state_hash_msg.hex()
            })
        except Exception as e:
            if safe_plugin:
                safe_plugin.log(f"cl-hive: Failed to send STATE_HASH to {peer_id[:16]}...: {e}", level='warn')


@plugin.subscribe("disconnect")
def on_peer_disconnected(**kwargs):
    """Update presence for disconnected peers."""
    peer_id = kwargs.get('id')
    if not peer_id or not database:
        return

    # Update VPN transport tracking
    if vpn_transport:
        vpn_transport.on_peer_disconnected(peer_id)

    member = database.get_member(peer_id)
    if not member:
        return
    now = int(time.time())
    database.update_member(peer_id, last_seen=now)
    database.update_presence(peer_id, is_online=False, now_ts=now, window_seconds=30 * 86400)


@plugin.subscribe("forward_event")
def on_forward_event(plugin: Plugin, **payload):
    """Track forwarding events for contribution, leech detection, and route probing."""
    # Handle contribution tracking
    if contribution_mgr:
        try:
            contribution_mgr.handle_forward_event(payload)
        except Exception as e:
            if safe_plugin:
                safe_plugin.log(f"Forward event handling error: {e}", level="warn")

    # Generate route probe data from successful forwards (Phase 7.4)
    if routing_map and database and our_pubkey:
        try:
            forward_event = payload.get("forward_event", payload)
            status = forward_event.get("status")
            if status == "settled":
                _record_forward_as_route_probe(forward_event)
        except Exception as e:
            if safe_plugin:
                safe_plugin.log(f"Route probe from forward error: {e}", level="debug")

    # Record routing revenue to pool (Phase 0 - Collective Economics)
    if routing_pool and our_pubkey:
        try:
            forward_event = payload.get("forward_event", payload)
            status = forward_event.get("status")
            if status == "settled":
                fee_msat = forward_event.get("fee_msat", 0)
                if fee_msat > 0:
                    fee_sats = fee_msat // 1000
                    if fee_sats > 0:
                        routing_pool.record_revenue(
                            member_id=our_pubkey,
                            amount_sats=fee_sats,
                            channel_id=forward_event.get("out_channel"),
                            payment_hash=forward_event.get("payment_hash")
                        )
        except Exception as e:
            if safe_plugin:
                safe_plugin.log(f"Pool revenue recording error: {e}", level="debug")


def _record_forward_as_route_probe(forward_event: Dict):
    """
    Record a settled forward as route probe data.

    While we don't know the full path, we can record that this hop
    (through our node) succeeded, which contributes to path success rates.
    """
    if not routing_map or not database or not safe_plugin:
        return

    try:
        in_channel = forward_event.get("in_channel", "")
        out_channel = forward_event.get("out_channel", "")
        fee_msat = forward_event.get("fee_msat", 0)
        out_msat = forward_event.get("out_msat", 0)

        if not in_channel or not out_channel:
            return

        # Get peer IDs for the channels
        funds = safe_plugin.rpc.listfunds()
        channels = {ch.get("short_channel_id"): ch for ch in funds.get("channels", [])}

        in_peer = channels.get(in_channel, {}).get("peer_id", "")
        out_peer = channels.get(out_channel, {}).get("peer_id", "")

        if not in_peer or not out_peer:
            return

        # Record this as a successful path segment: in_peer -> us -> out_peer
        # This is stored locally (no need to broadcast - each node sees their own forwards)
        database.store_route_probe(
            reporter_id=our_pubkey,
            destination=out_peer,  # The next hop in the path
            path=[in_peer, our_pubkey],  # Partial path we observed
            success=True,
            latency_ms=0,  # We don't have timing for forwards
            failure_reason="",
            failure_hop=-1,
            estimated_capacity_sats=out_msat // 1000 if out_msat else 0,
            total_fee_ppm=int((fee_msat * 1_000_000) / out_msat) if out_msat else 0,
            amount_probed_sats=out_msat // 1000 if out_msat else 0,
            timestamp=int(time.time())
        )
    except Exception:
        pass  # Silently ignore errors in route probe recording


# =============================================================================
# PHASE 3: INTENT LOCK HANDLERS
# =============================================================================

def handle_intent(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_INTENT message (remote lock request).

    When we receive an intent from another node:
    1. Record it for visibility
    2. Check for conflicts with our pending intents
    3. If conflict, apply tie-breaker (lowest pubkey wins)
    4. If we lose, abort our local intent
    """
    if not intent_mgr:
        return {"result": "continue"}

    # P3-02: Verify sender is a Hive member before processing
    if not database:
        return {"result": "continue"}
    member = database.get_member(peer_id)
    if not member:
        plugin.log(f"cl-hive: INTENT from non-member {peer_id[:16]}..., ignoring", level='warn')
        return {"result": "continue"}

    required_fields = ["intent_type", "target", "initiator", "timestamp"]
    for field in required_fields:
        if field not in payload:
            plugin.log(f"cl-hive: INTENT from {peer_id[:16]}... missing {field}", level='warn')
            return {"result": "continue"}

    if payload.get("initiator") != peer_id:
        plugin.log(f"cl-hive: INTENT from {peer_id[:16]}... initiator mismatch", level='warn')
        return {"result": "continue"}

    if payload.get("intent_type") not in {t.value for t in IntentType}:
        plugin.log(f"cl-hive: INTENT from {peer_id[:16]}... invalid intent_type", level='warn')
        return {"result": "continue"}

    if not isinstance(payload.get("target"), str) or not payload.get("target"):
        plugin.log(f"cl-hive: INTENT from {peer_id[:16]}... invalid target", level='warn')
        return {"result": "continue"}

    # Parse the remote intent
    remote_intent = Intent.from_dict(payload)
    
    # Record for visibility
    intent_mgr.record_remote_intent(remote_intent)
    
    # Check for conflicts
    has_conflict, we_win = intent_mgr.check_conflicts(remote_intent)
    
    if has_conflict:
        if we_win:
            # We win the tie-breaker - they should abort
            plugin.log(f"cl-hive: INTENT conflict with {peer_id[:16]}..., we WIN tie-breaker")
        else:
            # We lose - abort our local intent
            plugin.log(f"cl-hive: INTENT conflict with {peer_id[:16]}..., we LOSE tie-breaker")
            intent_mgr.abort_local_intent(
                target=remote_intent.target,
                intent_type=remote_intent.intent_type
            )
            
            # Broadcast our abort
            broadcast_intent_abort(remote_intent.target, remote_intent.intent_type)
    
    return {"result": "continue"}


def handle_intent_abort(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_INTENT_ABORT message (remote node yielding).

    Update our record to show the remote node aborted their intent.

    SECURITY: Requires cryptographic signature verification.
    Only the intent owner can abort their own intent.
    """
    if not intent_mgr:
        return {"result": "continue"}

    # SECURITY: Validate payload structure including signature field
    if not validate_intent_abort(payload):
        plugin.log(
            f"cl-hive: INTENT_ABORT rejected from {peer_id[:16]}...: invalid payload",
            level='warn'
        )
        return {"result": "continue"}

    intent_type = payload.get('intent_type')
    target = payload.get('target')
    initiator = payload.get('initiator')
    signature = payload.get('signature')

    # SECURITY: Verify cryptographic signature
    signing_payload = get_intent_abort_signing_payload(payload)
    try:
        result = safe_plugin.rpc.checkmessage(signing_payload, signature)
        if not result.get("verified") or result.get("pubkey") != initiator:
            plugin.log(
                f"cl-hive: INTENT_ABORT signature invalid from {peer_id[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: INTENT_ABORT signature check failed: {e}", level='warn')
        return {"result": "continue"}

    # SECURITY: Verify initiator matches peer_id (only abort your own intents)
    if initiator != peer_id:
        plugin.log(
            f"cl-hive: INTENT_ABORT initiator mismatch: claimed {initiator[:16]}... but peer is {peer_id[:16]}...",
            level='warn'
        )
        return {"result": "continue"}

    intent_mgr.record_remote_abort(intent_type, target, initiator)
    plugin.log(f"cl-hive: INTENT_ABORT from {peer_id[:16]}... for {target[:16]}...")

    return {"result": "continue"}


def broadcast_intent_abort(target: str, intent_type: str) -> None:
    """
    Broadcast signed HIVE_INTENT_ABORT to all Hive members.

    Called when we lose a tie-breaker and need to yield.

    SECURITY: All INTENT_ABORT messages are cryptographically signed.
    """
    if not database or not safe_plugin or not intent_mgr:
        return

    members = database.get_all_members()
    abort_payload = {
        'intent_type': intent_type,
        'target': target,
        'initiator': intent_mgr.our_pubkey,
        'timestamp': int(time.time()),
        'reason': 'tie_breaker_loss'
    }

    # Sign the payload
    signing_payload = get_intent_abort_signing_payload(abort_payload)
    try:
        sig_result = safe_plugin.rpc.signmessage(signing_payload)
        abort_payload['signature'] = sig_result['zbase']
    except Exception as e:
        plugin.log(f"cl-hive: Failed to sign INTENT_ABORT: {e}", level='error')
        return

    abort_msg = serialize(HiveMessageType.INTENT_ABORT, abort_payload)

    for member in members:
        member_id = member['peer_id']
        if member_id == intent_mgr.our_pubkey:
            continue  # Skip self

        try:
            safe_plugin.rpc.call("sendcustommsg", {
                "node_id": member_id,
                "msg": abort_msg.hex()
            })
        except Exception as e:
            safe_plugin.log(f"Failed to send INTENT_ABORT to {member_id[:16]}...: {e}", level='debug')


# =============================================================================
# PHASE 5: PROMOTION PROTOCOL HANDLERS
# =============================================================================

def _broadcast_to_members(message_bytes: bytes) -> int:
    """
    Broadcast a message to all hive members (excluding ourselves).

    Returns:
        Number of members the message was successfully sent to.
    """
    if not database or not safe_plugin:
        return 0

    sent_count = 0
    for member in database.get_all_members():
        tier = member.get("tier")
        # Broadcast to both members and admins
        if tier not in (MembershipTier.MEMBER.value,):
            continue
        member_id = member["peer_id"]
        if member_id == our_pubkey:
            continue
        try:
            safe_plugin.rpc.call("sendcustommsg", {
                "node_id": member_id,
                "msg": message_bytes.hex()
            })
            sent_count += 1
        except Exception as e:
            safe_plugin.log(f"Failed to send message to {member_id[:16]}...: {e}", level='debug')

    return sent_count


def _sync_member_policies(plugin: Plugin) -> None:
    """
    Sync fee policies for all existing members on startup.

    Called during initialization to ensure all members have correct
    fee policies set in cl-revenue-ops. This handles the case where
    the plugin was restarted or policies were reset.

    Policy assignment:
    - Admin: HIVE strategy (0 PPM fees)
    - Member: HIVE strategy (0 PPM fees)
    - Neophyte: dynamic strategy (normal fee behavior)
    """
    if not database or not bridge or bridge.status != BridgeStatus.ENABLED:
        return

    members = database.get_all_members()
    synced = 0

    for member in members:
        peer_id = member["peer_id"]
        tier = member.get("tier")

        # Skip ourselves
        if peer_id == our_pubkey:
            continue

        # Determine if this peer should have HIVE strategy
        # Both admin and member tiers get HIVE strategy
        is_hive_member = tier in (MembershipTier.MEMBER.value,)

        try:
            # Use bypass_rate_limit=True for startup sync
            success = bridge.set_hive_policy(peer_id, is_member=is_hive_member, bypass_rate_limit=True)
            if success:
                synced += 1
                plugin.log(
                    f"cl-hive: Synced policy for {peer_id[:16]}... "
                    f"({'hive' if is_hive_member else 'dynamic'})",
                    level='debug'
                )
        except Exception as e:
            plugin.log(
                f"cl-hive: Failed to sync policy for {peer_id[:16]}...: {e}",
                level='debug'
            )

    if synced > 0:
        plugin.log(f"cl-hive: Synced fee policies for {synced} member(s)")


def _sync_membership_on_startup(plugin: Plugin) -> None:
    """
    Broadcast signed membership list to all known peers on startup.

    This ensures all nodes converge to the same membership state
    when the plugin restarts.

    SECURITY: All FULL_SYNC messages are cryptographically signed.
    """
    if not database or not gossip_mgr or not safe_plugin:
        return

    members = database.get_all_members()
    if len(members) <= 1:
        return  # Just us, nothing to sync

    # Create signed FULL_SYNC with membership
    full_sync_msg = _create_signed_full_sync_msg()
    if not full_sync_msg:
        plugin.log("cl-hive: Failed to create signed FULL_SYNC for startup sync", level='error')
        return

    sent_count = 0
    for member in members:
        member_id = member["peer_id"]
        if member_id == our_pubkey:
            continue

        try:
            safe_plugin.rpc.call("sendcustommsg", {
                "node_id": member_id,
                "msg": full_sync_msg.hex()
            })
            sent_count += 1
        except Exception as e:
            plugin.log(f"cl-hive: Startup sync to {member_id[:16]}...: {e}", level='debug')

    if sent_count > 0:
        plugin.log(f"cl-hive: Broadcast membership to {sent_count} peer(s) on startup")


def handle_promotion_request(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    if not config or not config.membership_enabled or not membership_mgr:
        return {"result": "continue"}

    if not validate_promotion_request(payload):
        plugin.log(f"cl-hive: PROMOTION_REQUEST from {peer_id[:16]}... invalid payload", level='warn')
        return {"result": "continue"}

    target_pubkey = payload["target_pubkey"]
    request_id = payload["request_id"]
    timestamp = payload["timestamp"]

    if target_pubkey != peer_id:
        plugin.log(f"cl-hive: PROMOTION_REQUEST from {peer_id[:16]}... target mismatch", level='warn')
        return {"result": "continue"}

    target_member = database.get_member(target_pubkey)
    if not target_member or target_member.get("tier") != MembershipTier.NEOPHYTE.value:
        return {"result": "continue"}

    database.add_promotion_request(target_pubkey, request_id, status="pending")

    our_tier = membership_mgr.get_tier(our_pubkey) if our_pubkey else None
    if our_tier not in (MembershipTier.MEMBER.value,):
        return {"result": "continue"}

    if not config.auto_vouch_enabled:
        return {"result": "continue"}

    eval_result = membership_mgr.evaluate_promotion(target_pubkey)
    if not eval_result["eligible"]:
        return {"result": "continue"}

    existing_vouches = database.get_promotion_vouches(target_pubkey, request_id)
    for vouch in existing_vouches:
        if vouch.get("voucher_peer_id") == our_pubkey:
            return {"result": "continue"}

    vouch_ts = int(time.time())
    canonical = membership_mgr.build_vouch_message(target_pubkey, request_id, vouch_ts)
    try:
        sig = safe_plugin.rpc.signmessage(canonical)["zbase"]
    except Exception as e:
        plugin.log(f"cl-hive: Failed to sign vouch: {e}", level='warn')
        return {"result": "continue"}

    vouch_payload = {
        "target_pubkey": target_pubkey,
        "request_id": request_id,
        "timestamp": vouch_ts,
        "voucher_pubkey": our_pubkey,
        "sig": sig
    }
    vouch_msg = serialize(HiveMessageType.VOUCH, vouch_payload)
    _broadcast_to_members(vouch_msg)
    return {"result": "continue"}


def handle_vouch(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    if not config or not config.membership_enabled or not membership_mgr:
        return {"result": "continue"}

    if not validate_vouch(payload):
        plugin.log(f"cl-hive: VOUCH from {peer_id[:16]}... invalid payload", level='warn')
        return {"result": "continue"}

    if payload["voucher_pubkey"] != peer_id:
        plugin.log(f"cl-hive: VOUCH from {peer_id[:16]}... voucher mismatch", level='warn')
        return {"result": "continue"}

    voucher = database.get_member(peer_id)
    if not voucher or voucher.get("tier") not in (MembershipTier.MEMBER.value,):
        return {"result": "continue"}

    target_member = database.get_member(payload["target_pubkey"])
    if not target_member or target_member.get("tier") != MembershipTier.NEOPHYTE.value:
        return {"result": "continue"}

    now = int(time.time())
    if now - payload["timestamp"] > VOUCH_TTL_SECONDS:
        return {"result": "continue"}

    canonical = membership_mgr.build_vouch_message(
        payload["target_pubkey"], payload["request_id"], payload["timestamp"]
    )
    try:
        result = safe_plugin.rpc.checkmessage(canonical, payload["sig"])
    except Exception as e:
        plugin.log(f"cl-hive: VOUCH signature check failed: {e}", level='warn')
        return {"result": "continue"}

    if not result.get("verified") or result.get("pubkey") != payload["voucher_pubkey"]:
        return {"result": "continue"}

    if database.is_banned(payload["voucher_pubkey"]):
        return {"result": "continue"}

    local_tier = membership_mgr.get_tier(our_pubkey) if our_pubkey else None
    if local_tier not in (MembershipTier.MEMBER.value, MembershipTier.NEOPHYTE.value):
        return {"result": "continue"}

    stored = database.add_promotion_vouch(
        payload["target_pubkey"],
        payload["request_id"],
        payload["voucher_pubkey"],
        payload["sig"],
        payload["timestamp"]
    )
    if not stored:
        return {"result": "continue"}

    # Only members and admins can trigger auto-promotion
    if local_tier not in (MembershipTier.MEMBER.value,):
        return {"result": "continue"}

    active_members = membership_mgr.get_active_members()
    quorum = membership_mgr.calculate_quorum(len(active_members))
    vouches = database.get_promotion_vouches(payload["target_pubkey"], payload["request_id"])
    if len(vouches) < quorum:
        return {"result": "continue"}

    if not config.auto_promote_enabled:
        return {"result": "continue"}

    promotion_payload = {
        "target_pubkey": payload["target_pubkey"],
        "request_id": payload["request_id"],
        "vouches": [
            {
                "target_pubkey": v["target_peer_id"],
                "request_id": v["request_id"],
                "timestamp": v["timestamp"],
                "voucher_pubkey": v["voucher_peer_id"],
                "sig": v["sig"]
            } for v in vouches[:MAX_VOUCHES_IN_PROMOTION]
        ]
    }
    promo_msg = serialize(HiveMessageType.PROMOTION, promotion_payload)
    _broadcast_to_members(promo_msg)
    return {"result": "continue"}


def handle_promotion(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    if not config or not config.membership_enabled or not membership_mgr:
        return {"result": "continue"}

    if not validate_promotion(payload):
        plugin.log(f"cl-hive: PROMOTION from {peer_id[:16]}... invalid payload", level='warn')
        return {"result": "continue"}

    sender = database.get_member(peer_id)
    sender_tier = sender.get("tier") if sender else None
    if sender_tier not in (MembershipTier.MEMBER.value,):
        return {"result": "continue"}

    target_pubkey = payload["target_pubkey"]
    request_id = payload["request_id"]

    target_member = database.get_member(target_pubkey)
    if not target_member or target_member.get("tier") != MembershipTier.NEOPHYTE.value:
        return {"result": "continue"}

    request = database.get_promotion_request(target_pubkey, request_id)
    if request and request.get("status") == "accepted":
        return {"result": "continue"}

    active_members = membership_mgr.get_active_members()
    quorum = membership_mgr.calculate_quorum(len(active_members))

    seen_vouchers = set()
    valid_vouches = []
    now = int(time.time())

    for vouch in payload["vouches"]:
        if vouch["voucher_pubkey"] in seen_vouchers:
            continue
        if now - vouch["timestamp"] > VOUCH_TTL_SECONDS:
            continue
        if database.is_banned(vouch["voucher_pubkey"]):
            continue
        member = database.get_member(vouch["voucher_pubkey"])
        member_tier = member.get("tier") if member else None
        if member_tier not in (MembershipTier.MEMBER.value,):
            continue
        canonical = membership_mgr.build_vouch_message(
            vouch["target_pubkey"], vouch["request_id"], vouch["timestamp"]
        )
        try:
            result = safe_plugin.rpc.checkmessage(canonical, vouch["sig"])
        except Exception:
            continue
        if not result.get("verified") or result.get("pubkey") != vouch["voucher_pubkey"]:
            continue
        seen_vouchers.add(vouch["voucher_pubkey"])
        valid_vouches.append(vouch)

    if len(valid_vouches) < quorum:
        return {"result": "continue"}

    database.add_promotion_request(target_pubkey, request_id, status="accepted")
    database.update_promotion_request_status(target_pubkey, request_id, status="accepted")
    membership_mgr.set_tier(target_pubkey, MembershipTier.MEMBER.value)
    return {"result": "continue"}


def handle_member_left(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle MEMBER_LEFT message - a member voluntarily leaving the hive.

    Validates the signature and removes the member from the hive.
    """
    if not config or not database or not safe_plugin:
        return {"result": "continue"}

    if not validate_member_left(payload):
        plugin.log(f"cl-hive: MEMBER_LEFT from {peer_id[:16]}... invalid payload", level='warn')
        return {"result": "continue"}

    leaving_peer_id = payload["peer_id"]
    timestamp = payload["timestamp"]
    reason = payload["reason"]
    signature = payload["signature"]

    # Verify the message came from the leaving peer (self-signed)
    # The sender (peer_id) should match the leaving peer
    if peer_id != leaving_peer_id:
        plugin.log(f"cl-hive: MEMBER_LEFT sender mismatch: {peer_id[:16]}... != {leaving_peer_id[:16]}...", level='warn')
        return {"result": "continue"}

    # Check if member exists
    member = database.get_member(leaving_peer_id)
    if not member:
        plugin.log(f"cl-hive: MEMBER_LEFT for unknown peer {leaving_peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Verify signature
    canonical = f"hive:leave:{leaving_peer_id}:{timestamp}:{reason}"
    try:
        result = safe_plugin.rpc.checkmessage(canonical, signature)
        if not result.get("verified") or result.get("pubkey") != leaving_peer_id:
            plugin.log(f"cl-hive: MEMBER_LEFT signature invalid for {leaving_peer_id[:16]}...", level='warn')
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: MEMBER_LEFT signature check failed: {e}", level='warn')
        return {"result": "continue"}

    # Remove the member
    tier = member.get("tier")
    database.remove_member(leaving_peer_id)
    plugin.log(f"cl-hive: Member {leaving_peer_id[:16]}... ({tier}) left the hive: {reason}")

    # Revert their fee policy to dynamic if bridge is available
    if bridge and bridge.status == BridgeStatus.ENABLED:
        try:
            bridge.set_hive_policy(leaving_peer_id, is_member=False)
        except Exception as e:
            plugin.log(f"cl-hive: Failed to revert policy for {leaving_peer_id[:16]}...: {e}", level='debug')

    # Check if hive is now headless (no full members)
    all_members = database.get_all_members()
    member_count = sum(1 for m in all_members if m.get("tier") == MembershipTier.MEMBER.value)
    if member_count == 0 and len(all_members) > 0:
        plugin.log("cl-hive: WARNING - Hive has no full members (only neophytes). Promote neophytes to restore governance.", level='warn')

    return {"result": "continue"}


# =============================================================================
# BAN VOTING CONSTANTS
# =============================================================================

# Ban proposal voting period (7 days)
BAN_PROPOSAL_TTL_SECONDS = 7 * 24 * 3600

# Quorum threshold for ban approval (51%)
BAN_QUORUM_THRESHOLD = 0.51

# Cooldown before re-proposing ban for same peer (7 days)
BAN_COOLDOWN_SECONDS = 7 * 24 * 3600


def handle_ban_proposal(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle BAN_PROPOSAL message - a member proposing to ban another member.

    Validates the proposal and stores it for voting.
    """
    if not config or not database or not safe_plugin:
        return {"result": "continue"}

    if not validate_ban_proposal(payload):
        plugin.log(f"cl-hive: BAN_PROPOSAL from {peer_id[:16]}... invalid payload", level='warn')
        return {"result": "continue"}

    target_peer_id = payload["target_peer_id"]
    proposer_peer_id = payload["proposer_peer_id"]
    proposal_id = payload["proposal_id"]
    reason = payload["reason"]
    timestamp = payload["timestamp"]
    signature = payload["signature"]

    # Verify sender is the proposer
    if peer_id != proposer_peer_id:
        plugin.log(f"cl-hive: BAN_PROPOSAL sender mismatch", level='warn')
        return {"result": "continue"}

    # Verify proposer is a member or admin
    proposer = database.get_member(proposer_peer_id)
    if not proposer or proposer.get("tier") not in (MembershipTier.MEMBER.value,):
        plugin.log(f"cl-hive: BAN_PROPOSAL from non-member", level='warn')
        return {"result": "continue"}

    # Verify target is a member
    target = database.get_member(target_peer_id)
    if not target:
        plugin.log(f"cl-hive: BAN_PROPOSAL for non-member {target_peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Cannot ban yourself
    if target_peer_id == proposer_peer_id:
        return {"result": "continue"}

    # Verify signature
    canonical = f"hive:ban_proposal:{proposal_id}:{target_peer_id}:{timestamp}:{reason}"
    try:
        result = safe_plugin.rpc.checkmessage(canonical, signature)
        if not result.get("verified") or result.get("pubkey") != proposer_peer_id:
            plugin.log(f"cl-hive: BAN_PROPOSAL signature invalid", level='warn')
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: BAN_PROPOSAL signature check failed: {e}", level='warn')
        return {"result": "continue"}

    # Check if proposal already exists
    existing = database.get_ban_proposal(proposal_id)
    if existing:
        return {"result": "continue"}

    # Store proposal
    expires_at = timestamp + BAN_PROPOSAL_TTL_SECONDS
    database.create_ban_proposal(proposal_id, target_peer_id, proposer_peer_id,
                                 reason, timestamp, expires_at)
    plugin.log(f"cl-hive: Ban proposal {proposal_id[:16]}... for {target_peer_id[:16]}... by {proposer_peer_id[:16]}...")

    return {"result": "continue"}


def handle_ban_vote(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle BAN_VOTE message - a member voting on a ban proposal.

    Validates the vote, stores it, and checks if quorum is reached.
    """
    if not config or not database or not safe_plugin or not membership_mgr:
        return {"result": "continue"}

    if not validate_ban_vote(payload):
        plugin.log(f"cl-hive: BAN_VOTE from {peer_id[:16]}... invalid payload", level='warn')
        return {"result": "continue"}

    proposal_id = payload["proposal_id"]
    voter_peer_id = payload["voter_peer_id"]
    vote = payload["vote"]  # "approve" or "reject"
    timestamp = payload["timestamp"]
    signature = payload["signature"]

    # Verify sender is the voter
    if peer_id != voter_peer_id:
        plugin.log(f"cl-hive: BAN_VOTE sender mismatch", level='warn')
        return {"result": "continue"}

    # Verify voter is a member or admin
    voter = database.get_member(voter_peer_id)
    if not voter or voter.get("tier") not in (MembershipTier.MEMBER.value,):
        return {"result": "continue"}

    # Get the proposal
    proposal = database.get_ban_proposal(proposal_id)
    if not proposal or proposal.get("status") != "pending":
        return {"result": "continue"}

    # Verify signature
    canonical = f"hive:ban_vote:{proposal_id}:{vote}:{timestamp}"
    try:
        result = safe_plugin.rpc.checkmessage(canonical, signature)
        if not result.get("verified") or result.get("pubkey") != voter_peer_id:
            plugin.log(f"cl-hive: BAN_VOTE signature invalid", level='warn')
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: BAN_VOTE signature check failed: {e}", level='warn')
        return {"result": "continue"}

    # Store vote
    database.add_ban_vote(proposal_id, voter_peer_id, vote, timestamp, signature)
    plugin.log(f"cl-hive: Ban vote from {voter_peer_id[:16]}... on {proposal_id[:16]}...: {vote}")

    # Check if quorum reached
    _check_ban_quorum(proposal_id, proposal, plugin)

    return {"result": "continue"}


def _check_ban_quorum(proposal_id: str, proposal: Dict, plugin: Plugin) -> bool:
    """
    Check if a ban proposal has reached quorum and execute if so.

    Returns True if ban was executed.
    """
    if not database or not membership_mgr or not bridge:
        return False

    target_peer_id = proposal["target_peer_id"]

    # Get all votes
    votes = database.get_ban_votes(proposal_id)

    # Get eligible voters (members and admins, excluding target)
    all_members = database.get_all_members()
    eligible_voters = [
        m for m in all_members
        if m.get("tier") in (MembershipTier.MEMBER.value,)
        and m["peer_id"] != target_peer_id
    ]
    eligible_count = len(eligible_voters)

    if eligible_count == 0:
        return False

    # Count approve votes from eligible voters
    eligible_voter_ids = set(m["peer_id"] for m in eligible_voters)
    approve_count = sum(
        1 for v in votes
        if v["vote"] == "approve" and v["voter_peer_id"] in eligible_voter_ids
    )

    # Check quorum (51% of eligible voters)
    quorum_needed = int(eligible_count * BAN_QUORUM_THRESHOLD) + 1
    if approve_count >= quorum_needed:
        # Execute ban
        database.update_ban_proposal_status(proposal_id, "approved")
        proposer_id = proposal.get("proposer_peer_id", "quorum_vote")
        database.add_ban(target_peer_id, proposal.get("reason", "quorum_ban"), proposer_id)
        database.remove_member(target_peer_id)

        # Revert fee policy
        if bridge and bridge.status == BridgeStatus.ENABLED:
            try:
                bridge.set_hive_policy(target_peer_id, is_member=False)
            except Exception:
                pass

        plugin.log(f"cl-hive: Ban executed for {target_peer_id[:16]}... ({approve_count}/{eligible_count} votes)")

        # Broadcast BAN message
        ban_payload = {
            "peer_id": target_peer_id,
            "reason": proposal.get("reason", "quorum_ban"),
            "proposal_id": proposal_id
        }
        ban_msg = serialize(HiveMessageType.BAN, ban_payload)
        _broadcast_to_members(ban_msg)

        return True

    return False


# =============================================================================
# PHASE 6: CHANNEL COORDINATION - PEER AVAILABLE HANDLING
# =============================================================================

def handle_peer_available(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle PEER_AVAILABLE message - a hive member reporting a channel event.

    This is sent when:
    - A channel opens (local or remote initiated)
    - A channel closes (any type)
    - A peer's routing quality is exceptional

    Phase 6.1: ALL events are stored in peer_events table for topology intelligence.
    The receiving node uses this data to make informed expansion decisions.

    SECURITY: Requires cryptographic signature verification.
    """
    if not config or not database:
        return {"result": "continue"}

    if not validate_peer_available(payload):
        plugin.log(f"cl-hive: PEER_AVAILABLE from {peer_id[:16]}... invalid payload", level='warn')
        return {"result": "continue"}

    # SECURITY: Verify cryptographic signature
    reporter_peer_id = payload.get("reporter_peer_id")
    signature = payload.get("signature")
    signing_payload = get_peer_available_signing_payload(payload)

    try:
        result = safe_plugin.rpc.checkmessage(signing_payload, signature)
        if not result.get("verified") or result.get("pubkey") != reporter_peer_id:
            plugin.log(
                f"cl-hive: PEER_AVAILABLE signature invalid from {peer_id[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: PEER_AVAILABLE signature check failed: {e}", level='warn')
        return {"result": "continue"}

    # SECURITY: Verify reporter matches peer_id (prevent relay attacks)
    if reporter_peer_id != peer_id:
        plugin.log(
            f"cl-hive: PEER_AVAILABLE reporter mismatch: claimed {reporter_peer_id[:16]}... but peer is {peer_id[:16]}...",
            level='warn'
        )
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: PEER_AVAILABLE from non-member {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Apply rate limiting to prevent gossip flooding (Security Enhancement)
    if peer_available_limiter and not peer_available_limiter.is_allowed(peer_id):
        plugin.log(
            f"cl-hive: PEER_AVAILABLE from {peer_id[:16]}... rate limited (>10/min)",
            level='warn'
        )
        return {"result": "continue"}

    # Extract all fields from payload
    target_peer_id = payload["target_peer_id"]
    reporter_peer_id = payload["reporter_peer_id"]
    event_type = payload["event_type"]
    timestamp = payload["timestamp"]

    # Channel info
    channel_id = payload.get("channel_id", "")
    capacity_sats = payload.get("capacity_sats", 0)

    # Profitability data
    duration_days = payload.get("duration_days", 0)
    total_revenue_sats = payload.get("total_revenue_sats", 0)
    total_rebalance_cost_sats = payload.get("total_rebalance_cost_sats", 0)
    net_pnl_sats = payload.get("net_pnl_sats", 0)
    forward_count = payload.get("forward_count", 0)
    forward_volume_sats = payload.get("forward_volume_sats", 0)
    our_fee_ppm = payload.get("our_fee_ppm", 0)
    their_fee_ppm = payload.get("their_fee_ppm", 0)
    routing_score = payload.get("routing_score", 0.5)
    profitability_score = payload.get("profitability_score", 0.5)

    # Funding info
    our_funding_sats = payload.get("our_funding_sats", 0)
    their_funding_sats = payload.get("their_funding_sats", 0)
    opener = payload.get("opener", "")
    closer = payload.get("closer", "")
    reason = payload.get("reason", "")

    # Determine closer from event_type if not explicitly set
    if not closer and event_type.endswith('_close'):
        if event_type == 'remote_close':
            closer = 'remote'
        elif event_type == 'local_close':
            closer = 'local'
        elif event_type == 'mutual_close':
            closer = 'mutual'

    plugin.log(
        f"cl-hive: PEER_AVAILABLE from {reporter_peer_id[:16]}...: "
        f"target={target_peer_id[:16]}... event={event_type} "
        f"capacity={capacity_sats} pnl={net_pnl_sats}",
        level='info'
    )

    # =========================================================================
    # PHASE 6.1: Store ALL events for topology intelligence
    # =========================================================================
    database.store_peer_event(
        peer_id=target_peer_id,
        reporter_id=reporter_peer_id,
        event_type=event_type,
        timestamp=timestamp,
        channel_id=channel_id,
        capacity_sats=capacity_sats,
        duration_days=duration_days,
        total_revenue_sats=total_revenue_sats,
        total_rebalance_cost_sats=total_rebalance_cost_sats,
        net_pnl_sats=net_pnl_sats,
        forward_count=forward_count,
        forward_volume_sats=forward_volume_sats,
        our_fee_ppm=our_fee_ppm,
        their_fee_ppm=their_fee_ppm,
        routing_score=routing_score,
        profitability_score=profitability_score,
        our_funding_sats=our_funding_sats,
        their_funding_sats=their_funding_sats,
        opener=opener,
        closer=closer,
        reason=reason
    )

    # =========================================================================
    # Evaluate expansion opportunities (only for close events)
    # =========================================================================
    # Channel opens are informational only - no action needed
    if event_type == 'channel_open':
        return {"result": "continue"}

    # Don't open channels to ourselves
    if safe_plugin:
        try:
            our_id = safe_plugin.rpc.getinfo().get("id")
            if target_peer_id == our_id:
                return {"result": "continue"}
        except Exception:
            pass

    # Check if we already have a channel to this peer
    if safe_plugin:
        try:
            channels = safe_plugin.rpc.listpeerchannels(id=target_peer_id)
            if channels.get("channels"):
                plugin.log(
                    f"cl-hive: Already have channel to {target_peer_id[:16]}..., "
                    f"event stored for topology tracking",
                    level='debug'
                )
                return {"result": "continue"}
        except Exception:
            pass  # Peer not connected, which is fine

    # Check if target is in the ban list
    if database.is_banned(target_peer_id):
        plugin.log(f"cl-hive: Ignoring expansion to banned peer {target_peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Only consider expansion for remote-initiated closures
    # (local/mutual closes don't indicate the peer wants more channels)
    if event_type != 'remote_close':
        return {"result": "continue"}

    # Check quality thresholds before proposing expansion
    if routing_score < 0.2:
        plugin.log(
            f"cl-hive: Peer {target_peer_id[:16]}... has low routing score ({routing_score}), "
            f"not proposing expansion",
            level='debug'
        )
        return {"result": "continue"}

    cfg = config.snapshot()

    if not cfg.planner_enable_expansions:
        plugin.log(
            f"cl-hive: Expansions disabled, storing PEER_AVAILABLE for manual review",
            level='debug'
        )
        _store_peer_available_action(target_peer_id, reporter_peer_id, event_type,
                                     capacity_sats, routing_score, reason)
        return {"result": "continue"}

    # Check if on-chain feerates are low enough for channel opening
    feerate_allowed, current_feerate, feerate_reason = _check_feerate_for_expansion(
        cfg.max_expansion_feerate_perkb
    )
    if not feerate_allowed:
        plugin.log(
            f"cl-hive: On-chain fees too high for expansion ({feerate_reason}), "
            f"storing PEER_AVAILABLE for later when fees drop",
            level='info'
        )
        _store_peer_available_action(target_peer_id, reporter_peer_id, event_type,
                                     capacity_sats, routing_score,
                                     f"Deferred: {feerate_reason}")
        return {"result": "continue"}

    # =========================================================================
    # Phase 6.4: Trigger cooperative expansion round
    # =========================================================================
    if coop_expansion:
        # Start a cooperative expansion round for this peer
        round_id = coop_expansion.evaluate_expansion(
            target_peer_id=target_peer_id,
            event_type=event_type,
            reporter_id=reporter_peer_id,
            capacity_sats=capacity_sats,
            quality_score=profitability_score  # Use reported profitability as hint
        )

        if round_id:
            plugin.log(
                f"cl-hive: Started cooperative expansion round {round_id[:8]}... "
                f"for {target_peer_id[:16]}...",
                level='info'
            )
            # Broadcast our nomination to other hive members
            _broadcast_expansion_nomination(round_id, target_peer_id)
        else:
            plugin.log(
                f"cl-hive: No cooperative round started for {target_peer_id[:16]}... "
                f"(may be on cooldown or insufficient quality)",
                level='debug'
            )
    else:
        # Fallback: Store pending action for review
        if cfg.governance_mode in ('advisor', 'failsafe'):
            _store_peer_available_action(target_peer_id, reporter_peer_id, event_type,
                                         capacity_sats, routing_score, reason)
            plugin.log(
                f"cl-hive: Queued channel opportunity to {target_peer_id[:16]}... from PEER_AVAILABLE",
                level='info'
            )

    return {"result": "continue"}


def _check_feerate_for_expansion(max_feerate_perkb: int) -> tuple:
    """
    Check if current on-chain feerates allow channel expansion.

    Args:
        max_feerate_perkb: Maximum feerate threshold in sat/kB (0 = disabled)

    Returns:
        Tuple of (allowed: bool, current_feerate: int, reason: str)
    """
    if max_feerate_perkb == 0:
        return (True, 0, "feerate check disabled")

    if not safe_plugin:
        return (False, 0, "plugin not initialized")

    try:
        feerates = safe_plugin.rpc.feerates("perkb")
        # Use 'opening' feerate which is what fundchannel uses
        opening_feerate = feerates.get("perkb", {}).get("opening")

        if opening_feerate is None:
            # Fallback to min_acceptable if opening not available
            opening_feerate = feerates.get("perkb", {}).get("min_acceptable", 0)

        if opening_feerate == 0:
            return (True, 0, "feerate unavailable, allowing")

        if opening_feerate <= max_feerate_perkb:
            return (True, opening_feerate, "feerate acceptable")
        else:
            return (False, opening_feerate, f"feerate {opening_feerate} > max {max_feerate_perkb}")
    except Exception as e:
        # On error, be conservative and allow (don't block on RPC issues)
        return (True, 0, f"feerate check error: {e}")


def _get_spendable_balance(cfg) -> int:
    """
    Get onchain balance minus reserve, or 0 if unavailable.

    This is the amount available for channel opens after accounting for
    the configured reserve percentage.

    Args:
        cfg: Config snapshot with budget_reserve_pct

    Returns:
        Spendable balance in sats, or 0 if unavailable
    """
    if not safe_plugin:
        return 0
    try:
        funds = safe_plugin.rpc.listfunds()
        outputs = funds.get('outputs', [])
        onchain_balance = sum(
            (o.get('amount_msat', 0) // 1000 if isinstance(o.get('amount_msat'), int)
             else int(o.get('amount_msat', '0msat')[:-4]) // 1000
             if isinstance(o.get('amount_msat'), str) else o.get('value', 0))
            for o in outputs if o.get('status') == 'confirmed'
        )
        return int(onchain_balance * (1.0 - cfg.budget_reserve_pct))
    except Exception:
        return 0


def _cap_channel_size_to_budget(size_sats: int, cfg, context: str = "") -> tuple:
    """
    Cap channel size to available budget.

    Ensures proposed channel sizes don't exceed what we can actually afford.

    Args:
        size_sats: Proposed channel size
        cfg: Config snapshot
        context: Optional context string for logging

    Returns:
        Tuple of (capped_size, was_insufficient, was_capped)
        - capped_size: Final size (0 if insufficient funds)
        - was_insufficient: True if we can't afford minimum channel
        - was_capped: True if size was reduced to fit budget
    """
    spendable = _get_spendable_balance(cfg)

    # Check if we can afford minimum channel size
    if spendable < cfg.planner_min_channel_sats:
        if context and plugin:
            plugin.log(
                f"cl-hive: {context}: insufficient funds "
                f"({spendable:,} < {cfg.planner_min_channel_sats:,} min)",
                level='debug'
            )
        return (0, True, False)

    # Cap to what we can afford
    if size_sats > spendable:
        if context and plugin:
            plugin.log(
                f"cl-hive: {context}: capping channel size from {size_sats:,} to {spendable:,}",
                level='info'
            )
        return (spendable, False, True)

    return (size_sats, False, False)


def _store_peer_available_action(target_peer_id: str, reporter_peer_id: str,
                                  event_type: str, capacity_sats: int,
                                  routing_score: float, reason: str) -> None:
    """Store a PEER_AVAILABLE as a pending action for review/execution."""
    if not database:
        return

    cfg = config.snapshot() if config else None
    if not cfg:
        return

    # Determine suggested channel size
    suggested_sats = capacity_sats
    if capacity_sats == 0:
        suggested_sats = cfg.planner_default_channel_sats

    # Check affordability and cap to available budget
    capped_size, insufficient, was_capped = _cap_channel_size_to_budget(
        suggested_sats, cfg, context=f"PEER_AVAILABLE to {target_peer_id[:16]}..."
    )

    # Skip if we can't afford minimum channel
    if insufficient:
        if plugin:
            plugin.log(
                f"cl-hive: Skipping PEER_AVAILABLE action for {target_peer_id[:16]}...: "
                f"insufficient funds for minimum channel",
                level='info'
            )
        return

    database.add_pending_action(
        action_type="channel_open",
        payload={
            "target": target_peer_id,
            "amount_sats": capped_size,
            "original_amount_sats": suggested_sats if was_capped else None,
            "source": "peer_available",
            "reporter": reporter_peer_id,
            "event_type": event_type,
            "routing_score": routing_score,
            "reason": reason or f"Peer available via {event_type}",
            "budget_capped": was_capped,
        },
        expires_hours=24
    )


def broadcast_peer_available(target_peer_id: str, event_type: str,
                              channel_id: str = "",
                              capacity_sats: int = 0,
                              routing_score: float = 0.0,
                              profitability_score: float = 0.0,
                              reason: str = "",
                              # Profitability data
                              duration_days: int = 0,
                              total_revenue_sats: int = 0,
                              total_rebalance_cost_sats: int = 0,
                              net_pnl_sats: int = 0,
                              forward_count: int = 0,
                              forward_volume_sats: int = 0,
                              our_fee_ppm: int = 0,
                              their_fee_ppm: int = 0,
                              # Funding info (for opens)
                              our_funding_sats: int = 0,
                              their_funding_sats: int = 0,
                              opener: str = "") -> int:
    """
    Broadcast signed PEER_AVAILABLE to all hive members.

    SECURITY: All PEER_AVAILABLE messages are cryptographically signed.

    Args:
        target_peer_id: The external peer involved
        event_type: 'channel_open', 'channel_close', 'remote_close', etc.
        channel_id: The channel short ID
        capacity_sats: Channel capacity
        routing_score: Peer's routing quality score (0-1)
        profitability_score: Overall profitability score (0-1)
        reason: Human-readable reason

        # Profitability data (for closures):
        duration_days, total_revenue_sats, total_rebalance_cost_sats,
        net_pnl_sats, forward_count, forward_volume_sats,
        our_fee_ppm, their_fee_ppm

        # Funding info (for opens):
        our_funding_sats, their_funding_sats, opener

    Returns:
        Number of members message was sent to
    """
    if not safe_plugin or not database:
        return 0

    try:
        our_id = safe_plugin.rpc.getinfo().get("id")
    except Exception:
        return 0

    timestamp = int(time.time())

    # Build payload for signing
    signing_payload_dict = {
        "target_peer_id": target_peer_id,
        "reporter_peer_id": our_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "capacity_sats": capacity_sats,
    }

    # Sign the payload
    signing_str = get_peer_available_signing_payload(signing_payload_dict)
    try:
        sig_result = safe_plugin.rpc.signmessage(signing_str)
        signature = sig_result['zbase']
    except Exception as e:
        plugin.log(f"cl-hive: Failed to sign PEER_AVAILABLE: {e}", level='error')
        return 0

    msg = create_peer_available(
        target_peer_id=target_peer_id,
        reporter_peer_id=our_id,
        event_type=event_type,
        timestamp=timestamp,
        signature=signature,
        channel_id=channel_id,
        capacity_sats=capacity_sats,
        routing_score=routing_score,
        profitability_score=profitability_score,
        reason=reason,
        duration_days=duration_days,
        total_revenue_sats=total_revenue_sats,
        total_rebalance_cost_sats=total_rebalance_cost_sats,
        net_pnl_sats=net_pnl_sats,
        forward_count=forward_count,
        forward_volume_sats=forward_volume_sats,
        our_fee_ppm=our_fee_ppm,
        their_fee_ppm=their_fee_ppm,
        our_funding_sats=our_funding_sats,
        their_funding_sats=their_funding_sats,
        opener=opener
    )

    return _broadcast_to_members(msg)


def _broadcast_expansion_nomination(round_id: str, target_peer_id: str) -> int:
    """
    Broadcast an EXPANSION_NOMINATE message to all hive members.

    Args:
        round_id: The cooperative expansion round ID
        target_peer_id: The target peer for the expansion

    Returns:
        Number of members message was sent to
    """
    if not safe_plugin or not database or not coop_expansion:
        return 0

    try:
        our_id = safe_plugin.rpc.getinfo().get("id")
    except Exception:
        return 0

    # Get our nomination info
    try:
        funds = safe_plugin.rpc.listfunds()
        outputs = funds.get('outputs', [])
        available_liquidity = sum(
            (o.get('amount_msat', 0) // 1000 if isinstance(o.get('amount_msat'), int)
             else int(o.get('amount_msat', '0msat')[:-4]) // 1000
             if isinstance(o.get('amount_msat'), str) else o.get('value', 0))
            for o in outputs if o.get('status') == 'confirmed'
        )
    except Exception:
        available_liquidity = 0

    try:
        channels = safe_plugin.rpc.listpeerchannels()
        channel_count = len(channels.get('channels', []))
    except Exception:
        channel_count = 0

    # Check if we have a channel to target
    try:
        target_channels = safe_plugin.rpc.listpeerchannels(id=target_peer_id)
        has_existing = len(target_channels.get('channels', [])) > 0
    except Exception:
        has_existing = False

    # Get quality score for the target
    quality_score = 0.5
    if database:
        try:
            scorer = PeerQualityScorer(database, safe_plugin)
            result = scorer.calculate_score(target_peer_id)
            quality_score = result.overall_score
        except Exception:
            pass

    import time
    timestamp = int(time.time())

    # Build payload for signing (SECURITY: sign before sending)
    signing_payload = {
        "round_id": round_id,
        "target_peer_id": target_peer_id,
        "nominator_id": our_id,
        "timestamp": timestamp,
        "available_liquidity_sats": available_liquidity,
        "quality_score": quality_score,
        "has_existing_channel": has_existing,
        "channel_count": channel_count,
    }
    signing_message = get_expansion_nominate_signing_payload(signing_payload)

    # Sign the message with our node key
    try:
        sig_result = safe_plugin.rpc.signmessage(signing_message)
        signature = sig_result['zbase']
    except Exception as e:
        safe_plugin.log(f"cl-hive: Failed to sign nomination: {e}", level='error')
        return 0

    msg = create_expansion_nominate(
        round_id=round_id,
        target_peer_id=target_peer_id,
        nominator_id=our_id,
        timestamp=timestamp,
        signature=signature,
        available_liquidity_sats=available_liquidity,
        quality_score=quality_score,
        has_existing_channel=has_existing,
        channel_count=channel_count,
        reason="auto_nominate"
    )

    sent = _broadcast_to_members(msg)
    safe_plugin.log(
        f"cl-hive: [BROADCAST] Sent signed nomination for round {round_id[:8]}... "
        f"target={target_peer_id[:16]}... to {sent} members",
        level='info'
    )

    return sent


def _broadcast_expansion_elect(round_id: str, target_peer_id: str, elected_id: str,
                                channel_size_sats: int = 0, quality_score: float = 0.5,
                                nomination_count: int = 0) -> int:
    """
    Broadcast an EXPANSION_ELECT message to all hive members.

    SECURITY: The message is signed by the coordinator (us) to prevent
    election spoofing by malicious hive members.

    Args:
        round_id: The cooperative expansion round ID
        target_peer_id: The target peer for the expansion
        elected_id: The elected member who should open the channel
        channel_size_sats: Recommended channel size
        quality_score: Target's quality score
        nomination_count: Number of nominations received

    Returns:
        Number of members message was sent to
    """
    if not safe_plugin or not database:
        return 0

    try:
        coordinator_id = safe_plugin.rpc.getinfo().get("id")
    except Exception:
        return 0

    import time
    timestamp = int(time.time())

    # Build payload for signing (SECURITY: sign before sending)
    signing_payload = {
        "round_id": round_id,
        "target_peer_id": target_peer_id,
        "elected_id": elected_id,
        "coordinator_id": coordinator_id,
        "timestamp": timestamp,
        "channel_size_sats": channel_size_sats,
        "quality_score": quality_score,
        "nomination_count": nomination_count,
    }
    signing_message = get_expansion_elect_signing_payload(signing_payload)

    # Sign the message with our node key
    try:
        sig_result = safe_plugin.rpc.signmessage(signing_message)
        signature = sig_result['zbase']
    except Exception as e:
        safe_plugin.log(f"cl-hive: Failed to sign election: {e}", level='error')
        return 0

    msg = create_expansion_elect(
        round_id=round_id,
        target_peer_id=target_peer_id,
        elected_id=elected_id,
        coordinator_id=coordinator_id,
        timestamp=timestamp,
        signature=signature,
        channel_size_sats=channel_size_sats,
        quality_score=quality_score,
        nomination_count=nomination_count,
        reason="elected_by_coordinator"
    )

    sent = _broadcast_to_members(msg)
    if sent > 0:
        safe_plugin.log(
            f"cl-hive: Broadcast signed expansion election for round {round_id[:8]}... "
            f"elected={elected_id[:16]}... to {sent} members",
            level='info'
        )

    return sent


def _broadcast_expansion_decline(round_id: str, reason: str) -> int:
    """
    Broadcast an EXPANSION_DECLINE message to all hive members (Phase 8).

    Called when we (the elected member) cannot open the channel due to
    insufficient funds, high feerate, or other reasons. This triggers
    fallback to the next ranked candidate.

    SECURITY: The message is signed by the decliner (us) to prevent
    spoofing decline messages.

    Args:
        round_id: The cooperative expansion round ID
        reason: Why we're declining (insufficient_funds, feerate_high, etc.)

    Returns:
        Number of members message was sent to
    """
    if not safe_plugin or not database:
        return 0

    try:
        decliner_id = safe_plugin.rpc.getinfo().get("id")
    except Exception:
        return 0

    import time
    timestamp = int(time.time())

    # Build payload for signing (SECURITY: sign before sending)
    signing_payload = {
        "round_id": round_id,
        "decliner_id": decliner_id,
        "reason": reason,
        "timestamp": timestamp,
    }
    signing_message = get_expansion_decline_signing_payload(signing_payload)

    # Sign the message with our node key
    try:
        sig_result = safe_plugin.rpc.signmessage(signing_message)
        signature = sig_result['zbase']
    except Exception as e:
        safe_plugin.log(f"cl-hive: Failed to sign decline: {e}", level='error')
        return 0

    msg = create_expansion_decline(
        round_id=round_id,
        decliner_id=decliner_id,
        reason=reason,
        timestamp=timestamp,
        signature=signature,
    )

    sent = _broadcast_to_members(msg)
    if sent > 0:
        safe_plugin.log(
            f"cl-hive: Broadcast expansion decline for round {round_id[:8]}... "
            f"(reason={reason}) to {sent} members",
            level='info'
        )

    return sent


def handle_expansion_nominate(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle EXPANSION_NOMINATE message from another hive member.

    This message indicates a member is interested in opening a channel
    to a target peer during a cooperative expansion round.

    SECURITY: Verifies cryptographic signature from the nominator.
    """
    plugin.log(
        f"cl-hive: [NOMINATE] Received from {peer_id[:16]}... "
        f"round={payload.get('round_id', '')[:8]}... "
        f"nominator={payload.get('nominator_id', '')[:16]}...",
        level='info'
    )

    if not coop_expansion or not database:
        plugin.log("cl-hive: [NOMINATE] coop_expansion or database not initialized", level='warn')
        return {"result": "continue"}

    if not validate_expansion_nominate(payload):
        plugin.log(f"cl-hive: [NOMINATE] Invalid payload from {peer_id[:16]}...", level='warn')
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: [NOMINATE] Rejected - {peer_id[:16]}... not a member or banned", level='info')
        return {"result": "continue"}

    # SECURITY: Verify the cryptographic signature
    nominator_id = payload.get("nominator_id", "")
    signature = payload.get("signature", "")
    signing_message = get_expansion_nominate_signing_payload(payload)

    try:
        verify_result = plugin.rpc.checkmessage(signing_message, signature)
        if not verify_result.get("verified", False):
            plugin.log(
                f"cl-hive: [NOMINATE] Signature verification failed for {nominator_id[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
        # Verify the signature is from the claimed nominator
        recovered_pubkey = verify_result.get("pubkey", "")
        if recovered_pubkey != nominator_id:
            plugin.log(
                f"cl-hive: [NOMINATE] Signature mismatch: claimed={nominator_id[:16]}... "
                f"actual={recovered_pubkey[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: [NOMINATE] Signature verification error: {e}", level='warn')
        return {"result": "continue"}

    # Process the nomination
    result = coop_expansion.handle_nomination(peer_id, payload)

    plugin.log(
        f"cl-hive: [NOMINATE] Processed: success={result.get('success')}, "
        f"joined={result.get('joined')}, round={result.get('round_id', '')[:8]}...",
        level='info'
    )

    # If we joined a new round and added our nomination, broadcast it to other members
    # This ensures all members' nominations propagate across the network
    if result.get('joined') and result.get('success'):
        round_id = result.get('round_id', '')
        target_peer_id = payload.get('target_peer_id', '')
        if round_id and target_peer_id:
            plugin.log(
                f"cl-hive: [NOMINATE] Re-broadcasting our nomination for round {round_id[:8]}...",
                level='info'
            )
            _broadcast_expansion_nomination(round_id, target_peer_id)

    return {"result": "continue", "nomination_result": result}


def handle_expansion_elect(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle EXPANSION_ELECT message announcing the winner of an expansion round.

    If we are the elected member, we should proceed to open the channel.

    SECURITY: Verifies cryptographic signature from the coordinator.
    """
    if not coop_expansion or not database:
        return {"result": "continue"}

    if not validate_expansion_elect(payload):
        plugin.log(f"cl-hive: Invalid EXPANSION_ELECT from {peer_id[:16]}...", level='warn')
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: EXPANSION_ELECT from non-member {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # SECURITY: Verify the cryptographic signature from coordinator
    coordinator_id = payload.get("coordinator_id", "")
    signature = payload.get("signature", "")
    signing_message = get_expansion_elect_signing_payload(payload)

    try:
        verify_result = plugin.rpc.checkmessage(signing_message, signature)
        if not verify_result.get("verified", False):
            plugin.log(
                f"cl-hive: [ELECT] Signature verification failed for coordinator {coordinator_id[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
        # Verify the signature is from the claimed coordinator
        recovered_pubkey = verify_result.get("pubkey", "")
        if recovered_pubkey != coordinator_id:
            plugin.log(
                f"cl-hive: [ELECT] Signature mismatch: claimed={coordinator_id[:16]}... "
                f"actual={recovered_pubkey[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
        # Verify the coordinator is a hive member
        coordinator_member = database.get_member(coordinator_id)
        if not coordinator_member or database.is_banned(coordinator_id):
            plugin.log(
                f"cl-hive: [ELECT] Coordinator {coordinator_id[:16]}... not a member or banned",
                level='warn'
            )
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: [ELECT] Signature verification error: {e}", level='warn')
        return {"result": "continue"}

    plugin.log(
        f"cl-hive: [ELECT] Verified election from coordinator {coordinator_id[:16]}...",
        level='debug'
    )

    # Process the election
    result = coop_expansion.handle_elect(peer_id, payload)

    elected_id = payload.get("elected_id", "")
    target_peer_id = payload.get("target_peer_id", "")
    channel_size = payload.get("channel_size_sats", 0)

    # Check if we were elected
    if result.get("action") == "open_channel":
        plugin.log(
            f"cl-hive: We were elected to open channel to {target_peer_id[:16]}... "
            f"(size={channel_size})",
            level='info'
        )

        # Queue the channel open via pending actions
        if database and config:
            cfg = config.snapshot()
            proposed_size = channel_size or cfg.planner_default_channel_sats

            # Check affordability before queuing
            capped_size, insufficient, was_capped = _cap_channel_size_to_budget(
                proposed_size, cfg, f"EXPANSION_ELECT for {target_peer_id[:16]}..."
            )
            if insufficient:
                plugin.log(
                    f"cl-hive: [ELECT] Declining election: insufficient funds to open channel "
                    f"(proposed={proposed_size}, min={cfg.planner_min_channel_sats})",
                    level='info'
                )
                # Phase 8: Broadcast decline to trigger fallback
                round_id = payload.get("round_id", "")
                if round_id:
                    _broadcast_expansion_decline(round_id, "insufficient_funds")
                return {"result": "declined", "reason": "insufficient_funds"}
            if was_capped:
                plugin.log(
                    f"cl-hive: [ELECT] Capping channel size from {proposed_size} to {capped_size}",
                    level='info'
                )

            action_id = database.add_pending_action(
                action_type="channel_open",
                payload={
                    "target": target_peer_id,
                    "amount_sats": capped_size,
                    "source": "cooperative_expansion",
                    "round_id": payload.get("round_id", ""),
                    "reason": "Elected by hive for cooperative expansion"
                },
                expires_hours=24
            )
            plugin.log(f"cl-hive: Queued channel open to {target_peer_id[:16]}... (action_id={action_id})", level='info')
    else:
        plugin.log(
            f"cl-hive: {elected_id[:16]}... elected for round {payload.get('round_id', '')[:8]}... "
            f"(not us)",
            level='debug'
        )

    return {"result": "continue", "election_result": result}


def handle_expansion_decline(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle EXPANSION_DECLINE message from the elected member (Phase 8).

    When the elected member cannot afford the channel open or has another
    reason to decline, this message triggers fallback to the next candidate.

    SECURITY: Verifies cryptographic signature from the decliner.
    """
    if not coop_expansion or not database:
        return {"result": "continue"}

    if not validate_expansion_decline(payload):
        plugin.log(f"cl-hive: Invalid EXPANSION_DECLINE from {peer_id[:16]}...", level='warn')
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: EXPANSION_DECLINE from non-member {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # SECURITY: Verify the cryptographic signature from decliner
    decliner_id = payload.get("decliner_id", "")
    signature = payload.get("signature", "")
    signing_message = get_expansion_decline_signing_payload(payload)

    try:
        verify_result = plugin.rpc.checkmessage(signing_message, signature)
        if not verify_result.get("verified", False):
            plugin.log(
                f"cl-hive: [DECLINE] Signature verification failed for decliner {decliner_id[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
        # Verify the signature is from the claimed decliner
        recovered_pubkey = verify_result.get("pubkey", "")
        if recovered_pubkey != decliner_id:
            plugin.log(
                f"cl-hive: [DECLINE] Signature mismatch: claimed={decliner_id[:16]}... "
                f"actual={recovered_pubkey[:16]}...",
                level='warn'
            )
            return {"result": "continue"}
        # Verify the decliner is a hive member
        decliner_member = database.get_member(decliner_id)
        if not decliner_member or database.is_banned(decliner_id):
            plugin.log(
                f"cl-hive: [DECLINE] Decliner {decliner_id[:16]}... not a member or banned",
                level='warn'
            )
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: [DECLINE] Signature verification error: {e}", level='warn')
        return {"result": "continue"}

    round_id = payload.get("round_id", "")
    reason = payload.get("reason", "unknown")
    plugin.log(
        f"cl-hive: [DECLINE] Verified decline from {decliner_id[:16]}... "
        f"for round {round_id[:8]}... (reason={reason})",
        level='info'
    )

    # Process the decline - this may elect a fallback candidate
    result = coop_expansion.handle_decline(peer_id, payload)

    if result.get("action") == "fallback_elected":
        # A fallback candidate was elected
        new_elected = result.get("elected_id", "")
        our_id = None
        try:
            our_id = plugin.rpc.getinfo().get("id")
        except Exception:
            pass

        if new_elected == our_id:
            # We are the fallback candidate
            target_peer_id = result.get("target_peer_id", "")
            channel_size = result.get("channel_size_sats", 0)
            plugin.log(
                f"cl-hive: We are the fallback candidate for round {round_id[:8]}... "
                f"(target={target_peer_id[:16]}...)",
                level='info'
            )

            # Queue the channel open via pending actions
            if database and config:
                cfg = config.snapshot()
                proposed_size = channel_size or cfg.planner_default_channel_sats

                # Check affordability before queuing
                capped_size, insufficient, was_capped = _cap_channel_size_to_budget(
                    proposed_size, cfg, f"FALLBACK_ELECT for {target_peer_id[:16]}..."
                )
                if insufficient:
                    plugin.log(
                        f"cl-hive: [FALLBACK] Also declining: insufficient funds",
                        level='info'
                    )
                    # Broadcast our own decline
                    _broadcast_expansion_decline(round_id, "insufficient_funds")
                    return {"result": "declined", "reason": "insufficient_funds"}

                action_id = database.add_pending_action(
                    action_type="channel_open",
                    payload={
                        "target": target_peer_id,
                        "amount_sats": capped_size,
                        "source": "cooperative_expansion_fallback",
                        "round_id": round_id,
                        "reason": f"Fallback elected after {result.get('decline_count', 1)} decline(s)"
                    },
                    expires_hours=24
                )
                plugin.log(
                    f"cl-hive: Queued fallback channel open to {target_peer_id[:16]}... "
                    f"(action_id={action_id})",
                    level='info'
                )
        else:
            plugin.log(
                f"cl-hive: [DECLINE] Fallback elected {new_elected[:16]}... (not us)",
                level='debug'
            )

    elif result.get("action") == "cancelled":
        plugin.log(
            f"cl-hive: [DECLINE] Round {round_id[:8]}... cancelled: {result.get('reason', 'unknown')}",
            level='info'
        )

    return {"result": "continue", "decline_result": result}


# =============================================================================
# PHASE 7: FEE INTELLIGENCE MESSAGE HANDLERS
# =============================================================================

def handle_fee_intelligence(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle FEE_INTELLIGENCE message from a hive member.

    Validates signature and stores the fee observation for aggregation.
    """
    if not fee_intel_mgr or not database:
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: FEE_INTELLIGENCE from non-member {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Delegate to fee intelligence manager
    result = fee_intel_mgr.handle_fee_intelligence(peer_id, payload, safe_plugin.rpc)

    if result.get("success"):
        plugin.log(
            f"cl-hive: Stored fee intelligence from {peer_id[:16]}... "
            f"for {payload.get('target_peer_id', '')[:16]}...",
            level='debug'
        )
    elif result.get("error"):
        plugin.log(
            f"cl-hive: FEE_INTELLIGENCE rejected from {peer_id[:16]}...: {result.get('error')}",
            level='debug'
        )

    return {"result": "continue"}


def handle_health_report(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HEALTH_REPORT message from a hive member.

    Used for NNLB (No Node Left Behind) coordination.
    """
    if not fee_intel_mgr or not database:
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: HEALTH_REPORT from non-member {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Delegate to fee intelligence manager
    result = fee_intel_mgr.handle_health_report(peer_id, payload, safe_plugin.rpc)

    if result.get("success"):
        tier = result.get("tier", "unknown")
        plugin.log(
            f"cl-hive: Stored health report from {peer_id[:16]}... (tier={tier})",
            level='debug'
        )
    elif result.get("error"):
        plugin.log(
            f"cl-hive: HEALTH_REPORT rejected from {peer_id[:16]}...: {result.get('error')}",
            level='debug'
        )

    return {"result": "continue"}


def handle_liquidity_need(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle LIQUIDITY_NEED message from a hive member.

    Used for cooperative rebalancing coordination.
    """
    if not liquidity_coord or not database:
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: LIQUIDITY_NEED from non-member {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Delegate to liquidity coordinator
    result = liquidity_coord.handle_liquidity_need(peer_id, payload, safe_plugin.rpc)

    if result.get("success"):
        plugin.log(
            f"cl-hive: Stored liquidity need from {peer_id[:16]}...",
            level='debug'
        )
    elif result.get("error"):
        plugin.log(
            f"cl-hive: LIQUIDITY_NEED rejected from {peer_id[:16]}...: {result.get('error')}",
            level='debug'
        )

    return {"result": "continue"}


def handle_route_probe(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle ROUTE_PROBE message from a hive member.

    Used for collective routing intelligence.
    """
    if not routing_map or not database:
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: ROUTE_PROBE from non-member {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Delegate to routing map
    result = routing_map.handle_route_probe(peer_id, payload, safe_plugin.rpc)

    if result.get("success"):
        plugin.log(
            f"cl-hive: Stored route probe from {peer_id[:16]}...",
            level='debug'
        )
    elif result.get("error"):
        plugin.log(
            f"cl-hive: ROUTE_PROBE rejected from {peer_id[:16]}...: {result.get('error')}",
            level='debug'
        )

    return {"result": "continue"}


def handle_peer_reputation(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle PEER_REPUTATION message from a hive member.

    Used for collective peer reputation tracking.
    """
    if not peer_reputation_mgr or not database:
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: PEER_REPUTATION from non-member {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Delegate to peer reputation manager
    result = peer_reputation_mgr.handle_peer_reputation(peer_id, payload, safe_plugin.rpc)

    if result.get("success"):
        plugin.log(
            f"cl-hive: Stored peer reputation from {peer_id[:16]}...",
            level='debug'
        )
    elif result.get("error"):
        plugin.log(
            f"cl-hive: PEER_REPUTATION rejected from {peer_id[:16]}...: {result.get('error')}",
            level='debug'
        )

    return {"result": "continue"}


def handle_settlement_offer(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle SETTLEMENT_OFFER message from a hive member.

    Stores the member's BOLT12 offer for use in settlement calculations.
    """
    if not settlement_mgr or not database:
        return {"result": "continue"}

    # Verify sender is a hive member and not banned
    sender = database.get_member(peer_id)
    if not sender or database.is_banned(peer_id):
        plugin.log(f"cl-hive: SETTLEMENT_OFFER from non-member {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Extract payload fields
    offer_peer_id = payload.get("peer_id")
    bolt12_offer = payload.get("bolt12_offer")
    timestamp = payload.get("timestamp")
    signature = payload.get("signature")

    # Validate required fields
    if not all([offer_peer_id, bolt12_offer, signature]):
        plugin.log(f"cl-hive: SETTLEMENT_OFFER missing required fields from {peer_id[:16]}...", level='debug')
        return {"result": "continue"}

    # Verify the offer peer_id matches the sender (you can only broadcast your own offer)
    if offer_peer_id != peer_id:
        plugin.log(f"cl-hive: SETTLEMENT_OFFER peer_id mismatch from {peer_id[:16]}...", level='warn')
        return {"result": "continue"}

    # Verify the signature
    signing_payload = get_settlement_offer_signing_payload(offer_peer_id, bolt12_offer)
    try:
        verify_result = safe_plugin.rpc.call("checkmessage", {
            "message": signing_payload,
            "zbase": signature,
            "pubkey": offer_peer_id
        })
        if not verify_result.get("verified"):
            plugin.log(f"cl-hive: SETTLEMENT_OFFER invalid signature from {peer_id[:16]}...", level='warn')
            return {"result": "continue"}
    except Exception as e:
        plugin.log(f"cl-hive: SETTLEMENT_OFFER signature check failed: {e}", level='warn')
        return {"result": "continue"}

    # Store the offer
    result = settlement_mgr.register_offer(offer_peer_id, bolt12_offer)

    if "error" not in result:
        plugin.log(f"cl-hive: Stored settlement offer from {peer_id[:16]}...")
    else:
        plugin.log(f"cl-hive: Failed to store settlement offer: {result.get('error')}", level='debug')

    return {"result": "continue"}


def _broadcast_settlement_offer(peer_id: str, bolt12_offer: str) -> int:
    """
    Broadcast a settlement offer to all hive members.

    Args:
        peer_id: The member's node public key
        bolt12_offer: The BOLT12 offer string

    Returns:
        Number of members the message was sent to
    """
    if not safe_plugin or not handshake_mgr:
        return 0

    timestamp = int(time.time())

    # Sign the offer
    signing_payload = get_settlement_offer_signing_payload(peer_id, bolt12_offer)
    try:
        sign_result = safe_plugin.rpc.call("signmessage", {"message": signing_payload})
        signature = sign_result.get("zbase")
        if not signature:
            safe_plugin.log("cl-hive: Failed to sign settlement offer", level='warn')
            return 0
    except Exception as e:
        safe_plugin.log(f"cl-hive: Failed to sign settlement offer: {e}", level='warn')
        return 0

    # Create the message
    msg = create_settlement_offer(peer_id, bolt12_offer, timestamp, signature)

    # Broadcast to all members
    sent = _broadcast_to_members(msg)
    if sent > 0:
        safe_plugin.log(f"cl-hive: Broadcast settlement offer to {sent} member(s)")

    return sent


# =============================================================================
# PHASE 3: INTENT MONITOR BACKGROUND THREAD
# =============================================================================

def intent_monitor_loop():
    """
    Background thread that monitors pending intents and commits them.
    
    Runs every 5 seconds and:
    1. Checks for intents where hold period has elapsed
    2. Commits them if no abort signal was received
    3. Cleans up expired/stale intents
    """
    MONITOR_INTERVAL = 5  # seconds
    
    while not shutdown_event.is_set():
        try:
            if intent_mgr and database and config:
                process_ready_intents()
                intent_mgr.cleanup_expired_intents()
        except Exception as e:
            if safe_plugin:
                safe_plugin.log(f"Intent monitor error: {e}", level='warn')
        
        # Wait for next iteration or shutdown
        shutdown_event.wait(MONITOR_INTERVAL)


def process_ready_intents():
    """
    Process intents that are ready to commit.
    
    An intent is ready if:
    - Status is 'pending'
    - Current time > timestamp + hold_seconds
    """
    if not intent_mgr or not database or not config:
        return
    
    ready_intents = database.get_pending_intents_ready(config.intent_hold_seconds)

    for intent_row in ready_intents:
        intent_id = intent_row.get('id')
        intent_type = intent_row.get('intent_type')
        target = intent_row.get('target')

        # SECURITY (Issue #12): Check governance mode BEFORE committing
        # to prevent state inconsistency where intents are COMMITTED but never executed
        # In advisor mode, intents wait for AI/human approval
        # In failsafe mode, only emergency actions auto-execute (not intents)
        if config.governance_mode != "failsafe":
            if safe_plugin:
                safe_plugin.log(
                    f"cl-hive: Intent {intent_id} ready but not committing "
                    f"(mode={config.governance_mode})",
                    level='debug'
                )
            continue

        # Commit the intent (only in failsafe mode for backwards compatibility)
        if intent_mgr.commit_intent(intent_id):
            if safe_plugin:
                safe_plugin.log(f"cl-hive: Committed intent {intent_id}: {intent_type} -> {target[:16]}...")

            # Execute the action (callback registry)
            intent_mgr.execute_committed_intent(intent_row)


# =============================================================================
# PHASE 5: MEMBERSHIP MAINTENANCE LOOP
# =============================================================================

def membership_maintenance_loop():
    """
    Periodic pruning of membership-related data.

    Runs hourly to clean up:
    - Old contribution records (> 45 days)
    - Old vouches (> VOUCH_TTL)
    - Stale presence data
    - Old planner logs (> 30 days)
    - Expired/completed pending actions (> 7 days)
    """
    MAINTENANCE_INTERVAL = 3600  # seconds
    PRESENCE_WINDOW_SECONDS = 30 * 86400

    while not shutdown_event.is_set():
        try:
            if database:
                # Phase 5: Membership data pruning
                database.prune_old_contributions(older_than_days=45)
                database.prune_old_vouches(older_than_seconds=VOUCH_TTL_SECONDS)
                database.prune_presence(window_seconds=PRESENCE_WINDOW_SECONDS)

                # Phase 9: Planner and governance data pruning
                database.cleanup_expired_actions()  # Mark expired as 'expired'
                database.prune_planner_logs(older_than_days=30)
                database.prune_old_actions(older_than_days=7)
        except Exception as e:
            if safe_plugin:
                safe_plugin.log(f"Membership maintenance error: {e}", level='warn')

        shutdown_event.wait(MAINTENANCE_INTERVAL)


# =============================================================================
# PHASE 6: PLANNER BACKGROUND LOOP
# =============================================================================

# Security: Hard minimum interval to prevent Intent Storms
PLANNER_MIN_INTERVAL_SECONDS = 300  # 5 minutes minimum

# Jitter range to prevent all Hive nodes waking simultaneously
PLANNER_JITTER_SECONDS = 300  # ±5 minutes


def planner_loop():
    """
    Background thread that runs Planner cycles for topology optimization.

    Runs periodically to:
    1. Detect saturated targets and issue clboss-ignore
    2. Release ignores when saturation drops below threshold
    3. (If enabled) Propose channel expansions to underserved targets

    Security:
    - Enforces hard minimum interval (300s) to prevent Intent Storms
    - Adds random jitter to prevent simultaneous wake-up across swarm
    - Respects shutdown_event for graceful termination
    """
    # Run first cycle immediately on startup (for testing)
    first_run = True

    while not shutdown_event.is_set():
        try:
            if planner and config:
                # Take config snapshot at cycle start (determinism)
                cfg_snapshot = config.snapshot()
                run_id = secrets.token_hex(8)

                if safe_plugin:
                    safe_plugin.log(f"cl-hive: Planner cycle starting (run_id={run_id})")

                # Run the planner cycle
                decisions = planner.run_cycle(
                    cfg_snapshot,
                    shutdown_event=shutdown_event,
                    run_id=run_id
                )

                if safe_plugin:
                    safe_plugin.log(
                        f"cl-hive: Planner cycle complete: {len(decisions)} decisions"
                    )

                # Clean up expired expansion rounds
                if coop_expansion:
                    cleaned = coop_expansion.cleanup_expired_rounds()
                    if cleaned > 0 and safe_plugin:
                        safe_plugin.log(
                            f"cl-hive: Cleaned up {cleaned} expired expansion rounds"
                        )
        except Exception as e:
            if safe_plugin:
                safe_plugin.log(f"Planner loop error: {e}", level='warn')

        # Calculate next sleep interval
        if first_run:
            first_run = False

        if config:
            # SECURITY: Enforce hard minimum interval
            interval = max(config.planner_interval, PLANNER_MIN_INTERVAL_SECONDS)

            # Add random jitter (±5 minutes) to prevent synchronization
            jitter = secrets.randbelow(PLANNER_JITTER_SECONDS * 2) - PLANNER_JITTER_SECONDS
            sleep_time = interval + jitter
        else:
            sleep_time = 3600  # Default 1 hour if config unavailable

        # Wait for next cycle or shutdown
        shutdown_event.wait(sleep_time)


# =============================================================================
# PHASE 7: FEE INTELLIGENCE BACKGROUND LOOP
# =============================================================================

# Fee intelligence loop interval (1 hour default)
FEE_INTELLIGENCE_INTERVAL = 3600

# Health report broadcast interval (1 hour)
HEALTH_REPORT_INTERVAL = 3600

# Fee intelligence cleanup interval (keep 7 days)
FEE_INTELLIGENCE_MAX_AGE_HOURS = 168


def fee_intelligence_loop():
    """
    Background thread for cooperative fee coordination.

    Runs periodically to:
    1. Collect and broadcast our fee observations to hive members
    2. Aggregate received fee intelligence into peer profiles
    3. Broadcast our health report for NNLB coordination
    4. Clean up old fee intelligence records
    """
    # Wait for initialization
    shutdown_event.wait(60)

    while not shutdown_event.is_set():
        try:
            if not fee_intel_mgr or not database or not safe_plugin or not our_pubkey:
                shutdown_event.wait(60)
                continue

            # Step 1: Collect and broadcast our fee intelligence
            _broadcast_our_fee_intelligence()

            # Step 2: Aggregate all received fee intelligence
            try:
                updated = fee_intel_mgr.aggregate_fee_profiles()
                if updated > 0:
                    safe_plugin.log(
                        f"cl-hive: Aggregated {updated} peer fee profiles",
                        level='debug'
                    )
            except Exception as e:
                safe_plugin.log(f"cl-hive: Fee aggregation error: {e}", level='warn')

            # Step 3: Broadcast our health report
            _broadcast_health_report()

            # Step 4: Cleanup old records
            try:
                deleted = database.cleanup_old_fee_intelligence(FEE_INTELLIGENCE_MAX_AGE_HOURS)
                if deleted > 0:
                    safe_plugin.log(
                        f"cl-hive: Cleaned up {deleted} old fee intelligence records",
                        level='debug'
                    )
            except Exception as e:
                safe_plugin.log(f"cl-hive: Fee intelligence cleanup error: {e}", level='warn')

            # Step 5: Broadcast liquidity needs
            _broadcast_liquidity_needs()

            # Step 6: Cleanup old liquidity needs
            try:
                deleted_needs = database.cleanup_old_liquidity_needs(max_age_hours=24)
                if deleted_needs > 0:
                    safe_plugin.log(
                        f"cl-hive: Cleaned up {deleted_needs} old liquidity needs",
                        level='debug'
                    )
            except Exception as e:
                safe_plugin.log(f"cl-hive: Liquidity needs cleanup error: {e}", level='warn')

            # Step 7: Cleanup old route probes
            try:
                if routing_map:
                    # Clean database
                    deleted_probes = database.cleanup_old_route_probes(max_age_hours=24)
                    if deleted_probes > 0:
                        safe_plugin.log(
                            f"cl-hive: Cleaned up {deleted_probes} old route probes from database",
                            level='debug'
                        )
                    # Clean in-memory stats
                    cleaned_paths = routing_map.cleanup_stale_data()
                    if cleaned_paths > 0:
                        safe_plugin.log(
                            f"cl-hive: Cleaned up {cleaned_paths} stale paths from routing map",
                            level='debug'
                        )
            except Exception as e:
                safe_plugin.log(f"cl-hive: Route probe cleanup error: {e}", level='warn')

            # Step 9: Cleanup old peer reputation (Phase 5 - Advanced Cooperation)
            try:
                if peer_reputation_mgr:
                    # Clean database
                    deleted_reps = database.cleanup_old_peer_reputation(max_age_hours=168)
                    if deleted_reps > 0:
                        safe_plugin.log(
                            f"cl-hive: Cleaned up {deleted_reps} old peer reputation records",
                            level='debug'
                        )
                    # Clean in-memory aggregations
                    cleaned_reps = peer_reputation_mgr.cleanup_stale_data()
                    if cleaned_reps > 0:
                        safe_plugin.log(
                            f"cl-hive: Cleaned up {cleaned_reps} stale peer reputations",
                            level='debug'
                        )
            except Exception as e:
                safe_plugin.log(f"cl-hive: Peer reputation cleanup error: {e}", level='warn')

        except Exception as e:
            if safe_plugin:
                safe_plugin.log(f"cl-hive: Fee intelligence loop error: {e}", level='warn')

        # Wait for next cycle
        shutdown_event.wait(FEE_INTELLIGENCE_INTERVAL)


def _broadcast_our_fee_intelligence():
    """
    Collect fee observations from our channels and broadcast to hive.

    Gathers fee and performance data for each external peer we have
    channels with and broadcasts FEE_INTELLIGENCE messages.
    """
    if not fee_intel_mgr or not safe_plugin or not database or not our_pubkey:
        return

    try:
        # Get our channels
        funds = safe_plugin.rpc.listfunds()
        channels = funds.get("channels", [])

        # Get list of hive members (to exclude from external peer reporting)
        members = database.get_all_members()
        member_ids = {m.get("peer_id") for m in members}

        # Get forwarding stats if available
        try:
            forwards = safe_plugin.rpc.listforwards(status="settled")
            forwards_list = forwards.get("forwards", [])
        except Exception:
            forwards_list = []

        # Build forward stats by peer
        peer_forwards = {}
        seven_days_ago = int(time.time()) - (7 * 24 * 3600)
        for fwd in forwards_list:
            # Filter to last 7 days
            received_time = fwd.get("received_time", 0)
            if received_time < seven_days_ago:
                continue

            out_channel = fwd.get("out_channel")
            if out_channel:
                if out_channel not in peer_forwards:
                    peer_forwards[out_channel] = {
                        "count": 0,
                        "volume_msat": 0,
                        "fee_msat": 0
                    }
                peer_forwards[out_channel]["count"] += 1
                peer_forwards[out_channel]["volume_msat"] += fwd.get("out_msat", 0)
                peer_forwards[out_channel]["fee_msat"] += fwd.get("fee_msat", 0)

        # Collect fee intelligence for each external peer
        broadcast_count = 0
        for channel in channels:
            if channel.get("state") != "CHANNELD_NORMAL":
                continue

            peer_id = channel.get("peer_id")
            if not peer_id or peer_id in member_ids:
                # Skip hive members - only report on external peers
                continue

            short_channel_id = channel.get("short_channel_id")
            if not short_channel_id:
                continue

            # Get channel capacity and balance
            amount_msat = channel.get("amount_msat", 0)
            our_amount_msat = channel.get("our_amount_msat", 0)
            capacity_sats = amount_msat // 1000
            available_sats = our_amount_msat // 1000

            if capacity_sats == 0:
                continue

            utilization_pct = available_sats / capacity_sats if capacity_sats > 0 else 0

            # Determine flow direction based on balance
            if utilization_pct > 0.7:
                flow_direction = "source"  # We have excess, liquidity flows out
            elif utilization_pct < 0.3:
                flow_direction = "sink"  # We need liquidity, flows in
            else:
                flow_direction = "balanced"

            # Get forward stats for this channel
            stats = peer_forwards.get(short_channel_id, {})
            forward_count = stats.get("count", 0)
            forward_volume_sats = stats.get("volume_msat", 0) // 1000
            revenue_sats = stats.get("fee_msat", 0) // 1000

            # Get our fee rate for this channel (simplified - would need listpeerchannels)
            our_fee_ppm = 100  # Default, would query actual fee

            # Create and broadcast fee intelligence message
            try:
                msg = fee_intel_mgr.create_fee_intelligence_message(
                    target_peer_id=peer_id,
                    our_fee_ppm=our_fee_ppm,
                    their_fee_ppm=0,  # Would need to look up
                    forward_count=forward_count,
                    forward_volume_sats=forward_volume_sats,
                    revenue_sats=revenue_sats,
                    flow_direction=flow_direction,
                    utilization_pct=utilization_pct,
                    rpc=safe_plugin.rpc,
                    days_observed=7
                )

                if msg:
                    # Broadcast to all hive members
                    for member in members:
                        member_id = member.get("peer_id")
                        if not member_id or member_id == our_pubkey:
                            continue
                        try:
                            safe_plugin.rpc.call("sendcustommsg", {
                                "node_id": member_id,
                                "msg": msg.hex()
                            })
                            broadcast_count += 1
                        except Exception:
                            pass  # Peer might be offline

            except Exception as e:
                safe_plugin.log(
                    f"cl-hive: Failed to create fee intelligence for {peer_id[:16]}...: {e}",
                    level='debug'
                )

        if broadcast_count > 0:
            safe_plugin.log(
                f"cl-hive: Broadcast fee intelligence ({broadcast_count} messages)",
                level='debug'
            )

    except Exception as e:
        if safe_plugin:
            safe_plugin.log(f"cl-hive: Fee intelligence broadcast error: {e}", level='warn')


def _broadcast_health_report():
    """
    Calculate and broadcast our health report for NNLB coordination.
    """
    if not fee_intel_mgr or not safe_plugin or not database or not our_pubkey:
        return

    try:
        # Get our channel data
        funds = safe_plugin.rpc.listfunds()
        channels = funds.get("channels", [])

        capacity_sats = sum(
            ch.get("amount_msat", 0) // 1000
            for ch in channels if ch.get("state") == "CHANNELD_NORMAL"
        )
        available_sats = sum(
            ch.get("our_amount_msat", 0) // 1000
            for ch in channels if ch.get("state") == "CHANNELD_NORMAL"
        )
        channel_count = len([ch for ch in channels if ch.get("state") == "CHANNELD_NORMAL"])

        # Calculate actual daily revenue from forwarding stats
        daily_revenue_sats = 0
        try:
            forwards = safe_plugin.rpc.listforwards(status="settled")
            forwards_list = forwards.get("forwards", [])
            one_day_ago = time.time() - (24 * 3600)
            daily_revenue_sats = sum(
                fwd.get("fee_msat", 0) // 1000
                for fwd in forwards_list
                if fwd.get("received_time", 0) > one_day_ago
            )
        except Exception:
            pass

        # Get hive averages for comparison
        all_health = database.get_all_member_health()
        if all_health:
            hive_avg_capacity = sum(
                h.get("capacity_score", 50) for h in all_health
            ) / len(all_health) * 200000
            # Estimate hive average revenue from revenue scores
            hive_avg_revenue = sum(
                h.get("revenue_score", 50) for h in all_health
            ) / len(all_health) * 20  # Scale factor for reasonable default
        else:
            hive_avg_capacity = 10_000_000
            hive_avg_revenue = 1000  # Default 1000 sats/day

        # Calculate our health
        health = fee_intel_mgr.calculate_our_health(
            capacity_sats=capacity_sats,
            available_sats=available_sats,
            channel_count=channel_count,
            daily_revenue_sats=daily_revenue_sats,
            hive_avg_capacity=int(hive_avg_capacity),
            hive_avg_revenue=int(max(1, hive_avg_revenue))  # Avoid division by zero
        )

        # Store our own health record
        database.update_member_health(
            peer_id=our_pubkey,
            overall_health=health["overall_health"],
            capacity_score=health["capacity_score"],
            revenue_score=health["revenue_score"],
            connectivity_score=health["connectivity_score"],
            tier=health["tier"],
            needs_help=health["needs_help"],
            can_help_others=health["can_help_others"],
            needs_inbound=available_sats < capacity_sats * 0.3 if capacity_sats > 0 else False,
            needs_outbound=available_sats > capacity_sats * 0.7 if capacity_sats > 0 else False,
            needs_channels=channel_count < 5
        )

        # Create and broadcast health report
        msg = fee_intel_mgr.create_health_report_message(
            overall_health=health["overall_health"],
            capacity_score=health["capacity_score"],
            revenue_score=health["revenue_score"],
            connectivity_score=health["connectivity_score"],
            rpc=safe_plugin.rpc,
            needs_inbound=available_sats < capacity_sats * 0.3 if capacity_sats > 0 else False,
            needs_outbound=available_sats > capacity_sats * 0.7 if capacity_sats > 0 else False,
            needs_channels=channel_count < 5,
            can_provide_assistance=health["can_help_others"]
        )

        if msg:
            members = database.get_all_members()
            broadcast_count = 0
            for member in members:
                member_id = member.get("peer_id")
                if not member_id or member_id == our_pubkey:
                    continue
                try:
                    safe_plugin.rpc.call("sendcustommsg", {
                        "node_id": member_id,
                        "msg": msg.hex()
                    })
                    broadcast_count += 1
                except Exception:
                    pass

            if broadcast_count > 0:
                safe_plugin.log(
                    f"cl-hive: Broadcast health report (health={health['overall_health']}, "
                    f"tier={health['tier']}, to {broadcast_count} members)",
                    level='debug'
                )

    except Exception as e:
        if safe_plugin:
            safe_plugin.log(f"cl-hive: Health report broadcast error: {e}", level='warn')


def _broadcast_liquidity_needs():
    """
    Assess and broadcast our liquidity needs to hive members.

    Identifies channels that need rebalancing and broadcasts
    LIQUIDITY_NEED messages for cooperative assistance.
    """
    if not liquidity_coord or not safe_plugin or not database or not our_pubkey:
        return

    try:
        # Get our channel data
        funds = safe_plugin.rpc.listfunds()

        # Assess our liquidity needs
        needs = liquidity_coord.assess_our_liquidity_needs(funds)

        if not needs:
            return

        # Get hive members
        members = database.get_all_members()

        # Note: Cooperative rebalancing removed - we don't transfer funds between nodes.
        # Set can_provide values to 0 since we're information-only.
        # Broadcasting liquidity needs is still useful for fee coordination.

        broadcast_count = 0
        for need in needs[:3]:  # Broadcast top 3 needs
            msg = liquidity_coord.create_liquidity_need_message(
                need_type=need["need_type"],
                target_peer_id=need["target_peer_id"],
                amount_sats=need["amount_sats"],
                urgency=need["urgency"],
                max_fee_ppm=100,  # Willing to pay 100ppm
                reason=need["reason"],
                current_balance_pct=need["current_balance_pct"],
                can_provide_inbound=0,   # No cooperative rebalancing
                can_provide_outbound=0,  # No cooperative rebalancing
                rpc=safe_plugin.rpc
            )

            if msg:
                for member in members:
                    member_id = member.get("peer_id")
                    if not member_id or member_id == our_pubkey:
                        continue
                    try:
                        safe_plugin.rpc.call("sendcustommsg", {
                            "node_id": member_id,
                            "msg": msg.hex()
                        })
                        broadcast_count += 1
                    except Exception:
                        pass

        if broadcast_count > 0:
            safe_plugin.log(
                f"cl-hive: Broadcast {len(needs[:3])} liquidity needs to hive",
                level='debug'
            )

    except Exception as e:
        if safe_plugin:
            safe_plugin.log(f"cl-hive: Liquidity needs broadcast error: {e}", level='warn')


# =============================================================================
# RPC COMMANDS
# =============================================================================

@plugin.method("hive-status")
def hive_status(plugin: Plugin):
    """
    Get current Hive status and membership info.

    Returns:
        Dict with hive state, member count, governance mode, etc.
    """
    return rpc_status(_get_hive_context())


@plugin.method("hive-config")
def hive_config(plugin: Plugin):
    """
    Get current Hive configuration values.

    Shows all config options and their current values. Useful for verifying
    hot-reload changes made via `lightning-cli setconfig`.

    Example:
        lightning-cli hive-config

    Returns:
        Dict with all current config values and metadata.
    """
    return rpc_get_config(_get_hive_context())


@plugin.method("hive-reload-config")
def hive_reload_config(plugin: Plugin):
    """
    Reload configuration from CLN after using setconfig.

    CLN's setconfig command updates option values, but there's no automatic
    notification to plugins. Call this after using setconfig to sync the
    internal config object with CLN's current option values.

    Example:
        lightning-cli setconfig hive-governance-mode failsafe
        lightning-cli hive-reload-config

    Returns:
        Dict with list of updated options and any errors.
    """
    result = _reload_config_from_cln(plugin)
    result["config_version"] = config._version if config else 0
    return result


@plugin.method("hive-reinit-bridge")
def hive_reinit_bridge(plugin: Plugin):
    """
    Re-attempt bridge initialization if it failed at startup.

    Returns:
        Dict with bridge status and details.

    Permission: Admin only
    """
    return rpc_reinit_bridge(_get_hive_context())


@plugin.method("hive-vpn-status")
def hive_vpn_status(plugin: Plugin, peer_id: str = None):
    """
    Get VPN transport status and configuration.

    Shows the current VPN transport mode, configured subnets, peer mappings,
    and which hive members are connected via VPN.

    Args:
        peer_id: Optional - Get VPN info for a specific peer

    Returns:
        Dict with VPN transport configuration and status.

    Permission: Member (read-only status)
    """
    return rpc_vpn_status(_get_hive_context(), peer_id)


@plugin.method("hive-vpn-add-peer")
def hive_vpn_add_peer(plugin: Plugin, pubkey: str, vpn_address: str):
    """
    Add or update a VPN peer mapping.

    Maps a node's pubkey to its VPN address for routing hive gossip.

    Args:
        pubkey: Node pubkey
        vpn_address: VPN address in format ip:port or just ip (default port 9735)

    Returns:
        Dict with result.

    Permission: Admin only
    """
    return rpc_vpn_add_peer(_get_hive_context(), pubkey, vpn_address)


@plugin.method("hive-vpn-remove-peer")
def hive_vpn_remove_peer(plugin: Plugin, pubkey: str):
    """
    Remove a VPN peer mapping.

    Args:
        pubkey: Node pubkey to remove

    Returns:
        Dict with result.

    Permission: Admin only
    """
    return rpc_vpn_remove_peer(_get_hive_context(), pubkey)


@plugin.method("hive-members")
def hive_members(plugin: Plugin):
    """
    List all Hive members with their tier and stats.

    Returns:
        List of member records with tier, contribution ratio, uptime, etc.
    """
    return rpc_members(_get_hive_context())


@plugin.method("hive-propose-promotion")
def hive_propose_promotion(plugin: Plugin, target_peer_id: str,
                           proposer_peer_id: str = None):
    """
    Propose a neophyte for early promotion to member status.

    Any member can propose a neophyte for promotion before the 90-day
    probation period completes. When a majority (51%) of active members
    approve, the neophyte is promoted.

    Args:
        target_peer_id: The neophyte to propose for promotion
        proposer_peer_id: Optional, defaults to our pubkey

    Permission: Member only
    """
    from modules.rpc_commands import propose_promotion
    return propose_promotion(_get_hive_context(), target_peer_id, proposer_peer_id)


@plugin.method("hive-vote-promotion")
def hive_vote_promotion(plugin: Plugin, target_peer_id: str,
                        voter_peer_id: str = None):
    """
    Vote to approve a neophyte's promotion to member.

    Args:
        target_peer_id: The neophyte being voted on
        voter_peer_id: Optional, defaults to our pubkey

    Permission: Member only
    """
    from modules.rpc_commands import vote_promotion
    return vote_promotion(_get_hive_context(), target_peer_id, voter_peer_id)


@plugin.method("hive-pending-promotions")
def hive_pending_promotions(plugin: Plugin):
    """
    View pending manual promotion proposals.

    Returns:
        Dict with pending promotions and their approval status.
    """
    from modules.rpc_commands import pending_promotions
    return pending_promotions(_get_hive_context())


@plugin.method("hive-execute-promotion")
def hive_execute_promotion(plugin: Plugin, target_peer_id: str):
    """
    Execute a manual promotion if quorum has been reached.

    This bypasses the normal 90-day probation period when a majority
    of members have approved the promotion.

    Args:
        target_peer_id: The neophyte to promote

    Permission: Any member can execute once quorum is reached
    """
    from modules.rpc_commands import execute_promotion
    return execute_promotion(_get_hive_context(), target_peer_id)


@plugin.method("hive-topology")
def hive_topology(plugin: Plugin):
    """
    Get current topology analysis from the Planner.

    Returns:
        Dict with saturated targets, planner stats, and config.
    """
    return rpc_topology(_get_hive_context())


@plugin.method("hive-expansion-recommendations")
def hive_expansion_recommendations(plugin: Plugin, limit: int = 10):
    """
    Get expansion recommendations with cooperation module intelligence.

    Returns detailed recommendations integrating:
    - Hive coverage diversity (% of members with channels)
    - Network competition (peer channel count)
    - Bottleneck detection (from liquidity_coordinator)
    - Splice recommendations (from splice_coordinator)

    Args:
        limit: Maximum number of recommendations to return (default: 10)

    Returns:
        Dict with expansion recommendations and coverage summary.
    """
    return rpc_expansion_recommendations(_get_hive_context(), limit=limit)


@plugin.method("hive-channel-closed")
def hive_channel_closed(plugin: Plugin, peer_id: str, channel_id: str,
                        closer: str, close_type: str,
                        capacity_sats: int = 0,
                        # Profitability data
                        duration_days: int = 0,
                        total_revenue_sats: int = 0,
                        total_rebalance_cost_sats: int = 0,
                        net_pnl_sats: int = 0,
                        forward_count: int = 0,
                        forward_volume_sats: int = 0,
                        our_fee_ppm: int = 0,
                        their_fee_ppm: int = 0,
                        routing_score: float = 0.0,
                        profitability_score: float = 0.0):
    """
    Notification from cl-revenue-ops that a channel has closed.

    ALL closures are broadcast to hive members for topology awareness.
    This helps the hive make informed decisions about channel openings.

    Args:
        peer_id: The peer whose channel closed
        channel_id: The closed channel ID
        closer: Who initiated: 'local', 'remote', 'mutual', or 'unknown'
        close_type: Type of closure
        capacity_sats: Channel capacity that was closed

        # Profitability data from cl-revenue-ops:
        duration_days: How long the channel was open
        total_revenue_sats: Total routing fees earned
        total_rebalance_cost_sats: Total rebalancing costs
        net_pnl_sats: Net profit/loss for the channel
        forward_count: Number of forwards routed
        forward_volume_sats: Total volume routed through channel
        our_fee_ppm: Fee rate we charged
        their_fee_ppm: Fee rate they charged us
        routing_score: Routing quality score (0-1)
        profitability_score: Overall profitability score (0-1)

    Returns:
        Dict with action taken
    """
    if not config or not database:
        return {"error": "Hive not initialized"}

    result = {
        "peer_id": peer_id,
        "channel_id": channel_id,
        "closer": closer,
        "close_type": close_type,
        "action": "none",
        "broadcast_count": 0
    }

    # Don't notify about banned peers
    if database.is_banned(peer_id):
        result["action"] = "ignored"
        result["reason"] = "Peer is banned"
        return result

    # Map closer to event_type
    if closer == 'remote':
        event_type = 'remote_close'
    elif closer == 'local':
        event_type = 'local_close'
    elif closer == 'mutual':
        event_type = 'mutual_close'
    else:
        event_type = 'channel_close'

    # Broadcast to all hive members for topology awareness
    broadcast_count = broadcast_peer_available(
        target_peer_id=peer_id,
        event_type=event_type,
        channel_id=channel_id,
        capacity_sats=capacity_sats,
        routing_score=routing_score,
        profitability_score=profitability_score,
        duration_days=duration_days,
        total_revenue_sats=total_revenue_sats,
        total_rebalance_cost_sats=total_rebalance_cost_sats,
        net_pnl_sats=net_pnl_sats,
        forward_count=forward_count,
        forward_volume_sats=forward_volume_sats,
        our_fee_ppm=our_fee_ppm,
        their_fee_ppm=their_fee_ppm,
        reason=f"Channel {channel_id} closed ({closer})"
    )

    result["action"] = "notified_hive"
    result["broadcast_count"] = broadcast_count
    result["event_type"] = event_type
    result["message"] = f"Notified {broadcast_count} hive members about channel closure"

    plugin.log(
        f"cl-hive: Channel {channel_id} closed by {closer}, "
        f"notified {broadcast_count} members (pnl={net_pnl_sats} sats)",
        level='info'
    )

    return result


@plugin.method("hive-channel-opened")
def hive_channel_opened(plugin: Plugin, peer_id: str, channel_id: str,
                        opener: str, capacity_sats: int = 0,
                        our_funding_sats: int = 0, their_funding_sats: int = 0):
    """
    Notification from cl-revenue-ops that a channel has opened.

    ALL opens are broadcast to hive members for topology awareness.
    This helps the hive track who has channels to which peers.

    Args:
        peer_id: The peer the channel was opened with
        channel_id: The new channel ID
        opener: Who initiated: 'local' or 'remote'
        capacity_sats: Total channel capacity
        our_funding_sats: Amount we funded
        their_funding_sats: Amount they funded

    Returns:
        Dict with action taken
    """
    if not config or not database:
        return {"error": "Hive not initialized"}

    result = {
        "peer_id": peer_id,
        "channel_id": channel_id,
        "opener": opener,
        "capacity_sats": capacity_sats,
        "action": "none",
        "broadcast_count": 0
    }

    # Check if peer is a hive member (internal channel)
    member = database.get_member(peer_id)
    is_hive_internal = member is not None and not database.is_banned(peer_id)

    # Broadcast to all hive members
    broadcast_count = broadcast_peer_available(
        target_peer_id=peer_id,
        event_type='channel_open',
        channel_id=channel_id,
        capacity_sats=capacity_sats,
        our_funding_sats=our_funding_sats,
        their_funding_sats=their_funding_sats,
        opener=opener,
        reason=f"Channel {channel_id} opened ({opener})"
    )

    result["action"] = "notified_hive"
    result["broadcast_count"] = broadcast_count
    result["is_hive_internal"] = is_hive_internal
    result["message"] = f"Notified {broadcast_count} hive members about new channel"

    plugin.log(
        f"cl-hive: Channel {channel_id} opened with {peer_id[:16]}... ({opener}), "
        f"notified {broadcast_count} members",
        level='info'
    )

    return result


@plugin.method("hive-peer-events")
def hive_peer_events(plugin: Plugin, peer_id: str = None, event_type: str = None,
                     reporter_id: str = None, days: int = 90, limit: int = 100,
                     summary: bool = False):
    """
    Query peer events for topology intelligence (Phase 6.1).

    This RPC provides access to the peer_events table which stores all channel
    open/close events received from hive members. Use this data to understand
    peer quality and make informed channel decisions.

    Args:
        peer_id: Filter by external peer pubkey (optional)
        event_type: Filter by event type: channel_open, channel_close,
                    remote_close, local_close, mutual_close (optional)
        reporter_id: Filter by reporting hive member pubkey (optional)
        days: Only include events from last N days (default: 90)
        limit: Maximum number of events to return (default: 100, max: 500)
        summary: If True and peer_id is set, return aggregated summary instead

    Returns:
        If summary=False: Dict with events list and metadata
        If summary=True: Dict with aggregated statistics for the peer

    Examples:
        # Get all events from last 30 days
        hive-peer-events days=30

        # Get events for a specific peer
        hive-peer-events peer_id=02abc123...

        # Get summary statistics for a peer
        hive-peer-events peer_id=02abc123... summary=true

        # Get only remote close events
        hive-peer-events event_type=remote_close

        # Get events reported by a specific hive member
        hive-peer-events reporter_id=03def456...
    """
    if not database:
        return {"error": "Database not initialized"}

    # Bound limit
    limit = min(max(1, limit), 500)
    days = min(max(1, days), 365)

    # If summary requested with peer_id, return aggregated stats
    if summary and peer_id:
        stats = database.get_peer_event_summary(peer_id, days=days)
        return {
            "peer_id": peer_id,
            "days": days,
            "summary": stats,
        }

    # Otherwise return event list
    events = database.get_peer_events(
        peer_id=peer_id,
        event_type=event_type,
        reporter_id=reporter_id,
        days=days,
        limit=limit
    )

    # Get list of unique peers with events if no peer_id filter
    peers_with_events = []
    if not peer_id:
        peers_with_events = database.get_peers_with_events(days=days)

    return {
        "count": len(events),
        "limit": limit,
        "days": days,
        "filters": {
            "peer_id": peer_id,
            "event_type": event_type,
            "reporter_id": reporter_id,
        },
        "peers_with_events": len(peers_with_events),
        "events": events,
    }


@plugin.method("hive-peer-quality")
def hive_peer_quality(plugin: Plugin, peer_id: str = None, days: int = 90,
                      min_confidence: float = 0.0, limit: int = 50):
    """
    Calculate quality scores for external peers (Phase 6.2).

    Quality scores are based on historical channel event data from hive members.
    Use this to evaluate peer reliability, profitability, and routing potential
    before opening channels.

    Score Components:
        - Reliability (35%): Based on closure behavior and duration
        - Profitability (25%): Based on P&L and revenue data
        - Routing (25%): Based on forward activity
        - Consistency (15%): Based on agreement across reporters

    Args:
        peer_id: Specific peer to score (optional). If not provided,
                 returns scores for all peers with event data.
        days: Number of days of history to consider (default: 90)
        min_confidence: Minimum confidence threshold (0-1) to include (default: 0)
        limit: Maximum number of peers to return when peer_id not set (default: 50)

    Returns:
        Dict with quality scores and recommendations.

    Examples:
        # Get quality score for a specific peer
        hive-peer-quality peer_id=02abc123...

        # Get top 20 highest quality peers
        hive-peer-quality limit=20

        # Get only high-confidence scores
        hive-peer-quality min_confidence=0.5

        # Use 30 days of data instead of 90
        hive-peer-quality peer_id=02abc123... days=30
    """
    if not database:
        return {"error": "Database not initialized"}

    # Create scorer instance
    scorer = PeerQualityScorer(database, plugin)

    # Bound parameters
    days = min(max(1, days), 365)
    limit = min(max(1, limit), 200)
    min_confidence = max(0.0, min(1.0, min_confidence))

    if peer_id:
        # Single peer score
        result = scorer.calculate_score(peer_id, days=days)
        return {
            "peer_id": peer_id,
            "days": days,
            "score": result.to_dict(),
        }

    # All peers with event data
    results = scorer.get_scored_peers(days=days, min_confidence=min_confidence)

    # Limit results
    results = results[:limit]

    return {
        "count": len(results),
        "limit": limit,
        "days": days,
        "min_confidence": min_confidence,
        "peers": [r.to_dict() for r in results],
        "score_breakdown": {
            "excellent": len([r for r in results if r.recommendation == "excellent"]),
            "good": len([r for r in results if r.recommendation == "good"]),
            "neutral": len([r for r in results if r.recommendation == "neutral"]),
            "caution": len([r for r in results if r.recommendation == "caution"]),
            "avoid": len([r for r in results if r.recommendation == "avoid"]),
        }
    }


@plugin.method("hive-quality-check")
def hive_quality_check(plugin: Plugin, peer_id: str, days: int = 90,
                       min_score: float = 0.45):
    """
    Quick quality check for a peer - should we open a channel? (Phase 6.2)

    This is a convenience method for the planner and governance engine to
    quickly determine if a peer is suitable for channel opening.

    Args:
        peer_id: Peer to evaluate (required)
        days: Days of history to consider (default: 90)
        min_score: Minimum quality score required (default: 0.45)

    Returns:
        Dict with recommendation and reasoning.

    Examples:
        # Check if peer is suitable for channel
        hive-quality-check peer_id=02abc123...

        # Use stricter threshold
        hive-quality-check peer_id=02abc123... min_score=0.6
    """
    if not database:
        return {"error": "Database not initialized"}

    if not peer_id:
        return {"error": "peer_id is required"}

    # Create scorer and check
    scorer = PeerQualityScorer(database, plugin)
    should_open, reason = scorer.should_open_channel(
        peer_id, days=days, min_score=min_score
    )

    # Also get full score for context
    result = scorer.calculate_score(peer_id, days=days)

    return {
        "peer_id": peer_id,
        "should_open": should_open,
        "reason": reason,
        "overall_score": round(result.overall_score, 3),
        "confidence": round(result.confidence, 3),
        "recommendation": result.recommendation,
        "min_score_threshold": min_score,
    }


@plugin.method("hive-calculate-size")
def hive_calculate_size(plugin: Plugin, peer_id: str, capacity_sats: int = None,
                        channel_count: int = None, hive_share_pct: float = 0.0):
    """
    Calculate recommended channel size for a peer (Phase 6.3).

    This RPC previews what channel size would be recommended for a given peer,
    taking into account quality scores, network factors, and configuration.

    Args:
        peer_id: Target peer pubkey (required)
        capacity_sats: Target's public capacity in sats (optional, will lookup)
        channel_count: Target's channel count (optional, will lookup)
        hive_share_pct: Current hive share to target 0-1 (default: 0)

    Returns:
        Dict with recommended size, factors, and reasoning.

    Examples:
        # Calculate size for a peer (auto-lookup capacity)
        hive-calculate-size peer_id=02abc123...

        # Override capacity and channel count
        hive-calculate-size peer_id=02abc123... capacity_sats=100000000 channel_count=50

        # Simulate existing hive share
        hive-calculate-size peer_id=02abc123... hive_share_pct=0.05
    """
    if not database:
        return {"error": "Database not initialized"}

    if not config:
        return {"error": "Config not initialized"}

    if not peer_id:
        return {"error": "peer_id is required"}

    # Get config snapshot
    cfg = config.snapshot()

    # Lookup capacity and channel count if not provided
    if capacity_sats is None or channel_count is None:
        try:
            # Try to get from listchannels
            channels = plugin.rpc.listchannels(source=peer_id)
            peer_channels = channels.get('channels', [])

            if capacity_sats is None:
                capacity_sats = sum(c.get('amount_msat', 0) // 1000 for c in peer_channels)
                if capacity_sats == 0:
                    capacity_sats = 100_000_000  # Default 1 BTC if not found

            if channel_count is None:
                channel_count = len(peer_channels)
                if channel_count == 0:
                    channel_count = 20  # Default moderate connectivity
        except Exception as e:
            plugin.log(f"cl-hive: Error looking up peer info: {e}", level='debug')
            if capacity_sats is None:
                capacity_sats = 100_000_000  # Default 1 BTC
            if channel_count is None:
                channel_count = 20  # Default moderate

    # Get onchain balance
    try:
        funds = plugin.rpc.listfunds()
        outputs = funds.get('outputs', [])
        onchain_balance = sum(
            (o.get('amount_msat', 0) // 1000 if isinstance(o.get('amount_msat'), int)
             else int(o.get('amount_msat', '0msat')[:-4]) // 1000
             if isinstance(o.get('amount_msat'), str) else o.get('value', 0))
            for o in outputs if o.get('status') == 'confirmed'
        )
    except Exception:
        onchain_balance = cfg.planner_default_channel_sats * 10  # Assume adequate

    # Get available budget (considering all constraints)
    daily_remaining = database.get_available_budget(cfg.failsafe_budget_per_day)
    max_per_channel = int(cfg.failsafe_budget_per_day * cfg.budget_max_per_channel_pct)
    spendable_onchain = int(onchain_balance * (1.0 - cfg.budget_reserve_pct))
    available_budget = min(daily_remaining, max_per_channel, spendable_onchain)

    # Create quality scorer and channel sizer
    scorer = PeerQualityScorer(database, plugin)
    sizer = ChannelSizer(plugin=plugin, quality_scorer=scorer)

    # Calculate size with budget constraint
    result = sizer.calculate_size(
        target=peer_id,
        target_capacity_sats=capacity_sats,
        target_channel_count=channel_count,
        hive_share_pct=hive_share_pct,
        target_share_cap=cfg.market_share_cap_pct * 0.5,
        onchain_balance_sats=onchain_balance,
        min_channel_sats=cfg.planner_min_channel_sats,
        max_channel_sats=cfg.planner_max_channel_sats,
        default_channel_sats=cfg.planner_default_channel_sats,
        available_budget_sats=available_budget,
    )

    # Get budget summary
    budget_info = database.get_budget_summary(cfg.failsafe_budget_per_day, days=1)

    return {
        "peer_id": peer_id,
        "recommended_size_sats": result.recommended_size_sats,
        "recommended_size_btc": round(result.recommended_size_sats / 100_000_000, 4),
        "reasoning": result.reasoning,
        "factors": result.factors,
        "inputs": {
            "capacity_sats": capacity_sats,
            "channel_count": channel_count,
            "hive_share_pct": hive_share_pct,
            "onchain_balance_sats": onchain_balance,
        },
        "budget": {
            "daily_budget_sats": cfg.failsafe_budget_per_day,
            "spent_today_sats": budget_info['today']['spent_sats'],
            "daily_remaining_sats": daily_remaining,
            "max_per_channel_sats": max_per_channel,
            "reserve_pct": cfg.budget_reserve_pct,
            "spendable_onchain_sats": spendable_onchain,
            "effective_budget_sats": available_budget,
            "budget_limited": result.factors.get('budget_limited', False),
        },
        "config_bounds": {
            "min_channel_sats": cfg.planner_min_channel_sats,
            "max_channel_sats": cfg.planner_max_channel_sats,
            "default_channel_sats": cfg.planner_default_channel_sats,
        },
        "feerate": _get_feerate_info(cfg.max_expansion_feerate_perkb),
    }


def _get_feerate_info(max_feerate_perkb: int) -> dict:
    """Get current feerate information for expansion decisions."""
    allowed, current, reason = _check_feerate_for_expansion(max_feerate_perkb)
    return {
        "current_perkb": current,
        "max_allowed_perkb": max_feerate_perkb,
        "expansion_allowed": allowed,
        "reason": reason,
    }


@plugin.method("hive-expansion-status")
def hive_expansion_status(plugin: Plugin, round_id: str = None,
                          target_peer_id: str = None):
    """
    Get status of cooperative expansion rounds.

    Args:
        round_id: Get status of a specific round (optional)
        target_peer_id: Get rounds for a specific target peer (optional)

    Returns:
        Dict with expansion round status and statistics.
    """
    return rpc_expansion_status(_get_hive_context(), round_id=round_id,
                                target_peer_id=target_peer_id)


@plugin.method("hive-expansion-nominate")
def hive_expansion_nominate(plugin: Plugin, target_peer_id: str, round_id: str = None):
    """
    Manually trigger a cooperative expansion round for a peer (Phase 6.4).

    This RPC allows manually starting a cooperative expansion round
    for a target peer, useful for testing or when automatic triggering
    is disabled.

    Args:
        target_peer_id: The external peer to consider for expansion
        round_id: Optional existing round ID to join (if omitted, starts new round)

    Returns:
        Dict with round information.

    Examples:
        # Start a new expansion round
        hive-expansion-nominate target_peer_id=02abc123...

        # Join an existing round
        hive-expansion-nominate target_peer_id=02abc123... round_id=abc12345
    """
    if not coop_expansion:
        return {"error": "Cooperative expansion not initialized"}

    if not target_peer_id:
        return {"error": "target_peer_id is required"}

    # Check feerate and warn if high (but don't block manual operation)
    cfg = config.snapshot() if config else None
    max_feerate = cfg.max_expansion_feerate_perkb if cfg else 5000
    feerate_allowed, current_feerate, feerate_reason = _check_feerate_for_expansion(max_feerate)
    feerate_warning = None
    if not feerate_allowed:
        feerate_warning = f"Warning: on-chain fees are high ({feerate_reason}). Consider waiting for lower fees."

    if round_id:
        # Join existing round - create it locally if we don't have it
        round_obj = coop_expansion.get_round(round_id)
        if not round_obj:
            # Create the round locally to join it
            plugin.log(f"cl-hive: Creating local copy of remote round {round_id[:8]}...")
            coop_expansion.join_remote_round(
                round_id=round_id,
                target_peer_id=target_peer_id,
                trigger_reporter=our_pubkey or ""
            )

        # Broadcast our nomination
        _broadcast_expansion_nomination(round_id, target_peer_id)

        result = {
            "action": "joined",
            "round_id": round_id,
            "target_peer_id": target_peer_id,
        }
        if feerate_warning:
            result["warning"] = feerate_warning
            result["current_feerate_perkb"] = current_feerate
        return result

    # Start new round
    new_round_id = coop_expansion.start_round(
        target_peer_id=target_peer_id,
        trigger_event="manual",
        trigger_reporter=our_pubkey or "",
        quality_score=0.5
    )

    # Broadcast our nomination
    _broadcast_expansion_nomination(new_round_id, target_peer_id)

    result = {
        "action": "started",
        "round_id": new_round_id,
        "target_peer_id": target_peer_id,
    }
    if feerate_warning:
        result["warning"] = feerate_warning
        result["current_feerate_perkb"] = current_feerate
    return result


@plugin.method("hive-expansion-elect")
def hive_expansion_elect(plugin: Plugin, round_id: str):
    """
    Manually trigger election for an expansion round (Phase 6.4).

    Normally elections happen automatically after the nomination window.
    This RPC allows manually triggering an election early.

    Args:
        round_id: The round to elect for (required)

    Returns:
        Dict with election result.

    Examples:
        hive-expansion-elect round_id=abc12345
    """
    if not coop_expansion:
        return {"error": "Cooperative expansion not initialized"}

    if not round_id:
        return {"error": "round_id is required"}

    round_obj = coop_expansion.get_round(round_id)
    if not round_obj:
        return {"error": f"Round {round_id} not found"}

    # Run election
    elected_id = coop_expansion.elect_winner(round_id)

    if not elected_id:
        return {
            "round_id": round_id,
            "elected": False,
            "reason": round_obj.result if round_obj else "Unknown",
        }

    # Broadcast election result
    _broadcast_expansion_elect(
        round_id=round_id,
        target_peer_id=round_obj.target_peer_id,
        elected_id=elected_id,
        channel_size_sats=round_obj.recommended_size_sats,
        quality_score=round_obj.quality_score,
        nomination_count=len(round_obj.nominations)
    )

    # If we were elected, queue the pending action locally
    # (we won't receive our own broadcast message)
    if elected_id == our_pubkey and database and config:
        cfg = config.snapshot()
        proposed_size = round_obj.recommended_size_sats or cfg.planner_default_channel_sats

        # Check affordability before queuing
        capped_size, insufficient, was_capped = _cap_channel_size_to_budget(
            proposed_size, cfg, f"Local election for {round_obj.target_peer_id[:16]}..."
        )
        if insufficient:
            plugin.log(
                f"cl-hive: [ELECT] Cannot queue channel: insufficient funds "
                f"(proposed={proposed_size}, min={cfg.planner_min_channel_sats})",
                level='warn'
            )
            return {
                "round_id": round_id,
                "elected": True,
                "elected_id": elected_id,
                "error": "insufficient_funds",
                "reason": f"Cannot afford minimum channel size ({cfg.planner_min_channel_sats} sats)"
            }
        if was_capped:
            plugin.log(
                f"cl-hive: [ELECT] Capping local election channel size from {proposed_size} to {capped_size}",
                level='info'
            )

        action_id = database.add_pending_action(
            action_type="channel_open",
            payload={
                "target": round_obj.target_peer_id,
                "amount_sats": capped_size,
                "source": "cooperative_expansion",
                "round_id": round_id,
                "reason": "Elected by hive for cooperative expansion"
            },
            expires_hours=24
        )
        plugin.log(
            f"cl-hive: Queued channel open to {round_obj.target_peer_id[:16]}... "
            f"(action_id={action_id}, size={capped_size})",
            level='info'
        )

    return {
        "round_id": round_id,
        "elected": True,
        "elected_id": elected_id,
        "target_peer_id": round_obj.target_peer_id,
        "nomination_count": len(round_obj.nominations),
    }


@plugin.method("hive-planner-log")
def hive_planner_log(plugin: Plugin, limit: int = 50):
    """
    Get recent Planner decision logs.

    Args:
        limit: Maximum number of log entries to return (default: 50)

    Returns:
        Dict with log entries and count.
    """
    return rpc_planner_log(_get_hive_context(), limit=limit)


@plugin.method("hive-test-intent")
def hive_test_intent(plugin: Plugin, target: str, intent_type: str = "channel_open",
                     broadcast: bool = True):
    """
    Create and optionally broadcast a test intent (for simulation/testing).

    This command is for testing the Intent Lock Protocol and conflict resolution.

    Args:
        target: Target peer pubkey for the intent
        intent_type: Type of intent (channel_open, rebalance, ban_peer)
        broadcast: Whether to broadcast to Hive members (default: True)

    Returns:
        Dict with intent details and broadcast result.

    Example:
        lightning-cli hive-test-intent 02abc123...
    """
    # Permission check: Admin only (test commands)
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not planner or not planner.intent_manager:
        return {"error": "Intent manager not initialized"}

    intent_mgr = planner.intent_manager

    try:
        # Create the intent
        intent = intent_mgr.create_intent(intent_type, target)

        result = {
            "intent_id": intent.intent_id,
            "intent_type": intent.intent_type,
            "target": target,
            "initiator": intent.initiator,
            "timestamp": intent.timestamp,
            "expires_at": intent.expires_at,
            "hold_seconds": intent.expires_at - intent.timestamp,
            "status": intent.status,
            "broadcast": False,
            "broadcast_count": 0
        }

        # Broadcast if requested
        if broadcast:
            success = planner._broadcast_intent(intent)
            result["broadcast"] = success
            if success:
                members = database.get_all_members()
                our_id = plugin.rpc.getinfo()['id']
                result["broadcast_count"] = len([m for m in members if m.get('peer_id') != our_id])

        return result

    except Exception as e:
        return {"error": str(e)}


@plugin.method("hive-intent-status")
def hive_intent_status(plugin: Plugin):
    """
    Get current intent status (local and remote intents).

    Returns:
        Dict with pending intents and stats.
    """
    return rpc_intent_status(_get_hive_context())


@plugin.method("hive-test-pending-action")
def hive_test_pending_action(plugin: Plugin, action_type: str = "channel_open",
                              target: str = None, capacity_sats: int = 1000000,
                              reason: str = "test_action"):
    """
    Create a test pending action for AI advisor testing.

    This command creates an entry in the pending_actions table that the AI
    advisor can evaluate. Use this to test the advisor without triggering
    the actual planner.

    Args:
        action_type: Type of action (channel_open, ban, unban, expand)
        target: Target peer pubkey (default: uses first external node in graph)
        capacity_sats: Proposed capacity for channel_open (default: 1M sats)
        reason: Reason for the action (default: test_action)

    Returns:
        Dict with the created pending action details.

    Example:
        lightning-cli hive-test-pending-action
        lightning-cli hive-test-pending-action channel_open 02abc123... 500000 "underserved_target"
    """
    # Permission check: Admin only (test commands)
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database:
        return {"error": "Database not initialized"}

    # Get a target if not specified
    if not target:
        # Try to find an external node from the network graph
        try:
            channels = plugin.rpc.listchannels()
            our_id = plugin.rpc.getinfo()['id']
            members = database.get_all_members()
            member_ids = {m['peer_id'] for m in members}

            # Find a node that's not in our hive
            for ch in channels.get('channels', []):
                candidate = ch.get('destination')
                if candidate and candidate not in member_ids and candidate != our_id:
                    target = candidate
                    break

            if not target:
                return {"error": "No external target found in graph. Specify target manually."}
        except Exception as e:
            return {"error": f"Failed to find target: {e}"}

    # Build payload based on action type
    if action_type == "channel_open":
        # Create an intent for channel_open actions (required for approval)
        intent_id = None
        if planner and planner.intent_manager:
            try:
                intent = planner.intent_manager.create_intent("channel_open", target)
                intent_id = intent.intent_id
            except Exception as e:
                return {"error": f"Failed to create intent: {e}"}
        else:
            return {"error": "Intent manager not initialized (required for channel_open)"}

        payload = {
            "target": target,
            "capacity_sats": capacity_sats,
            "reason": reason,
            "intent_id": intent_id,
            "scoring": {
                "connectivity_score": 0.8,
                "fee_score": 0.7,
                "capacity_score": 0.6
            }
        }
    elif action_type == "ban":
        payload = {
            "target": target,
            "reason": reason,
            "evidence": "test_evidence"
        }
    else:
        payload = {
            "target": target,
            "action_type": action_type,
            "reason": reason
        }

    try:
        action_id = database.add_pending_action(action_type, payload, expires_hours=24)
        return {
            "status": "created",
            "action_id": action_id,
            "action_type": action_type,
            "target": target,
            "payload": payload,
            "expires_in_hours": 24
        }
    except Exception as e:
        return {"error": f"Failed to create pending action: {e}"}


@plugin.method("hive-pending-actions")
def hive_pending_actions(plugin: Plugin):
    """
    Get all pending actions awaiting operator approval.

    Returns:
        Dict with list of pending actions.
    """
    return rpc_pending_actions(_get_hive_context())


@plugin.method("hive-approve-action")
def hive_approve_action(plugin: Plugin, action_id="all", amount_sats: int = None):
    """
    Approve and execute pending action(s).

    Args:
        action_id: ID of the action to approve, or "all" to approve all pending actions.
            Defaults to "all" if not specified.
        amount_sats: Optional override for channel size (member budget control).
            If provided, uses this amount instead of the proposed amount.
            Must be >= min_channel_sats and will still be subject to budget limits.
            Only applies when approving a single action.

    Returns:
        Dict with approval result including budget details.

    Permission: Member or Admin only
    """
    return rpc_approve_action(_get_hive_context(), action_id, amount_sats)


@plugin.method("hive-reject-action")
def hive_reject_action(plugin: Plugin, action_id="all"):
    """
    Reject pending action(s).

    Args:
        action_id: ID of the action to reject, or "all" to reject all pending actions.
            Defaults to "all" if not specified.

    Returns:
        Dict with rejection result.

    Permission: Member or Admin only
    """
    return rpc_reject_action(_get_hive_context(), action_id)


@plugin.method("hive-budget-summary")
def hive_budget_summary(plugin: Plugin, days: int = 7):
    """
    Get budget usage summary for autonomous mode.

    Args:
        days: Number of days of history to include (default: 7)

    Returns:
        Dict with budget utilization and spending history.

    Permission: Member or Admin only
    """
    return rpc_budget_summary(_get_hive_context(), days)


# =============================================================================
# PHASE 7: FEE INTELLIGENCE RPC COMMANDS
# =============================================================================

@plugin.method("hive-fee-profiles")
def hive_fee_profiles(plugin: Plugin, peer_id: str = None):
    """
    Get aggregated fee profiles for external peers.

    Fee profiles are built from collective intelligence shared by hive members.
    Includes optimal fee recommendations based on elasticity and NNLB.

    Args:
        peer_id: Optional specific peer to query (otherwise returns all)

    Returns:
        Dict with fee profile(s) and aggregation stats.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database or not fee_intel_mgr:
        return {"error": "Fee intelligence not initialized"}

    if peer_id:
        # Query specific peer
        profile = database.get_peer_fee_profile(peer_id)
        if not profile:
            return {
                "peer_id": peer_id,
                "error": "No fee profile found",
                "hint": "No hive members have reported on this peer yet"
            }
        return {
            "profile": profile
        }
    else:
        # Return all profiles
        profiles = database.get_all_peer_fee_profiles()
        return {
            "profile_count": len(profiles),
            "profiles": profiles
        }


@plugin.method("hive-fee-recommendation")
def hive_fee_recommendation(plugin: Plugin, peer_id: str, channel_size: int = 0):
    """
    Get fee recommendation for an external peer.

    Uses collective fee intelligence and NNLB health adjustments
    to recommend optimal fee for maximum revenue while supporting
    struggling hive members.

    Args:
        peer_id: External peer to get recommendation for
        channel_size: Our channel size to this peer (for context)

    Returns:
        Dict with recommended fee and reasoning.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database or not fee_intel_mgr:
        return {"error": "Fee intelligence not initialized"}

    # Get our health for NNLB adjustment
    our_health = 50  # Default to healthy
    if our_pubkey:
        health_record = database.get_member_health(our_pubkey)
        if health_record:
            our_health = health_record.get("overall_health", 50)

    recommendation = fee_intel_mgr.get_fee_recommendation(
        target_peer_id=peer_id,
        our_channel_size=channel_size,
        our_health=our_health
    )

    return recommendation


@plugin.method("hive-fee-intelligence")
def hive_fee_intelligence(plugin: Plugin, max_age_hours: int = 24, peer_id: str = None):
    """
    Get raw fee intelligence reports.

    Returns individual fee observations from hive members before aggregation.

    Args:
        max_age_hours: Maximum age of reports to return (default 24)
        peer_id: Optional filter by target peer

    Returns:
        Dict with fee intelligence reports.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database:
        return {"error": "Database not initialized"}

    if peer_id:
        reports = database.get_fee_intelligence_for_peer(peer_id, max_age_hours)
    else:
        reports = database.get_all_fee_intelligence(max_age_hours)

    return {
        "report_count": len(reports),
        "max_age_hours": max_age_hours,
        "reports": reports
    }


@plugin.method("hive-aggregate-fees")
def hive_aggregate_fees(plugin: Plugin):
    """
    Trigger fee profile aggregation.

    Aggregates all recent fee intelligence into peer fee profiles.
    Normally runs automatically, but can be triggered manually.

    Returns:
        Dict with aggregation results.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not fee_intel_mgr:
        return {"error": "Fee intelligence manager not initialized"}

    updated_count = fee_intel_mgr.aggregate_fee_profiles()

    return {
        "status": "ok",
        "profiles_updated": updated_count
    }


@plugin.method("hive-fee-intel-query")
def hive_fee_intel_query(plugin: Plugin, peer_id: str = None, action: str = "query"):
    """
    Query aggregated fee intelligence from the hive.

    This RPC is designed for cl-revenue-ops to query competitor fee data
    for informing Hill Climbing fee decisions.

    Args:
        peer_id: Specific peer to query (None for all). Can also use
                 action="list" with peer_id=None to get all known peers.
        action: "query" (default) or "list"
            - query: Get aggregated profile for a single peer
            - list: Get all known peer profiles

    Returns for single peer (action="query"):
    {
        "peer_id": "02abc...",
        "avg_fee_charged": 250,
        "min_fee": 100,
        "max_fee": 500,
        "fee_volatility": 0.15,
        "estimated_elasticity": -0.8,
        "optimal_fee_estimate": 180,
        "confidence": 0.75,
        "market_share": 0.0,  # Calculated by caller with their capacity data
        "hive_capacity_sats": 6000000,
        "hive_reporters": 3,
        "last_updated": 1705000000
    }

    Returns for "list" action:
    {
        "peers": [...],  # List of profiles in same format
        "count": 25
    }

    Permission: None (accessible without hive membership for local cl-revenue-ops)
    """
    # No permission check - this is for local cl-revenue-ops integration
    # cl-revenue-ops runs on the same node, so it's trusted

    if not fee_intel_mgr:
        return {"error": "Fee intelligence manager not initialized"}

    if action == "list":
        profiles = fee_intel_mgr.get_all_profiles(limit=100)
        return {
            "peers": profiles,
            "count": len(profiles)
        }

    if not peer_id:
        return {"error": "peer_id required for query action"}

    profile = fee_intel_mgr.get_aggregated_profile(peer_id)
    if not profile:
        return {
            "error": "no_data",
            "peer_id": peer_id,
            "message": "No fee intelligence data for this peer"
        }

    return profile


@plugin.method("hive-report-fee-observation")
def hive_report_fee_observation(
    plugin: Plugin,
    peer_id: str,
    our_fee_ppm: int,
    their_fee_ppm: int = None,
    volume_sats: int = 0,
    forward_count: int = 0,
    period_hours: float = 1.0,
    revenue_rate: float = None
):
    """
    Receive fee observation from cl-revenue-ops.

    This RPC is designed for cl-revenue-ops to report its fee observations
    back to cl-hive for collective intelligence sharing.

    The observation is:
    1. Stored locally in fee_intelligence table
    2. (Optionally) Broadcast to hive via FEE_INTELLIGENCE message
    3. Used in fee profile aggregation

    Args:
        peer_id: External peer being observed
        our_fee_ppm: Our current fee toward this peer
        their_fee_ppm: Their fee toward us (if known)
        volume_sats: Volume routed in observation period
        forward_count: Number of forwards
        period_hours: Observation window length
        revenue_rate: Calculated revenue rate (sats/hour)

    Returns:
        {"status": "accepted", "observation_id": <id>}

    Permission: None (local cl-revenue-ops integration)
    """
    # No permission check - this is for local cl-revenue-ops integration

    if not database or not fee_intel_mgr:
        return {"error": "Fee intelligence not initialized"}

    if not peer_id:
        return {"error": "peer_id is required"}

    if our_fee_ppm < 0:
        return {"error": "our_fee_ppm must be non-negative"}

    # Store the observation
    try:
        timestamp = int(time.time())

        # Calculate revenue if not provided
        if revenue_rate is None and period_hours > 0:
            revenue_sats = (volume_sats * our_fee_ppm) // 1_000_000
            revenue_rate = revenue_sats / period_hours

        # Determine flow direction based on balance change (simplified)
        flow_direction = "balanced"

        # Calculate utilization (simplified - would need channel capacity)
        utilization_pct = 0.0

        # Store via fee_intel_mgr's observation handler
        observation_id = fee_intel_mgr.store_local_observation(
            target_peer_id=peer_id,
            our_fee_ppm=our_fee_ppm,
            their_fee_ppm=their_fee_ppm,
            forward_count=forward_count,
            forward_volume_sats=volume_sats,
            revenue_rate=revenue_rate or 0.0,
            flow_direction=flow_direction,
            utilization_pct=utilization_pct,
            timestamp=timestamp
        )

        return {
            "status": "accepted",
            "observation_id": observation_id,
            "peer_id": peer_id
        }

    except Exception as e:
        plugin.log(f"Error storing fee observation: {e}", level='warn')
        return {"error": f"Failed to store observation: {e}"}


@plugin.method("hive-trigger-fee-broadcast")
def hive_trigger_fee_broadcast(plugin: Plugin):
    """
    Manually trigger fee intelligence broadcast.

    Immediately collects fee observations from our channels and broadcasts
    to all hive members. Useful for testing or forcing an immediate update.

    Returns:
        Dict with broadcast results.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not fee_intel_mgr or not safe_plugin:
        return {"error": "Fee intelligence manager not initialized"}

    try:
        _broadcast_our_fee_intelligence()
        return {"status": "ok", "message": "Fee intelligence broadcast triggered"}
    except Exception as e:
        return {"error": f"Broadcast failed: {e}"}


@plugin.method("hive-trigger-health-report")
def hive_trigger_health_report(plugin: Plugin):
    """
    Manually trigger health report broadcast.

    Immediately calculates our health score and broadcasts to all hive members.
    Useful for testing NNLB or forcing an immediate health update.

    Returns:
        Dict with health report results.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not fee_intel_mgr or not safe_plugin:
        return {"error": "Fee intelligence manager not initialized"}

    try:
        _broadcast_health_report()
        # Return current health after broadcast
        if database and our_pubkey:
            health = database.get_member_health(our_pubkey)
            if health:
                return {
                    "status": "ok",
                    "message": "Health report broadcast triggered",
                    "our_health": health
                }
        return {"status": "ok", "message": "Health report broadcast triggered"}
    except Exception as e:
        return {"error": f"Health report broadcast failed: {e}"}


@plugin.method("hive-trigger-all")
def hive_trigger_all(plugin: Plugin):
    """
    Manually trigger all fee intelligence operations.

    Runs the complete fee intelligence cycle:
    1. Broadcast fee intelligence
    2. Aggregate fee profiles
    3. Broadcast health report

    Useful for testing or forcing immediate updates.

    Returns:
        Dict with all operation results.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not fee_intel_mgr or not safe_plugin:
        return {"error": "Fee intelligence manager not initialized"}

    results = {}

    try:
        _broadcast_our_fee_intelligence()
        results["fee_broadcast"] = "ok"
    except Exception as e:
        results["fee_broadcast"] = f"error: {e}"

    try:
        updated = fee_intel_mgr.aggregate_fee_profiles()
        results["profiles_aggregated"] = updated
    except Exception as e:
        results["profiles_aggregated"] = f"error: {e}"

    try:
        _broadcast_health_report()
        results["health_broadcast"] = "ok"
    except Exception as e:
        results["health_broadcast"] = f"error: {e}"

    # Get current state after operations
    if database and our_pubkey:
        health = database.get_member_health(our_pubkey)
        if health:
            results["our_health"] = health.get("overall_health")
            results["our_tier"] = health.get("tier")

    results["status"] = "ok"
    return results


@plugin.method("hive-nnlb-status")
def hive_nnlb_status(plugin: Plugin):
    """
    Get NNLB (No Node Left Behind) status.

    Shows health distribution across hive members and identifies
    struggling members who may need assistance.

    Returns:
        Dict with NNLB statistics and member health tiers.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not fee_intel_mgr:
        return {"error": "Fee intelligence manager not initialized"}

    return fee_intel_mgr.get_nnlb_status()


@plugin.method("hive-member-health")
def hive_member_health(plugin: Plugin, member_id: str = None, action: str = "query"):
    """
    Query NNLB health scores for fleet members.

    This is INFORMATION SHARING only - no fund movement.
    Used by cl-revenue-ops to adjust its own rebalancing priorities.

    Args:
        member_id: Specific member (None for self, "all" for fleet summary)
        action: "query" (default) or "aggregate" (fleet summary)

    Returns for single member:
    {
        "member_id": "02abc...",
        "health_score": 65,
        "health_tier": "stable",
        "budget_multiplier": 1.0,
        "capacity_score": 70,
        "revenue_score": 60,
        "connectivity_score": 72,
        ...
    }

    Returns for "aggregate" or member_id="all":
    {
        "fleet_health": 58,
        "member_count": 5,
        "struggling_count": 1,
        "vulnerable_count": 2,
        "stable_count": 2,
        "thriving_count": 0,
        "members": [...]
    }

    Permission: None (local cl-revenue-ops integration)
    """
    # No permission check - this is for local cl-revenue-ops integration

    if not database or not health_aggregator:
        return {"error": "Health tracking not initialized"}

    # Handle "all" member_id or "aggregate" action
    if member_id == "all" or action == "aggregate":
        summary = health_aggregator.get_fleet_health_summary()
        return summary

    # Query specific member or self
    target_id = member_id if member_id else our_pubkey
    if not target_id:
        return {"error": "No member specified and our_pubkey not set"}

    health = health_aggregator.get_our_health(target_id)
    if not health:
        return {
            "member_id": target_id,
            "error": "No health record found",
            # Return defaults for graceful degradation
            "health_score": 50,
            "health_tier": "stable",
            "budget_multiplier": 1.0
        }

    # Rename overall_health to health_score for API consistency
    health["health_score"] = health.pop("overall_health", 50)
    health["member_id"] = target_id

    return health


@plugin.method("hive-report-health")
def hive_report_health(
    plugin: Plugin,
    profitable_channels: int,
    underwater_channels: int,
    stagnant_channels: int,
    total_channels: int = None,
    revenue_trend: str = "stable",
    liquidity_score: int = 50
):
    """
    Report health status from cl-revenue-ops.

    Called periodically by cl-revenue-ops profitability analyzer.
    This shares INFORMATION - no sats move between nodes.

    The health score is calculated from profitability metrics and used
    to determine the node's NNLB budget multiplier for its own operations.

    Args:
        profitable_channels: Number of channels classified as profitable
        underwater_channels: Number of channels classified as underwater
        stagnant_channels: Number of stagnant/zombie channels
        total_channels: Total channel count (defaults to sum of above)
        revenue_trend: "improving", "stable", or "declining"
        liquidity_score: Liquidity balance score 0-100 (default 50)

    Returns:
        {"status": "reported", "health_score": 65, "health_tier": "stable",
         "budget_multiplier": 1.0}

    Permission: None (local cl-revenue-ops integration)
    """
    # No permission check - this is for local cl-revenue-ops integration

    if not database or not health_aggregator or not our_pubkey:
        return {"error": "Health tracking not initialized"}

    # Calculate total if not provided
    if total_channels is None:
        total_channels = profitable_channels + underwater_channels + stagnant_channels

    # Validate inputs
    if total_channels < 0:
        return {"error": "total_channels must be non-negative"}
    if revenue_trend not in ["improving", "stable", "declining"]:
        revenue_trend = "stable"
    liquidity_score = max(0, min(100, liquidity_score))

    try:
        # Update our health using the aggregator
        result = health_aggregator.update_our_health(
            profitable_channels=profitable_channels,
            underwater_channels=underwater_channels,
            stagnant_channels=stagnant_channels,
            total_channels=total_channels,
            revenue_trend=revenue_trend,
            liquidity_score=liquidity_score,
            our_pubkey=our_pubkey
        )

        return {
            "status": "reported",
            "health_score": result.get("health_score", 50),
            "health_tier": result.get("health_tier", "stable"),
            "budget_multiplier": result.get("budget_multiplier", 1.0)
        }

    except Exception as e:
        plugin.log(f"Error updating health: {e}", level='warn')
        return {"error": f"Failed to update health: {e}"}


@plugin.method("hive-calculate-health")
def hive_calculate_health(plugin: Plugin):
    """
    Calculate and return our node's health score.

    Uses local channel and revenue data to calculate health scores
    for NNLB purposes.

    Returns:
        Dict with our health assessment.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not fee_intel_mgr or not safe_plugin:
        return {"error": "Not initialized"}

    # Get our channel data
    try:
        funds = safe_plugin.rpc.listfunds()
        channels = funds.get("channels", [])

        capacity_sats = sum(
            ch.get("our_amount_msat", 0) // 1000 + ch.get("amount_msat", 0) // 1000 - ch.get("our_amount_msat", 0) // 1000
            for ch in channels if ch.get("state") == "CHANNELD_NORMAL"
        )
        available_sats = sum(
            ch.get("our_amount_msat", 0) // 1000
            for ch in channels if ch.get("state") == "CHANNELD_NORMAL"
        )
        channel_count = len([ch for ch in channels if ch.get("state") == "CHANNELD_NORMAL"])

    except Exception as e:
        return {"error": f"Failed to get channel data: {e}"}

    # Get hive averages for comparison
    all_health = database.get_all_member_health() if database else []
    if all_health:
        hive_avg_capacity = sum(h.get("capacity_score", 50) for h in all_health) / len(all_health) * 200000
    else:
        hive_avg_capacity = 10_000_000  # 10M default

    # Calculate health (revenue estimation simplified)
    health = fee_intel_mgr.calculate_our_health(
        capacity_sats=capacity_sats,
        available_sats=available_sats,
        channel_count=channel_count,
        daily_revenue_sats=0,  # Would need forwarding stats
        hive_avg_capacity=int(hive_avg_capacity)
    )

    return {
        "our_pubkey": our_pubkey,
        "channel_count": channel_count,
        "capacity_sats": capacity_sats,
        "available_sats": available_sats,
        **health
    }


@plugin.method("hive-routing-stats")
def hive_routing_stats(plugin: Plugin):
    """
    Get routing intelligence statistics.

    Shows collective routing intelligence from all hive members including
    path success rates, probe counts, and route suggestions.

    Returns:
        Dict with routing intelligence statistics.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not routing_map:
        return {"error": "Routing intelligence not initialized"}

    stats = routing_map.get_routing_stats()
    return {
        "paths_tracked": stats.get("total_paths", 0),
        "total_probes": stats.get("total_probes", 0),
        "total_successes": stats.get("total_successes", 0),
        "unique_destinations": stats.get("unique_destinations", 0),
        "high_quality_paths": stats.get("high_quality_paths", 0),
        "overall_success_rate": round(stats.get("overall_success_rate", 0.0), 3),
    }


@plugin.method("hive-route-suggest")
def hive_route_suggest(plugin: Plugin, destination: str, amount_sats: int = 100000):
    """
    Get route suggestions for a destination using hive intelligence.

    Uses collective routing data to suggest optimal paths.

    Args:
        destination: Target node pubkey
        amount_sats: Amount to route (default 100000)

    Returns:
        Dict with route suggestions.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not routing_map:
        return {"error": "Routing intelligence not initialized"}

    routes = routing_map.get_routes_to(destination, amount_sats)

    return {
        "destination": destination,
        "amount_sats": amount_sats,
        "route_count": len(routes),
        "routes": [
            {
                "path": list(r.path),
                "success_rate": r.success_rate,
                "expected_latency_ms": r.expected_latency_ms,
                "confidence": r.confidence,
            }
            for r in routes[:5]  # Top 5 suggestions
        ]
    }


@plugin.method("hive-peer-reputations")
def hive_peer_reputations(plugin: Plugin, peer_id: str = None):
    """
    Get aggregated peer reputations from hive intelligence.

    Peer reputations are aggregated from reports by all hive members
    with outlier detection to prevent manipulation.

    Args:
        peer_id: Optional specific peer to query

    Returns:
        Dict with peer reputation data.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not peer_reputation_mgr:
        return {"error": "Peer reputation manager not initialized"}

    if peer_id:
        rep = peer_reputation_mgr.get_reputation(peer_id)
        if not rep:
            return {
                "peer_id": peer_id,
                "error": "No reputation data found"
            }
        return {
            "peer_id": rep.peer_id,
            "reputation_score": rep.reputation_score,
            "confidence": rep.confidence,
            "avg_uptime": rep.avg_uptime,
            "avg_htlc_success": rep.avg_htlc_success,
            "avg_fee_stability": rep.avg_fee_stability,
            "total_force_closes": rep.total_force_closes,
            "report_count": rep.report_count,
            "reporter_count": len(rep.reporters),
            "warnings": rep.warnings,
        }
    else:
        stats = peer_reputation_mgr.get_reputation_stats()
        all_reps = peer_reputation_mgr.get_all_reputations()
        return {
            **stats,
            "reputations": [
                {
                    "peer_id": rep.peer_id,
                    "reputation_score": rep.reputation_score,
                    "confidence": rep.confidence,
                    "warnings": list(rep.warnings.keys()),
                }
                for rep in all_reps.values()
            ]
        }


@plugin.method("hive-reputation-stats")
def hive_reputation_stats(plugin: Plugin):
    """
    Get overall reputation tracking statistics.

    Returns summary statistics about tracked peer reputations.

    Returns:
        Dict with reputation statistics.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not peer_reputation_mgr:
        return {"error": "Peer reputation manager not initialized"}

    return peer_reputation_mgr.get_reputation_stats()


@plugin.method("hive-liquidity-needs")
def hive_liquidity_needs(plugin: Plugin, peer_id: str = None):
    """
    Get current liquidity needs from hive members.

    Shows liquidity requests from members that may need assistance
    with rebalancing or capacity.

    Args:
        peer_id: Optional filter by specific member

    Returns:
        Dict with liquidity needs.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database:
        return {"error": "Database not initialized"}

    if peer_id:
        needs = database.get_liquidity_needs_for_reporter(peer_id)
    else:
        needs = database.get_all_liquidity_needs(max_age_hours=24)

    return {
        "need_count": len(needs),
        "needs": needs
    }


@plugin.method("hive-liquidity-status")
def hive_liquidity_status(plugin: Plugin):
    """
    Get liquidity coordination status.

    Shows rebalance proposals, pending needs, and assistance statistics.

    Returns:
        Dict with liquidity coordination status.

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not liquidity_coord:
        return {"error": "Liquidity coordinator not initialized"}

    return liquidity_coord.get_status()


@plugin.method("hive-liquidity-state")
def hive_liquidity_state(plugin: Plugin, action: str = "status"):
    """
    Query fleet liquidity state for coordination.

    INFORMATION ONLY - no sats move between nodes. This enables nodes
    to make better independent decisions about fees and rebalancing.

    Args:
        action: "status" (overview), "needs" (who needs what)

    Returns for "status":
        Fleet liquidity state overview including:
        - Members with depleted/saturated channels
        - Common bottleneck peers
        - Rebalancing activity

    Returns for "needs":
        List of fleet liquidity needs with relevance scores

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not liquidity_coord:
        return {"error": "Liquidity coordinator not initialized"}

    if action == "status":
        return liquidity_coord.get_fleet_liquidity_state()
    elif action == "needs":
        return {"fleet_needs": liquidity_coord.get_fleet_liquidity_needs()}
    else:
        return {"error": f"Unknown action: {action}"}


@plugin.method("hive-report-liquidity-state")
def hive_report_liquidity_state(
    plugin: Plugin,
    depleted_channels: list = None,
    saturated_channels: list = None,
    rebalancing_active: bool = False,
    rebalancing_peers: list = None
):
    """
    Report liquidity state from cl-revenue-ops.

    INFORMATION SHARING - enables coordinated fee/rebalance decisions.
    No sats transfer between nodes.

    Called periodically by cl-revenue-ops profitability analyzer to share
    current channel states with the fleet.

    Args:
        depleted_channels: List of {peer_id, local_pct, capacity_sats}
        saturated_channels: List of {peer_id, local_pct, capacity_sats}
        rebalancing_active: Whether we're currently rebalancing
        rebalancing_peers: Which peers we're rebalancing through

    Returns:
        {"status": "recorded", "depleted_count": N, "saturated_count": M}

    Permission: None (local cl-revenue-ops integration)
    """
    # No permission check - this is for local cl-revenue-ops integration

    if not liquidity_coord or not our_pubkey:
        return {"error": "Liquidity coordinator not initialized"}

    return liquidity_coord.record_member_liquidity_report(
        member_id=our_pubkey,
        depleted_channels=depleted_channels or [],
        saturated_channels=saturated_channels or [],
        rebalancing_active=rebalancing_active,
        rebalancing_peers=rebalancing_peers
    )


@plugin.method("hive-check-rebalance-conflict")
def hive_check_rebalance_conflict(plugin: Plugin, peer_id: str):
    """
    Check if another fleet member is rebalancing through a peer.

    INFORMATION ONLY - helps avoid competing for same routes.

    Args:
        peer_id: The peer to check

    Returns:
        Conflict info if another member is rebalancing through this peer

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not liquidity_coord:
        return {"error": "Liquidity coordinator not initialized"}

    return liquidity_coord.check_rebalancing_conflict(peer_id)


@plugin.method("hive-splice-check")
def hive_splice_check(
    plugin: Plugin,
    peer_id: str,
    splice_type: str,
    amount_sats: int,
    channel_id: str = None
):
    """
    Check if a splice operation is safe for fleet connectivity.

    SAFETY CHECK ONLY - no fund movement between nodes.
    Each node manages its own splices. This is advisory.

    Use this before performing splice-out to ensure fleet connectivity
    is maintained. Splice-in is always safe (increases capacity).

    Args:
        peer_id: External peer being spliced from/to
        splice_type: "splice_in" or "splice_out"
        amount_sats: Amount to splice in/out
        channel_id: Optional specific channel ID

    Returns for splice_out:
        {
            "safety": "safe" | "coordinate" | "blocked",
            "reason": str,
            "can_proceed": bool,
            "fleet_capacity": int,
            "new_fleet_capacity": int,
            "fleet_share": float,
            "new_share": float,
            "recommendation": str (if not safe)
        }

    Returns for splice_in:
        {"safety": "safe", "reason": "Splice-in always safe"}

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not splice_coord:
        return {"error": "Splice coordinator not initialized"}

    if splice_type == "splice_in":
        return splice_coord.check_splice_in_safety(peer_id, amount_sats)
    elif splice_type == "splice_out":
        return splice_coord.check_splice_out_safety(peer_id, amount_sats, channel_id)
    else:
        return {"error": f"Unknown splice_type: {splice_type}, use 'splice_in' or 'splice_out'"}


@plugin.method("hive-splice-recommendations")
def hive_splice_recommendations(plugin: Plugin, peer_id: str):
    """
    Get splice recommendations for a specific peer.

    Returns info about fleet connectivity and safe splice amounts.
    INFORMATION ONLY - helps nodes make informed splice decisions.

    Args:
        peer_id: External peer to analyze

    Returns:
        {
            "peer_id": str,
            "fleet_capacity": int,
            "our_capacity": int,
            "other_member_capacity": int,
            "safe_splice_out_amount": int,
            "has_fleet_coverage": bool,
            "recommendations": [str]
        }

    Permission: Member or Admin
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not splice_coord:
        return {"error": "Splice coordinator not initialized"}

    return splice_coord.get_splice_recommendations(peer_id)


@plugin.method("hive-set-mode")
def hive_set_mode(plugin: Plugin, mode: str):
    """
    Change the governance mode at runtime.

    Args:
        mode: New governance mode ('advisor' or 'autonomous')

    Returns:
        Dict with new mode and previous mode.

    Permission: Admin only
    """
    return rpc_set_mode(_get_hive_context(), mode)


@plugin.method("hive-enable-expansions")
def hive_enable_expansions(plugin: Plugin, enabled: bool = True):
    """
    Enable or disable expansion proposals at runtime.

    Args:
        enabled: True to enable expansions, False to disable (default: True)

    Returns:
        Dict with new setting.

    Permission: Admin only
    """
    return rpc_enable_expansions(_get_hive_context(), enabled)


@plugin.method("hive-vouch")
def hive_vouch(plugin: Plugin, peer_id: str):
    """
    Manually vouch for a neophyte to support their promotion.

    Args:
        peer_id: Public key of the neophyte to vouch for

    Returns:
        Dict with vouch status.
    """
    if not config or not config.membership_enabled:
        return {"error": "membership_disabled"}
    if not membership_mgr or not our_pubkey or not database:
        return {"error": "membership_unavailable"}

    # Check our tier - must be member or admin to vouch
    our_tier = membership_mgr.get_tier(our_pubkey)
    if our_tier not in (MembershipTier.MEMBER.value,):
        return {"error": "permission_denied", "required_tier": "member"}

    # Check target is a neophyte
    target = database.get_member(peer_id)
    if not target:
        return {"error": "peer_not_found", "peer_id": peer_id}
    if target.get("tier") != MembershipTier.NEOPHYTE.value:
        return {"error": "peer_not_neophyte", "current_tier": target.get("tier")}

    # Check if target has a pending promotion request
    requests = database.get_promotion_requests(peer_id)
    pending_request = None
    for req in requests:
        if req.get("status") == "pending":
            pending_request = req
            break

    if not pending_request:
        return {"error": "no_pending_promotion_request", "peer_id": peer_id}

    request_id = pending_request["request_id"]

    # Check if we already vouched
    existing_vouches = database.get_promotion_vouches(peer_id, request_id)
    for vouch in existing_vouches:
        if vouch.get("voucher_peer_id") == our_pubkey:
            return {"error": "already_vouched", "peer_id": peer_id}

    # Create and sign vouch
    vouch_ts = int(time.time())
    canonical = membership_mgr.build_vouch_message(peer_id, request_id, vouch_ts)

    try:
        sig = safe_plugin.rpc.signmessage(canonical)["zbase"]
    except Exception as e:
        return {"error": f"Failed to sign vouch: {e}"}

    # Store locally
    database.add_promotion_vouch(peer_id, request_id, our_pubkey, sig, vouch_ts)

    # Broadcast to members
    vouch_payload = {
        "target_pubkey": peer_id,
        "request_id": request_id,
        "timestamp": vouch_ts,
        "voucher_pubkey": our_pubkey,
        "sig": sig
    }
    vouch_msg = serialize(HiveMessageType.VOUCH, vouch_payload)
    _broadcast_to_members(vouch_msg)

    # Check if quorum reached
    all_vouches = database.get_promotion_vouches(peer_id, request_id)
    active_members = membership_mgr.get_active_members()
    quorum = membership_mgr.calculate_quorum(len(active_members))
    quorum_reached = len(all_vouches) >= quorum

    # Auto-promote if quorum reached
    if quorum_reached and config.auto_promote_enabled:
        # Update member tier via membership manager (triggers set_hive_policy)
        membership_mgr.set_tier(peer_id, MembershipTier.MEMBER.value)
        database.update_promotion_request_status(peer_id, request_id, "accepted")
        plugin.log(f"cl-hive: Promoted {peer_id[:16]}... to member (quorum reached)")

        # Broadcast PROMOTION message
        promotion_payload = {
            "target_pubkey": peer_id,
            "request_id": request_id,
            "vouches": [
                {
                    "target_pubkey": v["target_peer_id"],
                    "request_id": v["request_id"],
                    "timestamp": v["timestamp"],
                    "voucher_pubkey": v["voucher_peer_id"],
                    "sig": v["sig"]
                } for v in all_vouches[:MAX_VOUCHES_IN_PROMOTION]
            ]
        }
        promo_msg = serialize(HiveMessageType.PROMOTION, promotion_payload)
        _broadcast_to_members(promo_msg)

    return {
        "status": "vouched",
        "peer_id": peer_id,
        "request_id": request_id,
        "vouch_count": len(all_vouches),
        "quorum_needed": quorum,
        "quorum_reached": quorum_reached,
    }


@plugin.method("hive-force-promote")
def hive_force_promote(plugin: Plugin, peer_id: str):
    """
    Admin command to force-promote a neophyte to member during bootstrap.

    This bypasses the normal quorum requirement when the hive is too small
    to reach quorum naturally. Only works when total member count < min_vouch_count.

    Args:
        peer_id: Public key of the neophyte to promote

    Returns:
        Dict with promotion status.

    Permission: Admin only, bootstrap phase only
    """
    # Permission check: Admin only
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database or not our_pubkey or not membership_mgr:
        return {"error": "Database not initialized"}

    # Check we're in bootstrap phase (member count < 3)
    # Note: This function is deprecated as admin tier was removed
    members = database.get_all_members()
    member_count = len(members)
    min_for_quorum = 3  # Hardcoded - vouch system removed

    if member_count >= min_for_quorum:
        return {
            "error": "bootstrap_complete",
            "message": f"Hive has {member_count} members, use normal promotion process",
            "member_count": member_count
        }

    # Check target is a neophyte
    target = database.get_member(peer_id)
    if not target:
        return {"error": "peer_not_found", "peer_id": peer_id}
    if target.get("tier") != MembershipTier.NEOPHYTE.value:
        return {"error": "peer_not_neophyte", "current_tier": target.get("tier")}

    # Force promote via membership manager (triggers set_hive_policy)
    success = membership_mgr.set_tier(peer_id, MembershipTier.MEMBER.value)
    if not success:
        return {"error": "promotion_failed", "peer_id": peer_id}

    plugin.log(f"cl-hive: Force-promoted {peer_id[:16]}... to member (bootstrap)")

    # Broadcast PROMOTION message to sync state
    promotion_payload = {
        "target_pubkey": peer_id,
        "request_id": f"bootstrap_{int(time.time())}",
        "vouches": [{
            "target_pubkey": peer_id,
            "request_id": f"bootstrap_{int(time.time())}",
            "timestamp": int(time.time()),
            "voucher_pubkey": our_pubkey,
            "sig": "admin_bootstrap"
        }]
    }
    promo_msg = serialize(HiveMessageType.PROMOTION, promotion_payload)
    _broadcast_to_members(promo_msg)

    return {
        "status": "promoted",
        "peer_id": peer_id,
        "new_tier": MembershipTier.MEMBER.value,
        "method": "admin_bootstrap",
        "remaining_bootstrap_slots": min_vouch - member_count - 1
    }


@plugin.method("hive-ban")
def hive_ban(plugin: Plugin, peer_id: str, reason: str):
    """
    Propose a ban for a peer.

    Args:
        peer_id: Public key of the peer to ban
        reason: Reason for the ban

    Returns:
        Dict with ban status.

    Permission: Admin only
    """
    # Permission check: Admin only
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database or not our_pubkey:
        return {"error": "Database not initialized"}

    # Check if already banned
    if database.is_banned(peer_id):
        return {"error": "peer_already_banned", "peer_id": peer_id}

    # Check if peer is a member
    member = database.get_member(peer_id)
    if not member:
        return {"error": "peer_not_member", "peer_id": peer_id}

    # Cannot ban admin
    if member.get("tier") == MembershipTier.MEMBER.value:
        return {"error": "cannot_ban_member", "peer_id": peer_id}

    # Sign the ban reason
    now = int(time.time())
    ban_message = f"BAN:{peer_id}:{reason}:{now}"

    try:
        sig = safe_plugin.rpc.signmessage(ban_message)["zbase"]
    except Exception as e:
        return {"error": f"Failed to sign ban: {e}"}

    # Add ban to database
    expires_at = now + (365 * 86400)  # 1 year default
    success = database.add_ban(
        peer_id=peer_id,
        reason=reason,
        reporter=our_pubkey,
        signature=sig,
        expires_at=expires_at
    )

    if not success:
        return {"error": "Failed to add ban", "peer_id": peer_id}

    plugin.log(f"cl-hive: Banned peer {peer_id[:16]}... reason: {reason}")

    return {
        "status": "banned",
        "peer_id": peer_id,
        "reason": reason,
        "reporter": our_pubkey,
        "expires_at": expires_at,
    }


@plugin.method("hive-promote-admin")
def hive_promote_admin(plugin: Plugin, peer_id: str):
    """
    DEPRECATED: Admin tier has been removed from the 2-tier membership system.

    The current system uses only NEOPHYTE and MEMBER tiers.
    Use hive-propose-promotion to promote neophytes to member.
    """
    return {
        "error": "deprecated",
        "message": "Admin tier removed. Use hive-propose-promotion for neophyte->member promotions."
    }


@plugin.method("hive-leave")
def hive_leave(plugin: Plugin, reason: str = "voluntary"):
    """
    Voluntarily leave the hive.

    This removes you from the hive member list and notifies other members.
    Your fee policies will be reverted to dynamic.

    Restrictions:
    - The last full member cannot leave (would make hive headless)
    - Promote a neophyte to member before leaving if you're the last one

    Args:
        reason: Optional reason for leaving (default: "voluntary")

    Returns:
        Dict with leave status.

    Permission: Any member
    """
    if not database or not our_pubkey or not safe_plugin:
        return {"error": "Hive not initialized"}

    # Check we're a member of the hive
    member = database.get_member(our_pubkey)
    if not member:
        return {"error": "not_a_member", "message": "You are not a member of any hive"}

    our_tier = member.get("tier")

    # Check if we're the last full member
    if our_tier == MembershipTier.MEMBER.value:
        all_members = database.get_all_members()
        member_count = sum(1 for m in all_members if m.get("tier") == MembershipTier.MEMBER.value)
        if member_count <= 1:
            return {
                "error": "cannot_leave",
                "message": "Cannot leave: you are the only full member. Promote a neophyte first, or the hive will become headless."
            }

    # Create signed leave message
    timestamp = int(time.time())
    canonical = f"hive:leave:{our_pubkey}:{timestamp}:{reason}"

    try:
        sig = safe_plugin.rpc.signmessage(canonical)["zbase"]
    except Exception as e:
        return {"error": f"Failed to sign leave message: {e}"}

    # Broadcast to members before removing ourselves
    leave_payload = {
        "peer_id": our_pubkey,
        "timestamp": timestamp,
        "reason": reason,
        "signature": sig
    }
    leave_msg = serialize(HiveMessageType.MEMBER_LEFT, leave_payload)
    _broadcast_to_members(leave_msg)

    # Revert our fee policy to dynamic
    if bridge and bridge.status == BridgeStatus.ENABLED:
        try:
            bridge.set_hive_policy(our_pubkey, is_member=False)
        except Exception:
            pass  # Best effort

    # Remove ourselves from the member list
    database.remove_member(our_pubkey)
    plugin.log(f"cl-hive: Left the hive ({our_tier}): {reason}")

    return {
        "status": "left",
        "peer_id": our_pubkey,
        "former_tier": our_tier,
        "reason": reason,
        "message": "You have left the hive. Fee policies reverted to dynamic."
    }


@plugin.method("hive-propose-ban")
def hive_propose_ban(plugin: Plugin, peer_id: str, reason: str = "no reason given"):
    """
    Propose banning a member from the hive.

    Requires quorum vote (51% of members) to execute.
    The proposal is valid for 7 days.

    Args:
        peer_id: Public key of the member to ban
        reason: Reason for the ban proposal (max 500 chars)

    Returns:
        Dict with proposal status.

    Permission: Member or Admin
    """
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database or not our_pubkey or not safe_plugin:
        return {"error": "Hive not initialized"}

    # Validate reason length
    if len(reason) > 500:
        return {"error": "reason_too_long", "max_length": 500}

    # Check target exists and is a member
    target = database.get_member(peer_id)
    if not target:
        return {"error": "peer_not_found", "peer_id": peer_id}

    # Cannot ban yourself
    if peer_id == our_pubkey:
        return {"error": "cannot_ban_self"}

    # Check for existing pending proposal
    existing = database.get_ban_proposal_for_target(peer_id)
    if existing and existing.get("status") == "pending":
        return {
            "error": "proposal_exists",
            "proposal_id": existing["proposal_id"],
            "message": "A ban proposal already exists for this peer"
        }

    # Generate proposal ID
    proposal_id = secrets.token_hex(16)
    timestamp = int(time.time())

    # Sign the proposal
    canonical = f"hive:ban_proposal:{proposal_id}:{peer_id}:{timestamp}:{reason}"
    try:
        sig = safe_plugin.rpc.signmessage(canonical)["zbase"]
    except Exception as e:
        return {"error": f"Failed to sign proposal: {e}"}

    # Store locally
    expires_at = timestamp + BAN_PROPOSAL_TTL_SECONDS
    database.create_ban_proposal(proposal_id, peer_id, our_pubkey,
                                 reason, timestamp, expires_at)

    # Add our vote (proposer auto-votes approve)
    vote_canonical = f"hive:ban_vote:{proposal_id}:approve:{timestamp}"
    vote_sig = safe_plugin.rpc.signmessage(vote_canonical)["zbase"]
    database.add_ban_vote(proposal_id, our_pubkey, "approve", timestamp, vote_sig)

    # Broadcast proposal
    proposal_payload = {
        "proposal_id": proposal_id,
        "target_peer_id": peer_id,
        "proposer_peer_id": our_pubkey,
        "reason": reason,
        "timestamp": timestamp,
        "signature": sig
    }
    proposal_msg = serialize(HiveMessageType.BAN_PROPOSAL, proposal_payload)
    _broadcast_to_members(proposal_msg)

    # Also broadcast our vote
    vote_payload = {
        "proposal_id": proposal_id,
        "voter_peer_id": our_pubkey,
        "vote": "approve",
        "timestamp": timestamp,
        "signature": vote_sig
    }
    vote_msg = serialize(HiveMessageType.BAN_VOTE, vote_payload)
    _broadcast_to_members(vote_msg)

    # Calculate quorum info
    all_members = database.get_all_members()
    eligible = [m for m in all_members
                if m.get("tier") in (MembershipTier.MEMBER.value,)
                and m["peer_id"] != peer_id]
    quorum_needed = int(len(eligible) * BAN_QUORUM_THRESHOLD) + 1

    plugin.log(f"cl-hive: Ban proposal created for {peer_id[:16]}...: {reason}")

    return {
        "status": "proposed",
        "proposal_id": proposal_id,
        "target_peer_id": peer_id,
        "reason": reason,
        "expires_at": expires_at,
        "votes_needed": quorum_needed,
        "votes_received": 1,
        "message": f"Ban proposal created. Need {quorum_needed} votes to execute."
    }


@plugin.method("hive-vote-ban")
def hive_vote_ban(plugin: Plugin, proposal_id: str, vote: str):
    """
    Vote on a pending ban proposal.

    Args:
        proposal_id: ID of the ban proposal
        vote: "approve" or "reject"

    Returns:
        Dict with vote status.

    Permission: Member or Admin
    """
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database or not our_pubkey or not safe_plugin:
        return {"error": "Hive not initialized"}

    # Validate vote
    if vote not in ("approve", "reject"):
        return {"error": "invalid_vote", "valid_options": ["approve", "reject"]}

    # Get proposal
    proposal = database.get_ban_proposal(proposal_id)
    if not proposal:
        return {"error": "proposal_not_found", "proposal_id": proposal_id}

    if proposal.get("status") != "pending":
        return {
            "error": "proposal_not_pending",
            "status": proposal.get("status"),
            "message": f"Proposal is {proposal.get('status')}, cannot vote"
        }

    # Check if expired
    now = int(time.time())
    if now > proposal.get("expires_at", 0):
        database.update_ban_proposal_status(proposal_id, "expired")
        return {"error": "proposal_expired"}

    # Cannot vote on proposal targeting self
    if proposal["target_peer_id"] == our_pubkey:
        return {"error": "cannot_vote_on_own_ban"}

    # Check if already voted
    existing_vote = database.get_ban_vote(proposal_id, our_pubkey)
    if existing_vote:
        if existing_vote["vote"] == vote:
            return {"error": "already_voted", "vote": vote}
        # Allow changing vote

    # Sign vote
    timestamp = int(time.time())
    canonical = f"hive:ban_vote:{proposal_id}:{vote}:{timestamp}"
    try:
        sig = safe_plugin.rpc.signmessage(canonical)["zbase"]
    except Exception as e:
        return {"error": f"Failed to sign vote: {e}"}

    # Store vote
    database.add_ban_vote(proposal_id, our_pubkey, vote, timestamp, sig)

    # Broadcast vote
    vote_payload = {
        "proposal_id": proposal_id,
        "voter_peer_id": our_pubkey,
        "vote": vote,
        "timestamp": timestamp,
        "signature": sig
    }
    vote_msg = serialize(HiveMessageType.BAN_VOTE, vote_payload)
    _broadcast_to_members(vote_msg)

    # Check if quorum reached
    was_executed = _check_ban_quorum(proposal_id, proposal, plugin)

    # Get current vote counts
    all_votes = database.get_ban_votes(proposal_id)
    all_members = database.get_all_members()
    eligible = [m for m in all_members
                if m.get("tier") in (MembershipTier.MEMBER.value,)
                and m["peer_id"] != proposal["target_peer_id"]]
    eligible_ids = set(m["peer_id"] for m in eligible)

    approve_count = sum(1 for v in all_votes if v["vote"] == "approve" and v["voter_peer_id"] in eligible_ids)
    reject_count = sum(1 for v in all_votes if v["vote"] == "reject" and v["voter_peer_id"] in eligible_ids)
    quorum_needed = int(len(eligible) * BAN_QUORUM_THRESHOLD) + 1

    result = {
        "status": "voted",
        "proposal_id": proposal_id,
        "vote": vote,
        "approve_count": approve_count,
        "reject_count": reject_count,
        "quorum_needed": quorum_needed,
    }

    if was_executed:
        result["status"] = "ban_executed"
        result["message"] = f"Ban executed! Target {proposal['target_peer_id'][:16]}... removed from hive."
    else:
        result["message"] = f"Vote recorded. {approve_count}/{quorum_needed} approvals."

    return result


@plugin.method("hive-pending-bans")
def hive_pending_bans(plugin: Plugin):
    """
    View pending ban proposals.

    Returns:
        Dict with pending ban proposals and their vote counts.

    Permission: Any member
    """
    return rpc_pending_bans(_get_hive_context())


@plugin.method("hive-contribution")
def hive_contribution(plugin: Plugin, peer_id: str = None):
    """
    View contribution stats for a peer or self.

    Args:
        peer_id: Optional peer to view (defaults to self)

    Returns:
        Dict with contribution statistics.
    """
    return rpc_contribution(_get_hive_context(), peer_id=peer_id)


# =============================================================================
# ROUTING POOL COMMANDS (Phase 0 - Collective Economics)
# =============================================================================

@plugin.method("hive-pool-status")
def hive_pool_status(plugin: Plugin, period: str = None):
    """
    Get current routing pool status and statistics.

    Args:
        period: Optional period to query (format: YYYY-WW, defaults to current week)

    Returns:
        Dict with pool status including revenue, contributions, and distributions.
    """
    return rpc_pool_status(_get_hive_context(), period=period)


@plugin.method("hive-pool-member-status")
def hive_pool_member_status(plugin: Plugin, peer_id: str = None):
    """
    Get routing pool status for a specific member.

    Args:
        peer_id: Member pubkey (defaults to self)

    Returns:
        Dict with member's pool status and history.
    """
    return rpc_pool_member_status(_get_hive_context(), peer_id=peer_id)


@plugin.method("hive-pool-snapshot")
def hive_pool_snapshot(plugin: Plugin, period: str = None):
    """
    Trigger a contribution snapshot for all hive members.

    Permission: Admin only

    Args:
        period: Optional period (format: YYYY-WW, defaults to current week)

    Returns:
        Dict with snapshot results.
    """
    return rpc_pool_snapshot(_get_hive_context(), period=period)


@plugin.method("hive-pool-distribution")
def hive_pool_distribution(plugin: Plugin, period: str = None):
    """
    Calculate distribution amounts for a period (dry run).

    Args:
        period: Optional period (format: YYYY-WW, defaults to current week)

    Returns:
        Dict with calculated distribution amounts.
    """
    return rpc_pool_distribution(_get_hive_context(), period=period)


@plugin.method("hive-pool-settle")
def hive_pool_settle(plugin: Plugin, period: str = None, dry_run: bool = True):
    """
    Settle a routing pool period and record distributions.

    Permission: Admin only

    Args:
        period: Period to settle (format: YYYY-WW, defaults to PREVIOUS week)
        dry_run: If True, calculate but don't record (default: True)

    Returns:
        Dict with settlement results.
    """
    return rpc_pool_settle(_get_hive_context(), period=period, dry_run=dry_run)


@plugin.method("hive-pool-record-revenue")
def hive_pool_record_revenue(plugin: Plugin, amount_sats: int,
                              channel_id: str = None, payment_hash: str = None):
    """
    Manually record routing revenue to the pool.

    Permission: Admin only

    Args:
        amount_sats: Revenue amount in satoshis
        channel_id: Optional channel ID
        payment_hash: Optional payment hash

    Returns:
        Dict with recording result.
    """
    return rpc_pool_record_revenue(
        _get_hive_context(),
        amount_sats=amount_sats,
        channel_id=channel_id,
        payment_hash=payment_hash
    )


# =============================================================================
# SETTLEMENT RPC METHODS (BOLT12 Revenue Distribution)
# =============================================================================

@plugin.method("hive-settlement-register-offer")
def hive_settlement_register_offer(plugin: Plugin, peer_id: str, bolt12_offer: str):
    """
    Register a BOLT12 offer for receiving settlement payments.

    Each hive member must register their offer to participate in revenue distribution.
    If registering your own offer, it will be broadcast to other hive members.

    Args:
        peer_id: Member's node public key
        bolt12_offer: BOLT12 offer string (starts with lno1...)

    Returns:
        Dict with registration result.
    """
    if not settlement_mgr:
        return {"error": "Settlement manager not initialized"}

    result = settlement_mgr.register_offer(peer_id, bolt12_offer)

    # Broadcast if this is our own offer and registration succeeded
    if "error" not in result and handshake_mgr:
        if peer_id == handshake_mgr.get_our_pubkey():
            broadcast_count = _broadcast_settlement_offer(peer_id, bolt12_offer)
            result["broadcast_count"] = broadcast_count

    return result


@plugin.method("hive-settlement-generate-offer")
def hive_settlement_generate_offer(plugin: Plugin):
    """
    Auto-generate and register a BOLT12 offer for this node.

    This creates a new BOLT12 offer for receiving settlement payments
    and registers it automatically. The offer is broadcast to all hive members.

    Returns:
        Dict with offer generation result.
    """
    if not settlement_mgr:
        return {"error": "Settlement manager not initialized"}
    if not handshake_mgr:
        return {"error": "Handshake manager not initialized"}

    our_pubkey = handshake_mgr.get_our_pubkey()
    result = settlement_mgr.generate_and_register_offer(our_pubkey)

    # Broadcast to hive members if generation succeeded
    if "error" not in result:
        # Get the full offer from the database
        bolt12_offer = settlement_mgr.get_offer(our_pubkey)
        if bolt12_offer:
            broadcast_count = _broadcast_settlement_offer(our_pubkey, bolt12_offer)
            result["broadcast_count"] = broadcast_count

    return result


@plugin.method("hive-settlement-list-offers")
def hive_settlement_list_offers(plugin: Plugin):
    """
    List all registered BOLT12 offers for settlement.

    Returns:
        Dict with list of registered offers.
    """
    if not settlement_mgr:
        return {"error": "Settlement manager not initialized"}
    return settlement_mgr.list_offers()


@plugin.method("hive-settlement-calculate")
def hive_settlement_calculate(plugin: Plugin):
    """
    Calculate fair shares for the current period without executing.

    Shows what each member would receive/pay based on:
    - 40% capacity weight
    - 40% routing volume weight
    - 20% uptime weight

    Returns:
        Dict with calculated fair shares.
    """
    from modules.settlement import MemberContribution

    if not settlement_mgr:
        return {"error": "Settlement manager not initialized"}
    if not routing_pool:
        return {"error": "Routing pool not initialized"}
    if not database:
        return {"error": "Database not initialized"}

    # Get pool status with member contributions
    pool_status = routing_pool.get_pool_status()
    pool_contributions = pool_status.get("contributions", [])

    # Convert pool data to MemberContribution objects
    member_contributions = []
    for contrib in pool_contributions:
        peer_id = contrib.get("member_id_full", contrib.get("member_id", ""))
        if not peer_id:
            continue

        # Get forwarding stats from contribution ledger
        contrib_stats = database.get_contribution_stats(peer_id, window_days=7)
        forwards_sats = contrib_stats.get("forwarded", 0)

        # Get fees earned from cl-revenue-ops if available
        fees_earned = 0
        if bridge and bridge.status == BridgeStatus.ENABLED:
            try:
                peer_report = bridge.safe_call("revenue-report-peer", peer_id=peer_id)
                if peer_report and "error" not in peer_report:
                    fees_earned = peer_report.get("fees_earned_sats", 0)
            except Exception:
                pass  # Fallback to 0 if revenue-ops unavailable

        # Get BOLT12 offer if registered
        offer = settlement_mgr.get_offer(peer_id)

        member_contributions.append(MemberContribution(
            peer_id=peer_id,
            capacity_sats=contrib.get("capacity_sats", 0),
            forwards_sats=forwards_sats,
            fees_earned_sats=fees_earned,
            uptime_pct=contrib.get("uptime_pct", 0.0),
            bolt12_offer=offer
        ))

    # Calculate fair shares
    results = settlement_mgr.calculate_fair_shares(member_contributions)

    # Format for JSON response
    return {
        "period": pool_status.get("period", "unknown"),
        "total_members": len(results),
        "total_fees_sats": sum(r.fees_earned for r in results),
        "fair_shares": [
            {
                "peer_id": r.peer_id[:16] + "...",
                "peer_id_full": r.peer_id,
                "fees_earned": r.fees_earned,
                "fair_share": r.fair_share,
                "balance": r.balance,
                "has_offer": r.bolt12_offer is not None,
                "status": "pays" if r.balance < 0 else ("receives" if r.balance > 0 else "even")
            }
            for r in results
        ],
        "payments_required": []  # Will be populated when there's actual revenue
    }


@plugin.method("hive-settlement-execute")
def hive_settlement_execute(plugin: Plugin, dry_run: bool = True):
    """
    Execute settlement for the current period.

    Calculates fair shares and generates BOLT12 payments from members
    with surplus to members with deficit.

    Args:
        dry_run: If True, calculate but don't execute payments (default: True)

    Returns:
        Dict with settlement execution result.
    """
    # First calculate fair shares (reuses the calculation logic)
    calc_result = hive_settlement_calculate(plugin)

    if "error" in calc_result:
        return calc_result

    # For dry run, just return the calculation
    if dry_run:
        calc_result["execution_status"] = "dry_run"
        calc_result["message"] = "Dry run - no payments executed"
        return calc_result

    # For actual execution, generate and execute payments
    # Note: BOLT12 payment execution is not yet implemented
    calc_result["execution_status"] = "not_implemented"
    calc_result["message"] = "BOLT12 payment execution pending implementation"
    return calc_result


@plugin.method("hive-settlement-history")
def hive_settlement_history(plugin: Plugin, limit: int = 10):
    """
    Get settlement history showing past periods and distributions.

    Args:
        limit: Number of periods to return (default: 10)

    Returns:
        Dict with settlement history.
    """
    if not settlement_mgr:
        return {"error": "Settlement manager not initialized"}
    return {"settlement_periods": settlement_mgr.get_settlement_history(limit=limit)}


@plugin.method("hive-settlement-period-details")
def hive_settlement_period_details(plugin: Plugin, period_id: int):
    """
    Get detailed information about a specific settlement period.

    Args:
        period_id: Settlement period ID

    Returns:
        Dict with period details including contributions, fair shares, and payments.
    """
    if not settlement_mgr:
        return {"error": "Settlement manager not initialized"}
    return settlement_mgr.get_period_details(period_id)


# =============================================================================
# YIELD METRICS RPC METHODS (Phase 1 - Metrics & Measurement)
# =============================================================================

@plugin.method("hive-yield-metrics")
def hive_yield_metrics(plugin: Plugin, channel_id: str = None, period_days: int = 30):
    """
    Get yield metrics for channels.

    Args:
        channel_id: Optional specific channel ID (defaults to all channels)
        period_days: Analysis period in days (default: 30)

    Returns:
        Dict with channel yield metrics including ROI, capital efficiency, turn rate.
    """
    return rpc_yield_metrics(_get_hive_context(), channel_id=channel_id, period_days=period_days)


@plugin.method("hive-yield-summary")
def hive_yield_summary(plugin: Plugin, period_days: int = 30):
    """
    Get fleet-wide yield summary.

    Args:
        period_days: Analysis period in days (default: 30)

    Returns:
        Dict with fleet yield summary including total revenue, avg ROI, efficiency.
    """
    return rpc_yield_summary(_get_hive_context(), period_days=period_days)


@plugin.method("hive-velocity-prediction")
def hive_velocity_prediction(plugin: Plugin, channel_id: str, hours: int = 24):
    """
    Predict channel state based on flow velocity.

    Args:
        channel_id: Channel ID to predict
        hours: Prediction horizon in hours (default: 24)

    Returns:
        Dict with velocity prediction including depletion/saturation risk.
    """
    return rpc_velocity_prediction(_get_hive_context(), channel_id=channel_id, hours=hours)


@plugin.method("hive-critical-velocity")
def hive_critical_velocity(plugin: Plugin, threshold_hours: int = 24):
    """
    Get channels with critical velocity (depleting/filling rapidly).

    Args:
        threshold_hours: Alert threshold in hours (default: 24)

    Returns:
        Dict with channels predicted to deplete or saturate within threshold.
    """
    return rpc_critical_velocity_channels(_get_hive_context(), threshold_hours=threshold_hours)


@plugin.method("hive-internal-competition")
def hive_internal_competition(plugin: Plugin):
    """
    Detect internal competition between hive members.

    Returns:
        Dict with competition instances where multiple hive members
        compete for the same source/destination routes.
    """
    return rpc_internal_competition(_get_hive_context())


# =============================================================================
# PHASE 2 FEE COORDINATION RPC METHODS
# =============================================================================

@plugin.method("hive-coord-fee-recommendation")
def hive_coord_fee_recommendation(
    plugin: Plugin,
    channel_id: str,
    current_fee: int = 500,
    local_balance_pct: float = 0.5,
    source: str = None,
    destination: str = None
):
    """
    Get coordinated fee recommendation for a channel (Phase 2 Fee Coordination).

    Uses corridor ownership, pheromone levels, stigmergic markers, and defense
    signals to recommend optimal fees while avoiding internal fleet competition.

    Args:
        channel_id: Channel ID to get recommendation for
        current_fee: Current fee in ppm (default: 500)
        local_balance_pct: Current local balance percentage (default: 0.5)
        source: Source peer hint for corridor lookup
        destination: Destination peer hint for corridor lookup

    Returns:
        Dict with fee recommendation, reasoning, and coordination factors.
    """
    return rpc_fee_recommendation(
        _get_hive_context(),
        channel_id=channel_id,
        current_fee=current_fee,
        local_balance_pct=local_balance_pct,
        source=source,
        destination=destination
    )


@plugin.method("hive-corridor-assignments")
def hive_corridor_assignments(plugin: Plugin, force_refresh: bool = False):
    """
    Get flow corridor assignments for the fleet.

    Shows which member is primary for each (source, destination) pair.

    Args:
        force_refresh: Force refresh of cached assignments

    Returns:
        Dict with corridor assignments and statistics.
    """
    return rpc_corridor_assignments(_get_hive_context(), force_refresh=force_refresh)


@plugin.method("hive-stigmergic-markers")
def hive_stigmergic_markers(plugin: Plugin, source: str = None, destination: str = None):
    """
    Get stigmergic route markers from the fleet.

    Shows fee signals left by members after routing attempts.

    Args:
        source: Filter by source peer
        destination: Filter by destination peer

    Returns:
        Dict with route markers and analysis.
    """
    return rpc_stigmergic_markers(_get_hive_context(), source=source, destination=destination)


@plugin.method("hive-deposit-marker")
def hive_deposit_marker(
    plugin: Plugin,
    source: str,
    destination: str,
    fee_ppm: int,
    success: bool,
    volume_sats: int = 0,
    channel_id: str = None,
    peer_id: str = None,
    amount_sats: int = 0
):
    """
    Deposit a stigmergic route marker.

    Args:
        source: Source peer ID
        destination: Destination peer ID
        fee_ppm: Fee charged in ppm
        success: Whether routing succeeded
        volume_sats: Volume routed in sats
        channel_id: Optional channel ID (for compatibility)
        peer_id: Optional peer ID (for compatibility)
        amount_sats: Optional amount (alias for volume_sats)

    Returns:
        Dict with deposited marker info.
    """
    # Use amount_sats as fallback for volume_sats
    actual_volume = volume_sats if volume_sats else amount_sats
    return rpc_deposit_marker(
        _get_hive_context(),
        source=source,
        destination=destination,
        fee_ppm=fee_ppm,
        success=success,
        volume_sats=actual_volume
    )


@plugin.method("hive-defense-status")
def hive_defense_status(plugin: Plugin, peer_id: str = None):
    """
    Get mycelium defense system status.

    Args:
        peer_id: Optional peer to check for threats (returns peer_threat info)

    Returns:
        Dict with active warnings and defensive fee adjustments.
        If peer_id specified, includes peer_threat with is_threat, threat_type, etc.
    """
    return rpc_defense_status(_get_hive_context(), peer_id=peer_id)


@plugin.method("hive-broadcast-warning")
def hive_broadcast_warning(
    plugin: Plugin,
    peer_id: str,
    threat_type: str = "drain",
    severity: float = 0.5
):
    """
    Broadcast a peer warning to the fleet.

    Permission: Member only

    Args:
        peer_id: Peer to warn about
        threat_type: Type of threat ('drain', 'unreliable', 'force_close')
        severity: Severity from 0.0 to 1.0

    Returns:
        Dict with broadcast result.
    """
    return rpc_broadcast_warning(
        _get_hive_context(),
        peer_id=peer_id,
        threat_type=threat_type,
        severity=severity
    )


@plugin.method("hive-pheromone-levels")
def hive_pheromone_levels(plugin: Plugin, channel_id: str = None):
    """
    Get pheromone levels for adaptive fee control.

    Args:
        channel_id: Optional specific channel

    Returns:
        Dict with pheromone levels.
    """
    return rpc_pheromone_levels(_get_hive_context(), channel_id=channel_id)


@plugin.method("hive-fee-coordination-status")
def hive_fee_coordination_status(plugin: Plugin):
    """
    Get overall fee coordination status.

    Returns:
        Dict with comprehensive fee coordination status.
    """
    return rpc_fee_coordination_status(_get_hive_context())


# =============================================================================
# YIELD OPTIMIZATION PHASE 3: COST REDUCTION
# =============================================================================

@plugin.method("hive-rebalance-recommendations")
def hive_rebalance_recommendations(
    plugin: Plugin,
    prediction_hours: int = 24
):
    """
    Get predictive rebalance recommendations.

    Analyzes channels to find those predicted to deplete or saturate,
    with recommendations for preemptive rebalancing at lower fees.

    Args:
        prediction_hours: How far ahead to predict (default: 24)

    Returns:
        Dict with rebalance recommendations sorted by urgency.
    """
    return rpc_rebalance_recommendations(
        _get_hive_context(),
        prediction_hours=prediction_hours
    )


@plugin.method("hive-fleet-rebalance-path")
def hive_fleet_rebalance_path(
    plugin: Plugin,
    from_channel: str,
    to_channel: str,
    amount_sats: int
):
    """
    Get fleet rebalance path recommendation.

    Checks if rebalancing through fleet members is cheaper than
    external routing.

    Args:
        from_channel: Source channel SCID
        to_channel: Destination channel SCID
        amount_sats: Amount to rebalance

    Returns:
        Dict with path recommendation and savings estimate.
    """
    return rpc_fleet_rebalance_path(
        _get_hive_context(),
        from_channel=from_channel,
        to_channel=to_channel,
        amount_sats=amount_sats
    )


@plugin.method("hive-report-rebalance-outcome")
def hive_report_rebalance_outcome(
    plugin: Plugin,
    from_channel: str,
    to_channel: str,
    amount_sats: int,
    cost_sats: int,
    success: bool,
    via_fleet: bool = False
):
    """
    Record a rebalance outcome for tracking and circular flow detection.

    Args:
        from_channel: Source channel SCID
        to_channel: Destination channel SCID
        amount_sats: Amount rebalanced
        cost_sats: Cost paid
        success: Whether rebalance succeeded
        via_fleet: Whether routed through fleet members

    Returns:
        Dict with recording result and any circular flow warnings.
    """
    return rpc_record_rebalance_outcome(
        _get_hive_context(),
        from_channel=from_channel,
        to_channel=to_channel,
        amount_sats=amount_sats,
        cost_sats=cost_sats,
        success=success,
        via_fleet=via_fleet
    )


@plugin.method("hive-circular-flow-status")
def hive_circular_flow_status(plugin: Plugin):
    """
    Get circular flow detection status.

    Shows any detected circular flows (e.g., A→B→C→A) that waste
    fees moving liquidity in circles.

    Returns:
        Dict with circular flow status and detected patterns.
    """
    return rpc_circular_flow_status(_get_hive_context())


@plugin.method("hive-cost-reduction-status")
def hive_cost_reduction_status(plugin: Plugin):
    """
    Get overall cost reduction status.

    Comprehensive view of all Phase 3 cost reduction systems.

    Returns:
        Dict with cost reduction status.
    """
    return rpc_cost_reduction_status(_get_hive_context())


# =============================================================================
# CHANNEL RATIONALIZATION RPC METHODS
# =============================================================================

@plugin.method("hive-coverage-analysis")
def hive_coverage_analysis(plugin: Plugin, peer_id: str = None):
    """
    Analyze fleet coverage for redundant channels.

    Shows which fleet members have channels to the same peers
    and determines ownership based on routing activity (stigmergic markers).

    Args:
        peer_id: Specific peer to analyze, or omit for all redundant peers

    Returns:
        Dict with coverage analysis showing ownership and redundancy.
    """
    return rpc_coverage_analysis(_get_hive_context(), peer_id=peer_id)


@plugin.method("hive-close-recommendations")
def hive_close_recommendations(plugin: Plugin, our_node_only: bool = False):
    """
    Get channel close recommendations for underperforming redundant channels.

    Uses stigmergic markers (routing success) to determine which member
    "owns" each peer relationship. Recommends closes for members with
    <10% of the owner's routing activity.

    Part of the Hive covenant: members follow swarm intelligence.

    Args:
        our_node_only: If True, only return recommendations for our node

    Returns:
        Dict with close recommendations sorted by urgency.
    """
    return rpc_close_recommendations(_get_hive_context(), our_node_only=our_node_only)


@plugin.method("hive-create-close-actions")
def hive_create_close_actions(plugin: Plugin):
    """
    Create pending_actions for close recommendations.

    Puts high-confidence close recommendations into the pending_actions
    queue for AI/human approval.

    Returns:
        Dict with number of actions created.
    """
    return rpc_create_close_actions(_get_hive_context())


@plugin.method("hive-rationalization-summary")
def hive_rationalization_summary(plugin: Plugin):
    """
    Get summary of channel rationalization analysis.

    Shows fleet coverage health: well-owned peers, contested peers,
    orphan peers (channels with no routing activity), and close recommendations.

    Returns:
        Dict with rationalization summary.
    """
    return rpc_rationalization_summary(_get_hive_context())


@plugin.method("hive-rationalization-status")
def hive_rationalization_status(plugin: Plugin):
    """
    Get channel rationalization status.

    Shows overall coverage health metrics and configuration thresholds.

    Returns:
        Dict with rationalization status.
    """
    return rpc_rationalization_status(_get_hive_context())


# =============================================================================
# PHASE 5: STRATEGIC POSITIONING COMMANDS
# =============================================================================

@plugin.method("hive-valuable-corridors")
def hive_valuable_corridors(plugin: Plugin, min_score: float = 0.05):
    """
    Get high-value routing corridors for strategic positioning.

    Corridors are scored by: Volume × Margin × (1/Competition)
    Higher scores indicate better positioning opportunities.

    Args:
        min_score: Minimum value score to include (default: 0.05)

    Returns:
        Dict with valuable corridors sorted by score.
    """
    return rpc_valuable_corridors(_get_hive_context(), min_score=min_score)


@plugin.method("hive-exchange-coverage")
def hive_exchange_coverage(plugin: Plugin):
    """
    Get priority exchange connectivity status.

    Shows which major Lightning exchanges the fleet is connected to
    (ACINQ, Kraken, Bitfinex, etc.) and which still need channels.

    Returns:
        Dict with exchange coverage analysis.
    """
    return rpc_exchange_coverage(_get_hive_context())


@plugin.method("hive-positioning-recommendations")
def hive_positioning_recommendations(plugin: Plugin, count: int = 5):
    """
    Get channel open recommendations for strategic positioning.

    Recommends where to open channels for maximum routing value,
    considering existing fleet coverage and competition.

    Args:
        count: Number of recommendations to return (default: 5)

    Returns:
        Dict with positioning recommendations sorted by priority.
    """
    return rpc_positioning_recommendations(_get_hive_context(), count=count)


@plugin.method("hive-flow-recommendations")
def hive_flow_recommendations(plugin: Plugin, channel_id: str = None):
    """
    Get Physarum-inspired flow recommendations for channel lifecycle.

    Channels evolve based on flow like slime mold tubes:
    - High flow (>2% daily) → strengthen (splice in capacity)
    - Low flow (<0.1% daily) → atrophy (recommend close)
    - Young + low flow → stimulate (fee reduction)

    Args:
        channel_id: Specific channel, or None for all non-hold recommendations

    Returns:
        Dict with flow recommendations.
    """
    return rpc_flow_recommendations(_get_hive_context(), channel_id=channel_id)


@plugin.method("hive-report-flow-intensity")
def hive_report_flow_intensity(plugin: Plugin, channel_id: str, peer_id: str, intensity: float):
    """
    Report flow intensity for a channel to the Physarum model.

    Flow intensity = Daily volume / Capacity
    This updates the slime-mold model that drives channel lifecycle decisions.

    Args:
        channel_id: Channel ID (SCID format)
        peer_id: Peer public key
        intensity: Observed flow intensity (0.0 to 1.0+)

    Returns:
        Dict with acknowledgment.
    """
    return rpc_report_flow_intensity(
        _get_hive_context(),
        channel_id=channel_id,
        peer_id=peer_id,
        intensity=intensity
    )


@plugin.method("hive-positioning-summary")
def hive_positioning_summary(plugin: Plugin):
    """
    Get summary of strategic positioning analysis.

    Shows high-value corridors, exchange coverage, and recommended actions.

    Returns:
        Dict with positioning summary.
    """
    return rpc_positioning_summary(_get_hive_context())


@plugin.method("hive-positioning-status")
def hive_positioning_status(plugin: Plugin):
    """
    Get strategic positioning status.

    Shows overall status, thresholds, and priority exchanges.

    Returns:
        Dict with positioning status.
    """
    return rpc_positioning_status(_get_hive_context())


# =============================================================================
# PHYSARUM AUTO-TRIGGER RPC METHODS (Phase 7.2)
# =============================================================================

@plugin.method("hive-physarum-cycle")
def hive_physarum_cycle(plugin: Plugin):
    """
    Execute one Physarum optimization cycle.

    Evaluates all channels and creates pending_actions for:
    - High-flow channels that should be strengthened (splice-in)
    - Old low-flow channels that should atrophy (close recommendation)
    - Young low-flow channels that need stimulation (fee reduction)

    All actions go through governance approval - nothing executes directly.

    Returns:
        Dict with cycle results including proposals created.
    """
    if not strategic_positioning_mgr:
        return {"error": "Strategic positioning manager not initialized"}

    result = strategic_positioning_mgr.physarum_mgr.execute_physarum_cycle()
    return result


@plugin.method("hive-physarum-status")
def hive_physarum_status(plugin: Plugin):
    """
    Get Physarum auto-trigger status.

    Shows configuration, thresholds, rate limits, and current usage.

    Returns:
        Dict with auto-trigger status.
    """
    if not strategic_positioning_mgr:
        return {"error": "Strategic positioning manager not initialized"}

    return strategic_positioning_mgr.physarum_mgr.get_auto_trigger_status()


@plugin.method("hive-request-promotion")
def hive_request_promotion(plugin: Plugin):
    """
    Request promotion from neophyte to member.
    """
    if not config or not config.membership_enabled:
        return {"error": "membership_disabled"}
    if not membership_mgr or not our_pubkey:
        return {"error": "membership_unavailable"}

    tier = membership_mgr.get_tier(our_pubkey)
    if tier != MembershipTier.NEOPHYTE.value:
        return {"error": "permission_denied", "required_tier": "neophyte"}

    request_id = secrets.token_hex(16)
    now = int(time.time())
    database.add_promotion_request(our_pubkey, request_id, status="pending")

    payload = {
        "target_pubkey": our_pubkey,
        "request_id": request_id,
        "timestamp": now
    }
    msg = serialize(HiveMessageType.PROMOTION_REQUEST, payload)
    _broadcast_to_members(msg)

    active_members = membership_mgr.get_active_members()
    quorum = membership_mgr.calculate_quorum(len(active_members))
    return {
        "status": "requested",
        "request_id": request_id,
        "vouches_needed": quorum
    }


@plugin.method("hive-genesis")
def hive_genesis(plugin: Plugin, hive_id: str = None):
    """
    Initialize this node as the Genesis (Admin) node of a new Hive.

    This creates the first member record with member privileges and
    generates a self-signed genesis ticket.

    Args:
        hive_id: Optional custom Hive identifier (auto-generated if not provided)

    Returns:
        Dict with genesis status and member ticket
    """
    if not database or not safe_plugin or not handshake_mgr:
        return {"error": "Hive not initialized"}

    try:
        result = handshake_mgr.genesis(hive_id)

        # Auto-generate and register BOLT12 offer for settlement
        if settlement_mgr:
            our_pubkey = handshake_mgr.get_our_pubkey()
            offer_result = settlement_mgr.generate_and_register_offer(our_pubkey)
            if "error" in offer_result:
                plugin.log(f"cl-hive: Failed to auto-register settlement offer: {offer_result['error']}", level='warn')
            else:
                result["settlement_offer"] = offer_result.get("status")
                plugin.log(f"cl-hive: Settlement offer auto-registered for genesis member")

        return result
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"Genesis failed: {e}"}


@plugin.method("hive-invite")
def hive_invite(plugin: Plugin, valid_hours: int = 24, requirements: int = 0,
                tier: str = 'neophyte'):
    """
    Generate an invitation ticket for a new member.

    Only full members can generate invite tickets. New members join as neophytes
    and can be promoted to member after meeting the promotion criteria.

    Args:
        valid_hours: Hours until ticket expires (default: 24)
        requirements: Bitmask of required features (default: 0 = none)
        tier: Starting tier - 'neophyte' (default) or 'member' (bootstrap only)

    Returns:
        Dict with base64-encoded ticket

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not handshake_mgr:
        return {"error": "Hive not initialized"}

    # Validate tier (2-tier system: member or neophyte)
    if tier not in ('neophyte', 'member'):
        return {"error": f"Invalid tier: {tier}. Use 'neophyte' (default) or 'member' (bootstrap)"}

    try:
        ticket = handshake_mgr.generate_invite_ticket(valid_hours, requirements, tier)
        bootstrap_note = " (BOOTSTRAP - grants full member tier)" if tier == 'member' else ""
        return {
            "status": "ticket_generated",
            "ticket": ticket,
            "valid_hours": valid_hours,
            "initial_tier": tier,
            "instructions": f"Share this ticket with the candidate.{bootstrap_note} They should use 'hive-join <ticket>' to request membership."
        }
    except PermissionError as e:
        return {"error": str(e)}
    except ValueError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"Failed to generate ticket: {e}"}


@plugin.method("hive-join")
def hive_join(plugin: Plugin, ticket: str, peer_id: str = None):
    """
    Request to join a Hive using an invitation ticket.
    
    This initiates the handshake protocol by sending a HELLO message
    to a known Hive member.
    
    Args:
        ticket: Base64-encoded invitation ticket
        peer_id: Node ID of a known Hive member (optional, extracted from ticket if not provided)
    
    Returns:
        Dict with join request status
    """
    if not handshake_mgr or not safe_plugin:
        return {"error": "Hive not initialized"}
    
    # Decode ticket to get admin pubkey if peer_id not provided
    try:
        ticket_obj = Ticket.from_base64(ticket)
        if not peer_id:
            peer_id = ticket_obj.admin_pubkey
    except Exception as e:
        return {"error": f"Invalid ticket format: {e}"}
    
    # Check if ticket is expired
    if ticket_obj.is_expired():
        return {"error": "Ticket has expired"}
    
    # Send HELLO message with our pubkey (for identity binding)
    from modules.protocol import create_hello
    our_pubkey = handshake_mgr.get_our_pubkey()
    hello_msg = create_hello(our_pubkey)
    
    try:
        safe_plugin.rpc.call("sendcustommsg", {
            "node_id": peer_id,
            "msg": hello_msg.hex()
        })
        
        return {
            "status": "join_requested",
            "target_peer": peer_id[:16] + "...",
            "hive_id": ticket_obj.hive_id,
            "message": "HELLO sent. Awaiting CHALLENGE from Hive member."
        }
    except Exception as e:
        return {"error": f"Failed to send HELLO: {e}"}


# =============================================================================
# ANTICIPATORY LIQUIDITY RPC METHODS (Phase 7.1)
# =============================================================================

@plugin.method("hive-record-flow")
def hive_record_flow(
    plugin: Plugin,
    channel_id: str,
    inbound_sats: int,
    outbound_sats: int,
    timestamp: int = None
):
    """
    Record a flow observation for pattern detection.

    Called periodically (e.g., hourly) to build flow history for
    temporal pattern detection and predictive rebalancing.

    Args:
        channel_id: Channel SCID
        inbound_sats: Satoshis received in this period
        outbound_sats: Satoshis sent in this period
        timestamp: Unix timestamp (defaults to now)

    Returns:
        Dict with recording result.
    """
    if not anticipatory_liquidity_mgr:
        return {"error": "Anticipatory liquidity manager not initialized"}

    anticipatory_liquidity_mgr.record_flow_sample(
        channel_id=channel_id,
        inbound_sats=inbound_sats,
        outbound_sats=outbound_sats,
        timestamp=timestamp
    )

    return {
        "status": "ok",
        "channel_id": channel_id,
        "net_flow": inbound_sats - outbound_sats
    }


@plugin.method("hive-detect-patterns")
def hive_detect_patterns(plugin: Plugin, channel_id: str = None, force_refresh: bool = False):
    """
    Detect temporal patterns in channel flow.

    Analyzes historical flow data to find recurring patterns by:
    - Hour of day (e.g., "high outbound 14:00-17:00 UTC")
    - Day of week (e.g., "high inbound on weekends")
    - Combined patterns (e.g., "Monday mornings drain")

    Args:
        channel_id: Specific channel to analyze (None for all)
        force_refresh: Force recalculation even if cached

    Returns:
        Dict with detected patterns.
    """
    if not anticipatory_liquidity_mgr:
        return {"error": "Anticipatory liquidity manager not initialized"}

    if channel_id:
        patterns = anticipatory_liquidity_mgr.detect_patterns(
            channel_id, force_refresh=force_refresh
        )
        return {
            "channel_id": channel_id,
            "pattern_count": len(patterns),
            "patterns": [p.to_dict() for p in patterns]
        }
    else:
        # Return summary for all channels
        summary = anticipatory_liquidity_mgr.get_patterns_summary()
        return summary


@plugin.method("hive-predict-liquidity")
def hive_predict_liquidity(
    plugin: Plugin,
    channel_id: str,
    hours_ahead: int = 12,
    current_local_pct: float = None,
    capacity_sats: int = None
):
    """
    Predict channel liquidity state N hours from now.

    Combines velocity analysis with temporal patterns to predict
    future balance and recommend preemptive actions.

    Args:
        channel_id: Channel SCID
        hours_ahead: Hours to predict (default: 12)
        current_local_pct: Current local balance % (fetched if not provided)
        capacity_sats: Channel capacity (fetched if not provided)

    Returns:
        Dict with liquidity prediction including risks and recommendations.
    """
    if not anticipatory_liquidity_mgr:
        return {"error": "Anticipatory liquidity manager not initialized"}

    prediction = anticipatory_liquidity_mgr.predict_liquidity(
        channel_id=channel_id,
        hours_ahead=hours_ahead,
        current_local_pct=current_local_pct,
        capacity_sats=capacity_sats
    )

    if not prediction:
        return {
            "error": "no_data",
            "channel_id": channel_id,
            "reason": "Insufficient flow history for prediction"
        }

    return prediction.to_dict()


@plugin.method("hive-anticipatory-predictions")
def hive_anticipatory_predictions(
    plugin: Plugin,
    hours_ahead: int = 12,
    min_risk: float = 0.3
):
    """
    Get liquidity predictions for all channels.

    Returns channels with significant depletion or saturation risk,
    enabling proactive rebalancing before problems occur.

    Args:
        hours_ahead: Prediction horizon in hours (default: 12)
        min_risk: Minimum risk threshold to include (default: 0.3)

    Returns:
        Dict with predictions for at-risk channels.
    """
    if not anticipatory_liquidity_mgr:
        return {"error": "Anticipatory liquidity manager not initialized"}

    predictions = anticipatory_liquidity_mgr.get_all_predictions(
        hours_ahead=hours_ahead,
        min_risk=min_risk
    )

    return {
        "hours_ahead": hours_ahead,
        "min_risk": min_risk,
        "prediction_count": len(predictions),
        "predictions": [p.to_dict() for p in predictions]
    }


@plugin.method("hive-fleet-anticipation")
def hive_fleet_anticipation(plugin: Plugin):
    """
    Get fleet-wide anticipatory positioning recommendations.

    Coordinates predictions across hive members to avoid competing
    for the same rebalance routes.

    Returns:
        Dict with fleet coordination recommendations.
    """
    if not anticipatory_liquidity_mgr:
        return {"error": "Anticipatory liquidity manager not initialized"}

    recommendations = anticipatory_liquidity_mgr.get_fleet_recommendations()

    return {
        "recommendation_count": len(recommendations),
        "recommendations": [r.to_dict() for r in recommendations]
    }


@plugin.method("hive-anticipatory-status")
def hive_anticipatory_status(plugin: Plugin):
    """
    Get anticipatory liquidity manager status.

    Returns operational status and configuration for diagnostics.

    Returns:
        Dict with manager status.
    """
    if not anticipatory_liquidity_mgr:
        return {"error": "Anticipatory liquidity manager not initialized"}

    return anticipatory_liquidity_mgr.get_status()


# =============================================================================
# TIME-BASED FEE RPC METHODS (Phase 7.4)
# =============================================================================

@plugin.method("hive-time-fee-status")
def hive_time_fee_status(plugin: Plugin):
    """
    Get time-based fee adjustment status.

    Returns current time context, active adjustments, and configuration.

    Returns:
        Dict with time-based fee status.
    """
    if not fee_coordination_mgr:
        return {"error": "Fee coordination manager not initialized"}

    return fee_coordination_mgr.get_time_fee_status()


@plugin.method("hive-time-fee-adjustment")
def hive_time_fee_adjustment(plugin: Plugin, channel_id: str, base_fee: int = 250):
    """
    Get time-based fee adjustment for a specific channel.

    Analyzes temporal patterns to determine optimal fee for current time.

    Args:
        channel_id: Channel short ID (e.g., "123x456x0")
        base_fee: Current/base fee in ppm (default: 250)

    Returns:
        Dict with adjustment details including recommended fee.
    """
    if not fee_coordination_mgr:
        return {"error": "Fee coordination manager not initialized"}

    return fee_coordination_mgr.get_time_fee_adjustment(channel_id, base_fee)


@plugin.method("hive-time-peak-hours")
def hive_time_peak_hours(plugin: Plugin, channel_id: str):
    """
    Get detected peak routing hours for a channel.

    Returns hours with above-average routing volume based on historical patterns.

    Args:
        channel_id: Channel short ID

    Returns:
        List of peak hour details with intensity and confidence.
    """
    if not fee_coordination_mgr:
        return {"error": "Fee coordination manager not initialized"}

    peak_hours = fee_coordination_mgr.get_channel_peak_hours(channel_id)
    return {
        "channel_id": channel_id,
        "peak_hours": peak_hours,
        "count": len(peak_hours)
    }


@plugin.method("hive-time-low-hours")
def hive_time_low_hours(plugin: Plugin, channel_id: str):
    """
    Get detected low-activity hours for a channel.

    Returns hours with below-average routing volume where fee reduction may help.

    Args:
        channel_id: Channel short ID

    Returns:
        List of low-activity hour details with intensity and confidence.
    """
    if not fee_coordination_mgr:
        return {"error": "Fee coordination manager not initialized"}

    low_hours = fee_coordination_mgr.get_channel_low_hours(channel_id)
    return {
        "channel_id": channel_id,
        "low_hours": low_hours,
        "count": len(low_hours)
    }


# =============================================================================
# MAIN
# =============================================================================

plugin.run()
