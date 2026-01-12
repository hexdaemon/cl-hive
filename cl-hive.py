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
    VOUCH_TTL_SECONDS, MAX_VOUCHES_IN_PROMOTION,
    create_challenge, create_welcome
)
from modules.handshake import HandshakeManager, Ticket, CHALLENGE_TTL_SECONDS
from modules.state_manager import StateManager
from modules.gossip import GossipManager
from modules.intent_manager import IntentManager, Intent, IntentType
from modules.bridge import Bridge, BridgeStatus, CircuitOpenError
from modules.contribution import ContributionManager
from modules.membership import MembershipManager, MembershipTier
from modules.planner import Planner
from modules.clboss_bridge import CLBossBridge
from modules.governance import DecisionEngine

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
our_pubkey: Optional[str] = None


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
        required_tier: 'admin' or 'member'

    Returns:
        None if permission granted, or error dict if denied
    """
    if not our_pubkey or not database:
        return {"error": "Not initialized"}

    member = database.get_member(our_pubkey)
    if not member:
        return {"error": "Not a Hive member", "required_tier": required_tier}

    current_tier = member.get('tier', 'neophyte')

    if required_tier == 'admin':
        if current_tier != 'admin':
            return {
                "error": "permission_denied",
                "message": "This command requires admin privileges",
                "current_tier": current_tier,
                "required_tier": "admin"
            }
    elif required_tier == 'member':
        if current_tier not in ('admin', 'member'):
            return {
                "error": "permission_denied",
                "message": "This command requires member or admin privileges",
                "current_tier": current_tier,
                "required_tier": "member"
            }

    return None  # Permission granted


# =============================================================================
# PLUGIN OPTIONS
# =============================================================================

plugin.add_option(
    name='hive-db-path',
    default='~/.lightning/cl_hive.db',
    description='Path to the SQLite database for Hive state'
)

plugin.add_option(
    name='hive-governance-mode',
    default='advisor',
    description='Governance mode: advisor (human approval), autonomous (auto-execute), oracle (external API)'
)

plugin.add_option(
    name='hive-neophyte-fee-discount',
    default='0.5',
    description='Fee discount for Neophyte members (0.5 = 50% of public rate)'
)

plugin.add_option(
    name='hive-member-fee-ppm',
    default='0',
    description='Fee charged to full Hive members (default: 0 = free)'
)

plugin.add_option(
    name='hive-probation-days',
    default='30',
    description='Minimum days as Neophyte before promotion eligibility'
)

plugin.add_option(
    name='hive-vouch-threshold',
    default='0.51',
    description='Percentage of member vouches required for promotion (0.51 = 51%)'
)

plugin.add_option(
    name='hive-min-vouch-count',
    default='3',
    description='Minimum number of vouches required for promotion'
)

plugin.add_option(
    name='hive-max-members',
    default='50',
    description='Maximum Hive members (Dunbar cap for gossip efficiency)'
)

plugin.add_option(
    name='hive-market-share-cap',
    default='0.20',
    description='Maximum market share per target (0.20 = 20%, anti-monopoly)'
)

plugin.add_option(
    name='hive-membership-enabled',
    default='true',
    description='Enable membership & promotion protocol (default: true)'
)

plugin.add_option(
    name='hive-auto-vouch',
    default='true',
    description='Auto-vouch for eligible neophytes (default: true)'
)

plugin.add_option(
    name='hive-auto-promote',
    default='true',
    description='Auto-promote when quorum reached (default: true)'
)

plugin.add_option(
    name='hive-ban-autotrigger',
    default='false',
    description='Auto-trigger ban proposal on sustained leeching (default: false)'
)

plugin.add_option(
    name='hive-intent-hold-seconds',
    default='60',
    description='Hold period before committing an Intent (conflict resolution)'
)

plugin.add_option(
    name='hive-gossip-threshold',
    default='0.10',
    description='Capacity change threshold to trigger gossip (0.10 = 10%)'
)

plugin.add_option(
    name='hive-heartbeat-interval',
    default='300',
    description='Heartbeat broadcast interval in seconds (default: 5 min)'
)

plugin.add_option(
    name='hive-planner-interval',
    default='3600',
    description='Planner cycle interval in seconds (default: 1 hour, minimum: 300)'
)

plugin.add_option(
    name='hive-planner-enable-expansions',
    default='false',
    description='Enable expansion proposals (new channel openings) in Planner'
)


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
    global database, config, safe_plugin, handshake_mgr, state_manager, gossip_mgr, intent_mgr, our_pubkey, bridge
    
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
        probation_days=int(options.get('hive-probation-days', '30')),
        vouch_threshold_pct=float(options.get('hive-vouch-threshold', '0.51')),
        min_vouch_count=int(options.get('hive-min-vouch-count', '3')),
        max_members=int(options.get('hive-max-members', '50')),
        market_share_cap_pct=float(options.get('hive-market-share-cap', '0.20')),
        intent_hold_seconds=int(options.get('hive-intent-hold-seconds', '60')),
        gossip_threshold_pct=float(options.get('hive-gossip-threshold', '0.10')),
        heartbeat_interval=int(options.get('hive-heartbeat-interval', '300')),
        planner_interval=int(options.get('hive-planner-interval', '3600')),
        planner_enable_expansions=_parse_bool(options.get('hive-planner-enable-expansions', 'false')),
    )
    
    # Initialize database
    database = HiveDatabase(config.db_path, safe_plugin)
    database.initialize()
    plugin.log(f"cl-hive: Database initialized at {config.db_path}")
    
    # Initialize handshake manager
    handshake_mgr = HandshakeManager(
        safe_plugin.rpc, database, safe_plugin,
        min_vouch_count=config.min_vouch_count
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
            plugin.log("cl-hive: CLBoss integration available (Gateway Pattern)")
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
        else:
            # Known but unimplemented message type (Phase 4+)
            plugin.log(f"cl-hive: Unhandled message type {msg_type.name} from {peer_id[:16]}...", level='debug')
            return {"result": "continue"}
            
    except Exception as e:
        plugin.log(f"cl-hive: Error handling {msg_type.name}: {e}", level='warn')
        return {"result": "continue"}


def handle_hello(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_HELLO message (ticket presentation).
    
    A candidate is presenting their invite ticket.
    Verify the ticket and respond with a CHALLENGE.
    """
    ticket_b64 = payload.get('ticket')
    if not ticket_b64:
        plugin.log(f"cl-hive: HELLO from {peer_id[:16]}... missing ticket", level='warn')
        return {"result": "continue"}
    
    # Verify the ticket
    is_valid, ticket, error = handshake_mgr.verify_ticket(ticket_b64)

    if not is_valid:
        plugin.log(f"cl-hive: Invalid ticket from {peer_id[:16]}...: {error}", level='warn')
        return {"result": "continue"}

    # Get initial tier from ticket (default to neophyte for backwards compatibility)
    initial_tier = getattr(ticket, 'initial_tier', 'neophyte')

    # Generate challenge nonce (stores initial_tier for use after ATTEST)
    nonce = handshake_mgr.generate_challenge(peer_id, ticket.requirements, initial_tier)
    
    # Get Hive ID from an existing admin
    members = database.get_all_members()
    hive_id = "unknown"
    for m in members:
        if m['tier'] == 'admin' and m.get('metadata'):
            import json
            metadata = json.loads(m['metadata'])
            hive_id = metadata.get('hive_id', 'unknown')
            break
    
    # Send CHALLENGE response
    challenge_msg = create_challenge(nonce, hive_id)
    
    try:
        safe_plugin.rpc.call("sendcustommsg", {
            "node_id": peer_id,
            "msg": challenge_msg.hex()
        })
        plugin.log(f"cl-hive: Sent CHALLENGE to {peer_id[:16]}...")
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

    # Get initial tier from pending challenge (bootstrap support)
    initial_tier = pending.get('initial_tier', 'neophyte')

    # Verification passed! Add member with appropriate tier
    database.add_member(
        peer_id=peer_id,
        tier=initial_tier,
        joined_at=int(time.time())
    )

    # If admin tier (bootstrap), also trigger policy sync
    if initial_tier == 'admin' and membership_mgr:
        membership_mgr.set_tier(peer_id, initial_tier)

    handshake_mgr.clear_challenge(peer_id)

    # Get Hive info for WELCOME
    members = database.get_all_members()
    hive_id = "hive"
    for m in members:
        if m['tier'] == 'admin' and m.get('metadata'):
            import json
            metadata = json.loads(m['metadata'])
            hive_id = metadata.get('hive_id', 'hive')
            break

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
        bootstrap_note = " [BOOTSTRAP]" if initial_tier == 'admin' else ""
        plugin.log(f"cl-hive: Sent WELCOME to {peer_id[:16]}... (new {initial_tier}){bootstrap_note}")
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

        # Also add the peer that welcomed us (they're the admin or existing member)
        database.add_member(peer_id, tier='admin', joined_at=now)

    # Initiate state sync with the peer that welcomed us
    if gossip_mgr and safe_plugin:
        state_hash_payload = gossip_mgr.create_state_hash_payload()
        state_hash_msg = serialize(HiveMessageType.STATE_HASH, state_hash_payload)

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
    """
    if not gossip_mgr:
        return {"result": "continue"}

    # P3-02: Verify sender is a Hive member before processing
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
    """
    if not gossip_mgr or not state_manager:
        return {"result": "continue"}

    hashes_match = gossip_mgr.process_state_hash(peer_id, payload)

    if not hashes_match:
        # State divergence detected - send FULL_SYNC with membership
        plugin.log(f"cl-hive: State divergence with {peer_id[:16]}..., sending FULL_SYNC")

        full_sync_payload = gossip_mgr.create_full_sync_payload()
        full_sync_payload["members"] = _create_membership_payload()
        full_sync_msg = serialize(HiveMessageType.FULL_SYNC, full_sync_payload)

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

    SECURITY: Only accept FULL_SYNC from Hive members to prevent
    state poisoning attacks from arbitrary peers.
    """
    if not gossip_mgr:
        return {"result": "continue"}

    # SECURITY: Membership check to prevent state poisoning (Issue #8)
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

        # Validate tier value
        if tier not in ("admin", "member", "neophyte"):
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


def _broadcast_full_sync_to_members(plugin: Plugin) -> None:
    """
    Broadcast FULL_SYNC with membership to all existing members.

    Called after adding a new member to ensure all nodes sync.
    """
    if not database or not gossip_mgr or not safe_plugin:
        plugin.log(f"cl-hive: _broadcast_full_sync_to_members: missing deps - db={database is not None}, gossip={gossip_mgr is not None}, plugin={safe_plugin is not None}", level='debug')
        return

    members = database.get_all_members()
    plugin.log(f"cl-hive: Broadcasting membership to {len(members)} known members")

    # Create FULL_SYNC payload with membership
    full_sync_payload = gossip_mgr.create_full_sync_payload()
    full_sync_payload["members"] = _create_membership_payload()

    full_sync_msg = serialize(HiveMessageType.FULL_SYNC, full_sync_payload)

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
def on_peer_connected(plugin: Plugin, id: str, **kwargs):
    """
    Hook called when a peer connects.

    If the peer is a Hive member, send a STATE_HASH message to
    initiate anti-entropy check and detect state divergence.
    """
    peer_id = id  # CLN v25+ uses 'id' instead of 'peer_id'
    if not database or not gossip_mgr:
        return

    # Check if this peer is a Hive member
    member = database.get_member(peer_id)
    if not member:
        return  # Not a Hive member, ignore

    now = int(time.time())
    database.update_member(peer_id, last_seen=now)
    database.update_presence(peer_id, is_online=True, now_ts=now, window_seconds=30 * 86400)
    
    plugin.log(f"cl-hive: Hive member {peer_id[:16]}... connected, sending STATE_HASH")
    
    # Send STATE_HASH for anti-entropy check
    state_hash_payload = gossip_mgr.create_state_hash_payload()
    state_hash_msg = serialize(HiveMessageType.STATE_HASH, state_hash_payload)
    
    try:
        safe_plugin.rpc.call("sendcustommsg", {
            "node_id": peer_id,
            "msg": state_hash_msg.hex()
        })
    except Exception as e:
        plugin.log(f"cl-hive: Failed to send STATE_HASH to {peer_id[:16]}...: {e}", level='warn')


@plugin.subscribe("disconnect")
def on_peer_disconnected(plugin: Plugin, id: str, **kwargs):
    """Update presence for disconnected peers."""
    peer_id = id  # CLN v25+ uses 'id' instead of 'peer_id'
    if not database:
        return
    member = database.get_member(peer_id)
    if not member:
        return
    now = int(time.time())
    database.update_member(peer_id, last_seen=now)
    database.update_presence(peer_id, is_online=False, now_ts=now, window_seconds=30 * 86400)


@plugin.subscribe("forward_event")
def on_forward_event(plugin: Plugin, **payload):
    """Track forwarding events for contribution and leech detection."""
    if not contribution_mgr:
        return
    try:
        contribution_mgr.handle_forward_event(payload)
    except Exception as e:
        if safe_plugin:
            safe_plugin.log(f"Forward event handling error: {e}", level="warn")


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
    """
    if not intent_mgr:
        return {"result": "continue"}
    
    intent_type = payload.get('intent_type')
    target = payload.get('target')
    initiator = payload.get('initiator')

    if not intent_type or not target or not initiator:
        plugin.log(f"cl-hive: INTENT_ABORT from {peer_id[:16]}... missing fields", level='warn')
        return {"result": "continue"}
    
    intent_mgr.record_remote_abort(intent_type, target, initiator)
    plugin.log(f"cl-hive: INTENT_ABORT from {peer_id[:16]}... for {target[:16]}...")
    
    return {"result": "continue"}


def broadcast_intent_abort(target: str, intent_type: str) -> None:
    """
    Broadcast HIVE_INTENT_ABORT to all Hive members.
    
    Called when we lose a tie-breaker and need to yield.
    """
    if not database or not safe_plugin or not intent_mgr:
        return
    
    members = database.get_all_members()
    abort_payload = {
        'intent_type': intent_type,
        'target': target,
        'initiator': intent_mgr.our_pubkey,
        'reason': 'tie_breaker_loss'
    }
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

def _broadcast_to_members(message_bytes: bytes) -> None:
    if not database or not safe_plugin:
        return
    for member in database.get_all_members():
        tier = member.get("tier")
        # Broadcast to both members and admins
        if tier not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value):
            continue
        member_id = member["peer_id"]
        if member_id == our_pubkey:
            continue
        try:
            safe_plugin.rpc.call("sendcustommsg", {
                "node_id": member_id,
                "msg": message_bytes.hex()
            })
        except Exception as e:
            safe_plugin.log(f"Failed to send promotion msg to {member_id[:16]}...: {e}", level='debug')


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
        is_hive_member = tier in (MembershipTier.ADMIN.value, MembershipTier.MEMBER.value)

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
    Broadcast membership list to all known peers on startup.

    This ensures all nodes converge to the same membership state
    when the plugin restarts.
    """
    if not database or not gossip_mgr or not safe_plugin:
        return

    members = database.get_all_members()
    if len(members) <= 1:
        return  # Just us, nothing to sync

    # Create FULL_SYNC with membership
    full_sync_payload = gossip_mgr.create_full_sync_payload()
    full_sync_payload["members"] = _create_membership_payload()
    full_sync_msg = serialize(HiveMessageType.FULL_SYNC, full_sync_payload)

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
    if our_tier not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value):
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
    if not voucher or voucher.get("tier") not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value):
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
    if local_tier not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value, MembershipTier.NEOPHYTE.value):
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
    if local_tier not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value):
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
    if sender_tier not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value):
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
        if member_tier not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value):
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

    # Check if hive is now headless (no admins)
    all_members = database.get_all_members()
    admin_count = sum(1 for m in all_members if m.get("tier") == MembershipTier.ADMIN.value)
    if admin_count == 0 and len(all_members) > 0:
        plugin.log("cl-hive: WARNING - Hive is now headless (no admins). Members can elect a new admin.", level='warn')

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
    if not proposer or proposer.get("tier") not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value):
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
    if not voter or voter.get("tier") not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value):
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
        if m.get("tier") in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value)
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
        if config.governance_mode != "autonomous":
            if safe_plugin:
                safe_plugin.log(
                    f"cl-hive: Intent {intent_id} ready but not committing "
                    f"(mode={config.governance_mode})",
                    level='debug'
                )
            continue

        # Commit the intent (only in autonomous mode)
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
# RPC COMMANDS
# =============================================================================

@plugin.method("hive-status")
def hive_status(plugin: Plugin):
    """
    Get current Hive status and membership info.
    
    Returns:
        Dict with hive state, member count, governance mode, etc.
    """
    if not database:
        return {"error": "Hive not initialized"}
    
    members = database.get_all_members()
    member_count = len([m for m in members if m['tier'] == 'member'])
    neophyte_count = len([m for m in members if m['tier'] == 'neophyte'])
    admin_count = len([m for m in members if m['tier'] == 'admin'])
    
    return {
        "status": "active" if members else "genesis_required",
        "governance_mode": config.governance_mode if config else "unknown",
        "members": {
            "total": len(members),
            "admin": admin_count,
            "member": member_count,
            "neophyte": neophyte_count,
        },
        "limits": {
            "max_members": config.max_members if config else 50,
            "market_share_cap": config.market_share_cap_pct if config else 0.20,
        },
        "version": "0.1.0-dev",
    }


@plugin.method("hive-reinit-bridge")
def hive_reinit_bridge(plugin: Plugin):
    """
    Re-attempt bridge initialization if it failed at startup.

    Useful for recovering from startup race conditions where cl-revenue-ops
    wasn't ready when cl-hive initialized. Also useful after cl-revenue-ops
    is installed or restarted.

    Returns:
        Dict with bridge status and details.

    Permission: Admin only
    """
    # Permission check: Admin only
    perm_error = _check_permission('admin')
    if perm_error:
        return perm_error

    if not bridge:
        return {"error": "Bridge module not initialized"}

    previous_status = bridge.status.value
    new_status = bridge.reinitialize()

    return {
        "previous_status": previous_status,
        "new_status": new_status.value,
        "revenue_ops_version": bridge._revenue_ops_version,
        "clboss_available": bridge._clboss_available,
        "message": (
            "Bridge enabled successfully" if new_status == BridgeStatus.ENABLED
            else "Bridge still disabled - check cl-revenue-ops installation"
        )
    }


@plugin.method("hive-members")
def hive_members(plugin: Plugin):
    """
    List all Hive members with their tier and stats.

    Returns:
        List of member records with tier, contribution ratio, uptime, etc.
    """
    if not database:
        return {"error": "Hive not initialized"}

    members = database.get_all_members()
    return {
        "count": len(members),
        "members": members,
    }


@plugin.method("hive-topology")
def hive_topology(plugin: Plugin):
    """
    Get current topology analysis from the Planner.

    Returns:
        Dict with saturated targets, planner stats, and config.
    """
    if not planner:
        return {"error": "Planner not initialized"}
    if not config:
        return {"error": "Config not initialized"}

    # Take config snapshot
    cfg = config.snapshot()

    # Refresh network cache before analysis
    planner._refresh_network_cache(force=True)

    # Get saturated targets
    saturated = planner.get_saturated_targets(cfg)
    saturated_list = [
        {
            "target": r.target[:16] + "...",
            "target_full": r.target,
            "hive_capacity_sats": r.hive_capacity_sats,
            "public_capacity_sats": r.public_capacity_sats,
            "hive_share_pct": round(r.hive_share_pct * 100, 2),
        }
        for r in saturated
    ]

    # Get planner stats
    stats = planner.get_planner_stats()

    return {
        "saturated_targets": saturated_list,
        "saturated_count": len(saturated_list),
        "ignored_peers": stats.get("ignored_peers", []),
        "ignored_count": stats.get("ignored_peers_count", 0),
        "network_cache_size": stats.get("network_cache_size", 0),
        "network_cache_age_seconds": stats.get("network_cache_age_seconds", 0),
        "config": {
            "market_share_cap_pct": cfg.market_share_cap_pct,
            "planner_interval_seconds": cfg.planner_interval,
            "expansions_enabled": cfg.planner_enable_expansions,
            "governance_mode": cfg.governance_mode,
        }
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
    if not database:
        return {"error": "Database not initialized"}

    # Bound limit to prevent excessive queries
    limit = min(max(1, limit), 500)

    logs = database.get_planner_logs(limit=limit)
    return {
        "count": len(logs),
        "limit": limit,
        "logs": logs,
    }


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
    perm_error = _check_permission('admin')
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
    if not planner or not planner.intent_manager:
        return {"error": "Intent manager not initialized"}

    intent_mgr = planner.intent_manager
    stats = intent_mgr.get_intent_stats()

    # Get pending local intents from DB
    pending = database.get_pending_intents() if database else []

    # Get remote intents from cache
    remote = intent_mgr.get_remote_intents()

    return {
        "local_pending": len(pending),
        "local_intents": pending,
        "remote_cached": len(remote),
        "remote_intents": [r.to_dict() for r in remote],
        "stats": stats
    }


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
    perm_error = _check_permission('admin')
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
    if not database:
        return {"error": "Database not initialized"}

    actions = database.get_pending_actions()
    return {
        "count": len(actions),
        "actions": actions,
    }


@plugin.method("hive-approve-action")
def hive_approve_action(plugin: Plugin, action_id: int):
    """
    Approve and execute a pending action.

    Args:
        action_id: ID of the action to approve

    Returns:
        Dict with approval result.

    Permission: Member or Admin only
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database:
        return {"error": "Database not initialized"}

    # Get the action
    action = database.get_pending_action_by_id(action_id)
    if not action:
        return {"error": "Action not found", "action_id": action_id}

    if action['status'] != 'pending':
        return {"error": f"Action already {action['status']}", "action_id": action_id}

    # Check if expired
    now = int(time.time())
    if action.get('expires_at') and now > action['expires_at']:
        database.update_action_status(action_id, 'expired')
        return {"error": "Action has expired", "action_id": action_id}

    action_type = action['action_type']
    payload = action['payload']

    # Execute based on action type
    if action_type == 'channel_open':
        # Get the intent and broadcast it
        intent_id = payload.get('intent_id')
        if not intent_id:
            return {"error": "Missing intent_id in action payload", "action_id": action_id}

        # Get intent from database
        intent_record = database.get_intent_by_id(intent_id)
        if not intent_record:
            return {"error": "Intent not found", "intent_id": intent_id}

        # Broadcast the intent to all members
        if intent_mgr:
            try:
                # Create an intent object for broadcasting
                from modules.intent_manager import Intent
                intent = Intent(
                    intent_id=intent_record['id'],
                    intent_type=intent_record['intent_type'],
                    target=intent_record['target'],
                    initiator=intent_record['initiator'],
                    timestamp=intent_record['timestamp'],
                    expires_at=intent_record['expires_at'],
                    status=intent_record['status']
                )

                # Broadcast to all members
                intent_payload = intent_mgr.create_intent_message(intent)
                msg = serialize(HiveMessageType.INTENT, intent_payload)
                members = database.get_all_members()

                broadcast_count = 0
                for member in members:
                    member_id = member.get('peer_id')
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

                plugin.log(f"cl-hive: Approved action {action_id}, broadcast intent to {broadcast_count} peers")

            except Exception as e:
                return {"error": f"Failed to broadcast intent: {e}", "action_id": action_id}

        # Update action status
        database.update_action_status(action_id, 'approved')

        return {
            "status": "approved",
            "action_id": action_id,
            "action_type": action_type,
            "target": payload.get('target'),
        }

    else:
        # Unknown action type - just mark as approved
        database.update_action_status(action_id, 'approved')
        return {
            "status": "approved",
            "action_id": action_id,
            "action_type": action_type,
            "note": "Unknown action type, marked as approved only"
        }


@plugin.method("hive-reject-action")
def hive_reject_action(plugin: Plugin, action_id: int):
    """
    Reject a pending action.

    Args:
        action_id: ID of the action to reject

    Returns:
        Dict with rejection result.

    Permission: Member or Admin only
    """
    # Permission check: Member or Admin
    perm_error = _check_permission('member')
    if perm_error:
        return perm_error

    if not database:
        return {"error": "Database not initialized"}

    # Get the action
    action = database.get_pending_action_by_id(action_id)
    if not action:
        return {"error": "Action not found", "action_id": action_id}

    if action['status'] != 'pending':
        return {"error": f"Action already {action['status']}", "action_id": action_id}

    # Also abort the associated intent if it exists
    payload = action['payload']
    intent_id = payload.get('intent_id')
    if intent_id:
        database.update_intent_status(intent_id, 'aborted')

    # Update action status
    database.update_action_status(action_id, 'rejected')

    plugin.log(f"cl-hive: Rejected action {action_id}")

    return {
        "status": "rejected",
        "action_id": action_id,
        "action_type": action['action_type'],
    }


@plugin.method("hive-set-mode")
def hive_set_mode(plugin: Plugin, mode: str):
    """
    Change the governance mode at runtime.

    Args:
        mode: New governance mode ('advisor', 'autonomous', or 'oracle')

    Returns:
        Dict with new mode and previous mode.

    Permission: Admin only
    """
    from modules.config import VALID_GOVERNANCE_MODES

    # Permission check: Admin only
    perm_error = _check_permission('admin')
    if perm_error:
        return perm_error

    if not config:
        return {"error": "Config not initialized"}

    # Validate mode
    mode_lower = mode.lower()
    if mode_lower not in VALID_GOVERNANCE_MODES:
        return {
            "error": f"Invalid mode: {mode}",
            "valid_modes": list(VALID_GOVERNANCE_MODES)
        }

    # Check for oracle URL if switching to oracle mode
    if mode_lower == 'oracle' and not config.oracle_url:
        return {
            "error": "Cannot switch to oracle mode: oracle_url not configured",
            "hint": "Set hive-oracle-url option or configure oracle_url"
        }

    # Store previous mode
    previous_mode = config.governance_mode

    # Update config
    config.governance_mode = mode_lower
    config._version += 1

    plugin.log(f"cl-hive: Governance mode changed from {previous_mode} to {mode_lower}")

    return {
        "status": "ok",
        "previous_mode": previous_mode,
        "current_mode": mode_lower,
    }


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
    # Permission check: Admin only
    perm_error = _check_permission('admin')
    if perm_error:
        return perm_error

    if not config:
        return {"error": "Config not initialized"}

    previous = config.planner_enable_expansions
    config.planner_enable_expansions = enabled
    config._version += 1

    plugin.log(f"cl-hive: Expansion proposals {'enabled' if enabled else 'disabled'}")

    return {
        "status": "ok",
        "previous_setting": previous,
        "expansions_enabled": enabled,
    }


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
    if our_tier not in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value):
        return {"error": "permission_denied", "required_tier": "member or admin"}

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
    perm_error = _check_permission('admin')
    if perm_error:
        return perm_error

    if not database or not our_pubkey or not membership_mgr:
        return {"error": "Database not initialized"}

    # Check we're in bootstrap phase (member count < min_vouch_count)
    members = database.get_all_members()
    member_count = len(members)
    min_vouch = config.min_vouch_count if config else 3

    if member_count >= min_vouch:
        return {
            "error": "bootstrap_complete",
            "message": f"Hive has {member_count} members, use normal vouch process",
            "member_count": member_count,
            "min_vouch_count": min_vouch
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
    perm_error = _check_permission('admin')
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
    if member.get("tier") == MembershipTier.ADMIN.value:
        return {"error": "cannot_ban_admin", "peer_id": peer_id}

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
    Propose or approve promoting a member to admin.

    Requires 100% admin approval. When an admin calls this:
    - If no pending proposal exists, creates one and adds their approval
    - If proposal exists, adds their approval
    - When all admins have approved, the member is promoted to admin

    Args:
        peer_id: Public key of the member to promote to admin

    Returns:
        Dict with promotion status.

    Permission: Admin only
    """
    # Permission check: Admin only
    perm_error = _check_permission('admin')
    if perm_error:
        return perm_error

    if not database or not our_pubkey or not membership_mgr:
        return {"error": "Database not initialized"}

    # Check target exists and is a member (not neophyte, not already admin)
    target = database.get_member(peer_id)
    if not target:
        return {"error": "peer_not_found", "peer_id": peer_id}

    target_tier = target.get("tier")
    if target_tier == MembershipTier.ADMIN.value:
        return {"error": "already_admin", "peer_id": peer_id}
    if target_tier == MembershipTier.NEOPHYTE.value:
        return {"error": "must_be_member_first", "peer_id": peer_id,
                "message": "Neophytes must be promoted to member before admin"}

    # Get all current admins
    all_members = database.get_all_members()
    admins = [m for m in all_members if m.get("tier") == MembershipTier.ADMIN.value]
    admin_count = len(admins)
    admin_pubkeys = set(m["peer_id"] for m in admins)

    # Create or get existing proposal
    existing = database.get_admin_promotion(peer_id)
    if not existing:
        database.create_admin_promotion(peer_id, our_pubkey)
        plugin.log(f"cl-hive: Admin promotion proposed for {peer_id[:16]}...")

    # Add our approval
    database.add_admin_promotion_approval(peer_id, our_pubkey)

    # Check approvals
    approvals = database.get_admin_promotion_approvals(peer_id)
    approval_pubkeys = set(a["approver_peer_id"] for a in approvals)

    # Only count approvals from current admins
    valid_approvals = approval_pubkeys & admin_pubkeys
    approvals_needed = admin_count
    approvals_received = len(valid_approvals)

    # Check if 100% approval reached
    if valid_approvals == admin_pubkeys:
        # Promote to admin
        success = membership_mgr.set_tier(peer_id, MembershipTier.ADMIN.value)
        if success:
            database.complete_admin_promotion(peer_id)
            plugin.log(f"cl-hive: Promoted {peer_id[:16]}... to ADMIN (100% approval)")

            # Broadcast promotion
            promotion_payload = {
                "target_pubkey": peer_id,
                "request_id": f"admin_promo_{int(time.time())}",
                "new_tier": "admin",
                "vouches": [{"approver": pk} for pk in valid_approvals]
            }
            promo_msg = serialize(HiveMessageType.PROMOTION, promotion_payload)
            _broadcast_to_members(promo_msg)

            return {
                "status": "promoted",
                "peer_id": peer_id,
                "new_tier": "admin",
                "approvals": list(valid_approvals),
                "message": f"Promoted to admin with {approvals_received}/{approvals_needed} approvals"
            }
        else:
            return {"error": "promotion_failed", "peer_id": peer_id}
    else:
        # Still waiting for more approvals
        missing = admin_pubkeys - valid_approvals
        return {
            "status": "pending",
            "peer_id": peer_id,
            "approvals_received": approvals_received,
            "approvals_needed": approvals_needed,
            "approved_by": list(valid_approvals),
            "waiting_for": [pk[:16] + "..." for pk in missing],
            "message": f"Need {approvals_needed - approvals_received} more admin approval(s)"
        }


@plugin.method("hive-pending-admin-promotions")
def hive_pending_admin_promotions(plugin: Plugin):
    """
    View pending admin promotion proposals.

    Returns:
        Dict with pending admin promotions and their approval status.

    Permission: Admin only
    """
    perm_error = _check_permission('admin')
    if perm_error:
        return perm_error

    if not database:
        return {"error": "Database not initialized"}

    # Get all current admins
    all_members = database.get_all_members()
    admins = [m for m in all_members if m.get("tier") == MembershipTier.ADMIN.value]
    admin_pubkeys = set(m["peer_id"] for m in admins)

    pending = database.get_pending_admin_promotions()
    result = []

    for p in pending:
        target = p["target_peer_id"]
        approvals = database.get_admin_promotion_approvals(target)
        approval_pubkeys = set(a["approver_peer_id"] for a in approvals)
        valid_approvals = approval_pubkeys & admin_pubkeys

        result.append({
            "peer_id": target,
            "proposed_by": p["proposed_by"],
            "proposed_at": p["proposed_at"],
            "approvals_received": len(valid_approvals),
            "approvals_needed": len(admins),
            "approved_by": [pk[:16] + "..." for pk in valid_approvals],
            "waiting_for": [pk[:16] + "..." for pk in (admin_pubkeys - valid_approvals)]
        })

    return {
        "count": len(result),
        "admin_count": len(admins),
        "pending_promotions": result
    }


@plugin.method("hive-resign-admin")
def hive_resign_admin(plugin: Plugin):
    """
    Resign from admin status, becoming a regular member.

    The last admin cannot resign - there must always be at least one admin.

    Returns:
        Dict with resignation status.

    Permission: Admin only
    """
    # Permission check: Admin only
    perm_error = _check_permission('admin')
    if perm_error:
        return perm_error

    if not database or not our_pubkey or not membership_mgr:
        return {"error": "Database not initialized"}

    # Get all current admins
    all_members = database.get_all_members()
    admins = [m for m in all_members if m.get("tier") == MembershipTier.ADMIN.value]
    admin_count = len(admins)

    # Cannot resign if we're the last admin
    if admin_count <= 1:
        return {
            "error": "cannot_resign",
            "message": "Cannot resign: you are the only admin. Promote another member to admin first."
        }

    # Demote self to member
    success = membership_mgr.set_tier(our_pubkey, MembershipTier.MEMBER.value)
    if success:
        plugin.log(f"cl-hive: Admin {our_pubkey[:16]}... resigned to member")

        # Broadcast tier change
        tier_payload = {
            "peer_id": our_pubkey,
            "new_tier": "member",
            "reason": "admin_resignation"
        }
        tier_msg = serialize(HiveMessageType.PROMOTION, tier_payload)
        _broadcast_to_members(tier_msg)

        return {
            "status": "resigned",
            "peer_id": our_pubkey,
            "new_tier": "member",
            "remaining_admins": admin_count - 1,
            "message": "Successfully resigned from admin. You are now a member."
        }
    else:
        return {"error": "resignation_failed", "message": "Failed to update tier"}


@plugin.method("hive-leave")
def hive_leave(plugin: Plugin, reason: str = "voluntary"):
    """
    Voluntarily leave the hive.

    This removes you from the hive member list and notifies other members.
    Your fee policies will be reverted to dynamic.

    Restrictions:
    - The last admin cannot leave (would make hive headless)
    - Admins should resign first or promote another admin before leaving

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

    # Check if we're the last admin
    if our_tier == MembershipTier.ADMIN.value:
        all_members = database.get_all_members()
        admin_count = sum(1 for m in all_members if m.get("tier") == MembershipTier.ADMIN.value)
        if admin_count <= 1:
            return {
                "error": "cannot_leave",
                "message": "Cannot leave: you are the only admin. Promote another member to admin first, or the hive will become headless."
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

    Requires quorum vote (51% of members/admins) to execute.
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
                if m.get("tier") in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value)
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
                if m.get("tier") in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value)
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
    if not database:
        return {"error": "Database not initialized"}

    # Clean up expired proposals
    now = int(time.time())
    database.cleanup_expired_ban_proposals(now)

    # Get pending proposals
    proposals = database.get_pending_ban_proposals()

    # Get eligible voters info
    all_members = database.get_all_members()

    result = []
    for p in proposals:
        target_id = p["target_peer_id"]
        eligible = [m for m in all_members
                    if m.get("tier") in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value)
                    and m["peer_id"] != target_id]
        eligible_ids = set(m["peer_id"] for m in eligible)
        quorum_needed = int(len(eligible) * BAN_QUORUM_THRESHOLD) + 1

        votes = database.get_ban_votes(p["proposal_id"])
        approve_count = sum(1 for v in votes if v["vote"] == "approve" and v["voter_peer_id"] in eligible_ids)
        reject_count = sum(1 for v in votes if v["vote"] == "reject" and v["voter_peer_id"] in eligible_ids)

        # Check if we've voted
        my_vote = None
        if our_pubkey:
            for v in votes:
                if v["voter_peer_id"] == our_pubkey:
                    my_vote = v["vote"]
                    break

        result.append({
            "proposal_id": p["proposal_id"],
            "target_peer_id": target_id,
            "target_tier": database.get_member(target_id).get("tier") if database.get_member(target_id) else "unknown",
            "proposer": p["proposer_peer_id"][:16] + "...",
            "reason": p["reason"],
            "proposed_at": p["proposed_at"],
            "expires_at": p["expires_at"],
            "approve_count": approve_count,
            "reject_count": reject_count,
            "quorum_needed": quorum_needed,
            "my_vote": my_vote
        })

    return {
        "count": len(result),
        "proposals": result
    }


@plugin.method("hive-contribution")
def hive_contribution(plugin: Plugin, peer_id: str = None):
    """
    View contribution stats for a peer or self.

    Args:
        peer_id: Optional peer to view (defaults to self)

    Returns:
        Dict with contribution statistics.
    """
    if not contribution_mgr or not database:
        return {"error": "Contribution tracking not available"}

    target_id = peer_id or our_pubkey
    if not target_id:
        return {"error": "No peer specified and our_pubkey not available"}

    # Get contribution stats
    stats = contribution_mgr.get_contribution_stats(target_id)

    # Get member info
    member = database.get_member(target_id)

    # Get leech status
    leech_status = contribution_mgr.check_leech_status(target_id)

    result = {
        "peer_id": target_id,
        "forwarded_msat": stats["forwarded"],
        "received_msat": stats["received"],
        "contribution_ratio": round(stats["ratio"], 4),
        "is_leech": leech_status["is_leech"],
    }

    if member:
        result["tier"] = member.get("tier")
        result["uptime_pct"] = member.get("uptime_pct")

    return result


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
    
    This creates the first member record with admin privileges and
    generates a self-signed genesis ticket.
    
    Args:
        hive_id: Optional custom Hive identifier (auto-generated if not provided)
    
    Returns:
        Dict with genesis status and admin ticket
    """
    if not database or not safe_plugin or not handshake_mgr:
        return {"error": "Hive not initialized"}
    
    try:
        result = handshake_mgr.genesis(hive_id)
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

    Only Admins can generate invite tickets. Bootstrap invites (tier='admin')
    can only be generated once (to create the second admin). After 2 admins
    exist, all new members join as neophytes and need vouches for promotion.

    Args:
        valid_hours: Hours until ticket expires (default: 24)
        requirements: Bitmask of required features (default: 0 = none)
        tier: Starting tier - 'neophyte' (default) or 'admin' (bootstrap only)

    Returns:
        Dict with base64-encoded ticket

    Permission: Admin only
    """
    # Permission check: Admin only
    perm_error = _check_permission('admin')
    if perm_error:
        return perm_error

    if not handshake_mgr:
        return {"error": "Hive not initialized"}

    # Validate tier
    if tier not in ('neophyte', 'admin'):
        return {"error": f"Invalid tier: {tier}. Use 'neophyte' or 'admin' (bootstrap)"}

    try:
        ticket = handshake_mgr.generate_invite_ticket(valid_hours, requirements, tier)
        bootstrap_note = " (BOOTSTRAP - grants admin tier)" if tier == 'admin' else ""
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
    
    # Send HELLO message
    from modules.protocol import create_hello
    hello_msg = create_hello(ticket)
    
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
# MAIN
# =============================================================================

plugin.run()
