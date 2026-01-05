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

import os
import signal
import threading
from typing import Dict, Optional, Any

from pyln.client import Plugin, RpcError

# Import our modules
from modules.config import HiveConfig
from modules.database import HiveDatabase
from modules.protocol import (
    HIVE_MAGIC, HiveMessageType, 
    is_hive_message, deserialize, serialize,
    create_challenge, create_welcome
)
from modules.handshake import HandshakeManager, Ticket
from modules.state_manager import StateManager
from modules.gossip import GossipManager

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


class ThreadSafeRpcProxy:
    """
    A thread-safe proxy for the plugin's RPC interface.
    
    Ensures all RPC calls are serialized through a lock, preventing
    race conditions when multiple background threads make concurrent
    calls to lightningd.
    """
    
    def __init__(self, rpc):
        """Wrap the original RPC object."""
        self._rpc = rpc
    
    def __getattr__(self, name):
        """Intercept attribute access to wrap RPC method calls."""
        original_method = getattr(self._rpc, name)
        
        if callable(original_method):
            def thread_safe_method(*args, **kwargs):
                with RPC_LOCK:
                    return original_method(*args, **kwargs)
            return thread_safe_method
        else:
            return original_method
    
    def call(self, method_name, payload=None):
        """Thread-safe wrapper for the generic RPC call method."""
        with RPC_LOCK:
            if payload:
                return self._rpc.call(method_name, payload)
            return self._rpc.call(method_name)


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
    global database, config, safe_plugin, handshake_mgr
    
    plugin.log("cl-hive: Initializing Swarm Intelligence layer...")
    
    # Create thread-safe plugin proxy
    safe_plugin = ThreadSafePluginProxy(plugin)
    
    # Build configuration from options
    config = HiveConfig(
        db_path=options.get('hive-db-path', '~/.lightning/cl_hive.db'),
        governance_mode=options.get('hive-governance-mode', 'advisor'),
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
    )
    
    # Initialize database
    database = HiveDatabase(config.db_path, safe_plugin)
    database.initialize()
    plugin.log(f"cl-hive: Database initialized at {config.db_path}")
    
    # Initialize handshake manager
    handshake_mgr = HandshakeManager(safe_plugin.rpc, database, safe_plugin)
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
    
    # Verify cl-revenue-ops dependency (Circuit Breaker pattern)
    try:
        status = safe_plugin.rpc.call("revenue-status")
        version = status.get("version", "unknown")
        plugin.log(f"cl-hive: Found cl-revenue-ops {version}")
        
        # Check minimum version (v1.4.0+ required for Strategic Exemption)
        # For now, just log - full version parsing can be added later
        if "1.4" not in version and "1.5" not in version and "2." not in version:
            plugin.log(
                f"cl-hive: WARNING - cl-revenue-ops {version} may not support HIVE strategy. "
                "Recommended: v1.4.0+",
                level='warn'
            )
    except RpcError as e:
        plugin.log(
            f"cl-hive: WARNING - cl-revenue-ops not detected ({e}). "
            "Hive policy integration will be unavailable.",
            level='warn'
        )
    except Exception as e:
        plugin.log(f"cl-hive: Error checking cl-revenue-ops: {e}", level='warn')
    
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
        else:
            # Known but unimplemented message type (Phase 3+)
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
    
    # Generate challenge nonce
    nonce = handshake_mgr.generate_challenge(peer_id)
    
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
            manifest_signature=attest_data['manifest_signature']
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
    expected_nonce = handshake_mgr.get_pending_challenge(peer_id)
    if not expected_nonce:
        plugin.log(f"cl-hive: ATTEST from {peer_id[:16]}... but no pending challenge", level='warn')
        return {"result": "continue"}
    
    # Reconstruct manifest for verification
    manifest_data = {
        "pubkey": payload.get('pubkey'),
        "version": payload.get('version'),
        "features": payload.get('features', []),
        "timestamp": payload.get('timestamp', 0),
        "nonce": expected_nonce  # Use our expected nonce
    }
    
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
    
    # Verification passed! Add as Neophyte member
    import time
    database.add_member(
        peer_id=peer_id,
        tier='neophyte',
        joined_at=int(time.time())
    )
    
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
    
    # Send WELCOME
    welcome_msg = create_welcome(hive_id, 'neophyte', len(members), state_hash)
    
    try:
        safe_plugin.rpc.call("sendcustommsg", {
            "node_id": peer_id,
            "msg": welcome_msg.hex()
        })
        plugin.log(f"cl-hive: Sent WELCOME to {peer_id[:16]}... (new neophyte)")
    except Exception as e:
        plugin.log(f"cl-hive: Failed to send WELCOME: {e}", level='warn')
    
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
    
    # TODO: Store Hive membership info, initiate state sync
    
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
    
    accepted = gossip_mgr.process_gossip(peer_id, payload)
    
    if accepted:
        plugin.log(f"cl-hive: GOSSIP accepted from {peer_id[:16]}... "
                   f"(v{payload.get('version', '?')})", level='debug')
    
    return {"result": "continue"}


def handle_state_hash(peer_id: str, payload: Dict, plugin: Plugin) -> Dict:
    """
    Handle HIVE_STATE_HASH message (anti-entropy check).
    
    Compare remote hash against our local state. If mismatch,
    send a FULL_SYNC with our complete state.
    """
    if not gossip_mgr or not state_manager:
        return {"result": "continue"}
    
    hashes_match = gossip_mgr.process_state_hash(peer_id, payload)
    
    if not hashes_match:
        # State divergence detected - send FULL_SYNC
        plugin.log(f"cl-hive: State divergence with {peer_id[:16]}..., sending FULL_SYNC")
        
        full_sync_payload = gossip_mgr.create_full_sync_payload()
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
    """
    if not gossip_mgr:
        return {"result": "continue"}
    
    updated = gossip_mgr.process_full_sync(peer_id, payload)
    plugin.log(f"cl-hive: FULL_SYNC from {peer_id[:16]}...: {updated} states updated")
    
    return {"result": "continue"}


# =============================================================================
# PEER CONNECTION HOOK (State Hash Exchange)
# =============================================================================

@plugin.subscribe("peer_connected")
def on_peer_connected(peer_id: str, plugin: Plugin, **kwargs):
    """
    Hook called when a peer connects.
    
    If the peer is a Hive member, send a STATE_HASH message to
    initiate anti-entropy check and detect state divergence.
    """
    if not database or not gossip_mgr:
        return
    
    # Check if this peer is a Hive member
    member = database.get_member(peer_id)
    if not member:
        return  # Not a Hive member, ignore
    
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
def hive_invite(plugin: Plugin, valid_hours: int = 24, requirements: int = 0):
    """
    Generate an invitation ticket for a new member.
    
    Only Admins can generate invite tickets.
    
    Args:
        valid_hours: Hours until ticket expires (default: 24)
        requirements: Bitmask of required features (default: 0 = none)
    
    Returns:
        Dict with base64-encoded ticket
    """
    if not handshake_mgr:
        return {"error": "Hive not initialized"}
    
    try:
        ticket = handshake_mgr.generate_invite_ticket(valid_hours, requirements)
        return {
            "status": "ticket_generated",
            "ticket": ticket,
            "valid_hours": valid_hours,
            "instructions": "Share this ticket with the candidate. They should use 'hive-join <ticket>' to request membership."
        }
    except PermissionError as e:
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
