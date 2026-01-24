"""
RPC Command Handlers for cl-hive

This module contains the implementation logic for hive-* RPC commands.
The actual @plugin.method() decorators remain in cl-hive.py, which creates
thin wrappers that call these handler functions.

Design Pattern:
    - Each handler receives a HiveContext with all dependencies
    - Handlers are pure functions that can be easily tested
    - Permission checks are done via check_permission() helper
"""

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional


@dataclass
class HiveContext:
    """
    Context object holding all dependencies for RPC command handlers.

    This bundles the global state that commands need access to,
    making dependencies explicit and handlers testable.
    """
    database: Any  # HiveDatabase
    config: Any    # HiveConfig
    safe_plugin: Any  # ThreadSafePluginProxy
    our_pubkey: str
    vpn_transport: Any = None  # VPNTransportManager
    planner: Any = None  # Planner
    quality_scorer: Any = None  # PeerQualityScorer
    bridge: Any = None  # Bridge
    intent_mgr: Any = None  # IntentManager
    membership_mgr: Any = None  # MembershipManager
    coop_expansion_mgr: Any = None  # CooperativeExpansionManager
    contribution_mgr: Any = None  # ContributionManager
    routing_pool: Any = None  # RoutingPool (Phase 0 - Collective Economics)
    yield_metrics_mgr: Any = None  # YieldMetricsManager (Phase 1 - Metrics)
    liquidity_coordinator: Any = None  # LiquidityCoordinator (for competition detection)
    fee_coordination_mgr: Any = None  # FeeCoordinationManager (Phase 2 - Fee Coordination)
    cost_reduction_mgr: Any = None  # CostReductionManager (Phase 3 - Cost Reduction)
    rationalization_mgr: Any = None  # RationalizationManager (Channel Rationalization)
    strategic_positioning_mgr: Any = None  # StrategicPositioningManager (Phase 5 - Strategic Positioning)
    log: Callable[[str, str], None] = None  # Logger function: (msg, level) -> None


def check_permission(ctx: HiveContext, required_tier: str) -> Optional[Dict[str, Any]]:
    """
    Check if the local node has the required tier for an RPC command.

    Args:
        ctx: HiveContext with database and our_pubkey
        required_tier: 'member' (only tier that has special permissions)

    Returns:
        None if permission granted, or error dict if denied
    """
    if not ctx.our_pubkey or not ctx.database:
        return {"error": "Not initialized"}

    member = ctx.database.get_member(ctx.our_pubkey)
    if not member:
        return {"error": "Not a Hive member", "required_tier": required_tier}

    current_tier = member.get('tier', 'neophyte')

    if required_tier == 'member':
        if current_tier != 'member':
            return {
                "error": "permission_denied",
                "message": "This command requires member privileges",
                "current_tier": current_tier,
                "required_tier": "member"
            }

    return None  # Permission granted


# =============================================================================
# VPN COMMANDS
# =============================================================================

def vpn_status(ctx: HiveContext, peer_id: str = None) -> Dict[str, Any]:
    """
    Get VPN transport status and configuration.

    Shows the current VPN transport mode, configured subnets, peer mappings,
    and which hive members are connected via VPN.

    Args:
        ctx: HiveContext
        peer_id: Optional - Get VPN info for a specific peer

    Returns:
        Dict with VPN transport configuration and status.
    """
    if not ctx.vpn_transport:
        return {"error": "VPN transport not initialized"}

    if peer_id:
        # Get info for specific peer
        peer_info = ctx.vpn_transport.get_peer_vpn_info(peer_id)
        if peer_info:
            return {
                "peer_id": peer_id,
                **peer_info
            }
        return {
            "peer_id": peer_id,
            "message": "No VPN info for this peer"
        }

    # Return full status
    return ctx.vpn_transport.get_status()


def vpn_add_peer(ctx: HiveContext, pubkey: str, vpn_address: str) -> Dict[str, Any]:
    """
    Add or update a VPN peer mapping.

    Maps a node's pubkey to its VPN address for routing hive gossip.

    Args:
        ctx: HiveContext
        pubkey: Node pubkey
        vpn_address: VPN address in format ip:port or just ip (default port 9735)

    Returns:
        Dict with result.

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.vpn_transport:
        return {"error": "VPN transport not initialized"}

    # Parse address
    if ':' in vpn_address:
        ip, port_str = vpn_address.rsplit(':', 1)
        port = int(port_str)
    else:
        ip = vpn_address
        port = 9735

    success = ctx.vpn_transport.add_vpn_peer(pubkey, ip, port)
    if success:
        return {
            "success": True,
            "pubkey": pubkey,
            "vpn_address": f"{ip}:{port}",
            "message": "VPN peer mapping added"
        }
    return {
        "success": False,
        "error": "Failed to add peer - max peers may be reached"
    }


def vpn_remove_peer(ctx: HiveContext, pubkey: str) -> Dict[str, Any]:
    """
    Remove a VPN peer mapping.

    Args:
        ctx: HiveContext
        pubkey: Node pubkey to remove

    Returns:
        Dict with result.

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.vpn_transport:
        return {"error": "VPN transport not initialized"}

    success = ctx.vpn_transport.remove_vpn_peer(pubkey)
    if success:
        return {
            "success": True,
            "pubkey": pubkey,
            "message": "VPN peer mapping removed"
        }
    return {
        "success": False,
        "pubkey": pubkey,
        "message": "Peer not found in VPN mappings"
    }


# =============================================================================
# STATUS/CONFIG COMMANDS
# =============================================================================

def status(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get current Hive status and membership info.

    Returns:
        Dict with hive state, member count, governance mode, etc.
    """
    if not ctx.database:
        return {"error": "Hive not initialized"}

    members = ctx.database.get_all_members()
    member_count = len([m for m in members if m['tier'] == 'member'])
    neophyte_count = len([m for m in members if m['tier'] == 'neophyte'])

    # Get our own membership status (used by cl-revenue-ops to detect hive mode)
    our_membership = {"tier": None, "joined_at": None}
    if ctx.our_pubkey:
        our_member = ctx.database.get_member(ctx.our_pubkey)
        if our_member:
            our_membership = {
                "tier": our_member.get("tier"),
                "joined_at": our_member.get("joined_at"),
                "pubkey": ctx.our_pubkey,
            }

    return {
        "status": "active" if members else "no_members",
        "governance_mode": ctx.config.governance_mode if ctx.config else "unknown",
        "membership": our_membership,  # Our own membership for cl-revenue-ops detection
        "members": {
            "total": len(members),
            "member": member_count,
            "neophyte": neophyte_count,
        },
        "limits": {
            "max_members": ctx.config.max_members if ctx.config else 50,
            "market_share_cap": ctx.config.market_share_cap_pct if ctx.config else 0.20,
        },
        "version": "0.1.0-dev",
    }


def get_config(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get current Hive configuration values.

    Shows all config options and their current values. Useful for verifying
    hot-reload changes made via `lightning-cli setconfig`.

    Returns:
        Dict with all current config values and metadata.
    """
    if not ctx.config:
        return {"error": "Hive not initialized"}

    return {
        "config_version": ctx.config._version,
        "hot_reload_enabled": True,
        "immutable": {
            "db_path": ctx.config.db_path,
        },
        "governance": {
            "governance_mode": ctx.config.governance_mode,
            "failsafe_budget_per_day": ctx.config.failsafe_budget_per_day,
            "failsafe_actions_per_hour": ctx.config.failsafe_actions_per_hour,
        },
        "membership": {
            "membership_enabled": ctx.config.membership_enabled,
            "auto_join_enabled": ctx.config.auto_join_enabled,
            "auto_vouch_enabled": ctx.config.auto_vouch_enabled,
            "auto_promote_enabled": ctx.config.auto_promote_enabled,
            "ban_autotrigger_enabled": ctx.config.ban_autotrigger_enabled,
            "neophyte_fee_discount_pct": ctx.config.neophyte_fee_discount_pct,
            "member_fee_ppm": ctx.config.member_fee_ppm,
            "probation_days": ctx.config.probation_days,
            "min_contribution_ratio": ctx.config.min_contribution_ratio,
            "min_uptime_pct": ctx.config.min_uptime_pct,
            "min_unique_peers": ctx.config.min_unique_peers,
            "max_members": ctx.config.max_members,
        },
        "protocol": {
            "market_share_cap_pct": ctx.config.market_share_cap_pct,
            "intent_hold_seconds": ctx.config.intent_hold_seconds,
            "intent_expire_seconds": ctx.config.intent_expire_seconds,
            "gossip_threshold_pct": ctx.config.gossip_threshold_pct,
            "heartbeat_interval": ctx.config.heartbeat_interval,
        },
        "planner": {
            "planner_interval": ctx.config.planner_interval,
            "planner_enable_expansions": ctx.config.planner_enable_expansions,
            "planner_min_channel_sats": ctx.config.planner_min_channel_sats,
            "planner_max_channel_sats": ctx.config.planner_max_channel_sats,
            "planner_default_channel_sats": ctx.config.planner_default_channel_sats,
        },
        "vpn": ctx.vpn_transport.get_status() if ctx.vpn_transport else {"enabled": False},
    }


def members(ctx: HiveContext) -> Dict[str, Any]:
    """
    List all Hive members with their tier and stats.

    Returns:
        Dict with list of all members and their details.
    """
    if not ctx.database:
        return {"error": "Hive not initialized"}

    all_members = ctx.database.get_all_members()
    return {
        "count": len(all_members),
        "members": all_members,
    }


# =============================================================================
# ACTION MANAGEMENT COMMANDS
# =============================================================================

def pending_actions(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get all pending actions awaiting operator approval.

    Returns:
        Dict with list of pending actions.
    """
    if not ctx.database:
        return {"error": "Database not initialized"}

    actions = ctx.database.get_pending_actions()
    return {
        "count": len(actions),
        "actions": actions,
    }


def reject_action(ctx: HiveContext, action_id) -> Dict[str, Any]:
    """
    Reject pending action(s).

    Args:
        ctx: HiveContext
        action_id: ID of the action to reject, or "all" to reject all pending actions

    Returns:
        Dict with rejection result.

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.database:
        return {"error": "Database not initialized"}

    # Handle "all" option
    if action_id == "all":
        return _reject_all_actions(ctx)

    # Single action rejection - validate action_id
    try:
        action_id = int(action_id)
    except (ValueError, TypeError):
        return {"error": "Invalid action_id, must be an integer or 'all'"}

    # Get the action
    action = ctx.database.get_pending_action_by_id(action_id)
    if not action:
        return {"error": "Action not found", "action_id": action_id}

    if action['status'] != 'pending':
        return {"error": f"Action already {action['status']}", "action_id": action_id}

    # Also abort the associated intent if it exists
    payload = action['payload']
    intent_id = payload.get('intent_id')
    if intent_id:
        ctx.database.update_intent_status(intent_id, 'aborted')

    # Update action status
    ctx.database.update_action_status(action_id, 'rejected')

    if ctx.log:
        ctx.log(f"cl-hive: Rejected action {action_id}", 'info')

    return {
        "status": "rejected",
        "action_id": action_id,
        "action_type": action['action_type'],
    }


MAX_BULK_ACTIONS = 100  # CLAUDE.md: "Bound everything"


def _reject_all_actions(ctx: HiveContext) -> Dict[str, Any]:
    """Reject all pending actions (up to MAX_BULK_ACTIONS)."""
    actions = ctx.database.get_pending_actions()

    if not actions:
        return {"status": "no_actions", "message": "No pending actions to reject"}

    # Bound the number of actions processed (CLAUDE.md safety constraint)
    total_pending = len(actions)
    actions = actions[:MAX_BULK_ACTIONS]

    rejected = []
    errors = []

    for action in actions:
        action_id = action['id']
        try:
            # Abort associated intent if exists
            payload = action.get('payload', {})
            intent_id = payload.get('intent_id')
            if intent_id:
                ctx.database.update_intent_status(intent_id, 'aborted')

            # Update action status
            ctx.database.update_action_status(action_id, 'rejected')
            rejected.append({
                "action_id": action_id,
                "action_type": action['action_type']
            })
        except Exception as e:
            errors.append({"action_id": action_id, "error": str(e)})

    if ctx.log:
        ctx.log(f"cl-hive: Rejected {len(rejected)} actions", 'info')

    result = {
        "status": "rejected_all",
        "rejected_count": len(rejected),
        "rejected": rejected,
        "errors": errors if errors else None
    }

    # Warn if there were more actions than we processed
    if total_pending > MAX_BULK_ACTIONS:
        result["warning"] = f"Only processed {MAX_BULK_ACTIONS} of {total_pending} pending actions"

    return result


def budget_summary(ctx: HiveContext, days: int = 7) -> Dict[str, Any]:
    """
    Get budget usage summary for failsafe mode.

    Args:
        ctx: HiveContext
        days: Number of days of history to include (default: 7)

    Returns:
        Dict with budget utilization and spending history.

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.database:
        return {"error": "Database not initialized"}

    cfg = ctx.config.snapshot() if ctx.config else None
    if not cfg:
        return {"error": "Config not initialized"}

    daily_budget = cfg.failsafe_budget_per_day
    summary = ctx.database.get_budget_summary(daily_budget, days)

    return {
        "daily_budget_sats": daily_budget,
        "governance_mode": cfg.governance_mode,
        **summary
    }


def approve_action(ctx: HiveContext, action_id, amount_sats: int = None) -> Dict[str, Any]:
    """
    Approve and execute pending action(s).

    Args:
        ctx: HiveContext
        action_id: ID of the action to approve, or "all" to approve all pending actions
        amount_sats: Optional override for channel size (member budget control).
            If provided, uses this amount instead of the proposed amount.
            Must be >= min_channel_sats and will still be subject to budget limits.
            Only applies when approving a single action.

    Returns:
        Dict with approval result including budget details.

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.database:
        return {"error": "Database not initialized"}

    # Handle "all" option
    if action_id == "all":
        return _approve_all_actions(ctx)

    # Single action approval - validate action_id
    try:
        action_id = int(action_id)
    except (ValueError, TypeError):
        return {"error": "Invalid action_id, must be an integer or 'all'"}

    # Get the action
    action = ctx.database.get_pending_action_by_id(action_id)
    if not action:
        return {"error": "Action not found", "action_id": action_id}

    if action['status'] != 'pending':
        return {"error": f"Action already {action['status']}", "action_id": action_id}

    # Check if expired
    now = int(time.time())
    if action.get('expires_at') and now > action['expires_at']:
        ctx.database.update_action_status(action_id, 'expired')
        return {"error": "Action has expired", "action_id": action_id}

    action_type = action['action_type']
    payload = action['payload']

    # Execute based on action type
    if action_type == 'channel_open':
        return _execute_channel_open(ctx, action_id, action_type, payload, amount_sats)

    else:
        # Unknown action type - just mark as approved
        ctx.database.update_action_status(action_id, 'approved')
        return {
            "status": "approved",
            "action_id": action_id,
            "action_type": action_type,
            "note": "Unknown action type, marked as approved only"
        }


def _approve_all_actions(ctx: HiveContext) -> Dict[str, Any]:
    """Approve and execute all pending actions (up to MAX_BULK_ACTIONS)."""
    actions = ctx.database.get_pending_actions()

    if not actions:
        return {"status": "no_actions", "message": "No pending actions to approve"}

    # Bound the number of actions processed (CLAUDE.md safety constraint)
    total_pending = len(actions)
    actions = actions[:MAX_BULK_ACTIONS]

    approved = []
    errors = []
    now = int(time.time())

    for action in actions:
        action_id = action['id']
        action_type = action['action_type']

        try:
            # Check if expired
            if action.get('expires_at') and now > action['expires_at']:
                ctx.database.update_action_status(action_id, 'expired')
                errors.append({
                    "action_id": action_id,
                    "error": "Action has expired"
                })
                continue

            payload = action.get('payload', {})

            # Execute based on action type
            if action_type == 'channel_open':
                result = _execute_channel_open(ctx, action_id, action_type, payload)
                if 'error' in result:
                    errors.append({
                        "action_id": action_id,
                        "error": result['error']
                    })
                else:
                    approved.append({
                        "action_id": action_id,
                        "action_type": action_type,
                        "result": result.get('status', 'approved')
                    })
            else:
                # Unknown action type - just mark as approved
                ctx.database.update_action_status(action_id, 'approved')
                approved.append({
                    "action_id": action_id,
                    "action_type": action_type,
                    "note": "Unknown action type, marked as approved only"
                })

        except Exception as e:
            errors.append({"action_id": action_id, "error": str(e)})

    if ctx.log:
        ctx.log(f"cl-hive: Approved {len(approved)} actions", 'info')

    result = {
        "status": "approved_all",
        "approved_count": len(approved),
        "approved": approved,
        "errors": errors if errors else None
    }

    # Warn if there were more actions than we processed
    if total_pending > MAX_BULK_ACTIONS:
        result["warning"] = f"Only processed {MAX_BULK_ACTIONS} of {total_pending} pending actions"

    return result


def _execute_channel_open(
    ctx: HiveContext,
    action_id: int,
    action_type: str,
    payload: Dict[str, Any],
    amount_sats: int = None
) -> Dict[str, Any]:
    """
    Execute a channel_open action.

    This is a helper function for approve_action that handles all the
    channel opening logic including budget calculation, intent broadcast,
    peer connection, and fundchannel execution.
    """
    # Import protocol for message serialization (lazy import to avoid circular deps)
    from modules.protocol import HiveMessageType, serialize
    from modules.intent_manager import Intent

    # Extract channel details from payload
    target = payload.get('target')
    context = payload.get('context', {})
    intent_id = context.get('intent_id') or payload.get('intent_id')

    # Get channel size from context (planner) or top-level (cooperative expansion)
    # Ensure we get an int - JSON parsing can sometimes return strings
    proposed_size = (
        context.get('channel_size_sats') or
        context.get('amount_sats') or
        payload.get('amount_sats') or
        payload.get('channel_size_sats') or
        1_000_000  # Default 1M sats
    )
    proposed_size = int(proposed_size)  # Ensure int type

    # Apply member override if provided
    if amount_sats is not None:
        channel_size_sats = int(amount_sats)
        override_applied = True
    else:
        channel_size_sats = proposed_size
        override_applied = False

    if not target:
        return {"error": "Missing target in action payload", "action_id": action_id}

    # Check for existing or pending channels to this target
    try:
        peer_channels = ctx.safe_plugin.rpc.listpeerchannels(target)
        channels = peer_channels.get('channels', [])
        for ch in channels:
            state = ch.get('state', '')
            # Block if there's already an active or pending channel
            if state in ('CHANNELD_AWAITING_LOCKIN', 'CHANNELD_NORMAL', 'DUALOPEND_AWAITING_LOCKIN'):
                existing_capacity = ch.get('total_msat', 0) // 1000
                funding_txid = ch.get('funding_txid', 'unknown')
                return {
                    "error": f"Already have {'pending' if 'AWAITING' in state else 'active'} channel to this peer",
                    "action_id": action_id,
                    "target": target,
                    "existing_channel_state": state,
                    "existing_capacity_sats": existing_capacity,
                    "existing_funding_txid": funding_txid,
                    "hint": "Wait for pending channel to confirm or close existing channel first"
                }
    except Exception as e:
        # If listpeerchannels fails, log but continue (peer might not be known yet)
        if ctx.log:
            ctx.log(f"cl-hive: Could not check existing channels: {e}", 'debug')

    # Calculate intelligent budget limits
    cfg = ctx.config.snapshot() if ctx.config else None
    budget_info = {}
    if cfg:
        # Get onchain balance for reserve calculation
        try:
            funds = ctx.safe_plugin.rpc.listfunds()
            onchain_sats = sum(o.get('amount_msat', 0) // 1000 for o in funds.get('outputs', [])
                               if o.get('status') == 'confirmed')
        except Exception:
            onchain_sats = 0

        # Calculate budget components:
        # 1. Daily budget remaining
        daily_remaining = ctx.database.get_available_budget(cfg.failsafe_budget_per_day)

        # 2. Onchain reserve limit (keep reserve_pct for future expansion)
        spendable_onchain = int(onchain_sats * (1.0 - cfg.budget_reserve_pct))

        # 3. Max per-channel limit (percentage of daily budget)
        max_per_channel = int(cfg.failsafe_budget_per_day * cfg.budget_max_per_channel_pct)

        # Effective budget is the minimum of all constraints
        effective_budget = min(daily_remaining, spendable_onchain, max_per_channel)

        budget_info = {
            "onchain_sats": onchain_sats,
            "reserve_pct": cfg.budget_reserve_pct,
            "spendable_onchain": spendable_onchain,
            "daily_budget": cfg.failsafe_budget_per_day,
            "daily_remaining": daily_remaining,
            "max_per_channel_pct": cfg.budget_max_per_channel_pct,
            "max_per_channel": max_per_channel,
            "effective_budget": effective_budget,
        }

        if channel_size_sats > effective_budget:
            # Reduce to effective budget if it's above minimum
            if effective_budget >= cfg.planner_min_channel_sats:
                if ctx.log:
                    ctx.log(
                        f"cl-hive: Reducing channel size from {channel_size_sats:,} to {effective_budget:,} "
                        f"due to budget constraints (daily={daily_remaining:,}, reserve={spendable_onchain:,}, "
                        f"per-channel={max_per_channel:,})",
                        'info'
                    )
                channel_size_sats = effective_budget
            else:
                limiting_factor = "daily budget" if daily_remaining == effective_budget else \
                                 "reserve limit" if spendable_onchain == effective_budget else \
                                 "per-channel limit"
                return {
                    "error": f"Insufficient budget for channel open ({limiting_factor})",
                    "action_id": action_id,
                    "requested_sats": channel_size_sats,
                    "effective_budget_sats": effective_budget,
                    "min_channel_sats": cfg.planner_min_channel_sats,
                    "budget_info": budget_info,
                }

        # Validate member override is within bounds
        if override_applied and channel_size_sats < cfg.planner_min_channel_sats:
            return {
                "error": f"Override amount {channel_size_sats:,} below minimum {cfg.planner_min_channel_sats:,}",
                "action_id": action_id,
                "min_channel_sats": cfg.planner_min_channel_sats,
            }

    # Get intent from database (if available)
    intent_record = None
    if intent_id and ctx.database:
        intent_record = ctx.database.get_intent_by_id(intent_id)

    # Step 1: Broadcast the intent to all hive members (coordination)
    broadcast_count = 0
    if ctx.intent_mgr and intent_record:
        try:
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
            intent_payload = ctx.intent_mgr.create_intent_message(intent)
            msg = serialize(HiveMessageType.INTENT, intent_payload)
            members = ctx.database.get_all_members()

            for member in members:
                member_id = member.get('peer_id')
                if not member_id or member_id == ctx.our_pubkey:
                    continue
                try:
                    ctx.safe_plugin.rpc.call("sendcustommsg", {
                        "node_id": member_id,
                        "msg": msg.hex()
                    })
                    broadcast_count += 1
                except Exception:
                    pass

            if ctx.log:
                ctx.log(f"cl-hive: Broadcast intent to {broadcast_count} hive members", 'info')

        except Exception as e:
            if ctx.log:
                ctx.log(f"cl-hive: Intent broadcast failed: {e}", 'warn')

    # Step 2: Connect to target if not already connected
    try:
        # Check if already connected
        peers = ctx.safe_plugin.rpc.listpeers(target)
        if not peers.get('peers'):
            # Try to connect (will fail if no address known, but that's OK)
            try:
                ctx.safe_plugin.rpc.connect(target)
                if ctx.log:
                    ctx.log(f"cl-hive: Connected to {target[:16]}...", 'info')
            except Exception as conn_err:
                if ctx.log:
                    ctx.log(f"cl-hive: Could not connect to {target[:16]}...: {conn_err}", 'warn')
                # Continue anyway - fundchannel might still work if peer connects to us
    except Exception:
        pass

    # Step 3: Execute fundchannel to actually open the channel
    try:
        if ctx.log:
            ctx.log(
                f"cl-hive: Opening channel to {target[:16]}... "
                f"for {channel_size_sats:,} sats",
                'info'
            )

        # fundchannel with the calculated size
        # Use rpc.call() for explicit control over parameter names
        result = ctx.safe_plugin.rpc.call("fundchannel", {
            "id": target,
            "amount": channel_size_sats,
            "announce": True  # Public channel
        })

        channel_id = result.get('channel_id', 'unknown')
        txid = result.get('txid', 'unknown')

        if ctx.log:
            ctx.log(
                f"cl-hive: Channel opened! txid={txid[:16]}... "
                f"channel_id={channel_id}",
                'info'
            )

        # Update intent status if we have one
        if intent_id and ctx.database:
            ctx.database.update_intent_status(intent_id, 'committed')

        # Update action status
        ctx.database.update_action_status(action_id, 'executed')

        # Record budget spending
        ctx.database.record_budget_spend(
            action_type='channel_open',
            amount_sats=channel_size_sats,
            target=target,
            action_id=action_id
        )
        if ctx.log:
            ctx.log(f"cl-hive: Recorded budget spend of {channel_size_sats:,} sats", 'debug')

        result = {
            "status": "executed",
            "action_id": action_id,
            "action_type": action_type,
            "target": target,
            "channel_size_sats": channel_size_sats,
            "proposed_size_sats": proposed_size,
            "channel_id": channel_id,
            "txid": txid,
            "broadcast_count": broadcast_count,
            "sizing_reasoning": context.get('sizing_reasoning', 'N/A'),
        }
        if override_applied:
            result["override_applied"] = True
            result["override_amount"] = amount_sats
        if budget_info:
            result["budget_info"] = budget_info
        return result

    except Exception as e:
        error_msg = str(e)
        if ctx.log:
            ctx.log(f"cl-hive: fundchannel failed: {error_msg}", 'error')

        # Update action status to failed
        ctx.database.update_action_status(action_id, 'failed')

        # Classify the error to determine if delegation is appropriate
        failure_info = _classify_channel_open_failure(error_msg)

        result = {
            "status": "failed",
            "action_id": action_id,
            "action_type": action_type,
            "target": target,
            "channel_size_sats": channel_size_sats,
            "error": error_msg,
            "broadcast_count": broadcast_count,
            "failure_type": failure_info["type"],
            "delegation_recommended": failure_info["delegation_recommended"],
        }

        # If delegation is recommended, try to find a hive member to delegate
        if failure_info["delegation_recommended"] and ctx.database:
            delegation_result = _attempt_channel_open_delegation(
                ctx, target, channel_size_sats, action_id, failure_info
            )
            if delegation_result:
                result["delegation"] = delegation_result

        return result


def _classify_channel_open_failure(error_msg: str) -> Dict[str, Any]:
    """
    Classify channel open failure to determine appropriate response.

    Failure types:
    - peer_offline: Peer not reachable (temporary, retry later)
    - peer_rejected: Peer actively refused connection (may need different opener)
    - openingd_crash: Protocol error or stale state (peer issue)
    - insufficient_funds: We don't have enough funds
    - channel_exists: Already have a channel
    - unknown: Unclassified error

    Returns:
        Dict with failure type and whether delegation is recommended
    """
    error_lower = error_msg.lower()

    # Peer actively closed connection - might reject us specifically
    if "peer closed connection" in error_lower or "connection refused" in error_lower:
        return {
            "type": "peer_rejected",
            "delegation_recommended": True,
            "reason": "Peer may be rejecting connections from this node (reputation/policy)",
            "retry_delay_seconds": 0,  # Don't retry ourselves
        }

    # Openingd died - often indicates stale channel state or peer protocol issue
    if "openingd died" in error_lower or "subdaemon" in error_lower:
        return {
            "type": "openingd_crash",
            "delegation_recommended": True,
            "reason": "Protocol error or stale channel state with peer",
            "retry_delay_seconds": 0,
        }

    # Peer unreachable - might be temporarily offline
    if "no addresses" in error_lower or "connection timed out" in error_lower:
        return {
            "type": "peer_offline",
            "delegation_recommended": False,  # Peer is down for everyone
            "reason": "Peer appears to be offline",
            "retry_delay_seconds": 3600,  # Retry in 1 hour
        }

    # Insufficient funds
    if "insufficient" in error_lower or "not enough" in error_lower:
        return {
            "type": "insufficient_funds",
            "delegation_recommended": True,  # Another node might have funds
            "reason": "Insufficient on-chain funds",
            "retry_delay_seconds": 0,
        }

    # Channel already exists
    if "already have" in error_lower or "channel exists" in error_lower:
        return {
            "type": "channel_exists",
            "delegation_recommended": False,
            "reason": "Channel already exists with this peer",
            "retry_delay_seconds": 0,
        }

    # Unknown error
    return {
        "type": "unknown",
        "delegation_recommended": False,
        "reason": "Unknown error - manual investigation needed",
        "retry_delay_seconds": 3600,
    }


def _attempt_channel_open_delegation(
    ctx: HiveContext,
    target: str,
    channel_size_sats: int,
    original_action_id: int,
    failure_info: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Attempt to delegate a failed channel open to another hive member.

    NOTE: Task delegation protocol not yet implemented. This function
    returns a placeholder status. Future implementation will use
    AI_TASK_REQUEST messages to coordinate channel opens across the fleet.

    Returns:
        Dict with delegation status indicating feature not yet available
    """
    if ctx.log:
        ctx.log(
            f"cl-hive: Channel open delegation not yet implemented "
            f"(target={target[:16]}..., failure={failure_info.get('type', 'unknown')})",
            'debug'
        )

    return {
        "status": "delegation_not_implemented",
        "message": "Channel open delegation to other hive members not yet available",
        "failure_type": failure_info.get("type", "unknown"),
        "suggestion": "Retry later or open channel manually from another node"
    }


# =============================================================================
# GOVERNANCE COMMANDS
# =============================================================================

def set_mode(ctx: HiveContext, mode: str) -> Dict[str, Any]:
    """
    Change the governance mode at runtime.

    Args:
        ctx: HiveContext
        mode: New governance mode ('advisor' or 'failsafe')

    Returns:
        Dict with new mode and previous mode.

    Permission: Member only
    """
    from modules.config import VALID_GOVERNANCE_MODES

    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.config:
        return {"error": "Config not initialized"}

    # Validate mode
    mode_lower = mode.lower()
    if mode_lower not in VALID_GOVERNANCE_MODES:
        return {
            "error": f"Invalid mode: {mode}",
            "valid_modes": list(VALID_GOVERNANCE_MODES)
        }

    # Store previous mode
    previous_mode = ctx.config.governance_mode

    # Update config
    ctx.config.governance_mode = mode_lower
    ctx.config._version += 1

    if ctx.log:
        ctx.log(f"cl-hive: Governance mode changed from {previous_mode} to {mode_lower}", 'info')

    return {
        "status": "ok",
        "previous_mode": previous_mode,
        "current_mode": mode_lower,
    }


def enable_expansions(ctx: HiveContext, enabled: bool = True) -> Dict[str, Any]:
    """
    Enable or disable expansion proposals at runtime.

    Args:
        ctx: HiveContext
        enabled: True to enable expansions, False to disable (default: True)

    Returns:
        Dict with new setting.

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.config:
        return {"error": "Config not initialized"}

    previous = ctx.config.planner_enable_expansions
    ctx.config.planner_enable_expansions = enabled
    ctx.config._version += 1

    if ctx.log:
        ctx.log(f"cl-hive: Expansion proposals {'enabled' if enabled else 'disabled'}", 'info')

    return {
        "status": "ok",
        "previous_setting": previous,
        "expansions_enabled": enabled,
    }


def pending_promotions(ctx: HiveContext) -> Dict[str, Any]:
    """
    View pending manual promotion proposals.

    Shows neophytes proposed for early promotion to member status
    and the current approval count for each proposal.

    Returns:
        Dict with pending promotions and their approval status.

    Permission: Any hive member (read-only)
    """
    if not ctx.database or not ctx.membership_mgr:
        return {"error": "Not initialized"}

    pending = ctx.membership_mgr.get_pending_promotions()

    return {
        "count": len(pending),
        "pending_promotions": pending
    }


def propose_promotion(ctx: HiveContext, target_peer_id: str,
                      proposer_peer_id: str = None) -> Dict[str, Any]:
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
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.membership_mgr:
        return {"error": "Membership manager not initialized"}

    proposer = proposer_peer_id or ctx.our_pubkey
    return ctx.membership_mgr.propose_manual_promotion(target_peer_id, proposer)


def vote_promotion(ctx: HiveContext, target_peer_id: str,
                   voter_peer_id: str = None) -> Dict[str, Any]:
    """
    Vote to approve a neophyte's promotion to member.

    Args:
        target_peer_id: The neophyte being voted on
        voter_peer_id: Optional, defaults to our pubkey

    Permission: Member only
    """
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.membership_mgr:
        return {"error": "Membership manager not initialized"}

    voter = voter_peer_id or ctx.our_pubkey
    return ctx.membership_mgr.vote_on_promotion(target_peer_id, voter)


def execute_promotion(ctx: HiveContext, target_peer_id: str) -> Dict[str, Any]:
    """
    Execute a manual promotion if quorum has been reached.

    This bypasses the normal 90-day probation period when a majority
    of members have approved the promotion.

    Args:
        target_peer_id: The neophyte to promote

    Permission: Any member can execute once quorum is reached
    """
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.membership_mgr:
        return {"error": "Membership manager not initialized"}

    return ctx.membership_mgr.execute_manual_promotion(target_peer_id)


def pending_bans(ctx: HiveContext) -> Dict[str, Any]:
    """
    View pending ban proposals.

    Returns:
        Dict with pending ban proposals and their vote counts.

    Permission: Any member
    """
    from modules.membership import MembershipTier, BAN_QUORUM_THRESHOLD

    if not ctx.database:
        return {"error": "Database not initialized"}

    # Clean up expired proposals
    now = int(time.time())
    ctx.database.cleanup_expired_ban_proposals(now)

    # Get pending proposals
    proposals = ctx.database.get_pending_ban_proposals()

    # Get eligible voters info
    all_members = ctx.database.get_all_members()

    result = []
    for p in proposals:
        target_id = p["target_peer_id"]
        eligible = [m for m in all_members
                    if m.get("tier") == MembershipTier.MEMBER.value
                    and m["peer_id"] != target_id]
        eligible_ids = set(m["peer_id"] for m in eligible)
        quorum_needed = int(len(eligible) * BAN_QUORUM_THRESHOLD) + 1

        votes = ctx.database.get_ban_votes(p["proposal_id"])
        approve_count = sum(1 for v in votes if v["vote"] == "approve" and v["voter_peer_id"] in eligible_ids)
        reject_count = sum(1 for v in votes if v["vote"] == "reject" and v["voter_peer_id"] in eligible_ids)

        # Check if we've voted
        my_vote = None
        if ctx.our_pubkey:
            for v in votes:
                if v["voter_peer_id"] == ctx.our_pubkey:
                    my_vote = v["vote"]
                    break

        result.append({
            "proposal_id": p["proposal_id"],
            "target_peer_id": target_id,
            "target_tier": ctx.database.get_member(target_id).get("tier") if ctx.database.get_member(target_id) else "unknown",
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


# =============================================================================
# Phase 4: Topology, Planner, and Query Commands
# =============================================================================

def reinit_bridge(ctx: HiveContext) -> Dict[str, Any]:
    """
    Re-attempt bridge initialization if it failed at startup.

    Permission: Member only
    """
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.bridge:
        return {"error": "Bridge module not initialized"}

    # Import BridgeStatus here to avoid circular imports
    from modules.bridge import BridgeStatus

    previous_status = ctx.bridge.status.value
    new_status = ctx.bridge.reinitialize()

    return {
        "previous_status": previous_status,
        "new_status": new_status.value,
        "revenue_ops_version": ctx.bridge._revenue_ops_version,
        "clboss_available": ctx.bridge._clboss_available,
        "message": (
            "Bridge enabled successfully" if new_status == BridgeStatus.ENABLED
            else "Bridge still disabled - check cl-revenue-ops installation"
        )
    }


def topology(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get current topology analysis from the Planner.

    Returns:
        Dict with saturated targets, planner stats, and config.
    """
    if not ctx.planner:
        return {"error": "Planner not initialized"}
    if not ctx.config:
        return {"error": "Config not initialized"}

    # Take config snapshot
    cfg = ctx.config.snapshot()

    # Refresh network cache before analysis
    ctx.planner._refresh_network_cache(force=True)

    # Get saturated targets
    saturated = ctx.planner.get_saturated_targets(cfg)
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
    stats = ctx.planner.get_planner_stats()

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


def planner_log(ctx: HiveContext, limit: int = 50) -> Dict[str, Any]:
    """
    Get recent Planner decision logs.

    Args:
        limit: Maximum number of log entries to return (default: 50)

    Returns:
        Dict with log entries and count.
    """
    if not ctx.database:
        return {"error": "Database not initialized"}

    # Bound limit to prevent excessive queries
    limit = min(max(1, limit), 500)

    logs = ctx.database.get_planner_logs(limit=limit)
    return {
        "count": len(logs),
        "limit": limit,
        "logs": logs,
    }


def expansion_recommendations(ctx: HiveContext, limit: int = 10) -> Dict[str, Any]:
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
    if not ctx.planner:
        return {"error": "Planner not initialized"}
    if not ctx.config:
        return {"error": "Config not initialized"}

    # Take config snapshot
    cfg = ctx.config.snapshot()

    # Refresh network cache
    ctx.planner._refresh_network_cache(force=True)

    # Get underserved targets (already uses cooperation modules)
    underserved = ctx.planner.get_underserved_targets(cfg)

    # Bound limit
    limit = min(max(1, limit), 50)
    underserved = underserved[:limit]

    # Build detailed recommendations
    recommendations = []
    coverage_stats = {
        "well_covered_peers": 0,
        "partially_covered_peers": 0,
        "uncovered_peers": 0,
        "bottleneck_peers": 0
    }

    for target_result in underserved:
        # Get full expansion recommendation
        rec = ctx.planner.get_expansion_recommendation(target_result.target, cfg)

        # Update coverage stats
        if rec.hive_coverage_pct >= 0.60:
            coverage_stats["well_covered_peers"] += 1
        elif rec.hive_coverage_pct >= 0.20:
            coverage_stats["partially_covered_peers"] += 1
        else:
            coverage_stats["uncovered_peers"] += 1

        if rec.is_bottleneck:
            coverage_stats["bottleneck_peers"] += 1

        # Get node alias if available
        alias = target_result.target[:12] + "..."
        try:
            if ctx.safe_plugin:
                node_info = ctx.safe_plugin.rpc.listnodes(id=target_result.target)
                nodes = node_info.get("nodes", [])
                if nodes and nodes[0].get("alias"):
                    alias = nodes[0]["alias"]
        except Exception:
            pass

        recommendations.append({
            "target": target_result.target[:16] + "...",
            "target_full": target_result.target,
            "alias": alias,
            "recommendation": rec.recommendation_type,
            "score": round(rec.score, 4),
            "hive_coverage": f"{rec.hive_members_count}/{ctx.planner._get_hive_members().__len__()} members ({rec.hive_coverage_pct:.0%})",
            "hive_coverage_pct": round(rec.hive_coverage_pct * 100, 1),
            "hive_members_count": rec.hive_members_count,
            "competition_level": rec.competition_level,
            "network_channels": rec.network_channels,
            "is_bottleneck": rec.is_bottleneck,
            "reasoning": rec.reasoning,
            "details": rec.details,
            "quality_score": round(target_result.quality_score, 3),
            "quality_recommendation": target_result.quality_recommendation
        })

    return {
        "recommendations": recommendations,
        "count": len(recommendations),
        "coverage_summary": coverage_stats,
        "cooperation_modules": {
            "liquidity_coordinator": ctx.planner.liquidity_coordinator is not None,
            "splice_coordinator": ctx.planner.splice_coordinator is not None,
            "health_aggregator": ctx.planner.health_aggregator is not None
        }
    }


def intent_status(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get current intent status (local and remote intents).

    Returns:
        Dict with pending intents and stats.
    """
    if not ctx.planner or not ctx.planner.intent_manager:
        return {"error": "Intent manager not initialized"}

    intent_mgr = ctx.planner.intent_manager
    stats = intent_mgr.get_intent_stats()

    # Get pending local intents from DB
    pending = ctx.database.get_pending_intents() if ctx.database else []

    # Get remote intents from cache
    remote = intent_mgr.get_remote_intents()

    return {
        "local_pending": len(pending),
        "local_intents": pending,
        "remote_cached": len(remote),
        "remote_intents": [r.to_dict() for r in remote],
        "stats": stats
    }


def contribution(ctx: HiveContext, peer_id: str = None) -> Dict[str, Any]:
    """
    View contribution stats for a peer or self.

    Args:
        peer_id: Optional peer to view (defaults to self)

    Returns:
        Dict with contribution statistics.
    """
    if not ctx.contribution_mgr or not ctx.database:
        return {"error": "Contribution tracking not available"}

    target_id = peer_id or ctx.our_pubkey
    if not target_id:
        return {"error": "No peer specified and our_pubkey not available"}

    # Get contribution stats
    stats = ctx.contribution_mgr.get_contribution_stats(target_id)

    # Get member info
    member = ctx.database.get_member(target_id)

    # Get leech status
    leech_status = ctx.contribution_mgr.check_leech_status(target_id)

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


def expansion_status(ctx: HiveContext, round_id: str = None,
                     target_peer_id: str = None) -> Dict[str, Any]:
    """
    Get status of cooperative expansion rounds.

    Args:
        round_id: Get status of a specific round (optional)
        target_peer_id: Get rounds for a specific target peer (optional)

    Returns:
        Dict with expansion round status and statistics.
    """
    if not ctx.coop_expansion_mgr:
        return {"error": "Cooperative expansion not initialized"}

    if round_id:
        # Get specific round
        round_obj = ctx.coop_expansion_mgr.get_round(round_id)
        if not round_obj:
            return {"error": f"Round {round_id} not found"}
        return {
            "round_id": round_id,
            "round": round_obj.to_dict(),
            "nominations": [
                {
                    "nominator": n.nominator_id[:16] + "...",
                    "liquidity": n.available_liquidity_sats,
                    "quality_score": round(n.quality_score, 3),
                    "channel_count": n.channel_count,
                    "has_existing": n.has_existing_channel,
                }
                for n in round_obj.nominations.values()
            ]
        }

    if target_peer_id:
        # Get rounds for target
        rounds = ctx.coop_expansion_mgr.get_rounds_for_target(target_peer_id)
        return {
            "target_peer_id": target_peer_id,
            "count": len(rounds),
            "rounds": [r.to_dict() for r in rounds],
        }

    # Get overall status
    return ctx.coop_expansion_mgr.get_status()


# =============================================================================
# ROUTING POOL COMMANDS (Phase 0 - Collective Economics)
# =============================================================================

def pool_status(ctx: HiveContext, period: str = None) -> Dict[str, Any]:
    """
    Get current routing pool status and statistics.

    Shows pool revenue, member contributions, and distribution info.

    Args:
        ctx: HiveContext
        period: Optional period to query (format: YYYY-WW, defaults to current week)

    Returns:
        Dict with pool status including revenue, contributions, and distributions.
    """
    if not ctx.routing_pool:
        return {"error": "Routing pool not initialized"}

    try:
        status = ctx.routing_pool.get_pool_status(period)
        return status
    except Exception as e:
        return {"error": f"Failed to get pool status: {e}"}


def pool_member_status(ctx: HiveContext, peer_id: str = None) -> Dict[str, Any]:
    """
    Get routing pool status for a specific member.

    Shows the member's contribution scores, revenue share, and distribution history.

    Args:
        ctx: HiveContext
        peer_id: Member pubkey (defaults to self)

    Returns:
        Dict with member's pool status and history.
    """
    if not ctx.routing_pool:
        return {"error": "Routing pool not initialized"}

    target_id = peer_id or ctx.our_pubkey
    if not target_id:
        return {"error": "No peer specified and our_pubkey not available"}

    try:
        status = ctx.routing_pool.get_member_status(target_id)
        return status
    except Exception as e:
        return {"error": f"Failed to get member status: {e}"}


def pool_snapshot(ctx: HiveContext, period: str = None) -> Dict[str, Any]:
    """
    Trigger a contribution snapshot for all hive members.

    Takes a snapshot of current contribution metrics for all members.
    This is typically done automatically but can be triggered manually.

    Args:
        ctx: HiveContext
        period: Optional period to snapshot (format: YYYY-WW, defaults to current week)

    Returns:
        Dict with snapshot results.

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.routing_pool:
        return {"error": "Routing pool not initialized"}

    try:
        result = ctx.routing_pool.snapshot_contributions(period)
        return {
            "status": "ok",
            "period": result.get("period"),
            "members_snapshotted": result.get("members_snapshotted", 0),
            "contributions": result.get("contributions", [])
        }
    except Exception as e:
        return {"error": f"Failed to snapshot contributions: {e}"}


def pool_distribution(ctx: HiveContext, period: str = None) -> Dict[str, Any]:
    """
    Calculate distribution amounts for a period (dry run).

    Shows what each member would receive if the period were settled now.
    Does NOT actually settle the period - use pool_settle for that.

    Args:
        ctx: HiveContext
        period: Optional period to calculate (format: YYYY-WW, defaults to current week)

    Returns:
        Dict with calculated distribution amounts for each member.
    """
    if not ctx.routing_pool:
        return {"error": "Routing pool not initialized"}

    try:
        import datetime

        # Get current period if not specified
        if period is None:
            now = datetime.datetime.now()
            year, week, _ = now.isocalendar()
            period = f"{year}-W{week:02d}"

        # Get revenue for the period
        revenue_info = ctx.routing_pool.db.get_pool_revenue(period=period)
        total_revenue = revenue_info.get('total_sats', 0)

        # calculate_distribution returns Dict[str, int] mapping member_id to amount
        distributions_dict = ctx.routing_pool.calculate_distribution(period)

        # Convert to list format for JSON response
        distributions_list = [
            {"member_id": mid, "amount_sats": amt}
            for mid, amt in distributions_dict.items()
        ]

        return {
            "status": "calculated",
            "period": period,
            "total_revenue_sats": total_revenue,
            "distributions": distributions_list,
            "note": "This is a dry run - use pool-settle to actually distribute"
        }
    except Exception as e:
        return {"error": f"Failed to calculate distribution: {e}"}


def pool_settle(ctx: HiveContext, period: str = None, dry_run: bool = True) -> Dict[str, Any]:
    """
    Settle a routing pool period and record distributions.

    Calculates final distributions and records them to the database.
    This marks the period as settled and distributions as finalized.

    Args:
        ctx: HiveContext
        period: Period to settle (format: YYYY-WW, defaults to PREVIOUS week)
        dry_run: If True, calculate but don't actually record (default: True)

    Returns:
        Dict with settlement results.

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.routing_pool:
        return {"error": "Routing pool not initialized"}

    try:
        import datetime

        # Get period (default to previous week for settlement)
        if period is None:
            now = datetime.datetime.now()
            last_week = now - datetime.timedelta(days=7)
            year, week, _ = last_week.isocalendar()
            period = f"{year}-W{week:02d}"

        if dry_run:
            # Just calculate
            revenue_info = ctx.routing_pool.db.get_pool_revenue(period=period)
            total_revenue = revenue_info.get('total_sats', 0)

            distributions_dict = ctx.routing_pool.calculate_distribution(period)
            distributions_list = [
                {"member_id": mid, "amount_sats": amt}
                for mid, amt in distributions_dict.items()
            ]

            return {
                "status": "dry_run",
                "period": period,
                "total_revenue_sats": total_revenue,
                "distributions": distributions_list,
                "note": "Set dry_run=false to actually settle this period"
            }
        else:
            # Actually settle
            result = ctx.routing_pool.settle_period(period)
            return {
                "status": "settled",
                "period": result.get("period"),
                "total_revenue_sats": result.get("total_revenue_sats", 0),
                "distributions": result.get("distributions", []),
                "settled_at": result.get("settled_at")
            }
    except Exception as e:
        return {"error": f"Failed to settle period: {e}"}


def pool_record_revenue(ctx: HiveContext, amount_sats: int, channel_id: str = None,
                        payment_hash: str = None) -> Dict[str, Any]:
    """
    Manually record routing revenue to the pool.

    Normally revenue is recorded automatically from forward events,
    but this allows manual recording for testing or corrections.

    Args:
        ctx: HiveContext
        amount_sats: Revenue amount in satoshis
        channel_id: Optional channel ID (SCID format)
        payment_hash: Optional payment hash for tracking

    Returns:
        Dict with recording result.

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.routing_pool:
        return {"error": "Routing pool not initialized"}

    if amount_sats <= 0:
        return {"error": "Amount must be positive"}

    if amount_sats > 1_000_000_000:  # 10 BTC sanity check
        return {"error": "Amount exceeds sanity limit (10 BTC)"}

    try:
        ctx.routing_pool.record_revenue(
            member_id=ctx.our_pubkey,
            amount_sats=amount_sats,
            channel_id=channel_id,
            payment_hash=payment_hash
        )
        return {
            "status": "ok",
            "recorded_sats": amount_sats,
            "member_id": ctx.our_pubkey[:16] + "...",
            "channel_id": channel_id
        }
    except Exception as e:
        return {"error": f"Failed to record revenue: {e}"}


# =============================================================================
# YIELD METRICS COMMANDS (Phase 1 - Metrics & Measurement)
# =============================================================================

def yield_metrics(ctx: HiveContext, channel_id: str = None,
                  period_days: int = 30) -> Dict[str, Any]:
    """
    Get yield metrics for channels.

    Shows ROI, capital efficiency, turn rate, and flow characteristics.

    Args:
        ctx: HiveContext
        channel_id: Optional specific channel (None for all)
        period_days: Analysis period in days (default: 30)

    Returns:
        Dict with channel yield metrics.
    """
    if not ctx.yield_metrics_mgr:
        return {"error": "Yield metrics manager not initialized"}

    try:
        metrics = ctx.yield_metrics_mgr.get_channel_yield_metrics(
            channel_id=channel_id,
            period_days=period_days
        )

        return {
            "status": "ok",
            "period_days": period_days,
            "channel_count": len(metrics),
            "channels": [m.to_dict() for m in metrics]
        }
    except Exception as e:
        return {"error": f"Failed to get yield metrics: {e}"}


def yield_summary(ctx: HiveContext, period_days: int = 30) -> Dict[str, Any]:
    """
    Get aggregated yield summary for the fleet.

    Shows total revenue, ROI, and channel health distribution.

    Args:
        ctx: HiveContext
        period_days: Analysis period in days (default: 30)

    Returns:
        Dict with fleet yield summary.
    """
    if not ctx.yield_metrics_mgr:
        return {"error": "Yield metrics manager not initialized"}

    try:
        summary = ctx.yield_metrics_mgr.get_fleet_yield_summary(
            period_days=period_days
        )

        return {
            "status": "ok",
            **summary.to_dict()
        }
    except Exception as e:
        return {"error": f"Failed to get yield summary: {e}"}


def velocity_prediction(ctx: HiveContext, channel_id: str,
                        hours: int = 24) -> Dict[str, Any]:
    """
    Predict channel balance at future time based on flow velocity.

    Shows depletion/saturation risk and recommended actions.

    Args:
        ctx: HiveContext
        channel_id: Channel to predict
        hours: Hours into the future to predict (default: 24)

    Returns:
        Dict with velocity prediction.
    """
    if not ctx.yield_metrics_mgr:
        return {"error": "Yield metrics manager not initialized"}

    if not channel_id:
        return {"error": "channel_id is required"}

    try:
        prediction = ctx.yield_metrics_mgr.predict_channel_state(
            channel_id=channel_id,
            hours=hours
        )

        if not prediction:
            return {"error": "Insufficient data for prediction"}

        return {
            "status": "ok",
            **prediction.to_dict()
        }
    except Exception as e:
        return {"error": f"Failed to predict channel state: {e}"}


def critical_velocity_channels(ctx: HiveContext,
                               threshold_hours: int = 24) -> Dict[str, Any]:
    """
    Get channels with critical velocity (depleting/filling rapidly).

    These channels need urgent attention (fee changes or rebalancing).

    Args:
        ctx: HiveContext
        threshold_hours: Alert if depletion/saturation within this time

    Returns:
        Dict with critical velocity channels.
    """
    if not ctx.yield_metrics_mgr:
        return {"error": "Yield metrics manager not initialized"}

    try:
        critical = ctx.yield_metrics_mgr.get_critical_velocity_channels(
            threshold_hours=threshold_hours
        )

        return {
            "status": "ok",
            "threshold_hours": threshold_hours,
            "critical_count": len(critical),
            "channels": [p.to_dict() for p in critical]
        }
    except Exception as e:
        return {"error": f"Failed to get critical velocity channels: {e}"}


def internal_competition(ctx: HiveContext) -> Dict[str, Any]:
    """
    Detect internal competition between fleet members.

    Shows routes where multiple members compete, causing fee undercutting.

    Args:
        ctx: HiveContext

    Returns:
        Dict with internal competition analysis.
    """
    if not ctx.liquidity_coordinator:
        return {"error": "Liquidity coordinator not initialized"}

    try:
        summary = ctx.liquidity_coordinator.get_internal_competition_summary()
        return summary
    except Exception as e:
        return {"error": f"Failed to detect internal competition: {e}"}


# =============================================================================
# PHASE 2: FEE COORDINATION RPC COMMANDS
# =============================================================================

def fee_recommendation(
    ctx: HiveContext,
    channel_id: str,
    current_fee: int = 500,
    local_balance_pct: float = 0.5,
    source: str = None,
    destination: str = None
) -> Dict[str, Any]:
    """
    Get coordinated fee recommendation for a channel.

    Combines corridor assignment, adaptive pheromone signals,
    stigmergic markers, and defensive adjustments.

    Args:
        ctx: HiveContext
        channel_id: Channel ID to get recommendation for
        current_fee: Current fee in ppm (default: 500)
        local_balance_pct: Current local balance percentage (default: 0.5)
        source: Source peer hint for corridor lookup
        destination: Destination peer hint for corridor lookup

    Returns:
        Dict with fee recommendation and reasoning.
    """
    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    try:
        # Get peer_id from channel if possible
        peer_id = ""
        if ctx.safe_plugin:
            try:
                channels = ctx.safe_plugin.rpc.listpeerchannels()
                for ch in channels.get("channels", []):
                    if ch.get("short_channel_id") == channel_id:
                        peer_id = ch.get("peer_id", "")
                        break
            except Exception:
                pass

        recommendation = ctx.fee_coordination_mgr.get_fee_recommendation(
            channel_id=channel_id,
            peer_id=peer_id,
            current_fee=current_fee,
            local_balance_pct=local_balance_pct,
            source_hint=source,
            destination_hint=destination
        )

        return recommendation.to_dict()

    except Exception as e:
        return {"error": f"Failed to get fee recommendation: {e}"}


def corridor_assignments(ctx: HiveContext, force_refresh: bool = False) -> Dict[str, Any]:
    """
    Get flow corridor assignments for the fleet.

    Shows which member is primary for each (source, destination) pair.

    Args:
        ctx: HiveContext
        force_refresh: Force refresh of cached assignments

    Returns:
        Dict with corridor assignments and statistics.
    """
    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    try:
        assignments = ctx.fee_coordination_mgr.corridor_mgr.get_assignments(
            force_refresh=force_refresh
        )

        # Categorize by competition level
        by_level = {
            "none": [], "low": [], "medium": [], "high": []
        }
        for a in assignments:
            level = a.corridor.competition_level
            if level in by_level:
                by_level[level].append(a.to_dict())

        return {
            "total_corridors": len(assignments),
            "by_competition_level": {
                level: len(items) for level, items in by_level.items()
            },
            "assignments": [a.to_dict() for a in assignments],
            "our_primary_corridors": [
                a.to_dict() for a in assignments
                if a.primary_member == ctx.our_pubkey
            ]
        }

    except Exception as e:
        return {"error": f"Failed to get corridor assignments: {e}"}


def stigmergic_markers(ctx: HiveContext, source: str = None, destination: str = None) -> Dict[str, Any]:
    """
    Get stigmergic route markers from the fleet.

    Shows fee signals left by members after routing attempts.

    Args:
        ctx: HiveContext
        source: Filter by source peer
        destination: Filter by destination peer

    Returns:
        Dict with route markers and analysis.
    """
    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    try:
        if source and destination:
            markers = ctx.fee_coordination_mgr.stigmergic_coord.read_markers(
                source, destination
            )
        else:
            markers = ctx.fee_coordination_mgr.stigmergic_coord.get_all_markers()

        # Analyze markers
        successful = [m for m in markers if m.success]
        failed = [m for m in markers if not m.success]

        avg_success_fee = (
            sum(m.fee_ppm for m in successful) / len(successful)
            if successful else 0
        )
        avg_failed_fee = (
            sum(m.fee_ppm for m in failed) / len(failed)
            if failed else 0
        )

        return {
            "total_markers": len(markers),
            "successful_markers": len(successful),
            "failed_markers": len(failed),
            "avg_successful_fee_ppm": int(avg_success_fee),
            "avg_failed_fee_ppm": int(avg_failed_fee),
            "markers": [m.to_dict() for m in markers[:50]],  # Limit output
            "filtered": {
                "source": source,
                "destination": destination
            } if source or destination else None
        }

    except Exception as e:
        return {"error": f"Failed to get stigmergic markers: {e}"}


def deposit_marker(
    ctx: HiveContext,
    source: str,
    destination: str,
    fee_ppm: int,
    success: bool,
    volume_sats: int
) -> Dict[str, Any]:
    """
    Deposit a stigmergic route marker.

    Used to report routing outcomes to the fleet for indirect coordination.

    Args:
        ctx: HiveContext
        source: Source peer ID
        destination: Destination peer ID
        fee_ppm: Fee charged in ppm
        success: Whether routing succeeded
        volume_sats: Volume routed in sats

    Returns:
        Dict with deposited marker info.
    """
    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    try:
        marker = ctx.fee_coordination_mgr.stigmergic_coord.deposit_marker(
            source=source,
            destination=destination,
            fee_charged=fee_ppm,
            success=success,
            volume_sats=volume_sats
        )

        return {
            "status": "deposited",
            "marker": marker.to_dict()
        }

    except Exception as e:
        return {"error": f"Failed to deposit marker: {e}"}


def defense_status(ctx: HiveContext, peer_id: str = None) -> Dict[str, Any]:
    """
    Get mycelium defense system status.

    Shows active warnings and defensive fee adjustments.
    If peer_id is specified, includes peer_threat info for that peer.

    Args:
        ctx: HiveContext
        peer_id: Optional peer to check for threats

    Returns:
        Dict with defense system status.
        If peer_id specified, includes peer_threat with is_threat, threat_type, etc.
    """
    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    try:
        result = ctx.fee_coordination_mgr.defense_system.get_defense_status()

        # If peer_id specified, add peer-specific threat info
        if peer_id:
            peer_threat = {
                "is_threat": False,
                "threat_type": None,
                "severity": 0.0,
                "defensive_multiplier": 1.0
            }

            # Check if this peer has any active warnings
            for warning in result.get("active_warnings", []):
                if warning.get("peer_id") == peer_id:
                    peer_threat = {
                        "is_threat": True,
                        "threat_type": warning.get("threat_type"),
                        "severity": warning.get("severity", 0.5),
                        "defensive_multiplier": warning.get("defensive_multiplier", 1.0)
                    }
                    break

            result["peer_threat"] = peer_threat

        return result

    except Exception as e:
        return {"error": f"Failed to get defense status: {e}"}


def broadcast_warning(
    ctx: HiveContext,
    peer_id: str,
    threat_type: str = "drain",
    severity: float = 0.5
) -> Dict[str, Any]:
    """
    Broadcast a peer warning to the fleet.

    Permission: Member only

    Args:
        ctx: HiveContext
        peer_id: Peer to warn about
        threat_type: Type of threat ('drain', 'unreliable', 'force_close')
        severity: Severity from 0.0 to 1.0

    Returns:
        Dict with broadcast result.
    """
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    if threat_type not in ("drain", "unreliable", "force_close"):
        return {"error": f"Invalid threat_type: {threat_type}"}

    if not 0.0 <= severity <= 1.0:
        return {"error": "Severity must be between 0.0 and 1.0"}

    try:
        from modules.fee_coordination import PeerWarning, WARNING_TTL_HOURS

        warning = PeerWarning(
            peer_id=peer_id,
            threat_type=threat_type,
            severity=severity,
            reporter=ctx.our_pubkey,
            timestamp=time.time(),
            ttl=WARNING_TTL_HOURS * 3600
        )

        success = ctx.fee_coordination_mgr.defense_system.broadcast_warning(warning)

        return {
            "status": "broadcast" if success else "stored_locally",
            "warning": warning.to_dict()
        }

    except Exception as e:
        return {"error": f"Failed to broadcast warning: {e}"}


def pheromone_levels(ctx: HiveContext, channel_id: str = None) -> Dict[str, Any]:
    """
    Get pheromone levels for adaptive fee control.

    Shows the "memory" of successful fees for channels.

    Args:
        ctx: HiveContext
        channel_id: Optional specific channel

    Returns:
        Dict with pheromone levels.
    """
    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    try:
        all_levels = ctx.fee_coordination_mgr.adaptive_controller.get_all_pheromone_levels()

        if channel_id:
            level = all_levels.get(channel_id, 0.0)
            return {
                "channel_id": channel_id,
                "pheromone_level": round(level, 2),
                "above_exploit_threshold": level > 10.0
            }

        # Sort by level descending
        sorted_levels = sorted(
            all_levels.items(),
            key=lambda x: x[1],
            reverse=True
        )

        return {
            "total_channels": len(all_levels),
            "channels_above_threshold": sum(
                1 for _, v in all_levels.items() if v > 10.0
            ),
            "levels": [
                {"channel_id": k, "level": round(v, 2)}
                for k, v in sorted_levels[:50]
            ]
        }

    except Exception as e:
        return {"error": f"Failed to get pheromone levels: {e}"}


def fee_coordination_status(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get overall fee coordination status.

    Comprehensive view of all Phase 2 coordination systems.

    Args:
        ctx: HiveContext

    Returns:
        Dict with fee coordination status.
    """
    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    try:
        return ctx.fee_coordination_mgr.get_coordination_status()

    except Exception as e:
        return {"error": f"Failed to get coordination status: {e}"}


# =============================================================================
# YIELD OPTIMIZATION PHASE 3: COST REDUCTION
# =============================================================================
# Reduce rebalancing costs by 50% through:
# - Predictive rebalancing (low urgency = low fees)
# - Fleet rebalance routing (internal paths cheaper)
# - Circular flow detection (eliminate waste)

def rebalance_recommendations(
    ctx: HiveContext,
    prediction_hours: int = 24
) -> Dict[str, Any]:
    """
    Get predictive rebalance recommendations.

    Analyzes channels to find those predicted to deplete or saturate,
    with recommendations for preemptive rebalancing at lower fees.

    Args:
        ctx: HiveContext
        prediction_hours: How far ahead to predict (default: 24)

    Returns:
        Dict with rebalance recommendations sorted by urgency.
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction not initialized"}

    try:
        recommendations = ctx.cost_reduction_mgr.get_rebalance_recommendations(
            prediction_hours=prediction_hours
        )

        # Summarize by urgency
        by_urgency = {
            "critical": [],
            "high": [],
            "medium": [],
            "low": []
        }

        for rec in recommendations:
            urgency = rec.get("urgency", "low")
            if urgency in by_urgency:
                by_urgency[urgency].append(rec)

        return {
            "recommendations": recommendations,
            "by_urgency": by_urgency,
            "total_count": len(recommendations),
            "critical_count": len(by_urgency["critical"]),
            "prediction_hours": prediction_hours
        }

    except Exception as e:
        return {"error": f"Failed to get rebalance recommendations: {e}"}


def fleet_rebalance_path(
    ctx: HiveContext,
    from_channel: str,
    to_channel: str,
    amount_sats: int
) -> Dict[str, Any]:
    """
    Get fleet rebalance path recommendation.

    Checks if rebalancing through fleet members is cheaper than
    external routing. Fleet members have coordinated fees and
    can offer internal "friendship" rates.

    Args:
        ctx: HiveContext
        from_channel: Source channel SCID
        to_channel: Destination channel SCID
        amount_sats: Amount to rebalance

    Returns:
        Dict with path recommendation and savings estimate.
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction not initialized"}

    try:
        return ctx.cost_reduction_mgr.get_fleet_rebalance_path(
            from_channel=from_channel,
            to_channel=to_channel,
            amount_sats=amount_sats
        )

    except Exception as e:
        return {"error": f"Failed to get fleet path: {e}"}


def record_rebalance_outcome(
    ctx: HiveContext,
    from_channel: str,
    to_channel: str,
    amount_sats: int,
    cost_sats: int,
    success: bool,
    via_fleet: bool = False
) -> Dict[str, Any]:
    """
    Record a rebalance outcome for tracking and circular flow detection.

    Should be called after each rebalance attempt (success or failure).
    Enables detection of wasteful circular flows like A→B→C→A.

    Args:
        ctx: HiveContext
        from_channel: Source channel SCID
        to_channel: Destination channel SCID
        amount_sats: Amount rebalanced
        cost_sats: Cost paid
        success: Whether rebalance succeeded
        via_fleet: Whether routed through fleet members

    Returns:
        Dict with recording result and any circular flow warnings.
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction not initialized"}

    try:
        return ctx.cost_reduction_mgr.record_rebalance_outcome(
            from_channel=from_channel,
            to_channel=to_channel,
            amount_sats=amount_sats,
            cost_sats=cost_sats,
            success=success,
            via_fleet=via_fleet
        )

    except Exception as e:
        return {"error": f"Failed to record rebalance outcome: {e}"}


def circular_flow_status(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get circular flow detection status.

    Shows any detected circular flows (e.g., A→B→C→A) that waste
    fees moving liquidity in circles.

    Args:
        ctx: HiveContext

    Returns:
        Dict with circular flow status and detected patterns.
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction not initialized"}

    try:
        return ctx.cost_reduction_mgr.circular_detector.get_circular_flow_status()

    except Exception as e:
        return {"error": f"Failed to get circular flow status: {e}"}


def cost_reduction_status(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get overall cost reduction status.

    Comprehensive view of all Phase 3 cost reduction systems:
    - Predictive rebalancing
    - Fleet routing
    - Circular flow detection

    Args:
        ctx: HiveContext

    Returns:
        Dict with cost reduction status.
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction not initialized"}

    try:
        return ctx.cost_reduction_mgr.get_cost_reduction_status()

    except Exception as e:
        return {"error": f"Failed to get cost reduction status: {e}"}


# =============================================================================
# CHANNEL RATIONALIZATION COMMANDS
# =============================================================================

def coverage_analysis(
    ctx: HiveContext,
    peer_id: str = None
) -> Dict[str, Any]:
    """
    Analyze fleet coverage for redundant channels.

    Shows which fleet members have channels to the same peers
    and determines ownership based on routing activity.

    Args:
        ctx: HiveContext
        peer_id: Specific peer to analyze, or None for all redundant peers

    Returns:
        Dict with coverage analysis showing ownership and redundancy.
    """
    if not ctx.rationalization_mgr:
        return {"error": "Rationalization not initialized"}

    try:
        return ctx.rationalization_mgr.analyze_coverage(peer_id=peer_id)

    except Exception as e:
        return {"error": f"Failed to analyze coverage: {e}"}


def close_recommendations(
    ctx: HiveContext,
    our_node_only: bool = False
) -> Dict[str, Any]:
    """
    Get channel close recommendations for underperforming redundant channels.

    Uses stigmergic markers (routing success) to determine which member
    "owns" each peer relationship. Recommends closes for members with
    <10% of the owner's routing activity.

    Args:
        ctx: HiveContext
        our_node_only: If True, only return recommendations for our node

    Returns:
        Dict with close recommendations sorted by urgency.
    """
    if not ctx.rationalization_mgr:
        return {"error": "Rationalization not initialized"}

    try:
        recommendations = ctx.rationalization_mgr.get_close_recommendations(
            for_our_node_only=our_node_only
        )

        # Summarize
        by_urgency = {"high": 0, "medium": 0, "low": 0}
        total_freed = 0
        for rec in recommendations:
            by_urgency[rec.get("urgency", "low")] += 1
            total_freed += rec.get("freed_capital_sats", 0)

        return {
            "recommendations": recommendations,
            "count": len(recommendations),
            "by_urgency": by_urgency,
            "potential_freed_capital_sats": total_freed,
            "potential_freed_btc": round(total_freed / 100_000_000, 4)
        }

    except Exception as e:
        return {"error": f"Failed to get close recommendations: {e}"}


def create_close_actions(ctx: HiveContext) -> Dict[str, Any]:
    """
    Create pending_actions for close recommendations.

    Puts high-confidence close recommendations into the pending_actions
    queue for AI/human approval.

    Args:
        ctx: HiveContext

    Returns:
        Dict with number of actions created.
    """
    if not ctx.rationalization_mgr:
        return {"error": "Rationalization not initialized"}

    try:
        return ctx.rationalization_mgr.create_close_actions()

    except Exception as e:
        return {"error": f"Failed to create close actions: {e}"}


def rationalization_summary(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get summary of channel rationalization analysis.

    Shows fleet coverage health: well-owned peers, contested peers,
    orphan peers, and recommended closes.

    Args:
        ctx: HiveContext

    Returns:
        Dict with rationalization summary.
    """
    if not ctx.rationalization_mgr:
        return {"error": "Rationalization not initialized"}

    try:
        return ctx.rationalization_mgr.get_summary()

    except Exception as e:
        return {"error": f"Failed to get rationalization summary: {e}"}


def rationalization_status(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get channel rationalization status.

    Shows overall health metrics and configuration thresholds.

    Args:
        ctx: HiveContext

    Returns:
        Dict with rationalization status.
    """
    if not ctx.rationalization_mgr:
        return {"error": "Rationalization not initialized"}

    try:
        return ctx.rationalization_mgr.get_status()

    except Exception as e:
        return {"error": f"Failed to get rationalization status: {e}"}


# =============================================================================
# YIELD OPTIMIZATION PHASE 5: STRATEGIC POSITIONING
# =============================================================================
# Position fleet on critical network paths:
# - RouteValueAnalyzer: High-value corridors with limited competition
# - FleetPositioningStrategy: Coordinated channel opens (max 2 per target)
# - PhysarumChannelManager: Flow-based channel lifecycle (strengthen/atrophy)

def valuable_corridors(
    ctx: HiveContext,
    min_score: float = 0.05
) -> Dict[str, Any]:
    """
    Get high-value routing corridors for strategic positioning.

    Corridors are scored by: Volume × Margin × (1/Competition)
    Higher scores indicate better positioning opportunities.

    Args:
        ctx: HiveContext
        min_score: Minimum value score to include (default: 0.05)

    Returns:
        Dict with valuable corridors sorted by score.
    """
    if not ctx.strategic_positioning_mgr:
        return {"error": "Strategic positioning not initialized"}

    try:
        corridors = ctx.strategic_positioning_mgr.get_valuable_corridors(
            min_score=min_score
        )

        # Categorize by value tier
        by_tier = {"high": [], "medium": [], "low": []}
        for c in corridors:
            tier = c.get("value_tier", "low")
            if tier in by_tier:
                by_tier[tier].append(c)

        return {
            "corridors": corridors,
            "total_count": len(corridors),
            "by_value_tier": {
                tier: len(items) for tier, items in by_tier.items()
            },
            "min_score_filter": min_score
        }

    except Exception as e:
        return {"error": f"Failed to get valuable corridors: {e}"}


def exchange_coverage(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get priority exchange connectivity status.

    Shows which major Lightning exchanges the fleet is connected to
    and which still need channels.

    Args:
        ctx: HiveContext

    Returns:
        Dict with exchange coverage analysis.
    """
    if not ctx.strategic_positioning_mgr:
        return {"error": "Strategic positioning not initialized"}

    try:
        return ctx.strategic_positioning_mgr.get_exchange_coverage()

    except Exception as e:
        return {"error": f"Failed to get exchange coverage: {e}"}


def positioning_recommendations(
    ctx: HiveContext,
    count: int = 5
) -> Dict[str, Any]:
    """
    Get channel open recommendations for strategic positioning.

    Recommends where to open channels for maximum routing value,
    considering existing fleet coverage and competition.

    Args:
        ctx: HiveContext
        count: Number of recommendations to return (default: 5)

    Returns:
        Dict with positioning recommendations sorted by priority.
    """
    if not ctx.strategic_positioning_mgr:
        return {"error": "Strategic positioning not initialized"}

    try:
        recommendations = ctx.strategic_positioning_mgr.get_positioning_recommendations(
            count=count
        )

        # Summarize by priority tier
        by_tier = {"critical": 0, "high": 0, "medium": 0, "low": 0}
        for rec in recommendations:
            tier = rec.get("priority_tier", "low")
            if tier in by_tier:
                by_tier[tier] += 1

        return {
            "recommendations": recommendations,
            "count": len(recommendations),
            "by_priority": by_tier
        }

    except Exception as e:
        return {"error": f"Failed to get positioning recommendations: {e}"}


def flow_recommendations(
    ctx: HiveContext,
    channel_id: str = None
) -> Dict[str, Any]:
    """
    Get Physarum-inspired flow recommendations for channel lifecycle.

    Channels evolve based on flow like slime mold tubes:
    - High flow (>2% daily) → strengthen (splice in)
    - Low flow (<0.1% daily) → atrophy (recommend close)
    - Young + low flow → stimulate (fee reduction)

    Args:
        ctx: HiveContext
        channel_id: Specific channel, or None for all non-hold recommendations

    Returns:
        Dict with flow recommendations.
    """
    if not ctx.strategic_positioning_mgr:
        return {"error": "Strategic positioning not initialized"}

    try:
        recommendations = ctx.strategic_positioning_mgr.get_flow_recommendations(
            channel_id=channel_id
        )

        # Summarize by action
        by_action = {
            "strengthen": 0,
            "stimulate": 0,
            "atrophy": 0,
            "hold": 0
        }
        total_redeploy = 0
        total_splice = 0

        for rec in recommendations:
            action = rec.get("action", "hold")
            if action in by_action:
                by_action[action] += 1
            total_redeploy += rec.get("capital_to_redeploy_sats", 0)
            total_splice += rec.get("splice_amount_sats", 0)

        return {
            "recommendations": recommendations,
            "count": len(recommendations),
            "by_action": by_action,
            "capital_to_redeploy_sats": total_redeploy,
            "recommended_splice_sats": total_splice
        }

    except Exception as e:
        return {"error": f"Failed to get flow recommendations: {e}"}


def report_flow_intensity(
    ctx: HiveContext,
    channel_id: str,
    peer_id: str,
    intensity: float
) -> Dict[str, Any]:
    """
    Report flow intensity for a channel to the Physarum model.

    Flow intensity = Daily volume / Capacity
    This updates the slime-mold model that drives channel lifecycle decisions.

    Args:
        ctx: HiveContext
        channel_id: Channel ID (SCID format)
        peer_id: Peer public key
        intensity: Observed flow intensity (0.0 to 1.0+)

    Returns:
        Dict with acknowledgment.
    """
    if not ctx.strategic_positioning_mgr:
        return {"error": "Strategic positioning not initialized"}

    try:
        return ctx.strategic_positioning_mgr.report_flow_intensity(
            channel_id=channel_id,
            peer_id=peer_id,
            intensity=intensity
        )

    except Exception as e:
        return {"error": f"Failed to report flow intensity: {e}"}


def positioning_summary(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get summary of strategic positioning analysis.

    Shows high-value corridors, exchange coverage, and recommended actions.

    Args:
        ctx: HiveContext

    Returns:
        Dict with positioning summary.
    """
    if not ctx.strategic_positioning_mgr:
        return {"error": "Strategic positioning not initialized"}

    try:
        return ctx.strategic_positioning_mgr.get_positioning_summary()

    except Exception as e:
        return {"error": f"Failed to get positioning summary: {e}"}


def positioning_status(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get strategic positioning status.

    Shows overall status, thresholds, and priority exchanges.

    Args:
        ctx: HiveContext

    Returns:
        Dict with positioning status.
    """
    if not ctx.strategic_positioning_mgr:
        return {"error": "Strategic positioning not initialized"}

    try:
        return ctx.strategic_positioning_mgr.get_status()

    except Exception as e:
        return {"error": f"Failed to get positioning status: {e}"}
