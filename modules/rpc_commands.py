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
    log: Callable[[str, str], None] = None  # Logger function: (msg, level) -> None


def check_permission(ctx: HiveContext, required_tier: str) -> Optional[Dict[str, Any]]:
    """
    Check if the local node has the required tier for an RPC command.

    Args:
        ctx: HiveContext with database and our_pubkey
        required_tier: 'admin' or 'member'

    Returns:
        None if permission granted, or error dict if denied
    """
    if not ctx.our_pubkey or not ctx.database:
        return {"error": "Not initialized"}

    member = ctx.database.get_member(ctx.our_pubkey)
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

    Permission: Admin only
    """
    # Permission check: Admin only
    perm_error = check_permission(ctx, 'admin')
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

    Permission: Admin only
    """
    # Permission check: Admin only
    perm_error = check_permission(ctx, 'admin')
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
    admin_count = len([m for m in members if m['tier'] == 'admin'])

    return {
        "status": "active" if members else "genesis_required",
        "governance_mode": ctx.config.governance_mode if ctx.config else "unknown",
        "members": {
            "total": len(members),
            "admin": admin_count,
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
            "autonomous_budget_per_day": ctx.config.autonomous_budget_per_day,
            "autonomous_actions_per_hour": ctx.config.autonomous_actions_per_hour,
            "oracle_url": ctx.config.oracle_url,
            "oracle_timeout_seconds": ctx.config.oracle_timeout_seconds,
        },
        "membership": {
            "membership_enabled": ctx.config.membership_enabled,
            "auto_vouch_enabled": ctx.config.auto_vouch_enabled,
            "auto_promote_enabled": ctx.config.auto_promote_enabled,
            "ban_autotrigger_enabled": ctx.config.ban_autotrigger_enabled,
            "neophyte_fee_discount_pct": ctx.config.neophyte_fee_discount_pct,
            "member_fee_ppm": ctx.config.member_fee_ppm,
            "probation_days": ctx.config.probation_days,
            "vouch_threshold_pct": ctx.config.vouch_threshold_pct,
            "min_vouch_count": ctx.config.min_vouch_count,
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

    Permission: Member or Admin only
    """
    # Permission check: Member or Admin
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
    Get budget usage summary for autonomous mode.

    Args:
        ctx: HiveContext
        days: Number of days of history to include (default: 7)

    Returns:
        Dict with budget utilization and spending history.

    Permission: Member or Admin only
    """
    # Permission check: Member or Admin
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.database:
        return {"error": "Database not initialized"}

    cfg = ctx.config.snapshot() if ctx.config else None
    if not cfg:
        return {"error": "Config not initialized"}

    daily_budget = cfg.autonomous_budget_per_day
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

    Permission: Member or Admin only
    """
    # Permission check: Member or Admin
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
    proposed_size = (
        context.get('channel_size_sats') or
        context.get('amount_sats') or
        payload.get('amount_sats') or
        payload.get('channel_size_sats') or
        1_000_000  # Default 1M sats
    )

    # Apply member override if provided
    if amount_sats is not None:
        channel_size_sats = amount_sats
        override_applied = True
    else:
        channel_size_sats = proposed_size
        override_applied = False

    if not target:
        return {"error": "Missing target in action payload", "action_id": action_id}

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
        daily_remaining = ctx.database.get_available_budget(cfg.autonomous_budget_per_day)

        # 2. Onchain reserve limit (keep reserve_pct for future expansion)
        spendable_onchain = int(onchain_sats * (1.0 - cfg.budget_reserve_pct))

        # 3. Max per-channel limit (percentage of daily budget)
        max_per_channel = int(cfg.autonomous_budget_per_day * cfg.budget_max_per_channel_pct)

        # Effective budget is the minimum of all constraints
        effective_budget = min(daily_remaining, spendable_onchain, max_per_channel)

        budget_info = {
            "onchain_sats": onchain_sats,
            "reserve_pct": cfg.budget_reserve_pct,
            "spendable_onchain": spendable_onchain,
            "daily_budget": cfg.autonomous_budget_per_day,
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

        return {
            "status": "failed",
            "action_id": action_id,
            "action_type": action_type,
            "target": target,
            "channel_size_sats": channel_size_sats,
            "error": error_msg,
            "broadcast_count": broadcast_count,
        }


# =============================================================================
# GOVERNANCE COMMANDS
# =============================================================================

def set_mode(ctx: HiveContext, mode: str) -> Dict[str, Any]:
    """
    Change the governance mode at runtime.

    Args:
        ctx: HiveContext
        mode: New governance mode ('advisor', 'autonomous', or 'oracle')

    Returns:
        Dict with new mode and previous mode.

    Permission: Admin only
    """
    from modules.config import VALID_GOVERNANCE_MODES

    # Permission check: Admin only
    perm_error = check_permission(ctx, 'admin')
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

    # Check for oracle URL if switching to oracle mode
    if mode_lower == 'oracle' and not ctx.config.oracle_url:
        return {
            "error": "Cannot switch to oracle mode: oracle_url not configured",
            "hint": "Set hive-oracle-url option or configure oracle_url"
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

    Permission: Admin only
    """
    # Permission check: Admin only
    perm_error = check_permission(ctx, 'admin')
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


def pending_admin_promotions(ctx: HiveContext) -> Dict[str, Any]:
    """
    View pending admin promotion proposals.

    Returns:
        Dict with pending admin promotions and their approval status.

    Permission: Admin only
    """
    from modules.membership import MembershipTier

    perm_error = check_permission(ctx, 'admin')
    if perm_error:
        return perm_error

    if not ctx.database:
        return {"error": "Database not initialized"}

    # Get all current admins
    all_members = ctx.database.get_all_members()
    admins = [m for m in all_members if m.get("tier") == MembershipTier.ADMIN.value]
    admin_pubkeys = set(m["peer_id"] for m in admins)

    pending = ctx.database.get_pending_admin_promotions()
    result = []

    for p in pending:
        target = p["target_peer_id"]
        approvals = ctx.database.get_admin_promotion_approvals(target)
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
                    if m.get("tier") in (MembershipTier.MEMBER.value, MembershipTier.ADMIN.value)
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

    Permission: Admin only
    """
    perm_error = check_permission(ctx, 'admin')
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
