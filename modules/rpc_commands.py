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
from typing import Any, Callable, Dict, List, Optional


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
    anticipatory_manager: Any = None  # AnticipatoryLiquidityManager (Phase 7.1 - Anticipatory Liquidity)
    our_id: str = ""  # Our node pubkey (alias for our_pubkey for consistency)
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

    # Validate pubkey format (66 hex chars for compressed secp256k1 key)
    import re
    if not re.match(r'^[0-9a-fA-F]{66}$', pubkey):
        return {"error": "Invalid pubkey format: expected 66 hex characters"}

    # Parse address
    if ':' in vpn_address:
        ip, port_str = vpn_address.rsplit(':', 1)
        try:
            port = int(port_str)
        except (ValueError, TypeError):
            return {"error": "Invalid port number"}
        if not (1 <= port <= 65535):
            return {"error": f"Port {port} out of valid range (1-65535)"}
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
            uptime_raw = our_member.get("uptime_pct", 0.0)
            # Normalize to 0-100 scale (DB stores 0.0-1.0)
            if uptime_raw <= 1.0:
                uptime_raw = round(uptime_raw * 100, 2)
            contribution_ratio = our_member.get("contribution_ratio", 0.0)
            # Enrich with live contribution ratio if available (Issue #59)
            if ctx.membership_mgr:
                contribution_ratio = ctx.membership_mgr.calculate_contribution_ratio(ctx.our_pubkey)
            our_membership = {
                "tier": our_member.get("tier"),
                "joined_at": our_member.get("joined_at"),
                "pubkey": ctx.our_pubkey,
                "uptime_pct": uptime_raw,
                "contribution_ratio": contribution_ratio,
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
        "version": "2.2.6",
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

    # Enrich with live contribution ratio from ledger (Issue #59)
    if ctx.membership_mgr:
        for m in all_members:
            peer_id = m.get("peer_id")
            if peer_id:
                m["contribution_ratio"] = ctx.membership_mgr.calculate_contribution_ratio(peer_id)
                # Format uptime as percentage (stored as 0.0-1.0 decimal)
                m["uptime_pct"] = round(m.get("uptime_pct", 0.0) * 100, 2)

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


def reject_action(ctx: HiveContext, action_id, reason=None) -> Dict[str, Any]:
    """
    Reject pending action(s).

    Args:
        ctx: HiveContext
        action_id: ID of the action to reject, or "all" to reject all pending actions
        reason: Optional reason for rejection (stored for learning)

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
        return _reject_all_actions(ctx, reason=reason)

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
    payload = action.get('payload', {})
    intent_id = payload.get('intent_id')
    if intent_id:
        ctx.database.update_intent_status(intent_id, 'aborted', reason="action_rejected")

    # Update action status with optional reason
    ctx.database.update_action_status(action_id, 'rejected', reason=reason)

    if ctx.log:
        reason_str = f" (reason: {reason})" if reason else ""
        ctx.log(f"cl-hive: Rejected action {action_id}{reason_str}", 'info')

    result = {
        "status": "rejected",
        "action_id": action_id,
        "action_type": action['action_type'],
    }
    if reason:
        result["reason"] = reason
    return result


MAX_BULK_ACTIONS = 100  # CLAUDE.md: "Bound everything"


def _reject_all_actions(ctx: HiveContext, reason=None) -> Dict[str, Any]:
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
                ctx.database.update_intent_status(intent_id, 'aborted', reason="action_rejected")

            # Update action status with optional reason
            ctx.database.update_action_status(action_id, 'rejected', reason=reason)
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
        days: Number of days of history to include (default: 7, max: 365)

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

    # Bound days parameter (CLAUDE.md: "Bound everything")
    try:
        days = min(max(int(days), 1), 365)
    except (ValueError, TypeError):
        days = 7

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
        if amount_sats is not None:
            return {"error": "amount_sats override not supported with 'all' — approve individually to set custom amounts"}
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
            errors.append({"action_id": action_id, "error": str(e) or f"{type(e).__name__}"})

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

    # Re-check feerate gate at approval time (feerates may have changed since proposal)
    cfg = ctx.config.snapshot() if ctx.config else None
    if cfg and ctx.safe_plugin:
        max_feerate = getattr(cfg, 'max_expansion_feerate_perkb', 5000)
        if max_feerate != 0:
            try:
                feerates = ctx.safe_plugin.rpc.feerates("perkb")
                opening_feerate = feerates.get("perkb", {}).get("opening")
                if opening_feerate is None:
                    opening_feerate = feerates.get("perkb", {}).get("min_acceptable", 0)
                if opening_feerate > 0 and opening_feerate > max_feerate:
                    ctx.database.update_action_status(action_id, 'failed')
                    return {
                        "error": "Feerate gate: on-chain fees too high for channel open",
                        "action_id": action_id,
                        "opening_feerate_perkb": opening_feerate,
                        "max_feerate_perkb": max_feerate,
                        "hint": "Wait for feerates to drop or increase hive-max-expansion-feerate"
                    }
            except Exception as e:
                if ctx.log:
                    ctx.log(f"cl-hive: Could not check feerates: {e}", 'debug')

    # Calculate intelligent budget limits
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
                except Exception as send_err:
                    if ctx.log:
                        ctx.log(f"cl-hive: Intent send to {member_id[:16]}... failed: {send_err}", 'debug')

            if ctx.log:
                ctx.log(f"cl-hive: Broadcast intent to {broadcast_count} hive members", 'info')

        except Exception as e:
            if ctx.log:
                ctx.log(f"cl-hive: Intent broadcast failed: {e}", 'warn')

    # Step 2: Connect to target if not already connected
    try:
        # Check if already connected
        peerchannels = ctx.safe_plugin.rpc.listpeerchannels(target)
        if not peerchannels.get('channels'):
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
            ctx.database.update_intent_status(intent_id, 'committed', reason="action_executed")

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
        error_msg = str(e) or f"{type(e).__name__} during channel open"
        if ctx.log:
            ctx.log(f"cl-hive: fundchannel failed: {error_msg}", 'error')

        # Update action status to failed
        try:
            ctx.database.update_action_status(action_id, 'failed')
        except Exception as db_err:
            if ctx.log:
                ctx.log(f"cl-hive: Failed to update action status: {db_err}", 'error')

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

    Uses the Task Delegation Protocol (Phase 10) to ask another hive
    member to open the channel on our behalf when we can't connect
    to the target peer.

    Returns:
        Dict with delegation status
    """
    if not ctx.database or not ctx.safe_plugin:
        return None

    # Import task manager from main module
    try:
        from modules.task_manager import TaskManager

        # Get task_mgr from the global context
        # We need to access it through the plugin's globals
        import sys
        main_module = sys.modules.get('__main__')
        if not main_module:
            # Try cl-hive module
            main_module = sys.modules.get('cl-hive')

        task_mgr = getattr(main_module, 'task_mgr', None) if main_module else None

        if not task_mgr:
            if ctx.log:
                ctx.log("cl-hive: Task manager not available for delegation", 'debug')
            return {
                "status": "delegation_unavailable",
                "message": "Task manager not initialized"
            }

        # Prepare failure context
        failure_context = {
            "original_action_id": original_action_id,
            "failure_type": failure_info.get("type", "unknown"),
            "failure_reason": failure_info.get("reason", ""),
            "requester_pubkey": ctx.our_pubkey
        }

        # Request channel open delegation
        result = task_mgr.request_channel_open_delegation(
            target_peer=target,
            channel_size_sats=channel_size_sats,
            rpc=ctx.safe_plugin.rpc,
            failure_context=failure_context
        )

        if ctx.log:
            if result.get("status") == "delegation_requested":
                ctx.log(
                    f"cl-hive: Delegated channel open to {result.get('delegated_to', 'unknown')} "
                    f"(request_id={result.get('request_id', '')})",
                    'info'
                )
            else:
                ctx.log(
                    f"cl-hive: Delegation failed: {result.get('status', 'unknown')}",
                    'debug'
                )

        return result

    except Exception as e:
        if ctx.log:
            ctx.log(f"cl-hive: Delegation error: {e}", 'warn')
        return {
            "status": "delegation_error",
            "message": str(e)
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
            "target_tier": next((m.get("tier", "unknown") for m in all_members if m["peer_id"] == target_id), "unknown"),
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
            "hive_coverage": f"{rec.hive_members_count}/{len(ctx.planner._get_hive_members())} members ({rec.hive_coverage_pct:.0%})",
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
        uptime_raw = member.get("uptime_pct", 0.0)
        # Normalize to 0-100 scale (DB stores 0.0-1.0)
        if uptime_raw is not None and uptime_raw <= 1.0:
            uptime_raw = round(uptime_raw * 100, 2)
        result["uptime_pct"] = uptime_raw

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
        import datetime
        # Get period if not specified
        if period is None:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            year, week, _ = now.isocalendar()
            period = f"{year}-{week:02d}"

        # Sync uptime from presence data before snapshotting
        # This ensures uptime_pct in hive_members is current
        if ctx.database:
            ctx.database.sync_uptime_from_presence(window_seconds=30 * 86400)

        # snapshot_contributions returns List[MemberContribution]
        contributions = ctx.routing_pool.snapshot_contributions(period)

        # Convert to serializable format
        contrib_list = []
        for c in contributions:
            contrib_list.append({
                "member_id": c.member_id[:16] + "..." if c.member_id else "",
                "member_id_full": c.member_id,
                "capacity_sats": c.total_capacity_sats,
                "weighted_capacity_sats": c.weighted_capacity_sats,
                "uptime_pct": round(c.uptime_pct * 100, 1),
                "pool_share": round(c.pool_share * 100, 2),
            })

        return {
            "status": "ok",
            "period": period,
            "members_snapshotted": len(contributions),
            "contributions": contrib_list
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
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            year, week, _ = now.isocalendar()
            period = f"{year}-{week:02d}"

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
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            last_week = now - datetime.timedelta(days=7)
            year, week, _ = last_week.isocalendar()
            period = f"{year}-{week:02d}"

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
            results = ctx.routing_pool.settle_period(period)
            # settle_period() returns a list of PoolDistribution objects.
            # Convert to a stable JSON shape for RPC callers.
            revenue_info = ctx.routing_pool.db.get_pool_revenue(period=period)
            total_revenue = revenue_info.get("total_sats", 0)
            settled_at = int(time.time())

            distributions = [
                {
                    "member_id": r.member_id,
                    "amount_sats": r.revenue_share_sats,
                    "contribution_share": r.contribution_share,
                }
                for r in results
            ]
            return {
                "status": "settled",
                "period": period,
                "total_revenue_sats": total_revenue,
                "distributions": distributions,
                "settled_at": settled_at,
                "distribution_count": len(distributions),
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

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    # Input validation
    fee_ppm = int(fee_ppm)
    volume_sats = int(volume_sats)
    if fee_ppm < 0 or fee_ppm > 50000:
        return {"error": "fee_ppm must be between 0 and 50000"}
    if volume_sats < 0 or volume_sats > 10_000_000_000:  # 100 BTC
        return {"error": "volume_sats out of range"}

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
            above = level > 10.0
            return {
                "channel_id": channel_id,
                "pheromone_level": round(level, 2),
                "above_exploit_threshold": above,
                # Also return in list format for cl-revenue-ops compatibility
                "pheromone_levels": [{
                    "channel_id": channel_id,
                    "level": round(level, 2),
                    "above_threshold": above
                }]
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
            ],
            "pheromone_levels": [
                {
                    "channel_id": k,
                    "level": round(v, 2),
                    "above_threshold": v > 10.0
                }
                for k, v in sorted_levels[:50]
            ]
        }

    except Exception as e:
        return {"error": f"Failed to get pheromone levels: {e}"}


def get_routing_intelligence(ctx: HiveContext, scid: str = None) -> Dict[str, Any]:
    """
    Get routing intelligence for channel(s).

    Exports pheromone levels, trends, and corridor membership for use by
    external fee optimization systems (e.g., cl-revenue-ops Thompson sampling).

    Args:
        ctx: HiveContext
        scid: Optional specific channel short_channel_id. If None, returns all.

    Returns:
        Dict with routing intelligence:
        {
            "channels": {
                "932263x1883x0": {
                    "pheromone_level": 3.98,
                    "pheromone_trend": "stable",  # rising/falling/stable
                    "last_forward_age_hours": 2.5,
                    "marker_count": 3,
                    "on_active_corridor": true
                },
                ...
            },
            "timestamp": 1234567890
        }
    """
    import time

    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination not initialized"}

    try:
        adaptive = ctx.fee_coordination_mgr.adaptive_controller
        stigmergic = ctx.fee_coordination_mgr.stigmergic_coord

        # Get all pheromone levels
        all_levels = adaptive.get_all_pheromone_levels()

        # Get pheromone timestamps and fees
        with adaptive._lock:
            pheromone_timestamps = dict(adaptive._pheromone_last_update)
            pheromone_fees = dict(adaptive._pheromone_fee)
            channel_peer_map = dict(adaptive._channel_peer_map)

        # Get all active markers
        all_markers = stigmergic.get_all_markers()

        # Build a set of (source, dest) pairs that have active markers
        active_corridors = set()
        marker_counts = {}  # (source, dest) -> count
        for marker in all_markers:
            key = (marker.source_peer_id, marker.destination_peer_id)
            active_corridors.add(key)
            marker_counts[key] = marker_counts.get(key, 0) + 1

        now = time.time()

        def get_channel_intel(channel_id: str) -> Dict[str, Any]:
            """Build intelligence dict for a single channel."""
            level = all_levels.get(channel_id, 0.0)
            last_update = pheromone_timestamps.get(channel_id, 0)
            peer_id = channel_peer_map.get(channel_id)

            # Calculate last forward age in hours
            if last_update > 0:
                last_forward_age_hours = round((now - last_update) / 3600, 2)
            else:
                last_forward_age_hours = None

            # Determine pheromone trend
            # If we have a recent update (last 6 hours) and high pheromone, it's rising
            # If pheromone is decaying (old update), it's falling
            # Otherwise stable
            if last_update > 0:
                hours_since_update = (now - last_update) / 3600
                if hours_since_update < 6 and level > 1.0:
                    trend = "rising"
                elif hours_since_update > 24 and level > 0.1:
                    trend = "falling"
                else:
                    trend = "stable"
            else:
                trend = "stable"

            # Check if this channel is on an active corridor
            on_active_corridor = False
            channel_marker_count = 0

            if peer_id:
                # Check all corridors involving this peer
                for (src, dst), count in marker_counts.items():
                    if src == peer_id or dst == peer_id:
                        on_active_corridor = True
                        channel_marker_count += count

            return {
                "pheromone_level": round(level, 2),
                "pheromone_trend": trend,
                "last_forward_age_hours": last_forward_age_hours,
                "marker_count": channel_marker_count,
                "on_active_corridor": on_active_corridor
            }

        # Build result
        if scid:
            # Single channel requested
            if scid not in all_levels and scid not in channel_peer_map:
                return {
                    "channels": {
                        scid: {
                            "pheromone_level": 0.0,
                            "pheromone_trend": "stable",
                            "last_forward_age_hours": None,
                            "marker_count": 0,
                            "on_active_corridor": False
                        }
                    },
                    "timestamp": int(now)
                }
            return {
                "channels": {scid: get_channel_intel(scid)},
                "timestamp": int(now)
            }

        # All channels
        channels = {}
        # Include all channels with pheromone levels
        for channel_id in all_levels.keys():
            channels[channel_id] = get_channel_intel(channel_id)

        # Also include channels that have peer mappings but no pheromone yet
        for channel_id in channel_peer_map.keys():
            if channel_id not in channels:
                channels[channel_id] = get_channel_intel(channel_id)

        return {
            "channels": channels,
            "timestamp": int(now),
            "total_channels": len(channels),
            "channels_with_pheromone": len(all_levels),
            "active_corridors": len(active_corridors)
        }

    except Exception as e:
        return {"error": f"Failed to get routing intelligence: {e}"}


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
    via_fleet: bool = False,
    failure_reason: str = ""
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
        failure_reason: Error description if failed

    Returns:
        Dict with recording result and any circular flow warnings.
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction not initialized"}

    try:
        result = ctx.cost_reduction_mgr.record_rebalance_outcome(
            from_channel=from_channel,
            to_channel=to_channel,
            amount_sats=amount_sats,
            cost_sats=cost_sats,
            success=success,
            via_fleet=via_fleet
        )
        if failure_reason and not success:
            result["failure_reason"] = failure_reason
        return result

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


def execute_hive_circular_rebalance(
    ctx: HiveContext,
    from_channel: str,
    to_channel: str,
    amount_sats: int,
    via_members: list = None,
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    Execute a circular rebalance through the hive using explicit sendpay route.

    This bypasses sling's automatic route finding and uses an explicit route
    through hive members, ensuring zero-fee internal routing.

    Args:
        ctx: HiveContext
        from_channel: Source channel SCID (where we have outbound liquidity)
        to_channel: Destination channel SCID (where we want more local balance)
        amount_sats: Amount to rebalance in satoshis
        via_members: Optional list of intermediate member pubkeys
        dry_run: If True, just show the route without executing (default: True)

    Returns:
        Dict with route details and execution result (or preview if dry_run)
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction not initialized"}

    # Permission check: fund movements require member tier
    if not dry_run:
        perm_err = check_permission(ctx, "member")
        if perm_err:
            return perm_err

    try:
        return ctx.cost_reduction_mgr.execute_hive_circular_rebalance(
            from_channel=from_channel,
            to_channel=to_channel,
            amount_sats=amount_sats,
            via_members=via_members,
            dry_run=dry_run,
            bridge=ctx.bridge
        )

    except Exception as e:
        return {"error": f"Failed to execute hive circular rebalance: {e}"}


# =============================================================================
# MCF (MIN-COST MAX-FLOW) PATH COMMAND
# =============================================================================

def mcf_optimized_path(
    ctx: HiveContext,
    from_channel: str,
    to_channel: str,
    amount_sats: int
) -> Dict[str, Any]:
    """
    Get MCF-optimized rebalance path between channels.

    Uses the latest MCF solution if available and valid,
    otherwise falls back to BFS-based fleet routing.

    Args:
        ctx: HiveContext
        from_channel: Source channel SCID
        to_channel: Destination channel SCID
        amount_sats: Amount to rebalance

    Returns:
        Dict with path recommendation including:
        - source: "mcf" or "bfs" indicating which algorithm found the path
        - fleet_path_available: Whether a fleet path exists
        - fleet_path: List of pubkeys in the path
        - estimated_fleet_cost_sats: Expected cost
        - recommendation: Recommended action
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction not initialized"}

    try:
        return ctx.cost_reduction_mgr.get_mcf_optimized_path(
            from_channel=from_channel,
            to_channel=to_channel,
            amount_sats=amount_sats
        )

    except Exception as e:
        return {"error": f"Failed to get MCF optimized path: {e}"}


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

    Permission: Member or higher (prevents neophytes from creating close proposals).

    Args:
        ctx: HiveContext

    Returns:
        Dict with number of actions created.
    """
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

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

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.strategic_positioning_mgr:
        return {"error": "Strategic positioning not initialized"}

    # Input validation
    intensity = float(intensity)
    if intensity < 0.0 or intensity > 100.0:
        return {"error": "intensity must be between 0.0 and 100.0"}

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


# =============================================================================
# NETWORK METRICS COMMANDS
# =============================================================================

def network_metrics(ctx: HiveContext, member_id: str = None) -> Dict[str, Any]:
    """
    Get network position metrics for hive members.

    These metrics include centrality, unique peers, bridge score, hive centrality,
    and rebalance hub scores. Used for fair share calculations and routing optimization.

    Args:
        ctx: HiveContext
        member_id: Specific member pubkey (omit for all members)

    Returns:
        Dict with network metrics for the specified member(s).
    """
    from . import network_metrics as nm

    calculator = nm.get_calculator()
    if not calculator:
        return {"error": "Network metrics calculator not initialized"}

    try:
        if member_id:
            metrics = calculator.get_member_metrics(member_id)
            if not metrics:
                return {"error": f"No metrics available for member {member_id[:16]}..."}
            return {"metrics": metrics.to_dict()}
        else:
            all_metrics = calculator.get_all_metrics()
            members = [m.to_dict() for m in all_metrics.values()]
            # Sort by rebalance hub score for consistency
            members.sort(key=lambda x: x.get("rebalance_hub_score", 0), reverse=True)
            return {
                "member_count": len(members),
                "members": members
            }

    except Exception as e:
        return {"error": f"Failed to get network metrics: {e}"}


def rebalance_hubs(
    ctx: HiveContext,
    top_n: int = 3,
    exclude_members: List[str] = None
) -> Dict[str, Any]:
    """
    Get the best zero-fee rebalance intermediaries in the hive.

    Nodes with high hive centrality make good rebalance hubs because they
    have channels to many other hive members. Routing rebalances through
    these nodes is free (0 ppm fees within hive).

    Args:
        ctx: HiveContext
        top_n: Number of top hubs to return (default: 3)
        exclude_members: Member IDs to exclude (e.g., source/dest of rebalance)

    Returns:
        Dict with ranked list of best rebalance hubs.
    """
    from . import network_metrics as nm

    calculator = nm.get_calculator()
    if not calculator:
        return {"error": "Network metrics calculator not initialized"}

    try:
        hubs = calculator.get_rebalance_hubs(top_n=top_n, exclude_members=exclude_members)
        hub_list = []
        for hub in hubs:
            hub_dict = hub.to_dict()
            # Get alias if available from state manager
            if getattr(ctx, 'state_manager', None):
                state = ctx.state_manager.get_peer_state(hub.member_id)
                if state and hasattr(state, 'alias') and state.alias:
                    hub_dict['alias'] = state.alias
            hub_list.append(hub_dict)

        return {
            "count": len(hub_list),
            "hubs": hub_list
        }

    except Exception as e:
        return {"error": f"Failed to get rebalance hubs: {e}"}


def rebalance_path(
    ctx: HiveContext,
    source_member: str,
    dest_member: str,
    max_hops: int = 2
) -> Dict[str, Any]:
    """
    Find the optimal zero-fee path for internal hive rebalancing.

    Finds a path through the hive's internal network from source to destination.
    All channels between hive members have 0 ppm fees, so internal rebalancing
    through these paths is free.

    Args:
        ctx: HiveContext
        source_member: Source member pubkey
        dest_member: Destination member pubkey
        max_hops: Maximum number of hops (default: 2)

    Returns:
        Dict with path information including intermediaries.
    """
    from . import network_metrics as nm

    calculator = nm.get_calculator()
    if not calculator:
        return {"error": "Network metrics calculator not initialized"}

    try:
        path = calculator.find_best_rebalance_path(
            source_member=source_member,
            dest_member=dest_member,
            max_hops=max_hops
        )

        if not path:
            return {
                "path_found": False,
                "path": [],
                "hop_count": 0,
                "note": f"No path within {max_hops} hops between these members"
            }

        # Enrich path with aliases
        enriched_path = []
        for peer_id in path:
            node_info = {"peer_id": peer_id}
            if getattr(ctx, 'state_manager', None):
                state = ctx.state_manager.get_peer_state(peer_id)
                if state and hasattr(state, 'alias') and state.alias:
                    node_info['alias'] = state.alias
            enriched_path.append(node_info)

        return {
            "path_found": True,
            "path": enriched_path,
            "hop_count": len(path) - 1,
            "source": enriched_path[0] if enriched_path else None,
            "destination": enriched_path[-1] if enriched_path else None,
            "intermediaries": enriched_path[1:-1] if len(enriched_path) > 2 else []
        }

    except Exception as e:
        return {"error": f"Failed to find rebalance path: {e}"}


def fleet_health(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get overall fleet connectivity health metrics.

    Returns aggregated metrics showing how well-connected the fleet is
    internally. Includes health score (0-100) and letter grade.

    Args:
        ctx: HiveContext

    Returns:
        Dict with fleet health metrics.
    """
    from . import network_metrics as nm

    calculator = nm.get_calculator()
    if not calculator:
        return {"error": "Network metrics calculator not initialized"}

    try:
        return calculator.get_fleet_health()

    except Exception as e:
        return {"error": f"Failed to get fleet health: {e}"}


def connectivity_alerts(ctx: HiveContext) -> Dict[str, Any]:
    """
    Check for fleet connectivity issues that need attention.

    Returns alerts for isolated members, disconnected members,
    low hub availability, and other connectivity problems.

    Args:
        ctx: HiveContext

    Returns:
        Dict with list of alerts sorted by severity.
    """
    from . import network_metrics as nm

    calculator = nm.get_calculator()
    if not calculator:
        return {"error": "Network metrics calculator not initialized"}

    try:
        alerts = calculator.check_connectivity_alerts()
        critical = sum(1 for a in alerts if a.get("severity") == "critical")
        warnings = sum(1 for a in alerts if a.get("severity") == "warning")
        info = sum(1 for a in alerts if a.get("severity") == "info")

        return {
            "alert_count": len(alerts),
            "critical_count": critical,
            "warning_count": warnings,
            "info_count": info,
            "alerts": alerts
        }

    except Exception as e:
        return {"error": f"Failed to check connectivity: {e}"}


def member_connectivity(ctx: HiveContext, member_id: str) -> Dict[str, Any]:
    """
    Get detailed connectivity report for a specific member.

    Shows how well-connected this member is within the fleet,
    comparison to fleet average, and recommendations for improvement.

    Args:
        ctx: HiveContext
        member_id: Member's public key

    Returns:
        Dict with connectivity details and recommendations.
    """
    from . import network_metrics as nm

    calculator = nm.get_calculator()
    if not calculator:
        return {"error": "Network metrics calculator not initialized"}

    try:
        return calculator.get_member_connectivity_report(member_id)

    except Exception as e:
        return {"error": f"Failed to get member connectivity: {e}"}


def neophyte_rankings(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get all neophytes ranked by their promotion readiness.

    Returns neophytes sorted by a readiness score (0-100) based on:
    - Probation progress (40%)
    - Uptime (20%)
    - Contribution ratio (20%)
    - Hive centrality (20%)

    Neophytes with high hive centrality may be eligible for fast-track
    promotion (after 30 days instead of 90 if centrality >= 0.5).

    Args:
        ctx: HiveContext

    Returns:
        Dict with ranked list of neophytes and their metrics.
    """
    if not ctx.membership_mgr:
        return {"error": "Membership manager not initialized"}

    try:
        rankings = ctx.membership_mgr.get_neophyte_rankings()
        eligible_count = sum(1 for n in rankings if n.get("eligible"))
        fast_track_count = sum(1 for n in rankings if n.get("fast_track_eligible"))

        return {
            "neophyte_count": len(rankings),
            "eligible_for_promotion": eligible_count,
            "fast_track_eligible": fast_track_count,
            "rankings": rankings
        }

    except Exception as e:
        return {"error": f"Failed to get neophyte rankings: {e}"}


# =============================================================================
# MCF (Min-Cost Max-Flow) COMMANDS
# =============================================================================

def mcf_status(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get MCF optimization status.

    Shows coordinator election, pending assignments, completion stats,
    and latest solution information.

    Args:
        ctx: HiveContext

    Returns:
        Dict with MCF status including:
        - enabled: Whether MCF is enabled
        - is_coordinator: Whether we are the elected coordinator
        - coordinator_id: Elected coordinator's pubkey
        - pending_assignments: Our pending MCF assignments
        - solution_info: Latest solution stats
        - ack_stats: Assignment acknowledgment stats
        - completion_stats: Completion report stats
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction manager not initialized"}

    try:
        # Get basic MCF status
        mcf_coord_status = ctx.cost_reduction_mgr.get_mcf_status()

        # Get coordinator info
        coordinator_id = ctx.cost_reduction_mgr.get_current_mcf_coordinator()

        # Get our assignments from liquidity coordinator
        pending_assignments = []
        if ctx.liquidity_coordinator:
            liq_status = ctx.liquidity_coordinator.get_mcf_status()
            pending_assignments = liq_status.get("pending_assignments", [])

        # Get ACK and completion stats
        acks = ctx.cost_reduction_mgr.get_mcf_acks()
        completions = ctx.cost_reduction_mgr.get_mcf_completions()

        success_count = sum(1 for c in completions if c.get("success"))
        failure_count = sum(1 for c in completions if not c.get("success"))

        return {
            "enabled": mcf_coord_status.get("enabled", False),
            "is_coordinator": mcf_coord_status.get("is_coordinator", False),
            "coordinator_id": coordinator_id,
            "our_pubkey": ctx.our_pubkey,
            "pending_assignments": {
                "count": len(pending_assignments),
                "assignments": pending_assignments[:5],  # First 5
            },
            "solution_info": {
                "valid": mcf_coord_status.get("solution_valid", False),
                "last_timestamp": mcf_coord_status.get("last_solution_timestamp", 0),
                "total_flow_sats": mcf_coord_status.get("total_flow_sats", 0),
                "total_cost_sats": mcf_coord_status.get("total_cost_sats", 0),
                "assignment_count": mcf_coord_status.get("assignment_count", 0),
            },
            "ack_stats": {
                "count": len(acks),
                "recent": acks[-5:] if acks else [],  # Last 5
            },
            "completion_stats": {
                "total": len(completions),
                "success": success_count,
                "failure": failure_count,
                "recent": completions[-5:] if completions else [],  # Last 5
            },
        }

    except Exception as e:
        return {"error": f"Failed to get MCF status: {e}"}


def mcf_solve(ctx: HiveContext, dry_run: bool = True) -> Dict[str, Any]:
    """
    Trigger MCF optimization cycle manually.

    Computes optimal rebalance assignments for the fleet. Only works if
    this node is the elected coordinator.

    Args:
        ctx: HiveContext
        dry_run: If True, compute solution but don't broadcast (default: True)

    Returns:
        Dict with solution details including:
        - coordinator: Whether we are coordinator
        - solution: Optimization results (flow, cost, assignments)
        - broadcast: Whether solution was broadcast

    Permission: Member only
    """
    # Permission check: Member only
    perm_error = check_permission(ctx, 'member')
    if perm_error:
        return perm_error

    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction manager not initialized"}

    try:
        # Check if we're coordinator
        coordinator_id = ctx.cost_reduction_mgr.get_current_mcf_coordinator()
        is_coordinator = coordinator_id == ctx.our_pubkey

        if not is_coordinator:
            return {
                "error": "Not coordinator",
                "message": "Only the elected coordinator can run MCF optimization",
                "coordinator_id": coordinator_id,
                "our_pubkey": ctx.our_pubkey
            }

        # Run optimization
        solution = ctx.cost_reduction_mgr.run_mcf_optimization()

        if not solution:
            return {
                "success": False,
                "message": "No solution generated (may not have enough demand)",
                "is_coordinator": True
            }

        result = {
            "success": True,
            "is_coordinator": True,
            "dry_run": dry_run,
            "solution": {
                "total_flow_sats": solution.get("total_flow_sats", 0),
                "total_cost_sats": solution.get("total_cost_sats", 0),
                "unmet_demand_sats": solution.get("unmet_demand_sats", 0),
                "assignment_count": len(solution.get("assignments", [])),
                "computation_time_ms": solution.get("computation_time_ms", 0),
                "iterations": solution.get("iterations", 0),
            },
            "assignments": solution.get("assignments", [])[:10],  # First 10
        }

        if not dry_run:
            result["broadcast"] = False
            result["message"] = "Solution generated. Fleet broadcast not yet implemented — use assignments to execute manually."
        else:
            result["broadcast"] = False
            result["message"] = "Dry run - solution not broadcast (use dry_run=false to generate)"

        return result

    except Exception as e:
        return {"error": f"MCF optimization failed: {e}"}


def mcf_assignments(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get detailed view of our MCF assignments.

    Shows all pending, executing, completed, and failed assignments
    from the current and recent MCF solutions.

    Args:
        ctx: HiveContext

    Returns:
        Dict with assignment details by status.
    """
    if not ctx.liquidity_coordinator:
        return {"error": "Liquidity coordinator not initialized"}

    try:
        status = ctx.liquidity_coordinator.get_mcf_status()

        # Get all assignments by status
        all_assignments = []
        if hasattr(ctx.liquidity_coordinator, 'get_all_assignments'):
            all_assignments = ctx.liquidity_coordinator.get_all_assignments()

        pending = [a for a in all_assignments if a.status == "pending"]
        executing = [a for a in all_assignments if a.status == "executing"]
        completed = [a for a in all_assignments if a.status == "completed"]
        failed = [a for a in all_assignments if a.status in ("failed", "rejected")]

        def format_assignment(a):
            return {
                "assignment_id": a.assignment_id,
                "from_channel": a.from_channel,
                "to_channel": a.to_channel,
                "amount_sats": a.amount_sats,
                "expected_cost_sats": a.expected_cost_sats,
                "priority": a.priority,
                "status": a.status,
                "coordinator_id": a.coordinator_id[:16] + "..." if a.coordinator_id else "",
            }

        return {
            "last_solution_timestamp": status.get("last_solution_timestamp", 0),
            "ack_sent": status.get("ack_sent", False),
            "summary": {
                "pending": len(pending),
                "executing": len(executing),
                "completed": len(completed),
                "failed": len(failed),
            },
            "pending": [format_assignment(a) for a in pending],
            "executing": [format_assignment(a) for a in executing],
            "completed": [format_assignment(a) for a in completed[-10:]],  # Last 10
            "failed": [format_assignment(a) for a in failed[-10:]],  # Last 10
        }

    except Exception as e:
        return {"error": f"Failed to get MCF assignments: {e}"}


# =============================================================================
# REVENUE OPS INTEGRATION COMMANDS
# =============================================================================
# These RPC methods provide data to cl-revenue-ops for improved fee optimization
# and rebalancing decisions. They expose cl-hive's intelligence layer.


def get_defense_status(ctx: HiveContext, scid: str = None) -> Dict[str, Any]:
    """
    Get defense status for channel(s).

    Returns whether channels are under defensive fee protection due to
    drain attacks, spam, or fee wars. Used by cl-revenue-ops to avoid
    overriding defensive fees during optimization.

    Args:
        ctx: HiveContext
        scid: Optional specific channel SCID. If None, returns all channels.

    Returns:
        Dict with defense status for each channel:
        {
            "channels": {
                "932263x1883x0": {
                    "under_defense": false,
                    "defense_type": null,
                    "defensive_fee_ppm": null,
                    "defense_started_at": null,
                    "defense_reason": null
                }
            }
        }
    """
    if not ctx.fee_coordination_mgr:
        return {"error": "Fee coordination manager not initialized"}

    try:
        channels_data = {}

        # Get all channels with defense status
        if ctx.safe_plugin:
            channels = ctx.safe_plugin.rpc.listpeerchannels()

            for ch in channels.get('channels', []):
                ch_scid = ch.get('short_channel_id')
                if not ch_scid:
                    continue

                # Skip if specific scid requested and this isn't it
                if scid and ch_scid != scid:
                    continue

                peer_id = ch.get('peer_id', '')

                # Check defense status from fee coordination manager
                defense_info = ctx.fee_coordination_mgr.get_channel_defense_status(
                    ch_scid, peer_id
                ) if hasattr(ctx.fee_coordination_mgr, 'get_channel_defense_status') else {}

                # Also check active warnings
                active_warnings = ctx.fee_coordination_mgr.get_active_warnings_for_peer(
                    peer_id
                ) if hasattr(ctx.fee_coordination_mgr, 'get_active_warnings_for_peer') else []

                under_defense = defense_info.get('under_defense', False) or len(active_warnings) > 0
                defense_type = defense_info.get('defense_type')

                if not defense_type and active_warnings:
                    # Derive from warnings
                    for warn in active_warnings:
                        if warn.get('threat_type') == 'drain':
                            defense_type = 'drain_protection'
                            break
                        elif warn.get('threat_type') == 'unreliable':
                            defense_type = 'spam_defense'
                            break

                channels_data[ch_scid] = {
                    "under_defense": under_defense,
                    "defense_type": defense_type,
                    "defensive_fee_ppm": defense_info.get('defensive_fee_ppm'),
                    "defense_started_at": defense_info.get('defense_started_at'),
                    "defense_reason": defense_info.get('defense_reason'),
                    "active_warnings": len(active_warnings),
                }

        return {"channels": channels_data}

    except Exception as e:
        return {"error": f"Failed to get defense status: {e}"}


def get_peer_quality(ctx: HiveContext, peer_id: str = None) -> Dict[str, Any]:
    """
    Get peer quality assessments from the hive's collective intelligence.

    Returns quality ratings based on uptime, routing success, fee stability,
    and fleet-wide reputation. Used by cl-revenue-ops to adjust optimization
    intensity - don't invest heavily in bad peers.

    Args:
        ctx: HiveContext
        peer_id: Optional specific peer ID. If None, returns all peers.

    Returns:
        Dict with peer quality assessments:
        {
            "peers": {
                "03abc...": {
                    "quality": "good",
                    "quality_score": 0.85,
                    "reasons": ["high_uptime", "good_routing_partner"],
                    "recommendation": "expand",
                    "last_assessed": 1707600000
                }
            }
        }
    """
    if not ctx.quality_scorer:
        return {"error": "Quality scorer not initialized"}

    try:
        peers_data = {}

        # Get peers to assess
        peer_list = []
        if peer_id:
            peer_list = [peer_id]
        elif ctx.safe_plugin:
            # Get all connected peers
            channels = ctx.safe_plugin.rpc.listpeerchannels()
            peer_list = list(set(
                ch.get('peer_id') for ch in channels.get('channels', [])
                if ch.get('peer_id')
            ))

        for pid in peer_list:
            # Get quality score from quality_scorer
            score_result = ctx.quality_scorer.score_peer(pid)

            quality_score = score_result.quality_score if score_result else 0.5
            recommendation = score_result.quality_recommendation if score_result else "maintain"

            # Classify quality tier
            if quality_score >= 0.7:
                quality = "good"
            elif quality_score >= 0.4:
                quality = "neutral"
            else:
                quality = "avoid"

            # Build reasons list
            reasons = []
            if score_result:
                if hasattr(score_result, 'uptime_score') and score_result.uptime_score >= 0.9:
                    reasons.append("high_uptime")
                if hasattr(score_result, 'success_rate_score') and score_result.success_rate_score >= 0.8:
                    reasons.append("good_routing_partner")
                if hasattr(score_result, 'fee_stability_score') and score_result.fee_stability_score >= 0.8:
                    reasons.append("stable_fees")
                if hasattr(score_result, 'force_close_penalty') and score_result.force_close_penalty > 0:
                    reasons.append("force_close_history")
                if quality_score < 0.4:
                    reasons.append("low_quality_score")

            # Get last assessment time from peer reputation manager
            last_assessed = None
            if ctx.database:
                # Check for peer events
                events = ctx.database.get_peer_events(peer_id=pid, limit=1)
                if events:
                    last_assessed = events[0].get('timestamp')

            peers_data[pid] = {
                "quality": quality,
                "quality_score": round(quality_score, 3),
                "reasons": reasons,
                "recommendation": recommendation,
                "last_assessed": last_assessed or int(time.time()),
            }

        return {"peers": peers_data}

    except Exception as e:
        return {"error": f"Failed to get peer quality: {e}"}


def get_fee_change_outcomes(ctx: HiveContext, scid: str = None,
                             days: int = 30) -> Dict[str, Any]:
    """
    Get outcomes of past fee changes for learning.

    Returns historical fee changes with before/after metrics to help
    cl-revenue-ops learn from past decisions and adjust Thompson priors.

    Args:
        ctx: HiveContext
        scid: Optional specific channel SCID. If None, returns all.
        days: Number of days of history to return (default: 30, max: 90)

    Returns:
        Dict with fee change outcomes:
        {
            "changes": [
                {
                    "scid": "932263x1883x0",
                    "timestamp": 1707500000,
                    "old_fee_ppm": 200,
                    "new_fee_ppm": 300,
                    "source": "advisor",
                    "outcome": {
                        "forwards_before_24h": 5,
                        "forwards_after_24h": 3,
                        "revenue_before_24h": 500,
                        "revenue_after_24h": 600,
                        "verdict": "positive"
                    }
                }
            ]
        }
    """
    if not ctx.database:
        return {"error": "Database not initialized"}

    # Bound days parameter
    days = min(max(1, days), 90)

    try:
        changes = []
        cutoff_ts = int(time.time()) - (days * 86400)

        # Query fee change history from database
        # This data may come from multiple sources:
        # 1. fee_coordination_mgr stigmergic markers
        # 2. database recorded fee changes
        # 3. routing_map pheromone history

        if ctx.fee_coordination_mgr:
            # Get markers which track fee changes
            markers = ctx.fee_coordination_mgr.get_all_markers() \
                if hasattr(ctx.fee_coordination_mgr, 'get_all_markers') else []

            # Filter by scid if specified
            if scid:
                markers = [m for m in markers if m.get('channel_id') == scid]

            for marker in markers:
                if marker.get('timestamp', 0) < cutoff_ts:
                    continue

                # Get outcome data if available
                outcome_data = marker.get('outcome', {})

                change_entry = {
                    "scid": marker.get('channel_id', ''),
                    "timestamp": marker.get('timestamp', 0),
                    "old_fee_ppm": marker.get('old_fee_ppm', 0),
                    "new_fee_ppm": marker.get('fee_ppm', 0),
                    "source": marker.get('source', 'unknown'),
                    "outcome": {
                        "forwards_before_24h": outcome_data.get('forwards_before', 0),
                        "forwards_after_24h": outcome_data.get('forwards_after', 0),
                        "revenue_before_24h": outcome_data.get('revenue_before', 0),
                        "revenue_after_24h": outcome_data.get('revenue_after', 0),
                        "verdict": outcome_data.get('verdict', 'unknown'),
                    }
                }
                changes.append(change_entry)

        # Sort by timestamp descending
        changes.sort(key=lambda x: x['timestamp'], reverse=True)

        return {"changes": changes[:200]}  # Limit to 200 entries

    except Exception as e:
        return {"error": f"Failed to get fee change outcomes: {e}"}


def get_channel_flags(ctx: HiveContext, scid: str = None) -> Dict[str, Any]:
    """
    Get special flags for channels.

    Returns flags identifying hive-internal channels that should be excluded
    from optimization (always 0 fee) or have other special treatment.

    Args:
        ctx: HiveContext
        scid: Optional specific channel SCID. If None, returns all channels.

    Returns:
        Dict with channel flags:
        {
            "channels": {
                "932263x1883x0": {
                    "is_hive_internal": false,
                    "is_hive_member": false,
                    "fixed_fee": null,
                    "exclude_from_optimization": false
                }
            }
        }
    """
    if not ctx.database:
        return {"error": "Database not initialized"}

    try:
        channels_data = {}

        # Get all hive members
        members = ctx.database.get_all_members()
        member_ids = set(m.get('peer_id') for m in members if m.get('peer_id'))

        # Get all channels
        if ctx.safe_plugin:
            channels = ctx.safe_plugin.rpc.listpeerchannels()

            for ch in channels.get('channels', []):
                ch_scid = ch.get('short_channel_id')
                if not ch_scid:
                    continue

                # Skip if specific scid requested and this isn't it
                if scid and ch_scid != scid:
                    continue

                peer_id = ch.get('peer_id', '')
                is_hive_member = peer_id in member_ids

                # Check if this is a hive-internal channel (between hive members)
                # Both ends must be hive members
                is_hive_internal = is_hive_member  # Our end is hive, check peer

                # Hive internal channels should have 0 fee
                fixed_fee = 0 if is_hive_internal else None
                exclude_from_optimization = is_hive_internal

                channels_data[ch_scid] = {
                    "is_hive_internal": is_hive_internal,
                    "is_hive_member": is_hive_member,
                    "fixed_fee": fixed_fee,
                    "exclude_from_optimization": exclude_from_optimization,
                    "peer_id": peer_id[:16] + "..." if peer_id else None,
                }

        return {"channels": channels_data}

    except Exception as e:
        return {"error": f"Failed to get channel flags: {e}"}


def get_mcf_targets(ctx: HiveContext) -> Dict[str, Any]:
    """
    Get MCF-computed optimal balance targets.

    Returns the Multi-Commodity Flow computed optimal local balance
    percentages for each channel. Used by cl-revenue-ops to guide
    rebalancing toward globally optimal distribution.

    Args:
        ctx: HiveContext

    Returns:
        Dict with MCF targets:
        {
            "targets": {
                "932263x1883x0": {
                    "optimal_local_pct": 45,
                    "current_local_pct": 30,
                    "delta_sats": 150000,
                    "priority": "high"
                }
            },
            "computed_at": 1707600000
        }
    """
    if not ctx.cost_reduction_mgr:
        return {"error": "Cost reduction manager not initialized"}

    try:
        targets_data = {}
        computed_at = 0

        # Get current MCF solution if available
        if hasattr(ctx.cost_reduction_mgr, 'get_current_mcf_solution'):
            solution = ctx.cost_reduction_mgr.get_current_mcf_solution()
            if solution:
                computed_at = solution.get('timestamp', 0)

                # Extract target balances from assignments
                assignments = solution.get('assignments', [])
                channel_deltas: Dict[str, int] = {}

                for assignment in assignments:
                    to_channel = assignment.get('to_channel')
                    from_channel = assignment.get('from_channel')
                    amount = assignment.get('amount_sats', 0)

                    if to_channel:
                        channel_deltas[to_channel] = channel_deltas.get(to_channel, 0) + amount
                    if from_channel:
                        channel_deltas[from_channel] = channel_deltas.get(from_channel, 0) - amount

                # Get current channel balances
                if ctx.safe_plugin:
                    channels = ctx.safe_plugin.rpc.listpeerchannels()

                    for ch in channels.get('channels', []):
                        ch_scid = ch.get('short_channel_id')
                        if not ch_scid:
                            continue

                        local_msat = ch.get('to_us_msat', 0)
                        if isinstance(local_msat, str):
                            local_msat = int(local_msat.replace('msat', ''))
                        total_msat = ch.get('total_msat', 0)
                        if isinstance(total_msat, str):
                            total_msat = int(total_msat.replace('msat', ''))

                        if total_msat <= 0:
                            continue

                        current_local_pct = (local_msat / total_msat) * 100
                        delta_sats = channel_deltas.get(ch_scid, 0)

                        # Calculate optimal based on delta
                        optimal_local_sats = (local_msat // 1000) + delta_sats
                        optimal_local_pct = (optimal_local_sats * 1000 / total_msat) * 100
                        optimal_local_pct = max(0, min(100, optimal_local_pct))

                        # Determine priority
                        abs_delta = abs(delta_sats)
                        if abs_delta > 500000:
                            priority = "high"
                        elif abs_delta > 100000:
                            priority = "medium"
                        else:
                            priority = "low"

                        targets_data[ch_scid] = {
                            "optimal_local_pct": round(optimal_local_pct, 1),
                            "current_local_pct": round(current_local_pct, 1),
                            "delta_sats": delta_sats,
                            "priority": priority,
                        }

        return {
            "targets": targets_data,
            "computed_at": computed_at,
        }

    except Exception as e:
        return {"error": f"Failed to get MCF targets: {e}"}


def get_nnlb_opportunities(ctx: HiveContext, min_amount: int = 50000) -> Dict[str, Any]:
    """
    Get Nearest-Neighbor Load Balancing opportunities.

    Returns low-cost rebalance opportunities between fleet members where
    the rebalance can be done at zero or minimal fee through hive-internal
    channels.

    Args:
        ctx: HiveContext
        min_amount: Minimum amount in sats to consider (default: 50000)

    Returns:
        Dict with NNLB opportunities:
        {
            "opportunities": [
                {
                    "source_scid": "932263x1883x0",
                    "sink_scid": "931308x1256x0",
                    "amount_sats": 200000,
                    "estimated_cost_sats": 0,
                    "path_hops": 1,
                    "is_hive_internal": true
                }
            ]
        }
    """
    if not ctx.anticipatory_manager:
        # Fall back to liquidity coordinator
        if not ctx.liquidity_coordinator:
            return {"error": "Neither anticipatory manager nor liquidity coordinator initialized"}

    try:
        opportunities = []

        # Get NNLB recommendations from anticipatory manager
        if ctx.anticipatory_manager and hasattr(ctx.anticipatory_manager, 'get_nnlb_opportunities'):
            nnlb_opps = ctx.anticipatory_manager.get_nnlb_opportunities(min_amount)
            for opp in nnlb_opps:
                opportunities.append({
                    "source_scid": opp.get('source_channel'),
                    "sink_scid": opp.get('sink_channel'),
                    "amount_sats": opp.get('amount_sats', 0),
                    "estimated_cost_sats": opp.get('estimated_cost', 0),
                    "path_hops": opp.get('path_hops', 1),
                    "is_hive_internal": opp.get('is_hive_internal', False),
                })
        elif ctx.liquidity_coordinator:
            # Use liquidity coordinator's circular flow detection
            if hasattr(ctx.liquidity_coordinator, 'get_circular_rebalance_opportunities'):
                circ_opps = ctx.liquidity_coordinator.get_circular_rebalance_opportunities()
                for opp in circ_opps:
                    if opp.get('amount_sats', 0) >= min_amount:
                        opportunities.append({
                            "source_scid": opp.get('from_channel'),
                            "sink_scid": opp.get('to_channel'),
                            "amount_sats": opp.get('amount_sats', 0),
                            "estimated_cost_sats": opp.get('cost_sats', 0),
                            "path_hops": opp.get('hops', 1),
                            "is_hive_internal": opp.get('is_hive_internal', True),
                        })

        # Sort by amount descending
        opportunities.sort(key=lambda x: x['amount_sats'], reverse=True)

        return {"opportunities": opportunities[:20]}  # Limit to 20

    except Exception as e:
        return {"error": f"Failed to get NNLB opportunities: {e}"}


def get_channel_ages(ctx: HiveContext, scid: str = None) -> Dict[str, Any]:
    """
    Get channel age information.

    Returns age and maturity classification for channels. Used by
    cl-revenue-ops to adjust exploration vs exploitation in Thompson
    sampling - new channels need more exploration, mature channels
    should exploit known-good fees.

    Args:
        ctx: HiveContext
        scid: Optional specific channel SCID. If None, returns all channels.

    Returns:
        Dict with channel ages:
        {
            "channels": {
                "932263x1883x0": {
                    "age_days": 45,
                    "maturity": "mature",
                    "first_forward_days_ago": 40,
                    "total_forwards": 250
                }
            }
        }
    """
    if not ctx.safe_plugin:
        return {"error": "Plugin not initialized"}

    try:
        channels_data = {}
        now = int(time.time())

        # Get all channels
        channels = ctx.safe_plugin.rpc.listpeerchannels()

        for ch in channels.get('channels', []):
            ch_scid = ch.get('short_channel_id')
            if not ch_scid:
                continue

            # Skip if specific scid requested and this isn't it
            if scid and ch_scid != scid:
                continue

            # Calculate age from funding confirmation
            # SCID format: blockheight x txindex x output
            # We can derive approximate age from blockheight
            try:
                parts = ch_scid.split('x')
                if len(parts) >= 1:
                    funding_block = int(parts[0])

                    # Get current blockheight
                    info = ctx.safe_plugin.rpc.getinfo()
                    current_block = info.get('blockheight', funding_block)

                    blocks_old = current_block - funding_block
                    # Approximate 10 minutes per block
                    age_days = (blocks_old * 10) / (60 * 24)
                    age_days = max(0, age_days)
                else:
                    age_days = 0
            except (ValueError, TypeError):
                age_days = 0

            # Classify maturity
            if age_days < 14:
                maturity = "new"
            elif age_days < 60:
                maturity = "developing"
            else:
                maturity = "mature"

            # Get forward statistics if available from database
            first_forward_days_ago = None
            total_forwards = 0

            if ctx.database:
                # Check peer events for forward activity
                peer_id = ch.get('peer_id', '')
                if peer_id:
                    events = ctx.database.get_peer_events(
                        peer_id=peer_id,
                        event_type='forward',
                        limit=1000
                    )
                    if events:
                        total_forwards = len(events)
                        oldest_event = min(e.get('timestamp', now) for e in events)
                        first_forward_days_ago = (now - oldest_event) / 86400

            channels_data[ch_scid] = {
                "age_days": round(age_days, 1),
                "maturity": maturity,
                "first_forward_days_ago": round(first_forward_days_ago, 1) if first_forward_days_ago else None,
                "total_forwards": total_forwards,
            }

        return {"channels": channels_data}

    except Exception as e:
        return {"error": f"Failed to get channel ages: {e}"}
