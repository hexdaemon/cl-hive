#!/usr/bin/env python3
"""
AI Advisor for cl-hive Governance

This script monitors hive-pending-actions and uses Claude to make
intelligent decisions about channel expansions, bans, and other
governance actions.

Usage:
    python ai_advisor.py --poll           # Poll once and decide
    python ai_advisor.py --daemon         # Run continuously
    python ai_advisor.py --dry-run        # Show decisions without executing

Requirements:
    pip install anthropic
    export ANTHROPIC_API_KEY=your_key

Author: Lightning Goats Team
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False
    print("Warning: anthropic package not installed. Using mock decisions.")


# =============================================================================
# CONFIGURATION
# =============================================================================

# Default lightning-cli command (can be overridden via env)
LIGHTNING_CLI = os.environ.get('LIGHTNING_CLI', 'lightning-cli')

# Poll interval in seconds (for daemon mode)
POLL_INTERVAL = int(os.environ.get('AI_ADVISOR_POLL_INTERVAL', '300'))  # 5 minutes

# Model to use
CLAUDE_MODEL = os.environ.get('CLAUDE_MODEL', 'claude-sonnet-4-20250514')

# Decision thresholds
MIN_NODE_CHANNELS = 5  # Minimum channels for a node to be considered established
MAX_FEE_PPM = 1000     # Maximum acceptable fee rate
MIN_CAPACITY_SATS = 1_000_000  # Minimum target capacity (1M sats)


# =============================================================================
# LIGHTNING RPC HELPERS
# =============================================================================

def run_lightning_cli(*args) -> Optional[Dict]:
    """Run a lightning-cli command and return JSON result."""
    try:
        cmd = [LIGHTNING_CLI] + list(args)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            print(f"Error running {' '.join(cmd)}: {result.stderr}")
            return None
    except subprocess.TimeoutExpired:
        print(f"Timeout running lightning-cli {args[0]}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def get_pending_actions() -> List[Dict]:
    """Get pending governance actions from hive."""
    result = run_lightning_cli('hive-pending-actions')
    if result:
        return result.get('actions', [])
    return []


def get_node_info(node_id: str) -> Dict[str, Any]:
    """Get information about a node from the graph."""
    result = run_lightning_cli('listnodes', node_id)
    if result and result.get('nodes'):
        return result['nodes'][0]
    return {}


def get_node_channels(node_id: str) -> List[Dict]:
    """Get channels for a node from the graph."""
    result = run_lightning_cli('listchannels')
    if result:
        # Filter by source node manually since listchannels positional args
        # don't work well through subprocess
        all_channels = result.get('channels', [])
        return [ch for ch in all_channels if ch.get('source') == node_id]
    return []


def get_hive_topology() -> Dict[str, Any]:
    """Get current hive topology analysis."""
    result = run_lightning_cli('hive-topology')
    return result or {}


def get_our_funds() -> Dict[str, Any]:
    """Get our current funds status."""
    result = run_lightning_cli('listfunds')
    if result:
        outputs = result.get('outputs', [])
        channels = result.get('channels', [])
        return {
            'onchain_sats': sum(o.get('amount_msat', 0) // 1000 for o in outputs if o.get('status') == 'confirmed'),
            'channel_count': len(channels),
            'channel_capacity_sats': sum(c.get('amount_msat', 0) // 1000 for c in channels)
        }
    return {'onchain_sats': 0, 'channel_count': 0, 'channel_capacity_sats': 0}


def approve_action(action_id: int) -> bool:
    """Approve a pending action."""
    result = run_lightning_cli('hive-approve-action', str(action_id))
    return result is not None and result.get('status') == 'approved'


def reject_action(action_id: int) -> bool:
    """Reject a pending action."""
    result = run_lightning_cli('hive-reject-action', str(action_id))
    return result is not None and result.get('status') == 'rejected'


# =============================================================================
# CONTEXT GATHERING
# =============================================================================

def gather_context(action: Dict) -> Dict[str, Any]:
    """Gather context about an action for AI evaluation."""
    context = {
        'action': action,
        'timestamp': datetime.now().isoformat(),
    }

    # Get target node info if applicable
    # Target can be at top level or nested in payload
    payload = action.get('payload', {})
    target = action.get('target') or payload.get('target') or action.get('peer_id')
    if target:
        context['target_info'] = get_node_info(target)
        context['target_channels'] = get_node_channels(target)
        context['target_channel_count'] = len(context['target_channels'])

        # Calculate average fee
        fees = [c.get('fee_per_millionth', 0) for c in context['target_channels']]
        context['target_avg_fee_ppm'] = sum(fees) / len(fees) if fees else 0

    # Get our funds
    context['our_funds'] = get_our_funds()

    # Get hive topology
    context['hive_topology'] = get_hive_topology()

    return context


# =============================================================================
# AI DECISION MAKING
# =============================================================================

def build_prompt(action: Dict, context: Dict) -> str:
    """Build a prompt for Claude to evaluate the action."""
    action_type = action.get('action_type', 'unknown')

    prompt = f"""You are an AI advisor for a Lightning Network node fleet called "The Hive".
Your job is to evaluate governance proposals and make decisions.

## Current Proposal

Action Type: {action_type}
Action ID: {action.get('id')}
Details: {json.dumps(action, indent=2)}

## Context

Target Node Info:
{json.dumps(context.get('target_info', {}), indent=2)}

Target has {context.get('target_channel_count', 0)} channels
Target average fee: {context.get('target_avg_fee_ppm', 0):.0f} ppm

Our Funds:
- Onchain: {context['our_funds'].get('onchain_sats', 0):,} sats
- Channels: {context['our_funds'].get('channel_count', 0)}
- Channel Capacity: {context['our_funds'].get('channel_capacity_sats', 0):,} sats

Hive Topology:
{json.dumps(context.get('hive_topology', {}), indent=2)}

## Decision Criteria

For CHANNEL_OPEN proposals, consider:
1. Does the target have good connectivity (>5 channels)?
2. Are the target's fees reasonable (<1000 ppm)?
3. Do we have sufficient onchain funds (>200k sats)?
4. Is this target already covered by another hive member?
5. Would this improve our routing capabilities?

For BAN proposals, consider:
1. Is there evidence of bad behavior?
2. Has the peer been consistently problematic?
3. Would banning harm the network?

## Your Response

Respond with EXACTLY one of these formats:

APPROVE: [brief reason]

or

REJECT: [brief reason]

Do not include any other text before or after your decision.
"""
    return prompt


def ask_claude(action: Dict, context: Dict) -> tuple[str, str]:
    """Ask Claude to evaluate an action. Returns (decision, reason)."""
    if not HAS_ANTHROPIC:
        # Mock decision for testing without API
        return mock_decision(action, context)

    # Check for API key
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print("No ANTHROPIC_API_KEY set, using mock decisions.")
        return mock_decision(action, context)

    client = anthropic.Anthropic(api_key=api_key)
    prompt = build_prompt(action, context)

    try:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )

        text = response.content[0].text.strip()

        # Parse response
        if text.startswith('APPROVE:'):
            return ('approve', text[8:].strip())
        elif text.startswith('REJECT:'):
            return ('reject', text[7:].strip())
        else:
            # Try to infer from text
            text_lower = text.lower()
            if 'approve' in text_lower and 'reject' not in text_lower:
                return ('approve', text)
            else:
                return ('reject', f"Unclear response: {text}")

    except Exception as e:
        print(f"Error calling Claude API: {e}")
        return ('reject', f"API error: {e}")


def mock_decision(action: Dict, context: Dict) -> tuple[str, str]:
    """Make a rule-based decision without AI (for testing)."""
    action_type = action.get('action_type', '')

    if action_type == 'channel_open':
        # Check target connectivity
        channel_count = context.get('target_channel_count', 0)
        if channel_count < MIN_NODE_CHANNELS:
            return ('reject', f"Target has only {channel_count} channels (minimum: {MIN_NODE_CHANNELS})")

        # Check target fees
        avg_fee = context.get('target_avg_fee_ppm', 0)
        if avg_fee > MAX_FEE_PPM:
            return ('reject', f"Target avg fee {avg_fee:.0f} ppm exceeds maximum {MAX_FEE_PPM}")

        # Check our funds
        onchain = context['our_funds'].get('onchain_sats', 0)
        if onchain < 200_000:
            return ('reject', f"Insufficient onchain funds: {onchain:,} sats")

        return ('approve', f"Target well-connected ({channel_count} channels), reasonable fees ({avg_fee:.0f} ppm)")

    elif action_type == 'ban':
        # Default to reject bans (require human review)
        return ('reject', "Ban proposals require human review")

    else:
        return ('reject', f"Unknown action type: {action_type}")


# =============================================================================
# MAIN ADVISOR LOGIC
# =============================================================================

def process_pending_actions(dry_run: bool = False) -> List[Dict]:
    """Process all pending actions and return decisions."""
    actions = get_pending_actions()
    decisions = []

    if not actions:
        print("No pending actions")
        return decisions

    print(f"Found {len(actions)} pending action(s)")

    for action in actions:
        action_id = action.get('id')
        action_type = action.get('action_type', 'unknown')

        print(f"\n{'='*60}")
        print(f"Evaluating action {action_id}: {action_type}")
        print(f"{'='*60}")

        # Gather context
        print("Gathering context...")
        context = gather_context(action)

        # Get AI decision
        print("Consulting AI advisor...")
        decision, reason = ask_claude(action, context)

        print(f"\nDecision: {decision.upper()}")
        print(f"Reason: {reason}")

        result = {
            'action_id': action_id,
            'action_type': action_type,
            'decision': decision,
            'reason': reason,
            'executed': False
        }

        # Execute decision
        if not dry_run:
            if decision == 'approve':
                print(f"Executing: hive-approve-action {action_id}")
                success = approve_action(action_id)
                result['executed'] = success
                if success:
                    print("Action approved successfully")
                else:
                    print("Failed to approve action")
            else:
                print(f"Executing: hive-reject-action {action_id}")
                success = reject_action(action_id)
                result['executed'] = success
                if success:
                    print("Action rejected successfully")
                else:
                    print("Failed to reject action")
        else:
            print("[DRY RUN] Would execute decision but skipping")

        decisions.append(result)

    return decisions


def daemon_mode(dry_run: bool = False):
    """Run continuously, polling for new actions."""
    print(f"Starting AI Advisor daemon (poll interval: {POLL_INTERVAL}s)")
    print("Press Ctrl+C to stop")

    while True:
        try:
            print(f"\n[{datetime.now().isoformat()}] Checking for pending actions...")
            process_pending_actions(dry_run)
            print(f"Sleeping for {POLL_INTERVAL} seconds...")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\nShutting down...")
            break
        except Exception as e:
            print(f"Error in daemon loop: {e}")
            time.sleep(60)  # Wait a minute before retrying


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='AI Advisor for cl-hive governance decisions'
    )
    parser.add_argument(
        '--poll', action='store_true',
        help='Poll once for pending actions and make decisions'
    )
    parser.add_argument(
        '--daemon', action='store_true',
        help='Run continuously, polling every POLL_INTERVAL seconds'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show decisions without executing them'
    )
    parser.add_argument(
        '--test', action='store_true',
        help='Test with a mock action (no lightning-cli required)'
    )

    args = parser.parse_args()

    if args.test:
        # Test mode with mock data
        print("Running in TEST mode with mock data")
        mock_action = {
            'id': 1,
            'action_type': 'channel_open',
            'target': '02abcd1234...',
            'proposed_at': datetime.now().isoformat(),
            'proposed_by': 'planner',
            'details': {'capacity': 1000000, 'reason': 'underserved_target'}
        }
        context = {
            'target_info': {'alias': 'TestNode', 'nodeid': '02abcd1234...'},
            'target_channel_count': 15,
            'target_avg_fee_ppm': 250,
            'our_funds': {'onchain_sats': 500000, 'channel_count': 5, 'channel_capacity_sats': 5000000},
            'hive_topology': {'network_cache_size': 13, 'saturated_count': 0}
        }
        decision, reason = ask_claude(mock_action, context)
        print(f"\nMock Action: {json.dumps(mock_action, indent=2)}")
        print(f"\nDecision: {decision.upper()}")
        print(f"Reason: {reason}")
        return

    if args.daemon:
        daemon_mode(dry_run=args.dry_run)
    elif args.poll:
        decisions = process_pending_actions(dry_run=args.dry_run)
        print(f"\n{'='*60}")
        print(f"Summary: {len(decisions)} action(s) processed")
        for d in decisions:
            status = "executed" if d['executed'] else "skipped"
            print(f"  - Action {d['action_id']}: {d['decision']} ({status})")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
