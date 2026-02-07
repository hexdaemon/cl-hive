"""
Governance Module for cl-hive (Phase 7)

Implements the Decision Engine that controls how Hive actions are executed:
- ADVISOR mode: Queue actions for AI/human approval via MCP server (primary mode)
- FAILSAFE mode: Auto-execute emergency actions within tight safety limits

Design Philosophy:
  ADVISOR mode is the primary decision path - AI (via MCP server) makes smart,
  context-aware decisions about channel opens, fee adjustments, and rebalancing.

  FAILSAFE mode is for emergency situations when AI is unavailable - it handles
  only critical safety actions (bans, rate limiting) within strict budget/rate
  limits. All strategic decisions still queue to pending_actions.

Security Constraints (GEMINI.md):
- Rule #3: Fail-Closed Bias - On any error, fall back to ADVISOR mode
- Rule #4: No Silent Fund Actions - All proposals logged to pending_actions

Author: Lightning Goats Team
"""

import json
import time
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


# =============================================================================
# CONSTANTS
# =============================================================================

# Default action expiry
DEFAULT_ACTION_EXPIRY_HOURS = 24

# Input validation bounds
MAX_ACTION_TYPE_LEN = 64
MAX_TARGET_LEN = 256


# =============================================================================
# ENUMS
# =============================================================================

class GovernanceMode(Enum):
    """Decision-making modes for the Hive."""
    ADVISOR = 'advisor'    # Queue for AI/human approval (primary mode)
    FAILSAFE = 'failsafe'  # Emergency auto-execute within tight limits


class DecisionResult(Enum):
    """Result of a governance decision."""
    APPROVED = 'approved'     # Execute immediately
    QUEUED = 'queued'         # Queued for manual approval
    DENIED = 'denied'         # Rejected by policy/oracle
    ERROR = 'error'           # Error during decision (fallback to ADVISOR)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class DecisionPacket:
    """
    Packet containing action details for governance decision.

    Contains all context needed to make an informed decision about the action.
    """
    action_type: str          # 'channel_open', 'rebalance', 'ban', 'emergency_ban'
    target: str               # Target peer pubkey
    context: Dict[str, Any]   # Additional context (capacity, share, balance)
    timestamp: int            # Unix timestamp

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps(asdict(self), sort_keys=True)


@dataclass
class DecisionResponse:
    """Response from governance decision."""
    result: DecisionResult
    action_id: Optional[int] = None   # ID in pending_actions table (if queued)
    reason: str = ""                   # Human-readable reason


# =============================================================================
# DECISION ENGINE
# =============================================================================

class DecisionEngine:
    """
    Governance decision engine for the Hive.

    Handles action proposals based on the configured governance mode:
    - ADVISOR: Queue to pending_actions for AI/human approval (primary mode)
    - FAILSAFE: Execute emergency actions within strict budget/rate limits

    ADVISOR mode is the primary path - the MCP server enables AI to make smart
    decisions about pending_actions. FAILSAFE is for when AI is unavailable.

    Thread Safety:
    - Uses config snapshot pattern for consistency
    - Rate limit state is tracked in memory with daily reset
    """

    def __init__(self, database, plugin=None):
        """
        Initialize the DecisionEngine.

        Args:
            database: HiveDatabase instance for pending_actions
            plugin: Plugin reference for logging
        """
        self.db = database
        self.plugin = plugin

        # Failsafe mode state tracking (budget and rate limits)
        self._daily_spend_sats: int = 0
        self._daily_spend_reset_day: int = 0  # Day of year for reset
        self._hourly_actions: List[int] = []  # Timestamps of recent actions

        # Executor callbacks (set by cl-hive.py)
        self._executors: Dict[str, Callable] = {}

    def _log(self, msg: str, level: str = 'info') -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[Governance] {msg}", level=level)

    # =========================================================================
    # PUBLIC API
    # =========================================================================

    def register_executor(self, action_type: str, executor: Callable) -> None:
        """
        Register an executor callback for an action type.

        Args:
            action_type: Type of action (e.g., 'channel_open')
            executor: Callable that takes (target, context) and executes the action
        """
        self._executors[action_type] = executor

    def propose_action(
        self,
        action_type: str,
        target: str,
        context: Dict[str, Any],
        cfg
    ) -> DecisionResponse:
        """
        Propose an action for governance decision.

        This is the main entry point for all governance decisions.
        Based on the governance mode, the action will be:
        - ADVISOR: Queued for AI/human approval via MCP server
        - FAILSAFE: Emergency actions executed if within limits, else queued

        Args:
            action_type: Type of action ('channel_open', 'rebalance', 'ban', 'emergency_ban')
            target: Target peer pubkey
            context: Additional context for decision
            cfg: Config snapshot

        Returns:
            DecisionResponse with result and details
        """
        if len(action_type) > MAX_ACTION_TYPE_LEN or len(str(target)) > MAX_TARGET_LEN:
            return DecisionResponse(
                result=DecisionResult.ERROR,
                reason="action_type or target exceeds maximum length"
            )

        # Create decision packet first (outside try block for better error messages)
        packet = DecisionPacket(
            action_type=action_type,
            target=target,
            context=context,
            timestamp=int(time.time())
        )

        try:
            mode = GovernanceMode(cfg.governance_mode)
            self._log(f"Proposing {action_type} to {target[:16]}... (mode={mode.value})")

            if mode == GovernanceMode.ADVISOR:
                return self._handle_advisor_mode(packet, cfg)
            elif mode == GovernanceMode.FAILSAFE:
                return self._handle_failsafe_mode(packet, cfg)
            else:
                # Unknown mode - fail closed to ADVISOR
                self._log(f"Unknown mode {mode}, falling back to ADVISOR", level='warn')
                return self._handle_advisor_mode(packet, cfg)

        except Exception as e:
            # GEMINI.md Rule #3: Fail-Closed Bias
            self._log(f"Error in governance decision: {e}, falling back to ADVISOR", level='warn')
            return self._handle_advisor_mode(packet, cfg)

    # =========================================================================
    # ADVISOR MODE
    # =========================================================================

    def _handle_advisor_mode(self, packet: DecisionPacket, cfg) -> DecisionResponse:
        """
        Handle action in ADVISOR mode - queue for AI/human approval.

        This is the primary decision path. Actions are queued to pending_actions
        where the AI (via MCP server) can make smart, context-aware decisions.

        Args:
            packet: Decision packet
            cfg: Config snapshot

        Returns:
            DecisionResponse with QUEUED result
        """
        # Queue to pending_actions for AI/human review
        action_id = self.db.add_pending_action(
            action_type=packet.action_type,
            payload={
                'target': packet.target,
                'context': packet.context,
                'timestamp': packet.timestamp,
            },
            expires_hours=DEFAULT_ACTION_EXPIRY_HOURS
        )

        self._log(f"Action queued for AI/human approval (id={action_id})")

        return DecisionResponse(
            result=DecisionResult.QUEUED,
            action_id=action_id,
            reason="Queued for AI/human approval (ADVISOR mode)"
        )

    # =========================================================================
    # FAILSAFE MODE
    # =========================================================================

    # Action types that can be auto-executed in failsafe mode
    FAILSAFE_ACTION_TYPES = frozenset({'emergency_ban', 'rate_limit_peer'})

    def _handle_failsafe_mode(self, packet: DecisionPacket, cfg) -> DecisionResponse:
        """
        Handle action in FAILSAFE mode - auto-execute emergency actions only.

        FAILSAFE mode is for when AI is unavailable. It only auto-executes
        critical safety actions (emergency bans, rate limiting). All strategic
        decisions (channel opens, fee changes, rebalancing) still queue to
        pending_actions for later AI review.

        Safety constraints:
        - Only emergency action types can auto-execute
        - Daily budget cap (cfg.failsafe_budget_per_day)
        - Hourly rate limit (cfg.failsafe_actions_per_hour)

        If limits exceeded or non-emergency action, falls back to queuing.

        Args:
            packet: Decision packet
            cfg: Config snapshot

        Returns:
            DecisionResponse with APPROVED/QUEUED result
        """
        # Only auto-execute emergency action types
        if packet.action_type not in self.FAILSAFE_ACTION_TYPES:
            self._log(
                f"Non-emergency action {packet.action_type} in FAILSAFE mode, queueing",
                level='info'
            )
            return self._handle_advisor_mode(packet, cfg)

        # Check daily budget
        amount_sats = packet.context.get('amount_sats', 0)
        if isinstance(amount_sats, (int, float)) and amount_sats < 0:
            amount_sats = 0
        if not self._check_budget(amount_sats, cfg):
            self._log(
                f"Daily budget exceeded ({self._daily_spend_sats} + {amount_sats} > "
                f"{cfg.failsafe_budget_per_day}), queueing action",
                level='warn'
            )
            return self._handle_advisor_mode(packet, cfg)

        # Check rate limit
        if not self._check_rate_limit(cfg):
            self._log(
                f"Hourly rate limit exceeded ({len(self._hourly_actions)} >= "
                f"{cfg.failsafe_actions_per_hour}), queueing action",
                level='warn'
            )
            return self._handle_advisor_mode(packet, cfg)

        # Execute the emergency action
        executor = self._executors.get(packet.action_type)
        if executor:
            try:
                executor(packet.target, packet.context)

                # Update tracking
                self._daily_spend_sats += amount_sats
                self._hourly_actions.append(int(time.time()))

                self._log(f"Emergency action executed (FAILSAFE mode)")

                return DecisionResponse(
                    result=DecisionResult.APPROVED,
                    reason="Emergency action executed (FAILSAFE mode)"
                )
            except Exception as e:
                self._log(f"Execution failed: {e}, queueing action", level='warn')
                return self._handle_advisor_mode(packet, cfg)
        else:
            # No executor registered - queue for manual handling
            self._log(f"No executor for {packet.action_type}, queueing action")
            return self._handle_advisor_mode(packet, cfg)

    def _check_budget(self, amount_sats: int, cfg) -> bool:
        """
        Check if amount is within daily budget (failsafe mode).

        Resets budget at midnight UTC.

        Args:
            amount_sats: Amount to check
            cfg: Config snapshot

        Returns:
            True if within budget, False otherwise
        """
        # Reset daily spend at midnight UTC
        now = time.time()
        current_day = int(now // 86400)  # Days since epoch

        if current_day != self._daily_spend_reset_day:
            self._daily_spend_sats = 0
            self._daily_spend_reset_day = current_day

        return (self._daily_spend_sats + amount_sats) <= cfg.failsafe_budget_per_day

    def _check_rate_limit(self, cfg) -> bool:
        """
        Check if within hourly rate limit (failsafe mode).

        Args:
            cfg: Config snapshot

        Returns:
            True if within limit, False otherwise
        """
        now = int(time.time())
        cutoff = now - 3600  # 1 hour ago

        # Prune old actions
        self._hourly_actions = [ts for ts in self._hourly_actions if ts > cutoff]

        return len(self._hourly_actions) < cfg.failsafe_actions_per_hour

    # =========================================================================
    # STATISTICS
    # =========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get current governance statistics."""
        now = int(time.time())
        cutoff = now - 3600

        # Prune old actions for accurate count
        recent_actions = [ts for ts in self._hourly_actions if ts > cutoff]

        return {
            'daily_spend_sats': self._daily_spend_sats,
            'daily_spend_reset_day': self._daily_spend_reset_day,
            'hourly_action_count': len(recent_actions),
            'registered_executors': list(self._executors.keys()),
        }

    def reset_limits(self) -> None:
        """Reset all rate limits and budget tracking (for testing)."""
        self._daily_spend_sats = 0
        self._daily_spend_reset_day = 0
        self._hourly_actions = []
