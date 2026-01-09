"""
Governance Module for cl-hive (Phase 7)

Implements the Decision Engine that controls how Hive actions are executed:
- ADVISOR mode: Queue actions for manual approval (human in the loop)
- AUTONOMOUS mode: Execute within safety limits (budget cap, rate limits)
- ORACLE mode: Delegate decisions to external API with fallback

Security Constraints (GEMINI.md):
- Rule #3: Fail-Closed Bias - On any error, fall back to ADVISOR mode
- Rule #4: No Silent Fund Actions - All proposals logged to pending_actions

Author: Lightning Goats Team
"""

import json
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


# =============================================================================
# CONSTANTS
# =============================================================================

# Oracle retry configuration
ORACLE_RETRY_COUNT = 1
ORACLE_RETRY_DELAY_SECONDS = 2

# Default action expiry
DEFAULT_ACTION_EXPIRY_HOURS = 24


# =============================================================================
# ENUMS
# =============================================================================

class GovernanceMode(Enum):
    """Decision-making modes for the Hive."""
    ADVISOR = 'advisor'       # Queue for manual approval
    AUTONOMOUS = 'autonomous' # Execute within safety limits
    ORACLE = 'oracle'         # Delegate to external API


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
    Packet sent to external oracle for decision.

    Contains all context needed for the oracle to make an informed decision.
    """
    action_type: str          # 'channel_open', 'rebalance', 'ban'
    target: str               # Target peer pubkey
    context: Dict[str, Any]   # Additional context (capacity, share, balance)
    timestamp: int            # Unix timestamp

    def to_json(self) -> str:
        """Serialize to JSON for API call."""
        return json.dumps(asdict(self), sort_keys=True)


@dataclass
class DecisionResponse:
    """Response from governance decision."""
    result: DecisionResult
    action_id: Optional[int] = None   # ID in pending_actions table (if queued)
    reason: str = ""                   # Human-readable reason
    oracle_response: Optional[Dict] = None  # Raw oracle response (if applicable)


# =============================================================================
# DECISION ENGINE
# =============================================================================

class DecisionEngine:
    """
    Governance decision engine for the Hive.

    Handles action proposals based on the configured governance mode:
    - ADVISOR: Queue to pending_actions, require manual approval
    - AUTONOMOUS: Execute if within budget/rate limits
    - ORACLE: Query external API, fallback to ADVISOR on failure

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

        # Autonomous mode state tracking
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
        - ADVISOR: Queued for manual approval
        - AUTONOMOUS: Executed if within limits, else queued
        - ORACLE: Decided by external API, with fallback

        Args:
            action_type: Type of action ('channel_open', 'rebalance', 'ban')
            target: Target peer pubkey
            context: Additional context for decision
            cfg: Config snapshot

        Returns:
            DecisionResponse with result and details
        """
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
            elif mode == GovernanceMode.AUTONOMOUS:
                return self._handle_autonomous_mode(packet, cfg)
            elif mode == GovernanceMode.ORACLE:
                return self._handle_oracle_mode(packet, cfg)
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
        Handle action in ADVISOR mode - queue for manual approval.

        Args:
            packet: Decision packet
            cfg: Config snapshot

        Returns:
            DecisionResponse with QUEUED result
        """
        # Queue to pending_actions
        action_id = self.db.add_pending_action(
            action_type=packet.action_type,
            payload={
                'target': packet.target,
                'context': packet.context,
                'timestamp': packet.timestamp,
            },
            expires_hours=DEFAULT_ACTION_EXPIRY_HOURS
        )

        self._log(f"Action queued for approval (id={action_id})")

        return DecisionResponse(
            result=DecisionResult.QUEUED,
            action_id=action_id,
            reason="Queued for manual approval (ADVISOR mode)"
        )

    # =========================================================================
    # AUTONOMOUS MODE
    # =========================================================================

    def _handle_autonomous_mode(self, packet: DecisionPacket, cfg) -> DecisionResponse:
        """
        Handle action in AUTONOMOUS mode - execute within safety limits.

        Safety constraints:
        - Daily budget cap (cfg.autonomous_budget_per_day)
        - Hourly rate limit (cfg.autonomous_actions_per_hour)

        If limits exceeded, falls back to queuing (ADVISOR behavior).

        Args:
            packet: Decision packet
            cfg: Config snapshot

        Returns:
            DecisionResponse with APPROVED/QUEUED/DENIED result
        """
        # Check daily budget
        amount_sats = packet.context.get('amount_sats', 0)
        if not self._check_budget(amount_sats, cfg):
            self._log(
                f"Daily budget exceeded ({self._daily_spend_sats} + {amount_sats} > "
                f"{cfg.autonomous_budget_per_day}), queueing action",
                level='warn'
            )
            return self._handle_advisor_mode(packet, cfg)

        # Check rate limit
        if not self._check_rate_limit(cfg):
            self._log(
                f"Hourly rate limit exceeded ({len(self._hourly_actions)} >= "
                f"{cfg.autonomous_actions_per_hour}), queueing action",
                level='warn'
            )
            return self._handle_advisor_mode(packet, cfg)

        # Execute the action
        executor = self._executors.get(packet.action_type)
        if executor:
            try:
                executor(packet.target, packet.context)

                # Update tracking
                self._daily_spend_sats += amount_sats
                self._hourly_actions.append(int(time.time()))

                self._log(f"Action executed (AUTONOMOUS mode)")

                return DecisionResponse(
                    result=DecisionResult.APPROVED,
                    reason="Executed within safety limits (AUTONOMOUS mode)"
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
        Check if amount is within daily budget.

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

        return (self._daily_spend_sats + amount_sats) <= cfg.autonomous_budget_per_day

    def _check_rate_limit(self, cfg) -> bool:
        """
        Check if within hourly rate limit.

        Args:
            cfg: Config snapshot

        Returns:
            True if within limit, False otherwise
        """
        now = int(time.time())
        cutoff = now - 3600  # 1 hour ago

        # Prune old actions
        self._hourly_actions = [ts for ts in self._hourly_actions if ts > cutoff]

        return len(self._hourly_actions) < cfg.autonomous_actions_per_hour

    # =========================================================================
    # ORACLE MODE
    # =========================================================================

    def _handle_oracle_mode(self, packet: DecisionPacket, cfg) -> DecisionResponse:
        """
        Handle action in ORACLE mode - delegate to external API.

        Implements fail-closed behavior:
        - Timeout -> fallback to ADVISOR
        - 5xx error -> fallback to ADVISOR
        - Malformed response -> fallback to ADVISOR
        - DENY response -> action rejected
        - APPROVE response -> execute action

        Args:
            packet: Decision packet
            cfg: Config snapshot

        Returns:
            DecisionResponse based on oracle decision
        """
        oracle_url = cfg.oracle_url

        if not oracle_url:
            self._log("Oracle URL not configured, falling back to ADVISOR", level='warn')
            return self._handle_advisor_mode(packet, cfg)

        # Query oracle with retry
        oracle_response = self._query_oracle(
            url=oracle_url,
            packet=packet,
            timeout=cfg.oracle_timeout_seconds
        )

        if oracle_response is None:
            # GEMINI.md Rule #3: Fail-Closed - fallback to ADVISOR
            self._log("Oracle query failed, falling back to ADVISOR", level='warn')
            return self._handle_advisor_mode(packet, cfg)

        # Parse oracle decision
        decision = oracle_response.get('decision', '').upper()
        reason = oracle_response.get('reason', '')

        if decision == 'APPROVE':
            # Execute the action
            executor = self._executors.get(packet.action_type)
            if executor:
                try:
                    executor(packet.target, packet.context)
                    self._log(f"Action approved and executed by oracle")

                    return DecisionResponse(
                        result=DecisionResult.APPROVED,
                        reason=f"Approved by oracle: {reason}",
                        oracle_response=oracle_response
                    )
                except Exception as e:
                    self._log(f"Execution failed after oracle approval: {e}", level='warn')
                    return self._handle_advisor_mode(packet, cfg)
            else:
                self._log(f"No executor for {packet.action_type}, queueing")
                return self._handle_advisor_mode(packet, cfg)

        elif decision == 'DENY':
            self._log(f"Action denied by oracle: {reason}")
            return DecisionResponse(
                result=DecisionResult.DENIED,
                reason=f"Denied by oracle: {reason}",
                oracle_response=oracle_response
            )
        else:
            # Malformed response - fail closed
            self._log(f"Invalid oracle response: {oracle_response}, falling back to ADVISOR", level='warn')
            return self._handle_advisor_mode(packet, cfg)

    def _query_oracle(
        self,
        url: str,
        packet: DecisionPacket,
        timeout: int
    ) -> Optional[Dict]:
        """
        Query the external oracle API.

        Implements:
        - POST request with JSON body
        - Configurable timeout
        - 1 retry after 2s delay

        Args:
            url: Oracle API URL
            packet: Decision packet to send
            timeout: Request timeout in seconds

        Returns:
            Dict response or None on failure
        """
        payload = packet.to_json().encode('utf-8')

        for attempt in range(ORACLE_RETRY_COUNT + 1):
            try:
                req = urllib.request.Request(
                    url,
                    data=payload,
                    headers={
                        'Content-Type': 'application/json',
                        'User-Agent': 'cl-hive/1.0'
                    },
                    method='POST'
                )

                with urllib.request.urlopen(req, timeout=timeout) as response:
                    if response.status == 200:
                        body = response.read().decode('utf-8')
                        return json.loads(body)
                    elif response.status >= 500:
                        self._log(f"Oracle returned {response.status}, retrying...", level='warn')
                    else:
                        # 4xx error - don't retry
                        self._log(f"Oracle returned {response.status}", level='warn')
                        return None

            except urllib.error.URLError as e:
                self._log(f"Oracle connection error: {e}", level='warn')
            except urllib.error.HTTPError as e:
                if e.code >= 500:
                    self._log(f"Oracle server error {e.code}, retrying...", level='warn')
                else:
                    self._log(f"Oracle HTTP error {e.code}", level='warn')
                    return None
            except json.JSONDecodeError as e:
                self._log(f"Oracle returned invalid JSON: {e}", level='warn')
                return None
            except Exception as e:
                self._log(f"Oracle query error: {e}", level='warn')

            # Wait before retry
            if attempt < ORACLE_RETRY_COUNT:
                time.sleep(ORACLE_RETRY_DELAY_SECONDS)

        return None

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
