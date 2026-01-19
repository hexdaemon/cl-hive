"""
Proactive AI Advisor Engine

Main advisor engine that runs every 3 hours with the following cycle:
1. Analyze comprehensive node state
2. Check goal progress and adjust strategy if needed
3. Scan all data sources for opportunities
4. Score opportunities (with learning adjustments)
5. Execute auto-actions within bounds
6. Queue remaining for approval
7. Measure outcomes of past decisions (6-24h ago)
8. Plan priorities for next cycle

Core Philosophy:
- Minimum Interference: Primarily tune plugin options, not direct control
- Conservative Approach: Default to no action when uncertain
- Temporary Overrides: When direct action needed, always set expiry

Usage:
    from proactive_advisor import ProactiveAdvisor

    advisor = ProactiveAdvisor(mcp_client, db)
    result = await advisor.run_cycle("Hive-Nexus-01")
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from goal_manager import GoalManager, Goal, GoalProgress

# =============================================================================
# Logging Setup
# =============================================================================

# Default log directory (can be overridden via environment)
LOG_DIR = os.environ.get("ADVISOR_LOG_DIR", "/home/sat/bin/cl-hive/production/logs")
LOG_FILE = os.path.join(LOG_DIR, "proactive_advisor.log")

# Setup logger
logger = logging.getLogger("proactive_advisor")


def setup_file_logging(log_file: str = None, level: int = logging.INFO) -> None:
    """
    Configure file logging for the proactive advisor.

    Args:
        log_file: Path to log file (default: LOG_FILE)
        level: Logging level (default: INFO)
    """
    if log_file is None:
        log_file = LOG_FILE

    # Create log directory if needed
    log_dir = os.path.dirname(log_file)
    if log_dir:
        Path(log_dir).mkdir(parents=True, exist_ok=True)

    # Configure logger
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers = []

    # File handler with rotation (10MB max, keep 5 backups)
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Also log to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(file_formatter)
    logger.addHandler(console_handler)

    logger.info(f"Logging initialized: {log_file}")
from learning_engine import LearningEngine, ActionOutcome
from opportunity_scanner import (
    OpportunityScanner,
    Opportunity,
    OpportunityType,
    ActionType,
    ActionClassification,
    SAFETY_CONSTRAINTS
)


# =============================================================================
# Constants
# =============================================================================

# Cycle timing
CYCLE_INTERVAL_HOURS = 3

# Budget tracking
DAILY_FEE_CHANGE_BUDGET = 20
DAILY_REBALANCE_BUDGET = 10
DAILY_REBALANCE_FEE_BUDGET_SATS = 50_000

# Conservative thresholds
MIN_ONCHAIN_RESERVE_SATS = 600_000
MAX_FEE_CHANGE_PCT = 15  # Max 15% per change (more conservative than 25%)
MIN_AUTO_EXECUTE_CONFIDENCE = 0.8


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class CycleResult:
    """Result of a complete advisor cycle."""
    cycle_id: str
    node_name: str
    timestamp: str
    duration_seconds: float
    success: bool

    # State analysis
    node_state_summary: Dict[str, Any] = field(default_factory=dict)

    # Goals
    goals_checked: int = 0
    goals_on_track: int = 0
    strategy_adjustments: List[str] = field(default_factory=list)

    # Opportunities
    opportunities_found: int = 0
    opportunities_by_type: Dict[str, int] = field(default_factory=dict)

    # Actions
    auto_executed: List[Dict] = field(default_factory=list)
    queued_for_review: List[Dict] = field(default_factory=list)
    skipped: List[Dict] = field(default_factory=list)

    # Learning
    outcomes_measured: int = 0
    learning_summary: Dict[str, Any] = field(default_factory=dict)

    # Planning
    next_cycle_priorities: List[str] = field(default_factory=list)

    # Errors
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "cycle_id": self.cycle_id,
            "node_name": self.node_name,
            "timestamp": self.timestamp,
            "duration_seconds": round(self.duration_seconds, 2),
            "success": self.success,
            "node_state_summary": self.node_state_summary,
            "goals_checked": self.goals_checked,
            "goals_on_track": self.goals_on_track,
            "strategy_adjustments": self.strategy_adjustments,
            "opportunities_found": self.opportunities_found,
            "opportunities_by_type": self.opportunities_by_type,
            "auto_executed_count": len(self.auto_executed),
            "queued_count": len(self.queued_for_review),
            "skipped_count": len(self.skipped),
            "outcomes_measured": self.outcomes_measured,
            "next_cycle_priorities": self.next_cycle_priorities,
            "errors": self.errors
        }


@dataclass
class DailyBudget:
    """Tracks daily action budgets to prevent over-action."""
    date: str
    fee_changes_used: int = 0
    rebalances_used: int = 0
    rebalance_fees_spent_sats: int = 0

    def can_change_fee(self) -> bool:
        return self.fee_changes_used < DAILY_FEE_CHANGE_BUDGET

    def can_rebalance(self) -> bool:
        return (self.rebalances_used < DAILY_REBALANCE_BUDGET
                and self.rebalance_fees_spent_sats < DAILY_REBALANCE_FEE_BUDGET_SATS)

    def record_fee_change(self):
        self.fee_changes_used += 1

    def record_rebalance(self, fee_sats: int):
        self.rebalances_used += 1
        self.rebalance_fees_spent_sats += fee_sats


# =============================================================================
# Main Advisor Class
# =============================================================================

class ProactiveAdvisor:
    """
    Main proactive advisor engine.

    Runs every 3 hours with comprehensive analysis, opportunity scanning,
    safe auto-execution, and learning from outcomes.
    """

    def __init__(self, mcp_client, db, log_file: str = None):
        """
        Initialize proactive advisor.

        Args:
            mcp_client: Client for calling MCP tools
            db: AdvisorDB instance
            log_file: Optional custom log file path
        """
        # Setup file logging first
        setup_file_logging(log_file)

        self.mcp = mcp_client
        self.db = db
        self.goal_manager = GoalManager(db)
        self.learning_engine = LearningEngine(db)
        self.scanner = OpportunityScanner(mcp_client, db)

        # Daily budget tracking (resets at midnight UTC)
        self._daily_budget = self._load_or_create_budget()

    def _load_or_create_budget(self) -> DailyBudget:
        """Load or create daily budget."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        stored = self.db.get_daily_budget(today)
        if stored:
            return DailyBudget(
                date=today,
                fee_changes_used=stored.get("fee_changes_used", 0),
                rebalances_used=stored.get("rebalances_used", 0),
                rebalance_fees_spent_sats=stored.get("rebalance_fees_spent_sats", 0)
            )
        return DailyBudget(date=today)

    def _save_budget(self):
        """Save daily budget to database."""
        self.db.save_daily_budget(
            self._daily_budget.date,
            {
                "fee_changes_used": self._daily_budget.fee_changes_used,
                "rebalances_used": self._daily_budget.rebalances_used,
                "rebalance_fees_spent_sats": self._daily_budget.rebalance_fees_spent_sats
            }
        )

    async def run_cycle(self, node_name: str) -> CycleResult:
        """
        Execute one complete 3-hour advisor cycle.

        Args:
            node_name: Node to advise

        Returns:
            CycleResult with all cycle details
        """
        cycle_start = time.time()
        now = datetime.now()

        logger.info("=" * 60)
        logger.info(f"PROACTIVE ADVISOR CYCLE - {node_name}")
        logger.info(f"Started: {now.isoformat()}")
        logger.info("=" * 60)

        result = CycleResult(
            cycle_id=f"{node_name}_{int(cycle_start)}",
            node_name=node_name,
            timestamp=now.isoformat(),
            duration_seconds=0,
            success=False
        )

        try:
            # Phase 1: Record snapshot for history
            logger.info("[Phase 1] Recording snapshot...")
            await self._record_snapshot(node_name)

            # Phase 2: Comprehensive state analysis
            logger.info("[Phase 2] Analyzing node state...")
            state = await self._analyze_node_state(node_name)
            result.node_state_summary = state.get("summary", {})
            summary = result.node_state_summary
            logger.info(f"  Capacity: {summary.get('total_capacity_sats', 0):,} sats")
            logger.info(f"  Channels: {summary.get('channel_count', 0)}")
            logger.info(f"  ROC: {summary.get('roc_pct', 0):.2f}%")
            logger.info(f"  Underwater: {summary.get('underwater_pct', 0):.1f}%")
            logger.info(f"  Bleeders: {summary.get('bleeder_count', 0)}")

            # Phase 3: Check goals and adjust strategy
            logger.info("[Phase 3] Checking goals...")
            goal_status = await self._check_goals(node_name, state)
            result.goals_checked = goal_status.get("goals_checked", 0)
            result.goals_on_track = goal_status.get("goals_on_track", 0)
            result.strategy_adjustments = goal_status.get("strategy_adjustments", [])
            logger.info(f"  Goals: {result.goals_checked} checked, {result.goals_on_track} on track")
            for adj in result.strategy_adjustments:
                logger.info(f"  Strategy adjustment: {adj}")

            # Phase 4: Scan for opportunities
            logger.info("[Phase 4] Scanning for opportunities...")
            opportunities = await self.scanner.scan_all(node_name, state)
            result.opportunities_found = len(opportunities)

            # Count by type
            for opp in opportunities:
                opp_type = opp.opportunity_type.value
                result.opportunities_by_type[opp_type] = \
                    result.opportunities_by_type.get(opp_type, 0) + 1

            logger.info(f"  Found {result.opportunities_found} opportunities")
            for opp_type, count in result.opportunities_by_type.items():
                logger.info(f"    {opp_type}: {count}")

            # Phase 5: Score with learning adjustments
            logger.info("[Phase 5] Scoring opportunities with learning adjustments...")
            scored = self._score_opportunities(opportunities, state)

            # Phase 6: Execute safe auto-actions
            logger.info("[Phase 6] Executing safe auto-actions...")
            auto_executed, skipped_budget = await self._execute_auto_actions(
                node_name, scored
            )
            result.auto_executed = [a.to_dict() for a in auto_executed]
            logger.info(f"  Auto-executed: {len(auto_executed)} actions")
            for action in auto_executed:
                logger.info(f"    ✓ {action.opportunity_type.value}: {action.description}")

            # Phase 7: Queue remaining for approval
            logger.info("[Phase 7] Queuing actions for approval...")
            queued = await self._queue_for_approval(node_name, scored, auto_executed)
            result.queued_for_review = [q.to_dict() for q in queued]
            result.skipped = [s.to_dict() for s in skipped_budget]
            logger.info(f"  Queued for review: {len(queued)}")
            for q in queued:
                logger.info(f"    → {q.opportunity_type.value}: {q.description}")
            if skipped_budget:
                logger.info(f"  Skipped (budget exhausted): {len(skipped_budget)}")

            # Phase 8: Measure past outcomes (learning)
            logger.info("[Phase 8] Measuring past outcomes for learning...")
            outcomes = self.learning_engine.measure_outcomes(
                hours_ago_min=6,
                hours_ago_max=24
            )
            result.outcomes_measured = len(outcomes)
            result.learning_summary = self.learning_engine.get_learning_summary()
            logger.info(f"  Outcomes measured: {len(outcomes)}")
            success_count = sum(1 for o in outcomes if o.success)
            if outcomes:
                logger.info(f"  Success rate: {success_count}/{len(outcomes)} ({100*success_count/len(outcomes):.0f}%)")

            # Phase 9: Plan next cycle
            logger.info("[Phase 9] Planning next cycle priorities...")
            result.next_cycle_priorities = self._plan_next_cycle(
                state, goal_status, outcomes
            )
            for priority in result.next_cycle_priorities:
                logger.info(f"  • {priority}")

            result.success = True

        except Exception as e:
            logger.error(f"Cycle failed with error: {e}", exc_info=True)
            result.errors.append(str(e))

        result.duration_seconds = time.time() - cycle_start

        # Store cycle result
        self.db.save_cycle_result(result.to_dict())

        # Final summary
        logger.info("-" * 60)
        logger.info("CYCLE COMPLETE")
        logger.info(f"  Duration: {result.duration_seconds:.1f}s")
        logger.info(f"  Success: {result.success}")
        logger.info(f"  Auto-executed: {len(result.auto_executed)}")
        logger.info(f"  Queued: {len(result.queued_for_review)}")
        logger.info(f"  Outcomes learned: {result.outcomes_measured}")
        logger.info("=" * 60)

        return result

    async def _record_snapshot(self, node_name: str) -> None:
        """Record current state snapshot for historical tracking."""
        try:
            await self.mcp.call(
                "advisor_record_snapshot",
                {"node": node_name, "snapshot_type": "hourly"}
            )
        except Exception:
            pass  # Non-critical

    async def _analyze_node_state(self, node_name: str) -> Dict[str, Any]:
        """
        Comprehensive node state analysis.

        Gathers all available data to build complete picture.
        """
        # Gather data (some may fail, that's ok)
        results = {}

        try:
            results["node_info"] = await self.mcp.call(
                "hive_node_info", {"node": node_name}
            )
        except Exception:
            results["node_info"] = {}

        try:
            results["channels"] = await self.mcp.call(
                "hive_channels", {"node": node_name}
            )
        except Exception:
            results["channels"] = {}

        try:
            results["dashboard"] = await self.mcp.call(
                "revenue_dashboard", {"node": node_name, "window_days": 30}
            )
        except Exception:
            results["dashboard"] = {}

        try:
            results["profitability"] = await self.mcp.call(
                "revenue_profitability", {"node": node_name}
            )
        except Exception:
            results["profitability"] = {}

        try:
            results["context"] = await self.mcp.call(
                "advisor_get_context_brief", {"days": 7}
            )
        except Exception:
            results["context"] = {}

        try:
            results["velocities"] = await self.mcp.call(
                "advisor_get_velocities", {"hours_threshold": 24}
            )
        except Exception:
            results["velocities"] = {}

        # Calculate summary metrics
        channels = results.get("channels", {}).get("channels", [])
        prof_list = results.get("profitability", {}).get("channels", [])
        dashboard = results.get("dashboard", {})

        total_capacity = sum(ch.get("capacity_sats", 0) for ch in channels)
        total_local = sum(ch.get("local_sats", 0) for ch in channels)
        avg_balance_ratio = total_local / total_capacity if total_capacity > 0 else 0.5

        # Profitability analysis
        underwater_count = sum(
            1 for p in prof_list
            if p.get("classification") == "underwater"
            or p.get("profitability_class") == "underwater"
        )
        profitable_count = sum(
            1 for p in prof_list
            if p.get("classification") == "profitable"
            or p.get("profitability_class") == "profitable"
        )

        total_prof = len(prof_list) if prof_list else 1
        underwater_pct = underwater_count / total_prof * 100
        profitable_pct = profitable_count / total_prof * 100

        roc_pct = dashboard.get("annualized_roc_pct", 0)
        bleeders = dashboard.get("bleeder_warnings", [])

        return {
            "summary": {
                "total_capacity_sats": total_capacity,
                "channel_count": len(channels),
                "avg_balance_ratio": round(avg_balance_ratio, 4),
                "roc_pct": roc_pct,
                "underwater_pct": round(underwater_pct, 2),
                "profitable_pct": round(profitable_pct, 2),
                "bleeder_count": len(bleeders),
            },
            "channels": channels,
            "profitability": prof_list,
            "context": results.get("context", {}),
            "velocities": results.get("velocities", {}),
            "dashboard": dashboard
        }

    async def _check_goals(
        self,
        node_name: str,
        state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check progress on active goals and adjust if needed."""
        summary = state.get("summary", {})

        # Get or create goals
        active_goals = self.goal_manager.get_active_goals()

        if not active_goals:
            # Analyze state and set new goals
            active_goals = self.goal_manager.analyze_and_set_goals(summary)

        # Check each goal's progress
        goals_on_track = 0
        strategy_adjustments = []
        progress_list = []

        for goal in active_goals:
            # Get current value for this metric
            current_value = summary.get(goal.target_metric, goal.current_value)

            # Check progress
            progress = self.goal_manager.check_progress(goal, current_value)
            progress_list.append(progress)

            if progress.on_track:
                goals_on_track += 1

            # Record checkpoint
            self.goal_manager.record_checkpoint(goal, current_value)

            # Update goal status if achieved/failed
            if goal.status != "active":
                self.goal_manager.update_goal_status(goal.goal_id, goal.status)

        # Get strategy adjustments for off-track goals
        strategy_adjustments = self.goal_manager.get_strategy_adjustments(
            active_goals, progress_list
        )

        return {
            "goals_checked": len(active_goals),
            "goals_on_track": goals_on_track,
            "all_on_track": goals_on_track == len(active_goals),
            "strategy_adjustments": strategy_adjustments,
            "progress": [
                {
                    "goal_id": p.goal_id,
                    "on_track": p.on_track,
                    "progress_pct": p.progress_pct,
                    "recommendation": p.recommendation
                }
                for p in progress_list
            ]
        }

    def _score_opportunities(
        self,
        opportunities: List[Opportunity],
        state: Dict[str, Any]
    ) -> List[Opportunity]:
        """Score opportunities with learning adjustments."""
        scored = []

        for opp in opportunities:
            # Base score from opportunity scanner
            base_score = opp.priority_score

            # Apply learning adjustments
            adjusted_confidence = self.learning_engine.get_adjusted_confidence(
                opp.confidence_score,
                opp.action_type.value,
                opp.opportunity_type.value
            )

            # Goal alignment bonus
            goal_bonus = self._calculate_goal_alignment(opp, state)

            # Final score
            final_score = base_score * (0.5 + adjusted_confidence * 0.5) * (1 + goal_bonus)

            opp.final_score = final_score
            opp.adjusted_confidence = adjusted_confidence
            opp.goal_alignment_bonus = goal_bonus

            scored.append(opp)

        # Sort by final score
        scored.sort(key=lambda x: x.final_score, reverse=True)

        return scored

    def _calculate_goal_alignment(
        self,
        opp: Opportunity,
        state: Dict[str, Any]
    ) -> float:
        """Calculate bonus for goal-aligned opportunities."""
        bonus = 0.0
        goals = self.goal_manager.get_active_goals()

        for goal in goals:
            # ROC goal: fee changes and bleeder fixes help
            if goal.target_metric == "roc_pct":
                if opp.opportunity_type in [
                    OpportunityType.BLEEDER_FIX,
                    OpportunityType.PEAK_HOUR_FEE
                ]:
                    bonus += 0.1 * goal.priority / 5

            # Underwater goal: policy changes help
            if goal.target_metric == "underwater_pct":
                if opp.opportunity_type in [
                    OpportunityType.BLEEDER_FIX,
                    OpportunityType.POLICY_CHANGE
                ]:
                    bonus += 0.1 * goal.priority / 5

            # Balance goal: rebalancing helps
            if goal.target_metric == "avg_balance_ratio":
                if opp.action_type == ActionType.REBALANCE:
                    bonus += 0.1 * goal.priority / 5

        return min(0.3, bonus)  # Cap at 30% bonus

    async def _execute_auto_actions(
        self,
        node_name: str,
        opportunities: List[Opportunity]
    ) -> Tuple[List[Opportunity], List[Opportunity]]:
        """
        Execute safe auto-actions within budget and constraints.

        Returns:
            Tuple of (executed, skipped_due_to_budget)
        """
        executed = []
        skipped = []

        # Check budget
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if self._daily_budget.date != today:
            self._daily_budget = DailyBudget(date=today)

        # Only consider high-confidence, auto-safe opportunities
        for opp in opportunities:
            if not opp.auto_execute_safe:
                continue

            if opp.adjusted_confidence < MIN_AUTO_EXECUTE_CONFIDENCE:
                continue

            # Check budget
            if opp.action_type == ActionType.FEE_CHANGE:
                if not self._daily_budget.can_change_fee():
                    skipped.append(opp)
                    continue
            elif opp.action_type == ActionType.REBALANCE:
                if not self._daily_budget.can_rebalance():
                    skipped.append(opp)
                    continue

            # Execute the action
            success = await self._execute_action(node_name, opp)

            if success:
                executed.append(opp)

                # Update budget
                if opp.action_type == ActionType.FEE_CHANGE:
                    self._daily_budget.record_fee_change()
                elif opp.action_type == ActionType.REBALANCE:
                    self._daily_budget.record_rebalance(100)  # Estimate fee

                # Record decision
                await self._record_decision(node_name, opp, "auto_executed")

        # Save budget
        self._save_budget()

        return executed, skipped

    async def _execute_action(
        self,
        node_name: str,
        opp: Opportunity
    ) -> bool:
        """Execute a single action."""
        try:
            if opp.action_type == ActionType.FEE_CHANGE:
                return await self._execute_fee_change(node_name, opp)
            elif opp.action_type == ActionType.REBALANCE:
                return await self._execute_rebalance(node_name, opp)
            elif opp.action_type == ActionType.CONFIG_CHANGE:
                return await self._execute_config_change(node_name, opp)
            else:
                return False
        except Exception:
            return False

    async def _execute_fee_change(
        self,
        node_name: str,
        opp: Opportunity
    ) -> bool:
        """Execute a fee change with conservative bounds."""
        if not opp.channel_id:
            return False

        # Get current fee
        current_state = opp.current_state
        current_fee = current_state.get("fee_ppm", 0)

        if current_fee == 0:
            return False

        # Calculate new fee based on opportunity type
        if opp.opportunity_type == OpportunityType.PEAK_HOUR_FEE:
            # Increase by up to 15%
            new_fee = int(current_fee * 1.15)
        elif opp.opportunity_type == OpportunityType.LOW_HOUR_FEE:
            # Decrease by up to 10%
            new_fee = int(current_fee * 0.90)
        elif opp.opportunity_type == OpportunityType.STAGNANT_CHANNEL:
            # Significant decrease to attract flow
            new_fee = max(50, int(current_fee * 0.7))
        elif opp.opportunity_type == OpportunityType.CRITICAL_SATURATION:
            # Decrease to push flow out
            new_fee = max(50, int(current_fee * 0.8))
        else:
            return False

        # Apply bounds
        new_fee = max(SAFETY_CONSTRAINTS["absolute_min_fee_ppm"], new_fee)
        new_fee = min(SAFETY_CONSTRAINTS["absolute_max_fee_ppm"], new_fee)

        # Don't change if delta is too small
        if abs(new_fee - current_fee) < 5:
            return False

        # Execute via revenue-ops (with temporary override)
        try:
            result = await self.mcp.call(
                "revenue_set_fee",
                {
                    "node": node_name,
                    "channel_id": opp.channel_id,
                    "fee_ppm": new_fee
                }
            )
            return result.get("success", False)
        except Exception:
            return False

    async def _execute_rebalance(
        self,
        node_name: str,
        opp: Opportunity
    ) -> bool:
        """Execute a rebalance (very conservative - mostly queue for review)."""
        # For now, we don't auto-execute rebalances - they're expensive
        # Just record the recommendation
        return False

    async def _execute_config_change(
        self,
        node_name: str,
        opp: Opportunity
    ) -> bool:
        """Execute a config change (very conservative)."""
        # Config changes are not auto-executed
        return False

    async def _queue_for_approval(
        self,
        node_name: str,
        opportunities: List[Opportunity],
        already_executed: List[Opportunity]
    ) -> List[Opportunity]:
        """Queue opportunities that need human review."""
        queued = []
        executed_ids = {id(o) for o in already_executed}

        for opp in opportunities:
            if id(opp) in executed_ids:
                continue

            # Skip very low confidence
            if opp.adjusted_confidence < SAFETY_CONSTRAINTS["min_confidence_for_queue"]:
                continue

            # Queue for review
            queued.append(opp)
            await self._record_decision(node_name, opp, "queued_for_review")

        return queued

    async def _record_decision(
        self,
        node_name: str,
        opp: Opportunity,
        status: str
    ) -> None:
        """Record a decision to the audit trail."""
        try:
            await self.mcp.call(
                "advisor_record_decision",
                {
                    "decision_type": opp.action_type.value,
                    "node": node_name,
                    "recommendation": opp.recommended_action,
                    "reasoning": opp.reasoning,
                    "channel_id": opp.channel_id,
                    "peer_id": opp.peer_id,
                    "confidence": opp.adjusted_confidence
                }
            )
        except Exception:
            pass  # Non-critical

    def _plan_next_cycle(
        self,
        state: Dict[str, Any],
        goal_status: Dict[str, Any],
        outcomes: List[ActionOutcome]
    ) -> List[str]:
        """Plan priorities for the next 3-hour cycle."""
        priorities = []
        summary = state.get("summary", {})

        # Based on goal progress
        if not goal_status.get("all_on_track"):
            for adj in goal_status.get("strategy_adjustments", []):
                priorities.append(f"STRATEGY: {adj}")

        # Based on current state
        if summary.get("underwater_pct", 0) > 40:
            priorities.append("FOCUS: Address underwater channels")

        if summary.get("bleeder_count", 0) > 5:
            priorities.append("URGENT: Fix bleeder channels")

        if summary.get("avg_balance_ratio", 0.5) < 0.3:
            priorities.append("BALANCE: Many channels depleted - prioritize inbound")
        elif summary.get("avg_balance_ratio", 0.5) > 0.7:
            priorities.append("BALANCE: Many channels saturated - attract outbound")

        # Based on recent outcomes
        if outcomes:
            recent_failures = [o for o in outcomes if not o.success]
            if len(recent_failures) > len(outcomes) * 0.5:
                priorities.append(
                    "CAUTION: High failure rate - increase confidence thresholds"
                )

        # Based on learning
        recommendations = self.learning_engine.get_action_type_recommendations()
        for rec in recommendations:
            if rec.get("severity") == "critical":
                priorities.append(f"LEARN: {rec.get('recommendation')}")

        # Default
        if not priorities:
            priorities.append("NORMAL: Continue balanced optimization")

        return priorities[:5]  # Limit to top 5

    async def get_status(self, node_name: str) -> Dict[str, Any]:
        """Get current advisor status for a node."""
        # Get goals
        goals = self.goal_manager.get_active_goals()
        goals_summary = self.goal_manager.get_goals_summary()

        # Get learning status
        learning = self.learning_engine.get_learning_summary()

        # Get recent cycle
        recent_cycles = self.db.get_recent_cycles(node_name, limit=1)

        # Get budget
        budget = {
            "date": self._daily_budget.date,
            "fee_changes_used": self._daily_budget.fee_changes_used,
            "fee_changes_remaining": DAILY_FEE_CHANGE_BUDGET - self._daily_budget.fee_changes_used,
            "rebalances_used": self._daily_budget.rebalances_used,
            "rebalances_remaining": DAILY_REBALANCE_BUDGET - self._daily_budget.rebalances_used
        }

        return {
            "node": node_name,
            "active_goals": goals_summary.get("total_active", 0),
            "goals": [g.to_dict() for g in goals],
            "learning_summary": learning,
            "last_cycle": recent_cycles[0] if recent_cycles else None,
            "daily_budget": budget,
            "constraints": SAFETY_CONSTRAINTS
        }
