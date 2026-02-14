"""
Tests for Proactive AI Advisor

Tests the goal manager, learning engine, opportunity scanner, and main advisor.
"""

import json
import os
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Add tools directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

from advisor_db import AdvisorDB
from goal_manager import GoalManager, Goal, GoalProgress, GOAL_TEMPLATES
from learning_engine import LearningEngine, ActionOutcome, LearnedParameters
from opportunity_scanner import (
    OpportunityScanner,
    Opportunity,
    OpportunityType,
    ActionType,
    ActionClassification,
    SAFETY_CONSTRAINTS
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    db = AdvisorDB(db_path)
    yield db

    # Cleanup
    try:
        os.unlink(db_path)
    except Exception:
        pass


@pytest.fixture
def goal_manager(temp_db):
    """Create a GoalManager with temp database."""
    return GoalManager(temp_db)


@pytest.fixture
def learning_engine(temp_db):
    """Create a LearningEngine with temp database."""
    return LearningEngine(temp_db)


@pytest.fixture
def mock_mcp_client():
    """Create a mock MCP client."""
    client = MagicMock()
    client.call = AsyncMock()
    return client


@pytest.fixture
def opportunity_scanner(mock_mcp_client, temp_db):
    """Create an OpportunityScanner with mock client."""
    return OpportunityScanner(mock_mcp_client, temp_db)


# =============================================================================
# Goal Manager Tests
# =============================================================================

class TestGoalManager:
    """Tests for GoalManager."""

    def test_analyze_and_set_goals_low_roc(self, goal_manager):
        """Test goal creation for low ROC."""
        state = {
            "roc_pct": 0.2,
            "underwater_pct": 25,
            "profitable_pct": 60,
            "avg_balance_ratio": 0.5,
            "bleeder_count": 2
        }

        goals = goal_manager.analyze_and_set_goals(state)

        # Should create ROC goal since ROC < 0.5%
        roc_goals = [g for g in goals if g.target_metric == "roc_pct"]
        assert len(roc_goals) == 1
        assert roc_goals[0].target_value <= 0.5  # Target is min(0.5, current*2)

    def test_analyze_and_set_goals_high_underwater(self, goal_manager):
        """Test goal creation for high underwater percentage."""
        state = {
            "roc_pct": 0.6,
            "underwater_pct": 45,  # > 30%
            "profitable_pct": 40,
            "avg_balance_ratio": 0.5,
            "bleeder_count": 2
        }

        goals = goal_manager.analyze_and_set_goals(state)

        # Should create underwater goal
        underwater_goals = [g for g in goals if g.target_metric == "underwater_pct"]
        assert len(underwater_goals) == 1
        assert underwater_goals[0].target_value < 45  # Target is lower

    def test_analyze_and_set_goals_many_bleeders(self, goal_manager):
        """Test goal creation for many bleeders."""
        state = {
            "roc_pct": 0.6,
            "underwater_pct": 25,
            "profitable_pct": 60,
            "avg_balance_ratio": 0.5,
            "bleeder_count": 8  # > 5
        }

        goals = goal_manager.analyze_and_set_goals(state)

        # Should create bleeder goal
        bleeder_goals = [g for g in goals if g.target_metric == "bleeder_count"]
        assert len(bleeder_goals) == 1
        assert bleeder_goals[0].target_value < 8

    def test_check_progress_on_track(self, goal_manager, temp_db):
        """Test progress checking for on-track goal."""
        # Create a goal that started 5 days ago with 30-day deadline
        now = int(time.time())
        five_days_ago = now - (5 * 86400)

        goal = Goal(
            goal_id="test_goal",
            goal_type="profitability",
            target_metric="roc_pct",
            current_value=0.2,
            target_value=0.5,
            deadline_days=30,
            created_at=five_days_ago,
            priority=5,
            checkpoints=[],
            status="active"
        )

        # Progress: started at 0.2, need to reach 0.5 (+0.3)
        # After 5 days (~17%), should have made ~17% progress (~0.05)
        # Current value 0.25 = +0.05, which is ~17% of 0.3
        current_value = 0.25  # On track

        progress = goal_manager.check_progress(goal, current_value)

        assert progress.on_track
        assert progress.progress_pct > 0

    def test_check_progress_behind(self, goal_manager):
        """Test progress checking for behind-schedule goal."""
        now = int(time.time())
        fifteen_days_ago = now - (15 * 86400)

        goal = Goal(
            goal_id="test_goal",
            goal_type="profitability",
            target_metric="roc_pct",
            current_value=0.2,
            target_value=0.5,
            deadline_days=30,
            created_at=fifteen_days_ago,
            priority=5,
            checkpoints=[],
            status="active"
        )

        # Halfway through time (15 days), should have ~50% progress
        # But we're still at 0.2 (0% progress)
        current_value = 0.2

        progress = goal_manager.check_progress(goal, current_value)

        assert not progress.on_track
        assert "behind" in progress.recommendation.lower()

    def test_check_progress_achieved(self, goal_manager):
        """Test progress checking for achieved goal."""
        now = int(time.time())
        ten_days_ago = now - (10 * 86400)

        goal = Goal(
            goal_id="test_goal",
            goal_type="profitability",
            target_metric="roc_pct",
            current_value=0.2,
            target_value=0.5,
            deadline_days=30,
            created_at=ten_days_ago,
            priority=5,
            checkpoints=[],
            status="active"
        )

        # Already at target
        current_value = 0.55

        progress = goal_manager.check_progress(goal, current_value)

        assert progress.progress_pct >= 100
        assert goal.status == "achieved"

    def test_create_custom_goal(self, goal_manager, temp_db):
        """Test custom goal creation."""
        goal = goal_manager.create_custom_goal(
            goal_type="channel_health",
            target_metric="avg_balance_ratio",
            current_value=0.3,
            target_value=0.5,
            deadline_days=14,
            priority=4
        )

        assert goal.goal_type == "channel_health"
        assert goal.target_metric == "avg_balance_ratio"
        assert goal.target_value == 0.5
        assert goal.priority == 4

        # Should be saved to database
        retrieved = temp_db.get_goal(goal.goal_id)
        assert retrieved is not None
        assert retrieved["target_value"] == 0.5


# =============================================================================
# Learning Engine Tests
# =============================================================================

class TestLearningEngine:
    """Tests for LearningEngine."""

    def test_get_adjusted_confidence_default(self, learning_engine):
        """Test confidence adjustment with default parameters."""
        # With default parameters (all 1.0), adjustment should be minimal
        adjusted = learning_engine.get_adjusted_confidence(
            base_confidence=0.7,
            action_type="fee_change",
            opportunity_type="unknown"
        )

        # Should be close to base * 1.0 * (0.5 + 0.5 * 0.5) = 0.7 * 0.75 = 0.525
        assert 0.4 < adjusted < 0.8

    def test_get_adjusted_confidence_learned(self, learning_engine, temp_db):
        """Test confidence adjustment with learned parameters."""
        # Set learned parameters
        learning_engine._params.action_type_confidence["fee_change"] = 1.3
        learning_engine._params.opportunity_success_rates["peak_hour_fee"] = 0.8

        adjusted = learning_engine.get_adjusted_confidence(
            base_confidence=0.7,
            action_type="fee_change",
            opportunity_type="peak_hour_fee"
        )

        # With high multipliers, should be higher
        assert adjusted > 0.6

    def test_should_skip_action_low_success(self, learning_engine):
        """Test skipping actions with low success rate."""
        learning_engine._params.opportunity_success_rates["bad_opportunity"] = 0.1

        should_skip, reason = learning_engine.should_skip_action(
            action_type="fee_change",
            opportunity_type="bad_opportunity",
            base_confidence=0.5
        )

        assert should_skip
        assert "success rate" in reason.lower()

    def test_should_skip_action_good_success(self, learning_engine):
        """Test not skipping actions with good success rate."""
        learning_engine._params.opportunity_success_rates["good_opportunity"] = 0.8

        should_skip, reason = learning_engine.should_skip_action(
            action_type="fee_change",
            opportunity_type="good_opportunity",
            base_confidence=0.7
        )

        assert not should_skip

    def test_get_learning_summary(self, learning_engine):
        """Test getting learning summary."""
        summary = learning_engine.get_learning_summary()

        assert "action_type_confidence" in summary
        assert "opportunity_success_rates" in summary
        assert "total_outcomes_measured" in summary
        assert "overall_success_rate" in summary


# =============================================================================
# Opportunity Scanner Tests
# =============================================================================

class TestOpportunityScanner:
    """Tests for OpportunityScanner."""

    def test_scan_velocity_alerts(self, opportunity_scanner):
        """Test scanning for velocity alerts."""
        import asyncio

        state = {
            "velocities": {
                "channels": [
                    {
                        "channel_id": "123x1x0",
                        "trend": "depleting",
                        "hours_until_depleted": 6,
                        "velocity_pct_per_hour": -2.0,
                        "current_balance_ratio": 0.15,
                        "confidence": 0.8
                    }
                ]
            }
        }

        opportunities = asyncio.new_event_loop().run_until_complete(
            opportunity_scanner._scan_velocity_alerts("test-node", state)
        )

        assert len(opportunities) == 1
        assert opportunities[0].opportunity_type == OpportunityType.CRITICAL_DEPLETION
        assert opportunities[0].priority_score > 0.8  # High urgency for 6h depletion

    def test_scan_profitability_bleeders(self, opportunity_scanner):
        """Test scanning for bleeder channels."""
        import asyncio

        state = {
            "profitability": [],
            "dashboard": {
                "bleeder_warnings": [
                    {
                        "channel_id": "456x1x0",
                        "peer_id": "02abc...",
                        "estimated_loss_sats": 500
                    }
                ]
            }
        }

        opportunities = asyncio.new_event_loop().run_until_complete(
            opportunity_scanner._scan_profitability("test-node", state)
        )

        bleeder_opps = [
            o for o in opportunities
            if o.opportunity_type == OpportunityType.BLEEDER_FIX
        ]
        assert len(bleeder_opps) == 1

    def test_scan_imbalanced_channels(self, opportunity_scanner):
        """Test scanning for imbalanced channels."""
        import asyncio

        state = {
            "channels": [
                {
                    "short_channel_id": "789x1x0",
                    "peer_id": "02def...",
                    "to_us_msat": 100000000,  # 100k sats
                    "total_msat": 1000000000  # 1M sats = 10% local
                }
            ]
        }

        opportunities = asyncio.new_event_loop().run_until_complete(
            opportunity_scanner._scan_imbalanced_channels("test-node", state)
        )

        assert len(opportunities) == 1
        assert opportunities[0].opportunity_type == OpportunityType.IMBALANCED_CHANNEL

    def test_classify_opportunity_auto_execute(self, opportunity_scanner):
        """Test classification of auto-execute opportunities."""
        opp = Opportunity(
            opportunity_type=OpportunityType.PEAK_HOUR_FEE,
            action_type=ActionType.FEE_CHANGE,
            channel_id="123x1x0",
            peer_id=None,
            node_name="test",
            priority_score=0.7,
            confidence_score=0.85,
            roi_estimate=0.5,
            description="Test",
            reasoning="Test",
            recommended_action="Test",
            predicted_benefit=100,
            classification=ActionClassification.AUTO_EXECUTE,
            auto_execute_safe=True
        )

        classification = opportunity_scanner.classify_opportunity(opp)
        assert classification == ActionClassification.AUTO_EXECUTE

    def test_classify_opportunity_channel_open(self, opportunity_scanner):
        """Test that channel opens always require approval."""
        opp = Opportunity(
            opportunity_type=OpportunityType.CHANNEL_OPEN,
            action_type=ActionType.CHANNEL_OPEN,
            channel_id=None,
            peer_id="02abc...",
            node_name="test",
            priority_score=0.9,
            confidence_score=0.95,
            roi_estimate=0.8,
            description="Test",
            reasoning="Test",
            recommended_action="Test",
            predicted_benefit=10000,
            classification=ActionClassification.REQUIRE_APPROVAL,
            auto_execute_safe=False
        )

        classification = opportunity_scanner.classify_opportunity(opp)
        assert classification == ActionClassification.REQUIRE_APPROVAL


# =============================================================================
# Database Schema Tests
# =============================================================================

class TestAdvisorDBSchema:
    """Tests for new database schema additions."""

    def test_save_and_get_goal(self, temp_db):
        """Test saving and retrieving goals."""
        goal = {
            "goal_id": "test_goal_1",
            "goal_type": "profitability",
            "target_metric": "roc_pct",
            "current_value": 0.2,
            "target_value": 0.5,
            "deadline_days": 30,
            "created_at": int(time.time()),
            "priority": 5,
            "checkpoints": [{"timestamp": int(time.time()), "value": 0.25}],
            "status": "active"
        }

        temp_db.save_goal(goal)
        retrieved = temp_db.get_goal("test_goal_1")

        assert retrieved is not None
        assert retrieved["target_value"] == 0.5
        assert len(retrieved["checkpoints"]) == 1

    def test_get_goals_by_status(self, temp_db):
        """Test filtering goals by status."""
        # Create goals with different statuses
        for i, status in enumerate(["active", "active", "achieved", "failed"]):
            goal = {
                "goal_id": f"goal_{i}",
                "goal_type": "profitability",
                "target_metric": "roc_pct",
                "current_value": 0.2,
                "target_value": 0.5,
                "deadline_days": 30,
                "created_at": int(time.time()),
                "priority": 3,
                "checkpoints": [],
                "status": status
            }
            temp_db.save_goal(goal)

        active_goals = temp_db.get_goals(status="active")
        assert len(active_goals) == 2

        all_goals = temp_db.get_goals()
        assert len(all_goals) == 4

    def test_save_and_get_learning_params(self, temp_db):
        """Test saving and retrieving learning parameters."""
        params = {
            "action_type_confidence": {"fee_change": 1.2, "rebalance": 0.9},
            "opportunity_success_rates": {"peak_hour_fee": 0.75},
            "total_outcomes_measured": 10,
            "overall_success_rate": 0.7
        }

        temp_db.save_learning_params(params)
        retrieved = temp_db.get_learning_params()

        assert retrieved["action_type_confidence"]["fee_change"] == 1.2
        assert retrieved["opportunity_success_rates"]["peak_hour_fee"] == 0.75

    def test_record_action_outcome(self, temp_db):
        """Test recording action outcomes."""
        outcome = {
            "action_id": 1,
            "action_type": "fee_change",
            "opportunity_type": "peak_hour_fee",
            "channel_id": "123x1x0",
            "node_name": "test-node",
            "decision_confidence": 0.8,
            "predicted_benefit": 100,
            "actual_benefit": 120,
            "success": True,
            "prediction_error": 0.2
        }

        outcome_id = temp_db.record_action_outcome(outcome)
        assert outcome_id > 0

    def test_save_and_get_cycle_result(self, temp_db):
        """Test saving and retrieving cycle results."""
        cycle = {
            "cycle_id": "test-node_12345",
            "node_name": "test-node",
            "timestamp": datetime.now().isoformat(),
            "duration_seconds": 5.2,
            "opportunities_found": 10,
            "auto_executed_count": 2,
            "queued_count": 5,
            "outcomes_measured": 3,
            "success": True
        }

        temp_db.save_cycle_result(cycle)
        cycles = temp_db.get_recent_cycles("test-node", limit=1)

        assert len(cycles) == 1
        assert cycles[0]["opportunities_found"] == 10

    def test_daily_budget(self, temp_db):
        """Test daily budget tracking."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        budget = {
            "fee_changes_used": 5,
            "rebalances_used": 2,
            "rebalance_fees_spent_sats": 1500
        }

        temp_db.save_daily_budget(today, budget)
        retrieved = temp_db.get_daily_budget(today)

        assert retrieved is not None
        assert retrieved["fee_changes_used"] == 5
        assert retrieved["rebalances_used"] == 2


# =============================================================================
# Safety Constraint Tests
# =============================================================================

class TestSafetyConstraints:
    """Tests for safety constraints."""

    def test_fee_bounds(self):
        """Test fee bounds are sensible."""
        assert SAFETY_CONSTRAINTS["absolute_min_fee_ppm"] >= 1
        assert SAFETY_CONSTRAINTS["absolute_max_fee_ppm"] <= 10000
        assert SAFETY_CONSTRAINTS["absolute_min_fee_ppm"] < SAFETY_CONSTRAINTS["absolute_max_fee_ppm"]

    def test_rebalance_limits(self):
        """Test rebalance limits are sensible."""
        assert SAFETY_CONSTRAINTS["max_rebalance_cost_pct"] <= 5.0
        assert SAFETY_CONSTRAINTS["max_single_rebalance_sats"] <= 2_000_000

    def test_onchain_reserve(self):
        """Test on-chain reserve is maintained."""
        assert SAFETY_CONSTRAINTS["min_onchain_sats"] >= 100_000

    def test_confidence_thresholds(self):
        """Test confidence thresholds are reasonable."""
        assert 0.5 <= SAFETY_CONSTRAINTS["min_confidence_auto_execute"] <= 1.0
        assert SAFETY_CONSTRAINTS["min_confidence_for_queue"] < SAFETY_CONSTRAINTS["min_confidence_auto_execute"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
