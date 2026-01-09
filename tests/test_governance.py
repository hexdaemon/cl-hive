"""
Tests for the Governance module (Phase 7).

Tests cover:
- ADVISOR mode: Queuing actions for manual approval
- AUTONOMOUS mode: Budget caps and rate limits
- ORACLE mode: External API with fallback
- Fail-closed behavior on errors
"""

import json
import time
import pytest
from unittest.mock import MagicMock, patch, Mock
from dataclasses import dataclass

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.governance import (
    DecisionEngine,
    GovernanceMode,
    DecisionResult,
    DecisionPacket,
    DecisionResponse,
    ORACLE_RETRY_COUNT,
    ORACLE_RETRY_DELAY_SECONDS,
)


# =============================================================================
# FIXTURES
# =============================================================================

@dataclass
class MockConfig:
    """Mock config snapshot for testing."""
    governance_mode: str = 'advisor'
    autonomous_budget_per_day: int = 10_000_000
    autonomous_actions_per_hour: int = 2
    oracle_url: str = None
    oracle_timeout_seconds: int = 5


@pytest.fixture
def mock_database():
    """Create a mock database."""
    db = MagicMock()
    db.add_pending_action.return_value = 1
    return db


@pytest.fixture
def mock_plugin():
    """Create a mock plugin."""
    plugin = MagicMock()
    return plugin


@pytest.fixture
def engine(mock_database, mock_plugin):
    """Create a DecisionEngine instance."""
    return DecisionEngine(database=mock_database, plugin=mock_plugin)


@pytest.fixture
def mock_config():
    """Create a mock config."""
    return MockConfig()


# =============================================================================
# ADVISOR MODE TESTS
# =============================================================================

class TestAdvisorMode:
    """Tests for ADVISOR mode (human in the loop)."""

    def test_advisor_mode_queues_action(self, engine, mock_database, mock_config):
        """ADVISOR mode should queue action for manual approval."""
        mock_config.governance_mode = 'advisor'

        response = engine.propose_action(
            action_type='channel_open',
            target='02' + 'a' * 64,
            context={'amount_sats': 1_000_000},
            cfg=mock_config
        )

        assert response.result == DecisionResult.QUEUED
        assert response.action_id == 1
        assert 'manual approval' in response.reason.lower()

        # Verify DB call
        mock_database.add_pending_action.assert_called_once()

    def test_advisor_mode_saves_correct_payload(self, engine, mock_database, mock_config):
        """ADVISOR mode should save action details in payload."""
        mock_config.governance_mode = 'advisor'
        target = '02' + 'b' * 64
        context = {'amount_sats': 500_000, 'hive_share': 0.05}

        engine.propose_action(
            action_type='channel_open',
            target=target,
            context=context,
            cfg=mock_config
        )

        call_args = mock_database.add_pending_action.call_args
        assert call_args[1]['action_type'] == 'channel_open'
        payload = call_args[1]['payload']
        assert payload['target'] == target
        assert payload['context'] == context


# =============================================================================
# AUTONOMOUS MODE TESTS
# =============================================================================

class TestAutonomousMode:
    """Tests for AUTONOMOUS mode (algorithmic execution)."""

    def test_autonomous_executes_within_limits(self, engine, mock_config):
        """AUTONOMOUS mode should execute when within limits."""
        mock_config.governance_mode = 'autonomous'

        # Register an executor
        executor = MagicMock()
        engine.register_executor('channel_open', executor)

        response = engine.propose_action(
            action_type='channel_open',
            target='02' + 'c' * 64,
            context={'amount_sats': 1_000_000},
            cfg=mock_config
        )

        assert response.result == DecisionResult.APPROVED
        executor.assert_called_once()

    def test_autonomous_budget_exceeded_queues(self, engine, mock_database, mock_config):
        """AUTONOMOUS mode should queue when daily budget exceeded."""
        mock_config.governance_mode = 'autonomous'
        mock_config.autonomous_budget_per_day = 1_000_000

        executor = MagicMock()
        engine.register_executor('channel_open', executor)

        # First action - within budget
        engine.propose_action(
            action_type='channel_open',
            target='02' + 'd' * 64,
            context={'amount_sats': 800_000},
            cfg=mock_config
        )

        # Second action - exceeds budget
        response = engine.propose_action(
            action_type='channel_open',
            target='02' + 'e' * 64,
            context={'amount_sats': 500_000},
            cfg=mock_config
        )

        assert response.result == DecisionResult.QUEUED
        mock_database.add_pending_action.assert_called()

    def test_autonomous_rate_limit_exceeded_queues(self, engine, mock_database, mock_config):
        """AUTONOMOUS mode should queue when hourly rate limit exceeded."""
        mock_config.governance_mode = 'autonomous'
        mock_config.autonomous_actions_per_hour = 2

        executor = MagicMock()
        engine.register_executor('channel_open', executor)

        # Execute 2 actions (at limit)
        for i in range(2):
            engine.propose_action(
                action_type='channel_open',
                target=f'02{chr(ord("f") + i)}' + 'x' * 62,
                context={'amount_sats': 100_000},
                cfg=mock_config
            )

        # Third action should be queued
        response = engine.propose_action(
            action_type='channel_open',
            target='02' + 'z' * 64,
            context={'amount_sats': 100_000},
            cfg=mock_config
        )

        assert response.result == DecisionResult.QUEUED

    def test_autonomous_budget_resets_daily(self, engine, mock_config):
        """AUTONOMOUS mode budget should reset at midnight UTC."""
        mock_config.governance_mode = 'autonomous'
        mock_config.autonomous_budget_per_day = 1_000_000

        executor = MagicMock()
        engine.register_executor('channel_open', executor)

        # Spend some budget
        engine._daily_spend_sats = 900_000
        engine._daily_spend_reset_day = int(time.time() // 86400) - 1  # Yesterday

        # Action should succeed (budget reset)
        response = engine.propose_action(
            action_type='channel_open',
            target='02' + 'g' * 64,
            context={'amount_sats': 500_000},
            cfg=mock_config
        )

        assert response.result == DecisionResult.APPROVED
        assert engine._daily_spend_sats == 500_000  # Reset + new spend

    def test_autonomous_no_executor_queues(self, engine, mock_database, mock_config):
        """AUTONOMOUS mode should queue if no executor registered."""
        mock_config.governance_mode = 'autonomous'

        # No executor registered for 'rebalance'
        response = engine.propose_action(
            action_type='rebalance',
            target='02' + 'h' * 64,
            context={'amount_sats': 100_000},
            cfg=mock_config
        )

        assert response.result == DecisionResult.QUEUED


# =============================================================================
# ORACLE MODE TESTS
# =============================================================================

class TestOracleMode:
    """Tests for ORACLE mode (external API delegation)."""

    def test_oracle_approve_executes(self, engine, mock_config):
        """ORACLE mode should execute when oracle approves."""
        mock_config.governance_mode = 'oracle'
        mock_config.oracle_url = 'http://oracle.example.com/decide'

        executor = MagicMock()
        engine.register_executor('channel_open', executor)

        # Mock successful oracle response
        with patch.object(engine, '_query_oracle') as mock_query:
            mock_query.return_value = {'decision': 'APPROVE', 'reason': 'Good target'}

            response = engine.propose_action(
                action_type='channel_open',
                target='02' + 'i' * 64,
                context={'amount_sats': 1_000_000},
                cfg=mock_config
            )

        assert response.result == DecisionResult.APPROVED
        assert 'oracle' in response.reason.lower()
        executor.assert_called_once()

    def test_oracle_deny_rejects(self, engine, mock_config):
        """ORACLE mode should reject when oracle denies."""
        mock_config.governance_mode = 'oracle'
        mock_config.oracle_url = 'http://oracle.example.com/decide'

        executor = MagicMock()
        engine.register_executor('channel_open', executor)

        with patch.object(engine, '_query_oracle') as mock_query:
            mock_query.return_value = {'decision': 'DENY', 'reason': 'Target saturated'}

            response = engine.propose_action(
                action_type='channel_open',
                target='02' + 'j' * 64,
                context={'amount_sats': 1_000_000},
                cfg=mock_config
            )

        assert response.result == DecisionResult.DENIED
        assert 'saturated' in response.reason.lower()
        executor.assert_not_called()

    def test_oracle_timeout_falls_back_to_advisor(self, engine, mock_database, mock_config):
        """ORACLE mode should fall back to ADVISOR on timeout."""
        mock_config.governance_mode = 'oracle'
        mock_config.oracle_url = 'http://oracle.example.com/decide'

        with patch.object(engine, '_query_oracle') as mock_query:
            mock_query.return_value = None  # Timeout/failure

            response = engine.propose_action(
                action_type='channel_open',
                target='02' + 'k' * 64,
                context={'amount_sats': 1_000_000},
                cfg=mock_config
            )

        assert response.result == DecisionResult.QUEUED
        mock_database.add_pending_action.assert_called()

    def test_oracle_malformed_response_falls_back(self, engine, mock_database, mock_config):
        """ORACLE mode should fall back on malformed response."""
        mock_config.governance_mode = 'oracle'
        mock_config.oracle_url = 'http://oracle.example.com/decide'

        with patch.object(engine, '_query_oracle') as mock_query:
            mock_query.return_value = {'invalid': 'response'}  # Missing 'decision'

            response = engine.propose_action(
                action_type='channel_open',
                target='02' + 'l' * 64,
                context={'amount_sats': 1_000_000},
                cfg=mock_config
            )

        assert response.result == DecisionResult.QUEUED

    def test_oracle_no_url_falls_back(self, engine, mock_database, mock_config):
        """ORACLE mode should fall back if no URL configured."""
        mock_config.governance_mode = 'oracle'
        mock_config.oracle_url = None

        response = engine.propose_action(
            action_type='channel_open',
            target='02' + 'm' * 64,
            context={'amount_sats': 1_000_000},
            cfg=mock_config
        )

        assert response.result == DecisionResult.QUEUED


# =============================================================================
# FAIL-CLOSED BEHAVIOR TESTS
# =============================================================================

class TestFailClosedBehavior:
    """Tests for fail-closed behavior (GEMINI.md Rule #3)."""

    def test_unknown_mode_falls_back(self, engine, mock_database, mock_config):
        """Unknown governance mode should fall back to ADVISOR."""
        mock_config.governance_mode = 'unknown'

        # This will raise ValueError when creating GovernanceMode enum
        # The engine should catch this and fall back
        response = engine.propose_action(
            action_type='channel_open',
            target='02' + 'n' * 64,
            context={'amount_sats': 1_000_000},
            cfg=mock_config
        )

        assert response.result == DecisionResult.QUEUED

    def test_executor_exception_falls_back(self, engine, mock_database, mock_config):
        """Executor exception should fall back to ADVISOR."""
        mock_config.governance_mode = 'autonomous'

        # Register failing executor
        def failing_executor(target, context):
            raise Exception("RPC failure")

        engine.register_executor('channel_open', failing_executor)

        response = engine.propose_action(
            action_type='channel_open',
            target='02' + 'o' * 64,
            context={'amount_sats': 100_000},
            cfg=mock_config
        )

        assert response.result == DecisionResult.QUEUED

    def test_database_failure_in_advisor_mode(self, engine, mock_database, mock_config):
        """Database failure in ADVISOR mode should still return response."""
        mock_config.governance_mode = 'advisor'
        mock_database.add_pending_action.side_effect = Exception("DB error")

        # Should raise exception (can't queue)
        with pytest.raises(Exception):
            engine.propose_action(
                action_type='channel_open',
                target='02' + 'p' * 64,
                context={'amount_sats': 1_000_000},
                cfg=mock_config
            )


# =============================================================================
# DECISION PACKET TESTS
# =============================================================================

class TestDecisionPacket:
    """Tests for DecisionPacket serialization."""

    def test_packet_to_json(self):
        """DecisionPacket should serialize to valid JSON."""
        packet = DecisionPacket(
            action_type='channel_open',
            target='02abc123',
            context={'hive_share': 0.05, 'capacity': 1000000},
            timestamp=1234567890
        )

        json_str = packet.to_json()
        parsed = json.loads(json_str)

        assert parsed['action_type'] == 'channel_open'
        assert parsed['target'] == '02abc123'
        assert parsed['context']['hive_share'] == 0.05
        assert parsed['timestamp'] == 1234567890


# =============================================================================
# STATISTICS TESTS
# =============================================================================

class TestStatistics:
    """Tests for governance statistics."""

    def test_get_stats(self, engine):
        """get_stats should return current state."""
        engine._daily_spend_sats = 5_000_000
        engine._hourly_actions = [int(time.time()) - 100, int(time.time()) - 200]
        engine.register_executor('channel_open', lambda t, c: None)

        stats = engine.get_stats()

        assert stats['daily_spend_sats'] == 5_000_000
        assert stats['hourly_action_count'] == 2
        assert 'channel_open' in stats['registered_executors']

    def test_reset_limits(self, engine):
        """reset_limits should clear all tracking."""
        engine._daily_spend_sats = 5_000_000
        engine._hourly_actions = [123, 456]

        engine.reset_limits()

        assert engine._daily_spend_sats == 0
        assert engine._hourly_actions == []
