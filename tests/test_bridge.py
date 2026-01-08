"""
Test Suite for cl-hive Integration Bridge.

Tests the Circuit Breaker pattern, feature detection,
and integration methods for cl-revenue-ops and CLBoss.

Author: Lightning Goats Team
"""

import pytest
import time
from unittest.mock import Mock, MagicMock, patch

# Add parent to path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyln.client import RpcError

from modules.bridge import (
    Bridge,
    CircuitBreaker,
    CircuitState,
    BridgeStatus,
    CircuitOpenError,
    BridgeDisabledError,
    VersionMismatchError,
    MAX_FAILURES,
    RESET_TIMEOUT,
    MIN_REVENUE_OPS_VERSION
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def mock_rpc():
    """Create a mock RPC proxy."""
    rpc = MagicMock()
    return rpc


@pytest.fixture
def mock_plugin():
    """Create a mock plugin for logging."""
    plugin = Mock()
    plugin.log = Mock()
    return plugin


@pytest.fixture
def circuit_breaker():
    """Create a CircuitBreaker instance."""
    return CircuitBreaker("test", max_failures=3, reset_timeout=60)


@pytest.fixture
def bridge(mock_rpc, mock_plugin):
    """Create a Bridge instance with mocks."""
    return Bridge(mock_rpc, mock_plugin)


# =============================================================================
# CIRCUIT BREAKER TESTS
# =============================================================================

class TestCircuitBreaker:
    """Test suite for CircuitBreaker class."""
    
    def test_initial_state_closed(self, circuit_breaker):
        """Circuit starts in CLOSED state."""
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.is_available() is True
    
    def test_success_resets_failure_count(self, circuit_breaker):
        """Success resets failure count."""
        circuit_breaker._failure_count = 2
        circuit_breaker.record_success()
        
        assert circuit_breaker._failure_count == 0
        assert circuit_breaker.state == CircuitState.CLOSED
    
    def test_failure_increments_count(self, circuit_breaker):
        """Failures increment the failure count."""
        circuit_breaker.record_failure()
        assert circuit_breaker._failure_count == 1
        
        circuit_breaker.record_failure()
        assert circuit_breaker._failure_count == 2
    
    def test_opens_after_max_failures(self, circuit_breaker):
        """Circuit opens after MAX_FAILURES consecutive failures."""
        for i in range(MAX_FAILURES):
            assert circuit_breaker.state == CircuitState.CLOSED
            circuit_breaker.record_failure()
        
        assert circuit_breaker.state == CircuitState.OPEN
        assert circuit_breaker.is_available() is False
    
    def test_transitions_to_half_open_after_timeout(self, circuit_breaker):
        """Circuit transitions to HALF_OPEN after reset timeout."""
        # Force open
        for _ in range(MAX_FAILURES):
            circuit_breaker.record_failure()
        assert circuit_breaker.state == CircuitState.OPEN
        
        # Simulate time passing
        circuit_breaker._last_failure_time = int(time.time()) - RESET_TIMEOUT - 1
        
        assert circuit_breaker.state == CircuitState.HALF_OPEN
        assert circuit_breaker.is_available() is True
    
    def test_half_open_success_closes_circuit(self, circuit_breaker):
        """Successful probe in HALF_OPEN closes circuit."""
        circuit_breaker._state = CircuitState.HALF_OPEN
        
        circuit_breaker.record_success()
        
        assert circuit_breaker.state == CircuitState.CLOSED
    
    def test_half_open_failure_reopens_circuit(self, circuit_breaker):
        """Failed probe in HALF_OPEN reopens circuit."""
        circuit_breaker._state = CircuitState.HALF_OPEN
        
        circuit_breaker.record_failure()
        
        assert circuit_breaker.state == CircuitState.OPEN
    
    def test_reset_returns_to_initial_state(self, circuit_breaker):
        """Reset returns circuit to initial CLOSED state."""
        # Get into a bad state
        for _ in range(MAX_FAILURES):
            circuit_breaker.record_failure()
        assert circuit_breaker.state == CircuitState.OPEN
        
        circuit_breaker.reset()
        
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker._failure_count == 0
    
    def test_get_stats(self, circuit_breaker):
        """get_stats returns expected fields."""
        stats = circuit_breaker.get_stats()
        
        assert stats["name"] == "test"
        assert stats["state"] == "closed"
        assert stats["failure_count"] == 0
        assert stats["max_failures"] == 3
        assert stats["reset_timeout"] == 60


# =============================================================================
# BRIDGE INITIALIZATION TESTS
# =============================================================================

class TestBridgeInitialization:
    """Test suite for Bridge initialization and feature detection."""
    
    def test_initial_status_disabled(self, bridge):
        """Bridge starts in DISABLED status."""
        assert bridge.status == BridgeStatus.DISABLED
    
    def test_detect_revenue_ops_not_found(self, bridge, mock_rpc):
        """Detection fails if cl-revenue-ops not in plugin list."""
        mock_rpc.plugin.return_value = {"plugins": [
            {"name": "clboss", "active": True}
        ]}
        
        status = bridge.initialize()
        
        assert status == BridgeStatus.DISABLED
        assert bridge._revenue_ops_version is None
    
    def test_detect_revenue_ops_inactive(self, bridge, mock_rpc):
        """Detection fails if cl-revenue-ops is inactive."""
        mock_rpc.plugin.return_value = {"plugins": [
            {"name": "cl-revenue-ops", "active": False}
        ]}
        
        status = bridge.initialize()
        
        assert status == BridgeStatus.DISABLED
    
    def test_detect_revenue_ops_success(self, bridge, mock_rpc):
        """Detection succeeds with valid cl-revenue-ops."""
        mock_rpc.plugin.return_value = {"plugins": [
            {"name": "cl-revenue-ops.py", "active": True}
        ]}
        mock_rpc.call.return_value = {"version": "v1.4.0"}
        
        status = bridge.initialize()
        
        assert status == BridgeStatus.ENABLED
        assert bridge._revenue_ops_version == "v1.4.0"
    
    def test_detect_revenue_ops_version_too_low(self, bridge, mock_rpc):
        """Detection fails if version is too old."""
        mock_rpc.plugin.return_value = {"plugins": [
            {"name": "cl-revenue-ops.py", "active": True}
        ]}
        mock_rpc.call.return_value = {"version": "v1.3.0"}
        
        status = bridge.initialize()
        
        assert status == BridgeStatus.DISABLED
    
    def test_detect_clboss_success(self, bridge, mock_rpc):
        """CLBoss detection works correctly."""
        mock_rpc.plugin.return_value = {"plugins": [
            {"name": "clboss", "active": True}
        ]}
        
        result = bridge._detect_clboss()
        
        assert result is True
        assert bridge._clboss_available is True
    
    def test_parse_version_with_v_prefix(self, bridge):
        """Version parsing handles 'v' prefix."""
        assert bridge._parse_version("v1.4.0") == (1, 4, 0)
        assert bridge._parse_version("v2.0.1") == (2, 0, 1)
    
    def test_parse_version_without_v_prefix(self, bridge):
        """Version parsing handles no prefix."""
        assert bridge._parse_version("1.4.0") == (1, 4, 0)
        assert bridge._parse_version("10.20.30") == (10, 20, 30)
    
    def test_parse_version_partial(self, bridge):
        """Version parsing handles partial versions."""
        assert bridge._parse_version("1.4") == (1, 4, 0)
        assert bridge._parse_version("2") == (0, 0, 0)  # Doesn't match pattern
    
    def test_parse_version_invalid(self, bridge):
        """Version parsing handles invalid strings."""
        assert bridge._parse_version("invalid") == (0, 0, 0)
        assert bridge._parse_version("") == (0, 0, 0)


# =============================================================================
# SAFE CALL TESTS
# =============================================================================

class TestSafeCall:
    """Test suite for safe_call wrapper."""
    
    def test_safe_call_disabled_raises_error(self, bridge):
        """safe_call raises BridgeDisabledError when disabled."""
        with pytest.raises(BridgeDisabledError):
            bridge.safe_call("test-method")
    
    def test_safe_call_circuit_open_raises_error(self, bridge, mock_rpc):
        """safe_call raises CircuitOpenError when circuit is open."""
        bridge._status = BridgeStatus.ENABLED
        
        # Force circuit open
        for _ in range(MAX_FAILURES):
            bridge._revenue_ops_cb.record_failure()
        
        with pytest.raises(CircuitOpenError):
            bridge.safe_call("test-method")
    
    def test_safe_call_success(self, bridge, mock_rpc):
        """safe_call succeeds and records success."""
        bridge._status = BridgeStatus.ENABLED
        mock_rpc.call.return_value = {"result": "ok"}
        
        result = bridge.safe_call("test-method")
        
        assert result == {"result": "ok"}
        assert bridge._revenue_ops_cb._failure_count == 0
    
    def test_safe_call_with_payload(self, bridge, mock_rpc):
        """safe_call passes payload correctly."""
        bridge._status = BridgeStatus.ENABLED
        mock_rpc.call.return_value = {"result": "ok"}
        
        bridge.safe_call("test-method", {"key": "value"})
        
        mock_rpc.call.assert_called_with("test-method", {"key": "value"})
    
    def test_safe_call_failure_records_failure(self, bridge, mock_rpc):
        """safe_call records failure on exception."""
        bridge._status = BridgeStatus.ENABLED
        mock_rpc.call.side_effect = RpcError("test-method", {}, "RPC error")
        
        with pytest.raises(RpcError):
            bridge.safe_call("test-method")
        
        assert bridge._revenue_ops_cb._failure_count == 1

    def test_safe_call_logic_error_does_not_trip_circuit(self, bridge, mock_rpc):
        """Logic errors do not increment failure count."""
        bridge._status = BridgeStatus.ENABLED
        mock_rpc.call.side_effect = ValueError("bad input")

        with pytest.raises(ValueError):
            bridge.safe_call("test-method")

        assert bridge._revenue_ops_cb._failure_count == 0

    def test_safe_call_circuit_open_fail_fast(self, bridge, mock_rpc):
        """Circuit open causes fail-fast without RPC call."""
        bridge._status = BridgeStatus.ENABLED

        for _ in range(MAX_FAILURES):
            bridge._revenue_ops_cb.record_failure()

        with pytest.raises(CircuitOpenError):
            bridge.safe_call("test-method")

        mock_rpc.call.assert_not_called()

    def test_safe_call_half_open_success_closes(self, bridge, mock_rpc):
        """Half-open probe success closes the circuit."""
        bridge._status = BridgeStatus.ENABLED
        mock_rpc.call.return_value = {"result": "ok"}

        bridge._revenue_ops_cb._state = CircuitState.OPEN
        bridge._revenue_ops_cb._last_failure_time = int(time.time()) - RESET_TIMEOUT - 1

        result = bridge.safe_call("test-method")

        assert result == {"result": "ok"}
        assert bridge._revenue_ops_cb.state == CircuitState.CLOSED


# =============================================================================
# REVENUE-OPS INTEGRATION TESTS
# =============================================================================

class TestRevenueOpsIntegration:
    """Test suite for cl-revenue-ops integration methods."""
    
    def test_set_hive_policy_disabled(self, bridge):
        """set_hive_policy returns False when disabled."""
        result = bridge.set_hive_policy("peer123", True)
        assert result is False
    
    def test_set_hive_policy_member(self, bridge, mock_rpc):
        """set_hive_policy sets HIVE strategy for members."""
        bridge._status = BridgeStatus.ENABLED
        mock_rpc.call.return_value = {"status": "success"}
        
        result = bridge.set_hive_policy("peer123" * 5, True)
        
        assert result is True
        mock_rpc.call.assert_called_with("revenue-policy", {
            "subcommand": "set",
            "peer_id": "peer123" * 5,
            "strategy": "hive",
            "rebalance": "enabled"
        })
    
    def test_set_hive_policy_non_member(self, bridge, mock_rpc):
        """set_hive_policy sets DYNAMIC strategy for non-members."""
        bridge._status = BridgeStatus.ENABLED
        mock_rpc.call.return_value = {"status": "success"}
        
        result = bridge.set_hive_policy("peer123" * 5, False)
        
        assert result is True
        mock_rpc.call.assert_called_with("revenue-policy", {
            "subcommand": "set",
            "peer_id": "peer123" * 5,
            "strategy": "dynamic"
        })
    
    def test_set_hive_policy_circuit_open(self, bridge, mock_rpc):
        """set_hive_policy handles circuit open gracefully."""
        bridge._status = BridgeStatus.ENABLED
        
        # Force circuit open
        for _ in range(MAX_FAILURES):
            bridge._revenue_ops_cb.record_failure()
        
        result = bridge.set_hive_policy("peer123", True)
        
        assert result is False
    
    def test_trigger_rebalance_success(self, bridge, mock_rpc):
        """trigger_rebalance initiates rebalance successfully."""
        bridge._status = BridgeStatus.ENABLED
        mock_rpc.call.return_value = {"status": "initiated"}
        
        result = bridge.trigger_rebalance("target_peer" * 5, 100000)
        
        assert result is True
        mock_rpc.call.assert_called_with("revenue-rebalance", {
            "from": "auto",
            "to": "target_peer" * 5,
            "amount": 100000
        })
    
    def test_trigger_rebalance_disabled(self, bridge):
        """trigger_rebalance returns False when disabled."""
        result = bridge.trigger_rebalance("target", 100000)
        assert result is False
    
    def test_get_peer_policy_success(self, bridge, mock_rpc):
        """get_peer_policy retrieves policy."""
        bridge._status = BridgeStatus.ENABLED
        mock_rpc.call.return_value = {"strategy": "hive", "base_fee": 0}
        
        result = bridge.get_peer_policy("peer123")
        
        assert result == {"strategy": "hive", "base_fee": 0}


# =============================================================================
# CLBOSS INTEGRATION TESTS
# =============================================================================

class TestClbossIntegration:
    """Test suite for CLBoss integration methods."""
    
    def test_ignore_peer_clboss_unavailable(self, bridge):
        """ignore_peer returns False when CLBoss unavailable."""
        result = bridge.ignore_peer("peer123")
        assert result is False
    
    def test_ignore_peer_success(self, bridge, mock_rpc):
        """ignore_peer calls clboss-ignore correctly."""
        bridge._status = BridgeStatus.ENABLED
        bridge._clboss_available = True
        mock_rpc.call.return_value = {}
        
        result = bridge.ignore_peer("peer123" * 5)
        
        assert result is True
        mock_rpc.call.assert_called_with("clboss-ignore", {"nodeid": "peer123" * 5})
    
    def test_unignore_peer_success(self, bridge, mock_rpc):
        """unignore_peer calls clboss-unignore correctly."""
        bridge._status = BridgeStatus.ENABLED
        bridge._clboss_available = True
        mock_rpc.call.return_value = {}
        
        result = bridge.unignore_peer("peer123" * 5)
        
        assert result is True
        mock_rpc.call.assert_called_with("clboss-unignore", {"nodeid": "peer123" * 5})
    
    def test_clboss_failure_records_failure(self, bridge, mock_rpc):
        """CLBoss failures are recorded in circuit breaker."""
        bridge._status = BridgeStatus.ENABLED
        bridge._clboss_available = True
        mock_rpc.call.side_effect = RpcError("clboss-ignore", {}, "CLBoss error")
        
        result = bridge.ignore_peer("peer123")
        
        assert result is False
        assert bridge._clboss_cb._failure_count == 1


# =============================================================================
# STATISTICS TESTS
# =============================================================================

class TestBridgeStats:
    """Test suite for bridge statistics."""
    
    def test_get_stats_disabled(self, bridge):
        """get_stats returns expected structure when disabled."""
        stats = bridge.get_stats()
        
        assert stats["status"] == "disabled"
        assert stats["revenue_ops"]["version"] is None
        assert stats["clboss"]["available"] is False
    
    def test_get_stats_enabled(self, bridge, mock_rpc):
        """get_stats returns expected structure when enabled."""
        bridge._status = BridgeStatus.ENABLED
        bridge._revenue_ops_version = "v1.4.0"
        bridge._clboss_available = True
        
        stats = bridge.get_stats()
        
        assert stats["status"] == "enabled"
        assert stats["revenue_ops"]["version"] == "v1.4.0"
        assert stats["revenue_ops"]["circuit_breaker"]["state"] == "closed"
        assert stats["clboss"]["available"] is True


# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
