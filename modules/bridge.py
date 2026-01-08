"""
Integration Bridge Module for cl-hive.

Implements the "Paranoid" Bridge pattern with Circuit Breaker for
safe integration with external plugins (cl-revenue-ops, clboss).

Circuit Breaker Pattern:
- CLOSED: Normal operation, requests pass through
- OPEN: Fail fast, no requests sent (dependency is down)
- HALF_OPEN: Probe mode, single test request to check recovery

This prevents cascading failures when a dependency hangs or crashes.

Author: Lightning Goats Team
"""

import json
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from pyln.client import RpcError
# =============================================================================
# CONSTANTS
# =============================================================================

# Circuit Breaker thresholds
MAX_FAILURES = 3          # Consecutive failures before opening circuit
RESET_TIMEOUT = 60        # Seconds to wait before probing (OPEN -> HALF_OPEN)
RPC_TIMEOUT = 5           # Timeout for RPC calls (seconds)

# Minimum required version of cl-revenue-ops
MIN_REVENUE_OPS_VERSION = (1, 4, 0)


# =============================================================================
# ENUMS
# =============================================================================

class CircuitState(Enum):
    """Circuit Breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Fail fast mode
    HALF_OPEN = "half_open"  # Probe mode


class BridgeStatus(Enum):
    """Overall bridge status."""
    ENABLED = "enabled"
    DISABLED = "disabled"
    DEGRADED = "degraded"


# =============================================================================
# EXCEPTIONS
# =============================================================================

class CircuitOpenError(Exception):
    """Raised when Circuit Breaker is OPEN and blocking requests."""
    pass


class BridgeDisabledError(Exception):
    """Raised when Bridge is disabled due to missing dependency."""
    pass


class VersionMismatchError(Exception):
    """Raised when dependency version is incompatible."""
    pass


# =============================================================================
# CIRCUIT BREAKER
# =============================================================================

class CircuitBreaker:
    """
    Implements the Circuit Breaker pattern for RPC calls.
    
    State transitions:
    - CLOSED -> OPEN: After MAX_FAILURES consecutive failures
    - OPEN -> HALF_OPEN: After RESET_TIMEOUT seconds
    - HALF_OPEN -> CLOSED: On successful probe
    - HALF_OPEN -> OPEN: On probe failure
    """
    
    def __init__(self, name: str, max_failures: int = MAX_FAILURES,
                 reset_timeout: int = RESET_TIMEOUT):
        """
        Initialize Circuit Breaker.
        
        Args:
            name: Identifier for logging
            max_failures: Failures before opening circuit
            reset_timeout: Seconds before probing
        """
        self.name = name
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0
        self._last_success_time = 0
    
    @property
    def state(self) -> CircuitState:
        """Get current state, checking for automatic transitions."""
        if self._state == CircuitState.OPEN:
            # Check if we should transition to HALF_OPEN
            now = int(time.time())
            if now - self._last_failure_time >= self.reset_timeout:
                self._state = CircuitState.HALF_OPEN
        return self._state
    
    def is_available(self) -> bool:
        """Check if requests can be made (not OPEN)."""
        return self.state != CircuitState.OPEN
    
    def record_success(self) -> None:
        """Record a successful call."""
        self._failure_count = 0
        self._last_success_time = int(time.time())
        
        if self._state == CircuitState.HALF_OPEN:
            # Probe succeeded, close the circuit
            self._state = CircuitState.CLOSED
    
    def record_failure(self) -> None:
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = int(time.time())
        
        if self._state == CircuitState.HALF_OPEN:
            # Probe failed, re-open the circuit
            self._state = CircuitState.OPEN
        elif self._failure_count >= self.max_failures:
            # Too many failures, open the circuit
            self._state = CircuitState.OPEN
    
    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "max_failures": self.max_failures,
            "reset_timeout": self.reset_timeout,
            "last_failure_ago": int(time.time()) - self._last_failure_time if self._last_failure_time else None,
            "last_success_ago": int(time.time()) - self._last_success_time if self._last_success_time else None
        }


# =============================================================================
# BRIDGE CLASS
# =============================================================================

class Bridge:
    """
    Integration Bridge for cl-hive to external plugins.
    
    Provides "Paranoid" error handling for calls to:
    - cl-revenue-ops: Fee strategy and rebalancing
    - clboss: Topology ignore/unignore
    
    Thread Safety:
    - Uses the thread-safe RPC proxy from cl-hive.py
    - Circuit breaker state is simple integers (thread-safe for reads)
    """
    
    def __init__(self, rpc, plugin=None):
        """
        Initialize the Bridge.
        
        Args:
            rpc: Thread-safe RPC proxy
            plugin: Optional plugin reference for logging
        """
        self.rpc = rpc
        self.plugin = plugin
        
        # Status tracking
        self._status = BridgeStatus.DISABLED
        self._revenue_ops_version: Optional[str] = None
        self._clboss_available = False
        self._clboss_unignore_supported = True

        self._rpc_socket_path = self._resolve_rpc_socket()
        self._use_subprocess = bool(
            self._rpc_socket_path and shutil.which("lightning-cli")
        )
        if not self._use_subprocess:
            self._log(
                "Bridge RPC timeout disabled: lightning-cli or rpc socket unavailable",
                level="warn"
            )
        
        # Circuit breakers for each integration
        self._revenue_ops_cb = CircuitBreaker("revenue-ops")
        self._clboss_cb = CircuitBreaker("clboss")

    def _resolve_rpc_socket(self) -> Optional[str]:
        """Resolve the Core Lightning RPC socket path if available."""
        if hasattr(self.rpc, "get_socket_path"):
            path = self.rpc.get_socket_path()
            if isinstance(path, str) and path:
                return path
        if hasattr(self.rpc, "socket_path"):
            path = self.rpc.socket_path
            if isinstance(path, str) and path:
                return path
        if hasattr(self.rpc, "_rpc") and hasattr(self.rpc._rpc, "socket_path"):
            path = self.rpc._rpc.socket_path
            if isinstance(path, str) and path:
                return path
        return None
    
    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[Bridge] {msg}", level=level)
    
    # =========================================================================
    # INITIALIZATION & FEATURE DETECTION
    # =========================================================================
    
    def initialize(self) -> BridgeStatus:
        """
        Detect available integrations and verify versions.
        
        Should be called once during plugin startup.
        
        Returns:
            BridgeStatus indicating availability
        """
        revenue_ops_ok = self._detect_revenue_ops()
        clboss_ok = self._detect_clboss()
        
        if revenue_ops_ok:
            self._status = BridgeStatus.ENABLED
            self._log(f"Bridge enabled: cl-revenue-ops {self._revenue_ops_version}")
        else:
            self._status = BridgeStatus.DISABLED
            self._log("Bridge disabled: cl-revenue-ops not available", level='warn')
        
        if clboss_ok:
            self._log("CLBoss integration available")
        
        return self._status
    
    def _detect_revenue_ops(self) -> bool:
        """
        Detect cl-revenue-ops plugin and verify version.
        
        Returns:
            True if cl-revenue-ops is available and compatible
        """
        try:
            # Check plugin is loaded
            plugins = self.rpc.plugin("list")
            
            revenue_ops_active = False
            for p in plugins.get('plugins', []):
                if 'cl-revenue-ops' in p.get('name', ''):
                    revenue_ops_active = p.get('active', False)
                    break
            
            if not revenue_ops_active:
                self._log("cl-revenue-ops plugin not found or not active")
                return False
            
            # Check version
            status = self.rpc.call("revenue-status")
            version_str = status.get("version", "0.0.0")
            self._revenue_ops_version = version_str
            
            # Parse version
            version_tuple = self._parse_version(version_str)
            if version_tuple < MIN_REVENUE_OPS_VERSION:
                self._log(
                    f"cl-revenue-ops version {version_str} < required {MIN_REVENUE_OPS_VERSION}",
                    level='warn'
                )
                return False
            
            self._revenue_ops_cb.record_success()
            return True
            
        except Exception as e:
            self._log(f"Failed to detect cl-revenue-ops: {e}", level='warn')
            self._revenue_ops_cb.record_failure()
            return False
    
    def _detect_clboss(self) -> bool:
        """
        Detect clboss plugin.
        
        Returns:
            True if clboss is available
        """
        try:
            plugins = self.rpc.plugin("list")
            
            for p in plugins.get('plugins', []):
                if 'clboss' in p.get('name', '').lower():
                    self._clboss_available = p.get('active', False)
                    if self._clboss_available:
                        self._clboss_cb.record_success()
                    return self._clboss_available
            
            return False
            
        except Exception as e:
            self._log(f"Failed to detect clboss: {e}", level='debug')
            return False
    
    def _parse_version(self, version_str: str) -> Tuple[int, int, int]:
        """
        Parse version string to tuple.
        
        Args:
            version_str: Version like "v1.4.0" or "1.4.0"
            
        Returns:
            Tuple of (major, minor, patch)
        """
        # Strip leading 'v' if present
        version_str = version_str.lstrip('v')
        
        # Extract numbers
        match = re.match(r'(\d+)\.(\d+)\.?(\d*)', version_str)
        if match:
            major = int(match.group(1))
            minor = int(match.group(2))
            patch = int(match.group(3)) if match.group(3) else 0
            return (major, minor, patch)
        
        return (0, 0, 0)
    
    # =========================================================================
    # SAFE CALL WRAPPER
    # =========================================================================

    def _call_via_lightning_cli(self, method: str, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute an RPC call via lightning-cli with a hard timeout."""
        if not self._rpc_socket_path:
            raise BridgeDisabledError("RPC socket path unavailable")

        cmd = ["lightning-cli", "--rpc-file", self._rpc_socket_path, method]
        if payload:
            cmd.append(json.dumps(payload, separators=(',', ':')))

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=RPC_TIMEOUT,
            check=False
        )

        if result.returncode != 0:
            err_msg = (result.stderr or result.stdout or "").strip()
            raise RpcError(method, payload or {}, err_msg or "RPC error")

        output = result.stdout.strip()
        if not output:
            return {}

        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise RpcError(method, payload or {}, f"Invalid JSON response: {exc}")

    def _call_direct(self, method: str, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute an RPC call directly via the RPC proxy."""
        if payload:
            return self.rpc.call(method, payload)
        return self.rpc.call(method)

    def safe_call(self, method: str, payload: Dict = None,
                  circuit_breaker: CircuitBreaker = None) -> Dict[str, Any]:
        """
        Execute an RPC call with Circuit Breaker protection.
        
        Args:
            method: RPC method name
            payload: Optional payload dict
            circuit_breaker: Which circuit breaker to use
            
        Returns:
            RPC response
            
        Raises:
            CircuitOpenError: If circuit is open
            BridgeDisabledError: If bridge is disabled
        """
        if self._status == BridgeStatus.DISABLED:
            raise BridgeDisabledError("Bridge is disabled")
        
        cb = circuit_breaker or self._revenue_ops_cb
        
        if not cb.is_available():
            raise CircuitOpenError(f"Circuit {cb.name} is OPEN")
        
        try:
            if self._use_subprocess:
                result = self._call_via_lightning_cli(method, payload)
            else:
                result = self._call_direct(method, payload)

            cb.record_success()
            return result
        except subprocess.TimeoutExpired:
            cb.record_failure()
            self._log(
                f"RPC call {method} timed out after {RPC_TIMEOUT}s",
                level='warn'
            )
            raise TimeoutError(f"RPC call {method} timed out after {RPC_TIMEOUT}s")
        except RpcError as e:
            cb.record_failure()
            self._log(f"RPC call {method} failed: {e}", level='warn')
            raise
        except TimeoutError as e:
            cb.record_failure()
            self._log(f"RPC call {method} timed out: {e}", level='warn')
            raise
        except Exception as e:
            self._log(f"RPC call {method} failed: {e}", level='warn')
            raise
    
    # =========================================================================
    # REVENUE-OPS INTEGRATION
    # =========================================================================
    
    def set_hive_policy(self, peer_id: str, is_member: bool) -> bool:
        """
        Set Hive fee policy for a peer.
        
        Args:
            peer_id: Node public key
            is_member: True for Hive member (0 PPM), False for non-member
            
        Returns:
            True if policy was set successfully
        """
        if self._status == BridgeStatus.DISABLED:
            self._log(f"Cannot set policy for {peer_id[:16]}...: Bridge disabled")
            return False
        
        try:
            if is_member:
                # Set HIVE strategy with rebalancing enabled
                result = self.safe_call("revenue-policy", {
                    "subcommand": "set",
                    "peer_id": peer_id,
                    "strategy": "hive",
                    "rebalance": "enabled"
                })
            else:
                # Revert to dynamic strategy
                result = self.safe_call("revenue-policy", {
                    "subcommand": "set",
                    "peer_id": peer_id,
                    "strategy": "dynamic"
                })
            
            success = result.get("status") == "success"
            if success:
                self._log(f"Set {'hive' if is_member else 'dynamic'} policy for {peer_id[:16]}...")
            else:
                self._log(f"Policy set returned: {result}", level='warn')
            
            return success
            
        except CircuitOpenError:
            self._log(f"Circuit open, cannot set policy for {peer_id[:16]}...")
            return False
        except Exception as e:
            self._log(f"Failed to set policy for {peer_id[:16]}...: {e}", level='warn')
            return False
    
    def trigger_rebalance(self, target_peer: str, amount_sats: int) -> bool:
        """
        Trigger a rebalance toward a Hive peer.
        
        Uses cl-revenue-ops Strategic Exemption to bypass profitability checks.
        
        Args:
            target_peer: Destination peer_id
            amount_sats: Amount to rebalance in satoshis
            
        Returns:
            True if rebalance was initiated successfully
        """
        if self._status == BridgeStatus.DISABLED:
            return False
        
        try:
            result = self.safe_call("revenue-rebalance", {
                "from": "auto",
                "to": target_peer,
                "amount": amount_sats
            })
            
            success = result.get("status") in ("success", "initiated", "pending")
            if success:
                self._log(f"Rebalance initiated: {amount_sats} sats -> {target_peer[:16]}...")
            
            return success
            
        except CircuitOpenError:
            return False
        except Exception as e:
            self._log(f"Rebalance failed: {e}", level='warn')
            return False
    
    def get_peer_policy(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current policy for a peer.
        
        Args:
            peer_id: Node public key
            
        Returns:
            Policy dict or None if unavailable
        """
        if self._status == BridgeStatus.DISABLED:
            return None
        
        try:
            result = self.safe_call("revenue-policy", {
                "subcommand": "get",
                "peer_id": peer_id
            })
            return result
            
        except Exception:
            return None
    
    # =========================================================================
    # CLBOSS INTEGRATION
    # =========================================================================
    
    def ignore_peer(self, peer_id: str) -> bool:
        """
        Tell CLBoss to ignore a peer for channel management.
        
        Used to prevent CLBoss from opening redundant channels
        to targets the Hive already covers.
        
        Args:
            peer_id: Node public key to ignore
            
        Returns:
            True if successful
        """
        if not self._clboss_available:
            self._log(f"CLBoss not available, cannot ignore {peer_id[:16]}...")
            return False
        
        try:
            result = self.safe_call(
                "clboss-ignore",
                {"nodeid": peer_id},
                self._clboss_cb
            )
            
            self._log(f"CLBoss ignoring {peer_id[:16]}...")
            return True
            
        except Exception as e:
            self._log(f"Failed to ignore peer in CLBoss: {e}", level='warn')
            return False
    
    def unignore_peer(self, peer_id: str) -> bool:
        """
        Tell CLBoss to stop ignoring a peer.
        
        Args:
            peer_id: Node public key to unignore
            
        Returns:
            True if successful
        """
        if not self._clboss_available or not self._clboss_unignore_supported:
            return False
        
        try:
            result = self.safe_call(
                "clboss-unignore",
                {"nodeid": peer_id},
                self._clboss_cb
            )
            
            self._log(f"CLBoss unignoring {peer_id[:16]}...")
            return True
            
        except Exception as e:
            msg = str(e).lower()
            if "unknown command" in msg or "method not found" in msg:
                self._clboss_unignore_supported = False
            self._log(f"Failed to unignore peer in CLBoss: {e}", level='warn')
            return False
    
    # =========================================================================
    # STATUS & STATISTICS
    # =========================================================================
    
    @property
    def status(self) -> BridgeStatus:
        """Get current bridge status."""
        return self._status
    
    def get_stats(self) -> Dict[str, Any]:
        """Get bridge statistics."""
        return {
            "status": self._status.value,
            "revenue_ops": {
                "version": self._revenue_ops_version,
                "circuit_breaker": self._revenue_ops_cb.get_stats()
            },
            "clboss": {
                "available": self._clboss_available,
                "circuit_breaker": self._clboss_cb.get_stats() if self._clboss_available else None
            }
        }
