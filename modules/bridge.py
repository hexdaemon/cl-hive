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
HALF_OPEN_SUCCESS_THRESHOLD = 3  # Consecutive successes needed to close circuit (Issue #10)

# Minimum required version of cl-revenue-ops
MIN_REVENUE_OPS_VERSION = (1, 4, 0)

# Startup retry configuration (Issue: plugin startup race condition)
STARTUP_RETRY_ATTEMPTS = 5        # Number of retries for cl-revenue-ops detection
STARTUP_RETRY_BASE_DELAY = 1.0    # Base delay in seconds (doubles each retry)

# Security hardening (Issue #27)
POLICY_RATE_LIMIT_SECONDS = 60       # Min seconds between policy changes per peer
MAX_REBALANCE_SATS = 10_000_000      # 0.1 BTC max per single rebalance (safety cap)
MAX_DAILY_REBALANCE_SATS = 50_000_000  # 0.5 BTC max per day (aggregate cap)


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
                 reset_timeout: int = RESET_TIMEOUT,
                 half_open_success_threshold: int = HALF_OPEN_SUCCESS_THRESHOLD):
        """
        Initialize Circuit Breaker.

        Args:
            name: Identifier for logging
            max_failures: Failures before opening circuit
            reset_timeout: Seconds before probing
            half_open_success_threshold: Consecutive successes needed in HALF_OPEN
        """
        self.name = name
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.half_open_success_threshold = half_open_success_threshold

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_success_count = 0  # Track consecutive successes in HALF_OPEN
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
        """
        Record a successful call.

        SECURITY (Issue #10): In HALF_OPEN state, require multiple consecutive
        successes before fully closing the circuit to prevent rapid flapping
        with unstable dependencies.
        """
        self._failure_count = 0
        self._last_success_time = int(time.time())

        if self._state == CircuitState.HALF_OPEN:
            self._half_open_success_count += 1
            # Only close after multiple consecutive successes
            if self._half_open_success_count >= self.half_open_success_threshold:
                self._state = CircuitState.CLOSED
                self._half_open_success_count = 0
        else:
            # Reset counter when in CLOSED state
            self._half_open_success_count = 0
    
    def record_failure(self) -> None:
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = int(time.time())

        if self._state == CircuitState.HALF_OPEN:
            # Probe failed, re-open the circuit and reset success counter
            self._state = CircuitState.OPEN
            self._half_open_success_count = 0
        elif self._failure_count >= self.max_failures:
            # Too many failures, open the circuit
            self._state = CircuitState.OPEN
    
    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_success_count = 0
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

        # Security hardening: Rate limiting (Issue #27)
        self._policy_last_change: Dict[str, float] = {}  # peer_id -> timestamp
        self._daily_rebalance_sats = 0  # Aggregate rebalance amount today
        self._daily_rebalance_reset = 0  # Timestamp of last daily reset

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

        Should be called once during plugin startup. Uses exponential backoff
        retry to handle plugin startup race conditions where cl-revenue-ops
        may not be fully initialized yet.

        Returns:
            BridgeStatus indicating availability
        """
        revenue_ops_ok = False

        # Retry with exponential backoff to handle startup race condition
        for attempt in range(STARTUP_RETRY_ATTEMPTS):
            revenue_ops_ok = self._detect_revenue_ops()
            if revenue_ops_ok:
                break

            # Don't sleep after the last attempt
            if attempt < STARTUP_RETRY_ATTEMPTS - 1:
                delay = STARTUP_RETRY_BASE_DELAY * (2 ** attempt)
                self._log(
                    f"cl-revenue-ops not ready, retry {attempt + 1}/{STARTUP_RETRY_ATTEMPTS} "
                    f"in {delay:.1f}s",
                    level='debug'
                )
                time.sleep(delay)

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

    def reinitialize(self) -> BridgeStatus:
        """
        Re-attempt bridge initialization if currently disabled.

        Useful for recovering from startup race conditions or after
        cl-revenue-ops is installed/restarted.

        Returns:
            BridgeStatus indicating new availability
        """
        if self._status == BridgeStatus.ENABLED:
            self._log("Bridge already enabled, skipping reinitialize")
            return self._status

        self._log("Attempting bridge re-initialization")
        return self.initialize()

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
        """Execute an RPC call via lightning-cli with a hard timeout.

        Uses -k (keyword args) format to properly pass named parameters
        to CLN methods.
        """
        if not self._rpc_socket_path:
            raise BridgeDisabledError("RPC socket path unavailable")

        # Use -k for keyword args format: key=value
        cmd = ["lightning-cli", "--rpc-file", self._rpc_socket_path, "-k", method]
        if payload:
            for key, value in payload.items():
                # Handle different value types
                if value is None:
                    continue
                elif isinstance(value, bool):
                    cmd.append(f"{key}={str(value).lower()}")
                elif isinstance(value, (dict, list)):
                    cmd.append(f"{key}={json.dumps(value, separators=(',', ':'))}")
                else:
                    cmd.append(f"{key}={value}")

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
    
    def set_hive_policy(self, peer_id: str, is_member: bool,
                        bypass_rate_limit: bool = False) -> bool:
        """
        Set Hive fee policy for a peer.

        Args:
            peer_id: Node public key
            is_member: True for Hive member (0 PPM), False for non-member
            bypass_rate_limit: Skip rate limiting (use sparingly, e.g., initial setup)

        Returns:
            True if policy was set successfully

        Security:
            Rate limited to one change per peer per POLICY_RATE_LIMIT_SECONDS (Issue #27)
        """
        if self._status == BridgeStatus.DISABLED:
            self._log(f"Cannot set policy for {peer_id[:16]}...: Bridge disabled")
            return False

        # Security: Rate limit policy changes per peer (Issue #27)
        now = time.time()
        if not bypass_rate_limit:
            last_change = self._policy_last_change.get(peer_id, 0)
            if now - last_change < POLICY_RATE_LIMIT_SECONDS:
                wait_time = int(POLICY_RATE_LIMIT_SECONDS - (now - last_change))
                self._log(
                    f"Rate limited: Cannot change policy for {peer_id[:16]}... "
                    f"(wait {wait_time}s)",
                    level='debug'
                )
                return False

        try:
            if is_member:
                # Set HIVE strategy with rebalancing enabled
                result = self.safe_call("revenue-policy", {
                    "action": "set",
                    "peer_id": peer_id,
                    "strategy": "hive",
                    "rebalance": "enabled"
                })
            else:
                # Revert to dynamic strategy
                result = self.safe_call("revenue-policy", {
                    "action": "set",
                    "peer_id": peer_id,
                    "strategy": "dynamic"
                })

            success = result.get("status") == "success"
            if success:
                self._policy_last_change[peer_id] = now  # Update rate limit tracker
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
    
    def _get_channel_scid(self, peer_id: str) -> Optional[str]:
        """
        Get the Short Channel ID for a channel with a peer.

        Args:
            peer_id: Node public key

        Returns:
            SCID string (e.g., "930866x2599x2") or None if no channel found
        """
        try:
            result = self.rpc.listpeerchannels(id=peer_id)
            channels = result.get('channels', [])

            # Find an active channel
            for ch in channels:
                state = ch.get('state', '')
                if state == 'CHANNELD_NORMAL':
                    scid = ch.get('short_channel_id')
                    if scid:
                        return scid

            return None
        except Exception as e:
            self._log(f"Failed to get SCID for {peer_id[:16]}...: {e}", level='debug')
            return None

    def _check_daily_rebalance_budget(self, amount_sats: int) -> bool:
        """
        Check if rebalance fits within daily budget.

        Resets the daily counter if a new day has started (UTC).

        Args:
            amount_sats: Amount to check

        Returns:
            True if within budget, False if would exceed
        """
        now = time.time()
        # Reset daily counter at midnight UTC
        current_day = int(now // 86400)
        last_reset_day = int(self._daily_rebalance_reset // 86400) if self._daily_rebalance_reset else 0

        if current_day > last_reset_day:
            self._daily_rebalance_sats = 0
            self._daily_rebalance_reset = now

        return (self._daily_rebalance_sats + amount_sats) <= MAX_DAILY_REBALANCE_SATS

    def trigger_rebalance(self, target_peer: str, amount_sats: int,
                          source_peer: str) -> bool:
        """
        Trigger a rebalance toward a Hive peer.

        Uses cl-revenue-ops Strategic Exemption to bypass profitability checks.

        Args:
            target_peer: Destination peer_id (will lookup SCID automatically)
            amount_sats: Amount to rebalance in satoshis
            source_peer: Source peer_id to drain liquidity from (required)

        Returns:
            True if rebalance was initiated successfully

        Security (Issue #27):
            - Single rebalance capped at MAX_REBALANCE_SATS (0.1 BTC)
            - Daily aggregate capped at MAX_DAILY_REBALANCE_SATS (0.5 BTC)

        Note:
            Both source_peer and target_peer are resolved to SCIDs before calling
            cl-revenue-ops. The Strategic Exemption allows negative-EV rebalances
            to Hive members up to the configured tolerance.
        """
        if self._status == BridgeStatus.DISABLED:
            return False

        # Security: Validate amount bounds (Issue #27)
        if amount_sats <= 0:
            self._log("Rebalance rejected: amount must be positive", level='warn')
            return False

        if amount_sats > MAX_REBALANCE_SATS:
            self._log(
                f"Rebalance rejected: {amount_sats} sats exceeds max "
                f"{MAX_REBALANCE_SATS} sats per operation",
                level='warn'
            )
            return False

        if not self._check_daily_rebalance_budget(amount_sats):
            self._log(
                f"Rebalance rejected: would exceed daily limit of "
                f"{MAX_DAILY_REBALANCE_SATS} sats (current: {self._daily_rebalance_sats})",
                level='warn'
            )
            return False

        # Look up target channel SCID
        target_scid = self._get_channel_scid(target_peer)
        if not target_scid:
            self._log(f"No active channel found for target peer {target_peer[:16]}...")
            return False

        # Look up source channel SCID (required)
        source_scid = self._get_channel_scid(source_peer)
        if not source_scid:
            self._log(f"No active channel found for source peer {source_peer[:16]}...")
            return False

        try:
            result = self.safe_call("revenue-rebalance", {
                "from_channel": source_scid,
                "to_channel": target_scid,
                "amount_sats": amount_sats
            })

            success = result.get("status") in ("success", "initiated", "pending")
            if success:
                self._daily_rebalance_sats += amount_sats  # Track for daily budget
                self._log(f"Rebalance initiated: {amount_sats} sats {source_scid} -> {target_scid}")

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
                "action": "get",
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
        """Get bridge statistics including security limits."""
        return {
            "status": self._status.value,
            "revenue_ops": {
                "version": self._revenue_ops_version,
                "circuit_breaker": self._revenue_ops_cb.get_stats()
            },
            "clboss": {
                "available": self._clboss_available,
                "circuit_breaker": self._clboss_cb.get_stats() if self._clboss_available else None
            },
            "security_limits": {
                "policy_rate_limit_seconds": POLICY_RATE_LIMIT_SECONDS,
                "max_rebalance_sats": MAX_REBALANCE_SATS,
                "max_daily_rebalance_sats": MAX_DAILY_REBALANCE_SATS,
                "daily_rebalance_used_sats": self._daily_rebalance_sats,
                "daily_rebalance_remaining_sats": max(0, MAX_DAILY_REBALANCE_SATS - self._daily_rebalance_sats)
            }
        }
