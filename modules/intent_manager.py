"""
Intent Manager Module for cl-hive.

Implements the Intent Lock Protocol for deterministic conflict resolution
to prevent "Thundering Herd" race conditions when multiple nodes attempt
the same action simultaneously.

Protocol Flow (Announce-Wait-Commit):
1. ANNOUNCE: Node broadcasts HIVE_INTENT with (type, target, initiator, timestamp)
2. WAIT: Hold for `intent_hold_seconds` (default: 60s)
3. COMMIT: If no conflicts received/lost, execute the action

Tie-Breaker Rule:
- If two nodes announce conflicting intents, the node with the
  lexicographically LOWEST pubkey wins.
- Loser must broadcast HIVE_INTENT_ABORT and update status='aborted'.

Author: Lightning Goats Team
"""

import threading
import time
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

# =============================================================================
# CONSTANTS
# =============================================================================

# Default hold period before committing an intent (seconds)
DEFAULT_HOLD_SECONDS = 60

# Maximum age for stale intents before cleanup (seconds) - 1 hour
STALE_INTENT_THRESHOLD = 3600

# Maximum number of remote intents to cache (DoS protection - P3-01)
MAX_REMOTE_INTENTS = 200

# Intent status values
STATUS_PENDING = 'pending'
STATUS_COMMITTED = 'committed'
STATUS_ABORTED = 'aborted'
STATUS_EXPIRED = 'expired'
STATUS_FAILED = 'failed'

# All valid statuses
VALID_STATUSES = {STATUS_PENDING, STATUS_COMMITTED, STATUS_ABORTED, STATUS_EXPIRED, STATUS_FAILED}

# Valid status transitions (from -> set of allowed to)
VALID_TRANSITIONS = {
    STATUS_PENDING: {STATUS_COMMITTED, STATUS_ABORTED, STATUS_EXPIRED},
    STATUS_COMMITTED: {STATUS_FAILED},
    # Terminal states: no transitions out
    STATUS_ABORTED: set(),
    STATUS_EXPIRED: set(),
    STATUS_FAILED: set(),
}


# =============================================================================
# ENUMS
# =============================================================================

class IntentType(str, Enum):
    """
    Supported intent types for coordinated actions.
    
    Using str, Enum for JSON serialization compatibility.
    """
    CHANNEL_OPEN = 'channel_open'
    REBALANCE = 'rebalance'
    BAN_PEER = 'ban_peer'


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class Intent:
    """
    Represents an Intent lock for a coordinated action.

    Attributes:
        intent_type: Type of action (channel_open, rebalance, ban_peer)
        target: Target identifier (peer_id for channel_open/ban, route for rebalance)
        initiator: Public key of the node proposing the action
        timestamp: Unix timestamp when intent was announced
        expires_at: Unix timestamp when intent expires
        status: Current status (pending, committed, aborted, expired)
        intent_id: Database ID (set after insertion)
        amount_sats: Committed budget for this intent (Phase 8, optional)
        budget_proof_timestamp: When budget was verified (Phase 8, optional)
    """
    intent_type: str
    target: str
    initiator: str
    timestamp: int
    expires_at: int
    status: str = STATUS_PENDING
    intent_id: Optional[int] = None
    # Phase 8: Budget commitment fields (optional, for backward compatibility)
    amount_sats: int = 0
    budget_proof_timestamp: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            'intent_type': self.intent_type,
            'target': self.target,
            'initiator': self.initiator,
            'timestamp': self.timestamp,
            'expires_at': self.expires_at,
            'status': self.status
        }
        # Phase 8: Include budget fields if set (backward compatible)
        if self.amount_sats > 0:
            result['amount_sats'] = self.amount_sats
        if self.budget_proof_timestamp > 0:
            result['budget_proof_timestamp'] = self.budget_proof_timestamp
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any], intent_id: Optional[int] = None) -> 'Intent':
        """Create from dictionary."""
        return cls(
            intent_type=data['intent_type'],
            target=data['target'],
            initiator=data['initiator'],
            timestamp=data['timestamp'],
            expires_at=data.get('expires_at', data['timestamp'] + DEFAULT_HOLD_SECONDS),
            status=data.get('status', STATUS_PENDING),
            intent_id=intent_id,
            # Phase 8: Budget fields (optional, default to 0)
            amount_sats=data.get('amount_sats', 0),
            budget_proof_timestamp=data.get('budget_proof_timestamp', 0),
        )
    
    def is_expired(self) -> bool:
        """Check if this intent has expired."""
        return int(time.time()) > self.expires_at
    
    def is_conflicting(self, other: 'Intent') -> bool:
        """
        Check if this intent conflicts with another.
        
        Two intents conflict if they have the same type and target,
        and both are still pending.
        """
        return (
            self.intent_type == other.intent_type and
            self.target == other.target and
            self.status == STATUS_PENDING and
            other.status == STATUS_PENDING
        )


# =============================================================================
# INTENT MANAGER CLASS
# =============================================================================

class IntentManager:
    """
    Manages the Intent Lock Protocol for conflict-free coordination.
    
    Responsibilities:
    - Create and announce new intents
    - Detect and resolve conflicts using deterministic tie-breaker
    - Track pending intents and their expiration
    - Commit or abort intents based on conflict resolution
    
    Thread Safety:
    - All database operations use thread-local connections
    - Intent state is primarily managed via database
    """
    
    def __init__(self, database, plugin=None, our_pubkey: str = None,
                 hold_seconds: int = DEFAULT_HOLD_SECONDS,
                 expire_seconds: int = None):
        """
        Initialize the IntentManager.

        Args:
            database: HiveDatabase instance for persistence
            plugin: Optional plugin reference for logging and RPC
            our_pubkey: Our node's public key (for tie-breaker)
            hold_seconds: Seconds to wait before committing
            expire_seconds: Intent TTL in seconds (defaults to hold_seconds * 2)
        """
        self.db = database
        self.plugin = plugin
        self.our_pubkey = our_pubkey
        self.hold_seconds = hold_seconds
        self.expire_seconds = expire_seconds if expire_seconds is not None else hold_seconds * 2

        # Callback registry for intent commit actions
        self._commit_callbacks: Dict[str, Callable] = {}

        # Lock protecting _commit_callbacks
        self._callback_lock = threading.Lock()

        # Lock protecting _remote_intents
        self._remote_lock = threading.Lock()

        # Track remote intents for visibility
        self._remote_intents: Dict[str, Intent] = {}
    
    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[IntentManager] {msg}", level=level)
    
    def set_our_pubkey(self, pubkey: str) -> None:
        """Set our node's public key (called after init)."""
        self.our_pubkey = pubkey
    
    # =========================================================================
    # STATUS VALIDATION
    # =========================================================================

    def _validate_transition(self, intent_id: int, new_status: str) -> bool:
        """
        Validate that a status transition is allowed.

        Queries current status from DB and checks against VALID_TRANSITIONS.

        Args:
            intent_id: Database ID of the intent
            new_status: Desired new status

        Returns:
            True if transition is valid
        """
        if new_status not in VALID_STATUSES:
            self._log(f"Invalid status '{new_status}' for intent {intent_id}", level="warn")
            return False

        row = self.db.get_intent_by_id(intent_id)
        if not row:
            self._log(f"Intent {intent_id} not found for transition check", level="warn")
            return False

        current = row.get('status')
        allowed = VALID_TRANSITIONS.get(current, set())
        if new_status not in allowed:
            self._log(f"Invalid transition for intent {intent_id}: "
                     f"'{current}' -> '{new_status}' (allowed: {allowed})", level="warn")
            return False

        return True

    # =========================================================================
    # INTENT CREATION
    # =========================================================================

    def create_intent(self, intent_type: str, target: str) -> Optional[Intent]:
        """
        Create a new local intent and persist to database.

        Checks for existing pending intents for the same target/type to
        prevent duplicate intents from being created.

        Args:
            intent_type: Type of action (from IntentType enum)
            target: Target identifier

        Returns:
            The created Intent object with database ID, or None if
            our_pubkey not set, invalid type, or a duplicate already exists
        """
        if not self.our_pubkey:
            self._log("Cannot create intent: our_pubkey not set", level="warn")
            return None

        # Validate intent_type against known enum values
        valid_types = {t.value for t in IntentType}
        if intent_type not in valid_types:
            self._log(f"Invalid intent_type '{intent_type}' "
                     f"(valid: {sorted(valid_types)})", level="warn")
            return None

        # Check for existing pending intent for same target/type
        existing = self.db.get_conflicting_intents(target, intent_type)
        for row in existing:
            if row.get('initiator') == self.our_pubkey:
                self._log(f"Duplicate intent rejected: {intent_type} -> {target[:16]}... "
                         f"(existing ID: {row.get('id')})", level="warn")
                return None

        now = int(time.time())
        expires_at = now + self.expire_seconds

        # Pass timestamp to DB to ensure Intent object and DB record match
        intent_id = self.db.create_intent(
            intent_type=intent_type,
            target=target,
            initiator=self.our_pubkey,
            expires_seconds=self.expire_seconds,
            timestamp=now
        )

        intent = Intent(
            intent_type=intent_type,
            target=target,
            initiator=self.our_pubkey,
            timestamp=now,
            expires_at=expires_at,
            status=STATUS_PENDING,
            intent_id=intent_id
        )

        self._log(f"Created intent: {intent_type} -> {target[:16]}... (ID: {intent_id})")

        return intent
    
    def create_intent_message(self, intent: Intent) -> Dict[str, Any]:
        """
        Create a HIVE_INTENT message payload.
        
        Args:
            intent: The Intent to broadcast
            
        Returns:
            Dict payload for serialization
        """
        return {
            'intent_type': intent.intent_type,
            'target': intent.target,
            'initiator': intent.initiator,
            'timestamp': intent.timestamp,
            'expires_at': intent.expires_at
        }
    
    # =========================================================================
    # CONFLICT DETECTION & RESOLUTION
    # =========================================================================
    
    def check_conflicts(self, remote_intent: Intent) -> Tuple[bool, bool]:
        """
        Check for conflicts with a remote intent.
        
        Uses the Tie-Breaker Rule: Lowest lexicographical pubkey wins.
        
        Args:
            remote_intent: Intent received from another node
            
        Returns:
            Tuple of (has_conflict, we_win)
            - has_conflict: True if there's a local pending intent for same target
            - we_win: True if we win the tie-breaker (our pubkey < their pubkey)
        """
        # Guard: our_pubkey must be set for tie-breaker comparison
        if not self.our_pubkey:
            self._log("Cannot resolve conflict: our_pubkey not set", level="warn")
            return (True, False)  # Conflict exists, we lose (safe default)

        # Query local pending intents for same target
        local_conflicts = self.db.get_conflicting_intents(
            target=remote_intent.target,
            intent_type=remote_intent.intent_type
        )

        if not local_conflicts:
            return (False, False)

        # We have a conflict - apply tie-breaker
        # Lowest lexicographical pubkey wins
        we_win = self.our_pubkey < remote_intent.initiator
        
        self._log(f"Conflict detected for {remote_intent.target[:16]}...: "
                 f"us={self.our_pubkey[:16]}... vs them={remote_intent.initiator[:16]}... "
                 f"-> {'WE WIN' if we_win else 'WE LOSE'}")
        
        return (True, we_win)
    
    def abort_local_intent(self, target: str, intent_type: str) -> bool:
        """
        Abort our local pending intent for a target.
        
        Called when we lose a tie-breaker to a remote node.
        
        Args:
            target: Target identifier
            intent_type: Type of intent
            
        Returns:
            True if an intent was aborted
        """
        local_intents = self.db.get_conflicting_intents(target, intent_type)
        
        aborted = False
        for intent_row in local_intents:
            intent_id = intent_row.get('id')
            if intent_id:
                self.db.update_intent_status(intent_id, STATUS_ABORTED, reason="tie_breaker_loss")
                self._log(f"Aborted local intent {intent_id} for {target[:16]}... (lost tie-breaker)")
                aborted = True

        return aborted
    
    def create_abort_message(self, intent: Intent) -> Dict[str, Any]:
        """
        Create a HIVE_INTENT_ABORT message payload.
        
        Args:
            intent: The Intent being aborted
            
        Returns:
            Dict payload for serialization
        """
        return {
            'intent_type': intent.intent_type,
            'target': intent.target,
            'initiator': intent.initiator,
            'timestamp': intent.timestamp,
            'reason': 'tie_breaker_loss'
        }
    
    # =========================================================================
    # REMOTE INTENT TRACKING
    # =========================================================================
    
    def record_remote_intent(self, intent: Intent) -> None:
        """
        Record a remote intent for visibility/tracking.

        Enforces MAX_REMOTE_INTENTS limit with timestamp-based eviction (P3-01).

        Args:
            intent: Remote intent received from network
        """
        # Validate timestamp: reject intents too far in the future or too old
        now = time.time()
        if intent.timestamp > now + 300:
            self._log(f"Rejected remote intent from {intent.initiator[:16]}...: "
                     f"timestamp {int(intent.timestamp)} is too far in the future", level="warn")
            return
        if intent.timestamp < now - 86400:
            self._log(f"Rejected remote intent from {intent.initiator[:16]}...: "
                     f"timestamp {int(intent.timestamp)} is too old (>24h)", level="warn")
            return

        key = f"{intent.intent_type}:{intent.target}:{intent.initiator}"

        with self._remote_lock:
            # P3-01: Enforce cache size limit - evict by insertion order (Python 3.7+)
            # Using insertion order prevents attackers from crafting old timestamps
            # to evict legitimate recent intents.
            if key not in self._remote_intents and len(self._remote_intents) >= MAX_REMOTE_INTENTS:
                evict_key = next(iter(self._remote_intents))
                del self._remote_intents[evict_key]
                self._log(f"Evicted oldest remote intent (cache full at {MAX_REMOTE_INTENTS})", level='debug')

            self._remote_intents[key] = intent

        self._log(f"Recorded remote intent from {intent.initiator[:16]}...: "
                 f"{intent.intent_type} -> {intent.target[:16]}...", level='debug')
    
    def record_remote_abort(self, intent_type: str, target: str, initiator: str) -> None:
        """
        Record that a remote node aborted their intent.

        Args:
            intent_type: Type of intent
            target: Target identifier
            initiator: Node that aborted
        """
        key = f"{intent_type}:{target}:{initiator}"
        with self._remote_lock:
            if key in self._remote_intents:
                self._remote_intents[key].status = STATUS_ABORTED
        self._log(f"Remote intent aborted by {initiator[:16]}...: "
                 f"{intent_type} -> {target[:16]}...", level='debug')
    
    def get_remote_intents(self, target: str = None) -> List[Intent]:
        """
        Get tracked remote intents, optionally filtered by target.

        Returns defensive copies to prevent callers from mutating
        cached state without holding the lock.

        Args:
            target: Optional target to filter by

        Returns:
            List of remote Intent objects (copies)
        """
        with self._remote_lock:
            intents = [
                Intent.from_dict(i.to_dict(), i.intent_id)
                for i in self._remote_intents.values()
            ]

        if target:
            intents = [i for i in intents if i.target == target]

        return intents
    
    # =========================================================================
    # COMMIT LOGIC
    # =========================================================================
    
    def register_commit_callback(self, intent_type: str, callback: Callable) -> None:
        """
        Register a callback function for when an intent commits.

        Args:
            intent_type: Type of intent to handle
            callback: Function(intent) to call on commit
        """
        with self._callback_lock:
            self._commit_callbacks[intent_type] = callback
        self._log(f"Registered commit callback for {intent_type}")
    
    def get_pending_intents_ready_to_commit(self) -> List[Dict]:
        """
        Get local intents that are ready to commit.

        An intent is ready if:
        - Status is 'pending'
        - Current time > timestamp + hold_seconds
        - Intent has not expired

        Returns:
            List of intent rows from database
        """
        return self.db.get_pending_intents_ready(self.hold_seconds)
    
    def commit_intent(self, intent_id: int) -> bool:
        """
        Commit a pending intent and trigger its action.

        Validates the pending -> committed transition before updating.

        Args:
            intent_id: Database ID of the intent

        Returns:
            True if commit succeeded
        """
        if not self._validate_transition(intent_id, STATUS_COMMITTED):
            return False

        success = self.db.update_intent_status(intent_id, STATUS_COMMITTED)

        if success:
            self._log(f"Committed intent {intent_id}")

        return success
    
    def execute_committed_intent(self, intent_row: Dict) -> bool:
        """
        Execute the action for a committed intent.

        On callback exception, immediately marks the intent as failed
        rather than leaving it in 'committed' for the recovery sweep.

        Args:
            intent_row: Intent data from database

        Returns:
            True if action executed successfully
        """
        intent_type = intent_row.get('intent_type')
        intent_id = intent_row.get('id')

        with self._callback_lock:
            callback = self._commit_callbacks.get(intent_type)

        if not callback:
            self._log(f"No callback registered for {intent_type}", level='warn')
            return False

        try:
            intent = Intent.from_dict(intent_row, intent_id)
            callback(intent)
            return True
        except Exception as e:
            reason = f"callback_exception: {e}"
            self._log(f"Failed to execute intent {intent_id}: {e}", level='warn')
            if intent_id:
                self.db.update_intent_status(intent_id, STATUS_FAILED, reason=reason)
            return False
    
    # =========================================================================
    # CLEANUP
    # =========================================================================
    
    def clear_intents_by_peer(self, peer_id: str) -> int:
        """
        Clear all intent locks held by a specific peer (e.g., on ban).

        Aborts pending DB intents and removes from remote cache.

        Args:
            peer_id: The peer whose intents to clear

        Returns:
            Number of intents cleared
        """
        cleared = 0

        # Clear from DB: abort any pending intents by this peer
        try:
            pending = self.db.get_pending_intents()
            for intent_row in pending:
                if intent_row.get("initiator") == peer_id:
                    intent_id = intent_row.get("id")
                    if intent_id:
                        self.db.update_intent_status(intent_id, STATUS_ABORTED, reason="peer_banned")
                        cleared += 1
        except Exception as e:
            self._log(f"Error clearing DB intents for {peer_id[:16]}...: {e}", level='warn')

        # Clear from remote cache
        with self._remote_lock:
            stale_keys = [
                key for key, intent in self._remote_intents.items()
                if intent.initiator == peer_id
            ]
            for key in stale_keys:
                del self._remote_intents[key]
            cleared += len(stale_keys)

        if cleared:
            self._log(f"Cleared {cleared} intents for peer {peer_id[:16]}...")

        return cleared

    def cleanup_expired_intents(self) -> int:
        """
        Clean up expired and stale intents.

        Returns:
            Number of intents cleaned up
        """
        count = self.db.cleanup_expired_intents()

        # Also clean up remote intent cache
        now = int(time.time())
        with self._remote_lock:
            stale_keys = [
                key for key, intent in self._remote_intents.items()
                if now > intent.expires_at + STALE_INTENT_THRESHOLD
            ]
            for key in stale_keys:
                del self._remote_intents[key]
        
        if count > 0 or stale_keys:
            self._log(f"Cleaned up {count} DB intents, {len(stale_keys)} cached remote intents")
        
        return count + len(stale_keys)
    
    def recover_stuck_intents(self, max_age_seconds: int = 300) -> int:
        """
        Recover intents stuck in 'committed' state.

        Intents that remain in 'committed' for longer than max_age_seconds
        are marked as 'failed', freeing up the target for new intents.

        Args:
            max_age_seconds: Max age in seconds before marking as failed

        Returns:
            Number of intents recovered
        """
        count = self.db.recover_stuck_intents(max_age_seconds)
        if count > 0:
            self._log(f"Recovered {count} stuck committed intent(s) older than {max_age_seconds}s")
        return count

    # =========================================================================
    # STATISTICS
    # =========================================================================

    def get_intent_stats(self) -> Dict[str, Any]:
        """
        Get statistics about current intents.
        
        Returns:
            Dict with intent metrics
        """
        with self._remote_lock:
            remote_count = len(self._remote_intents)
        with self._callback_lock:
            callbacks = list(self._commit_callbacks.keys())
        return {
            'hold_seconds': self.hold_seconds,
            'expire_seconds': self.expire_seconds,
            'our_pubkey': self.our_pubkey[:16] + '...' if self.our_pubkey else None,
            'remote_intents_cached': remote_count,
            'registered_callbacks': callbacks,
        }
