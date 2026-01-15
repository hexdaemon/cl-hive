"""
Budget Manager Module for cl-hive.

Manages temporary budget holds during cooperative expansion rounds
to prevent double-spending when multiple concurrent rounds could
claim the same liquidity.

Phase 8: Hive-wide Affordability Consensus

Author: Lightning Goats Team
"""

import time
import uuid
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional


# =============================================================================
# CONSTANTS
# =============================================================================

# Maximum duration for a budget hold (matches ROUND_EXPIRE_SECONDS)
MAX_HOLD_DURATION_SECONDS = 120

# Maximum concurrent holds per member (DoS protection)
MAX_CONCURRENT_HOLDS = 3

# Cleanup interval for expired holds
CLEANUP_INTERVAL_SECONDS = 60


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class BudgetHold:
    """
    Represents a temporary budget reservation during an expansion round.

    Attributes:
        hold_id: Unique identifier for this hold
        round_id: The expansion round this hold is for
        peer_id: The member who created this hold (our pubkey)
        amount_sats: Amount reserved
        created_at: Unix timestamp when hold was created
        expires_at: Unix timestamp when hold expires
        status: 'active', 'released', 'consumed', 'expired'
        consumed_by: Action ID or channel ID if consumed
        consumed_at: Unix timestamp when consumed
    """
    hold_id: str
    round_id: str
    peer_id: str
    amount_sats: int
    created_at: int
    expires_at: int
    status: str = "active"
    consumed_by: Optional[str] = None
    consumed_at: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BudgetHold':
        """Create from dictionary."""
        return cls(
            hold_id=data.get("hold_id", ""),
            round_id=data.get("round_id", ""),
            peer_id=data.get("peer_id", ""),
            amount_sats=data.get("amount_sats", 0),
            created_at=data.get("created_at", 0),
            expires_at=data.get("expires_at", 0),
            status=data.get("status", "active"),
            consumed_by=data.get("consumed_by"),
            consumed_at=data.get("consumed_at"),
        )

    def is_active(self) -> bool:
        """Check if hold is currently active (not expired/released)."""
        if self.status != "active":
            return False
        return int(time.time()) < self.expires_at


# =============================================================================
# BUDGET HOLD MANAGER CLASS
# =============================================================================

class BudgetHoldManager:
    """
    Manages temporary budget holds during expansion rounds.

    Holds are:
    - Created when nominating for a round (reserve funds)
    - Released when round completes or is cancelled
    - Consumed when the channel is actually opened
    - Enforced to prevent double-spending across concurrent rounds

    Thread Safety:
    - All database operations use thread-local connections
    - In-memory cache for fast lookups
    """

    def __init__(self, database, our_pubkey: str, plugin=None):
        """
        Initialize the BudgetHoldManager.

        Args:
            database: HiveDatabase instance for persistence
            our_pubkey: Our node's public key
            plugin: Optional plugin reference for logging
        """
        self.db = database
        self.our_pubkey = our_pubkey
        self.plugin = plugin

        # In-memory cache for active holds (hold_id -> BudgetHold)
        self._holds: Dict[str, BudgetHold] = {}

        # Track last cleanup time
        self._last_cleanup: int = 0

    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[BudgetManager] {msg}", level=level)

    def _generate_hold_id(self) -> str:
        """Generate a unique hold ID."""
        return f"hold_{uuid.uuid4().hex[:16]}"

    # =========================================================================
    # HOLD CREATION
    # =========================================================================

    def create_hold(self, round_id: str, amount_sats: int,
                    duration_seconds: int = MAX_HOLD_DURATION_SECONDS) -> Optional[str]:
        """
        Create a budget hold for an expansion round.

        Args:
            round_id: The expansion round ID
            amount_sats: Amount to reserve in satoshis
            duration_seconds: How long to hold (max MAX_HOLD_DURATION_SECONDS)

        Returns:
            hold_id if successful, None if failed (e.g., max holds reached)
        """
        # Cleanup expired holds first
        self.cleanup_expired_holds()

        # Check concurrent hold limit
        active_holds = self.get_active_holds()
        if len(active_holds) >= MAX_CONCURRENT_HOLDS:
            self._log(f"Cannot create hold: max concurrent holds ({MAX_CONCURRENT_HOLDS}) reached")
            return None

        # Check if we already have a hold for this round
        for hold in active_holds:
            if hold.round_id == round_id:
                self._log(f"Hold already exists for round {round_id[:8]}...")
                return hold.hold_id

        # Cap duration
        duration = min(duration_seconds, MAX_HOLD_DURATION_SECONDS)

        now = int(time.time())
        hold_id = self._generate_hold_id()

        hold = BudgetHold(
            hold_id=hold_id,
            round_id=round_id,
            peer_id=self.our_pubkey,
            amount_sats=amount_sats,
            created_at=now,
            expires_at=now + duration,
            status="active",
        )

        # Store in memory
        self._holds[hold_id] = hold

        # Persist to database
        if self.db:
            self.db.create_budget_hold(
                hold_id=hold_id,
                round_id=round_id,
                peer_id=self.our_pubkey,
                amount_sats=amount_sats,
                expires_seconds=duration,
            )

        self._log(f"Created budget hold {hold_id[:12]}... for {amount_sats:,} sats "
                  f"(round={round_id[:8]}..., expires in {duration}s)")

        return hold_id

    # =========================================================================
    # HOLD RELEASE
    # =========================================================================

    def release_hold(self, hold_id: str) -> bool:
        """
        Release a budget hold (round completed or cancelled).

        Args:
            hold_id: The hold ID to release

        Returns:
            True if released, False if not found or already released
        """
        hold = self._holds.get(hold_id)
        if not hold:
            # Try loading from database
            if self.db:
                hold_data = self.db.get_budget_hold(hold_id)
                if hold_data:
                    hold = BudgetHold.from_dict(hold_data)

        if not hold:
            self._log(f"Cannot release hold {hold_id}: not found")
            return False

        if hold.status != "active":
            self._log(f"Cannot release hold {hold_id}: status is {hold.status}")
            return False

        # Update status
        hold.status = "released"

        # Update in memory
        self._holds[hold_id] = hold

        # Update in database
        if self.db:
            self.db.release_budget_hold(hold_id)

        self._log(f"Released budget hold {hold_id[:12]}... ({hold.amount_sats:,} sats)")
        return True

    def release_holds_for_round(self, round_id: str) -> int:
        """
        Release all holds associated with a round.

        Args:
            round_id: The expansion round ID

        Returns:
            Number of holds released
        """
        released = 0
        for hold in list(self._holds.values()):
            if hold.round_id == round_id and hold.status == "active":
                if self.release_hold(hold.hold_id):
                    released += 1

        # Also check database for holds not in memory
        if self.db:
            db_holds = self.db.get_holds_for_round(round_id)
            for hold_data in db_holds:
                hold_id = hold_data.get("hold_id")
                if hold_id and hold_id not in self._holds:
                    if hold_data.get("status") == "active":
                        self.db.release_budget_hold(hold_id)
                        released += 1

        return released

    # =========================================================================
    # HOLD CONSUMPTION
    # =========================================================================

    def consume_hold(self, hold_id: str, consumed_by: str) -> bool:
        """
        Mark a hold as consumed (channel was opened).

        Args:
            hold_id: The hold ID to consume
            consumed_by: The action_id or channel_id that consumed the budget

        Returns:
            True if consumed, False if not found or not active
        """
        hold = self._holds.get(hold_id)
        if not hold:
            if self.db:
                hold_data = self.db.get_budget_hold(hold_id)
                if hold_data:
                    hold = BudgetHold.from_dict(hold_data)

        if not hold:
            self._log(f"Cannot consume hold {hold_id}: not found")
            return False

        if hold.status != "active":
            self._log(f"Cannot consume hold {hold_id}: status is {hold.status}")
            return False

        # Update status
        hold.status = "consumed"
        hold.consumed_by = consumed_by
        hold.consumed_at = int(time.time())

        # Update in memory
        self._holds[hold_id] = hold

        # Update in database
        if self.db:
            self.db.consume_budget_hold(hold_id, consumed_by)

        self._log(f"Consumed budget hold {hold_id[:12]}... by {consumed_by[:16]}...")
        return True

    # =========================================================================
    # BUDGET QUERIES
    # =========================================================================

    def get_available_budget(self, total_onchain_sats: int,
                              reserve_pct: float = 0.20) -> int:
        """
        Get available budget after accounting for active holds.

        Args:
            total_onchain_sats: Total onchain balance in satoshis
            reserve_pct: Percentage to keep in reserve

        Returns:
            Available budget in satoshis
        """
        # Calculate base spendable amount
        spendable = int(total_onchain_sats * (1.0 - reserve_pct))

        # Subtract active holds
        held = self.get_total_held()

        available = max(0, spendable - held)
        return available

    def get_total_held(self) -> int:
        """Get total amount held across all active holds."""
        self.cleanup_expired_holds()
        total = 0
        for hold in self._holds.values():
            if hold.is_active():
                total += hold.amount_sats
        return total

    def get_active_holds(self) -> List[BudgetHold]:
        """Get all currently active holds."""
        self.cleanup_expired_holds()
        return [h for h in self._holds.values() if h.is_active()]

    def get_hold(self, hold_id: str) -> Optional[BudgetHold]:
        """Get a specific hold by ID."""
        hold = self._holds.get(hold_id)
        if hold:
            return hold

        # Try database
        if self.db:
            hold_data = self.db.get_budget_hold(hold_id)
            if hold_data:
                return BudgetHold.from_dict(hold_data)

        return None

    def get_hold_for_round(self, round_id: str) -> Optional[BudgetHold]:
        """Get the active hold for a specific round, if any."""
        for hold in self._holds.values():
            if hold.round_id == round_id and hold.is_active():
                return hold
        return None

    def get_next_expiry(self) -> int:
        """Get the timestamp of the next hold expiry, or 0 if no active holds."""
        active = self.get_active_holds()
        if not active:
            return 0
        return min(h.expires_at for h in active)

    # =========================================================================
    # MAINTENANCE
    # =========================================================================

    def cleanup_expired_holds(self) -> int:
        """
        Mark expired holds as expired.

        Returns:
            Number of holds expired
        """
        now = int(time.time())

        # Rate limit cleanup
        if now - self._last_cleanup < CLEANUP_INTERVAL_SECONDS:
            return 0

        self._last_cleanup = now
        expired_count = 0

        for hold_id, hold in list(self._holds.items()):
            if hold.status == "active" and now >= hold.expires_at:
                hold.status = "expired"
                self._holds[hold_id] = hold

                if self.db:
                    self.db.expire_budget_hold(hold_id)

                expired_count += 1
                self._log(f"Expired budget hold {hold_id[:12]}...")

        return expired_count

    def load_from_database(self) -> int:
        """
        Load active holds from database into memory.

        Returns:
            Number of holds loaded
        """
        if not self.db:
            return 0

        holds = self.db.get_active_holds_for_peer(self.our_pubkey)
        loaded = 0

        for hold_data in holds:
            hold = BudgetHold.from_dict(hold_data)
            if hold.is_active():
                self._holds[hold.hold_id] = hold
                loaded += 1

        self._log(f"Loaded {loaded} active budget holds from database")
        return loaded

    # =========================================================================
    # STATISTICS
    # =========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get budget hold statistics."""
        active = self.get_active_holds()

        return {
            "active_holds": len(active),
            "total_held_sats": self.get_total_held(),
            "max_concurrent_holds": MAX_CONCURRENT_HOLDS,
            "next_expiry": self.get_next_expiry(),
            "holds": [
                {
                    "hold_id": h.hold_id[:12] + "...",
                    "round_id": h.round_id[:8] + "..." if h.round_id else "",
                    "amount_sats": h.amount_sats,
                    "expires_in_sec": max(0, h.expires_at - int(time.time())),
                }
                for h in active
            ],
        }
