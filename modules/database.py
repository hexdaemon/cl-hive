"""
Database module for cl-hive

Handles SQLite persistence for:
- Hive membership registry
- Intent locks for conflict resolution
- Hive state (HiveMap) cache
- Contribution ledger (anti-leech tracking)
- Ban list (distributed immunity)

Thread Safety:
- Uses threading.local() to provide each thread with its own SQLite connection
- Prevents race conditions during concurrent writes
"""

import sqlite3
import os
import time
import json
import threading
from typing import Dict, List, Optional, Any
from pathlib import Path


class HiveDatabase:
    """
    SQLite database manager for the Hive plugin.
    
    Provides persistence for:
    - Member registry (peer_id, tier, contribution, uptime)
    - Intent locks (conflict resolution)
    - Hive state cache (fleet topology view)
    - Contribution ledger (forwarding stats)
    - Ban list (shared immunity)
    
    Thread Safety:
    - Each thread gets its own isolated SQLite connection via threading.local()
    - WAL mode enabled for better concurrent read/write performance
    """
    
    def __init__(self, db_path: str, plugin):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to SQLite database file
            plugin: Reference to the pyln Plugin (or proxy) for logging
        """
        self.db_path = os.path.expanduser(db_path)
        self.plugin = plugin
        # Thread-local storage for connections
        self._local = threading.local()
        
    def _get_connection(self) -> sqlite3.Connection:
        """
        Get or create a thread-local database connection.
        
        Each thread gets its own isolated connection to prevent race conditions
        during concurrent database operations.
        
        Returns:
            sqlite3.Connection: Thread-local database connection
        """
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Create new connection for this thread
            self._local.conn = sqlite3.connect(
                self.db_path,
                isolation_level=None  # Autocommit mode
            )
            self._local.conn.row_factory = sqlite3.Row
            
            # Enable Write-Ahead Logging for better multi-thread concurrency
            self._local.conn.execute("PRAGMA journal_mode=WAL;")
            
            self.plugin.log(
                f"HiveDatabase: Created thread-local connection (thread={threading.current_thread().name})",
                level='debug'
            )
        return self._local.conn
    
    def initialize(self):
        """Create database tables if they don't exist."""
        conn = self._get_connection()
        
        # =====================================================================
        # HIVE MEMBERS TABLE
        # =====================================================================
        # Core membership registry tracking tier, contribution, and uptime
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hive_members (
                peer_id TEXT PRIMARY KEY,
                tier TEXT NOT NULL DEFAULT 'neophyte',
                joined_at INTEGER NOT NULL,
                promoted_at INTEGER,
                contribution_ratio REAL DEFAULT 0.0,
                uptime_pct REAL DEFAULT 0.0,
                vouch_count INTEGER DEFAULT 0,
                last_seen INTEGER,
                metadata TEXT
            )
        """)
        
        # =====================================================================
        # INTENT LOCKS TABLE
        # =====================================================================
        # Tracks Intent Lock protocol state for conflict resolution
        conn.execute("""
            CREATE TABLE IF NOT EXISTS intent_locks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                intent_type TEXT NOT NULL,
                target TEXT NOT NULL,
                initiator TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                signature TEXT
            )
        """)
        
        # Index for quick lookup of active intents by target
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_intent_locks_target 
            ON intent_locks(target, status)
        """)
        
        # =====================================================================
        # HIVE STATE TABLE
        # =====================================================================
        # Local cache of fleet state (HiveMap)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hive_state (
                peer_id TEXT PRIMARY KEY,
                capacity_sats INTEGER,
                available_sats INTEGER,
                fee_policy TEXT,
                topology TEXT,
                last_gossip INTEGER,
                state_hash TEXT,
                version INTEGER DEFAULT 0
            )
        """)
        
        # =====================================================================
        # CONTRIBUTION LEDGER TABLE
        # =====================================================================
        # Tracks forwarding events for contribution ratio calculation
        conn.execute("""
            CREATE TABLE IF NOT EXISTS contribution_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peer_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                timestamp INTEGER NOT NULL
            )
        """)
        
        # Index for efficient ratio calculation
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_contribution_peer_time 
            ON contribution_ledger(peer_id, timestamp)
        """)

        # =====================================================================
        # PROMOTION VOUCHES TABLE
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS promotion_vouches (
                target_peer_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                voucher_peer_id TEXT NOT NULL,
                sig TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                PRIMARY KEY (target_peer_id, request_id, voucher_peer_id)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_promotion_vouches_target_req
            ON promotion_vouches(target_peer_id, request_id)
        """)

        # =====================================================================
        # PROMOTION REQUESTS TABLE
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS promotion_requests (
                target_peer_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                PRIMARY KEY (target_peer_id, request_id)
            )
        """)

        # =====================================================================
        # PEER PRESENCE TABLE
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS peer_presence (
                peer_id TEXT PRIMARY KEY,
                last_change_ts INTEGER NOT NULL,
                is_online INTEGER NOT NULL,
                online_seconds_rolling INTEGER NOT NULL,
                window_start_ts INTEGER NOT NULL
            )
        """)

        # =====================================================================
        # LEECH FLAGS TABLE
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leech_flags (
                peer_id TEXT PRIMARY KEY,
                low_since_ts INTEGER NOT NULL,
                ban_triggered INTEGER NOT NULL DEFAULT 0
            )
        """)
        
        # =====================================================================
        # HIVE BANS TABLE
        # =====================================================================
        # Shared ban list for distributed immunity
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hive_bans (
                peer_id TEXT PRIMARY KEY,
                reason TEXT,
                reporter TEXT NOT NULL,
                signature TEXT,
                banned_at INTEGER NOT NULL,
                expires_at INTEGER
            )
        """)
        
        # =====================================================================
        # PENDING ACTIONS TABLE (Advisor Mode)
        # =====================================================================
        # Stores proposed actions awaiting operator approval
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                proposed_at INTEGER NOT NULL,
                expires_at INTEGER,
                status TEXT DEFAULT 'pending'
            )
        """)

        # =====================================================================
        # PLANNER LOG TABLE (Phase 6)
        # =====================================================================
        # Audit log for automated planner decisions
        conn.execute("""
            CREATE TABLE IF NOT EXISTS hive_planner_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                target TEXT,
                result TEXT NOT NULL,
                details TEXT
            )
        """)
        
        conn.execute("PRAGMA optimize;")
        self.plugin.log("HiveDatabase: Schema initialized")
    
    # =========================================================================
    # MEMBERSHIP OPERATIONS
    # =========================================================================
    
    def add_member(self, peer_id: str, tier: str = 'neophyte', 
                   joined_at: Optional[int] = None,
                   promoted_at: Optional[int] = None) -> bool:
        """
        Add a new member to the Hive.
        
        Args:
            peer_id: 66-character hex public key
            tier: 'admin', 'member', or 'neophyte'
            joined_at: Unix timestamp (defaults to now)
            promoted_at: Unix timestamp if promoted (None for neophytes)
            
        Returns:
            True if successful, False if member already exists
        """
        conn = self._get_connection()
        now = int(time.time())
        
        try:
            conn.execute("""
                INSERT INTO hive_members (peer_id, tier, joined_at, promoted_at, last_seen)
                VALUES (?, ?, ?, ?, ?)
            """, (peer_id, tier, joined_at or now, promoted_at, now))
            return True
        except sqlite3.IntegrityError:
            return False  # Already exists
    
    def get_member(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """Get member info by peer_id."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM hive_members WHERE peer_id = ?",
            (peer_id,)
        ).fetchone()
        return dict(row) if row else None
    
    def get_all_members(self) -> List[Dict[str, Any]]:
        """Get all Hive members."""
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT * FROM hive_members ORDER BY tier, joined_at"
        ).fetchall()
        return [dict(row) for row in rows]
    
    def update_member(self, peer_id: str, **kwargs) -> bool:
        """
        Update member fields.
        
        Allowed fields: tier, contribution_ratio, uptime_pct, vouch_count, 
                       last_seen, promoted_at, metadata
        """
        allowed = {'tier', 'contribution_ratio', 'uptime_pct', 'vouch_count',
                   'last_seen', 'promoted_at', 'metadata'}
        updates = {k: v for k, v in kwargs.items() if k in allowed}
        
        if not updates:
            return False
        
        conn = self._get_connection()
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [peer_id]
        
        result = conn.execute(
            f"UPDATE hive_members SET {set_clause} WHERE peer_id = ?",
            values
        )
        return result.rowcount > 0
    
    def remove_member(self, peer_id: str) -> bool:
        """Remove a member from the Hive."""
        conn = self._get_connection()
        result = conn.execute(
            "DELETE FROM hive_members WHERE peer_id = ?",
            (peer_id,)
        )
        return result.rowcount > 0
    
    def get_member_count_by_tier(self) -> Dict[str, int]:
        """Get count of members by tier."""
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT tier, COUNT(*) as count FROM hive_members GROUP BY tier"
        ).fetchall()
        return {row['tier']: row['count'] for row in rows}
    
    # =========================================================================
    # INTENT LOCK OPERATIONS
    # =========================================================================
    
    def create_intent(self, intent_type: str, target: str, initiator: str,
                      expires_seconds: int = 300) -> int:
        """
        Create a new Intent lock.
        
        Args:
            intent_type: 'channel_open', 'rebalance', 'ban_peer'
            target: Target peer_id or identifier
            initiator: Our node pubkey
            expires_seconds: Lock TTL
            
        Returns:
            Intent ID
        """
        conn = self._get_connection()
        now = int(time.time())
        expires = now + expires_seconds
        
        cursor = conn.execute("""
            INSERT INTO intent_locks (intent_type, target, initiator, timestamp, expires_at, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
        """, (intent_type, target, initiator, now, expires))
        
        return cursor.lastrowid
    
    def get_conflicting_intents(self, target: str, intent_type: str) -> List[Dict]:
        """Get active intents for the same target."""
        conn = self._get_connection()
        now = int(time.time())
        
        rows = conn.execute("""
            SELECT * FROM intent_locks 
            WHERE target = ? AND intent_type = ? AND status = 'pending' AND expires_at > ?
        """, (target, intent_type, now)).fetchall()
        
        return [dict(row) for row in rows]
    
    def update_intent_status(self, intent_id: int, status: str) -> bool:
        """Update Intent status: 'pending', 'committed', 'aborted'."""
        conn = self._get_connection()
        result = conn.execute(
            "UPDATE intent_locks SET status = ? WHERE id = ?",
            (status, intent_id)
        )
        return result.rowcount > 0
    
    def cleanup_expired_intents(self) -> int:
        """Remove expired Intent locks."""
        conn = self._get_connection()
        now = int(time.time())
        result = conn.execute(
            "DELETE FROM intent_locks WHERE expires_at < ?",
            (now,)
        )
        return result.rowcount
    
    def get_pending_intents_ready(self, hold_seconds: int) -> List[Dict]:
        """
        Get pending intents where hold period has elapsed.

        Args:
            hold_seconds: The hold period that must have passed

        Returns:
            List of intent rows ready to commit
        """
        conn = self._get_connection()
        now = int(time.time())
        cutoff = now - hold_seconds

        rows = conn.execute("""
            SELECT * FROM intent_locks
            WHERE status = 'pending' AND timestamp <= ? AND expires_at > ?
            ORDER BY timestamp
        """, (cutoff, now)).fetchall()

        return [dict(row) for row in rows]

    def get_pending_intents(self) -> List[Dict]:
        """
        Get all active pending intents.

        Returns:
            List of pending intent rows that haven't expired
        """
        conn = self._get_connection()
        now = int(time.time())

        rows = conn.execute("""
            SELECT * FROM intent_locks
            WHERE status = 'pending' AND expires_at > ?
            ORDER BY timestamp
        """, (now,)).fetchall()

        return [dict(row) for row in rows]

    def get_intent_by_id(self, intent_id: int) -> Optional[Dict]:
        """Get a specific intent by ID."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM intent_locks WHERE id = ?",
            (intent_id,)
        ).fetchone()
        return dict(row) if row else None
    
    # =========================================================================
    # HIVE STATE OPERATIONS
    # =========================================================================
    
    def update_hive_state(self, peer_id: str, capacity_sats: int,
                          available_sats: int, fee_policy: Dict,
                          topology: List[str], state_hash: str) -> None:
        """Update local cache of a peer's Hive state."""
        conn = self._get_connection()
        now = int(time.time())
        
        conn.execute("""
            INSERT OR REPLACE INTO hive_state 
            (peer_id, capacity_sats, available_sats, fee_policy, topology, 
             last_gossip, state_hash, version)
            VALUES (?, ?, ?, ?, ?, ?, ?, 
                    COALESCE((SELECT version FROM hive_state WHERE peer_id = ?), 0) + 1)
        """, (
            peer_id, capacity_sats, available_sats,
            json.dumps(fee_policy), json.dumps(topology),
            now, state_hash, peer_id
        ))
    
    def get_hive_state(self, peer_id: str) -> Optional[Dict]:
        """Get cached state for a Hive peer."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM hive_state WHERE peer_id = ?",
            (peer_id,)
        ).fetchone()
        
        if not row:
            return None
        
        result = dict(row)
        result['fee_policy'] = json.loads(result['fee_policy'] or '{}')
        result['topology'] = json.loads(result['topology'] or '[]')
        return result
    
    def get_all_hive_states(self) -> List[Dict]:
        """Get cached state for all Hive peers."""
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM hive_state").fetchall()
        
        results = []
        for row in rows:
            result = dict(row)
            result['fee_policy'] = json.loads(result['fee_policy'] or '{}')
            result['topology'] = json.loads(result['topology'] or '[]')
            results.append(result)
        return results
    
    # =========================================================================
    # CONTRIBUTION TRACKING
    # =========================================================================
    
    # P5-03: Absolute cap on contribution ledger rows to prevent unbounded DB growth
    MAX_CONTRIBUTION_ROWS = 500000

    def record_contribution(self, peer_id: str, direction: str,
                            amount_sats: int) -> bool:
        """
        Record a forwarding event for contribution tracking.

        P5-03: Rejects inserts if ledger exceeds MAX_CONTRIBUTION_ROWS.

        Args:
            peer_id: The Hive peer involved
            direction: 'forwarded' (we routed for them) or 'received' (they routed for us)
            amount_sats: Amount in satoshis

        Returns:
            True if recorded, False if rejected due to DB cap
        """
        conn = self._get_connection()

        # P5-03: Check absolute row limit before inserting
        row = conn.execute("SELECT COUNT(*) as cnt FROM contribution_ledger").fetchone()
        if row and row['cnt'] >= self.MAX_CONTRIBUTION_ROWS:
            self.plugin.log(
                f"HiveDatabase: Contribution ledger at cap ({self.MAX_CONTRIBUTION_ROWS}), rejecting insert",
                level='warn'
            )
            return False

        now = int(time.time())

        conn.execute("""
            INSERT INTO contribution_ledger (peer_id, direction, amount_sats, timestamp)
            VALUES (?, ?, ?, ?)
        """, (peer_id, direction, amount_sats, now))
        return True

    def get_contribution_stats(self, peer_id: str, window_days: int = 30) -> Dict[str, int]:
        """
        Get contribution totals within the window.
        
        Returns:
            Dict with forwarded and received totals in sats
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (window_days * 86400)
        
        rows = conn.execute("""
            SELECT direction, SUM(amount_sats) as total
            FROM contribution_ledger
            WHERE peer_id = ? AND timestamp > ?
            GROUP BY direction
        """, (peer_id, cutoff)).fetchall()
        
        forwarded = 0
        received = 0
        for row in rows:
            if row['direction'] == 'forwarded':
                forwarded = row['total'] or 0
            elif row['direction'] == 'received':
                received = row['total'] or 0
        
        return {"forwarded": forwarded, "received": received}
    
    def get_contribution_ratio(self, peer_id: str, window_days: int = 30) -> float:
        """
        Calculate contribution ratio: forwarded / received.
        
        A ratio > 1.0 means the peer contributes more than they take.
        A ratio < 1.0 means the peer is a net consumer (potential leech).
        
        Args:
            peer_id: Hive peer to check
            window_days: Lookback period
            
        Returns:
            Contribution ratio (default 1.0 if no data)
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (window_days * 86400)
        
        rows = conn.execute("""
            SELECT direction, SUM(amount_sats) as total
            FROM contribution_ledger
            WHERE peer_id = ? AND timestamp > ?
            GROUP BY direction
        """, (peer_id, cutoff)).fetchall()
        
        forwarded = 0
        received = 0
        for row in rows:
            if row['direction'] == 'forwarded':
                forwarded = row['total'] or 0
            elif row['direction'] == 'received':
                received = row['total'] or 0
        
        if received == 0:
            return 1.0 if forwarded == 0 else float('inf')
        
        return forwarded / received
    
    def prune_old_contributions(self, older_than_days: int = 45) -> int:
        """Remove contribution records older than specified days."""
        conn = self._get_connection()
        cutoff = int(time.time()) - (older_than_days * 86400)
        result = conn.execute(
            "DELETE FROM contribution_ledger WHERE timestamp < ?",
            (cutoff,)
        )
        return result.rowcount

    # =========================================================================
    # PROMOTION VOUCHES
    # =========================================================================

    def add_promotion_vouch(self, target_peer_id: str, request_id: str,
                            voucher_peer_id: str, sig: str, timestamp: int) -> bool:
        """Insert a promotion vouch (idempotent)."""
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT INTO promotion_vouches
                (target_peer_id, request_id, voucher_peer_id, sig, timestamp)
                VALUES (?, ?, ?, ?, ?)
            """, (target_peer_id, request_id, voucher_peer_id, sig, timestamp))
            return True
        except sqlite3.IntegrityError:
            return False

    def get_promotion_vouches(self, target_peer_id: str, request_id: str) -> List[Dict[str, Any]]:
        """Get vouches for a promotion request."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM promotion_vouches
            WHERE target_peer_id = ? AND request_id = ?
            ORDER BY timestamp
        """, (target_peer_id, request_id)).fetchall()
        return [dict(row) for row in rows]

    def prune_old_vouches(self, older_than_seconds: int) -> int:
        """Remove old vouches outside the TTL."""
        conn = self._get_connection()
        cutoff = int(time.time()) - older_than_seconds
        result = conn.execute(
            "DELETE FROM promotion_vouches WHERE timestamp < ?",
            (cutoff,)
        )
        return result.rowcount

    # =========================================================================
    # PROMOTION REQUESTS
    # =========================================================================

    def add_promotion_request(self, target_peer_id: str, request_id: str,
                              status: str = "pending") -> bool:
        """Record a promotion request (idempotent)."""
        conn = self._get_connection()
        now = int(time.time())
        try:
            conn.execute("""
                INSERT INTO promotion_requests (target_peer_id, request_id, status, created_at)
                VALUES (?, ?, ?, ?)
            """, (target_peer_id, request_id, status, now))
            return True
        except sqlite3.IntegrityError:
            return False

    def get_promotion_request(self, target_peer_id: str, request_id: str) -> Optional[Dict[str, Any]]:
        """Get a promotion request record."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM promotion_requests
            WHERE target_peer_id = ? AND request_id = ?
        """, (target_peer_id, request_id)).fetchone()
        return dict(row) if row else None

    def update_promotion_request_status(self, target_peer_id: str, request_id: str,
                                        status: str) -> bool:
        """Update a promotion request status."""
        conn = self._get_connection()
        result = conn.execute("""
            UPDATE promotion_requests
            SET status = ?
            WHERE target_peer_id = ? AND request_id = ?
        """, (status, target_peer_id, request_id))
        return result.rowcount > 0

    # =========================================================================
    # PEER PRESENCE
    # =========================================================================

    def get_presence(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """Get presence record for a peer."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM peer_presence WHERE peer_id = ?",
            (peer_id,)
        ).fetchone()
        return dict(row) if row else None

    def update_presence(self, peer_id: str, is_online: bool, now_ts: int,
                        window_seconds: int) -> None:
        """
        Update presence using a rolling accumulator.
        """
        conn = self._get_connection()
        existing = self.get_presence(peer_id)
        if not existing:
            conn.execute("""
                INSERT INTO peer_presence
                (peer_id, last_change_ts, is_online, online_seconds_rolling, window_start_ts)
                VALUES (?, ?, ?, ?, ?)
            """, (peer_id, now_ts, 1 if is_online else 0, 0, now_ts))
            return

        last_change_ts = existing["last_change_ts"]
        online_seconds = existing["online_seconds_rolling"]
        window_start_ts = existing["window_start_ts"]
        was_online = bool(existing["is_online"])

        if was_online:
            online_seconds += max(0, now_ts - last_change_ts)

        if now_ts - window_start_ts > window_seconds:
            window_start_ts = now_ts - window_seconds
            if online_seconds > window_seconds:
                online_seconds = window_seconds

        conn.execute("""
            UPDATE peer_presence
            SET last_change_ts = ?, is_online = ?, online_seconds_rolling = ?, window_start_ts = ?
            WHERE peer_id = ?
        """, (now_ts, 1 if is_online else 0, online_seconds, window_start_ts, peer_id))

    def prune_presence(self, window_seconds: int) -> int:
        """Clamp rolling windows to the configured window length."""
        conn = self._get_connection()
        now = int(time.time())
        cutoff = now - window_seconds
        result = conn.execute("""
            UPDATE peer_presence
            SET window_start_ts = ?, 
                online_seconds_rolling = CASE
                    WHEN online_seconds_rolling > ? THEN ?
                    ELSE online_seconds_rolling
                END
            WHERE window_start_ts < ?
        """, (cutoff, window_seconds, window_seconds, cutoff))
        return result.rowcount

    # =========================================================================
    # LEECH FLAGS
    # =========================================================================

    def get_leech_flag(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """Get leech flag for a peer."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM leech_flags WHERE peer_id = ?",
            (peer_id,)
        ).fetchone()
        return dict(row) if row else None

    def set_leech_flag(self, peer_id: str, low_since_ts: int, ban_triggered: bool) -> None:
        """Upsert a leech flag."""
        conn = self._get_connection()
        conn.execute("""
            INSERT INTO leech_flags (peer_id, low_since_ts, ban_triggered)
            VALUES (?, ?, ?)
            ON CONFLICT(peer_id) DO UPDATE SET
                low_since_ts = excluded.low_since_ts,
                ban_triggered = excluded.ban_triggered
        """, (peer_id, low_since_ts, 1 if ban_triggered else 0))

    def clear_leech_flag(self, peer_id: str) -> None:
        """Clear leech flag."""
        conn = self._get_connection()
        conn.execute(
            "DELETE FROM leech_flags WHERE peer_id = ?",
            (peer_id,)
        )
    
    # =========================================================================
    # BAN LIST OPERATIONS
    # =========================================================================
    
    def add_ban(self, peer_id: str, reason: str, reporter: str,
                signature: Optional[str] = None, 
                expires_days: Optional[int] = None) -> bool:
        """
        Add a peer to the ban list.
        
        Args:
            peer_id: Peer to ban
            reason: Human-readable reason
            reporter: Node that reported the ban
            signature: Cryptographic proof (optional)
            expires_days: Ban duration (None = permanent)
            
        Returns:
            True if added, False if already banned
        """
        conn = self._get_connection()
        now = int(time.time())
        expires = now + (expires_days * 86400) if expires_days else None
        
        try:
            conn.execute("""
                INSERT INTO hive_bans (peer_id, reason, reporter, signature, banned_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (peer_id, reason, reporter, signature, now, expires))
            return True
        except sqlite3.IntegrityError:
            return False
    
    def is_banned(self, peer_id: str) -> bool:
        """Check if a peer is banned."""
        conn = self._get_connection()
        now = int(time.time())
        
        row = conn.execute("""
            SELECT 1 FROM hive_bans 
            WHERE peer_id = ? AND (expires_at IS NULL OR expires_at > ?)
        """, (peer_id, now)).fetchone()
        
        return row is not None
    
    def get_ban_info(self, peer_id: str) -> Optional[Dict]:
        """Get ban details for a peer."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM hive_bans WHERE peer_id = ?",
            (peer_id,)
        ).fetchone()
        return dict(row) if row else None
    
    def remove_ban(self, peer_id: str) -> bool:
        """Remove a ban (unban a peer)."""
        conn = self._get_connection()
        result = conn.execute(
            "DELETE FROM hive_bans WHERE peer_id = ?",
            (peer_id,)
        )
        return result.rowcount > 0
    
    def get_all_bans(self) -> List[Dict]:
        """Get all active bans."""
        conn = self._get_connection()
        now = int(time.time())
        rows = conn.execute("""
            SELECT * FROM hive_bans 
            WHERE expires_at IS NULL OR expires_at > ?
        """, (now,)).fetchall()
        return [dict(row) for row in rows]
    
    # =========================================================================
    # PENDING ACTIONS (Advisor Mode)
    # =========================================================================
    
    def add_pending_action(self, action_type: str, payload: Dict,
                           expires_hours: int = 24) -> int:
        """
        Add a pending action for operator approval.
        
        Args:
            action_type: Type of action (e.g., 'channel_open', 'ban')
            payload: Action details as dict
            expires_hours: Hours until action expires
            
        Returns:
            Action ID
        """
        conn = self._get_connection()
        now = int(time.time())
        expires = now + (expires_hours * 3600)
        
        cursor = conn.execute("""
            INSERT INTO pending_actions (action_type, payload, proposed_at, expires_at, status)
            VALUES (?, ?, ?, ?, 'pending')
        """, (action_type, json.dumps(payload), now, expires))
        
        return cursor.lastrowid
    
    def get_pending_actions(self) -> List[Dict]:
        """Get all pending actions awaiting approval."""
        conn = self._get_connection()
        now = int(time.time())
        
        rows = conn.execute("""
            SELECT * FROM pending_actions 
            WHERE status = 'pending' AND expires_at > ?
            ORDER BY proposed_at
        """, (now,)).fetchall()
        
        results = []
        for row in rows:
            result = dict(row)
            result['payload'] = json.loads(result['payload'])
            results.append(result)
        return results
    
    def get_pending_action_by_id(self, action_id: int) -> Optional[Dict]:
        """Get a specific pending action by ID."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM pending_actions WHERE id = ?",
            (action_id,)
        ).fetchone()

        if not row:
            return None

        result = dict(row)
        result['payload'] = json.loads(result['payload'])
        return result

    def update_action_status(self, action_id: int, status: str) -> bool:
        """Update action status: 'pending', 'approved', 'rejected', 'expired'."""
        conn = self._get_connection()
        result = conn.execute(
            "UPDATE pending_actions SET status = ? WHERE id = ?",
            (status, action_id)
        )
        return result.rowcount > 0
    
    def cleanup_expired_actions(self) -> int:
        """Mark expired actions."""
        conn = self._get_connection()
        now = int(time.time())
        result = conn.execute(
            "UPDATE pending_actions SET status = 'expired' WHERE status = 'pending' AND expires_at < ?",
            (now,)
        )
        return result.rowcount

    # =========================================================================
    # PLANNER LOGGING (Phase 6)
    # =========================================================================

    def log_planner_action(self, action_type: str, result: str, 
                           target: Optional[str] = None, 
                           details: Optional[Dict[str, Any]] = None) -> None:
        """
        Log a decision made by the Planner.
        
        Args:
            action_type: What the planner did (e.g., 'saturation_check', 'expansion')
            result: Outcome ('success', 'skipped', 'failed', 'proposed')
            target: Target peer related to the action
            details: Additional context as dict
        """
        conn = self._get_connection()
        now = int(time.time())
        details_json = json.dumps(details) if details else None
        
        conn.execute("""
            INSERT INTO hive_planner_log (timestamp, action_type, target, result, details)
            VALUES (?, ?, ?, ?, ?)
        """, (now, action_type, target, result, details_json))

    def get_planner_logs(self, limit: int = 50) -> List[Dict]:
        """Get recent planner logs."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM hive_planner_log 
            ORDER BY timestamp DESC LIMIT ?
        """, (limit,)).fetchall()
        
        results = []
        for row in rows:
            result = dict(row)
            if result['details']:
                try:
                    result['details'] = json.loads(result['details'])
                except json.JSONDecodeError:
                    pass
            results.append(result)
        return results
