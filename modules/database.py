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
        # ADMIN PROMOTION TABLE (requires 100% admin approval)
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_promotions (
                target_peer_id TEXT PRIMARY KEY,
                proposed_by TEXT NOT NULL,
                proposed_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_promotion_approvals (
                target_peer_id TEXT NOT NULL,
                approver_peer_id TEXT NOT NULL,
                approved_at INTEGER NOT NULL,
                PRIMARY KEY (target_peer_id, approver_peer_id)
            )
        """)

        # =====================================================================
        # BAN PROPOSAL TABLES (Hybrid Governance)
        # =====================================================================
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ban_proposals (
                proposal_id TEXT PRIMARY KEY,
                target_peer_id TEXT NOT NULL,
                proposer_peer_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                proposed_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ban_votes (
                proposal_id TEXT NOT NULL,
                voter_peer_id TEXT NOT NULL,
                vote TEXT NOT NULL,
                voted_at INTEGER NOT NULL,
                signature TEXT NOT NULL,
                PRIMARY KEY (proposal_id, voter_peer_id)
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

        # =====================================================================
        # PEER EVENTS TABLE (Phase 6.1 - Topology Intelligence)
        # =====================================================================
        # Stores PEER_AVAILABLE events for quality metrics and topology decisions
        # Events include channel opens, closes, and quality reports from hive members
        conn.execute("""
            CREATE TABLE IF NOT EXISTS peer_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peer_id TEXT NOT NULL,
                reporter_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                channel_id TEXT,
                capacity_sats INTEGER DEFAULT 0,
                duration_days INTEGER DEFAULT 0,
                total_revenue_sats INTEGER DEFAULT 0,
                total_rebalance_cost_sats INTEGER DEFAULT 0,
                net_pnl_sats INTEGER DEFAULT 0,
                forward_count INTEGER DEFAULT 0,
                forward_volume_sats INTEGER DEFAULT 0,
                our_fee_ppm INTEGER DEFAULT 0,
                their_fee_ppm INTEGER DEFAULT 0,
                routing_score REAL DEFAULT 0.5,
                profitability_score REAL DEFAULT 0.5,
                our_funding_sats INTEGER DEFAULT 0,
                their_funding_sats INTEGER DEFAULT 0,
                opener TEXT,
                closer TEXT,
                reason TEXT
            )
        """)

        # Index for querying events by peer
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_peer_events_peer_ts
            ON peer_events(peer_id, timestamp DESC)
        """)

        # Index for querying events by type
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_peer_events_type_ts
            ON peer_events(event_type, timestamp DESC)
        """)

        # Index for querying events by reporter (hive member)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_peer_events_reporter
            ON peer_events(reporter_id, timestamp DESC)
        """)

        # =====================================================================
        # BUDGET TRACKING TABLE (Phase 6 - Autonomous Mode Limits)
        # =====================================================================
        # Tracks daily spending for autonomous mode budget enforcement
        conn.execute("""
            CREATE TABLE IF NOT EXISTS budget_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_key TEXT NOT NULL,
                action_type TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                target TEXT,
                action_id INTEGER,
                timestamp INTEGER NOT NULL
            )
        """)

        # Index for querying budget by date
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_budget_date_key
            ON budget_tracking(date_key)
        """)

        # =====================================================================
        # FEE INTELLIGENCE TABLE (Phase 7 - Cooperative Fee Coordination)
        # =====================================================================
        # Stores fee intelligence reports from hive members
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fee_intelligence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_id TEXT NOT NULL,
                target_peer_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                our_fee_ppm INTEGER,
                their_fee_ppm INTEGER,
                forward_count INTEGER,
                forward_volume_sats INTEGER,
                revenue_sats INTEGER,
                flow_direction TEXT,
                utilization_pct REAL,
                last_fee_change_ppm INTEGER,
                volume_delta_pct REAL,
                days_observed INTEGER,
                signature TEXT NOT NULL
            )
        """)

        # Index for querying by target peer
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_fee_intel_target
            ON fee_intelligence(target_peer_id)
        """)

        # Index for querying by reporter
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_fee_intel_reporter
            ON fee_intelligence(reporter_id)
        """)

        # =====================================================================
        # MEMBER HEALTH TABLE (Phase 7 - NNLB Health Tracking)
        # =====================================================================
        # Stores health reports from hive members for NNLB coordination
        conn.execute("""
            CREATE TABLE IF NOT EXISTS member_health (
                peer_id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                overall_health INTEGER,
                capacity_score INTEGER,
                revenue_score INTEGER,
                connectivity_score INTEGER,
                tier TEXT,
                needs_help INTEGER DEFAULT 0,
                can_help_others INTEGER DEFAULT 0,
                needs_inbound INTEGER DEFAULT 0,
                needs_outbound INTEGER DEFAULT 0,
                needs_channels INTEGER DEFAULT 0,
                assistance_budget_sats INTEGER DEFAULT 0
            )
        """)

        # =====================================================================
        # PEER FEE PROFILES TABLE (Phase 7 - Aggregated Fee Intelligence)
        # =====================================================================
        # Stores aggregated fee profiles for external peers
        conn.execute("""
            CREATE TABLE IF NOT EXISTS peer_fee_profiles (
                peer_id TEXT PRIMARY KEY,
                reporter_count INTEGER DEFAULT 0,
                avg_fee_charged REAL DEFAULT 0,
                min_fee_charged INTEGER DEFAULT 0,
                max_fee_charged INTEGER DEFAULT 0,
                total_hive_volume INTEGER DEFAULT 0,
                total_hive_revenue INTEGER DEFAULT 0,
                avg_utilization REAL DEFAULT 0,
                estimated_elasticity REAL DEFAULT 0,
                optimal_fee_estimate INTEGER DEFAULT 0,
                last_update INTEGER NOT NULL,
                confidence REAL DEFAULT 0
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

    def get_promotion_requests(self, target_peer_id: str) -> List[Dict[str, Any]]:
        """Get all promotion requests for a peer."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM promotion_requests
            WHERE target_peer_id = ?
            ORDER BY created_at DESC
        """, (target_peer_id,)).fetchall()
        return [dict(row) for row in rows]

    # =========================================================================
    # ADMIN PROMOTIONS (100% admin approval required)
    # =========================================================================

    def create_admin_promotion(self, target_peer_id: str, proposed_by: str) -> bool:
        """Create or update an admin promotion proposal."""
        conn = self._get_connection()
        now = int(time.time())
        try:
            conn.execute("""
                INSERT OR REPLACE INTO admin_promotions
                (target_peer_id, proposed_by, proposed_at, status)
                VALUES (?, ?, ?, 'pending')
            """, (target_peer_id, proposed_by, now))
            return True
        except Exception:
            return False

    def get_admin_promotion(self, target_peer_id: str) -> Optional[Dict[str, Any]]:
        """Get admin promotion proposal for a peer."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM admin_promotions WHERE target_peer_id = ?
        """, (target_peer_id,)).fetchone()
        return dict(row) if row else None

    def add_admin_promotion_approval(self, target_peer_id: str,
                                      approver_peer_id: str) -> bool:
        """Add an admin's approval for a promotion."""
        conn = self._get_connection()
        now = int(time.time())
        try:
            conn.execute("""
                INSERT OR REPLACE INTO admin_promotion_approvals
                (target_peer_id, approver_peer_id, approved_at)
                VALUES (?, ?, ?)
            """, (target_peer_id, approver_peer_id, now))
            return True
        except Exception:
            return False

    def get_admin_promotion_approvals(self, target_peer_id: str) -> List[Dict[str, Any]]:
        """Get all approvals for an admin promotion."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM admin_promotion_approvals WHERE target_peer_id = ?
        """, (target_peer_id,)).fetchall()
        return [dict(row) for row in rows]

    def complete_admin_promotion(self, target_peer_id: str) -> bool:
        """Mark admin promotion as complete."""
        conn = self._get_connection()
        try:
            conn.execute("""
                UPDATE admin_promotions SET status = 'complete'
                WHERE target_peer_id = ?
            """, (target_peer_id,))
            return True
        except Exception:
            return False

    def get_pending_admin_promotions(self) -> List[Dict[str, Any]]:
        """Get all pending admin promotions."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM admin_promotions WHERE status = 'pending'
        """).fetchall()
        return [dict(row) for row in rows]

    # =========================================================================
    # BAN PROPOSALS (Hybrid Governance)
    # =========================================================================

    def create_ban_proposal(self, proposal_id: str, target_peer_id: str,
                           proposer_peer_id: str, reason: str,
                           proposed_at: int, expires_at: int) -> bool:
        """Create a new ban proposal."""
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT INTO ban_proposals
                (proposal_id, target_peer_id, proposer_peer_id, reason,
                 proposed_at, expires_at, status)
                VALUES (?, ?, ?, ?, ?, ?, 'pending')
            """, (proposal_id, target_peer_id, proposer_peer_id, reason,
                  proposed_at, expires_at))
            conn.commit()
            return True
        except Exception:
            return False

    def get_ban_proposal(self, proposal_id: str) -> Optional[Dict[str, Any]]:
        """Get a ban proposal by ID."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM ban_proposals WHERE proposal_id = ?",
            (proposal_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_ban_proposal_for_target(self, target_peer_id: str) -> Optional[Dict[str, Any]]:
        """Get pending ban proposal for a target peer."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM ban_proposals
            WHERE target_peer_id = ? AND status = 'pending'
            ORDER BY proposed_at DESC LIMIT 1
        """, (target_peer_id,)).fetchone()
        return dict(row) if row else None

    def get_pending_ban_proposals(self) -> List[Dict[str, Any]]:
        """Get all pending ban proposals."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM ban_proposals WHERE status = 'pending'
            ORDER BY proposed_at DESC
        """).fetchall()
        return [dict(row) for row in rows]

    def update_ban_proposal_status(self, proposal_id: str, status: str) -> bool:
        """Update ban proposal status (pending, approved, rejected, expired)."""
        conn = self._get_connection()
        try:
            conn.execute("""
                UPDATE ban_proposals SET status = ? WHERE proposal_id = ?
            """, (status, proposal_id))
            conn.commit()
            return conn.total_changes > 0
        except Exception:
            return False

    def add_ban_vote(self, proposal_id: str, voter_peer_id: str,
                    vote: str, voted_at: int, signature: str) -> bool:
        """Add or update a vote on a ban proposal."""
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO ban_votes
                (proposal_id, voter_peer_id, vote, voted_at, signature)
                VALUES (?, ?, ?, ?, ?)
            """, (proposal_id, voter_peer_id, vote, voted_at, signature))
            conn.commit()
            return True
        except Exception:
            return False

    def get_ban_votes(self, proposal_id: str) -> List[Dict[str, Any]]:
        """Get all votes for a ban proposal."""
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT * FROM ban_votes WHERE proposal_id = ?",
            (proposal_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def get_ban_vote(self, proposal_id: str, voter_peer_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific vote on a ban proposal."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM ban_votes
            WHERE proposal_id = ? AND voter_peer_id = ?
        """, (proposal_id, voter_peer_id)).fetchone()
        return dict(row) if row else None

    def cleanup_expired_ban_proposals(self, now: int) -> int:
        """Mark expired ban proposals and return count."""
        conn = self._get_connection()
        conn.execute("""
            UPDATE ban_proposals
            SET status = 'expired'
            WHERE status = 'pending' AND expires_at < ?
        """, (now,))
        conn.commit()
        return conn.total_changes

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

    # Absolute cap on planner log rows (GEMINI.md Rule #2: Unbounded Input Protection)
    MAX_PLANNER_LOG_ROWS = 10000

    def log_planner_action(self, action_type: str, result: str,
                           target: Optional[str] = None,
                           details: Optional[Dict[str, Any]] = None) -> None:
        """
        Log a decision made by the Planner.

        Implements ring-buffer behavior: when MAX_PLANNER_LOG_ROWS is exceeded,
        oldest 10% of entries are pruned to make room.

        Args:
            action_type: What the planner did (e.g., 'saturation_check', 'expansion')
            result: Outcome ('success', 'skipped', 'failed', 'proposed')
            target: Target peer related to the action
            details: Additional context as dict
        """
        conn = self._get_connection()
        now = int(time.time())
        details_json = json.dumps(details) if details else None

        # Check row count and prune if at cap (ring-buffer behavior)
        row = conn.execute("SELECT COUNT(*) as cnt FROM hive_planner_log").fetchone()
        if row and row['cnt'] >= self.MAX_PLANNER_LOG_ROWS:
            # Delete oldest 10% to make room
            prune_count = self.MAX_PLANNER_LOG_ROWS // 10
            conn.execute("""
                DELETE FROM hive_planner_log WHERE id IN (
                    SELECT id FROM hive_planner_log ORDER BY timestamp ASC LIMIT ?
                )
            """, (prune_count,))
            self.plugin.log(
                f"HiveDatabase: Planner log at cap ({self.MAX_PLANNER_LOG_ROWS}), pruned {prune_count} oldest entries",
                level='debug'
            )

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

    def prune_planner_logs(self, older_than_days: int = 30) -> int:
        """
        Remove planner logs older than specified days.

        Args:
            older_than_days: Delete logs older than this many days (default: 30)

        Returns:
            Number of records deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (older_than_days * 86400)
        result = conn.execute(
            "DELETE FROM hive_planner_log WHERE timestamp < ?",
            (cutoff,)
        )
        return result.rowcount

    def prune_old_actions(self, older_than_days: int = 7) -> int:
        """
        Remove non-pending actions older than specified days.

        Only deletes actions that are already approved, rejected, or expired.
        Pending actions are left alone (they may still be reviewed).

        Args:
            older_than_days: Delete actions older than this many days (default: 7)

        Returns:
            Number of records deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (older_than_days * 86400)
        result = conn.execute("""
            DELETE FROM pending_actions
            WHERE status != 'pending' AND proposed_at < ?
        """, (cutoff,))
        return result.rowcount

    # =========================================================================
    # PEER EVENTS (Phase 6.1 - Topology Intelligence)
    # =========================================================================

    def store_peer_event(self, peer_id: str, reporter_id: str, event_type: str,
                         timestamp: int, channel_id: str = None,
                         capacity_sats: int = 0, duration_days: int = 0,
                         total_revenue_sats: int = 0, total_rebalance_cost_sats: int = 0,
                         net_pnl_sats: int = 0, forward_count: int = 0,
                         forward_volume_sats: int = 0, our_fee_ppm: int = 0,
                         their_fee_ppm: int = 0, routing_score: float = 0.5,
                         profitability_score: float = 0.5, our_funding_sats: int = 0,
                         their_funding_sats: int = 0, opener: str = None,
                         closer: str = None, reason: str = None) -> int:
        """
        Store a peer event from a PEER_AVAILABLE message.

        These events are used for:
        - Calculating peer quality scores (routing, profitability, stability)
        - Informing topology decisions (which peers to expand to)
        - Tracking channel lifecycle across the hive

        Args:
            peer_id: External peer involved in the event
            reporter_id: Hive member reporting the event
            event_type: Type of event (channel_open, remote_close, etc.)
            timestamp: Unix timestamp of the event
            channel_id: Channel short ID (if applicable)
            capacity_sats: Channel capacity
            duration_days: How long channel was open (for closes)
            total_revenue_sats: Routing fees earned
            total_rebalance_cost_sats: Rebalancing costs
            net_pnl_sats: Net profit/loss
            forward_count: Number of forwards routed
            forward_volume_sats: Total volume routed
            our_fee_ppm: Fee rate we charged
            their_fee_ppm: Fee rate they charged us
            routing_score: Routing quality score (0-1)
            profitability_score: Profitability score (0-1)
            our_funding_sats: Amount we funded (for opens)
            their_funding_sats: Amount they funded (for opens)
            opener: Who opened (local/remote)
            closer: Who closed (local/remote/mutual)
            reason: Human-readable reason

        Returns:
            ID of the inserted event, or -1 on failure
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute("""
                INSERT INTO peer_events (
                    peer_id, reporter_id, event_type, timestamp, channel_id,
                    capacity_sats, duration_days, total_revenue_sats,
                    total_rebalance_cost_sats, net_pnl_sats, forward_count,
                    forward_volume_sats, our_fee_ppm, their_fee_ppm,
                    routing_score, profitability_score, our_funding_sats,
                    their_funding_sats, opener, closer, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                peer_id, reporter_id, event_type, timestamp, channel_id,
                capacity_sats, duration_days, total_revenue_sats,
                total_rebalance_cost_sats, net_pnl_sats, forward_count,
                forward_volume_sats, our_fee_ppm, their_fee_ppm,
                routing_score, profitability_score, our_funding_sats,
                their_funding_sats, opener, closer, reason
            ))
            event_id = cursor.lastrowid
            self.plugin.log(
                f"Stored peer event: {event_type} for {peer_id[:16]}... "
                f"from {reporter_id[:16]}... (id={event_id})",
                level='debug'
            )
            return event_id
        except Exception as e:
            self.plugin.log(f"Failed to store peer event: {e}", level='error')
            return -1

    def get_peer_events(self, peer_id: str = None, event_type: str = None,
                        reporter_id: str = None, days: int = 90,
                        limit: int = 500) -> List[Dict[str, Any]]:
        """
        Query peer events with optional filters.

        Args:
            peer_id: Filter by external peer (optional)
            event_type: Filter by event type (optional)
            reporter_id: Filter by reporting hive member (optional)
            days: Only include events from last N days (default: 90)
            limit: Maximum number of events to return (default: 500)

        Returns:
            List of event dictionaries, ordered by timestamp descending
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days * 86400)

        query = "SELECT * FROM peer_events WHERE timestamp > ?"
        params = [cutoff]

        if peer_id:
            query += " AND peer_id = ?"
            params.append(peer_id)
        if event_type:
            query += " AND event_type = ?"
            params.append(event_type)
        if reporter_id:
            query += " AND reporter_id = ?"
            params.append(reporter_id)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_peer_event_summary(self, peer_id: str, days: int = 90) -> Dict[str, Any]:
        """
        Get aggregated event statistics for a peer.

        Useful for calculating quality scores and making topology decisions.

        Args:
            peer_id: The external peer to summarize
            days: Only include events from last N days (default: 90)

        Returns:
            Dict with aggregated statistics:
            - event_count: Total number of events
            - open_count: Number of channel opens
            - close_count: Number of channel closes
            - remote_close_count: Closes initiated by remote
            - local_close_count: Closes initiated by local
            - mutual_close_count: Mutual closes
            - total_revenue_sats: Sum of revenue across all closes
            - total_rebalance_cost_sats: Sum of rebalance costs
            - total_net_pnl_sats: Sum of net P&L
            - total_forward_count: Sum of forwards
            - avg_routing_score: Average routing score
            - avg_profitability_score: Average profitability score
            - avg_duration_days: Average channel duration
            - reporters: List of unique hive members who reported
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days * 86400)

        # Get all events for this peer
        rows = conn.execute("""
            SELECT * FROM peer_events
            WHERE peer_id = ? AND timestamp > ?
            ORDER BY timestamp DESC
        """, (peer_id, cutoff)).fetchall()

        events = [dict(row) for row in rows]

        if not events:
            return {
                "peer_id": peer_id,
                "event_count": 0,
                "open_count": 0,
                "close_count": 0,
                "remote_close_count": 0,
                "local_close_count": 0,
                "mutual_close_count": 0,
                "total_revenue_sats": 0,
                "total_rebalance_cost_sats": 0,
                "total_net_pnl_sats": 0,
                "total_forward_count": 0,
                "avg_routing_score": 0.5,
                "avg_profitability_score": 0.5,
                "avg_duration_days": 0,
                "reporters": []
            }

        # Aggregate statistics
        open_events = [e for e in events if e['event_type'] == 'channel_open']
        close_events = [e for e in events if e['event_type'].endswith('_close')]
        remote_closes = [e for e in close_events if e.get('closer') == 'remote']
        local_closes = [e for e in close_events if e.get('closer') == 'local']
        mutual_closes = [e for e in close_events if e.get('closer') == 'mutual']

        total_revenue = sum(e.get('total_revenue_sats', 0) for e in close_events)
        total_rebalance = sum(e.get('total_rebalance_cost_sats', 0) for e in close_events)
        total_pnl = sum(e.get('net_pnl_sats', 0) for e in close_events)
        total_forwards = sum(e.get('forward_count', 0) for e in close_events)

        routing_scores = [e.get('routing_score', 0.5) for e in events if e.get('routing_score')]
        profit_scores = [e.get('profitability_score', 0.5) for e in events if e.get('profitability_score')]
        durations = [e.get('duration_days', 0) for e in close_events if e.get('duration_days')]

        reporters = list(set(e['reporter_id'] for e in events))

        return {
            "peer_id": peer_id,
            "event_count": len(events),
            "open_count": len(open_events),
            "close_count": len(close_events),
            "remote_close_count": len(remote_closes),
            "local_close_count": len(local_closes),
            "mutual_close_count": len(mutual_closes),
            "total_revenue_sats": total_revenue,
            "total_rebalance_cost_sats": total_rebalance,
            "total_net_pnl_sats": total_pnl,
            "total_forward_count": total_forwards,
            "avg_routing_score": sum(routing_scores) / len(routing_scores) if routing_scores else 0.5,
            "avg_profitability_score": sum(profit_scores) / len(profit_scores) if profit_scores else 0.5,
            "avg_duration_days": sum(durations) / len(durations) if durations else 0,
            "reporters": reporters
        }

    def get_recent_channel_events(self, event_types: List[str] = None,
                                   days: int = 7, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent channel events across all peers.

        Useful for topology monitoring and cooperative expansion decisions.

        Args:
            event_types: Filter by event types (default: all)
            days: Only include events from last N days (default: 7)
            limit: Maximum number of events (default: 100)

        Returns:
            List of recent events with peer summaries
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days * 86400)

        if event_types:
            placeholders = ','.join('?' * len(event_types))
            query = f"""
                SELECT * FROM peer_events
                WHERE timestamp > ? AND event_type IN ({placeholders})
                ORDER BY timestamp DESC LIMIT ?
            """
            params = [cutoff] + event_types + [limit]
        else:
            query = """
                SELECT * FROM peer_events
                WHERE timestamp > ?
                ORDER BY timestamp DESC LIMIT ?
            """
            params = [cutoff, limit]

        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_peers_with_events(self, days: int = 90) -> List[str]:
        """
        Get list of all external peers that have event history.

        Args:
            days: Only include peers with events in last N days

        Returns:
            List of peer_id strings
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days * 86400)

        rows = conn.execute("""
            SELECT DISTINCT peer_id FROM peer_events
            WHERE timestamp > ?
        """, (cutoff,)).fetchall()

        return [row['peer_id'] for row in rows]

    def prune_peer_events(self, older_than_days: int = 180) -> int:
        """
        Remove peer events older than specified days.

        Keeps database size manageable while retaining useful history.

        Args:
            older_than_days: Delete events older than this (default: 180)

        Returns:
            Number of records deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (older_than_days * 86400)
        result = conn.execute(
            "DELETE FROM peer_events WHERE timestamp < ?",
            (cutoff,)
        )
        deleted = result.rowcount
        if deleted > 0:
            self.plugin.log(f"Pruned {deleted} old peer events", level='info')
        return deleted

    # =========================================================================
    # BUDGET TRACKING
    # =========================================================================

    def get_today_date_key(self) -> str:
        """Get today's date key in YYYY-MM-DD format (UTC)."""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime('%Y-%m-%d')

    def record_budget_spend(self, action_type: str, amount_sats: int,
                            target: str = None, action_id: int = None) -> bool:
        """
        Record a budget expenditure for tracking.

        Args:
            action_type: Type of action (channel_open, etc.)
            amount_sats: Amount spent in satoshis
            target: Optional target peer ID
            action_id: Optional action ID reference

        Returns:
            True if recorded successfully
        """
        conn = self._get_connection()
        date_key = self.get_today_date_key()
        now = int(time.time())

        try:
            conn.execute("""
                INSERT INTO budget_tracking
                (date_key, action_type, amount_sats, target, action_id, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (date_key, action_type, amount_sats, target, action_id, now))
            return True
        except Exception as e:
            self.plugin.log(f"Failed to record budget spend: {e}", level='error')
            return False

    def get_daily_spend(self, date_key: str = None) -> int:
        """
        Get total spending for a given day.

        Args:
            date_key: Date in YYYY-MM-DD format (default: today)

        Returns:
            Total satoshis spent on that day
        """
        conn = self._get_connection()
        if date_key is None:
            date_key = self.get_today_date_key()

        result = conn.execute("""
            SELECT COALESCE(SUM(amount_sats), 0) as total
            FROM budget_tracking WHERE date_key = ?
        """, (date_key,)).fetchone()

        return result['total'] if result else 0

    def get_available_budget(self, daily_budget: int) -> int:
        """
        Get available budget for today.

        Args:
            daily_budget: Configured daily budget in satoshis

        Returns:
            Available budget (daily_budget - spent today)
        """
        spent_today = self.get_daily_spend()
        available = max(0, daily_budget - spent_today)
        return available

    def get_budget_summary(self, daily_budget: int, days: int = 7) -> Dict[str, Any]:
        """
        Get budget summary for the past N days.

        Args:
            daily_budget: Configured daily budget
            days: Number of days to include (default: 7)

        Returns:
            Dict with budget summary info
        """
        conn = self._get_connection()
        from datetime import datetime, timezone, timedelta

        # Get spending for past N days
        today = datetime.now(timezone.utc)
        daily_spending = []

        for i in range(days):
            day = today - timedelta(days=i)
            date_key = day.strftime('%Y-%m-%d')
            spent = self.get_daily_spend(date_key)
            daily_spending.append({
                'date': date_key,
                'spent_sats': spent,
                'budget_sats': daily_budget,
                'utilization_pct': round((spent / daily_budget) * 100, 1) if daily_budget > 0 else 0
            })

        # Get today's details
        today_key = self.get_today_date_key()
        today_spent = self.get_daily_spend(today_key)
        available = max(0, daily_budget - today_spent)

        # Get action breakdown for today
        rows = conn.execute("""
            SELECT action_type, COUNT(*) as count, SUM(amount_sats) as total
            FROM budget_tracking WHERE date_key = ?
            GROUP BY action_type
        """, (today_key,)).fetchall()
        action_breakdown = {row['action_type']: {'count': row['count'], 'total': row['total']}
                          for row in rows}

        return {
            'today': {
                'date': today_key,
                'daily_budget_sats': daily_budget,
                'spent_sats': today_spent,
                'available_sats': available,
                'utilization_pct': round((today_spent / daily_budget) * 100, 1) if daily_budget > 0 else 0
            },
            'action_breakdown': action_breakdown,
            'history': daily_spending
        }

    # =========================================================================
    # FEE INTELLIGENCE OPERATIONS (Phase 7)
    # =========================================================================

    def store_fee_intelligence(
        self,
        reporter_id: str,
        target_peer_id: str,
        timestamp: int,
        our_fee_ppm: int,
        their_fee_ppm: int,
        forward_count: int,
        forward_volume_sats: int,
        revenue_sats: int,
        flow_direction: str,
        utilization_pct: float,
        signature: str,
        last_fee_change_ppm: int = 0,
        volume_delta_pct: float = 0.0,
        days_observed: int = 1
    ) -> int:
        """
        Store a fee intelligence report.

        Args:
            reporter_id: Hive member who reported this
            target_peer_id: External peer being reported on
            timestamp: Unix timestamp of the report
            our_fee_ppm: Fee charged to the peer
            their_fee_ppm: Fee the peer charges us
            forward_count: Number of forwards
            forward_volume_sats: Total volume routed
            revenue_sats: Fees earned from this peer
            flow_direction: 'source', 'sink', or 'balanced'
            utilization_pct: Channel utilization (0.0-1.0)
            signature: Cryptographic signature of the report
            last_fee_change_ppm: Previous fee rate
            volume_delta_pct: Volume change after fee change
            days_observed: How long peer has been observed

        Returns:
            ID of the inserted record
        """
        conn = self._get_connection()
        cursor = conn.execute("""
            INSERT INTO fee_intelligence (
                reporter_id, target_peer_id, timestamp, our_fee_ppm, their_fee_ppm,
                forward_count, forward_volume_sats, revenue_sats, flow_direction,
                utilization_pct, last_fee_change_ppm, volume_delta_pct, days_observed,
                signature
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            reporter_id, target_peer_id, timestamp, our_fee_ppm, their_fee_ppm,
            forward_count, forward_volume_sats, revenue_sats, flow_direction,
            utilization_pct, last_fee_change_ppm, volume_delta_pct, days_observed,
            signature
        ))
        return cursor.lastrowid

    def get_fee_intelligence_for_peer(
        self,
        target_peer_id: str,
        max_age_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get all fee intelligence reports for a specific external peer.

        Args:
            target_peer_id: External peer to get reports for
            max_age_hours: Maximum age of reports in hours

        Returns:
            List of fee intelligence reports
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)
        rows = conn.execute("""
            SELECT * FROM fee_intelligence
            WHERE target_peer_id = ? AND timestamp >= ?
            ORDER BY timestamp DESC
        """, (target_peer_id, cutoff)).fetchall()
        return [dict(row) for row in rows]

    def get_fee_intelligence_by_reporter(
        self,
        reporter_id: str,
        max_age_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get all fee intelligence reports from a specific reporter.

        Args:
            reporter_id: Hive member who reported
            max_age_hours: Maximum age of reports in hours

        Returns:
            List of fee intelligence reports
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)
        rows = conn.execute("""
            SELECT * FROM fee_intelligence
            WHERE reporter_id = ? AND timestamp >= ?
            ORDER BY timestamp DESC
        """, (reporter_id, cutoff)).fetchall()
        return [dict(row) for row in rows]

    def get_all_fee_intelligence(
        self,
        max_age_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get all recent fee intelligence reports.

        Args:
            max_age_hours: Maximum age of reports in hours

        Returns:
            List of fee intelligence reports
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)
        rows = conn.execute("""
            SELECT * FROM fee_intelligence
            WHERE timestamp >= ?
            ORDER BY target_peer_id, timestamp DESC
        """, (cutoff,)).fetchall()
        return [dict(row) for row in rows]

    def cleanup_old_fee_intelligence(self, max_age_hours: int = 168) -> int:
        """
        Remove old fee intelligence records.

        Args:
            max_age_hours: Maximum age to keep (default 7 days)

        Returns:
            Number of records deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)
        cursor = conn.execute("""
            DELETE FROM fee_intelligence WHERE timestamp < ?
        """, (cutoff,))
        return cursor.rowcount

    # =========================================================================
    # PEER FEE PROFILES OPERATIONS (Phase 7)
    # =========================================================================

    def update_peer_fee_profile(
        self,
        peer_id: str,
        reporter_count: int,
        avg_fee_charged: float,
        min_fee_charged: int,
        max_fee_charged: int,
        total_hive_volume: int,
        total_hive_revenue: int,
        avg_utilization: float,
        estimated_elasticity: float = 0.0,
        optimal_fee_estimate: int = 0,
        confidence: float = 0.5
    ) -> None:
        """
        Update or insert aggregated fee profile for an external peer.

        Args:
            peer_id: External peer ID
            reporter_count: Number of hive members reporting on this peer
            avg_fee_charged: Average fee charged by hive to this peer
            min_fee_charged: Minimum fee charged
            max_fee_charged: Maximum fee charged
            total_hive_volume: Total volume hive routes through this peer
            total_hive_revenue: Total revenue from this peer
            avg_utilization: Average channel utilization
            estimated_elasticity: Estimated price elasticity (-1 to 1)
            optimal_fee_estimate: Recommended optimal fee
            confidence: Confidence score (0-1)
        """
        conn = self._get_connection()
        now = int(time.time())
        conn.execute("""
            INSERT INTO peer_fee_profiles (
                peer_id, reporter_count, avg_fee_charged, min_fee_charged,
                max_fee_charged, total_hive_volume, total_hive_revenue,
                avg_utilization, estimated_elasticity, optimal_fee_estimate,
                last_update, confidence
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(peer_id) DO UPDATE SET
                reporter_count = excluded.reporter_count,
                avg_fee_charged = excluded.avg_fee_charged,
                min_fee_charged = excluded.min_fee_charged,
                max_fee_charged = excluded.max_fee_charged,
                total_hive_volume = excluded.total_hive_volume,
                total_hive_revenue = excluded.total_hive_revenue,
                avg_utilization = excluded.avg_utilization,
                estimated_elasticity = excluded.estimated_elasticity,
                optimal_fee_estimate = excluded.optimal_fee_estimate,
                last_update = excluded.last_update,
                confidence = excluded.confidence
        """, (
            peer_id, reporter_count, avg_fee_charged, min_fee_charged,
            max_fee_charged, total_hive_volume, total_hive_revenue,
            avg_utilization, estimated_elasticity, optimal_fee_estimate,
            now, confidence
        ))

    def get_peer_fee_profile(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get aggregated fee profile for an external peer.

        Args:
            peer_id: External peer ID

        Returns:
            Fee profile dict or None if not found
        """
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM peer_fee_profiles WHERE peer_id = ?
        """, (peer_id,)).fetchone()
        return dict(row) if row else None

    def get_all_peer_fee_profiles(self) -> List[Dict[str, Any]]:
        """
        Get all aggregated fee profiles.

        Returns:
            List of fee profile dicts
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM peer_fee_profiles ORDER BY reporter_count DESC
        """).fetchall()
        return [dict(row) for row in rows]

    # =========================================================================
    # MEMBER HEALTH OPERATIONS (Phase 7 - NNLB)
    # =========================================================================

    def update_member_health(
        self,
        peer_id: str,
        overall_health: int,
        capacity_score: int,
        revenue_score: int,
        connectivity_score: int,
        tier: str = 'healthy',
        needs_help: bool = False,
        can_help_others: bool = False,
        needs_inbound: bool = False,
        needs_outbound: bool = False,
        needs_channels: bool = False,
        assistance_budget_sats: int = 0
    ) -> None:
        """
        Update health record for a hive member.

        Args:
            peer_id: Hive member peer ID
            overall_health: Overall health score (0-100)
            capacity_score: Capacity score (0-100)
            revenue_score: Revenue score (0-100)
            connectivity_score: Connectivity score (0-100)
            tier: 'thriving', 'healthy', 'struggling', or 'critical'
            needs_help: Whether member needs assistance
            can_help_others: Whether member can provide assistance
            needs_inbound: Whether member needs inbound liquidity
            needs_outbound: Whether member needs outbound liquidity
            needs_channels: Whether member needs more channels
            assistance_budget_sats: How much member can spend helping
        """
        conn = self._get_connection()
        now = int(time.time())
        conn.execute("""
            INSERT INTO member_health (
                peer_id, timestamp, overall_health, capacity_score,
                revenue_score, connectivity_score, tier, needs_help,
                can_help_others, needs_inbound, needs_outbound,
                needs_channels, assistance_budget_sats
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(peer_id) DO UPDATE SET
                timestamp = excluded.timestamp,
                overall_health = excluded.overall_health,
                capacity_score = excluded.capacity_score,
                revenue_score = excluded.revenue_score,
                connectivity_score = excluded.connectivity_score,
                tier = excluded.tier,
                needs_help = excluded.needs_help,
                can_help_others = excluded.can_help_others,
                needs_inbound = excluded.needs_inbound,
                needs_outbound = excluded.needs_outbound,
                needs_channels = excluded.needs_channels,
                assistance_budget_sats = excluded.assistance_budget_sats
        """, (
            peer_id, now, overall_health, capacity_score,
            revenue_score, connectivity_score, tier,
            1 if needs_help else 0,
            1 if can_help_others else 0,
            1 if needs_inbound else 0,
            1 if needs_outbound else 0,
            1 if needs_channels else 0,
            assistance_budget_sats
        ))

    def get_member_health(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get health record for a hive member.

        Args:
            peer_id: Hive member peer ID

        Returns:
            Health record dict or None if not found
        """
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM member_health WHERE peer_id = ?
        """, (peer_id,)).fetchone()
        if not row:
            return None
        result = dict(row)
        # Convert integer flags to booleans
        result['needs_help'] = bool(result.get('needs_help', 0))
        result['can_help_others'] = bool(result.get('can_help_others', 0))
        result['needs_inbound'] = bool(result.get('needs_inbound', 0))
        result['needs_outbound'] = bool(result.get('needs_outbound', 0))
        result['needs_channels'] = bool(result.get('needs_channels', 0))
        return result

    def get_all_member_health(self) -> List[Dict[str, Any]]:
        """
        Get health records for all hive members.

        Returns:
            List of health record dicts
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM member_health ORDER BY overall_health ASC
        """).fetchall()
        results = []
        for row in rows:
            result = dict(row)
            result['needs_help'] = bool(result.get('needs_help', 0))
            result['can_help_others'] = bool(result.get('can_help_others', 0))
            result['needs_inbound'] = bool(result.get('needs_inbound', 0))
            result['needs_outbound'] = bool(result.get('needs_outbound', 0))
            result['needs_channels'] = bool(result.get('needs_channels', 0))
            results.append(result)
        return results

    def get_struggling_members(self, threshold: int = 40) -> List[Dict[str, Any]]:
        """
        Get members with health below threshold (NNLB candidates).

        Args:
            threshold: Health score threshold (default 40)

        Returns:
            List of health records for struggling members
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM member_health
            WHERE overall_health < ? OR needs_help = 1
            ORDER BY overall_health ASC
        """, (threshold,)).fetchall()
        results = []
        for row in rows:
            result = dict(row)
            result['needs_help'] = bool(result.get('needs_help', 0))
            result['can_help_others'] = bool(result.get('can_help_others', 0))
            result['needs_inbound'] = bool(result.get('needs_inbound', 0))
            result['needs_outbound'] = bool(result.get('needs_outbound', 0))
            result['needs_channels'] = bool(result.get('needs_channels', 0))
            results.append(result)
        return results

    def get_helping_members(self) -> List[Dict[str, Any]]:
        """
        Get members who can provide assistance to others.

        Returns:
            List of health records for members who can help
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM member_health
            WHERE can_help_others = 1
            ORDER BY overall_health DESC
        """).fetchall()
        results = []
        for row in rows:
            result = dict(row)
            result['needs_help'] = bool(result.get('needs_help', 0))
            result['can_help_others'] = bool(result.get('can_help_others', 0))
            result['needs_inbound'] = bool(result.get('needs_inbound', 0))
            result['needs_outbound'] = bool(result.get('needs_outbound', 0))
            result['needs_channels'] = bool(result.get('needs_channels', 0))
            results.append(result)
        return results
