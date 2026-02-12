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
import hashlib
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Tuple, Generator
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
            # Use isolation_level=None (autocommit mode) - each statement commits immediately.
            # This prevents long-running implicit transactions from holding locks.
            # For explicit transactions, use BEGIN/COMMIT/ROLLBACK directly.
            self._local.conn = sqlite3.connect(
                self.db_path,
                isolation_level=None,  # Autocommit mode - critical for multi-threaded access
                timeout=30.0  # Wait up to 30s for locks instead of failing immediately
            )
            self._local.conn.row_factory = sqlite3.Row

            # Enable Write-Ahead Logging for better multi-thread concurrency
            self._local.conn.execute("PRAGMA journal_mode=WAL;")
            
            self.plugin.log(
                f"HiveDatabase: Created thread-local connection (thread={threading.current_thread().name})",
                level='debug'
            )
        return self._local.conn

    def close_connection(self):
        """Close the thread-local connection if it exists."""
        conn = getattr(self._local, 'conn', None)
        if conn:
            try:
                conn.close()
            except Exception:
                pass
            self._local.conn = None

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Context manager for atomic database transactions.

        Use this for multi-step operations that must succeed or fail together.

        Example:
            with self.transaction() as conn:
                conn.execute("INSERT INTO table1 ...")
                conn.execute("INSERT INTO table2 ...")
            # Both inserts committed, or both rolled back on error

        Yields:
            sqlite3.Connection: The thread-local connection in transaction mode
        """
        conn = self._get_connection()
        try:
            # BEGIN IMMEDIATE acquires write lock immediately, preventing deadlocks
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass  # Don't mask the original exception
            raise

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
                metadata TEXT,
                addresses TEXT
            )
        """)
        # Add addresses column if upgrading from older schema
        try:
            conn.execute(
                "ALTER TABLE hive_members ADD COLUMN addresses TEXT"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists

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

        # Add reason column for audit trail if upgrading from older schema
        try:
            conn.execute(
                "ALTER TABLE intent_locks ADD COLUMN reason TEXT"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists

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
        # MANUAL PROMOTION TABLE (requires majority member approval)
        # NOTE: Table name kept as admin_promotions for backward compatibility
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
                status TEXT NOT NULL DEFAULT 'pending',
                proposal_type TEXT NOT NULL DEFAULT 'standard'
            )
        """)
        # Add proposal_type column if upgrading from older schema
        try:
            conn.execute(
                "ALTER TABLE ban_proposals ADD COLUMN proposal_type TEXT NOT NULL DEFAULT 'standard'"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists

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
        # LOCAL FEE TRACKING TABLE (Settlement Phase)
        # =====================================================================
        # Persists fee tracking state across restarts to prevent revenue loss
        conn.execute("""
            CREATE TABLE IF NOT EXISTS local_fee_tracking (
                id INTEGER PRIMARY KEY DEFAULT 1,
                earned_sats INTEGER NOT NULL DEFAULT 0,
                forward_count INTEGER NOT NULL DEFAULT 0,
                period_start_ts INTEGER NOT NULL DEFAULT 0,
                last_broadcast_ts INTEGER NOT NULL DEFAULT 0,
                last_broadcast_amount INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL DEFAULT 0
            )
        """)

        # =====================================================================
        # CONTRIBUTION RATE LIMITS TABLE
        # =====================================================================
        # Persists rate limit state across restarts to prevent bypass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS contribution_rate_limits (
                peer_id TEXT PRIMARY KEY,
                window_start INTEGER NOT NULL,
                event_count INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rate_limits_window "
            "ON contribution_rate_limits(window_start)"
        )

        conn.execute("""
            CREATE TABLE IF NOT EXISTS contribution_daily_stats (
                id INTEGER PRIMARY KEY DEFAULT 1,
                window_start_ts INTEGER NOT NULL DEFAULT 0,
                event_count INTEGER NOT NULL DEFAULT 0
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
                status TEXT DEFAULT 'pending',
                rejection_reason TEXT
            )
        """)
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_pending_actions_status_expires
            ON pending_actions(status, expires_at)""")
        conn.execute("""CREATE INDEX IF NOT EXISTS idx_pending_actions_type_proposed
            ON pending_actions(action_type, proposed_at)""")

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
        # PLANNER IGNORED PEERS TABLE
        # =====================================================================
        # Persistent storage for manually ignored peers (prevents planner from
        # opening channels to these peers until released)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS planner_ignored_peers (
                peer_id TEXT PRIMARY KEY,
                ignored_at INTEGER NOT NULL,
                reason TEXT,
                expires_at INTEGER
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
        # BUDGET HOLDS TABLE (Phase 8 - Hive-wide Affordability)
        # =====================================================================
        # Temporary budget reservations during expansion rounds
        conn.execute("""
            CREATE TABLE IF NOT EXISTS budget_holds (
                hold_id TEXT PRIMARY KEY,
                round_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                consumed_by TEXT,
                consumed_at INTEGER
            )
        """)

        # Index for querying holds by peer and status
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_budget_holds_peer_status
            ON budget_holds(peer_id, status)
        """)

        # Index for querying holds by round
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_budget_holds_round
            ON budget_holds(round_id)
        """)

        # Index for querying expiring holds
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_budget_holds_expires
            ON budget_holds(expires_at) WHERE status = 'active'
        """)

        # =====================================================================
        # DELEGATION ATTEMPTS TABLE (Phase 8 - Cooperative Failure Handling)
        # =====================================================================
        # Tracks channel open delegation attempts when local opens fail
        conn.execute("""
            CREATE TABLE IF NOT EXISTS delegation_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                original_action_id INTEGER NOT NULL,
                target TEXT NOT NULL,
                delegation_count INTEGER NOT NULL,
                failure_type TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                completed_by TEXT,
                completed_at INTEGER
            )
        """)

        # Index for querying by target
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_delegation_target
            ON delegation_attempts(target)
        """)

        # =====================================================================
        # TASK REQUESTS TABLE (Phase 10 - Task Delegation Protocol)
        # =====================================================================
        # Tracks tasks we've requested from other hive members
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_requests_outgoing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL UNIQUE,
                target_member_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                task_target TEXT NOT NULL,
                amount_sats INTEGER,
                priority TEXT NOT NULL,
                deadline_timestamp INTEGER NOT NULL,
                failure_context TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at INTEGER NOT NULL,
                responded_at INTEGER,
                response_status TEXT,
                response_reason TEXT,
                result_data TEXT
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_outgoing_status
            ON task_requests_outgoing(status)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_outgoing_member
            ON task_requests_outgoing(target_member_id)
        """)

        # =====================================================================
        # TASK ASSIGNMENTS TABLE (Phase 10 - Task Delegation Protocol)
        # =====================================================================
        # Tracks tasks other hive members have requested from us
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_requests_incoming (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL UNIQUE,
                requester_id TEXT NOT NULL,
                task_type TEXT NOT NULL,
                task_target TEXT NOT NULL,
                amount_sats INTEGER,
                priority TEXT NOT NULL,
                deadline_timestamp INTEGER NOT NULL,
                failure_context TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                received_at INTEGER NOT NULL,
                accepted_at INTEGER,
                completed_at INTEGER,
                result_data TEXT,
                failure_reason TEXT
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_incoming_status
            ON task_requests_incoming(status)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_task_incoming_requester
            ON task_requests_incoming(requester_id)
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

        # =====================================================================
        # LIQUIDITY NEEDS TABLE (Phase 7.3 - Cooperative Rebalancing)
        # =====================================================================
        # Stores liquidity needs broadcast by hive members
        conn.execute("""
            CREATE TABLE IF NOT EXISTS liquidity_needs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_id TEXT NOT NULL,
                need_type TEXT NOT NULL,
                target_peer_id TEXT,
                amount_sats INTEGER NOT NULL,
                urgency TEXT DEFAULT 'medium',
                max_fee_ppm INTEGER DEFAULT 0,
                reason TEXT,
                current_balance_pct REAL DEFAULT 0.5,
                timestamp INTEGER NOT NULL,
                UNIQUE(reporter_id, target_peer_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_liquidity_needs_reporter "
            "ON liquidity_needs(reporter_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_liquidity_needs_urgency "
            "ON liquidity_needs(urgency)"
        )

        # =====================================================================
        # MEMBER LIQUIDITY STATE TABLE (Phase 2 - Liquidity Intelligence)
        # =====================================================================
        # Stores current liquidity state from cl-revenue-ops reports.
        # INFORMATION ONLY - no fund transfers, enables coordinated decisions.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS member_liquidity_state (
                peer_id TEXT PRIMARY KEY,
                depleted_count INTEGER DEFAULT 0,
                saturated_count INTEGER DEFAULT 0,
                rebalancing_active INTEGER DEFAULT 0,
                rebalancing_peers TEXT DEFAULT '[]',
                timestamp INTEGER NOT NULL
            )
        """)

        # =====================================================================
        # ROUTE PROBES TABLE (Phase 7.4 - Routing Intelligence)
        # =====================================================================
        # Stores route probe observations from hive members
        conn.execute("""
            CREATE TABLE IF NOT EXISTS route_probes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_id TEXT NOT NULL,
                destination TEXT NOT NULL,
                path TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                success INTEGER NOT NULL,
                latency_ms INTEGER DEFAULT 0,
                failure_reason TEXT DEFAULT '',
                failure_hop INTEGER DEFAULT -1,
                estimated_capacity_sats INTEGER DEFAULT 0,
                total_fee_ppm INTEGER DEFAULT 0,
                amount_probed_sats INTEGER DEFAULT 0,
                UNIQUE(reporter_id, destination, path, timestamp)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_route_probes_destination "
            "ON route_probes(destination)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_route_probes_timestamp "
            "ON route_probes(timestamp)"
        )

        # =====================================================================
        # PEER REPUTATION TABLE (Phase 5 - Advanced Cooperation)
        # =====================================================================
        # Stores reputation reports about external peers from hive members
        conn.execute("""
            CREATE TABLE IF NOT EXISTS peer_reputation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reporter_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                uptime_pct REAL DEFAULT 1.0,
                response_time_ms INTEGER DEFAULT 0,
                force_close_count INTEGER DEFAULT 0,
                fee_stability REAL DEFAULT 1.0,
                htlc_success_rate REAL DEFAULT 1.0,
                channel_age_days INTEGER DEFAULT 0,
                total_routed_sats INTEGER DEFAULT 0,
                warnings TEXT DEFAULT '[]',
                observation_days INTEGER DEFAULT 7
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_peer_reputation_peer_id "
            "ON peer_reputation(peer_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_peer_reputation_timestamp "
            "ON peer_reputation(timestamp)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_peer_reputation_reporter "
            "ON peer_reputation(reporter_id)"
        )

        # =====================================================================
        # ROUTING POOL TABLES (Phase 0 - Collective Economics)
        # =====================================================================

        # Pool contributions - snapshot of member contributions per period
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pool_contributions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                member_id TEXT NOT NULL,
                period TEXT NOT NULL,

                -- Capital metrics (70% weight)
                total_capacity_sats INTEGER DEFAULT 0,
                weighted_capacity_sats INTEGER DEFAULT 0,
                uptime_pct REAL DEFAULT 0.0,

                -- Position metrics (20% weight)
                betweenness_centrality REAL DEFAULT 0.0,
                unique_peers INTEGER DEFAULT 0,
                bridge_score REAL DEFAULT 0.0,

                -- Operations metrics (10% weight)
                routing_success_rate REAL DEFAULT 1.0,
                avg_response_time_ms REAL DEFAULT 0.0,

                -- Computed share
                pool_share REAL DEFAULT 0.0,

                recorded_at INTEGER NOT NULL,
                UNIQUE(member_id, period)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pool_contributions_period "
            "ON pool_contributions(period)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pool_contributions_member "
            "ON pool_contributions(member_id)"
        )

        # Pool revenue - individual routing revenue events
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pool_revenue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                member_id TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                channel_id TEXT,
                payment_hash TEXT,
                recorded_at INTEGER NOT NULL,
                UNIQUE(payment_hash) ON CONFLICT IGNORE
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pool_revenue_recorded "
            "ON pool_revenue(recorded_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pool_revenue_member "
            "ON pool_revenue(member_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pool_revenue_payment_hash "
            "ON pool_revenue(payment_hash)"
        )

        # Pool distributions - settlement records
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pool_distributions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period TEXT NOT NULL,
                member_id TEXT NOT NULL,
                contribution_share REAL NOT NULL,
                revenue_share_sats INTEGER NOT NULL,
                total_pool_revenue_sats INTEGER NOT NULL,
                settled_at INTEGER NOT NULL,
                UNIQUE(member_id, period)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pool_distributions_period "
            "ON pool_distributions(period)"
        )

        # =====================================================================
        # FLOW SAMPLES TABLE (Phase 7.1 - Anticipatory Liquidity)
        # =====================================================================
        # Stores hourly flow samples for temporal pattern detection
        conn.execute("""
            CREATE TABLE IF NOT EXISTS flow_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                hour INTEGER NOT NULL,
                day_of_week INTEGER NOT NULL,
                inbound_sats INTEGER NOT NULL DEFAULT 0,
                outbound_sats INTEGER NOT NULL DEFAULT 0,
                net_flow_sats INTEGER NOT NULL DEFAULT 0,
                timestamp INTEGER NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_flow_samples_channel_ts "
            "ON flow_samples(channel_id, timestamp DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_flow_samples_hour "
            "ON flow_samples(hour)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_flow_samples_day "
            "ON flow_samples(day_of_week)"
        )

        # =====================================================================
        # TEMPORAL PATTERNS TABLE (Phase 7.1 - Anticipatory Liquidity)
        # =====================================================================
        # Stores detected temporal patterns for liquidity prediction
        conn.execute("""
            CREATE TABLE IF NOT EXISTS temporal_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                hour_of_day INTEGER,
                day_of_week INTEGER,
                direction TEXT NOT NULL,
                intensity REAL NOT NULL DEFAULT 1.0,
                confidence REAL NOT NULL DEFAULT 0.5,
                samples INTEGER NOT NULL DEFAULT 0,
                avg_flow_sats INTEGER NOT NULL DEFAULT 0,
                detected_at INTEGER NOT NULL,
                UNIQUE(channel_id, hour_of_day, day_of_week)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_temporal_patterns_channel "
            "ON temporal_patterns(channel_id)"
        )

        # =====================================================================
        # SPLICE SESSIONS TABLE (Phase 11 - Hive-Splice Coordination)
        # =====================================================================
        # Tracks splice operations coordinated between hive members
        conn.execute("""
            CREATE TABLE IF NOT EXISTS splice_sessions (
                session_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                initiator TEXT NOT NULL,
                splice_type TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                psbt TEXT,
                commitments_secured INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                txid TEXT,
                error_message TEXT,
                timeout_at INTEGER NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_splice_sessions_channel "
            "ON splice_sessions(channel_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_splice_sessions_peer "
            "ON splice_sessions(peer_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_splice_sessions_status "
            "ON splice_sessions(status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_splice_sessions_timeout "
            "ON splice_sessions(timeout_at) WHERE status NOT IN ('completed', 'aborted', 'failed')"
        )

        # =====================================================================
        # DISTRIBUTED SETTLEMENT TABLES (Phase 12)
        # =====================================================================

        # Settlement proposals - one proposal per period
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settlement_proposals (
                proposal_id TEXT PRIMARY KEY,
                period TEXT NOT NULL UNIQUE,
                proposer_peer_id TEXT NOT NULL,
                proposed_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                data_hash TEXT NOT NULL,
                plan_hash TEXT,
                total_fees_sats INTEGER NOT NULL,
                member_count INTEGER NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_settlement_proposals_period "
            "ON settlement_proposals(period)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_settlement_proposals_status "
            "ON settlement_proposals(status)"
        )
        # Add last_broadcast_at column if upgrading from older schema (Issue #49)
        try:
            conn.execute(
                "ALTER TABLE settlement_proposals ADD COLUMN last_broadcast_at INTEGER"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists
        # Add contributions_json column for rebroadcast support (Issue #49)
        try:
            conn.execute(
                "ALTER TABLE settlement_proposals ADD COLUMN contributions_json TEXT"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists
        # Add plan_hash column for deterministic payment plan binding (Phase 12 v2).
        try:
            conn.execute(
                "ALTER TABLE settlement_proposals ADD COLUMN plan_hash TEXT"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Settlement ready votes - quorum tracking (51%)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settlement_ready_votes (
                proposal_id TEXT NOT NULL,
                voter_peer_id TEXT NOT NULL,
                data_hash TEXT NOT NULL,
                voted_at INTEGER NOT NULL,
                signature TEXT NOT NULL,
                PRIMARY KEY (proposal_id, voter_peer_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_settlement_votes_proposal "
            "ON settlement_ready_votes(proposal_id)"
        )

        # Settlement execution confirmations
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settlement_executions (
                proposal_id TEXT NOT NULL,
                executor_peer_id TEXT NOT NULL,
                payment_hash TEXT,
                amount_paid_sats INTEGER,
                executed_at INTEGER NOT NULL,
                signature TEXT NOT NULL,
                PRIMARY KEY (proposal_id, executor_peer_id)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_settlement_exec_proposal "
            "ON settlement_executions(proposal_id)"
        )
        # Add plan_hash column for deterministic plan completion validation (Phase 12 v2).
        try:
            conn.execute(
                "ALTER TABLE settlement_executions ADD COLUMN plan_hash TEXT"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Settled periods - prevent double settlement (critical!)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settled_periods (
                period TEXT PRIMARY KEY,
                proposal_id TEXT NOT NULL,
                settled_at INTEGER NOT NULL,
                total_distributed_sats INTEGER NOT NULL
            )
        """)

        # Settlement sub-payments - crash recovery for partial execution (S-2 fix)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settlement_sub_payments (
                proposal_id TEXT NOT NULL,
                from_peer_id TEXT NOT NULL,
                to_peer_id TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                payment_hash TEXT,
                status TEXT NOT NULL DEFAULT 'completed',
                created_at INTEGER NOT NULL,
                PRIMARY KEY (proposal_id, from_peer_id, to_peer_id)
            )
        """)

        # Fee reports from hive members - persisted for settlement calculations
        # This stores FEE_REPORT gossip data so it survives restarts
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fee_reports (
                peer_id TEXT NOT NULL,
                period TEXT NOT NULL,
                fees_earned_sats INTEGER NOT NULL,
                forward_count INTEGER NOT NULL,
                period_start INTEGER NOT NULL,
                period_end INTEGER NOT NULL,
                received_at INTEGER NOT NULL,
                rebalance_costs_sats INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (peer_id, period)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_fee_reports_period
            ON fee_reports(period)
        """)
        # Add rebalance_costs_sats column if upgrading from older schema
        try:
            conn.execute(
                "ALTER TABLE fee_reports ADD COLUMN rebalance_costs_sats INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add rejection_reason column if upgrading from older schema
        try:
            conn.execute(
                "ALTER TABLE pending_actions ADD COLUMN rejection_reason TEXT"
            )
        except sqlite3.OperationalError:
            pass  # Column already exists

        # =====================================================================
        # PEER CAPABILITIES TABLE (Phase B - Version Tolerance)
        # =====================================================================
        # Stores peer feature sets and max supported protocol version
        conn.execute("""
            CREATE TABLE IF NOT EXISTS peer_capabilities (
                peer_id TEXT PRIMARY KEY,
                features TEXT NOT NULL DEFAULT '[]',
                max_protocol_version INTEGER NOT NULL DEFAULT 1,
                plugin_version TEXT DEFAULT '',
                updated_at INTEGER NOT NULL
            )
        """)

        # =====================================================================
        # PROTO EVENTS TABLE (Phase C - Deterministic Idempotency)
        # =====================================================================
        # Persistent dedup for state-changing protocol messages
        conn.execute("""
            CREATE TABLE IF NOT EXISTS proto_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                actor_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                received_at INTEGER NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_proto_events_created
            ON proto_events(created_at)
        """)

        # =====================================================================
        # PROTO OUTBOX TABLE (Phase D - Reliable Delivery)
        # =====================================================================
        # Per-peer message delivery tracking with retry and backoff
        conn.execute("""
            CREATE TABLE IF NOT EXISTS proto_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                msg_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                msg_type INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                created_at INTEGER NOT NULL,
                sent_at INTEGER,
                next_retry_at INTEGER NOT NULL,
                retry_count INTEGER NOT NULL DEFAULT 0,
                expires_at INTEGER NOT NULL,
                last_error TEXT,
                acked_at INTEGER,
                UNIQUE(msg_id, peer_id)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_proto_outbox_retry
            ON proto_outbox(status, next_retry_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_proto_outbox_peer
            ON proto_outbox(peer_id, status)
        """)

        # Pheromone level persistence (routing intelligence)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pheromone_levels (
                channel_id TEXT PRIMARY KEY,
                level REAL NOT NULL,
                fee_ppm INTEGER NOT NULL,
                last_update REAL NOT NULL
            )
        """)

        # Stigmergic marker persistence (routing intelligence)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stigmergic_markers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                depositor TEXT NOT NULL,
                source_peer_id TEXT NOT NULL,
                destination_peer_id TEXT NOT NULL,
                fee_ppm INTEGER NOT NULL,
                success INTEGER NOT NULL,
                volume_sats INTEGER NOT NULL,
                timestamp REAL NOT NULL,
                strength REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_markers_route
            ON stigmergic_markers(source_peer_id, destination_peer_id)
        """)

        # Defense warning report persistence
        conn.execute("""
            CREATE TABLE IF NOT EXISTS defense_warning_reports (
                peer_id TEXT NOT NULL,
                reporter_id TEXT NOT NULL,
                threat_type TEXT NOT NULL,
                severity REAL NOT NULL,
                timestamp REAL NOT NULL,
                ttl REAL NOT NULL,
                evidence_json TEXT,
                PRIMARY KEY (peer_id, reporter_id)
            )
        """)

        # Defense active fee persistence
        conn.execute("""
            CREATE TABLE IF NOT EXISTS defense_active_fees (
                peer_id TEXT PRIMARY KEY,
                multiplier REAL NOT NULL,
                expires_at REAL NOT NULL,
                threat_type TEXT NOT NULL,
                reporter TEXT NOT NULL,
                report_count INTEGER NOT NULL
            )
        """)

        # Remote pheromone persistence (fleet-shared fee intelligence)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS remote_pheromones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                peer_id TEXT NOT NULL,
                reporter_id TEXT NOT NULL,
                level REAL NOT NULL,
                fee_ppm INTEGER NOT NULL,
                timestamp REAL NOT NULL,
                weight REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_remote_pheromones_peer
            ON remote_pheromones(peer_id)
        """)

        # Fee observation persistence (network fee volatility samples)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fee_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                fee_ppm INTEGER NOT NULL
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
            tier: 'member' or 'neophyte'
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
            "SELECT * FROM hive_members ORDER BY tier, joined_at LIMIT 1000"
        ).fetchall()
        return [dict(row) for row in rows]

    def get_membership_hash(self) -> str:
        """
        Calculate deterministic hash of membership state.

        Includes peer_id and tier for each member, sorted by peer_id.
        Used to detect membership divergence between nodes and trigger
        FULL_SYNC when tiers differ.

        Returns:
            Hex-encoded SHA256 hash of membership state
        """
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT peer_id, tier FROM hive_members ORDER BY peer_id"
        ).fetchall()

        # Build list of (peer_id, tier) tuples
        member_tuples = [(row['peer_id'], row['tier']) for row in rows]

        # Serialize to canonical JSON
        json_str = json.dumps(member_tuples, sort_keys=True, separators=(',', ':'))

        # Calculate SHA256
        hash_hex = hashlib.sha256(json_str.encode('utf-8')).hexdigest()

        return hash_hex

    def update_member(self, peer_id: str, **kwargs) -> bool:
        """
        Update member fields.

        Allowed fields: tier, contribution_ratio, uptime_pct, vouch_count,
                       last_seen, promoted_at, metadata, addresses
        """
        allowed = {'tier', 'contribution_ratio', 'uptime_pct', 'vouch_count',
                   'last_seen', 'promoted_at', 'metadata', 'addresses'}
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
                      expires_seconds: int = 300,
                      timestamp: Optional[int] = None) -> int:
        """
        Create a new Intent lock.

        Args:
            intent_type: 'channel_open', 'rebalance', 'ban_peer'
            target: Target peer_id or identifier
            initiator: Our node pubkey
            expires_seconds: Lock TTL
            timestamp: Creation timestamp (uses current time if None)

        Returns:
            Intent ID
        """
        conn = self._get_connection()
        now = timestamp if timestamp is not None else int(time.time())
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
    
    def update_intent_status(self, intent_id: int, status: str, reason: str = None) -> bool:
        """Update Intent status with optional reason for audit trail."""
        conn = self._get_connection()
        if reason:
            result = conn.execute(
                "UPDATE intent_locks SET status = ?, reason = ? WHERE id = ?",
                (status, reason, intent_id)
            )
        else:
            result = conn.execute(
                "UPDATE intent_locks SET status = ? WHERE id = ?",
                (status, intent_id)
            )
        return result.rowcount > 0
    
    def cleanup_expired_intents(self) -> int:
        """Soft-delete expired intents, then purge terminal intents after 24h.

        Phase 1: Mark pending expired intents as 'expired' (preserves audit trail).
        Phase 2: Hard-delete terminal intents (expired/aborted/failed) older than 24h.

        Returns:
            Total number of intents affected (soft-deleted + purged)
        """
        conn = self._get_connection()
        now = int(time.time())

        # Phase 1: Soft-delete - mark pending expired intents
        r1 = conn.execute(
            "UPDATE intent_locks SET status = 'expired', reason = 'ttl_expired' "
            "WHERE status = 'pending' AND expires_at < ?",
            (now,)
        )

        # Phase 2: Purge terminal intents older than 24 hours
        purge_cutoff = now - 86400
        r2 = conn.execute(
            "DELETE FROM intent_locks "
            "WHERE status IN ('expired', 'aborted', 'failed') AND expires_at < ?",
            (purge_cutoff,)
        )

        return r1.rowcount + r2.rowcount
    
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

    def recover_stuck_intents(self, max_age_seconds: int = 300) -> int:
        """
        Mark intents stuck in 'committed' state as 'failed'.

        Intents that remain in 'committed' for longer than max_age_seconds
        are assumed to have failed execution and are freed for retry.

        Args:
            max_age_seconds: Max age in seconds before marking as failed

        Returns:
            Number of intents recovered
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - max_age_seconds
        result = conn.execute(
            "UPDATE intent_locks SET status = 'failed', reason = 'stuck_recovery' "
            "WHERE status = 'committed' AND timestamp < ?",
            (cutoff,)
        )
        return result.rowcount

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
                          topology: List[str], state_hash: str,
                          version: Optional[int] = None) -> None:
        """Update local cache of a peer's Hive state.

        Uses version-guarded writes: only writes if the new version is
        higher than what's already in the DB, preventing late-arriving
        writes from overwriting newer state after concurrent updates.
        """
        conn = self._get_connection()
        now = int(time.time())

        fee_json = json.dumps(fee_policy)
        topo_json = json.dumps(topology)

        if version is not None:
            # Insert if new, or update only if our version is higher
            conn.execute("""
                INSERT INTO hive_state
                (peer_id, capacity_sats, available_sats, fee_policy, topology,
                 last_gossip, state_hash, version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(peer_id) DO UPDATE SET
                    capacity_sats = excluded.capacity_sats,
                    available_sats = excluded.available_sats,
                    fee_policy = excluded.fee_policy,
                    topology = excluded.topology,
                    last_gossip = excluded.last_gossip,
                    state_hash = excluded.state_hash,
                    version = excluded.version
                WHERE excluded.version > hive_state.version
            """, (
                peer_id, capacity_sats, available_sats,
                fee_json, topo_json,
                now, state_hash, version
            ))
        else:
            # Auto-increment for backward compatibility
            conn.execute("""
                INSERT INTO hive_state
                (peer_id, capacity_sats, available_sats, fee_policy, topology,
                 last_gossip, state_hash, version)
                VALUES (?, ?, ?, ?, ?, ?, ?,
                        COALESCE((SELECT version FROM hive_state WHERE peer_id = ?), 0) + 1)
                ON CONFLICT(peer_id) DO UPDATE SET
                    capacity_sats = excluded.capacity_sats,
                    available_sats = excluded.available_sats,
                    fee_policy = excluded.fee_policy,
                    topology = excluded.topology,
                    last_gossip = excluded.last_gossip,
                    state_hash = excluded.state_hash,
                    version = COALESCE((SELECT version FROM hive_state WHERE peer_id = ?), 0) + 1
            """, (
                peer_id, capacity_sats, available_sats,
                fee_json, topo_json,
                now, state_hash, peer_id, peer_id
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
        rows = conn.execute("SELECT * FROM hive_state LIMIT 1000").fetchall()

        results = []
        for row in rows:
            result = dict(row)
            result['fee_policy'] = json.loads(result['fee_policy'] or '{}')
            result['topology'] = json.loads(result['topology'] or '[]')
            results.append(result)
        return results

    def delete_hive_state(self, peer_id: str) -> None:
        """Delete a peer's cached Hive state from the database."""
        conn = self._get_connection()
        conn.execute("DELETE FROM hive_state WHERE peer_id = ?", (peer_id,))

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
    # MANUAL PROMOTIONS (majority member approval required)
    # NOTE: Method names kept as "admin_promotion" for backward compatibility
    # =========================================================================

    def create_admin_promotion(self, target_peer_id: str, proposed_by: str) -> bool:
        """Create or update a manual promotion proposal."""
        conn = self._get_connection()
        now = int(time.time())
        try:
            # Clear stale approvals from any previous proposal for this target
            conn.execute("""
                DELETE FROM admin_promotion_approvals WHERE target_peer_id = ?
            """, (target_peer_id,))
            conn.execute("""
                INSERT OR REPLACE INTO admin_promotions
                (target_peer_id, proposed_by, proposed_at, status)
                VALUES (?, ?, ?, 'pending')
            """, (target_peer_id, proposed_by, now))
            return True
        except Exception:
            return False

    def get_admin_promotion(self, target_peer_id: str) -> Optional[Dict[str, Any]]:
        """Get manual promotion proposal for a peer."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM admin_promotions WHERE target_peer_id = ?
        """, (target_peer_id,)).fetchone()
        return dict(row) if row else None

    def add_admin_promotion_approval(self, target_peer_id: str,
                                      approver_peer_id: str) -> bool:
        """Add a member's approval for a promotion."""
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
        """Get all approvals for a manual promotion."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM admin_promotion_approvals WHERE target_peer_id = ?
        """, (target_peer_id,)).fetchall()
        return [dict(row) for row in rows]

    def complete_admin_promotion(self, target_peer_id: str) -> bool:
        """Mark manual promotion as complete."""
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
        """Get all pending manual promotions."""
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
                           proposed_at: int, expires_at: int,
                           proposal_type: str = 'standard') -> bool:
        """
        Create a new ban proposal.

        Args:
            proposal_id: Unique proposal identifier
            target_peer_id: Member to ban
            proposer_peer_id: Member proposing the ban
            reason: Reason for ban
            proposed_at: Unix timestamp
            expires_at: Unix timestamp when proposal expires
            proposal_type: 'standard' or 'settlement_gaming'
                - standard: Normal quorum voting (51% approve to ban)
                - settlement_gaming: Reversed voting (non-votes = approve,
                  must actively vote 'reject' to prevent ban)
        """
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT INTO ban_proposals
                (proposal_id, target_peer_id, proposer_peer_id, reason,
                 proposed_at, expires_at, status, proposal_type)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
            """, (proposal_id, target_peer_id, proposer_peer_id, reason,
                  proposed_at, expires_at, proposal_type))
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
            cursor = conn.execute("""
                UPDATE ban_proposals SET status = ? WHERE proposal_id = ?
            """, (status, proposal_id))
            return cursor.rowcount > 0
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
        cursor = conn.execute("""
            UPDATE ban_proposals
            SET status = 'expired'
            WHERE status = 'pending' AND expires_at < ?
        """, (now,))
        return cursor.rowcount

    def prune_old_ban_data(self, older_than_days: int = 180) -> int:
        """
        Remove old ban proposals and their votes for terminal states.

        Only prunes proposals in terminal states (approved, rejected, expired).
        Pending proposals are never pruned.

        Args:
            older_than_days: Remove records older than this many days

        Returns:
            Number of ban proposals deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (older_than_days * 86400)

        with self.transaction() as tx_conn:
            # Delete votes for old terminal proposals first (foreign key safety)
            tx_conn.execute("""
                DELETE FROM ban_votes WHERE proposal_id IN (
                    SELECT proposal_id FROM ban_proposals
                    WHERE status IN ('approved', 'rejected', 'expired')
                    AND proposed_at < ?
                )
            """, (cutoff,))

            # Delete the old terminal proposals
            cursor = tx_conn.execute("""
                DELETE FROM ban_proposals
                WHERE status IN ('approved', 'rejected', 'expired')
                AND proposed_at < ?
            """, (cutoff,))
            return cursor.rowcount

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

        Wrapped in a transaction to prevent TOCTOU race between the
        existence check and the subsequent INSERT/UPDATE.
        """
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM peer_presence WHERE peer_id = ?",
                (peer_id,)
            ).fetchone()

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

    def sync_uptime_from_presence(self, window_seconds: int = 30 * 86400) -> int:
        """
        Calculate uptime percentage from peer_presence and update hive_members.

        Uses a single JOIN query instead of N+1 individual lookups.

        For each member with presence data, calculates:
        uptime_pct = online_seconds_rolling / elapsed_window_time

        Args:
            window_seconds: Rolling window size (default 30 days)

        Returns:
            Number of members updated
        """
        conn = self._get_connection()
        now = int(time.time())

        # Single JOIN query: members with their presence data
        rows = conn.execute("""
            SELECT m.peer_id, p.online_seconds_rolling, p.window_start_ts,
                   p.is_online, p.last_change_ts
            FROM hive_members m
            JOIN peer_presence p ON m.peer_id = p.peer_id
        """).fetchall()

        updated = 0
        with self.transaction() as tx_conn:
            for row in rows:
                online_seconds = row['online_seconds_rolling']

                # If currently online, add time since last state change
                if row['is_online']:
                    online_seconds += max(0, now - row['last_change_ts'])

                # Calculate window elapsed time
                elapsed = max(1, now - row['window_start_ts'])

                # Cap at window size
                if elapsed > window_seconds:
                    elapsed = window_seconds
                if online_seconds > elapsed:
                    online_seconds = elapsed

                uptime_pct = online_seconds / elapsed

                tx_conn.execute(
                    "UPDATE hive_members SET uptime_pct = ? WHERE peer_id = ?",
                    (uptime_pct, row['peer_id'])
                )
                updated += 1

        return updated

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
            LIMIT 1000
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

    def update_action_status(self, action_id: int, status: str, reason: str = None) -> bool:
        """Update action status: 'pending', 'approved', 'rejected', 'expired'."""
        conn = self._get_connection()
        if reason:
            result = conn.execute(
                "UPDATE pending_actions SET status = ?, rejection_reason = ? WHERE id = ?",
                (status, reason, action_id)
            )
        else:
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

    # Maximum pending actions to scan (CLAUDE.md: "Bound everything")
    MAX_PENDING_ACTIONS_SCAN = 100

    def has_pending_action_for_target(self, target: str) -> bool:
        """
        Check if there's a pending action for the given target.

        Args:
            target: Target pubkey to check

        Returns:
            True if a pending action exists for this target

        Note:
            Scans at most MAX_PENDING_ACTIONS_SCAN rows to bound query time.
        """
        conn = self._get_connection()
        now = int(time.time())

        # Escape LIKE metacharacters in target to prevent over-matching
        escaped = target.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

        # Use LIKE for initial filtering, then parse JSON to confirm
        # This is more efficient than scanning all rows
        rows = conn.execute("""
            SELECT payload FROM pending_actions
            WHERE status = 'pending' AND expires_at > ?
            AND payload LIKE ? ESCAPE '\\'
            LIMIT ?
        """, (now, f'%{escaped}%', self.MAX_PENDING_ACTIONS_SCAN)).fetchall()

        for row in rows:
            try:
                payload = json.loads(row['payload'])
                if payload.get('target') == target:
                    return True
            except (json.JSONDecodeError, TypeError):
                continue

        return False

    def was_recently_rejected(self, target: str, cooldown_seconds: int = 86400) -> bool:
        """
        Check if a target was recently rejected (within cooldown period).

        This prevents the planner from repeatedly proposing the same peer
        that keeps getting rejected.

        Args:
            target: Target pubkey to check
            cooldown_seconds: How long to wait before re-proposing (default: 24 hours)

        Returns:
            True if the target was rejected within the cooldown period

        Note:
            Scans at most MAX_PENDING_ACTIONS_SCAN rows to bound query time.
        """
        conn = self._get_connection()
        now = int(time.time())
        cutoff = now - cooldown_seconds

        # Escape LIKE metacharacters in target to prevent over-matching
        escaped = target.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

        # Use LIKE for initial filtering, then parse JSON to confirm
        rows = conn.execute("""
            SELECT payload FROM pending_actions
            WHERE status = 'rejected' AND proposed_at > ?
            AND payload LIKE ? ESCAPE '\\'
            LIMIT ?
        """, (cutoff, f'%{escaped}%', self.MAX_PENDING_ACTIONS_SCAN)).fetchall()

        for row in rows:
            try:
                payload = json.loads(row['payload'])
                if payload.get('target') == target:
                    return True
            except (json.JSONDecodeError, TypeError):
                continue

        return False

    def get_rejection_count(self, target: str, days: int = 30) -> int:
        """
        Get the number of times a target was rejected in the given period.

        Args:
            target: Target pubkey to check
            days: Look-back period in days

        Returns:
            Number of rejections for this target (capped at MAX_PENDING_ACTIONS_SCAN)

        Note:
            Scans at most MAX_PENDING_ACTIONS_SCAN rows to bound query time.
        """
        conn = self._get_connection()
        now = int(time.time())
        cutoff = now - (days * 86400)

        # Escape LIKE metacharacters in target to prevent over-matching
        escaped = target.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

        # Use LIKE for initial filtering, then parse JSON to confirm
        rows = conn.execute("""
            SELECT payload FROM pending_actions
            WHERE status = 'rejected' AND proposed_at > ?
            AND payload LIKE ? ESCAPE '\\'
            LIMIT ?
        """, (cutoff, f'%{escaped}%', self.MAX_PENDING_ACTIONS_SCAN)).fetchall()

        count = 0
        for row in rows:
            try:
                payload = json.loads(row['payload'])
                if payload.get('target') == target:
                    count += 1
            except (json.JSONDecodeError, TypeError):
                continue

        return count

    def create_pending_action(
        self,
        action_type: str,
        payload: str,
        proposed_at: int,
        expires_at: int
    ) -> int:
        """
        Create a pending action with explicit timestamps.

        Similar to add_pending_action but accepts pre-computed timestamps
        and string payload (already JSON-encoded).

        Args:
            action_type: Type of action (e.g., 'physarum_strengthen')
            payload: JSON-encoded action details
            proposed_at: Unix timestamp when proposed
            expires_at: Unix timestamp when expires

        Returns:
            Action ID
        """
        conn = self._get_connection()

        cursor = conn.execute("""
            INSERT INTO pending_actions (action_type, payload, proposed_at, expires_at, status)
            VALUES (?, ?, ?, ?, 'pending')
        """, (action_type, payload, proposed_at, expires_at))

        return cursor.lastrowid

    def count_pending_actions_since(
        self,
        action_type: str,
        since_timestamp: int
    ) -> int:
        """
        Count pending actions of a type since a timestamp.

        Used for rate limiting auto-trigger actions.

        Args:
            action_type: Type of action to count
            since_timestamp: Count actions created after this time

        Returns:
            Number of actions (any status) of this type since timestamp
        """
        conn = self._get_connection()

        row = conn.execute("""
            SELECT COUNT(*) as cnt FROM pending_actions
            WHERE action_type = ? AND proposed_at >= ?
        """, (action_type, since_timestamp)).fetchone()

        return row['cnt'] if row else 0

    def count_outbox_pending(self) -> int:
        """
        Count outbox entries ready for sending or retry.

        More efficient than get_outbox_pending() when only a count is needed.

        Returns:
            Count of pending entries.
        """
        conn = self._get_connection()
        now = int(time.time())
        row = conn.execute(
            """SELECT COUNT(*) as cnt FROM proto_outbox
               WHERE status IN ('queued', 'sent')
                 AND next_retry_at <= ?
                 AND expires_at > ?""",
            (now, now)
        ).fetchone()
        return row['cnt'] if row else 0

    def has_recent_action_for_channel(
        self,
        channel_id: str,
        action_type: str,
        since_timestamp: int
    ) -> bool:
        """
        Check if a channel had a recent action of the specified type.

        Used to prevent duplicate actions on the same channel.

        Args:
            channel_id: Channel SCID to check
            action_type: Type of action to check
            since_timestamp: Check for actions after this time

        Returns:
            True if channel has a recent action of this type
        """
        conn = self._get_connection()

        # Escape LIKE metacharacters in channel_id to prevent over-matching
        escaped = channel_id.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')

        # Use LIKE for initial filtering, then parse to confirm
        rows = conn.execute("""
            SELECT payload FROM pending_actions
            WHERE action_type = ? AND proposed_at >= ?
            AND payload LIKE ? ESCAPE '\\'
            LIMIT 10
        """, (action_type, since_timestamp, f'%{escaped}%')).fetchall()

        for row in rows:
            try:
                payload = json.loads(row['payload'])
                if payload.get('channel_id') == channel_id:
                    return True
            except (json.JSONDecodeError, TypeError):
                continue

        return False

    def get_recent_expansion_rejections(self, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get all expansion-related rejections in the given time period.

        This is used to detect global constraints (like insufficient liquidity)
        that affect ALL expansion proposals, not just specific targets.

        Args:
            hours: Look-back period in hours (default: 24)

        Returns:
            List of rejected expansion actions with their payloads

        Note:
            Scans at most MAX_PENDING_ACTIONS_SCAN rows to bound query time.
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (hours * 3600)

        rows = conn.execute("""
            SELECT id, action_type, payload, proposed_at, status, rejection_reason
            FROM pending_actions
            WHERE status = 'rejected'
            AND action_type IN ('channel_open', 'expansion')
            AND proposed_at > ?
            ORDER BY proposed_at DESC
            LIMIT ?
        """, (cutoff, self.MAX_PENDING_ACTIONS_SCAN)).fetchall()

        results = []
        for row in rows:
            try:
                result = dict(row)
                result['payload'] = json.loads(result['payload'])
                results.append(result)
            except (json.JSONDecodeError, TypeError):
                continue

        return results

    # Maximum lookback for consecutive rejection counting (7 days).
    # Prevents ancient rejections from permanently deadlocking the planner.
    REJECTION_LOOKBACK_HOURS = 168

    def count_consecutive_expansion_rejections(self) -> int:
        """
        Count consecutive expansion rejections without any approvals.

        This detects patterns where ALL expansion proposals are being rejected
        (e.g., due to global liquidity constraints), regardless of target.

        Only counts rejections within REJECTION_LOOKBACK_HOURS (7 days) to
        prevent ancient rejections from permanently deadlocking the planner.

        Returns:
            Number of consecutive rejections since last approval/execution
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (self.REJECTION_LOOKBACK_HOURS * 3600)

        # Get the most recent actions within the lookback window, ordered by time
        # Look for the first non-rejection to break the streak
        rows = conn.execute("""
            SELECT status FROM pending_actions
            WHERE action_type IN ('channel_open', 'expansion')
            AND proposed_at > ?
            ORDER BY proposed_at DESC
            LIMIT ?
        """, (cutoff, self.MAX_PENDING_ACTIONS_SCAN)).fetchall()

        consecutive = 0
        for row in rows:
            if row['status'] == 'rejected':
                consecutive += 1
            elif row['status'] in ('approved', 'executed'):
                # Hit an approval, stop counting
                break
            # Skip 'pending' and 'expired' - they don't break the streak

        return consecutive

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

        Wrapped in a transaction so the COUNT + DELETE + INSERT are atomic.

        Args:
            action_type: What the planner did (e.g., 'saturation_check', 'expansion')
            result: Outcome ('success', 'skipped', 'failed', 'proposed')
            target: Target peer related to the action
            details: Additional context as dict
        """
        now = int(time.time())
        details_json = json.dumps(details) if details else None

        with self.transaction() as conn:
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

    # =========================================================================
    # PLANNER IGNORED PEERS
    # =========================================================================

    def add_ignored_peer(self, peer_id: str, reason: str = "manual",
                         duration_hours: Optional[int] = None) -> bool:
        """
        Add a peer to the planner ignore list.

        Ignored peers will not be selected as expansion targets until
        the ignore is released or expires.

        Args:
            peer_id: Pubkey of peer to ignore
            reason: Reason for ignoring (e.g., "manual", "connection_failed")
            duration_hours: Optional expiration in hours (None = permanent until released)

        Returns:
            True if added, False if already ignored
        """
        conn = self._get_connection()
        now = int(time.time())
        expires_at = now + (duration_hours * 3600) if duration_hours else None

        try:
            conn.execute("""
                INSERT OR REPLACE INTO planner_ignored_peers
                (peer_id, ignored_at, reason, expires_at)
                VALUES (?, ?, ?, ?)
            """, (peer_id, now, reason, expires_at))
            return True
        except Exception as e:
            self.plugin.log(f"HiveDatabase: Failed to add ignored peer: {e}", level='warning')
            return False

    def remove_ignored_peer(self, peer_id: str) -> bool:
        """
        Remove a peer from the planner ignore list.

        Args:
            peer_id: Pubkey of peer to unignore

        Returns:
            True if removed, False if not found
        """
        conn = self._get_connection()
        result = conn.execute(
            "DELETE FROM planner_ignored_peers WHERE peer_id = ?",
            (peer_id,)
        )
        return result.rowcount > 0

    def get_ignored_peers(self, include_expired: bool = False) -> List[Dict]:
        """
        Get list of currently ignored peers.

        Args:
            include_expired: If True, include expired ignores (default: False)

        Returns:
            List of ignored peer records
        """
        conn = self._get_connection()
        now = int(time.time())

        if include_expired:
            rows = conn.execute("""
                SELECT * FROM planner_ignored_peers
                ORDER BY ignored_at DESC
            """).fetchall()
        else:
            # Only return non-expired ignores
            rows = conn.execute("""
                SELECT * FROM planner_ignored_peers
                WHERE expires_at IS NULL OR expires_at > ?
                ORDER BY ignored_at DESC
            """, (now,)).fetchall()

        return [dict(row) for row in rows]

    def is_peer_ignored(self, peer_id: str) -> bool:
        """
        Check if a peer is currently ignored.

        Args:
            peer_id: Pubkey to check

        Returns:
            True if peer is ignored (and not expired)
        """
        conn = self._get_connection()
        now = int(time.time())
        row = conn.execute("""
            SELECT 1 FROM planner_ignored_peers
            WHERE peer_id = ? AND (expires_at IS NULL OR expires_at > ?)
        """, (peer_id, now)).fetchone()
        return row is not None

    def cleanup_expired_ignores(self) -> int:
        """
        Remove expired ignore entries.

        Returns:
            Number of expired ignores removed
        """
        conn = self._get_connection()
        now = int(time.time())
        result = conn.execute(
            "DELETE FROM planner_ignored_peers WHERE expires_at IS NOT NULL AND expires_at < ?",
            (now,)
        )
        return result.rowcount

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

        # Calculate per-reporter scores for disagreement detection
        reporter_scores = {}
        for reporter_id in reporters:
            reporter_events = [e for e in events if e['reporter_id'] == reporter_id]
            r_routing = [e.get('routing_score', 0.5) for e in reporter_events if e.get('routing_score')]
            r_profit = [e.get('profitability_score', 0.5) for e in reporter_events if e.get('profitability_score')]
            reporter_scores[reporter_id] = {
                "event_count": len(reporter_events),
                "avg_routing_score": sum(r_routing) / len(r_routing) if r_routing else 0.5,
                "avg_profitability_score": sum(r_profit) / len(r_profit) if r_profit else 0.5,
            }

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
            "reporters": reporters,
            "reporter_scores": reporter_scores
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

    def get_peers_with_events(self, days: int = 90, limit: int = 500) -> List[str]:
        """
        Get list of all external peers that have event history.

        Args:
            days: Only include peers with events in last N days
            limit: Maximum number of peers to return (default 500)

        Returns:
            List of peer_id strings
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days * 86400)

        rows = conn.execute("""
            SELECT DISTINCT peer_id FROM peer_events
            WHERE timestamp > ?
            LIMIT ?
        """, (cutoff, limit)).fetchall()

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

    def prune_budget_tracking(self, older_than_days: int = 90) -> int:
        """
        Remove old budget tracking records.

        Args:
            older_than_days: Delete records older than this (default: 90)

        Returns:
            Number of records deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (older_than_days * 86400)
        result = conn.execute(
            "DELETE FROM budget_tracking WHERE timestamp < ?", (cutoff,)
        )
        deleted = result.rowcount
        if deleted > 0:
            self.plugin.log(
                f"HiveDatabase: Pruned {deleted} budget_tracking rows older than {older_than_days}d",
                level='info'
            )
        return deleted

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

    def record_delegation_attempt(
        self,
        original_action_id: int,
        target: str,
        delegation_count: int,
        failure_type: str
    ) -> bool:
        """
        Record a channel open delegation attempt.

        Args:
            original_action_id: ID of the failed action being delegated
            target: Target peer ID for the channel
            delegation_count: Number of delegation requests sent
            failure_type: Type of failure that triggered delegation

        Returns:
            True if recorded successfully
        """
        conn = self._get_connection()
        now = int(time.time())

        try:
            conn.execute("""
                INSERT INTO delegation_attempts
                (original_action_id, target, delegation_count, failure_type, timestamp, status)
                VALUES (?, ?, ?, ?, ?, 'pending')
            """, (original_action_id, target, delegation_count, failure_type, now))
            return True
        except Exception as e:
            # Table might not exist yet - that's OK for new feature
            self.plugin.log(f"Failed to record delegation attempt: {e}", level='debug')
            return False

    # =========================================================================
    # TASK DELEGATION (Phase 10)
    # =========================================================================

    def create_outgoing_task_request(
        self,
        request_id: str,
        target_member_id: str,
        task_type: str,
        task_target: str,
        amount_sats: Optional[int],
        priority: str,
        deadline_timestamp: int,
        failure_context: Optional[str] = None
    ) -> bool:
        """
        Record an outgoing task request to another hive member.

        Args:
            request_id: Unique request identifier
            target_member_id: Member we're asking to perform the task
            task_type: Type of task (expand_to, etc.)
            task_target: Target of the task (peer to open channel to, etc.)
            amount_sats: Amount in satoshis (if applicable)
            priority: Task priority
            deadline_timestamp: When the task should be completed
            failure_context: JSON string with context about why we're delegating

        Returns:
            True if recorded successfully
        """
        conn = self._get_connection()
        now = int(time.time())

        try:
            conn.execute("""
                INSERT INTO task_requests_outgoing
                (request_id, target_member_id, task_type, task_target, amount_sats,
                 priority, deadline_timestamp, failure_context, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """, (request_id, target_member_id, task_type, task_target, amount_sats,
                  priority, deadline_timestamp, failure_context, now))
            return True
        except Exception as e:
            self.plugin.log(f"Failed to create outgoing task request: {e}", level='debug')
            return False

    def update_outgoing_task_response(
        self,
        request_id: str,
        response_status: str,
        response_reason: Optional[str] = None,
        result_data: Optional[str] = None
    ) -> bool:
        """
        Update an outgoing task request with the response.

        Args:
            request_id: Request to update
            response_status: Status from response (accepted/rejected/completed/failed)
            response_reason: Reason for rejection/failure
            result_data: JSON string with result data

        Returns:
            True if updated successfully
        """
        conn = self._get_connection()
        now = int(time.time())

        # Map response status to our tracking status
        if response_status == 'accepted':
            new_status = 'in_progress'
        elif response_status == 'completed':
            new_status = 'completed'
        elif response_status in ('rejected', 'failed'):
            new_status = 'failed'
        else:
            new_status = response_status

        try:
            conn.execute("""
                UPDATE task_requests_outgoing
                SET status = ?, responded_at = ?, response_status = ?,
                    response_reason = ?, result_data = ?
                WHERE request_id = ?
            """, (new_status, now, response_status, response_reason, result_data, request_id))
            return True
        except Exception as e:
            self.plugin.log(f"Failed to update outgoing task: {e}", level='debug')
            return False

    def get_outgoing_task(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get an outgoing task request by ID."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM task_requests_outgoing WHERE request_id = ?
        """, (request_id,)).fetchone()
        return dict(row) if row else None

    def get_pending_outgoing_tasks(self) -> List[Dict[str, Any]]:
        """Get all pending outgoing task requests."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM task_requests_outgoing
            WHERE status IN ('pending', 'in_progress')
            ORDER BY created_at DESC
        """).fetchall()
        return [dict(row) for row in rows]

    def create_incoming_task_request(
        self,
        request_id: str,
        requester_id: str,
        task_type: str,
        task_target: str,
        amount_sats: Optional[int],
        priority: str,
        deadline_timestamp: int,
        failure_context: Optional[str] = None
    ) -> bool:
        """
        Record an incoming task request from another hive member.

        Args:
            request_id: Unique request identifier
            requester_id: Member requesting the task
            task_type: Type of task
            task_target: Target of the task
            amount_sats: Amount in satoshis (if applicable)
            priority: Task priority
            deadline_timestamp: When the task should be completed
            failure_context: JSON string with context

        Returns:
            True if recorded successfully
        """
        conn = self._get_connection()
        now = int(time.time())

        try:
            conn.execute("""
                INSERT INTO task_requests_incoming
                (request_id, requester_id, task_type, task_target, amount_sats,
                 priority, deadline_timestamp, failure_context, status, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """, (request_id, requester_id, task_type, task_target, amount_sats,
                  priority, deadline_timestamp, failure_context, now))
            return True
        except Exception as e:
            self.plugin.log(f"Failed to create incoming task request: {e}", level='debug')
            return False

    def update_incoming_task_status(
        self,
        request_id: str,
        status: str,
        result_data: Optional[str] = None,
        failure_reason: Optional[str] = None
    ) -> bool:
        """
        Update an incoming task request status.

        Args:
            request_id: Request to update
            status: New status (accepted/completed/failed/rejected)
            result_data: JSON string with result data
            failure_reason: Reason for failure/rejection

        Returns:
            True if updated successfully
        """
        conn = self._get_connection()
        now = int(time.time())

        try:
            if status == 'accepted':
                conn.execute("""
                    UPDATE task_requests_incoming
                    SET status = ?, accepted_at = ?
                    WHERE request_id = ?
                """, (status, now, request_id))
            elif status == 'completed':
                conn.execute("""
                    UPDATE task_requests_incoming
                    SET status = ?, completed_at = ?, result_data = ?
                    WHERE request_id = ?
                """, (status, now, result_data, request_id))
            else:
                conn.execute("""
                    UPDATE task_requests_incoming
                    SET status = ?, failure_reason = ?
                    WHERE request_id = ?
                """, (status, failure_reason, request_id))
            return True
        except Exception as e:
            self.plugin.log(f"Failed to update incoming task: {e}", level='debug')
            return False

    def get_incoming_task(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get an incoming task request by ID."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM task_requests_incoming WHERE request_id = ?
        """, (request_id,)).fetchone()
        return dict(row) if row else None

    def get_pending_incoming_tasks(self) -> List[Dict[str, Any]]:
        """Get all pending/accepted incoming task requests."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM task_requests_incoming
            WHERE status IN ('pending', 'accepted')
            ORDER BY priority DESC, received_at ASC
        """).fetchall()
        return [dict(row) for row in rows]

    def count_active_incoming_tasks(self) -> int:
        """Count tasks we've accepted but not completed."""
        conn = self._get_connection()
        result = conn.execute("""
            SELECT COUNT(*) as count FROM task_requests_incoming
            WHERE status = 'accepted'
        """).fetchone()
        return result['count'] if result else 0

    def cleanup_expired_tasks(self, max_age_hours: int = 24) -> int:
        """
        Clean up old task requests.

        Args:
            max_age_hours: Delete tasks older than this

        Returns:
            Number of tasks deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)

        try:
            # Clean outgoing
            cursor = conn.execute("""
                DELETE FROM task_requests_outgoing
                WHERE created_at < ? AND status IN ('completed', 'failed')
            """, (cutoff,))
            deleted = cursor.rowcount

            # Clean incoming
            cursor = conn.execute("""
                DELETE FROM task_requests_incoming
                WHERE received_at < ? AND status IN ('completed', 'failed', 'rejected')
            """, (cutoff,))
            deleted += cursor.rowcount

            return deleted
        except Exception:
            return 0

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
    # BUDGET HOLDS OPERATIONS (Phase 8 - Hive-wide Affordability)
    # =========================================================================

    def create_budget_hold(self, hold_id: str, round_id: str, peer_id: str,
                           amount_sats: int, expires_seconds: int) -> bool:
        """
        Create a new budget hold.

        Args:
            hold_id: Unique hold identifier
            round_id: Expansion round ID
            peer_id: Member creating the hold
            amount_sats: Amount to reserve
            expires_seconds: Seconds until hold expires

        Returns:
            True if created, False on error
        """
        conn = self._get_connection()
        now = int(time.time())
        expires_at = now + expires_seconds

        try:
            conn.execute("""
                INSERT OR REPLACE INTO budget_holds
                (hold_id, round_id, peer_id, amount_sats, created_at, expires_at, status)
                VALUES (?, ?, ?, ?, ?, ?, 'active')
            """, (hold_id, round_id, peer_id, amount_sats, now, expires_at))
            return True
        except Exception:
            return False

    def release_budget_hold(self, hold_id: str) -> bool:
        """Release a budget hold (round completed/cancelled)."""
        conn = self._get_connection()
        try:
            result = conn.execute("""
                UPDATE budget_holds SET status = 'released'
                WHERE hold_id = ? AND status = 'active'
            """, (hold_id,))
            return result.rowcount > 0
        except Exception:
            return False

    def consume_budget_hold(self, hold_id: str, consumed_by: str) -> bool:
        """Mark a hold as consumed (channel opened)."""
        conn = self._get_connection()
        now = int(time.time())
        try:
            result = conn.execute("""
                UPDATE budget_holds
                SET status = 'consumed', consumed_by = ?, consumed_at = ?
                WHERE hold_id = ? AND status = 'active'
            """, (consumed_by, now, hold_id))
            return result.rowcount > 0
        except Exception:
            return False

    def expire_budget_hold(self, hold_id: str) -> bool:
        """Mark a hold as expired."""
        conn = self._get_connection()
        try:
            result = conn.execute("""
                UPDATE budget_holds SET status = 'expired'
                WHERE hold_id = ? AND status = 'active'
            """, (hold_id,))
            return result.rowcount > 0
        except Exception:
            return False

    def get_budget_hold(self, hold_id: str) -> Optional[Dict[str, Any]]:
        """Get a budget hold by ID."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM budget_holds WHERE hold_id = ?
        """, (hold_id,)).fetchone()
        return dict(row) if row else None

    def get_active_holds_for_peer(self, peer_id: str) -> List[Dict[str, Any]]:
        """Get all active holds for a peer."""
        conn = self._get_connection()
        now = int(time.time())
        rows = conn.execute("""
            SELECT * FROM budget_holds
            WHERE peer_id = ? AND status = 'active' AND expires_at > ?
            ORDER BY created_at DESC
        """, (peer_id, now)).fetchall()
        return [dict(row) for row in rows]

    def get_holds_for_round(self, round_id: str) -> List[Dict[str, Any]]:
        """Get all holds for a specific round."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM budget_holds WHERE round_id = ?
        """, (round_id,)).fetchall()
        return [dict(row) for row in rows]

    def get_total_held_for_peer(self, peer_id: str) -> int:
        """Get total amount held for a peer across active holds."""
        conn = self._get_connection()
        now = int(time.time())
        row = conn.execute("""
            SELECT COALESCE(SUM(amount_sats), 0) as total
            FROM budget_holds
            WHERE peer_id = ? AND status = 'active' AND expires_at > ?
        """, (peer_id, now)).fetchone()
        return row['total'] if row else 0

    def cleanup_expired_holds(self) -> int:
        """Mark all expired holds as expired. Returns count."""
        conn = self._get_connection()
        now = int(time.time())
        cursor = conn.execute("""
            UPDATE budget_holds SET status = 'expired'
            WHERE status = 'active' AND expires_at <= ?
        """, (now,))
        return cursor.rowcount

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
            LIMIT 10000
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

    def get_all_peer_fee_profiles(self, limit: int = 500) -> List[Dict[str, Any]]:
        """
        Get all aggregated fee profiles.

        Args:
            limit: Maximum number of profiles to return (default 500)

        Returns:
            List of fee profile dicts
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM peer_fee_profiles ORDER BY reporter_count DESC LIMIT ?
        """, (limit,)).fetchall()
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
        tier: str = 'stable',
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
            tier: 'struggling', 'vulnerable', 'stable', or 'thriving'
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
            SELECT * FROM member_health ORDER BY overall_health ASC LIMIT 1000
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

    # =========================================================================
    # MEMBER LIQUIDITY STATE OPERATIONS (Phase 2 - Liquidity Intelligence)
    # =========================================================================
    # Stores liquidity state reports from cl-revenue-ops instances.
    # INFORMATION ONLY - enables coordinated decisions, no fund transfers.

    def update_member_liquidity_state(
        self,
        member_id: str,
        depleted_count: int,
        saturated_count: int,
        rebalancing_active: bool = False,
        rebalancing_peers: List[str] = None,
        timestamp: Optional[int] = None
    ) -> None:
        """
        Update liquidity state for a hive member.

        INFORMATION SHARING - enables coordinated fee/rebalance decisions.
        No sats transfer between nodes.

        Args:
            member_id: Hive member peer ID
            depleted_count: Number of depleted channels
            saturated_count: Number of saturated channels
            rebalancing_active: Whether member is currently rebalancing
            rebalancing_peers: Which peers they're rebalancing through
            timestamp: When the report was made
        """
        import json
        conn = self._get_connection()
        ts = timestamp or int(time.time())
        peers_json = json.dumps(rebalancing_peers or [])

        conn.execute("""
            INSERT INTO member_liquidity_state (
                peer_id, depleted_count, saturated_count,
                rebalancing_active, rebalancing_peers, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(peer_id) DO UPDATE SET
                depleted_count = excluded.depleted_count,
                saturated_count = excluded.saturated_count,
                rebalancing_active = excluded.rebalancing_active,
                rebalancing_peers = excluded.rebalancing_peers,
                timestamp = excluded.timestamp
        """, (
            member_id, depleted_count, saturated_count,
            1 if rebalancing_active else 0, peers_json, ts
        ))

    def get_member_liquidity_state(
        self,
        member_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get liquidity state for a hive member.

        Args:
            member_id: Hive member peer ID

        Returns:
            Liquidity state dict or None if not found
        """
        import json
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM member_liquidity_state WHERE peer_id = ?
        """, (member_id,)).fetchone()

        if not row:
            return None

        result = dict(row)
        result['rebalancing_active'] = bool(result.get('rebalancing_active', 0))
        result['rebalancing_peers'] = json.loads(
            result.get('rebalancing_peers', '[]')
        )
        return result

    def get_all_member_liquidity_states(self) -> List[Dict[str, Any]]:
        """
        Get liquidity state for all members.

        Returns:
            List of liquidity state dicts
        """
        import json
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM member_liquidity_state
            ORDER BY timestamp DESC LIMIT 1000
        """).fetchall()

        results = []
        for row in rows:
            result = dict(row)
            result['rebalancing_active'] = bool(result.get('rebalancing_active', 0))
            result['rebalancing_peers'] = json.loads(
                result.get('rebalancing_peers', '[]')
            )
            results.append(result)
        return results

    # =========================================================================
    # LIQUIDITY NEEDS OPERATIONS (Phase 7.3 - Cooperative Rebalancing)
    # =========================================================================

    def store_liquidity_need(
        self,
        reporter_id: str,
        need_type: str,
        target_peer_id: str,
        amount_sats: int,
        urgency: str = "medium",
        max_fee_ppm: int = 0,
        reason: str = "",
        current_balance_pct: float = 0.5,
        timestamp: Optional[int] = None
    ):
        """
        Store or update a liquidity need.

        Args:
            reporter_id: Hive member reporting the need
            need_type: Type of need (inbound/outbound/rebalance)
            target_peer_id: External peer involved
            amount_sats: Amount needed
            urgency: Urgency level
            max_fee_ppm: Maximum fee willing to pay
            reason: Reason for the need
            current_balance_pct: Current local balance percentage
            timestamp: When the need was reported
        """
        conn = self._get_connection()
        now = timestamp or int(time.time())
        conn.execute("""
            INSERT OR REPLACE INTO liquidity_needs
            (reporter_id, need_type, target_peer_id, amount_sats, urgency,
             max_fee_ppm, reason, current_balance_pct, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            reporter_id, need_type, target_peer_id, amount_sats, urgency,
            max_fee_ppm, reason, current_balance_pct, now
        ))

    def get_liquidity_need(
        self,
        reporter_id: str,
        target_peer_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get a specific liquidity need.

        Args:
            reporter_id: Hive member who reported
            target_peer_id: Target peer

        Returns:
            Liquidity need dict or None
        """
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM liquidity_needs
            WHERE reporter_id = ? AND target_peer_id = ?
        """, (reporter_id, target_peer_id)).fetchone()
        return dict(row) if row else None

    def get_all_liquidity_needs(
        self,
        max_age_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get all recent liquidity needs.

        Args:
            max_age_hours: Maximum age to include

        Returns:
            List of liquidity need dicts
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)
        rows = conn.execute("""
            SELECT * FROM liquidity_needs
            WHERE timestamp >= ?
            ORDER BY
                CASE urgency
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'medium' THEN 3
                    ELSE 4
                END,
                timestamp DESC
            LIMIT 10000
        """, (cutoff,)).fetchall()
        return [dict(row) for row in rows]

    def get_liquidity_needs_for_reporter(
        self,
        reporter_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get all liquidity needs from a specific reporter.

        Args:
            reporter_id: Hive member peer ID

        Returns:
            List of liquidity need dicts
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM liquidity_needs
            WHERE reporter_id = ?
            ORDER BY timestamp DESC
        """, (reporter_id,)).fetchall()
        return [dict(row) for row in rows]

    def get_urgent_liquidity_needs(
        self,
        urgency_levels: List[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get liquidity needs by urgency level.

        Args:
            urgency_levels: List of urgency levels to include
                           (default: critical, high)

        Returns:
            List of liquidity need dicts
        """
        if urgency_levels is None:
            urgency_levels = ["critical", "high"]

        conn = self._get_connection()
        placeholders = ",".join("?" * len(urgency_levels))
        rows = conn.execute(f"""
            SELECT * FROM liquidity_needs
            WHERE urgency IN ({placeholders})
            ORDER BY
                CASE urgency
                    WHEN 'critical' THEN 1
                    WHEN 'high' THEN 2
                    ELSE 3
                END,
                timestamp DESC
        """, urgency_levels).fetchall()
        return [dict(row) for row in rows]

    def cleanup_old_liquidity_needs(self, max_age_hours: int = 24) -> int:
        """
        Remove old liquidity need records.

        Args:
            max_age_hours: Maximum age to keep

        Returns:
            Number of records deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)
        cursor = conn.execute("""
            DELETE FROM liquidity_needs WHERE timestamp < ?
        """, (cutoff,))
        return cursor.rowcount

    # =========================================================================
    # ROUTE PROBES OPERATIONS (Phase 7.4 - Routing Intelligence)
    # =========================================================================

    def store_route_probe(
        self,
        reporter_id: str,
        destination: str,
        path: List[str],
        success: bool,
        latency_ms: int = 0,
        failure_reason: str = "",
        failure_hop: int = -1,
        estimated_capacity_sats: int = 0,
        total_fee_ppm: int = 0,
        amount_probed_sats: int = 0,
        timestamp: Optional[int] = None
    ):
        """
        Store a route probe observation.

        Args:
            reporter_id: Hive member reporting the probe
            destination: Final destination pubkey
            path: List of intermediate hop pubkeys
            success: Whether probe succeeded
            latency_ms: Round-trip latency
            failure_reason: Reason for failure
            failure_hop: Index of failing hop
            estimated_capacity_sats: Estimated capacity
            total_fee_ppm: Total route fees
            amount_probed_sats: Amount probed
            timestamp: When probe was performed
        """
        conn = self._get_connection()
        now = timestamp or int(time.time())

        # Store path as JSON string
        import json
        path_str = json.dumps(path)

        conn.execute("""
            INSERT OR IGNORE INTO route_probes
            (reporter_id, destination, path, timestamp, success, latency_ms,
             failure_reason, failure_hop, estimated_capacity_sats, total_fee_ppm,
             amount_probed_sats)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            reporter_id, destination, path_str, now,
            1 if success else 0, latency_ms,
            failure_reason, failure_hop, estimated_capacity_sats,
            total_fee_ppm, amount_probed_sats
        ))

    def get_route_probes_for_destination(
        self,
        destination: str,
        max_age_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get route probes for a specific destination.

        Args:
            destination: Destination pubkey
            max_age_hours: Maximum age to include

        Returns:
            List of route probe dicts
        """
        import json
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)

        rows = conn.execute("""
            SELECT * FROM route_probes
            WHERE destination = ? AND timestamp >= ?
            ORDER BY timestamp DESC
        """, (destination, cutoff)).fetchall()

        results = []
        for row in rows:
            probe = dict(row)
            # Parse path from JSON
            try:
                probe["path"] = json.loads(probe.get("path", "[]"))
            except (json.JSONDecodeError, TypeError):
                probe["path"] = []
            probe["success"] = bool(probe.get("success", 0))
            results.append(probe)

        return results

    def get_all_route_probes(
        self,
        max_age_hours: int = 24
    ) -> List[Dict[str, Any]]:
        """
        Get all recent route probes.

        Args:
            max_age_hours: Maximum age to include

        Returns:
            List of route probe dicts
        """
        import json
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)

        rows = conn.execute("""
            SELECT * FROM route_probes
            WHERE timestamp >= ?
            ORDER BY timestamp DESC
            LIMIT 10000
        """, (cutoff,)).fetchall()

        results = []
        for row in rows:
            probe = dict(row)
            try:
                probe["path"] = json.loads(probe.get("path", "[]"))
            except (json.JSONDecodeError, TypeError):
                probe["path"] = []
            probe["success"] = bool(probe.get("success", 0))
            results.append(probe)

        return results

    def get_route_probe_stats(
        self,
        destination: str
    ) -> Dict[str, Any]:
        """
        Get aggregated statistics for routes to a destination.

        Args:
            destination: Destination pubkey

        Returns:
            Dict with route statistics
        """
        conn = self._get_connection()

        row = conn.execute("""
            SELECT
                COUNT(*) as probe_count,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as success_count,
                AVG(CASE WHEN success = 1 THEN latency_ms ELSE NULL END) as avg_latency,
                AVG(CASE WHEN success = 1 THEN total_fee_ppm ELSE NULL END) as avg_fee,
                MAX(CASE WHEN success = 1 THEN timestamp ELSE 0 END) as last_success,
                COUNT(DISTINCT reporter_id) as reporter_count
            FROM route_probes
            WHERE destination = ?
        """, (destination,)).fetchone()

        if not row:
            return {
                "probe_count": 0,
                "success_count": 0,
                "success_rate": 0.0,
                "avg_latency_ms": 0,
                "avg_fee_ppm": 0,
                "last_success": 0,
                "reporter_count": 0
            }

        probe_count = row["probe_count"] or 0
        success_count = row["success_count"] or 0

        return {
            "probe_count": probe_count,
            "success_count": success_count,
            "success_rate": success_count / probe_count if probe_count > 0 else 0.0,
            "avg_latency_ms": int(row["avg_latency"] or 0),
            "avg_fee_ppm": int(row["avg_fee"] or 0),
            "last_success": row["last_success"] or 0,
            "reporter_count": row["reporter_count"] or 0
        }

    def cleanup_old_route_probes(self, max_age_hours: int = 168) -> int:
        """
        Remove old route probe records.

        Args:
            max_age_hours: Maximum age to keep (default 7 days)

        Returns:
            Number of records deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)
        cursor = conn.execute("""
            DELETE FROM route_probes WHERE timestamp < ?
        """, (cutoff,))
        return cursor.rowcount

    # =========================================================================
    # PEER REPUTATION OPERATIONS (Phase 5 - Advanced Cooperation)
    # =========================================================================

    def store_peer_reputation(
        self,
        reporter_id: str,
        peer_id: str,
        timestamp: int,
        uptime_pct: float = 1.0,
        response_time_ms: int = 0,
        force_close_count: int = 0,
        fee_stability: float = 1.0,
        htlc_success_rate: float = 1.0,
        channel_age_days: int = 0,
        total_routed_sats: int = 0,
        warnings: list = None,
        observation_days: int = 7
    ):
        """
        Store a peer reputation report.

        Args:
            reporter_id: Hive member reporting
            peer_id: External peer being reported on
            timestamp: Report timestamp
            uptime_pct: Peer uptime (0-1)
            response_time_ms: Average HTLC response time
            force_close_count: Force closes by peer
            fee_stability: Fee stability (0-1)
            htlc_success_rate: HTLC success rate (0-1)
            channel_age_days: Channel age
            total_routed_sats: Total volume routed
            warnings: List of warning codes
            observation_days: Days covered by report
        """
        conn = self._get_connection()
        warnings_json = json.dumps(warnings or [])

        conn.execute("""
            INSERT INTO peer_reputation (
                reporter_id, peer_id, timestamp, uptime_pct, response_time_ms,
                force_close_count, fee_stability, htlc_success_rate,
                channel_age_days, total_routed_sats, warnings, observation_days
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            reporter_id, peer_id, timestamp, uptime_pct, response_time_ms,
            force_close_count, fee_stability, htlc_success_rate,
            channel_age_days, total_routed_sats, warnings_json, observation_days
        ))

    def get_peer_reputation_reports(
        self,
        peer_id: str,
        max_age_hours: int = 168
    ) -> list:
        """
        Get all reputation reports for a specific peer.

        Args:
            peer_id: External peer pubkey
            max_age_hours: Maximum age of reports to include

        Returns:
            List of reputation report dicts
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)

        rows = conn.execute("""
            SELECT * FROM peer_reputation
            WHERE peer_id = ? AND timestamp > ?
            ORDER BY timestamp DESC
        """, (peer_id, cutoff)).fetchall()

        reports = []
        for row in rows:
            report = dict(row)
            # Parse warnings JSON
            report["warnings"] = json.loads(report.get("warnings", "[]"))
            reports.append(report)

        return reports

    def get_all_peer_reputation_reports(
        self,
        max_age_hours: int = 168
    ) -> list:
        """
        Get all reputation reports.

        Args:
            max_age_hours: Maximum age of reports to include

        Returns:
            List of all reputation report dicts
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)

        rows = conn.execute("""
            SELECT * FROM peer_reputation
            WHERE timestamp > ?
            ORDER BY timestamp DESC
            LIMIT 10000
        """, (cutoff,)).fetchall()

        reports = []
        for row in rows:
            report = dict(row)
            report["warnings"] = json.loads(report.get("warnings", "[]"))
            reports.append(report)

        return reports

    def get_peer_reputation_reporters(self, peer_id: str) -> list:
        """
        Get list of reporters who have submitted reports for a peer.

        Args:
            peer_id: External peer pubkey

        Returns:
            List of unique reporter pubkeys
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT DISTINCT reporter_id FROM peer_reputation
            WHERE peer_id = ?
        """, (peer_id,)).fetchall()

        return [row["reporter_id"] for row in rows]

    def cleanup_old_peer_reputation(self, max_age_hours: int = 168) -> int:
        """
        Remove old peer reputation records.

        Args:
            max_age_hours: Maximum age to keep (default 7 days)

        Returns:
            Number of records deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (max_age_hours * 3600)
        cursor = conn.execute("""
            DELETE FROM peer_reputation WHERE timestamp < ?
        """, (cutoff,))
        return cursor.rowcount

    # =========================================================================
    # ROUTING POOL OPERATIONS (Phase 0 - Collective Economics)
    # =========================================================================

    def record_pool_revenue(
        self,
        member_id: str,
        amount_sats: int,
        channel_id: str = None,
        payment_hash: str = None
    ) -> int:
        """
        Record routing revenue for the pool.

        All revenue goes to the collective pool, not individual members.
        This enables profit sharing based on contributions.

        Args:
            member_id: Pubkey of member who routed the payment
            amount_sats: Fee revenue in satoshis
            channel_id: Channel that earned the fee (optional)
            payment_hash: Payment hash for deduplication (optional)

        Returns:
            Row ID of the recorded revenue
        """
        conn = self._get_connection()

        cursor = conn.execute("""
            INSERT OR IGNORE INTO pool_revenue
            (member_id, amount_sats, channel_id, payment_hash, recorded_at)
            VALUES (?, ?, ?, ?, ?)
        """, (member_id, amount_sats, channel_id, payment_hash, int(time.time())))
        return cursor.lastrowid or 0

    def get_pool_revenue(
        self,
        period: str = None,
        start_time: int = None,
        end_time: int = None
    ) -> Dict[str, Any]:
        """
        Get pool revenue statistics.

        Args:
            period: Period string (e.g., "2025-W03") - calculates time bounds
            start_time: Start timestamp (alternative to period)
            end_time: End timestamp (alternative to period)

        Returns:
            Dict with total_sats, transaction_count, by_member breakdown
        """
        conn = self._get_connection()

        # Calculate time bounds
        if period:
            start_time, end_time = self._period_to_timestamps(period)
        elif start_time is None:
            # Default to last 7 days
            end_time = int(time.time())
            start_time = end_time - (7 * 24 * 3600)

        # Total revenue
        total = conn.execute("""
            SELECT COALESCE(SUM(amount_sats), 0) as total,
                   COUNT(*) as count
            FROM pool_revenue
            WHERE recorded_at >= ? AND recorded_at < ?
        """, (start_time, end_time)).fetchone()

        # By member
        by_member = conn.execute("""
            SELECT member_id,
                   SUM(amount_sats) as revenue_sats,
                   COUNT(*) as transaction_count
            FROM pool_revenue
            WHERE recorded_at >= ? AND recorded_at < ?
            GROUP BY member_id
            ORDER BY revenue_sats DESC
        """, (start_time, end_time)).fetchall()

        return {
            "total_sats": total["total"],
            "transaction_count": total["count"],
            "start_time": start_time,
            "end_time": end_time,
            "by_member": [dict(row) for row in by_member]
        }

    def record_pool_contribution(
        self,
        member_id: str,
        period: str,
        total_capacity_sats: int,
        weighted_capacity_sats: int,
        uptime_pct: float,
        betweenness_centrality: float,
        unique_peers: int,
        bridge_score: float,
        routing_success_rate: float,
        avg_response_time_ms: float,
        pool_share: float
    ) -> bool:
        """
        Record a member's contribution snapshot for a period.

        Args:
            member_id: Member pubkey
            period: Period string (e.g., "2025-W03")
            total_capacity_sats: Total channel capacity
            weighted_capacity_sats: Capacity weighted by position quality
            uptime_pct: Uptime percentage (0-1)
            betweenness_centrality: Network centrality score
            unique_peers: Number of peers only this member connects to
            bridge_score: Score for connecting network clusters
            routing_success_rate: HTLC success rate (0-1)
            avg_response_time_ms: Average forwarding response time
            pool_share: Computed share of pool (0-1)

        Returns:
            True if recorded, False if duplicate
        """
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO pool_contributions
                (member_id, period, total_capacity_sats, weighted_capacity_sats,
                 uptime_pct, betweenness_centrality, unique_peers, bridge_score,
                 routing_success_rate, avg_response_time_ms, pool_share, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (member_id, period, total_capacity_sats, weighted_capacity_sats,
                  uptime_pct, betweenness_centrality, unique_peers, bridge_score,
                  routing_success_rate, avg_response_time_ms, pool_share,
                  int(time.time())))
            return True
        except sqlite3.Error as e:
            self.plugin.log(f"Error recording pool contribution: {e}", level='error')
            return False

    def get_pool_contributions(self, period: str) -> List[Dict[str, Any]]:
        """
        Get all member contributions for a period.

        Args:
            period: Period string (e.g., "2025-W03")

        Returns:
            List of contribution dicts sorted by pool_share descending
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM pool_contributions
            WHERE period = ?
            ORDER BY pool_share DESC
        """, (period,)).fetchall()
        return [dict(row) for row in rows]

    def get_member_contribution_history(
        self,
        member_id: str,
        limit: int = 12
    ) -> List[Dict[str, Any]]:
        """
        Get contribution history for a member.

        Args:
            member_id: Member pubkey
            limit: Max periods to return

        Returns:
            List of contribution dicts, most recent first
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM pool_contributions
            WHERE member_id = ?
            ORDER BY period DESC
            LIMIT ?
        """, (member_id, limit)).fetchall()
        return [dict(row) for row in rows]

    def record_pool_distribution(
        self,
        period: str,
        member_id: str,
        contribution_share: float,
        revenue_share_sats: int,
        total_pool_revenue_sats: int
    ) -> bool:
        """
        Record a distribution settlement for a period.

        Args:
            period: Period string
            member_id: Member pubkey
            contribution_share: Member's share of contributions (0-1)
            revenue_share_sats: Amount distributed to member
            total_pool_revenue_sats: Total pool revenue for period

        Returns:
            True if recorded
        """
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO pool_distributions
                (period, member_id, contribution_share, revenue_share_sats,
                 total_pool_revenue_sats, settled_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (period, member_id, contribution_share, revenue_share_sats,
                  total_pool_revenue_sats, int(time.time())))
            return True
        except sqlite3.Error as e:
            self.plugin.log(f"Error recording distribution: {e}", level='error')
            return False

    def get_pool_distributions(self, period: str) -> List[Dict[str, Any]]:
        """
        Get all distributions for a period.

        Args:
            period: Period string

        Returns:
            List of distribution dicts
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM pool_distributions
            WHERE period = ?
            ORDER BY revenue_share_sats DESC
        """, (period,)).fetchall()
        return [dict(row) for row in rows]

    def get_member_distribution_history(
        self,
        member_id: str,
        limit: int = 12
    ) -> List[Dict[str, Any]]:
        """
        Get distribution history for a member.

        Args:
            member_id: Member pubkey
            limit: Max periods to return

        Returns:
            List of distribution dicts, most recent first
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM pool_distributions
            WHERE member_id = ?
            ORDER BY period DESC
            LIMIT ?
        """, (member_id, limit)).fetchall()
        return [dict(row) for row in rows]

    def _period_to_timestamps(self, period: str) -> tuple:
        """
        Convert period string to start/end timestamps.

        Supports formats:
        - "2025-W03" (ISO week)
        - "2025-01" (month)
        - "2025-01-15" (day)

        Returns:
            (start_timestamp, end_timestamp)
        """
        import datetime

        if "-W" in period:
            # ISO week format: 2025-W03
            year, week = period.split("-W")
            # Monday of that week (use ISO week format: %G=ISO year, %V=ISO week, %u=ISO weekday)
            start = datetime.datetime.strptime(f"{year}-W{week}-1", "%G-W%V-%u").replace(
                tzinfo=datetime.timezone.utc
            )
            end = start + datetime.timedelta(days=7)
        elif len(period) == 7:
            # Month format: 2025-01
            start = datetime.datetime.strptime(f"{period}-01", "%Y-%m-%d").replace(
                tzinfo=datetime.timezone.utc
            )
            # First of next month
            if start.month == 12:
                end = start.replace(year=start.year + 1, month=1)
            else:
                end = start.replace(month=start.month + 1)
        else:
            # Day format: 2025-01-15
            start = datetime.datetime.strptime(period, "%Y-%m-%d").replace(
                tzinfo=datetime.timezone.utc
            )
            end = start + datetime.timedelta(days=1)

        return (int(start.timestamp()), int(end.timestamp()))

    def cleanup_old_pool_revenue(self, days_to_keep: int = 90) -> int:
        """
        Remove old pool revenue records to limit database growth.

        Args:
            days_to_keep: Days of revenue records to retain

        Returns:
            Number of rows deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days_to_keep * 86400)
        result = conn.execute(
            "DELETE FROM pool_revenue WHERE recorded_at < ?", (cutoff,)
        )
        return result.rowcount

    def cleanup_old_pool_contributions(self, periods_to_keep: int = 12) -> int:
        """
        Remove old pool contribution records, keeping only the most recent periods.

        Args:
            periods_to_keep: Number of most recent periods to retain

        Returns:
            Number of rows deleted
        """
        conn = self._get_connection()
        result = conn.execute("""
            DELETE FROM pool_contributions
            WHERE period NOT IN (
                SELECT DISTINCT period FROM pool_contributions
                ORDER BY period DESC LIMIT ?
            )
        """, (periods_to_keep,))
        return result.rowcount

    def cleanup_old_pool_distributions(self, days_to_keep: int = 365) -> int:
        """
        Remove old pool distribution records to limit database growth.

        Args:
            days_to_keep: Days of distribution records to retain

        Returns:
            Number of rows deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days_to_keep * 86400)
        result = conn.execute(
            "DELETE FROM pool_distributions WHERE settled_at < ?", (cutoff,)
        )
        return result.rowcount

    # =========================================================================
    # FLOW SAMPLES OPERATIONS (Phase 7.1 - Anticipatory Liquidity)
    # =========================================================================

    def record_flow_sample(
        self,
        channel_id: str,
        hour: int,
        day_of_week: int,
        inbound_sats: int,
        outbound_sats: int,
        net_flow_sats: int,
        timestamp: int
    ) -> bool:
        """
        Record a flow sample for pattern analysis.

        Args:
            channel_id: Channel SCID
            hour: Hour of day (0-23)
            day_of_week: Day of week (0=Monday, 6=Sunday)
            inbound_sats: Satoshis received
            outbound_sats: Satoshis sent
            net_flow_sats: Net flow (inbound - outbound)
            timestamp: Unix timestamp

        Returns:
            True if recorded successfully
        """
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT INTO flow_samples
                (channel_id, hour, day_of_week, inbound_sats, outbound_sats,
                 net_flow_sats, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (channel_id, hour, day_of_week, inbound_sats, outbound_sats,
                  net_flow_sats, timestamp))
            return True
        except Exception as e:
            self.plugin.log(
                f"Failed to record flow sample: {e}",
                level="debug"
            )
            return False

    def get_flow_samples(
        self,
        channel_id: str,
        days: int = 14
    ) -> List[Dict[str, Any]]:
        """
        Get flow samples for a channel.

        Args:
            channel_id: Channel SCID
            days: Number of days of history to retrieve

        Returns:
            List of flow sample dicts
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days * 24 * 3600)

        rows = conn.execute("""
            SELECT * FROM flow_samples
            WHERE channel_id = ? AND timestamp > ?
            ORDER BY timestamp DESC
        """, (channel_id, cutoff)).fetchall()

        return [dict(row) for row in rows]

    def get_all_flow_samples(
        self,
        days: int = 14
    ) -> List[Dict[str, Any]]:
        """
        Get all flow samples within timeframe.

        Args:
            days: Number of days of history to retrieve

        Returns:
            List of flow sample dicts
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days * 24 * 3600)

        rows = conn.execute("""
            SELECT * FROM flow_samples
            WHERE timestamp > ?
            ORDER BY timestamp DESC
            LIMIT 50000
        """, (cutoff,)).fetchall()

        return [dict(row) for row in rows]

    def prune_old_flow_samples(self, days_to_keep: int = 30) -> int:
        """
        Remove old flow samples to limit database growth.

        Args:
            days_to_keep: Days of samples to retain

        Returns:
            Number of rows deleted
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days_to_keep * 24 * 3600)

        result = conn.execute("""
            DELETE FROM flow_samples
            WHERE timestamp < ?
        """, (cutoff,))

        deleted = result.rowcount
        if deleted > 0:
            self.plugin.log(
                f"Pruned {deleted} old flow samples",
                level="debug"
            )
        return deleted

    # =========================================================================
    # TEMPORAL PATTERNS OPERATIONS (Phase 7.1 - Anticipatory Liquidity)
    # =========================================================================

    def save_temporal_pattern(
        self,
        channel_id: str,
        hour_of_day: Optional[int],
        day_of_week: Optional[int],
        direction: str,
        intensity: float,
        confidence: float,
        samples: int,
        avg_flow_sats: int,
        detected_at: int
    ) -> bool:
        """
        Save or update a temporal pattern.

        Args:
            channel_id: Channel SCID
            hour_of_day: Hour (0-23) or None for all hours
            day_of_week: Day (0-6) or None for all days
            direction: "inbound", "outbound", or "balanced"
            intensity: Relative intensity (1.0 = average)
            confidence: Pattern confidence (0.0-1.0)
            samples: Number of observations
            avg_flow_sats: Average flow in this window
            detected_at: Detection timestamp

        Returns:
            True if saved successfully
        """
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT INTO temporal_patterns
                (channel_id, hour_of_day, day_of_week, direction, intensity,
                 confidence, samples, avg_flow_sats, detected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(channel_id, hour_of_day, day_of_week)
                DO UPDATE SET
                    direction = excluded.direction,
                    intensity = excluded.intensity,
                    confidence = excluded.confidence,
                    samples = excluded.samples,
                    avg_flow_sats = excluded.avg_flow_sats,
                    detected_at = excluded.detected_at
            """, (channel_id, hour_of_day, day_of_week, direction, intensity,
                  confidence, samples, avg_flow_sats, detected_at))
            return True
        except Exception as e:
            self.plugin.log(
                f"Failed to save temporal pattern: {e}",
                level="debug"
            )
            return False

    def get_temporal_patterns(
        self,
        channel_id: str = None,
        min_confidence: float = 0.5
    ) -> List[Dict[str, Any]]:
        """
        Get temporal patterns, optionally filtered by channel.

        Args:
            channel_id: Filter by channel (None for all)
            min_confidence: Minimum confidence threshold

        Returns:
            List of pattern dicts
        """
        conn = self._get_connection()

        if channel_id:
            rows = conn.execute("""
                SELECT * FROM temporal_patterns
                WHERE channel_id = ? AND confidence >= ?
                ORDER BY confidence DESC
            """, (channel_id, min_confidence)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM temporal_patterns
                WHERE confidence >= ?
                ORDER BY confidence DESC
            """, (min_confidence,)).fetchall()

        return [dict(row) for row in rows]

    def clear_temporal_patterns(self, channel_id: str = None) -> int:
        """
        Clear temporal patterns, optionally for a specific channel.

        Args:
            channel_id: Channel to clear (None for all)

        Returns:
            Number of patterns deleted
        """
        conn = self._get_connection()

        if channel_id:
            result = conn.execute("""
                DELETE FROM temporal_patterns
                WHERE channel_id = ?
            """, (channel_id,))
        else:
            result = conn.execute("DELETE FROM temporal_patterns")

        return result.rowcount

    # =========================================================================
    # LOCAL FEE TRACKING OPERATIONS
    # =========================================================================

    def save_local_fee_tracking(self, earned_sats: int, forward_count: int,
                                 period_start_ts: int, last_broadcast_ts: int,
                                 last_broadcast_amount: int) -> bool:
        """
        Persist local fee tracking state to survive restarts.

        Args:
            earned_sats: Total fees earned in current period
            forward_count: Number of forwards in current period
            period_start_ts: Period start timestamp
            last_broadcast_ts: Timestamp of last fee broadcast
            last_broadcast_amount: Fees at last broadcast

        Returns:
            True if saved successfully
        """
        conn = self._get_connection()
        now = int(time.time())

        try:
            conn.execute("""
                INSERT OR REPLACE INTO local_fee_tracking
                (id, earned_sats, forward_count, period_start_ts,
                 last_broadcast_ts, last_broadcast_amount, updated_at)
                VALUES (1, ?, ?, ?, ?, ?, ?)
            """, (earned_sats, forward_count, period_start_ts,
                  last_broadcast_ts, last_broadcast_amount, now))
            return True
        except Exception:
            return False

    def load_local_fee_tracking(self) -> Optional[Dict[str, int]]:
        """
        Load persisted fee tracking state.

        Returns:
            Dict with earned_sats, forward_count, period_start_ts,
            last_broadcast_ts, last_broadcast_amount, or None if not found
        """
        conn = self._get_connection()

        row = conn.execute("""
            SELECT earned_sats, forward_count, period_start_ts,
                   last_broadcast_ts, last_broadcast_amount
            FROM local_fee_tracking WHERE id = 1
        """).fetchone()

        if not row:
            return None

        return {
            "earned_sats": row["earned_sats"],
            "forward_count": row["forward_count"],
            "period_start_ts": row["period_start_ts"],
            "last_broadcast_ts": row["last_broadcast_ts"],
            "last_broadcast_amount": row["last_broadcast_amount"]
        }

    # =========================================================================
    # CONTRIBUTION RATE LIMIT OPERATIONS
    # =========================================================================

    def save_contribution_rate_limit(self, peer_id: str, window_start: int,
                                      event_count: int) -> bool:
        """
        Persist per-peer contribution rate limit state.

        Args:
            peer_id: Peer's public key
            window_start: Window start timestamp
            event_count: Events in current window

        Returns:
            True if saved successfully
        """
        conn = self._get_connection()

        try:
            conn.execute("""
                INSERT OR REPLACE INTO contribution_rate_limits
                (peer_id, window_start, event_count)
                VALUES (?, ?, ?)
            """, (peer_id, window_start, event_count))
            return True
        except Exception:
            return False

    def load_contribution_rate_limits(self) -> Dict[str, Tuple[int, int]]:
        """
        Load all persisted contribution rate limits.

        Returns:
            Dict mapping peer_id to (window_start, event_count)
        """
        conn = self._get_connection()

        rows = conn.execute("""
            SELECT peer_id, window_start, event_count
            FROM contribution_rate_limits
        """).fetchall()

        return {
            row["peer_id"]: (row["window_start"], row["event_count"])
            for row in rows
        }

    def save_contribution_daily_stats(self, window_start_ts: int,
                                       event_count: int) -> bool:
        """
        Persist global daily contribution stats.

        Args:
            window_start_ts: Daily window start timestamp
            event_count: Total events in current window

        Returns:
            True if saved successfully
        """
        conn = self._get_connection()

        try:
            conn.execute("""
                INSERT OR REPLACE INTO contribution_daily_stats
                (id, window_start_ts, event_count)
                VALUES (1, ?, ?)
            """, (window_start_ts, event_count))
            return True
        except Exception:
            return False

    def load_contribution_daily_stats(self) -> Optional[Dict[str, int]]:
        """
        Load persisted global daily contribution stats.

        Returns:
            Dict with window_start_ts and event_count, or None if not found
        """
        conn = self._get_connection()

        row = conn.execute("""
            SELECT window_start_ts, event_count
            FROM contribution_daily_stats WHERE id = 1
        """).fetchone()

        if not row:
            return None

        return {
            "window_start_ts": row["window_start_ts"],
            "event_count": row["event_count"]
        }

    def cleanup_old_rate_limits(self, max_age_seconds: int = 86400) -> int:
        """
        Clean up rate limit entries older than max_age_seconds.

        Args:
            max_age_seconds: Maximum age before cleanup (default 24h)

        Returns:
            Number of entries removed
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - max_age_seconds

        result = conn.execute("""
            DELETE FROM contribution_rate_limits
            WHERE window_start < ?
        """, (cutoff,))

        return result.rowcount

    # =========================================================================
    # SPLICE SESSION OPERATIONS (Phase 11)
    # =========================================================================

    # Valid values for splice session fields (kept in sync with protocol.py)
    _VALID_SPLICE_INITIATORS = {'local', 'remote'}
    _VALID_SPLICE_TYPES = {'splice_in', 'splice_out'}
    _VALID_SPLICE_STATUSES = {
        'pending', 'init_sent', 'init_received', 'updating',
        'signing', 'completed', 'aborted', 'failed'
    }
    _MAX_SPLICE_AMOUNT_SATS = 2_100_000_000_000_000  # 21M BTC in sats

    def create_splice_session(
        self,
        session_id: str,
        channel_id: str,
        peer_id: str,
        initiator: str,
        splice_type: str,
        amount_sats: int,
        timeout_seconds: int = 300
    ) -> bool:
        """
        Create a new splice session.

        Args:
            session_id: Unique session identifier
            channel_id: Channel being spliced
            peer_id: Hive member we're splicing with
            initiator: 'local' or 'remote'
            splice_type: 'splice_in' or 'splice_out'
            amount_sats: Amount to splice
            timeout_seconds: Session timeout (default 5 min)

        Returns:
            True if created successfully
        """
        # Validate inputs
        if initiator not in self._VALID_SPLICE_INITIATORS:
            self.plugin.log(f"Invalid splice initiator: {initiator}", level='warn')
            return False
        if splice_type not in self._VALID_SPLICE_TYPES:
            self.plugin.log(f"Invalid splice type: {splice_type}", level='warn')
            return False
        if not isinstance(amount_sats, int) or amount_sats <= 0 or amount_sats > self._MAX_SPLICE_AMOUNT_SATS:
            self.plugin.log(f"Invalid splice amount: {amount_sats}", level='warn')
            return False

        conn = self._get_connection()
        now = int(time.time())
        timeout_at = now + timeout_seconds

        try:
            conn.execute("""
                INSERT INTO splice_sessions
                (session_id, channel_id, peer_id, initiator, splice_type,
                 amount_sats, status, created_at, updated_at, timeout_at)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
            """, (session_id, channel_id, peer_id, initiator, splice_type,
                  amount_sats, now, now, timeout_at))
            return True
        except Exception as e:
            self.plugin.log(f"Failed to create splice session: {e}", level='debug')
            return False

    def get_splice_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a splice session by ID.

        Args:
            session_id: Session identifier

        Returns:
            Session data dict, or None if not found
        """
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM splice_sessions WHERE session_id = ?
        """, (session_id,)).fetchone()
        return dict(row) if row else None

    def get_active_splice_for_channel(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """
        Get active splice session for a channel.

        Args:
            channel_id: Channel ID

        Returns:
            Active session data, or None if no active session
        """
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM splice_sessions
            WHERE channel_id = ?
            AND status NOT IN ('completed', 'aborted', 'failed')
            ORDER BY created_at DESC
            LIMIT 1
        """, (channel_id,)).fetchone()
        return dict(row) if row else None

    def get_active_splice_for_peer(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get active splice session for a peer.

        Args:
            peer_id: Peer pubkey

        Returns:
            Active session data, or None if no active session
        """
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM splice_sessions
            WHERE peer_id = ?
            AND status NOT IN ('completed', 'aborted', 'failed')
            ORDER BY created_at DESC
            LIMIT 1
        """, (peer_id,)).fetchone()
        return dict(row) if row else None

    def update_splice_session(
        self,
        session_id: str,
        status: Optional[str] = None,
        psbt: Optional[str] = None,
        commitments_secured: Optional[bool] = None,
        txid: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> bool:
        """
        Update a splice session.

        Args:
            session_id: Session identifier
            status: New status
            psbt: Updated PSBT
            commitments_secured: Whether commitments are secured
            txid: Transaction ID if broadcast
            error_message: Error message if failed

        Returns:
            True if updated
        """
        conn = self._get_connection()
        now = int(time.time())

        updates = {"updated_at": now}
        if status is not None:
            if status not in self._VALID_SPLICE_STATUSES:
                self.plugin.log(f"Invalid splice status: {status}", level='warn')
                return False
            updates["status"] = status
            if status in ('completed', 'aborted', 'failed'):
                updates["completed_at"] = now
        if psbt is not None:
            updates["psbt"] = psbt
        if commitments_secured is not None:
            updates["commitments_secured"] = 1 if commitments_secured else 0
        if txid is not None:
            updates["txid"] = txid
        if error_message is not None:
            updates["error_message"] = error_message

        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [session_id]

        try:
            result = conn.execute(
                f"UPDATE splice_sessions SET {set_clause} WHERE session_id = ?",
                values
            )
            return result.rowcount > 0
        except Exception as e:
            self.plugin.log(f"Failed to update splice session: {e}", level='debug')
            return False

    def cleanup_expired_splice_sessions(self) -> int:
        """
        Mark expired splice sessions as failed.

        Returns:
            Number of sessions cleaned up
        """
        conn = self._get_connection()
        now = int(time.time())

        result = conn.execute("""
            UPDATE splice_sessions
            SET status = 'failed', error_message = 'timeout', completed_at = ?
            WHERE status NOT IN ('completed', 'aborted', 'failed')
            AND timeout_at < ?
        """, (now, now))

        return result.rowcount

    def get_pending_splice_sessions(self) -> List[Dict[str, Any]]:
        """
        Get all pending/active splice sessions.

        Returns:
            List of active session dicts
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM splice_sessions
            WHERE status NOT IN ('completed', 'aborted', 'failed')
            ORDER BY created_at ASC
        """).fetchall()
        return [dict(row) for row in rows]

    def delete_splice_session(self, session_id: str) -> bool:
        """
        Delete a splice session.

        Args:
            session_id: Session to delete

        Returns:
            True if deleted
        """
        conn = self._get_connection()
        result = conn.execute("""
            DELETE FROM splice_sessions WHERE session_id = ?
        """, (session_id,))
        return result.rowcount > 0

    # =========================================================================
    # FEE REPORT PERSISTENCE
    # =========================================================================

    def save_fee_report(
        self,
        peer_id: str,
        period: str,
        fees_earned_sats: int,
        forward_count: int,
        period_start: int,
        period_end: int,
        rebalance_costs_sats: int = 0
    ) -> bool:
        """
        Save or update a fee report from a hive member.

        This persists FEE_REPORT gossip data so it survives node restarts.
        Uses UPSERT to handle updates from the same peer for the same period.

        Args:
            peer_id: Member's node public key
            period: Settlement period (YYYY-WW format)
            fees_earned_sats: Fees earned in the period
            forward_count: Number of forwards in the period
            period_start: Period start timestamp
            period_end: Period end timestamp (report time)
            rebalance_costs_sats: Rebalancing costs in the period

        Returns:
            True if saved successfully
        """
        conn = self._get_connection()
        now = int(time.time())

        try:
            conn.execute("""
                INSERT INTO fee_reports
                (peer_id, period, fees_earned_sats, forward_count,
                 period_start, period_end, received_at, rebalance_costs_sats)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(peer_id, period) DO UPDATE SET
                    fees_earned_sats = excluded.fees_earned_sats,
                    forward_count = excluded.forward_count,
                    period_end = excluded.period_end,
                    received_at = excluded.received_at,
                    rebalance_costs_sats = excluded.rebalance_costs_sats
            """, (peer_id, period, fees_earned_sats, forward_count,
                  period_start, period_end, now, rebalance_costs_sats))
            return True
        except Exception as e:
            self.plugin.log(f"HiveDatabase: Failed to save fee report: {e}", level='warn')
            return False

    def get_fee_reports_for_period(self, period: str) -> List[Dict[str, Any]]:
        """
        Get all fee reports for a settlement period.

        Args:
            period: Settlement period (YYYY-WW format)

        Returns:
            List of fee report dicts with peer_id, fees_earned_sats, rebalance_costs_sats, etc.
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT peer_id, fees_earned_sats, forward_count,
                   period_start, period_end, received_at, rebalance_costs_sats
            FROM fee_reports
            WHERE period = ?
            ORDER BY peer_id
        """, (period,)).fetchall()
        return [dict(row) for row in rows]

    def get_latest_fee_reports(self) -> List[Dict[str, Any]]:
        """
        Get the most recent fee report for each peer.

        Returns:
            List of the latest fee report for each peer
        """
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT f1.peer_id, f1.period, f1.fees_earned_sats, f1.forward_count,
                   f1.period_start, f1.period_end, f1.received_at, f1.rebalance_costs_sats
            FROM fee_reports f1
            INNER JOIN (
                SELECT peer_id, MAX(received_at) as max_received
                FROM fee_reports
                GROUP BY peer_id
            ) f2 ON f1.peer_id = f2.peer_id AND f1.received_at = f2.max_received
            ORDER BY f1.peer_id
        """).fetchall()
        return [dict(row) for row in rows]

    def cleanup_old_fee_reports(self, keep_periods: int = 4) -> int:
        """
        Remove fee reports older than keep_periods weeks.

        Args:
            keep_periods: Number of recent periods to keep (default 4 weeks)

        Returns:
            Number of rows deleted
        """
        conn = self._get_connection()
        # Get current period and calculate cutoff
        import datetime
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        cutoff_date = now - datetime.timedelta(weeks=keep_periods)
        cutoff_period = f"{cutoff_date.isocalendar()[0]}-W{cutoff_date.isocalendar()[1]:02d}"

        result = conn.execute("""
            DELETE FROM fee_reports WHERE period < ?
        """, (cutoff_period,))
        return result.rowcount

    # =========================================================================
    # DISTRIBUTED SETTLEMENT OPERATIONS (Phase 12)
    # =========================================================================

    def add_settlement_proposal(
        self,
        proposal_id: str,
        period: str,
        proposer_peer_id: str,
        data_hash: str,
        total_fees_sats: int,
        member_count: int,
        plan_hash: Optional[str] = None,
        expires_in_seconds: int = 86400,  # 24 hours
        contributions_json: Optional[str] = None
    ) -> bool:
        """
        Add a new settlement proposal.

        Args:
            proposal_id: Unique proposal identifier
            period: Settlement period (YYYY-WW format)
            proposer_peer_id: Peer who proposed this settlement
            data_hash: Canonical hash of settlement data for verification
            total_fees_sats: Total fees to distribute
            member_count: Number of participating members
            expires_in_seconds: Time until proposal expires
            contributions_json: JSON-encoded contributions for rebroadcast (Issue #49)

        Returns:
            True if created, False if proposal for this period already exists
        """
        conn = self._get_connection()
        now = int(time.time())
        expires_at = now + expires_in_seconds

        try:
            conn.execute("""
                INSERT INTO settlement_proposals
                (proposal_id, period, proposer_peer_id, proposed_at, expires_at,
                 status, data_hash, plan_hash, total_fees_sats, member_count, last_broadcast_at,
                 contributions_json)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)
            """, (proposal_id, period, proposer_peer_id, now, expires_at,
                  data_hash, plan_hash, total_fees_sats, member_count, now, contributions_json))
            return True
        except sqlite3.IntegrityError:
            # Period already has a proposal
            return False

    def get_settlement_proposal(self, proposal_id: str) -> Optional[Dict[str, Any]]:
        """Get a settlement proposal by ID."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM settlement_proposals WHERE proposal_id = ?",
            (proposal_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_settlement_proposal_by_period(self, period: str) -> Optional[Dict[str, Any]]:
        """Get a settlement proposal by period (YYYY-WW)."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM settlement_proposals WHERE period = ?",
            (period,)
        ).fetchone()
        return dict(row) if row else None

    def get_pending_settlement_proposals(self) -> List[Dict[str, Any]]:
        """Get all pending settlement proposals."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM settlement_proposals
            WHERE status = 'pending'
            ORDER BY proposed_at ASC
        """).fetchall()
        return [dict(row) for row in rows]

    def get_ready_settlement_proposals(self) -> List[Dict[str, Any]]:
        """Get all ready-to-execute settlement proposals."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM settlement_proposals
            WHERE status = 'ready'
            ORDER BY proposed_at ASC
        """).fetchall()
        return [dict(row) for row in rows]

    def get_proposals_needing_rebroadcast(
        self,
        rebroadcast_interval_seconds: int,
        our_peer_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get pending proposals that need rebroadcast (Issue #49).

        Returns proposals where:
        - Status is 'pending' (not yet at quorum)
        - Not expired
        - We are the proposer
        - Last broadcast was more than rebroadcast_interval_seconds ago

        Args:
            rebroadcast_interval_seconds: Minimum seconds between broadcasts
            our_peer_id: Our node's public key (only proposer rebroadcasts)

        Returns:
            List of proposals needing rebroadcast
        """
        conn = self._get_connection()
        now = int(time.time())
        cutoff = now - rebroadcast_interval_seconds

        rows = conn.execute("""
            SELECT * FROM settlement_proposals
            WHERE status = 'pending'
            AND expires_at > ?
            AND proposer_peer_id = ?
            AND (last_broadcast_at IS NULL OR last_broadcast_at < ?)
            ORDER BY proposed_at ASC
        """, (now, our_peer_id, cutoff)).fetchall()
        return [dict(row) for row in rows]

    def update_proposal_broadcast_time(
        self,
        proposal_id: str,
        timestamp: Optional[int] = None
    ) -> bool:
        """
        Update the last_broadcast_at timestamp for a proposal (Issue #49).

        Args:
            proposal_id: Proposal to update
            timestamp: Broadcast timestamp (defaults to now)

        Returns:
            True if updated, False if proposal not found
        """
        conn = self._get_connection()
        if timestamp is None:
            timestamp = int(time.time())

        result = conn.execute("""
            UPDATE settlement_proposals
            SET last_broadcast_at = ?
            WHERE proposal_id = ?
        """, (timestamp, proposal_id))
        return result.rowcount > 0

    def update_settlement_proposal_status(
        self, proposal_id: str, status: str
    ) -> bool:
        """Update a settlement proposal's status."""
        conn = self._get_connection()
        result = conn.execute("""
            UPDATE settlement_proposals SET status = ? WHERE proposal_id = ?
        """, (status, proposal_id))
        return result.rowcount > 0

    def add_settlement_ready_vote(
        self,
        proposal_id: str,
        voter_peer_id: str,
        data_hash: str,
        signature: str
    ) -> bool:
        """
        Add a ready vote for a settlement proposal.

        Args:
            proposal_id: Proposal being voted on
            voter_peer_id: Peer casting the vote
            data_hash: Hash voter calculated (must match proposal)
            signature: Voter's signature

        Returns:
            True if vote added, False if already voted
        """
        conn = self._get_connection()
        now = int(time.time())

        try:
            conn.execute("""
                INSERT INTO settlement_ready_votes
                (proposal_id, voter_peer_id, data_hash, voted_at, signature)
                VALUES (?, ?, ?, ?, ?)
            """, (proposal_id, voter_peer_id, data_hash, now, signature))
            return True
        except sqlite3.IntegrityError:
            return False  # Already voted

    def get_settlement_ready_votes(self, proposal_id: str) -> List[Dict[str, Any]]:
        """Get all ready votes for a proposal."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM settlement_ready_votes WHERE proposal_id = ?
        """, (proposal_id,)).fetchall()
        return [dict(row) for row in rows]

    def count_settlement_ready_votes(self, proposal_id: str) -> int:
        """Count ready votes for a proposal."""
        conn = self._get_connection()
        result = conn.execute("""
            SELECT COUNT(*) FROM settlement_ready_votes WHERE proposal_id = ?
        """, (proposal_id,)).fetchone()
        return result[0] if result else 0

    def has_voted_settlement(self, proposal_id: str, voter_peer_id: str) -> bool:
        """Check if a peer has already voted on a proposal."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT 1 FROM settlement_ready_votes
            WHERE proposal_id = ? AND voter_peer_id = ?
        """, (proposal_id, voter_peer_id)).fetchone()
        return row is not None

    def add_settlement_execution(
        self,
        proposal_id: str,
        executor_peer_id: str,
        signature: str,
        payment_hash: Optional[str] = None,
        amount_paid_sats: Optional[int] = None,
        plan_hash: Optional[str] = None,
    ) -> bool:
        """
        Record a settlement execution by a member.

        Args:
            proposal_id: Proposal being executed
            executor_peer_id: Peer who executed their payment
            signature: Signature proving execution
            payment_hash: Payment hash if payment was made
            amount_paid_sats: Amount paid in sats

        Returns:
            True if recorded, False if already executed
        """
        conn = self._get_connection()
        now = int(time.time())

        try:
            conn.execute("""
                INSERT INTO settlement_executions
                (proposal_id, executor_peer_id, payment_hash, amount_paid_sats,
                 executed_at, signature, plan_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (proposal_id, executor_peer_id, payment_hash, amount_paid_sats,
                  now, signature, plan_hash))
            return True
        except sqlite3.IntegrityError:
            return False  # Already executed

    def get_settlement_executions(self, proposal_id: str) -> List[Dict[str, Any]]:
        """Get all execution records for a proposal."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM settlement_executions WHERE proposal_id = ?
        """, (proposal_id,)).fetchall()
        return [dict(row) for row in rows]

    def has_executed_settlement(
        self, proposal_id: str, executor_peer_id: str
    ) -> bool:
        """Check if a peer has already executed their settlement payment."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT 1 FROM settlement_executions
            WHERE proposal_id = ? AND executor_peer_id = ?
        """, (proposal_id, executor_peer_id)).fetchone()
        return row is not None

    def record_settlement_sub_payment(
        self, proposal_id: str, from_peer_id: str, to_peer_id: str,
        amount_sats: int, payment_hash: str, status: str
    ) -> bool:
        """Record a completed sub-payment for crash recovery (S-2 fix)."""
        conn = self._get_connection()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO settlement_sub_payments
                (proposal_id, from_peer_id, to_peer_id, amount_sats,
                 payment_hash, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (proposal_id, from_peer_id, to_peer_id, amount_sats,
                  payment_hash, status, int(time.time())))
            return True
        except Exception:
            return False

    def get_settlement_sub_payment(
        self, proposal_id: str, from_peer_id: str, to_peer_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get a specific sub-payment record for crash recovery."""
        conn = self._get_connection()
        row = conn.execute("""
            SELECT * FROM settlement_sub_payments
            WHERE proposal_id = ? AND from_peer_id = ? AND to_peer_id = ?
        """, (proposal_id, from_peer_id, to_peer_id)).fetchone()
        return dict(row) if row else None

    def is_period_settled(self, period: str) -> bool:
        """Check if a period has already been settled."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT 1 FROM settled_periods WHERE period = ?",
            (period,)
        ).fetchone()
        return row is not None

    def mark_period_settled(
        self,
        period: str,
        proposal_id: str,
        total_distributed_sats: int
    ) -> bool:
        """
        Mark a settlement period as complete.

        Args:
            period: Period that was settled (YYYY-WW)
            proposal_id: Proposal that completed the settlement
            total_distributed_sats: Total sats distributed

        Returns:
            True if marked, False if already settled
        """
        conn = self._get_connection()
        now = int(time.time())

        try:
            conn.execute("""
                INSERT INTO settled_periods
                (period, proposal_id, settled_at, total_distributed_sats)
                VALUES (?, ?, ?, ?)
            """, (period, proposal_id, now, total_distributed_sats))
            return True
        except sqlite3.IntegrityError:
            return False  # Already settled

    def get_settled_periods(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recently settled periods."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM settled_periods
            ORDER BY settled_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(row) for row in rows]

    def cleanup_expired_settlement_proposals(self) -> int:
        """
        Mark expired settlement proposals as 'expired'.

        Returns:
            Number of proposals cleaned up
        """
        conn = self._get_connection()
        now = int(time.time())

        result = conn.execute("""
            UPDATE settlement_proposals
            SET status = 'expired'
            WHERE status = 'pending'
            AND expires_at < ?
        """, (now,))

        return result.rowcount

    def prune_old_settlement_data(self, older_than_days: int = 90) -> int:
        """
        Remove old settlement data (proposals, votes, executions).

        Wrapped in a transaction so all three DELETEs succeed or fail together,
        preventing orphaned votes/executions if interrupted mid-prune.

        Args:
            older_than_days: Remove data older than this many days

        Returns:
            Total number of rows deleted
        """
        cutoff = int(time.time()) - (older_than_days * 86400)
        total = 0

        with self.transaction() as conn:
            # Get old proposal IDs first
            old_proposals = conn.execute("""
                SELECT proposal_id FROM settlement_proposals
                WHERE proposed_at < ?
            """, (cutoff,)).fetchall()

            old_ids = [row[0] for row in old_proposals]

            if old_ids:
                placeholders = ",".join("?" * len(old_ids))

                # Delete executions
                result = conn.execute(
                    f"DELETE FROM settlement_executions WHERE proposal_id IN ({placeholders})",
                    old_ids
                )
                total += result.rowcount

                # Delete votes
                result = conn.execute(
                    f"DELETE FROM settlement_ready_votes WHERE proposal_id IN ({placeholders})",
                    old_ids
                )
                total += result.rowcount

                # Delete proposals
                result = conn.execute(
                    f"DELETE FROM settlement_proposals WHERE proposal_id IN ({placeholders})",
                    old_ids
                )
                total += result.rowcount

        return total

    def prune_old_settlement_periods(self, older_than_days: int = 365) -> int:
        """
        Remove old fee_reports and pool data older than specified days.

        Prunes fee_reports, pool_contributions, pool_revenue, and
        pool_distributions that are older than the cutoff.

        Args:
            older_than_days: Remove data older than this many days

        Returns:
            Total number of rows deleted
        """
        cutoff = int(time.time()) - (older_than_days * 86400)
        total = 0

        with self.transaction() as conn:
            # Prune old fee reports by period_end timestamp
            result = conn.execute(
                "DELETE FROM fee_reports WHERE period_end < ?", (cutoff,)
            )
            total += result.rowcount

            # Prune old pool revenue
            result = conn.execute(
                "DELETE FROM pool_revenue WHERE recorded_at < ?", (cutoff,)
            )
            total += result.rowcount

            # Prune old pool distributions
            result = conn.execute(
                "DELETE FROM pool_distributions WHERE settled_at < ?", (cutoff,)
            )
            total += result.rowcount

        return total

    # =========================================================================
    # PEER CAPABILITIES (Phase B - Version Tolerance)
    # =========================================================================

    def save_peer_capabilities(self, peer_id: str, features: list) -> bool:
        """
        Save or update a peer's advertised capabilities.

        Parses 'proto-vN' from the features list to populate max_protocol_version.

        Args:
            peer_id: Peer's public key
            features: List of feature strings from ATTEST manifest

        Returns:
            True if saved successfully
        """
        if not isinstance(features, list):
            return False

        max_proto = 1
        plugin_version = ''
        for f in features:
            if isinstance(f, str) and f.startswith('proto-v'):
                try:
                    v = int(f[7:])
                    max_proto = max(max_proto, v)
                except ValueError:
                    pass
            if isinstance(f, str) and f.startswith('cl-hive'):
                plugin_version = f

        conn = self._get_connection()
        try:
            conn.execute(
                """INSERT INTO peer_capabilities
                   (peer_id, features, max_protocol_version, plugin_version, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(peer_id) DO UPDATE SET
                       features = excluded.features,
                       max_protocol_version = excluded.max_protocol_version,
                       plugin_version = excluded.plugin_version,
                       updated_at = excluded.updated_at""",
                (peer_id, json.dumps(features), max_proto, plugin_version, int(time.time()))
            )
            return True
        except Exception as e:
            self.plugin.log(f"HiveDatabase: save_peer_capabilities error: {e}", level='warn')
            return False

    def get_peer_capabilities(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a peer's capabilities record.

        Returns:
            Dict with features, max_protocol_version, plugin_version, updated_at
            or None if not found.
        """
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM peer_capabilities WHERE peer_id = ?",
            (peer_id,)
        ).fetchone()
        if not row:
            return None
        result = dict(row)
        try:
            result['features'] = json.loads(result.get('features', '[]'))
        except (json.JSONDecodeError, TypeError):
            result['features'] = []
        return result

    def get_peer_max_protocol_version(self, peer_id: str) -> int:
        """
        Get the max protocol version a peer supports.

        Returns:
            Integer version (defaults to 1 if unknown).
        """
        conn = self._get_connection()
        row = conn.execute(
            "SELECT max_protocol_version FROM peer_capabilities WHERE peer_id = ?",
            (peer_id,)
        ).fetchone()
        return row['max_protocol_version'] if row else 1

    # =========================================================================
    # PROTO EVENTS (Phase C - Deterministic Idempotency)
    # =========================================================================

    def record_proto_event(self, event_id: str, event_type: str, actor_id: str) -> bool:
        """
        Record a protocol event for idempotency.

        Uses INSERT OR IGNORE so duplicate event_ids are silently skipped.

        Args:
            event_id: SHA256-based unique event identifier
            event_type: Message type name (e.g. 'MEMBER_LEFT')
            actor_id: Peer that originated the event

        Returns:
            True if this is a new event (inserted), False if duplicate.
        """
        conn = self._get_connection()
        now = int(time.time())
        try:
            result = conn.execute(
                """INSERT OR IGNORE INTO proto_events
                   (event_id, event_type, actor_id, created_at, received_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (event_id, event_type, actor_id, now, now)
            )
            return result.rowcount > 0
        except Exception as e:
            self.plugin.log(f"HiveDatabase: record_proto_event error: {e}", level='warn')
            return False

    def has_proto_event(self, event_id: str) -> bool:
        """Check if a protocol event has already been recorded."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT 1 FROM proto_events WHERE event_id = ?",
            (event_id,)
        ).fetchone()
        return row is not None

    def cleanup_proto_events(self, max_age_seconds: int = 30 * 86400) -> int:
        """
        Remove proto_events older than max_age_seconds.

        Args:
            max_age_seconds: Maximum age in seconds (default 30 days)

        Returns:
            Number of rows pruned.
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - max_age_seconds
        result = conn.execute(
            "DELETE FROM proto_events WHERE created_at < ?",
            (cutoff,)
        )
        return result.rowcount

    # =========================================================================
    # PROTO OUTBOX OPERATIONS (Phase D - Reliable Delivery)
    # =========================================================================

    def enqueue_outbox(self, msg_id: str, peer_id: str, msg_type: int,
                       payload_json: str, expires_at: int) -> bool:
        """
        Enqueue a message for reliable delivery to a specific peer.

        Uses INSERT OR IGNORE for idempotent enqueue (same msg_id+peer_id
        is silently ignored).

        Args:
            msg_id: Unique message identifier
            peer_id: Target peer pubkey
            msg_type: HiveMessageType integer value
            payload_json: JSON-serialized payload
            expires_at: Unix timestamp when message expires

        Returns:
            True if inserted, False if duplicate or error.
        """
        conn = self._get_connection()
        now = int(time.time())
        try:
            result = conn.execute(
                """INSERT OR IGNORE INTO proto_outbox
                   (msg_id, peer_id, msg_type, payload_json, status,
                    created_at, next_retry_at, expires_at)
                   VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)""",
                (msg_id, peer_id, msg_type, payload_json, now, now, expires_at)
            )
            return result.rowcount > 0
        except Exception as e:
            self.plugin.log(f"enqueue_outbox error: {e}", level='warn')
            return False

    def get_outbox_pending(self, limit: int = 50) -> list:
        """
        Get outbox entries ready for sending or retry.

        Returns entries where:
        - status is 'queued' or 'sent' (pending ack)
        - next_retry_at <= now (ready to retry)
        - expires_at > now (not expired)

        Args:
            limit: Maximum entries to return (default 50)

        Returns:
            List of dicts with outbox entry fields.
        """
        conn = self._get_connection()
        now = int(time.time())
        rows = conn.execute(
            """SELECT id, msg_id, peer_id, msg_type, payload_json, status,
                      created_at, sent_at, next_retry_at, retry_count,
                      expires_at, last_error
               FROM proto_outbox
               WHERE status IN ('queued', 'sent')
                 AND next_retry_at <= ?
                 AND expires_at > ?
               ORDER BY next_retry_at ASC
               LIMIT ?""",
            (now, now, limit)
        ).fetchall()
        return [dict(row) for row in rows]

    def update_outbox_sent(self, msg_id: str, peer_id: str,
                           next_retry_at: int) -> bool:
        """
        Mark an outbox entry as sent and schedule next retry.

        Args:
            msg_id: Message identifier
            peer_id: Target peer pubkey
            next_retry_at: Unix timestamp for next retry attempt

        Returns:
            True if updated, False otherwise.
        """
        conn = self._get_connection()
        now = int(time.time())
        result = conn.execute(
            """UPDATE proto_outbox
               SET status = 'sent', sent_at = ?, retry_count = retry_count + 1,
                   next_retry_at = ?
               WHERE msg_id = ? AND peer_id = ?
                 AND status IN ('queued', 'sent')""",
            (now, next_retry_at, msg_id, peer_id)
        )
        return result.rowcount > 0

    def update_outbox_retry(self, msg_id: str, peer_id: str,
                            next_retry_at: int) -> bool:
        """
        Schedule next retry for a failed send WITHOUT incrementing retry_count.

        Used when send_fn fails (peer unreachable) — the message was never
        transmitted, so retry budget should not be consumed.

        Args:
            msg_id: Message identifier
            peer_id: Target peer pubkey
            next_retry_at: Unix timestamp for next retry attempt

        Returns:
            True if updated, False otherwise.
        """
        conn = self._get_connection()
        result = conn.execute(
            """UPDATE proto_outbox
               SET next_retry_at = ?
               WHERE msg_id = ? AND peer_id = ?
                 AND status IN ('queued', 'sent')""",
            (next_retry_at, msg_id, peer_id)
        )
        return result.rowcount > 0

    def ack_outbox(self, msg_id: str, peer_id: str) -> bool:
        """
        Mark an outbox entry as acknowledged.

        Args:
            msg_id: Message identifier (the _event_id)
            peer_id: Peer that acknowledged

        Returns:
            True if updated, False otherwise.
        """
        conn = self._get_connection()
        now = int(time.time())
        result = conn.execute(
            """UPDATE proto_outbox
               SET status = 'acked', acked_at = ?
               WHERE msg_id = ? AND peer_id = ?
                 AND status IN ('queued', 'sent')""",
            (now, msg_id, peer_id)
        )
        return result.rowcount > 0

    def ack_outbox_by_type(self, peer_id: str, msg_type: int,
                           match_field: str, match_value: str) -> int:
        """
        Acknowledge outbox entries by type and payload field match.

        Used for implicit acks: e.g. receiving SETTLEMENT_READY clears the
        SETTLEMENT_PROPOSE outbox entries for that peer+proposal_id.

        Args:
            peer_id: Peer that implicitly acknowledged
            msg_type: The original message type integer to match
            match_field: JSON field name to match in payload
            match_value: Expected value of the field

        Returns:
            Number of entries acknowledged.
        """
        conn = self._get_connection()
        now = int(time.time())
        # Use json_extract for matching payload fields
        # Fallback to LIKE for SQLite versions without json_extract
        try:
            result = conn.execute(
                """UPDATE proto_outbox
                   SET status = 'acked', acked_at = ?
                   WHERE peer_id = ? AND msg_type = ?
                     AND status IN ('queued', 'sent')
                     AND json_extract(payload_json, ?) = ?""",
                (now, peer_id, msg_type, f'$.{match_field}', match_value)
            )
            return result.rowcount
        except Exception:
            # Fallback: match using LIKE pattern for older SQLite
            # Escape LIKE metacharacters in match_value to prevent over-matching
            safe_value = match_value.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
            pattern = f'"{match_field}":"{safe_value}"'
            try:
                result = conn.execute(
                    """UPDATE proto_outbox
                       SET status = 'acked', acked_at = ?
                       WHERE peer_id = ? AND msg_type = ?
                         AND status IN ('queued', 'sent')
                         AND payload_json LIKE ? ESCAPE '\\'""",
                    (now, peer_id, msg_type, f'%{pattern}%')
                )
                return result.rowcount
            except Exception as e:
                self.plugin.log(f"ack_outbox_by_type error: {e}", level='warn')
                return 0

    def fail_outbox(self, msg_id: str, peer_id: str, error: str) -> bool:
        """
        Mark an outbox entry as permanently failed.

        Args:
            msg_id: Message identifier
            peer_id: Target peer pubkey
            error: Error description

        Returns:
            True if updated, False otherwise.
        """
        conn = self._get_connection()
        result = conn.execute(
            """UPDATE proto_outbox
               SET status = 'failed', last_error = ?
               WHERE msg_id = ? AND peer_id = ?
                 AND status IN ('queued', 'sent')""",
            (error[:500], msg_id, peer_id)
        )
        return result.rowcount > 0

    def expire_outbox(self) -> int:
        """
        Mark expired outbox entries.

        Returns:
            Number of entries expired.
        """
        conn = self._get_connection()
        now = int(time.time())
        result = conn.execute(
            """UPDATE proto_outbox
               SET status = 'expired'
               WHERE expires_at <= ? AND status IN ('queued', 'sent')""",
            (now,)
        )
        return result.rowcount

    def cleanup_outbox(self, max_age_seconds: int = 7 * 86400) -> int:
        """
        Delete terminal outbox entries (acked/failed/expired) older than threshold.

        Args:
            max_age_seconds: Maximum age in seconds (default 7 days)

        Returns:
            Number of entries cleaned up.
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - max_age_seconds
        result = conn.execute(
            """DELETE FROM proto_outbox
               WHERE status IN ('acked', 'failed', 'expired')
                 AND created_at < ?""",
            (cutoff,)
        )
        return result.rowcount

    def count_inflight_for_peer(self, peer_id: str) -> int:
        """
        Count active (queued or sent) outbox entries for a peer.

        Used for backpressure: reject new enqueues when too many are inflight.

        Args:
            peer_id: Target peer pubkey

        Returns:
            Count of inflight entries.
        """
        conn = self._get_connection()
        row = conn.execute(
            """SELECT COUNT(*) as cnt FROM proto_outbox
               WHERE peer_id = ? AND status IN ('queued', 'sent')""",
            (peer_id,)
        ).fetchone()
        return row['cnt'] if row else 0

    # =========================================================================
    # ROUTING INTELLIGENCE PERSISTENCE
    # =========================================================================

    def save_pheromone_levels(self, levels: List[Dict[str, Any]]) -> int:
        """
        Bulk-save pheromone levels (full-table replace).

        Args:
            levels: List of dicts with channel_id, level, fee_ppm, last_update

        Returns:
            Number of rows written.
        """
        conn = self._get_connection()
        conn.execute("DELETE FROM pheromone_levels")
        for row in levels:
            conn.execute(
                """INSERT INTO pheromone_levels (channel_id, level, fee_ppm, last_update)
                   VALUES (?, ?, ?, ?)""",
                (row['channel_id'], row['level'], row['fee_ppm'], row['last_update'])
            )
        conn.commit()
        return len(levels)

    def load_pheromone_levels(self) -> List[Dict[str, Any]]:
        """Load all persisted pheromone levels."""
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM pheromone_levels").fetchall()
        return [dict(r) for r in rows]

    def save_stigmergic_markers(self, markers: List[Dict[str, Any]]) -> int:
        """
        Bulk-save stigmergic markers (full-table replace).

        Args:
            markers: List of dicts with depositor, source_peer_id,
                     destination_peer_id, fee_ppm, success, volume_sats,
                     timestamp, strength

        Returns:
            Number of rows written.
        """
        conn = self._get_connection()
        conn.execute("DELETE FROM stigmergic_markers")
        for row in markers:
            conn.execute(
                """INSERT INTO stigmergic_markers
                   (depositor, source_peer_id, destination_peer_id,
                    fee_ppm, success, volume_sats, timestamp, strength)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (row['depositor'], row['source_peer_id'],
                 row['destination_peer_id'], row['fee_ppm'],
                 1 if row['success'] else 0, row['volume_sats'],
                 row['timestamp'], row['strength'])
            )
        conn.commit()
        return len(markers)

    def load_stigmergic_markers(self) -> List[Dict[str, Any]]:
        """Load all persisted stigmergic markers."""
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM stigmergic_markers").fetchall()
        return [dict(r) for r in rows]

    def get_pheromone_count(self) -> int:
        """Get count of persisted pheromone levels."""
        conn = self._get_connection()
        row = conn.execute("SELECT COUNT(*) as cnt FROM pheromone_levels").fetchone()
        return row['cnt'] if row else 0

    def get_latest_marker_timestamp(self) -> Optional[float]:
        """Get the most recent marker timestamp, or None if empty."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT MAX(timestamp) as latest FROM stigmergic_markers"
        ).fetchone()
        return row['latest'] if row and row['latest'] is not None else None

    def save_defense_state(self, reports: List[Dict[str, Any]],
                           active_fees: List[Dict[str, Any]]) -> int:
        """
        Bulk-save defense warning reports and active fees (full-table replace).

        Args:
            reports: List of dicts with peer_id, reporter_id, threat_type,
                     severity, timestamp, ttl, evidence_json
            active_fees: List of dicts with peer_id, multiplier, expires_at,
                         threat_type, reporter, report_count

        Returns:
            Total number of rows written across both tables.
        """
        conn = self._get_connection()
        conn.execute("DELETE FROM defense_warning_reports")
        conn.execute("DELETE FROM defense_active_fees")
        for row in reports:
            conn.execute(
                """INSERT INTO defense_warning_reports
                   (peer_id, reporter_id, threat_type, severity, timestamp, ttl, evidence_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (row['peer_id'], row['reporter_id'], row['threat_type'],
                 row['severity'], row['timestamp'], row['ttl'],
                 row.get('evidence_json', '{}'))
            )
        for row in active_fees:
            conn.execute(
                """INSERT INTO defense_active_fees
                   (peer_id, multiplier, expires_at, threat_type, reporter, report_count)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (row['peer_id'], row['multiplier'], row['expires_at'],
                 row['threat_type'], row['reporter'], row['report_count'])
            )
        conn.commit()
        return len(reports) + len(active_fees)

    def load_defense_state(self) -> Dict[str, Any]:
        """
        Load persisted defense warning reports and active fees.

        Returns:
            Dict with 'reports' and 'active_fees' lists.
        """
        conn = self._get_connection()
        report_rows = conn.execute(
            "SELECT * FROM defense_warning_reports"
        ).fetchall()
        fee_rows = conn.execute(
            "SELECT * FROM defense_active_fees"
        ).fetchall()
        return {
            'reports': [dict(r) for r in report_rows],
            'active_fees': [dict(r) for r in fee_rows],
        }

    def save_remote_pheromones(self, pheromones: List[Dict[str, Any]]) -> int:
        """
        Bulk-save remote pheromones (full-table replace).

        Args:
            pheromones: List of dicts with peer_id, reporter_id, level,
                        fee_ppm, timestamp, weight

        Returns:
            Number of rows written.
        """
        conn = self._get_connection()
        conn.execute("DELETE FROM remote_pheromones")
        for row in pheromones:
            conn.execute(
                """INSERT INTO remote_pheromones
                   (peer_id, reporter_id, level, fee_ppm, timestamp, weight)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (row['peer_id'], row['reporter_id'], row['level'],
                 row['fee_ppm'], row['timestamp'], row['weight'])
            )
        conn.commit()
        return len(pheromones)

    def load_remote_pheromones(self) -> List[Dict[str, Any]]:
        """Load all persisted remote pheromones."""
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM remote_pheromones").fetchall()
        return [dict(r) for r in rows]

    def save_fee_observations(self, observations: List[Dict[str, Any]]) -> int:
        """
        Bulk-save fee observations (full-table replace).

        Args:
            observations: List of dicts with timestamp, fee_ppm

        Returns:
            Number of rows written.
        """
        conn = self._get_connection()
        conn.execute("DELETE FROM fee_observations")
        for row in observations:
            conn.execute(
                """INSERT INTO fee_observations (timestamp, fee_ppm)
                   VALUES (?, ?)""",
                (row['timestamp'], row['fee_ppm'])
            )
        conn.commit()
        return len(observations)

    def load_fee_observations(self) -> List[Dict[str, Any]]:
        """Load all persisted fee observations."""
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM fee_observations").fetchall()
        return [dict(r) for r in rows]
