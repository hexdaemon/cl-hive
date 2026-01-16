"""
AI Advisor Database - Historical tracking for intelligent decision-making.

This module provides persistent storage for:
- Fleet snapshots (hourly/daily state for trend analysis)
- Channel history (balance, fees, flow over time)
- Decision audit trail (recommendations and outcomes)
- Computed metrics (velocity, trends, predictions)

Usage:
    from advisor_db import AdvisorDB

    db = AdvisorDB("/path/to/advisor.db")
    db.record_fleet_snapshot(report_data)
    db.record_channel_state(node, channel_data)

    # Query trends
    velocity = db.get_channel_velocity("alice", "243x1x0")
    trends = db.get_fleet_trends(days=7)
"""

import hashlib
import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# =============================================================================
# Database Schema
# =============================================================================

SCHEMA_VERSION = 2

SCHEMA = """
-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);

-- Fleet-wide periodic snapshots
CREATE TABLE IF NOT EXISTS fleet_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    snapshot_type TEXT NOT NULL,  -- 'hourly', 'daily', 'manual'

    -- Fleet aggregates
    total_nodes INTEGER,
    nodes_healthy INTEGER,
    nodes_unhealthy INTEGER,
    total_channels INTEGER,
    total_capacity_sats INTEGER,
    total_onchain_sats INTEGER,

    -- Financial
    total_revenue_sats INTEGER,
    total_costs_sats INTEGER,
    net_profit_sats INTEGER,

    -- Channel health
    channels_balanced INTEGER,
    channels_needs_inbound INTEGER,
    channels_needs_outbound INTEGER,

    -- Hive state
    hive_member_count INTEGER,
    pending_actions INTEGER,

    -- Full report JSON for detailed queries
    full_report TEXT
);
CREATE INDEX IF NOT EXISTS idx_fleet_snapshots_time ON fleet_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_fleet_snapshots_type ON fleet_snapshots(snapshot_type, timestamp);

-- Per-channel historical data
CREATE TABLE IF NOT EXISTS channel_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    node_name TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    peer_id TEXT NOT NULL,

    -- Balance state
    capacity_sats INTEGER,
    local_sats INTEGER,
    remote_sats INTEGER,
    balance_ratio REAL,

    -- Flow metrics
    flow_state TEXT,
    flow_ratio REAL,
    confidence REAL,
    forward_count INTEGER,

    -- Fees
    fee_ppm INTEGER,
    fee_base_msat INTEGER,

    -- Health flags
    needs_inbound INTEGER,
    needs_outbound INTEGER,
    is_balanced INTEGER
);
CREATE INDEX IF NOT EXISTS idx_channel_history_lookup
    ON channel_history(node_name, channel_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_channel_history_time ON channel_history(timestamp);

-- Computed channel velocity (updated periodically)
CREATE TABLE IF NOT EXISTS channel_velocity (
    node_name TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    updated_at INTEGER NOT NULL,

    -- Current state
    current_local_sats INTEGER,
    current_balance_ratio REAL,

    -- Velocity metrics (change per hour)
    balance_velocity_sats_per_hour REAL,
    balance_velocity_pct_per_hour REAL,

    -- Predictions
    hours_until_depleted REAL,      -- NULL if not depleting
    hours_until_full REAL,          -- NULL if not filling
    predicted_depletion_time INTEGER,

    -- Trend
    trend TEXT,  -- 'depleting', 'filling', 'stable', 'unknown'
    trend_confidence REAL,

    PRIMARY KEY (node_name, channel_id)
);

-- AI decision audit trail
CREATE TABLE IF NOT EXISTS ai_decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER NOT NULL,
    decision_type TEXT NOT NULL,
    node_name TEXT NOT NULL,
    channel_id TEXT,
    peer_id TEXT,

    -- Recommendation
    recommendation TEXT NOT NULL,
    reasoning TEXT,
    confidence REAL,

    -- Status tracking
    status TEXT DEFAULT 'recommended',
    executed_at INTEGER,
    execution_result TEXT,

    -- Outcome (filled later)
    outcome_measured_at INTEGER,
    outcome_success INTEGER,
    outcome_metrics TEXT,

    -- Snapshot at decision time for outcome comparison
    snapshot_metrics TEXT
);
CREATE INDEX IF NOT EXISTS idx_ai_decisions_type ON ai_decisions(decision_type, timestamp);
CREATE INDEX IF NOT EXISTS idx_ai_decisions_status ON ai_decisions(status);

-- Alert deduplication tracking
CREATE TABLE IF NOT EXISTS alert_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type TEXT NOT NULL,      -- 'zombie', 'bleeder', 'depleting', 'velocity', 'unprofitable'
    node_name TEXT NOT NULL,
    channel_id TEXT,
    peer_id TEXT,
    alert_hash TEXT UNIQUE,        -- hash(type+node+channel) for dedup

    first_flagged INTEGER NOT NULL,
    last_flagged INTEGER NOT NULL,
    times_flagged INTEGER DEFAULT 1,
    severity TEXT DEFAULT 'warning',  -- 'info', 'warning', 'critical'
    message TEXT,

    resolved INTEGER DEFAULT 0,
    resolved_at INTEGER,
    resolution_action TEXT
);
CREATE INDEX IF NOT EXISTS idx_alert_history_hash ON alert_history(alert_hash);
CREATE INDEX IF NOT EXISTS idx_alert_history_unresolved ON alert_history(resolved, last_flagged);

-- Long-term peer intelligence
CREATE TABLE IF NOT EXISTS peer_intelligence (
    peer_id TEXT PRIMARY KEY,
    alias TEXT,
    first_seen INTEGER,
    last_seen INTEGER,

    -- Reliability metrics
    channels_opened INTEGER DEFAULT 0,
    channels_closed INTEGER DEFAULT 0,
    force_closes INTEGER DEFAULT 0,
    avg_channel_lifetime_days REAL,

    -- Performance metrics
    total_forwards INTEGER DEFAULT 0,
    total_revenue_sats INTEGER DEFAULT 0,
    total_costs_sats INTEGER DEFAULT 0,
    avg_fee_earned_ppm REAL,

    -- Computed scores
    profitability_score REAL,
    reliability_score REAL,
    recommendation TEXT DEFAULT 'unknown'  -- 'excellent', 'good', 'neutral', 'caution', 'avoid'
);
CREATE INDEX IF NOT EXISTS idx_peer_intelligence_recommendation ON peer_intelligence(recommendation);
"""


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ChannelVelocity:
    """Computed velocity metrics for a channel."""
    node_name: str
    channel_id: str
    current_local_sats: int
    current_balance_ratio: float
    velocity_sats_per_hour: float
    velocity_pct_per_hour: float
    hours_until_depleted: Optional[float]
    hours_until_full: Optional[float]
    trend: str  # 'depleting', 'filling', 'stable', 'unknown'
    confidence: float

    @property
    def is_critical(self) -> bool:
        """True if channel will deplete/fill within 24 hours."""
        if self.hours_until_depleted and self.hours_until_depleted < 24:
            return True
        if self.hours_until_full and self.hours_until_full < 24:
            return True
        return False

    @property
    def urgency(self) -> str:
        """Return urgency level."""
        hours = self.hours_until_depleted or self.hours_until_full
        if not hours:
            return "none"
        if hours < 4:
            return "critical"
        if hours < 12:
            return "high"
        if hours < 24:
            return "medium"
        return "low"


@dataclass
class FleetTrend:
    """Trend metrics for the fleet."""
    period_hours: int
    revenue_change_pct: float
    capacity_change_pct: float
    channel_count_change: int
    health_trend: str  # 'improving', 'stable', 'declining'
    channels_depleting: int
    channels_filling: int


@dataclass
class AlertStatus:
    """Status of an alert for deduplication."""
    alert_type: str
    node_name: str
    channel_id: Optional[str]
    is_new: bool
    first_flagged: Optional[datetime]
    last_flagged: Optional[datetime]
    times_flagged: int
    hours_since_last: float
    action: str  # 'flag', 'skip', 'escalate', 'mention_unresolved'
    message: str


@dataclass
class PeerIntelligence:
    """Peer reputation and performance metrics."""
    peer_id: str
    alias: Optional[str]
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    channels_opened: int
    channels_closed: int
    force_closes: int
    avg_channel_lifetime_days: Optional[float]
    total_forwards: int
    total_revenue_sats: int
    total_costs_sats: int
    profitability_score: Optional[float]
    reliability_score: Optional[float]
    recommendation: str  # 'excellent', 'good', 'neutral', 'caution', 'avoid', 'unknown'


@dataclass
class ContextBrief:
    """Pre-run context summary for the AI advisor."""
    period_days: int
    # Fleet metrics
    total_capacity_sats: int
    capacity_change_pct: float
    total_channels: int
    channel_count_change: int
    # Financial
    period_revenue_sats: int
    revenue_change_pct: float
    # Velocity alerts
    channels_depleting: int
    channels_filling: int
    critical_velocity_channels: List[str]
    # Unresolved alerts
    unresolved_alerts: List[Dict]
    # Recent decisions
    recent_decisions_count: int
    decisions_by_type: Dict[str, int]
    # Summary text
    summary_text: str


# =============================================================================
# Database Class
# =============================================================================

class AdvisorDB:
    """AI Advisor database for historical tracking and trend analysis."""

    def __init__(self, db_path: str = None):
        """Initialize database connection."""
        if db_path is None:
            db_path = str(Path.home() / ".lightning" / "advisor.db")

        self.db_path = db_path
        self._local = threading.local()

        # Ensure directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Initialize schema
        self._init_schema()

    @contextmanager
    def _get_conn(self):
        """Get thread-local database connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(self.db_path)
            self._local.conn.row_factory = sqlite3.Row
            # Enable WAL mode for better concurrency
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")

        try:
            yield self._local.conn
        except Exception:
            self._local.conn.rollback()
            raise

    def _init_schema(self):
        """Initialize database schema."""
        with self._get_conn() as conn:
            # Check current version
            try:
                row = conn.execute(
                    "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
                ).fetchone()
                current_version = row[0] if row else 0
            except sqlite3.OperationalError:
                current_version = 0

            if current_version < SCHEMA_VERSION:
                # Apply schema
                conn.executescript(SCHEMA)
                conn.execute(
                    "INSERT OR REPLACE INTO schema_version (version, applied_at) VALUES (?, ?)",
                    (SCHEMA_VERSION, int(datetime.now().timestamp()))
                )
                conn.commit()

    # =========================================================================
    # Recording Methods
    # =========================================================================

    def record_fleet_snapshot(self, report: Dict[str, Any],
                              snapshot_type: str = "manual") -> int:
        """Record a fleet snapshot from a monitor report."""
        summary = report.get("fleet_summary", {})
        channel_health = summary.get("channel_health", {})
        topology = report.get("hive_topology", {})

        # Calculate financials from nodes
        total_revenue = 0
        total_costs = 0
        for node_data in report.get("nodes", {}).values():
            history = node_data.get("lifetime_history", {})
            total_revenue += history.get("lifetime_revenue_sats", 0)
            total_costs += history.get("lifetime_total_costs_sats", 0)

        with self._get_conn() as conn:
            cursor = conn.execute("""
                INSERT INTO fleet_snapshots (
                    timestamp, snapshot_type,
                    total_nodes, nodes_healthy, nodes_unhealthy,
                    total_channels, total_capacity_sats, total_onchain_sats,
                    total_revenue_sats, total_costs_sats, net_profit_sats,
                    channels_balanced, channels_needs_inbound, channels_needs_outbound,
                    hive_member_count, pending_actions,
                    full_report
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                int(datetime.now().timestamp()),
                snapshot_type,
                summary.get("total_nodes", 0),
                summary.get("nodes_healthy", 0),
                summary.get("nodes_unhealthy", 0),
                summary.get("total_channels", 0),
                summary.get("total_capacity_sats", 0),
                summary.get("total_onchain_sats", 0),
                total_revenue,
                total_costs,
                total_revenue - total_costs,
                channel_health.get("balanced", 0),
                channel_health.get("needs_inbound", 0),
                channel_health.get("needs_outbound", 0),
                topology.get("member_count", 0),
                summary.get("total_pending_actions", 0),
                json.dumps(report)
            ))
            conn.commit()
            return cursor.lastrowid

    def record_channel_states(self, report: Dict[str, Any]) -> int:
        """Record channel states from all nodes in a report."""
        timestamp = int(datetime.now().timestamp())
        count = 0

        with self._get_conn() as conn:
            for node_name, node_data in report.get("nodes", {}).items():
                if not node_data.get("healthy"):
                    continue

                for ch in node_data.get("channels_detail", []):
                    conn.execute("""
                        INSERT INTO channel_history (
                            timestamp, node_name, channel_id, peer_id,
                            capacity_sats, local_sats, remote_sats, balance_ratio,
                            flow_state, flow_ratio, confidence, forward_count,
                            fee_ppm, fee_base_msat,
                            needs_inbound, needs_outbound, is_balanced
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        timestamp,
                        node_name,
                        ch.get("channel_id", ""),
                        ch.get("peer_id", ""),
                        ch.get("capacity_sats", 0),
                        ch.get("local_sats", 0),
                        ch.get("remote_sats", 0),
                        ch.get("balance_ratio", 0),
                        ch.get("flow_state", "unknown"),
                        ch.get("flow_ratio", 0),
                        ch.get("confidence", 0),
                        ch.get("forward_count", 0),
                        ch.get("fee_ppm", 0),
                        ch.get("fee_base_msat", 0),
                        1 if ch.get("needs_inbound") else 0,
                        1 if ch.get("needs_outbound") else 0,
                        1 if ch.get("is_balanced") else 0
                    ))
                    count += 1

            conn.commit()

        # Update velocity calculations
        self._update_channel_velocities()

        return count

    def _update_channel_velocities(self):
        """Recalculate channel velocities based on recent history."""
        # Use last 6 hours of data for velocity calculation
        cutoff = int((datetime.now() - timedelta(hours=6)).timestamp())

        with self._get_conn() as conn:
            # Get distinct channels with recent data
            channels = conn.execute("""
                SELECT DISTINCT node_name, channel_id
                FROM channel_history
                WHERE timestamp > ?
            """, (cutoff,)).fetchall()

            for row in channels:
                node_name, channel_id = row['node_name'], row['channel_id']

                # Get oldest and newest readings
                readings = conn.execute("""
                    SELECT timestamp, local_sats, balance_ratio, capacity_sats
                    FROM channel_history
                    WHERE node_name = ? AND channel_id = ?
                    AND timestamp > ?
                    ORDER BY timestamp
                """, (node_name, channel_id, cutoff)).fetchall()

                if len(readings) < 2:
                    continue

                oldest = readings[0]
                newest = readings[-1]

                time_diff_hours = (newest['timestamp'] - oldest['timestamp']) / 3600.0
                if time_diff_hours < 0.1:  # Less than 6 minutes
                    continue

                # Calculate velocity
                sats_change = newest['local_sats'] - oldest['local_sats']
                velocity_sats = sats_change / time_diff_hours

                ratio_change = newest['balance_ratio'] - oldest['balance_ratio']
                velocity_pct = (ratio_change * 100) / time_diff_hours

                # Determine trend
                if abs(velocity_pct) < 0.5:  # Less than 0.5% per hour
                    trend = "stable"
                elif velocity_sats < 0:
                    trend = "depleting"
                else:
                    trend = "filling"

                # Calculate time until depleted/full
                hours_depleted = None
                hours_full = None

                if trend == "depleting" and velocity_sats < 0:
                    hours_depleted = newest['local_sats'] / abs(velocity_sats)
                elif trend == "filling" and velocity_sats > 0:
                    remote = newest['capacity_sats'] - newest['local_sats']
                    hours_full = remote / velocity_sats

                # Confidence based on data points
                confidence = min(1.0, len(readings) / 10.0)

                # Upsert velocity record
                conn.execute("""
                    INSERT OR REPLACE INTO channel_velocity (
                        node_name, channel_id, updated_at,
                        current_local_sats, current_balance_ratio,
                        balance_velocity_sats_per_hour, balance_velocity_pct_per_hour,
                        hours_until_depleted, hours_until_full,
                        predicted_depletion_time,
                        trend, trend_confidence
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    node_name, channel_id,
                    int(datetime.now().timestamp()),
                    newest['local_sats'],
                    newest['balance_ratio'],
                    velocity_sats,
                    velocity_pct,
                    hours_depleted,
                    hours_full,
                    int(datetime.now().timestamp() + hours_depleted * 3600) if hours_depleted else None,
                    trend,
                    confidence
                ))

            conn.commit()

    # =========================================================================
    # Query Methods
    # =========================================================================

    def get_channel_velocity(self, node_name: str, channel_id: str) -> Optional[ChannelVelocity]:
        """Get velocity metrics for a specific channel."""
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT * FROM channel_velocity
                WHERE node_name = ? AND channel_id = ?
            """, (node_name, channel_id)).fetchone()

            if not row:
                return None

            return ChannelVelocity(
                node_name=row['node_name'],
                channel_id=row['channel_id'],
                current_local_sats=row['current_local_sats'],
                current_balance_ratio=row['current_balance_ratio'],
                velocity_sats_per_hour=row['balance_velocity_sats_per_hour'],
                velocity_pct_per_hour=row['balance_velocity_pct_per_hour'],
                hours_until_depleted=row['hours_until_depleted'],
                hours_until_full=row['hours_until_full'],
                trend=row['trend'],
                confidence=row['trend_confidence']
            )

    def get_critical_channels(self, hours_threshold: float = 24) -> List[ChannelVelocity]:
        """Get channels that will deplete or fill within threshold hours."""
        results = []

        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM channel_velocity
                WHERE (hours_until_depleted IS NOT NULL AND hours_until_depleted < ?)
                   OR (hours_until_full IS NOT NULL AND hours_until_full < ?)
                ORDER BY COALESCE(hours_until_depleted, hours_until_full)
            """, (hours_threshold, hours_threshold)).fetchall()

            for row in rows:
                results.append(ChannelVelocity(
                    node_name=row['node_name'],
                    channel_id=row['channel_id'],
                    current_local_sats=row['current_local_sats'],
                    current_balance_ratio=row['current_balance_ratio'],
                    velocity_sats_per_hour=row['balance_velocity_sats_per_hour'],
                    velocity_pct_per_hour=row['balance_velocity_pct_per_hour'],
                    hours_until_depleted=row['hours_until_depleted'],
                    hours_until_full=row['hours_until_full'],
                    trend=row['trend'],
                    confidence=row['trend_confidence']
                ))

        return results

    def get_channel_history(self, node_name: str, channel_id: str,
                            hours: int = 24) -> List[Dict]:
        """Get historical data for a channel."""
        cutoff = int((datetime.now() - timedelta(hours=hours)).timestamp())

        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM channel_history
                WHERE node_name = ? AND channel_id = ?
                AND timestamp > ?
                ORDER BY timestamp
            """, (node_name, channel_id, cutoff)).fetchall()

            return [dict(row) for row in rows]

    def get_fleet_trends(self, days: int = 7) -> Optional[FleetTrend]:
        """Get fleet-wide trends over specified period."""
        now = datetime.now()
        cutoff = int((now - timedelta(days=days)).timestamp())

        with self._get_conn() as conn:
            # Get oldest and newest snapshots in period
            oldest = conn.execute("""
                SELECT * FROM fleet_snapshots
                WHERE timestamp > ?
                ORDER BY timestamp ASC LIMIT 1
            """, (cutoff,)).fetchone()

            newest = conn.execute("""
                SELECT * FROM fleet_snapshots
                ORDER BY timestamp DESC LIMIT 1
            """).fetchone()

            if not oldest or not newest:
                return None

            # Calculate changes
            revenue_old = oldest['total_revenue_sats'] or 0
            revenue_new = newest['total_revenue_sats'] or 0
            revenue_change = ((revenue_new - revenue_old) / revenue_old * 100) if revenue_old > 0 else 0

            capacity_old = oldest['total_capacity_sats'] or 0
            capacity_new = newest['total_capacity_sats'] or 0
            capacity_change = ((capacity_new - capacity_old) / capacity_old * 100) if capacity_old > 0 else 0

            channel_change = (newest['total_channels'] or 0) - (oldest['total_channels'] or 0)

            # Determine health trend
            health_old = (oldest['nodes_healthy'] or 0) / max(oldest['total_nodes'] or 1, 1)
            health_new = (newest['nodes_healthy'] or 0) / max(newest['total_nodes'] or 1, 1)

            if health_new > health_old + 0.1:
                health_trend = "improving"
            elif health_new < health_old - 0.1:
                health_trend = "declining"
            else:
                health_trend = "stable"

            # Count depleting/filling channels
            velocity_stats = conn.execute("""
                SELECT
                    SUM(CASE WHEN trend = 'depleting' THEN 1 ELSE 0 END) as depleting,
                    SUM(CASE WHEN trend = 'filling' THEN 1 ELSE 0 END) as filling
                FROM channel_velocity
            """).fetchone()

            return FleetTrend(
                period_hours=days * 24,
                revenue_change_pct=round(revenue_change, 2),
                capacity_change_pct=round(capacity_change, 2),
                channel_count_change=channel_change,
                health_trend=health_trend,
                channels_depleting=velocity_stats['depleting'] or 0,
                channels_filling=velocity_stats['filling'] or 0
            )

    def get_recent_snapshots(self, limit: int = 24) -> List[Dict]:
        """Get recent fleet snapshots."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT
                    timestamp, snapshot_type,
                    total_nodes, nodes_healthy, total_channels,
                    total_capacity_sats, net_profit_sats,
                    channels_balanced, channels_needs_inbound, channels_needs_outbound
                FROM fleet_snapshots
                ORDER BY timestamp DESC
                LIMIT ?
            """, (limit,)).fetchall()

            return [dict(row) for row in rows]

    # =========================================================================
    # Decision Tracking
    # =========================================================================

    def record_decision(self, decision_type: str, node_name: str,
                        recommendation: str, reasoning: str = None,
                        channel_id: str = None, peer_id: str = None,
                        confidence: float = None) -> int:
        """Record an AI decision/recommendation."""
        with self._get_conn() as conn:
            cursor = conn.execute("""
                INSERT INTO ai_decisions (
                    timestamp, decision_type, node_name, channel_id, peer_id,
                    recommendation, reasoning, confidence, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'recommended')
            """, (
                int(datetime.now().timestamp()),
                decision_type,
                node_name,
                channel_id,
                peer_id,
                recommendation,
                reasoning,
                confidence
            ))
            conn.commit()
            return cursor.lastrowid

    def get_pending_decisions(self) -> List[Dict]:
        """Get decisions that haven't been acted upon."""
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM ai_decisions
                WHERE status = 'recommended'
                ORDER BY timestamp DESC
            """).fetchall()

            return [dict(row) for row in rows]

    # =========================================================================
    # Maintenance
    # =========================================================================

    def cleanup_old_data(self, days_to_keep: int = 30):
        """Remove old historical data to manage database size."""
        cutoff = int((datetime.now() - timedelta(days=days_to_keep)).timestamp())

        with self._get_conn() as conn:
            # Keep daily snapshots longer, remove hourly after cutoff
            conn.execute("""
                DELETE FROM fleet_snapshots
                WHERE snapshot_type = 'hourly' AND timestamp < ?
            """, (cutoff,))

            conn.execute("""
                DELETE FROM channel_history
                WHERE timestamp < ?
            """, (cutoff,))

            conn.commit()

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        with self._get_conn() as conn:
            stats = {}

            stats['fleet_snapshots'] = conn.execute(
                "SELECT COUNT(*) as count FROM fleet_snapshots"
            ).fetchone()['count']

            stats['channel_history_records'] = conn.execute(
                "SELECT COUNT(*) as count FROM channel_history"
            ).fetchone()['count']

            stats['channels_tracked'] = conn.execute(
                "SELECT COUNT(DISTINCT node_name || channel_id) as count FROM channel_history"
            ).fetchone()['count']

            stats['ai_decisions'] = conn.execute(
                "SELECT COUNT(*) as count FROM ai_decisions"
            ).fetchone()['count']

            oldest = conn.execute(
                "SELECT MIN(timestamp) as ts FROM fleet_snapshots"
            ).fetchone()['ts']
            stats['oldest_snapshot'] = datetime.fromtimestamp(oldest).isoformat() if oldest else None

            # Add new table stats
            stats['alerts_total'] = conn.execute(
                "SELECT COUNT(*) as count FROM alert_history"
            ).fetchone()['count']

            stats['alerts_unresolved'] = conn.execute(
                "SELECT COUNT(*) as count FROM alert_history WHERE resolved = 0"
            ).fetchone()['count']

            stats['peers_tracked'] = conn.execute(
                "SELECT COUNT(*) as count FROM peer_intelligence"
            ).fetchone()['count']

            return stats

    # =========================================================================
    # Alert Deduplication
    # =========================================================================

    def _make_alert_hash(self, alert_type: str, node_name: str,
                         channel_id: str = None) -> str:
        """Create unique hash for alert deduplication."""
        key = f"{alert_type}:{node_name}:{channel_id or 'none'}"
        return hashlib.md5(key.encode()).hexdigest()[:16]

    def check_alert(self, alert_type: str, node_name: str,
                    channel_id: str = None) -> AlertStatus:
        """Check if an alert should be raised or skipped (deduplication)."""
        alert_hash = self._make_alert_hash(alert_type, node_name, channel_id)
        now = datetime.now()
        now_ts = int(now.timestamp())

        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT * FROM alert_history
                WHERE alert_hash = ? AND resolved = 0
            """, (alert_hash,)).fetchone()

            if not row:
                # New alert
                return AlertStatus(
                    alert_type=alert_type,
                    node_name=node_name,
                    channel_id=channel_id,
                    is_new=True,
                    first_flagged=None,
                    last_flagged=None,
                    times_flagged=0,
                    hours_since_last=0,
                    action="flag",
                    message="New issue detected - flag it"
                )

            # Existing unresolved alert
            first_flagged = datetime.fromtimestamp(row['first_flagged'])
            last_flagged = datetime.fromtimestamp(row['last_flagged'])
            hours_since = (now - last_flagged).total_seconds() / 3600
            times_flagged = row['times_flagged']

            # Determine action based on time since last flag
            if hours_since < 24:
                action = "skip"
                message = f"Already flagged {hours_since:.1f}h ago - skip to reduce noise"
            elif hours_since < 72:
                action = "mention_unresolved"
                message = f"Flagged {times_flagged}x over {(now - first_flagged).days}d - still unresolved"
            else:
                action = "escalate"
                message = f"Unresolved for {(now - first_flagged).days}d - escalate"

            return AlertStatus(
                alert_type=alert_type,
                node_name=node_name,
                channel_id=channel_id,
                is_new=False,
                first_flagged=first_flagged,
                last_flagged=last_flagged,
                times_flagged=times_flagged,
                hours_since_last=hours_since,
                action=action,
                message=message
            )

    def record_alert(self, alert_type: str, node_name: str,
                     channel_id: str = None, peer_id: str = None,
                     severity: str = "warning", message: str = None) -> AlertStatus:
        """Record an alert (handles dedup automatically)."""
        alert_hash = self._make_alert_hash(alert_type, node_name, channel_id)
        now_ts = int(datetime.now().timestamp())

        with self._get_conn() as conn:
            # Try to update existing
            cursor = conn.execute("""
                UPDATE alert_history
                SET last_flagged = ?,
                    times_flagged = times_flagged + 1,
                    severity = ?,
                    message = ?
                WHERE alert_hash = ? AND resolved = 0
            """, (now_ts, severity, message, alert_hash))

            if cursor.rowcount == 0:
                # Insert new alert
                conn.execute("""
                    INSERT INTO alert_history (
                        alert_type, node_name, channel_id, peer_id, alert_hash,
                        first_flagged, last_flagged, times_flagged, severity, message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """, (alert_type, node_name, channel_id, peer_id, alert_hash,
                      now_ts, now_ts, severity, message))

            conn.commit()

        # Return current status
        return self.check_alert(alert_type, node_name, channel_id)

    def resolve_alert(self, alert_type: str, node_name: str,
                      channel_id: str = None,
                      resolution_action: str = None) -> bool:
        """Mark an alert as resolved."""
        alert_hash = self._make_alert_hash(alert_type, node_name, channel_id)
        now_ts = int(datetime.now().timestamp())

        with self._get_conn() as conn:
            cursor = conn.execute("""
                UPDATE alert_history
                SET resolved = 1,
                    resolved_at = ?,
                    resolution_action = ?
                WHERE alert_hash = ? AND resolved = 0
            """, (now_ts, resolution_action, alert_hash))
            conn.commit()
            return cursor.rowcount > 0

    def get_unresolved_alerts(self, hours: int = 72) -> List[Dict]:
        """Get unresolved alerts from the last N hours."""
        cutoff = int((datetime.now() - timedelta(hours=hours)).timestamp())

        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT * FROM alert_history
                WHERE resolved = 0 AND first_flagged > ?
                ORDER BY last_flagged DESC
            """, (cutoff,)).fetchall()

            results = []
            for row in rows:
                results.append({
                    "alert_type": row['alert_type'],
                    "node_name": row['node_name'],
                    "channel_id": row['channel_id'],
                    "peer_id": row['peer_id'],
                    "first_flagged": datetime.fromtimestamp(row['first_flagged']).isoformat(),
                    "last_flagged": datetime.fromtimestamp(row['last_flagged']).isoformat(),
                    "times_flagged": row['times_flagged'],
                    "severity": row['severity'],
                    "message": row['message'],
                    "days_open": (datetime.now() - datetime.fromtimestamp(row['first_flagged'])).days
                })

            return results

    # =========================================================================
    # Peer Intelligence
    # =========================================================================

    def update_peer_intelligence(self, peer_id: str, alias: str = None,
                                  channels_opened: int = None,
                                  channels_closed: int = None,
                                  force_closes: int = None,
                                  total_forwards: int = None,
                                  total_revenue_sats: int = None,
                                  total_costs_sats: int = None) -> None:
        """Update or create peer intelligence record."""
        now_ts = int(datetime.now().timestamp())

        with self._get_conn() as conn:
            # Check if exists
            existing = conn.execute(
                "SELECT * FROM peer_intelligence WHERE peer_id = ?",
                (peer_id,)
            ).fetchone()

            if existing:
                # Update existing - only update non-null values
                updates = []
                values = []

                updates.append("last_seen = ?")
                values.append(now_ts)

                if alias is not None:
                    updates.append("alias = ?")
                    values.append(alias)
                if channels_opened is not None:
                    updates.append("channels_opened = ?")
                    values.append(channels_opened)
                if channels_closed is not None:
                    updates.append("channels_closed = ?")
                    values.append(channels_closed)
                if force_closes is not None:
                    updates.append("force_closes = ?")
                    values.append(force_closes)
                if total_forwards is not None:
                    updates.append("total_forwards = ?")
                    values.append(total_forwards)
                if total_revenue_sats is not None:
                    updates.append("total_revenue_sats = ?")
                    values.append(total_revenue_sats)
                if total_costs_sats is not None:
                    updates.append("total_costs_sats = ?")
                    values.append(total_costs_sats)

                values.append(peer_id)
                conn.execute(f"""
                    UPDATE peer_intelligence SET {', '.join(updates)}
                    WHERE peer_id = ?
                """, values)
            else:
                # Insert new
                conn.execute("""
                    INSERT INTO peer_intelligence (
                        peer_id, alias, first_seen, last_seen,
                        channels_opened, channels_closed, force_closes,
                        total_forwards, total_revenue_sats, total_costs_sats
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (peer_id, alias, now_ts, now_ts,
                      channels_opened or 0, channels_closed or 0, force_closes or 0,
                      total_forwards or 0, total_revenue_sats or 0, total_costs_sats or 0))

            # Recalculate scores
            self._update_peer_scores(conn, peer_id)
            conn.commit()

    def _update_peer_scores(self, conn, peer_id: str) -> None:
        """Recalculate peer profitability and reliability scores."""
        row = conn.execute(
            "SELECT * FROM peer_intelligence WHERE peer_id = ?",
            (peer_id,)
        ).fetchone()

        if not row:
            return

        # Calculate reliability score (0-1)
        # Penalize force closes heavily
        total_closes = (row['channels_closed'] or 0) + (row['force_closes'] or 0)
        if total_closes == 0:
            reliability = 1.0
        else:
            force_ratio = (row['force_closes'] or 0) / total_closes
            reliability = max(0, 1.0 - (force_ratio * 2))  # Force closes hurt 2x

        # Calculate profitability score (net profit per channel opened)
        channels_opened = row['channels_opened'] or 1
        net_profit = (row['total_revenue_sats'] or 0) - (row['total_costs_sats'] or 0)
        profitability = net_profit / channels_opened

        # Determine recommendation
        if reliability >= 0.9 and profitability > 1000:
            recommendation = 'excellent'
        elif reliability >= 0.7 and profitability > 0:
            recommendation = 'good'
        elif reliability >= 0.5 and profitability >= -500:
            recommendation = 'neutral'
        elif reliability < 0.5 or (row['force_closes'] or 0) >= 2:
            recommendation = 'avoid'
        else:
            recommendation = 'caution'

        conn.execute("""
            UPDATE peer_intelligence
            SET profitability_score = ?,
                reliability_score = ?,
                recommendation = ?
            WHERE peer_id = ?
        """, (profitability, reliability, recommendation, peer_id))

    def get_peer_intelligence(self, peer_id: str) -> Optional[PeerIntelligence]:
        """Get peer intelligence record."""
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT * FROM peer_intelligence WHERE peer_id = ?",
                (peer_id,)
            ).fetchone()

            if not row:
                return None

            return PeerIntelligence(
                peer_id=row['peer_id'],
                alias=row['alias'],
                first_seen=datetime.fromtimestamp(row['first_seen']) if row['first_seen'] else None,
                last_seen=datetime.fromtimestamp(row['last_seen']) if row['last_seen'] else None,
                channels_opened=row['channels_opened'] or 0,
                channels_closed=row['channels_closed'] or 0,
                force_closes=row['force_closes'] or 0,
                avg_channel_lifetime_days=row['avg_channel_lifetime_days'],
                total_forwards=row['total_forwards'] or 0,
                total_revenue_sats=row['total_revenue_sats'] or 0,
                total_costs_sats=row['total_costs_sats'] or 0,
                profitability_score=row['profitability_score'],
                reliability_score=row['reliability_score'],
                recommendation=row['recommendation'] or 'unknown'
            )

    def get_all_peer_intelligence(self) -> List[PeerIntelligence]:
        """Get all peer intelligence records."""
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM peer_intelligence ORDER BY profitability_score DESC"
            ).fetchall()

            return [PeerIntelligence(
                peer_id=row['peer_id'],
                alias=row['alias'],
                first_seen=datetime.fromtimestamp(row['first_seen']) if row['first_seen'] else None,
                last_seen=datetime.fromtimestamp(row['last_seen']) if row['last_seen'] else None,
                channels_opened=row['channels_opened'] or 0,
                channels_closed=row['channels_closed'] or 0,
                force_closes=row['force_closes'] or 0,
                avg_channel_lifetime_days=row['avg_channel_lifetime_days'],
                total_forwards=row['total_forwards'] or 0,
                total_revenue_sats=row['total_revenue_sats'] or 0,
                total_costs_sats=row['total_costs_sats'] or 0,
                profitability_score=row['profitability_score'],
                reliability_score=row['reliability_score'],
                recommendation=row['recommendation'] or 'unknown'
            ) for row in rows]

    # =========================================================================
    # Context Brief
    # =========================================================================

    def get_context_brief(self, days: int = 7) -> ContextBrief:
        """Get pre-run context summary for the AI advisor."""
        now = datetime.now()
        cutoff = int((now - timedelta(days=days)).timestamp())
        prev_cutoff = int((now - timedelta(days=days * 2)).timestamp())

        with self._get_conn() as conn:
            # Current period stats
            current = conn.execute("""
                SELECT
                    MAX(total_capacity_sats) as capacity,
                    MAX(total_channels) as channels,
                    SUM(CASE WHEN total_revenue_sats IS NOT NULL THEN total_revenue_sats ELSE 0 END) as revenue
                FROM fleet_snapshots
                WHERE timestamp > ?
            """, (cutoff,)).fetchone()

            # Previous period stats for comparison
            previous = conn.execute("""
                SELECT
                    MAX(total_capacity_sats) as capacity,
                    MAX(total_channels) as channels,
                    SUM(CASE WHEN total_revenue_sats IS NOT NULL THEN total_revenue_sats ELSE 0 END) as revenue
                FROM fleet_snapshots
                WHERE timestamp > ? AND timestamp <= ?
            """, (prev_cutoff, cutoff)).fetchone()

            # Calculate changes
            curr_capacity = current['capacity'] or 0
            prev_capacity = previous['capacity'] or 0
            capacity_change = ((curr_capacity - prev_capacity) / prev_capacity * 100) if prev_capacity > 0 else 0

            curr_channels = current['channels'] or 0
            prev_channels = previous['channels'] or 0
            channel_change = curr_channels - prev_channels

            curr_revenue = current['revenue'] or 0
            prev_revenue = previous['revenue'] or 0
            revenue_change = ((curr_revenue - prev_revenue) / prev_revenue * 100) if prev_revenue > 0 else 0

            # Velocity alerts
            velocity_stats = conn.execute("""
                SELECT
                    SUM(CASE WHEN trend = 'depleting' THEN 1 ELSE 0 END) as depleting,
                    SUM(CASE WHEN trend = 'filling' THEN 1 ELSE 0 END) as filling
                FROM channel_velocity
            """).fetchone()

            critical_channels = conn.execute("""
                SELECT channel_id FROM channel_velocity
                WHERE (hours_until_depleted IS NOT NULL AND hours_until_depleted < 24)
                   OR (hours_until_full IS NOT NULL AND hours_until_full < 24)
            """).fetchall()

            # Unresolved alerts
            unresolved = self.get_unresolved_alerts(hours=days * 24)

            # Recent decisions
            decisions = conn.execute("""
                SELECT decision_type, COUNT(*) as count
                FROM ai_decisions
                WHERE timestamp > ?
                GROUP BY decision_type
            """, (cutoff,)).fetchall()

            decisions_by_type = {row['decision_type']: row['count'] for row in decisions}
            total_decisions = sum(decisions_by_type.values())

            # Build summary text
            summary_parts = []
            summary_parts.append(f"Period: last {days} days")

            if capacity_change != 0:
                direction = "up" if capacity_change > 0 else "down"
                summary_parts.append(f"Capacity: {curr_capacity:,} sats ({direction} {abs(capacity_change):.1f}%)")
            else:
                summary_parts.append(f"Capacity: {curr_capacity:,} sats (unchanged)")

            if revenue_change != 0:
                direction = "up" if revenue_change > 0 else "down"
                summary_parts.append(f"Revenue: {curr_revenue:,} sats ({direction} {abs(revenue_change):.1f}%)")

            depleting = velocity_stats['depleting'] or 0
            filling = velocity_stats['filling'] or 0
            if depleting > 0 or filling > 0:
                summary_parts.append(f"Velocity alerts: {depleting} depleting, {filling} filling")

            if unresolved:
                summary_parts.append(f"Unresolved flags: {len(unresolved)}")

            if total_decisions > 0:
                summary_parts.append(f"Decisions made: {total_decisions}")

            return ContextBrief(
                period_days=days,
                total_capacity_sats=curr_capacity,
                capacity_change_pct=round(capacity_change, 2),
                total_channels=curr_channels,
                channel_count_change=channel_change,
                period_revenue_sats=curr_revenue,
                revenue_change_pct=round(revenue_change, 2),
                channels_depleting=depleting,
                channels_filling=filling,
                critical_velocity_channels=[row['channel_id'] for row in critical_channels],
                unresolved_alerts=unresolved,
                recent_decisions_count=total_decisions,
                decisions_by_type=decisions_by_type,
                summary_text=" | ".join(summary_parts)
            )

    # =========================================================================
    # Outcome Measurement
    # =========================================================================

    def measure_decision_outcomes(self, min_hours: int = 24,
                                   max_hours: int = 72) -> List[Dict]:
        """Measure outcomes for decisions made between min and max hours ago."""
        now = datetime.now()
        min_cutoff = int((now - timedelta(hours=max_hours)).timestamp())
        max_cutoff = int((now - timedelta(hours=min_hours)).timestamp())

        measured = []

        with self._get_conn() as conn:
            # Get decisions that need outcome measurement
            decisions = conn.execute("""
                SELECT * FROM ai_decisions
                WHERE timestamp > ? AND timestamp < ?
                AND outcome_measured_at IS NULL
                AND status IN ('recommended', 'approved', 'executed')
            """, (min_cutoff, max_cutoff)).fetchall()

            for decision in decisions:
                outcome = self._measure_single_outcome(conn, decision)
                if outcome:
                    measured.append(outcome)

            conn.commit()

        return measured

    def _measure_single_outcome(self, conn, decision) -> Optional[Dict]:
        """Measure outcome for a single decision."""
        decision_type = decision['decision_type']
        node_name = decision['node_name']
        channel_id = decision['channel_id']
        decision_time = decision['timestamp']
        now_ts = int(datetime.now().timestamp())

        # Get snapshot at decision time
        snapshot_before = None
        if decision['snapshot_metrics']:
            try:
                snapshot_before = json.loads(decision['snapshot_metrics'])
            except json.JSONDecodeError:
                pass

        # For channel-related decisions, compare channel state
        if channel_id and decision_type in ('flag_channel', 'approve', 'reject'):
            # Get current channel state
            current = conn.execute("""
                SELECT * FROM channel_history
                WHERE node_name = ? AND channel_id = ?
                ORDER BY timestamp DESC LIMIT 1
            """, (node_name, channel_id)).fetchone()

            if not current:
                # Channel may have been closed - that's an outcome
                outcome_success = 0  # Neutral - can't measure
                outcome_metrics = {"note": "Channel no longer exists"}
            else:
                # Compare forward count, balance ratio
                outcome_metrics = {
                    "current_balance_ratio": current['balance_ratio'],
                    "current_flow_state": current['flow_state']
                }

                if snapshot_before:
                    old_ratio = snapshot_before.get('balance_ratio', 0.5)
                    new_ratio = current['balance_ratio'] or 0.5

                    # Did balance improve toward 0.5?
                    old_dist = abs(old_ratio - 0.5)
                    new_dist = abs(new_ratio - 0.5)

                    if new_dist < old_dist - 0.05:
                        outcome_success = 1  # Improved
                    elif new_dist > old_dist + 0.05:
                        outcome_success = -1  # Worsened
                    else:
                        outcome_success = 0  # Unchanged
                else:
                    outcome_success = 0  # Can't compare

        else:
            # Generic outcome - just mark as measured
            outcome_success = 0
            outcome_metrics = {"note": "No specific metrics for this decision type"}

        # Update decision record
        conn.execute("""
            UPDATE ai_decisions
            SET outcome_measured_at = ?,
                outcome_success = ?,
                outcome_metrics = ?
            WHERE id = ?
        """, (now_ts, outcome_success, json.dumps(outcome_metrics), decision['id']))

        return {
            "decision_id": decision['id'],
            "decision_type": decision_type,
            "node_name": node_name,
            "channel_id": channel_id,
            "outcome_success": outcome_success,
            "outcome_metrics": outcome_metrics
        }
