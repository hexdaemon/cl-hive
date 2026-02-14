#!/usr/bin/env python3
"""
Hive Fleet Monitor - Real-time monitoring and daily reports

This daemon monitors Lightning nodes running cl-hive and cl-revenue-ops,
providing:
- Real-time alerts for pending actions, health issues, and events
- Daily financial and operational reports
- Continuous status tracking

Usage:
    # Start real-time monitor (daemon mode)
    ./hive-monitor.py --config nodes.json monitor

    # Generate daily report
    ./hive-monitor.py --config nodes.json report --output report.json

    # Run with cron for daily reports (add to crontab):
    # 0 9 * * * /path/to/hive-monitor.py --config /path/to/nodes.json report --output /path/to/reports/$(date +%%Y-%%m-%%d).json

Environment:
    HIVE_NODES_CONFIG - Path to nodes.json (alternative to --config)
    HIVE_MONITOR_INTERVAL - Polling interval in seconds (default: 60)
    HIVE_REPORTS_DIR - Directory for daily reports (default: ./reports)
"""

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import signal

# Local imports
try:
    from advisor_db import AdvisorDB
except ImportError:
    # Allow running without database
    AdvisorDB = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("hive-monitor")


# =============================================================================
# Node Connection (simplified from MCP server)
# =============================================================================

@dataclass
class NodeConnection:
    """Connection to a CLN node via docker exec (for Polar)."""
    name: str
    docker_container: str
    lightning_dir: str = "/home/clightning/.lightning"
    network: str = "regtest"

    def call(self, method: str, params: Dict = None) -> Dict:
        """Call CLN via docker exec."""
        cmd = [
            "docker", "exec", self.docker_container,
            "lightning-cli",
            f"--lightning-dir={self.lightning_dir}",
            f"--network={self.network}",
            method
        ]

        if params:
            for key, value in params.items():
                if isinstance(value, bool):
                    cmd.append(f"{key}={'true' if value else 'false'}")
                elif isinstance(value, (int, float)):
                    cmd.append(f"{key}={value}")
                elif isinstance(value, str):
                    cmd.append(f"{key}={value}")
                else:
                    cmd.append(f"{key}={json.dumps(value)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            if result.returncode != 0:
                return {"error": result.stderr.strip()}
            return json.loads(result.stdout) if result.stdout.strip() else {}
        except subprocess.TimeoutExpired:
            return {"error": "Command timed out"}
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON response: {e}"}
        except Exception as e:
            return {"error": str(e)}


def load_nodes(config_path: str) -> Dict[str, NodeConnection]:
    """Load node configuration."""
    with open(config_path) as f:
        config = json.load(f)

    nodes = {}
    network = config.get("network", "regtest")
    lightning_dir = config.get("lightning_dir", "/home/clightning/.lightning")

    for node_config in config.get("nodes", []):
        node = NodeConnection(
            name=node_config["name"],
            docker_container=node_config["docker_container"],
            lightning_dir=lightning_dir,
            network=network
        )
        nodes[node.name] = node

    return nodes


# =============================================================================
# State Tracking
# =============================================================================

@dataclass
class NodeState:
    """Tracked state for a node."""
    name: str
    last_check: datetime = None
    pending_action_ids: Set[int] = field(default_factory=set)
    governance_mode: str = ""
    channel_count: int = 0
    total_capacity_sats: int = 0
    onchain_sats: int = 0
    # Revenue ops state
    daily_revenue_sats: int = 0
    daily_costs_sats: int = 0
    active_rebalances: int = 0
    # Health
    is_healthy: bool = True
    last_error: str = ""


@dataclass
class Alert:
    """An alert to be reported."""
    timestamp: datetime
    node: str
    alert_type: str  # pending_action, health_issue, fee_change, rebalance, etc.
    severity: str    # info, warning, critical
    message: str
    details: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        return d


class FleetMonitor:
    """Monitors a fleet of Hive nodes."""

    MAX_ALERTS = 1000

    def __init__(self, nodes: Dict[str, NodeConnection], db_path: str = None):
        self.nodes = nodes
        self.state: Dict[str, NodeState] = {}
        self.alerts: List[Alert] = []
        self.report_data: Dict[str, Any] = {}

        # Initialize advisor database for historical tracking
        self.db = None
        if AdvisorDB is not None:
            try:
                self.db = AdvisorDB(db_path)
                logger.info(f"Advisor database initialized at {self.db.db_path}")
            except Exception as e:
                logger.warning(f"Could not initialize advisor database: {e}")

        # Initialize state for each node
        for name in nodes:
            self.state[name] = NodeState(name=name)

    def add_alert(self, node: str, alert_type: str, severity: str,
                  message: str, details: Dict = None):
        """Add an alert."""
        alert = Alert(
            timestamp=datetime.now(),
            node=node,
            alert_type=alert_type,
            severity=severity,
            message=message,
            details=details or {}
        )
        self.alerts.append(alert)
        if len(self.alerts) > self.MAX_ALERTS:
            self.alerts = self.alerts[-self.MAX_ALERTS:]

        # Log based on severity
        log_msg = f"[{node}] {message}"
        if severity == "critical":
            logger.critical(log_msg)
        elif severity == "warning":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

    def check_node(self, name: str) -> Dict[str, Any]:
        """Check a single node's status."""
        node = self.nodes[name]
        state = self.state[name]
        state.last_check = datetime.now()

        result = {
            "name": name,
            "timestamp": state.last_check.isoformat(),
            "hive": {},
            "revenue_ops": {},
            "errors": []
        }

        # Check hive status
        hive_status = node.call("hive-status")
        if "error" in hive_status:
            result["errors"].append(f"hive-status: {hive_status['error']}")
            state.is_healthy = False
            state.last_error = hive_status['error']
        else:
            state.is_healthy = True
            state.governance_mode = hive_status.get("governance_mode", "unknown")
            result["hive"]["status"] = hive_status

        # Check pending actions
        pending = node.call("hive-pending-actions", {"status": "pending"})
        if "error" not in pending:
            actions = pending.get("actions", [])
            current_ids = {a.get("id") for a in actions}

            # Alert on new pending actions
            new_ids = current_ids - state.pending_action_ids
            for action in actions:
                if action.get("id") in new_ids:
                    self.add_alert(
                        node=name,
                        alert_type="pending_action",
                        severity="warning",
                        message=f"New pending action: {action.get('action_type')} (ID: {action.get('id')})",
                        details=action
                    )

            state.pending_action_ids = current_ids
            result["hive"]["pending_actions"] = len(actions)

        # Check revenue-ops status
        rev_status = node.call("revenue-status")
        if "error" not in rev_status:
            result["revenue_ops"]["status"] = rev_status

        # Check revenue-ops dashboard
        dashboard = node.call("revenue-dashboard", {"window_days": 1})
        if "error" not in dashboard:
            result["revenue_ops"]["dashboard_1d"] = dashboard

        # Get channel info
        funds = node.call("listfunds")
        if "error" not in funds:
            channels = funds.get("channels", [])
            state.channel_count = len(channels)
            state.total_capacity_sats = sum(
                c.get("amount_msat", 0) // 1000 for c in channels
            )
            outputs = funds.get("outputs", [])
            state.onchain_sats = sum(
                o.get("amount_msat", 0) // 1000
                for o in outputs if o.get("status") == "confirmed"
            )
            result["funds"] = {
                "channel_count": state.channel_count,
                "total_capacity_sats": state.total_capacity_sats,
                "onchain_sats": state.onchain_sats
            }

        return result

    def check_all_nodes(self) -> Dict[str, Any]:
        """Check all nodes in the fleet."""
        results = {}
        for name in self.nodes:
            try:
                results[name] = self.check_node(name)
            except Exception as e:
                logger.error(f"Error checking {name}: {e}")
                results[name] = {"error": str(e)}
        return results

    def _get_hive_topology(self) -> Dict[str, Any]:
        """Get hive membership and topology from a healthy node."""
        topology = {
            "members": [],
            "member_count": 0,
            "admin_count": 0,
            "member_tier_count": 0,
            "neophyte_count": 0
        }

        # Find a healthy node to query
        for name, node in self.nodes.items():
            if self.state[name].is_healthy:
                members = node.call("hive-members")
                if "error" not in members:
                    topology["members"] = members.get("members", [])
                    topology["member_count"] = len(topology["members"])
                    for m in topology["members"]:
                        tier = m.get("tier", "").lower()
                        if tier == "admin":
                            topology["admin_count"] += 1
                        elif tier == "member":
                            topology["member_tier_count"] += 1
                        elif tier == "neophyte":
                            topology["neophyte_count"] += 1
                    break

        return topology

    def _get_channel_details(self, node: NodeConnection) -> List[Dict]:
        """Get detailed channel info including fees, balances, and flow state."""
        channels = []

        # Get peer channels with balances
        peer_channels = node.call("listpeerchannels")
        if "error" in peer_channels:
            return channels

        # Get revenue-ops flow state if available
        rev_status = node.call("revenue-status")
        flow_states = {}
        if "error" not in rev_status:
            for ch in rev_status.get("channel_states", []):
                flow_states[ch.get("channel_id")] = ch

        for ch in peer_channels.get("channels", []):
            if ch.get("state") != "CHANNELD_NORMAL":
                continue

            scid = ch.get("short_channel_id", "")
            total_msat = ch.get("total_msat", 0)
            our_msat = ch.get("to_us_msat", 0)

            # Calculate balance ratio
            total_sats = total_msat // 1000 if isinstance(total_msat, int) else int(total_msat.replace("msat", "")) // 1000
            our_sats = our_msat // 1000 if isinstance(our_msat, int) else int(our_msat.replace("msat", "")) // 1000
            balance_ratio = our_sats / total_sats if total_sats > 0 else 0.5

            # Get flow state
            flow = flow_states.get(scid, {})

            channel_info = {
                "channel_id": scid,
                "peer_id": ch.get("peer_id", ""),
                "capacity_sats": total_sats,
                "local_sats": our_sats,
                "remote_sats": total_sats - our_sats,
                "balance_ratio": round(balance_ratio, 3),
                # Fee info
                "fee_base_msat": ch.get("updates", {}).get("local", {}).get("fee_base_msat", 0),
                "fee_ppm": ch.get("updates", {}).get("local", {}).get("fee_proportional_millionths", 0),
                # Flow state from revenue-ops
                "flow_state": flow.get("state", "unknown"),
                "flow_ratio": round(flow.get("flow_ratio", 0), 3),
                "confidence": round(flow.get("confidence", 0), 2),
                "forward_count": flow.get("forward_count", 0),
                # Health indicators
                "needs_inbound": balance_ratio > 0.8,
                "needs_outbound": balance_ratio < 0.2,
                "is_balanced": 0.35 <= balance_ratio <= 0.65
            }
            channels.append(channel_info)

        return channels

    def _analyze_rebalance_opportunities(self, channels: List[Dict]) -> List[Dict]:
        """Identify channels that need rebalancing."""
        opportunities = []

        sources = [c for c in channels if c.get("needs_outbound") and c["local_sats"] > 50000]
        sinks = [c for c in channels if c.get("needs_inbound") and c["remote_sats"] > 50000]

        for sink in sinks:
            for source in sources:
                if sink["peer_id"] != source["peer_id"]:
                    amount = min(
                        source["local_sats"] - 50000,  # Leave some buffer
                        sink["remote_sats"] - 50000,
                        500000  # Cap at 500k sats
                    )
                    if amount > 10000:
                        opportunities.append({
                            "from_channel": source["channel_id"],
                            "to_channel": sink["channel_id"],
                            "amount_sats": amount,
                            "reason": f"Rebalance from depleted ({source['balance_ratio']:.0%}) to full ({sink['balance_ratio']:.0%})"
                        })

        # Sort by amount descending
        opportunities.sort(key=lambda x: x["amount_sats"], reverse=True)
        return opportunities[:5]  # Top 5

    def _generate_ai_recommendations(self, report: Dict) -> List[Dict]:
        """Generate intelligent recommendations based on report data."""
        recommendations = []

        # Check for critical velocity channels (from database)
        if self.db:
            try:
                critical_channels = self.db.get_critical_channels(hours_threshold=12)
                for v in critical_channels[:3]:  # Top 3 most urgent
                    if v.trend == "depleting":
                        recommendations.append({
                            "type": "velocity_alert",
                            "message": f"{v.node_name}: Channel {v.channel_id} depleting - {v.hours_until_depleted:.1f} hours until empty",
                            "priority": "critical" if v.urgency == "critical" else "high",
                            "action": f"Rebalance inbound to {v.channel_id} immediately",
                            "velocity": f"{v.velocity_sats_per_hour:,.0f} sats/hour outflow"
                        })
                    elif v.trend == "filling":
                        recommendations.append({
                            "type": "velocity_alert",
                            "message": f"{v.node_name}: Channel {v.channel_id} filling - {v.hours_until_full:.1f} hours until full",
                            "priority": "high" if v.urgency in ("critical", "high") else "warning",
                            "action": f"Rebalance outbound from {v.channel_id}",
                            "velocity": f"{v.velocity_sats_per_hour:,.0f} sats/hour inflow"
                        })
            except Exception as e:
                logger.warning(f"Error checking velocity alerts: {e}")

        # Check for pending actions
        total_pending = report["fleet_summary"].get("total_pending_actions", 0)
        if total_pending > 0:
            recommendations.append({
                "type": "action_required",
                "message": f"{total_pending} pending actions need review",
                "priority": "high"
            })

        # Check for unhealthy nodes
        unhealthy = report["fleet_summary"].get("nodes_unhealthy", 0)
        if unhealthy > 0:
            recommendations.append({
                "type": "health_check",
                "message": f"{unhealthy} node(s) not running cl-hive - consider installing plugin",
                "priority": "critical"
            })

        # Analyze fleet-wide metrics
        for name, node_data in report.get("nodes", {}).items():
            if not node_data.get("healthy"):
                continue

            # Check for bleeder channels
            dashboard = node_data.get("dashboard_30d", {})
            if dashboard.get("bleeder_count", 0) > 0:
                recommendations.append({
                    "type": "bleeding_channel",
                    "message": f"{name}: {dashboard['bleeder_count']} channel(s) losing money on rebalancing",
                    "priority": "warning",
                    "action": f"Review rebalance policy on {name}"
                })

            # Check profitability
            profit = node_data.get("profitability", {}).get("summary", {})
            if profit.get("underwater_count", 0) > profit.get("profitable_count", 0):
                recommendations.append({
                    "type": "profitability",
                    "message": f"{name}: More underwater channels ({profit['underwater_count']}) than profitable ({profit['profitable_count']})",
                    "priority": "warning",
                    "action": "Consider fee adjustments or closing unprofitable channels"
                })

            # Check for rebalance opportunities
            rebalance_ops = node_data.get("rebalance_opportunities", [])
            if rebalance_ops:
                top_op = rebalance_ops[0]
                recommendations.append({
                    "type": "rebalance_opportunity",
                    "message": f"{name}: Can rebalance {top_op['amount_sats']:,} sats from {top_op['from_channel']} to {top_op['to_channel']}",
                    "priority": "info",
                    "action": f"revenue-rebalance from_channel={top_op['from_channel']} to_channel={top_op['to_channel']} amount_sats={top_op['amount_sats']}"
                })

            # Check for fee optimization opportunities
            channels = node_data.get("channels_detail", [])
            for ch in channels:
                # High flow but low fee
                if ch.get("flow_ratio", 0) > 0.3 and ch.get("fee_ppm", 0) < 200:
                    recommendations.append({
                        "type": "fee_optimization",
                        "message": f"{name}: Channel {ch['channel_id']} has high flow ({ch['flow_ratio']:.0%}) but low fee ({ch['fee_ppm']} ppm)",
                        "priority": "info",
                        "action": f"Consider raising fee on {ch['channel_id']}"
                    })
                    break  # Only one fee rec per node

        return recommendations

    def generate_daily_report(self) -> Dict[str, Any]:
        """Generate a comprehensive daily report with AI decision support."""
        report = {
            "generated_at": datetime.now().isoformat(),
            "report_type": "daily",
            "fleet_summary": {},
            "hive_topology": {},
            "nodes": {},
            "alerts_24h": [],
            "recommendations": []
        }

        total_capacity = 0
        total_onchain = 0
        total_channels = 0
        total_pending_actions = 0
        nodes_healthy = 0
        nodes_unhealthy = 0
        fleet_channels = []

        # Get hive topology first
        report["hive_topology"] = self._get_hive_topology()

        for name, node in self.nodes.items():
            state = self.state[name]

            # Get detailed info for each node
            node_report = {
                "name": name,
                "healthy": state.is_healthy,
                "governance_mode": state.governance_mode,
                "channels": state.channel_count,
                "capacity_sats": state.total_capacity_sats,
                "onchain_sats": state.onchain_sats,
                "pending_actions": len(state.pending_action_ids),
            }

            if state.is_healthy:
                # Get detailed channel info with fees and balances
                channel_details = self._get_channel_details(node)
                node_report["channels_detail"] = channel_details
                fleet_channels.extend([(name, c) for c in channel_details])

                # Analyze rebalance opportunities
                node_report["rebalance_opportunities"] = self._analyze_rebalance_opportunities(channel_details)

                # Get profitability summary
                profitability = node.call("revenue-profitability")
                if "error" not in profitability:
                    node_report["profitability"] = profitability

                # Get 30-day dashboard
                dashboard = node.call("revenue-dashboard", {"window_days": 30})
                if "error" not in dashboard:
                    node_report["dashboard_30d"] = dashboard

                # Get history
                history = node.call("revenue-history")
                if "error" not in history:
                    node_report["lifetime_history"] = history

                # Get planner insights
                planner_log = node.call("hive-planner-log", {"limit": 5})
                if "error" not in planner_log:
                    node_report["recent_planner_decisions"] = planner_log.get("entries", [])

                # Get topology view
                topology = node.call("hive-topology")
                if "error" not in topology:
                    node_report["topology"] = topology

            report["nodes"][name] = node_report

            # Aggregate stats
            total_capacity += state.total_capacity_sats
            total_onchain += state.onchain_sats
            total_channels += state.channel_count
            total_pending_actions += len(state.pending_action_ids)
            if state.is_healthy:
                nodes_healthy += 1
            else:
                nodes_unhealthy += 1

        # Fleet summary
        report["fleet_summary"] = {
            "total_nodes": len(self.nodes),
            "nodes_healthy": nodes_healthy,
            "nodes_unhealthy": nodes_unhealthy,
            "total_channels": total_channels,
            "total_capacity_sats": total_capacity,
            "total_capacity_btc": total_capacity / 100_000_000,
            "total_onchain_sats": total_onchain,
            "total_pending_actions": total_pending_actions
        }

        # Fleet-wide channel analysis
        balanced_count = sum(1 for _, c in fleet_channels if c.get("is_balanced"))
        needs_inbound = sum(1 for _, c in fleet_channels if c.get("needs_inbound"))
        needs_outbound = sum(1 for _, c in fleet_channels if c.get("needs_outbound"))

        report["fleet_summary"]["channel_health"] = {
            "balanced": balanced_count,
            "needs_inbound": needs_inbound,
            "needs_outbound": needs_outbound,
            "total_analyzed": len(fleet_channels)
        }

        # Recent alerts (last 24 hours)
        cutoff = datetime.now() - timedelta(hours=24)
        report["alerts_24h"] = [
            a.to_dict() for a in self.alerts
            if a.timestamp > cutoff
        ]

        # Generate AI recommendations
        report["recommendations"] = self._generate_ai_recommendations(report)

        # Record to historical database if available
        if self.db:
            try:
                self.db.record_fleet_snapshot(report, snapshot_type="daily")
                self.db.record_channel_states(report)
                logger.info("Recorded snapshot to advisor database")

                # Add velocity and trend data to report
                report["velocity_analysis"] = self._get_velocity_analysis()
                report["fleet_trends"] = self._get_fleet_trends()
            except Exception as e:
                logger.warning(f"Failed to record to database: {e}")

        self.report_data = report
        return report

    def _get_velocity_analysis(self) -> Dict[str, Any]:
        """Get channel velocity analysis from database."""
        if not self.db:
            return {}

        try:
            critical = self.db.get_critical_channels(hours_threshold=24)
            return {
                "critical_channels": [
                    {
                        "node": v.node_name,
                        "channel_id": v.channel_id,
                        "trend": v.trend,
                        "velocity_sats_per_hour": round(v.velocity_sats_per_hour, 0),
                        "hours_until_action": round(v.hours_until_depleted or v.hours_until_full or 0, 1),
                        "urgency": v.urgency,
                        "current_balance_pct": round(v.current_balance_ratio * 100, 1)
                    }
                    for v in critical
                ],
                "critical_count": len(critical),
                "channels_depleting": len([v for v in critical if v.trend == "depleting"]),
                "channels_filling": len([v for v in critical if v.trend == "filling"])
            }
        except Exception as e:
            logger.warning(f"Error getting velocity analysis: {e}")
            return {}

    def _get_fleet_trends(self) -> Dict[str, Any]:
        """Get fleet trends from database."""
        if not self.db:
            return {}

        try:
            trends = self.db.get_fleet_trends(days=7)
            if not trends:
                return {"message": "Insufficient historical data for trend analysis"}

            return {
                "period_days": 7,
                "revenue_change_pct": trends.revenue_change_pct,
                "capacity_change_pct": trends.capacity_change_pct,
                "channel_count_change": trends.channel_count_change,
                "health_trend": trends.health_trend,
                "channels_depleting": trends.channels_depleting,
                "channels_filling": trends.channels_filling
            }
        except Exception as e:
            logger.warning(f"Error getting fleet trends: {e}")
            return {}


# =============================================================================
# Monitor Daemon
# =============================================================================

class MonitorDaemon:
    """Background monitor that runs continuously."""

    def __init__(self, monitor: FleetMonitor, interval: int = 60):
        self.monitor = monitor
        self.interval = interval
        self.running = False
        self.reports_dir = Path(os.environ.get("HIVE_REPORTS_DIR", "./reports"))
        self.last_hourly_snapshot = None

    async def run(self):
        """Main monitoring loop."""
        self.running = True
        logger.info(f"Starting monitor daemon (interval: {self.interval}s)")
        logger.info(f"Monitoring {len(self.monitor.nodes)} nodes")

        # Initial check
        self.monitor.check_all_nodes()

        last_daily_report = None

        while self.running:
            try:
                # Regular status check
                self.monitor.check_all_nodes()

                now = datetime.now()

                # Record hourly snapshot to database
                if self.monitor.db:
                    if self.last_hourly_snapshot is None or \
                       (now - self.last_hourly_snapshot).total_seconds() >= 3600:
                        self._record_hourly_snapshot()
                        self.last_hourly_snapshot = now

                # Generate daily report at midnight or on first run
                if last_daily_report is None or now.date() > last_daily_report.date():
                    self._save_daily_report()
                    last_daily_report = now

                await asyncio.sleep(self.interval)

            except asyncio.CancelledError:
                logger.info("Monitor daemon cancelled")
                break
            except Exception as e:
                logger.error(f"Monitor error: {e}")
                await asyncio.sleep(self.interval)

    def stop(self):
        """Stop the daemon."""
        self.running = False

    def _record_hourly_snapshot(self):
        """Record hourly snapshot to advisor database."""
        try:
            # Generate a quick report for the snapshot
            report = self.monitor.generate_daily_report()
            self.monitor.db.record_fleet_snapshot(report, snapshot_type="hourly")
            self.monitor.db.record_channel_states(report)
            logger.info("Recorded hourly snapshot to advisor database")
        except Exception as e:
            logger.warning(f"Failed to record hourly snapshot: {e}")

    def _save_daily_report(self):
        """Generate and save daily report."""
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        report = self.monitor.generate_daily_report()
        filename = self.reports_dir / f"{datetime.now().strftime('%Y-%m-%d')}.json"

        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)

        logger.info(f"Daily report saved to {filename}")


# =============================================================================
# CLI
# =============================================================================

def cmd_monitor(args):
    """Run the monitor daemon."""
    config_path = args.config or os.environ.get("HIVE_NODES_CONFIG")
    if not config_path:
        logger.error("No config file specified. Use --config or set HIVE_NODES_CONFIG")
        sys.exit(1)

    nodes = load_nodes(config_path)
    if not nodes:
        logger.error("No nodes configured")
        sys.exit(1)

    monitor = FleetMonitor(nodes)
    daemon = MonitorDaemon(
        monitor,
        interval=args.interval or int(os.environ.get("HIVE_MONITOR_INTERVAL", 60))
    )

    # Handle shutdown gracefully
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def shutdown(signum, frame):
        logger.info("Shutdown signal received")
        daemon.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_until_complete(daemon.run())
    finally:
        loop.close()


def cmd_report(args):
    """Generate a report."""
    config_path = args.config or os.environ.get("HIVE_NODES_CONFIG")
    if not config_path:
        logger.error("No config file specified. Use --config or set HIVE_NODES_CONFIG")
        sys.exit(1)

    nodes = load_nodes(config_path)
    if not nodes:
        logger.error("No nodes configured")
        sys.exit(1)

    monitor = FleetMonitor(nodes)

    # Initial check to populate state
    logger.info("Checking all nodes...")
    monitor.check_all_nodes()

    # Generate report
    logger.info("Generating report...")
    report = monitor.generate_daily_report()

    # Output
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Report saved to {output_path}")
    else:
        print(json.dumps(report, indent=2))


def cmd_check(args):
    """Quick check of all nodes."""
    config_path = args.config or os.environ.get("HIVE_NODES_CONFIG")
    if not config_path:
        logger.error("No config file specified")
        sys.exit(1)

    nodes = load_nodes(config_path)
    monitor = FleetMonitor(nodes)
    results = monitor.check_all_nodes()
    print(json.dumps(results, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description="Hive Fleet Monitor - Real-time monitoring and reports"
    )
    parser.add_argument("--config", "-c", help="Path to nodes.json config file")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # monitor command
    monitor_parser = subparsers.add_parser("monitor", help="Run continuous monitoring daemon")
    monitor_parser.add_argument("--interval", "-i", type=int, default=60,
                                help="Check interval in seconds (default: 60)")

    # report command
    report_parser = subparsers.add_parser("report", help="Generate a daily report")
    report_parser.add_argument("--output", "-o", help="Output file path (default: stdout)")

    # check command
    check_parser = subparsers.add_parser("check", help="Quick status check")

    args = parser.parse_args()

    if args.command == "monitor":
        cmd_monitor(args)
    elif args.command == "report":
        cmd_report(args)
    elif args.command == "check":
        cmd_check(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
