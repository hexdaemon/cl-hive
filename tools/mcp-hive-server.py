#!/usr/bin/env python3
"""
MCP Server for cl-hive Fleet Management

This MCP server allows Claude Code to manage a fleet of Lightning nodes
running cl-hive and cl-revenue-ops. It connects to nodes via CLN's REST API
and exposes tools for:

cl-hive tools:
- Viewing pending actions and approving/rejecting them
- Checking hive status across all nodes
- Managing channels, topology, and governance mode

cl-revenue-ops tools:
- Channel profitability analysis and financial dashboards
- Fee management with Hill Climbing optimization
- Rebalancing with EV-based decision making
- Peer policy management (dynamic/static/hive/passive strategies)
- Runtime configuration and debugging

Usage:
    # Add to Claude Code settings (~/.claude/claude_code_config.json):
    {
      "mcpServers": {
        "hive": {
          "command": "python3",
          "args": ["/path/to/mcp-hive-server.py"],
          "env": {
            "HIVE_NODES_CONFIG": "/path/to/nodes.json"
          }
        }
      }
    }

    # nodes.json format:
    {
      "nodes": [
        {
          "name": "alice",
          "rest_url": "https://localhost:8181",
          "rune": "...",
          "ca_cert": "/path/to/ca.pem"
        }
      ]
    }
"""

import asyncio
import json
import logging
import os
import ssl
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

# Add tools directory to path for advisor_db import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from advisor_db import AdvisorDB

# MCP SDK imports
try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent, Resource
except ImportError:
    print("MCP SDK not installed. Run: pip install mcp")
    raise

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-hive")

# =============================================================================
# Strategy Prompt Loading
# =============================================================================

STRATEGY_DIR = os.environ.get('HIVE_STRATEGY_DIR', '')


def load_strategy(name: str) -> str:
    """
    Load a strategy prompt from a markdown file.

    Strategy files are expected in HIVE_STRATEGY_DIR with .md extension.
    Returns empty string if file not found or HIVE_STRATEGY_DIR not set.

    Args:
        name: Base name of strategy file (without .md extension)

    Returns:
        Content of the strategy file, or empty string
    """
    if not STRATEGY_DIR:
        return ""
    path = os.path.join(STRATEGY_DIR, f"{name}.md")
    try:
        with open(path, 'r') as f:
            content = f.read().strip()
            logger.debug(f"Loaded strategy prompt: {name}")
            return "\n\n" + content
    except FileNotFoundError:
        logger.debug(f"Strategy file not found: {path}")
        return ""
    except Exception as e:
        logger.warning(f"Error loading strategy {name}: {e}")
        return ""


# =============================================================================
# Node Connection
# =============================================================================

@dataclass
class NodeConnection:
    """Connection to a CLN node via REST API or Docker exec (for Polar)."""
    name: str
    rest_url: str = ""
    rune: str = ""
    ca_cert: Optional[str] = None
    client: Optional[httpx.AsyncClient] = None
    # Polar/Docker mode
    docker_container: Optional[str] = None
    lightning_dir: str = "/home/clightning/.lightning"
    network: str = "regtest"

    async def connect(self):
        """Initialize the HTTP client (if using REST)."""
        if self.docker_container:
            logger.info(f"Using docker exec for {self.name} ({self.docker_container})")
            return

        ssl_context = None
        if self.ca_cert and os.path.exists(self.ca_cert):
            ssl_context = ssl.create_default_context()
            ssl_context.load_verify_locations(self.ca_cert)

        self.client = httpx.AsyncClient(
            base_url=self.rest_url,
            headers={"Rune": self.rune},
            verify=ssl_context if ssl_context else False,
            timeout=30.0
        )
        logger.info(f"Connected to {self.name} at {self.rest_url}")

    async def close(self):
        """Close the HTTP client."""
        if self.client:
            await self.client.aclose()

    async def call(self, method: str, params: Dict = None) -> Dict:
        """Call a CLN RPC method via REST or docker exec."""
        # Docker exec mode (for Polar)
        if self.docker_container:
            return await self._call_docker(method, params)

        # REST mode
        if not self.client:
            await self.connect()

        try:
            response = await self.client.post(
                f"/v1/{method}",
                json=params or {}
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"RPC error on {self.name}: {e}")
            return {"error": str(e)}

    async def _call_docker(self, method: str, params: Dict = None) -> Dict:
        """Call CLN via docker exec (for Polar testing)."""
        import subprocess

        # Build command
        cmd = [
            "docker", "exec", self.docker_container,
            "lightning-cli",
            f"--lightning-dir={self.lightning_dir}",
            f"--network={self.network}",
            method
        ]

        # Add params as JSON if provided
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


class HiveFleet:
    """Manages connections to multiple Hive nodes."""

    def __init__(self):
        self.nodes: Dict[str, NodeConnection] = {}

    def load_config(self, config_path: str):
        """Load node configuration from JSON file."""
        with open(config_path) as f:
            config = json.load(f)

        mode = config.get("mode", "rest")
        network = config.get("network", "regtest")
        lightning_dir = config.get("lightning_dir", "/home/clightning/.lightning")

        for node_config in config.get("nodes", []):
            if mode == "docker":
                # Docker exec mode (for Polar testing)
                node = NodeConnection(
                    name=node_config["name"],
                    docker_container=node_config["docker_container"],
                    lightning_dir=lightning_dir,
                    network=network
                )
            else:
                # REST mode (for production)
                node = NodeConnection(
                    name=node_config["name"],
                    rest_url=node_config["rest_url"],
                    rune=node_config["rune"],
                    ca_cert=node_config.get("ca_cert")
                )
            self.nodes[node.name] = node

        logger.info(f"Loaded {len(self.nodes)} nodes from config (mode={mode})")

    async def connect_all(self):
        """Connect to all nodes."""
        for node in self.nodes.values():
            try:
                await node.connect()
            except Exception as e:
                logger.error(f"Failed to connect to {node.name}: {e}")

    async def close_all(self):
        """Close all connections."""
        for node in self.nodes.values():
            await node.close()

    def get_node(self, name: str) -> Optional[NodeConnection]:
        """Get a node by name."""
        return self.nodes.get(name)

    async def call_all(self, method: str, params: Dict = None) -> Dict[str, Any]:
        """Call an RPC method on all nodes."""
        results = {}
        for name, node in self.nodes.items():
            results[name] = await node.call(method, params)
        return results


# Global fleet instance
fleet = HiveFleet()

# Global advisor database instance
ADVISOR_DB_PATH = os.environ.get('ADVISOR_DB_PATH', str(Path.home() / ".lightning" / "advisor.db"))
advisor_db: Optional[AdvisorDB] = None


# =============================================================================
# MCP Server
# =============================================================================

server = Server("hive-fleet-manager")


@server.list_tools()
async def list_tools() -> List[Tool]:
    """List available tools for Hive management."""
    return [
        Tool(
            name="hive_status",
            description="Get status of all Hive nodes in the fleet. Shows membership, health, and pending actions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Specific node name (optional, defaults to all nodes)"
                    }
                }
            }
        ),
        Tool(
            name="hive_pending_actions",
            description="Get pending actions that need approval across the fleet. Shows channel opens, bans, expansions waiting for decision.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Specific node name (optional, defaults to all nodes)"
                    }
                }
            }
        ),
        Tool(
            name="hive_approve_action",
            description=f"Approve a pending action on a node. The action will be executed.{load_strategy('approval_criteria')}",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name where action exists"
                    },
                    "action_id": {
                        "type": "integer",
                        "description": "ID of the action to approve"
                    },
                    "reason": {
                        "type": "string",
                        "description": "Reason for approval"
                    }
                },
                "required": ["node", "action_id"]
            }
        ),
        Tool(
            name="hive_reject_action",
            description=f"Reject a pending action on a node. The action will not be executed.{load_strategy('approval_criteria')}",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name where action exists"
                    },
                    "action_id": {
                        "type": "integer",
                        "description": "ID of the action to reject"
                    },
                    "reason": {
                        "type": "string",
                        "description": "Reason for rejection"
                    }
                },
                "required": ["node", "action_id", "reason"]
            }
        ),
        Tool(
            name="hive_members",
            description="List all members of the Hive with their status and health scores.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node to query (optional, defaults to first node)"
                    }
                }
            }
        ),
        Tool(
            name="hive_node_info",
            description="Get detailed info about a specific Lightning node including channels, balance, and peers.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to get info for"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_channels",
            description="List channels for a node with balance and fee information.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_set_fees",
            description="Set channel fees for a specific channel on a node.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID (short_channel_id format)"
                    },
                    "fee_ppm": {
                        "type": "integer",
                        "description": "Fee rate in parts per million"
                    },
                    "base_fee_msat": {
                        "type": "integer",
                        "description": "Base fee in millisatoshis (default: 0)"
                    }
                },
                "required": ["node", "channel_id", "fee_ppm"]
            }
        ),
        Tool(
            name="hive_topology_analysis",
            description="Get topology analysis from the Hive planner. Shows opportunities for channel opens and optimizations.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node to analyze"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_governance_mode",
            description="Get or set the governance mode for a node (advisor, failsafe). Advisor is the primary AI-driven mode; failsafe is for emergencies when AI is unavailable.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["advisor", "failsafe"],
                        "description": "New mode to set (optional, omit to just get current mode). 'advisor' = AI-driven decisions, 'failsafe' = emergency auto-execute"
                    }
                },
                "required": ["node"]
            }
        ),
        # =====================================================================
        # cl-revenue-ops Tools
        # =====================================================================
        Tool(
            name="revenue_status",
            description="Get cl-revenue-ops plugin status including fee controller state, recent changes, and configuration.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="revenue_profitability",
            description="Get channel profitability analysis including ROI, costs, revenue, and classification (profitable/underwater/zombie).",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Specific channel ID (optional, omit for all channels)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="revenue_dashboard",
            description="Get financial health dashboard with TLV, operating margin, annualized ROC, and bleeder channel warnings.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "window_days": {
                        "type": "integer",
                        "description": "P&L calculation window in days (default: 30)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="revenue_policy",
            description="Manage peer-level fee and rebalance policies. Actions: list, get, set, delete.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "action": {
                        "type": "string",
                        "enum": ["list", "get", "set", "delete"],
                        "description": "Policy action to perform"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Peer pubkey (required for get/set/delete)"
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["dynamic", "static", "hive", "passive"],
                        "description": "Fee strategy (for set action)"
                    },
                    "rebalance": {
                        "type": "string",
                        "enum": ["enabled", "disabled", "source_only", "sink_only"],
                        "description": "Rebalance mode (for set action)"
                    },
                    "fee_ppm": {
                        "type": "integer",
                        "description": "Fixed fee PPM (required for static strategy)"
                    }
                },
                "required": ["node", "action"]
            }
        ),
        Tool(
            name="revenue_set_fee",
            description="Manually set fee for a channel with clboss coordination. Use force=true to override bounds.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID (SCID format)"
                    },
                    "fee_ppm": {
                        "type": "integer",
                        "description": "Fee rate in parts per million"
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Override min/max bounds (rate-limited)"
                    }
                },
                "required": ["node", "channel_id", "fee_ppm"]
            }
        ),
        Tool(
            name="revenue_rebalance",
            description="Trigger a manual rebalance between channels with profit/budget constraints.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "from_channel": {
                        "type": "string",
                        "description": "Source channel SCID"
                    },
                    "to_channel": {
                        "type": "string",
                        "description": "Destination channel SCID"
                    },
                    "amount_sats": {
                        "type": "integer",
                        "description": "Amount to rebalance in satoshis"
                    },
                    "max_fee_sats": {
                        "type": "integer",
                        "description": "Maximum acceptable fee (optional)"
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Bypass safety checks (rate-limited)"
                    }
                },
                "required": ["node", "from_channel", "to_channel", "amount_sats"]
            }
        ),
        Tool(
            name="revenue_report",
            description="Generate financial reports: summary, peer, hive, policies, or costs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "report_type": {
                        "type": "string",
                        "enum": ["summary", "peer", "hive", "policies", "costs"],
                        "description": "Type of report to generate"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Peer pubkey (required for peer report)"
                    }
                },
                "required": ["node", "report_type"]
            }
        ),
        Tool(
            name="revenue_config",
            description="Get or set cl-revenue-ops runtime configuration.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "action": {
                        "type": "string",
                        "enum": ["get", "set", "reset", "list-mutable"],
                        "description": "Config action"
                    },
                    "key": {
                        "type": "string",
                        "description": "Configuration key (for get/set/reset)"
                    },
                    "value": {
                        "type": "string",
                        "description": "New value (for set action)"
                    }
                },
                "required": ["node", "action"]
            }
        ),
        Tool(
            name="revenue_debug",
            description="Get diagnostic information for troubleshooting fee or rebalance issues.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "debug_type": {
                        "type": "string",
                        "enum": ["fee", "rebalance"],
                        "description": "Type of debug info (fee adjustments or rebalancing)"
                    }
                },
                "required": ["node", "debug_type"]
            }
        ),
        Tool(
            name="revenue_history",
            description="Get lifetime financial history including closed channels.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    }
                },
                "required": ["node"]
            }
        ),
        # =====================================================================
        # Advisor Database Tools - Historical tracking and trend analysis
        # =====================================================================
        Tool(
            name="advisor_record_snapshot",
            description="Record the current fleet state to the advisor database for historical tracking. Call this at the START of each advisor run to track state over time. This enables trend analysis and velocity calculations.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to record snapshot for"
                    },
                    "snapshot_type": {
                        "type": "string",
                        "enum": ["manual", "hourly", "daily"],
                        "description": "Type of snapshot (default: manual)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="advisor_get_trends",
            description="Get fleet-wide trend analysis over specified period. Shows revenue change, capacity change, health trends, and channels depleting/filling. Use this to understand how the node is performing over time.",
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 7)"
                    }
                }
            }
        ),
        Tool(
            name="advisor_get_velocities",
            description="Get channels with critical velocity - those depleting or filling rapidly. Returns channels predicted to deplete or fill within the threshold hours. Use this to identify channels that need urgent attention (rebalancing, fee changes).",
            inputSchema={
                "type": "object",
                "properties": {
                    "hours_threshold": {
                        "type": "number",
                        "description": "Alert threshold in hours (default: 24). Channels predicted to deplete/fill within this time are returned."
                    }
                }
            }
        ),
        Tool(
            name="advisor_get_channel_history",
            description="Get historical data for a specific channel including balance, fees, and flow over time. Use this to understand a channel's behavior patterns.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID (SCID format)"
                    },
                    "hours": {
                        "type": "integer",
                        "description": "Hours of history to retrieve (default: 24)"
                    }
                },
                "required": ["node", "channel_id"]
            }
        ),
        Tool(
            name="advisor_record_decision",
            description="Record an AI decision to the audit trail. Call this after making any significant decision (approval, rejection, flagging channels). This builds a history of decisions for learning and accountability.",
            inputSchema={
                "type": "object",
                "properties": {
                    "decision_type": {
                        "type": "string",
                        "enum": ["approve", "reject", "flag_channel", "fee_change", "rebalance"],
                        "description": "Type of decision made"
                    },
                    "node": {
                        "type": "string",
                        "description": "Node name where decision applies"
                    },
                    "recommendation": {
                        "type": "string",
                        "description": "What was decided/recommended"
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Why this decision was made"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Related channel ID (optional)"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Related peer ID (optional)"
                    },
                    "confidence": {
                        "type": "number",
                        "description": "Confidence score 0-1 (optional)"
                    }
                },
                "required": ["decision_type", "node", "recommendation"]
            }
        ),
        Tool(
            name="advisor_get_recent_decisions",
            description="Get recent AI decisions from the audit trail. Use this to review past decisions and avoid repeating the same recommendations.",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of decisions to return (default: 20)"
                    }
                }
            }
        ),
        Tool(
            name="advisor_db_stats",
            description="Get advisor database statistics including record counts and oldest data timestamp. Use this to verify the database is collecting data properly.",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        # =====================================================================
        # New Advisor Intelligence Tools
        # =====================================================================
        Tool(
            name="advisor_get_context_brief",
            description="Get a pre-run context summary with situational awareness. Call this at the START of each run to understand: revenue/capacity trends, velocity alerts, unresolved flags, and recent decisions. This gives you memory across runs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "days": {
                        "type": "integer",
                        "description": "Number of days to analyze (default: 7)"
                    }
                }
            }
        ),
        Tool(
            name="advisor_check_alert",
            description="Check if a channel issue should be flagged or skipped (deduplication). Call this BEFORE flagging any channel to avoid repeating alerts. Returns action: 'flag' (new issue), 'skip' (already flagged <24h), 'mention_unresolved' (24-72h), or 'escalate' (>72h).",
            inputSchema={
                "type": "object",
                "properties": {
                    "alert_type": {
                        "type": "string",
                        "enum": ["zombie", "bleeder", "depleting", "velocity", "unprofitable"],
                        "description": "Type of alert"
                    },
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID (SCID format)"
                    }
                },
                "required": ["alert_type", "node"]
            }
        ),
        Tool(
            name="advisor_record_alert",
            description="Record an alert for a channel issue. Only call this after advisor_check_alert returns action='flag'. This tracks when issues were flagged to prevent alert fatigue.",
            inputSchema={
                "type": "object",
                "properties": {
                    "alert_type": {
                        "type": "string",
                        "enum": ["zombie", "bleeder", "depleting", "velocity", "unprofitable"],
                        "description": "Type of alert"
                    },
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID (SCID format)"
                    },
                    "severity": {
                        "type": "string",
                        "enum": ["info", "warning", "critical"],
                        "description": "Alert severity (default: warning)"
                    },
                    "message": {
                        "type": "string",
                        "description": "Alert message/description"
                    }
                },
                "required": ["alert_type", "node"]
            }
        ),
        Tool(
            name="advisor_resolve_alert",
            description="Mark an alert as resolved. Call this when an issue has been addressed (channel closed, rebalanced, etc.).",
            inputSchema={
                "type": "object",
                "properties": {
                    "alert_type": {
                        "type": "string",
                        "enum": ["zombie", "bleeder", "depleting", "velocity", "unprofitable"],
                        "description": "Type of alert"
                    },
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID (SCID format)"
                    },
                    "resolution_action": {
                        "type": "string",
                        "description": "What action resolved the alert (e.g., 'channel_closed', 'rebalanced')"
                    }
                },
                "required": ["alert_type", "node"]
            }
        ),
        Tool(
            name="advisor_get_peer_intel",
            description="Get peer intelligence for a pubkey. Shows reliability score, profitability, force-close history, and recommendation ('excellent', 'good', 'neutral', 'caution', 'avoid'). Use this when evaluating channel open proposals.",
            inputSchema={
                "type": "object",
                "properties": {
                    "peer_id": {
                        "type": "string",
                        "description": "Peer public key"
                    }
                },
                "required": ["peer_id"]
            }
        ),
        Tool(
            name="advisor_measure_outcomes",
            description="Measure outcomes for decisions made 24-72 hours ago. This checks if channel health improved or worsened after decisions were made, enabling learning from past actions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "min_hours": {
                        "type": "integer",
                        "description": "Minimum hours since decision (default: 24)"
                    },
                    "max_hours": {
                        "type": "integer",
                        "description": "Maximum hours since decision (default: 72)"
                    }
                }
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict) -> List[TextContent]:
    """Handle tool calls."""

    try:
        if name == "hive_status":
            result = await handle_hive_status(arguments)
        elif name == "hive_pending_actions":
            result = await handle_pending_actions(arguments)
        elif name == "hive_approve_action":
            result = await handle_approve_action(arguments)
        elif name == "hive_reject_action":
            result = await handle_reject_action(arguments)
        elif name == "hive_members":
            result = await handle_members(arguments)
        elif name == "hive_node_info":
            result = await handle_node_info(arguments)
        elif name == "hive_channels":
            result = await handle_channels(arguments)
        elif name == "hive_set_fees":
            result = await handle_set_fees(arguments)
        elif name == "hive_topology_analysis":
            result = await handle_topology_analysis(arguments)
        elif name == "hive_governance_mode":
            result = await handle_governance_mode(arguments)
        # cl-revenue-ops tools
        elif name == "revenue_status":
            result = await handle_revenue_status(arguments)
        elif name == "revenue_profitability":
            result = await handle_revenue_profitability(arguments)
        elif name == "revenue_dashboard":
            result = await handle_revenue_dashboard(arguments)
        elif name == "revenue_policy":
            result = await handle_revenue_policy(arguments)
        elif name == "revenue_set_fee":
            result = await handle_revenue_set_fee(arguments)
        elif name == "revenue_rebalance":
            result = await handle_revenue_rebalance(arguments)
        elif name == "revenue_report":
            result = await handle_revenue_report(arguments)
        elif name == "revenue_config":
            result = await handle_revenue_config(arguments)
        elif name == "revenue_debug":
            result = await handle_revenue_debug(arguments)
        elif name == "revenue_history":
            result = await handle_revenue_history(arguments)
        # Advisor database tools
        elif name == "advisor_record_snapshot":
            result = await handle_advisor_record_snapshot(arguments)
        elif name == "advisor_get_trends":
            result = await handle_advisor_get_trends(arguments)
        elif name == "advisor_get_velocities":
            result = await handle_advisor_get_velocities(arguments)
        elif name == "advisor_get_channel_history":
            result = await handle_advisor_get_channel_history(arguments)
        elif name == "advisor_record_decision":
            result = await handle_advisor_record_decision(arguments)
        elif name == "advisor_get_recent_decisions":
            result = await handle_advisor_get_recent_decisions(arguments)
        elif name == "advisor_db_stats":
            result = await handle_advisor_db_stats(arguments)
        # New advisor intelligence tools
        elif name == "advisor_get_context_brief":
            result = await handle_advisor_get_context_brief(arguments)
        elif name == "advisor_check_alert":
            result = await handle_advisor_check_alert(arguments)
        elif name == "advisor_record_alert":
            result = await handle_advisor_record_alert(arguments)
        elif name == "advisor_resolve_alert":
            result = await handle_advisor_resolve_alert(arguments)
        elif name == "advisor_get_peer_intel":
            result = await handle_advisor_get_peer_intel(arguments)
        elif name == "advisor_measure_outcomes":
            result = await handle_advisor_measure_outcomes(arguments)
        else:
            result = {"error": f"Unknown tool: {name}"}

        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    except Exception as e:
        logger.exception(f"Error in tool {name}")
        return [TextContent(type="text", text=json.dumps({"error": str(e)}))]


# =============================================================================
# Tool Handlers
# =============================================================================

async def handle_hive_status(args: Dict) -> Dict:
    """Get Hive status from nodes."""
    node_name = args.get("node")

    if node_name:
        node = fleet.get_node(node_name)
        if not node:
            return {"error": f"Unknown node: {node_name}"}
        result = await node.call("hive-status")
        return {node_name: result}
    else:
        return await fleet.call_all("hive-status")


async def handle_pending_actions(args: Dict) -> Dict:
    """Get pending actions from nodes."""
    node_name = args.get("node")

    if node_name:
        node = fleet.get_node(node_name)
        if not node:
            return {"error": f"Unknown node: {node_name}"}
        result = await node.call("hive-pending-actions")
        return {node_name: result}
    else:
        results = {}
        for name, node in fleet.nodes.items():
            results[name] = await node.call("hive-pending-actions")
        return results


async def handle_approve_action(args: Dict) -> Dict:
    """Approve a pending action."""
    node_name = args.get("node")
    action_id = args.get("action_id")
    reason = args.get("reason", "Approved by Claude Code")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Note: reason is for logging only, not passed to plugin
    return await node.call("hive-approve-action", {
        "action_id": action_id
    })


async def handle_reject_action(args: Dict) -> Dict:
    """Reject a pending action."""
    node_name = args.get("node")
    action_id = args.get("action_id")
    reason = args.get("reason")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Note: reason is for logging only, not passed to plugin
    return await node.call("hive-reject-action", {
        "action_id": action_id
    })


async def handle_members(args: Dict) -> Dict:
    """Get Hive members."""
    node_name = args.get("node")

    if node_name:
        node = fleet.get_node(node_name)
    else:
        # Use first available node
        node = next(iter(fleet.nodes.values()), None)

    if not node:
        return {"error": "No nodes available"}

    return await node.call("hive-members")


async def handle_node_info(args: Dict) -> Dict:
    """Get node info."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    info = await node.call("getinfo")
    funds = await node.call("listfunds")

    return {
        "info": info,
        "funds_summary": {
            "onchain_sats": sum(o.get("amount_msat", 0) // 1000
                               for o in funds.get("outputs", [])
                               if o.get("status") == "confirmed"),
            "channel_count": len(funds.get("channels", [])),
            "total_channel_sats": sum(c.get("amount_msat", 0) // 1000
                                      for c in funds.get("channels", []))
        }
    }


async def handle_channels(args: Dict) -> Dict:
    """Get channel list."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("listpeerchannels")


async def handle_set_fees(args: Dict) -> Dict:
    """Set channel fees."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    fee_ppm = args.get("fee_ppm")
    base_fee_msat = args.get("base_fee_msat", 0)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("setchannel", {
        "id": channel_id,
        "feebase": base_fee_msat,
        "feeppm": fee_ppm
    })


async def handle_topology_analysis(args: Dict) -> Dict:
    """Get topology analysis from planner log and topology view."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get both planner log and topology info
    planner_log = await node.call("hive-planner-log", {"limit": 10})
    topology = await node.call("hive-topology")

    return {
        "planner_log": planner_log,
        "topology": topology
    }


async def handle_governance_mode(args: Dict) -> Dict:
    """Get or set governance mode."""
    node_name = args.get("node")
    mode = args.get("mode")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    if mode:
        return await node.call("hive-set-mode", {"mode": mode})
    else:
        status = await node.call("hive-status")
        return {"mode": status.get("governance_mode", "unknown")}


# =============================================================================
# MCP Resources
# =============================================================================

@server.list_resources()
async def list_resources() -> List[Resource]:
    """List available resources for fleet monitoring."""
    resources = [
        Resource(
            uri="hive://fleet/status",
            name="Fleet Status",
            description="Current status of all Hive nodes including health, channels, and governance mode",
            mimeType="application/json"
        ),
        Resource(
            uri="hive://fleet/pending-actions",
            name="Pending Actions",
            description="All pending actions across the fleet that need approval",
            mimeType="application/json"
        ),
        Resource(
            uri="hive://fleet/summary",
            name="Fleet Summary",
            description="Aggregated fleet metrics: total capacity, channels, health status",
            mimeType="application/json"
        )
    ]

    # Add per-node resources
    for node_name in fleet.nodes:
        resources.append(Resource(
            uri=f"hive://node/{node_name}/status",
            name=f"{node_name} Status",
            description=f"Detailed status for node {node_name}",
            mimeType="application/json"
        ))
        resources.append(Resource(
            uri=f"hive://node/{node_name}/channels",
            name=f"{node_name} Channels",
            description=f"Channel list and balances for {node_name}",
            mimeType="application/json"
        ))
        resources.append(Resource(
            uri=f"hive://node/{node_name}/profitability",
            name=f"{node_name} Profitability",
            description=f"Channel profitability analysis for {node_name}",
            mimeType="application/json"
        ))

    return resources


@server.read_resource()
async def read_resource(uri: str) -> str:
    """Read a specific resource."""
    from urllib.parse import urlparse

    parsed = urlparse(uri)

    if parsed.scheme != "hive":
        raise ValueError(f"Unknown URI scheme: {parsed.scheme}")

    path_parts = parsed.path.strip("/").split("/")

    # Fleet-wide resources
    if parsed.netloc == "fleet":
        if len(path_parts) == 1:
            resource_type = path_parts[0]

            if resource_type == "status":
                # Get status from all nodes
                results = {}
                for name, node in fleet.nodes.items():
                    status = await node.call("hive-status")
                    info = await node.call("getinfo")
                    results[name] = {
                        "hive_status": status,
                        "node_info": {
                            "alias": info.get("alias", "unknown"),
                            "id": info.get("id", "unknown"),
                            "blockheight": info.get("blockheight", 0)
                        }
                    }
                return json.dumps(results, indent=2)

            elif resource_type == "pending-actions":
                # Get all pending actions
                results = {}
                total_pending = 0
                for name, node in fleet.nodes.items():
                    pending = await node.call("hive-pending-actions")
                    actions = pending.get("actions", [])
                    results[name] = {
                        "count": len(actions),
                        "actions": actions
                    }
                    total_pending += len(actions)
                return json.dumps({
                    "total_pending": total_pending,
                    "by_node": results
                }, indent=2)

            elif resource_type == "summary":
                # Aggregate fleet summary
                summary = {
                    "total_nodes": len(fleet.nodes),
                    "nodes_healthy": 0,
                    "nodes_unhealthy": 0,
                    "total_channels": 0,
                    "total_capacity_sats": 0,
                    "total_onchain_sats": 0,
                    "total_pending_actions": 0,
                    "nodes": {}
                }

                for name, node in fleet.nodes.items():
                    status = await node.call("hive-status")
                    funds = await node.call("listfunds")
                    pending = await node.call("hive-pending-actions")

                    channels = funds.get("channels", [])
                    outputs = funds.get("outputs", [])
                    pending_count = len(pending.get("actions", []))

                    channel_sats = sum(c.get("amount_msat", 0) // 1000 for c in channels)
                    onchain_sats = sum(o.get("amount_msat", 0) // 1000
                                       for o in outputs if o.get("status") == "confirmed")

                    is_healthy = "error" not in status

                    summary["nodes"][name] = {
                        "healthy": is_healthy,
                        "governance_mode": status.get("governance_mode", "unknown"),
                        "channels": len(channels),
                        "capacity_sats": channel_sats,
                        "onchain_sats": onchain_sats,
                        "pending_actions": pending_count
                    }

                    if is_healthy:
                        summary["nodes_healthy"] += 1
                    else:
                        summary["nodes_unhealthy"] += 1
                    summary["total_channels"] += len(channels)
                    summary["total_capacity_sats"] += channel_sats
                    summary["total_onchain_sats"] += onchain_sats
                    summary["total_pending_actions"] += pending_count

                summary["total_capacity_btc"] = summary["total_capacity_sats"] / 100_000_000
                return json.dumps(summary, indent=2)

    # Per-node resources
    elif parsed.netloc == "node":
        if len(path_parts) >= 2:
            node_name = path_parts[0]
            resource_type = path_parts[1]

            node = fleet.get_node(node_name)
            if not node:
                raise ValueError(f"Unknown node: {node_name}")

            if resource_type == "status":
                status = await node.call("hive-status")
                info = await node.call("getinfo")
                funds = await node.call("listfunds")
                pending = await node.call("hive-pending-actions")

                channels = funds.get("channels", [])
                outputs = funds.get("outputs", [])

                return json.dumps({
                    "node": node_name,
                    "alias": info.get("alias", "unknown"),
                    "pubkey": info.get("id", "unknown"),
                    "hive_status": status,
                    "channels": len(channels),
                    "capacity_sats": sum(c.get("amount_msat", 0) // 1000 for c in channels),
                    "onchain_sats": sum(o.get("amount_msat", 0) // 1000
                                        for o in outputs if o.get("status") == "confirmed"),
                    "pending_actions": len(pending.get("actions", []))
                }, indent=2)

            elif resource_type == "channels":
                channels = await node.call("listpeerchannels")
                return json.dumps(channels, indent=2)

            elif resource_type == "profitability":
                profitability = await node.call("revenue-profitability")
                return json.dumps(profitability, indent=2)

    raise ValueError(f"Unknown resource URI: {uri}")


# =============================================================================
# cl-revenue-ops Tool Handlers
# =============================================================================

async def handle_revenue_status(args: Dict) -> Dict:
    """Get cl-revenue-ops plugin status."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("revenue-status")


async def handle_revenue_profitability(args: Dict) -> Dict:
    """Get channel profitability analysis."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if channel_id:
        params["channel_id"] = channel_id

    return await node.call("revenue-profitability", params if params else None)


async def handle_revenue_dashboard(args: Dict) -> Dict:
    """Get financial health dashboard."""
    node_name = args.get("node")
    window_days = args.get("window_days", 30)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("revenue-dashboard", {"window_days": window_days})


async def handle_revenue_policy(args: Dict) -> Dict:
    """Manage peer-level policies."""
    node_name = args.get("node")
    action = args.get("action")
    peer_id = args.get("peer_id")
    strategy = args.get("strategy")
    rebalance = args.get("rebalance")
    fee_ppm = args.get("fee_ppm")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Build the action string for revenue-policy command
    if action == "list":
        return await node.call("revenue-policy", {"action": "list"})
    elif action == "get":
        if not peer_id:
            return {"error": "peer_id required for get action"}
        return await node.call("revenue-policy", {"action": "get", "peer_id": peer_id})
    elif action == "delete":
        if not peer_id:
            return {"error": "peer_id required for delete action"}
        return await node.call("revenue-policy", {"action": "delete", "peer_id": peer_id})
    elif action == "set":
        if not peer_id:
            return {"error": "peer_id required for set action"}
        params = {"action": "set", "peer_id": peer_id}
        if strategy:
            params["strategy"] = strategy
        if rebalance:
            params["rebalance"] = rebalance
        if fee_ppm is not None:
            params["fee_ppm"] = fee_ppm
        return await node.call("revenue-policy", params)
    else:
        return {"error": f"Unknown action: {action}"}


async def handle_revenue_set_fee(args: Dict) -> Dict:
    """Set channel fee with clboss coordination."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    fee_ppm = args.get("fee_ppm")
    force = args.get("force", False)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {
        "channel_id": channel_id,
        "fee_ppm": fee_ppm
    }
    if force:
        params["force"] = True

    return await node.call("revenue-set-fee", params)


async def handle_revenue_rebalance(args: Dict) -> Dict:
    """Trigger manual rebalance."""
    node_name = args.get("node")
    from_channel = args.get("from_channel")
    to_channel = args.get("to_channel")
    amount_sats = args.get("amount_sats")
    max_fee_sats = args.get("max_fee_sats")
    force = args.get("force", False)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {
        "from_channel": from_channel,
        "to_channel": to_channel,
        "amount_sats": amount_sats
    }
    if max_fee_sats is not None:
        params["max_fee_sats"] = max_fee_sats
    if force:
        params["force"] = True

    return await node.call("revenue-rebalance", params)


async def handle_revenue_report(args: Dict) -> Dict:
    """Generate financial reports."""
    node_name = args.get("node")
    report_type = args.get("report_type")
    peer_id = args.get("peer_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {"report_type": report_type}
    if peer_id and report_type == "peer":
        params["peer_id"] = peer_id

    return await node.call("revenue-report", params)


async def handle_revenue_config(args: Dict) -> Dict:
    """Get or set runtime configuration."""
    node_name = args.get("node")
    action = args.get("action")
    key = args.get("key")
    value = args.get("value")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {"action": action}
    if key:
        params["key"] = key
    if value is not None and action == "set":
        params["value"] = value

    return await node.call("revenue-config", params)


async def handle_revenue_debug(args: Dict) -> Dict:
    """Get diagnostic information."""
    node_name = args.get("node")
    debug_type = args.get("debug_type")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    if debug_type == "fee":
        return await node.call("revenue-fee-debug")
    elif debug_type == "rebalance":
        return await node.call("revenue-rebalance-debug")
    else:
        return {"error": f"Unknown debug type: {debug_type}"}


async def handle_revenue_history(args: Dict) -> Dict:
    """Get lifetime financial history."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("revenue-history")


# =============================================================================
# Advisor Database Tool Handlers
# =============================================================================

def ensure_advisor_db() -> AdvisorDB:
    """Ensure advisor database is initialized."""
    global advisor_db
    if advisor_db is None:
        advisor_db = AdvisorDB(ADVISOR_DB_PATH)
        logger.info(f"Initialized advisor database at {ADVISOR_DB_PATH}")
    return advisor_db


async def handle_advisor_record_snapshot(args: Dict) -> Dict:
    """Record current fleet state to the advisor database."""
    node_name = args.get("node")
    snapshot_type = args.get("snapshot_type", "manual")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    db = ensure_advisor_db()

    # Gather data from the node
    try:
        hive_status = await node.call("hive-status")
        funds = await node.call("listfunds")
        pending = await node.call("hive-pending-actions")

        # Try to get revenue data if plugin is installed
        try:
            dashboard = await node.call("revenue-dashboard", {"window_days": 30})
            profitability = await node.call("revenue-profitability")
            history = await node.call("revenue-history")
        except Exception:
            dashboard = {}
            profitability = {}
            history = {}

        channels = funds.get("channels", [])
        outputs = funds.get("outputs", [])

        # Build report structure for database
        report = {
            "fleet_summary": {
                "total_nodes": 1,
                "nodes_healthy": 1 if "error" not in hive_status else 0,
                "nodes_unhealthy": 0 if "error" not in hive_status else 1,
                "total_channels": len(channels),
                "total_capacity_sats": sum(c.get("amount_msat", 0) // 1000 for c in channels),
                "total_onchain_sats": sum(o.get("amount_msat", 0) // 1000
                                          for o in outputs if o.get("status") == "confirmed"),
                "total_pending_actions": len(pending.get("actions", [])),
                "channel_health": {
                    "balanced": 0,
                    "needs_inbound": 0,
                    "needs_outbound": 0
                }
            },
            "hive_topology": {
                "member_count": len(hive_status.get("members", []))
            },
            "nodes": {
                node_name: {
                    "healthy": "error" not in hive_status,
                    "channels_detail": [],
                    "lifetime_history": history
                }
            }
        }

        # Process channel details for history
        channels_data = await node.call("listpeerchannels")
        prof_data = profitability.get("channels", [])
        prof_by_id = {c.get("channel_id"): c for c in prof_data}

        for ch in channels_data.get("channels", []):
            scid = ch.get("short_channel_id", "")
            prof_ch = prof_by_id.get(scid, {})

            local_msat = ch.get("to_us_msat", 0)
            if isinstance(local_msat, str):
                local_msat = int(local_msat.replace("msat", ""))
            capacity_msat = ch.get("total_msat", 0)
            if isinstance(capacity_msat, str):
                capacity_msat = int(capacity_msat.replace("msat", ""))

            local_sats = local_msat // 1000
            capacity_sats = capacity_msat // 1000
            remote_sats = capacity_sats - local_sats
            balance_ratio = local_sats / capacity_sats if capacity_sats > 0 else 0

            # Extract fee info
            updates = ch.get("updates", {})
            local_updates = updates.get("local", {})
            fee_ppm = local_updates.get("fee_proportional_millionths", 0)
            fee_base = local_updates.get("fee_base_msat", 0)

            ch_detail = {
                "channel_id": scid,
                "peer_id": ch.get("peer_id", ""),
                "capacity_sats": capacity_sats,
                "local_sats": local_sats,
                "remote_sats": remote_sats,
                "balance_ratio": round(balance_ratio, 4),
                "flow_state": prof_ch.get("profitability_class", "unknown"),
                "flow_ratio": prof_ch.get("roi_annual_pct", 0),
                "confidence": 1.0,
                "forward_count": 0,
                "fee_ppm": fee_ppm,
                "fee_base_msat": fee_base,
                "needs_inbound": balance_ratio > 0.8,
                "needs_outbound": balance_ratio < 0.2,
                "is_balanced": 0.2 <= balance_ratio <= 0.8
            }
            report["nodes"][node_name]["channels_detail"].append(ch_detail)

            # Update health counters
            if ch_detail["is_balanced"]:
                report["fleet_summary"]["channel_health"]["balanced"] += 1
            elif ch_detail["needs_inbound"]:
                report["fleet_summary"]["channel_health"]["needs_inbound"] += 1
            elif ch_detail["needs_outbound"]:
                report["fleet_summary"]["channel_health"]["needs_outbound"] += 1

        # Record to database
        snapshot_id = db.record_fleet_snapshot(report, snapshot_type)
        channels_recorded = db.record_channel_states(report)

        return {
            "success": True,
            "snapshot_id": snapshot_id,
            "channels_recorded": channels_recorded,
            "snapshot_type": snapshot_type,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.exception("Error recording snapshot")
        return {"error": f"Failed to record snapshot: {str(e)}"}


async def handle_advisor_get_trends(args: Dict) -> Dict:
    """Get fleet-wide trend analysis."""
    days = args.get("days", 7)

    db = ensure_advisor_db()

    trends = db.get_fleet_trends(days)
    if not trends:
        return {
            "message": "Not enough historical data for trend analysis. Record more snapshots over time.",
            "snapshots_available": len(db.get_recent_snapshots(100))
        }

    return {
        "period_days": days,
        "revenue_change_pct": trends.revenue_change_pct,
        "capacity_change_pct": trends.capacity_change_pct,
        "channel_count_change": trends.channel_count_change,
        "health_trend": trends.health_trend,
        "channels_depleting": trends.channels_depleting,
        "channels_filling": trends.channels_filling
    }


async def handle_advisor_get_velocities(args: Dict) -> Dict:
    """Get channels with critical velocity."""
    hours_threshold = args.get("hours_threshold", 24)

    db = ensure_advisor_db()

    critical_channels = db.get_critical_channels(hours_threshold)

    if not critical_channels:
        return {
            "message": f"No channels predicted to deplete or fill within {hours_threshold} hours",
            "critical_count": 0
        }

    channels = []
    for ch in critical_channels:
        channels.append({
            "node": ch.node_name,
            "channel_id": ch.channel_id,
            "current_balance_ratio": round(ch.current_balance_ratio, 4),
            "velocity_pct_per_hour": round(ch.velocity_pct_per_hour, 4),
            "trend": ch.trend,
            "hours_until_depleted": round(ch.hours_until_depleted, 1) if ch.hours_until_depleted else None,
            "hours_until_full": round(ch.hours_until_full, 1) if ch.hours_until_full else None,
            "urgency": ch.urgency,
            "confidence": round(ch.confidence, 2)
        })

    return {
        "critical_count": len(channels),
        "hours_threshold": hours_threshold,
        "channels": channels
    }


async def handle_advisor_get_channel_history(args: Dict) -> Dict:
    """Get historical data for a specific channel."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    hours = args.get("hours", 24)

    db = ensure_advisor_db()

    history = db.get_channel_history(node_name, channel_id, hours)
    velocity = db.get_channel_velocity(node_name, channel_id)

    result = {
        "node": node_name,
        "channel_id": channel_id,
        "hours_requested": hours,
        "data_points": len(history),
        "history": []
    }

    for h in history:
        result["history"].append({
            "timestamp": datetime.fromtimestamp(h["timestamp"]).isoformat(),
            "local_sats": h["local_sats"],
            "balance_ratio": round(h["balance_ratio"], 4),
            "fee_ppm": h["fee_ppm"],
            "flow_state": h["flow_state"]
        })

    if velocity:
        result["velocity"] = {
            "trend": velocity.trend,
            "velocity_pct_per_hour": round(velocity.velocity_pct_per_hour, 4),
            "hours_until_depleted": round(velocity.hours_until_depleted, 1) if velocity.hours_until_depleted else None,
            "hours_until_full": round(velocity.hours_until_full, 1) if velocity.hours_until_full else None,
            "confidence": round(velocity.confidence, 2)
        }

    return result


async def handle_advisor_record_decision(args: Dict) -> Dict:
    """Record an AI decision to the audit trail."""
    decision_type = args.get("decision_type")
    node_name = args.get("node")
    recommendation = args.get("recommendation")
    reasoning = args.get("reasoning")
    channel_id = args.get("channel_id")
    peer_id = args.get("peer_id")
    confidence = args.get("confidence")

    db = ensure_advisor_db()

    decision_id = db.record_decision(
        decision_type=decision_type,
        node_name=node_name,
        recommendation=recommendation,
        reasoning=reasoning,
        channel_id=channel_id,
        peer_id=peer_id,
        confidence=confidence
    )

    return {
        "success": True,
        "decision_id": decision_id,
        "decision_type": decision_type,
        "timestamp": datetime.now().isoformat()
    }


async def handle_advisor_get_recent_decisions(args: Dict) -> Dict:
    """Get recent AI decisions from the audit trail."""
    limit = args.get("limit", 20)

    db = ensure_advisor_db()

    # Get recent decisions
    with db._get_conn() as conn:
        rows = conn.execute("""
            SELECT id, timestamp, decision_type, node_name, channel_id, peer_id,
                   recommendation, reasoning, confidence, status
            FROM ai_decisions
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,)).fetchall()

    decisions = []
    for row in rows:
        decisions.append({
            "id": row["id"],
            "timestamp": datetime.fromtimestamp(row["timestamp"]).isoformat(),
            "decision_type": row["decision_type"],
            "node": row["node_name"],
            "channel_id": row["channel_id"],
            "peer_id": row["peer_id"],
            "recommendation": row["recommendation"],
            "reasoning": row["reasoning"],
            "confidence": row["confidence"],
            "status": row["status"]
        })

    return {
        "count": len(decisions),
        "decisions": decisions
    }


async def handle_advisor_db_stats(args: Dict) -> Dict:
    """Get advisor database statistics."""
    db = ensure_advisor_db()

    stats = db.get_stats()
    stats["database_path"] = ADVISOR_DB_PATH

    return stats


async def handle_advisor_get_context_brief(args: Dict) -> Dict:
    """Get pre-run context summary for AI advisor."""
    db = ensure_advisor_db()
    days = args.get("days", 7)

    brief = db.get_context_brief(days)

    # Serialize dataclass to dict
    return {
        "period_days": brief.period_days,
        "total_capacity_sats": brief.total_capacity_sats,
        "capacity_change_pct": brief.capacity_change_pct,
        "total_channels": brief.total_channels,
        "channel_count_change": brief.channel_count_change,
        "period_revenue_sats": brief.period_revenue_sats,
        "revenue_change_pct": brief.revenue_change_pct,
        "channels_depleting": brief.channels_depleting,
        "channels_filling": brief.channels_filling,
        "critical_velocity_channels": brief.critical_velocity_channels,
        "unresolved_alerts": brief.unresolved_alerts,
        "recent_decisions_count": brief.recent_decisions_count,
        "decisions_by_type": brief.decisions_by_type,
        "summary_text": brief.summary_text
    }


async def handle_advisor_check_alert(args: Dict) -> Dict:
    """Check if an alert should be raised (deduplication)."""
    db = ensure_advisor_db()

    alert_type = args.get("alert_type")
    node_name = args.get("node")
    channel_id = args.get("channel_id")

    if not alert_type or not node_name:
        return {"error": "alert_type and node are required"}

    status = db.check_alert(alert_type, node_name, channel_id)

    return {
        "alert_type": status.alert_type,
        "node_name": status.node_name,
        "channel_id": status.channel_id,
        "is_new": status.is_new,
        "first_flagged": status.first_flagged.isoformat() if status.first_flagged else None,
        "last_flagged": status.last_flagged.isoformat() if status.last_flagged else None,
        "times_flagged": status.times_flagged,
        "hours_since_last": status.hours_since_last,
        "action": status.action,
        "message": status.message
    }


async def handle_advisor_record_alert(args: Dict) -> Dict:
    """Record an alert (handles dedup automatically)."""
    db = ensure_advisor_db()

    alert_type = args.get("alert_type")
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    peer_id = args.get("peer_id")
    severity = args.get("severity", "warning")
    message = args.get("message")

    if not alert_type or not node_name:
        return {"error": "alert_type and node are required"}

    status = db.record_alert(alert_type, node_name, channel_id, peer_id, severity, message)

    return {
        "recorded": True,
        "alert_type": status.alert_type,
        "is_new": status.is_new,
        "times_flagged": status.times_flagged,
        "action": status.action
    }


async def handle_advisor_resolve_alert(args: Dict) -> Dict:
    """Mark an alert as resolved."""
    db = ensure_advisor_db()

    alert_type = args.get("alert_type")
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    resolution_action = args.get("resolution_action")

    if not alert_type or not node_name:
        return {"error": "alert_type and node are required"}

    resolved = db.resolve_alert(alert_type, node_name, channel_id, resolution_action)

    return {
        "resolved": resolved,
        "alert_type": alert_type,
        "node_name": node_name,
        "channel_id": channel_id
    }


async def handle_advisor_get_peer_intel(args: Dict) -> Dict:
    """Get peer intelligence/reputation data."""
    db = ensure_advisor_db()

    peer_id = args.get("peer_id")

    if peer_id:
        intel = db.get_peer_intelligence(peer_id)
        if not intel:
            return {"error": f"No data for peer: {peer_id}"}

        return {
            "peer_id": intel.peer_id,
            "alias": intel.alias,
            "first_seen": intel.first_seen.isoformat() if intel.first_seen else None,
            "last_seen": intel.last_seen.isoformat() if intel.last_seen else None,
            "channels_opened": intel.channels_opened,
            "channels_closed": intel.channels_closed,
            "force_closes": intel.force_closes,
            "avg_channel_lifetime_days": intel.avg_channel_lifetime_days,
            "total_forwards": intel.total_forwards,
            "total_revenue_sats": intel.total_revenue_sats,
            "total_costs_sats": intel.total_costs_sats,
            "profitability_score": intel.profitability_score,
            "reliability_score": intel.reliability_score,
            "recommendation": intel.recommendation
        }
    else:
        # Return all peers
        all_intel = db.get_all_peer_intelligence()
        return {
            "count": len(all_intel),
            "peers": [{
                "peer_id": intel.peer_id,
                "alias": intel.alias,
                "channels_opened": intel.channels_opened,
                "force_closes": intel.force_closes,
                "total_forwards": intel.total_forwards,
                "total_revenue_sats": intel.total_revenue_sats,
                "profitability_score": intel.profitability_score,
                "reliability_score": intel.reliability_score,
                "recommendation": intel.recommendation
            } for intel in all_intel]
        }


async def handle_advisor_measure_outcomes(args: Dict) -> Dict:
    """Measure outcomes for past decisions."""
    db = ensure_advisor_db()

    min_hours = args.get("min_hours", 24)
    max_hours = args.get("max_hours", 72)

    outcomes = db.measure_decision_outcomes(min_hours, max_hours)

    return {
        "measured_count": len(outcomes),
        "outcomes": outcomes
    }


# =============================================================================
# Main
# =============================================================================

async def main():
    """Run the MCP server."""
    # Load node configuration
    config_path = os.environ.get("HIVE_NODES_CONFIG")
    if config_path and os.path.exists(config_path):
        fleet.load_config(config_path)
        await fleet.connect_all()
    else:
        logger.warning("No HIVE_NODES_CONFIG set - running without nodes")
        logger.info("Set HIVE_NODES_CONFIG=/path/to/nodes.json to connect to nodes")

    # Run the MCP server
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

    # Cleanup
    await fleet.close_all()


if __name__ == "__main__":
    asyncio.run(main())
