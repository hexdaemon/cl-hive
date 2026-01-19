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

# Goat Feeder configuration
# Revenue is tracked via LNbits API - payments with "⚡CyberHerd Treats⚡" in memo
GOAT_FEEDER_PATTERN = "⚡CyberHerd Treats⚡"
LNBITS_URL = "http://127.0.0.1:3002"
LNBITS_INVOICE_KEY = "ac0dcb0cdab94f72b757d0f3aa85d08a"

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
        Tool(
            name="hive_expansion_mode",
            description="Get or set the expansion mode for a node. When enabled, the planner can propose channel opens to improve topology.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "enabled": {
                        "type": "boolean",
                        "description": "Enable or disable expansions (optional, omit to just get current status)"
                    }
                },
                "required": ["node"]
            }
        ),
        # =====================================================================
        # Splice Coordination Tools (Phase 3)
        # =====================================================================
        Tool(
            name="hive_splice_check",
            description="Check if a splice operation is safe for fleet connectivity. SAFETY CHECK ONLY - each node manages its own funds. Use before splice-out to ensure fleet connectivity is maintained.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "External peer being spliced from/to"
                    },
                    "splice_type": {
                        "type": "string",
                        "enum": ["splice_in", "splice_out"],
                        "description": "Type of splice operation"
                    },
                    "amount_sats": {
                        "type": "integer",
                        "description": "Amount to splice in/out (satoshis)"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Optional specific channel ID"
                    }
                },
                "required": ["node", "peer_id", "splice_type", "amount_sats"]
            }
        ),
        Tool(
            name="hive_splice_recommendations",
            description="Get splice recommendations for a specific peer. Returns info about fleet connectivity and safe splice amounts. INFORMATION ONLY - helps make informed splice decisions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "External peer to analyze"
                    }
                },
                "required": ["node", "peer_id"]
            }
        ),
        Tool(
            name="hive_liquidity_intelligence",
            description="Get fleet liquidity intelligence for coordinated decisions. Information sharing only - shows which members need what, enabling coordinated fee/rebalance decisions. No fund movement between nodes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "action": {
                        "type": "string",
                        "enum": ["status", "needs"],
                        "description": "Query type: 'status' for overview, 'needs' for fleet liquidity needs"
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
            description="""Manage peer-level fee and rebalance policies. Actions: list, get, set, delete.

Use static policies to lock in fees for problem channels that Hill Climbing can't fix:
- Stagnant (100% local, no flow): strategy=static, fee_ppm=50, rebalance=disabled
- Depleted (<10% local): strategy=static, fee_ppm=200, rebalance=sink_only
- Zombie (offline/inactive): strategy=static, fee_ppm=2000, rebalance=disabled

Remove policies with action=delete when channels recover.""",
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
            description="""Manually set fee for a channel with clboss coordination. Use force=true to override bounds.

Use this for underwater bleeders with active flow where you want to adjust fees but keep Hill Climbing active.
For stagnant/depleted/zombie channels, prefer revenue_policy with strategy=static instead.

Fee targets: stagnant=50ppm, depleted=150-250ppm, active underwater=100-600ppm, zombie=2000+ppm.""",
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
        Tool(
            name="revenue_outgoing",
            description="Get goat feeder P&L: Lightning Goats revenue (incoming donations) vs CyberHerd Treats expenses (outgoing rewards). Shows goat feeder profitability separate from routing.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "window_days": {
                        "type": "integer",
                        "description": "Time window in days (default: 30)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="revenue_competitor_analysis",
            description="Get competitor fee analysis from hive intelligence. Shows how our fees compare to competitors, market positioning opportunities, and recommended fee adjustments.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Specific peer pubkey (optional, omit for top N by reporters)"
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top peers to analyze (default: 10)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="goat_feeder_history",
            description="Get historical goat feeder P&L from the advisor database. Shows snapshots over time for trend analysis.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (optional, omit for all nodes)"
                    },
                    "days": {
                        "type": "integer",
                        "description": "Days of history to retrieve (default: 30)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="goat_feeder_trends",
            description="Get goat feeder trend analysis comparing current vs previous period. Shows if goat feeder profitability is improving, stable, or declining.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (optional, omit for all nodes)"
                    },
                    "days": {
                        "type": "integer",
                        "description": "Analysis period in days (default: 7)"
                    }
                },
                "required": []
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
        ),
        # =====================================================================
        # Routing Pool Tools - Collective Economics (Phase 0)
        # =====================================================================
        Tool(
            name="pool_status",
            description="Get routing pool status including revenue, contributions, and distributions. Shows collective economics metrics for the hive.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "period": {
                        "type": "string",
                        "description": "Period to query (format: YYYY-WW, defaults to current week)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="pool_member_status",
            description="Get routing pool status for a specific member including contribution scores, revenue share, and distribution history.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Member pubkey (defaults to self)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="pool_distribution",
            description="Calculate distribution amounts for a period (dry run). Shows what each member would receive if settled now.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "period": {
                        "type": "string",
                        "description": "Period to calculate (format: YYYY-WW, defaults to current week)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="pool_snapshot",
            description="Trigger a contribution snapshot for all hive members. Records current contribution metrics for the period.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "period": {
                        "type": "string",
                        "description": "Period to snapshot (format: YYYY-WW, defaults to current week)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="pool_settle",
            description="Settle a routing pool period and record distributions. Use dry_run=true first to preview.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "period": {
                        "type": "string",
                        "description": "Period to settle (format: YYYY-WW, defaults to previous week)"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, calculate but don't record (default: true)"
                    }
                },
                "required": ["node"]
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
        elif name == "hive_expansion_mode":
            result = await handle_expansion_mode(arguments)
        # Splice coordination tools
        elif name == "hive_splice_check":
            result = await handle_splice_check(arguments)
        elif name == "hive_splice_recommendations":
            result = await handle_splice_recommendations(arguments)
        elif name == "hive_liquidity_intelligence":
            result = await handle_liquidity_intelligence(arguments)
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
        elif name == "revenue_outgoing":
            result = await handle_revenue_outgoing(arguments)
        elif name == "revenue_competitor_analysis":
            result = await handle_revenue_competitor_analysis(arguments)
        elif name == "goat_feeder_history":
            result = await handle_goat_feeder_history(arguments)
        elif name == "goat_feeder_trends":
            result = await handle_goat_feeder_trends(arguments)
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
        # Routing Pool tools
        elif name == "pool_status":
            result = await handle_pool_status(arguments)
        elif name == "pool_member_status":
            result = await handle_pool_member_status(arguments)
        elif name == "pool_distribution":
            result = await handle_pool_distribution(arguments)
        elif name == "pool_snapshot":
            result = await handle_pool_snapshot(arguments)
        elif name == "pool_settle":
            result = await handle_pool_settle(arguments)
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
    """Get channel list with flow profiles and profitability data."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get raw channel data
    channels_result = await node.call("listpeerchannels")

    # Try to get profitability data from revenue-ops
    try:
        profitability = await node.call("revenue-profitability")
    except Exception:
        profitability = None

    # Enhance channels with flow data from listpeerchannels fields
    if "channels" in channels_result:
        for channel in channels_result["channels"]:
            scid = channel.get("short_channel_id")
            if not scid:
                continue

            # Extract in/out payment counts from CLN
            in_fulfilled = channel.get("in_payments_fulfilled", 0)
            out_fulfilled = channel.get("out_payments_fulfilled", 0)
            in_msat = channel.get("in_fulfilled_msat", 0)
            out_msat = channel.get("out_fulfilled_msat", 0)

            # Calculate flow profile
            total_payments = in_fulfilled + out_fulfilled
            if total_payments == 0:
                flow_profile = "inactive"
                inbound_outbound_ratio = 0.0
            elif out_fulfilled == 0:
                flow_profile = "inbound_only"
                inbound_outbound_ratio = float('inf')
            elif in_fulfilled == 0:
                flow_profile = "outbound_only"
                inbound_outbound_ratio = 0.0
            else:
                inbound_outbound_ratio = round(in_fulfilled / out_fulfilled, 2)
                if inbound_outbound_ratio > 3.0:
                    flow_profile = "inbound_dominant"
                elif inbound_outbound_ratio < 0.33:
                    flow_profile = "outbound_dominant"
                else:
                    flow_profile = "balanced"

            # Add flow metrics to channel
            channel["flow_profile"] = flow_profile
            channel["inbound_outbound_ratio"] = inbound_outbound_ratio if inbound_outbound_ratio != float('inf') else "infinite"
            channel["inbound_payments"] = in_fulfilled
            channel["outbound_payments"] = out_fulfilled
            channel["inbound_volume_sats"] = in_msat // 1000 if isinstance(in_msat, int) else 0
            channel["outbound_volume_sats"] = out_msat // 1000 if isinstance(out_msat, int) else 0

            # Add profitability data if available
            if profitability and "channels_by_class" in profitability:
                for class_name, class_channels in profitability["channels_by_class"].items():
                    for ch in class_channels:
                        if ch.get("channel_id") == scid:
                            channel["profitability_class"] = class_name
                            channel["net_profit_sats"] = ch.get("net_profit_sats", 0)
                            channel["roi_percentage"] = ch.get("roi_percentage", 0)
                            break

    return channels_result


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
    """
    Get topology analysis from planner log and topology view.

    Enhanced with cooperation module data (Phase 7):
    - Expansion recommendations with hive coverage diversity
    - Network competition analysis
    - Bottleneck peer identification
    - Coverage summary
    """
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get planner log, topology info, and expansion recommendations
    planner_log = await node.call("hive-planner-log", {"limit": 10})
    topology = await node.call("hive-topology")

    # Get expansion recommendations with cooperation module intelligence
    try:
        expansion_recs = await node.call("hive-expansion-recommendations", {"limit": 10})
    except Exception as e:
        # Graceful fallback if RPC not available
        expansion_recs = {"error": str(e), "recommendations": []}

    return {
        "planner_log": planner_log,
        "topology": topology,
        "expansion_recommendations": expansion_recs.get("recommendations", []),
        "coverage_summary": expansion_recs.get("coverage_summary", {}),
        "cooperation_modules": expansion_recs.get("cooperation_modules", {})
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


async def handle_expansion_mode(args: Dict) -> Dict:
    """Get or set expansion mode."""
    node_name = args.get("node")
    enabled = args.get("enabled")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    if enabled is not None:
        result = await node.call("hive-enable-expansions", {"enabled": enabled})
        return result
    else:
        # Get current status
        status = await node.call("hive-status")
        planner = status.get("planner", {})
        return {
            "expansions_enabled": planner.get("expansions_enabled", False),
            "max_feerate_perkb": planner.get("max_expansion_feerate_perkb", 5000)
        }


# =============================================================================
# Splice Coordination Handlers (Phase 3)
# =============================================================================

async def handle_splice_check(args: Dict) -> Dict:
    """
    Check if a splice operation is safe for fleet connectivity.

    SAFETY CHECK ONLY - each node manages its own funds.
    Returns safety assessment with fleet capacity analysis.
    """
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    splice_type = args.get("splice_type")
    amount_sats = args.get("amount_sats")
    channel_id = args.get("channel_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {
        "peer_id": peer_id,
        "splice_type": splice_type,
        "amount_sats": amount_sats
    }
    if channel_id:
        params["channel_id"] = channel_id

    result = await node.call("hive-splice-check", params)

    # Add context for AI advisor
    if result.get("safety") == "blocked":
        result["ai_recommendation"] = (
            "DO NOT proceed with this splice - it would break fleet connectivity. "
            "Another member should open a channel to this peer first."
        )
    elif result.get("safety") == "coordinate":
        result["ai_recommendation"] = (
            "Consider delaying this splice to allow fleet coordination. "
            "Fleet connectivity would be reduced but not broken."
        )
    else:
        result["ai_recommendation"] = "Safe to proceed with this splice operation."

    return result


async def handle_splice_recommendations(args: Dict) -> Dict:
    """
    Get splice recommendations for a specific peer.

    Returns fleet connectivity info and safe splice amounts.
    INFORMATION ONLY - helps make informed splice decisions.
    """
    node_name = args.get("node")
    peer_id = args.get("peer_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-splice-recommendations", {"peer_id": peer_id})


async def handle_liquidity_intelligence(args: Dict) -> Dict:
    """
    Get fleet liquidity intelligence for coordinated decisions.

    Information sharing only - no fund movement between nodes.
    Shows fleet liquidity state and needs for coordination.
    """
    node_name = args.get("node")
    action = args.get("action", "status")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-liquidity-state", {"action": action})

    # Add context about what this data means
    if action == "needs" and result.get("fleet_needs"):
        needs = result["fleet_needs"]
        high_priority = [n for n in needs if n.get("severity") == "high"]
        if high_priority:
            result["ai_note"] = (
                f"{len(high_priority)} fleet members have high-priority liquidity needs. "
                "Consider fee adjustments to help direct flow to struggling members."
            )
    elif action == "status":
        summary = result.get("fleet_summary", {})
        depleted_count = summary.get("members_with_depleted_channels", 0)
        if depleted_count > 0:
            result["ai_note"] = (
                f"{depleted_count} members have depleted channels. "
                "Fleet may benefit from coordinated fee adjustments."
            )

    return result


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
    """Get cl-revenue-ops plugin status with competitor intelligence info."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get base status from cl-revenue-ops
    status = await node.call("revenue-status")

    if "error" in status:
        return status

    # Add competitor intelligence status from cl-hive
    try:
        intel_result = await node.call("hive-fee-intel-query", {"action": "list"})

        if intel_result.get("error"):
            status["competitor_intelligence"] = {
                "enabled": False,
                "error": intel_result.get("error"),
                "data_quality": "unavailable"
            }
        else:
            peers = intel_result.get("peers", [])
            peers_tracked = len(peers)

            # Calculate data quality based on confidence scores
            if peers_tracked == 0:
                data_quality = "no_data"
            else:
                avg_confidence = sum(p.get("confidence", 0) for p in peers) / peers_tracked
                if avg_confidence > 0.6:
                    data_quality = "good"
                elif avg_confidence > 0.3:
                    data_quality = "moderate"
                else:
                    data_quality = "stale"

            # Find most recent update
            last_sync = max(
                (p.get("last_updated", 0) for p in peers),
                default=0
            )

            status["competitor_intelligence"] = {
                "enabled": True,
                "peers_tracked": peers_tracked,
                "last_sync": last_sync,
                "data_quality": data_quality
            }

    except Exception as e:
        status["competitor_intelligence"] = {
            "enabled": False,
            "error": str(e),
            "data_quality": "unavailable"
        }

    return status


async def handle_revenue_profitability(args: Dict) -> Dict:
    """Get channel profitability analysis with market context."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if channel_id:
        params["channel_id"] = channel_id

    # Get profitability data
    profitability = await node.call("revenue-profitability", params if params else None)

    if "error" in profitability:
        return profitability

    # Try to add market context from competitor intelligence
    try:
        channels = profitability.get("channels", [])

        # Build a map of peer_id -> intel for quick lookup
        intel_map = {}
        intel_result = await node.call("hive-fee-intel-query", {"action": "list"})
        if not intel_result.get("error"):
            for peer in intel_result.get("peers", []):
                pid = peer.get("peer_id")
                if pid:
                    intel_map[pid] = peer

        # Add market context to each channel
        for channel in channels:
            peer_id = channel.get("peer_id")
            if peer_id and peer_id in intel_map:
                intel = intel_map[peer_id]
                their_avg = intel.get("avg_fee_charged", 0)
                our_fee = channel.get("our_fee_ppm", 0)

                # Determine position
                if their_avg == 0:
                    position = "unknown"
                    suggested_adjustment = None
                elif our_fee < their_avg * 0.8:
                    position = "underpriced"
                    suggested_adjustment = f"+{their_avg - our_fee} ppm"
                elif our_fee > their_avg * 1.2:
                    position = "premium"
                    suggested_adjustment = f"-{our_fee - their_avg} ppm"
                else:
                    position = "competitive"
                    suggested_adjustment = None

                channel["market_context"] = {
                    "competitor_avg_fee": their_avg,
                    "market_position": position,
                    "suggested_adjustment": suggested_adjustment,
                    "confidence": intel.get("confidence", 0)
                }
            else:
                channel["market_context"] = None

    except Exception as e:
        # Don't fail if competitor intel is unavailable
        logger.debug(f"Could not add market context: {e}")

    return profitability


async def handle_revenue_dashboard(args: Dict) -> Dict:
    """Get financial health dashboard with routing and goat feeder revenue."""
    node_name = args.get("node")
    window_days = args.get("window_days", 30)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get base dashboard from cl-revenue-ops (routing P&L)
    dashboard = await node.call("revenue-dashboard", {"window_days": window_days})

    if "error" in dashboard:
        return dashboard

    import time
    since_timestamp = int(time.time()) - (window_days * 86400)

    # Fetch goat feeder revenue from LNbits
    goat_feeder = await get_goat_feeder_revenue(since_timestamp)

    # Extract routing P&L data from cl-revenue-ops dashboard structure
    # Data is in "period" and "financial_health", not "pnl_summary"
    period = dashboard.get("period", {})
    financial_health = dashboard.get("financial_health", {})
    routing_revenue = period.get("gross_revenue_sats", 0)
    routing_opex = period.get("opex_sats", 0)
    routing_net = financial_health.get("net_profit_sats", 0)

    # Initialize pnl structure for building enhanced response
    pnl = {}

    # Goat feeder revenue (no expenses tracked)
    goat_revenue = goat_feeder.get("total_sats", 0)
    goat_count = goat_feeder.get("payment_count", 0)

    # Combined totals
    total_revenue = routing_revenue + goat_revenue
    total_net = routing_net + goat_revenue  # Goat revenue adds directly to profit

    # Calculate combined operating margin
    if total_revenue > 0:
        combined_margin_pct = round((total_net / total_revenue) * 100, 2)
    else:
        combined_margin_pct = financial_health.get("operating_margin_pct", 0.0)

    # Build enhanced P&L structure
    # Note: opex_breakdown not exposed in dashboard API, set to 0
    pnl["routing"] = {
        "revenue_sats": routing_revenue,
        "opex_sats": routing_opex,
        "net_profit_sats": routing_net,
        "opex_breakdown": {
            "rebalance_cost_sats": 0,
            "closure_cost_sats": 0,
            "splice_cost_sats": 0
        }
    }

    pnl["goat_feeder"] = {
        "revenue_sats": goat_revenue,
        "payment_count": goat_count,
        "source": "LNbits"
    }

    # Record goat feeder snapshot to advisor database for historical tracking
    try:
        db = ensure_advisor_db()
        db.record_goat_feeder_snapshot(
            node_name=node_name,
            window_days=window_days,
            revenue_sats=goat_revenue,
            revenue_count=goat_count,
            expense_sats=0,
            expense_count=0,
            expense_routing_fee_sats=0
        )
    except Exception as e:
        logger.warning(f"Failed to record goat feeder snapshot: {e}")

    pnl["combined"] = {
        "total_revenue_sats": total_revenue,
        "total_opex_sats": routing_opex,
        "net_profit_sats": total_net,
        "operating_margin_pct": combined_margin_pct
    }

    # Update top-level fields for backwards compatibility
    pnl["gross_revenue_sats"] = total_revenue
    pnl["net_profit_sats"] = total_net
    pnl["operating_margin_pct"] = combined_margin_pct

    dashboard["pnl_summary"] = pnl

    return dashboard


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


async def get_goat_feeder_revenue(since_timestamp: int) -> Dict[str, Any]:
    """
    Fetch goat feeder revenue from LNbits.

    Queries the LNbits wallet for payments with "⚡CyberHerd Treats⚡" in the memo.
    These are incoming payments to the sat wallet from the goat feeder.

    Args:
        since_timestamp: Only count payments after this timestamp

    Returns:
        Dict with total_sats and payment_count
    """
    import urllib.request
    import json

    try:
        # Query LNbits payments API using urllib (no external dependencies)
        req = urllib.request.Request(
            f"{LNBITS_URL}/api/v1/payments",
            headers={"X-Api-Key": LNBITS_INVOICE_KEY}
        )
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status != 200:
                return {"total_sats": 0, "payment_count": 0, "error": f"API error: {response.status}"}
            payments = json.loads(response.read())

        total_sats = 0
        payment_count = 0

        for payment in payments:
            # Only count incoming payments (positive amount)
            amount = payment.get("amount", 0)
            if amount <= 0:
                continue

            # Check if memo matches goat feeder pattern
            memo = payment.get("memo", "") or ""
            if GOAT_FEEDER_PATTERN not in memo:
                continue

            # Parse timestamp (LNbits uses ISO date string in 'time' field)
            payment_time_str = payment.get("time", "")
            try:
                from datetime import datetime
                # Handle ISO format with or without timezone
                if "." in payment_time_str:
                    payment_time = datetime.fromisoformat(payment_time_str.replace("Z", "+00:00"))
                else:
                    payment_time = datetime.fromisoformat(payment_time_str)
                payment_timestamp = int(payment_time.timestamp())
            except (ValueError, TypeError):
                payment_timestamp = 0

            if payment_timestamp < since_timestamp:
                continue

            # LNbits amounts are in millisats
            total_sats += amount // 1000
            payment_count += 1

        return {
            "total_sats": total_sats,
            "payment_count": payment_count
        }

    except Exception as e:
        logger.warning(f"Error fetching goat feeder revenue from LNbits: {e}")
        return {
            "total_sats": 0,
            "payment_count": 0,
            "error": str(e)
        }


async def handle_revenue_outgoing(args: Dict) -> Dict:
    """Get goat feeder revenue from LNbits."""
    window_days = args.get("window_days", 30)

    import time
    since_timestamp = int(time.time()) - (window_days * 86400)

    # Get goat feeder revenue from LNbits
    revenue = await get_goat_feeder_revenue(since_timestamp)

    return {
        "window_days": window_days,
        "goat_feeder": {
            "revenue_sats": revenue.get("total_sats", 0),
            "payment_count": revenue.get("payment_count", 0),
            "pattern": GOAT_FEEDER_PATTERN,
            "source": f"LNbits ({LNBITS_URL})"
        },
        "error": revenue.get("error")
    }


async def handle_revenue_competitor_analysis(args: Dict) -> Dict:
    """
    Get competitor fee analysis from hive intelligence.

    Shows:
    - How our fees compare to competitors
    - Market positioning opportunities
    - Recommended fee adjustments

    Uses the hive-fee-intel-query RPC to get aggregated competitor data.
    """
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    top_n = args.get("top_n", 10)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Query competitor intelligence from cl-hive
    if peer_id:
        # Single peer query
        intel_result = await node.call("hive-fee-intel-query", {
            "peer_id": peer_id,
            "action": "query"
        })

        if intel_result.get("error"):
            return {
                "node": node_name,
                "error": intel_result.get("error"),
                "message": intel_result.get("message", "No data available")
            }

        # Get our current fee to this peer for comparison
        channels_result = await node.call("listchannels", {"source": peer_id})

        our_fee = 0
        for channel in channels_result.get("channels", []):
            if channel.get("source") == peer_id:
                our_fee = channel.get("fee_per_millionth", 0)
                break

        # Analyze positioning
        their_avg_fee = intel_result.get("avg_fee_charged", 0)
        analysis = _analyze_market_position(our_fee, their_avg_fee, intel_result)

        return {
            "node": node_name,
            "analysis": [analysis],
            "summary": {
                "underpriced_count": 1 if analysis.get("market_position") == "underpriced" else 0,
                "competitive_count": 1 if analysis.get("market_position") == "competitive" else 0,
                "premium_count": 1 if analysis.get("market_position") == "premium" else 0,
                "total_opportunity_sats": 0  # Single peer, no aggregate
            }
        }

    else:
        # List all known peers
        intel_result = await node.call("hive-fee-intel-query", {"action": "list"})

        if intel_result.get("error"):
            return {
                "node": node_name,
                "error": intel_result.get("error")
            }

        peers = intel_result.get("peers", [])[:top_n]

        # Analyze each peer
        analyses = []
        underpriced = 0
        competitive = 0
        premium = 0

        for peer_intel in peers:
            pid = peer_intel.get("peer_id", "")
            their_avg_fee = peer_intel.get("avg_fee_charged", 0)

            # For batch, we use optimal_fee_estimate as proxy for "our fee"
            # since getting actual channel fees for all peers is expensive
            our_fee = peer_intel.get("optimal_fee_estimate", their_avg_fee)

            analysis = _analyze_market_position(our_fee, their_avg_fee, peer_intel)
            analysis["peer_id"] = pid
            analyses.append(analysis)

            if analysis.get("market_position") == "underpriced":
                underpriced += 1
            elif analysis.get("market_position") == "competitive":
                competitive += 1
            else:
                premium += 1

        return {
            "node": node_name,
            "analysis": analyses,
            "summary": {
                "underpriced_count": underpriced,
                "competitive_count": competitive,
                "premium_count": premium,
                "peers_analyzed": len(analyses)
            }
        }


def _analyze_market_position(our_fee: int, their_avg_fee: int, intel: Dict) -> Dict:
    """
    Analyze market position relative to competitor.

    Returns analysis dict with position and recommendation.
    """
    confidence = intel.get("confidence", 0)
    elasticity = intel.get("estimated_elasticity", 0)
    optimal_estimate = intel.get("optimal_fee_estimate", 0)

    # Determine position
    if their_avg_fee == 0:
        position = "unknown"
        opportunity = "hold"
        reasoning = "No competitor fee data available"
    elif our_fee < their_avg_fee * 0.8:
        position = "underpriced"
        opportunity = "raise_fees"
        diff_pct = ((their_avg_fee - our_fee) / their_avg_fee * 100) if their_avg_fee > 0 else 0
        reasoning = f"We're {diff_pct:.0f}% cheaper than competitors"
    elif our_fee > their_avg_fee * 1.2:
        position = "premium"
        opportunity = "lower_fees" if elasticity < -0.5 else "hold"
        diff_pct = ((our_fee - their_avg_fee) / their_avg_fee * 100) if their_avg_fee > 0 else 0
        reasoning = f"We're {diff_pct:.0f}% more expensive than competitors"
    else:
        position = "competitive"
        opportunity = "hold"
        reasoning = "Fees are competitively positioned"

    suggested_fee = optimal_estimate if optimal_estimate > 0 else our_fee

    return {
        "our_fee_ppm": our_fee,
        "their_avg_fee": their_avg_fee,
        "market_position": position,
        "opportunity": opportunity,
        "suggested_fee": suggested_fee,
        "confidence": confidence,
        "reasoning": reasoning
    }


async def handle_goat_feeder_history(args: Dict) -> Dict:
    """Get historical goat feeder P&L from the advisor database."""
    node_name = args.get("node")
    days = args.get("days", 30)

    db = ensure_advisor_db()
    history = db.get_goat_feeder_history(node_name=node_name, days=days)

    if not history:
        return {
            "snapshots": [],
            "count": 0,
            "note": "No goat feeder history found. Run revenue_dashboard to start recording snapshots."
        }

    return {
        "snapshots": [
            {
                "timestamp": s.timestamp.isoformat(),
                "node_name": s.node_name,
                "window_days": s.window_days,
                "revenue_sats": s.revenue_sats,
                "revenue_count": s.revenue_count,
                "expense_sats": s.expense_sats,
                "expense_count": s.expense_count,
                "net_profit_sats": s.net_profit_sats,
                "profitable": s.profitable
            }
            for s in history
        ],
        "count": len(history),
        "summary": db.get_goat_feeder_summary(node_name=node_name)
    }


async def handle_goat_feeder_trends(args: Dict) -> Dict:
    """Get goat feeder trend analysis."""
    node_name = args.get("node")
    days = args.get("days", 7)

    db = ensure_advisor_db()
    trends = db.get_goat_feeder_trends(node_name=node_name, days=days)

    if not trends:
        return {
            "error": "Insufficient data for trend analysis",
            "note": "Run revenue_dashboard multiple times over several days to collect enough data for trends."
        }

    return trends


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
# Routing Pool Handlers (Phase 0 - Collective Economics)
# =============================================================================

async def handle_pool_status(args: Dict) -> Dict:
    """Get routing pool status."""
    node_name = args.get("node")
    period = args.get("period")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if period:
        params["period"] = period

    return await node.call("hive-pool-status", params)


async def handle_pool_member_status(args: Dict) -> Dict:
    """Get pool status for a specific member."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if peer_id:
        params["peer_id"] = peer_id

    return await node.call("hive-pool-member-status", params)


async def handle_pool_distribution(args: Dict) -> Dict:
    """Calculate distribution for a period."""
    node_name = args.get("node")
    period = args.get("period")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if period:
        params["period"] = period

    return await node.call("hive-pool-distribution", params)


async def handle_pool_snapshot(args: Dict) -> Dict:
    """Trigger contribution snapshot."""
    node_name = args.get("node")
    period = args.get("period")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if period:
        params["period"] = period

    return await node.call("hive-pool-snapshot", params)


async def handle_pool_settle(args: Dict) -> Dict:
    """Settle a routing pool period."""
    node_name = args.get("node")
    period = args.get("period")
    dry_run = args.get("dry_run", True)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {"dry_run": dry_run}
    if period:
        params["period"] = period

    return await node.call("hive-pool-settle", params)


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
