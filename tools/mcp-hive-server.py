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
import threading
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
        """Load node configuration from JSON file.

        Supports three configuration styles:
        1. Global mode="rest" - all nodes use REST API
        2. Global mode="docker" - all nodes use docker exec
        3. Per-node mode - each node specifies its own connection type

        Per-node config example:
        {
            "nodes": [
                {"name": "mainnet", "mode": "rest", "rest_url": "...", "rune": "..."},
                {"name": "docker-node", "mode": "docker", "docker_container": "...", "lightning_dir": "...", "network": "..."}
            ]
        }
        """
        with open(config_path) as f:
            config = json.load(f)

        global_mode = config.get("mode", "rest")
        global_network = config.get("network", "regtest")
        global_lightning_dir = config.get("lightning_dir", "/home/clightning/.lightning")

        for node_config in config.get("nodes", []):
            # Per-node mode overrides global mode
            node_mode = node_config.get("mode", global_mode)

            if node_mode == "docker":
                # Docker exec mode
                node = NodeConnection(
                    name=node_config["name"],
                    docker_container=node_config.get("docker_container"),
                    lightning_dir=node_config.get("lightning_dir", global_lightning_dir),
                    network=node_config.get("network", global_network)
                )
            else:
                # REST mode (default)
                node = NodeConnection(
                    name=node_config["name"],
                    rest_url=node_config.get("rest_url"),
                    rune=node_config.get("rune"),
                    ca_cert=node_config.get("ca_cert")
                )
            self.nodes[node.name] = node

        logger.info(f"Loaded {len(self.nodes)} nodes from config (global_mode={global_mode})")

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
            name="hive_propose_promotion",
            description="Propose a neophyte for early promotion to member status. Any member can propose a neophyte for promotion before the 90-day probation period completes. When a majority (51%) of active members approve, the neophyte is promoted.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (must be a member node)"
                    },
                    "target_peer_id": {
                        "type": "string",
                        "description": "The neophyte's public key to propose for promotion"
                    }
                },
                "required": ["node", "target_peer_id"]
            }
        ),
        Tool(
            name="hive_vote_promotion",
            description="Vote to approve a neophyte's promotion to member. Only members can vote. When majority is reached, the neophyte can be promoted.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (must be a member node)"
                    },
                    "target_peer_id": {
                        "type": "string",
                        "description": "The neophyte's public key being voted on"
                    }
                },
                "required": ["node", "target_peer_id"]
            }
        ),
        Tool(
            name="hive_pending_promotions",
            description="Get all pending manual promotion proposals with their approval status.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node to query"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_execute_promotion",
            description="Execute a manual promotion if quorum has been reached. This bypasses the normal 90-day probation period when a majority of members have approved.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "target_peer_id": {
                        "type": "string",
                        "description": "The neophyte's public key to promote"
                    }
                },
                "required": ["node", "target_peer_id"]
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
        Tool(
            name="hive_gossip_stats",
            description="Get gossip statistics and state versions for debugging. Shows version numbers for each peer to verify state synchronization is working correctly.",
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
        # Anticipatory Liquidity Tools (Phase 7.1)
        # =====================================================================
        Tool(
            name="hive_anticipatory_status",
            description="Get anticipatory liquidity manager status. Shows pattern detection state, prediction cache, and configuration.",
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
            name="hive_detect_patterns",
            description="Detect temporal patterns in channel flow. Analyzes historical data to find recurring patterns by hour-of-day and day-of-week that can predict future liquidity needs.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Specific channel ID (optional, omit for summary of all patterns)"
                    },
                    "force_refresh": {
                        "type": "boolean",
                        "description": "Force recalculation even if cached (default: false)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_predict_liquidity",
            description="Predict channel liquidity state N hours from now. Combines velocity analysis with temporal patterns to predict future balance and recommend preemptive rebalancing.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID to predict"
                    },
                    "hours_ahead": {
                        "type": "integer",
                        "description": "Hours to predict ahead (default: 12)"
                    }
                },
                "required": ["node", "channel_id"]
            }
        ),
        Tool(
            name="hive_anticipatory_predictions",
            description="""Get liquidity predictions for all channels at risk - key for proactive management.

**When to use:** Include in state analysis to identify channels that will need attention soon, before they become critical.

**Returns channels predicted to:**
- Deplete (run out of outbound liquidity)
- Saturate (fill up with inbound)

**For each at-risk channel shows:**
- Depletion/saturation risk score (0.0-1.0)
- Hours until predicted problem
- Recommended preemptive action
- Confidence level based on historical patterns

**Integration:** advisor_run_cycle automatically gathers this. Use standalone when focusing on specific liquidity concerns.

**Action guidance:**
- Risk >0.7 + <12h: Urgent rebalancing needed
- Risk >0.5 + <24h: Queue for review
- Risk >0.3: Monitor and consider fee adjustments""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "hours_ahead": {
                        "type": "integer",
                        "description": "Prediction horizon in hours (default: 12)"
                    },
                    "min_risk": {
                        "type": "number",
                        "description": "Minimum risk threshold 0.0-1.0 to include (default: 0.3)"
                    }
                },
                "required": ["node"]
            }
        ),
        # =====================================================================
        # Time-Based Fee Tools (Phase 7.4)
        # =====================================================================
        Tool(
            name="hive_time_fee_status",
            description="Get time-based fee adjustment status. Shows current time context, active adjustments across channels, and configuration for temporal fee optimization.",
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
            name="hive_time_fee_adjustment",
            description="Get time-based fee adjustment for a specific channel. Analyzes temporal patterns to determine optimal fee for current time (higher during peak, lower during quiet periods).",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel short ID (e.g., '123x456x0')"
                    },
                    "base_fee": {
                        "type": "integer",
                        "description": "Current/base fee in ppm (default: 250)"
                    }
                },
                "required": ["node", "channel_id"]
            }
        ),
        Tool(
            name="hive_time_peak_hours",
            description="Get detected peak routing hours for a channel. Shows hours with above-average volume where fee increases may capture premium.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel short ID"
                    }
                },
                "required": ["node", "channel_id"]
            }
        ),
        Tool(
            name="hive_time_low_hours",
            description="Get detected low-activity hours for a channel. Shows hours with below-average volume where fee decreases may attract flow.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel short ID"
                    }
                },
                "required": ["node", "channel_id"]
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
            description="""Get competitor fee analysis - understand market positioning.

**When to use:** Before adjusting fees on high-volume channels, check competitive landscape.

**Shows for each analyzed peer:**
- Our fee vs competitor median fee
- Market position (underpricing, premium, competitive)
- Fee gap in ppm
- Recommendation: 'undercut' (we can raise), 'premium' (we're high), 'hold'

**Integration:** advisor_scan_opportunities uses this to identify fee adjustment opportunities.

**Action guidance:**
- Large positive gap (competitors higher): Opportunity to raise fees
- Large negative gap (we're higher): May be losing routes, consider reduction
- Competitive: Hold current fee, focus elsewhere""",
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
            description="""Get a pre-run context summary with situational awareness and memory across runs.

**When to use:** Call this at the START of every advisory session to establish context before taking any actions.

**Provides:**
- Revenue and capacity trends over the analysis period
- Velocity alerts for channels at risk of depletion/saturation
- Unresolved flags that need attention
- Recent AI decisions to avoid repeating advice
- Key performance indicators (KPIs) compared to baseline

**Why this matters:** Without context, you'll repeat the same observations and recommendations. This tool gives you "memory" so you can track progress and identify what's changed since last run.

**Best practice workflow:**
1. advisor_get_context_brief (understand current state)
2. advisor_scan_opportunities (see what needs attention)
3. Take targeted actions based on findings""",
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
        # Proactive Advisor Tools - Goal-driven autonomous management
        # =====================================================================
        Tool(
            name="advisor_run_cycle",
            description="""Run one complete proactive advisor cycle with comprehensive intelligence gathering.

**When to use:** Run this every 3 hours or when you need a full analysis with auto-execution of safe actions.

**What it does:**
1. Records state snapshot for historical tracking
2. Gathers comprehensive intelligence from ALL available systems:
   - Core: node info, channels, dashboard, profitability
   - Fleet coordination: defense warnings, internal competition, fee coordination
   - Predictive: anticipatory predictions, critical velocity
   - Strategic: positioning, yield, flow recommendations
   - Cost reduction: rebalance recommendations, circular flows
   - Collective warnings: ban candidates, rationalization
3. Checks goal progress and adjusts strategy
4. Scans 14 opportunity sources in parallel
5. Scores opportunities with learning adjustments
6. Auto-executes safe actions within daily budget
7. Queues risky actions for approval
8. Measures outcomes of past decisions (6-24h ago)
9. Plans priorities for next cycle

**Returns:** Comprehensive cycle result with opportunities found, actions taken, and next priorities.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to advise"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="advisor_run_cycle_all",
            description="""Run proactive advisor cycle on ALL nodes in the fleet in parallel.

**When to use:** For fleet-wide advisory reports. Runs advisor_run_cycle on every configured node simultaneously.

**Returns:** Combined results from all nodes with:
- Per-node cycle results
- Fleet-wide summary (total opportunities, actions, etc.)
- Aggregated health metrics""",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="advisor_get_goals",
            description="Get current advisor goals and progress. Shows what the advisor is optimizing for and whether it's on track.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (for context)"
                    },
                    "status": {
                        "type": "string",
                        "enum": ["active", "achieved", "failed", "abandoned"],
                        "description": "Filter by status (optional, defaults to all)"
                    }
                }
            }
        ),
        Tool(
            name="advisor_set_goal",
            description="Set or update an advisor goal. Goals drive the advisor's decision-making and prioritization.",
            inputSchema={
                "type": "object",
                "properties": {
                    "goal_type": {
                        "type": "string",
                        "enum": ["profitability", "routing_volume", "channel_health"],
                        "description": "Type of goal"
                    },
                    "target_metric": {
                        "type": "string",
                        "description": "Metric to optimize (e.g., 'roc_pct', 'underwater_pct', 'avg_balance_ratio')"
                    },
                    "current_value": {
                        "type": "number",
                        "description": "Current value of the metric"
                    },
                    "target_value": {
                        "type": "number",
                        "description": "Target value to achieve"
                    },
                    "deadline_days": {
                        "type": "integer",
                        "description": "Days to achieve the goal"
                    },
                    "priority": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 5,
                        "description": "Priority 1-5, higher = more important (default: 3)"
                    }
                },
                "required": ["goal_type", "target_metric", "target_value"]
            }
        ),
        Tool(
            name="advisor_get_learning",
            description="Get the advisor's learned parameters. Shows what the advisor has learned about which actions work, including action type confidence and opportunity success rates.",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="advisor_get_status",
            description="Get comprehensive advisor status including goals, learning summary, last cycle results, and daily budget.",
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
            name="advisor_get_cycle_history",
            description="Get history of advisor cycles. Shows past decisions, opportunities found, and outcomes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (optional, omit for all nodes)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum cycles to return (default: 10)"
                    }
                }
            }
        ),
        Tool(
            name="advisor_scan_opportunities",
            description="""Scan for optimization opportunities without executing any actions.

**When to use:** Use this for read-only analysis when you want to see what the advisor recommends without taking action.

**Scans 14 data sources in parallel:**
- Core: velocity alerts, profitability issues, time-based fees, imbalanced channels, config tuning
- Fleet coordination: defense warnings, internal competition
- Cost reduction: circular flows, rebalance recommendations
- Strategic: positioning opportunities, competitor analysis, rationalization
- Collective warnings: ban candidates

**Returns:**
- total_opportunities: Count of all opportunities found
- auto_execute_safe: Count that would be auto-executed
- queue_for_review: Count needing human review
- require_approval: Count needing explicit approval
- opportunities: Top 20 scored opportunities with details
- state_summary: Current node health metrics""",
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
        ),
        # =======================================================================
        # Phase 1: Yield Metrics Tools
        # =======================================================================
        Tool(
            name="yield_metrics",
            description="Get yield metrics for channels including ROI, capital efficiency, turn rate, and flow intensity. Use to identify which channels are performing well.",
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
                    },
                    "period_days": {
                        "type": "integer",
                        "description": "Analysis period in days (default: 30)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="yield_summary",
            description="Get fleet-wide yield summary including total revenue, average ROI, capital efficiency, and channel health distribution.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "period_days": {
                        "type": "integer",
                        "description": "Analysis period in days (default: 30)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="velocity_prediction",
            description="Predict channel state based on flow velocity. Shows depletion/saturation risk and recommended actions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID to predict"
                    },
                    "hours": {
                        "type": "integer",
                        "description": "Prediction horizon in hours (default: 24)"
                    }
                },
                "required": ["node", "channel_id"]
            }
        ),
        Tool(
            name="critical_velocity",
            description="Get channels with critical velocity - those depleting or filling rapidly. Returns channels predicted to deplete or saturate within threshold.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "threshold_hours": {
                        "type": "integer",
                        "description": "Alert threshold in hours (default: 24)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="internal_competition",
            description="""Detect internal competition between hive members.

**When to use:** Check before proposing fee changes to avoid counterproductive fee wars with fleet members.

**Shows:**
- Conflicts where multiple members compete for the same source/destination routes
- Wasted resources from internal competition
- Corridor ownership based on routing activity

**Integration:** The advisor_run_cycle automatically checks this when scanning for opportunities. Use standalone when evaluating specific fee decisions.""",
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
        # =======================================================================
        # Phase 2: Fee Coordination Tools
        # =======================================================================
        Tool(
            name="coord_fee_recommendation",
            description="""Get coordinated fee recommendation for a channel using fleet-wide intelligence.

**When to use:** Before making any fee change, call this to get the optimal fee that considers:
- Corridor assignment (who "owns" this route in the fleet)
- Pheromone signals (learned successful fees from past routing)
- Stigmergic markers (signals left by other members after routing attempts)
- Defensive adjustments (if peer has warnings)
- Balance state (depleting channels need different fees than saturated ones)

**Best practice:** Use this instead of manually calculating fees. It incorporates collective intelligence from the entire hive.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID to get recommendation for"
                    },
                    "current_fee": {
                        "type": "integer",
                        "description": "Current fee in ppm (default: 500)"
                    },
                    "local_balance_pct": {
                        "type": "number",
                        "description": "Current local balance percentage (default: 0.5)"
                    },
                    "source": {
                        "type": "string",
                        "description": "Source peer hint for corridor lookup"
                    },
                    "destination": {
                        "type": "string",
                        "description": "Destination peer hint for corridor lookup"
                    }
                },
                "required": ["node", "channel_id"]
            }
        ),
        Tool(
            name="corridor_assignments",
            description="Get flow corridor assignments for the fleet. Shows which member is primary for each (source, destination) pair to eliminate internal competition.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "force_refresh": {
                        "type": "boolean",
                        "description": "Force refresh of cached assignments (default: false)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="stigmergic_markers",
            description="Get stigmergic route markers from the fleet. Shows fee signals left by members after routing attempts for indirect coordination.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "source": {
                        "type": "string",
                        "description": "Filter by source peer"
                    },
                    "destination": {
                        "type": "string",
                        "description": "Filter by destination peer"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="defense_status",
            description="""Get mycelium defense system status - critical for avoiding bad peers.

**When to use:** Check BEFORE recommending any actions involving specific peers. This is part of the pre-cycle intelligence gathering.

**Shows:**
- Active warnings about draining peers (peers that consistently take liquidity without sending)
- Unreliable peers (high failure rates, force-close history)
- Defensive fee adjustments already applied
- Severity levels: info, warning, high, critical

**Integration:** advisor_run_cycle automatically incorporates this data. Cross-reference with ban_candidates for severe cases.

**Action guidance:**
- 'info' warnings: Monitor only
- 'warning' severity: Apply defensive fee policy
- 'high'/'critical': Consider channel closure or ban proposal""",
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
            name="ban_candidates",
            description="Get peers that should be considered for ban proposals. Uses accumulated warnings from local threat detection and peer reputation reports from hive members. Set auto_propose=true to automatically create ban proposals for severe cases.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "auto_propose": {
                        "type": "boolean",
                        "description": "If true, automatically create ban proposals for severe cases (default: false)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="accumulated_warnings",
            description="Get accumulated warning information for a specific peer. Combines local threat detection with aggregated peer reputation data from other hive members. Shows whether peer should be auto-banned.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Peer public key to check warnings for"
                    }
                },
                "required": ["node", "peer_id"]
            }
        ),
        Tool(
            name="pheromone_levels",
            description="Get pheromone levels for adaptive fee control. Shows the 'memory' of successful fees for channels.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Optional specific channel"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="fee_coordination_status",
            description="Get overall fee coordination status. Comprehensive view of all Phase 2 coordination systems including corridors, markers, and defense.",
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
        # Phase 3: Cost Reduction tools
        Tool(
            name="rebalance_recommendations",
            description="""Get predictive rebalance recommendations - proactive vs reactive liquidity management.

**When to use:** Include in analysis to identify channels that will need rebalancing BEFORE they become critical. Cheaper to rebalance proactively than when urgent.

**Uses:**
- Velocity prediction (flow rate trends)
- Historical patterns (temporal flow patterns)
- EV calculation (expected value of rebalancing)

**Returns recommendations with:**
- Source and destination channels
- Recommended amount
- Urgency level (high/medium/low)
- Expected ROI
- Confidence score

**Integration:** advisor_run_cycle checks this automatically. Use standalone when focusing on rebalancing strategy.

**Best practice:** Also call fleet_rebalance_path to check if cheaper internal routes exist.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "prediction_hours": {
                        "type": "integer",
                        "description": "Hours to predict ahead (default: 24)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="fleet_rebalance_path",
            description="Find internal fleet rebalance paths. Checks if rebalancing can be done through other fleet members at lower cost than market routes.",
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
                    }
                },
                "required": ["node", "from_channel", "to_channel", "amount_sats"]
            }
        ),
        Tool(
            name="circular_flow_status",
            description="Get circular flow detection status. Shows detected wasteful circular patterns (A→B→C→A) and their cost impact.",
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
            name="cost_reduction_status",
            description="Get overall cost reduction status. Comprehensive view of Phase 3 systems including predictive rebalancing, fleet routing, and circular flow detection.",
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
        # Routing Intelligence tools (Phase 4 - Cooperative Routing)
        Tool(
            name="routing_stats",
            description="Get collective routing intelligence statistics. Shows aggregated data from all hive members including path success rates, probe counts, and overall routing health.",
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
            name="route_suggest",
            description="Get route suggestions for a destination using hive intelligence. Uses collective routing data from all members to suggest optimal paths with success rates and latency estimates.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "destination": {
                        "type": "string",
                        "description": "Target node public key"
                    },
                    "amount_sats": {
                        "type": "integer",
                        "description": "Amount to route in satoshis (default: 100000)"
                    }
                },
                "required": ["node", "destination"]
            }
        ),
        # Channel Rationalization tools
        Tool(
            name="coverage_analysis",
            description="Analyze fleet coverage for redundant channels. Shows which fleet members have channels to the same peers and determines ownership based on routing activity (stigmergic markers).",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Specific peer to analyze (optional, omit for all redundant peers)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="close_recommendations",
            description="Get channel close recommendations for underperforming redundant channels. Uses stigmergic markers to determine ownership - recommends closes for members with <10% of the owner's routing activity. Part of the Hive covenant: members follow swarm intelligence.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "our_node_only": {
                        "type": "boolean",
                        "description": "If true, only return recommendations for this node"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="rationalization_summary",
            description="Get summary of channel rationalization analysis. Shows fleet coverage health: well-owned peers, contested peers, orphan peers (no routing activity), and close recommendations.",
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
            name="rationalization_status",
            description="Get channel rationalization status. Shows overall coverage health metrics and configuration thresholds.",
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
        # =============================================================================
        # Phase 5: Strategic Positioning Tools
        # =============================================================================
        Tool(
            name="valuable_corridors",
            description="Get high-value routing corridors for strategic positioning. Corridors are scored by: Volume × Margin × (1/Competition). Use this to identify where to position for maximum routing revenue.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "min_score": {
                        "type": "number",
                        "description": "Minimum value score to include (default: 0.05)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="exchange_coverage",
            description="Get priority exchange connectivity status. Shows which major Lightning exchanges (ACINQ, Kraken, Bitfinex, etc.) the fleet is connected to and which still need channels.",
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
            name="positioning_recommendations",
            description="Get channel open recommendations for strategic positioning. Recommends where to open channels for maximum routing value, considering existing fleet coverage and competition.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "count": {
                        "type": "integer",
                        "description": "Number of recommendations to return (default: 5)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="flow_recommendations",
            description="Get Physarum-inspired flow recommendations for channel lifecycle. Channels evolve based on flow like slime mold tubes: high flow → strengthen (splice in), low flow → atrophy (recommend close), young + low flow → stimulate (fee reduction).",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Specific channel, or omit for all non-hold recommendations"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="positioning_summary",
            description="Get summary of strategic positioning analysis. Shows high-value corridors, exchange coverage, and recommended actions for optimal fleet positioning.",
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
            name="positioning_status",
            description="Get strategic positioning status. Shows overall status, thresholds (strengthen/atrophy flow thresholds), and list of priority exchanges.",
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
        # Physarum Auto-Trigger Tools (Phase 7.2)
        # =====================================================================
        Tool(
            name="physarum_cycle",
            description="Execute one Physarum optimization cycle. Evaluates all channels and creates pending_actions for: high-flow channels (strengthen/splice-in), old low-flow channels (atrophy/close), young low-flow channels (stimulate/fee reduction). All actions go through governance approval.",
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
            name="physarum_status",
            description="Get Physarum auto-trigger status. Shows configuration (auto_strengthen/atrophy/stimulate enabled), thresholds (flow intensity triggers), rate limits (max actions per day/week), and current usage.",
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
        # Settlement Tools (BOLT12 Revenue Distribution)
        # =====================================================================
        Tool(
            name="settlement_register_offer",
            description="Register a BOLT12 offer for receiving settlement payments. Each hive member must register their offer to participate in revenue distribution.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Member's node public key"
                    },
                    "bolt12_offer": {
                        "type": "string",
                        "description": "BOLT12 offer string (starts with lno1...)"
                    }
                },
                "required": ["node", "peer_id", "bolt12_offer"]
            }
        ),
        Tool(
            name="settlement_generate_offer",
            description="Auto-generate and register a BOLT12 offer for a node. Creates a new BOLT12 offer for receiving settlement payments and registers it automatically. Use this for nodes that joined before automatic offer generation was implemented.",
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
            name="settlement_list_offers",
            description="List all registered BOLT12 offers for settlement. Shows which members have registered offers and can participate in revenue distribution.",
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
            name="settlement_calculate",
            description="Calculate fair shares for the current period without executing. Shows what each member would receive/pay based on: 40% capacity weight, 40% routing volume weight, 20% uptime weight.",
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
            name="settlement_execute",
            description="Execute settlement for the current period. Calculates fair shares and generates BOLT12 payments from members with surplus to members with deficit. Requires all participating members to have registered offers.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, calculate but don't execute payments (default: true)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="settlement_history",
            description="Get settlement history showing past periods, total fees distributed, and member participation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of periods to return (default: 10)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="settlement_period_details",
            description="Get detailed information about a specific settlement period including contributions, fair shares, and payments.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "period_id": {
                        "type": "integer",
                        "description": "Settlement period ID"
                    }
                },
                "required": ["node", "period_id"]
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
        elif name == "hive_propose_promotion":
            result = await handle_propose_promotion(arguments)
        elif name == "hive_vote_promotion":
            result = await handle_vote_promotion(arguments)
        elif name == "hive_pending_promotions":
            result = await handle_pending_promotions(arguments)
        elif name == "hive_execute_promotion":
            result = await handle_execute_promotion(arguments)
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
        elif name == "hive_bump_version":
            result = await handle_bump_version(arguments)
        elif name == "hive_gossip_stats":
            result = await handle_gossip_stats(arguments)
        # Splice coordination tools
        elif name == "hive_splice_check":
            result = await handle_splice_check(arguments)
        elif name == "hive_splice_recommendations":
            result = await handle_splice_recommendations(arguments)
        elif name == "hive_liquidity_intelligence":
            result = await handle_liquidity_intelligence(arguments)
        # Anticipatory Liquidity tools (Phase 7.1)
        elif name == "hive_anticipatory_status":
            result = await handle_anticipatory_status(arguments)
        elif name == "hive_detect_patterns":
            result = await handle_detect_patterns(arguments)
        elif name == "hive_predict_liquidity":
            result = await handle_predict_liquidity(arguments)
        elif name == "hive_anticipatory_predictions":
            result = await handle_anticipatory_predictions(arguments)
        # Time-Based Fee tools (Phase 7.4)
        elif name == "hive_time_fee_status":
            result = await handle_time_fee_status(arguments)
        elif name == "hive_time_fee_adjustment":
            result = await handle_time_fee_adjustment(arguments)
        elif name == "hive_time_peak_hours":
            result = await handle_time_peak_hours(arguments)
        elif name == "hive_time_low_hours":
            result = await handle_time_low_hours(arguments)
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
        # Proactive Advisor tools
        elif name == "advisor_run_cycle":
            result = await handle_advisor_run_cycle(arguments)
        elif name == "advisor_run_cycle_all":
            result = await handle_advisor_run_cycle_all(arguments)
        elif name == "advisor_get_goals":
            result = await handle_advisor_get_goals(arguments)
        elif name == "advisor_set_goal":
            result = await handle_advisor_set_goal(arguments)
        elif name == "advisor_get_learning":
            result = await handle_advisor_get_learning(arguments)
        elif name == "advisor_get_status":
            result = await handle_advisor_get_status(arguments)
        elif name == "advisor_get_cycle_history":
            result = await handle_advisor_get_cycle_history(arguments)
        elif name == "advisor_scan_opportunities":
            result = await handle_advisor_scan_opportunities(arguments)
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
        # Phase 1: Yield Metrics tools
        elif name == "yield_metrics":
            result = await handle_yield_metrics(arguments)
        elif name == "yield_summary":
            result = await handle_yield_summary(arguments)
        elif name == "velocity_prediction":
            result = await handle_velocity_prediction(arguments)
        elif name == "critical_velocity":
            result = await handle_critical_velocity(arguments)
        elif name == "internal_competition":
            result = await handle_internal_competition(arguments)
        # Phase 2: Fee Coordination tools
        elif name == "coord_fee_recommendation":
            result = await handle_fee_recommendation(arguments)
        elif name == "corridor_assignments":
            result = await handle_corridor_assignments(arguments)
        elif name == "stigmergic_markers":
            result = await handle_stigmergic_markers(arguments)
        elif name == "defense_status":
            result = await handle_defense_status(arguments)
        elif name == "ban_candidates":
            result = await handle_ban_candidates(arguments)
        elif name == "accumulated_warnings":
            result = await handle_accumulated_warnings(arguments)
        elif name == "pheromone_levels":
            result = await handle_pheromone_levels(arguments)
        elif name == "fee_coordination_status":
            result = await handle_fee_coordination_status(arguments)
        # Phase 3: Cost Reduction tools
        elif name == "rebalance_recommendations":
            result = await handle_rebalance_recommendations(arguments)
        elif name == "fleet_rebalance_path":
            result = await handle_fleet_rebalance_path(arguments)
        elif name == "circular_flow_status":
            result = await handle_circular_flow_status(arguments)
        elif name == "cost_reduction_status":
            result = await handle_cost_reduction_status(arguments)
        # Routing Intelligence tools
        elif name == "routing_stats":
            result = await handle_routing_stats(arguments)
        elif name == "route_suggest":
            result = await handle_route_suggest(arguments)
        # Channel Rationalization tools
        elif name == "coverage_analysis":
            result = await handle_coverage_analysis(arguments)
        elif name == "close_recommendations":
            result = await handle_close_recommendations(arguments)
        elif name == "rationalization_summary":
            result = await handle_rationalization_summary(arguments)
        elif name == "rationalization_status":
            result = await handle_rationalization_status(arguments)
        # Phase 5: Strategic Positioning
        elif name == "valuable_corridors":
            result = await handle_valuable_corridors(arguments)
        elif name == "exchange_coverage":
            result = await handle_exchange_coverage(arguments)
        elif name == "positioning_recommendations":
            result = await handle_positioning_recommendations(arguments)
        elif name == "flow_recommendations":
            result = await handle_flow_recommendations(arguments)
        elif name == "positioning_summary":
            result = await handle_positioning_summary(arguments)
        elif name == "positioning_status":
            result = await handle_positioning_status(arguments)
        # Physarum Auto-Trigger tools (Phase 7.2)
        elif name == "physarum_cycle":
            result = await handle_physarum_cycle(arguments)
        elif name == "physarum_status":
            result = await handle_physarum_status(arguments)
        # Settlement tools (BOLT12 Revenue Distribution)
        elif name == "settlement_register_offer":
            result = await handle_settlement_register_offer(arguments)
        elif name == "settlement_generate_offer":
            result = await handle_settlement_generate_offer(arguments)
        elif name == "settlement_list_offers":
            result = await handle_settlement_list_offers(arguments)
        elif name == "settlement_calculate":
            result = await handle_settlement_calculate(arguments)
        elif name == "settlement_execute":
            result = await handle_settlement_execute(arguments)
        elif name == "settlement_history":
            result = await handle_settlement_history(arguments)
        elif name == "settlement_period_details":
            result = await handle_settlement_period_details(arguments)
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


async def handle_propose_promotion(args: Dict) -> Dict:
    """Propose a neophyte for early promotion to member status."""
    node_name = args.get("node")
    target_peer_id = args.get("target_peer_id")

    if not node_name or not target_peer_id:
        return {"error": "node and target_peer_id are required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get our pubkey as the proposer
    info = await node.call("getinfo")
    proposer_peer_id = info.get("id")

    return await node.call("hive-propose-promotion",
                           target_peer_id=target_peer_id,
                           proposer_peer_id=proposer_peer_id)


async def handle_vote_promotion(args: Dict) -> Dict:
    """Vote to approve a neophyte's promotion to member."""
    node_name = args.get("node")
    target_peer_id = args.get("target_peer_id")

    if not node_name or not target_peer_id:
        return {"error": "node and target_peer_id are required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get our pubkey as the voter
    info = await node.call("getinfo")
    voter_peer_id = info.get("id")

    return await node.call("hive-vote-promotion",
                           target_peer_id=target_peer_id,
                           voter_peer_id=voter_peer_id)


async def handle_pending_promotions(args: Dict) -> Dict:
    """Get all pending manual promotion proposals."""
    node_name = args.get("node")

    if not node_name:
        return {"error": "node is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-pending-promotions")


async def handle_execute_promotion(args: Dict) -> Dict:
    """Execute a manual promotion if quorum has been reached."""
    node_name = args.get("node")
    target_peer_id = args.get("target_peer_id")

    if not node_name or not target_peer_id:
        return {"error": "node and target_peer_id are required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-execute-promotion", target_peer_id=target_peer_id)


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


async def handle_bump_version(args: Dict) -> Dict:
    """Bump the gossip state version for restart recovery."""
    node_name = args.get("node")
    version = args.get("version")

    if not version:
        return {"error": "version is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-bump-version", {"version": version})


async def handle_gossip_stats(args: Dict) -> Dict:
    """Get gossip statistics and state versions for debugging."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-gossip-stats", {})


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
# Anticipatory Liquidity Handlers (Phase 7.1)
# =============================================================================

async def handle_anticipatory_status(args: Dict) -> Dict:
    """
    Get anticipatory liquidity manager status.

    Shows pattern detection state, prediction cache, and configuration.
    """
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-anticipatory-status", {})


async def handle_detect_patterns(args: Dict) -> Dict:
    """
    Detect temporal patterns in channel flow.

    Analyzes historical flow data to find recurring patterns by
    hour-of-day and day-of-week that can predict future liquidity needs.
    """
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    force_refresh = args.get("force_refresh", False)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {"force_refresh": force_refresh}
    if channel_id:
        params["channel_id"] = channel_id

    result = await node.call("hive-detect-patterns", params)

    # Add helpful context
    if result.get("patterns"):
        patterns = result["patterns"]
        outbound_patterns = [p for p in patterns if p.get("direction") == "outbound"]
        inbound_patterns = [p for p in patterns if p.get("direction") == "inbound"]
        if outbound_patterns:
            result["ai_note"] = (
                f"Detected {len(outbound_patterns)} outbound (drain) patterns and "
                f"{len(inbound_patterns)} inbound patterns. "
                "Use these to anticipate rebalancing needs before they become urgent."
            )

    return result


async def handle_predict_liquidity(args: Dict) -> Dict:
    """
    Predict channel liquidity state N hours from now.

    Combines velocity analysis with temporal patterns to predict
    future balance and recommend preemptive rebalancing.
    """
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    hours_ahead = args.get("hours_ahead", 12)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    if not channel_id:
        return {"error": "channel_id is required"}

    result = await node.call("hive-predict-liquidity", {
        "channel_id": channel_id,
        "hours_ahead": hours_ahead
    })

    # Add actionable recommendations
    if result.get("recommended_action") == "preemptive_rebalance":
        urgency = result.get("urgency", "low")
        hours = result.get("hours_to_critical")
        if hours:
            result["ai_recommendation"] = (
                f"Urgency: {urgency}. Predicted to hit critical state in ~{hours:.0f} hours. "
                "Consider rebalancing now while fees are lower."
            )
    elif result.get("recommended_action") == "fee_adjustment":
        result["ai_recommendation"] = (
            "Fee adjustment recommended to attract/repel flow before imbalance worsens."
        )

    return result


async def handle_anticipatory_predictions(args: Dict) -> Dict:
    """
    Get liquidity predictions for all channels at risk.

    Returns channels with significant depletion or saturation risk,
    enabling proactive rebalancing before problems occur.
    """
    node_name = args.get("node")
    hours_ahead = args.get("hours_ahead", 12)
    min_risk = args.get("min_risk", 0.3)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-anticipatory-predictions", {
        "hours_ahead": hours_ahead,
        "min_risk": min_risk
    })

    # Summarize findings
    if result.get("predictions"):
        predictions = result["predictions"]
        critical = [p for p in predictions if p.get("urgency") in ["critical", "urgent"]]
        preemptive = [p for p in predictions if p.get("urgency") == "preemptive"]

        if critical:
            result["ai_summary"] = (
                f"{len(critical)} channels need urgent attention (depleting/saturating soon). "
                f"{len(preemptive)} channels are in preemptive window (good time to rebalance)."
            )
        elif preemptive:
            result["ai_summary"] = (
                f"No urgent issues. {len(preemptive)} channels in preemptive window - "
                "ideal time to rebalance at lower cost."
            )
        else:
            result["ai_summary"] = "All channels stable. No anticipatory action needed."

    return result


# =============================================================================
# Time-Based Fee Handlers (Phase 7.4)
# =============================================================================

async def handle_time_fee_status(args: Dict) -> Dict:
    """
    Get time-based fee adjustment status.

    Shows current time context, active adjustments, and configuration.
    """
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-time-fee-status", {})

    # Add AI summary
    if result.get("active_adjustments", 0) > 0:
        adjustments = result.get("adjustments", [])
        increases = [a for a in adjustments if a.get("adjustment_type") == "peak_increase"]
        decreases = [a for a in adjustments if a.get("adjustment_type") == "low_decrease"]
        result["ai_summary"] = (
            f"Time-based fees active: {len(increases)} peak increases, "
            f"{len(decreases)} low-activity decreases. "
            f"Current time: {result.get('current_hour', 0):02d}:00 UTC {result.get('current_day_name', '')}"
        )
    else:
        result["ai_summary"] = (
            f"No time-based adjustments active at "
            f"{result.get('current_hour', 0):02d}:00 UTC {result.get('current_day_name', '')}. "
            f"System {'enabled' if result.get('enabled') else 'disabled'}."
        )

    return result


async def handle_time_fee_adjustment(args: Dict) -> Dict:
    """
    Get time-based fee adjustment for a specific channel.

    Analyzes temporal patterns to determine optimal fee for current time.
    """
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    base_fee = args.get("base_fee", 250)

    if not channel_id:
        return {"error": "channel_id is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-time-fee-adjustment", {
        "channel_id": channel_id,
        "base_fee": base_fee
    })

    # Add AI summary
    if result.get("adjustment_type") == "peak_increase":
        result["ai_summary"] = (
            f"Peak hour detected: fee increased from {result.get('base_fee_ppm')} to "
            f"{result.get('adjusted_fee_ppm')} ppm (+{result.get('adjustment_pct', 0):.1f}%). "
            f"Intensity: {result.get('pattern_intensity', 0):.0%}"
        )
    elif result.get("adjustment_type") == "low_decrease":
        result["ai_summary"] = (
            f"Low activity detected: fee decreased from {result.get('base_fee_ppm')} to "
            f"{result.get('adjusted_fee_ppm')} ppm ({result.get('adjustment_pct', 0):.1f}%). "
            f"May attract flow."
        )
    else:
        result["ai_summary"] = (
            f"No time adjustment for channel {channel_id} at current time. "
            f"Base fee {base_fee} ppm unchanged."
        )

    return result


async def handle_time_peak_hours(args: Dict) -> Dict:
    """
    Get detected peak routing hours for a channel.

    Shows hours with above-average volume where fee increases capture premium.
    """
    node_name = args.get("node")
    channel_id = args.get("channel_id")

    if not channel_id:
        return {"error": "channel_id is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-time-peak-hours", {"channel_id": channel_id})

    # Add AI summary
    count = result.get("count", 0)
    if count > 0:
        hours = result.get("peak_hours", [])
        top_hours = hours[:3]
        hour_strs = [
            f"{h.get('hour', 0):02d}:00 {h.get('day_name', 'Any')} ({h.get('direction', 'both')})"
            for h in top_hours
        ]
        result["ai_summary"] = (
            f"Detected {count} peak hours for channel {channel_id}. "
            f"Top periods: {', '.join(hour_strs)}. "
            "Consider fee increases during these times."
        )
    else:
        result["ai_summary"] = (
            f"No peak hours detected for channel {channel_id}. "
            "Need more flow history for pattern detection."
        )

    return result


async def handle_time_low_hours(args: Dict) -> Dict:
    """
    Get detected low-activity hours for a channel.

    Shows hours with below-average volume where fee decreases may help.
    """
    node_name = args.get("node")
    channel_id = args.get("channel_id")

    if not channel_id:
        return {"error": "channel_id is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-time-low-hours", {"channel_id": channel_id})

    # Add AI summary
    count = result.get("count", 0)
    if count > 0:
        hours = result.get("low_hours", [])
        top_hours = hours[:3]
        hour_strs = [
            f"{h.get('hour', 0):02d}:00 {h.get('day_name', 'Any')}"
            for h in top_hours
        ]
        result["ai_summary"] = (
            f"Detected {count} low-activity periods for channel {channel_id}. "
            f"Quietest: {', '.join(hour_strs)}. "
            "Consider fee decreases to attract flow."
        )
    else:
        result["ai_summary"] = (
            f"No low-activity patterns detected for channel {channel_id}. "
            "Channel may have consistent activity or need more history."
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
# Proactive Advisor Handlers
# =============================================================================

# Import proactive advisor modules (lazy import to avoid circular deps)
_proactive_advisor = None
_goal_manager = None
_learning_engine = None
_opportunity_scanner = None


def _get_proactive_advisor():
    """Lazy-load proactive advisor components."""
    global _proactive_advisor, _goal_manager, _learning_engine, _opportunity_scanner

    if _proactive_advisor is None:
        try:
            from goal_manager import GoalManager
            from learning_engine import LearningEngine
            from opportunity_scanner import OpportunityScanner
            from proactive_advisor import ProactiveAdvisor

            db = ensure_advisor_db()
            _goal_manager = GoalManager(db)
            _learning_engine = LearningEngine(db)

            # Create a simple MCP client wrapper
            class MCPClientWrapper:
                # Map tool names to handler names (some handlers drop the prefix)
                TOOL_TO_HANDLER = {
                    "hive_node_info": "handle_node_info",
                    "hive_channels": "handle_channels",
                    "hive_status": "handle_hive_status",
                    "hive_pending_actions": "handle_pending_actions",
                    "hive_set_fees": "handle_set_fees",
                }

                async def call(self, tool_name, params):
                    # Route to internal handlers
                    handler_name = self.TOOL_TO_HANDLER.get(tool_name, f"handle_{tool_name}")
                    handler = globals().get(handler_name)
                    if handler:
                        return await handler(params)
                    return {"error": f"Unknown tool: {tool_name}"}

            mcp_client = MCPClientWrapper()
            _opportunity_scanner = OpportunityScanner(mcp_client, db)
            _proactive_advisor = ProactiveAdvisor(mcp_client, db)

        except ImportError as e:
            logger.error(f"Failed to import proactive advisor modules: {e}")
            return None

    return _proactive_advisor


async def handle_advisor_run_cycle(args: Dict) -> Dict:
    """Run one complete proactive advisor cycle."""
    node_name = args.get("node")
    if not node_name:
        return {"error": "node is required"}

    advisor = _get_proactive_advisor()
    if not advisor:
        return {"error": "Proactive advisor modules not available"}

    try:
        result = await advisor.run_cycle(node_name)
        return result.to_dict()
    except Exception as e:
        logger.exception("Error running advisor cycle")
        return {"error": f"Failed to run cycle: {str(e)}"}


async def handle_advisor_run_cycle_all(args: Dict) -> Dict:
    """Run proactive advisor cycle on ALL nodes in the fleet in parallel."""
    advisor = _get_proactive_advisor()
    if not advisor:
        return {"error": "Proactive advisor modules not available"}

    # Get all node names
    node_names = list(fleet.nodes.keys())
    if not node_names:
        return {"error": "No nodes configured in fleet"}

    # Run cycles in parallel
    async def run_node_cycle(node_name: str) -> Dict:
        try:
            result = await advisor.run_cycle(node_name)
            return {"node": node_name, "success": True, "result": result.to_dict()}
        except Exception as e:
            logger.exception(f"Error running advisor cycle on {node_name}")
            return {"node": node_name, "success": False, "error": str(e)}

    results = await asyncio.gather(*[run_node_cycle(name) for name in node_names])

    # Aggregate results
    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]

    # Calculate fleet-wide summary
    total_opportunities = sum(
        r.get("result", {}).get("opportunities_found", 0) for r in successful
    )
    total_auto_executed = sum(
        r.get("result", {}).get("auto_executed_count", 0) for r in successful
    )
    total_queued = sum(
        r.get("result", {}).get("queued_count", 0) for r in successful
    )
    total_channels = sum(
        r.get("result", {}).get("node_state_summary", {}).get("channel_count", 0)
        for r in successful
    )

    # Collect all strategy adjustments
    all_adjustments = []
    for r in successful:
        node = r.get("node")
        adjustments = r.get("result", {}).get("strategy_adjustments", [])
        for adj in adjustments:
            all_adjustments.append(f"[{node}] {adj}")

    # Collect opportunities by type across fleet
    fleet_opportunities = {}
    for r in successful:
        for opp_type, count in r.get("result", {}).get("opportunities_by_type", {}).items():
            fleet_opportunities[opp_type] = fleet_opportunities.get(opp_type, 0) + count

    return {
        "fleet_summary": {
            "nodes_processed": len(successful),
            "nodes_failed": len(failed),
            "total_channels": total_channels,
            "total_opportunities": total_opportunities,
            "total_auto_executed": total_auto_executed,
            "total_queued": total_queued,
            "opportunities_by_type": fleet_opportunities,
            "strategy_adjustments": all_adjustments
        },
        "node_results": results,
        "failed_nodes": [r.get("node") for r in failed] if failed else []
    }


async def handle_advisor_get_goals(args: Dict) -> Dict:
    """Get current advisor goals."""
    db = ensure_advisor_db()
    status = args.get("status")

    goals = db.get_goals(status=status)

    return {
        "count": len(goals),
        "goals": goals
    }


async def handle_advisor_set_goal(args: Dict) -> Dict:
    """Set or update an advisor goal."""
    import time as time_module

    db = ensure_advisor_db()

    goal_type = args.get("goal_type")
    target_metric = args.get("target_metric")
    target_value = args.get("target_value")

    if not goal_type or not target_metric or target_value is None:
        return {"error": "goal_type, target_metric, and target_value are required"}

    now = int(time_module.time())
    goal = {
        "goal_id": f"{target_metric}_{now}",
        "goal_type": goal_type,
        "target_metric": target_metric,
        "current_value": args.get("current_value", 0),
        "target_value": target_value,
        "deadline_days": args.get("deadline_days", 30),
        "created_at": now,
        "priority": args.get("priority", 3),
        "checkpoints": [],
        "status": "active"
    }

    db.save_goal(goal)

    return {
        "success": True,
        "goal_id": goal["goal_id"],
        "message": f"Goal created: {goal_type} - {target_metric} to {target_value}"
    }


async def handle_advisor_get_learning(args: Dict) -> Dict:
    """Get learned parameters."""
    advisor = _get_proactive_advisor()
    if not advisor:
        # Fallback to raw database query
        db = ensure_advisor_db()
        params = db.get_learning_params()
        return {
            "action_type_confidence": params.get("action_type_confidence", {}),
            "opportunity_success_rates": params.get("opportunity_success_rates", {}),
            "total_outcomes_measured": params.get("total_outcomes_measured", 0),
            "overall_success_rate": params.get("overall_success_rate", 0.5)
        }

    return advisor.learning_engine.get_learning_summary()


async def handle_advisor_get_status(args: Dict) -> Dict:
    """Get comprehensive advisor status."""
    node_name = args.get("node")
    if not node_name:
        return {"error": "node is required"}

    advisor = _get_proactive_advisor()
    if not advisor:
        return {"error": "Proactive advisor modules not available"}

    try:
        return await advisor.get_status(node_name)
    except Exception as e:
        return {"error": f"Failed to get status: {str(e)}"}


async def handle_advisor_get_cycle_history(args: Dict) -> Dict:
    """Get history of advisor cycles."""
    db = ensure_advisor_db()

    node_name = args.get("node")
    limit = args.get("limit", 10)

    cycles = db.get_recent_cycles(node_name, limit)

    return {
        "count": len(cycles),
        "cycles": cycles
    }


async def handle_advisor_scan_opportunities(args: Dict) -> Dict:
    """Scan for optimization opportunities without executing."""
    node_name = args.get("node")
    if not node_name:
        return {"error": "node is required"}

    advisor = _get_proactive_advisor()
    if not advisor:
        return {"error": "Proactive advisor modules not available"}

    try:
        # Get node state
        state = await advisor._analyze_node_state(node_name)

        # Scan for opportunities
        opportunities = await advisor.scanner.scan_all(node_name, state)

        # Score them
        scored = advisor._score_opportunities(opportunities, state)

        # Classify
        auto, queue, require = advisor.scanner.filter_safe_opportunities(scored)

        return {
            "node": node_name,
            "total_opportunities": len(opportunities),
            "auto_execute_safe": len(auto),
            "queue_for_review": len(queue),
            "require_approval": len(require),
            "opportunities": [opp.to_dict() for opp in scored[:20]],  # Top 20
            "state_summary": state.get("summary", {})
        }
    except Exception as e:
        logger.exception("Error scanning opportunities")
        return {"error": f"Failed to scan opportunities: {str(e)}"}


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
# Phase 1: Yield Metrics Handlers
# =============================================================================

async def handle_yield_metrics(args: Dict) -> Dict:
    """Get yield metrics for channels."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    period_days = args.get("period_days", 30)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {"period_days": period_days}
    if channel_id:
        params["channel_id"] = channel_id

    return await node.call("hive-yield-metrics", params)


async def handle_yield_summary(args: Dict) -> Dict:
    """Get fleet-wide yield summary."""
    node_name = args.get("node")
    period_days = args.get("period_days", 30)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-yield-summary", {"period_days": period_days})


async def handle_velocity_prediction(args: Dict) -> Dict:
    """Predict channel state based on flow velocity."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    hours = args.get("hours", 24)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    if not channel_id:
        return {"error": "channel_id is required"}

    return await node.call("hive-velocity-prediction", {
        "channel_id": channel_id,
        "hours": hours
    })


async def handle_critical_velocity(args: Dict) -> Dict:
    """Get channels with critical velocity."""
    node_name = args.get("node")
    threshold_hours = args.get("threshold_hours", 24)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-critical-velocity", {
        "threshold_hours": threshold_hours
    })


async def handle_internal_competition(args: Dict) -> Dict:
    """Detect internal competition between hive members."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-internal-competition", {})


# =============================================================================
# Phase 2: Fee Coordination Handlers
# =============================================================================

async def handle_fee_recommendation(args: Dict) -> Dict:
    """Get coordinated fee recommendation for a channel."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    current_fee = args.get("current_fee", 500)
    local_balance_pct = args.get("local_balance_pct", 0.5)
    source = args.get("source")
    destination = args.get("destination")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    if not channel_id:
        return {"error": "channel_id is required"}

    params = {
        "channel_id": channel_id,
        "current_fee": current_fee,
        "local_balance_pct": local_balance_pct
    }
    if source:
        params["source"] = source
    if destination:
        params["destination"] = destination

    return await node.call("hive-coord-fee-recommendation", params)


async def handle_corridor_assignments(args: Dict) -> Dict:
    """Get flow corridor assignments for the fleet."""
    node_name = args.get("node")
    force_refresh = args.get("force_refresh", False)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-corridor-assignments", {
        "force_refresh": force_refresh
    })


async def handle_stigmergic_markers(args: Dict) -> Dict:
    """Get stigmergic route markers from the fleet."""
    node_name = args.get("node")
    source = args.get("source")
    destination = args.get("destination")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if source:
        params["source"] = source
    if destination:
        params["destination"] = destination

    return await node.call("hive-stigmergic-markers", params)


async def handle_defense_status(args: Dict) -> Dict:
    """Get mycelium defense system status."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-defense-status", {})


async def handle_ban_candidates(args: Dict) -> Dict:
    """Get peers that should be considered for ban proposals."""
    node_name = args.get("node")
    auto_propose = args.get("auto_propose", False)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-ban-candidates", {"auto_propose": auto_propose})

    # Add AI-friendly note
    candidates = result.get("ban_candidates", [])
    if candidates:
        result["ai_note"] = (
            f"Found {len(candidates)} ban candidates. "
            f"Most severe: {candidates[0].get('peer_id', 'unknown')[:16]}... "
            f"with severity {candidates[0].get('severity_weighted', 0):.1f}. "
            f"Use auto_propose=true to automatically create ban proposals for severe cases."
        )
    else:
        result["ai_note"] = "No ban candidates found. The fleet has no peers with accumulated warnings meeting the ban threshold."

    return result


async def handle_accumulated_warnings(args: Dict) -> Dict:
    """Get accumulated warning information for a specific peer."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")

    if not peer_id:
        return {"error": "peer_id is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-accumulated-warnings", {"peer_id": peer_id})

    # Add AI-friendly note
    severity = result.get("severity_weighted", 0)
    reporters = result.get("total_reporters", 0)
    should_ban = result.get("should_auto_ban", False)

    if should_ban:
        result["ai_note"] = (
            f"ALERT: Peer {peer_id[:16]}... exceeds auto-ban threshold. "
            f"Severity: {severity:.1f}, Reporters: {reporters}. "
            f"Reason: {result.get('auto_ban_reason', 'Multiple severe warnings')}. "
            f"Use ban_candidates with auto_propose=true to create ban proposal."
        )
    elif severity > 0:
        result["ai_note"] = (
            f"Peer {peer_id[:16]}... has warnings but below ban threshold. "
            f"Severity: {severity:.1f}, Reporters: {reporters}. "
            f"Ban threshold is 2.0 severity or 3+ reporters with same warning."
        )
    else:
        result["ai_note"] = f"Peer {peer_id[:16]}... has no accumulated warnings."

    return result


async def handle_pheromone_levels(args: Dict) -> Dict:
    """Get pheromone levels for adaptive fee control."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if channel_id:
        params["channel_id"] = channel_id

    return await node.call("hive-pheromone-levels", params)


async def handle_fee_coordination_status(args: Dict) -> Dict:
    """Get overall fee coordination status."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-fee-coordination-status", {})


# =============================================================================
# Phase 3: Cost Reduction Handlers
# =============================================================================

async def handle_rebalance_recommendations(args: Dict) -> Dict:
    """Get predictive rebalance recommendations."""
    node_name = args.get("node")
    prediction_hours = args.get("prediction_hours", 24)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-rebalance-recommendations", {
        "prediction_hours": prediction_hours
    })


async def handle_fleet_rebalance_path(args: Dict) -> Dict:
    """Find internal fleet rebalance paths."""
    node_name = args.get("node")
    from_channel = args.get("from_channel")
    to_channel = args.get("to_channel")
    amount_sats = args.get("amount_sats")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-fleet-rebalance-path", {
        "from_channel": from_channel,
        "to_channel": to_channel,
        "amount_sats": amount_sats
    })


async def handle_circular_flow_status(args: Dict) -> Dict:
    """Get circular flow detection status."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-circular-flow-status", {})


async def handle_cost_reduction_status(args: Dict) -> Dict:
    """Get overall cost reduction status."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-cost-reduction-status", {})


# =============================================================================
# Routing Intelligence Handlers (Phase 4 - Cooperative Routing)
# =============================================================================

async def handle_routing_stats(args: Dict) -> Dict:
    """Get collective routing intelligence statistics."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-routing-stats", {})

    # Add AI-friendly note
    paths = result.get("paths_tracked", 0)
    probes = result.get("total_probes", 0)
    success_rate = result.get("overall_success_rate", 0)

    if probes > 0:
        result["ai_note"] = (
            f"Routing intelligence: {paths} paths tracked from {probes} probes. "
            f"Overall success rate: {success_rate * 100:.1f}%. "
            f"High quality paths (>90% success): {result.get('high_quality_paths', 0)}. "
            "Use route_suggest to get recommendations for specific destinations."
        )
    else:
        result["ai_note"] = (
            "No routing probes collected yet. Route probes are shared between hive members "
            "to build collective routing intelligence. Data will accumulate over time."
        )

    return result


async def handle_route_suggest(args: Dict) -> Dict:
    """Get route suggestions for a destination using hive intelligence."""
    node_name = args.get("node")
    destination = args.get("destination")
    amount_sats = args.get("amount_sats", 100000)

    if not destination:
        return {"error": "destination is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-route-suggest", {
        "destination": destination,
        "amount_sats": amount_sats
    })

    # Add AI-friendly note
    route_count = result.get("route_count", 0)
    routes = result.get("routes", [])

    if route_count > 0:
        best = routes[0] if routes else {}
        result["ai_note"] = (
            f"Found {route_count} routes to {destination[:16]}... "
            f"Best route has {best.get('success_rate', 0) * 100:.1f}% success rate, "
            f"~{best.get('expected_latency_ms', 0)}ms latency, "
            f"confidence: {best.get('confidence', 0) * 100:.0f}%."
        )
    else:
        result["ai_note"] = (
            f"No routes found to {destination[:16]}... in hive routing intelligence. "
            "Route data is built from shared probes - this destination may not have been probed yet."
        )

    return result


# =============================================================================
# Channel Rationalization Handlers
# =============================================================================

async def handle_coverage_analysis(args: Dict) -> Dict:
    """Analyze fleet coverage for redundant channels."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if peer_id:
        params["peer_id"] = peer_id

    return await node.call("hive-coverage-analysis", params)


async def handle_close_recommendations(args: Dict) -> Dict:
    """Get channel close recommendations for underperforming redundant channels."""
    node_name = args.get("node")
    our_node_only = args.get("our_node_only", False)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-close-recommendations", {
        "our_node_only": our_node_only
    })


async def handle_rationalization_summary(args: Dict) -> Dict:
    """Get summary of channel rationalization analysis."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-rationalization-summary", {})


async def handle_rationalization_status(args: Dict) -> Dict:
    """Get channel rationalization status."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-rationalization-status", {})


# =============================================================================
# Phase 5: Strategic Positioning Handlers
# =============================================================================

async def handle_valuable_corridors(args: Dict) -> Dict:
    """Get high-value routing corridors for strategic positioning."""
    node_name = args.get("node")
    min_score = args.get("min_score", 0.05)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-valuable-corridors", {"min_score": min_score})


async def handle_exchange_coverage(args: Dict) -> Dict:
    """Get priority exchange connectivity status."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-exchange-coverage", {})


async def handle_positioning_recommendations(args: Dict) -> Dict:
    """Get channel open recommendations for strategic positioning."""
    node_name = args.get("node")
    count = args.get("count", 5)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-positioning-recommendations", {"count": count})


async def handle_flow_recommendations(args: Dict) -> Dict:
    """Get Physarum-inspired flow recommendations for channel lifecycle."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if channel_id:
        params["channel_id"] = channel_id

    return await node.call("hive-flow-recommendations", params)


async def handle_positioning_summary(args: Dict) -> Dict:
    """Get summary of strategic positioning analysis."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-positioning-summary", {})


async def handle_positioning_status(args: Dict) -> Dict:
    """Get strategic positioning status."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-positioning-status", {})


# =============================================================================
# Physarum Auto-Trigger Handlers (Phase 7.2)
# =============================================================================

async def handle_physarum_cycle(args: Dict) -> Dict:
    """
    Execute one Physarum optimization cycle.

    Evaluates channels and creates pending_actions for lifecycle changes.
    """
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-physarum-cycle", {})

    # Add helpful summary
    if result.get("actions_created"):
        actions = result["actions_created"]
        strengthen = [a for a in actions if a.get("action_type") == "physarum_strengthen"]
        atrophy = [a for a in actions if a.get("action_type") == "physarum_atrophy"]
        stimulate = [a for a in actions if a.get("action_type") == "physarum_stimulate"]

        summary_parts = []
        if strengthen:
            summary_parts.append(f"{len(strengthen)} splice-in proposals")
        if atrophy:
            summary_parts.append(f"{len(atrophy)} close recommendations")
        if stimulate:
            summary_parts.append(f"{len(stimulate)} fee reduction proposals")

        if summary_parts:
            result["ai_summary"] = (
                f"Physarum cycle created: {', '.join(summary_parts)}. "
                "Review in pending_actions and approve/reject."
            )
    else:
        result["ai_summary"] = "Physarum cycle completed. No actions needed - all channels within optimal range."

    return result


async def handle_physarum_status(args: Dict) -> Dict:
    """
    Get Physarum auto-trigger status.

    Shows configuration, thresholds, rate limits, and current usage.
    """
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-physarum-status", {})

    # Add configuration guidance
    if result.get("auto_strengthen_enabled") and result.get("auto_atrophy_enabled") is False:
        result["ai_note"] = (
            "Auto-atrophy is disabled (safe default). "
            "Close recommendations always require human approval."
        )

    return result


# =============================================================================
# Settlement Handlers (BOLT12 Revenue Distribution)
# =============================================================================
# Settlement database is managed remotely by cl-hive plugin on each node.
# All settlement operations are performed via remote RPC calls.
# =============================================================================


async def handle_settlement_register_offer(args: Dict) -> Dict:
    """Register a BOLT12 offer for receiving settlement payments."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    bolt12_offer = args.get("bolt12_offer")

    if not peer_id:
        return {"error": "peer_id is required"}
    if not bolt12_offer:
        return {"error": "bolt12_offer is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-settlement-register-offer", {
        "peer_id": peer_id,
        "bolt12_offer": bolt12_offer
    })

    if "error" not in result:
        result["ai_note"] = (
            f"Offer registered for {peer_id[:16]}... "
            "This member can now participate in revenue settlement."
        )

    return result


async def handle_settlement_generate_offer(args: Dict) -> Dict:
    """Auto-generate and register a BOLT12 offer for a node."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-settlement-generate-offer", {})

    if "error" not in result:
        status = result.get("status", "unknown")
        if status == "already_registered":
            result["ai_note"] = "This node already has a registered settlement offer."
        elif status == "generated_and_registered":
            result["ai_note"] = (
                "Successfully generated and registered a BOLT12 offer for settlement. "
                "This node can now participate in revenue distribution."
            )

    return result


async def handle_settlement_list_offers(args: Dict) -> Dict:
    """List all registered BOLT12 offers."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-settlement-list-offers", {})

    if "error" in result:
        return result

    offers = result.get("offers", [])
    active = [o for o in offers if o.get("active")]
    inactive = [o for o in offers if not o.get("active")]

    return {
        "total_offers": len(offers),
        "active_offers": len(active),
        "inactive_offers": len(inactive),
        "offers": offers,
        "ai_note": (
            f"{len(active)} members have registered offers and can participate in settlement. "
            f"{len(inactive)} offers are deactivated."
        )
    }


async def handle_settlement_calculate(args: Dict) -> Dict:
    """Calculate fair shares for the current period without executing."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-settlement-calculate", {})
    except Exception as e:
        return {"error": f"Failed to calculate settlement: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly note
    fair_shares = result.get("fair_shares", [])
    surplus_members = [r for r in fair_shares if r.get("balance", 0) < 0]
    deficit_members = [r for r in fair_shares if r.get("balance", 0) > 0]
    payments = result.get("payments_required", [])

    result["ai_note"] = (
        f"Settlement calculation complete. {len(surplus_members)} members earned more than fair share "
        f"and would pay {len(deficit_members)} members who earned less. "
        f"Total of {len(payments)} payments totaling {sum(p.get('amount_sats', 0) for p in payments)} sats."
    )

    return result


async def handle_settlement_execute(args: Dict) -> Dict:
    """Execute settlement for the current period."""
    node_name = args.get("node")
    dry_run = args.get("dry_run", True)  # Default to dry run for safety

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-settlement-execute", {"dry_run": dry_run})
    except Exception as e:
        return {"error": f"Failed to execute settlement: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly note
    if dry_run:
        result["ai_note"] = (
            "DRY RUN - No payments executed. "
            "Set dry_run=false to execute actual payments. "
            "Ensure all participating members have registered BOLT12 offers first."
        )
    else:
        payments = result.get("payments_executed", [])
        result["ai_note"] = (
            f"Settlement executed. {len(payments)} BOLT12 payments initiated."
        )

    return result


async def handle_settlement_history(args: Dict) -> Dict:
    """Get settlement history."""
    node_name = args.get("node")
    limit = args.get("limit", 10)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-settlement-history", {"limit": limit})
    except Exception as e:
        return {"error": f"Failed to get settlement history: {e}"}

    if "error" in result:
        return result

    periods = result.get("settlement_periods", [])
    result["ai_note"] = f"Showing last {len(periods)} settlement periods."

    return result


async def handle_settlement_period_details(args: Dict) -> Dict:
    """Get detailed information about a specific settlement period."""
    node_name = args.get("node")
    period_id = args.get("period_id")

    if period_id is None:
        return {"error": "period_id is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-settlement-period-details", {"period_id": period_id})
    except Exception as e:
        return {"error": f"Failed to get period details: {e}"}

    return result


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
