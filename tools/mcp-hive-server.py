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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

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
                    },
                    "status": {
                        "type": "string",
                        "enum": ["pending", "approved", "rejected", "executed"],
                        "description": "Filter by status (default: pending)"
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
    status = args.get("status", "pending")

    if node_name:
        node = fleet.get_node(node_name)
        if not node:
            return {"error": f"Unknown node: {node_name}"}
        result = await node.call("hive-pending-actions", {"status": status})
        return {node_name: result}
    else:
        results = {}
        for name, node in fleet.nodes.items():
            results[name] = await node.call("hive-pending-actions", {"status": status})
        return results


async def handle_approve_action(args: Dict) -> Dict:
    """Approve a pending action."""
    node_name = args.get("node")
    action_id = args.get("action_id")
    reason = args.get("reason", "Approved by Claude Code")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-approve-action", {
        "action_id": action_id,
        "reason": reason
    })


async def handle_reject_action(args: Dict) -> Dict:
    """Reject a pending action."""
    node_name = args.get("node")
    action_id = args.get("action_id")
    reason = args.get("reason")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-reject-action", {
        "action_id": action_id,
        "reason": reason
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
                    pending = await node.call("hive-pending-actions", {"status": "pending"})
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
                    pending = await node.call("hive-pending-actions", {"status": "pending"})

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
                pending = await node.call("hive-pending-actions", {"status": "pending"})

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
