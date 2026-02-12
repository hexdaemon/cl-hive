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
import re
import ssl
import sys
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from urllib.parse import urlparse

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
STRATEGY_MAX_CHARS = int(os.environ.get("HIVE_STRATEGY_MAX_CHARS", "4000"))

# TLS safety controls
HIVE_ALLOW_INSECURE_TLS = os.environ.get("HIVE_ALLOW_INSECURE_TLS", "false").lower() == "true"
# Allow cleartext REST to non-local hosts (not recommended)
HIVE_ALLOW_INSECURE_HTTP = os.environ.get("HIVE_ALLOW_INSECURE_HTTP", "false").lower() == "true"
# Normalize tool responses (wrap in ok/data or ok/error)
HIVE_NORMALIZE_RESPONSES = os.environ.get("HIVE_NORMALIZE_RESPONSES", "true").lower() == "true"

# Configurable timeouts
HIVE_HTTP_TIMEOUT = float(os.environ.get("HIVE_HTTP_TIMEOUT", "30"))
HIVE_DOCKER_TIMEOUT = float(os.environ.get("HIVE_DOCKER_TIMEOUT", "30"))

# Optional RPC method allowlist (path to JSON file with list of allowed methods)
HIVE_ALLOWED_METHODS_FILE = os.environ.get("HIVE_ALLOWED_METHODS")
_allowed_methods: Optional[set] = None


def _check_method_allowed(method: str) -> bool:
    """Check if an RPC method is allowed by the optional allowlist."""
    global _allowed_methods
    if _allowed_methods is None:
        if not HIVE_ALLOWED_METHODS_FILE:
            return True  # No allowlist = allow all
        try:
            with open(HIVE_ALLOWED_METHODS_FILE) as f:
                _allowed_methods = set(json.load(f))
        except Exception:
            # Parse error: deny all and stop retrying on every call
            _allowed_methods = set()
            return False
    return method in _allowed_methods


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
        logger.debug(f"Strategy dir not set; skipping strategy load for {name}")
        return ""
    # Reject names with path traversal characters
    if not re.match(r'^[a-zA-Z0-9_-]+$', name):
        logger.warning(f"Strategy name rejected (invalid chars): {name!r}")
        return ""
    path = os.path.join(STRATEGY_DIR, f"{name}.md")
    # Resolve and enforce directory boundary
    resolved = os.path.realpath(path)
    strategy_root = os.path.realpath(STRATEGY_DIR)
    if not resolved.startswith(strategy_root + os.sep) and resolved != strategy_root:
        logger.warning(f"Strategy path escaped directory: {name!r}")
        return ""
    try:
        with open(resolved, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read().strip()
            if len(content) > STRATEGY_MAX_CHARS:
                content = content[:STRATEGY_MAX_CHARS].rstrip() + "\n\n[truncated]"
            logger.debug(f"Loaded strategy prompt: {name}")
            return "\n\n" + content
    except FileNotFoundError:
        logger.debug(f"Strategy file not found: {resolved}")
        return ""
    except Exception as e:
        logger.warning(f"Error loading strategy {name}: {e}")
        return ""


# =============================================================================
# Node Connection
# =============================================================================

def _is_local_host(hostname: str) -> bool:
    return hostname in {"127.0.0.1", "localhost", "::1"}



def _validate_node_config(node_config: Dict, node_mode: str) -> Optional[str]:
    name = node_config.get("name")
    if not name:
        return "Node missing required 'name' field."

    if node_mode == "docker":
        if not node_config.get("docker_container"):
            return f"Node '{name}' is docker mode but missing docker_container."
        return None

    rest_url = node_config.get("rest_url")
    rune = node_config.get("rune")
    if not rest_url:
        return f"Node '{name}' is rest mode but missing rest_url."
    if not rune:
        return f"Node '{name}' is rest mode but missing rune."
    parsed = urlparse(rest_url)
    if not parsed.scheme or not parsed.netloc:
        return f"Node '{name}' has invalid rest_url."
    if parsed.scheme == "http" and not _is_local_host(parsed.hostname or "") and not HIVE_ALLOW_INSECURE_HTTP:
        return f"Node '{name}' uses http for non-local host. Set HIVE_ALLOW_INSECURE_HTTP=true to override."
    return None


def _normalize_response(result: Any) -> Dict[str, Any]:
    if isinstance(result, dict) and "error" in result:
        error_msg = result.get("error") or result.get("message") or "Unknown error"
        return {"ok": False, "error": error_msg, "details": result}
    return {"ok": True, "data": result}


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

        verify: Any
        if ssl_context:
            verify = ssl_context
        else:
            if self.rest_url.startswith("https://"):
                if not HIVE_ALLOW_INSECURE_TLS:
                    raise ValueError(
                        f"TLS verification required for {self.name} but no ca_cert configured. "
                        "Set HIVE_ALLOW_INSECURE_TLS=true to override (not recommended)."
                    )
                logger.warning(
                    "TLS verification disabled for %s (no ca_cert configured).",
                    self.name
                )
            verify = False

        # SECURITY: Never log self.rune or request headers containing it
        self.client = httpx.AsyncClient(
            base_url=self.rest_url,
            headers={"Rune": self.rune},
            verify=verify,
            timeout=HIVE_HTTP_TIMEOUT
        )
        logger.info(f"Connected to {self.name} at {self.rest_url}")

    async def close(self):
        """Close the HTTP client."""
        if self.client:
            await self.client.aclose()

    async def call(self, method: str, params: Dict = None) -> Dict:
        """Call a CLN RPC method via REST or docker exec."""
        if not _check_method_allowed(method):
            return {"error": f"Method '{method}' not in allowlist"}

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
        except httpx.HTTPStatusError as e:
            body = {}
            try:
                body = e.response.json()
            except Exception:
                body = {"error": e.response.text.strip()} if e.response.text else {}
            # Extract the actual CLN error message from the response body
            error_msg = (
                body.get("message")  # CLN REST error format: {"code": ..., "message": "..."}
                or body.get("error")  # fallback plain error
                or str(e)
                or f"HTTP {e.response.status_code} from {self.name}"
            )
            logger.error(f"RPC error on {self.name}: {error_msg}")
            return {"error": error_msg, "details": body}
        except httpx.HTTPError as e:
            error_msg = str(e) or f"{type(e).__name__} connecting to {self.name}"
            logger.error(f"RPC error on {self.name}: {error_msg}")
            return {"error": error_msg}

    async def _call_docker(self, method: str, params: Dict = None) -> Dict:
        """Call CLN via docker exec (for Polar testing)."""
        # Build command
        cmd = [
            "docker", "exec", self.docker_container,
            "lightning-cli",
            f"--lightning-dir={self.lightning_dir}",
            f"--network={self.network}",
            "--",  # Separate options from method/params
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
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=HIVE_DOCKER_TIMEOUT
            )
            if proc.returncode != 0:
                err_text = stderr.decode().strip()[:500]
                return {"error": err_text or f"Command failed with exit code {proc.returncode}"}
            return json.loads(stdout.decode()) if stdout.strip() else {}
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            return {"error": "Command timed out"}
        except json.JSONDecodeError as e:
            return {"error": f"Invalid JSON response: {e}"}
        except Exception as e:
            return {"error": str(e) or f"{type(e).__name__} in docker exec"}


class HiveFleet:
    """Manages connections to multiple Hive nodes."""

    def __init__(self):
        self.nodes: Dict[str, NodeConnection] = {}
        self._node_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._max_concurrent_per_node = 5

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

        seen_names = set()
        for node_config in config.get("nodes", []):
            # Per-node mode overrides global mode
            node_mode = node_config.get("mode", global_mode)
            error = _validate_node_config(node_config, node_mode)
            if error:
                raise ValueError(error)
            name = node_config.get("name")
            if name in seen_names:
                raise ValueError(f"Duplicate node name '{name}' in config.")
            seen_names.add(name)

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
            self._node_semaphores[node.name] = asyncio.Semaphore(self._max_concurrent_per_node)

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

    async def call_all(self, method: str, params: Dict = None, timeout: float = 30.0) -> Dict[str, Any]:
        """Call an RPC method on all nodes in parallel."""
        async def call_with_timeout(name: str, node: NodeConnection) -> tuple:
            sem = self._node_semaphores.get(name)
            try:
                if sem:
                    async with sem:
                        result = await asyncio.wait_for(node.call(method, params), timeout=timeout)
                else:
                    result = await asyncio.wait_for(node.call(method, params), timeout=timeout)
                return (name, result)
            except asyncio.TimeoutError:
                logger.warning(f"Timeout calling {method} on {name}")
                return (name, {"error": f"Timeout after {timeout}s"})
            except Exception as e:
                logger.error(f"Error calling {method} on {name}: {e}")
                return (name, {"error": str(e) or f"{type(e).__name__} calling {method}"})

        tasks = [call_with_timeout(name, node) for name, node in self.nodes.items()]
        results_list = await asyncio.gather(*tasks)
        return dict(results_list)
    
    async def health_check(self, timeout: float = 5.0) -> Dict[str, Any]:
        """Quick health check on all nodes - returns status without heavy operations."""
        async def check_node(name: str, node: NodeConnection) -> tuple:
            try:
                start = asyncio.get_running_loop().time()
                result = await asyncio.wait_for(node.call("getinfo"), timeout=timeout)
                latency = asyncio.get_running_loop().time() - start
                if "error" in result:
                    return (name, {"status": "error", "error": result["error"]})
                return (name, {
                    "status": "healthy",
                    "latency_ms": round(latency * 1000),
                    "alias": result.get("alias", "unknown"),
                    "blockheight": result.get("blockheight", 0)
                })
            except asyncio.TimeoutError:
                return (name, {"status": "timeout", "error": f"No response in {timeout}s"})
            except Exception as e:
                return (name, {"status": "error", "error": str(e) or type(e).__name__})

        tasks = [check_node(name, node) for name, node in self.nodes.items()]
        results_list = await asyncio.gather(*tasks)
        return dict(results_list)


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
            name="hive_health",
            description="Quick health check on all nodes. Returns status, latency, and basic info without heavy operations. Use this for fast connectivity checks.",
            inputSchema={
                "type": "object",
                "properties": {
                    "timeout": {
                        "type": "number",
                        "description": "Timeout in seconds per node (default: 5)"
                    }
                }
            }
        ),
        Tool(
            name="hive_fleet_snapshot",
            description="Consolidated fleet snapshot for quick monitoring. Returns node health, channel stats, 24h routing stats, pending actions, and top issues.",
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
            name="hive_anomalies",
            description="Detect anomalies outside normal ranges (revenue drops, drain patterns, peer disconnects).",
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
            name="hive_compare_periods",
            description="Compare routing performance across two periods with channel-level improvements/degradations.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (required)"
                    },
                    "period1_days": {
                        "type": "integer",
                        "description": "Days for period 1 (default: 7)"
                    },
                    "period2_days": {
                        "type": "integer",
                        "description": "Days for period 2 (default: 7)"
                    },
                    "offset_days": {
                        "type": "integer",
                        "description": "Days offset for period 2 end (default: 7)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_channel_deep_dive",
            description="Get comprehensive context for a channel or peer (balances, profitability, flow, fees, forwards, issues).",
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
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Peer pubkey"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_recommended_actions",
            description="Return prioritized recommended actions with reasoning and estimated effort.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (optional, defaults to all nodes)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max actions to return (default: 10)"
                    }
                }
            }
        ),
        Tool(
            name="hive_peer_search",
            description="Search peers by alias substring (case-insensitive).",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Alias substring to search for"
                    },
                    "node": {
                        "type": "string",
                        "description": "Node name (optional, defaults to all nodes)"
                    }
                },
                "required": ["query"]
            }
        ),
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
            name="hive_onboard_new_members",
            description="""Detect new hive members and generate strategic channel suggestions.

Runs independently of the advisor cycle to provide immediate onboarding support when new members join.

**What it does:**
1. Scans hive membership for neophytes and recently joined members (< 30 days)
2. Checks if each new member has been "onboarded" (received suggestions before)
3. For un-onboarded members, generates channel suggestions:
   - Existing fleet members should open channels TO the new member
   - New member should open channels to strategic targets (valuable corridors, exchanges)
4. Creates pending_actions entries for channel open suggestions
5. Marks members as onboarded to avoid repeat suggestions

**When to use:**
- Run periodically (e.g., hourly) via cron independent of advisor
- Run manually after a new member joins
- Run after promoting a neophyte to member

**Returns:** Summary of new members found and suggestions created.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to run onboarding check from"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, report suggestions without creating pending_actions (default: false)"
                    }
                },
                "required": ["node"]
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
        # --- Membership lifecycle ---
        Tool(
            name="hive_vouch",
            description="Vouch for a neophyte to support their promotion to full member. Vouches count toward the quorum needed for promotion.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "peer_id": {"type": "string", "description": "Public key of the neophyte to vouch for"}
                },
                "required": ["node", "peer_id"]
            }
        ),
        Tool(
            name="hive_leave",
            description="Voluntarily leave the hive. Removes this node from the member list and notifies other members. The last full member cannot leave.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "reason": {"type": "string", "description": "Reason for leaving (default: voluntary)"}
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_force_promote",
            description="Force-promote a neophyte to member during bootstrap phase. Only works when the hive is too small to reach normal vouch quorum. Admin only.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "peer_id": {"type": "string", "description": "Public key of the neophyte to promote"}
                },
                "required": ["node", "peer_id"]
            }
        ),
        Tool(
            name="hive_request_promotion",
            description="Request promotion from neophyte to member. Broadcasts a promotion request to all hive members for voting.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"}
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_remove_member",
            description="Remove a member from the hive (admin maintenance). Use to clean up stale/orphaned member entries. Cannot remove yourself - use hive_leave instead.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "peer_id": {"type": "string", "description": "Public key of the member to remove"},
                    "reason": {"type": "string", "description": "Reason for removal (default: maintenance)"}
                },
                "required": ["node", "peer_id"]
            }
        ),
        Tool(
            name="hive_genesis",
            description="Initialize this node as the genesis (first) node of a new hive. Creates the first member record with full privileges.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "hive_id": {"type": "string", "description": "Custom hive identifier (auto-generated if not provided)"}
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_invite",
            description="Generate an invitation ticket for a new member to join the hive. Only full members can generate invites.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "valid_hours": {"type": "integer", "description": "Hours until ticket expires (default: 24)"},
                    "tier": {"type": "string", "description": "Starting tier: 'neophyte' (default) or 'member' (bootstrap only)", "enum": ["neophyte", "member"]}
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_join",
            description="Join a hive using an invitation ticket. Initiates the handshake protocol with a known hive member.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "ticket": {"type": "string", "description": "Base64-encoded invitation ticket"},
                    "peer_id": {"type": "string", "description": "Node ID of a known hive member (optional, extracted from ticket if not provided)"}
                },
                "required": ["node", "ticket"]
            }
        ),
        # --- Ban governance ---
        Tool(
            name="hive_propose_ban",
            description="Propose banning a member from the hive. Requires quorum vote (51%% of members) to execute. Proposal is valid for 7 days.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "peer_id": {"type": "string", "description": "Public key of the member to ban"},
                    "reason": {"type": "string", "description": "Reason for the ban proposal (max 500 chars)"}
                },
                "required": ["node", "peer_id", "reason"]
            }
        ),
        Tool(
            name="hive_vote_ban",
            description="Vote on a pending ban proposal. Use hive_pending_bans to see active proposals first.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "proposal_id": {"type": "string", "description": "ID of the ban proposal"},
                    "vote": {"type": "string", "description": "Vote: 'approve' or 'reject'", "enum": ["approve", "reject"]}
                },
                "required": ["node", "proposal_id", "vote"]
            }
        ),
        Tool(
            name="hive_pending_bans",
            description="View pending ban proposals with vote counts, quorum status, and your vote. Shows all active ban proposals awaiting votes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"}
                },
                "required": ["node"]
            }
        ),
        # --- Health/reputation monitoring ---
        Tool(
            name="hive_nnlb_status",
            description="Get NNLB (No Node Left Behind) status. Shows health distribution across hive members and identifies struggling members who may need assistance.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"}
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_peer_reputations",
            description="Get aggregated peer reputations from hive intelligence. Peer reputations are aggregated from reports by all hive members with outlier detection.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "peer_id": {"type": "string", "description": "Optional specific peer to query (omit for all peers)"}
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_reputation_stats",
            description="Get overall reputation tracking statistics. Returns summary statistics about tracked peer reputations across the fleet.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"}
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_contribution",
            description="View contribution statistics for a peer. Shows forwarding contribution ratio, uptime, and leech detection status.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {"type": "string", "description": "Node name"},
                    "peer_id": {"type": "string", "description": "Optional peer to view (defaults to self)"}
                },
                "required": ["node"]
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
            description="Set channel fees for a specific channel on a node. IMPORTANT: Hive member channels must have 0 fees. This tool will block non-zero fees on hive channels unless force=true.",
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
                    },
                    "force": {
                        "type": "boolean",
                        "description": "Override hive zero-fee guard (default: false)"
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
            name="hive_planner_ignore",
            description="Add a peer to the planner ignore list. Ignored peers will not be selected as channel open targets until released or expired. Use when a peer rejects connections or should be skipped.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Pubkey of peer to ignore"
                    },
                    "reason": {
                        "type": "string",
                        "description": "Reason for ignoring (default: 'manual')"
                    },
                    "duration_hours": {
                        "type": "integer",
                        "description": "Hours until auto-expire (0 = permanent until released)"
                    }
                },
                "required": ["node", "peer_id"]
            }
        ),
        Tool(
            name="hive_planner_unignore",
            description="Remove a peer from the planner ignore list, allowing the planner to propose channels to this peer again.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "peer_id": {
                        "type": "string",
                        "description": "Pubkey of peer to unignore"
                    }
                },
                "required": ["node", "peer_id"]
            }
        ),
        Tool(
            name="hive_planner_ignored_peers",
            description="Get list of currently ignored peers for the planner.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "include_expired": {
                        "type": "boolean",
                        "description": "Include expired ignores (default: false)"
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
            name="hive_bump_version",
            description="Bump the gossip state version for restart recovery. Used to force a state version increment after node restart to trigger re-sync with peers.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "version": {
                        "type": "integer",
                        "description": "New version number to set"
                    }
                },
                "required": ["node", "version"]
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
            name="hive_splice",
            description="Execute a coordinated splice operation with a hive member. Splices resize channels without closing them. Requires the channel to be with another hive member. The initiating node provides the on-chain funds for splice-in operations.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (the node that will provide funds for splice-in)"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID to splice (short_channel_id format like 123x456x0)"
                    },
                    "relative_amount": {
                        "type": "integer",
                        "description": "Amount in satoshis. Positive = splice-in (add funds), Negative = splice-out (remove funds)"
                    },
                    "feerate_per_kw": {
                        "type": "integer",
                        "description": "Optional feerate in sat/kw (default: urgent rate)"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, preview the operation without executing"
                    },
                    "force": {
                        "type": "boolean",
                        "description": "If true, skip safety warnings for splice-out"
                    }
                },
                "required": ["node", "channel_id", "relative_amount"]
            }
        ),
        Tool(
            name="hive_splice_status",
            description="Get status of active splice sessions. Shows ongoing splice operations and their current state.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "session_id": {
                        "type": "string",
                        "description": "Optional specific session ID to query"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_splice_abort",
            description="Abort an active splice session. Use this if a splice is stuck or needs to be cancelled.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "session_id": {
                        "type": "string",
                        "description": "Session ID to abort"
                    }
                },
                "required": ["node", "session_id"]
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
        # Routing Intelligence Tools (Pheromones + Stigmergic Markers)
        # =====================================================================
        Tool(
            name="hive_backfill_routing_intelligence",
            description="""Backfill pheromone levels and stigmergic markers from historical forwards.

**When to use:** Run this ONCE after enabling routing intelligence to populate
the swarm systems with historical data. Also useful after restarts.

**What it does:**
- Reads historical forward events from CLN
- Populates pheromone levels (fee memory per channel)
- Creates stigmergic markers (fleet coordination signals)

**Parameters:**
- days: Number of days of history to process (default: 30)
- status_filter: "settled" (default), "failed", or "all"

**Returns:** Statistics on processed forwards and current intelligence levels.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "days": {
                        "type": "integer",
                        "description": "Days of history to process (default: 30)"
                    },
                    "status_filter": {
                        "type": "string",
                        "description": "Forward status: settled, failed, or all (default: settled)",
                        "enum": ["settled", "failed", "all"]
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_routing_intelligence_status",
            description="""Get current status of routing intelligence systems (pheromones + markers).

**Shows:**
- Pheromone levels per channel (memory of successful fees)
- Active stigmergic markers (fleet coordination signals)
- Marker success/failure breakdown
- Configuration thresholds

**Use this to verify routing intelligence is being collected and to understand
which channels have strong fee signals.""",
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
            name="revenue_portfolio",
            description="""Analyze channel portfolio using Mean-Variance (Markowitz) optimization.

Treats channels as assets in a portfolio to optimize liquidity allocation for maximum risk-adjusted returns.

Returns:
- summary: Portfolio Sharpe ratio, diversification ratio, improvement potential
- optimal_allocations: Target liquidity % per channel
- recommendations: Prioritized rebalance actions
- hedging_opportunities: Negatively correlated channel pairs (natural hedges)
- concentration_risks: Highly correlated pairs (undiversified risk)

Use risk_aversion: 0.5=aggressive, 1.0=balanced, 2.0=conservative""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "risk_aversion": {
                        "type": "number",
                        "description": "Risk aversion (0.5=aggressive, 1.0=balanced, 2.0=conservative)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="revenue_portfolio_summary",
            description="Get lightweight portfolio summary: Sharpe ratio, diversification, improvement potential.",
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
            name="revenue_portfolio_rebalance",
            description="""Get portfolio-optimized rebalance recommendations.

Prioritizes rebalances that improve portfolio efficiency (Sharpe ratio) over individual channel balance.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "max_recommendations": {
                        "type": "integer",
                        "description": "Maximum recommendations to return (default: 5)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="revenue_portfolio_correlations",
            description="""Get channel correlation analysis to identify hedging opportunities and concentration risks.

Hedging opportunities: Negatively correlated channels that naturally balance each other.
Concentration risks: Highly correlated channels that move together (undiversified).""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "min_correlation": {
                        "type": "number",
                        "description": "Minimum |correlation| to include (default: 0.3)"
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
                        "type": ["string", "number", "boolean"],
                        "description": "New value (for set action)"
                    }
                },
                "required": ["node", "action"]
            }
        ),
        Tool(
            name="config_adjust",
            description="""Adjust cl-revenue-ops config with tracking for learning and analysis.

Records the adjustment in advisor database, enabling outcome measurement and 
effectiveness analysis over time. Use instead of revenue_config when you want
to track the decision and learn from outcomes.

**Recommended config keys for advisor tuning:**
- min_fee_ppm: Fee floor (raise if drain detected, lower if stagnating)
- max_fee_ppm: Fee ceiling (adjust based on competitive positioning)
- daily_budget_sats: Rebalance budget (scale with profitability)
- rebalance_max_amount: Max rebalance size per operation
- thompson_observation_decay_hours: Shorter (72h) in volatile, longer (168h) in stable
- hive_prior_weight: Trust in hive intelligence (0-1)
- scarcity_threshold: When to apply scarcity pricing (0-1)

**Trigger reasons:** drain_detected, stagnation, profitability_low, profitability_high,
budget_exhausted, market_conditions, competitive_pressure, channel_health""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "config_key": {
                        "type": "string",
                        "description": "Config key to adjust"
                    },
                    "new_value": {
                        "type": ["string", "number", "boolean"],
                        "description": "New value to set"
                    },
                    "trigger_reason": {
                        "type": "string",
                        "description": "Why making this change (e.g., drain_detected, stagnation)"
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "Detailed explanation of the decision"
                    },
                    "confidence": {
                        "type": "number",
                        "description": "0-1 confidence in the change"
                    },
                    "context_metrics": {
                        "type": "object",
                        "description": "Relevant metrics at time of change for outcome comparison"
                    }
                },
                "required": ["node", "config_key", "new_value", "trigger_reason"]
            }
        ),
        Tool(
            name="config_adjustment_history",
            description="""Get history of config adjustments for analysis and learning.

Use this to review what changes were made, why, and their outcomes.
Essential for understanding which adjustments worked and which didn't.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Filter by node (optional)"
                    },
                    "config_key": {
                        "type": "string",
                        "description": "Filter by specific config key (optional)"
                    },
                    "days": {
                        "type": "integer",
                        "description": "How far back to look (default: 30)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max records (default: 50)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="config_effectiveness",
            description="""Analyze effectiveness of config adjustments over time.

Shows success rates, learned optimal ranges, and recommendations
based on historical adjustment outcomes. Use to understand which
config values work best for this fleet.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Filter by node (optional)"
                    },
                    "config_key": {
                        "type": "string",
                        "description": "Filter by specific config key (optional)"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="config_measure_outcomes",
            description="""Measure outcomes for pending config adjustments.

Compares current metrics against metrics at time of adjustment
to determine if changes were successful. Should be called periodically
(e.g., 24-48h after adjustments) to evaluate effectiveness.

This enables the learning loop: adjust -> measure -> learn -> improve.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "hours_since": {
                        "type": "integer",
                        "description": "Only measure adjustments older than this (default: 24)"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, show what would be measured without recording"
                    }
                },
                "required": []
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
        # =====================================================================
        # Diagnostic Tools - Data pipeline health checks and validation
        # =====================================================================
        Tool(
            name="hive_node_diagnostic",
            description="""Run a comprehensive diagnostic on a single node.

**Returns in one call:**
- Channel balances (total capacity, local/remote, balance ratios)
- 24h forwarding stats (count, volume, revenue, avg fee)
- Sling rebalancer status (if available)
- Installed plugin list

**When to use:** First tool to call when investigating node issues or verifying data pipeline health.""",
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
            name="revenue_ops_health",
            description="""Validate cl-revenue-ops data pipeline health.

**Checks 4 RPC endpoints:**
- revenue-dashboard: P&L data availability
- revenue-profitability: Channel classification data
- revenue-rebalance-debug: Rebalance subsystem state
- revenue-status: Plugin operational status

**Returns:** Per-check pass/fail/error/warn status + overall health (healthy/warning/unhealthy/degraded).

**When to use:** After deploying changes or when advisor reports unexpected data.""",
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
            name="advisor_validate_data",
            description="""Validate advisor snapshot data quality.

**Checks:**
- Zero-value detection: channels with 0 capacity or 0 local balance
- Missing IDs: channels without short_channel_id or peer_id
- Flow state consistency: balance ratios outside 0-1 range
- Live comparison: snapshot balances vs current listpeerchannels data

**When to use:** After recording a snapshot, to verify data integrity. Catches the zero-balance and missing-data bugs that were previously found.""",
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
            name="advisor_dedup_status",
            description="""Check for duplicate and stale pending decisions.

**Returns:**
- Pending decision count grouped by (decision_type, node, channel)
- Duplicate groups (same type+node+channel with multiple pending decisions)
- Stale decisions (pending > 48 hours)
- Outcome measurement coverage (decisions with measured outcomes vs total)

**When to use:** Before running advisor cycle, to clean up stale recommendations.""",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="rebalance_diagnostic",
            description="""Diagnose rebalancing subsystem health.

**Checks:**
- Sling plugin availability
- Active sling jobs and their status
- Rebalance rejection reasons from revenue-rebalance-debug
- Capital controls state
- Budget availability

**When to use:** When rebalances are failing or not executing as expected.""",
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
                    },
                    "predicted_benefit": {
                        "type": "integer",
                        "description": "Predicted benefit in sats from opportunity scanner (optional)"
                    },
                    "snapshot_metrics": {
                        "type": "string",
                        "description": "JSON snapshot of decision context metrics (optional)"
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
        # Phase 3: Automation Tools - Autonomous Fleet Management
        # =====================================================================
        Tool(
            name="auto_evaluate_proposal",
            description="""Evaluate a pending proposal against automated criteria and optionally execute.

**When to use:** Use this to get an automated evaluation of a pending action with reasoning.
Can auto-execute approve/reject if dry_run=false and decision is not "escalate".

**Evaluation Criteria:**
- Channel opens: approve if ≥15 channels, quality≥0.4 (not "avoid"), within budget, positive return
- Channel opens: reject if <10 channels, quality="avoid", over budget
- Fee changes: approve if ≤25% change, within 50-1500ppm range
- Rebalances: approve if EV-positive, ≤500k sats

**Returns:**
- decision: "approve" | "reject" | "escalate"
- reasoning: Explanation of the decision
- action_executed: Whether action was executed (only if dry_run=false and decision!=escalate)""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "action_id": {
                        "type": "integer",
                        "description": "ID of the pending action to evaluate"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, evaluate only without executing (default: true)"
                    }
                },
                "required": ["node", "action_id"]
            }
        ),
        Tool(
            name="process_all_pending",
            description="""Batch process all pending actions across the fleet.

**When to use:** Run periodically (e.g., every 4 hours) to handle routine proposals automatically
and surface only those requiring human review.

**What it does:**
1. Gets pending actions from all configured nodes
2. Evaluates each against automated criteria
3. If dry_run=false: executes approve/reject decisions
4. Aggregates results into approved, rejected, escalated lists

**Returns:**
- summary: Quick overview (counts by category)
- approved: Actions that were/would be approved
- rejected: Actions that were/would be rejected
- escalated: Actions requiring human review
- by_node: Per-node breakdown""",
            inputSchema={
                "type": "object",
                "properties": {
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, evaluate only without executing (default: true)"
                    }
                }
            }
        ),
        Tool(
            name="stagnant_channels",
            description="""List channels with ≥95% local balance (stagnant) with enriched context.

**When to use:** Run as part of fleet health checks to identify channels that aren't routing.
These channels have capital locked up without generating revenue.

**Returns per channel:**
- peer_alias, capacity, local_pct
- channel_age_days (calculated from SCID)
- days_since_last_forward
- peer_quality (from advisor_get_peer_intel)
- current_fee_ppm, current_policy
- recommendation: "close" | "fee_reduction" | "static_policy" | "wait"
- reasoning: Why this recommendation""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "min_local_pct": {
                        "type": "number",
                        "description": "Minimum local balance percentage to consider stagnant (default: 95)"
                    },
                    "min_age_days": {
                        "type": "integer",
                        "description": "Minimum channel age in days (default: 0)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="remediate_stagnant",
            description="""Auto-remediate stagnant channels based on age and peer quality.

**When to use:** Run periodically (e.g., daily) to automatically apply remediation strategies
to stagnant channels that meet criteria.

**Remediation Rules:**
- <30 days old: skip (too young to judge)
- 30-90 days + neutral/good peer: reduce fee to 50ppm to attract flow
- >90 days + neutral peer: apply static policy, disable rebalance
- any age + "avoid" peer: flag for close review (never auto-close)

**Returns:**
- actions_taken: List of remediation actions applied
- channels_skipped: Channels that didn't match criteria
- flagged_for_review: Channels with "avoid" peers needing human decision""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, report what would be done without executing (default: true)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="execute_safe_opportunities",
            description="""Execute opportunities marked as auto_execute_safe.

**When to use:** Run after advisor_scan_opportunities to automatically execute low-risk
optimizations like small fee adjustments.

**What it does:**
1. Calls advisor_scan_opportunities to get current opportunities
2. Filters for auto_execute_safe=true
3. Executes each via appropriate tool (revenue_set_fee, etc.)
4. Logs all decisions to advisor DB for audit trail

**Safety:**
- Only executes opportunities the scanner marked as safe
- All decisions logged for review
- dry_run mode available for preview

**Returns:**
- executed_count: Number of opportunities executed
- skipped_count: Number skipped (not safe or dry_run)
- executed: Details of executed opportunities
- skipped: Details of skipped opportunities""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, report what would be done without executing (default: true)"
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
        # Kalman Velocity Integration Tools
        # =======================================================================
        Tool(
            name="kalman_velocity_query",
            description="""Query Kalman-estimated velocity for a channel.

**What it provides:**
- Consensus velocity estimate from fleet members running Kalman filters
- Uncertainty bounds for confidence weighting
- Flow ratio and regime change detection

**Why use Kalman instead of simple averages:**
- Kalman filters provide optimal state estimation
- Tracks both ratio AND velocity as a state vector
- Adapts faster to regime changes than EMA
- Proper uncertainty quantification

**When to use:** Before rebalancing decisions or fee changes to understand the true velocity trend.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel ID to query velocity for"
                    }
                },
                "required": ["node", "channel_id"]
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
            name="execute_hive_circular_rebalance",
            description="Execute a circular rebalance through hive members using explicit sendpay routes. Uses 0-fee internal hive channels for cost-free liquidity rebalancing. Specify from_channel (source) and to_channel (destination) on your node, and optionally via_members to control the route through the hive triangle/mesh.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "from_channel": {
                        "type": "string",
                        "description": "Source channel SCID to drain liquidity from"
                    },
                    "to_channel": {
                        "type": "string",
                        "description": "Destination channel SCID to add liquidity to"
                    },
                    "amount_sats": {
                        "type": "integer",
                        "description": "Amount to rebalance in satoshis"
                    },
                    "via_members": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of hive member pubkeys to route through (in order). If omitted, uses direct path between from/to channel peers."
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, calculate route but don't execute (default: true)"
                    }
                },
                "required": ["node", "from_channel", "to_channel", "amount_sats"]
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
        ),
        # Phase 12: Distributed Settlement
        Tool(
            name="distributed_settlement_status",
            description="Get distributed settlement status including pending proposals, ready settlements, and participation. Shows which nodes have voted and executed their payments.",
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
            name="distributed_settlement_proposals",
            description="Get all settlement proposals with voting status. Shows proposal details, vote counts, and quorum progress.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "status": {
                        "type": "string",
                        "description": "Filter by status: pending, ready, completed, expired (optional)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="distributed_settlement_participation",
            description="Get settlement participation rates for all members. Identifies nodes that consistently skip votes or fail to execute payments - potential gaming behavior.",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "periods": {
                        "type": "integer",
                        "description": "Number of recent periods to analyze (default: 10)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_network_metrics",
            description="""Get network position metrics for hive members.

**Metrics provided:**
- **external_centrality**: Betweenness centrality approximation (routing importance)
- **unique_peers**: External peers only this member connects to
- **bridge_score**: Ratio indicating bridge function (0-1, higher = connects more unique peers)
- **hive_centrality**: Internal fleet connectivity (0-1, higher = more fleet connections)
- **hive_reachability**: Fraction of fleet reachable in 1-2 hops
- **rebalance_hub_score**: Suitability as internal rebalance intermediary

**Use cases:**
- Pool share calculation (position contributes 20% of share)
- Identifying best rebalance hub nodes
- Promotion eligibility evaluation
- Strategic channel planning""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query from"
                    },
                    "member_id": {
                        "type": "string",
                        "description": "Specific member pubkey (optional, omit for all members)"
                    },
                    "force_refresh": {
                        "type": "boolean",
                        "description": "Bypass cache and recalculate (default: false)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_rebalance_hubs",
            description="""Get best members to use as zero-fee rebalance intermediaries.

High hive_centrality nodes make excellent rebalance hubs because:
- They have direct connections to many fleet members
- They can route rebalances between otherwise disconnected members
- Zero-fee hive channels make them cost-effective paths

**Returns** top N members ranked by rebalance_hub_score with:
- Hub score and hive centrality
- Number of fleet connections
- Fleet reachability percentage
- Rationale for recommendation
- Suggested use (zero_fee_intermediary or backup_path)

**Use for:**
- Planning internal fleet rebalances
- Identifying which members should maintain high liquidity
- Optimizing rebalance routing paths""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query from"
                    },
                    "top_n": {
                        "type": "integer",
                        "description": "Number of top hubs to return (default: 3)"
                    },
                    "exclude_members": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Member pubkeys to exclude (e.g., rebalance source/dest)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_rebalance_path",
            description="""Find optimal path for internal hive rebalance between two members.

For zero-fee hive rebalances, finds the best route through high-centrality
intermediary nodes when direct path isn't available.

**Returns:**
- Path as list of member pubkeys (source -> intermediaries -> dest)
- Or null if no path found within max_hops

**Use before** executing internal rebalances to find cheapest route.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query from"
                    },
                    "source_member": {
                        "type": "string",
                        "description": "Starting member pubkey"
                    },
                    "dest_member": {
                        "type": "string",
                        "description": "Destination member pubkey"
                    },
                    "max_hops": {
                        "type": "integer",
                        "description": "Maximum intermediaries (default: 2)"
                    }
                },
                "required": ["node", "source_member", "dest_member"]
            }
        ),
        # Fleet Health Monitoring Tools
        Tool(
            name="hive_fleet_health",
            description="""Get overall fleet connectivity health metrics.

Returns aggregated metrics showing how well-connected the fleet is internally.

**Shows:**
- avg_hive_centrality: Average internal connectivity (0-1)
- avg_hive_reachability: Average fleet reachability (0-1)
- hub_count: Members suitable as rebalance hubs
- isolated_count: Members with limited connectivity
- health_score: Overall health (0-100)
- health_grade: Letter grade A-F

**Use for:** Monitoring fleet health, identifying connectivity issues early.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query from"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_connectivity_alerts",
            description="""Check for fleet connectivity issues that need attention.

Returns alerts sorted by severity:
- **critical**: Disconnected members (no hive channels)
- **warning**: Isolated members (<50% reachability), low hub availability
- **info**: Low centrality members

**Use for:** Proactive monitoring, identifying members needing help connecting.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query from"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_member_connectivity",
            description="""Get detailed connectivity report for a specific member.

**Shows:**
- Connection status (well_connected, partial, isolated, disconnected)
- Metrics vs fleet average
- List of members not connected to
- Top 3 recommended connections (highest centrality targets)

**Use for:** Helping specific members improve their fleet connectivity.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query from"
                    },
                    "member_id": {
                        "type": "string",
                        "description": "Member pubkey to analyze"
                    }
                },
                "required": ["node", "member_id"]
            }
        ),
        # Promotion Criteria Tools
        Tool(
            name="hive_neophyte_rankings",
            description="""Get all neophytes ranked by promotion readiness.

Ranks neophytes by a readiness score (0-100) based on:
- Probation progress (40%)
- Uptime (20%)
- Contribution ratio (20%)
- Hive centrality (20%) - demonstrates commitment to fleet

**Fast-track eligibility:**
Neophytes with hive_centrality >= 0.5 can be promoted after 30 days
instead of the full 90-day probation (if all other criteria met).

**Shows for each neophyte:**
- readiness_score: 0-100 overall score
- eligible: Ready for auto-promotion
- fast_track_eligible: Can skip remaining probation
- blocking_reasons: What's preventing promotion

**Use for:** Identifying neophytes close to promotion, recognizing commitment.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query from"
                    }
                },
                "required": ["node"]
            }
        ),
        # MCF (Min-Cost Max-Flow) Optimization tools (Phase 15)
        Tool(
            name="hive_mcf_status",
            description="""Get MCF (Min-Cost Max-Flow) optimizer status.

The MCF optimizer computes globally optimal rebalance assignments for the fleet.
Shows circuit breaker state, health metrics, and current solution status.

**Returns:**
- enabled: Whether MCF optimization is active
- is_coordinator: Whether this node is the current MCF coordinator
- coordinator_id: Current coordinator's pubkey
- circuit_breaker_state: CLOSED (healthy), OPEN (failing), HALF_OPEN (recovering)
- health_metrics: Solution staleness, success/failure counts
- last_solution: Timestamp and stats from most recent optimization
- pending_assignments: Number of assignments waiting to be executed""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_mcf_solve",
            description="""Trigger MCF optimization cycle manually.

Runs the Min-Cost Max-Flow solver to compute optimal fleet-wide rebalancing.
Only effective when called on the current coordinator node.

**Returns:**
- solution: Computed optimal assignments
- total_flow: Total sats being rebalanced
- total_cost: Expected cost in sats
- assignments_count: Number of member assignments
- network_stats: Nodes and edges in optimization network

**Note:** Solution is automatically broadcast to fleet members.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (should be coordinator)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_mcf_assignments",
            description="""Get pending MCF assignments for a node.

Shows rebalance assignments computed by fleet-wide MCF optimization.
Each assignment specifies source channel, destination channel, amount,
expected cost, and execution priority.

**Assignment lifecycle:**
- pending: Waiting to be claimed
- executing: Currently being processed
- completed: Successfully executed
- failed: Execution failed
- expired: Assignment timed out

**Returns:**
- pending: Assignments waiting for execution
- executing: Currently processing
- completed_recent: Recently completed (last 24h)
- failed_recent: Recently failed (last 24h)""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="hive_mcf_optimized_path",
            description="""Get MCF-optimized rebalance path between channels.

Uses the latest MCF solution if available and valid, otherwise falls back to BFS.
Returns the optimal path for rebalancing liquidity between two channels.

**Returns:**
- path: List of pubkeys forming the route
- source: "mcf" or "bfs" indicating which algorithm found the path
- cost_estimate_ppm: Expected routing cost
- hops: Number of hops in the path""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query"
                    },
                    "source_channel": {
                        "type": "string",
                        "description": "Source channel SCID (e.g., 933128x1345x0)"
                    },
                    "dest_channel": {
                        "type": "string",
                        "description": "Destination channel SCID"
                    },
                    "amount_sats": {
                        "type": "integer",
                        "description": "Amount to rebalance in satoshis"
                    }
                },
                "required": ["node", "source_channel", "dest_channel", "amount_sats"]
            }
        ),
        Tool(
            name="hive_mcf_health",
            description="""Get detailed MCF health and circuit breaker metrics.

Provides comprehensive view of MCF optimizer health including:
- Circuit breaker state and transition history
- Solution staleness tracking
- Assignment success/failure rates
- Recovery status after failures

**Circuit Breaker States:**
- CLOSED: Normal operation, MCF running
- OPEN: Too many failures, MCF disabled temporarily
- HALF_OPEN: Testing recovery with limited operations

**Health Assessment:**
- healthy: All systems nominal
- degraded: Some issues but operational
- unhealthy: Circuit breaker open, MCF disabled""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query"
                    }
                },
                "required": ["node"]
            }
        ),
        # =====================================================================
        # Phase 4: Membership & Settlement Tools (Hex Automation)
        # =====================================================================
        Tool(
            name="membership_dashboard",
            description="""Get unified membership lifecycle view.

**Returns:**
- neophytes: count, rankings (from hive_neophyte_rankings), promotion_eligible, fast_track_eligible
- members: count, contribution_scores (from hive_contribution), health (from hive_nnlb_status)
- pending_actions: pending_promotions count, pending_bans count
- onboarding_needed: members without channel suggestions

**When to use:** For quick membership health overview during heartbeat checks.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="check_neophytes",
            description="""Check for promotion-ready neophytes and optionally propose promotions.

Calls hive_neophyte_rankings and for each eligible or fast_track_eligible neophyte:
- Checks if already in pending_promotions
- If not pending and dry_run=false: calls hive_propose_promotion

**Returns:**
- proposed_count: Number of promotions proposed this run
- already_pending_count: Number already in voting
- details: Per-neophyte breakdown with eligibility and status

**Default:** dry_run=true (preview only)""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, preview without proposing (default: true)"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="settlement_readiness",
            description="""Pre-settlement validation check.

Validates that the hive is ready for settlement:
- Checks all members have BOLT12 offers registered
- Reviews participation history for potential gaming
- Calculates expected distribution via settlement_calculate

**Returns:**
- ready: Boolean indicating if settlement can proceed
- blockers: List of issues preventing settlement
- missing_offers: Members without BOLT12 offers
- low_participation: Members with <50% historical participation
- expected_distribution: Preview of what each member would receive
- recommendation: "settle_now" | "wait" | "fix_blockers"

**When to use:** Before running settlement to ensure clean execution.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="run_settlement_cycle",
            description="""Execute a full settlement cycle.

**Steps:**
1. Calls pool_snapshot to record current contributions
2. Calls settlement_calculate for distribution preview
3. If dry_run=false: calls settlement_execute to distribute funds

**Returns:**
- period: Settlement period (YYYY-WW format)
- snapshot_recorded: Whether contribution snapshot was taken
- total_distributed_sats: Total sats distributed (0 if dry_run)
- per_member_breakdown: What each member received/would receive
- dry_run: Whether this was a preview

**Default:** dry_run=true (preview only)

**When to use:** Weekly settlement execution (typically Sunday).""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to run settlement from"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, preview without executing (default: true)"
                    }
                },
                "required": ["node"]
            }
        ),
        # =====================================================================
        # Phase 5: Monitoring & Health Tools (Hex Automation)
        # =====================================================================
        Tool(
            name="fleet_health_summary",
            description="""Quick fleet health overview for monitoring.

**Returns:**
- nodes: Per-node status (online, channel_count, total_capacity_sats)
- channel_distribution: % profitable, % underwater, % stagnant (from revenue_profitability)
- routing_24h: volume_sats, revenue_sats, forward_count
- alerts: Active alert counts by severity (critical, warning, info)
- mcf_health: MCF optimizer status and circuit breaker state
- nnlb_struggling: Members identified as struggling by NNLB

**When to use:** Heartbeat health checks (3x daily).""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name (optional, defaults to all nodes)"
                    }
                }
            }
        ),
        Tool(
            name="routing_intelligence_health",
            description="""Check routing intelligence data quality.

**Returns:**
- pheromone_coverage:
  - channels_with_data: Count of channels with pheromone signals
  - stale_count: Channels with data older than 7 days
  - coverage_pct: Percentage of channels with fresh data
- stigmergic_markers:
  - active_count: Number of active markers
  - corridors_tracked: Unique corridors being tracked
- needs_backfill: Boolean - true if data is insufficient
- recommendation: "healthy" | "needs_backfill" | "partially_stale"

**When to use:** During deep checks to verify routing intelligence is collecting properly.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query"
                    }
                },
                "required": ["node"]
            }
        ),
        Tool(
            name="advisor_channel_history",
            description="""Query past advisor decisions for a specific channel.

**Returns:**
- decisions: List of past decisions with:
  - decision_type: fee_change, rebalance, flag_channel, etc.
  - recommendation: What was recommended
  - reasoning: Why
  - timestamp: When the decision was made
  - outcome: If measured (improved/unchanged/worsened)
- pattern_detection:
  - repeated_recommendations: Same advice given >2 times
  - conflicting_decisions: Back-and-forth changes detected
  - decision_frequency: Average days between decisions

**When to use:** Before making decisions on a channel, check what was tried before.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "channel_id": {
                        "type": "string",
                        "description": "Channel SCID to query"
                    },
                    "days": {
                        "type": "integer",
                        "description": "Days of history to retrieve (default: 30)"
                    }
                },
                "required": ["node", "channel_id"]
            }
        ),
        Tool(
            name="connectivity_recommendations",
            description="""Get actionable connectivity improvement recommendations.

Takes alerts from hive_connectivity_alerts and enriches them with specific actions.

**Returns per alert:**
- alert_type: disconnected, isolated, low_connectivity
- member: pubkey and alias of affected member
- recommendation:
  - who_should_act: Member pubkey/alias who should take action
  - action: open_channel_to, improve_uptime, add_liquidity
  - target: Target pubkey if applicable (for channel opens)
  - expected_improvement: Description of expected benefit
  - priority: 1-5 (5 = most urgent)

**When to use:** After connectivity_alerts shows issues, get specific remediation steps.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name to query"
                    }
                },
                "required": ["node"]
            }
        ),
        # =====================================================================
        # Automation Tools (Phase 2 - Hex Enhancement)
        # =====================================================================
        Tool(
            name="bulk_policy",
            description="""Apply policies to multiple channels matching criteria.

Batch policy application for channel categories:
- filter_type: "stagnant" | "zombie" | "underwater" | "depleted" | "custom"
- strategy: "static" | "passive" | "dynamic"
- fee_ppm: Target fee for static strategy
- rebalance: "enabled" | "disabled" | "source_only" | "sink_only"

Default is dry_run=true which previews without applying.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "filter_type": {
                        "type": "string",
                        "enum": ["stagnant", "zombie", "underwater", "depleted", "custom"],
                        "description": "Channel filter type"
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["static", "passive", "dynamic"],
                        "description": "Fee strategy to apply"
                    },
                    "fee_ppm": {
                        "type": "integer",
                        "description": "Fee PPM for static strategy"
                    },
                    "rebalance": {
                        "type": "string",
                        "enum": ["enabled", "disabled", "source_only", "sink_only"],
                        "description": "Rebalance setting"
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "Preview without applying (default: true)"
                    },
                    "custom_filter": {
                        "type": "object",
                        "description": "Custom filter criteria for filter_type='custom'"
                    }
                },
                "required": ["node", "filter_type"]
            }
        ),
        Tool(
            name="enrich_peer",
            description="""Get external data for peer evaluation from mempool.space.

Queries the public mempool.space Lightning API to get:
- alias: Node alias
- capacity_sats: Total node capacity
- channel_count: Number of channels
- first_seen: When node first appeared
- updated_at: Last update time

Gracefully falls back if API is unavailable.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "peer_id": {
                        "type": "string",
                        "description": "Peer public key (hex)"
                    },
                    "timeout_seconds": {
                        "type": "number",
                        "description": "API timeout (default: 10)"
                    }
                },
                "required": ["peer_id"]
            }
        ),
        Tool(
            name="enrich_proposal",
            description="""Enhance a pending action with external peer data.

Takes a pending action and enriches it with:
- External peer data from mempool.space
- Peer quality assessment
- Enhanced recommendation based on combined data

Use before approving/rejecting channel opens or policy changes.""",
            inputSchema={
                "type": "object",
                "properties": {
                    "node": {
                        "type": "string",
                        "description": "Node name"
                    },
                    "action_id": {
                        "type": "integer",
                        "description": "Pending action ID to enrich"
                    }
                },
                "required": ["node", "action_id"]
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict) -> List[TextContent]:
    """Handle tool calls via registry dispatch."""
    try:
        handler = TOOL_HANDLERS.get(name)
        if handler is None:
            result = {"error": f"Unknown tool: {name}"}
        else:
            result = await handler(arguments)

        if HIVE_NORMALIZE_RESPONSES:
            result = _normalize_response(result)
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    except Exception as e:
        logger.exception(f"Error in tool {name}")
        error_msg = str(e) or f"{type(e).__name__} in {name}"
        error_result = {"error": error_msg}
        if HIVE_NORMALIZE_RESPONSES:
            error_result = {"ok": False, "error": error_msg}
        return [TextContent(type="text", text=json.dumps(error_result))]


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


def _extract_msat(value: Any) -> int:
    if isinstance(value, dict) and "msat" in value:
        try:
            return int(value.get("msat", 0))
        except (ValueError, TypeError):
            return 0
    if isinstance(value, str) and value.endswith("msat"):
        try:
            return int(value[:-4])
        except ValueError:
            return 0
    if isinstance(value, (int, float)):
        return int(value)
    return 0


def _channel_totals(channel: Dict) -> Dict[str, int]:
    # Use explicit None checks — `or` chaining treats 0 as falsy
    total_raw = channel.get("total_msat")
    if total_raw is None:
        total_raw = channel.get("channel_total_msat")
    if total_raw is None:
        total_raw = channel.get("amount_msat")
    total_msat = _extract_msat(total_raw)

    local_raw = channel.get("to_us_msat")
    if local_raw is None:
        local_raw = channel.get("our_amount_msat")
    if local_raw is None:
        local_raw = channel.get("our_msat")
    local_msat = _extract_msat(local_raw)

    return {"total_msat": total_msat, "local_msat": local_msat}


def _coerce_ts(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0


def _forward_stats(forwards: List[Dict], start_ts: int, end_ts: int) -> Dict[str, Any]:
    forward_count = 0
    total_volume_msat = 0
    total_revenue_msat = 0
    per_channel: Dict[str, Dict[str, int]] = {}

    for fwd in forwards:
        resolved = _coerce_ts(fwd.get("resolved_time") or fwd.get("resolved_at") or 0)
        if resolved <= 0 or resolved < start_ts or resolved > end_ts:
            continue

        forward_count += 1
        in_msat = _extract_msat(fwd.get("in_msat"))
        out_msat = _extract_msat(fwd.get("out_msat"))
        volume_msat = out_msat if out_msat else in_msat
        revenue_msat = max(0, in_msat - out_msat) if in_msat and out_msat else 0

        total_volume_msat += volume_msat
        total_revenue_msat += revenue_msat

        out_channel = fwd.get("out_channel") or fwd.get("out_channel_id") or fwd.get("out_scid")
        if out_channel:
            entry = per_channel.setdefault(out_channel, {"revenue_msat": 0, "volume_msat": 0, "count": 0})
            entry["revenue_msat"] += revenue_msat
            entry["volume_msat"] += volume_msat
            entry["count"] += 1

    avg_fee_ppm = int((total_revenue_msat * 1_000_000) / total_volume_msat) if total_volume_msat else 0

    return {
        "forward_count": forward_count,
        "total_volume_msat": total_volume_msat,
        "total_revenue_msat": total_revenue_msat,
        "avg_fee_ppm": avg_fee_ppm,
        "per_channel": per_channel
    }


def _flow_profile(channel: Dict) -> Dict[str, Any]:
    in_fulfilled = channel.get("in_payments_fulfilled", 0)
    out_fulfilled = channel.get("out_payments_fulfilled", 0)
    in_msat = channel.get("in_fulfilled_msat", 0)
    out_msat = channel.get("out_fulfilled_msat", 0)

    total = in_fulfilled + out_fulfilled
    if total == 0:
        flow_profile = "inactive"
        ratio = 0.0
    elif out_fulfilled == 0:
        flow_profile = "inbound_only"
        ratio = float("inf")
    elif in_fulfilled == 0:
        flow_profile = "outbound_only"
        ratio = 0.0
    else:
        ratio = round(in_fulfilled / out_fulfilled, 2)
        if ratio > 3.0:
            flow_profile = "inbound_dominant"
        elif ratio < 0.33:
            flow_profile = "outbound_dominant"
        else:
            flow_profile = "balanced"

    return {
        "flow_profile": flow_profile,
        "inbound_outbound_ratio": ratio if ratio != float("inf") else 999.99,
        "inbound_payments": in_fulfilled,
        "outbound_payments": out_fulfilled,
        "inbound_volume_sats": _extract_msat(in_msat) // 1000,
        "outbound_volume_sats": _extract_msat(out_msat) // 1000
    }


def _scid_to_age_days(scid: str, current_blockheight: int) -> Optional[int]:
    """
    Calculate channel age in days from short_channel_id.
    
    SCID format: BLOCKxTXINDEXxOUTPUT (e.g., 933128x1345x0)
    
    Args:
        scid: Short channel ID
        current_blockheight: Current blockchain height
        
    Returns:
        Approximate age in days, or None if SCID is invalid
    """
    if not scid or 'x' not in str(scid):
        return None
    try:
        funding_block = int(str(scid).split('x')[0])
        if funding_block <= 0 or funding_block > current_blockheight:
            return None
        blocks_elapsed = current_blockheight - funding_block
        return max(0, blocks_elapsed // 144)  # ~144 blocks per day
    except (ValueError, IndexError):
        return None


async def _node_fleet_snapshot(node: NodeConnection) -> Dict[str, Any]:
    import time

    now = int(time.time())
    since_24h = now - 86400

    info, peers, channels_result, pending, forwards = await asyncio.gather(
        node.call("getinfo"),
        node.call("listpeers"),
        node.call("listpeerchannels"),
        node.call("hive-pending-actions"),
        node.call("listforwards", {"status": "settled"}),
    )
    forward_count = 0
    total_volume_msat = 0
    total_revenue_msat = 0
    stats_24h = _forward_stats(forwards.get("forwards", []), since_24h, now)
    forward_count = stats_24h["forward_count"]
    total_volume_msat = stats_24h["total_volume_msat"]
    total_revenue_msat = stats_24h["total_revenue_msat"]

    # Channel stats
    channels = channels_result.get("channels", [])
    channel_count = len(channels)
    total_capacity_msat = 0
    total_local_msat = 0
    low_balance_channels = []

    for ch in channels:
        totals = _channel_totals(ch)
        total_msat = totals["total_msat"]
        local_msat = totals["local_msat"]
        if total_msat <= 0:
            continue
        total_capacity_msat += total_msat
        total_local_msat += local_msat
        local_pct = local_msat / total_msat if total_msat else 0.0
        if local_pct < 0.2:
            low_balance_channels.append({
                "channel_id": ch.get("short_channel_id"),
                "peer_id": ch.get("peer_id"),
                "local_pct": round(local_pct * 100, 2)
            })

    local_balance_pct = round((total_local_msat / total_capacity_msat) * 100, 2) if total_capacity_msat else 0.0

    # Issues (bleeders, zombies) from revenue-profitability if available
    issues = []
    try:
        profitability = await node.call("revenue-profitability")
        channels_by_class = profitability.get("channels_by_class", {})
        for class_name in ("underwater", "zombie", "stagnant_candidate"):
            severity = "warning" if class_name == "underwater" else "info"
            for ch in channels_by_class.get(class_name, [])[:3]:
                issues.append({
                    "type": class_name,
                    "severity": severity,
                    "channel_id": ch.get("channel_id"),
                    "details": {
                        "net_profit_sats": ch.get("net_profit_sats"),
                        "roi_percentage": ch.get("roi_percentage"),
                        "flow_profile": ch.get("flow_profile"),
                    }
                })
    except Exception as e:
        logger.debug(f"Could not fetch profitability issues: {e}")

    for ch in low_balance_channels:
        issues.append({
            "type": "critical_low_balance",
            "severity": "critical",
            "channel_id": ch.get("channel_id"),
            "peer_id": ch.get("peer_id"),
            "details": {"local_pct": ch.get("local_pct")}
        })

    # Sort issues: critical first, then warning, then info
    severity_rank = {"critical": 0, "warning": 1, "info": 2}
    issues_sorted = sorted(issues, key=lambda x: severity_rank.get(x.get("severity", "info"), 3))
    top_issues = issues_sorted[:3]

    return {
        "node": node.name,
        "health": {
            "alias": info.get("alias", "unknown"),
            "pubkey": info.get("id", "unknown"),
            "blockheight": info.get("blockheight", 0),
            "peers": len(peers.get("peers", [])),
            "sync_status": info.get("warning_bitcoind_sync", "") or info.get("warning_lightningd_sync", "")
        },
        "channels": {
            "count": channel_count,
            "total_capacity_msat": total_capacity_msat,
            "total_local_msat": total_local_msat,
            "local_balance_pct": local_balance_pct
        },
        "routing_24h": {
            "forward_count": forward_count,
            "total_volume_msat": total_volume_msat,
            "total_revenue_msat": total_revenue_msat
        },
        "pending_actions": len(pending.get("actions", [])),
        "top_issues": top_issues
    }


async def handle_health(args: Dict) -> Dict:
    """Quick health check on all nodes."""
    timeout = args.get("timeout", 5.0)
    return await fleet.health_check(timeout=timeout)


async def handle_fleet_snapshot(args: Dict) -> Dict:
    """Get consolidated fleet snapshot."""
    node_name = args.get("node")

    if node_name:
        node = fleet.get_node(node_name)
        if not node:
            return {"error": f"Unknown node: {node_name}"}
        return await _node_fleet_snapshot(node)

    tasks = []
    for node in fleet.nodes.values():
        tasks.append(_node_fleet_snapshot(node))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    snapshots = {}
    for idx, result in enumerate(results):
        node = list(fleet.nodes.values())[idx]
        if isinstance(result, Exception):
            snapshots[node.name] = {"error": str(result)}
        else:
            snapshots[node.name] = result
    return snapshots


async def _node_anomalies(node: NodeConnection) -> Dict[str, Any]:
    import time

    anomalies: List[Dict[str, Any]] = []
    now = int(time.time())

    # Revenue velocity drop: last 24h vs 7-day daily average
    forwards = await node.call("listforwards", {"status": "settled"})
    forwards_list = forwards.get("forwards", [])
    last_24h = _forward_stats(forwards_list, now - 86400, now)
    last_7d = _forward_stats(forwards_list, now - (7 * 86400), now)
    avg_daily_revenue = last_7d["total_revenue_msat"] / 7 if last_7d["total_revenue_msat"] else 0

    if avg_daily_revenue > 0 and last_24h["total_revenue_msat"] < avg_daily_revenue * 0.5:
        anomalies.append({
            "type": "revenue_velocity_drop",
            "severity": "warning",
            "channel": None,
            "peer": None,
            "details": {
                "last_24h_revenue_msat": last_24h["total_revenue_msat"],
                "avg_daily_revenue_msat": int(avg_daily_revenue)
            },
            "recommendation": "Investigate fee changes, liquidity imbalance, or peer connectivity issues."
        })

    # Drain patterns: channels losing >10% balance per day (requires advisor DB velocity)
    try:
        db = ensure_advisor_db()
        channels = await node.call("listpeerchannels")
        for ch in channels.get("channels", []):
            scid = ch.get("short_channel_id")
            if not scid:
                continue
            velocity = db.get_channel_velocity(node.name, scid)
            if not velocity:
                continue
            # 10% per day ~= 0.4167% per hour
            if velocity.velocity_pct_per_hour <= -0.4167:
                anomalies.append({
                    "type": "drain_pattern",
                    "severity": "critical" if velocity.velocity_pct_per_hour <= -1.0 else "warning",
                    "channel": scid,
                    "peer": ch.get("peer_id"),
                    "details": {
                        "velocity_pct_per_hour": round(velocity.velocity_pct_per_hour, 3),
                        "trend": velocity.trend,
                        "hours_until_depleted": velocity.hours_until_depleted
                    },
                    "recommendation": "Consider rebalancing or adjusting fees to slow depletion."
                })
    except Exception:
        pass

    # Peer connectivity: frequent disconnects (best-effort heuristics)
    peers = await node.call("listpeers")
    for peer in peers.get("peers", []):
        peer_id = peer.get("id")
        num_disconnects = peer.get("num_disconnects") or peer.get("disconnects")
        num_connects = peer.get("num_connects") or peer.get("connects")
        if num_disconnects is None:
            continue
        if num_disconnects >= 5 and (num_connects is None or num_disconnects > num_connects):
            anomalies.append({
                "type": "peer_disconnects",
                "severity": "warning",
                "channel": None,
                "peer": peer_id,
                "details": {
                    "num_disconnects": num_disconnects,
                    "num_connects": num_connects
                },
                "recommendation": "Monitor peer reliability and consider defensive fee policy."
            })

    return {
        "node": node.name,
        "anomalies": anomalies
    }


async def handle_anomalies(args: Dict) -> Dict:
    """Detect anomalies outside normal ranges."""
    node_name = args.get("node")

    if node_name:
        node = fleet.get_node(node_name)
        if not node:
            return {"error": f"Unknown node: {node_name}"}
        return await _node_anomalies(node)

    tasks = [ _node_anomalies(node) for node in fleet.nodes.values() ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    output = {}
    for idx, result in enumerate(results):
        node = list(fleet.nodes.values())[idx]
        if isinstance(result, Exception):
            output[node.name] = {"error": str(result)}
        else:
            output[node.name] = result
    return output


async def handle_compare_periods(args: Dict) -> Dict:
    """Compare two routing periods for a node."""
    import time

    node_name = args.get("node")
    period1_days = int(args.get("period1_days", 7))
    period2_days = int(args.get("period2_days", 7))
    offset_days = int(args.get("offset_days", 7))

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    now = int(time.time())
    p1_start = now - (period1_days * 86400)
    p1_end = now
    p2_end = now - (offset_days * 86400)
    p2_start = p2_end - (period2_days * 86400)

    forwards = await node.call("listforwards", {"status": "settled"})
    forwards_list = forwards.get("forwards", [])

    p1 = _forward_stats(forwards_list, p1_start, p1_end)
    p2 = _forward_stats(forwards_list, p2_start, p2_end)

    def metric_compare(key: str) -> Dict[str, Any]:
        v1 = p1.get(key, 0)
        v2 = p2.get(key, 0)
        delta = v1 - v2
        pct = round((delta / v2) * 100, 2) if v2 else None
        return {"period1": v1, "period2": v2, "delta": delta, "percent_change": pct}

    metrics = {
        "total_revenue_msat": metric_compare("total_revenue_msat"),
        "total_volume_msat": metric_compare("total_volume_msat"),
        "forward_count": metric_compare("forward_count"),
        "avg_fee_ppm": metric_compare("avg_fee_ppm")
    }

    # Channel improvements/degradations based on revenue delta
    channel_deltas: List[Dict[str, Any]] = []
    all_channels = set(p1["per_channel"].keys()) | set(p2["per_channel"].keys())
    for ch_id in all_channels:
        rev1 = p1["per_channel"].get(ch_id, {}).get("revenue_msat", 0)
        rev2 = p2["per_channel"].get(ch_id, {}).get("revenue_msat", 0)
        delta = rev1 - rev2
        pct = round((delta / rev2) * 100, 2) if rev2 else None
        channel_deltas.append({
            "channel_id": ch_id,
            "period1_revenue_msat": rev1,
            "period2_revenue_msat": rev2,
            "delta_revenue_msat": delta,
            "percent_change": pct
        })

    improved = sorted(channel_deltas, key=lambda x: x["delta_revenue_msat"], reverse=True)[:5]
    degraded = sorted(channel_deltas, key=lambda x: x["delta_revenue_msat"])[:5]

    return {
        "node": node_name,
        "periods": {
            "period1": {"start_ts": p1_start, "end_ts": p1_end, "days": period1_days},
            "period2": {"start_ts": p2_start, "end_ts": p2_end, "days": period2_days, "offset_days": offset_days}
        },
        "metrics": metrics,
        "improved_channels": improved,
        "degraded_channels": degraded
    }


async def handle_channel_deep_dive(args: Dict) -> Dict:
    """Get comprehensive context for a channel or peer."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    peer_id = args.get("peer_id")

    if not node_name:
        return {"error": "node is required"}
    if not channel_id and not peer_id:
        return {"error": "channel_id or peer_id is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Resolve channel and peer from listpeerchannels
    channels_result = await node.call("listpeerchannels")
    channels = channels_result.get("channels", [])
    target_channel = None
    if channel_id:
        for ch in channels:
            if ch.get("short_channel_id") == channel_id:
                target_channel = ch
                peer_id = ch.get("peer_id")
                break
    elif peer_id:
        for ch in channels:
            if ch.get("peer_id") == peer_id:
                target_channel = ch
                channel_id = ch.get("short_channel_id")
                break

    if not target_channel:
        return {"error": "Channel not found for given channel_id/peer_id"}

    # Basic info
    totals = _channel_totals(target_channel)
    total_msat = totals["total_msat"]
    local_msat = totals["local_msat"]
    remote_msat = max(0, total_msat - local_msat)
    local_pct = round((local_msat / total_msat) * 100, 2) if total_msat else 0.0

    # Gather remaining RPC calls in parallel (all independent after finding target_channel)
    peers, prof, debug, forwards = await asyncio.gather(
        node.call("listpeers"),
        node.call("revenue-profitability", {"channel_id": channel_id}),
        node.call("revenue-fee-debug"),
        node.call("listforwards", {"status": "settled"}),
        return_exceptions=True,
    )

    # Process peers result
    if isinstance(peers, Exception):
        peers = {"peers": []}
    peer_info = next((p for p in peers.get("peers", []) if p.get("id") == peer_id), {})
    peer_alias = peer_info.get("alias") or peer_info.get("alias_or_local", "") or ""
    connected = bool(peer_info.get("connected", False))

    # Fallback to listnodes if peer not in listpeers (disconnected peer)
    if not peer_alias and peer_id:
        try:
            nodes_result = await node.call("listnodes", {"id": peer_id})
            if nodes_result.get("nodes"):
                peer_alias = nodes_result["nodes"][0].get("alias", "")
        except Exception:
            pass  # Best effort fallback

    # Calculate channel age from SCID
    channel_age_days = None
    try:
        info_result = await node.call("getinfo")
        current_blockheight = info_result.get("blockheight", 0)
        if current_blockheight and channel_id:
            channel_age_days = _scid_to_age_days(channel_id, current_blockheight)
    except Exception:
        pass  # Best effort

    # Profitability
    profitability = {}
    if not isinstance(prof, Exception):
        prof_data = prof.get("profitability", {})
        if prof_data:
            profitability = {
                "lifetime_revenue_sats": prof_data.get("total_contribution_sats", 0),
                "lifetime_cost_sats": prof_data.get("total_costs_sats", 0),
                "net_profit_sats": prof_data.get("net_profit_sats", 0),
                "roi_percentage": prof_data.get("roi_percentage", 0),
                "classification": prof_data.get("profitability_class", "unknown"),
                "forward_count": prof_data.get("forward_count", 0),
                "volume_routed_sats": prof_data.get("volume_routed_sats", 0),
                "flow_profile": prof_data.get("flow_profile", "unknown"),
                "days_active": prof_data.get("days_active", 0),
            }
    else:
        logger.debug(f"Could not fetch profitability for {channel_id}: {prof}")

    # Flow analysis + velocity
    flow = _flow_profile(target_channel)
    velocity = None
    try:
        db = ensure_advisor_db()
        velocity = db.get_channel_velocity(node_name, channel_id)
    except Exception:
        velocity = None

    flow_analysis = {
        "classification": flow.get("flow_profile"),
        "inbound_outbound_ratio": flow.get("inbound_outbound_ratio"),
        "recent_volumes_sats": {
            "inbound": flow.get("inbound_volume_sats"),
            "outbound": flow.get("outbound_volume_sats")
        },
        "velocity": {
            "sats_per_hour": getattr(velocity, "velocity_sats_per_hour", None),
            "pct_per_hour": getattr(velocity, "velocity_pct_per_hour", None),
            "trend": getattr(velocity, "trend", None),
            "hours_until_depleted": getattr(velocity, "hours_until_depleted", None),
            "hours_until_full": getattr(velocity, "hours_until_full", None)
        } if velocity else None
    }

    # Fee history (best-effort)
    local_updates = target_channel.get("updates", {}).get("local", {})
    fee_history = {
        "current_fee_ppm": local_updates.get("fee_proportional_millionths", 0),
        "current_base_fee_msat": local_updates.get("fee_base_msat", 0),
        "recent_changes": None
    }
    if not isinstance(debug, Exception):
        fee_history["recent_changes"] = debug.get("recent_fee_changes")

    # Process forwards result
    if isinstance(forwards, Exception):
        forwards = {"forwards": []}
    recent = []
    for fwd in sorted(
        forwards.get("forwards", []),
        key=lambda f: _coerce_ts(f.get("resolved_time") or f.get("resolved_at") or 0),
        reverse=True
    ):
        if fwd.get("out_channel") == channel_id or fwd.get("in_channel") == channel_id:
            in_msat = _extract_msat(fwd.get("in_msat"))
            out_msat = _extract_msat(fwd.get("out_msat"))
            recent.append({
                "resolved_time": _coerce_ts(fwd.get("resolved_time") or fwd.get("resolved_at") or 0),
                "in_msat": in_msat,
                "out_msat": out_msat,
                "fee_msat": max(0, in_msat - out_msat)
            })
        if len(recent) >= 10:
            break

    # Issues
    issues = []
    if local_pct < 20:
        issues.append({"type": "critical_low_balance", "severity": "critical", "details": {"local_pct": local_pct}})
    if profitability.get("classification") in {"bleeder", "zombie"}:
        issues.append({
            "type": profitability.get("classification"),
            "severity": "warning" if profitability.get("classification") == "bleeder" else "info"
        })

    return {
        "node": node_name,
        "channel_id": channel_id,
        "peer_id": peer_id,
        "basic": {
            "capacity_msat": total_msat,
            "local_msat": local_msat,
            "remote_msat": remote_msat,
            "local_balance_pct": local_pct,
            "peer_alias": peer_alias,
            "connected": connected,
            "channel_age_days": channel_age_days
        },
        "profitability": profitability,
        "flow_analysis": flow_analysis,
        "fee_history": fee_history,
        "recent_forwards": recent,
        "issues": issues
    }


def _action_priority(action: Dict[str, Any]) -> Dict[str, Any]:
    action_type = action.get("action_type", "")
    base = 5
    effort = "medium"
    impact = "moderate"

    if action_type in {"channel_open", "channel_close"}:
        base = 7
        effort = "involved"
        impact = "high"
    elif action_type in {"fee_change", "set_fee"}:
        base = 6
        effort = "quick"
        impact = "moderate"
    elif action_type in {"rebalance", "circular_rebalance"}:
        base = 6
        effort = "medium"
        impact = "moderate"

    return {"priority": base, "effort": effort, "impact": impact}


async def _node_recommended_actions(node: NodeConnection, limit: int) -> Dict[str, Any]:
    actions: List[Dict[str, Any]] = []

    pending = await node.call("hive-pending-actions")
    for action in pending.get("actions", []):
        meta = _action_priority(action)
        actions.append({
            "source": "pending_action",
            "node": node.name,
            "action": action,
            "priority": meta["priority"],
            "reasoning": action.get("reasoning") or action.get("reason") or "Pending action requires review.",
            "expected_impact": meta["impact"],
            "effort": meta["effort"]
        })

    # Add anomaly-driven recommendations
    anomalies = await _node_anomalies(node)
    for a in anomalies.get("anomalies", []):
        priority = 7 if a.get("severity") == "critical" else 5
        effort = "quick" if a.get("type") in {"revenue_velocity_drop", "peer_disconnects"} else "medium"
        actions.append({
            "source": "anomaly",
            "node": node.name,
            "action": {
                "type": a.get("type"),
                "channel": a.get("channel"),
                "peer": a.get("peer")
            },
            "priority": priority,
            "reasoning": a.get("recommendation"),
            "expected_impact": "moderate" if priority <= 6 else "high",
            "effort": effort
        })

    actions_sorted = sorted(actions, key=lambda x: x.get("priority", 0), reverse=True)
    return {"node": node.name, "actions": actions_sorted[:limit]}


async def handle_recommended_actions(args: Dict) -> Dict:
    """Return prioritized list of recommended actions."""
    node_name = args.get("node")
    limit = int(args.get("limit", 10))

    if node_name:
        node = fleet.get_node(node_name)
        if not node:
            return {"error": f"Unknown node: {node_name}"}
        return await _node_recommended_actions(node, limit)

    tasks = [_node_recommended_actions(node, limit) for node in fleet.nodes.values()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    output = {}
    for idx, result in enumerate(results):
        node = list(fleet.nodes.values())[idx]
        if isinstance(result, Exception):
            output[node.name] = {"error": str(result)}
        else:
            output[node.name] = result
    return output


async def _node_peer_search(node: NodeConnection, query: str) -> Dict[str, Any]:
    query_lower = query.lower()

    peers, channels_result, nodes_result = await asyncio.gather(
        node.call("listpeers"),
        node.call("listpeerchannels"),
        node.call("listnodes"),
        return_exceptions=True,
    )

    # Handle potential exceptions from gather
    if isinstance(peers, Exception):
        peers = {"peers": []}
    if isinstance(channels_result, Exception):
        channels_result = {"channels": []}
    channels = channels_result.get("channels", [])

    # Build pubkey -> alias map from listnodes (best-effort)
    alias_map = {}
    if not isinstance(nodes_result, Exception):
        for n in nodes_result.get("nodes", []):
            pubkey = n.get("nodeid")
            alias = n.get("alias")
            if pubkey and alias:
                alias_map[pubkey] = alias

    channel_by_peer = {}
    for ch in channels:
        peer_id = ch.get("peer_id")
        if not peer_id:
            continue
        channel_by_peer.setdefault(peer_id, []).append(ch)

    matches = []
    for peer in peers.get("peers", []):
        peer_id = peer.get("id")
        alias = alias_map.get(peer_id) or peer.get("alias") or peer.get("alias_or_local") or ""
        if query_lower not in alias.lower():
            continue

        # Use first channel if multiple
        ch = None
        if peer_id in channel_by_peer:
            ch = channel_by_peer[peer_id][0]

        capacity_sats = 0
        local_balance_pct = None
        channel_id = None
        if ch:
            totals = _channel_totals(ch)
            total_msat = totals["total_msat"]
            local_msat = totals["local_msat"]
            capacity_sats = total_msat // 1000 if total_msat else 0
            local_balance_pct = round((local_msat / total_msat) * 100, 2) if total_msat else None
            channel_id = ch.get("short_channel_id")

        matches.append({
            "pubkey": peer_id,
            "alias": alias,
            "channel_id": channel_id,
            "capacity_sats": capacity_sats,
            "local_balance_pct": local_balance_pct,
            "connected": bool(peer.get("connected", False))
        })

    return {"node": node.name, "matches": matches}


async def handle_peer_search(args: Dict) -> Dict:
    """Search peers by alias substring."""
    query = args.get("query", "")
    node_name = args.get("node")

    if not query:
        return {"error": "query is required"}

    if node_name:
        node = fleet.get_node(node_name)
        if not node:
            return {"error": f"Unknown node: {node_name}"}
        return await _node_peer_search(node, query)

    tasks = [_node_peer_search(node, query) for node in fleet.nodes.values()]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    output = {}
    for idx, result in enumerate(results):
        node = list(fleet.nodes.values())[idx]
        if isinstance(result, Exception):
            output[node.name] = {"error": str(result)}
        else:
            output[node.name] = result
    return output


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
        return await fleet.call_all("hive-pending-actions")


async def handle_approve_action(args: Dict) -> Dict:
    """Approve a pending action."""
    node_name = args.get("node")
    action_id = args.get("action_id")
    reason = args.get("reason", "Approved by Claude Code")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    logger.info(f"Approving action {action_id} on {node_name}: {reason}")

    # Record approval reason in advisor DB if available
    try:
        db = ensure_advisor_db()
        db.record_decision(
            decision_type="approve_action",
            node_name=node_name,
            recommendation=f"Approved action {action_id}",
            reasoning=reason
        )
    except Exception:
        pass  # Advisor DB is optional

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

    params = {"action_id": action_id}
    if reason:
        params["reason"] = reason
    return await node.call("hive-reject-action", params)


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


async def handle_onboard_new_members(args: Dict) -> Dict:
    """
    Detect new hive members and generate strategic channel suggestions.

    Runs independently of the advisor cycle to provide immediate onboarding
    support when new members join the hive.
    """
    import time

    node_name = args.get("node")
    dry_run = args.get("dry_run", False)

    if not node_name:
        return {"error": "node is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Initialize advisor DB for onboarding tracking (uses configured ADVISOR_DB_PATH)
    db = ensure_advisor_db()

    # Gather required data in parallel
    try:
        members_data, node_info, channels_data = await asyncio.gather(
            node.call("hive-members"),
            node.call("getinfo"),
            node.call("listpeerchannels"),
        )
    except Exception as e:
        return {"error": f"Failed to gather node data: {e}"}

    our_pubkey = node_info.get("id", "")
    members_list = members_data.get("members", [])

    # Get our current peers
    our_peers = set()
    for ch in channels_data.get("channels", []):
        peer_id = ch.get("peer_id")
        if peer_id:
            our_peers.add(peer_id)

    # Try to get positioning data for strategic targets
    positioning = {}
    try:
        positioning = await handle_positioning_summary({"node": node_name})
    except Exception:
        pass  # Positioning data is optional

    valuable_corridors = positioning.get("valuable_corridors", [])
    exchange_gaps = positioning.get("exchange_gaps", [])

    # Find new members that need onboarding
    new_members_found = []
    suggestions_created = []
    already_onboarded = []

    for member in members_list:
        member_pubkey = member.get("pubkey") or member.get("peer_id")
        member_alias = member.get("alias", "")
        tier = member.get("tier", "unknown")
        joined_at = member.get("joined_at", 0)

        if not member_pubkey:
            continue

        # Skip ourselves
        if member_pubkey == our_pubkey:
            continue

        # Check if this is a new member (neophyte or recently joined)
        is_neophyte = tier == "neophyte"
        is_recent = False
        if joined_at:
            age_days = (time.time() - joined_at) / 86400
            is_recent = age_days < 30

        # Skip if not new
        if not is_neophyte and not is_recent:
            continue

        # Check if already onboarded
        if db.is_member_onboarded(member_pubkey):
            already_onboarded.append({
                "pubkey": member_pubkey[:16] + "...",
                "alias": member_alias,
                "tier": tier
            })
            continue

        new_members_found.append({
            "pubkey": member_pubkey,
            "alias": member_alias,
            "tier": tier,
            "is_neophyte": is_neophyte,
            "age_days": (time.time() - joined_at) / 86400 if joined_at else None
        })

        # Generate suggestions for this new member

        # 1. Suggest we open a channel to them (if we don't have one)
        if member_pubkey not in our_peers:
            suggestion = {
                "type": "open_channel_to_new_member",
                "target_pubkey": member_pubkey,
                "target_alias": member_alias,
                "target_tier": tier,
                "recommended_size_sats": 3000000,  # 3M sats default
                "reasoning": f"New {tier} member joined hive. Opening a channel strengthens fleet connectivity."
            }

            if not dry_run:
                # Create pending_action for this suggestion
                try:
                    await node.call("hive-test-pending-action", {
                        "action_type": "channel_open",
                        "target": member_pubkey,
                        "capacity_sats": 3000000,
                        "reason": f"onboard_{member_alias}"
                    })
                    suggestion["pending_action_created"] = True
                except Exception as e:
                    suggestion["pending_action_created"] = False
                    suggestion["error"] = str(e) or type(e).__name__

            suggestions_created.append(suggestion)

        # 2. Suggest strategic targets for the new member
        for corridor in valuable_corridors[:2]:
            target_peer = corridor.get("target_peer") or corridor.get("destination_peer_id")
            if not target_peer:
                continue

            score = corridor.get("value_score", 0)
            if score < 0.3:
                continue

            suggestion = {
                "type": "suggest_target_for_new_member",
                "new_member_pubkey": member_pubkey[:16] + "...",
                "new_member_alias": member_alias,
                "suggested_target": target_peer[:16] + "...",
                "corridor_value_score": score,
                "reasoning": f"New member could strengthen fleet coverage of high-value corridor (score: {score:.2f})"
            }
            suggestions_created.append(suggestion)

        # 3. Suggest exchange connections for the new member
        for exchange in exchange_gaps[:1]:
            exchange_pubkey = exchange.get("pubkey")
            exchange_name = exchange.get("name", "Unknown Exchange")

            if not exchange_pubkey:
                continue

            suggestion = {
                "type": "suggest_exchange_for_new_member",
                "new_member_pubkey": member_pubkey[:16] + "...",
                "new_member_alias": member_alias,
                "suggested_exchange": exchange_name,
                "exchange_pubkey": exchange_pubkey[:16] + "...",
                "reasoning": f"Fleet lacks connection to {exchange_name}. New member could fill this gap."
            }
            suggestions_created.append(suggestion)

        # Mark as onboarded (unless dry run)
        if not dry_run:
            db.mark_member_onboarded(member_pubkey)

    return {
        "node": node_name,
        "dry_run": dry_run,
        "new_members_found": len(new_members_found),
        "new_members": new_members_found,
        "suggestions_created": len(suggestions_created),
        "suggestions": suggestions_created,
        "already_onboarded": len(already_onboarded),
        "already_onboarded_members": already_onboarded,
        "summary": f"Found {len(new_members_found)} new members, created {len(suggestions_created)} suggestions"
                   + (" (dry run - no actions taken)" if dry_run else "")
    }


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

    return await node.call("hive-propose-promotion", {
        "target_peer_id": target_peer_id,
        "proposer_peer_id": proposer_peer_id
    })


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

    return await node.call("hive-vote-promotion", {
        "target_peer_id": target_peer_id,
        "voter_peer_id": voter_peer_id
    })


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

    return await node.call("hive-execute-promotion", {"target_peer_id": target_peer_id})


# =============================================================================
# Membership Lifecycle Handlers
# =============================================================================

async def handle_vouch(args: Dict) -> Dict:
    """Vouch for a neophyte."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-vouch", {"peer_id": peer_id})


async def handle_leave(args: Dict) -> Dict:
    """Leave the hive voluntarily."""
    node_name = args.get("node")
    reason = args.get("reason", "voluntary")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-leave", {"reason": reason})


async def handle_force_promote(args: Dict) -> Dict:
    """Force-promote a neophyte during bootstrap."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-force-promote", {"peer_id": peer_id})


async def handle_request_promotion(args: Dict) -> Dict:
    """Request promotion from neophyte to member."""
    node_name = args.get("node")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-request-promotion")


async def handle_remove_member(args: Dict) -> Dict:
    """Remove a member from the hive."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    reason = args.get("reason", "maintenance")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-remove-member", {"peer_id": peer_id, "reason": reason})


async def handle_genesis(args: Dict) -> Dict:
    """Initialize a new hive."""
    node_name = args.get("node")
    hive_id = args.get("hive_id")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    params = {}
    if hive_id:
        params["hive_id"] = hive_id
    return await node.call("hive-genesis", params)


async def handle_invite(args: Dict) -> Dict:
    """Generate an invitation ticket."""
    node_name = args.get("node")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    params = {}
    if args.get("valid_hours") is not None:
        params["valid_hours"] = args["valid_hours"]
    if args.get("tier"):
        params["tier"] = args["tier"]
    return await node.call("hive-invite", params)


async def handle_join(args: Dict) -> Dict:
    """Join a hive using an invitation ticket."""
    node_name = args.get("node")
    ticket = args.get("ticket")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    params = {"ticket": ticket}
    if args.get("peer_id"):
        params["peer_id"] = args["peer_id"]
    return await node.call("hive-join", params)


# =============================================================================
# Ban Governance Handlers
# =============================================================================

async def handle_propose_ban(args: Dict) -> Dict:
    """Propose banning a member."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    reason = args.get("reason", "no reason given")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-propose-ban", {"peer_id": peer_id, "reason": reason})


async def handle_vote_ban(args: Dict) -> Dict:
    """Vote on a pending ban proposal."""
    node_name = args.get("node")
    proposal_id = args.get("proposal_id")
    vote = args.get("vote")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-vote-ban", {"proposal_id": proposal_id, "vote": vote})


async def handle_pending_bans(args: Dict) -> Dict:
    """View pending ban proposals."""
    node_name = args.get("node")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-pending-bans")


# =============================================================================
# Health/Reputation Monitoring Handlers
# =============================================================================

async def handle_nnlb_status(args: Dict) -> Dict:
    """Get NNLB (No Node Left Behind) status."""
    node_name = args.get("node")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-nnlb-status")


async def handle_peer_reputations(args: Dict) -> Dict:
    """Get aggregated peer reputations."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    params = {}
    if peer_id:
        params["peer_id"] = peer_id
    return await node.call("hive-peer-reputations", params)


async def handle_reputation_stats(args: Dict) -> Dict:
    """Get overall reputation tracking statistics."""
    node_name = args.get("node")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    return await node.call("hive-reputation-stats")


async def handle_contribution(args: Dict) -> Dict:
    """View contribution stats for a peer."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    params = {}
    if peer_id:
        params["peer_id"] = peer_id
    return await node.call("hive-contribution", params)


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
            channel["inbound_outbound_ratio"] = inbound_outbound_ratio if inbound_outbound_ratio != float('inf') else 999.99
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
                            channel["forward_count"] = ch.get("forward_count", 0)
                            channel["fees_earned_sats"] = ch.get("fees_earned_sats", 0)
                            channel["volume_routed_sats"] = ch.get("volume_routed_sats", 0)
                            break

    return channels_result


async def handle_set_fees(args: Dict) -> Dict:
    """Set channel fees. Routes through cl-revenue-ops to enforce hive zero-fee policy."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    fee_ppm = args.get("fee_ppm")
    base_fee_msat = args.get("base_fee_msat", 0)
    force = args.get("force", False)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Guard: check if the target channel peer is a hive member (zero-fee policy)
    if fee_ppm and int(fee_ppm) > 0 and not force:
        try:
            members_result = await node.call("hive-members")
            member_ids = {m.get("peer_id") for m in members_result.get("members", [])}
            # Resolve channel_id to peer_id
            channels = await node.call("listpeerchannels")
            for ch in channels.get("channels", []):
                scid = ch.get("short_channel_id", "")
                peer_id = ch.get("peer_id", "")
                if scid == channel_id or peer_id == channel_id:
                    if peer_id in member_ids:
                        return {
                            "error": "Cannot set non-zero fees on hive member channel",
                            "channel_id": channel_id,
                            "peer_id": peer_id,
                            "hint": "Hive channels must have 0 fees. Use force=true to override."
                        }
                    break
        except Exception:
            pass  # Fail open on guard check — setchannel itself will still work

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

    # Get planner log, topology info, and expansion recommendations in parallel
    planner_log, topology, expansion_recs = await asyncio.gather(
        node.call("hive-planner-log", {"limit": 10}),
        node.call("hive-topology"),
        node.call("hive-expansion-recommendations", {"limit": 10}),
        return_exceptions=True,
    )

    # Handle potential exceptions
    if isinstance(planner_log, Exception):
        planner_log = {"error": str(planner_log)}
    if isinstance(topology, Exception):
        topology = {"error": str(topology)}
    if isinstance(expansion_recs, Exception):
        expansion_recs = {"error": str(expansion_recs), "recommendations": []}

    return {
        "planner_log": planner_log,
        "topology": topology,
        "expansion_recommendations": expansion_recs.get("recommendations", []),
        "coverage_summary": expansion_recs.get("coverage_summary", {}),
        "cooperation_modules": expansion_recs.get("cooperation_modules", {})
    }


async def handle_planner_ignore(args: Dict) -> Dict:
    """Add a peer to the planner ignore list."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")
    reason = args.get("reason", "manual")
    duration_hours = args.get("duration_hours", 0)

    if not node_name or not peer_id:
        return {"error": "node and peer_id are required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-planner-ignore", {
        "peer_id": peer_id,
        "reason": reason,
        "duration_hours": duration_hours
    })


async def handle_planner_unignore(args: Dict) -> Dict:
    """Remove a peer from the planner ignore list."""
    node_name = args.get("node")
    peer_id = args.get("peer_id")

    if not node_name or not peer_id:
        return {"error": "node and peer_id are required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-planner-unignore", {"peer_id": peer_id})


async def handle_planner_ignored_peers(args: Dict) -> Dict:
    """Get list of ignored peers."""
    node_name = args.get("node")
    include_expired = args.get("include_expired", False)

    if not node_name:
        return {"error": "node is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("hive-planner-ignored-peers", {
        "include_expired": include_expired
    })


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


async def handle_splice(args: Dict) -> Dict:
    """
    Execute a coordinated splice operation with a hive member.

    Splices resize channels without closing them:
    - Positive amount = splice-in (add funds from on-chain)
    - Negative amount = splice-out (remove funds to on-chain)

    The initiating node provides the on-chain funds for splice-in.
    """
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    relative_amount = args.get("relative_amount")
    feerate_per_kw = args.get("feerate_per_kw")
    dry_run = args.get("dry_run", False)
    force = args.get("force", False)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {
        "channel_id": channel_id,
        "relative_amount": relative_amount,
        "dry_run": dry_run,
        "force": force
    }
    if feerate_per_kw is not None:
        params["feerate_per_kw"] = feerate_per_kw

    result = await node.call("hive-splice", params)

    # Add context about the result
    if result.get("dry_run"):
        result["ai_note"] = (
            f"Dry run preview: {result.get('splice_type')} of {result.get('amount_sats'):,} sats "
            f"on channel {channel_id}. Remove dry_run=true to execute."
        )
    elif result.get("success"):
        result["ai_note"] = (
            f"Splice initiated successfully. Session: {result.get('session_id')}. "
            f"Status: {result.get('status')}. Monitor with hive_splice_status."
        )
    elif result.get("error"):
        result["ai_note"] = f"Splice failed: {result.get('message', result.get('error'))}"

    return result


async def handle_splice_status(args: Dict) -> Dict:
    """
    Get status of active splice sessions.

    Shows ongoing splice operations and their current state.
    """
    node_name = args.get("node")
    session_id = args.get("session_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {}
    if session_id:
        params["session_id"] = session_id

    return await node.call("hive-splice-status", params)


async def handle_splice_abort(args: Dict) -> Dict:
    """
    Abort an active splice session.

    Use this if a splice is stuck or needs to be cancelled.
    """
    node_name = args.get("node")
    session_id = args.get("session_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-splice-abort", {"session_id": session_id})

    if result.get("success"):
        result["ai_note"] = f"Splice session {session_id} aborted successfully."

    return result


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
# Routing Intelligence Handlers (Pheromones + Stigmergic Markers)
# =============================================================================

async def handle_backfill_routing_intelligence(args: Dict) -> Dict:
    """
    Backfill pheromone levels and stigmergic markers from historical forwards.

    Reads historical forward data and populates the fee coordination systems
    to bootstrap swarm intelligence.
    """
    node_name = args.get("node")
    days = args.get("days", 30)
    status_filter = args.get("status_filter", "settled")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-backfill-routing-intelligence", {
        "days": days,
        "status_filter": status_filter
    })

    # Add AI summary
    if result.get("status") == "success":
        processed = result.get("processed", 0)
        pheromone_channels = result.get("current_pheromone_channels", 0)
        active_markers = result.get("current_active_markers", 0)
        result["ai_summary"] = (
            f"Backfill complete: processed {processed} forwards from {days} days. "
            f"Pheromone levels on {pheromone_channels} channels, "
            f"{active_markers} stigmergic markers active. "
            "Future forwards will now update swarm intelligence automatically."
        )
    elif result.get("status") == "no_data":
        result["ai_summary"] = (
            f"No forwards found to backfill. "
            "Run this again after the node has processed some routing traffic."
        )
    else:
        result["ai_summary"] = f"Backfill failed: {result.get('error', 'unknown error')}"

    return result


async def handle_routing_intelligence_status(args: Dict) -> Dict:
    """
    Get current status of routing intelligence systems (pheromones + markers).

    Shows pheromone levels, stigmergic markers, and configuration.
    """
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result = await node.call("hive-routing-intelligence-status", {})

    # Add AI summary
    pheromone_count = result.get("pheromone_channels", 0)
    marker_count = result.get("active_markers", 0)
    successful = result.get("successful_markers", 0)
    failed = result.get("failed_markers", 0)

    if pheromone_count == 0 and marker_count == 0:
        result["status"] = "empty"
        result["ai_summary"] = (
            "No routing intelligence data yet. "
            "Run hive_backfill_routing_intelligence to populate from historical forwards, "
            "or wait for new forwards to accumulate."
        )
    else:
        result["status"] = "active"
        result["ai_summary"] = (
            f"Routing intelligence active: {pheromone_count} channels with pheromone levels, "
            f"{marker_count} stigmergic markers ({successful} successful, {failed} failed). "
            "This data helps coordinate fees across the hive."
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
        channels_by_class = profitability.get("channels_by_class", {})
        channels = []
        for class_channels in channels_by_class.values():
            if isinstance(class_channels, list):
                channels.extend(class_channels)

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
    """Get financial health dashboard with routing revenue."""
    node_name = args.get("node")
    window_days = args.get("window_days", 30)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get base dashboard from cl-revenue-ops (routing P&L)
    dashboard = await node.call("revenue-dashboard", {"window_days": window_days})

    if "error" in dashboard:
        return dashboard

    # Extract routing P&L data from cl-revenue-ops dashboard structure
    # Use defensive null handling - values may be None even with defaults
    period = dashboard.get("period", {})
    financial_health = dashboard.get("financial_health", {})
    routing_revenue = period.get("gross_revenue_sats") or 0
    routing_opex = period.get("opex_sats") or 0
    routing_net = financial_health.get("net_profit_sats") or 0

    operating_margin_pct = financial_health.get("operating_margin_pct") or 0.0

    pnl = {
        "routing": {
            "revenue_sats": routing_revenue,
            "opex_sats": routing_opex,
            "net_profit_sats": routing_net,
            "operating_margin_pct": operating_margin_pct,
            "opex_breakdown": {
                "rebalance_cost_sats": period.get("rebalance_cost_sats", 0),
                "closure_cost_sats": period.get("closure_cost_sats", 0),
                "splice_cost_sats": period.get("splice_cost_sats", 0),
            }
        }
    }

    # Update top-level fields for backwards compatibility
    pnl["gross_revenue_sats"] = routing_revenue
    pnl["net_profit_sats"] = routing_net
    pnl["operating_margin_pct"] = operating_margin_pct

    dashboard["pnl_summary"] = pnl

    return dashboard


async def handle_revenue_portfolio(args: Dict) -> Dict:
    """Full portfolio analysis using Mean-Variance optimization."""
    node_name = args.get("node")
    risk_aversion = args.get("risk_aversion", 1.0)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("revenue-portfolio", {"risk_aversion": risk_aversion})


async def handle_revenue_portfolio_summary(args: Dict) -> Dict:
    """Get lightweight portfolio summary metrics."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("revenue-portfolio-summary", {})


async def handle_revenue_portfolio_rebalance(args: Dict) -> Dict:
    """Get portfolio-optimized rebalance recommendations."""
    node_name = args.get("node")
    max_recommendations = args.get("max_recommendations", 5)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("revenue-portfolio-rebalance", {
        "max_recommendations": max_recommendations
    })


async def handle_revenue_portfolio_correlations(args: Dict) -> Dict:
    """Get channel correlation analysis."""
    node_name = args.get("node")
    min_correlation = args.get("min_correlation", 0.3)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    return await node.call("revenue-portfolio-correlations", {
        "min_correlation": min_correlation
    })


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


async def handle_config_adjust(args: Dict) -> Dict:
    """
    Adjust cl-revenue-ops config with tracking for analysis and learning.
    
    Records the adjustment in advisor database with context metrics,
    enabling outcome measurement and effectiveness analysis over time.
    
    Recommended config keys for advisor tuning:
    - min_fee_ppm: Fee floor (raise if drain detected, lower if stagnating)
    - max_fee_ppm: Fee ceiling (adjust based on competitive positioning)
    - daily_budget_sats: Rebalance budget (scale with profitability)
    - rebalance_max_amount: Max rebalance size
    - thompson_observation_decay_hours: Shorter in volatile conditions
    - hive_prior_weight: Trust in hive intelligence (0-1)
    - scarcity_threshold: When to apply scarcity pricing
    
    Args:
        node: Node name to adjust
        config_key: Config key to change
        new_value: New value to set
        trigger_reason: Why making this change (e.g., 'drain_detected', 'stagnation', 
                       'profitability_low', 'budget_exhausted', 'market_conditions')
        reasoning: Detailed explanation of the decision
        confidence: 0-1 confidence in the change
        context_metrics: Optional dict of relevant metrics at time of change
        
    Returns:
        Result including adjustment_id for later outcome tracking
    """
    node_name = args.get("node")
    config_key = args.get("config_key")
    new_value = args.get("new_value")
    trigger_reason = args.get("trigger_reason")
    reasoning = args.get("reasoning")
    confidence = args.get("confidence")
    context_metrics = args.get("context_metrics", {})
    
    if not all([node_name, config_key, new_value is not None, trigger_reason]):
        return {"error": "Required: node, config_key, new_value, trigger_reason"}
    
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    
    # Get current value first
    current_config = await node.call("revenue-config", {"action": "get", "key": config_key})
    if "error" in current_config:
        return current_config
    
    old_value = current_config.get("config", {}).get(config_key)
    
    # Apply the change
    result = await node.call("revenue-config", {
        "action": "set",
        "key": config_key,
        "value": str(new_value)  # revenue-config expects string values
    })
    
    if "error" in result:
        return result
    
    # Record in advisor database
    db = ensure_advisor_db()
    adjustment_id = db.record_config_adjustment(
        node_name=node_name,
        config_key=config_key,
        old_value=old_value,
        new_value=new_value,
        trigger_reason=trigger_reason,
        reasoning=reasoning,
        confidence=confidence,
        context_metrics=context_metrics
    )
    
    return {
        "success": True,
        "adjustment_id": adjustment_id,
        "node": node_name,
        "config_key": config_key,
        "old_value": old_value,
        "new_value": new_value,
        "trigger_reason": trigger_reason,
        "message": f"Config {config_key} changed from {old_value} to {new_value}. "
                   f"Track outcome with adjustment_id={adjustment_id}"
    }


async def handle_config_adjustment_history(args: Dict) -> Dict:
    """
    Get history of config adjustments for analysis.
    
    Use this to review what changes were made, why, and their outcomes.
    
    Args:
        node: Filter by node (optional)
        config_key: Filter by specific config key (optional)
        days: How far back to look (default: 30)
        limit: Max records (default: 50)
        
    Returns:
        List of adjustment records with outcomes
    """
    node_name = args.get("node")
    config_key = args.get("config_key")
    days = args.get("days", 30)
    limit = args.get("limit", 50)
    
    db = ensure_advisor_db()
    history = db.get_config_adjustment_history(
        node_name=node_name,
        config_key=config_key,
        days=days,
        limit=limit
    )
    
    # Parse JSON fields for readability
    for record in history:
        for field in ['old_value', 'new_value', 'context_metrics', 'outcome_metrics']:
            if record.get(field):
                try:
                    record[field] = json.loads(record[field])
                except (json.JSONDecodeError, TypeError):
                    pass
    
    return {
        "count": len(history),
        "adjustments": history
    }


async def handle_config_effectiveness(args: Dict) -> Dict:
    """
    Analyze effectiveness of config adjustments.
    
    Shows success rates, learned optimal ranges, and recommendations
    based on historical adjustment outcomes.
    
    Args:
        node: Filter by node (optional)
        config_key: Filter by specific config key (optional)
        
    Returns:
        Effectiveness analysis with learned ranges and success rates
    """
    node_name = args.get("node")
    config_key = args.get("config_key")
    
    db = ensure_advisor_db()
    effectiveness = db.get_config_effectiveness(
        node_name=node_name,
        config_key=config_key
    )
    
    # Parse context_ranges JSON
    for r in effectiveness.get("learned_ranges", []):
        if r.get("context_ranges"):
            try:
                r["context_ranges"] = json.loads(r["context_ranges"])
            except (json.JSONDecodeError, TypeError):
                pass
    
    return effectiveness


async def handle_config_measure_outcomes(args: Dict) -> Dict:
    """
    Measure outcomes for pending config adjustments.
    
    Compares current metrics against metrics at time of adjustment
    to determine if the change was successful.
    
    Should be called periodically (e.g., 24-48h after adjustments)
    to evaluate effectiveness.
    
    Args:
        hours_since: Only measure adjustments older than this (default: 24)
        dry_run: If true, show what would be measured without recording
        
    Returns:
        List of measured outcomes
    """
    hours_since = args.get("hours_since", 24)
    dry_run = args.get("dry_run", False)
    
    db = ensure_advisor_db()
    pending = db.get_pending_outcome_measurements(hours_since=hours_since)
    
    if not pending:
        return {"message": "No pending outcome measurements", "measured": []}
    
    results = []
    
    for adj in pending:
        node_name = adj["node_name"]
        config_key = adj["config_key"]
        
        node = fleet.get_node(node_name)
        if not node:
            results.append({
                "adjustment_id": adj["id"],
                "error": f"Node {node_name} not found"
            })
            continue
        
        # Get current metrics based on config type
        try:
            if config_key in ["min_fee_ppm", "max_fee_ppm"]:
                # Measure fee effectiveness via revenue
                dashboard = await node.call("revenue-dashboard", {"window_days": 1})
                current_metrics = {
                    "revenue_sats": dashboard.get("period", {}).get("gross_revenue_sats", 0),
                    "forward_count": dashboard.get("period", {}).get("forward_count", 0),
                    "volume_sats": dashboard.get("period", {}).get("volume_sats", 0)
                }
            elif config_key in ["daily_budget_sats", "rebalance_max_amount"]:
                # Measure rebalance effectiveness
                dashboard = await node.call("revenue-dashboard", {"window_days": 1})
                current_metrics = {
                    "rebalance_cost_sats": dashboard.get("period", {}).get("rebalance_cost_sats", 0),
                    "net_profit_sats": dashboard.get("financial_health", {}).get("net_profit_sats", 0)
                }
            else:
                # Generic metrics
                dashboard = await node.call("revenue-dashboard", {"window_days": 1})
                current_metrics = {
                    "net_profit_sats": dashboard.get("financial_health", {}).get("net_profit_sats", 0),
                    "operating_margin_pct": dashboard.get("financial_health", {}).get("operating_margin_pct", 0)
                }
        except Exception as e:
            results.append({
                "adjustment_id": adj["id"],
                "error": str(e)
            })
            continue
        
        # Compare with context metrics at time of change
        context_metrics = {}
        if adj.get("context_metrics"):
            try:
                context_metrics = json.loads(adj["context_metrics"])
            except (json.JSONDecodeError, TypeError):
                pass
        
        # Determine success based on improvement
        success = False
        notes = []
        
        if config_key in ["min_fee_ppm", "max_fee_ppm"]:
            # Success if revenue or volume improved
            old_rev = context_metrics.get("revenue_sats", 0)
            new_rev = current_metrics.get("revenue_sats", 0)
            if new_rev >= old_rev:
                success = True
                notes.append(f"Revenue maintained/improved: {old_rev} -> {new_rev}")
            else:
                notes.append(f"Revenue decreased: {old_rev} -> {new_rev}")
                
        elif config_key in ["daily_budget_sats", "rebalance_max_amount"]:
            # Success if net profit improved or costs reduced
            old_profit = context_metrics.get("net_profit_sats", 0)
            new_profit = current_metrics.get("net_profit_sats", 0)
            if new_profit >= old_profit:
                success = True
                notes.append(f"Profit maintained/improved: {old_profit} -> {new_profit}")
            else:
                notes.append(f"Profit decreased: {old_profit} -> {new_profit}")
        else:
            # Default: check margin improvement
            old_margin = context_metrics.get("operating_margin_pct", 0)
            new_margin = current_metrics.get("operating_margin_pct", 0)
            if new_margin >= old_margin:
                success = True
                notes.append(f"Margin maintained/improved: {old_margin} -> {new_margin}")
            else:
                notes.append(f"Margin decreased: {old_margin} -> {new_margin}")
        
        outcome = {
            "adjustment_id": adj["id"],
            "node": node_name,
            "config_key": config_key,
            "old_value": adj["old_value"],
            "new_value": adj["new_value"],
            "trigger_reason": adj["trigger_reason"],
            "success": success,
            "notes": "; ".join(notes),
            "context_metrics": context_metrics,
            "current_metrics": current_metrics
        }
        
        if not dry_run:
            db.record_config_outcome(
                adjustment_id=adj["id"],
                outcome_metrics=current_metrics,
                success=success,
                notes="; ".join(notes)
            )
        
        results.append(outcome)
    
    return {
        "dry_run": dry_run,
        "measured_count": len(results),
        "successful": sum(1 for r in results if r.get("success")),
        "failed": sum(1 for r in results if r.get("success") is False),
        "errors": sum(1 for r in results if "error" in r),
        "results": results
    }


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



# =============================================================================
# Diagnostic Tool Handlers
# =============================================================================


async def handle_hive_node_diagnostic(args: Dict) -> Dict:
    """Comprehensive single-node diagnostic."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    import time
    now = int(time.time())
    since_24h = now - 86400

    result: Dict[str, Any] = {"node": node_name}

    # Channel balances
    try:
        channels_result = await node.call("listpeerchannels")
        channels = channels_result.get("channels", [])
        total_capacity_msat = 0
        total_local_msat = 0
        channel_count = 0
        zero_balance_channels = []
        for ch in channels:
            state = ch.get("state", "")
            if "CHANNELD_NORMAL" not in state:
                continue
            channel_count += 1
            totals = _channel_totals(ch)
            total_capacity_msat += totals["total_msat"]
            total_local_msat += totals["local_msat"]
            if totals["total_msat"] == 0:
                zero_balance_channels.append(ch.get("short_channel_id", "unknown"))
        result["channels"] = {
            "count": channel_count,
            "total_capacity_sats": total_capacity_msat // 1000,
            "total_local_sats": total_local_msat // 1000,
            "total_remote_sats": (total_capacity_msat - total_local_msat) // 1000,
            "avg_balance_ratio": round(total_local_msat / total_capacity_msat, 3) if total_capacity_msat else 0,
            "zero_balance_channels": zero_balance_channels,
        }
    except Exception as e:
        result["channels"] = {"error": str(e)}

    # 24h forwarding stats
    try:
        forwards = await node.call("listforwards", {"status": "settled"})
        stats = _forward_stats(forwards.get("forwards", []), since_24h, now)
        result["forwards_24h"] = stats
    except Exception as e:
        result["forwards_24h"] = {"error": str(e)}

    # Sling status
    try:
        sling = await node.call("sling-status")
        result["sling_status"] = sling
    except Exception as e:
        result["sling_status"] = {"error": str(e), "note": "sling plugin may not be installed"}

    # Plugin list
    try:
        plugins = await node.call("plugin", {"subcommand": "list"})
        plugin_names = []
        for p in plugins.get("plugins", []):
            name = p.get("name", "")
            # Extract just the filename from the path
            plugin_names.append(name.split("/")[-1] if "/" in name else name)
        result["plugins"] = plugin_names
    except Exception as e:
        result["plugins"] = {"error": str(e)}

    return result


async def handle_revenue_ops_health(args: Dict) -> Dict:
    """Validate cl-revenue-ops data pipeline health."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    checks: Dict[str, Dict[str, Any]] = {}

    # Check 1: revenue-dashboard
    try:
        dashboard = await node.call("revenue-dashboard", {"window_days": 7})
        if "error" in dashboard:
            checks["dashboard"] = {"status": "error", "detail": dashboard["error"]}
        else:
            has_revenue = dashboard.get("total_revenue_sats", 0) is not None
            has_channels = dashboard.get("active_channels", 0) is not None
            if has_revenue and has_channels:
                checks["dashboard"] = {"status": "pass", "active_channels": dashboard.get("active_channels"), "total_revenue_sats": dashboard.get("total_revenue_sats")}
            else:
                checks["dashboard"] = {"status": "warn", "detail": "Dashboard returned but missing expected fields"}
    except Exception as e:
        checks["dashboard"] = {"status": "error", "detail": str(e)}

    # Check 2: revenue-profitability
    try:
        prof = await node.call("revenue-profitability")
        if "error" in prof:
            checks["profitability"] = {"status": "error", "detail": prof["error"]}
        else:
            channel_count = len(prof.get("channels", prof.get("channels_by_class", {}).get("all", [])))
            checks["profitability"] = {"status": "pass", "channels_analyzed": channel_count}
    except Exception as e:
        checks["profitability"] = {"status": "error", "detail": str(e)}

    # Check 3: revenue-rebalance-debug
    try:
        rebal = await node.call("revenue-rebalance-debug")
        if "error" in rebal:
            checks["rebalance_debug"] = {"status": "error", "detail": rebal["error"]}
        else:
            checks["rebalance_debug"] = {"status": "pass", "keys": list(rebal.keys())[:10]}
    except Exception as e:
        checks["rebalance_debug"] = {"status": "error", "detail": str(e)}

    # Check 4: revenue-status
    try:
        status = await node.call("revenue-status")
        if "error" in status:
            checks["status"] = {"status": "error", "detail": status["error"]}
        else:
            checks["status"] = {"status": "pass", "detail": status}
    except Exception as e:
        checks["status"] = {"status": "error", "detail": str(e)}

    # Overall health
    statuses = [c["status"] for c in checks.values()]
    if all(s == "pass" for s in statuses):
        overall = "healthy"
    elif all(s == "error" for s in statuses):
        overall = "unhealthy"
    elif "error" in statuses:
        overall = "degraded"
    else:
        overall = "warning"

    return {
        "node": node_name,
        "overall_health": overall,
        "checks": checks,
    }


async def handle_advisor_validate_data(args: Dict) -> Dict:
    """Validate advisor snapshot data quality."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    import time
    issues = []
    stats: Dict[str, Any] = {}

    # Get recent snapshot data from advisor DB
    try:
        db = ensure_advisor_db()
        snapshots = db.get_recent_snapshots(limit=1)
        if not snapshots:
            return {"node": node_name, "issues": [{"severity": "warn", "detail": "No snapshots found in advisor DB"}], "stats": {}}
        stats["latest_snapshot_age_secs"] = int(time.time()) - snapshots[0].get("timestamp", 0)
        stats["latest_snapshot_type"] = snapshots[0].get("snapshot_type", "unknown")
    except Exception as e:
        issues.append({"severity": "error", "detail": f"Cannot read advisor DB: {e}"})

    # Get channel_history records for this node
    channel_records = []
    try:
        db = ensure_advisor_db()
        with db._get_conn() as conn:
            rows = conn.execute("""
                SELECT channel_id, peer_id, capacity_sats, local_sats, remote_sats, balance_ratio
                FROM channel_history
                WHERE node_name = ?
                AND timestamp > ?
                ORDER BY timestamp DESC
                LIMIT 200
            """, (node_name, int(time.time()) - 3600)).fetchall()
            channel_records = [dict(r) for r in rows]
    except Exception as e:
        issues.append({"severity": "error", "detail": f"Cannot query channel_history: {e}"})

    stats["channel_records_last_hour"] = len(channel_records)

    # Check for zero-value issues
    zero_capacity = [r for r in channel_records if r.get("capacity_sats", 0) == 0]
    zero_local = [r for r in channel_records if r.get("local_sats", 0) == 0 and r.get("remote_sats", 0) == 0]
    if zero_capacity:
        issues.append({
            "severity": "critical",
            "detail": f"{len(zero_capacity)} channel records with zero capacity",
            "channels": [r.get("channel_id", "?") for r in zero_capacity[:5]],
        })
    if zero_local:
        issues.append({
            "severity": "warn",
            "detail": f"{len(zero_local)} channel records with both local and remote = 0",
            "channels": [r.get("channel_id", "?") for r in zero_local[:5]],
        })

    # Check for missing IDs
    missing_channel_id = [r for r in channel_records if not r.get("channel_id")]
    missing_peer_id = [r for r in channel_records if not r.get("peer_id")]
    if missing_channel_id:
        issues.append({"severity": "critical", "detail": f"{len(missing_channel_id)} records missing channel_id"})
    if missing_peer_id:
        issues.append({"severity": "warn", "detail": f"{len(missing_peer_id)} records missing peer_id"})

    # Check balance ratio consistency
    bad_ratio = [r for r in channel_records if r.get("balance_ratio") is not None and (r["balance_ratio"] < 0 or r["balance_ratio"] > 1)]
    if bad_ratio:
        issues.append({
            "severity": "warn",
            "detail": f"{len(bad_ratio)} records with balance_ratio outside 0-1 range",
            "examples": [{"channel_id": r.get("channel_id"), "ratio": r.get("balance_ratio")} for r in bad_ratio[:3]],
        })

    # Compare snapshot vs live data
    try:
        channels_result = await node.call("listpeerchannels")
        live_channels = {}
        for ch in channels_result.get("channels", []):
            scid = ch.get("short_channel_id")
            if scid and "CHANNELD_NORMAL" in ch.get("state", ""):
                totals = _channel_totals(ch)
                live_channels[scid] = {
                    "capacity_sats": totals["total_msat"] // 1000,
                    "local_sats": totals["local_msat"] // 1000,
                }

        # Deduplicate channel_records to most recent per channel_id
        seen_channels: Dict[str, Dict] = {}
        for r in channel_records:
            cid = r.get("channel_id")
            if cid and cid not in seen_channels:
                seen_channels[cid] = r

        mismatches = []
        for cid, snapshot in seen_channels.items():
            live = live_channels.get(cid)
            if not live:
                continue
            snap_cap = snapshot.get("capacity_sats", 0)
            live_cap = live.get("capacity_sats", 0)
            if live_cap > 0 and snap_cap == 0:
                mismatches.append({"channel_id": cid, "issue": "snapshot has 0 capacity, live has data", "live_capacity_sats": live_cap})

        stats["live_channels"] = len(live_channels)
        stats["snapshot_channels_matched"] = len(seen_channels)
        if mismatches:
            issues.append({
                "severity": "critical",
                "detail": f"{len(mismatches)} channels with snapshot=0 but live data exists",
                "mismatches": mismatches[:5],
            })
    except Exception as e:
        issues.append({"severity": "warn", "detail": f"Could not compare with live data: {e}"})

    return {
        "node": node_name,
        "issue_count": len(issues),
        "critical_count": len([i for i in issues if i.get("severity") == "critical"]),
        "issues": issues,
        "stats": stats,
    }


async def handle_advisor_dedup_status(args: Dict) -> Dict:
    """Check for duplicate and stale pending decisions."""
    import time
    now = int(time.time())
    stale_threshold = now - (48 * 3600)

    try:
        db = ensure_advisor_db()
    except Exception as e:
        return {"error": f"Cannot initialize advisor DB: {e}"}

    pending = db.get_pending_decisions()

    # Group by (decision_type, node_name, channel_id)
    groups: Dict[str, list] = {}
    stale_count = 0
    for d in pending:
        key = f"{d.get('decision_type', '?')}|{d.get('node_name', '?')}|{d.get('channel_id', '?')}"
        groups.setdefault(key, []).append(d)
        if d.get("timestamp", now) < stale_threshold:
            stale_count += 1

    duplicates = []
    for key, decisions in groups.items():
        if len(decisions) > 1:
            parts = key.split("|")
            duplicates.append({
                "decision_type": parts[0],
                "node_name": parts[1],
                "channel_id": parts[2],
                "count": len(decisions),
                "oldest_timestamp": min(d.get("timestamp", 0) for d in decisions),
                "newest_timestamp": max(d.get("timestamp", 0) for d in decisions),
            })

    # Outcome coverage stats
    try:
        db_stats = db.get_stats()
        total_decisions = db_stats.get("ai_decisions", 0)
        total_outcomes = db.count_outcomes()
    except Exception:
        total_decisions = 0
        total_outcomes = 0

    return {
        "pending_total": len(pending),
        "unique_groups": len(groups),
        "duplicate_groups": duplicates,
        "stale_count_48h": stale_count,
        "outcome_coverage": {
            "total_decisions": total_decisions,
            "total_outcomes": total_outcomes,
            "coverage_pct": round(total_outcomes / total_decisions * 100, 1) if total_decisions else 0,
        },
    }


async def handle_rebalance_diagnostic(args: Dict) -> Dict:
    """Diagnose rebalancing subsystem health."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    result: Dict[str, Any] = {"node": node_name}
    diagnosis = []

    # Check sling plugin availability
    sling_available = False
    try:
        plugins = await node.call("plugin", {"subcommand": "list"})
        for p in plugins.get("plugins", []):
            name = p.get("name", "")
            if "sling" in name.lower():
                sling_available = True
                break
        result["sling_installed"] = sling_available
        if not sling_available:
            diagnosis.append("Sling plugin is NOT installed — rebalancing unavailable")
    except Exception as e:
        result["sling_installed"] = None
        diagnosis.append(f"Cannot check plugin list: {e}")

    # Get revenue-rebalance-debug for structured diagnostics
    try:
        rebal = await node.call("revenue-rebalance-debug")
        if "error" in rebal:
            result["rebalance_debug"] = {"error": rebal["error"]}
            diagnosis.append(f"revenue-rebalance-debug error: {rebal['error']}")
        else:
            result["rebalance_debug"] = rebal

            # Extract key diagnostic info
            rejections = rebal.get("rejection_reasons", rebal.get("rejections", {}))
            if rejections:
                result["rejection_reasons"] = rejections
                for reason, count in rejections.items() if isinstance(rejections, dict) else []:
                    if count > 0:
                        diagnosis.append(f"Rejection: {reason} ({count} channels)")

            capital_controls = rebal.get("capital_controls", {})
            if capital_controls:
                result["capital_controls"] = capital_controls

            budget = rebal.get("budget", rebal.get("budget_state", {}))
            if budget:
                result["budget_state"] = budget
    except Exception as e:
        result["rebalance_debug"] = {"error": str(e)}
        diagnosis.append(f"Cannot call revenue-rebalance-debug: {e}")

    # Try sling-status for active jobs
    if sling_available:
        try:
            sling = await node.call("sling-status")
            result["sling_status"] = sling
        except Exception as e:
            result["sling_status"] = {"error": str(e)}
            diagnosis.append(f"sling-status call failed: {e}")

    result["diagnosis"] = diagnosis if diagnosis else ["All rebalance subsystems operational"]
    return result


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
        except Exception as e:
            logger.warning(f"Revenue data unavailable for {node_name}: {e}")
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
        channels_by_class = profitability.get("channels_by_class", {})
        if not channels_by_class and "error" in profitability:
            logger.warning(f"Profitability returned error for {node_name}: {profitability.get('error')}")
        prof_data = []
        for class_name, class_channels in channels_by_class.items():
            if isinstance(class_channels, list):
                for ch in class_channels:
                    ch["profitability_class"] = class_name
                    prof_data.append(ch)
        prof_by_id = {c.get("channel_id"): c for c in prof_data}
        if prof_data:
            logger.info(f"Profitability data: {len(prof_data)} channels classified for {node_name}")
        else:
            logger.warning(f"No profitability classification data available for {node_name}")

        for ch in channels_data.get("channels", []):
            if ch.get("state") != "CHANNELD_NORMAL":
                continue
            scid = ch.get("short_channel_id", "")
            if not scid:
                continue
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
                "flow_ratio": prof_ch.get("roi_percentage", 0),
                "confidence": 1.0,
                "forward_count": prof_ch.get("forward_count", 0),
                "fees_earned_sats": prof_ch.get("fees_earned_sats", 0),
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
    predicted_benefit = args.get("predicted_benefit")
    snapshot_metrics = args.get("snapshot_metrics")

    db = ensure_advisor_db()

    decision_id = db.record_decision(
        decision_type=decision_type,
        node_name=node_name,
        recommendation=recommendation,
        reasoning=reasoning,
        channel_id=channel_id,
        peer_id=peer_id,
        confidence=confidence,
        predicted_benefit=predicted_benefit,
        snapshot_metrics=snapshot_metrics
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
                   recommendation, reasoning, confidence, status,
                   outcome_measured_at, outcome_success, outcome_metrics
            FROM ai_decisions
            ORDER BY timestamp DESC
            LIMIT ?
        """, (limit,)).fetchall()

    decisions = []
    for row in rows:
        decision = {
            "id": row["id"],
            "timestamp": datetime.fromtimestamp(row["timestamp"]).isoformat(),
            "decision_type": row["decision_type"],
            "node": row["node_name"],
            "channel_id": row["channel_id"],
            "peer_id": row["peer_id"],
            "recommendation": row["recommendation"],
            "reasoning": row["reasoning"],
            "confidence": row["confidence"],
            "status": row["status"],
            "outcome_success": row["outcome_success"],
            "outcome_measured_at": datetime.fromtimestamp(row["outcome_measured_at"]).isoformat() if row["outcome_measured_at"] else None,
        }
        if row["outcome_metrics"]:
            try:
                decision["outcome_metrics"] = json.loads(row["outcome_metrics"])
            except (json.JSONDecodeError, TypeError):
                decision["outcome_metrics"] = row["outcome_metrics"]
        decisions.append(decision)

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
    """
    Get peer intelligence/reputation data with network graph analysis.

    When a specific peer_id is provided, queries both:
    1. Local experience data (from advisor_db)
    2. Network graph data (from CLN listnodes/listchannels)

    This provides comprehensive peer evaluation for channel open decisions.
    """
    db = ensure_advisor_db()

    peer_id = args.get("peer_id")

    if peer_id:
        # Get local experience data
        intel = db.get_peer_intelligence(peer_id)

        local_data = {}
        if intel:
            local_data = {
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

        # Get network graph data from first available node
        graph_data = {}
        is_existing_peer = False
        node = next(iter(fleet.nodes.values()), None)

        if node:
            try:
                # Query listnodes for peer info
                # NOTE: Requires listnodes, listchannels, listpeers permissions in rune
                nodes_result = await node.call("listnodes", {"id": peer_id})
                if nodes_result.get("error"):
                    graph_data["rpc_errors"] = graph_data.get("rpc_errors", [])
                    graph_data["rpc_errors"].append(f"listnodes: {nodes_result['error']}")
                elif nodes_result and nodes_result.get("nodes"):
                    node_info = nodes_result["nodes"][0]
                    graph_data["alias"] = node_info.get("alias", "")
                    graph_data["last_timestamp"] = node_info.get("last_timestamp", 0)

                # Query listchannels for peer's channels
                channels_result = await node.call("listchannels", {"source": peer_id})
                if channels_result.get("error"):
                    graph_data["rpc_errors"] = graph_data.get("rpc_errors", [])
                    graph_data["rpc_errors"].append(f"listchannels: {channels_result['error']}")
                channels = channels_result.get("channels", [])

                graph_data["channel_count"] = len(channels)

                if channels:
                    capacities = []
                    fees = []

                    for ch in channels:
                        cap = ch.get("amount_msat", 0)
                        if isinstance(cap, str):
                            cap = int(cap.replace("msat", ""))
                        capacities.append(cap // 1000)

                        fee_ppm = ch.get("fee_per_millionth", 0)
                        fees.append(fee_ppm)

                    graph_data["total_capacity_sats"] = sum(capacities)
                    graph_data["avg_channel_size_sats"] = graph_data["total_capacity_sats"] // len(capacities) if capacities else 0

                    if fees:
                        sorted_fees = sorted(fees)
                        graph_data["median_fee_ppm"] = sorted_fees[len(sorted_fees) // 2]
                        graph_data["min_fee_ppm"] = sorted_fees[0]
                        graph_data["max_fee_ppm"] = sorted_fees[-1]

                    graph_data["is_well_connected"] = len(channels) >= 15

                # Check if we already have a channel with this peer
                peers_result = await node.call("listpeers", {"id": peer_id})
                if peers_result.get("error"):
                    graph_data["rpc_errors"] = graph_data.get("rpc_errors", [])
                    graph_data["rpc_errors"].append(f"listpeers: {peers_result['error']}")
                elif peers_result and peers_result.get("peers"):
                    peer_info = peers_result["peers"][0]
                    if peer_info.get("channels"):
                        is_existing_peer = True

            except Exception as e:
                graph_data["error"] = str(e)

        # Calculate channel open criteria
        channel_open_criteria = {
            "meets_min_channels": graph_data.get("channel_count", 0) >= 15,
            "meets_fee_criteria": graph_data.get("median_fee_ppm", 9999) <= 500,
            "has_force_close_history": (local_data.get("force_closes", 0) or 0) > 0,
            "is_existing_peer": is_existing_peer,
        }

        # Calculate approval
        channel_open_criteria["approved"] = (
            channel_open_criteria["meets_min_channels"] and
            not channel_open_criteria["has_force_close_history"] and
            not channel_open_criteria["is_existing_peer"] and
            local_data.get("recommendation", "neutral") not in ("avoid", "caution")
        )

        return {
            "peer_id": peer_id,
            "local_experience": local_data if local_data else None,
            "network_graph": graph_data if graph_data else None,
            "channel_open_criteria": channel_open_criteria,
            "recommendation": local_data.get("recommendation", "unknown") if local_data else (
                "good" if channel_open_criteria["approved"] else "neutral"
            )
        }
    else:
        # Return all peers (local data only)
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
                    "hive_routing_intelligence_status": "handle_routing_intelligence_status",
                    "hive_backfill_routing_intelligence": "handle_backfill_routing_intelligence",
                    "hive_members": "handle_members",
                }

                async def call(self, tool_name, params):
                    # Route to internal handlers via TOOL_HANDLERS registry
                    handler = TOOL_HANDLERS.get(tool_name)
                    if not handler:
                        # Fallback: try explicit mapping for non-standard names
                        handler_name = self.TOOL_TO_HANDLER.get(tool_name)
                        if handler_name:
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
# Phase 3: Automation Tool Handlers
# =============================================================================

async def handle_auto_evaluate_proposal(args: Dict) -> Dict:
    """Evaluate a pending proposal against automated criteria and optionally execute."""
    node_name = args.get("node")
    action_id = args.get("action_id")
    dry_run = args.get("dry_run", True)

    if not node_name or action_id is None:
        return {"error": "node and action_id are required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get the specific pending action
    pending_result = await node.call("hive-pending-actions")
    if "error" in pending_result:
        return pending_result

    actions = pending_result.get("actions", [])
    action = None
    for a in actions:
        if a.get("action_id") == action_id or a.get("id") == action_id:
            action = a
            break

    if not action:
        return {"error": f"Action {action_id} not found in pending actions"}

    action_type = action.get("action_type") or action.get("type", "unknown")
    target = action.get("target") or action.get("peer_id") or action.get("target_pubkey", "")

    decision = "escalate"
    reasoning = []
    action_executed = False

    # Evaluate based on action type
    if action_type in ("channel_open", "open_channel"):
        # Get peer intel for channel open evaluation
        peer_intel = await handle_advisor_get_peer_intel({"peer_id": target})
        graph_data = peer_intel.get("network_graph", {})
        local_data = peer_intel.get("local_experience", {}) or {}
        criteria = peer_intel.get("channel_open_criteria", {})

        channel_count = graph_data.get("channel_count", 0)
        recommendation = peer_intel.get("recommendation", "unknown")
        capacity_sats = action.get("capacity_sats") or action.get("amount_sats", 0)

        # Budget check (placeholder - could be configurable)
        budget_limit = 10_000_000  # 10M sats default
        within_budget = capacity_sats <= budget_limit

        # Evaluate criteria
        if recommendation == "avoid" or local_data.get("force_closes", 0) > 0:
            decision = "reject"
            reasoning.append(f"Peer has 'avoid' recommendation or force close history")
        elif channel_count < 10:
            decision = "reject"
            reasoning.append(f"Peer has only {channel_count} channels (<10 minimum)")
        elif not within_budget:
            decision = "reject"
            reasoning.append(f"Capacity {capacity_sats:,} sats exceeds budget of {budget_limit:,} sats")
        elif channel_count >= 15 and recommendation not in ("avoid", "caution"):
            # Good peer with enough connectivity
            if within_budget:
                decision = "approve"
                reasoning.append(f"Peer has {channel_count} channels (≥15)")
                reasoning.append(f"Peer recommendation: {recommendation}")
                reasoning.append(f"Capacity {capacity_sats:,} sats within budget")
            else:
                decision = "escalate"
                reasoning.append(f"Good peer but capacity {capacity_sats:,} sats needs review")
        else:
            decision = "escalate"
            reasoning.append(f"Peer has {channel_count} channels (10-15 range, needs review)")

    elif action_type in ("fee_change", "set_fee"):
        current_fee = action.get("current_fee_ppm", 0)
        new_fee = action.get("new_fee_ppm") or action.get("fee_ppm", 0)

        # Calculate percentage change
        if current_fee > 0:
            change_pct = abs(new_fee - current_fee) / current_fee * 100
        else:
            change_pct = 100 if new_fee > 0 else 0

        # Evaluate criteria
        if 50 <= new_fee <= 1500 and change_pct <= 25:
            decision = "approve"
            reasoning.append(f"Fee change from {current_fee} to {new_fee} ppm ({change_pct:.1f}% change)")
            reasoning.append("Within acceptable range (50-1500 ppm, ≤25% change)")
        elif new_fee < 50 or new_fee > 1500:
            decision = "escalate"
            reasoning.append(f"New fee {new_fee} ppm outside standard range (50-1500 ppm)")
        else:
            decision = "escalate"
            reasoning.append(f"Fee change of {change_pct:.1f}% exceeds 25% threshold")

    elif action_type in ("rebalance", "circular_rebalance"):
        amount_sats = action.get("amount_sats", 0)
        ev_positive = action.get("ev_positive", action.get("expected_value", 0) > 0)

        # Evaluate criteria
        if amount_sats <= 500_000 and ev_positive:
            decision = "approve"
            reasoning.append(f"Rebalance amount {amount_sats:,} sats (≤500k)")
            reasoning.append("EV-positive expected")
        elif amount_sats > 500_000:
            decision = "escalate"
            reasoning.append(f"Rebalance amount {amount_sats:,} sats exceeds 500k limit")
        else:
            decision = "escalate"
            reasoning.append("Rebalance EV not clearly positive, needs review")

    else:
        decision = "escalate"
        reasoning.append(f"Unknown action type '{action_type}', requires human review")

    # Execute if not dry_run and decision is not escalate
    if not dry_run and decision != "escalate":
        db = ensure_advisor_db()
        if decision == "approve":
            result = await handle_approve_action({
                "node": node_name,
                "action_id": action_id,
                "reason": f"Auto-approved: {'; '.join(reasoning)}"
            })
            action_executed = "error" not in result
            if action_executed:
                db.record_decision(
                    decision_type="auto_approve",
                    node_name=node_name,
                    recommendation=f"Approved {action_type}",
                    reasoning="; ".join(reasoning),
                    peer_id=target
                )
        elif decision == "reject":
            result = await handle_reject_action({
                "node": node_name,
                "action_id": action_id,
                "reason": f"Auto-rejected: {'; '.join(reasoning)}"
            })
            action_executed = "error" not in result
            if action_executed:
                db.record_decision(
                    decision_type="auto_reject",
                    node_name=node_name,
                    recommendation=f"Rejected {action_type}",
                    reasoning="; ".join(reasoning),
                    peer_id=target
                )

    return {
        "node": node_name,
        "action_id": action_id,
        "action_type": action_type,
        "decision": decision,
        "reasoning": reasoning,
        "dry_run": dry_run,
        "action_executed": action_executed,
        "ai_note": f"Decision: {decision.upper()}. {'; '.join(reasoning)}"
    }


async def handle_process_all_pending(args: Dict) -> Dict:
    """Batch process all pending actions across the fleet."""
    dry_run = args.get("dry_run", True)

    # Get pending actions from all nodes
    all_pending = await fleet.call_all("hive-pending-actions")

    approved = []
    rejected = []
    escalated = []
    errors = []
    by_node = {}

    for node_name, pending_result in all_pending.items():
        by_node[node_name] = {
            "approved": [],
            "rejected": [],
            "escalated": [],
            "errors": []
        }

        if "error" in pending_result:
            errors.append({"node": node_name, "error": pending_result["error"]})
            by_node[node_name]["errors"].append(pending_result["error"])
            continue

        actions = pending_result.get("actions", [])

        for action in actions:
            action_id = action.get("action_id") or action.get("id")
            if action_id is None:
                continue

            # Evaluate each action
            eval_result = await handle_auto_evaluate_proposal({
                "node": node_name,
                "action_id": action_id,
                "dry_run": dry_run
            })

            if "error" in eval_result:
                errors.append({
                    "node": node_name,
                    "action_id": action_id,
                    "error": eval_result["error"]
                })
                by_node[node_name]["errors"].append(eval_result["error"])
                continue

            decision = eval_result.get("decision", "escalate")
            entry = {
                "node": node_name,
                "action_id": action_id,
                "action_type": eval_result.get("action_type"),
                "decision": decision,
                "reasoning": eval_result.get("reasoning", []),
                "executed": eval_result.get("action_executed", False)
            }

            if decision == "approve":
                approved.append(entry)
                by_node[node_name]["approved"].append(entry)
            elif decision == "reject":
                rejected.append(entry)
                by_node[node_name]["rejected"].append(entry)
            else:
                escalated.append(entry)
                by_node[node_name]["escalated"].append(entry)

    return {
        "dry_run": dry_run,
        "summary": {
            "total_processed": len(approved) + len(rejected) + len(escalated),
            "approved_count": len(approved),
            "rejected_count": len(rejected),
            "escalated_count": len(escalated),
            "error_count": len(errors)
        },
        "approved": approved,
        "rejected": rejected,
        "escalated": escalated,
        "errors": errors if errors else None,
        "by_node": by_node,
        "ai_note": (
            f"Processed {len(approved) + len(rejected) + len(escalated)} actions. "
            f"Approved: {len(approved)}, Rejected: {len(rejected)}, "
            f"Escalated (need human review): {len(escalated)}"
            + (" [DRY RUN - no actions executed]" if dry_run else "")
        )
    }


async def handle_stagnant_channels(args: Dict) -> Dict:
    """List channels with high local balance (stagnant) with enriched context."""
    node_name = args.get("node")
    min_local_pct = args.get("min_local_pct", 95)
    min_age_days = args.get("min_age_days", 0)

    if not node_name:
        return {"error": "node is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Gather data
    try:
        info_result, channels_result, forwards_result = await asyncio.gather(
            node.call("getinfo"),
            node.call("listpeerchannels"),
            node.call("listforwards", {"status": "settled"}),
            return_exceptions=True
        )
    except Exception as e:
        return {"error": f"Failed to gather data: {e}"}

    if isinstance(info_result, Exception):
        return {"error": f"Failed to get node info: {info_result}"}

    current_blockheight = info_result.get("blockheight", 0)

    if isinstance(channels_result, Exception):
        channels_result = {"channels": []}
    if isinstance(forwards_result, Exception):
        forwards_result = {"forwards": []}

    channels = channels_result.get("channels", [])
    forwards = forwards_result.get("forwards", [])

    # Build forward history by channel
    import time as time_module
    now = time_module.time()
    forward_by_channel = {}
    for fwd in forwards:
        in_ch = fwd.get("in_channel")
        out_ch = fwd.get("out_channel")
        resolved_time = fwd.get("resolved_time", 0)
        if in_ch:
            if in_ch not in forward_by_channel or resolved_time > forward_by_channel[in_ch]:
                forward_by_channel[in_ch] = resolved_time
        if out_ch:
            if out_ch not in forward_by_channel or resolved_time > forward_by_channel[out_ch]:
                forward_by_channel[out_ch] = resolved_time

    # Get nodes list for alias lookup
    nodes_result = await node.call("listnodes")
    alias_map = {}
    if not isinstance(nodes_result, Exception) and "nodes" in nodes_result:
        for n in nodes_result.get("nodes", []):
            nid = n.get("nodeid")
            alias = n.get("alias")
            if nid and alias:
                alias_map[nid] = alias

    stagnant_channels = []

    for ch in channels:
        if ch.get("state") != "CHANNELD_NORMAL":
            continue

        scid = ch.get("short_channel_id", "")
        peer_id = ch.get("peer_id", "")

        # Calculate balances
        totals = _channel_totals(ch)
        total_msat = totals["total_msat"]
        local_msat = totals["local_msat"]

        if total_msat == 0:
            continue

        local_pct = (local_msat / total_msat) * 100

        # Skip if not stagnant enough
        if local_pct < min_local_pct:
            continue

        # Calculate channel age
        channel_age_days = _scid_to_age_days(scid, current_blockheight)

        # Skip if too young
        if channel_age_days is not None and channel_age_days < min_age_days:
            continue

        # Get last forward time
        last_forward_ts = forward_by_channel.get(scid, 0)
        days_since_forward = None
        if last_forward_ts > 0:
            days_since_forward = (now - last_forward_ts) / 86400

        # Get peer intel
        peer_intel = await handle_advisor_get_peer_intel({"peer_id": peer_id})
        peer_quality = peer_intel.get("recommendation", "unknown")
        local_exp = peer_intel.get("local_experience", {}) or {}
        graph_data = peer_intel.get("network_graph", {}) or {}

        # Get current fee
        updates = ch.get("updates", {})
        local_updates = updates.get("local", {})
        current_fee_ppm = local_updates.get("fee_proportional_millionths", 0)

        # Determine recommendation
        recommendation = "wait"
        reasoning = ""

        if channel_age_days is not None and channel_age_days < 30:
            recommendation = "wait"
            reasoning = f"Channel only {channel_age_days} days old, too young to judge"
        elif peer_quality == "avoid":
            recommendation = "close"
            reasoning = "Peer has 'avoid' rating - consider closing"
        elif channel_age_days is not None and channel_age_days > 90:
            if peer_quality in ("neutral", "unknown"):
                recommendation = "static_policy"
                reasoning = f"Stagnant for {channel_age_days} days with neutral peer - apply static low-fee policy"
            else:
                recommendation = "fee_reduction"
                reasoning = f"Stagnant for {channel_age_days} days - try fee reduction to attract flow"
        elif channel_age_days is not None and 30 <= channel_age_days <= 90:
            if peer_quality not in ("avoid", "caution"):
                recommendation = "fee_reduction"
                reasoning = f"Stagnant for {channel_age_days} days - try fee reduction to 50ppm"
            else:
                recommendation = "wait"
                reasoning = f"Peer has '{peer_quality}' rating, monitor before action"
        else:
            recommendation = "wait"
            reasoning = "Insufficient data for recommendation"

        stagnant_channels.append({
            "channel_id": scid,
            "peer_id": peer_id,
            "peer_alias": alias_map.get(peer_id, local_exp.get("alias", "")),
            "capacity_sats": total_msat // 1000,
            "local_pct": round(local_pct, 1),
            "channel_age_days": channel_age_days,
            "days_since_last_forward": round(days_since_forward, 1) if days_since_forward else None,
            "peer_quality": peer_quality,
            "peer_channel_count": graph_data.get("channel_count", 0),
            "current_fee_ppm": current_fee_ppm,
            "recommendation": recommendation,
            "reasoning": reasoning
        })

    # Sort by local_pct descending, then by age
    stagnant_channels.sort(key=lambda x: (-x["local_pct"], -(x["channel_age_days"] or 0)))

    return {
        "node": node_name,
        "min_local_pct": min_local_pct,
        "min_age_days": min_age_days,
        "stagnant_count": len(stagnant_channels),
        "channels": stagnant_channels,
        "ai_note": (
            f"Found {len(stagnant_channels)} stagnant channels (≥{min_local_pct}% local balance). "
            f"Recommendations: "
            f"{sum(1 for c in stagnant_channels if c['recommendation'] == 'close')} close, "
            f"{sum(1 for c in stagnant_channels if c['recommendation'] == 'fee_reduction')} fee_reduction, "
            f"{sum(1 for c in stagnant_channels if c['recommendation'] == 'static_policy')} static_policy, "
            f"{sum(1 for c in stagnant_channels if c['recommendation'] == 'wait')} wait"
        )
    }


async def handle_remediate_stagnant(args: Dict) -> Dict:
    """Auto-remediate stagnant channels based on age and peer quality."""
    node_name = args.get("node")
    dry_run = args.get("dry_run", True)

    if not node_name:
        return {"error": "node is required"}

    # Get stagnant channels
    stagnant_result = await handle_stagnant_channels({
        "node": node_name,
        "min_local_pct": 95,
        "min_age_days": 0
    })

    if "error" in stagnant_result:
        return stagnant_result

    channels = stagnant_result.get("channels", [])
    db = ensure_advisor_db()

    actions_taken = []
    channels_skipped = []
    flagged_for_review = []

    for ch in channels:
        scid = ch.get("channel_id")
        peer_id = ch.get("peer_id")
        peer_alias = ch.get("peer_alias", "")
        age_days = ch.get("channel_age_days")
        peer_quality = ch.get("peer_quality", "unknown")
        recommendation = ch.get("recommendation")
        current_fee = ch.get("current_fee_ppm", 0)

        action = None
        action_detail = {}

        # Apply remediation rules
        if age_days is not None and age_days < 30:
            # Too young - skip
            channels_skipped.append({
                "channel_id": scid,
                "peer_alias": peer_alias,
                "reason": f"Too young ({age_days} days < 30 day threshold)"
            })
            continue

        if peer_quality == "avoid":
            # Flag for close review, never auto-close
            flagged_for_review.append({
                "channel_id": scid,
                "peer_id": peer_id,
                "peer_alias": peer_alias,
                "peer_quality": peer_quality,
                "age_days": age_days,
                "reason": "Peer has 'avoid' rating - manual close review needed"
            })
            continue

        if age_days is not None and 30 <= age_days <= 90:
            if peer_quality in ("neutral", "good", "excellent", "unknown"):
                # Reduce fee to 50ppm to attract flow
                if current_fee > 50:
                    action = "fee_reduction"
                    action_detail = {
                        "channel_id": scid,
                        "peer_alias": peer_alias,
                        "old_fee_ppm": current_fee,
                        "new_fee_ppm": 50,
                        "reason": f"Stagnant {age_days} days, reducing fee to attract flow"
                    }
                else:
                    channels_skipped.append({
                        "channel_id": scid,
                        "peer_alias": peer_alias,
                        "reason": f"Fee already low ({current_fee} ppm)"
                    })
                    continue

        elif age_days is not None and age_days > 90:
            if peer_quality in ("neutral", "unknown"):
                # Apply static policy, disable rebalance
                action = "static_policy"
                action_detail = {
                    "channel_id": scid,
                    "peer_id": peer_id,
                    "peer_alias": peer_alias,
                    "strategy": "static",
                    "fee_ppm": 50,
                    "rebalance": "disabled",
                    "reason": f"Stagnant {age_days} days with neutral peer - applying static policy"
                }
            elif peer_quality in ("good", "excellent"):
                # Good peer but stagnant - try fee reduction first
                if current_fee > 50:
                    action = "fee_reduction"
                    action_detail = {
                        "channel_id": scid,
                        "peer_alias": peer_alias,
                        "old_fee_ppm": current_fee,
                        "new_fee_ppm": 50,
                        "reason": f"Stagnant {age_days} days, trying fee reduction before static policy"
                    }
                else:
                    action = "static_policy"
                    action_detail = {
                        "channel_id": scid,
                        "peer_id": peer_id,
                        "peer_alias": peer_alias,
                        "strategy": "static",
                        "fee_ppm": 50,
                        "rebalance": "disabled",
                        "reason": f"Stagnant {age_days} days, fee already low - applying static policy"
                    }

        # Execute action if not dry_run
        if action and not dry_run:
            try:
                if action == "fee_reduction":
                    result = await handle_revenue_set_fee({
                        "node": node_name,
                        "channel_id": scid,
                        "fee_ppm": 50
                    })
                    action_detail["executed"] = "error" not in result
                    if "error" in result:
                        action_detail["error"] = result["error"]
                    else:
                        db.record_decision(
                            decision_type="auto_remediate_stagnant",
                            node_name=node_name,
                            channel_id=scid,
                            recommendation=f"Fee reduction: {current_fee} -> 50 ppm",
                            reasoning=action_detail["reason"]
                        )

                elif action == "static_policy":
                    result = await handle_revenue_policy({
                        "node": node_name,
                        "action": "set",
                        "peer_id": peer_id,
                        "strategy": "static",
                        "fee_ppm": 50,
                        "rebalance": "disabled"
                    })
                    action_detail["executed"] = "error" not in result
                    if "error" in result:
                        action_detail["error"] = result["error"]
                    else:
                        db.record_decision(
                            decision_type="auto_remediate_stagnant",
                            node_name=node_name,
                            channel_id=scid,
                            peer_id=peer_id,
                            recommendation=f"Applied static policy: 50ppm, rebalance disabled",
                            reasoning=action_detail["reason"]
                        )
            except Exception as e:
                action_detail["executed"] = False
                action_detail["error"] = str(e)
        elif action:
            action_detail["executed"] = False
            action_detail["dry_run"] = True

        if action:
            action_detail["action"] = action
            actions_taken.append(action_detail)

    return {
        "node": node_name,
        "dry_run": dry_run,
        "summary": {
            "total_stagnant": len(channels),
            "actions_taken": len(actions_taken),
            "channels_skipped": len(channels_skipped),
            "flagged_for_review": len(flagged_for_review)
        },
        "actions_taken": actions_taken,
        "channels_skipped": channels_skipped,
        "flagged_for_review": flagged_for_review,
        "ai_note": (
            f"Processed {len(channels)} stagnant channels. "
            f"Actions: {len(actions_taken)}, Skipped: {len(channels_skipped)}, "
            f"Flagged for review: {len(flagged_for_review)}"
            + (" [DRY RUN - no changes made]" if dry_run else "")
        )
    }


async def handle_execute_safe_opportunities(args: Dict) -> Dict:
    """Execute opportunities marked as auto_execute_safe."""
    node_name = args.get("node")
    dry_run = args.get("dry_run", True)

    if not node_name:
        return {"error": "node is required"}

    # Scan for opportunities
    scan_result = await handle_advisor_scan_opportunities({"node": node_name})

    if "error" in scan_result:
        return scan_result

    opportunities = scan_result.get("opportunities", [])
    auto_safe_count = scan_result.get("auto_execute_safe", 0)

    db = ensure_advisor_db()
    executed = []
    skipped = []

    for opp in opportunities:
        # Check if marked as auto-safe
        is_safe = opp.get("auto_execute_safe", False)
        opp_type = opp.get("type") or opp.get("opportunity_type", "unknown")
        channel_id = opp.get("channel_id")
        peer_id = opp.get("peer_id")

        if not is_safe:
            skipped.append({
                "type": opp_type,
                "channel_id": channel_id,
                "reason": "Not marked as auto_execute_safe"
            })
            continue

        # Execute based on opportunity type
        action_result = None
        action_detail = {
            "type": opp_type,
            "channel_id": channel_id,
            "peer_id": peer_id,
            "details": opp
        }

        # Determine action category from action_type or opportunity_type
        action_type = opp.get("action_type", "")

        if not dry_run:
            try:
                # Fee change opportunities (match by action_type or specific opportunity_type)
                if action_type == "fee_change" or opp_type in (
                    "fee_adjustment", "fee_change", "hill_climb_fee",
                    "stagnant_channel", "peak_hour_fee", "low_hour_fee",
                    "critical_saturation", "competitor_undercut",
                    "pheromone_fee_adjust", "stigmergic_coordination",
                    "fleet_consensus_fee", "bleeder_fix", "imbalanced_channel"
                ):
                    new_fee = opp.get("recommended_fee") or opp.get("new_fee_ppm")

                    # Calculate fee from current state if not explicitly set
                    if not new_fee and channel_id:
                        current_state = opp.get("current_state", {})
                        current_fee = current_state.get("fee_ppm") or current_state.get("fee_per_millionth", 0)

                        if opp_type == "stagnant_channel":
                            # Stagnant: reduce to 50 ppm floor (match remediation logic)
                            new_fee = max(50, int(current_fee * 0.7)) if current_fee > 50 else 50
                        elif opp_type == "critical_saturation":
                            # Saturated: reduce by 20% to encourage outflow
                            new_fee = max(25, int(current_fee * 0.8)) if current_fee else None
                        elif opp_type == "peak_hour_fee":
                            # Peak: increase by 15%
                            new_fee = min(5000, int(current_fee * 1.15)) if current_fee else None
                        elif opp_type in ("low_hour_fee", "competitor_undercut"):
                            # Low hour / undercut: reduce by 10%
                            new_fee = max(25, int(current_fee * 0.9)) if current_fee else None
                        elif current_fee:
                            # Generic fee change: reduce by 15%
                            new_fee = max(25, int(current_fee * 0.85))

                    if new_fee and channel_id:
                        # Enforce hard bounds (safety constraints)
                        new_fee = max(25, min(5000, int(new_fee)))
                        action_result = await handle_revenue_set_fee({
                            "node": node_name,
                            "channel_id": channel_id,
                            "fee_ppm": new_fee
                        })
                        action_detail["action"] = "revenue_set_fee"
                        action_detail["new_fee_ppm"] = new_fee
                    else:
                        action_detail["action"] = "skipped_no_fee"
                        action_result = {"skipped": True, "reason": f"No target fee for {opp_type}"}

                elif opp_type in ("time_based_fee",):
                    # Time-based fees are usually handled by the plugin automatically
                    action_detail["action"] = "time_fee_handled_by_plugin"
                    action_result = {"message": "Time-based fees handled automatically by plugin"}

                elif action_type == "rebalance" or opp_type in ("rebalance", "circular_rebalance", "preemptive_rebalance"):
                    amount = opp.get("amount_sats", 0)
                    if amount <= 500_000:  # Only execute small rebalances
                        source = opp.get("source_channel")
                        dest = opp.get("dest_channel")
                        if source and dest:
                            action_result = await handle_execute_hive_circular_rebalance({
                                "node": node_name,
                                "source_channel": source,
                                "dest_channel": dest,
                                "amount_sats": amount,
                                "dry_run": False
                            })
                            action_detail["action"] = "circular_rebalance"
                    else:
                        action_detail["action"] = "skipped_large_rebalance"
                        action_result = {"skipped": True, "reason": f"Amount {amount} > 500k limit"}

                else:
                    action_detail["action"] = "no_handler"
                    action_result = {"skipped": True, "reason": f"No handler for type {opp_type}"}

                if action_result:
                    action_detail["result"] = action_result
                    action_detail["executed"] = "error" not in action_result and not action_result.get("skipped")

                    # Log to advisor DB
                    if action_detail.get("executed"):
                        db.record_decision(
                            decision_type="auto_execute_safe",
                            node_name=node_name,
                            channel_id=channel_id,
                            peer_id=peer_id,
                            recommendation=f"Executed {opp_type}",
                            reasoning=f"Auto-safe opportunity: {opp.get('description', opp_type)}",
                            predicted_benefit=opp.get("benefit_sats")
                        )

            except Exception as e:
                action_detail["executed"] = False
                action_detail["error"] = str(e)

        else:
            action_detail["executed"] = False
            action_detail["dry_run"] = True

        executed.append(action_detail)

    executed_count = sum(1 for e in executed if e.get("executed", False))

    return {
        "node": node_name,
        "dry_run": dry_run,
        "total_opportunities": len(opportunities),
        "auto_safe_available": auto_safe_count,
        "executed_count": executed_count,
        "skipped_count": len(skipped),
        "executed": executed,
        "skipped": skipped if skipped else None,
        "ai_note": (
            f"Processed {len(opportunities)} opportunities. "
            f"Executed: {executed_count}, Skipped: {len(skipped)}"
            + (" [DRY RUN - no changes made]" if dry_run else "")
        )
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
# Kalman Velocity Integration Handlers
# =============================================================================

async def handle_kalman_velocity_query(args: Dict) -> Dict:
    """Query Kalman-estimated velocity for a channel."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    if not channel_id:
        return {"error": "channel_id is required"}

    return await node.call("hive-query-kalman-velocity", {
        "channel_id": channel_id
    })


# =============================================================================
# Phase 2: Fee Coordination Handlers
# =============================================================================

async def handle_coord_fee_recommendation(args: Dict) -> Dict:
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


async def handle_execute_hive_circular_rebalance(args: Dict) -> Dict:
    """Execute a circular rebalance through hive members using explicit sendpay routes."""
    node_name = args.get("node")
    from_channel = args.get("from_channel")
    to_channel = args.get("to_channel")
    amount_sats = args.get("amount_sats")
    via_members = args.get("via_members")
    dry_run = args.get("dry_run", True)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    params = {
        "from_channel": from_channel,
        "to_channel": to_channel,
        "amount_sats": amount_sats,
        "dry_run": dry_run
    }
    if via_members is not None:
        params["via_members"] = via_members

    return await node.call("hive-execute-circular-rebalance", params)


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
# Distributed Settlement Handlers (Phase 12)
# =============================================================================

async def handle_distributed_settlement_status(args: Dict) -> Dict:
    """Get distributed settlement status including proposals and participation."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-distributed-settlement-status", {})
    except Exception as e:
        return {"error": f"Failed to get distributed settlement status: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    pending = result.get("pending_proposals", 0)
    ready = result.get("ready_proposals", 0)
    recent = result.get("recent_settlements", 0)

    result["ai_note"] = (
        f"Distributed settlement status: {pending} pending proposal(s), "
        f"{ready} ready to execute, {recent} recent settlement(s). "
        "Pending proposals await votes from quorum (51%). "
        "Ready proposals have reached quorum and are executing payments."
    )

    return result


async def handle_distributed_settlement_proposals(args: Dict) -> Dict:
    """Get settlement proposals with voting status."""
    node_name = args.get("node")
    status_filter = args.get("status")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        params = {}
        if status_filter:
            params["status"] = status_filter
        result = await node.call("hive-distributed-settlement-proposals", params)
    except Exception as e:
        return {"error": f"Failed to get settlement proposals: {e}"}

    if "error" in result:
        return result

    proposals = result.get("proposals", [])
    for prop in proposals:
        vote_count = prop.get("vote_count", 0)
        member_count = prop.get("member_count", 0)
        quorum_needed = (member_count // 2) + 1 if member_count > 0 else 1
        prop["quorum_progress"] = f"{vote_count}/{quorum_needed}"
        prop["quorum_pct"] = round((vote_count / quorum_needed) * 100, 1) if quorum_needed > 0 else 0

    result["ai_note"] = f"Found {len(proposals)} settlement proposal(s). Quorum is 51% of members."

    return result


async def handle_distributed_settlement_participation(args: Dict) -> Dict:
    """Get settlement participation rates to identify gaming behavior."""
    node_name = args.get("node")
    periods = args.get("periods", 10)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-distributed-settlement-participation", {"periods": periods})
    except Exception as e:
        return {"error": f"Failed to get participation data: {e}"}

    if "error" in result:
        return result

    # Analyze for gaming behavior
    members = result.get("members", [])
    suspects = []
    for m in members:
        vote_rate = m.get("vote_rate", 100)
        exec_rate = m.get("execution_rate", 100)
        # Flag members with low participation who owe money
        if vote_rate < 50 or exec_rate < 50:
            owes_money = m.get("total_owed", 0) < 0
            if owes_money:
                suspects.append({
                    "peer_id": m.get("peer_id", "")[:16] + "...",
                    "vote_rate": vote_rate,
                    "execution_rate": exec_rate,
                    "total_owed": m.get("total_owed", 0),
                    "risk": "HIGH" if vote_rate < 30 and owes_money else "MEDIUM"
                })

    result["gaming_suspects"] = suspects
    result["ai_note"] = (
        f"Analyzed {len(members)} member(s) over {periods} period(s). "
        f"Found {len(suspects)} potential gaming suspect(s). "
        "Low vote/execution rates combined with owing money indicates gaming behavior. "
        "Consider proposing ban for HIGH risk members."
    )

    return result


# =============================================================================
# Network Metrics Handlers
# =============================================================================

async def handle_network_metrics(args: Dict) -> Dict:
    """Get network position metrics for hive members."""
    node_name = args.get("node")
    member_id = args.get("member_id")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        params = {}
        if member_id:
            params["member_id"] = member_id

        result = await node.call("hive-network-metrics", params)
    except Exception as e:
        return {"error": f"Failed to get network metrics: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    if member_id:
        metrics = result.get("metrics", {})
        hive_centrality = metrics.get("hive_centrality", 0)
        rebalance_hub_score = metrics.get("rebalance_hub_score", 0)

        if rebalance_hub_score > 0.7:
            hub_note = "Excellent rebalance hub - ideal for zero-fee internal routing."
        elif rebalance_hub_score > 0.4:
            hub_note = "Good rebalance hub - useful for internal routing."
        else:
            hub_note = "Limited as rebalance hub - fewer internal connections."

        result["ai_note"] = (
            f"Member hive centrality: {hive_centrality:.1%}, "
            f"rebalance hub score: {rebalance_hub_score:.2f}. "
            f"{hub_note}"
        )
    else:
        members = result.get("members", [])
        top_hubs = sorted(members, key=lambda m: m.get("rebalance_hub_score", 0), reverse=True)[:3]
        hub_names = [m.get("alias", m.get("member_id", "")[:16]) for m in top_hubs]
        result["ai_note"] = (
            f"Analyzed {len(members)} member(s). "
            f"Top rebalance hubs: {', '.join(hub_names)}. "
            "Use hive_rebalance_hubs for detailed routing recommendations."
        )

    return result


async def handle_rebalance_hubs(args: Dict) -> Dict:
    """Get the best zero-fee rebalance intermediaries in the hive."""
    node_name = args.get("node")
    top_n = args.get("top_n", 3)
    exclude = args.get("exclude_members")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        params = {"top_n": top_n}
        if exclude:
            params["exclude_members"] = exclude

        result = await node.call("hive-rebalance-hubs", params)
    except Exception as e:
        return {"error": f"Failed to get rebalance hubs: {e}"}

    if "error" in result:
        return result

    hubs = result.get("hubs", [])
    if hubs:
        best_hub = hubs[0]
        result["ai_note"] = (
            f"Found {len(hubs)} suitable rebalance hub(s). "
            f"Best hub: {best_hub.get('alias', best_hub.get('member_id', '')[:16])} "
            f"with {best_hub.get('hive_peer_count', 0)} hive connections and "
            f"score {best_hub.get('rebalance_hub_score', 0):.2f}. "
            "Route internal rebalances through these nodes for zero-fee liquidity shifts."
        )
    else:
        result["ai_note"] = (
            "No suitable rebalance hubs found. "
            "Fleet may need more internal channel connections."
        )

    return result


async def handle_rebalance_path(args: Dict) -> Dict:
    """Find the optimal zero-fee path for internal rebalancing."""
    node_name = args.get("node")
    source = args.get("source_member")
    dest = args.get("dest_member")
    max_hops = args.get("max_hops", 2)

    if not source or not dest:
        return {"error": "source_member and dest_member are required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-rebalance-path", {
            "source_member": source,
            "dest_member": dest,
            "max_hops": max_hops
        })
    except Exception as e:
        return {"error": f"Failed to find rebalance path: {e}"}

    if "error" in result:
        return result

    path = result.get("path", [])
    if path:
        hop_count = len(path) - 1
        via_hubs = path[1:-1] if len(path) > 2 else []
        if via_hubs:
            hub_names = [h.get("alias", h.get("peer_id", "")[:16]) for h in via_hubs]
            result["ai_note"] = (
                f"Found {hop_count}-hop zero-fee path via {', '.join(hub_names)}. "
                "All channels between hive members have 0 ppm fees. "
                "Rebalancing through this path costs nothing in routing fees."
            )
        else:
            result["ai_note"] = (
                "Direct channel exists between source and destination. "
                "No intermediaries needed - direct zero-fee rebalance possible."
            )
    else:
        result["ai_note"] = (
            f"No path found within {max_hops} hops. "
            "Members may not be connected through the internal hive network. "
            "Consider opening channels between these members or through shared hubs."
        )

    return result


# =============================================================================
# Fleet Health Monitoring Handlers
# =============================================================================

async def handle_fleet_health(args: Dict) -> Dict:
    """Get overall fleet connectivity health metrics."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-fleet-health", {})
    except Exception as e:
        return {"error": f"Failed to get fleet health: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    grade = result.get("health_grade", "?")
    score = result.get("health_score", 0)
    isolated = result.get("isolated_count", 0)
    disconnected = result.get("disconnected_count", 0)
    hubs = result.get("hub_count", 0)
    members = result.get("member_count", 0)

    if grade in ("A", "B"):
        status = "healthy"
    elif grade == "C":
        status = "acceptable"
    else:
        status = "needs attention"

    notes = [f"Fleet connectivity is {status} (Grade {grade}, Score {score}/100)."]

    if disconnected > 0:
        notes.append(f"CRITICAL: {disconnected} member(s) have no hive channels!")
    if isolated > 0:
        notes.append(f"WARNING: {isolated} member(s) have limited fleet reachability.")
    if hubs < 2 and members >= 3:
        notes.append(f"Low hub availability ({hubs} hubs for {members} members).")

    result["ai_note"] = " ".join(notes)

    return result


async def handle_connectivity_alerts(args: Dict) -> Dict:
    """Check for fleet connectivity issues that need attention."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-connectivity-alerts", {})
    except Exception as e:
        return {"error": f"Failed to check connectivity: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    critical = result.get("critical_count", 0)
    warnings = result.get("warning_count", 0)
    info = result.get("info_count", 0)
    total = result.get("alert_count", 0)

    if total == 0:
        result["ai_note"] = "No connectivity issues detected. Fleet is well-connected."
    elif critical > 0:
        result["ai_note"] = (
            f"URGENT: {critical} critical alert(s)! "
            "Disconnected members need immediate attention. "
            "Review alerts and help them establish hive channels."
        )
    elif warnings > 0:
        result["ai_note"] = (
            f"{warnings} warning(s) found. "
            "Some members have limited connectivity. "
            "Consider helping them open additional hive channels."
        )
    else:
        result["ai_note"] = (
            f"{info} informational alert(s). "
            "Minor connectivity improvements possible but not urgent."
        )

    return result


async def handle_member_connectivity(args: Dict) -> Dict:
    """Get detailed connectivity report for a specific member."""
    node_name = args.get("node")
    member_id = args.get("member_id")

    if not member_id:
        return {"error": "member_id is required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-member-connectivity", {
            "member_id": member_id
        })
    except Exception as e:
        return {"error": f"Failed to get member connectivity: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    status = result.get("status", "unknown")
    status_msg = result.get("status_message", "")
    connections = result.get("connections", {})
    recommendations = result.get("recommended_connections", [])
    comparison = result.get("fleet_comparison", {})

    notes = [f"Status: {status_msg}"]

    connected_to = connections.get("connected_to", 0)
    not_connected = connections.get("not_connected_to", 0)
    total = connections.get("total_fleet_members", 0)

    if not_connected > 0 and recommendations:
        rec_names = [r.get("member_id_short", "?") for r in recommendations[:2]]
        notes.append(
            f"Connected to {connected_to}/{total} members. "
            f"Recommended connections: {', '.join(rec_names)}"
        )

    if comparison.get("above_average"):
        notes.append("Connectivity is above fleet average.")
    else:
        notes.append("Connectivity is below fleet average - improvement recommended.")

    result["ai_note"] = " ".join(notes)

    return result


async def handle_neophyte_rankings(args: Dict) -> Dict:
    """Get all neophytes ranked by promotion readiness."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-neophyte-rankings", {})
    except Exception as e:
        return {"error": f"Failed to get neophyte rankings: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    neophyte_count = result.get("neophyte_count", 0)
    eligible = result.get("eligible_for_promotion", 0)
    fast_track = result.get("fast_track_eligible", 0)
    rankings = result.get("rankings", [])

    if neophyte_count == 0:
        result["ai_note"] = "No neophytes in the fleet. All members are fully promoted."
    elif eligible > 0:
        top = rankings[0] if rankings else {}
        result["ai_note"] = (
            f"{eligible} neophyte(s) eligible for promotion! "
            f"Top candidate: {top.get('peer_id_short', '?')} "
            f"(readiness: {top.get('readiness_score', 0)}/100). "
            "Consider running evaluate_promotion to confirm eligibility."
        )
    elif fast_track > 0:
        result["ai_note"] = (
            f"{fast_track} neophyte(s) eligible for fast-track promotion "
            "due to high hive centrality (>=0.5). "
            "They've demonstrated commitment by connecting to fleet members."
        )
    else:
        # Find the top neophyte and what's blocking them
        if rankings:
            top = rankings[0]
            blockers = top.get("blocking_reasons", [])
            days = top.get("days_as_neophyte", 0)
            result["ai_note"] = (
                f"{neophyte_count} neophyte(s), none yet eligible. "
                f"Top candidate ({top.get('peer_id_short', '?')}) at {top.get('readiness_score', 0)}/100 "
                f"after {days:.0f} days. "
                f"Blocking: {', '.join(blockers[:2]) if blockers else 'time remaining'}."
            )
        else:
            result["ai_note"] = f"{neophyte_count} neophyte(s), none yet eligible for promotion."

    return result


# =============================================================================
# MCF (Min-Cost Max-Flow) Optimization Handlers (Phase 15)
# =============================================================================

async def handle_mcf_status(args: Dict) -> Dict:
    """Get MCF optimizer status."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-mcf-status", {})
    except Exception as e:
        return {"error": f"Failed to get MCF status: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    enabled = result.get("enabled", False)
    is_coord = result.get("is_coordinator", False)
    cb_state = result.get("circuit_breaker_state", "unknown")
    pending = result.get("pending_assignments", 0)
    last_solution = result.get("last_solution_timestamp", 0)

    if not enabled:
        result["ai_note"] = "MCF optimization is disabled. Fleet using BFS fallback for rebalancing."
    elif cb_state == "open":
        result["ai_note"] = (
            "Circuit breaker OPEN - MCF temporarily disabled due to failures. "
            "Will attempt recovery after cooldown period. BFS fallback active."
        )
    elif cb_state == "half_open":
        result["ai_note"] = (
            "Circuit breaker HALF_OPEN - MCF testing recovery with limited operations."
        )
    elif is_coord:
        result["ai_note"] = (
            f"This node is MCF coordinator. "
            f"{pending} pending assignment(s). "
            f"Circuit breaker healthy (CLOSED)."
        )
    else:
        coord_short = result.get("coordinator_id", "")[:16]
        result["ai_note"] = (
            f"MCF active. Coordinator: {coord_short}... "
            f"{pending} pending assignment(s) for this node."
        )

    return result


async def handle_mcf_solve(args: Dict) -> Dict:
    """Trigger MCF optimization cycle."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-mcf-solve", {})
    except Exception as e:
        return {"error": f"Failed to run MCF solve: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    if result.get("solution"):
        sol = result["solution"]
        total_flow = sol.get("total_flow", 0)
        total_cost = sol.get("total_cost", 0)
        assignments = sol.get("assignments_count", 0)
        cost_ppm = (total_cost * 1_000_000 // total_flow) if total_flow > 0 else 0

        result["ai_note"] = (
            f"MCF solution computed: {assignments} assignment(s), "
            f"{total_flow:,} sats total flow, "
            f"{total_cost:,} sats cost ({cost_ppm} ppm effective). "
            "Solution broadcast to fleet."
        )
    elif result.get("skipped"):
        result["ai_note"] = f"MCF solve skipped: {result.get('reason', 'unknown reason')}"
    else:
        result["ai_note"] = "MCF solve completed but no solution generated."

    return result


async def handle_mcf_assignments(args: Dict) -> Dict:
    """Get pending MCF assignments."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        result = await node.call("hive-mcf-assignments", {})
    except Exception as e:
        return {"error": f"Failed to get MCF assignments: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    pending = result.get("pending", [])
    executing = result.get("executing", [])
    completed = result.get("completed_recent", [])
    failed = result.get("failed_recent", [])

    pending_count = len(pending)
    executing_count = len(executing)
    completed_count = len(completed)
    failed_count = len(failed)

    if pending_count == 0 and executing_count == 0:
        if completed_count > 0 or failed_count > 0:
            success_rate = completed_count * 100 // (completed_count + failed_count) if (completed_count + failed_count) > 0 else 0
            result["ai_note"] = (
                f"No active assignments. Recent: {completed_count} completed, "
                f"{failed_count} failed ({success_rate}% success rate)."
            )
        else:
            result["ai_note"] = "No MCF assignments (pending or recent). Awaiting next optimization cycle."
    else:
        total_pending_sats = sum(a.get("amount_sats", 0) for a in pending)
        result["ai_note"] = (
            f"{pending_count} pending ({total_pending_sats:,} sats), "
            f"{executing_count} executing. "
            f"Recent: {completed_count} completed, {failed_count} failed."
        )

    return result


async def handle_mcf_optimized_path(args: Dict) -> Dict:
    """Get MCF-optimized rebalance path."""
    node_name = args.get("node")
    source_channel = args.get("source_channel")
    dest_channel = args.get("dest_channel")
    amount_sats = args.get("amount_sats")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    if not source_channel or not dest_channel or not amount_sats:
        return {"error": "Required: source_channel, dest_channel, amount_sats"}

    try:
        result = await node.call("hive-mcf-optimized-path", {
            "source_channel": source_channel,
            "dest_channel": dest_channel,
            "amount_sats": amount_sats
        })
    except Exception as e:
        return {"error": f"Failed to get MCF path: {e}"}

    if "error" in result:
        return result

    # Add AI-friendly analysis
    path = result.get("path", [])
    source = result.get("source", "unknown")
    cost_ppm = result.get("cost_estimate_ppm", 0)
    hops = len(path) - 1 if path else 0

    if path:
        result["ai_note"] = (
            f"Path found via {source.upper()}: {hops} hop(s), ~{cost_ppm} ppm cost. "
            f"Route: {' -> '.join([p[:8] + '...' for p in path])}"
        )
    else:
        result["ai_note"] = "No path found between specified channels."

    return result


async def handle_mcf_health(args: Dict) -> Dict:
    """Get detailed MCF health metrics."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    try:
        # Get MCF status which includes health metrics
        result = await node.call("hive-mcf-status", {})
    except Exception as e:
        return {"error": f"Failed to get MCF health: {e}"}

    if "error" in result:
        return result

    # Extract and format health-specific information
    health_result = {
        "enabled": result.get("enabled", False),
        "circuit_breaker": {
            "state": result.get("circuit_breaker_state", "unknown"),
            "failure_count": result.get("failure_count", 0),
            "success_count": result.get("success_count", 0),
            "last_failure": result.get("last_failure_time"),
            "last_failure_reason": result.get("last_failure_reason")
        },
        "health_metrics": result.get("health_metrics", {}),
        "solution_staleness": result.get("solution_staleness", {}),
        "is_healthy": result.get("is_healthy", True)
    }

    # Compute overall health assessment
    cb_state = health_result["circuit_breaker"]["state"]
    is_healthy = health_result.get("is_healthy", True)
    failure_count = health_result["circuit_breaker"]["failure_count"]

    if cb_state == "open":
        health_result["health_assessment"] = "unhealthy"
        health_result["ai_note"] = (
            f"MCF UNHEALTHY: Circuit breaker OPEN after {failure_count} failures. "
            f"Last failure: {health_result['circuit_breaker'].get('last_failure_reason', 'unknown')}. "
            "MCF disabled, using BFS fallback. Will attempt recovery after cooldown."
        )
    elif cb_state == "half_open":
        health_result["health_assessment"] = "recovering"
        health_result["ai_note"] = (
            "MCF RECOVERING: Circuit breaker testing limited operations. "
            "If next attempts succeed, will return to normal. "
            "If they fail, will revert to OPEN state."
        )
    elif not is_healthy:
        health_result["health_assessment"] = "degraded"
        staleness = result.get("solution_staleness", {})
        stale_cycles = staleness.get("consecutive_stale_cycles", 0)
        health_result["ai_note"] = (
            f"MCF DEGRADED: {stale_cycles} consecutive stale cycles. "
            "Solutions may be outdated. Check gossip freshness and coordinator connectivity."
        )
    else:
        health_result["health_assessment"] = "healthy"
        metrics = health_result.get("health_metrics", {})
        success = metrics.get("successful_assignments", 0)
        failed = metrics.get("failed_assignments", 0)
        total = success + failed
        rate = (success * 100 // total) if total > 0 else 100
        health_result["ai_note"] = (
            f"MCF HEALTHY: Circuit breaker CLOSED, {rate}% assignment success rate "
            f"({success}/{total} assignments)."
        )

    return health_result


# =============================================================================
# Phase 4: Membership & Settlement Handlers (Hex Automation)
# =============================================================================

async def handle_membership_dashboard(args: Dict) -> Dict:
    """Get unified membership lifecycle view."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Gather data from multiple sources in parallel
    try:
        members_data, neophyte_rankings, nnlb_data, pending_promos, pending_bans = await asyncio.gather(
            node.call("hive-members"),
            node.call("hive-neophyte-rankings", {}),
            node.call("hive-nnlb-status", {}),
            node.call("hive-pending-promotions", {}),
            node.call("hive-pending-bans", {}),
            return_exceptions=True,
        )
    except Exception as e:
        return {"error": f"Failed to gather membership data: {e}"}

    # Process members
    members_list = []
    if not isinstance(members_data, Exception):
        members_list = members_data.get("members", [])

    member_count = len([m for m in members_list if m.get("tier") == "member"])
    neophyte_count = len([m for m in members_list if m.get("tier") == "neophyte"])

    # Process neophyte rankings
    neophytes_info = {"count": neophyte_count, "rankings": [], "promotion_eligible": 0, "fast_track_eligible": 0}
    if not isinstance(neophyte_rankings, Exception):
        rankings = neophyte_rankings.get("rankings", [])
        neophytes_info["rankings"] = rankings[:5]  # Top 5
        neophytes_info["promotion_eligible"] = neophyte_rankings.get("eligible_for_promotion", 0)
        neophytes_info["fast_track_eligible"] = neophyte_rankings.get("fast_track_eligible", 0)

    # Process NNLB status for member health
    members_health = {"count": member_count, "health_distribution": {}, "struggling_members": []}
    if not isinstance(nnlb_data, Exception):
        members_health["health_distribution"] = nnlb_data.get("health_distribution", {})
        members_health["struggling_members"] = nnlb_data.get("struggling_members", [])[:3]  # Top 3

    # Process pending actions
    pending_actions = {"pending_promotions": 0, "pending_bans": 0}
    if not isinstance(pending_promos, Exception):
        pending_actions["pending_promotions"] = len(pending_promos.get("proposals", []))
    if not isinstance(pending_bans, Exception):
        pending_actions["pending_bans"] = len(pending_bans.get("proposals", []))

    # Check for onboarding needs (members without recent channel suggestions)
    db = ensure_advisor_db()
    onboarding_needed = []
    for member in members_list:
        pubkey = member.get("pubkey") or member.get("peer_id")
        if pubkey and not db.is_member_onboarded(pubkey):
            onboarding_needed.append({
                "pubkey": pubkey[:16] + "...",
                "alias": member.get("alias", ""),
                "tier": member.get("tier", "unknown")
            })

    # Build AI note
    notes = []
    if neophytes_info["promotion_eligible"] > 0:
        notes.append(f"{neophytes_info['promotion_eligible']} neophyte(s) ready for promotion!")
    if members_health["struggling_members"]:
        notes.append(f"{len(members_health['struggling_members'])} member(s) struggling (NNLB).")
    if pending_actions["pending_promotions"] > 0:
        notes.append(f"{pending_actions['pending_promotions']} promotion vote(s) pending.")
    if onboarding_needed:
        notes.append(f"{len(onboarding_needed)} member(s) need onboarding.")

    return {
        "node": node_name,
        "neophytes": neophytes_info,
        "members": members_health,
        "pending_actions": pending_actions,
        "onboarding_needed": onboarding_needed[:5],
        "onboarding_needed_count": len(onboarding_needed),
        "ai_note": " ".join(notes) if notes else "Membership health is good. No urgent actions needed."
    }


async def handle_check_neophytes(args: Dict) -> Dict:
    """Check for promotion-ready neophytes and optionally propose promotions."""
    node_name = args.get("node")
    dry_run = args.get("dry_run", True)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get neophyte rankings and pending promotions in parallel
    try:
        rankings_data, pending_data = await asyncio.gather(
            node.call("hive-neophyte-rankings", {}),
            node.call("hive-pending-promotions", {}),
        )
    except Exception as e:
        return {"error": f"Failed to get neophyte data: {e}"}

    if "error" in rankings_data:
        return rankings_data

    rankings = rankings_data.get("rankings", [])
    pending_proposals = pending_data.get("proposals", []) if "error" not in pending_data else []

    # Build set of already-pending pubkeys
    pending_pubkeys = set()
    for prop in pending_proposals:
        target = prop.get("target_peer_id") or prop.get("target")
        if target:
            pending_pubkeys.add(target)

    # Process each neophyte
    proposed_count = 0
    already_pending_count = 0
    details = []

    for neo in rankings:
        peer_id = neo.get("peer_id")
        peer_id_short = neo.get("peer_id_short", peer_id[:16] + "..." if peer_id else "?")
        is_eligible = neo.get("eligible", False)
        is_fast_track = neo.get("fast_track_eligible", False)
        readiness = neo.get("readiness_score", 0)

        detail = {
            "peer_id_short": peer_id_short,
            "readiness_score": readiness,
            "eligible": is_eligible,
            "fast_track_eligible": is_fast_track,
            "status": "not_eligible"
        }

        if not (is_eligible or is_fast_track):
            detail["blocking_reasons"] = neo.get("blocking_reasons", [])
            details.append(detail)
            continue

        # Check if already pending
        if peer_id in pending_pubkeys:
            detail["status"] = "already_pending"
            already_pending_count += 1
            details.append(detail)
            continue

        # Eligible and not pending - propose if not dry run
        if dry_run:
            detail["status"] = "would_propose"
            proposed_count += 1
        else:
            try:
                # Get our pubkey as proposer
                info = await node.call("getinfo")
                proposer_id = info.get("id")

                result = await node.call("hive-propose-promotion", {
                    "target_peer_id": peer_id,
                    "proposer_peer_id": proposer_id
                })

                if "error" in result:
                    detail["status"] = "proposal_failed"
                    detail["error"] = result.get("error")
                else:
                    detail["status"] = "proposed"
                    proposed_count += 1
            except Exception as e:
                detail["status"] = "proposal_failed"
                detail["error"] = str(e) or type(e).__name__

        details.append(detail)

    ai_note = f"Checked {len(rankings)} neophyte(s). "
    if proposed_count > 0:
        ai_note += f"{'Would propose' if dry_run else 'Proposed'} {proposed_count} for promotion. "
    if already_pending_count > 0:
        ai_note += f"{already_pending_count} already pending. "
    if dry_run and proposed_count > 0:
        ai_note += "Run with dry_run=false to execute."

    return {
        "node": node_name,
        "dry_run": dry_run,
        "neophyte_count": len(rankings),
        "proposed_count": proposed_count,
        "already_pending_count": already_pending_count,
        "details": details,
        "ai_note": ai_note
    }


async def handle_settlement_readiness(args: Dict) -> Dict:
    """Pre-settlement validation check."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    blockers = []
    missing_offers = []
    low_participation = []

    # Gather required data in parallel
    try:
        members_data, offers_data, participation_data, calc_data = await asyncio.gather(
            node.call("hive-members"),
            node.call("hive-settlement-list-offers", {}),
            node.call("hive-distributed-settlement-participation", {"periods": 10}),
            node.call("hive-settlement-calculate", {}),
            return_exceptions=True,
        )
    except Exception as e:
        return {"error": f"Failed to gather settlement data: {e}"}

    # Check members have BOLT12 offers
    members_list = []
    if not isinstance(members_data, Exception):
        members_list = members_data.get("members", [])

    offers_set = set()
    if not isinstance(offers_data, Exception):
        for offer in offers_data.get("offers", []):
            peer_id = offer.get("peer_id") or offer.get("member_id")
            if peer_id:
                offers_set.add(peer_id)

    for member in members_list:
        pubkey = member.get("pubkey") or member.get("peer_id")
        if pubkey and pubkey not in offers_set:
            missing_offers.append({
                "pubkey": pubkey[:16] + "...",
                "alias": member.get("alias", "")
            })

    if missing_offers:
        blockers.append(f"{len(missing_offers)} member(s) missing BOLT12 offers")

    # Check participation history
    if not isinstance(participation_data, Exception):
        for member in participation_data.get("members", []):
            vote_rate = member.get("vote_rate", 100)
            exec_rate = member.get("execution_rate", 100)
            if vote_rate < 50 or exec_rate < 50:
                low_participation.append({
                    "pubkey": (member.get("peer_id", "")[:16] + "...") if member.get("peer_id") else "?",
                    "vote_rate": vote_rate,
                    "execution_rate": exec_rate
                })

    if low_participation:
        blockers.append(f"{len(low_participation)} member(s) with <50% participation")

    # Get expected distribution
    expected_distribution = []
    total_to_distribute = 0
    if not isinstance(calc_data, Exception) and "error" not in calc_data:
        total_to_distribute = calc_data.get("total_to_distribute_sats", 0)
        for dist in calc_data.get("distributions", []):
            expected_distribution.append({
                "member": dist.get("alias") or (dist.get("peer_id", "")[:16] + "..."),
                "amount_sats": dist.get("amount_sats", 0),
                "contribution_pct": dist.get("contribution_pct", 0)
            })

    if total_to_distribute == 0:
        blockers.append("No funds to distribute (pool empty)")

    # Determine readiness
    ready = len(blockers) == 0
    if ready:
        recommendation = "settle_now"
    elif len(blockers) == 1 and "participation" in blockers[0]:
        recommendation = "wait"  # Low participation is a soft blocker
    else:
        recommendation = "fix_blockers"

    ai_note = ""
    if ready:
        ai_note = f"Ready to settle! {total_to_distribute:,} sats to distribute among {len(expected_distribution)} members."
    else:
        ai_note = f"Settlement blocked: {'; '.join(blockers)}. "
        if recommendation == "wait":
            ai_note += "Consider proceeding anyway if participation issues are acceptable."

    return {
        "node": node_name,
        "ready": ready,
        "blockers": blockers,
        "missing_offers": missing_offers,
        "low_participation": low_participation,
        "expected_distribution": expected_distribution[:10],  # Top 10
        "total_to_distribute_sats": total_to_distribute,
        "recommendation": recommendation,
        "ai_note": ai_note
    }


async def handle_run_settlement_cycle(args: Dict) -> Dict:
    """Execute a full settlement cycle."""
    import time
    from datetime import datetime

    node_name = args.get("node")
    dry_run = args.get("dry_run", True)

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Determine current period
    now = datetime.utcnow()
    period = f"{now.year}-W{now.isocalendar()[1]:02d}"

    # Step 1: Record contribution snapshot
    snapshot_result = None
    try:
        snapshot_result = await node.call("hive-pool-snapshot", {})
    except Exception as e:
        logger.warning(f"Pool snapshot failed: {e}")

    snapshot_recorded = snapshot_result is not None and "error" not in snapshot_result

    # Step 2: Calculate distribution
    try:
        calc_result = await node.call("hive-settlement-calculate", {})
    except Exception as e:
        return {"error": f"Settlement calculation failed: {e}"}

    if "error" in calc_result:
        return calc_result

    total_to_distribute = calc_result.get("total_to_distribute_sats", 0)
    distributions = calc_result.get("distributions", [])

    per_member_breakdown = []
    for dist in distributions:
        per_member_breakdown.append({
            "member": dist.get("alias") or (dist.get("peer_id", "")[:16] + "..."),
            "peer_id_short": (dist.get("peer_id", "")[:16] + "...") if dist.get("peer_id") else "?",
            "amount_sats": dist.get("amount_sats", 0),
            "contribution_pct": dist.get("contribution_pct", 0)
        })

    # Step 3: Execute if not dry run
    total_distributed = 0
    execution_result = None
    if not dry_run and total_to_distribute > 0:
        try:
            execution_result = await node.call("hive-settlement-execute", {"dry_run": False})
            if "error" not in execution_result:
                total_distributed = execution_result.get("total_distributed_sats", total_to_distribute)
        except Exception as e:
            return {"error": f"Settlement execution failed: {e}"}

    ai_note = f"Settlement cycle for {period}. "
    if dry_run:
        ai_note += f"DRY RUN: Would distribute {total_to_distribute:,} sats among {len(per_member_breakdown)} members. "
        ai_note += "Run with dry_run=false to execute."
    else:
        if total_distributed > 0:
            ai_note += f"Distributed {total_distributed:,} sats to {len(per_member_breakdown)} members."
        else:
            ai_note += "No funds were distributed (pool may be empty)."

    return {
        "node": node_name,
        "period": period,
        "dry_run": dry_run,
        "snapshot_recorded": snapshot_recorded,
        "total_calculated_sats": total_to_distribute,
        "total_distributed_sats": total_distributed if not dry_run else 0,
        "per_member_breakdown": per_member_breakdown,
        "execution_result": execution_result if not dry_run else None,
        "ai_note": ai_note
    }


# =============================================================================
# Phase 5: Monitoring & Health Handlers (Hex Automation)
# =============================================================================

async def handle_fleet_health_summary(args: Dict) -> Dict:
    """Quick fleet health overview for monitoring."""
    node_name = args.get("node")

    # If specific node, just query that one
    if node_name:
        nodes_to_check = [fleet.get_node(node_name)]
        if not nodes_to_check[0]:
            return {"error": f"Unknown node: {node_name}"}
    else:
        nodes_to_check = list(fleet.nodes.values())

    nodes_status = {}
    channel_stats = {"profitable": 0, "underwater": 0, "stagnant": 0, "total": 0}
    routing_24h = {"volume_sats": 0, "revenue_sats": 0, "forward_count": 0}
    alerts_by_severity = {"critical": 0, "warning": 0, "info": 0}
    mcf_status = {}
    nnlb_struggling = []
    seen_struggling_peers = set()  # For deduplication across nodes

    for node in nodes_to_check:
        # Gather data for this node in parallel
        try:
            info, channels, dashboard, prof, mcf, nnlb, conn_alerts = await asyncio.gather(
                node.call("getinfo"),
                node.call("listpeerchannels"),
                node.call("revenue-dashboard", {"window_days": 1}),
                node.call("revenue-profitability", {}),
                node.call("hive-mcf-status", {}),
                node.call("hive-nnlb-status", {}),
                node.call("hive-connectivity-alerts", {}),
                return_exceptions=True,
            )
        except Exception as e:
            nodes_status[node.name] = {"status": "error", "error": str(e)}
            continue

        # Node status
        node_status = {"status": "online"}
        if isinstance(info, Exception) or "error" in info:
            node_status["status"] = "offline"
            node_status["error"] = str(info) if isinstance(info, Exception) else info.get("error")
        else:
            node_status["alias"] = info.get("alias", "")
            node_status["blockheight"] = info.get("blockheight", 0)

        # Channel count and capacity
        if not isinstance(channels, Exception):
            ch_list = channels.get("channels", [])
            node_status["channel_count"] = len(ch_list)
            total_cap = sum(_channel_totals(ch)["total_msat"] for ch in ch_list) // 1000
            node_status["total_capacity_sats"] = total_cap

        nodes_status[node.name] = node_status

        # Profitability distribution
        if not isinstance(prof, Exception) and "error" not in prof:
            for ch in prof.get("channels", []):
                channel_stats["total"] += 1
                classification = ch.get("profitability_class", "unknown")
                if classification in ("profitable", "strong"):
                    channel_stats["profitable"] += 1
                elif classification in ("bleeder", "underwater"):
                    channel_stats["underwater"] += 1
                elif classification == "zombie":
                    channel_stats["stagnant"] += 1
                # Check for stagnant by balance
                local_pct = ch.get("local_balance_pct", 50)
                if local_pct >= 99:
                    channel_stats["stagnant"] += 1

        # 24h routing stats
        if not isinstance(dashboard, Exception) and "error" not in dashboard:
            period = dashboard.get("period", {})
            routing_24h["volume_sats"] += period.get("volume_sats", 0)
            routing_24h["revenue_sats"] += period.get("gross_revenue_sats", 0) or 0
            routing_24h["forward_count"] += period.get("forward_count", 0)

        # MCF status (use first node's status)
        if not mcf_status and not isinstance(mcf, Exception) and "error" not in mcf:
            mcf_status = {
                "enabled": mcf.get("enabled", False),
                "circuit_breaker_state": mcf.get("circuit_breaker_state", "unknown"),
                "is_healthy": mcf.get("is_healthy", True)
            }

        # NNLB struggling members (dedupe by peer_id, derive issue from health)
        if not isinstance(nnlb, Exception) and "error" not in nnlb:
            for member in nnlb.get("struggling_members", []):
                peer_id = member.get("peer_id", "")
                health = member.get("health", 0)
                # Derive issue from health score
                if health < 20:
                    issue = "critical"
                elif health < 40:
                    issue = "low_health"
                else:
                    issue = "below_threshold"
                # Dedupe: only add if not already seen (first node wins)
                if peer_id and peer_id not in seen_struggling_peers:
                    seen_struggling_peers.add(peer_id)
                    nnlb_struggling.append({
                        "peer_id": peer_id[:16] + "...",  # Truncated for readability
                        "health": health,
                        "issue": issue,
                        "reporting_node": node.name
                    })

        # Connectivity alerts
        if not isinstance(conn_alerts, Exception) and "error" not in conn_alerts:
            alerts_by_severity["critical"] += conn_alerts.get("critical_count", 0)
            alerts_by_severity["warning"] += conn_alerts.get("warning_count", 0)
            alerts_by_severity["info"] += conn_alerts.get("info_count", 0)

    # Calculate percentages
    total_channels = channel_stats["total"]
    channel_distribution = {
        "profitable_pct": round(channel_stats["profitable"] * 100 / total_channels, 1) if total_channels else 0,
        "underwater_pct": round(channel_stats["underwater"] * 100 / total_channels, 1) if total_channels else 0,
        "stagnant_pct": round(channel_stats["stagnant"] * 100 / total_channels, 1) if total_channels else 0,
        "total_channels": total_channels
    }

    # Build AI note
    notes = []
    online_count = sum(1 for n in nodes_status.values() if n.get("status") == "online")
    notes.append(f"{online_count}/{len(nodes_status)} nodes online.")

    if routing_24h["forward_count"] > 0:
        notes.append(f"24h: {routing_24h['forward_count']} forwards, {routing_24h['revenue_sats']:,} sats revenue.")

    if alerts_by_severity["critical"] > 0:
        notes.append(f"CRITICAL: {alerts_by_severity['critical']} alert(s)!")
    elif alerts_by_severity["warning"] > 0:
        notes.append(f"{alerts_by_severity['warning']} warning(s).")

    if mcf_status.get("circuit_breaker_state") == "open":
        notes.append("MCF circuit breaker OPEN!")

    if nnlb_struggling:
        notes.append(f"{len(nnlb_struggling)} member(s) struggling.")

    return {
        "nodes": nodes_status,
        "channel_distribution": channel_distribution,
        "routing_24h": routing_24h,
        "alerts": alerts_by_severity,
        "mcf_health": mcf_status,
        "nnlb_struggling": nnlb_struggling[:5],
        "ai_note": " ".join(notes)
    }


async def handle_routing_intelligence_health(args: Dict) -> Dict:
    """Check routing intelligence data quality."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    import time

    # Get routing intelligence status and channel list
    try:
        intel_status, channels_data = await asyncio.gather(
            node.call("hive-routing-intelligence-status", {}),
            node.call("listpeerchannels"),
        )
    except Exception as e:
        return {"error": f"Failed to get routing intelligence: {e}"}

    if "error" in intel_status:
        return intel_status

    # Calculate pheromone coverage
    # Handle both nested (pheromones.channels) and flat (pheromone_levels) formats
    pheromone_channels = intel_status.get("pheromone_levels", [])
    if not pheromone_channels:
        pheromones = intel_status.get("pheromones", {})
        if isinstance(pheromones, dict):
            pheromone_channels = pheromones.get("channels", [])
        elif isinstance(pheromones, list):
            pheromone_channels = pheromones
    channels_with_data = intel_status.get("pheromone_channels", len(pheromone_channels))

    total_channels = len(channels_data.get("channels", [])) if "error" not in channels_data else 0

    # Check for stale data (>7 days old)
    stale_threshold = time.time() - (7 * 24 * 3600)
    stale_count = 0
    for ch in pheromone_channels:
        last_update = ch.get("last_update", 0) if isinstance(ch, dict) else 0
        if last_update > 0 and last_update < stale_threshold:
            stale_count += 1

    coverage_pct = round(channels_with_data * 100 / total_channels, 1) if total_channels else 0

    # Get stigmergic marker stats - handle both dict and list formats
    markers_data = intel_status.get("stigmergic_markers", [])
    if isinstance(markers_data, list):
        active_markers = intel_status.get("active_markers", len(markers_data))
        # Count unique corridors from markers
        corridors = set()
        for m in markers_data:
            if isinstance(m, dict):
                corridor = m.get("corridor") or m.get("corridor_id")
                if corridor:
                    corridors.add(corridor)
        corridors_tracked = len(corridors)
    else:
        active_markers = markers_data.get("active_count", 0)
        corridors_tracked = markers_data.get("corridors_tracked", 0)

    # Determine health assessment
    needs_backfill = channels_with_data == 0 or coverage_pct < 30
    if needs_backfill:
        recommendation = "needs_backfill"
    elif stale_count > channels_with_data * 0.3:
        recommendation = "partially_stale"
    else:
        recommendation = "healthy"

    ai_note = f"Routing intelligence coverage: {coverage_pct}% ({channels_with_data}/{total_channels} channels). "
    if stale_count > 0:
        ai_note += f"{stale_count} channel(s) have stale data (>7 days). "
    if needs_backfill:
        ai_note += "Run hive_backfill_routing_intelligence to populate data."
    elif recommendation == "partially_stale":
        ai_note += "Some data is stale. Consider partial backfill."
    else:
        ai_note += "Data quality is healthy."

    return {
        "node": node_name,
        "pheromone_coverage": {
            "channels_with_data": channels_with_data,
            "total_channels": total_channels,
            "stale_count": stale_count,
            "coverage_pct": coverage_pct
        },
        "stigmergic_markers": {
            "active_count": active_markers,
            "corridors_tracked": corridors_tracked
        },
        "needs_backfill": needs_backfill,
        "recommendation": recommendation,
        "ai_note": ai_note
    }


async def handle_advisor_channel_history_tool(args: Dict) -> Dict:
    """Query past advisor decisions for a specific channel."""
    node_name = args.get("node")
    channel_id = args.get("channel_id")
    days = args.get("days", 30)

    if not node_name or not channel_id:
        return {"error": "node and channel_id are required"}

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Query advisor database for decisions on this channel
    db = ensure_advisor_db()

    import time
    cutoff_ts = time.time() - (days * 24 * 3600)

    decisions = db.get_decisions_for_channel(node_name, channel_id, since_ts=cutoff_ts)

    # Analyze patterns
    decision_types = {}
    recommendations = {}
    outcomes = {"improved": 0, "unchanged": 0, "worsened": 0, "unknown": 0}
    timestamps = []

    for dec in decisions:
        # Count by type
        dtype = dec.get("decision_type", "unknown")
        decision_types[dtype] = decision_types.get(dtype, 0) + 1

        # Count recommendations
        rec = dec.get("recommendation", "")
        if rec:
            recommendations[rec] = recommendations.get(rec, 0) + 1

        # Count outcomes
        outcome = dec.get("outcome", "unknown")
        outcomes[outcome] = outcomes.get(outcome, 0) + 1

        timestamps.append(dec.get("timestamp", 0))

    # Detect repeated recommendations (same advice >2 times)
    repeated = [r for r, count in recommendations.items() if count > 2]

    # Detect conflicting decisions (back-and-forth)
    conflicting = []
    if "fee_increase" in decision_types and "fee_decrease" in decision_types:
        conflicting.append("fee_increase vs fee_decrease")

    # Calculate decision frequency
    decision_frequency_days = None
    if len(timestamps) >= 2:
        timestamps.sort()
        avg_gap = (timestamps[-1] - timestamps[0]) / (len(timestamps) - 1)
        decision_frequency_days = round(avg_gap / 86400, 1)

    ai_note = f"Found {len(decisions)} decision(s) for channel {channel_id} in last {days} days. "
    if repeated:
        ai_note += f"Repeated recommendations: {', '.join(repeated)}. "
    if conflicting:
        ai_note += f"Conflicting decisions detected: {', '.join(conflicting)}. "
    if outcomes["improved"] > outcomes["worsened"]:
        ai_note += "Past decisions have generally helped."
    elif outcomes["worsened"] > outcomes["improved"]:
        ai_note += "Past decisions haven't been effective - try different approach."

    return {
        "node": node_name,
        "channel_id": channel_id,
        "days_queried": days,
        "decision_count": len(decisions),
        "decisions": decisions[:20],  # Limit to 20 most recent
        "pattern_detection": {
            "repeated_recommendations": repeated,
            "conflicting_decisions": conflicting,
            "decision_frequency_days": decision_frequency_days,
            "outcomes_summary": outcomes
        },
        "decision_type_counts": decision_types,
        "ai_note": ai_note
    }


async def handle_connectivity_recommendations(args: Dict) -> Dict:
    """Get actionable connectivity improvement recommendations."""
    node_name = args.get("node")

    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}

    # Get connectivity alerts and member info
    try:
        alerts_data, members_data, fleet_health = await asyncio.gather(
            node.call("hive-connectivity-alerts", {}),
            node.call("hive-members"),
            node.call("hive-fleet-health", {}),
        )
    except Exception as e:
        return {"error": f"Failed to get connectivity data: {e}"}

    if "error" in alerts_data:
        return alerts_data

    alerts = alerts_data.get("alerts", [])
    members_list = members_data.get("members", []) if "error" not in members_data else []

    # Build pubkey -> alias map
    alias_map = {}
    for m in members_list:
        pubkey = m.get("pubkey") or m.get("peer_id")
        if pubkey:
            alias_map[pubkey] = m.get("alias", pubkey[:16] + "...")

    # Get well-connected members as potential targets
    well_connected = []
    for m in members_list:
        connections = m.get("hive_channel_count", 0)
        if connections >= 3:
            well_connected.append({
                "pubkey": m.get("pubkey") or m.get("peer_id"),
                "alias": m.get("alias", ""),
                "connections": connections
            })

    recommendations = []
    for alert in alerts:
        alert_type = alert.get("type", "unknown")
        severity = alert.get("severity", "info")
        affected_member = alert.get("member_id") or alert.get("peer_id")
        affected_alias = alias_map.get(affected_member, affected_member[:16] + "..." if affected_member else "?")

        rec = {
            "alert_type": alert_type,
            "severity": severity,
            "member": {
                "pubkey": affected_member[:16] + "..." if affected_member else "?",
                "alias": affected_alias
            },
            "recommendation": {}
        }

        # Generate specific recommendations based on alert type
        if alert_type in ("disconnected", "no_hive_channels"):
            # Member has no hive channels - they need to open to someone
            target = well_connected[0] if well_connected else None
            rec["recommendation"] = {
                "who_should_act": affected_alias,
                "action": "open_channel_to",
                "target": target["alias"] if target else "any well-connected member",
                "target_pubkey": target["pubkey"][:16] + "..." if target else None,
                "expected_improvement": "Establishes fleet connectivity, enables zero-fee rebalancing",
                "priority": 5
            }
        elif alert_type in ("isolated", "low_connectivity"):
            # Member has few connections - others should open to them
            rec["recommendation"] = {
                "who_should_act": "well-connected members",
                "action": "open_channel_to",
                "target": affected_alias,
                "target_pubkey": affected_member[:16] + "..." if affected_member else None,
                "expected_improvement": "Improves mesh connectivity, reduces path length",
                "priority": 3
            }
        elif alert_type == "offline":
            rec["recommendation"] = {
                "who_should_act": affected_alias,
                "action": "improve_uptime",
                "target": None,
                "expected_improvement": "Node must be online to participate in routing and governance",
                "priority": 4
            }
        elif alert_type == "low_liquidity":
            rec["recommendation"] = {
                "who_should_act": affected_alias,
                "action": "add_liquidity",
                "target": None,
                "expected_improvement": "More capital enables more routing revenue",
                "priority": 2
            }
        else:
            rec["recommendation"] = {
                "who_should_act": affected_alias,
                "action": "investigate",
                "target": None,
                "expected_improvement": "Unknown - manual review needed",
                "priority": 1
            }

        recommendations.append(rec)

    # Sort by priority
    recommendations.sort(key=lambda x: x["recommendation"].get("priority", 0), reverse=True)

    # Build AI note
    critical_count = sum(1 for r in recommendations if r["severity"] == "critical")
    warning_count = sum(1 for r in recommendations if r["severity"] == "warning")

    ai_note = f"Generated {len(recommendations)} recommendation(s). "
    if critical_count > 0:
        ai_note += f"{critical_count} CRITICAL requiring immediate action. "
    if warning_count > 0:
        ai_note += f"{warning_count} warnings. "
    if not recommendations:
        ai_note = "No connectivity issues found. Fleet is well-connected."

    return {
        "node": node_name,
        "recommendation_count": len(recommendations),
        "recommendations": recommendations[:10],  # Top 10
        "well_connected_targets": well_connected[:3],
        "ai_note": ai_note
    }


# =============================================================================
# Automation Tools (Phase 2 - Hex Enhancement)
# =============================================================================

async def handle_stagnant_channels(args: Dict) -> Dict:
    """List channels with ≥95% local balance with enriched context."""
    import time
    
    node_name = args.get("node")
    min_local_pct = args.get("min_local_pct", 95)
    min_age_days = args.get("min_age_days", 14)
    
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    
    # Get current blockheight for age calculation
    info = await node.call("getinfo")
    if "error" in info:
        return info
    current_blockheight = info.get("blockheight", 0)
    
    # Get all channels
    channels_result = await node.call("listpeerchannels")
    if "error" in channels_result:
        return channels_result
    
    # Get forwards for last forward calculation
    forwards = await node.call("listforwards", {"status": "settled"})
    forwards_list = forwards.get("forwards", []) if not forwards.get("error") else []
    
    # Build map of channel -> last forward timestamp
    channel_last_forward: Dict[str, int] = {}
    for fwd in forwards_list:
        for ch_key in ["in_channel", "out_channel"]:
            ch_id = fwd.get(ch_key)
            if ch_id:
                ts = _coerce_ts(fwd.get("resolved_time") or fwd.get("resolved_at") or 0)
                if ch_id not in channel_last_forward or ts > channel_last_forward[ch_id]:
                    channel_last_forward[ch_id] = ts
    
    # Get peer intel if available
    peer_intel_map: Dict[str, Dict] = {}
    try:
        db = ensure_advisor_db()
        # Will be populated per-peer as needed
    except Exception:
        db = None
    
    now = int(time.time())
    stagnant_channels = []
    
    for ch in channels_result.get("channels", []):
        totals = _channel_totals(ch)
        total_msat = totals["total_msat"]
        local_msat = totals["local_msat"]
        
        if total_msat == 0:
            continue
            
        local_pct = round((local_msat / total_msat) * 100, 2)
        
        if local_pct < min_local_pct:
            continue
        
        channel_id = ch.get("short_channel_id", "")
        peer_id = ch.get("peer_id", "")
        
        # Calculate channel age
        channel_age_days = _scid_to_age_days(channel_id, current_blockheight) if channel_id else None
        
        if channel_age_days is not None and channel_age_days < min_age_days:
            continue
        
        # Get peer alias
        peer_alias = ""
        try:
            nodes_result = await node.call("listnodes", {"id": peer_id})
            if nodes_result.get("nodes"):
                peer_alias = nodes_result["nodes"][0].get("alias", "")
        except Exception:
            pass
        
        # Get current fee
        local_updates = ch.get("updates", {}).get("local", {})
        current_fee_ppm = local_updates.get("fee_proportional_millionths", 0)
        
        # Calculate days since last forward
        last_forward_ts = channel_last_forward.get(channel_id, 0)
        days_since_forward = None
        if last_forward_ts > 0:
            days_since_forward = (now - last_forward_ts) // 86400
        
        # Get peer quality from advisor if available
        peer_quality = None
        peer_recommendation = None
        if db and peer_id:
            try:
                intel = db.get_peer_intel(peer_id)
                if intel:
                    peer_quality = intel.get("quality_score")
                    peer_recommendation = intel.get("recommendation")
            except Exception:
                pass
        
        # Generate recommendation
        recommendation = "wait"
        reasoning = ""
        
        if peer_recommendation == "avoid":
            recommendation = "close"
            reasoning = "Peer marked as 'avoid' - consider closing channel"
        elif channel_age_days is not None and channel_age_days > 90:
            if days_since_forward is not None and days_since_forward > 30:
                recommendation = "close"
                reasoning = f"Channel >90 days old with no forwards in {days_since_forward} days"
            elif current_fee_ppm > 100:
                recommendation = "fee_reduction"
                reasoning = f"Channel >90 days old, try reducing fee from {current_fee_ppm} ppm"
            else:
                recommendation = "static_policy"
                reasoning = "Channel >90 days old with low fee already - apply static policy"
        elif channel_age_days is not None and channel_age_days > 30:
            if current_fee_ppm > 200:
                recommendation = "fee_reduction"
                reasoning = f"Consider reducing fee from {current_fee_ppm} ppm to attract flow"
            else:
                recommendation = "wait"
                reasoning = "Channel 30-90 days old - give more time to attract flow"
        else:
            recommendation = "wait"
            reasoning = "Channel too young for intervention"
        
        stagnant_channels.append({
            "channel_id": channel_id,
            "peer_id": peer_id,
            "peer_alias": peer_alias,
            "capacity_sats": total_msat // 1000,
            "local_pct": local_pct,
            "channel_age_days": channel_age_days,
            "days_since_last_forward": days_since_forward,
            "peer_quality": peer_quality,
            "current_fee_ppm": current_fee_ppm,
            "recommendation": recommendation,
            "reasoning": reasoning
        })
    
    # Sort by recommendation priority: close > fee_reduction > static_policy > wait
    priority = {"close": 0, "fee_reduction": 1, "static_policy": 2, "wait": 3}
    stagnant_channels.sort(key=lambda x: (priority.get(x["recommendation"], 99), -(x.get("channel_age_days") or 0)))
    
    return {
        "node": node_name,
        "stagnant_count": len(stagnant_channels),
        "channels": stagnant_channels,
        "ai_note": f"Found {len(stagnant_channels)} stagnant channels (≥{min_local_pct}% local, ≥{min_age_days} days old)"
    }


async def handle_bulk_policy(args: Dict) -> Dict:
    """Apply policies to multiple channels matching criteria."""
    node_name = args.get("node")
    filter_type = args.get("filter_type")
    strategy = args.get("strategy")
    fee_ppm = args.get("fee_ppm")
    rebalance = args.get("rebalance")
    dry_run = args.get("dry_run", True)
    custom_filter = args.get("custom_filter", {})
    
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    
    if not filter_type:
        return {"error": "filter_type is required"}
    
    # Get channels based on filter type
    matched_channels = []
    
    if filter_type == "stagnant":
        # Use stagnant_channels logic
        stagnant_result = await handle_stagnant_channels({
            "node": node_name,
            "min_local_pct": custom_filter.get("min_local_pct", 95),
            "min_age_days": custom_filter.get("min_age_days", 14)
        })
        if "error" in stagnant_result:
            return stagnant_result
        matched_channels = stagnant_result.get("channels", [])
        
    elif filter_type == "zombie":
        # Get profitability and find zombies
        prof = await node.call("revenue-profitability", {})
        if "error" in prof:
            return prof
        channels_by_class = prof.get("channels_by_class", {})
        for ch in channels_by_class.get("zombie", []):
            matched_channels.append({
                "channel_id": ch.get("short_channel_id"),
                "peer_id": ch.get("peer_id"),
                "peer_alias": ch.get("peer_alias", ""),
                "classification": "zombie"
            })
            
    elif filter_type == "underwater":
        prof = await node.call("revenue-profitability", {})
        if "error" in prof:
            return prof
        channels_by_class = prof.get("channels_by_class", {})
        for ch in channels_by_class.get("bleeder", []):
            matched_channels.append({
                "channel_id": ch.get("short_channel_id"),
                "peer_id": ch.get("peer_id"),
                "peer_alias": ch.get("peer_alias", ""),
                "classification": "bleeder"
            })
            
    elif filter_type == "depleted":
        # Channels with <5% local balance
        channels_result = await node.call("listpeerchannels")
        if "error" in channels_result:
            return channels_result
        for ch in channels_result.get("channels", []):
            totals = _channel_totals(ch)
            if totals["total_msat"] == 0:
                continue
            local_pct = (totals["local_msat"] / totals["total_msat"]) * 100
            if local_pct < 5:
                matched_channels.append({
                    "channel_id": ch.get("short_channel_id"),
                    "peer_id": ch.get("peer_id"),
                    "local_pct": round(local_pct, 2),
                    "classification": "depleted"
                })
                
    elif filter_type == "custom":
        # Custom filter based on provided criteria
        channels_result = await node.call("listpeerchannels")
        if "error" in channels_result:
            return channels_result
        for ch in channels_result.get("channels", []):
            # Apply custom filters
            match = True
            totals = _channel_totals(ch)
            local_pct = (totals["local_msat"] / totals["total_msat"] * 100) if totals["total_msat"] else 0
            
            if "min_local_pct" in custom_filter and local_pct < custom_filter["min_local_pct"]:
                match = False
            if "max_local_pct" in custom_filter and local_pct > custom_filter["max_local_pct"]:
                match = False
            if "min_capacity_sats" in custom_filter and (totals["total_msat"] // 1000) < custom_filter["min_capacity_sats"]:
                match = False
                
            if match:
                matched_channels.append({
                    "channel_id": ch.get("short_channel_id"),
                    "peer_id": ch.get("peer_id"),
                    "local_pct": round(local_pct, 2)
                })
    else:
        return {"error": f"Unknown filter_type: {filter_type}"}
    
    # Apply policies
    applied = []
    errors = []
    
    for ch in matched_channels:
        peer_id = ch.get("peer_id")
        if not peer_id:
            continue
            
        if dry_run:
            applied.append({
                "peer_id": peer_id,
                "channel_id": ch.get("channel_id"),
                "would_apply": {
                    "strategy": strategy,
                    "fee_ppm": fee_ppm,
                    "rebalance": rebalance
                }
            })
        else:
            # Actually apply the policy
            params = {"action": "set", "peer_id": peer_id}
            if strategy:
                params["strategy"] = strategy
            if fee_ppm is not None:
                params["fee_ppm"] = fee_ppm
            if rebalance:
                params["rebalance"] = rebalance
                
            result = await node.call("revenue-policy", params)
            if "error" in result:
                errors.append({"peer_id": peer_id, "error": result["error"]})
            else:
                applied.append({
                    "peer_id": peer_id,
                    "channel_id": ch.get("channel_id"),
                    "applied": params
                })
    
    return {
        "node": node_name,
        "filter_type": filter_type,
        "matched_count": len(matched_channels),
        "applied_count": len(applied),
        "dry_run": dry_run,
        "applied": applied,
        "errors": errors if errors else None,
        "ai_note": f"{'Would apply' if dry_run else 'Applied'} policies to {len(applied)} channels matching '{filter_type}' filter"
    }


async def handle_enrich_peer(args: Dict) -> Dict:
    """Get external data for peer evaluation from mempool.space."""
    peer_id = args.get("peer_id")
    timeout_seconds = args.get("timeout_seconds", 10)
    
    if not peer_id:
        return {"error": "peer_id is required"}
    
    # Validate peer_id format (should be 66 hex chars)
    if not isinstance(peer_id, str) or len(peer_id) != 66:
        return {"error": "peer_id must be a 66-character hex pubkey"}
    
    MEMPOOL_API = "https://mempool.space/api"
    
    result = {
        "peer_id": peer_id,
        "source": "mempool.space",
        "available": False
    }
    
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            resp = await client.get(f"{MEMPOOL_API}/v1/lightning/nodes/{peer_id}")
            
            if resp.status_code == 200:
                data = resp.json()
                result["available"] = True
                result["alias"] = data.get("alias", "")
                result["capacity_sats"] = data.get("capacity", 0)
                result["channel_count"] = data.get("active_channel_count", 0)
                result["first_seen"] = data.get("first_seen")
                result["updated_at"] = data.get("updated_at")
                result["color"] = data.get("color", "")
                
                # Calculate node age if first_seen is available
                if data.get("first_seen"):
                    import time
                    node_age_days = (int(time.time()) - data["first_seen"]) // 86400
                    result["node_age_days"] = node_age_days
                    
            elif resp.status_code == 404:
                result["error"] = "Node not found in mempool.space database"
            else:
                result["error"] = f"API returned status {resp.status_code}"
                
    except httpx.TimeoutException:
        result["error"] = f"API timeout after {timeout_seconds}s"
    except Exception as e:
        result["error"] = f"API error: {str(e)}"
    
    return result


async def handle_enrich_proposal(args: Dict) -> Dict:
    """Enhance a pending action with external peer data."""
    node_name = args.get("node")
    action_id = args.get("action_id")
    
    node = fleet.get_node(node_name)
    if not node:
        return {"error": f"Unknown node: {node_name}"}
    
    if action_id is None:
        return {"error": "action_id is required"}
    
    # Get pending actions
    pending = await node.call("hive-pending-actions")
    if "error" in pending:
        return pending
    
    # Find the specific action
    target_action = None
    for action in pending.get("actions", []):
        if action.get("id") == action_id:
            target_action = action
            break
    
    if not target_action:
        return {"error": f"Action {action_id} not found in pending actions"}
    
    # Extract peer_id from action
    peer_id = target_action.get("peer_id") or target_action.get("target_peer") or target_action.get("details", {}).get("peer_id")
    
    if not peer_id:
        return {
            "action": target_action,
            "enrichment": None,
            "note": "No peer_id found in action to enrich"
        }
    
    # Get external peer data
    external_data = await handle_enrich_peer({"peer_id": peer_id})
    
    # Get internal peer intel if available
    internal_intel = None
    try:
        db = ensure_advisor_db()
        if db:
            internal_intel = db.get_peer_intel(peer_id)
    except Exception:
        pass
    
    # Generate enhanced recommendation
    recommendation = None
    reasoning = []
    
    action_type = target_action.get("action_type", "")
    
    if action_type in ("channel_open", "expansion"):
        # Evaluate for channel open
        if external_data.get("available"):
            capacity = external_data.get("capacity_sats", 0)
            channels = external_data.get("channel_count", 0)
            node_age = external_data.get("node_age_days", 0)
            
            score = 0
            if capacity > 100_000_000:  # >1 BTC
                score += 2
                reasoning.append(f"Good capacity: {capacity:,} sats")
            elif capacity > 10_000_000:  # >0.1 BTC
                score += 1
                reasoning.append(f"Moderate capacity: {capacity:,} sats")
            else:
                reasoning.append(f"Low capacity: {capacity:,} sats")
                
            if channels >= 15:
                score += 2
                reasoning.append(f"Well-connected: {channels} channels")
            elif channels >= 5:
                score += 1
                reasoning.append(f"Some connectivity: {channels} channels")
            else:
                reasoning.append(f"Low connectivity: {channels} channels")
                
            if node_age > 365:
                score += 1
                reasoning.append(f"Established node: {node_age} days old")
            elif node_age < 30:
                reasoning.append(f"New node: only {node_age} days old")
                
            if score >= 4:
                recommendation = "approve"
            elif score >= 2:
                recommendation = "review"
            else:
                recommendation = "caution"
        else:
            reasoning.append("External data unavailable - manual review recommended")
            recommendation = "review"
            
        if internal_intel:
            if internal_intel.get("recommendation") == "avoid":
                recommendation = "reject"
                reasoning.append("Internal intel: peer marked as 'avoid'")
            elif internal_intel.get("quality_score", 0) > 0.7:
                reasoning.append(f"Internal intel: good quality score ({internal_intel['quality_score']:.2f})")
    
    return {
        "node": node_name,
        "action_id": action_id,
        "action": target_action,
        "external_data": external_data,
        "internal_intel": internal_intel,
        "recommendation": recommendation,
        "reasoning": reasoning,
        "ai_note": f"Enriched action {action_id} with peer data. Recommendation: {recommendation or 'N/A'}"
    }


# =============================================================================
# Tool Dispatch Registry
# =============================================================================

TOOL_HANDLERS: Dict[str, Any] = {
    # Hive core tools
    "hive_health": handle_health,
    "hive_fleet_snapshot": handle_fleet_snapshot,
    "hive_anomalies": handle_anomalies,
    "hive_compare_periods": handle_compare_periods,
    "hive_channel_deep_dive": handle_channel_deep_dive,
    "hive_recommended_actions": handle_recommended_actions,
    "hive_peer_search": handle_peer_search,
    "hive_status": handle_hive_status,
    "hive_pending_actions": handle_pending_actions,
    "hive_approve_action": handle_approve_action,
    "hive_reject_action": handle_reject_action,
    "hive_members": handle_members,
    "hive_onboard_new_members": handle_onboard_new_members,
    "hive_propose_promotion": handle_propose_promotion,
    "hive_vote_promotion": handle_vote_promotion,
    "hive_pending_promotions": handle_pending_promotions,
    "hive_execute_promotion": handle_execute_promotion,
    # Membership lifecycle
    "hive_vouch": handle_vouch,
    "hive_leave": handle_leave,
    "hive_force_promote": handle_force_promote,
    "hive_request_promotion": handle_request_promotion,
    "hive_remove_member": handle_remove_member,
    "hive_genesis": handle_genesis,
    "hive_invite": handle_invite,
    "hive_join": handle_join,
    # Ban governance
    "hive_propose_ban": handle_propose_ban,
    "hive_vote_ban": handle_vote_ban,
    "hive_pending_bans": handle_pending_bans,
    # Health/reputation monitoring
    "hive_nnlb_status": handle_nnlb_status,
    "hive_peer_reputations": handle_peer_reputations,
    "hive_reputation_stats": handle_reputation_stats,
    "hive_contribution": handle_contribution,
    "hive_node_info": handle_node_info,
    "hive_channels": handle_channels,
    "hive_set_fees": handle_set_fees,
    "hive_topology_analysis": handle_topology_analysis,
    "hive_planner_ignore": handle_planner_ignore,
    "hive_planner_unignore": handle_planner_unignore,
    "hive_planner_ignored_peers": handle_planner_ignored_peers,
    "hive_governance_mode": handle_governance_mode,
    "hive_expansion_mode": handle_expansion_mode,
    "hive_bump_version": handle_bump_version,
    "hive_gossip_stats": handle_gossip_stats,
    # Splice coordination
    "hive_splice_check": handle_splice_check,
    "hive_splice_recommendations": handle_splice_recommendations,
    "hive_splice": handle_splice,
    "hive_splice_status": handle_splice_status,
    "hive_splice_abort": handle_splice_abort,
    "hive_liquidity_intelligence": handle_liquidity_intelligence,
    # Anticipatory Liquidity (Phase 7.1)
    "hive_anticipatory_status": handle_anticipatory_status,
    "hive_detect_patterns": handle_detect_patterns,
    "hive_predict_liquidity": handle_predict_liquidity,
    "hive_anticipatory_predictions": handle_anticipatory_predictions,
    # Time-Based Fee (Phase 7.4)
    "hive_time_fee_status": handle_time_fee_status,
    "hive_time_fee_adjustment": handle_time_fee_adjustment,
    "hive_time_peak_hours": handle_time_peak_hours,
    "hive_time_low_hours": handle_time_low_hours,
    # Routing Intelligence (Pheromones + Stigmergic Markers)
    "hive_backfill_routing_intelligence": handle_backfill_routing_intelligence,
    "hive_routing_intelligence_status": handle_routing_intelligence_status,
    # cl-revenue-ops
    "revenue_status": handle_revenue_status,
    "revenue_profitability": handle_revenue_profitability,
    "revenue_dashboard": handle_revenue_dashboard,
    "revenue_portfolio": handle_revenue_portfolio,
    "revenue_portfolio_summary": handle_revenue_portfolio_summary,
    "revenue_portfolio_rebalance": handle_revenue_portfolio_rebalance,
    "revenue_portfolio_correlations": handle_revenue_portfolio_correlations,
    "revenue_policy": handle_revenue_policy,
    "revenue_set_fee": handle_revenue_set_fee,
    "revenue_rebalance": handle_revenue_rebalance,
    "revenue_report": handle_revenue_report,
    "revenue_config": handle_revenue_config,
    "config_adjust": handle_config_adjust,
    "config_adjustment_history": handle_config_adjustment_history,
    "config_effectiveness": handle_config_effectiveness,
    "config_measure_outcomes": handle_config_measure_outcomes,
    "revenue_debug": handle_revenue_debug,
    "revenue_history": handle_revenue_history,
    "revenue_competitor_analysis": handle_revenue_competitor_analysis,
    # Diagnostic tools
    "hive_node_diagnostic": handle_hive_node_diagnostic,
    "revenue_ops_health": handle_revenue_ops_health,
    "advisor_validate_data": handle_advisor_validate_data,
    "advisor_dedup_status": handle_advisor_dedup_status,
    "rebalance_diagnostic": handle_rebalance_diagnostic,
    # Advisor database
    "advisor_record_snapshot": handle_advisor_record_snapshot,
    "advisor_get_trends": handle_advisor_get_trends,
    "advisor_get_velocities": handle_advisor_get_velocities,
    "advisor_get_channel_history": handle_advisor_get_channel_history,
    "advisor_record_decision": handle_advisor_record_decision,
    "advisor_get_recent_decisions": handle_advisor_get_recent_decisions,
    "advisor_db_stats": handle_advisor_db_stats,
    # Advisor intelligence
    "advisor_get_context_brief": handle_advisor_get_context_brief,
    "advisor_check_alert": handle_advisor_check_alert,
    "advisor_record_alert": handle_advisor_record_alert,
    "advisor_resolve_alert": handle_advisor_resolve_alert,
    "advisor_get_peer_intel": handle_advisor_get_peer_intel,
    "advisor_measure_outcomes": handle_advisor_measure_outcomes,
    # Proactive Advisor
    "advisor_run_cycle": handle_advisor_run_cycle,
    "advisor_run_cycle_all": handle_advisor_run_cycle_all,
    "advisor_get_goals": handle_advisor_get_goals,
    "advisor_set_goal": handle_advisor_set_goal,
    "advisor_get_learning": handle_advisor_get_learning,
    "advisor_get_status": handle_advisor_get_status,
    "advisor_get_cycle_history": handle_advisor_get_cycle_history,
    "advisor_scan_opportunities": handle_advisor_scan_opportunities,
    # Phase 3: Automation Tools
    "auto_evaluate_proposal": handle_auto_evaluate_proposal,
    "process_all_pending": handle_process_all_pending,
    "stagnant_channels": handle_stagnant_channels,
    "remediate_stagnant": handle_remediate_stagnant,
    "execute_safe_opportunities": handle_execute_safe_opportunities,
    # Routing Pool
    "pool_status": handle_pool_status,
    "pool_member_status": handle_pool_member_status,
    "pool_distribution": handle_pool_distribution,
    "pool_snapshot": handle_pool_snapshot,
    "pool_settle": handle_pool_settle,
    # Phase 1: Yield Metrics
    "yield_metrics": handle_yield_metrics,
    "yield_summary": handle_yield_summary,
    "velocity_prediction": handle_velocity_prediction,
    "critical_velocity": handle_critical_velocity,
    "internal_competition": handle_internal_competition,
    # Kalman Velocity Integration
    "kalman_velocity_query": handle_kalman_velocity_query,
    # Phase 2: Fee Coordination
    "coord_fee_recommendation": handle_coord_fee_recommendation,
    "corridor_assignments": handle_corridor_assignments,
    "stigmergic_markers": handle_stigmergic_markers,
    "defense_status": handle_defense_status,
    "ban_candidates": handle_ban_candidates,
    "accumulated_warnings": handle_accumulated_warnings,
    "pheromone_levels": handle_pheromone_levels,
    "fee_coordination_status": handle_fee_coordination_status,
    # Phase 3: Cost Reduction
    "rebalance_recommendations": handle_rebalance_recommendations,
    "fleet_rebalance_path": handle_fleet_rebalance_path,
    "circular_flow_status": handle_circular_flow_status,
    "execute_hive_circular_rebalance": handle_execute_hive_circular_rebalance,
    "cost_reduction_status": handle_cost_reduction_status,
    # Routing Intelligence
    "routing_stats": handle_routing_stats,
    "route_suggest": handle_route_suggest,
    # Channel Rationalization
    "coverage_analysis": handle_coverage_analysis,
    "close_recommendations": handle_close_recommendations,
    "rationalization_summary": handle_rationalization_summary,
    "rationalization_status": handle_rationalization_status,
    # Phase 5: Strategic Positioning
    "valuable_corridors": handle_valuable_corridors,
    "exchange_coverage": handle_exchange_coverage,
    "positioning_recommendations": handle_positioning_recommendations,
    "flow_recommendations": handle_flow_recommendations,
    "positioning_summary": handle_positioning_summary,
    "positioning_status": handle_positioning_status,
    # Physarum Auto-Trigger (Phase 7.2)
    "physarum_cycle": handle_physarum_cycle,
    "physarum_status": handle_physarum_status,
    # Settlement (BOLT12 Revenue Distribution)
    "settlement_register_offer": handle_settlement_register_offer,
    "settlement_generate_offer": handle_settlement_generate_offer,
    "settlement_list_offers": handle_settlement_list_offers,
    "settlement_calculate": handle_settlement_calculate,
    "settlement_execute": handle_settlement_execute,
    "settlement_history": handle_settlement_history,
    "settlement_period_details": handle_settlement_period_details,
    # Phase 12: Distributed Settlement
    "distributed_settlement_status": handle_distributed_settlement_status,
    "distributed_settlement_proposals": handle_distributed_settlement_proposals,
    "distributed_settlement_participation": handle_distributed_settlement_participation,
    # Network Metrics
    "hive_network_metrics": handle_network_metrics,
    "hive_rebalance_hubs": handle_rebalance_hubs,
    "hive_rebalance_path": handle_rebalance_path,
    # Fleet Health Monitoring
    "hive_fleet_health": handle_fleet_health,
    "hive_connectivity_alerts": handle_connectivity_alerts,
    "hive_member_connectivity": handle_member_connectivity,
    # Promotion Criteria
    "hive_neophyte_rankings": handle_neophyte_rankings,
    # MCF (Min-Cost Max-Flow) Optimization (Phase 15)
    "hive_mcf_status": handle_mcf_status,
    "hive_mcf_solve": handle_mcf_solve,
    "hive_mcf_assignments": handle_mcf_assignments,
    "hive_mcf_optimized_path": handle_mcf_optimized_path,
    "hive_mcf_health": handle_mcf_health,
    # Phase 4: Membership & Settlement (Hex Automation)
    "membership_dashboard": handle_membership_dashboard,
    "check_neophytes": handle_check_neophytes,
    "settlement_readiness": handle_settlement_readiness,
    "run_settlement_cycle": handle_run_settlement_cycle,
    # Phase 5: Monitoring & Health (Hex Automation)
    "fleet_health_summary": handle_fleet_health_summary,
    "routing_intelligence_health": handle_routing_intelligence_health,
    "advisor_channel_history": handle_advisor_channel_history_tool,
    "connectivity_recommendations": handle_connectivity_recommendations,
    # Phase 2: Automation Tools (Hex Enhancement)
    "bulk_policy": handle_bulk_policy,
    "enrich_peer": handle_enrich_peer,
    "enrich_proposal": handle_enrich_proposal,
}


# =============================================================================
# Main
# =============================================================================

async def main():
    """Run the MCP server."""
    # Load node configuration
    config_path = os.environ.get("HIVE_NODES_CONFIG")
    if config_path and os.path.exists(config_path):
        try:
            fleet.load_config(config_path)
            await fleet.connect_all()
        except Exception as e:
            logger.error(f"Failed to load/connect nodes: {e}")
            sys.exit(1)
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
