# MCP Server for AI Agent Integration

The `tools/mcp-hive-server.py` provides a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server that allows AI agents to manage your Hive fleet directly. This works with any MCP-compatible agent: Moltbots, Claude Code, Clawdbot, or similar tools.

> 📖 **For AI agents**: See [MOLTY.md](../MOLTY.md) for agent-specific instructions on how to use these tools effectively.

## Overview

Instead of running an embedded oracle plugin on each node, this approach lets an external AI agent act as the decision-maker:

```
┌─────────────────┐
│   AI Agent      │  ← AI Decision Making
│  (MCP Client)   │     (Moltbots, Claude Code, Clawdbot, etc.)
└────────┬────────┘
         │ MCP Protocol
         ▼
┌─────────────────┐
│ mcp-hive-server │  ← Fleet Management Tools
└────────┬────────┘
         │ REST API / Docker Exec
         ▼
┌─────────────────────────────────────┐
│  Hive Fleet (alice, bob, carol...)  │
│  Running cl-hive plugin             │
└─────────────────────────────────────┘
```

## Prerequisites

- Python 3.10+
- Claude Code CLI installed
- Hive fleet running with cl-hive plugin

## Installation

### 1. Create Python Virtual Environment

```bash
cd /path/to/cl-hive
python3 -m venv .venv
.venv/bin/pip install mcp httpx
```

### 2. Create Node Configuration

Copy an example config from `config/` and customize it. Two modes are supported:

#### Production Mode (REST API)

For production nodes with CLN REST API enabled:

```json
{
  "mode": "rest",
  "nodes": [
    {
      "name": "node1",
      "rest_url": "https://node1.example.com:3001",
      "rune": "your-rune-here",
      "ca_cert": "/path/to/ca.pem"
    },
    {
      "name": "node2",
      "rest_url": "https://node2.example.com:3001",
      "rune": "your-rune-here",
      "ca_cert": "/path/to/ca.pem"
    }
  ]
}
```

**Getting a Rune:**
```bash
lightning-cli createrune
```

**REST API Setup:**
Ensure your CLN node has the `clnrest` plugin enabled with appropriate configuration:
```
# In your CLN config
clnrest-port=3001
clnrest-host=0.0.0.0
```

#### Development Mode (Docker Exec)

For testing with Polar or local Docker containers:

```json
{
  "mode": "docker",
  "network": "regtest",
  "lightning_dir": "/home/clightning/.lightning",
  "nodes": [
    {
      "name": "alice",
      "docker_container": "polar-n1-alice"
    },
    {
      "name": "bob",
      "docker_container": "polar-n1-bob"
    }
  ]
}
```

### 3. Configure Your AI Agent

#### Option A: mcporter (recommended for Moltbots/Clawdbot)

Add to `~/.mcporter/mcporter.json`:

```json
{
  "servers": {
    "hive": {
      "command": ["/path/to/cl-hive/.venv/bin/python", "/path/to/cl-hive/tools/mcp-hive-server.py"],
      "env": {
        "HIVE_NODES_CONFIG": "/path/to/cl-hive/production/nodes.production.json"
      }
    }
  }
}
```

#### Option B: Claude Code native MCP

Create `.mcp.json` in your cl-hive directory:

```json
{
  "mcpServers": {
    "hive": {
      "command": "/path/to/cl-hive/.venv/bin/python",
      "args": ["/path/to/cl-hive/tools/mcp-hive-server.py"],
      "env": {
        "HIVE_NODES_CONFIG": "/path/to/cl-hive/nodes.json"
      }
    }
  }
}
```

### 4. Verify Connection

Test the MCP server is working:

```bash
# With mcporter
mcporter call hive.hive_status

# With Claude Code
claude -p "Use hive_status to check the fleet"
```

## Available Tools

### cl-hive Tools

| Tool | Description |
|------|-------------|
| `hive_status` | Get hive status from all nodes (membership, health, governance mode) |
| `hive_pending_actions` | View actions awaiting approval in advisor mode |
| `hive_approve_action` | Approve a pending action for execution |
| `hive_reject_action` | Reject a pending action with reason |
| `hive_members` | List all hive members with tier and stats |
| `hive_node_info` | Get detailed node info (peers, channels, balance) |
| `hive_channels` | List channels with balance and fee information |
| `hive_set_fees` | Set channel fees for a specific channel |
| `hive_topology_analysis` | Get planner log and topology view |
| `hive_governance_mode` | Get or set governance mode (advisor/autonomous) |

### cl-revenue-ops Tools

| Tool | Description |
|------|-------------|
| `revenue_status` | Plugin status, fee controller state, recent changes |
| `revenue_profitability` | Channel ROI, costs, revenue, classification |
| `revenue_dashboard` | Financial health: TLV, operating margin, ROC |
| `revenue_policy` | Manage peer-level fee/rebalance policies |
| `revenue_set_fee` | Set channel fee with clboss coordination |
| `revenue_rebalance` | Trigger manual rebalance with EV constraints |
| `revenue_report` | Generate summary, peer, hive, or cost reports |
| `revenue_config` | Get/set runtime configuration |
| `revenue_debug` | Diagnostic info for fee or rebalance issues |
| `revenue_history` | Lifetime financial history including closed channels |

### Advisor Database Tools

These tools maintain a local SQLite database for historical tracking and trend analysis:

#### Core Tools

| Tool | Description |
|------|-------------|
| `advisor_get_context_brief` | Pre-run summary with situational awareness (call at START of each run) |
| `advisor_record_snapshot` | Record current fleet state for trend tracking (call at START of each run) |
| `advisor_get_trends` | Get fleet-wide trends over 7/30 days (revenue, capacity, health) |
| `advisor_get_velocities` | Find channels depleting/filling within threshold hours |
| `advisor_get_channel_history` | Get historical data for a specific channel |
| `advisor_record_decision` | Record AI decision to audit trail (call after each approval/rejection) |
| `advisor_get_recent_decisions` | Get recent decisions to avoid repeating recommendations |
| `advisor_db_stats` | Database statistics (record counts, oldest data) |

#### Alert Deduplication Tools

| Tool | Description |
|------|-------------|
| `advisor_check_alert` | Check if alert should be raised (returns: flag/skip/escalate/mention_unresolved) |
| `advisor_record_alert` | Record a new alert (handles deduplication automatically) |
| `advisor_resolve_alert` | Mark an alert as resolved |

#### Peer Intelligence Tools

| Tool | Description |
|------|-------------|
| `advisor_get_peer_intel` | Get peer reputation, reliability score, and recommendation |

#### Outcome Tracking Tools

| Tool | Description |
|------|-------------|
| `advisor_measure_outcomes` | Measure if past decisions (24-72h ago) led to positive outcomes |

**Configuration:**
- Set `ADVISOR_DB_PATH` environment variable to customize database location
- Default: `~/.lightning/advisor.db`

**Capabilities enabled:**
- **Context injection**: Pre-run summary with trends, unresolved alerts, recent decisions
- **Alert deduplication**: Avoid re-flagging same issues every 15 minutes
- **Peer intelligence**: Track peer reliability and profitability over time
- **Outcome tracking**: Measure if decisions led to positive results
- **Velocity tracking**: Predict when channels will deplete/fill
- **Trend analysis**: Compare metrics over time
- **Decision audit**: Track all AI decisions with reasoning

## Available Resources

MCP Resources allow Claude to automatically see fleet status:

| Resource URI | Description |
|-------------|-------------|
| `hive://fleet/status` | Status of all nodes |
| `hive://fleet/pending-actions` | Pending actions needing approval |
| `hive://fleet/summary` | Aggregated fleet metrics |
| `hive://node/{name}/status` | Per-node detailed status |
| `hive://node/{name}/channels` | Channel list and balances |
| `hive://node/{name}/profitability` | Revenue analysis |

## Usage Examples

Once configured, your AI agent can manage the fleet:

```
"Show me the status of all hive nodes"
"What pending actions need approval?"
"Approve action 5 on alice - good expansion target"
"Set fees to 500 PPM on channel 123x1x0 on bob"
"What's the current topology analysis for carol?"
"Switch alice to autonomous mode"
```

For detailed agent instructions, see [MOLTY.md](../MOLTY.md).

## Security Considerations

1. **Rune Permissions**: Create restricted runes that only allow the RPC methods needed.

   **IMPORTANT**: All method patterns must be in a SINGLE array for OR logic. Multiple arrays are ANDed together (which won't work).

   ```bash
   # CORRECT - single array with all methods (OR logic)
   lightning-cli createrune restrictions='[["method^hive-","method^getinfo","method^listfunds","method^listpeerchannels","method^setchannel","method^revenue-","method^feerates"]]'

   # With rate limit (60 calls/min) - two arrays ANDed together
   lightning-cli createrune restrictions='[["method^hive-","method^getinfo","method^listfunds","method^listpeerchannels","method^setchannel","method^revenue-","method^feerates"],["rate=300"]]'
   ```

   **WRONG** - this ANDs all conditions (impossible to satisfy):
   ```bash
   # DON'T DO THIS - separate arrays means method must match ALL patterns
   lightning-cli createrune restrictions='[["method^hive-"],["method^getinfo"],["method^listfunds"]]'
   ```

2. **Network Security**:
   - Use TLS certificates for REST API connections
   - Consider VPN for remote node access
   - Never expose REST API to public internet without authentication

3. **Governance Mode**: Start with `advisor` mode to review all actions before execution:
   ```bash
   lightning-cli hive-set-mode advisor
   ```

## Troubleshooting

### MCP Server Won't Start

Check the virtual environment has required packages:
```bash
.venv/bin/python -c "import mcp; import httpx; print('OK')"
```

### Connection Errors (REST Mode)

1. Verify REST API is running: `curl -k https://node:3001/v1/getinfo -H "Rune: your-rune"`
2. Check CA certificate path is correct
3. Ensure firewall allows connection

### Connection Errors (Docker Mode)

1. Verify container is running: `docker ps | grep polar`
2. Test command manually: `docker exec polar-n1-alice lightning-cli getinfo`
3. Check lightning directory path matches container configuration

### Tool Errors

If a tool returns an error, check:
1. The node has cl-hive plugin loaded: `lightning-cli plugin list | grep hive`
2. The specific RPC command exists: `lightning-cli help | grep hive`

## Development

### Testing Without MCP

You can test the server's functionality directly:

```python
import asyncio
import json

# Test docker exec mode
async def test():
    from mcp_hive_server import HiveFleet

    fleet = HiveFleet()
    fleet.load_config("nodes.json")
    await fleet.connect_all()

    # Test hive-status on all nodes
    results = await fleet.call_all("hive-status")
    print(json.dumps(results, indent=2))

    await fleet.close_all()

asyncio.run(test())
```

### Adding New Tools

To add a new tool:

1. Add the tool definition in `list_tools()`
2. Add a handler function `handle_your_tool(args)`
3. Add the dispatch in `call_tool()`

## Monitoring Daemon

The `tools/hive-monitor.py` daemon provides real-time monitoring and daily reports:

```bash
# Quick status check
./tools/hive-monitor.py --config nodes.json check

# Generate daily report
./tools/hive-monitor.py --config nodes.json report --output report.json

# Run continuous monitoring (alerts for new pending actions, health issues)
./tools/hive-monitor.py --config nodes.json monitor --interval 60
```

## AI Agent Integration

For integrating with your AI agent:

- [MOLTY.md](../MOLTY.md) - Agent instructions for using cl-hive tools
- [CLAUDE.md](../CLAUDE.md) - Development guidance for working on the codebase

> ⚠️ **Deprecated**: The automated systemd timer approach in [AI_ADVISOR_SETUP.md](AI_ADVISOR_SETUP.md) is deprecated. Use direct agent integration instead.

## Related Documentation

- [Governance Modes](../README.md#governance-modes) - Understanding advisor/autonomous modes
- [Polar Testing](testing/polar.md) - Testing with Polar network
