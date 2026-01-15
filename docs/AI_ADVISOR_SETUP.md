# AI Advisor Setup Guide

This guide walks through setting up an automated AI advisor for your Lightning node using Claude Code and the cl-hive MCP server. The advisor runs on a separate management server and connects to your production node via REST API.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Architecture](#architecture)
4. [Step-by-Step Setup](#step-by-step-setup)
5. [Configuration Reference](#configuration-reference)
6. [Customizing the Advisor](#customizing-the-advisor)
7. [Monitoring and Maintenance](#monitoring-and-maintenance)
8. [Troubleshooting](#troubleshooting)

## Overview

The AI advisor provides intelligent oversight for your Lightning node:

| Feature | Description |
|---------|-------------|
| **Pending Action Review** | Approves/rejects channel opens based on criteria |
| **Financial Monitoring** | Tracks revenue, costs, and operating margin |
| **Channel Health** | Flags zombie, bleeder, and unprofitable channels |
| **Automated Reports** | Logs decisions and warnings every 15 minutes |

### What the Advisor Does

- Reviews channel open proposals from the planner
- Makes approval decisions based on configurable criteria
- Monitors financial health via revenue dashboard
- Identifies problematic channels for human review
- Logs all actions and warnings

### What the Advisor Does NOT Do

- Adjust fees (cl-revenue-ops handles this automatically)
- Trigger rebalances (cl-revenue-ops handles this automatically)
- Close channels (only flags for review)
- Make changes outside defined safety limits

## Prerequisites

### On Your Lightning Node

- Core Lightning with cl-hive plugin installed
- cl-revenue-ops plugin installed (for financial monitoring)
- clnrest plugin enabled for REST API access
- Governance mode set to `advisor`

### On Your Management Server

- Linux server with systemd (Ubuntu 20.04+ recommended)
- Python 3.10+
- Node.js 18+ (for Claude Code CLI)
- Network access to Lightning node (VPN recommended)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     MANAGEMENT SERVER                            │
│                                                                  │
│   ┌────────────────┐    ┌──────────────────────────────────┐   │
│   │ systemd timer  │───▶│         Claude Code CLI          │   │
│   │ (15 min cycle) │    │  - Loads system prompt           │   │
│   └────────────────┘    │  - Executes advisor logic        │   │
│                         │  - Makes decisions               │   │
│                         └──────────────┬───────────────────┘   │
│                                        │                        │
│                         ┌──────────────▼───────────────────┐   │
│                         │       MCP Hive Server            │   │
│                         │  - Translates tool calls to RPC  │   │
│                         │  - Manages REST API connection   │   │
│                         └──────────────┬───────────────────┘   │
└────────────────────────────────────────┼────────────────────────┘
                                         │
                              VPN / Private Network
                                         │
┌────────────────────────────────────────▼────────────────────────┐
│                     LIGHTNING NODE                               │
│                                                                  │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐    │
│   │   clnrest   │  │   cl-hive   │  │   cl-revenue-ops    │    │
│   │  REST API   │◀─│   plugin    │  │      plugin         │    │
│   │  :3010      │  │  (advisor)  │  │  (fee automation)   │    │
│   └─────────────┘  └─────────────┘  └─────────────────────┘    │
│                                                                  │
│                      Core Lightning                              │
└──────────────────────────────────────────────────────────────────┘
```

## Step-by-Step Setup

### Step 1: Configure Your Lightning Node

On your production Lightning node:

```bash
# 1. Verify plugins are loaded
lightning-cli plugin list | grep -E "hive|revenue"

# 2. Set governance mode to advisor
lightning-cli hive-set-mode advisor

# 3. Check clnrest configuration
# In your CLN config file:
clnrest-port=3010
clnrest-host=0.0.0.0  # Or your VPN IP
clnrest-protocol=https

# 4. Create restricted rune for the advisor
lightning-cli createrune restrictions='[["method^hive-","method^getinfo","method^listfunds","method^listpeerchannels","method^setchannel","method^revenue-","method^feerates"],["rate=300"]]'
```

**Save the rune** - you'll need it for the management server configuration.

### Step 2: Set Up Management Server

```bash
# 1. Clone the repository
git clone https://github.com/santyr/cl-hive.git
cd cl-hive

# 2. Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install httpx mcp pyln-client

# 3. Create production folder from template
cp -r production.example production
```

### Step 3: Configure Node Connection

Edit `production/nodes.production.json`:

```json
{
  "mode": "rest",
  "nodes": [
    {
      "name": "mainnet",
      "rest_url": "https://10.8.0.1:3010",
      "rune": "YOUR_RUNE_FROM_STEP_1",
      "ca_cert": null
    }
  ]
}
```

**Configuration Options:**

| Field | Description |
|-------|-------------|
| `name` | Identifier for the node (used in MCP tool calls) |
| `rest_url` | Full URL to clnrest API (use VPN IP if applicable) |
| `rune` | Commando rune from Step 1 |
| `ca_cert` | Path to CA certificate (null for self-signed with -k) |

### Step 4: Install Claude Code CLI

```bash
# Install Claude Code
npm install -g @anthropic-ai/claude-code

# Configure API key (choose one method)

# Method 1: Environment variable
export ANTHROPIC_API_KEY="your-api-key"

# Method 2: API key file (persistent)
mkdir -p ~/.anthropic
echo "your-api-key" > ~/.anthropic/api_key
chmod 600 ~/.anthropic/api_key
```

### Step 5: Test the Connection

```bash
cd ~/cl-hive
source .venv/bin/activate

# Test 1: REST API connectivity
curl -k -X POST \
  -H "Rune: YOUR_RUNE" \
  https://YOUR_NODE_IP:3010/v1/getinfo

# Test 2: MCP server loads
HIVE_NODES_CONFIG=production/nodes.production.json \
  python3 tools/mcp-hive-server.py --help

# Test 3: Claude with MCP tools
claude -p "Use hive_node_info for mainnet" \
  --mcp-config production/mcp-config.json \
  --allowedTools "mcp__hive__*"

# Test 4: Full advisor run
./production/scripts/run-advisor.sh
```

### Step 6: Install Systemd Timer

```bash
# Create systemd user directory
mkdir -p ~/.config/systemd/user

# Create service file (adjust WorkingDirectory path as needed)
cat > ~/.config/systemd/user/hive-advisor.service << 'EOF'
[Unit]
Description=Hive AI Advisor - Review and Act on Pending Actions
After=network-online.target

[Service]
Type=oneshot
Environment=PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin
WorkingDirectory=%h/cl-hive
ExecStart=%h/cl-hive/production/scripts/run-advisor.sh
TimeoutStartSec=300
StandardOutput=journal
StandardError=journal
SyslogIdentifier=hive-advisor
MemoryMax=1G
CPUQuota=80%
Restart=no

[Install]
WantedBy=default.target
EOF

# Copy timer
cp ~/cl-hive/production/systemd/hive-advisor.timer ~/.config/systemd/user/

# Enable and start
systemctl --user daemon-reload
systemctl --user enable hive-advisor.timer
systemctl --user start hive-advisor.timer

# Verify
systemctl --user status hive-advisor.timer
systemctl --user list-timers | grep hive
```

## Configuration Reference

### Rune Syntax

Commando runes use array-based restrictions:

- **Single array** = OR logic (match any)
- **Multiple arrays** = AND logic (must match all)

```bash
# CORRECT: All methods in ONE array (OR)
restrictions='[["method^hive-","method^getinfo","method^revenue-"]]'

# CORRECT: Methods OR'd, then AND with rate limit
restrictions='[["method^hive-","method^getinfo","method^revenue-"],["rate=300"]]'

# WRONG: This ANDs all methods (impossible to satisfy)
restrictions='[["method^hive-"],["method^getinfo"],["method^revenue-"]]'
```

### Strategy Prompts

| File | Purpose |
|------|---------|
| `system_prompt.md` | AI personality, safety limits, output format |
| `approval_criteria.md` | Rules for approving/rejecting channel opens |

### Safety Constraints

Default limits in `system_prompt.md`:

```markdown
- Maximum 3 channel opens per day
- Maximum 500,000 sats in channel opens per day
- No fee changes greater than 30% from current value
- No rebalances greater than 100,000 sats
- Always leave at least 200,000 sats on-chain reserve
```

## Customizing the Advisor

### Change Check Interval

Edit `~/.config/systemd/user/hive-advisor.timer`:

```ini
[Timer]
OnCalendar=*:0/15    # Every 15 minutes (default)
OnCalendar=*:0/30    # Every 30 minutes
OnCalendar=*:00      # Every hour
```

Reload after changes:

```bash
systemctl --user daemon-reload
```

### Modify Approval Criteria

Edit `production/strategy-prompts/approval_criteria.md`:

```markdown
## Channel Open Approval Criteria

**APPROVE if ALL conditions met:**
- Target has >10 active channels
- Target average fee <1000 ppm
- On-chain fees <50 sat/vB
- Would not exceed 5% allocation to peer

**REJECT if ANY condition:**
- Target has <5 channels
- On-chain fees >100 sat/vB
- Insufficient on-chain balance
```

### Adjust Safety Limits

Edit `production/strategy-prompts/system_prompt.md`:

```markdown
## Safety Constraints (NEVER EXCEED)

- Maximum 5 channel opens per day
- Maximum 1,000,000 sats in channel opens per day
- Always leave at least 500,000 sats on-chain reserve
```

### Add Custom Analysis

The advisor prompt in `run-advisor.sh` can be customized:

```bash
claude -p "Your custom prompt here..."
```

## Monitoring and Maintenance

### View Logs

```bash
# Live systemd logs
journalctl --user -u hive-advisor.service -f

# Log files
ls -la ~/cl-hive/production/logs/
tail -f ~/cl-hive/production/logs/advisor_*.log
```

### Check Timer Status

```bash
# Timer status
systemctl --user status hive-advisor.timer

# Next scheduled runs
systemctl --user list-timers | grep hive
```

### Manual Operations

```bash
# Trigger immediate run
systemctl --user start hive-advisor.service

# Pause automation
systemctl --user stop hive-advisor.timer

# Resume automation
systemctl --user start hive-advisor.timer

# Disable completely
systemctl --user disable hive-advisor.timer
```

### Log Rotation

Logs older than 7 days are automatically deleted by `run-advisor.sh`.

## Troubleshooting

### Connection Issues

| Error | Cause | Solution |
|-------|-------|----------|
| `curl: (7) Failed to connect` | Node unreachable | Check VPN, firewall, clnrest config |
| `405 Method Not Allowed` | Using GET instead of POST | clnrest requires POST requests |
| `401 Unauthorized` | Invalid or missing rune | Check rune in config matches node |
| `500 Internal Server Error` | Plugin error | Check CLN logs, plugin loaded |
| `Not permitted: too soon` | Rate limit hit | Increase `rate=` in rune |

### Rune Issues

```bash
# Test rune directly
curl -k -X POST \
  -H "Rune: YOUR_RUNE" \
  https://YOUR_NODE:3010/v1/hive-status

# Create new rune with correct syntax
lightning-cli createrune restrictions='[["method^hive-","method^getinfo","method^listfunds","method^listpeerchannels","method^setchannel","method^revenue-","method^feerates"],["rate=300"]]'
```

### Claude Code Issues

```bash
# Test Claude works
claude -p "Hello"

# Check API key
echo $ANTHROPIC_API_KEY

# Verbose mode
claude -p "Hello" --verbose
```

### MCP Server Issues

```bash
# Ensure venv activated
source ~/cl-hive/.venv/bin/activate

# Check dependencies
python3 -c "import mcp; import httpx; print('OK')"

# Test standalone
HIVE_NODES_CONFIG=production/nodes.production.json \
  python3 tools/mcp-hive-server.py --help
```

### Systemd Issues

```bash
# Check service status
systemctl --user status hive-advisor.service

# View detailed errors
journalctl --user -u hive-advisor.service -n 50

# Reload after config changes
systemctl --user daemon-reload

# Re-enable if disabled
systemctl --user enable hive-advisor.timer
systemctl --user start hive-advisor.timer
```

## Security Best Practices

1. **Rune Security**
   - Use minimal required permissions
   - Include rate limits
   - Store securely (production/ is gitignored)

2. **Network Security**
   - Use VPN for node access
   - Never expose clnrest to public internet
   - Consider TLS certificates

3. **API Cost Control**
   - `--max-budget-usd 0.50` limits per-run cost
   - 15-minute interval prevents excessive calls

4. **Governance Mode**
   - Keep node in `advisor` mode
   - All actions require AI approval
   - No autonomous fund movements

## Related Documentation

- [MCP Server Reference](MCP_SERVER.md) - Complete tool documentation
- [Quick Start Guide](../production.example/README.md) - Condensed setup steps
- [Governance Modes](../README.md#governance-modes) - Advisor vs autonomous
