# Production AI Advisor Deployment

This folder contains templates for deploying the cl-hive AI Advisor on a production management server. The advisor runs automatically every 15 minutes, reviewing pending actions, monitoring financial health, and flagging problematic channels.

## Architecture

```
┌─────────────────────────┐
│   Management Server     │
│   (runs Claude Code)    │
│                         │
│  ┌───────────────────┐  │
│  │  systemd timer    │  │  ← Triggers every 15 min
│  │  (hive-advisor)   │  │
│  └─────────┬─────────┘  │
│            │            │
│  ┌─────────▼─────────┐  │
│  │  Claude Code      │  │  ← AI Decision Making
│  │  + MCP Server     │  │
│  └─────────┬─────────┘  │
└────────────┼────────────┘
             │ REST API (VPN)
             ▼
┌─────────────────────────┐
│   Production Node       │
│   (Lightning + Hive)    │
│                         │
│  - cl-hive plugin       │
│  - cl-revenue-ops       │
│  - clnrest API          │
└─────────────────────────┘
```

## Quick Start

### 1. Clone and Setup

```bash
# On your management server
git clone https://github.com/santyr/cl-hive.git
cd cl-hive

# Create production folder from template
cp -r production.example production

# Setup Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install httpx mcp pyln-client
```

### 2. Generate Commando Rune (on Lightning node)

**IMPORTANT**: All method patterns must be in ONE array for OR logic.

```bash
# On your production Lightning node
lightning-cli createrune restrictions='[["method^hive-","method^getinfo","method^listfunds","method^listpeerchannels","method^setchannel","method^revenue-","method^feerates"],["rate=300"]]'
```

Save the returned rune string.

### 3. Configure Node Connection

Edit `production/nodes.production.json`:

```json
{
  "mode": "rest",
  "nodes": [
    {
      "name": "mainnet",
      "rest_url": "https://YOUR_NODE_IP:3010",
      "rune": "YOUR_RUNE_STRING_HERE",
      "ca_cert": null
    }
  ]
}
```

### 4. Install Claude Code CLI

```bash
# Install Claude Code
npm install -g @anthropic-ai/claude-code

# Set API key
export ANTHROPIC_API_KEY="your-api-key"
# Or permanently:
mkdir -p ~/.anthropic
echo "your-api-key" > ~/.anthropic/api_key
chmod 600 ~/.anthropic/api_key
```

### 5. Test Connection

```bash
cd ~/cl-hive
source .venv/bin/activate

# Test REST API directly
curl -k -X POST \
  -H "Rune: YOUR_RUNE" \
  https://YOUR_NODE_IP:3010/v1/getinfo

# Test MCP server
HIVE_NODES_CONFIG=production/nodes.production.json \
  python3 tools/mcp-hive-server.py --help

# Test Claude with MCP
claude -p "Use hive_node_info for mainnet" \
  --mcp-config production/mcp-config.json \
  --allowedTools "mcp__hive__*"
```

### 6. Install Systemd Timer

```bash
# Create systemd user directory
mkdir -p ~/.config/systemd/user

# Copy service files (adjust path if cl-hive is not in ~/cl-hive)
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

cp ~/cl-hive/production/systemd/hive-advisor.timer ~/.config/systemd/user/

# Enable and start
systemctl --user daemon-reload
systemctl --user enable hive-advisor.timer
systemctl --user start hive-advisor.timer

# Verify
systemctl --user status hive-advisor.timer
```

## What the AI Advisor Does

Every 15 minutes, the advisor:

1. **Checks Pending Actions** - Reviews channel open proposals from the planner
2. **Approves/Rejects** - Makes decisions based on approval criteria
3. **Monitors Financial Health** - Checks revenue dashboard for issues
4. **Flags Problematic Channels** - Identifies zombies, bleeders, unprofitable channels
5. **Reports Summary** - Logs actions taken and any warnings

### What It Does NOT Do

- **Does not adjust fees** - cl-revenue-ops handles this automatically
- **Does not trigger rebalances** - cl-revenue-ops handles this automatically
- **Does not close channels** - Only flags for human review

## Files

| File | Purpose |
|------|---------|
| `nodes.production.json` | Lightning node REST API connection |
| `mcp-config.json` | MCP server configuration template |
| `strategy-prompts/system_prompt.md` | AI advisor personality, rules, safety limits |
| `strategy-prompts/approval_criteria.md` | Channel open approval/rejection criteria |
| `systemd/hive-advisor.timer` | 15-minute interval timer |
| `systemd/hive-advisor.service` | Oneshot service definition |
| `scripts/run-advisor.sh` | Main advisor runner (generates runtime config) |
| `scripts/install.sh` | Systemd installation helper |
| `scripts/health-check.sh` | Quick setup verification |

## Customization

### Change Check Interval

Edit `~/.config/systemd/user/hive-advisor.timer`:

```ini
[Timer]
# Every 15 minutes (default)
OnCalendar=*:0/15

# Every 30 minutes
OnCalendar=*:0/30

# Every hour
OnCalendar=*:00
```

Then reload: `systemctl --user daemon-reload`

### Adjust Safety Limits

Edit `production/strategy-prompts/system_prompt.md`:

```markdown
## Safety Constraints (NEVER EXCEED)

- Maximum 3 channel opens per day
- Maximum 500,000 sats in channel opens per day
- Always leave at least 200,000 sats on-chain reserve
```

### Customize Approval Criteria

Edit `production/strategy-prompts/approval_criteria.md` to change what channel opens get approved.

## Monitoring

```bash
# View timer status
systemctl --user status hive-advisor.timer

# List upcoming runs
systemctl --user list-timers | grep hive

# Watch live logs
journalctl --user -u hive-advisor.service -f

# View log files
ls -la ~/cl-hive/production/logs/
tail -f ~/cl-hive/production/logs/advisor_*.log

# Manual trigger
systemctl --user start hive-advisor.service

# Pause automation
systemctl --user stop hive-advisor.timer

# Resume automation
systemctl --user start hive-advisor.timer
```

## Troubleshooting

### Timer Not Running

```bash
systemctl --user is-enabled hive-advisor.timer
systemctl --user daemon-reload
systemctl --user enable hive-advisor.timer
systemctl --user start hive-advisor.timer
```

### REST API Connection Errors

```bash
# Test connection (use POST, not GET)
curl -k -X POST \
  -H "Rune: YOUR_RUNE" \
  https://YOUR_NODE_IP:3010/v1/getinfo

# Common issues:
# - Wrong port (check clnrest-port in CLN config)
# - Rune syntax wrong (all methods must be in ONE array)
# - Rate limit hit (increase rate= in rune)
```

### Claude Errors

```bash
# Test Claude directly
claude -p "Hello"

# Check API key
echo $ANTHROPIC_API_KEY
cat ~/.anthropic/api_key

# Test with verbose output
claude -p "Hello" --verbose
```

### MCP Server Errors

```bash
# Ensure venv is activated
source ~/cl-hive/.venv/bin/activate

# Test MCP server standalone
HIVE_NODES_CONFIG=production/nodes.production.json \
  python3 tools/mcp-hive-server.py --help

# Check for import errors
python3 -c "import mcp; import httpx; print('OK')"
```

### "Method not permitted" Errors

Your rune doesn't have permission for the method. Create a new rune with correct permissions:

```bash
lightning-cli createrune restrictions='[["method^hive-","method^getinfo","method^listfunds","method^listpeerchannels","method^setchannel","method^revenue-","method^feerates"],["rate=300"]]'
```

## Security Notes

- The `production/` folder is gitignored - it contains your rune (secret)
- Keep your commando rune secure - it grants API access
- Use VPN for remote node access
- Consider TLS certificates for REST API (`ca_cert` in nodes.json)
- The advisor runs with `--max-budget-usd 0.50` per run to limit API costs

## Related Documentation

- [MCP Server Reference](../docs/MCP_SERVER.md) - Full tool documentation
- [AI Advisor Setup Guide](../docs/AI_ADVISOR_SETUP.md) - Detailed setup walkthrough
- [Governance Modes](../README.md#governance-modes) - Advisor vs autonomous mode
