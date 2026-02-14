# Production AI Advisor Deployment

This folder contains templates for deploying the cl-hive AI Advisor on a production management server.

## Quick Start

### 1. Copy to Production

```bash
# On your management server
git clone https://github.com/santyr/cl-hive.git
cd cl-hive
cp -r production.example production
```

### 2. Configure Node Connection

Edit `production/nodes.production.json`:

```json
{
  "mode": "rest",
  "nodes": [
    {
      "name": "mainnet",
      "rest_url": "https://YOUR_NODE_IP:3001",
      "rune": "YOUR_COMMANDO_RUNE",
      "ca_cert": null
    }
  ]
}
```

**Generate a Commando Rune** (on your Lightning node):

```bash
lightning-cli createrune restrictions='[
  ["method^list", "method^get", "method=hive-*", "method=revenue-*",
   "method=setchannel", "method=fundchannel"],
  ["rate=60"]
]'
```

### 3. Install Claude Code CLI

```bash
# Install Claude Code
npm install -g @anthropic-ai/claude-code

# Set API key
mkdir -p ~/.anthropic
echo "YOUR_ANTHROPIC_API_KEY" > ~/.anthropic/api_key
chmod 600 ~/.anthropic/api_key
```

### 4. Test Connection

```bash
cd ~/cl-hive
./production/scripts/health-check.sh

# Manual test run
claude -p --mcp-config production/mcp-config.json "Use hive_status to check node health"
```

### 5. Install Systemd Timer

```bash
./production/scripts/install.sh
```

## Files

| File | Purpose |
|------|---------|
| `nodes.production.json` | Lightning node REST API connection |
| `mcp-config.json` | MCP server configuration |
| `strategy-prompts/system_prompt.md` | AI advisor personality and rules |
| `strategy-prompts/approval_criteria.md` | Decision criteria for actions |
| `systemd/hive-advisor.timer` | 15-minute interval timer |
| `systemd/hive-advisor.service` | Oneshot service definition |
| `scripts/run-advisor.sh` | Main advisor runner script |
| `scripts/install.sh` | Systemd installation script |
| `scripts/health-check.sh` | Quick setup verification |

## Customization

### Change Check Interval

Edit `systemd/hive-advisor.timer`:

```ini
# Every 15 minutes (default)
OnCalendar=*:0/15

# Every 30 minutes
OnCalendar=*:0/30

# Every hour
OnCalendar=*:00
```

### Adjust Safety Limits

Edit `strategy-prompts/system_prompt.md` to change:
- Maximum channel opens per day
- Maximum sats in channel opens
- Fee change limits
- Rebalance limits

### Add Custom Strategy

Create new files in `strategy-prompts/` and reference them in the approval criteria.

## Monitoring

```bash
# View timer status
systemctl --user status hive-advisor.timer

# List upcoming runs
systemctl --user list-timers

# Watch live logs
journalctl --user -u hive-advisor.service -f

# View recent logs
ls -la production/logs/

# Manual trigger
systemctl --user start hive-advisor.service
```

## Troubleshooting

### Timer not running

```bash
# Check if timer is enabled
systemctl --user is-enabled hive-advisor.timer

# Re-run installation
./production/scripts/install.sh
```

### Connection errors

```bash
# Test REST API directly
curl -k -H "Rune: YOUR_RUNE" https://YOUR_NODE:3001/v1/getinfo

# Check MCP server
python3 tools/mcp-hive-server.py --help
```

### Claude errors

```bash
# Check API key
cat ~/.anthropic/api_key

# Test Claude directly
claude -p "Hello"
```

## Security Notes

- The `production/` folder is gitignored - it contains your rune (secret)
- Keep your commando rune secure
- Use restrictive rune permissions (see rune generation above)
- Consider TLS certificates for REST API (`ca_cert` in nodes.json)
