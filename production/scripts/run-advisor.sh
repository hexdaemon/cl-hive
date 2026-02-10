#!/bin/bash
#
# Hive Proactive AI Advisor Runner Script
# Runs Claude Code with MCP server to execute the proactive advisor cycle
# The advisor analyzes state, tracks goals, scans opportunities, and learns from outcomes
#
set -euo pipefail

# Determine directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROD_DIR="$(dirname "$SCRIPT_DIR")"
HIVE_DIR="$(dirname "$PROD_DIR")"
LOG_DIR="${PROD_DIR}/logs"
DATE=$(date +%Y%m%d)

# Ensure log directory exists
mkdir -p "$LOG_DIR"

# Use daily log file (appends throughout the day)
LOG_FILE="${LOG_DIR}/advisor_${DATE}.log"

# Change to hive directory
cd "$HIVE_DIR"

# Activate virtual environment if it exists
if [[ -f "${HIVE_DIR}/.venv/bin/activate" ]]; then
    source "${HIVE_DIR}/.venv/bin/activate"
fi

echo "" >> "$LOG_FILE"
echo "================================================================================" >> "$LOG_FILE"
echo "=== Proactive AI Advisor Run: $(date) ===" | tee -a "$LOG_FILE"
echo "================================================================================" >> "$LOG_FILE"

# Load system prompt from file
if [[ -f "${PROD_DIR}/strategy-prompts/system_prompt.md" ]]; then
    SYSTEM_PROMPT=$(cat "${PROD_DIR}/strategy-prompts/system_prompt.md")
else
    echo "WARNING: System prompt file not found, using default" | tee -a "$LOG_FILE"
    SYSTEM_PROMPT="You are an AI advisor for a Lightning node. Run the proactive advisor cycle and summarize results."
fi

# Advisor database location
ADVISOR_DB="${PROD_DIR}/data/advisor.db"
mkdir -p "$(dirname "$ADVISOR_DB")"

# Generate MCP config with absolute paths
MCP_CONFIG_TMP="${PROD_DIR}/.mcp-config-runtime.json"
cat > "$MCP_CONFIG_TMP" << MCPEOF
{
  "mcpServers": {
    "hive": {
      "command": "${HIVE_DIR}/.venv/bin/python",
      "args": ["${HIVE_DIR}/tools/mcp-hive-server.py"],
      "env": {
        "HIVE_NODES_CONFIG": "${PROD_DIR}/nodes.production.json",
        "HIVE_STRATEGY_DIR": "${PROD_DIR}/strategy-prompts",
        "ADVISOR_DB_PATH": "${ADVISOR_DB}",
        "ADVISOR_LOG_DIR": "${LOG_DIR}",
        "HIVE_ALLOW_INSECURE_TLS": "true",
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
MCPEOF

# Increase Node.js heap size to handle large MCP responses
export NODE_OPTIONS="--max-old-space-size=2048"

# Run Claude with MCP server
# The proactive advisor runs a complete 9-phase optimization cycle:
# 1) Record snapshot 2) Analyze state 3) Check goals 4) Scan opportunities
# 5) Score with learning 6) Auto-execute safe actions 7) Queue risky actions
# 8) Measure outcomes 9) Plan next cycle
claude -p "Run the proactive advisor cycle on ALL nodes using advisor_run_cycle_all. After the cycle completes:

## AUTO-PROCESS CHANNEL OPENS
For each pending channel_open action on each node, automatically approve or reject based on these criteria:

APPROVE only if ALL conditions met:
- Target node has >15 active channels (strong connectivity)
- Target's median fee is <500 ppm (quality routing partner)
- Current on-chain fees are <20 sat/vB
- Channel size is 2-10M sats
- Node has <30 total channels AND <40% underwater channels
- Opening maintains 500k sats on-chain reserve
- Not a duplicate channel to existing peer

REJECT if ANY condition applies:
- Target has <10 channels (insufficient connectivity)
- On-chain fees >30 sat/vB (wait for lower fees)
- Node already has >30 channels (focus on profitability)
- Node has >40% underwater channels (fix existing first)
- Amount below 1M sats or above 10M sats
- Would create duplicate channel
- Insufficient on-chain balance for reserve

Use hive_approve_action or hive_reject_action for each pending channel_open.

## REPORT SECTIONS
After processing actions, provide a report with these sections:

### FLEET HEALTH (use advisor_get_trends and hive_status)
- Total nodes and their status (online/offline)
- Fleet-wide capacity and revenue trends (7-day)
- Hive membership summary (members/neophytes)
- Any internal competition or coordination issues

### PER-NODE SUMMARIES (for each node)
1) Node state (capacity, channels, ROC%, underwater%)
2) Goals progress and strategy adjustments needed
3) Opportunities found by type and actions taken/queued
4) Next cycle priorities

### ACTIONS TAKEN
- List channel opens approved with reasoning
- List channel opens rejected with reasoning" \
    --mcp-config "$MCP_CONFIG_TMP" \
    --system-prompt "$SYSTEM_PROMPT" \
    --model sonnet \
    --max-budget-usd 1.00 \
    --allowedTools "mcp__hive__*" \
    --output-format text \
    2>&1 | tee -a "$LOG_FILE"

echo "=== Run completed: $(date) ===" | tee -a "$LOG_FILE"

# Cleanup old logs (keep last 7 days)
find "$LOG_DIR" -name "advisor_*.log" -mtime +7 -delete 2>/dev/null || true

exit 0
