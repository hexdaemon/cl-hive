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
# The advisor uses enhanced automation tools for efficient fleet management

# Build the prompt - pipe via stdin to avoid all shell escaping issues
# NOTE: System prompt is embedded in user prompt to avoid shell escaping issues with --append-system-prompt
ADVISOR_PROMPT_FILE=$(mktemp)
cat > "$ADVISOR_PROMPT_FILE" << 'PROMPTEOF'
You are the AI Advisor for the Lightning Hive fleet (hive-nexus-01 and hive-nexus-02).

## CRITICAL RULES (MANDATORY)
- Call each tool FIRST, then report its EXACT output values
- Copy numbers exactly - do not round, estimate, or paraphrase
- If a tool fails, say "Tool call failed" - never fabricate data
- Volume=0 with Revenue>0 is IMPOSSIBLE - verify data consistency

## WORKFLOW
1. Quick Assessment: Call fleet_health_summary, membership_dashboard, routing_intelligence_health (BOTH nodes)
2. Process Pending: process_all_pending(dry_run=true), then process_all_pending(dry_run=false)  
3. Health Analysis: critical_velocity, stagnant_channels, advisor_get_trends (BOTH nodes)
4. Generate Report: Use EXACT values from tool outputs

## FORBIDDEN ACTIONS
- Do NOT call execute_safe_opportunities
- Do NOT call remediate_stagnant with dry_run=false
- Do NOT execute any fee changes
- Report recommendations for HUMAN REVIEW only

## AUTO-APPROVE CRITERIA
- Channel opens: Target has >=15 channels, median fee <500ppm, on-chain <20 sat/vB, size 2-10M sats
- Fee changes: Change <=25% from current, new fee 50-1500 ppm range
- Rebalances: Amount <=500k sats, EV-positive

## AUTO-REJECT CRITERIA  
- Channel opens: Target <10 channels, on-chain >30 sat/vB, amount <1M or >10M sats
- Any action on "avoid" rated peers

## ESCALATE TO HUMAN
- Channel open >5M sats
- Conflicting signals
- Repeated failures (3+ similar rejections)
- Any close/splice operation

Run the complete advisor workflow now. Call tools on BOTH nodes.
PROMPTEOF

# Pipe prompt via stdin - avoids all command-line escaping issues
cat "$ADVISOR_PROMPT_FILE" | claude -p \
    --mcp-config "$MCP_CONFIG_TMP" \
    --model sonnet \
    --allowedTools "mcp__hive__*" \
    --output-format text \
    2>&1 | tee -a "$LOG_FILE"

rm -f "$ADVISOR_PROMPT_FILE"

echo "=== Run completed: $(date) ===" | tee -a "$LOG_FILE"

# Cleanup old logs (keep last 7 days)
find "$LOG_DIR" -name "advisor_*.log" -mtime +7 -delete 2>/dev/null || true

# Extract summary from the run and send to Hex via OpenClaw
# Get the last run's output (between the last two "===" markers)
SUMMARY=$(tail -200 "$LOG_FILE" | grep -v "^===" | head -100 | tr '\n' ' ' | cut -c1-2000)

# Write summary to a file for Hex to pick up on next heartbeat
SUMMARY_FILE="${PROD_DIR}/data/last-advisor-summary.txt"
{
    echo "=== Advisor Run $(date) ==="
    tail -200 "$LOG_FILE" | grep -v "^===" | head -100
} > "$SUMMARY_FILE"

# Also send wake event to OpenClaw main session via gateway API
GATEWAY_PORT=18789
WAKE_TEXT="Hive Advisor cycle completed at $(date). Review summary at: ${SUMMARY_FILE}"

curl -s -X POST "http://127.0.0.1:${GATEWAY_PORT}/api/cron/wake" \
    -H "Content-Type: application/json" \
    -d "{\"text\": \"${WAKE_TEXT}\", \"mode\": \"now\"}" \
    2>/dev/null || true

exit 0
