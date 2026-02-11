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
claude -p "Run the complete advisor workflow as defined in the system prompt:

1. **Quick Assessment**: fleet_health_summary, membership_dashboard, routing_intelligence_health
2. **Process Pending**: process_all_pending on all nodes (preview with dry_run=true, then execute)
3. **Execute Opportunities**: execute_safe_opportunities on all nodes
4. **Remediate Stagnant**: Check stagnant_channels, apply remediate_stagnant where appropriate
5. **Health Analysis**: critical_velocity, connectivity_recommendations, advisor_get_trends
6. **Generate Report**: Follow the output format in system prompt

Run on ALL fleet nodes. Use the enhanced automation tools - they handle criteria evaluation automatically." \
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
