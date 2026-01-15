#!/bin/bash
#
# Hive AI Advisor Runner Script
# Runs Claude Code with MCP server to review pending actions
#
set -euo pipefail

# Determine directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROD_DIR="$(dirname "$SCRIPT_DIR")"
HIVE_DIR="$(dirname "$PROD_DIR")"
LOG_DIR="${PROD_DIR}/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Ensure log directory exists
mkdir -p "$LOG_DIR"

LOG_FILE="${LOG_DIR}/advisor_${TIMESTAMP}.log"

# Change to hive directory (MCP server expects relative paths)
cd "$HIVE_DIR"

echo "=== Hive AI Advisor Run: $(date) ===" | tee "$LOG_FILE"

# Load system prompt from file
if [[ -f "${PROD_DIR}/strategy-prompts/system_prompt.md" ]]; then
    SYSTEM_PROMPT=$(cat "${PROD_DIR}/strategy-prompts/system_prompt.md")
else
    echo "WARNING: System prompt file not found, using default" | tee -a "$LOG_FILE"
    SYSTEM_PROMPT="You are an AI advisor for a Lightning node. Review pending actions and make decisions."
fi

# Run Claude with MCP server
# --allowedTools restricts to only hive/revenue tools for safety
claude -p \
    --mcp-config "${PROD_DIR}/mcp-config.json" \
    --system-prompt "$SYSTEM_PROMPT" \
    --model sonnet \
    --max-budget-usd 0.50 \
    --allowedTools "mcp__hive__*" \
    "Review all pending actions using hive_pending_actions. For each action, evaluate against the approval criteria and either approve or reject with clear reasoning. Then check revenue_dashboard for fleet health and report any issues." \
    2>&1 | tee -a "$LOG_FILE"

echo "=== Run completed: $(date) ===" | tee -a "$LOG_FILE"

# Cleanup old logs (keep last 7 days)
find "$LOG_DIR" -name "advisor_*.log" -mtime +7 -delete 2>/dev/null || true

exit 0
