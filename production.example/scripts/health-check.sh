#!/bin/bash
#
# Quick health check for the Hive AI Advisor setup
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROD_DIR="$(dirname "$SCRIPT_DIR")"
HIVE_DIR="$(dirname "$PROD_DIR")"

echo "=== Hive AI Advisor Health Check ==="
echo ""

# Check 1: Config files exist
echo "1. Checking configuration files..."
if [[ -f "${PROD_DIR}/nodes.production.json" ]]; then
    echo "   [OK] nodes.production.json exists"
    # Check if it's still using placeholder values
    if grep -q "YOUR_NODE_IP" "${PROD_DIR}/nodes.production.json"; then
        echo "   [WARN] nodes.production.json still has placeholder values - update before use!"
    fi
else
    echo "   [FAIL] nodes.production.json not found"
fi

if [[ -f "${PROD_DIR}/mcp-config.json" ]]; then
    echo "   [OK] mcp-config.json exists"
else
    echo "   [FAIL] mcp-config.json not found"
fi

# Check 2: Strategy prompts exist
echo ""
echo "2. Checking strategy prompts..."
for prompt in system_prompt.md approval_criteria.md; do
    if [[ -f "${PROD_DIR}/strategy-prompts/${prompt}" ]]; then
        echo "   [OK] ${prompt}"
    else
        echo "   [MISS] ${prompt} not found"
    fi
done

# Check 3: Claude CLI available
echo ""
echo "3. Checking Claude CLI..."
if command -v claude &> /dev/null; then
    echo "   [OK] claude command found"
    claude --version 2>/dev/null || echo "   (version check failed)"
else
    echo "   [FAIL] claude command not found - install Claude Code CLI"
fi

# Check 4: Python and MCP server
echo ""
echo "4. Checking MCP server..."
if [[ -f "${HIVE_DIR}/tools/mcp-hive-server.py" ]]; then
    echo "   [OK] mcp-hive-server.py exists"
else
    echo "   [FAIL] mcp-hive-server.py not found"
fi

# Check 5: Systemd timer status
echo ""
echo "5. Checking systemd timer..."
if systemctl --user is-active hive-advisor.timer &> /dev/null; then
    echo "   [OK] Timer is active"
    systemctl --user list-timers hive-advisor.timer --no-pager 2>/dev/null || true
else
    echo "   [INFO] Timer not running (run install.sh to set up)"
fi

# Check 6: Recent logs
echo ""
echo "6. Recent advisor logs..."
if ls "${PROD_DIR}/logs/advisor_"*.log &> /dev/null; then
    LATEST_LOG=$(ls -t "${PROD_DIR}/logs/advisor_"*.log | head -1)
    echo "   Latest log: $(basename "$LATEST_LOG")"
    echo "   Last 5 lines:"
    tail -5 "$LATEST_LOG" | sed 's/^/   /'
else
    echo "   [INFO] No logs yet (advisor hasn't run)"
fi

echo ""
echo "=== Health check complete ==="
