#!/bin/bash
# hive-watchdog.sh - Monitor and auto-restart hung cl-hive plugins
# Run via cron: */15 * * * * /home/sat/bin/cl-hive/tools/hive-watchdog.sh

set -euo pipefail

NODES_CONFIG="${HIVE_NODES_CONFIG:-/home/sat/bin/cl-hive/production/nodes.production.json}"
LOG_FILE="/tmp/hive-watchdog.log"
TIMEOUT=10

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

check_and_restart_plugin() {
    local node_name="$1"
    local rest_url="$2"
    local rune="$3"
    local plugin_path="$4"
    
    # Test hive-status with timeout
    response=$(timeout "$TIMEOUT" curl -sk -X POST \
        -H "Rune: $rune" \
        -H "Content-Type: application/json" \
        -d '{}' \
        "${rest_url}/v1/hive-status" 2>&1) || response="TIMEOUT"
    
    if [[ "$response" == "TIMEOUT" ]] || [[ "$response" == *"error"* && "$response" != *"governance_mode"* ]]; then
        log "WARNING: $node_name hive-status failed, restarting plugin..."
        
        # Stop plugin
        timeout 15 curl -sk -X POST \
            -H "Rune: $rune" \
            -H "Content-Type: application/json" \
            -d "{\"subcommand\": \"stop\", \"plugin\": \"$plugin_path\"}" \
            "${rest_url}/v1/plugin" 2>/dev/null || true
        
        sleep 2
        
        # Start plugin
        restart_result=$(timeout 15 curl -sk -X POST \
            -H "Rune: $rune" \
            -H "Content-Type: application/json" \
            -d "{\"subcommand\": \"start\", \"plugin\": \"$plugin_path\"}" \
            "${rest_url}/v1/plugin" 2>&1) || restart_result="FAILED"
        
        if [[ "$restart_result" == *"active\":true"* ]]; then
            log "OK: $node_name plugin restarted successfully"
        else
            log "ERROR: $node_name plugin restart failed: $restart_result"
        fi
    else
        log "OK: $node_name healthy"
    fi
}

# Main
log "=== Hive Watchdog Check ==="

if [[ ! -f "$NODES_CONFIG" ]]; then
    log "ERROR: Config not found: $NODES_CONFIG"
    exit 1
fi

# Parse nodes config and check each
# Note: Adjust plugin paths per node as needed
jq -r '.nodes[] | "\(.name)|\(.rest_url)|\(.rune)"' "$NODES_CONFIG" | while IFS='|' read -r name url rune; do
    # Determine plugin path based on node
    if [[ "$name" == "hive-nexus-01" ]]; then
        plugin_path="/data/lightningd/plugins/cl-hive/cl-hive.py"
    else
        plugin_path="/opt/cl-hive/cl-hive.py"
    fi
    
    check_and_restart_plugin "$name" "$url" "$rune" "$plugin_path"
done

log "=== Watchdog Complete ==="
