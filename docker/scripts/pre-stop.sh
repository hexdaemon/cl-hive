#!/bin/bash
# =============================================================================
# cl-hive Pre-Stop Hook
# =============================================================================
# Graceful shutdown script for Lightning node.
# Called before container stop to ensure clean shutdown.
#
# This script:
#   1. Signals the node to stop accepting new HTLCs
#   2. Waits for pending operations to complete
#   3. Flushes database to disk
#   4. Signals supervisor for clean shutdown
#
# Usage: Called automatically by Docker/supervisord, or manually:
#   ./pre-stop.sh
# =============================================================================

set -euo pipefail

# Configuration
NETWORK="${NETWORK:-bitcoin}"
LIGHTNING_DIR="${LIGHTNING_DIR:-/data/lightning/$NETWORK}"
MAX_WAIT_SECONDS="${MAX_WAIT_SECONDS:-90}"
HTLC_WAIT_SECONDS="${HTLC_WAIT_SECONDS:-30}"

# Logging
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [pre-stop] $1"; }
log_warn() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [pre-stop] WARNING: $1"; }
log_error() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] [pre-stop] ERROR: $1" >&2; }

# Lightning CLI wrapper
lcli() {
    lightning-cli --lightning-dir="$LIGHTNING_DIR" "$@" 2>/dev/null
}

# =============================================================================
# Shutdown Steps
# =============================================================================

check_node_running() {
    if ! lcli getinfo >/dev/null; then
        log "Node is not running or not responding"
        return 1
    fi
    return 0
}

get_pending_htlcs() {
    local count
    count=$(lcli listpeerchannels 2>/dev/null | \
        grep -c '"state": "HTLC_' || echo "0")
    echo "$count"
}

wait_for_htlcs() {
    log "Waiting for pending HTLCs to resolve..."

    local start_time
    start_time=$(date +%s)

    while true; do
        local pending
        pending=$(get_pending_htlcs)

        if [[ "$pending" -eq 0 ]]; then
            log "All HTLCs resolved"
            return 0
        fi

        local elapsed=$(( $(date +%s) - start_time ))
        if [[ $elapsed -gt $HTLC_WAIT_SECONDS ]]; then
            log_warn "HTLC timeout reached with $pending pending HTLCs"
            return 0  # Continue shutdown anyway
        fi

        log "Waiting for $pending pending HTLCs... (${elapsed}s/${HTLC_WAIT_SECONDS}s)"
        sleep 2
    done
}

disable_new_htlcs() {
    log "Disabling new HTLC acceptance..."

    # Set all channels to not accepting HTLCs (if supported)
    # This is a best-effort operation
    local channels
    channels=$(lcli listpeerchannels 2>/dev/null | grep -o '"short_channel_id": "[^"]*"' | cut -d'"' -f4 || true)

    for scid in $channels; do
        # Try to disable HTLC acceptance - this may not be supported in all versions
        lcli setchannel "$scid" null null null null false 2>/dev/null || true
    done

    log "HTLC acceptance disabled for outbound channels"
}

flush_database() {
    log "Flushing database to disk..."

    # Force WAL checkpoint for SQLite
    if command -v sqlite3 &>/dev/null; then
        sqlite3 "$LIGHTNING_DIR/lightningd.sqlite3" "PRAGMA wal_checkpoint(TRUNCATE);" 2>/dev/null || true
        sqlite3 "$LIGHTNING_DIR/cl-hive.db" "PRAGMA wal_checkpoint(TRUNCATE);" 2>/dev/null || true
    fi

    # Sync filesystem
    sync

    log "Database flushed"
}

notify_hive() {
    log "Notifying hive of shutdown..."

    # Send shutdown notification to hive peers
    lcli hive-notify-shutdown 2>/dev/null || true
}

stop_lightningd() {
    log "Sending stop command to lightningd..."

    # Use lightning-cli stop for graceful shutdown
    if lcli stop 2>/dev/null; then
        log "Stop command sent successfully"
    else
        log_warn "Stop command failed - node may have already stopped"
    fi

    # Wait for process to exit
    local start_time
    start_time=$(date +%s)

    while pgrep -x lightningd >/dev/null; do
        local elapsed=$(( $(date +%s) - start_time ))
        if [[ $elapsed -gt $MAX_WAIT_SECONDS ]]; then
            log_warn "Shutdown timeout reached - sending SIGTERM"
            pkill -TERM lightningd 2>/dev/null || true
            sleep 5

            if pgrep -x lightningd >/dev/null; then
                log_error "Process still running - sending SIGKILL"
                pkill -KILL lightningd 2>/dev/null || true
            fi
            break
        fi

        log "Waiting for lightningd to stop... (${elapsed}s/${MAX_WAIT_SECONDS}s)"
        sleep 2
    done

    log "lightningd stopped"
}

# =============================================================================
# Main
# =============================================================================

main() {
    log "Starting graceful shutdown sequence..."
    log "Network: $NETWORK"
    log "Lightning dir: $LIGHTNING_DIR"

    # Check if node is running
    if ! check_node_running; then
        log "Node not running - nothing to do"
        exit 0
    fi

    # Get node info for logging
    local node_id
    node_id=$(lcli getinfo | grep -o '"id": "[^"]*"' | cut -d'"' -f4 | head -c 20)
    log "Shutting down node: ${node_id}..."

    # Step 1: Disable new HTLCs
    disable_new_htlcs

    # Step 2: Wait for pending HTLCs
    wait_for_htlcs

    # Step 3: Notify hive members
    notify_hive

    # Step 4: Flush database
    flush_database

    # Step 5: Stop lightningd gracefully
    stop_lightningd

    log "Graceful shutdown complete"
}

# Handle signals
trap 'log "Received signal - accelerating shutdown"; MAX_WAIT_SECONDS=10' SIGTERM SIGINT

main "$@"
