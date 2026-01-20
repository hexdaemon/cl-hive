#!/bin/bash
# =============================================================================
# cl-hive Rollback Script
# =============================================================================
# Rolls back to a previous version using a backup.
#
# Usage:
#   ./rollback.sh                         # Rollback to latest backup
#   ./rollback.sh /path/to/backup         # Rollback to specific backup
#   ./rollback.sh --list                  # List available backups
# =============================================================================

set -euo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# Load environment
if [[ -f "$DOCKER_DIR/.env" ]]; then
    set -a
    source "$DOCKER_DIR/.env"
    set +a
fi

# Configuration
BACKUP_LOCATION="${BACKUP_LOCATION:-/backups}"
CONTAINER_NAME="${CONTAINER_NAME:-cl-hive-node}"
NETWORK="${NETWORK:-bitcoin}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# Logging
log() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "\n${CYAN}==> $1${NC}"; }

# =============================================================================
# Functions
# =============================================================================

list_backups() {
    log_step "Available backups in $BACKUP_LOCATION"
    echo ""

    local count=0
    while IFS= read -r backup_dir; do
        if [[ -f "$backup_dir/manifest.json" ]]; then
            local timestamp
            timestamp=$(basename "$backup_dir" | sed 's/backup_//' | sed 's/_/ /')

            local network
            network=$(grep -o '"network": "[^"]*"' "$backup_dir/manifest.json" 2>/dev/null | cut -d'"' -f4 || echo "unknown")

            local encrypted="no"
            if [[ -f "$backup_dir/hsm/hsm_secret.gpg" ]]; then
                encrypted="yes"
            fi

            local size
            size=$(du -sh "$backup_dir" | cut -f1)

            printf "  %-40s  network: %-8s  encrypted: %-3s  size: %s\n" \
                "$(basename "$backup_dir")" "$network" "$encrypted" "$size"
            ((count++))
        fi
    done < <(find "$BACKUP_LOCATION" -maxdepth 1 -type d -name "backup_*" | sort -r)

    echo ""

    if [[ $count -eq 0 ]]; then
        log_warning "No backups found in $BACKUP_LOCATION"
        return 1
    fi

    log "Total: $count backup(s)"
    echo ""
    echo "To rollback:"
    echo "  ./rollback.sh /backups/backup_YYYYMMDD_HHMMSS"
}

get_latest_backup() {
    local latest
    latest=$(find "$BACKUP_LOCATION" -maxdepth 1 -type d -name "backup_*" | sort -r | head -1)

    if [[ -z "$latest" ]]; then
        log_error "No backups found in $BACKUP_LOCATION"
        exit 1
    fi

    echo "$latest"
}

confirm_rollback() {
    local backup_path="$1"

    echo ""
    echo -e "${RED}${BOLD}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}${BOLD}                    ROLLBACK WARNING                             ${NC}"
    echo -e "${RED}${BOLD}════════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "This will:"
    echo "  1. STOP the current Lightning node"
    echo "  2. RESTORE data from backup"
    echo "  3. Restart with restored data"
    echo ""
    echo "Backup: $backup_path"

    if [[ -f "$backup_path/manifest.json" ]]; then
        local backup_time
        backup_time=$(grep -o '"timestamp": "[^"]*"' "$backup_path/manifest.json" | cut -d'"' -f4)
        echo "Backup time: $backup_time"
    fi

    echo ""
    echo -e "${YELLOW}WARNING: Any channels opened or modified since this backup${NC}"
    echo -e "${YELLOW}         may be FORCE-CLOSED or have incorrect state!${NC}"
    echo ""

    read -p "Type 'ROLLBACK' to confirm: " confirmation

    if [[ "$confirmation" != "ROLLBACK" ]]; then
        log "Rollback cancelled"
        exit 0
    fi
}

perform_rollback() {
    local backup_path="$1"

    log_step "Starting rollback to: $backup_path"

    # Verify backup
    if [[ ! -f "$backup_path/manifest.json" ]]; then
        log_error "Invalid backup - missing manifest.json"
        exit 1
    fi

    # Use restore script
    if [[ -x "$SCRIPT_DIR/restore.sh" ]]; then
        "$SCRIPT_DIR/restore.sh" --force "$backup_path"
    else
        log_error "restore.sh not found or not executable"
        exit 1
    fi
}

health_check() {
    log_step "Verifying rollback..."

    local retries=5
    while [[ $retries -gt 0 ]]; do
        sleep 10

        # Check container running
        if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
            log_warning "Container not running yet..."
            ((retries--))
            continue
        fi

        # Check Lightning RPC
        if docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" getinfo &>/dev/null; then
            log_success "Node is responding"
            return 0
        fi

        log_warning "Node not responding yet..."
        ((retries--))
    done

    log_error "Health check failed after rollback"
    return 1
}

print_usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS] [BACKUP_PATH]

Options:
    --list      List available backups
    --latest    Rollback to most recent backup
    --force     Skip confirmation prompt
    --help      Show this help message

Arguments:
    BACKUP_PATH  Path to backup directory (e.g., /backups/backup_20240101_120000)

Examples:
    ./rollback.sh --list                      # List backups
    ./rollback.sh --latest                    # Rollback to latest backup
    ./rollback.sh /backups/backup_20240101_120000  # Specific backup
EOF
}

# =============================================================================
# Main
# =============================================================================

main() {
    local backup_path=""
    local force=false
    local list_only=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --list)
                list_only=true
                shift
                ;;
            --latest)
                backup_path=$(get_latest_backup)
                shift
                ;;
            --force)
                force=true
                shift
                ;;
            --help|-h)
                print_usage
                exit 0
                ;;
            -*)
                log_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
            *)
                backup_path="$1"
                shift
                ;;
        esac
    done

    # Header
    echo ""
    echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}${BOLD}               cl-hive Rollback Script                         ${NC}"
    echo -e "${RED}═══════════════════════════════════════════════════════════════${NC}"
    echo ""

    # List mode
    if [[ "$list_only" == "true" ]]; then
        list_backups
        exit $?
    fi

    # Check backup path
    if [[ -z "$backup_path" ]]; then
        log_error "No backup specified"
        echo ""
        echo "Use --list to see available backups, or specify a path:"
        echo "  ./rollback.sh /backups/backup_20240101_120000"
        echo "  ./rollback.sh --latest"
        exit 1
    fi

    # Verify backup exists
    if [[ ! -d "$backup_path" ]]; then
        log_error "Backup not found: $backup_path"
        exit 1
    fi

    # Confirmation
    if [[ "$force" != "true" ]]; then
        confirm_rollback "$backup_path"
    fi

    # Perform rollback
    perform_rollback "$backup_path"

    # Verify
    if health_check; then
        echo ""
        echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
        echo -e "${GREEN}${BOLD}                 Rollback Successful!                          ${NC}"
        echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
        echo ""
        echo "  Verify node state:"
        echo "    docker-compose exec cln lightning-cli getinfo"
        echo "    docker-compose exec cln lightning-cli listpeerchannels"
        echo ""
    else
        log_error "Rollback may have failed - manual verification required"
        exit 1
    fi
}

main "$@"
