#!/bin/bash
# =============================================================================
# cl-hive Backup Restore Script
# =============================================================================
# Restores Lightning node data from encrypted backups.
#
# IMPORTANT: This script will STOP the node and REPLACE existing data.
# Make sure you understand the implications before running.
#
# Usage:
#   ./restore.sh /path/to/backup_20240101_120000
#   ./restore.sh --latest                    # Restore most recent backup
#   ./restore.sh --hsm-only /path/to/backup  # Restore only hsm_secret
#   ./restore.sh --list                      # List available backups
#
# Configuration (via .env or environment):
#   BACKUP_LOCATION      - Backup source (default: /backups)
#   GPG_KEY_ID           - GPG key for decryption
#   NETWORK              - Bitcoin network (default: bitcoin)
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
GPG_KEY_ID="${GPG_KEY_ID:-}"
NETWORK="${NETWORK:-bitcoin}"
CONTAINER_NAME="${CONTAINER_NAME:-cl-hive-node}"

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

# =============================================================================
# Functions
# =============================================================================

list_backups() {
    log "Available backups in $BACKUP_LOCATION:"
    echo ""

    local count=0
    while IFS= read -r backup_dir; do
        if [[ -f "$backup_dir/manifest.json" ]]; then
            local timestamp
            timestamp=$(basename "$backup_dir" | sed 's/backup_//')

            local encrypted="no"
            if [[ -f "$backup_dir/hsm/hsm_secret.gpg" ]]; then
                encrypted="yes"
            fi

            local size
            size=$(du -sh "$backup_dir" | cut -f1)

            printf "  %s  [encrypted: %s, size: %s]\n" "$backup_dir" "$encrypted" "$size"
            ((count++))
        fi
    done < <(find "$BACKUP_LOCATION" -maxdepth 1 -type d -name "backup_*" | sort -r)

    if [[ $count -eq 0 ]]; then
        log_warning "No backups found"
        return 1
    fi

    echo ""
    log "Total: $count backup(s)"
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

verify_backup() {
    local backup_path="$1"

    if [[ ! -d "$backup_path" ]]; then
        log_error "Backup directory not found: $backup_path"
        return 1
    fi

    if [[ ! -f "$backup_path/manifest.json" ]]; then
        log_error "Invalid backup - missing manifest.json"
        return 1
    fi

    # Check critical files
    if [[ ! -f "$backup_path/hsm/hsm_secret.gpg" ]] && [[ ! -f "$backup_path/hsm/hsm_secret" ]]; then
        log_error "Invalid backup - missing hsm_secret"
        return 1
    fi

    log_success "Backup verified: $backup_path"
    return 0
}

stop_container() {
    log "Stopping container..."

    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        # Graceful shutdown via lightning-cli
        docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" \
            stop 2>/dev/null || true

        # Wait for graceful stop
        local timeout=60
        while docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$" && [[ $timeout -gt 0 ]]; do
            sleep 1
            ((timeout--))
        done

        # Force stop if still running
        if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
            docker stop "$CONTAINER_NAME"
        fi
    fi

    log_success "Container stopped"
}

start_container() {
    log "Starting container..."
    docker-compose -f "$DOCKER_DIR/docker-compose.yml" up -d

    # Wait for startup
    sleep 10

    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_success "Container started"
    else
        log_error "Container failed to start"
        return 1
    fi
}

import_gpg_key() {
    local key_file="$DOCKER_DIR/secrets/backup_gpg_key"

    if [[ -f "$key_file" ]]; then
        log "Importing GPG key from $key_file..."
        gpg --import "$key_file" 2>/dev/null || true
        GPG_KEY_ID=$(gpg --list-keys --keyid-format LONG 2>/dev/null | grep -A1 "cl-hive-backup" | grep -oP '[A-F0-9]{16}' | head -1)
        log_success "GPG key imported: $GPG_KEY_ID"
    else
        log_warning "GPG key file not found - will need passphrase for decryption"
    fi
}

restore_hsm_secret() {
    local backup_path="$1"
    local target_dir="/data/lightning/$NETWORK"

    log "Restoring hsm_secret..."

    # Create temp directory
    local temp_dir
    temp_dir=$(mktemp -d)

    # Check if encrypted
    if [[ -f "$backup_path/hsm/hsm_secret.gpg" ]]; then
        log "Decrypting hsm_secret..."

        # Import GPG key if needed
        import_gpg_key

        gpg --decrypt --output "$temp_dir/hsm_secret" "$backup_path/hsm/hsm_secret.gpg"
    else
        cp "$backup_path/hsm/hsm_secret" "$temp_dir/hsm_secret"
    fi

    # Verify checksum if available
    if [[ -f "$backup_path/hsm/hsm_secret.sha256" ]]; then
        local expected_hash
        expected_hash=$(cat "$backup_path/hsm/hsm_secret.sha256" | awk '{print $1}')
        local actual_hash
        actual_hash=$(sha256sum "$temp_dir/hsm_secret" | awk '{print $1}')

        if [[ "$expected_hash" != "$actual_hash" ]]; then
            log_error "hsm_secret checksum mismatch!"
            rm -rf "$temp_dir"
            return 1
        fi
        log_success "hsm_secret checksum verified"
    fi

    # Copy to container volume
    docker cp "$temp_dir/hsm_secret" "$CONTAINER_NAME:$target_dir/hsm_secret"

    # Set permissions
    docker exec "$CONTAINER_NAME" chmod 400 "$target_dir/hsm_secret"

    # Cleanup
    shred -u "$temp_dir/hsm_secret"
    rm -rf "$temp_dir"

    log_success "hsm_secret restored"
}

restore_database() {
    local backup_path="$1"
    local target_dir="/data/lightning/$NETWORK"

    log "Restoring database..."

    local temp_dir
    temp_dir=$(mktemp -d)

    # Check if encrypted
    local db_archive
    if [[ -f "$backup_path/database.tar.gz.gpg" ]]; then
        log "Decrypting database..."
        import_gpg_key
        gpg --decrypt --output "$temp_dir/database.tar.gz" "$backup_path/database.tar.gz.gpg"
        db_archive="$temp_dir/database.tar.gz"
    else
        db_archive="$backup_path/database.tar.gz"
    fi

    # Extract
    tar -xzf "$db_archive" -C "$temp_dir"

    # Verify checksums
    if [[ -f "$temp_dir/database/checksums.sha256" ]]; then
        if (cd "$temp_dir/database" && sha256sum -c checksums.sha256 &>/dev/null); then
            log_success "Database checksum verified"
        else
            log_warning "Database checksum mismatch - proceeding anyway"
        fi
    fi

    # Copy to container
    docker cp "$temp_dir/database/lightningd.sqlite3" "$CONTAINER_NAME:$target_dir/"
    docker cp "$temp_dir/database/lightningd.sqlite3-wal" "$CONTAINER_NAME:$target_dir/" 2>/dev/null || true
    docker cp "$temp_dir/database/lightningd.sqlite3-shm" "$CONTAINER_NAME:$target_dir/" 2>/dev/null || true

    # Cleanup
    rm -rf "$temp_dir"

    log_success "Database restored"
}

restore_config() {
    local backup_path="$1"
    local target_dir="/data/lightning/$NETWORK"

    if [[ ! -f "$backup_path/config.tar.gz" ]]; then
        log_warning "No config backup found - skipping"
        return 0
    fi

    log "Restoring configuration..."

    local temp_dir
    temp_dir=$(mktemp -d)

    tar -xzf "$backup_path/config.tar.gz" -C "$temp_dir"

    # Restore config file
    if [[ -f "$temp_dir/config/config" ]]; then
        docker cp "$temp_dir/config/config" "$CONTAINER_NAME:$target_dir/"
    fi

    # Restore hive database
    if [[ -f "$temp_dir/config/cl-hive.db" ]]; then
        docker cp "$temp_dir/config/cl-hive.db" "$CONTAINER_NAME:$target_dir/"
    fi

    # Cleanup
    rm -rf "$temp_dir"

    log_success "Configuration restored"
}

confirm_restore() {
    local backup_path="$1"

    echo ""
    echo -e "${RED}${BOLD}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}${BOLD}                         WARNING                                ${NC}"
    echo -e "${RED}${BOLD}════════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "This will:"
    echo "  1. STOP the running Lightning node"
    echo "  2. REPLACE existing data with backup data"
    echo "  3. Restart the node with restored data"
    echo ""
    echo "Backup: $backup_path"
    echo "Network: $NETWORK"
    echo ""
    echo -e "${YELLOW}Any channels opened since this backup may be FORCE-CLOSED!${NC}"
    echo ""

    read -p "Type 'RESTORE' to confirm: " confirmation

    if [[ "$confirmation" != "RESTORE" ]]; then
        log "Restore cancelled"
        exit 0
    fi
}

print_usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS] [BACKUP_PATH]

Options:
    --latest        Restore the most recent backup
    --hsm-only      Restore only the hsm_secret file
    --list          List available backups
    --force         Skip confirmation prompt
    --help          Show this help message

Arguments:
    BACKUP_PATH     Path to backup directory (e.g., /backups/backup_20240101_120000)

Environment Variables:
    BACKUP_LOCATION   Backup source directory (default: /backups)
    GPG_KEY_ID        GPG key ID for decryption
    NETWORK           Bitcoin network (default: bitcoin)
    CONTAINER_NAME    Docker container name (default: cl-hive-node)

Examples:
    ./restore.sh --list                           # List available backups
    ./restore.sh --latest                         # Restore most recent backup
    ./restore.sh /backups/backup_20240101_120000  # Restore specific backup
    ./restore.sh --hsm-only --latest              # Restore only hsm_secret
EOF
}

# =============================================================================
# Main
# =============================================================================

main() {
    local backup_path=""
    local hsm_only=false
    local force=false
    local list_only=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --latest)
                backup_path=$(get_latest_backup)
                shift
                ;;
            --hsm-only)
                hsm_only=true
                shift
                ;;
            --list)
                list_only=true
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

    # List mode
    if [[ "$list_only" == "true" ]]; then
        list_backups
        exit $?
    fi

    # Validate backup path
    if [[ -z "$backup_path" ]]; then
        log_error "No backup specified. Use --latest or provide a path."
        print_usage
        exit 1
    fi

    # Verify backup exists
    verify_backup "$backup_path"

    # Confirmation
    if [[ "$force" != "true" ]]; then
        confirm_restore "$backup_path"
    fi

    log ""
    log "Starting restore from: $backup_path"
    log ""

    # Stop container (we need it running initially to copy files)
    # First, let's check if container exists
    if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log "Container doesn't exist - creating temporary container..."
        docker-compose -f "$DOCKER_DIR/docker-compose.yml" up -d
        sleep 10
    fi

    if [[ "$hsm_only" == "true" ]]; then
        stop_container
        # For hsm-only, we need a temporary container
        docker run -d --name "${CONTAINER_NAME}_restore" \
            -v "$(docker volume ls -q | grep lightning-data):/data/lightning" \
            ubuntu:24.04 sleep infinity

        CONTAINER_NAME="${CONTAINER_NAME}_restore"
        restore_hsm_secret "$backup_path"

        docker rm -f "${CONTAINER_NAME}"
        CONTAINER_NAME="${CONTAINER_NAME%_restore}"
        start_container
    else
        stop_container

        # Create temporary restore container
        local volume_name
        volume_name=$(docker volume ls -q | grep -E "cl-hive.*lightning-data" | head -1)

        if [[ -z "$volume_name" ]]; then
            log_error "Could not find lightning-data volume"
            exit 1
        fi

        docker run -d --name "${CONTAINER_NAME}_restore" \
            -v "${volume_name}:/data/lightning" \
            ubuntu:24.04 sleep infinity

        local original_container="$CONTAINER_NAME"
        CONTAINER_NAME="${CONTAINER_NAME}_restore"

        # Create target directory
        docker exec "$CONTAINER_NAME" mkdir -p "/data/lightning/$NETWORK"

        # Restore all components
        restore_hsm_secret "$backup_path"
        restore_database "$backup_path"
        restore_config "$backup_path"

        # Cleanup restore container
        docker rm -f "${CONTAINER_NAME}"
        CONTAINER_NAME="$original_container"

        # Start node
        start_container
    fi

    log ""
    log "========================================="
    log_success "Restore completed successfully!"
    log "========================================="
    log ""
    log "Next steps:"
    log "  1. Check node status: docker-compose exec cln lightning-cli getinfo"
    log "  2. Verify channels: docker-compose exec cln lightning-cli listchannels"
    log "  3. Check hive status: docker-compose exec cln lightning-cli hive-status"
}

main "$@"
