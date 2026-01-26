#!/bin/bash
# =============================================================================
# cl-hive Automated Backup Script
# =============================================================================
# Creates encrypted backups of Lightning node data with special handling for
# the critical hsm_secret file.
#
# Usage:
#   ./backup.sh                  # Full backup
#   ./backup.sh --hsm-only       # Backup only hsm_secret
#   ./backup.sh --no-encrypt     # Skip encryption
#   ./backup.sh --verify         # Verify last backup
#
# Configuration (via .env or environment):
#   BACKUP_LOCATION      - Backup destination (default: /backups)
#   BACKUP_RETENTION     - Days to keep backups (default: 30)
#   BACKUP_ENCRYPTION    - Enable GPG encryption (default: true)
#   GPG_KEY_ID           - GPG key for encryption (auto-generated if not set)
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
BACKUP_LOCATION="${BACKUP_LOCATION:-./backups}"
BACKUP_RETENTION="${BACKUP_RETENTION:-30}"
BACKUP_ENCRYPTION="${BACKUP_ENCRYPTION:-true}"
GPG_KEY_ID="${GPG_KEY_ID:-}"
NETWORK="${NETWORK:-bitcoin}"
CONTAINER_NAME="${CONTAINER_NAME:-cl-hive-node}"

# Backup timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="$BACKUP_LOCATION/backup_$TIMESTAMP"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Logging
log() { echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# =============================================================================
# Functions
# =============================================================================

check_prerequisites() {
    log "Checking prerequisites..."

    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_error "Container '$CONTAINER_NAME' is not running"
        exit 1
    fi

    # Check backup location
    if [[ ! -d "$BACKUP_LOCATION" ]]; then
        log "Creating backup directory: $BACKUP_LOCATION"
        mkdir -p "$BACKUP_LOCATION"
    fi

    # Check GPG if encryption enabled
    if [[ "$BACKUP_ENCRYPTION" == "true" ]]; then
        if ! command -v gpg &>/dev/null; then
            log_warning "GPG not found - installing..."
            apt-get update -qq && apt-get install -y -qq gnupg
        fi

        # Generate key if needed
        if [[ -z "$GPG_KEY_ID" || "$GPG_KEY_ID" == "auto" ]]; then
            setup_gpg_key
        fi
    fi

    log_success "Prerequisites check passed"
}

setup_gpg_key() {
    local key_file="$DOCKER_DIR/secrets/backup_gpg_key"

    if [[ -f "$key_file" ]]; then
        log "Importing existing GPG key..."
        gpg --import "$key_file" 2>/dev/null || true
        GPG_KEY_ID=$(gpg --list-keys --keyid-format LONG 2>/dev/null | grep -A1 "cl-hive-backup" | grep -oP '[A-F0-9]{16}' | head -1)
    fi

    if [[ -z "$GPG_KEY_ID" ]]; then
        log "Generating new GPG key for backups..."

        # Generate key with batch mode
        cat > /tmp/gpg_batch << EOF
%echo Generating cl-hive backup key
Key-Type: RSA
Key-Length: 4096
Subkey-Type: RSA
Subkey-Length: 4096
Name-Real: cl-hive-backup
Name-Email: backup@cl-hive.local
Expire-Date: 0
%no-protection
%commit
%echo Done
EOF
        gpg --batch --gen-key /tmp/gpg_batch
        rm -f /tmp/gpg_batch

        # Export private key for recovery
        GPG_KEY_ID=$(gpg --list-keys --keyid-format LONG 2>/dev/null | grep -A1 "cl-hive-backup" | grep -oP '[A-F0-9]{16}' | head -1)
        gpg --export-secret-keys --armor "$GPG_KEY_ID" > "$key_file"
        chmod 600 "$key_file"

        log_success "GPG key generated: $GPG_KEY_ID"
        log_warning "IMPORTANT: Back up $key_file to secure location!"
    fi
}

backup_hsm_secret() {
    log "Backing up hsm_secret (critical)..."

    local hsm_dir="$BACKUP_DIR/hsm"
    mkdir -p "$hsm_dir"

    # Copy hsm_secret from container
    # CLN creates network subdir inside LIGHTNING_DIR, so path is $NETWORK/$NETWORK
    docker cp "$CONTAINER_NAME:/data/lightning/$NETWORK/$NETWORK/hsm_secret" "$hsm_dir/hsm_secret"

    # Fix ownership (docker cp creates files as root)
    sudo chown "$USER:$USER" "$hsm_dir/hsm_secret"
    chmod 600 "$hsm_dir/hsm_secret"

    # Create checksum
    sha256sum "$hsm_dir/hsm_secret" > "$hsm_dir/hsm_secret.sha256"

    # Encrypt if enabled
    if [[ "$BACKUP_ENCRYPTION" == "true" ]] && [[ -n "$GPG_KEY_ID" ]]; then
        gpg --encrypt --recipient "$GPG_KEY_ID" --output "$hsm_dir/hsm_secret.gpg" "$hsm_dir/hsm_secret"
        # Securely delete unencrypted
        shred -u "$hsm_dir/hsm_secret"
        log_success "hsm_secret encrypted with GPG"
    else
        # Set restrictive permissions
        chmod 400 "$hsm_dir/hsm_secret"
        log_warning "hsm_secret NOT encrypted - secure this file!"
    fi

    log_success "hsm_secret backed up to $hsm_dir"
}

backup_database() {
    log "Backing up Lightning database..."

    local db_dir="$BACKUP_DIR/database"
    mkdir -p "$db_dir"

    # Stop accepting new operations
    docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" \
        stop 2>/dev/null || true
    sleep 5

    # Copy database files
    docker cp "$CONTAINER_NAME:/data/lightning/$NETWORK/lightningd.sqlite3" "$db_dir/"

    # Copy database backups if they exist
    docker cp "$CONTAINER_NAME:/data/lightning/$NETWORK/lightningd.sqlite3-wal" "$db_dir/" 2>/dev/null || true
    docker cp "$CONTAINER_NAME:/data/lightning/$NETWORK/lightningd.sqlite3-shm" "$db_dir/" 2>/dev/null || true

    # Fix ownership (docker cp creates files as root)
    sudo chown -R "$USER:$USER" "$db_dir/"

    # Copy plugin databases (cl-hive and cl-revenue-ops)
    log "Backing up plugin databases..."
    docker cp "$CONTAINER_NAME:/data/lightning/$NETWORK/$NETWORK/cl_hive.db" "$db_dir/" 2>/dev/null || \
        docker cp "$CONTAINER_NAME:/root/.lightning/cl_hive.db" "$db_dir/" 2>/dev/null || \
        log_warning "cl-hive database not found"
    docker cp "$CONTAINER_NAME:/data/lightning/$NETWORK/$NETWORK/revenue_ops.db" "$db_dir/" 2>/dev/null || \
        docker cp "$CONTAINER_NAME:/root/.lightning/revenue_ops.db" "$db_dir/" 2>/dev/null || \
        log_warning "cl-revenue-ops database not found"

    # Create checksum
    sha256sum "$db_dir"/* > "$db_dir/checksums.sha256"

    # Compress
    tar -czf "$BACKUP_DIR/database.tar.gz" -C "$BACKUP_DIR" database
    rm -rf "$db_dir"

    # Encrypt if enabled
    if [[ "$BACKUP_ENCRYPTION" == "true" ]] && [[ -n "$GPG_KEY_ID" ]]; then
        gpg --encrypt --recipient "$GPG_KEY_ID" --output "$BACKUP_DIR/database.tar.gz.gpg" "$BACKUP_DIR/database.tar.gz"
        shred -u "$BACKUP_DIR/database.tar.gz"
        log_success "Database encrypted"
    fi

    # Restart node
    docker start "$CONTAINER_NAME" 2>/dev/null || true

    log_success "Database backed up"
}

backup_config() {
    log "Backing up configuration..."

    local config_dir="$BACKUP_DIR/config"
    mkdir -p "$config_dir"

    # Copy configuration files
    docker cp "$CONTAINER_NAME:/data/lightning/$NETWORK/config" "$config_dir/" 2>/dev/null || true
    docker cp "$CONTAINER_NAME:/etc/lightning/cl-hive.conf" "$config_dir/" 2>/dev/null || true

    # Copy Tor hidden service hostname
    docker cp "$CONTAINER_NAME:/var/lib/tor/cln-service/hostname" "$config_dir/tor_hostname" 2>/dev/null || true

    # Copy hive database
    docker cp "$CONTAINER_NAME:/data/lightning/$NETWORK/cl-hive.db" "$config_dir/" 2>/dev/null || true

    # Fix ownership (docker cp creates files as root)
    sudo chown -R "$USER:$USER" "$config_dir/" 2>/dev/null || true

    # Compress
    tar -czf "$BACKUP_DIR/config.tar.gz" -C "$BACKUP_DIR" config
    rm -rf "$config_dir"

    log_success "Configuration backed up"
}

backup_channel_state() {
    log "Backing up channel state..."

    local channels_dir="$BACKUP_DIR/channels"
    mkdir -p "$channels_dir"

    # Export channel backup via lightning-cli
    docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" \
        staticbackup 2>/dev/null > "$channels_dir/staticbackup.json" || true

    # Copy gossip store
    docker cp "$CONTAINER_NAME:/data/lightning/$NETWORK/gossip_store" "$channels_dir/" 2>/dev/null || true

    # Fix ownership (docker cp creates files as root)
    sudo chown -R "$USER:$USER" "$channels_dir/" 2>/dev/null || true

    # Compress
    tar -czf "$BACKUP_DIR/channels.tar.gz" -C "$BACKUP_DIR" channels
    rm -rf "$channels_dir"

    log_success "Channel state backed up"
}

create_manifest() {
    log "Creating backup manifest..."

    cat > "$BACKUP_DIR/manifest.json" << EOF
{
    "version": "1.0",
    "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "network": "$NETWORK",
    "container": "$CONTAINER_NAME",
    "encrypted": $BACKUP_ENCRYPTION,
    "gpg_key_id": "${GPG_KEY_ID:-null}",
    "contents": [
        "hsm/",
        "database.tar.gz${BACKUP_ENCRYPTION:+.gpg}",
        "config.tar.gz",
        "channels.tar.gz"
    ],
    "node_info": $(docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" getinfo 2>/dev/null || echo '{}')
}
EOF

    log_success "Manifest created"
}

cleanup_old_backups() {
    log "Cleaning up backups older than $BACKUP_RETENTION days..."

    local deleted=0
    while IFS= read -r -d '' old_backup; do
        if [[ -d "$old_backup" ]]; then
            rm -rf "$old_backup"
            ((deleted++))
        fi
    done < <(find "$BACKUP_LOCATION" -maxdepth 1 -type d -name "backup_*" -mtime +$BACKUP_RETENTION -print0 2>/dev/null)

    if [[ $deleted -gt 0 ]]; then
        log_success "Deleted $deleted old backup(s)"
    fi
}

verify_backup() {
    local backup_path="${1:-$(ls -td "$BACKUP_LOCATION"/backup_* 2>/dev/null | head -1)}"

    if [[ -z "$backup_path" || ! -d "$backup_path" ]]; then
        log_error "No backup found to verify"
        return 1
    fi

    log "Verifying backup: $backup_path"

    local errors=0

    # Check manifest
    if [[ ! -f "$backup_path/manifest.json" ]]; then
        log_error "Missing manifest.json"
        ((errors++))
    else
        log_success "Manifest exists"
    fi

    # Check hsm_secret
    if [[ -f "$backup_path/hsm/hsm_secret.gpg" ]]; then
        log_success "hsm_secret exists (encrypted)"
    elif [[ -f "$backup_path/hsm/hsm_secret" ]]; then
        log_warning "hsm_secret exists (unencrypted!)"
        # Verify checksum
        if [[ -f "$backup_path/hsm/hsm_secret.sha256" ]]; then
            if (cd "$backup_path/hsm" && sha256sum -c hsm_secret.sha256 &>/dev/null); then
                log_success "hsm_secret checksum valid"
            else
                log_error "hsm_secret checksum INVALID"
                ((errors++))
            fi
        fi
    else
        log_error "Missing hsm_secret"
        ((errors++))
    fi

    # Check database
    if [[ -f "$backup_path/database.tar.gz.gpg" ]] || [[ -f "$backup_path/database.tar.gz" ]]; then
        log_success "Database backup exists"
    else
        log_error "Missing database backup"
        ((errors++))
    fi

    # Check config
    if [[ -f "$backup_path/config.tar.gz" ]]; then
        log_success "Config backup exists"
    else
        log_warning "Missing config backup (not critical)"
    fi

    # Summary
    if [[ $errors -eq 0 ]]; then
        log_success "Backup verification PASSED"
        return 0
    else
        log_error "Backup verification FAILED with $errors error(s)"
        return 1
    fi
}

print_usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
    --hsm-only      Backup only the hsm_secret file
    --no-encrypt    Skip GPG encryption
    --verify        Verify the last backup instead of creating new
    --help          Show this help message

Environment Variables:
    BACKUP_LOCATION      Backup destination (default: /backups)
    BACKUP_RETENTION     Days to keep backups (default: 30)
    BACKUP_ENCRYPTION    Enable GPG encryption (default: true)
    GPG_KEY_ID           GPG key ID for encryption
    NETWORK              Bitcoin network (default: bitcoin)
    CONTAINER_NAME       Docker container name (default: cl-hive-node)

Examples:
    ./backup.sh                     # Full encrypted backup
    ./backup.sh --hsm-only          # Critical hsm_secret only
    ./backup.sh --no-encrypt        # Without encryption
    BACKUP_LOCATION=/mnt/backup ./backup.sh   # Custom location
EOF
}

# =============================================================================
# Main
# =============================================================================

main() {
    local hsm_only=false
    local verify_only=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --hsm-only)
                hsm_only=true
                shift
                ;;
            --no-encrypt)
                BACKUP_ENCRYPTION=false
                shift
                ;;
            --verify)
                verify_only=true
                shift
                ;;
            --help|-h)
                print_usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done

    # Verify mode
    if [[ "$verify_only" == "true" ]]; then
        verify_backup
        exit $?
    fi

    log "Starting cl-hive backup..."
    log "Backup location: $BACKUP_LOCATION"
    log "Network: $NETWORK"
    log "Encryption: $BACKUP_ENCRYPTION"

    # Run backup
    check_prerequisites
    mkdir -p "$BACKUP_DIR"

    if [[ "$hsm_only" == "true" ]]; then
        backup_hsm_secret
    else
        backup_hsm_secret
        backup_database
        backup_config
        backup_channel_state
        create_manifest
        cleanup_old_backups
    fi

    # Final verification
    verify_backup "$BACKUP_DIR"

    log ""
    log "========================================="
    log_success "Backup completed: $BACKUP_DIR"
    log "========================================="

    # Size info
    local backup_size
    backup_size=$(du -sh "$BACKUP_DIR" | cut -f1)
    log "Total size: $backup_size"
}

main "$@"
