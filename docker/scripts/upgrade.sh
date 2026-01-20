#!/bin/bash
# =============================================================================
# cl-hive Safe Upgrade Script
# =============================================================================
# Upgrades the cl-hive Docker deployment with automatic backup and rollback
# on failure.
#
# Usage:
#   ./upgrade.sh                    # Upgrade to latest
#   ./upgrade.sh --version v1.2.0   # Upgrade to specific version
#   ./upgrade.sh --dry-run          # Show what would happen
#   ./upgrade.sh --skip-backup      # Skip pre-upgrade backup (not recommended)
#
# The script will:
#   1. Create a backup of current state
#   2. Pull/build new image
#   3. Stop current container gracefully
#   4. Start new container
#   5. Verify health
#   6. Rollback on failure
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
CONTAINER_NAME="${CONTAINER_NAME:-cl-hive-node}"
NETWORK="${NETWORK:-bitcoin}"
IMAGE_NAME="${IMAGE_NAME:-cl-hive-node}"
HEALTH_CHECK_RETRIES="${HEALTH_CHECK_RETRIES:-10}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-10}"

# State
BACKUP_PATH=""
CURRENT_IMAGE=""
NEW_IMAGE=""
DRY_RUN=false
SKIP_BACKUP=false
TARGET_VERSION=""

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

check_prerequisites() {
    log_step "Checking prerequisites..."

    # Check docker
    if ! command -v docker &>/dev/null; then
        log_error "Docker not found"
        exit 1
    fi

    # Check docker-compose
    if ! command -v docker-compose &>/dev/null; then
        log_error "docker-compose not found"
        exit 1
    fi

    # Check we're in the right directory
    if [[ ! -f "$DOCKER_DIR/docker-compose.yml" ]]; then
        log_error "docker-compose.yml not found in $DOCKER_DIR"
        exit 1
    fi

    # Check if container exists
    if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_warning "Container '$CONTAINER_NAME' not found - will create new"
    fi

    log_success "Prerequisites check passed"
}

get_current_version() {
    log_step "Getting current version info..."

    # Get current image
    CURRENT_IMAGE=$(docker inspect "$CONTAINER_NAME" --format '{{.Config.Image}}' 2>/dev/null || echo "unknown")

    # Get current plugin version
    local plugin_version
    plugin_version=$(docker exec "$CONTAINER_NAME" cat /opt/cl-hive/VERSION 2>/dev/null || echo "unknown")

    log "Current image: $CURRENT_IMAGE"
    log "Current cl-hive version: $plugin_version"

    # Get node info
    local node_info
    node_info=$(docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" getinfo 2>/dev/null || echo "{}")

    local node_id
    node_id=$(echo "$node_info" | grep -o '"id": "[^"]*"' | cut -d'"' -f4 | head -c 20 || echo "unknown")
    local num_channels
    num_channels=$(echo "$node_info" | grep -o '"num_active_channels": [0-9]*' | cut -d':' -f2 | tr -d ' ' || echo "0")

    log "Node ID: ${node_id}..."
    log "Active channels: $num_channels"
}

create_backup() {
    if [[ "$SKIP_BACKUP" == "true" ]]; then
        log_warning "Skipping backup (--skip-backup specified)"
        return 0
    fi

    log_step "Creating pre-upgrade backup..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log "[DRY RUN] Would run: $SCRIPT_DIR/backup.sh"
        BACKUP_PATH="/backups/backup_dryrun"
        return 0
    fi

    # Run backup script
    if [[ -x "$SCRIPT_DIR/backup.sh" ]]; then
        "$SCRIPT_DIR/backup.sh"
        BACKUP_PATH=$(ls -td /backups/backup_* 2>/dev/null | head -1)
        log_success "Backup created: $BACKUP_PATH"
    else
        log_error "Backup script not found or not executable"
        exit 1
    fi
}

pull_new_image() {
    log_step "Pulling/building new image..."

    if [[ -n "$TARGET_VERSION" ]]; then
        NEW_IMAGE="${IMAGE_NAME}:${TARGET_VERSION}"
    else
        NEW_IMAGE="${IMAGE_NAME}:latest"
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log "[DRY RUN] Would build image: $NEW_IMAGE"
        return 0
    fi

    # Build new image
    cd "$DOCKER_DIR/.."
    docker-compose -f "$DOCKER_DIR/docker-compose.yml" build --no-cache

    log_success "New image built: $NEW_IMAGE"
}

stop_current() {
    log_step "Stopping current container gracefully..."

    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log "Container not running"
        return 0
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        log "[DRY RUN] Would stop container: $CONTAINER_NAME"
        return 0
    fi

    # Use pre-stop script for graceful shutdown
    if [[ -x "$SCRIPT_DIR/pre-stop.sh" ]]; then
        docker exec "$CONTAINER_NAME" /opt/cl-hive/docker/scripts/pre-stop.sh 2>/dev/null || true
    fi

    # Stop container with grace period
    docker-compose -f "$DOCKER_DIR/docker-compose.yml" stop -t 120

    log_success "Container stopped gracefully"
}

start_new() {
    log_step "Starting new container..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log "[DRY RUN] Would start container with new image"
        return 0
    fi

    cd "$DOCKER_DIR"
    docker-compose up -d

    log "Waiting for container to start..."
    sleep 10

    log_success "Container started"
}

health_check() {
    log_step "Running health checks..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log "[DRY RUN] Would run health checks"
        return 0
    fi

    local retries=$HEALTH_CHECK_RETRIES

    while [[ $retries -gt 0 ]]; do
        log "Health check attempt $(( HEALTH_CHECK_RETRIES - retries + 1 ))/$HEALTH_CHECK_RETRIES..."

        # Check 1: Container running
        if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
            log_warning "Container not running"
            ((retries--))
            sleep $HEALTH_CHECK_INTERVAL
            continue
        fi

        # Check 2: Lightning RPC responding
        if ! docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" getinfo &>/dev/null; then
            log_warning "Lightning RPC not responding"
            ((retries--))
            sleep $HEALTH_CHECK_INTERVAL
            continue
        fi

        # Check 3: Hive plugin loaded
        if ! docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" hive-status &>/dev/null; then
            log_warning "Hive plugin not responding"
            ((retries--))
            sleep $HEALTH_CHECK_INTERVAL
            continue
        fi

        # All checks passed
        log_success "All health checks passed"
        return 0
    done

    log_error "Health checks failed after $HEALTH_CHECK_RETRIES attempts"
    return 1
}

rollback() {
    log_step "Rolling back to previous version..."

    log_warning "Upgrade failed - initiating rollback"

    if [[ "$DRY_RUN" == "true" ]]; then
        log "[DRY RUN] Would rollback to: $CURRENT_IMAGE"
        return 0
    fi

    # Stop new container
    docker-compose -f "$DOCKER_DIR/docker-compose.yml" stop -t 30 2>/dev/null || true

    # Restore backup if available
    if [[ -n "$BACKUP_PATH" && -d "$BACKUP_PATH" ]]; then
        log "Restoring from backup: $BACKUP_PATH"
        "$SCRIPT_DIR/restore.sh" --force "$BACKUP_PATH"
    else
        # Just restart with old image
        log_warning "No backup available - attempting restart only"
        docker-compose -f "$DOCKER_DIR/docker-compose.yml" up -d
    fi

    # Verify rollback
    sleep 10
    if health_check; then
        log_warning "Rollback successful - running previous version"
        exit 1
    else
        log_error "CRITICAL: Rollback failed - manual intervention required!"
        log_error "Backup location: $BACKUP_PATH"
        exit 2
    fi
}

print_summary() {
    echo ""
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}${BOLD}                    Upgrade Complete!                          ${NC}"
    echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  Previous image: $CURRENT_IMAGE"
    echo "  New image:      $NEW_IMAGE"
    echo "  Backup:         ${BACKUP_PATH:-none}"
    echo ""
    echo "  Verify with:"
    echo "    docker-compose exec cln lightning-cli getinfo"
    echo "    docker-compose exec cln lightning-cli hive-status"
    echo ""
}

print_usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
    --version VERSION   Upgrade to specific version/tag
    --dry-run           Show what would happen without making changes
    --skip-backup       Skip pre-upgrade backup (not recommended)
    --force             Skip confirmation prompts
    --help              Show this help message

Environment Variables:
    CONTAINER_NAME         Docker container name (default: cl-hive-node)
    IMAGE_NAME             Image name (default: cl-hive-node)
    NETWORK                Bitcoin network (default: bitcoin)
    HEALTH_CHECK_RETRIES   Number of health check retries (default: 10)

Examples:
    ./upgrade.sh                      # Upgrade to latest
    ./upgrade.sh --version v1.2.0     # Upgrade to specific version
    ./upgrade.sh --dry-run            # Preview upgrade steps
EOF
}

# =============================================================================
# Main
# =============================================================================

main() {
    local force=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --version)
                TARGET_VERSION="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --skip-backup)
                SKIP_BACKUP=true
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
            *)
                log_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
    done

    # Header
    echo ""
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}${BOLD}               cl-hive Upgrade Script                          ${NC}"
    echo -e "${CYAN}═══════════════════════════════════════════════════════════════${NC}"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}                    [DRY RUN MODE]                             ${NC}"
    fi
    echo ""

    # Run upgrade steps
    check_prerequisites
    get_current_version

    # Confirmation
    if [[ "$force" != "true" && "$DRY_RUN" != "true" ]]; then
        echo ""
        read -p "Proceed with upgrade? [y/N]: " confirm
        if [[ ! "${confirm,,}" =~ ^(yes|y)$ ]]; then
            log "Upgrade cancelled"
            exit 0
        fi
    fi

    create_backup
    pull_new_image
    stop_current
    start_new

    # Health check with rollback on failure
    if ! health_check; then
        rollback
    fi

    if [[ "$DRY_RUN" != "true" ]]; then
        print_summary
    else
        echo ""
        log "[DRY RUN] All steps would complete successfully"
    fi
}

main "$@"
