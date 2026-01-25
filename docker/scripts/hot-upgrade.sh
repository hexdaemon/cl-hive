#!/bin/bash
# =============================================================================
# cl-hive Hot Upgrade Script
# =============================================================================
# Upgrades plugins WITHOUT rebuilding the Docker image by pulling latest
# changes and restarting lightningd.
#
# Usage:
#   ./hot-upgrade.sh              # Upgrade both plugins
#   ./hot-upgrade.sh hive         # Upgrade only cl-hive  
#   ./hot-upgrade.sh revenue      # Upgrade only cl-revenue-ops
#   ./hot-upgrade.sh --check      # Check for updates without applying
#
# Prerequisites:
#   - cl-hive must be mounted from host (default in docker-compose.yml)
#   - For cl-revenue-ops hot upgrades, add volume mount to compose file
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$DOCKER_DIR")"
CONTAINER_NAME="${CONTAINER_NAME:-cl-hive-node}"
NETWORK="${NETWORK:-bitcoin}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "\n${CYAN}==> $1${NC}"; }

check_container() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_error "Container ${CONTAINER_NAME} is not running"
        exit 1
    fi
}

get_git_version() {
    local repo="$1"
    git -C "$repo" rev-parse --short HEAD 2>/dev/null || echo "unknown"
}

get_remote_version() {
    local repo="$1"
    git -C "$repo" fetch --quiet 2>/dev/null || true
    git -C "$repo" rev-parse --short origin/main 2>/dev/null || echo "unknown"
}

upgrade_cl_hive() {
    log_step "Checking cl-hive for updates..."

    cd "$PROJECT_ROOT"

    local current=$(get_git_version .)
    local remote=$(get_remote_version .)

    echo "  Current: $current"
    echo "  Remote:  $remote"

    if [ "$current" == "$remote" ]; then
        log_info "cl-hive is up to date"
        return 0
    fi

    if [ "$CHECK_ONLY" == "true" ]; then
        log_warn "Update available: $current -> $remote"
        return 1
    fi

    log_info "Pulling latest cl-hive..."
    
    # Stash local changes if any
    if ! git diff --quiet 2>/dev/null; then
        log_warn "Stashing local changes..."
        git stash
    fi

    git pull origin main
    
    log_info "cl-hive upgraded: $current -> $(get_git_version .)"
    return 1  # Signal upgrade was performed
}

upgrade_cl_revenue_ops() {
    log_step "Checking cl-revenue-ops for updates..."

    # Check if git is available in container
    if ! docker exec "$CONTAINER_NAME" test -d /opt/cl-revenue-ops/.git 2>/dev/null; then
        log_warn "cl-revenue-ops is baked into image (not a git repo)"
        log_info "To enable hot upgrades, add to docker-compose.yml volumes:"
        echo "    - /path/to/cl_revenue_ops:/opt/cl-revenue-ops:ro"
        return 0
    fi

    local current=$(docker exec "$CONTAINER_NAME" git -C /opt/cl-revenue-ops rev-parse --short HEAD 2>/dev/null || echo "unknown")
    docker exec "$CONTAINER_NAME" git -C /opt/cl-revenue-ops fetch --quiet 2>/dev/null || true
    local remote=$(docker exec "$CONTAINER_NAME" git -C /opt/cl-revenue-ops rev-parse --short origin/main 2>/dev/null || echo "unknown")

    echo "  Current: $current"
    echo "  Remote:  $remote"

    if [ "$current" == "$remote" ]; then
        log_info "cl-revenue-ops is up to date"
        return 0
    fi

    if [ "$CHECK_ONLY" == "true" ]; then
        log_warn "Update available: $current -> $remote"
        return 1
    fi

    log_info "Pulling latest cl-revenue-ops..."
    docker exec "$CONTAINER_NAME" git -C /opt/cl-revenue-ops pull origin main

    log_info "cl-revenue-ops upgraded: $current -> $(docker exec "$CONTAINER_NAME" git -C /opt/cl-revenue-ops rev-parse --short HEAD)"
    return 1
}

restart_plugin() {
    local plugin_path="$1"
    local plugin_name="$2"

    log_step "Restarting $plugin_name plugin..."

    # Try to stop the plugin gracefully
    if docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" plugin stop "$plugin_path" 2>/dev/null; then
        log_info "Stopped $plugin_name"
        sleep 1
    else
        log_warn "Failed to stop $plugin_name (may not be running)"
    fi

    # Start the plugin
    if docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" plugin start "$plugin_path" 2>/dev/null; then
        log_info "Started $plugin_name"
        return 0
    else
        log_error "Failed to start $plugin_name"
        return 1
    fi
}

restart_plugins() {
    local hive_upgraded="$1"
    local revenue_upgraded="$2"

    log_step "Restarting upgraded plugins..."

    local restart_failed=false

    # Restart plugins individually (faster, no channel downtime)
    if [ "$hive_upgraded" == "true" ]; then
        if ! restart_plugin "/opt/cl-hive/cl-hive.py" "cl-hive"; then
            restart_failed=true
        fi
    fi

    if [ "$revenue_upgraded" == "true" ]; then
        if ! restart_plugin "/opt/cl-revenue-ops/cl-revenue-ops.py" "cl-revenue-ops"; then
            restart_failed=true
        fi
    fi

    if [ "$restart_failed" == "true" ]; then
        log_warn "Plugin restart failed, falling back to lightningd restart..."
        restart_lightningd
    else
        log_info "Plugin restart complete"
    fi
}

restart_lightningd() {
    log_step "Restarting lightningd to load updated plugins..."

    # Graceful restart via supervisorctl
    docker exec "$CONTAINER_NAME" supervisorctl restart lightningd

    log_info "Waiting for node to become healthy..."

    local retries=30
    while [ $retries -gt 0 ]; do
        if docker exec "$CONTAINER_NAME" lightning-cli --lightning-dir="/data/lightning/$NETWORK" getinfo >/dev/null 2>&1; then
            log_info "Node is healthy"
            return 0
        fi
        echo -n "."
        sleep 2
        retries=$((retries - 1))
    done
    echo ""

    log_error "Node did not become healthy after restart"
    log_warn "Check logs: docker logs $CONTAINER_NAME"
    return 1
}

show_versions() {
    echo ""
    echo "Current versions:"
    echo -n "  cl-hive:        "
    get_git_version "$PROJECT_ROOT"
    
    echo -n "  cl-revenue-ops: "
    if docker exec "$CONTAINER_NAME" test -d /opt/cl-revenue-ops/.git 2>/dev/null; then
        docker exec "$CONTAINER_NAME" git -C /opt/cl-revenue-ops rev-parse --short HEAD 2>/dev/null || echo "unknown"
    else
        echo "baked (use full upgrade.sh to update)"
    fi
}

print_usage() {
    cat << 'EOF'
Usage: hot-upgrade.sh [OPTION] [COMPONENT]

Hot upgrade plugins without rebuilding the Docker image.

Components:
    hive        Upgrade only cl-hive
    revenue     Upgrade only cl-revenue-ops
    (none)      Upgrade both

Options:
    --check, -c     Check for updates without applying
    --help, -h      Show this help

Examples:
    ./hot-upgrade.sh              # Upgrade all plugins
    ./hot-upgrade.sh --check      # Check what updates are available
    ./hot-upgrade.sh hive         # Upgrade only cl-hive

Note: cl-hive is mounted from host by default. For cl-revenue-ops hot
upgrades, add this to docker-compose.yml volumes:
    - /path/to/cl_revenue_ops:/opt/cl-revenue-ops:ro
EOF
}

main() {
    local upgrade_hive=true
    local upgrade_revenue=true
    CHECK_ONLY=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            hive)
                upgrade_revenue=false
                shift
                ;;
            revenue)
                upgrade_hive=false
                shift
                ;;
            --check|-c)
                CHECK_ONLY=true
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
    echo -e "${CYAN}======================================================================${NC}"
    echo -e "${CYAN}${BOLD}            cl-hive Hot Upgrade Script                               ${NC}"
    echo -e "${CYAN}======================================================================${NC}"
    if [ "$CHECK_ONLY" == "true" ]; then
        echo -e "${YELLOW}                    [CHECK MODE]                                     ${NC}"
    fi

    check_container
    show_versions

    local hive_upgraded=false
    local revenue_upgraded=false

    if [ "$upgrade_hive" == "true" ]; then
        if ! upgrade_cl_hive; then
            hive_upgraded=true
        fi
    fi

    if [ "$upgrade_revenue" == "true" ]; then
        if ! upgrade_cl_revenue_ops; then
            revenue_upgraded=true
        fi
    fi

    if [ "$CHECK_ONLY" == "true" ]; then
        echo ""
        log_info "Check complete (no changes made)"
        exit 0
    fi

    if [ "$hive_upgraded" == "true" ] || [ "$revenue_upgraded" == "true" ]; then
        # Prefer plugin restart over lightningd restart (faster, no channel downtime)
        restart_plugins "$hive_upgraded" "$revenue_upgraded"
        echo ""
        show_versions
        echo ""
        log_info "Hot upgrade complete!"
    else
        echo ""
        log_info "Everything is up to date"
    fi
}

main "$@"