#!/bin/bash
# =============================================================================
# cl-hive Configuration Validation Script
# =============================================================================
# Validates configuration before starting the node.
# Run this before docker-compose up to catch configuration errors early.
#
# Usage:
#   ./validate-config.sh           # Validate all
#   ./validate-config.sh --quick   # Quick validation only
#   ./validate-config.sh --fix     # Attempt to fix common issues
# =============================================================================

set -euo pipefail

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# Counters
ERRORS=0
WARNINGS=0

# Logging
log() { echo -e "$1"; }
log_check() { echo -en "  Checking $1... "; }
log_ok() { echo -e "${GREEN}OK${NC}"; }
log_warn() { echo -e "${YELLOW}WARNING${NC}: $1"; ((WARNINGS++)); }
log_error() { echo -e "${RED}FAILED${NC}: $1"; ((ERRORS++)); }
log_skip() { echo -e "${CYAN}SKIPPED${NC}"; }

# =============================================================================
# Validation Functions
# =============================================================================

check_env_file() {
    log_check ".env file exists"
    if [[ -f "$DOCKER_DIR/.env" ]]; then
        log_ok
        return 0
    else
        log_error ".env file not found. Run ./setup.sh or copy .env.example"
        return 1
    fi
}

check_required_vars() {
    local env_file="$DOCKER_DIR/.env"

    log ""
    log "${BOLD}Required Variables:${NC}"

    # Load env file
    set -a
    source "$env_file" 2>/dev/null || true
    set +a

    # Bitcoin RPC
    log_check "BITCOIN_RPCUSER"
    if [[ -n "${BITCOIN_RPCUSER:-}" ]]; then
        log_ok
    else
        log_error "BITCOIN_RPCUSER is required"
    fi

    log_check "BITCOIN_RPCPASSWORD or secret file"
    if [[ -n "${BITCOIN_RPCPASSWORD:-}" ]]; then
        log_ok
    elif [[ -f "$DOCKER_DIR/secrets/bitcoin_rpc_password" ]]; then
        log_ok
    else
        log_error "BITCOIN_RPCPASSWORD not set and secrets/bitcoin_rpc_password not found"
    fi

    log_check "BITCOIN_RPCHOST"
    if [[ -n "${BITCOIN_RPCHOST:-}" ]]; then
        log_ok
    else
        log_warn "BITCOIN_RPCHOST not set, will use default"
    fi

    log_check "NETWORK"
    case "${NETWORK:-bitcoin}" in
        bitcoin|testnet|signet|regtest)
            log_ok
            ;;
        *)
            log_error "Invalid NETWORK: ${NETWORK}. Must be: bitcoin, testnet, signet, regtest"
            ;;
    esac
}

check_bitcoin_rpc() {
    log ""
    log "${BOLD}Bitcoin RPC Connectivity:${NC}"

    local env_file="$DOCKER_DIR/.env"
    set -a
    source "$env_file" 2>/dev/null || true
    set +a

    local host="${BITCOIN_RPCHOST:-host.docker.internal}"
    local port="${BITCOIN_RPCPORT:-8332}"
    local user="${BITCOIN_RPCUSER:-}"
    local pass="${BITCOIN_RPCPASSWORD:-}"

    # Try to read from secrets
    if [[ -z "$pass" && -f "$DOCKER_DIR/secrets/bitcoin_rpc_password" ]]; then
        pass=$(cat "$DOCKER_DIR/secrets/bitcoin_rpc_password")
    fi

    if [[ -z "$user" || -z "$pass" ]]; then
        log_check "Bitcoin RPC connection"
        log_skip
        return 0
    fi

    log_check "Bitcoin RPC connection to $host:$port"

    local response
    response=$(curl -s --max-time 10 --user "$user:$pass" \
        --data-binary '{"jsonrpc":"1.0","method":"getblockchaininfo","params":[]}' \
        -H 'content-type: text/plain;' \
        "http://$host:$port/" 2>&1) || true

    if echo "$response" | grep -q '"result"'; then
        log_ok

        # Check sync status
        local progress
        progress=$(echo "$response" | grep -o '"verificationprogress":[0-9.]*' | cut -d':' -f2)
        if [[ -n "$progress" ]]; then
            local pct
            pct=$(echo "$progress * 100" | bc 2>/dev/null || echo "0")
            if (( $(echo "$progress < 0.999" | bc -l 2>/dev/null || echo "0") )); then
                log_warn "Bitcoin is still syncing: ${pct}%"
            fi
        fi
    else
        log_error "Cannot connect to Bitcoin RPC"
        if echo "$response" | grep -qi "connection refused"; then
            log "        → Is Bitcoin Core running?"
        elif echo "$response" | grep -qi "unauthorized"; then
            log "        → Check RPC credentials"
        fi
    fi
}

check_secrets() {
    log ""
    log "${BOLD}Secrets Configuration:${NC}"

    log_check "secrets/ directory"
    if [[ -d "$DOCKER_DIR/secrets" ]]; then
        log_ok
    else
        log_warn "secrets/ directory not found"
        return 0
    fi

    log_check "secrets/ permissions"
    local perms
    perms=$(stat -c '%a' "$DOCKER_DIR/secrets" 2>/dev/null || stat -f '%Lp' "$DOCKER_DIR/secrets")
    if [[ "$perms" == "700" ]]; then
        log_ok
    else
        log_warn "secrets/ should have 700 permissions (current: $perms)"
    fi

    # Check individual secret files
    for secret in bitcoin_rpc_password wg_private_key hive_rune; do
        local secret_file="$DOCKER_DIR/secrets/$secret"
        if [[ -f "$secret_file" ]]; then
            log_check "$secret file permissions"
            perms=$(stat -c '%a' "$secret_file" 2>/dev/null || stat -f '%Lp' "$secret_file")
            if [[ "$perms" == "600" ]]; then
                log_ok
            else
                log_warn "$secret should have 600 permissions (current: $perms)"
            fi
        fi
    done
}

check_wireguard() {
    log ""
    log "${BOLD}WireGuard Configuration:${NC}"

    local env_file="$DOCKER_DIR/.env"
    set -a
    source "$env_file" 2>/dev/null || true
    set +a

    if [[ "${WIREGUARD_ENABLED:-false}" != "true" ]]; then
        log_check "WireGuard"
        log_skip
        return 0
    fi

    log_check "WireGuard private key"
    if [[ -n "${WG_PRIVATE_KEY:-}" || -f "$DOCKER_DIR/secrets/wg_private_key" ]]; then
        log_ok
    else
        log_error "WireGuard enabled but no private key configured"
    fi

    log_check "WireGuard address"
    if [[ -n "${WG_ADDRESS:-}" ]]; then
        if [[ "$WG_ADDRESS" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+$ ]]; then
            log_ok
        else
            log_error "Invalid WG_ADDRESS format: $WG_ADDRESS (expected: x.x.x.x/xx)"
        fi
    else
        log_error "WG_ADDRESS is required when WireGuard is enabled"
    fi

    log_check "WireGuard peer configuration"
    if [[ -n "${WG_PEER_PUBLIC_KEY:-}" && -n "${WG_PEER_ENDPOINT:-}" ]]; then
        log_ok
    else
        log_error "WG_PEER_PUBLIC_KEY and WG_PEER_ENDPOINT are required"
    fi
}

check_docker() {
    log ""
    log "${BOLD}Docker Environment:${NC}"

    log_check "Docker is running"
    if docker info &>/dev/null; then
        log_ok
    else
        log_error "Docker is not running or not accessible"
        return 1
    fi

    log_check "docker-compose available"
    if command -v docker-compose &>/dev/null; then
        log_ok
    else
        log_error "docker-compose not found"
    fi

    log_check "docker-compose.yml exists"
    if [[ -f "$DOCKER_DIR/docker-compose.yml" ]]; then
        log_ok
    else
        log_error "docker-compose.yml not found"
    fi

    log_check "docker-compose.yml syntax"
    if docker-compose -f "$DOCKER_DIR/docker-compose.yml" config --quiet 2>/dev/null; then
        log_ok
    else
        log_error "docker-compose.yml has syntax errors"
    fi
}

check_volumes() {
    log ""
    log "${BOLD}Docker Volumes:${NC}"

    log_check "lightning-data volume"
    if docker volume ls -q | grep -q "lightning-data"; then
        log_ok

        # Check volume size
        local size
        size=$(docker system df -v 2>/dev/null | grep "lightning-data" | awk '{print $4}' || echo "unknown")
        log "        Size: $size"
    else
        log_warn "lightning-data volume doesn't exist (will be created on start)"
    fi
}

check_ports() {
    log ""
    log "${BOLD}Port Availability:${NC}"

    local env_file="$DOCKER_DIR/.env"
    set -a
    source "$env_file" 2>/dev/null || true
    set +a

    local lightning_port="${LIGHTNING_PORT:-9735}"
    local wireguard_port="${WIREGUARD_PORT:-51820}"

    log_check "Port $lightning_port (Lightning P2P)"
    if ! ss -tlnp 2>/dev/null | grep -q ":$lightning_port " && \
       ! netstat -tlnp 2>/dev/null | grep -q ":$lightning_port "; then
        log_ok
    else
        log_warn "Port $lightning_port may already be in use"
    fi

    if [[ "${WIREGUARD_ENABLED:-false}" == "true" ]]; then
        log_check "Port $wireguard_port/udp (WireGuard)"
        if ! ss -ulnp 2>/dev/null | grep -q ":$wireguard_port " && \
           ! netstat -ulnp 2>/dev/null | grep -q ":$wireguard_port "; then
            log_ok
        else
            log_warn "Port $wireguard_port/udp may already be in use"
        fi
    fi
}

check_resources() {
    log ""
    log "${BOLD}System Resources:${NC}"

    log_check "Available memory"
    local mem_available
    mem_available=$(free -g 2>/dev/null | awk '/^Mem:/{print $7}' || echo "unknown")
    if [[ "$mem_available" != "unknown" && "$mem_available" -ge 4 ]]; then
        log_ok
        log "        Available: ${mem_available}G"
    elif [[ "$mem_available" != "unknown" ]]; then
        log_warn "Only ${mem_available}G available (recommend 4G+)"
    else
        log_skip
    fi

    log_check "Available disk space"
    local disk_available
    disk_available=$(df -BG "$DOCKER_DIR" 2>/dev/null | awk 'NR==2{print $4}' | tr -d 'G' || echo "unknown")
    if [[ "$disk_available" != "unknown" && "$disk_available" -ge 10 ]]; then
        log_ok
        log "        Available: ${disk_available}G"
    elif [[ "$disk_available" != "unknown" ]]; then
        log_warn "Only ${disk_available}G available (recommend 10G+)"
    else
        log_skip
    fi
}

fix_issues() {
    log ""
    log "${BOLD}Attempting to fix common issues...${NC}"

    # Fix secrets permissions
    if [[ -d "$DOCKER_DIR/secrets" ]]; then
        chmod 700 "$DOCKER_DIR/secrets"
        chmod 600 "$DOCKER_DIR/secrets"/* 2>/dev/null || true
        log "  Fixed: secrets/ permissions"
    fi

    # Create missing directories
    mkdir -p "$DOCKER_DIR/secrets"
    mkdir -p "$DOCKER_DIR/wireguard"
    mkdir -p "$DOCKER_DIR/config"
    log "  Fixed: Created missing directories"
}

print_summary() {
    log ""
    log "═══════════════════════════════════════════════════════════════"

    if [[ $ERRORS -eq 0 && $WARNINGS -eq 0 ]]; then
        log "${GREEN}${BOLD}All checks passed!${NC}"
        log ""
        log "Ready to start: ${CYAN}docker-compose up -d${NC}"
    elif [[ $ERRORS -eq 0 ]]; then
        log "${YELLOW}${BOLD}Validation completed with $WARNINGS warning(s)${NC}"
        log ""
        log "You can proceed, but review warnings above."
        log "Start with: ${CYAN}docker-compose up -d${NC}"
    else
        log "${RED}${BOLD}Validation failed with $ERRORS error(s) and $WARNINGS warning(s)${NC}"
        log ""
        log "Fix the errors above before starting the node."
        log "Run ${CYAN}./setup.sh${NC} for guided configuration."
    fi

    log "═══════════════════════════════════════════════════════════════"

    return $ERRORS
}

print_usage() {
    cat << EOF
Usage: $(basename "$0") [OPTIONS]

Options:
    --quick     Quick validation (skip network tests)
    --fix       Attempt to fix common issues
    --help      Show this help message

Examples:
    ./validate-config.sh          # Full validation
    ./validate-config.sh --quick  # Skip network connectivity tests
    ./validate-config.sh --fix    # Fix permissions and create directories
EOF
}

# =============================================================================
# Main
# =============================================================================

main() {
    local quick=false
    local fix=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --quick)
                quick=true
                shift
                ;;
            --fix)
                fix=true
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

    log ""
    log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"
    log "${CYAN}${BOLD}           cl-hive Configuration Validator                     ${NC}"
    log "${CYAN}═══════════════════════════════════════════════════════════════${NC}"

    # Run checks
    check_env_file || true
    check_required_vars
    check_secrets
    check_wireguard

    if [[ "$quick" != "true" ]]; then
        check_bitcoin_rpc
    fi

    check_docker
    check_volumes
    check_ports
    check_resources

    # Fix issues if requested
    if [[ "$fix" == "true" ]]; then
        fix_issues
    fi

    # Print summary
    print_summary
}

main "$@"
