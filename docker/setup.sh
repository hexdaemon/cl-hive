#!/bin/bash
# =============================================================================
# cl-hive Production Setup Wizard
# =============================================================================
# Interactive configuration wizard for production deployments.
# Creates .env, docker-compose.override.yml, and secrets directory.
#
# Usage: ./setup.sh [--non-interactive]
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default values
DEFAULT_BITCOIN_RPCHOST="host.docker.internal"
DEFAULT_BITCOIN_RPCPORT="8332"
DEFAULT_NETWORK="bitcoin"
DEFAULT_ALIAS="cl-hive-node"
DEFAULT_RGB="e33502"
DEFAULT_LIGHTNING_PORT="9736"
DEFAULT_NETWORK_MODE="tor"
DEFAULT_WIREGUARD_ENABLED="false"
DEFAULT_RTL_PORT="3000"
DEFAULT_BACKUP_LOCATION="/backups"
DEFAULT_BACKUP_RETENTION="30"
DEFAULT_CPU_LIMIT="4"
DEFAULT_MEMORY_LIMIT="8"

# Check if running interactively
INTERACTIVE=true
if [[ "$1" == "--non-interactive" ]]; then
    INTERACTIVE=false
fi

# =============================================================================
# Helper Functions
# =============================================================================

print_header() {
    echo ""
    echo -e "${CYAN}┌─────────────────────────────────────────┐${NC}"
    echo -e "${CYAN}│${BOLD}     cl-hive Production Setup Wizard     ${NC}${CYAN}│${NC}"
    echo -e "${CYAN}└─────────────────────────────────────────┘${NC}"
    echo ""
}

print_step() {
    local step=$1
    local total=$2
    local title=$3
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Step $step/$total: $title${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${CYAN}ℹ${NC} $1"
}

prompt() {
    local var_name=$1
    local prompt_text=$2
    local default_value=$3
    local is_secret=${4:-false}

    if [[ "$is_secret" == "true" ]]; then
        read -sp "  → $prompt_text: " value
        echo ""
    else
        if [[ -n "$default_value" ]]; then
            read -p "  → $prompt_text [$default_value]: " value
            value="${value:-$default_value}"
        else
            read -p "  → $prompt_text: " value
        fi
    fi

    eval "$var_name=\"$value\""
}

prompt_yes_no() {
    local var_name=$1
    local prompt_text=$2
    local default=$3  # Y or N

    local prompt_suffix
    if [[ "$default" == "Y" ]]; then
        prompt_suffix="[Y/n]"
    else
        prompt_suffix="[y/N]"
    fi

    read -p "  → $prompt_text $prompt_suffix: " value
    value="${value:-$default}"

    if [[ "${value,,}" =~ ^(yes|y)$ ]]; then
        eval "$var_name=true"
    else
        eval "$var_name=false"
    fi
}

prompt_choice() {
    local var_name=$1
    local prompt_text=$2
    shift 2
    local options=("$@")

    echo "  $prompt_text"
    local i=1
    for opt in "${options[@]}"; do
        echo "    ($i) $opt"
        ((i++))
    done

    read -p "  → Select [1]: " choice
    choice="${choice:-1}"

    if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#options[@]} )); then
        eval "$var_name=\"${options[$((choice-1))]}\""
    else
        eval "$var_name=\"${options[0]}\""
    fi
}

validate_bitcoin_rpc() {
    local host=$1
    local port=$2
    local user=$3
    local pass=$4

    print_info "Testing Bitcoin RPC connection..."

    local response
    response=$(curl -s --max-time 10 --user "$user:$pass" \
        --data-binary '{"jsonrpc":"1.0","method":"getblockchaininfo","params":[]}' \
        -H 'content-type: text/plain;' \
        "http://$host:$port/" 2>&1) || true

    if echo "$response" | grep -q '"result"'; then
        print_success "Bitcoin RPC connection successful"

        # Extract network info
        local chain
        chain=$(echo "$response" | grep -o '"chain":"[^"]*"' | cut -d'"' -f4)
        local blocks
        blocks=$(echo "$response" | grep -o '"blocks":[0-9]*' | cut -d':' -f2)

        print_info "  Chain: $chain"
        print_info "  Blocks: $blocks"
        return 0
    else
        print_warning "Could not connect to Bitcoin RPC"
        print_info "Response: $response"
        return 1
    fi
}

# =============================================================================
# Main Setup Flow
# =============================================================================

main() {
    print_header

    # Check for existing configuration
    if [[ -f ".env" ]]; then
        print_warning "Existing .env file found"
        prompt_yes_no OVERWRITE "Overwrite existing configuration?" "N"
        if [[ "$OVERWRITE" != "true" ]]; then
            print_info "Setup cancelled. Use --force to overwrite."
            exit 0
        fi
        # Backup existing
        cp .env ".env.backup.$(date +%Y%m%d%H%M%S)"
        print_success "Existing .env backed up"
    fi

    # -------------------------------------------------------------------------
    # Step 1: Bitcoin RPC Configuration
    # -------------------------------------------------------------------------
    print_step 1 6 "Bitcoin RPC Configuration"

    prompt BITCOIN_RPCHOST "Bitcoin RPC Host" "$DEFAULT_BITCOIN_RPCHOST"
    prompt BITCOIN_RPCPORT "Bitcoin RPC Port" "$DEFAULT_BITCOIN_RPCPORT"
    prompt BITCOIN_RPCUSER "Bitcoin RPC Username" ""
    prompt BITCOIN_RPCPASSWORD "Bitcoin RPC Password" "" true

    if [[ -z "$BITCOIN_RPCUSER" || -z "$BITCOIN_RPCPASSWORD" ]]; then
        print_error "Bitcoin RPC credentials are required"
        exit 1
    fi

    # Validate connection
    if ! validate_bitcoin_rpc "$BITCOIN_RPCHOST" "$BITCOIN_RPCPORT" "$BITCOIN_RPCUSER" "$BITCOIN_RPCPASSWORD"; then
        prompt_yes_no CONTINUE_ANYWAY "Continue anyway?" "N"
        if [[ "$CONTINUE_ANYWAY" != "true" ]]; then
            print_error "Setup cancelled - fix Bitcoin RPC and try again"
            exit 1
        fi
    fi

    # -------------------------------------------------------------------------
    # Step 2: Network Selection
    # -------------------------------------------------------------------------
    print_step 2 6 "Network Selection"

    prompt_choice NETWORK "Select Bitcoin network:" "bitcoin (mainnet)" "testnet" "signet" "regtest"

    # Normalize network name
    case "$NETWORK" in
        "bitcoin (mainnet)") NETWORK="bitcoin" ;;
        *) NETWORK="${NETWORK%% *}" ;;  # Remove anything after space
    esac

    print_success "Network: $NETWORK"

    # -------------------------------------------------------------------------
    # Step 3: Node Identity & RTL
    # -------------------------------------------------------------------------
    print_step 3 6 "Node Identity & Web Interface"

    prompt ALIAS "Node Alias" "$DEFAULT_ALIAS"
    prompt RGB "Node Color (hex, no #)" "$DEFAULT_RGB"

    # Validate color
    if ! [[ "$RGB" =~ ^[0-9A-Fa-f]{6}$ ]]; then
        print_warning "Invalid color format, using default"
        RGB="$DEFAULT_RGB"
    fi

    print_success "Alias: $ALIAS (color: #$RGB)"

    echo ""
    print_info "Ride The Lightning (RTL) Web Interface"
    print_info "RTL provides a web UI for managing your Lightning node."
    echo ""
    prompt_yes_no RTL_ENABLED "Enable RTL web interface?" "Y"

    if [[ "$RTL_ENABLED" == "true" ]]; then
        prompt RTL_PASSWORD "RTL web interface password" "" true
        if [[ -z "$RTL_PASSWORD" ]]; then
            RTL_PASSWORD="changeme"
            print_warning "Using default password 'changeme' - CHANGE THIS IN PRODUCTION!"
        fi
        prompt RTL_PORT "RTL web interface port" "$DEFAULT_RTL_PORT"
        print_success "RTL will be accessible at http://localhost:$RTL_PORT"
    else
        RTL_PASSWORD=""
        RTL_PORT="$DEFAULT_RTL_PORT"
        print_info "RTL disabled - you can enable it later by editing .env"
    fi

    # -------------------------------------------------------------------------
    # Step 4: Privacy & Networking
    # -------------------------------------------------------------------------
    print_step 4 6 "Privacy & Networking"

    prompt LIGHTNING_PORT "Lightning P2P port" "$DEFAULT_LIGHTNING_PORT"

    echo ""
    print_info "Network Mode Options:"
    echo "  1. tor      - Tor-only, anonymous, no clearnet (recommended)"
    echo "  2. clearnet - Direct connections only, requires public IP"
    echo "  3. hybrid   - Both Tor and clearnet"
    echo ""
    prompt_choice NETWORK_MODE "Select network mode:" "tor" "clearnet" "hybrid"

    case "$NETWORK_MODE" in
        tor)
            print_success "Tor-only mode - your node will be accessible via .onion address"
            ANNOUNCE_ADDR=""
            ;;
        clearnet)
            print_info "Clearnet mode requires a public address"
            prompt ANNOUNCE_ADDR "Public announce address (ip:port or hostname:port)" ""
            if [[ -z "$ANNOUNCE_ADDR" ]]; then
                print_warning "No announce address set - node will not be discoverable!"
            fi
            ;;
        hybrid)
            print_success "Hybrid mode - accessible via Tor and optionally clearnet"
            prompt ANNOUNCE_ADDR "Public announce address (optional, press Enter to skip)" ""
            if [[ -n "$ANNOUNCE_ADDR" ]]; then
                print_success "Clearnet address: $ANNOUNCE_ADDR"
            else
                print_info "No clearnet address - node reachable via Tor only"
            fi
            ;;
    esac

    echo ""
    prompt_yes_no WIREGUARD_ENABLED "Enable WireGuard VPN?" "N"

    if [[ "$WIREGUARD_ENABLED" == "true" ]]; then
        echo ""
        print_info "WireGuard Configuration"
        print_info "Your VPN administrator should provide these values."
        echo ""

        prompt_choice WG_CONFIG_METHOD "Configuration method:" "Environment variables" "Mount config file"

        if [[ "$WG_CONFIG_METHOD" == "Environment variables" ]]; then
            prompt WG_PRIVATE_KEY "Your WireGuard private key" "" true
            prompt WG_ADDRESS "Your VPN IP address (e.g., 10.8.0.2/24)" ""
            prompt WG_PEER_PUBLIC_KEY "VPN server public key" ""
            prompt WG_PEER_ENDPOINT "VPN server endpoint (host:port)" ""
            prompt WG_DNS "DNS server on VPN (optional)" ""
        else
            print_info "Mount your wg0.conf to ./wireguard/wg0.conf"
            mkdir -p wireguard
            WG_CONFIG_PATH="./wireguard"
        fi
    fi

    # -------------------------------------------------------------------------
    # Step 5: Backup Configuration
    # -------------------------------------------------------------------------
    print_step 5 6 "Backup Configuration"

    prompt BACKUP_LOCATION "Backup storage location" "$DEFAULT_BACKUP_LOCATION"

    prompt_yes_no BACKUP_ENCRYPTION "Enable backup encryption (GPG)?" "Y"

    if [[ "$BACKUP_ENCRYPTION" == "true" ]]; then
        prompt_choice GPG_METHOD "GPG key configuration:" "Generate new key" "Use existing key ID"

        if [[ "$GPG_METHOD" == "Generate new key" ]]; then
            print_info "A GPG key will be generated during first backup"
            GPG_KEY_ID="auto"
        else
            prompt GPG_KEY_ID "GPG Key ID" ""
        fi
    fi

    prompt BACKUP_RETENTION "Backup retention days" "$DEFAULT_BACKUP_RETENTION"

    # -------------------------------------------------------------------------
    # Step 6: Resource Limits
    # -------------------------------------------------------------------------
    print_step 6 6 "Resource Limits"

    print_info "Configure container resource limits for production stability."
    echo ""

    prompt CPU_LIMIT "CPU cores limit" "$DEFAULT_CPU_LIMIT"
    prompt MEMORY_LIMIT "Memory limit (GB)" "$DEFAULT_MEMORY_LIMIT"

    # Validate numeric
    if ! [[ "$CPU_LIMIT" =~ ^[0-9]+\.?[0-9]*$ ]]; then
        CPU_LIMIT="$DEFAULT_CPU_LIMIT"
    fi
    if ! [[ "$MEMORY_LIMIT" =~ ^[0-9]+$ ]]; then
        MEMORY_LIMIT="$DEFAULT_MEMORY_LIMIT"
    fi

    CPU_RESERVATION=$(echo "$CPU_LIMIT / 2" | bc -l | xargs printf "%.1f")
    MEMORY_RESERVATION=$((MEMORY_LIMIT / 2))

    print_success "CPU: ${CPU_LIMIT} cores (${CPU_RESERVATION} reserved)"
    print_success "Memory: ${MEMORY_LIMIT}G (${MEMORY_RESERVATION}G reserved)"

    # -------------------------------------------------------------------------
    # Generate Configuration Files
    # -------------------------------------------------------------------------
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}Generating Configuration...${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""

    # Create required directories
    mkdir -p secrets
    chmod 700 secrets
    mkdir -p config

    # Write secrets (not in .env)
    echo -n "$BITCOIN_RPCPASSWORD" > secrets/bitcoin_rpc_password
    chmod 600 secrets/bitcoin_rpc_password
    print_success "Created secrets/bitcoin_rpc_password"

    if [[ -n "$WG_PRIVATE_KEY" ]]; then
        echo -n "$WG_PRIVATE_KEY" > secrets/wg_private_key
        chmod 600 secrets/wg_private_key
        print_success "Created secrets/wg_private_key"
    fi

    # Generate .env file (non-sensitive values)
    cat > .env << EOF
# cl-hive Production Node Environment Configuration
# Generated by setup.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
#
# IMPORTANT: Sensitive values are stored in secrets/ directory
# Do not commit .env or secrets/ to version control

# =============================================================================
# BITCOIN RPC
# =============================================================================
BITCOIN_RPCHOST=$BITCOIN_RPCHOST
BITCOIN_RPCPORT=$BITCOIN_RPCPORT
BITCOIN_RPCUSER=$BITCOIN_RPCUSER
# Password is in secrets/bitcoin_rpc_password

# =============================================================================
# NETWORK
# =============================================================================
NETWORK=$NETWORK

# =============================================================================
# NODE IDENTITY
# =============================================================================
ALIAS=$ALIAS
RGB=$RGB

# =============================================================================
# NETWORK MODE & CONNECTIVITY
# =============================================================================
LIGHTNING_PORT=$LIGHTNING_PORT
NETWORK_MODE=$NETWORK_MODE
ANNOUNCE_ADDR=${ANNOUNCE_ADDR:-}
WIREGUARD_ENABLED=$WIREGUARD_ENABLED
EOF

    if [[ "$WIREGUARD_ENABLED" == "true" ]]; then
        cat >> .env << EOF

# WireGuard Configuration
WG_ADDRESS=${WG_ADDRESS:-}
WG_PEER_PUBLIC_KEY=${WG_PEER_PUBLIC_KEY:-}
WG_PEER_ENDPOINT=${WG_PEER_ENDPOINT:-}
WG_DNS=${WG_DNS:-}
WG_PEER_KEEPALIVE=25
# Private key is in secrets/wg_private_key
EOF
    fi

    cat >> .env << EOF

# =============================================================================
# CL-HIVE
# =============================================================================
HIVE_GOVERNANCE_MODE=advisor
CLBOSS_ENABLED=true
LOG_LEVEL=info

# =============================================================================
# RESOURCE LIMITS
# =============================================================================
CPU_LIMIT=$CPU_LIMIT
CPU_RESERVATION=$CPU_RESERVATION
MEMORY_LIMIT=${MEMORY_LIMIT}G
MEMORY_RESERVATION=${MEMORY_RESERVATION}G

# =============================================================================
# BACKUP
# =============================================================================
BACKUP_LOCATION=$BACKUP_LOCATION
BACKUP_RETENTION=$BACKUP_RETENTION
BACKUP_ENCRYPTION=$BACKUP_ENCRYPTION
GPG_KEY_ID=${GPG_KEY_ID:-}

# =============================================================================
# RIDE THE LIGHTNING
# =============================================================================
RTL_ENABLED=$RTL_ENABLED
RTL_PASSWORD=${RTL_PASSWORD:-changeme}
RTL_PORT=${RTL_PORT:-3000}
EOF

    # Add COMPOSE_PROFILES if RTL is enabled
    if [[ "$RTL_ENABLED" == "true" ]]; then
        cat >> .env << EOF
# Enable RTL profile
COMPOSE_PROFILES=rtl
EOF
    fi

    cat >> .env << EOF

# =============================================================================
# PORTS
# =============================================================================
LIGHTNING_PORT=$LIGHTNING_PORT
WIREGUARD_PORT=51820
EOF

    print_success "Created .env"

    # Generate docker-compose.override.yml for resource limits
    # Build the override file with conditional WireGuard support
    cat > docker-compose.override.yml << EOF
# cl-hive Production Overrides
# Generated by setup.sh on $(date -u +"%Y-%m-%dT%H:%M:%SZ")
#
# This file extends docker-compose.yml with production settings.
# It is automatically loaded by docker-compose.

services:
  cln:
    # Resource limits for production stability
    deploy:
      resources:
        limits:
          cpus: '${CPU_LIMIT}'
          memory: ${MEMORY_LIMIT}G
        reservations:
          cpus: '${CPU_RESERVATION}'
          memory: ${MEMORY_RESERVATION}G

    # Graceful shutdown
    stop_grace_period: 120s
    stop_signal: SIGTERM

    # Additional volumes for backups
    volumes:
      - lightning-data:/data/lightning
      - \${CUSTOM_CONFIG_PATH:-./config}:/etc/lightning/custom:ro
      - ${BACKUP_LOCATION}:/backups
EOF

    # Add WireGuard volume if enabled
    if [[ "$WIREGUARD_ENABLED" == "true" ]]; then
        cat >> docker-compose.override.yml << EOF
      - \${WIREGUARD_CONFIG_PATH:-./wireguard}:/etc/wireguard:ro
EOF
    fi

    # Add environment variables for secrets
    cat >> docker-compose.override.yml << EOF

    # Read secrets from files
    environment:
      - BITCOIN_RPCPASSWORD_FILE=/run/secrets/bitcoin_rpc_password
EOF

    # Add WireGuard env if enabled
    if [[ "$WIREGUARD_ENABLED" == "true" ]]; then
        cat >> docker-compose.override.yml << EOF
      - WG_PRIVATE_KEY_FILE=/run/secrets/wg_private_key
EOF
    fi

    # Add secrets mounts
    cat >> docker-compose.override.yml << EOF

    secrets:
      - bitcoin_rpc_password
EOF

    # Add WireGuard secret if enabled
    if [[ "$WIREGUARD_ENABLED" == "true" ]]; then
        cat >> docker-compose.override.yml << EOF
      - wg_private_key
EOF
    fi

    # Define secrets at top level
    cat >> docker-compose.override.yml << EOF

secrets:
  bitcoin_rpc_password:
    file: ./secrets/bitcoin_rpc_password
EOF

    # Add WireGuard secret definition if enabled
    if [[ "$WIREGUARD_ENABLED" == "true" ]]; then
        cat >> docker-compose.override.yml << EOF
  wg_private_key:
    file: ./secrets/wg_private_key
EOF
    fi

    print_success "Created docker-compose.override.yml"

    # Create backup directory
    mkdir -p "$BACKUP_LOCATION" 2>/dev/null || true

    # Update .gitignore
    if ! grep -q "secrets/" .gitignore 2>/dev/null; then
        cat >> .gitignore << EOF

# Production secrets (never commit)
secrets/
*.backup.*
EOF
        print_success "Updated .gitignore"
    fi

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    echo ""
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}${BOLD}Setup Complete!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "${BOLD}Configuration Summary:${NC}"
    echo "  Network:     $NETWORK"
    echo "  Alias:       $ALIAS"
    echo "  Bitcoin RPC: $BITCOIN_RPCHOST:$BITCOIN_RPCPORT"
    echo "  LN Port:     $LIGHTNING_PORT"
    echo "  Net Mode:    $NETWORK_MODE"
    echo "  WireGuard:   $WIREGUARD_ENABLED"
    if [[ "$RTL_ENABLED" == "true" ]]; then
        echo "  RTL:         http://localhost:$RTL_PORT"
    else
        echo "  RTL:         disabled"
    fi
    echo "  Resources:   ${CPU_LIMIT} CPUs, ${MEMORY_LIMIT}G RAM"
    echo "  Backups:     $BACKUP_LOCATION (${BACKUP_RETENTION} days)"
    echo ""
    echo -e "${BOLD}Files Created:${NC}"
    echo "  ✓ .env"
    echo "  ✓ docker-compose.override.yml"
    echo "  ✓ secrets/bitcoin_rpc_password"
    [[ -n "$WG_PRIVATE_KEY" ]] && echo "  ✓ secrets/wg_private_key"
    echo ""
    echo -e "${BOLD}Next Steps:${NC}"
    echo ""
    echo -e "  1. Review configuration:"
    echo -e "     ${CYAN}cat .env${NC}"
    echo ""
    echo -e "  2. Start the node:"
    echo -e "     ${CYAN}docker-compose up -d${NC}"
    echo ""
    echo -e "  3. Check status:"
    echo -e "     ${CYAN}docker-compose logs -f${NC}"
    echo -e "     ${CYAN}docker-compose exec cln lightning-cli getinfo${NC}"
    echo ""
    if [[ "$RTL_ENABLED" == "true" ]]; then
        echo -e "  4. Access RTL web interface:"
        echo -e "     ${CYAN}http://localhost:$RTL_PORT${NC}"
        echo ""
    fi
    if [[ "$NETWORK" == "bitcoin" ]]; then
        echo -e "  ${YELLOW}⚠ IMPORTANT: Back up your hsm_secret after first start!${NC}"
        echo -e "     ${CYAN}./scripts/backup.sh${NC}"
        echo ""
    fi
}

# Run main
main "$@"
