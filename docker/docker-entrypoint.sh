#!/bin/bash
set -e

# =============================================================================
# cl-hive Production Node Entrypoint
# =============================================================================
# Environment Variables:
#   BITCOIN_RPCHOST      - Bitcoin RPC host (default: 127.0.0.1)
#   BITCOIN_RPCPORT      - Bitcoin RPC port (default: 8332)
#   BITCOIN_RPCUSER      - Bitcoin RPC username (required)
#   BITCOIN_RPCPASSWORD  - Bitcoin RPC password (required)
#   NETWORK              - bitcoin, testnet, signet, regtest (default: bitcoin)
#   ALIAS                - Node alias (default: cl-hive-node)
#   RGB                  - Node color in hex (default: FF9900)
#   ANNOUNCE_ADDR        - Public address to announce (optional)
#   TOR_ENABLED          - Enable Tor (default: true)
#   WIREGUARD_ENABLED    - Enable WireGuard (default: false)
#   WIREGUARD_CONFIG     - Path to WireGuard config (default: /etc/wireguard/wg0.conf)
#   HIVE_GOVERNANCE_MODE - advisor, autonomous, oracle (default: advisor)
#   CLBOSS_ENABLED       - Enable CLBOSS (default: true, optional - hive works without it)
#   LOG_LEVEL            - debug, info, unusual, broken (default: info)
# =============================================================================

echo "=== cl-hive Production Node ==="
echo "Starting initialization..."

# -----------------------------------------------------------------------------
# Secret Loading Function
# -----------------------------------------------------------------------------
# Reads secrets from Docker secrets files (/run/secrets/) or environment
load_secret() {
    local var_name="$1"
    local file_var="${var_name}_FILE"
    local secret_value=""

    # Check for _FILE environment variable pointing to secret
    if [[ -n "${!file_var:-}" && -f "${!file_var}" ]]; then
        secret_value=$(cat "${!file_var}")
        echo "Loaded $var_name from file"
    # Check standard Docker secrets location
    elif [[ -f "/run/secrets/${var_name,,}" ]]; then
        secret_value=$(cat "/run/secrets/${var_name,,}")
        echo "Loaded $var_name from Docker secret"
    # Fall back to environment variable
    elif [[ -n "${!var_name:-}" ]]; then
        secret_value="${!var_name}"
    fi

    # Export the value
    export "$var_name"="$secret_value"
}

# -----------------------------------------------------------------------------
# Default Values
# -----------------------------------------------------------------------------

BITCOIN_RPCHOST="${BITCOIN_RPCHOST:-127.0.0.1}"
BITCOIN_RPCPORT="${BITCOIN_RPCPORT:-8332}"
NETWORK="${NETWORK:-bitcoin}"
ALIAS="${ALIAS:-cl-hive-node}"
RGB="${RGB:-FF9900}"
TOR_ENABLED="${TOR_ENABLED:-true}"
WIREGUARD_ENABLED="${WIREGUARD_ENABLED:-false}"
HIVE_GOVERNANCE_MODE="${HIVE_GOVERNANCE_MODE:-advisor}"
CLBOSS_ENABLED="${CLBOSS_ENABLED:-true}"
LOG_LEVEL="${LOG_LEVEL:-info}"

# -----------------------------------------------------------------------------
# Load Secrets
# -----------------------------------------------------------------------------
echo "Loading secrets..."
load_secret BITCOIN_RPCPASSWORD
load_secret WG_PRIVATE_KEY

# Set network-specific defaults
case "$NETWORK" in
    testnet)
        BITCOIN_RPCPORT="${BITCOIN_RPCPORT:-18332}"
        LIGHTNING_DIR="/data/lightning/testnet"
        ;;
    signet)
        BITCOIN_RPCPORT="${BITCOIN_RPCPORT:-38332}"
        LIGHTNING_DIR="/data/lightning/signet"
        ;;
    regtest)
        BITCOIN_RPCPORT="${BITCOIN_RPCPORT:-18443}"
        LIGHTNING_DIR="/data/lightning/regtest"
        ;;
    *)
        LIGHTNING_DIR="/data/lightning/bitcoin"
        ;;
esac

mkdir -p "$LIGHTNING_DIR"

# -----------------------------------------------------------------------------
# Validate Required Variables
# -----------------------------------------------------------------------------

if [ -z "$BITCOIN_RPCUSER" ]; then
    echo "ERROR: BITCOIN_RPCUSER is required"
    exit 1
fi

if [ -z "$BITCOIN_RPCPASSWORD" ]; then
    echo "ERROR: BITCOIN_RPCPASSWORD is required"
    exit 1
fi

# -----------------------------------------------------------------------------
# Generate Lightning Configuration
# -----------------------------------------------------------------------------

echo "Generating lightning configuration..."

CONFIG_FILE="$LIGHTNING_DIR/config"

cat > "$CONFIG_FILE" << EOF
# cl-hive Production Node Configuration
# Generated at $(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Network
network=$NETWORK

# Node Identity
alias=$ALIAS
rgb=$RGB

# Bitcoin Backend
bitcoin-rpcconnect=$BITCOIN_RPCHOST
bitcoin-rpcport=$BITCOIN_RPCPORT
bitcoin-rpcuser=$BITCOIN_RPCUSER
bitcoin-rpcpassword=$BITCOIN_RPCPASSWORD

# Logging
log-level=$LOG_LEVEL
log-file=$LIGHTNING_DIR/lightningd.log

# Database
wallet=sqlite3://$LIGHTNING_DIR/lightningd.sqlite3

# Plugins directory
plugin-dir=/root/.lightning/plugins
EOF

# Add announce address if specified
if [ -n "$ANNOUNCE_ADDR" ]; then
    echo "announce-addr=$ANNOUNCE_ADDR" >> "$CONFIG_FILE"
fi

# -----------------------------------------------------------------------------
# Tor Configuration
# -----------------------------------------------------------------------------

if [ "$TOR_ENABLED" = "true" ]; then
    echo "Configuring Tor..."

    # Update torrc with correct paths
    cat > /etc/tor/torrc << EOF
DataDirectory /var/lib/tor
HiddenServiceDir /var/lib/tor/cln-service
HiddenServicePort 9735 127.0.0.1:9735
HiddenServiceVersion 3
SocksPort 9050
Log notice file /var/log/tor/notices.log
EOF

    # Add Tor settings to lightning config
    cat >> "$CONFIG_FILE" << EOF

# Tor Configuration
proxy=127.0.0.1:9050
always-use-proxy=true
bind-addr=127.0.0.1:9735
EOF

    # Ensure Tor directories exist with correct permissions
    mkdir -p /var/lib/tor/cln-service /var/log/tor
    chown -R debian-tor:debian-tor /var/lib/tor /var/log/tor
    chmod 700 /var/lib/tor/cln-service

    echo "Tor configured - hidden service will be created on first start"
else
    echo "Tor disabled"
    cat >> "$CONFIG_FILE" << EOF

# Direct connections (Tor disabled)
bind-addr=0.0.0.0:9735
EOF
fi

# -----------------------------------------------------------------------------
# WireGuard Configuration
# -----------------------------------------------------------------------------

if [ "$WIREGUARD_ENABLED" = "true" ]; then
    echo "Configuring WireGuard..."

    WG_CONFIG_FILE="/etc/wireguard/wg0.conf"
    WG_CONFIG_GENERATED=false

    # Option 1: Generate config from environment variables
    if [ -n "$WG_PRIVATE_KEY" ] && [ -n "$WG_PEER_PUBLIC_KEY" ]; then
        echo "Generating WireGuard config from environment variables..."

        # Extract VPN subnet from WG_ADDRESS (e.g., 10.8.0.2/24 -> 10.8.0.0/24)
        WG_ADDR="${WG_ADDRESS:-10.8.0.2/24}"
        WG_SUBNET=$(echo "$WG_ADDR" | sed -E 's/\.[0-9]+\//.0\//')

        mkdir -p /etc/wireguard
        cat > "$WG_CONFIG_FILE" << EOF
[Interface]
PrivateKey = $WG_PRIVATE_KEY
Address = $WG_ADDR
MTU = 1420
EOF

        # Add DNS if specified
        if [ -n "$WG_DNS" ]; then
            echo "DNS = $WG_DNS" >> "$WG_CONFIG_FILE"
        fi

        # Add peer configuration
        # AllowedIPs is set to VPN subnet only (extracted from WG_ADDRESS)
        cat >> "$WG_CONFIG_FILE" << EOF

[Peer]
PublicKey = $WG_PEER_PUBLIC_KEY
Endpoint = ${WG_PEER_ENDPOINT}
AllowedIPs = $WG_SUBNET
PersistentKeepalive = ${WG_PEER_KEEPALIVE:-25}
EOF

        chmod 600 "$WG_CONFIG_FILE"
        WG_CONFIG_GENERATED=true
        echo "WireGuard config generated (VPN subnet: $WG_SUBNET)"

    # Option 2: Use mounted config file
    elif [ -f "$WG_CONFIG_FILE" ]; then
        echo "Using mounted WireGuard config"
        WG_CONFIG_GENERATED=true
    else
        echo "WARNING: WireGuard enabled but no config provided"
        echo "Set WG_PRIVATE_KEY and WG_PEER_PUBLIC_KEY, or mount config to /etc/wireguard/wg0.conf"
    fi

    # Bring up WireGuard if config exists
    if [ "$WG_CONFIG_GENERATED" = "true" ]; then
        # Load WireGuard kernel module
        modprobe wireguard 2>/dev/null || echo "WireGuard module may already be loaded"

        # Bring up interface
        wg-quick up wg0 || echo "WireGuard interface may already be up"

        # Show connection status
        echo "WireGuard interface status:"
        wg show wg0 2>/dev/null || echo "Could not show WireGuard status"

        echo "WireGuard configured successfully"
    fi
else
    echo "WireGuard disabled"
fi

# -----------------------------------------------------------------------------
# CLBOSS Configuration
# -----------------------------------------------------------------------------

if [ "$CLBOSS_ENABLED" = "true" ]; then
    echo "CLBOSS enabled (optional integration)"
else
    echo "CLBOSS disabled (optional) - hive uses native expansion control"
    rm -f /root/.lightning/plugins/clboss
fi

# -----------------------------------------------------------------------------
# cl-hive Configuration
# -----------------------------------------------------------------------------

echo "Configuring cl-hive..."

cat >> "$CONFIG_FILE" << EOF

# =============================================================================
# cl-hive Configuration
# =============================================================================

hive-governance-mode=$HIVE_GOVERNANCE_MODE
EOF

# Append additional hive config if exists
if [ -f /etc/lightning/cl-hive.conf ]; then
    echo "" >> "$CONFIG_FILE"
    grep -v "^hive-governance-mode" /etc/lightning/cl-hive.conf >> "$CONFIG_FILE" || true
fi

# -----------------------------------------------------------------------------
# Environment Variables for Plugins
# -----------------------------------------------------------------------------

# Export for cl-revenue-ops if needed
export LIGHTNING_DIR="$LIGHTNING_DIR"

# -----------------------------------------------------------------------------
# Wait for Bitcoin RPC
# -----------------------------------------------------------------------------

echo "Waiting for Bitcoin RPC at $BITCOIN_RPCHOST:$BITCOIN_RPCPORT..."

MAX_RETRIES=60
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if curl -s --user "$BITCOIN_RPCUSER:$BITCOIN_RPCPASSWORD" \
        --data-binary '{"jsonrpc":"1.0","method":"getblockchaininfo","params":[]}' \
        -H 'content-type: text/plain;' \
        "http://$BITCOIN_RPCHOST:$BITCOIN_RPCPORT/" > /dev/null 2>&1; then
        echo "Bitcoin RPC available"
        break
    fi

    RETRY_COUNT=$((RETRY_COUNT + 1))
    echo "Waiting for Bitcoin RPC... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 5
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "ERROR: Bitcoin RPC not available after $MAX_RETRIES attempts"
    exit 1
fi

# -----------------------------------------------------------------------------
# Display Configuration Summary
# -----------------------------------------------------------------------------

echo ""
echo "=== Configuration Summary ==="
echo "Network:        $NETWORK"
echo "Alias:          $ALIAS"
echo "Bitcoin RPC:    $BITCOIN_RPCHOST:$BITCOIN_RPCPORT"
echo "Tor:            $TOR_ENABLED"
echo "WireGuard:      $WIREGUARD_ENABLED"
echo "CLBOSS:         $CLBOSS_ENABLED"
echo "Hive Mode:      $HIVE_GOVERNANCE_MODE"
echo "Lightning Dir:  $LIGHTNING_DIR"
echo "============================="
echo ""

# -----------------------------------------------------------------------------
# Pre-flight Validation
# -----------------------------------------------------------------------------

# Validate critical configuration
if [ -z "$BITCOIN_RPCPASSWORD" ]; then
    echo "WARNING: BITCOIN_RPCPASSWORD not loaded - check secrets configuration"
fi

# Ensure supervisor log directory exists
mkdir -p /var/log/supervisor

# Copy pre-stop script if not present
if [ -d /opt/cl-hive/docker/scripts ]; then
    cp /opt/cl-hive/docker/scripts/pre-stop.sh /usr/local/bin/ 2>/dev/null || true
    chmod +x /usr/local/bin/pre-stop.sh 2>/dev/null || true
fi

echo "Initialization complete. Starting services..."

# -----------------------------------------------------------------------------
# Execute Command
# -----------------------------------------------------------------------------

exec "$@"
