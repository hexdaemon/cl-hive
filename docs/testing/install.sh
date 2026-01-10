#!/bin/bash
#
# Install clboss, cl-revenue-ops, and cl-hive plugins on Polar CLN nodes
#
# Usage: ./install.sh <network-id>
# Example: ./install.sh 1
#
# Environment variables:
#   HIVE_NODES     - CLN nodes to install full hive stack (default: "alice bob carol")
#   VANILLA_NODES  - CLN nodes without hive plugins (default: "dave erin")
#   REVENUE_OPS_PATH - Path to cl_revenue_ops repo (default: /home/sat/cl_revenue_ops)
#   HIVE_PATH      - Path to cl-hive repo (default: /home/sat/cl-hive)
#   SKIP_CLBOSS    - Set to 1 to skip clboss installation on hive nodes
#

set -e

NETWORK_ID="${1:-1}"
HIVE_NODES="${HIVE_NODES:-alice bob carol}"
VANILLA_NODES="${VANILLA_NODES:-dave erin}"
REVENUE_OPS_PATH="${REVENUE_OPS_PATH:-/home/sat/cl_revenue_ops}"
HIVE_PATH="${HIVE_PATH:-/home/sat/cl-hive}"
SKIP_CLBOSS="${SKIP_CLBOSS:-0}"

# CLI command for Polar CLN containers
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

echo "========================================"
echo "Polar Plugin Installer"
echo "========================================"
echo "Network ID: $NETWORK_ID"
echo "Hive Nodes: $HIVE_NODES"
echo "Vanilla Nodes: $VANILLA_NODES"
echo "cl-revenue-ops: $REVENUE_OPS_PATH"
echo "cl-hive: $HIVE_PATH"
echo "Skip CLBOSS: $SKIP_CLBOSS"
echo ""

# Track installation results
HIVE_SUCCESS=0
HIVE_FAIL=0
VANILLA_SUCCESS=0
VANILLA_FAIL=0

#
# Install dependencies on a CLN container
#
install_cln_deps() {
    local container=$1

    echo "  [1/2] Installing dependencies (apt)..."
    docker exec -u root $container apt-get update -qq 2>/dev/null
    docker exec -u root $container apt-get install -y -qq \
        build-essential autoconf autoconf-archive automake libtool pkg-config \
        libev-dev libcurl4-gnutls-dev libsqlite3-dev libunwind-dev \
        python3 python3-pip python3-json5 python3-flask python3-gunicorn \
        git jq > /dev/null 2>&1

    echo "  [2/2] Installing pyln-client (pip)..."
    docker exec -u root $container pip3 install --break-system-packages -q pyln-client 2>/dev/null

    docker exec $container mkdir -p /home/clightning/.lightning/plugins
}

#
# Build and install CLBOSS
#
install_clboss() {
    local container=$1

    if [ "$SKIP_CLBOSS" == "1" ]; then
        echo "  Skipping CLBOSS (SKIP_CLBOSS=1)"
        return 0
    fi

    echo "  Building CLBOSS (this may take several minutes)..."

    # Check if clboss already exists
    if docker exec $container test -f /home/clightning/.lightning/plugins/clboss 2>/dev/null; then
        echo "    CLBOSS already installed, skipping build"
        return 0
    fi

    docker exec $container bash -c "
        cd /tmp &&
        if [ ! -d clboss ]; then
            git clone --recurse-submodules https://github.com/ZmnSCPxj/clboss.git
        fi &&
        cd clboss &&
        autoreconf -i &&
        ./configure &&
        make -j\$(nproc) &&
        cp clboss /home/clightning/.lightning/plugins/
    " 2>&1 | while read line; do echo "    $line"; done

    echo "    CLBOSS build complete"
}

#
# Install hive plugins (cl-revenue-ops, cl-hive)
#
install_hive_plugins() {
    local container=$1

    echo "  Copying cl-revenue-ops..."
    docker cp "$REVENUE_OPS_PATH" $container:/home/clightning/.lightning/plugins/cl-revenue-ops

    echo "  Copying cl-hive..."
    docker cp "$HIVE_PATH" $container:/home/clightning/.lightning/plugins/cl-hive

    echo "  Setting permissions..."
    docker exec -u root $container chown -R clightning:clightning /home/clightning/.lightning/plugins
    docker exec $container chmod +x /home/clightning/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py
    docker exec $container chmod +x /home/clightning/.lightning/plugins/cl-hive/cl-hive.py
}

#
# Load plugins on a hive node
#
load_hive_plugins() {
    local container=$1

    echo "  Loading plugins..."

    if [ "$SKIP_CLBOSS" != "1" ]; then
        if docker exec $container $CLI plugin start /home/clightning/.lightning/plugins/clboss 2>/dev/null; then
            echo "    clboss: loaded"
        else
            echo "    clboss: FAILED"
        fi
    fi

    if docker exec $container $CLI plugin start /home/clightning/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py 2>/dev/null; then
        echo "    cl-revenue-ops: loaded"
    else
        echo "    cl-revenue-ops: FAILED"
    fi

    if docker exec $container $CLI plugin start /home/clightning/.lightning/plugins/cl-hive/cl-hive.py 2>/dev/null; then
        echo "    cl-hive: loaded"
    else
        echo "    cl-hive: FAILED"
    fi
}

#
# Install on HIVE nodes (full stack)
#
echo "========================================"
echo "Installing on HIVE Nodes"
echo "========================================"

for node in $HIVE_NODES; do
    CONTAINER="polar-n${NETWORK_ID}-${node}"

    echo ""
    echo "--- $node ($CONTAINER) ---"

    # Check container exists
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
        echo "  WARNING: Container not found, skipping"
        ((HIVE_FAIL++))
        continue
    fi

    install_cln_deps $CONTAINER
    install_clboss $CONTAINER
    install_hive_plugins $CONTAINER
    load_hive_plugins $CONTAINER

    ((HIVE_SUCCESS++))
done

#
# Install on VANILLA nodes (dependencies only, no plugins)
#
if [ -n "$VANILLA_NODES" ]; then
    echo ""
    echo "========================================"
    echo "Installing on VANILLA Nodes (deps only)"
    echo "========================================"

    for node in $VANILLA_NODES; do
        CONTAINER="polar-n${NETWORK_ID}-${node}"

        echo ""
        echo "--- $node ($CONTAINER) ---"

        # Check container exists
        if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
            echo "  WARNING: Container not found, skipping"
            ((VANILLA_FAIL++))
            continue
        fi

        install_cln_deps $CONTAINER
        echo "  No plugins to install (vanilla node)"

        ((VANILLA_SUCCESS++))
    done
fi

#
# Summary
#
echo ""
echo "========================================"
echo "Installation Summary"
echo "========================================"
echo ""
echo "Hive Nodes:    $HIVE_SUCCESS installed, $HIVE_FAIL skipped"
echo "Vanilla Nodes: $VANILLA_SUCCESS installed, $VANILLA_FAIL skipped"
echo ""

#
# Detect LND and Eclair nodes
#
echo "========================================"
echo "External Node Detection"
echo "========================================"
echo ""

# Check for LND nodes
LND_NODES=$(docker ps --format '{{.Names}}' | grep "polar-n${NETWORK_ID}-" | grep -i lnd || true)
if [ -n "$LND_NODES" ]; then
    echo "LND Nodes found:"
    for lnd in $LND_NODES; do
        node_name=$(echo $lnd | sed "s/polar-n${NETWORK_ID}-//")
        pubkey=$(docker exec $lnd lncli --network=regtest getinfo 2>/dev/null | jq -r '.identity_pubkey' || echo "unavailable")
        echo "  $node_name: $pubkey"
    done
else
    echo "LND Nodes: none found"
fi
echo ""

# Check for Eclair nodes
ECLAIR_NODES=$(docker ps --format '{{.Names}}' | grep "polar-n${NETWORK_ID}-" | grep -i eclair || true)
if [ -n "$ECLAIR_NODES" ]; then
    echo "Eclair Nodes found:"
    for eclair in $ECLAIR_NODES; do
        node_name=$(echo $eclair | sed "s/polar-n${NETWORK_ID}-//")
        pubkey=$(docker exec $eclair eclair-cli getinfo 2>/dev/null | jq -r '.nodeId' || echo "unavailable")
        echo "  $node_name: $pubkey"
    done
else
    echo "Eclair Nodes: none found"
fi
echo ""

#
# Quick verification commands
#
echo "========================================"
echo "Verification Commands"
echo "========================================"
echo ""
echo "# Verify hive plugins loaded:"
echo "docker exec polar-n${NETWORK_ID}-alice $CLI plugin list | grep -E '(clboss|revenue|hive)'"
echo ""
echo "# Check hive status:"
echo "docker exec polar-n${NETWORK_ID}-alice $CLI hive-status"
echo ""
echo "# Run automated tests:"
echo "./test.sh all ${NETWORK_ID}"
echo ""
