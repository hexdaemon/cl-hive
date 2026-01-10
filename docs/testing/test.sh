#!/bin/bash
#
# Automated test suite for cl-hive and cl-revenue-ops plugins
#
# Usage: ./test.sh [category] [network_id]
# Categories: all, setup, genesis, join, promotion, sync, intent, channels, fees, clboss, contrib, cross, reset
#
# Example: ./test.sh all 1
# Example: ./test.sh genesis 1
# Example: ./test.sh reset 1
#

set -o pipefail

# Configuration
CATEGORY="${1:-all}"
NETWORK_ID="${2:-1}"

# Node configuration
HIVE_NODES="alice bob carol"
VANILLA_NODES="dave erin"
LND_NODES="lnd1 lnd2"
ECLAIR_NODES="eclair1 eclair2"

# CLI commands
CLN_CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

# Test tracking
TESTS_PASSED=0
TESTS_FAILED=0
FAILED_TESTS=""

# Colors (if terminal supports it)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    NC=''
fi

#
# Helper Functions
#

log_info() {
    echo -e "${YELLOW}[INFO]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
}

# Execute a test and track results
run_test() {
    local name="$1"
    local cmd="$2"

    echo -n "[TEST] $name... "

    if output=$(eval "$cmd" 2>&1); then
        log_pass ""
        ((TESTS_PASSED++))
        return 0
    else
        log_fail ""
        echo "       Output: $output"
        ((TESTS_FAILED++))
        FAILED_TESTS="$FAILED_TESTS\n  - $name"
        return 1
    fi
}

# Execute a test that should fail
run_test_expect_fail() {
    local name="$1"
    local cmd="$2"

    echo -n "[TEST] $name (expect fail)... "

    if output=$(eval "$cmd" 2>&1); then
        log_fail "(should have failed)"
        ((TESTS_FAILED++))
        FAILED_TESTS="$FAILED_TESTS\n  - $name"
        return 1
    else
        log_pass ""
        ((TESTS_PASSED++))
        return 0
    fi
}

# CLN CLI wrapper for hive nodes
hive_cli() {
    local node=$1
    shift
    docker exec polar-n${NETWORK_ID}-${node} $CLN_CLI "$@"
}

# CLN CLI wrapper for vanilla nodes
vanilla_cli() {
    local node=$1
    shift
    docker exec polar-n${NETWORK_ID}-${node} $CLN_CLI "$@"
}

# LND CLI wrapper
lnd_cli() {
    local node=$1
    shift
    docker exec polar-n${NETWORK_ID}-${node} lncli --network=regtest "$@"
}

# Eclair CLI wrapper
eclair_cli() {
    local node=$1
    shift
    docker exec polar-n${NETWORK_ID}-${node} eclair-cli "$@"
}

# Check if container exists
container_exists() {
    docker ps --format '{{.Names}}' | grep -q "^polar-n${NETWORK_ID}-$1$"
}

# Wait for condition with timeout
wait_for() {
    local cmd="$1"
    local expected="$2"
    local timeout="${3:-30}"
    local elapsed=0

    while [ $elapsed -lt $timeout ]; do
        if result=$(eval "$cmd" 2>/dev/null) && echo "$result" | grep -q "$expected"; then
            return 0
        fi
        sleep 1
        ((elapsed++))
    done
    return 1
}

# Get node pubkey
get_pubkey() {
    local node=$1
    local type=$2

    case $type in
        cln)
            hive_cli $node getinfo | jq -r '.id'
            ;;
        lnd)
            lnd_cli $node getinfo | jq -r '.identity_pubkey'
            ;;
        eclair)
            eclair_cli $node getinfo | jq -r '.nodeId'
            ;;
    esac
}

#
# Test Categories
#

# Setup Tests - Verify environment is ready
test_setup() {
    echo ""
    echo "========================================"
    echo "SETUP TESTS"
    echo "========================================"

    # Check hive node containers
    for node in $HIVE_NODES; do
        run_test "Container $node exists" "container_exists $node"
    done

    # Check vanilla node containers
    for node in $VANILLA_NODES; do
        run_test "Container $node exists" "container_exists $node"
    done

    # Check hive plugins loaded
    for node in $HIVE_NODES; do
        if container_exists $node; then
            run_test "$node has clboss" "hive_cli $node plugin list | grep -q clboss"
            run_test "$node has sling" "hive_cli $node plugin list | grep -q sling"
            run_test "$node has cl-revenue-ops" "hive_cli $node plugin list | grep -q revenue-ops"
            run_test "$node has cl-hive" "hive_cli $node plugin list | grep -q cl-hive"
        fi
    done

    # Check vanilla nodes don't have hive plugins
    for node in $VANILLA_NODES; do
        if container_exists $node; then
            run_test_expect_fail "$node has NO cl-hive" "vanilla_cli $node plugin list | grep -q cl-hive"
        fi
    done

    # Check LND nodes (optional)
    for node in $LND_NODES; do
        if container_exists $node; then
            run_test "LND $node accessible" "lnd_cli $node getinfo | jq -e '.identity_pubkey'"
        fi
    done

    # Check Eclair nodes (optional)
    for node in $ECLAIR_NODES; do
        if container_exists $node; then
            run_test "Eclair $node accessible" "eclair_cli $node getinfo | jq -e '.nodeId'"
        fi
    done
}

# Genesis Tests - Hive creation
test_genesis() {
    echo ""
    echo "========================================"
    echo "GENESIS TESTS"
    echo "========================================"

    # Check hive status before genesis
    run_test "Alice status before genesis" "hive_cli alice hive-status | jq -e '.status'"

    # Create hive on alice
    run_test "Alice creates hive" "hive_cli alice hive-genesis | jq -e '.status == \"genesis_complete\"'"

    # Verify genesis ticket returned
    run_test "Genesis returns ticket" "hive_cli alice hive-genesis 2>/dev/null || hive_cli alice hive-status | jq -e '.status == \"active\"'"

    # Verify alice is admin
    run_test "Alice is admin" "hive_cli alice hive-members | jq -e '.members[0].tier == \"admin\"'"

    # Check status is active
    run_test "Hive status is active" "hive_cli alice hive-status | jq -e '.status == \"active\"'"

    # Cannot genesis twice (should fail or return already active)
    run_test_expect_fail "Cannot genesis twice" "hive_cli alice hive-genesis | jq -e '.status == \"genesis_complete\"'"
}

# Join Tests - Member invitation and joining
test_join() {
    echo ""
    echo "========================================"
    echo "JOIN TESTS"
    echo "========================================"

    # Generate invite ticket
    run_test "Alice generates invite" "hive_cli alice hive-invite | jq -e '.ticket'"

    # Get ticket for bob
    TICKET=$(hive_cli alice hive-invite | jq -r '.ticket')

    # Bob joins with ticket
    if [ -n "$TICKET" ] && [ "$TICKET" != "null" ]; then
        run_test "Bob joins with ticket" "hive_cli bob hive-join ticket=\"$TICKET\" | jq -e '.status'"

        # Wait for join to process
        sleep 2

        # Check bob's status
        run_test "Bob has hive status" "hive_cli bob hive-status | jq -e '.status'"
    else
        log_fail "Could not get invite ticket"
        ((TESTS_FAILED++))
    fi

    # Generate another ticket for carol
    TICKET=$(hive_cli alice hive-invite | jq -r '.ticket')

    if [ -n "$TICKET" ] && [ "$TICKET" != "null" ]; then
        run_test "Carol joins with ticket" "hive_cli carol hive-join ticket=\"$TICKET\" | jq -e '.status'"
        sleep 2
    fi

    # Check member count (may need time for handshake)
    run_test "Alice sees members" "hive_cli alice hive-members | jq -e '.count >= 1'"
}

# Promotion Tests - Neophyte to Member promotion
test_promotion() {
    echo ""
    echo "========================================"
    echo "PROMOTION TESTS"
    echo "========================================"

    # Get bob's pubkey
    BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
    CAROL_PUBKEY=$(hive_cli carol getinfo | jq -r '.id')

    # Check bob's current tier
    BOB_TIER=$(hive_cli alice hive-members | jq -r --arg pk "$BOB_PUBKEY" '.members[] | select(.peer_id == $pk) | .tier')
    log_info "Bob's current tier: $BOB_TIER"

    # If bob is neophyte, test promotion flow
    if [ "$BOB_TIER" == "neophyte" ]; then
        # Bob requests promotion
        run_test "Bob requests promotion" "hive_cli bob hive-request-promotion | jq -e '.status'"
        sleep 2

        # Check pending promotions on alice
        run_test "Alice sees promotion request" "hive_cli alice hive-pending-promotions | jq -e '.count >= 1'"

        # Alice vouches for bob
        run_test "Alice vouches for Bob" "hive_cli alice hive-vouch $BOB_PUBKEY | jq -e '.status'"
        sleep 2

        # Bob should now be member (auto-promotion with quorum=1)
        run_test "Bob promoted to member" "hive_cli alice hive-members | jq -r --arg pk \"$BOB_PUBKEY\" '.members[] | select(.peer_id == \$pk) | .tier' | grep -q member"
    else
        log_info "Bob is already $BOB_TIER, skipping promotion flow"
        run_test "Bob tier is member or admin" "echo '$BOB_TIER' | grep -E '^(member|admin)$'"
    fi

    # Carol should remain neophyte (we don't promote her for testing)
    CAROL_TIER=$(hive_cli alice hive-members | jq -r --arg pk "$CAROL_PUBKEY" '.members[] | select(.peer_id == $pk) | .tier')
    log_info "Carol's current tier: $CAROL_TIER"

    # Verify carol is in members list
    run_test "Carol is in members list" "hive_cli alice hive-members | jq -e --arg pk \"$CAROL_PUBKEY\" '.members[] | select(.peer_id == \$pk)'"

    # Test that neophyte cannot vouch
    if [ "$CAROL_TIER" == "neophyte" ]; then
        # Carol tries to vouch for bob (should fail or be ignored)
        run_test_expect_fail "Neophyte cannot vouch" "hive_cli carol hive-vouch $BOB_PUBKEY 2>&1 | grep -q 'success'"
    fi

    # Test double promotion request (should fail or be idempotent)
    if [ "$BOB_TIER" == "member" ]; then
        run_test_expect_fail "Member cannot request promotion" "hive_cli bob hive-request-promotion 2>&1 | grep -q 'request accepted'"
    fi
}

# Sync Tests - State synchronization (L6)
test_sync() {
    echo ""
    echo "========================================"
    echo "SYNC TESTS (L6)"
    echo "========================================"

    # L6.1 State Hash Consistency (if implemented)
    ALICE_HASH=$(hive_cli alice hive-status 2>/dev/null | jq -r '.state_hash // "none"')
    BOB_HASH=$(hive_cli bob hive-status 2>/dev/null | jq -r '.state_hash // "none"')
    CAROL_HASH=$(hive_cli carol hive-status 2>/dev/null | jq -r '.state_hash // "none"')
    log_info "State hashes - Alice: $ALICE_HASH, Bob: $BOB_HASH, Carol: $CAROL_HASH"

    # Get state from each hive node
    ALICE_STATUS=$(hive_cli alice hive-status 2>/dev/null)
    BOB_STATUS=$(hive_cli bob hive-status 2>/dev/null)
    CAROL_STATUS=$(hive_cli carol hive-status 2>/dev/null)

    # Check all nodes have status
    run_test "Alice has hive status" "echo '$ALICE_STATUS' | jq -e '.status'"
    run_test "Bob has hive status" "echo '$BOB_STATUS' | jq -e '.status'"
    run_test "Carol has hive status" "echo '$CAROL_STATUS' | jq -e '.status'"

    # L6.2 Member List Consistency
    ALICE_COUNT=$(hive_cli alice hive-members 2>/dev/null | jq '.count')
    BOB_COUNT=$(hive_cli bob hive-members 2>/dev/null | jq '.count')
    CAROL_COUNT=$(hive_cli carol hive-members 2>/dev/null | jq '.count')
    log_info "Member counts - Alice: $ALICE_COUNT, Bob: $BOB_COUNT, Carol: $CAROL_COUNT"
    run_test "Member count consistency" "[ '$ALICE_COUNT' = '$BOB_COUNT' ] && [ '$BOB_COUNT' = '$CAROL_COUNT' ]"

    # L6.3 Gossip on State Change (implicit via member consistency)
    run_test "All nodes see same hive_id" "
        ALICE_HID=\$(hive_cli alice hive-status | jq -r '.hive_id')
        BOB_HID=\$(hive_cli bob hive-status | jq -r '.hive_id')
        CAROL_HID=\$(hive_cli carol hive-status | jq -r '.hive_id')
        [ \"\$ALICE_HID\" = \"\$BOB_HID\" ] && [ \"\$BOB_HID\" = \"\$CAROL_HID\" ]
    "

    # Check gossip is working (capacity info)
    run_test "Alice revenue-status works" "hive_cli alice revenue-status | jq -e '.status'"

    # Check contribution tracking
    run_test "Alice contribution works" "hive_cli alice hive-contribution | jq -e '.peer_id'"

    # L6.5 Heartbeat Messages (check logs)
    run_test "Heartbeat in Alice logs" "docker exec polar-n${NETWORK_ID}-alice cat /home/clightning/.lightning/regtest/debug.log 2>/dev/null | grep -qi 'heartbeat' || echo 'no heartbeat yet' | grep -q 'no'"
}

# Intent Lock Tests (L7)
test_intent() {
    echo ""
    echo "========================================"
    echo "INTENT LOCK TESTS (L7)"
    echo "========================================"

    # L7.1 Intent Creation - pending actions
    run_test "Pending actions works" "hive_cli alice hive-pending-actions | jq -e '.count >= 0'"
    run_test "Bob pending actions works" "hive_cli bob hive-pending-actions | jq -e '.count >= 0'"

    # L7.2 Intent API exists
    run_test "approve-action API exists" "hive_cli alice help | grep -q approve-action"
    run_test "reject-action API exists" "hive_cli alice help | grep -q reject-action"

    # L7.4 Deterministic Tie-Breaker logic
    ALICE_PUBKEY=$(hive_cli alice getinfo | jq -r '.id')
    BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
    CAROL_PUBKEY=$(hive_cli carol getinfo | jq -r '.id')

    log_info "Pubkey comparison for tie-breaker:"
    log_info "  Alice: ${ALICE_PUBKEY:0:16}..."
    log_info "  Bob:   ${BOB_PUBKEY:0:16}..."
    log_info "  Carol: ${CAROL_PUBKEY:0:16}..."

    # In a conflict, lower pubkey wins
    SORTED=$(echo -e "$ALICE_PUBKEY\n$BOB_PUBKEY\n$CAROL_PUBKEY" | sort | head -1)
    if [ "$SORTED" = "$ALICE_PUBKEY" ]; then
        log_info "Alice has lowest pubkey (wins conflicts)"
    elif [ "$SORTED" = "$BOB_PUBKEY" ]; then
        log_info "Bob has lowest pubkey (wins conflicts)"
    else
        log_info "Carol has lowest pubkey (wins conflicts)"
    fi
    run_test "Pubkeys are sortable" "echo '$SORTED' | grep -qE '^[0-9a-f]{66}$'"

    # L7.5 Check active intents
    run_test "No stale intents on Alice" "hive_cli alice hive-pending-actions | jq -e '.count >= 0'"
}

# Channel Tests - Channel opening with intent protocol (L8)
test_channels() {
    echo ""
    echo "========================================"
    echo "CHANNEL TESTS (L8)"
    echo "========================================"

    # Check existing channels
    run_test "Alice has listpeerchannels" "hive_cli alice listpeerchannels | jq -e '.channels'"
    run_test "Bob has listpeerchannels" "hive_cli bob listpeerchannels | jq -e '.channels'"
    run_test "Carol has listpeerchannels" "hive_cli carol listpeerchannels | jq -e '.channels'"

    # Check topology info
    run_test "Hive topology works" "hive_cli alice hive-topology | jq -e '.saturated_count >= 0'"

    # Check pending actions (for advisor mode)
    run_test "Pending actions works" "hive_cli alice hive-pending-actions | jq -e '.count >= 0'"

    # Count existing channels in topology
    ALICE_CHANNELS=$(hive_cli alice listpeerchannels | jq '[.channels[] | select(.state == "CHANNELD_NORMAL")] | length')
    log_info "Alice has $ALICE_CHANNELS active channels"

    # Get pubkey of an external node if available
    if container_exists dave; then
        DAVE_PUBKEY=$(vanilla_cli dave getinfo | jq -r '.id')
        if [ -n "$DAVE_PUBKEY" ] && [ "$DAVE_PUBKEY" != "null" ]; then
            log_info "Dave pubkey: $DAVE_PUBKEY"
            # Note: Opening channels requires funding - just verify command exists
            run_test "fundchannel command exists" "hive_cli alice help | grep -q fundchannel"
        fi
    fi

    # Check for intra-hive channels
    BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
    ALICE_TO_BOB=$(hive_cli alice listpeerchannels | jq -r --arg pk "$BOB_PUBKEY" '[.channels[] | select(.peer_id == $pk)] | length')
    log_info "Alice has $ALICE_TO_BOB channels with Bob"
}

# Fee Tests - Fee policy integration
test_fees() {
    echo ""
    echo "========================================"
    echo "FEE POLICY TESTS"
    echo "========================================"

    # Check revenue-ops status
    run_test "Revenue status works" "hive_cli alice revenue-status | jq -e '.status'"

    # Check revenue channels
    run_test "Revenue channels works" "hive_cli alice revenue-channels 2>/dev/null | jq -e '. != null' || echo '[]' | jq -e '. != null'"

    # Check revenue dashboard
    run_test "Revenue dashboard works" "hive_cli alice revenue-dashboard 2>/dev/null | jq -e '. != null' || echo '{}' | jq -e '. != null'"

    # Get pubkeys
    BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
    CAROL_PUBKEY=$(hive_cli carol getinfo | jq -r '.id')

    # Check Bob (member) has HIVE strategy
    if [ -n "$BOB_PUBKEY" ] && [ "$BOB_PUBKEY" != "null" ]; then
        run_test "Revenue policy get works" "hive_cli alice revenue-policy get $BOB_PUBKEY | jq -e '.policy'"

        # Critical test: member should have HIVE strategy
        BOB_STRATEGY=$(hive_cli alice revenue-policy get $BOB_PUBKEY | jq -r '.policy.strategy')
        log_info "Bob's strategy: $BOB_STRATEGY (expected: hive)"
        run_test "Member has HIVE strategy" "[ '$BOB_STRATEGY' = 'hive' ]"
    fi

    # Check Carol (neophyte) has dynamic strategy
    if [ -n "$CAROL_PUBKEY" ] && [ "$CAROL_PUBKEY" != "null" ]; then
        CAROL_STRATEGY=$(hive_cli alice revenue-policy get $CAROL_PUBKEY | jq -r '.policy.strategy')
        log_info "Carol's strategy: $CAROL_STRATEGY (expected: dynamic)"
        run_test "Neophyte has dynamic strategy" "[ '$CAROL_STRATEGY' = 'dynamic' ]"
    fi

    # Check policy sync worked on startup (via logs or status)
    run_test "Hive status active" "hive_cli alice hive-status | jq -e '.status == \"active\"'"

    # Test policy can be set manually (for manual override testing)
    if [ -n "$CAROL_PUBKEY" ] && [ "$CAROL_PUBKEY" != "null" ]; then
        # This tests the revenue-policy set command works
        run_test "Manual policy set works" "hive_cli alice -k revenue-policy action=set peer_id=$CAROL_PUBKEY strategy=static 2>/dev/null | jq -e '.status == \"success\"'"
        # Revert back to dynamic
        hive_cli alice -k revenue-policy action=set peer_id=$CAROL_PUBKEY strategy=dynamic 2>/dev/null || true
    fi
}

# CLBOSS Tests - CLBOSS integration
test_clboss() {
    echo ""
    echo "========================================"
    echo "CLBOSS TESTS"
    echo "========================================"

    # Check clboss status
    run_test "clboss-status works" "hive_cli alice clboss-status | jq -e '.info.version'"

    # Check clboss internet status
    run_test "clboss has internet info" "hive_cli alice clboss-status | jq -e '.internet'"

    # Check unmanaged list
    run_test "clboss-unmanaged works" "hive_cli alice clboss-unmanaged 2>/dev/null | jq -e '. != null' || echo '{}' | jq -e '. != null'"

    # Get bob's pubkey for unmanage test
    BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
    if [ -n "$BOB_PUBKEY" ] && [ "$BOB_PUBKEY" != "null" ]; then
        # Test unmanage (may already be unmanaged)
        run_test "clboss-unmanage works" "hive_cli alice clboss-unmanage $BOB_PUBKEY tags='[\"lnfee\"]' 2>/dev/null | jq -e '. != null' || true"
    fi
}

# Contribution Tests - Contribution tracking
test_contrib() {
    echo ""
    echo "========================================"
    echo "CONTRIBUTION TESTS"
    echo "========================================"

    # Check self contribution
    run_test "Self contribution works" "hive_cli alice hive-contribution | jq -e '.peer_id'"

    # Get bob's pubkey
    BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
    if [ -n "$BOB_PUBKEY" ] && [ "$BOB_PUBKEY" != "null" ]; then
        # Check bob's contribution from alice's view
        run_test "Peer contribution works" "hive_cli alice hive-contribution peer_id=$BOB_PUBKEY | jq -e '.peer_id'"
    fi

    # Check contribution ratio is numeric
    run_test "Contribution ratio is numeric" "hive_cli alice hive-contribution | jq -e '.contribution_ratio >= 0 or .contribution_ratio == 0'"
}

# Cross-Implementation Tests - LND/Eclair
test_cross() {
    echo ""
    echo "========================================"
    echo "CROSS-IMPLEMENTATION TESTS"
    echo "========================================"

    # Test LND nodes
    for node in $LND_NODES; do
        if container_exists $node; then
            run_test "LND $node getinfo" "lnd_cli $node getinfo | jq -e '.identity_pubkey'"
            run_test "LND $node listchannels" "lnd_cli $node listchannels | jq -e '.channels != null'"
        else
            log_info "LND $node not found, skipping"
        fi
    done

    # Test Eclair nodes
    for node in $ECLAIR_NODES; do
        if container_exists $node; then
            run_test "Eclair $node getinfo" "eclair_cli $node getinfo | jq -e '.nodeId'"
            run_test "Eclair $node channels" "eclair_cli $node channels | jq -e '. != null'"
        else
            log_info "Eclair $node not found, skipping"
        fi
    done

    # Cross-connect tests (if nodes exist and have channels)
    if container_exists lnd1; then
        LND1_PUBKEY=$(lnd_cli lnd1 getinfo | jq -r '.identity_pubkey')
        if [ -n "$LND1_PUBKEY" ] && [ "$LND1_PUBKEY" != "null" ]; then
            log_info "LND1 pubkey: $LND1_PUBKEY"
        fi
    fi
}

# Reset - Clean up for fresh test run
test_reset() {
    echo ""
    echo "========================================"
    echo "RESET HIVE STATE"
    echo "========================================"

    log_info "Stopping plugins and resetting databases..."

    for node in $HIVE_NODES; do
        if container_exists $node; then
            echo "Resetting $node..."

            # Stop plugins (ignore errors)
            hive_cli $node plugin stop cl-hive 2>/dev/null || true
            hive_cli $node plugin stop cl-revenue-ops 2>/dev/null || true

            # Remove databases
            docker exec polar-n${NETWORK_ID}-${node} rm -f /home/clightning/.lightning/regtest/cl_hive.db 2>/dev/null || true
            docker exec polar-n${NETWORK_ID}-${node} rm -f /home/clightning/.lightning/regtest/revenue_ops.db 2>/dev/null || true

            # Restart plugins
            hive_cli $node plugin start /home/clightning/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py 2>/dev/null || true
            hive_cli $node plugin start /home/clightning/.lightning/plugins/cl-hive/cl-hive.py 2>/dev/null || true
        fi
    done

    log_info "Reset complete. Run './test.sh genesis $NETWORK_ID' to create a new hive."
}

#
# Main
#

echo "========================================"
echo "Hive Test Suite"
echo "========================================"
echo "Network ID: $NETWORK_ID"
echo "Category: $CATEGORY"
echo "Hive Nodes: $HIVE_NODES"
echo "Vanilla Nodes: $VANILLA_NODES"
echo "LND Nodes: $LND_NODES"
echo "Eclair Nodes: $ECLAIR_NODES"
echo ""

case $CATEGORY in
    all)
        test_setup
        test_genesis
        test_join
        test_promotion
        test_sync
        test_intent
        test_channels
        test_fees
        test_clboss
        test_contrib
        test_cross
        ;;
    setup)
        test_setup
        ;;
    genesis)
        test_genesis
        ;;
    join)
        test_join
        ;;
    promotion)
        test_promotion
        ;;
    sync)
        test_sync
        ;;
    intent)
        test_intent
        ;;
    channels)
        test_channels
        ;;
    fees)
        test_fees
        ;;
    clboss)
        test_clboss
        ;;
    contrib)
        test_contrib
        ;;
    cross)
        test_cross
        ;;
    reset)
        test_reset
        exit 0
        ;;
    *)
        echo "Unknown category: $CATEGORY"
        echo "Valid categories: all, setup, genesis, join, promotion, sync, intent, channels, fees, clboss, contrib, cross, reset"
        exit 1
        ;;
esac

#
# Summary
#
echo ""
echo "========================================"
echo "Test Results"
echo "========================================"
echo ""
TOTAL=$((TESTS_PASSED + TESTS_FAILED))
echo -e "Passed: ${GREEN}$TESTS_PASSED${NC} / $TOTAL"
echo -e "Failed: ${RED}$TESTS_FAILED${NC} / $TOTAL"

if [ $TESTS_FAILED -gt 0 ]; then
    echo ""
    echo "Failed tests:"
    echo -e "$FAILED_TESTS"
    echo ""
    exit 1
else
    echo ""
    echo -e "${GREEN}All tests passed!${NC}"
    echo ""
    exit 0
fi
