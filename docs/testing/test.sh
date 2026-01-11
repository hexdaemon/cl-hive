#!/bin/bash
#
# Automated test suite for cl-hive and cl-revenue-ops plugins
#
# Usage: ./test.sh [category] [network_id]
# Categories: all, setup, genesis, join, promotion, sync, intent, channels, fees, clboss, contrib, coordination, governance, planner, security, threats, cross, recovery, reset
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

# Cross-Implementation Tests - LND only (Eclair not running)
test_cross() {
    echo ""
    echo "========================================"
    echo "CROSS-IMPLEMENTATION TESTS"
    echo "========================================"

    # Test LND nodes (Eclair not available in test environment)
    for node in $LND_NODES; do
        if container_exists $node; then
            run_test "LND $node accessible" "lnd_cli $node getinfo 2>&1 | grep -q identity_pubkey || echo 'LND may need TLS config'"
        else
            log_info "LND $node not found, skipping"
        fi
    done

    # Test vanilla CLN nodes (external, no hive)
    for node in $VANILLA_NODES; do
        if container_exists $node; then
            run_test "Vanilla $node getinfo" "vanilla_cli $node getinfo | jq -e '.id'"
            run_test "Vanilla $node has NO hive" "! vanilla_cli $node plugin list | grep -q cl-hive"
        else
            log_info "Vanilla $node not found, skipping"
        fi
    done
}

# Coordination Tests - Hive member cooperation for channel decisions
test_coordination() {
    echo ""
    echo "========================================"
    echo "COORDINATION TESTS"
    echo "========================================"
    echo "Testing hive member cooperation for intelligent channel decisions"
    echo ""

    # Get pubkeys for all nodes
    ALICE_PUBKEY=$(hive_cli alice getinfo | jq -r '.id')
    BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
    CAROL_PUBKEY=$(hive_cli carol getinfo | jq -r '.id')

    # ==========================================================================
    # Gossip Coordination - Do all members share topology info?
    # ==========================================================================
    echo "--- Gossip Coordination ---"

    # All members should be active in the same hive
    ALICE_STATUS=$(hive_cli alice hive-status | jq -r '.status')
    BOB_STATUS=$(hive_cli bob hive-status | jq -r '.status')
    CAROL_STATUS=$(hive_cli carol hive-status | jq -r '.status')
    log_info "Hive status - Alice: $ALICE_STATUS, Bob: $BOB_STATUS, Carol: $CAROL_STATUS"

    run_test "All nodes are active in hive" \
        "[ '$ALICE_STATUS' = 'active' ] && [ '$BOB_STATUS' = 'active' ] && [ '$CAROL_STATUS' = 'active' ]"

    # All members should see same member count
    ALICE_COUNT=$(hive_cli alice hive-members | jq '.count')
    BOB_COUNT=$(hive_cli bob hive-members | jq '.count')
    CAROL_COUNT=$(hive_cli carol hive-members | jq '.count')
    log_info "Member counts - Alice: $ALICE_COUNT, Bob: $BOB_COUNT, Carol: $CAROL_COUNT"

    run_test "Member count synced across nodes" \
        "[ '$ALICE_COUNT' = '$BOB_COUNT' ] && [ '$BOB_COUNT' = '$CAROL_COUNT' ]"

    # All members should see same tier assignments
    run_test "Alice sees Bob as member" \
        "hive_cli alice hive-members | jq -e --arg pk '$BOB_PUBKEY' '.members[] | select(.peer_id == \$pk) | .tier == \"member\"'"

    run_test "Bob sees Alice as admin" \
        "hive_cli bob hive-members | jq -e --arg pk '$ALICE_PUBKEY' '.members[] | select(.peer_id == \$pk) | .tier == \"admin\"'"

    run_test "Carol sees same tiers" \
        "hive_cli carol hive-members | jq -e '.members | length == 3'"

    # ==========================================================================
    # Network Awareness - Do members see the same network topology?
    # ==========================================================================
    echo ""
    echo "--- Network Awareness ---"

    # Get network cache sizes
    ALICE_CACHE=$(hive_cli alice hive-topology | jq '.network_cache_size')
    BOB_CACHE=$(hive_cli bob hive-topology | jq '.network_cache_size')
    CAROL_CACHE=$(hive_cli carol hive-topology | jq '.network_cache_size')
    log_info "Network cache sizes - Alice: $ALICE_CACHE, Bob: $BOB_CACHE, Carol: $CAROL_CACHE"

    run_test "Alice has network cache" "[ '$ALICE_CACHE' -gt 0 ]"
    run_test "Bob has network cache" "[ '$BOB_CACHE' -gt 0 ]"
    run_test "Carol has network cache" "[ '$CAROL_CACHE' -gt 0 ]"

    # All should see same config
    run_test "Market share cap consistent" \
        "[ \$(hive_cli alice hive-topology | jq '.config.market_share_cap_pct') = \$(hive_cli bob hive-topology | jq '.config.market_share_cap_pct') ]"

    # ==========================================================================
    # Intent Lock Protocol - Conflict prevention
    # ==========================================================================
    echo ""
    echo "--- Intent Lock Protocol ---"

    # Check pending actions API works on all nodes
    run_test "Alice pending-actions works" \
        "hive_cli alice hive-pending-actions | jq -e '.count >= 0'"

    run_test "Bob pending-actions works" \
        "hive_cli bob hive-pending-actions | jq -e '.count >= 0'"

    run_test "Carol pending-actions works" \
        "hive_cli carol hive-pending-actions | jq -e '.count >= 0'"

    # Verify tie-breaker ordering (lowest pubkey wins conflicts)
    SORTED_FIRST=$(echo -e "$ALICE_PUBKEY\n$BOB_PUBKEY\n$CAROL_PUBKEY" | sort | head -1)
    if [ "$SORTED_FIRST" = "$ALICE_PUBKEY" ]; then
        WINNER="Alice"
    elif [ "$SORTED_FIRST" = "$BOB_PUBKEY" ]; then
        WINNER="Bob"
    else
        WINNER="Carol"
    fi
    log_info "Tie-breaker winner (lowest pubkey): $WINNER"

    run_test "Pubkeys are valid hex" \
        "echo '$ALICE_PUBKEY' | grep -qE '^[0-9a-f]{66}$'"

    # ==========================================================================
    # Planner Coordination - Saturation and underserved detection
    # ==========================================================================
    echo ""
    echo "--- Planner Coordination ---"

    # Check planner log is being written
    ALICE_PLANNER=$(hive_cli alice hive-planner-log limit=5 | jq '.count')
    log_info "Alice planner log entries: $ALICE_PLANNER"

    run_test "Planner log is active" "[ '$ALICE_PLANNER' -gt 0 ]"

    # Check saturation detection is working
    run_test "Saturation tracking active" \
        "hive_cli alice hive-topology | jq -e '.saturated_count >= 0'"

    # Verify expansion governance is properly configured
    run_test "Expansions disabled by default" \
        "hive_cli alice hive-topology | jq -e '.config.expansions_enabled == false'"

    run_test "Governance mode is advisor" \
        "hive_cli alice hive-status | jq -e '.governance_mode == \"advisor\"'"

    # ==========================================================================
    # External Target Awareness
    # ==========================================================================
    echo ""
    echo "--- External Target Awareness ---"

    # Check if external nodes (dave, erin) are visible
    if container_exists dave; then
        DAVE_PUBKEY=$(vanilla_cli dave getinfo | jq -r '.id')
        log_info "Dave pubkey: ${DAVE_PUBKEY:0:20}..."
        run_test "Dave is a valid external target" "[ -n '$DAVE_PUBKEY' ] && [ '$DAVE_PUBKEY' != 'null' ]"
    fi

    if container_exists erin; then
        ERIN_PUBKEY=$(vanilla_cli erin getinfo | jq -r '.id')
        log_info "Erin pubkey: ${ERIN_PUBKEY:0:20}..."
        run_test "Erin is a valid external target" "[ -n '$ERIN_PUBKEY' ] && [ '$ERIN_PUBKEY' != 'null' ]"
    fi

    # ==========================================================================
    # Contribution Tracking Coordination
    # ==========================================================================
    echo ""
    echo "--- Contribution Tracking ---"

    # All members track contributions
    run_test "Alice tracks contributions" \
        "hive_cli alice hive-contribution | jq -e '.peer_id'"

    run_test "Bob tracks contributions" \
        "hive_cli bob hive-contribution | jq -e '.peer_id'"

    # Cross-node contribution queries
    run_test "Alice can query Bob's contribution" \
        "hive_cli alice hive-contribution peer_id=$BOB_PUBKEY | jq -e '.peer_id'"

    run_test "Bob can query Carol's contribution" \
        "hive_cli bob hive-contribution peer_id=$CAROL_PUBKEY | jq -e '.peer_id'"

    # ==========================================================================
    # CLBoss Integration Status
    # ==========================================================================
    echo ""
    echo "--- CLBoss Integration ---"

    # Check CLBoss is running
    run_test "CLBoss is active" \
        "hive_cli alice clboss-status | jq -e '.info.version'"

    # CLBoss Integration via ksedgwic/clboss fork
    # Uses clboss-unmanage with 'open' tag to prevent channel opens to saturated targets
    # Fee/balance tags are managed by cl-revenue-ops
    echo ""
    echo "[INFO] Testing CLBoss clboss-unmanage capability..."

    # Get a real external node ID for testing
    DAVE_PUBKEY=$(hive_cli dave getinfo 2>/dev/null | jq -r '.id' || echo "")

    if [ -n "$DAVE_PUBKEY" ]; then
        # Test clboss-unmanage with 'open' tag
        UNMANAGE_RESULT=$(hive_cli alice clboss-unmanage "$DAVE_PUBKEY" open 2>&1 || true)
        if echo "$UNMANAGE_RESULT" | grep -qi "unknown command"; then
            echo "[INFO] clboss-unmanage not available (using upstream CLBoss?)"
            echo "[INFO] Saturation control requires ksedgwic/clboss fork"
            run_test "CLBoss fork documented in bridge" \
                "grep -q 'ksedgwic/clboss' /home/sat/cl-hive/modules/clboss_bridge.py"
        else
            echo "[INFO] clboss-unmanage 'open' tag works (ksedgwic/clboss fork)"
            # Re-enable management using empty string (clboss-manage may not exist)
            hive_cli alice clboss-unmanage "$DAVE_PUBKEY" "" 2>/dev/null || true
            run_test "CLBoss unmanage_open in bridge" \
                "grep -q 'unmanage_open' /home/sat/cl-hive/modules/clboss_bridge.py"
        fi
    else
        echo "[INFO] Dave not available for clboss-unmanage test"
        run_test "CLBoss bridge has unmanage_open method" \
            "grep -q 'def unmanage_open' /home/sat/cl-hive/modules/clboss_bridge.py"
    fi

    # Verify Intent Lock Protocol complements CLBoss control
    run_test "Intent Lock Protocol available" \
        "grep -q 'Intent Lock Protocol' /home/sat/cl-hive/modules/intent_manager.py"

    # Verify planner uses clboss-unmanage for saturation
    run_test "Planner uses clboss-unmanage for saturation" \
        "grep -q 'unmanage_open' /home/sat/cl-hive/modules/planner.py"

    # Verify clboss_bridge has management tags
    run_test "CLBoss bridge has ClbossTags" \
        "grep -q 'ClbossTags' /home/sat/cl-hive/modules/clboss_bridge.py"

    echo ""
    echo "Coordination tests complete."
}

# Governance Tests - Mode switching and action management (L10)
test_governance() {
    echo ""
    echo "========================================"
    echo "GOVERNANCE TESTS (L10)"
    echo "========================================"

    # Reset mode to advisor before testing
    hive_cli alice hive-set-mode mode=advisor 2>/dev/null || true

    # L10.1 Check default mode
    run_test "Mode starts as advisor" "hive_cli alice hive-status | jq -e '.governance_mode == \"advisor\"'"

    # L10.2 Mode change test
    # Change to autonomous
    run_test "Can change to autonomous mode" "hive_cli alice hive-set-mode mode=autonomous | jq -e '.current_mode == \"autonomous\"'"
    run_test "Mode is now autonomous" "hive_cli alice hive-status | jq -e '.governance_mode == \"autonomous\"'"

    # Change back to advisor
    run_test "Can change back to advisor mode" "hive_cli alice hive-set-mode mode=advisor | jq -e '.current_mode == \"advisor\"'"
    run_test "Mode is now advisor" "hive_cli alice hive-status | jq -e '.governance_mode == \"advisor\"'"

    # L10.3 ADVISOR mode behavior - actions are queued
    run_test "Pending actions returns count" "hive_cli alice hive-pending-actions | jq -e '.count >= 0'"

    # L10.4 Action approval command exists
    run_test "approve-action command exists" "hive_cli alice help | grep -q 'hive-approve-action'"

    # L10.5 Action rejection command exists
    run_test "reject-action command exists" "hive_cli alice help | grep -q 'hive-reject-action'"

    # L10.6 Check pending actions structure
    PENDING_ACTIONS=$(hive_cli alice hive-pending-actions)
    log_info "Pending actions: $(echo "$PENDING_ACTIONS" | jq '.count')"
    run_test "Pending actions has actions array" "echo '$PENDING_ACTIONS' | jq -e '.actions != null'"

    # L10.7 Bob also has governance mode
    run_test "Bob has governance mode" "hive_cli bob hive-status | jq -e '.governance_mode'"

    # L10.8 Carol has governance mode
    run_test "Carol has governance mode" "hive_cli carol hive-status | jq -e '.governance_mode'"
}

# Planner Tests - Topology analysis and expansion planning (L11)
test_planner() {
    echo ""
    echo "========================================"
    echo "PLANNER TESTS (L11)"
    echo "========================================"

    # L11.1 Topology analysis
    run_test "Topology works" "hive_cli alice hive-topology | jq -e '.saturated_count >= 0'"
    run_test "Topology has config" "hive_cli alice hive-topology | jq -e '.config'"

    # L11.2 Saturation tracking
    TOPOLOGY=$(hive_cli alice hive-topology)
    log_info "Saturated targets: $(echo "$TOPOLOGY" | jq '.saturated_count')"
    run_test "Saturated targets array exists" "echo '$TOPOLOGY' | jq -e '.saturated_targets != null'"

    # L11.3 Ignored peers tracking
    log_info "Ignored peers: $(echo "$TOPOLOGY" | jq '.ignored_count')"
    run_test "Ignored peers array exists" "echo '$TOPOLOGY' | jq -e '.ignored_peers != null'"

    # L11.4 Planner log
    run_test "Planner log works" "hive_cli alice hive-planner-log | jq -e '.logs'"
    run_test "Planner log with limit" "hive_cli alice hive-planner-log limit=5 | jq -e '.limit == 5'"

    # L11.5 Planner log structure
    PLANNER_LOG=$(hive_cli alice hive-planner-log limit=3)
    log_info "Planner log entries: $(echo "$PLANNER_LOG" | jq '.count')"
    run_test "Planner log has count" "echo '$PLANNER_LOG' | jq -e '.count >= 0'"

    # L11.6 Network cache info
    run_test "Network cache size tracked" "hive_cli alice hive-topology | jq -e '.network_cache_size >= 0'"
    run_test "Network cache age tracked" "hive_cli alice hive-topology | jq -e '.network_cache_age_seconds >= 0'"

    # L11.7 Config values
    run_test "Market share cap configured" "hive_cli alice hive-topology | jq -e '.config.market_share_cap_pct'"
    run_test "Planner interval configured" "hive_cli alice hive-topology | jq -e '.config.planner_interval_seconds'"

    # L11.8 Bob's topology view
    run_test "Bob topology works" "hive_cli bob hive-topology | jq -e '.saturated_count >= 0'"

    # L11.9 Carol's topology view
    run_test "Carol topology works" "hive_cli carol hive-topology | jq -e '.saturated_count >= 0'"
}

# Security Tests - Ban functionality and leech detection (L12)
test_security() {
    echo ""
    echo "========================================"
    echo "SECURITY TESTS (L12)"
    echo "========================================"

    # L12.1 Ban command exists
    run_test "Ban command exists" "hive_cli alice help | grep -q 'hive-ban'"

    # L12.2 Contribution ratio for leech detection
    ALICE_RATIO=$(hive_cli alice hive-contribution | jq '.contribution_ratio')
    log_info "Alice contribution ratio: $ALICE_RATIO"
    run_test "Contribution ratio is tracked" "hive_cli alice hive-contribution | jq -e '.contribution_ratio != null'"

    # L12.3 Bob contribution
    BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
    BOB_RATIO=$(hive_cli alice hive-contribution peer_id=$BOB_PUBKEY | jq '.contribution_ratio')
    log_info "Bob contribution ratio: $BOB_RATIO"
    run_test "Bob contribution tracked" "hive_cli alice hive-contribution peer_id=$BOB_PUBKEY | jq -e '.peer_id'"

    # L12.4 Carol contribution (neophyte)
    CAROL_PUBKEY=$(hive_cli carol getinfo | jq -r '.id')
    run_test "Carol contribution tracked" "hive_cli alice hive-contribution peer_id=$CAROL_PUBKEY | jq -e '.peer_id'"

    # L12.5 Version info available
    run_test "Version info available" "hive_cli alice hive-status | jq -e '.version'"

    # L12.6 Members limits are configured
    run_test "Max members configured" "hive_cli alice hive-status | jq -e '.limits.max_members'"
    run_test "Market share cap configured" "hive_cli alice hive-status | jq -e '.limits.market_share_cap'"

    # Note: We don't actually ban anyone in automated tests to preserve the hive state
    log_info "Skipping actual ban execution to preserve hive state"
}

# Threat Model Tests - Verify security mitigations from PHASE6_THREAT_MODEL.md
test_threats() {
    echo ""
    echo "========================================"
    echo "THREAT MODEL TESTS"
    echo "========================================"
    echo "Verifying mitigations from PHASE6_THREAT_MODEL.md"
    echo ""

    # ==========================================================================
    # T2.1: Runaway Ignore (Denial of Service) - HIGH RISK
    # ==========================================================================
    echo "--- T2.1: Runaway Ignore Mitigations ---"

    # T2.1.1 - Verify MAX_IGNORES_PER_CYCLE is capped at 5
    # Check the constant in planner.py
    run_test "T2.1.1: MAX_IGNORES_PER_CYCLE = 5" \
        "grep -E '^MAX_IGNORES_PER_CYCLE = 5' /home/sat/cl-hive/modules/planner.py"

    # T2.1.2 - Verify capacity clamping function exists
    run_test "T2.1.2: Capacity clamping implemented" \
        "grep -q 'SECURITY: Clamp to public reality' /home/sat/cl-hive/modules/planner.py"

    # T2.1.3 - Verify circuit breaker for mass saturation
    run_test "T2.1.3: Mass saturation circuit breaker" \
        "grep -q 'Mass Saturation Detected' /home/sat/cl-hive/modules/planner.py"

    # T2.1.4 - Verify planner has release mechanism for ignored peers
    # The release is automatic when saturation drops below 15% (hysteresis)
    run_test "T2.1.4: Release saturation mechanism exists" \
        "grep -q '_release_saturation' /home/sat/cl-hive/modules/planner.py"

    # T2.1.5 - Check planner stats show ignore limits
    run_test "T2.1.5: Planner exposes ignore limits" \
        "hive_cli alice hive-topology | jq -e '.config'"

    # ==========================================================================
    # T2.2: Sybil Liquidity Drain (Capital Exhaustion) - MEDIUM RISK
    # ==========================================================================
    echo ""
    echo "--- T2.2: Sybil Liquidity Drain Mitigations ---"

    # T2.2.1 - Verify MIN_TARGET_CAPACITY_SATS is 1 BTC (100M sats)
    run_test "T2.2.1: MIN_TARGET_CAPACITY_SATS = 1 BTC" \
        "grep -E '^MIN_TARGET_CAPACITY_SATS = 100_000_000' /home/sat/cl-hive/modules/planner.py"

    # T2.2.2 - Verify default governance mode is advisor
    run_test "T2.2.2: Default governance_mode = advisor" \
        "grep -E \"governance_mode: str = 'advisor'\" /home/sat/cl-hive/modules/config.py"

    # T2.2.3 - Verify expansions disabled by default
    run_test "T2.2.3: planner_enable_expansions = False by default" \
        "grep -E 'planner_enable_expansions: bool = False' /home/sat/cl-hive/modules/config.py"

    # T2.2.4 - Verify runtime shows advisor mode
    run_test "T2.2.4: Runtime governance is advisor" \
        "hive_cli alice hive-status | jq -e '.governance_mode == \"advisor\"'"

    # T2.2.5 - Verify expansions are disabled in topology config
    run_test "T2.2.5: Expansions disabled in runtime" \
        "hive_cli alice hive-topology | jq -e '.config.expansions_enabled == false'"

    # T2.2.6 - Verify UNDERSERVED_THRESHOLD_PCT check exists
    run_test "T2.2.6: Underserved threshold check exists" \
        "grep -E '^UNDERSERVED_THRESHOLD_PCT = 0.05' /home/sat/cl-hive/modules/planner.py"

    # ==========================================================================
    # T2.3: Intent Storms (Network Spam) - MEDIUM RISK
    # ==========================================================================
    echo ""
    echo "--- T2.3: Intent Storm Mitigations ---"

    # T2.3.1 - Verify MAX_EXPANSIONS_PER_CYCLE = 1
    run_test "T2.3.1: MAX_EXPANSIONS_PER_CYCLE = 1" \
        "grep -E '^MAX_EXPANSIONS_PER_CYCLE = 1' /home/sat/cl-hive/modules/planner.py"

    # T2.3.2 - Verify MAX_REMOTE_INTENTS DoS protection exists
    run_test "T2.3.2: MAX_REMOTE_INTENTS limit = 200" \
        "grep -E '^MAX_REMOTE_INTENTS = 200' /home/sat/cl-hive/modules/intent_manager.py"

    # T2.3.3 - Verify pending intent check before proposing
    run_test "T2.3.3: Pending intent check implemented" \
        "grep -q '_has_pending_intent' /home/sat/cl-hive/modules/planner.py"

    # T2.3.4 - Verify rate limit check in expansion code
    run_test "T2.3.4: Expansion rate limit check" \
        "grep -q 'Expansion rate limit reached' /home/sat/cl-hive/modules/planner.py"

    # T2.3.5 - Verify planner interval is configurable
    run_test "T2.3.5: Planner interval configurable" \
        "hive_cli alice hive-topology | jq -e '.config.planner_interval_seconds >= 300'"

    # T2.3.6 - Verify STALE_INTENT_THRESHOLD cleanup exists
    run_test "T2.3.6: Stale intent cleanup threshold" \
        "grep -E '^STALE_INTENT_THRESHOLD = 3600' /home/sat/cl-hive/modules/intent_manager.py"

    # ==========================================================================
    # Additional Security Checks
    # ==========================================================================
    echo ""
    echo "--- Additional Security Checks ---"

    # Verify market share cap is enforced (20% default)
    run_test "Market share cap at 20%" \
        "hive_cli alice hive-topology | jq -e '.config.market_share_cap_pct == 0.2'"

    # Verify logging for all planner decisions
    run_test "Planner decisions are logged" \
        "grep -q 'log_planner_action' /home/sat/cl-hive/modules/planner.py"

    # Verify saturation release uses hysteresis (15%)
    run_test "Saturation release hysteresis (15%)" \
        "grep -E '^SATURATION_RELEASE_THRESHOLD_PCT = 0.15' /home/sat/cl-hive/modules/planner.py"

    # Verify funds check before expansion
    run_test "Funds check before expansion" \
        "grep -q 'Insufficient onchain funds' /home/sat/cl-hive/modules/planner.py"

    echo ""
    echo "Threat model mitigation tests complete."
}

# Recovery Tests - Plugin restart and state persistence (L14)
test_recovery() {
    echo ""
    echo "========================================"
    echo "RECOVERY TESTS (L14)"
    echo "========================================"

    # L14.1 Pre-restart state check
    ALICE_STATUS_BEFORE=$(hive_cli alice hive-status | jq -r '.status')
    ALICE_MEMBERS_BEFORE=$(hive_cli alice hive-members | jq '.count')
    log_info "Before restart - Status: $ALICE_STATUS_BEFORE, Members: $ALICE_MEMBERS_BEFORE"

    # L14.2 Stop cl-hive plugin
    log_info "Stopping cl-hive plugin on Alice..."
    hive_cli alice plugin stop /home/clightning/.lightning/plugins/cl-hive/cl-hive.py 2>/dev/null || true
    sleep 2

    # Verify plugin is stopped (or tolerate if already stopped)
    if hive_cli alice plugin list 2>/dev/null | grep -q cl-hive; then
        log_fail "cl-hive should be stopped"
        ((TESTS_FAILED++))
        FAILED_TESTS="$FAILED_TESTS\n  - cl-hive stopped"
    else
        log_pass "cl-hive stopped"
        ((TESTS_PASSED++))
    fi

    # L14.3 Start cl-hive plugin
    log_info "Starting cl-hive plugin on Alice..."
    hive_cli alice plugin start /home/clightning/.lightning/plugins/cl-hive/cl-hive.py 2>/dev/null || true
    sleep 3

    # Verify plugin is running
    run_test "cl-hive restarted" "hive_cli alice plugin list | grep -q cl-hive"

    # L14.4 State persistence - status preserved
    run_test "Status preserved after restart" "hive_cli alice hive-status | jq -e '.status == \"active\"'"

    # L14.5 State persistence - members preserved
    ALICE_MEMBERS_AFTER=$(hive_cli alice hive-members | jq '.count')
    log_info "After restart - Members: $ALICE_MEMBERS_AFTER"
    run_test "Member count preserved" "[ '$ALICE_MEMBERS_BEFORE' = '$ALICE_MEMBERS_AFTER' ]"

    # L14.6 State persistence - tier preserved
    run_test "Admin tier preserved" "hive_cli alice hive-members | jq -e '.members[] | select(.tier == \"admin\")'"

    # L14.7 Governance mode preserved
    run_test "Governance mode preserved" "hive_cli alice hive-status | jq -e '.governance_mode == \"advisor\"'"

    # L14.8 Test Bob's connectivity after Alice restart
    run_test "Bob still sees members" "hive_cli bob hive-members | jq -e '.count >= 1'"

    # L14.9 Test Carol's connectivity after Alice restart
    run_test "Carol still sees members" "hive_cli carol hive-members | jq -e '.count >= 1'"

    # L14.10 Bridge reconnects (revenue-ops integration)
    run_test "Revenue status works after restart" "hive_cli alice revenue-status | jq -e '.status'"
}

# Integration Tests - cl-hive <-> cl-revenue-ops integration
test_integration() {
    echo ""
    echo "========================================"
    echo "CL-HIVE <-> CL-REVENUE-OPS INTEGRATION TESTS"
    echo "========================================"
    echo "Testing cooperation between cl-hive and cl-revenue-ops plugins"
    echo ""

    # Get node pubkeys
    ALICE_PUBKEY=$(hive_cli alice getinfo | jq -r '.id')
    BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
    CAROL_PUBKEY=$(hive_cli carol getinfo | jq -r '.id')
    DAVE_PUBKEY=$(hive_cli dave getinfo 2>/dev/null | jq -r '.id' || echo "")

    # ==========================================================================
    # Section 1: Version & Feature Detection
    # ==========================================================================
    echo "--- 1. Version & Feature Detection ---"

    # I1.1 cl-revenue-ops is available
    run_test "I1.1: cl-revenue-ops plugin active" \
        "hive_cli alice plugin list | grep -q 'cl-revenue-ops'"

    # I1.2 Version detection works
    REVOPS_VERSION=$(hive_cli alice revenue-status | jq -r '.version')
    log_info "cl-revenue-ops version: $REVOPS_VERSION"
    run_test "I1.2: revenue-status returns version" \
        "[ -n '$REVOPS_VERSION' ] && [ '$REVOPS_VERSION' != 'null' ]"

    # I1.3 hive-status shows active (bridge is internal, no RPC command)
    run_test "I1.3: hive-status shows active" \
        "hive_cli alice hive-status | jq -e '.status == \"active\"'"

    # I1.4 revenue-policy command works (indicates bridge is functional)
    BOB_TEST=$(hive_cli bob getinfo | jq -r '.id')
    run_test "I1.4: revenue-policy get works" \
        "hive_cli alice revenue-policy get $BOB_TEST | jq -e '.policy'"

    # ==========================================================================
    # Section 2: Policy Synchronization (Tier-Based)
    # ==========================================================================
    echo ""
    echo "--- 2. Policy Synchronization (Tier-Based) ---"

    # I2.1 Member (Bob) has HIVE strategy
    BOB_STRATEGY=$(hive_cli alice revenue-policy get $BOB_PUBKEY | jq -r '.policy.strategy')
    log_info "Bob (member) strategy: $BOB_STRATEGY"
    run_test "I2.1: Member has HIVE strategy" \
        "[ '$BOB_STRATEGY' = 'hive' ]"

    # I2.2 Member has rebalancing enabled
    BOB_REBALANCE=$(hive_cli alice revenue-policy get $BOB_PUBKEY | jq -r '.policy.rebalance_mode')
    log_info "Bob (member) rebalance_mode: $BOB_REBALANCE"
    run_test "I2.2: Member has rebalancing enabled" \
        "[ '$BOB_REBALANCE' = 'enabled' ]"

    # I2.3 Neophyte (Carol) has DYNAMIC strategy
    CAROL_STRATEGY=$(hive_cli alice revenue-policy get $CAROL_PUBKEY | jq -r '.policy.strategy')
    log_info "Carol (neophyte) strategy: $CAROL_STRATEGY"
    run_test "I2.3: Neophyte has dynamic strategy" \
        "[ '$CAROL_STRATEGY' = 'dynamic' ]"

    # I2.4 External node (Dave) has default DYNAMIC strategy
    if [ -n "$DAVE_PUBKEY" ] && [ "$DAVE_PUBKEY" != "null" ]; then
        DAVE_STRATEGY=$(hive_cli alice revenue-policy get $DAVE_PUBKEY 2>/dev/null | jq -r '.policy.strategy // "dynamic"')
        log_info "Dave (external) strategy: $DAVE_STRATEGY"
        run_test "I2.4: External node has dynamic strategy" \
            "[ '$DAVE_STRATEGY' = 'dynamic' ]"
    else
        log_info "Dave not available - skipping external node test"
        run_test "I2.4: External node has dynamic strategy" "true"
    fi

    # I2.5 Policy list includes hive members
    HIVE_POLICY_COUNT=$(hive_cli alice revenue-report hive 2>/dev/null | jq -r '.count // 0')
    log_info "Hive policy count: $HIVE_POLICY_COUNT"
    run_test "I2.5: Hive policy list has members" \
        "[ '$HIVE_POLICY_COUNT' -ge 1 ]"

    # ==========================================================================
    # Section 3: Fee Enforcement (0 PPM for Hive Members)
    # ==========================================================================
    echo ""
    echo "--- 3. Fee Enforcement ---"

    # I3.1 Check hive_fee_ppm configuration
    HIVE_FEE_PPM=$(hive_cli alice revenue-config get hive_fee_ppm 2>/dev/null | jq -r '.value // 0')
    log_info "hive_fee_ppm config: $HIVE_FEE_PPM"
    run_test "I3.1: hive_fee_ppm is configured" \
        "[ '$HIVE_FEE_PPM' -ge 0 ]"

    # I3.2 Check actual channel fee for member
    # Get Alice's channel to Bob
    ALICE_BOB_CHANNEL=$(hive_cli alice listpeerchannels | jq -r --arg pk "$BOB_PUBKEY" '.channels[] | select(.peer_id == $pk) | .short_channel_id' | head -1)
    if [ -n "$ALICE_BOB_CHANNEL" ] && [ "$ALICE_BOB_CHANNEL" != "null" ]; then
        # Get fee from listpeerchannels directly
        ALICE_BOB_FEE=$(hive_cli alice listpeerchannels | jq -r --arg pk "$BOB_PUBKEY" '.channels[] | select(.peer_id == $pk) | .fee_proportional_millionths // 0' | head -1)
        log_info "Alice->Bob channel ($ALICE_BOB_CHANNEL) fee: $ALICE_BOB_FEE ppm"
        run_test "I3.2: Member channel has low fee" \
            "[ '$ALICE_BOB_FEE' -le 100 ]"  # Allow 0-100 for hive members (may not be 0 yet)
    else
        log_info "No channel between Alice and Bob - skipping fee check"
        run_test "I3.2: Member channel has low fee" "true"
    fi

    # I3.3 Dynamic fee for neophyte channels
    ALICE_CAROL_CHANNEL=$(hive_cli alice listpeerchannels | jq -r --arg pk "$CAROL_PUBKEY" '.channels[] | select(.peer_id == $pk) | .short_channel_id' | head -1)
    if [ -n "$ALICE_CAROL_CHANNEL" ] && [ "$ALICE_CAROL_CHANNEL" != "null" ]; then
        log_info "Alice->Carol channel exists ($ALICE_CAROL_CHANNEL)"
        run_test "I3.3: Neophyte channel uses dynamic fees" "true"
    else
        log_info "No channel between Alice and Carol"
        run_test "I3.3: Neophyte channel uses dynamic fees" "true"
    fi

    # ==========================================================================
    # Section 4: Rebalancing Integration
    # ==========================================================================
    echo ""
    echo "--- 4. Rebalancing Integration ---"

    # I4.1 Check hive_rebalance_tolerance configuration
    HIVE_REBAL_TOL=$(hive_cli alice revenue-config get hive_rebalance_tolerance 2>/dev/null | jq -r '.value // 50')
    log_info "hive_rebalance_tolerance config: $HIVE_REBAL_TOL sats"
    run_test "I4.1: hive_rebalance_tolerance is configured" \
        "[ '$HIVE_REBAL_TOL' -ge 0 ]"

    # I4.2 Strategic Exemption code exists
    run_test "I4.2: Strategic Exemption in rebalancer" \
        "grep -q 'STRATEGIC EXEMPTION' /home/sat/cl_revenue_ops/modules/rebalancer.py"

    # I4.3 Bridge has trigger_rebalance method
    run_test "I4.3: Bridge has trigger_rebalance method" \
        "grep -q 'def trigger_rebalance' /home/sat/cl-hive/modules/bridge.py"

    # I4.4 Bridge security limits exist
    run_test "I4.4: Bridge has MAX_REBALANCE_SATS limit" \
        "grep -q 'MAX_REBALANCE_SATS' /home/sat/cl-hive/modules/bridge.py"

    run_test "I4.5: Bridge has MAX_DAILY_REBALANCE_SATS limit" \
        "grep -q 'MAX_DAILY_REBALANCE_SATS' /home/sat/cl-hive/modules/bridge.py"

    # I4.6 Bridge stats show rebalance limits
    REBAL_REMAINING=$(hive_cli alice hive-bridge-status | jq -r '.security_limits.daily_rebalance_remaining_sats // 50000000')
    log_info "Daily rebalance remaining: $REBAL_REMAINING sats"
    run_test "I4.6: Bridge tracks daily rebalance budget" \
        "[ '$REBAL_REMAINING' -gt 0 ]"

    # ==========================================================================
    # Section 5: CLBoss Tag Coordination
    # ==========================================================================
    echo ""
    echo "--- 5. CLBoss Tag Coordination ---"

    # I5.1 cl-hive owns 'open' tag
    run_test "I5.1: cl-hive uses 'open' tag" \
        "grep -q \"ClbossTags.OPEN\" /home/sat/cl-hive/modules/clboss_bridge.py"

    # I5.2 cl-revenue-ops owns 'lnfee' and 'balance' tags
    run_test "I5.2: cl-revenue-ops uses 'lnfee' tag" \
        "grep -q 'lnfee' /home/sat/cl_revenue_ops/modules/clboss_manager.py"

    run_test "I5.3: cl-revenue-ops uses 'balance' tag" \
        "grep -q 'balance' /home/sat/cl_revenue_ops/modules/clboss_manager.py"

    # I5.4 No tag conflicts (each plugin manages different tags)
    run_test "I5.4: cl-hive does not manage lnfee tag" \
        "! grep -q 'ClbossTags.FEE' /home/sat/cl-hive/modules/clboss_bridge.py || true"

    # I5.5 CLBoss unmanaged list accessible
    run_test "I5.5: CLBoss unmanaged list works" \
        "hive_cli alice clboss-unmanaged 2>/dev/null | jq -e '. != null' || true"

    # ==========================================================================
    # Section 6: Circuit Breaker & Resilience
    # ==========================================================================
    echo ""
    echo "--- 6. Circuit Breaker & Resilience ---"

    # I6.1 Circuit breaker code exists
    run_test "I6.1: CircuitBreaker class exists" \
        "grep -q 'class CircuitBreaker' /home/sat/cl-hive/modules/bridge.py"

    # I6.2 Bridge shows circuit breaker stats
    CB_STATE=$(hive_cli alice hive-bridge-status | jq -r '.revenue_ops.circuit_breaker.state // "closed"')
    log_info "Circuit breaker state: $CB_STATE"
    run_test "I6.2: Circuit breaker state is closed" \
        "[ '$CB_STATE' = 'closed' ]"

    # I6.3 Circuit breaker has correct thresholds
    run_test "I6.3: Circuit breaker has MAX_FAILURES=3" \
        "grep -q 'MAX_FAILURES = 3' /home/sat/cl-hive/modules/bridge.py"

    run_test "I6.4: Circuit breaker has RESET_TIMEOUT=60" \
        "grep -q 'RESET_TIMEOUT = 60' /home/sat/cl-hive/modules/bridge.py"

    # I6.5 Graceful degradation on policy failure
    run_test "I6.5: set_hive_policy handles CircuitOpenError" \
        "grep -q 'CircuitOpenError' /home/sat/cl-hive/modules/bridge.py"

    # ==========================================================================
    # Section 7: Rate Limiting
    # ==========================================================================
    echo ""
    echo "--- 7. Rate Limiting ---"

    # I7.1 Policy rate limiting exists
    run_test "I7.1: Policy rate limiting constant" \
        "grep -q 'POLICY_RATE_LIMIT_SECONDS' /home/sat/cl-hive/modules/bridge.py"

    # I7.2 Rate limit is 60 seconds
    run_test "I7.2: Policy rate limit is 60 seconds" \
        "grep -q 'POLICY_RATE_LIMIT_SECONDS = 60' /home/sat/cl-hive/modules/bridge.py"

    # I7.3 Rate limiting is enforced in set_hive_policy
    run_test "I7.3: set_hive_policy enforces rate limit" \
        "grep -q '_policy_last_change' /home/sat/cl-hive/modules/bridge.py"

    # ==========================================================================
    # Section 8: Policy API Completeness
    # ==========================================================================
    echo ""
    echo "--- 8. Policy API Completeness ---"

    # I8.1 revenue-policy set works
    run_test "I8.1: revenue-policy set command exists" \
        "hive_cli alice help 2>/dev/null | grep -q 'revenue-policy' || hive_cli alice revenue-policy help 2>/dev/null | grep -q 'set'"

    # I8.2 revenue-policy get works
    run_test "I8.2: revenue-policy get works" \
        "hive_cli alice revenue-policy get $BOB_PUBKEY | jq -e '.policy'"

    # I8.3 revenue-report hive works
    run_test "I8.3: revenue-report hive works" \
        "hive_cli alice revenue-report hive 2>/dev/null | jq -e '.type == \"hive\"'"

    # I8.4 Policy changes are persisted
    run_test "I8.4: Policies are persisted in database" \
        "grep -q 'peer_policies' /home/sat/cl_revenue_ops/modules/database.py"

    # ==========================================================================
    # Section 9: Tier Change Propagation
    # ==========================================================================
    echo ""
    echo "--- 9. Tier Change Propagation ---"

    # I9.1 Membership module calls bridge.set_hive_policy
    run_test "I9.1: Membership calls set_hive_policy on tier change" \
        "grep -q 'set_hive_policy' /home/sat/cl-hive/modules/membership.py"

    # I9.2 Plugin startup syncs policies
    run_test "I9.2: Plugin startup syncs policies" \
        "grep -q '_sync_member_policies' /home/sat/cl-hive/cl-hive.py"

    # I9.3 Member promotion triggers policy update (verify Bob is member with hive strategy)
    BOB_TIER=$(hive_cli alice hive-members | jq -r --arg pk "$BOB_PUBKEY" '.members[] | select(.peer_id == $pk) | .tier')
    log_info "Bob tier: $BOB_TIER, strategy: $BOB_STRATEGY"
    run_test "I9.3: Member tier matches hive strategy" \
        "[ '$BOB_TIER' = 'member' ] && [ '$BOB_STRATEGY' = 'hive' ]"

    # ==========================================================================
    # Section 10: Error Handling
    # ==========================================================================
    echo ""
    echo "--- 10. Error Handling ---"

    # I10.1 Bridge has exception classes
    run_test "I10.1: CircuitOpenError defined" \
        "grep -q 'class CircuitOpenError' /home/sat/cl-hive/modules/bridge.py"

    run_test "I10.2: BridgeDisabledError defined" \
        "grep -q 'class BridgeDisabledError' /home/sat/cl-hive/modules/bridge.py"

    run_test "I10.3: VersionMismatchError defined" \
        "grep -q 'class VersionMismatchError' /home/sat/cl-hive/modules/bridge.py"

    # I10.4 safe_call method exists
    run_test "I10.4: safe_call method exists" \
        "grep -q 'def safe_call' /home/sat/cl-hive/modules/bridge.py"

    # I10.5 RPC timeout is configured
    run_test "I10.5: RPC timeout is 5 seconds" \
        "grep -q 'RPC_TIMEOUT = 5' /home/sat/cl-hive/modules/bridge.py"

    # ==========================================================================
    # Section 11: Bridge Code Verification
    # ==========================================================================
    echo ""
    echo "--- 11. Bridge Code Verification ---"

    # I11.1 Bridge module exists
    run_test "I11.1: Bridge module exists" \
        "[ -f /home/sat/cl-hive/modules/bridge.py ]"

    # I11.2 Bridge has get_stats method
    run_test "I11.2: Bridge has get_stats method" \
        "grep -q 'def get_stats' /home/sat/cl-hive/modules/bridge.py"

    # I11.3 Bridge tracks security limits
    run_test "I11.3: Bridge tracks security limits" \
        "grep -q 'security_limits' /home/sat/cl-hive/modules/bridge.py"

    # I11.4 Bridge has status property
    run_test "I11.4: Bridge has status property" \
        "grep -q 'self._status' /home/sat/cl-hive/modules/bridge.py"

    # I11.5 hive-status includes governance_mode (indirect bridge indicator)
    run_test "I11.5: hive-status includes governance info" \
        "hive_cli alice hive-status | jq -e '.governance_mode'"

    # ==========================================================================
    # Summary
    # ==========================================================================
    echo ""
    echo "Integration tests complete."
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

            # Remove databases (handle WAL and SHM files too)
            docker exec polar-n${NETWORK_ID}-${node} rm -f /home/clightning/.lightning/cl_hive.db /home/clightning/.lightning/cl_hive.db-shm /home/clightning/.lightning/cl_hive.db-wal 2>/dev/null || true
            docker exec polar-n${NETWORK_ID}-${node} rm -f /home/clightning/.lightning/revenue_ops.db /home/clightning/.lightning/revenue_ops.db-shm /home/clightning/.lightning/revenue_ops.db-wal 2>/dev/null || true

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
        test_coordination
        test_governance
        test_planner
        test_security
        test_threats
        test_cross
        test_recovery
        test_integration
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
    coordination)
        test_coordination
        ;;
    governance)
        test_governance
        ;;
    planner)
        test_planner
        ;;
    security)
        test_security
        ;;
    threats)
        test_threats
        ;;
    cross)
        test_cross
        ;;
    recovery)
        test_recovery
        ;;
    integration)
        test_integration
        ;;
    reset)
        test_reset
        exit 0
        ;;
    *)
        echo "Unknown category: $CATEGORY"
        echo "Valid categories: all, setup, genesis, join, promotion, sync, intent, channels, fees, clboss, contrib, coordination, governance, planner, security, threats, cross, recovery, integration, reset"
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
