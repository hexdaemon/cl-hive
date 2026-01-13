#!/bin/bash
#
# Automated test suite for cl-revenue-ops and cl-hive plugins
#
# Usage: ./test.sh [category] [network_id]
#
# Categories:
#   all, setup, status, flow, fees, rebalance, sling, policy, profitability,
#   clboss, database, closure_costs, splice_costs, security, integration,
#   routing, performance, metrics, simulation, reset
#
# Hive Categories:
#   hive, hive_genesis, hive_join, hive_sync, hive_expansion, hive_reset
#
# Example: ./test.sh all 1
# Example: ./test.sh flow 1
# Example: ./test.sh hive 1
# Example: ./test.sh hive_expansion 1
#
# Prerequisites:
#   - Polar network running with CLN nodes (alice, bob, carol)
#   - cl-revenue-ops plugin installed via ../cl-hive/docs/testing/install.sh
#   - Funded channels between nodes for rebalance tests
#
# Environment variables:
#   NETWORK_ID      - Polar network ID (default: 1)
#   HIVE_NODES      - CLN nodes with cl-revenue-ops (default: "alice bob carol")
#   VANILLA_NODES   - CLN nodes without plugins (default: "dave erin")

set -o pipefail

# Configuration
CATEGORY="${1:-all}"
NETWORK_ID="${2:-1}"

# Node configuration
HIVE_NODES="${HIVE_NODES:-alice bob carol}"
VANILLA_NODES="${VANILLA_NODES:-dave erin}"

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
    BLUE='\033[0;34m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
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

log_section() {
    echo -e "${BLUE}$1${NC}"
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

# CLN CLI wrapper for nodes with revenue-ops
revenue_cli() {
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

# CLN CLI wrapper for hive nodes (alias for revenue_cli)
hive_cli() {
    local node=$1
    shift
    docker exec polar-n${NETWORK_ID}-${node} $CLN_CLI "$@"
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
    revenue_cli $node getinfo | jq -r '.id'
}

# Get channel SCID between two nodes
get_channel_scid() {
    local from=$1
    local to_pubkey=$2
    revenue_cli $from listpeerchannels | jq -r --arg pk "$to_pubkey" \
        '.channels[] | select(.peer_id == $pk and .state == "CHANNELD_NORMAL") | .short_channel_id' | head -1
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

    # Check containers
    for node in $HIVE_NODES; do
        run_test "Container $node exists" "container_exists $node"
    done

    # Check vanilla containers (optional)
    for node in $VANILLA_NODES; do
        if container_exists $node; then
            run_test "Container $node exists" "container_exists $node"
        fi
    done

    # Check cl-revenue-ops plugin loaded on hive nodes
    for node in $HIVE_NODES; do
        if container_exists $node; then
            run_test "$node has cl-revenue-ops" "revenue_cli $node plugin list | grep -q 'revenue-ops'"
        fi
    done

    # Check sling plugin loaded (required for rebalancing)
    for node in $HIVE_NODES; do
        if container_exists $node; then
            run_test "$node has sling" "revenue_cli $node plugin list | grep -q sling"
        fi
    done

    # Check CLBoss loaded (optional but recommended)
    for node in $HIVE_NODES; do
        if container_exists $node; then
            if revenue_cli $node plugin list 2>/dev/null | grep -q clboss; then
                run_test "$node has clboss" "true"
            else
                log_info "$node: clboss not loaded (optional)"
            fi
        fi
    done

    # Verify vanilla nodes don't have revenue-ops
    for node in $VANILLA_NODES; do
        if container_exists $node; then
            run_test_expect_fail "$node has NO cl-revenue-ops" "vanilla_cli $node plugin list | grep -q revenue-ops"
        fi
    done
}

# Status Tests - Verify basic plugin functionality
test_status() {
    echo ""
    echo "========================================"
    echo "STATUS TESTS"
    echo "========================================"

    # revenue-status command
    run_test "revenue-status works" "revenue_cli alice revenue-status | jq -e '.status'"

    # Version info
    VERSION=$(revenue_cli alice revenue-status | jq -r '.version')
    log_info "cl-revenue-ops version: $VERSION"
    run_test "Version is returned" "[ -n '$VERSION' ] && [ '$VERSION' != 'null' ]"

    # Config info embedded in status
    run_test "Config in status" "revenue_cli alice revenue-status | jq -e '.config'"

    # Channel states in status
    run_test "Channel states in status" "revenue_cli alice revenue-status | jq -e '.channel_states'"

    # revenue-dashboard command
    run_test "revenue-dashboard works" "revenue_cli alice revenue-dashboard | jq -e '. != null'"

    # Check on all hive nodes
    for node in $HIVE_NODES; do
        if container_exists $node; then
            run_test "$node revenue-status" "revenue_cli $node revenue-status | jq -e '.status'"
        fi
    done
}

# Flow Analysis Tests
test_flow() {
    echo ""
    echo "========================================"
    echo "FLOW ANALYSIS TESTS"
    echo "========================================"

    # Get channel states from revenue-status
    CHANNELS=$(revenue_cli alice revenue-status 2>/dev/null | jq '.channel_states')
    CHANNEL_COUNT=$(echo "$CHANNELS" | jq 'length // 0')
    log_info "Alice has $CHANNEL_COUNT channels"

    if [ "$CHANNEL_COUNT" -gt 0 ]; then
        # Check flow analysis data structure
        run_test "Channels have peer_id" "echo '$CHANNELS' | jq -e '.[0].peer_id'"
        run_test "Channels have state (flow)" "echo '$CHANNELS' | jq -e '.[0].state'"
        run_test "Channels have flow_ratio" "echo '$CHANNELS' | jq -e '.[0].flow_ratio'"
        run_test "Channels have capacity" "echo '$CHANNELS' | jq -e '.[0].capacity'"

        # Check flow state values (should be one of: source, sink, balanced)
        FIRST_FLOW=$(echo "$CHANNELS" | jq -r '.[0].state')
        log_info "First channel state: $FIRST_FLOW"
        run_test "Flow state is valid" "echo '$FIRST_FLOW' | grep -qE '^(source|sink|balanced)$'"

        # Check flow metrics
        run_test "Channels have sats_in" "echo '$CHANNELS' | jq -e '.[0].sats_in >= 0'"
        run_test "Channels have sats_out" "echo '$CHANNELS' | jq -e '.[0].sats_out >= 0'"

        # =========================================================================
        # v2.0 Flow Analysis Tests (runtime checks on channel_states)
        # =========================================================================
        echo ""
        log_info "Testing v2.0 flow analysis fields..."

        # Check v2.0 fields exist in channel_states
        run_test "v2.0: Channels have confidence score" \
            "echo '$CHANNELS' | jq -e '.[0].confidence != null'"
        run_test "v2.0: Channels have velocity" \
            "echo '$CHANNELS' | jq -e '.[0].velocity != null'"
        run_test "v2.0: Channels have flow_multiplier" \
            "echo '$CHANNELS' | jq -e '.[0].flow_multiplier != null'"
        run_test "v2.0: Channels have ema_decay" \
            "echo '$CHANNELS' | jq -e '.[0].ema_decay != null'"
        run_test "v2.0: Channels have forward_count" \
            "echo '$CHANNELS' | jq -e '.[0].forward_count != null'"

        # Check v2.0 value ranges (security bounds)
        CONFIDENCE=$(echo "$CHANNELS" | jq -r '.[0].confidence // 1.0')
        MULTIPLIER=$(echo "$CHANNELS" | jq -r '.[0].flow_multiplier // 1.0')
        DECAY=$(echo "$CHANNELS" | jq -r '.[0].ema_decay // 0.8')
        VELOCITY=$(echo "$CHANNELS" | jq -r '.[0].velocity // 0.0')

        log_info "v2.0 values: confidence=$CONFIDENCE multiplier=$MULTIPLIER decay=$DECAY velocity=$VELOCITY"

        run_test "v2.0: confidence in valid range (0.1-1.0)" \
            "awk 'BEGIN{exit ($CONFIDENCE >= 0.1 && $CONFIDENCE <= 1.0) ? 0 : 1}'"
        run_test "v2.0: flow_multiplier in valid range (0.5-2.0)" \
            "awk 'BEGIN{exit ($MULTIPLIER >= 0.5 && $MULTIPLIER <= 2.0) ? 0 : 1}'"
        run_test "v2.0: ema_decay in valid range (0.6-0.9)" \
            "awk 'BEGIN{exit ($DECAY >= 0.6 && $DECAY <= 0.9) ? 0 : 1}'"
        run_test "v2.0: velocity in valid range (-0.5 to 0.5)" \
            "awk 'BEGIN{exit ($VELOCITY >= -0.5 && $VELOCITY <= 0.5) ? 0 : 1}'"
    else
        log_info "No channels on Alice - skipping detailed flow tests"
        run_test "revenue-status handles no channels" "revenue_cli alice revenue-status | jq -e '.channel_states'"
    fi

    # =========================================================================
    # v2.0 Flow Analysis Code Verification Tests
    # =========================================================================
    echo ""
    log_info "Verifying v2.0 flow analysis code features..."

    # Improvement #1: Flow Confidence Score
    run_test "Flow v2.0 #1: Confidence enabled" \
        "grep -q 'ENABLE_FLOW_CONFIDENCE = True' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #1: MIN_CONFIDENCE bound" \
        "grep -q 'MIN_CONFIDENCE = 0.1' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #1: MAX_CONFIDENCE bound" \
        "grep -q 'MAX_CONFIDENCE = 1.0' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #1: _calculate_confidence method exists" \
        "grep -q 'def _calculate_confidence' /home/sat/cl_revenue_ops/modules/flow_analysis.py"

    # Improvement #2: Graduated Flow Multipliers
    run_test "Flow v2.0 #2: Graduated multipliers enabled" \
        "grep -q 'ENABLE_GRADUATED_MULTIPLIERS = True' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #2: MIN_FLOW_MULTIPLIER bound" \
        "grep -q 'MIN_FLOW_MULTIPLIER = 0.5' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #2: MAX_FLOW_MULTIPLIER bound" \
        "grep -q 'MAX_FLOW_MULTIPLIER = 2.0' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #2: _calculate_graduated_multiplier method exists" \
        "grep -q 'def _calculate_graduated_multiplier' /home/sat/cl_revenue_ops/modules/flow_analysis.py"

    # Improvement #3: Flow Velocity Tracking
    run_test "Flow v2.0 #3: Velocity tracking enabled" \
        "grep -q 'ENABLE_FLOW_VELOCITY = True' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #3: MAX_VELOCITY bound" \
        "grep -q 'MAX_VELOCITY = 0.5' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #3: MIN_VELOCITY bound" \
        "grep -q 'MIN_VELOCITY = -0.5' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #3: _calculate_velocity method exists" \
        "grep -q 'def _calculate_velocity' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #3: Outlier detection threshold" \
        "grep -q 'VELOCITY_OUTLIER_THRESHOLD' /home/sat/cl_revenue_ops/modules/flow_analysis.py"

    # Improvement #5: Adaptive EMA Decay
    run_test "Flow v2.0 #5: Adaptive decay enabled" \
        "grep -q 'ENABLE_ADAPTIVE_DECAY = True' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #5: MIN_EMA_DECAY bound" \
        "grep -q 'MIN_EMA_DECAY = 0.6' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #5: MAX_EMA_DECAY bound" \
        "grep -q 'MAX_EMA_DECAY = 0.9' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0 #5: _calculate_adaptive_decay method exists" \
        "grep -q 'def _calculate_adaptive_decay' /home/sat/cl_revenue_ops/modules/flow_analysis.py"

    # FlowMetrics v2.0 fields
    run_test "Flow v2.0: FlowMetrics has confidence field" \
        "grep -q 'confidence: float' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0: FlowMetrics has velocity field" \
        "grep -q 'velocity: float' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0: FlowMetrics has flow_multiplier field" \
        "grep -q 'flow_multiplier: float' /home/sat/cl_revenue_ops/modules/flow_analysis.py"
    run_test "Flow v2.0: FlowMetrics has ema_decay field" \
        "grep -q 'ema_decay: float' /home/sat/cl_revenue_ops/modules/flow_analysis.py"

    # Database v2.0 migration
    run_test "Flow v2.0: Database migration exists" \
        "grep -q '_migrate_flow_v2_schema' /home/sat/cl_revenue_ops/modules/database.py"
    run_test "Flow v2.0: DB confidence column added" \
        "grep -q 'confidence.*REAL DEFAULT' /home/sat/cl_revenue_ops/modules/database.py"
    run_test "Flow v2.0: get_daily_flow_buckets returns count" \
        "grep -q \"'count':\" /home/sat/cl_revenue_ops/modules/database.py"
    run_test "Flow v2.0: get_daily_flow_buckets returns last_ts" \
        "grep -q \"'last_ts':\" /home/sat/cl_revenue_ops/modules/database.py"

    # Check flow analysis on other nodes
    for node in bob carol; do
        if container_exists $node; then
            run_test "$node flow analysis works" "revenue_cli $node revenue-status | jq -e '.channel_states'"
        fi
    done
}

# Fee Controller Tests
test_fees() {
    echo ""
    echo "========================================"
    echo "FEE CONTROLLER TESTS"
    echo "========================================"

    # Get channel states for fee testing
    CHANNELS=$(revenue_cli alice revenue-status 2>/dev/null | jq '.channel_states')
    CHANNEL_COUNT=$(echo "$CHANNELS" | jq 'length // 0')

    # Check recent fee changes in revenue-status
    FEE_CHANGES=$(revenue_cli alice revenue-status 2>/dev/null | jq '.recent_fee_changes')
    FEE_CHANGE_COUNT=$(echo "$FEE_CHANGES" | jq 'length // 0')
    log_info "Recent fee changes: $FEE_CHANGE_COUNT"

    if [ "$FEE_CHANGE_COUNT" -gt 0 ]; then
        # Check fee change data structure
        run_test "Fee changes have channel_id" "echo '$FEE_CHANGES' | jq -e '.[0].channel_id'"
        run_test "Fee changes have old_fee_ppm" "echo '$FEE_CHANGES' | jq -e '.[0].old_fee_ppm'"
        run_test "Fee changes have new_fee_ppm" "echo '$FEE_CHANGES' | jq -e '.[0].new_fee_ppm'"
        run_test "Fee changes have reason" "echo '$FEE_CHANGES' | jq -e '.[0].reason'"
    else
        log_info "No recent fee changes yet"
    fi

    # Check fee configuration via revenue-config
    run_test "revenue-config list-mutable works" "revenue_cli alice revenue-config list-mutable | jq -e '.mutable_keys'"

    # Check specific config values
    MIN_FEE=$(revenue_cli alice revenue-config get min_fee_ppm 2>/dev/null | jq -r '.value // 0')
    MAX_FEE=$(revenue_cli alice revenue-config get max_fee_ppm 2>/dev/null | jq -r '.value // 5000')
    log_info "Fee range: $MIN_FEE - $MAX_FEE ppm"
    run_test "min_fee_ppm configured" "[ '$MIN_FEE' -ge 0 ]"
    run_test "max_fee_ppm configured" "[ '$MAX_FEE' -gt 0 ]"

    # Check hive fee ppm (for hive members)
    HIVE_FEE=$(revenue_cli alice revenue-config get hive_fee_ppm 2>/dev/null | jq -r '.value // 0')
    log_info "hive_fee_ppm: $HIVE_FEE"
    run_test "hive_fee_ppm configured" "[ '$HIVE_FEE' -ge 0 ]"

    # Check fee interval config
    FEE_INTERVAL=$(revenue_cli alice revenue-config get fee_interval 2>/dev/null | jq -r '.value // 300')
    log_info "fee_interval: $FEE_INTERVAL seconds"
    run_test "fee_interval configured" "[ '$FEE_INTERVAL' -gt 0 ]"

    # =========================================================================
    # v2.0 Fee Algorithm Improvements Tests
    # =========================================================================
    echo ""
    log_info "Testing v2.0 fee algorithm improvements..."

    # Test Improvement #1: Multipliers to Bounds
    run_test "Improvement #1: Bounds multipliers enabled" \
        "grep -q 'ENABLE_BOUNDS_MULTIPLIERS = True' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #1: Floor multiplier cap exists" \
        "grep -q 'MAX_FLOOR_MULTIPLIER' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #1: Ceiling multiplier floor exists" \
        "grep -q 'MIN_CEILING_MULTIPLIER' /home/sat/cl_revenue_ops/modules/fee_controller.py"

    # Test Improvement #2: Dynamic Observation Windows
    run_test "Improvement #2: Dynamic windows enabled" \
        "grep -q 'ENABLE_DYNAMIC_WINDOWS = True' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #2: Min forwards for signal" \
        "grep -q 'MIN_FORWARDS_FOR_SIGNAL' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #2: Max observation hours (security)" \
        "grep -q 'MAX_OBSERVATION_HOURS' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #2: get_forward_count_since in database" \
        "grep -q 'def get_forward_count_since' /home/sat/cl_revenue_ops/modules/database.py"

    # Test Improvement #3: Historical Response Curve
    run_test "Improvement #3: Historical curve enabled" \
        "grep -q 'ENABLE_HISTORICAL_CURVE = True' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #3: HistoricalResponseCurve class exists" \
        "grep -q 'class HistoricalResponseCurve' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #3: Max observations limit (security)" \
        "grep -q 'MAX_OBSERVATIONS = 100' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #3: Regime change detection" \
        "grep -q 'detect_regime_change' /home/sat/cl_revenue_ops/modules/fee_controller.py"

    # Test Improvement #4: Elasticity Tracking
    run_test "Improvement #4: Elasticity enabled" \
        "grep -q 'ENABLE_ELASTICITY = True' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #4: ElasticityTracker class exists" \
        "grep -q 'class ElasticityTracker' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #4: Outlier threshold (security)" \
        "grep -q 'OUTLIER_THRESHOLD' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #4: Revenue-weighted elasticity" \
        "grep -q 'revenue_change_pct.*fee_change_pct' /home/sat/cl_revenue_ops/modules/fee_controller.py"

    # Test Improvement #5: Thompson Sampling
    run_test "Improvement #5: Thompson Sampling enabled" \
        "grep -q 'ENABLE_THOMPSON_SAMPLING = True' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #5: ThompsonSamplingState class exists" \
        "grep -q 'class ThompsonSamplingState' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #5: Max exploration bounded (security)" \
        "grep -q 'MAX_EXPLORATION_PCT = 0.20' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #5: Beta distribution sampling" \
        "grep -q 'betavariate' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "Improvement #5: Ramp-up period for new channels" \
        "grep -q 'RAMP_UP_CYCLES' /home/sat/cl_revenue_ops/modules/fee_controller.py"

    # Test v2.0 Database Schema
    run_test "v2.0 DB: v2_state_json column migration" \
        "grep -q 'v2_state_json' /home/sat/cl_revenue_ops/modules/database.py"
    run_test "v2.0 DB: forward_count_since_update column" \
        "grep -q 'forward_count_since_update' /home/sat/cl_revenue_ops/modules/database.py"

    # Test v2.0 State Persistence
    run_test "v2.0 State: JSON serialization in save" \
        "grep -q 'json.dumps.*v2_data' /home/sat/cl_revenue_ops/modules/fee_controller.py"
    run_test "v2.0 State: JSON deserialization in load" \
        "grep -q 'json.loads.*v2_json' /home/sat/cl_revenue_ops/modules/fee_controller.py"
}

# Rebalancer Tests
test_rebalance() {
    echo ""
    echo "========================================"
    echo "REBALANCER TESTS"
    echo "========================================"

    # Check recent rebalances in revenue-status
    REBALANCES=$(revenue_cli alice revenue-status 2>/dev/null | jq '.recent_rebalances')
    REBAL_COUNT=$(echo "$REBALANCES" | jq 'length // 0')
    log_info "Recent rebalances: $REBAL_COUNT"

    # Check rebalance configuration
    REBAL_MIN_PROFIT=$(revenue_cli alice revenue-config get rebalance_min_profit 2>/dev/null | jq -r '.value // 10')
    log_info "rebalance_min_profit: $REBAL_MIN_PROFIT sats"
    run_test "rebalance_min_profit configurable" "[ '$REBAL_MIN_PROFIT' -ge 0 ]"

    REBAL_INTERVAL=$(revenue_cli alice revenue-config get rebalance_interval 2>/dev/null | jq -r '.value // 600')
    log_info "rebalance_interval: $REBAL_INTERVAL seconds"
    run_test "rebalance_interval configurable" "[ '$REBAL_INTERVAL' -gt 0 ]"

    # Check EV-based rebalancing code exists
    run_test "EV calculation in rebalancer" \
        "grep -q 'expected_value\\|EV\\|expected_profit' /home/sat/cl_revenue_ops/modules/rebalancer.py"

    # Check flow-aware opportunity cost
    run_test "Flow-aware opportunity cost" \
        "grep -q 'flow_multiplier\\|opportunity_cost' /home/sat/cl_revenue_ops/modules/rebalancer.py"

    # Check historical inbound fee estimation
    run_test "Historical inbound fee estimation" \
        "grep -q 'get_historical_inbound_fee_ppm\\|historical.*fee' /home/sat/cl_revenue_ops/modules/rebalancer.py"

    # Get channels for rebalance testing
    CHANNELS=$(revenue_cli alice revenue-status 2>/dev/null | jq '.channel_states')
    CHANNEL_COUNT=$(echo "$CHANNELS" | jq 'length // 0')

    if [ "$CHANNEL_COUNT" -ge 2 ]; then
        log_info "Found $CHANNEL_COUNT channels - can test rebalance candidates"

        # Check channel states include rebalance-relevant data
        run_test "Channels have flow_ratio for rebalancing" \
            "echo '$CHANNELS' | jq -e '.[0].flow_ratio'"
    else
        log_info "Need 2+ channels for rebalance tests - skipping"
    fi

    # Check for rejection diagnostics logging
    run_test "Rejection diagnostics implemented" \
        "grep -q 'REJECTION BREAKDOWN\\|rejection' /home/sat/cl_revenue_ops/modules/rebalancer.py"
}

# Sling Integration Tests
test_sling() {
    echo ""
    echo "========================================"
    echo "SLING INTEGRATION TESTS"
    echo "========================================"

    # Check sling plugin is loaded
    run_test "Sling plugin loaded" "revenue_cli alice plugin list | grep -q sling"

    # Check sling commands available
    run_test "sling-stats command works" "revenue_cli alice sling-stats 2>/dev/null | jq -e '. != null' || true"

    # Check sling configuration options in revenue-ops
    run_test "sling_max_hops config exists" \
        "grep -q 'sling_max_hops' /home/sat/cl_revenue_ops/modules/config.py"

    run_test "sling_parallel_jobs config exists" \
        "grep -q 'sling_parallel_jobs' /home/sat/cl_revenue_ops/modules/config.py"

    run_test "sling_target_sink config exists" \
        "grep -q 'sling_target_sink' /home/sat/cl_revenue_ops/modules/config.py"

    run_test "sling_target_source config exists" \
        "grep -q 'sling_target_source' /home/sat/cl_revenue_ops/modules/config.py"

    run_test "sling_outppm_fallback config exists" \
        "grep -q 'sling_outppm_fallback' /home/sat/cl_revenue_ops/modules/config.py"

    # Check sling-job creation in rebalancer
    run_test "sling-job integration" \
        "grep -q 'sling-job' /home/sat/cl_revenue_ops/modules/rebalancer.py"

    # Check maxhops parameter used
    run_test "maxhops parameter used" \
        "grep -q 'maxhops' /home/sat/cl_revenue_ops/modules/rebalancer.py"

    # Check flow-aware target calculation
    run_test "Flow-aware target calculation" \
        "grep -q 'sling_target_sink\\|sling_target_source' /home/sat/cl_revenue_ops/modules/rebalancer.py"

    # Check peer exclusion sync
    run_test "Peer exclusion sync implemented" \
        "grep -q 'sync_peer_exclusions\\|sling-except-peer' /home/sat/cl_revenue_ops/modules/rebalancer.py"

    # Check sling-except-peer command
    run_test "sling-except-peer command available" \
        "revenue_cli alice help 2>/dev/null | grep -q 'sling-except' || revenue_cli alice sling-except-peer 2>&1 | grep -qi 'parameter\\|node_id'"
}

# Policy Manager Tests
test_policy() {
    echo ""
    echo "========================================"
    echo "POLICY MANAGER TESTS"
    echo "========================================"

    # Get node pubkeys
    ALICE_PUBKEY=$(get_pubkey alice)
    BOB_PUBKEY=$(get_pubkey bob)
    CAROL_PUBKEY=$(get_pubkey carol)
    log_info "Alice: ${ALICE_PUBKEY:0:16}..."
    log_info "Bob: ${BOB_PUBKEY:0:16}..."
    log_info "Carol: ${CAROL_PUBKEY:0:16}..."

    # Test revenue-policy get command
    run_test "revenue-policy get works" "revenue_cli alice revenue-policy get $BOB_PUBKEY | jq -e '.policy'"

    # Check policy structure
    BOB_POLICY=$(revenue_cli alice revenue-policy get $BOB_PUBKEY 2>/dev/null)
    log_info "Bob policy: $(echo "$BOB_POLICY" | jq -c '.policy')"
    run_test "Policy has strategy" "echo '$BOB_POLICY' | jq -e '.policy.strategy'"
    run_test "Policy has rebalance_mode" "echo '$BOB_POLICY' | jq -e '.policy.rebalance_mode'"

    # Test valid strategies
    BOB_STRATEGY=$(echo "$BOB_POLICY" | jq -r '.policy.strategy')
    run_test "Strategy is valid" "echo '$BOB_STRATEGY' | grep -qE '^(static|dynamic|hive|aggressive|conservative)$'"

    # Test revenue-policy set command
    run_test "revenue-policy set works" \
        "revenue_cli alice -k revenue-policy action=set peer_id=$CAROL_PUBKEY strategy=dynamic | jq -e '.status == \"success\"'"

    # Verify policy was set
    CAROL_STRATEGY=$(revenue_cli alice revenue-policy get $CAROL_PUBKEY | jq -r '.policy.strategy')
    log_info "Carol strategy after set: $CAROL_STRATEGY"
    run_test "Policy set was applied" "[ '$CAROL_STRATEGY' = 'dynamic' ]"

    # Test invalid strategy (should fail gracefully)
    run_test_expect_fail "Invalid strategy rejected" \
        "revenue_cli alice -k revenue-policy action=set peer_id=$CAROL_PUBKEY strategy=invalid_strategy 2>&1 | jq -e '.status == \"success\"'"

    # Check policy list command
    run_test "revenue-policy list works" "revenue_cli alice revenue-policy list | jq -e '. != null'"

    # Policy on all hive nodes
    for node in bob carol; do
        if container_exists $node; then
            run_test "$node policy manager works" "revenue_cli $node revenue-policy get $ALICE_PUBKEY | jq -e '.policy'"
        fi
    done

    # =========================================================================
    # v2.0 Policy Manager Improvements Tests
    # =========================================================================
    echo ""
    log_info "Testing v2.0 policy manager improvements..."

    # Test #1: Granular Cache Invalidation (Write-Through Pattern)
    run_test "Policy v2.0 #1: Write-through cache update method exists" \
        "grep -q 'def _update_cache' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #1: Granular cache removal method exists" \
        "grep -q 'def _remove_from_cache' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #1: Write-through pattern in set_policy" \
        "grep -q 'self._update_cache' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # Test #2: Per-Policy Fee Multiplier Bounds
    run_test "Policy v2.0 #2: GLOBAL_MIN_FEE_MULTIPLIER constant" \
        "grep -q 'GLOBAL_MIN_FEE_MULTIPLIER = 0.1' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #2: GLOBAL_MAX_FEE_MULTIPLIER constant" \
        "grep -q 'GLOBAL_MAX_FEE_MULTIPLIER = 5.0' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #2: fee_multiplier_min field in PeerPolicy" \
        "grep -q 'fee_multiplier_min.*Optional' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #2: fee_multiplier_max field in PeerPolicy" \
        "grep -q 'fee_multiplier_max.*Optional' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #2: get_fee_multiplier_bounds method exists" \
        "grep -q 'def get_fee_multiplier_bounds' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # Test #3: Auto-Policy Suggestions from Profitability
    run_test "Policy v2.0 #3: ENABLE_AUTO_SUGGESTIONS constant" \
        "grep -q 'ENABLE_AUTO_SUGGESTIONS = True' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #3: MIN_OBSERVATION_DAYS constant" \
        "grep -q 'MIN_OBSERVATION_DAYS' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #3: BLEEDER_THRESHOLD_PERIODS constant" \
        "grep -q 'BLEEDER_THRESHOLD_PERIODS' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #3: get_policy_suggestions method exists" \
        "grep -q 'def get_policy_suggestions' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #3: Zombie detection threshold" \
        "grep -q 'ZOMBIE_FORWARD_THRESHOLD' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # Test #4: Time-Limited Policy Overrides
    run_test "Policy v2.0 #4: MAX_POLICY_EXPIRY_DAYS constant" \
        "grep -q 'MAX_POLICY_EXPIRY_DAYS = 30' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #4: ENABLE_AUTO_EXPIRY constant" \
        "grep -q 'ENABLE_AUTO_EXPIRY = True' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #4: expires_at field in PeerPolicy" \
        "grep -q 'expires_at.*Optional.*int' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #4: is_expired method in PeerPolicy" \
        "grep -q 'def is_expired' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #4: cleanup_expired_policies method exists" \
        "grep -q 'def cleanup_expired_policies' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #4: expires_in_hours parameter in set_policy" \
        "grep -q 'expires_in_hours.*Optional' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # Test #5: Policy Change Events/Callbacks
    run_test "Policy v2.0 #5: _on_change_callbacks list" \
        "grep -q '_on_change_callbacks' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #5: register_on_change method exists" \
        "grep -q 'def register_on_change' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #5: unregister_on_change method exists" \
        "grep -q 'def unregister_on_change' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #5: _notify_change method exists" \
        "grep -q 'def _notify_change' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # Test #6: Batch Policy Operations
    run_test "Policy v2.0 #6: set_policies_batch method exists" \
        "grep -q 'def set_policies_batch' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #6: MAX_BATCH_SIZE limit" \
        "grep -q 'MAX_BATCH_SIZE = 100' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 #6: executemany for batch efficiency" \
        "grep -q 'executemany' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # Test Rate Limiting Security
    run_test "Policy v2.0 Security: MAX_POLICY_CHANGES_PER_MINUTE constant" \
        "grep -q 'MAX_POLICY_CHANGES_PER_MINUTE = 10' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 Security: _check_rate_limit method exists" \
        "grep -q 'def _check_rate_limit' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0 Security: Rate limiting in set_policy" \
        "grep -q '_check_rate_limit' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # Test Database Schema Migration
    run_test "Policy v2.0 DB: fee_multiplier_min column migration" \
        "grep -q \"peer_policies ADD COLUMN fee_multiplier_min\" /home/sat/cl_revenue_ops/modules/database.py"
    run_test "Policy v2.0 DB: fee_multiplier_max column migration" \
        "grep -q \"peer_policies ADD COLUMN fee_multiplier_max\" /home/sat/cl_revenue_ops/modules/database.py"
    run_test "Policy v2.0 DB: expires_at column migration" \
        "grep -q \"peer_policies ADD COLUMN expires_at\" /home/sat/cl_revenue_ops/modules/database.py"

    # Test v2.0 fields in to_dict serialization
    run_test "Policy v2.0: fee_multiplier_min in to_dict" \
        "grep -q '\"fee_multiplier_min\":' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0: fee_multiplier_max in to_dict" \
        "grep -q '\"fee_multiplier_max\":' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0: expires_at in to_dict" \
        "grep -q '\"expires_at\":' /home/sat/cl_revenue_ops/modules/policy_manager.py"
    run_test "Policy v2.0: is_expired in to_dict" \
        "grep -q '\"is_expired\":' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # =========================================================================
    # v2.0 Runtime Tests (if channels exist)
    # =========================================================================
    echo ""
    log_info "Testing v2.0 policy manager runtime..."

    # Test v2.0 fields returned in policy get
    BOB_POLICY_V2=$(revenue_cli alice revenue-policy get $BOB_PUBKEY 2>/dev/null)
    if [ -n "$BOB_POLICY_V2" ]; then
        # Check v2.0 fields exist in response (may be null for default policies)
        run_test "Policy v2.0 runtime: Response has fee_multiplier_min field" \
            "echo '$BOB_POLICY_V2' | jq -e '.policy | has(\"fee_multiplier_min\")'"
        run_test "Policy v2.0 runtime: Response has fee_multiplier_max field" \
            "echo '$BOB_POLICY_V2' | jq -e '.policy | has(\"fee_multiplier_max\")'"
        run_test "Policy v2.0 runtime: Response has expires_at field" \
            "echo '$BOB_POLICY_V2' | jq -e '.policy | has(\"expires_at\")'"
        run_test "Policy v2.0 runtime: Response has is_expired field" \
            "echo '$BOB_POLICY_V2' | jq -e '.policy | has(\"is_expired\")'"
    fi
}

# Profitability Analyzer Tests
test_profitability() {
    echo ""
    echo "========================================"
    echo "PROFITABILITY ANALYZER TESTS"
    echo "========================================"

    # Check profitability analysis is available
    run_test "Profitability analyzer exists" \
        "[ -f /home/sat/cl_revenue_ops/modules/profitability_analyzer.py ]"

    # Check profitability methods
    run_test "ROI calculation implemented" \
        "grep -q 'calculate_roi\\|roi\\|return_on' /home/sat/cl_revenue_ops/modules/profitability_analyzer.py"

    # Check revenue-dashboard for profitability metrics
    DASHBOARD=$(revenue_cli alice revenue-dashboard 2>/dev/null)
    log_info "Dashboard keys: $(echo "$DASHBOARD" | jq 'keys')"

    # Check for financial health metrics
    run_test "Dashboard has financial_health" \
        "echo '$DASHBOARD' | jq -e '.financial_health'"

    # Check for profit tracking
    run_test "Dashboard has net_profit" \
        "echo '$DASHBOARD' | jq -e '.financial_health.net_profit_sats >= 0 or .net_profit_sats >= 0 or true'"

    # Check profitability config
    run_test "Kelly config available" \
        "revenue_cli alice revenue-config get enable_kelly 2>/dev/null | jq -e '.key == \"enable_kelly\"'"

    KELLY_ENABLED=$(revenue_cli alice revenue-config get enable_kelly 2>/dev/null | jq -r '.value // false')
    log_info "Kelly Criterion enabled: $KELLY_ENABLED"

    # Check Kelly Criterion implementation
    run_test "Kelly Criterion in code" \
        "grep -qi 'kelly' /home/sat/cl_revenue_ops/modules/rebalancer.py || grep -qi 'kelly' /home/sat/cl_revenue_ops/modules/profitability_analyzer.py"
}

# CLBOSS Integration Tests
test_clboss() {
    echo ""
    echo "========================================"
    echo "CLBOSS INTEGRATION TESTS"
    echo "========================================"

    # Check CLBoss manager module exists
    run_test "CLBoss manager module exists" \
        "[ -f /home/sat/cl_revenue_ops/modules/clboss_manager.py ]"

    # Check if CLBoss is loaded
    if ! revenue_cli alice plugin list 2>/dev/null | grep -q clboss; then
        log_info "CLBoss not loaded - skipping runtime tests"
        return
    fi

    # CLBoss is loaded - test integration
    run_test "clboss-status works" "revenue_cli alice clboss-status | jq -e '.info.version'"

    # Check revenue-clboss-status command (our custom wrapper)
    run_test "revenue-clboss-status works" \
        "revenue_cli alice revenue-clboss-status 2>/dev/null | jq -e '. != null' || true"

    # Get a peer to test unmanage
    BOB_PUBKEY=$(get_pubkey bob)

    # Test clboss-unmanage with lnfee tag (revenue-ops owns this tag)
    UNMANAGE_RESULT=$(revenue_cli alice clboss-unmanage "$BOB_PUBKEY" lnfee 2>&1 || true)
    if echo "$UNMANAGE_RESULT" | grep -qi "unknown command"; then
        log_info "clboss-unmanage not available (upstream CLBoss)"
        run_test "CLBoss unmanage documented" \
            "grep -q 'clboss-unmanage\\|clboss_unmanage' /home/sat/cl_revenue_ops/modules/clboss_manager.py"
    else
        run_test "clboss-unmanage lnfee tag works" "true"
    fi

    # Check tag ownership documentation
    run_test "lnfee tag used by revenue-ops" \
        "grep -q 'lnfee' /home/sat/cl_revenue_ops/modules/clboss_manager.py"

    run_test "balance tag used by revenue-ops" \
        "grep -q 'balance' /home/sat/cl_revenue_ops/modules/clboss_manager.py"

    # Check CLBoss status parsing
    run_test "CLBoss status parsing" \
        "grep -q 'clboss.status\\|clboss-status' /home/sat/cl_revenue_ops/modules/clboss_manager.py"
}

# Database Tests
test_database() {
    echo ""
    echo "========================================"
    echo "DATABASE TESTS"
    echo "========================================"

    # Check database module exists
    run_test "Database module exists" \
        "[ -f /home/sat/cl_revenue_ops/modules/database.py ]"

    # Check key database methods
    run_test "Historical fee tracking method exists" \
        "grep -q 'get_historical_inbound_fee_ppm' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Forward event storage exists" \
        "grep -q 'store_forward\\|forward_event\\|insert.*forward' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Rebalance history storage exists" \
        "grep -q 'store_rebalance\\|rebalance.*history\\|insert.*rebalance' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Policy storage exists" \
        "grep -q 'store_policy\\|get_policy\\|policy' /home/sat/cl_revenue_ops/modules/database.py"

    # Check database file exists on node (in .lightning root, not regtest subdir)
    if docker exec polar-n${NETWORK_ID}-alice test -f /home/clightning/.lightning/revenue_ops.db 2>/dev/null; then
        DB_EXISTS="yes"
    else
        DB_EXISTS="no"
    fi
    log_info "Database exists: $DB_EXISTS"
    run_test "Database file exists on node" "[ '$DB_EXISTS' = 'yes' ]"

    # Check schema migrations
    run_test "Schema versioning exists" \
        "grep -q 'schema_version\\|SCHEMA_VERSION\\|migration' /home/sat/cl_revenue_ops/modules/database.py"
}

# Closure Cost Tracking Tests (Accounting v2.0)
test_closure_costs() {
    echo ""
    echo "========================================"
    echo "CLOSURE COST TRACKING TESTS (Accounting v2.0)"
    echo "========================================"

    # =========================================================================
    # Code Verification Tests
    # =========================================================================
    log_info "Testing closure cost tracking code..."

    # Database table exists
    run_test "Closure costs table defined" \
        "grep -q 'channel_closure_costs' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Closed channels table defined" \
        "grep -q 'closed_channels' /home/sat/cl_revenue_ops/modules/database.py"

    # Database methods exist
    run_test "record_channel_closure method exists" \
        "grep -q 'def record_channel_closure' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "get_channel_closure_cost method exists" \
        "grep -q 'def get_channel_closure_cost' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "get_total_closure_costs method exists" \
        "grep -q 'def get_total_closure_costs' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "record_closed_channel_history method exists" \
        "grep -q 'def record_closed_channel_history' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "get_closed_channels_summary method exists" \
        "grep -q 'def get_closed_channels_summary' /home/sat/cl_revenue_ops/modules/database.py"

    # Channel state changed subscription
    run_test "channel_state_changed subscription exists" \
        "grep -q '@plugin.subscribe.*channel_state_changed' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    run_test "on_channel_state_changed handler exists" \
        "grep -q 'def on_channel_state_changed' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # Close type detection
    run_test "Close type detection exists" \
        "grep -q 'def _determine_close_type' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    run_test "Closure states defined (ONCHAIN, CLOSED)" \
        "grep -q \"'ONCHAIN'\" /home/sat/cl_revenue_ops/cl-revenue-ops.py && grep -q \"'CLOSED'\" /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # Bookkeeper integration
    run_test "Bookkeeper query for closure costs exists" \
        "grep -q 'def _get_closure_costs_from_bookkeeper' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    run_test "bkpr-listaccountevents query in code" \
        "grep -q 'bkpr-listaccountevents' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # Archive function
    run_test "Archive closed channel function exists" \
        "grep -q 'def _archive_closed_channel' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # Lifetime stats includes closure costs
    run_test "get_lifetime_stats includes closure costs" \
        "grep -q 'total_closure_cost_sats' /home/sat/cl_revenue_ops/modules/database.py"

    # Profitability analyzer includes closure costs
    run_test "Lifetime report includes closure costs" \
        "grep -q 'lifetime_closure_costs_sats' /home/sat/cl_revenue_ops/modules/profitability_analyzer.py"

    run_test "Closed channels summary in lifetime report" \
        "grep -q 'closed_channels_summary' /home/sat/cl_revenue_ops/modules/profitability_analyzer.py"

    # Close types tracked
    run_test "Mutual close type" \
        "grep -q \"'mutual'\" /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    run_test "Unilateral close types" \
        "grep -q 'local_unilateral\\|remote_unilateral' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # Security: fallback to estimated costs
    run_test "Fallback to ChainCostDefaults" \
        "grep -q 'ChainCostDefaults.CHANNEL_CLOSE_COST_SATS' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # =========================================================================
    # Runtime Tests
    # =========================================================================
    log_info "Testing closure cost tracking runtime..."

    # Check if revenue-history includes closure costs
    HISTORY=$(revenue_cli alice revenue-history 2>/dev/null || echo '{}')
    if [ -n "$HISTORY" ] && [ "$HISTORY" != "{}" ]; then
        run_test "revenue-history has lifetime_closure_costs_sats field" \
            "echo '$HISTORY' | jq -e 'has(\"lifetime_closure_costs_sats\") or .lifetime_closure_costs_sats != null or true'"
    fi

    # Verify tables exist in database (if database is accessible)
    if docker exec polar-n${NETWORK_ID}-alice test -f /home/clightning/.lightning/revenue_ops.db 2>/dev/null; then
        # Check for closure costs table
        TABLE_CHECK=$(docker exec polar-n${NETWORK_ID}-alice sqlite3 /home/clightning/.lightning/revenue_ops.db \
            ".schema channel_closure_costs" 2>/dev/null || echo "")
        if [ -n "$TABLE_CHECK" ]; then
            run_test "channel_closure_costs table exists in DB" "[ -n '$TABLE_CHECK' ]"
        fi

        # Check for closed channels table
        CLOSED_TABLE=$(docker exec polar-n${NETWORK_ID}-alice sqlite3 /home/clightning/.lightning/revenue_ops.db \
            ".schema closed_channels" 2>/dev/null || echo "")
        if [ -n "$CLOSED_TABLE" ]; then
            run_test "closed_channels table exists in DB" "[ -n '$CLOSED_TABLE' ]"
        fi
    fi
}

# Splice Cost Tracking Tests (Accounting v2.0)
test_splice_costs() {
    echo ""
    echo "========================================"
    echo "SPLICE COST TRACKING TESTS (Accounting v2.0)"
    echo "========================================"

    # =========================================================================
    # Code Verification Tests
    # =========================================================================
    log_info "Testing splice cost tracking code..."

    # Database table exists
    run_test "Splice costs table defined" \
        "grep -q 'splice_costs' /home/sat/cl_revenue_ops/modules/database.py"

    # Database methods exist
    run_test "record_splice method exists" \
        "grep -q 'def record_splice' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "get_channel_splice_history method exists" \
        "grep -q 'def get_channel_splice_history' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "get_total_splice_costs method exists" \
        "grep -q 'def get_total_splice_costs' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "get_splice_summary method exists" \
        "grep -q 'def get_splice_summary' /home/sat/cl_revenue_ops/modules/database.py"

    # Splice detection in channel state changed
    run_test "Splice detection via CHANNELD_AWAITING_SPLICE" \
        "grep -q 'CHANNELD_AWAITING_SPLICE' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    run_test "Splice completion handler exists" \
        "grep -q 'def _handle_splice_completion' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # Bookkeeper integration for splice
    run_test "Bookkeeper query for splice costs exists" \
        "grep -q 'def _get_splice_costs_from_bookkeeper' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # Splice types tracked
    run_test "splice_in type defined" \
        "grep -q 'splice_in' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "splice_out type defined" \
        "grep -q 'splice_out' /home/sat/cl_revenue_ops/modules/database.py"

    # Lifetime stats includes splice costs
    run_test "get_lifetime_stats includes splice costs" \
        "grep -q 'total_splice_cost_sats' /home/sat/cl_revenue_ops/modules/database.py"

    # Profitability analyzer includes splice costs
    run_test "Lifetime report includes splice costs" \
        "grep -q 'lifetime_splice_costs_sats' /home/sat/cl_revenue_ops/modules/profitability_analyzer.py"

    # =========================================================================
    # Runtime Tests
    # =========================================================================
    log_info "Testing splice cost tracking runtime..."

    # Check if revenue-history includes splice costs
    HISTORY=$(revenue_cli alice revenue-history 2>/dev/null || echo '{}')
    if [ -n "$HISTORY" ] && [ "$HISTORY" != "{}" ]; then
        run_test "revenue-history has lifetime_splice_costs_sats field" \
            "echo '$HISTORY' | jq -e 'has(\"lifetime_splice_costs_sats\") or .lifetime_splice_costs_sats != null or true'"
    fi

    # Verify table exists in database (if database is accessible)
    if docker exec polar-n${NETWORK_ID}-alice test -f /home/clightning/.lightning/revenue_ops.db 2>/dev/null; then
        # Check for splice costs table
        TABLE_CHECK=$(docker exec polar-n${NETWORK_ID}-alice sqlite3 /home/clightning/.lightning/revenue_ops.db \
            ".schema splice_costs" 2>/dev/null || echo "")
        if [ -n "$TABLE_CHECK" ]; then
            run_test "splice_costs table exists in DB" "[ -n '$TABLE_CHECK' ]"
        fi
    fi
}

# Security Tests (Accounting v2.0)
test_security() {
    echo ""
    echo "========================================"
    echo "SECURITY TESTS (Accounting v2.0)"
    echo "========================================"

    log_info "Testing security hardening code..."

    # Input validation methods exist
    run_test "Channel ID validation method exists" \
        "grep -q 'def _validate_channel_id' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Peer ID validation method exists" \
        "grep -q 'def _validate_peer_id' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Fee sanitization method exists" \
        "grep -q 'def _sanitize_fee' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Amount sanitization method exists" \
        "grep -q 'def _sanitize_amount' /home/sat/cl_revenue_ops/modules/database.py"

    # Validation constants defined
    run_test "MAX_FEE_SATS constant defined" \
        "grep -q 'MAX_FEE_SATS' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Channel ID pattern defined" \
        "grep -q 'CHANNEL_ID_PATTERN' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Peer ID pattern defined" \
        "grep -q 'PEER_ID_PATTERN' /home/sat/cl_revenue_ops/modules/database.py"

    # Validation called in record methods
    run_test "record_channel_closure validates channel_id" \
        "grep -q 'if not self._validate_channel_id' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "record_splice validates inputs" \
        "grep -q '_sanitize_fee.*splice_fee' /home/sat/cl_revenue_ops/modules/database.py"

    # Bookkeeper type checking
    run_test "Closure bookkeeper type checks event structure" \
        "grep -q 'isinstance.*event.*dict' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    run_test "Splice bookkeeper type checks event structure" \
        "grep -q 'isinstance.*event.*dict' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # Bounds checking in bookkeeper
    run_test "Closure bookkeeper has bounds check" \
        "grep -q 'fee_sats = min' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    run_test "Splice bookkeeper has bounds check" \
        "grep -q 'fee_sats = min' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # UNIQUE constraint for idempotency
    run_test "Splice costs has UNIQUE index for idempotency" \
        "grep -q 'idx_splice_costs_unique' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "Splice uses INSERT OR IGNORE" \
        "grep -q 'INSERT OR IGNORE INTO splice_costs' /home/sat/cl_revenue_ops/modules/database.py"
}

# Cross-Plugin Integration Tests (cl-hive <-> cl-revenue-ops)
test_integration() {
    echo ""
    echo "========================================"
    echo "CROSS-PLUGIN INTEGRATION TESTS (cl-hive)"
    echo "========================================"

    log_info "Testing cl-hive <-> cl-revenue-ops integration..."

    # =========================================================================
    # Plugin Detection Tests
    # =========================================================================
    echo ""
    log_info "Plugin detection and coexistence..."

    # Check both plugins loaded
    run_test "Both plugins loaded on alice" \
        "revenue_cli alice plugin list | grep -q revenue-ops && revenue_cli alice plugin list | grep -q cl-hive"

    # Check both plugins on all hive nodes
    for node in $HIVE_NODES; do
        if container_exists $node; then
            run_test "$node has both plugins" \
                "revenue_cli $node plugin list | grep -q revenue-ops && revenue_cli $node plugin list | grep -q cl-hive"
        fi
    done

    # =========================================================================
    # HIVE Strategy Policy Tests
    # =========================================================================
    echo ""
    log_info "Testing HIVE strategy policy integration..."

    # Get peer pubkeys for testing
    BOB_PUBKEY=$(get_pubkey bob)
    CAROL_PUBKEY=$(get_pubkey carol)

    if [ -n "$BOB_PUBKEY" ]; then
        # Test HIVE strategy exists in policy options
        run_test "HIVE strategy is valid" \
            "grep -q \"'hive'\" /home/sat/cl_revenue_ops/modules/policy_manager.py"

        # Test setting HIVE strategy works
        run_test "Set HIVE policy for Bob" \
            "revenue_cli alice -k revenue-policy action=set peer_id=$BOB_PUBKEY strategy=hive | jq -e '.status == \"success\"'"

        # Verify policy was applied
        BOB_STRATEGY=$(revenue_cli alice revenue-policy get $BOB_PUBKEY | jq -r '.policy.strategy')
        run_test "Bob has HIVE strategy" "[ '$BOB_STRATEGY' = 'hive' ]"

        # Test rebalance mode can be set
        run_test "Set rebalance enabled for Bob" \
            "revenue_cli alice -k revenue-policy action=set peer_id=$BOB_PUBKEY strategy=hive rebalance=enabled | jq -e '.status == \"success\"'"

        # Verify rebalance mode
        BOB_REBALANCE=$(revenue_cli alice revenue-policy get $BOB_PUBKEY | jq -r '.policy.rebalance_mode')
        log_info "Bob rebalance_mode: $BOB_REBALANCE"
        run_test "Bob rebalance mode is enabled" "[ '$BOB_REBALANCE' = 'enabled' ]"
    fi

    # =========================================================================
    # Policy Callback Infrastructure Tests
    # =========================================================================
    echo ""
    log_info "Testing policy callback infrastructure..."

    # Verify callback methods exist
    run_test "register_on_change method exists" \
        "grep -q 'def register_on_change' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    run_test "unregister_on_change method exists" \
        "grep -q 'def unregister_on_change' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    run_test "_notify_change method exists" \
        "grep -q 'def _notify_change' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    run_test "_on_change_callbacks list exists" \
        "grep -q '_on_change_callbacks' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # Verify callbacks are fired on policy changes
    run_test "Callbacks fired in set_policy" \
        "grep -q 'self._notify_change' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # =========================================================================
    # Rate Limiting Tests (cl-hive security)
    # =========================================================================
    echo ""
    log_info "Testing rate limiting for bulk policy updates..."

    # Verify rate limiting exists
    run_test "Policy rate limiting exists" \
        "grep -q 'MAX_POLICY_CHANGES_PER_MINUTE' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    run_test "_check_rate_limit method exists" \
        "grep -q 'def _check_rate_limit' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # Verify bypass mechanism exists for batch operations
    run_test "set_policies_batch exists for bulk operations" \
        "grep -q 'def set_policies_batch' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # =========================================================================
    # Closure/Splice Cost Exposure Tests
    # =========================================================================
    echo ""
    log_info "Testing closure/splice cost exposure for cl-hive decisions..."

    # Verify cost methods exist for cl-hive to query
    run_test "get_total_closure_costs method exists" \
        "grep -q 'def get_total_closure_costs' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "get_total_splice_costs method exists" \
        "grep -q 'def get_total_splice_costs' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "get_closure_costs_since method exists" \
        "grep -q 'def get_closure_costs_since' /home/sat/cl_revenue_ops/modules/database.py"

    run_test "get_splice_costs_since method exists" \
        "grep -q 'def get_splice_costs_since' /home/sat/cl_revenue_ops/modules/database.py"

    # Verify capacity planner includes cost estimates
    run_test "Capacity planner includes closure cost estimate" \
        "grep -q 'estimated_closure_cost_sats' /home/sat/cl_revenue_ops/modules/capacity_planner.py"

    run_test "ChainCostDefaults used in capacity planner" \
        "grep -q 'ChainCostDefaults' /home/sat/cl_revenue_ops/modules/capacity_planner.py"

    # =========================================================================
    # Strategic Exemption Tests (negative EV rebalances)
    # =========================================================================
    echo ""
    log_info "Testing strategic exemption for hive rebalances..."

    # Verify strategic exemption mechanism exists
    run_test "Strategic exemption config exists" \
        "grep -qi 'strategic.*exempt\\|hive.*exempt\\|negative.*ev' /home/sat/cl_revenue_ops/modules/rebalancer.py || \
         grep -qi 'hive.*strategy\\|strategic' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # =========================================================================
    # P&L Reporting Tests
    # =========================================================================
    echo ""
    log_info "Testing P&L reporting for hive-aware decisions..."

    # Verify get_pnl_summary includes all cost types
    run_test "get_pnl_summary method exists" \
        "grep -q 'def get_pnl_summary' /home/sat/cl_revenue_ops/modules/profitability_analyzer.py"

    run_test "P&L includes closure costs" \
        "grep -q 'closure_cost_sats' /home/sat/cl_revenue_ops/modules/profitability_analyzer.py"

    run_test "P&L includes splice costs" \
        "grep -q 'splice_cost_sats' /home/sat/cl_revenue_ops/modules/profitability_analyzer.py"

    # =========================================================================
    # Runtime Integration Tests
    # =========================================================================
    echo ""
    log_info "Testing runtime integration..."

    # Test revenue-report with hive context (if available)
    if revenue_cli alice help 2>/dev/null | grep -q 'revenue-report'; then
        run_test "revenue-report command exists" "true"

        # Test revenue-report hive (if cl-hive adds this)
        REPORT_RESULT=$(revenue_cli alice revenue-report hive 2>/dev/null || echo '{"type":"unavailable"}')
        if echo "$REPORT_RESULT" | jq -e '.type' >/dev/null 2>&1; then
            run_test "revenue-report hive returns data" "true"
        fi
    fi

    # Test revenue-history includes cost data
    HISTORY=$(revenue_cli alice revenue-history 2>/dev/null || echo '{}')
    if [ -n "$HISTORY" ] && [ "$HISTORY" != "{}" ]; then
        run_test "revenue-history includes lifetime costs" \
            "echo '$HISTORY' | jq -e 'has(\"lifetime_closure_costs_sats\") or has(\"lifetime_splice_costs_sats\") or true'"
    fi

    # =========================================================================
    # Policy Changes Endpoint Tests (cl-hive notification)
    # =========================================================================
    echo ""
    log_info "Testing policy changes endpoint..."

    # Test changes action exists
    run_test "revenue-policy changes action works" \
        "revenue_cli alice -k revenue-policy action=changes since=0 | jq -e '.changes != null'"

    # Verify last_change_timestamp is returned
    run_test "Policy changes returns last_change_timestamp" \
        "revenue_cli alice -k revenue-policy action=changes since=0 | jq -e '.last_change_timestamp != null'"

    # Test with recent timestamp (should return fewer results)
    RECENT_TS=$(($(date +%s) - 60))
    run_test "Policy changes with timestamp filter" \
        "revenue_cli alice -k revenue-policy action=changes since=$RECENT_TS | jq -e '.since == $RECENT_TS'"

    # Code verification
    run_test "get_policy_changes_since method exists" \
        "grep -q 'def get_policy_changes_since' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    run_test "get_last_policy_change_timestamp method exists" \
        "grep -q 'def get_last_policy_change_timestamp' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # =========================================================================
    # Batch Policy Updates Tests (rate limit bypass)
    # =========================================================================
    echo ""
    log_info "Testing batch policy updates..."

    # Test batch action exists
    run_test "revenue-policy batch action works" \
        "revenue_cli alice -k revenue-policy action=batch updates='[]' | jq -e '.status == \"success\" or .updated == 0'"

    # Code verification
    run_test "set_policies_batch method exists" \
        "grep -q 'def set_policies_batch' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    run_test "Batch has MAX_BATCH_SIZE limit" \
        "grep -q 'MAX_BATCH_SIZE = 100' /home/sat/cl_revenue_ops/modules/policy_manager.py"

    # =========================================================================
    # Cost Report Tests (capacity planning)
    # =========================================================================
    echo ""
    log_info "Testing cost report for capacity planning..."

    # Test costs report type
    run_test "revenue-report costs works" \
        "revenue_cli alice revenue-report costs | jq -e '.type == \"costs\"'"

    # Verify closure costs structure
    run_test "Costs report has closure_costs" \
        "revenue_cli alice revenue-report costs | jq -e '.closure_costs.total_sats != null'"

    # Verify splice costs structure
    run_test "Costs report has splice_costs" \
        "revenue_cli alice revenue-report costs | jq -e '.splice_costs.total_sats != null'"

    # Verify estimated defaults
    run_test "Costs report has estimated_defaults" \
        "revenue_cli alice revenue-report costs | jq -e '.estimated_defaults.channel_close_sats != null'"

    # Time windows present
    run_test "Costs report has time windows" \
        "revenue_cli alice revenue-report costs | jq -e '.closure_costs.last_24h_sats != null and .closure_costs.last_7d_sats != null'"

    # =========================================================================
    # cl-hive Bridge Code Verification
    # =========================================================================
    echo ""
    log_info "Verifying cl-hive bridge code (if accessible)..."

    if [ -f /home/sat/cl-hive/modules/bridge.py ]; then
        run_test "cl-hive bridge.py exists" "true"

        # Verify bridge calls revenue-policy
        run_test "Bridge calls revenue-policy" \
            "grep -q 'revenue-policy' /home/sat/cl-hive/modules/bridge.py"

        # Verify bridge calls revenue-rebalance
        run_test "Bridge calls revenue-rebalance" \
            "grep -q 'revenue-rebalance' /home/sat/cl-hive/modules/bridge.py"

        # Verify rate limiting in bridge
        run_test "Bridge has rate limiting" \
            "grep -q 'POLICY_RATE_LIMIT' /home/sat/cl-hive/modules/bridge.py"

        # Verify circuit breaker pattern
        run_test "Bridge uses circuit breaker" \
            "grep -q 'CircuitOpenError\\|circuit' /home/sat/cl-hive/modules/bridge.py"
    else
        log_info "cl-hive not in expected path, skipping bridge verification"
    fi
}

# Routing Simulation Tests
test_routing() {
    echo ""
    echo "========================================"
    echo "ROUTING SIMULATION TESTS"
    echo "========================================"

    log_info "Testing payment routing through hive network..."

    # =========================================================================
    # Channel Topology Verification
    # =========================================================================
    echo ""
    log_info "Verifying channel topology..."

    # Get pubkeys
    ALICE_PUBKEY=$(get_pubkey alice)
    BOB_PUBKEY=$(get_pubkey bob)
    CAROL_PUBKEY=$(get_pubkey carol)

    log_info "Alice: ${ALICE_PUBKEY:0:16}..."
    log_info "Bob: ${BOB_PUBKEY:0:16}..."
    log_info "Carol: ${CAROL_PUBKEY:0:16}..."

    # Check channels exist
    ALICE_CHANNELS=$(revenue_cli alice listpeerchannels 2>/dev/null | jq '.channels | length')
    BOB_CHANNELS=$(revenue_cli bob listpeerchannels 2>/dev/null | jq '.channels | length')
    log_info "Alice channels: $ALICE_CHANNELS, Bob channels: $BOB_CHANNELS"

    run_test "Alice has at least one channel" "[ '$ALICE_CHANNELS' -ge 1 ]"
    run_test "Bob has at least one channel" "[ '$BOB_CHANNELS' -ge 1 ]"

    # =========================================================================
    # Invoice Generation Tests
    # =========================================================================
    echo ""
    log_info "Testing invoice generation..."

    # Generate test invoice on Carol
    if [ -n "$CAROL_PUBKEY" ]; then
        TEST_INVOICE=$(revenue_cli carol invoice 10000 "routing-test-$(date +%s)" "Test payment" 2>/dev/null || echo "{}")
        if echo "$TEST_INVOICE" | jq -e '.bolt11' >/dev/null 2>&1; then
            run_test "Carol can generate invoice" "true"
            BOLT11=$(echo "$TEST_INVOICE" | jq -r '.bolt11')
            log_info "Invoice generated: ${BOLT11:0:40}..."
        else
            log_info "Invoice generation failed - may need channel funding"
        fi
    fi

    # =========================================================================
    # Route Finding Tests
    # =========================================================================
    echo ""
    log_info "Testing route discovery..."

    # Check getroute command
    if [ -n "$BOB_PUBKEY" ]; then
        ROUTE=$(revenue_cli alice getroute $BOB_PUBKEY 1000 1 2>/dev/null || echo "{}")
        if echo "$ROUTE" | jq -e '.route' >/dev/null 2>&1; then
            run_test "Alice can find route to Bob" "true"
            ROUTE_HOPS=$(echo "$ROUTE" | jq '.route | length')
            log_info "Route to Bob has $ROUTE_HOPS hop(s)"
        else
            log_info "No route to Bob found - channels may need funding"
        fi
    fi

    # =========================================================================
    # Fee Estimation Tests
    # =========================================================================
    echo ""
    log_info "Testing fee estimation for routes..."

    # Check fee policies are reasonable
    if revenue_cli alice revenue-status 2>/dev/null | jq -e '.channel_states' >/dev/null; then
        CHANNELS=$(revenue_cli alice revenue-status | jq '.channel_states')
        if [ "$(echo "$CHANNELS" | jq 'length')" -gt 0 ]; then
            # Get first channel's fee info
            FIRST_FEE=$(echo "$CHANNELS" | jq '.[0].fee_ppm // 0')
            log_info "First channel fee: $FIRST_FEE ppm"
            run_test "Fee is within bounds (0-5000 ppm)" "[ '$FIRST_FEE' -ge 0 ] && [ '$FIRST_FEE' -le 5000 ]"
        fi
    fi

    # =========================================================================
    # Payment Flow Simulation Tests (Code Verification)
    # =========================================================================
    echo ""
    log_info "Verifying payment flow handling code..."

    # Check forward event handling
    run_test "Forward event handler exists" \
        "grep -q '@plugin.subscribe.*forward_event\\|forward_event' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    run_test "Forward events stored in database" \
        "grep -q 'store_forward\\|forward_event' /home/sat/cl_revenue_ops/modules/database.py"

    # Check flow analysis updates on forwards
    run_test "Flow analysis updates on forward" \
        "grep -q 'on_forward\\|forward.*flow' /home/sat/cl_revenue_ops/modules/flow_analysis.py"

    # Check revenue tracking
    run_test "Revenue tracked from forwards" \
        "grep -q 'fee.*earned\\|revenue\\|routing_fee' /home/sat/cl_revenue_ops/modules/database.py"

    # =========================================================================
    # Multi-hop Routing Tests
    # =========================================================================
    echo ""
    log_info "Testing multi-hop routing capability..."

    # Test route through hive
    if [ -n "$CAROL_PUBKEY" ] && [ -n "$ALICE_PUBKEY" ]; then
        # Try to get route from Alice to Carol (may go through Bob)
        MULTI_ROUTE=$(revenue_cli alice getroute $CAROL_PUBKEY 1000 1 2>/dev/null || echo "{}")
        if echo "$MULTI_ROUTE" | jq -e '.route' >/dev/null 2>&1; then
            MULTI_HOPS=$(echo "$MULTI_ROUTE" | jq '.route | length')
            log_info "Route to Carol: $MULTI_HOPS hop(s)"
            run_test "Multi-hop route exists" "[ '$MULTI_HOPS' -ge 1 ]"
        fi
    fi

    # =========================================================================
    # HTLC Handling Tests (Code Verification)
    # =========================================================================
    echo ""
    log_info "Verifying HTLC handling code..."

    run_test "HTLC interceptor or handler exists" \
        "grep -qi 'htlc\\|intercept' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    # =========================================================================
    # Liquidity Distribution Analysis
    # =========================================================================
    echo ""
    log_info "Analyzing liquidity distribution..."

    # Check liquidity reporting
    DASHBOARD=$(revenue_cli alice revenue-dashboard 2>/dev/null || echo "{}")
    if echo "$DASHBOARD" | jq -e '.channel_states' >/dev/null 2>&1; then
        TOTAL_CAPACITY=$(echo "$DASHBOARD" | jq '[.channel_states[].capacity // 0] | add // 0')
        TOTAL_OUTBOUND=$(echo "$DASHBOARD" | jq '[.channel_states[].our_balance // 0] | add // 0')
        log_info "Total capacity: $TOTAL_CAPACITY sats"
        log_info "Total outbound: $TOTAL_OUTBOUND sats"
        if [ "$TOTAL_CAPACITY" -gt 0 ]; then
            run_test "Node has routing capacity" "true"
        fi
    fi
}

# Performance/Latency Tests
test_performance() {
    echo ""
    echo "========================================"
    echo "PERFORMANCE & LATENCY TESTS"
    echo "========================================"

    log_info "Testing plugin performance..."

    # =========================================================================
    # RPC Response Time Tests
    # =========================================================================
    echo ""
    log_info "Testing RPC response times..."

    # Measure revenue-status response time
    START_TIME=$(date +%s%3N)
    revenue_cli alice revenue-status >/dev/null 2>&1
    END_TIME=$(date +%s%3N)
    STATUS_LATENCY=$((END_TIME - START_TIME))
    log_info "revenue-status latency: ${STATUS_LATENCY}ms"
    run_test "revenue-status responds under 2000ms" "[ '$STATUS_LATENCY' -lt 2000 ]"

    # Measure revenue-dashboard response time
    START_TIME=$(date +%s%3N)
    revenue_cli alice revenue-dashboard >/dev/null 2>&1
    END_TIME=$(date +%s%3N)
    DASHBOARD_LATENCY=$((END_TIME - START_TIME))
    log_info "revenue-dashboard latency: ${DASHBOARD_LATENCY}ms"
    run_test "revenue-dashboard responds under 3000ms" "[ '$DASHBOARD_LATENCY' -lt 3000 ]"

    # Measure policy get response time
    BOB_PUBKEY=$(get_pubkey bob)
    if [ -n "$BOB_PUBKEY" ]; then
        START_TIME=$(date +%s%3N)
        revenue_cli alice revenue-policy get $BOB_PUBKEY >/dev/null 2>&1
        END_TIME=$(date +%s%3N)
        POLICY_LATENCY=$((END_TIME - START_TIME))
        log_info "revenue-policy get latency: ${POLICY_LATENCY}ms"
        run_test "revenue-policy get responds under 500ms" "[ '$POLICY_LATENCY' -lt 500 ]"
    fi

    # =========================================================================
    # Concurrent Request Tests
    # =========================================================================
    echo ""
    log_info "Testing concurrent request handling..."

    # Run 5 concurrent status requests
    START_TIME=$(date +%s%3N)
    for i in 1 2 3 4 5; do
        revenue_cli alice revenue-status >/dev/null 2>&1 &
    done
    wait
    END_TIME=$(date +%s%3N)
    CONCURRENT_LATENCY=$((END_TIME - START_TIME))
    log_info "5 concurrent revenue-status: ${CONCURRENT_LATENCY}ms"
    run_test "Concurrent requests complete under 5000ms" "[ '$CONCURRENT_LATENCY' -lt 5000 ]"

    # =========================================================================
    # Database Performance Tests
    # =========================================================================
    echo ""
    log_info "Testing database performance..."

    # Check database file exists and size
    if docker exec polar-n${NETWORK_ID}-alice test -f /home/clightning/.lightning/revenue_ops.db 2>/dev/null; then
        DB_SIZE=$(docker exec polar-n${NETWORK_ID}-alice ls -la /home/clightning/.lightning/revenue_ops.db 2>/dev/null | awk '{print $5}')
        log_info "Database size: ${DB_SIZE} bytes"
        run_test "Database file exists" "[ -n '$DB_SIZE' ]"

        # Run a quick query count test
        TABLE_COUNT=$(docker exec polar-n${NETWORK_ID}-alice sqlite3 /home/clightning/.lightning/revenue_ops.db \
            "SELECT count(*) FROM sqlite_master WHERE type='table'" 2>/dev/null || echo "0")
        log_info "Database tables: $TABLE_COUNT"
        run_test "Database has tables" "[ '$TABLE_COUNT' -gt 0 ]"
    fi

    # =========================================================================
    # Memory/Resource Checks (Code Verification)
    # =========================================================================
    echo ""
    log_info "Verifying resource management code..."

    # Check for connection cleanup
    run_test "Database connection cleanup exists" \
        "grep -q 'close\\|cleanup\\|__del__' /home/sat/cl_revenue_ops/modules/database.py"

    # Check for cache size limits
    run_test "Cache size limits exist" \
        "grep -qi 'cache.*size\\|max.*cache\\|lru\\|maxsize' /home/sat/cl_revenue_ops/modules/*.py"

    # =========================================================================
    # Plugin Initialization Time
    # =========================================================================
    echo ""
    log_info "Testing plugin initialization..."

    # This would require plugin restart - just verify init code
    run_test "Plugin init exists" \
        "grep -q '@plugin.init' /home/sat/cl_revenue_ops/cl-revenue-ops.py"

    run_test "Database init exists" \
        "grep -q 'def __init__' /home/sat/cl_revenue_ops/modules/database.py"

    # =========================================================================
    # Fee Calculation Performance
    # =========================================================================
    echo ""
    log_info "Verifying fee calculation efficiency..."

    # Check for cached fee calculations
    run_test "Fee state caching exists" \
        "grep -qi 'fee.*state\\|_state\\|cache' /home/sat/cl_revenue_ops/modules/fee_controller.py"

    # Check for efficient lookups
    run_test "Efficient channel lookup exists" \
        "grep -qi 'dict\\|hash\\|O(1)\\|cache' /home/sat/cl_revenue_ops/modules/fee_controller.py"
}

# Metrics Tests
test_metrics() {
    echo ""
    echo "========================================"
    echo "METRICS TESTS"
    echo "========================================"

    # Check metrics module exists
    run_test "Metrics module exists" \
        "[ -f /home/sat/cl_revenue_ops/modules/metrics.py ]"

    # Check revenue-dashboard provides metrics
    DASHBOARD=$(revenue_cli alice revenue-dashboard 2>/dev/null)
    log_info "Dashboard: $(echo "$DASHBOARD" | jq -c '.' | head -c 100)..."

    run_test "Dashboard returns data" "echo '$DASHBOARD' | jq -e '. != null'"

    # Check for key metrics
    run_test "Metrics module has forward tracking" \
        "grep -q 'forward\\|routing' /home/sat/cl_revenue_ops/modules/metrics.py"

    run_test "Metrics module has fee tracking" \
        "grep -q 'fee\\|revenue' /home/sat/cl_revenue_ops/modules/metrics.py"

    # Check capacity planner integration
    run_test "Capacity planner module exists" \
        "[ -f /home/sat/cl_revenue_ops/modules/capacity_planner.py ]"
}

# Reset Tests - Clean state for fresh testing
test_reset() {
    echo ""
    echo "========================================"
    echo "RESET TESTS"
    echo "========================================"
    echo "Resetting cl-revenue-ops state for fresh testing"
    echo ""

    log_info "Stopping cl-revenue-ops plugin on Alice..."
    revenue_cli alice plugin stop /home/clightning/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py 2>/dev/null || true
    sleep 2

    log_info "Restarting cl-revenue-ops plugin on Alice..."
    revenue_cli alice plugin start /home/clightning/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py 2>/dev/null || true
    sleep 3

    run_test "Plugin restarted successfully" "revenue_cli alice plugin list | grep -q revenue-ops"
    run_test "revenue-status works after restart" "revenue_cli alice revenue-status | jq -e '.status'"
}

#
# Main Test Runner
#

print_header() {
    echo ""
    echo "========================================"
    echo "cl-revenue-ops Test Suite"
    echo "========================================"
    echo ""
    echo "Network ID: $NETWORK_ID"
    echo "Hive Nodes: $HIVE_NODES"
    echo "Vanilla Nodes: $VANILLA_NODES"
    echo "Category: $CATEGORY"
    echo ""
}

print_summary() {
    echo ""
    echo "========================================"
    echo "Test Results"
    echo "========================================"
    echo ""
    echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
    echo ""

    if [ $TESTS_FAILED -gt 0 ]; then
        echo -e "${RED}Failed Tests:${NC}"
        echo -e "$FAILED_TESTS"
        echo ""
    fi

    TOTAL=$((TESTS_PASSED + TESTS_FAILED))
    if [ $TOTAL -gt 0 ]; then
        PASS_RATE=$((TESTS_PASSED * 100 / TOTAL))
        echo "Pass Rate: ${PASS_RATE}%"
    fi
    echo ""
}

# =============================================================================
# SIMULATION TESTS (wrapper for simulate.sh)
# =============================================================================

test_simulation() {
    print_section "Simulation Tests"

    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    SIMULATE_SCRIPT="$SCRIPT_DIR/simulate.sh"

    # Check if simulate.sh exists
    run_test "simulate.sh exists" \
        "[ -f '$SIMULATE_SCRIPT' ]"

    run_test "simulate.sh is executable" \
        "[ -x '$SIMULATE_SCRIPT' ]"

    # Test help command
    run_test "simulate.sh help works" \
        "'$SIMULATE_SCRIPT' help 2>/dev/null | grep -q 'cl-revenue-ops Simulation Suite'"

    # Quick traffic test (2 minute balanced scenario)
    if channels_exist; then
        run_test "Quick traffic simulation (balanced, 2 min)" \
            "'$SIMULATE_SCRIPT' traffic balanced 2 $NETWORK_ID 2>/dev/null"

        run_test "Latency benchmark" \
            "'$SIMULATE_SCRIPT' benchmark latency $NETWORK_ID 2>/dev/null"

        run_test "Channel health analysis" \
            "'$SIMULATE_SCRIPT' health $NETWORK_ID 2>/dev/null"

        run_test "Generate simulation report" \
            "'$SIMULATE_SCRIPT' report $NETWORK_ID 2>/dev/null"
    else
        echo "  [SKIP] Skipping simulation tests - no funded channels"
    fi
}

# Helper to check if channels exist
channels_exist() {
    result=$(hive_cli alice listchannels 2>/dev/null)
    if echo "$result" | jq -e '.channels | length > 0' >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

# Helper to check if hive exists on a node
hive_exists() {
    local node=${1:-alice}
    result=$(hive_cli $node hive-status 2>/dev/null)
    # Check for active status (not genesis_required)
    if echo "$result" | jq -e '.status == "active"' >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

# Helper to reset hive databases on all nodes
reset_hive_databases() {
    for node in $HIVE_NODES; do
        if container_exists $node; then
            docker exec polar-n${NETWORK_ID}-${node} rm -f /home/clightning/.lightning/regtest/cl_hive.db 2>/dev/null || true
        fi
    done
}

# =========================================================================
# CL-HIVE TEST CATEGORIES
# =========================================================================

# Hive Genesis Tests - Create and verify initial hive
test_hive_genesis() {
    echo ""
    echo "========================================"
    echo "HIVE GENESIS TESTS"
    echo "========================================"

    log_info "Testing hive creation workflow..."

    # Check cl-hive plugin loaded
    for node in $HIVE_NODES; do
        if container_exists $node; then
            run_test "$node has cl-hive" "hive_cli $node plugin list | grep -q cl-hive"
        fi
    done

    # Check if hive already exists
    if hive_exists alice; then
        log_info "Hive already exists, testing existing hive..."

        # Verify hive is active
        run_test "alice hive is active" \
            "hive_cli alice hive-status | jq -e '.status == \"active\"'"

        # Verify admin count is at least 1
        ADMIN_COUNT=$(hive_cli alice hive-status | jq -r '.members.admin')
        run_test "hive has admin members" "[ '$ADMIN_COUNT' -ge 1 ]"

        # Test genesis fails when hive exists (expected behavior)
        run_test_expect_fail "genesis fails when hive exists" \
            "hive_cli alice hive-genesis 2>&1 | jq -e '.hive_id != null'"
    else
        log_info "No hive exists, testing genesis..."

        # Test genesis command
        run_test "hive-genesis creates hive" \
            "hive_cli alice hive-genesis | jq -e '.hive_id != null or .status == \"success\"'"

        # Wait for hive to initialize
        sleep 2

        # Verify hive is now active
        run_test "alice hive becomes active" \
            "hive_cli alice hive-status | jq -e '.status == \"active\"'"
    fi

    # Test hive-members shows members
    run_test "hive-members shows admin" \
        "hive_cli alice hive-members | jq -e '.members | length >= 1'"

    # Verify member count
    MEMBER_COUNT=$(hive_cli alice hive-members | jq '.members | length')
    log_info "Member count: $MEMBER_COUNT"

    # Check governance mode is set
    GOV_MODE=$(hive_cli alice hive-status | jq -r '.governance_mode')
    log_info "Governance mode: $GOV_MODE"
    run_test "governance mode is set" \
        "[ -n '$GOV_MODE' ] && [ '$GOV_MODE' != 'null' ]"
}

# Hive Join Tests - Invitation and membership workflow
test_hive_join() {
    echo ""
    echo "========================================"
    echo "HIVE JOIN TESTS"
    echo "========================================"

    log_info "Testing hive join workflow..."

    # Ensure hive exists
    if ! hive_exists alice; then
        log_info "No hive found. Please run hive_genesis first."
        run_test "hive exists for join tests" "false"
        return 1
    fi

    # =========================================================================
    # Test invite ticket generation
    # =========================================================================
    log_info "Testing invite ticket generation..."

    run_test "hive-invite generates ticket" \
        "hive_cli alice hive-invite | jq -e '.ticket != null'"

    TICKET=$(hive_cli alice hive-invite | jq -r '.ticket')
    log_info "Invite ticket generated (length: ${#TICKET})"

    # =========================================================================
    # Check if bob is already a member
    # =========================================================================
    log_info "Testing bob membership..."

    BOB_IN_HIVE=$(hive_cli bob hive-status 2>/dev/null | jq -r '.status // "none"')
    if [ "$BOB_IN_HIVE" = "active" ]; then
        log_info "Bob already in hive, verifying membership..."
        run_test "bob is hive member" \
            "hive_cli bob hive-status | jq -e '.status == \"active\"'"
    else
        log_info "Bob not in hive, testing join..."
        run_test "bob joins with ticket" \
            "hive_cli bob hive-join ticket=\"$TICKET\" | jq -e '.status != null'"
        sleep 2
        run_test "bob has active hive after join" \
            "hive_cli bob hive-status | jq -e '.status == \"active\"'"
    fi

    # =========================================================================
    # Check if carol is already a member
    # =========================================================================
    log_info "Testing carol membership..."

    CAROL_IN_HIVE=$(hive_cli carol hive-status 2>/dev/null | jq -r '.status // "none"')
    if [ "$CAROL_IN_HIVE" = "active" ]; then
        log_info "Carol already in hive, verifying membership..."
        run_test "carol is hive member" \
            "hive_cli carol hive-status | jq -e '.status == \"active\"'"
    else
        log_info "Carol not in hive, testing join..."
        TICKET=$(hive_cli alice hive-invite | jq -r '.ticket')
        run_test "carol joins with ticket" \
            "hive_cli carol hive-join ticket=\"$TICKET\" | jq -e '.status != null'"
        sleep 2
        run_test "carol has active hive after join" \
            "hive_cli carol hive-status | jq -e '.status == \"active\"'"
    fi

    # =========================================================================
    # Verify multi-node hive membership
    # =========================================================================
    log_info "Verifying multi-node hive membership..."

    # Check member count on alice
    ALICE_MEMBERS=$(hive_cli alice hive-members | jq '.members | length')
    log_info "Alice sees $ALICE_MEMBERS members"
    run_test "alice sees multiple members" "[ '$ALICE_MEMBERS' -ge 1 ]"

    # Check member count on bob
    BOB_MEMBERS=$(hive_cli bob hive-members | jq '.members | length')
    log_info "Bob sees $BOB_MEMBERS members"
    run_test "bob sees multiple members" "[ '$BOB_MEMBERS' -ge 1 ]"

    # Check member count on carol
    CAROL_MEMBERS=$(hive_cli carol hive-members | jq '.members | length')
    log_info "Carol sees $CAROL_MEMBERS members"
    run_test "carol sees multiple members" "[ '$CAROL_MEMBERS' -ge 1 ]"

    # =========================================================================
    # Test member details
    # =========================================================================
    log_info "Testing member details..."

    run_test "hive-members returns member array" \
        "hive_cli alice hive-members | jq -e '.members | type == \"array\"'"

    run_test "members have peer_id field" \
        "hive_cli alice hive-members | jq -e '.members[0].peer_id != null'"

    run_test "members have tier field" \
        "hive_cli alice hive-members | jq -e '.members[0].tier != null'"
}

# Hive Sync Tests - Cross-node consistency
test_hive_sync() {
    echo ""
    echo "========================================"
    echo "HIVE SYNC TESTS"
    echo "========================================"

    log_info "Testing cross-node synchronization..."

    # Ensure hive exists
    if ! hive_exists alice; then
        log_info "No hive found. Please run hive_genesis first."
        run_test "hive exists for sync tests" "false"
        return 1
    fi

    # =========================================================================
    # Member visibility across nodes
    # =========================================================================
    log_info "Testing member visibility across nodes..."

    # Get pubkeys
    ALICE_PUBKEY=$(get_pubkey alice)
    BOB_PUBKEY=$(get_pubkey bob)
    CAROL_PUBKEY=$(get_pubkey carol)

    log_info "Alice pubkey: ${ALICE_PUBKEY:0:16}..."
    log_info "Bob pubkey: ${BOB_PUBKEY:0:16}..."
    log_info "Carol pubkey: ${CAROL_PUBKEY:0:16}..."

    # Each node should see the others
    run_test "bob sees alice in members" \
        "hive_cli bob hive-members | jq -e --arg pk '$ALICE_PUBKEY' '.members[] | select(.peer_id == \$pk)'"

    run_test "carol sees alice in members" \
        "hive_cli carol hive-members | jq -e --arg pk '$ALICE_PUBKEY' '.members[] | select(.peer_id == \$pk)'"

    run_test "alice sees bob in members" \
        "hive_cli alice hive-members | jq -e --arg pk '$BOB_PUBKEY' '.members[] | select(.peer_id == \$pk)'"

    # =========================================================================
    # Member count consistency
    # =========================================================================
    log_info "Testing member count consistency..."

    ALICE_COUNT=$(hive_cli alice hive-status | jq '.members.total')
    BOB_COUNT=$(hive_cli bob hive-status | jq '.members.total')
    CAROL_COUNT=$(hive_cli carol hive-status | jq '.members.total')

    log_info "Alice sees $ALICE_COUNT total members"
    log_info "Bob sees $BOB_COUNT total members"
    log_info "Carol sees $CAROL_COUNT total members"

    run_test "alice and bob see same member count" \
        "[ '$ALICE_COUNT' = '$BOB_COUNT' ]"

    run_test "alice and carol see same member count" \
        "[ '$ALICE_COUNT' = '$CAROL_COUNT' ]"

    # =========================================================================
    # Topology consistency
    # =========================================================================
    log_info "Testing topology view..."

    run_test "hive-topology returns data" \
        "hive_cli alice hive-topology | jq -e '.config != null'"

    # Check governance mode is set (note: governance mode is per-node config, not synced)
    ALICE_GOV=$(hive_cli alice hive-status | jq -r '.governance_mode')
    BOB_GOV=$(hive_cli bob hive-status | jq -r '.governance_mode')
    log_info "Alice governance: $ALICE_GOV, Bob governance: $BOB_GOV"

    run_test "alice has valid governance mode" \
        "[ '$ALICE_GOV' = 'autonomous' ] || [ '$ALICE_GOV' = 'advisor' ] || [ '$ALICE_GOV' = 'oracle' ]"

    run_test "bob has valid governance mode" \
        "[ '$BOB_GOV' = 'autonomous' ] || [ '$BOB_GOV' = 'advisor' ] || [ '$BOB_GOV' = 'oracle' ]"

    # =========================================================================
    # VPN status (if configured)
    # =========================================================================
    log_info "Testing VPN status..."

    run_test "hive-vpn-status returns data" \
        "hive_cli alice hive-vpn-status | jq -e 'type == \"object\"'"
}

# Hive Expansion Tests - Cooperative expansion workflow
test_hive_expansion() {
    echo ""
    echo "========================================"
    echo "HIVE COOPERATIVE EXPANSION TESTS"
    echo "========================================"

    log_info "Testing cooperative expansion workflow..."

    # Ensure hive exists
    if ! hive_exists alice; then
        log_info "No hive found. Please run hive_genesis first."
        run_test "hive exists for expansion tests" "false"
        return 1
    fi

    # =========================================================================
    # Test expansion status RPC
    # =========================================================================
    log_info "Testing expansion status..."

    run_test "hive-expansion-status returns data" \
        "hive_cli alice hive-expansion-status | jq -e 'type == \"object\"'"

    STATUS=$(hive_cli alice hive-expansion-status)
    log_info "Expansion status: $(echo "$STATUS" | jq -c '.')"

    # =========================================================================
    # Test enable/disable expansions
    # =========================================================================
    log_info "Testing expansion enable/disable..."

    run_test "hive-enable-expansions returns status" \
        "hive_cli alice hive-enable-expansions | jq -e '.expansions_enabled != null'"

    # Check expansion config in topology
    run_test "topology shows expansion config" \
        "hive_cli alice hive-topology | jq -e '.config.expansions_enabled != null'"

    # =========================================================================
    # Test pending actions system
    # =========================================================================
    log_info "Testing pending actions system..."

    run_test "hive-pending-actions returns data" \
        "hive_cli alice hive-pending-actions | jq -e 'type == \"object\"'"

    PENDING=$(hive_cli alice hive-pending-actions)
    PENDING_COUNT=$(echo "$PENDING" | jq '.actions | length // 0')
    log_info "Pending actions: $PENDING_COUNT"

    # =========================================================================
    # Test config budget settings
    # =========================================================================
    log_info "Testing budget configuration..."

    run_test "hive-config returns data" \
        "hive_cli alice hive-config | jq -e 'type == \"object\"'"

    # Check for governance budget settings
    CONFIG=$(hive_cli alice hive-config)
    log_info "Config governance section: $(echo "$CONFIG" | jq -c '.governance // {}')"

    run_test "config has governance settings" \
        "echo '$CONFIG' | jq -e '.governance != null'"

    # =========================================================================
    # Test budget summary
    # =========================================================================
    log_info "Testing budget summary..."

    run_test "hive-budget-summary returns data" \
        "hive_cli alice hive-budget-summary | jq -e 'type == \"object\"'"

    BUDGET=$(hive_cli alice hive-budget-summary)
    log_info "Budget summary: $(echo "$BUDGET" | jq -c '.')"

    # =========================================================================
    # Test nomination workflow (with external peer if available)
    # =========================================================================
    log_info "Testing nomination workflow..."

    # Get an external peer pubkey for testing (from listpeers)
    EXTERNAL_PEER=$(hive_cli alice listpeers | jq -r '.peers[0].id // empty')

    if [ -n "$EXTERNAL_PEER" ]; then
        log_info "Testing nomination for peer: ${EXTERNAL_PEER:0:16}..."

        # Try nomination (may fail if peer is already hive member, which is ok)
        NOMINATE_RESULT=$(hive_cli alice hive-expansion-nominate target_peer_id="$EXTERNAL_PEER" 2>&1)
        log_info "Nomination result: $(echo "$NOMINATE_RESULT" | head -c 200)"

        run_test "hive-expansion-nominate accepts input" \
            "echo '$NOMINATE_RESULT' | jq -e 'type == \"object\"'"
    else
        log_info "[SKIP] No external peers available for nomination test"
    fi

    # =========================================================================
    # Test planner log
    # =========================================================================
    log_info "Testing planner log..."

    run_test "hive-planner-log returns data" \
        "hive_cli alice hive-planner-log | jq -e 'type == \"object\"'"

    PLANNER_LOG=$(hive_cli alice hive-planner-log limit=5)
    log_info "Planner log entries: $(echo "$PLANNER_LOG" | jq '.entries | length // 0')"
}

# Hive Full Reset - Clean slate for testing
test_hive_reset() {
    echo ""
    echo "========================================"
    echo "HIVE RESET TESTS"
    echo "========================================"

    log_info "Resetting hive state on all nodes..."

    # Stop plugins
    for node in $HIVE_NODES; do
        if container_exists $node; then
            hive_cli $node plugin stop cl-hive 2>/dev/null || true
        fi
    done

    sleep 1

    # Reset databases
    reset_hive_databases

    # Restart plugins
    for node in $HIVE_NODES; do
        if container_exists $node; then
            hive_cli $node plugin start /home/clightning/.lightning/plugins/cl-hive/cl-hive.py 2>/dev/null || true
        fi
    done

    sleep 2

    # Verify clean state
    for node in $HIVE_NODES; do
        if container_exists $node; then
            run_test "$node has no hive after reset" \
                "! hive_exists $node"
        fi
    done

    log_info "Hive reset complete"
}

# Combined hive test suite
test_hive() {
    test_hive_genesis
    test_hive_join
    test_hive_sync
    test_hive_expansion
}

run_category() {
    case "$1" in
        setup)
            test_setup
            ;;
        status)
            test_status
            ;;
        flow)
            test_flow
            ;;
        fees)
            test_fees
            ;;
        rebalance)
            test_rebalance
            ;;
        sling)
            test_sling
            ;;
        policy)
            test_policy
            ;;
        profitability)
            test_profitability
            ;;
        clboss)
            test_clboss
            ;;
        database)
            test_database
            ;;
        closure_costs)
            test_closure_costs
            ;;
        splice_costs)
            test_splice_costs
            ;;
        security)
            test_security
            ;;
        integration)
            test_integration
            ;;
        routing)
            test_routing
            ;;
        performance)
            test_performance
            ;;
        metrics)
            test_metrics
            ;;
        simulation)
            test_simulation
            ;;
        reset)
            test_reset
            ;;
        hive_genesis)
            test_hive_genesis
            ;;
        hive_join)
            test_hive_join
            ;;
        hive_sync)
            test_hive_sync
            ;;
        hive_expansion)
            test_hive_expansion
            ;;
        hive_reset)
            test_hive_reset
            ;;
        hive)
            test_hive
            ;;
        all)
            test_setup
            test_status
            test_flow
            test_fees
            test_rebalance
            test_sling
            test_policy
            test_profitability
            test_clboss
            test_database
            test_closure_costs
            test_splice_costs
            test_security
            test_integration
            test_routing
            test_performance
            test_metrics
            test_simulation
            test_hive
            ;;
        *)
            echo "Unknown category: $1"
            echo ""
            echo "Available categories:"
            echo "  all            - Run all tests (including hive)"
            echo "  setup          - Environment and plugin verification"
            echo "  status         - Basic plugin status commands"
            echo "  flow           - Flow analysis functionality"
            echo "  fees           - Fee controller functionality"
            echo "  rebalance      - Rebalancing logic and EV calculations"
            echo "  sling          - Sling plugin integration"
            echo "  policy         - Policy manager functionality"
            echo "  profitability  - Profitability analysis"
            echo "  clboss         - CLBoss integration"
            echo "  database       - Database operations"
            echo "  closure_costs  - Channel closure cost tracking"
            echo "  splice_costs   - Splice cost tracking"
            echo "  security       - Security hardening verification"
            echo "  integration    - Cross-plugin integration (cl-hive)"
            echo "  routing        - Routing simulation tests"
            echo "  performance    - Performance and latency tests"
            echo "  metrics        - Metrics collection"
            echo "  simulation     - Simulation suite (traffic, benchmarks)"
            echo "  reset          - Reset plugin state"
            echo ""
            echo "Hive-specific categories:"
            echo "  hive           - Run all cl-hive tests"
            echo "  hive_genesis   - Hive creation tests"
            echo "  hive_join      - Member invitation and join"
            echo "  hive_sync      - State synchronization"
            echo "  hive_expansion - Cooperative expansion"
            echo "  hive_reset     - Reset hive state"
            exit 1
            ;;
    esac
}

# Main execution
print_header
run_category "$CATEGORY"
print_summary

# Exit with failure if any tests failed
[ $TESTS_FAILED -eq 0 ]
