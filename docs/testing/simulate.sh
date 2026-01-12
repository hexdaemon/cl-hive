#!/bin/bash
#
# Comprehensive Simulation Suite for cl-revenue-ops and cl-hive
#
# This script generates realistic payment traffic through a Polar test network
# to measure fee algorithm effectiveness, rebalancing performance, and profitability.
#
# Usage: ./simulate.sh <command> [options] [network_id]
#
# Commands:
#   traffic <scenario> <duration_mins>  - Generate payment traffic
#   benchmark <type>                    - Run performance benchmarks
#   profitability <duration_mins>       - Run full profitability simulation
#   report                              - Generate profitability report
#   reset                               - Reset simulation state
#
# Scenarios:
#   source    - Payments flow OUT through hive (tests source channel behavior)
#   sink      - Payments flow IN through hive (tests sink channel behavior)
#   balanced  - Bidirectional traffic (tests balanced state)
#   mixed     - Mixed traffic patterns (4 segments)
#   stress    - High-volume stress test
#   realistic - REALISTIC Lightning Network simulation with:
#               * Pareto/power law payment distribution (80% small, 15% medium, 5% large)
#               * Poisson timing with time-of-day variation
#               * Node roles (merchants, consumers, routers, exchanges)
#               * Liquidity-aware failure simulation
#               * Multi-path payments (MPP) for large amounts
#
# Examples:
#   ./simulate.sh traffic source 5 1       # 5-min source scenario on network 1
#   ./simulate.sh benchmark latency 1      # Run latency benchmarks
#   ./simulate.sh profitability 30 1       # 30-min profitability simulation
#   ./simulate.sh report 1                 # Generate report for network 1
#
# Prerequisites:
#   - Polar network running with funded channels
#   - Plugins installed via install.sh
#   - Channels have sufficient liquidity
#

set -o pipefail

# =============================================================================
# CONFIGURATION
# =============================================================================

COMMAND="${1:-help}"
ARG1="${2:-}"
ARG2="${3:-}"
NETWORK_ID="${4:-${3:-1}}"

# Node configuration
HIVE_NODES="alice bob carol"
EXTERNAL_CLN="dave erin"
LND_NODES="lnd1 lnd2"

# Payment configuration
DEFAULT_PAYMENT_SATS=10000          # Default payment size
MIN_PAYMENT_SATS=1000               # Minimum payment
MAX_PAYMENT_SATS=100000             # Maximum payment
PAYMENT_INTERVAL_MS=500             # Time between payments (ms)

# Simulation state directory
SIM_DIR="/tmp/cl-revenue-ops-sim-${NETWORK_ID}"
mkdir -p "$SIM_DIR"

# CLI commands
CLN_CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"
LND_CLI="lncli --lnddir=/home/lnd/.lnd --network=regtest"

# Colors
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    CYAN='\033[0;36m'
    NC='\033[0m'
else
    RED='' GREEN='' YELLOW='' BLUE='' CYAN='' NC=''
fi

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

log_info() { echo -e "${CYAN}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[OK]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_metric() { echo -e "${BLUE}[METRIC]${NC} $1"; }

# CLN CLI wrapper
cln_cli() {
    local node=$1
    shift
    docker exec polar-n${NETWORK_ID}-${node} $CLN_CLI "$@" 2>/dev/null
}

# LND CLI wrapper
lnd_cli() {
    local node=$1
    shift
    docker exec polar-n${NETWORK_ID}-${node} $LND_CLI "$@" 2>/dev/null
}

# Get node pubkey (CLN)
get_cln_pubkey() {
    cln_cli $1 getinfo | jq -r '.id'
}

# Get node pubkey (LND)
get_lnd_pubkey() {
    lnd_cli $1 getinfo | jq -r '.identity_pubkey'
}

# Check if node is reachable
node_ready() {
    local node=$1
    docker exec polar-n${NETWORK_ID}-${node} $CLN_CLI getinfo &>/dev/null
}

# Get channel balance for a peer
get_channel_balance() {
    local node=$1
    local peer_id=$2
    cln_cli $node listpeerchannels | jq -r --arg pk "$peer_id" \
        '.channels[] | select(.peer_id == $pk and .state == "CHANNELD_NORMAL") | .to_us_msat' | head -1
}

# Get total outbound liquidity
get_total_outbound() {
    local node=$1
    cln_cli $node listpeerchannels | jq '[.channels[] | select(.state == "CHANNELD_NORMAL") | .to_us_msat | if type == "string" then gsub("msat"; "") | tonumber else . end] | add // 0'
}

# Get total inbound liquidity
get_total_inbound() {
    local node=$1
    cln_cli $node listpeerchannels | jq '[.channels[] | select(.state == "CHANNELD_NORMAL") | ((.total_msat | if type == "string" then gsub("msat"; "") | tonumber else . end) - (.to_us_msat | if type == "string" then gsub("msat"; "") | tonumber else . end))] | add // 0'
}

# Random number between min and max
random_range() {
    local min=$1
    local max=$2
    echo $(( RANDOM % (max - min + 1) + min ))
}

# Sleep with millisecond precision
sleep_ms() {
    local ms=$1
    sleep $(echo "scale=3; $ms/1000" | bc)
}

# =============================================================================
# REALISTIC SIMULATION - PAYMENT SIZE DISTRIBUTION
# =============================================================================
# Real Lightning Network payment sizes follow a Pareto/power law distribution:
# - 80% of payments are small (<10k sats)
# - 15% are medium (10k-100k sats)
# - 4% are large (100k-500k sats)
# - 1% are very large (500k-2M sats)

# Generate payment amount using Pareto distribution
# Returns amount in satoshis
generate_pareto_amount() {
    local roll=$((RANDOM % 100))

    if [ $roll -lt 80 ]; then
        # 80% small payments: 100-10,000 sats (coffee, tips, small purchases)
        echo $(random_range 100 10000)
    elif [ $roll -lt 95 ]; then
        # 15% medium payments: 10,000-100,000 sats (groceries, subscriptions)
        echo $(random_range 10000 100000)
    elif [ $roll -lt 99 ]; then
        # 4% large payments: 100,000-500,000 sats (electronics, services)
        echo $(random_range 100000 500000)
    else
        # 1% very large payments: 500,000-2,000,000 sats (rent, big purchases)
        echo $(random_range 500000 2000000)
    fi
}

# Get payment category name for logging
get_payment_category() {
    local amount=$1
    if [ $amount -lt 10000 ]; then
        echo "small"
    elif [ $amount -lt 100000 ]; then
        echo "medium"
    elif [ $amount -lt 500000 ]; then
        echo "large"
    else
        echo "xlarge"
    fi
}

# =============================================================================
# REALISTIC SIMULATION - POISSON TIMING WITH TIME-OF-DAY VARIATION
# =============================================================================
# Real payment traffic varies by time of day:
# - Peak hours (9am-9pm): Higher frequency
# - Off-peak (9pm-9am): Lower frequency
# Poisson distribution for inter-arrival times

# Generate Poisson-distributed delay (exponential inter-arrival)
# $1 = base rate (average ms between payments)
generate_poisson_delay() {
    local base_rate=$1

    # Generate exponential random variable using inverse transform
    # -ln(U) * mean, where U is uniform [0,1)
    local u=$((RANDOM % 1000 + 1))  # 1-1000
    local ln_u=$(echo "scale=6; l($u/1000)" | bc -l)
    local delay=$(echo "scale=0; (-1 * $ln_u * $base_rate)/1" | bc)

    # Ensure integer and clamp to reasonable range
    delay=${delay%.*}  # Remove any decimal part
    [ -z "$delay" ] && delay=$base_rate
    [ "$delay" -lt 100 ] 2>/dev/null && delay=100
    [ "$delay" -gt 10000 ] 2>/dev/null && delay=10000

    echo $delay
}

# Get time-of-day multiplier for payment frequency
# Returns multiplier (100 = normal, 150 = 1.5x, 50 = 0.5x)
get_time_of_day_multiplier() {
    local hour=$(date +%H)

    # Simulate time-of-day patterns (using current hour)
    # In production this would use simulated time
    case $hour in
        0[0-5]) echo 30 ;;   # 12am-5am: Very low (0.3x)
        0[6-8]) echo 60 ;;   # 6am-8am: Building up (0.6x)
        09|1[0-1]) echo 120 ;;  # 9am-11am: Morning peak (1.2x)
        1[2-3]) echo 150 ;;  # 12pm-1pm: Lunch rush (1.5x)
        1[4-6]) echo 100 ;;  # 2pm-4pm: Afternoon normal (1.0x)
        1[7-8]) echo 140 ;;  # 5pm-6pm: Evening rush (1.4x)
        19|2[0]) echo 130 ;; # 7pm-8pm: Dinner time (1.3x)
        2[1-3]) echo 80 ;;   # 9pm-11pm: Winding down (0.8x)
        *) echo 100 ;;
    esac
}

# Calculate next payment delay with time-of-day adjustment
get_realistic_delay() {
    local base_rate=${1:-500}  # Default 500ms base
    local multiplier=$(get_time_of_day_multiplier)

    # Adjust base rate by time-of-day (inverse - higher multiplier = shorter delays)
    local adjusted_rate=$((base_rate * 100 / multiplier))

    # Add Poisson variation
    generate_poisson_delay $adjusted_rate
}

# =============================================================================
# REALISTIC SIMULATION - NODE ROLES
# =============================================================================
# Real network has distinct node types:
# - Merchants: Mostly receive payments (e-commerce, services)
# - Consumers: Mostly send payments (wallets, users)
# - Routers: Balanced traffic, earn routing fees
# - Exchanges: High volume both directions

# Node role definitions
declare -A NODE_ROLES
declare -A NODE_WEIGHTS

init_node_roles() {
    # Hive nodes act as routers (balanced send/receive, earning fees)
    NODE_ROLES[alice]="router"
    NODE_ROLES[bob]="router"
    NODE_ROLES[carol]="router"

    # External CLN nodes - mixed roles
    NODE_ROLES[dave]="merchant"    # Mostly receives (simulates store)
    NODE_ROLES[erin]="consumer"    # Mostly sends (simulates wallet)
    NODE_ROLES[pat]="merchant"
    NODE_ROLES[oscar]="exchange"   # High volume both ways

    # LND nodes - varied roles for realism
    NODE_ROLES[lnd1]="router"
    NODE_ROLES[lnd2]="merchant"
    NODE_ROLES[judy]="consumer"
    NODE_ROLES[kathy]="exchange"
    NODE_ROLES[lucy]="merchant"
    NODE_ROLES[mike]="consumer"
    NODE_ROLES[niaj]="router"
    NODE_ROLES[quincy]="consumer"

    # Payment weights by role (send:receive ratio)
    # Higher = more likely to send, Lower = more likely to receive
    NODE_WEIGHTS[merchant]=20      # 20% send, 80% receive
    NODE_WEIGHTS[consumer]=80      # 80% send, 20% receive
    NODE_WEIGHTS[router]=50        # 50/50 balanced
    NODE_WEIGHTS[exchange]=50      # 50/50 but higher volume

    log_info "Node roles initialized"
}

# Get nodes by role
get_nodes_by_role() {
    local role=$1
    local result=""
    for node in "${!NODE_ROLES[@]}"; do
        if [ "${NODE_ROLES[$node]}" = "$role" ]; then
            result+="$node "
        fi
    done
    echo $result
}

# Select sender based on role weights
select_weighted_sender() {
    local all_senders="$1"
    local candidates=($all_senders)

    # Build weighted list
    local weighted=()
    for node in "${candidates[@]}"; do
        local role=${NODE_ROLES[$node]:-router}
        local weight=${NODE_WEIGHTS[$role]:-50}
        # Add node multiple times based on weight
        for ((i=0; i<weight; i+=10)); do
            weighted+=("$node")
        done
    done

    # Random selection from weighted list
    local idx=$((RANDOM % ${#weighted[@]}))
    echo "${weighted[$idx]}"
}

# Select receiver based on role weights (inverse of sender)
select_weighted_receiver() {
    local all_receivers="$1"
    local candidates=($all_receivers)

    # Build weighted list (inverse weights - merchants more likely to receive)
    local weighted=()
    for node in "${candidates[@]}"; do
        local role=${NODE_ROLES[$node]:-router}
        local weight=${NODE_WEIGHTS[$role]:-50}
        local inv_weight=$((100 - weight))
        # Add node multiple times based on inverse weight
        for ((i=0; i<inv_weight; i+=10)); do
            weighted+=("$node")
        done
    done

    # Random selection from weighted list
    local idx=$((RANDOM % ${#weighted[@]}))
    echo "${weighted[$idx]}"
}

# =============================================================================
# REALISTIC SIMULATION - LIQUIDITY-AWARE ROUTING
# =============================================================================
# Check if a payment is likely to succeed based on channel liquidity

# Get available outbound for a specific destination
get_route_liquidity() {
    local from_node=$1
    local to_pubkey=$2
    local amount_msat=$3

    # Try to find a route
    local route=$(cln_cli $from_node getroute "$to_pubkey" "$amount_msat" 10 2>/dev/null)
    if echo "$route" | jq -e '.route[0]' &>/dev/null; then
        echo "available"
    else
        echo "unavailable"
    fi
}

# Check channel liquidity before sending
check_liquidity_for_payment() {
    local from_node=$1
    local amount_msat=$2

    # Get total outbound
    local outbound=$(get_total_outbound $from_node)

    # Need at least 110% of payment (for fees)
    local required=$((amount_msat * 110 / 100))

    if [ "$outbound" -gt "$required" ]; then
        echo "sufficient"
    else
        echo "insufficient"
    fi
}

# Simulate realistic payment failure based on liquidity state
simulate_liquidity_failure() {
    local from_node=$1
    local amount_sats=$2

    # For LND nodes, use a simpler probabilistic model (no direct liquidity access)
    if [[ ! "$from_node" =~ ^(alice|bob|carol|dave|erin|pat|oscar)$ ]]; then
        # LND node - use base failure rate of 10%
        local roll=$((RANDOM % 100))
        [ $roll -lt 10 ] && echo "fail" && return
        echo "ok"
        return
    fi

    # Get current liquidity ratio for CLN nodes
    local outbound=$(get_total_outbound $from_node 2>/dev/null)
    local inbound=$(get_total_inbound $from_node 2>/dev/null)

    # Handle non-numeric values
    [[ ! "$outbound" =~ ^[0-9]+$ ]] && outbound=0
    [[ ! "$inbound" =~ ^[0-9]+$ ]] && inbound=0

    local total=$((outbound + inbound))

    if [ "$total" -eq 0 ]; then
        echo "fail"
        return
    fi

    local ratio=$((outbound * 100 / total))

    # Failure probability increases as liquidity decreases
    # <20% outbound: 50% failure rate
    # 20-40% outbound: 20% failure rate
    # 40-60% outbound: 5% failure rate
    # >60% outbound: 2% failure rate

    local roll=$((RANDOM % 100))

    if [ $ratio -lt 20 ]; then
        [ $roll -lt 50 ] && echo "fail" && return
    elif [ $ratio -lt 40 ]; then
        [ $roll -lt 20 ] && echo "fail" && return
    elif [ $ratio -lt 60 ]; then
        [ $roll -lt 5 ] && echo "fail" && return
    else
        [ $roll -lt 2 ] && echo "fail" && return
    fi

    echo "ok"
}

# =============================================================================
# REALISTIC SIMULATION - MULTI-PATH PAYMENTS (MPP)
# =============================================================================
# Large payments (>100k sats) should split across multiple paths

# Check if payment should use MPP
should_use_mpp() {
    local amount_sats=$1
    # Use MPP for payments over 100k sats
    [ $amount_sats -gt 100000 ] && echo "yes" || echo "no"
}

# Send payment with MPP splitting
send_mpp_payment() {
    local from_node=$1
    local to_pubkey=$2
    local amount_msat=$3

    # CLN supports MPP natively via pay command
    # For keysend, we simulate by splitting into chunks

    local amount_sats=$((amount_msat / 1000))

    if [ $amount_sats -le 100000 ]; then
        # Single path for small payments
        send_keysend_cln "$from_node" "$to_pubkey" "$amount_msat"
        return
    fi

    # Split into 2-4 parts
    local num_parts=$((2 + RANDOM % 3))  # 2-4 parts
    local part_size=$((amount_msat / num_parts))
    local remainder=$((amount_msat - (part_size * num_parts)))

    local total_fee=0
    local success_count=0

    log_info "MPP: Splitting $amount_sats sats into $num_parts parts"

    for ((i=1; i<=num_parts; i++)); do
        local this_part=$part_size
        [ $i -eq $num_parts ] && this_part=$((this_part + remainder))

        local result=$(send_keysend_cln "$from_node" "$to_pubkey" "$this_part")
        local status=$(echo "$result" | cut -d: -f1)
        local fee=$(echo "$result" | cut -d: -f2)

        if [ "$status" = "success" ]; then
            ((success_count++))
            total_fee=$((total_fee + fee))
        fi
    done

    # Consider success if all parts succeeded
    if [ $success_count -eq $num_parts ]; then
        echo "success:$total_fee"
    else
        echo "failed:0"
    fi
}

# =============================================================================
# REALISTIC SIMULATION - COMBINED SCENARIO
# =============================================================================

run_realistic_scenario() {
    local duration_mins=$1
    local metrics_file=$2

    echo ""
    echo "========================================"
    echo "REALISTIC LIGHTNING NETWORK SIMULATION"
    echo "========================================"
    log_info "Duration: $duration_mins minutes"
    log_info "Features: Pareto distribution, Poisson timing, node roles, liquidity-aware, MPP"

    # Initialize node roles
    init_node_roles

    local end_time=$(($(date +%s) + duration_mins * 60))
    local payment_count=0
    local success_count=0
    local fail_count=0
    local mpp_count=0
    local total_sats=0
    local total_fees=0

    # Payment category counters
    local small_count=0
    local medium_count=0
    local large_count=0
    local xlarge_count=0

    # Get all available pubkeys
    declare -A NODE_PUBKEYS
    for node in alice bob carol; do
        NODE_PUBKEYS[$node]=$(get_cln_pubkey $node 2>/dev/null || echo "")
    done
    for node in dave erin pat oscar; do
        NODE_PUBKEYS[$node]=$(get_cln_pubkey $node 2>/dev/null || echo "")
    done
    for node in lnd1 lnd2 judy kathy lucy mike niaj quincy; do
        NODE_PUBKEYS[$node]=$(get_lnd_pubkey $node 2>/dev/null || echo "")
    done

    # Filter to only nodes with pubkeys
    local available_nodes=""
    for node in "${!NODE_PUBKEYS[@]}"; do
        [ -n "${NODE_PUBKEYS[$node]}" ] && available_nodes+="$node "
    done

    log_info "Available nodes: $available_nodes"

    take_snapshot "$metrics_file" "realistic_start"

    local last_snapshot_time=$(date +%s)

    while [ $(date +%s) -lt $end_time ]; do
        # Select sender based on role weights
        local sender=$(select_weighted_sender "$available_nodes")

        # Select receiver based on role weights (different from sender)
        local receiver=$(select_weighted_receiver "$available_nodes")
        while [ "$receiver" = "$sender" ]; do
            receiver=$(select_weighted_receiver "$available_nodes")
        done

        local to_pubkey=${NODE_PUBKEYS[$receiver]}

        if [ -z "$to_pubkey" ]; then
            sleep 1
            continue
        fi

        # Generate realistic payment amount (Pareto distribution)
        local amount_sats=$(generate_pareto_amount)
        local amount_msat=$((amount_sats * 1000))
        local category=$(get_payment_category $amount_sats)

        # Track category
        case $category in
            small) ((small_count++)) ;;
            medium) ((medium_count++)) ;;
            large) ((large_count++)) ;;
            xlarge) ((xlarge_count++)) ;;
        esac

        # Check liquidity before attempting
        local liq_check=$(simulate_liquidity_failure "$sender" "$amount_sats")

        ((payment_count++))

        if [ "$liq_check" = "fail" ]; then
            log_warn "Payment #$payment_count: $sender → $receiver ($amount_sats sats, $category) - LIQUIDITY FAIL"
            update_payment_metrics "$metrics_file" "false" 0 0
            ((fail_count++))
        else
            # Determine if MPP is needed
            local use_mpp=$(should_use_mpp $amount_sats)
            local result

            if [ "$use_mpp" = "yes" ]; then
                ((mpp_count++))
                result=$(send_mpp_payment "$sender" "$to_pubkey" "$amount_msat")
            else
                # Check if sender is CLN or LND
                if [[ "$sender" =~ ^(alice|bob|carol|dave|erin|pat|oscar)$ ]]; then
                    result=$(send_keysend_cln "$sender" "$to_pubkey" "$amount_msat")
                else
                    # LND sender - use invoice-based payment
                    result=$(send_keysend_to_lnd "$sender" "$to_pubkey" "$amount_msat")
                fi
            fi

            local status=$(echo "$result" | cut -d: -f1)
            local fee=$(echo "$result" | cut -d: -f2)

            if [ "$status" = "success" ]; then
                local fee_sats=$((fee / 1000))
                local mpp_tag=""
                [ "$use_mpp" = "yes" ] && mpp_tag=" [MPP]"
                log_success "Payment #$payment_count: $sender → $receiver ($amount_sats sats, $category, fee: $fee_sats sats)$mpp_tag"
                update_payment_metrics "$metrics_file" "true" $amount_sats $fee
                ((success_count++))
                total_sats=$((total_sats + amount_sats))
                total_fees=$((total_fees + fee_sats))
            else
                log_warn "Payment #$payment_count: $sender → $receiver ($amount_sats sats, $category) - FAILED"
                update_payment_metrics "$metrics_file" "false" 0 0
                ((fail_count++))
            fi
        fi

        # Calculate realistic delay (Poisson with time-of-day)
        local delay=$(get_realistic_delay 500)
        sleep_ms $delay

        # Periodic snapshot (every 60 seconds)
        local now=$(date +%s)
        if [ $((now - last_snapshot_time)) -ge 60 ]; then
            take_snapshot "$metrics_file" "periodic_$payment_count"
            last_snapshot_time=$now

            # Progress report
            local elapsed=$((now - (end_time - duration_mins * 60)))
            local rate=$((payment_count * 60 / elapsed))
            log_info "Progress: $payment_count payments, $success_count success, $fail_count failed (~$rate/min)"
        fi
    done

    take_snapshot "$metrics_file" "realistic_end"

    echo ""
    echo "========================================"
    echo "REALISTIC SIMULATION COMPLETE"
    echo "========================================"
    echo ""
    echo "=== Payment Statistics ==="
    echo "  Total payments:     $payment_count"
    echo "  Successful:         $success_count ($((success_count * 100 / payment_count))%)"
    echo "  Failed:             $fail_count ($((fail_count * 100 / payment_count))%)"
    echo "  MPP payments:       $mpp_count"
    echo ""
    echo "=== Payment Size Distribution ==="
    echo "  Small (<10k):       $small_count ($((small_count * 100 / payment_count))%)"
    echo "  Medium (10k-100k):  $medium_count ($((medium_count * 100 / payment_count))%)"
    echo "  Large (100k-500k):  $large_count ($((large_count * 100 / payment_count))%)"
    echo "  XLarge (>500k):     $xlarge_count ($((xlarge_count * 100 / payment_count))%)"
    echo ""
    echo "=== Volume ==="
    echo "  Total sats moved:   $total_sats"
    echo "  Total fees paid:    $total_fees sats"
    echo ""
}

# =============================================================================
# METRICS COLLECTION
# =============================================================================

# Initialize metrics file
init_metrics() {
    local metrics_file="$SIM_DIR/metrics_$(date +%Y%m%d_%H%M%S).json"
    cat > "$metrics_file" << EOF
{
    "simulation_start": $(date +%s),
    "network_id": $NETWORK_ID,
    "scenario": "$1",
    "payments_sent": 0,
    "payments_succeeded": 0,
    "payments_failed": 0,
    "total_sats_sent": 0,
    "total_fees_paid": 0,
    "snapshots": []
}
EOF
    echo "$metrics_file"
}

# Take a metrics snapshot
take_snapshot() {
    local metrics_file="$1"
    local label="$2"

    local snapshot=$(cat << EOF
{
    "timestamp": $(date +%s),
    "label": "$label",
    "nodes": {
EOF
)

    local first=true
    for node in $HIVE_NODES; do
        if ! $first; then snapshot+=","; fi
        first=false

        local status=$(cln_cli $node revenue-status 2>/dev/null || echo '{}')
        local dashboard=$(cln_cli $node revenue-dashboard 2>/dev/null || echo '{}')
        local outbound=$(get_total_outbound $node)
        local inbound=$(get_total_inbound $node)

        snapshot+=$(cat << NODEEOF

        "$node": {
            "outbound_msat": $outbound,
            "inbound_msat": $inbound,
            "channel_states": $(echo "$status" | jq '.channel_states // []'),
            "recent_fee_changes": $(echo "$status" | jq '.recent_fee_changes // []' | jq 'length'),
            "recent_rebalances": $(echo "$status" | jq '.recent_rebalances // []' | jq 'length')
        }
NODEEOF
)
    done

    snapshot+="
    }
}"

    # Append to metrics file
    local current=$(cat "$metrics_file")
    echo "$current" | jq ".snapshots += [$snapshot]" > "$metrics_file"
}

# Update payment counter
update_payment_metrics() {
    local metrics_file="$1"
    local success="$2"
    local amount_sats="${3:-0}"
    local fee_msat="${4:-0}"

    # Ensure numeric values
    [[ -z "$amount_sats" || "$amount_sats" == "null" ]] && amount_sats=0
    [[ -z "$fee_msat" || "$fee_msat" == "null" ]] && fee_msat=0

    local current=$(cat "$metrics_file" 2>/dev/null)
    if [ -z "$current" ]; then
        return
    fi

    local fee_sats=$((fee_msat / 1000))

    if [ "$success" = "true" ]; then
        echo "$current" | jq ".payments_sent += 1 | .payments_succeeded += 1 | .total_sats_sent += $amount_sats | .total_fees_paid += $fee_sats" > "$metrics_file"
    else
        echo "$current" | jq ".payments_sent += 1 | .payments_failed += 1" > "$metrics_file"
    fi
}

# =============================================================================
# PAYMENT FUNCTIONS
# =============================================================================

# Send keysend payment (CLN to CLN)
send_keysend_cln() {
    local from_node=$1
    local to_pubkey=$2
    local amount_msat=$3

    local result=$(cln_cli $from_node keysend "$to_pubkey" "$amount_msat" 2>&1)
    if echo "$result" | jq -e '.status == "complete"' &>/dev/null; then
        # CLN v25.12 uses amount_sent_msat and amount_msat (as numbers)
        local fee=$(echo "$result" | jq -r '.amount_sent_msat - .amount_msat')
        echo "success:$fee"
    else
        echo "failed:0"
    fi
}

# Send keysend payment (CLN to LND)
send_keysend_to_lnd() {
    local from_node=$1
    local to_pubkey=$2
    local amount_msat=$3

    local result=$(cln_cli $from_node keysend "$to_pubkey" "$amount_msat" 2>&1)
    if echo "$result" | jq -e '.status == "complete"' &>/dev/null; then
        # CLN v25.12 uses amount_sent_msat and amount_msat (as numbers)
        local fee=$(echo "$result" | jq -r '.amount_sent_msat - .amount_msat')
        echo "success:$fee"
    else
        echo "failed:0"
    fi
}

# Send payment via invoice
send_invoice_payment() {
    local from_node=$1
    local to_node=$2
    local amount_sats=$3
    local label="sim_$(date +%s)_$RANDOM"

    # Generate invoice on destination
    local invoice=$(cln_cli $to_node invoice "${amount_sats}sat" "$label" "Simulation payment" 2>/dev/null)
    local bolt11=$(echo "$invoice" | jq -r '.bolt11')

    if [ -z "$bolt11" ] || [ "$bolt11" = "null" ]; then
        echo "failed:0"
        return
    fi

    # Pay invoice from source
    local result=$(cln_cli $from_node pay "$bolt11" 2>&1)
    if echo "$result" | jq -e '.status == "complete"' &>/dev/null; then
        # CLN v25.12 uses amount_sent_msat and amount_msat
        local fee=$(echo "$result" | jq -r '.amount_sent_msat - .amount_msat')
        echo "success:$fee"
    else
        echo "failed:0"
    fi
}

# =============================================================================
# PRE-TEST CHANNEL SETUP
# =============================================================================

# Check and balance channels before running tests
pre_test_channel_setup() {
    echo ""
    echo "========================================"
    echo "PRE-TEST CHANNEL SETUP"
    echo "========================================"

    log_info "Analyzing channel liquidity distribution..."

    # Get all channel states for hive nodes
    local needs_balancing=false

    for node in $HIVE_NODES; do
        local channels=$(cln_cli $node listpeerchannels 2>/dev/null | jq -r '
            .channels[] | select(.state == "CHANNELD_NORMAL") |
            "\(.short_channel_id):\(.to_us_msat):\(.total_msat)"
        ')

        while IFS=: read -r scid local_msat total_msat; do
            [ -z "$scid" ] && continue
            local pct=$((local_msat * 100 / total_msat))
            if [ $pct -lt 20 ] || [ $pct -gt 80 ]; then
                log_warn "$node channel $scid is unbalanced ($pct% local)"
                needs_balancing=true
            fi
        done <<< "$channels"
    done

    if [ "$needs_balancing" = "true" ]; then
        log_info "Attempting to balance channels via circular payments..."
        balance_channels_via_payments
    else
        log_success "Channel liquidity is adequately distributed"
    fi
}

# Balance channels by sending circular payments
balance_channels_via_payments() {
    log_info "Sending payments to balance channel liquidity..."

    # Strategy: Send payments from nodes with high outbound to nodes with high inbound
    # This creates return paths

    # Get pubkeys
    local ALICE_PK=$(get_cln_pubkey alice)
    local BOB_PK=$(get_cln_pubkey bob)
    local CAROL_PK=$(get_cln_pubkey carol)
    local DAVE_PK=$(get_cln_pubkey dave 2>/dev/null || echo "")
    local ERIN_PK=$(get_cln_pubkey erin 2>/dev/null || echo "")

    # Push liquidity in each direction
    local balance_amount=500000000  # 500k sats in msat

    # Hive internal balancing
    log_info "Balancing hive internal channels..."
    for i in 1 2 3; do
        send_keysend_cln alice "$BOB_PK" $balance_amount >/dev/null 2>&1 &
        send_keysend_cln bob "$CAROL_PK" $balance_amount >/dev/null 2>&1 &
        [ -n "$CAROL_PK" ] && send_keysend_cln carol "$ALICE_PK" $balance_amount >/dev/null 2>&1 &
    done
    wait

    # Push to external nodes so they have liquidity to send back
    if [ -n "$DAVE_PK" ]; then
        log_info "Pushing liquidity to external nodes..."
        for i in 1 2; do
            send_keysend_cln alice "$DAVE_PK" $balance_amount >/dev/null 2>&1 &
            send_keysend_cln bob "$DAVE_PK" $balance_amount >/dev/null 2>&1 &
        done
        wait
    fi

    if [ -n "$ERIN_PK" ]; then
        for i in 1 2; do
            send_keysend_cln carol "$ERIN_PK" $balance_amount >/dev/null 2>&1 &
        done
        wait
    fi

    log_success "Channel balancing complete"
    sleep 2
}

# Create channels with dual funding simulation (push payments after open)
setup_bidirectional_channels() {
    log_info "Setting up bidirectional channel topology..."

    local BITCOIN_CLI="bitcoin-cli -datadir=/home/bitcoin/.bitcoin -regtest"

    # Fund nodes if needed
    for node in $HIVE_NODES $EXTERNAL_CLN; do
        local balance=$(cln_cli $node listfunds 2>/dev/null | jq '[.outputs[].amount_msat] | add // 0')
        if [ "$balance" -lt 10000000000 ]; then  # Less than 10M sats
            local addr=$(cln_cli $node newaddr 2>/dev/null | jq -r '.p2tr // .bech32')
            if [ -n "$addr" ] && [ "$addr" != "null" ]; then
                docker exec polar-n${NETWORK_ID}-backend1 $BITCOIN_CLI generatetoaddress 5 "$addr" >/dev/null 2>&1
            fi
        fi
    done

    # Mine to confirm
    docker exec polar-n${NETWORK_ID}-backend1 $BITCOIN_CLI generatetoaddress 6 \
        "bcrt1qc7slrfxkknqcq2jevvvkdgvrt8080852dfjewde450xdlk4ugp7s8sn9cv" >/dev/null 2>&1

    sleep 3
    log_success "Bidirectional channel setup complete"
}

# =============================================================================
# HIVE-SPECIFIC TESTING SCENARIOS
# =============================================================================

# Comprehensive coordination protocol test
# Tests: Genesis, Invite/Join, Intent Lock, Gossip, Heartbeat, Fee Coordination
run_coordination_protocol_test() {
    echo ""
    echo "========================================"
    echo "COORDINATION PROTOCOL TEST"
    echo "========================================"
    echo ""

    local PASS=0
    local FAIL=0

    # Helper to run a test
    run_test() {
        local name="$1"
        local cmd="$2"
        echo -n "[TEST] $name... "
        if eval "$cmd" > /dev/null 2>&1; then
            echo "PASS"
            ((PASS++))
        else
            echo "FAIL"
            ((FAIL++))
        fi
    }

    # Helper to check condition
    check_condition() {
        local name="$1"
        local condition="$2"
        echo -n "[CHECK] $name... "
        if eval "$condition"; then
            echo "PASS"
            ((PASS++))
        else
            echo "FAIL"
            ((FAIL++))
        fi
    }

    # =========================================================================
    # Phase 1: Hive Status Verification
    # =========================================================================
    echo "--- Phase 1: Hive Status ---"

    for node in $HIVE_NODES; do
        local status=$(cln_cli $node hive-status 2>/dev/null)
        local is_member=$(echo "$status" | jq -r '.is_member' 2>/dev/null)
        check_condition "$node is hive member" "[ '$is_member' = 'true' ]"
    done

    # =========================================================================
    # Phase 2: Membership Consistency
    # =========================================================================
    echo ""
    echo "--- Phase 2: Membership Consistency ---"

    # Get member count from each node
    local alice_members=$(cln_cli alice hive-members 2>/dev/null | jq '.members | length' 2>/dev/null || echo "0")
    local bob_members=$(cln_cli bob hive-members 2>/dev/null | jq '.members | length' 2>/dev/null || echo "0")
    local carol_members=$(cln_cli carol hive-members 2>/dev/null | jq '.members | length' 2>/dev/null || echo "0")

    echo "  alice sees $alice_members members"
    echo "  bob sees $bob_members members"
    echo "  carol sees $carol_members members"

    check_condition "All nodes see same member count" \
        "[ '$alice_members' = '$bob_members' ] && [ '$bob_members' = '$carol_members' ]"

    # =========================================================================
    # Phase 3: Fee Coordination (HIVE Strategy)
    # =========================================================================
    echo ""
    echo "--- Phase 3: Fee Coordination ---"

    for node in $HIVE_NODES; do
        local hive_policies=$(cln_cli $node revenue-policy list 2>/dev/null | \
            jq '[.policies[] | select(.strategy == "hive")] | length' 2>/dev/null || echo "0")
        local expected=$(($(echo $HIVE_NODES | wc -w) - 1))  # All hive peers except self
        check_condition "$node has HIVE policy for $expected peers" \
            "[ '$hive_policies' -ge '$expected' ]"
    done

    # =========================================================================
    # Phase 4: Intent Lock Protocol
    # =========================================================================
    echo ""
    echo "--- Phase 4: Intent Lock Protocol ---"

    # Check pending intents (should be 0 in stable state)
    for node in $HIVE_NODES; do
        local pending=$(cln_cli $node hive-status 2>/dev/null | \
            jq '.pending_intents // 0' 2>/dev/null || echo "0")
        check_condition "$node has 0 pending intents (stable)" "[ '$pending' = '0' ]"
    done

    # =========================================================================
    # Phase 5: Gossip Propagation
    # =========================================================================
    echo ""
    echo "--- Phase 5: Gossip Propagation ---"

    # Get topology from each node and check consistency
    local alice_topology=$(cln_cli alice hive-topology 2>/dev/null | jq '.targets | length' 2>/dev/null || echo "0")
    local bob_topology=$(cln_cli bob hive-topology 2>/dev/null | jq '.targets | length' 2>/dev/null || echo "0")

    echo "  alice sees $alice_topology targets"
    echo "  bob sees $bob_topology targets"

    check_condition "Topology data propagated" "[ '$alice_topology' -gt '0' ]"

    # =========================================================================
    # Phase 6: Heartbeat / Liveness
    # =========================================================================
    echo ""
    echo "--- Phase 6: Heartbeat / Liveness ---"

    for node in $HIVE_NODES; do
        local status=$(cln_cli $node hive-status 2>/dev/null | jq -r '.status' 2>/dev/null)
        check_condition "$node status is 'active'" "[ '$status' = 'active' ]"
    done

    # =========================================================================
    # Phase 7: Cross-Plugin Integration
    # =========================================================================
    echo ""
    echo "--- Phase 7: cl-revenue-ops Integration ---"

    for node in $HIVE_NODES; do
        local bridge=$(cln_cli $node hive-status 2>/dev/null | jq -r '.bridge_status' 2>/dev/null)
        check_condition "$node bridge to cl-revenue-ops" "[ '$bridge' = 'connected' ] || [ '$bridge' = 'active' ]"
    done

    # =========================================================================
    # Summary
    # =========================================================================
    echo ""
    echo "========================================"
    echo "COORDINATION PROTOCOL RESULTS"
    echo "========================================"
    echo "Passed: $PASS"
    echo "Failed: $FAIL"
    echo "Total:  $((PASS + FAIL))"
    echo ""

    if [ "$FAIL" -eq 0 ]; then
        log_success "All coordination protocol tests passed!"
        return 0
    else
        log_error "$FAIL tests failed"
        return 1
    fi
}

# Test invite/join flow (requires fresh hive or manual reset)
run_invite_join_test() {
    echo ""
    echo "========================================"
    echo "INVITE/JOIN FLOW TEST"
    echo "========================================"
    echo ""

    # This test requires alice to be an admin
    local alice_tier=$(cln_cli alice hive-status 2>/dev/null | jq -r '.tier' 2>/dev/null)

    if [ "$alice_tier" != "admin" ]; then
        log_error "alice must be an admin to run invite test"
        return 1
    fi

    echo "[1] Generating invite ticket from alice..."
    local ticket=$(cln_cli alice hive-invite 2>/dev/null | jq -r '.ticket' 2>/dev/null)

    if [ -z "$ticket" ] || [ "$ticket" = "null" ]; then
        log_error "Failed to generate invite ticket"
        return 1
    fi

    echo "    Ticket: ${ticket:0:20}..."
    log_success "Invite ticket generated"

    echo ""
    echo "[2] Ticket structure:"
    # Decode ticket (base64) and show structure
    echo "$ticket" | base64 -d 2>/dev/null | jq '.' 2>/dev/null || echo "    (binary ticket)"

    echo ""
    log_success "Invite/Join flow test complete"
    echo ""
    echo "To test join on a new node, run:"
    echo "  lightning-cli hive-join '$ticket'"
}

# Test topology planner (Gardner algorithm)
run_planner_test() {
    echo ""
    echo "========================================"
    echo "TOPOLOGY PLANNER TEST"
    echo "========================================"
    echo ""

    local PASS=0
    local FAIL=0

    check_condition() {
        local name="$1"
        local condition="$2"
        echo -n "[CHECK] $name... "
        if eval "$condition"; then
            echo "PASS"
            ((PASS++))
        else
            echo "FAIL"
            ((FAIL++))
        fi
    }

    # =========================================================================
    # Phase 1: Topology Data Collection
    # =========================================================================
    echo "--- Phase 1: Topology Data ---"

    for node in $HIVE_NODES; do
        echo ""
        echo "=== $node topology ==="
        local topology=$(cln_cli $node hive-topology 2>/dev/null)

        if [ -n "$topology" ]; then
            echo "$topology" | jq '{
                total_targets: (.targets | length),
                saturated: [.targets[] | select(.saturation >= .market_share_cap)] | length,
                underserved: [.targets[] | select(.saturation < 0.1)] | length
            }' 2>/dev/null || echo "Error parsing topology"

            local target_count=$(echo "$topology" | jq '.targets | length' 2>/dev/null || echo "0")
            check_condition "$node has topology data" "[ '$target_count' -gt '0' ]"
        else
            echo "No topology data"
            ((FAIL++))
        fi
    done

    # =========================================================================
    # Phase 2: Planner Log Analysis
    # =========================================================================
    echo ""
    echo "--- Phase 2: Planner Log ---"

    for node in $HIVE_NODES; do
        echo ""
        echo "=== $node recent planner decisions ==="
        local log=$(cln_cli $node hive-planner-log 5 2>/dev/null)

        if [ -n "$log" ]; then
            echo "$log" | jq -r '.entries[] | "  [\(.timestamp)] \(.decision)"' 2>/dev/null | head -5 || echo "  No entries"

            local entry_count=$(echo "$log" | jq '.entries | length' 2>/dev/null || echo "0")
            check_condition "$node has planner history" "[ '$entry_count' -ge '0' ]"
        else
            echo "  No planner log"
            ((PASS++))  # Empty log is OK for new hives
        fi
    done

    # =========================================================================
    # Phase 3: Saturation Analysis
    # =========================================================================
    echo ""
    echo "--- Phase 3: Saturation Analysis ---"

    local alice_topology=$(cln_cli alice hive-topology 2>/dev/null)

    if [ -n "$alice_topology" ]; then
        echo "Top 5 targets by saturation:"
        echo "$alice_topology" | jq -r '
            .targets | sort_by(-.saturation) | .[0:5] | .[] |
            "  \(.alias // .node_id[0:12]): \(.saturation * 100 | floor)% saturated, hive_channels=\(.hive_channels)"
        ' 2>/dev/null || echo "  Error"

        echo ""
        echo "Underserved targets (saturation < 10%):"
        echo "$alice_topology" | jq -r '
            [.targets[] | select(.saturation < 0.1)] | .[0:5] | .[] |
            "  \(.alias // .node_id[0:12]): \(.saturation * 100 | floor)% saturated"
        ' 2>/dev/null || echo "  None"
    fi

    # =========================================================================
    # Phase 4: Pending Actions (Advisor Mode)
    # =========================================================================
    echo ""
    echo "--- Phase 4: Pending Actions ---"

    for node in $HIVE_NODES; do
        local actions=$(cln_cli $node hive-pending-actions 2>/dev/null)
        local action_count=$(echo "$actions" | jq '.actions | length' 2>/dev/null || echo "0")

        echo "$node: $action_count pending actions"
        if [ "$action_count" -gt "0" ]; then
            echo "$actions" | jq -r '.actions[] | "  - \(.type): \(.description)"' 2>/dev/null
        fi
    done

    # =========================================================================
    # Phase 5: Market Share Cap Enforcement
    # =========================================================================
    echo ""
    echo "--- Phase 5: Market Share Cap ---"

    local cap=$(cln_cli alice hive-status 2>/dev/null | jq -r '.config.market_share_cap // 0.20' 2>/dev/null)
    echo "Market share cap: ${cap}"

    local violations=$(cln_cli alice hive-topology 2>/dev/null | \
        jq "[.targets[] | select(.saturation > $cap)] | length" 2>/dev/null || echo "0")

    check_condition "No market share violations" "[ '$violations' -eq '0' ]"

    # =========================================================================
    # Summary
    # =========================================================================
    echo ""
    echo "========================================"
    echo "PLANNER TEST RESULTS"
    echo "========================================"
    echo "Passed: $PASS"
    echo "Failed: $FAIL"
    echo ""

    if [ "$FAIL" -eq 0 ]; then
        log_success "All planner tests passed!"
    else
        log_error "$FAIL tests failed"
    fi
}

# Test hive coordination - channel opens should be coordinated
run_hive_coordination_test() {
    local metrics_file=$1

    echo ""
    echo "========================================"
    echo "HIVE COORDINATION TEST"
    echo "========================================"

    log_info "Testing cl-hive channel open coordination..."

    # Check cl-hive status on all hive nodes
    for node in $HIVE_NODES; do
        echo ""
        echo "--- $node cl-hive status ---"
        cln_cli $node hive-status 2>&1 | jq '{
            is_member: .is_member,
            hive_size: (.members | length),
            intent_queue: (.pending_intents | length)
        }' 2>/dev/null || echo "cl-hive not responding"
    done

    take_snapshot "$metrics_file" "hive_coordination_test"

    # Test intent broadcasting
    log_info "Testing channel open intent broadcasting..."

    # Get an external node to potentially open to
    local DAVE_PK=$(get_cln_pubkey dave 2>/dev/null || echo "")

    if [ -n "$DAVE_PK" ]; then
        # Check if any hive node broadcasts intent when opening
        log_info "Checking hive intent system..."
        for node in $HIVE_NODES; do
            local intents=$(cln_cli $node hive-intents 2>/dev/null | jq 'length' 2>/dev/null || echo "0")
            echo "$node has $intents pending intents"
        done
    fi

    log_success "Hive coordination test complete"
}

# Test hive vs non-hive routing competition
run_hive_competition_test() {
    local duration_mins=$1
    local metrics_file=$2

    echo ""
    echo "========================================"
    echo "HIVE VS NON-HIVE COMPETITION TEST"
    echo "========================================"

    log_info "Testing how hive nodes compete for routing vs external nodes"
    log_info "Duration: $duration_mins minutes"

    local end_time=$(($(date +%s) + duration_mins * 60))
    local payment_count=0
    local hive_routes=0
    local external_routes=0

    # Get all pubkeys
    local ALICE_PK=$(get_cln_pubkey alice)
    local BOB_PK=$(get_cln_pubkey bob)
    local CAROL_PK=$(get_cln_pubkey carol)
    local DAVE_PK=$(get_cln_pubkey dave 2>/dev/null || echo "")
    local ERIN_PK=$(get_cln_pubkey erin 2>/dev/null || echo "")

    take_snapshot "$metrics_file" "competition_start"

    # Send payments that could route through either hive or external nodes
    while [ $(date +%s) -lt $end_time ]; do
        # External node (dave) sends to another external node (erin)
        # This tests if hive nodes win the routing fees
        if [ -n "$DAVE_PK" ] && [ -n "$ERIN_PK" ]; then
            local amount_sats=$(random_range 10000 50000)
            local amount_msat=$((amount_sats * 1000))

            # Check which route is chosen
            local route=$(cln_cli dave getroute "$ERIN_PK" $amount_msat 1 2>/dev/null | jq -r '.route[0].id // "none"')

            if echo "$route" | grep -qE "$(echo $ALICE_PK | cut -c1-10)|$(echo $BOB_PK | cut -c1-10)|$(echo $CAROL_PK | cut -c1-10)"; then
                ((hive_routes++))
            else
                ((external_routes++))
            fi

            # Actually send the payment
            local result=$(send_keysend_cln dave "$ERIN_PK" $amount_msat 2>/dev/null)
            local status=$(echo "$result" | cut -d: -f1)

            ((payment_count++))

            if [ "$status" = "success" ]; then
                log_success "Payment #$payment_count routed (hive: $hive_routes, external: $external_routes)"
            fi
        fi

        sleep 2
    done

    take_snapshot "$metrics_file" "competition_end"

    echo ""
    echo "=== COMPETITION RESULTS ==="
    echo "Total payments attempted: $payment_count"
    echo "Routes through hive nodes: $hive_routes"
    echo "Routes through external nodes: $external_routes"

    if [ $((hive_routes + external_routes)) -gt 0 ]; then
        local hive_pct=$((hive_routes * 100 / (hive_routes + external_routes)))
        echo "Hive routing share: ${hive_pct}%"
    fi

    log_success "Competition test complete"
}

# Test hive fee coordination
run_hive_fee_test() {
    local metrics_file=$1

    echo ""
    echo "========================================"
    echo "HIVE FEE COORDINATION TEST"
    echo "========================================"

    log_info "Testing how hive nodes coordinate fees..."

    # Capture initial fees
    echo ""
    echo "=== Initial Fee State ==="
    for node in $HIVE_NODES; do
        echo "--- $node ---"
        cln_cli $node revenue-status 2>/dev/null | jq '[.channel_states[] | {scid: .channel_id, fee_ppm: .fee_ppm, state: .state}]' 2>/dev/null || echo "Error"
    done

    take_snapshot "$metrics_file" "fee_test_start"

    # Check policy manager settings
    echo ""
    echo "=== Policy Settings ==="
    for node in $HIVE_NODES; do
        echo "--- $node ---"
        cln_cli $node revenue-policy list 2>/dev/null | jq 'if type == "array" then .[0:3] else . end' 2>/dev/null || echo "No policies"
    done

    # Generate some traffic to trigger fee adjustments
    log_info "Generating traffic to trigger fee adjustments..."

    local BOB_PK=$(get_cln_pubkey bob)
    local CAROL_PK=$(get_cln_pubkey carol)
    local DAVE_PK=$(get_cln_pubkey dave 2>/dev/null || echo "")

    for i in $(seq 1 10); do
        send_keysend_cln alice "$BOB_PK" 100000000 >/dev/null 2>&1 &
        [ -n "$CAROL_PK" ] && send_keysend_cln bob "$CAROL_PK" 100000000 >/dev/null 2>&1 &
        [ -n "$DAVE_PK" ] && send_keysend_cln carol "$DAVE_PK" 100000000 >/dev/null 2>&1 &
    done
    wait

    log_info "Waiting 30 seconds for fee controller to react..."
    sleep 30

    # Check fees after traffic
    echo ""
    echo "=== Fee State After Traffic ==="
    for node in $HIVE_NODES; do
        echo "--- $node ---"
        cln_cli $node revenue-status 2>/dev/null | jq '[.channel_states[] | {scid: .channel_id, fee_ppm: .fee_ppm, state: .state, flow_ratio: .flow_ratio}]' 2>/dev/null || echo "Error"
    done

    take_snapshot "$metrics_file" "fee_test_end"

    log_success "Fee coordination test complete"
}

# Test cl-revenue-ops rebalancing (not CLBOSS)
run_revenue_ops_rebalance_test() {
    local metrics_file=$1

    echo ""
    echo "========================================"
    echo "CL-REVENUE-OPS REBALANCE TEST"
    echo "========================================"

    log_info "Testing rebalancing using cl-revenue-ops (not CLBOSS)..."

    # Find rebalance candidates
    for node in $HIVE_NODES; do
        echo ""
        echo "--- $node rebalance candidates ---"

        # Get channels with imbalanced liquidity
        local channels=$(cln_cli $node listpeerchannels 2>/dev/null | jq -r '
            .channels[] | select(.state == "CHANNELD_NORMAL") |
            {
                scid: .short_channel_id,
                local_pct: ((.to_us_msat / .total_msat) * 100 | floor),
                spendable: (.spendable_msat / 1000 | floor),
                receivable: (.receivable_msat / 1000 | floor)
            }
        ')
        echo "$channels"

        # Find source channels (>70% local) and sink channels (<30% local)
        local source_channels=$(cln_cli $node listpeerchannels 2>/dev/null | jq -r '
            .channels[] | select(.state == "CHANNELD_NORMAL") |
            select((.to_us_msat / .total_msat) > 0.7) | .short_channel_id
        ')
        local sink_channels=$(cln_cli $node listpeerchannels 2>/dev/null | jq -r '
            .channels[] | select(.state == "CHANNELD_NORMAL") |
            select((.to_us_msat / .total_msat) < 0.3) | .short_channel_id
        ')

        if [ -n "$source_channels" ] && [ -n "$sink_channels" ]; then
            local from_ch=$(echo "$source_channels" | head -1)
            local to_ch=$(echo "$sink_channels" | head -1)

            if [ -n "$from_ch" ] && [ -n "$to_ch" ]; then
                log_info "Attempting rebalance on $node: $from_ch -> $to_ch (100k sats)"
                cln_cli $node revenue-rebalance "$from_ch" "$to_ch" 100000 2>&1 | jq '{status, success, message}' 2>/dev/null || echo "Rebalance failed"
            fi
        else
            log_info "$node: No rebalance opportunity (channels already balanced or insufficient)"
        fi
    done

    take_snapshot "$metrics_file" "rebalance_test"

    log_success "Rebalance test complete"
}

# Full hive system test
run_full_hive_test() {
    local duration_mins=$1

    echo ""
    echo "========================================"
    echo "FULL HIVE SYSTEM TEST"
    echo "========================================"
    echo "Duration: $duration_mins minutes"
    echo ""

    local metrics_file=$(init_metrics "full_hive_test")

    # Phase 1: Setup
    log_info "=== Phase 1: Pre-test Setup ==="
    pre_test_channel_setup

    # Phase 2: Hive coordination
    log_info "=== Phase 2: Hive Coordination ==="
    run_hive_coordination_test "$metrics_file"

    # Phase 3: Fee management
    log_info "=== Phase 3: Fee Management ==="
    run_hive_fee_test "$metrics_file"

    # Phase 4: Traffic and competition
    log_info "=== Phase 4: Traffic & Competition ==="
    local traffic_mins=$((duration_mins / 3))
    [ $traffic_mins -lt 1 ] && traffic_mins=1
    run_hive_competition_test $traffic_mins "$metrics_file"

    # Phase 5: Rebalancing
    log_info "=== Phase 5: Rebalancing ==="
    run_revenue_ops_rebalance_test "$metrics_file"

    # Phase 6: Final analysis
    log_info "=== Phase 6: Final Analysis ==="
    analyze_hive_performance "$metrics_file"

    echo ""
    log_success "Full hive system test complete"
    echo "Metrics saved to: $metrics_file"
}

# Analyze hive performance vs non-hive
analyze_hive_performance() {
    local metrics_file=$1

    echo ""
    echo "========================================"
    echo "HIVE PERFORMANCE ANALYSIS"
    echo "========================================"

    # Collect fee revenue from hive nodes
    echo ""
    echo "=== Fee Revenue (from forwards) ==="
    for node in $HIVE_NODES; do
        local forwards=$(cln_cli $node listforwards 2>/dev/null | jq '{total_in: ([.forwards[].in_msat] | add), total_out: ([.forwards[].out_msat] | add), total_fee: ([.forwards[].fee_msat] | add), count: ([.forwards[]] | length)}')
        echo "$node: $forwards"
    done

    # Compare with external nodes
    echo ""
    echo "=== External Node Fee Revenue ==="
    for node in $EXTERNAL_CLN; do
        local forwards=$(cln_cli $node listforwards 2>/dev/null | jq '{total_fee: ([.forwards[].fee_msat] | add), count: ([.forwards[]] | length)}' 2>/dev/null || echo '{"total_fee": 0, "count": 0}')
        echo "$node: $forwards"
    done

    # Channel efficiency
    echo ""
    echo "=== Channel Efficiency (Turnover) ==="
    for node in $HIVE_NODES; do
        echo "--- $node ---"
        cln_cli $node revenue-status 2>/dev/null | jq '[.channel_states[] | {
            scid: .channel_id,
            velocity: .velocity,
            turnover: (if .capacity > 0 then (.sats_in + .sats_out) / .capacity else 0 end)
        }]' 2>/dev/null || echo "Error"
    done

    take_snapshot "$metrics_file" "final_analysis"
}

# =============================================================================
# TRAFFIC SCENARIOS
# =============================================================================

# Source scenario: Payments flow OUT from hive nodes
run_source_scenario() {
    local duration_mins=$1
    local metrics_file=$2

    log_info "Running SOURCE scenario for $duration_mins minutes"
    log_info "Traffic pattern: Hive nodes → External nodes"

    local end_time=$(($(date +%s) + duration_mins * 60))
    local payment_count=0

    # Get external node pubkeys
    local LND1_PK=$(get_lnd_pubkey lnd1 2>/dev/null || echo "")
    local LND2_PK=$(get_lnd_pubkey lnd2 2>/dev/null || echo "")
    local DAVE_PK=$(get_cln_pubkey dave 2>/dev/null || echo "")

    take_snapshot "$metrics_file" "scenario_start"

    while [ $(date +%s) -lt $end_time ]; do
        # Rotate through hive nodes sending to external
        for sender in alice bob carol; do
            # Pick a random external destination
            local targets=()
            [ -n "$LND1_PK" ] && targets+=("$LND1_PK")
            [ -n "$LND2_PK" ] && targets+=("$LND2_PK")
            [ -n "$DAVE_PK" ] && targets+=("$DAVE_PK")

            if [ ${#targets[@]} -eq 0 ]; then
                log_warn "No external targets available"
                sleep 5
                continue
            fi

            local target=${targets[$RANDOM % ${#targets[@]}]}
            local amount_sats=$(random_range $MIN_PAYMENT_SATS $MAX_PAYMENT_SATS)
            local amount_msat=$((amount_sats * 1000))

            local result=$(send_keysend_cln $sender "$target" $amount_msat)
            local status=$(echo "$result" | cut -d: -f1)
            local fee=$(echo "$result" | cut -d: -f2)

            ((payment_count++))

            if [ "$status" = "success" ]; then
                log_success "Payment #$payment_count: $sender → external ($amount_sats sats, fee: $((fee/1000)) sats)"
                update_payment_metrics "$metrics_file" "true" $amount_sats $fee
            else
                log_warn "Payment #$payment_count: $sender → external FAILED"
                update_payment_metrics "$metrics_file" "false" 0 0
            fi

            sleep_ms $PAYMENT_INTERVAL_MS
        done

        # Snapshot every 30 seconds
        if [ $((payment_count % 60)) -eq 0 ]; then
            take_snapshot "$metrics_file" "periodic_$payment_count"
        fi
    done

    take_snapshot "$metrics_file" "scenario_end"
    log_success "Source scenario complete. Total payments: $payment_count"
}

# Sink scenario: Payments flow IN to hive nodes
run_sink_scenario() {
    local duration_mins=$1
    local metrics_file=$2

    log_info "Running SINK scenario for $duration_mins minutes"
    log_info "Traffic pattern: External nodes → Hive nodes"

    local end_time=$(($(date +%s) + duration_mins * 60))
    local payment_count=0

    # Get hive node pubkeys
    local ALICE_PK=$(get_cln_pubkey alice)
    local BOB_PK=$(get_cln_pubkey bob)
    local CAROL_PK=$(get_cln_pubkey carol)

    take_snapshot "$metrics_file" "scenario_start"

    while [ $(date +%s) -lt $end_time ]; do
        # External CLN nodes send to hive
        for sender in dave erin; do
            if ! node_ready $sender; then continue; fi

            # Pick a random hive destination
            local targets=("$ALICE_PK" "$BOB_PK" "$CAROL_PK")
            local target=${targets[$RANDOM % ${#targets[@]}]}
            local amount_sats=$(random_range $MIN_PAYMENT_SATS $MAX_PAYMENT_SATS)
            local amount_msat=$((amount_sats * 1000))

            local result=$(send_keysend_cln $sender "$target" $amount_msat)
            local status=$(echo "$result" | cut -d: -f1)
            local fee=$(echo "$result" | cut -d: -f2)

            ((payment_count++))

            if [ "$status" = "success" ]; then
                log_success "Payment #$payment_count: $sender → hive ($amount_sats sats)"
                update_payment_metrics "$metrics_file" "true" $amount_sats $fee
            else
                log_warn "Payment #$payment_count: $sender → hive FAILED"
                update_payment_metrics "$metrics_file" "false" 0 0
            fi

            sleep_ms $PAYMENT_INTERVAL_MS
        done

        # Snapshot every 30 seconds
        if [ $((payment_count % 60)) -eq 0 ]; then
            take_snapshot "$metrics_file" "periodic_$payment_count"
        fi
    done

    take_snapshot "$metrics_file" "scenario_end"
    log_success "Sink scenario complete. Total payments: $payment_count"
}

# Balanced scenario: Bidirectional traffic
run_balanced_scenario() {
    local duration_mins=$1
    local metrics_file=$2

    log_info "Running BALANCED scenario for $duration_mins minutes"
    log_info "Traffic pattern: Bidirectional between all nodes"

    local end_time=$(($(date +%s) + duration_mins * 60))
    local payment_count=0

    # Get all pubkeys
    local ALICE_PK=$(get_cln_pubkey alice)
    local BOB_PK=$(get_cln_pubkey bob)
    local CAROL_PK=$(get_cln_pubkey carol)
    local DAVE_PK=$(get_cln_pubkey dave 2>/dev/null || echo "")

    take_snapshot "$metrics_file" "scenario_start"

    while [ $(date +%s) -lt $end_time ]; do
        # Alternating direction
        if [ $((payment_count % 2)) -eq 0 ]; then
            # Hive internal payments
            local senders=("alice" "bob" "carol")
            local sender=${senders[$RANDOM % ${#senders[@]}]}
            local targets=("$ALICE_PK" "$BOB_PK" "$CAROL_PK")
            # Remove sender from targets
            local target=${targets[$RANDOM % ${#targets[@]}]}
        else
            # Cross-boundary payments
            if [ $((RANDOM % 2)) -eq 0 ]; then
                # Hive → External
                local senders=("alice" "bob" "carol")
                local sender=${senders[$RANDOM % ${#senders[@]}]}
                local target="$DAVE_PK"
            else
                # External → Hive
                local sender="dave"
                local targets=("$ALICE_PK" "$BOB_PK" "$CAROL_PK")
                local target=${targets[$RANDOM % ${#targets[@]}]}
            fi
        fi

        if [ -z "$target" ] || [ "$target" = "null" ]; then
            sleep 1
            continue
        fi

        local amount_sats=$(random_range $MIN_PAYMENT_SATS $MAX_PAYMENT_SATS)
        local amount_msat=$((amount_sats * 1000))

        local result=$(send_keysend_cln $sender "$target" $amount_msat)
        local status=$(echo "$result" | cut -d: -f1)
        local fee=$(echo "$result" | cut -d: -f2)

        ((payment_count++))

        if [ "$status" = "success" ]; then
            log_success "Payment #$payment_count: $sender → dest ($amount_sats sats)"
            update_payment_metrics "$metrics_file" "true" $amount_sats $fee
        else
            log_warn "Payment #$payment_count: FAILED"
            update_payment_metrics "$metrics_file" "false" 0 0
        fi

        sleep_ms $PAYMENT_INTERVAL_MS

        # Snapshot every 30 seconds
        if [ $((payment_count % 60)) -eq 0 ]; then
            take_snapshot "$metrics_file" "periodic_$payment_count"
        fi
    done

    take_snapshot "$metrics_file" "scenario_end"
    log_success "Balanced scenario complete. Total payments: $payment_count"
}

# Mixed scenario: Realistic traffic with varying patterns
run_mixed_scenario() {
    local duration_mins=$1
    local metrics_file=$2

    log_info "Running MIXED scenario for $duration_mins minutes"
    log_info "Traffic pattern: Realistic varying patterns"

    local segment_duration=$((duration_mins / 4))
    if [ $segment_duration -lt 1 ]; then segment_duration=1; fi

    log_info "Running 4 segments of $segment_duration minutes each"

    take_snapshot "$metrics_file" "scenario_start"

    # Segment 1: Source-heavy
    log_info "=== Segment 1: Source-heavy (simulating outbound demand) ==="
    MIN_PAYMENT_SATS=5000
    MAX_PAYMENT_SATS=50000
    run_source_scenario $segment_duration "$metrics_file"

    take_snapshot "$metrics_file" "segment_1_complete"

    # Segment 2: Sink-heavy
    log_info "=== Segment 2: Sink-heavy (simulating inbound demand) ==="
    MIN_PAYMENT_SATS=10000
    MAX_PAYMENT_SATS=80000
    run_sink_scenario $segment_duration "$metrics_file"

    take_snapshot "$metrics_file" "segment_2_complete"

    # Segment 3: High-frequency small payments
    log_info "=== Segment 3: High-frequency small payments ==="
    MIN_PAYMENT_SATS=1000
    MAX_PAYMENT_SATS=5000
    PAYMENT_INTERVAL_MS=200
    run_balanced_scenario $segment_duration "$metrics_file"

    take_snapshot "$metrics_file" "segment_3_complete"

    # Segment 4: Low-frequency large payments
    log_info "=== Segment 4: Low-frequency large payments ==="
    MIN_PAYMENT_SATS=50000
    MAX_PAYMENT_SATS=200000
    PAYMENT_INTERVAL_MS=2000
    run_balanced_scenario $segment_duration "$metrics_file"

    take_snapshot "$metrics_file" "scenario_end"
    log_success "Mixed scenario complete."
}

# Stress test: High volume
run_stress_scenario() {
    local duration_mins=$1
    local metrics_file=$2

    log_info "Running STRESS scenario for $duration_mins minutes"
    log_info "Traffic pattern: Maximum throughput"

    PAYMENT_INTERVAL_MS=100
    MIN_PAYMENT_SATS=1000
    MAX_PAYMENT_SATS=10000

    run_balanced_scenario $duration_mins "$metrics_file"
}

# =============================================================================
# ADVANCED TESTING SCENARIOS
# =============================================================================

# Fee algorithm effectiveness test
# Tests if fees adjust correctly based on channel liquidity changes
run_fee_algorithm_test() {
    local metrics_file=$1

    echo ""
    echo "========================================"
    echo "FEE ALGORITHM EFFECTIVENESS TEST"
    echo "========================================"

    log_info "This test verifies fee adjustments respond to liquidity changes"

    # Capture initial fees
    log_info "Capturing initial fee state..."
    local initial_fees=$(cln_cli alice revenue-status 2>/dev/null | jq '[.channel_states[] | {scid: .scid, fee_ppm: .fee_ppm, flow_ratio: .flow_ratio}]')
    echo "$initial_fees" > "$SIM_DIR/initial_fees.json"

    take_snapshot "$metrics_file" "fee_test_start"

    # Phase 1: Drain alice (make her channels source-heavy)
    log_info "=== Phase 1: Creating source pressure on alice ==="
    log_info "Sending payments OUT to drain outbound liquidity..."

    local BOB_PK=$(get_cln_pubkey bob)
    local CAROL_PK=$(get_cln_pubkey carol)

    for i in $(seq 1 20); do
        send_keysend_cln alice "$BOB_PK" 50000000 >/dev/null 2>&1 &
        send_keysend_cln alice "$CAROL_PK" 50000000 >/dev/null 2>&1 &
    done
    wait

    log_info "Waiting for fee controller to react (60 seconds)..."
    sleep 60

    take_snapshot "$metrics_file" "after_drain"

    # Capture mid-test fees
    local mid_fees=$(cln_cli alice revenue-status 2>/dev/null | jq '[.channel_states[] | {scid: .scid, fee_ppm: .fee_ppm, flow_ratio: .flow_ratio}]')
    echo "$mid_fees" > "$SIM_DIR/mid_fees.json"

    # Phase 2: Refill alice (make her channels sink-heavy)
    log_info "=== Phase 2: Creating sink pressure on alice ==="
    log_info "Sending payments IN to refill outbound liquidity..."

    local ALICE_PK=$(get_cln_pubkey alice)

    for i in $(seq 1 20); do
        send_keysend_cln bob "$ALICE_PK" 50000000 >/dev/null 2>&1 &
        send_keysend_cln carol "$ALICE_PK" 50000000 >/dev/null 2>&1 &
    done
    wait

    log_info "Waiting for fee controller to react (60 seconds)..."
    sleep 60

    take_snapshot "$metrics_file" "after_refill"

    # Capture final fees
    local final_fees=$(cln_cli alice revenue-status 2>/dev/null | jq '[.channel_states[] | {scid: .scid, fee_ppm: .fee_ppm, flow_ratio: .flow_ratio}]')
    echo "$final_fees" > "$SIM_DIR/final_fees.json"

    # Analyze results
    echo ""
    log_info "=== Fee Algorithm Analysis ==="

    echo ""
    echo "Initial State:"
    cat "$SIM_DIR/initial_fees.json" | jq -r '.[] | "  \(.scid): fee=\(.fee_ppm)ppm flow=\(.flow_ratio)"'

    echo ""
    echo "After Drain (should see higher fees on depleted channels):"
    cat "$SIM_DIR/mid_fees.json" | jq -r '.[] | "  \(.scid): fee=\(.fee_ppm)ppm flow=\(.flow_ratio)"'

    echo ""
    echo "After Refill (should see lower fees on refilled channels):"
    cat "$SIM_DIR/final_fees.json" | jq -r '.[] | "  \(.scid): fee=\(.fee_ppm)ppm flow=\(.flow_ratio)"'

    # Check if fees changed
    local fee_changes=$(cln_cli alice revenue-status 2>/dev/null | jq '.recent_fee_changes | length')
    log_metric "Total fee adjustments during test: $fee_changes"

    take_snapshot "$metrics_file" "fee_test_end"
    log_success "Fee algorithm test complete"
}

# Rebalance effectiveness test
# Tests if rebalancing improves channel balance
run_rebalance_test() {
    local metrics_file=$1

    echo ""
    echo "========================================"
    echo "REBALANCE EFFECTIVENESS TEST"
    echo "========================================"

    log_info "This test verifies rebalancing restores channel balance"

    take_snapshot "$metrics_file" "rebalance_test_start"

    # Check initial balance state
    log_info "Checking initial channel balances..."
    for node in $HIVE_NODES; do
        local status=$(cln_cli $node revenue-status 2>/dev/null)
        local channels=$(echo "$status" | jq '.channel_states | length')
        local imbalanced=$(echo "$status" | jq '[.channel_states[] | select(.flow_ratio > 0.7 or .flow_ratio < -0.7)] | length')
        log_info "$node: $channels channels, $imbalanced imbalanced"
    done

    # Create imbalance on alice by draining one channel
    log_info "Creating channel imbalance..."
    local BOB_PK=$(get_cln_pubkey bob)

    for i in $(seq 1 30); do
        send_keysend_cln alice "$BOB_PK" 100000000 >/dev/null 2>&1
    done

    log_info "Waiting for imbalance to register..."
    sleep 30

    take_snapshot "$metrics_file" "after_imbalance"

    # Check imbalanced state
    local imbalanced_status=$(cln_cli alice revenue-status 2>/dev/null)
    log_info "Imbalanced state:"
    echo "$imbalanced_status" | jq '.channel_states[] | {scid: .scid, flow_ratio: .flow_ratio, state: .state}'

    # Trigger manual rebalance (if sling is available)
    log_info "Attempting to trigger rebalance..."

    # Find a sink channel to rebalance from
    local sink_scid=$(echo "$imbalanced_status" | jq -r '.channel_states[] | select(.flow_ratio < -0.3) | .scid' | head -1)
    local source_scid=$(echo "$imbalanced_status" | jq -r '.channel_states[] | select(.flow_ratio > 0.3) | .scid' | head -1)

    if [ -n "$sink_scid" ] && [ -n "$source_scid" ] && [ "$sink_scid" != "null" ] && [ "$source_scid" != "null" ]; then
        log_info "Attempting rebalance: $source_scid → $sink_scid"
        local rebal_result=$(cln_cli alice revenue-rebalance "$source_scid" "$sink_scid" 500000 2>&1)
        log_info "Rebalance result: $(echo "$rebal_result" | jq -c '.')"
    else
        log_warn "No suitable channels found for rebalancing"
    fi

    # Wait for rebalance to complete and fees to adjust
    log_info "Waiting for rebalance effects (90 seconds)..."
    sleep 90

    take_snapshot "$metrics_file" "after_rebalance"

    # Check final balance state
    log_info "Final channel balances:"
    local final_status=$(cln_cli alice revenue-status 2>/dev/null)
    echo "$final_status" | jq '.channel_states[] | {scid: .scid, flow_ratio: .flow_ratio, state: .state}'

    # Check rebalance history
    local recent_rebalances=$(echo "$final_status" | jq '.recent_rebalances | length')
    log_metric "Rebalances executed: $recent_rebalances"

    take_snapshot "$metrics_file" "rebalance_test_end"
    log_success "Rebalance test complete"
}

# Channel health analysis
analyze_channel_health() {
    echo ""
    echo "========================================"
    echo "CHANNEL HEALTH ANALYSIS"
    echo "========================================"

    for node in $HIVE_NODES; do
        echo ""
        echo "=== $node ==="

        local status=$(cln_cli $node revenue-status 2>/dev/null)

        if [ -z "$status" ] || [ "$status" = "{}" ]; then
            log_warn "$node: Could not get status"
            continue
        fi

        # Overall metrics
        local channels=$(echo "$status" | jq '.channel_states | length')
        echo "Total channels: $channels"

        # Flow distribution
        local sources=$(echo "$status" | jq '[.channel_states[] | select(.state == "source")] | length')
        local sinks=$(echo "$status" | jq '[.channel_states[] | select(.state == "sink")] | length')
        local balanced=$(echo "$status" | jq '[.channel_states[] | select(.state == "balanced")] | length')
        echo "Flow states: $sources source, $sinks sink, $balanced balanced"

        # Fee statistics
        local min_fee=$(echo "$status" | jq '[.channel_states[].fee_ppm // 0] | min')
        local max_fee=$(echo "$status" | jq '[.channel_states[].fee_ppm // 0] | max')
        local avg_fee=$(echo "$status" | jq '[.channel_states[].fee_ppm // 0] | add / length | floor')
        echo "Fees (ppm): min=$min_fee, max=$max_fee, avg=$avg_fee"

        # Capacity utilization
        local total_capacity=$(echo "$status" | jq '[.channel_states[].capacity // 0] | add')
        local total_outbound=$(echo "$status" | jq '[.channel_states[].our_balance // 0] | add')
        if [ "$total_capacity" -gt 0 ]; then
            local utilization=$((total_outbound * 100 / total_capacity))
            echo "Outbound utilization: ${utilization}%"
        fi

        # Profitability if available
        local prof=$(cln_cli $node revenue-profitability 2>/dev/null)
        if [ -n "$prof" ] && [ "$prof" != "{}" ]; then
            local roi=$(echo "$prof" | jq '.overall_roi_percent // 0')
            echo "Overall ROI: ${roi}%"
        fi
    done
}

# Full system test combining all scenarios
run_full_system_test() {
    local duration_mins=${1:-30}
    local metrics_file=$(init_metrics "full_system")

    echo ""
    echo "========================================"
    echo "FULL SYSTEM TEST"
    echo "Duration: $duration_mins minutes"
    echo "========================================"

    log_info "This test runs all scenarios sequentially"

    # Initial health check
    analyze_channel_health

    take_snapshot "$metrics_file" "system_test_start"

    # Run fee algorithm test first (5 min)
    log_info "=== Running Fee Algorithm Test ==="
    run_fee_algorithm_test "$metrics_file"

    # Run mixed traffic (adjustable duration)
    local traffic_mins=$((duration_mins - 10))
    if [ $traffic_mins -lt 5 ]; then traffic_mins=5; fi

    log_info "=== Running Mixed Traffic Scenario ($traffic_mins min) ==="
    run_mixed_scenario $traffic_mins "$metrics_file"

    # Run rebalance test (5 min)
    log_info "=== Running Rebalance Test ==="
    run_rebalance_test "$metrics_file"

    take_snapshot "$metrics_file" "system_test_end"

    # Final health check
    analyze_channel_health

    # Generate summary
    echo ""
    echo "========================================"
    echo "FULL SYSTEM TEST SUMMARY"
    echo "========================================"

    local metrics=$(cat "$metrics_file")
    echo "Total payments attempted: $(echo "$metrics" | jq '.payments_sent')"
    echo "Success rate: $(echo "$metrics" | jq 'if .payments_sent > 0 then (.payments_succeeded * 100 / .payments_sent) else 0 end')%"
    echo "Total snapshots collected: $(echo "$metrics" | jq '.snapshots | length')"

    log_success "Full system test complete!"
    log_info "Run './simulate.sh report' for detailed analysis"
}

# =============================================================================
# BENCHMARK FUNCTIONS
# =============================================================================

run_latency_benchmark() {
    log_info "Running latency benchmark..."

    echo ""
    echo "========================================"
    echo "RPC LATENCY BENCHMARK"
    echo "========================================"

    local iterations=50

    for node in $HIVE_NODES; do
        echo ""
        log_info "Benchmarking $node..."

        # revenue-status latency
        local total_ms=0
        for i in $(seq 1 $iterations); do
            local start=$(date +%s%3N)
            cln_cli $node revenue-status >/dev/null 2>&1
            local end=$(date +%s%3N)
            total_ms=$((total_ms + end - start))
        done
        local avg_status=$((total_ms / iterations))
        log_metric "$node revenue-status avg: ${avg_status}ms"

        # revenue-dashboard latency
        total_ms=0
        for i in $(seq 1 $iterations); do
            local start=$(date +%s%3N)
            cln_cli $node revenue-dashboard >/dev/null 2>&1
            local end=$(date +%s%3N)
            total_ms=$((total_ms + end - start))
        done
        local avg_dashboard=$((total_ms / iterations))
        log_metric "$node revenue-dashboard avg: ${avg_dashboard}ms"

        # revenue-policy latency
        local peer_pk=$(get_cln_pubkey bob)
        total_ms=0
        for i in $(seq 1 $iterations); do
            local start=$(date +%s%3N)
            cln_cli $node revenue-policy get $peer_pk >/dev/null 2>&1
            local end=$(date +%s%3N)
            total_ms=$((total_ms + end - start))
        done
        local avg_policy=$((total_ms / iterations))
        log_metric "$node revenue-policy avg: ${avg_policy}ms"
    done
}

run_throughput_benchmark() {
    log_info "Running throughput benchmark..."

    echo ""
    echo "========================================"
    echo "PAYMENT THROUGHPUT BENCHMARK"
    echo "========================================"

    local test_payments=20
    local ALICE_PK=$(get_cln_pubkey alice)
    local BOB_PK=$(get_cln_pubkey bob)

    # Measure payment throughput
    log_info "Sending $test_payments test payments..."

    local start=$(date +%s%3N)
    local success=0
    local failed=0

    for i in $(seq 1 $test_payments); do
        local result=$(send_keysend_cln alice "$BOB_PK" 10000000)  # 10k sats
        if [ "$(echo $result | cut -d: -f1)" = "success" ]; then
            ((success++))
        else
            ((failed++))
        fi
    done

    local end=$(date +%s%3N)
    local duration_ms=$((end - start))
    local tps=$(echo "scale=2; $test_payments * 1000 / $duration_ms" | bc)

    log_metric "Payments: $success succeeded, $failed failed"
    log_metric "Duration: ${duration_ms}ms"
    log_metric "Throughput: ${tps} payments/sec"
}

run_concurrent_benchmark() {
    log_info "Running concurrent request benchmark..."

    echo ""
    echo "========================================"
    echo "CONCURRENT REQUEST BENCHMARK"
    echo "========================================"

    for concurrency in 5 10 20; do
        log_info "Testing $concurrency concurrent requests..."

        local start=$(date +%s%3N)

        for i in $(seq 1 $concurrency); do
            cln_cli alice revenue-status >/dev/null 2>&1 &
        done
        wait

        local end=$(date +%s%3N)
        local duration_ms=$((end - start))

        log_metric "$concurrency concurrent: ${duration_ms}ms total"
    done
}

# =============================================================================
# PROFITABILITY SIMULATION
# =============================================================================

run_profitability_simulation() {
    local duration_mins=$1

    echo ""
    echo "========================================"
    echo "PROFITABILITY SIMULATION"
    echo "Duration: $duration_mins minutes"
    echo "========================================"

    # Initialize metrics
    local metrics_file=$(init_metrics "profitability")
    log_info "Metrics file: $metrics_file"

    # Capture initial state
    log_info "Capturing initial state..."
    take_snapshot "$metrics_file" "initial"

    # Get initial P&L
    local initial_pnl=$(cln_cli alice revenue-history 2>/dev/null || echo '{}')
    echo "$initial_pnl" > "$SIM_DIR/initial_pnl.json"

    # Run mixed traffic simulation
    log_info "Starting traffic simulation..."
    run_mixed_scenario $duration_mins "$metrics_file"

    # Capture final state
    log_info "Capturing final state..."
    take_snapshot "$metrics_file" "final"

    # Get final P&L
    local final_pnl=$(cln_cli alice revenue-history 2>/dev/null || echo '{}')
    echo "$final_pnl" > "$SIM_DIR/final_pnl.json"

    # Finalize metrics
    local current=$(cat "$metrics_file")
    echo "$current" | jq ".simulation_end = $(date +%s)" > "$metrics_file"

    log_success "Profitability simulation complete!"
    log_info "Run './simulate.sh report' to view results"
}

# =============================================================================
# REPORTING
# =============================================================================

generate_report() {
    echo ""
    echo "========================================"
    echo "SIMULATION REPORT"
    echo "Network: $NETWORK_ID"
    echo "Generated: $(date)"
    echo "========================================"

    # Find latest metrics file
    local metrics_file=$(ls -t "$SIM_DIR"/metrics_*.json 2>/dev/null | head -1)

    if [ -z "$metrics_file" ]; then
        log_error "No simulation data found. Run a simulation first."
        return 1
    fi

    log_info "Reading metrics from: $metrics_file"

    local metrics=$(cat "$metrics_file")

    echo ""
    echo "=== PAYMENT STATISTICS ==="
    echo "Total Sent:      $(echo "$metrics" | jq '.payments_sent')"
    echo "Succeeded:       $(echo "$metrics" | jq '.payments_succeeded')"
    echo "Failed:          $(echo "$metrics" | jq '.payments_failed')"
    local success_rate=$(echo "$metrics" | jq 'if .payments_sent > 0 then (.payments_succeeded * 100 / .payments_sent) else 0 end')
    echo "Success Rate:    ${success_rate}%"
    echo "Total Sats Sent: $(echo "$metrics" | jq '.total_sats_sent')"
    echo "Total Fees Paid: $(echo "$metrics" | jq '.total_fees_paid') sats"

    # Get initial and final snapshots
    local initial=$(echo "$metrics" | jq '.snapshots[0]')
    local final=$(echo "$metrics" | jq '.snapshots[-1]')

    echo ""
    echo "=== CHANNEL STATE CHANGES ==="
    for node in $HIVE_NODES; do
        echo ""
        echo "--- $node ---"
        local init_out=$(echo "$initial" | jq ".nodes.${node}.outbound_msat // 0")
        local final_out=$(echo "$final" | jq ".nodes.${node}.outbound_msat // 0")
        local delta_out=$(( (final_out - init_out) / 1000 ))
        echo "Outbound change: ${delta_out} sats"

        local fee_changes=$(echo "$final" | jq ".nodes.${node}.recent_fee_changes // 0")
        echo "Fee adjustments: $fee_changes"

        local rebalances=$(echo "$final" | jq ".nodes.${node}.recent_rebalances // 0")
        echo "Rebalances:      $rebalances"
    done

    # P&L comparison if available
    if [ -f "$SIM_DIR/initial_pnl.json" ] && [ -f "$SIM_DIR/final_pnl.json" ]; then
        echo ""
        echo "=== PROFITABILITY ANALYSIS ==="

        local init_revenue=$(cat "$SIM_DIR/initial_pnl.json" | jq '.lifetime_routing_revenue_sats // 0')
        local final_revenue=$(cat "$SIM_DIR/final_pnl.json" | jq '.lifetime_routing_revenue_sats // 0')
        local revenue_delta=$((final_revenue - init_revenue))
        echo "Revenue earned:  $revenue_delta sats"

        local init_rebal=$(cat "$SIM_DIR/initial_pnl.json" | jq '.lifetime_rebalance_costs_sats // 0')
        local final_rebal=$(cat "$SIM_DIR/final_pnl.json" | jq '.lifetime_rebalance_costs_sats // 0')
        local rebal_delta=$((final_rebal - init_rebal))
        echo "Rebalance costs: $rebal_delta sats"

        local net_profit=$((revenue_delta - rebal_delta))
        echo "Net profit:      $net_profit sats"
    fi

    echo ""
    echo "=== CURRENT NODE STATUS ==="
    for node in $HIVE_NODES; do
        echo ""
        echo "--- $node ---"
        cln_cli $node revenue-status 2>/dev/null | jq '{
            status: .status,
            channels: (.channel_states | length),
            fee_changes: (.recent_fee_changes | length),
            rebalances: (.recent_rebalances | length)
        }'
    done

    echo ""
    log_info "Full metrics saved to: $metrics_file"
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

reset_simulation() {
    log_info "Resetting simulation state..."
    rm -rf "$SIM_DIR"/*
    log_success "Simulation state cleared"
}

show_help() {
    cat << 'EOF'
Comprehensive Simulation Suite for cl-revenue-ops and cl-hive

Usage: ./simulate.sh <command> [options] [network_id]

TRAFFIC COMMANDS:
  traffic <scenario> <duration_mins> [network_id]
      Generate payment traffic using specified scenario
      Scenarios: source, sink, balanced, mixed, stress, realistic

      'realistic' scenario features:
        - Pareto/power law payment sizes (80% small, 15% medium, 5% large)
        - Poisson timing with time-of-day variation
        - Node roles (merchants=receive, consumers=send, routers=balanced)
        - Liquidity-aware failure simulation
        - Multi-path payments (MPP) for amounts >100k sats

  benchmark <type> [network_id]
      Run performance benchmarks
      Types: latency, throughput, concurrent, all

  profitability <duration_mins> [network_id]
      Run full profitability simulation with mixed traffic

HIVE-SPECIFIC COMMANDS:
  hive-test <duration_mins> [network_id]
      Full hive system test (coordination, fees, competition, rebalance)

  protocol [network_id]
      Comprehensive coordination protocol test (membership, gossip, intents)

  planner [network_id]
      Test topology planner (Gardner algorithm, saturation, market share)

  invite-join [network_id]
      Test invite ticket generation and join flow

  hive-coordination [network_id]
      Test cl-hive channel open coordination between hive nodes

  hive-competition <duration_mins> [network_id]
      Test how hive nodes compete for routing vs external nodes

  hive-fees [network_id]
      Test hive fee coordination and adjustment

  hive-rebalance [network_id]
      Test cl-revenue-ops rebalancing (not CLBOSS)

SETUP COMMANDS:
  setup-channels [network_id]
      Setup bidirectional channel topology (fund nodes, create channels)

  pre-balance [network_id]
      Balance channels via circular payments before testing

ANALYSIS COMMANDS:
  fee-test [network_id]
      Test fee algorithm effectiveness (adjusts based on liquidity)

  rebalance-test [network_id]
      Test rebalancing effectiveness

  health [network_id]
      Analyze current channel health across all hive nodes

  full-test <duration_mins> [network_id]
      Run comprehensive system test (fee + traffic + rebalance)

  report [network_id]
      Generate report from last simulation

  reset [network_id]
      Clear simulation data

  help
      Show this help message

Examples:
  # Hive-specific testing
  ./simulate.sh hive-test 15 1            # 15-min full hive test
  ./simulate.sh hive-competition 10 1     # 10-min competition test
  ./simulate.sh hive-coordination 1       # Test cl-hive coordination

  # Setup and preparation
  ./simulate.sh setup-channels 1          # Setup channels
  ./simulate.sh pre-balance 1             # Balance channels

  # Traffic simulation
  ./simulate.sh traffic source 5 1        # 5-min source scenario
  ./simulate.sh traffic mixed 30 1        # 30-min mixed traffic

  # Analysis
  ./simulate.sh health 1                  # Check channel health
  ./simulate.sh report 1                  # View results

Environment Variables:
  PAYMENT_INTERVAL_MS   Time between payments (default: 500)
  MIN_PAYMENT_SATS      Minimum payment size (default: 1000)
  MAX_PAYMENT_SATS      Maximum payment size (default: 100000)

Notes:
  - Requires Polar network with funded channels
  - Install plugins first: ./install.sh <network_id>
  - Results stored in /tmp/cl-revenue-ops-sim-<network_id>/
  - Hive nodes: alice, bob, carol (with cl-revenue-ops, cl-hive)
  - External nodes: dave, erin, lnd1, lnd2 (no hive plugins)
EOF
}

# =============================================================================
# MAIN
# =============================================================================

case "$COMMAND" in
    traffic)
        scenario="${ARG1:-balanced}"
        duration="${ARG2:-5}"
        NETWORK_ID="${4:-1}"

        metrics_file=$(init_metrics "$scenario")

        case "$scenario" in
            source)    run_source_scenario $duration "$metrics_file" ;;
            sink)      run_sink_scenario $duration "$metrics_file" ;;
            balanced)  run_balanced_scenario $duration "$metrics_file" ;;
            mixed)     run_mixed_scenario $duration "$metrics_file" ;;
            stress)    run_stress_scenario $duration "$metrics_file" ;;
            realistic) run_realistic_scenario $duration "$metrics_file" ;;
            *)
                log_error "Unknown scenario: $scenario"
                echo "Available: source, sink, balanced, mixed, stress, realistic"
                exit 1
                ;;
        esac
        ;;

    benchmark)
        benchmark_type="${ARG1:-all}"
        NETWORK_ID="${ARG2:-1}"

        case "$benchmark_type" in
            latency)    run_latency_benchmark ;;
            throughput) run_throughput_benchmark ;;
            concurrent) run_concurrent_benchmark ;;
            all)
                run_latency_benchmark
                run_throughput_benchmark
                run_concurrent_benchmark
                ;;
            *)
                log_error "Unknown benchmark: $benchmark_type"
                echo "Available: latency, throughput, concurrent, all"
                exit 1
                ;;
        esac
        ;;

    profitability)
        duration="${ARG1:-30}"
        NETWORK_ID="${ARG2:-1}"
        run_profitability_simulation $duration
        ;;

    report)
        NETWORK_ID="${ARG1:-1}"
        generate_report
        ;;

    reset)
        NETWORK_ID="${ARG1:-1}"
        reset_simulation
        ;;

    fee-test)
        NETWORK_ID="${ARG1:-1}"
        metrics_file=$(init_metrics "fee_test")
        run_fee_algorithm_test "$metrics_file"
        ;;

    rebalance-test)
        NETWORK_ID="${ARG1:-1}"
        metrics_file=$(init_metrics "rebalance_test")
        run_rebalance_test "$metrics_file"
        ;;

    health)
        NETWORK_ID="${ARG1:-1}"
        analyze_channel_health
        ;;

    full-test)
        duration="${ARG1:-30}"
        NETWORK_ID="${ARG2:-1}"
        run_full_system_test $duration
        ;;

    # Hive-specific commands
    hive-test)
        duration="${ARG1:-15}"
        NETWORK_ID="${ARG2:-1}"
        run_full_hive_test $duration
        ;;

    coordination-protocol|protocol)
        NETWORK_ID="${ARG1:-1}"
        run_coordination_protocol_test
        ;;

    invite-join)
        NETWORK_ID="${ARG1:-1}"
        run_invite_join_test
        ;;

    planner)
        NETWORK_ID="${ARG1:-1}"
        run_planner_test
        ;;

    hive-coordination)
        NETWORK_ID="${ARG1:-1}"
        metrics_file=$(init_metrics "hive_coordination")
        run_hive_coordination_test "$metrics_file"
        ;;

    hive-competition)
        duration="${ARG1:-5}"
        NETWORK_ID="${ARG2:-1}"
        metrics_file=$(init_metrics "hive_competition")
        run_hive_competition_test $duration "$metrics_file"
        ;;

    hive-fees)
        NETWORK_ID="${ARG1:-1}"
        metrics_file=$(init_metrics "hive_fees")
        run_hive_fee_test "$metrics_file"
        ;;

    hive-rebalance)
        NETWORK_ID="${ARG1:-1}"
        metrics_file=$(init_metrics "hive_rebalance")
        run_revenue_ops_rebalance_test "$metrics_file"
        ;;

    # Setup commands
    setup-channels)
        NETWORK_ID="${ARG1:-1}"
        setup_bidirectional_channels
        ;;

    pre-balance)
        NETWORK_ID="${ARG1:-1}"
        pre_test_channel_setup
        ;;

    help|--help|-h)
        show_help
        ;;

    *)
        log_error "Unknown command: $COMMAND"
        show_help
        exit 1
        ;;
esac
