#!/bin/bash
# Generate a CLN rune with permissions required for the MCP server
#
# Usage:
#   ./generate-mcp-rune.sh [node-name]
#
# This script generates a rune with all permissions needed for:
# - Hive plugin operations (hive-*)
# - Revenue-ops plugin operations (revenue-*)
# - Core CLN queries (getinfo, listfunds, listpeerchannels, etc.)
# - Peer intelligence (listnodes, listchannels, listpeers)
# - Fee management (setchannel)
#
# The rune can be used in nodes.production.json for the MCP server.

set -e

NODE_NAME="${1:-default}"

echo "Generating MCP rune for node: $NODE_NAME"
echo ""

# Define restrictions - each array element is an OR condition
# Multiple elements in the array mean ANY of these methods are allowed
RESTRICTIONS='[
  ["method^hive-"],
  ["method^revenue-"],
  ["method=getinfo"],
  ["method=listfunds"],
  ["method=listpeerchannels"],
  ["method=setchannel"],
  ["method=feerates"],
  ["method=listinvoices"],
  ["method=listpays"],
  ["method=listnodes"],
  ["method=listchannels"],
  ["method=listpeers"],
  ["method=plugin"]
]'

# Compact the JSON for the command
COMPACT_RESTRICTIONS=$(echo "$RESTRICTIONS" | jq -c .)

echo "Restrictions:"
echo "$RESTRICTIONS" | jq .
echo ""

# Check if we can access lightning-cli
if command -v lightning-cli &> /dev/null; then
    echo "Generating rune via lightning-cli..."
    echo ""
    echo "Command:"
    echo "  lightning-cli createrune restrictions='$COMPACT_RESTRICTIONS'"
    echo ""

    # Generate the rune
    RESULT=$(lightning-cli createrune restrictions="$COMPACT_RESTRICTIONS" 2>&1) || {
        echo "Error: Failed to create rune"
        echo "$RESULT"
        echo ""
        echo "Make sure lightningd is running and you have access to the socket."
        exit 1
    }

    RUNE=$(echo "$RESULT" | jq -r '.rune')
    UNIQUE_ID=$(echo "$RESULT" | jq -r '.unique_id')

    echo "Success! Generated rune:"
    echo ""
    echo "  Rune: $RUNE"
    echo "  ID:   $UNIQUE_ID"
    echo ""
    echo "Add this to your nodes.production.json:"
    echo ""
    echo "  {"
    echo "    \"name\": \"$NODE_NAME\","
    echo "    \"rest_url\": \"https://your-node:3001\","
    echo "    \"rune\": \"$RUNE\","
    echo "    \"ca_cert\": null"
    echo "  }"
    echo ""
else
    echo "lightning-cli not found. Run this command on your CLN node:"
    echo ""
    echo "  lightning-cli createrune restrictions='$COMPACT_RESTRICTIONS'"
    echo ""
    echo "Or via docker:"
    echo ""
    echo "  docker exec <container> lightning-cli createrune restrictions='$COMPACT_RESTRICTIONS'"
    echo ""
fi

echo "Permissions granted by this rune:"
echo "  - hive-*:          All hive plugin methods"
echo "  - revenue-*:       All revenue-ops methods"
echo "  - getinfo:         Node identity and status"
echo "  - listfunds:       On-chain and channel balances"
echo "  - listpeerchannels: Channel details"
echo "  - setchannel:      Fee adjustments"
echo "  - feerates:        On-chain fee estimates"
echo "  - listinvoices:    Invoice queries"
echo "  - listpays:        Payment history"
echo "  - listnodes:       Network graph - node info"
echo "  - listchannels:    Network graph - channel info"
echo "  - listpeers:       Connected peer info"
echo "  - plugin:          Plugin management"
