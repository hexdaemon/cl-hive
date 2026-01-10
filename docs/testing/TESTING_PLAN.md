# Comprehensive Hive Testing Plan

## Overview

This document provides a structured testing plan for cl-hive functionality in the Polar/Docker environment. Tests are organized in dependency order - each level requires all previous levels to pass.

---

## Test Environment

### Required Nodes

| Node | Type | Role | Plugins |
|------|------|------|---------|
| alice | CLN v25.12 | Hive Admin | clboss, sling, cl-revenue-ops, cl-hive |
| bob | CLN v25.12 | Hive Member | clboss, sling, cl-revenue-ops, cl-hive |
| carol | CLN v25.12 | Hive Neophyte | clboss, sling, cl-revenue-ops, cl-hive |
| dave | CLN v25.12 | External | none (vanilla) |
| erin | CLN v25.12 | External | none (vanilla) |
| lnd1 | LND | External | none |
| lnd2 | LND | External | none |

### Channel Topology (for advanced tests)

```
HIVE FLEET                           EXTERNAL
alice ─── bob ─── carol              dave ─── erin
  │         │        │
  └── lnd1  └── lnd2 └── dave
```

### CLI Reference

```bash
# Hive nodes
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"
hive_cli() { docker exec polar-n1-$1 $CLI "${@:2}"; }

# LND nodes
lnd_cli() { docker exec polar-n1-$1 lncli --network=regtest "${@:2}"; }

# Vanilla CLN nodes
vanilla_cli() { docker exec polar-n1-$1 $CLI "${@:2}"; }
```

---

## Level 0: Environment Setup

**Prerequisites:** Polar network running, install.sh executed

### L0.1 Container Verification
```bash
# Test: All containers are running
for node in alice bob carol dave erin; do
    docker ps --filter "name=polar-n1-$node" --format "{{.Names}}" | grep -q "$node"
done
```

### L0.2 Network Connectivity
```bash
# Test: Nodes can communicate
hive_cli alice getinfo
hive_cli bob getinfo
hive_cli carol getinfo
```

---

## Level 1: Plugin Loading

**Depends on:** Level 0

### L1.1 Plugin Stack Verification
```bash
# Test: All plugins loaded in correct order
for node in alice bob carol; do
    hive_cli $node plugin list | grep -q clboss
    hive_cli $node plugin list | grep -q sling
    hive_cli $node plugin list | grep -q cl-revenue-ops
    hive_cli $node plugin list | grep -q cl-hive
done
```

### L1.2 Plugin Status Checks
```bash
# Test: cl-revenue-ops is operational
hive_cli alice revenue-status | jq -e '.status == "running"'
hive_cli alice revenue-status | jq -e '.version == "1.4.0"'

# Test: cl-hive is operational (pre-genesis)
hive_cli alice hive-status | jq -e '.status == "genesis_required"'
```

### L1.3 CLBOSS Integration
```bash
# Test: CLBOSS is running
hive_cli alice clboss-status | jq -e '.info.version'
```

### L1.4 Vanilla Nodes Have No Hive
```bash
# Test: dave and erin don't have hive plugins
! vanilla_cli dave plugin list | grep -q cl-hive
! vanilla_cli erin plugin list | grep -q cl-hive
```

---

## Level 2: Genesis & Identity

**Depends on:** Level 1

### L2.1 Genesis Creation
```bash
# Test: Alice creates the hive
hive_cli alice hive-genesis | jq -e '.status == "genesis_complete"'
hive_cli alice hive-genesis | jq -e '.hive_id'
hive_cli alice hive-genesis | jq -e '.admin_pubkey'
```

### L2.2 Post-Genesis Status
```bash
# Test: Alice is now admin
hive_cli alice hive-status | jq -e '.status == "active"'
hive_cli alice hive-members | jq -e '.count == 1'
hive_cli alice hive-members | jq -e '.members[0].tier == "admin"'
```

### L2.3 Genesis Idempotency
```bash
# Test: Cannot genesis twice (should fail or return already active)
! hive_cli alice hive-genesis | jq -e '.status == "genesis_complete"'
```

### L2.4 Genesis Ticket Validity
```bash
# Test: Genesis ticket is stored in admin metadata
hive_cli alice hive-members | jq -e '.members[0].metadata' | grep -q genesis_ticket
```

---

## Level 3: Join Protocol (Handshake)

**Depends on:** Level 2

### L3.1 Invite Ticket Generation
```bash
# Test: Admin can generate invite ticket
TICKET=$(hive_cli alice hive-invite | jq -r '.ticket')
[ -n "$TICKET" ] && [ "$TICKET" != "null" ]
```

### L3.2 Ticket Expiry Options
```bash
# Test: Custom expiry is accepted
hive_cli alice hive-invite valid_hours=1 | jq -e '.ticket'
hive_cli alice hive-invite valid_hours=168 | jq -e '.ticket'
```

### L3.3 Peer Connection Requirement
```bash
# Test: Ensure Bob is connected to Alice before join
ALICE_PUBKEY=$(hive_cli alice getinfo | jq -r '.id')
hive_cli bob connect "${ALICE_PUBKEY}@polar-n1-alice:9735" 2>/dev/null || true
hive_cli bob listpeers | jq -e ".peers[] | select(.id == \"$ALICE_PUBKEY\")"
```

### L3.4 Join with Valid Ticket
```bash
# Test: Bob joins successfully
TICKET=$(hive_cli alice hive-invite | jq -r '.ticket')
hive_cli bob hive-join ticket="$TICKET" | jq -e '.status'
sleep 3  # Wait for handshake completion

# Verify Bob has a hive status
hive_cli bob hive-status | jq -e '.status == "active"'
```

### L3.5 Member Count Update
```bash
# Test: Alice now sees 2 members
hive_cli alice hive-members | jq -e '.count == 2'
```

### L3.6 Join Assigns Neophyte Tier
```bash
# Test: Bob joined as neophyte
BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
hive_cli alice hive-members | jq -e --arg pk "$BOB_PUBKEY" \
    '.members[] | select(.peer_id == $pk) | .tier == "neophyte"'
```

### L3.7 Carol Joins (Third Member)
```bash
# Test: Carol joins successfully
ALICE_PUBKEY=$(hive_cli alice getinfo | jq -r '.id')
hive_cli carol connect "${ALICE_PUBKEY}@polar-n1-alice:9735" 2>/dev/null || true

TICKET=$(hive_cli alice hive-invite | jq -r '.ticket')
hive_cli carol hive-join ticket="$TICKET" | jq -e '.status'
sleep 3

hive_cli alice hive-members | jq -e '.count == 3'
```

### L3.8 Expired Ticket Rejection
```bash
# Test: Expired ticket is rejected
# Note: This requires waiting for ticket expiry or mocking time
# Manual test: Generate ticket with valid_hours=0, wait, try to join
```

### L3.9 Invalid Ticket Rejection
```bash
# Test: Malformed ticket fails
! hive_cli carol hive-join ticket="invalid_base64_garbage"
```

---

## Level 4: Fee Policy Integration (Bridge)

**Depends on:** Level 3

### L4.1 Bridge Status
```bash
# Test: Bridge is enabled
hive_cli alice hive-status | jq -e '.version'
# Check logs for "Bridge ENABLED"
docker exec polar-n1-alice cat /home/clightning/.lightning/debug.log | grep -q "Bridge ENABLED"
```

### L4.2 Policy Sync on Startup
```bash
# Test: Policies are synced when plugin starts
docker exec polar-n1-alice cat /home/clightning/.lightning/debug.log | grep -q "Synced fee policies"
```

### L4.3 Member Gets HIVE Strategy
```bash
# First promote Bob to member (see Level 5), then:
BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
hive_cli alice revenue-policy get "$BOB_PUBKEY" | jq -e '.policy.strategy == "hive"'
```

### L4.4 Neophyte Gets Dynamic Strategy
```bash
# Test: Carol (neophyte) has dynamic strategy
CAROL_PUBKEY=$(hive_cli carol getinfo | jq -r '.id')
hive_cli alice revenue-policy get "$CAROL_PUBKEY" | jq -e '.policy.strategy == "dynamic"'
```

### L4.5 Admin Self-Policy
```bash
# Test: Alice's own policy is N/A (we don't set policy for ourselves)
# This is implied - no explicit test needed
```

### L4.6 Policy Update on Promotion
```bash
# Test: After promoting Bob, his policy changes to HIVE
# (Covered in Level 5 promotion tests)
```

---

## Level 5: Membership Tiers & Promotion

**Depends on:** Level 4

### L5.1 Current Tier Check
```bash
# Test: Each node knows its own tier
hive_cli alice hive-status | jq -e '.tier == "admin"' || true
hive_cli bob hive-status | jq -e '.tier == "neophyte"' || true
```

### L5.2 Neophyte Requests Promotion
```bash
# Test: Bob (neophyte) can request promotion
hive_cli bob hive-request-promotion | jq -e '.status'
```

### L5.3 Admin Can Vouch
```bash
# Test: Alice (admin) vouches for Bob
BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
hive_cli alice hive-vouch "$BOB_PUBKEY" | jq -e '.status == "vouched"'
```

### L5.4 Auto-Promotion on Quorum
```bash
# Test: With min-vouch-count=1, Bob is auto-promoted
BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
hive_cli alice hive-members | jq -e --arg pk "$BOB_PUBKEY" \
    '.members[] | select(.peer_id == $pk) | .tier == "member"'
```

### L5.5 Promoted Member Gets HIVE Policy
```bash
# Test: After promotion, Bob has HIVE strategy
BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
hive_cli alice revenue-policy get "$BOB_PUBKEY" | jq -e '.policy.strategy == "hive"'
```

### L5.6 Member Cannot Request Promotion
```bash
# Test: Bob (now member) cannot request promotion again
! hive_cli bob hive-request-promotion 2>&1 | grep -q "already.*member"
```

### L5.7 Neophyte Cannot Vouch
```bash
# Test: Carol (neophyte) cannot vouch for anyone
BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
! hive_cli carol hive-vouch "$BOB_PUBKEY" 2>&1 | grep -q "success"
```

### L5.8 Member Can Vouch
```bash
# Test: Bob (member) can now vouch for Carol
# First Carol requests promotion
hive_cli carol hive-request-promotion | jq -e '.status'
CAROL_PUBKEY=$(hive_cli carol getinfo | jq -r '.id')
hive_cli bob hive-vouch "$CAROL_PUBKEY" | jq -e '.status == "vouched"'
```

### L5.9 Quorum Calculation
```bash
# Test: Quorum is max(3, ceil(active_members * 0.51))
# With 2 active members (alice, bob), quorum = max(3, ceil(2*0.51)) = max(3, 2) = 3
# But with min-vouch-count=1 config, quorum is 1
```

---

## Level 6: State Synchronization (Gossip)

**Depends on:** Level 5

### L6.1 State Hash Consistency
```bash
# Test: All members have matching state hash
ALICE_HASH=$(hive_cli alice hive-status | jq -r '.state_hash // empty')
BOB_HASH=$(hive_cli bob hive-status | jq -r '.state_hash // empty')
CAROL_HASH=$(hive_cli carol hive-status | jq -r '.state_hash // empty')

# If state hashes are implemented, they should match
```

### L6.2 Member List Consistency
```bash
# Test: All nodes see the same members
ALICE_COUNT=$(hive_cli alice hive-members | jq '.count')
BOB_COUNT=$(hive_cli bob hive-members | jq '.count')
CAROL_COUNT=$(hive_cli carol hive-members | jq '.count')

[ "$ALICE_COUNT" = "$BOB_COUNT" ] && [ "$BOB_COUNT" = "$CAROL_COUNT" ]
```

### L6.3 Gossip on State Change
```bash
# Test: Changes propagate via gossip
# This is implicitly tested by member count consistency
```

### L6.4 Anti-Entropy on Reconnect
```bash
# Test: State sync happens when peers reconnect
# Disconnect Bob from Alice, reconnect, verify sync
```

### L6.5 Heartbeat Messages
```bash
# Test: Heartbeat messages are sent periodically
# Check logs for heartbeat activity
docker exec polar-n1-alice cat /home/clightning/.lightning/debug.log | grep -i heartbeat
```

---

## Level 7: Intent Lock Protocol

**Depends on:** Level 6

### L7.1 Intent Creation
```bash
# Test: Intent can be created via approve-action flow
# (Requires ADVISOR mode)
hive_cli alice hive-pending-actions | jq -e '.count >= 0'
```

### L7.2 Intent Broadcast
```bash
# Test: Intent is broadcast to all members
# This is implicit in the conflict resolution tests
```

### L7.3 Conflict Detection
```bash
# Test: Two nodes targeting same peer detect conflict
# Requires manual coordination or test harness
```

### L7.4 Deterministic Tie-Breaker
```bash
# Test: Lower pubkey wins conflict
# Requires comparing pubkeys: min(alice_pubkey, bob_pubkey) wins
ALICE_PUBKEY=$(hive_cli alice getinfo | jq -r '.id')
BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
echo "Alice: $ALICE_PUBKEY"
echo "Bob: $BOB_PUBKEY"
# Lower one should win in conflict
```

### L7.5 Intent Commit After Hold Period
```bash
# Test: Intent commits after hold_seconds if no conflict
# Requires waiting for hold period (default 30s)
```

### L7.6 Intent Abort on Conflict Loss
```bash
# Test: Loser aborts and broadcasts INTENT_ABORT
# Requires manual test scenario
```

---

## Level 8: Channel Operations

**Depends on:** Level 7, requires funded channels in Polar

### L8.1 Channel List Verification
```bash
# Test: Can list peer channels
hive_cli alice listpeerchannels | jq -e '.channels'
```

### L8.2 Open Channel to External Node
```bash
# Test: Alice opens channel to lnd1
# This requires on-chain funds - use Polar's funding feature
LND1_PUBKEY=$(lnd_cli lnd1 getinfo | jq -r '.identity_pubkey')
# hive_cli alice fundchannel "$LND1_PUBKEY" 1000000  # Requires funds
```

### L8.3 Intent Protocol for Channel Open
```bash
# Test: Channel open triggers Intent broadcast
# In ADVISOR mode, appears in pending-actions
# In AUTONOMOUS mode, broadcasts INTENT before executing
```

### L8.4 No Race Conditions
```bash
# Test: Two hive members don't open redundant channels to same target
# Requires coordinating two nodes and observing conflict resolution
```

### L8.5 Channel Opens to Hive Members
```bash
# Test: Open channel alice → bob (intra-hive)
BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
# hive_cli alice fundchannel "$BOB_PUBKEY" 1000000  # Requires funds
```

### L8.6 Fee Setting on New Channel
```bash
# Test: New channel to hive member gets HIVE fees (0 ppm)
# Verify via listpeerchannels fee_base_msat and fee_proportional_millionths
```

---

## Level 9: Routing & Contribution Tracking

**Depends on:** Level 8 (funded channels required)

### L9.1 Contribution Stats Available
```bash
# Test: Can query contribution stats
hive_cli alice hive-contribution | jq -e '.peer_id'
hive_cli alice hive-contribution | jq -e '.contribution_ratio >= 0'
```

### L9.2 Peer Contribution Query
```bash
# Test: Can query specific peer's contribution
BOB_PUBKEY=$(hive_cli bob getinfo | jq -r '.id')
hive_cli alice hive-contribution peer_id="$BOB_PUBKEY" | jq -e '.peer_id'
```

### L9.3 Forward Event Tracking
```bash
# Test: Forwards are tracked
# Requires routing a payment through the hive
# Create invoice on carol, pay from lnd1 through alice/bob
```

### L9.4 Contribution Ratio Calculation
```bash
# Test: Ratio = forwarded / received
# After routing payments, verify ratio updates
```

### L9.5 Zero Division Protection
```bash
# Test: Ratio handles zero received gracefully
# New members with no activity should show ratio 0.0 or Inf
```

---

## Level 10: Governance Modes

**Depends on:** Level 9

### L10.1 Default Mode Check
```bash
# Test: Default mode is ADVISOR
hive_cli alice hive-status | jq -e '.governance_mode == "advisor"'
```

### L10.2 Mode Change
```bash
# Test: Can change mode
hive_cli alice hive-set-mode mode=autonomous | jq -e '.new_mode == "autonomous"'
hive_cli alice hive-status | jq -e '.governance_mode == "autonomous"'

# Reset to advisor
hive_cli alice hive-set-mode mode=advisor
```

### L10.3 ADVISOR Mode Behavior
```bash
# Test: Actions are queued, not executed
# Trigger an action (e.g., via planner suggestion)
hive_cli alice hive-pending-actions | jq -e '.count >= 0'
```

### L10.4 Action Approval Flow
```bash
# Test: Can approve pending action
# If there's a pending action:
# ACTION_ID=$(hive_cli alice hive-pending-actions | jq -r '.actions[0].id')
# hive_cli alice hive-approve-action action_id=$ACTION_ID
```

### L10.5 Action Rejection Flow
```bash
# Test: Can reject pending action
# If there's a pending action:
# ACTION_ID=$(hive_cli alice hive-pending-actions | jq -r '.actions[0].id')
# hive_cli alice hive-reject-action action_id=$ACTION_ID
```

### L10.6 AUTONOMOUS Mode Safety Limits
```bash
# Test: Budget and rate limits are enforced
# Requires triggering multiple actions and checking limits
```

### L10.7 ORACLE Mode (Optional)
```bash
# Test: Oracle mode queries external API
# Requires oracle_url configuration
```

---

## Level 11: Planner & Topology

**Depends on:** Level 10

### L11.1 Topology Analysis
```bash
# Test: Can get topology analysis
hive_cli alice hive-topology | jq -e '.saturated_count >= 0'
hive_cli alice hive-topology | jq -e '.underserved_count >= 0'
```

### L11.2 Saturation Detection
```bash
# Test: Targets with >20% hive share are marked saturated
# Requires actual channels to verify
```

### L11.3 Underserved Detection
```bash
# Test: High-value targets with <5% share are underserved
```

### L11.4 Planner Log
```bash
# Test: Can view planner decisions
hive_cli alice hive-planner-log | jq -e '.logs'
hive_cli alice hive-planner-log limit=5 | jq -e '.logs | length <= 5'
```

### L11.5 CLBoss Ignore Integration
```bash
# Test: Saturated targets trigger clboss-ignore
# Check clboss-status or clboss-ignored list
```

### L11.6 Rate Limiting
```bash
# Test: Max 1 channel open intent per hour
# Requires observing planner behavior over time
```

---

## Level 12: Ban & Security

**Depends on:** Level 11

### L12.1 Admin Can Propose Ban
```bash
# Test: Admin can ban a peer
CAROL_PUBKEY=$(hive_cli carol getinfo | jq -r '.id')
hive_cli alice hive-ban "$CAROL_PUBKEY" reason="testing"
```

### L12.2 Ban Requires Consensus
```bash
# Test: Ban proposal goes through intent protocol
# Other members must also approve (in production config)
```

### L12.3 Banned Peer Removed
```bash
# Test: Banned peer is removed from members list
# After ban is executed:
# ! hive_cli alice hive-members | jq -e --arg pk "$CAROL_PUBKEY" \
#     '.members[] | select(.peer_id == $pk)'
```

### L12.4 Banned Peer Cannot Rejoin
```bash
# Test: Banned peer's join attempts are rejected
# Generate new ticket, try to join as banned peer
```

### L12.5 Leech Detection
```bash
# Test: Low contribution ratio triggers warnings
# Requires sustained low ratio (< 0.5) over time
```

---

## Level 13: Cross-Implementation Tests

**Depends on:** Level 8 (funded channels)

### L13.1 LND Node Accessibility
```bash
# Test: Can communicate with LND nodes
lnd_cli lnd1 getinfo | jq -e '.identity_pubkey'
lnd_cli lnd2 getinfo | jq -e '.identity_pubkey'
```

### L13.2 Channel to LND
```bash
# Test: Hive member can open channel to LND
# alice → lnd1 channel
```

### L13.3 Routing Through LND
```bash
# Test: Payments route through LND nodes
# Create invoice on lnd1, pay from carol
```

### L13.4 Eclair Node Accessibility (Optional)
```bash
# Test: Can communicate with Eclair nodes
# docker exec polar-n1-eclair1 eclair-cli getinfo
```

### L13.5 Channel to Eclair (Optional)
```bash
# Test: Hive member can open channel to Eclair
```

### L13.6 Mixed Network Routing
```bash
# Test: Payment routes through mixed CLN/LND/Eclair path
```

---

## Level 14: Failure & Recovery

**Depends on:** All previous levels

### L14.1 Plugin Restart Recovery
```bash
# Test: Plugin recovers state after restart
hive_cli alice plugin stop cl-hive
sleep 2
hive_cli alice plugin start /home/clightning/.lightning/plugins/cl-hive/cl-hive.py

# Verify state is preserved
hive_cli alice hive-status | jq -e '.status == "active"'
hive_cli alice hive-members | jq -e '.count >= 1'
```

### L14.2 Node Restart Recovery
```bash
# Test: State survives node restart
# Restart alice container in Polar
# Verify hive state is restored from database
```

### L14.3 Network Partition Recovery
```bash
# Test: Anti-entropy sync after reconnection
# Disconnect bob from alice, make changes, reconnect
# Verify state converges
```

### L14.4 Bridge Failure Handling
```bash
# Test: cl-hive survives if cl-revenue-ops crashes
hive_cli alice plugin stop cl-revenue-ops
# cl-hive should log warning but not crash
hive_cli alice hive-status | jq -e '.status'
# Restart revenue-ops
hive_cli alice plugin start /home/clightning/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py
```

### L14.5 CLBoss Failure Handling
```bash
# Test: cl-hive survives if clboss crashes
hive_cli alice plugin stop clboss
hive_cli alice hive-status | jq -e '.status'
# Restart clboss
hive_cli alice plugin start /home/clightning/.lightning/plugins/clboss
```

### L14.6 Database Corruption Recovery
```bash
# Test: Graceful handling of database issues
# (Manual test - corrupt database and observe behavior)
```

---

## Test Execution Order

### Phase 1: Basic Setup (No Channels Required)
1. Level 0: Environment Setup
2. Level 1: Plugin Loading
3. Level 2: Genesis & Identity
4. Level 3: Join Protocol
5. Level 4: Fee Policy Integration
6. Level 5: Membership Tiers & Promotion

### Phase 2: State & Coordination (No Channels Required)
7. Level 6: State Synchronization
8. Level 7: Intent Lock Protocol

### Phase 3: Channel Operations (Requires Polar Funding)
9. Level 8: Channel Operations
10. Level 9: Routing & Contribution Tracking

### Phase 4: Advanced Features
11. Level 10: Governance Modes
12. Level 11: Planner & Topology
13. Level 12: Ban & Security
14. Level 13: Cross-Implementation Tests
15. Level 14: Failure & Recovery

---

## Quick Reference: Current Test Coverage

| Level | Status | test.sh Category |
|-------|--------|------------------|
| L0-L1 | Tested | `setup` |
| L2 | Tested | `genesis` |
| L3 | Tested | `join` |
| L4 | Tested | `fees` |
| L5 | Tested | `promotion` |
| L6 | Tested | `sync` |
| L7 | Tested | `intent` |
| L8 | Tested | `channels` |
| L9 | Tested | `contrib` |
| L10 | Tested | `governance` |
| L11 | Tested | `planner` |
| L12 | Tested | `security` |
| L13 | Partial | `cross` (LND TLS config issue) |
| L14 | Tested | `recovery` |

---

## Running Tests

### Automated Tests
```bash
cd /home/sat/cl-hive/docs/testing

# Run all implemented tests (115 tests)
./test.sh all 1

# Run specific category
./test.sh setup 1       # L0-L1: Environment setup
./test.sh genesis 1     # L2: Genesis creation
./test.sh join 1        # L3: Join protocol
./test.sh promotion 1   # L5: Member promotion
./test.sh fees 1        # L4: Fee policy integration
./test.sh sync 1        # L6: State synchronization
./test.sh intent 1      # L7: Intent lock protocol
./test.sh channels 1    # L8: Channel operations
./test.sh contrib 1     # L9: Contribution tracking
./test.sh governance 1  # L10: Governance modes
./test.sh planner 1     # L11: Planner & topology
./test.sh security 1    # L12: Security & bans
./test.sh cross 1       # L13: Cross-implementation
./test.sh recovery 1    # L14: Failure recovery

# Reset and start fresh
./test.sh reset 1
./setup-hive.sh 1
./test.sh all 1
```

### Manual Test Execution
```bash
# Set up CLI helper
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"
hive_cli() { docker exec polar-n1-$1 $CLI "${@:2}"; }

# Run individual tests from this plan
# Copy/paste commands from each level
```

---

## Adding New Tests

When implementing new tests, add them to `test.sh` following this pattern:

```bash
test_<category>() {
    echo ""
    echo "========================================"
    echo "<CATEGORY> TESTS"
    echo "========================================"

    run_test "Test description" "command | jq -e 'condition'"
    run_test_expect_fail "Should fail" "command that should fail"
}
```

Update the case statement in `test.sh` to include the new category.

---

*Testing Plan Version: 1.0*
*Last Updated: January 2026*
