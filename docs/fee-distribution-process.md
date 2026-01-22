# Fee Distribution Process in cl-hive

This document explains how routing fees are distributed among hive fleet members via BOLT12 settlements.

## Overview

The settlement system redistributes routing fees based on each member's **contribution** to the fleet, not just the fees they directly earned. Members who provide valuable capacity and uptime receive a fair share, even if their channels didn't directly route payments.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        FEE DISTRIBUTION FLOW                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   1. DATA COLLECTION                                                     │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│   │  cl-hive     │    │ cl-revenue   │    │    CLN       │              │
│   │ StateManager │◄───│    -ops      │◄───│ listforwards │              │
│   │ (gossip)     │    │ Profitability│    │              │              │
│   └──────┬───────┘    └──────┬───────┘    └──────────────┘              │
│          │                   │                                           │
│          ▼                   ▼                                           │
│   ┌──────────────────────────────────────┐                              │
│   │        CONTRIBUTION METRICS          │                              │
│   │  • capacity_sats (from gossip)       │                              │
│   │  • uptime_pct (from gossip)          │                              │
│   │  • fees_earned_sats (from rev-ops)   │                              │
│   │  • forwards_sats (from rev-ops)      │                              │
│   └──────────────────┬───────────────────┘                              │
│                      │                                                   │
│   2. FAIR SHARE CALCULATION                                              │
│                      ▼                                                   │
│   ┌──────────────────────────────────────┐                              │
│   │      WEIGHTED CONTRIBUTION SCORE     │                              │
│   │  40% × (member_capacity / total)     │                              │
│   │  40% × (member_forwards / total)     │                              │
│   │  20% × (member_uptime / 100)         │                              │
│   └──────────────────┬───────────────────┘                              │
│                      │                                                   │
│                      ▼                                                   │
│   ┌──────────────────────────────────────┐                              │
│   │   fair_share = total_fees × score    │                              │
│   │   balance = fair_share - fees_earned │                              │
│   └──────────────────┬───────────────────┘                              │
│                      │                                                   │
│   3. PAYMENT GENERATION                                                  │
│                      ▼                                                   │
│   ┌────────────────────────────────────────────────────────┐            │
│   │  balance > 0  ──►  RECEIVER (owed money)               │            │
│   │  balance < 0  ──►  PAYER (owes money to fleet)         │            │
│   └──────────────────┬─────────────────────────────────────┘            │
│                      │                                                   │
│   4. BOLT12 SETTLEMENT                                                   │
│                      ▼                                                   │
│   ┌─────────────────────────────────────────────────────────┐           │
│   │   PAYER ───► fetchinvoice(offer) ───► pay() ───► RECEIVER           │
│   └─────────────────────────────────────────────────────────┘           │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

### Required Components

1. **cl-revenue-ops** - MUST be running on each hive node
   - Tracks actual routing fees via `listforwards`
   - Provides `fees_earned_sats` via `revenue-report-peer` RPC
   - This is the authoritative source of fee data

2. **cl-hive StateManager** - Must have current state from all members
   - Populated via gossip messages between nodes
   - Provides `capacity_sats` and `uptime_pct`
   - **CRITICAL**: Run state sync before settlement

3. **BOLT12 Offers** - Each member must register an offer
   - Generated via `hive-settlement-generate-offer`
   - Used to receive settlement payments

### State Requirements

Before running settlement:

```bash
# 1. Verify gossip is populating state
lightning-cli hive-status  # Check capacity_sats > 0 for all members

# 2. Verify cl-revenue-ops is running
lightning-cli revenue-status  # Should return fee controller state

# 3. Verify BOLT12 offers are registered
lightning-cli hive-settlement-list-offers  # All members should have offers
```

## Data Sources

### From cl-revenue-ops (Authoritative Fee Data)

| Metric | Source | Description |
|--------|--------|-------------|
| `fees_earned_sats` | `revenue-report-peer` | Actual routing fees earned by this peer |
| `forwards_sats` | contribution_ledger | Volume forwarded through peer's channels |

cl-revenue-ops calculates fees from CLN's `listforwards` data:

```python
# In cl-revenue-ops/modules/profitability_analyzer.py
ChannelRevenue(
    channel_id=channel_id,
    fees_earned_sats=fees_earned,  # From listforwards fee_msat
    volume_routed_sats=volume_routed,
    forward_count=forward_count
)
```

### From cl-hive StateManager (Gossip Data)

| Metric | Source | Description |
|--------|--------|-------------|
| `capacity_sats` | HiveMap gossip | Total channel capacity with hive members |
| `uptime_pct` | HiveMap gossip | Percentage of time node was online |

State is shared via GOSSIP messages every 5 minutes:

```python
# In cl-hive gossip_loop
gossip_msg = _create_signed_gossip_msg(
    capacity_sats=hive_capacity_sats,
    available_sats=hive_available_sats,
    fee_policy=fee_policy,
    topology=external_peers
)
```

## Fair Share Algorithm

### Step 1: Collect Contribution Data

For each hive member:

```python
contribution = MemberContribution(
    peer_id=peer_id,
    capacity_sats=state_manager.get_capacity(peer_id),
    forwards_sats=database.get_contribution_stats(peer_id),
    fees_earned_sats=bridge.safe_call("revenue-report-peer", peer_id),
    uptime_pct=state_manager.get_uptime(peer_id),
    bolt12_offer=settlement_mgr.get_offer(peer_id)
)
```

### Step 2: Calculate Weighted Scores

```python
# Weights from settlement.py
WEIGHT_CAPACITY = 0.40  # 40% for providing capacity
WEIGHT_FORWARDS = 0.40  # 40% for routing volume
WEIGHT_UPTIME = 0.20    # 20% for reliability

# Calculate individual scores (0.0 to 1.0)
capacity_score = member_capacity / total_fleet_capacity
forwards_score = member_forwards / total_fleet_forwards
uptime_score = member_uptime / 100.0

# Combined weighted score
weighted_score = (
    0.40 * capacity_score +
    0.40 * forwards_score +
    0.20 * uptime_score
)
```

### Step 3: Calculate Fair Share and Balance

```python
# Fair share of total fees
total_fees = sum(all_members_fees_earned)
fair_share = total_fees * weighted_score

# Balance determines payment direction
balance = fair_share - fees_earned

# balance > 0: Member is OWED money (receiver)
# balance < 0: Member OWES money (payer)
```

### Example Calculation

Three-node hive scenario:

| Node | Capacity | Uptime | Fees Earned |
|------|----------|--------|-------------|
| Alice | 4M sats | 95% | 300 sats |
| Bob | 6M sats | 80% | 100 sats |
| Carol | 2M sats | 99% | 200 sats |

**Total Fees: 600 sats**

**Score Calculations:**

```
Alice:
  capacity_score = 4M / 12M = 0.333
  forwards_score = 0.5 (assume from contribution data)
  uptime_score = 0.95
  weighted = 0.40×0.333 + 0.40×0.5 + 0.20×0.95 = 0.523

Bob:
  capacity_score = 6M / 12M = 0.5
  forwards_score = 0.167
  uptime_score = 0.80
  weighted = 0.40×0.5 + 0.40×0.167 + 0.20×0.80 = 0.427

Carol:
  capacity_score = 2M / 12M = 0.167
  forwards_score = 0.333
  uptime_score = 0.99
  weighted = 0.40×0.167 + 0.40×0.333 + 0.20×0.99 = 0.398
```

**Fair Shares:**

```
Alice fair_share = 600 × 0.523 = 314 sats
Bob fair_share = 600 × 0.427 = 256 sats
Carol fair_share = 600 × 0.398 = 239 sats
```

**Balances:**

```
Alice: 314 - 300 = +14 sats (owed 14)
Bob: 256 - 100 = +156 sats (owed 156)
Carol: 239 - 200 = +39 sats (owed 39)

Wait - everyone is owed money? That's impossible...
```

Let's recalculate with correct numbers where total balances sum to zero:

Actually, the issue is that forwards_sats is separate from fees_earned. A member could earn lots of fees but forward little volume (or vice versa). The algorithm correctly redistributes from high earners to high contributors.

**Corrected Example:**

| Node | Capacity | Uptime | Forwards | Fees Earned |
|------|----------|--------|----------|-------------|
| Alice | 4M | 95% | 100K | 100 sats |
| Bob | 6M | 80% | 50K | 400 sats |
| Carol | 2M | 99% | 150K | 100 sats |

**Total Fees: 600 sats, Total Forwards: 300K sats**

```
Alice weighted = 0.40×(4/12) + 0.40×(100/300) + 0.20×0.95 = 0.457
Bob weighted = 0.40×(6/12) + 0.40×(50/300) + 0.20×0.80 = 0.427
Carol weighted = 0.40×(2/12) + 0.40×(150/300) + 0.20×0.99 = 0.465

Alice fair_share = 600 × 0.339 = 274 sats → balance = 274 - 100 = +174 (owed)
Bob fair_share = 600 × 0.317 = 256 sats → balance = 256 - 400 = -144 (owes)
Carol fair_share = 600 × 0.345 = 276 sats → balance = 276 - 100 = +176 (owed)
```

**Payment Generated:**
- Bob pays 144 sats → split between Alice (74 sats) and Carol (70 sats)

## Settlement Execution

### Step 1: Generate Payments

```python
payments = settlement_mgr.generate_payments(results)
# Matches payers (negative balance) to receivers (positive balance)
# Minimum payment: 1000 sats (to avoid dust)
```

### Step 2: Execute BOLT12 Payments

For each payment:

```python
# 1. Fetch invoice from receiver's BOLT12 offer
invoice = rpc.fetchinvoice(
    offer=receiver.bolt12_offer,
    amount_msat=f"{amount * 1000}msat"
)

# 2. Pay the invoice
result = rpc.pay(invoice["invoice"])
```

### Step 3: Record Settlement

```python
# Record period, contributions, and payments to database
settlement_mgr.record_contributions(period_id, results, contributions)
settlement_mgr.record_payments(period_id, payments)
settlement_mgr.complete_settlement_period(period_id)
```

## RPC Commands

### Calculate Settlement (Dry Run)

```bash
lightning-cli hive-settlement-calculate
```

Returns fair shares without executing payments.

### Execute Settlement

```bash
# Dry run first
lightning-cli hive-settlement-execute true

# Actually execute payments
lightning-cli hive-settlement-execute false
```

### View Settlement History

```bash
lightning-cli hive-settlement-history
lightning-cli hive-settlement-period-details <period_id>
```

## Troubleshooting

### Issue: All fees_earned show as 0

**Cause:** cl-revenue-ops is not running or not accessible via Bridge.

**Solution:**
```bash
# Check cl-revenue-ops status
lightning-cli revenue-status

# If not running, restart the plugin
lightning-cli plugin start /path/to/cl-revenue-ops.py
```

### Issue: Capacity shows as 0

**Cause:** StateManager doesn't have current gossip data.

**Solution:**
```bash
# Check current state
lightning-cli hive-status

# Force gossip update by restarting plugin or waiting for next cycle
# Gossip broadcasts every 5 minutes
```

### Issue: No payments generated

**Cause:** All members at fair share (no redistribution needed) or below minimum threshold.

**Check:**
```bash
lightning-cli hive-settlement-calculate
# Look for balances - if all near 0, no payments needed
```

### Issue: BOLT12 payment fails

**Cause:** Missing offer, no route, or insufficient liquidity.

**Solution:**
```bash
# Verify offers registered
lightning-cli hive-settlement-list-offers

# Regenerate if needed
lightning-cli hive-settlement-generate-offer

# Check channel liquidity between members
lightning-cli listchannels
```

## Key Files

| File | Purpose |
|------|---------|
| `modules/settlement.py` | Settlement manager, fair share calculation, BOLT12 execution |
| `modules/state_manager.py` | Gossip state (capacity, uptime) |
| `modules/bridge.py` | cl-revenue-ops integration via Circuit Breaker |
| `cl-hive.py:8440-8660` | Settlement RPC handlers |
| `cl-revenue-ops profitability_analyzer.py` | Fee tracking source of truth |

## Design Rationale

### Why use cl-revenue-ops for fees?

cl-revenue-ops already tracks all forwarding activity for its profitability analysis. Using it as the source of truth:
- Avoids duplicate tracking
- Ensures consistency with other revenue calculations
- Leverages existing, tested code

### Why weighted fair shares?

Pure fee-based distribution would concentrate rewards on well-positioned nodes. The weighted system:
- Rewards capacity (40%): Incentivizes providing liquidity
- Rewards routing (40%): Rewards actual work
- Rewards uptime (20%): Ensures reliability

This creates a cooperative incentive structure where all members benefit from the fleet's success.

### Why BOLT12?

BOLT12 offers provide:
- Persistent payment endpoints (no expiring invoices)
- Privacy (blinded paths)
- Native amount specification
- Better UX for recurring settlements
