# DID + Cashu Hive Settlements Protocol

**Status:** Proposal / Design Draft  
**Author:** Hex (`did:cid:bagaaierajrr7k6izcrdfwqxpgtrobflsv5oibymfnthjazkkokaugszyh4ka`)  
**Date:** 2026-02-14  
**Feedback:** Open — file issues or comment in #singularity

---

## Abstract

This document defines a trustless settlement protocol for the Lightning Hive. It specifies how obligations between hive nodes — routing revenue shares, rebalancing costs, liquidity leases, splice contributions, pheromone market fees, intelligence payments, and penalty slashing — are tracked, netted, escrowed, and settled using Archon DIDs for identity, Cashu escrow tickets for conditional payment, and the DID Reputation Schema for trust calibration.

The result is a system where nodes operated by different parties can participate in the same hive without trusting each other. Obligations accumulate during normal hive operation, are periodically netted to minimize token volume, and settle through Cashu escrow tickets with cryptographic proof of work performed. Nodes that defect lose bonds and reputation. Nodes that cooperate earn credit lines and better terms.

---

## Motivation

### The Trust Problem at Scale

The Lightning Hive coordinates fleets of Lightning nodes through pheromone markers, gossip protocols, and stigmergic signals. Today, settlements between hive nodes are internal accounting — a ledger entry in the hive coordinator's database. This works when one operator controls all nodes. It breaks the moment a second operator joins.

#### Stage 1: Single-Operator Fleet

One operator, multiple nodes. All revenue, all costs, one wallet. No settlement needed — it's just moving money between your own pockets.

**Trust requirement:** None. You trust yourself.

#### Stage 2: Multi-Operator Fleet

Two or more operators pool their nodes into a hive for better routing, shared intelligence, and coordinated liquidity. Node A forwards HTLCs through Node B's channels. Node B rebalances using Node A's liquidity. Who owes whom?

**Trust requirement:** Bilateral trust between known operators. Handshake deals, spreadsheets, manual settlement. Works for 2–5 operators who know each other. Doesn't scale.

**Failure modes:**
- Operator A claims they forwarded 500 HTLCs; Operator B says 300. No verifiable proof.
- Operator B rebalanced through Operator A's channels but disputes the fee charged.
- One operator stops paying. The other has no recourse except leaving the hive.

#### Stage 3: Open Hive Membership

Any node with sufficient bond and reputation can join the hive. Operators don't know each other personally. The hive grows to dozens or hundreds of nodes across the globe.

**Trust requirement:** Zero trust between operators. The protocol must enforce correct settlement through cryptography and economic incentives. This is what this spec builds.

### Why Not Just Lightning Payments?

Settling every inter-node obligation with a Lightning payment has problems:

| Issue | Impact |
|-------|--------|
| Routing fees accumulate | Hive nodes paying routing fees to settle with each other is circular and wasteful |
| Requires online sender | Nodes may be intermittently connected |
| No conditionality | Lightning payments are unconditional — no "pay only if work was verified" |
| No netting | Every obligation requires a separate payment; no way to offset bilateral debts |
| Privacy leakage | Routing nodes observe settlement payments between hive members |

Cashu escrow tickets solve all of these. Bearer tokens with conditional spending, offline capability, perfect netting compatibility, and blind signature privacy.

---

## Settlement Types

### 1. Routing Revenue Sharing

**Scenario:** Node A forwarded HTLCs through Node B's channels (or vice versa). The hive's coordinated routing directed traffic through a path spanning multiple operators' nodes. Revenue should be split based on each node's contribution to the forwarding chain.

**Obligation calculation:**

```
For each forwarded HTLC through a multi-operator path:
  total_fee = fee collected by the forwarding chain
  contribution(node_i) = proportional to:
    - Channel capacity committed
    - Liquidity consumed (directional)
    - Position in route (source/sink premium)
    - Opportunity cost (what else could that liquidity have earned?)

  share(node_i) = total_fee × contribution(node_i) / Σ contributions
```

**Proof mechanism:** Signed forwarding receipts. Each node in the hive path signs an `HTLCForwardReceipt` containing:

```json
{
  "type": "HTLCForwardReceipt",
  "htlc_id": "<payment_hash>:<channel_id>",
  "amount_msat": 500000,
  "fee_msat": 150,
  "incoming_channel": "931770x2363x0",
  "outgoing_channel": "932263x1883x0",
  "timestamp": "2026-02-14T12:34:56Z",
  "hive_path_id": "<deterministic hash of full path>",
  "signer": "did:cid:<node_did>",
  "signature": "<secp256k1 sig over above fields>"
}
```

Both the incoming and outgoing nodes sign the receipt. A complete routing proof is a chain of receipts covering the full path.

**Settlement frequency:** Batched. Routing receipts accumulate over a settlement window (default: 24 hours). At settlement, bilateral net amounts are computed and settled via Cashu tickets.

### 2. Rebalancing Cost Settlement

**Scenario:** Node A requested (or the hive coordinator recommended) a rebalance that used Node B's liquidity. Node B bears opportunity cost — those sats were committed to A's rebalance instead of earning routing fees.

**Obligation calculation:**

```
rebalance_cost(B) =
  routing_fees_paid_through_B +
  opportunity_cost(B, amount, duration) +
  B's_risk_premium
```

Opportunity cost is estimated from B's recent routing revenue per sat of capacity. Risk premium is configurable per node.

**Proof mechanism:** Signed rebalance receipts from both endpoints:

```json
{
  "type": "RebalanceReceipt",
  "rebalance_id": "<unique_id>",
  "initiator": "did:cid:<node_a_did>",
  "liquidity_provider": "did:cid:<node_b_did>",
  "amount_sats": 500000,
  "route_fees_paid_msat": 2500,
  "channels_used": ["931770x2363x0", "932263x1883x0"],
  "duration_seconds": 45,
  "timestamp": "2026-02-14T13:00:00Z",
  "initiator_signature": "<sig>",
  "provider_signature": "<sig>"
}
```

Both parties sign. If either refuses to sign, the rebalance obligation is disputed (see [Dispute Resolution](#dispute-resolution)).

### 3. Channel Leasing / Liquidity Rental

**Scenario:** Node A wants inbound liquidity from Node B. B opens a channel to A (or keeps an existing channel well-balanced toward A) for a defined period. A pays B for this time-bounded access to capacity.

**Obligation calculation:**

```
lease_cost = capacity_sats × lease_rate_ppm × lease_duration_days / 365
```

Lease rate is market-driven — nodes advertise rates via pheromone markers.

**Proof mechanism:** Periodic heartbeat attestations. The lessee (A) and lessor (B) exchange signed heartbeats confirming the leased capacity was available:

```json
{
  "type": "LeaseHeartbeat",
  "lease_id": "<unique_id>",
  "lessor": "did:cid:<node_b_did>",
  "lessee": "did:cid:<node_a_did>",
  "capacity_sats": 5000000,
  "direction": "inbound_to_lessee",
  "available": true,
  "measured_at": "2026-02-14T14:00:00Z",
  "lessor_signature": "<sig>"
}
```

Heartbeats are exchanged every hour (configurable). If a heartbeat is missed or shows `available: false`, the lease payment is prorated. Three consecutive missed heartbeats terminate the lease.

**Escrow:** The full lease payment is escrowed upfront in a Cashu ticket with progressive release — a milestone ticket where each day's portion is released upon that day's heartbeat attestations.

**DID + macaroon integration:** The lease is formalized as a `HiveLeaseMacaroon` — an L402 macaroon with caveats binding it to the lessee's DID, the capacity amount, and the lease duration. The macaroon serves as a bearer proof of the lease agreement.

### 4. Cooperative Splicing Settlements

**Scenario:** Multiple hive members participate in a splice transaction — adding or removing funds from an existing channel. Each participant's contribution ratio determines their future revenue share from that channel.

**Obligation calculation:**

```
revenue_share(node_i) = contribution(node_i) / total_channel_capacity_after_splice
```

Revenue share is recalculated at each splice event. Historical contribution is tracked.

**Proof mechanism:** On-chain transaction verification. The splice transaction is a Bitcoin transaction with inputs from multiple parties. Each input is signed by the contributing node's key. The transaction itself is the proof.

```json
{
  "type": "SpliceReceipt",
  "channel_id": "931770x2363x0",
  "splice_txid": "abc123...",
  "participants": [
    { "did": "did:cid:<node_a>", "contribution_sats": 2000000, "share_pct": 40 },
    { "did": "did:cid:<node_b>", "contribution_sats": 3000000, "share_pct": 60 }
  ],
  "new_capacity_sats": 5000000,
  "timestamp": "2026-02-14T15:00:00Z",
  "signatures": ["<sig_a>", "<sig_b>"]
}
```

**Escrow:** Each participant's future revenue share is enforced through ongoing routing revenue sharing tickets (Type 1). The splice receipt becomes the authoritative source for share ratios.

### 5. Shared Channel Opens

**Scenario:** Multiple hive members co-fund a new channel to a strategically important peer. The channel is opened with combined funds, and future routing revenue is split by contribution ratio.

This is structurally identical to cooperative splicing but for new channels. The key difference: there's no existing channel to modify, so the initial funding transaction requires more coordination.

**Proof mechanism:** Same as splicing — the funding transaction with multi-party inputs is on-chain proof. A `SharedChannelReceipt` records contribution ratios.

**Revenue distribution:** Routing revenue from the shared channel is accumulated and distributed per settlement window according to the recorded contribution ratios.

### 6. Pheromone Market

**Scenario:** Nodes pay for priority pheromone placement — advertising their routes as preferred paths through the hive's stigmergic signaling system. This is essentially paying for route advertising.

**Obligation calculation:**

```
pheromone_cost = base_placement_fee + (priority_level × priority_multiplier)
```

Priority levels: `standard` (free, best-effort), `boosted` (2× visibility), `premium` (guaranteed top placement for duration).

**Proof mechanism:** The escrow ticket's HTLC secret is revealed when routing actually flows through the advertised path. This makes pheromone advertising pay-for-performance:

```
Advertiser pays → Escrow ticket created
  HTLC secret held by: the next node in the advertised path
  Secret revealed when: an HTLC is successfully forwarded through the path
  Timeout: if no traffic within the placement window, advertiser reclaims
```

```json
{
  "type": "PheromoneReceipt",
  "pheromone_id": "<marker_hash>",
  "advertiser": "did:cid:<node_did>",
  "path_advertised": ["03abc...", "03def...", "03ghi..."],
  "placement_level": "boosted",
  "htlcs_routed": 12,
  "total_amount_routed_msat": 5000000,
  "period": { "start": "2026-02-14T00:00:00Z", "end": "2026-02-14T12:00:00Z" },
  "verifier_signatures": ["<sig from each path node>"]
}
```

### 7. Intelligence Sharing

**Scenario:** Nodes pay for routing intelligence data — success rates, fee maps, liquidity estimates, channel health assessments. Better data leads to better routing decisions.

**Obligation calculation:**

```
intelligence_cost = base_query_fee + (data_freshness_premium × recency_factor)
```

Premium for real-time data vs. stale historical data.

**Proof mechanism:** Correlation-based. The escrow ticket's HTLC secret is revealed when the purchased data demonstrably led to successful routes:

```
Buyer requests intelligence → Seller provides data + holds HTLC secret
  Buyer uses data to route payments
  If routes succeed at rates better than baseline:
    Buyer acknowledges value → Secret revealed → Seller paid
  If data was stale/wrong:
    Timeout → Buyer reclaims
```

```json
{
  "type": "IntelligenceReceipt",
  "query_id": "<unique_id>",
  "seller": "did:cid:<seller_did>",
  "buyer": "did:cid:<buyer_did>",
  "data_type": "fee_map",
  "data_hash": "sha256:<hash_of_provided_data>",
  "routing_success_before": 0.72,
  "routing_success_after": 0.89,
  "measurement_window_hours": 6,
  "buyer_signature": "<sig>",
  "seller_signature": "<sig>"
}
```

**Verification challenge:** Correlation doesn't prove causation. A node's routing success might improve for reasons unrelated to the purchased data. The protocol uses a statistical approach: if routing success improves by more than a threshold (configurable, default: 10% relative improvement) within the measurement window, the data is deemed useful.

### 8. Penalty Settlements

**Scenario:** A node violated hive policy. Examples:
- Fee undercutting — setting fees below the hive's coordinated minimum, stealing traffic
- Unannounced channel close — closing a channel that other hive members depended on for routing
- Data leakage — sharing hive intelligence with non-members
- Free-riding — consuming hive routing intelligence without contributing data
- Heartbeat failure — repeatedly failing to respond to hive coordination messages

**Obligation calculation:**

```
penalty = base_penalty(violation_type) × severity_multiplier × repeat_offender_multiplier
```

| Violation | Base Penalty | Severity Range |
|-----------|-------------|----------------|
| Fee undercutting | 1,000 sats | 1–5× (based on magnitude) |
| Unannounced close | 10,000 sats | 1–10× (based on channel size) |
| Data leakage | 50,000 sats | 1–5× (based on sensitivity) |
| Free-riding | 5,000 sats | 1–3× (based on duration) |
| Heartbeat failure | 500 sats | 1× per missed window |

**Proof mechanism:** Policy violation is detected by peer nodes and reported with signed evidence:

```json
{
  "type": "ViolationReport",
  "violation_type": "fee_undercutting",
  "offender": "did:cid:<offender_did>",
  "reporter": "did:cid:<reporter_did>",
  "evidence": {
    "channel_id": "931770x2363x0",
    "observed_fee_ppm": 5,
    "hive_minimum_fee_ppm": 50,
    "gossip_timestamp": "2026-02-14T16:00:00Z"
  },
  "reporter_signature": "<sig>"
}
```

Violations require quorum confirmation — at least N/2+1 hive members must independently observe and report the violation before penalty is applied. This prevents false accusation attacks.

**Penalty execution:** The penalty is deducted from the offender's posted bond (see [Bond System](#bond-system)). If the bond is insufficient, the node's reputation is slashed and future settlement terms worsen.

---

## Settlement Protocol Flow

### Obligation Accumulation

During normal hive operation, obligations accumulate as structured events in each node's local settlement ledger:

```
┌──────────────────────────────────────────────────────────────┐
│                   Node A Settlement Ledger                    │
│                                                               │
│  [2026-02-14 12:00] ROUTING_SHARE  +150 msat  from Node B   │
│  [2026-02-14 12:01] ROUTING_SHARE  -80 msat   to Node C     │
│  [2026-02-14 12:15] REBALANCE_COST -2500 msat to Node B     │
│  [2026-02-14 12:30] LEASE_PAYMENT  -5000 msat to Node D     │
│  [2026-02-14 13:00] INTEL_PAYMENT  -100 msat  to Node E     │
│  [2026-02-14 13:05] ROUTING_SHARE  +200 msat  from Node C   │
│  [2026-02-14 13:10] PHEROMONE_FEE  -50 msat   to Node B     │
│  ...                                                          │
└──────────────────────────────────────────────────────────────┘
```

Each entry is backed by a signed receipt (routing receipts, rebalance receipts, etc.). The ledger is append-only and cryptographically committed — each entry includes a hash of the previous entry, forming a hash chain.

### Settlement Windows

Settlement windows are configurable per-node and per-relationship:

| Mode | Window | Best For | Overhead |
|------|--------|----------|----------|
| **Real-time micro** | Per-event | Low-trust relationships, small amounts | High (1 ticket per event) |
| **Hourly batch** | 1 hour | Active routing relationships | Medium |
| **Daily batch** | 24 hours | Standard hive members | Low |
| **Weekly batch** | 7 days | Highly trusted, high-volume relationships | Minimal |

Settlement mode is negotiated during the hive PKI handshake and can be adjusted based on trust tier (see [Credit and Trust Tiers](#credit-and-trust-tiers)).

### Netting

Before creating Cashu escrow tickets, obligations are netted to minimize token volume.

#### Bilateral Netting

Between any two nodes, all obligations in the settlement window are summed:

```
net_obligation(A→B) = Σ (A owes B) - Σ (B owes A)

If net_obligation > 0: A pays B
If net_obligation < 0: B pays A
If net_obligation = 0: No settlement needed
```

**Example:**
```
A owes B: 150 (routing) + 2500 (rebalance) + 50 (pheromone) = 2700 msat
B owes A: 300 (routing) = 300 msat
Net: A pays B 2400 msat
```

One Cashu ticket instead of four.

#### Multilateral Netting

For hives with many members, multilateral netting further reduces settlement volume. The netting algorithm finds the minimum set of payments that satisfies all net obligations:

```
Given N nodes with bilateral net obligations:
  Compute net position for each node:
    net_position(i) = Σ (all owed to i) - Σ (all owed by i)
  
  Nodes with positive net position are net receivers
  Nodes with negative net position are net payers
  
  Minimum payments = max(|net_receivers|, |net_payers|) - 1
```

**Example with 4 nodes:**
```
Bilateral nets:
  A→B: 1000    B→C: 500    C→D: 300
  A→C: 200     B→D: 400
  
Net positions:
  A: -1200 (net payer)
  B: +100  (net receiver)
  C: +400  (net receiver)
  D: +700  (net receiver)
  
Multilateral settlement (3 payments instead of 5):
  A→B: 100
  A→C: 400
  A→D: 700
```

Multilateral netting requires all nodes to agree on the obligation set. This is achieved through the gossip protocol — nodes exchange signed obligation summaries and verify they agree on bilateral nets before computing the multilateral solution.

### Cashu Escrow Ticket Flow

After netting, each net obligation becomes a Cashu escrow ticket following the [DID + Cashu Task Escrow Protocol](./DID-CASHU-TASK-ESCROW.md).

#### For Routine Settlements (Routing Revenue, Rebalancing Costs)

```
Net Payer (A)                  Net Receiver (B)              Mint
     │                              │                          │
     │  1. Compute net obligation   │                          │
     │     (both sides agree)       │                          │
     │  ◄──────────────────────►    │                          │
     │                              │                          │
     │  2. Mint Cashu ticket:       │                          │
     │     P2PK: B's DID pubkey     │                          │
     │     HTLC: H(settlement_hash) │                          │
     │     Timelock: window + buffer│                          │
     │  ──────────────────────────────────────────────────►    │
     │                              │                          │
     │  3. Receive token            │                          │
     │  ◄──────────────────────────────────────────────────    │
     │                              │                          │
     │  4. Send ticket + signed     │                          │
     │     obligation summary       │                          │
     │  ────────────────────────►   │                          │
     │                              │                          │
     │     5. Verify obligation     │                          │
     │        summary matches       │                          │
     │        local ledger          │                          │
     │                              │                          │
     │     6. Sign acknowledgment   │                          │
     │        (reveals settlement   │                          │
     │         preimage)            │                          │
     │  ◄────────────────────────   │                          │
     │                              │                          │
     │                              │  7. Redeem token:        │
     │                              │     sig(B_key) + preimage│
     │                              │  ──────────────────────► │
     │                              │                          │
     │                              │  8. Sats received        │
     │                              │  ◄────────────────────── │
     │                              │                          │
```

The settlement hash is computed deterministically from the obligation summary:

```
settlement_hash = SHA256(
  sort(obligations) || settlement_window_id || payer_did || receiver_did
)
```

Both parties can independently compute this hash, ensuring they agree on what's being settled.

#### For Leases and Ongoing Obligations

Lease settlements use milestone tickets — one sub-ticket per heartbeat period:

```
Lessee (A)                     Lessor (B)
     │                              │
     │  1. Mint milestone tickets:  │
     │     24 tickets (one per hour)│
     │     Each: P2PK(B) +         │
     │     HTLC(H(heartbeat_i))    │
     │  ────────────────────────►   │
     │                              │
     │  [Each hour:]                │
     │     2. B sends heartbeat     │
     │        attestation           │
     │  ◄────────────────────────   │
     │                              │
     │     3. A verifies capacity   │
     │        is available          │
     │                              │
     │     4. A reveals             │
     │        heartbeat_preimage_i  │
     │  ────────────────────────►   │
     │                              │
     │     5. B redeems ticket_i    │
     │                              │
```

#### For Penalty Settlements

Penalties are deducted directly from the offender's bond (see [Bond System](#bond-system)). No new escrow ticket is needed — the bond itself is a pre-posted Cashu token with spending conditions that include penalty clauses.

### Dispute Resolution

When nodes disagree on obligation amounts:

#### Step 1: Evidence Comparison

Both nodes exchange their signed receipt chains for the disputed period. Receipts signed by both parties are authoritative. Receipts signed by only one party are flagged.

#### Step 2: Peer Arbitration

If evidence comparison doesn't resolve the dispute, the disagreement is broadcast to N randomly selected hive members (the "arbitration panel"). Each panel member:

1. Reviews both parties' evidence
2. Votes on the correct obligation amount
3. Signs their vote

Majority vote determines the settlement amount. Panel members are compensated from a small arbitration fee split between the disputing parties.

#### Step 3: Reputation Consequences

The party whose claimed amount deviates more from the arbitration result receives a `neutral` or `revoke` reputation signal in the `hive:node` profile. Repeated disputes erode trust tier and increase settlement costs.

#### Step 4: Bond Forfeiture

For egregious disputes (evidence of fabricated receipts, dishonest claims), the arbitration panel can recommend bond slashing. This requires supermajority (2/3) panel agreement.

---

## Proof Mechanisms

### Summary of Proof Types

| Settlement Type | Proof Type | Signed By | Verifiable By |
|----------------|-----------|-----------|---------------|
| Routing revenue | `HTLCForwardReceipt` chain | Each hop node | Any node with the receipt chain |
| Rebalancing | `RebalanceReceipt` | Both endpoints | Any node with the receipt |
| Lease | `LeaseHeartbeat` series | Lessor (each heartbeat) | Lessee + arbitration panel |
| Splice | `SpliceReceipt` + on-chain tx | All participants | Anyone (on-chain verification) |
| Shared channel | `SharedChannelReceipt` + funding tx | All contributors | Anyone (on-chain verification) |
| Pheromone | `PheromoneReceipt` + forward receipts | Path nodes | Any node observing the path |
| Intelligence | `IntelligenceReceipt` + routing stats | Buyer + seller | Statistical verification |
| Penalty | `ViolationReport` + quorum sigs | Reporter + quorum | Any hive member |

### Receipt Storage

Receipts are stored locally by each node and optionally published to the Archon network for reputation building. The hash chain of receipts ensures tamper evidence — modifying any receipt invalidates all subsequent hashes.

### Receipt Expiry

Receipts are retained for a configurable period (default: 90 days). After expiry, they can be pruned from local storage. Before pruning, a summary credential is generated and published:

```json
{
  "type": "SettlementSummary",
  "subject": "did:cid:<node_did>",
  "period": { "start": "...", "end": "..." },
  "total_settled_msat": 5000000,
  "settlement_count": 47,
  "disputes": 0,
  "receipt_merkle_root": "sha256:<root_of_all_receipts>",
  "signer": "did:cid:<node_did>",
  "signature": "<sig>"
}
```

The merkle root allows selective disclosure — a node can prove a specific receipt existed without revealing all receipts.

---

## Bond System

### Overview

Nodes post Cashu bonds when joining the hive. Bonds serve as economic commitment — skin in the game that aligns incentives and provides a slashing mechanism for policy violations.

### Bond Structure

A bond is a Cashu token with special spending conditions:

```json
{
  "type": "HiveBond",
  "node_did": "did:cid:<node_did>",
  "amount_sats": 50000,
  "posted_at": "2026-02-14T00:00:00Z",
  "conditions": {
    "P2PK": "<hive_multisig_pubkey>",
    "timelock": "2026-08-14T00:00:00Z",
    "refund": "<node_operator_pubkey>",
    "slash_conditions": [
      "policy_violation_quorum",
      "repeated_dispute_loss",
      "heartbeat_abandonment"
    ]
  }
}
```

The bond is locked to a hive multisig key — a threshold key requiring M-of-N hive founding members to authorize spending. This prevents any single entity from stealing bonds.

**Refund path:** After the bond timelock expires (default: 6 months), the node operator can reclaim their bond — provided no outstanding slash claims exist. Bond renewal is required for continued hive membership.

### Bond Sizing

Bond size scales with the privileges requested:

| Privilege Level | Minimum Bond | Access Granted |
|----------------|-------------|----------------|
| **Observer** | 0 sats | Read-only hive gossip, no settlement participation |
| **Basic routing** | 10,000 sats | Routing revenue sharing, basic intelligence access |
| **Full member** | 50,000 sats | All settlement types, pheromone market, liquidity leasing |
| **Liquidity provider** | 100,000 sats | Channel leasing, splice participation, premium pheromone placement |
| **Founding member** | 250,000 sats | Governance voting, arbitration panel eligibility, highest credit tier |

Bond amounts are denominated in sats and may be adjusted by hive governance based on market conditions.

### Slashing

Bonds are slashed (partially or fully) for proven policy violations:

```
slash_amount = penalty_base × severity × (1 + repeat_count × 0.5)
```

Slashing requires:
1. A `ViolationReport` with quorum confirmation (N/2+1)
2. The arbitration panel (if disputed) confirms the violation
3. The hive multisig signs a slash transaction against the bond

Slashed amounts are distributed:
- 50% to the aggrieved party (if applicable)
- 30% to the arbitration panel (compensation)
- 20% burned (removed from circulation — deflationary)

### Bond + Reputation Interaction

Bonds and reputation are complementary trust signals:

```
trust_level(node) = f(bond_amount, reputation_score, tenure)
```

| Bond | Reputation | Trust Level | Settlement Terms |
|------|-----------|-------------|-----------------|
| High | High | Maximum | Largest credit lines, weekly settlement |
| High | Low | Moderate | Standard terms, daily settlement |
| Low | High | Moderate | Standard terms, daily settlement |
| Low | Low | Minimum | Pre-paid escrow only, per-event settlement |

Bond without reputation means the node has capital at risk but no track record — moderate trust. Reputation without bond means the node has a track record but no current capital commitment — also moderate trust. Both together signal maximum trustworthiness.

Bond status is recorded in the `hive:node` reputation profile:

```json
{
  "domain": "hive:node",
  "metrics": {
    "routing_reliability": 0.95,
    "uptime": 99.1,
    "htlc_success_rate": 0.97,
    "bond_amount_sats": 50000,
    "bond_slashes": 0,
    "bond_tenure_days": 180
  }
}
```

---

## Credit and Trust Tiers

### Tier Definitions

| Tier | Requirements | Credit Line | Settlement Window | Escrow Model |
|------|-------------|------------|-------------------|-------------|
| **Newcomer** | Bond posted, no history | 0 sats | Per-event | Pre-paid escrow for all obligations |
| **Established** | 30+ days, 0 disputes, reputation > 60 | 10,000 msat | Hourly batch | Escrow for obligations > credit line |
| **Trusted** | 90+ days, ≤1 dispute, reputation > 75 | 100,000 msat | Daily batch | Bilateral netting, escrow for net amount only |
| **Senior** | 180+ days, 0 disputes in 90d, reputation > 85 | 500,000 msat | Weekly batch | Multilateral netting, minimal escrow |
| **Founding** | Genesis member or governance-approved | 2,000,000 msat | Weekly batch | Bilateral credit, periodic true-up |

### Credit Line Mechanics

A credit line means the node can accumulate obligations up to the credit limit before escrow is required:

```
If accumulated_obligations(A→B) < credit_line(A, tier):
  No escrow needed — obligation recorded in ledger, settled at window end
Else:
  Excess must be escrowed immediately via Cashu ticket
```

Credit lines are bilateral — Node A's credit with Node B depends on A's tier as perceived by B. Different nodes may assign different tiers to the same peer based on their direct experience.

### Tier Progression

```
Newcomer → Established → Trusted → Senior
   │           │            │          │
   │  30 days  │  90 days   │ 180 days │
   │  no       │  ≤1        │  0 recent│
   │  disputes │  dispute   │  disputes│
   │           │            │          │
   └───────────┴────────────┴──────────┘
              Automatic Progression
              (can be accelerated by
               higher bond + reputation)
```

Tier demotion is immediate upon bond slash or dispute loss. Demotion drops the node one full tier and resets the progression timer.

### Mapping to DID Reputation Schema

Trust tiers are derived from the `hive:node` profile in the [DID Reputation Schema](./DID-REPUTATION-SCHEMA.md):

```
tier = compute_tier(
  reputation_score(hive:node),  // from aggregated DIDReputationCredentials
  bond_amount,                   // current bond posting
  tenure_days,                   // days since hive join
  dispute_history                // from settlement records
)
```

The reputation score aggregation follows the schema's [weighted aggregation algorithm](./DID-REPUTATION-SCHEMA.md#aggregation-algorithm), with issuer diversity, recency decay, and evidence strength all factored in.

---

## Multi-Operator Fleet Dynamics

### Competing Operators in the Same Hive

The settlement protocol enables a novel topology: operators who are economic competitors (they all want routing revenue) cooperating in the same hive because cooperation produces more total revenue than competition.

#### Why Cooperate?

A lone node with 50 channels competes against the entire Lightning network. A hive of 50 nodes with 500 channels coordinates routing, shares intelligence, and presents unified liquidity — capturing far more routing volume.

```
Individual routing revenue (competitive):   R_solo
Hive routing revenue (cooperative):         R_hive
Hive member share:                          R_hive / N

For cooperation to be rational:
  R_hive / N > R_solo
  R_hive > N × R_solo

This holds when:
  - Coordinated routing captures traffic that no individual node could
  - Shared intelligence improves everyone's routing success rate
  - Unified liquidity management reduces rebalancing costs
  - Network effects: each new member adds value for all existing members
```

### Incentive Alignment

The settlement protocol aligns incentives through:

1. **Revenue sharing proportional to contribution** — Nodes earn based on liquidity committed, not just presence. Free-riding is unprofitable.

2. **Bonds make defection expensive** — A node that defects (fee undercutting, data leakage) loses their bond. The bond must exceed the expected gain from defection.

3. **Reputation is persistent** — Bad behavior follows the DID across hives. A node that defects from one hive carries that `revoke` credential forever.

4. **Credit lines reward loyalty** — Long-tenured cooperators get better settlement terms, reducing their operational costs. Defection resets this to zero.

### Game Theory Analysis

#### The Settlement Game

Model the hive as a repeated game between N operators. Each round, each operator chooses:
- **Cooperate (C):** Honest reporting, fair settlement, policy compliance
- **Defect (D):** Fabricate receipts, undercut fees, free-ride on intelligence

**Payoff matrix (simplified, 2 players):**

```
                    Player B
                 C           D
Player A   C  (3, 3)      (0, 5)
           D  (5, 0)      (1, 1)
```

One-shot: Defect dominates. Repeated (infinite horizon): Tit-for-tat with bond forfeiture makes cooperation the Nash equilibrium.

**Key parameters for cooperation equilibrium:**
```
Bond > max_gain_from_single_defection
Reputation_cost > present_value(future_cooperation_benefits × defection_discount)
Detection_probability > 1 - (bond / defection_gain)
```

With the proof mechanisms defined above (signed receipts, quorum detection, on-chain verification), detection probability is high for most violation types. Combined with bonds that exceed single-defection gains, the equilibrium strongly favors cooperation.

#### Free-Rider Prevention

Free-riders consume hive benefits (intelligence, coordinated routing) without contributing:

| Free-Rider Strategy | Detection | Prevention |
|---------------------|-----------|-----------|
| Consume intelligence, contribute none | Contribution tracking per node | Minimum contribution requirement; intelligence access gated by contribution score |
| Route through hive paths, don't share revenue | Signed forwarding receipts missing from expected paths | Hive routing prefers nodes with complete receipt histories |
| Join hive for reputation, don't participate | Activity metrics in `hive:node` profile | Tier demotion for inactivity; bond reclamation delayed |

#### Cartel/Collusion Resistance

A subset of hive members could collude to dominate governance, manipulate settlements, or extract rents:

| Collusion Strategy | Resistance Mechanism |
|-------------------|---------------------|
| Fabricate reputation for each other | Sybil resistance in aggregation (issuer diversity, stake weighting) |
| Stack arbitration panels | Random panel selection weighted by stake + reputation |
| Coordinate fee policy against non-colluders | Fee policy transparency via gossip; non-colluders can exit |
| Accumulate governance votes | Quadratic or conviction voting; one-DID-one-vote with sybil penalties |

The fundamental protection: **exit is free.** Any node can leave the hive at any time, reclaim their bond (minus pending obligations), and join or form a different hive. This limits the extractive power of any cartel.

---

## Integration with Existing Hive Protocol

### Pheromone System Integration

Pheromone markers — the hive's stigmergic signaling mechanism — are extended to carry settlement metadata:

```json
{
  "type": "pheromone_marker",
  "marker_type": "route_preference",
  "path": ["03abc...", "03def...", "03ghi..."],
  "strength": 0.85,
  "decay_rate": 0.02,
  "settlement_metadata": {
    "revenue_share_model": "proportional",
    "settlement_window": "daily",
    "credit_tiers": {
      "03abc...": "trusted",
      "03def...": "established",
      "03ghi...": "newcomer"
    },
    "net_obligations_msat": {
      "03abc→03def": 1500,
      "03def→03ghi": -800
    }
  }
}
```

Settlement metadata in pheromone markers enables:
- **Informed routing decisions** — Prefer paths where settlement terms are favorable
- **Credit-aware path selection** — Avoid paths where credit limits are near exhaustion
- **Obligation-aware load balancing** — Distribute routing to equalize bilateral obligations (natural netting)

### Stigmergic Settlement Markers

New marker types for settlement-specific signals:

| Marker Type | Purpose | Decay |
|-------------|---------|-------|
| `settlement_pending` | Flags a path with unsettled obligations | Fast (clears after settlement) |
| `credit_available` | Advertises available credit on a path | Moderate |
| `bond_healthy` | Signals that path nodes have healthy bonds | Slow |
| `dispute_active` | Warns of an ongoing settlement dispute on a path | Persists until resolved |

### Gossip Protocol Extensions

The hive gossip protocol is extended with settlement-related message types:

| Message Type | Content | Propagation |
|-------------|---------|-------------|
| `settlement_summary` | Net obligation summary for a bilateral pair | Direct (bilateral only) |
| `netting_proposal` | Multilateral netting proposal | Broadcast to all participants |
| `netting_ack` | Agreement to multilateral netting result | Broadcast to all participants |
| `bond_posting` | Announcement of new bond or renewal | Broadcast (full hive) |
| `violation_report` | Policy violation with evidence | Broadcast (full hive) |
| `arbitration_vote` | Panel member's vote on a dispute | Direct to disputing parties + panel |

### PKI Handshake Extension

The existing hive PKI handshake is extended to include settlement parameters:

```
Existing handshake:
  1. Node key exchange
  2. DID credential presentation
  3. Hive membership verification

Extended handshake (new steps):
  4. Bond status attestation (current bond amount, last slash, tenure)
  5. Settlement preference negotiation:
     - Preferred settlement window
     - Acceptable mints for Cashu tickets
     - Credit tier assertion + supporting reputation credentials
  6. Initial credit line establishment
```

### Migration Path

#### Phase 0: Current State (Internal Accounting)
All settlements are ledger entries in the hive coordinator. Works for single-operator.

#### Phase 1: Structured Receipts
Introduce signed receipts for all settlement types. Continue with internal accounting but build the receipt chain. No Cashu escrow yet — this phase is about establishing the proof substrate.

**Compatibility:** Fully backward compatible. Single-operator hives see no change.

#### Phase 2: Optional Escrow
Multi-operator relationships can opt into Cashu escrow for settlement. Single-operator internal settlements remain unchanged. Both modes coexist.

**Compatibility:** Opt-in per bilateral relationship.

#### Phase 3: Default Escrow
Cashu escrow becomes the default for all multi-operator settlements. Single-operator internal settlements can still use internal accounting but receipts are required.

**Compatibility:** Multi-operator hives require escrow. Single-operator unchanged.

#### Phase 4: Full Trustless
All settlements use the full protocol — bonds, credit tiers, netting, escrow. Hive membership is permissionless (bond + minimum reputation). Internal accounting deprecated.

---

## Privacy

### Settlement Amounts

Cashu blind signatures ensure that settlement amounts are hidden from non-participants:

- **The mint** sees token amounts at minting and redemption but cannot correlate them (blind signatures break linkability)
- **Other hive members** see that settlements occurred (via gossip) but not the amounts
- **The gossip protocol** carries obligation *existence* but not *magnitude* — pheromone markers show "settlement pending" but not "5000 msat owed"

### Routing Data

Routing intelligence shared between nodes is privacy-sensitive — it reveals traffic patterns, fee strategies, and liquidity positions. The protocol handles this through:

| Data Type | Sharing Model | Privacy Level |
|-----------|--------------|---------------|
| Forwarding receipts | Bilateral only (payer ↔ receiver) | High — only parties to the HTLC see details |
| Aggregate routing stats | Hive-wide gossip | Medium — anonymized, no per-HTLC details |
| Fee maps | Paid intelligence (need-to-buy) | High — encrypted to buyer's DID key |
| Liquidity estimates | Hive-wide gossip | Medium — directional, not exact amounts |
| Settlement summaries | Bilateral (detailed) / Hive (aggregate) | High bilateral, medium hive |

### Reputation: Public Signal, Private Details

The DID Reputation Schema produces public reputation credentials — anyone can see a node's `hive:node` score. But the underlying settlement details (specific amounts, specific counterparties, specific disputes) remain private:

```
Public:
  - Node X has routing_reliability: 0.95
  - Node X has been a hive member for 180 days
  - Node X has 0 bond slashes
  
Private:
  - Node X settled 5,000,000 msat with Node Y last week
  - Node X disputed a 50,000 msat obligation with Node Z
  - Node X leases 10M sats of capacity from Node W
```

### What the Mint Learns

| Mint Observes | Mint Does NOT Learn |
|--------------|-------------------|
| Token denominations minted | Which node minted them or why |
| Token denominations redeemed | Which node redeemed or what settlement they're for |
| Minting/redemption timing | The bilateral relationship or obligation type |
| Total volume through the mint | The netting computation or gross obligations |

The mint is a fungible ecash issuer — it processes blind signatures and has no semantic understanding of the settlement protocol. Using multiple mints further reduces any single mint's visibility.

---

## Implementation Roadmap

### Phase 1: Receipt Infrastructure (3–4 weeks)
- Define receipt schemas for all 8 settlement types
- Implement receipt signing and verification in cl-hive
- Build hash-chain receipt ledger with merkle root computation
- Add receipt exchange to the gossip protocol

### Phase 2: Bilateral Netting (2–3 weeks)
- Implement bilateral obligation tracking per peer
- Build netting computation engine
- Add settlement window configuration (per-node, per-peer)
- Settlement summary gossip messages

### Phase 3: Bond System (3–4 weeks)
- Cashu bond minting with multisig spending conditions
- Bond posting during hive PKI handshake
- Violation detection framework (quorum-based)
- Slashing mechanism with bond forfeiture

### Phase 4: Cashu Escrow Integration (3–4 weeks)
- Connect netting output to [DID + Cashu Task Escrow](./DID-CASHU-TASK-ESCROW.md) ticket creation
- Implement settlement-specific HTLC secret generation and reveal
- Milestone tickets for lease settlements
- Refund path for disputed/expired settlements

### Phase 5: Credit Tiers (2–3 weeks)
- Trust tier computation from reputation + bond + tenure
- Credit line management and enforcement
- Automatic tier progression/demotion
- Integration with [DID Reputation Schema](./DID-REPUTATION-SCHEMA.md) `hive:node` profile

### Phase 6: Multilateral Netting (3–4 weeks)
- Multilateral netting algorithm implementation
- Gossip-based obligation set agreement
- Netting proposal/acknowledgment protocol
- Fallback to bilateral if multilateral consensus fails

### Phase 7: Dispute Resolution (2–3 weeks)
- Arbitration panel selection algorithm
- Evidence comparison and voting protocol
- Reputation consequences for dispute outcomes
- Bond forfeiture workflow for egregious violations

### Phase 8: Pheromone Market + Intelligence Market (4–6 weeks)
- Pheromone placement escrow (pay-for-performance)
- Intelligence data packaging and verification
- Correlation-based proof for intelligence value
- Market price discovery via hive gossip

---

## Open Questions

1. **Mint selection:** Should the hive operate its own Cashu mint, or rely on external mints? A hive mint centralizes trust but simplifies operations. External mints distribute trust but add coordination overhead.

2. **Netting frequency vs. privacy:** More frequent netting reduces credit exposure but generates more Cashu token operations, potentially leaking timing information to the mint. What's the optimal tradeoff?

3. **Cross-hive settlements:** If a node belongs to multiple hives, how do settlements interact? Can obligations in one hive be netted against obligations in another?

4. **Bond denomination:** Should bonds be denominated in sats (fixed) or in a percentage of the node's channel capacity (dynamic)? Fixed is simpler; dynamic adapts to node size.

5. **Penalty calibration:** How do we set penalty amounts that are punitive enough to deter but not so harsh they discourage participation? Should penalties be governance-adjustable?

6. **Multilateral netting trust:** The multilateral netting algorithm requires all parties to agree on the obligation set. What if one party strategically disagrees to force bilateral (more expensive) settlement with a specific counterparty?

7. **Lease market dynamics:** How do we prevent a race to the bottom on lease rates? Should there be a hive-minimum lease rate, or is pure market pricing sufficient?

8. **Intelligence verification:** The correlation-based proof for intelligence value is inherently noisy. What statistical significance threshold is appropriate? How do we handle cases where intelligence is valuable but the buyer's routing improves for unrelated reasons?

9. **Arbitration incentives:** How do we ensure arbitration panel members are honest? Their compensation comes from the arbitration fee, but they could collude with one party. Should there be a "meta-arbitration" mechanism?

10. **Emergency settlement:** What happens if a node needs to leave the hive urgently (e.g., detected compromise)? How are outstanding obligations settled when one party is rushing for the exit?

---

## References

- [DID + L402 Remote Fleet Management](./DID-L402-FLEET-MANAGEMENT.md)
- [DID + Cashu Task Escrow Protocol](./DID-CASHU-TASK-ESCROW.md)
- [DID Reputation Schema](./DID-REPUTATION-SCHEMA.md)
- [Cashu NUT-10: Spending Conditions](https://github.com/cashubtc/nuts/blob/main/10.md)
- [Cashu NUT-11: Pay-to-Public-Key (P2PK)](https://github.com/cashubtc/nuts/blob/main/11.md)
- [Cashu NUT-14: Hashed Timelock Contracts](https://github.com/cashubtc/nuts/blob/main/14.md)
- [Cashu Protocol](https://cashu.space/)
- [BOLT 2: Peer Protocol for Channel Management](https://github.com/lightning/bolts/blob/master/02-peer-protocol.md)
- [BOLT 7: P2P Node and Channel Discovery](https://github.com/lightning/bolts/blob/master/07-routing-gossip.md)
- [W3C DID Core 1.0](https://www.w3.org/TR/did-core/)
- [W3C Verifiable Credentials Data Model 2.0](https://www.w3.org/TR/vc-data-model-2.0/)
- [Archon: Decentralized Identity for AI Agents](https://github.com/archetech/archon)
- [Lightning Hive: Swarm Intelligence for Lightning](https://github.com/lightning-goats/cl-hive)
- [Nisan & Rougearden, "Algorithmic Game Theory", Cambridge University Press (2007)](https://www.cs.cmu.edu/~sandholm/cs15-892F13/algorithmic-game-theory.pdf) — Chapters on mechanism design and repeated games
- [Shapley, L.S. "A Value for n-Person Games" (1953)](https://doi.org/10.1515/9781400881970-018) — Foundation for contribution-proportional revenue sharing

---

*Feedback welcome. File issues on [cl-hive](https://github.com/lightning-goats/cl-hive) or discuss in #singularity.*

*— Hex ⬡*
