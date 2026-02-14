# DID + L402 Remote Fleet Management

**Status:** Proposal / Design Draft  
**Author:** Hex (`did:cid:bagaaierajrr7k6izcrdfwqxpgtrobflsv5oibymfnthjazkkokaugszyh4ka`)  
**Date:** 2026-02-14  
**Feedback:** Open — file issues or comment in #singularity

---

## Abstract

This document proposes a protocol for authenticated, paid remote fleet management in the Lightning Hive. It combines three existing technologies:

- **Archon DIDs** for agent identity and authorization
- **L402 / Cashu** for micropayment-gated access
- **Bolt 8** (Lightning P2P transport) for encrypted command delivery

The result is a system where agents can manage Lightning nodes they don't own — authenticated by verifiable credentials, paid per action or subscription, communicating over the existing Lightning peer network. No new infrastructure required.

---

## Motivation

### Current State

The Lightning Hive coordinates a fleet of nodes through gossip protocols, pheromone markers, and a centralized AI advisor. The advisor runs on the fleet operator's infrastructure and has direct access to node RPCs.

This works for a single operator managing their own fleet. It doesn't scale to:

1. **Third-party management** — A skilled routing advisor managing nodes for multiple operators
2. **Decentralized fleets** — Hive members granting management authority to each other
3. **Paid services** — Advisors being compensated for their expertise
4. **Trustless delegation** — Granting limited access without sharing node credentials

### The Opportunity

Lightning node routing optimization is complex. Most node operators either:
- Run default settings (leaving revenue on the table)
- Spend significant time manually tuning (not scalable)
- Trust third-party services with full node access (security risk)

A protocol for authenticated, paid, scoped remote management would create a **marketplace for routing expertise** — where the best advisors serve the most nodes, and their track records are cryptographically verifiable.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   AGENT (Advisor)                     │
│                                                       │
│  ┌──────────┐  ┌──────────┐  ┌───────────────────┐  │
│  │ Archon   │  │ Lightning│  │ Management Engine  │  │
│  │ Keymaster│  │ Wallet   │  │ (fee optimization, │  │
│  │ (DID)    │  │ (L402/   │  │  rebalancing, etc) │  │
│  │          │  │  Cashu)  │  │                    │  │
│  └────┬─────┘  └────┬─────┘  └────────┬──────────┘  │
│       │              │                  │             │
│       └──────────────┼──────────────────┘             │
│                      │                                │
│              ┌───────▼────────┐                       │
│              │ Schema Builder │                       │
│              │ (sign + attach │                       │
│              │  credential +  │                       │
│              │  payment)      │                       │
│              └───────┬────────┘                       │
└──────────────────────┼────────────────────────────────┘
                       │
                 Bolt 8 Transport
              (Custom TLV Messages)
                       │
┌──────────────────────┼────────────────────────────────┐
│              ┌───────▼────────┐                       │
│              │ Schema Handler │                       │
│              │ (validate cred │                       │
│              │  + payment +   │                       │
│              │  policy check) │                       │
│              └───────┬────────┘                       │
│                      │                                │
│       ┌──────────────┼──────────────────┐             │
│       │              │                  │             │
│  ┌────▼─────┐  ┌─────▼────┐  ┌─────────▼──────────┐ │
│  │ Archon   │  │ Payment  │  │ CLN Plugin          │ │
│  │Gatekeeper│  │ Verifier │  │ (cl-hive /          │ │
│  │ (DID     │  │ (L402 /  │  │  cl-revenue-ops)    │ │
│  │  verify) │  │  Cashu)  │  │                     │ │
│  └──────────┘  └──────────┘  └─────────────────────┘ │
│                                                       │
│                   NODE (Managed)                       │
└───────────────────────────────────────────────────────┘
```

---

## Protocol Components

### 1. Identity Layer (Archon DIDs)

#### Management Credentials

A node operator issues a **Management Credential** to an agent's DID. This is a W3C Verifiable Credential specifying:

```json
{
  "@context": ["https://www.w3.org/2018/credentials/v1", "https://hive.lightning/management/v1"],
  "type": ["VerifiableCredential", "HiveManagementCredential"],
  "issuer": "did:cid:<node_operator_did>",
  "credentialSubject": {
    "id": "did:cid:<agent_did>",
    "nodeId": "03abcdef...",
    "permissions": {
      "monitor": true,
      "fee_policy": true,
      "rebalance": true,
      "config_tune": true,
      "channel_open": false,
      "channel_close": false,
      "splice": false
    },
    "constraints": {
      "max_fee_change_pct": 50,
      "max_rebalance_sats": 1000000,
      "max_daily_actions": 100,
      "allowed_schemas": ["hive:fee-policy/*", "hive:rebalance/*", "hive:config/*", "hive:monitor/*"]
    },
    "tier": "standard",
    "compensation": {
      "model": "per_action",
      "rate_sats": 10,
      "currency": "L402|cashu"
    }
  },
  "issuanceDate": "2026-02-14T00:00:00Z",
  "expirationDate": "2026-03-14T00:00:00Z"
}
```

#### Permission Tiers

| Tier | Permissions | Trust Level | Danger Score Range | Typical Use |
|------|-----------|-------------|-------------------|-------------|
| `monitor` | Read-only metrics, health checks | Minimal | 1–2 | Monitoring services, dashboards |
| `standard` | Fee policy, rebalancing, config tuning | Moderate | 3–5 | Routine optimization |
| `advanced` | All standard + channel opens, splicing, expansion proposals | High | 6–7 | Full fleet management |
| `admin` | All permissions including channel closes, emergency ops | Maximum | 8–10 | Trusted long-term partner |

Tiers are enforced both by the credential scope AND by the node's local policy engine. Even if a credential grants `channel_close`, the node can reject it based on local policy.

#### Credential Lifecycle

1. **Issuance:** Operator creates credential via Archon Keymaster, specifying scope and duration
2. **Presentation:** Agent includes credential with each management command
3. **Verification:** Node verifies credential against Archon network (DID resolution + signature check)
4. **Revocation:** Operator can revoke at any time via Archon. Node checks revocation status before executing commands
5. **Renewal:** Credentials have expiration dates. Auto-renewal possible if both parties agree

### 2. Payment Layer (L402 / Cashu)

#### Payment Models

| Model | Flow | Best For |
|-------|------|----------|
| **Per-action** | Each management command includes a Cashu token or L402 proof | Low-volume, pay-as-you-go |
| **Subscription** | Agent pre-pays for a time window; receives an L402 macaroon valid for N actions | High-volume, predictable |
| **Performance** | Base fee + bonus tied to outcome metrics (routing revenue delta) | Aligned incentives |

#### Per-Action Flow (Cashu)

> **Note:** The simple per-action flow below is suitable for low-risk, unconditional payments. For conditional escrow — where payment is released only on provable task completion — see the full [DID + Cashu Task Escrow Protocol](./DID-CASHU-TASK-ESCROW.md). That spec defines escrow tickets with P2PK + HTLC + timelock conditions for atomic task-completion-equals-payment-release.

```
Agent                                    Node
  │                                        │
  │  1. Management Schema                  │
  │     + DID Credential                   │
  │     + Cashu Token (10 sats)            │
  │  ─────────────────────────────────►    │
  │                                        │
  │     2. Verify DID credential           │
  │     3. Redeem Cashu token with mint    │
  │     4. Validate schema against policy  │
  │     5. Execute action                  │
  │                                        │
  │  6. Signed Receipt                     │
  │     + Action result                    │
  │     + New node state hash              │
  │  ◄─────────────────────────────────    │
  │                                        │
```

#### Subscription Flow (L402)

```
Agent                                    Node
  │                                        │
  │  1. Request subscription               │
  │     + DID Credential                   │
  │  ─────────────────────────────────►    │
  │                                        │
  │  2. HTTP 402 + Lightning Invoice       │
  │     (1000 sats / 30 days)             │
  │  ◄─────────────────────────────────    │
  │                                        │
  │  3. Pay invoice                        │
  │  ─────────────────────────────────►    │
  │                                        │
  │  4. L402 Macaroon                      │
  │     Caveats:                           │
  │     - did = did:cid:<agent>            │
  │     - tier = standard                  │
  │     - expires = 2026-03-14             │
  │     - max_actions = 1000               │
  │  ◄─────────────────────────────────    │
  │                                        │
  │  [Subsequent commands include macaroon │
  │   instead of per-action payment]       │
  │                                        │
```

#### Escrow Model (Conditional Payment)

For tasks where payment should be contingent on provable completion, the protocol uses **Cashu escrow tickets** — tokens with composite spending conditions (P2PK + HTLC + timelock). The operator mints a token locked to the agent's DID-derived pubkey and a hash whose preimage the node reveals only on successful task execution. This makes payment release atomic with task completion.

The full escrow protocol — including ticket types (single-task, batch, milestone, performance), danger-score-based pricing, failure modes, and mint trust considerations — is specified in the [DID + Cashu Task Escrow Protocol](./DID-CASHU-TASK-ESCROW.md).

#### Performance-Based Payment

For performance-based pricing, the node tracks a baseline metric (e.g., 7-day average routing revenue) at the start of the management period. At settlement:

```
bonus = max(0, (current_revenue - baseline_revenue)) × performance_share
```

Settlement happens via the hive's existing distributed settlement protocol, with the advisor's DID as a payment recipient. The settlement is triggered automatically when the management credential expires or renews.

#### Why Cashu for Per-Action

- **No routing overhead** — Cashu tokens are bearer instruments, no Lightning payment per command
- **Atomic** — Token + command are a single message. Either both succeed or neither does
- **Budgetable** — Operator mints a batch of tokens as the agent's spending allowance
- **Private** — Blind signatures mean the mint can't correlate tokens to commands
- **Offline-capable** — Agent can hold tokens and spend them without real-time Lightning connectivity

### 3. Transport Layer (Bolt 8 + Custom Messages)

#### Why Bolt 8

| Property | Benefit |
|----------|---------|
| Already deployed | Every Lightning node has it on port 9735 |
| Encrypted | Noise_XK with forward secrecy — management commands are invisible to observers |
| Authenticated | Both sides prove node key ownership during handshake |
| NAT-friendly | Uses existing Lightning peer connection, no extra ports |
| Extensible | Custom message types (odd TLV, type ≥ 32768) supported by CLN and LND |

#### Message Format

Management messages use a custom Lightning message type in the odd (experimental) range:

```
Type: 49152 (0xC000) — Hive Management Message

TLV Payload:
  [1] schema_type    : utf8     (e.g., "hive:fee-policy/v1")
  [2] schema_payload : json     (the actual command)
  [3] credential     : bytes    (serialized Archon VC)
  [4] payment_proof  : bytes    (L402 macaroon OR Cashu token)
  [5] signature      : bytes    (agent's DID signature over [1]+[2])
  [6] nonce          : u64      (replay protection)
  [7] timestamp      : u64      (unix epoch seconds)

Response Type: 49153 (0xC001) — Hive Management Response

TLV Payload:
  [1] request_nonce  : u64      (echo of request nonce)
  [2] status         : u8       (0=success, 1=rejected, 2=error)
  [3] result         : json     (action result or error details)
  [4] state_hash     : bytes32  (hash of node state after action)
  [5] signature      : bytes    (node's signature over response)
  [6] receipt        : bytes    (signed receipt for audit trail)
```

#### Replay Protection

- Each command includes a monotonically increasing nonce
- Node tracks the last nonce per agent DID
- Commands with nonce ≤ last seen are rejected
- Timestamp must be within ±5 minutes of node's clock

#### Message Size

Bolt 8 messages have a 65535-byte limit. A typical management command (schema + credential + payment) is ~2-4 KB, well within limits. For batch operations, the agent sends multiple messages sequentially.

### 4. Schema Layer

#### Schema Registry

Schemas are versioned, structured command definitions. They define:
- What parameters are required/optional
- Valid ranges for each parameter
- Required permission tier
- Expected response format

Schemas are published as Archon verifiable credentials, enabling:
- Version discovery (agents can check what schemas a node supports)
- Governance (new schemas proposed and voted on by hive members)
- Compatibility checking (agent verifies node supports schema version before sending)

#### Core Schemas

##### `hive:fee-policy/v1`

Set fee anchors and policy for channels.

```json
{
  "schema": "hive:fee-policy/v1",
  "action": "set_anchor",
  "params": {
    "channel_id": "931770x2363x0",
    "target_fee_ppm": 150,
    "confidence": 0.7,
    "ttl_hours": 24,
    "reason": "Stagnant channel, reducing fee to attract outflow"
  }
}
```

**Required tier:** `standard`  
**Danger score:** 3 (see [Task Taxonomy & Danger Scoring](#task-taxonomy--danger-scoring))  
**Constraints:** `target_fee_ppm` must be within credential's `max_fee_change_pct` of current fee

##### `hive:rebalance/v1`

Trigger a rebalance operation.

```json
{
  "schema": "hive:rebalance/v1",
  "action": "circular_rebalance",
  "params": {
    "from_channel": "931770x2363x0",
    "to_channel": "932263x1883x0",
    "amount_sats": 500000,
    "max_fee_ppm": 500,
    "prefer_hive_route": true
  }
}
```

**Required tier:** `standard`  
**Danger score:** 4–5 (depends on amount; see [Task Taxonomy](#task-taxonomy--danger-scoring))  
**Constraints:** `amount_sats` ≤ credential's `max_rebalance_sats`; `max_fee_ppm` ≤ 1000

##### `hive:config/v1`

Adjust cl-revenue-ops algorithm parameters.

```json
{
  "schema": "hive:config/v1",
  "action": "adjust",
  "params": {
    "parameter": "min_fee_ppm",
    "value": 20,
    "trigger_reason": "stagnation",
    "confidence": 0.6,
    "context_metrics": {
      "revenue_24h": 23,
      "stagnant_count": 7,
      "forward_count_24h": 5
    }
  }
}
```

**Required tier:** `standard`  
**Danger score:** 3–4 (algorithm tuning is reversible but affects routing behavior)  
**Constraints:** Parameter must be in allowed list; value within valid range; respects isolation windows

##### `hive:monitor/v1`

Read-only queries for node health and metrics.

```json
{
  "schema": "hive:monitor/v1",
  "action": "health_summary",
  "params": {
    "include_channels": true,
    "include_forwards": true,
    "hours": 24
  }
}
```

**Required tier:** `monitor`  
**Danger score:** 1 (read-only, zero risk)  
**Constraints:** Read-only, no state changes

##### `hive:expansion/v1`

Propose channel opens or topology changes.

```json
{
  "schema": "hive:expansion/v1",
  "action": "propose_channel_open",
  "params": {
    "peer_id": "02abc...",
    "capacity_sats": 5000000,
    "push_sats": 0,
    "reasoning": "High-volume peer with complementary connectivity",
    "peer_intel": { ... }
  }
}
```

**Required tier:** `advanced`  
**Danger score:** 6 (commits on-chain funds; see [Task Taxonomy](#task-taxonomy--danger-scoring))  
**Constraints:** Creates a pending action for operator approval; does NOT auto-execute

#### Schema Versioning

Schemas use semantic versioning. The node advertises supported schemas during the initial capability exchange:

```json
{
  "supported_schemas": [
    "hive:fee-policy/v1",
    "hive:fee-policy/v2",
    "hive:rebalance/v1",
    "hive:config/v1",
    "hive:monitor/v1"
  ]
}
```

Agents MUST check compatibility before sending commands. Version negotiation follows the same pattern as Lightning feature bits.

---

## Task Taxonomy & Danger Scoring

Every action an agent can take on a managed Lightning node is catalogued here with a **danger score** from 1 (harmless) to 10 (catastrophic if misused). This taxonomy is foundational — it drives permission tiers, pricing, approval workflows, and the trust model that follows.

### Scoring Dimensions

Each task is evaluated across five dimensions. The danger score is the **maximum** across dimensions (not the average), because a single catastrophic dimension dominates:

| Dimension | 1–2 (Low) | 3–5 (Medium) | 6–8 (High) | 9–10 (Critical) |
|-----------|-----------|--------------|------------|-----------------|
| **Reversibility** | Instantly undoable | Undoable within hours | Requires on-chain action to undo | Irreversible (funds lost) |
| **Financial Exposure** | 0 sats at risk | < 100k sats | 100k–10M sats | > 10M sats or entire wallet |
| **Time Sensitivity** | No compounding | Compounds over days | Compounds over hours | Immediate/permanent damage |
| **Blast Radius** | Single metric | Single channel | Multiple channels | Entire node or fleet |
| **Recovery Difficulty** | Trivial | Moderate effort | Requires expertise + time | May be unrecoverable |

### Category 1: Monitoring & Read-Only Operations

All read-only operations. No state changes, no risk.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Health summary | Node uptime, version, block height | **1** | monitor | `hive:monitor/v1` | Pure read |
| Channel list | List all channels with balances | **1** | monitor | `hive:monitor/v1` | Pure read |
| Forward history | Query routing history and earnings | **1** | monitor | `hive:monitor/v1` | Pure read |
| Peer list | Connected peers and connection status | **1** | monitor | `hive:monitor/v1` | Pure read |
| Invoice list | Past invoices (paid/unpaid) | **1** | monitor | `hive:monitor/v1` | Pure read |
| Payment list | Outgoing payment history | **1** | monitor | `hive:monitor/v1` | Pure read |
| HTLC snapshot | In-flight HTLCs across channels | **1** | monitor | `hive:monitor/v1` | Pure read |
| Fee report | Current fee settings per channel | **1** | monitor | `hive:monitor/v1` | Pure read |
| On-chain balance | Wallet balance, UTXOs | **1** | monitor | `hive:monitor/v1` | Pure read |
| Network graph query | Local gossip graph data | **1** | monitor | `hive:monitor/v1` | Pure read |
| Log streaming | Subscribe to filtered log output | **2** | monitor | `hive:monitor/v1` | Read-only but may leak operational details; slightly elevated |
| Plugin status | List running plugins and their state | **1** | monitor | `hive:monitor/v1` | Pure read |
| Backup status | Last backup time, integrity check result | **1** | monitor | `hive:monitor/v1` | Pure read |

### Category 2: Fee Management

Adjusting how the node prices its liquidity. Reversible but affects revenue and routing behavior.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Set base fee (single channel) | Adjust base_fee_msat on one channel | **2** | standard | `hive:fee-policy/v1` | Instantly reversible; affects one channel |
| Set fee rate (single channel) | Adjust fee_rate_ppm on one channel | **3** | standard | `hive:fee-policy/v1` | Reversible but bad rates compound — mispricing bleeds sats via unfavorable forwards |
| Set base fee (bulk) | Adjust base_fee_msat across multiple/all channels | **4** | standard | `hive:fee-policy/v1` | Same as single but blast radius is the whole node |
| Set fee rate (bulk) | Adjust fee_rate_ppm across multiple/all channels | **5** | standard | `hive:fee-policy/v1` | Node-wide mispricing can drain liquidity in hours |
| Set fee to zero | Set 0/0 fees on a channel | **4** | standard | `hive:fee-policy/v1` | Attracts heavy traffic, drains outbound liquidity rapidly; reversible but damage accrues fast |
| Fee schedule / automation rules | Configure time-based or threshold-based fee rules | **4** | standard | `hive:config/v1` | Autonomous fee changes amplify mistakes over time |

### Category 3: HTLC Policy

Controls what payments the node will forward. Misconfiguration can silently kill routing or expose the node to griefing.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Set min HTLC (single channel) | Minimum HTLC amount to forward | **2** | standard | `hive:fee-policy/v1` | Low risk; too high just reduces volume |
| Set max HTLC (single channel) | Maximum HTLC amount to forward | **3** | standard | `hive:fee-policy/v1` | Too low kills large payments; too high increases griefing surface |
| Set CLTV delta | Timelock delta for forwarded HTLCs | **4** | standard | `hive:fee-policy/v1` | Too low → force close risk if chain congested; too high → payments avoid you |
| Set HTLC limits (bulk) | Min/max HTLC across all channels | **5** | standard | `hive:fee-policy/v1` | Node-wide blast radius; bad CLTV delta on all channels is dangerous |

### Category 4: Forwarding Policy

Enable/disable forwarding on channels. Directly controls whether the node routes payments.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Disable channel (single) | Set channel to private/disabled | **3** | standard | `hive:fee-policy/v1` | Reversible; reduces routing but no fund risk |
| Enable channel (single) | Re-enable a disabled channel | **2** | standard | `hive:fee-policy/v1` | Restoring normal state; low risk |
| Disable all forwarding | Disable forwarding on every channel | **6** | advanced | `hive:config/v1` | Node goes dark for routing; revenue stops instantly; recovery requires re-enabling each channel |
| Enable all forwarding | Re-enable forwarding on every channel | **3** | standard | `hive:config/v1` | Restoring normal state but could re-expose channels that were intentionally disabled |

### Category 5: Liquidity Management (Rebalancing)

Moving sats between channels. Costs fees and can fail, but funds stay within the node's own channels.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Circular rebalance (small) | Self-pay to move < 100k sats between channels | **3** | standard | `hive:rebalance/v1` | Costs routing fees but amount is bounded; funds stay on-node |
| Circular rebalance (large) | Self-pay to move > 100k sats | **5** | standard | `hive:rebalance/v1` | Higher fee exposure; failed partial routes can leave stuck HTLCs temporarily |
| Submarine swap (loop out) | Move on-chain → off-chain liquidity via swap service | **5** | standard | `hive:rebalance/v1` | Involves third-party swap provider; fees + timing risk; funds temporarily in-flight |
| Submarine swap (loop in) | Move off-chain → on-chain | **5** | standard | `hive:rebalance/v1` | Same as loop out, opposite direction |
| Liquidity marketplace (Pool/Magma) | Buy/sell inbound liquidity via marketplace | **5** | advanced | `hive:rebalance/v1` | Commits funds to contracts with third parties; terms are binding |
| Peer-assisted rebalance | Coordinate rebalance with a hive peer | **4** | standard | `hive:rebalance/v1` | Requires trust in peer; lower fee than circular but depends on coordination |
| Auto-rebalance rules | Configure automated rebalancing triggers | **6** | advanced | `hive:config/v1` | Autonomous spending of routing fees; mistakes compound without human oversight |

### Category 6: Channel Lifecycle

Opening and closing channels. These are on-chain transactions with real financial commitment and varying degrees of irreversibility.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Open channel (single, small) | Open channel < 1M sats | **5** | advanced | `hive:expansion/v1` | On-chain tx; funds locked until close; poor peer choice wastes capital |
| Open channel (single, large) | Open channel ≥ 1M sats | **6** | advanced | `hive:expansion/v1` | Significant capital commitment; same irreversibility |
| Open channel (batch) | Open multiple channels in single tx | **7** | advanced | `hive:expansion/v1` | Multiplied capital commitment; single bad decision affects multiple channels |
| Close channel (cooperative) | Mutual close with peer agreement | **6** | admin | `hive:channel/v1` | Funds return on-chain after confirmation; channel capacity lost; must re-open to restore |
| Close channel (unilateral) | Force close without peer cooperation | **7** | admin | `hive:channel/v1` | Funds locked for CSV delay (often 144+ blocks); penalty risk if old state broadcast |
| Close channel (force, punitive) | Force close a channel suspected of cheating | **8** | admin | `hive:channel/v1` | High stakes — wrong call means you lose; right call means they lose. Must be correct. |
| Close all channels | Force close every channel | **10** | admin | `hive:emergency/v1` | **Nuclear option.** All liquidity goes on-chain. Node is completely defunded. Recovery takes days/weeks. Only for catastrophic compromise. |

### Category 7: Splicing

In-place channel resizing. Relatively new protocol feature; irreversible once confirmed on-chain.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Splice-in (add funds) | Increase channel capacity by adding on-chain funds | **5** | advanced | `hive:splice/v1` | On-chain tx; funds committed; but adds to existing healthy channel |
| Splice-out (remove funds) | Decrease channel capacity, withdraw to on-chain | **6** | advanced | `hive:splice/v1` | Reduces channel capacity; may break routing if channel becomes too small |
| Splice + open (complex) | Combine splice with new channel open in single tx | **7** | advanced | `hive:splice/v1` | Complex multi-output tx; higher failure surface; larger capital movement |

### Category 8: Peer Management

Managing connections to other Lightning nodes. Low risk for connections; higher for disconnections.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Connect to peer | Establish TCP/Tor connection to a node | **2** | standard | `hive:peer/v1` | No fund risk; just a network connection |
| Disconnect peer (no channels) | Drop connection to peer with no shared channels | **2** | standard | `hive:peer/v1` | No impact; can reconnect anytime |
| Disconnect peer (with channels) | Drop connection to peer with active channels | **4** | standard | `hive:peer/v1` | Channels go inactive; HTLCs may time out; peer may force close if prolonged |
| Ban peer | Permanently block a peer | **5** | advanced | `hive:peer/v1` | If channels exist, this effectively kills them; hard to undo social damage |

### Category 9: Payments & Invoicing

Sending sats out of the node. This is spending money.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Create invoice | Generate a Lightning invoice to receive | **1** | monitor | `hive:payment/v1` | Receiving money; no risk |
| Keysend (small) | Send < 10k sats without invoice | **4** | standard | `hive:payment/v1` | Irreversible payment; small amount bounds exposure |
| Keysend (large) | Send ≥ 10k sats without invoice | **6** | advanced | `hive:payment/v1` | Irreversible; significant sats leave the node permanently |
| Pay invoice (small) | Pay a Lightning invoice < 10k sats | **4** | standard | `hive:payment/v1` | Same as keysend; invoice provides accountability |
| Pay invoice (large) | Pay a Lightning invoice ≥ 10k sats | **6** | advanced | `hive:payment/v1` | Irreversible; large amount leaves node |
| Multi-path payment | Pay via MPP across multiple channels | **5** | standard | `hive:payment/v1` | Spreads risk across paths but still irreversible |

### Category 10: Wallet & On-Chain Operations

Direct on-chain Bitcoin operations. These are irreversible blockchain transactions.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Generate address | Create new on-chain receive address | **1** | monitor | `hive:wallet/v1` | Receiving; no risk |
| Send on-chain (small) | Send < 100k sats on-chain | **6** | advanced | `hive:wallet/v1` | Irreversible; funds leave the node's custody entirely |
| Send on-chain (large) | Send ≥ 100k sats on-chain | **8** | admin | `hive:wallet/v1` | Irreversible; major funds leave custody |
| Send on-chain (sweep) | Send entire wallet balance | **9** | admin | `hive:wallet/v1` | Empties the wallet; effectively drains the node |
| UTXO consolidation | Combine UTXOs into fewer outputs | **4** | advanced | `hive:wallet/v1` | On-chain tx but funds stay in same wallet; cost is mining fees |
| Coin selection / UTXO freeze | Mark UTXOs as reserved or frozen | **3** | standard | `hive:wallet/v1` | Reversible; just metadata; but can block channel opens if done wrong |
| Bump fee (CPFP/RBF) | Accelerate an unconfirmed transaction | **4** | advanced | `hive:wallet/v1` | Spends additional sats on fees; bounded risk |

### Category 11: Plugin Management

Starting, stopping, and configuring CLN plugins. Plugins can have arbitrary power.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| List plugins | Show running plugins | **1** | monitor | `hive:plugin/v1` | Read-only |
| Start plugin (known/approved) | Start a plugin from the approved list | **4** | advanced | `hive:plugin/v1` | Plugins execute with full node access; even approved ones can misbehave |
| Stop plugin | Stop a running plugin | **5** | advanced | `hive:plugin/v1` | May disrupt functionality (e.g., stopping a rebalancer mid-operation) |
| Start plugin (arbitrary) | Start an unapproved/unknown plugin | **9** | admin | `hive:plugin/v1` | Arbitrary code execution with full node RPC access; equivalent to root |
| Configure plugin | Change plugin parameters | **4** | advanced | `hive:plugin/v1` | Depends on the plugin; bounded by plugin's own validation |

### Category 12: Node Configuration

Changing how the node itself operates. Affects all channels and operations.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| View configuration | Read current config | **1** | monitor | `hive:config/v1` | Read-only |
| Set alias/color | Change node's gossip alias or color | **1** | standard | `hive:config/v1` | Cosmetic; no operational impact |
| Set network address | Change advertised address (IP/Tor) | **5** | advanced | `hive:config/v1` | Wrong address makes node unreachable; peers can't connect |
| Enable/disable Tor | Toggle Tor connectivity | **5** | advanced | `hive:config/v1` | Can make node unreachable to Tor-only peers or expose clearnet IP |
| Set max channel size | Change maximum channel capacity accepted | **3** | standard | `hive:config/v1` | Limits future channels; doesn't affect existing |
| Set dust limit | Change dust threshold | **4** | advanced | `hive:config/v1` | Affects HTLC handling; too low = chain spam; too high = lost small payments |
| Restart node | Gracefully restart the Lightning daemon | **7** | admin | `hive:config/v1` | Temporary downtime; all HTLCs in flight may fail; channels go offline |

### Category 13: Backup Operations

Managing node state backups. Critical for disaster recovery.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Trigger backup | Create a new backup of node state | **2** | standard | `hive:backup/v1` | Safe — creates new backup without modifying state |
| Verify backup | Check backup integrity | **1** | monitor | `hive:backup/v1` | Read-only verification |
| Export SCB | Export Static Channel Backup file | **3** | standard | `hive:backup/v1` | Safe to create but the file itself is sensitive — could be used to force close all channels if misused |
| Restore from backup | Restore node state from backup | **10** | admin | `hive:backup/v1` | **Catastrophic if wrong backup used.** Old state = penalty transactions = loss of all channel funds. Only for actual disaster recovery. |

### Category 14: Emergency Operations

Last-resort actions for compromised or failing nodes. Maximum danger, maximum impact.

| Task | Description | Danger | Tier | Schema | Rationale |
|------|------------|--------|------|--------|-----------|
| Emergency disable forwarding | Immediately stop all routing | **6** | advanced | `hive:emergency/v1` | Stops revenue but prevents further damage; reversible |
| Emergency fee spike | Set all fees to maximum to deter routing | **5** | advanced | `hive:emergency/v1` | Soft version of disabling; deters traffic without fully stopping it |
| Force close specific channel | Emergency close of a suspected-compromised channel | **8** | admin | `hive:emergency/v1` | Funds locked for CSV; may lose in-flight HTLCs; but limits blast radius |
| Force close all channels | Nuclear option — close everything | **10** | admin | `hive:emergency/v1` | Total defunding; all funds locked on-chain; recovery takes days/weeks; only for catastrophic compromise |
| Revoke all agent credentials | Disable all remote management access | **3** | admin | `hive:emergency/v1` | Safe and prudent if compromise suspected; can re-issue later |

### Danger Score Distribution

```
Score 1  [██████████████] 14 tasks  — Read-only, receive-only
Score 2  [███████]         7 tasks  — Cosmetic, backup, simple peer ops
Score 3  [████████]        8 tasks  — Single-channel fee changes, simple policies
Score 4  [██████████]     10 tasks  — Bulk policies, small payments, config changes
Score 5  [██████████]     10 tasks  — Swaps, large rebalances, network config
Score 6  [████████]        8 tasks  — Channel opens, on-chain sends, large payments
Score 7  [████]            4 tasks  — Batch opens, unilateral closes, restarts
Score 8  [███]             3 tasks  — Large on-chain sends, punitive closes
Score 9  [██]              2 tasks  — Wallet sweep, arbitrary plugin execution
Score 10 [██]              2 tasks  — Close all channels, restore from backup
```

### Pricing Implications

Danger score directly feeds into per-action pricing. The cost of delegated management should reflect the risk the operator is transferring to the agent.

#### Base Pricing by Danger Tier

| Danger Range | Pricing Tier | Base Cost (sats/action) | Credential Required | Approval Mode |
|-------------|-------------|------------------------|-------------------|---------------|
| **1–2** (Routine) | Free / Minimal | 0–5 | `monitor` | Auto-execute |
| **3–4** (Standard) | Low | 5–25 | `standard` | Auto-execute (high-rep agent) or queue |
| **5–6** (Elevated) | Medium | 25–100 | `standard` / `advanced` | Auto-execute (high-rep) or queue for review |
| **7–8** (High) | Premium | 100–500 | `advanced` / `admin` | Require explicit operator confirmation |
| **9–10** (Critical) | Critical | 500+ or flat fee | `admin` | Multi-sig: N-of-M confirmations required |

#### Mutual Trust Discount

Pricing is modulated by **mutual reputation** — both the agent's track record AND the operator's history of fair dealing:

```
effective_price = base_price × agent_trust_modifier × operator_trust_modifier

agent_trust_modifier:
  - New agent (no history):       1.5x  (premium for unknown risk)
  - Established (>30 days):       1.0x  (baseline)
  - Proven (>90 days, good metrics): 0.7x  (discount for reliability)

operator_trust_modifier:
  - New operator:                 1.0x  (baseline)
  - History of disputes:          1.3x  (agent charges more for difficult clients)
  - Clean history:                0.9x  (discount for easy clients)
```

For **performance-based pricing**, the danger score sets the floor: even if performance bonuses drive the bulk of compensation, agents should receive minimum per-action fees proportional to the risk they're managing.

### Permission Mapping

The mapping from danger score to permission tier follows a conservative principle: **the minimum tier that can safely execute a task without undue risk to node funds.**

| Danger Score | Minimum Tier | Reasoning |
|-------------|-------------|-----------|
| 1–2 | `monitor` | No state changes or negligible impact |
| 3–4 | `standard` | Reversible changes, bounded financial impact |
| 5 | `standard` (with constraints) | Moderate risk, requires credential constraints (amount limits, rate limits) |
| 6 | `advanced` | Significant capital commitment or irreversible on-chain action |
| 7 | `advanced` (with approval queue) | Even advanced agents should queue these for operator review |
| 8 | `admin` | Only fully trusted agents; operator confirmation required |
| 9 | `admin` (restricted) | Must be explicitly granted per-task; not included in blanket admin |
| 10 | `admin` + multi-sig | Should never auto-execute; requires N-of-M confirmation |

Note that a `standard` credential with tight constraints (low `max_rebalance_sats`, low `max_fee_change_pct`) can safely handle score-5 tasks. The constraint system in the Management Credential acts as a continuous dial, not just a tier gate.

### Approval Workflows

The approval flow for each action is determined by `danger_score × agent_reputation_inverse`:

```
approval_level = danger_score × (1 / agent_reputation_score)

where agent_reputation_score ∈ [0.5, 2.0]:
  0.5 = brand new, untested agent
  1.0 = baseline established agent
  2.0 = highly proven, long-tenure agent
```

#### Workflow Definitions

**Auto-Execute** (approval_level < 4)
- Action executes immediately upon credential + payment validation
- Receipt generated and logged
- Operator notified async (daily digest or real-time, configurable)

**Queue for Review** (approval_level 4–6)
- Action is validated and held in a pending queue
- Operator receives notification with action details, agent reputation, and risk assessment
- Auto-expires after configurable timeout (default: 24h)
- Operator can approve, reject, or modify parameters

**Require Explicit Confirmation** (approval_level 7–8)
- Action is validated, held, and operator is actively pinged (push notification, Nostr DM, etc.)
- Agent receives a challenge: must re-sign the action after operator's pre-approval
- Two-step: operator approves → agent confirms → execution
- Timeout: 4h (shorter because these are usually time-sensitive)

**Multi-Sig Confirmation** (approval_level > 8)
- Requires N-of-M confirmations from designated approvers
- Approvers are defined in the node's local policy (e.g., 2-of-3: operator + backup operator + trusted advisor)
- Each approver signs the action independently via their DID
- Action executes only when threshold is met
- No timeout — waits indefinitely until threshold met or explicitly cancelled

#### Example Scenarios

| Task | Danger | Agent Rep | Approval Level | Workflow |
|------|--------|-----------|---------------|----------|
| Set fee rate (single) | 3 | Proven (2.0) | 1.5 | Auto-execute |
| Set fee rate (single) | 3 | New (0.5) | 6.0 | Queue for review |
| Circular rebalance (large) | 5 | Established (1.0) | 5.0 | Queue for review |
| Circular rebalance (large) | 5 | Proven (2.0) | 2.5 | Auto-execute |
| Open channel (large) | 6 | Proven (2.0) | 3.0 | Auto-execute |
| Open channel (large) | 6 | New (0.5) | 12.0 | Multi-sig |
| Force close all | 10 | Proven (2.0) | 5.0 | Queue for review |
| Force close all | 10 | Established (1.0) | 10.0 | Multi-sig |

Note that even a proven agent gets "Queue for review" for nuclear operations. The system is intentionally conservative — the maximum damage a compromised proven-agent can cause is bounded by the approval_level floor.

#### Configurable Override

Operators can override the calculated approval level per-task or per-category:

```json
{
  "approval_overrides": {
    "channel_close_*": "always_confirm",
    "fee_policy_*": "auto_execute",
    "emergency_*": "multi_sig_2_of_3"
  }
}
```

This ensures operators retain ultimate control over their risk tolerance, regardless of computed approval levels.

---

## Trust Model

### Defense in Depth

Three independent layers of validation, each sufficient to block unauthorized actions:

1. **DID Credential** — Is this agent authorized? Is the credential valid, unexpired, unrevoked? Does it grant the required permission tier?

2. **Payment Proof** — Has the agent paid for this action? Is the L402 macaroon valid? Is the Cashu token redeemable?

3. **Local Policy** — Does the node's own policy allow this action, regardless of credential scope? (e.g., "never change fees more than 25% in 24h")

All three must pass. An agent with a valid credential and payment proof can still be blocked by local policy.

### Threat Model

| Threat | Mitigation |
|--------|-----------|
| Stolen credential | Expiration + revocation via Archon. Operator can revoke instantly. |
| Replay attack | Monotonic nonce + timestamp window. Node tracks per-agent nonce state. |
| Malicious fee manipulation | Local policy engine enforces bounds. Credential constraints limit change magnitude. |
| Payment fraud | Cashu tokens are verified with mint before execution. L402 macaroons are cryptographically bound. |
| Man-in-the-middle | Bolt 8 provides authenticated encryption. Management messages are additionally signed by agent DID. |
| Agent compromise | Credential scope limits blast radius. `monitor` tier can't modify anything. Operator can revoke immediately. |
| Denial of service | Rate limiting per DID. Daily action cap in credential constraints. |

### Audit Trail

Every management action produces a signed receipt containing:
- The original command (schema + params)
- The agent's DID and credential reference
- The payment proof
- The execution result
- A state hash (node state before and after)
- The node's signature over all of the above

Receipts are stored locally and can be published to the Archon network for verifiable reputation building.

---

## Reputation System

> **Note:** The reputation system described here implements the **`hive:advisor` profile** of the general [DID Reputation Schema](./DID-REPUTATION-SCHEMA.md). That spec defines a universal `DIDReputationCredential` format for any DID holder — this section describes the Lightning fleet-specific application.

### Agent Reputation

An agent's reputation is built from verifiable, cryptographic evidence:

1. **Management Receipts** — Signed by the managed node, proving the agent took specific actions
2. **Outcome Measurements** — Revenue delta, channel health delta, measured N days after action
3. **Client Credentials** — Operators issuing "this agent managed my node from X to Y with Z% revenue improvement"
4. **Tenure** — Duration of continuous management relationships

The `HiveAdvisorReputationCredential` is a `DIDReputationCredential` with `domain: "hive:advisor"`:

```json
{
  "@context": [
    "https://www.w3.org/2018/credentials/v1",
    "https://archon.technology/schemas/reputation/v1"
  ],
  "type": ["VerifiableCredential", "DIDReputationCredential"],
  "issuer": "did:cid:<node_operator>",
  "credentialSubject": {
    "id": "did:cid:<agent_did>",
    "domain": "hive:advisor",
    "period": {
      "start": "2026-02-14T00:00:00Z",
      "end": "2026-03-14T00:00:00Z"
    },
    "metrics": {
      "revenue_delta_pct": 340,
      "actions_taken": 87,
      "uptime_pct": 99.2,
      "channels_managed": 19
    },
    "outcome": "renew",
    "evidence": [
      {
        "type": "SignedReceipt",
        "id": "did:cid:<receipt_credential_did>",
        "description": "87 signed management receipts from managed node"
      }
    ]
  }
}
```

See [DID Reputation Schema — `hive:advisor` Profile](./DID-REPUTATION-SCHEMA.md#profile-hiveadvisor) for the full metric definitions and aggregation rules.

### Discovering Advisors

Agents can publish their capabilities and reputation to the Archon network:

```json
{
  "type": "HiveAdvisorProfile",
  "subject": "did:cid:<agent_did>",
  "capabilities": ["fee-optimization", "rebalancing", "expansion-planning"],
  "supported_schemas": ["hive:fee-policy/v1", "hive:rebalance/v1", "hive:config/v1"],
  "pricing": {
    "model": "performance",
    "base_sats_monthly": 5000,
    "performance_share_pct": 10
  },
  "reputation": {
    "nodes_managed": 12,
    "avg_revenue_improvement_pct": 180,
    "avg_tenure_days": 45,
    "credentials": ["did:cid:...", "did:cid:..."]
  }
}
```

Node operators discover advisors by querying the Archon network for `HiveAdvisorProfile` credentials, filtering by capabilities, pricing, and verified reputation.

---

## Integration with Existing Hive Protocol

### Settlement Integration

Remote fleet management generates settlement obligations — the managed node may owe advisors performance bonuses, and advisors may owe nodes for resources consumed during management actions. The [DID + Cashu Hive Settlements Protocol](./DID-HIVE-SETTLEMENTS.md) defines how these obligations are tracked, netted, and settled trustlessly. Management receipts (signed by both parties per this spec) serve as the proof substrate for settlement computation.

### Enrollment via Hive PKI

The existing hive PKI handshake is extended to include management credential exchange:

1. Node joins the hive (existing PKI handshake)
2. Node operator generates a `HiveManagementCredential` for the fleet advisor's DID
3. Credential is shared during the next hive gossip round
4. Advisor's node detects the credential and establishes a Bolt 8 management channel
5. Advisor begins sending management commands

### Relationship to Existing Advisor

The current centralized advisor (Claude-based, running on fleet operator's infrastructure) would be the first "client" of this protocol. Instead of direct RPC access, it would authenticate via DID and communicate via schemas.

**Migration path:**
1. **Phase 1:** Current advisor continues with direct RPC. Schemas are defined and tested.
2. **Phase 2:** Advisor communicates via schemas over local RPC (same machine, but using the schema format)
3. **Phase 3:** Advisor communicates via Bolt 8 transport (can now run on any machine)
4. **Phase 4:** Third-party advisors can offer management services

### Governance

New schemas are proposed through the existing hive governance process:
1. Any member proposes a new schema type
2. Members review and vote (quorum required)
3. Approved schemas are published as verifiable credentials
4. Nodes update their supported schema list

Schema proposals that grant new permissions require higher quorum thresholds.

---

## Implementation Roadmap

### Phase 1: Schema Definition (2-4 weeks)
- Define core schemas (fee-policy, rebalance, config, monitor)
- Build schema validation library
- Add schema-based command interface to cl-hive plugin
- Unit tests with mock data

### Phase 2: DID Authentication (2-4 weeks)
- Integrate Archon credential verification into cl-hive
- Implement management credential issuance in Archon Keymaster
- Build credential validation middleware
- Implement revocation checking

### Phase 3: Payment Integration (2-4 weeks)
- L402 macaroon issuance and verification
- Cashu token redemption
- Per-action and subscription payment models
- Payment accounting and receipt generation

### Phase 4: Bolt 8 Transport (2-4 weeks)
- Custom message type registration (49152/49153)
- Message serialization/deserialization
- Replay protection (nonce tracking)
- CLN custom message handler integration

### Phase 5: Reputation & Discovery (4-6 weeks)
- Reputation credential schema
- Advisor profile publishing
- Discovery queries via Archon network
- Performance measurement and auto-credentialing

### Phase 6: Marketplace (ongoing)
- Advisor onboarding flow
- Multi-advisor support per node
- Conflict resolution (multiple advisors, competing recommendations)
- Economic optimization (advisor fee competition)

---

## Open Questions

1. **Conflict resolution:** If a node has multiple advisors, how are conflicting recommendations resolved? Priority by tier? Most recent credential? Voting?

2. **Schema evolution:** How do we handle breaking schema changes? Feature bit negotiation (like Lightning)? Grace periods?

3. **Mint trust:** For Cashu payments, which mint(s) are trusted? Node operator's choice? Hive-endorsed mints?

4. **Latency:** Bolt 8 custom messages add a round trip per command. For time-sensitive actions (velocity alerts), is this acceptable? Should critical schemas have a "pre-authorized" mode?

5. **Cross-implementation:** This design assumes CLN. How portable is it to LND/Eclair/LDK? Custom messages are supported but implementations vary.

6. **Privacy:** Management receipts prove what actions an advisor took. Should there be an option to keep management relationships private (no public reputation building)?

---

## References

- [BOLT 8: Encrypted and Authenticated Transport](https://github.com/lightning/bolts/blob/master/08-transport.md)
- [L402: Lightning HTTP 402 Protocol](https://docs.lightning.engineering/the-lightning-network/l402)
- [Cashu: Chaumian Ecash for Bitcoin](https://cashu.space/)
- [W3C DID Core 1.0](https://www.w3.org/TR/did-core/)
- [W3C Verifiable Credentials Data Model 2.0](https://www.w3.org/TR/vc-data-model-2.0/)
- [DID + Cashu Task Escrow Protocol](./DID-CASHU-TASK-ESCROW.md)
- [DID + Cashu Hive Settlements Protocol](./DID-HIVE-SETTLEMENTS.md)
- [DID Reputation Schema](./DID-REPUTATION-SCHEMA.md)
- [Archon: Decentralized Identity for AI Agents](https://github.com/archetech/archon)
- [Lightning Hive: Swarm Intelligence for Lightning](https://github.com/lightning-goats/cl-hive)
- [CLN Custom Messages](https://docs.corelightning.org/reference/lightning-sendcustommsg)
- [DID Reputation Schema](./DID-REPUTATION-SCHEMA.md)

---

*Feedback welcome. File issues on [cl-hive](https://github.com/lightning-goats/cl-hive) or discuss in #singularity.*

*— Hex ⬡*
