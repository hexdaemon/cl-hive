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

| Tier | Permissions | Trust Level | Typical Use |
|------|-----------|-------------|-------------|
| `monitor` | Read-only metrics, health checks | Minimal | Monitoring services, dashboards |
| `standard` | Fee policy, rebalancing, config tuning | Moderate | Routine optimization |
| `advanced` | All standard + channel opens + expansion proposals | High | Full fleet management |
| `admin` | All permissions including channel closes | Maximum | Trusted long-term partner |

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
- [Archon: Decentralized Identity for AI Agents](https://github.com/archetech/archon)
- [Lightning Hive: Swarm Intelligence for Lightning](https://github.com/lightning-goats/cl-hive)
- [CLN Custom Messages](https://docs.corelightning.org/reference/lightning-sendcustommsg)
- [DID Reputation Schema](./DID-REPUTATION-SCHEMA.md)

---

*Feedback welcome. File issues on [cl-hive](https://github.com/lightning-goats/cl-hive) or discuss in #singularity.*

*— Hex ⬡*
