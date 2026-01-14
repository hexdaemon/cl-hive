# Payment-Based Inter-Hive Protocol Specification

**Version:** 0.1.0-draft
**Status:** Proposal
**Authors:** cl-hive contributors
**Date:** 2025-01-14

## Abstract

This specification defines a Lightning payment-based protocol for inter-hive communication, discovery, and trust verification. All coordination uses actual Lightning payments as the transport and verification layer, ensuring that claims about network position, liquidity, and relationships are economically verified rather than trusted.

**Core Principle**: Payments don't lie. Use them to verify everything.

## Table of Contents

1. [Motivation](#1-motivation)
2. [Design Principles](#2-design-principles)
3. [Payment-Based Communication](#3-payment-based-communication)
4. [Hive Discovery Protocol](#4-hive-discovery-protocol)
5. [Hidden Hive Detection](#5-hidden-hive-detection)
6. [Reputation-Gated Messaging](#6-reputation-gated-messaging)
7. [Continuous Verification](#7-continuous-verification)
8. [Economic Security Model](#8-economic-security-model)
9. [Protocol Messages](#9-protocol-messages)
10. [Implementation Guidelines](#10-implementation-guidelines)

---

## 1. Motivation

### 1.1 The Problem with Message-Based Protocols

Traditional protocols rely on signed messages:
- Messages can claim anything ("I have 100 BTC capacity")
- Signatures prove identity, not capability
- No cost to lie (spam, false claims)
- Network position is self-reported

### 1.2 Payments as Proof

Lightning payments inherently prove:
- **Channel existence**: Payment fails if no path
- **Liquidity**: Payment fails if insufficient balance
- **Network position**: Route reveals actual topology
- **Bidirectional capability**: Can send AND receive
- **Economic commitment**: Real sats at stake

### 1.3 Trust Through Verification

Instead of:
```
"Trust me, I'm a friendly hive" → OK, you're trusted
```

We get:
```
"Trust me, I'm a friendly hive" → Prove it with payments → Verified or rejected
```

---

## 2. Design Principles

### 2.1 Payment as Authentication

Every claim must be backed by a payment that proves the claim:

| Claim | Payment Proof |
|-------|---------------|
| "I exist" | Receive my payment |
| "I can reach you" | Send you a payment |
| "I have liquidity" | Send large payment |
| "I'm part of Hive X" | Payment from Hive X admin |
| "I'm not hostile" | Stake payment in escrow |

### 2.2 Continuous Verification

Trust is not a state, it's a continuous stream of verified payments:

```
Initial verification → Periodic re-verification → Every interaction verified
         ↓                      ↓                         ↓
    Stake payment         Heartbeat payments        Message payments
```

### 2.3 Economic Deterrence

Make attacks expensive:
- Every message costs sats
- False claims forfeit stakes
- Reputation requires sustained payment history
- Detection costs less than evasion

### 2.4 Symmetry

If you can query me, I can query you. No asymmetric information advantages.

---

## 3. Payment-Based Communication

### 3.1 Message Payment Structure

All inter-hive messages are sent via keysend with custom TLV:

```
┌─────────────────────────────────────────────────────────────┐
│                    HIVE MESSAGE PAYMENT                      │
├─────────────────────────────────────────────────────────────┤
│  Amount: message_fee + optional_stake                        │
│                                                              │
│  TLV Records:                                                │
│    5482373484 (keysend preimage)                            │
│    48495645 ("HIVE" magic):                                  │
│      {                                                       │
│        "protocol": "hive_inter",                            │
│        "version": 1,                                         │
│        "msg_type": "query_hive_status",                     │
│        "payload": {...},                                     │
│        "reply_invoice": "lnbc...",                          │
│        "stake_hash": "abc123...",                           │
│        "sender_hive": "hive_xyz" | null                     │
│      }                                                       │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 Message Fee Schedule

| Message Type | Base Fee | Stake Required | Reply Expected |
|--------------|----------|----------------|----------------|
| ping | 10 sats | No | Yes (pong) |
| query_hive_status | 100 sats | No | Yes |
| hive_introduction | 1,000 sats | 10,000 sats | Yes |
| federation_request | 10,000 sats | 100,000 sats | Yes |
| intel_share | 500 sats | No | Optional |
| defense_alert | 0 sats | 50,000 sats | Yes |
| reputation_query | 100 sats | No | Yes |

### 3.3 Reply Mechanism (Privacy-Preserving)

**Problem**: BOLT11 invoices leak sender information:
- Node ID embedded in invoice
- Route hints reveal channel structure
- Payment hash allows correlation

**Solution**: Use keysend-based replies with encrypted reply tokens.

```python
class PrivacyPreservingReply:
    """Reply mechanism that doesn't leak sender identity."""

    def __init__(self):
        # Rotate reply encryption key daily
        self.reply_key = self.derive_daily_reply_key()
        self.pending_replies = {}  # reply_token -> callback

    def create_reply_token(self, msg_type: str, correlation_id: str) -> str:
        """Create encrypted reply token that only we can decode."""

        # Token contains: timestamp, msg_type, correlation_id
        token_data = {
            "ts": int(time.time()),
            "msg": msg_type,
            "cid": correlation_id
        }

        # Encrypt with our reply key (AES-GCM or ChaCha20-Poly1305)
        # Only we can decrypt this token
        plaintext = json.dumps(token_data).encode()
        nonce = os.urandom(12)

        # Use CLN's HSM for encryption if available, else local key
        ciphertext = self.encrypt_with_reply_key(plaintext, nonce)

        # Base64 encode for transport
        return base64.b64encode(nonce + ciphertext).decode()

    def decode_reply_token(self, token: str) -> Optional[dict]:
        """Decode a reply token we previously created."""

        try:
            raw = base64.b64decode(token)
            nonce = raw[:12]
            ciphertext = raw[12:]

            plaintext = self.decrypt_with_reply_key(ciphertext, nonce)
            token_data = json.loads(plaintext)

            # Verify token isn't expired (max 24 hours)
            if time.time() - token_data["ts"] > 86400:
                return None

            return token_data

        except Exception:
            return None

def send_hive_message(self, target: str, msg_type: str, payload: dict) -> str:
    """Send payment-based hive message with privacy-preserving reply."""

    # Create correlation ID for this message
    correlation_id = generate_id()

    # Create encrypted reply token (instead of invoice)
    reply_token = self.reply_handler.create_reply_token(
        msg_type=msg_type,
        correlation_id=correlation_id
    )

    # Calculate total amount
    amount = MESSAGE_FEES[msg_type]
    if msg_type in STAKE_REQUIRED:
        amount += STAKE_REQUIRED[msg_type]

    # Build TLV payload - NO invoice, just reply token
    tlv_payload = {
        "protocol": "hive_inter",
        "version": 1,
        "msg_type": msg_type,
        "payload": payload,
        "reply_token": reply_token,  # Encrypted token, not invoice
        "stake_hash": self.create_stake_hash() if msg_type in STAKE_REQUIRED else None,
        "sender_hive": self.our_hive_id
    }

    # Send keysend with TLV
    result = self.keysend(
        destination=target,
        amount_msat=amount * 1000,
        tlv_records={
            5482373484: os.urandom(32),  # keysend preimage
            48495645: json.dumps(tlv_payload).encode()
        }
    )

    # Store pending reply callback
    self.reply_handler.pending_replies[correlation_id] = {
        "target": target,
        "msg_type": msg_type,
        "sent_at": time.time()
    }

    return correlation_id

def send_reply(self, original_sender: str, reply_token: str, response: dict) -> bool:
    """Send reply via keysend (not invoice payment)."""

    # We know the sender's node ID from the keysend we received
    # Send reply directly via keysend with the reply token

    reply_payload = {
        "protocol": "hive_inter",
        "version": 1,
        "msg_type": response["msg_type"],
        "payload": response["payload"],
        "in_reply_to": reply_token  # Include their token for correlation
    }

    result = self.keysend(
        destination=original_sender,
        amount_msat=MESSAGE_FEES.get(response["msg_type"], 100) * 1000,
        tlv_records={
            5482373484: os.urandom(32),
            48495645: json.dumps(reply_payload).encode()
        }
    )

    return result.success

def handle_reply(self, payment: Payment) -> Optional[dict]:
    """Handle incoming reply to our message."""

    msg = self.extract_hive_message(payment)
    if not msg or "in_reply_to" not in msg:
        return None

    # Decode the reply token to find our original message
    token_data = self.reply_handler.decode_reply_token(msg["in_reply_to"])
    if not token_data:
        return None  # Invalid or expired token

    # Match to pending reply
    correlation_id = token_data["cid"]
    pending = self.reply_handler.pending_replies.get(correlation_id)

    if pending:
        # Valid reply to our message
        del self.reply_handler.pending_replies[correlation_id]
        return {
            "original_msg_type": token_data["msg"],
            "correlation_id": correlation_id,
            "response": msg["payload"]
        }

    return None
```

**Why This Is More Private:**

| Aspect | BOLT11 Invoice | Reply Token |
|--------|---------------|-------------|
| Reveals node ID | Yes | No |
| Reveals route hints | Yes | No |
| Correlatable payment hash | Yes | No (keysend uses random preimage) |
| Replayable | Yes (same invoice) | No (token expires, single use) |
| Third-party observable | Invoice can be shared | Token only meaningful to creator |

### 3.4 Payment Verification

Every received message is verified:

```python
def verify_message_payment(self, payment: Payment) -> MessageVerification:
    """Verify incoming hive message payment."""

    # Extract TLV
    hive_tlv = payment.tlv_records.get(48495645)
    if not hive_tlv:
        return MessageVerification(valid=False, reason="no_hive_tlv")

    try:
        msg = json.loads(hive_tlv)
    except:
        return MessageVerification(valid=False, reason="invalid_json")

    # Verify protocol
    if msg.get("protocol") != "hive_inter":
        return MessageVerification(valid=False, reason="wrong_protocol")

    # Verify payment amount covers fee
    required_fee = MESSAGE_FEES.get(msg["msg_type"], 0)
    required_stake = STAKE_REQUIRED.get(msg["msg_type"], 0)

    if payment.amount_msat < (required_fee + required_stake) * 1000:
        return MessageVerification(valid=False, reason="insufficient_payment")

    # Reply token is encrypted and doesn't leak info - just store it
    # We'll use it when sending our reply via keysend

    return MessageVerification(
        valid=True,
        msg_type=msg["msg_type"],
        payload=msg["payload"],
        sender=payment.sender,  # Known from keysend routing
        sender_hive=msg.get("sender_hive"),
        stake_amount=required_stake,
        reply_token=msg.get("reply_token")  # Encrypted, privacy-preserving
    )
```

---

## 4. Hive Discovery Protocol

### 4.1 Direct Query: "Are You A Hive?"

Any node can query any other node:

```
┌─────────┐                              ┌─────────┐
│ Node A  │                              │ Node B  │
└────┬────┘                              └────┬────┘
     │                                        │
     │  Payment: 100 sats                     │
     │  TLV: query_hive_status                │
     │  reply_invoice: lnbc100n...            │
     │ ─────────────────────────────────────► │
     │                                        │
     │         Payment: 100 sats              │
     │         TLV: hive_status_response      │
     │ ◄───────────────────────────────────── │
     │                                        │
```

**Query Message:**
```json
{
  "msg_type": "query_hive_status",
  "payload": {
    "query_id": "q_abc123",
    "include_members": false,
    "include_federation": false
  }
}
```

**Response Options:**

1. **"Yes, I'm in a hive":**
```json
{
  "msg_type": "hive_status_response",
  "payload": {
    "query_id": "q_abc123",
    "is_hive_member": true,
    "hive_id": "hive_xyz789",
    "member_tier": "member",
    "hive_public": true,
    "verification_offer": {
      "type": "admin_voucher",
      "admin_node": "03admin...",
      "voucher_payment": 1000
    }
  }
}
```

2. **"No, I'm independent":**
```json
{
  "msg_type": "hive_status_response",
  "payload": {
    "query_id": "q_abc123",
    "is_hive_member": false,
    "open_to_joining": true,
    "requirements": ["min_capacity_10m", "min_channels_5"]
  }
}
```

3. **"None of your business"** (valid response):
```json
{
  "msg_type": "hive_status_response",
  "payload": {
    "query_id": "q_abc123",
    "declined": true,
    "reason": "private"
  }
}
```

### 4.2 Hive Membership Verification

Claims of hive membership must be verified:

```
┌─────────┐         ┌─────────┐         ┌─────────────┐
│ Querier │         │ Claimer │         │ Hive Admin  │
└────┬────┘         └────┬────┘         └──────┬──────┘
     │                   │                      │
     │  "Are you in      │                      │
     │   hive_xyz?"      │                      │
     │ ─────────────────►│                      │
     │                   │                      │
     │  "Yes, verify     │                      │
     │   with admin"     │                      │
     │ ◄─────────────────│                      │
     │                   │                      │
     │  Payment: 1000 sats                      │
     │  "Is 03claimer... in your hive?"        │
     │ ────────────────────────────────────────►│
     │                   │                      │
     │  Payment: 1000 sats                      │
     │  "Yes, member since <date>,             │
     │   tier: member, voucher: <sig>"         │
     │ ◄────────────────────────────────────────│
     │                   │                      │
```

**Admin Voucher:**
```json
{
  "msg_type": "membership_voucher",
  "payload": {
    "hive_id": "hive_xyz789",
    "member_node": "03claimer...",
    "member_since": 1700000000,
    "member_tier": "member",
    "voucher_expires": 1705234567,
    "voucher_signature": "admin_sig_of_above_fields"
  }
}
```

### 4.3 Hive Introduction Protocol

When hives want to establish contact:

```python
class HiveIntroduction:
    """Protocol for hive-to-hive introduction."""

    def initiate_introduction(self, target_hive_admin: str) -> IntroductionResult:
        """Initiate introduction to another hive."""

        # Step 1: Send introduction with stake
        intro_payment = self.send_hive_message(
            target=target_hive_admin,
            msg_type="hive_introduction",
            payload={
                "our_hive_id": self.hive_id,
                "our_admin_nodes": self.get_admin_nodes(),
                "our_member_count": self.get_member_count(),
                "our_capacity_tier": self.get_capacity_tier(),
                "introduction_stake": 10000,  # sats locked
                "proposed_relationship": "observer",
                "our_public_reputation": self.get_public_reputation()
            }
        )

        # Stake is locked until:
        # - They respond positively (stake returned)
        # - They respond negatively (stake returned minus fee)
        # - Timeout (stake returned)
        # - We misbehave (stake forfeited)

        return self.await_introduction_response(intro_payment)

    def handle_introduction(self, msg: HiveMessage) -> IntroductionResponse:
        """Handle incoming hive introduction."""

        # Verify stake was included
        if msg.stake_amount < 10000:
            return self.reject_introduction("insufficient_stake")

        # Verify their claims with payment probes
        verification = self.verify_hive_claims(msg.payload)

        if not verification.passed:
            # Return stake minus verification fee
            self.return_stake(msg, deduct=1000)
            return self.reject_introduction(verification.reason)

        # Check our policy toward unknown hives
        if not self.accept_new_introductions():
            self.return_stake(msg, deduct=0)
            return self.reject_introduction("not_accepting")

        # Accept introduction, return stake, begin observation
        self.return_stake(msg, deduct=0)
        self.create_hive_relationship(
            hive_id=msg.payload["our_hive_id"],
            status="observing",
            introduced_at=time.time()
        )

        return self.accept_introduction()
```

---

## 5. Hidden Hive Detection

### 5.1 The Challenge

Sophisticated hives may hide their coordination:
- Use non-zero internal fees (1-5 ppm)
- Stagger actions over days
- Avoid naming patterns
- Use diverse external peers
- **Detect and evade probes by temporarily changing behavior**

### 5.2 Anti-Evasion Probe Design

**Problem**: If probes are detectable, hives can evade by changing behavior during probing.

**Detection Signatures to Avoid**:
- Round payment amounts (10,000 sats)
- Systematic node pair coverage
- Rapid sequential payments
- Payments from known "probe" nodes
- Unusual payment patterns (small amounts, no invoice)

**Stealthy Probing Principles**:

```python
class StealthyProber:
    """Probe design that resists detection and evasion."""

    def design_stealth_probe(self, target: str) -> ProbeDesign:
        """Design a probe that looks like normal traffic."""

        return ProbeDesign(
            # Use realistic amounts (from actual payment distribution)
            amount=self.sample_realistic_amount(),

            # Random timing (not systematic)
            delay=random.expovariate(1/3600),  # Exponential, avg 1 hour

            # Use rotating source nodes
            source=self.select_probe_source(),

            # Include plausible payment context
            payment_context="invoice",  # Not keysend

            # Add noise payments that aren't probes
            include_decoys=True
        )

    def sample_realistic_amount(self) -> int:
        """Sample from real payment amount distribution."""
        # Based on network payment statistics
        # Avoid round numbers
        base_amounts = [
            1_000, 2_500, 5_000, 7_500, 10_000, 15_000,
            25_000, 50_000, 100_000, 250_000, 500_000
        ]
        base = random.choice(base_amounts)
        # Add random noise: ±10%
        noise = random.uniform(-0.1, 0.1)
        return int(base * (1 + noise))

    def select_probe_source(self) -> str:
        """Select source node for probe to avoid fingerprinting."""
        # Rotate through multiple source nodes
        # Don't always probe from our hive's nodes
        # Use "neutral" nodes when possible
        sources = self.get_available_probe_sources()
        return random.choice(sources)
```

**Extended Observation Window**:

```python
class ExtendedProbeWindow:
    """Probe over extended periods to defeat temporary evasion."""

    # Minimum observation period
    MIN_OBSERVATION_DAYS = 30

    # Probes per node pair
    PROBES_PER_PAIR = 50

    def schedule_extended_probes(
        self,
        nodes: List[str],
        days: int = 30
    ) -> ProbeSchedule:
        """Schedule probes over extended period."""

        schedule = []

        for src, dst in permutations(nodes, 2):
            # Distribute probes randomly over the period
            for _ in range(self.PROBES_PER_PAIR):
                # Random time within observation window
                probe_time = time.time() + random.uniform(0, days * 86400)

                schedule.append(ProbeTask(
                    source=src,
                    destination=dst,
                    scheduled_time=probe_time,
                    probe_design=self.design_stealth_probe(dst)
                ))

        # Randomize order to prevent pattern detection
        random.shuffle(schedule)

        return ProbeSchedule(tasks=schedule, duration_days=days)

    def analyze_extended_results(
        self,
        results: List[ProbeResult]
    ) -> ExtendedAnalysis:
        """Analyze results from extended probing."""

        # Group by time period to detect evasion attempts
        daily_results = self.group_by_day(results)

        # Check for inconsistent behavior (evasion signature)
        consistency_scores = []
        for day1, day2 in combinations(daily_results.keys(), 2):
            r1 = daily_results[day1]
            r2 = daily_results[day2]
            consistency = self.compare_day_results(r1, r2)
            consistency_scores.append(consistency)

        # Low consistency = possible evasion attempts
        avg_consistency = np.mean(consistency_scores)

        if avg_consistency < 0.7:
            return ExtendedAnalysis(
                verdict="evasion_suspected",
                confidence=0.8,
                reason="Inconsistent behavior across observation period",
                recommendation="Classify as suspicious, increase monitoring"
            )

        # Analyze aggregated results
        return self.analyze_aggregated(results)
```

**Parallel Multi-Source Probing**:

```python
def parallel_probe_from_multiple_sources(
    self,
    target_cluster: List[str]
) -> MultiSourceResults:
    """Probe from multiple independent sources simultaneously."""

    # Use probes from:
    # 1. Our own hive nodes
    # 2. Federated hive nodes (with permission)
    # 3. Neutral routing nodes we have channels with

    probe_sources = {
        "own": self.get_own_probe_nodes(),
        "federated": self.get_federated_probe_nodes(),
        "neutral": self.get_neutral_probe_nodes()
    }

    results_by_source = {}

    for source_type, sources in probe_sources.items():
        results_by_source[source_type] = []
        for source in sources:
            for target in target_cluster:
                result = self.probe_route(source, target)
                results_by_source[source_type].append(result)

    # Compare results across sources
    # If target cluster treats different sources differently = intelligence
    return self.compare_multi_source_results(results_by_source)
```

### 5.3 Payment-Based Probing

**Payments reveal what messages cannot:**

```python
class HiddenHiveDetector:
    """Detect hidden hives through payment probing."""

    def probe_suspected_cluster(self, nodes: List[str]) -> ClusterAnalysis:
        """Probe suspected hive cluster with payments."""

        results = {
            "internal_routing": {},
            "fee_consistency": {},
            "liquidity_correlation": {},
            "response_timing": {},
        }

        # Probe 1: Internal routing costs
        for src in nodes:
            for dst in nodes:
                if src != dst:
                    probe = self.probe_route(src, dst, amount=10000)
                    results["internal_routing"][(src, dst)] = {
                        "success": probe.success,
                        "fee_paid": probe.fee_msat,
                        "hops": probe.hop_count,
                        "time_ms": probe.time_ms
                    }

        # Probe 2: External routing comparison
        external_nodes = self.get_random_external_nodes(10)
        for src in nodes:
            for ext in external_nodes:
                probe = self.probe_route(src, ext, amount=10000)
                results["fee_consistency"][src] = self.compare_fees(
                    internal=results["internal_routing"],
                    external=probe
                )

        # Analysis
        return self.analyze_cluster_probes(results)

    def analyze_cluster_probes(self, results: dict) -> ClusterAnalysis:
        """Analyze probe results for hive indicators."""

        indicators = {}

        # Indicator 1: Internal fees significantly lower than external
        internal_fees = [r["fee_paid"] for r in results["internal_routing"].values() if r["success"]]
        external_fees = [r["fee_paid"] for r in results["fee_consistency"].values() if r.get("success")]

        if internal_fees and external_fees:
            fee_ratio = np.mean(internal_fees) / np.mean(external_fees)
            indicators["fee_ratio"] = fee_ratio
            indicators["fee_suspicious"] = fee_ratio < 0.3  # Internal 70%+ cheaper

        # Indicator 2: Unusually high internal routing success
        internal_success = sum(1 for r in results["internal_routing"].values() if r["success"])
        internal_total = len(results["internal_routing"])

        indicators["internal_success_rate"] = internal_success / internal_total if internal_total else 0
        indicators["success_suspicious"] = indicators["internal_success_rate"] > 0.95

        # Indicator 3: Correlated liquidity (simultaneous balance changes)
        # This requires multiple probes over time
        indicators["liquidity_correlation"] = self.calculate_liquidity_correlation(results)

        # Indicator 4: Response timing patterns
        # Hive nodes may have similar latency patterns
        indicators["timing_correlation"] = self.calculate_timing_correlation(results)

        # Overall assessment
        suspicion_score = sum([
            0.4 if indicators.get("fee_suspicious") else 0,
            0.3 if indicators.get("success_suspicious") else 0,
            0.2 if indicators.get("liquidity_correlation", 0) > 0.7 else 0,
            0.1 if indicators.get("timing_correlation", 0) > 0.8 else 0,
        ])

        return ClusterAnalysis(
            nodes=nodes,
            indicators=indicators,
            suspicion_score=suspicion_score,
            likely_hive=suspicion_score > 0.6,
            confidence=min(suspicion_score * 1.5, 1.0)
        )
```

### 5.3 Liquidity Correlation Analysis

Hidden hives often have correlated liquidity (rebalancing together):

```python
def detect_liquidity_correlation(self, nodes: List[str], days: int = 7) -> float:
    """Detect correlated liquidity changes through repeated probing."""

    # Probe each node periodically
    probe_schedule = []
    for _ in range(days * 24):  # Hourly probes
        for node in nodes:
            probe_schedule.append((time.time() + random.uniform(0, 3600), node))

    # Execute probes and record available liquidity
    liquidity_series = {node: [] for node in nodes}

    for probe_time, node in sorted(probe_schedule):
        time.sleep(max(0, probe_time - time.time()))

        # Probe max sendable to node
        max_sendable = self.probe_max_sendable(node)
        liquidity_series[node].append((probe_time, max_sendable))

    # Calculate pairwise correlation
    correlations = []
    for n1, n2 in combinations(nodes, 2):
        series1 = [v for _, v in liquidity_series[n1]]
        series2 = [v for _, v in liquidity_series[n2]]

        corr = np.corrcoef(series1, series2)[0, 1]
        correlations.append(corr)

    # High average correlation suggests coordinated liquidity management
    return np.mean(correlations) if correlations else 0.0
```

### 5.4 Fee Response Correlation

Probe how nodes respond to fee changes:

```python
def detect_fee_correlation(self, nodes: List[str]) -> float:
    """Detect if nodes change fees in correlation."""

    # Monitor fee changes over time
    fee_history = {node: [] for node in nodes}

    # Record initial fees
    for node in nodes:
        channels = self.get_node_channels(node)
        for chan in channels:
            fee_history[node].append({
                "time": time.time(),
                "channel": chan.scid,
                "fee_ppm": chan.fee_ppm
            })

    # Monitor for changes over 7 days
    # (In practice, subscribe to gossip updates)

    # Analyze: do fee changes cluster in time?
    all_changes = []
    for node, history in fee_history.items():
        for i in range(1, len(history)):
            if history[i]["fee_ppm"] != history[i-1]["fee_ppm"]:
                all_changes.append({
                    "node": node,
                    "time": history[i]["time"],
                    "change": history[i]["fee_ppm"] - history[i-1]["fee_ppm"]
                })

    # Calculate temporal clustering
    return self.calculate_temporal_clustering(all_changes)
```

### 5.5 Active Unmasking

If we suspect a hidden hive, we can try to unmask it:

```python
def attempt_unmask(self, suspected_nodes: List[str]) -> UnmaskResult:
    """Attempt to unmask a suspected hidden hive."""

    unmask_techniques = [
        self.probe_internal_routing,      # See if they have preferential internal routing
        self.stress_test_liquidity,       # See if one node's stress affects others
        self.fee_pressure_test,           # Raise fees and see if they coordinate response
        self.direct_query_all,            # Just ask each node directly
    ]

    evidence = []

    for technique in unmask_techniques:
        result = technique(suspected_nodes)
        if result.reveals_coordination:
            evidence.append(result)

    if len(evidence) >= 2:
        return UnmaskResult(
            unmasked=True,
            confidence=min(0.5 + len(evidence) * 0.15, 0.95),
            evidence=evidence,
            recommended_action="classify_as_hidden_hive"
        )

    return UnmaskResult(
        unmasked=False,
        confidence=0.3,
        evidence=evidence,
        recommended_action="continue_monitoring"
    )
```

---

## 6. Reputation-Gated Messaging

### 6.1 Core Principle

**No reputation = No communication (or very expensive communication)**

```python
class ReputationGate:
    """Gate all inter-hive communication by reputation."""

    # Fee multipliers by reputation tier
    FEE_MULTIPLIERS = {
        "unknown": 10.0,      # 10x fees for unknown senders
        "observed": 5.0,      # 5x for observed
        "neutral": 2.0,       # 2x for neutral
        "cooperative": 1.0,   # Standard for cooperative
        "federated": 0.5,     # Discount for federated
        "hostile": float('inf'),  # Blocked
        "parasitic": float('inf'),  # Blocked
    }

    def calculate_message_fee(
        self,
        sender: str,
        msg_type: str
    ) -> int:
        """Calculate fee for sender to send message type."""

        base_fee = MESSAGE_FEES[msg_type]

        # Get sender's hive and reputation
        sender_hive = self.get_hive_for_node(sender)

        if sender_hive is None:
            # Unknown independent node
            multiplier = self.FEE_MULTIPLIERS["unknown"]
        else:
            classification = sender_hive.classification
            multiplier = self.FEE_MULTIPLIERS.get(classification, 10.0)

        if multiplier == float('inf'):
            return -1  # Blocked, no fee will work

        return int(base_fee * multiplier)

    def should_accept_message(
        self,
        payment: Payment,
        msg: HiveMessage
    ) -> Tuple[bool, str]:
        """Determine if message should be accepted."""

        required_fee = self.calculate_message_fee(
            sender=payment.sender,
            msg_type=msg.msg_type
        )

        if required_fee == -1:
            return False, "sender_blocked"

        if payment.amount_msat < required_fee * 1000:
            return False, f"insufficient_fee_for_reputation"

        return True, "accepted"
```

### 6.2 Reputation Earning Through Payments

Reputation is earned through successful payment interactions with **diverse, independent counterparties**.

**Anti-Gaming Measures:**
- Circular payments detected and excluded
- Counterparty diversity required
- Only third-party routed payments count toward volume
- Self-referential paths discounted

```python
class PaymentReputation:
    """Build reputation through payment history with anti-gaming."""

    # Minimum counterparties for reputation
    MIN_COUNTERPARTIES = 10
    # Maximum volume credit from single counterparty
    MAX_SINGLE_COUNTERPARTY_PCT = 0.20  # 20%

    def record_payment_interaction(
        self,
        counterparty: str,
        direction: str,  # "sent" or "received"
        amount_sats: int,
        success: bool,
        context: str,  # "routing", "direct", "hive_message"
        route_hops: int,  # Number of hops in route
        route_nodes: List[str]  # Nodes in route (for circular detection)
    ):
        """Record a payment interaction for reputation."""

        # Detect circular payment (sender in route)
        is_circular = self.detect_circular_payment(counterparty, route_nodes)

        self.db.execute("""
            INSERT INTO payment_interactions
            (counterparty, direction, amount_sats, success, context,
             route_hops, is_circular, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (counterparty, direction, amount_sats, success, context,
              route_hops, is_circular, time.time()))

        # Update reputation score
        self.update_reputation(counterparty)

    def detect_circular_payment(
        self,
        counterparty: str,
        route_nodes: List[str]
    ) -> bool:
        """Detect if payment is circular (wash trading)."""

        # Check if counterparty appears in route (excluding endpoints)
        if counterparty in route_nodes[1:-1]:
            return True

        # Check if we've seen rapid back-and-forth with this counterparty
        recent = self.get_recent_interactions(counterparty, minutes=60)
        if len(recent) > 10:
            # More than 10 interactions in an hour = suspicious
            return True

        # Check if counterparty is in our "suspected circular" list
        if self.is_suspected_circular_partner(counterparty):
            return True

        return False

    def calculate_counterparty_diversity(
        self,
        interactions: List[Interaction]
    ) -> float:
        """Calculate diversity of counterparties (0-1 scale)."""

        if not interactions:
            return 0.0

        # Count unique counterparties
        counterparties = set(i.counterparty for i in interactions)
        unique_count = len(counterparties)

        # Calculate volume concentration (Herfindahl index)
        total_volume = sum(i.amount_sats for i in interactions)
        if total_volume == 0:
            return 0.0

        volume_by_counterparty = {}
        for i in interactions:
            volume_by_counterparty[i.counterparty] = \
                volume_by_counterparty.get(i.counterparty, 0) + i.amount_sats

        # Herfindahl index: sum of squared market shares
        hhi = sum(
            (vol / total_volume) ** 2
            for vol in volume_by_counterparty.values()
        )

        # Convert to diversity score (1 - HHI, normalized)
        # HHI of 1.0 = all volume with one counterparty = 0 diversity
        # HHI of 1/N = equal distribution = high diversity
        diversity_score = 1.0 - hhi

        # Also require minimum unique counterparties
        counterparty_score = min(unique_count / self.MIN_COUNTERPARTIES, 1.0)

        return (diversity_score * 0.6 + counterparty_score * 0.4)

    def calculate_payment_reputation(self, node: str) -> PaymentReputationScore:
        """Calculate reputation from payment history with anti-gaming."""

        interactions = self.get_interactions(node, days=90)

        # Exclude circular payments
        valid_interactions = [i for i in interactions if not i.is_circular]

        if len(valid_interactions) < 10:
            return PaymentReputationScore(
                score=0.0,
                confidence=0.1,
                reason="insufficient_valid_history"
            )

        # Check counterparty diversity
        diversity = self.calculate_counterparty_diversity(valid_interactions)

        if diversity < 0.3:
            return PaymentReputationScore(
                score=0.0,
                confidence=0.2,
                reason="insufficient_counterparty_diversity"
            )

        # Cap volume credit per counterparty
        volume_by_cp = {}
        for i in valid_interactions:
            volume_by_cp[i.counterparty] = \
                volume_by_cp.get(i.counterparty, 0) + i.amount_sats

        total_raw_volume = sum(volume_by_cp.values())
        max_per_cp = total_raw_volume * self.MAX_SINGLE_COUNTERPARTY_PCT

        # Capped volume (no single counterparty > 20% of total)
        capped_volume = sum(min(vol, max_per_cp) for vol in volume_by_cp.values())

        # Only count multi-hop payments toward routing reputation
        routed_interactions = [i for i in valid_interactions if i.route_hops >= 2]
        routing_volume = sum(i.amount_sats for i in routed_interactions)

        # Metrics
        success_rate = sum(1 for i in valid_interactions if i.success) / len(valid_interactions)

        # Directional balance
        sent = sum(i.amount_sats for i in valid_interactions if i.direction == "sent")
        received = sum(i.amount_sats for i in valid_interactions if i.direction == "received")
        balance_ratio = min(sent, received) / max(sent, received, 1)

        # Consistency
        consistency = self.calculate_interaction_consistency(valid_interactions)

        # Calculate score with diversity as major factor
        score = (
            0.25 * success_rate +
            0.20 * min(capped_volume / 10_000_000, 1.0) +
            0.15 * balance_ratio +
            0.15 * consistency +
            0.25 * diversity  # Diversity is now 25% of score
        )

        confidence = min(len(valid_interactions) / 100, 1.0) * diversity

        return PaymentReputationScore(
            score=score,
            confidence=confidence,
            total_volume=capped_volume,
            routing_volume=routing_volume,
            success_rate=success_rate,
            balance_ratio=balance_ratio,
            diversity_score=diversity,
            interaction_count=len(valid_interactions),
            excluded_circular=len(interactions) - len(valid_interactions)
        )
```

### 6.3 Reputation Verification Challenges

Periodically challenge counterparties to verify reputation:

```python
class ReputationChallenge:
    """Challenge counterparties to verify their reputation."""

    def issue_challenge(self, target: str, stake: int = 10000) -> Challenge:
        """Issue a reputation verification challenge."""

        # Create a challenge that requires them to:
        # 1. Receive a payment from us
        # 2. Send a payment back within time limit
        # 3. Route a payment for us

        challenge = Challenge(
            challenge_id=generate_id(),
            target=target,
            stake=stake,
            created_at=time.time(),
            expires_at=time.time() + 3600,  # 1 hour
            tasks=[
                {"type": "receive", "amount": 1000, "status": "pending"},
                {"type": "send_back", "amount": 900, "status": "pending"},
                {"type": "route", "amount": 5000, "status": "pending"},
            ]
        )

        # Send initial challenge payment
        self.send_challenge_payment(target, challenge)

        return challenge

    def verify_challenge_completion(self, challenge: Challenge) -> ChallengeResult:
        """Verify if challenge was completed."""

        completed_tasks = sum(1 for t in challenge.tasks if t["status"] == "completed")
        total_tasks = len(challenge.tasks)

        if completed_tasks == total_tasks:
            # Full completion - reputation boost
            return ChallengeResult(
                passed=True,
                reputation_delta=0.1,
                stake_returned=True
            )
        elif completed_tasks > 0:
            # Partial completion
            return ChallengeResult(
                passed=False,
                reputation_delta=-0.05,
                stake_returned=True,
                note="partial_completion"
            )
        else:
            # No completion - forfeit stake
            return ChallengeResult(
                passed=False,
                reputation_delta=-0.2,
                stake_returned=False,
                note="challenge_failed"
            )
```

---

## 7. Continuous Verification

### 7.1 Trust Decay Without Verification

Even federated hives must continuously prove trustworthiness:

```python
class ContinuousVerification:
    """Continuously verify all hive relationships."""

    # Required verification frequency by relationship level
    VERIFICATION_INTERVALS = {
        "unknown": 3600,        # Every hour
        "observed": 14400,      # Every 4 hours
        "neutral": 86400,       # Daily
        "cooperative": 259200,  # Every 3 days
        "federated": 604800,    # Weekly
    }

    def run_verification_loop(self):
        """Continuous verification loop."""

        while not self.shutdown_event.is_set():
            for hive in self.get_all_known_hives():
                interval = self.VERIFICATION_INTERVALS.get(
                    hive.classification, 3600
                )

                if time.time() - hive.last_verified > interval:
                    self.verify_hive(hive)

            self.shutdown_event.wait(60)  # Check every minute

    def verify_hive(self, hive: DetectedHive) -> VerificationResult:
        """Verify a hive is still trustworthy."""

        verifications = []

        # 1. Verify members are still reachable via payment
        for member in hive.members[:5]:  # Sample 5 members
            probe = self.send_verification_payment(member, amount=100)
            verifications.append({
                "type": "reachability",
                "node": member,
                "passed": probe.success
            })

        # 2. Verify behavior hasn't changed
        recent_behavior = self.analyze_recent_behavior(hive.hive_id, days=7)
        verifications.append({
            "type": "behavior",
            "passed": recent_behavior.consistent_with_classification
        })

        # 3. Verify economic relationship is balanced
        economic = self.analyze_economic_relationship(hive.hive_id)
        verifications.append({
            "type": "economic",
            "passed": economic.is_balanced
        })

        # 4. For federated: verify they're honoring agreements
        if hive.classification == "federated":
            federation = self.get_federation(hive.hive_id)
            compliance = self.verify_federation_compliance(federation)
            verifications.append({
                "type": "federation_compliance",
                "passed": compliance.is_compliant
            })

        # Calculate result
        passed_count = sum(1 for v in verifications if v["passed"])
        total_count = len(verifications)

        if passed_count == total_count:
            status = "verified"
            action = "maintain_classification"
        elif passed_count >= total_count * 0.7:
            status = "partial"
            action = "increase_monitoring"
        else:
            status = "failed"
            action = "downgrade_classification"

        # Update verification timestamp
        self.update_hive_verification(hive.hive_id, time.time(), status)

        return VerificationResult(
            hive_id=hive.hive_id,
            verifications=verifications,
            status=status,
            action=action
        )
```

### 7.2 Federation Heartbeat Payments

Federated hives exchange regular heartbeat payments:

```python
class FederationHeartbeat:
    """Exchange heartbeat payments with federated hives."""

    HEARTBEAT_AMOUNT = 1000  # sats
    HEARTBEAT_INTERVAL = 86400  # Daily

    def send_heartbeat(self, federation_id: str) -> HeartbeatResult:
        """Send heartbeat payment to federated hive."""

        federation = self.get_federation(federation_id)
        their_admin = federation.their_admin_node

        # Include current status in heartbeat
        heartbeat_payload = {
            "heartbeat_id": generate_id(),
            "our_status": {
                "member_count": self.get_member_count(),
                "health": self.get_health_summary(),
                "active_alerts": self.get_active_alert_count()
            },
            "federation_status": {
                "our_compliance": True,
                "issues_detected": [],
                "next_review": federation.next_review_timestamp
            }
        }

        # Send heartbeat as payment with TLV
        result = self.send_hive_message(
            target=their_admin,
            msg_type="federation_heartbeat",
            payload=heartbeat_payload
        )

        if result.success:
            self.record_heartbeat_sent(federation_id)
        else:
            self.record_heartbeat_failure(federation_id, result.error)

            # Multiple failures = verification concern
            failures = self.count_recent_heartbeat_failures(federation_id)
            if failures >= 3:
                self.flag_federation_for_review(federation_id)

        return result

    def handle_heartbeat(self, msg: HiveMessage) -> HeartbeatResponse:
        """Handle incoming heartbeat from federated hive."""

        federation = self.get_federation_by_sender(msg.sender)

        if federation is None:
            return HeartbeatResponse(
                accepted=False,
                reason="not_federated"
            )

        # Verify heartbeat payment was sufficient
        if msg.payment_amount < self.HEARTBEAT_AMOUNT:
            return HeartbeatResponse(
                accepted=False,
                reason="insufficient_heartbeat_payment"
            )

        # Record received heartbeat
        self.record_heartbeat_received(federation.federation_id, msg.payload)

        # Send response heartbeat
        self.schedule_heartbeat_response(federation.federation_id)

        return HeartbeatResponse(
            accepted=True,
            our_status=self.get_status_summary()
        )
```

### 7.3 Verification Failure Consequences

```python
def handle_verification_failure(
    self,
    hive_id: str,
    failure_type: str,
    severity: str
) -> List[str]:
    """Handle verification failure."""

    actions = []
    hive = self.get_hive(hive_id)

    if severity == "critical":
        # Immediate downgrade
        if hive.classification == "federated":
            self.suspend_federation(hive_id)
            self.reclassify_hive(hive_id, "observed")
            actions.append("federation_suspended")
            actions.append("downgraded_to_observed")
        else:
            new_class = self.downgrade_classification(hive.classification)
            self.reclassify_hive(hive_id, new_class)
            actions.append(f"downgraded_to_{new_class}")

    elif severity == "warning":
        # Increase monitoring, potential downgrade
        self.increase_monitoring(hive_id)
        self.record_warning(hive_id, failure_type)
        actions.append("increased_monitoring")

        # Check for pattern of warnings
        warnings = self.count_recent_warnings(hive_id, days=30)
        if warnings >= 3:
            self.schedule_classification_review(hive_id)
            actions.append("review_scheduled")

    # Notify federated hives of verification failure
    if hive.classification in ["cooperative", "federated"]:
        self.notify_federates_of_issue(hive_id, failure_type, severity)
        actions.append("federates_notified")

    return actions
```

---

## 8. Economic Security Model

### 8.1 Attack Cost Analysis

| Attack | Without Payment Protocol | With Payment Protocol |
|--------|-------------------------|----------------------|
| Fake hive creation | Free | Cost of real channels + liquidity |
| False hive membership claim | Free | Must receive voucher payment from admin |
| Federation request spam | Free | 10,000 sats + 100,000 stake per request |
| Hidden hive operation | Free | Detectable via payment probing |
| Reputation fraud | Easy | Requires sustained payment history |
| Intelligence gathering | Free | Must pay for every query |
| Long con infiltration | Time only | Time + significant locked capital |

### 8.2 Stake Requirements

```python
STAKE_SCHEDULE = {
    # Relationship establishment
    "hive_introduction": 10_000,           # 10k sats (Lightning)
    "federation_request_level_1": 100_000,  # 100k sats (Lightning or on-chain)
    "federation_request_level_2": 1_000_000,  # 1M sats (on-chain required)
    "federation_request_level_3": 10_000_000,  # 10M sats (on-chain required)
    "federation_request_level_4": 50_000_000,  # 50M sats (on-chain required)

    # Message stakes (for high-trust messages)
    "defense_alert": 50_000,               # Must have skin in game for alerts
    "intel_share_high_value": 100_000,     # Stake behind valuable intel

    # Verification stakes
    "reputation_challenge": 10_000,         # Challenge stake
    "membership_voucher_request": 5_000,    # Verify membership
}

# Stakes >= 1M sats MUST use on-chain Bitcoin escrow
ON_CHAIN_THRESHOLD = 1_000_000

STAKE_VESTING = {
    # How long until stake is returned (in days)
    "federation_level_1": 180,   # 6 months
    "federation_level_2": 365,   # 1 year
    "federation_level_3": 730,   # 2 years
    "federation_level_4": 1095,  # 3 years
}

STAKE_FORFEIT_TRIGGERS = [
    "hostile_action_detected",
    "federation_terms_violation",
    "false_intel_provided",
    "false_membership_claim",
    "false_defense_alert",
    "verification_fraud",
]
```

### 8.2.1 Bitcoin Timelock Escrow for High-Value Stakes

**Problem with Lightning-Based Stakes:**
- Lightning payments are immediate and irreversible
- 2-of-2 multisig can result in "stake hostage" where one party refuses to cooperate
- No on-chain enforcement of vesting periods
- Counterparty can disappear with stake

**Solution**: Use Bitcoin Script with timelocks for high-value federation stakes.

#### Escrow Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BITCOIN TIMELOCK ESCROW                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Staker (Alice)              Recipient (Bob)                        │
│       │                           │                                 │
│       │ 1. Create escrow tx      │                                 │
│       │    with timelock script  │                                 │
│       │ ─────────────────────►   │                                 │
│       │                          │                                 │
│       │        On-chain UTXO     │                                 │
│       │    ┌─────────────────┐   │                                 │
│       │    │ Script Options: │   │                                 │
│       │    │ A) Bob + Alice  │   │ (cooperative release)           │
│       │    │ B) Bob + proof  │   │ (unilateral claim with evidence)│
│       │    │ C) Alice after  │   │ (timeout refund)                │
│       │    │    timelock     │   │                                 │
│       │    └─────────────────┘   │                                 │
│       │                          │                                 │
└─────────────────────────────────────────────────────────────────────┘
```

#### Bitcoin Script for Escrow

```python
class BitcoinTimelockEscrow:
    """On-chain escrow using Bitcoin Script timelocks."""

    # Script template:
    # OP_IF
    #     # Path A: Cooperative release (2-of-2)
    #     <alice_pubkey> OP_CHECKSIGVERIFY
    #     <bob_pubkey> OP_CHECKSIG
    # OP_ELSE
    #     OP_IF
    #         # Path B: Bob claims with forfeit proof
    #         OP_SHA256 <forfeit_proof_hash> OP_EQUALVERIFY
    #         <bob_pubkey> OP_CHECKSIG
    #     OP_ELSE
    #         # Path C: Alice refund after timelock
    #         <timelock_blocks> OP_CHECKSEQUENCEVERIFY OP_DROP
    #         <alice_pubkey> OP_CHECKSIG
    #     OP_ENDIF
    # OP_ENDIF

    def create_escrow_script(
        self,
        staker_pubkey: bytes,
        recipient_pubkey: bytes,
        forfeit_proof_hash: bytes,
        timelock_blocks: int
    ) -> bytes:
        """Create escrow script with three spending paths."""

        script = CScript([
            # Path A: Cooperative 2-of-2
            OP_IF,
                staker_pubkey, OP_CHECKSIGVERIFY,
                recipient_pubkey, OP_CHECKSIG,
            OP_ELSE,
                OP_IF,
                    # Path B: Recipient claims with proof of violation
                    OP_SHA256, forfeit_proof_hash, OP_EQUALVERIFY,
                    recipient_pubkey, OP_CHECKSIG,
                OP_ELSE,
                    # Path C: Staker refund after timelock
                    timelock_blocks, OP_CHECKSEQUENCEVERIFY, OP_DROP,
                    staker_pubkey, OP_CHECKSIG,
                OP_ENDIF,
            OP_ENDIF
        ])

        return script

    def create_escrow_address(
        self,
        staker_pubkey: bytes,
        recipient_pubkey: bytes,
        forfeit_conditions: List[str],
        vesting_days: int
    ) -> EscrowAddress:
        """Create P2WSH escrow address."""

        # Calculate timelock in blocks (~144 blocks/day)
        timelock_blocks = vesting_days * 144

        # Create forfeit proof hash (hash of known forfeit conditions)
        forfeit_proof_hash = self.create_forfeit_proof_hash(forfeit_conditions)

        # Build script
        script = self.create_escrow_script(
            staker_pubkey=staker_pubkey,
            recipient_pubkey=recipient_pubkey,
            forfeit_proof_hash=forfeit_proof_hash,
            timelock_blocks=timelock_blocks
        )

        # Create P2WSH address
        script_hash = sha256(script)
        address = bech32_encode("bc", 0, script_hash)

        return EscrowAddress(
            address=address,
            script=script.hex(),
            staker_pubkey=staker_pubkey.hex(),
            recipient_pubkey=recipient_pubkey.hex(),
            timelock_blocks=timelock_blocks,
            forfeit_proof_hash=forfeit_proof_hash.hex()
        )
```

#### Forfeit Proof System

```python
class ForfeitProofSystem:
    """Generate and verify proofs of stake forfeit conditions."""

    # Forfeit conditions must be cryptographically provable
    PROVABLE_FORFEIT_CONDITIONS = {
        "hostile_action_detected": {
            "proof_type": "signed_evidence",
            "required_signatures": 1,  # Any hive admin
            "evidence_schema": {
                "action_type": str,
                "timestamp": int,
                "evidence_data": str,
                "witness_signatures": List[str]
            }
        },
        "federation_terms_violation": {
            "proof_type": "signed_evidence",
            "required_signatures": 2,  # Multiple witnesses
            "evidence_schema": {
                "violation_type": str,
                "federation_id": str,
                "term_violated": str,
                "evidence_data": str,
                "witness_signatures": List[str]
            }
        },
        "false_intel_provided": {
            "proof_type": "contradiction_proof",
            "required": ["original_intel", "contradicting_evidence"],
            "evidence_schema": {
                "intel_hash": str,
                "intel_timestamp": int,
                "contradicting_data": str,
                "contradiction_timestamp": int
            }
        },
        "verification_fraud": {
            "proof_type": "cryptographic_proof",
            "required": ["claimed_data", "actual_data", "signature"],
            "evidence_schema": {
                "claimed_value": str,
                "actual_value": str,
                "signed_claim": str,  # Their signature on false claim
            }
        }
    }

    def create_forfeit_proof_hash(
        self,
        forfeit_conditions: List[str]
    ) -> bytes:
        """Create hash commitment of acceptable forfeit proofs."""

        # Hash each condition type
        condition_hashes = []
        for condition in forfeit_conditions:
            if condition not in self.PROVABLE_FORFEIT_CONDITIONS:
                raise ValueError(f"Non-provable condition: {condition}")

            # Create deterministic hash of condition schema
            schema = self.PROVABLE_FORFEIT_CONDITIONS[condition]
            condition_hash = sha256(
                json.dumps(schema, sort_keys=True).encode()
            )
            condition_hashes.append(condition_hash)

        # Merkle root of condition hashes
        return self.merkle_root(condition_hashes)

    def create_forfeit_proof(
        self,
        condition: str,
        evidence: dict
    ) -> ForfeitProof:
        """Create a proof that can unlock escrow via Path B."""

        config = self.PROVABLE_FORFEIT_CONDITIONS[condition]

        # Validate evidence matches schema
        self.validate_evidence(evidence, config["evidence_schema"])

        # Collect required signatures
        if config["proof_type"] == "signed_evidence":
            if len(evidence.get("witness_signatures", [])) < config["required_signatures"]:
                raise ValueError("Insufficient witness signatures")

        # Create proof that matches forfeit_proof_hash
        proof_data = {
            "condition": condition,
            "evidence": evidence,
            "timestamp": int(time.time())
        }

        # The preimage that hashes to forfeit_proof_hash
        proof_preimage = self.compute_proof_preimage(condition, proof_data)

        return ForfeitProof(
            condition=condition,
            evidence=evidence,
            preimage=proof_preimage
        )

    def verify_forfeit_proof(
        self,
        proof: ForfeitProof,
        expected_hash: bytes
    ) -> bool:
        """Verify a forfeit proof can unlock the escrow."""

        # Hash the preimage
        actual_hash = sha256(proof.preimage)

        if actual_hash != expected_hash:
            return False

        # Verify evidence is valid
        config = self.PROVABLE_FORFEIT_CONDITIONS[proof.condition]
        return self.validate_evidence(proof.evidence, config["evidence_schema"])
```

#### Escrow Lifecycle

```python
class EscrowLifecycle:
    """Manage the lifecycle of Bitcoin timelock escrows."""

    def initiate_federation_escrow(
        self,
        their_hive_id: str,
        federation_level: int,
        our_pubkey: bytes
    ) -> EscrowInitiation:
        """Initiate escrow for federation stake."""

        stake_amount = STAKE_SCHEDULE[f"federation_request_level_{federation_level}"]
        vesting_days = STAKE_VESTING[f"federation_level_{federation_level}"]

        # Get their pubkey from their admin node
        their_pubkey = self.request_escrow_pubkey(their_hive_id)

        # Define forfeit conditions for this level
        forfeit_conditions = [
            "hostile_action_detected",
            "federation_terms_violation",
            "verification_fraud"
        ]

        # Create escrow address
        escrow = self.escrow_system.create_escrow_address(
            staker_pubkey=our_pubkey,
            recipient_pubkey=their_pubkey,
            forfeit_conditions=forfeit_conditions,
            vesting_days=vesting_days
        )

        # Create and broadcast funding transaction
        funding_tx = self.create_funding_tx(
            escrow_address=escrow.address,
            amount_sats=stake_amount
        )

        # Record escrow
        self.db.execute("""
            INSERT INTO bitcoin_escrows
            (escrow_id, counterparty_hive, federation_level, amount_sats,
             escrow_address, script_hex, our_pubkey, their_pubkey,
             timelock_blocks, forfeit_proof_hash, funding_txid,
             status, created_at, vests_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'funded', ?, ?)
        """, (
            generate_id(),
            their_hive_id,
            federation_level,
            stake_amount,
            escrow.address,
            escrow.script,
            our_pubkey.hex(),
            their_pubkey.hex(),
            escrow.timelock_blocks,
            escrow.forfeit_proof_hash,
            funding_tx.txid,
            int(time.time()),
            int(time.time()) + (vesting_days * 86400)
        ))

        return EscrowInitiation(
            escrow_id=escrow.address,
            funding_txid=funding_tx.txid,
            amount_sats=stake_amount,
            vests_at=int(time.time()) + (vesting_days * 86400),
            escrow_details=escrow
        )

    def release_escrow_cooperative(
        self,
        escrow_id: str,
        their_signature: bytes
    ) -> str:
        """Release escrow via Path A (cooperative 2-of-2)."""

        escrow = self.get_escrow(escrow_id)

        # Create spending transaction to staker (us)
        spend_tx = self.create_cooperative_release_tx(
            escrow=escrow,
            their_signature=their_signature
        )

        # Sign with our key
        our_signature = self.sign_tx(spend_tx, escrow)

        # Broadcast
        txid = self.broadcast_tx(spend_tx)

        # Update status
        self.update_escrow_status(escrow_id, "released_cooperative", txid)

        return txid

    def claim_escrow_with_proof(
        self,
        escrow_id: str,
        forfeit_proof: ForfeitProof
    ) -> str:
        """Claim escrow via Path B (forfeit proof)."""

        escrow = self.get_escrow(escrow_id)

        # Verify the forfeit proof
        if not self.forfeit_system.verify_forfeit_proof(
            proof=forfeit_proof,
            expected_hash=bytes.fromhex(escrow.forfeit_proof_hash)
        ):
            raise ValueError("Invalid forfeit proof")

        # Create spending transaction with forfeit proof
        spend_tx = self.create_forfeit_claim_tx(
            escrow=escrow,
            forfeit_proof=forfeit_proof
        )

        # Broadcast
        txid = self.broadcast_tx(spend_tx)

        # Update status
        self.update_escrow_status(escrow_id, "forfeited", txid)

        return txid

    def reclaim_escrow_after_timeout(
        self,
        escrow_id: str
    ) -> str:
        """Reclaim escrow via Path C (timelock expiry)."""

        escrow = self.get_escrow(escrow_id)

        # Check timelock has expired
        current_height = self.get_block_height()
        funding_height = self.get_tx_height(escrow.funding_txid)

        if current_height < funding_height + escrow.timelock_blocks:
            blocks_remaining = (funding_height + escrow.timelock_blocks) - current_height
            raise ValueError(f"Timelock not expired: {blocks_remaining} blocks remaining")

        # Create spending transaction (no signature needed from counterparty)
        spend_tx = self.create_timeout_refund_tx(escrow=escrow)

        # Broadcast
        txid = self.broadcast_tx(spend_tx)

        # Update status
        self.update_escrow_status(escrow_id, "refunded_timeout", txid)

        return txid
```

#### Database Schema for Escrows

```sql
-- Bitcoin escrow tracking
CREATE TABLE bitcoin_escrows (
    escrow_id TEXT PRIMARY KEY,
    counterparty_hive TEXT NOT NULL,
    federation_level INTEGER,
    amount_sats INTEGER NOT NULL,
    escrow_address TEXT NOT NULL,
    script_hex TEXT NOT NULL,
    our_pubkey TEXT NOT NULL,
    their_pubkey TEXT NOT NULL,
    timelock_blocks INTEGER NOT NULL,
    forfeit_proof_hash TEXT NOT NULL,
    funding_txid TEXT,
    spending_txid TEXT,
    status TEXT DEFAULT 'pending',  -- pending, funded, released_cooperative, forfeited, refunded_timeout
    forfeit_reason TEXT,
    created_at INTEGER NOT NULL,
    vests_at INTEGER NOT NULL,
    resolved_at INTEGER
);

CREATE INDEX idx_escrows_counterparty ON bitcoin_escrows(counterparty_hive);
CREATE INDEX idx_escrows_status ON bitcoin_escrows(status);
CREATE INDEX idx_escrows_vests ON bitcoin_escrows(vests_at);
```

#### Security Properties

| Property | How Achieved |
|----------|--------------|
| No stake hostage | Timelock Path C: staker can always reclaim after timeout |
| Provable forfeit | Path B requires cryptographic proof of violation |
| No trusted third party | Pure Bitcoin Script, no arbiters needed |
| Cooperative efficiency | Path A allows instant release with both signatures |
| Transparent vesting | Timelock visible on-chain |
| Dispute resolution | Evidence-based forfeit proofs, verifiable by anyone |

#### When to Use Each Stake Type

| Stake Amount | Method | Reason |
|--------------|--------|--------|
| < 100k sats | Lightning payment | Low cost, fast, acceptable risk |
| 100k - 1M sats | Lightning or on-chain | Optionally use on-chain for more security |
| > 1M sats | On-chain required | Stake hostage risk too high for Lightning |
| Federation L3+ | On-chain required | Multi-year commitment needs on-chain enforcement |

### 8.3 Payment Flow Tracking

Track all payment flows for economic analysis:

```sql
CREATE TABLE hive_payment_flows (
    id INTEGER PRIMARY KEY,
    counterparty_node TEXT NOT NULL,
    counterparty_hive TEXT,
    direction TEXT NOT NULL,          -- 'inbound', 'outbound'
    amount_sats INTEGER NOT NULL,
    fee_paid_sats INTEGER,
    purpose TEXT NOT NULL,            -- 'routing', 'message', 'stake', 'heartbeat'
    success BOOLEAN NOT NULL,
    timestamp INTEGER NOT NULL,

    -- For routing payments
    was_routing BOOLEAN DEFAULT FALSE,
    route_source TEXT,
    route_destination TEXT,

    -- For hive messages
    message_type TEXT,
    message_id TEXT
);

CREATE INDEX idx_payment_flows_counterparty ON hive_payment_flows(counterparty_node, timestamp);
CREATE INDEX idx_payment_flows_hive ON hive_payment_flows(counterparty_hive, timestamp);
```

### 8.4 Economic Anomaly Detection

```python
class EconomicAnomalyDetector:
    """Detect economic anomalies in hive relationships."""

    def detect_anomalies(self, hive_id: str) -> List[EconomicAnomaly]:
        """Detect economic anomalies with a hive."""

        anomalies = []
        flows = self.get_payment_flows(hive_id, days=30)

        # Anomaly 1: Sudden volume spike (potential attack setup)
        recent_volume = sum(f.amount_sats for f in flows if f.timestamp > time.time() - 86400)
        historical_avg = self.get_historical_daily_volume(hive_id)

        if recent_volume > historical_avg * 5:
            anomalies.append(EconomicAnomaly(
                type="volume_spike",
                severity="warning",
                details=f"24h volume {recent_volume} vs avg {historical_avg}"
            ))

        # Anomaly 2: Asymmetric flow (potential extraction)
        inbound = sum(f.amount_sats for f in flows if f.direction == "inbound")
        outbound = sum(f.amount_sats for f in flows if f.direction == "outbound")

        if outbound > 0 and inbound / outbound < 0.2:
            anomalies.append(EconomicAnomaly(
                type="asymmetric_extraction",
                severity="critical",
                details=f"Inbound/outbound ratio: {inbound/outbound:.2f}"
            ))

        # Anomaly 3: Message payment without routing relationship
        message_payments = [f for f in flows if f.purpose == "message"]
        routing_payments = [f for f in flows if f.purpose == "routing"]

        if len(message_payments) > 10 and len(routing_payments) == 0:
            anomalies.append(EconomicAnomaly(
                type="message_only_relationship",
                severity="warning",
                details="Many messages but no routing - possible reconnaissance"
            ))

        # Anomaly 4: Stake without follow-through
        stakes = [f for f in flows if f.purpose == "stake"]
        introductions = self.get_introduction_completions(hive_id)

        if len(stakes) > 3 and len(introductions) == 0:
            anomalies.append(EconomicAnomaly(
                type="repeated_abandoned_stakes",
                severity="warning",
                details="Multiple stakes placed but introductions abandoned"
            ))

        return anomalies
```

---

## 9. Protocol Messages

### 9.1 Message Type Registry

| Type ID | Name | Fee | Stake | Description |
|---------|------|-----|-------|-------------|
| 1 | ping | 10 | - | Basic connectivity test |
| 2 | pong | 10 | - | Ping response |
| 10 | query_hive_status | 100 | - | Ask if node is in hive |
| 11 | hive_status_response | 100 | - | Response to status query |
| 20 | hive_introduction | 1,000 | 10,000 | Introduce our hive |
| 21 | introduction_response | 1,000 | - | Response to introduction |
| 30 | membership_voucher_request | 500 | 5,000 | Request membership proof |
| 31 | membership_voucher | 500 | - | Membership proof from admin |
| 40 | federation_request | 10,000 | varies | Request federation |
| 41 | federation_response | 10,000 | - | Federation decision |
| 50 | federation_heartbeat | 1,000 | - | Regular federation check-in |
| 51 | heartbeat_response | 1,000 | - | Heartbeat acknowledgment |
| 60 | reputation_query | 100 | - | Query reputation |
| 61 | reputation_response | 100 | - | Reputation data |
| 70 | reputation_challenge | 500 | 10,000 | Issue reputation challenge |
| 71 | challenge_response | 500 | - | Challenge completion |
| 80 | intel_share | 500 | varies | Share intelligence |
| 81 | intel_acknowledgment | 100 | - | Acknowledge intel receipt |
| 90 | defense_alert | 0 | 50,000 | Alert about threat |
| 91 | defense_response | 0 | - | Response to alert |
| 100 | verification_probe | 100 | - | Verification payment |
| 101 | verification_response | 100 | - | Verification acknowledgment |

### 9.2 Message Schemas

See Appendix A for full JSON schemas for each message type.

---

## 10. Implementation Guidelines

### 10.1 Prerequisites

| Requirement | Status | Notes |
|-------------|--------|-------|
| cl-hive | Required | Base coordination |
| Keysend support | Required | For payment-based messages |
| Custom TLV support | Required | For message payloads |
| Route probing | Required | For hidden hive detection |
| On-chain wallet | Required | For Bitcoin timelock escrows |
| HSM signing | Required | For escrow transactions |

### 10.2 New RPC Commands

| Command | Description |
|---------|-------------|
| `hive-query <node>` | Query if node is in a hive |
| `hive-introduce <admin>` | Introduce our hive to another |
| `hive-verify-membership <node> <hive>` | Verify membership claim |
| `hive-probe-cluster <nodes...>` | Probe for hidden hive |
| `hive-challenge <node>` | Issue reputation challenge |
| `hive-payment-reputation <node>` | Get payment-based reputation |
| `hive-economic-analysis <hive>` | Analyze economic relationship |

### 10.3 Database Schema Additions

```sql
-- Payment-based reputation
CREATE TABLE payment_reputation (
    node_id TEXT PRIMARY KEY,
    total_volume_sats INTEGER DEFAULT 0,
    success_rate REAL DEFAULT 0,
    balance_ratio REAL DEFAULT 0,
    interaction_count INTEGER DEFAULT 0,
    last_interaction INTEGER,
    reputation_score REAL DEFAULT 0,
    confidence REAL DEFAULT 0
);

-- Hive message log
CREATE TABLE hive_messages (
    id INTEGER PRIMARY KEY,
    direction TEXT NOT NULL,          -- 'sent', 'received'
    counterparty TEXT NOT NULL,
    counterparty_hive TEXT,
    msg_type INTEGER NOT NULL,
    payment_amount_sats INTEGER,
    stake_amount_sats INTEGER,
    payload TEXT,                      -- JSON
    reply_token TEXT,                  -- Encrypted reply token (privacy-preserving)
    correlation_id TEXT,              -- For matching replies
    status TEXT,                       -- 'sent', 'delivered', 'replied', 'failed'
    timestamp INTEGER NOT NULL
);

-- Verification history
CREATE TABLE verification_history (
    id INTEGER PRIMARY KEY,
    hive_id TEXT NOT NULL,
    verification_type TEXT NOT NULL,
    result TEXT NOT NULL,              -- 'passed', 'partial', 'failed'
    details TEXT,                      -- JSON
    timestamp INTEGER NOT NULL
);

-- Stakes and bonds
CREATE TABLE active_stakes (
    stake_id TEXT PRIMARY KEY,
    counterparty_hive TEXT NOT NULL,
    purpose TEXT NOT NULL,
    amount_sats INTEGER NOT NULL,
    locked_at INTEGER NOT NULL,
    vests_at INTEGER,
    status TEXT DEFAULT 'locked',      -- 'locked', 'vesting', 'returned', 'forfeited'
    forfeit_reason TEXT
);
```

---

## Appendix A: Full Message Schemas

### A.1 query_hive_status

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["msg_type", "payload"],
  "properties": {
    "msg_type": {"const": "query_hive_status"},
    "payload": {
      "type": "object",
      "required": ["query_id"],
      "properties": {
        "query_id": {"type": "string"},
        "include_members": {"type": "boolean", "default": false},
        "include_federation": {"type": "boolean", "default": false},
        "our_hive_id": {"type": "string"}
      }
    },
    "reply_token": {
      "type": "string",
      "description": "Encrypted token for privacy-preserving keysend reply"
    }
  }
}
```

### A.2 hive_introduction

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["msg_type", "payload", "stake_hash"],
  "properties": {
    "msg_type": {"const": "hive_introduction"},
    "payload": {
      "type": "object",
      "required": ["our_hive_id", "our_admin_nodes", "introduction_stake"],
      "properties": {
        "our_hive_id": {"type": "string"},
        "our_admin_nodes": {
          "type": "array",
          "items": {"type": "string"},
          "minItems": 1
        },
        "our_member_count": {"type": "integer", "minimum": 1},
        "our_capacity_tier": {
          "type": "string",
          "enum": ["small", "medium", "large", "xlarge"]
        },
        "introduction_stake": {"type": "integer", "minimum": 10000},
        "proposed_relationship": {
          "type": "string",
          "enum": ["observer", "partner", "allied"]
        },
        "our_public_reputation": {"type": "number", "minimum": 0, "maximum": 1}
      }
    },
    "stake_hash": {"type": "string"},
    "reply_token": {
      "type": "string",
      "description": "Encrypted token for privacy-preserving keysend reply"
    },
    "escrow_pubkey": {
      "type": "string",
      "description": "Public key for Bitcoin timelock escrow (if stake >= 1M sats)"
    }
  }
}
```

---

## Changelog

- **0.1.1-draft** (2025-01-14): Security hardening
  - Fixed circular payment reputation farming with diversity requirements and wash trading detection
  - Fixed probe evasion via stealth probing and extended observation windows
  - Fixed reply invoice information leakage with privacy-preserving keysend reply tokens
  - Added Bitcoin timelock escrow for high-value stakes (>= 1M sats)
  - Added forfeit proof system for cryptographically provable violations
  - Added escrow lifecycle management (cooperative release, forfeit claim, timeout refund)
- **0.1.0-draft** (2025-01-14): Initial specification draft
