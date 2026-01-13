# Cooperative Fee Coordination Design Document

## Overview

This document explores how hive members can cooperatively set fees, rebalance channels, and share intelligence to maximize collective profitability while ensuring no node is left behind.

**Guiding Principles:**
1. **No Node Left Behind**: Smaller nodes must benefit; the hive's strength is its weakest member
2. **Don't Trust, Verify**: All messages require cryptographic signatures; members are potentially hostile
3. **Collective Alpha**: Information asymmetry benefits the hive, not individuals

---

## Part 1: Cooperative Fee Setting

### 1.1 Problem Statement

Currently, each hive member runs cl-revenue-ops independently with the HIVE strategy (0-fee for members). However, fees to **external peers** are set individually without coordination, leading to:

- **Suboptimal pricing**: Members may undercut each other on popular routes
- **Missed opportunities**: No collective intelligence on fee elasticity
- **Uneven revenue**: Larger nodes capture routing while smaller nodes starve

### 1.2 Proposed Solution: Fee Intelligence Sharing

#### 1.2.1 New Message Type: FEE_INTELLIGENCE

Share fee-related observations across hive members:

```python
@dataclass
class FeeIntelligence:
    """Fee intelligence report from a hive member."""
    reporter_id: str              # Who observed this
    target_peer_id: str           # External peer
    timestamp: int
    signature: str                # REQUIRED: Sign with reporter's key

    # Current fee configuration
    our_fee_ppm: int              # Fee we charge to this peer
    their_fee_ppm: int            # Fee they charge us

    # Performance metrics (last 7 days)
    forward_count: int            # Number of forwards
    forward_volume_sats: int      # Total volume routed
    revenue_sats: int             # Fees earned

    # Flow analysis
    flow_direction: str           # 'source', 'sink', 'balanced'
    utilization_pct: float        # Channel utilization (0-1)

    # Elasticity observation
    last_fee_change_ppm: int      # Previous fee rate
    volume_delta_pct: float       # Volume change after fee change

    # Confidence
    days_observed: int            # How long we've had this channel
```

#### 1.2.2 Aggregated Fee View

Each node maintains an aggregated view of external peers:

```python
@dataclass
class PeerFeeProfile:
    """Aggregated fee intelligence for an external peer."""
    peer_id: str

    # Aggregated from multiple reporters
    reporters: List[str]          # Hive members with channels to this peer

    # Fee statistics
    avg_fee_charged: float        # Average fee hive charges this peer
    min_fee_charged: int          # Lowest fee any member charges
    max_fee_charged: int          # Highest fee any member charges

    # Performance (aggregated)
    total_hive_volume: int        # Total volume hive routes through this peer
    total_hive_revenue: int       # Total revenue hive earns from this peer
    avg_utilization: float        # Average channel utilization

    # Elasticity estimate
    estimated_elasticity: float   # Price sensitivity (-1 to 1)
    optimal_fee_estimate: int     # Recommended fee based on collective data

    # Quality from quality_scorer
    quality_score: float

    # Timestamps
    last_update: int
    confidence: float             # Based on reporter count and data freshness
```

### 1.3 Cooperative Fee Strategies

#### 1.3.1 Strategy: HIVE_COORDINATED

New fee strategy for external peers, leveraging collective intelligence:

```python
class CoordinatedFeeStrategy:
    """
    Fee strategy that uses hive intelligence for optimal pricing.

    Replaces individual hill-climbing with collective optimization.
    """

    # Weight factors for fee recommendation
    WEIGHT_QUALITY = 0.25         # Higher quality = can charge more
    WEIGHT_ELASTICITY = 0.30      # Price sensitivity matters most
    WEIGHT_COMPETITION = 0.20    # What others in hive charge
    WEIGHT_FAIRNESS = 0.25       # No Node Left Behind factor

    def calculate_recommended_fee(
        self,
        peer_id: str,
        our_channel_size: int,
        profile: PeerFeeProfile,
        our_node_health: float  # 0-1, from NNLB health scoring
    ) -> int:
        """
        Calculate recommended fee for an external peer.

        NNLB Integration: Struggling nodes get fee priority
        """
        base_fee = profile.optimal_fee_estimate

        # Quality adjustment: higher quality peers tolerate higher fees
        quality_mult = 0.8 + (profile.quality_score * 0.4)  # 0.8x to 1.2x

        # Elasticity adjustment: elastic demand = lower fees
        if profile.estimated_elasticity < -0.5:
            elasticity_mult = 0.7  # Very elastic, keep fees low
        elif profile.estimated_elasticity < 0:
            elasticity_mult = 0.9  # Somewhat elastic
        else:
            elasticity_mult = 1.1  # Inelastic, can raise fees

        # Competition adjustment: don't undercut hive members
        if base_fee < profile.avg_fee_charged:
            competition_mult = 1.0  # Already below average
        else:
            competition_mult = 0.95  # Slightly undercut average

        # NNLB Fairness: struggling nodes get fee priority
        if our_node_health < 0.4:
            # Struggling node: recommend LOWER fees to attract traffic
            fairness_mult = 0.7 + (our_node_health * 0.5)  # 0.7x to 0.9x
        elif our_node_health > 0.7:
            # Healthy node: can afford higher fees, yield to others
            fairness_mult = 1.0 + ((our_node_health - 0.7) * 0.3)  # 1.0x to 1.1x
        else:
            fairness_mult = 1.0

        recommended = int(
            base_fee *
            quality_mult *
            elasticity_mult *
            competition_mult *
            fairness_mult
        )

        return max(1, min(recommended, 5000))  # Bounds: 1-5000 ppm
```

#### 1.3.2 Fee Recommendation Protocol

```
1. COLLECT: Each member reports FEE_INTELLIGENCE periodically (hourly)
2. AGGREGATE: Each member builds PeerFeeProfile from all reports
3. RECOMMEND: Calculate optimal fee using collective data
4. APPLY: Update fee via cl-revenue-ops PolicyManager
5. VERIFY: Compare results, adjust strategy
```

### 1.4 Security: Signed Fee Intelligence

All FEE_INTELLIGENCE messages must be signed:

```python
def create_fee_intelligence(
    reporter_id: str,
    target_peer_id: str,
    metrics: dict,
    rpc  # For signmessage
) -> bytes:
    """Create signed FEE_INTELLIGENCE message."""
    payload = {
        "reporter_id": reporter_id,
        "target_peer_id": target_peer_id,
        "timestamp": int(time.time()),
        **metrics
    }

    # Sign the canonical payload
    signing_message = get_fee_intelligence_signing_payload(payload)
    sig_result = rpc.signmessage(signing_message)
    payload["signature"] = sig_result["zbase"]

    return serialize(HiveMessageType.FEE_INTELLIGENCE, payload)


def handle_fee_intelligence(peer_id: str, payload: dict, plugin) -> dict:
    """Handle incoming FEE_INTELLIGENCE with signature verification."""
    # Verify reporter is a hive member
    reporter_id = payload.get("reporter_id")
    if not database.get_member(reporter_id):
        return {"error": "reporter not a member"}

    # VERIFY SIGNATURE (Don't Trust, Verify)
    signature = payload.get("signature")
    signing_message = get_fee_intelligence_signing_payload(payload)

    verify_result = plugin.rpc.checkmessage(signing_message, signature)
    if not verify_result.get("verified"):
        plugin.log(f"FEE_INTELLIGENCE signature verification failed", level='warn')
        return {"error": "invalid signature"}

    if verify_result.get("pubkey") != reporter_id:
        plugin.log(f"FEE_INTELLIGENCE signature mismatch", level='warn')
        return {"error": "signature mismatch"}

    # Store and aggregate
    store_fee_intelligence(payload)
    return {"success": True}
```

---

## Part 2: Cooperative Rebalancing

### 2.1 Problem Statement

Current rebalancing is node-local: each member rebalances its own channels without awareness of hive-wide liquidity needs. This leads to:

- **Circular waste**: Member A rebalances to peer X while Member B rebalances away from X
- **Missed synergies**: Members could push liquidity to each other at zero cost
- **NNLB violation**: Struggling nodes can't afford rebalancing costs

### 2.2 Proposed Solution: Hive Liquidity Coordination

#### 2.2.1 New Message Type: LIQUIDITY_NEED

Members broadcast their liquidity needs:

```python
@dataclass
class LiquidityNeed:
    """Broadcast liquidity requirements."""
    reporter_id: str
    timestamp: int
    signature: str

    # What we need
    need_type: str                # 'inbound', 'outbound', 'rebalance'
    target_peer_id: str           # External peer (or hive member for internal)
    amount_sats: int              # How much we need
    urgency: str                  # 'critical', 'high', 'medium', 'low'
    max_fee_ppm: int              # Maximum fee we'll pay

    # Why we need it
    reason: str                   # 'channel_depleted', 'opportunity', 'nnlb_assist'
    current_balance_pct: float    # Current local balance percentage

    # Our capacity to help others (reciprocity)
    can_provide_inbound: int      # Sats of inbound we can provide
    can_provide_outbound: int     # Sats of outbound we can provide
```

#### 2.2.2 Internal Hive Rebalancing (Zero Cost)

Rebalancing between hive members should be FREE:

```python
class HiveRebalanceCoordinator:
    """
    Coordinate zero-cost rebalancing between hive members.

    Since hive members have 0-fee channels to each other,
    circular rebalancing within the hive is essentially free.
    """

    def find_internal_rebalance_opportunity(
        self,
        needs: List[LiquidityNeed],
        our_state: HivePeerState
    ) -> Optional[RebalanceProposal]:
        """
        Find a rebalance that helps another member at minimal cost.

        Example:
        - Alice needs outbound to ExternalPeer X
        - Bob has excess outbound to ExternalPeer X
        - Bob can push to Alice via hive (0 fee), Alice pushes to X
        """
        for need in needs:
            if need.reporter_id == our_id:
                continue

            # Can we help this member?
            if need.need_type == 'outbound':
                # They need outbound to target
                # Do we have excess outbound to that target?
                our_balance = get_channel_balance(need.target_peer_id)
                if our_balance and our_balance.local_pct > 0.7:
                    # We have excess, propose internal rebalance
                    return RebalanceProposal(
                        type='internal_push',
                        from_member=our_id,
                        to_member=need.reporter_id,
                        target_peer=need.target_peer_id,
                        amount=min(need.amount_sats, our_balance.excess_sats),
                        estimated_cost=0,  # Internal rebalance is free
                        nnlb_priority=get_member_health(need.reporter_id)
                    )

        return None
```

#### 2.2.3 NNLB Rebalancing Priority

Struggling nodes get rebalancing assistance:

```python
def prioritize_rebalance_requests(needs: List[LiquidityNeed]) -> List[LiquidityNeed]:
    """
    Sort rebalance needs by NNLB priority.

    Struggling nodes get helped first.
    """
    def nnlb_priority(need: LiquidityNeed) -> float:
        member_health = get_member_health(need.reporter_id)

        # Lower health = higher priority (inverted)
        health_priority = 1.0 - member_health

        # Urgency multiplier
        urgency_mult = {
            'critical': 2.0,
            'high': 1.5,
            'medium': 1.0,
            'low': 0.5
        }.get(need.urgency, 1.0)

        return health_priority * urgency_mult

    return sorted(needs, key=nnlb_priority, reverse=True)
```

### 2.3 Coordinated External Rebalancing

When internal rebalancing isn't possible, coordinate external rebalancing:

```python
@dataclass
class RebalanceCoordinationRound:
    """Coordinate rebalancing to avoid conflicts."""
    round_id: str
    started_at: int
    coordinator_id: str           # Who initiated this round
    signature: str

    # Participants
    participants: List[str]       # Members who need rebalancing

    # Proposed actions (non-conflicting)
    actions: List[RebalanceAction]

    # Expected outcome
    total_cost_sats: int
    beneficiaries: List[str]      # Members who benefit


class RebalanceAction:
    """Single rebalance action in a coordinated round."""
    executor_id: str              # Who performs this rebalance
    from_peer: str                # Source peer
    to_peer: str                  # Destination peer
    amount_sats: int
    max_fee_sats: int

    # NNLB: Who benefits?
    primary_beneficiary: str      # Member who most needs this
    is_nnlb_assist: bool          # Is this helping a struggling member?
```

---

## Part 3: Information Sharing Protocols

### 3.1 What Information Can Be Shared

Based on existing infrastructure, hive members can share:

| Data Type | Source | Current State | Cooperative Use |
|-----------|--------|---------------|-----------------|
| **Channel Events** | PEER_AVAILABLE | Implemented | Quality scoring |
| **Fee Configuration** | GOSSIP | Implemented (own fees) | Needs: external peer fees |
| **Flow Direction** | cl-revenue-ops | Local only | **NEW: Share via FEE_INTELLIGENCE** |
| **Elasticity Data** | cl-revenue-ops | Local only | **NEW: Share for collective optimization** |
| **Rebalance Costs** | cl-revenue-ops | Local only | **NEW: Share via LIQUIDITY_NEED** |
| **Route Quality** | renepay probes | Not implemented | **NEW: ROUTE_PROBE message** |

### 3.2 New Message Type: ROUTE_PROBE

Share payment path quality observations:

```python
@dataclass
class RouteProbe:
    """
    Report on payment path quality.

    Members can probe routes and share results to build
    collective routing intelligence.
    """
    reporter_id: str
    timestamp: int
    signature: str

    # Route definition
    destination: str              # Final destination pubkey
    path: List[str]               # Intermediate hops (pubkeys)

    # Probe results
    success: bool
    latency_ms: int               # Round-trip time
    failure_reason: str           # If failed: 'temporary', 'permanent', 'capacity'
    failure_hop: int              # Which hop failed (index)

    # Capacity observations
    estimated_capacity_sats: int  # Max amount that would succeed

    # Fee observations
    total_fee_ppm: int            # Total fees for this route
    per_hop_fees: List[int]       # Fee at each hop
```

### 3.3 Collective Routing Map

Aggregate route probes to build a shared routing map:

```python
class HiveRoutingMap:
    """
    Collective routing intelligence from all hive members.

    Each member contributes observations; all benefit from
    the aggregated routing knowledge.
    """

    def get_best_route_to(
        self,
        destination: str,
        amount_sats: int
    ) -> Optional[RouteSuggestion]:
        """
        Get best known route to destination based on collective probes.

        Returns route with:
        - Highest success rate
        - Lowest fees
        - Sufficient capacity
        """
        probes = self.get_probes_for_destination(destination)

        # Filter by capacity
        viable = [p for p in probes if p.estimated_capacity_sats >= amount_sats]

        # Score by success rate and fees
        scored = []
        for probe in viable:
            success_rate = self.get_path_success_rate(probe.path)
            fee_score = 1.0 / (1 + probe.total_fee_ppm / 1000)

            # Prefer paths through hive members (0 fee hops)
            hive_hop_count = sum(1 for hop in probe.path if is_hive_member(hop))
            hive_bonus = 0.1 * hive_hop_count

            score = success_rate * fee_score + hive_bonus
            scored.append((probe, score))

        if not scored:
            return None

        best_probe, _ = max(scored, key=lambda x: x[1])
        return RouteSuggestion(
            path=best_probe.path,
            expected_fee_ppm=best_probe.total_fee_ppm,
            confidence=self.get_path_confidence(best_probe.path)
        )
```

---

## Part 4: No Node Left Behind (NNLB) Implementation

### 4.1 Member Health Scoring

Track each member's health to identify who needs help:

```python
@dataclass
class MemberHealth:
    """
    Comprehensive health assessment for NNLB.

    Combines multiple factors to identify struggling members.
    """
    peer_id: str
    timestamp: int

    # Capacity metrics (0-100)
    capacity_score: int           # Total channel capacity vs hive average
    balance_score: int            # How well-balanced are channels

    # Revenue metrics (0-100)
    revenue_score: int            # Daily revenue vs hive average
    profitability_score: int      # ROI on capital deployed

    # Connectivity metrics (0-100)
    connectivity_score: int       # Number and quality of external connections
    centrality_score: int         # Position in network graph

    # Overall health (0-100)
    overall_health: int

    # Classification
    tier: str                     # 'thriving', 'healthy', 'struggling', 'critical'
    needs_help: bool
    can_help_others: bool

    # Specific recommendations
    recommendations: List[str]


def calculate_member_health(
    peer_id: str,
    hive_states: Dict[str, HivePeerState],
    fee_profiles: Dict[str, PeerFeeProfile]
) -> MemberHealth:
    """Calculate comprehensive health score for a member."""
    state = hive_states.get(peer_id)
    if not state:
        return MemberHealth(peer_id=peer_id, overall_health=0, tier='unknown')

    # Get hive averages for comparison
    avg_capacity = sum(s.capacity_sats for s in hive_states.values()) / len(hive_states)

    # Capacity score: compare to hive average
    capacity_score = min(100, int(state.capacity_sats / avg_capacity * 50))

    # Revenue score: from fee intelligence (if available)
    member_revenue = get_member_revenue(peer_id, fee_profiles)
    avg_revenue = get_hive_average_revenue(fee_profiles)
    revenue_score = min(100, int(member_revenue / max(1, avg_revenue) * 50))

    # Connectivity: count external connections
    connectivity_score = min(100, len(state.topology) * 10)

    # Overall weighted average
    overall = int(
        capacity_score * 0.30 +
        revenue_score * 0.35 +
        connectivity_score * 0.35
    )

    # Classify
    if overall >= 75:
        tier = 'thriving'
        needs_help = False
        can_help = True
    elif overall >= 50:
        tier = 'healthy'
        needs_help = False
        can_help = True
    elif overall >= 25:
        tier = 'struggling'
        needs_help = True
        can_help = False
    else:
        tier = 'critical'
        needs_help = True
        can_help = False

    return MemberHealth(
        peer_id=peer_id,
        timestamp=int(time.time()),
        capacity_score=capacity_score,
        revenue_score=revenue_score,
        connectivity_score=connectivity_score,
        overall_health=overall,
        tier=tier,
        needs_help=needs_help,
        can_help_others=can_help,
        recommendations=generate_nnlb_recommendations(peer_id, state, overall)
    )
```

### 4.2 NNLB Assistance Actions

#### 4.2.1 Fee Priority for Struggling Nodes

```python
def apply_nnlb_fee_adjustment(
    member_health: MemberHealth,
    base_fee: int
) -> int:
    """
    Adjust fee recommendation based on NNLB.

    Struggling nodes get lower fees to attract traffic.
    Thriving nodes yield fee alpha to help others.
    """
    if member_health.tier == 'critical':
        # Critical: 30% of normal fee to attract ANY traffic
        return int(base_fee * 0.3)
    elif member_health.tier == 'struggling':
        # Struggling: 60% of normal fee
        return int(base_fee * 0.6)
    elif member_health.tier == 'thriving':
        # Thriving: can afford 110% to yield to others
        return int(base_fee * 1.1)
    else:
        # Healthy: normal fees
        return base_fee
```

#### 4.2.2 Liquidity Assistance

```python
def generate_nnlb_assistance_proposal(
    struggling_member: str,
    thriving_members: List[str]
) -> Optional[AssistanceProposal]:
    """
    Generate proposal for thriving members to help struggling member.

    Types of assistance:
    1. Channel open: Thriving member opens channel to struggling
    2. Liquidity push: Push sats to struggling member's depleted channels
    3. Fee yield: Raise own fees to push traffic to struggling member
    """
    struggling_health = get_member_health(struggling_member)

    proposals = []

    for thriving in thriving_members:
        thriving_health = get_member_health(thriving)

        if not thriving_health.can_help_others:
            continue

        # Check what kind of help is most needed
        if struggling_health.capacity_score < 30:
            # Needs more capacity: propose channel open
            proposals.append(AssistanceProposal(
                type='channel_open',
                from_member=thriving,
                to_member=struggling_member,
                amount_sats=calculate_helpful_channel_size(thriving, struggling_member),
                expected_benefit=15,  # Health point improvement estimate
            ))

        elif struggling_health.revenue_score < 30:
            # Needs more traffic: propose fee coordination
            proposals.append(AssistanceProposal(
                type='fee_yield',
                from_member=thriving,
                to_member=struggling_member,
                fee_increase_ppm=50,  # Raise own fees by 50ppm
                expected_benefit=10,
            ))

    # Return highest impact proposal
    if proposals:
        return max(proposals, key=lambda p: p.expected_benefit)
    return None
```

### 4.3 NNLB Message Type: HEALTH_REPORT

Share health status for collective awareness:

```python
@dataclass
class HealthReport:
    """
    Periodic health report for NNLB coordination.

    Allows hive to identify who needs help without
    explicitly asking (preserves dignity).
    """
    reporter_id: str
    timestamp: int
    signature: str

    # Self-reported health (verified against gossip data)
    overall_health: int           # 0-100
    capacity_score: int
    revenue_score: int
    connectivity_score: int

    # Specific needs (optional)
    needs_inbound: bool
    needs_outbound: bool
    needs_channels: bool

    # Willingness to help
    can_provide_assistance: bool
    assistance_budget_sats: int   # How much can spend helping others
```

---

## Part 5: Additional Cooperative Opportunities

### 5.1 Cooperative Channel Close Timing

Coordinate channel closures to minimize on-chain fees:

```python
@dataclass
class ClosureCoordination:
    """
    Coordinate channel closures for optimal timing.

    - Batch closures during low-fee periods
    - Avoid closing channels that another member needs
    - Coordinate mutual closes for fee savings
    """
    proposed_closes: List[ChannelClose]
    optimal_block_target: int     # When fees are expected lowest
    total_estimated_fees: int

    # Conflict detection
    conflicts: List[str]          # Channels another member depends on
```

### 5.2 Cooperative Splice Coordination

Coordinate channel splices for topology optimization:

```python
@dataclass
class SpliceProposal:
    """
    Propose cooperative splice operation.

    Multiple members can coordinate splices to:
    - Resize channels optimally
    - Batch on-chain transactions
    - Maintain balanced hive topology
    """
    round_id: str
    coordinator_id: str
    signature: str

    operations: List[SpliceOperation]
    batch_txid: str               # Shared transaction (if batched)
    total_fee_savings: int        # vs individual operations
```

### 5.3 Cooperative Peer Reputation

Share reputation data about external peers:

```python
@dataclass
class PeerReputation:
    """
    Share reputation observations about external peers.

    Aggregate experiences to warn about:
    - Unreliable peers (frequent force closes)
    - Fee manipulation (sudden fee spikes)
    - Routing issues (failed HTLCs)
    """
    peer_id: str
    reporter_id: str
    timestamp: int
    signature: str

    # Reliability
    uptime_pct: float             # How often peer is online
    response_time_ms: int         # Average HTLC response time
    force_close_count: int        # Number of force closes initiated

    # Behavior
    fee_stability: float          # How stable are their fees (0-1)
    htlc_success_rate: float      # % of HTLCs that succeed

    # Warnings
    warnings: List[str]           # Specific issues observed
```

### 5.4 Cooperative Liquidity Advertising

Advertise available liquidity for incoming channels:

```python
@dataclass
class LiquidityAdvertisement:
    """
    Advertise available liquidity for strategic channel opens.

    External nodes wanting hive connectivity can see where
    liquidity is available and request channels.
    """
    advertiser_id: str            # Hive member offering liquidity
    timestamp: int
    signature: str

    # What's available
    available_sats: int           # How much we can deploy
    min_channel_size: int
    max_channel_size: int

    # Terms
    lease_rate_ppm: int           # If offering liquidity ads
    min_duration_days: int        # Minimum channel duration

    # Preferences
    preferred_peers: List[str]    # External peers we'd like channels with
    avoided_peers: List[str]      # Peers we won't open to
```

### 5.5 Cooperative Invoice Routing Hints

Share optimal routing hints for invoices:

```python
def generate_hive_routing_hints(
    destination: str,  # Hive member receiving payment
    amount_sats: int
) -> List[RouteHint]:
    """
    Generate routing hints that prefer hive paths.

    By including hive members in route hints, we:
    - Increase hive routing revenue
    - Ensure reliable payment paths
    - Distribute traffic across members (NNLB)
    """
    hints = []

    # Get healthy hive members with good connectivity
    healthy_members = get_healthy_hive_members()

    for member in healthy_members:
        # Check if they have path to destination
        if has_channel_to(member, destination):
            hints.append(RouteHint(
                pubkey=member,
                short_channel_id=get_channel_id(member, destination),
                fee_base_msat=0,      # 0 fee for hive
                fee_ppm=0,
                cltv_delta=40
            ))

    # Prioritize struggling members (NNLB)
    hints.sort(key=lambda h: get_member_health(h.pubkey).overall_health)

    return hints[:3]  # Return top 3 hints
```

---

## Part 6: Security Considerations

### 6.1 Message Signing Requirements

**ALL new message types MUST be signed:**

| Message Type | Signer | Verification |
|--------------|--------|--------------|
| FEE_INTELLIGENCE | reporter_id | checkmessage against reporter |
| LIQUIDITY_NEED | reporter_id | checkmessage against reporter |
| ROUTE_PROBE | reporter_id | checkmessage against reporter |
| HEALTH_REPORT | reporter_id | checkmessage against reporter |
| REBALANCE_COORDINATION | coordinator_id | checkmessage against coordinator |
| PEER_REPUTATION | reporter_id | checkmessage against reporter |

### 6.2 Data Validation

```python
def validate_fee_intelligence(payload: dict) -> bool:
    """
    Validate FEE_INTELLIGENCE payload.

    SECURITY: Bound all values to prevent manipulation.
    """
    # Fee bounds
    if not (0 <= payload.get('our_fee_ppm', 0) <= 10000):
        return False

    # Volume bounds (prevent overflow)
    if payload.get('forward_volume_sats', 0) > 1_000_000_000_000:  # 10k BTC max
        return False

    # Timestamp freshness (reject old data)
    if abs(time.time() - payload.get('timestamp', 0)) > 3600:  # 1 hour max
        return False

    # Utilization bounds
    if not (0 <= payload.get('utilization_pct', 0) <= 1):
        return False

    return True
```

### 6.3 Reputation Attack Prevention

```python
def apply_reputation_with_skepticism(
    reports: List[PeerReputation],
    peer_id: str
) -> AggregatedReputation:
    """
    Aggregate reputation reports with skepticism.

    SECURITY: Don't trust any single reporter.
    """
    # Require multiple reporters for strong claims
    if len(reports) < 3:
        return AggregatedReputation(confidence='low')

    # Outlier detection: remove reports that differ significantly
    median_uptime = statistics.median(r.uptime_pct for r in reports)
    filtered = [r for r in reports if abs(r.uptime_pct - median_uptime) < 0.2]

    # Cross-check against our own observations if we have them
    our_observation = get_our_observation(peer_id)
    if our_observation:
        # Weight our own data 2x
        filtered.append(our_observation)
        filtered.append(our_observation)

    return aggregate_with_weights(filtered)
```

### 6.4 Rate Limiting

All new message types subject to rate limiting:

```python
# Rate limits per message type
RATE_LIMITS = {
    'FEE_INTELLIGENCE': (10, 3600),    # 10 per hour per sender
    'LIQUIDITY_NEED': (5, 3600),       # 5 per hour per sender
    'ROUTE_PROBE': (20, 3600),         # 20 per hour per sender
    'HEALTH_REPORT': (1, 3600),        # 1 per hour per sender
    'PEER_REPUTATION': (5, 86400),     # 5 per day per sender
}
```

---

## Part 7: Implementation Phases

### Phase 1: Fee Intelligence (Immediate)
1. Add FEE_INTELLIGENCE message type with signing
2. Add fee profile aggregation
3. Integrate with cl-revenue-ops PolicyManager

### Phase 2: NNLB Health Scoring (Short-term)
1. Add HEALTH_REPORT message type
2. Implement member health calculation
3. Add NNLB fee adjustment

### Phase 3: Cooperative Rebalancing (Medium-term)
1. Add LIQUIDITY_NEED message type
2. Implement internal hive rebalancing
3. Add coordinated external rebalancing

### Phase 4: Routing Intelligence (Long-term)
1. Add ROUTE_PROBE message type
2. Implement HiveRoutingMap
3. Integrate with renepay or custom routing

### Phase 5: Advanced Cooperation (Future)
1. Splice coordination
2. Closure timing
3. Liquidity advertising

---

## Appendix A: Message Type Summary

| ID | Type | Purpose | Signed |
|----|------|---------|--------|
| 32809 | FEE_INTELLIGENCE | Share fee observations | YES |
| 32811 | LIQUIDITY_NEED | Broadcast rebalancing needs | YES |
| 32813 | ROUTE_PROBE | Share routing observations | YES |
| 32815 | HEALTH_REPORT | NNLB health status | YES |
| 32817 | REBALANCE_COORDINATION | Coordinate rebalancing | YES |
| 32819 | PEER_REPUTATION | Share peer reputation | YES |

---

## Appendix B: Database Schema Additions

```sql
-- Fee intelligence aggregation
CREATE TABLE fee_intelligence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reporter_id TEXT NOT NULL,
    target_peer_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    our_fee_ppm INTEGER,
    their_fee_ppm INTEGER,
    forward_count INTEGER,
    forward_volume_sats INTEGER,
    revenue_sats INTEGER,
    flow_direction TEXT,
    utilization_pct REAL,
    volume_delta_pct REAL,
    signature TEXT NOT NULL
);

-- Member health tracking
CREATE TABLE member_health (
    peer_id TEXT PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    overall_health INTEGER,
    capacity_score INTEGER,
    revenue_score INTEGER,
    connectivity_score INTEGER,
    tier TEXT,
    needs_help INTEGER,
    can_help_others INTEGER
);

-- Route probes
CREATE TABLE route_probes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    reporter_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    path TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    success INTEGER,
    latency_ms INTEGER,
    estimated_capacity_sats INTEGER,
    total_fee_ppm INTEGER,
    signature TEXT NOT NULL
);
```
