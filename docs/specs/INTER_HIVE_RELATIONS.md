# Inter-Hive Relations Protocol Specification

**Version:** 0.1.0-draft
**Status:** Proposal
**Authors:** cl-hive contributors
**Date:** 2025-01-14

## Abstract

This specification defines protocols for detecting, classifying, and managing relationships with other Lightning Network node fleets ("hives"). It establishes reputation systems, policy frameworks, and federation mechanisms while maintaining security against hostile actors.

## Table of Contents

1. [Motivation](#1-motivation)
2. [Design Principles](#2-design-principles)
3. [Hive Detection](#3-hive-detection)
4. [Hive Classification](#4-hive-classification)
5. [Reputation System](#5-reputation-system)
6. [Policy Framework](#6-policy-framework)
7. [Federation Protocol](#7-federation-protocol)
8. [Security Considerations](#8-security-considerations)
9. [Implementation Guidelines](#9-implementation-guidelines)

---

## 1. Motivation

### 1.1 The Multi-Hive Future

As coordinated node management becomes more common, the Lightning Network will contain multiple independent hives:
- Commercial routing operations
- Community cooperatives
- Geographic clusters
- Protocol-specific fleets (LSPs, exchanges)

### 1.2 Strategic Necessity

Without inter-hive awareness:
- We can't distinguish coordinated competitors from random nodes
- We miss opportunities for mutually beneficial cooperation
- We're vulnerable to predatory fleet behavior
- We can't form defensive alliances

### 1.3 Trust Challenges

Other hives may be:
- **Cooperative**: Potential allies for mutual benefit
- **Competitive**: Fair market rivals
- **Hostile**: Actively harmful actors
- **Deceptive**: Appearing friendly while extracting value

**Core Principle**: Don't trust. Verify.

---

## 2. Design Principles

### 2.1 Verify Everything

Never trust self-reported data. All classifications based on:
- Observed behavior over time
- Verifiable on-chain actions
- Third-party corroboration
- Economic incentive analysis

### 2.2 Assume Predatory Until Proven Otherwise

**All detected hives start at `predatory` classification.** They are competing for the same ecological niche (routing fees, liquidity, market position). Trust is earned through sustained positive interactions over extended periods, never granted or assumed.

**Rationale**: In a competitive network:
- Resources (routing flows, liquidity corridors) are finite
- Every hive is incentivized to maximize their share
- Cooperation must be economically rational for both parties
- The cost of trusting a predator exceeds the cost of slowly verifying a friend

### 2.3 Gradual Trust Building

```
detected → predatory → competitive → neutral → cooperative → federated
              ↓            ↓           ↓            ↓
           hostile     parasitic   [downgrade]  [revoked]
```

**Trust progression requirements**:
- predatory → competitive: 60 days observation, no hostile actions, balanced economic relationship
- competitive → neutral: 90 days, positive behavior score, reciprocal value exchange
- neutral → cooperative: 180 days, high reliability, verified reciprocity
- cooperative → federated: 365 days, formal agreement, mutual stake

Trust increases **very slowly**, decreases **immediately**.

### 2.4 Reciprocity Required

Relationships must be mutually beneficial. One-sided value extraction triggers automatic downgrade.

### 2.5 Isolation by Default

Hive internal information is never shared with external hives unless explicitly federated and verified.

### 2.6 Stealth Operations First

**Never reveal hive membership when detecting or interacting with unknown hives.**

Our strategy:
1. **Detect silently**: Use passive observation and normal routing behavior
2. **Build reputation first**: Establish ourselves as reliable routing nodes before any hive revelation
3. **Gather intelligence**: Learn their behavior, capacity, and intentions through routing
4. **Reveal strategically**: Only disclose hive membership when economically advantageous

**Why stealth matters**:
- Revealing hive status invites targeted competition
- Predatory hives may specifically target known hive members
- Intelligence gathering is more effective when undetected
- Reputation earned "anonymously" is more trustworthy

### 2.7 Normal Routing as Cover

All intelligence gathering and reputation building should be indistinguishable from normal Lightning Network activity:
- Route payments through suspected hive members
- Accept their routing requests
- Open channels that make economic sense anyway
- Set fees based on normal revenue optimization

**No hive-specific protocol messages until trust is established.**

---

## 3. Hive Detection

### 3.1 Detection Methods

#### 3.1.1 Channel Pattern Analysis

Identify node clusters with coordinated characteristics:

```python
class HiveDetector:
    def analyze_cluster(self, nodes: List[str]) -> HiveSignature:
        signals = {
            "internal_zero_fee": self.check_internal_fees(nodes),
            "coordinated_opens": self.check_open_timing(nodes),
            "fee_synchronization": self.check_fee_patterns(nodes),
            "capacity_distribution": self.check_capacity_patterns(nodes),
            "common_peers": self.check_peer_overlap(nodes),
            "naming_patterns": self.check_alias_patterns(nodes),
        }
        return HiveSignature(nodes=nodes, signals=signals)
```

**Detection Signals**:

| Signal | Weight | Description |
|--------|--------|-------------|
| Internal zero-fee | 0.9 | Channels between suspected members have 0 ppm |
| Coordinated opens | 0.7 | Multiple nodes open to same target within hours |
| Fee synchronization | 0.6 | Fee changes occur simultaneously |
| Shared peer set | 0.5 | Unusually high overlap in channel partners |
| Naming patterns | 0.3 | Similar aliases (e.g., "HiveX-1", "HiveX-2") |
| Geographic clustering | 0.4 | Nodes in same IP ranges or regions |

**Confidence Threshold**: Σ(signals × weights) > 2.0 → likely hive

#### 3.1.2 Behavioral Analysis

Track coordinated actions over time:

```python
def detect_coordinated_behavior(self, timeframe_hours=168):
    """Detect hives through behavioral correlation."""
    events = self.get_network_events(timeframe_hours)

    correlations = {}
    for event in events:
        # Find nodes that acted within 1 hour of each other
        correlated = self.find_correlated_actors(event, window_hours=1)
        for pair in combinations(correlated, 2):
            correlations[pair] = correlations.get(pair, 0) + 1

    # Cluster highly correlated nodes
    return self.cluster_correlated_nodes(correlations, threshold=5)
```

#### 3.1.3 Self-Identification

Some hives may announce themselves via:
- Custom TLV in channel announcements
- Public registry (future)
- Direct introduction protocol

**Trust Level**: Self-identification alone = 0. Must be verified by behavior.

#### 3.1.4 Intelligence Sharing (Federated Hives Only)

Trusted federated hives may share hive detection intelligence:

```json
{
  "type": "hive_intel_share",
  "from_hive": "hive_abc123",
  "detected_hive": {
    "suspected_members": ["02xyz...", "03abc..."],
    "confidence": 0.75,
    "classification": "competitive",
    "evidence_summary": ["coordinated_fees", "shared_peers"],
    "first_detected": 1705234567
  },
  "attestation": {...}
}
```

### 3.2 Hive Signature

```python
@dataclass
class HiveSignature:
    hive_id: str                    # Generated hash of member set
    suspected_members: List[str]    # Node pubkeys
    confidence: float               # 0.0 - 1.0
    detection_method: str           # "pattern", "behavior", "self_id", "intel"
    first_detected: int             # Unix timestamp
    last_confirmed: int             # Last behavioral confirmation
    signals: Dict[str, float]       # Detection signals and scores

    def stable_id(self) -> str:
        """Generate stable ID from sorted member list."""
        return hashlib.sha256(
            ",".join(sorted(self.suspected_members)).encode()
        ).hexdigest()[:16]
```

### 3.3 Hive Registry

```sql
CREATE TABLE detected_hives (
    hive_id TEXT PRIMARY KEY,
    members TEXT NOT NULL,          -- JSON array of pubkeys
    confidence REAL NOT NULL,
    classification TEXT DEFAULT 'predatory',  -- All hives start as predatory
    reputation_score REAL DEFAULT 0.0,
    first_detected INTEGER NOT NULL,
    last_updated INTEGER NOT NULL,
    detection_evidence TEXT,        -- JSON
    policy_id INTEGER REFERENCES hive_policies(id),
    our_revelation_status TEXT DEFAULT 'hidden',  -- hidden, partial, revealed
    their_awareness TEXT DEFAULT 'unknown'        -- unknown, suspects, knows
);

CREATE TABLE hive_members (
    node_id TEXT PRIMARY KEY,
    hive_id TEXT REFERENCES detected_hives(hive_id),
    confidence REAL NOT NULL,
    first_seen INTEGER NOT NULL,
    last_confirmed INTEGER NOT NULL
);

-- Track our routing reputation with each detected hive
CREATE TABLE hive_reputation_building (
    hive_id TEXT PRIMARY KEY,
    payments_routed_through INTEGER DEFAULT 0,
    payments_routed_for INTEGER DEFAULT 0,
    volume_routed_through_sats INTEGER DEFAULT 0,
    volume_routed_for_sats INTEGER DEFAULT 0,
    fees_earned_sats INTEGER DEFAULT 0,
    fees_paid_sats INTEGER DEFAULT 0,
    channels_with_members INTEGER DEFAULT 0,
    avg_success_rate REAL DEFAULT 0.0,
    first_interaction INTEGER,
    last_interaction INTEGER,
    reputation_score REAL DEFAULT 0.0,
    ready_for_revelation BOOLEAN DEFAULT FALSE,

    FOREIGN KEY (hive_id) REFERENCES detected_hives(hive_id)
);
```

---

## 3.5 Stealth-First Detection Strategy

### 3.5.1 Core Principle: Detect Without Revealing

When discovering and analyzing other hives, **never use hive-specific protocol messages**. All detection and initial reputation building must be done through normal Lightning Network activity.

```python
class StealthHiveDetector:
    """Detect hives without revealing our own hive membership."""

    def detect_silently(self) -> List[HiveSignature]:
        """Detect hives using only passive observation and normal routing."""

        methods = [
            # Passive methods - no interaction required
            self.analyze_gossip_patterns,       # Fee changes, channel opens
            self.analyze_graph_topology,        # Clustering analysis
            self.analyze_historical_data,       # Past routing patterns

            # Active but indistinguishable from normal behavior
            self.probe_via_normal_payments,     # Real payments, realistic amounts
            self.observe_routing_behavior,      # How they route our payments
        ]

        # NEVER USE:
        # - Hive-specific TLV messages
        # - "Are you a hive?" queries
        # - Any custom protocol that reveals hive awareness

        candidates = []
        for method in methods:
            detected = method()
            candidates.extend(detected)

        return self.deduplicate_and_rank(candidates)

    def probe_via_normal_payments(self) -> List[HiveSignature]:
        """Probe using payments that look like normal traffic."""

        # Use economically rational payments
        # - Real payment amounts (not probe-like round numbers)
        # - To destinations we have reason to pay
        # - Through routes that make economic sense

        # Record which nodes cluster together based on:
        # - Internal routing costs
        # - Success rates
        # - Timing patterns

        pass  # Implementation details in stealth probing section
```

### 3.5.2 Information Asymmetry Advantage

**Goal**: Know more about them than they know about us.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    INFORMATION ASYMMETRY MATRIX                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  THEY DON'T KNOW:           │  WE KNOW:                            │
│  • We are a hive            │  • They are a hive                   │
│  • We detected them         │  • Their suspected members           │
│  • We're building rep       │  • Their routing patterns            │
│  • Our hive members         │  • Their fee strategies              │
│  • Our coordinated strategy │  • Their liquidity distribution      │
│                             │  • Their response to market changes  │
│                                                                     │
│  MAINTAIN THIS ADVANTAGE AS LONG AS POSSIBLE                        │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.5.3 Pre-Revelation Reputation Building

Before revealing hive membership, build a solid routing reputation through normal activity.

```python
class PreRevelationReputationBuilder:
    """Build reputation with detected hives before revealing ourselves."""

    # Thresholds for "ready to reveal"
    MIN_ROUTING_DAYS = 90
    MIN_PAYMENTS_ROUTED = 100
    MIN_VOLUME_SATS = 10_000_000
    MIN_SUCCESS_RATE = 0.95
    MIN_CHANNEL_INTERACTIONS = 3

    def build_reputation_silently(self, hive_id: str):
        """Build reputation through normal routing behavior."""

        hive_members = self.get_hive_members(hive_id)

        # Strategy 1: Be a reliable routing partner
        # - Accept their HTLCs promptly
        # - Maintain good liquidity on channels with them
        # - Set competitive (but not suspicious) fees

        # Strategy 2: Route payments through them
        # - Use them for legitimate routing when economical
        # - Builds mutual familiarity
        # - Reveals their reliability to us

        # Strategy 3: Open strategic channels
        # - To members that make economic sense anyway
        # - Don't open to all members (obvious coordination)
        # - Stagger opens over weeks/months

        for member in hive_members[:3]:  # Start with 1-3 members
            if self.channel_makes_economic_sense(member):
                # Open channel through normal process
                # cl-revenue-ops will set fees normally
                self.schedule_organic_channel_open(member)

    def check_ready_for_revelation(self, hive_id: str) -> RevelationReadiness:
        """Check if we've built sufficient reputation to reveal."""

        stats = self.get_reputation_stats(hive_id)

        checks = {
            "sufficient_time": stats.days_interacting >= self.MIN_ROUTING_DAYS,
            "sufficient_volume": stats.volume_routed_sats >= self.MIN_VOLUME_SATS,
            "sufficient_payments": stats.payments_routed >= self.MIN_PAYMENTS_ROUTED,
            "good_success_rate": stats.success_rate >= self.MIN_SUCCESS_RATE,
            "multiple_touchpoints": stats.channel_interactions >= self.MIN_CHANNEL_INTERACTIONS,
        }

        ready = all(checks.values())

        # Additional check: Is revelation economically rational?
        revelation_benefit = self.estimate_revelation_benefit(hive_id)
        checks["positive_ev"] = revelation_benefit > 0

        return RevelationReadiness(
            hive_id=hive_id,
            ready=ready and checks["positive_ev"],
            checks=checks,
            stats=stats,
            estimated_benefit=revelation_benefit,
            recommendation=self.get_revelation_recommendation(checks)
        )

    def estimate_revelation_benefit(self, hive_id: str) -> int:
        """Estimate sats benefit/cost of revealing hive membership."""

        benefits = 0
        costs = 0

        # Potential benefits:
        # - Reduced fees from cooperative relationship
        # - Better routing priority
        # - Intelligence sharing
        # - Coordinated defense

        # Potential costs:
        # - Targeted competition
        # - Loss of information asymmetry
        # - Federation obligations

        hive = self.get_hive(hive_id)

        if hive.classification in ["hostile", "parasitic"]:
            # Never reveal to hostile hives
            return -float('inf')

        if hive.classification == "predatory":
            # Too early, keep building reputation
            return -1_000_000

        # For competitive/neutral hives, calculate based on potential
        if hive.classification in ["competitive", "neutral"]:
            potential_fee_savings = self.estimate_fee_savings(hive_id)
            potential_volume_increase = self.estimate_volume_increase(hive_id)
            competition_risk = self.estimate_competition_risk(hive_id)

            benefits = potential_fee_savings + potential_volume_increase
            costs = competition_risk

        return benefits - costs
```

### 3.5.4 Graduated Revelation Protocol

When ready to reveal, do so gradually:

```python
class GraduatedRevelation:
    """Reveal hive membership in controlled stages."""

    REVELATION_STAGES = [
        "hidden",           # No indication we're a hive
        "hinted",           # Subtle signals (e.g., coordinated but deniable)
        "acknowledged",     # Respond to their query but don't initiate
        "partial_reveal",   # Reveal some members, not all
        "full_reveal",      # Complete hive disclosure
    ]

    def execute_graduated_revelation(
        self,
        hive_id: str,
        target_stage: str
    ) -> RevelationResult:
        """Execute revelation to specified stage."""

        current_stage = self.get_current_revelation_stage(hive_id)

        if self.REVELATION_STAGES.index(target_stage) <= \
           self.REVELATION_STAGES.index(current_stage):
            return RevelationResult(success=False, reason="cannot_de-escalate")

        # Execute stage-appropriate revelation
        if target_stage == "hinted":
            # Allow some coordination to be visible
            # But maintain plausible deniability
            self.allow_visible_coordination(hive_id)

        elif target_stage == "acknowledged":
            # If they query us, acknowledge
            # But don't initiate contact
            self.set_acknowledgment_policy(hive_id, respond_only=True)

        elif target_stage == "partial_reveal":
            # Reveal 1-2 members as "contacts"
            # Keep rest of hive hidden
            contacts = self.select_contact_nodes(count=2)
            self.reveal_as_contacts(hive_id, contacts)

        elif target_stage == "full_reveal":
            # Full hive introduction
            # Only after extensive reputation building
            if not self.check_ready_for_revelation(hive_id).ready:
                return RevelationResult(success=False, reason="not_ready")

            self.initiate_full_introduction(hive_id)

        self.update_revelation_status(hive_id, target_stage)
        return RevelationResult(success=True, new_stage=target_stage)

    def respond_to_their_query(
        self,
        from_node: str,
        query_type: str
    ) -> Optional[Response]:
        """Respond to their hive query based on our policy."""

        their_hive = self.get_hive_for_node(from_node)

        if their_hive is None:
            # Unknown node asking - be cautious
            return self.deny_hive_membership()

        our_policy = self.get_revelation_stage(their_hive.hive_id)

        if our_policy == "hidden":
            # Deny everything
            return Response(
                is_hive_member=False,
                reason="We are independent nodes"
            )

        elif our_policy == "acknowledged":
            # Acknowledge but minimal info
            return Response(
                is_hive_member=True,
                hive_id=None,  # Don't reveal hive ID yet
                member_count=None,
                contact_node=self.our_primary_contact()
            )

        elif our_policy in ["partial_reveal", "full_reveal"]:
            # Provide appropriate level of detail
            return self.generate_appropriate_response(their_hive, our_policy)

        return self.deny_hive_membership()
```

### 3.5.5 When to Reveal (Decision Framework)

```python
def should_reveal_to_hive(self, hive_id: str) -> RevelationDecision:
    """Decide whether to reveal hive membership."""

    hive = self.get_hive(hive_id)
    our_rep = self.get_our_reputation_with(hive_id)

    # NEVER reveal to:
    if hive.classification in ["hostile", "parasitic"]:
        return RevelationDecision(
            reveal=False,
            reason="hostile_classification",
            recommendation="maintain_hidden_indefinitely"
        )

    # NOT YET - keep building reputation:
    if hive.classification == "predatory":
        return RevelationDecision(
            reveal=False,
            reason="still_predatory_classification",
            recommendation="continue_silent_reputation_building"
        )

    # CONSIDER revealing if:
    if hive.classification == "competitive":
        if our_rep.days_interacting >= 90 and our_rep.success_rate >= 0.95:
            return RevelationDecision(
                reveal=True,
                reason="sufficient_competitive_reputation",
                recommendation="graduated_reveal_to_acknowledged",
                target_stage="acknowledged"
            )

    # LIKELY reveal if:
    if hive.classification == "neutral":
        if our_rep.ready_for_revelation:
            return RevelationDecision(
                reveal=True,
                reason="ready_for_cooperative_relationship",
                recommendation="graduated_reveal_to_partial",
                target_stage="partial_reveal"
            )

    # DEFINITELY reveal if:
    if hive.classification == "cooperative":
        # They've proven themselves, full reveal makes sense
        return RevelationDecision(
            reveal=True,
            reason="cooperative_relationship_established",
            recommendation="proceed_to_full_reveal",
            target_stage="full_reveal"
        )

    return RevelationDecision(
        reveal=False,
        reason="default_caution",
        recommendation="continue_observation"
    )
```

---

## 3.6 Stealth Strategy Security Hardening

The stealth-first approach has critical vulnerabilities. This section addresses them.

### 3.6.1 Core Assumption: Mutual Detection

**CRITICAL**: Stealth is a **bonus**, not a security mechanism. Always assume sophisticated hives have already detected us.

```python
class MutualDetectionAssumption:
    """
    Security model: Assume they know about us.

    Why:
    - They're running the same detection algorithms we are
    - Our hive behavior (zero-fee internal, coordinated actions) is visible in gossip
    - Any sophisticated attacker will detect us before we detect them
    - Relying on stealth creates dangerous overconfidence

    Implication:
    - Stealth operations are for intelligence gathering, not security
    - All defenses must assume we are already known
    - Information asymmetry is hoped for, never relied upon
    """

    SECURITY_POSTURE = "assume_detected"

    def plan_defense(self, threat: str) -> DefensePlan:
        """Plan defense assuming they know about us."""

        # WRONG: "They don't know we're a hive, so we're safe"
        # RIGHT: "They probably know, so we must be prepared"

        return DefensePlan(
            assume_detected=True,
            prepare_for_targeted_attack=True,
            dont_rely_on_stealth_for_security=True
        )
```

### 3.6.2 Remove Detectable Fee Discrimination

**Problem**: Charging predatory hives 1.5x fees reveals our awareness of them.

**Fix**: Use identical fees for all hives, differentiate through limits and monitoring only.

```python
# BEFORE (Detectable):
DEFAULT_POLICIES = {
    "predatory": HivePolicy(fee_multiplier=1.5),  # They can detect this!
    "competitive": HivePolicy(fee_multiplier=1.2),
    "neutral": HivePolicy(fee_multiplier=1.0),
}

# AFTER (Undetectable):
DEFAULT_POLICIES = {
    "predatory": HivePolicy(
        fee_multiplier=1.0,              # Same fees as everyone
        max_htlc_exposure_sats=2_000_000, # Limit exposure instead
        enhanced_monitoring=True,         # Watch closely
        internal_risk_score=0.8,          # Track risk internally
    ),
    "competitive": HivePolicy(
        fee_multiplier=1.0,              # Same fees
        max_htlc_exposure_sats=5_000_000,
        enhanced_monitoring=True,
        internal_risk_score=0.5,
    ),
    "neutral": HivePolicy(
        fee_multiplier=1.0,
        max_htlc_exposure_sats=10_000_000,
        enhanced_monitoring=False,
        internal_risk_score=0.2,
    ),
}

class UndetectableDifferentiation:
    """Differentiate treatment without revealing awareness."""

    # What they CAN'T detect (safe to differentiate):
    UNDETECTABLE_MEASURES = [
        "max_htlc_exposure",        # Internal limit, invisible to them
        "internal_risk_scoring",    # Our internal tracking
        "monitoring_intensity",     # How closely we watch
        "rebalancing_priority",     # Which channels we prioritize
        "channel_acceptance_delay", # Slightly slower acceptance
    ]

    # What they CAN detect (must be uniform):
    DETECTABLE_MEASURES = [
        "fee_rates",               # Visible in gossip and routing
        "base_fees",               # Visible in gossip
        "channel_acceptance",      # Pattern of accepts/rejects
        "htlc_response_time",      # Must be consistent
        "routing_availability",    # Must route for them
    ]
```

### 3.6.3 Consistent Denial Policy

**Problem**: Differential responses to hive queries reveal our classification system.

**Fix**: Always deny initially, regardless of our internal classification.

```python
class ConsistentDenialPolicy:
    """Respond identically to all hive queries until WE initiate revelation."""

    def respond_to_hive_query(self, from_node: str, query: HiveQuery) -> Response:
        """
        CRITICAL: Response must be identical regardless of:
        - Who is asking
        - What we know about them
        - Our internal classification of them

        Differential responses reveal our intelligence.
        """

        their_hive = self.get_hive_for_node(from_node)  # We know this
        our_classification = their_hive.classification if their_hive else None

        # WRONG: Different responses based on classification
        # if our_classification == "hostile":
        #     return deny_completely()
        # elif our_classification == "cooperative":
        #     return acknowledge()

        # RIGHT: Identical response to everyone
        # Until WE decide to initiate revelation

        if not self.have_we_initiated_revelation(their_hive):
            # We haven't revealed to them yet - deny uniformly
            return Response(
                is_hive_member=False,
                message="We operate as independent nodes",
                # Identical response regardless of who asks
            )
        else:
            # We previously initiated revelation to this hive
            return self.get_appropriate_response_for_stage(their_hive)

    def initiate_revelation(self, hive_id: str, stage: str) -> bool:
        """
        WE control when revelation happens.
        They cannot trigger revelation by querying us.
        """

        # Only reveal when we decide to, not when they ask
        if not self.revelation_conditions_met(hive_id):
            return False

        # Record that we initiated
        self.record_revelation_initiated(hive_id, stage)

        # Now send revelation message (we initiate, not respond)
        self.send_revelation_message(hive_id, stage)

        return True
```

### 3.6.4 Anti-Gaming: Randomized Upgrade Criteria

**Problem**: Published, deterministic criteria let attackers game the classification system.

**Fix**: Add randomization and hidden factors to upgrade requirements.

```python
class AntiGamingClassification:
    """Make classification gaming impractical."""

    # Base requirements (public knowledge)
    BASE_REQUIREMENTS = {
        "predatory_to_competitive": {
            "min_days": 60,
            "no_hostile_acts": True,
            "balanced_economics": True,
        },
        "competitive_to_neutral": {
            "min_days": 90,
            "positive_score_min": 5.0,
        },
    }

    # Hidden randomization (attacker can't know)
    RANDOMIZATION = {
        "day_variance": 0.3,        # ±30% on day requirements
        "score_variance": 0.2,      # ±20% on score requirements
        "random_delay_days": (0, 30),  # 0-30 day random delay after meeting criteria
    }

    def check_upgrade_eligible(
        self,
        hive_id: str,
        from_class: str,
        to_class: str
    ) -> UpgradeEligibility:
        """Check if upgrade is allowed with randomization."""

        base_req = self.BASE_REQUIREMENTS.get(f"{from_class}_to_{to_class}")
        hive = self.get_hive(hive_id)

        # Apply randomization (seeded per-hive for consistency)
        random.seed(hash(hive_id + self.secret_salt))

        actual_min_days = base_req["min_days"] * (1 + random.uniform(
            -self.RANDOMIZATION["day_variance"],
            self.RANDOMIZATION["day_variance"]
        ))

        random_delay = random.randint(*self.RANDOMIZATION["random_delay_days"])

        # Check base criteria
        days_observed = self.days_since_detection(hive_id)

        if days_observed < actual_min_days:
            return UpgradeEligibility(
                eligible=False,
                reason="insufficient_observation_time",
                # Don't reveal actual requirement
                message="Continue demonstrating positive behavior"
            )

        # Add random delay even after criteria met
        if not self.random_delay_passed(hive_id, random_delay):
            return UpgradeEligibility(
                eligible=False,
                reason="additional_observation_required",
                message="Continue demonstrating positive behavior"
            )

        # Check ungameable factors
        ungameable = self.check_ungameable_factors(hive_id)
        if not ungameable.passed:
            return UpgradeEligibility(
                eligible=False,
                reason=ungameable.reason,
                message="Classification requirements not met"
            )

        return UpgradeEligibility(eligible=True)

    def check_ungameable_factors(self, hive_id: str) -> UngameableCheck:
        """Check factors that attackers cannot easily game."""

        checks = {}

        # Factor 1: Network-wide reputation (requires community trust)
        # Attacker would need to deceive entire network, not just us
        network_rep = self.get_network_wide_reputation(hive_id)
        checks["network_reputation"] = network_rep > 0.5

        # Factor 2: Third-party attestations (from our federated hives)
        # Attacker would need to deceive multiple independent hives
        attestations = self.get_federated_attestations(hive_id)
        checks["third_party_trust"] = len(attestations) >= 1

        # Factor 3: Historical consistency (can't fake history)
        # Nodes must have existed for extended period
        avg_node_age = self.get_avg_member_age_days(hive_id)
        checks["historical_presence"] = avg_node_age > 180

        # Factor 4: Economic skin in the game (costly to fake)
        # Must have significant real routing volume with diverse parties
        routing_stats = self.get_routing_statistics(hive_id)
        checks["economic_activity"] = (
            routing_stats.total_volume > 100_000_000 and
            routing_stats.unique_counterparties > 50
        )

        # Factor 5: Behavioral consistency (hard to maintain fake persona)
        # Must not show suspicious behavior variance
        behavior_variance = self.calculate_behavior_variance(hive_id)
        checks["behavioral_consistency"] = behavior_variance < 0.3

        passed = all(checks.values())

        return UngameableCheck(
            passed=passed,
            checks=checks,
            reason=None if passed else self.get_failure_reason(checks)
        )
```

### 3.6.5 Deadlock-Breaking Mechanism

**Problem**: Two hives using identical stealth strategies create permanent deadlock.

**Fix**: Automatic deadlock detection and resolution protocol.

```python
class DeadlockBreaker:
    """Detect and break mutual-predatory deadlocks."""

    # Deadlock detection thresholds
    DEADLOCK_INDICATORS = {
        "mutual_predatory_days": 90,      # Both predatory for 90+ days
        "no_hostile_acts_days": 60,        # Neither acted hostile
        "positive_routing_history": True,  # Route each other's payments fine
        "economic_balance_ok": True,       # No extraction pattern
    }

    def detect_deadlock(self, hive_id: str) -> Optional[Deadlock]:
        """Detect if we're in a mutual-predatory deadlock."""

        hive = self.get_hive(hive_id)

        # Only check hives we've classified as predatory for a while
        if hive.classification != "predatory":
            return None

        days_as_predatory = self.days_at_classification(hive_id, "predatory")
        if days_as_predatory < self.DEADLOCK_INDICATORS["mutual_predatory_days"]:
            return None

        # Check if this looks like a deadlock (good behavior, no progress)
        indicators = {
            "long_duration": days_as_predatory >= 90,
            "no_hostile_acts": self.count_hostile_acts(hive_id, days=60) == 0,
            "positive_routing": self.routing_success_rate(hive_id) > 0.9,
            "economic_balance": self.is_economically_balanced(hive_id),
        }

        if all(indicators.values()):
            return Deadlock(
                hive_id=hive_id,
                duration_days=days_as_predatory,
                indicators=indicators,
                likely_cause="mutual_stealth_strategy"
            )

        return None

    def break_deadlock(self, deadlock: Deadlock) -> DeadlockResolution:
        """Attempt to break a detected deadlock."""

        hive_id = deadlock.hive_id

        # Option 1: Unilateral upgrade with caution
        # We take the risk of upgrading first
        resolution_strategy = self.select_resolution_strategy(deadlock)

        if resolution_strategy == "cautious_upgrade":
            return self.execute_cautious_upgrade(hive_id)

        elif resolution_strategy == "probe_their_stance":
            return self.execute_stance_probe(hive_id)

        elif resolution_strategy == "third_party_introduction":
            return self.request_third_party_intro(hive_id)

        elif resolution_strategy == "economic_signal":
            return self.send_economic_signal(hive_id)

    def execute_cautious_upgrade(self, hive_id: str) -> DeadlockResolution:
        """Upgrade classification with enhanced monitoring."""

        # Upgrade from predatory to competitive
        # But with extra safeguards

        self.upgrade_classification(
            hive_id=hive_id,
            new_classification="competitive",
            reason="deadlock_break_attempt",
            safeguards={
                "enhanced_monitoring": True,
                "instant_downgrade_on_hostile": True,
                "economic_trip_wire": 0.7,  # Downgrade if balance drops below 0.7
                "review_after_days": 30,
            }
        )

        return DeadlockResolution(
            strategy="cautious_upgrade",
            action_taken="upgraded_to_competitive",
            safeguards_enabled=True
        )

    def execute_stance_probe(self, hive_id: str) -> DeadlockResolution:
        """
        Probe their classification of us without revealing ours.

        Method: Subtle behavioral changes that a friendly hive would respond to.
        """

        # Signal 1: Slightly improve routing priority for their payments
        # A friendly hive monitoring us would notice

        # Signal 2: Open a small channel to one of their peripheral members
        # Could be interpreted as normal business OR as outreach

        # Signal 3: Route a slightly larger payment through them
        # Tests their treatment of us

        self.execute_stance_probe_signals(hive_id)

        # Monitor for response over 14 days
        self.schedule_probe_response_check(hive_id, days=14)

        return DeadlockResolution(
            strategy="stance_probe",
            action_taken="probe_signals_sent",
            monitoring_period_days=14
        )

    def send_economic_signal(self, hive_id: str) -> DeadlockResolution:
        """
        Send economic signal that demonstrates goodwill.

        More costly than words, but not a full revelation.
        """

        # Deliberately route profitable payments through them
        # This costs us fees but signals cooperative intent

        signal_budget = 10000  # sats we're willing to "spend" on signaling

        self.route_goodwill_payments(
            through_hive=hive_id,
            budget_sats=signal_budget,
            duration_days=7
        )

        return DeadlockResolution(
            strategy="economic_signal",
            action_taken="goodwill_payments_routed",
            cost_sats=signal_budget
        )

    def request_third_party_intro(self, hive_id: str) -> DeadlockResolution:
        """Request introduction through a mutually trusted third party."""

        # Find federated hives that might know both of us
        our_federates = self.get_federated_hives()

        potential_introducers = []
        for federate in our_federates:
            # Ask federate if they have relationship with target
            if self.federate_knows_hive(federate, hive_id):
                potential_introducers.append(federate)

        if potential_introducers:
            # Request introduction through most trusted introducer
            introducer = self.select_best_introducer(potential_introducers)
            self.request_introduction(introducer, hive_id)

            return DeadlockResolution(
                strategy="third_party_introduction",
                action_taken="introduction_requested",
                introducer=introducer.hive_id
            )

        return DeadlockResolution(
            strategy="third_party_introduction",
            action_taken="no_introducer_available",
            fallback="try_economic_signal"
        )
```

### 3.6.6 Limit Intelligence Leakage

**Problem**: Routing through predatory hives for "intelligence" gives them intelligence about us.

**Fix**: Minimize direct interaction, use passive observation instead.

```python
class MinimalInteractionPolicy:
    """Minimize intelligence leakage during observation phase."""

    def get_observation_policy(self, classification: str) -> ObservationPolicy:
        """Get observation policy that minimizes our exposure."""

        if classification == "predatory":
            return ObservationPolicy(
                # DON'T actively probe
                active_probing=False,

                # DON'T route through them for intelligence
                route_through_for_intel=False,

                # DON'T open channels to them
                initiate_channels=False,

                # DO observe passively
                passive_observation=True,

                # DO monitor gossip for their behavior
                gossip_monitoring=True,

                # DO accept their routing (earn fees, observe)
                accept_their_routing=True,

                # DO accept channel opens (with limits)
                accept_channel_opens=True,
                accept_channel_max_size=5_000_000,

                # Use third-party observation when possible
                use_third_party_observation=True,
            )

        elif classification == "competitive":
            return ObservationPolicy(
                active_probing=False,          # Still don't probe
                route_through_for_intel=False, # Don't route for intel
                initiate_channels=True,        # Can initiate if economic
                passive_observation=True,
                gossip_monitoring=True,
                accept_their_routing=True,
                accept_channel_opens=True,
                accept_channel_max_size=20_000_000,
                use_third_party_observation=True,
            )

        # For neutral and above, normal interaction is fine
        return ObservationPolicy.default()

    def observe_via_third_party(self, hive_id: str) -> ThirdPartyObservation:
        """
        Observe hive behavior through third parties.

        Less intelligence leakage than direct interaction.
        """

        # Ask federated hives about their experience
        federate_reports = []
        for federate in self.get_federated_hives():
            if self.federate_interacts_with(federate, hive_id):
                report = self.request_hive_report(federate, hive_id)
                federate_reports.append(report)

        # Analyze network-wide reputation data
        network_data = self.get_network_reputation_data(hive_id)

        # Monitor their behavior toward neutral third parties
        third_party_observations = self.observe_their_third_party_behavior(hive_id)

        return ThirdPartyObservation(
            federate_reports=federate_reports,
            network_reputation=network_data,
            third_party_behavior=third_party_observations,
            # We learned about them without them learning about us
            our_exposure="minimal"
        )
```

### 3.6.7 Economic Trip Wires

**Problem**: During reputation building, they can extract value while we wait.

**Fix**: Automatic defensive triggers if economic extraction detected.

```python
class EconomicTripWires:
    """Automatic defense triggers during observation period."""

    # Trip wire thresholds
    TRIP_WIRES = {
        # If they're taking more than 3x what they give, something's wrong
        "revenue_imbalance_ratio": 3.0,

        # If we're losing money on the relationship
        "net_loss_threshold_sats": -50_000,

        # If they're draining our channels without reciprocal flow
        "liquidity_drain_pct": 0.7,  # 70% drain without return

        # If they're probing us extensively
        "probe_count_threshold": 20,  # per week

        # If they're jamming our channels
        "htlc_failure_rate_threshold": 0.3,  # 30% failure rate
    }

    def check_trip_wires(self, hive_id: str) -> List[TripWireAlert]:
        """Check if any economic trip wires have been triggered."""

        alerts = []

        # Check revenue imbalance
        revenue_to_them = self.get_revenue_to_hive(hive_id, days=30)
        revenue_from_them = self.get_revenue_from_hive(hive_id, days=30)

        if revenue_from_them > 0:
            ratio = revenue_to_them / revenue_from_them
            if ratio > self.TRIP_WIRES["revenue_imbalance_ratio"]:
                alerts.append(TripWireAlert(
                    type="revenue_imbalance",
                    severity="warning",
                    details=f"Revenue ratio {ratio:.1f}:1 in their favor",
                    action="increase_monitoring"
                ))

        # Check net position
        net_position = revenue_from_them - revenue_to_them
        if net_position < self.TRIP_WIRES["net_loss_threshold_sats"]:
            alerts.append(TripWireAlert(
                type="net_loss",
                severity="critical",
                details=f"Net loss of {abs(net_position)} sats",
                action="reduce_exposure"
            ))

        # Check liquidity drain
        liquidity_stats = self.get_liquidity_flow(hive_id, days=30)
        if liquidity_stats.drain_ratio > self.TRIP_WIRES["liquidity_drain_pct"]:
            alerts.append(TripWireAlert(
                type="liquidity_drain",
                severity="critical",
                details=f"Channel drain at {liquidity_stats.drain_ratio:.0%}",
                action="close_channels"
            ))

        # Check for excessive probing
        probe_count = self.count_likely_probes(hive_id, days=7)
        if probe_count > self.TRIP_WIRES["probe_count_threshold"]:
            alerts.append(TripWireAlert(
                type="excessive_probing",
                severity="warning",
                details=f"{probe_count} likely probes in 7 days",
                action="flag_as_suspicious"
            ))

        return alerts

    def handle_trip_wire_alert(self, alert: TripWireAlert, hive_id: str):
        """Handle a triggered trip wire."""

        if alert.severity == "critical":
            # Immediate defensive action
            if alert.action == "reduce_exposure":
                self.reduce_htlc_limits(hive_id)
                self.pause_channel_accepts(hive_id)

            elif alert.action == "close_channels":
                self.schedule_graceful_channel_closure(hive_id)

            # Reset classification timer
            self.reset_classification_progress(hive_id)

            # Log for pattern analysis
            self.log_trip_wire_event(hive_id, alert)

        elif alert.severity == "warning":
            # Increased monitoring
            self.increase_monitoring(hive_id)
            self.extend_observation_period(hive_id, days=30)
```

### 3.6.8 Defense Posture: Always Prepared

**Problem**: Stealth creates false confidence; we're unprepared when detected.

**Fix**: Maintain defensive posture regardless of stealth status.

```python
class DefensivePosture:
    """
    Maintain defenses assuming we are detected.

    Stealth is a bonus for intelligence gathering.
    Security comes from defensive preparation, not hiding.
    """

    def get_defensive_readiness(self) -> DefensiveReadiness:
        """Assess our defensive readiness assuming we're known."""

        return DefensiveReadiness(
            # Can we withstand coordinated fee attack?
            fee_attack_resilience=self.assess_fee_attack_resilience(),

            # Can we withstand liquidity drain?
            liquidity_drain_resilience=self.assess_liquidity_resilience(),

            # Can we withstand channel jamming?
            jamming_resilience=self.assess_jamming_resilience(),

            # Do we have defensive alliances?
            alliance_strength=self.assess_alliance_strength(),

            # Can we respond quickly to attacks?
            response_capability=self.assess_response_capability(),
        )

    def prepare_for_being_known(self, detected_hive_id: str):
        """
        Prepare defenses as if this hive knows about us.

        Called for every detected hive, regardless of our stealth status.
        """

        hive = self.get_hive(detected_hive_id)

        # Assess threat level
        threat = self.assess_threat_if_they_know(hive)

        # Prepare proportional defenses
        if threat.level == "high":
            self.prepare_high_threat_defenses(hive)
        elif threat.level == "medium":
            self.prepare_medium_threat_defenses(hive)
        else:
            self.prepare_basic_defenses(hive)

    def prepare_high_threat_defenses(self, hive: DetectedHive):
        """Prepare for high-threat hive that knows about us."""

        defenses = [
            # Limit exposure to their nodes
            self.set_htlc_limits_for_hive(hive.hive_id, max_sats=1_000_000),

            # Prepare coordinated response with allies
            self.alert_federated_hives(hive.hive_id, threat_level="elevated"),

            # Prepare fee response strategy
            self.prepare_fee_response_plan(hive.hive_id),

            # Prepare channel closure strategy
            self.prepare_graceful_exit_plan(hive.hive_id),

            # Monitor for attack patterns
            self.enable_attack_pattern_detection(hive.hive_id),
        ]

        return defenses
```

### 3.6.9 Summary: Hardened Stealth Strategy

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    HARDENED STEALTH STRATEGY                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  CORE PRINCIPLE:                                                        │
│  Stealth is for intelligence. Security is from preparation.             │
│                                                                         │
│  KEY CHANGES:                                                           │
│  ✓ Assume mutual detection - don't rely on stealth for safety          │
│  ✓ No detectable fee discrimination - same fees, different limits       │
│  ✓ Consistent denial - same response regardless of who asks             │
│  ✓ Randomized criteria - attackers can't game deterministic rules       │
│  ✓ Deadlock breaking - automatic resolution of mutual-predatory         │
│  ✓ Minimal interaction - observe passively, don't leak intelligence     │
│  ✓ Economic trip wires - automatic defense on extraction patterns       │
│  ✓ Always prepared - defenses ready regardless of stealth status        │
│                                                                         │
│  STEALTH PROVIDES:                                                      │
│  • Intelligence advantage (maybe)                                       │
│  • First-mover advantage (maybe)                                        │
│  • Nothing else - don't rely on it                                      │
│                                                                         │
│  SECURITY PROVIDES:                                                     │
│  • Resilience to attack                                                 │
│  • Rapid response capability                                            │
│  • Allied coordination                                                  │
│  • Economic trip wires                                                  │
│  • Everything we actually need                                          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Hive Classification

### 4.1 Classification Categories

| Category | Description | Default Policy | Starting Point |
|----------|-------------|----------------|----------------|
| `predatory` | **Default for all detected hives** - Assumed competing for resources | Restricted | Yes |
| `competitive` | Competing for same corridors, demonstrated fair play | Cautious | No |
| `neutral` | Balanced relationship, no positive or negative bias | Standard | No |
| `cooperative` | Mutually beneficial interactions verified | Favorable | No |
| `federated` | Formal alliance with verified trust + stakes | Allied | No |
| `hostile` | Actively harmful behavior confirmed | Defensive | No |
| `parasitic` | Free-riding on infrastructure without reciprocity | Blocked | No |

**Key Change**: There is no "unknown" or "observed" category. All hives are immediately classified as `predatory` upon detection. This forces us to:
- Never extend trust prematurely
- Treat every new hive as a competitor
- Require proof of good behavior before upgrading

### 4.2 Classification Criteria

#### 4.2.1 Behavioral Indicators

**Positive Indicators** (toward cooperative):
- Reciprocal channel opens
- Fair fee pricing (not undercutting)
- Route reliability (low failure rate)
- Timely HTLC resolution
- Balanced liquidity flow

**Negative Indicators** (toward hostile):
- Coordinated fee undercutting
- Channel jamming patterns
- Probe attacks from multiple members
- Forced closure campaigns
- Liquidity drain without reciprocity

```python
class BehaviorAnalyzer:
    POSITIVE_SIGNALS = {
        "reciprocal_opens": 2.0,
        "fair_pricing": 1.5,
        "route_reliability": 1.0,
        "balanced_flow": 1.0,
        "timely_htlc": 0.5,
    }

    NEGATIVE_SIGNALS = {
        "fee_undercutting": -2.0,
        "channel_jamming": -3.0,
        "probe_attacks": -2.5,
        "forced_closures": -3.0,
        "liquidity_drain": -2.0,
        "sybil_behavior": -4.0,
    }

    def calculate_behavior_score(self, hive_id: str, days: int = 30) -> float:
        events = self.get_hive_events(hive_id, days)
        score = 0.0
        for event in events:
            if event.type in self.POSITIVE_SIGNALS:
                score += self.POSITIVE_SIGNALS[event.type]
            elif event.type in self.NEGATIVE_SIGNALS:
                score += self.NEGATIVE_SIGNALS[event.type]
        return score
```

#### 4.2.2 Economic Analysis

```python
def analyze_economic_relationship(self, hive_id: str) -> EconomicProfile:
    """Analyze value exchange with another hive."""

    # Revenue we earn from routing their payments
    revenue_from = self.calculate_revenue_from_hive(hive_id)

    # Revenue they earn from routing our payments
    revenue_to = self.calculate_revenue_to_hive(hive_id)

    # Channel capacity we provide to them
    capacity_to = self.calculate_capacity_provided(hive_id)

    # Channel capacity they provide to us
    capacity_from = self.calculate_capacity_received(hive_id)

    # Calculate balance
    revenue_ratio = revenue_from / max(revenue_to, 1)
    capacity_ratio = capacity_from / max(capacity_to, 1)

    return EconomicProfile(
        revenue_balance=revenue_ratio,
        capacity_balance=capacity_ratio,
        is_parasitic=revenue_ratio < 0.2 and capacity_ratio < 0.3,
        is_predatory=revenue_ratio < 0.1 and capacity_to > 0,
        is_mutual=0.5 < revenue_ratio < 2.0 and 0.5 < capacity_ratio < 2.0
    )
```

### 4.3 Classification State Machine

```
                    DETECTED
                        │
                        ▼
                 ┌──────────────┐
                 │  PREDATORY   │◄────────────────────────────┐
                 │  (default)   │                             │
                 └──────┬───────┘                             │
                        │                                     │
           60 days, no hostile acts,                    downgrade
           balanced economics                                 │
                        │                                     │
                        ▼                                     │
                 ┌──────────────┐                      ┌──────┴───────┐
                 │ COMPETITIVE  │                      │   HOSTILE    │
                 │ (fair rival) │                      │  (confirmed  │
                 └──────┬───────┘                      │   attacks)   │
                        │                              └──────────────┘
           90 days, positive score,                           ▲
           reciprocal value                                   │
                        │                              immediate on
                        ▼                              attack detection
                 ┌──────────────┐                             │
                 │   NEUTRAL    │─────────────────────────────┤
                 │ (balanced)   │                             │
                 └──────┬───────┘                             │
                        │                                     │
           180 days, high reliability,                        │
           verified reciprocity                               │
                        │                                     │
                        ▼                                     │
                 ┌──────────────┐                             │
                 │ COOPERATIVE  │─────────────────────────────┤
                 │  (mutual)    │                             │
                 └──────┬───────┘                             │
                        │                                     │
           365 days, formal agreement,                        │
           mutual stake in escrow                             │
                        │                              ┌──────┴───────┐
                        ▼                              │  PARASITIC   │
                 ┌──────────────┐                      │ (free-rider) │
                 │  FEDERATED   │                      └──────────────┘
                 │  (allied)    │                             ▲
                 └──────────────┘                             │
                                                     extraction without
                                                       reciprocity
```

**Transition Rules**:

| From | To | Trigger | Minimum Time |
|------|-----|---------|--------------|
| predatory | competitive | No hostile acts, balanced economics, positive interactions | 60 days |
| predatory | hostile | Confirmed attack or malicious behavior | Immediate |
| predatory | parasitic | Continued extraction, no reciprocity | 30 days |
| competitive | neutral | Positive behavior score > 5.0, reciprocal value exchange | 90 days |
| competitive | predatory | Economic imbalance detected | Immediate |
| neutral | cooperative | High reliability, verified reciprocity, score > 15.0 | 180 days |
| neutral | predatory | Negative behavior or economic extraction | Immediate |
| cooperative | federated | Formal handshake, mutual stake in escrow | 365 days |
| cooperative | predatory | Breach of informal agreement | Immediate |
| federated | cooperative | Minor terms violation, reduced trust | After review |
| federated | hostile | Federation betrayal | Immediate |
| any | hostile | Confirmed attack or malicious behavior | Immediate |
| hostile | predatory | 180 days no hostile acts, economic rebalance | 180 days |

### 4.4 Classification Confidence

```python
def calculate_classification_confidence(
    self,
    hive_id: str,
    classification: str
) -> float:
    """Calculate confidence in current classification."""

    factors = {
        "observation_days": min(self.days_observed(hive_id) / 90, 1.0),
        "interaction_count": min(self.interaction_count(hive_id) / 100, 1.0),
        "behavior_consistency": self.behavior_consistency(hive_id),
        "economic_data_quality": self.economic_data_quality(hive_id),
        "corroboration": self.external_corroboration(hive_id),
    }

    weights = {
        "observation_days": 0.2,
        "interaction_count": 0.2,
        "behavior_consistency": 0.3,
        "economic_data_quality": 0.2,
        "corroboration": 0.1,
    }

    return sum(factors[k] * weights[k] for k in factors)
```

---

## 5. Reputation System

### 5.1 Multi-Dimensional Reputation

Reputation is not a single score but multiple dimensions:

```python
@dataclass
class HiveReputation:
    hive_id: str

    # Core dimensions (0.0 - 1.0 scale)
    reliability: float      # Route success, uptime
    fairness: float         # Pricing, not predatory
    reciprocity: float      # Balanced value exchange
    security: float         # No attacks, clean behavior
    responsiveness: float   # Timely actions, communication

    # Metadata
    sample_size: int        # Number of data points
    last_updated: int       # Unix timestamp
    confidence: float       # Overall confidence in scores

    def overall_score(self) -> float:
        """Weighted overall reputation."""
        weights = {
            "reliability": 0.25,
            "fairness": 0.20,
            "reciprocity": 0.25,
            "security": 0.20,
            "responsiveness": 0.10,
        }
        return sum(
            getattr(self, dim) * weight
            for dim, weight in weights.items()
        )
```

### 5.2 Reputation Calculation

#### 5.2.1 Reliability

```python
def calculate_reliability(self, hive_id: str, days: int = 30) -> float:
    """Calculate reliability based on routing performance."""

    members = self.get_hive_members(hive_id)

    metrics = {
        "route_success_rate": self.avg_route_success(members, days),
        "htlc_resolution_time": self.normalize_htlc_time(members, days),
        "channel_uptime": self.avg_channel_uptime(members, days),
        "forced_closure_rate": 1.0 - self.forced_closure_rate(members, days),
    }

    weights = [0.35, 0.25, 0.25, 0.15]
    return sum(m * w for m, w in zip(metrics.values(), weights))
```

#### 5.2.2 Fairness

```python
def calculate_fairness(self, hive_id: str) -> float:
    """Calculate fairness based on pricing and behavior."""

    factors = {
        # Are their fees reasonable vs network average?
        "fee_reasonableness": self.compare_fees_to_network(hive_id),

        # Do they undercut specifically to steal routes?
        "no_predatory_pricing": 1.0 - self.detect_predatory_pricing(hive_id),

        # Do they honor informal agreements?
        "agreement_adherence": self.agreement_adherence_rate(hive_id),

        # Equal treatment (no discrimination)?
        "equal_treatment": self.equal_treatment_score(hive_id),
    }

    return sum(factors.values()) / len(factors)
```

#### 5.2.3 Reciprocity

```python
def calculate_reciprocity(self, hive_id: str) -> float:
    """Calculate reciprocity in relationship."""

    economic = self.analyze_economic_relationship(hive_id)

    # Ideal ratio is 1.0 (balanced)
    revenue_score = 1.0 - min(abs(1.0 - economic.revenue_balance), 1.0)
    capacity_score = 1.0 - min(abs(1.0 - economic.capacity_balance), 1.0)

    # Check for reciprocal actions
    action_reciprocity = self.action_reciprocity_score(hive_id)

    return (revenue_score * 0.4 + capacity_score * 0.3 + action_reciprocity * 0.3)
```

#### 5.2.4 Security

```python
def calculate_security(self, hive_id: str) -> float:
    """Calculate security score (absence of malicious behavior)."""

    incidents = {
        "probe_attacks": self.count_probe_attacks(hive_id),
        "jamming_attempts": self.count_jamming_attempts(hive_id),
        "sybil_indicators": self.sybil_indicator_count(hive_id),
        "forced_closures_initiated": self.forced_closures_against_us(hive_id),
        "suspicious_htlc_patterns": self.suspicious_htlc_count(hive_id),
    }

    # Each incident type reduces score
    penalties = {
        "probe_attacks": 0.1,
        "jamming_attempts": 0.2,
        "sybil_indicators": 0.3,
        "forced_closures_initiated": 0.15,
        "suspicious_htlc_patterns": 0.1,
    }

    score = 1.0
    for incident_type, count in incidents.items():
        score -= min(count * penalties[incident_type], 0.5)

    return max(score, 0.0)
```

### 5.3 Reputation Decay

Reputation should decay over time without new data:

```python
def apply_reputation_decay(self, reputation: HiveReputation) -> HiveReputation:
    """Apply time-based decay to reputation scores."""

    days_since_update = (time.time() - reputation.last_updated) / 86400

    # Decay factor: lose 10% per 30 days of no data
    decay_factor = 0.9 ** (days_since_update / 30)

    # Pull scores toward neutral (0.5) with decay
    def decay_toward_neutral(score: float) -> float:
        neutral = 0.5
        return neutral + (score - neutral) * decay_factor

    return HiveReputation(
        hive_id=reputation.hive_id,
        reliability=decay_toward_neutral(reputation.reliability),
        fairness=decay_toward_neutral(reputation.fairness),
        reciprocity=decay_toward_neutral(reputation.reciprocity),
        security=decay_toward_neutral(reputation.security),
        responsiveness=decay_toward_neutral(reputation.responsiveness),
        sample_size=reputation.sample_size,
        last_updated=reputation.last_updated,
        confidence=reputation.confidence * decay_factor,
    )
```

### 5.4 Reputation Events

```sql
CREATE TABLE reputation_events (
    id INTEGER PRIMARY KEY,
    hive_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    dimension TEXT NOT NULL,        -- reliability, fairness, etc.
    impact REAL NOT NULL,           -- Positive or negative
    evidence TEXT,                  -- JSON proof
    timestamp INTEGER NOT NULL,
    expires INTEGER,                -- When this event stops affecting score

    FOREIGN KEY (hive_id) REFERENCES detected_hives(hive_id)
);

CREATE INDEX idx_reputation_events_hive ON reputation_events(hive_id, timestamp);
```

---

## 6. Policy Framework

### 6.1 Policy Templates

```python
@dataclass
class HivePolicy:
    policy_id: str
    name: str
    classification: str

    # Fee policies
    fee_multiplier: float           # 1.0 = standard, 0.5 = discount, 2.0 = premium
    min_fee_ppm: int
    max_fee_ppm: int

    # Channel policies
    accept_channel_opens: bool
    initiate_channel_opens: bool
    max_channels_per_member: int
    min_channel_size_sats: int
    max_channel_size_sats: int

    # Routing policies
    route_through: bool             # Allow routing via their nodes
    route_to: bool                  # Allow payments to their nodes
    max_htlc_exposure_sats: int

    # Information sharing
    share_fee_intelligence: bool
    share_hive_detection: bool
    share_reputation_data: bool

    # Monitoring
    enhanced_monitoring: bool
    log_all_interactions: bool
```

### 6.2 Default Policies by Classification

**Note**: All newly detected hives start at `predatory`. There are no "unknown" or "observed" states - assume competition until proven otherwise.

**CRITICAL**: All policies use `fee_multiplier=1.0` to avoid detectable discrimination. Differentiation is done through HTLC limits and internal risk scoring only. See Section 3.6.2.

```python
DEFAULT_POLICIES = {
    # DEFAULT for all newly detected hives
    "predatory": HivePolicy(
        name="Predatory Hive - Restricted (DEFAULT)",
        classification="predatory",
        fee_multiplier=1.0,              # SAME AS EVERYONE - no detectable discrimination
        min_fee_ppm=10,                  # Normal fee bounds
        max_fee_ppm=5000,
        accept_channel_opens=True,       # Accept to build rep, but cautiously
        initiate_channel_opens=False,    # Don't initiate - let them come to us
        max_channels_per_member=1,       # Limit exposure
        min_channel_size_sats=2_000_000, # Only larger channels
        max_channel_size_sats=10_000_000,
        route_through=True,              # Route to earn fees and observe
        route_to=True,
        max_htlc_exposure_sats=2_000_000, # KEY DIFFERENTIATOR - internal limit
        share_fee_intelligence=False,
        share_hive_detection=False,
        share_reputation_data=False,
        enhanced_monitoring=True,
        log_all_interactions=True,
        reveal_hive_status=False,        # NEVER reveal to predatory hives
        internal_risk_score=0.8,         # Internal tracking only
    ),

    # After 60+ days of fair behavior
    "competitive": HivePolicy(
        name="Competitive Hive - Cautious Rival",
        classification="competitive",
        fee_multiplier=1.0,              # SAME AS EVERYONE
        min_fee_ppm=10,
        max_fee_ppm=5000,
        accept_channel_opens=True,
        initiate_channel_opens=True,     # Can initiate if makes economic sense
        max_channels_per_member=2,
        min_channel_size_sats=1_000_000,
        max_channel_size_sats=20_000_000,
        route_through=True,
        route_to=True,
        max_htlc_exposure_sats=5_000_000, # Higher limit than predatory
        share_fee_intelligence=False,
        share_hive_detection=False,
        share_reputation_data=False,
        enhanced_monitoring=True,        # Still monitor
        log_all_interactions=True,
        reveal_hive_status=False,        # Don't reveal yet
        internal_risk_score=0.5,
    ),

    # After 90+ days of positive behavior
    "neutral": HivePolicy(
        name="Neutral Hive - Standard",
        classification="neutral",
        fee_multiplier=1.0,
        min_fee_ppm=10,
        max_fee_ppm=5000,
        accept_channel_opens=True,
        initiate_channel_opens=True,
        max_channels_per_member=2,
        min_channel_size_sats=500_000,
        max_channel_size_sats=50_000_000,
        route_through=True,
        route_to=True,
        max_htlc_exposure_sats=10_000_000,
        share_fee_intelligence=False,
        share_hive_detection=False,
        share_reputation_data=False,
        enhanced_monitoring=False,
        log_all_interactions=False,
    ),

    "cooperative": HivePolicy(
        name="Cooperative Hive - Favorable",
        classification="cooperative",
        fee_multiplier=0.8,
        min_fee_ppm=5,
        max_fee_ppm=5000,
        accept_channel_opens=True,
        initiate_channel_opens=True,
        max_channels_per_member=5,
        min_channel_size_sats=100_000,
        max_channel_size_sats=100_000_000,
        route_through=True,
        route_to=True,
        max_htlc_exposure_sats=50_000_000,
        share_fee_intelligence=True,
        share_hive_detection=True,
        share_reputation_data=False,
        enhanced_monitoring=False,
        log_all_interactions=False,
    ),

    "federated": HivePolicy(
        name="Federated Hive - Allied",
        classification="federated",
        fee_multiplier=0.5,
        min_fee_ppm=0,
        max_fee_ppm=5000,
        accept_channel_opens=True,
        initiate_channel_opens=True,
        max_channels_per_member=10,
        min_channel_size_sats=100_000,
        max_channel_size_sats=500_000_000,
        route_through=True,
        route_to=True,
        max_htlc_exposure_sats=100_000_000,
        share_fee_intelligence=True,
        share_hive_detection=True,
        share_reputation_data=True,
        enhanced_monitoring=False,
        log_all_interactions=False,
    ),

    "hostile": HivePolicy(
        name="Hostile Hive - Defensive",
        classification="hostile",
        fee_multiplier=3.0,
        min_fee_ppm=500,
        max_fee_ppm=5000,
        accept_channel_opens=False,
        initiate_channel_opens=False,
        max_channels_per_member=0,
        min_channel_size_sats=0,
        max_channel_size_sats=0,
        route_through=True,           # Still route (earn fees from them)
        route_to=True,
        max_htlc_exposure_sats=500_000,
        share_fee_intelligence=False,
        share_hive_detection=False,
        share_reputation_data=False,
        enhanced_monitoring=True,
        log_all_interactions=True,
        reveal_hive_status=False,     # NEVER reveal to hostile
    ),

    # Note: "predatory" is defined at the top as the DEFAULT entry point

    "parasitic": HivePolicy(
        name="Parasitic Hive - Blocked",
        classification="parasitic",
        fee_multiplier=5.0,
        min_fee_ppm=1000,
        max_fee_ppm=5000,
        accept_channel_opens=False,
        initiate_channel_opens=False,
        max_channels_per_member=0,
        min_channel_size_sats=0,
        max_channel_size_sats=0,
        route_through=False,          # Block routing
        route_to=False,
        max_htlc_exposure_sats=0,
        share_fee_intelligence=False,
        share_hive_detection=False,
        share_reputation_data=False,
        enhanced_monitoring=True,
        log_all_interactions=True,
    ),
}
```

### 6.3 Policy Application

```python
class HivePolicyEngine:
    def get_policy_for_node(self, node_id: str) -> HivePolicy:
        """Get effective policy for a node."""

        # Check if node belongs to detected hive
        hive = self.get_hive_for_node(node_id)

        if hive is None:
            return DEFAULT_POLICIES["neutral"]  # Non-hive independent node

        # Get hive classification
        classification = hive.classification

        # Check for policy override
        override = self.get_policy_override(hive.hive_id)
        if override:
            return override

        # Default to "predatory" policy if classification unknown
        return DEFAULT_POLICIES.get(classification, DEFAULT_POLICIES["predatory"])

    def should_accept_channel(self, node_id: str, amount_sats: int) -> Tuple[bool, str]:
        """Determine if we should accept a channel open."""
        policy = self.get_policy_for_node(node_id)

        if not policy.accept_channel_opens:
            return False, f"Policy blocks opens from {policy.classification} hives"

        if amount_sats < policy.min_channel_size_sats:
            return False, f"Channel too small for {policy.classification} policy"

        if amount_sats > policy.max_channel_size_sats:
            return False, f"Channel too large for {policy.classification} policy"

        # Check existing channel count
        existing = self.count_channels_with_hive(node_id)
        if existing >= policy.max_channels_per_member:
            return False, f"Max channels reached for this hive member"

        return True, "Accepted"

    def get_fee_for_node(self, node_id: str, base_fee: int) -> int:
        """Calculate fee for routing to/through a node."""
        policy = self.get_policy_for_node(node_id)
        return int(base_fee * policy.fee_multiplier)
```

### 6.4 Policy Override Commands

```
hive-relation-policy set <hive_id> <policy_name>
hive-relation-policy override <hive_id> fee_multiplier=0.5
hive-relation-policy reset <hive_id>
hive-relation-policy list
```

---

## 7. Federation Protocol

### 7.1 Federation Levels

| Level | Trust | Shared Data | Joint Actions |
|-------|-------|-------------|---------------|
| 0: None | Zero | Nothing | None |
| 1: Observer | Low | Public data only | None |
| 2: Partner | Medium | Fee intel, hive detection | Coordinated defense |
| 3: Allied | High | Reputation, strategies | Joint expansion |
| 4: Integrated | Full | Full transparency | Full coordination |

### 7.2 Federation Handshake

#### 7.2.1 Introduction

```json
{
  "type": "federation_introduce",
  "version": 1,
  "from_hive": {
    "hive_id": "hive_abc123",
    "member_count": 5,
    "total_capacity_tier": "large",
    "established_timestamp": 1700000000,
    "admin_contact_node": "03xyz..."
  },
  "proposal": {
    "requested_level": 2,
    "offered_benefits": ["fee_intel_sharing", "coordinated_defense"],
    "requested_benefits": ["fee_intel_sharing", "hive_detection_sharing"],
    "trial_period_days": 30
  },
  "credentials": {
    "attestation": {...},
    "references": []           # Other federated hives that vouch
  },
  "signature": "..."
}
```

#### 7.2.2 Verification Period

Before accepting federation:
1. Observe behavior for `trial_period_days`
2. Verify claimed member count matches detection
3. Check references with existing federated hives
4. Analyze economic relationship potential

```python
def evaluate_federation_proposal(self, proposal: FederationProposal) -> FederationEvaluation:
    """Evaluate a federation proposal."""

    checks = {
        "member_count_verified": self.verify_member_count(proposal),
        "behavior_acceptable": self.check_behavior_history(proposal.from_hive),
        "economic_potential": self.analyze_economic_potential(proposal.from_hive),
        "references_valid": self.verify_references(proposal.credentials.references),
        "no_hostile_history": self.check_hostile_history(proposal.from_hive),
    }

    all_passed = all(checks.values())

    return FederationEvaluation(
        proposal_id=proposal.id,
        checks=checks,
        recommendation="accept" if all_passed else "reject",
        suggested_level=min(proposal.requested_level, 2) if all_passed else 0,
        notes=self.generate_evaluation_notes(checks),
    )
```

#### 7.2.3 Acceptance

```json
{
  "type": "federation_accept",
  "version": 1,
  "proposal_id": "prop_xyz789",
  "from_hive": "hive_def456",
  "to_hive": "hive_abc123",

  "agreement": {
    "level": 2,
    "effective_timestamp": 1705234567,
    "review_timestamp": 1707826567,    // 30 days
    "terms": {
      "share_fee_intel": true,
      "share_hive_detection": true,
      "share_reputation": false,
      "coordinated_defense": true,
      "joint_expansion": false
    },
    "termination_notice_days": 7
  },

  "signatures": {
    "from_hive": "...",
    "to_hive": "..."
  }
}
```

### 7.3 Federation Data Exchange

#### 7.3.1 Fee Intelligence Sharing

```json
{
  "type": "federation_fee_intel",
  "from_hive": "hive_abc123",
  "to_hive": "hive_def456",
  "timestamp": 1705234567,

  "intel": {
    "corridor_fees": [
      {
        "corridor": "exchanges_to_retail",
        "avg_fee_ppm": 150,
        "trend": "increasing",
        "sample_size": 500
      }
    ],
    "competitor_analysis": [
      {
        "hive_id": "hive_hostile1",
        "classification": "predatory",
        "observed_tactics": ["undercutting", "jamming"]
      }
    ]
  },

  "attestation": {...}
}
```

#### 7.3.2 Coordinated Defense

```json
{
  "type": "federation_defense_alert",
  "from_hive": "hive_abc123",
  "timestamp": 1705234567,
  "priority": "high",

  "threat": {
    "threat_type": "coordinated_attack",
    "attacker_hive": "hive_hostile1",
    "attack_vector": "channel_jamming",
    "affected_corridors": ["us_to_eu"],
    "evidence": [...]
  },

  "requested_response": {
    "action": "increase_fees_to_attacker",
    "parameters": {"fee_multiplier": 3.0},
    "duration_hours": 24
  },

  "attestation": {...}
}
```

### 7.4 Federation Management

```sql
CREATE TABLE federations (
    federation_id TEXT PRIMARY KEY,
    our_hive_id TEXT NOT NULL,
    their_hive_id TEXT NOT NULL,
    level INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',   -- pending, active, suspended, terminated
    established_timestamp INTEGER,
    last_review_timestamp INTEGER,
    next_review_timestamp INTEGER,
    terms TEXT,                                -- JSON agreement terms
    trust_score REAL DEFAULT 0.5,

    UNIQUE(our_hive_id, their_hive_id)
);

CREATE TABLE federation_events (
    id INTEGER PRIMARY KEY,
    federation_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    data TEXT,                                 -- JSON
    timestamp INTEGER NOT NULL,

    FOREIGN KEY (federation_id) REFERENCES federations(federation_id)
);
```

### 7.5 Federation Trust Verification

```python
class FederationVerifier:
    """Continuously verify federated hive behavior matches agreements."""

    def verify_federation(self, federation_id: str) -> VerificationResult:
        federation = self.get_federation(federation_id)
        their_hive = federation.their_hive_id

        violations = []

        # Check for terms violations
        if federation.terms.get("no_undercutting"):
            if self.detect_undercutting(their_hive):
                violations.append("undercutting_detected")

        # Check for hostile actions despite federation
        if self.detect_hostile_actions(their_hive):
            violations.append("hostile_action_detected")

        # Check reciprocity
        if federation.level >= 2:
            intel_received = self.count_intel_received(their_hive)
            intel_sent = self.count_intel_sent(their_hive)
            if intel_received < intel_sent * 0.5:
                violations.append("insufficient_reciprocity")

        # Calculate trust adjustment
        trust_delta = -0.1 * len(violations) if violations else 0.02
        new_trust = max(0, min(1, federation.trust_score + trust_delta))

        return VerificationResult(
            federation_id=federation_id,
            violations=violations,
            trust_score=new_trust,
            recommendation=self.get_recommendation(violations, new_trust),
        )

    def get_recommendation(self, violations: List[str], trust: float) -> str:
        if "hostile_action_detected" in violations:
            return "terminate_immediately"
        if trust < 0.3:
            return "suspend_and_review"
        if violations:
            return "warn_and_monitor"
        return "continue"
```

---

## 8. Security Considerations

### 8.1 Sybil Attacks

**Threat**: Attacker creates fake "friendly" hive to gain trust and intelligence.

**Mitigations**:
- Long observation periods before trust upgrade
- Economic analysis (fake hives have low real activity)
- Cross-reference with federated hives
- Channel history verification (new nodes are suspicious)

```python
def detect_sybil_hive(self, hive_id: str) -> SybilRisk:
    """Detect potential sybil hive."""

    members = self.get_hive_members(hive_id)

    risk_factors = {
        # New nodes are suspicious
        "avg_node_age_days": self.avg_node_age(members),

        # Low real routing activity
        "routing_volume": self.total_routing_volume(members),

        # Few external relationships
        "external_channel_ratio": self.external_channel_ratio(members),

        # Concentrated funding sources
        "funding_concentration": self.funding_source_concentration(members),

        # Suspiciously perfect behavior
        "behavior_variance": self.behavior_variance(members),
    }

    # Score each factor
    sybil_score = 0.0
    if risk_factors["avg_node_age_days"] < 90:
        sybil_score += 0.3
    if risk_factors["routing_volume"] < 1_000_000:
        sybil_score += 0.2
    if risk_factors["external_channel_ratio"] < 0.3:
        sybil_score += 0.2
    if risk_factors["funding_concentration"] > 0.8:
        sybil_score += 0.2
    if risk_factors["behavior_variance"] < 0.1:
        sybil_score += 0.1  # Too perfect = suspicious

    return SybilRisk(
        hive_id=hive_id,
        risk_score=sybil_score,
        risk_factors=risk_factors,
        recommendation="high_scrutiny" if sybil_score > 0.5 else "normal",
    )
```

### 8.2 Intelligence Gathering

**Threat**: Hostile hive poses as friendly to gather intelligence.

**Mitigations**:
- Tiered information sharing (more trust = more data)
- Sensitive data only at federation level 3+
- Monitor for data leakage to third parties
- Time-delayed sharing of strategic information

### 8.3 Infiltration

**Threat**: Hostile actor joins our hive to gather intelligence or sabotage.

**Mitigations**:
- Standard hive membership vetting applies
- Cross-reference new member with known hostile hive members
- Monitor member behavior for coordination with external hives

```python
def check_infiltration_risk(self, new_member: str) -> InfiltrationRisk:
    """Check if new member might be infiltrator."""

    # Check if node appears in any detected hostile hive
    hostile_hives = self.get_hives_by_classification(["hostile", "predatory", "parasitic"])

    for hive in hostile_hives:
        if new_member in hive.suspected_members:
            return InfiltrationRisk(
                node_id=new_member,
                risk_level="critical",
                reason=f"Node is member of {hive.classification} hive {hive.hive_id}",
                recommendation="reject",
            )

        # Check channel relationships with hostile hive
        overlap = self.channel_overlap(new_member, hive.suspected_members)
        if overlap > 0.5:
            return InfiltrationRisk(
                node_id=new_member,
                risk_level="high",
                reason=f"High channel overlap ({overlap:.0%}) with {hive.classification} hive",
                recommendation="reject_or_extended_probation",
            )

    return InfiltrationRisk(
        node_id=new_member,
        risk_level="low",
        reason="No hostile hive association detected",
        recommendation="standard_vetting",
    )
```

### 8.4 Federation Betrayal

**Threat**: Federated hive turns hostile or leaks shared intelligence.

**Mitigations**:
- Continuous verification of federated hive behavior
- Automatic suspension on trust score drop
- Limited blast radius (tiered information sharing)
- Federation termination protocol

```python
def handle_federation_breach(self, federation_id: str, breach_type: str):
    """Handle detected federation breach."""

    federation = self.get_federation(federation_id)
    their_hive = federation.their_hive_id

    # Immediate actions
    actions = []

    if breach_type == "hostile_action":
        # Immediate termination
        self.terminate_federation(federation_id, reason=breach_type)
        self.reclassify_hive(their_hive, "hostile")
        actions.append("federation_terminated")
        actions.append("hive_reclassified_hostile")

    elif breach_type == "intelligence_leak":
        # Suspend and investigate
        self.suspend_federation(federation_id)
        self.increase_monitoring(their_hive)
        actions.append("federation_suspended")
        actions.append("enhanced_monitoring_enabled")

    elif breach_type == "terms_violation":
        # Warn and reduce trust
        self.warn_federation(federation_id, breach_type)
        self.reduce_federation_level(federation_id)
        actions.append("warning_issued")
        actions.append("federation_level_reduced")

    # Alert federated hives
    self.broadcast_to_federated(
        type="federation_breach_alert",
        breaching_hive=their_hive,
        breach_type=breach_type,
        our_response=actions,
    )

    return actions
```

### 8.5 Coordinated Attack Defense

```python
class CoordinatedDefense:
    """Coordinate defense with federated hives."""

    def request_coordinated_defense(
        self,
        attacker_hive: str,
        attack_type: str,
        evidence: List[Dict],
    ) -> DefenseCoordination:
        """Request coordinated defense from federated hives."""

        # Determine appropriate response
        response_plan = self.create_response_plan(attacker_hive, attack_type)

        # Request participation from federated hives
        participants = []
        for federation in self.get_active_federations(min_level=2):
            response = self.request_defense_participation(
                federation.their_hive_id,
                attacker_hive=attacker_hive,
                response_plan=response_plan,
                evidence=evidence,
            )
            if response.will_participate:
                participants.append(federation.their_hive_id)

        # Execute coordinated response
        if len(participants) >= response_plan.min_participants:
            self.execute_coordinated_response(response_plan, participants)

        return DefenseCoordination(
            attacker=attacker_hive,
            response_plan=response_plan,
            participants=participants,
            status="active" if participants else "solo_defense",
        )
```

---

## 9. Implementation Guidelines

### 9.1 Prerequisites

| Requirement | Status | Notes |
|-------------|--------|-------|
| cl-hive | Required | Base coordination |
| cl-revenue-ops | Required | Fee execution |
| Gossip analysis module | Required | For detection |
| Graph analysis capability | Required | For pattern detection |

### 9.2 Phased Rollout

**Phase 1: Detection Only**
- Implement hive detection algorithms
- Build hive registry
- Manual classification only
- No automated policies

**Phase 2: Classification & Reputation**
- Automated classification based on behavior
- Multi-dimensional reputation system
- Basic policy framework
- Human approval for classification changes

**Phase 3: Policy Automation**
- Automated policy application
- Real-time fee adjustments
- Channel decision automation
- Human override capability

**Phase 4: Federation**
- Federation handshake protocol
- Intelligence sharing
- Coordinated defense
- Multi-hive operations

### 9.3 RPC Commands

| Command | Description |
|---------|-------------|
| `hive-relation-detect` | Trigger hive detection scan |
| `hive-relation-list` | List detected hives |
| `hive-relation-info <hive_id>` | Get details on a hive |
| `hive-relation-classify <hive_id> <class>` | Manually classify a hive |
| `hive-relation-reputation <hive_id>` | Get reputation details |
| `hive-relation-policy <hive_id>` | Get effective policy |
| `hive-relation-federate <hive_id>` | Initiate federation |
| `hive-relation-unfederate <hive_id>` | Terminate federation |
| `hive-relation-federations` | List federations |

### 9.4 Database Schema Summary

```sql
-- Core tables
detected_hives          -- Detected hive registry
hive_members           -- Node to hive mappings
hive_reputation        -- Multi-dimensional reputation
reputation_events      -- Reputation change log
hive_policies          -- Policy configurations
federations            -- Federation agreements
federation_events      -- Federation activity log
hive_interactions      -- Interaction history for analysis
```

---

## Appendix A: Detection Signal Weights

| Signal | Weight | Threshold | Notes |
|--------|--------|-----------|-------|
| Internal zero-fee | 0.9 | 3+ channels | Strong indicator |
| Coordinated opens | 0.7 | 3+ opens in 24h | Time correlation |
| Fee synchronization | 0.6 | 90% correlation | Statistical analysis |
| Shared peer set | 0.5 | >60% overlap | Jaccard similarity |
| Naming patterns | 0.3 | Regex match | Weak signal alone |
| Geographic clustering | 0.4 | Same /24 subnet | IP analysis |
| Funding source | 0.5 | >80% same source | On-chain analysis |

---

## Appendix B: Reputation Score Interpretation

| Overall Score | Interpretation | Recommended Policy |
|--------------|----------------|-------------------|
| 0.9 - 1.0 | Excellent | Federation candidate |
| 0.7 - 0.9 | Good | Cooperative |
| 0.5 - 0.7 | Neutral | Standard |
| 0.3 - 0.5 | Concerning | Enhanced monitoring |
| 0.1 - 0.3 | Poor | Restricted |
| 0.0 - 0.1 | Hostile | Blocked |

---

## Changelog

- **0.3.0-draft** (2025-01-14): Stealth strategy security hardening
  - Added Section 3.6: Stealth Strategy Security Hardening
  - Core assumption change: Assume mutual detection, stealth is bonus not security
  - Removed fee discrimination: All hives get same fees (1.0x multiplier)
    - Differentiation via HTLC limits and internal risk scoring only
    - Fee discrimination was detectable and revealed our awareness
  - Added consistent denial policy: Same response regardless of who asks
    - We control when revelation happens, not them
  - Added anti-gaming measures for classification upgrades
    - Randomized day requirements (±30%)
    - Random delays (0-30 days) after criteria met
    - Ungameable factors: network reputation, third-party attestations, historical presence
  - Added deadlock-breaking mechanism
    - Automatic detection of mutual-predatory stalemates
    - Resolution strategies: cautious upgrade, stance probe, economic signal, third-party intro
  - Added minimal interaction policy for predatory hives
    - No active probing, no routing for intelligence
    - Passive observation and third-party reports instead
  - Added economic trip wires
    - Automatic defense on revenue imbalance (>3:1), net loss, liquidity drain
    - Trip wire triggers reset classification progress
  - Added defensive posture requirement
    - Prepare defenses assuming detection regardless of stealth status
- **0.2.0-draft** (2025-01-14): Predatory-first strategy overhaul
  - Changed default classification from "unknown" to "predatory" for all detected hives
  - Added stealth-first detection strategy (Section 3.5)
    - Detect hives without revealing our own hive membership
    - Information asymmetry advantage concept
  - Added pre-revelation reputation building protocol
    - 90+ days interaction before considering revelation
    - Economic benefit calculation for revelation decisions
  - Added graduated revelation protocol
    - Stages: hidden → hinted → acknowledged → partial → full
    - Never reveal to hostile/parasitic hives
  - Removed "unknown" and "observed" classification categories
  - Added "competitive" classification between predatory and neutral
  - Updated trust progression timelines (60/90/180/365 days)
  - Updated default policies to support stealth operations
  - Added `reveal_hive_status` flag to all policies
  - Added `hive_reputation_building` table for tracking pre-revelation reputation
- **0.1.0-draft** (2025-01-14): Initial specification draft
