# Yield Optimization Implementation Plan

**Goal**: Achieve and exceed Block's 9.7% annual yield on Lightning routing
**Date**: January 2025
**Status**: Planning

---

## Analysis: How Block Achieves 9.7%

### Block's Advantage: Captive Flow

Block (Cash App) earns 9.7% on 184 BTC because they have **captive flow**:

```
Cash App User → [Block's Node] → Lightning Network → Destination
```

Every Cash App Lightning payment MUST traverse Block's node. This creates:
- **Zero competition** for that specific flow
- **Price insensitivity** - users don't see/compare fees
- **Guaranteed volume** - millions of users, daily transactions
- **Fee maximization** - they charge up to 2,147,483,647 ppm (yes, that's 214,748%)

### Why Extreme Fees Work for Block

| Factor | Block | Typical Node |
|--------|-------|--------------|
| Flow source | Captive (Cash App) | Competitive (network) |
| User fee visibility | Hidden | Visible |
| Competition | None | Intense |
| Volume guarantee | High | Variable |
| Fee elasticity | Inelastic | Highly elastic |

### The Hive's Reality

We don't have captive flow. Our options:

1. **Create captive flow** (LSP services, integrations)
2. **Position on critical paths** (be unavoidable for certain routes)
3. **Reduce costs** (lower rebalancing = higher net yield)
4. **Coordinate fleet** (avoid competing with ourselves)
5. **Exploit information advantage** (see more, act faster)

---

## Current cl-hive Capabilities Assessment

### What We Have (Strong Foundation)

| Module | Capability | Yield Relevance |
|--------|------------|-----------------|
| `fee_intelligence.py` | Fee sharing between members | Coordinate pricing |
| `liquidity_coordinator.py` | Bottleneck detection, rebalance coordination | Reduce costs |
| `routing_intelligence.py` | Route probe aggregation | Information advantage |
| `health_aggregator.py` | NNLB health tiers | Prioritize struggling members |
| `splice_coordinator.py` | Splice safety checks | Capital efficiency |
| `planner.py` | Topology optimization | Strategic positioning |
| `peer_reputation.py` | Reputation tracking | Avoid bad peers |

### What's Missing (Gaps to Fill)

| Gap | Impact | Priority |
|-----|--------|----------|
| No flow velocity tracking | Can't predict demand | HIGH |
| No coordinated fee strategy | Internal competition | HIGH |
| No LSP capability | No captive flow | HIGH |
| No capital efficiency metrics | Unknown ROI per channel | MEDIUM |
| No route monopoly detection | Miss positioning opportunities | MEDIUM |
| No adaptive fee decay | Suboptimal hill climbing | MEDIUM |

---

## Yield Math: What's Achievable

### Conservative Scenario (Match Block: 10%)

```
Fleet capacity: 10 BTC (1,000,000,000 sats)
Target yield: 10% annually
Required revenue: 100,000,000 sats/year
             = 8,333,333 sats/month
             = 273,973 sats/day

At 500 ppm average fee:
  Required daily volume: 273,973 / 0.0005 = 547,946,000 sats
                       = 5.48 BTC/day routed

At 50% capacity utilization (5 BTC available for routing):
  Required turns: 5.48 / 5 = 1.1 turns/day
```

**Verdict**: Achievable with good positioning and moderate fees.

### Aggressive Scenario (Exceed Block: 15%+)

Three paths to exceed 10%:

1. **Higher fees via positioning**
   - Be on routes with less competition
   - Target specific flow corridors (exchanges ↔ merchants)

2. **Lower costs via coordination**
   - Reduce rebalancing costs by 50% through fleet coordination
   - Pre-position liquidity before demand spikes

3. **Create captive flow via LSP**
   - Offer LSP services to wallets/businesses
   - JIT channel opens capture 100% of flow

### LQWD's 24% - How They Claim It

LQWD reports 24% annualized. Likely includes:
- JIT channel fees (LSPS2)
- On-chain fee savings passed to customers
- Higher risk tolerance (larger channels to fewer peers)
- Possibly accounting for unrealized gains

**Key insight**: LSP services dramatically increase yield potential.

---

## Implementation Plan

### Phase 0: Routing Pool Foundation (Weeks 1-2)

**Goal**: Establish collective economics as the foundation for all other optimizations

The routing pool is not an add-on—it's the foundation that makes everything else work. Without shared economics, members will always have incentive to defect from coordination.

#### 0.1 Pool Data Schema

```python
# Add to modules/database.py

POOL_TABLES = """
-- Member contributions (snapshot each period)
CREATE TABLE IF NOT EXISTS pool_contributions (
    id INTEGER PRIMARY KEY,
    member_id TEXT NOT NULL,
    period TEXT NOT NULL,  -- e.g., "2025-W03"

    -- Capital metrics
    total_capacity_sats INTEGER,
    weighted_capacity_sats INTEGER,
    uptime_pct REAL,

    -- Position metrics
    betweenness_centrality REAL,
    unique_peers INTEGER,
    bridge_score REAL,

    -- Operations metrics
    routing_success_rate REAL,
    avg_response_time_ms REAL,

    -- Computed
    pool_share REAL,

    recorded_at INTEGER,
    UNIQUE(member_id, period)
);

-- Revenue earned (individual transactions)
CREATE TABLE IF NOT EXISTS pool_revenue (
    id INTEGER PRIMARY KEY,
    member_id TEXT NOT NULL,
    amount_sats INTEGER NOT NULL,
    channel_id TEXT,
    recorded_at INTEGER
);

-- Distributions (settlements)
CREATE TABLE IF NOT EXISTS pool_distributions (
    id INTEGER PRIMARY KEY,
    period TEXT NOT NULL,
    member_id TEXT NOT NULL,
    contribution_share REAL,
    amount_sats INTEGER,
    settled_at INTEGER,
    UNIQUE(member_id, period)
);
"""
```

#### 0.2 Contribution Tracking

```python
# New module: modules/routing_pool.py

class RoutingPool:
    """
    Collective profit sharing for the hive.

    All routing revenue goes to pool, distributed proportionally
    based on capital, position, and operational contributions.
    """

    CAPITAL_WEIGHT = 0.70
    POSITION_WEIGHT = 0.20
    OPERATIONS_WEIGHT = 0.10

    SETTLEMENT_PERIOD_DAYS = 7

    def __init__(self, database, plugin, state_manager, health_aggregator):
        self.db = database
        self.plugin = plugin
        self.state_manager = state_manager
        self.health_aggregator = health_aggregator

    def record_revenue(self, member_id: str, amount_sats: int, channel_id: str = None):
        """Record routing revenue (goes to pool, not individual)."""
        self.db.execute(
            "INSERT INTO pool_revenue (member_id, amount_sats, channel_id, recorded_at) "
            "VALUES (?, ?, ?, ?)",
            (member_id, amount_sats, channel_id, int(time.time()))
        )

    def snapshot_contributions(self, period: str):
        """Snapshot all member contributions for a period."""
        members = self.state_manager.get_all_peer_states()

        for member in members:
            contrib = self._calculate_contribution(member)
            self.db.execute(
                "INSERT OR REPLACE INTO pool_contributions "
                "(member_id, period, total_capacity_sats, weighted_capacity_sats, "
                "uptime_pct, betweenness_centrality, unique_peers, bridge_score, "
                "routing_success_rate, avg_response_time_ms, pool_share, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (member.peer_id, period, contrib.total_capacity,
                 contrib.weighted_capacity, contrib.uptime,
                 contrib.centrality, contrib.unique_peers, contrib.bridge_score,
                 contrib.success_rate, contrib.response_time,
                 contrib.pool_share, int(time.time()))
            )

    def calculate_distribution(self, period: str) -> Dict[str, int]:
        """Calculate fair distribution for a settlement period."""
        # Get total revenue for period
        total_revenue = self.db.execute(
            "SELECT SUM(amount_sats) FROM pool_revenue "
            "WHERE recorded_at >= ? AND recorded_at < ?",
            self._period_bounds(period)
        ).fetchone()[0] or 0

        # Get contributions
        contributions = self.db.execute(
            "SELECT member_id, pool_share FROM pool_contributions "
            "WHERE period = ?", (period,)
        ).fetchall()

        total_shares = sum(c[1] for c in contributions)

        distributions = {}
        for member_id, share in contributions:
            pct = share / total_shares if total_shares > 0 else 0
            distributions[member_id] = int(total_revenue * pct)

        return distributions

    def get_pool_status(self) -> Dict:
        """Get current pool status for MCP/display."""
        current_period = self._current_period()

        return {
            "period": current_period,
            "total_revenue_sats": self._get_period_revenue(current_period),
            "member_count": self._get_member_count(),
            "contributions": self._get_contribution_summary(current_period),
            "projected_distribution": self.calculate_distribution(current_period)
        }
```

#### 0.3 MCP Integration

```python
# Add to tools/mcp-hive-server.py

@server.tool("pool_status")
async def handle_pool_status(args: Dict) -> Dict:
    """Get routing pool status - revenue, contributions, projections."""
    node = fleet.get_node(args.get("node"))
    return await node.call("hive-pool-status")

@server.tool("pool_distribution")
async def handle_pool_distribution(args: Dict) -> Dict:
    """Get distribution calculation for a period."""
    node = fleet.get_node(args.get("node"))
    period = args.get("period", "current")
    return await node.call("hive-pool-distribution", {"period": period})
```

**Deliverables**:
- [ ] Pool database schema
- [ ] RoutingPool class with contribution tracking
- [ ] Revenue recording hooks in forwarding events
- [ ] MCP tools for pool visibility
- [ ] Weekly contribution snapshots

---

### Phase 1: Metrics & Measurement (Weeks 3-4)

**Goal**: Measure current state, establish baselines

#### 1.1 Capital Efficiency Metrics

Add tracking for ROI per channel:

```python
@dataclass
class ChannelYieldMetrics:
    channel_id: str
    peer_id: str
    capacity_sats: int

    # Revenue
    routing_revenue_sats: int
    period_days: int

    # Costs
    open_cost_sats: int
    rebalance_cost_sats: int
    opportunity_cost_sats: int  # What we'd earn elsewhere

    # Computed
    net_yield_annual_pct: float
    capital_efficiency: float  # revenue / capacity
    turn_rate: float  # volume / capacity
```

**Implementation**: Extend `revenue_profitability` in cl-revenue-ops bridge.

#### 1.2 Flow Velocity Tracking (already partially exists)

Enhance `advisor_get_velocities` to predict future states:

```python
def predict_channel_state(self, channel_id: str, hours: int = 24) -> Dict:
    """
    Predict channel balance at future time.

    Returns:
        predicted_local_pct: Expected local balance percentage
        depletion_risk: Probability of hitting 0% local
        saturation_risk: Probability of hitting 100% local
        recommended_action: "none" | "preemptive_rebalance" | "raise_fees"
    """
```

**Implementation**: Add to `advisor_db.py`, expose via MCP.

#### 1.3 Internal Competition Detection

Detect when fleet members compete for same flow:

```python
def detect_internal_competition(self) -> List[Dict]:
    """
    Find routes where multiple hive members are competing.

    Returns list of:
        source: Origin peer
        destination: Destination peer
        competing_members: List of members with channels to both
        recommendation: "coordinate_fees" | "specialize"
    """
```

**Implementation**: Add to `liquidity_coordinator.py`.

---

### Phase 2: Fee Coordination (Weeks 5-8)

**Goal**: Eliminate internal competition, optimize collective pricing

#### 2.1 Route Ownership Model

Designate "primary" member for each flow corridor:

```python
class FlowCorridorAssignment:
    """
    Assign flow corridors to specific members to avoid competition.

    Algorithm:
    1. Identify all (source, destination) pairs fleet serves
    2. For each pair, assign to member with best:
       - Position (shortest path)
       - Capacity (more liquidity)
       - Historical performance (higher success rate)
    3. Non-primary members set higher fees (fallback only)
    """
```

**Fee Strategy**:
- Primary member: Competitive fee (target volume)
- Secondary members: Premium fee (capture overflow)
- Result: No undercutting, maximized collective revenue

#### 2.2 Adaptive Fee Decay (Pheromone Evaporation)

Replace fixed hill-climbing with adaptive rates:

```python
class AdaptiveFeeController:
    def calculate_adjustment_rate(self, channel_id: str) -> float:
        """
        Faster adjustment during volatility, slower during stability.

        Factors:
        - Balance velocity (fast change = fast adapt)
        - Network fee volatility (market moving = fast adapt)
        - Recent success rate (failures = fast adapt)
        """
        velocity = self.get_balance_velocity(channel_id)
        fee_volatility = self.get_network_fee_volatility()
        success_rate = self.get_recent_success_rate(channel_id)

        # Base rate
        base_rate = 0.1  # 10% adjustment per cycle

        # Velocity multiplier (1x to 3x)
        velocity_mult = 1 + min(2, abs(velocity) / 0.1)

        # Volatility multiplier (1x to 2x)
        volatility_mult = 1 + min(1, fee_volatility / 100)

        # Success penalty (low success = faster adjustment)
        success_mult = 1 + (1 - success_rate)

        return base_rate * velocity_mult * volatility_mult * success_mult
```

#### 2.3 Fleet-Wide Fee Floor

Establish minimum fees to prevent race-to-bottom:

```python
# In fee_intelligence.py
FLEET_FEE_FLOOR_PPM = 100  # Never go below this
FLEET_FEE_CEILING_PPM = 2500  # Don't price out flow

def calculate_coordinated_fee(
    self,
    channel_id: str,
    is_primary: bool,
    market_fee_ppm: int
) -> int:
    """
    Calculate fee that respects fleet coordination.
    """
    if is_primary:
        # Primary: competitive but above floor
        return max(FLEET_FEE_FLOOR_PPM, market_fee_ppm)
    else:
        # Secondary: premium for overflow
        return max(FLEET_FEE_FLOOR_PPM * 2, int(market_fee_ppm * 1.5))
```

---

### Phase 3: Cost Reduction (Weeks 9-12)

**Goal**: Reduce rebalancing costs by 50%

#### 3.1 Predictive Rebalancing

Move liquidity BEFORE it's needed:

```python
class PredictiveRebalancer:
    """
    Rebalance based on predictions, not current state.

    Benefits:
    - Lower urgency = lower fees paid
    - Better timing = more route options
    - Proactive = never desperate
    """

    def should_preemptive_rebalance(self, channel_id: str) -> Optional[Dict]:
        # Get prediction
        pred = self.predict_channel_state(channel_id, hours=12)

        if pred['depletion_risk'] > 0.7:
            return {
                'action': 'rebalance_in',
                'reason': 'predicted_depletion',
                'urgency': 'low',  # We have time
                'max_fee_ppm': 500  # Can be picky
            }
        elif pred['saturation_risk'] > 0.7:
            return {
                'action': 'rebalance_out',
                'reason': 'predicted_saturation',
                'urgency': 'low',
                'max_fee_ppm': 500
            }
        return None
```

#### 3.2 Fleet Rebalance Routing

Use fleet members as rebalance hops when cheaper:

```python
def find_fleet_rebalance_path(
    self,
    from_channel: str,
    to_channel: str,
    amount_sats: int
) -> Optional[Dict]:
    """
    Find rebalance path through other fleet members.

    Often cheaper because:
    - Fleet members have coordinated fees
    - Can use internal "friendship" rates
    - Better liquidity information
    """
    # Check if path exists through fleet
    fleet_path = self._find_internal_path(from_channel, to_channel)

    if fleet_path:
        fleet_cost = self._estimate_fleet_path_cost(fleet_path, amount_sats)
        external_cost = self._estimate_external_path_cost(from_channel, to_channel, amount_sats)

        if fleet_cost < external_cost * 0.8:  # 20% savings threshold
            return {
                'path': fleet_path,
                'estimated_cost': fleet_cost,
                'savings_pct': (external_cost - fleet_cost) / external_cost
            }
    return None
```

#### 3.3 Circular Rebalance Detection

Identify and prevent wasteful circular flows:

```python
def detect_circular_flows(self) -> List[Dict]:
    """
    Detect when fleet pays fees to move liquidity in circles.

    Example: A→B→C→A where A, B, C are all fleet members
    This is pure cost with no benefit.
    """
    # Analyze recent rebalances across fleet
    # Flag any that form cycles
    # Recommend stopping one leg
```

---

### Phase 4: Captive Flow (Weeks 13-20)

**Goal**: Create guaranteed flow sources via LSP services

#### 4.1 Fleet LSP Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    HIVE LSP SERVICE                      │
│                                                          │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│   │   Node A     │  │   Node B     │  │   Node C     │ │
│   │   (Primary)  │  │  (Failover)  │  │  (Failover)  │ │
│   └──────────────┘  └──────────────┘  └──────────────┘ │
│          ▲                 ▲                 ▲          │
│          └─────────────────┼─────────────────┘          │
│                            │                            │
│                    Load Balancer                        │
│                            │                            │
└────────────────────────────┼────────────────────────────┘
                             │
                     LSPS API (unified)
                             │
                      ┌──────┴──────┐
                      │   Wallets   │
                      │  Merchants  │
                      │    Apps     │
                      └─────────────┘
```

#### 4.2 LSPS Implementation

Implement LSPS1 (channel ordering) and LSPS2 (JIT channels):

```python
# New module: modules/lsp_service.py

class HiveLSPService:
    """
    Lightning Service Provider backed by the fleet.

    Implements:
    - LSPS1: Channel ordering
    - LSPS2: JIT channel opening
    - Fleet load balancing
    - Automatic failover
    """

    def handle_channel_request(self, request: Dict) -> Dict:
        """
        Handle LSPS1 channel request.

        1. Select best fleet member to fulfill
        2. Open channel from that member
        3. Track for fee attribution
        """

    def handle_jit_invoice(self, invoice: Dict) -> Dict:
        """
        Handle LSPS2 JIT channel request.

        1. Generate wrapped invoice
        2. When paid, open channel just-in-time
        3. Deduct channel fee from payment
        """
```

#### 4.3 LSP Fee Structure

```python
# LSP fees (in addition to routing fees)
LSP_CHANNEL_OPEN_FEE_PCT = 1.0    # 1% of channel size
LSP_JIT_CHANNEL_FEE_PCT = 2.0     # 2% of first payment
LSP_INBOUND_LIQUIDITY_FEE_PPM = 1000  # 0.1% for inbound

# Example economics:
# Customer requests 1M sat inbound liquidity
# - Channel open fee: 10,000 sats
# - Monthly liquidity rental: 1,000 sats
# - All routing through us: fees captured
#
# Annual yield on 1M sat channel:
# - Open fee: 10,000 (1%)
# - Liquidity rental: 12,000 (1.2%)
# - Routing fees: ~10,000 (1% at 500ppm, 2 turns/day)
# Total: ~32,000 sats = 3.2% per channel
# But: 100% of customer's flow goes through us
```

---

### Phase 5: Strategic Positioning (Weeks 21-26)

**Goal**: Position fleet on critical network paths

#### 5.1 Identify High-Value Routes

```python
class RouteValueAnalyzer:
    """
    Identify routes with high volume and limited competition.
    """

    def find_valuable_corridors(self) -> List[Dict]:
        """
        Returns corridors ranked by:
        - Volume (historical routing volume)
        - Margin (fees paid / competition)
        - Accessibility (can we get position?)
        """

    def find_bridge_positions(self) -> List[Dict]:
        """
        Find "bridge" nodes that connect network clusters.
        Being on the bridge = mandatory routing.
        """
```

#### 5.2 Coordinated Positioning

```python
class FleetPositioningStrategy:
    """
    Coordinate channel opens to maximize fleet coverage.

    Principles:
    1. Don't duplicate - one member per target
    2. Complementary positions - cover different regions
    3. Bridge priority - control chokepoints
    """

    def recommend_next_open(self, member_id: str) -> Dict:
        """
        Recommend next channel open for member.

        Considers:
        - What fleet already covers
        - Where gaps exist
        - Member's current position
        - Target value
        """
```

#### 5.3 Exchange Connectivity

Exchanges are high-value endpoints. Strategy:

```python
PRIORITY_EXCHANGES = [
    # Known high-volume Lightning exchanges
    "ACINQ",           # Phoenix wallet backend
    "Kraken",          # Major exchange
    "Bitfinex",        # Major exchange
    "River Financial", # Bitcoin-focused
    "Cash App",        # Block (they'll route TO us)
    "Strike",          # High volume
]

def prioritize_exchange_channels(self):
    """
    Fleet should have channels to all major exchanges.
    Coordinate: each member covers different exchanges.
    """
```

---

## Projected Yield by Phase

| Phase | Timeline | Expected Yield | Cumulative |
|-------|----------|----------------|------------|
| Baseline (current) | Now | 2-4% | 2-4% |
| **Phase 0: Routing Pool** | Week 2 | +1% (alignment) | 3-5% |
| Phase 1: Metrics | Week 4 | +1% (awareness) | 4-6% |
| Phase 2: Fee Coordination | Week 8 | +2% (no competition) | 6-8% |
| Phase 3: Cost Reduction | Week 12 | +2% (lower costs) | 8-10% |
| Phase 4: LSP Services | DEFERRED | -- | -- |
| Phase 5: Positioning | Week 16 | +3% (strategic) | 11-13% |
| Swarm Intelligence | Ongoing | +2-4% (optimization) | 13-17% |

**Target**: 13-17% annual yield (significantly exceeds Block's 9.7%)

**Key Insight**: Phase 0 (Routing Pool) is foundational. Without shared economics, coordination phases (2, 3, 5) face defection incentives. Pool-first approach ensures all subsequent optimizations benefit the collective.

---

## Key Success Metrics

### Primary KPIs

1. **Pool Yield** (target: 13%+)
   ```
   Pool Yield = Total Pool Revenue / Total Pool Capacity
   ```

2. **Distribution Fairness** (target: Gini < 0.3)
   ```
   Gini Coefficient of distributions vs contributions
   ```

3. **Internal Competition Index** (target: 0)
   ```
   ICI = Revenue Lost to Undercutting / Total Revenue
   With pool: should be zero (no incentive to undercut)
   ```

4. **Rebalancing Efficiency** (target: >3x)
   ```
   RE = Fees Earned from Rebalanced Liquidity / Rebalancing Cost
   ```

### Pool-Specific KPIs

5. **Contribution Variance** (target: stable)
   ```
   CV = StdDev(member_shares) / Mean(member_shares)
   Lower = more equal participation
   ```

6. **Revenue per Capacity** (target: increasing)
   ```
   RPC = Pool Revenue / Pool Capacity (weekly)
   Primary efficiency metric
   ```

7. **Member Retention** (target: 100%)
   ```
   MR = Members at end of period / Members at start
   Pool should incentivize staying
   ```

### Secondary KPIs

8. **Prediction Accuracy** (target: >80%)
   ```
   PA = Correct Predictions / Total Predictions
   ```

9. **Position Value** (target: increasing)
   ```
   PV = Betweenness Centrality of Fleet
   ```

10. **Cost per Sat Routed** (target: decreasing)
    ```
    CPR = Total Costs / Total Volume Routed
    ```

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Exchange channels unavailable | Medium | High | Multiple approaches, persistent outreach |
| LSP market too competitive | Medium | Medium | Differentiate on reliability, fleet backing |
| Internal coordination failures | Low | High | Clear protocols, automated checks |
| Network topology changes | Medium | Medium | Adaptive positioning, continuous monitoring |
| Regulatory concerns | Low | High | Legal review, conservative public posture |

---

---

## Swarm Intelligence Integration

The following biological concepts from the research report map directly to yield optimization:

### Concept 1: Adaptive Pheromone Evaporation → Fee Learning

**Biological Basis**: Ant colonies use variable pheromone decay rates—faster in dynamic environments, slower in stable ones. IEACO research shows fixed rates get stuck in local optima.

**Application to Phase 2.2 (Adaptive Fee Decay)**:

```python
class PheromoneBasedFeeController:
    """
    Fee adjustment inspired by ant colony pheromone dynamics.

    Pheromone = "memory" of what worked
    Evaporation = forgetting rate
    Deposit = reinforcement from success
    """

    def calculate_evaporation_rate(self, channel_id: str) -> float:
        """
        Dynamic evaporation based on environment stability.

        Stable environment (low velocity, consistent fees):
          - Low evaporation (0.1-0.3)
          - Exploit known good fees

        Dynamic environment (high velocity, fee changes):
          - High evaporation (0.5-0.9)
          - Explore new fee points quickly
        """
        velocity = abs(self.get_balance_velocity(channel_id))
        network_volatility = self.get_fee_volatility()

        # Base evaporation
        base = 0.2

        # Velocity factor: faster drain = faster adaptation
        velocity_factor = min(0.4, velocity * 4)  # Cap at 0.4 addition

        # Volatility factor: market moving = faster adaptation
        volatility_factor = min(0.3, network_volatility / 200)

        return min(0.9, base + velocity_factor + volatility_factor)

    def update_fee_pheromone(
        self,
        channel_id: str,
        current_fee: int,
        routing_success: bool,
        revenue_sats: int
    ):
        """
        Update fee "pheromone" based on routing outcomes.

        Success → deposit pheromone (reinforce this fee)
        Failure → no deposit (let it evaporate)
        High revenue → stronger deposit
        """
        evap_rate = self.calculate_evaporation_rate(channel_id)

        # Evaporate existing pheromone
        self.fee_pheromone[channel_id] *= (1 - evap_rate)

        if routing_success:
            # Deposit proportional to revenue
            deposit = revenue_sats / 1000  # Scale factor
            self.fee_pheromone[channel_id] += deposit

    def suggest_fee(self, channel_id: str) -> int:
        """
        Suggest fee based on pheromone trails.

        High pheromone at current fee → stay
        Low pheromone → explore (increase or decrease)
        """
        pheromone = self.fee_pheromone.get(channel_id, 0)
        current_fee = self.get_current_fee(channel_id)

        if pheromone > self.EXPLOIT_THRESHOLD:
            # Strong signal - exploit current fee
            return current_fee
        else:
            # Weak signal - explore
            # Try higher if depleting, lower if saturating
            balance_pct = self.get_local_balance_pct(channel_id)
            if balance_pct < 0.3:
                return int(current_fee * 1.15)  # Raise to slow outflow
            elif balance_pct > 0.7:
                return int(current_fee * 0.85)  # Lower to attract flow
            else:
                return current_fee
```

**Yield Impact**: Faster convergence to optimal fees, escape local optima, +0.5-1% yield improvement.

---

### Concept 2: Stigmergy → Indirect Fee Coordination

**Biological Basis**: Ants coordinate without direct communication by leaving pheromone trails in the environment. Other ants read these trails and adjust behavior.

**Application to Phase 2.1 (Route Ownership)**:

Instead of explicit "ownership assignment," use stigmergic markers:

```python
class StigmergicFeeCoordination:
    """
    Fleet members coordinate fees by observing each other's
    routing outcomes, not through direct messaging.

    The "environment" is the shared routing intelligence map.
    """

    def deposit_route_marker(
        self,
        source: str,
        destination: str,
        fee_charged: int,
        success: bool,
        volume_sats: int
    ):
        """
        Leave a marker in shared routing map after routing attempt.

        Other fleet members will see this and adjust their fees
        for the same route accordingly.
        """
        marker = RouteMarker(
            depositor=self.our_pubkey,
            source=source,
            destination=destination,
            fee_ppm=fee_charged,
            success=success,
            volume=volume_sats,
            timestamp=time.time(),
            strength=volume_sats / 100000  # Larger payments = stronger signal
        )

        # Broadcast via existing gossip (or store in shared state)
        self.routing_map.add_marker(marker)

    def read_route_markers(self, source: str, destination: str) -> List[RouteMarker]:
        """
        Read markers left by other fleet members for this route.
        """
        markers = self.routing_map.get_markers(source, destination)

        # Apply decay (older markers = weaker signal)
        now = time.time()
        for m in markers:
            age_hours = (now - m.timestamp) / 3600
            m.strength *= math.exp(-age_hours / 24)  # 24-hour half-life

        return [m for m in markers if m.strength > 0.1]  # Filter weak

    def calculate_coordinated_fee(self, source: str, destination: str) -> int:
        """
        Set fee based on stigmergic signals from fleet.

        If another member is successfully routing at fee X:
          - Don't undercut (set fee >= X)
          - They "own" this route via demonstrated success

        If another member is failing at fee X:
          - We can try lower fee (opportunity)
          - Or avoid this route entirely
        """
        markers = self.read_route_markers(source, destination)

        if not markers:
            return self.default_fee  # No signals, use default

        # Find strongest successful marker
        successful = [m for m in markers if m.success]
        if successful:
            best = max(successful, key=lambda m: m.strength)
            # Don't undercut successful fleet member
            return max(self.min_fee, best.fee_ppm)

        # All failures - try lower or avoid
        avg_failed_fee = sum(m.fee_ppm for m in markers) / len(markers)
        return int(avg_failed_fee * 0.8)  # Try 20% lower
```

**Yield Impact**: Eliminates internal competition without explicit coordination overhead. +1-2% from reduced undercutting.

---

### Concept 3: Physarum Flow Optimization → Channel Lifecycle

**Biological Basis**: Slime mold strengthens tubes with high flow and lets low-flow tubes atrophy. This naturally optimizes the network without central planning.

**Application to Phase 3 (Cost Reduction) and Phase 5 (Positioning)**:

```python
class PhysarumChannelManager:
    """
    Channels evolve based on flow, like slime mold tubes.

    High flow → strengthen (splice in capacity)
    Low flow → atrophy (reduce capacity or close)
    """

    # Thresholds (calibrate based on fleet economics)
    STRENGTHEN_THRESHOLD = 0.02   # 2% daily turn rate
    ATROPHY_THRESHOLD = 0.001     # 0.1% daily turn rate
    MATURITY_DAYS = 30            # Give new channels time

    def calculate_flow_intensity(self, channel_id: str, days: int = 7) -> float:
        """
        Flow intensity = volume / capacity over time.

        This is the "nutrient flow" that determines tube fate.
        """
        stats = self.get_channel_stats(channel_id, days)
        if not stats or stats.capacity == 0:
            return 0

        daily_volume = stats.total_volume / days
        return daily_volume / stats.capacity

    def get_channel_recommendation(self, channel_id: str) -> Dict:
        """
        Physarum-inspired recommendation for channel.
        """
        flow = self.calculate_flow_intensity(channel_id)
        age_days = self.get_channel_age_days(channel_id)
        revenue = self.get_channel_revenue(channel_id)

        if flow > self.STRENGTHEN_THRESHOLD:
            # High flow - this tube should grow
            splice_amount = self._calculate_splice_amount(channel_id, flow)
            return {
                'action': 'strengthen',
                'method': 'splice_in',
                'amount_sats': splice_amount,
                'reason': f'Flow intensity {flow:.3f} exceeds threshold',
                'expected_yield_improvement': flow * 0.5  # Rough estimate
            }

        elif flow < self.ATROPHY_THRESHOLD:
            if age_days < self.MATURITY_DAYS:
                # Young channel - try to attract flow first
                return {
                    'action': 'stimulate',
                    'method': 'reduce_fees',
                    'target_fee': self._calculate_stimulation_fee(channel_id),
                    'reason': f'Young channel with low flow, attempting stimulation'
                }
            else:
                # Mature channel with no flow - let it go
                return {
                    'action': 'atrophy',
                    'method': 'cooperative_close',
                    'reason': f'Mature channel with flow {flow:.4f} below threshold',
                    'capital_to_redeploy': self.get_channel_capacity(channel_id)
                }
        else:
            # Moderate flow - maintain
            return {
                'action': 'maintain',
                'reason': f'Flow intensity {flow:.3f} is healthy'
            }

    def _calculate_splice_amount(self, channel_id: str, flow: float) -> int:
        """
        How much to splice in based on flow intensity.

        Higher flow = more capacity needed to capture it.
        """
        current_capacity = self.get_channel_capacity(channel_id)

        # Target: bring flow intensity down to 1.5x threshold
        target_intensity = self.STRENGTHEN_THRESHOLD * 1.5
        target_capacity = (flow / target_intensity) * current_capacity

        splice_amount = int(target_capacity - current_capacity)

        # Cap at reasonable amount
        return min(splice_amount, current_capacity)  # Max 2x current
```

**Yield Impact**: Capital automatically flows to highest-yield channels. Reduces drag from zombie channels. +1-2% yield.

---

### Concept 4: Mycelium Warning Signals → Collective Defense

**Biological Basis**: When a tree is attacked by pests, it sends chemical signals through the mycelium network. Neighboring trees receive the warning and activate defenses.

**Application to Phase 2 (Fee Coordination) - Defensive Fees**:

```python
class MyceliumDefenseSystem:
    """
    Fleet-wide defense against draining/malicious peers.

    When one member detects a threat, all members respond.
    """

    # Warning message type (add to protocol.py)
    MSG_PEER_WARNING = 0x4857  # "HW" = Hive Warning

    def detect_threat(self, peer_id: str) -> Optional[Dict]:
        """
        Detect peers that are draining us or behaving badly.
        """
        stats = self.get_peer_stats(peer_id, days=7)

        # Threat indicators
        drain_rate = stats.outflow / max(stats.inflow, 1)
        failure_rate = stats.failed_forwards / max(stats.total_forwards, 1)

        if drain_rate > 5.0:  # 5:1 outflow ratio
            return {
                'threat_type': 'drain',
                'peer_id': peer_id,
                'severity': min(1.0, drain_rate / 10),
                'evidence': {'drain_rate': drain_rate}
            }

        if failure_rate > 0.5:  # >50% failures
            return {
                'threat_type': 'unreliable',
                'peer_id': peer_id,
                'severity': failure_rate,
                'evidence': {'failure_rate': failure_rate}
            }

        return None

    def broadcast_warning(self, threat: Dict):
        """
        Send warning to fleet (like chemical signal through mycelium).
        """
        warning = {
            'type': self.MSG_PEER_WARNING,
            'peer_id': threat['peer_id'],
            'threat_type': threat['threat_type'],
            'severity': threat['severity'],
            'reporter': self.our_pubkey,
            'timestamp': time.time(),
            'ttl': 24 * 3600  # 24-hour warning
        }

        self.gossip.broadcast(warning)

    def handle_warning(self, warning: Dict):
        """
        Respond to warning from another fleet member.

        Collective response = raise fees to threatening peer.
        """
        peer_id = warning['peer_id']
        severity = warning['severity']

        # Verify we have channel to this peer
        channel = self.get_channel_to_peer(peer_id)
        if not channel:
            return

        # Calculate defensive fee increase
        current_fee = self.get_channel_fee(channel.id)

        # More severe = higher fee increase
        multiplier = 1 + (severity * 2)  # 1x to 3x
        defensive_fee = int(current_fee * multiplier)

        # Apply with expiration
        self.set_temporary_fee(
            channel.id,
            defensive_fee,
            expires_at=warning['timestamp'] + warning['ttl']
        )

        self.log(f"Defensive fee {defensive_fee} applied to {peer_id[:12]} "
                 f"(warning from {warning['reporter'][:12]})")

    def check_warning_expiration(self):
        """
        Warnings expire - return to normal fees if threat subsides.

        Like pheromone evaporation - signal fades over time.
        """
        now = time.time()
        for channel_id, temp_fee in self.temporary_fees.items():
            if now > temp_fee['expires_at']:
                self.restore_normal_fee(channel_id)
```

**Yield Impact**: Prevents liquidity drain from bad actors. Protects margins collectively. +0.5-1% by stopping losses.

---

### Concept 5: Robustness + Responsiveness → Noise Filtering

**Biological Basis**: Evolved collectives are robust to noise but responsive to salient signals. They achieve this through nonlinear feedback that amplifies important changes.

**Application to Phase 1 & 2 (Metrics and Fee Coordination)**:

```python
class SalienceDetector:
    """
    Filter noise, amplify important signals.

    Not every balance change matters.
    Not every fee change in network matters.
    Detect what's SALIENT and respond only to that.
    """

    # Salience thresholds
    VELOCITY_NOISE_FLOOR = 0.01      # <1%/hour is noise
    FEE_CHANGE_NOISE_FLOOR = 0.05    # <5% fee change is noise
    VOLUME_SPIKE_THRESHOLD = 3.0     # 3x normal = salient

    def is_salient_velocity_change(
        self,
        channel_id: str,
        old_velocity: float,
        new_velocity: float
    ) -> bool:
        """
        Is this velocity change worth responding to?
        """
        # Absolute change
        abs_change = abs(new_velocity - old_velocity)
        if abs_change < self.VELOCITY_NOISE_FLOOR:
            return False  # Noise

        # Relative change (direction reversal is always salient)
        if old_velocity * new_velocity < 0:
            return True  # Direction changed!

        # Large magnitude change
        if abs_change > abs(old_velocity) * 0.5:
            return True  # >50% change in velocity

        return False

    def calculate_response_strength(self, salience: float) -> float:
        """
        Nonlinear response - small salience = small response,
        high salience = amplified response.

        Like neural activation function.
        """
        # Sigmoid-like response
        # Below threshold: minimal response
        # Above threshold: rapid increase
        # Saturates at high end

        threshold = 0.3
        steepness = 10

        return 1 / (1 + math.exp(-steepness * (salience - threshold)))

    def filter_fee_updates(self, updates: List[Dict]) -> List[Dict]:
        """
        Filter incoming fee intelligence to only salient changes.
        """
        salient = []
        for update in updates:
            old_fee = self.last_known_fee.get(update['channel_id'], 0)
            new_fee = update['fee_ppm']

            if old_fee == 0:
                salient.append(update)  # New channel, always salient
                continue

            pct_change = abs(new_fee - old_fee) / old_fee
            if pct_change > self.FEE_CHANGE_NOISE_FLOOR:
                update['salience'] = pct_change
                update['response_strength'] = self.calculate_response_strength(pct_change)
                salient.append(update)

        return salient
```

**Yield Impact**: Reduces thrashing from reacting to noise. More stable fees = better routing reputation. +0.3-0.5% yield.

---

### Summary: Swarm Integration Points

| Concept | Phase | Implementation | Yield Impact |
|---------|-------|----------------|--------------|
| Pheromone Evaporation | 2.2 | Adaptive fee learning rate | +0.5-1% |
| Stigmergy | 2.1 | Indirect route ownership via markers | +1-2% |
| Physarum Flow | 3 & 5 | Flow-based channel lifecycle | +1-2% |
| Mycelium Warnings | 2 | Collective defense system | +0.5-1% |
| Robustness/Responsiveness | 1 & 2 | Salience filtering | +0.3-0.5% |

**Total additional yield from swarm integration: +3.3-6.5%**

This potentially pushes the achievable yield from 10-12% to **13-18%**.

---

## Immediate Next Steps

### Priority 1: Routing Pool (Foundation)
1. **Implement Phase 0.1**: Pool database schema in `database.py`
2. **Implement Phase 0.2**: `RoutingPool` class in new `modules/routing_pool.py`
3. **Implement Phase 0.3**: Revenue recording hooks in forward event handling
4. **Add MCP tools**: `pool_status`, `pool_distribution`

### Priority 2: Metrics
5. **Implement Phase 1.1**: Add `ChannelYieldMetrics` to revenue tracking
6. **Implement Phase 1.2**: Enhance velocity prediction

### Priority 3: Swarm Intelligence
7. **Prototype**: Pheromone-based fee controller (adaptive evaporation)
8. **Prototype**: Salience detector for noise filtering
9. **Design**: Stigmergic route markers (indirect coordination)

### Priority 4: Positioning
10. **Plan Phase 5**: Exchange connectivity strategy
11. **Implement**: Physarum-inspired channel lifecycle

---

---

## Routing Pool: Collective Profit Sharing

### The Concept

A "routing pool" treats the hive as a collective enterprise where:
- Members contribute capital (channels)
- Fleet earns routing fees collectively
- Profits distributed proportional to contribution

This mirrors **mining pools** but for routing:

| Mining Pool | Routing Pool |
|-------------|--------------|
| Contribute hashrate | Contribute liquidity |
| Pool finds blocks | Fleet routes payments |
| Reward ∝ hashrate | Reward ∝ capital deployed |
| Reduces variance | Reduces variance |

### Why This Makes Sense

**1. Aligns with Swarm Intelligence**

Mycelium networks don't track "which fungal thread earned this nutrient." Resources flow to where needed, and the whole network benefits. A routing pool embodies this principle.

**2. Eliminates Internal Competition Completely**

If we share profits, there's zero incentive to undercut fleet members. Your peer's successful route IS your success. This is the ultimate solution to Phase 2's coordination problem.

**3. Enables True Capital Efficiency**

Capital can flow to best positions without individual node operators worrying "but that channel isn't mine." The fleet optimizes collectively.

**4. Reduces Variance**

Individual nodes have high variance—some days great, some days nothing. Pooling smooths returns, making yield more predictable (attractive for serious capital).

**5. Attracts External Capital**

Investors could contribute capital to the pool without running nodes. Fleet operators provide expertise, capital providers provide liquidity. Everyone earns proportionally.

### Contribution Metrics

What constitutes "contribution" to the pool?

```python
@dataclass
class MemberContribution:
    """Track what each member contributes to the pool."""

    member_id: str

    # Capital contribution (primary factor)
    total_capacity_sats: int          # Sum of all channel capacities
    weighted_capacity_sats: int       # Capacity × position_quality
    avg_uptime_pct: float             # Availability matters

    # Position contribution (secondary factor)
    betweenness_centrality: float     # How critical is their position?
    unique_peers: int                 # Peers only they connect to
    bridge_score: float               # Do they connect clusters?

    # Operational contribution (tertiary factor)
    routing_success_rate: float       # Reliable forwarding?
    avg_response_time_ms: float       # Fast forwarding?

    def calculate_pool_share(self) -> float:
        """
        Calculate member's share of pool profits.

        Primary: 70% based on capital
        Secondary: 20% based on position
        Tertiary: 10% based on operations
        """
        capital_score = self.weighted_capacity_sats / POOL_TOTAL_CAPACITY
        position_score = (self.betweenness_centrality * 0.5 +
                         self.bridge_score * 0.5)
        ops_score = (self.routing_success_rate * 0.7 +
                    (1 - self.avg_response_time_ms/1000) * 0.3)

        return (capital_score * 0.70 +
                position_score * 0.20 +
                ops_score * 0.10)
```

### Revenue Attribution

How do we know who "earned" a payment that hopped through multiple fleet nodes?

**Option A: First-Touch Attribution**
- Credit goes to the fleet node that received the payment from outside
- Simple, but ignores contribution of intermediate hops

**Option B: Pro-Rata by Hop**
- Split credit among all fleet nodes in the path
- Fairer, but complex accounting

**Option C: Pool Everything (Recommended)**
- Don't attribute individual payments at all
- All fleet revenue goes to pool
- Distribute based on contribution metrics (above)
- Simplest, most aligned with swarm philosophy

```python
class RoutingPool:
    """
    Collective profit sharing for the hive.
    """

    def __init__(self):
        self.settlement_period_days = 7  # Weekly settlement
        self.revenue_buffer = 0          # Accumulated fees

    def record_routing_revenue(self, member_id: str, amount_sats: int):
        """
        Record revenue earned by any member.
        Goes to pool, not individual.
        """
        self.revenue_buffer += amount_sats
        self.log_revenue(member_id, amount_sats)  # For transparency

    def calculate_distributions(self) -> Dict[str, int]:
        """
        Calculate how to distribute pool to members.
        """
        contributions = self.get_all_contributions()
        total_shares = sum(c.calculate_pool_share() for c in contributions)

        distributions = {}
        for contrib in contributions:
            share = contrib.calculate_pool_share() / total_shares
            distributions[contrib.member_id] = int(self.revenue_buffer * share)

        return distributions

    def settle(self):
        """
        Distribute accumulated revenue to members.

        In practice: could be actual payments, or just accounting
        entries if members trust each other.
        """
        distributions = self.calculate_distributions()

        for member_id, amount in distributions.items():
            self.record_distribution(member_id, amount)

        self.revenue_buffer = 0
```

### Implementation Considerations

**Trust Model**

| Model | Description | Pros | Cons |
|-------|-------------|------|------|
| **Honor System** | Members report earnings, trust distribution | Simple | Requires trust |
| **Transparent Ledger** | All earnings visible to all members | Verifiable | Privacy concerns |
| **Smart Contract** | On-chain distribution logic | Trustless | Complexity, fees |
| **Custodial Pool** | Central party holds/distributes | Simple UX | Counterparty risk |

**Recommended**: Start with **Transparent Ledger** among trusted members. Each node reports earnings to shared database (already have `advisor_db`). Distribution calculated openly. Actual settlement can be informal initially.

**Minimum Viable Pool**

```python
# Add to advisor_db.py

def record_pool_contribution(self, member_id: str, period: str):
    """Record member's contribution metrics for a period."""
    self.db.execute("""
        INSERT INTO pool_contributions
        (member_id, period, capacity_sats, uptime_pct,
         centrality, success_rate, recorded_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, ...)

def record_pool_revenue(self, member_id: str, amount_sats: int):
    """Record revenue earned (goes to pool)."""
    self.db.execute("""
        INSERT INTO pool_revenue
        (member_id, amount_sats, recorded_at)
        VALUES (?, ?, ?)
    """, ...)

def calculate_pool_distribution(self, period: str) -> Dict[str, int]:
    """Calculate fair distribution for a period."""
    # ... implementation per above logic
```

### Regulatory Considerations

A routing pool could be viewed as:
- **Investment contract** (members invest capital, expect returns)
- **Partnership** (joint enterprise for profit)
- **Cooperative** (member-owned, democratic)

**Mitigation**:
- Keep membership small and trust-based initially
- No external "investors" - only active node operators
- Frame as operational coordination, not investment scheme
- Consider legal structure if scaling (LLC, cooperative)

### Alignment with Swarm Intelligence

The routing pool is the **economic manifestation** of swarm principles:

| Swarm Principle | Pool Implementation |
|-----------------|---------------------|
| Stigmergy | Revenue traces show what works; pool follows |
| Mycelium sharing | Resources flow to where needed, returns shared |
| Collective intelligence | Fleet optimizes as unit, not individuals |
| Robustness | No single point of failure in economics |

This is arguably the most "hive-like" structure possible.

---

## cl-revenue-ops Integration Requirements

The yield optimization plan requires changes to both cl-hive (coordination layer) and cl-revenue-ops (execution layer). This section details what needs to change in cl-revenue-ops.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           cl-hive (Coordinator)                          │
│                                                                          │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│   │ Routing Pool │  │ Fee Intel    │  │ Liquidity    │  │ Planner    │ │
│   │ (Phase 0)    │  │ Manager      │  │ Coordinator  │  │            │ │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └────────────┘ │
│          │                 │                 │                          │
└──────────┼─────────────────┼─────────────────┼──────────────────────────┘
           │                 │                 │
           │    ┌────────────┴────────────┐    │
           │    │      hive_bridge.py      │    │
           │    │   (Circuit Breaker +     │    │
           │    │        Caching)          │    │
           │    └────────────┬────────────┘    │
           │                 │                 │
           ▼                 ▼                 ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                       cl-revenue-ops (Executor)                           │
│                                                                           │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│   │ Fee          │  │ Rebalancer   │  │ Profitability│  │ Policy      │ │
│   │ Controller   │  │ (EV-based)   │  │ Analyzer     │  │ Manager     │ │
│   │ (Hill Climb) │  │              │  │              │  │             │ │
│   └──────────────┘  └──────────────┘  └──────────────┘  └─────────────┘ │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

### Changes by Phase

#### Phase 0: Routing Pool Integration

**cl-revenue-ops Changes**:
- Add `report_routing_revenue()` to `hive_bridge.py`
- Hook forward events to push revenue to cl-hive
- Add `query_pool_status()` for visibility

**Effort**: ~50 lines

#### Phase 1: Metrics Sharing

**cl-revenue-ops Changes**:
- Add `revenue-yield-metrics` RPC command
- Add `report_channel_metrics()` bridge method
- Periodic metrics push to cl-hive

**Effort**: ~100 lines

#### Phase 2: Fee Coordination (Major)

**cl-revenue-ops Changes**:

1. **New Fee Strategy**: `HIVE_COORDINATED`
   - Query cl-hive for coordinated fee recommendations
   - Respect fleet fee floor/ceiling
   - Fall back to local Hill Climbing if hive unavailable

2. **Stigmergic Route Markers**:
   - `deposit_route_marker()` - leave markers after routing
   - `read_route_markers()` - read markers from fleet
   - Indirect coordination without explicit messaging

**New cl-hive RPC Commands Needed**:
- `hive-fee-recommendation` - coordinated fee for a channel
- `hive-deposit-route-marker` - leave stigmergic marker
- `hive-read-route-markers` - read fleet markers

**Effort**: ~200-400 lines

#### Phase 3: Cost Reduction

**cl-revenue-ops Changes**:

1. **Predictive Rebalancing**:
   - Query cl-hive for velocity predictions
   - Rebalance early when urgency is low = cheaper fees
   - `should_preemptive_rebalance()` method

2. **Fleet Rebalance Paths**:
   - Check if routing through fleet members is cheaper
   - Fleet members have coordinated (lower) fees
   - `find_fleet_rebalance_path()` method

3. **Circular Flow Detection**:
   - Detect wasteful A→B→C→A patterns within fleet
   - Stop one leg to eliminate waste

**Effort**: ~150 lines

#### Phase 5: Strategic Positioning

**cl-revenue-ops Changes**:

1. **Flow Intensity Calculation**:
   - `calculate_flow_intensity()` in profitability_analyzer
   - Volume / Capacity metric for Physarum decisions

2. **Physarum Recommendations**:
   - High flow → recommend splice-in
   - Low flow → recommend cooperative close
   - Report to cl-hive for fleet coordination

**Effort**: ~100 lines

### Implementation Sprints

| Sprint | Weeks | Focus | cl-revenue-ops Changes |
|--------|-------|-------|------------------------|
| 1 | 1-2 | Pool Integration | `report_routing_revenue()`, forward hooks |
| 2 | 3-4 | Metrics | `yield-metrics` RPC, periodic push |
| 3 | 5-8 | Fee Coordination | `HIVE_COORDINATED` strategy, route markers |
| 4 | 9-12 | Cost Reduction | Predictive rebalancing, fleet paths |
| 5 | 13-16 | Positioning | Flow intensity, Physarum recommendations |

### Backward Compatibility

All changes are **additive** and maintain backward compatibility:

- cl-revenue-ops works standalone (without cl-hive)
- Local Hill Climbing remains the fallback
- Circuit breaker prevents hive failures from cascading
- Existing policies (`dynamic`, `static`, `hive`, `passive`) unchanged

### Total Effort Estimate

| Component | Lines | Complexity |
|-----------|-------|------------|
| Phase 0 (Pool) | ~50 | Low |
| Phase 1 (Metrics) | ~100 | Low |
| Phase 2 (Fees) | ~300 | Medium |
| Phase 3 (Costs) | ~150 | Medium |
| Phase 5 (Position) | ~100 | Low |
| **Total** | **~700** | Medium |

### Key Design Decisions

1. **cl-hive Coordinates, cl-revenue-ops Executes**: Clear separation of concerns
2. **Graceful Degradation**: Always fall back to local-only mode
3. **Eventual Consistency**: Accept stale data with reduced confidence
4. **Idempotent Operations**: Safe retries for all bridge methods

See `docs/planning/CL_REVENUE_OPS_INTEGRATION.md` for detailed implementation specifications.

---

## Conclusion: Can We Match Block's 9.7%?

**Yes, through operational excellence.**

Block has captive flow we can't replicate directly. But we can match their yield through:

1. **Eliminate waste** from internal competition (Phase 2)
2. **Reduce costs** through prediction and coordination (Phase 3)
3. **Position strategically** on high-value routes (Phase 5)
4. **Pool profits** to align incentives completely (Routing Pool)

The fleet's structural advantages—information sharing, coordinated pricing, collective positioning, shared economics—enable yields that individual nodes cannot achieve.

**Target**: 10-15% annual yield within 4 months of implementation.

This matches or exceeds Block's 9.7% through coordination rather than market captivity. The components compound:
- Fee coordination prevents undercutting (+2%)
- Cost reduction improves margins (+2%)
- Strategic positioning captures premium routes (+3%)
- Swarm intelligence optimizations (+3-6%)
- Routing pool alignment (enables all of the above)

LSP services remain a future option to push beyond 15%.
