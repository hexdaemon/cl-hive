# cl-revenue-ops Integration Analysis for Yield Optimization

**Date**: January 2026
**Status**: Analysis Complete

---

## Executive Summary

To achieve the yield optimization goals (13-17% annual), cl-revenue-ops needs targeted enhancements that integrate with cl-hive's coordination layer. The existing `hive_bridge.py` provides a solid foundation, but several new capabilities are required.

**Key Finding**: cl-revenue-ops is already well-architected for fleet integration. Most changes are additive rather than architectural.

---

## Current Integration Points

### What Already Exists in cl-revenue-ops

| Component | Location | Current Capability |
|-----------|----------|-------------------|
| **Hive Bridge** | `hive_bridge.py` | Fee intelligence queries, health reporting, liquidity coordination, splice safety |
| **Policy Manager** | `policy_manager.py` | `strategy=hive` for fleet members (zero-fee routing) |
| **Fee Controller** | `fee_controller.py` | Hill Climbing with historical response curves |
| **Rebalancer** | `rebalancer.py` | EV-based with Hive peer exemption (negative EV allowed) |
| **Profitability** | `profitability_analyzer.py` | Per-channel ROI, P&L tracking |
| **Flow Analysis** | `flow_analysis.py` | Source/Sink detection, velocity tracking |

### Current cl-hive → cl-revenue-ops Communication

```
cl-hive                              cl-revenue-ops
   │                                      │
   │  hive-fee-intel-query ◄──────────────┤ Query competitor fees
   │  hive-report-fee-observation ◄───────┤ Report our observations
   │  hive-member-health ◄────────────────┤ Query/report health
   │  hive-liquidity-state ◄──────────────┤ Query fleet liquidity
   │  hive-report-liquidity-state ◄───────┤ Report our liquidity
   │  hive-check-rebalance-conflict ◄─────┤ Avoid rebalance collision
   │  hive-splice-check ◄─────────────────┤ Splice safety check
   │                                      │
```

---

## Required Changes by Phase

### Phase 0: Routing Pool Integration

**Goal**: Report routing revenue to cl-hive for pool accounting

**Changes Required in cl-revenue-ops**:

1. **New Bridge Method**: `report_routing_revenue()`
   ```python
   # Add to hive_bridge.py
   def report_routing_revenue(
       self,
       amount_sats: int,
       channel_id: str = None,
       payment_hash: str = None
   ) -> bool:
       """
       Report routing revenue to cl-hive pool.
       Called after each successful forward.
       """
       if not self.is_available():
           return False

       try:
           result = self.plugin.rpc.call("hive-pool-record-revenue", {
               "amount_sats": amount_sats,
               "channel_id": channel_id,
               "payment_hash": payment_hash
           })
           return not result.get("error")
       except Exception:
           return False
   ```

2. **Hook into Forward Events**: In `cl-revenue-ops.py`, the forward_event subscription should call the bridge
   ```python
   # In forward_event handler
   if hive_bridge and hive_bridge.is_available():
       fee_sats = forward_event.get("fee_msat", 0) // 1000
       if fee_sats > 0:
           hive_bridge.report_routing_revenue(
               amount_sats=fee_sats,
               channel_id=forward_event.get("out_channel")
           )
   ```

3. **New Bridge Method**: `query_pool_status()`
   ```python
   def query_pool_status(self) -> Optional[Dict[str, Any]]:
       """Query pool status for display/decisions."""
       if not self.is_available():
           return None
       try:
           return self.plugin.rpc.call("hive-pool-status", {})
       except Exception:
           return None
   ```

**Effort**: ~50 lines, LOW complexity

---

### Phase 1: Enhanced Metrics Sharing

**Goal**: Expose more profitability data to cl-hive

**Changes Required**:

1. **Expose ChannelYieldMetrics via RPC**
   ```python
   # New RPC command in cl-revenue-ops.py
   @plugin.method("revenue-yield-metrics")
   def yield_metrics(channel_id: str = None):
       """
       Get yield metrics for MCP/cl-hive consumption.
       Returns ROI, turn rate, capital efficiency per channel.
       """
       return profitability_analyzer.get_yield_metrics(channel_id)
   ```

2. **Bridge Method to Report Metrics**
   ```python
   # Add to hive_bridge.py
   def report_channel_metrics(
       self,
       channel_id: str,
       roi_pct: float,
       turn_rate: float,
       capital_efficiency: float
   ) -> bool:
       """Report channel metrics for fleet-wide analysis."""
       # Used by cl-hive for Physarum-style channel lifecycle
   ```

3. **Periodic Metrics Push**: Add to fee adjustment loop
   ```python
   # After each fee cycle, push metrics
   if hive_bridge and hive_bridge.is_available():
       for channel in channels:
           metrics = profitability_analyzer.get_channel_metrics(channel.id)
           hive_bridge.report_channel_metrics(
               channel_id=channel.id,
               roi_pct=metrics.roi_pct,
               turn_rate=metrics.turn_rate,
               capital_efficiency=metrics.capital_efficiency
           )
   ```

**Effort**: ~100 lines, LOW complexity

---

### Phase 2: Fee Coordination

**Goal**: Implement fleet-wide coordinated pricing

This is the most significant change area. Two approaches:

#### Approach A: Hive-Controlled Fees (Recommended)

cl-hive calculates coordinated fees, cl-revenue-ops executes them.

**Changes in cl-revenue-ops**:

1. **New Fee Strategy**: `HIVE_COORDINATED`
   ```python
   # Add to policy_manager.py
   class FeeStrategy(Enum):
       DYNAMIC = "dynamic"
       STATIC = "static"
       HIVE = "hive"          # Existing: zero-fee for members
       PASSIVE = "passive"
       HIVE_COORDINATED = "hive_coordinated"  # NEW: Follow cl-hive pricing
   ```

2. **Bridge Method**: `query_coordinated_fee()`
   ```python
   # Add to hive_bridge.py
   def query_coordinated_fee(
       self,
       peer_id: str,
       channel_id: str,
       current_fee: int,
       local_balance_pct: float
   ) -> Optional[Dict[str, Any]]:
       """
       Query cl-hive for coordinated fee recommendation.

       Returns:
           {
               "recommended_fee_ppm": int,
               "is_primary": bool,  # Are we the primary for this route?
               "floor_ppm": int,    # Fleet minimum
               "ceiling_ppm": int,  # Fleet maximum
               "reason": str
           }
       """
       if not self.is_available():
           return None

       try:
           return self.plugin.rpc.call("hive-fee-recommendation", {
               "peer_id": peer_id,
               "channel_id": channel_id,
               "current_fee_ppm": current_fee,
               "local_balance_pct": local_balance_pct
           })
       except Exception:
           return None
   ```

3. **Modify Fee Controller**: Respect hive recommendations
   ```python
   # In fee_controller.py, modify calculate_optimal_fee()
   def calculate_optimal_fee(self, channel_id: str, ...) -> int:
       policy = self.policy_manager.get_policy(peer_id)

       if policy.strategy == FeeStrategy.HIVE_COORDINATED:
           # Query cl-hive for coordinated fee
           hive_rec = self.hive_bridge.query_coordinated_fee(
               peer_id=peer_id,
               channel_id=channel_id,
               current_fee=current_fee,
               local_balance_pct=local_pct
           )
           if hive_rec:
               # Respect fleet floor/ceiling
               fee = hive_rec["recommended_fee_ppm"]
               fee = max(fee, hive_rec.get("floor_ppm", self.min_fee))
               fee = min(fee, hive_rec.get("ceiling_ppm", self.max_fee))
               return fee

       # Fall back to local Hill Climbing
       return self._hill_climb_fee(channel_id, ...)
   ```

#### Approach B: Pheromone-Based Local Learning

Integrate swarm intelligence concepts directly into fee_controller.py.

**Changes**:

1. **Adaptive Evaporation Rate**
   ```python
   # Add to fee_controller.py
   def calculate_evaporation_rate(self, channel_id: str) -> float:
       """
       Dynamic evaporation based on environment stability.
       From swarm intelligence research: IEACO adaptive rates.
       """
       velocity = abs(self.get_balance_velocity(channel_id))
       network_volatility = self.get_fee_volatility()

       base = 0.2
       velocity_factor = min(0.4, velocity * 4)
       volatility_factor = min(0.3, network_volatility / 200)

       return min(0.9, base + velocity_factor + volatility_factor)
   ```

2. **Stigmergic Route Markers** (via cl-hive)
   ```python
   # Add to hive_bridge.py
   def deposit_route_marker(
       self,
       source: str,
       destination: str,
       fee_charged: int,
       success: bool,
       volume_sats: int
   ) -> bool:
       """
       Leave a marker in shared routing map after routing attempt.
       Other fleet members read these for indirect coordination.
       """
       return self.plugin.rpc.call("hive-deposit-route-marker", {
           "source": source,
           "destination": destination,
           "fee_ppm": fee_charged,
           "success": success,
           "volume_sats": volume_sats
       })

   def read_route_markers(self, source: str, destination: str) -> List[Dict]:
       """Read markers left by other fleet members."""
       return self.plugin.rpc.call("hive-read-route-markers", {
           "source": source,
           "destination": destination
       }).get("markers", [])
   ```

**Recommendation**: Start with Approach A (simpler), evolve to Approach B for swarm optimization.

**Effort**: ~200-400 lines, MEDIUM complexity

---

### Phase 3: Cost Reduction

**Goal**: Reduce rebalancing costs through prediction and coordination

**Changes Required**:

1. **Predictive Rebalancing Mode**
   ```python
   # Add to rebalancer.py
   def should_preemptive_rebalance(self, channel_id: str) -> Optional[Dict]:
       """
       Predict future state and rebalance early when we have time.
       Early rebalancing = lower fees = lower costs.
       """
       # Query cl-hive for velocity prediction
       pred = self.hive_bridge.query_velocity_prediction(channel_id, hours=12)

       if pred and pred.get("depletion_risk", 0) > 0.7:
           return {
               "action": "rebalance_in",
               "urgency": "low",  # We have time
               "max_fee_ppm": 300  # Can be picky about cost
           }
       return None
   ```

2. **Fleet Rebalance Path Preference**
   ```python
   # Add to rebalancer.py
   def find_fleet_rebalance_path(
       self,
       from_channel: str,
       to_channel: str,
       amount_sats: int
   ) -> Optional[Dict]:
       """
       Check if rebalancing through fleet members is cheaper.
       Fleet members have coordinated fees (often lower).
       """
       fleet_path = self.hive_bridge.query_fleet_rebalance_path(
           from_channel=from_channel,
           to_channel=to_channel,
           amount_sats=amount_sats
       )

       if fleet_path and fleet_path.get("available"):
           fleet_cost = fleet_path.get("estimated_cost_sats")
           external_cost = self._estimate_external_cost(from_channel, to_channel, amount_sats)

           if fleet_cost < external_cost * 0.8:  # 20% savings threshold
               return fleet_path
       return None
   ```

3. **Circular Flow Detection**
   ```python
   # Add to hive_bridge.py
   def check_circular_flow(self) -> List[Dict]:
       """
       Detect when fleet is paying fees to move liquidity in circles.
       A→B→C→A where all are fleet members = pure waste.
       """
       return self.plugin.rpc.call("hive-detect-circular-flows", {}).get("circular_flows", [])
   ```

**Effort**: ~150 lines, MEDIUM complexity

---

### Phase 5: Strategic Positioning (Physarum Channel Lifecycle)

**Goal**: Flow-based channel lifecycle decisions

**Changes Required**:

1. **Calculate Flow Intensity**
   ```python
   # Add to profitability_analyzer.py
   def calculate_flow_intensity(self, channel_id: str, days: int = 7) -> float:
       """
       Flow intensity = volume / capacity over time.
       This is the "nutrient flow" that determines channel fate.
       """
       stats = self.get_channel_stats(channel_id, days)
       if not stats or stats.capacity == 0:
           return 0

       daily_volume = stats.total_volume / days
       return daily_volume / stats.capacity
   ```

2. **Physarum Recommendations**
   ```python
   # Add to capacity_planner.py
   STRENGTHEN_THRESHOLD = 0.02   # 2% daily turn rate
   ATROPHY_THRESHOLD = 0.001     # 0.1% daily turn rate

   def get_physarum_recommendation(self, channel_id: str) -> Dict:
       """
       Physarum-inspired recommendation for channel.
       High flow → strengthen (splice in)
       Low flow → atrophy (close)
       """
       flow = self.profitability_analyzer.calculate_flow_intensity(channel_id)
       age_days = self.get_channel_age_days(channel_id)

       if flow > STRENGTHEN_THRESHOLD:
           return {
               "action": "strengthen",
               "method": "splice_in",
               "reason": f"High flow intensity {flow:.3f}"
           }
       elif flow < ATROPHY_THRESHOLD and age_days > 30:
           return {
               "action": "atrophy",
               "method": "cooperative_close",
               "reason": f"Low flow intensity {flow:.4f}"
           }
       else:
           return {"action": "maintain"}
   ```

3. **Report to cl-hive for Fleet Coordination**
   ```python
   # Add to hive_bridge.py
   def report_channel_lifecycle_recommendation(
       self,
       channel_id: str,
       peer_id: str,
       recommendation: str,
       flow_intensity: float
   ) -> bool:
       """Report channel lifecycle recommendation for fleet coordination."""
       return self.plugin.rpc.call("hive-channel-lifecycle", {
           "channel_id": channel_id,
           "peer_id": peer_id,
           "recommendation": recommendation,
           "flow_intensity": flow_intensity
       })
   ```

**Effort**: ~100 lines, LOW complexity

---

## New RPC Commands Needed in cl-hive

To support the cl-revenue-ops integration, cl-hive needs these new RPC commands:

| Command | Purpose | Priority |
|---------|---------|----------|
| `hive-pool-record-revenue` | Record revenue from cl-revenue-ops | HIGH (Phase 0) |
| `hive-fee-recommendation` | Get coordinated fee for a channel | HIGH (Phase 2) |
| `hive-deposit-route-marker` | Leave stigmergic marker | MEDIUM (Phase 2) |
| `hive-read-route-markers` | Read markers from fleet | MEDIUM (Phase 2) |
| `hive-velocity-prediction` | Get balance velocity prediction | MEDIUM (Phase 3) |
| `hive-fleet-rebalance-path` | Query fleet rebalance route | MEDIUM (Phase 3) |
| `hive-detect-circular-flows` | Detect wasteful circular flows | LOW (Phase 3) |
| `hive-channel-lifecycle` | Report lifecycle recommendation | LOW (Phase 5) |

---

## Implementation Order

### Sprint 1 (Weeks 1-2): Pool Integration
1. ✅ Phase 0 already implemented in cl-hive
2. Add `report_routing_revenue()` to cl-revenue-ops hive_bridge
3. Hook forward events to report revenue
4. Test pool accumulation

### Sprint 2 (Weeks 3-4): Metrics & Visibility
1. Add `revenue-yield-metrics` RPC command
2. Add `report_channel_metrics()` bridge method
3. Expose metrics to MCP

### Sprint 3 (Weeks 5-8): Fee Coordination
1. Add `HIVE_COORDINATED` fee strategy
2. Implement `hive-fee-recommendation` in cl-hive
3. Add fleet fee floor/ceiling enforcement
4. Integrate with fee_controller.py

### Sprint 4 (Weeks 9-12): Cost Reduction
1. Add predictive rebalancing mode
2. Implement fleet rebalance path preference
3. Add circular flow detection

### Sprint 5 (Weeks 13-16): Positioning
1. Add flow intensity calculation
2. Implement Physarum recommendations
3. Report lifecycle recommendations to fleet

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Bridge failures cascade | Low | Medium | Circuit breaker already exists |
| Fee recommendation conflicts | Medium | Low | Local Hill Climbing as fallback |
| Revenue reporting gaps | Medium | Low | Idempotent recording, periodic reconciliation |
| Rebalance path outdated | Medium | Low | TTL on path recommendations |

---

## Summary

cl-revenue-ops is well-positioned for yield optimization integration:

- **Minimal architectural changes** - mostly additive
- **Existing bridge pattern** - proven circuit breaker + caching
- **Clear separation of concerns** - cl-hive coordinates, cl-revenue-ops executes
- **Graceful degradation** - local-only mode when hive unavailable

**Total estimated effort**: ~600-800 lines across 4-5 sprints

The biggest value comes from Phase 2 (Fee Coordination) which eliminates internal competition - estimated +2-3% yield improvement alone.
