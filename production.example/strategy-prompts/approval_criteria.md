# Action Approval Criteria

## Node Context (Hive-Nexus-01)

- **Capacity**: ~165M sats across 25 channels (~6.6M avg channel size)
- **On-chain**: ~4.5M sats available
- **Health**: 36% profitable, 40% underwater, 20% stagnant - prioritize quality over growth
- **Strategy**: Focus on improving existing channel profitability before expansion

---

## Channel Open Actions

### APPROVE if ALL conditions are met:
- Target node has >15 active channels (strong connectivity required)
- Target has proven routing volume (check 1ML or Amboss reputation)
- Target's median fee is <500 ppm (quality routing partner)
- Current on-chain fees are <20 sat/vB (excellent opening conditions)
- Opening would not exceed 3% of our total capacity to this peer
- We maintain 500k sats on-chain reserve after opening
- Target is not already a peer with existing channel
- Channel size is 2-10M sats (matches our avg channel size)

### REJECT if ANY condition applies:
- Target has <10 channels (insufficient connectivity)
- On-chain fees >30 sat/vB (wait for lower fees - mempool often clears)
- Insufficient on-chain balance (amount + 500k reserve)
- Target has any force-close history in past 6 months
- Would create duplicate channel to existing peer
- Amount is below 1M sats (not worth on-chain cost)
- We already have >30 channels (focus on profitability first)
- Target is a known drain node or has poor reputation

### DEFER (reject with reason "needs_review") if:
- Target information is incomplete or ambiguous
- Channel size >10M sats (large commitment)
- Target is a new node (<3 months old)
- Any uncertainty about the decision
- Our node has >5 underwater channels (should fix existing first)

---

## Fee Change Actions

### APPROVE:
- Fee increases on channels with >65% outbound (protect liquidity)
- Fee decreases on channels with <35% outbound (attract flow)
- Changes that are <25% from current fee (gradual adjustment)
- Changes within 50-1500 ppm range (our target operating range)
- Increases on channels that are currently profitable (protect margin)
- Decreases on underwater channels to attract flow

### REJECT:
- Changes >40% in either direction (too aggressive, destabilizes routing)
- Would set fee below 50 ppm (attracts low-value drain)
- Would set fee above 2000 ppm (prices out legitimate flow)
- Fee decrease on already-draining channel (wrong direction)
- Fee increase on channel with <30% outbound (will kill remaining flow)

---

## Rebalance Actions

### APPROVE:
- Rebalance is clearly EV-positive (expected revenue > 2x cost)
- Channel is at critical imbalance (<15% or >85% local)
- Cost is <1.5% of rebalance amount
- Amount is reasonable (50k-200k sats typical)
- Both source and destination channels are healthy/profitable

### REJECT:
- Rebalance cost >2% of amount (too expensive given our margins)
- Channel balance is acceptable (20-80% range)
- Source channel is underwater/bleeder (don't throw good sats after bad)
- Destination channel has poor routing history
- Amount >300k sats without clear justification
- Rebalancing into a channel we're considering closing

---

## General Principles

1. **Profitability Focus**: With 40% underwater channels, prioritize fixing existing over expansion
2. **Cost Discipline**: Our 0.17% ROC means every sat of cost matters significantly
3. **Quality Over Quantity**: Reject marginal opportunities - wait for clearly good ones
4. **Conservative Approach**: When uncertain, reject with reasoning and flag for human review
5. **Low Fee Environment**: Current mempool is 1-2 sat/vB - be opportunistic on opens when criteria met
6. **Bleeder Awareness**: Avoid actions that could worsen our 11 flagged problem channels
