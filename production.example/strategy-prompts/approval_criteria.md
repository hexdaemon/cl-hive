# Action Approval Criteria

## Channel Open Actions

### APPROVE if ALL conditions are met:
- Target node has >10 active channels (good connectivity)
- Target's average fee is <1000 ppm (reasonable routing partner)
- Current on-chain fees are <50 sat/vB (reasonable opening cost)
- Opening would not exceed 5% of total capacity to this peer
- We have sufficient on-chain balance (amount + 200k sats reserve)
- Target is not already a peer with existing channel

### REJECT if ANY condition applies:
- Target has <5 channels (poor connectivity, risky)
- On-chain fees >100 sat/vB (wait for lower fees)
- Insufficient on-chain balance for channel + reserve
- Target has recent force-close history (check if available)
- Would create duplicate channel to existing peer
- Amount is below minimum viable (< 500k sats)

### DEFER (reject with reason "needs_review") if:
- Target information is incomplete
- Unusual channel size requested (> 5M sats)
- Any uncertainty about the decision

---

## Fee Change Actions

### APPROVE:
- Fee increases on channels with >70% outbound (protect against drain)
- Fee decreases on channels with <30% outbound (attract inbound flow)
- Changes that are <30% from current fee
- Changes that keep fee in reasonable range (10-2500 ppm)

### REJECT:
- Changes >50% in either direction (too aggressive)
- Would set fee below 10 ppm (too cheap, attracts abuse)
- Would set fee above 2500 ppm (too expensive, no flow)
- Channel is currently imbalanced in opposite direction of change

---

## Rebalance Actions

### APPROVE:
- Rebalance is EV-positive (expected revenue > cost)
- Channel is approaching critical imbalance (<10% or >90%)
- Cost is <2% of rebalance amount
- Amount is reasonable (<100k sats for auto-approval)

### REJECT:
- Rebalance cost >3% of amount (too expensive)
- Channel balance is already acceptable (20-80% range)
- Source or destination channel has issues
- Amount exceeds safety limits

---

## General Principles

1. **Safety First**: When uncertain, reject with clear reasoning
2. **Cost Awareness**: Always consider on-chain fees and rebalancing costs
3. **Balance Diversity**: Avoid concentrating too much capacity with single peers
4. **Long-term Thinking**: Prefer sustainable improvements over quick fixes
