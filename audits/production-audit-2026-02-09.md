# Production Audit: cl-hive + cl_revenue_ops
**Date**: 2026-02-09
**Auditor**: Claude Opus 4.6 (automated analysis)
**Scope**: Full operational audit of both plugins using production database data

---

## Fleet Status (Live — Feb 10, 2026)

- **Nodes**: 3 members (nexus-01, nexus-02, nexus-03)
- **This node (nexus-02)**: 16 channels, 55M sats capacity, 75% local / 25% remote
- **Total revenue earned**: 955 sats (51 forwards in ~3 weeks)
- **Total costs**: 3,189 sats channel opens + failed rebalance fees
- **Net P&L: -2,234 sats** (operating at a loss)

---

## Test Suite Status

- **cl-hive**: 1,431 passed, 1 failed (pre-existing `test_outbox.py::TestOutboxManagerBackoff::test_backoff_base`), 1 skipped
- **cl_revenue_ops**: 371 passed, 0 failed

---

## CRITICAL Issues

### 1. Advisor System Not Running (Timer Not Installed)
The systemd timer `hive-advisor.timer` exists but **is not installed or active**. The advisor (which runs as Claude Sonnet via MCP) hasn't executed since Feb 5. This means:
- No new AI decisions in 5 days
- No outcome measurement happening
- No opportunity scanning
- The Phase 4 predicted benefit fix (deployed Feb 9) has never run

**Fix**: `systemctl --user enable --now hive-advisor.timer`

### 2. Financial Snapshot Fix Just Took Effect
The `a1f703a` fix for zero-balance snapshots is working now:
- Feb 10 00:24: `local=41.5M, remote=13.6M, capacity=55M, 16 channels` (CORRECT)
- Feb 9 all day: `local=0, remote=0, capacity=0` (still broken pre-fix)

### 3. All Automated Rebalances Failing
5 most recent rebalance attempts (Feb 7): **ALL failed or timed out**
- All 200,000 sat attempts via sling
- `actual_fee_sats = NULL` for all (never completed)
- Budget reservations: 23 released, only 1,234 sats total ever reserved

### 4. Hive Channel Fees Fixed (Verified Live)
- `933128x1345x0` (nexus-01): **0 ppm** (correct)
- `933882x99x0` (nexus-03): **0 ppm** (correct)
- Was 5-25 ppm for 2 weeks before the `enforce_limits` fix deployed

### 5. Expansion Stuck in Rejection Loop
- 475 planner cycles, 349 expansions skipped (73%)
- 26 channel_open proposals rejected, 12 expired
- Currently in "25 consecutive rejections, 24h cooldown"
- Recent cycles only run `saturation_check` — nothing proposed

---

## HIGH Priority Issues

### 6. Predicted Benefit Pipeline (Code Fixed, Not Yet Running)
- All 1,079 AI decisions: `snapshot_metrics = NULL`
- All 1,038 outcomes: `predicted_benefit = 0`
- All opportunity types: `"unknown"`
- Learning engine can't compute meaningful prediction errors
- **Code is deployed**, needs advisor timer to start running

### 7. Daily Budget Tracking All Zeros
```
date       | spent | earned | budget
2026-02-05 |     0 |      0 |      0
2026-01-30 |     0 |      0 |      0
(all rows zero)
```

### 8. Fee Change Revenue Measurement Broken
- 557 fee_change outcomes measured: ALL show `actual_benefit = 0`
- Only rebalance outcomes measure anything (all negative: avg -2,707 sats)
- The learning engine can't tell which fee changes helped

### 9. Severely One-Sided Channels
Live balances show 13 of 15 non-HIVE channels at 73-100% local. Two channels at 1% local (depleted) with fees jacked to 1,550 ppm. The node can barely receive forwards.

### 10. Member Health Disparity — nexus-03 Critical
| Member | Health | Tier | Available/Capacity |
|--------|--------|------|-------------------|
| nexus-01 | 71 | healthy | 3.2M / 5.1M |
| nexus-02 | 34 | vulnerable | 2.3M / 2.6M |
| nexus-03 | **8** | **critical** | **52K / 3.5M** |

NNLB correctly identifies nexus-03 needs help (`needs_help=1, needs_channels=1`), but no assistance is being executed.

---

## MEDIUM Priority Issues

### 11. Thompson Sampler Stuck in Cold Start
Most channels show `thompson_cold_start (fwds=0)` — the fee optimizer has no data to learn from because there are so few forwards (51 total in 3 weeks). Only 3 channels have seen any forwards at all.

### 12. Contribution Tracking Empty
All hive members show `contribution_ratio=0.0, uptime_pct=0.0, vouch_count=0`. The contribution system isn't accumulating data.

### 13. Config Overrides May Be Too Aggressive
| Override | Value | Concern |
|----------|-------|---------|
| `min_fee_ppm=25` | Now bypassed for HIVE | Was the root cause of non-zero hive fees |
| `rebalance_min_profit_ppm=100` | May prevent rebalances for small channels |
| `sling_chunk_size_sats=200000` | May be too large for channel sizes |

### 14. Pre-existing Test Failure
`test_outbox.py::TestOutboxManagerBackoff::test_backoff_base` — 1 pre-existing failure in cl-hive test suite.

---

## What's Working

1. **Plugin communication**: Both plugins are running, deployed with latest code
2. **Hive gossip + state sync**: Planner cycles execute, saturation checks run
3. **Fee optimization loop**: Thompson+AIMD running, making fee adjustments
4. **Hive peer detection**: Peer policies correctly set to `strategy=hive`
5. **HIVE zero-fee enforcement**: Working correctly since Feb 7
6. **Financial snapshots**: Just fixed, now recording real data
7. **Fee intelligence sharing**: 7,541 records of cross-fleet fee data
8. **Health scoring**: NNLB tiers correctly computed
9. **Phase 8 RPC parallelization**: Deployed, reducing MCP response times

---

## Deployment Status

| Repo | Deployed Commit | Date | Notes |
|------|----------------|------|-------|
| cl-hive | `5da05cd` | Feb 9, 07:36 | Includes predicted benefit pipeline, tests, RPC parallelization |
| cl_revenue_ops | `4c4dabf` | Feb 9, 17:28 | Includes financial snapshot fix, rebalance success rate fix |

---

## Recommended Actions (Priority Order)

| Priority | Action | Impact |
|----------|--------|--------|
| **P0** | Install/enable advisor timer | Enables the entire AI decision loop |
| **P0** | Investigate sling rebalance failures | 5/5 recent attempts failed |
| **P1** | Lower `rebalance_min_profit_ppm` to 25-50 | Current 100 may be preventing profitable rebalances |
| **P1** | Address nexus-03 critical health | Either open channels TO it, or reduce channel count |
| **P2** | Fix daily budget tracking (all zeros) | Budget enforcement is non-functional |
| **P2** | Fix fee_change outcome measurement | 557 outcomes all zero — can't learn from fee changes |
| **P2** | Break expansion rejection loop | Either lower approval bar or add rejection memory |
| **P3** | Fix outbox backoff test | Pre-existing test failure |
| **P3** | Lower `sling_chunk_size_sats` | 200K may be too large for current channel sizes |

---

## Are the Plugins Doing What They're Designed To Do?

**Short answer**: The foundation works, but the operational feedback loop is broken at multiple points.

**cl-hive** correctly manages membership, gossip, topology analysis, and health scoring. But its expansion decisions never get approved, its NNLB assistance never executes, and the advisor that should drive decisions hasn't run in 5 days.

**cl_revenue_ops** correctly handles fee optimization (Thompson+AIMD), peer policy enforcement, and hive channel detection. But rebalancing consistently fails, financial tracking was broken until today, and the fee optimizer is starved of forward data.

**The integration** works at the data-sharing level but not at the action level. Information flows correctly (fee intelligence, health scores, peer policies), but coordinated actions (rebalancing, expansion, assistance) are not materializing. The single biggest issue is the advisor timer not being active — it's the brain of the system and hasn't run in 5 days.
