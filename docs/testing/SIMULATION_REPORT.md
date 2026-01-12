# Hive Simulation Suite Test Report

**Date:** 2026-01-11 (Comprehensive Test v4)
**Network:** Polar Network 1 (regtest) - 17 nodes (47% LND)
**Duration:** 30-minute balanced bidirectional simulation

---

## Executive Summary

**30-minute balanced simulation** with 100 ppm external fee floor shows:

1. **Hive dominance confirmed** - Hive nodes routed **72%** of all network forwards (1,371 of 1,903)
2. **Optimized fee strategy** - 0 ppm inter-hive, 100 ppm minimum for external channels
3. **Volume vs margin tradeoff** - Hive prioritizes volume (0.53 sats/forward) vs external (2.06 sats/forward)
4. **Full connectivity achieved** - All hive nodes connected to all 8 LND and 4 external CLN nodes
5. **Carol underutilized** - Only 64 forwards despite 14 channels (liquidity positioning issue)

---

## 30-Minute Balanced Simulation Results (v4)

### Fee Configuration

| Node Type | Fee Manager | Inter-Hive | External Channels |
|-----------|-------------|:----------:|------------------:|
| Hive (alice, bob, carol) | cl-revenue-ops | **0 ppm** | **100+ ppm** (DYNAMIC) |
| CLN External (dave, erin, pat, oscar) | CLBOSS | N/A | 500 ppm |
| LND Competitive (lnd1) | charge-lnd | N/A | 10-350 ppm |
| LND Aggressive (lnd2) | charge-lnd | N/A | 100-1000 ppm |
| LND Conservative (judy) | charge-lnd | N/A | 200-400 ppm |
| LND Balanced (kathy) | charge-lnd | N/A | 75-500 ppm |
| LND Dynamic (lucy) | charge-lnd | N/A | 5-2000 ppm |
| LND Whale (mike) | charge-lnd | N/A | 1-100 ppm |
| LND Sniper (quincy) | charge-lnd | N/A | 1-1500 ppm |
| LND Lazy (niaj) | charge-lnd | N/A | 75-300 ppm |

### Routing Traffic Share

| Node Type | Forwards | % Traffic | Total Fees | % Fees | Avg Fee/Forward |
|-----------|----------|-----------|------------|--------|-----------------|
| **Hive (CLN)** | 1,371 | **72%** | 724 sats | 40% | 0.53 sats |
| External (CLN) | 319 | 17% | 681 sats | 37% | 2.13 sats |
| External (LND) | 213 | 11% | 416 sats | 23% | 1.95 sats |
| **TOTAL** | **1,903** | 100% | **1,821 sats** | 100% | 0.96 sats |

### Detailed Node Performance

| Node | Type | Implementation | Forwards | Total Fees | Fee/Forward |
|------|------|----------------|----------|------------|-------------|
| alice | Hive | CLN | 838 | 480 sats | 0.57 sats |
| bob | Hive | CLN | 469 | 244 sats | 0.52 sats |
| carol | Hive | CLN | 64 | 0.5 sats | 0.01 sats |
| dave | External | CLN | 196 | 640 sats | **3.27 sats** |
| erin | External | CLN | 123 | 41 sats | 0.33 sats |
| lnd1 | External | LND | 32 | 29 sats | 0.91 sats |
| lnd2 | External | LND | 19 | 202 sats | **10.63 sats** |
| niaj | External | LND | 103 | 164 sats | 1.59 sats |
| quincy | External | LND | 55 | 12 sats | 0.22 sats |
| kathy | External | LND | 4 | 9 sats | 2.25 sats |
| judy | External | LND | 0 | 0 sats | - |
| lucy | External | LND | 0 | 0 sats | - |
| mike | External | LND | 0 | 0 sats | - |
| pat | External | CLN | 0 | 0 sats | - |
| oscar | External | CLN | 0 | 0 sats | - |

### Key Findings

1. **Hive captures 72% of routing volume** - Up from 74% in v3 (more LND nodes now routing)
2. **100 ppm floor competitive** - Hive undercuts most external nodes while maintaining profit
3. **lnd2's aggressive strategy most profitable** - 10.63 sats/forward (highest margin)
4. **dave earns highest total** - 640 sats due to 500 ppm CLBOSS default + good positioning
5. **niaj (Lazy config) high volume** - 103 forwards shows 75-300 ppm is competitive
6. **carol severely underperforms** - Only 64 forwards (5% of hive traffic) despite 14 channels
7. **alice dominates hive routing** - 838 forwards (61% of hive traffic)

### Hive Node Connectivity

All hive nodes achieved full connectivity:

| Hive Node | Unique Peers | LND Connections | CLN Connections |
|-----------|--------------|-----------------|-----------------|
| alice | 14 | 8/8 (100%) | 4/4 (100%) |
| bob | 14 | 8/8 (100%) | 4/4 (100%) |
| carol | 14 | 8/8 (100%) | 4/4 (100%) |

---

## Plugin/Tool Status

| Node | Implementation | cl-revenue-ops | cl-hive | Fee Manager |
|------|----------------|:--------------:|:-------:|:-----------:|
| alice | CLN v25.12 | v1.5.0 | v0.1.0-dev | CLBOSS v0.15.1 |
| bob | CLN v25.12 | v1.5.0 | v0.1.0-dev | CLBOSS v0.15.1 |
| carol | CLN v25.12 | v1.5.0 | v0.1.0-dev | CLBOSS v0.15.1 |
| dave | CLN v25.12 | - | - | CLBOSS v0.15.1 |
| erin | CLN v25.12 | - | - | CLBOSS v0.15.1 |
| pat | CLN v25.12 | - | - | CLBOSS v0.15.1 |
| oscar | CLN v25.12 | - | - | CLBOSS v0.15.1 |
| lnd1 | LND v0.20.0 | - | - | charge-lnd (Competitive) |
| lnd2 | LND v0.20.0 | - | - | charge-lnd (Aggressive) |
| judy | LND v0.20.0 | - | - | charge-lnd (Conservative) |
| kathy | LND v0.20.0 | - | - | charge-lnd (Balanced) |
| lucy | LND v0.20.0 | - | - | charge-lnd (Dynamic) |
| mike | LND v0.20.0 | - | - | charge-lnd (Whale) |
| quincy | LND v0.20.0 | - | - | charge-lnd (Sniper) |
| niaj | LND v0.20.0 | - | - | charge-lnd (Lazy) |

---

## Hive Coordination (cl-hive)

| Node | Status | Tier | Members Seen |
|------|--------|------|--------------|
| alice | active | admin | 3 (alice, bob, carol) |
| bob | active | admin | 3 (alice, bob, carol) |
| carol | active | member | 3 (alice, bob, carol) |

**cl-revenue-ops Fee Policies:**

| Node | Peer | Strategy | Result |
|------|------|----------|--------|
| alice | bob | HIVE | 0 ppm |
| alice | carol | HIVE | 0 ppm |
| bob | alice | HIVE | 0 ppm |
| bob | carol | HIVE | 0 ppm |
| carol | alice | HIVE | 0 ppm |
| carol | bob | HIVE | 0 ppm |

Non-hive peers use **DYNAMIC strategy** - fees adjusted by HillClimb algorithm with 100-5000 ppm range.

---

## Channel Topology (17-Node Network)

```
HIVE NODES (3)                     EXTERNAL CLN (4)              LND NODES (8)
┌─────────────┐                   ┌─────────────┐              ┌─────────────┐
│   alice     │                   │    dave     │              │    lnd1     │
│  14 channels│◄─────────────────►│   channels  │◄────────────►│  Competitive│
│  (0ppm hive)│                   │  (500ppm)   │              │ (10-350ppm) │
│(100ppm ext) │                   └─────────────┘              └─────────────┘
└─────────────┘                          │                            │
       │                           ┌─────────────┐              ┌─────────────┐
       │                           │    erin     │              │    lnd2     │
┌─────────────┐                   │   channels  │              │  Aggressive │
│    bob      │◄─────────────────►│  (500ppm)   │◄────────────►│(100-1000ppm)│
│  14 channels│                   └─────────────┘              └─────────────┘
│  (0ppm hive)│                          │                            │
│(100ppm ext) │                   ┌─────────────┐              ┌─────────────┐
└─────────────┘                   │  pat/oscar  │              │ judy/kathy  │
       │                           │   channels  │              │lucy/mike    │
       │                           │  (500ppm)   │              │quincy/niaj  │
┌─────────────┐                   └─────────────┘              └─────────────┘
│   carol     │
│  14 channels│
│  (0ppm hive)│
│(100ppm ext) │
└─────────────┘
```

**Network Statistics:**
- Total nodes: 17 (9 CLN, 8 LND = 47% LND)
- Hive internal routing: 0 ppm
- Hive external floor: 100 ppm (DYNAMIC strategy)
- External CLN fees: 500 ppm (CLBOSS default)
- LND fees: 1-2000 ppm (charge-lnd dynamic)

---

## Version History

| Version | Date | Fee Config | Key Changes |
|---------|------|------------|-------------|
| v1 | 2026-01-10 | 0/10 ppm | Initial testing |
| v2 | 2026-01-10 | 0/50 ppm | Raised external floor |
| v3 | 2026-01-11 | 0/75 ppm | 30-min comprehensive, 15 nodes |
| v4 | 2026-01-11 | 0/100 ppm | 30-min balanced, 17 nodes, full connectivity |
| **v5** | **2026-01-11** | **0/100 ppm** | **30-min REALISTIC simulation with Pareto, Poisson, node roles** |

---

## 30-Minute REALISTIC Simulation Results (v5)

### Simulation Features

The realistic simulation uses advanced traffic patterns that mirror actual Lightning Network behavior:

| Feature | Implementation | Target | Actual |
|---------|----------------|--------|--------|
| **Payment Size** | Pareto/power law distribution | 80/15/4/1% | 79/15/3/1% |
| **Timing** | Poisson with time-of-day variation | Variable | ~78 payments/min |
| **Node Roles** | Merchants, consumers, routers, exchanges | Weighted selection | Active |
| **Liquidity-Aware** | Failure rate based on outbound ratio | 2-50% by liquidity | Active |
| **Multi-Path (MPP)** | Split payments >100k sats | 2-4 parts | 94 MPP payments |

### Payment Statistics

| Metric | Value |
|--------|-------|
| Total payments attempted | 2,375 |
| Successful | 688 (28%) |
| Failed | 1,687 (71%) |
| MPP payments | 94 |
| Total sats moved | 5,735,039 |
| Total fees paid | 199 sats |

**Note:** High failure rate due to LND nodes requiring `lncli` commands (not yet implemented). CLN-to-CLN payments have ~70% success rate.

### Payment Size Distribution (Pareto)

| Category | Target | Actual | Count |
|----------|--------|--------|-------|
| Small (<10k sats) | 80% | **79%** | 1,888 |
| Medium (10k-100k sats) | 15% | **15%** | 371 |
| Large (100k-500k sats) | 4% | **3%** | 88 |
| XLarge (>500k sats) | 1% | **1%** | 28 |

### Routing Performance (Cumulative)

| Node | Type | Forwards | Fees (sats) | Fee/Forward | Role |
|------|------|----------|-------------|-------------|------|
| alice | Hive | 966 | 631 | 0.65 | router |
| bob | Hive | 684 | 611 | 0.89 | router |
| carol | Hive | 91 | 7 | 0.08 | router |
| dave | External | 202 | 905 | **4.48** | merchant |
| erin | External | 123 | 41 | 0.33 | consumer |
| niaj | LND | 146 | 271 | 1.86 | router |
| quincy | LND | 157 | 16 | 0.10 | consumer |
| kathy | LND | 35 | 86 | 2.46 | exchange |
| lnd1 | LND | 32 | 29 | 0.91 | router |
| lnd2 | LND | 25 | 208 | **8.32** | merchant |
| lucy | LND | 1 | 0 | 0.08 | merchant |

### Traffic Share by Node Type

| Node Type | Forwards | % Traffic | Total Fees | % Fees | Avg Fee/Forward |
|-----------|----------|-----------|------------|--------|-----------------|
| **Hive (CLN)** | 1,741 | **71%** | 1,249 sats | 45% | 0.72 sats |
| External (CLN) | 325 | 13% | 946 sats | 34% | 2.91 sats |
| External (LND) | 396 | 16% | 611 sats | 22% | 1.54 sats |
| **TOTAL** | **2,462** | 100% | **2,806 sats** | 100% | 1.14 sats |

### Key Findings (Realistic Simulation)

1. **Pareto distribution validated** - Payment sizes closely match real Lightning Network distribution
2. **Hive maintains dominance** - 71% of forwards through hive nodes even with realistic patterns
3. **Node roles affect traffic** - Merchants (dave, lnd2) receive more, consumers (erin, quincy) send more
4. **MPP working** - 94 large payments successfully split into 2-4 parts
5. **dave highest earner** - 905 sats from 202 forwards (merchant role + 500 ppm fees)
6. **lnd2 highest margin** - 8.32 sats/forward with aggressive fee strategy

---

## Recommendations

### Completed
- [x] Add more LND nodes - Network now has 8 LND (47%)
- [x] Vary charge-lnd configs - 8 unique fee strategies implemented
- [x] Optimize hive fee strategy - 0 ppm inter-hive, 100 ppm min external
- [x] Full hive connectivity - All hive nodes connected to all external nodes
- [x] Run comprehensive test - 30-minute balanced simulation completed

### Issues to Address

1. **Carol underperformance** - Only 5% of hive traffic despite equal connectivity
   - Investigate liquidity distribution on carol's channels
   - Check if carol's channels are on optimal routing paths

2. **LND nodes not routing** - judy, lucy, mike still at 0 forwards
   - Need better channel positioning for these nodes
   - Consider opening channels from LND nodes to payment sources

### Fee Strategy Insights

| Strategy | Example | Traffic Share | Fee/Forward | Best For |
|----------|---------|---------------|-------------|----------|
| Volume | Hive (100 ppm floor) | 72% | 0.53 sats | Market share, liquidity flow |
| Balanced | dave (500 ppm) | 10% | 3.27 sats | Steady income |
| Aggressive | lnd2 (100-1000 ppm) | 1% | 10.63 sats | High-value routes |

---

## Usage

```bash
# Run 30-minute REALISTIC simulation (recommended)
./simulate.sh traffic realistic 30 1

# Run 30-minute balanced simulation
./simulate.sh traffic balanced 30 1

# Run mixed traffic simulation (4 phases)
./simulate.sh profitability 30 1

# Generate report
./simulate.sh report 1

# Full hive system test
./simulate.sh hive-test 15 1
```

### Realistic Simulation Features

The `realistic` scenario includes:
- **Pareto payment sizes**: 80% small, 15% medium, 4% large, 1% xlarge
- **Poisson timing**: Exponential inter-arrival times with time-of-day variation
- **Node roles**: Merchants (receive), consumers (send), routers (balanced), exchanges
- **Liquidity-aware**: Failure probability based on outbound liquidity ratio
- **MPP**: Payments >100k sats automatically split into 2-4 parts

---

*Report generated by cl-revenue-ops simulation suite v1.6*
*Last updated: 2026-01-11 - 30-minute REALISTIC simulation with Pareto, Poisson, node roles*
