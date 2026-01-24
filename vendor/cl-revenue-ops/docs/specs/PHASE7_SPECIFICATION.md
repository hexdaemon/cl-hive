# Phase 7: "The 1% Node" Strategy Path

## Technical Specification — Version 1.3.1 (Hardened & Optimized)

| Field | Value |
|-------|-------|
| **Date** | January 3, 2026 |
| **Target Version** | cl-revenue-ops v1.3.1 |
| **Status** | ✅ v1.3.0 Deployed; v1.3.1 In Progress |

---

## 1. Executive Summary

Phase 7 transitions from an "Optimizing Node" to a "Market Making Node." This specification includes the Red Team hardened defenses (v1.3.0) and the new Liquidity Efficiency Suite (v1.3.1).

| Threat / Goal | Feature | Status |
|---------------|---------|--------|
| **L1 Variance** | Vegas Reflex | ✅ Implemented (v1.3.0) |
| **Resource Exhaustion** | Scarcity Pricing | ✅ Implemented (v1.3.0) |
| **Config Race Conditions** | Dynamic Config | ✅ Implemented (v1.3.0) |
| **Capital Efficiency** | Volume-Weighted Targets | 🚧 In Progress (v1.3.1) |
| **Gossip Waste** | Futility Circuit Breaker | 🚧 In Progress (v1.3.1) |

### Implementation Priority

1. **Features 1-3 (Defenses):** Completed & Deployed.
2. **Feature 4 (Liquidity Efficiency):** Immediate priority.
3. **Deferred to v1.4:** Flow Asymmetry, Peer-Level Syncing.

---

## 2. Completed Features (v1.3.0)

### Feature 1: Dynamic Runtime Configuration
**Objective:** Safe hot-swap of algorithmic parameters.
*   **Architecture:** `ConfigSnapshot` immutable pattern + Transactional DB Writes.
*   **Safety:** Prevents "Torn Reads" and "Ghost Configs."

### Feature 2: Mempool Acceleration ("Vegas Reflex")
**Objective:** Protect against toxic arbitrage during L1 spikes.
*   **Algorithm:** Exponential Decay State (Intensity 0.0-1.0).
*   **Safety:** No fixed latch time; intensity naturally fades. Probabilistic trigger prevents front-running.

### Feature 3: HTLC Slot Scarcity Pricing
**Objective:** Exponentially price capacity as slots fill up.
*   **Algorithm:** Value-Weighted Utilization (prevents Dust Flood).
*   **Curve:** 1.0x at 35% utilization → 3.0x at 100%.
*   **Fix Applied:** Virgin Channel Amnesty (Remote-opened channels bypass scarcity until first payment).

---

## 3. New Feature Specification (v1.3.1)

### Feature 4: Liquidity Efficiency Suite

**Objective:** Optimize capital allocation (ROE) and stop wasting resources on broken paths.

#### 4.1 Volume-Weighted Liquidity Targets (Smart Allocation)

**Problem:** Current logic targets fixed 50% capacity. A 10M sat channel moving 10k/day traps 5M sats of "Lazy Capital."
**Solution:** Target inventory based on **Velocity**.

**Algorithm (`modules/rebalancer.py`):**
1.  **Calculate Velocity:** `daily_vol = (sats_in + sats_out) / 7`.
2.  **Inventory Goal:** `vol_target = daily_vol * 3` (3 days of buffer).
3.  **Capacity Cap:** `cap_target = capacity * 0.5` (Never exceed 50%).
4.  **Burst Floor:** `min_floor = 500_000` (Always keep burst capacity).

**Formula:**
`target_sats = max(min_floor, min(cap_target, vol_target))`

#### 4.2 Futility Circuit Breaker

**Problem:** Exponential backoff slows retries but doesn't stop them. Dead channels consume gossip bandwidth and lock HTLCs.
**Solution:** Hard cap on failures.

**Logic (`modules/rebalancer.py`):**
1.  **Check:** `failure_count` from DB.
2.  **Trigger:** If `failure_count > 10`.
3.  **Action:**
    *   Check `last_failure_time`.
    *   If `< 48 hours` ago: **SKIP** candidate entirely (do not calculate EV).
    *   Log: "Futility Circuit Breaker active."

---

## 4. Deferred Features (v1.4)

The following features require additional data collection and are deferred:

### Feature 5: Flow Asymmetry (Rare Liquidity Premium)
- **Reason:** Requires 30 days of traffic forensics to distinguish "One-Way Streets" from "Self-Loops."

### Feature 6: Peer-Level Atomic Fee Syncing
- **Reason:** High-02 "Anchor & Drain" arbitrage risk. Requires "Floor-Only" architecture redesign.

---

## 5. Security Mitigations Summary

| Vulnerability | Severity | Mitigation | Status |
|---------------|----------|------------|--------|
| **Vegas Latch Bomb** | Critical | Exponential decay state | ✅ Fixed |
| **Config Torn Read** | Critical | ConfigSnapshot pattern | ✅ Fixed |
| **Ghost Config** | High | Transactional Write/Read-Back | ✅ Fixed |
| **Dust Flood** | High | Value-weighted utilization | ✅ Fixed |
| **Trap & Trap Deadlock** | High | Rebalancer forecast check | ✅ Fixed |
| **Virgin Channel Poison** | High | Virgin Amnesty logic | ✅ Fixed |
| **Phantom Spending** | Medium | Orphan Job Cleanup | ✅ Fixed |

---

## 6. Implementation Checklist (v1.3.1)

### Modified Files
| File | Change |
|------|--------|
| `modules/rebalancer.py` | Implement `_analyze_rebalance_ev` target logic |
| `modules/rebalancer.py` | Implement `find_rebalance_candidates` futility check |

---

*Specification Author: Lightning Goats Team*  
*Last Updated: January 3, 2026*