# Open Source Recruitment Strategy

## Executive Summary

Release cl_revenue_ops as a standalone open-source CLN plugin that provides immediate value to any node operator. Hive-specific features are included but optional, activating only when connected to a hive. This creates a natural recruitment funnel while the "hive alpha" (coordinated intelligence, revenue pooling, collective positioning) justifies giving away the base functionality.

---

## Part 1: cl_revenue_ops Open Source Release

### 1.1 Current State

cl_revenue_ops currently provides:
- **Hill Climbing fee optimizer** - Adaptive fee adjustment based on flow
- **Rebalancing coordination** - Sling integration with profit constraints
- **Profitability tracking** - Per-channel ROI, classification (profitable/underwater/zombie)
- **Financial dashboard** - TLV, operating margin, P&L reporting
- **Peer policies** - Static/dynamic fee strategies per peer

### 1.2 Hive-Aware Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      cl_revenue_ops                              │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    CORE (Always Active)                      ││
│  │  • Hill Climbing fee optimization                           ││
│  │  • Sling rebalancing with profit constraints                ││
│  │  • Per-channel profitability tracking                       ││
│  │  • Financial dashboard & reporting                          ││
│  │  • Peer-level fee policies                                  ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                   │
│                    [Hive Connection Detected?]                   │
│                         │           │                            │
│                        YES          NO                           │
│                         │           │                            │
│  ┌──────────────────────▼──┐    ┌──▼─────────────────────────┐  │
│  │   HIVE MODE (Optional)  │    │   STANDALONE MODE          │  │
│  │  • Coordinated fees     │    │  • Local optimization only │  │
│  │  • Revenue pool contrib │    │  • No external deps        │  │
│  │  • Shared flow intel    │    │  • Full functionality      │  │
│  │  • Collective defense   │    │  • Just no hive benefits   │  │
│  │  • Physarum triggers    │    │                            │  │
│  └─────────────────────────┘    └────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### 1.3 Implementation Tasks

#### Phase 1: Code Preparation (Week 1-2)

| Task | Description | Files |
|------|-------------|-------|
| **1.1** | Add hive detection flag | `cl_revenue_ops.py` |
| **1.2** | Make hive callbacks optional | `fee_controller.py`, `rebalancer.py` |
| **1.3** | Add `--hive-enabled` option (default: auto-detect) | Plugin options |
| **1.4** | Create hive interface abstraction | `hive_interface.py` (new) |
| **1.5** | Ensure standalone mode works without cl-hive | All modules |
| **1.6** | Add configuration for hive endpoint | `config.py` |

**Hive Detection Logic:**
```python
# In cl_revenue_ops.py

def detect_hive_connection():
    """Auto-detect if cl-hive plugin is loaded and configured."""
    try:
        # Check if cl-hive plugin is running
        plugins = plugin.rpc.plugin("list")
        hive_loaded = any("cl-hive" in p.get("name", "") for p in plugins.get("plugins", []))

        if not hive_loaded:
            return False

        # Check if we're a hive member
        status = plugin.rpc.call("hive-status")
        return status.get("membership", {}).get("tier") in ["admin", "member", "neophyte"]

    except Exception:
        return False

# Plugin startup
HIVE_MODE = detect_hive_connection() if plugin.get_option("hive-enabled") == "auto" else plugin.get_option("hive-enabled") == "true"
```

**Optional Hive Callbacks:**
```python
# In fee_controller.py

async def adjust_fee(channel_id: str, new_fee: int):
    """Adjust channel fee with optional hive coordination."""

    # Always do local adjustment
    result = await _local_fee_adjustment(channel_id, new_fee)

    # If hive mode, notify hive for coordination
    if HIVE_MODE:
        try:
            await _notify_hive_fee_change(channel_id, new_fee)
        except Exception as e:
            # Hive notification failed - continue anyway
            logger.warning(f"Hive notification failed: {e}")

    return result
```

#### Phase 2: Documentation (Week 2)

| Task | Description |
|------|-------------|
| **2.1** | Write comprehensive README.md |
| **2.2** | Create QUICKSTART.md for 5-minute setup |
| **2.3** | Document all RPC commands |
| **2.4** | Add configuration examples |
| **2.5** | Write "Hive Benefits" section explaining alpha |
| **2.6** | Create CONTRIBUTING.md |

**README Structure:**
```markdown
# cl_revenue_ops

Intelligent fee optimization and rebalancing for Core Lightning nodes.

## Features
- 🎯 Hill Climbing fee optimizer
- ⚖️ Profit-constrained rebalancing
- 📊 Per-channel profitability tracking
- 💰 Financial dashboard

## Quick Start
[5-minute setup instructions]

## Hive Integration (Optional)
cl_revenue_ops can run standalone OR as part of a Lightning Hive.

**Standalone Mode:** Full functionality, local optimization only.

**Hive Mode:** Unlocks additional benefits:
- 🐝 Coordinated fee strategies across fleet
- 💎 Revenue pooling with fair distribution
- 🧠 Shared flow intelligence
- 🛡️ Collective defense against drain attacks
- 🔮 Predictive liquidity positioning

[Learn more about joining a Hive →](https://github.com/santyr/cl-hive)
```

#### Phase 3: Repository Setup (Week 2)

| Task | Description |
|------|-------------|
| **3.1** | Create public GitHub repo |
| **3.2** | Choose license (MIT recommended) |
| **3.3** | Set up GitHub Actions for CI |
| **3.4** | Add issue templates |
| **3.5** | Create release workflow |
| **3.6** | Add security policy |

#### Phase 4: Community Launch (Week 3)

| Task | Description |
|------|-------------|
| **4.1** | Announce on Lightning-dev mailing list |
| **4.2** | Post on Twitter/Nostr |
| **4.3** | Submit to awesome-lightning lists |
| **4.4** | Write introductory blog post |
| **4.5** | Create demo video |

### 1.4 Hive Alpha Features (What Makes Joining Worth It)

These features ONLY activate in hive mode:

| Feature | Standalone | Hive Mode | Value Proposition |
|---------|------------|-----------|-------------------|
| Hill Climbing | ✅ | ✅ | Same |
| Rebalancing | ✅ | ✅ | Same |
| Profitability | ✅ | ✅ | Same |
| **Coordinated Fees** | ❌ | ✅ | Fleet-wide optimization, no internal competition |
| **Revenue Pooling** | ❌ | ✅ | Fair distribution based on contribution |
| **Flow Intelligence** | ❌ | ✅ | See aggregate flow patterns across hive |
| **Physarum Triggers** | ❌ | ✅ | Automatic channel opens to hot corridors |
| **Anticipatory Liquidity** | ❌ | ✅ | Pre-position based on predictions |
| **Collective Defense** | ❌ | ✅ | Coordinated response to drain attacks |
| **AI Advisor** | ❌ | ✅ | Proactive optimization with learning |

---

## Part 2: Public Hive Portal

### 2.1 Portal Overview

A web-based interface for:
1. **Discovery** - Learn about hive benefits
2. **Application** - Apply to join with node details
3. **Vetting** - Automated + human review process
4. **Onboarding** - Receive genesis ticket, setup instructions
5. **Dashboard** - Member stats, revenue share, health

### 2.2 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PUBLIC PORTAL                                 │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │
│  │  Landing    │  │ Application │  │  Member     │  │   Admin     │ │
│  │   Page      │  │   Form      │  │  Dashboard  │  │   Panel     │ │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘ │
└─────────┼────────────────┼────────────────┼────────────────┼────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         PORTAL API                                   │
│  • Node verification    • Genesis ticket generation                  │
│  • Application processing  • Stats aggregation                       │
│  • Member authentication   • Revenue reporting                       │
└─────────────────────────────────────────────────────────────────────┘
          │                │                │
          ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      HIVE INFRASTRUCTURE                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │  cl-hive    │  │  Membership │  │  Revenue    │                  │
│  │  Plugin     │  │  Database   │  │  Pool       │                  │
│  └─────────────┘  └─────────────┘  └─────────────┘                  │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.3 Application Flow

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   DISCOVER   │────▶│    APPLY     │────▶│    VERIFY    │
│              │     │              │     │              │
│ • Benefits   │     │ • Node pubkey│     │ • Auto-check │
│ • Requirements│    │ • Capacity   │     │ • Uptime     │
│ • FAQ        │     │ • Goals      │     │ • History    │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                  │
                     ┌────────────────────────────┤
                     │                            │
                     ▼                            ▼
              ┌──────────────┐            ┌──────────────┐
              │   APPROVED   │            │   REJECTED   │
              │              │            │              │
              │ • Genesis    │            │ • Reason     │
              │   ticket     │            │ • Retry      │
              │ • Setup guide│            │   guidance   │
              └──────┬───────┘            └──────────────┘
                     │
                     ▼
              ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
              │  NEOPHYTE    │────▶│   MEMBER     │────▶│    ADMIN     │
              │  (30 days)   │     │              │     │              │
              │              │     │ • Full share │     │ • Governance │
              │ • Probation  │     │ • Voting     │     │ • Vouching   │
              │ • 50% share  │     │ • Vouching   │     │ • Operations │
              └──────────────┘     └──────────────┘     └──────────────┘
```

### 2.4 Membership Requirements

#### Minimum Requirements (Auto-Checked)

| Requirement | Threshold | Rationale |
|-------------|-----------|-----------|
| Node Age | > 30 days | Proves commitment |
| Channels | ≥ 5 active | Minimum connectivity |
| Capacity | ≥ 10M sats | Meaningful contribution |
| Uptime | > 95% (30d) | Reliability |
| Force Closes | < 2 (6mo) | Good citizenship |
| cl_revenue_ops | Installed | Technical compatibility |

#### Soft Factors (Human Review)

- Node reputation (1ML, Amboss scores)
- Community involvement
- Geographic/network diversity value
- Stated goals alignment

### 2.5 Genesis Ticket System

```python
# Genesis ticket structure
{
    "ticket_id": "HIVE-2026-0142",
    "admin_pubkey": "03abc...",  # Issuing admin
    "hive_id": "HIVE-NEXUS-01",
    "applicant_pubkey": "02def...",
    "requirements": {
        "min_capacity_sats": 10_000_000,
        "min_channels": 5,
        "min_uptime_pct": 95
    },
    "issued_at": 1768900000,
    "expires_at": 1769504800,  # 7 days to activate
    "initial_tier": "neophyte",
    "probation_days": 30,
    "revenue_share_pct": 50,  # During probation
    "signature": "rsig..."  # Admin signature
}
```

### 2.6 Implementation Tasks

#### Phase 1: Portal Backend (Week 1-2)

| Task | Description | Tech |
|------|-------------|------|
| **1.1** | Application submission API | Python/FastAPI |
| **1.2** | Node verification service | Lightning API integration |
| **1.3** | Genesis ticket generation | Signing with admin key |
| **1.4** | Member authentication | LNURL-auth |
| **1.5** | Stats aggregation service | SQLite + MCP bridge |
| **1.6** | Webhook for hive events | cl-hive integration |

#### Phase 2: Portal Frontend (Week 2-3)

| Task | Description | Tech |
|------|-------------|------|
| **2.1** | Landing page | Static HTML/Tailwind |
| **2.2** | Application form | Form validation |
| **2.3** | Status checker | Real-time updates |
| **2.4** | Member dashboard | Charts, stats |
| **2.5** | Admin panel | Application review |

#### Phase 3: Hive Integration (Week 3-4)

| Task | Description |
|------|-------------|
| **3.1** | Auto-verification of genesis tickets |
| **3.2** | Probation period tracking |
| **3.3** | Automatic tier promotion |
| **3.4** | Revenue share calculation |
| **3.5** | Member health monitoring |
| **3.6** | Expulsion workflow |

#### Phase 4: Launch (Week 4)

| Task | Description |
|------|-------------|
| **4.1** | Beta test with trusted operators |
| **4.2** | Security audit of portal |
| **4.3** | Documentation and FAQ |
| **4.4** | Public announcement |
| **4.5** | Monitor and iterate |

### 2.7 Anti-Abuse Measures

| Measure | Implementation |
|---------|---------------|
| **Sybil Prevention** | One application per pubkey, cooldown on rejection |
| **Leech Detection** | Contribution ratio monitoring (existing) |
| **Bad Actor Removal** | Voting-based expulsion, automatic for violations |
| **Capacity Manipulation** | Check historical capacity, not just current |
| **Uptime Gaming** | Use multiple data sources (1ML, Amboss, direct) |

### 2.8 Revenue Sharing Model

```
┌─────────────────────────────────────────────────────────────┐
│                    REVENUE POOL                              │
│                                                              │
│  Total Pool = Sum of all hive forward fees                   │
│                                                              │
│  Distribution Formula:                                       │
│  ┌─────────────────────────────────────────────────────────┐│
│  │                                                          ││
│  │  member_share = pool_total × (                          ││
│  │      0.4 × capacity_ratio +     # 40% by capacity       ││
│  │      0.4 × forward_ratio +      # 40% by routing work   ││
│  │      0.2 × uptime_ratio         # 20% by reliability    ││
│  │  ) × tier_multiplier                                    ││
│  │                                                          ││
│  │  tier_multiplier:                                       ││
│  │    neophyte = 0.5  (probation)                          ││
│  │    member   = 1.0  (full share)                         ││
│  │    admin    = 1.0  (same as member)                     ││
│  │                                                          ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

---

## Part 3: Timeline & Milestones

### Overall Timeline: 6 Weeks

```
Week 1-2: cl_revenue_ops preparation
  ├── Hive-aware code refactoring
  ├── Standalone mode testing
  └── Documentation writing

Week 2-3: Portal development
  ├── Backend API
  ├── Frontend pages
  └── Integration testing

Week 3-4: Integration & testing
  ├── Hive ↔ Portal integration
  ├── Genesis ticket flow
  └── Beta testing

Week 4-5: Launch preparation
  ├── Security review
  ├── Documentation finalization
  └── Community outreach prep

Week 5-6: Public launch
  ├── cl_revenue_ops release
  ├── Portal goes live
  └── Monitoring & iteration
```

### Success Metrics

| Metric | Target (3 months) |
|--------|-------------------|
| cl_revenue_ops GitHub stars | 100+ |
| cl_revenue_ops installations | 50+ |
| Hive applications | 20+ |
| Hive members | 10+ |
| Combined hive capacity | 1B+ sats |
| Hive routing revenue | 100k+ sats/month |

---

## Part 4: Files to Create/Modify

### cl_revenue_ops Changes

| File | Change |
|------|--------|
| `cl_revenue_ops.py` | Add `--hive-enabled` option, auto-detection |
| `hive_interface.py` | NEW - Abstraction for hive communication |
| `fee_controller.py` | Optional hive callbacks |
| `rebalancer.py` | Optional hive callbacks |
| `README.md` | Comprehensive documentation |
| `QUICKSTART.md` | 5-minute setup guide |
| `CONTRIBUTING.md` | Contribution guidelines |

### Portal (New Repository: `hive-portal`)

| File | Purpose |
|------|---------|
| `api/main.py` | FastAPI application |
| `api/routes/applications.py` | Application submission/status |
| `api/routes/members.py` | Member dashboard data |
| `api/routes/admin.py` | Admin panel endpoints |
| `api/services/verification.py` | Node verification logic |
| `api/services/tickets.py` | Genesis ticket generation |
| `frontend/index.html` | Landing page |
| `frontend/apply.html` | Application form |
| `frontend/dashboard.html` | Member dashboard |
| `frontend/admin.html` | Admin panel |

### cl-hive Changes

| File | Change |
|------|--------|
| `modules/membership.py` | Portal ticket validation |
| `modules/contribution.py` | Revenue share calculation |
| `tools/mcp-hive-server.py` | Portal webhook endpoints |

---

## Decisions Made

1. **Hosting**: VPS at hive.lightning-goats.com served by nginx
2. **Domain**: hive.lightning-goats.com (subdomain)
3. **Identity**: TBD (LNURL-auth or signed message)
4. **Revenue Distribution**: Lightning via BOLT12 offers (all nodes are CLN)
5. **Release Order**: cl_revenue_ops first, then portal
6. **App Stores**: Target Umbrel, Start9, and RaspiBlitz

## Remaining Questions

1. **Governance**: How are admins selected/removed?
2. **Branding**: Consistent visual identity for hive ecosystem?
3. **Settlement Frequency**: Weekly? Bi-weekly?

---

## Part 5: BOLT12 Revenue Settlement System

### 5.1 Overview

Since all hive nodes run CLN, we use BOLT12 offers for trustless revenue settlement.
Each member creates a static offer; the coordinator fetches invoices and pays during settlement.

### 5.2 Settlement Algorithm

```python
def calculate_settlement(period_stats: Dict) -> List[Payment]:
    """Calculate net payments needed to balance the revenue pool."""

    total_pool = sum(m["fees_earned"] for m in period_stats["members"])

    payments = []

    for member in period_stats["members"]:
        # Calculate fair share based on contribution
        contribution_score = (
            0.40 * (member["capacity"] / period_stats["total_capacity"]) +
            0.40 * (member["forwards"] / period_stats["total_forwards"]) +
            0.20 * (member["uptime"] / 100)
        ) * member["tier_multiplier"]

        fair_share = total_pool * contribution_score
        balance = fair_share - member["fees_earned"]

        member["fair_share"] = fair_share
        member["balance"] = balance  # positive = receive, negative = pay

    # Separate into senders and receivers
    senders = [m for m in period_stats["members"] if m["balance"] < 0]
    receivers = [m for m in period_stats["members"] if m["balance"] > 0]

    # Sort for optimal matching
    senders.sort(key=lambda x: x["balance"])  # Most negative first
    receivers.sort(key=lambda x: x["balance"], reverse=True)  # Most positive first

    # Match senders to receivers (greedy algorithm)
    for sender in senders:
        remaining = abs(sender["balance"])

        for receiver in receivers:
            if receiver["balance"] <= 0:
                continue

            amount = min(remaining, receiver["balance"])
            if amount >= 1000:  # Minimum 1000 sats to avoid dust
                payments.append(Payment(
                    from_pubkey=sender["pubkey"],
                    to_pubkey=receiver["pubkey"],
                    to_offer=receiver["bolt12_offer"],
                    amount_msat=int(amount * 1000),
                    reason="hive_settlement"
                ))
                receiver["balance"] -= amount
                remaining -= amount

            if remaining < 1000:
                break

    return payments
```

### 5.3 Settlement Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    WEEKLY SETTLEMENT CYCLE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  DAY 7, 00:00 UTC - SNAPSHOT                                    │
│  ├── Collect fees_earned from each member                       │
│  ├── Collect forwards_count from each member                    │
│  ├── Collect uptime metrics                                     │
│  └── Lock snapshot (immutable)                                  │
│                                                                  │
│  DAY 7, 00:05 UTC - CALCULATION                                 │
│  ├── Calculate contribution scores                              │
│  ├── Calculate fair shares                                      │
│  ├── Calculate net balances                                     │
│  ├── Generate payment list                                      │
│  └── Publish proposed settlement for review                     │
│                                                                  │
│  DAY 7, 00:30 UTC - EXECUTION                                   │
│  ├── For each payment in list:                                  │
│  │   ├── Sender: fetchinvoice from receiver's BOLT12 offer     │
│  │   ├── Sender: pay invoice                                    │
│  │   └── Record preimage as proof                               │
│  └── Mark settlement complete                                   │
│                                                                  │
│  DAY 7, 01:00 UTC - VERIFICATION                                │
│  ├── All members can verify their payments                      │
│  ├── Coordinator publishes settlement receipt                   │
│  └── Reset counters for next period                             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 5.4 Member BOLT12 Offer Registration

```python
# Member setup (one-time)
async def register_settlement_offer(self, node_name: str) -> str:
    """Create and register BOLT12 offer for receiving settlement payments."""

    # Create offer on member's node
    result = await self.rpc.call("offer", {
        "amount": "any",
        "description": f"Hive Revenue Settlement - {self.hive_id}",
        "label": "hive-settlement"
    })

    offer = result["bolt12"]

    # Register with hive coordinator
    await self.hive.register_offer(
        pubkey=self.pubkey,
        offer=offer,
        offer_type="settlement"
    )

    return offer
```

### 5.5 Database Schema for Settlement

```sql
-- Settlement periods
CREATE TABLE settlement_periods (
    period_id TEXT PRIMARY KEY,
    start_time INTEGER NOT NULL,
    end_time INTEGER NOT NULL,
    status TEXT DEFAULT 'pending',  -- pending, calculating, executing, complete, failed
    total_pool_sats INTEGER,
    member_count INTEGER,
    payment_count INTEGER,
    created_at INTEGER DEFAULT (strftime('%s', 'now'))
);

-- Member snapshots per period
CREATE TABLE settlement_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_id TEXT NOT NULL,
    pubkey TEXT NOT NULL,
    fees_earned_sats INTEGER NOT NULL,
    forwards_count INTEGER NOT NULL,
    capacity_sats INTEGER NOT NULL,
    uptime_pct REAL NOT NULL,
    tier TEXT NOT NULL,
    contribution_score REAL,
    fair_share_sats INTEGER,
    balance_sats INTEGER,  -- positive = receive, negative = pay
    FOREIGN KEY (period_id) REFERENCES settlement_periods(period_id)
);

-- Settlement payments
CREATE TABLE settlement_payments (
    payment_id TEXT PRIMARY KEY,
    period_id TEXT NOT NULL,
    from_pubkey TEXT NOT NULL,
    to_pubkey TEXT NOT NULL,
    amount_msat INTEGER NOT NULL,
    bolt12_offer TEXT NOT NULL,
    bolt12_invoice TEXT,
    payment_preimage TEXT,
    status TEXT DEFAULT 'pending',  -- pending, fetching, paying, complete, failed
    executed_at INTEGER,
    error TEXT,
    FOREIGN KEY (period_id) REFERENCES settlement_periods(period_id)
);

-- Member offers
CREATE TABLE member_offers (
    pubkey TEXT PRIMARY KEY,
    bolt12_offer TEXT NOT NULL,
    registered_at INTEGER NOT NULL,
    last_verified INTEGER
);
```

---

## Part 6: Node App Store Packages

### 6.1 Umbrel App

**Structure:**
```
umbrel-cl-revenue-ops/
├── docker-compose.yml
├── umbrel-app.yml        # App manifest
├── exports.sh            # Environment exports
├── icon.svg              # 256x256 icon
└── gallery/
    ├── 1.png             # 1440x900 screenshots
    ├── 2.png
    └── 3.png
```

**umbrel-app.yml:**
```yaml
manifestVersion: 1
id: cl-revenue-ops
name: CL Revenue Ops
tagline: Intelligent fee optimization for Core Lightning
icon: https://raw.githubusercontent.com/lightninggoats/cl-revenue-ops/main/icon.svg
category: Lightning
version: "1.0.0"
port: 3847
description: >
  Hill climbing fee optimizer, profit-constrained rebalancing,
  and per-channel profitability tracking. Can run standalone
  or connect to a Lightning Hive for enhanced features.
developer: Lightning Goats
website: https://github.com/lightninggoats/cl-revenue-ops
repo: https://github.com/lightninggoats/cl-revenue-ops
support: https://github.com/lightninggoats/cl-revenue-ops/issues
dependencies:
  - core-lightning
```

### 6.2 Start9 Package

**Structure:**
```
cl-revenue-ops-startos/
├── Dockerfile
├── docker_entrypoint.sh
├── manifest.yaml
├── instructions.md
├── icon.png
├── prepare.sh           # Build environment setup
└── Makefile
```

**manifest.yaml:**
```yaml
id: cl-revenue-ops
title: CL Revenue Ops
version: 1.0.0
release-notes: Initial release
license: MIT
wrapper-repo: https://github.com/lightninggoats/cl-revenue-ops-startos
upstream-repo: https://github.com/lightninggoats/cl-revenue-ops
support-site: https://github.com/lightninggoats/cl-revenue-ops/issues
marketing-site: https://hive.lightning-goats.com
description:
  short: Fee optimization for Core Lightning
  long: |
    Hill climbing fee optimizer, profit-constrained rebalancing,
    and per-channel profitability tracking for CLN nodes.
assets:
  icon: icon.png
  instructions: instructions.md
main:
  type: docker
  image: main
  entrypoint: docker_entrypoint.sh
dependencies:
  c-lightning:
    version: ">=23.0.0"
    requirement: required
```

### 6.3 RaspiBlitz Bonus Script

**bonus.cl-revenue-ops.sh:**
```bash
#!/bin/bash
# cl-revenue-ops bonus script for RaspiBlitz

# command info
if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "-help" ]; then
  echo "Config script for cl-revenue-ops"
  echo "bonus.cl-revenue-ops.sh [on|off|menu|status]"
  exit 0
fi

source /mnt/hdd/raspiblitz.conf

# status
if [ "$1" = "status" ]; then
  if [ -d "/home/bitcoin/cl-revenue-ops" ]; then
    echo "installed=1"
  else
    echo "installed=0"
  fi
  exit 0
fi

# install
if [ "$1" = "on" ] || [ "$1" = "1" ]; then
  echo "*** INSTALL CL-REVENUE-OPS ***"

  cd /home/bitcoin
  git clone https://github.com/lightninggoats/cl-revenue-ops.git
  cd cl-revenue-ops

  # Link to CLN plugins directory
  ln -s /home/bitcoin/cl-revenue-ops/cl_revenue_ops.py \
        /home/bitcoin/.lightning/plugins/cl_revenue_ops.py

  # Restart CLN to load plugin
  sudo systemctl restart lightningd

  echo "*** CL-REVENUE-OPS INSTALLED ***"
  exit 0
fi

# uninstall
if [ "$1" = "off" ]; then
  echo "*** UNINSTALL CL-REVENUE-OPS ***"

  rm -f /home/bitcoin/.lightning/plugins/cl_revenue_ops.py
  rm -rf /home/bitcoin/cl-revenue-ops
  sudo systemctl restart lightningd

  echo "*** CL-REVENUE-OPS REMOVED ***"
  exit 0
fi
```

### 6.4 Self-Hosted Community App Store (Umbrel)

We can host our own Umbrel community app store at hive.lightning-goats.com:

**Structure:**
```
hive-app-store/
├── umbrel-app-store.yml
├── cl-revenue-ops/
│   ├── docker-compose.yml
│   ├── umbrel-app.yml
│   └── ...
└── cl-hive/
    ├── docker-compose.yml
    ├── umbrel-app.yml
    └── ...
```

**umbrel-app-store.yml:**
```yaml
id: hive-apps
name: Lightning Hive Apps
tagline: Apps for Lightning node operators and Hive members
```

Users add the store URL in Umbrel settings, then can install our apps directly.

---

## Part 7: Revised Timeline

### Phase 1: cl_revenue_ops Release (Weeks 1-3)

| Week | Tasks |
|------|-------|
| **1** | Code refactoring for standalone mode, hive interface abstraction |
| **2** | Documentation, testing on standalone CLN node |
| **3** | Umbrel/RaspiBlitz packaging, public release |

### Phase 2: Settlement System (Weeks 3-4)

| Week | Tasks |
|------|-------|
| **3** | BOLT12 offer registration system |
| **4** | Settlement calculation and execution engine |

### Phase 3: Portal Development (Weeks 4-6)

| Week | Tasks |
|------|-------|
| **4** | Backend API (FastAPI on VPS) |
| **5** | Frontend pages, nginx setup at hive.lightning-goats.com |
| **6** | Integration testing, beta launch |

### Phase 4: App Store Expansion (Weeks 6-8)

| Week | Tasks |
|------|-------|
| **6** | Start9 package submission |
| **7** | Self-hosted Umbrel community store |
| **8** | Monitoring, iteration, community feedback |

---

## Part 8: Success Metrics

| Metric | 1 Month | 3 Months | 6 Months |
|--------|---------|----------|----------|
| cl_revenue_ops installs | 20 | 100 | 300 |
| GitHub stars | 30 | 150 | 500 |
| Hive applications | 5 | 30 | 100 |
| Active hive members | 3 | 15 | 50 |
| Combined hive capacity | 500M sats | 2B sats | 10B sats |
| Monthly settlement volume | 50k sats | 500k sats | 2M sats |

---

## Next Steps

1. ✅ Plan approved
2. Begin cl_revenue_ops refactoring for standalone mode
3. Create hive_interface.py abstraction layer
4. Write comprehensive documentation
5. Package for Umbrel (first target)
6. Public release and announcement
