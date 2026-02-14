# The Hive: Swarm Intelligence for Lightning Node Operators

**Turn your solo Lightning node into part of a coordinated fleet.**

---
![Image](https://r2.primal.net/cache/9/97/87/9978775eca7fbe1f5f78548d888580613a8080ec826080580024d98526fdd4e6.png)

## The Problem with Running a Lightning Node Alone

If you run a Lightning routing node, you know the struggle. You're competing against nodes with more capital, better connections, and teams of developers optimizing their operations. You spend hours analyzing channels, adjusting fees, and rebalancing—only to watch your carefully positioned liquidity drain to zero while larger operators capture the flow.

The economics are brutal: rebalancing costs eat your margins, fee competition drives rates to zero, and you're always one step behind the market. Most solo operators earn less than 1% annual return on their capital. Many give up entirely.

**What if there was another way?**

---

## Introducing The Hive

The Hive is an open-source coordination layer that transforms independent Lightning nodes into a unified fleet. Think of it as forming a guild with other node operators—you remain fully independent and sovereign over your funds, but you gain the collective intelligence and coordination benefits of operating together.

Built on two Core Lightning plugins:
- **cl-hive**: The coordination layer ("The Diplomat")
- **cl-revenue-ops**: The execution layer ("The CFO")

Together, they implement what we call "Swarm Intelligence"—the same principles that allow ant colonies and bee hives to solve complex optimization problems through simple local rules and information sharing.

---

## How It Works

### Zero-Fee Internal Routing

The most immediate benefit: **hive members route through each other at zero fees**.

When you need to rebalance a channel, instead of paying 50-200 PPM to route through the public network, you route through your fleet members for free. This single feature can reduce your operating costs by 30-50%.

Your external channels still earn fees from the network. But internal fleet channels become free highways for moving your own liquidity.

### Coordinated Fee Optimization

Solo operators face a dilemma: lower fees to attract flow, or raise fees to capture margin? Lower your fees and your neighbor undercuts you. Raise them and traffic disappears.

Hive members share fee intelligence through a system inspired by how ants leave pheromone trails. When one member discovers an optimal fee point, that information propagates through the fleet. Members coordinate instead of competing—the rising tide lifts all boats.

The fee algorithm uses **Thompson Sampling**, a Bayesian approach that balances exploration and exploitation. It learns what fees work for each channel while avoiding the race-to-the-bottom that plagues solo operators.

### Predictive Liquidity Positioning

The hive uses **Kalman filtering** to predict flow patterns before they happen. By analyzing velocity trends across the fleet, it detects when demand is about to spike on a particular corridor.

This means liquidity is pre-positioned *before* channels deplete—capturing routing fees that solo operators miss because they're always reacting rather than anticipating.

### Fleet-Wide Rebalancing Optimization

When rebalancing is needed, the hive doesn't just find *a* route—it finds the **globally optimal** set of movements using Min-Cost Max-Flow algorithms.

Instead of three members independently trying to rebalance (potentially competing for the same routes), the MCF solver computes which member should move what amount through which path to satisfy everyone's needs with minimum total cost.

### Portfolio Theory for Channels

The hive applies **Markowitz Mean-Variance optimization** to channel management. Instead of optimizing each channel in isolation, it treats your channels as a portfolio and optimizes for risk-adjusted returns (Sharpe ratio).

This surfaces insights like:
- Which channels are hedging each other (negatively correlated)
- Where you have concentration risk (highly correlated channels)
- How to allocate liquidity for maximum risk-adjusted return

### The Routing Pool: Collective Revenue Sharing

This is a new concept for Lightning: **pooled routing revenue with weighted distribution**.

Here's the problem with traditional routing: one node might have perfectly positioned liquidity that enables a route, but a different node in the path actually earns the fee. The node providing the strategic position gets nothing. Over time, this creates misaligned incentives—why maintain expensive liquidity positions if someone else captures the value?

The hive solves this with a **Routing Pool**. Members contribute to a collective revenue pool, and distributions are calculated based on weighted contributions:

| Factor | Weight | What It Measures |
|--------|--------|------------------|
| **Capital** | 70% | Liquidity committed to fleet channels |
| **Operations** | 10% | Uptime, reliability, responsiveness |
| **Position** | 20% | Strategic value of your network position |

At the end of each period, pool revenue is distributed proportionally. A node with great positioning but modest capital still earns from routes it helped enable. A node with large capital but poor positioning earns less than raw capacity would suggest.

**Why this matters:**

- **Aligned incentives**: Everyone benefits when the fleet succeeds
- **Fair compensation**: Strategic positioning is rewarded, not just raw capital
- **Reduced competition**: Members cooperate to maximize pool revenue rather than competing for individual fees
- **Smoothed returns**: High-variance routing income becomes more predictable

The pool is transparent—every member can see contributions, revenue, and distributions. Settlement happens on a configurable schedule (weekly by default). No trust required: it's math, not promises.

---

## The Technical Stack

Both plugins are written in Python for Core Lightning:

**cl-hive** handles:
- PKI authentication using CLN's HSM (no external crypto libraries)
- Gossip protocol with anti-entropy (consistent fleet state)
- Intent Lock protocol (prevents "thundering herd" race conditions)
- Membership tiers (Member → Neophyte with algorithmic promotion)
- Topology planning and expansion coordination
- Splice coordination between members

**cl-revenue-ops** handles:
- Thompson Sampling + AIMD fee optimization
- EV-based rebalancing with sling integration
- Kalman-filtered flow analysis
- Per-peer policy management
- Portfolio optimization
- Profitability tracking and reporting

The architecture is deliberately layered: cl-hive coordinates *what* should happen, cl-revenue-ops executes *how* it happens. You can run cl-revenue-ops standalone for significant benefits, or connect to a hive for the full experience.

---

## What You Keep

**Full sovereignty.** Your keys never leave your node. Your funds never leave your channels. The hive shares *information*, never sats.

Each node makes independent decisions about its own operations. The hive provides intelligence and coordination, but you remain in complete control. You can disconnect at any time with zero impact to your funds.

**Your node identity.** You don't become anonymous or hidden. You keep your pubkey, your reputation, your existing channels. Joining the hive adds capability without taking anything away.

---

## The Membership Model

The hive uses a three-tier membership system:

**Neophyte** (Probation Period)
- 90-day probation to prove reliability
- Discounted internal fees (not quite zero)
- Read-only access to fleet intelligence
- Must maintain >99% uptime and positive contribution ratio

**Member** (Full Access)
- Zero-fee internal routing
- Full participation in fee coordination
- Push and pull rebalancing privileges
- Voting rights on governance decisions
- Can invite new members

Promotion from Neophyte to Member is algorithmic—based on uptime, contribution ratio, and topological value. No politics, no favoritism. Prove your value and you're promoted automatically.

---

## Real Numbers

Our fleet currently operates three nodes with 47 channels:

| Node | Capacity | Channels |
|------|----------|----------|
| Hive-Nexus-01 | 268,227,946 sats (~2.68 BTC) | 37 |
| Hive-Nexus-02 | 19,582,893 sats (~0.20 BTC) | 8 |
| cyber-hornet-1 | 3,550,000 sats (~0.04 BTC) | 2 |
| **Total Fleet** | **~291M sats (~2.91 BTC)** | **47** |

Expected benefits based on the architecture:

- **Rebalancing costs**: Significantly reduced due to zero-fee internal routing (external rebalancing typically costs 50-200 PPM)
- **Fee optimization**: Thompson Sampling provides systematic Bayesian exploration vs. manual guesswork
- **Operational overhead**: AI-assisted decision queues replace hours of manual channel analysis

As the hive grows, these benefits compound. More members mean more internal routing paths, better flow prediction, and stronger market positioning.

---

## Governance: Advisor Mode

The hive defaults to **Advisor Mode**—a human-in-the-loop governance model where the system proposes actions and humans approve them.

Channel opens, fee changes, and rebalances are queued as "pending actions" that you review before execution. An MCP server provides Claude Code integration, enabling AI-assisted fleet management while keeping humans in control of all fund movements.

For operators who want more automation, there's an Autonomous mode with strict safety bounds. But we recommend starting with Advisor mode until you trust the system.

---

## How to Join

### Step 1: Connect to Our Nodes

Open channels to one or more of our fleet members:

**cyber-hornet-1**
```
03796a3c5b18080db99b0b880e2e326db9f5eb6bf3d7394b924f633da3eae31412@ch36z4vnycie5y4aibq7ve226reqheow7ltyy5kaulsh2yypz56aqsid.onion:9736
```

**Hive-Nexus-01**
```
0382d558331b9a0c1d141f56b71094646ad6111e34e197d47385205019b03afdc3@45.76.234.192:9735
```

**Hive-Nexus-02**
```
03fe48e8a64f14fa0aa7d9d16500754b3b906c729acfb867c00423fd4b0b9b56c2@45.76.234.192:9736
```

### Step 2: Install the Plugins

#### Option A: Docker (Easiest)

Spin up a complete node with all plugins pre-configured in minutes:

```bash
git clone https://github.com/lightning-goats/cl-hive
cd cl-hive/docker
cp .env.example .env        # Edit with your settings
docker-compose up -d
```

That's it. You get Core Lightning, cl-hive, cl-revenue-ops, and all dependencies in a single container. Hot upgrades are simple:

```bash
./scripts/hot-upgrade.sh
```

#### Option B: Manual Installation

For existing Core Lightning nodes (v23.05+):

```bash
# Clone the plugins
git clone https://github.com/lightning-goats/cl-hive
git clone https://github.com/lightning-goats/cl_revenue_ops

# Install dependencies
pip install pyln-client>=24.0

# Copy plugins to your CLN plugin directory
cp cl-hive/cl-hive.py ~/.lightning/plugins/
cp -r cl-hive/modules ~/.lightning/plugins/cl-hive-modules
cp cl_revenue_ops/cl-revenue-ops.py ~/.lightning/plugins/
cp -r cl_revenue_ops/modules ~/.lightning/plugins/cl-revenue-ops-modules

# Enable in your config (~/.lightning/config)
echo "plugin=/home/YOUR_USER/.lightning/plugins/cl-hive.py" >> ~/.lightning/config
echo "plugin=/home/YOUR_USER/.lightning/plugins/cl-revenue-ops.py" >> ~/.lightning/config

# Restart lightningd
lightning-cli stop && lightningd
```

**Note:** cl-revenue-ops requires the [sling](https://github.com/daywalker90/sling) plugin for rebalancing.

### Step 3: Request an Invite

Once your node is connected and plugins are running, reach out to request an invite ticket. We'll verify your node is healthy and issue a ticket that lets you join as a Neophyte.

### Step 4: Prove Your Value

During your 90-day probation:
- Maintain >99% uptime
- Route traffic for the fleet (contribution ratio ≥ 1.0)
- Connect to at least one peer the hive doesn't already cover

Meet these criteria and you'll be automatically promoted to full Member status with zero-fee internal routing.

---

## The Vision

Lightning's routing layer has a centralization problem. A handful of large nodes capture most of the flow because they have the capital and engineering resources to optimize at scale.

The hive is our answer: **give independent operators the same coordination benefits through open-source software**.

We're not building a company or a walled garden. The code is open source (MIT licensed). The protocol is documented. Anyone can fork it, run their own hive, or improve the algorithms.

Our goal is a Lightning network with many competing hives—each providing coordination benefits to their members while the hives themselves compete and cooperate at a higher level. A truly decentralized routing layer built on cooperation rather than pure competition.

---

## Get Involved

**Run the plugins**: Even without joining a hive, cl-revenue-ops provides significant value as a standalone fee optimizer and rebalancer.

**GitHub**:
- [cl-hive](https://github.com/lightning-goats/cl-hive)
- [cl-revenue-ops](https://github.com/lightning-goats/cl_revenue_ops)

**Open a channel**: Connect to our nodes listed above. Even if you don't join the hive immediately, you'll be routing with well-maintained nodes running cutting-edge optimization.

**Contribute**: Found a bug? Have an idea? PRs welcome. The hive gets smarter with every contributor.

---

## Frequently Asked Questions

**Q: Do I need to trust the other hive members with my funds?**

No. Funds never leave your node. The hive coordinates information—routing intelligence, fee recommendations, rebalance suggestions—but every action on your node is executed by your node. Your keys, your coins.

**Q: What if a hive member goes rogue?**

The membership system includes contribution tracking and ban mechanisms. Members who leech without contributing can be removed by vote. The governance mode also lets you review all proposed actions before execution.

**Q: Can I run cl-revenue-ops without cl-hive?**

Yes. cl-revenue-ops works fully standalone. You get Thompson Sampling fees, EV-based rebalancing, Kalman flow analysis, and portfolio optimization without any fleet coordination. Many operators start here before joining a hive.

**Q: What about privacy?**

Hive members share operational data: channel capacities, fee policies, flow patterns. They do not share payment data, invoices, or customer information. The gossip protocol is encrypted between members.

**Q: How much capital do I need?**

There's no minimum, but routing economics generally favor nodes with at least a few million sats in well-connected channels. Smaller nodes benefit more from the cost reduction (zero-fee internal routing) than from routing revenue.

---

## The Bottom Line

Running a Lightning node alone is hard. The margins are thin, the competition is fierce, and the operational overhead is significant.

The hive doesn't eliminate these challenges—but it gives you allies. Zero-fee internal routing cuts your costs. Coordinated fee optimization prevents races to the bottom. Predictive liquidity captures flow you'd otherwise miss.

You stay sovereign. You stay independent. But you're no longer alone.

**Join the hive.**

---

*The Hive is an open-source project by the Lightning Goats team. No venture funding, no token, no bullshit—just node operators helping each other succeed.*
