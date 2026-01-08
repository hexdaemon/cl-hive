# cl-hive

**The Coordination Layer for Core Lightning Fleets.**

## Overview
`cl-hive` is a Core Lightning plugin that enables "Swarm Intelligence" across independent nodes. It transforms a group of disparate Lightning nodes into a coordinated fleet that shares state, optimizes topology, and manages liquidity collectively.

### Core Features
- **Secure PKI Handshake:** Cryptographic authentication using Core Lightning's HSM-bound keys. No external crypto libraries required.
- **Shared State (HiveMap):** Efficient gossip protocol with anti-entropy (state hashing) ensures all members have a consistent view of fleet capacity and topology.
- **Intent Lock Protocol:** Deterministic conflict resolution prevents "thundering herd" issues when multiple nodes attempt the same coordinated action.
- **Topology Planner (The Gardner):** Automated algorithm that detects saturated targets and proposes expansions to underserved high-value peers.
- **Hierarchical Membership:** Supports `admin`, `member`, and `neophyte` tiers with algorithmic promotion based on uptime and contribution.
- **Paranoid Bridge:** Resilient integration with `cl-revenue-ops` and `clboss` using the Circuit Breaker pattern.

## Relationship to cl-revenue-ops
`cl-hive` acts as the **"Diplomat"** or **"Chief Strategy Officer"** that communicates with other nodes in the fleet. It is designed to work alongside [cl-revenue-ops](https://github.com/LightningGoats/cl-revenue-ops), which acts as the **"CFO"** managing local channel profitability and fee policies.

## Governance Modes
`cl-hive` supports three distinct governance modes to match your risk profile:
- **Advisor:** The plugin logs recommendations and queues actions for manual approval.
- **Autonomous:** The plugin executes actions automatically within strict safety bounds.
- **Oracle:** The plugin consults an external API for final decision-making.

## Installation

### Prerequisites
- Core Lightning (CLN) v23.05+
- Python 3.8+
- `cl-revenue-ops` v1.4.0+ (Recommended for full functionality)

### Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/santyr/cl-hive.git
   cd cl-hive
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start CLN with the plugin:
   ```bash
   lightningd --plugin=/path/to/cl-hive/cl-hive.py
   ```

## Usage

### Core Commands
- `hive-status`: Get current membership tier, fleet size, and governance mode.
- `hive-genesis`: Initialize as the founding Admin of a new Hive.
- `hive-invite`: Generate an invitation ticket for a new member.
- `hive-join <ticket>`: Join an existing Hive using an invitation ticket.
- `hive-members`: List all Hive members and their current stats.

### Topology & Planning
- `hive-topology`: View saturation analysis and underserved targets.
- `hive-planner-log`: Review recent decisions made by the Gardner algorithm.

## Configuration
All options can be set in your CLN config file or passed as CLI arguments:
- `hive-governance-mode`: `advisor` (default), `autonomous`, or `oracle`.
- `hive-market-share-cap`: Maximum Hive market share per target (default: `0.20`).
- `hive-planner-interval`: Seconds between optimization cycles (default: `3600`).
- `hive-membership-enabled`: Enable/disable promotion and vouching (default: `true`).

## Documentation
- [Phase 6 Plan: Topology Optimization](docs/planning/PHASE6_PLAN.md)
- [Implementation Roadmap](docs/planning/IMPLEMENTATION_PLAN.md)
- [Threat Model](docs/planning/PHASE6_THREAT_MODEL.md)

## License
MIT