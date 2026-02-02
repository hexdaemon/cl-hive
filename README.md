# cl-hive

**The Coordination Layer for Core Lightning Fleets.**

## Overview

`cl-hive` is a Core Lightning plugin that enables "Swarm Intelligence" across independent nodes. It transforms a group of disparate Lightning nodes into a coordinated fleet that shares state, optimizes topology, and manages liquidity collectively.

## Architecture

```
cl-hive (Coordination Layer - "The Diplomat")
    ↓
cl-revenue-ops (Execution Layer - "The CFO")
    ↓
Core Lightning
```

`cl-hive` acts as the **"Diplomat"** or **"Chief Strategy Officer"** that communicates with other nodes in the fleet. It works alongside [cl-revenue-ops](https://github.com/lightning-goats/cl_revenue_ops), which acts as the **"CFO"** managing local channel profitability and fee policies.

## Core Features

### Secure PKI Handshake
Cryptographic authentication using Core Lightning's HSM-bound keys. No external crypto libraries required.

### Shared State (HiveMap)
Efficient gossip protocol with anti-entropy (state hashing) ensures all members have a consistent view of fleet capacity and topology.

### Intent Lock Protocol
Deterministic conflict resolution prevents "thundering herd" issues when multiple nodes attempt the same coordinated action.

### Topology Planner (The Gardner)
Automated algorithm that detects saturated targets and proposes expansions to underserved high-value peers. Includes feerate gate to prevent expensive channel opens during high-fee periods.

### Hierarchical Membership
Supports `admin`, `member`, and `neophyte` tiers with algorithmic promotion based on uptime and contribution.

### Cooperative Fee Coordination
Fleet-wide fee intelligence sharing and aggregation for coordinated fee strategies.

### No Node Left Behind (NNLB)
Health monitoring and liquidity needs detection across the fleet.

### Coordinated Splicing (Phase 11)
Automated splice operations between hive members with full PSBT exchange workflow. Resize channels without closing them - splice-in to add capacity, splice-out to remove.

### Min-Cost Max-Flow Optimization (MCF)
Global fleet-wide rebalancing optimization using Successive Shortest Paths algorithm. Automatically prefers zero-fee hive internal channels, prevents circular flows, and coordinates simultaneous rebalances across the fleet with version-aware coordinator election and staleness-based failover.

### Anticipatory Liquidity Management
Predictive liquidity positioning using Kalman-filtered flow velocity estimation and intra-day pattern detection. Detects temporal patterns (surge, drain, quiet periods) and recommends proactive rebalancing before demand spikes.

### VPN Transport Support
Optional WireGuard VPN integration for secure fleet communication.

## Governance Modes

| Mode | Behavior |
|------|----------|
| `advisor` | Log recommendations and queue actions for manual approval (default) |
| `autonomous` | Execute actions automatically within strict safety bounds |

## Join the Lightning Hive

Want to join an existing hive fleet? The Lightning Hive is actively accepting new members.

**Current Hive Nodes:**

| Node | Connection |
|------|------------|
| ⚡Lightning Goats CLN⚡ | `0382d558331b9a0c1d141f56b71094646ad6111e34e197d47385205019b03afdc3@45.76.234.192:9735` |
| Hive-Nexus-02 | `03fe48e8a64f14fa0aa7d9d16500754b3b906c729acfb867c00423fd4b0b9b56c2@45.76.234.192:9736` |

**To join:**
1. Run your own CLN node with cl-hive
2. Request an invite ticket via [Nostr](https://njump.me/hex@lightning-goats.com) or [GitHub Issues](https://github.com/lightning-goats/cl-hive/issues)
3. Open a channel to a hive member (skin in the game)
4. Use the ticket to join as a neophyte
5. Get vouched by existing members for full membership

See [Joining the Hive](docs/JOINING_THE_HIVE.md) for the complete guide.

## Installation

### Prerequisites
- Core Lightning (CLN) v23.05+
- Python 3.8+
- `cl-revenue-ops` v1.4.0+ (Recommended for full functionality)

### Optional Integrations
- **CLBoss**: Not required. If installed, cl-hive coordinates to prevent redundant channel opens.
- **Sling**: Not required for cl-hive. Rebalancing is handled by cl-revenue-ops.

### Setup

```bash
# Clone the repository
git clone https://github.com/lightning-goats/cl-hive.git
cd cl-hive

# Install dependencies
pip install -r requirements.txt

# Start CLN with the plugin
lightningd --plugin=/path/to/cl-hive/cl-hive.py
```

## RPC Commands

### Hive Management

| Command | Description |
|---------|-------------|
| `hive-genesis` | Initialize as the founding Admin of a new Hive |
| `hive-invite` | Generate an invitation ticket for a new member |
| `hive-join <ticket>` | Join an existing Hive using an invitation ticket |
| `hive-leave` | Leave the current Hive |
| `hive-status` | Get current membership tier, fleet size, and governance mode |
| `hive-members` | List all Hive members and their current stats |
| `hive-config` | View current configuration |
| `hive-set-mode <mode>` | Change governance mode (advisor/autonomous/oracle) |

### Membership & Governance

| Command | Description |
|---------|-------------|
| `hive-vouch <peer_id>` | Vouch for a neophyte's promotion to member |
| `hive-request-promotion` | Request promotion from neophyte to member |
| `hive-force-promote <peer_id>` | Admin: Force-promote a member |
| `hive-promote-admin <peer_id>` | Admin: Nominate a member for admin promotion |
| `hive-pending-admin-promotions` | List pending admin promotion requests |
| `hive-resign-admin` | Resign from admin role |
| `hive-ban <peer_id>` | Admin: Ban a member from the Hive |
| `hive-propose-ban <peer_id>` | Propose a ban for member vote |
| `hive-vote-ban <ban_id> <vote>` | Vote on a pending ban proposal |
| `hive-pending-bans` | List pending ban proposals |
| `hive-contribution` | View contribution stats for all members |

### Topology & Planning

| Command | Description |
|---------|-------------|
| `hive-topology` | View saturation analysis and underserved targets |
| `hive-planner-log` | Review recent decisions made by the Gardner algorithm |
| `hive-calculate-size <target>` | Calculate optimal channel size for a target |
| `hive-enable-expansions <true/false>` | Enable/disable expansion proposals |

### Cooperative Expansion

| Command | Description |
|---------|-------------|
| `hive-expansion-status` | View current expansion election status |
| `hive-expansion-nominate <target>` | Nominate a target for fleet expansion |
| `hive-expansion-elect <target>` | Trigger election for expansion to target |

### Intent Protocol

| Command | Description |
|---------|-------------|
| `hive-intent-status` | View active intent locks |
| `hive-test-intent <target> <action>` | Test intent protocol (debug) |

### Pending Actions (Advisor Mode)

| Command | Description |
|---------|-------------|
| `hive-pending-actions` | List actions awaiting approval |
| `hive-approve-action <id or "all">` | Approve pending action(s) |
| `hive-reject-action <id or "all">` | Reject pending action(s) |
| `hive-budget-summary` | View budget usage and limits |

### Fee Coordination

| Command | Description |
|---------|-------------|
| `hive-fee-profiles` | View fee profiles for all Hive members |
| `hive-fee-recommendation <target>` | Get fee recommendation for a target |
| `hive-fee-intelligence` | View aggregated fee intelligence |
| `hive-aggregate-fees` | Aggregate fee data from all members |
| `hive-trigger-fee-broadcast` | Manually trigger fee profile broadcast |

### Health & Monitoring

| Command | Description |
|---------|-------------|
| `hive-member-health` | View health status of all members |
| `hive-calculate-health <peer_id>` | Calculate health score for a peer |
| `hive-nnlb-status` | View No Node Left Behind status |
| `hive-trigger-health-report` | Manually trigger health report |
| `hive-trigger-all` | Trigger all periodic broadcasts |

### Routing & Reputation

| Command | Description |
|---------|-------------|
| `hive-routing-stats` | View routing statistics |
| `hive-route-suggest <destination>` | Get route suggestions through Hive |
| `hive-peer-reputations` | View peer reputation scores |
| `hive-reputation-stats` | View aggregated reputation statistics |

### Liquidity

| Command | Description |
|---------|-------------|
| `hive-liquidity-needs` | View liquidity needs across the fleet |
| `hive-liquidity-status` | View current liquidity status |

### Peer Quality & Events

| Command | Description |
|---------|-------------|
| `hive-peer-quality` | View peer quality metrics |
| `hive-quality-check` | Run quality check on all peers |
| `hive-peer-events` | View recent peer events |
| `hive-channel-opened <scid>` | Record channel open event |
| `hive-channel-closed <scid>` | Record channel close event |

### Splice Coordination

| Command | Description |
|---------|-------------|
| `hive-splice <channel_id> <amount>` | Execute coordinated splice with hive member (positive=in, negative=out) |
| `hive-splice-status [session_id]` | View active splice sessions |
| `hive-splice-abort <session_id>` | Abort an active splice session |
| `hive-splice-check <peer_id> <type> <amount>` | Check if splice is safe for fleet connectivity |
| `hive-splice-recommendations <peer_id>` | Get splice recommendations for a peer |

### VPN Transport

| Command | Description |
|---------|-------------|
| `hive-vpn-status` | View VPN transport status |
| `hive-vpn-add-peer <pubkey> <addr>` | Add a VPN peer mapping |
| `hive-vpn-remove-peer <pubkey>` | Remove a VPN peer mapping |

### MCF Optimization

| Command | Description |
|---------|-------------|
| `hive-mcf-status` | View MCF solver state and coordinator election |
| `hive-mcf-solve` | Trigger manual MCF optimization cycle |
| `hive-mcf-assignments` | View pending/completed rebalance assignments |
| `hive-mcf-path <from> <to> <amount>` | Get optimized routing path |
| `hive-mcf-health` | View MCF solver health metrics |

### Bridge & Debug

| Command | Description |
|---------|-------------|
| `hive-reinit-bridge` | Reinitialize the cl-revenue-ops bridge |
| `hive-test-pending-action` | Create test pending action (debug) |

## Configuration Options

All options can be set in your CLN config file or passed as CLI arguments. Most options support hot-reload via `lightning-cli setconfig`.

### Core Settings

| Option | Default | Description |
|--------|---------|-------------|
| `hive-db-path` | `~/.lightning/cl_hive.db` | SQLite database path (immutable) |
| `hive-governance-mode` | `advisor` | Governance mode: advisor, autonomous, oracle |
| `hive-max-members` | `50` | Maximum Hive members (Dunbar cap) |

### Membership Settings

| Option | Default | Description |
|--------|---------|-------------|
| `hive-membership-enabled` | `true` | Enable membership & promotion protocol |
| `hive-probation-days` | `30` | Minimum days as Neophyte before promotion |
| `hive-vouch-threshold` | `0.51` | Percentage of vouches required (51%) |
| `hive-min-vouch-count` | `3` | Minimum number of vouches required |
| `hive-auto-vouch` | `true` | Auto-vouch for eligible neophytes |
| `hive-auto-promote` | `true` | Auto-promote when quorum reached |
| `hive-ban-autotrigger` | `false` | Auto-trigger ban on sustained leeching |

### Fee Settings

| Option | Default | Description |
|--------|---------|-------------|
| `hive-neophyte-fee-discount` | `0.5` | Fee discount for Neophytes (50%) |
| `hive-member-fee-ppm` | `0` | Fee for full members (0 = free) |

### Planner Settings

| Option | Default | Description |
|--------|---------|-------------|
| `hive-planner-interval` | `3600` | Planner cycle interval (seconds) |
| `hive-planner-enable-expansions` | `false` | Enable expansion proposals |
| `hive-planner-min-channel-sats` | `1000000` | Minimum expansion channel size |
| `hive-planner-max-channel-sats` | `50000000` | Maximum expansion channel size |
| `hive-planner-default-channel-sats` | `5000000` | Default expansion channel size |
| `hive-market-share-cap` | `0.20` | Maximum market share per target (20%) |
| `hive-max-expansion-feerate` | `5000` | Max feerate (sat/kB) for expansions |

### Protocol Settings

| Option | Default | Description |
|--------|---------|-------------|
| `hive-intent-hold-seconds` | `60` | Intent hold period for conflict resolution |
| `hive-gossip-threshold` | `0.10` | Capacity change threshold for gossip (10%) |
| `hive-heartbeat-interval` | `300` | Heartbeat broadcast interval (5 min) |

### Budget Settings (Autonomous Mode)

| Option | Default | Description |
|--------|---------|-------------|
| `hive-autonomous-budget-per-day` | `10000000` | Daily budget for autonomous opens (sats) |
| `hive-budget-reserve-pct` | `0.20` | Reserve percentage of onchain balance |
| `hive-budget-max-per-channel-pct` | `0.50` | Max per-channel spend of daily budget |

### VPN Transport Settings

| Option | Default | Description |
|--------|---------|-------------|
| `hive-transport-mode` | `any` | Transport mode: any, vpn-only, vpn-preferred |
| `hive-vpn-subnets` | `` | VPN subnets (CIDR, comma-separated) |
| `hive-vpn-bind` | `` | VPN bind address (ip:port) |
| `hive-vpn-peers` | `` | VPN peer mappings (pubkey@ip:port) |
| `hive-vpn-required-messages` | `all` | Messages requiring VPN: all, gossip, intent, sync, none |

## AI Agent Integration (MCP Server)

The `mcp-hive-server.py` provides Model Context Protocol (MCP) tools for AI-assisted fleet management. Works with any MCP-compatible agent: Moltbots, Claude Code, Clawdbot, or similar.

```
"Show me the status of all hive nodes"
"What pending actions need approval?"
"Check the revenue dashboard for both nodes"
```

See:
- [MOLTY.md](MOLTY.md) - Agent instructions for using cl-hive tools
- [MCP Server Documentation](docs/MCP_SERVER.md) - Full setup and tool reference

## Documentation

| Document | Description |
|----------|-------------|
| [Joining the Hive](docs/JOINING_THE_HIVE.md) | How to join an existing hive |
| [MOLTY.md](MOLTY.md) | AI agent instructions |
| [MCP Server](docs/MCP_SERVER.md) | MCP server setup and tool reference |
| [Cooperative Fee Coordination](docs/design/cooperative-fee-coordination.md) | Fee coordination design |
| [VPN Transport](docs/design/VPN_HIVE_TRANSPORT.md) | VPN transport design |
| [Liquidity Integration](docs/design/LIQUIDITY_INTEGRATION.md) | cl-revenue-ops integration |
| [Architecture](docs/ARCHITECTURE.md) | Complete protocol specification |
| [Docker Deployment](docker/README.md) | Docker deployment guide |
| [Threat Model](docs/security/THREAT_MODEL.md) | Security threat analysis |

## Testing

```bash
# Run all tests
python3 -m pytest tests/

# Run specific test file
python3 -m pytest tests/test_planner.py

# Run with verbose output
python3 -m pytest tests/ -v
```

## License

MIT
