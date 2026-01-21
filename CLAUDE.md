# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

cl-hive is a Core Lightning plugin implementing distributed "Swarm Intelligence" for Lightning node fleets. It coordinates multiple nodes through PKI authentication, shared state gossip, and distributed governance. Designed to work alongside [cl-revenue-ops](https://github.com/lightning-goats/cl_revenue_ops) which handles local fee/rebalancing decisions.

## Commands

```bash
# Run all tests
python3 -m pytest tests/

# Run specific test file
python3 -m pytest tests/test_planner.py

# Run with verbose output
python3 -m pytest tests/ -v

# Run tests matching a pattern
python3 -m pytest tests/ -k "test_feerate"
```

No build system - this is a CLN plugin deployed by copying `cl-hive.py` and `modules/` to the plugin directory.

## Architecture

```
cl-hive (Coordination Layer - "The Diplomat")
    ↓
cl-revenue-ops (Execution Layer - "The CFO")
    ↓
Core Lightning
```

### Three-Layer Design
- **cl-hive**: Manages fleet topology, membership, and consensus decisions
- **cl-revenue-ops**: Executes fee policies and rebalancing (called via RPC)
- **Core Lightning**: Underlying node operations and HSM-based crypto

### Module Organization

| Module | Purpose |
|--------|---------|
| `protocol.py` | BOLT 8 custom messages (magic: "HIVE" = 0x48495645, types 32769-32843) |
| `handshake.py` | PKI auth using CLN signmessage/checkmessage |
| `state_manager.py` | HiveMap distributed state + anti-entropy sync |
| `gossip.py` | Threshold-based gossip (10% capacity change) with 5-min heartbeat |
| `intent_manager.py` | Intent Lock protocol - Announce-Wait-Commit with lexicographic tie-breaker |
| `bridge.py` | Circuit Breaker pattern for cl-revenue-ops integration |
| `clboss_bridge.py` | Optional CLBoss integration for saturation control |
| `membership.py` | Three-tier system: Admin → Member → Neophyte with vouch-based promotion |
| `contribution.py` | Forwarding stats and anti-leech detection |
| `planner.py` | Topology optimization - saturation analysis, expansion election, feerate gate |
| `config.py` | Hot-reloadable configuration with snapshot pattern |
| `database.py` | SQLite with WAL mode, thread-local connections |

### Key Patterns

**Thread Safety**:
- `RPC_LOCK` with 10-second timeout serializes all RPC calls
- `ThreadSafeRpcProxy` wraps the plugin.rpc object
- Thread-local SQLite connections with WAL mode

**Graceful Shutdown**:
- `shutdown_event` checked in all background loops
- Use `shutdown_event.wait(interval)` not `time.sleep()`

**Circuit Breaker** (in bridge.py):
- States: CLOSED → OPEN (after 3 failures) → HALF_OPEN (after 60s)
- All external plugin calls go through `safe_call()` wrapper

**Configuration Snapshot**:
- Use `config.snapshot()` at cycle start
- Never read mutable config mid-cycle

**Message Protocol**:
- 4-byte magic prefix filters non-Hive messages immediately
- "Peek & Check" pattern in custommsg hook
- JSON payload, max 65535 bytes per message

### Governance Modes

| Mode | Behavior |
|------|----------|
| `advisor` | **Primary mode** - Queue to pending_actions for AI/human approval via MCP server |
| `failsafe` | Emergency mode - Auto-execute only critical safety actions (bans) within strict limits |

### Database Tables

| Table | Purpose |
|-------|---------|
| `hive_members` | Member roster with tiers and stats |
| `intent_locks` | Active intent locks for conflict resolution |
| `hive_state` | Key-value store for persistent state |
| `contribution_ledger` | Forwarding contribution tracking |
| `hive_bans` | Ban proposals and votes |
| `promotion_requests` | Pending promotion requests |
| `hive_planner_log` | Planner decision audit log |
| `pending_actions` | Actions awaiting approval (advisor mode) |

## Safety Constraints

These are non-negotiable:

1. **Fail closed**: On invalid input, RPC errors, schema mismatches → do nothing, log
2. **Bound everything**: Message sizes, list sizes, DB growth, caches, loop runtime
3. **No silent fund actions**: Never move funds unless governance mode explicitly allows
4. **Identity binding**: Sender peer_id must match claimed pubkey in payload
5. **DoS protection**: Max 200 remote intents cached, rate limits on all loops
6. **Hive channels always zero fees**: Channels between hive fleet members MUST have 0 ppm fees (both base and proportional). Never apply static policies to hive channels.

## Planner Rules

The Planner proposes topology changes but cannot open channels directly:
- May log decisions to `hive_planner_log`
- May create `pending_actions` entries in advisor mode
- May broadcast INTENT messages when governance mode allows
- Max 5 ignores per cycle
- 20% market share cap per target

### Feerate Gate
- Expansions blocked when on-chain feerate > `hive-max-expansion-feerate` (default: 5000 sat/kB)
- Set to 0 to disable feerate checking
- Uses CLN `feerates` RPC to get current opening feerate

### Cooperative Expansion (Phase 6.4)
- Fleet-wide election for expansion targets
- Nomination → Election → Winner opens channel
- Prevents thundering herd via Intent Lock Protocol

## Optional Integrations

### CLBoss (Optional)
CLBoss is **not required** for cl-hive to function. The hive provides its own:
- **Channel opening**: Cooperative expansion with feerate gate
- **Fee management**: Delegated to cl-revenue-ops
- **Rebalancing**: Delegated to cl-revenue-ops + sling

If CLBoss IS installed, cl-hive will:
- Detect it automatically via plugin list
- Use `clboss-unmanage` to prevent redundant channel opens to saturated targets
- Coordinate via the "Gateway Pattern" to avoid conflicts

To run without CLBoss: Simply don't install it. No configuration needed.

### Sling (Optional for cl-hive)
Sling rebalancer is optional for cl-hive. cl-revenue-ops handles rebalancing coordination.
Note: Sling IS required for cl-revenue-ops itself.

## Development Notes

- Only external dependency: `pyln-client>=24.0`
- All crypto done via CLN HSM (signmessage/checkmessage) - no crypto libs imported
- Plugin options defined at top of `cl-hive.py` (30 configurable parameters)
- Background loops: intent_monitor_loop, membership_loop, planner_loop, gossip_loop

## Testing Conventions

- Test files in `tests/` directory
- Use pytest fixtures for mocking (see `conftest.py`)
- Mock RPC calls, never hit real network
- Test categories: unit, integration, feerate, planner, membership

## File Structure

```
cl-hive/
├── cl-hive.py              # Main plugin entry point
├── modules/
│   ├── protocol.py         # Message types and encoding
│   ├── handshake.py        # PKI authentication
│   ├── state_manager.py    # Distributed state
│   ├── gossip.py           # Gossip protocol
│   ├── intent_manager.py   # Intent locks
│   ├── bridge.py           # cl-revenue-ops bridge
│   ├── clboss_bridge.py    # Optional CLBoss bridge
│   ├── membership.py       # Member management
│   ├── contribution.py     # Contribution tracking
│   ├── planner.py          # Topology planner
│   ├── governance.py       # Decision engine (advisor/failsafe)
│   ├── config.py           # Configuration
│   └── database.py         # Database layer
├── tools/
│   ├── mcp-hive-server.py  # MCP server for Claude Code integration
│   ├── hive-monitor.py     # Real-time monitoring daemon
│   └── ai_advisor.py       # AI advisor utility
├── config/
│   ├── nodes.rest.example.json    # REST API config example
│   └── nodes.docker.example.json  # Docker/Polar config example
├── tests/                  # Test suite
├── docs/                   # Documentation
│   ├── design/             # Design documents
│   ├── planning/           # Implementation plans
│   ├── security/           # Security docs
│   ├── specs/              # Specifications
│   └── testing/            # Testing guides & scripts
├── audits/                 # Security audits
└── docker/                 # Docker deployment
```
