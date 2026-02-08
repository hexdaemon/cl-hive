# Changelog

All notable changes to cl-hive will be documented in this file.

## [Unreleased]

## [2.2.8] - 2026-02-07

### Added
- vitality plugin v0.2.3 for automatic plugin health monitoring and restart
- Thread safety locks in 7 coordination modules (AdaptiveFeeController, StigmergicCoordinator, MyceliumDefenseSystem, TimeBasedFeeAdjuster, FeeCoordinationManager, VPNTransportManager)
- Cache bounds to prevent memory bloat (500-1000 entry limits on peer/route caches)
- Docker image version 2.2.8

### Fixed
- **Thread Safety**: Fixed race conditions in concurrent modification of shared state
- **Governance Bypass**: task_manager expansion now routes through governance engine (security)
- **Outbox Retry**: Parse/serialization errors now fail permanently instead of infinite retry
- **P0 Crashes**: Fixed AttributeError on _get_topology_snapshot() and None handling in task execution
- **P1 Logic Errors**: Fixed analyzer references, field names, method calls across 12 modules
- **P2 Edge Cases**: MCF solution validation, force_close counting, yield metric clamping

### Removed
- trustedcoin plugin (explorer-only Bitcoin backend no longer needed)

### Changed
- Updated .env.example documentation to reflect vitality instead of trustedcoin

## [1.9.0] - 2026-01-24

### Added
- **Distributed Settlement System**: Fully decentralized settlement with proposal/vote/execute consensus
  - Any member can propose settlement for completed weeks
  - Members auto-vote when data hash matches (quorum: 51%)
  - Each node pays their share via BOLT12 offers
  - Period-based idempotency prevents double settlement
- **FEE_REPORT Gossip**: Nodes broadcast fee earnings for canonical settlement calculations
- **Uptime Tracking**: Automatic uptime percentage calculation from peer presence data
- **Docker Update Documentation**: Step-by-step guide for updating Docker deployments

### Fixed
- **ISO Week Parsing**: Fixed week boundary calculations using `fromisocalendar()`
- **Settlement Fee Discovery**: Now checks database fee_reports before in-memory state
- **Pool Snapshot RPC**: Fixed handling of `List[MemberContribution]` return type
- **Uptime in Fair Share**: Fixed 0% uptime by syncing from peer_presence to hive_members
- **Backfill Column Names**: Fixed `hive-backfill-fees` to use correct schema columns
- **Period Parameter**: `hive-fee-reports period=latest` now recognized as special case

### Changed
- **Settlement Weights**: 30% capacity, 60% routing volume, 10% uptime (updated from 40/40/20)
- Uptime synced on startup and hourly via maintenance loop

## [1.7.1] - 2026-01-22

### Fixed
- **State Version Persistence**: Fixed gossip version resetting to 1 after plugin restart
- **Fee Tracking Persistence**: Local fee tracking now survives restarts for accurate settlement calculations
- **Contribution Rate Limits**: Per-peer and daily rate limits now persist across restarts
- **Bridge Policy Sync**: Bridge policies automatically sync with database state on startup
- **State Change Detection**: State version only increments when state actually changes

## [1.7.0] - 2026-01-22

### Added
- **BOLT12 Settlement System**: Auto-generate and sync settlement offers across hive nodes, execute payments for fee distribution
- **Fleet-wide Advisor**: `advisor_run_cycle_all` runs proactive cycles on all nodes in parallel
- **Comprehensive AI Intelligence**: Integrated routing intelligence, salience detection, and collective warnings into advisor
- **Real-time Fee Gossip**: Settlement calculations now use live fee data from gossip
- **Plugin Database Backup**: Automated backup daemon for cl-hive and cl-revenue-ops databases
- **RTL Web Interface**: Ride The Lightning optional service in Docker deployment
- **hive-remove-member Command**: Safely remove members from the hive
- **CLBOSS Optional**: Disable CLBOSS via `CLBOSS_ENABLED=false` environment variable
- **Docker Development Mounts**: Edit plugins without rebuilding container

### Fixed
- **Duplicate Channel Opens**: Planner now checks for pending channels before proposing opens to same peer
- **Budget Validation**: Validate on-chain budget before proposing channel expansions
- **Auto-join Disabled**: Default to disabled to avoid CLN crash bug on concurrent channel opens
- **min_vouch Undefined**: Fixed hive-force-promote command when min_vouch config missing
- **SQLite Backup**: Use Python sqlite3 module instead of CLI dependency
- **Production Hardening**: Docker deployment stability improvements
- **Membership System**: Removed legacy ADMIN tier references from 2-tier system
- **Settlement Database**: Multiple fixes for routing pool and settlement data handling
- **Docker Port Conflicts**: Resolved startup issues and security restrictions

### Changed
- **Expansions Enabled**: `hive-planner-enable-expansions` now defaults to `true`
- **Settlement Weights**: Updated fair share calculation weights (40% capacity, 40% volume, 20% uptime)
- **Tor Keys Persistent**: Hidden service keys now persist across container restarts

## [1.6.0] - 2026-01-15

### Changed
- Repository transferred to lightning-goats organization
- Updated all GitHub URLs from santyr/cl-hive to lightning-goats/cl-hive

## [1.1.0] - 2026-01-10

### Added
- Initial MCP server integration
- Proactive advisor system
- Strategic positioning analysis

## [1.0.0] - 2026-01-05

### Added
- Production-ready Docker deployment
- Core Lightning v25.12.1 support
- Full hive coordination protocol
- cl-revenue-ops integration
