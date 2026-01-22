# Changelog

All notable changes to cl-hive will be documented in this file.

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
