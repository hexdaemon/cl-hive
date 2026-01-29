"""
Modules package for cl-hive

This package contains the core modules for the Hive swarm intelligence layer:
- config: Configuration dataclass and snapshot pattern
- database: SQLite persistence with thread-local connections
- protocol: BOLT 8 custom message types and serialization (Phase 1)
- handshake: PKI-based handshake protocol (Phase 1)
- state_manager: HiveMap distributed state (Phase 2)
- gossip: Threshold gossiping and anti-entropy sync (Phase 2)
- intent_manager: Intent Lock conflict resolution (Phase 3)
- bridge: cl-revenue-ops integration (Phase 4)
- clboss_bridge: CLBoss conflict prevention (Phase 4)
- membership: Two-tier membership system (Phase 5)
- contribution: Contribution ratio tracking (Phase 5)
- planner: Topology optimization (Phase 6)
- quality_scorer: Peer quality scoring (Phase 6.2)
- cooperative_expansion: Coordinated channel opening (Phase 6.4)
- governance: Decision engine modes (Phase 7)
- vpn_transport: VPN-based transport policy for hive gossip
"""

__version__ = "2.2.3"
