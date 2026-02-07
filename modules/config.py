"""
Configuration module for cl-hive

Contains the HiveConfig dataclass that holds all tunable parameters
for the Hive swarm intelligence layer.

Implements the ConfigSnapshot pattern from cl-revenue-ops for
thread-safe configuration access during background operations.
"""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, FrozenSet, TYPE_CHECKING

if TYPE_CHECKING:
    from .database import HiveDatabase


# Immutable keys that cannot be changed at runtime
IMMUTABLE_CONFIG_KEYS: FrozenSet[str] = frozenset({
    'db_path',
})

# Type mapping for config fields (for validation)
CONFIG_FIELD_TYPES: Dict[str, type] = {
    'governance_mode': str,
    'membership_enabled': bool,
    'auto_join_enabled': bool,
    'auto_vouch_enabled': bool,
    'auto_promote_enabled': bool,
    'ban_autotrigger_enabled': bool,
    'neophyte_fee_discount_pct': float,
    'member_fee_ppm': int,
    'probation_days': int,
    'min_contribution_ratio': float,
    'min_uptime_pct': float,
    'min_unique_peers': int,
    'max_members': int,
    'market_share_cap_pct': float,
    'intent_hold_seconds': int,
    'intent_expire_seconds': int,
    'gossip_threshold_pct': float,
    'heartbeat_interval': int,
    'planner_interval': int,
    'planner_enable_expansions': bool,
    'planner_min_channel_sats': int,
    'planner_max_channel_sats': int,
    'planner_default_channel_sats': int,
    # Governance (Phase 7) - Failsafe mode limits
    'failsafe_budget_per_day': int,
    'failsafe_actions_per_hour': int,
    'budget_reserve_pct': float,
    'budget_max_per_channel_pct': float,
    # Feerate gate
    'max_expansion_feerate_perkb': int,
}

# Range constraints for numeric fields
CONFIG_FIELD_RANGES: Dict[str, tuple] = {
    'neophyte_fee_discount_pct': (0.0, 1.0),
    'member_fee_ppm': (0, 100000),
    'probation_days': (1, 365),
    'min_contribution_ratio': (0.0, 10.0),
    'min_uptime_pct': (50.0, 100.0),
    'min_unique_peers': (0, 10),
    'max_members': (2, 100),
    'market_share_cap_pct': (0.0, 1.0),
    'intent_hold_seconds': (10, 600),
    'intent_expire_seconds': (60, 3600),
    'gossip_threshold_pct': (0.01, 0.5),
    'heartbeat_interval': (60, 3600),
    'planner_interval': (300, 86400),  # Min 5 minutes, max 24 hours
    'planner_min_channel_sats': (100_000, 100_000_000),  # 100k to 100M sats
    'planner_max_channel_sats': (1_000_000, 1_000_000_000),  # 1M to 1B sats (10 BTC)
    'planner_default_channel_sats': (100_000, 500_000_000),  # 100k to 500M sats (5 BTC)
    # Governance (Phase 7) - Failsafe mode limits (tighter than old autonomous)
    'failsafe_budget_per_day': (100_000, 10_000_000),  # 100k to 10M sats (reduced max)
    'failsafe_actions_per_hour': (1, 5),  # 1 to 5 actions per hour (reduced max)
    'budget_reserve_pct': (0.05, 0.50),  # 5% to 50% reserve
    'budget_max_per_channel_pct': (0.10, 1.0),  # 10% to 100% of daily budget per channel
    # Feerate gate for expansions
    'max_expansion_feerate_perkb': (1000, 100000),  # 1-100 sat/vB (perkb = 4x perkw)
}

# Valid governance modes
# - advisor: Primary mode - AI (via MCP server) reviews pending_actions
# - failsafe: Emergency mode - auto-execute critical safety actions when AI unavailable
VALID_GOVERNANCE_MODES = {'advisor', 'failsafe'}


@dataclass
class HiveConfig:
    """
    Configuration container for the Hive plugin.
    
    All values can be set via plugin options at startup.
    """
    
    # Database path
    db_path: str = '~/.lightning/cl_hive.db'
    
    # Governance Mode (advisor is primary, failsafe is emergency backup)
    governance_mode: str = 'advisor'  # 'advisor' (AI-driven), 'failsafe' (emergency)

    # Phase 5 safety knobs
    membership_enabled: bool = True
    auto_join_enabled: bool = False       # Auto-send HELLO on peer_connected (disabled: CLN crash bug)
    auto_vouch_enabled: bool = True
    auto_promote_enabled: bool = True
    ban_autotrigger_enabled: bool = False
    
    # Membership Economics
    neophyte_fee_discount_pct: float = 0.5    # 50% of public rate for neophytes
    member_fee_ppm: int = 0                    # 0-fee for full members
    probation_days: int = 90                   # 90 days probation before auto-promotion

    # Auto-Promotion Criteria (no vouching required - meritocratic)
    min_contribution_ratio: float = 1.0        # Must forward at least as much as received
    min_uptime_pct: float = 95.0               # 95% uptime required
    min_unique_peers: int = 1                  # Must bring at least 1 unique peer
    
    # Ecological Limits
    max_members: int = 50                      # Dunbar cap for gossip efficiency
    market_share_cap_pct: float = 0.20         # 20% max per target (anti-monopoly)
    
    # Intent Lock Protocol
    intent_hold_seconds: int = 60              # Wait before committing Intent
    intent_expire_seconds: int = 300           # Lock TTL (5 minutes)
    
    # Gossip Protocol
    gossip_threshold_pct: float = 0.10         # 10% capacity change triggers gossip
    heartbeat_interval: int = 300              # 5 minutes between heartbeats

    # Planner (Phase 6)
    planner_interval: int = 3600               # 1 hour between planner cycles
    planner_enable_expansions: bool = False    # Disabled by default (safety)
    planner_min_channel_sats: int = 1_000_000  # 1M sats minimum channel size
    planner_max_channel_sats: int = 50_000_000  # 50M sats maximum channel size
    planner_default_channel_sats: int = 5_000_000  # 5M sats default channel size

    # Governance (Phase 7) - Failsafe mode limits
    failsafe_budget_per_day: int = 10_000_000    # 10M sats daily budget (5M per channel at 50%)
    failsafe_actions_per_hour: int = 2           # Max 2 emergency actions per hour
    budget_reserve_pct: float = 0.20             # Reserve 20% of onchain for future expansion
    budget_max_per_channel_pct: float = 0.50     # Max 50% of daily budget per single channel

    # Feerate gate for expansions (sat/kB, where 1 sat/vB = 4 sat/kB approx)
    # Default 5000 sat/kB = ~1.25 sat/vB - conservative low-fee threshold
    max_expansion_feerate_perkb: int = 5000

    # Internal version tracking
    _version: int = field(default=0, repr=False, compare=False)
    
    def snapshot(self) -> 'HiveConfigSnapshot':
        """
        Create an immutable snapshot for cycle execution.
        
        All worker cycles MUST capture a snapshot at cycle start and use
        only that snapshot for the duration of the cycle. This prevents
        torn reads when config is updated mid-cycle.
        """
        return HiveConfigSnapshot.from_config(self)
    
    def validate(self) -> Optional[str]:
        """
        Validate configuration values.
        
        Returns:
            Error message if invalid, None if valid
        """
        if self.governance_mode not in VALID_GOVERNANCE_MODES:
            return f"Invalid governance_mode: {self.governance_mode}. Valid: {VALID_GOVERNANCE_MODES}"
        
        for key, (min_val, max_val) in CONFIG_FIELD_RANGES.items():
            value = getattr(self, key, None)
            if value is not None and not (min_val <= value <= max_val):
                return f"Config {key}={value} out of range [{min_val}, {max_val}]"

        # Cross-field constraints
        if self.planner_min_channel_sats > self.planner_max_channel_sats:
            return (f"planner_min_channel_sats ({self.planner_min_channel_sats}) > "
                    f"planner_max_channel_sats ({self.planner_max_channel_sats})")
        if (self.planner_default_channel_sats < self.planner_min_channel_sats or
                self.planner_default_channel_sats > self.planner_max_channel_sats):
            return (f"planner_default_channel_sats ({self.planner_default_channel_sats}) "
                    f"outside [{self.planner_min_channel_sats}, {self.planner_max_channel_sats}]")

        return None


@dataclass(frozen=True)
class HiveConfigSnapshot:
    """
    Immutable configuration snapshot for thread-safe cycle execution.
    
    This frozen dataclass prevents accidental mutation and ensures
    consistency when a background loop captures config at cycle start.
    """
    
    # Core settings (immutable snapshot)
    db_path: str
    governance_mode: str
    membership_enabled: bool
    auto_join_enabled: bool
    auto_vouch_enabled: bool
    auto_promote_enabled: bool
    ban_autotrigger_enabled: bool
    neophyte_fee_discount_pct: float
    member_fee_ppm: int
    probation_days: int
    min_contribution_ratio: float
    min_uptime_pct: float
    min_unique_peers: int
    max_members: int
    market_share_cap_pct: float
    intent_hold_seconds: int
    intent_expire_seconds: int
    gossip_threshold_pct: float
    heartbeat_interval: int
    planner_interval: int
    planner_enable_expansions: bool
    planner_min_channel_sats: int
    planner_max_channel_sats: int
    planner_default_channel_sats: int
    # Governance (Phase 7) - Failsafe mode limits
    failsafe_budget_per_day: int
    failsafe_actions_per_hour: int
    budget_reserve_pct: float
    budget_max_per_channel_pct: float
    max_expansion_feerate_perkb: int
    version: int

    @classmethod
    def from_config(cls, config: HiveConfig) -> 'HiveConfigSnapshot':
        """Create a frozen snapshot from mutable config."""
        return cls(
            db_path=config.db_path,
            governance_mode=config.governance_mode,
            membership_enabled=config.membership_enabled,
            auto_join_enabled=config.auto_join_enabled,
            auto_vouch_enabled=config.auto_vouch_enabled,
            auto_promote_enabled=config.auto_promote_enabled,
            ban_autotrigger_enabled=config.ban_autotrigger_enabled,
            neophyte_fee_discount_pct=config.neophyte_fee_discount_pct,
            member_fee_ppm=config.member_fee_ppm,
            probation_days=config.probation_days,
            min_contribution_ratio=config.min_contribution_ratio,
            min_uptime_pct=config.min_uptime_pct,
            min_unique_peers=config.min_unique_peers,
            max_members=config.max_members,
            market_share_cap_pct=config.market_share_cap_pct,
            intent_hold_seconds=config.intent_hold_seconds,
            intent_expire_seconds=config.intent_expire_seconds,
            gossip_threshold_pct=config.gossip_threshold_pct,
            heartbeat_interval=config.heartbeat_interval,
            planner_interval=config.planner_interval,
            planner_enable_expansions=config.planner_enable_expansions,
            planner_min_channel_sats=config.planner_min_channel_sats,
            planner_max_channel_sats=config.planner_max_channel_sats,
            planner_default_channel_sats=config.planner_default_channel_sats,
            failsafe_budget_per_day=config.failsafe_budget_per_day,
            failsafe_actions_per_hour=config.failsafe_actions_per_hour,
            budget_reserve_pct=config.budget_reserve_pct,
            budget_max_per_channel_pct=config.budget_max_per_channel_pct,
            max_expansion_feerate_perkb=config.max_expansion_feerate_perkb,
            version=config._version,
        )
