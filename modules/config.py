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
    'auto_vouch_enabled': bool,
    'auto_promote_enabled': bool,
    'ban_autotrigger_enabled': bool,
    'neophyte_fee_discount_pct': float,
    'member_fee_ppm': int,
    'probation_days': int,
    'vouch_threshold_pct': float,
    'min_vouch_count': int,
    'max_members': int,
    'market_share_cap_pct': float,
    'intent_hold_seconds': int,
    'intent_expire_seconds': int,
    'gossip_threshold_pct': float,
    'heartbeat_interval': int,
    'planner_interval': int,
    'planner_enable_expansions': bool,
    # Governance (Phase 7)
    'autonomous_budget_per_day': int,
    'autonomous_actions_per_hour': int,
    'oracle_url': str,
    'oracle_timeout_seconds': int,
}

# Range constraints for numeric fields
CONFIG_FIELD_RANGES: Dict[str, tuple] = {
    'neophyte_fee_discount_pct': (0.0, 1.0),
    'member_fee_ppm': (0, 100000),
    'probation_days': (1, 365),
    'vouch_threshold_pct': (0.0, 1.0),
    'min_vouch_count': (1, 50),
    'max_members': (2, 100),
    'market_share_cap_pct': (0.0, 1.0),
    'intent_hold_seconds': (10, 600),
    'intent_expire_seconds': (60, 3600),
    'gossip_threshold_pct': (0.01, 0.5),
    'heartbeat_interval': (60, 3600),
    'planner_interval': (300, 86400),  # Min 5 minutes, max 24 hours
    # Governance (Phase 7)
    'autonomous_budget_per_day': (100_000, 100_000_000),  # 100k to 100M sats
    'autonomous_actions_per_hour': (1, 10),  # 1 to 10 actions per hour
    'oracle_timeout_seconds': (1, 30),  # 1 to 30 seconds
}

# Valid governance modes
VALID_GOVERNANCE_MODES = {'advisor', 'autonomous', 'oracle'}


@dataclass
class HiveConfig:
    """
    Configuration container for the Hive plugin.
    
    All values can be set via plugin options at startup.
    """
    
    # Database path
    db_path: str = '~/.lightning/cl_hive.db'
    
    # Governance Mode
    governance_mode: str = 'advisor'  # 'advisor', 'autonomous', 'oracle'

    # Phase 5 safety knobs
    membership_enabled: bool = True
    auto_vouch_enabled: bool = True
    auto_promote_enabled: bool = True
    ban_autotrigger_enabled: bool = False
    
    # Membership Economics
    neophyte_fee_discount_pct: float = 0.5    # 50% of public rate for neophytes
    member_fee_ppm: int = 0                    # 0-fee for full members
    probation_days: int = 30                   # Minimum days before promotion
    
    # Promotion Consensus
    vouch_threshold_pct: float = 0.51          # 51% of members must vouch
    min_vouch_count: int = 3                   # Minimum 3 vouches required
    
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

    # Governance (Phase 7)
    autonomous_budget_per_day: int = 10_000_000  # 10M sats daily budget
    autonomous_actions_per_hour: int = 2         # Max 2 actions per hour
    oracle_url: Optional[str] = None             # External oracle API URL
    oracle_timeout_seconds: int = 5              # Oracle API timeout

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
    auto_vouch_enabled: bool
    auto_promote_enabled: bool
    ban_autotrigger_enabled: bool
    neophyte_fee_discount_pct: float
    member_fee_ppm: int
    probation_days: int
    vouch_threshold_pct: float
    min_vouch_count: int
    max_members: int
    market_share_cap_pct: float
    intent_hold_seconds: int
    intent_expire_seconds: int
    gossip_threshold_pct: float
    heartbeat_interval: int
    planner_interval: int
    planner_enable_expansions: bool
    # Governance (Phase 7)
    autonomous_budget_per_day: int
    autonomous_actions_per_hour: int
    oracle_url: Optional[str]
    oracle_timeout_seconds: int
    version: int
    
    @classmethod
    def from_config(cls, config: HiveConfig) -> 'HiveConfigSnapshot':
        """Create a frozen snapshot from mutable config."""
        return cls(
            db_path=config.db_path,
            governance_mode=config.governance_mode,
            membership_enabled=config.membership_enabled,
            auto_vouch_enabled=config.auto_vouch_enabled,
            auto_promote_enabled=config.auto_promote_enabled,
            ban_autotrigger_enabled=config.ban_autotrigger_enabled,
            neophyte_fee_discount_pct=config.neophyte_fee_discount_pct,
            member_fee_ppm=config.member_fee_ppm,
            probation_days=config.probation_days,
            vouch_threshold_pct=config.vouch_threshold_pct,
            min_vouch_count=config.min_vouch_count,
            max_members=config.max_members,
            market_share_cap_pct=config.market_share_cap_pct,
            intent_hold_seconds=config.intent_hold_seconds,
            intent_expire_seconds=config.intent_expire_seconds,
            gossip_threshold_pct=config.gossip_threshold_pct,
            heartbeat_interval=config.heartbeat_interval,
            planner_interval=config.planner_interval,
            planner_enable_expansions=config.planner_enable_expansions,
            autonomous_budget_per_day=config.autonomous_budget_per_day,
            autonomous_actions_per_hour=config.autonomous_actions_per_hour,
            oracle_url=config.oracle_url,
            oracle_timeout_seconds=config.oracle_timeout_seconds,
            version=config._version,
        )
