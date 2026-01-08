"""
Planner Module for cl-hive (Phase 6: Topology Optimization)

Implements the "Gardner" algorithm for automated topology management:
- Saturation Analysis: Calculate Hive market share per target
- Guard Mechanism: Issue clboss-ignore for saturated targets
- Expansion Proposals: (Future tickets - not implemented here)

Security Constraints (Red Team - PHASE6_THREAT_MODEL):
- Gossip capacity is CLAMPED to public listchannels data
- Max 5 new ignores per cycle (abort if exceeded)
- All decisions logged to hive_planner_log table

This ticket (6-01) implements ONLY saturation detection and guard mechanism.
Expansion logic will be added in later tickets.

Author: Lightning Goats Team
"""

import time
import secrets
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from pyln.client import RpcError
except ImportError:
    # For testing without pyln installed
    class RpcError(Exception):
        """Stub RpcError for testing."""
        pass


# =============================================================================
# CONSTANTS
# =============================================================================

# Cache refresh interval (seconds) - avoid hammering listchannels
NETWORK_CACHE_TTL_SECONDS = 300

# Maximum ignores per cycle (Red Team mitigation)
MAX_IGNORES_PER_CYCLE = 5

# Saturation release threshold (hysteresis to avoid flip-flopping)
SATURATION_RELEASE_THRESHOLD_PCT = 0.15  # Release ignore at 15%

# Minimum public capacity to consider a target (anti-Sybil)
MIN_TARGET_CAPACITY_SATS = 100_000_000  # 1 BTC


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ChannelInfo:
    """Represents a channel from listchannels."""
    source: str
    destination: str
    short_channel_id: str
    capacity_sats: int
    active: bool


@dataclass
class SaturationResult:
    """Result of saturation calculation for a target."""
    target: str
    hive_capacity_sats: int
    public_capacity_sats: int
    hive_share_pct: float
    is_saturated: bool
    should_release: bool


# =============================================================================
# PLANNER CLASS
# =============================================================================

class Planner:
    """
    Topology optimization engine for the Hive swarm.

    Analyzes network topology to:
    1. Detect targets where Hive has excessive market share (saturation)
    2. Issue clboss-ignore to prevent further capital accumulation
    3. Release ignores when saturation drops below threshold

    Thread Safety:
    - Uses config snapshot pattern (cfg passed to run_cycle)
    - Network cache is refreshed per-cycle
    - No sleeping inside run_cycle
    """

    def __init__(self, state_manager, database, bridge, clboss_bridge, plugin=None):
        """
        Initialize the Planner.

        Args:
            state_manager: StateManager for accessing Hive peer states
            database: HiveDatabase for logging and membership data
            bridge: Integration Bridge for cl-revenue-ops
            clboss_bridge: CLBossBridge for ignore/unignore operations
            plugin: Plugin reference for RPC and logging
        """
        self.state_manager = state_manager
        self.db = database
        self.bridge = bridge
        self.clboss = clboss_bridge
        self.plugin = plugin

        # Network cache (refreshed each cycle)
        self._network_cache: Dict[str, List[ChannelInfo]] = {}
        self._network_cache_time: int = 0

        # Track currently ignored peers (to avoid duplicate ignores)
        self._ignored_peers: Set[str] = set()

    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[Planner] {msg}", level=level)

    # =========================================================================
    # NETWORK CACHE
    # =========================================================================

    def _refresh_network_cache(self, force: bool = False) -> bool:
        """
        Refresh the network channel cache from listchannels.

        Implements efficient caching to minimize RPC load.
        Deduplicates bidirectional channels (A->B and B->A counted once).

        Args:
            force: Force refresh even if cache is fresh

        Returns:
            True if cache was refreshed successfully, False on error
        """
        now = int(time.time())

        # Use cached data if still fresh
        if not force and (now - self._network_cache_time) < NETWORK_CACHE_TTL_SECONDS:
            return True

        if not self.plugin:
            self._log("Cannot refresh network cache: no plugin reference", level='warn')
            return False

        try:
            # Fetch all public channels
            result = self.plugin.rpc.listchannels()
            channels_raw = result.get('channels', [])

            # Build capacity map: target -> list of channels TO that target
            # Deduplicate: for bidirectional channels, count capacity once
            capacity_map: Dict[str, List[ChannelInfo]] = {}
            seen_pairs: Set[str] = set()

            for ch in channels_raw:
                source = ch.get('source', '')
                dest = ch.get('destination', '')
                scid = ch.get('short_channel_id', '')

                if not source or not dest or not scid:
                    continue

                # Parse capacity (may be int or dict with msat)
                capacity_raw = ch.get('amount_msat') or ch.get('satoshis', 0)
                if isinstance(capacity_raw, dict):
                    capacity_sats = capacity_raw.get('msat', 0) // 1000
                elif isinstance(capacity_raw, str) and capacity_raw.endswith('msat'):
                    capacity_sats = int(capacity_raw[:-4]) // 1000
                elif isinstance(capacity_raw, int):
                    # Could be msat or sats depending on field
                    if capacity_raw > 10_000_000_000:  # Likely msat
                        capacity_sats = capacity_raw // 1000
                    else:
                        capacity_sats = capacity_raw
                else:
                    capacity_sats = 0

                active = ch.get('active', True)

                # Create normalized pair key for dedup (smaller pubkey first)
                pair_key = tuple(sorted([source, dest])) + (scid,)
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                info = ChannelInfo(
                    source=source,
                    destination=dest,
                    short_channel_id=scid,
                    capacity_sats=capacity_sats,
                    active=active
                )

                # Index by destination (target)
                if dest not in capacity_map:
                    capacity_map[dest] = []
                capacity_map[dest].append(info)

                # Also index by source (for bidirectional lookup)
                if source not in capacity_map:
                    capacity_map[source] = []
                capacity_map[source].append(info)

            self._network_cache = capacity_map
            self._network_cache_time = now

            self._log(f"Network cache refreshed: {len(seen_pairs)} channels, "
                     f"{len(capacity_map)} targets", level='debug')
            return True

        except RpcError as e:
            self._log(f"listchannels RPC failed: {e}", level='warn')
            return False
        except Exception as e:
            self._log(f"Network cache refresh error: {e}", level='warn')
            return False

    def _get_public_capacity_to_target(self, target: str) -> int:
        """
        Get total public network capacity to a target.

        Args:
            target: Target node pubkey

        Returns:
            Total capacity in satoshis (0 if not found)
        """
        channels = self._network_cache.get(target, [])
        return sum(ch.capacity_sats for ch in channels if ch.active)

    # =========================================================================
    # SATURATION LOGIC
    # =========================================================================

    def _get_hive_members(self) -> List[str]:
        """Get list of Hive member pubkeys."""
        if not self.db:
            return []
        members = self.db.get_all_members()
        return [m['peer_id'] for m in members if m.get('tier') in ('member', 'admin')]

    def _get_hive_capacity_to_target(self, target: str, hive_members: List[str]) -> int:
        """
        Calculate total Hive capacity to a target.

        SECURITY: Clamps gossip-reported capacity to public listchannels maximum.
        This prevents attackers from inflating saturation via fake gossip.

        Args:
            target: Target node pubkey
            hive_members: List of Hive member pubkeys

        Returns:
            Total Hive capacity in satoshis (clamped to public reality)
        """
        if not self.state_manager:
            return 0

        # Get all known Hive peer states (list -> dict for lookup)
        all_states_list = self.state_manager.get_all_peer_states()
        all_states = {s.peer_id: s for s in all_states_list}

        # Get public capacity for reality check
        public_channels = self._network_cache.get(target, [])

        # Build map: (source, dest) -> max public capacity
        public_capacity_map: Dict[Tuple[str, str], int] = {}
        for ch in public_channels:
            key = (ch.source, ch.destination)
            public_capacity_map[key] = max(
                public_capacity_map.get(key, 0),
                ch.capacity_sats
            )
            # Also check reverse direction
            key_rev = (ch.destination, ch.source)
            public_capacity_map[key_rev] = max(
                public_capacity_map.get(key_rev, 0),
                ch.capacity_sats
            )

        total_hive_capacity = 0

        for member_pubkey in hive_members:
            state = all_states.get(member_pubkey)
            if not state:
                continue

            # Check if this member's topology includes the target
            topology = getattr(state, 'topology', []) or []
            if target not in topology:
                continue

            # Get claimed capacity from gossip
            claimed_capacity = getattr(state, 'capacity_sats', 0)

            # SECURITY: Clamp to public reality
            # Look up the actual public capacity for this (member, target) pair
            public_max = public_capacity_map.get((member_pubkey, target), 0)
            if public_max == 0:
                # Also try reverse
                public_max = public_capacity_map.get((target, member_pubkey), 0)

            if public_max > 0:
                clamped_capacity = min(claimed_capacity, public_max)
            else:
                # No public channel found - don't trust gossip at all
                clamped_capacity = 0

            total_hive_capacity += clamped_capacity

        return total_hive_capacity

    def _calculate_hive_share(self, target: str, cfg) -> SaturationResult:
        """
        Calculate Hive's market share for a target.

        Args:
            target: Target node pubkey
            cfg: Config snapshot for thresholds

        Returns:
            SaturationResult with share calculation
        """
        hive_members = self._get_hive_members()

        # Get public capacity (denominator)
        public_capacity = self._get_public_capacity_to_target(target)

        # Get Hive capacity (numerator, clamped)
        hive_capacity = self._get_hive_capacity_to_target(target, hive_members)

        # Calculate share
        if public_capacity <= 0:
            hive_share = 0.0
        else:
            hive_share = hive_capacity / public_capacity

        # Check saturation threshold
        is_saturated = hive_share >= cfg.market_share_cap_pct

        # Check release threshold (hysteresis)
        should_release = hive_share < SATURATION_RELEASE_THRESHOLD_PCT

        return SaturationResult(
            target=target,
            hive_capacity_sats=hive_capacity,
            public_capacity_sats=public_capacity,
            hive_share_pct=hive_share,
            is_saturated=is_saturated,
            should_release=should_release
        )

    def get_saturated_targets(self, cfg) -> List[SaturationResult]:
        """
        Get all targets where Hive exceeds market share cap.

        Args:
            cfg: Config snapshot

        Returns:
            List of SaturationResult for saturated targets
        """
        saturated = []

        # Check all known targets in network cache
        for target in self._network_cache.keys():
            # Skip targets below minimum capacity (anti-Sybil)
            public_capacity = self._get_public_capacity_to_target(target)
            if public_capacity < MIN_TARGET_CAPACITY_SATS:
                continue

            result = self._calculate_hive_share(target, cfg)
            if result.is_saturated:
                saturated.append(result)

        return saturated

    # =========================================================================
    # GUARD MECHANISM
    # =========================================================================

    def _enforce_saturation(self, cfg, run_id: str) -> List[Dict[str, Any]]:
        """
        Enforce saturation limits by issuing clboss-ignore.

        SECURITY CONSTRAINTS:
        - Max 5 new ignores per cycle (abort if exceeded)
        - Idempotent: skip already-ignored peers
        - Log all decisions to hive_planner_log

        Args:
            cfg: Config snapshot
            run_id: Unique identifier for this cycle

        Returns:
            List of decision records for testing
        """
        decisions = []

        # Refresh network cache
        if not self._refresh_network_cache():
            self._log("Failed to refresh network cache, aborting saturation enforcement", level='warn')
            self.db.log_planner_action(
                action_type='saturation_check',
                result='failed',
                details={'reason': 'network_cache_refresh_failed', 'run_id': run_id}
            )
            return decisions

        # Get saturated targets
        saturated_targets = self.get_saturated_targets(cfg)

        # Count new ignores needed
        new_ignores_needed = []
        for result in saturated_targets:
            if result.target not in self._ignored_peers:
                new_ignores_needed.append(result)

        # SECURITY: Check rate limit
        if len(new_ignores_needed) > MAX_IGNORES_PER_CYCLE:
            self._log(
                f"Mass Saturation Detected: {len(new_ignores_needed)} targets exceed cap. "
                f"Aborting cycle (max {MAX_IGNORES_PER_CYCLE}/cycle).",
                level='warn'
            )
            self.db.log_planner_action(
                action_type='saturation_check',
                result='aborted',
                details={
                    'reason': 'mass_saturation_detected',
                    'targets_count': len(new_ignores_needed),
                    'max_allowed': MAX_IGNORES_PER_CYCLE,
                    'run_id': run_id
                }
            )
            decisions.append({
                'action': 'abort',
                'reason': 'mass_saturation_detected',
                'targets_count': len(new_ignores_needed)
            })
            return decisions

        # Issue ignores for new saturated targets
        ignores_issued = 0
        for result in new_ignores_needed:
            if ignores_issued >= MAX_IGNORES_PER_CYCLE:
                break

            # Check if CLBoss is available
            if not self.clboss or not self.clboss._available:
                self._log(f"CLBoss unavailable, cannot ignore {result.target[:16]}...", level='debug')
                self.db.log_planner_action(
                    action_type='ignore',
                    result='skipped',
                    target=result.target,
                    details={
                        'reason': 'clboss_unavailable',
                        'hive_share_pct': round(result.hive_share_pct, 4),
                        'run_id': run_id
                    }
                )
                decisions.append({
                    'action': 'ignore_skipped',
                    'target': result.target,
                    'reason': 'clboss_unavailable'
                })
                continue

            # Issue ignore
            success = self.clboss.ignore_peer(result.target)
            if success:
                self._ignored_peers.add(result.target)
                ignores_issued += 1

                self._log(
                    f"Ignored saturated target {result.target[:16]}... "
                    f"(share={result.hive_share_pct:.1%})"
                )
                self.db.log_planner_action(
                    action_type='ignore',
                    result='success',
                    target=result.target,
                    details={
                        'hive_share_pct': round(result.hive_share_pct, 4),
                        'hive_capacity_sats': result.hive_capacity_sats,
                        'public_capacity_sats': result.public_capacity_sats,
                        'run_id': run_id
                    }
                )
                decisions.append({
                    'action': 'ignore',
                    'target': result.target,
                    'result': 'success',
                    'hive_share_pct': result.hive_share_pct
                })
            else:
                self._log(f"Failed to ignore {result.target[:16]}...", level='warn')
                self.db.log_planner_action(
                    action_type='ignore',
                    result='failed',
                    target=result.target,
                    details={
                        'hive_share_pct': round(result.hive_share_pct, 4),
                        'run_id': run_id
                    }
                )
                decisions.append({
                    'action': 'ignore',
                    'target': result.target,
                    'result': 'failed'
                })

        # Log summary
        self.db.log_planner_action(
            action_type='saturation_check',
            result='completed',
            details={
                'saturated_targets': len(saturated_targets),
                'new_ignores_issued': ignores_issued,
                'run_id': run_id
            }
        )

        return decisions

    def _release_saturation(self, cfg, run_id: str) -> List[Dict[str, Any]]:
        """
        Release ignores for targets that are no longer saturated.

        Uses hysteresis (15% threshold) to prevent flip-flopping.

        Args:
            cfg: Config snapshot
            run_id: Unique identifier for this cycle

        Returns:
            List of decision records
        """
        decisions = []

        # Check currently ignored peers
        peers_to_release = []
        for peer in list(self._ignored_peers):
            result = self._calculate_hive_share(peer, cfg)
            if result.should_release:
                peers_to_release.append((peer, result))

        # Issue unignores
        for peer, result in peers_to_release:
            if not self.clboss or not self.clboss._available:
                continue

            success = self.clboss.unignore_peer(peer)
            if success:
                self._ignored_peers.discard(peer)

                self._log(
                    f"Released ignore on {peer[:16]}... "
                    f"(share={result.hive_share_pct:.1%} < {SATURATION_RELEASE_THRESHOLD_PCT:.0%})"
                )
                self.db.log_planner_action(
                    action_type='unignore',
                    result='success',
                    target=peer,
                    details={
                        'hive_share_pct': round(result.hive_share_pct, 4),
                        'run_id': run_id
                    }
                )
                decisions.append({
                    'action': 'unignore',
                    'target': peer,
                    'result': 'success'
                })

        return decisions

    # =========================================================================
    # RUN CYCLE
    # =========================================================================

    def run_cycle(self, cfg, *, shutdown_event=None, now=None, run_id=None) -> List[Dict]:
        """
        Execute one planning cycle.

        This is the main entry point called by the planner_loop thread.
        No sleeping inside this method - caller handles timing.

        Args:
            cfg: Config snapshot (use config.snapshot() at cycle start)
            shutdown_event: Threading event to check for shutdown
            now: Current timestamp (for testing)
            run_id: Unique identifier for this cycle

        Returns:
            List of decision records for testing
        """
        if shutdown_event and shutdown_event.is_set():
            return []

        if now is None:
            now = int(time.time())
        if run_id is None:
            run_id = secrets.token_hex(8)

        self._log(f"Starting planner cycle (run_id={run_id})")
        decisions = []

        try:
            # Refresh network cache first
            if not self._refresh_network_cache(force=True):
                self._log("Network cache refresh failed, skipping cycle", level='warn')
                self.db.log_planner_action(
                    action_type='cycle',
                    result='failed',
                    details={'reason': 'cache_refresh_failed', 'run_id': run_id}
                )
                return []

            # Enforce saturation limits (Guard mechanism)
            saturation_decisions = self._enforce_saturation(cfg, run_id)
            decisions.extend(saturation_decisions)

            # Release over-ignored peers (best effort)
            release_decisions = self._release_saturation(cfg, run_id)
            decisions.extend(release_decisions)

            # NOTE: Expansion logic will be added in Ticket 6-03
            # For now, this ticket only implements saturation detection and guard

            self._log(f"Planner cycle complete (run_id={run_id}): {len(decisions)} decisions")
            self.db.log_planner_action(
                action_type='cycle',
                result='completed',
                details={
                    'decisions_count': len(decisions),
                    'run_id': run_id
                }
            )

        except Exception as e:
            self._log(f"Planner cycle error: {e}", level='warn')
            self.db.log_planner_action(
                action_type='cycle',
                result='error',
                details={'error': str(e), 'run_id': run_id}
            )

        return decisions

    # =========================================================================
    # STATISTICS
    # =========================================================================

    def get_planner_stats(self) -> Dict[str, Any]:
        """Get current planner statistics."""
        return {
            'network_cache_size': len(self._network_cache),
            'network_cache_age_seconds': int(time.time()) - self._network_cache_time,
            'ignored_peers_count': len(self._ignored_peers),
            'ignored_peers': list(self._ignored_peers)[:10],  # Limit for display
            'max_ignores_per_cycle': MAX_IGNORES_PER_CYCLE,
            'saturation_release_threshold_pct': SATURATION_RELEASE_THRESHOLD_PCT,
            'min_target_capacity_sats': MIN_TARGET_CAPACITY_SATS,
        }
