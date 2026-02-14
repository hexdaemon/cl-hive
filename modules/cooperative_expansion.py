"""
Cooperative Expansion Module for cl-hive (Phase 6.4)

Coordinates channel opening decisions across hive members to optimize
topology without redundant connections or conflicts.

When a peer becomes available (e.g., after a remote close), this module:
1. Evaluates if the peer is worth opening a channel to
2. Starts a nomination round where interested members self-nominate
3. Elects the best candidate based on liquidity, diversity, and load balancing
4. Broadcasts the election result so only one member opens

Election Criteria:
- Must not already have a channel to the target
- Must have sufficient liquidity (> min channel size)
- Preference for members with fewer total channels (load balancing)
- Preference for members who haven't recently opened channels (fairness)
- Quality score tiebreaker

Author: Lightning Goats Team
"""

import secrets
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Set, TYPE_CHECKING
from enum import Enum

if TYPE_CHECKING:
    from .database import HiveDatabase
    from .quality_scorer import PeerQualityScorer


class ExpansionRoundState(Enum):
    """State of a cooperative expansion round."""
    NOMINATING = "nominating"    # Collecting nominations
    ELECTING = "electing"        # Determining winner
    ELECTED = "elected"          # Winner announced
    COMPLETED = "completed"      # Channel opened
    CANCELLED = "cancelled"      # Round cancelled
    EXPIRED = "expired"          # Timed out


@dataclass
class Nomination:
    """A nomination from a hive member for a target peer."""
    nominator_id: str
    target_peer_id: str
    timestamp: int
    available_liquidity_sats: int
    quality_score: float
    has_existing_channel: bool
    channel_count: int
    reason: str = ""


@dataclass
class ExpansionRound:
    """Represents a cooperative expansion round for a target peer."""
    round_id: str
    target_peer_id: str
    started_at: int
    state: ExpansionRoundState
    trigger_event: str  # 'remote_close', 'peer_available', 'manual', etc.
    trigger_reporter: str  # Who triggered this round
    nominations: Dict[str, Nomination] = field(default_factory=dict)
    elected_id: Optional[str] = None
    recommended_size_sats: int = 0
    quality_score: float = 0.5
    expires_at: int = 0
    completed_at: int = 0
    result: str = ""
    # Phase 8: Fallback support
    ranked_candidates: List[tuple] = field(default_factory=list)  # [(pubkey, score), ...]
    decline_count: int = 0  # Number of declines received
    declined_by: List[str] = field(default_factory=list)  # Pubkeys who declined

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "round_id": self.round_id,
            "target_peer_id": self.target_peer_id,
            "started_at": self.started_at,
            "state": self.state.value,
            "trigger_event": self.trigger_event,
            "trigger_reporter": self.trigger_reporter[:16] + "..." if self.trigger_reporter else "",
            "nomination_count": len(self.nominations),
            "nominators": [n[:16] + "..." for n in self.nominations.keys()],
            "elected_id": self.elected_id[:16] + "..." if self.elected_id else None,
            "recommended_size_sats": self.recommended_size_sats,
            "quality_score": round(self.quality_score, 3),
            "expires_at": self.expires_at,
            "completed_at": self.completed_at,
            "result": self.result,
            # Phase 8: Fallback support
            "decline_count": self.decline_count,
            "declined_by": [d[:16] + "..." for d in self.declined_by],
            "fallback_candidates": len(self.ranked_candidates),
        }


class CooperativeExpansionManager:
    """
    Manages cooperative expansion rounds for the hive.

    This coordinator ensures that when an external peer becomes available
    (e.g., after closing a channel with one hive member), the hive can
    collectively decide who should open a channel to that peer.

    Flow:
    1. PEER_AVAILABLE event triggers evaluate_expansion()
    2. If peer is high quality, start_round() creates a new round
    3. Hive members self-nominate via handle_nomination()
    4. After NOMINATION_WINDOW, elect_winner() picks the best candidate
    5. Winner receives EXPANSION_ELECT and proceeds to open channel

    Thread Safety:
    - Uses a lock to protect round state
    - Background thread handles round expiration
    """

    # Timing constants
    NOMINATION_WINDOW_SECONDS = 30    # Time to collect nominations
    ELECTION_TIMEOUT_SECONDS = 10     # Time to announce election
    ROUND_EXPIRE_SECONDS = 120        # Max round lifetime
    COOLDOWN_SECONDS = 300            # Min time between rounds for same target

    # Election criteria weights
    WEIGHT_LIQUIDITY = 0.25           # Available liquidity
    WEIGHT_CHANNEL_COUNT = 0.30       # Fewer channels = higher score
    WEIGHT_RECENT_OPENS = 0.20        # Haven't opened recently = higher score
    WEIGHT_QUALITY_AGREEMENT = 0.25   # Quality score agreement

    # Limits
    MAX_ACTIVE_ROUNDS = 5             # Max concurrent expansion rounds
    MIN_NOMINATIONS_FOR_ELECTION = 1  # Min nominations to proceed
    MIN_QUALITY_SCORE = 0.45          # Min quality to trigger round
    MAX_FALLBACK_ATTEMPTS = 2         # Max declines before cancelling round (Phase 8)

    def __init__(
        self,
        database: 'HiveDatabase',
        quality_scorer: 'PeerQualityScorer' = None,
        plugin=None,
        our_id: str = None,
        config_getter=None,
        state_manager=None,
        rationalization_mgr=None
    ):
        """
        Initialize the CooperativeExpansionManager.

        Args:
            database: HiveDatabase for storing round state
            quality_scorer: PeerQualityScorer for evaluating peers
            plugin: Plugin instance for RPC and logging
            our_id: Our node's pubkey
            config_getter: Callable that returns current HiveConfig
            state_manager: StateManager for fleet budget queries (Phase 8)
            rationalization_mgr: RationalizationManager for redundancy detection
        """
        self.database = database
        self.quality_scorer = quality_scorer
        self.plugin = plugin
        self.our_id = our_id
        self._config_getter = config_getter
        self.state_manager = state_manager
        self.rationalization_mgr = rationalization_mgr

        # Active rounds (keyed by round_id)
        self._rounds: Dict[str, ExpansionRound] = {}
        self._lock = threading.Lock()

        # Track recent opens for fairness
        self._recent_opens: Dict[str, int] = {}  # member_id -> last_open_timestamp

        # Track cooldowns per target
        self._target_cooldowns: Dict[str, int] = {}  # target_id -> cooldown_until

    def set_rationalization_manager(self, rationalization_mgr) -> None:
        """
        Set rationalization manager after initialization.

        This allows wiring the rationalization manager after it's created,
        avoiding circular dependency issues.

        Args:
            rationalization_mgr: RationalizationManager for redundancy detection
        """
        self.rationalization_mgr = rationalization_mgr

    def _log(self, msg: str, level: str = "info") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"[CoopExpansion] {msg}", level=level)

    def _generate_round_id(self) -> str:
        """Generate a unique round ID."""
        return secrets.token_hex(8)

    def _get_our_id(self) -> str:
        """Get our node's pubkey."""
        if self.our_id:
            return self.our_id
        if self.plugin:
            try:
                return self.plugin.rpc.getinfo().get("id", "")
            except Exception:
                pass
        return ""

    def _check_redundancy(self, target_peer_id: str) -> tuple:
        """
        Check if opening a channel to this target would create unhealthy redundancy.

        Uses rationalization manager to detect if the fleet already has
        sufficient coverage to this peer based on stigmergic markers.

        Slime mold principle: Don't extend tendrils to territory that
        another part of the organism is already exploiting successfully.

        Args:
            target_peer_id: The target peer to check

        Returns:
            Tuple of (is_redundant: bool, reason: str)
        """
        if not self.rationalization_mgr:
            return False, ""

        try:
            coverage = self.rationalization_mgr.analyze_coverage(peer_id=target_peer_id)
            if "error" in coverage:
                return False, ""

            coverages = coverage.get("coverages", [])
            for cov in coverages:
                if cov.get("peer_id") == target_peer_id:
                    redundancy_count = cov.get("redundancy_count", 0)
                    owner = cov.get("owner_pubkey")
                    owner_strength = cov.get("owner_marker_strength", 0)

                    # MAX_HEALTHY_REDUNDANCY = 2
                    if redundancy_count >= 2:
                        return True, (
                            f"Already has {redundancy_count} fleet channels "
                            f"(owner: {owner[:16] if owner else 'none'}..., "
                            f"strength: {owner_strength:.1f})"
                        )

            return False, ""

        except Exception as e:
            self._log(f"Error checking redundancy: {e}", level='debug')
            return False, ""

    def _get_onchain_balance(self) -> int:
        """Get our available onchain balance."""
        if not self.plugin:
            return 0
        try:
            funds = self.plugin.rpc.listfunds()
            outputs = funds.get('outputs', [])
            def _parse_output_sats(o):
                amt = o.get('amount_msat')
                if isinstance(amt, int):
                    return amt // 1000
                if isinstance(amt, str):
                    try:
                        return int(amt.rstrip('msat')) // 1000
                    except (ValueError, TypeError):
                        return o.get('value', 0)
                return o.get('value', 0)

            return sum(
                _parse_output_sats(o)
                for o in outputs if o.get('status') == 'confirmed'
            )
        except Exception:
            return 0

    def _get_budget_constrained_liquidity(self) -> int:
        """
        Get available liquidity constrained by budget settings.

        Applies:
        1. Reserve percentage (keeps some onchain for future ops)
        2. Daily budget limit
        3. Max per-channel percentage of daily budget

        Returns:
            Available sats for channel opening
        """
        raw_balance = self._get_onchain_balance()

        if not self._config_getter:
            # No config, use 80% of raw balance as default
            return int(raw_balance * 0.8)

        try:
            cfg = self._config_getter()
            if not cfg:
                return int(raw_balance * 0.8)

            # Apply reserve percentage (keep some onchain)
            reserve_pct = getattr(cfg, 'budget_reserve_pct', 0.20)
            after_reserve = int(raw_balance * (1.0 - reserve_pct))

            # Apply daily budget limit
            daily_budget = getattr(cfg, 'failsafe_budget_per_day', 1_000_000)

            # Apply max per-channel percentage
            max_per_channel_pct = getattr(cfg, 'budget_max_per_channel_pct', 0.50)
            max_per_channel = int(daily_budget * max_per_channel_pct)

            # Return the minimum of constraints
            available = min(after_reserve, daily_budget, max_per_channel)

            self._log(
                f"Budget-constrained liquidity: {available:,} sats "
                f"(raw={raw_balance:,}, reserve={reserve_pct:.0%}, "
                f"daily={daily_budget:,}, max_per_ch={max_per_channel:,})",
                level='debug'
            )

            return available
        except Exception as e:
            self._log(f"Error calculating budget-constrained liquidity: {e}", level='warn')
            return int(raw_balance * 0.8)

    def check_fleet_affordability(self, min_channel_sats: int = 0) -> Dict[str, Any]:
        """
        Check if ANY hive member can afford the minimum channel size.

        Uses cached budget data from recent GOSSIP messages via StateManager.
        Falls back to local-only check if StateManager not available.

        Args:
            min_channel_sats: Minimum channel size required. If 0, uses config default.

        Returns:
            Dict with:
                - can_afford: True if at least one member can afford
                - affordable_members: List of peer_ids that can afford
                - total_available_sats: Sum of all member budgets
                - stale_count: Number of members with stale budget data
                - freshness_avg_sec: Average age of budget data
                - local_budget_sats: Our own budget (always fresh)
        """
        # Determine minimum channel size
        if min_channel_sats == 0 and self._config_getter:
            try:
                cfg = self._config_getter()
                if cfg:
                    min_channel_sats = getattr(cfg, 'planner_min_channel_sats', 100_000)
            except Exception:
                min_channel_sats = 100_000

        # Get our local budget (always fresh)
        local_budget = self._get_budget_constrained_liquidity()

        # If no state_manager, fall back to local-only check
        if not self.state_manager:
            can_afford = local_budget >= min_channel_sats
            return {
                "can_afford": can_afford,
                "affordable_members": [self._get_our_id()] if can_afford else [],
                "total_available_sats": local_budget,
                "stale_count": 0,
                "freshness_avg_sec": 0,
                "local_budget_sats": local_budget,
                "source": "local_only",
            }

        # Query state_manager for fleet-wide budget summary
        try:
            summary = self.state_manager.get_fleet_budget_summary(
                min_channel_sats=min_channel_sats,
                stale_threshold_sec=600  # 10 minutes
            )

            # Add our local budget to the result
            summary["local_budget_sats"] = local_budget
            summary["source"] = "fleet_gossip"

            # If we can afford but aren't in the list (our gossip might be stale),
            # add ourselves
            our_id = self._get_our_id()
            if local_budget >= min_channel_sats and our_id not in summary.get("affordable_members", []):
                summary["affordable_members"].append(our_id)
                summary["can_afford"] = True

            # Log if no one can afford
            if not summary.get("can_afford"):
                self._log(
                    f"Fleet affordability check: no member can afford {min_channel_sats:,} sats "
                    f"(total_available={summary.get('total_available_sats', 0):,})",
                    level='info'
                )
            else:
                self._log(
                    f"Fleet affordability check: {len(summary.get('affordable_members', []))} members "
                    f"can afford {min_channel_sats:,} sats",
                    level='debug'
                )

            return summary
        except Exception as e:
            self._log(f"Error checking fleet affordability: {e}", level='warn')
            # Fall back to local-only
            can_afford = local_budget >= min_channel_sats
            return {
                "can_afford": can_afford,
                "affordable_members": [self._get_our_id()] if can_afford else [],
                "total_available_sats": local_budget,
                "stale_count": 0,
                "freshness_avg_sec": 0,
                "local_budget_sats": local_budget,
                "source": "local_fallback",
                "error": str(e),
            }

    def _get_channel_count(self) -> int:
        """Get our total channel count."""
        if not self.plugin:
            return 0
        try:
            channels = self.plugin.rpc.listpeerchannels()
            return len(channels.get('channels', []))
        except Exception:
            return 0

    def _has_channel_to(self, peer_id: str) -> bool:
        """Check if we have an existing channel to a peer."""
        if not self.plugin:
            return False
        try:
            channels = self.plugin.rpc.listpeerchannels(id=peer_id)
            return len(channels.get('channels', [])) > 0
        except Exception:
            return False

    def evaluate_expansion(
        self,
        target_peer_id: str,
        event_type: str,
        reporter_id: str,
        capacity_sats: int = 0,
        quality_score: float = None
    ) -> Optional[str]:
        """
        Evaluate whether to start a cooperative expansion round for a peer.

        Called when a PEER_AVAILABLE event is received (typically remote_close).

        Args:
            target_peer_id: The external peer that became available
            event_type: The event type that triggered this
            reporter_id: The hive member who reported the event
            capacity_sats: Capacity of the closed channel
            quality_score: Pre-calculated quality score (optional)

        Returns:
            round_id if a round was started, None otherwise
        """
        now = int(time.time())

        # Check if we're on cooldown for this target
        cooldown_until = self._target_cooldowns.get(target_peer_id, 0)
        if now < cooldown_until:
            self._log(
                f"Target {target_peer_id[:16]}... on cooldown until {cooldown_until}",
                level='debug'
            )
            return None

        # Check if we already have an active round for this target
        with self._lock:
            for round_obj in self._rounds.values():
                if (round_obj.target_peer_id == target_peer_id and
                    round_obj.state in (ExpansionRoundState.NOMINATING, ExpansionRoundState.ELECTING)):
                    self._log(
                        f"Round already active for {target_peer_id[:16]}...",
                        level='debug'
                    )
                    return None

            # Check max active rounds
            active_count = sum(
                1 for r in self._rounds.values()
                if r.state in (ExpansionRoundState.NOMINATING, ExpansionRoundState.ELECTING)
            )
            if active_count >= self.MAX_ACTIVE_ROUNDS:
                self._log("Max active rounds reached", level='debug')
                return None

        # Calculate quality score if not provided
        if quality_score is None and self.quality_scorer:
            result = self.quality_scorer.calculate_score(target_peer_id)
            quality_score = result.overall_score

        # Check minimum quality threshold
        if quality_score is not None and quality_score < self.MIN_QUALITY_SCORE:
            self._log(
                f"Target {target_peer_id[:16]}... quality too low: {quality_score:.2f}",
                level='debug'
            )
            return None

        # Slime mold: Check if this would create unhealthy redundancy
        is_redundant, redundancy_reason = self._check_redundancy(target_peer_id)
        if is_redundant:
            self._log(
                f"Skipping {target_peer_id[:16]}...: redundant coverage - {redundancy_reason}",
                level='info'
            )
            return None

        # Phase 8: Pre-flight affordability check
        # Don't start rounds if no hive member can afford minimum channel
        affordability = self.check_fleet_affordability()
        if not affordability.get("can_afford", False):
            self._log(
                f"Skipping expansion for {target_peer_id[:16]}...: no member can afford "
                f"(total_available={affordability.get('total_available_sats', 0):,} sats)",
                level='info'
            )
            return None

        # Start a new round
        return self.start_round(
            target_peer_id=target_peer_id,
            trigger_event=event_type,
            trigger_reporter=reporter_id,
            quality_score=quality_score or 0.5,
            recommended_size_sats=capacity_sats  # Use closed channel size as hint
        )

    def start_round(
        self,
        target_peer_id: str,
        trigger_event: str,
        trigger_reporter: str,
        quality_score: float = 0.5,
        recommended_size_sats: int = 0
    ) -> str:
        """
        Start a new cooperative expansion round.

        Args:
            target_peer_id: The external peer to consider
            trigger_event: What triggered this round
            trigger_reporter: Who triggered it
            quality_score: Pre-calculated quality score
            recommended_size_sats: Suggested channel size

        Returns:
            The round_id of the new round
        """
        now = int(time.time())
        round_id = self._generate_round_id()

        round_obj = ExpansionRound(
            round_id=round_id,
            target_peer_id=target_peer_id,
            started_at=now,
            state=ExpansionRoundState.NOMINATING,
            trigger_event=trigger_event,
            trigger_reporter=trigger_reporter,
            quality_score=quality_score,
            recommended_size_sats=recommended_size_sats,
            expires_at=now + self.ROUND_EXPIRE_SECONDS,
        )

        with self._lock:
            self._rounds[round_id] = round_obj

        self._log(
            f"Started expansion round {round_id[:8]}... for {target_peer_id[:16]}... "
            f"(quality={quality_score:.2f}, trigger={trigger_event})"
        )

        # Auto-nominate ourselves if eligible
        self._auto_nominate(round_id, target_peer_id, quality_score)

        return round_id

    def join_remote_round(
        self,
        round_id: str,
        target_peer_id: str,
        trigger_reporter: str,
        quality_score: float = 0.5
    ) -> bool:
        """
        Join a remote expansion round (create it locally with the given round_id).

        This is called when we receive a round_id from another member and want
        to participate in that round.

        Args:
            round_id: The remote round ID to join
            target_peer_id: The target peer for the expansion
            trigger_reporter: Who reported this round to us
            quality_score: Pre-calculated quality score

        Returns:
            True if joined successfully, False if round already exists
        """
        now = int(time.time())
        round_obj = ExpansionRound(
            round_id=round_id,
            target_peer_id=target_peer_id,
            started_at=now,
            state=ExpansionRoundState.NOMINATING,
            trigger_event="joined",
            trigger_reporter=trigger_reporter,
            quality_score=quality_score,
            expires_at=now + self.ROUND_EXPIRE_SECONDS,
        )

        with self._lock:
            if round_id in self._rounds:
                return False  # Already have this round
            self._rounds[round_id] = round_obj

        self._log(
            f"Joined remote expansion round {round_id[:8]}... for {target_peer_id[:16]}..."
        )

        # Auto-nominate ourselves
        self._auto_nominate(round_id, target_peer_id, quality_score)

        return True

    def _auto_nominate(self, round_id: str, target_peer_id: str, quality_score: float) -> None:
        """Auto-nominate ourselves for a round if we're eligible."""
        our_id = self._get_our_id()
        if not our_id:
            return

        # Check if we have a channel to the target
        if self._has_channel_to(target_peer_id):
            self._log(
                f"Not nominating - already have channel to {target_peer_id[:16]}...",
                level='debug'
            )
            return

        # Check budget-constrained liquidity (respects reserve and daily budget)
        available = self._get_budget_constrained_liquidity()
        min_required = 1_000_000  # 1M sats minimum
        if available < min_required:
            self._log(
                f"Not nominating - insufficient budget-constrained liquidity: {available:,} sats",
                level='debug'
            )
            return

        # Create nomination with budget-constrained liquidity
        nomination = Nomination(
            nominator_id=our_id,
            target_peer_id=target_peer_id,
            timestamp=int(time.time()),
            available_liquidity_sats=available,
            quality_score=quality_score,
            has_existing_channel=False,
            channel_count=self._get_channel_count(),
            reason="auto_nominate"
        )

        self.add_nomination(round_id, nomination)

    def add_nomination(self, round_id: str, nomination: Nomination) -> bool:
        """
        Add a nomination to a round.

        Args:
            round_id: The round to add to
            nomination: The nomination to add

        Returns:
            True if added, False if round not found or not accepting
        """
        with self._lock:
            round_obj = self._rounds.get(round_id)
            if not round_obj:
                return False

            if round_obj.state != ExpansionRoundState.NOMINATING:
                return False

            # Don't allow nominations from members who already have a channel
            if nomination.has_existing_channel:
                self._log(
                    f"Rejecting nomination from {nomination.nominator_id[:16]}... "
                    f"- has existing channel",
                    level='debug'
                )
                return False

            round_obj.nominations[nomination.nominator_id] = nomination

        self._log(
            f"Added nomination from {nomination.nominator_id[:16]}... "
            f"for round {round_id[:8]}... (liquidity={nomination.available_liquidity_sats})"
        )

        return True

    def handle_nomination(self, peer_id: str, payload: Dict) -> Dict:
        """
        Handle an incoming EXPANSION_NOMINATE message.

        When we receive a nomination for a round we don't know about,
        we join that round (creating it locally with the same round_id).
        This ensures all hive members coordinate on the same expansion round.

        Args:
            peer_id: Sender's pubkey
            payload: Nomination payload

        Returns:
            Response dict
        """
        round_id = payload.get("round_id")
        target_peer_id = payload.get("target_peer_id", "")
        if not round_id:
            return {"error": "missing round_id"}

        # Validate round_id format (prevent oversized or non-string IDs)
        if not isinstance(round_id, str) or len(round_id) > 64:
            return {"success": False, "error": "invalid_round_id"}

        # If we don't know about this round, join it
        with self._lock:
            round_obj = self._rounds.get(round_id)

        if not round_obj and target_peer_id:
            # Check if we have an active round for the same target (merge scenario)
            existing_round_id = None
            with self._lock:
                for rid, r in self._rounds.items():
                    if (r.target_peer_id == target_peer_id and
                        r.state in (ExpansionRoundState.NOMINATING, ExpansionRoundState.ELECTING)):
                        existing_round_id = rid
                        break

            if existing_round_id:
                # We have a different round for same target - use the one with lower ID (deterministic merge)
                if round_id < existing_round_id:
                    # Remote round wins, migrate our nominations
                    self._log(f"Merging our round {existing_round_id[:8]}... into remote {round_id[:8]}...")
                    with self._lock:
                        old_round = self._rounds.pop(existing_round_id, None)
                        if old_round:
                            # Create the new round with remote ID
                            new_round = ExpansionRound(
                                round_id=round_id,
                                target_peer_id=target_peer_id,
                                started_at=old_round.started_at,
                                state=ExpansionRoundState.NOMINATING,
                                trigger_event="merged",
                                trigger_reporter=peer_id,
                                quality_score=payload.get("quality_score", 0.5),
                                expires_at=old_round.started_at + self.ROUND_EXPIRE_SECONDS,
                            )
                            # Copy our nominations
                            new_round.nominations = old_round.nominations.copy()
                            self._rounds[round_id] = new_round
                else:
                    # Our round wins, ignore remote
                    self._log(f"Keeping our round {existing_round_id[:8]}..., ignoring remote {round_id[:8]}...")
                    round_id = existing_round_id
            else:
                # Check active round count before creating from remote
                with self._lock:
                    active_count = sum(
                        1 for r in self._rounds.values()
                        if r.state in (ExpansionRoundState.NOMINATING, ExpansionRoundState.ELECTING)
                    )
                if active_count >= self.MAX_ACTIVE_ROUNDS:
                    self._log(f"Ignoring remote round {round_id[:8]}...: max active rounds reached", level='debug')
                    return {"success": False, "error": "max_active_rounds"}

                # No active round for this target - join the remote round
                self._log(f"Joining remote expansion round {round_id[:8]}... for {target_peer_id[:16]}...")
                now = int(time.time())
                new_round = ExpansionRound(
                    round_id=round_id,
                    target_peer_id=target_peer_id,
                    started_at=now,
                    state=ExpansionRoundState.NOMINATING,
                    trigger_event="joined",
                    trigger_reporter=peer_id,
                    quality_score=payload.get("quality_score", 0.5),
                    expires_at=now + self.ROUND_EXPIRE_SECONDS,
                )
                with self._lock:
                    self._rounds[round_id] = new_round

                # Auto-nominate ourselves for this round
                self._auto_nominate(round_id, target_peer_id, payload.get("quality_score", 0.5))

        nomination = Nomination(
            nominator_id=peer_id,  # Always use authenticated sender, never trust payload
            target_peer_id=target_peer_id,
            timestamp=payload.get("timestamp", int(time.time())),
            available_liquidity_sats=max(0, min(100_000_000_000, payload.get("available_liquidity_sats", 0))),  # Cap at 1000 BTC
            quality_score=max(0.0, min(1.0, payload.get("quality_score", 0.5))),  # Clamp 0-1
            has_existing_channel=payload.get("has_existing_channel", False),
            channel_count=payload.get("channel_count", 0),
            reason=payload.get("reason", "")
        )

        success = self.add_nomination(round_id, nomination)
        return {"success": success, "round_id": round_id, "joined": round_obj is None}

    def elect_winner(self, round_id: str) -> Optional[str]:
        """
        Elect a winner for an expansion round.

        Uses weighted scoring based on:
        - Available liquidity (more = better, up to a point)
        - Channel count (fewer = better, for load balancing)
        - Recent opens (longer since last open = better, for fairness)
        - Quality score agreement (higher = better)

        Args:
            round_id: The round to elect for

        Returns:
            elected_id if successful, None otherwise
        """
        with self._lock:
            round_obj = self._rounds.get(round_id)
            if not round_obj:
                return None

            if round_obj.state != ExpansionRoundState.NOMINATING:
                return None

            nominations = list(round_obj.nominations.values())

            if len(nominations) < self.MIN_NOMINATIONS_FOR_ELECTION:
                round_obj.state = ExpansionRoundState.CANCELLED
                round_obj.result = f"insufficient_nominations ({len(nominations)})"
                self._log(
                    f"Round {round_id[:8]}... cancelled - only {len(nominations)} nominations"
                )
                return None

            round_obj.state = ExpansionRoundState.ELECTING

        # Score each nomination
        scored = []
        now = int(time.time())

        for nom in nominations:
            score = 0.0
            factors = {}

            # Liquidity score (0-1): log scale, caps at 100M sats
            import math
            liquidity_btc = nom.available_liquidity_sats / 100_000_000
            liquidity_score = max(0.0, min(1.0, 0.3 + 0.7 * math.log10(max(0.01, liquidity_btc)) / 2))
            score += liquidity_score * self.WEIGHT_LIQUIDITY
            factors['liquidity'] = round(liquidity_score, 3)

            # Channel count score (0-1): fewer channels = higher score
            # 0 channels = 1.0, 50+ channels = 0.3
            channel_score = max(0.3, 1.0 - (nom.channel_count / 70))
            score += channel_score * self.WEIGHT_CHANNEL_COUNT
            factors['channel_count'] = round(channel_score, 3)

            # Recent opens score (0-1): longer since last open = higher
            last_open = self._recent_opens.get(nom.nominator_id, 0)
            time_since = now - last_open
            if time_since >= 86400:  # 24 hours
                recent_score = 1.0
            elif time_since >= 3600:  # 1 hour
                recent_score = 0.7
            else:
                recent_score = 0.3
            score += recent_score * self.WEIGHT_RECENT_OPENS
            factors['recent_opens'] = round(recent_score, 3)

            # Quality agreement score (use their quality score directly)
            quality_score = nom.quality_score
            score += quality_score * self.WEIGHT_QUALITY_AGREEMENT
            factors['quality'] = round(quality_score, 3)

            factors['total'] = round(score, 3)
            scored.append((nom, score, factors))

        # Sort by score descending, then nominator_id ascending for determinism
        scored.sort(key=lambda x: (-x[1], x[0].nominator_id))

        # Winner is highest scored
        winner, winner_score, winner_factors = scored[0]

        # Phase 8: Store top 3 candidates for potential fallback
        # Format: [(pubkey, score), ...]
        ranked_candidates = [(nom.nominator_id, score) for nom, score, _ in scored[:3]]

        # Capture target_peer_id inside lock for thread safety
        target_peer_id = None
        with self._lock:
            round_obj = self._rounds.get(round_id)
            if round_obj:
                round_obj.elected_id = winner.nominator_id
                round_obj.state = ExpansionRoundState.ELECTED
                round_obj.result = f"elected with score {winner_score:.3f}"
                round_obj.ranked_candidates = ranked_candidates  # Phase 8: Store for fallback
                target_peer_id = round_obj.target_peer_id

            # Track this as a recent open for fairness (inside lock)
            self._recent_opens[winner.nominator_id] = now

            # Set cooldown for this target (inside lock)
            if target_peer_id:
                self._target_cooldowns[target_peer_id] = now + self.COOLDOWN_SECONDS

        self._log(
            f"Round {round_id[:8]}... elected {winner.nominator_id[:16]}... "
            f"(score={winner_score:.3f}, factors={winner_factors})"
        )

        return winner.nominator_id

    def handle_elect(self, peer_id: str, payload: Dict) -> Dict:
        """
        Handle an incoming EXPANSION_ELECT message.

        If we're the elected member, this signals we should proceed
        with opening the channel. Either way, update our local round state.

        Args:
            peer_id: Sender's pubkey
            payload: Election payload

        Returns:
            Response dict with action to take
        """
        round_id = payload.get("round_id")
        elected_id = payload.get("elected_id")
        target_peer_id = payload.get("target_peer_id")
        channel_size_sats = payload.get("channel_size_sats", 0)

        # Update local round state if we have this round
        with self._lock:
            round_obj = self._rounds.get(round_id)
            if round_obj:
                # Only accept election for rounds in valid pre-election states
                if round_obj.state in (ExpansionRoundState.COMPLETED,
                                       ExpansionRoundState.CANCELLED,
                                       ExpansionRoundState.EXPIRED):
                    self._log(
                        f"Round {round_id[:8]}... ignoring election - already {round_obj.state.value}"
                    )
                    return {"action": "ignored", "reason": f"round_already_{round_obj.state.value}"}
                round_obj.state = ExpansionRoundState.COMPLETED
                round_obj.elected_id = elected_id
                round_obj.recommended_size_sats = channel_size_sats
                round_obj.completed_at = int(time.time())
                round_obj.result = f"elected:{elected_id[:16]}..."
                self._log(
                    f"Round {round_id[:8]}... completed - elected {elected_id[:16]}..."
                )

        our_id = self._get_our_id()

        if elected_id == our_id:
            self._log(
                f"We were elected to open channel to {target_peer_id[:16]}... "
                f"(size={channel_size_sats})"
            )
            return {
                "action": "open_channel",
                "target_peer_id": target_peer_id,
                "channel_size_sats": channel_size_sats,
                "round_id": round_id,
            }

        return {"action": "none", "round_id": round_id}

    def handle_decline(self, peer_id: str, payload: Dict) -> Dict:
        """
        Handle an incoming EXPANSION_DECLINE message (Phase 8).

        When the elected member declines (insufficient funds, feerate, etc.),
        fallback to the next ranked candidate if available.

        Args:
            peer_id: Sender's pubkey (should match decliner_id)
            payload: Decline payload with round_id, decliner_id, reason

        Returns:
            Response dict with fallback_elected (new winner) or cancelled
        """
        round_id = payload.get("round_id")
        decliner_id = payload.get("decliner_id")
        reason = payload.get("reason", "unknown")

        if not round_id or not decliner_id:
            return {"error": "missing round_id or decliner_id"}

        # Verify sender matches decliner
        if peer_id != decliner_id:
            self._log(
                f"Decline mismatch: sender {peer_id[:16]}... != decliner {decliner_id[:16]}...",
                level='warning'
            )
            return {"error": "sender does not match decliner"}

        with self._lock:
            round_obj = self._rounds.get(round_id)
            if not round_obj:
                return {"error": "round not found"}

            # Only accept decline from the currently elected member
            if round_obj.elected_id != decliner_id:
                self._log(
                    f"Decline from non-elected member {decliner_id[:16]}... "
                    f"(elected={round_obj.elected_id[:16] if round_obj.elected_id else 'none'}...)",
                    level='warning'
                )
                return {"error": "decliner is not the elected member"}

            # Check if we've exceeded max fallback attempts
            if round_obj.decline_count >= self.MAX_FALLBACK_ATTEMPTS:
                round_obj.state = ExpansionRoundState.CANCELLED
                round_obj.result = f"cancelled_after_{round_obj.decline_count}_declines"
                self._log(
                    f"Round {round_id[:8]}... cancelled - max fallback attempts reached"
                )
                return {
                    "action": "cancelled",
                    "round_id": round_id,
                    "reason": "max_fallbacks_exceeded",
                }

            # Track this decline
            round_obj.decline_count += 1
            round_obj.declined_by.append(decliner_id)

            self._log(
                f"Round {round_id[:8]}... received decline from {decliner_id[:16]}... "
                f"(reason={reason}, decline #{round_obj.decline_count})"
            )

            # Find next candidate not in declined_by
            next_candidate = None
            next_score = 0.0
            for candidate_id, score in round_obj.ranked_candidates:
                if candidate_id not in round_obj.declined_by:
                    next_candidate = candidate_id
                    next_score = score
                    break

            if not next_candidate:
                # No more candidates available
                round_obj.state = ExpansionRoundState.CANCELLED
                round_obj.result = f"no_fallback_candidates_after_{round_obj.decline_count}_declines"
                self._log(
                    f"Round {round_id[:8]}... cancelled - no fallback candidates available"
                )
                return {
                    "action": "cancelled",
                    "round_id": round_id,
                    "reason": "no_fallback_candidates",
                }

            # Elect the fallback candidate
            round_obj.elected_id = next_candidate
            round_obj.result = f"fallback_elected with score {next_score:.3f}"
            target_peer_id = round_obj.target_peer_id
            channel_size_sats = round_obj.recommended_size_sats
            decline_count = round_obj.decline_count

            # Track this as a recent open for fairness (inside lock)
            self._recent_opens[next_candidate] = int(time.time())

        self._log(
            f"Round {round_id[:8]}... fallback elected {next_candidate[:16]}... "
            f"(score={next_score:.3f})"
        )

        return {
            "action": "fallback_elected",
            "round_id": round_id,
            "elected_id": next_candidate,
            "target_peer_id": target_peer_id,
            "channel_size_sats": channel_size_sats,
            "decline_count": decline_count,
        }

    def complete_round(self, round_id: str, success: bool, result: str = "") -> None:
        """
        Mark a round as completed.

        Args:
            round_id: The round to complete
            success: Whether the channel was successfully opened
            result: Result description
        """
        with self._lock:
            round_obj = self._rounds.get(round_id)
            if round_obj:
                round_obj.state = ExpansionRoundState.COMPLETED
                round_obj.completed_at = int(time.time())
                round_obj.result = result or ("success" if success else "failed")

        self._log(f"Round {round_id[:8]}... completed: {result}")

    def cancel_round(self, round_id: str, reason: str = "") -> None:
        """Cancel an active round."""
        with self._lock:
            round_obj = self._rounds.get(round_id)
            if round_obj and round_obj.state in (
                ExpansionRoundState.NOMINATING,
                ExpansionRoundState.ELECTING
            ):
                round_obj.state = ExpansionRoundState.CANCELLED
                round_obj.result = reason or "cancelled"

        self._log(f"Round {round_id[:8]}... cancelled: {reason}")

    def get_round(self, round_id: str) -> Optional[ExpansionRound]:
        """Get a round by ID.

        Note: Returns a direct reference to the internal round object.
        Callers must not mutate the returned object outside of the
        ExpansionCoordinator's lock.
        """
        with self._lock:
            return self._rounds.get(round_id)

    def get_active_rounds(self) -> List[ExpansionRound]:
        """Get all active rounds."""
        with self._lock:
            return [
                r for r in self._rounds.values()
                if r.state in (ExpansionRoundState.NOMINATING, ExpansionRoundState.ELECTING)
            ]

    def get_rounds_for_target(self, target_peer_id: str) -> List[ExpansionRound]:
        """Get all rounds for a specific target."""
        with self._lock:
            return [
                r for r in self._rounds.values()
                if r.target_peer_id == target_peer_id
            ]

    def cleanup_expired_rounds(self) -> int:
        """
        Clean up expired rounds.

        Returns:
            Number of rounds cleaned up
        """
        now = int(time.time())
        cleaned = 0

        with self._lock:
            for round_id, round_obj in list(self._rounds.items()):
                if round_obj.expires_at > 0 and now > round_obj.expires_at:
                    if round_obj.state in (
                        ExpansionRoundState.NOMINATING,
                        ExpansionRoundState.ELECTING
                    ):
                        round_obj.state = ExpansionRoundState.EXPIRED
                        round_obj.result = "expired"
                        cleaned += 1

            # Remove very old rounds (> 1 hour)
            old_threshold = now - 3600
            expired_ids = [
                rid for rid, r in self._rounds.items()
                if r.state in (
                    ExpansionRoundState.COMPLETED,
                    ExpansionRoundState.CANCELLED,
                    ExpansionRoundState.EXPIRED
                ) and r.started_at < old_threshold
            ]
            for rid in expired_ids:
                del self._rounds[rid]

            # Prune stale _recent_opens (older than 7 days) and expired _target_cooldowns (inside lock)
            week_ago = now - 7 * 86400
            self._recent_opens = {k: v for k, v in self._recent_opens.items() if v > week_ago}
            self._target_cooldowns = {k: v for k, v in self._target_cooldowns.items() if v > now}

        if cleaned > 0:
            self._log(f"Cleaned up {cleaned} expired rounds")

        return cleaned

    def get_status(self) -> Dict[str, Any]:
        """Get overall status of the cooperative expansion system."""
        with self._lock:
            active = [r for r in self._rounds.values()
                     if r.state in (ExpansionRoundState.NOMINATING, ExpansionRoundState.ELECTING)]
            # ELECTED and COMPLETED are both "finished" rounds
            completed = [r for r in self._rounds.values()
                        if r.state in (ExpansionRoundState.ELECTED, ExpansionRoundState.COMPLETED)]
            cancelled = [r for r in self._rounds.values()
                        if r.state in (ExpansionRoundState.CANCELLED, ExpansionRoundState.EXPIRED)]

        return {
            "active_rounds": len(active),
            "completed_rounds": len(completed),
            "cancelled_rounds": len(cancelled),
            "total_rounds": len(self._rounds),
            "max_active_rounds": self.MAX_ACTIVE_ROUNDS,
            "active": [r.to_dict() for r in active],
            "recent_completed": [r.to_dict() for r in completed[-5:]],
            "cooldowns": {
                k[:16] + "...": v
                for k, v in self._target_cooldowns.items()
                if v > int(time.time())
            },
        }
