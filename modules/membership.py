"""
Membership module for cl-hive.

Implements tier management, promotion evaluation, and quorum logic.
"""

import math
import time
from enum import Enum
from typing import Any, Dict, List, Optional

from . import network_metrics


ACTIVE_MEMBER_WINDOW_SECONDS = 24 * 3600
BAN_QUORUM_THRESHOLD = 0.51  # 51% quorum for ban proposals
CONTRIBUTION_RATIO_NO_DATA = 999999999


class MembershipTier(str, Enum):
    """
    Membership tiers.

    Two-tier system:
    - NEOPHYTE: New members in 90-day probation period. Can route but cannot vote.
    - MEMBER: Full members. Can vote, participate in settlements, vouch for others.

    Promotion is automatic when criteria are met (no admin approval needed).
    """
    NEOPHYTE = "neophyte"
    MEMBER = "member"


class MembershipManager:
    """Membership logic and promotion evaluation."""

    def __init__(self, db, state_manager, contribution_mgr, bridge, config, plugin=None,
                 metrics_calculator=None):
        self.db = db
        self.state_manager = state_manager
        self.contribution_mgr = contribution_mgr
        self.bridge = bridge
        self.config = config
        self.plugin = plugin
        self.metrics_calculator = metrics_calculator

    def _log(self, msg: str, level: str = "info") -> None:
        if self.plugin:
            self.plugin.log(f"[Membership] {msg}", level=level)

    def get_tier(self, peer_id: str) -> Optional[str]:
        member = self.db.get_member(peer_id)
        return member["tier"] if member else None

    def set_tier(self, peer_id: str, tier: str) -> bool:
        now = int(time.time())
        # Set promoted_at for member tier
        promoted_at = now if tier == MembershipTier.MEMBER.value else None

        updated = self.db.update_member(peer_id, tier=tier, promoted_at=promoted_at)
        if not updated:
            return False

        # Members get hive policy (0 PPM fees)
        is_full_member = tier == MembershipTier.MEMBER.value
        if self.bridge and getattr(self.bridge, "status", None) and self.bridge.status.value == "enabled":
            try:
                self.bridge.set_hive_policy(peer_id, is_member=is_full_member)
            except Exception:
                self._log(f"Bridge policy update failed for {peer_id[:16]}...", level="warn")

        return True

    def sync_bridge_policies(self) -> int:
        """
        Sync bridge policies with database membership state.

        Call this on startup to ensure all members have correct 0 ppm
        fee policy applied, even if a previous set_tier() bridge call failed.

        Returns:
            Number of policies synced
        """
        if not self.bridge:
            return 0

        # Check if bridge is enabled
        if not (hasattr(self.bridge, "status") and
                self.bridge.status and
                self.bridge.status.value == "enabled"):
            self._log("Bridge not enabled, skipping policy sync")
            return 0

        synced = 0
        members = self.db.get_all_members()

        for member in members:
            peer_id = member.get("peer_id")
            tier = member.get("tier")
            if not peer_id:
                continue

            is_full_member = tier == MembershipTier.MEMBER.value
            try:
                # Bypass rate limit on startup sync
                success = self.bridge.set_hive_policy(
                    peer_id, is_member=is_full_member, bypass_rate_limit=True
                )
                if success:
                    synced += 1
            except Exception as exc:
                self._log(f"Failed to sync policy for {peer_id[:16]}...: {exc}", level="warn")

        if synced > 0:
            self._log(f"Synced bridge policies for {synced} members")

        return synced

    def is_probation_complete(self, peer_id: str) -> bool:
        member = self.db.get_member(peer_id)
        if not member:
            return False
        joined_at = member.get("joined_at")
        if not isinstance(joined_at, int):
            return False
        return int(time.time()) >= joined_at + (self.config.probation_days * 86400)

    def calculate_uptime(self, peer_id: str) -> float:
        presence = self.db.get_presence(peer_id)
        if not presence:
            return 0.0

        now = int(time.time())
        online_seconds = presence["online_seconds_rolling"]
        last_change = presence["last_change_ts"]
        window_start = presence["window_start_ts"]
        is_online = bool(presence["is_online"])
        window_seconds = max(1, now - window_start)

        if is_online:
            online_seconds += max(0, now - last_change)

        uptime_pct = min(100.0, (online_seconds / window_seconds) * 100.0)
        return uptime_pct

    def calculate_contribution_ratio(self, peer_id: str) -> float:
        stats = self.contribution_mgr.get_contribution_stats(peer_id, window_days=30)
        forwarded = stats["forwarded"]
        received = stats["received"]
        if received == 0:
            return 1.0 if forwarded == 0 else CONTRIBUTION_RATIO_NO_DATA
        return forwarded / received

    def get_unique_peers(self, peer_id: str) -> List[str]:
        """
        Get external peers that only this member connects to.

        Uses shared NetworkMetricsCalculator if available for cached,
        consistent results across all modules.
        """
        # Try shared calculator first (preferred)
        calculator = self.metrics_calculator or network_metrics.get_calculator()
        if calculator:
            return calculator.get_unique_peers(peer_id)

        # Fallback: local calculation
        return self._calculate_unique_peers_local(peer_id)

    def _calculate_unique_peers_local(self, peer_id: str) -> List[str]:
        """Local fallback for unique peers calculation."""
        peer_state = self.state_manager.get_peer_state(peer_id)
        if not peer_state:
            return []

        peer_topology = set(peer_state.topology or [])
        if not peer_topology:
            return []

        member_peers = set()
        for member in self.db.get_all_members():
            if member.get("tier") != MembershipTier.MEMBER.value:
                continue
            state = self.state_manager.get_peer_state(member["peer_id"])
            if state and state.topology:
                member_peers.update(state.topology)

        unique = peer_topology - member_peers
        return list(unique)

    def evaluate_promotion(self, peer_id: str) -> Dict[str, Any]:
        """
        Evaluate if a neophyte is eligible for automatic promotion to member.

        Criteria (all must be met):
        1. Probation period complete (90 days default)
        2. Uptime >= min_uptime_pct (95% default)
        3. Contribution ratio >= min_contribution_ratio (1.0 default)
        4. Unique peers >= min_unique_peers (1 default)

        Fast-track promotion (optional):
        If hive_centrality >= 0.5 (well-connected to fleet), probation can be
        waived after minimum 30 days if all other criteria are met.

        No vouching required - purely meritocratic.
        """
        reasons: List[str] = []

        member = self.db.get_member(peer_id)
        if not member:
            reasons.append("unknown_peer")
            return {"eligible": False, "reasons": reasons}

        if member.get("tier") != MembershipTier.NEOPHYTE.value:
            reasons.append("not_neophyte")
            return {"eligible": False, "reasons": reasons}

        # Get hive centrality metrics
        hive_metrics = self._get_hive_centrality_metrics(peer_id)
        hive_centrality = hive_metrics.get("hive_centrality", 0.0)
        hive_peer_count = hive_metrics.get("hive_peer_count", 0)

        # Check for fast-track eligibility (high connectivity)
        fast_track_eligible = False
        fast_track_min_days = 30
        if hive_centrality >= 0.5:
            joined_at = member.get("joined_at")
            if joined_at:
                days_as_member = (int(time.time()) - joined_at) / (24 * 3600)
                if days_as_member >= fast_track_min_days:
                    fast_track_eligible = True

        # Check probation period (can be bypassed with fast-track)
        probation_complete = self.is_probation_complete(peer_id)
        if not probation_complete and not fast_track_eligible:
            reasons.append("probation_incomplete")

        # Check uptime (use config value)
        uptime = self.calculate_uptime(peer_id)
        min_uptime = getattr(self.config, 'min_uptime_pct', 95.0)
        if uptime < min_uptime:
            reasons.append(f"uptime_below_threshold ({uptime:.1f}% < {min_uptime}%)")

        # Check contribution ratio (use config value)
        ratio = self.calculate_contribution_ratio(peer_id)
        min_ratio = getattr(self.config, 'min_contribution_ratio', 1.0)
        if ratio < min_ratio:
            reasons.append(f"contribution_ratio_below_threshold ({ratio:.2f} < {min_ratio})")

        # Check unique peers (use config value)
        unique_peers = self.get_unique_peers(peer_id)
        min_peers = getattr(self.config, 'min_unique_peers', 1)
        if len(unique_peers) < min_peers:
            reasons.append(f"unique_peers_below_threshold ({len(unique_peers)} < {min_peers})")

        eligible = len(reasons) == 0
        return {
            "eligible": eligible,
            "reasons": reasons,
            "uptime_pct": uptime,
            "contribution_ratio": ratio,
            "unique_peers": unique_peers,
            "hive_centrality": round(hive_centrality, 3),
            "hive_peer_count": hive_peer_count,
            "fast_track": {
                "eligible": fast_track_eligible,
                "reason": "high_hive_centrality" if fast_track_eligible else None,
                "min_days": fast_track_min_days,
                "min_centrality": 0.5
            },
            "thresholds": {
                "min_uptime_pct": min_uptime,
                "min_contribution_ratio": min_ratio,
                "min_unique_peers": min_peers,
                "probation_days": self.config.probation_days
            }
        }

    def _get_hive_centrality_metrics(self, peer_id: str) -> Dict[str, Any]:
        """
        Get hive centrality metrics for a peer.

        Uses the shared network metrics calculator if available.
        """
        calculator = self.metrics_calculator or network_metrics.get_calculator()
        if calculator:
            metrics = calculator.get_member_metrics(peer_id)
            if metrics:
                return {
                    "hive_centrality": metrics.hive_centrality,
                    "hive_peer_count": metrics.hive_peer_count,
                    "hive_reachability": metrics.hive_reachability,
                    "rebalance_hub_score": metrics.rebalance_hub_score
                }

        # Fallback: no metrics available
        return {
            "hive_centrality": 0.0,
            "hive_peer_count": 0,
            "hive_reachability": 0.0,
            "rebalance_hub_score": 0.0
        }

    def get_neophyte_rankings(self) -> List[Dict[str, Any]]:
        """
        Get all neophytes ranked by their promotion readiness.

        Useful for identifying which neophytes are closest to promotion
        and which ones are demonstrating high commitment via connectivity.

        Returns:
            List of neophytes with their metrics, sorted by readiness score.
        """
        neophytes = []

        for member in self.db.get_all_members():
            if member.get("tier") != MembershipTier.NEOPHYTE.value:
                continue

            peer_id = member["peer_id"]
            evaluation = self.evaluate_promotion(peer_id)

            # Calculate a readiness score (0-100)
            score = 0

            # Probation progress (0-40 points)
            joined_at = member.get("joined_at", int(time.time()))
            days_elapsed = (int(time.time()) - joined_at) / (24 * 3600)
            probation_days = self.config.probation_days
            probation_progress = min(days_elapsed / probation_days, 1.0)
            score += probation_progress * 40

            # Uptime (0-20 points)
            uptime = evaluation.get("uptime_pct", 0)
            min_uptime = evaluation.get("thresholds", {}).get("min_uptime_pct", 95)
            uptime_score = min(uptime / min_uptime, 1.0) if min_uptime > 0 else 0
            score += uptime_score * 20

            # Contribution (0-20 points)
            ratio = evaluation.get("contribution_ratio", 0)
            min_ratio = evaluation.get("thresholds", {}).get("min_contribution_ratio", 1.0)
            contrib_score = min(ratio / min_ratio, 1.0) if min_ratio > 0 else 0
            score += contrib_score * 20

            # Hive connectivity bonus (0-20 points)
            hive_centrality = evaluation.get("hive_centrality", 0)
            score += hive_centrality * 20

            neophytes.append({
                "peer_id": peer_id,
                "peer_id_short": peer_id[:16] + "...",
                "readiness_score": round(score, 1),
                "eligible": evaluation.get("eligible", False),
                "fast_track_eligible": evaluation.get("fast_track", {}).get("eligible", False),
                "days_as_neophyte": round(days_elapsed, 1),
                "uptime_pct": evaluation.get("uptime_pct", 0),
                "contribution_ratio": evaluation.get("contribution_ratio", 0),
                "hive_centrality": hive_centrality,
                "hive_peer_count": evaluation.get("hive_peer_count", 0),
                "blocking_reasons": evaluation.get("reasons", [])
            })

        # Sort by readiness score (highest first)
        neophytes.sort(key=lambda x: x["readiness_score"], reverse=True)

        return neophytes

    def get_active_members(self) -> List[str]:
        now = int(time.time())
        active = []
        for member in self.db.get_all_members():
            if member.get("tier") != MembershipTier.MEMBER.value:
                continue
            last_seen = member.get("last_seen")
            if not isinstance(last_seen, int):
                continue
            if now - last_seen > ACTIVE_MEMBER_WINDOW_SECONDS:
                continue
            if self.db.is_banned(member["peer_id"]):
                continue
            active.append(member["peer_id"])
        return active

    def calculate_quorum(self, active_members: int) -> int:
        """
        Calculate quorum for voting (bans, promotions, etc).

        Uses simple majority (51%) with minimum of 2 votes, except for
        single-member bootstrap case where 1 vote is sufficient.
        """
        # Bootstrap case: single member can approve alone
        if active_members == 1:
            return 1

        threshold = math.ceil(active_members * 0.51)  # Simple majority
        return max(2, threshold)

    def build_vouch_message(self, target_pubkey: str, request_id: str, timestamp: int) -> str:
        """
        DEPRECATED: Vouch-based promotion is no longer used.
        Kept for backward compatibility with existing message handlers.
        """
        return f"hive:vouch:{target_pubkey}:{request_id}:{timestamp}"

    # =========================================================================
    # MANUAL PROMOTION (majority vote bypass of probation period)
    # =========================================================================

    def propose_manual_promotion(self, target_peer_id: str, proposer_peer_id: str) -> Dict[str, Any]:
        """
        Propose a neophyte for early promotion to member status.

        Any member can propose a neophyte for promotion before the 90-day
        probation period completes. When a majority (51%) of active members
        approve, the neophyte is promoted.

        Args:
            target_peer_id: The neophyte to propose for promotion
            proposer_peer_id: The member making the proposal

        Returns:
            Dict with success status, message, and proposal details
        """
        # Verify proposer is a member
        proposer_tier = self.get_tier(proposer_peer_id)
        if proposer_tier != MembershipTier.MEMBER.value:
            return {
                "success": False,
                "error": "only_members_can_propose",
                "message": "Only members can propose promotions"
            }

        # Verify target is a neophyte
        target_tier = self.get_tier(target_peer_id)
        if target_tier is None:
            return {
                "success": False,
                "error": "unknown_peer",
                "message": "Target peer is not in the hive"
            }
        if target_tier != MembershipTier.NEOPHYTE.value:
            return {
                "success": False,
                "error": "not_neophyte",
                "message": "Target is already a member or not a neophyte"
            }

        # Check if there's already a pending proposal
        existing = self.db.get_admin_promotion(target_peer_id)
        if existing and existing.get("status") == "pending":
            return {
                "success": False,
                "error": "proposal_exists",
                "message": "A promotion proposal already exists for this peer"
            }

        # Create the proposal
        created = self.db.create_admin_promotion(target_peer_id, proposer_peer_id)
        if not created:
            return {
                "success": False,
                "error": "db_error",
                "message": "Failed to create proposal"
            }

        # Proposer's vote counts as an approval
        self.db.add_admin_promotion_approval(target_peer_id, proposer_peer_id)

        self._log(f"Manual promotion proposed for {target_peer_id[:16]}... by {proposer_peer_id[:16]}...")

        return {
            "success": True,
            "message": "Promotion proposal created",
            "target_peer_id": target_peer_id,
            "proposed_by": proposer_peer_id,
            "approvals": 1
        }

    def vote_on_promotion(self, target_peer_id: str, voter_peer_id: str) -> Dict[str, Any]:
        """
        Vote to approve a neophyte's promotion to member.

        Args:
            target_peer_id: The neophyte being voted on
            voter_peer_id: The member casting the vote

        Returns:
            Dict with success status and current approval count
        """
        # Verify voter is a member
        voter_tier = self.get_tier(voter_peer_id)
        if voter_tier != MembershipTier.MEMBER.value:
            return {
                "success": False,
                "error": "only_members_can_vote",
                "message": "Only members can vote on promotions"
            }

        # Check proposal exists
        proposal = self.db.get_admin_promotion(target_peer_id)
        if not proposal or proposal.get("status") != "pending":
            return {
                "success": False,
                "error": "no_pending_proposal",
                "message": "No pending promotion proposal for this peer"
            }

        # Check if already voted
        approvals = self.db.get_admin_promotion_approvals(target_peer_id)
        voter_ids = [a["approver_peer_id"] for a in approvals]
        if voter_peer_id in voter_ids:
            return {
                "success": False,
                "error": "already_voted",
                "message": "You have already voted on this promotion"
            }

        # Add the vote
        self.db.add_admin_promotion_approval(target_peer_id, voter_peer_id)

        self._log(f"Promotion vote added for {target_peer_id[:16]}... by {voter_peer_id[:16]}...")

        # Check if quorum reached
        quorum_result = self.check_promotion_quorum(target_peer_id)

        return {
            "success": True,
            "message": "Vote recorded",
            "target_peer_id": target_peer_id,
            "approvals": quorum_result["approvals"],
            "required": quorum_result["required"],
            "quorum_reached": quorum_result["quorum_reached"]
        }

    def check_promotion_quorum(self, target_peer_id: str) -> Dict[str, Any]:
        """
        Check if a promotion proposal has reached majority quorum.

        Returns:
            Dict with approval count, required count, and quorum status
        """
        active_members = self.get_active_members()
        required = self.calculate_quorum(len(active_members))

        approvals = self.db.get_admin_promotion_approvals(target_peer_id)
        # Only count approvals from current active members
        active_set = set(active_members)
        valid_approvals = [a for a in approvals if a["approver_peer_id"] in active_set]

        quorum_reached = len(valid_approvals) >= required

        return {
            "target_peer_id": target_peer_id,
            "approvals": len(valid_approvals),
            "required": required,
            "active_members": len(active_members),
            "quorum_reached": quorum_reached,
            "approvers": [a["approver_peer_id"] for a in valid_approvals]
        }

    def execute_manual_promotion(self, target_peer_id: str) -> Dict[str, Any]:
        """
        Execute a manual promotion if quorum has been reached.

        This bypasses the normal 90-day probation period when a majority
        of members have approved the promotion.

        Returns:
            Dict with success status and result details
        """
        # Check quorum
        quorum = self.check_promotion_quorum(target_peer_id)
        if not quorum["quorum_reached"]:
            return {
                "success": False,
                "error": "quorum_not_reached",
                "message": f"Need {quorum['required']} approvals, have {quorum['approvals']}",
                **quorum
            }

        # Verify target is still a neophyte
        target_tier = self.get_tier(target_peer_id)
        if target_tier != MembershipTier.NEOPHYTE.value:
            return {
                "success": False,
                "error": "not_neophyte",
                "message": "Target is no longer a neophyte"
            }

        # Execute the promotion
        promoted = self.set_tier(target_peer_id, MembershipTier.MEMBER.value)
        if not promoted:
            return {
                "success": False,
                "error": "promotion_failed",
                "message": "Failed to update tier"
            }

        # Mark proposal as complete
        self.db.complete_admin_promotion(target_peer_id)

        self._log(f"Manual promotion executed for {target_peer_id[:16]}... with {quorum['approvals']} approvals")

        return {
            "success": True,
            "message": "Neophyte promoted to member",
            "target_peer_id": target_peer_id,
            "approvals": quorum["approvals"],
            "approvers": quorum["approvers"]
        }

    def get_pending_promotions(self) -> List[Dict[str, Any]]:
        """
        Get all pending manual promotion proposals with their status.

        Returns:
            List of pending proposals with approval counts
        """
        pending = self.db.get_pending_admin_promotions()
        result = []

        for p in pending:
            target = p["target_peer_id"]
            quorum = self.check_promotion_quorum(target)

            result.append({
                "target_peer_id": target,
                "proposed_by": p["proposed_by"],
                "proposed_at": p["proposed_at"],
                "approvals": quorum["approvals"],
                "required": quorum["required"],
                "quorum_reached": quorum["quorum_reached"],
                "approvers": quorum["approvers"]
            })

        return result
