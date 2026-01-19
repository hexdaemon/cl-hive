"""
Splice Coordinator Module

Coordinates timing of splice operations to maintain fleet connectivity.
SAFETY CHECKS ONLY - no fund movement between nodes.

Each node manages its own splices independently, but checks with
the fleet before splice-out to avoid creating connectivity gaps.

How this helps without fund transfer:
- Prevents a node from splicing out if it would break fleet connectivity
- Coordinates timing so another member can open capacity first
- Advisory only - nodes can proceed with their own decision

Author: Lightning Goats Team
"""

import time
from typing import Any, Dict, List, Optional

# =============================================================================
# CONSTANTS
# =============================================================================

# Safety levels
SPLICE_SAFE = "safe"
SPLICE_COORDINATE = "coordinate"  # Wait for another member to add capacity
SPLICE_BLOCKED = "blocked"         # Would break connectivity

# Minimum fleet capacity to maintain to any peer (as fraction of peer's total)
MIN_FLEET_CAPACITY_PCT = 0.05  # 5% - conservative threshold

# Minimum absolute capacity to maintain (1M sats)
MIN_FLEET_CAPACITY_SATS = 1_000_000

# Cache TTL for channel lookups (seconds)
CHANNEL_CACHE_TTL = 300

# Maximum age for liquidity state data to consider valid
MAX_STATE_AGE_HOURS = 1


# =============================================================================
# SPLICE COORDINATOR
# =============================================================================

class SpliceCoordinator:
    """
    Coordinates splice timing to maintain fleet connectivity.

    SAFETY CHECKS ONLY - each node manages its own funds.
    This is advisory - nodes can proceed with their own decision.
    """

    def __init__(self, database: Any, plugin: Any, state_manager: Any = None):
        """
        Initialize the splice coordinator.

        Args:
            database: HiveDatabase instance
            plugin: Plugin instance for RPC calls and logging
            state_manager: Optional StateManager instance for peer state
        """
        self.database = database
        self.plugin = plugin
        self.state_manager = state_manager

        # Cache for channel data
        self._channel_cache: Dict[str, tuple] = {}  # peer_id -> (data, timestamp)

    def _log(self, message: str, level: str = "debug") -> None:
        """Log a message if plugin is available."""
        if self.plugin:
            self.plugin.log(f"SPLICE_COORD: {message}", level=level)

    def check_splice_out_safety(
        self,
        peer_id: str,
        amount_sats: int,
        channel_id: str = None
    ) -> Dict[str, Any]:
        """
        Check if splice-out is safe for fleet connectivity.

        SAFETY CHECK ONLY - no fund movement.

        Args:
            peer_id: External peer we're splicing from
            amount_sats: Amount to splice out
            channel_id: Optional specific channel ID

        Returns:
            Safety assessment with recommendation:
            {
                "safety": "safe" | "coordinate" | "blocked",
                "reason": str,
                "can_proceed": bool,
                "fleet_capacity": int,
                "new_fleet_capacity": int,
                "fleet_share": float,
                "new_share": float,
                "recommendation": str (if not safe)
            }
        """
        try:
            # Get current fleet capacity to this peer
            fleet_capacity = self._get_fleet_capacity_to_peer(peer_id)
            our_capacity = self._get_our_capacity_to_peer(peer_id)
            peer_total = self._get_peer_total_capacity(peer_id)

            self._log(
                f"Splice check: peer={peer_id[:12]}... amount={amount_sats}, "
                f"fleet_cap={fleet_capacity}, our_cap={our_capacity}, peer_total={peer_total}"
            )

            # If we don't know the peer's total capacity, allow local decision
            if peer_total == 0:
                return {
                    "safety": SPLICE_SAFE,
                    "reason": "Unknown peer total capacity, local decision",
                    "can_proceed": True,
                    "fleet_capacity": fleet_capacity,
                    "our_capacity": our_capacity
                }

            current_share = fleet_capacity / peer_total if peer_total > 0 else 0
            new_fleet_capacity = max(0, fleet_capacity - amount_sats)
            new_share = new_fleet_capacity / peer_total if peer_total > 0 else 0

            # Check if we'd maintain minimum connectivity
            if (new_share >= MIN_FLEET_CAPACITY_PCT and
                    new_fleet_capacity >= MIN_FLEET_CAPACITY_SATS):
                return {
                    "safety": SPLICE_SAFE,
                    "reason": f"Post-splice fleet share {new_share:.1%} above minimum {MIN_FLEET_CAPACITY_PCT:.1%}",
                    "can_proceed": True,
                    "fleet_capacity": fleet_capacity,
                    "new_fleet_capacity": new_fleet_capacity,
                    "fleet_share": current_share,
                    "new_share": new_share
                }

            # Check if other members have sufficient capacity
            other_member_capacity = fleet_capacity - our_capacity
            if other_member_capacity >= MIN_FLEET_CAPACITY_SATS:
                return {
                    "safety": SPLICE_SAFE,
                    "reason": f"Other members have {other_member_capacity:,} sats to this peer",
                    "can_proceed": True,
                    "fleet_capacity": fleet_capacity,
                    "other_member_capacity": other_member_capacity,
                    "fleet_share": current_share,
                    "new_share": new_share
                }

            # Check if this is a partial splice that maintains minimum
            if our_capacity > amount_sats:
                remaining = our_capacity - amount_sats
                if remaining >= MIN_FLEET_CAPACITY_SATS:
                    return {
                        "safety": SPLICE_SAFE,
                        "reason": f"Partial splice leaves {remaining:,} sats capacity",
                        "can_proceed": True,
                        "fleet_capacity": fleet_capacity,
                        "remaining_capacity": remaining,
                        "fleet_share": current_share,
                        "new_share": new_share
                    }

            # Would create connectivity gap - recommend coordination
            if new_fleet_capacity > 0:
                return {
                    "safety": SPLICE_COORDINATE,
                    "reason": f"Would reduce fleet share to {new_share:.1%}",
                    "can_proceed": True,  # Advisory only
                    "recommendation": "Consider waiting for another member to add capacity first",
                    "fleet_capacity": fleet_capacity,
                    "new_fleet_capacity": new_fleet_capacity,
                    "fleet_share": current_share,
                    "new_share": new_share
                }

            # Would completely break connectivity
            return {
                "safety": SPLICE_BLOCKED,
                "reason": f"Would eliminate fleet connectivity to this peer",
                "can_proceed": False,  # Strong recommendation against
                "recommendation": "Another member should open channel to this peer first",
                "fleet_capacity": fleet_capacity,
                "new_fleet_capacity": 0,
                "fleet_share": current_share,
                "new_share": 0
            }

        except Exception as e:
            self._log(f"Error checking splice safety: {e}", level="warning")
            # Fail open - allow local decision
            return {
                "safety": SPLICE_SAFE,
                "reason": f"Safety check failed ({e}), local decision",
                "can_proceed": True,
                "error": str(e)
            }

    def check_splice_in_safety(
        self,
        peer_id: str,
        amount_sats: int
    ) -> Dict[str, Any]:
        """
        Check splice-in safety (always safe - increases capacity).

        Args:
            peer_id: External peer we're splicing into
            amount_sats: Amount to splice in

        Returns:
            Safety assessment (always safe for splice-in)
        """
        return {
            "safety": SPLICE_SAFE,
            "reason": "Splice-in always safe (increases capacity)",
            "can_proceed": True,
            "amount_sats": amount_sats
        }

    def _get_fleet_capacity_to_peer(self, peer_id: str) -> int:
        """
        Get total fleet capacity to an external peer.

        Combines:
        - Our own capacity
        - Reported capacity from other hive members (via liquidity state)
        """
        total = 0

        # Get our own capacity first
        total += self._get_our_capacity_to_peer(peer_id)

        # Add capacity from other members via state manager
        if self.state_manager:
            try:
                for peer_state in self.state_manager.get_all_peer_states():
                    # Check if this member has the target peer in their topology
                    topology = peer_state.topology or []
                    if peer_id in topology:
                        # Estimate based on their average channel size
                        # This is an approximation since we don't track per-channel data
                        if peer_state.capacity_sats > 0 and len(topology) > 0:
                            avg_channel = peer_state.capacity_sats // len(topology)
                            total += avg_channel
            except Exception as e:
                self._log(f"Error getting fleet capacity from state: {e}", level="debug")

        return total

    def _get_our_capacity_to_peer(self, peer_id: str) -> int:
        """Get our capacity to an external peer."""
        # Check cache first
        cache_key = f"our_to_{peer_id}"
        if cache_key in self._channel_cache:
            data, timestamp = self._channel_cache[cache_key]
            if time.time() - timestamp < CHANNEL_CACHE_TTL:
                return data

        try:
            channels = self.plugin.rpc.listpeerchannels(id=peer_id)
            total = sum(
                ch.get("total_msat", 0) // 1000
                for ch in channels.get("channels", [])
                if ch.get("state") == "CHANNELD_NORMAL"
            )

            # Cache result
            self._channel_cache[cache_key] = (total, time.time())
            return total

        except Exception as e:
            self._log(f"Error getting our capacity to {peer_id[:12]}...: {e}", level="debug")
            return 0

    def _get_peer_total_capacity(self, peer_id: str) -> int:
        """Get external peer's total public capacity."""
        # Check cache first
        cache_key = f"peer_total_{peer_id}"
        if cache_key in self._channel_cache:
            data, timestamp = self._channel_cache[cache_key]
            if time.time() - timestamp < CHANNEL_CACHE_TTL:
                return data

        try:
            # Get channels where this peer is the source
            channels = self.plugin.rpc.listchannels(source=peer_id)
            total = sum(
                ch.get("amount_msat", 0) // 1000
                for ch in channels.get("channels", [])
            )

            # Cache result
            self._channel_cache[cache_key] = (total, time.time())
            return total

        except Exception as e:
            self._log(f"Error getting peer total capacity for {peer_id[:12]}...: {e}", level="debug")
            return 0

    def get_splice_recommendations(self, peer_id: str) -> Dict[str, Any]:
        """
        Get splice recommendations for a peer.

        Returns info about fleet connectivity and safe splice amounts.

        Args:
            peer_id: External peer to analyze

        Returns:
            Recommendations for splice operations
        """
        fleet_capacity = self._get_fleet_capacity_to_peer(peer_id)
        our_capacity = self._get_our_capacity_to_peer(peer_id)
        peer_total = self._get_peer_total_capacity(peer_id)
        other_member_capacity = fleet_capacity - our_capacity

        current_share = fleet_capacity / peer_total if peer_total > 0 else 0

        # Calculate safe splice-out amount
        if other_member_capacity >= MIN_FLEET_CAPACITY_SATS:
            # Other members have coverage, we can splice out fully
            safe_splice_out = our_capacity
        else:
            # Need to maintain minimum connectivity
            required = max(
                MIN_FLEET_CAPACITY_SATS,
                int(peer_total * MIN_FLEET_CAPACITY_PCT)
            )
            safe_splice_out = max(0, fleet_capacity - required)

        return {
            "peer_id": peer_id,
            "fleet_capacity": fleet_capacity,
            "our_capacity": our_capacity,
            "other_member_capacity": other_member_capacity,
            "peer_total_capacity": peer_total,
            "fleet_share": current_share,
            "safe_splice_out_amount": safe_splice_out,
            "has_fleet_coverage": other_member_capacity >= MIN_FLEET_CAPACITY_SATS,
            "recommendations": self._build_recommendations(
                our_capacity, other_member_capacity, safe_splice_out
            )
        }

    def _build_recommendations(
        self,
        our_capacity: int,
        other_member_capacity: int,
        safe_splice_out: int
    ) -> List[str]:
        """Build human-readable recommendations."""
        recs = []

        if other_member_capacity >= MIN_FLEET_CAPACITY_SATS:
            recs.append("Other fleet members provide coverage - safe to splice out fully")
        elif safe_splice_out > 0:
            recs.append(f"Safe to splice out up to {safe_splice_out:,} sats")
        else:
            recs.append("Splicing out would break fleet connectivity")
            recs.append("Wait for another member to open channel first")

        if our_capacity == 0:
            recs.append("You have no channel to this peer")

        return recs

    def get_status(self) -> Dict[str, Any]:
        """Get splice coordinator status."""
        return {
            "active": True,
            "cache_entries": len(self._channel_cache),
            "min_fleet_capacity_pct": MIN_FLEET_CAPACITY_PCT,
            "min_fleet_capacity_sats": MIN_FLEET_CAPACITY_SATS
        }

    def clear_cache(self) -> None:
        """Clear the channel cache."""
        self._channel_cache.clear()
        self._log("Channel cache cleared")
