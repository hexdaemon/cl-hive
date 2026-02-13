"""
Tests for rebalancing activity coordination (Gaps A+C, D).

Covers:
- Targeted DB update preserves depleted/saturated counts
- Coordinator merges in-memory state correctly
- Coordinator rejects non-member updates
- Enriched needs stored and used by assess_our_liquidity_needs
"""

import pytest
import time
import threading
from unittest.mock import MagicMock

from modules.liquidity_coordinator import (
    LiquidityCoordinator,
    NEED_OUTBOUND,
    NEED_INBOUND,
    URGENCY_HIGH,
    URGENCY_MEDIUM,
)


class MockPlugin:
    def __init__(self):
        self.logs = []
        self.rpc = MagicMock()

    def log(self, msg, level="info"):
        self.logs.append({"msg": msg, "level": level})


class MockDatabase:
    def __init__(self):
        self.members = {}
        self._liquidity_state = {}

    def get_all_members(self):
        return list(self.members.values())

    def get_member(self, peer_id):
        return self.members.get(peer_id)

    def update_member_liquidity_state(self, **kwargs):
        self._liquidity_state[kwargs.get("member_id")] = kwargs

    def update_rebalancing_activity(self, member_id, rebalancing_active,
                                     rebalancing_peers=None, timestamp=None):
        existing = self._liquidity_state.get(member_id, {})
        existing["rebalancing_active"] = rebalancing_active
        existing["rebalancing_peers"] = rebalancing_peers or []
        existing["member_id"] = member_id
        self._liquidity_state[member_id] = existing

    def get_member_liquidity_state(self, member_id):
        return self._liquidity_state.get(member_id)

    def store_liquidity_need(self, **kwargs):
        pass

    def get_member_health(self, peer_id):
        return None


class MockStateManager:
    def get(self, key, default=None):
        return default

    def set(self, key, value):
        pass

    def get_state(self, key, default=None):
        return default

    def set_state(self, key, value):
        pass

    def get_all_peer_states(self):
        return []


PEER1 = "02" + "a" * 64
OUR_PUBKEY = "02" + "0" * 64


class TestUpdateRebalancingActivityPreservesData:
    """Targeted rebalancing activity update preserves depleted/saturated counts."""

    def setup_method(self):
        self.db = MockDatabase()
        self.db.members = {PEER1: {"peer_id": PEER1, "tier": "member"},
                           OUR_PUBKEY: {"peer_id": OUR_PUBKEY, "tier": "admin"}}
        self.plugin = MockPlugin()
        self.coord = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey=OUR_PUBKEY,
            state_manager=MockStateManager()
        )

    def test_update_rebalancing_activity_preserves_depleted_count(self):
        """Existing row's depleted_channels should be unchanged after activity update."""
        # First record a full liquidity report
        self.coord.record_member_liquidity_report(
            member_id=PEER1,
            depleted_channels=[{"peer_id": "ext1", "local_pct": 0.1, "capacity_sats": 1000000}],
            saturated_channels=[{"peer_id": "ext2", "local_pct": 0.9, "capacity_sats": 500000}],
            rebalancing_active=False,
            rebalancing_peers=[]
        )

        # Now do a targeted activity update
        result = self.coord.update_rebalancing_activity(
            member_id=PEER1,
            rebalancing_active=True,
            rebalancing_peers=["ext1", "ext3"]
        )
        assert result["status"] == "updated"

        # Verify depleted_channels preserved in memory
        state = self.coord._member_liquidity_state[PEER1]
        assert len(state["depleted_channels"]) == 1
        assert state["depleted_channels"][0]["peer_id"] == "ext1"
        assert len(state["saturated_channels"]) == 1
        assert state["rebalancing_active"] is True
        assert state["rebalancing_peers"] == ["ext1", "ext3"]

    def test_update_rebalancing_activity_creates_row_if_missing(self):
        """No prior in-memory state — should create entry with rebalancing fields."""
        result = self.coord.update_rebalancing_activity(
            member_id=PEER1,
            rebalancing_active=True,
            rebalancing_peers=["ext1"]
        )
        assert result["status"] == "updated"

        state = self.coord._member_liquidity_state[PEER1]
        assert state["rebalancing_active"] is True
        assert state["rebalancing_peers"] == ["ext1"]
        assert "timestamp" in state

    def test_coordinator_merges_in_memory_state(self):
        """Existing depleted_channels preserved after targeted update."""
        # Manually set in-memory state
        self.coord._member_liquidity_state[PEER1] = {
            "depleted_channels": [{"peer_id": "ext1"}],
            "saturated_channels": [],
            "rebalancing_active": False,
            "rebalancing_peers": [],
            "timestamp": int(time.time()) - 60
        }

        self.coord.update_rebalancing_activity(
            member_id=PEER1,
            rebalancing_active=True,
            rebalancing_peers=["ext2"]
        )

        state = self.coord._member_liquidity_state[PEER1]
        # depleted_channels should still be there
        assert state["depleted_channels"] == [{"peer_id": "ext1"}]
        assert state["rebalancing_active"] is True
        assert state["rebalancing_peers"] == ["ext2"]

    def test_coordinator_rejects_non_member(self):
        """Unknown peer should return error."""
        result = self.coord.update_rebalancing_activity(
            member_id="02" + "f" * 64,
            rebalancing_active=True,
            rebalancing_peers=[]
        )
        assert result.get("error") == "member_not_found"


class TestEnrichedNeedsIntegration:
    """Enriched liquidity needs from cl-revenue-ops override raw assessment."""

    def setup_method(self):
        self.db = MockDatabase()
        self.db.members = {OUR_PUBKEY: {"peer_id": OUR_PUBKEY, "tier": "admin"}}
        self.plugin = MockPlugin()
        self.coord = LiquidityCoordinator(
            database=self.db,
            plugin=self.plugin,
            our_pubkey=OUR_PUBKEY,
            state_manager=MockStateManager()
        )

    def test_enriched_needs_stored_in_record(self):
        """record_member_liquidity_report stores enriched_needs."""
        enriched = [
            {"need_type": "outbound", "target_peer_id": "ext1",
             "amount_sats": 50000, "urgency": "high",
             "flow_state": "source", "flow_ratio": 0.8}
        ]
        result = self.coord.record_member_liquidity_report(
            member_id=OUR_PUBKEY,
            depleted_channels=[],
            saturated_channels=[],
            enriched_needs=enriched
        )
        assert result["status"] == "recorded"
        state = self.coord._member_liquidity_state[OUR_PUBKEY]
        assert "enriched_needs" in state
        assert len(state["enriched_needs"]) == 1
        assert state["enriched_needs"][0]["flow_state"] == "source"

    def test_enriched_needs_bounded_to_10(self):
        """Enriched needs should be capped at 10 entries."""
        enriched = [
            {"need_type": "outbound", "target_peer_id": f"ext{i}",
             "amount_sats": 50000, "urgency": "high"}
            for i in range(20)
        ]
        self.coord.record_member_liquidity_report(
            member_id=OUR_PUBKEY,
            depleted_channels=[],
            saturated_channels=[],
            enriched_needs=enriched
        )
        state = self.coord._member_liquidity_state[OUR_PUBKEY]
        assert len(state["enriched_needs"]) == 10

    def test_assess_our_liquidity_needs_prefers_enriched(self):
        """assess_our_liquidity_needs returns enriched needs when available."""
        enriched = [
            {"need_type": "outbound", "target_peer_id": "ext1",
             "amount_sats": 50000, "urgency": "high",
             "flow_state": "source"}
        ]
        self.coord.record_member_liquidity_report(
            member_id=OUR_PUBKEY,
            depleted_channels=[],
            saturated_channels=[],
            enriched_needs=enriched
        )

        # Even with funds that would produce different raw needs,
        # enriched needs should be returned
        funds = {"channels": [
            {"state": "CHANNELD_NORMAL", "peer_id": "ext99",
             "amount_msat": 10000000000, "our_amount_msat": 500000000}
        ]}
        needs = self.coord.assess_our_liquidity_needs(funds)
        assert len(needs) == 1
        assert needs[0]["flow_state"] == "source"

    def test_assess_falls_back_to_raw_without_enriched(self):
        """Without enriched needs, raw threshold assessment is used."""
        funds = {"channels": [
            {"state": "CHANNELD_NORMAL", "peer_id": "ext1",
             "amount_msat": 10000000000, "our_amount_msat": 500000000}
        ]}
        needs = self.coord.assess_our_liquidity_needs(funds)
        # 500M / 10B = 5% local — below 20% threshold
        assert len(needs) == 1
        assert needs[0]["need_type"] == NEED_OUTBOUND

    def test_enriched_needs_not_stored_when_none(self):
        """No enriched_needs key when param is None."""
        self.coord.record_member_liquidity_report(
            member_id=OUR_PUBKEY,
            depleted_channels=[],
            saturated_channels=[]
        )
        state = self.coord._member_liquidity_state[OUR_PUBKEY]
        assert "enriched_needs" not in state
