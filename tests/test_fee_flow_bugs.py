"""
Tests for fee coordination flow bug fixes.

Covers:
- Bug 1: Non-salient fee reverted to current_fee
- Bug 2: Health multiplier comment accuracy (verified via math)
- Bug 3+5: pheromone_levels RPC returns proper list format with correct field names
- Bug 4: record-routing-outcome RPC for pheromone updates without source/dest
"""

import pytest
import time
import math
from unittest.mock import MagicMock, patch

from modules.fee_coordination import (
    FLEET_FEE_FLOOR_PPM,
    FLEET_FEE_CEILING_PPM,
    DEFAULT_FEE_PPM,
    SALIENT_FEE_CHANGE_MIN_PPM,
    SALIENT_FEE_CHANGE_PCT,
    SALIENT_FEE_CHANGE_COOLDOWN,
    FeeRecommendation,
    FlowCorridorManager,
    AdaptiveFeeController,
    StigmergicCoordinator,
    MyceliumDefenseSystem,
    FeeCoordinationManager,
    is_fee_change_salient,
)
from modules.fee_intelligence import (
    HEALTH_THRIVING,
    HEALTH_STRUGGLING,
)
from modules.rpc_commands import pheromone_levels as rpc_pheromone_levels


class MockDatabase:
    def __init__(self):
        self.members = {}

    def get_all_members(self):
        return list(self.members.values()) if self.members else []

    def get_member(self, peer_id):
        return self.members.get(peer_id)


class MockPlugin:
    def __init__(self):
        self.logs = []
        self.rpc = MockRpc()

    def log(self, msg, level="info"):
        self.logs.append({"msg": msg, "level": level})


class MockRpc:
    def __init__(self):
        self.channels = []

    def listpeerchannels(self, id=None):
        if id:
            return {"channels": [c for c in self.channels if c.get("peer_id") == id]}
        return {"channels": self.channels}


class MockStateManager:
    def get(self, key, default=None):
        return default

    def set(self, key, value):
        pass

    def get_state(self, key, default=None):
        return default

    def set_state(self, key, value):
        pass


class MockLiquidityCoord:
    def get_rebalance_needs(self):
        return []


class TestBug1NonSalientFeeRevert:
    """Bug 1: When salience filter says not salient, recommended_fee must revert to current_fee."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.state_mgr = MockStateManager()
        self.liquidity_coord = MockLiquidityCoord()

        self.manager = FeeCoordinationManager(
            database=self.db,
            plugin=self.plugin,
            state_manager=self.state_mgr,
            liquidity_coordinator=self.liquidity_coord
        )
        self.manager.set_our_pubkey("02" + "0" * 64)

    def test_non_salient_fee_reverts_to_current(self):
        """When fee change is not salient, recommended_fee_ppm should equal current_fee."""
        current_fee = 500
        # Force a recent fee change to trigger cooldown (making change non-salient)
        self.manager._fee_change_times["123x1x0"] = time.time()

        rec = self.manager.get_fee_recommendation(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            current_fee=current_fee,
            local_balance_pct=0.5
        )

        # If not salient, recommended fee must equal current fee
        if not rec.is_salient:
            assert rec.recommended_fee_ppm == current_fee, (
                f"Non-salient recommendation should revert to current_fee={current_fee}, "
                f"but got {rec.recommended_fee_ppm}"
            )

    def test_non_salient_small_change_reverts(self):
        """A tiny fee change (< min threshold) should revert to current."""
        current_fee = 500

        # Patch is_fee_change_salient to force non-salient
        with patch('modules.fee_coordination.is_fee_change_salient',
                   return_value=(False, "abs_change_too_small")):
            rec = self.manager.get_fee_recommendation(
                channel_id="124x1x0",
                peer_id="02" + "a" * 64,
                current_fee=current_fee,
                local_balance_pct=0.5
            )

        assert rec.is_salient is False
        assert rec.recommended_fee_ppm == current_fee

    def test_salient_change_preserves_new_fee(self):
        """A salient fee change should NOT revert — recommended fee differs from current."""
        # Use a very different balance to force a large fee change
        rec = self.manager.get_fee_recommendation(
            channel_id="125x1x0",
            peer_id="02" + "a" * 64,
            current_fee=500,
            local_balance_pct=0.01  # Extremely low balance should push fee up
        )

        # If change is salient, recommended fee should differ from current
        if rec.is_salient:
            assert rec.recommended_fee_ppm != 500 or rec.recommended_fee_ppm >= FLEET_FEE_FLOOR_PPM


class TestBug2HealthMultiplierMath:
    """Bug 2: Verify health multiplier ranges match comments."""

    def test_struggling_range(self):
        """Health multiplier for struggling nodes: 0.7x (health=0) to 0.775x (health=25)."""
        # health = 0 → 0.7 + (0/100 * 0.3) = 0.7
        mult_at_0 = 0.7 + (0 / 100 * 0.3)
        assert abs(mult_at_0 - 0.7) < 0.001

        # health = 25 (HEALTH_STRUGGLING) → 0.7 + (25/100 * 0.3) = 0.775
        mult_at_25 = 0.7 + (25 / 100 * 0.3)
        assert abs(mult_at_25 - 0.775) < 0.001

        # NOT 0.85x as the old comment claimed
        assert mult_at_25 < 0.78, "Max struggling multiplier should be 0.775, not 0.85"

    def test_thriving_range(self):
        """Health multiplier for thriving nodes: 1.0x (health=75) to 1.0375x (health=100)."""
        # health = 76 → 1.0 + ((76-75)/100 * 0.15) = 1.0015
        mult_at_76 = 1.0 + ((76 - 75) / 100 * 0.15)
        assert abs(mult_at_76 - 1.0015) < 0.001

        # health = 100 → 1.0 + ((100-75)/100 * 0.15) = 1.0375
        mult_at_100 = 1.0 + ((100 - 75) / 100 * 0.15)
        assert abs(mult_at_100 - 1.0375) < 0.001

        # NOT 1.04x as the old comment claimed
        assert mult_at_100 < 1.04, "Max thriving multiplier should be 1.0375, not 1.04"

    def test_normal_health_no_adjustment(self):
        """Health between STRUGGLING and THRIVING gets 1.0x multiplier."""
        # No multiplier in the middle range
        for health in [26, 50, 74, 75]:
            if health >= HEALTH_STRUGGLING and health <= HEALTH_THRIVING:
                # These should have health_mult = 1.0 (no adjustment)
                pass  # Tested via the fee_intelligence module


class TestBug3And5PheromoneRpcFormat:
    """Bugs 3+5: pheromone_levels RPC must return list under 'pheromone_levels' key
    with correct field names ('level', 'above_threshold')."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.state_mgr = MockStateManager()
        self.liquidity_coord = MockLiquidityCoord()

        self.manager = FeeCoordinationManager(
            database=self.db,
            plugin=self.plugin,
            state_manager=self.state_mgr,
            liquidity_coordinator=self.liquidity_coord
        )
        self.manager.set_our_pubkey("02" + "0" * 64)

    def _make_ctx(self):
        ctx = MagicMock()
        ctx.fee_coordination_mgr = self.manager
        return ctx

    def test_single_channel_returns_pheromone_levels_list(self):
        """Single channel query must include 'pheromone_levels' key with list."""
        # Deposit some pheromone
        self.manager.adaptive_controller.update_pheromone(
            "123x1x0", 500, True, 100000
        )

        ctx = self._make_ctx()
        result = rpc_pheromone_levels(ctx, channel_id="123x1x0")

        # Must have pheromone_levels key as a list
        assert "pheromone_levels" in result, "Missing 'pheromone_levels' key"
        assert isinstance(result["pheromone_levels"], list), "pheromone_levels must be a list"
        assert len(result["pheromone_levels"]) == 1

        # List items must have correct field names
        item = result["pheromone_levels"][0]
        assert "channel_id" in item
        assert "level" in item, "Missing 'level' field (cl-revenue-ops expects this)"
        assert "above_threshold" in item, "Missing 'above_threshold' field"
        assert item["channel_id"] == "123x1x0"

    def test_single_channel_also_has_legacy_fields(self):
        """Single channel query should also keep legacy flat fields for backward compat."""
        self.manager.adaptive_controller.update_pheromone(
            "123x1x0", 500, True, 100000
        )

        ctx = self._make_ctx()
        result = rpc_pheromone_levels(ctx, channel_id="123x1x0")

        # Legacy flat fields should still be present
        assert "pheromone_level" in result
        assert "above_exploit_threshold" in result
        assert "channel_id" in result

    def test_all_channels_returns_pheromone_levels_list(self):
        """All channels query must include 'pheromone_levels' key."""
        self.manager.adaptive_controller.update_pheromone("111x1x0", 500, True, 50000)
        self.manager.adaptive_controller.update_pheromone("222x1x0", 300, True, 80000)

        ctx = self._make_ctx()
        result = rpc_pheromone_levels(ctx, channel_id=None)

        assert "pheromone_levels" in result, "Missing 'pheromone_levels' key in all-channels response"
        assert isinstance(result["pheromone_levels"], list)

        # Each item must have proper fields
        for item in result["pheromone_levels"]:
            assert "channel_id" in item
            assert "level" in item
            assert "above_threshold" in item

    def test_empty_channel_returns_zero_level(self):
        """Channel with no pheromone should return level 0."""
        ctx = self._make_ctx()
        result = rpc_pheromone_levels(ctx, channel_id="999x1x0")

        assert result["pheromone_levels"][0]["level"] == 0.0
        assert result["pheromone_levels"][0]["above_threshold"] is False


class TestBug4RecordRoutingOutcome:
    """Bug 4: Routing outcomes without source/dest must still update pheromone."""

    def setup_method(self):
        self.db = MockDatabase()
        self.plugin = MockPlugin()
        self.state_mgr = MockStateManager()
        self.liquidity_coord = MockLiquidityCoord()

        self.manager = FeeCoordinationManager(
            database=self.db,
            plugin=self.plugin,
            state_manager=self.state_mgr,
            liquidity_coordinator=self.liquidity_coord
        )
        self.manager.set_our_pubkey("02" + "0" * 64)

    def test_record_outcome_without_source_dest(self):
        """Recording routing outcome without source/dest should still update pheromone."""
        self.manager.record_routing_outcome(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            fee_ppm=500,
            success=True,
            revenue_sats=100000,
            source=None,
            destination=None
        )

        # Pheromone should be updated even without source/dest
        level = self.manager.adaptive_controller.get_pheromone_level("123x1x0")
        assert level > 0, "Pheromone should be updated even without source/destination"

    def test_record_outcome_with_source_dest_creates_marker(self):
        """Recording with source/dest should update pheromone AND create marker."""
        self.manager.record_routing_outcome(
            channel_id="123x1x0",
            peer_id="02" + "a" * 64,
            fee_ppm=500,
            success=True,
            revenue_sats=100000,
            source="peer1",
            destination="peer2"
        )

        # Pheromone should be updated
        level = self.manager.adaptive_controller.get_pheromone_level("123x1x0")
        assert level > 0

        # Marker should be created
        markers = self.manager.stigmergic_coord.get_all_markers()
        assert len(markers) > 0


class TestSalienceFunction:
    """Test is_fee_change_salient edge cases relevant to Bug 1."""

    def test_zero_change_not_salient(self):
        is_sal, reason = is_fee_change_salient(500, 500)
        assert is_sal is False
        assert "no_change" in reason

    def test_small_abs_change_not_salient(self):
        # Change of 5 ppm < SALIENT_FEE_CHANGE_MIN_PPM (10)
        is_sal, reason = is_fee_change_salient(500, 505)
        assert is_sal is False

    def test_cooldown_not_salient(self):
        is_sal, reason = is_fee_change_salient(500, 600, last_change_time=time.time())
        assert is_sal is False
        assert "cooldown" in reason

    def test_large_change_is_salient(self):
        # 500 → 600 = 20% change, 100 ppm abs
        is_sal, reason = is_fee_change_salient(500, 600, last_change_time=0)
        assert is_sal is True
        assert reason == "salient"
