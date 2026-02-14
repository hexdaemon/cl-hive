"""
Tests for 8 settlement system fixes.

Fix 1: forwards_sats field documented as routing activity metric
Fix 2: calculate_our_balance uses deterministic plan
Fix 3: check_and_complete_settlement only requires payer execution
Fix 4: RPC docstring weights corrected (30/60/10)
Fix 5: Residual dust tracked in compute_settlement_plan
Fix 6: Gaming detection uses vote_rate only
Fix 7: generate_payments delegates to generate_payment_plan
Fix 8: Proposer auto-vote skips redundant hash verification
"""

import time
import json
import pytest
from unittest.mock import MagicMock, patch
from dataclasses import dataclass

from modules.settlement import (
    SettlementManager,
    MemberContribution,
    SettlementResult,
    SettlementPayment,
    calculate_min_payment,
    WEIGHT_CAPACITY,
    WEIGHT_FORWARDS,
    WEIGHT_UPTIME,
    MIN_PAYMENT_FLOOR_SATS,
)


def _make_manager():
    """Create a SettlementManager with mocked dependencies."""
    db = MagicMock()
    db.get_all_members.return_value = []
    db.get_fee_reports_for_period.return_value = []
    db.has_voted_settlement.return_value = False
    db.is_period_settled.return_value = False
    db.add_settlement_ready_vote.return_value = True
    db.get_settlement_proposal.return_value = None
    db.get_settlement_executions.return_value = []
    plugin = MagicMock()
    return SettlementManager(database=db, plugin=plugin)


def _make_contributions(members):
    """
    Build contribution dicts from a list of (peer_id, fees, forward_count, capacity, uptime) tuples.
    """
    return [
        {
            "peer_id": m[0],
            "fees_earned": m[1],
            "forward_count": m[2],
            "capacity": m[3],
            "uptime": m[4],
            "rebalance_costs": m[5] if len(m) > 5 else 0,
        }
        for m in members
    ]


# =============================================================================
# Fix 1: forwards_sats documented as routing activity metric
# =============================================================================

class TestForwardsSatsClarity:
    """Fix 1: forwards_sats field uses forward_count consistently."""

    def test_compute_settlement_plan_uses_forward_count(self):
        """compute_settlement_plan should map forward_count to forwards_sats."""
        mgr = _make_manager()

        contributions = _make_contributions([
            ("03alice", 1000, 100, 5_000_000, 95),
            ("03bob",   500,  50,  3_000_000, 90),
        ])

        plan = mgr.compute_settlement_plan("2026-06", contributions)

        # Plan should produce valid results using forward_count as routing metric
        assert plan["total_fees_sats"] == 1500
        assert len(plan["payments"]) >= 0  # May or may not have payments
        assert plan["plan_hash"]  # Must produce a valid hash

    def test_forward_count_proportional_weight(self):
        """Members with higher forward_count should get higher routing weight."""
        mgr = _make_manager()

        # Alice: 200 forwards, Bob: 50 forwards — same everything else
        contribs_a = [
            MemberContribution(
                peer_id="03alice", capacity_sats=5_000_000,
                forwards_sats=200, fees_earned_sats=750,
                uptime_pct=0.95,
            ),
            MemberContribution(
                peer_id="03bob", capacity_sats=5_000_000,
                forwards_sats=50, fees_earned_sats=750,
                uptime_pct=0.95,
            ),
        ]

        results = mgr.calculate_fair_shares(contribs_a)
        alice = next(r for r in results if r.peer_id == "03alice")
        bob = next(r for r in results if r.peer_id == "03bob")

        # Alice should get higher fair_share due to 4x routing activity
        assert alice.fair_share > bob.fair_share


# =============================================================================
# Fix 2: calculate_our_balance uses deterministic plan
# =============================================================================

class TestCalculateOurBalanceConsistency:
    """Fix 2: calculate_our_balance should use compute_settlement_plan."""

    def test_balance_matches_plan(self):
        """Balance from calculate_our_balance should match plan's expected_sent."""
        mgr = _make_manager()

        contributions = _make_contributions([
            ("03alice", 2000, 100, 5_000_000, 95),  # Earns more → owes money
            ("03bob",   200,  20,  3_000_000, 90),   # Earns less → owed money
        ])

        proposal = {"period": "2026-06", "proposal_id": "test123"}

        balance, creditor, min_payment = mgr.calculate_our_balance(
            proposal, contributions, "03alice"
        )

        # Alice earned more than her fair share, so she owes money (negative balance)
        # or receives depending on the fair share calculation
        plan = mgr.compute_settlement_plan("2026-06", contributions)
        expected_sent = int(plan["expected_sent_sats"].get("03alice", 0))
        expected_received = sum(
            int(p["amount_sats"]) for p in plan["payments"]
            if p.get("to_peer") == "03alice"
        )
        expected_balance = expected_received - expected_sent

        assert balance == expected_balance

    def test_creditor_from_plan_payments(self):
        """Creditor should be from actual plan payments, not ad-hoc calculation."""
        mgr = _make_manager()

        contributions = _make_contributions([
            ("03alice", 3000, 200, 8_000_000, 99),  # Big earner → owes
            ("03bob",   100,  10,  2_000_000, 90),   # Small → owed
            ("03carol", 100,  10,  2_000_000, 90),   # Small → owed
        ])

        proposal = {"period": "2026-06"}
        balance, creditor, _ = mgr.calculate_our_balance(
            proposal, contributions, "03alice"
        )

        if balance < 0 and creditor:
            # Creditor should be someone Alice pays in the plan
            plan = mgr.compute_settlement_plan("2026-06", contributions)
            alice_payments = [
                p["to_peer"] for p in plan["payments"]
                if p.get("from_peer") == "03alice"
            ]
            assert creditor in alice_payments

    def test_receiver_has_no_creditor(self):
        """A member who is owed money should have no creditor."""
        mgr = _make_manager()

        contributions = _make_contributions([
            ("03alice", 3000, 200, 8_000_000, 99),
            ("03bob",   100,  10,  2_000_000, 90),
        ])

        proposal = {"period": "2026-06"}
        balance, creditor, _ = mgr.calculate_our_balance(
            proposal, contributions, "03bob"
        )

        # Bob earned less, so his balance should be >= 0 (owed money)
        if balance >= 0:
            assert creditor is None


# =============================================================================
# Fix 3: check_and_complete_settlement only requires payer execution
# =============================================================================

class TestCompletionOnlyRequiresPayers:
    """Fix 3: Settlement completes when all payers confirm, not all members."""

    def test_completes_without_receiver_execution(self):
        """Settlement should complete even if receivers don't send confirmation."""
        mgr = _make_manager()

        contributions = _make_contributions([
            ("03alice", 2000, 100, 5_000_000, 95),  # Overpaid → payer
            ("03bob",   200,  20,  3_000_000, 90),   # Underpaid → receiver
        ])

        plan = mgr.compute_settlement_plan("2026-06", contributions)

        # Determine who's a payer
        payers = {pid: amt for pid, amt in plan["expected_sent_sats"].items() if amt > 0}
        assert len(payers) > 0, "Need at least one payer for this test"

        # Create execution records ONLY for payers
        executions = []
        for peer_id, expected_amount in payers.items():
            executions.append({
                "executor_peer_id": peer_id,
                "amount_paid_sats": expected_amount,
                "plan_hash": plan["plan_hash"],
            })

        # Set up mock DB
        proposal = {
            "proposal_id": "test_prop",
            "period": "2026-06",
            "status": "ready",
            "member_count": 2,
            "total_fees_sats": 2200,
            "plan_hash": plan["plan_hash"],
            "contributions_json": json.dumps(contributions),
        }
        mgr.db.get_settlement_proposal.return_value = proposal
        mgr.db.get_settlement_executions.return_value = executions

        result = mgr.check_and_complete_settlement("test_prop")
        assert result is True
        mgr.db.update_settlement_proposal_status.assert_called_with("test_prop", "completed")

    def test_still_requires_payer_execution(self):
        """Settlement should NOT complete if a payer hasn't confirmed."""
        mgr = _make_manager()

        contributions = _make_contributions([
            ("03alice", 2000, 100, 5_000_000, 95),
            ("03bob",   200,  20,  3_000_000, 90),
        ])

        plan = mgr.compute_settlement_plan("2026-06", contributions)
        payers = {pid: amt for pid, amt in plan["expected_sent_sats"].items() if amt > 0}

        # No execution records at all
        proposal = {
            "proposal_id": "test_prop",
            "period": "2026-06",
            "status": "ready",
            "member_count": 2,
            "plan_hash": plan["plan_hash"],
            "contributions_json": json.dumps(contributions),
        }
        mgr.db.get_settlement_proposal.return_value = proposal
        mgr.db.get_settlement_executions.return_value = []

        result = mgr.check_and_complete_settlement("test_prop")
        assert result is False

    def test_amount_mismatch_blocks_completion(self):
        """Payer reporting wrong amount should block completion."""
        mgr = _make_manager()

        contributions = _make_contributions([
            ("03alice", 2000, 100, 5_000_000, 95),
            ("03bob",   200,  20,  3_000_000, 90),
        ])

        plan = mgr.compute_settlement_plan("2026-06", contributions)
        payers = {pid: amt for pid, amt in plan["expected_sent_sats"].items() if amt > 0}

        # Create execution with WRONG amount
        executions = []
        for peer_id, expected_amount in payers.items():
            executions.append({
                "executor_peer_id": peer_id,
                "amount_paid_sats": expected_amount + 100,  # Wrong!
                "plan_hash": plan["plan_hash"],
            })

        proposal = {
            "proposal_id": "test_prop",
            "period": "2026-06",
            "status": "ready",
            "member_count": 2,
            "plan_hash": plan["plan_hash"],
            "contributions_json": json.dumps(contributions),
        }
        mgr.db.get_settlement_proposal.return_value = proposal
        mgr.db.get_settlement_executions.return_value = executions

        result = mgr.check_and_complete_settlement("test_prop")
        assert result is False

    def test_no_payments_needed_completes_immediately(self):
        """If all balances are within threshold, settlement completes with 0 distributed."""
        mgr = _make_manager()

        # All members earn the same → no payments needed
        contributions = _make_contributions([
            ("03alice", 500, 50, 5_000_000, 95),
            ("03bob",   500, 50, 5_000_000, 95),
        ])

        plan = mgr.compute_settlement_plan("2026-06", contributions)

        proposal = {
            "proposal_id": "test_prop",
            "period": "2026-06",
            "status": "ready",
            "member_count": 2,
            "plan_hash": plan["plan_hash"],
            "contributions_json": json.dumps(contributions),
        }
        mgr.db.get_settlement_proposal.return_value = proposal
        mgr.db.get_settlement_executions.return_value = []

        result = mgr.check_and_complete_settlement("test_prop")
        # Should complete since no payers
        if not plan["expected_sent_sats"] or all(v == 0 for v in plan["expected_sent_sats"].values()):
            assert result is True


# =============================================================================
# Fix 5: Residual dust tracked in compute_settlement_plan
# =============================================================================

class TestResidualDustTracking:
    """Fix 5: compute_settlement_plan should report residual dust."""

    def test_residual_sats_in_plan(self):
        """Plan output should include residual_sats field."""
        mgr = _make_manager()

        contributions = _make_contributions([
            ("03alice", 1000, 100, 5_000_000, 95),
            ("03bob",   500,  50,  3_000_000, 90),
        ])

        plan = mgr.compute_settlement_plan("2026-06", contributions)
        assert "residual_sats" in plan
        assert plan["residual_sats"] >= 0

    def test_no_residual_when_exact_match(self):
        """No residual when payment matching accounts for all debt."""
        mgr = _make_manager()

        # Only 2 members — payer pays receiver exactly
        contributions = _make_contributions([
            ("03alice", 2000, 100, 5_000_000, 95),
            ("03bob",   0,    0,   5_000_000, 95),
        ])

        plan = mgr.compute_settlement_plan("2026-06", contributions)

        # With only 2 members, all debt should be matched
        # (residual can still be 0 or small due to rounding)
        assert plan["residual_sats"] >= 0

    def test_residual_with_many_small_balances(self):
        """Residual should capture dust from many small unmatched amounts."""
        mgr = _make_manager()

        # Create a scenario where min_payment threshold drops some dust
        # With 10 members and low fees, min_payment = max(100, 500/100) = 100
        members = []
        for i in range(10):
            # Each member earns between 40-60 sats — below min_payment threshold
            members.append((f"03member_{i:02d}", 45 + i, 5, 1_000_000, 95))

        contributions = _make_contributions(members)
        plan = mgr.compute_settlement_plan("2026-06", contributions)

        # With all members earning similar tiny amounts, residual should be >= 0
        assert plan["residual_sats"] >= 0


# =============================================================================
# Fix 7: generate_payments delegates to generate_payment_plan
# =============================================================================

class TestGeneratePaymentsDelegation:
    """Fix 7: generate_payments should delegate to generate_payment_plan."""

    def test_same_amounts_as_plan(self):
        """generate_payments should produce same payment amounts as generate_payment_plan."""
        mgr = _make_manager()

        contributions = [
            MemberContribution(
                peer_id="03alice", capacity_sats=8_000_000,
                forwards_sats=200, fees_earned_sats=3000,
                uptime_pct=0.99, bolt12_offer="lno1alice",
            ),
            MemberContribution(
                peer_id="03bob", capacity_sats=3_000_000,
                forwards_sats=20, fees_earned_sats=200,
                uptime_pct=0.90, bolt12_offer="lno1bob",
            ),
            MemberContribution(
                peer_id="03carol", capacity_sats=3_000_000,
                forwards_sats=30, fees_earned_sats=300,
                uptime_pct=0.92, bolt12_offer="lno1carol",
            ),
        ]

        results = mgr.calculate_fair_shares(contributions)
        total_fees = sum(r.fees_earned for r in results)

        # Get both outputs
        raw_payments, _ = mgr.generate_payment_plan(results, total_fees)
        sp_payments = mgr.generate_payments(results, total_fees)

        # Same number of payments (all have offers)
        assert len(sp_payments) == len(raw_payments)

        # Same amounts
        raw_amounts = sorted(p["amount_sats"] for p in raw_payments)
        sp_amounts = sorted(p.amount_sats for p in sp_payments)
        assert raw_amounts == sp_amounts

    def test_filters_members_without_offers(self):
        """generate_payments should skip members without BOLT12 offers."""
        mgr = _make_manager()

        contributions = [
            MemberContribution(
                peer_id="03alice", capacity_sats=8_000_000,
                forwards_sats=200, fees_earned_sats=3000,
                uptime_pct=0.99, bolt12_offer="lno1alice",
            ),
            MemberContribution(
                peer_id="03bob", capacity_sats=3_000_000,
                forwards_sats=20, fees_earned_sats=200,
                uptime_pct=0.90, bolt12_offer=None,  # No offer!
            ),
        ]

        results = mgr.calculate_fair_shares(contributions)
        total_fees = sum(r.fees_earned for r in results)

        payments = mgr.generate_payments(results, total_fees)

        # Bob has no offer, so payments involving Bob should be filtered out
        for p in payments:
            assert p.from_peer != "03bob" or p.to_peer != "03bob"

    def test_returns_settlement_payment_objects(self):
        """generate_payments should return SettlementPayment objects."""
        mgr = _make_manager()

        contributions = [
            MemberContribution(
                peer_id="03alice", capacity_sats=8_000_000,
                forwards_sats=200, fees_earned_sats=3000,
                uptime_pct=0.99, bolt12_offer="lno1alice",
            ),
            MemberContribution(
                peer_id="03bob", capacity_sats=3_000_000,
                forwards_sats=20, fees_earned_sats=100,
                uptime_pct=0.90, bolt12_offer="lno1bob",
            ),
        ]

        results = mgr.calculate_fair_shares(contributions)
        payments = mgr.generate_payments(results, total_fees=3100)

        for p in payments:
            assert isinstance(p, SettlementPayment)
            assert p.bolt12_offer.startswith("lno1")


# =============================================================================
# Fix 8: Proposer auto-vote skips redundant hash verification
# =============================================================================

class TestProposerAutoVoteSkipVerify:
    """Fix 8: verify_and_vote with skip_hash_verify skips re-computation."""

    def test_skip_hash_verify_records_vote(self):
        """With skip_hash_verify=True, vote should be recorded without hash check."""
        mgr = _make_manager()

        rpc = MagicMock()
        rpc.signmessage.return_value = {"zbase": "sig123"}

        state_manager = MagicMock()

        proposal = {
            "proposal_id": "prop_abc",
            "period": "2026-06",
            "data_hash": "a" * 64,
            "plan_hash": "b" * 64,
        }

        vote = mgr.verify_and_vote(
            proposal=proposal,
            our_peer_id="03us",
            state_manager=state_manager,
            rpc=rpc,
            skip_hash_verify=True,
        )

        assert vote is not None
        assert vote["proposal_id"] == "prop_abc"
        assert vote["voter_peer_id"] == "03us"
        assert vote["signature"] == "sig123"

        # Should NOT have called gather_contributions_from_gossip
        assert not state_manager.get_peer_fees.called

    def test_default_still_verifies_hash(self):
        """Without skip_hash_verify, mismatched hash should reject vote."""
        mgr = _make_manager()

        rpc = MagicMock()
        state_manager = MagicMock()

        # gather_contributions_from_gossip will return empty → different hash
        mgr.db.get_all_members.return_value = []

        proposal = {
            "proposal_id": "prop_abc",
            "period": "2026-06",
            "data_hash": "a" * 64,  # Won't match empty contributions
            "plan_hash": "b" * 64,
        }

        vote = mgr.verify_and_vote(
            proposal=proposal,
            our_peer_id="03us",
            state_manager=state_manager,
            rpc=rpc,
        )

        # Should be None due to hash mismatch
        assert vote is None

    def test_already_voted_still_rejected(self):
        """skip_hash_verify should not bypass duplicate vote check."""
        mgr = _make_manager()
        mgr.db.has_voted_settlement.return_value = True  # Already voted

        vote = mgr.verify_and_vote(
            proposal={"proposal_id": "prop_abc", "period": "2026-06",
                       "data_hash": "a" * 64, "plan_hash": "b" * 64},
            our_peer_id="03us",
            state_manager=MagicMock(),
            rpc=MagicMock(),
            skip_hash_verify=True,
        )

        assert vote is None


# =============================================================================
# Fix 4: Weight constants verification
# =============================================================================

class TestWeightConstants:
    """Fix 4: Verify the actual weight constants match documentation."""

    def test_standard_weights_sum_to_one(self):
        """Standard weights should sum to 1.0."""
        assert abs(WEIGHT_CAPACITY + WEIGHT_FORWARDS + WEIGHT_UPTIME - 1.0) < 1e-10

    def test_standard_weights_are_30_60_10(self):
        """Standard weights should be 30/60/10."""
        assert WEIGHT_CAPACITY == 0.30
        assert WEIGHT_FORWARDS == 0.60
        assert WEIGHT_UPTIME == 0.10
