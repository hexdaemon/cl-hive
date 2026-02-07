"""
Settlement module for cl-hive

Implements BOLT12-based revenue settlement for hive fleet members.

Fair Share Algorithm:
- 30% weight: Capacity contribution (member_capacity / fleet_capacity)
- 60% weight: Routing contribution (member_forwards / fleet_forwards)
- 10% weight: Uptime contribution (member_uptime / fleet_uptime)

Settlement Flow:
1. Each member registers a BOLT12 offer for receiving payments
2. At settlement time, collect fees_earned from each member
3. Calculate fair_share for each member
4. Generate payment list (surplus members pay deficit members)
5. Execute payments via BOLT12

Thread Safety:
- Uses thread-local database connections via HiveDatabase pattern
"""

import time
import json
import sqlite3
import threading
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal, ROUND_DOWN

from . import network_metrics


# Settlement period (weekly)
SETTLEMENT_PERIOD_SECONDS = 7 * 24 * 60 * 60  # 1 week

# Minimum payment threshold floor (absolute minimum to avoid dust)
MIN_PAYMENT_FLOOR_SATS = 100

# Distributed settlement plan version. Bump when the deterministic payment-plan
# algorithm changes in a way that affects plan hashes.
DISTRIBUTED_SETTLEMENT_PLAN_VERSION = 2


def calculate_min_payment(total_fees: int, member_count: int) -> int:
    """
    Calculate dynamic minimum payment threshold.

    Formula: max(FLOOR, total_fees / (members * 10))

    This ensures:
    - Small fleets with small fees can still settle (100 sat floor)
    - Larger fleets don't spam tiny payments
    - Scales with fee volume

    Examples:
    - 307 sats, 2 members: max(100, 307/20) = 100 sats
    - 10000 sats, 5 members: max(100, 10000/50) = 200 sats
    - 100000 sats, 10 members: max(100, 100000/100) = 1000 sats
    """
    if member_count <= 0:
        return MIN_PAYMENT_FLOOR_SATS
    dynamic_min = total_fees // (member_count * 10)
    return max(MIN_PAYMENT_FLOOR_SATS, dynamic_min)

# Fair share weights (standard mode)
WEIGHT_CAPACITY = 0.30
WEIGHT_FORWARDS = 0.60
WEIGHT_UPTIME = 0.10

# Fair share weights (network-optimized mode - Use Case 6)
# Rewards members who contribute more to fleet connectivity
WEIGHT_CAPACITY_NETWORK = 0.25
WEIGHT_FORWARDS_NETWORK = 0.55
WEIGHT_UPTIME_NETWORK = 0.10
WEIGHT_NETWORK_POSITION = 0.10  # 10% for hive centrality contribution

# Network position calculation settings
HIGH_CENTRALITY_THRESHOLD = 0.7  # Members above this get full network bonus
MIN_CENTRALITY_FOR_BONUS = 0.3   # Members below this get no network bonus


@dataclass
class MemberContribution:
    """A member's contribution metrics for a settlement period."""
    peer_id: str
    capacity_sats: int
    forwards_sats: int
    fees_earned_sats: int
    uptime_pct: float
    bolt12_offer: Optional[str] = None
    # Rebalancing costs for net profit calculation (Issue #42)
    rebalance_costs_sats: int = 0
    # Network position metrics (Use Case 6)
    hive_centrality: float = 0.0
    rebalance_hub_score: float = 0.0

    @property
    def net_profit_sats(self) -> int:
        """Net profit capped at 0 (no negative contributions)."""
        return max(0, self.fees_earned_sats - self.rebalance_costs_sats)


@dataclass
class SettlementResult:
    """Result of settlement calculation for one member."""
    peer_id: str
    fees_earned: int
    fair_share: int
    balance: int  # positive = owed money, negative = owes money
    bolt12_offer: Optional[str] = None
    # Rebalancing costs for net profit settlement (Issue #42)
    rebalance_costs: int = 0
    net_profit: int = 0
    # Network position contribution (Use Case 6)
    network_score: float = 0.0
    network_bonus_sats: int = 0


@dataclass
class SettlementPayment:
    """A payment to execute in settlement."""
    from_peer: str
    to_peer: str
    amount_sats: int
    bolt12_offer: str
    status: str = "pending"
    payment_hash: Optional[str] = None
    error: Optional[str] = None


class SettlementManager:
    """
    Manages BOLT12-based revenue settlement for the hive fleet.

    Responsibilities:
    - BOLT12 offer registration for members
    - Fair share calculation based on contributions
    - Settlement payment generation and execution
    - Settlement history tracking
    """

    def __init__(self, database, plugin, rpc=None):
        """
        Initialize the settlement manager.

        Args:
            database: HiveDatabase instance for persistence
            plugin: Reference to the pyln Plugin for logging
            rpc: RPC interface for Lightning operations (optional)
        """
        self.db = database
        self.plugin = plugin
        self.rpc = rpc
        self._local = threading.local()

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        return self.db._get_connection()

    def initialize_tables(self):
        """Create settlement-related database tables."""
        conn = self._get_connection()

        # =====================================================================
        # SETTLEMENT OFFERS TABLE
        # =====================================================================
        # BOLT12 offers registered by each member for receiving payments
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settlement_offers (
                peer_id TEXT PRIMARY KEY,
                bolt12_offer TEXT NOT NULL,
                registered_at INTEGER NOT NULL,
                last_verified INTEGER,
                active INTEGER DEFAULT 1
            )
        """)

        # =====================================================================
        # SETTLEMENT PERIODS TABLE
        # =====================================================================
        # Record of each settlement period
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settlement_periods (
                period_id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time INTEGER NOT NULL,
                end_time INTEGER NOT NULL,
                status TEXT DEFAULT 'pending',
                total_fees_sats INTEGER DEFAULT 0,
                total_members INTEGER DEFAULT 0,
                settled_at INTEGER,
                metadata TEXT
            )
        """)

        # =====================================================================
        # SETTLEMENT CONTRIBUTIONS TABLE
        # =====================================================================
        # Per-member contributions for each settlement period
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settlement_contributions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id INTEGER NOT NULL,
                peer_id TEXT NOT NULL,
                capacity_sats INTEGER NOT NULL,
                forwards_sats INTEGER NOT NULL,
                fees_earned_sats INTEGER NOT NULL,
                uptime_pct REAL NOT NULL,
                fair_share_sats INTEGER NOT NULL,
                balance_sats INTEGER NOT NULL,
                rebalance_costs_sats INTEGER NOT NULL DEFAULT 0,
                net_profit_sats INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (period_id) REFERENCES settlement_periods(period_id),
                UNIQUE (period_id, peer_id)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_settlement_contrib_period
            ON settlement_contributions(period_id)
        """)
        # Add columns if upgrading from older schema (Issue #42: net profit settlement)
        try:
            conn.execute(
                "ALTER TABLE settlement_contributions ADD COLUMN rebalance_costs_sats INTEGER NOT NULL DEFAULT 0"
            )
        except Exception:
            pass  # Column already exists
        try:
            conn.execute(
                "ALTER TABLE settlement_contributions ADD COLUMN net_profit_sats INTEGER NOT NULL DEFAULT 0"
            )
        except Exception:
            pass  # Column already exists

        # =====================================================================
        # SETTLEMENT PAYMENTS TABLE
        # =====================================================================
        # Individual payment records
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settlement_payments (
                payment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_id INTEGER NOT NULL,
                from_peer_id TEXT NOT NULL,
                to_peer_id TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                bolt12_offer TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                payment_hash TEXT,
                paid_at INTEGER,
                error TEXT,
                FOREIGN KEY (period_id) REFERENCES settlement_periods(period_id)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_settlement_payments_period
            ON settlement_payments(period_id)
        """)

        self.plugin.log("Settlement tables initialized")

    # =========================================================================
    # BOLT12 OFFER MANAGEMENT
    # =========================================================================

    def register_offer(self, peer_id: str, bolt12_offer: str) -> Dict[str, Any]:
        """
        Register a BOLT12 offer for a member.

        Args:
            peer_id: Member's node public key
            bolt12_offer: BOLT12 offer string (lno1...)

        Returns:
            Dict with status and offer details
        """
        if not bolt12_offer.startswith("lno1"):
            return {"error": "Invalid BOLT12 offer format (must start with lno1)"}

        conn = self._get_connection()
        now = int(time.time())

        conn.execute("""
            INSERT INTO settlement_offers (peer_id, bolt12_offer, registered_at, active)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(peer_id) DO UPDATE SET
                bolt12_offer = excluded.bolt12_offer,
                registered_at = excluded.registered_at,
                active = 1
        """, (peer_id, bolt12_offer, now))

        self.plugin.log(f"Registered BOLT12 offer for {peer_id[:16]}...")

        return {
            "status": "registered",
            "peer_id": peer_id,
            "offer": bolt12_offer[:40] + "...",
            "registered_at": now
        }

    def get_offer(self, peer_id: str) -> Optional[str]:
        """Get the BOLT12 offer for a member."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT bolt12_offer FROM settlement_offers WHERE peer_id = ? AND active = 1",
            (peer_id,)
        ).fetchone()
        return row["bolt12_offer"] if row else None

    def list_offers(self) -> Dict[str, Any]:
        """List all registered BOLT12 offers."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT peer_id, bolt12_offer, registered_at, last_verified, active
            FROM settlement_offers
            ORDER BY registered_at DESC
        """).fetchall()
        return {"offers": [dict(row) for row in rows]}

    def deactivate_offer(self, peer_id: str) -> Dict[str, Any]:
        """Deactivate a member's BOLT12 offer."""
        conn = self._get_connection()
        conn.execute(
            "UPDATE settlement_offers SET active = 0 WHERE peer_id = ?",
            (peer_id,)
        )
        return {"status": "deactivated", "peer_id": peer_id}

    def generate_and_register_offer(self, peer_id: str) -> Dict[str, Any]:
        """
        Generate a BOLT12 offer and register it for settlement.

        This is called automatically when a node joins the hive to ensure
        they can participate in revenue settlement from the start.

        Args:
            peer_id: The member's node public key (must be our own pubkey)

        Returns:
            Dict with status, offer details, or error
        """
        if not self.rpc:
            return {"error": "No RPC interface available"}

        # Check if we already have an active offer
        existing = self.get_offer(peer_id)
        if existing:
            self.plugin.log(f"Settlement offer already registered for {peer_id[:16]}...")
            return {
                "status": "already_registered",
                "peer_id": peer_id,
                "offer": existing[:40] + "..."
            }

        try:
            # Generate BOLT12 offer using CLN's offer RPC
            # 'any' means any amount, description identifies purpose
            result = self.rpc.offer(
                amount="any",
                description="hive settlement"
            )

            if "bolt12" not in result:
                return {"error": "Failed to generate BOLT12 offer: no bolt12 in response"}

            bolt12_offer = result["bolt12"]

            # Register the offer
            reg_result = self.register_offer(peer_id, bolt12_offer)

            self.plugin.log(f"Auto-generated and registered settlement offer for {peer_id[:16]}...")

            return {
                "status": "generated_and_registered",
                "peer_id": peer_id,
                "offer": bolt12_offer[:40] + "...",
                "offer_id": result.get("offer_id")
            }

        except Exception as e:
            self.plugin.log(f"Failed to generate settlement offer: {e}", level='warn')
            return {"error": f"Failed to generate offer: {e}"}

    # =========================================================================
    # FAIR SHARE CALCULATION
    # =========================================================================

    def calculate_fair_shares(
        self,
        contributions: List[MemberContribution],
        network_optimized: bool = False
    ) -> List[SettlementResult]:
        """
        Calculate fair share for each member based on contributions.

        Standard Fair Share Algorithm (all scores normalized across fleet):
        - 30% weight: capacity_contribution = member_capacity / total_capacity
        - 60% weight: routing_contribution = member_forwards / total_forwards
        - 10% weight: uptime_contribution = member_uptime / total_uptime

        Network-Optimized Mode (Use Case 6):
        - 25% weight: capacity_contribution
        - 55% weight: routing_contribution
        - 10% weight: uptime_contribution
        - 10% weight: network_position = normalized hive centrality

        Network position rewards members who maintain better fleet connectivity,
        contributing to overall routing capability even if they don't earn direct fees.

        Each member's fair_share = total_net_profit * weighted_contribution_score
        Balance = fair_share - net_profit (what member keeps minus their fair share)
        - Positive balance = member is owed money
        - Negative balance = member owes money

        Issue #42: Settlement now uses NET PROFIT (fees - rebalance costs) instead of
        gross fees. This ensures members who spend heavily on rebalancing don't
        subsidize those who don't. Net profit is capped at 0 (no negative contributions).

        Args:
            contributions: List of member contributions
            network_optimized: If True, include network position in calculation

        Returns:
            List of settlement results with fair shares and balances
        """
        if not contributions:
            return []

        # Calculate totals - use net profit instead of gross fees (Issue #42)
        total_capacity = sum(c.capacity_sats for c in contributions)
        total_forwards = sum(c.forwards_sats for c in contributions)
        total_net_profit = sum(c.net_profit_sats for c in contributions)
        total_uptime = sum(c.uptime_pct for c in contributions)

        if total_net_profit == 0:
            return [
                SettlementResult(
                    peer_id=c.peer_id,
                    fees_earned=c.fees_earned_sats,
                    rebalance_costs=c.rebalance_costs_sats,
                    net_profit=c.net_profit_sats,
                    fair_share=0,
                    balance=0,
                    bolt12_offer=c.bolt12_offer
                )
                for c in contributions
            ]

        # Enrich contributions with network metrics if needed
        if network_optimized:
            contributions = self._enrich_with_network_metrics(contributions)

        # Calculate total network score for normalization
        total_network_score = sum(c.hive_centrality for c in contributions)

        # Step 1: compute unnormalized weighted contribution scores per member.
        raw_scores: Dict[str, float] = {}
        raw_network_component: Dict[str, float] = {}
        for member in contributions:
            capacity_score = (member.capacity_sats / total_capacity) if total_capacity > 0 else 0.0
            forwards_score = (member.forwards_sats / total_forwards) if total_forwards > 0 else 0.0
            uptime_score = (member.uptime_pct / total_uptime) if total_uptime > 0 else 0.0

            if network_optimized:
                network_score = (
                    (member.hive_centrality / total_network_score) if total_network_score > 0 else 0.0
                )
                if member.hive_centrality < MIN_CENTRALITY_FOR_BONUS:
                    network_score = 0.0

                base_component = (
                    WEIGHT_CAPACITY_NETWORK * capacity_score +
                    WEIGHT_FORWARDS_NETWORK * forwards_score +
                    WEIGHT_UPTIME_NETWORK * uptime_score
                )
                network_component = WEIGHT_NETWORK_POSITION * network_score
                score = base_component + network_component

                raw_scores[member.peer_id] = score
                raw_network_component[member.peer_id] = network_component
            else:
                score = (
                    WEIGHT_CAPACITY * capacity_score +
                    WEIGHT_FORWARDS * forwards_score +
                    WEIGHT_UPTIME * uptime_score
                )
                raw_scores[member.peer_id] = score
                raw_network_component[member.peer_id] = 0.0

        total_score = sum(raw_scores.values())
        if total_score <= 0:
            # Extremely defensive fallback: equal split.
            raw_scores = {m.peer_id: 1.0 for m in contributions}
            raw_network_component = {m.peer_id: 0.0 for m in contributions}
            total_score = float(len(contributions))

        # Step 2: normalize scores so they sum to 1.0 across the fleet.
        norm_scores: Dict[str, float] = {pid: (s / total_score) for pid, s in raw_scores.items()}

        # Step 3: allocate integer fair_shares that sum exactly to total_net_profit
        # using a largest-remainder method (deterministic tie-break by peer_id).
        ideals: Dict[str, float] = {pid: (total_net_profit * w) for pid, w in norm_scores.items()}
        floors: Dict[str, int] = {pid: int(v) for pid, v in ideals.items()}
        allocated = sum(floors.values())
        remainder = total_net_profit - allocated

        # Sort by fractional remainder desc, then peer_id asc for determinism.
        frac_order = sorted(
            ideals.keys(),
            key=lambda pid: (-(ideals[pid] - floors[pid]), pid)
        )
        for i in range(max(0, remainder)):
            floors[frac_order[i % len(frac_order)]] += 1

        # Step 4: build SettlementResult list
        results: List[SettlementResult] = []
        for member in sorted(contributions, key=lambda m: m.peer_id):
            fair_share = floors.get(member.peer_id, 0)
            member_net_profit = member.net_profit_sats
            balance = fair_share - member_net_profit

            network_component = raw_network_component.get(member.peer_id, 0.0)
            network_score = 0.0
            network_bonus_sats = 0
            if network_optimized and total_net_profit > 0 and total_score > 0:
                # Report network contribution as a proportion of total normalized score.
                network_score = round(network_component / total_score, 6)
                network_bonus_sats = int(total_net_profit * (network_component / total_score))

            results.append(SettlementResult(
                peer_id=member.peer_id,
                fees_earned=member.fees_earned_sats,
                rebalance_costs=member.rebalance_costs_sats,
                net_profit=member_net_profit,
                fair_share=fair_share,
                balance=balance,
                bolt12_offer=member.bolt12_offer,
                network_score=network_score,
                network_bonus_sats=network_bonus_sats
            ))

        # Accounting identity should now hold exactly.
        total_balance = sum(r.balance for r in results)
        if total_balance != 0:
            self.plugin.log(
                f"Warning: Settlement balance mismatch of {total_balance} sats",
                level='warn'
            )

        return results

    @staticmethod
    def _plan_hash(
        plan_version: int,
        period: str,
        data_hash: str,
        min_payment_sats: int,
        payments: List[Dict[str, Any]],
    ) -> str:
        import hashlib

        # Canonicalize payments ordering.
        canon_payments = sorted(
            payments,
            key=lambda p: (p.get("from_peer", ""), p.get("to_peer", ""), int(p.get("amount_sats", 0)))
        )
        payload = {
            "v": plan_version,
            "period": period,
            "data_hash": data_hash,
            "min_payment_sats": int(min_payment_sats),
            "payments": canon_payments,
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def generate_payment_plan(
        self,
        results: List[SettlementResult],
        total_fees: int,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """
        Deterministically generate a full settlement payment plan.

        Unlike generate_payments(), this does NOT filter based on BOLT12 offer
        availability. The plan is the authoritative expected transfers; offer
        availability is an execution-time concern.

        Returns:
            (payments, min_payment_sats)
        """
        member_count = len(results)
        min_payment = calculate_min_payment(total_fees, member_count)

        # Deterministic ordering, including tie-break by peer_id.
        payers = [r for r in results if r.balance < -min_payment]
        receivers = [r for r in results if r.balance > min_payment]
        payers.sort(key=lambda r: (r.balance, r.peer_id))
        receivers.sort(key=lambda r: (-r.balance, r.peer_id))

        payer_remaining = {p.peer_id: -p.balance for p in payers}
        receiver_remaining = {r.peer_id: r.balance for r in receivers}

        payments: List[Dict[str, Any]] = []
        for payer in payers:
            owing = payer_remaining.get(payer.peer_id, 0)
            if owing <= 0:
                continue
            for receiver in receivers:
                owed = receiver_remaining.get(receiver.peer_id, 0)
                if owed <= 0:
                    continue
                amount = min(owing, owed)
                if amount < min_payment:
                    continue
                payments.append(
                    {"from_peer": payer.peer_id, "to_peer": receiver.peer_id, "amount_sats": int(amount)}
                )
                owing -= amount
                owed -= amount
                payer_remaining[payer.peer_id] = owing
                receiver_remaining[receiver.peer_id] = owed
                if owing <= 0:
                    break

        return payments, min_payment

    def compute_settlement_plan(
        self,
        period: str,
        contributions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Compute a deterministic settlement plan (payments + hashes) from a canonical
        contributions snapshot.
        """
        member_contributions: List[MemberContribution] = []
        for c in contributions:
            uptime = c.get("uptime", 100)
            try:
                uptime_pct = float(uptime) / 100.0
            except Exception:
                uptime_pct = 1.0

            member_contributions.append(
                MemberContribution(
                    peer_id=c["peer_id"],
                    capacity_sats=int(c.get("capacity", 0)),
                    forwards_sats=int(c.get("forward_count", 0)),
                    fees_earned_sats=int(c.get("fees_earned", 0)),
                    rebalance_costs_sats=int(c.get("rebalance_costs", 0)),
                    uptime_pct=uptime_pct,
                )
            )

        data_hash = self.calculate_settlement_hash(period, contributions)
        results = self.calculate_fair_shares(member_contributions)
        total_fees = sum(int(c.get("fees_earned", 0)) for c in contributions)
        payments, min_payment = self.generate_payment_plan(results, total_fees=total_fees)
        plan_hash = self._plan_hash(
            plan_version=DISTRIBUTED_SETTLEMENT_PLAN_VERSION,
            period=period,
            data_hash=data_hash,
            min_payment_sats=min_payment,
            payments=payments,
        )

        expected_sent: Dict[str, int] = {}
        for p in payments:
            expected_sent[p["from_peer"]] = expected_sent.get(p["from_peer"], 0) + int(p["amount_sats"])

        return {
            "plan_version": DISTRIBUTED_SETTLEMENT_PLAN_VERSION,
            "period": period,
            "data_hash": data_hash,
            "plan_hash": plan_hash,
            "min_payment_sats": min_payment,
            "payments": payments,
            "expected_sent_sats": expected_sent,
            "total_fees_sats": total_fees,
        }

    def _enrich_with_network_metrics(
        self,
        contributions: List[MemberContribution]
    ) -> List[MemberContribution]:
        """
        Enrich member contributions with network position metrics.

        Fetches hive centrality and rebalance hub score for each member.

        Args:
            contributions: List of member contributions

        Returns:
            Updated list with network metrics populated
        """
        calculator = network_metrics.get_calculator()
        if not calculator:
            return contributions

        for contrib in contributions:
            metrics = calculator.get_member_metrics(contrib.peer_id)
            if metrics:
                contrib.hive_centrality = metrics.hive_centrality
                contrib.rebalance_hub_score = metrics.rebalance_hub_score

        return contributions

    # =========================================================================
    # PAYMENT GENERATION
    # =========================================================================

    def generate_payments(
        self,
        results: List[SettlementResult],
        total_fees: int = 0
    ) -> List[SettlementPayment]:
        """
        Generate payment list from settlement results.

        Matches members with negative balance (owe money) to members with
        positive balance (owed money) to create payment list.

        Args:
            results: List of settlement results
            total_fees: Total fees for dynamic minimum calculation

        Returns:
            List of payments to execute
        """
        # Calculate dynamic minimum payment threshold
        member_count = len(results)
        min_payment = calculate_min_payment(total_fees, member_count)

        # Separate into payers (owe money) and receivers (owed money)
        payers = [r for r in results if r.balance < -min_payment and r.bolt12_offer]
        receivers = [r for r in results if r.balance > min_payment and r.bolt12_offer]

        if not payers or not receivers:
            return []

        # Sort by absolute balance (largest first)
        payers.sort(key=lambda x: x.balance)  # Most negative first
        receivers.sort(key=lambda x: x.balance, reverse=True)  # Most positive first

        payments = []
        payer_remaining = {p.peer_id: -p.balance for p in payers}  # Amount they owe
        receiver_remaining = {r.peer_id: r.balance for r in receivers}  # Amount owed to them

        # Match payers to receivers
        for payer in payers:
            if payer_remaining[payer.peer_id] <= 0:
                continue

            for receiver in receivers:
                if receiver_remaining[receiver.peer_id] <= 0:
                    continue

                # Calculate payment amount
                amount = min(
                    payer_remaining[payer.peer_id],
                    receiver_remaining[receiver.peer_id]
                )

                if amount < min_payment:
                    continue

                payments.append(SettlementPayment(
                    from_peer=payer.peer_id,
                    to_peer=receiver.peer_id,
                    amount_sats=amount,
                    bolt12_offer=receiver.bolt12_offer
                ))

                payer_remaining[payer.peer_id] -= amount
                receiver_remaining[receiver.peer_id] -= amount

        return payments

    # =========================================================================
    # SETTLEMENT EXECUTION
    # =========================================================================

    def create_settlement_period(self) -> int:
        """Create a new settlement period record."""
        conn = self._get_connection()
        now = int(time.time())

        cursor = conn.execute("""
            INSERT INTO settlement_periods (start_time, end_time, status)
            VALUES (?, ?, 'pending')
        """, (now - SETTLEMENT_PERIOD_SECONDS, now))

        return cursor.lastrowid

    def record_contributions(
        self,
        period_id: int,
        results: List[SettlementResult],
        contributions: List[MemberContribution]
    ):
        """Record contributions and results for a settlement period."""
        conn = self._get_connection()

        # Create lookup for contributions
        contrib_map = {c.peer_id: c for c in contributions}

        total_fees = sum(r.fees_earned for r in results)

        for result in results:
            contrib = contrib_map.get(result.peer_id)
            if not contrib:
                continue

            conn.execute("""
                INSERT INTO settlement_contributions (
                    period_id, peer_id, capacity_sats, forwards_sats,
                    fees_earned_sats, uptime_pct, fair_share_sats, balance_sats,
                    rebalance_costs_sats, net_profit_sats
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                period_id,
                result.peer_id,
                contrib.capacity_sats,
                contrib.forwards_sats,
                result.fees_earned,
                contrib.uptime_pct,
                result.fair_share,
                result.balance,
                result.rebalance_costs,
                result.net_profit
            ))

        # Update period totals
        conn.execute("""
            UPDATE settlement_periods
            SET total_fees_sats = ?, total_members = ?
            WHERE period_id = ?
        """, (total_fees, len(results), period_id))

    def record_payments(self, period_id: int, payments: List[SettlementPayment]):
        """Record planned payments for a settlement period."""
        conn = self._get_connection()

        for payment in payments:
            conn.execute("""
                INSERT INTO settlement_payments (
                    period_id, from_peer_id, to_peer_id, amount_sats,
                    bolt12_offer, status
                ) VALUES (?, ?, ?, ?, ?, 'pending')
            """, (
                period_id,
                payment.from_peer,
                payment.to_peer,
                payment.amount_sats,
                payment.bolt12_offer
            ))

    async def execute_payment(self, payment: SettlementPayment) -> SettlementPayment:
        """
        Execute a single settlement payment via BOLT12.

        Args:
            payment: Payment to execute

        Returns:
            Updated payment with status and payment_hash
        """
        if not self.rpc:
            payment.status = "error"
            payment.error = "No RPC interface available"
            return payment

        try:
            # Use fetchinvoice to get invoice from BOLT12 offer
            invoice_result = self.rpc.fetchinvoice(
                offer=payment.bolt12_offer,
                amount_msat=f"{payment.amount_sats * 1000}msat"
            )

            if "invoice" not in invoice_result:
                payment.status = "error"
                payment.error = "Failed to fetch invoice from offer"
                return payment

            bolt12_invoice = invoice_result["invoice"]

            # Pay the invoice
            pay_result = self.rpc.pay(bolt12_invoice)

            if pay_result.get("status") == "complete":
                payment.status = "completed"
                payment.payment_hash = pay_result.get("payment_hash")
            else:
                payment.status = "error"
                payment.error = pay_result.get("message", "Payment failed")

        except Exception as e:
            payment.status = "error"
            payment.error = str(e)

        return payment

    def update_payment_status(
        self,
        period_id: int,
        from_peer: str,
        to_peer: str,
        status: str,
        payment_hash: Optional[str] = None,
        error: Optional[str] = None
    ):
        """Update payment status in database."""
        conn = self._get_connection()
        now = int(time.time())

        conn.execute("""
            UPDATE settlement_payments
            SET status = ?, payment_hash = ?, paid_at = ?, error = ?
            WHERE period_id = ? AND from_peer_id = ? AND to_peer_id = ?
        """, (status, payment_hash, now if status == "completed" else None, error,
              period_id, from_peer, to_peer))

    def complete_settlement_period(self, period_id: int):
        """Mark a settlement period as complete."""
        conn = self._get_connection()
        now = int(time.time())

        conn.execute("""
            UPDATE settlement_periods
            SET status = 'completed', settled_at = ?
            WHERE period_id = ?
        """, (now, period_id))

    # =========================================================================
    # REPORTING
    # =========================================================================

    def get_settlement_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent settlement periods."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT period_id, start_time, end_time, status,
                   total_fees_sats, total_members, settled_at
            FROM settlement_periods
            ORDER BY period_id DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(row) for row in rows]

    def get_period_details(self, period_id: int) -> Dict[str, Any]:
        """Get detailed information about a settlement period."""
        conn = self._get_connection()

        # Get period info
        period = conn.execute("""
            SELECT * FROM settlement_periods WHERE period_id = ?
        """, (period_id,)).fetchone()

        if not period:
            return {"error": "Period not found"}

        # Get contributions
        contributions = conn.execute("""
            SELECT * FROM settlement_contributions WHERE period_id = ?
        """, (period_id,)).fetchall()

        # Get payments
        payments = conn.execute("""
            SELECT * FROM settlement_payments WHERE period_id = ?
        """, (period_id,)).fetchall()

        return {
            "period": dict(period),
            "contributions": [dict(c) for c in contributions],
            "payments": [dict(p) for p in payments]
        }

    def get_member_settlement_history(
        self,
        peer_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get settlement history for a specific member."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT c.*, p.start_time, p.end_time, p.status as period_status
            FROM settlement_contributions c
            JOIN settlement_periods p ON c.period_id = p.period_id
            WHERE c.peer_id = ?
            ORDER BY c.period_id DESC
            LIMIT ?
        """, (peer_id, limit)).fetchall()
        return [dict(row) for row in rows]

    # =========================================================================
    # DISTRIBUTED SETTLEMENT (Phase 12)
    # =========================================================================

    @staticmethod
    def get_period_string(timestamp: Optional[int] = None) -> str:
        """
        Get the YYYY-WW period string for a given timestamp.

        Args:
            timestamp: Unix timestamp (defaults to now)

        Returns:
            Period string in YYYY-WW format (ISO week)
        """
        import datetime
        if timestamp is None:
            timestamp = int(time.time())
        dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        iso_year, iso_week, _ = dt.isocalendar()
        return f"{iso_year}-{iso_week:02d}"

    @staticmethod
    def get_previous_period() -> str:
        """Get the period string for the previous week."""
        import datetime
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        prev_week = now - datetime.timedelta(weeks=1)
        iso_year, iso_week, _ = prev_week.isocalendar()
        return f"{iso_year}-{iso_week:02d}"

    @staticmethod
    def calculate_settlement_hash(
        period: str,
        contributions: List[Dict[str, Any]]
    ) -> str:
        """
        Calculate the canonical hash for settlement data.

        This ensures all nodes calculate the same amounts by using
        a deterministic hash of the contribution data.

        Args:
            period: Settlement period (YYYY-WW)
            contributions: List of contribution dicts with peer_id, fees_earned, capacity, costs

        Returns:
            SHA256 hash (64 hex chars)
        """
        import hashlib

        # Sort contributions by peer_id for determinism
        sorted_contribs = sorted(contributions, key=lambda x: x.get('peer_id', ''))

        # Build canonical string - include costs for net profit settlement (Issue #42)
        canonical_parts = [period]
        for c in sorted_contribs:
            peer_id = c.get('peer_id', '')
            fees = c.get('fees_earned', 0)
            costs = c.get('rebalance_costs', 0)
            capacity = c.get('capacity', 0)
            uptime = c.get('uptime', 100)
            canonical_parts.append(f"{peer_id}:{fees}:{costs}:{capacity}:{uptime}")

        canonical_string = "|".join(canonical_parts)
        return hashlib.sha256(canonical_string.encode()).hexdigest()

    def gather_contributions_from_gossip(
        self,
        state_manager,
        period: str
    ) -> List[Dict[str, Any]]:
        """
        Gather contribution data from gossiped FEE_REPORT messages.

        This uses PERSISTED fee reports from the database (survives restarts),
        falling back to in-memory state_manager data if needed.

        Args:
            state_manager: HiveStateManager with gossiped fee data
            period: Settlement period (for filtering)

        Returns:
            List of contribution dicts with peer_id, fees_earned, rebalance_costs, capacity, uptime
        """
        contributions = []

        # Get all members
        all_members = self.db.get_all_members()

        # Get persisted fee reports for this period from database
        db_fee_reports = self.db.get_fee_reports_for_period(period)
        db_fees_by_peer = {r['peer_id']: r for r in db_fee_reports}

        for member in all_members:
            peer_id = member['peer_id']

            # First try database (persisted), then fall back to state manager (in-memory)
            if peer_id in db_fees_by_peer:
                db_report = db_fees_by_peer[peer_id]
                fees_earned = db_report.get('fees_earned_sats', 0)
                forward_count = db_report.get('forward_count', 0)
                rebalance_costs = db_report.get('rebalance_costs_sats', 0)
            else:
                # Fall back to in-memory state (may be from current session)
                fee_data = state_manager.get_peer_fees(peer_id)
                fees_earned = fee_data.get('fees_earned_sats', 0)
                forward_count = fee_data.get('forward_count', 0)
                rebalance_costs = fee_data.get('rebalance_costs_sats', 0)

            # Get capacity from state
            peer_state = state_manager.get_peer_state(peer_id)

            # Canonicalize uptime for hashing/settlement math.
            # hive_members.uptime_pct is stored as a fraction (0-1) by
            # HiveDatabase.sync_uptime_from_presence(); some older code paths
            # may store a percentage (0-100). Normalize to an integer percent.
            uptime_val = member.get('uptime_pct', 1.0)
            try:
                uptime_f = float(uptime_val)
                if uptime_f <= 1.0:
                    uptime_f *= 100.0
                uptime = int(round(max(0.0, min(100.0, uptime_f))))
            except Exception:
                uptime = 100

            contributions.append({
                'peer_id': peer_id,
                'fees_earned': fees_earned,
                'rebalance_costs': rebalance_costs,
                'capacity': peer_state.capacity_sats if peer_state else 0,
                'uptime': uptime,
                'forward_count': forward_count,
            })

        return contributions

    def create_proposal(
        self,
        period: str,
        our_peer_id: str,
        state_manager,
        rpc
    ) -> Optional[Dict[str, Any]]:
        """
        Create a settlement proposal for a given period.

        This gathers contribution data from gossiped FEE_REPORT messages,
        calculates the canonical hash, and creates the proposal.

        Args:
            period: Settlement period (YYYY-WW)
            our_peer_id: Our node's public key
            state_manager: HiveStateManager with gossiped fee data
            rpc: RPC proxy for signing

        Returns:
            Proposal dict if created, None if period already has proposal
        """
        import secrets

        # Check if period already has a proposal
        existing = self.db.get_settlement_proposal_by_period(period)
        if existing:
            self.plugin.log(
                f"Settlement proposal already exists for {period}",
                level='debug'
            )
            return None

        # Check if period is already settled
        if self.db.is_period_settled(period):
            self.plugin.log(
                f"Period {period} already settled",
                level='debug'
            )
            return None

        # Gather contribution data from gossip
        contributions = self.gather_contributions_from_gossip(state_manager, period)

        if not contributions:
            self.plugin.log("No contributions to settle", level='debug')
            return None

        # Calculate canonical hash + deterministic payment plan hash.
        plan = self.compute_settlement_plan(period, contributions)
        data_hash = plan["data_hash"]
        plan_hash = plan["plan_hash"]

        # Calculate totals
        total_fees = plan["total_fees_sats"]
        member_count = len(contributions)

        # Generate proposal ID
        proposal_id = secrets.token_hex(16)
        timestamp = int(time.time())

        # Create proposal in database (store contributions for rebroadcast - Issue #49)
        contributions_json = json.dumps(contributions)
        if not self.db.add_settlement_proposal(
            proposal_id=proposal_id,
            period=period,
            proposer_peer_id=our_peer_id,
            data_hash=data_hash,
            plan_hash=plan_hash,
            total_fees_sats=total_fees,
            member_count=member_count,
            contributions_json=contributions_json
        ):
            return None

        self.plugin.log(
            f"Created settlement proposal {proposal_id[:16]}... for {period}: "
            f"{total_fees} sats, {member_count} members"
        )

        return {
            'proposal_id': proposal_id,
            'period': period,
            'proposer_peer_id': our_peer_id,
            'data_hash': data_hash,
            'plan_hash': plan_hash,
            'total_fees_sats': total_fees,
            'member_count': member_count,
            'contributions': contributions,
            'timestamp': timestamp,
        }

    def verify_and_vote(
        self,
        proposal: Dict[str, Any],
        our_peer_id: str,
        state_manager,
        rpc
    ) -> Optional[Dict[str, Any]]:
        """
        Verify a settlement proposal's data hash and vote if it matches.

        This independently calculates the data hash from gossiped FEE_REPORT
        data and votes if it matches the proposal.

        Args:
            proposal: Proposal dict from SETTLEMENT_PROPOSE message
            our_peer_id: Our node's public key
            state_manager: HiveStateManager with gossiped fee data
            rpc: RPC proxy for signing

        Returns:
            Vote dict if vote cast, None if hash mismatch or already voted
        """
        proposal_id = proposal.get('proposal_id')
        period = proposal.get('period')
        proposed_hash = proposal.get('data_hash')
        proposed_plan_hash = proposal.get('plan_hash')

        # Check if we already voted
        if self.db.has_voted_settlement(proposal_id, our_peer_id):
            self.plugin.log(
                f"Already voted on proposal {proposal_id[:16]}...",
                level='debug'
            )
            return None

        # Check if period already settled
        if self.db.is_period_settled(period):
            self.plugin.log(
                f"Period {period} already settled, skipping vote",
                level='debug'
            )
            return None

        # Gather our own contribution data and calculate hashes.
        # We verify both the canonical data hash and the derived deterministic plan hash.
        our_contributions = self.gather_contributions_from_gossip(state_manager, period)
        our_plan = self.compute_settlement_plan(period, our_contributions)
        our_hash = our_plan["data_hash"]
        our_plan_hash = our_plan["plan_hash"]

        # Verify hash matches
        if our_hash != proposed_hash:
            self.plugin.log(
                f"Hash mismatch for proposal {proposal_id[:16]}...: "
                f"ours={our_hash[:16]}... theirs={proposed_hash[:16]}...",
                level='warn'
            )
            return None

        if not isinstance(proposed_plan_hash, str) or len(proposed_plan_hash) != 64:
            self.plugin.log(
                f"Missing/invalid plan_hash for proposal {proposal_id[:16]}...",
                level='warn'
            )
            return None

        if our_plan_hash != proposed_plan_hash:
            self.plugin.log(
                f"Plan hash mismatch for proposal {proposal_id[:16]}...: "
                f"ours={our_plan_hash[:16]}... theirs={proposed_plan_hash[:16]}...",
                level='warn'
            )
            return None

        timestamp = int(time.time())

        # Sign the vote
        from modules.protocol import get_settlement_ready_signing_payload
        vote_payload = {
            'proposal_id': proposal_id,
            'voter_peer_id': our_peer_id,
            'data_hash': our_hash,
            'timestamp': timestamp,
        }
        signing_payload = get_settlement_ready_signing_payload(vote_payload)

        try:
            sig_result = rpc.signmessage(signing_payload)
            signature = sig_result.get('zbase', '')
        except Exception as e:
            self.plugin.log(f"Failed to sign settlement vote: {e}", level='warn')
            return None

        # Record vote in database
        if not self.db.add_settlement_ready_vote(
            proposal_id=proposal_id,
            voter_peer_id=our_peer_id,
            data_hash=our_hash,
            signature=signature
        ):
            return None

        self.plugin.log(
            f"Voted on settlement proposal {proposal_id[:16]}... (hash verified)"
        )

        return {
            'proposal_id': proposal_id,
            'voter_peer_id': our_peer_id,
            'data_hash': our_hash,
            'timestamp': timestamp,
            'signature': signature,
        }

    def check_quorum_and_mark_ready(
        self,
        proposal_id: str,
        member_count: int
    ) -> bool:
        """
        Check if a proposal has reached quorum (51%) and mark it ready.

        Args:
            proposal_id: Proposal to check
            member_count: Total number of members in the proposal

        Returns:
            True if quorum reached and status updated
        """
        vote_count = self.db.count_settlement_ready_votes(proposal_id)
        quorum_needed = (member_count // 2) + 1

        if vote_count >= quorum_needed:
            proposal = self.db.get_settlement_proposal(proposal_id)
            if proposal and proposal.get('status') == 'pending':
                self.db.update_settlement_proposal_status(proposal_id, 'ready')
                self.plugin.log(
                    f"Settlement proposal {proposal_id[:16]}... reached quorum "
                    f"({vote_count}/{member_count})"
                )
                return True

        return False

    def calculate_our_balance(
        self,
        proposal: Dict[str, Any],
        contributions: List[Dict[str, Any]],
        our_peer_id: str
    ) -> Tuple[int, Optional[str], int]:
        """
        Calculate our balance in a settlement (positive = owed, negative = owe).

        Args:
            proposal: Proposal dict
            contributions: List of contribution dicts
            our_peer_id: Our node's public key

        Returns:
            Tuple of (balance_sats, creditor_peer_id or None, min_payment_threshold)
        """
        # Convert to MemberContribution objects
        member_contributions = [
            MemberContribution(
                peer_id=c['peer_id'],
                capacity_sats=c.get('capacity', 0),
                forwards_sats=c.get('forward_count', 0) * 100000,  # Estimate
                fees_earned_sats=c.get('fees_earned', 0),
                uptime_pct=c.get('uptime', 100),
            )
            for c in contributions
        ]

        # Calculate fair shares
        results = self.calculate_fair_shares(member_contributions)

        # Calculate dynamic minimum payment
        total_fees = sum(c.get('fees_earned', 0) for c in contributions)
        member_count = len(contributions)
        min_payment = calculate_min_payment(total_fees, member_count)

        # Find our result
        our_result = None
        for result in results:
            if result.peer_id == our_peer_id:
                our_result = result
                break

        if not our_result:
            return (0, None, min_payment)

        # If we owe money (negative balance), find who to pay
        if our_result.balance < -min_payment:
            # Find member with highest positive balance (most owed)
            creditors = [r for r in results if r.balance > min_payment]
            if creditors:
                creditors.sort(key=lambda x: x.balance, reverse=True)
                return (our_result.balance, creditors[0].peer_id, min_payment)

        return (our_result.balance, None, min_payment)

    async def execute_our_settlement(
        self,
        proposal: Dict[str, Any],
        contributions: List[Dict[str, Any]],
        our_peer_id: str,
        rpc
    ) -> Optional[Dict[str, Any]]:
        """
        Execute our settlement payment if we owe money.

        Args:
            proposal: Proposal dict
            contributions: List of contribution dicts
            our_peer_id: Our node's public key
            rpc: RPC proxy for payment

        Returns:
            Execution result dict if payment made, None otherwise
        """
        proposal_id = proposal.get('proposal_id')
        period = proposal.get('period')
        if not proposal_id or not period:
            return None

        # Check if already executed
        if self.db.has_executed_settlement(proposal_id, our_peer_id):
            self.plugin.log(
                f"Already executed settlement for {proposal_id[:16]}...",
                level='debug'
            )
            return None

        # Compute the authoritative plan from the proposal's canonical contributions snapshot.
        plan = self.compute_settlement_plan(period, contributions)
        expected_plan_hash = proposal.get("plan_hash")
        if isinstance(expected_plan_hash, str) and len(expected_plan_hash) == 64:
            if plan["plan_hash"] != expected_plan_hash:
                self.plugin.log(
                    f"SETTLEMENT: Refusing to execute {proposal_id[:16]}... "
                    f"(plan hash mismatch ours={plan['plan_hash'][:16]}... "
                    f"theirs={expected_plan_hash[:16]}...)",
                    level="warn"
                )
                return None

        expected_sent = int(plan["expected_sent_sats"].get(our_peer_id, 0))
        our_payments = [p for p in plan["payments"] if p.get("from_peer") == our_peer_id]

        total_sent = 0
        payment_hashes: List[str] = []

        for p in our_payments:
            to_peer = p["to_peer"]
            amount = int(p["amount_sats"])
            offer = self.get_offer(to_peer)
            if not offer:
                self.plugin.log(
                    f"SETTLEMENT: Missing BOLT12 offer for receiver {to_peer[:16]}... "
                    f"(proposal {proposal_id[:16]}...)",
                    level="warn"
                )
                return None

            pay = SettlementPayment(
                from_peer=our_peer_id,
                to_peer=to_peer,
                amount_sats=amount,
                bolt12_offer=offer,
            )
            pay = await self.execute_payment(pay)
            if pay.status != "completed":
                self.plugin.log(
                    f"SETTLEMENT: Payment failed to {to_peer[:16]}... for {amount} sats: {pay.error}",
                    level="warn"
                )
                return None

            total_sent += amount
            if pay.payment_hash:
                payment_hashes.append(pay.payment_hash)

        if total_sent != expected_sent:
            self.plugin.log(
                f"SETTLEMENT: Refusing to confirm execution for {proposal_id[:16]}... "
                f"(sent {total_sent} sats, expected {expected_sent} sats)",
                level="warn"
            )
            return None

        timestamp = int(time.time())
        from modules.protocol import get_settlement_executed_signing_payload
        exec_payload = {
            'proposal_id': proposal_id,
            'executor_peer_id': our_peer_id,
            'plan_hash': plan["plan_hash"],
            # total_sent is the authoritative value for completion checks.
            'total_sent_sats': total_sent,
            # Keep legacy fields for older listeners; these are informational only.
            'payment_hash': payment_hashes[0] if len(payment_hashes) == 1 else '',
            'amount_paid_sats': total_sent,
            'timestamp': timestamp,
        }
        signing_payload = get_settlement_executed_signing_payload(exec_payload)
        sig_result = rpc.signmessage(signing_payload)
        signature = sig_result.get('zbase', '')

        self.db.add_settlement_execution(
            proposal_id=proposal_id,
            executor_peer_id=our_peer_id,
            signature=signature,
            payment_hash=exec_payload.get("payment_hash") or None,
            amount_paid_sats=total_sent,
            plan_hash=plan["plan_hash"],
        )

        return {
            'proposal_id': proposal_id,
            'executor_peer_id': our_peer_id,
            'plan_hash': plan["plan_hash"],
            'total_sent_sats': total_sent,
            'payment_hash': exec_payload.get("payment_hash") or None,
            'amount_paid_sats': total_sent,
            'timestamp': timestamp,
            'signature': signature,
        }

    def check_and_complete_settlement(self, proposal_id: str) -> bool:
        """
        Check if all members have executed and complete the settlement.

        Args:
            proposal_id: Proposal to check

        Returns:
            True if settlement completed
        """
        proposal = self.db.get_settlement_proposal(proposal_id)
        if not proposal:
            return False

        if proposal.get('status') != 'ready':
            return False

        period = proposal.get('period')
        member_count = proposal.get('member_count', 0)
        total_fees = proposal.get('total_fees_sats', 0)

        # Get all executions
        executions = self.db.get_settlement_executions(proposal_id)
        exec_count = len(executions)

        # Require a canonical contributions snapshot to validate the plan.
        contributions_json = proposal.get("contributions_json")
        if not contributions_json:
            return False
        try:
            contributions = json.loads(contributions_json)
        except Exception:
            return False

        plan = self.compute_settlement_plan(period, contributions)
        expected_plan_hash = proposal.get("plan_hash")
        if isinstance(expected_plan_hash, str) and len(expected_plan_hash) == 64:
            if plan["plan_hash"] != expected_plan_hash:
                self.plugin.log(
                    f"SETTLEMENT: Cannot complete {proposal_id[:16]}... (plan hash mismatch)",
                    level="warn"
                )
                return False

        # Validate that each participant has executed and their reported totals match.
        executions_by_peer = {e.get("executor_peer_id"): e for e in executions}

        for c in contributions:
            peer_id = c.get("peer_id")
            if not peer_id:
                continue
            ex = executions_by_peer.get(peer_id)
            if not ex:
                return False

            # Check plan hash binding (newer clients).
            ex_plan_hash = ex.get("plan_hash")
            if isinstance(ex_plan_hash, str) and len(ex_plan_hash) == 64:
                if ex_plan_hash != plan["plan_hash"]:
                    return False

            expected_sent = int(plan["expected_sent_sats"].get(peer_id, 0))
            actual_sent = int(ex.get("amount_paid_sats", 0) or 0)
            if actual_sent != expected_sent:
                return False

        if exec_count >= member_count:
            # All members have confirmed correctly - mark as complete
            self.db.update_settlement_proposal_status(proposal_id, 'completed')

            # Mark period as settled (sum of expected sends is deterministic).
            total_distributed = sum(int(v) for v in plan["expected_sent_sats"].values())
            self.db.mark_period_settled(period, proposal_id, total_distributed)

            self.plugin.log(
                f"Settlement {proposal_id[:16]}... completed: "
                f"{total_distributed} sats distributed for {period}"
            )
            return True

        return False

    def get_distributed_settlement_status(self) -> Dict[str, Any]:
        """
        Get current distributed settlement status for monitoring.

        Returns:
            Status dict with pending/ready proposals, recent settlements
        """
        pending = self.db.get_pending_settlement_proposals()
        ready = self.db.get_ready_settlement_proposals()
        settled = self.db.get_settled_periods(limit=5)

        return {
            'pending_proposals': len(pending),
            'ready_proposals': len(ready),
            'recent_settlements': len(settled),
            'pending': pending,
            'ready': ready,
            'settled_periods': settled,
        }
