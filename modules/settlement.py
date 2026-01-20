"""
Settlement module for cl-hive

Implements BOLT12-based revenue settlement for hive fleet members.

Fair Share Algorithm:
- 40% weight: Capacity contribution (total_capacity / fleet_capacity)
- 40% weight: Routing contribution (forwards_routed / fleet_forwards)
- 20% weight: Uptime contribution (uptime_pct / 100)

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
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal, ROUND_DOWN


# Settlement period (weekly)
SETTLEMENT_PERIOD_SECONDS = 7 * 24 * 60 * 60  # 1 week

# Minimum payment threshold (don't send dust)
MIN_PAYMENT_SATS = 1000

# Fair share weights
WEIGHT_CAPACITY = 0.40
WEIGHT_FORWARDS = 0.40
WEIGHT_UPTIME = 0.20


@dataclass
class MemberContribution:
    """A member's contribution metrics for a settlement period."""
    peer_id: str
    capacity_sats: int
    forwards_sats: int
    fees_earned_sats: int
    uptime_pct: float
    bolt12_offer: Optional[str] = None


@dataclass
class SettlementResult:
    """Result of settlement calculation for one member."""
    peer_id: str
    fees_earned: int
    fair_share: int
    balance: int  # positive = owed money, negative = owes money
    bolt12_offer: Optional[str] = None


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
                FOREIGN KEY (period_id) REFERENCES settlement_periods(period_id),
                UNIQUE (period_id, peer_id)
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_settlement_contrib_period
            ON settlement_contributions(period_id)
        """)

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

    def list_offers(self) -> List[Dict[str, Any]]:
        """List all registered BOLT12 offers."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT peer_id, bolt12_offer, registered_at, last_verified, active
            FROM settlement_offers
            ORDER BY registered_at DESC
        """).fetchall()
        return [dict(row) for row in rows]

    def deactivate_offer(self, peer_id: str) -> Dict[str, Any]:
        """Deactivate a member's BOLT12 offer."""
        conn = self._get_connection()
        conn.execute(
            "UPDATE settlement_offers SET active = 0 WHERE peer_id = ?",
            (peer_id,)
        )
        return {"status": "deactivated", "peer_id": peer_id}

    # =========================================================================
    # FAIR SHARE CALCULATION
    # =========================================================================

    def calculate_fair_shares(
        self,
        contributions: List[MemberContribution]
    ) -> List[SettlementResult]:
        """
        Calculate fair share for each member based on contributions.

        Fair Share Algorithm:
        - 40% weight: capacity_contribution = member_capacity / total_capacity
        - 40% weight: routing_contribution = member_forwards / total_forwards
        - 20% weight: uptime_contribution = member_uptime / 100

        Each member's fair_share = total_fees * weighted_contribution_score
        Balance = fair_share - fees_earned
        - Positive balance = member is owed money
        - Negative balance = member owes money

        Args:
            contributions: List of member contributions

        Returns:
            List of settlement results with fair shares and balances
        """
        if not contributions:
            return []

        # Calculate totals
        total_capacity = sum(c.capacity_sats for c in contributions)
        total_forwards = sum(c.forwards_sats for c in contributions)
        total_fees = sum(c.fees_earned_sats for c in contributions)

        if total_fees == 0:
            return [
                SettlementResult(
                    peer_id=c.peer_id,
                    fees_earned=0,
                    fair_share=0,
                    balance=0,
                    bolt12_offer=c.bolt12_offer
                )
                for c in contributions
            ]

        results = []

        for member in contributions:
            # Calculate contribution scores (0.0 to 1.0)
            capacity_score = (
                member.capacity_sats / total_capacity
                if total_capacity > 0 else 0
            )
            forwards_score = (
                member.forwards_sats / total_forwards
                if total_forwards > 0 else 0
            )
            uptime_score = member.uptime_pct / 100.0

            # Weighted contribution score
            weighted_score = (
                WEIGHT_CAPACITY * capacity_score +
                WEIGHT_FORWARDS * forwards_score +
                WEIGHT_UPTIME * uptime_score
            )

            # Fair share of total fees
            fair_share = int(total_fees * weighted_score)

            # Balance: positive = owed money, negative = owes money
            balance = fair_share - member.fees_earned_sats

            results.append(SettlementResult(
                peer_id=member.peer_id,
                fees_earned=member.fees_earned_sats,
                fair_share=fair_share,
                balance=balance,
                bolt12_offer=member.bolt12_offer
            ))

        # Verify settlement balances sum to zero (accounting identity)
        total_balance = sum(r.balance for r in results)
        if abs(total_balance) > len(results):  # Allow small rounding errors
            self.plugin.log(
                f"Warning: Settlement balance mismatch of {total_balance} sats",
                level='warn'
            )

        return results

    # =========================================================================
    # PAYMENT GENERATION
    # =========================================================================

    def generate_payments(
        self,
        results: List[SettlementResult]
    ) -> List[SettlementPayment]:
        """
        Generate payment list from settlement results.

        Matches members with negative balance (owe money) to members with
        positive balance (owed money) to create payment list.

        Args:
            results: List of settlement results

        Returns:
            List of payments to execute
        """
        # Separate into payers (owe money) and receivers (owed money)
        payers = [r for r in results if r.balance < -MIN_PAYMENT_SATS and r.bolt12_offer]
        receivers = [r for r in results if r.balance > MIN_PAYMENT_SATS and r.bolt12_offer]

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

                if amount < MIN_PAYMENT_SATS:
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
                    fees_earned_sats, uptime_pct, fair_share_sats, balance_sats
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                period_id,
                result.peer_id,
                contrib.capacity_sats,
                contrib.forwards_sats,
                result.fees_earned,
                contrib.uptime_pct,
                result.fair_share,
                result.balance
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
