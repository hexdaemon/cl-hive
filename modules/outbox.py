"""
OutboxManager for cl-hive (Phase D: Reliable Delivery).

Provides durable, per-peer message delivery with exponential backoff,
explicit MSG_ACK handling, and implicit ack resolution via domain responses.

Design:
- Each critical broadcast creates N outbox rows (one per target peer).
- Unicast messages create a single row.
- The outbox_retry_loop calls retry_pending() every 30 seconds.
- Messages are retried with exponential backoff (30s -> 1h cap).
- Explicit MSG_ACK or implicit domain responses clear entries.
- Backpressure: MAX_INFLIGHT_PER_PEER limits per-peer queue depth.
"""

import json
import random
import time
from typing import Any, Callable, Dict, List, Optional

from modules.protocol import (
    HiveMessageType,
    IMPLICIT_ACK_MAP,
    IMPLICIT_ACK_MATCH_FIELD,
    serialize,
)


class OutboxManager:
    """Manages reliable delivery of critical hive protocol messages."""

    # Retry policy constants
    BASE_RETRY_SECONDS = 30
    MAX_RETRY_SECONDS = 3600      # 1 hour cap
    MAX_RETRIES = 20
    DEFAULT_TTL_SECONDS = 24 * 3600  # 24 hours
    MAX_INFLIGHT_PER_PEER = 10

    def __init__(self, database, send_fn, get_members_fn, our_pubkey, log_fn):
        """
        Args:
            database: HiveDatabase instance
            send_fn: Callable(peer_id, msg_bytes) -> bool for sending messages
            get_members_fn: Callable() -> List[str] returning member peer_ids
            our_pubkey: Our node's pubkey
            log_fn: Callable(msg, level) for logging
        """
        self._db = database
        self._send_fn = send_fn
        self._get_members_fn = get_members_fn
        self._our_pubkey = our_pubkey
        self._log = log_fn

    def enqueue(self, msg_id: str, msg_type: HiveMessageType, payload: Dict[str, Any],
                peer_ids: Optional[List[str]] = None) -> int:
        """
        Enqueue a message for reliable delivery.

        If peer_ids is None, broadcast to all current members.
        Returns the number of outbox entries created.
        """
        if peer_ids is None:
            peer_ids = self._get_members_fn()

        if not peer_ids:
            return 0

        now = int(time.time())
        expires_at = now + self.DEFAULT_TTL_SECONDS
        payload_json = json.dumps(payload, separators=(',', ':'))

        enqueued = 0
        for pid in peer_ids:
            if pid == self._our_pubkey:
                continue

            # Backpressure check
            inflight = self._db.count_inflight_for_peer(pid)
            if inflight >= self.MAX_INFLIGHT_PER_PEER:
                self._log(
                    f"Outbox: backpressure for {pid[:16]}... "
                    f"({inflight} inflight, dropping {msg_id[:16]}...)",
                    level='warn'
                )
                continue

            if self._db.enqueue_outbox(msg_id, pid, int(msg_type),
                                       payload_json, expires_at):
                enqueued += 1

        return enqueued

    def process_ack(self, peer_id: str, ack_msg_id: str, status: str) -> bool:
        """
        Handle explicit MSG_ACK from a peer.

        Args:
            peer_id: Peer that sent the ack
            ack_msg_id: The msg_id being acknowledged
            status: "ok", "invalid", or "retry_later"

        Returns:
            True if an outbox entry was found and updated.
        """
        if status == "ok":
            return self._db.ack_outbox(ack_msg_id, peer_id)
        elif status == "invalid":
            return self._db.fail_outbox(ack_msg_id, peer_id,
                                        "remote_invalid")
        # "retry_later" - leave as-is, will retry on schedule
        return False

    def process_implicit_ack(self, peer_id: str,
                             response_type: HiveMessageType,
                             response_payload: Dict[str, Any]) -> int:
        """
        Handle a domain-specific response that implies acknowledgment.

        E.g. receiving SETTLEMENT_READY from a peer clears our outbox
        SETTLEMENT_PROPOSE entries for that peer+proposal_id.

        Returns:
            Number of outbox entries cleared.
        """
        request_type = IMPLICIT_ACK_MAP.get(response_type)
        if request_type is None:
            return 0

        match_field = IMPLICIT_ACK_MATCH_FIELD.get(response_type)
        if not match_field:
            return 0

        match_value = response_payload.get(match_field)
        if not match_value or not isinstance(match_value, str):
            return 0

        return self._db.ack_outbox_by_type(
            peer_id, int(request_type), match_field, match_value
        )

    def retry_pending(self) -> Dict[str, int]:
        """
        Called by background loop. Retries pending messages.

        Returns:
            Stats dict with counts of sent, failed, skipped.
        """
        stats = {"sent": 0, "failed": 0, "skipped": 0}

        pending = self._db.get_outbox_pending(limit=50)
        if not pending:
            return stats

        for entry in pending:
            msg_id = entry["msg_id"]
            peer_id = entry["peer_id"]
            msg_type = entry["msg_type"]
            payload_json = entry["payload_json"]
            retry_count = entry["retry_count"]

            # Check max retries
            if retry_count >= self.MAX_RETRIES:
                self._db.fail_outbox(msg_id, peer_id,
                                     f"max_retries_exceeded ({self.MAX_RETRIES})")
                stats["failed"] += 1
                self._log(
                    f"Outbox: max retries for {msg_id[:16]}... -> {peer_id[:16]}...",
                    level='debug'
                )
                continue

            # Reconstruct and send
            try:
                payload = json.loads(payload_json)
                msg_bytes = serialize(HiveMessageType(msg_type), payload)
            except Exception as e:
                # Parse/serialize errors are permanent — retrying won't help
                self._db.fail_outbox(msg_id, peer_id,
                                     f"parse_error: {str(e)[:100]}")
                stats["failed"] += 1
                self._log(
                    f"Outbox: permanent parse error for {msg_id[:16]}...: {e}",
                    level='warn'
                )
                continue

            try:
                success = self._send_fn(peer_id, msg_bytes)
            except Exception as e:
                success = False

            if success:
                next_retry = self._calculate_next_retry(retry_count)
                self._db.update_outbox_sent(msg_id, peer_id, next_retry)
                stats["sent"] += 1
            else:
                next_retry = self._calculate_next_retry(retry_count)
                self._db.update_outbox_sent(msg_id, peer_id, next_retry)
                stats["skipped"] += 1

        return stats

    def expire_and_cleanup(self) -> Dict[str, int]:
        """
        Expire stale entries and cleanup old terminal entries.

        Returns:
            Stats dict with expired and cleaned counts.
        """
        expired = self._db.expire_outbox()
        cleaned = self._db.cleanup_outbox()
        return {"expired": expired, "cleaned": cleaned}

    def _calculate_next_retry(self, retry_count: int) -> int:
        """
        Calculate next retry timestamp using exponential backoff with jitter.

        Formula: min(BASE * 2^count + jitter, MAX)
        Jitter is 0-25% of the delay to prevent thundering herd.
        """
        delay = min(
            self.BASE_RETRY_SECONDS * (2 ** retry_count),
            self.MAX_RETRY_SECONDS
        )
        # Add 0-25% jitter
        jitter = random.uniform(0, delay * 0.25)
        return int(time.time() + delay + jitter)

    def stats(self) -> Dict[str, Any]:
        """Return outbox stats for monitoring."""
        try:
            pending = self._db.get_outbox_pending(limit=1000)
            # Count by status from a broader query isn't available,
            # but we can report pending count
            return {
                "pending_count": len(pending),
            }
        except Exception:
            return {"pending_count": 0}
