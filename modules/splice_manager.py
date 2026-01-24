"""
Splice Manager for cl-hive.

Implements coordinated splice operations between hive members.
Handles the full splice_init -> splice_update -> splice_signed workflow.

This module provides:
- Coordinated splice initiation with hive members
- Message handling for splice protocol messages
- Session tracking and timeout management
- Safety checks via SpliceCoordinator

Author: Lightning Goats Team
"""

import json
import secrets
import time
from typing import Any, Callable, Dict, List, Optional

from .protocol import (
    HiveMessageType,
    # Splice constants
    SPLICE_SESSION_TIMEOUT_SECONDS,
    SPLICE_TYPE_IN, SPLICE_TYPE_OUT, VALID_SPLICE_TYPES,
    SPLICE_STATUS_PENDING, SPLICE_STATUS_INIT_SENT, SPLICE_STATUS_INIT_RECEIVED,
    SPLICE_STATUS_UPDATING, SPLICE_STATUS_SIGNING, SPLICE_STATUS_COMPLETED,
    SPLICE_STATUS_ABORTED, SPLICE_STATUS_FAILED,
    SPLICE_REJECT_NOT_MEMBER, SPLICE_REJECT_NO_CHANNEL, SPLICE_REJECT_CHANNEL_BUSY,
    SPLICE_REJECT_SAFETY_BLOCKED, SPLICE_REJECT_NO_SPLICING, SPLICE_REJECT_SESSION_EXISTS,
    SPLICE_REJECT_INSUFFICIENT_FUNDS, SPLICE_REJECT_INVALID_AMOUNT, SPLICE_REJECT_DECLINED,
    SPLICE_ABORT_TIMEOUT, SPLICE_ABORT_USER_CANCELLED, SPLICE_ABORT_RPC_ERROR,
    SPLICE_ABORT_INVALID_PSBT, SPLICE_ABORT_SIGNATURE_FAILED,
    SPLICE_INIT_REQUEST_RATE_LIMIT, SPLICE_MESSAGE_RATE_LIMIT,
    # Validation functions
    validate_splice_init_request_payload,
    validate_splice_init_response_payload,
    validate_splice_update_payload,
    validate_splice_signed_payload,
    validate_splice_abort_payload,
    # Signing payload functions
    get_splice_init_request_signing_payload,
    get_splice_init_response_signing_payload,
    get_splice_update_signing_payload,
    get_splice_signed_signing_payload,
    get_splice_abort_signing_payload,
    # Message creation functions
    create_splice_init_request,
    create_splice_init_response,
    create_splice_update,
    create_splice_signed,
    create_splice_abort,
)


class SpliceManager:
    """
    Manages coordinated splice operations between hive members.

    Responsibilities:
    - Initiate splices with hive member peers
    - Process incoming splice messages
    - Track splice session state
    - Coordinate with SpliceCoordinator for safety checks
    """

    def __init__(
        self,
        database: Any,
        plugin: Any,
        splice_coordinator: Any,
        our_pubkey: str
    ):
        """
        Initialize the splice manager.

        Args:
            database: HiveDatabase instance
            plugin: Plugin instance for RPC/logging
            splice_coordinator: SpliceCoordinator for safety checks
            our_pubkey: Our node's public key
        """
        self.db = database
        self.plugin = plugin
        self.splice_coord = splice_coordinator
        self.our_pubkey = our_pubkey

        # Rate limiting trackers
        self._init_rate: Dict[str, List[int]] = {}
        self._message_rate: Dict[str, List[int]] = {}

    def _log(self, msg: str, level: str = 'info'):
        """Log a message."""
        if self.plugin:
            self.plugin.log(f"cl-hive: SpliceManager: {msg}", level=level)

    def _check_rate_limit(
        self,
        sender_id: str,
        tracker: Dict[str, List[int]],
        limit: tuple
    ) -> bool:
        """Check if sender is within rate limit."""
        max_count, window_seconds = limit
        now = int(time.time())
        cutoff = now - window_seconds

        if sender_id not in tracker:
            tracker[sender_id] = []

        # Remove old entries
        tracker[sender_id] = [t for t in tracker[sender_id] if t > cutoff]

        return len(tracker[sender_id]) < max_count

    def _record_message(self, sender_id: str, tracker: Dict[str, List[int]]):
        """Record a message for rate limiting."""
        now = int(time.time())
        if sender_id not in tracker:
            tracker[sender_id] = []
        tracker[sender_id].append(now)

    def _generate_session_id(self) -> str:
        """Generate a unique session ID."""
        return f"splice_{self.our_pubkey[:8]}_{int(time.time())}_{secrets.token_hex(4)}"

    def _verify_signature(
        self,
        payload: Dict[str, Any],
        signing_payload_fn: Callable,
        sender_id: str,
        rpc
    ) -> bool:
        """Verify a message signature."""
        signature = payload.get("signature")
        if not signature:
            return False

        signing_msg = signing_payload_fn(payload)
        try:
            result = rpc.checkmessage(signing_msg, signature)
            if not result.get("verified"):
                return False
            if result.get("pubkey") != sender_id:
                self._log(f"Signature pubkey mismatch: {result.get('pubkey')[:16]}... != {sender_id[:16]}...")
                return False
            return True
        except Exception as e:
            self._log(f"Signature verification error: {e}", level='error')
            return False

    def _get_channel_for_peer(self, peer_id: str, rpc) -> Optional[Dict[str, Any]]:
        """Get the channel with a hive member peer."""
        try:
            result = rpc.call("listpeerchannels", {"id": peer_id})
            channels = result.get("channels", [])
            # Return first normal channel
            for ch in channels:
                if ch.get("state") == "CHANNELD_NORMAL":
                    return ch
            return None
        except Exception as e:
            self._log(f"Error getting channel for peer {peer_id[:16]}...: {e}", level='debug')
            return None

    def _send_message(self, peer_id: str, msg: bytes, rpc) -> bool:
        """Send a message to a peer."""
        try:
            rpc.call("sendcustommsg", {
                "node_id": peer_id,
                "msg": msg.hex()
            })
            return True
        except Exception as e:
            self._log(f"Failed to send message to {peer_id[:16]}...: {e}", level='warn')
            return False

    # =========================================================================
    # INITIATE SPLICE
    # =========================================================================

    def initiate_splice(
        self,
        peer_id: str,
        channel_id: str,
        relative_amount: int,
        rpc,
        feerate_perkw: Optional[int] = None,
        dry_run: bool = False,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Initiate a splice operation with a hive member.

        Args:
            peer_id: Hive member to splice with
            channel_id: Channel to splice
            relative_amount: Positive = splice-in, Negative = splice-out
            rpc: RPC proxy
            feerate_perkw: Optional feerate (default: use urgent)
            dry_run: Preview without executing
            force: Skip safety warnings

        Returns:
            Dict with result of initiation
        """
        self._log(f"Initiating splice: peer={peer_id[:16]}... channel={channel_id} amount={relative_amount}")

        # Determine splice type
        if relative_amount > 0:
            splice_type = SPLICE_TYPE_IN
            amount_sats = relative_amount
        elif relative_amount < 0:
            splice_type = SPLICE_TYPE_OUT
            amount_sats = abs(relative_amount)
        else:
            return {"error": "invalid_amount", "message": "Amount cannot be zero"}

        # Check if peer is a hive member
        member = self.db.get_member(peer_id)
        if not member:
            return {"error": "not_member", "message": f"Peer {peer_id[:16]}... is not a hive member"}

        # Check if channel exists and is normal
        channel = self._get_channel_for_peer(peer_id, rpc)
        if not channel:
            return {"error": "no_channel", "message": f"No active channel with peer {peer_id[:16]}..."}

        # Get both short_channel_id and full channel_id
        short_channel_id = channel.get("short_channel_id")
        full_channel_id = channel.get("channel_id")  # 32-byte hex format needed for splice_init

        # User can provide either format
        if channel_id != short_channel_id and channel_id != full_channel_id:
            return {
                "error": "channel_mismatch",
                "message": f"Channel ID mismatch: {channel_id} not found (scid={short_channel_id})"
            }

        # Check for active splice on this channel
        existing = self.db.get_active_splice_for_channel(channel_id)
        if existing:
            return {
                "error": "session_exists",
                "message": f"Active splice session exists: {existing['session_id']}"
            }

        # Safety check for splice-out
        if splice_type == SPLICE_TYPE_OUT and self.splice_coord:
            safety = self.splice_coord.check_splice_out_safety(peer_id, amount_sats, channel_id)
            if not safety.get("can_proceed") and not force:
                return {
                    "error": "safety_blocked",
                    "safety": safety,
                    "message": f"Splice-out blocked: {safety.get('reason')}. Use force=true to override."
                }
            if safety.get("safety") != "safe" and not force:
                self._log(f"Splice-out safety warning: {safety.get('reason')}", level='warn')

        # Get feerate if not provided
        if feerate_perkw is None:
            try:
                feerates = rpc.feerates(style="perkw")
                feerate_perkw = feerates.get("perkw", {}).get("urgent", 10000)
            except Exception:
                feerate_perkw = 10000  # Default fallback

        # If dry run, return preview
        if dry_run:
            return {
                "dry_run": True,
                "peer_id": peer_id,
                "channel_id": channel_id,
                "splice_type": splice_type,
                "amount_sats": amount_sats,
                "feerate_perkw": feerate_perkw,
                "message": "Dry run - no action taken"
            }

        # Call splice_init to get initial PSBT
        # Note: splice_init requires the full 32-byte hex channel_id, not short_channel_id
        try:
            splice_result = rpc.call("splice_init", {
                "channel_id": full_channel_id,
                "relative_amount": relative_amount,
                "feerate_per_kw": feerate_perkw,
                "force_feerate": False
            })
            psbt = splice_result.get("psbt")
            if not psbt:
                return {"error": "splice_init_failed", "message": "No PSBT returned from splice_init"}
        except Exception as e:
            self._log(f"splice_init failed: {e}", level='error')
            return {"error": "splice_init_failed", "message": str(e)}

        # Generate session ID and create session
        session_id = self._generate_session_id()
        now = int(time.time())

        self.db.create_splice_session(
            session_id=session_id,
            channel_id=channel_id,
            peer_id=peer_id,
            initiator="local",
            splice_type=splice_type,
            amount_sats=amount_sats,
            timeout_seconds=SPLICE_SESSION_TIMEOUT_SECONDS
        )
        self.db.update_splice_session(session_id, status=SPLICE_STATUS_INIT_SENT, psbt=psbt)

        # Create and send SPLICE_INIT_REQUEST
        msg = create_splice_init_request(
            initiator_id=self.our_pubkey,
            session_id=session_id,
            channel_id=channel_id,
            splice_type=splice_type,
            amount_sats=amount_sats,
            psbt=psbt,
            timestamp=now,
            rpc=rpc,
            feerate_perkw=feerate_perkw
        )

        if not msg:
            self.db.update_splice_session(
                session_id,
                status=SPLICE_STATUS_FAILED,
                error_message="Failed to create SPLICE_INIT_REQUEST"
            )
            return {"error": "message_failed", "message": "Failed to create splice init request"}

        if not self._send_message(peer_id, msg, rpc):
            self.db.update_splice_session(
                session_id,
                status=SPLICE_STATUS_FAILED,
                error_message="Failed to send SPLICE_INIT_REQUEST"
            )
            return {"error": "send_failed", "message": "Failed to send splice init request"}

        self._log(f"Sent SPLICE_INIT_REQUEST: session={session_id}")

        return {
            "success": True,
            "session_id": session_id,
            "channel_id": channel_id,
            "peer_id": peer_id,
            "splice_type": splice_type,
            "amount_sats": amount_sats,
            "status": SPLICE_STATUS_INIT_SENT,
            "message": "Splice initiated, waiting for peer response"
        }

    # =========================================================================
    # MESSAGE HANDLERS
    # =========================================================================

    def handle_splice_init_request(
        self,
        sender_id: str,
        payload: Dict[str, Any],
        rpc
    ) -> Dict[str, Any]:
        """
        Handle incoming SPLICE_INIT_REQUEST message.

        Args:
            sender_id: Peer who sent the request
            payload: Message payload
            rpc: RPC proxy

        Returns:
            Dict with handling result
        """
        self._log(f"Received SPLICE_INIT_REQUEST from {sender_id[:16]}...")

        # Rate limit check
        if not self._check_rate_limit(sender_id, self._init_rate, SPLICE_INIT_REQUEST_RATE_LIMIT):
            self._log(f"Rate limited splice init from {sender_id[:16]}...")
            return {"error": "rate_limited"}

        # Validate payload
        if not validate_splice_init_request_payload(payload):
            self._log(f"Invalid splice init payload from {sender_id[:16]}...")
            return {"error": "invalid_payload"}

        # Verify initiator matches sender
        initiator_id = payload.get("initiator_id")
        if initiator_id != sender_id:
            self._log(f"Initiator mismatch: {initiator_id[:16]}... != {sender_id[:16]}...")
            return {"error": "initiator_mismatch"}

        # Verify sender is a hive member
        member = self.db.get_member(sender_id)
        if not member:
            self._log(f"Splice init from non-member {sender_id[:16]}...")
            self._send_reject(sender_id, payload.get("session_id"), SPLICE_REJECT_NOT_MEMBER, rpc)
            return {"error": "not_member"}

        # Verify signature
        if not self._verify_signature(payload, get_splice_init_request_signing_payload, sender_id, rpc):
            self._log("Splice init signature verification failed")
            return {"error": "invalid_signature"}

        # Record for rate limiting
        self._record_message(sender_id, self._init_rate)

        session_id = payload.get("session_id")
        channel_id = payload.get("channel_id")
        splice_type = payload.get("splice_type")
        amount_sats = payload.get("amount_sats")
        psbt = payload.get("psbt")
        feerate_perkw = payload.get("feerate_perkw")

        # Check if we have a channel with this peer
        channel = self._get_channel_for_peer(sender_id, rpc)
        if not channel:
            self._log(f"No channel with {sender_id[:16]}...")
            self._send_reject(sender_id, session_id, SPLICE_REJECT_NO_CHANNEL, rpc)
            return {"error": "no_channel"}

        # Verify channel ID matches
        actual_channel_id = channel.get("short_channel_id", channel.get("channel_id"))
        if channel_id != actual_channel_id:
            self._log(f"Channel ID mismatch in splice request")
            self._send_reject(sender_id, session_id, SPLICE_REJECT_NO_CHANNEL, rpc)
            return {"error": "channel_mismatch"}

        # Check for existing active splice
        existing = self.db.get_active_splice_for_channel(channel_id)
        if existing:
            self._log(f"Channel {channel_id} already has active splice")
            self._send_reject(sender_id, session_id, SPLICE_REJECT_CHANNEL_BUSY, rpc)
            return {"error": "channel_busy"}

        # Create session for tracking
        self.db.create_splice_session(
            session_id=session_id,
            channel_id=channel_id,
            peer_id=sender_id,
            initiator="remote",
            splice_type=splice_type,
            amount_sats=amount_sats,
            timeout_seconds=SPLICE_SESSION_TIMEOUT_SECONDS
        )
        self.db.update_splice_session(session_id, status=SPLICE_STATUS_INIT_RECEIVED, psbt=psbt)

        # Call splice_update with their PSBT
        try:
            update_result = rpc.call("splice_update", {
                "channel_id": channel_id,
                "psbt": psbt
            })
            our_psbt = update_result.get("psbt")
            commitments_secured = update_result.get("commitments_secured", False)

            if not our_psbt:
                raise Exception("No PSBT returned from splice_update")

        except Exception as e:
            self._log(f"splice_update failed: {e}", level='error')
            self.db.update_splice_session(
                session_id,
                status=SPLICE_STATUS_FAILED,
                error_message=str(e)
            )
            self._send_reject(sender_id, session_id, SPLICE_REJECT_DECLINED, rpc)
            return {"error": "splice_update_failed", "message": str(e)}

        # Update session with our PSBT
        self.db.update_splice_session(
            session_id,
            psbt=our_psbt,
            commitments_secured=commitments_secured
        )

        # Send acceptance response
        now = int(time.time())
        response = create_splice_init_response(
            responder_id=self.our_pubkey,
            session_id=session_id,
            accepted=True,
            timestamp=now,
            rpc=rpc,
            psbt=our_psbt
        )

        if not response or not self._send_message(sender_id, response, rpc):
            self._log("Failed to send splice init response")
            return {"error": "response_failed"}

        self._log(f"Accepted splice {session_id}, commitments_secured={commitments_secured}")

        # If commitments are secured, proceed to signing
        if commitments_secured:
            return self._proceed_to_signing(session_id, sender_id, channel_id, our_psbt, rpc)

        return {"success": True, "session_id": session_id, "status": SPLICE_STATUS_UPDATING}

    def handle_splice_init_response(
        self,
        sender_id: str,
        payload: Dict[str, Any],
        rpc
    ) -> Dict[str, Any]:
        """
        Handle incoming SPLICE_INIT_RESPONSE message.

        Args:
            sender_id: Peer who sent the response
            payload: Message payload
            rpc: RPC proxy

        Returns:
            Dict with handling result
        """
        self._log(f"Received SPLICE_INIT_RESPONSE from {sender_id[:16]}...")

        # Validate payload
        if not validate_splice_init_response_payload(payload):
            self._log(f"Invalid splice init response payload")
            return {"error": "invalid_payload"}

        # Verify responder matches sender
        responder_id = payload.get("responder_id")
        if responder_id != sender_id:
            self._log(f"Responder mismatch")
            return {"error": "responder_mismatch"}

        # Verify signature
        if not self._verify_signature(payload, get_splice_init_response_signing_payload, sender_id, rpc):
            self._log("Splice init response signature verification failed")
            return {"error": "invalid_signature"}

        session_id = payload.get("session_id")
        accepted = payload.get("accepted")

        # Get session
        session = self.db.get_splice_session(session_id)
        if not session:
            self._log(f"Unknown session {session_id}")
            return {"error": "unknown_session"}

        if session.get("peer_id") != sender_id:
            self._log(f"Session peer mismatch")
            return {"error": "peer_mismatch"}

        if session.get("initiator") != "local":
            self._log(f"We're not the initiator of session {session_id}")
            return {"error": "not_initiator"}

        if not accepted:
            reason = payload.get("reason", "rejected")
            self._log(f"Splice rejected: {reason}")
            self.db.update_splice_session(
                session_id,
                status=SPLICE_STATUS_FAILED,
                error_message=f"Peer rejected: {reason}"
            )
            return {"success": False, "rejected": True, "reason": reason}

        # Update with peer's PSBT
        peer_psbt = payload.get("psbt")
        if not peer_psbt:
            self._log("No PSBT in acceptance response")
            return {"error": "no_psbt"}

        # Continue splice_update loop
        try:
            update_result = rpc.call("splice_update", {
                "channel_id": session.get("channel_id"),
                "psbt": peer_psbt
            })
            our_psbt = update_result.get("psbt")
            commitments_secured = update_result.get("commitments_secured", False)

        except Exception as e:
            self._log(f"splice_update failed: {e}", level='error')
            self._send_abort(sender_id, session_id, SPLICE_ABORT_RPC_ERROR, rpc)
            self.db.update_splice_session(
                session_id,
                status=SPLICE_STATUS_FAILED,
                error_message=str(e)
            )
            return {"error": "splice_update_failed", "message": str(e)}

        self.db.update_splice_session(
            session_id,
            status=SPLICE_STATUS_UPDATING,
            psbt=our_psbt,
            commitments_secured=commitments_secured
        )

        if commitments_secured:
            return self._proceed_to_signing(
                session_id, sender_id, session.get("channel_id"), our_psbt, rpc
            )

        # Send update
        now = int(time.time())
        update_msg = create_splice_update(
            sender_id=self.our_pubkey,
            session_id=session_id,
            psbt=our_psbt,
            commitments_secured=False,
            timestamp=now,
            rpc=rpc
        )

        if update_msg:
            self._send_message(sender_id, update_msg, rpc)

        return {"success": True, "session_id": session_id, "status": SPLICE_STATUS_UPDATING}

    def handle_splice_update(
        self,
        sender_id: str,
        payload: Dict[str, Any],
        rpc
    ) -> Dict[str, Any]:
        """
        Handle incoming SPLICE_UPDATE message.

        Args:
            sender_id: Peer who sent the update
            payload: Message payload
            rpc: RPC proxy

        Returns:
            Dict with handling result
        """
        self._log(f"Received SPLICE_UPDATE from {sender_id[:16]}...")

        # Validate payload
        if not validate_splice_update_payload(payload):
            return {"error": "invalid_payload"}

        # Verify sender matches payload
        if payload.get("sender_id") != sender_id:
            return {"error": "sender_mismatch"}

        # Verify signature
        if not self._verify_signature(payload, get_splice_update_signing_payload, sender_id, rpc):
            return {"error": "invalid_signature"}

        session_id = payload.get("session_id")
        peer_psbt = payload.get("psbt")
        peer_secured = payload.get("commitments_secured")

        # Get session
        session = self.db.get_splice_session(session_id)
        if not session:
            return {"error": "unknown_session"}

        if session.get("peer_id") != sender_id:
            return {"error": "peer_mismatch"}

        # Continue splice_update
        try:
            update_result = rpc.call("splice_update", {
                "channel_id": session.get("channel_id"),
                "psbt": peer_psbt
            })
            our_psbt = update_result.get("psbt")
            commitments_secured = update_result.get("commitments_secured", False)

        except Exception as e:
            self._log(f"splice_update failed: {e}", level='error')
            self._send_abort(sender_id, session_id, SPLICE_ABORT_RPC_ERROR, rpc)
            self.db.update_splice_session(
                session_id,
                status=SPLICE_STATUS_FAILED,
                error_message=str(e)
            )
            return {"error": "splice_update_failed"}

        self.db.update_splice_session(
            session_id,
            psbt=our_psbt,
            commitments_secured=commitments_secured
        )

        if commitments_secured:
            return self._proceed_to_signing(
                session_id, sender_id, session.get("channel_id"), our_psbt, rpc
            )

        # Send update back
        now = int(time.time())
        update_msg = create_splice_update(
            sender_id=self.our_pubkey,
            session_id=session_id,
            psbt=our_psbt,
            commitments_secured=False,
            timestamp=now,
            rpc=rpc
        )

        if update_msg:
            self._send_message(sender_id, update_msg, rpc)

        return {"success": True, "session_id": session_id, "status": SPLICE_STATUS_UPDATING}

    def handle_splice_signed(
        self,
        sender_id: str,
        payload: Dict[str, Any],
        rpc
    ) -> Dict[str, Any]:
        """
        Handle incoming SPLICE_SIGNED message.

        Args:
            sender_id: Peer who sent the signed PSBT
            payload: Message payload
            rpc: RPC proxy

        Returns:
            Dict with handling result
        """
        self._log(f"Received SPLICE_SIGNED from {sender_id[:16]}...")

        # Validate payload
        if not validate_splice_signed_payload(payload):
            return {"error": "invalid_payload"}

        # Verify sender
        if payload.get("sender_id") != sender_id:
            return {"error": "sender_mismatch"}

        # Verify signature
        if not self._verify_signature(payload, get_splice_signed_signing_payload, sender_id, rpc):
            return {"error": "invalid_signature"}

        session_id = payload.get("session_id")
        signed_psbt = payload.get("signed_psbt")
        txid = payload.get("txid")

        # Get session
        session = self.db.get_splice_session(session_id)
        if not session:
            return {"error": "unknown_session"}

        if session.get("peer_id") != sender_id:
            return {"error": "peer_mismatch"}

        # If peer sent txid, splice is complete
        if txid:
            self._log(f"Splice {session_id} completed with txid {txid}")
            self.db.update_splice_session(
                session_id,
                status=SPLICE_STATUS_COMPLETED,
                txid=txid
            )
            return {
                "success": True,
                "session_id": session_id,
                "status": SPLICE_STATUS_COMPLETED,
                "txid": txid
            }

        # Otherwise, sign and finalize
        if signed_psbt:
            try:
                signed_result = rpc.call("splice_signed", {
                    "channel_id": session.get("channel_id"),
                    "psbt": signed_psbt
                })
                result_txid = signed_result.get("txid")

                self._log(f"Splice {session_id} finalized with txid {result_txid}")
                self.db.update_splice_session(
                    session_id,
                    status=SPLICE_STATUS_COMPLETED,
                    txid=result_txid
                )

                # Notify peer of completion
                now = int(time.time())
                complete_msg = create_splice_signed(
                    sender_id=self.our_pubkey,
                    session_id=session_id,
                    timestamp=now,
                    rpc=rpc,
                    txid=result_txid
                )
                if complete_msg:
                    self._send_message(sender_id, complete_msg, rpc)

                return {
                    "success": True,
                    "session_id": session_id,
                    "status": SPLICE_STATUS_COMPLETED,
                    "txid": result_txid
                }

            except Exception as e:
                self._log(f"splice_signed failed: {e}", level='error')
                self.db.update_splice_session(
                    session_id,
                    status=SPLICE_STATUS_FAILED,
                    error_message=str(e)
                )
                return {"error": "splice_signed_failed", "message": str(e)}

        return {"error": "no_psbt_or_txid"}

    def handle_splice_abort(
        self,
        sender_id: str,
        payload: Dict[str, Any],
        rpc
    ) -> Dict[str, Any]:
        """
        Handle incoming SPLICE_ABORT message.

        Args:
            sender_id: Peer who sent the abort
            payload: Message payload
            rpc: RPC proxy

        Returns:
            Dict with handling result
        """
        self._log(f"Received SPLICE_ABORT from {sender_id[:16]}...")

        # Validate payload
        if not validate_splice_abort_payload(payload):
            return {"error": "invalid_payload"}

        # Verify sender
        if payload.get("sender_id") != sender_id:
            return {"error": "sender_mismatch"}

        # Verify signature
        if not self._verify_signature(payload, get_splice_abort_signing_payload, sender_id, rpc):
            return {"error": "invalid_signature"}

        session_id = payload.get("session_id")
        reason = payload.get("reason")

        # Get session
        session = self.db.get_splice_session(session_id)
        if not session:
            return {"error": "unknown_session"}

        if session.get("peer_id") != sender_id:
            return {"error": "peer_mismatch"}

        self._log(f"Splice {session_id} aborted by peer: {reason}")
        self.db.update_splice_session(
            session_id,
            status=SPLICE_STATUS_ABORTED,
            error_message=f"Peer aborted: {reason}"
        )

        return {"success": True, "aborted": True, "reason": reason}

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _send_reject(
        self,
        peer_id: str,
        session_id: str,
        reason: str,
        rpc
    ):
        """Send a rejection response."""
        now = int(time.time())
        response = create_splice_init_response(
            responder_id=self.our_pubkey,
            session_id=session_id,
            accepted=False,
            timestamp=now,
            rpc=rpc,
            reason=reason
        )
        if response:
            self._send_message(peer_id, response, rpc)

    def _send_abort(
        self,
        peer_id: str,
        session_id: str,
        reason: str,
        rpc
    ):
        """Send an abort message."""
        now = int(time.time())
        msg = create_splice_abort(
            sender_id=self.our_pubkey,
            session_id=session_id,
            reason=reason,
            timestamp=now,
            rpc=rpc
        )
        if msg:
            self._send_message(peer_id, msg, rpc)

    def _proceed_to_signing(
        self,
        session_id: str,
        peer_id: str,
        channel_id: str,
        psbt: str,
        rpc
    ) -> Dict[str, Any]:
        """Proceed to signing phase after commitments secured."""
        self._log(f"Proceeding to signing for session {session_id}")

        self.db.update_splice_session(session_id, status=SPLICE_STATUS_SIGNING)

        try:
            # Sign the PSBT
            signed_result = rpc.call("splice_signed", {
                "channel_id": channel_id,
                "psbt": psbt
            })
            txid = signed_result.get("txid")

            if txid:
                # We got the txid, splice complete
                self._log(f"Splice {session_id} completed with txid {txid}")
                self.db.update_splice_session(
                    session_id,
                    status=SPLICE_STATUS_COMPLETED,
                    txid=txid
                )

                # Notify peer
                now = int(time.time())
                complete_msg = create_splice_signed(
                    sender_id=self.our_pubkey,
                    session_id=session_id,
                    timestamp=now,
                    rpc=rpc,
                    txid=txid
                )
                if complete_msg:
                    self._send_message(peer_id, complete_msg, rpc)

                return {
                    "success": True,
                    "session_id": session_id,
                    "status": SPLICE_STATUS_COMPLETED,
                    "txid": txid
                }
            else:
                # Need to exchange signed PSBT
                signed_psbt = signed_result.get("psbt")
                if signed_psbt:
                    now = int(time.time())
                    msg = create_splice_signed(
                        sender_id=self.our_pubkey,
                        session_id=session_id,
                        timestamp=now,
                        rpc=rpc,
                        signed_psbt=signed_psbt
                    )
                    if msg:
                        self._send_message(peer_id, msg, rpc)

                return {
                    "success": True,
                    "session_id": session_id,
                    "status": SPLICE_STATUS_SIGNING
                }

        except Exception as e:
            self._log(f"Signing failed: {e}", level='error')
            self._send_abort(peer_id, session_id, SPLICE_ABORT_SIGNATURE_FAILED, rpc)
            self.db.update_splice_session(
                session_id,
                status=SPLICE_STATUS_FAILED,
                error_message=str(e)
            )
            return {"error": "signing_failed", "message": str(e)}

    # =========================================================================
    # SESSION MANAGEMENT
    # =========================================================================

    def cleanup_expired_sessions(self) -> int:
        """Clean up expired splice sessions."""
        count = self.db.cleanup_expired_splice_sessions()
        if count > 0:
            self._log(f"Cleaned up {count} expired splice sessions")
        return count

    def get_session_status(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a splice session."""
        return self.db.get_splice_session(session_id)

    def get_active_sessions(self) -> List[Dict[str, Any]]:
        """Get all active splice sessions."""
        return self.db.get_pending_splice_sessions()

    def abort_session(self, session_id: str, rpc) -> Dict[str, Any]:
        """
        Abort a splice session.

        Args:
            session_id: Session to abort
            rpc: RPC proxy

        Returns:
            Dict with abort result
        """
        session = self.db.get_splice_session(session_id)
        if not session:
            return {"error": "unknown_session"}

        if session.get("status") in (SPLICE_STATUS_COMPLETED, SPLICE_STATUS_ABORTED, SPLICE_STATUS_FAILED):
            return {"error": "session_already_ended", "status": session.get("status")}

        # Send abort to peer
        self._send_abort(session.get("peer_id"), session_id, SPLICE_ABORT_USER_CANCELLED, rpc)

        # Update session
        self.db.update_splice_session(
            session_id,
            status=SPLICE_STATUS_ABORTED,
            error_message="User cancelled"
        )

        return {"success": True, "session_id": session_id, "status": SPLICE_STATUS_ABORTED}
