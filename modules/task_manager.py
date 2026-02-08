"""
Task Manager for cl-hive.

Implements the Task Delegation Protocol (Phase 10) for coordinating
tasks between hive members. When a node can't complete a task (e.g.,
peer rejects channel open), it can delegate to another hive member.

Author: Lightning Goats Team
"""

import json
import time
from typing import Any, Callable, Dict, List, Optional

from .protocol import (
    HiveMessageType,
    create_task_request,
    create_task_response,
    validate_task_request_payload,
    validate_task_response_payload,
    get_task_request_signing_payload,
    get_task_response_signing_payload,
    TASK_REQUEST_RATE_LIMIT,
    TASK_RESPONSE_RATE_LIMIT,
    TASK_TYPE_EXPAND_TO,
    TASK_STATUS_ACCEPTED,
    TASK_STATUS_REJECTED,
    TASK_STATUS_COMPLETED,
    TASK_STATUS_FAILED,
    TASK_REJECT_BUSY,
    TASK_REJECT_NO_FUNDS,
    TASK_REJECT_NO_CONNECTION,
    TASK_REJECT_POLICY,
    TASK_PRIORITY_NORMAL,
    TASK_DEFAULT_DEADLINE_HOURS,
    MAX_PENDING_TASKS,
)


class TaskManager:
    """
    Manages task delegation between hive members.

    Responsibilities:
    - Send task requests to other members
    - Process incoming task requests
    - Track task status
    - Execute delegated tasks
    """

    def __init__(
        self,
        database: Any,
        plugin: Any,
        our_pubkey: str
    ):
        """
        Initialize the task manager.

        Args:
            database: HiveDatabase instance
            plugin: Plugin instance for RPC/logging
            our_pubkey: Our node's public key
        """
        self.db = database
        self.plugin = plugin
        self.our_pubkey = our_pubkey

        # Governance engine reference (set by cl-hive.py after init)
        self.decision_engine: Any = None

        # Rate limiting trackers
        self._request_rate: Dict[str, List[int]] = {}
        self._response_rate: Dict[str, List[int]] = {}

        # Callback for executing tasks
        self._task_executor: Optional[Callable] = None

    def set_task_executor(self, executor: Callable):
        """
        Set the callback for executing tasks.

        The executor function should have signature:
            executor(task_type: str, task_params: dict) -> dict

        Returns dict with:
            - success: bool
            - result: dict (if success)
            - error: str (if failure)
        """
        self._task_executor = executor

    def _log(self, msg: str, level: str = 'info'):
        """Log a message."""
        if self.plugin:
            self.plugin.log(f"cl-hive: TaskManager: {msg}", level=level)

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

        # Evict empty/stale keys to prevent unbounded dict growth
        if len(tracker) > 200:
            stale = [k for k, v in tracker.items() if not v]
            for k in stale:
                del tracker[k]

        return len(tracker[sender_id]) < max_count

    def _record_message(self, sender_id: str, tracker: Dict[str, List[int]]):
        """Record a message for rate limiting."""
        now = int(time.time())
        if sender_id not in tracker:
            tracker[sender_id] = []
        tracker[sender_id].append(now)

    # =========================================================================
    # OUTGOING TASK REQUESTS
    # =========================================================================

    def request_task(
        self,
        target_member_id: str,
        task_type: str,
        task_params: Dict[str, Any],
        rpc,
        priority: str = TASK_PRIORITY_NORMAL,
        deadline_hours: int = TASK_DEFAULT_DEADLINE_HOURS,
        failure_context: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Send a task request to another hive member.

        Args:
            target_member_id: Member to send request to
            task_type: Type of task (expand_to, etc.)
            task_params: Task-specific parameters
            rpc: RPC proxy for signing and sending
            priority: Task priority
            deadline_hours: Hours until deadline
            failure_context: Context about why we're delegating

        Returns:
            Dict with request status, or None on error
        """
        now = int(time.time())
        request_id = f"task_{self.our_pubkey[:8]}_{now}_{target_member_id[:8]}"
        deadline_timestamp = now + (deadline_hours * 3600)

        # Create the message
        msg = create_task_request(
            requester_id=self.our_pubkey,
            request_id=request_id,
            timestamp=now,
            task_type=task_type,
            task_params=task_params,
            priority=priority,
            deadline_timestamp=deadline_timestamp,
            rpc=rpc,
            failure_context=failure_context
        )

        if not msg:
            self._log(f"Failed to create task request message", level='warn')
            return None

        # Send to target member
        try:
            rpc.call("sendcustommsg", {
                "node_id": target_member_id,
                "msg": msg.hex()
            })
        except Exception as e:
            self._log(f"Failed to send task request to {target_member_id[:16]}...: {e}", level='warn')
            return None

        # Record in database
        self.db.create_outgoing_task_request(
            request_id=request_id,
            target_member_id=target_member_id,
            task_type=task_type,
            task_target=task_params.get('target', ''),
            amount_sats=task_params.get('amount_sats'),
            priority=priority,
            deadline_timestamp=deadline_timestamp,
            failure_context=json.dumps(failure_context) if failure_context else None
        )

        self._log(
            f"Sent task request {request_id} to {target_member_id[:16]}... "
            f"(type={task_type}, target={task_params.get('target', '')[:16]}...)"
        )

        return {
            "request_id": request_id,
            "target_member": target_member_id,
            "task_type": task_type,
            "status": "sent"
        }

    def request_channel_open_delegation(
        self,
        target_peer: str,
        channel_size_sats: int,
        rpc,
        failure_context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Request another hive member to open a channel to a peer.

        This is a convenience method for the most common delegation case.

        Args:
            target_peer: Peer we want a channel opened to
            channel_size_sats: Desired channel size
            rpc: RPC proxy
            failure_context: Why we couldn't open it ourselves

        Returns:
            Dict with delegation status
        """
        # Find capable hive members
        members = self.db.get_all_members()
        capable_members = []

        for member in members:
            member_id = member.get('peer_id')
            if not member_id or member_id == self.our_pubkey:
                continue

            # Check member health
            health = self.db.get_member_health(member_id)
            if health:
                overall_health = health.get('overall_health', 0)
                can_help = health.get('can_help_others', False)
                if overall_health >= 50 and can_help:
                    capable_members.append({
                        "peer_id": member_id,
                        "health": overall_health,
                        "tier": health.get('tier', 'unknown')
                    })

        if not capable_members:
            self._log("No capable members available for channel open delegation", level='debug')
            return {"status": "no_capable_members"}

        # Sort by health (highest first)
        capable_members.sort(key=lambda m: m['health'], reverse=True)

        # Send to top candidate
        best_member = capable_members[0]
        result = self.request_task(
            target_member_id=best_member['peer_id'],
            task_type=TASK_TYPE_EXPAND_TO,
            task_params={
                "target": target_peer,
                "amount_sats": channel_size_sats
            },
            rpc=rpc,
            priority=TASK_PRIORITY_NORMAL,
            failure_context=failure_context
        )

        if result:
            return {
                "status": "delegation_requested",
                "request_id": result.get("request_id"),
                "delegated_to": best_member['peer_id'][:16] + "...",
                "delegated_to_health": best_member['health']
            }
        else:
            return {"status": "delegation_failed"}

    # =========================================================================
    # INCOMING TASK REQUESTS
    # =========================================================================

    def handle_task_request(
        self,
        sender_id: str,
        payload: Dict[str, Any],
        rpc
    ) -> Dict[str, Any]:
        """
        Handle an incoming TASK_REQUEST message.

        Args:
            sender_id: Peer who sent the request
            payload: Message payload
            rpc: RPC proxy for verification and response

        Returns:
            Dict with handling result
        """
        # Rate limit check
        if not self._check_rate_limit(sender_id, self._request_rate, TASK_REQUEST_RATE_LIMIT):
            self._log(f"Rate limited task request from {sender_id[:16]}...")
            return {"error": "rate_limited"}

        # Validate payload
        if not validate_task_request_payload(payload):
            self._log(f"Invalid task request payload from {sender_id[:16]}...")
            return {"error": "invalid_payload"}

        # Verify requester matches sender
        requester_id = payload.get("requester_id")
        if requester_id != sender_id:
            self._log(f"Task request requester mismatch: {requester_id[:16]}... != {sender_id[:16]}...")
            return {"error": "requester_mismatch"}

        # Verify sender is a hive member
        member = self.db.get_member(requester_id)
        if not member:
            self._log(f"Task request from non-member {requester_id[:16]}...")
            return {"error": "not_a_member"}

        # Verify signature
        signature = payload.get("signature")
        signing_msg = get_task_request_signing_payload(payload)

        try:
            verify_result = rpc.checkmessage(signing_msg, signature)
            if not verify_result.get("verified"):
                self._log("Task request signature verification failed")
                return {"error": "invalid_signature"}
            if verify_result.get("pubkey") != requester_id:
                self._log("Task request signature pubkey mismatch")
                return {"error": "signature_mismatch"}
        except Exception as e:
            self._log(f"Signature verification error: {e}", level='error')
            return {"error": "verification_failed"}

        # Record for rate limiting
        self._record_message(sender_id, self._request_rate)

        # Check if we can accept this task
        request_id = payload.get("request_id")
        task_type = payload.get("task_type")
        task_params = payload.get("task_params", {})

        # Check capacity
        active_tasks = self.db.count_active_incoming_tasks()
        if active_tasks >= MAX_PENDING_TASKS:
            self._send_task_response(
                requester_id, request_id, TASK_STATUS_REJECTED,
                rpc, reason=TASK_REJECT_BUSY
            )
            return {"status": "rejected", "reason": "busy"}

        # Task-specific checks
        if task_type == TASK_TYPE_EXPAND_TO:
            can_accept, reject_reason = self._can_accept_expand_task(task_params, rpc)
            if not can_accept:
                self._send_task_response(
                    requester_id, request_id, TASK_STATUS_REJECTED,
                    rpc, reason=reject_reason
                )
                return {"status": "rejected", "reason": reject_reason}

        # Accept the task
        self.db.create_incoming_task_request(
            request_id=request_id,
            requester_id=requester_id,
            task_type=task_type,
            task_target=task_params.get('target', ''),
            amount_sats=task_params.get('amount_sats'),
            priority=payload.get('priority', TASK_PRIORITY_NORMAL),
            deadline_timestamp=payload.get('deadline_timestamp'),
            failure_context=json.dumps(payload.get('failure_context')) if payload.get('failure_context') else None
        )

        self.db.update_incoming_task_status(request_id, 'accepted')

        # Send acceptance
        self._send_task_response(
            requester_id, request_id, TASK_STATUS_ACCEPTED, rpc
        )

        self._log(
            f"Accepted task {request_id} from {requester_id[:16]}... "
            f"(type={task_type}, target={task_params.get('target', '')[:16]}...)"
        )

        # Route through governance engine for approval
        if self.decision_engine:
            try:
                context = {
                    "action": "delegated_task_execute",
                    "task_type": task_type,
                    "task_params": task_params,
                    "requester_id": requester_id,
                    "request_id": request_id,
                }
                decision = self.decision_engine.propose_action(
                    action_type="channel_open" if task_type == TASK_TYPE_EXPAND_TO else "delegated_task",
                    target=task_params.get("target", requester_id),
                    context=context,
                )
                # In advisor mode, this queues to pending_actions — do NOT execute
                if not getattr(decision, "approved", False):
                    self._log(
                        f"Task {request_id} queued for governance approval "
                        f"(mode={getattr(decision, 'mode', 'unknown')})"
                    )
                    self.db.update_incoming_task_status(request_id, "pending_approval")
                    return {"status": "pending_approval", "request_id": request_id}
            except Exception as e:
                self._log(f"Governance check failed for task {request_id}: {e}", level='error')
                # Fail closed: do not execute without governance approval
                self.db.update_incoming_task_status(request_id, "pending_approval")
                return {"status": "pending_approval", "request_id": request_id}
        else:
            # No decision engine available — fail closed, queue for manual review
            self._log(
                f"No governance engine — task {request_id} queued for manual approval",
                level='warn'
            )
            self.db.update_incoming_task_status(request_id, "pending_approval")
            return {"status": "pending_approval", "request_id": request_id}

        # Only reaches here if governance explicitly approved (failsafe emergency)
        self._execute_task(request_id, task_type, task_params, requester_id, rpc)

        return {"status": "accepted", "request_id": request_id}

    def _can_accept_expand_task(
        self,
        task_params: Dict[str, Any],
        rpc
    ) -> tuple:
        """
        Check if we can accept an expand_to task.

        Returns:
            (can_accept: bool, reject_reason: str or None)
        """
        target = task_params.get('target')
        amount_sats = task_params.get('amount_sats', 0)

        # Check if we have enough funds
        try:
            funds = rpc.listfunds()
            outputs = funds.get('outputs', [])
            confirmed_sats = sum(
                o.get('amount_msat', 0) // 1000
                for o in outputs
                if o.get('status') == 'confirmed'
            )

            # Need amount + reserve
            required = amount_sats + 100000  # 100k reserve
            if confirmed_sats < required:
                return (False, TASK_REJECT_NO_FUNDS)
        except Exception:
            return (False, TASK_REJECT_NO_FUNDS)

        # Check if we can connect to the target
        try:
            # Try to get peer info (doesn't actually connect)
            peers = rpc.listpeers(target)
            # If we already have a channel, that's fine
        except Exception:
            pass  # Connection check happens during execution

        return (True, None)

    def _execute_task(
        self,
        request_id: str,
        task_type: str,
        task_params: Dict[str, Any],
        requester_id: str,
        rpc
    ):
        """
        Execute an accepted task.

        Args:
            request_id: Task request ID
            task_type: Type of task
            task_params: Task parameters
            requester_id: Who requested the task
            rpc: RPC proxy
        """
        if task_type == TASK_TYPE_EXPAND_TO:
            self._execute_expand_task(request_id, task_params, requester_id, rpc)
        else:
            self._log(f"Unknown task type: {task_type}", level='warn')
            self.db.update_incoming_task_status(
                request_id, 'failed', failure_reason=f"Unknown task type: {task_type}"
            )
            self._send_task_response(
                requester_id, request_id, TASK_STATUS_FAILED,
                rpc, reason=f"Unknown task type: {task_type}"
            )

    def _execute_expand_task(
        self,
        request_id: str,
        task_params: Dict[str, Any],
        requester_id: str,
        rpc
    ):
        """Execute a channel open task."""
        target = task_params.get('target')
        amount_sats = task_params.get('amount_sats')

        if not target or amount_sats is None:
            self._log("Invalid expand task params: missing target or amount_sats", level='error')
            self.db.update_incoming_task_status(
                request_id, 'failed',
                result_data=json.dumps({"error": "missing target or amount_sats"})
            )
            return

        self._log(f"Executing expand_to task: {target[:16]}... for {amount_sats} sats")

        try:
            # Attempt to open the channel
            result = rpc.fundchannel(target, amount_sats, announce=True)

            # Success!
            txid = result.get('txid', '')
            channel_id = result.get('channel_id', '')

            self.db.update_incoming_task_status(
                request_id, 'completed',
                result_data=json.dumps({
                    "txid": txid,
                    "channel_id": channel_id,
                    "amount_sats": amount_sats
                })
            )

            self._send_task_response(
                requester_id, request_id, TASK_STATUS_COMPLETED,
                rpc, result={
                    "txid": txid,
                    "channel_id": channel_id,
                    "amount_sats": amount_sats
                }
            )

            self._log(
                f"Completed expand_to task {request_id}: "
                f"opened channel {channel_id[:16]}... to {target[:16]}..."
            )

        except Exception as e:
            error_msg = str(e)
            self._log(f"Failed expand_to task {request_id}: {error_msg}", level='warn')

            self.db.update_incoming_task_status(
                request_id, 'failed', failure_reason=error_msg
            )

            self._send_task_response(
                requester_id, request_id, TASK_STATUS_FAILED,
                rpc, reason=error_msg
            )

    def _send_task_response(
        self,
        requester_id: str,
        request_id: str,
        status: str,
        rpc,
        reason: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None
    ):
        """Send a task response message."""
        now = int(time.time())

        msg = create_task_response(
            responder_id=self.our_pubkey,
            request_id=request_id,
            timestamp=now,
            status=status,
            rpc=rpc,
            reason=reason,
            result=result
        )

        if not msg:
            self._log(f"Failed to create task response", level='warn')
            return

        try:
            rpc.call("sendcustommsg", {
                "node_id": requester_id,
                "msg": msg.hex()
            })
        except Exception as e:
            self._log(f"Failed to send task response to {requester_id[:16]}...: {e}", level='warn')

    # =========================================================================
    # INCOMING TASK RESPONSES
    # =========================================================================

    def handle_task_response(
        self,
        sender_id: str,
        payload: Dict[str, Any],
        rpc
    ) -> Dict[str, Any]:
        """
        Handle an incoming TASK_RESPONSE message.

        Args:
            sender_id: Peer who sent the response
            payload: Message payload
            rpc: RPC proxy for verification

        Returns:
            Dict with handling result
        """
        # Rate limit check
        if not self._check_rate_limit(sender_id, self._response_rate, TASK_RESPONSE_RATE_LIMIT):
            self._log(f"Rate limited task response from {sender_id[:16]}...")
            return {"error": "rate_limited"}

        # Validate payload
        if not validate_task_response_payload(payload):
            self._log(f"Invalid task response payload from {sender_id[:16]}...")
            return {"error": "invalid_payload"}

        # Verify responder matches sender
        responder_id = payload.get("responder_id")
        if responder_id != sender_id:
            self._log(f"Task response responder mismatch")
            return {"error": "responder_mismatch"}

        # Verify signature
        signature = payload.get("signature")
        signing_msg = get_task_response_signing_payload(payload)

        try:
            verify_result = rpc.checkmessage(signing_msg, signature)
            if not verify_result.get("verified"):
                self._log("Task response signature verification failed")
                return {"error": "invalid_signature"}
            if verify_result.get("pubkey") != responder_id:
                self._log("Task response signature pubkey mismatch")
                return {"error": "signature_mismatch"}
        except Exception as e:
            self._log(f"Signature verification error: {e}", level='error')
            return {"error": "verification_failed"}

        # Record for rate limiting
        self._record_message(sender_id, self._response_rate)

        # Find the original request
        request_id = payload.get("request_id")
        task = self.db.get_outgoing_task(request_id)

        if not task:
            self._log(f"Task response for unknown request: {request_id}")
            return {"error": "unknown_request"}

        # Update our tracking
        status = payload.get("status")
        reason = payload.get("reason")
        result = payload.get("result")

        self.db.update_outgoing_task_response(
            request_id=request_id,
            response_status=status,
            response_reason=reason,
            result_data=json.dumps(result) if result else None
        )

        self._log(
            f"Received task response for {request_id}: status={status}"
            + (f", reason={reason}" if reason else "")
        )

        return {
            "status": "processed",
            "request_id": request_id,
            "response_status": status,
            "result": result
        }
