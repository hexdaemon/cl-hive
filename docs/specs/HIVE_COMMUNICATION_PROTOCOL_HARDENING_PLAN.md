# Hive Communication Protocol Hardening Plan

This document is a concrete, staged plan to harden cl-hive's fleet communication protocol (BOLT 8 `custommsg` overlay + optional relay), fix known correctness/reliability bugs, and make upgrades safe across heterogeneous fleet versions.

Scope:
- Transport: how bytes move between hive members
- Messaging: envelope, message identity, signing, schema/units
- Reliability: dedup, replay protection, acks/retries, persistence, chunking
- Observability: protocol metrics, tracing, and operator tooling

Non-goals (for this plan):
- Replacing Lightning transport entirely with an external bus
- Changing business logic algorithms (planner/MCF/etc) except where needed for protocol correctness


## Current State Summary

Transport:
- cl-hive uses CLN's `sendcustommsg` and `custommsg` hook (BOLT 8 encrypted peer-to-peer transport).
- Messages are encoded as: `HIVE_MAGIC` (4 bytes) + JSON envelope (`modules/protocol.py`).

Envelope:
- `serialize()` wraps a `{type, version, payload}` JSON object and prepends `b'HIVE'`.
- `deserialize()` rejects any envelope whose `version != PROTOCOL_VERSION`.

Relay:
- Some messages are relayed with `_relay` metadata (TTL and relay path) via `RelayManager` (`modules/relay.py`).
- Deduplication is in-memory only with a short expiry window (defaults: 5 minutes, max 10k message IDs).

Signing:
- Many message types have custom signing payload rules in `modules/protocol.py`.
- Verification is implemented in handlers using CLN `checkmessage`.
- Not all message types have uniform requirements for `sender_id`, timestamps, or idempotency keys.


## Problems To Fix (Bugs + Design Gaps)

### P0: Upgrade Safety / Fleet Partition Risk
- `deserialize()` drops messages when `version != PROTOCOL_VERSION`, which creates hard partitions during rolling upgrades.

### P0: Weak Idempotency and Replay Protection
- Relay dedup is memory-only; node restart can re-process old events.
- `msg_id` is derived from the full payload (excluding `_relay`) which often includes timestamps; semantically identical events can still re-broadcast with different IDs.
- Many state-changing operations do not use a stable `event_id`/`op_id` that is persisted and enforced as unique.

### P0: Missing Reliability Guarantees for Critical Messages
- `sendcustommsg` is best-effort; there are no receipts/acks and no retransmission.
- There is no durable outbox; restarts lose pending operations.

### P1: Canonical Units and Schema Drift
- Some fields are inconsistently represented (example class: uptime in 0..1 vs 0..100 vs integer percent).
- A canonical units table is missing from the spec, and validation is inconsistent.

### P1: Payload Size / Chunking / Flow Control
- Large "batch" messages risk approaching size limits with no chunking or compression strategy.
- There is no per-peer/per-message-type rate limiting at the protocol layer.

### P2: Observability Gaps
- Operators cannot easily answer: "What messages are failing? Who is spamming? Which peers are behind?"
- There is no cross-message tracing identifier in logs.


## Design Principles (What "Good" Looks Like)

1. Backward-compatible upgrades:
- A fleet with mixed versions must continue to communicate (degraded features allowed).

2. Deterministic idempotency:
- Every state-changing message has a stable, unique `event_id` with DB-enforced uniqueness.

3. Reliability where needed:
- Critical workflows have ack/retry with a durable outbox and bounded retries.
- Non-critical telemetry remains best-effort.

4. Tight schemas:
- Canonical units and bounds are defined, validated, and tested.

5. Security posture:
- Replay protection and rate limiting exist at the protocol edge.
- Signatures bind to the fields that define semantic meaning, not to incidental transport details.


## Proposed Architecture (Incremental, Not a Rewrite)

### Layer 1: Envelope v2 (Additive)
Introduce an "envelope v2" with stable message identity and uniform signing hooks, while still accepting the current v1 envelope.

Envelope v2 fields:
- `type`: int (HiveMessageType)
- `v`: int (envelope version, not equal to app schema)
- `sender_id`: pubkey of signer/originator
- `ts`: unix seconds (origin timestamp)
- `msg_id`: 32 hex chars (stable ID for dedup and ack)
- `body`: dict (message-type-specific content)
- `sig`: zbase signature over canonical signing payload

Rules:
- `msg_id` is derived from canonical content excluding transport metadata and excluding fields expected to vary between retries (example: omit relay hop data).
- Receivers can enforce "accept window" for `ts` to mitigate replay.
- Signatures always cover: `type`, `sender_id`, `ts`, `msg_id`, and a hash of the canonicalized `body`.

Compatibility:
- Continue to accept v1 envelopes (`{type, version, payload}`) for a full deprecation window.
- Emit v2 envelopes only when peer capability indicates support.

Implementation targets:
- `modules/protocol.py`: new `serialize_v2()` / `deserialize_any()` and canonical signing helpers.
- `cl-hive.py`: dispatch should accept v1 or v2 and normalize to an internal structure.


### Layer 2: Reliability (Ack/Retry + Durable Outbox) For Critical Messages
Add a small, generic reliability layer for message types that must be eventually delivered.

New message types:
- `MSG_ACK`: ack by `msg_id` with status (ok, invalid, retry_later)
- `MSG_NACK`: explicit rejection with reason code (optional, used sparingly)

Outbox:
- Persist outgoing critical messages in DB with status: queued, sent, acked, failed, expired.
- A background loop retries until acked or max retry/time budget is exceeded.

Inbox:
- Persist "processed event ids" for critical state-changing events (longer than 5 minutes).
- For v2, persist `msg_id` and `sender_id` with a TTL policy.

Retry policy:
- Exponential backoff with jitter.
- Bounded concurrency per peer to avoid floods.

Implementation targets:
- `modules/database.py`: new tables:
  - `proto_outbox(msg_id PRIMARY KEY, peer_id, type, body_json, sent_at, retry_count, status, last_error, expires_at)`
  - `proto_inbox_dedup(sender_id, msg_id, first_seen_at, PRIMARY KEY(sender_id, msg_id))`
  - `proto_events(event_id PRIMARY KEY, type, actor_id, created_at)` for idempotent operations
- `cl-hive.py`: new background loop for outbox retries.
- `modules/protocol.py`: message constructors + validation for `MSG_ACK`.


### Layer 3: Chunking For Large Payloads (Optional, Only If Needed)
Add chunking for batch payloads that can exceed size limits.

New message types:
- `MSG_CHUNK`: `{chunk_id, idx, total, inner_type, inner_hash, data_b64}`
- `MSG_CHUNK_ACK`: optional for controlling resends

Rules:
- Reassemble only if all chunks arrive within a time window.
- Verify `inner_hash` before dispatching the reconstructed message.

Implementation targets:
- `modules/protocol.py`: chunk encode/decode helpers.
- `modules/database.py`: temporary chunk assembly storage with expiry.


## Detailed Work Plan (Phases)

### Phase A: Protocol Audit and Spec Freeze (No Behavior Change)
Goals:
- Capture current behavior and standardize canonical units and signing rules.

Tasks:
1. Generate a protocol matrix (message type, handler, signed, relayed, idempotency key).
2. Write a canonical "units and bounds" table for all payload fields used in protocol messages.
3. Add tests for validators to enforce units/bounds (start with top 10 message types by importance).

Acceptance:
- A new doc exists in `docs/specs/` and is reviewed.
- Validators match the doc for the audited set.


### Phase B: Fix Versioning Partition Risk (Backward-Compatible)
Goals:
- Stop hard-failing on envelope version mismatch.

Tasks:
1. Change `deserialize()` behavior:
   - Accept `version` in an allowed set (example: 1..N) or treat it as informational if the envelope parses.
   - Gate features by handshake capabilities, not by rejecting messages at decode time.
2. Add a handshake capability field:
   - Add `supported_protocol_versions` or `features` list to HELLO/ATTEST.
   - Persist peer capabilities in DB.

Acceptance:
- Mixed-version nodes can continue to exchange core messages.


### Phase C: Deterministic Idempotency (Critical State-Changing Flows)
Goals:
- Ensure restarts and duplicates cannot cause double-apply.

Tasks:
1. For each state-changing message family (promotion, bans, splice, settlement, tasks):
   - Define `event_id` rules (stable, unique).
   - Enforce DB uniqueness.
2. Update handlers to:
   - Check event_id before applying side effects.
   - Return early on duplicates.
3. Extend relay dedup logic:
   - Use `event_id` preferentially when present.

Acceptance:
- Restart replay tests do not double-apply membership/promotions/bans.


### Phase D: Reliable Delivery For Critical Messages (Ack/Retry + Outbox)
Goals:
- Make critical workflows eventually deliver within bounds.

Tasks:
1. Implement `MSG_ACK` and outbox persistence.
2. Mark critical message types as "reliable" and route via outbox sending.
3. Implement receiver-side ack emission:
   - Ack only after validation and persistence.
4. Add backpressure:
   - Per-peer max in-flight reliable messages.

Acceptance:
- Integration tests simulate dropped messages and show eventual convergence.


### Phase E: Chunking (Only If Needed After Measuring)
Goals:
- Handle large batches without silent failure or truncation.

Tasks:
1. Identify batch messages that exceed safe size thresholds in real operation.
2. Implement chunking only for those message types.
3. Add size-based auto-chunking and reassembly tests.

Acceptance:
- Large batches deliver successfully under size constraints.


### Phase F: Observability and Operator Controls
Goals:
- Make protocol health visible and debuggable.

Tasks:
1. Add protocol metrics in DB:
   - per-peer message counts, rejects, acks, retry counts.
2. Add RPC commands:
   - `hive-proto-stats`, `hive-proto-outbox`, `hive-proto-peer <id>`
3. Add structured logging:
   - Include `msg_id`, `event_id`, `origin`, and `type` in logs.

Acceptance:
- Operators can explain stuck workflows via RPC outputs.



## Suggested Review Checklist

1. Which message types are "critical" (must be reliable)?
2. What is the acceptable delivery time (minutes/hours)?
3. What is the acceptable operational complexity (pure Lightning vs optional VPN vs external bus)?
4. What is the upgrade window and deprecation policy for v1 envelopes?

