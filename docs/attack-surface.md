# Attack Surface Map (Initial)

Date: 2026-01-31
Scope: cl-hive plugin + tools

## Primary Entry Points (Untrusted Inputs)
1. CLN custom messages (BOLT8) via `@plugin.hook("custommsg")` in `cl-hive.py`.
2. CLN peer lifecycle notifications via `@plugin.subscribe("connect")` and `@plugin.subscribe("disconnect")`.
3. CLN forward events via `@plugin.subscribe("forward_event")`.
4. CLN peer connection hook via `@plugin.hook("peer_connected")` (autodiscovery).
5. Local RPC commands via `@plugin.method("hive-*")` (assume local admin, but treat as attackable if CLN RPC is exposed).
6. Dynamic configuration via `setconfig` + `hive-reload-config`.

## External Network Dependencies
- Lightning RPC: `pyln.client` RPC calls and `lightning-cli` in `modules/bridge.py`.
- External HTTP calls: `tools/external_peer_intel.py` (1ml.com; TLS verify disabled) and `tools/mcp-hive-server.py` (httpx to LNbits and other endpoints).

## Persistence / Storage Surfaces
- SQLite: `modules/database.py` (member state, pending actions, tasks, settlements, reports).
- On-disk config: plugin options stored in CLN; internal config in `modules/config.py`.
- Logs: plugin log output (potentially untrusted input echoed).

## Message Serialization / Validation
- Protocol framing: `modules/protocol.py` (magic prefix, type dispatch, size limits, signature payloads).
- Handshake auth: `modules/handshake.py` (challenge/attest, rate limits).
- Relay metadata + dedup: `modules/relay.py`.
- Gossip processing: `modules/gossip.py`.
- Task delegation: `modules/task_manager.py` + task message types in `modules/protocol.py`.
- Settlement + splice coordination: `modules/settlement.py`, `modules/splice_manager.py`, `modules/splice_coordinator.py`.

## Background Threads / Timers (Concurrency Surfaces)
- Planner, gossip loop, health/metrics, task processing, and other background cycles in `cl-hive.py` and related managers.
- Thread-safe RPC wrapper uses a global lock (`RPC_LOCK`) in `cl-hive.py`.

## High-Risk Modules (Initial Triage)
- `cl-hive.py`: custommsg dispatch, RPC methods, hooks/subscriptions.
- `modules/protocol.py`: deserialization, limits, signature payloads.
- `modules/handshake.py`: identity proof + replay/nonce handling.
- `modules/gossip.py` + `modules/relay.py`: message amplification and dedup.
- `modules/state_manager.py` + `modules/database.py`: state integrity + persistence.
- `modules/task_manager.py`: task request/response validation.
- `modules/settlement.py` + `modules/splice_manager.py`: funds/PSBT safety.
- `modules/vpn_transport.py`: transport policy enforcement.
- `modules/bridge.py`: RPC proxy + shelling out to `lightning-cli`.
- `tools/external_peer_intel.py`: external HTTP with weak TLS.
- `tools/mcp-hive-server.py`: external HTTP client and tool exposure.

## Immediate Triage Questions
- Are all custommsg handlers enforcing `sender_id`/signature/permission binding?
- Are size, depth, and list limits applied to every incoming payload?
- Are replay protections enforced for signed messages?
- Are RPC methods gated by membership tier where required?
- Are background tasks bounded to prevent CPU/Disk amplification?
