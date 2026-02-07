# cl-hive Red Team Plan

Date: 2026-01-31
Owner: Security Lead & Maintainer AI

## Mission
Survive the audit by identifying, reproducing, and fixing vulnerabilities with minimal, auditable changes and regression tests.

## Rules (Security Workflow)
- Reproduction first: no code changes until a test exists under `tests/security/`.
- Fail closed: ambiguous inputs or compromised subsystems must shut down and log.
- No silent patches: every fix requires a GitHub issue and a clear commit message describing impact.
- Identity & auth: re-verify `sender_id`, `signatures`, and `db_permissions` on every frame.
- Resource bounding: validate JSON depth, list length, log rotation, and disk/memory caps.

## Phases
1. Recon
   - Map entry points and trust boundaries
   - Inventory message formats and persistence paths
   - Exit: attack surface doc + protocol/schema inventory

2. Auth & Identity
   - Verify bindings per frame
   - Replay protection and session fixation checks
   - Exit: all binding tests green with negative cases

3. Resource DoS
   - OOM, disk fill, log storms
   - JSON depth/size, list length, timeout caps
   - Exit: hard limits enforced and tested

4. Concurrency & State
   - Races, duplicate execution, partial writes
   - Exit: invariant tests catch races

5. Logic & Policy
   - Governance, routing, liquidity, fee logic abuse
   - Exit: exploit paths blocked with tests

6. Regression
   - Run security tests and baseline suite
   - Exit: all tests pass

## Subagent Assignments
- Agent A (Crypto/Protocol): handshake, protocol framing, transport, settlement
  - `modules/handshake.py`, `modules/protocol.py`, `modules/vpn_transport.py`, `modules/relay.py`, `modules/settlement.py`
- Agent B (Concurrency/State): locks, DB consistency, gossip vectors
  - `modules/state_manager.py`, `modules/database.py`, `modules/task_manager.py`, `modules/gossip.py`, `modules/routing_pool.py`
- Agent C (Systems/Resources): memory/disk/logs/metrics
  - `modules/health_aggregator.py`, `modules/network_metrics.py`, logging paths in `cl-hive.py`
- Agent D (QA/Exploit): PoCs + regression tests
  - `tests/security/`

## Triage Output Format
Use the GH CLI to create security issues:

```bash
gh issue create --title "[SECURITY] {Component}: {Short Description}" --label "security,red-team,severity-{level}" --body "
**Vulnerability:** {Explanation of the flaw}
**Severity:** {Critical/High/Medium/Low}
**Affected Files:** ...
**Reproduction Plan:** Create a test case in `tests/security/test_exploit_{id}.py` that triggers {bad behavior}.
**Fix Criteria:**
1. The test case passes.
2. No global lock contention introduced.
"
```

## Exit Criteria
- All security issues have:
  - Reproduction test in `tests/security/`
  - Fix patch with minimal changes
  - Clear commit message describing impact
  - Issue updated in vulnerability register
