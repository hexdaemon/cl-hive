# MCP Hive Server Review And Hardening Plan

Targets:
- `tools/mcp-hive-server.py` (MCP server / tool surface / node transport)
- `tools/advisor_db.py` (SQLite advisor DB used by MCP tools)
- Any `tools/*.py` modules imported by the MCP server (proactive advisor stack)

Goal:
- Reduce correctness risk (deadlocks, hangs, inconsistent results)
- Reduce security risk (path traversal, dangerous RPC access, credential leakage)
- Improve operability (timeouts, retries, clearer errors, structured output)
- Improve maintainability (reduce gigantic if/elif dispatch, shared helpers, tests)


## Findings (Bugs / Risks)

### P0: Blocking Docker Calls In Async Context
File: `tools/mcp-hive-server.py`
- `NodeConnection._call_docker()` uses `subprocess.run(...)` directly.
- This blocks the asyncio event loop for up to 30s (or more if the process stalls), impacting *all* concurrent MCP tool calls.

Impact:
- Latency spikes; "server feels hung"; timeouts that look like MCP/Claude issues but are actually event loop starvation.


### P0: Strategy Prompt Loader Is Path-Traversal Prone
File: `tools/mcp-hive-server.py`
- `load_strategy(name)` builds `path = os.path.join(STRATEGY_DIR, f\"{name}.md\")`.
- If `name` can be influenced (directly or indirectly) and contains `../`, it can read files outside `STRATEGY_DIR`.
- Even if currently only used with fixed names, this is a footgun.


### P0: AdvisorDB Connection Caching Is Unsafe Under Async Concurrency
File: `tools/advisor_db.py`
- Uses `threading.local()` and caches a single SQLite connection per thread in `_get_conn()`.
- MCP server handlers are async; multiple concurrent tool calls on the same event loop run in the same thread and can overlap DB access.
- SQLite connections are not re-entrant; this can produce intermittent errors ("recursive cursor", "database is locked") or subtle corruption risk.


### P1: Overly Strict Envelope Version Rejection For Node REST Calls (Operational)
File: `tools/mcp-hive-server.py`
- Not a protocol bug, but a UX problem: many node calls simply forward whatever REST returns.
- When errors happen, they are returned as raw dicts with inconsistent shapes.
- `HIVE_NORMALIZE_RESPONSES` exists but is off by default; callers can’t rely on output shape.


### P1: Handler Dispatch Is Large, Hard To Audit, Easy To Break
File: `tools/mcp-hive-server.py`
- `call_tool()` is a massive `if/elif` chain.
- Adding tools can introduce unreachable branches, duplicated names, or inconsistent validation patterns.


### P1: Heavy Node RPC Sequences Are Mostly Serial
File: `tools/mcp-hive-server.py`
- Some handlers call multiple RPCs sequentially per node (example: fleet snapshot, advisor snapshot recording).
- This inflates latency and increases chance of timeouts.


### P2: Incomplete Input Validation / Guardrails
File: `tools/mcp-hive-server.py`
- Tools can trigger actions (`approve`, `reject`, rebalances, fee changes, etc).
- There is no explicit allowlist/denylist for sensitive operations beyond "whatever tools exist".
- In docker mode, `_call_docker()` will run any `lightning-cli METHOD` requested by the tool handler.

This might be intended, but if the MCP server is reused beyond trusted environments, it becomes a sharp edge.


## Hardening Plan (Staged)

### Stage 0: Add Tests Before Refactors (1-2 PRs)
1. Add unit tests for:
   - Strategy loader sanitization (no traversal).
   - Docker call wrapper uses async subprocess or executor.
   - AdvisorDB concurrency: parallel tasks do not throw and results are consistent.
2. Add a "tool registry" test:
   - Verifies `list_tools()` names are unique.
   - Verifies each tool name has a callable handler.

Deliverables:
- `tests/test_mcp_hive_server.py` (new).
- Minimal mocks for `NodeConnection.call()` and `AdvisorDB`.


### Stage 1 (P0): Fix Docker Blocking (Async Subprocess)
File: `tools/mcp-hive-server.py`
1. Replace `subprocess.run(...)` with one of:
   - `asyncio.create_subprocess_exec(...)` + `await proc.communicate()`
   - Or `await asyncio.to_thread(subprocess.run, ...)` as an interim fix.
2. Enforce timeouts:
   - Keep per-call timeout, but ensure the asyncio task is not blocked by sync subprocess.
3. Return structured error output that includes:
   - exit code
   - stderr snippet (bounded)
   - command (redacted if necessary)

Acceptance:
- Running docker-mode calls does not block other tool calls.


### Stage 2 (P0): Fix `load_strategy()` Path Traversal
File: `tools/mcp-hive-server.py`
1. Sanitize `name`:
   - Allow only `[a-zA-Z0-9_-]+` and reject others.
2. Resolve and enforce directory boundary:
   - `Path(STRATEGY_DIR).resolve()` and `Path(path).resolve()` must be under it.
3. Open with explicit encoding and errors mode:
   - `open(..., encoding="utf-8", errors="replace")`.

Acceptance:
- Attempted traversal returns empty string and logs at debug/warn.


### Stage 3 (P0): Make AdvisorDB Async-Safe
File: `tools/advisor_db.py`
Pick one of these approaches (recommended order):

Option A (simple, safe): serialize DB access with a lock
1. Add `self._lock = threading.Lock()` (or `asyncio.Lock` at the call site).
2. In every public method, wrap DB operations with the lock.
3. Keep WAL mode.

Option B (better for concurrency): no cached connections; one connection per operation
1. Remove thread-local caching and create a new connection in `_get_conn()`.
2. Set `timeout=...` and `isolation_level=None` if appropriate.

Option C (async-native): use `aiosqlite`
1. Convert AdvisorDB to async methods.
2. Keep a single connection and serialize access via a queue/lock.

Acceptance:
- Parallel MCP tool calls involving AdvisorDB do not error.


### Stage 4 (P1): Tool Dispatch Refactor (Registry)
File: `tools/mcp-hive-server.py`
1. Replace `if/elif` chain with a mapping:
   - `TOOL_HANDLERS: dict[str, Callable[[dict], Awaitable[dict]]]`
2. Enforce a consistent argument validation pattern:
   - `require_fields(args, [...])`
   - `get_node_or_error(fleet, node_name)`
3. Centralize normalization:
   - Make `HIVE_NORMALIZE_RESPONSES` default to true, or always normalize and keep raw under `details`.

Acceptance:
- Adding tools is one-line registration.
- Unknown tools return consistent error shape.


### Stage 5 (P1): Performance Improvements (Parallelize Node RPCs)
File: `tools/mcp-hive-server.py`
1. Convert serial per-node RPC chains to parallel groups with bounded concurrency:
   - `asyncio.gather(...)` for independent calls.
   - A per-node semaphore to prevent overloading nodes.
2. Add per-tool time budgets:
   - Fail fast with partial results rather than hanging.

Acceptance:
- Fleet snapshot and advisor snapshot tools are noticeably faster on multi-node configs.


### Stage 6 (P2): Guardrails And Secrets Hygiene
Files: `tools/mcp-hive-server.py`, config docs
1. Ensure runes and sensitive headers are never logged.
2. Optional allowlist mode:
   - `HIVE_ALLOWED_METHODS=/path/to/allowlist.json` for node RPC methods.
3. Add "dry-run" variants for destructive actions where possible.

Acceptance:
- Accidentally enabling debug logs does not expose runes.


## Quick “Fix Now” Candidates (Low Risk / High Value)
1. Replace deprecated `asyncio.get_event_loop()` usage with `asyncio.get_running_loop()` in async fns.
2. Add environment-configurable HTTP timeouts (connect/read/write) rather than a single `timeout=30.0`.
3. Normalize msat extraction everywhere through `_extract_msat()` (already exists) and remove ad-hoc parsing.


## Proposed Outputs / Docs Updates
1. Add a short section to `docs/MCP_SERVER.md` describing:
   - docker vs REST mode tradeoffs
   - recommended safety env vars (`HIVE_ALLOW_INSECURE_TLS`, `HIVE_ALLOW_INSECURE_HTTP`)
   - expected timeout behavior
2. Add `tools/README.md` describing the tool stack and how to run tests.
