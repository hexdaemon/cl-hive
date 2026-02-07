"""
Tests for MCP Hive Server hardening changes.

Covers:
- Strategy loader path traversal sanitization
- Docker call uses async subprocess (not blocking)
- Tool registry uniqueness
- AdvisorDB concurrent access safety
- _normalize_response() coverage
- _extract_msat() coverage
"""

import asyncio
import json
import os
import sys
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Add tools directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

from advisor_db import AdvisorDB


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def temp_db():
    """Create a temporary database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name

    db = AdvisorDB(db_path)
    yield db

    try:
        os.unlink(db_path)
    except Exception:
        pass


@pytest.fixture
def strategy_dir(tmp_path):
    """Create a temporary strategy directory with a test file."""
    strat = tmp_path / "strategies"
    strat.mkdir()
    (strat / "valid-name.md").write_text("# Test Strategy\nThis is valid.")
    (strat / "another_one.md").write_text("# Another\nAlso valid.")
    return str(strat)


# =============================================================================
# Strategy Loader Sanitization Tests (Stage 2)
# =============================================================================

class TestLoadStrategy:
    """Test load_strategy() path traversal prevention."""

    def test_path_traversal_rejected(self, strategy_dir):
        """Paths with ../ should be rejected."""
        from mcp_hive_server_helpers import load_strategy_with_dir
        result = load_strategy_with_dir("../etc/passwd", strategy_dir)
        assert result == ""

    def test_valid_name_works(self, strategy_dir):
        """Valid alphanumeric names with hyphens should load."""
        from mcp_hive_server_helpers import load_strategy_with_dir
        result = load_strategy_with_dir("valid-name", strategy_dir)
        assert "Test Strategy" in result

    def test_slash_rejected(self, strategy_dir):
        """Names containing slashes should be rejected."""
        from mcp_hive_server_helpers import load_strategy_with_dir
        result = load_strategy_with_dir("has/slash", strategy_dir)
        assert result == ""

    def test_dot_dot_rejected(self, strategy_dir):
        """Names with dot-dot sequences should be rejected."""
        from mcp_hive_server_helpers import load_strategy_with_dir
        result = load_strategy_with_dir("..valid", strategy_dir)
        assert result == ""

    def test_empty_name_rejected(self, strategy_dir):
        """Empty names should be rejected."""
        from mcp_hive_server_helpers import load_strategy_with_dir
        result = load_strategy_with_dir("", strategy_dir)
        assert result == ""

    def test_underscore_name_works(self, strategy_dir):
        """Names with underscores should work."""
        from mcp_hive_server_helpers import load_strategy_with_dir
        result = load_strategy_with_dir("another_one", strategy_dir)
        assert "Another" in result

    def test_nonexistent_returns_empty(self, strategy_dir):
        """Non-existent files should return empty string."""
        from mcp_hive_server_helpers import load_strategy_with_dir
        result = load_strategy_with_dir("does-not-exist", strategy_dir)
        assert result == ""

    def test_no_strategy_dir_returns_empty(self):
        """When strategy dir is not set, should return empty."""
        from mcp_hive_server_helpers import load_strategy_with_dir
        result = load_strategy_with_dir("valid-name", "")
        assert result == ""


# =============================================================================
# Docker Call Async Test (Stage 1)
# =============================================================================

class TestDockerCallAsync:
    """Verify _call_docker uses asyncio.create_subprocess_exec."""

    def test_call_docker_is_async(self):
        """_call_docker should use asyncio.create_subprocess_exec, not subprocess.run."""
        import inspect
        # Import the module to check source
        server_path = os.path.join(
            os.path.dirname(__file__), '..', 'tools', 'mcp-hive-server.py'
        )
        with open(server_path, 'r') as f:
            source = f.read()

        # Verify async subprocess is used in _call_docker
        # Find the _call_docker method
        start = source.find("async def _call_docker")
        assert start != -1, "_call_docker method not found"
        # Find the next method or class definition
        next_method = source.find("\n    async def ", start + 1)
        if next_method == -1:
            next_method = source.find("\nclass ", start + 1)
        docker_method = source[start:next_method] if next_method != -1 else source[start:]

        assert "asyncio.create_subprocess_exec" in docker_method, \
            "_call_docker should use asyncio.create_subprocess_exec"
        assert "subprocess.run" not in docker_method, \
            "_call_docker should NOT use blocking subprocess.run"

    def test_call_docker_timeout_handling(self):
        """Docker call should handle timeout gracefully."""
        server_path = os.path.join(
            os.path.dirname(__file__), '..', 'tools', 'mcp-hive-server.py'
        )
        with open(server_path, 'r') as f:
            source = f.read()

        start = source.find("async def _call_docker")
        assert start != -1
        # Verify timeout handling
        next_def = source.find("\n    async def ", start + 1)
        if next_def == -1:
            next_def = source.find("\nclass ", start + 1)
        docker_method = source[start:next_def] if next_def != -1 else source[start:]
        assert "asyncio.TimeoutError" in docker_method
        assert "Command timed out" in docker_method


# =============================================================================
# Tool Registry Uniqueness Test (Stage 4)
# =============================================================================

class TestToolRegistry:
    """Test that all tool names are unique."""

    def test_no_duplicate_tool_names(self):
        """Parse tool names from the source and verify no duplicates."""
        import re
        server_path = os.path.join(
            os.path.dirname(__file__), '..', 'tools', 'mcp-hive-server.py'
        )
        with open(server_path, 'r') as f:
            source = f.read()

        # Find all Tool(name="...") declarations
        tool_names = re.findall(r'Tool\(\s*name="([^"]+)"', source)
        assert len(tool_names) > 0, "No tools found"

        seen = set()
        duplicates = []
        for name in tool_names:
            if name in seen:
                duplicates.append(name)
            seen.add(name)

        assert len(duplicates) == 0, f"Duplicate tool names: {duplicates}"

    def test_registry_covers_all_tools(self):
        """TOOL_HANDLERS should have an entry for every tool except hive_health."""
        import re
        server_path = os.path.join(
            os.path.dirname(__file__), '..', 'tools', 'mcp-hive-server.py'
        )
        with open(server_path, 'r') as f:
            source = f.read()

        # Find all Tool(name="...") declarations
        tool_names = set(re.findall(r'Tool\(\s*name="([^"]+)"', source))
        # hive_health is handled inline
        tool_names.discard("hive_health")

        # Find all TOOL_HANDLERS keys
        handler_keys = set(re.findall(r'"([^"]+)":\s*handle_', source))

        missing = tool_names - handler_keys
        assert len(missing) == 0, f"Tools missing from TOOL_HANDLERS: {missing}"

    def test_unknown_tool_returns_error(self):
        """The call_tool function should return error for unknown tools."""
        server_path = os.path.join(
            os.path.dirname(__file__), '..', 'tools', 'mcp-hive-server.py'
        )
        with open(server_path, 'r') as f:
            source = f.read()

        # Verify the unknown tool handling exists
        assert '"Unknown tool:' in source or "'Unknown tool:" in source


# =============================================================================
# AdvisorDB Concurrent Access Test (Stage 3)
# =============================================================================

class TestAdvisorDBConcurrency:
    """Test AdvisorDB is safe for concurrent async access."""

    def test_fresh_connections_per_operation(self, temp_db):
        """Each _get_conn() call should create a fresh connection."""
        # Call _get_conn twice and verify they work independently
        with temp_db._get_conn() as conn1:
            conn1.execute("SELECT 1")

        with temp_db._get_conn() as conn2:
            conn2.execute("SELECT 1")

    def test_no_threading_local(self, temp_db):
        """AdvisorDB should not use threading.local()."""
        assert not hasattr(temp_db, '_local'), \
            "AdvisorDB should not use threading.local() (_local attribute)"

    def test_concurrent_writes(self, temp_db):
        """Multiple concurrent writes should not error."""
        import concurrent.futures

        def write_snapshot(i):
            report = {
                "fleet_summary": {
                    "total_nodes": i,
                    "nodes_healthy": i,
                    "nodes_unhealthy": 0,
                    "total_channels": i * 2,
                    "total_capacity_sats": i * 1000000,
                    "total_onchain_sats": i * 500000,
                    "channel_health": {}
                },
                "hive_topology": {},
                "nodes": {}
            }
            return temp_db.record_fleet_snapshot(report, "manual")

        # Run concurrent writes using threads (simulating async context)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(write_snapshot, i) for i in range(10)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        # All should have succeeded with unique IDs
        assert len(results) == 10
        assert len(set(results)) == 10  # All unique IDs

    def test_async_concurrent_reads(self, temp_db):
        """Concurrent async reads should not error."""
        # Insert some test data first
        report = {
            "fleet_summary": {
                "total_nodes": 3,
                "nodes_healthy": 3,
                "nodes_unhealthy": 0,
                "total_channels": 6,
                "total_capacity_sats": 3000000,
                "total_onchain_sats": 1500000,
                "channel_health": {}
            },
            "hive_topology": {},
            "nodes": {}
        }
        temp_db.record_fleet_snapshot(report, "manual")

        async def run_concurrent():
            async def read_stats():
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, temp_db.get_stats)

            tasks = [read_stats() for _ in range(10)]
            return await asyncio.gather(*tasks)

        results = asyncio.run(run_concurrent())

        assert len(results) == 10
        for r in results:
            assert isinstance(r, dict)


# =============================================================================
# _normalize_response() Tests
# =============================================================================

class TestNormalizeResponse:
    """Test the _normalize_response helper."""

    def test_error_dict_gets_ok_false(self):
        """Dicts with 'error' key should get ok: false."""
        from mcp_hive_server_helpers import normalize_response
        result = normalize_response({"error": "something went wrong"})
        assert result["ok"] is False
        assert result["error"] == "something went wrong"

    def test_clean_dict_gets_ok_true(self):
        """Dicts without 'error' key should get ok: true."""
        from mcp_hive_server_helpers import normalize_response
        result = normalize_response({"channels": 5, "status": "healthy"})
        assert result["ok"] is True
        assert result["data"]["channels"] == 5

    def test_empty_dict_gets_ok_true(self):
        """Empty dicts should get ok: true."""
        from mcp_hive_server_helpers import normalize_response
        result = normalize_response({})
        assert result["ok"] is True

    def test_list_input_gets_ok_true(self):
        """List inputs should get ok: true."""
        from mcp_hive_server_helpers import normalize_response
        result = normalize_response([1, 2, 3])
        assert result["ok"] is True
        assert result["data"] == [1, 2, 3]

    def test_error_with_details(self):
        """Error responses should include details."""
        from mcp_hive_server_helpers import normalize_response
        input_dict = {"error": "timeout", "code": 408}
        result = normalize_response(input_dict)
        assert result["ok"] is False
        assert result["details"]["code"] == 408


# =============================================================================
# _extract_msat() Tests
# =============================================================================

class TestExtractMsat:
    """Test the _extract_msat helper."""

    def test_dict_with_msat_key(self):
        """Dict with 'msat' key should extract the value."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat({"msat": 1000}) == 1000

    def test_string_msat_suffix(self):
        """String ending in 'msat' should parse the number."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat("1000msat") == 1000

    def test_integer_passthrough(self):
        """Integer values should pass through."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat(5000) == 5000

    def test_float_truncated(self):
        """Float values should be truncated to int."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat(1234.56) == 1234

    def test_none_returns_zero(self):
        """None should return 0."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat(None) == 0

    def test_empty_string_returns_zero(self):
        """Empty string should return 0."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat("") == 0

    def test_invalid_string_returns_zero(self):
        """Non-msat string should return 0."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat("notanumber") == 0

    def test_dict_msat_string_value(self):
        """Dict with string msat value should parse."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat({"msat": "5000"}) == 5000

    def test_zero_msat_string(self):
        """'0msat' should return 0."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat("0msat") == 0

    def test_invalid_msat_string(self):
        """'abcmsat' should return 0."""
        from mcp_hive_server_helpers import extract_msat
        assert extract_msat("abcmsat") == 0


# =============================================================================
# Configurable Timeout Tests (QF2)
# =============================================================================

class TestConfigurableTimeouts:
    """Test that timeout env vars are used."""

    def test_http_timeout_env_var(self):
        """HIVE_HTTP_TIMEOUT should be configurable."""
        server_path = os.path.join(
            os.path.dirname(__file__), '..', 'tools', 'mcp-hive-server.py'
        )
        with open(server_path, 'r') as f:
            source = f.read()

        assert 'HIVE_HTTP_TIMEOUT' in source
        assert 'HIVE_DOCKER_TIMEOUT' in source

    def test_deprecated_event_loop_removed(self):
        """asyncio.get_event_loop() should not be used."""
        server_path = os.path.join(
            os.path.dirname(__file__), '..', 'tools', 'mcp-hive-server.py'
        )
        with open(server_path, 'r') as f:
            source = f.read()

        assert "get_event_loop()" not in source, \
            "Deprecated asyncio.get_event_loop() still in use"
        assert "get_running_loop()" in source


# =============================================================================
# Method Allowlist Tests (Stage 6)
# =============================================================================

class TestMethodAllowlist:
    """Test the _check_method_allowed function."""

    def test_allowlist_present_in_source(self):
        """_check_method_allowed should exist in the codebase."""
        server_path = os.path.join(
            os.path.dirname(__file__), '..', 'tools', 'mcp-hive-server.py'
        )
        with open(server_path, 'r') as f:
            source = f.read()

        assert "def _check_method_allowed" in source
        assert "HIVE_ALLOWED_METHODS" in source
