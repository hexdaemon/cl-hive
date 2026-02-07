"""
Extracted helper functions from mcp-hive-server.py for testability.

These pure functions are used by tests without requiring the full MCP server
dependencies (httpx, mcp SDK, etc.).
"""

import logging
import os
import re
from typing import Any, Dict

logger = logging.getLogger("mcp-hive")


def load_strategy_with_dir(name: str, strategy_dir: str) -> str:
    """
    Load a strategy prompt from a markdown file.

    This is the testable version of load_strategy() that accepts
    the strategy directory as a parameter instead of using a global.
    """
    if not strategy_dir:
        return ""
    # Reject names with path traversal characters
    if not re.match(r'^[a-zA-Z0-9_-]+$', name):
        logger.warning(f"Strategy name rejected (invalid chars): {name!r}")
        return ""
    strategy_max_chars = 4000
    path = os.path.join(strategy_dir, f"{name}.md")
    # Resolve and enforce directory boundary
    resolved = os.path.realpath(path)
    strategy_root = os.path.realpath(strategy_dir)
    if not resolved.startswith(strategy_root + os.sep) and resolved != strategy_root:
        logger.warning(f"Strategy path escaped directory: {name!r}")
        return ""
    try:
        with open(resolved, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read().strip()
            if len(content) > strategy_max_chars:
                content = content[:strategy_max_chars].rstrip() + "\n\n[truncated]"
            return "\n\n" + content
    except FileNotFoundError:
        return ""
    except Exception as e:
        logger.warning(f"Error loading strategy {name}: {e}")
        return ""


def normalize_response(result: Any) -> Dict[str, Any]:
    """Normalize a tool response into ok/data or ok/error shape."""
    if isinstance(result, dict) and "error" in result:
        return {"ok": False, "error": result.get("error"), "details": result}
    return {"ok": True, "data": result}


def extract_msat(value: Any) -> int:
    """Extract millisatoshi value from various CLN response formats."""
    if isinstance(value, dict) and "msat" in value:
        return int(value.get("msat", 0))
    if isinstance(value, str) and value.endswith("msat"):
        try:
            return int(value[:-4])
        except ValueError:
            return 0
    if isinstance(value, (int, float)):
        return int(value)
    return 0
