"""
CLBoss Bridge Module for cl-hive.

Provides a small gateway wrapper for CLBoss integration:
- Detect availability from plugin list.
- Ignore/unignore peers to prevent redundant channel opens.

Explicitly avoids clboss-manage/unmanage; fee control belongs to cl-revenue-ops.
"""

from typing import Any, Dict

from pyln.client import RpcError


class CLBossBridge:
    """Gateway wrapper around CLBoss RPC calls."""

    def __init__(self, rpc, plugin=None):
        self.rpc = rpc
        self.plugin = plugin
        self._available = False
        self._supports_unignore = True

    def _log(self, msg: str, level: str = "info") -> None:
        if self.plugin:
            self.plugin.log(f"[CLBossBridge] {msg}", level=level)

    def detect_clboss(self) -> bool:
        """Detect whether CLBoss is registered and active."""
        try:
            plugins = self.rpc.plugin("list")
            for entry in plugins.get("plugins", []):
                if "clboss" in entry.get("name", "").lower():
                    self._available = entry.get("active", False)
                    return self._available
            self._available = False
            return False
        except Exception as exc:
            self._available = False
            self._log(f"CLBoss detection failed: {exc}", level="warn")
            return False

    def ignore_peer(self, peer_id: str) -> bool:
        """Tell CLBoss to ignore a peer for channel management."""
        if not self._available:
            self._log(f"CLBoss not available, cannot ignore {peer_id[:16]}...")
            return False
        try:
            self.rpc.call("clboss-ignore", {"nodeid": peer_id})
            self._log(f"CLBoss ignoring {peer_id[:16]}...")
            return True
        except RpcError as exc:
            self._log(f"CLBoss ignore failed: {exc}", level="warn")
            return False

    def unignore_peer(self, peer_id: str) -> bool:
        """Tell CLBoss to stop ignoring a peer, if supported."""
        if not self._available or not self._supports_unignore:
            return False
        try:
            self.rpc.call("clboss-unignore", {"nodeid": peer_id})
            self._log(f"CLBoss unignoring {peer_id[:16]}...")
            return True
        except RpcError as exc:
            msg = str(exc).lower()
            if "unknown command" in msg or "method not found" in msg:
                self._supports_unignore = False
                self._log("CLBoss does not support clboss-unignore", level="warn")
            else:
                self._log(f"CLBoss unignore failed: {exc}", level="warn")
            return False
