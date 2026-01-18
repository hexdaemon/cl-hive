"""
Contribution tracking module for cl-hive.

Tracks forwarding events for contribution ratio and anti-leech signals.
"""

import time
from typing import Any, Dict, Optional, Tuple


CHANNEL_MAP_REFRESH_SECONDS = 300
MAX_CONTRIB_EVENTS_PER_PEER_PER_HOUR = 120
MAX_EVENT_MSAT = 10 ** 14
LEECH_WARN_RATIO = 0.5
LEECH_BAN_RATIO = 0.4
LEECH_WINDOW_DAYS = 7

# P5-02: Global daily limit across all peers (anti-Sybil DoS protection)
MAX_CONTRIB_EVENTS_PER_DAY_TOTAL = 10000


class ContributionManager:
    """Tracks contribution stats and leech detection."""

    def __init__(self, rpc, db, plugin, config):
        self.rpc = rpc
        self.db = db
        self.plugin = plugin
        self.config = config
        self._channel_map: Dict[str, str] = {}
        self._last_refresh = 0
        self._rate_limits: Dict[str, Tuple[int, int]] = {}
        # P5-02: Track global daily contribution event count
        self._daily_count = 0
        self._daily_window_start = int(time.time())

    def _log(self, msg: str, level: str = "info") -> None:
        if self.plugin:
            self.plugin.log(f"[Contribution] {msg}", level=level)

    def _parse_msat(self, value: Any) -> Optional[int]:
        if isinstance(value, int):
            return value
        if isinstance(value, dict) and "msat" in value:
            return self._parse_msat(value["msat"])
        if isinstance(value, str):
            text = value.strip()
            if text.endswith("msat"):
                text = text[:-4]
            if text.isdigit():
                return int(text)
        return None

    def _refresh_channel_map(self) -> None:
        now = int(time.time())
        if now - self._last_refresh < CHANNEL_MAP_REFRESH_SECONDS:
            return
        try:
            data = self.rpc.listpeerchannels()
        except Exception as exc:
            self._log(f"Failed to refresh channel map: {exc}", level="warn")
            return

        mapping: Dict[str, str] = {}
        # listpeerchannels returns {"channels": [...]} with peer_id in each channel
        for channel in data.get("channels", []):
            peer_id = channel.get("peer_id")
            if not peer_id:
                continue
            for key in ("short_channel_id", "channel_id", "scid"):
                chan_id = channel.get(key)
                if chan_id:
                    mapping[str(chan_id)] = peer_id

        self._channel_map = mapping
        self._last_refresh = now

    def _lookup_peer(self, channel_id: str) -> Optional[str]:
        self._refresh_channel_map()
        return self._channel_map.get(channel_id)

    def _allow_daily_global(self) -> bool:
        """
        P5-02: Check global daily limit across all peers.

        Returns False if daily cap exceeded (resets after 24h).
        """
        now = int(time.time())
        # Reset counter if 24h have passed
        if now - self._daily_window_start >= 86400:
            self._daily_window_start = now
            self._daily_count = 0
        if self._daily_count >= MAX_CONTRIB_EVENTS_PER_DAY_TOTAL:
            return False
        self._daily_count += 1
        return True

    def _allow_record(self, peer_id: str) -> bool:
        """Check per-peer rate limit and global daily limit."""
        # P5-02: Check global daily limit first
        if not self._allow_daily_global():
            return False

        now = int(time.time())
        window_start, count = self._rate_limits.get(peer_id, (now, 0))
        if now - window_start >= 3600:
            window_start = now
            count = 0
        if count >= MAX_CONTRIB_EVENTS_PER_PEER_PER_HOUR:
            return False
        self._rate_limits[peer_id] = (window_start, count + 1)
        return True

    def handle_forward_event(self, payload: Dict[str, Any]) -> None:
        """Process a forward_event notification safely."""
        if not isinstance(payload, dict):
            return
        if payload.get("status") not in (None, "settled"):
            return

        in_channel = payload.get("in_channel")
        out_channel = payload.get("out_channel")
        if not in_channel or not out_channel:
            return

        in_msat = self._parse_msat(payload.get("in_msat"))
        out_msat = self._parse_msat(payload.get("out_msat"))
        if in_msat is None or out_msat is None:
            return
        amount_msat = min(in_msat, out_msat)
        if amount_msat <= 0 or amount_msat > MAX_EVENT_MSAT:
            return

        in_peer = self._lookup_peer(str(in_channel))
        out_peer = self._lookup_peer(str(out_channel))
        if not in_peer and not out_peer:
            return

        amount_sats = amount_msat // 1000
        if amount_sats <= 0:
            return

        if in_peer and in_peer != out_peer:
            member = self.db.get_member(in_peer)
            if member and member.get("tier") in ("member", "neophyte"):
                if self._allow_record(in_peer):
                    self.db.record_contribution(in_peer, "forwarded", amount_sats)
                    self.check_leech_status(in_peer)

        if out_peer and out_peer != in_peer:
            member = self.db.get_member(out_peer)
            if member and member.get("tier") in ("member", "neophyte"):
                if self._allow_record(out_peer):
                    self.db.record_contribution(out_peer, "received", amount_sats)
                    self.check_leech_status(out_peer)

    def get_contribution_stats(self, peer_id: str, window_days: int = 30) -> Dict[str, Any]:
        stats = self.db.get_contribution_stats(peer_id, window_days=window_days)
        forwarded = stats["forwarded"]
        received = stats["received"]
        ratio = 1.0 if received == 0 else forwarded / received
        return {"forwarded": forwarded, "received": received, "ratio": ratio}

    def check_leech_status(self, peer_id: str) -> Dict[str, Any]:
        stats = self.get_contribution_stats(peer_id, window_days=LEECH_WINDOW_DAYS)
        ratio = stats["ratio"]

        if ratio > LEECH_BAN_RATIO:
            self.db.clear_leech_flag(peer_id)
            return {"is_leech": ratio < LEECH_WARN_RATIO, "ratio": ratio}

        now = int(time.time())
        flag = self.db.get_leech_flag(peer_id)
        if not flag:
            self.db.set_leech_flag(peer_id, now, False)
            return {"is_leech": True, "ratio": ratio}

        low_since = flag["low_since_ts"]
        ban_triggered = bool(flag["ban_triggered"])
        if not ban_triggered and now - low_since >= (LEECH_WINDOW_DAYS * 86400):
            if self.config.ban_autotrigger_enabled:
                self._log(f"Leech ban trigger for {peer_id[:16]}... (ratio={ratio:.2f})", level="warn")
                self.db.set_leech_flag(peer_id, low_since, True)
            else:
                self._log(f"Leech ban proposal flagged for {peer_id[:16]}... (ratio={ratio:.2f})", level="warn")
                self.db.set_leech_flag(peer_id, low_since, True)

        return {"is_leech": True, "ratio": ratio}
