"""
Tests for Phase B: Protocol version tolerance.

Ensures deserialize() accepts a range of versions, rejects out-of-range
versions, and that create_hello() advertises supported_versions.

Run with: pytest tests/test_protocol_versioning.py -v
"""

import json
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.protocol import (
    HIVE_MAGIC,
    PROTOCOL_VERSION,
    SUPPORTED_VERSIONS,
    MIN_SUPPORTED_VERSION,
    MAX_SUPPORTED_VERSION,
    HiveMessageType,
    serialize,
    deserialize,
    create_hello,
)


# =============================================================================
# HELPERS
# =============================================================================

def _make_raw(version: int, msg_type: int, payload: dict) -> bytes:
    """Build raw bytes with a specific envelope version."""
    envelope = {
        "version": version,
        "type": msg_type,
        "payload": payload,
    }
    return HIVE_MAGIC + json.dumps(envelope).encode("utf-8")


# =============================================================================
# VERSION RANGE ACCEPTANCE
# =============================================================================

class TestVersionTolerance:

    def test_current_version_accepted(self):
        """deserialize() accepts the current PROTOCOL_VERSION (v1)."""
        data = _make_raw(PROTOCOL_VERSION, HiveMessageType.HELLO, {"pubkey": "abc"})
        msg_type, payload = deserialize(data)
        assert msg_type == HiveMessageType.HELLO
        assert payload["pubkey"] == "abc"

    def test_future_version_accepted(self):
        """deserialize() accepts MAX_SUPPORTED_VERSION (v2) for rolling upgrades."""
        data = _make_raw(MAX_SUPPORTED_VERSION, HiveMessageType.HELLO, {"pubkey": "xyz"})
        msg_type, payload = deserialize(data)
        assert msg_type == HiveMessageType.HELLO
        assert payload["pubkey"] == "xyz"

    def test_out_of_range_version_rejected(self):
        """deserialize() rejects version 99 (not in SUPPORTED_VERSIONS)."""
        data = _make_raw(99, HiveMessageType.HELLO, {"pubkey": "abc"})
        msg_type, payload = deserialize(data)
        assert msg_type is None
        assert payload is None

    def test_version_zero_rejected(self):
        """deserialize() rejects version 0."""
        data = _make_raw(0, HiveMessageType.HELLO, {"pubkey": "abc"})
        msg_type, payload = deserialize(data)
        assert msg_type is None

    def test_negative_version_rejected(self):
        """deserialize() rejects negative versions."""
        data = _make_raw(-1, HiveMessageType.HELLO, {"pubkey": "abc"})
        msg_type, payload = deserialize(data)
        assert msg_type is None

    def test_supported_versions_set(self):
        """SUPPORTED_VERSIONS contains expected range."""
        assert 1 in SUPPORTED_VERSIONS
        assert 2 in SUPPORTED_VERSIONS
        assert 0 not in SUPPORTED_VERSIONS
        assert 3 not in SUPPORTED_VERSIONS


# =============================================================================
# ENVELOPE VERSION INJECTION
# =============================================================================

class TestEnvelopeVersionInjection:

    def test_envelope_version_present_v1(self):
        """Deserialized payload includes _envelope_version for v1."""
        data = _make_raw(1, HiveMessageType.HELLO, {"pubkey": "a"})
        _, payload = deserialize(data)
        assert payload["_envelope_version"] == 1

    def test_envelope_version_present_v2(self):
        """Deserialized payload includes _envelope_version for v2."""
        data = _make_raw(2, HiveMessageType.HELLO, {"pubkey": "a"})
        _, payload = deserialize(data)
        assert payload["_envelope_version"] == 2

    def test_serialize_deserialize_roundtrip_preserves_version(self):
        """Serialize with current version, deserialize gets _envelope_version."""
        raw = serialize(HiveMessageType.HELLO, {"pubkey": "test"})
        msg_type, payload = deserialize(raw)
        assert msg_type == HiveMessageType.HELLO
        assert payload["_envelope_version"] == PROTOCOL_VERSION


# =============================================================================
# create_hello() CHANGES
# =============================================================================

class TestCreateHelloVersions:

    def test_hello_includes_supported_versions(self):
        """create_hello() payload contains supported_versions list."""
        raw = create_hello("02" + "a" * 64)
        _, payload = deserialize(raw)
        assert "supported_versions" in payload
        assert sorted(payload["supported_versions"]) == sorted(SUPPORTED_VERSIONS)

    def test_hello_backward_compatible(self):
        """Old nodes ignore supported_versions - message still parses."""
        raw = create_hello("02" + "b" * 64)
        msg_type, payload = deserialize(raw)
        assert msg_type == HiveMessageType.HELLO
        assert payload["pubkey"] == "02" + "b" * 64
        # protocol_version still present for backward compat
        assert payload["protocol_version"] == PROTOCOL_VERSION

    def test_old_hello_without_supported_versions_still_parses(self):
        """A HELLO from an old node (no supported_versions) still deserializes."""
        data = _make_raw(1, HiveMessageType.HELLO, {
            "pubkey": "02" + "c" * 64,
            "protocol_version": 1,
        })
        msg_type, payload = deserialize(data)
        assert msg_type == HiveMessageType.HELLO
        assert "supported_versions" not in payload  # old node doesn't send it


# =============================================================================
# PEER CAPABILITIES (Database)
# =============================================================================

class TestPeerCapabilities:

    @pytest.fixture
    def db(self, tmp_path):
        from unittest.mock import Mock
        from modules.database import HiveDatabase
        mock_plugin = Mock()
        mock_plugin.log = Mock()
        db = HiveDatabase(str(tmp_path / "test.db"), mock_plugin)
        db.initialize()
        return db

    def test_save_and_get_roundtrip(self, db):
        """save_peer_capabilities / get_peer_capabilities round-trip."""
        features = ["splice", "proto-v2"]
        db.save_peer_capabilities("peer1", features)
        caps = db.get_peer_capabilities("peer1")
        assert caps is not None
        assert caps["features"] == features
        assert caps["max_protocol_version"] == 2

    def test_get_nonexistent_peer(self, db):
        """get_peer_capabilities returns None for unknown peer."""
        assert db.get_peer_capabilities("unknown") is None

    def test_get_max_protocol_version_default(self, db):
        """get_peer_max_protocol_version returns 1 for unknown peer."""
        assert db.get_peer_max_protocol_version("unknown") == 1

    def test_get_max_protocol_version_saved(self, db):
        """get_peer_max_protocol_version returns saved value."""
        db.save_peer_capabilities("peer2", ["proto-v3", "splice"])
        assert db.get_peer_max_protocol_version("peer2") == 3

    def test_update_capabilities(self, db):
        """Updating capabilities replaces old values."""
        db.save_peer_capabilities("peer1", ["proto-v1"])
        db.save_peer_capabilities("peer1", ["proto-v2", "splice"])
        caps = db.get_peer_capabilities("peer1")
        assert caps["max_protocol_version"] == 2
        assert "splice" in caps["features"]

    def test_invalid_features_rejected(self, db):
        """Non-list features argument returns False."""
        assert db.save_peer_capabilities("peer1", "not-a-list") is False
