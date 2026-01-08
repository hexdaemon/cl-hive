"""
Unit tests for cl-hive protocol layer.

Tests:
1. Magic Byte Verification - Non-HIVE messages are ignored
2. Round Trip - Serialize -> Deserialize preserves data
3. Message Types - All MVP message types are handled
4. Ticket Expiry - Expired tickets are rejected

Run with: pytest tests/test_protocol.py -v
"""

import pytest
import time
import json
from unittest.mock import Mock, MagicMock

# Import modules under test
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.protocol import (
    HIVE_MAGIC,
    HiveMessageType,
    serialize,
    deserialize,
    is_hive_message,
    create_hello,
    create_challenge,
    create_attest,
    create_welcome
)

from modules.handshake import (
    Ticket,
    Manifest,
    Requirements,
    DEFAULT_TICKET_HOURS,
    NONCE_SIZE
)


# =============================================================================
# MAGIC BYTE TESTS
# =============================================================================

class TestMagicBytes:
    """Test magic byte verification (Peek & Check)."""
    
    def test_valid_magic_prefix(self):
        """Messages with HIVE magic should be recognized."""
        data = HIVE_MAGIC + b'{"type":32769}'
        assert is_hive_message(data) is True
    
    def test_invalid_magic_prefix(self):
        """Messages without HIVE magic should be rejected."""
        data = b'FAKE{"type":32769}'
        assert is_hive_message(data) is False
    
    def test_empty_message(self):
        """Empty messages should be rejected."""
        assert is_hive_message(b'') is False
    
    def test_short_message(self):
        """Messages shorter than 4 bytes should be rejected."""
        assert is_hive_message(b'HIV') is False
        assert is_hive_message(b'HI') is False
        assert is_hive_message(b'H') is False
    
    def test_only_magic_no_payload(self):
        """Message with only magic but no payload should still pass magic check."""
        assert is_hive_message(HIVE_MAGIC) is True
    
    def test_other_plugin_message(self):
        """Messages from other plugins should be passed through."""
        # Simulate a message from another plugin using experimental range
        other_plugin_msg = b'BOLT' + b'{"type":32800}'
        assert is_hive_message(other_plugin_msg) is False


# =============================================================================
# SERIALIZATION ROUND-TRIP TESTS
# =============================================================================

class TestSerialization:
    """Test serialize/deserialize round-trip."""
    
    def test_hello_round_trip(self):
        """HELLO message should survive serialize -> deserialize."""
        original_payload = {"ticket": "base64encodedticket", "protocol_version": 1}
        
        data = serialize(HiveMessageType.HELLO, original_payload)
        msg_type, payload = deserialize(data)
        
        assert msg_type == HiveMessageType.HELLO
        assert payload['ticket'] == original_payload['ticket']
        assert payload['protocol_version'] == original_payload['protocol_version']
    
    def test_challenge_round_trip(self):
        """CHALLENGE message should survive serialize -> deserialize."""
        original_payload = {"nonce": "a" * 64, "hive_id": "hive_12345"}
        
        data = serialize(HiveMessageType.CHALLENGE, original_payload)
        msg_type, payload = deserialize(data)
        
        assert msg_type == HiveMessageType.CHALLENGE
        assert payload['nonce'] == original_payload['nonce']
        assert payload['hive_id'] == original_payload['hive_id']
    
    def test_attest_round_trip(self):
        """ATTEST message should survive serialize -> deserialize."""
        original_payload = {
            "pubkey": "02" + "a" * 64,
            "version": "cl-hive v0.1.0",
            "features": ["splice", "dual-fund"],
            "nonce_signature": "sig1",
            "manifest_signature": "sig2"
        }
        
        data = serialize(HiveMessageType.ATTEST, original_payload)
        msg_type, payload = deserialize(data)
        
        assert msg_type == HiveMessageType.ATTEST
        assert payload['pubkey'] == original_payload['pubkey']
        assert payload['features'] == original_payload['features']
    
    def test_welcome_round_trip(self):
        """WELCOME message should survive serialize -> deserialize."""
        original_payload = {
            "hive_id": "hive_test",
            "tier": "neophyte",
            "member_count": 5,
            "state_hash": "0" * 64
        }
        
        data = serialize(HiveMessageType.WELCOME, original_payload)
        msg_type, payload = deserialize(data)
        
        assert msg_type == HiveMessageType.WELCOME
        assert payload['tier'] == "neophyte"
        assert payload['member_count'] == 5
    
    def test_complex_payload(self):
        """Complex nested payloads should serialize correctly."""
        original_payload = {
            "simple": "string",
            "number": 12345,
            "float": 3.14159,
            "nested": {"key": "value", "list": [1, 2, 3]},
            "unicode": "こんにちは",
        }
        
        data = serialize(HiveMessageType.HELLO, original_payload)
        msg_type, payload = deserialize(data)
        
        assert payload['nested']['key'] == "value"
        assert payload['nested']['list'] == [1, 2, 3]
        assert payload['unicode'] == "こんにちは"
    
    def test_deserialize_invalid_json(self):
        """Invalid JSON after magic should return None."""
        data = HIVE_MAGIC + b'not valid json'
        msg_type, payload = deserialize(data)
        
        assert msg_type is None
        assert payload is None
    
    def test_deserialize_missing_type(self):
        """JSON without 'type' field should return None."""
        data = HIVE_MAGIC + b'{"payload": "data"}'
        msg_type, payload = deserialize(data)
        
        assert msg_type is None
        assert payload is None


# =============================================================================
# MESSAGE HELPER TESTS
# =============================================================================

class TestMessageHelpers:
    """Test convenience functions for creating messages."""
    
    def test_create_hello(self):
        """create_hello should produce valid HELLO message."""
        data = create_hello("myticket123")
        
        assert data[:4] == HIVE_MAGIC
        msg_type, payload = deserialize(data)
        assert msg_type == HiveMessageType.HELLO
        assert payload['ticket'] == "myticket123"
    
    def test_create_challenge(self):
        """create_challenge should produce valid CHALLENGE message."""
        nonce = "deadbeef" * 8
        data = create_challenge(nonce, "hive_abc")
        
        msg_type, payload = deserialize(data)
        assert msg_type == HiveMessageType.CHALLENGE
        assert payload['nonce'] == nonce
        assert payload['hive_id'] == "hive_abc"
    
    def test_create_attest(self):
        """create_attest should produce valid ATTEST message."""
        manifest = {
            "pubkey": "02" + "a" * 64,
            "version": "v1.0",
            "features": ["splice"],
            "timestamp": 1234567890,
            "nonce": "deadbeef" * 8
        }
        data = create_attest(
            pubkey="02" + "a" * 64,
            version="v1.0",
            features=["splice"],
            nonce_signature="nsig",
            manifest_signature="msig",
            manifest=manifest
        )
        
        msg_type, payload = deserialize(data)
        assert msg_type == HiveMessageType.ATTEST
        assert "splice" in payload['features']
    
    def test_create_welcome(self):
        """create_welcome should produce valid WELCOME message."""
        data = create_welcome("hive_xyz", "member", 10, "hash123")
        
        msg_type, payload = deserialize(data)
        assert msg_type == HiveMessageType.WELCOME
        assert payload['tier'] == "member"
        assert payload['member_count'] == 10


# =============================================================================
# TICKET TESTS
# =============================================================================

class TestTicket:
    """Test ticket structure and expiry."""
    
    def test_ticket_to_json_excludes_signature(self):
        """Ticket JSON for signing should not include signature."""
        ticket = Ticket(
            admin_pubkey="02" + "a" * 64,
            hive_id="hive_test",
            requirements=0,
            issued_at=1000,
            expires_at=2000,
            signature="should_not_appear"
        )
        
        ticket_json = ticket.to_json()
        parsed = json.loads(ticket_json)
        
        assert 'signature' not in parsed
        assert parsed['admin_pubkey'] == ticket.admin_pubkey
    
    def test_ticket_base64_round_trip(self):
        """Ticket should survive base64 encode -> decode."""
        original = Ticket(
            admin_pubkey="02" + "b" * 64,
            hive_id="hive_roundtrip",
            requirements=Requirements.SPLICE | Requirements.DUAL_FUND,
            issued_at=int(time.time()),
            expires_at=int(time.time()) + 3600,
            signature="test_signature"
        )
        
        encoded = original.to_base64()
        decoded = Ticket.from_base64(encoded)
        
        assert decoded.admin_pubkey == original.admin_pubkey
        assert decoded.hive_id == original.hive_id
        assert decoded.requirements == original.requirements
        assert decoded.signature == original.signature
    
    def test_ticket_not_expired(self):
        """Fresh ticket should not be expired."""
        ticket = Ticket(
            admin_pubkey="02" + "c" * 64,
            hive_id="hive_fresh",
            requirements=0,
            issued_at=int(time.time()),
            expires_at=int(time.time()) + 3600,  # 1 hour from now
            signature="sig"
        )
        
        assert ticket.is_expired() is False
    
    def test_ticket_expired(self):
        """Old ticket should be expired."""
        ticket = Ticket(
            admin_pubkey="02" + "d" * 64,
            hive_id="hive_old",
            requirements=0,
            issued_at=1000,
            expires_at=2000,  # Way in the past
            signature="sig"
        )
        
        assert ticket.is_expired() is True
    
    def test_ticket_just_expired(self):
        """Ticket that just expired should be detected."""
        ticket = Ticket(
            admin_pubkey="02" + "e" * 64,
            hive_id="hive_edge",
            requirements=0,
            issued_at=int(time.time()) - 10,
            expires_at=int(time.time()) - 1,  # Expired 1 second ago
            signature="sig"
        )
        
        assert ticket.is_expired() is True


# =============================================================================
# REQUIREMENTS BITMASK TESTS
# =============================================================================

class TestRequirements:
    """Test feature requirement bitmasks."""
    
    def test_no_requirements(self):
        """NONE should be zero."""
        assert Requirements.NONE == 0
    
    def test_single_requirement(self):
        """Single requirements should be powers of 2."""
        assert Requirements.SPLICE == 1
        assert Requirements.DUAL_FUND == 2
        assert Requirements.ANCHOR == 4
        assert Requirements.ONION_MSG == 8
    
    def test_combined_requirements(self):
        """Combined requirements should use bitwise OR."""
        combined = Requirements.SPLICE | Requirements.DUAL_FUND
        
        assert combined & Requirements.SPLICE
        assert combined & Requirements.DUAL_FUND
        assert not (combined & Requirements.ANCHOR)
    
    def test_all_requirements(self):
        """All requirements combined."""
        all_reqs = (Requirements.SPLICE | Requirements.DUAL_FUND | 
                    Requirements.ANCHOR | Requirements.ONION_MSG)
        
        assert all_reqs == 15  # 1 + 2 + 4 + 8


# =============================================================================
# MANIFEST TESTS
# =============================================================================

class TestManifest:
    """Test manifest structure."""
    
    def test_manifest_to_json(self):
        """Manifest JSON should be deterministic (sorted keys)."""
        manifest = Manifest(
            pubkey="02" + "f" * 64,
            version="v1.0",
            features=["splice", "dual-fund"],
            timestamp=1234567890,
            nonce="abc123"
        )
        
        json1 = manifest.to_json()
        json2 = manifest.to_json()
        
        assert json1 == json2  # Deterministic
        
        parsed = json.loads(json1)
        assert parsed['pubkey'] == manifest.pubkey
        assert parsed['features'] == manifest.features


# =============================================================================
# EDGE CASES
# =============================================================================

class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_deserialize_empty_payload(self):
        """Empty payload after magic should handle gracefully."""
        data = HIVE_MAGIC + b''
        msg_type, payload = deserialize(data)
        
        assert msg_type is None
    
    def test_deserialize_invalid_message_type(self):
        """Unknown message type should raise ValueError (caught internally)."""
        # Message type 99999 doesn't exist
        data = HIVE_MAGIC + b'{"type": 99999, "version": 1, "payload": {}}'
        msg_type, payload = deserialize(data)
        
        assert msg_type is None
    
    def test_serialize_special_characters(self):
        """Special characters in payload should be handled."""
        payload = {
            "quotes": 'He said "hello"',
            "newlines": "line1\nline2",
            "backslash": "path\\to\\file",
            "emoji": "🐝⚡"
        }
        
        data = serialize(HiveMessageType.HELLO, payload)
        msg_type, result = deserialize(data)
        
        assert result['emoji'] == "🐝⚡"
        assert result['quotes'] == 'He said "hello"'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
