"""
VPN Transport Module for cl-hive.

Manages VPN-based communication for hive gossip, providing:
- VPN subnet detection
- Peer address resolution (VPN vs clearnet)
- Transport policy enforcement
- Connection routing decisions

This enables hive gossip to be routed exclusively through a WireGuard VPN
while maintaining public Lightning channels over Tor/clearnet.

Transport Modes:
- any: Accept hive gossip from any interface (default)
- vpn-only: Only accept hive gossip from VPN interface
- vpn-preferred: Prefer VPN, fall back to any

Author: Lightning Goats Team
"""

import ipaddress
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


# =============================================================================
# CONSTANTS
# =============================================================================

# Default VPN port for Lightning
DEFAULT_VPN_PORT = 9735

# Cache duration for peer VPN status (seconds)
VPN_STATUS_CACHE_TTL = 300

# Maximum number of VPN subnets to configure
MAX_VPN_SUBNETS = 10

# Maximum number of VPN peer mappings
MAX_VPN_PEERS = 100


# =============================================================================
# ENUMS
# =============================================================================

class TransportMode(Enum):
    """Hive transport modes for gossip routing."""
    ANY = "any"                    # Accept from any interface
    VPN_ONLY = "vpn-only"          # VPN required for hive gossip
    VPN_PREFERRED = "vpn-preferred"  # Prefer VPN, allow fallback


class MessageRequirement(Enum):
    """Which message types require VPN transport."""
    ALL = "all"            # All hive messages require VPN
    GOSSIP = "gossip"      # Only gossip messages
    INTENT = "intent"      # Only intent messages
    SYNC = "sync"          # Only sync messages
    NONE = "none"          # No messages require VPN (monitoring only)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class VPNPeerMapping:
    """Maps a node pubkey to its VPN address."""
    pubkey: str
    vpn_ip: str
    vpn_port: int = DEFAULT_VPN_PORT
    added_at: int = field(default_factory=lambda: int(time.time()))

    @property
    def vpn_address(self) -> str:
        """Get formatted VPN address."""
        return f"{self.vpn_ip}:{self.vpn_port}"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "pubkey": self.pubkey,
            "vpn_ip": self.vpn_ip,
            "vpn_port": self.vpn_port,
            "vpn_address": self.vpn_address,
            "added_at": self.added_at
        }


@dataclass
class VPNConnectionInfo:
    """Tracks VPN connection state for a peer."""
    peer_id: str
    vpn_ip: Optional[str] = None
    connected_via_vpn: bool = False
    last_verified: int = 0
    connection_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "peer_id": self.peer_id,
            "vpn_ip": self.vpn_ip,
            "connected_via_vpn": self.connected_via_vpn,
            "last_verified": self.last_verified,
            "connection_count": self.connection_count
        }


# =============================================================================
# VPN TRANSPORT MANAGER
# =============================================================================

class VPNTransportManager:
    """
    Manages VPN transport policy for hive communication.

    Responsibilities:
    - Detect if peer connection is via VPN
    - Enforce transport policy for hive messages
    - Resolve peer addresses for VPN routing
    - Track VPN connectivity status

    Thread Safety:
    - Lock protects stats, peer connections, and config state
    - Configure uses snapshot-swap pattern for atomic reconfiguration
    """

    def __init__(self, plugin=None):
        """
        Initialize the VPN Transport Manager.

        Args:
            plugin: Optional plugin reference for logging and RPC
        """
        self.plugin = plugin

        # Lock protecting mutable state
        self._lock = threading.Lock()

        # Transport mode
        self._mode: TransportMode = TransportMode.ANY

        # Which message types require VPN
        self._required_messages: Set[MessageRequirement] = set()

        # VPN subnets for detection
        self._vpn_subnets: List[ipaddress.IPv4Network] = []

        # Peer to VPN address mapping
        self._vpn_peers: Dict[str, VPNPeerMapping] = {}

        # Track connection info per peer
        self._peer_connections: Dict[str, VPNConnectionInfo] = {}

        # VPN bind address (optional)
        self._vpn_bind: Optional[Tuple[str, int]] = None

        # Statistics
        self._stats = {
            "messages_accepted": 0,
            "messages_rejected": 0,
            "vpn_connections": 0,
            "non_vpn_connections": 0
        }

        # Configuration state
        self._configured = False

    # =========================================================================
    # CONFIGURATION
    # =========================================================================

    def configure(self,
                  mode: str = "any",
                  vpn_subnets: str = "",
                  vpn_bind: str = "",
                  vpn_peers: str = "",
                  required_messages: str = "all") -> Dict[str, Any]:
        """
        Configure VPN transport settings.

        Args:
            mode: Transport mode (any, vpn-only, vpn-preferred)
            vpn_subnets: Comma-separated CIDR subnets
            vpn_bind: VPN bind address (ip:port)
            vpn_peers: Comma-separated pubkey@ip:port mappings
            required_messages: Which messages require VPN (all, gossip, intent, sync, none)

        Returns:
            Configuration result dictionary
        """
        result = {
            "success": True,
            "mode": None,
            "subnets": [],
            "peers": 0,
            "bind": None,
            "warnings": []
        }

        # Build config in local variables, then atomic swap
        new_mode = TransportMode.ANY
        try:
            new_mode = TransportMode(mode.lower().strip())
            result["mode"] = new_mode.value
        except ValueError:
            self._log(f"Invalid transport mode '{mode}', using 'any'", level='warn')
            result["mode"] = "any"
            result["warnings"].append(f"Invalid mode '{mode}', defaulting to 'any'")

        # Parse required messages
        new_required: Set[MessageRequirement] = set()
        if required_messages:
            for req in required_messages.lower().split(','):
                req = req.strip()
                try:
                    new_required.add(MessageRequirement(req))
                except ValueError:
                    result["warnings"].append(f"Invalid message requirement '{req}'")

        # Default to ALL if nothing specified and mode is not ANY
        if not new_required and new_mode != TransportMode.ANY:
            new_required.add(MessageRequirement.ALL)

        # Parse VPN subnets
        new_subnets: List[ipaddress.IPv4Network] = []
        if vpn_subnets:
            for subnet in vpn_subnets.split(','):
                subnet = subnet.strip()
                if not subnet:
                    continue
                if len(new_subnets) >= MAX_VPN_SUBNETS:
                    result["warnings"].append(f"Max {MAX_VPN_SUBNETS} subnets, ignoring extras")
                    break
                try:
                    network = ipaddress.IPv4Network(subnet, strict=False)
                    new_subnets.append(network)
                    result["subnets"].append(str(network))
                except ValueError as e:
                    self._log(f"Invalid VPN subnet '{subnet}': {e}", level='warn')
                    result["warnings"].append(f"Invalid subnet '{subnet}'")

        # Parse VPN bind
        new_bind: Optional[Tuple[str, int]] = None
        if vpn_bind:
            try:
                vpn_bind = vpn_bind.strip()
                if ':' in vpn_bind:
                    ip, port = vpn_bind.rsplit(':', 1)
                    new_bind = (ip, int(port))
                else:
                    new_bind = (vpn_bind, DEFAULT_VPN_PORT)
                result["bind"] = f"{new_bind[0]}:{new_bind[1]}"
            except ValueError as e:
                self._log(f"Invalid VPN bind '{vpn_bind}': {e}", level='warn')
                result["warnings"].append(f"Invalid bind '{vpn_bind}'")

        # Parse peer mappings
        new_peers: Dict[str, VPNPeerMapping] = {}
        if vpn_peers:
            for mapping in vpn_peers.split(','):
                mapping = mapping.strip()
                if not mapping or '@' not in mapping:
                    continue
                if len(new_peers) >= MAX_VPN_PEERS:
                    result["warnings"].append(f"Max {MAX_VPN_PEERS} peers, ignoring extras")
                    break
                try:
                    pubkey, addr = mapping.split('@', 1)
                    pubkey = pubkey.strip()
                    addr = addr.strip()

                    if ':' in addr:
                        ip, port = addr.rsplit(':', 1)
                        port = int(port)
                    else:
                        ip = addr
                        port = DEFAULT_VPN_PORT

                    # Validate IP is in VPN subnet (if subnets configured)
                    if new_subnets:
                        try:
                            ip_addr = ipaddress.IPv4Address(ip)
                            if not any(ip_addr in subnet for subnet in new_subnets):
                                result["warnings"].append(
                                    f"Peer {pubkey[:16]}... IP {ip} not in VPN subnets"
                                )
                        except ValueError:
                            pass

                    new_peers[pubkey] = VPNPeerMapping(
                        pubkey=pubkey,
                        vpn_ip=ip,
                        vpn_port=port
                    )
                except ValueError as e:
                    self._log(f"Invalid VPN peer mapping '{mapping}': {e}", level='warn')
                    result["warnings"].append(f"Invalid peer mapping '{mapping}'")

        # Atomic swap under lock
        with self._lock:
            self._mode = new_mode
            self._required_messages = new_required
            self._vpn_subnets = new_subnets
            self._vpn_bind = new_bind
            self._vpn_peers = new_peers
            self._configured = True

        result["peers"] = len(new_peers)

        self._log(
            f"VPN transport configured: mode={self._mode.value}, "
            f"subnets={len(self._vpn_subnets)}, peers={len(self._vpn_peers)}"
        )

        return result

    # =========================================================================
    # VPN DETECTION
    # =========================================================================

    def is_vpn_address(self, ip_address: str) -> bool:
        """
        Check if an IP address is within configured VPN subnets.

        Args:
            ip_address: IP address to check (string)

        Returns:
            True if address is in any VPN subnet
        """
        if not self._vpn_subnets:
            return False

        try:
            # Handle IPv4-mapped IPv6 addresses
            if ip_address.startswith('::ffff:'):
                ip_address = ip_address[7:]

            ip = ipaddress.IPv4Address(ip_address)
            return any(ip in subnet for subnet in self._vpn_subnets)
        except (ValueError, ipaddress.AddressValueError):
            # Not a valid IPv4, might be IPv6 or hostname
            return False

    def extract_ip_from_address(self, address: str) -> Optional[str]:
        """
        Extract IP address from various address formats.

        Handles:
        - ip:port
        - [ipv6]:port
        - hostname:port
        - bare IP

        Args:
            address: Address string

        Returns:
            IP address or None
        """
        if not address:
            return None

        address = address.strip()

        try:
            # Handle IPv6 with brackets
            if address.startswith('['):
                end_bracket = address.find(']')
                if end_bracket > 0:
                    return address[1:end_bracket]

            # Handle IP:port format
            if ':' in address:
                # Could be IPv6 or IPv4:port
                parts = address.rsplit(':', 1)
                if len(parts) == 2:
                    potential_ip = parts[0]
                    # Check if it looks like an IP
                    try:
                        ipaddress.IPv4Address(potential_ip)
                        return potential_ip
                    except ValueError:
                        # Might be IPv6 or hostname
                        if ':' in potential_ip:
                            return potential_ip  # IPv6
                        return None  # Hostname

            # Bare IP or hostname
            try:
                ipaddress.IPv4Address(address)
                return address
            except ValueError:
                return None

        except Exception:
            return None

    # =========================================================================
    # TRANSPORT POLICY
    # =========================================================================

    def should_accept_hive_message(self,
                                    peer_id: str,
                                    message_type: str = "",
                                    peer_address: Optional[str] = None) -> Tuple[bool, str]:
        """
        Check if a hive message should be accepted based on transport policy.

        Args:
            peer_id: Node pubkey of the peer
            message_type: Type of hive message (e.g., "GOSSIP", "INTENT", "STATE_HASH")
            peer_address: Optional peer IP address

        Returns:
            Tuple of (accept: bool, reason: str)
        """
        # Snapshot mutable config under lock
        with self._lock:
            mode = self._mode
            required_messages = set(self._required_messages)
            vpn_subnets = list(self._vpn_subnets)

        # Always accept in ANY mode
        if mode == TransportMode.ANY:
            with self._lock:
                self._stats["messages_accepted"] += 1
            return (True, "any transport allowed")

        # Check if this message type requires VPN
        if not self._message_requires_vpn_snapshot(message_type, required_messages):
            with self._lock:
                self._stats["messages_accepted"] += 1
            return (True, f"message type '{message_type}' does not require VPN")

        # Get or update connection info
        conn_info = self._get_or_create_connection_info(peer_id)

        # Check if peer is connected via VPN
        with self._lock:
            is_vpn = conn_info.connected_via_vpn

        # If we have a peer address, verify it
        if peer_address and not is_vpn:
            ip = self.extract_ip_from_address(peer_address)
            if ip and self.is_vpn_address(ip):
                is_vpn = True
                with self._lock:
                    conn_info.connected_via_vpn = True
                    conn_info.vpn_ip = ip
                    conn_info.last_verified = int(time.time())

        # Check against configured VPN peers
        if not is_vpn and peer_id in self._vpn_peers:
            # Peer is configured as VPN peer but connection might not be via VPN
            # This is a policy violation in VPN_ONLY mode
            pass

        # Apply transport mode policy
        if mode == TransportMode.VPN_ONLY:
            if is_vpn:
                with self._lock:
                    self._stats["messages_accepted"] += 1
                return (True, "vpn transport verified")
            else:
                with self._lock:
                    self._stats["messages_rejected"] += 1
                self._log(
                    f"Rejected {message_type} from {peer_id[:16]}...: non-VPN connection",
                    level='debug'
                )
                return (False, "vpn-only mode: non-VPN connection rejected")

        if mode == TransportMode.VPN_PREFERRED:
            with self._lock:
                self._stats["messages_accepted"] += 1
            if is_vpn:
                return (True, "vpn transport (preferred)")
            else:
                return (True, "vpn-preferred: allowing non-VPN fallback")

        # Default accept
        with self._lock:
            self._stats["messages_accepted"] += 1
        return (True, "transport check passed")

    def _message_requires_vpn(self, message_type: str) -> bool:
        """
        Check if a message type requires VPN transport.

        Args:
            message_type: Hive message type

        Returns:
            True if VPN is required for this message type
        """
        if MessageRequirement.NONE in self._required_messages:
            return False

        if MessageRequirement.ALL in self._required_messages:
            return True

        message_type_upper = message_type.upper()

        if MessageRequirement.GOSSIP in self._required_messages:
            if "GOSSIP" in message_type_upper or "STATE" in message_type_upper:
                return True

        if MessageRequirement.INTENT in self._required_messages:
            if "INTENT" in message_type_upper:
                return True

        if MessageRequirement.SYNC in self._required_messages:
            if "SYNC" in message_type_upper or "FULL_STATE" in message_type_upper:
                return True

        return False

    @staticmethod
    def _message_requires_vpn_snapshot(
        message_type: str,
        required_messages: set
    ) -> bool:
        """Check if a message type requires VPN using a pre-snapshotted set."""
        if MessageRequirement.NONE in required_messages:
            return False

        if MessageRequirement.ALL in required_messages:
            return True

        message_type_upper = message_type.upper()

        if MessageRequirement.GOSSIP in required_messages:
            if "GOSSIP" in message_type_upper or "STATE" in message_type_upper:
                return True

        if MessageRequirement.INTENT in required_messages:
            if "INTENT" in message_type_upper:
                return True

        if MessageRequirement.SYNC in required_messages:
            if "SYNC" in message_type_upper or "FULL_STATE" in message_type_upper:
                return True

        return False

    # =========================================================================
    # PEER MANAGEMENT
    # =========================================================================

    def get_vpn_address(self, peer_id: str) -> Optional[str]:
        """
        Get the configured VPN address for a peer.

        Args:
            peer_id: Node pubkey

        Returns:
            VPN address string (ip:port) or None
        """
        with self._lock:
            mapping = self._vpn_peers.get(peer_id)
            return mapping.vpn_address if mapping else None

    def add_vpn_peer(self, pubkey: str, vpn_ip: str, vpn_port: int = DEFAULT_VPN_PORT) -> bool:
        """
        Add or update a VPN peer mapping.

        Args:
            pubkey: Node pubkey
            vpn_ip: VPN IP address
            vpn_port: VPN port

        Returns:
            True if added successfully
        """
        with self._lock:
            if len(self._vpn_peers) >= MAX_VPN_PEERS and pubkey not in self._vpn_peers:
                self._log(f"Cannot add peer {pubkey[:16]}...: max peers reached", level='warn')
                return False

            self._vpn_peers[pubkey] = VPNPeerMapping(
                pubkey=pubkey,
                vpn_ip=vpn_ip,
                vpn_port=vpn_port
            )
        self._log(f"Added VPN peer mapping: {pubkey[:16]}... -> {vpn_ip}:{vpn_port}")
        return True

    def remove_vpn_peer(self, pubkey: str) -> bool:
        """
        Remove a VPN peer mapping.

        Args:
            pubkey: Node pubkey

        Returns:
            True if removed
        """
        with self._lock:
            if pubkey in self._vpn_peers:
                del self._vpn_peers[pubkey]
                self._log(f"Removed VPN peer mapping: {pubkey[:16]}...")
                return True
            return False

    def _get_or_create_connection_info(self, peer_id: str) -> VPNConnectionInfo:
        """Get or create connection info for a peer."""
        with self._lock:
            if peer_id not in self._peer_connections:
                if len(self._peer_connections) > 500:
                    # Evict oldest entry
                    oldest_key = min(self._peer_connections, key=lambda k: self._peer_connections[k].last_verified)
                    del self._peer_connections[oldest_key]
                self._peer_connections[peer_id] = VPNConnectionInfo(peer_id=peer_id)
            return self._peer_connections[peer_id]

    # =========================================================================
    # CONNECTION EVENTS
    # =========================================================================

    def on_peer_connected(self, peer_id: str, address: Optional[str] = None) -> Dict[str, Any]:
        """
        Handle peer connection event.

        Args:
            peer_id: Connected peer's pubkey
            address: Connection address if known

        Returns:
            Connection info dictionary
        """
        conn_info = self._get_or_create_connection_info(peer_id)
        with self._lock:
            conn_info.connection_count += 1
            conn_info.last_verified = int(time.time())

            is_vpn = False
            if address:
                ip = self.extract_ip_from_address(address)
                if ip:
                    is_vpn = self.is_vpn_address(ip)
                    if is_vpn:
                        conn_info.vpn_ip = ip
                        conn_info.connected_via_vpn = True
                        self._stats["vpn_connections"] += 1
                        self._log(f"Peer {peer_id[:16]}... connected via VPN ({ip})")
                    else:
                        conn_info.connected_via_vpn = False
                        self._stats["non_vpn_connections"] += 1

        return {
            "peer_id": peer_id,
            "connected_via_vpn": is_vpn,
            "address": address
        }

    def on_peer_disconnected(self, peer_id: str) -> None:
        """
        Handle peer disconnection.

        Args:
            peer_id: Disconnected peer's pubkey
        """
        with self._lock:
            if peer_id in self._peer_connections:
                self._peer_connections[peer_id].connected_via_vpn = False

    # =========================================================================
    # STATUS AND DIAGNOSTICS
    # =========================================================================

    def get_status(self) -> Dict[str, Any]:
        """
        Get VPN transport status.

        Returns:
            Status dictionary
        """
        with self._lock:
            vpn_connected = [
                pid for pid, info in self._peer_connections.items()
                if info.connected_via_vpn
            ]

            return {
                "configured": self._configured,
                "mode": self._mode.value,
                "required_messages": [r.value for r in self._required_messages],
                "vpn_subnets": [str(s) for s in self._vpn_subnets],
                "vpn_bind": f"{self._vpn_bind[0]}:{self._vpn_bind[1]}" if self._vpn_bind else None,
                "configured_peers": len(self._vpn_peers),
                "vpn_connected_peers": vpn_connected,
                "vpn_connected_count": len(vpn_connected),
                "statistics": self._stats.copy(),
                "peer_mappings": {
                    k[:16] + "...": v.vpn_address
                    for k, v in self._vpn_peers.items()
                }
            }

    def get_peer_vpn_info(self, peer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get VPN info for a specific peer.

        Args:
            peer_id: Node pubkey

        Returns:
            Peer VPN info or None
        """
        result = {}

        with self._lock:
            # Check configured mapping
            if peer_id in self._vpn_peers:
                result["configured_mapping"] = self._vpn_peers[peer_id].to_dict()

            # Check connection info
            if peer_id in self._peer_connections:
                result["connection_info"] = self._peer_connections[peer_id].to_dict()

        return result if result else None

    def is_enabled(self) -> bool:
        """Check if VPN transport is actively enforcing policy."""
        return self._configured and self._mode != TransportMode.ANY

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def _log(self, message: str, level: str = 'info') -> None:
        """Log with optional plugin reference."""
        if self.plugin:
            self.plugin.log(f"vpn-transport: {message}", level=level)

    def reset_statistics(self) -> Dict[str, int]:
        """Reset and return statistics."""
        with self._lock:
            old_stats = self._stats.copy()
            self._stats = {
                "messages_accepted": 0,
                "messages_rejected": 0,
                "vpn_connections": 0,
                "non_vpn_connections": 0
            }
        return old_stats
