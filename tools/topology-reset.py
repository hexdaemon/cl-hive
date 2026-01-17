#!/usr/bin/env python3
r"""
Topology Reset and Market Share Growth Simulation

This script resets the Polar network to a sparse, clustered topology
where hive nodes act as bridges between clusters. This allows testing
how the hive gains routing market share.

Topology Design:

    [Cluster A]              [Cluster B]
    oscar --- pat            lnd1 --- lnd2
       \     /                  \     /
        dave                    (via bob)
          |                        |
    ======|======  HIVE  ==========|==========
          |                        |
       alice ------ bob ------ carol
          |                        |
    ======|========================|==========
          |
        erin

    [Cluster C]

Usage:
    python3 topology-reset.py status      # Show current topology
    python3 topology-reset.py close-all   # Close all channels
    python3 topology-reset.py create      # Create new sparse topology
    python3 topology-reset.py simulate    # Run market share growth simulation
    python3 topology-reset.py full        # Full reset + create + simulate
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

# Configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
NETWORK_ID = "1"

# Channel size (in sats)
DEFAULT_CHANNEL_SIZE = 1_000_000  # 1M sats
LARGE_CHANNEL_SIZE = 2_000_000   # 2M sats for hive backbone

# CLI paths
CLN_CLI_PATH = "/usr/local/bin/lightning-cli"
CLN_CLI_ARGS = ["--lightning-dir=/home/clightning/.lightning", "--network=regtest"]
LND_CLI_PATH = "/opt/lnd/lncli"
LND_CLI_ARGS = ["--lnddir=/home/lnd/.lnd", "--network=regtest"]
LND_USER = "lnd"  # LND containers need to run as lnd user
BITCOIN_CLI_PATH = "/opt/bitcoin-30.0/bin/bitcoin-cli"
BITCOIN_CLI_ARGS = ["-regtest", "-datadir=/home/bitcoin/.bitcoin"]

# Node types (only actual Lightning nodes, not charge-lnd containers)
CLN_NODES = ["alice", "bob", "carol", "dave", "erin", "oscar", "pat"]
LND_NODES = ["lnd1", "lnd2"]
BITCOIN_NODE = "backend1"

# Hive members (have cl-hive plugin)
HIVE_NODES = ["alice", "bob", "carol"]

# Target topology: sparse clusters with hive as bridges
# Available nodes: alice, bob, carol (hive), dave, erin, oscar, pat (CLN), lnd1, lnd2 (LND)
#
# Topology design:
#
#     [Cluster A]              [Cluster B]
#     oscar --- pat            lnd1 --- lnd2
#        \     /                  \     /
#         dave                    (via bob)
#           |                        |
#     ======|======  HIVE  ==========|==========
#           |                        |
#        alice ------ bob ------ carol
#           |                        |
#     ======|========================|==========
#           |
#         erin
#
#     [Cluster C]
#
# Format: (node1, node2, channel_size, opener)
TARGET_TOPOLOGY = [
    # === HIVE BACKBONE (high capacity triangle) ===
    ("alice", "bob", LARGE_CHANNEL_SIZE, "alice"),
    ("bob", "carol", LARGE_CHANNEL_SIZE, "bob"),
    ("alice", "carol", LARGE_CHANNEL_SIZE, "carol"),

    # === CLUSTER A: dave, oscar, pat (connected via alice) ===
    ("dave", "oscar", DEFAULT_CHANNEL_SIZE, "dave"),
    ("dave", "pat", DEFAULT_CHANNEL_SIZE, "dave"),
    ("oscar", "pat", DEFAULT_CHANNEL_SIZE, "oscar"),
    # Bridge to hive (ONLY alice connects to this cluster)
    ("alice", "dave", LARGE_CHANNEL_SIZE, "alice"),

    # === CLUSTER B: lnd1, lnd2 (connected via bob) ===
    # LND nodes connect to bob, creating a separate cluster
    ("bob", "lnd1", LARGE_CHANNEL_SIZE, "bob"),
    ("bob", "lnd2", LARGE_CHANNEL_SIZE, "bob"),

    # === CLUSTER C: erin (connected via carol) ===
    # Erin is isolated, only reachable via carol
    ("carol", "erin", LARGE_CHANNEL_SIZE, "carol"),
]


@dataclass
class ChannelInfo:
    """Channel information."""
    channel_id: str
    peer_id: str
    state: str
    capacity_sats: int
    local_sats: int
    funding_txid: str


class TopologyManager:
    """Manages Polar network topology."""

    def __init__(self):
        self.node_pubkeys: Dict[str, str] = {}

    def _docker_exec(self, container: str, cmd_args: List[str], user: Optional[str] = None) -> Tuple[bool, str]:
        """Execute command in docker container using list args (safe from injection)."""
        container_name = f"polar-n{NETWORK_ID}-{container}"
        full_cmd = ["docker", "exec"]
        if user:
            full_cmd.extend(["-u", user])
        full_cmd.append(container_name)
        full_cmd.extend(cmd_args)
        try:
            result = subprocess.run(full_cmd, capture_output=True, text=True, timeout=60)
            return result.returncode == 0, result.stdout
        except subprocess.TimeoutExpired:
            return False, "timeout"
        except Exception as e:
            return False, str(e)

    def _cln_rpc(self, node: str, method: str, params: Optional[List[str]] = None) -> Tuple[bool, Any]:
        """Call CLN RPC method."""
        cmd_args = [CLN_CLI_PATH] + CLN_CLI_ARGS + [method]
        if params:
            cmd_args.extend(params)
        success, output = self._docker_exec(node, cmd_args)
        if success and output:
            try:
                return True, json.loads(output)
            except json.JSONDecodeError:
                return True, output.strip()
        return False, output

    def _lnd_rpc(self, node: str, method: str, params: Optional[List[str]] = None) -> Tuple[bool, Any]:
        """Call LND RPC method."""
        cmd_args = [LND_CLI_PATH] + LND_CLI_ARGS + [method]
        if params:
            cmd_args.extend(params)
        success, output = self._docker_exec(node, cmd_args, user=LND_USER)
        if success and output:
            try:
                return True, json.loads(output)
            except json.JSONDecodeError:
                return True, output.strip()
        return False, output

    def _bitcoin_rpc(self, method: str, params: Optional[List[str]] = None) -> Tuple[bool, Any]:
        """Call Bitcoin RPC method."""
        cmd_args = [BITCOIN_CLI_PATH] + BITCOIN_CLI_ARGS + [method]
        if params:
            cmd_args.extend(params)
        success, output = self._docker_exec(BITCOIN_NODE, cmd_args)
        if success and output:
            try:
                return True, json.loads(output)
            except json.JSONDecodeError:
                return True, output.strip()
        return False, output

    def _mine_blocks(self, count: int = 6) -> bool:
        """Mine blocks to confirm transactions."""
        # Get an address to mine to
        success, result = self._bitcoin_rpc("-generate", [str(count)])
        if success:
            print(f"  Mined {count} blocks")
            return True
        # Try alternative method
        success, addr = self._bitcoin_rpc("getnewaddress")
        if success:
            success, result = self._bitcoin_rpc("generatetoaddress", [str(count), addr.strip()])
            if success:
                print(f"  Mined {count} blocks")
                return True
        print(f"  Failed to mine blocks: {result}")
        return False

    def _get_pubkey(self, node: str) -> Optional[str]:
        """Get node pubkey (cached)."""
        if node in self.node_pubkeys:
            return self.node_pubkeys[node]

        if node in CLN_NODES:
            success, info = self._cln_rpc(node, "getinfo")
            if success and isinstance(info, dict):
                pubkey = info.get("id")
                if pubkey:
                    self.node_pubkeys[node] = pubkey
                    return pubkey
        elif node in LND_NODES:
            success, info = self._lnd_rpc(node, "getinfo")
            if success and isinstance(info, dict):
                pubkey = info.get("identity_pubkey")
                if pubkey:
                    self.node_pubkeys[node] = pubkey
                    return pubkey
        return None

    def _get_node_address(self, node: str) -> Optional[str]:
        """Get node's network address for connection."""
        container = f"polar-n{NETWORK_ID}-{node}"

        if node in CLN_NODES:
            success, info = self._cln_rpc(node, "getinfo")
            if success and isinstance(info, dict):
                pubkey = info.get("id")
                return f"{pubkey}@{container}:9735"
        elif node in LND_NODES:
            success, info = self._lnd_rpc(node, "getinfo")
            if success and isinstance(info, dict):
                pubkey = info.get("identity_pubkey")
                return f"{pubkey}@{container}:9735"
        return None

    def _get_channels(self, node: str) -> List[ChannelInfo]:
        """Get all channels for a node."""
        channels = []

        if node in CLN_NODES:
            success, result = self._cln_rpc(node, "listpeerchannels")
            if success and isinstance(result, dict):
                for ch in result.get("channels", []):
                    total = ch.get("total_msat", 0)
                    if isinstance(total, str):
                        total = int(total.replace("msat", ""))
                    local = ch.get("to_us_msat", 0)
                    if isinstance(local, str):
                        local = int(local.replace("msat", ""))

                    channels.append(ChannelInfo(
                        channel_id=ch.get("short_channel_id", ch.get("channel_id", "unknown")),
                        peer_id=ch.get("peer_id", ""),
                        state=ch.get("state", "unknown"),
                        capacity_sats=total // 1000,
                        local_sats=local // 1000,
                        funding_txid=ch.get("funding_txid", "")
                    ))
        elif node in LND_NODES:
            success, result = self._lnd_rpc(node, "listchannels")
            if success and isinstance(result, dict):
                for ch in result.get("channels", []):
                    channels.append(ChannelInfo(
                        channel_id=ch.get("chan_id", "unknown"),
                        peer_id=ch.get("remote_pubkey", ""),
                        state="active" if ch.get("active") else "inactive",
                        capacity_sats=int(ch.get("capacity", 0)),
                        local_sats=int(ch.get("local_balance", 0)),
                        funding_txid=ch.get("channel_point", "").split(":")[0]
                    ))
        return channels

    def _connect_nodes(self, node1: str, node2: str) -> bool:
        """Connect two nodes as peers."""
        addr = self._get_node_address(node2)
        if not addr:
            print(f"    Could not get address for {node2}")
            return False

        if node1 in CLN_NODES:
            success, result = self._cln_rpc(node1, "connect", [addr])
            if success:
                return True
            if "already" in str(result).lower():
                return True
        elif node1 in LND_NODES:
            parts = addr.split("@")
            if len(parts) == 2:
                success, result = self._lnd_rpc(node1, "connect", [parts[0], parts[1]])
                if success:
                    return True
                if "already" in str(result).lower():
                    return True
        return False

    def _open_channel(self, opener: str, peer: str, size_sats: int) -> bool:
        """Open a channel from opener to peer."""
        peer_pubkey = self._get_pubkey(peer)
        if not peer_pubkey:
            print(f"    Could not get pubkey for {peer}")
            return False

        if not self._connect_nodes(opener, peer):
            print(f"    Could not connect {opener} to {peer}")
            return False

        time.sleep(1)

        if opener in CLN_NODES:
            success, result = self._cln_rpc(opener, "fundchannel", [peer_pubkey, str(size_sats)])
            if success:
                return True
            print(f"    Channel open failed: {result}")
        elif opener in LND_NODES:
            success, result = self._lnd_rpc(opener, "openchannel",
                [f"--node_key={peer_pubkey}", f"--local_amt={size_sats}"])
            if success:
                return True
            print(f"    Channel open failed: {result}")
        return False

    def _close_channel(self, node: str, channel: ChannelInfo) -> bool:
        """Close a channel."""
        if node in CLN_NODES:
            success, result = self._cln_rpc(node, "close", [channel.channel_id])
            return success
        elif node in LND_NODES:
            success, result = self._lnd_rpc(node, "closechannel",
                [f"--funding_txid={channel.funding_txid}", "--output_index=0"])
            return success
        return False

    # =========================================================================
    # COMMANDS
    # =========================================================================

    def status(self):
        """Show current network topology."""
        print("\n" + "=" * 60)
        print("CURRENT NETWORK TOPOLOGY")
        print("=" * 60)

        all_nodes = CLN_NODES + LND_NODES
        total_channels = 0
        total_capacity = 0

        for node in all_nodes:
            pubkey = self._get_pubkey(node)
            if not pubkey:
                print(f"\n{node}: NOT REACHABLE")
                continue

            channels = self._get_channels(node)
            active = [c for c in channels if c.state in ["CHANNELD_NORMAL", "active"]]

            is_hive = node in HIVE_NODES
            label = "[HIVE]" if is_hive else "[EXT]"

            print(f"\n{label} {node} ({pubkey[:12]}...)")
            print(f"  Active channels: {len(active)}")

            for ch in active:
                peer_name = "unknown"
                for n, pk in self.node_pubkeys.items():
                    if pk == ch.peer_id:
                        peer_name = n
                        break
                print(f"    -> {peer_name}: {ch.capacity_sats:,} sats (local: {ch.local_sats:,})")
                total_channels += 1
                total_capacity += ch.capacity_sats

        print(f"\n" + "-" * 40)
        print(f"Total unique channels: ~{total_channels // 2}")
        print(f"Total capacity: ~{total_capacity // 2:,} sats")

    def close_all(self):
        """Close all channels in the network."""
        print("\n" + "=" * 60)
        print("CLOSING ALL CHANNELS")
        print("=" * 60)

        closed_count = 0

        for node in CLN_NODES:
            channels = self._get_channels(node)
            active = [c for c in channels if c.state == "CHANNELD_NORMAL"]

            if not active:
                continue

            print(f"\n{node}: closing {len(active)} channels...")

            for ch in active:
                print(f"  Closing {ch.channel_id}...", end=" ")
                if self._close_channel(node, ch):
                    print("OK")
                    closed_count += 1
                else:
                    print("FAILED")

        if closed_count > 0:
            print(f"\nMining blocks to confirm {closed_count} channel closes...")
            self._mine_blocks(6)
            time.sleep(5)
            self._mine_blocks(6)

        print(f"\nClosed {closed_count} channels")

    def create(self):
        """Create the new sparse topology."""
        print("\n" + "=" * 60)
        print("CREATING SPARSE CLUSTERED TOPOLOGY")
        print("=" * 60)

        print("\n[1/3] Discovering nodes...")
        for node in CLN_NODES + LND_NODES:
            pubkey = self._get_pubkey(node)
            if pubkey:
                print(f"  + {node}: {pubkey[:16]}...")
            else:
                print(f"  x {node}: not reachable")

        print("\n[2/3] Ensuring nodes have funds...")
        for node in CLN_NODES:
            success, funds = self._cln_rpc(node, "listfunds")
            if success and isinstance(funds, dict):
                outputs = funds.get("outputs", [])
                total = sum(o.get("amount_msat", 0) for o in outputs)
                if isinstance(total, int):
                    total_sats = total // 1000
                else:
                    total_sats = 0
                print(f"  {node}: {total_sats:,} sats available")

                if total_sats < 10_000_000:
                    success, addr = self._cln_rpc(node, "newaddr")
                    if success and isinstance(addr, dict):
                        address = addr.get("bech32")
                        if address:
                            self._bitcoin_rpc("sendtoaddress", [address, "1.0"])
                            print(f"    Funded {node} with 1 BTC")

        self._mine_blocks(6)
        time.sleep(3)

        print("\n[3/3] Creating channels...")
        created = 0
        failed = 0

        for node1, node2, size, opener in TARGET_TOPOLOGY:
            print(f"  {opener} -> {node2} ({size:,} sats)...", end=" ")

            channels = self._get_channels(opener)
            peer_pubkey = self._get_pubkey(node2)
            existing = [c for c in channels if c.peer_id == peer_pubkey and c.state == "CHANNELD_NORMAL"]

            if existing:
                print("EXISTS")
                continue

            if self._open_channel(opener, node2, size):
                print("OK")
                created += 1
                if created % 3 == 0:
                    self._mine_blocks(1)
            else:
                print("FAILED")
                failed += 1

        print("\nMining blocks to confirm channels...")
        self._mine_blocks(6)
        time.sleep(5)
        self._mine_blocks(6)

        print(f"\nCreated {created} channels, {failed} failed")

    def simulate(self):
        """Run market share growth simulation."""
        print("\n" + "=" * 60)
        print("MARKET SHARE GROWTH SIMULATION")
        print("=" * 60)

        sys.path.insert(0, str(SCRIPT_DIR))

        # Import inline to avoid circular dependency
        try:
            from hive_simulation import HiveSimulation
        except ImportError:
            print("ERROR: Could not import hive_simulation module")
            print("Make sure hive-simulation.py exists in tools/")
            return

        sim = HiveSimulation()

        print("\n--- PHASE 1: BASELINE MEASUREMENT ---")
        sim.measure()

        print("\n--- PHASE 2: TRAFFIC GENERATION (5 min) ---")
        sim.generate_traffic(5)

        print("\n--- PHASE 3: FINAL MEASUREMENT ---")
        sim.measure()

        print("\n" + "=" * 60)
        print("SIMULATION COMPLETE")
        print("=" * 60)

    def full(self):
        """Run full reset: close all, create topology, simulate."""
        print("\n" + "=" * 60)
        print("FULL TOPOLOGY RESET AND SIMULATION")
        print("=" * 60)
        print(f"Started at: {datetime.now().isoformat()}")

        print("\n>>> CURRENT STATUS")
        self.status()

        print("\n>>> CLOSING ALL CHANNELS")
        self.close_all()

        print("\nWaiting 30 seconds for channel closes to propagate...")
        time.sleep(30)

        print("\n>>> CREATING NEW TOPOLOGY")
        self.create()

        print("\nWaiting 30 seconds for channels to become active...")
        time.sleep(30)

        print("\n>>> NEW TOPOLOGY STATUS")
        self.status()

        print("\n>>> RUNNING SIMULATION")
        self.simulate()

        print(f"\nCompleted at: {datetime.now().isoformat()}")


def main():
    parser = argparse.ArgumentParser(description="Topology Reset and Market Share Simulation")
    parser.add_argument("command", choices=["status", "close-all", "create", "simulate", "full"],
                       help="Command to run")

    args = parser.parse_args()

    mgr = TopologyManager()

    if args.command == "status":
        mgr.status()
    elif args.command == "close-all":
        mgr.close_all()
    elif args.command == "create":
        mgr.create()
    elif args.command == "simulate":
        mgr.simulate()
    elif args.command == "full":
        mgr.full()


if __name__ == "__main__":
    main()
