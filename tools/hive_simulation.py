#!/usr/bin/env python3
"""
Comprehensive Hive Market Share Simulation

This script tests how well the hive performs in gaining routing market share
by simulating realistic payment traffic and measuring routing metrics.

Usage:
    python3 hive-simulation.py setup       # Setup: add nodes to hive, start daemons
    python3 hive-simulation.py traffic     # Generate payment traffic
    python3 hive-simulation.py measure     # Measure routing market share
    python3 hive-simulation.py full        # Run complete simulation
    python3 hive-simulation.py cleanup     # Stop daemons and cleanup
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field

# Add parent directory for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Configuration
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
CONFIG_FILE = PROJECT_DIR / "nodes.json"
NETWORK_ID = "1"

# Node categories
INITIAL_HIVE_NODES = ["alice", "bob", "carol"]  # Already in hive (have cl-hive plugin)
NODES_TO_ADD = []  # No more nodes to add (dave/erin don't have cl-hive plugin)
OPTIONAL_HIVE_NODES = []  # No optional nodes with plugin
NON_HIVE_CLN = ["dave", "erin", "oscar", "pat"]  # External CLN nodes (no hive plugin)
NON_HIVE_LND = []  # LND nodes excluded for now (different CLI setup)

# Simulation parameters
PAYMENT_INTERVAL_SECONDS = 2.0
MIN_PAYMENT_SATS = 1000
MAX_PAYMENT_SATS = 100000
SIMULATION_DURATION_MINUTES = 10

# CLI paths (used inside containers)
CLN_CLI_PATH = "/usr/local/bin/lightning-cli"
CLN_CLI_ARGS = ["--lightning-dir=/home/clightning/.lightning", "--network=regtest"]
LND_CLI_PATH = "/usr/local/bin/lncli"
LND_CLI_ARGS = ["--lnddir=/home/lnd/.lnd", "--network=regtest"]


@dataclass
class RoutingMetrics:
    """Track routing metrics over time."""
    timestamp: datetime
    node: str
    forwards_count: int
    forwards_sats: int
    fees_earned_msat: int

@dataclass
class SimulationState:
    """Track simulation state."""
    start_time: datetime = field(default_factory=datetime.now)
    payments_attempted: int = 0
    payments_succeeded: int = 0
    payments_failed: int = 0
    total_sats_moved: int = 0
    hive_forwards: int = 0
    non_hive_forwards: int = 0
    hive_fees_earned: int = 0
    non_hive_fees_earned: int = 0
    snapshots: List[Dict] = field(default_factory=list)


class HiveSimulation:
    """Comprehensive hive market share simulation."""

    def __init__(self, config_file: Path = CONFIG_FILE):
        self.config_file = config_file
        self.config = self._load_config()
        self.state = SimulationState()
        self.daemon_pids: Dict[str, int] = {}
        self.node_pubkeys: Dict[str, str] = {}

    def _load_config(self) -> Dict:
        """Load nodes configuration."""
        if not self.config_file.exists():
            print(f"Config file not found: {self.config_file}")
            print("Using default docker configuration")
            return {
                "mode": "docker",
                "network": "regtest",
                "lightning_dir": "/home/clightning/.lightning"
            }
        with open(self.config_file) as f:
            return json.load(f)

    def _docker_exec(self, container: str, cmd_args: List[str]) -> Tuple[bool, str]:
        """Execute command in docker container using execvp-style args (no shell)."""
        container_name = f"polar-n{NETWORK_ID}-{container}"
        full_cmd = ["docker", "exec", container_name] + cmd_args
        try:
            result = subprocess.run(
                full_cmd, capture_output=True, text=True, timeout=30
            )
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
                return True, output
        return False, output

    def _lnd_rpc(self, node: str, method: str, params: Optional[List[str]] = None) -> Tuple[bool, Any]:
        """Call LND RPC method."""
        cmd_args = [LND_CLI_PATH] + LND_CLI_ARGS + [method]
        if params:
            cmd_args.extend(params)
        success, output = self._docker_exec(node, cmd_args)
        if success and output:
            try:
                return True, json.loads(output)
            except json.JSONDecodeError:
                return True, output
        return False, output

    def _get_pubkey(self, node: str) -> Optional[str]:
        """Get node pubkey (cached)."""
        if node in self.node_pubkeys:
            return self.node_pubkeys[node]

        # Try CLN first
        success, info = self._cln_rpc(node, "getinfo")
        if success and isinstance(info, dict):
            pubkey = info.get("id")
            if pubkey:
                self.node_pubkeys[node] = pubkey
                return pubkey

        # Try LND
        success, info = self._lnd_rpc(node, "getinfo")
        if success and isinstance(info, dict):
            pubkey = info.get("identity_pubkey")
            if pubkey:
                self.node_pubkeys[node] = pubkey
                return pubkey

        return None

    def _get_forwarding_stats(self, node: str) -> Dict:
        """Get forwarding statistics for a node."""
        success, result = self._cln_rpc(node, "listforwards")
        if not success or not isinstance(result, dict):
            return {"count": 0, "sats": 0, "fees_msat": 0}

        forwards = result.get("forwards", [])
        settled = [f for f in forwards if f.get("status") == "settled"]

        total_msat = sum(f.get("out_msat", 0) for f in settled)
        if isinstance(total_msat, str):
            total_msat = int(total_msat.replace("msat", ""))

        fees_msat = sum(f.get("fee_msat", 0) for f in settled)
        if isinstance(fees_msat, str):
            fees_msat = int(fees_msat.replace("msat", ""))

        return {
            "count": len(settled),
            "sats": total_msat // 1000,
            "fees_msat": fees_msat
        }

    def _check_hive_membership(self, node: str) -> bool:
        """Check if node is a hive member."""
        # Get the node's pubkey
        pubkey = self._get_pubkey(node)
        if not pubkey:
            return False

        # Check hive-members from any hive node
        for hive_node in INITIAL_HIVE_NODES:
            success, result = self._cln_rpc(hive_node, "hive-members")
            if success and isinstance(result, dict):
                members = result.get("members", [])
                for m in members:
                    if isinstance(m, dict) and m.get("peer_id") == pubkey:
                        return True
                break  # Only need to check one node
        return False

    def _generate_invite_ticket(self, admin_node: str) -> Optional[str]:
        """Generate an invitation ticket for adding new members."""
        success, result = self._cln_rpc(admin_node, "hive-invite")
        if success and isinstance(result, dict):
            return result.get("ticket")
        return None

    def _add_node_to_hive(self, node: str, ticket: str) -> bool:
        """Add a node to the hive using genesis ticket."""
        success, result = self._cln_rpc(node, "hive-join", [ticket])
        if success:
            print(f"  + {node} joined the hive")
            return True
        print(f"  x Failed to add {node} to hive: {result}")
        return False

    def _send_keysend(self, from_node: str, to_pubkey: str, amount_msat: int) -> Tuple[bool, int]:
        """Send a keysend payment. Returns (success, fee_msat)."""
        # Try CLN keysend
        success, result = self._cln_rpc(
            from_node, "keysend", [to_pubkey, str(amount_msat)]
        )
        if success and isinstance(result, dict):
            status = result.get("status")
            if status == "complete":
                fee = result.get("msatoshi_sent", amount_msat) - amount_msat
                return True, fee
        return False, 0

    def _send_lnd_keysend(self, from_node: str, to_pubkey: str, amount_sats: int) -> Tuple[bool, int]:
        """Send keysend from LND node."""
        success, result = self._lnd_rpc(
            from_node, "sendpayment", [f"--dest={to_pubkey}", f"--amt={amount_sats}", "--keysend"]
        )
        if success and isinstance(result, dict):
            status = result.get("status")
            if status == "SUCCEEDED":
                fee = int(result.get("fee_msat", 0))
                return True, fee
        return False, 0

    # =========================================================================
    # SETUP COMMANDS
    # =========================================================================

    def setup(self, add_optional: bool = False) -> bool:
        """Setup simulation: add nodes to hive, start daemons."""
        print("\n" + "=" * 60)
        print("HIVE SIMULATION SETUP")
        print("=" * 60)

        # Step 1: Verify initial hive nodes
        print("\n[1/4] Verifying initial hive nodes...")
        for node in INITIAL_HIVE_NODES:
            pubkey = self._get_pubkey(node)
            if pubkey:
                is_member = self._check_hive_membership(node)
                status = "+ hive member" if is_member else "o not in hive"
                print(f"  {node}: {pubkey[:16]}... {status}")
            else:
                print(f"  {node}: x not reachable")

        # Step 2: Add new nodes to hive
        print("\n[2/4] Adding nodes to hive...")
        nodes_to_add = NODES_TO_ADD.copy()
        if add_optional:
            nodes_to_add.extend(OPTIONAL_HIVE_NODES)

        admin_node = INITIAL_HIVE_NODES[0]
        for node in nodes_to_add:
            pubkey = self._get_pubkey(node)
            if not pubkey:
                print(f"  {node}: x not reachable, skipping")
                continue

            # Check if already a member
            if self._check_hive_membership(node):
                print(f"  {node}: already a hive member")
                continue

            # Generate ticket and join
            print(f"  Adding {node} to hive...")
            ticket = self._generate_invite_ticket(admin_node)
            if ticket:
                self._add_node_to_hive(node, ticket)
            else:
                print(f"  x Failed to generate invite ticket")

        # Step 3: Verify hive topology
        print("\n[3/4] Verifying hive topology...")
        success, members_result = self._cln_rpc(admin_node, "hive-members")
        if success and isinstance(members_result, dict):
            members = members_result.get("members", [])
            print(f"  Hive has {len(members)} members")
            for m in members:
                if isinstance(m, dict):
                    peer_id = m.get("peer_id", "unknown")[:16]
                    tier = m.get("tier", "unknown")
                    # Try to find alias by matching pubkey
                    alias = "unknown"
                    for name, pk in self.node_pubkeys.items():
                        if pk == m.get("peer_id"):
                            alias = name
                            break
                    print(f"    - {alias} ({peer_id}...): {tier}")

        # Step 4: Start monitoring daemons
        print("\n[4/4] Starting monitoring daemons...")
        self._start_daemons()

        print("\n" + "=" * 60)
        print("SETUP COMPLETE")
        print("=" * 60)
        return True

    def _start_daemons(self):
        """Start ai_advisor and hive-monitor daemons."""
        # Start hive-monitor daemon
        monitor_script = str(SCRIPT_DIR / "hive-monitor.py")
        monitor_cmd = [sys.executable, monitor_script, "--daemon", "--interval", "60"]
        try:
            proc = subprocess.Popen(
                monitor_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True
            )
            self.daemon_pids["hive-monitor"] = proc.pid
            print(f"  + hive-monitor daemon started (PID: {proc.pid})")
        except Exception as e:
            print(f"  x Failed to start hive-monitor: {e}")

        # Start ai_advisor daemon
        advisor_script = str(SCRIPT_DIR / "ai_advisor.py")
        advisor_cmd = [sys.executable, advisor_script, "--daemon"]
        try:
            proc = subprocess.Popen(
                advisor_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True
            )
            self.daemon_pids["ai_advisor"] = proc.pid
            print(f"  + ai_advisor daemon started (PID: {proc.pid})")
        except Exception as e:
            print(f"  x Failed to start ai_advisor: {e}")

    # =========================================================================
    # TRAFFIC GENERATION
    # =========================================================================

    def generate_traffic(self, duration_minutes: int = SIMULATION_DURATION_MINUTES):
        """Generate payment traffic from non-hive nodes."""
        print("\n" + "=" * 60)
        print("GENERATING PAYMENT TRAFFIC")
        print("=" * 60)
        print(f"Duration: {duration_minutes} minutes")
        print(f"Payment interval: {PAYMENT_INTERVAL_SECONDS}s")
        print(f"Payment range: {MIN_PAYMENT_SATS}-{MAX_PAYMENT_SATS} sats")

        # Get all available node pubkeys
        print("\nDiscovering nodes...")
        all_nodes = (
            INITIAL_HIVE_NODES + NODES_TO_ADD +
            NON_HIVE_CLN + NON_HIVE_LND
        )
        available_nodes = []
        for node in all_nodes:
            pubkey = self._get_pubkey(node)
            if pubkey:
                available_nodes.append(node)
                print(f"  + {node}: {pubkey[:16]}...")
            else:
                print(f"  x {node}: not reachable")

        if len(available_nodes) < 2:
            print("ERROR: Need at least 2 nodes for traffic generation")
            return

        # Take initial snapshot
        print("\nTaking initial routing snapshot...")
        initial_stats = self._take_routing_snapshot()

        # Generate traffic
        print(f"\nGenerating traffic for {duration_minutes} minutes...")
        print("(Press Ctrl+C to stop early)\n")

        end_time = time.time() + (duration_minutes * 60)
        payment_count = 0

        try:
            while time.time() < end_time:
                # Select random sender (mix of hive and non-hive for bidirectional traffic)
                # This generates traffic that routes THROUGH nodes rather than just to them
                sender = random.choice(available_nodes)

                # Select random receiver (different from sender)
                receivers = [n for n in available_nodes if n != sender]
                if not receivers:
                    continue
                receiver = random.choice(receivers)

                # Generate payment amount (Pareto-like distribution)
                roll = random.randint(1, 100)
                if roll <= 80:
                    amount = random.randint(MIN_PAYMENT_SATS, 10000)
                elif roll <= 95:
                    amount = random.randint(10000, 50000)
                else:
                    amount = random.randint(50000, MAX_PAYMENT_SATS)

                # Send payment
                to_pubkey = self.node_pubkeys.get(receiver)
                if not to_pubkey:
                    continue

                payment_count += 1
                self.state.payments_attempted += 1

                # Try CLN keysend first, then LND
                if sender in NON_HIVE_LND:
                    success, fee = self._send_lnd_keysend(sender, to_pubkey, amount)
                else:
                    success, fee = self._send_keysend(sender, to_pubkey, amount * 1000)

                if success:
                    self.state.payments_succeeded += 1
                    self.state.total_sats_moved += amount
                    status = "+"
                else:
                    self.state.payments_failed += 1
                    status = "x"

                elapsed = int(time.time() - self.state.start_time.timestamp())
                print(f"  [{elapsed:4d}s] {status} {sender} -> {receiver}: {amount:,} sats")

                # Wait before next payment
                time.sleep(PAYMENT_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n\nTraffic generation interrupted by user")

        # Take final snapshot
        print("\nTaking final routing snapshot...")
        final_stats = self._take_routing_snapshot()

        # Report results
        self._report_traffic_results(initial_stats, final_stats)

    def _take_routing_snapshot(self) -> Dict[str, Dict]:
        """Take a snapshot of routing stats for all nodes."""
        snapshot = {}
        all_hive = INITIAL_HIVE_NODES + NODES_TO_ADD

        for node in all_hive + NON_HIVE_CLN:
            stats = self._get_forwarding_stats(node)
            snapshot[node] = stats
            is_hive = node in all_hive
            label = "[HIVE]" if is_hive else "[EXT]"
            print(f"  {label} {node}: {stats['count']} forwards, {stats['sats']:,} sats, {stats['fees_msat']} msat fees")

        return snapshot

    def _report_traffic_results(self, initial: Dict, final: Dict):
        """Report traffic generation results."""
        print("\n" + "=" * 60)
        print("TRAFFIC GENERATION RESULTS")
        print("=" * 60)

        print(f"\nPayments:")
        print(f"  Attempted: {self.state.payments_attempted}")
        print(f"  Succeeded: {self.state.payments_succeeded}")
        print(f"  Failed:    {self.state.payments_failed}")
        if self.state.payments_attempted > 0:
            success_rate = (self.state.payments_succeeded / self.state.payments_attempted) * 100
            print(f"  Success rate: {success_rate:.1f}%")
        print(f"  Total moved: {self.state.total_sats_moved:,} sats")

        # Calculate routing deltas
        print(f"\nRouting Changes:")
        all_hive = INITIAL_HIVE_NODES + NODES_TO_ADD
        hive_forwards = 0
        hive_fees = 0
        non_hive_forwards = 0
        non_hive_fees = 0

        for node in initial:
            init_stats = initial[node]
            final_stats = final.get(node, {"count": 0, "sats": 0, "fees_msat": 0})

            delta_forwards = final_stats["count"] - init_stats["count"]
            delta_sats = final_stats["sats"] - init_stats["sats"]
            delta_fees = final_stats["fees_msat"] - init_stats["fees_msat"]

            is_hive = node in all_hive
            label = "[HIVE]" if is_hive else "[EXT]"

            if delta_forwards > 0:
                print(f"  {label} {node}: +{delta_forwards} forwards, +{delta_sats:,} sats, +{delta_fees} msat")

                if is_hive:
                    hive_forwards += delta_forwards
                    hive_fees += delta_fees
                else:
                    non_hive_forwards += delta_forwards
                    non_hive_fees += delta_fees

        # Market share calculation
        total_forwards = hive_forwards + non_hive_forwards
        if total_forwards > 0:
            hive_share = (hive_forwards / total_forwards) * 100
            print(f"\nMarket Share:")
            print(f"  Hive forwards:     {hive_forwards} ({hive_share:.1f}%)")
            print(f"  Non-hive forwards: {non_hive_forwards} ({100-hive_share:.1f}%)")
            print(f"  Hive fees earned:  {hive_fees} msat")
            print(f"  Non-hive fees:     {non_hive_fees} msat")
        else:
            print("\nNo routing activity detected during simulation")

    # =========================================================================
    # MEASUREMENT
    # =========================================================================

    def measure(self):
        """Measure current hive routing market share."""
        print("\n" + "=" * 60)
        print("HIVE ROUTING MARKET SHARE")
        print("=" * 60)

        all_hive = INITIAL_HIVE_NODES + NODES_TO_ADD
        hive_stats = {"forwards": 0, "sats": 0, "fees": 0}
        non_hive_stats = {"forwards": 0, "sats": 0, "fees": 0}

        print("\nNode Statistics:")

        for node in all_hive:
            stats = self._get_forwarding_stats(node)
            hive_stats["forwards"] += stats["count"]
            hive_stats["sats"] += stats["sats"]
            hive_stats["fees"] += stats["fees_msat"]
            print(f"  [HIVE] {node}: {stats['count']} forwards, {stats['sats']:,} sats routed, {stats['fees_msat']} msat fees")

        for node in NON_HIVE_CLN:
            stats = self._get_forwarding_stats(node)
            non_hive_stats["forwards"] += stats["count"]
            non_hive_stats["sats"] += stats["sats"]
            non_hive_stats["fees"] += stats["fees_msat"]
            print(f"  [EXT]  {node}: {stats['count']} forwards, {stats['sats']:,} sats routed, {stats['fees_msat']} msat fees")

        total_forwards = hive_stats["forwards"] + non_hive_stats["forwards"]
        total_sats = hive_stats["sats"] + non_hive_stats["sats"]
        total_fees = hive_stats["fees"] + non_hive_stats["fees"]

        print("\n" + "-" * 40)
        print("MARKET SHARE SUMMARY")
        print("-" * 40)

        if total_forwards > 0:
            fwd_share = (hive_stats["forwards"] / total_forwards) * 100
            print(f"Forward count share: {fwd_share:.1f}% hive / {100-fwd_share:.1f}% external")

        if total_sats > 0:
            sat_share = (hive_stats["sats"] / total_sats) * 100
            print(f"Volume share:        {sat_share:.1f}% hive / {100-sat_share:.1f}% external")

        if total_fees > 0:
            fee_share = (hive_stats["fees"] / total_fees) * 100
            print(f"Fee revenue share:   {fee_share:.1f}% hive / {100-fee_share:.1f}% external")

        print(f"\nHive totals:     {hive_stats['forwards']} forwards, {hive_stats['sats']:,} sats, {hive_stats['fees']} msat")
        print(f"External totals: {non_hive_stats['forwards']} forwards, {non_hive_stats['sats']:,} sats, {non_hive_stats['fees']} msat")

    # =========================================================================
    # FULL SIMULATION
    # =========================================================================

    def full_simulation(self, duration_minutes: int = SIMULATION_DURATION_MINUTES, add_optional: bool = False):
        """Run complete simulation: setup, traffic, measure."""
        print("\n" + "=" * 60)
        print("COMPREHENSIVE HIVE SIMULATION")
        print("=" * 60)
        print(f"Started at: {datetime.now().isoformat()}")

        # Setup
        self.setup(add_optional=add_optional)

        # Wait for hive to stabilize
        print("\nWaiting 10 seconds for hive to stabilize...")
        time.sleep(10)

        # Initial measurement
        print("\n--- INITIAL STATE ---")
        self.measure()

        # Generate traffic
        self.generate_traffic(duration_minutes)

        # Final measurement
        print("\n--- FINAL STATE ---")
        self.measure()

        # Cleanup
        self.cleanup()

        print("\n" + "=" * 60)
        print("SIMULATION COMPLETE")
        print(f"Ended at: {datetime.now().isoformat()}")
        print("=" * 60)

    # =========================================================================
    # CLEANUP
    # =========================================================================

    def cleanup(self):
        """Stop daemons and cleanup."""
        print("\n[Cleanup] Stopping daemons...")

        for name, pid in self.daemon_pids.items():
            try:
                os.kill(pid, signal.SIGTERM)
                print(f"  + Stopped {name} (PID: {pid})")
            except ProcessLookupError:
                print(f"  - {name} already stopped")
            except Exception as e:
                print(f"  x Failed to stop {name}: {e}")


def main():
    parser = argparse.ArgumentParser(description="Hive Market Share Simulation")
    parser.add_argument("command", choices=["setup", "traffic", "measure", "full", "cleanup"],
                       help="Command to run")
    parser.add_argument("--duration", "-d", type=int, default=SIMULATION_DURATION_MINUTES,
                       help="Traffic duration in minutes (default: 10)")
    parser.add_argument("--add-optional", "-o", action="store_true",
                       help="Add optional nodes (dave, erin) to hive")

    args = parser.parse_args()

    sim = HiveSimulation()

    if args.command == "setup":
        sim.setup(add_optional=args.add_optional)
    elif args.command == "traffic":
        sim.generate_traffic(args.duration)
    elif args.command == "measure":
        sim.measure()
    elif args.command == "full":
        sim.full_simulation(args.duration, add_optional=args.add_optional)
    elif args.command == "cleanup":
        sim.cleanup()


if __name__ == "__main__":
    main()
