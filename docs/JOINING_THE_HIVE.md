# Joining the Hive - Quick Start Guide

This guide covers how to join an existing cl-hive fleet using the Docker image.

## Prerequisites

- Docker and Docker Compose installed
- Bitcoin Core node (mainnet) with RPC access
- On-chain funds for opening a channel (skin in the game)
- Contact with an existing hive member willing to sponsor you

## Membership Flow Overview

```
1. Get invite ticket from existing member
2. Start your node and connect to the member
3. Open a channel (skin in the game)
4. Use the ticket to join as neophyte
5. Request promotion
6. Members vouch for you
7. Automatic promotion when quorum reached
```

## Step 1: Clone and Configure

```bash
git clone https://github.com/lightning-goats/cl-hive.git
cd cl-hive/docker
cp .env.example .env
```

Edit `.env` with your configuration:

```bash
# Required: Your node alias
NODE_ALIAS=YourNodeName

# Required: Bitcoin Core RPC credentials
BITCOIN_RPC_HOST=your-bitcoin-node
BITCOIN_RPC_USER=your-rpc-user
BITCOIN_RPC_PASSWORD=your-rpc-password

# Network ports (adjust if needed)
LIGHTNING_PORT=9735
REST_PORT=3001
```

## Step 2: Start the Node

```bash
docker-compose up -d
```

Wait for the node to sync (check logs):

```bash
docker logs -f cl-hive-node
```

## Step 3: Get Invite Ticket from Member

Contact an existing hive member. They will:

1. Generate an invite ticket for you:
```bash
# Member runs this on their node:
lightning-cli hive-invite
```

2. Share the resulting ticket (a long base64 string) with you securely.

**Current Hive Members:**

| Node | Connection String |
|------|-------------------|
| ⚡Lightning Goats CLN⚡ (nexus-01) | `0382d558331b9a0c1d141f56b71094646ad6111e34e197d47385205019b03afdc3@45.76.234.192:9735` |
| Hive-Nexus-02 | `03fe48e8a64f14fa0aa7d9d16500754b3b906c729acfb867c00423fd4b0b9b56c2@45.76.234.192:9736` |

**Tor (onion) addresses:**
- nexus-01: `xsp4whqtphjnby335a3ihtje55gidhf4pnv3blrgustplyxfnpsgeuyd.onion:9735`
- nexus-02: `vxykasr6vdl77ph6hvo4a3mxfj2wbirwujdyrg4scowuhix7pp53l7yd.onion:9736`

**To request an invite ticket:**
- Nostr: `hex@lightning-goats.com` (npub1qkjnsgk6zrszkmk2c7ywycvh46ylp3kw4kud8y8a20m93y5synvqewl0sq)
- GitHub: Open an issue at https://github.com/lightning-goats/cl-hive/issues

You can also find active members via Lightning network explorers or community channels.

## Step 4: Connect and Open Channel

**Skin in the game**: You open a channel to the member first, demonstrating commitment.

1. Connect to the member's node:
```bash
docker exec cl-hive-node lightning-cli connect <member-pubkey>@<host>:<port>
```

2. Open a channel (recommended: 1M+ sats):
```bash
docker exec cl-hive-node lightning-cli fundchannel <member-pubkey> 1000000
```

3. Wait for the channel to confirm (3+ confirmations).

## Step 5: Join the Hive

Use the invite ticket to register as a neophyte:

```bash
docker exec cl-hive-node lightning-cli hive-join <ticket>
```

This adds you to the hive membership database as a **neophyte**.

## Step 6: Request Promotion

Once you're a neophyte, request promotion to full member:

```bash
docker exec cl-hive-node lightning-cli hive-request-promotion
```

This broadcasts your promotion request to all members.

## Step 7: Get Vouched

Members can now vouch for your promotion:

```bash
# Members run this on their nodes:
lightning-cli hive-vouch <your-pubkey>
```

**Important**: Members can only vouch for peers who:
1. Are already registered as neophytes (via `hive-join`)
2. Have a pending promotion request (via `hive-request-promotion`)

When enough members vouch (51% quorum), you're automatically promoted to member.

## Step 8: Verify Membership

Check your membership status:

```bash
docker exec cl-hive-node lightning-cli hive-status
```

You should see yourself as a `member` once quorum is reached. During the vouching process, you'll appear as `neophyte`.

## Step 9: Register for Settlement

Generate and register your BOLT12 offer for receiving settlement payments:

```bash
docker exec cl-hive-node lightning-cli hive-settlement-generate-offer
```

This is required to participate in weekly fee distribution.

## Useful Commands

| Command | Description |
|---------|-------------|
| `hive-status` | View hive membership and health |
| `hive-members` | List all hive members |
| `hive-channels` | View hive channel status |
| `hive-fee-reports` | View gossiped fee data |
| `hive-distributed-settlement-status` | Check settlement status |
| `hive-settlement-calculate` | Preview settlement calculation |

## How Settlement Works

1. **Weekly cycle**: Settlements run for each ISO week (Mon-Sun)
2. **Automatic proposals**: Any member can propose settlement for the previous week
3. **Quorum voting**: Members verify the data hash and vote
4. **Distributed execution**: Each node pays their share via BOLT12

Fair share calculation:
- 30% weight: Channel capacity
- 60% weight: Routing volume (forwards)
- 10% weight: Uptime

## Updating Your Node

cl-hive supports **hot updates** - reload plugins without restarting the node or losing channel state.

### Hot Update Script (Recommended)

Use the built-in hot upgrade script:

```bash
cd cl-hive/docker/scripts

# Check for updates (dry run)
./hot-upgrade.sh --check

# Upgrade both cl-hive and cl-revenue-ops
./hot-upgrade.sh

# Upgrade only cl-hive
./hot-upgrade.sh hive

# Upgrade only cl-revenue-ops
./hot-upgrade.sh revenue
```

The script will:
1. Check for available updates
2. Pull the latest code
3. Restart only the upgraded plugins (no node downtime)

### Manual Hot Update

If you prefer manual control:

```bash
# Pull changes inside the container
docker exec cl-hive-node bash -c "cd /opt/cl-hive && git pull"

# Reload the plugin
docker exec cl-hive-node lightning-cli plugin stop /opt/cl-hive/cl-hive.py
docker exec cl-hive-node lightning-cli plugin start /opt/cl-hive/cl-hive.py
```

### Full Container Rebuild (Major Updates)

For major version updates or dependency changes, use the full upgrade script:

```bash
cd cl-hive/docker/scripts
./upgrade.sh
```

Or manually rebuild:

```bash
cd cl-hive/docker
git pull origin main
docker-compose build --no-cache
docker-compose down && docker-compose up -d
```

### Rollback

Use the rollback script:

```bash
cd cl-hive/docker/scripts
./rollback.sh
```

Or manually:

```bash
docker exec cl-hive-node bash -c "cd /opt/cl-hive && git checkout <previous-commit-hash>"
docker exec cl-hive-node lightning-cli plugin stop /opt/cl-hive/cl-hive.py
docker exec cl-hive-node lightning-cli plugin start /opt/cl-hive/cl-hive.py
```

### Data Persistence

Your Lightning data is stored in Docker volumes and persists across updates:
- `/data/lightning` - Channel database, keys, and state
- `/data/bitcoin` - Bitcoin data (if not using external node)

These volumes are NOT deleted by `docker-compose down`. Only `docker-compose down -v` removes volumes (avoid this unless you want to start fresh).

## Troubleshooting

### Node not connecting to peers
```bash
docker exec cl-hive-node lightning-cli listpeers
```
Ensure your firewall allows inbound connections on port 9735.

### Not receiving gossip
Check that you're connected to at least one hive member:
```bash
docker exec cl-hive-node lightning-cli hive-members
```

### Settlement shows 0 fees
Ensure cl-revenue-ops is running and the bridge is enabled:
```bash
docker exec cl-hive-node lightning-cli hive-backfill-fees
```

## Security Notes

- Keep your `hsm_secret` backed up securely
- The Docker container runs with restricted permissions
- Hive channels between members always use 0 fees
- All governance actions require cryptographic signatures

## Getting Help

- GitHub Issues: https://github.com/lightning-goats/cl-hive/issues
- Check logs: `docker logs cl-hive-node`
