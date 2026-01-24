# Joining the Hive - Quick Start Guide

This guide covers how to join an existing cl-hive fleet using the Docker image.

## Prerequisites

- Docker and Docker Compose installed
- Bitcoin Core node (mainnet) with RPC access
- On-chain funds for opening a channel (skin in the game)
- Contact with an existing hive member willing to vouch for you

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

## Step 3: Get Member Connection Info

Contact an existing hive member and request their connection info:
- Their node's pubkey
- Their connection address (pubkey@host:port)

**Current Hive Members:**

| Node | Connection String |
|------|-------------------|
| Hive-Nexus-01 | `0382d558331b9a0c1d141f56b71094646ad6111e34e197d47385205019b03afdc3@45.76.234.192:9735` |
| Hive-Nexus-02 | `03fe48e8a64f14fa0aa7d9d16500754b3b906c729acfb867c00423fd4b0b9b56c2@vxykasr6vdl77ph6hvo4a3mxfj2wbirwujdyrg4scowuhix7pp53l7yd.onion:9736` |

You can also find active members via Lightning network explorers or community channels.

## Step 4: Open Channel and Request Membership

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

4. The member will vouch for you:
```bash
# Member runs this on their node:
lightning-cli hive-vouch <your-pubkey>
```

**Note**: The original flow also works - a member can open a channel to you and then vouch. Both approaches are valid; opening the channel yourself shows commitment.

## Step 5: Verify Membership

Once vouched, check your membership status:

```bash
docker exec cl-hive-node lightning-cli hive-status
```

You should see yourself listed as a `neophyte`. After the probation period (or early promotion by member vote), you'll become a full `member`.

## Step 6: Register for Settlement

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

When new versions are released, update your node with these steps:

### Step 1: Pull Latest Changes

```bash
cd cl-hive
git pull origin main
```

### Step 2: Rebuild the Docker Image

```bash
cd docker
docker-compose build --no-cache
```

The `--no-cache` flag ensures all layers are rebuilt with the latest code.

### Step 3: Restart the Container

```bash
docker-compose down
docker-compose up -d
```

### Step 4: Verify the Update

Check that the node started correctly:

```bash
docker logs -f cl-hive-node
```

Verify the plugin loaded:

```bash
docker exec cl-hive-node lightning-cli plugin list | grep hive
```

### Quick Update (Single Command)

For a quick update when you're confident about the changes:

```bash
cd cl-hive && git pull && cd docker && docker-compose build --no-cache && docker-compose down && docker-compose up -d
```

### Preserving Data

Your Lightning data is stored in Docker volumes and persists across updates:
- `/data/lightning` - Channel database, keys, and state
- `/data/bitcoin` - Bitcoin data (if not using external node)

These volumes are NOT deleted by `docker-compose down`. Only `docker-compose down -v` removes volumes (avoid this unless you want to start fresh).

### Rollback

If an update causes issues, rollback to a previous version:

```bash
git checkout <previous-commit-hash>
cd docker
docker-compose build --no-cache
docker-compose down && docker-compose up -d
```

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
