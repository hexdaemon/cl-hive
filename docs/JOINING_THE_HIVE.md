# Joining the Hive - Quick Start Guide

This guide covers how to join an existing cl-hive fleet using the Docker image.

## Prerequisites

- Docker and Docker Compose installed
- Bitcoin Core node (mainnet) with RPC access
- Existing hive admin's node pubkey for vouching

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

## Step 3: Get Your Node Info

Once synced, get your node's pubkey:

```bash
docker exec cl-hive-node lightning-cli getinfo
```

Note your `id` (pubkey) - you'll need this for the admin to vouch for you.

## Step 4: Request Membership

Contact an existing hive admin and provide:
- Your node's pubkey
- Your node's connection address (pubkey@host:port)

The admin will:
1. Connect to your node
2. Open a channel (hive channels use 0 fees)
3. Vouch for you with: `lightning-cli hive-vouch <your-pubkey>`

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
