# Bitcoin RPC Recovery Runbook

## Severity: HIGH

Use this runbook when the Lightning node loses connection to Bitcoin Core RPC.

## Symptoms

- Lightning node shows "bitcoind: not fully synced" errors
- Unable to open/close channels
- Payment routing fails
- Logs show: `bitcoin-cli: Connection refused`

## Diagnosis

### Step 1: Check Lightning Node Status

```bash
# Check if node is running
docker-compose exec cln lightning-cli getinfo

# Expected error if Bitcoin RPC is down:
# "warning_bitcoind_sync": "Bitcoind is not synced to the network"
```

### Step 2: Test Bitcoin RPC Directly

```bash
# From inside the container
docker-compose exec cln curl -s --user "$BITCOIN_RPCUSER:$BITCOIN_RPCPASSWORD" \
  --data-binary '{"jsonrpc":"1.0","method":"getblockchaininfo","params":[]}' \
  -H 'content-type: text/plain;' \
  "http://$BITCOIN_RPCHOST:$BITCOIN_RPCPORT/"

# From host (if Bitcoin is local)
bitcoin-cli getblockchaininfo
```

### Step 3: Check Network Connectivity

```bash
# Can we reach the host?
docker-compose exec cln ping -c 3 $BITCOIN_RPCHOST

# Is the port open?
docker-compose exec cln nc -zv $BITCOIN_RPCHOST $BITCOIN_RPCPORT
```

## Common Causes and Fixes

### Cause 1: Bitcoin Core Stopped

```bash
# Check if bitcoind is running (on Bitcoin host)
systemctl status bitcoind
# or
ps aux | grep bitcoind

# Restart Bitcoin Core
systemctl restart bitcoind
# or
bitcoind -daemon
```

### Cause 2: RPC Credentials Changed

1. Check your `.env` file:
   ```bash
   grep BITCOIN_RPC docker/.env
   ```

2. Compare with Bitcoin Core config:
   ```bash
   cat ~/.bitcoin/bitcoin.conf | grep rpc
   ```

3. Update `.env` if needed and restart:
   ```bash
   docker-compose restart
   ```

### Cause 3: Network/Firewall Issue

```bash
# On Bitcoin host, check if port is listening
ss -tlnp | grep 8332

# Check firewall
sudo ufw status
sudo iptables -L -n | grep 8332
```

### Cause 4: Bitcoin Core Syncing

```bash
# Check sync progress
bitcoin-cli getblockchaininfo | grep -E "blocks|headers|verificationprogress"

# Wait for sync to complete (verificationprogress should be ~1.0)
```

### Cause 5: WireGuard VPN Down (if using VPN)

```bash
# Check WireGuard status
docker-compose exec cln wg show

# If no output or errors, restart WireGuard
docker-compose exec cln wg-quick down wg0
docker-compose exec cln wg-quick up wg0
```

## Recovery Steps

### Quick Recovery

```bash
# Step 1: Verify Bitcoin RPC is working
bitcoin-cli getblockchaininfo

# Step 2: Restart Lightning container
docker-compose restart

# Step 3: Wait for sync
docker-compose logs -f cln | grep -i bitcoin

# Step 4: Verify recovery
docker-compose exec cln lightning-cli getinfo
```

### Full Recovery (if quick fails)

```bash
# Step 1: Stop Lightning
docker-compose stop

# Step 2: Verify Bitcoin RPC config
cat docker/.env | grep BITCOIN

# Step 3: Test connection manually
curl -s --user "USER:PASS" \
  --data-binary '{"jsonrpc":"1.0","method":"getblockchaininfo"}' \
  "http://HOST:PORT/"

# Step 4: Fix any issues found

# Step 5: Restart
docker-compose up -d

# Step 6: Monitor
docker-compose logs -f cln
```

## Prevention

1. **Monitor Bitcoin Core**: Set up alerting for bitcoind process
2. **Redundant RPC**: Consider multiple Bitcoin backends
3. **Network Monitoring**: Alert on connectivity issues
4. **Automatic Restart**: Configure systemd restart for bitcoind

## Impact Assessment

While Bitcoin RPC is down:
- **Channels**: Existing channels stay open but may become stale
- **Payments**: Outgoing payments fail
- **Routing**: Cannot route payments (removed from pathfinding)
- **Force-closes**: Cannot detect if peers try to cheat

**Maximum safe downtime**: ~1-2 hours before channel state becomes concerning

## Related Runbooks

- [Emergency Shutdown](./emergency-shutdown.md)
- [Tor Recovery](./tor-recovery.md)
