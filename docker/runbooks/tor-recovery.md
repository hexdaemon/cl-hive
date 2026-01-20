# Tor Recovery Runbook

## Severity: MEDIUM

Use this runbook when Tor hidden service is not working properly.

## Symptoms

- Node unreachable via .onion address
- Peers cannot connect
- Logs show Tor-related errors
- `lightning-cli getinfo` shows no .onion in addresses

## Diagnosis

### Step 1: Check Tor Service Status

```bash
# Check if Tor is running in container
docker-compose exec cln pgrep -x tor

# Check Tor logs
docker-compose exec cln cat /var/log/tor/notices.log | tail -50

# Check supervisor status
docker-compose exec cln supervisorctl status tor
```

### Step 2: Verify Hidden Service

```bash
# Check if hidden service directory exists
docker-compose exec cln ls -la /var/lib/tor/cln-service/

# Check hostname (your .onion address)
docker-compose exec cln cat /var/lib/tor/cln-service/hostname

# Check private key exists
docker-compose exec cln ls -la /var/lib/tor/cln-service/hs_ed25519_secret_key
```

### Step 3: Check Lightning Tor Configuration

```bash
# Check if Lightning is using Tor
docker-compose exec cln lightning-cli getinfo | grep -i tor

# Check config
docker-compose exec cln cat /data/lightning/bitcoin/config | grep -i proxy
```

## Common Causes and Fixes

### Cause 1: Tor Service Not Running

```bash
# Restart Tor via supervisor
docker-compose exec cln supervisorctl restart tor

# Wait for bootstrap
sleep 30

# Check status
docker-compose exec cln supervisorctl status tor
```

### Cause 2: Permission Issues

```bash
# Fix Tor directory permissions
docker-compose exec cln chown -R debian-tor:debian-tor /var/lib/tor
docker-compose exec cln chmod 700 /var/lib/tor/cln-service

# Restart Tor
docker-compose exec cln supervisorctl restart tor
```

### Cause 3: Corrupted Hidden Service

```bash
# WARNING: This generates a NEW .onion address!
# Only do this if old address is not working

# Backup old keys first
docker-compose exec cln cp -r /var/lib/tor/cln-service /tmp/tor-backup

# Remove corrupted service
docker-compose exec cln rm -rf /var/lib/tor/cln-service

# Restart Tor to generate new
docker-compose exec cln supervisorctl restart tor

# Wait and get new address
sleep 30
docker-compose exec cln cat /var/lib/tor/cln-service/hostname
```

### Cause 4: Tor Network Issues

```bash
# Check Tor connectivity
docker-compose exec cln torsocks curl -s https://check.torproject.org/api/ip

# Check circuit status
docker-compose exec cln cat /var/lib/tor/state | grep -i circuit

# Force new circuits
docker-compose exec cln kill -HUP $(pgrep -x tor)
```

### Cause 5: Tor Configuration Error

```bash
# Check torrc
docker-compose exec cln cat /etc/tor/torrc

# Verify it contains:
# HiddenServiceDir /var/lib/tor/cln-service
# HiddenServicePort 9735 127.0.0.1:9735

# Test configuration
docker-compose exec cln tor --verify-config
```

## Recovery Steps

### Quick Recovery

```bash
# Step 1: Restart Tor
docker-compose exec cln supervisorctl restart tor

# Step 2: Wait for bootstrap (watch logs)
docker-compose exec cln tail -f /var/log/tor/notices.log

# Look for: "Bootstrapped 100%: Done"

# Step 3: Verify
docker-compose exec cln cat /var/lib/tor/cln-service/hostname
```

### Full Recovery

```bash
# Step 1: Stop everything
docker-compose stop

# Step 2: Recreate Tor config
cat > /tmp/torrc << 'EOF'
DataDirectory /var/lib/tor
HiddenServiceDir /var/lib/tor/cln-service
HiddenServicePort 9735 127.0.0.1:9735
HiddenServiceVersion 3
SocksPort 9050
Log notice file /var/log/tor/notices.log
EOF

docker cp /tmp/torrc cl-hive-node:/etc/tor/torrc

# Step 3: Fix permissions
docker-compose up -d
docker-compose exec cln chown -R debian-tor:debian-tor /var/lib/tor
docker-compose exec cln chown debian-tor:debian-tor /etc/tor/torrc

# Step 4: Restart
docker-compose restart

# Step 5: Verify
sleep 60
docker-compose exec cln cat /var/lib/tor/cln-service/hostname
docker-compose exec cln lightning-cli getinfo | grep -i address
```

## Preserving Your .onion Address

Your .onion address is derived from the private key in:
`/var/lib/tor/cln-service/hs_ed25519_secret_key`

**BACKUP THIS FILE** to preserve your address across reinstalls:

```bash
# Backup
docker cp cl-hive-node:/var/lib/tor/cln-service/hs_ed25519_secret_key ./tor-secret-key.backup

# Restore (on new install)
docker cp ./tor-secret-key.backup cl-hive-node:/var/lib/tor/cln-service/hs_ed25519_secret_key
docker-compose exec cln chown debian-tor:debian-tor /var/lib/tor/cln-service/hs_ed25519_secret_key
docker-compose exec cln chmod 600 /var/lib/tor/cln-service/hs_ed25519_secret_key
```

## Impact Assessment

While Tor is down:
- **Connectivity**: Tor-only peers cannot reach you
- **Privacy**: If you have clearnet too, node still works but less private
- **Channels**: Existing channels unaffected
- **Routing**: May be excluded from Tor-only routes

## Prevention

1. **Monitor Tor**: Alert on Tor process death
2. **Backup Keys**: Keep hidden service keys backed up
3. **Redundancy**: Consider clearnet+Tor hybrid
4. **Regular Checks**: Include Tor in health checks

## Related Runbooks

- [Emergency Shutdown](./emergency-shutdown.md)
- [Bitcoin RPC Recovery](./bitcoin-rpc-recovery.md)
