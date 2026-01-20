# Bridge Circuit Breaker Runbook

## Severity: MEDIUM

Use this runbook when the cl-hive ↔ cl-revenue-ops bridge is in a failed state.

## Background

The bridge uses a Circuit Breaker pattern:
- **CLOSED**: Normal operation, calls go through
- **OPEN**: Calls blocked after 3 failures, 60-second cooldown
- **HALF_OPEN**: Testing if service recovered

## Symptoms

- Revenue ops commands fail: `revenue-status`, `revenue-profitability`
- Hive status shows bridge disabled
- Logs show: `Bridge circuit breaker OPEN`
- Fee adjustments not happening

## Diagnosis

### Step 1: Check Bridge Status

```bash
# Check hive status for bridge info
docker-compose exec cln lightning-cli hive-status

# Look for:
# "bridge_enabled": false
# "bridge_status": "open"
```

### Step 2: Check cl-revenue-ops Plugin

```bash
# Verify plugin is loaded
docker-compose exec cln lightning-cli plugin list | grep revenue

# Try direct revenue-ops command
docker-compose exec cln lightning-cli revenue-status
```

### Step 3: Check Logs

```bash
# Bridge-specific logs
docker-compose logs cln | grep -i "bridge\|circuit"

# Revenue ops logs
docker-compose logs cln | grep -i "revenue-ops"
```

## Common Causes and Fixes

### Cause 1: cl-revenue-ops Plugin Not Loaded

```bash
# Check if plugin exists
docker-compose exec cln ls -la /root/.lightning/plugins/ | grep revenue

# Try to start it
docker-compose exec cln lightning-cli plugin start /opt/cl-revenue-ops/cl-revenue-ops.py

# If it fails, check Python
docker-compose exec cln python3 -c "from pyln.client import Plugin; print('OK')"
```

### Cause 2: Plugin Crashed

```bash
# Check for errors
docker-compose logs cln | grep -i "revenue.*error\|revenue.*exception" | tail -20

# Restart the plugin
docker-compose exec cln lightning-cli plugin stop cl-revenue-ops 2>/dev/null || true
docker-compose exec cln lightning-cli plugin start /opt/cl-revenue-ops/cl-revenue-ops.py
```

### Cause 3: RPC Timeout

```bash
# Check if system is overloaded
docker stats cl-hive-node --no-stream

# Check pending operations
docker-compose exec cln lightning-cli listhtlcs | grep pending

# If system is slow, wait and retry
```

### Cause 4: Database Lock

```bash
# Check for lock contention
docker-compose exec cln sqlite3 /data/lightning/bitcoin/cl-revenue-ops.db "PRAGMA journal_mode"

# If locked, may need restart
docker-compose restart
```

## Recovery Steps

### Quick Recovery: Reinitialize Bridge

```bash
# This resets the circuit breaker
docker-compose exec cln lightning-cli hive-reinit-bridge

# Verify
docker-compose exec cln lightning-cli hive-status | grep bridge
```

### Medium Recovery: Restart Plugins

```bash
# Stop both plugins
docker-compose exec cln lightning-cli plugin stop cl-hive 2>/dev/null || true
docker-compose exec cln lightning-cli plugin stop cl-revenue-ops 2>/dev/null || true

# Wait
sleep 5

# Start revenue-ops first
docker-compose exec cln lightning-cli plugin start /opt/cl-revenue-ops/cl-revenue-ops.py

# Wait for init
sleep 10

# Start cl-hive
docker-compose exec cln lightning-cli plugin start /opt/cl-hive/cl-hive.py

# Verify
docker-compose exec cln lightning-cli hive-status
docker-compose exec cln lightning-cli revenue-status
```

### Full Recovery: Container Restart

```bash
# If plugins won't cooperate
docker-compose restart

# Wait for full startup
sleep 60

# Verify both working
docker-compose exec cln lightning-cli plugin list
docker-compose exec cln lightning-cli hive-status
docker-compose exec cln lightning-cli revenue-status
```

## Circuit Breaker States

### Understanding States

```
CLOSED (healthy)
    │
    ├──[3 failures]──→ OPEN (blocking)
    │                    │
    │                    ├──[60s timeout]──→ HALF_OPEN (testing)
    │                    │                      │
    │                    │    ┌─[failure]───────┘
    │                    │    │
    │                    │    ▼
    │                    │  OPEN
    │                    │
    └──[success]───────────────────────────┘
```

### Manual State Transitions

```bash
# Force reset (OPEN → CLOSED)
docker-compose exec cln lightning-cli hive-reinit-bridge

# There's no direct way to set other states - they transition automatically
```

## Impact of Bridge Down

When bridge is OPEN:
- **Fee Management**: Stops, uses last known fees
- **Rebalancing**: Stops
- **Profitability Tracking**: Pauses
- **Routing**: Still works, just no dynamic fee adjustment

**This is by design** - the bridge failure shouldn't crash the node.

## Prevention

1. **Monitor Bridge**: Alert on circuit breaker state changes
2. **Resource Limits**: Ensure adequate CPU/memory
3. **Database Maintenance**: Regular VACUUM on databases
4. **Plugin Updates**: Keep both plugins compatible versions

## Monitoring Command

```bash
# Quick bridge health check
docker-compose exec cln lightning-cli hive-status | jq '.bridge // {}'

# Full diagnostic
docker-compose exec cln lightning-cli hive-status && \
docker-compose exec cln lightning-cli revenue-status
```

## Related Runbooks

- [Emergency Shutdown](./emergency-shutdown.md)
- [Database Corruption](./database-corruption.md)
