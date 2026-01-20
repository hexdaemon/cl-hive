# Emergency Shutdown Runbook

## Severity: CRITICAL

Use this runbook when you need to shut down the Lightning node immediately due to:
- Security breach
- Uncontrolled fund loss
- Critical bug exploitation
- Hardware failure
- Other emergencies

## Quick Shutdown (< 1 minute)

```bash
# FASTEST - kills all processes immediately
docker-compose kill

# Or from any directory
docker kill cl-hive-node
```

**Warning**: This may cause data loss and channel force-closes.

## Graceful Emergency Shutdown (2-5 minutes)

```bash
# Step 1: Stop accepting new HTLCs
docker-compose exec cln lightning-cli setchannel all null null null null false 2>/dev/null

# Step 2: Wait briefly for pending HTLCs (max 30 seconds)
sleep 30

# Step 3: Stop lightningd gracefully
docker-compose exec cln lightning-cli stop

# Step 4: Stop container
docker-compose stop -t 30
```

## If Docker is Unresponsive

```bash
# Find the process
ps aux | grep lightningd

# Kill directly
sudo kill -TERM <pid>

# If still running after 30 seconds
sudo kill -KILL <pid>
```

## If System is Unresponsive

1. **Hardware power-off** as last resort
2. On reboot, check data integrity:
   ```bash
   docker-compose exec cln sqlite3 /data/lightning/bitcoin/lightningd.sqlite3 "PRAGMA integrity_check"
   ```

## Post-Emergency Checklist

- [ ] Investigate root cause
- [ ] Check channel states: `lightning-cli listpeerchannels`
- [ ] Check for pending HTLCs: `lightning-cli listhtlcs`
- [ ] Verify database integrity
- [ ] Check peer connectivity
- [ ] Review logs for the incident
- [ ] Create incident report

## When NOT to Use Emergency Shutdown

- Routine maintenance (use `./scripts/pre-stop.sh` instead)
- Planned upgrades (use `./scripts/upgrade.sh`)
- Normal restarts (use `docker-compose restart`)

## Contact Points

- **Hive Admin**: [Configure in your deployment]
- **Node Operator**: [Your contact]
- **Emergency Channel**: [Slack/Discord/etc]

## Related Runbooks

- [Bitcoin RPC Recovery](./bitcoin-rpc-recovery.md)
- [Channel Force Close](./channel-force-close.md)
- [Database Corruption](./database-corruption.md)
