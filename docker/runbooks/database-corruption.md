# Database Corruption Runbook

## Severity: CRITICAL

Use this runbook when you suspect database corruption in Lightning or plugin databases.

## Symptoms

- Node crashes on startup
- Logs show: `SQLITE_CORRUPT`, `database disk image is malformed`
- Commands fail with database errors
- Missing channels or incorrect balances

## Critical Warning

**DO NOT** run the node with corrupted database - you may lose funds!

## Diagnosis

### Step 1: Stop the Node

```bash
# Stop immediately if running
docker-compose stop

# Do NOT restart until database is verified/fixed
```

### Step 2: Check Database Integrity

```bash
# Lightning database (CRITICAL - contains channel state)
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 \
  apt-get update && apt-get install -y sqlite3 && \
  sqlite3 /data/lightning/bitcoin/lightningd.sqlite3 "PRAGMA integrity_check"

# Expected output: "ok"
# Any other output indicates corruption

# cl-hive database
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 \
  sqlite3 /data/lightning/bitcoin/cl-hive.db "PRAGMA integrity_check"

# cl-revenue-ops database
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 \
  sqlite3 /data/lightning/bitcoin/cl-revenue-ops.db "PRAGMA integrity_check"
```

### Step 3: Check WAL Files

```bash
# List database files
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 \
  ls -la /data/lightning/bitcoin/*.sqlite3*

# If -wal or -shm files exist, they need to be properly committed
```

## Recovery Procedures

### Scenario 1: Lightning Database Corrupted

**This is the most critical scenario - contains channel state and funds.**

```bash
# Step 1: DO NOT start the node

# Step 2: Check if you have a recent backup
ls -la /backups/

# Step 3: Restore from backup
./scripts/restore.sh --latest

# Step 4: Verify restoration
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 \
  sqlite3 /data/lightning/bitcoin/lightningd.sqlite3 "PRAGMA integrity_check"

# Step 5: Start and verify
docker-compose up -d
docker-compose exec cln lightning-cli listpeerchannels
```

**If no backup available:**
- Contact Core Lightning support
- Your counterparties can force-close channels
- Your hsm_secret can recover on-chain funds

### Scenario 2: cl-hive Database Corrupted

Less critical - no funds at risk.

```bash
# Option A: Restore from backup
# Check if hive db was backed up
tar -tzf /backups/backup_latest/config.tar.gz | grep hive

# Restore just the hive database
./scripts/restore.sh --config-only /backups/backup_latest

# Option B: Delete and recreate
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 \
  rm /data/lightning/bitcoin/cl-hive.db

# The plugin will recreate on next start
# You'll need to rejoin the hive
docker-compose up -d
docker-compose exec cln lightning-cli hive-join "YOUR_INVITE_CODE"
```

### Scenario 3: cl-revenue-ops Database Corrupted

Not critical - historical data only.

```bash
# Delete and recreate
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 \
  rm /data/lightning/bitcoin/cl-revenue-ops.db

# Plugin recreates on startup
docker-compose up -d
docker-compose exec cln lightning-cli revenue-status
```

### Scenario 4: WAL File Issues

```bash
# Try to checkpoint the WAL
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 \
  sqlite3 /data/lightning/bitcoin/lightningd.sqlite3 "PRAGMA wal_checkpoint(TRUNCATE)"

# If that fails, try recovery mode
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 bash -c "
  sqlite3 /data/lightning/bitcoin/lightningd.sqlite3 '.dump' > /tmp/dump.sql
  mv /data/lightning/bitcoin/lightningd.sqlite3 /data/lightning/bitcoin/lightningd.sqlite3.corrupt
  rm -f /data/lightning/bitcoin/lightningd.sqlite3-wal
  rm -f /data/lightning/bitcoin/lightningd.sqlite3-shm
  sqlite3 /data/lightning/bitcoin/lightningd.sqlite3 < /tmp/dump.sql
"
```

## SQLite Recovery Tools

### Using `.recover`

```bash
# SQLite's built-in recovery
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 bash -c "
  apt-get update && apt-get install -y sqlite3
  sqlite3 /data/lightning/bitcoin/lightningd.sqlite3 '.recover' > /tmp/recovered.sql
  sqlite3 /data/lightning/bitcoin/lightningd_recovered.sqlite3 < /tmp/recovered.sql
"

# Then compare:
# - Check row counts
# - Verify critical tables exist
```

### Manual Table Recovery

```bash
# If specific tables are corrupted
docker run --rm -v cl-hive_lightning-data:/data ubuntu:24.04 bash -c "
  apt-get update && apt-get install -y sqlite3
  cd /data/lightning/bitcoin

  # Dump good tables
  sqlite3 lightningd.sqlite3 '.schema' > schema.sql
  sqlite3 lightningd.sqlite3 'SELECT * FROM channels' > channels.csv 2>/dev/null || true

  # Create new database with schema
  sqlite3 lightningd_new.sqlite3 < schema.sql

  # Import data
  # ... manual process for each table
"
```

## Prevention

1. **Regular Backups**: Run `./scripts/backup.sh` daily
2. **Proper Shutdown**: Always use `./scripts/pre-stop.sh`
3. **Power Protection**: UPS for hardware
4. **WAL Checkpoints**: Automatic in our config
5. **Monitor Disk Space**: Alert before full

## Database Files Reference

| File | Purpose | Criticality |
|------|---------|-------------|
| `lightningd.sqlite3` | Lightning node state | CRITICAL - funds |
| `lightningd.sqlite3-wal` | Write-ahead log | Part of above |
| `lightningd.sqlite3-shm` | Shared memory | Part of above |
| `cl-hive.db` | Hive membership/state | Medium |
| `cl-revenue-ops.db` | Revenue tracking | Low |
| `gossip_store` | Network graph | Low - rebuilds |

## Emergency Contacts

If you have significant funds at risk:
- Core Lightning GitHub Issues
- Bitcoin Development mailing list
- Your backup files (hsm_secret especially)

## Post-Recovery Checklist

- [ ] Verify database integrity: `PRAGMA integrity_check`
- [ ] Check channel states: `listpeerchannels`
- [ ] Verify balances: `listfunds`
- [ ] Check for pending HTLCs
- [ ] Reconnect to peers
- [ ] Verify hive membership
- [ ] Create fresh backup
- [ ] Document the incident

## Related Runbooks

- [Emergency Shutdown](./emergency-shutdown.md)
- [Channel Force Close](./channel-force-close.md)
