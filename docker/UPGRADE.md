# cl-hive Upgrade Guide

This guide covers upgrading your cl-hive Docker deployment safely.

## Quick Upgrade

For most upgrades:

```bash
cd docker
./scripts/upgrade.sh
```

The upgrade script will:
1. Create a backup automatically
2. Build the new image
3. Stop the node gracefully
4. Start with the new version
5. Verify health
6. Rollback automatically if health checks fail

## Version Compatibility Matrix

| From Version | To Version | Notes |
|--------------|------------|-------|
| 0.1.x | 0.2.x | Database migration required (automatic) |
| 0.2.x | 0.3.x | Direct upgrade supported |
| Any | Latest | Always create backup first |

### Core Lightning Compatibility

| cl-hive Version | CLN Version | Notes |
|-----------------|-------------|-------|
| 0.1.x | 24.x, 25.x | Full support |
| 0.2.x | 25.x+ | Requires CLN 25.02+ |

## Pre-Upgrade Checklist

Before upgrading:

- [ ] Read the [release notes](https://github.com/LightningGoats/cl-hive/releases)
- [ ] Check disk space: `df -h` (need at least 5GB free)
- [ ] Verify current backup is recent: `./scripts/backup.sh --verify`
- [ ] Check channel states: `lightning-cli listpeerchannels`
- [ ] Note any pending HTLCs: `lightning-cli listhtlcs`
- [ ] Review pending actions: `lightning-cli hive-pending-actions`

## Upgrade Methods

### Method 1: Automatic Upgrade (Recommended)

```bash
# Preview what will happen
./scripts/upgrade.sh --dry-run

# Perform upgrade
./scripts/upgrade.sh

# Or upgrade to specific version
./scripts/upgrade.sh --version v0.2.0
```

### Method 2: Manual Upgrade

```bash
# Step 1: Create backup
./scripts/backup.sh

# Step 2: Stop gracefully
./scripts/pre-stop.sh
docker-compose stop

# Step 3: Pull latest code
cd ..
git pull
cd docker

# Step 4: Rebuild image
docker-compose build --no-cache

# Step 5: Start new version
docker-compose up -d

# Step 6: Verify
docker-compose exec cln lightning-cli getinfo
docker-compose exec cln lightning-cli hive-status
```

### Method 3: Zero-Downtime (Advanced)

For critical nodes where downtime must be minimized:

```bash
# Build new image while current is running
docker-compose build --no-cache

# Stop accepting new HTLCs (if supported)
docker-compose exec cln lightning-cli setchannel all null null null null false

# Wait for pending HTLCs (check every 10s)
while docker-compose exec cln lightning-cli listhtlcs | grep -q pending; do
  sleep 10
done

# Quick restart (< 30 seconds downtime)
docker-compose down && docker-compose up -d
```

## Rollback Procedures

### Automatic Rollback

If upgrade health checks fail, the upgrade script automatically rolls back.

### Manual Rollback

```bash
# List available backups
./scripts/rollback.sh --list

# Rollback to specific backup
./scripts/rollback.sh /backups/backup_20240101_120000

# Or rollback to latest backup
./scripts/rollback.sh --latest
```

### Emergency Rollback

If scripts don't work:

```bash
# Stop everything
docker-compose down

# Check backup exists
ls -la /backups/

# Manual restore
docker run --rm -v cl-hive_lightning-data:/data -v /backups/backup_latest:/backup \
  ubuntu bash -c "cp /backup/hsm/hsm_secret /data/lightning/bitcoin/ && \
                  tar xzf /backup/database.tar.gz -C /data/lightning/bitcoin/"

# Restart
docker-compose up -d
```

## Database Migrations

### Automatic Migrations

cl-hive handles database migrations automatically on startup. The plugin:
1. Detects current schema version
2. Applies any pending migrations
3. Logs migration status

### Manual Migration (if needed)

```bash
# Backup database first
docker-compose exec cln sqlite3 /data/lightning/bitcoin/cl-hive.db ".backup /tmp/hive.db.backup"
docker cp cl-hive-node:/tmp/hive.db.backup ./

# Run migration manually (if needed)
docker-compose exec cln python3 -c "
from modules.database import Database
db = Database('/data/lightning/bitcoin/cl-hive.db')
db.migrate()
"
```

## Troubleshooting Upgrades

### Upgrade Stuck at Health Check

```bash
# Check container logs
docker-compose logs --tail=100 cln

# Check lightningd logs
docker-compose exec cln cat /data/lightning/bitcoin/lightningd.log | tail -100

# Check if RPC is responding
docker-compose exec cln lightning-cli --network=bitcoin getinfo
```

### Plugin Not Loading After Upgrade

```bash
# Check plugin list
docker-compose exec cln lightning-cli plugin list

# Check for Python errors
docker-compose exec cln python3 -c "import pyln.client; print('OK')"

# Manually start plugin
docker-compose exec cln lightning-cli plugin start /opt/cl-hive/cl-hive.py
```

### Database Corruption After Upgrade

```bash
# Stop node
docker-compose stop

# Check database integrity
docker-compose exec cln sqlite3 /data/lightning/bitcoin/lightningd.sqlite3 "PRAGMA integrity_check"

# If corrupted, restore from backup
./scripts/restore.sh --latest
```

### Channel Issues After Upgrade

```bash
# Check channel states
docker-compose exec cln lightning-cli listpeerchannels

# If channels show incorrect state, try reconnecting
docker-compose exec cln lightning-cli connect <peer_id>

# Check for pending HTLCs
docker-compose exec cln lightning-cli listhtlcs
```

## Post-Upgrade Verification

After upgrading, verify:

```bash
# 1. Node is running
docker-compose exec cln lightning-cli getinfo

# 2. All plugins loaded
docker-compose exec cln lightning-cli plugin list | grep cl-hive

# 3. Hive status
docker-compose exec cln lightning-cli hive-status

# 4. Revenue ops status
docker-compose exec cln lightning-cli revenue-status

# 5. Channel connectivity
docker-compose exec cln lightning-cli listpeerchannels | grep '"connected": true' | wc -l

# 6. Check logs for errors
docker-compose logs --tail=50 cln | grep -i error
```

## Version-Specific Notes

### Upgrading to 0.2.x

- **Breaking Change**: Governance mode `autonomous` renamed to `failsafe`
- **Database**: New tables for advisor system (automatic migration)
- **Config**: New options for Physarum optimization

Update your `.env`:
```bash
# Old
HIVE_GOVERNANCE_MODE=autonomous

# New
HIVE_GOVERNANCE_MODE=failsafe
```

### Upgrading to 0.3.x

- **New Feature**: Phase 2 fee coordination
- **Dependency**: Requires CLN 25.02 or later
- **Config**: New corridor assignment options

## Disaster Recovery

If upgrade fails catastrophically:

1. **Don't panic** - Your funds are safe if you have the hsm_secret backup

2. **Stop everything**:
   ```bash
   docker-compose down
   ```

3. **Check what you have**:
   ```bash
   # Backup hsm_secret
   ls /backups/*/hsm/

   # Check database backups
   ls /backups/*/database*
   ```

4. **Fresh install with restore**:
   ```bash
   # Remove existing data (ONLY if you have backup!)
   docker volume rm cl-hive_lightning-data

   # Start fresh
   docker-compose up -d

   # Wait for init
   sleep 30

   # Restore from backup
   ./scripts/restore.sh --latest
   ```

5. **If all else fails**:
   - Keep your hsm_secret safe
   - Contact the community for help
   - Your channel counterparties can force-close if needed
