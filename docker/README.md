# cl-hive Docker Deployment

Production-ready Docker image for cl-hive Lightning nodes with Tor, WireGuard, and full plugin stack.

## Features

- **Core Lightning** v25+ with all plugins
- **Tor** hidden service for privacy
- **WireGuard** VPN support (optional)
- **cl-revenue-ops** for fee optimization
- **cl-hive** for fleet coordination

### Production Features

- Interactive setup wizard
- Docker secrets management
- Resource limits and reservations
- Security hardening (no-new-privileges, cap_drop)
- Graceful shutdown with HTLC draining
- Automated encrypted backups
- Upgrade/rollback with health checks
- Operational runbooks
- Structured logging

### Optional Integrations
- **CLBOSS** for automated channel management (not required - hive uses native expansion)
- **sling** for rebalancing (not required - handled by cl-revenue-ops)

## Quick Start

### Production Setup (Recommended)

```bash
cd docker

# Run the interactive setup wizard
./setup.sh

# This will:
# - Configure Bitcoin RPC connection
# - Set up network and node identity
# - Configure Tor and optional WireGuard
# - Set resource limits
# - Create secrets directory
# - Generate .env and docker-compose.override.yml

# Validate configuration
./scripts/validate-config.sh

# Start the node
docker-compose up -d

# Monitor startup
docker-compose logs -f
```

### Manual Setup

```bash
cd docker

# Copy and edit environment file
cp .env.example .env
nano .env

# Start
docker-compose up -d
```

## Production Deployment

### 1. Pre-Deployment Checklist

- [ ] Bitcoin Core is synced and RPC accessible
- [ ] Adequate disk space (10GB+ recommended)
- [ ] Adequate memory (4GB+ recommended)
- [ ] Firewall configured (port 9735 for Lightning)
- [ ] Backup strategy planned

### 2. Using Docker Secrets (Recommended)

For production, use Docker secrets instead of environment variables:

```bash
# Create secrets directory
mkdir -p secrets
chmod 700 secrets

# Create secret files
echo "your_bitcoin_rpc_password" > secrets/bitcoin_rpc_password
chmod 600 secrets/bitcoin_rpc_password

# Use production compose file
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### 3. Resource Limits

Configure in `.env` or `docker-compose.override.yml`:

```yaml
# .env
CPU_LIMIT=4
CPU_RESERVATION=2
MEMORY_LIMIT=8G
MEMORY_RESERVATION=4G
```

### 4. Backup Configuration

```bash
# Set backup location and encryption
BACKUP_LOCATION=/backups
BACKUP_ENCRYPTION=true
BACKUP_RETENTION=30  # days

# Run first backup
./scripts/backup.sh

# Verify backup
./scripts/backup.sh --verify
```

## Configuration Reference

### Environment Variables

#### Core Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `BITCOIN_RPCHOST` | `host.docker.internal` | Bitcoin RPC host |
| `BITCOIN_RPCPORT` | `8332` | Bitcoin RPC port |
| `BITCOIN_RPCUSER` | - | Bitcoin RPC username (required) |
| `BITCOIN_RPCPASSWORD` | - | Bitcoin RPC password (or use secret) |
| `NETWORK` | `bitcoin` | Network: bitcoin, testnet, signet, regtest |
| `ALIAS` | `cl-hive-node` | Node alias |
| `RGB` | `FF9900` | Node color (hex) |
| `ANNOUNCE_ADDR` | - | Public address to announce |

#### Privacy & Networking

| Variable | Default | Description |
|----------|---------|-------------|
| `TOR_ENABLED` | `true` | Enable Tor hidden service |
| `WIREGUARD_ENABLED` | `false` | Enable WireGuard VPN |

#### Resource Limits

| Variable | Default | Description |
|----------|---------|-------------|
| `CPU_LIMIT` | `4` | Maximum CPU cores |
| `CPU_RESERVATION` | `2` | Reserved CPU cores |
| `MEMORY_LIMIT` | `8G` | Maximum memory |
| `MEMORY_RESERVATION` | `4G` | Reserved memory |

#### WireGuard Settings

| Variable | Description |
|----------|-------------|
| `WG_PRIVATE_KEY` | Your WireGuard private key |
| `WG_ADDRESS` | Your VPN IP address (e.g., `10.8.0.2/24`) |
| `WG_PEER_PUBLIC_KEY` | VPN server's public key |
| `WG_PEER_ENDPOINT` | VPN server endpoint (host:port) |
| `WG_DNS` | DNS server on VPN (optional) |

#### Backup Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKUP_LOCATION` | `/backups` | Backup destination |
| `BACKUP_RETENTION` | `30` | Days to keep backups |
| `BACKUP_ENCRYPTION` | `true` | Enable GPG encryption |
| `GPG_KEY_ID` | auto | GPG key for encryption |

### Volumes

| Path | Description |
|------|-------------|
| `/data/lightning` | Lightning node data (persistent) |
| `/backups` | Backup storage |
| `/etc/wireguard` | WireGuard configuration |
| `/etc/lightning/custom` | Custom configuration files |

## Operations

### Check Node Status

```bash
# View logs
docker-compose logs -f

# Check node info
docker-compose exec cln lightning-cli getinfo

# Check hive status
docker-compose exec cln lightning-cli hive-status

# Check revenue operations
docker-compose exec cln lightning-cli revenue-status
```

### Backup and Restore

```bash
# Create backup
./scripts/backup.sh

# Backup hsm_secret only (fastest, most critical)
./scripts/backup.sh --hsm-only

# Verify backup
./scripts/backup.sh --verify

# List available backups
./scripts/restore.sh --list

# Restore from backup
./scripts/restore.sh /backups/backup_20240101_120000
```

### Upgrade

```bash
# Preview upgrade
./scripts/upgrade.sh --dry-run

# Perform upgrade (with automatic backup and rollback)
./scripts/upgrade.sh

# Upgrade to specific version
./scripts/upgrade.sh --version v1.2.0

# Manual rollback if needed
./scripts/rollback.sh --latest
```

See [UPGRADE.md](UPGRADE.md) for detailed upgrade procedures.

### Graceful Shutdown

```bash
# Recommended: Use pre-stop script
docker-compose exec cln /usr/local/bin/pre-stop.sh

# Then stop
docker-compose stop
```

## Tor Configuration

Tor is enabled by default. The hidden service address is created on first start.

### Get Tor Address

```bash
docker-compose exec cln cat /var/lib/tor/cln-service/hostname
```

### Disable Tor

Set in `.env`:
```bash
TOR_ENABLED=false
ANNOUNCE_ADDR=your.public.ip:9735
```

## WireGuard Configuration

WireGuard VPN allows secure connection to your bitcoind backend.

### Setup

1. Get VPN credentials from your administrator
2. Run `./setup.sh` and follow WireGuard prompts
3. Or configure manually in `.env`:

```bash
WIREGUARD_ENABLED=true
WG_PRIVATE_KEY=your_generated_private_key
WG_ADDRESS=10.8.0.2/24
WG_PEER_PUBLIC_KEY=server_public_key_here
WG_PEER_ENDPOINT=vpn.example.com:51820
BITCOIN_RPCHOST=10.8.0.1  # Bitcoin via VPN
```

## Hive Operations

### Initialize Genesis (First Node)

```bash
docker-compose exec cln lightning-cli hive-genesis "my-hive-name"
```

### Generate Invite

```bash
docker-compose exec cln lightning-cli hive-invite 24  # 24 hour validity
```

### Join Existing Hive

```bash
docker-compose exec cln lightning-cli hive-join "HIVE1-INVITE-..."
```

### Check Members

```bash
docker-compose exec cln lightning-cli hive-members
```

## Monitoring

### Log Aggregation

For production monitoring, configure Fluent Bit to ship logs to your preferred destination:

```bash
# Configure logging
cp logging/fluent-bit.conf.example logging/fluent-bit.conf
# Edit for your Elasticsearch/Loki/etc

# Run Fluent Bit
docker run -d --name fluent-bit \
  -v ./logging:/fluent-bit/etc:ro \
  -v /var/run/docker.sock:/var/run/docker.sock:ro \
  fluent/fluent-bit:latest
```

### Health Checks

```bash
# Quick health check
docker-compose exec cln lightning-cli getinfo && \
docker-compose exec cln lightning-cli hive-status && \
docker-compose exec cln lightning-cli revenue-status

# Validate configuration
./scripts/validate-config.sh
```

## Troubleshooting

### Bitcoin RPC Connection Failed

See [runbooks/bitcoin-rpc-recovery.md](runbooks/bitcoin-rpc-recovery.md)

### Tor Hidden Service Not Created

See [runbooks/tor-recovery.md](runbooks/tor-recovery.md)

### Bridge Disabled

See [runbooks/bridge-circuit-breaker.md](runbooks/bridge-circuit-breaker.md)

### Database Issues

See [runbooks/database-corruption.md](runbooks/database-corruption.md)

### Emergency Shutdown

See [runbooks/emergency-shutdown.md](runbooks/emergency-shutdown.md)

## Security Considerations

1. **Secrets Management**
   - Use `secrets/` directory for sensitive values
   - Never commit secrets to version control
   - Secrets directory has 700 permissions

2. **Backup Security**
   - Enable GPG encryption for backups
   - Store hsm_secret backup separately and securely
   - Test restore procedures regularly

3. **Network Security**
   - Use Tor for privacy
   - Use WireGuard for secure Bitcoin RPC connection
   - Firewall: only expose necessary ports

4. **Container Security**
   - `no-new-privileges` enabled
   - Minimal capabilities (cap_drop: ALL)
   - Resource limits prevent DoS

5. **Updates**
   - Keep image updated for security fixes
   - Use `./scripts/upgrade.sh` for safe upgrades
   - Monitor security advisories

## File Structure

```
docker/
├── docker-compose.yml          # Base compose configuration
├── docker-compose.prod.yml     # Production overlay with secrets
├── docker-compose.override.yml # Generated by setup.sh
├── Dockerfile                  # Image build
├── docker-entrypoint.sh        # Container entrypoint
├── supervisord.conf            # Process management
├── .env.example                # Environment template
├── .env                        # Your configuration
├── setup.sh                    # Interactive setup wizard
├── UPGRADE.md                  # Upgrade procedures
├── secrets/                    # Docker secrets (gitignored)
│   └── .gitkeep
├── scripts/
│   ├── backup.sh               # Automated backups
│   ├── restore.sh              # Restore from backup
│   ├── upgrade.sh              # Safe upgrades
│   ├── rollback.sh             # Rollback to backup
│   ├── pre-stop.sh             # Graceful shutdown
│   └── validate-config.sh      # Configuration validation
├── logging/
│   ├── fluent-bit.conf         # Log shipper config
│   └── parsers.conf            # Log parsers
└── runbooks/
    ├── emergency-shutdown.md
    ├── bitcoin-rpc-recovery.md
    ├── tor-recovery.md
    ├── channel-force-close.md
    ├── bridge-circuit-breaker.md
    └── database-corruption.md
```

## Building the Image

### Prerequisites

The Docker build requires cl-revenue-ops to be placed in the `vendor` directory:

```bash
# From cl-hive root directory
mkdir -p vendor
cp -r /path/to/cl-revenue-ops vendor/cl-revenue-ops
```

### Build

```bash
# From cl-hive root directory
docker build -t cl-hive-node:0.1.0 -f docker/Dockerfile .

# Or via docker-compose
docker-compose build
```

### Image Contents

| Component | Version |
|-----------|---------|
| Ubuntu | 24.04 |
| Core Lightning | v25.02.1 |
| CLBOSS | latest (ksedgwic fork, optional) |
| Sling | v4.1.3 |
| cl-revenue-ops | bundled |
| cl-hive | bundled |
| Tor | 0.4.8.x |
| WireGuard | 1.0.x |
| Python | 3.12 |

## Support

- **Documentation**: See the `runbooks/` directory for operational procedures
- **Issues**: Report bugs at https://github.com/LightningGoats/cl-hive/issues
- **Upgrades**: See [UPGRADE.md](UPGRADE.md) for version-specific notes
