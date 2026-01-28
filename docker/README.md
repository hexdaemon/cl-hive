# cl-hive Docker Deployment

Production-ready Docker image for cl-hive Lightning nodes with Tor, WireGuard, and full plugin stack.

## Features

- **Core Lightning** v25+ with all plugins
- **Ride The Lightning** web interface for node management
- **Tor** hidden service for privacy
- **WireGuard** VPN support (optional)
- **cl-revenue-ops** for fee optimization
- **cl-hive** for fleet coordination

### Required Plugins (Pre-installed)
- **CLBOSS** - Automated channel management (ksedgwic fork with clboss-unmanage)
- **Sling** - Rebalancing engine (required by cl-revenue-ops)
- **c-lightning-REST** - REST API for RTL web interface
- **cl-revenue-ops** - Fee optimization and profitability tracking
- **cl-hive** - Fleet coordination and swarm intelligence

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

## Quick Start

### Option 1: Pre-built Image (Recommended)

No compilation required. Uses pre-built images from GitHub Container Registry.

```bash
cd docker

# Copy and edit environment file
cp .env.example .env
nano .env  # Configure Bitcoin RPC, alias, etc.

# Pull and start (no build needed!)
docker-compose pull
docker-compose up -d

# Monitor startup
docker-compose logs -f
```

**Upgrade with pre-built images:**
```bash
docker-compose pull
docker-compose up -d
```

### Option 2: Build from Source

For developers or when you need custom modifications.

```bash
cd docker

# Copy environment and build config
cp .env.example .env
cp docker-compose.build.yml docker-compose.override.yml
nano .env

# Clone cl-revenue-ops (required for build)
git clone https://github.com/lightning-goats/cl_revenue_ops.git ../../cl_revenue_ops

# Build and start
docker-compose up -d --build
```

**Hot upgrades (no rebuild):**
```bash
./scripts/hot-upgrade.sh
```

### Option 3: Interactive Setup Wizard

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

## Production Deployment

### 1. Pre-Deployment Checklist

- [ ] Bitcoin Core is synced and RPC accessible
- [ ] Adequate disk space (10GB+ recommended)
- [ ] Adequate memory (4GB+ recommended)
- [ ] Firewall configured (port 9736 for Lightning, 3000 for RTL)
- [ ] RTL password changed from default
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
| `RGB` | `e33502` | Node color (hex) |

#### Network Mode & Connectivity

| Variable | Default | Description |
|----------|---------|-------------|
| `LIGHTNING_PORT` | `9736` | Lightning P2P port |
| `NETWORK_MODE` | `tor` | Network mode: `tor`, `clearnet`, or `hybrid` |
| `ANNOUNCE_ADDR` | - | Public address (required for clearnet/hybrid) |
| `WIREGUARD_ENABLED` | `false` | Enable WireGuard VPN |

**Network Modes:**
- **tor** - Tor-only, anonymous, no clearnet exposure (default)
- **clearnet** - Direct connections only, requires `ANNOUNCE_ADDR`
- **hybrid** - Both Tor hidden service and clearnet

#### Ride The Lightning (RTL)

| Variable | Default | Description |
|----------|---------|-------------|
| `RTL_ENABLED` | `true` | Enable RTL web interface |
| `RTL_PASSWORD` | `changeme` | RTL web interface password (**change this!**) |
| `RTL_PORT` | `3000` | RTL web interface port |
| `COMPOSE_PROFILES` | `rtl` | Set to `rtl` to start RTL (auto-set when RTL_ENABLED=true) |

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

### Ride The Lightning Web Interface

RTL provides a web-based UI for managing your Lightning node.

```bash
# RTL is enabled by default when using setup.sh
# Or enable manually by setting in .env:
RTL_ENABLED=true
COMPOSE_PROFILES=rtl

# Start with RTL profile (if not set in .env)
docker-compose --profile rtl up -d

# Access RTL at http://localhost:3000 (or your configured RTL_PORT)
# Default password is set in RTL_PASSWORD (change from 'changeme'!)

# View RTL logs
docker-compose logs -f rtl

# Restart RTL
docker-compose restart rtl

# Disable RTL by removing COMPOSE_PROFILES from .env or:
docker-compose stop rtl
```

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

There are three upgrade methods depending on your deployment:

#### Method 1: Pre-built Image Upgrade (Recommended)

For deployments using pre-built images from GHCR. Fastest and safest.

```bash
# Backup first (always!)
./scripts/backup.sh

# Pull new image and restart
docker-compose pull
docker-compose up -d

# Verify
docker-compose exec cln lightning-cli getinfo
docker-compose exec cln lightning-cli hive-status
```

**Upgrade to specific version:**
```bash
# Edit .env to set version
CL_HIVE_VERSION=2.3.0

# Or override directly
CL_HIVE_VERSION=2.3.0 docker-compose pull
docker-compose up -d
```

#### Method 2: Hot Upgrade (For build-from-source deployments)

Updates cl-hive and cl-revenue-ops without rebuilding the Docker image. Only works if plugins are mounted from host.

```bash
# Check for available updates
./scripts/hot-upgrade.sh --check

# Upgrade all plugins (pulls git changes and restarts lightningd)
./scripts/hot-upgrade.sh

# Upgrade only cl-hive
./scripts/hot-upgrade.sh hive

# Upgrade only cl-revenue-ops
./scripts/hot-upgrade.sh revenue
```

**How it works:**
- cl-hive is mounted from the host, so `git pull` updates it immediately
- Restarts lightningd via supervisorctl to load new plugin code
- No image rebuild required

#### Method 3: Full Rebuild (For Core Lightning or system updates)

Rebuilds the Docker image. Use when upgrading Core Lightning, system packages, or after major changes.

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

### Clearnet or Hybrid Mode

To use clearnet instead of Tor, set in `.env`:
```bash
# Clearnet only (no Tor)
NETWORK_MODE=clearnet
ANNOUNCE_ADDR=your.public.ip:9736

# Or hybrid (both Tor and clearnet)
NETWORK_MODE=hybrid
ANNOUNCE_ADDR=your.public.ip:9736
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

**Common cause: Missing `rpcallowip` in Bitcoin config**

Docker containers use a bridge network (typically 172.17.0.0/16 or 172.20.0.0/16). Your Bitcoin Core must allow connections from this network:

```bash
# Add to your bitcoin.conf
rpcallowip=172.16.0.0/12  # Covers all Docker networks

# Or more restrictive (check your Docker network)
docker network inspect docker_lightning-network | grep Subnet
rpcallowip=172.20.0.0/16  # Use the subnet shown

# Restart Bitcoin Core after changes
```

See [runbooks/bitcoin-rpc-recovery.md](runbooks/bitcoin-rpc-recovery.md) for more details

### bitcoin-cli Not Found / Build Timeout

If the Docker build fails downloading bitcoin-cli from bitcoincore.org (timeout), or lightningd fails with "bitcoin-cli exec failed":

```bash
# Copy bitcoin-cli from your host
cp /usr/local/bin/bitcoin-cli docker/bitcoin-cli

# Uncomment the mount in docker-compose.yml:
# - ./bitcoin-cli:/usr/local/bin/bitcoin-cli:ro

# Restart
docker-compose up -d
```

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
   - `NET_ADMIN` and `NET_RAW` capabilities for Tor/WireGuard
   - Resource limits prevent DoS
   - Processes run as root inside container (required for network config)

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
│   ├── upgrade.sh              # Full image upgrades
│   ├── hot-upgrade.sh          # Quick plugin updates (no rebuild)
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

## Docker Images

### Pre-built Images (Recommended)

Pre-built multi-architecture images are available on GitHub Container Registry:

```bash
# Pull latest stable release
docker pull ghcr.io/lightning-goats/cl-hive-node:latest

# Pull specific version
docker pull ghcr.io/lightning-goats/cl-hive-node:2.2.1

# Available architectures:
# - linux/amd64 (x86_64)
# - linux/arm64 (Apple Silicon, Raspberry Pi 4+)
```

**Advantages of pre-built images:**
- No compilation required (faster startup)
- Tested and verified before release
- Consistent across all deployments
- Easy upgrades: `docker-compose pull && docker-compose up -d`

### Building from Source

For developers or custom modifications:

```bash
# Prerequisites: Clone cl-revenue-ops next to cl-hive
git clone https://github.com/lightning-goats/cl_revenue_ops.git ../cl_revenue_ops

# Use the build override
cp docker-compose.build.yml docker-compose.override.yml

# Build
docker-compose build

# Or build directly
docker build -t cl-hive-node:local -f docker/Dockerfile ..
```

### Image Contents

| Component | Version | Required |
|-----------|---------|----------|
| Ubuntu | 24.04 | Yes |
| Core Lightning | v25.12.1 | Yes |
| CLBOSS | latest (ksedgwic fork) | Yes |
| Sling | v4.1.3 | Yes |
| c-lightning-REST | v0.10.7 | Yes |
| cl-revenue-ops | latest (from GitHub) | Yes |
| cl-hive | bundled | Yes |
| Tor | 0.4.8.x | Yes |
| WireGuard | 1.0.x | Optional |
| Python | 3.12 | Yes |
| Ride The Lightning | 0.15.2 | Yes |

## Support

- **Documentation**: See the `runbooks/` directory for operational procedures
- **Issues**: Report bugs at https://github.com/lightning-goats/cl-hive/issues
- **Upgrades**: See [UPGRADE.md](UPGRADE.md) for version-specific notes
