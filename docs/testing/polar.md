# Polar Testing Guide for cl-revenue-ops and cl-hive

This guide covers installing and testing cl-revenue-ops, cl-hive, and their dependencies (clboss, sling) on a Polar regtest environment.

## Prerequisites

- Polar installed ([lightningpolar.com](https://lightningpolar.com))
- Docker running
- Plugin repositories cloned locally

---

## Network Setup

Create the following 9 nodes in Polar before running the install script:

### Required Nodes

| Node Name | Implementation | Version | Purpose | Plugins |
|-----------|---------------|---------|---------|---------|
| alice | Core Lightning | v25.12 | Hive Admin | clboss, cl-revenue-ops, cl-hive |
| bob | Core Lightning | v25.12 | Hive Member | clboss, cl-revenue-ops, cl-hive |
| carol | Core Lightning | v25.12 | Hive Member | clboss, cl-revenue-ops, cl-hive |
| dave | Core Lightning | v25.12 | External CLN | none (vanilla) |
| erin | Core Lightning | v25.12 | External CLN | none (vanilla) |
| lnd1 | LND | latest | External LND | none |
| lnd2 | LND | latest | External LND | none |
| eclair1 | Eclair | latest | External Eclair | none |
| eclair2 | Eclair | latest | External Eclair | none |

### Channel Topology

Create channels in Polar to match this topology:

```
                    HIVE FLEET                          EXTERNAL NODES
┌─────────────────────────────────────────┐    ┌─────────────────────────────┐
│                                         │    │                             │
│   alice ──────── bob ──────── carol     │    │   dave ──────── erin        │
│     │             │             │       │    │     │                       │
└─────┼─────────────┼─────────────┼───────┘    └─────┼───────────────────────┘
      │             │             │                  │
      │             │             │                  │
      ▼             ▼             ▼                  ▼
   ┌──────┐     ┌──────┐     ┌──────┐          ┌──────────┐
   │ lnd1 │     │ lnd2 │     │ dave │          │ eclair1  │
   └──┬───┘     └──┬───┘     └──────┘          └────┬─────┘
      │            │                                │
      ▼            ▼                                ▼
   ┌──────────┐ ┌──────────┐                  ┌──────────┐
   │ eclair1  │ │ eclair2  │                  │ eclair2  │
   └──────────┘ └──────────┘                  └──────────┘
```

**Channel Purposes:**
- alice↔bob↔carol: Internal hive communication and state sync
- alice→lnd1, bob→lnd2, carol→dave: Hive to external channels (tests intent protocol)
- lnd1→eclair1, lnd2→eclair2: Cross-implementation routing paths
- dave→erin→eclair1→eclair2: External routing network

---

## Architecture

```
HIVE FLEET (with plugins)              EXTERNAL NODES (no hive plugins)
┌─────────────────────────────┐       ┌─────────────────────────────┐
│  alice (CLN v25.12)         │       │  lnd1 (LND)                 │
│  ├── clboss                 │       │  lnd2 (LND)                 │
│  ├── cl-revenue-ops         │◄─────►│  eclair1 (Eclair)           │
│  └── cl-hive                │       │  eclair2 (Eclair)           │
│                             │       │  dave (CLN - vanilla)       │
│  bob (CLN v25.12)           │       │  erin (CLN - vanilla)       │
│  ├── clboss                 │       └─────────────────────────────┘
│  ├── cl-revenue-ops         │
│  └── cl-hive                │
│                             │
│  carol (CLN v25.12)         │
│  ├── clboss                 │
│  ├── cl-revenue-ops         │
│  └── cl-hive                │
└─────────────────────────────┘
```

**Plugin Load Order:** clboss → cl-revenue-ops → cl-hive

---

## Installation

### Option A: Quick Install Script

Use the provided installation script:

```bash
# Find your Polar network ID (usually 1, 2, etc.)
ls ~/.polar/networks/

# Run installer (replace 1 with your network ID)
./install.sh 1
```

**Note:** First run takes 5-10 minutes per node to build clboss from source.

### Option B: Manual Installation

#### Step 1: Identify Container Names

```bash
docker ps --filter "ancestor=polarlightning/clightning" --format "{{.Names}}"
```

Typical names: `polar-n1-alice`, `polar-n1-bob`, `polar-n1-carol`

#### Step 2: Install Build Dependencies

```bash
CONTAINER="polar-n1-alice"

docker exec -u root $CONTAINER apt-get update
docker exec -u root $CONTAINER apt-get install -y \
    build-essential autoconf autoconf-archive automake libtool pkg-config \
    libev-dev libcurl4-gnutls-dev libsqlite3-dev \
    python3 python3-pip git
docker exec -u root $CONTAINER pip3 install pyln-client
```

#### Step 3: Build and Install CLBOSS

```bash
docker exec $CONTAINER bash -c "
    cd /tmp &&
    git clone --recurse-submodules https://github.com/ZmnSCPxj/clboss.git &&
    cd clboss &&
    autoreconf -i &&
    ./configure &&
    make -j$(nproc) &&
    cp clboss /home/clightning/.lightning/plugins/
"
```

#### Step 4: Copy Python Plugins

```bash
docker cp /home/sat/cl_revenue_ops $CONTAINER:/home/clightning/.lightning/plugins/
docker cp /home/sat/cl-hive $CONTAINER:/home/clightning/.lightning/plugins/

docker exec -u root $CONTAINER chown -R clightning:clightning /home/clightning/.lightning/plugins
docker exec $CONTAINER chmod +x /home/clightning/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py
docker exec $CONTAINER chmod +x /home/clightning/.lightning/plugins/cl-hive/cl-hive.py
```

#### Step 5: Load Plugins (in order)

```bash
# Polar containers require explicit lightning-cli path
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"
docker exec $CONTAINER $CLI plugin start /home/clightning/.lightning/plugins/clboss
docker exec $CONTAINER $CLI plugin start /home/clightning/.lightning/plugins/cl-revenue-ops/cl-revenue-ops.py
docker exec $CONTAINER $CLI plugin start /home/clightning/.lightning/plugins/cl-hive/cl-hive.py
```

### Option C: Docker Volume Mount (Persistent)

Create `~/.polar/networks/<network-id>/docker-compose.override.yml`:

```yaml
version: '3'
services:
  alice:
    volumes:
      - /home/sat/cl_revenue_ops:/home/clightning/.lightning/plugins/cl-revenue-ops:ro
      - /home/sat/cl-hive:/home/clightning/.lightning/plugins/cl-hive:ro
  bob:
    volumes:
      - /home/sat/cl_revenue_ops:/home/clightning/.lightning/plugins/cl-revenue-ops:ro
      - /home/sat/cl-hive:/home/clightning/.lightning/plugins/cl-hive:ro
  carol:
    volumes:
      - /home/sat/cl_revenue_ops:/home/clightning/.lightning/plugins/cl-revenue-ops:ro
      - /home/sat/cl-hive:/home/clightning/.lightning/plugins/cl-hive:ro
```

**Note:** Volume mounts don't help with clboss - it must be built inside each container.

Restart the network in Polar UI after creating this file.

---

## Configuration

### cl-revenue-ops (Testing Config)

```ini
revenue-ops-flow-interval=300
revenue-ops-fee-interval=120
revenue-ops-rebalance-interval=60
revenue-ops-min-fee-ppm=1
revenue-ops-max-fee-ppm=1000
revenue-ops-daily-budget-sats=10000
revenue-ops-clboss-enabled=true
```

### cl-hive (Testing Config)

```ini
hive-governance-mode=advisor
hive-probation-days=0
hive-min-vouch-count=1
hive-heartbeat-interval=60
```

---

## Testing

### Test 1: Verify Plugin Loading

```bash
# Set up CLI alias for Polar
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

for node in alice bob carol; do
    echo "=== $node ==="
    docker exec polar-n1-$node $CLI plugin list | grep -E "(clboss|revenue|hive)"
done
```

### Test 2: CLBOSS Status

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"
docker exec polar-n1-alice $CLI clboss-status
```

### Test 3: cl-revenue-ops Status

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"
docker exec polar-n1-alice $CLI revenue-status
docker exec polar-n1-alice $CLI revenue-channels
docker exec polar-n1-alice $CLI revenue-dashboard
```

### Test 4: Hive Genesis

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

# Alice creates a Hive
docker exec polar-n1-alice $CLI hive-genesis

# Verify
docker exec polar-n1-alice $CLI hive-status
```

### Test 5: Hive Join

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

# Alice generates invite
TICKET=$(docker exec polar-n1-alice $CLI hive-invite | jq -r '.ticket')

# Bob joins (use named parameter)
docker exec polar-n1-bob $CLI hive-join ticket="$TICKET"

# Verify
docker exec polar-n1-bob $CLI hive-status
docker exec polar-n1-alice $CLI hive-members
```

### Test 6: State Sync

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

ALICE_HASH=$(docker exec polar-n1-alice $CLI hive-status | jq -r '.state_hash')
BOB_HASH=$(docker exec polar-n1-bob $CLI hive-status | jq -r '.state_hash')
echo "Alice: $ALICE_HASH"
echo "Bob: $BOB_HASH"
# Hashes should match
```

### Test 7: Fee Policy Integration

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

BOB_PUBKEY=$(docker exec polar-n1-bob $CLI getinfo | jq -r '.id')
docker exec polar-n1-alice $CLI revenue-policy get $BOB_PUBKEY
# Should show strategy: hive
```

### Test 8: Three-Node Hive

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

TICKET=$(docker exec polar-n1-alice $CLI hive-invite | jq -r '.ticket')
docker exec polar-n1-carol $CLI hive-join ticket="$TICKET"
docker exec polar-n1-alice $CLI hive-members
# Should show 3 members
```

### Test 9: CLBOSS Integration

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

# Verify cl-revenue-ops can unmanage peers from clboss
BOB_PUBKEY=$(docker exec polar-n1-bob $CLI getinfo | jq -r '.id')
docker exec polar-n1-alice $CLI clboss-unmanage $BOB_PUBKEY
docker exec polar-n1-alice $CLI clboss-unmanaged
# Should show Bob as unmanaged
```

---

## Troubleshooting

### Plugin Fails to Load

```bash
# Check Python dependencies
docker exec polar-n1-alice pip3 list | grep pyln

# Check plugin permissions
docker exec polar-n1-alice ls -la /home/clightning/.lightning/plugins/

# Check clboss binary exists
docker exec polar-n1-alice ls -la /home/clightning/.lightning/plugins/clboss
```

### CLBOSS Build Fails

```bash
# Check build dependencies
docker exec polar-n1-alice dpkg -l | grep -E "(autoconf|libev|libcurl)"

# Try rebuilding
docker exec polar-n1-alice bash -c "cd /tmp/clboss && make clean && make -j$(nproc)"
```

### View Plugin Logs

```bash
docker exec polar-n1-alice tail -100 /home/clightning/.lightning/debug.log | grep -E "(clboss|revenue|hive)"
```

### Permission Issues

```bash
docker exec -u root polar-n1-alice chown -R clightning:clightning /home/clightning/.lightning/plugins
```

---

## Cleanup

### Stop Plugins

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

for node in alice bob carol; do
    docker exec polar-n1-$node $CLI plugin stop cl-hive || true
    docker exec polar-n1-$node $CLI plugin stop cl-revenue-ops || true
    docker exec polar-n1-$node $CLI plugin stop clboss || true
done
```

### Reset Databases

```bash
for node in alice bob carol; do
    docker exec polar-n1-$node rm -f /home/clightning/.lightning/regtest/revenue_ops.db
    docker exec polar-n1-$node rm -f /home/clightning/.lightning/regtest/cl_hive.db
    docker exec polar-n1-$node rm -f /home/clightning/.lightning/regtest/clboss.sqlite3
done
```

---

## Automated Testing

Use the `test.sh` script for comprehensive automated testing:

```bash
# Run all tests
./test.sh all 1

# Run specific test category
./test.sh genesis 1
./test.sh join 1
./test.sh sync 1
./test.sh channels 1
./test.sh fees 1
./test.sh clboss 1
./test.sh contrib 1
./test.sh cross 1

# Reset and run fresh
./test.sh reset 1
./test.sh all 1
```

### Test Categories

| Category | Description |
|----------|-------------|
| setup | Verify containers and plugin loading |
| genesis | Hive creation and admin ticket |
| join | Member invitation and join workflow |
| sync | State synchronization between members |
| channels | Channel opening with intent protocol |
| fees | Fee policy and HIVE strategy |
| clboss | CLBOSS integration (unmanage/ignore) |
| contrib | Contribution tracking and ratios |
| cross | Cross-implementation (LND/Eclair) tests |

---

## Cross-Implementation CLI Reference

### LND Nodes

```bash
# Get node info
docker exec polar-n1-lnd1 lncli --network=regtest getinfo

# Get pubkey
docker exec polar-n1-lnd1 lncli --network=regtest getinfo | jq -r '.identity_pubkey'

# List channels
docker exec polar-n1-lnd1 lncli --network=regtest listchannels

# Create invoice
docker exec polar-n1-lnd1 lncli --network=regtest addinvoice --amt=1000
```

### Eclair Nodes

```bash
# Get node info
docker exec polar-n1-eclair1 eclair-cli getinfo

# Get pubkey
docker exec polar-n1-eclair1 eclair-cli getinfo | jq -r '.nodeId'

# List channels
docker exec polar-n1-eclair1 eclair-cli channels

# Create invoice
docker exec polar-n1-eclair1 eclair-cli createinvoice --amountMsat=1000000 --description="test"
```

### Vanilla CLN Nodes (dave, erin)

```bash
CLI="lightning-cli --lightning-dir=/home/clightning/.lightning --network=regtest"

# Get node info
docker exec polar-n1-dave $CLI getinfo

# List channels
docker exec polar-n1-dave $CLI listpeerchannels

# Create invoice
docker exec polar-n1-dave $CLI invoice 1000sat "test" "test invoice"
```
