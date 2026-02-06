# cl-hive Production Node
# Core Lightning + Tor + WireGuard + Full Plugin Stack
#
# Build: docker build -t cl-hive-node .
# Run:   docker-compose up -d

FROM ubuntu:24.04

LABEL maintainer="Lightning Goats Team"
LABEL version="2.2.6"
LABEL description="Production Lightning node with cl-hive coordination"

# Prevent interactive prompts during install
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# =============================================================================
# SYSTEM PACKAGES
# =============================================================================

RUN apt-get update && apt-get install -y \
    # Build essentials
    build-essential \
    autoconf \
    automake \
    libtool \
    pkg-config \
    git \
    curl \
    wget \
    # Core Lightning dependencies
    libgmp-dev \
    libsqlite3-dev \
    libpq5 \
    python3 \
    python3-pip \
    python3-venv \
    net-tools \
    zlib1g-dev \
    libsodium-dev \
    gettext \
    # Tor
    tor \
    # WireGuard
    wireguard \
    wireguard-tools \
    # Networking utilities
    iproute2 \
    iptables \
    dnsutils \
    # Process management
    supervisor \
    # JSON processing
    jq \
    # File watching for backup
    inotify-tools \
    # Node.js for c-lightning-REST
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# =============================================================================
# BITCOIN CLI (required for CLN's bcli plugin)
# =============================================================================
# NOTE: bitcoin-cli is mounted from host via docker-compose.yml
# The download step is skipped to avoid network issues with bitcoincore.org
# If you need bitcoin-cli baked into the image, uncomment below:
#
# ARG BITCOIN_VERSION=28.1
# RUN ARCH=$(uname -m) \
#     && if [ "$ARCH" = "x86_64" ]; then ARCH="x86_64-linux-gnu"; fi \
#     && if [ "$ARCH" = "aarch64" ]; then ARCH="aarch64-linux-gnu"; fi \
#     && curl -SLO "https://bitcoincore.org/bin/bitcoin-core-${BITCOIN_VERSION}/bitcoin-${BITCOIN_VERSION}-${ARCH}.tar.gz" \
#     && tar -xzf bitcoin-${BITCOIN_VERSION}-${ARCH}.tar.gz \
#     && install -m 0755 bitcoin-${BITCOIN_VERSION}/bin/bitcoin-cli /usr/local/bin/ \
#     && rm -rf bitcoin-${BITCOIN_VERSION} bitcoin-${BITCOIN_VERSION}-${ARCH}.tar.gz

# =============================================================================
# CORE LIGHTNING
# =============================================================================
# AMD64: Use pre-built binary from GitHub releases (fast)
# ARM64: Build from source (CLN doesn't publish ARM64 Ubuntu 24.04 binaries)

ARG CLN_VERSION=v25.12.1

# Install build dependencies for ARM64 source compilation
RUN apt-get update && apt-get install -y \
    libpq-dev \
    valgrind \
    cppcheck \
    shellcheck \
    libsecp256k1-dev \
    lowdown \
    && rm -rf /var/lib/apt/lists/*

# Install CLN: pre-built on AMD64, source build on ARM64
RUN ARCH=$(uname -m) \
    && if [ "$ARCH" = "x86_64" ]; then \
        echo "Installing pre-built CLN for AMD64..." \
        && curl -SLO "https://github.com/ElementsProject/lightning/releases/download/${CLN_VERSION}/clightning-${CLN_VERSION}-Ubuntu-24.04-amd64.tar.xz" \
        && tar -xf clightning-${CLN_VERSION}-Ubuntu-24.04-amd64.tar.xz -C /usr/local --strip-components=2 \
        && rm clightning-${CLN_VERSION}-Ubuntu-24.04-amd64.tar.xz; \
    elif [ "$ARCH" = "aarch64" ]; then \
        echo "Building CLN from source for ARM64..." \
        && pip install --no-cache-dir mako grpcio-tools \
        && git clone --depth 1 --branch ${CLN_VERSION} https://github.com/ElementsProject/lightning.git /tmp/lightning \
        && cd /tmp/lightning \
        && pip install --no-cache-dir -r requirements.txt \
        && ./configure --prefix=/usr/local \
        && make -j$(nproc) \
        && make install \
        && cd / \
        && rm -rf /tmp/lightning; \
    fi \
    && ldconfig

# =============================================================================
# PYTHON ENVIRONMENT
# =============================================================================

# Create virtual environment for plugins
RUN python3 -m venv /opt/cln-plugins-venv
ENV PATH="/opt/cln-plugins-venv/bin:$PATH"

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyln-client>=24.0 \
    requests \
    anthropic

# =============================================================================
# CLBOSS (REQUIRED - ksedgwic fork with clboss-unmanage support)
# =============================================================================
# CLBOSS provides automated channel management and liquidity optimization.
# Required for cl-hive fleet coordination.

WORKDIR /tmp

RUN apt-get update && apt-get install -y \
    libev-dev \
    libcurl4-openssl-dev \
    autoconf-archive \
    libunwind-dev \
    && rm -rf /var/lib/apt/lists/*

# Build CLBOSS from ksedgwic fork (has clboss-unmanage support)
RUN git clone --depth 1 https://github.com/ksedgwic/clboss.git \
    && cd clboss \
    && mkdir -p m4 \
    && autoreconf -fi \
    && ./configure \
    && make -j$(nproc) \
    && make install \
    && cd .. \
    && rm -rf clboss

# =============================================================================
# TRUSTEDCOIN PLUGIN (OPTIONAL)
# =============================================================================
# Trustedcoin replaces bcli for Bitcoin backend, using block explorers.
# Useful for VPS deployments without local bitcoind.
# - Explorer-only mode: No bitcoind required, uses public explorers
# - Hybrid mode: bitcoind primary with explorer fallback

ARG TRUSTEDCOIN_VERSION=v0.8.6
RUN ARCH=$(uname -m) \
    && if [ "$ARCH" = "x86_64" ]; then ARCH="linux-amd64"; fi \
    && if [ "$ARCH" = "aarch64" ]; then ARCH="linux-arm64"; fi \
    && wget -O /tmp/trustedcoin.tar.gz "https://github.com/nbd-wtf/trustedcoin/releases/download/${TRUSTEDCOIN_VERSION}/trustedcoin-${TRUSTEDCOIN_VERSION}-${ARCH}.tar.gz" \
    && tar -xzf /tmp/trustedcoin.tar.gz -C /tmp \
    && mv /tmp/trustedcoin /usr/local/bin/trustedcoin \
    && chmod +x /usr/local/bin/trustedcoin \
    && rm /tmp/trustedcoin.tar.gz

# =============================================================================
# SLING PLUGIN (REQUIRED)
# =============================================================================
# Sling provides rebalancing capabilities required by cl-revenue-ops.

ARG SLING_VERSION=v4.1.3
RUN ARCH=$(uname -m) \
    && if [ "$ARCH" = "x86_64" ]; then TRIPLE="x86_64-linux-gnu"; fi \
    && if [ "$ARCH" = "aarch64" ]; then TRIPLE="aarch64-linux-gnu"; fi \
    && wget -O /tmp/sling.tar.gz "https://github.com/daywalker90/sling/releases/download/${SLING_VERSION}/sling-${SLING_VERSION}-${TRIPLE}.tar.gz" \
    && tar -xzf /tmp/sling.tar.gz -C /tmp \
    && mv /tmp/sling /usr/local/bin/sling \
    && chmod +x /usr/local/bin/sling \
    && rm /tmp/sling.tar.gz

# =============================================================================
# C-LIGHTNING-REST (for RTL integration)
# =============================================================================
# REST API plugin enabling Ride The Lightning web interface

ARG CLN_REST_VERSION=v0.10.7
RUN git clone --depth 1 --branch ${CLN_REST_VERSION} https://github.com/Ride-The-Lightning/c-lightning-REST.git /opt/c-lightning-REST \
    && cd /opt/c-lightning-REST \
    && npm install --omit=dev \
    && chmod +x cl-rest.js

# =============================================================================
# CL-REVENUE-OPS PLUGIN
# =============================================================================
# Fallback: Clone from GitHub if not mounted from host.
# For hot upgrades, mount from host in docker-compose.yml:
#   - /path/to/cl_revenue_ops:/opt/cl-revenue-ops:ro

ARG CL_REVENUE_OPS_VERSION=v2.2.5
RUN git clone --depth 1 --branch ${CL_REVENUE_OPS_VERSION} https://github.com/lightning-goats/cl_revenue_ops.git /opt/cl-revenue-ops \
    && chmod +x /opt/cl-revenue-ops/cl-revenue-ops.py

# =============================================================================
# CL-HIVE PLUGIN
# =============================================================================
# Fallback: Copy files if not mounted from host.
# For hot upgrades, mount from host in docker-compose.yml:
#   - /path/to/cl-hive:/opt/cl-hive:ro
# When mounted, the host's git repo enables ./scripts/hot-upgrade.sh

COPY . /opt/cl-hive
RUN chmod +x /opt/cl-hive/cl-hive.py

# =============================================================================
# TOR CONFIGURATION
# =============================================================================

RUN mkdir -p /var/lib/tor/cln-service \
    && chown -R debian-tor:debian-tor /var/lib/tor

COPY docker/torrc /etc/tor/torrc

# =============================================================================
# DIRECTORY STRUCTURE
# =============================================================================

RUN mkdir -p /root/.lightning/bitcoin \
    && mkdir -p /root/.lightning/plugins \
    && mkdir -p /data/lightning \
    && mkdir -p /data/bitcoin

# Symlink plugins to CLN plugins directory
RUN ln -sf /opt/cl-hive/cl-hive.py /root/.lightning/plugins/cl-hive.py \
    && ln -sf /opt/cl-hive/modules /root/.lightning/plugins/modules \
    && ln -sf /opt/cl-revenue-ops/cl-revenue-ops.py /root/.lightning/plugins/cl-revenue-ops.py \
    && ln -sf /opt/cl-revenue-ops/modules /root/.lightning/plugins/revenue-modules \
    && ln -sf /usr/local/bin/clboss /root/.lightning/plugins/clboss \
    && ln -sf /usr/local/bin/sling /root/.lightning/plugins/sling \
    && ln -sf /opt/c-lightning-REST/cl-rest.js /root/.lightning/plugins/cl-rest.js

# =============================================================================
# CONFIGURATION FILES
# =============================================================================

COPY docker/docker-entrypoint.sh /usr/local/bin/
COPY docker/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY docker/lightning.conf.template /etc/lightning/lightning.conf.template
COPY cl-hive.conf.sample /etc/lightning/cl-hive.conf

# Copy shutdown and utility scripts
COPY docker/scripts/pre-stop.sh /usr/local/bin/
COPY docker/scripts/lightningd-wrapper.sh /usr/local/bin/
COPY docker/scripts/emergency-recover-watcher.sh /usr/local/bin/
COPY docker/scripts/plugin-db-backup.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    /usr/local/bin/pre-stop.sh \
    /usr/local/bin/lightningd-wrapper.sh \
    /usr/local/bin/emergency-recover-watcher.sh \
    /usr/local/bin/plugin-db-backup.sh

# =============================================================================
# PORTS
# =============================================================================

# Lightning P2P
EXPOSE 9736
# c-lightning-REST API (for RTL)
EXPOSE 3001
# Tor SOCKS (internal)
EXPOSE 9050
# WireGuard
EXPOSE 51820/udp

# =============================================================================
# VOLUMES
# =============================================================================

VOLUME ["/data/lightning", "/data/bitcoin", "/etc/wireguard"]

# =============================================================================
# ENTRYPOINT
# =============================================================================

WORKDIR /root

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
