# Channel Force Close Runbook

## Severity: HIGH

Use this runbook when you need to force close a channel or when a peer has force-closed on you.

## When to Force Close

**DO force close if:**
- Peer is offline for extended period (weeks)
- Peer is malicious/unresponsive
- Channel is stuck in bad state
- Security concern with peer

**DO NOT force close if:**
- Minor temporary issues
- Peer just needs a restart
- You can negotiate mutual close

## Diagnosis

### Check Channel State

```bash
# List all channels with their states
docker-compose exec cln lightning-cli listpeerchannels

# Check specific channel
docker-compose exec cln lightning-cli listpeerchannels <peer_id>

# States to look for:
# - CHANNELD_NORMAL: healthy
# - CHANNELD_AWAITING_LOCKIN: funding pending
# - AWAITING_UNILATERAL: force close in progress
# - FUNDING_SPEND_SEEN: close detected
# - ONCHAIN: being resolved on-chain
```

### Check Pending HTLCs

```bash
# Critical: Check for pending HTLCs before force close
docker-compose exec cln lightning-cli listhtlcs

# If HTLCs are pending, consider waiting or they may fail
```

## Force Close Procedure

### Step 1: Attempt Mutual Close First

```bash
# Try mutual close (cooperative, cheaper)
docker-compose exec cln lightning-cli close <channel_id>

# Or with specific timeout
docker-compose exec cln lightning-cli close <channel_id> 300
```

### Step 2: Force Close if Mutual Fails

```bash
# Force close (unilateral)
docker-compose exec cln lightning-cli close <channel_id> 1

# This broadcasts your commitment transaction
```

### Step 3: Monitor the Close

```bash
# Watch channel state
watch -n 60 'docker-compose exec cln lightning-cli listpeerchannels | grep -A 20 "AWAITING\|ONCHAIN"'

# Check on-chain status
docker-compose exec cln lightning-cli listfunds | grep -A 5 "outputs"
```

## Handling Inbound Force Closes

If a peer force-closed on you:

### Step 1: Detect the Close

```bash
# Check for channels in closing states
docker-compose exec cln lightning-cli listpeerchannels | grep -B 5 -A 20 "AWAITING_UNILATERAL\|ONCHAIN"
```

### Step 2: Verify They're Not Cheating

```bash
# Check the logs for penalty transaction
docker-compose logs cln | grep -i "penalty\|breach\|revoked"

# If cheating detected, CLN automatically publishes penalty tx
```

### Step 3: Wait for Resolution

```bash
# Force closes have timelocks (typically 144 blocks / ~1 day)
# Monitor progress
docker-compose exec cln lightning-cli listpeerchannels
```

## Timelock Periods

| Close Type | Your Funds Available |
|-----------|---------------------|
| Mutual close | Immediate (next block) |
| You force close | After to_self_delay (~144 blocks) |
| Peer force closes | Immediate (next block) |
| Penalty (they cheated) | Immediate |

## Cost Considerations

Force closes are expensive:
- **On-chain fees**: You pay the commitment tx fee
- **Stuck HTLCs**: May expire and lose value
- **Opportunity cost**: Funds locked during timelock

Estimate costs:
```bash
# Check current fee rates
docker-compose exec cln lightning-cli feerates perkb

# A force close typically costs 5,000-50,000 sats
```

## Recovery of Funds

### After Your Force Close

```bash
# Funds appear in wallet after timelock
docker-compose exec cln lightning-cli listfunds

# If not appearing after expected time:
docker-compose exec cln lightning-cli withdraw <your_address> all
```

### After Peer Force Close

```bash
# Should appear immediately
docker-compose exec cln lightning-cli listfunds

# Sweep to your wallet
docker-compose exec cln lightning-cli withdraw <your_address> all
```

## Emergency: Peer Broadcasting Old State

If you detect a peer cheating:

```bash
# 1. Check logs for breach detection
docker-compose logs cln | grep -i breach

# 2. CLN should automatically handle this
# The penalty transaction claims ALL their funds

# 3. If not happening, contact support immediately
```

## Hive Considerations

When force closing a hive member channel:

```bash
# Notify hive before force close
docker-compose exec cln lightning-cli hive-notify-close <peer_id>

# This removes them from active routing considerations
```

## Post-Close Checklist

- [ ] Verify funds recovered: `listfunds`
- [ ] Update routing tables: happens automatically
- [ ] Consider reopening if peer issue resolved
- [ ] Update hive if was a member: `hive-status`
- [ ] Document reason for close
- [ ] Monitor for any remaining HTLCs

## Related Runbooks

- [Emergency Shutdown](./emergency-shutdown.md)
- [Database Corruption](./database-corruption.md)
