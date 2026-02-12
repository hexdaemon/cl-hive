# Archon Integration for Hive Governance Messaging

## Overview

Optional Archon DID integration for cl-hive enables cryptographically signed, verifiable governance messaging between hive members. Messages are delivered via Archon dmail (encrypted DID-to-DID communication).

## Configuration

### Node Configuration

Add to `config.json` or via `hive-config`:

```json
{
  "archon": {
    "enabled": false,
    "our_did": "did:cid:bagaaiera...",
    "gatekeeper_url": "https://archon.technology",
    "passphrase_env": "ARCHON_PASSPHRASE",
    "auto_notify": ["health_critical", "ban_proposal", "settlement_complete"],
    "message_retention_days": 90
  }
}
```

### Member Contact Registry

Each member can register their Archon DID for receiving governance messages:

```bash
lightning-cli hive-register-contact \
  peer_id="03796a3c5b18080d..." \
  alias="cypher" \
  archon_did="did:cid:bagaaiera..." \
  notify_preferences='["health", "governance", "settlement"]'
```

---

## Governance Message Categories

### 1. Membership Lifecycle

#### 1.1 New Member Joined
**Trigger:** `handle_join_complete()` / new member added to hive
**Recipients:** All existing members
**Template:**
```
Subject: [HIVE] New Member Joined: {alias}

A new member has joined the hive.

Member: {peer_id}
Alias: {alias}
Tier: {tier}
Joined: {timestamp}
Channels: {channel_count}
Capacity: {capacity_sats} sats

Welcome them to the fleet!

— Hive Governance System
Signed: {hive_admin_did}
```

#### 1.2 Welcome Message (to new member)
**Trigger:** Member successfully joins
**Recipients:** New member only
**Template:**
```
Subject: [HIVE] Welcome to {hive_name}

Welcome to the hive!

Your membership:
- Tier: neophyte (90-day probation)
- Voting rights: Limited until promotion
- Settlement: Eligible after first cycle

Getting Started:
1. Open channels to other fleet members (0 fee internally)
2. Participate in routing to build contribution score
3. Request promotion after demonstrating value

Fleet Members:
{member_list}

Questions? Contact: {admin_contact}

— Hive Governance System
```

#### 1.3 Member Left
**Trigger:** `handle_member_left()`
**Recipients:** All members
**Template:**
```
Subject: [HIVE] Member Departed: {alias}

A member has left the hive.

Member: {peer_id}
Alias: {alias}
Reason: {reason}  # voluntary, banned, inactive
Duration: {membership_duration}

{if reason == "voluntary"}
Their channels remain open but are no longer hive-internal.
Consider adjusting fees on channels to this peer.
{/if}

— Hive Governance System
```

---

### 2. Promotion Governance

#### 2.1 Promotion Proposed
**Trigger:** `hive-propose-promotion` called
**Recipients:** All voting members + the nominee
**Template:**
```
Subject: [HIVE] Promotion Proposal: {alias} → Member

A promotion has been proposed.

Nominee: {peer_id} ({alias})
Current Tier: neophyte
Proposed Tier: member
Proposer: {proposer_alias}

Nominee Stats:
- Membership Duration: {days} days
- Contribution Score: {score}
- Routing Volume: {volume_sats} sats
- Vouches: {vouch_count}

Vote Deadline: {deadline}
Quorum Required: {quorum_pct}% ({quorum_count} votes)

To vote:
  lightning-cli hive-vote-promotion {peer_id} approve="true"

— Hive Governance System
```

#### 2.2 Promotion Vote Cast
**Trigger:** `hive-vote-promotion` called
**Recipients:** Nominee + proposer
**Template:**
```
Subject: [HIVE] Vote Cast on Your Promotion

A vote has been cast on the promotion proposal.

Voter: {voter_alias}
Vote: {approve/reject}
Current Tally: {approve_count} approve / {reject_count} reject
Quorum: {current}/{required}

{if quorum_reached}
Quorum reached! Promotion will be executed.
{else}
{remaining} more votes needed.
{/if}

— Hive Governance System
```

#### 2.3 Promotion Executed
**Trigger:** Quorum reached and promotion applied
**Recipients:** All members
**Template:**
```
Subject: [HIVE] Promotion Complete: {alias} is now a Member

The promotion has been executed.

Member: {peer_id} ({alias})
New Tier: member
Effective: {timestamp}

New privileges:
- Full voting rights
- Settlement participation
- Can propose new members

Final Vote: {approve_count} approve / {reject_count} reject

Congratulations {alias}!

— Hive Governance System
```

---

### 3. Ban Governance

#### 3.1 Ban Proposed
**Trigger:** `handle_ban_proposal()` or gaming detected
**Recipients:** All voting members + accused (optional)
**Template:**
```
Subject: [HIVE] ⚠️ Ban Proposal: {alias}

A ban has been proposed against a hive member.

Accused: {peer_id} ({alias})
Proposer: {proposer_alias}
Reason: {reason}

Evidence:
{evidence_details}

Vote Deadline: {deadline}
Quorum Required: {quorum_pct}% to ban

To vote:
  lightning-cli hive-vote-ban {peer_id} {proposal_id} approve="true|false"

NOTE: Non-votes count as implicit approval after deadline.

— Hive Governance System
```

#### 3.2 Ban Vote Cast
**Trigger:** Ban vote received
**Recipients:** Proposer + accused
**Template:**
```
Subject: [HIVE] Ban Vote Update: {alias}

A vote has been cast on the ban proposal.

Voter: {voter_alias}
Vote: {approve_ban/reject_ban}
Current Tally: {approve_count} ban / {reject_count} keep
Rejection Threshold: {threshold} (to prevent ban)

{if ban_prevented}
Ban has been rejected. Member remains in good standing.
{/if}

— Hive Governance System
```

#### 3.3 Ban Executed
**Trigger:** Ban quorum reached
**Recipients:** All members + banned member
**Template:**
```
Subject: [HIVE] 🚫 Member Banned: {alias}

A member has been banned from the hive.

Banned: {peer_id} ({alias})
Reason: {reason}
Effective: {timestamp}
Duration: {permanent/until_date}

Final Vote: {approve_count} ban / {reject_count} keep
Implicit approvals: {implicit_count}

Actions taken:
- Removed from member list
- Settlement distributions suspended
- Peer ID added to ban list

{if channels_remain}
Note: {channel_count} channels remain open. Consider closing.
{/if}

— Hive Governance System
```

---

### 4. Settlement Governance

#### 4.1 Settlement Cycle Starting
**Trigger:** `settlement_loop()` initiates new cycle
**Recipients:** All members
**Template:**
```
Subject: [HIVE] Settlement Cycle {period} Starting

A new settlement cycle is beginning.

Period: {period_id}
Start: {start_timestamp}
End: {end_timestamp}

Current Pool:
- Total Revenue: {total_revenue_sats} sats
- Eligible Members: {member_count}
- Your Contribution: {your_contribution_pct}%

Ensure your BOLT12 offer is registered:
  lightning-cli hive-register-settlement-offer {your_bolt12}

— Hive Governance System
```

#### 4.2 Settlement Ready to Execute
**Trigger:** All members confirmed ready
**Recipients:** All participating members
**Template:**
```
Subject: [HIVE] Settlement {period} Ready for Execution

Settlement is ready to execute.

Period: {period_id}
Total Pool: {total_sats} sats

Distribution Preview:
{for each member}
  {alias}: {amount_sats} sats ({contribution_pct}%)
{/for}

Execution will begin in {countdown}.
Payments via BOLT12 offers.

— Hive Governance System
```

#### 4.3 Settlement Complete
**Trigger:** `handle_settlement_executed()`
**Recipients:** All participating members
**Template:**
```
Subject: [HIVE] ✅ Settlement {period} Complete

Settlement has been executed successfully.

Period: {period_id}
Total Distributed: {total_sats} sats

Your Receipt:
- Amount Received: {your_amount_sats} sats
- Contribution Score: {your_score}
- Payment Hash: {payment_hash}

Full Distribution:
{for each member}
  {alias}: {amount_sats} sats ✓
{/for}

This message serves as a cryptographic receipt.

— Hive Governance System
Signed: {settlement_coordinator_did}
```

#### 4.4 Settlement Gaming Detected
**Trigger:** `_check_settlement_gaming_and_propose_bans()`
**Recipients:** All members + accused
**Template:**
```
Subject: [HIVE] ⚠️ Settlement Gaming Detected

Potential settlement gaming has been detected.

Accused: {peer_id} ({alias})
Violation: {violation_type}

Evidence:
- Metric: {metric_name}
- Your Value: {member_value}
- Fleet Median: {median_value}
- Z-Score: {z_score} (threshold: {threshold})

{if auto_ban_proposed}
A ban proposal has been automatically created.
Proposal ID: {proposal_id}
{/if}

— Hive Governance System
```

---

### 5. Health & Alerts

#### 5.1 Member Health Critical
**Trigger:** NNLB health score < threshold
**Recipients:** Affected member + fleet coordinator
**Template:**
```
Subject: [HIVE] 🔴 Health Critical: {alias} ({health_score}/100)

Your node health has dropped to critical levels.

Node: {peer_id} ({alias})
Health Score: {health_score}/100
Tier: {health_tier}  # critical, struggling, stable, thriving

Issues Detected:
{for each issue}
  - {issue_description}
{/for}

Recommended Actions:
1. {recommendation_1}
2. {recommendation_2}
3. {recommendation_3}

The fleet may offer assistance via NNLB rebalancing.
Contact {coordinator_alias} if you need help.

— Hive Health Monitor
```

#### 5.2 Fleet-Wide Alert
**Trigger:** Admin or automated detection
**Recipients:** All members
**Template:**
```
Subject: [HIVE] 📢 Fleet Alert: {alert_title}

An important alert for all fleet members.

Alert Type: {alert_type}
Severity: {severity}
Time: {timestamp}

Details:
{alert_body}

Required Action: {action_required}
Deadline: {deadline}

— Hive Governance System
```

---

### 6. Channel Coordination

#### 6.1 Channel Open Suggestion
**Trigger:** Expansion recommendations or MCF optimization
**Recipients:** Specific member
**Template:**
```
Subject: [HIVE] Channel Suggestion: Open to {target_alias}

The fleet coordinator suggests opening a channel.

Target: {target_peer_id} ({target_alias})
Suggested Size: {size_sats} sats
Reason: {reason}

Benefits:
- {benefit_1}
- {benefit_2}

To proceed:
  lightning-cli fundchannel {target_peer_id} {size_sats}

This is a suggestion, not a requirement.

— Fleet Coordinator
```

#### 6.2 Channel Close Recommendation
**Trigger:** Rationalization analysis
**Recipients:** Channel owner
**Template:**
```
Subject: [HIVE] Channel Review: Consider Closing {channel_id}

A channel has been flagged for potential closure.

Channel: {short_channel_id}
Peer: {peer_alias}
Reason: {reason}

Analysis:
- Age: {age_days} days
- Your Routing Activity: {your_routing_pct}%
- Owner's Routing Activity: {owner_routing_pct}%
- Recommendation: {close/keep/monitor}

{if close_recommended}
This peer is better served by {owner_alias} who routes {owner_pct}% of traffic.
Closing would free {capacity_sats} sats for better positioning.
{/if}

— Fleet Rationalization System
```

#### 6.3 Splice Coordination
**Trigger:** `hive-splice` initiated
**Recipients:** Splice counterparty
**Template:**
```
Subject: [HIVE] Splice Request: {channel_id}

A splice operation has been proposed for your channel.

Channel: {short_channel_id}
Initiator: {initiator_alias}
Operation: {add/remove} {amount_sats} sats

Current State:
- Capacity: {current_capacity} sats
- Your Balance: {your_balance} sats

Proposed State:
- New Capacity: {new_capacity} sats
- Your New Balance: {new_balance} sats

To accept:
  lightning-cli hive-splice-accept {splice_id}

To reject:
  lightning-cli hive-splice-reject {splice_id}

Expires: {expiry_timestamp}

— Hive Splice Coordinator
```

---

### 7. Positioning & Strategy

#### 7.1 Positioning Proposal
**Trigger:** Physarum/positioning analysis
**Recipients:** Relevant members
**Template:**
```
Subject: [HIVE] Positioning Proposal: {corridor_name}

A strategic positioning opportunity has been identified.

Corridor: {source} → {destination}
Value Score: {corridor_score}
Current Coverage: {coverage_pct}%

Proposal:
{proposal_details}

Assigned Member: {assigned_alias}
Reason: {assignment_reason}

Expected Impact:
- Revenue Increase: ~{revenue_estimate} sats/month
- Network Position: {position_improvement}

— Fleet Strategist
```

#### 7.2 MCF Assignment
**Trigger:** MCF optimizer assigns rebalance task
**Recipients:** Assigned member
**Template:**
```
Subject: [HIVE] MCF Assignment: Rebalance {from_channel} → {to_channel}

You've been assigned a rebalance task by the MCF optimizer.

Assignment ID: {assignment_id}
From Channel: {from_channel} ({from_balance}% local)
To Channel: {to_channel} ({to_balance}% local)
Amount: {amount_sats} sats
Max Fee: {max_fee_sats} sats

Deadline: {deadline}
Priority: {priority}

To claim and execute:
  lightning-cli hive-claim-mcf-assignment {assignment_id}

If you cannot complete this, it will be reassigned.

— MCF Optimizer
```

---

## Database Schema

```sql
-- Member contact registry for Archon messaging
CREATE TABLE member_archon_contacts (
    peer_id TEXT PRIMARY KEY,
    alias TEXT,
    archon_did TEXT,                    -- did:cid:bagaaiera...
    notify_preferences TEXT,            -- JSON: ["health", "governance", "settlement"]
    registered_at INTEGER,
    verified_at INTEGER,                -- When DID ownership was verified
    last_message_at INTEGER
);

-- Outbound message queue
CREATE TABLE archon_message_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_type TEXT NOT NULL,         -- 'promotion_proposed', 'settlement_complete', etc.
    recipient_did TEXT NOT NULL,
    recipient_peer_id TEXT,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    priority TEXT DEFAULT 'normal',     -- 'low', 'normal', 'high', 'critical'
    created_at INTEGER NOT NULL,
    scheduled_for INTEGER,              -- For delayed delivery
    sent_at INTEGER,
    delivery_status TEXT DEFAULT 'pending',  -- 'pending', 'sent', 'failed', 'delivered'
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    message_cid TEXT                    -- IPFS CID after sending
);

-- Inbound message tracking
CREATE TABLE archon_message_inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_cid TEXT UNIQUE,
    sender_did TEXT NOT NULL,
    sender_peer_id TEXT,
    subject TEXT,
    body TEXT,
    received_at INTEGER NOT NULL,
    read_at INTEGER,
    message_type TEXT,                  -- Parsed from subject/body
    archived INTEGER DEFAULT 0
);

-- Message templates (customizable)
CREATE TABLE archon_message_templates (
    template_id TEXT PRIMARY KEY,
    subject_template TEXT NOT NULL,
    body_template TEXT NOT NULL,
    variables TEXT,                     -- JSON list of required variables
    updated_at INTEGER
);

CREATE INDEX idx_message_queue_status ON archon_message_queue(delivery_status, created_at);
CREATE INDEX idx_message_inbox_sender ON archon_message_inbox(sender_did, received_at);
```

---

## Implementation Plan

*Priority order based on RFC feedback (Morningstar 2026-02-12)*

### Phase 1: Settlement Receipts (Highest Value)
1. Core `HiveArchonBridge` class for Keymaster integration
2. Database tables: contacts, message queue, templates
3. Settlement receipt template (signed, verifiable)
4. `hive-settlement-receipt` RPC
5. Auto-send on `handle_settlement_executed()`

### Phase 2: DID Setup + Backup Integration
1. Docker wizard: "Enable Archon governance messaging? (y/n)"
2. `archon-backup` skill integration for vault recovery
3. Three tiers: self-custody (default), fleet-custodial (opt-in), no DID
4. Passphrase handling via Docker secrets
5. Recovery path documentation

### Phase 3: Nostr Hybrid for Health Alerts
1. Add `nostr_npub` and `nostr_relays` to contacts table
2. Dual-send for critical events (Nostr + Archon)
3. Health critical alerts via both channels
4. Nostr: push notification, Archon: permanent receipt
5. Correlation logging for audit

### Phase 4: Contact Registry + Verification
1. `hive-register-contact` RPC — Map peer_id → DID + npub
2. Challenge-response DID verification flow
3. `verified_at` timestamp tracking
4. Contact import/export (JSON format)

### Phase 5: Ban Governance
1. Ban proposal templates with evidence
2. Vote tracking and execution receipts
3. Auto-notify on proposal, vote, execution
4. Verifiable credentials for votes (future)

### Phase 6: Full Governance Suite
1. Remaining templates (25+ types)
2. Dispute resolution flow
3. Config change governance
4. Emergency coordinator actions with audit trail
5. Message urgency categorization (immediate/batched/receipts)

### Phase 7: Advisor + Rate Limiting
1. Advisor sends dmails on behalf of fleet
2. Per-sender rate limits with escalation path
3. Inbox polling and message history
4. Daily digest option for batched messages

---

## RPC Methods

```python
# Contact management
hive-register-contact(peer_id, alias, archon_did, notify_preferences)
hive-update-contact(peer_id, ...)
hive-remove-contact(peer_id)
hive-list-contacts()
hive-verify-contact(peer_id)  # Challenge-response DID verification

# Messaging
hive-dmail-send(recipient, subject, body, priority)
hive-dmail-broadcast(tier, subject, body)  # Send to all members of tier
hive-dmail-check()  # Poll for new messages
hive-dmail-inbox(limit, offset, unread_only)
hive-dmail-read(message_id)
hive-dmail-queue-status()

# Templates
hive-dmail-templates()
hive-dmail-template-preview(template_id, variables)
hive-dmail-template-update(template_id, subject, body)
```

---

---

## Additional Governance Events (from RFC feedback)

### 8. Dispute Resolution

#### 8.1 Dispute Filed
**Trigger:** Member files formal dispute
**Recipients:** All voting members + dispute parties
**Template:**
```
Subject: [HIVE] ⚖️ Dispute Filed: {dispute_title}

A formal dispute has been filed.

Complainant: {complainant_alias}
Respondent: {respondent_alias}
Type: {dispute_type}  # fee_disagreement, force_close, settlement_calculation, other

Description:
{dispute_description}

Evidence:
{evidence_summary}

Resolution Deadline: {deadline}
Arbitration Required: {yes/no}

To respond:
  lightning-cli hive-dispute-respond {dispute_id} response="..."

— Hive Governance System
```

#### 8.2 Dispute Resolved
**Trigger:** Resolution reached (vote, arbitration, or settlement)
**Recipients:** All members + dispute parties
**Template:**
```
Subject: [HIVE] ⚖️ Dispute Resolved: {dispute_title}

The dispute has been resolved.

Resolution: {resolution_summary}
Method: {vote/arbitration/settlement}
Decision: {in_favor_of}

Actions Required:
{for each action}
  - {party}: {required_action}
{/for}

This decision is final and binding.

— Hive Governance System
Signed: {arbitrator_did}
```

---

### 9. Config Change Governance

#### 9.1 Config Change Proposed
**Trigger:** Admin proposes fleet-wide parameter change
**Recipients:** All voting members
**Template:**
```
Subject: [HIVE] 🔧 Config Change Proposal: {param_name}

A fleet-wide configuration change has been proposed.

Parameter: {param_name}
Category: {category}  # settlement, health, fees, governance
Current Value: {current_value}
Proposed Value: {new_value}
Proposer: {proposer_alias}

Rationale:
{rationale}

Impact Assessment:
{impact_summary}

Vote Deadline: {deadline}
Quorum Required: {quorum_pct}%

To vote:
  lightning-cli hive-vote-config {proposal_id} approve="true|false"

— Hive Governance System
```

#### 9.2 Config Change Executed
**Trigger:** Quorum reached and config applied
**Recipients:** All members
**Template:**
```
Subject: [HIVE] 🔧 Config Updated: {param_name}

A configuration change has been applied.

Parameter: {param_name}
Old Value: {old_value}
New Value: {new_value}
Effective: {timestamp}

Final Vote: {approve_count} approve / {reject_count} reject

All nodes will apply this change within {propagation_time}.

— Hive Governance System
```

---

### 10. Emergency Coordinator Actions

#### 10.1 Emergency Override Executed
**Trigger:** Coordinator bypasses normal governance for urgent action
**Recipients:** All members
**Template:**
```
Subject: [HIVE] 🚨 Emergency Action: {action_title}

An emergency action has been taken by the coordinator.

Action: {action_description}
Coordinator: {coordinator_alias}
Time: {timestamp}
Severity: {severity}

Justification:
{justification}

Affected:
{for each affected}
  - {member_alias}: {impact}
{/for}

This action was taken under emergency authority. A retrospective review
will be conducted at the next governance meeting.

— Hive Governance System
Signed: {coordinator_did}
```

#### 10.2 Emergency Authority Invoked
**Trigger:** Coordinator declares emergency state
**Recipients:** All members
**Template:**
```
Subject: [HIVE] 🚨 Emergency State Declared

The fleet coordinator has declared an emergency state.

Reason: {reason}
Duration: {expected_duration}
Authority Level: {level}  # advisory, limited, full

During this period:
- Normal governance votes may be expedited
- Coordinator may take {allowed_actions}
- All emergency actions will be logged and audited

Emergency ends: {end_condition}

— Hive Governance System
```

---

## Nostr Hybrid Architecture

For real-time notifications combined with permanent audit trails.

### Design

| Channel | Use Case | Properties |
|---------|----------|------------|
| **Nostr** | Real-time alerts | Push notifications, low latency, ephemeral |
| **Archon dmail** | Permanent receipts | Verifiable, encrypted, audit trail |

### Dual-Send Events

Critical events send via both channels:
- Nostr: Immediate notification
- Archon: "Full receipt available via dmail [CID]"

Events using dual-send:
- Health critical alerts
- Ban votes (proposal + execution)
- Settlement complete
- Emergency actions

### Database Extension

```sql
-- Add Nostr npub to contacts
ALTER TABLE member_archon_contacts ADD COLUMN nostr_npub TEXT;
ALTER TABLE member_archon_contacts ADD COLUMN nostr_relays TEXT;  -- JSON array

-- Track dual-send correlation
ALTER TABLE archon_message_queue ADD COLUMN nostr_event_id TEXT;
```

### Implementation

1. On critical event:
   ```python
   # Send Nostr first (real-time)
   nostr_event_id = send_nostr_dm(npub, short_alert)
   
   # Send Archon (permanent receipt)
   cid = send_archon_dmail(did, full_message)
   
   # Correlate for audit
   log_dual_send(event_type, nostr_event_id, cid)
   ```

2. Nostr message format:
   ```
   🔔 [HIVE] {short_summary}
   Full receipt: archon:dmail:{cid}
   ```

---

## Message Urgency Categories

### Immediate (send now)
- Health critical alerts
- Ban proposals and votes
- Emergency actions
- Settlement gaming detected

### Batched (daily digest option)
- Promotion proposals
- Channel suggestions
- Positioning proposals
- Non-critical health updates

### Receipts (immediate, permanent)
- Settlement complete (signed receipt)
- Ban executed
- Config change executed
- Dispute resolved

---

## DID Verification Flow

Challenge-response verification to prove DID ownership:

```
1. Member claims DID: hive-register-contact peer_id=X archon_did=did:cid:Y

2. Fleet generates random challenge:
   challenge = random_bytes(32).hex()
   store_challenge(peer_id, challenge, expires=1h)

3. Fleet sends challenge to claimed DID:
   Subject: [HIVE] Verify Your DID
   Body: Sign this challenge: {challenge}
         Reply with signature to complete verification.

4. Member signs with DID private key:
   signature = keymaster_sign(challenge)
   hive-verify-contact peer_id=X signature=Z

5. Fleet verifies signature:
   if keymaster_verify(did, challenge, signature):
       mark_verified(peer_id, timestamp)
       send_confirmation()
   else:
       reject_verification()
```

---

## Rate Limiting

### Per-Sender Limits
| Sender Type | Limit | Window |
|-------------|-------|--------|
| Regular member | 10 msgs | 1 hour |
| Coordinator | 50 msgs | 1 hour |
| System (auto) | 100 msgs | 1 hour |
| Broadcast | 3 msgs | 24 hours |

### Escalation Path
Critical alerts bypass rate limits:
- `priority = "critical"` → no rate limit
- Requires coordinator signature
- Logged for audit

---

## DID Recovery & Backup

### Self-Custody (Default)
Integration with `archon-backup` skill:

1. During setup: Auto-backup DID credentials to personal vault
2. On node rebuild: "Restore DID from vault or create new?"
3. Recovery path documented in setup wizard

```bash
# Backup during setup
archon-backup backup-to-vault ~/.archon/wallet.json node-did-vault

# Restore on rebuild
archon-backup restore-from-vault node-did-vault ~/.archon/wallet.json
```

### Fleet-Custodial (Opt-in)
For operators who prefer convenience:

1. Coordinator holds encrypted backup of member DIDs
2. Member can request recovery via signed request
3. Trade-off: convenience vs full sovereignty

```sql
-- Optional custodial backup storage
CREATE TABLE member_did_backups (
    peer_id TEXT PRIMARY KEY,
    encrypted_backup BLOB,          -- Encrypted with member's recovery key
    backup_created_at INTEGER,
    recovery_key_hint TEXT,         -- Hint for recovery key, not the key itself
    last_recovery_request INTEGER
);
```

### Recovery Tiers
| Tier | Method | Sovereignty | Convenience |
|------|--------|-------------|-------------|
| Full self-custody | Personal vault only | ★★★★★ | ★★☆☆☆ |
| Fleet-custodial | Coordinator backup | ★★★☆☆ | ★★★★☆ |
| No DID | Minimal mode | N/A | ★★★★★ |

---

## Security Considerations

1. **Passphrase handling**: Never log or expose `ARCHON_PASSPHRASE`
2. **DID verification**: Challenge-response verification before trusting claimed DIDs
3. **Rate limiting**: Per-sender limits with critical escalation path
4. **Encryption**: All dmails are E2E encrypted by Archon
5. **Non-repudiation**: All messages signed by sender DID
6. **Retention policy**: Auto-delete old messages per config
7. **Emergency audit**: All emergency actions logged with coordinator signature
8. **Backup security**: Custodial backups encrypted with member-controlled keys
