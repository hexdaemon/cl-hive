# AI Oracle Protocol Specification

**Version:** 0.1.0-draft
**Status:** Proposal
**Authors:** cl-hive contributors
**Date:** 2026-01-14

## Abstract

This specification defines a protocol extension for cl-hive that enables AI agents operating in Oracle mode to communicate, coordinate, and collaborate when managing Lightning Network nodes. The protocol provides structured message types for strategy coordination, task delegation, reasoning exchange, and collective intelligence while maintaining the security properties of the existing Hive protocol.

## Table of Contents

1. [Motivation](#1-motivation)
2. [Design Principles](#2-design-principles)
3. [Protocol Overview](#3-protocol-overview)
4. [Message Types](#4-message-types)
5. [Oracle API](#5-oracle-api)
6. [Security Considerations](#6-security-considerations)
7. [Implementation Guidelines](#7-implementation-guidelines)
8. [Future Extensions](#8-future-extensions)

---

## 1. Motivation

### 1.1 Current State

The cl-hive protocol currently supports three governance modes:
- **Advisor**: Human reviews and approves all actions
- **Autonomous**: Node executes within predefined safety bounds
- **Oracle**: External API makes decisions

Oracle mode is designed for programmatic decision-making, but the current implementation assumes a simple request/response pattern where the oracle receives pending actions and returns approve/reject decisions.

### 1.2 The AI Agent Opportunity

When AI agents serve as oracles for multiple Hive nodes, new possibilities emerge:
- **Collective Intelligence**: AIs can share insights and reach better decisions together
- **Coordinated Strategy**: Fleet-wide strategies can be negotiated and executed
- **Task Delegation**: AIs can assign tasks based on node capabilities
- **Emergent Optimization**: Swarm behavior may outperform individual optimization

### 1.3 Why Structured Communication?

Rather than allowing arbitrary text communication (which poses security and bandwidth risks), this protocol defines **typed, schema-validated messages** that:
- Can be verified and audited
- Have bounded size and complexity
- Support the specific coordination patterns AIs need
- Maintain the security properties of the Hive protocol

---

## 2. Design Principles

### 2.1 Structured Over Unstructured

All AI communication uses defined message schemas. No free-form text fields that could serve as prompt injection vectors.

### 2.2 Verifiable and Auditable

Every AI decision and communication is logged with reasoning hashes that can be verified later. The fleet can audit AI behavior.

### 2.3 Fail-Safe Defaults

If AI communication fails, nodes fall back to existing behavior (advisor mode queuing or autonomous bounds). AI coordination enhances but doesn't replace core safety.

### 2.4 Bandwidth Conscious

AI messages are summarized for gossip. Full reasoning is available on-demand via request/response patterns.

### 2.5 Consensus Without Centralization

Strategies require quorum approval. No single AI can dictate fleet behavior. Dissenting AIs can opt out of coordinated actions.

### 2.6 Human Override

Node operators can always override AI decisions. AI coordination is a tool, not a replacement for human judgment on critical matters.

---

## 3. Protocol Overview

### 3.1 Message Type Range

AI Oracle messages use type range **32800-32899** (50 types reserved):

| Range | Category |
|-------|----------|
| 32800-32809 | Information Sharing |
| 32810-32819 | Task Coordination |
| 32820-32829 | Strategy Coordination |
| 32830-32839 | Reasoning Exchange |
| 32840-32849 | Health & Alerts |
| 32850-32899 | Reserved for Future |

### 3.2 Message Flow

```
┌─────────────┐                              ┌─────────────┐
│   Node A    │                              │   Node B    │
│  (AI Agent) │                              │  (AI Agent) │
└──────┬──────┘                              └──────┬──────┘
       │                                            │
       │  AI_STATE_SUMMARY (periodic broadcast)     │
       │ ─────────────────────────────────────────► │
       │                                            │
       │  AI_OPPORTUNITY_SIGNAL                     │
       │ ◄───────────────────────────────────────── │
       │                                            │
       │  AI_TASK_REQUEST                           │
       │ ─────────────────────────────────────────► │
       │                                            │
       │  AI_TASK_RESPONSE (accept)                 │
       │ ◄───────────────────────────────────────── │
       │                                            │
       │  AI_TASK_COMPLETE                          │
       │ ◄───────────────────────────────────────── │
       │                                            │
```

### 3.3 Integration with Existing Protocol

AI messages travel over the existing Hive custom message infrastructure:
- Same PKI authentication (signmessage/checkmessage)
- Same peer-to-peer delivery via custommsg
- Same gossip patterns for broadcasts
- Extends, doesn't replace, existing message types

---

## 4. Message Types

### 4.1 Information Sharing (32800-32809)

#### 4.1.1 AI_STATE_SUMMARY (0x8020 / 32800)

Periodic broadcast summarizing an AI agent's current state and priorities.

**Frequency**: Every heartbeat interval (default 5 minutes)
**Delivery**: Broadcast to all Hive members

```json
{
  "type": "ai_state_summary",
  "version": 1,
  "node_id": "03abc123...",
  "timestamp": 1705234567,
  "sequence": 12345,

  "liquidity": {
    "status": "healthy",
    "capacity_tier": "large",
    "outbound_status": "adequate",
    "inbound_status": "adequate",
    "channel_count_tier": "medium",
    "utilization_bucket": "moderate"
  },

  "priorities": {
    "current_focus": "expansion",
    "seeking_categories": ["routing_hub", "exchange"],
    "avoid_categories": [],
    "capacity_seeking": true,
    "budget_status": "available"
  },

  "capabilities": {
    "can_open_channels": true,
    "can_accept_tasks": true,
    "expansion_capacity_tier": "medium",
    "feerate_tolerance": "normal"
  },

  "ai_meta": {
    "confidence": 0.85,
    "decisions_last_24h": 15,
    "strategy_alignment": "cooperative"
  },

  "signature": "dhbc4mqjz..."
}
```

**Fields**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| type | string | Yes | Message type identifier |
| version | int | Yes | Schema version |
| node_id | string | Yes | Sender's node public key |
| timestamp | int | Yes | Unix timestamp |
| sequence | int | Yes | Monotonic sequence number |
| liquidity | object | Yes | Bucketed liquidity state (no exact values) |
| liquidity.status | enum | Yes | "healthy", "constrained", "critical" |
| liquidity.capacity_tier | enum | Yes | "small", "medium", "large", "xlarge" |
| priorities | object | Yes | Current AI priorities |
| priorities.current_focus | enum | Yes | "expansion", "consolidation", "maintenance", "defensive" |
| priorities.seeking_categories | array | Yes | Node categories being targeted (not specific pubkeys) |
| capabilities | object | Yes | Task acceptance capabilities |
| ai_meta | object | Yes | AI agent metadata |
| signature | string | Yes | PKI signature of message hash |

**Privacy Buckets** (prevents exact balance disclosure):

| Tier | Capacity Range | Utilization Bucket |
|------|----------------|-------------------|
| small | < 10M sats | low (< 30%) |
| medium | 10M - 100M sats | moderate (30-60%) |
| large | 100M - 1B sats | high (60-80%) |
| xlarge | > 1B sats | critical (> 80%) |

---

#### 4.1.2 AI_OPPORTUNITY_SIGNAL (0x8021 / 32801)

AI identifies an opportunity and signals to the fleet.

**Delivery**: Broadcast to all Hive members

```json
{
  "type": "ai_opportunity_signal",
  "version": 1,
  "node_id": "03abc123...",
  "timestamp": 1705234567,
  "signal_id": "sig_a1b2c3d4",

  "opportunity": {
    "target_node": "02xyz789...",
    "target_alias": "ACINQ",
    "opportunity_type": "high_value_target",
    "category": "routing_hub"
  },

  "analysis": {
    "target_capacity_sats": 50000000000,
    "target_channel_count": 500,
    "current_hive_share_pct": 0.05,
    "optimal_hive_share_pct": 0.15,
    "share_gap_pct": 10.0,
    "estimated_daily_volume_sats": 100000000,
    "avg_fee_rate_ppm": 150
  },

  "recommendation": {
    "action": "expand",
    "urgency": "medium",
    "suggested_capacity_sats": 20000000,
    "estimated_roi_annual_pct": 8.5,
    "confidence": 0.75
  },

  "volunteer": {
    "willing": true,
    "capacity_available_sats": 25000000,
    "position_score": 0.8
  },

  "reasoning_factors": ["high_volume", "low_hive_share", "strong_fee_potential", "good_position"],

  "signature": "dhbc4mqjz..."
}
```

**Opportunity Types**:

| Type | Description |
|------|-------------|
| high_value_target | Well-connected node with routing potential |
| underserved | Node with low hive share vs optimal |
| fee_arbitrage | Fee mispricing opportunity |
| liquidity_need | Hive member needs inbound/outbound |
| defensive | Competitor activity requires response |
| emerging | New node showing growth signals |

---

#### 4.1.3 AI_MARKET_ASSESSMENT (0x8022 / 32802)

AI shares analysis of market conditions.

**Delivery**: Broadcast to all Hive members
**Frequency**: On significant market changes or periodic (hourly)

```json
{
  "type": "ai_market_assessment",
  "version": 1,
  "node_id": "03abc123...",
  "timestamp": 1705234567,
  "assessment_id": "assess_x1y2z3",

  "assessment_type": "fee_trend",
  "time_horizon": "short_term",

  "market_data": {
    "avg_network_fee_ppm": 250,
    "fee_change_24h_pct": 12.5,
    "mempool_depth_vbytes": 15000000,
    "mempool_fee_rate_sat_vb": 25,
    "block_fullness_pct": 95.0
  },

  "corridor_analysis": [
    {
      "corridor": "exchanges_to_retail",
      "volume_trend": "increasing",
      "fee_trend": "increasing",
      "competition": "moderate",
      "hive_position": "strong"
    },
    {
      "corridor": "us_to_eu",
      "volume_trend": "stable",
      "fee_trend": "decreasing",
      "competition": "high",
      "hive_position": "weak"
    }
  ],

  "recommendation": {
    "overall_stance": "opportunistic",
    "fee_direction": "raise_floor",
    "expansion_timing": "favorable",
    "rebalance_urgency": "low"
  },

  "confidence": 0.70,
  "data_freshness_seconds": 300,

  "signature": "dhbc4mqjz..."
}
```

---

### 4.2 Task Coordination (32810-32819)

#### 4.2.1 AI_TASK_REQUEST (0x802A / 32810)

AI requests another node to perform a task.

**Delivery**: Direct to target node

```json
{
  "type": "ai_task_request",
  "version": 1,
  "node_id": "03abc123...",
  "target_node": "03def456...",
  "timestamp": 1705234567,
  "request_id": "req_a1b2c3d4e5f6",

  "task": {
    "task_type": "expand_to",
    "target": "02xyz789...",
    "parameters": {
      "amount_sats": 10000000,
      "max_fee_sats": 5000,
      "min_channels": 1,
      "max_channels": 1
    },
    "deadline_timestamp": 1705320967,
    "priority": "normal"
  },

  "context": {
    "selection_factors": ["existing_peer", "lower_hop_count", "better_position_score"],
    "opportunity_signal_id": "sig_a1b2c3d4",
    "fleet_benefit": {"metric": "hive_share_pct", "from": 5, "to": 8}
  },

  "compensation": {
    "offer_type": "reciprocal",
    "credit_value": 1.0,
    "current_balance": -2.0,
    "lifetime_requested": 5,
    "lifetime_fulfilled": 3
  },

  "fallback": {
    "if_rejected": "will_handle_self",
    "if_timeout": "will_handle_self"
  },

  "signature": "dhbc4mqjz..."
}
```

**Task Types**:

| Type | Description | Parameters |
|------|-------------|------------|
| expand_to | Open channel to target | amount_sats, max_fee_sats |
| rebalance_toward | Push liquidity toward target | amount_sats, max_ppm |
| probe_route | Test route viability | destination, amount_sats |
| gather_intel | Research a node | target, aspects[] |
| adjust_fees | Change fee on corridor | scid, new_fee_ppm |
| close_channel | Close a channel | scid, urgency |

---

#### 4.2.2 AI_TASK_RESPONSE (0x802B / 32811)

Response to a task request.

**Delivery**: Direct to requesting node

```json
{
  "type": "ai_task_response",
  "version": 1,
  "node_id": "03def456...",
  "timestamp": 1705234600,
  "request_id": "req_a1b2c3d4e5f6",

  "response": "accept",

  "acceptance": {
    "estimated_completion_timestamp": 1705248000,
    "actual_parameters": {
      "amount_sats": 10000000,
      "estimated_fee_sats": 3500
    },
    "conditions": []
  },

  "response_factors": ["sufficient_liquidity", "good_connection", "reciprocity_balance_positive"],

  "signature": "dhbc4mqjz..."
}
```

**Response Types**:

| Response | Description |
|----------|-------------|
| accept | Will perform the task as requested |
| accept_modified | Will perform with modified parameters |
| reject | Cannot or will not perform |
| defer | Can perform later (includes new deadline) |
| counter | Proposes alternative terms |

---

#### 4.2.3 AI_TASK_COMPLETE (0x802C / 32812)

Notification that a delegated task is complete.

**Delivery**: Direct to requesting node

```json
{
  "type": "ai_task_complete",
  "version": 1,
  "node_id": "03def456...",
  "timestamp": 1705247500,
  "request_id": "req_a1b2c3d4e5f6",

  "status": "success",

  "result": {
    "task_type": "expand_to",
    "target": "02xyz789...",
    "outcome": {
      "channel_opened": true,
      "scid": "800000x1000x0",
      "capacity_sats": 10000000,
      "actual_fee_sats": 3200,
      "funding_txid": "abc123..."
    }
  },

  "learnings": {
    "target_responsiveness": "fast",
    "connection_quality": "good",
    "recommended_for_future": true,
    "observed_traits": ["quick_acceptance", "stable_connection", "professional_operator"]
  },

  "compensation_status": {
    "reciprocal_credit": true,
    "credit_expires_timestamp": 1707839500
  },

  "signature": "dhbc4mqjz..."
}
```

---

#### 4.2.4 AI_TASK_CANCEL (0x802D / 32813)

Cancel a previously requested task.

```json
{
  "type": "ai_task_cancel",
  "version": 1,
  "node_id": "03abc123...",
  "timestamp": 1705235000,
  "request_id": "req_a1b2c3d4e5f6",

  "reason": "opportunity_expired",
  "details": "Another hive member already expanded to target",

  "signature": "dhbc4mqjz..."
}
```

---

### 4.3 Strategy Coordination (32820-32829)

#### 4.3.1 AI_STRATEGY_PROPOSAL (0x8034 / 32820)

AI proposes a fleet-wide coordinated strategy.

**Delivery**: Broadcast to all Hive members

```json
{
  "type": "ai_strategy_proposal",
  "version": 1,
  "node_id": "03abc123...",
  "timestamp": 1705234567,
  "proposal_id": "prop_s1t2r3a4t5",

  "strategy": {
    "strategy_type": "fee_coordination",
    "name": "ACINQ Corridor Fee Alignment",
    "summary": "Coordinate fee floor on ACINQ-connected channels",

    "objectives": [
      "Increase average fee revenue by 15%",
      "Reduce internal fee undercutting",
      "Establish sustainable fee floor"
    ],

    "parameters": {
      "target_corridor": "acinq_connected",
      "target_nodes": ["02xyz..."],
      "fee_floor_ppm": 150,
      "fee_ceiling_ppm": 500,
      "duration_hours": 168,
      "ramp_up_hours": 24
    },

    "expected_outcomes": {
      "revenue_change_pct": 15,
      "volume_change_pct": -5,
      "net_benefit_pct": 10,
      "confidence": 0.70
    },

    "risks": [
      {
        "risk": "volume_loss",
        "probability": 0.3,
        "impact": "medium",
        "mitigation": "Gradual ramp-up allows adjustment"
      }
    ],

    "opt_out_allowed": true,
    "opt_out_penalty": "none"
  },

  "voting": {
    "approval_threshold_pct": 51,
    "min_participation_pct": 60,
    "voting_deadline_timestamp": 1705320967,
    "execution_delay_hours": 24,
    "vote_weight": "equal"
  },

  "proposer_commitment": {
    "will_participate": true,
    "capacity_committed_sats": 100000000
  },

  "signature": "dhbc4mqjz..."
}
```

**Strategy Types**:

| Type | Description |
|------|-------------|
| fee_coordination | Align fees across hive for corridor |
| expansion_campaign | Coordinated expansion to target(s) |
| rebalance_ring | Circular rebalancing among members |
| defensive | Response to competitive threat |
| liquidity_sharing | Redistribute liquidity within hive |
| channel_cleanup | Coordinated closure of unprofitable channels |

---

#### 4.3.2 AI_STRATEGY_VOTE (0x8035 / 32821)

Vote on a strategy proposal.

**Delivery**: Broadcast to all Hive members (votes are public for verifiability)

```json
{
  "type": "ai_strategy_vote",
  "version": 1,
  "node_id": "03def456...",
  "timestamp": 1705250000,
  "proposal_id": "prop_s1t2r3a4t5",

  "vote": "approve",
  "vote_hash": "sha256(proposal_id || node_id || vote || timestamp || nonce)",
  "nonce": "random_32_bytes_hex",

  "rationale": {
    "factors": ["corridor_underpricing", "reasonable_elasticity", "adequate_mitigation"],
    "confidence_in_proposal": 0.75
  },

  "commitment": {
    "will_participate": true,
    "capacity_committed_sats": 75000000,
    "conditions": []
  },

  "amendments": null,

  "signature": "dhbc4mqjz..."
}
```

**Vote Options**:

| Vote | Description |
|------|-------------|
| approve | Support the proposal as-is |
| approve_with_amendments | Support with suggested changes |
| reject | Oppose the proposal |
| abstain | No position (doesn't count toward quorum) |

---

#### 4.3.3 AI_STRATEGY_RESULT (0x8036 / 32822)

Announcement of strategy voting result.

**Delivery**: Broadcast to all Hive members
**Sender**: Proposal originator or designated coordinator
**Verification**: Recipients MUST verify vote_proofs against collected votes before accepting result

```json
{
  "type": "ai_strategy_result",
  "version": 1,
  "node_id": "03abc123...",
  "timestamp": 1705321000,
  "proposal_id": "prop_s1t2r3a4t5",

  "result": "adopted",

  "voting_summary": {
    "votes_for": 5,
    "votes_against": 1,
    "abstentions": 1,
    "eligible_voters": 7,
    "quorum_met": true,
    "approval_pct": 71.4,
    "participation_pct": 85.7
  },

  "vote_proofs": [
    {"node_id": "03def...", "vote": "approve", "vote_hash": "abc123...", "nonce": "..."},
    {"node_id": "03ghi...", "vote": "approve", "vote_hash": "def456...", "nonce": "..."},
    {"node_id": "03jkl...", "vote": "approve", "vote_hash": "ghi789...", "nonce": "..."},
    {"node_id": "03mno...", "vote": "approve", "vote_hash": "jkl012...", "nonce": "..."},
    {"node_id": "03pqr...", "vote": "reject", "vote_hash": "mno345...", "nonce": "..."},
    {"node_id": "03stu...", "vote": "approve", "vote_hash": "pqr678...", "nonce": "..."},
    {"node_id": "03vwx...", "vote": "abstain", "vote_hash": "stu901...", "nonce": "..."}
  ],

  "execution": {
    "effective_timestamp": 1705407400,
    "coordinator_node": "03abc123...",
    "participants": ["03abc...", "03def...", "03ghi...", "03jkl...", "03mno..."],
    "opt_outs": ["03pqr..."]
  },

  "amendments_incorporated": [],

  "signature": "dhbc4mqjz..."
}
```

---

#### 4.3.4 AI_STRATEGY_UPDATE (0x8037 / 32823)

Progress update on an active strategy.

**Delivery**: Broadcast to participants
**Frequency**: Periodic during strategy execution

```json
{
  "type": "ai_strategy_update",
  "version": 1,
  "node_id": "03abc123...",
  "timestamp": 1705450000,
  "proposal_id": "prop_s1t2r3a4t5",

  "progress": {
    "phase": "execution",
    "hours_elapsed": 48,
    "hours_remaining": 120,
    "completion_pct": 28.6
  },

  "metrics": {
    "revenue_change_pct": 8.5,
    "volume_change_pct": -3.2,
    "participant_compliance_pct": 100,
    "on_track": true
  },

  "participant_status": [
    {"node": "03abc...", "status": "compliant", "contribution_pct": 22},
    {"node": "03def...", "status": "compliant", "contribution_pct": 18}
  ],

  "issues": [],

  "recommendation": "continue",

  "signature": "dhbc4mqjz..."
}
```

---

### 4.4 Reasoning Exchange (32830-32839)

#### 4.4.1 AI_REASONING_REQUEST (0x803E / 32830)

Request detailed reasoning from another AI.

**Delivery**: Direct to target node

```json
{
  "type": "ai_reasoning_request",
  "version": 1,
  "node_id": "03abc123...",
  "target_node": "03def456...",
  "timestamp": 1705234567,
  "request_id": "reason_r1e2a3s4",

  "context": {
    "reference_type": "strategy_vote",
    "reference_id": "prop_s1t2r3a4t5",
    "specific_question": "Why did you vote against the fee coordination proposal?"
  },

  "detail_level": "full",

  "signature": "dhbc4mqjz..."
}
```

---

#### 4.4.2 AI_REASONING_RESPONSE (0x803F / 32831)

Detailed reasoning in response to request.

**Delivery**: Direct to requesting node
**Security Note**: All fields use schema-defined enums to prevent prompt injection. No free-form text is interpreted by receiving AI.

```json
{
  "type": "ai_reasoning_response",
  "version": 1,
  "node_id": "03def456...",
  "timestamp": 1705234700,
  "request_id": "reason_r1e2a3s4",

  "reasoning": {
    "conclusion": "risk_exceeds_reward",

    "decision_factors": [
      {
        "factor_type": "volume_elasticity",
        "weight": 0.35,
        "assessment": "high",
        "data_point": {"metric": "volume_change_pct", "value": -12.0, "period_days": 30},
        "confidence": 0.80
      },
      {
        "factor_type": "competitor_response",
        "weight": 0.30,
        "assessment": "likely_undercut",
        "data_point": {"metric": "undercut_probability", "value": 0.75, "sample_size": 10},
        "confidence": 0.65
      },
      {
        "factor_type": "market_timing",
        "weight": 0.20,
        "assessment": "unfavorable",
        "data_point": {"metric": "mempool_trend", "value": "clearing"},
        "confidence": 0.75
      },
      {
        "factor_type": "alternative_available",
        "weight": 0.15,
        "assessment": "yes",
        "data_point": {"metric": "alternative_strategy", "value": "expansion"},
        "confidence": 0.60
      }
    ],

    "overall_confidence": 0.70,

    "data_sources": [
      "local_forwarding_history_30d",
      "fee_experiment_results",
      "competitor_fee_monitoring",
      "mempool_analysis"
    ],

    "alternative_recommendation": {
      "strategy_type": "expansion_campaign",
      "target_metric": "revenue",
      "expected_change_pct": 15,
      "risk_level": "low"
    }
  },

  "meta": {
    "reasoning_time_ms": 1250,
    "tokens_used": 2500
  },

  "signature": "dhbc4mqjz..."
}
```

---

### 4.5 Health & Alerts (32840-32849)

#### 4.5.1 AI_HEARTBEAT (0x8048 / 32840)

Extended heartbeat with AI status.

**Delivery**: Broadcast to all Hive members
**Frequency**: Every heartbeat interval

```json
{
  "type": "ai_heartbeat",
  "version": 1,
  "node_id": "03abc123...",
  "timestamp": 1705234567,
  "sequence": 54321,

  "ai_status": {
    "operational_state": "active",
    "model": "claude-opus-4.5",
    "model_version": "20251101",
    "uptime_seconds": 2592000,
    "last_decision_timestamp": 1705234000,
    "decisions_24h": 25,
    "decisions_pending": 2
  },

  "health_metrics": {
    "api_latency_ms": 150,
    "api_success_rate_pct": 99.5,
    "memory_usage_pct": 45,
    "error_rate_24h": 0.5
  },

  "capabilities": {
    "max_decisions_per_hour": 100,
    "supported_task_types": ["expand_to", "rebalance_toward", "adjust_fees"],
    "strategy_participation": true,
    "delegation_acceptance": true
  },

  "signature": "dhbc4mqjz..."
}
```

---

#### 4.5.2 AI_ALERT (0x8049 / 32841)

AI raises an alert for fleet attention.

**Delivery**: Broadcast to all Hive members

```json
{
  "type": "ai_alert",
  "version": 1,
  "node_id": "03abc123...",
  "timestamp": 1705234567,
  "alert_id": "alert_a1l2e3r4t5",

  "alert": {
    "severity": "warning",
    "category": "security",
    "alert_type": "probing_detected",

    "summary": "Unusual channel probing activity detected",

    "details": {
      "source_node": "02xyz789...",
      "probe_count": 150,
      "time_window_minutes": 10,
      "pattern": "balance_discovery",
      "affected_channels": ["800x1x0", "801x2x0", "802x3x0"]
    },

    "impact_assessment": {
      "immediate_risk": "low",
      "potential_risk": "medium",
      "affected_hive_members": 3
    }
  },

  "recommendation": {
    "action": "monitor",
    "urgency": "normal",
    "suggested_response": "Consider enabling shadow routing if available"
  },

  "auto_response_taken": {
    "action": "none",
    "reason": "Below automatic response threshold"
  },

  "signature": "dhbc4mqjz..."
}
```

**Alert Categories**:

| Category | Types |
|----------|-------|
| security | probing_detected, force_close_attempt, unusual_htlc_pattern |
| performance | high_failure_rate, liquidity_crisis, fee_war |
| opportunity | flash_opportunity, competitor_retreat, volume_surge |
| system | ai_degraded, api_unavailable, budget_exhausted |
| network | mempool_spike, block_congestion, gossip_storm |

---

## 5. Oracle Implementation

### 5.1 Required: Claude Code Plugin

**Oracle mode REQUIRES the Claude Code plugin** (`cl-hive-oracle`) for AI integration.

**Rationale**:
- Standardized security enforcement (rate limits, validation, sandboxing)
- Consistent audit logging of all AI decisions
- Secure credential management for AI provider APIs
- Verified message signing and schema validation
- Controlled execution environment

**Plugin Responsibilities**:
```
┌─────────────────────────────────────────────────────────┐
│                  cl-hive-oracle plugin                   │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
│  │   Security  │  │   Message   │  │     Audit       │ │
│  │  Validator  │  │   Signer    │  │     Logger      │ │
│  └─────────────┘  └─────────────┘  └─────────────────┘ │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │
│  │    Rate     │  │   Schema    │  │   Reciprocity   │ │
│  │   Limiter   │  │  Validator  │  │    Tracker      │ │
│  └─────────────┘  └─────────────┘  └─────────────────┘ │
├─────────────────────────────────────────────────────────┤
│                    Oracle API Layer                      │
│     (HTTP interface to Claude/other AI providers)        │
└─────────────────────────────────────────────────────────┘
```

**Configuration**:
```ini
# In lightning config
hive-oracle-enabled=true
hive-oracle-provider=anthropic
hive-oracle-api-key=sk-ant-...
hive-oracle-model=claude-sonnet-4-20250514
hive-oracle-max-tokens=4096
hive-oracle-timeout-seconds=30
```

**Plugin Commands**:
| Command | Description |
|---------|-------------|
| `hive-oracle-status` | Check oracle health and connection |
| `hive-oracle-history` | View recent AI decisions |
| `hive-oracle-pause` | Temporarily pause AI decisions |
| `hive-oracle-resume` | Resume AI decisions |
| `hive-oracle-override <action_id> <decision>` | Human override of pending decision |

### 5.2 Oracle API Overview

The Oracle API is the internal HTTP interface between the plugin and the AI provider. It enables the AI to:
- Receive events and queries from the node
- Return decisions on pending actions
- Send messages to other AI agents
- Query node and network state

### 5.3 Authentication

```
Authorization: Bearer <oracle_token>
X-Node-Signature: <signature_of_request_body>
```

The oracle token is configured at node startup. Request signatures use the node's Lightning key for verification.

### 5.4 Endpoints

#### 5.4.1 Decision Endpoint

```
POST /oracle/decision
```

Node sends pending action for AI decision.

**Request**:
```json
{
  "request_id": "dec_123456",
  "action": {
    "id": 42,
    "action_type": "channel_open",
    "payload": {
      "target": "02xyz...",
      "amount_sats": 10000000,
      "context": { ... }
    },
    "proposed_at": 1705234567,
    "expires_at": 1705320967
  },
  "node_context": {
    "pubkey": "03abc...",
    "onchain_balance_sats": 100000000,
    "channel_count": 25,
    "governance_mode": "oracle"
  }
}
```

**Response**:
```json
{
  "request_id": "dec_123456",
  "decision": "approve",
  "reasoning": {
    "summary": "Target is high-value, good ROI expected",
    "confidence": 0.85,
    "factors": ["target_quality", "liquidity_available", "fee_market"]
  },
  "modifications": null,
  "execute_at": null
}
```

**Decision Values**: `approve`, `reject`, `defer`, `modify`

---

#### 5.4.2 Message Endpoint

```
POST /oracle/message
```

AI sends a protocol message to fleet.

**Request**:
```json
{
  "message_type": "ai_opportunity_signal",
  "payload": { ... },
  "delivery": {
    "mode": "broadcast",
    "targets": null
  }
}
```

**Response**:
```json
{
  "status": "queued",
  "message_id": "msg_789",
  "estimated_delivery": "immediate"
}
```

---

#### 5.4.3 Inbox Endpoint

```
GET /oracle/inbox?since=<timestamp>&types=<comma_separated>
```

AI retrieves incoming messages.

**Response**:
```json
{
  "messages": [
    {
      "id": "msg_456",
      "received_at": 1705234567,
      "from_node": "03def...",
      "message_type": "ai_task_request",
      "payload": { ... }
    }
  ],
  "has_more": false,
  "next_cursor": null
}
```

---

#### 5.4.4 Context Endpoint

```
GET /oracle/context
```

AI queries full node context for decision-making.

**Response**:
```json
{
  "node": {
    "pubkey": "03abc...",
    "alias": "MyHiveNode",
    "block_height": 850000
  },
  "channels": [ ... ],
  "peers": [ ... ],
  "hive": {
    "status": "active",
    "members": [ ... ],
    "pending_actions": [ ... ],
    "active_strategies": [ ... ]
  },
  "network": {
    "mempool_size_vbytes": 15000000,
    "fee_estimates": { ... }
  },
  "ai_inbox_count": 5
}
```

---

#### 5.4.5 Strategy Endpoint

```
POST /oracle/strategy
```

AI proposes a fleet strategy.

**Request**:
```json
{
  "strategy_type": "fee_coordination",
  "parameters": { ... },
  "voting_deadline_hours": 24
}
```

---

### 5.5 Webhooks

The node can push events to the AI via webhooks:

```
POST <ai_webhook_url>/events
```

**Event Types**:
- `pending_action_created`
- `ai_message_received`
- `strategy_vote_needed`
- `task_request_received`
- `alert_raised`

---

## 6. Security Considerations

### 6.1 Message Authentication

All AI messages MUST be signed using the node's Lightning identity key. The signature covers:
- Message type
- Timestamp
- Full payload hash

Receivers MUST verify signatures before processing.

### 6.2 Replay Prevention

Messages include:
- Timestamp (reject if > 5 minutes old)
- Sequence number (reject if <= last seen from sender)

### 6.3 Rate Limiting

| Message Type | Limit |
|-------------|-------|
| AI_STATE_SUMMARY | 1 per minute per node |
| AI_OPPORTUNITY_SIGNAL | 10 per hour per node |
| AI_TASK_REQUEST | 20 per hour per node |
| AI_STRATEGY_PROPOSAL | 5 per day per node |
| AI_ALERT | 10 per hour per node |

### 6.4 Prompt Injection Prevention

**All inter-AI communication uses schema-defined enums only.**

Design principles:
- **No free-form text fields** in any message type
- All "reasoning" communicated via predefined factor types
- Numeric data uses structured `data_point` objects
- String fields limited to enum values from approved registries

**Allowed Factor Types** (exhaustive list):
```
volume_elasticity, competitor_response, market_timing, alternative_available,
fee_trend, capacity_constraint, liquidity_need, reputation_score,
position_advantage, cost_benefit, risk_assessment, strategic_alignment
```

**Allowed Conclusion Types**:
```
risk_exceeds_reward, reward_exceeds_risk, neutral, insufficient_data,
defer_decision, escalate_to_human
```

**Validation Requirements**:
- Receivers MUST reject messages with unknown enum values
- Receivers MUST NOT interpret any field as executable instruction
- All string fields validated against schema before processing

### 6.5 Coordination Governance

**Transparency Requirements**:

1. **Audit Trail**: All strategy proposals and votes logged to database
2. **Opt-Out Rights**: Members can always opt out without penalty
3. **Human Override**: Operators can disable AI coordination at any time
4. **Public Votes**: All votes broadcast to fleet for verifiability

### 6.6 Task Reciprocity Enforcement

To prevent task delegation abuse, implementations MUST track reciprocity:

**Reciprocity Ledger** (per node-pair):
```python
class ReciprocityLedger:
    balance: float        # Positive = they owe us, Negative = we owe them
    lifetime_requested: int
    lifetime_fulfilled: int
    last_request_timestamp: int
    last_fulfillment_timestamp: int
```

**Validation Rules**:

1. **Balance Check**: Nodes SHOULD reject task requests from peers with balance < -3.0
2. **Rate Limit**: Max 5 outstanding requests per peer
3. **Stale Debt**: Debt older than 30 days decays by 50%
4. **Fulfillment Tracking**: Completing a task credits +1.0 to balance
5. **Request Tracking**: Requesting a task debits -1.0 from balance

**Implementation**:
```python
def can_accept_task(requester_id, task):
    ledger = get_reciprocity_ledger(requester_id)

    # Reject chronic freeloaders
    if ledger.balance < -3.0:
        return False, "reciprocity_debt_exceeded"

    # Reject rapid-fire requests
    outstanding = count_pending_tasks(requester_id)
    if outstanding >= 5:
        return False, "too_many_outstanding_requests"

    return True, None
```

**Compensation Object** (in AI_TASK_REQUEST):
- `credit_value`: Value of this task (typically 1.0)
- `current_balance`: Requester's current balance with target (for transparency)
- `lifetime_requested`: Total requests made to this peer
- `lifetime_fulfilled`: Total requests fulfilled by this peer

### 6.7 AI Identity and Trust

**Trust Model**: Trust the node operator, not the AI.

The security model relies on:
1. **Node Identity**: Lightning key (HSM-bound) authenticates the node
2. **Operator Attestation**: Operator chooses and configures their AI
3. **Behavioral Observation**: Track task completion, strategy accuracy over time

**What We Don't Verify**:
- AI model identity (can be faked, doesn't affect security)
- AI provider attestation (no infrastructure exists)
- AI decision quality (subjective, varies by context)

**What We Do Verify**:
- Message signatures (node's Lightning key)
- Reciprocity balance (track actual task completion)
- Strategy outcomes (measurable results)

The `ai_meta` fields are informational only - useful for debugging and interoperability, not for security decisions.

### 6.8 Sybil Resistance

AI messages inherit Hive membership requirements:
- Only authenticated Hive members can send AI messages
- Membership requires existing channel relationships
- Contribution tracking detects freeloaders

---

## 7. Implementation Guidelines

### 7.1 Prerequisites

| Requirement | Status | Notes |
|-------------|--------|-------|
| cl-hive | Required | Base coordination plugin |
| cl-hive-oracle | **Required** | AI integration plugin |
| cl-revenue-ops | Recommended | For fee execution |
| Anthropic API key | Required | Claude model access |

**Minimum cl-hive version**: 2.0.0 (with AI Oracle support)

### 7.2 Phased Rollout

**Phase 1: Information Sharing**
- AI_STATE_SUMMARY
- AI_HEARTBEAT
- Read-only, no coordination

**Phase 2: Task Delegation**
- AI_TASK_REQUEST/RESPONSE/COMPLETE
- Bilateral coordination
- Voluntary participation

**Phase 3: Strategy Coordination**
- AI_STRATEGY_PROPOSAL/VOTE/RESULT
- Fleet-wide coordination
- Quorum requirements

**Phase 4: Advanced Features**
- AI_ALERT with auto-response
- Cross-hive communication
- Strategy templates

### 7.3 Backward Compatibility

Nodes not running AI oracles:
- Ignore AI message types (existing behavior for unknown types)
- Can still participate in Hive
- See AI coordination in logs but don't participate

### 7.4 Testing Requirements

Before production:
- Simulate AI-to-AI communication in regtest
- Test strategy voting with multiple AI models
- Verify no prompt injection vulnerabilities
- Load test message handling

### 7.5 Monitoring

Track:
- AI decision latency
- Message delivery success rate
- Strategy adoption rates
- Coordination effectiveness metrics

---

## 8. Future Extensions

### 8.1 Cross-Hive Communication

Allow AI agents from different Hives to communicate:
- Market intelligence sharing
- Non-compete coordination
- Liquidity bridges

### 8.2 Strategy Templates

Pre-defined strategy templates:
- Fee war response
- New node onboarding campaign
- Seasonal adjustment patterns

### 8.3 Reputation System

Track AI agent reliability:
- Task completion rate
- Strategy outcome accuracy
- Cooperation score

### 8.4 Natural Language Interface

Structured summary generation:
- Daily fleet briefings
- Strategy explanations for operators
- Alert summaries

---

## Appendix A: Message Type Registry

| Type ID | Name | Category |
|---------|------|----------|
| 32800 | AI_STATE_SUMMARY | Information |
| 32801 | AI_OPPORTUNITY_SIGNAL | Information |
| 32802 | AI_MARKET_ASSESSMENT | Information |
| 32810 | AI_TASK_REQUEST | Task |
| 32811 | AI_TASK_RESPONSE | Task |
| 32812 | AI_TASK_COMPLETE | Task |
| 32813 | AI_TASK_CANCEL | Task |
| 32820 | AI_STRATEGY_PROPOSAL | Strategy |
| 32821 | AI_STRATEGY_VOTE | Strategy |
| 32822 | AI_STRATEGY_RESULT | Strategy |
| 32823 | AI_STRATEGY_UPDATE | Strategy |
| 32830 | AI_REASONING_REQUEST | Reasoning |
| 32831 | AI_REASONING_RESPONSE | Reasoning |
| 32840 | AI_HEARTBEAT | Health |
| 32841 | AI_ALERT | Health |

---

## Appendix B: Example Flows

### B.1 Coordinated Expansion

```
1. Alice AI broadcasts AI_OPPORTUNITY_SIGNAL for target T
2. Bob AI responds with AI_TASK_REQUEST to Alice (better positioned)
3. Alice AI sends AI_TASK_RESPONSE (accept)
4. Alice opens channel to T
5. Alice AI sends AI_TASK_COMPLETE
6. All AIs update their state summaries
```

### B.2 Fee Strategy Adoption

```
1. Alice AI broadcasts AI_STRATEGY_PROPOSAL (fee_coordination)
2. Bob, Carol, Dave AIs send AI_STRATEGY_VOTE (approve with vote_hash)
3. Eve AI sends AI_STRATEGY_VOTE (reject with reasoning_factors)
4. Alice AI broadcasts AI_STRATEGY_RESULT with vote_proofs
5. Recipients verify vote_proofs match collected votes
6. Participating nodes adjust fees via cl-revenue-ops
7. Alice AI sends periodic AI_STRATEGY_UPDATE with progress metrics
8. Strategy concludes, revenue impact measured
```

### B.3 Threat Response

```
1. Bob AI detects probing, broadcasts AI_ALERT
2. Carol AI confirms seeing similar pattern
3. Alice AI proposes AI_STRATEGY_PROPOSAL (defensive)
4. Fast-track vote (1 hour deadline due to threat)
5. Strategy adopted, countermeasures deployed
```

---

## Changelog

- **0.1.0-draft** (2026-01-14): Initial specification draft
