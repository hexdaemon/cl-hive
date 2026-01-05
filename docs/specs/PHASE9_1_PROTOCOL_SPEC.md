# Phase 9.1 Spec: The Nervous System (Protocol & Auth)

## 1. Transport Layer
All Hive communication occurs over **BOLT 8** (Encrypted Lightning Connection).
*   **Mechanism:** `sendcustommsg` RPC.
*   **Message ID Range:** `32769` - `33000` (Odd numbers to allow ignoring by non-Hive peers).

### 1.1 Wire Format

To mitigate the risk of message ID collisions in the experimental range (`32768+`), all cl-hive custom messages MUST use a **4-byte Magic Prefix**.

#### Structure
```
┌────────────────────┬────────────────────────────────────┐
│  Magic Bytes (4)   │           Payload (N)              │
├────────────────────┼────────────────────────────────────┤
│     0x48495645     │  [Message-Type-Specific Content]   │
│     ("HIVE")       │                                    │
└────────────────────┴────────────────────────────────────┘
```

#### Magic Bytes Specification
| Byte | Hex Value | ASCII |
|------|-----------|-------|
| 0    | `0x48`    | 'H'   |
| 1    | `0x49`    | 'I'   |
| 2    | `0x56`    | 'V'   |
| 3    | `0x45`    | 'E'   |

**Full Magic:** `0x48495645`

#### Receiver Behavior (MANDATORY)

When processing incoming `custommsg` events, the cl-hive plugin MUST:

1.  **Peek:** Read the first 4 bytes of the payload.
2.  **Check:** Compare against `0x48495645`.
3.  **Accept:** If magic matches, strip the prefix and process the remaining payload.
4.  **Pass-Through:** If magic does NOT match, return `{"result": "continue"}` to allow other plugins to handle the message.

This ensures cl-hive coexists peacefully with other plugins using the experimental message range.

## 2. Authentication: PKI & Manifests
To prevent shared-secret fragility, The Hive uses **Signed Manifests**.

### 2.1 The Invitation (Ticket)
An Admin Node generates a signed blob.
*   **Command:** `revenue-hive-invite --valid-hours=24 --req-splice`
*   **Payload:** `[Admin_Pubkey + Requirements_Bitmask + Expiration_Timestamp + Admin_Signature]`

### 2.2 The Handshake Flow
When Candidate (A) connects to Member (B):

1.  **A -> B (`HIVE_HELLO`):** Sends the **Ticket**.
2.  **B -> A (`HIVE_CHALLENGE`):** Sends a random 32-byte `Nonce`.
3.  **A -> B (`HIVE_ATTEST`):** Sends a **Signed Manifest**:
    ```json
    {
      "pubkey": "Node_A_Key",
      "version": "cl-revenue-ops v1.4.2",
      "features": ["splice", "dual-fund"],
      "nonce_reply": "signed_nonce"
    }
    ```
4.  **B (Verification):**
    *   Checks Ticket validity (Admin Sig + Expiry).
    *   Checks Manifest Signature (Identity Proof).
    *   **Active Probe:** B attempts a harmless technical negotiation (e.g., `splice_init`) to verify A actually supports the claimed features.
5.  **B -> A (`HIVE_WELCOME`):** Session established.

## 3. Message Types
| ID | Name | Payload |
| :--- | :--- | :--- |
| 32769 | `HIVE_HELLO` | Ticket |
| 32771 | `HIVE_CHALLENGE` | Nonce |
| 32773 | `HIVE_ATTEST` | Manifest + Sig |
| 32775 | `HIVE_WELCOME` | HiveID |
| 32777 | `HIVE_GOSSIP` | State Update (See 9.2) |
| 32779 | `HIVE_INTENT` | Lock Request (See 9.2) |
