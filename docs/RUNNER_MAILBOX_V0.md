# Runner Mailbox v0

This document defines the initial Phase-C mailbox lease module:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/mailbox_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/mailbox_v0.c`

Scope:
- claim inbox entries via DBI leases
- acknowledge claimed entries (delete inbox + lease atomically)
- requeue claimed entries (move inbox seq + clear lease atomically)

## Data model

Lease key:
- same bytes as inbox key (`[worker_id:u64be][seq:u64be]`)

Lease value:
- magic `LSE0`
- `owner_worker` (`u64`)
- `deadline_ts` (`s64`)
- `attempts` (`u32`)

## Claim behavior

`sap_runner_mailbox_v0_claim(...)`:
1. verifies inbox entry exists
2. if no lease: inserts new lease (`attempts=1`)
3. if lease exists and not expired (`now_ts <= deadline_ts`): returns `SAP_BUSY`
4. if lease exists and expired: CAS-replaces lease with incremented attempts

## Ack behavior

`sap_runner_mailbox_v0_ack(...)`:
- requires exact expected lease token match (owner/deadline/attempts)
- atomically deletes inbox entry and lease record

## Requeue behavior

`sap_runner_mailbox_v0_requeue(...)`:
- requires exact expected lease token match
- copies existing frame to new inbox sequence (NOOVERWRITE)
- deletes old inbox entry and old lease record

## Status

This is a Phase-C scaffold focused on deterministic state transitions and CAS
guards. Lease timeouts, timer wakeups, and dispatcher wiring are next steps.
