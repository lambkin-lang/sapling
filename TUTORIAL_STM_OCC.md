# STM/OCC Tutorial for Sapling

This guide explains the concurrency terms used in this project and maps them
to concrete Sapling APIs so readers can reason about correctness and
performance.

## Why this document exists

Sapling provides:
- MVCC snapshots
- nested transactions
- compare-and-swap (CAS)
- commit-time watch notifications

Those are building blocks for higher-level transactional systems (including
Wasm host runners using message passing and DB-backed coordination).

This tutorial focuses on the fundamentals: STM/OCC mental models, conflict
handling, retries, and where CAS fits.

## Mental model: what Sapling guarantees

1. Read transactions (`TXN_RDONLY`) see a stable snapshot.
2. Only one top-level write transaction can be active at a time.
3. Nested write transactions are "commit to parent" and become durable only
   when the outermost transaction commits.
4. Writes are atomic at commit: readers see before or after, not partial state.
5. Watch callbacks run after top-level commit for matching watched DBI/prefix.

## STM vs OCC (and where Sapling fits)

### STM (Software Transactional Memory)

STM is a programming model where code runs inside `atomic { ... }` blocks.
The runtime tracks reads/writes and retries if a conflict is detected.

Key idea: developers write logic as if it is isolated; runtime provides retry
and commit behavior.

### OCC (Optimistic Concurrency Control)

OCC assumes conflicts are uncommon:
1. Execute optimistically.
2. Validate near commit.
3. If conflict: abort and retry.

OCC is usually cheaper than lock-heavy pessimistic designs when contention is
low to moderate.

### Sapling in practice

Sapling itself serializes top-level writers, so host-side systems usually apply
OCC around Sapling operations when coordinating multiple worker threads:
- read/compute optimistically
- do short write transactions
- retry on transient conflicts (`SAP_BUSY`) or logical conflicts (for CAS-like
  patterns)

## CAS and related conflict patterns

### CAS (Compare-And-Swap)

CAS means: "write new value only if current value still equals expected value."

Sapling API:
```c
int txn_put_if(Txn *txn, uint32_t dbi,
               const void *key, uint32_t key_len,
               const void *val, uint32_t val_len,
               const void *expected_val, uint32_t expected_len);
```

Important return codes:
- `SAP_OK`: update applied
- `SAP_CONFLICT`: expected value mismatch
- `SAP_NOTFOUND`: key missing

Use CAS for:
- leases
- state transitions (e.g. `pending -> running`)
- optimistic guards in retry loops

### Write-write conflict vs logical conflict

- Write-write conflict: two writers contend for the writer slot (`SAP_BUSY`).
- Logical conflict: business precondition changed (CAS mismatch).

Both can trigger retry, but logical conflicts may require re-reading and
replanning rather than blind retry.

## Nested transactions in Sapling

Nested transaction behavior:
1. Child begins with parent's working view.
2. Child commit merges changes into parent.
3. Child abort discards child-only changes.
4. Nothing is durable until outermost commit.

This maps well to composable "atomic subroutines" where inner operations can
fail without forcing whole-process failure.

## Designing host-level atomic blocks for Wasm workers

For a host runner with system threads and single-threaded Wasm guests:

1. Use message passing and DB records as the only communication channels.
2. Keep Wasm execution side-effect free inside conceptual atomic blocks.
3. Materialize side effects only after successful commit.

Recommended flow:
1. Start attempt.
2. Read state (snapshot or key reads).
3. Run guest logic and build intended writes/messages.
4. Open short write txn and apply writes with CAS/guards.
5. Commit, or retry on conflict.

Avoid long write transactions around guest execution.

## Retry policy guidelines

Good automatic retry cases:
- `SAP_BUSY`
- `SAP_CONFLICT` in known idempotent transition loops
- transient lease-race failures

Do not blindly retry:
- invalid input / schema errors
- deterministic logic bugs
- repeated high-contention hotspots without backoff

Use:
- bounded retries
- jittered backoff
- conflict metrics
- dead-letter/fallback path after max attempts

## Watch notifications and coordination

Watch APIs:
```c
int db_watch_dbi(DB *db, uint32_t dbi,
                 const void *prefix, uint32_t prefix_len,
                 sap_watch_fn cb, void *ctx);

int db_unwatch_dbi(DB *db, uint32_t dbi,
                   const void *prefix, uint32_t prefix_len,
                   sap_watch_fn cb, void *ctx);
```

Current behavior highlights:
- DBI-scoped and prefix-scoped.
- Triggered after top-level commit.
- Duplicate registration returns `SAP_EXISTS`.
- Register/unregister while a write txn is active returns `SAP_BUSY`.

Use watches as wake-up hints, not as the sole source of truth. The DB remains
the source of truth.

## Practical patterns

### Pattern: lease claim

1. Read task row.
2. CAS state from `queued` to `leased(worker, deadline)`.
3. On `SAP_CONFLICT`, retry with backoff.

### Pattern: outbox commit

1. Execute logic and prepare outputs.
2. In one write txn:
   - update state
   - append outbox messages
3. Commit.
4. Dispatcher reads outbox and delivers.

This prevents "state changed but message lost" inconsistencies.

### Pattern: nested helper transactions

When writing reusable library code:
- helpers may use nested txns to isolate partial changes
- helpers should not assume durability at child commit

## Common mistakes

1. Running heavy compute while holding an open write txn.
2. Assuming child commit is durable.
3. Treating watch callbacks as guaranteed delivery queues.
4. Retrying non-idempotent side effects.
5. No backoff under contention.

## Glossary

- Atomicity: all-or-nothing effect of a transaction.
- CAS (Compare-And-Swap): conditional update based on expected current value.
- Commit: finalize transaction changes.
- Conflict: operation cannot proceed due contention or failed precondition.
- DBI: logical named/indexed database inside one `DB*`.
- Idempotent: repeated execution yields the same result.
- Isolation: concurrent operations do not observe partial intermediate state.
- Lease: temporary claim on work/state with owner and expiry.
- MVCC: Multi-Version Concurrency Control; readers see snapshots while writes
  proceed.
- OCC: Optimistic Concurrency Control (validate and retry on conflict).
- Outbox pattern: persist messages/events in DB transaction, dispatch later.
- Read set: keys/values observed during optimistic execution.
- Retry loop: re-run transaction attempt after retryable conflict.
- Snapshot: consistent read view at a specific transaction point.
- STM: Software Transactional Memory programming model (`atomic {}` semantics).
- Top-level transaction: transaction with no parent.
- Nested transaction: child transaction scoped under a parent transaction.
- Watch: callback registration for commit-time notifications on DBI/prefix.
- Write set: buffered intended mutations for commit.

## Suggested next reading

- `README.md` for build/runtime constraints
- `FEATURES.md` for implemented and planned features
- `tests/unit/test_sapling.c` sections:
  - nested transaction tests
  - `txn_put_if` tests
  - watch notification and watch API hardening tests
