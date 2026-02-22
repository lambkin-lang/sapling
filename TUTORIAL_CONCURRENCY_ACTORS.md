# STM, OCC, and Actor Tutorial for Sapling

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

## Wasm, WIT, and the Lambkin Actor Model

Sapling serves as the semantic foundation for **Lambkin**, a Wasm-aligned programming language built on top of the Actor model. This integration involves several key concepts:

### Semantic Foundation
- **WebAssembly (Wasm) Linear Memory**: Wasm's single linear memory model is fundamental. Workers share *no memory* other than what is mediated through Sapling (the database and message queues).
- **WIT IDL Models**: Schema layouts, database records, and the internal structures are robustly defined using WebAssembly Interface Types (WIT). The WIT file acts as the ultimate ground truth for both the C host engine and the Wasm guest modules.
- **WASI Standards**: The messaging layers (like Phase C's mailbox, outbox, and timers) align conceptually with the Bytecode Alliance's `wasi-messaging` specifications. Sapling's underlying storage abstractions map cleanly to `wasi-keyvalue`.

### The Lambkin Language Experience
Lambkin programmers do not explicitly deal with a traditional heap or raw pointers. Instead:
- State is referenced and passed around using **paths and database cursors**.
- Business logic is written inside `atomic { ... }` blocks which translate seamlessly to Sapling's nested transactions.
- The language provides **extensive static checking** to guarantee that the logic inside an `atomic` block is strictly side-effect free.
- The closest construct to a heap allocation is a lambda closure. However, closures in Lambkin are thread-confined and execute dynamically; they are not "storable" types, preserving the capability to cleanly rollback state during a transaction abort without memory leaks.

### Virtual Actors in Sapling
Because threads share no memory and all communication occurs through records and queues, each Wasm module instance functions essentially as a **Virtual Actor**.
- An Actor wakes up when a message arrives via a mailbox or timer intent.
- It deterministically processes the message within an optimistic transaction.
- It atomically commits state changes to the database AND zero or more new messages to an outbox.
- If contention occurs (`SAP_BUSY` or `SAP_CONFLICT`), the statically side-effect free actor logic is cleanly rolled back and replayed against fresh database state.

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

- Actor: An isolated, concurrent computational entity that communicates strictly via message passing. In Sapling, a Wasm worker.
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
- WASI: WebAssembly System Interface. In Sapling, specifically relates to semantic mappings for `wasi-keyvalue` and `wasi-messaging`.
- Watch: callback registration for commit-time notifications on DBI/prefix.
- WIT: WebAssembly Interface Types. The Interface Description Language used to contractually define runtime data structures.
- Write set: buffered intended mutations for commit.

## Suggested next reading

- `README.md` for build/runtime constraints
- `FEATURES.md` for implemented and planned features
- `tests/unit/test_sapling.c` sections:
  - nested transaction tests
  - `txn_put_if` tests
  - watch notification and watch API hardening tests
