# Sapling Feature Roadmap

Sapling is the storage engine for Lambkin's substrate tables — the shared-state
layer that threads communicate through. This document tracks features beyond the
current sorted key-value API, ordered by priority for the language runtime.

## Current capabilities (done)

- Copy-on-write B+ tree with MVCC snapshot isolation
- Nested transactions (commit-to-parent, abort-restore)
- Pluggable `PageAllocator` for page storage (Wasm linear-memory friendly)
- Custom key comparator (`keycmp_fn`)
- Entry count tracking + `SapStat` / `db_stat` / `txn_stat`
- Bidirectional cursor (seek, first, last, next, prev)
- Cursor reuse (`cursor_renew`)
- Key-only cursor access (`cursor_get_key`)
- Range counting (`txn_count_range`, exact scan-backed)
- Sorted-load API (`txn_load_sorted`, empty-DBI O(n) fast path)
- Benchmark harness (`make bench-run`)
- Benchmark CI guardrail (`make bench-ci`, baseline-backed)
- Conditional put / CAS (`txn_put_if`)
- Snapshot checkpoint/restore (`db_checkpoint`, `db_restore`)
- Watch notifications (`db_watch`, `db_unwatch`, commit-time delivery)
- DupSort (`DBI_DUPSORT`) with duplicate navigation/count APIs
- Prefix helpers (`cursor_seek_prefix`, `cursor_in_prefix`)
- Thread safety (`SAPLING_THREADED`, TSan-clean)
- Input validation (key/val length bounds)
- Binary-safe keys and values

---

## Priority 1 — Enables basic substrate table patterns (DONE)

### cursor_put / cursor_del (done)
Mutate at cursor position without re-traversing from root. Critical for
scan-and-update patterns ("update all rows where status = pending"). Without
this, each mutation during a scan is O(log n); with it, the scan+mutate is O(n).

```c
int cursor_put(Cursor *cur, const void *val, uint32_t val_len, unsigned flags);
int cursor_del(Cursor *cur);
```

`cursor_put` replaces the value at the current key. `cursor_del` removes the
current entry and advances the cursor to the next position.

### Put flags (NOOVERWRITE, RESERVE) (done)
Distinguish insert-only from upsert, and enable zero-copy writes.

```c
#define SAP_NOOVERWRITE 0x01u  /* fail with SAP_EXISTS if key present   */
#define SAP_RESERVE     0x02u  /* allocate space, return pointer         */
#define SAP_EXISTS       6     /* new error code                         */

int txn_put_flags(Txn *txn,
                  const void *key, uint32_t key_len,
                  const void *val, uint32_t val_len,
                  unsigned flags, void **reserved_out);
```

`SAP_NOOVERWRITE` — the language needs "INSERT and fail if exists" for primary
key enforcement. Currently `txn_put` always upserts silently.

`SAP_RESERVE` — allocate space for the value in the leaf page and return a
writable pointer. The caller writes directly into the page, avoiding a memcpy.
Natural fit for Wasm linear memory where values are fixed-layout structs.

### Multiple named databases (done — integer-indexed)
A single `DB*` with shared page pool hosting multiple logical B+ trees. Each
tree has its own root page and comparator, but they share transactions and
page allocation. This is how substrate tables become a real multi-table database.

```c
typedef uint32_t DBI;  /* database index / handle */

int dbi_open(DB *db, uint32_t dbi, keycmp_fn cmp, void *cmp_ctx, unsigned flags);
int dbi_set_dupsort(DB *db, uint32_t dbi, keycmp_fn vcmp, void *vcmp_ctx);
int dbi_stat(Txn *txn, uint32_t dbi, SapStat *stat);

/* DBI-aware data APIs */
int txn_get_dbi(Txn *txn, uint32_t dbi, const void *key, ...);
int txn_put_dbi(Txn *txn, uint32_t dbi, const void *key, ...);
Cursor *cursor_open_dbi(Txn *txn, uint32_t dbi);
```

Without this, each Lambkin table is an independent `DB*` with its own allocator
and page space, and there's no way to atomically update rows across tables.

`dbi_open`/`dbi_set_dupsort` are metadata operations and require no active
transactions; they return `SAP_BUSY` if readers or a writer are active.

---

## Priority 2 — Enables relational patterns and indexes

### DupSort (sorted duplicate keys)
A single key mapping to multiple sorted values. Essential for secondary indexes
where many rows map to the same index value.

```c
#define DBI_DUPSORT  0x01u  /* flag for dbi_open */

int cursor_next_dup (Cursor *cur);  /* next value for same key  */
int cursor_prev_dup (Cursor *cur);  /* prev value for same key  */
int cursor_first_dup(Cursor *cur);  /* first value for cur key  */
int cursor_last_dup (Cursor *cur);  /* last value for cur key   */
int cursor_count_dup(Cursor *cur, uint64_t *count);
```

Without DupSort, secondary indexes require composite keys
(`index_value || primary_key`), which makes count queries and existence checks
awkward.

### Prefix / range-end helpers
Native support for bounded iteration without manual key comparison in the
caller.

```c
/* Position at first key with given prefix; returns NOTFOUND if none */
int cursor_seek_prefix(Cursor *cur, const void *prefix, uint32_t len);

/* Check if current key starts with prefix (for loop termination) */
int cursor_in_prefix(Cursor *cur, const void *prefix, uint32_t len);
```

Prefix scans are the most common query pattern when composite keys encode
table+column+value. Having these as primitives simplifies the language-generated
code.

---

## Priority 3 — Performance and ergonomics

### Bulk sorted load (initial implementation)
`txn_load_sorted` is available with sorted-input validation and now uses an
O(n) leaf/internal builder when loading into an empty DBI.

For non-empty, non-DUPSORT DBIs in a clean write transaction state, it uses a
merge+rebuild fast path (preserving upsert semantics). Other cases still fall
back to the regular put path to preserve behavior.

```c
int txn_load_sorted(Txn *txn, DBI dbi,
                    const void *const *keys, const uint32_t *key_lens,
                    const void *const *vals, const uint32_t *val_lens,
                    uint32_t count);
```

### cursor_renew (done)
Reuse a cursor struct across multiple seeks without close+open overhead.
Reduces allocation pressure in tight loops.

```c
int cursor_renew(Cursor *cur, Txn *txn);
```

### Key-only cursor mode (done)
Sometimes you just need keys (existence checks, building key sets). Avoiding
value access saves cache lines when values are large.

```c
int cursor_get_key(Cursor *cur,
                   const void **key_out, uint32_t *key_len_out);
```

### Entry count per range (done, exact for now)
`txn_count_range` is available with half-open semantics `[lo, hi)` and currently
uses cursor iteration, so results are exact. This can later be replaced with an
approximate structural estimator if needed for planning-speed tradeoffs.

```c
int txn_count_range(Txn *txn, DBI dbi,
                    const void *lo, uint32_t lo_len,
                    const void *hi, uint32_t hi_len,
                    uint64_t *count_out);
```

---

## Priority 4 — Thread communication primitives

### Watched keys / change notifications (done)
For reactive substrate tables where threads subscribe to changes on specific
keys or prefixes. A writer thread commits, and all watchers get notified with
the new value. This turns Sapling from a passive store into an active
communication channel.

```c
typedef void (*sap_watch_fn)(const void *key, uint32_t key_len,
                             const void *val, uint32_t val_len,
                             void *ctx);

int db_watch  (DB *db, const void *prefix, uint32_t prefix_len,
               sap_watch_fn cb, void *ctx);
int db_unwatch(DB *db, const void *prefix, uint32_t prefix_len,
               sap_watch_fn cb, void *ctx);
int db_watch_dbi  (DB *db, uint32_t dbi,
                   const void *prefix, uint32_t prefix_len,
                   sap_watch_fn cb, void *ctx);
int db_unwatch_dbi(DB *db, uint32_t dbi,
                   const void *prefix, uint32_t prefix_len,
                   sap_watch_fn cb, void *ctx);
```

Notifications fire after top-level `txn_commit` for keys changed in the watched
DBI that match the watched prefix. Child transaction commits merge pending
changes into the parent and do not notify until the outer commit. Callbacks run
synchronously on the committing thread. Registrations affect write
transactions started after the watcher is installed.

`db_watch_dbi`/`db_unwatch_dbi` provide DBI-scoped registrations for non-DUPSORT
DBIs. Duplicate registrations return `SAP_EXISTS`. Register/unregister calls
return `SAP_BUSY` while a write transaction is active.

### Conditional put (compare-and-swap) (done)
Storage-level CAS for lock-free coordination patterns between threads.

```c
int txn_put_if(Txn *txn, DBI dbi,
               const void *key,  uint32_t key_len,
               const void *val,  uint32_t val_len,
               const void *expected_val, uint32_t expected_len);
```

Returns `SAP_OK` if the current value matches `expected_val` and was replaced.
Returns `SAP_CONFLICT` (new error code) if the current value differs. Returns
`SAP_NOTFOUND` if the key doesn't exist.

Current implementation targets non-DUPSORT DBIs (returns `SAP_ERROR` for
DUPSORT because equality semantics are ambiguous across duplicate sets).

### Snapshot serialization / checkpointing (done)
Serialize the current committed state to a byte buffer (or write callback) for
persistence, migration, debugging, or network transfer. The meta-page
infrastructure already exists — this adds the I/O layer.

```c
typedef int (*sap_write_fn)(const void *buf, uint32_t len, void *ctx);
typedef int (*sap_read_fn)(void *buf, uint32_t len, void *ctx);

int db_checkpoint(DB *db, sap_write_fn writer, void *ctx);
int db_restore   (DB *db, sap_read_fn reader, void *ctx);
```

---

## Priority 5 — C Host Runner for Wasm (next major milestone)

### Decision (locked)
The host runner will be implemented in C (pthread-based), with possible tight
coupling to Sapling internals when justified by performance or simpler control
flow.

Post-WIT roadmap update (current execution order):
1. Runner correctness core first:
   atomic-block semantics, retry behavior, and nested-atomic correctness are
   the immediate critical path.
2. Serialization and schema evolution policy is a gate:
   freeze a v0 message/intent encoding and compatibility rules before broad
   runner feature expansion.
3. Tooling expansion is parallel/background:
   widen lint/static-analysis scope incrementally without blocking core runner
   correctness milestones.
4. Optimization and internal coupling are deferred:
   profile-guided tight coupling happens only after correctness + observability
   baselines are in place.
5. Component-model runtime path is deferred:
   WIT remains source-of-truth for schema/codegen, but runtime execution does
   not depend on component-model machinery.

Worker model:
- one OS thread per worker
- one Wasm instance/Store per worker
- independent Wasm linear memory per worker (no shared Wasm memory)
- symmetric DB access for all workers through Sapling

Cross-worker coordination is restricted to:
1. serialized messages (Canonical ABI-compatible payloads)
2. database state and queues

No shared mutable memory is used between workers.

### Transaction contract for language atomic blocks
Atomic blocks in guest code are treated as side-effect-free transactional logic:
- retries may be automatic on retryable conflicts
- nested atomic blocks are closed-nested (child merges into parent context)
- only outermost successful commit makes changes durable

Inside an atomic block:
- allowed: transactional reads/writes and message/timer intent creation
- forbidden: external irreversible effects (I/O, network sends, host side effects)

This contract is what makes automatic retry sound.

### Execution model (host-side OCC around Sapling)
To avoid long writer hold times, Wasm must not execute while a top-level Sapling
write txn is open.

Per-attempt flow:
1. open read snapshot
2. execute Wasm atomic block against a host tx context (`read_set`, `write_set`,
   buffered intents)
3. close read snapshot
4. open short write txn, validate/apply, commit
5. publish buffered intents only after successful commit

Retry policy:
- retryable: `SAP_BUSY`, `SAP_CONFLICT`, lease-race style transient failures
- non-retryable: deterministic logic/type errors, traps, validation bugs
- bounded retries with jittered backoff and metrics

### Initial DBI layout for runner coordination
Proposed baseline DBIs:
- DBI 0: application state (default)
- DBI 1: inbox (`(worker_id, seq)` -> message)
- DBI 2: outbox (`(seq)` -> message/event)
- DBI 3: leases (`msg_id` -> owner/deadline/attempts)
- DBI 4: timers (`(due_ts, msg_id)` -> payload)
- DBI 5: dedupe/idempotency keys

Watcher usage:
- `db_watch_dbi` on inbox/timer prefixes for wake-up hints
- DB remains source of truth; watchers are only low-latency nudges

### Repository restructuring plan (approved direction)
The previous "all C files at top level" layout should be retired in favor of a
structure that scales with runner complexity and reliability requirements.

Proposed target layout:
- `include/sapling/`: public headers (stable API surface)
- `src/sapling/`: storage engine implementation
- `src/runner/`: C host runner (threads, scheduler, retries, message loop)
- `src/wasi/`: Wasm host glue and component call adapters
- `src/common/`: reusable utilities (buffers, clocks, IDs, hashing, metrics)
- `examples/`: native + wasm smoke/integration examples
- `tests/unit/`: deterministic unit tests by subsystem
- `tests/integration/`: multi-worker end-to-end scenarios
- `tests/stress/`: long-running contention/retry/fault tests
- `tools/`: schema inspectors, log/replay helpers, migration tools
- `docs/`: architecture, protocol, and ops documentation

Migration strategy:
- keep top-level compatibility for one transition window
- move code in slices (engine first, then runner, then tests/tools)
- preserve Make targets while introducing internal path changes

### Required tooling and components
To keep a C-based system reliable, these components are required (not optional):

Build and toolchain:
- pinned C compiler matrix (clang primary, gcc secondary)
- pinned WASI SDK and `wasm-tools` versions in CI
- reproducible build mode (`-fno-omit-frame-pointer`, deterministic flags)

Code quality:
- formatter (`clang-format`) with enforced style check target
- static analysis (`clang-tidy` and `cppcheck`) in CI
- warnings-as-errors across all targets

Runtime correctness:
- sanitizer matrix: ASan/UBSan/TSan (already in place, must remain gating)
- leak checks in dedicated target/job
- optional `-fsanitize=pointer-compare,pointer-subtract` hardening passes

Concurrency and durability testing:
- deterministic retry/conflict harness for atomic-block runtime
- fault injection hooks (allocation failure, forced `SAP_BUSY`, crash-at-step)
- crash-recovery test suite around checkpoint/restore and message outbox replay

Protocol and schema discipline:
- message envelope version field + schema compatibility tests
- WIT schema package as source of truth (`schemas/wit/runtime-schema.wit`)
- generated DBI schema manifest (name, key layout, value layout, migration notes)
- explicit migration tooling for DBI additions/format changes

Operational visibility:
- structured logs (attempt id, worker id, msg id, retry cause, commit latency)
- metrics export hooks (conflicts, retries, queue depth, lease timeout counts)
- trace IDs propagated through inbox -> execution -> outbox

### Preemptive issues and controls
Likely future failure modes and planned controls:
- writer contention spikes:
  enforce short write txns; alert on commit latency and retry-rate thresholds
- duplicate message delivery during retries:
  idempotency DBI + outbox pattern + dedupe keys
- schema drift across components:
  versioned message envelopes and migration tests as CI gate
- hidden coupling between runner and DB internals:
  explicit internal API boundary and "public API only" fallback mode
- nondeterministic race bugs:
  deterministic stress harness and replayable event logs
- memory ownership regressions in C:
  allocator wrappers with ownership annotations + sanitizer + fault-injection

### Implementation phases

#### Phase 0 — Repository and tooling foundation
- migrate to target directory structure with compatibility shims
- add formatting, static analysis, and schema-manifest checks
- add deterministic/fault-injection harness scaffolding

Phase 0 status (initiated):
- done: engine moved to `include/sapling` + `src/sapling` with top-level shims
- done: initial layout skeleton (`src/common`, `src/runner`, `src/wasi`,
  `tests/{unit,integration,stress}`, `examples`, `tools`, `schemas`, `docs`)
- done: WIT-first schema pipeline (`wit-schema-check`, `wit-schema-generate`,
  `wit-schema-cc-check`, generated `schemas/dbi_manifest.csv`, generated C DBI metadata)
- done: deterministic fault-injection harness scaffold
- in progress: expand lint/static-analysis scope from phase-0 files to entire
  codebase once legacy formatting debt is paid down

#### Phase A — Runner skeleton + contracts
- C host process with worker threads and Wasm instance lifecycle
- v0 message/intent serialization format and compatibility contract
- dispatch loop with explicit schema/version guardrails
- DBI bootstrap and schema/version guard

Phase A status (started):
- done: frozen v0 wire contract module (`src/runner/wire_v0.h`,
  `src/runner/wire_v0.c`) with strict encode/decode validation and unit tests
  (`tests/unit/runner_wire_test.c`)
- done: worker lifecycle scaffold (`src/runner/runner_v0.h`,
  `src/runner/runner_v0.c`) with DBI bootstrap from generated WIT metadata,
  schema-version guard key, and step-dispatch callback integration
  (`tests/unit/runner_lifecycle_test.c`)
- done: step dispatch connected to DB-backed inbox cursor loop (DBI 1 inbox,
  worker-prefixed key scan, callback dispatch, post-dispatch delete)
- done: worker shell around lifecycle state machine (`SapRunnerV0Worker` with
  tick/stop APIs and optional pthread start/join under `SAPLING_THREADED`)
- next: wire worker shell to Wasm invocation shim in `src/wasi`

#### Phase B — Atomic runtime
- host tx context (`read_set`/`write_set`/intent buffer)
- nested atomic context stack (closed nesting/savepoints)
- commit/abort/retry engine with bounded policy
- deterministic integration tests for conflict retry + nested rollback/commit

#### Phase C — Mailbox, leases, timers
- claim/ack/requeue flows with CAS guards
- outbox dispatch after commit
- timer ingestion and due-time wake scheduling

#### Phase D — Reliability and observability
- deterministic replay hooks (optional)
- conflict/retry/latency counters
- crash recovery checks using checkpoint/restore

#### Phase E — Optimization (optional tight coupling)
- profile-guided coupling points with Sapling internals
- keep public API path as default correctness baseline
- explicitly non-blocking for runner functional bring-up

#### Phase F — Packaging and operational readiness
- stable config surface (worker counts, retry policy, lease durations)
- deployment/runbook docs and failure-mode playbooks
- release checklist with compatibility and migration verification

---

## Priority 6 — Advanced / future

### Overflow pages for large values
Currently, key+value must fit in a single page (minus header+slot overhead).
With 4KB pages and 10-byte leaf header, the practical limit is ~4080 bytes.
Overflow pages would chain multiple pages for values exceeding this limit.

This is architecturally significant (changes how `txn_get` returns pointers,
affects COW granularity, complicates split logic). Worth deferring until the
language layer actually needs values > 4KB.

### Value-level comparator (for DupSort)
When DupSort is enabled, a separate comparator can define value ordering within
each key. LMDB calls this `mdb_set_dupsort`.

```c
int dbi_set_dupsort(DB *db, uint32_t dbi, keycmp_fn vcmp, void *vcmp_ctx);
```

### TTL / automatic expiry
Entries that expire after a duration. Useful for caches, sessions, and
rate-limiting counters. Would require a background sweep or lazy deletion
during cursor traversal.

### Range delete
Delete all entries in a key range without scanning entry-by-entry. Can be
implemented efficiently by unlinking entire subtrees when they fall within the
range.

```c
int txn_del_range(Txn *txn, DBI dbi,
                  const void *lo, uint32_t lo_len,
                  const void *hi, uint32_t hi_len,
                  uint64_t *deleted_count);
```

### Merge operator
Apply a transformation to an existing value without read-modify-write. Useful
for counters, append-only lists, and accumulation patterns.

```c
typedef void (*sap_merge_fn)(const void *old_val, uint32_t old_len,
                             const void *operand,  uint32_t op_len,
                             void *new_val,        uint32_t *new_len,
                             void *ctx);

int txn_merge(Txn *txn, DBI dbi,
              const void *key, uint32_t key_len,
              const void *operand, uint32_t op_len,
              sap_merge_fn merge, void *ctx);
```
