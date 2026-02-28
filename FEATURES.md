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
- Range delete (`txn_del_range`, exact scan-backed)
- Merge helper (`txn_merge`, callback-defined)
- Overflow value storage (non-DUPSORT, chained overflow pages)
- TTL helper APIs (`txn_put_ttl_dbi`, `txn_get_ttl_dbi`, `txn_sweep_ttl_dbi`)
- Sorted-load API (`txn_load_sorted`, empty-DBI O(n) fast path)
- Benchmark harness (`make bench-run`)
- Benchmark CI guardrail (`make bench-ci`, baseline-backed)
- Conditional put / CAS (`txn_put_if`)
- Snapshot checkpoint/restore (`db_checkpoint`, `db_restore`)
- Watch notifications (`db_watch`, `db_unwatch`, commit-time delivery)
- DupSort (`DBI_DUPSORT`) with duplicate navigation/count APIs
- DupSort value comparator configuration (`dbi_set_dupsort`)
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
Current `cursor_put` contract is `flags == 0` on non-DUPSORT DBIs.
When a replacement would require overflow that cannot fit the key shape,
`cursor_put` returns `SAP_FULL` without dropping the existing row.

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

### DupSort (sorted duplicate keys) (done)
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

### Prefix / range-end helpers (done)
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

### Bulk sorted load (done)
`txn_load_sorted` is complete. For loading into an empty DBI, it uses an
`O(n)` leaf/internal builder.

For non-empty, non-DUPSORT DBIs in a clean write transaction state, it uses a
merge+rebuild fast path (preserving upsert semantics). 

**Discrepancy / Remaining Optimization:** `txn_load_sorted` on a `DUPSORT` database currently falls back to a loop of standard `O(log n)` inserts rather than the optimized `O(n)` tree-builder. Addressing this is a future optimization.

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

### Entry count per range (done)
`txn_count_range` is complete with half-open semantics `[lo, hi)`.

**Discrepancy / Remaining Optimization:** It currently uses cursor iteration, so results are exact but O(k) for k elements. This can later be replaced with an approximate structural estimator if `O(1)` counting is needed for query planning-speed tradeoffs.

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

No shared mutable memory is used between workers, with the MVCC B+ Tree acting as the sole safe state-sharing channel.

### Threaded Host Wasm-Modeling and Memory Ownership
The threaded host serves as the foundation for modeling Wasm-systems natively. For performance, passing large structures (e.g., large `text` blocks) between workers via deep-copy messaging is prohibitive.
- **Ownership Transfer Mechanism**: A protocol must go into place to cheaply transfer memory ownership of isolated structures (like `text` buffers) between threads without copying.
- **Z3 Static Analysis**: The Lambkin compiler leverages a Z3 solver during its flow analysis. Since it can statically prove that references don't escape or are uniquely passed, we can safely allow this ownership transfer at the C/Host level without expensive runtime locking or garbage collection. The Wasm-like semantics of the host provide strong runtime sandbox guarantees, backed by the compiler's strict proofs.

### Transaction contract for language atomic blocks
Atomic blocks in guest code are treated as side-effect-free transactional logic:
- retries may be automatic on retryable conflicts
- nested atomic blocks are closed-nested (child merges into parent context)
- only outermost successful commit makes changes durable
- Host API for guests to interact with Sapling DBIs within an atomic block (done)

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
- DBI 5: dedupe/idempotency + retry budget counters
- DBI 6: dead-letter (`(worker_id, seq)` -> failed frame + failure metadata)

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
- done: expand lint/static-analysis scope from phase-0 files to entire
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
- done: worker shell wired to initial Wasm invocation shim in `src/wasi`
  (`shim_v0` callback contract + integration tests)
- done: callback stub replaced with concrete runtime-backed guest invocation
  (`runtime_v0` + shim integration tests)
- done: initial Phase B host tx context (`src/runner/txctx_v0`) with read-set
  tracking, write-set staging, intent buffering, and validate/apply helpers
  (`tests/unit/runner_txctx_test.c`)
- done: nested atomic context stack scaffold (`src/runner/txstack_v0`) with
  closed-nesting push/commit/abort flow and root commit guards
  (`tests/unit/runner_txstack_test.c`)
- done: bounded retry attempt engine scaffold (`src/runner/attempt_v0`) around
  snapshot execution, root validate/apply, and post-commit intent sink
  (`tests/unit/runner_attempt_test.c`)
- done: deterministic integration coverage for conflict-retry + nested
  rollback/commit (`tests/integration/runner_atomic_integration_test.c`)
- done: initial Phase C mailbox lease claim/ack/requeue scaffold
  (`src/runner/mailbox_v0` + `tests/unit/runner_mailbox_test.c`)
- done: initial outbox append/drain + committed-intent publisher adapter
  (`src/runner/outbox_v0` + `tests/unit/runner_outbox_test.c`)
- done: initial timer ingestion and due-drain path (`src/runner/timer_v0` +
  `tests/unit/runner_timer_test.c`)
- done: timer scheduling helper scaffold (`src/runner/scheduler_v0`) for
  next-due lookup + bounded sleep-budget calculation
- done: worker shell now drains due timers in `worker_tick` and uses scheduler
  helper for timer-aware idle sleep budgeting in threaded loop
- done: composed attempt intent sink (`src/runner/intent_sink_v0`) routes both
  `OUTBOX_EMIT` and `TIMER_ARM` through one callback path
- done: lease-aware inbox handling now requeues claimed messages on handler
  failure, clears lease state, and keeps retryable failures in-band
- done: retry-budget/dead-letter routing scaffold
  (`src/runner/dead_letter_v0`) with DBI 5 retry counters and DBI 6 dead-letter
  records for exhausted retryable failures or invalid frames
- done: WASI shim handler now executes through `attempt_v0` and publishes
  committed intents via composed sink (`intent_sink_v0`)
- done: generic attempt-backed runner handler contract exposed in
  `src/runner/attempt_handler_v0` for non-WASI integrations
- done: non-WASI example runner path wired through `attempt_handler_v0`
  (`examples/native/runner_native_example.c`)
- done: dead-letter replay/drain tooling path via `dead_letter_v0` APIs
  (`sap_runner_dead_letter_v0_drain`, `sap_runner_dead_letter_v0_replay`) with
  expanded unit coverage and operational policy docs
- done: runner reliability counters for conflict/busy/non-retryable failures,
  requeues/dead-letter moves, and step latency aggregation
- done: runner checkpoint/restore recovery integration coverage for inbox and
  dead-letter continuity (`runner_recovery_integration_test.c`)
- done: optional deterministic replay hook surface on `runner_v0` for
  inbox/timer/disposition event capture during debugging
- done: stable runner policy/config surface for lease/requeue/retry-budget
  tuning (`SapRunnerV0Policy` + setter APIs)
- done: threaded worker stop/join signaling is now synchronized; worker loop
  no longer exits on transient `SAP_BUSY`; threaded regression coverage added
- done: worker clock hooks now drive inbox lease-claim timing and timer/inbox
  step-latency measurements (not only scheduler idle-sleep decisions)
- next: execute reprioritized backlog below (starting with future Priority 6
  items based on product demand)

#### Phase B — Atomic runtime
- host tx context (`read_set`/`write_set`/intent buffer)
- nested atomic context stack (closed nesting/savepoints)
- commit/abort/retry engine with bounded policy
- deterministic integration tests for conflict retry + nested rollback/commit

Phase B status (started):
- done: `txctx_v0` scaffolding for read-set capture, write-set coalescing,
  intent frame buffering, read validation, and staged write apply
- done: `txstack_v0` nested frame push/commit/abort with child->parent merge
  semantics and nested read-your-write behavior across frame depth
- done: `attempt_v0` bounded retry loop (`SAP_BUSY`/`SAP_CONFLICT`) with
  backoff policy hooks and run stats
- done: deterministic integration test that exercises conflict injection,
  bounded retry, child commit/abort merge semantics, and post-commit intents
- done: Phase C mailbox lease claim/ack/requeue scaffolding with CAS-style
  lease token guards and takeover-on-expiry behavior
- done: Phase C outbox append/drain callback path and attempt intent-sink
  publisher integration for `OUTBOX_EMIT`
- done: Phase C timer append/due-drain path and attempt intent-sink publisher
  integration for `TIMER_ARM`
- done: Phase C scheduler helper for next-due timer lookup and bounded sleep
  budget decisions
- done: timer-aware wake/sleep integrated into worker thread loop and due-timer
  dispatch integrated into worker tick path
- done: composed intent sink for outbox+timer publication
- done: mailbox lease claim/ack/requeue integrated into inbox polling with
  retry-aware requeue semantics
- done: mailbox-claimed message execution through `attempt_v0` in live WASI
  worker flow with retry stats captured in shim state
- done: retry-budget/dead-letter policy for repeatedly failing retryable
  messages in runner inbox polling path
- done: generic runner-level attempt handler contract (beyond WASI shim) via
  `attempt_handler_v0`
- done: additional runner integration path via non-WASI native example
  (`examples/native/runner_native_example.c`)
- done: dead-letter drain/replay tooling API and policy docs in Phase C
- done: initial Phase D reliability counters surfaced on `SapRunnerV0`
- done: crash-recovery checks around checkpoint/restore for runner flows
- done: deterministic replay hooks (optional) for postmortem debugging
- done: stable runner config knobs for worker retry/lease behavior
- done: threaded worker stop/join signaling is now synchronized; worker loop
  now treats transient `SAP_BUSY` as retryable
- done: worker clock hooks now cover lease timing + latency measurement paths
- next: execute reprioritized backlog below (starting with future Priority 6
  items based on product demand)

#### Phase C — Mailbox, leases, timers
- claim/ack/requeue flows with CAS guards
- outbox dispatch after commit
- timer ingestion and due-time wake scheduling

Phase C status (started):
- done: mailbox lease key/value encoding and DBI 3 claim/ack/requeue APIs
  with exact lease token guards
- done: DBI 2 outbox append/drain API with conflict-safe delete-if-match and
  attempt-intent publisher adapter
- done: DBI 4 timer append/due-drain API with due-time key ordering and
  attempt-intent publisher adapter
- done: scheduler helper to derive earliest due timestamp and bounded idle
  sleep duration
- done: worker-loop integration for timer-aware wake/sleep decisions and due
  timer dispatch
- done: composed intent-sink adapter for mixed outbox+timer intent streams
- done: lease-aware worker coordination for retryable and fatal callback errors
  now preserves message durability while clearing stale lease records
- done: retry-budget counter tracking in DBI 5 and dead-letter diversion to DBI 6
  for exhausted retryable failures
- done: dead-letter replay/drain helpers for operational tooling plus
  replay-policy documentation (`docs/RUNNER_DEAD_LETTER_POLICY.md`)
- done: reliability counters for retries/conflicts/requeues/dead-letter moves
  and step latency in runner lifecycle path
- done: runner checkpoint/restore integration checks for inbox + dead-letter
  state continuity and resumed dispatch behavior
- done: deterministic replay hook events for inbox/timer step flow and
  disposition outcomes
- done: stable runner config surface for retry budget, lease TTL, and requeue
  attempt search budget
- done: threaded worker stop/join signaling hardened and transient `SAP_BUSY`
  recovery in worker loop validated under threaded regression
- done: worker time-hook usage expanded from scheduler-only to inbox lease and
  latency paths
- done: added threaded C-level order-pipeline sample with four worker threads
  and DB-backed stage queues (`examples/native/runner_threaded_pipeline_example.c`,
  `make runner-threaded-pipeline-example`)
- next: execute reprioritized backlog below (starting with future Priority 6
  items based on product demand)

#### Reprioritized runner backlog (known issues + planned features)
1. [Done][P1] Replay fidelity gap fixed: timer replay events now preserve timer
   key sequence, and lifecycle coverage asserts sequence-correct replay events.
2. [Done][P1] CI/threaded coverage gap fixed: added a threaded runner lifecycle
   TSan target and wired it into phase check dependencies for regression gating.
3. [Done][P2] Replay hook frame ownership contract is now explicit in API/docs
   (callback-scoped frame bytes; hooks must copy for post-callback retention).
4. [Done][P2] Runner observability export surface is now available via explicit
   host sink hooks for reliability metrics snapshots and structured log events.
5. [Done][P3] Phase E profile-guided coupling study scaffold added via
   `bench_runner_phasee.c` and `docs/RUNNER_PHASEE_COUPLING_STUDY.md`, with
   benchmarked baseline public-API polling vs study-only fused storage path.
6. [Done][P2] Phase F operational readiness package documented:
   `docs/RUNNER_PHASEF_RUNBOOK.md` + `docs/RUNNER_PHASEF_RELEASE_CHECKLIST.md`,
   with automated checklist entry point `make runner-release-checklist`.
7. [Done][P1] Schema status metadata reconciled with implemented runner DBIs:
   DBI 1-6 now marked `active`, and `tools/check_runner_dbi_status.py` was
   added and wired into `make schema-check` to prevent manifest/docs/runtime
   status drift regressions.
8. [Done][P2] WASI v0 runtime evolved to a pluggable adapter surface:
   `sap_wasi_runtime_v0_init_adapter` + optional streaming invoke support were
   added, and shim init now supports configurable reply-buffer capacity via
   `sap_wasi_shim_v0_init_with_options` (removing fixed-cap-only behavior).
9. [Done][P1] Investigate and harden concurrent multi-writer threaded
   behavior in runner-style workloads sharing one DB handle:
   - added storage hardening guards in allocator/leaf insert paths to avoid
     out-of-bounds writes on corrupted free-space metadata
   - added optional shared DB gate APIs on runner workers plus transient
     `SAP_NOTFOUND`/`SAP_CONFLICT` normalization in worker tick path
   - stabilized runtime semantics under sustained threaded churn
     (`runner-multiwriter-stress` safely completes without deadlocks)
10. [Done][P1] Added dedicated threaded runner-style multi-writer stress
    harness target (`tests/stress/runner_multiwriter_stress.c`,
    `make runner-multiwriter-stress`) and build gating entry point
    (`make runner-multiwriter-stress-build`) wired into phase-C checks;
    runtime stress execution has been stabilized and passes reliably.

#### Phase D — Reliability and observability
- deterministic replay hooks (optional)
- conflict/retry/latency counters
- crash recovery checks using checkpoint/restore

#### Phase E — Optimization (optional tight coupling)
- profile-guided coupling points with Sapling internals
- keep public API path as default correctness baseline
- explicitly non-blocking for runner functional bring-up

Phase E status:
- done: introduced coupling-study benchmark harness
  (`bench_runner_phasee.c`) comparing baseline runner public-API polling against
  a study-only fused storage candidate path, plus usage/guardrail docs in
  `docs/RUNNER_PHASEE_COUPLING_STUDY.md`

#### Phase F — Packaging and operational readiness
- stable config surface (worker counts, retry policy, lease durations)
- deployment/runbook docs and failure-mode playbooks
- release checklist with compatibility and migration verification

Phase F status:
- done: runner operations runbook added (`docs/RUNNER_PHASEF_RUNBOOK.md`)
- done: release checklist document and automation entry point added
  (`docs/RUNNER_PHASEF_RELEASE_CHECKLIST.md`, `make runner-release-checklist`)

---

## Priority 6 — Advanced / Feature Parity (DONE)

### Overflow pages for large values (done)
Non-DUPSORT values can now spill to chained overflow pages when a value no
longer fits in a single leaf cell. The leaf stores an overflow reference and
logical value length, while read paths reconstruct bytes transparently.

Current scope:
- non-DUPSORT `txn_put` / `txn_get` / cursor reads
- update/delete/range-delete cleanup of old overflow chains
- CAS (`txn_put_if`) and merge (`txn_merge`) paths operate on overflow values
- deterministic failure-atomicity coverage for overflow allocation failures
  (abort/commit and checkpoint/restore continuity)
- tree-rewrite cleanup paths and checkpoint/restore compatibility
- explicit contract-matrix coverage across `txn_put*`, `txn_load_sorted`,
  `cursor_put`, and `txn_merge` for non-DUPSORT vs DUPSORT overflow behavior

Current constraints:
- DUPSORT values remain inline-only
- `SAP_RESERVE` is rejected when the target write would require overflow

### Value-level comparator (for DupSort) (done)
When DupSort is enabled, `dbi_set_dupsort` installs a value comparator that
defines duplicate ordering for each key (including cursor duplicate traversal
order and sorted-load validation).

```c
int dbi_set_dupsort(DB *db, uint32_t dbi, keycmp_fn vcmp, void *vcmp_ctx);
```

### TTL / automatic expiry (done)
Initial helper APIs now support TTL-managed keyspaces using a companion
metadata DBI:

```c
int txn_put_ttl_dbi(Txn *txn, DBI data_dbi, DBI ttl_dbi,
                    const void *key, uint32_t key_len,
                    const void *val, uint32_t val_len,
                    uint64_t expires_at_ms);
int txn_get_ttl_dbi(Txn *txn, DBI data_dbi, DBI ttl_dbi,
                    const void *key, uint32_t key_len,
                    uint64_t now_ms,
                    const void **val_out, uint32_t *val_len_out);
int txn_sweep_ttl_dbi_limit(Txn *txn, DBI data_dbi, DBI ttl_dbi,
                            uint64_t now_ms, uint64_t max_to_delete,
                            uint64_t *deleted_count_out);
int txn_sweep_ttl_dbi(Txn *txn, DBI data_dbi, DBI ttl_dbi,
                      uint64_t now_ms, uint64_t *deleted_count_out);
```

Current behavior:
- `txn_put_ttl_dbi` performs atomic nested writes of data + expiry metadata.
- `txn_get_ttl_dbi` returns `SAP_NOTFOUND` for expired/missing metadata rows.
- `txn_sweep_ttl_dbi_limit` bounds per-call delete work via `max_to_delete`.
- `txn_sweep_ttl_dbi` walks a time-ordered expiry index and removes expired keys
  from both DBIs in one atomic helper.

Current constraints:
- caller must provision a distinct non-DUPSORT `ttl_dbi`.
- `ttl_dbi` uses reserved key prefixes for internal lookup/index rows.
- TTL helper keys must satisfy `key_len <= UINT16_MAX - 9`.

Next priorities:
1. [x] Harden sweep correctness: when draining index entries, verify lookup
   expiry exactly matches index expiry before deleting data rows (mismatch
   should prune stale metadata only).
2. [x] Add a protected TTL metadata mode so raw `txn_put*`/`txn_del*` calls
   cannot violate reserved lookup/index key-prefix invariants in `ttl_dbi`.
3. [x] Add resumable sweep checkpoints to continue long expiration drains.
4. [x] Add optional lazy-expiry deletes on read/cursor paths in write txns.
5. [x] Add host-runner background sweep cadence and observability counters.

### Range delete (done)
`txn_del_range` is available with half-open semantics `[lo, hi)` and currently
uses cursor-driven deletion, so results are exact and predictable across DBI
modes (including DUPSORT duplicates counted as separate entries). A future
optimization can still replace this with subtree-unlink fast paths.

```c
int txn_del_range(Txn *txn, DBI dbi,
                  const void *lo, uint32_t lo_len,
                  const void *hi, uint32_t hi_len,
                  uint64_t *deleted_count);
```

### Merge operator (done, callback-defined)
`txn_merge` is available for non-DUPSORT DBIs and applies a callback-defined
transform to the current value (or empty value when the key is missing).
Callbacks receive output capacity in `*new_len` and report produced bytes. If a
callback reports a size larger than inline capacity, Sapling retries once with
the requested size (up to `UINT16_MAX`) so merged values can spill to overflow
pages. Requests above `UINT16_MAX` still return `SAP_FULL`.

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

---

## Priority 7 — Refinement and Formal Verification (Lambkin Alignment)

Sapling is specifically designed as the semantic host for **Lambkin**, a statically typed, refinement-based language. Lambkin strictly adheres to the **"Big analysis, small binaries"** philosophy: it leverages heavy compiler analysis (Liquid Fixpoint, Z3, extensive escape analysis) to mathematically prove the safety and bounds of programs. This enables incredibly tiny, extremely fast embedded deployments that strip away heavy runtime machinery.

Because Lambkin favors Universal Wasm with linear memory over heavier options like WasmGC, Sapling must gracefully co-evolve to map these language guarantees down to the host storage tier.

### 1. WIT Semantic Annotations (Liquid WIT) (done)
Extend the WIT IDL with a pseudo-annotation DSL inside the comments (e.g., `/// @refine(value >= 0)`).
- **Goal:** The custom `wit_schema_codegen.py` parser will read these annotations to automatically generate C-level assert checks, protective `SAP_INVALID` barriers at the Wasm boundary, and inputs for the deterministic `fault_harness`.

### 2. Zero-Cost `atomic` Reads via Escape Analysis
Because the Lambkin compiler can statically prove when an `atomic { ... }` block contains only reads and no cross-thread side-effects:
- **Goal:** Emitting a specialized "read-only guarantee" hint to the Sapling runner. The runner can instantly map the Wasm invocation to a lock-free `TXN_RDONLY` MVCC snapshot without securing the database write mutex or tracking a write-set.

### 3. Trusted Execution Proofs (Eliding Host Checks)
Wasm workers are traditionally treated as black boxes, forcing Sapling to redundantly validate schema inputs and bounding restrictions on every call.
- **Goal:** Allow the Lambkin compiler to embed a `.lambkin_verified` proof section into the binary. When Sapling loads a proven binary, it enters a "Trusted Mode" that bypasses redundant host-side defensive validation and executes bare-metal fast-path operations.

### 4. ACSL and Frama-C Host Verification
If the Lambkin language guarantees absolutely correct interaction from the guest, the Sapling C engine becomes the weakest point.
- **Goal:** Annotate the core engine logic (e.g. `txn_sweep_ttl_dbi`, `txn_put_if`, allocation boundaries) using ACSL (ANSI/ISO C Specification Language). Utilize external tools like Frama-C or CBMC to formally prove that the host environment perfectly honors the constraints expected by the Lambkin Z3 proofs.

---

## Priority 8 — Universal Wasm Data Structures & Unification

To serve as a high-performance Wasm target out of the box, existing capabilities (`sapling.c`, `seq.c`) will be brought under a unified architecture, expanding to structures uniquely suited for WebAssembly instruction sets.

### Unified Node-Allocation and COW Mechanism
- **Goal:** Both the Finger Tree (`seq.c`) and the B+ Tree (`sapling.c`) currently use independent allocation schemes (raw `malloc` versus an internal `PageAllocator`). We will unify them under a single, Universal MVP Wasm linear memory allocator.
- **Benefit:** Reduces binary size, ensures cache-cohesive node fetching, and eliminates duplicate implementation of garbage collection or deferred-free logic. It is optimized specifically for compilation to Wasm using Wasmtime/WASI, but naturally remains runnable as fast native C.

### Transaction Machine Sharing
- **Goal:** Pull the Transaction context (`struct Txn`, `watches`, rollback states) out of the sole B+ Tree implementation into a generalized "Transactional Store" layer.
- **Benefit:** Allows the Finger Tree, B+ Tree, and all future Tries to effortlessly participate in the same `txnid` snapshoting, rollback, and uncommitted write visibility rules. They share the same mutation history, allowing arbitrary composite interactions in atomic blocks.

### Wasm-Optimized Map/Set Implementations
- **Big-Endian Patricia Trie (BEPT) (done):** A highly compact radix tree ideal for integer-keyed maps or fast memory indexing. It branches cleanly using bitwise checks and natively lowers to zero-overhead Wasm instructions like `i32/64.clz` (count leading zeros).
- **Hash Array Mapped Trie (HAMT):** Excellent for general hash maps/sets. It relies heavily on bitmap querying, which cleanly compiles down to Wasm's native `popcnt` instruction, offering very high memory density and "almost hash table-like" speed without array-resize overheads.

### Strong Mutable `text` Implementation
- **Goal:** Implement a sophisticated string type (`text`) optimized for heavily mutated text (like a Rope or a Piece Table), leveraging the Finger Tree engine.
- **Next steps:** Finalize how ownership transfer works across threads for these large sequential allocations (integrating with Lambkin's Z3 compiler proofs) to avoid massive allocation copying when exchanging text over the substrate.

---

## Priority 9 — Tunable Data Structures (Compile-Time Subsets)

- **Goal:** Introduce compile-time meta-programming (via C macros or static feature flags) to strip down or scale up features. Small applications shouldn't pay the binary-size or memory overhead for transaction nested-stack handling if they only need a basic dictionary.
- **Benefit:** Manage the trade-off between **executable size** and **speed / capability**. Congent subsets of functionality can be turned on or off gracefully depending on the specific application's requirement profile.
