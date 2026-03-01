# Sapling

Sapling is a clean-room, MIT-licensed storage engine in portable C, built as the
shared-state substrate for **Lambkin**, a statically typed, refinement-based
language. It provides a copy-on-write B+
tree with MVCC snapshots, nested transactions, and a family of companion data
structures — all designed from the ground up for WebAssembly linear memory.

The project follows Lambkin's **"big analysis, small binaries"** philosophy:
heavy compile-time verification enables tiny, fast embedded deployments that
strip away runtime overhead. Sapling targets Universal Wasm with linear memory
rather than heavier options like WasmGC, and every abstraction is shaped by that
constraint.

## Architecture at a glance

Sapling models a complete Wasm worker environment. A C host runner manages
threads, each hosting an independent Wasm instance with its own linear memory.
Workers communicate exclusively through serialized messages and shared database
state — there is no shared mutable memory between them. The MVCC B+ tree is the
sole safe state-sharing channel.

```
┌───────────────────────────────────────────────────┐
│                    C Host Runner                  │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐   │
│  │  Worker 0  │  │  Worker 1  │  │  Worker N  │   │
│  │  (pthread) │  │  (pthread) │  │  (pthread) │   │
│  │ ┌────────┐ │  │ ┌────────┐ │  │ ┌────────┐ │   │
│  │ │  Wasm  │ │  │ │  Wasm  │ │  │ │  Wasm  │ │   │
│  │ │Instance│ │  │ │Instance│ │  │ │Instance│ │   │
│  │ │(linear │ │  │ │(linear │ │  │ │(linear │ │   │
│  │ │ memory)│ │  │ │ memory)│ │  │ │ memory)│ │   │
│  │ └───┬────┘ │  │ └───┬────┘ │  │ └───┬────┘ │   │
│  │     │shim  │  │     │shim  │  │     │shim  │   │
│  └─────┼──────┘  └─────┼──────┘  └─────┼──────┘   │
│        └───────────────┼───────────────┘          │
│                        ▼                          │
│  ┌─────────────────────────────────────────────┐  │
│  │         Sapling (MVCC B+ Tree Store)        │  │
│  │  DBI 0: app_state    DBI 3: leases          │  │
│  │  DBI 1: inbox        DBI 4: timers (schema) │  │
│  │  DBI 2: outbox       DBI 5: dedupe          │  │
│  │                      DBI 6: dead_letter     │  │
│  └─────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────┘
```

Note: the runner's due-timer runtime index is BEPT-backed, with DBI 4 as the
durable source of truth. Timer writes are dual-written (DBI 4 + BEPT), runner
bootstrap rebuilds BEPT from DBI 4, and timer reads self-heal BEPT drift.

## Data structures

### B+ Tree (`sapling.c`)

The core storage engine. A copy-on-write B+ tree with page-based allocation,
configurable page sizes (256–65535 bytes), and four page types: meta, internal,
leaf, and overflow.

It provides MVCC snapshot isolation, nested transactions with closed-nesting
semantics (child merges into parent on commit), and up to 32 concurrent database
indexes (DBIs) sharing a single page pool. Readers are lock-free. Writers
serialize through a single mutex, but the OCC execution model keeps write
transactions extremely short — Wasm executes against a read snapshot, and the
write transaction only opens for a brief validate/apply/commit window.

Key capabilities: DupSort for secondary indexes, overflow pages for large
values, TTL expiry helpers, prefix-scoped watch notifications, CAS
(`txn_put_if`), range count/delete, merge operator, sorted bulk load, and
checkpoint/restore serialization.

### Arena Allocator (`arena.c`)

A pluggable page and node allocator that abstracts away the backing memory
strategy. This is what makes the same C code run natively, under WASI, or inside
a browser's linear memory without changing a line of application logic.

Backing strategies: `MALLOC` (standard native chunking), `MMAP` (file-backed),
`WASI_FD` (WASI filesystem), `LINEAR` (simple array expansion for
browser/Workers environments), and `CUSTOM` (test instrumentation callbacks).
Page allocation serves the B+ tree; node allocation serves the companion data
structures (Seq, BEPT, Text).

### Thatch (`thatch.c`)

A cursor-oriented, mostly-serialized memory model for bulk-processed immutable
trees. Thatch regions are arena-backed pages with bump allocators, and traversal
uses `(region, cursor)` pairs — where a cursor is just a `uint32_t` byte offset
— so navigation requires zero heap allocation.

Thatch is designed for data that arrives as a batch (like a JSON document or a
message payload), gets processed once, and is then either committed or discarded.
During a transaction, regions are owned by the transaction's linked list. After
commit they are sealed (immutable) and detached. After abort they are freed
immediately. Skip pointers provide O(1) subtree bypass for selective traversal.

A companion JSON parser (`thatch_json.c`) parses JSON directly into Thatch
regions with jq-style path queries, overflow protection, and strict failure
cleanup.

### Finger-Tree Sequence (`seq.c`)

A 2-3 finger tree parameterized by element count. Provides O(1) amortized
push/pop at both ends, O(log n) indexed access, O(log n) concatenation, and
O(log n) split. Cached size measures give O(1) length queries.

Seq is the building block for ordered collections that need efficient edits at
the boundaries — the kind of thing you reach for when an array's O(n) insert
cost becomes a problem but you still need indexed access.

### Mutable Unicode Text (`text.c`)

Built on Seq, this is a handle-based text rope where each element is one of four
handle types: direct Unicode codepoints, runtime-defined literal-table IDs,
subtree/COW references, or a reserved variant. A runtime resolver expands mixed
handles during traversal with depth and visit guards to prevent runaway
expansion.

Text supports copy-on-write cloning and is designed for the Lambkin `text` type,
where large text values can be transferred between workers by handing off a COW
reference rather than deep-copying bytes.

### Big-Endian Patricia Trie (`bept.c`)

A compact radix tree for integer-keyed maps, branching on big-endian bit
prefixes. Keys are arrays of 32-bit words, with the word count chosen at
runtime — 2 words for 64-bit keys, 4 for 128-bit, and so on. The branching
primitive is `clz` (count leading zeros) on a single `uint32_t`, which maps
directly to Wasm's `i32.clz` instruction. This is a deliberate design choice:
wider key types like C's `__int128` would require synthesized multi-word
operations that don't exist in Universal Wasm, so BEPT stays within the
instruction set that compiles cleanly. Useful for fast memory indexing and timer
lookups where keys are naturally integer-typed and lexicographic ordering of the
big-endian representation gives time-ordered iteration for free.

## Host runner

The runner is a multi-phase C implementation for managing Wasm workers. It
implements the full lifecycle: message serialization (frozen v0 wire format),
transaction orchestration with bounded retry and backoff, mailbox coordination
with lease-based claim/ack/requeue, outbox publication, timer scheduling, dead
letter handling, deduplication, and checkpoint/restore recovery.

The atomic block contract is central: guest Wasm executes against a read
snapshot, buffering intents (messages and timers) in a host transaction context.
Only after successful root-level commit do intents become visible. Nested atomic
blocks use closed nesting. This is what makes automatic retry sound — atomic
blocks are side-effect-free until commit.

The runner integrates with Wasm through a WASI shim layer (`src/wasi/`), but
also exposes a generic `attempt_handler_v0` contract for non-WASI native
integrations.

## Wire format

Messages and intents use a frozen v0 binary encoding with strict validation.
Message headers are 60 bytes; intent headers are 28 bytes. Message kinds are
`COMMAND`, `EVENT`, and `TIMER`. Intent kinds are `OUTBOX_EMIT` and `TIMER_ARM`.
The encoding is designed for zero-copy reads from Wasm linear memory.

## Schema pipeline

The WIT interface definition (`schemas/wit/runtime-schema.wit`) is the canonical
source of truth for DBI layouts. A code generator (`tools/wit_codegen.c`)
produces C metadata (`generated/wit_schema_dbis.h` and
`generated/wit_schema_dbis.c`). `make schema-check` validates the full pipeline:
WIT syntax, codegen freshness, C compilation, and manifest consistency with
runtime usage.

## Build and test

The compiler matrix is clang (primary) and gcc (secondary), C11 strict with
`-Wall -Wextra -Werror`. Sanitizer coverage includes ASan/UBSan by default and
TSan for threaded builds.

Use the Makefile as the source of truth for `make` commands to use and for the
expected project layout.

## Documentation

The `docs/` directory contains detailed design documents for each subsystem.
`FEATURES.md` is the feature roadmap and phase tracker. Notable entry points:

- `TUTORIAL_CONCURRENCY_ACTORS.md` — STM/OCC, CAS, retry loops, nested
  transactions, the Lambkin Actor Model
- `docs/THATCH_DESIGN.md` — ownership, lifetime, sealing, region allocation
- `docs/SEQ_DESIGN.md` — finger-tree structure, concatenation, split algorithms
- `docs/RUNNER_WIRE_V0.md` — frozen v0 serialization contract
- `docs/WIT_SCHEMA.md` — schema conventions and codegen pipeline
- `docs/RUNNER_PHASEF_RUNBOOK.md` — operational guide and incident handling

---

## Work in progress

These items are actively being developed or recently stabilized and may still
need attention under load.

1. **WIT codegen hardening follow-through.** Complex WIT types now emit real C
   layouts (no `unknown_layout` placeholders), with structural validators and
   skip-pointer segment checks. Remaining work is maintenance: keep
   schema/codegen drift checks and adversarial tests current as the schema
   evolves.

## Pending work and known gaps

### Optimization opportunities (functional, deferred)

- **DupSort bulk load.** `txn_load_sorted` on DupSort DBIs falls back to
  O(log n) per-row inserts rather than the O(n) tree-builder used for non-DupSort
  empty-DBI loads.

- **Range count estimator.** `txn_count_range` uses cursor iteration (exact but
  O(k)). An approximate structural estimator would give O(1) for query planning.

- **Range delete fast path.** `txn_del_range` also uses cursor-driven deletion.
  Subtree-unlink could make bulk deletes significantly cheaper.

### Test coverage status

- **BEPT** coverage includes rollback, 128-bit regression, deletion-collapse
  edge cases, word-boundary key patterns, and allocator-pressure behavior.

- **Arena allocator** coverage includes basic alloc/free paths plus
  backing-strategy switching, exhaustion behavior, and multi-region
  fragmentation/reuse checks.

- **Runner scheduler, lease, and dedupe** subsystems each have around 110 lines
  of test code. Coverage is functional but not deep.

### Recently resolved quality issues

- **Multi-writer stress hardening.** Resolved. The threaded 4-stage
  pipeline stress harness (`make runner-multiwriter-stress`) passes reliably
  with per-round corruption telemetry and free-list structural validation.
  A burn-in profile (`make runner-multiwriter-stress-burn-in`) runs 32 rounds
  of 256 orders for extended soak testing. A fault-injected variant
  (`make runner-multiwriter-stress-fault`) verifies graceful degradation under
  configurable page-alloc failure rates — workers survive transient OOM and the
  pipeline makes forward progress despite faults. Additional coverage includes
  HAMT concurrent stress, deferred-page pressure, and fault-injected B+ tree
  stress tests.

- **Error code families are disjoint.** Resolved. All subsystems now share a
  unified `ERR_*` taxonomy defined in `include/sapling/err.h`. Previous
  per-subsystem error families were removed and generic catch-all returns were
  decomposed into specific codes (`ERR_OOM`, `ERR_INVALID`, `ERR_CORRUPT`) at
  each call site. A diagnostic helper `err_to_string()` is available for
  logging.

- **Legacy error wording in docs/examples.** Resolved. Public docs/examples now
  consistently refer to `ERR_*` result codes for subsystem APIs.

- **Allocator usage is asymmetric.** Resolved. All companion subsystems
  (Seq, BEPT, HAMT, Text, TextLiteral, TextTreeRegistry) now allocate
  exclusively through `SapMemArena`. Transaction-scoped metadata uses
  `sap_txn_scratch_alloc`; growable arrays use the new `SapTxnVec`
  (arena-backed growable array in `txn_vec.h`/`txn_vec.c`); env-scoped
  structs use `sap_arena_alloc_node`. Two host-only `malloc` call sites
  remain in `text.c` (deep-tree path buffer, `text_to_utf8_full` output
  buffer) and are guarded by `#ifndef SAP_NO_MALLOC`. A `nomalloc.h` header
  (`#pragma GCC poison malloc calloc realloc free`) enforces this at compile
  time; run `make nomalloc-check` to verify.

- **Memory cleanup questions in text.c.** Resolved. The inline questions
  referenced at lines 561 and 600 no longer exist (line numbers drifted after
  prior edits). The code at those locations (`text_insert_range` epilogue and
  `text_shared_retain` in `text_clone`) is clean with correct arena-based
  lifecycle management.

### Reuse and alignment opportunities

- **~~Unified node allocator.~~** Partially addressed. `SapTxnVec` provides a
  shared arena-backed growable-array abstraction used by Seq, HAMT,
  TextLiteral, and TextTreeRegistry. All subsystems now flow through
  `sap_arena_alloc_node`/`sap_arena_alloc_page` for persistent data and
  `sap_txn_scratch_alloc` for transaction-scoped metadata. A higher-level
  `PageAllocator`-style interface for cross-structure allocation tracking
  remains a future opportunity.

- **Shared transaction machine.** The B+ tree owns the `Txn` context and MVCC
  machinery. Extracting a generalized transactional-store layer would let Seq,
  BEPT, and Text participate in the same snapshot/rollback semantics — enabling
  atomic blocks that span multiple data structure types without the runner
  needing to orchestrate them manually.

- **BEPT timer integration hardening.** Completed with a deterministic
  persist-or-rebuild contract:
  - BEPT init is wired into `db_open`, and init is idempotent
  - timers are durably stored in DBI 4 and indexed in BEPT for fast due lookup
  - bootstrap rebuilds BEPT from DBI 4 (`sap_runner_timer_v0_sync_index`)
  - timer reads validate and self-heal BEPT index drift against DBI 4

- **Wire format and Thatch convergence.** The runner wire format (`wire_v0`)
  defines its own encode/decode for message envelopes. Thatch provides a general
  cursor-oriented serialized data model. There may be an opportunity to express
  wire payloads as Thatch regions, which would give message handling the same
  zero-allocation traversal and skip-pointer benefits that Thatch provides for
  JSON.

## Remaining work

This is a concrete task list, ordered roughly by impact. Items marked with a
phase reference relate to the runner implementation track described in
`FEATURES.md`.

### Must do (completed)

- [x] Wire a real clock source into the WASI shim's `atomic_ctx.now_ms` for TTL
  sweep correctness
- [x] Harden BEPT timer integration: standardize subsystem initialization
  (remove manual call-site init requirements)
- [x] Define and test BEPT timer-index durability semantics for
  `db_checkpoint`/`db_restore` (deterministic DBI4 source-of-truth rebuild)
- [x] Expand BEPT test coverage further: deletion edge cases, word-boundary
  keys, arena exhaustion
- [x] Expand arena allocator tests: backing-strategy switching, exhaustion
  behavior, multi-region fragmentation
- [x] Extend WIT codegen to produce usable C struct layouts for compound types
  (`message-envelope`, `lease-state`, `worker-id`) instead of `unknown_layout`
  placeholders

### Should do

- [ ] Finalize policy for host-only `malloc` usage in `text.c` conversion paths
  (document guarantees and decide whether to add an arena-backed alternative)
- [ ] Deepen runner scheduler, lease, and dedupe test coverage
- [ ] Implement DupSort-aware `txn_load_sorted` using the tree-builder fast path

### Could do (future alignment)

- [ ] Design and implement a unified node-allocation interface for Seq, BEPT,
  and Text
- [ ] Extract a generalized transactional-store layer from the B+ tree `Txn`
  context
- [ ] Evaluate expressing wire payloads as Thatch regions for zero-allocation
  message traversal
- [ ] Add an approximate structural estimator for `txn_count_range`
- [ ] Add a subtree-unlink fast path for `txn_del_range`

## License

MIT
