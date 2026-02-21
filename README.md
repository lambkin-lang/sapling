# sapling
Clean-room, MIT-licensed copy-on-write B+ tree in portable C with MVCC snapshots,
nested transactions, multi-DBI support, cursor reuse/key-only helpers, and
DupSort/prefix helpers, plus range count, sorted-load, CAS, checkpoint APIs,
and prefix watch notifications.

## Build and test
- `make` builds `libsapling.a`
- `make test` runs the full native test suite
- `make asan` runs AddressSanitizer/UBSan
- `make tsan` runs ThreadSanitizer (`SAPLING_THREADED`)
- `make bench` builds `bench_sapling`
- `make bench-run BENCH_COUNT=100000 BENCH_ROUNDS=3` runs empty and non-empty sorted-load benchmarks
- `make bench-ci` runs benchmark regression guardrails using `benchmarks/baseline.env`
- `make wit-schema-check` validates WIT in `schemas/wit/` using `wasm-tools`
- `make wit-schema-generate` regenerates schema artifacts from WIT
- `make wit-schema-cc-check` compile-checks generated C schema metadata
- `make schema-check` runs WIT validation + codegen + generated-C compile check + `schemas/dbi_manifest.csv` validation
- `make runner-wire-test` runs v0 runner message/intent wire-format tests
- `make runner-lifecycle-test` runs phase-A runner lifecycle/schema-guard tests
- `make runner-txctx-test` runs phase-B host tx context tests
- `make runner-txstack-test` runs phase-B nested tx stack tests
- `make runner-attempt-test` runs phase-B bounded retry attempt tests
- `make runner-integration-test` runs deterministic phase-B retry+nested integration tests
- `make runner-mailbox-test` runs phase-C mailbox claim/ack/requeue tests
- `make runner-dead-letter-test` runs phase-C dead-letter move tests
- `make runner-outbox-test` runs phase-C outbox append/drain tests
- `make runner-timer-test` runs phase-C timer intent ingestion/drain tests
- `make runner-scheduler-test` runs phase-C timer scheduling helper tests
- `make runner-intent-sink-test` runs composed outbox+timer intent sink tests
- `make wasi-runtime-test` runs concrete WASI runtime wrapper tests
- `make wasi-shim-test` runs phase-A runner<->wasi shim integration tests
- `make stress-harness` runs deterministic fault-injection harness scaffolding
- `make phasea-check` runs phase-0 checks plus phase-A runner tests
- `make phaseb-check` runs phase-A checks plus phase-B tx context tests
- `make phasec-check` runs phase-B checks plus phase-C mailbox/dead-letter/outbox/timer tests

Benchmark guardrail overrides:
- `BENCH_BASELINE=benchmarks/baseline.env` selects the baseline file
- `BENCH_ALLOWED_REGRESSION_PCT=45` overrides the allowed regression budget

## Wasm build checks
Sapling includes explicit `wasm32-wasi` targets:
- `make wasm-lib` builds `libsapling_wasm.a`
- `make wasm-check` builds `wasm_smoke.wasm`

Both require a WASI sysroot:
- `make wasm-check WASI_SYSROOT=/path/to/wasi-sysroot`

## Learning guide
- `TUTORIAL_STM_OCC.md` explains STM/OCC, CAS, retry loops, nested
  transactions, watch semantics, and key concurrency terminology.
- `docs/REPO_LAYOUT.md` describes the source tree migration and compatibility
  shims.
- `docs/WIT_SCHEMA.md` describes WIT-first schema conventions and codegen.
- `docs/RUNNER_WIRE_V0.md` defines the frozen v0 runner serialization contract.
- `docs/RUNNER_LIFECYCLE_V0.md` defines lifecycle bootstrap and schema-version guard behavior.
- `docs/RUNNER_TXCTX_V0.md` defines the initial Phase-B host tx context behavior.
- `docs/RUNNER_TXSTACK_V0.md` defines closed-nested atomic stack behavior.
- `docs/RUNNER_ATTEMPT_V0.md` defines bounded retry-attempt orchestration.
- `docs/RUNNER_MAILBOX_V0.md` defines mailbox lease claim/ack/requeue behavior.
- `docs/RUNNER_DEAD_LETTER_V0.md` defines dead-letter move/record behavior.
- `docs/RUNNER_OUTBOX_V0.md` defines outbox append/drain and intent-publisher behavior.
- `docs/RUNNER_TIMER_V0.md` defines timer intent ingestion and due-time draining.
- `docs/RUNNER_SCHEDULER_V0.md` defines timer next-due and sleep-budget helpers.
- `docs/RUNNER_INTENT_SINK_V0.md` defines the composed outbox+timer intent sink.
- `docs/WASI_RUNTIME_V0.md` defines the concrete runtime invocation layer used by the shim.
- `docs/WASI_SHIM_V0.md` defines runner worker integration with the WASI shim/runtime path.

## Source layout transition (phase 0)
- Canonical engine sources now live in:
  - `include/sapling/sapling.h`
  - `src/sapling/sapling.c`
- Top-level `sapling.h` and `sapling.c` are temporary compatibility shims.

## Important runtime constraints
- `db_open` requires `page_size` in `[256, 65535]`.
- `dbi_open` and `dbi_set_dupsort` are metadata operations and require no
  active transactions; otherwise they return `SAP_BUSY`.
- `cursor_open_dbi` returns `NULL` for invalid DBI handles.
- Watch callbacks are driven by top-level commits for keys in the watched DBI.
  Watch registrations affect write transactions started after registration.
- `db_watch_dbi`/`db_unwatch_dbi` provide DBI-scoped watches; DUPSORT DBIs are
  rejected. Watch registration/unregistration returns `SAP_BUSY` while a write
  transaction is active.

## Allocation model
`PageAllocator` controls allocation of persistent tree/meta pages. Sapling may
still use `malloc/free` internally for temporary scratch buffers.
