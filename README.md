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
