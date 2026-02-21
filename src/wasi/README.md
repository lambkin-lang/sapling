# WASI Host Glue Module

Planned home for host-side Wasm/WASI adapters, component bindings, and
invocation plumbing used by the C runner.

Implemented foundation:
- `runtime_v0.h` / `runtime_v0.c`: concrete runtime invocation wrapper with
  call/error tracking around guest entrypoints.
- `shim_v0.h` / `shim_v0.c`: adapter layer that plugs `SapRunnerV0Worker` into
  `runtime_v0`, with optional outbox emission for invocation results.
