# WASI Host Glue Module

Planned home for host-side Wasm/WASI adapters, component bindings, and
invocation plumbing used by the C runner.

Implemented foundation:
- `shim_v0.h` / `shim_v0.c`: adapter layer that plugs `SapRunnerV0Worker` into
  a guest-call callback, with optional outbox emission for invocation results.
