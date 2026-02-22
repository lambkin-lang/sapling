# Repository Layout (Phase 0)

This repository is transitioning from a top-level C file layout into a staged
source tree for long-term maintainability.

## Canonical locations

- `include/sapling/`: public headers
- `src/sapling/`: storage engine implementation
- `src/common/`: shared utilities (fault injection, buffers, helpers)
- `src/runner/`: host runner implementation (planned)
- `src/wasi/`: Wasm host glue (planned)
- `tests/`: unit/integration/stress tests
- `examples/`: runnable examples
- `tools/`: validation and migration utilities
- `schemas/`: schema sources and generated manifests (`schemas/wit/` for WIT)
- `generated/`: generated C metadata derived from schema/tooling
- `docs/`: architecture and ops guides

## Compatibility window

The Phase 0 compatibility window has closed. Top-level `sapling.c` and `sapling.h` shims have been retired.
All code must include `sapling/sapling.h` from `include/`.
