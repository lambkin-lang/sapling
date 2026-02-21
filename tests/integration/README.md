# Integration Tests

Cross-module tests that exercise runner + DB + Wasm behavior end to end.

Current deterministic Phase-B integration:
- `runner_atomic_integration_test.c`: retry-on-conflict + nested commit/abort
  behavior through `attempt_v0` and `txstack_v0`.
