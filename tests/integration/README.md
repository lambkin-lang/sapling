# Integration Tests

Cross-module tests that exercise runner + DB + Wasm behavior end to end.

Current deterministic Phase-B integration:
- `runner_atomic_integration_test.c`: retry-on-conflict + nested commit/abort
  behavior through `attempt_v0` and `txstack_v0`.
- `runner_recovery_integration_test.c`: runner inbox/dead-letter checkpoint and
  restore recovery continuity through `runner_v0` + `dead_letter_v0`.
