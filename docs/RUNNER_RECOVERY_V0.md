# Runner Recovery Checks v0

This document defines checkpoint/restore recovery checks for runner state.

Primary integration test:
- `tests/integration/runner_recovery_integration_test.c`

Build target:
- `make runner-recovery-test`

## Covered invariants

The integration test validates:
1. Runner-specific DBIs (inbox + dead-letter) are fully captured in
   `db_checkpoint(...)`.
2. Mutations after snapshot (message dispatch + dead-letter replay) are
   reverted by `db_restore(...)`.
3. After restore, runner dispatch can resume and process restored inbox frames
   correctly.

## Why this matters

Runner reliability depends on durable queue semantics across process restarts
or crash recovery. This check ensures checkpoint/restore preserves coordination
state, not just application DBI data.
