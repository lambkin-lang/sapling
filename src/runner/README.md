# Runner Module

Planned home for the C host runner:
- worker thread orchestration
- retry/commit state machine
- mailbox and timer scheduling loop

Implemented foundation:
- `wire_v0.h` / `wire_v0.c`: strict v0 message + intent serialization contract
  with version checks and deterministic decode validation.
- `runner_v0.h` / `runner_v0.c`: phase-A lifecycle scaffold with DBI bootstrap,
  schema-version guard, single-step dispatch callback integration, and
  DB-backed inbox polling.
- `SapRunnerV0Worker` shell: runner tick/stop APIs and optional pthread
  start/join helpers (gated by `SAPLING_THREADED`).
- `txctx_v0.h` / `txctx_v0.c`: phase-B host transaction context scaffold with
  read-set tracking, write-set staging, intent buffering, and
  validate/apply helpers for short write-txn commit phases.
- `txstack_v0.h` / `txstack_v0.c`: phase-B nested atomic stack scaffold with
  closed-nesting push/commit/abort semantics and root commit guards.
- `attempt_v0.h` / `attempt_v0.c`: phase-B bounded retry attempt engine around
  snapshot execution, root validation/apply, and post-commit intent sink.
- `mailbox_v0.h` / `mailbox_v0.c`: phase-C mailbox lease claim/ack/requeue
  scaffold with CAS-style lease token guards.
- `outbox_v0.h` / `outbox_v0.c`: phase-C outbox append/drain APIs plus
  attempt-intent publisher adapter for committed outbox emission.
