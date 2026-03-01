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
  DB-backed inbox polling with lease-aware claim/ack/requeue handling plus
  retry-budget dead-letter routing, reliability counters, stable policy knobs,
  optional replay event hooks, and host-configurable observability sinks
  (metrics snapshots + structured log events).
- `SapRunnerV0Worker` shell: runner tick/stop APIs and optional pthread
  start/join helpers (gated by `SAPLING_THREADED`), synchronized stop signaling,
  retryable handling of transient `ERR_BUSY`/`ERR_NOT_FOUND`/`ERR_CONFLICT`,
  optional shared DB gate coordination for threaded workers, and clock hooks
  applied to lease timing + step-latency measurement in worker-driven paths.
- `txctx_v0.h` / `txctx_v0.c`: phase-B host transaction context scaffold with
  read-set tracking, write-set staging, intent buffering, and
  validate/apply helpers for short write-txn commit phases.
- `txstack_v0.h` / `txstack_v0.c`: phase-B nested atomic stack scaffold with
  closed-nesting push/commit/abort semantics and root commit guards.
- `attempt_v0.h` / `attempt_v0.c`: phase-B bounded retry attempt engine around
  snapshot execution, root validation/apply, and post-commit intent sink.
- `attempt_handler_v0.h` / `attempt_handler_v0.c`: generic runner-handler
  adapter that executes message handling through `attempt_v0`.
- `examples/native/runner_native_example.c`: non-WASI example worker path using
  `attempt_handler_v0` plus composed outbox/timer intent sink.
- `bench_runner_phasee.c`: Phase E coupling-study benchmark comparing baseline
  public-API polling against a study-only fused storage candidate path.
- `docs/RUNNER_PHASEF_RUNBOOK.md`: operational playbook for startup, health,
  failure handling, recovery, and shutdown.
- `docs/RUNNER_PHASEF_RELEASE_CHECKLIST.md`: release gate checklist for schema,
  reliability, observability, and benchmark validation.
- `tests/integration/runner_recovery_integration_test.c`: checkpoint/restore
  recovery continuity for inbox + dead-letter runner state.
- `mailbox_v0.h` / `mailbox_v0.c`: phase-C mailbox lease claim/ack/requeue
  scaffold with CAS-style lease token guards.
- `dead_letter_v0.h` / `dead_letter_v0.c`: phase-C dead-letter move helpers for
  exhausted retry-budget messages, plus drain/replay tooling APIs.
- `outbox_v0.h` / `outbox_v0.c`: phase-C outbox append/drain APIs plus
  attempt-intent publisher adapter for committed outbox emission.
- `timer_v0.h` / `timer_v0.c`: phase-C timer append/due-drain APIs plus
  attempt-intent publisher adapter for committed timer ingestion.
- `scheduler_v0.h` / `scheduler_v0.c`: phase-C next-due lookup and sleep-budget
  helpers for timer-aware worker idle behavior.
- `intent_sink_v0.h` / `intent_sink_v0.c`: composed intent sink that routes
  `OUTBOX_EMIT` and `TIMER_ARM` to the correct Phase-C publisher.
