# Runner Attempt Handler v0

This document defines the generic attempt-backed handler adapter:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/attempt_handler_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/attempt_handler_v0.c`

## Purpose

Expose a reusable runner handler contract that executes message handling through
`attempt_v0`, without tying the behavior to the WASI shim.

## API

- `sap_runner_attempt_handler_v0_init(...)`
  - binds DB handle, atomic callback, optional intent sink, and default retry policy
- `sap_runner_attempt_handler_v0_set_policy(...)`
  - overrides default attempt policy (`max_retries`, backoff, sleep hook)
- `sap_runner_attempt_handler_v0_runner_handler(...)`
  - adapter matching `sap_runner_v0_message_handler` signature
  - runs callback through `sap_runner_attempt_v0_run(...)`
  - stores latest run stats in `last_stats`

Atomic callback signature:
- `atomic_fn(stack, read_txn, runner, msg, ctx)`
- can stage writes/intents via `txstack_v0`
- runs under bounded retry orchestration from `attempt_v0`

## Tests

`tests/unit/runner_attempt_handler_test.c` verifies:
- successful commit path with post-commit intent sink delivery
- conflict retry behavior and stats reporting
