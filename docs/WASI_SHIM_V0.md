# WASI Shim v0

`src/wasi/shim_v0.*` provides the first concrete integration point between the
runner worker shell and guest invocation logic.

## Purpose

- connect worker handling to a concrete runtime object (`runtime_v0`)
- allow `SapRunnerV0Worker` to process inbox messages through that callback
- execute guest calls through `attempt_v0` retry flow
- publish committed intents through the composed sink (`intent_sink_v0`)

## API

- `sap_wasi_shim_v0_options_default`
  - fills defaults for init options (default reply cap, outbox disabled)
- `sap_wasi_shim_v0_init_with_options`
  - binds DB handle, runtime instance, and explicit shim options
  - supports caller-provided reply buffer/cap for larger replies
- `sap_wasi_shim_v0_init`
  - compatibility helper using default inline reply buffer capacity
- `sap_wasi_shim_v0_runner_handler`
  - handler adapter passed into runner lifecycle APIs
- `sap_wasi_shim_v0_worker_init`
  - convenience wrapper: initializes `SapRunnerV0Worker` with shim handler

Runtime invocation:
- shim runs `sap_runner_attempt_v0_run(...)` and invokes
  `sap_wasi_runtime_v0_invoke` inside the attempt atomic callback
- request input is `msg.payload`
- reply payload bytes are copied into configured shim response buffer
- retryable errors follow attempt-policy retry/backoff rules
- attempt stats are exposed on `shim.last_attempt_stats`

## Outbox behavior

When `emit_outbox_events` is enabled and guest callback returns a non-empty
reply payload, the shim atomic callback:
1. builds a reply `LMSG` frame (`kind=event`)
2. pushes `OUTBOX_EMIT` intent into tx stack
3. after successful attempt commit, intent sink publishes to outbox DBI (DBI 2)
4. updates `next_outbox_seq` from sink state

When disabled or reply length is zero, no outbox record is emitted.

## Tests

`tests/unit/wasi_shim_test.c` verifies:
- inbox -> shim -> outbox happy path
- retryable callback errors drive attempt retries before inbox requeue
- non-retryable callback errors propagate while preserving inbox durability
- custom reply-buffer cap wiring through `sap_wasi_shim_v0_init_with_options`
