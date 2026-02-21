# WASI Shim v0

`src/wasi/shim_v0.*` provides the first concrete integration point between the
runner worker shell and guest invocation logic.

## Purpose

- expose a stable callback contract for guest execution
- allow `SapRunnerV0Worker` to process inbox messages through that callback
- optionally emit invocation results to outbox (DBI 2)

## API

- `sap_wasi_shim_v0_init`
  - binds DB handle, guest callback, and shim options
- `sap_wasi_shim_v0_runner_handler`
  - handler adapter passed into runner lifecycle APIs
- `sap_wasi_shim_v0_worker_init`
  - convenience wrapper: initializes `SapRunnerV0Worker` with shim handler

Guest callback signature:
- `sap_wasi_shim_v0_guest_call`
  - input: decoded `SapRunnerMessageV0`
  - output: reply payload bytes (written to provided buffer)
  - return: `SAP_OK` on success, non-`SAP_OK` to fail message processing

## Outbox behavior

When `emit_outbox_events` is enabled and guest callback returns a non-empty
reply payload, the shim:
1. builds a reply `LMSG` frame (`kind=event`)
2. writes frame to outbox DBI (DBI 2) with big-endian `u64` sequence key
3. increments `next_outbox_seq`

When disabled or reply length is zero, no outbox record is emitted.

## Tests

`tests/unit/wasi_shim_test.c` verifies:
- inbox -> shim -> outbox happy path
- callback error propagation with inbox retention
