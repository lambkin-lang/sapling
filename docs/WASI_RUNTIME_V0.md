# WASI Runtime v0

`src/wasi/runtime_v0.*` is the concrete guest invocation layer used by
`shim_v0`. It replaces the direct shim callback stub with a runtime object that
tracks invocation state.

## API

- `sap_wasi_runtime_v0_init`
  - binds an entry name and invocation function into a runtime instance
- `sap_wasi_runtime_v0_invoke`
  - invokes the runtime entry with message payload bytes as request input
  - updates `calls` and `last_rc` on every invocation

## Invocation contract

Entry signature:
- input: request bytes (`msg.payload`)
- output: reply bytes (written into caller-provided buffer)
- return: `SAP_OK` on success, non-`SAP_OK` on invocation error

The runtime wrapper enforces:
- non-null runtime/function/buffer arguments
- reply length does not exceed provided capacity

## Tests

`tests/unit/wasi_runtime_test.c` verifies:
- successful invoke path with call counters
- error propagation and runtime error-state capture
