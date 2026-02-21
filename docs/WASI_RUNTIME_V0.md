# WASI Runtime v0

`src/wasi/runtime_v0.*` is the guest invocation layer used by `shim_v0`.
It provides a pluggable runtime adapter surface and tracks invocation state.

## API

- `sap_wasi_runtime_v0_init_adapter`
  - binds a named `SapWasiRuntimeV0Adapter` + adapter context into a runtime
    instance
- `sap_wasi_runtime_v0_init`
  - compatibility helper that wraps a legacy buffered callback into the adapter
    surface
- `sap_wasi_runtime_v0_invoke`
  - invokes the runtime adapter with message payload bytes as request input
  - updates `calls` and `last_rc` on every invocation

## Invocation contract

Buffered adapter callback:
- input: request bytes (`msg.payload`)
- output: reply bytes (written into caller-provided buffer)
- return: `SAP_OK` on success, non-`SAP_OK` on invocation error

Streaming adapter callback:
- input: request bytes
- output: zero or more write-callback chunks
- return: `SAP_OK` on success, non-`SAP_OK` on invocation error

The runtime wrapper enforces:
- non-null runtime/adapter/buffer arguments
- reply length does not exceed provided capacity

## Tests

`tests/unit/wasi_runtime_test.c` verifies:
- successful buffered invoke path with call counters
- error propagation and runtime error-state capture
- streaming adapter success path
- streaming reply overflow handling (`SAP_FULL`)
