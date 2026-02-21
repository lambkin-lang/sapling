# Examples

Runnable examples for native host and Wasm guest workflows.

- Native non-WASI runner path:
  - source: `examples/native/runner_native_example.c`
  - run: `make runner-native-example`

- Native threaded pipeline path (4 C worker threads):
  - source: `examples/native/runner_threaded_pipeline_example.c`
  - run: `make runner-threaded-pipeline-example`
