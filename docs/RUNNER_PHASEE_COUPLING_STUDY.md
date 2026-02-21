# Runner Phase E Coupling Study

This document defines the optional Phase E benchmark harness:
- `bench_runner_phasee.c`
- `make runner-phasee-bench-run`

Goal: quantify the upside of tighter storage coupling before changing the
runner’s correctness-first public-API path.

## Scenarios

The harness runs two scenarios on identical inbox workloads:

1. `baseline_poll_public_api`
- Uses `sap_runner_v0_poll_inbox(...)` with a no-op handler.
- Exercises the current public-API lifecycle path:
  - read-next inbox frame
  - lease claim
  - frame decode + callback
  - ack (delete inbox + lease)

2. `candidate_fused_storage`
- Study-only path implemented in benchmark code.
- Fuses claim+ack-like storage operations into one write transaction per
  message and decodes frame bytes before commit.
- Does not invoke user/guest handler logic and is not used by `runner_v0`.

## Run

Default run:

```sh
make runner-phasee-bench-run
```

Override workload shape:

```sh
make runner-phasee-bench-run \
  RUNNER_PHASEE_BENCH_COUNT=20000 \
  RUNNER_PHASEE_BENCH_ROUNDS=8 \
  RUNNER_PHASEE_BENCH_BATCH=128
```

## Output

The harness prints:
- average seconds and message throughput for both scenarios
- `speedup(candidate/baseline)`

Interpretation:
- `> 1.0x` means fused study path is faster.
- A persistent margin over repeated runs indicates potential value in evaluating
  narrowly-scoped coupling experiments.

## Guardrails

- Baseline public-API path remains default production behavior.
- Study results are informational; they do not alter runtime policy.
- Any future coupling change should preserve:
  - durable inbox/dead-letter guarantees
  - lease correctness under contention
  - “no guest handler execution while holding write transaction” contract
  - replay/metrics/log observability surfaces
