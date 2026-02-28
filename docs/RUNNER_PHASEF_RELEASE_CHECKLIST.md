# Runner Phase F Release Checklist

This checklist is the release gate for runner/storage integration changes.

Automated entry point:

```sh
make runner-release-checklist
```

This runs:
- `make phasec-check`
- `make runner-phasee-bench-run` with release benchmark settings

## 1) Code and schema freeze

- Confirm `schemas/wit/runtime-schema.wit` and generated DBI manifest changes
  are intentional.
- Confirm wire format compatibility policy remains valid:
  - `docs/RUNNER_WIRE_V0.md`
  - `docs/WIT_SCHEMA.md`

## 2) Full correctness suite

- `make phasec-check`

Expected:
- all unit/integration targets pass
- threaded lifecycle TSan target passes
- no lint regressions (`make style-check` is available for formatting audits)

## 3) Coupling-study benchmark

- `make runner-phasee-bench-run`

Record in release notes:
- `baseline_poll_public_api` avg seconds/msg-s
- `candidate_fused_storage` avg seconds/msg-s
- `speedup(candidate/baseline)`

Note:
- benchmark is informational; it does not change default runner behavior.

## 4) Recovery and dead-letter readiness

- Confirm recovery integration remains green:
  - `tests/integration/runner_recovery_integration_test.c`
- Confirm dead-letter tooling APIs still compile and pass:
  - `sap_runner_dead_letter_v0_drain(...)`
  - `sap_runner_dead_letter_v0_replay(...)`

## 5) Operational docs and knobs

- Validate docs are current:
  - `docs/RUNNER_LIFECYCLE_V0.md`
  - `docs/RUNNER_DEAD_LETTER_POLICY.md`
  - `docs/RUNNER_PHASEF_RUNBOOK.md`
- Validate policy defaults and override behavior:
  - lease TTL
  - requeue attempts
  - retry budget

## 6) Final sign-off

- Compatibility: schema + wire changes reviewed
- Reliability: retries/dead-letter/recovery checks green
- Observability: metrics/log/replay sink hooks validated
- Performance: benchmark outputs captured for this release
