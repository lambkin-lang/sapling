# Runner Phase F Operations Runbook

This runbook covers day-2 operations for the C host runner and Sapling-backed
coordination DB.

Scope:
- `src/runner/runner_v0.*` lifecycle/worker loop
- DBI inbox/outbox/leases/timers/retry/dead-letter flows
- reliability + replay + observability sink surfaces

## Startup

1. Ensure DB schema metadata is present and current:
- `sap_runner_v0_bootstrap_dbis(db)`
- `sap_runner_v0_ensure_schema_version(db, major, minor, bootstrap_if_missing)`

2. Initialize runner worker(s):
- `sap_runner_v0_worker_init(...)`
- apply policy knobs before start:
  - `sap_runner_v0_worker_set_policy(...)`
  - `sap_runner_v0_worker_set_idle_policy(...)`

3. Attach observability sinks before workload:
- `sap_runner_v0_set_metrics_sink(...)`
- `sap_runner_v0_set_log_sink(...)`
- optional replay tracing: `sap_runner_v0_set_replay_hook(...)`

4. Start loop:
- threaded: `sap_runner_v0_worker_start(...)` + `sap_runner_v0_worker_join(...)`
- non-threaded host loop: repeated `sap_runner_v0_worker_tick(...)`

## Health Signals

Track these metrics counters (`SapRunnerV0Metrics`):
- `step_attempts`, `step_successes`
- `retryable_failures` split by `conflict_failures`, `busy_failures`
- `non_retryable_failures`
- `requeues`, `dead_letter_moves`
- `step_latency_samples`, `step_latency_total_ms`, `step_latency_max_ms`

Track log events (`SapRunnerV0LogEvent`):
- step failure events (retryable/non-retryable)
- disposition events (requeue/dead-letter)
- worker loop errors (`SAP_RUNNER_V0_LOG_EVENT_WORKER_ERROR`)

## Common Failures and Actions

1. Busy spikes (`ERR_BUSY`)
- Confirm transient writer contention first.
- Check `busy_failures` trend and worker idle behavior.
- If sustained, reduce concurrency pressure or raise batching tolerance.

2. Conflict spikes (`ERR_CONFLICT`)
- Inspect atomic block contention patterns.
- Evaluate retry budget / lease TTL tuning:
  - `retry_budget_max`
  - `requeue_max_attempts`
  - `lease_ttl_ms`

3. Dead-letter growth
- Drain records with `sap_runner_dead_letter_v0_drain(...)`.
- Classify failure codes and retry counts.
- Replay selected records using `sap_runner_dead_letter_v0_replay(...)` only
  after validating root-cause fix.

4. Worker hard error (`last_error != ERR_OK`)
- Capture recent structured log events + replay events.
- Stop worker cleanly (`sap_runner_v0_worker_request_stop`).
- Preserve checkpoint before restart when possible.

## Recovery

Use checkpoint/restore continuity checks as operational baseline:
- `tests/integration/runner_recovery_integration_test.c`
- documented in `docs/RUNNER_RECOVERY_V0.md`

Recommended production incident flow:
1. checkpoint current state
2. restore into staging environment
3. validate inbox/dead-letter continuity and replay decisions
4. only then apply production replay/remediation steps

## Shutdown

1. Request stop (`sap_runner_v0_worker_request_stop`).
2. Join threaded worker if started.
3. Call `sap_runner_v0_worker_shutdown` / `sap_runner_v0_shutdown`.
4. Flush host sink pipelines and close DB.
