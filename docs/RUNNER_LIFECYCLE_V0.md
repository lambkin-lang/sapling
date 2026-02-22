# Runner Lifecycle v0

This document defines the Phase-A lifecycle scaffold implemented by:
- `src/runner/runner_v0.h`
- `src/runner/runner_v0.c`

The goal is to lock down bootstrap and guard behavior before the full worker
loop and OCC runtime are added.

## Boot sequence

`sap_runner_v0_init` performs:
1. DBI bootstrap from generated schema metadata (`generated/wit_schema_dbis.h`)
2. schema-version guard check (or bootstrap-create, if configured)
3. transition to `RUNNING` state

If any step fails, initialization returns a non-`SAP_OK` status and the runner
remains stopped.

## DBI bootstrap

`sap_runner_v0_bootstrap_dbis` opens required DBIs using generated schema
metadata. Current behavior:
- requires DBI 0 (`app_state`) to be present in schema metadata
- opens DBIs `1..N` with default comparator and flags
- fails fast if generated DBI indices are non-contiguous

This gives a deterministic "known DB layout" precondition for Phase B/C logic.

## Schema-version guard

`sap_runner_v0_ensure_schema_version` reads a marker key in DBI 0:
- key: `runner.schema.version`
- value format (8 bytes):
  - bytes `[0..4)`: magic `RSV0`
  - bytes `[4..6)`: expected schema major (`u16`, little-endian)
  - bytes `[6..8)`: expected schema minor (`u16`, little-endian)

Behavior:
- marker present and matching: `SAP_OK`
- marker present but invalid/mismatched: `SAP_CONFLICT`
- marker missing:
  - returns `SAP_NOTFOUND` when bootstrap is disabled
  - writes marker and returns `SAP_OK` when bootstrap is enabled

## Step dispatch scaffold

`sap_runner_v0_run_step` is a single-step dispatch primitive:
1. decode one `LMSG` frame via `wire_v0`
2. enforce worker targeting (`msg.to_worker == runner.worker_id`)
3. invoke callback
4. increment `steps_completed` on successful callback

This is an intentionally small scaffold for building the later inbox-driven
dispatch loop.

## Inbox loop scaffold

`sap_runner_v0_poll_inbox` connects dispatch to DB-backed inbox records:
- DBI: `inbox` (DBI 1)
- key encoding: `[worker_id:u64be][seq:u64be]` (16 bytes)
- value: serialized `LMSG` frame bytes

Per message attempt:
1. read oldest matching worker-prefixed key under a read txn
2. copy key/frame bytes out of txn memory
3. claim the message lease in DBI 3 (short write txn; uses injected worker
   clock when running through `SapRunnerV0Worker` hooks)
4. run message decode + callback outside write txn
5. on success, ack (delete inbox + lease atomically)
6. on callback failure:
   - retryable failures are counted in DBI 5 (`dedupe`) via `retry:<message_id>`
   - when retry budget is not exhausted: requeue to tail with lease-token guard
   - when retry budget is exhausted (or frame decode fails): move to DBI 6
     dead-letter and clear inbox+lease atomically

This keeps handler execution outside write transactions while still using inbox
records as the source of truth and avoiding stale lease buildup.

Retry behavior in the scaffold:
- retryable callback errors (`SAP_BUSY`, `SAP_CONFLICT`) are requeued until
  retry budget is hit; then they are dead-lettered
- non-retryable callback errors are also requeued for durability, then returned
  to the caller so worker policy can decide whether to stop/escalate

## Due-timer dispatch scaffold

`sap_runner_v0_worker_tick` now also drains due timers after inbox polling
(up to remaining `max_batch` capacity):
- DBI: `timers` (DBI 4)
- key encoding: `[due_ts:i64be][seq:u64be]` (16 bytes)
- value: serialized `LMSG` frame bytes

Due timer frames are dispatched through the same `sap_runner_v0_run_step`
path and deleted with key/value match guards.

## Reliability counters

`SapRunnerV0` now tracks lightweight reliability metrics:
- step attempts/successes
- retryable failures split by `SAP_CONFLICT` and `SAP_BUSY`
- non-retryable failures
- requeue and dead-letter move counts
- step latency samples/total/max (millisecond resolution)

API:
- `sap_runner_v0_metrics_snapshot(...)`
- `sap_runner_v0_metrics_reset(...)`
- `sap_runner_v0_set_metrics_sink(...)`

Counters are updated by inbox and due-timer dispatch paths and are intended as
the baseline observability substrate for Phase D.
When worker time hooks are installed, inbox/timer latency samples are measured
using that injected clock source for deterministic testing.

Metrics sink behavior:
- receives synchronous snapshots after counter updates
- callback sees a copy of `SapRunnerV0Metrics` (safe to retain)
- sink should avoid blocking or re-entering runner APIs

## Log sink (optional)

`sap_runner_v0_set_log_sink(...)` installs an optional callback for structured
runner lifecycle log events:
- retryable/non-retryable step failures
- disposition outcomes (requeue/dead-letter)
- worker loop errors surfaced by `sap_runner_v0_worker_tick`/threaded loop

Each `SapRunnerV0LogEvent` includes:
- event kind
- worker id
- sequence (inbox/timer key sequence, or `0` for worker-level errors)
- status code
- detail field (`retry_count` for retry-path disposition events)

## Runner policy surface

`SapRunnerV0Policy` defines stable runtime knobs:
- `lease_ttl_ms`
- `requeue_max_attempts`
- `retry_budget_max`

APIs:
- `sap_runner_v0_policy_default(...)`
- `sap_runner_v0_set_policy(...)`
- `sap_runner_v0_worker_set_policy(...)`

This allows tuning retry/dead-letter and lease behavior without editing
internal constants.

## Deterministic replay hook (optional)

`sap_runner_v0_set_replay_hook(...)` installs an optional callback that receives
per-step event records for postmortem reconstruction:
- inbox attempt/result
- timer attempt/result
- disposition actions (requeue, dead-letter move)

Each event includes worker id, sequence (when applicable), step result code,
and frame bytes for immediate capture by the callback. Timer events now carry
the timer key sequence from DBI 4.

Replay hook frame-lifetime contract:
- `event.frame` bytes are runner-owned and callback-scoped
- hook implementations must copy frame bytes they need after callback return

## Worker shell

`SapRunnerV0Worker` wraps lifecycle + poll behavior for host scheduling:
- `sap_runner_v0_worker_init`: initialize runner and handler context
- `sap_runner_v0_worker_tick`: process up to `max_batch` entries across inbox
  and due timers
- `sap_runner_v0_worker_set_idle_policy`: configure max idle sleep budget
- `sap_runner_v0_worker_set_time_hooks`: optional clock/sleep hook injection
  used by lease timing, timer due checks, idle-sleep calculations, and
  dispatch latency timing in worker-driven paths
- `sap_runner_v0_db_gate_init` / `sap_runner_v0_db_gate_shutdown`: initialize
  optional shared DB gate mutex for threaded worker groups
- `sap_runner_v0_worker_set_db_gate`: attach a shared DB gate so worker tick
  and idle-sleep due checks serialize DB access across workers
- `sap_runner_v0_worker_compute_idle_sleep_ms`: compute timer-aware idle sleep
  from next due timer and configured max idle budget
- `sap_runner_v0_worker_request_stop` / `sap_runner_v0_worker_shutdown`

Threaded helpers are also exposed:
- `sap_runner_v0_worker_start`
- `sap_runner_v0_worker_join`

These launch a polling loop only when `SAPLING_THREADED` is enabled; idle
sleep uses timer-aware budgeting from the scheduler helper. Without threaded
support they return `SAP_ERROR`.

Worker tick transient handling:
- `SAP_BUSY` remains the canonical retryable idle signal
- `SAP_NOTFOUND` and `SAP_CONFLICT` from worker poll/timer paths are now
  normalized to `SAP_BUSY` in worker tick to avoid fatal thread exit under
  transient contention/reordering races
