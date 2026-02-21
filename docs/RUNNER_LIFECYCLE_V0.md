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
3. run message decode + callback outside write txn
4. open short write txn and delete the inbox entry if key/value still match

This keeps handler execution outside write transactions while still using inbox
records as the source of truth.

## Due-timer dispatch scaffold

`sap_runner_v0_worker_tick` now also drains due timers after inbox polling
(up to remaining `max_batch` capacity):
- DBI: `timers` (DBI 4)
- key encoding: `[due_ts:i64be][seq:u64be]` (16 bytes)
- value: serialized `LMSG` frame bytes

Due timer frames are dispatched through the same `sap_runner_v0_run_step`
path and deleted with key/value match guards.

## Worker shell

`SapRunnerV0Worker` wraps lifecycle + poll behavior for host scheduling:
- `sap_runner_v0_worker_init`: initialize runner and handler context
- `sap_runner_v0_worker_tick`: process up to `max_batch` entries across inbox
  and due timers
- `sap_runner_v0_worker_set_idle_policy`: configure max idle sleep budget
- `sap_runner_v0_worker_set_time_hooks`: optional clock/sleep hook injection
- `sap_runner_v0_worker_compute_idle_sleep_ms`: compute timer-aware idle sleep
  from next due timer and configured max idle budget
- `sap_runner_v0_worker_request_stop` / `sap_runner_v0_worker_shutdown`

Threaded helpers are also exposed:
- `sap_runner_v0_worker_start`
- `sap_runner_v0_worker_join`

These launch a polling loop only when `SAPLING_THREADED` is enabled; idle
sleep uses timer-aware budgeting from the scheduler helper. Without threaded
support they return `SAP_ERROR`.
