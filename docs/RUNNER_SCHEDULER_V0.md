# Runner Scheduler v0

This document defines the timer scheduling helper scaffold:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/scheduler_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/scheduler_v0.c`

## Scope

1. `sap_runner_scheduler_v0_next_due(...)`
- reads the earliest due timestamp from DBI 4 timers
- returns `SAP_NOTFOUND` when no timers are queued

2. `sap_runner_scheduler_v0_compute_sleep_ms(...)`
- computes bounded idle sleep from `(now_ts, next_due_ts, max_sleep_ms)`
- returns `0` when work is due now or overdue

This is a helper layer for wiring timer-aware sleep/wake behavior into worker
run loops.
