# Runner Timer v0

This document defines the Phase-C timer scaffold:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/timer_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/timer_v0.c`

## Scope

1. Timer append:
- writes payload into DBI 4 keyed by `[due_ts:i64be][seq:u64be]`

2. Due drain:
- scans oldest timers
- processes entries with `due_ts <= now_ts`
- callback + delete-if-match dequeue

3. Intent publisher adapter:
- decodes `SapRunnerIntentV0`
- accepts `TIMER_ARM` intents with `HAS_DUE_TS`
- appends timer payload with monotonic publisher sequence

The publisher callback is compatible with `sap_runner_attempt_v0_run(...)`.

## Current limitation

`OUTBOX_EMIT` intents are rejected by this adapter. Use `outbox_v0` publisher
for outbox intents or a composed sink for mixed intent streams.
