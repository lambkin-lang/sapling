# Runner Outbox v0

This document defines the Phase-C outbox scaffold:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/outbox_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/outbox_v0.c`

## Scope

1. Outbox append:
- write frame bytes to DBI 2 at key `[seq:u64be]` with NOOVERWRITE guard

2. Outbox drain:
- read oldest frame in DBI 2
- invoke callback
- delete only if key/value still match (conflict-safe dequeue)

3. Intent publisher adapter:
- decodes `SapRunnerIntentV0`
- accepts `OUTBOX_EMIT` intents
- appends intent payload bytes to DBI 2 using monotonic sequence state

The publisher function is compatible with `sap_runner_attempt_v0_run(...)`
intent sink callbacks.

## Current limitation

`TIMER_ARM` intents are currently rejected by this adapter. Timer ingestion is
the next Phase-C step.
