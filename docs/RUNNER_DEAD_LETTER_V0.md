# Runner Dead-Letter v0

This document defines the Phase-C dead-letter helper module:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/dead_letter_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/dead_letter_v0.c`

## Scope

1. Encode/decode dead-letter records:
- magic `DLQ0`
- `failure_rc` (`s32`)
- `attempts` (`u32`)
- original frame length + bytes

2. Move claimed inbox entries to dead-letter DBI (DBI 6):
- validates exact lease token
- writes dead-letter record under the same `(worker_id, seq)` key
- deletes inbox + lease atomically

## Runner integration

`runner_v0` now uses this module when:
- message frame decode fails (non-recoverable wire error)
- retryable failures exceed runner retry budget

In both cases, the message is removed from inbox flow and retained in DBI 6 for
inspection/replay tooling.
