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

3. Drain dead-letter records for tooling workflows:
- iterates DBI 6 in key order
- decodes each record and invokes caller callback
- deletes each drained record with compare-before-delete guard

4. Replay dead-letter records back to inbox:
- loads and decodes one dead-letter record
- re-enqueues its original frame to inbox `(worker_id, replay_seq)` with
  `SAP_NOOVERWRITE`
- removes the original dead-letter record on success

## Runner integration

`runner_v0` now uses this module when:
- message frame decode fails (non-recoverable wire error)
- retryable failures exceed runner retry budget

In both cases, the message is removed from inbox flow and retained in DBI 6 for
inspection/replay tooling.

## Tooling APIs

- `sap_runner_dead_letter_v0_drain(...)`
  - callback receives `(worker_id, seq, record)` per dead-letter entry
  - callback returning non-`ERR_OK` aborts the drain and leaves remaining
    records untouched
- `sap_runner_dead_letter_v0_replay(...)`
  - requeues one dead-letter record back to inbox and removes it from DBI 6
  - returns `ERR_EXISTS` if the destination inbox key already exists
