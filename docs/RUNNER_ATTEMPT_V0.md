# Runner Attempt v0

This document defines the initial bounded retry engine module:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/attempt_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/attempt_v0.c`

It executes one logical atomic block with OCC-style retries:
1. reset tx stack and open a root frame
2. open read snapshot and run guest/atomic callback
3. close read snapshot
4. open short write txn, validate root reads, apply root writes, commit
5. after successful commit, publish buffered intent frames via sink callback

## Retry behavior

Retryable return codes:
- `SAP_BUSY`
- `SAP_CONFLICT`

Policy knobs:
- `max_retries`
- `initial_backoff_us`
- `max_backoff_us`
- optional sleep hook (`sleep_fn`) for deterministic tests/custom schedulers

Stats reported per run:
- attempts
- retries
- conflict retries
- busy retries
- last return code

## Scope and limitations

This is a scaffold focused on deterministic control flow and bounded retries.
It intentionally does not yet include:
- jitter source plumbing/entropy policy
- durable attempt IDs and structured metrics emitters
- lease/timer mailbox integration
