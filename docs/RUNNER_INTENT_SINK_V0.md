# Runner Intent Sink v0

This document defines the composed intent-sink scaffold:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/intent_sink_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/intent_sink_v0.c`

## Purpose

Provide a single `attempt_v0` sink callback that can publish both:
- `OUTBOX_EMIT` intents to DBI 2 via `outbox_v0`
- `TIMER_ARM` intents to DBI 4 via `timer_v0`

This removes the single-kind limitation of using the outbox or timer adapter
directly as the attempt sink.

## Behavior

1. Decode one `LINT` frame.
2. Route by `intent.kind`:
- `OUTBOX_EMIT` -> outbox publisher
- `TIMER_ARM` -> timer publisher
3. Reject unknown/invalid frames with `SAP_ERROR`.
