# Runner Dead-Letter Policy

This document defines operational policy for DBI 6 dead-letter records.

## When Records Enter Dead Letter

`runner_v0` moves a claimed inbox message to dead letter when:
- frame decoding fails (invalid/truncated wire format)
- retry budget is exhausted for retryable failures

Each record stores:
- `failure_rc`
- retry `attempts`
- original encoded message frame

## Recommended Triage Workflow

1. Drain records with `sap_runner_dead_letter_v0_drain(...)` into an operator
   callback that:
   - decodes the message frame
   - classifies failure cause (`wire`, `conflict budget`, `busy budget`, etc.)
   - writes an audit log entry
2. Route records by class:
   - malformed frame or schema mismatch: quarantine and investigate producer
   - transient conflict/busy exhaustion: eligible for replay
   - deterministic logic failures: hold until code fix is deployed

## Replay Guardrails

Use `sap_runner_dead_letter_v0_replay(...)` only when:
- the root cause is understood
- idempotency/dedupe semantics are in place for the message family
- destination inbox sequence is chosen to avoid collisions

Replay behavior:
- inserts original frame into inbox `(worker_id, replay_seq)` using
  `SAP_NOOVERWRITE`
- removes the dead-letter record only if inbox insert succeeds
- returns `SAP_EXISTS` when replay sequence already exists

## Retention and Observability

Minimum operational controls:
- record count alarms for DBI 6 growth rate
- periodic drain/export job for audit retention
- replay audit trail: `worker_id`, source seq, replay seq, `failure_rc`,
  attempts, and timestamp

Suggested policy:
- malformed wire records: retain for forensic window (for example, 7 days)
- retry-budget records: replay quickly after mitigation, then purge
