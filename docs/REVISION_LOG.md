# Sapling Revision Log

This document tracks completed milestones and meaningful implementation shifts.
It is intentionally closer to a technical revision history than product release
notes.

## 2026-03-03

### Allocator observability and controls completed
- Added unified allocator telemetry and budget controls across:
  - `sap_arena_alloc_page`
  - `sap_arena_alloc_node`
  - `sap_txn_scratch_alloc`
  - `SapTxnVec` reserve/growth paths
- Added env and txn snapshot surfaces with reset-relative diffs and active/high
  water gauges.
- Added budget reject counters and OOM/failure visibility for allocation paths.
- Exposed allocator telemetry in `SapRunnerV0Metrics` and runner metrics sinks.

### CI quality gates tightened
- Completed missing quality gates in CI.
- Added strict macOS sanitizer/leak-check gating.
- Added stronger compiler/sanitizer matrix coverage and verification targets.

## 2026-03-01

### Runner operational readiness milestone
- Added operations runbook (`docs/RUNNER_PHASEF_RUNBOOK.md`).
- Added release checklist (`docs/RUNNER_PHASEF_RELEASE_CHECKLIST.md`).
- Added release checklist automation entry point (`make runner-release-checklist`).

### Runner observability and debugging maturity
- Added metrics sink and structured log sink integration to runner lifecycle.
- Added deterministic replay hooks and replay-contract documentation.
- Expanded reliability counters (retry/conflict/requeue/dead-letter/latency).

## 2026-Q1 (foundational execution tranche)

### Phase 0 foundation completed
- Repository reshaped into subsystem-oriented layout (`src/sapling`,
  `src/runner`, `src/wasi`, `src/common`, `tests/*`, `docs`, `tools`).
- WIT-first schema/codegen pipeline established and wired into checks.
- Deterministic fault-injection harness scaffolding added.
- Lint/static-analysis scope expanded to the full codebase.

### Runner phases A-C completed
- Frozen v0 wire contract, schema guards, and DBI bootstrap.
- Atomic runtime scaffolding (`txctx_v0`, `txstack_v0`, `attempt_v0`).
- Mailbox lease claim/ack/requeue, outbox publish, due-timer scheduling/drain.
- Retry-budget/dead-letter routing and replay tooling.
- Threaded worker hardening and deterministic stress coverage.

### Storage and substrate milestones
- Unified arena allocation model adopted across Seq, BEPT, HAMT, Text,
  TextLiteral, TextTreeRegistry, and Thatch.
- Shared `SapEnv`/`SapTxnCtx` transaction substrate adopted by rollback-capable
  subsystems, with remaining work focused on semantic hardening and removal of
  remaining B+ tree-specific orchestration.
- TTL helper hardening completed: strict index/lookup expiry match checks,
  protected TTL metadata mode, resumable sweep checkpoints, optional lazy-expiry
  deletes, and runner background sweep cadence/metrics.
- Error taxonomy unified under `ERR_*`, with docs/examples aligned.
