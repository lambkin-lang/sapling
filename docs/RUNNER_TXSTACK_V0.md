# Runner Tx Stack v0

This document defines the initial nested atomic context stack module:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/txstack_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/txstack_v0.c`

It builds on `txctx_v0` to model closed nesting:
- each `push` adds a child atomic frame
- child `commit_top` merges staged state into parent
- child `abort_top` discards child-only state

## Key behaviors

1. Closed nesting merge:
- child read-set entries merge into parent (except keys already covered by
  parent writes)
- child write-set entries coalesce into parent staged writes
- child buffered intent frames append to parent intent buffer

2. Nested read semantics (`sap_runner_txstack_v0_read_dbi`):
- read-your-write across nesting levels
- search order:
  - top->root staged writes
  - top->root cached reads
  - snapshot read recorded in current frame

3. Root commit guards:
- `sap_runner_txstack_v0_validate_root_reads`
- `sap_runner_txstack_v0_apply_root_writes`

These require exactly one open frame (`depth == 1`) so commit/apply cannot run
while child frames are still open.

## Why this exists now

This is the Phase-B building block for language-level nested `atomic` blocks:
- inner commit behaves like "merge into parent savepoint"
- inner abort rolls back only inner staged state
- durability is still controlled by the eventual outermost commit flow

Retry policy and full attempt loops are intentionally the next layer.
