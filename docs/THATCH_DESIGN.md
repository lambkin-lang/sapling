# Thatch Packed Data Subsystem — Design & Ownership Contract

## Overview

Thatch is a cursor-oriented, mostly-serialized memory model for bulk-processed
immutable trees.  All data lives in arena-backed pages (ThatchRegion) and is
accessed through zero-allocation `(region, cursor)` pairs (ThatchVal).

## Key Types

| Type | Description |
|------|-------------|
| `ThatchRegion` | Arena-backed page with a bump allocator.  Owns one data page and is itself stored as an arena node. |
| `ThatchVal` | `(const ThatchRegion *, ThatchCursor)` — a small value type.  No allocation, freely copyable. |
| `ThatchCursor` | `uint32_t` byte offset into a region's data page. |
| `ThatchTxnState` | Per-transaction linked list of active regions, allocated from txn scratch memory. |

## Ownership & Lifetime Rules

### Who Owns a Region?

1. **During a transaction** — the region is owned by the transaction's
   `ThatchTxnState.active_regions` linked list.  It can be written to,
   released, or will be cleaned up automatically on abort.

2. **After commit (root txn)** — regions are sealed (immutable) and detached
   from the transaction.  The arena continues to own the backing memory.
   ThatchVal handles pointing into the region remain valid for the lifetime
   of the arena.

3. **After commit (nested txn)** — regions are sealed and transferred to the
   parent transaction's active list.  If the parent later aborts, the
   child-committed regions are freed.  If the parent commits, they propagate
   upward (or are finalized at the root).

4. **After abort** — all regions in the aborting transaction's list are freed
   immediately (both the data page and the region node).  ThatchVal handles
   pointing into these regions become invalid.

### Explicit Release

`thatch_region_release(txn, region)` frees a region before transaction end.
This is used for error-path cleanup (e.g., failed JSON parse).

**Contract:**
- The region MUST be in the given transaction's active list.
- Releasing a region not owned by the txn returns `THATCH_INVALID` and
  performs no freeing (prevents UAF on double-release or wrong-owner).
- After release, the region pointer and any ThatchVal handles into it are
  invalid.

### Region Allocation

ThatchRegion structs are allocated from the arena (via `sap_arena_alloc_node`),
NOT from txn scratch memory.  This ensures regions survive transaction commit
and remain valid for post-commit readers.  Txn scratch memory is strictly
ephemeral — freed on both commit and abort.

## Nested Transaction Semantics

Thatch follows the standard nested transaction model used by the rest of the
Sapling transaction system:

| Event | Behavior |
|-------|----------|
| Child begin | Child gets fresh `ThatchTxnState` with `parent_state` link |
| Child commit | Seal child regions, merge (prepend) into parent's active list |
| Child abort | Free all child regions immediately |
| Parent commit (after child commit) | Seal and finalize child regions |
| Parent abort (after child commit) | Free child regions (they were merged into parent's list) |

This ensures that a child commit is tentative — the parent retains the right
to abort and roll back all child work.

## Skip Pointers

Arrays and objects use 4-byte backpatched skip pointers for O(1) subtree
bypass.  `thatch_reserve_skip` allocates the slot, `thatch_commit_skip`
backpatches the length.

**Bounds contract:** `thatch_commit_skip` validates that `skip_loc` is within
the written region and that there is room for the 4-byte slot.  Returns
`THATCH_BOUNDS` on violation.

## Sealing

`thatch_seal(txn, region)` marks a region as immutable.  Sealed regions reject
all write operations (`thatch_write_tag`, `thatch_write_data`,
`thatch_reserve_skip`, `thatch_commit_skip` all return `THATCH_INVALID`).

Commit automatically seals all regions in the committing transaction.

## Arena Active-Pages Accounting

`sap_arena_active_pages` returns `chunk_count - 1 - free_pgno_count` (the `-1`
accounts for the reserved pgno 0 slot).  Both page frees and node frees push
their slot IDs into the shared `free_pgnos` pool, so node-level allocations
(such as ThatchRegion structs) are correctly counted.  This means
`sap_arena_active_pages` reflects the total number of live slots — not just
page-sized allocations.
