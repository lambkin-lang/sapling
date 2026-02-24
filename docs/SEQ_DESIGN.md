# Seq — Finger-Tree Sequence Design

## Overview

`seq.h` / `seq.c` provide a mutable, heap-allocated sequence backed by a
**2-3 finger tree** parameterised by *size* (leaf-element count).  The data
structure is independent of the Sapling B+ tree and has no dependencies on
database internals.

## Complexity

| Operation          | Time              |
|--------------------|-------------------|
| `seq_push_front`   | O(1) amortised    |
| `seq_push_back`    | O(1) amortised    |
| `seq_pop_front`    | O(1) amortised    |
| `seq_pop_back`     | O(1) amortised    |
| `seq_get`          | O(log n)          |
| `seq_concat`       | O(log n)          |
| `seq_split_at`     | O(log n)          |
| `seq_length`       | O(1)              |
| `seq_free`         | O(n)              |

## Data Structure

### Node shapes

A finger tree at **item depth** *d* is one of:

```
Empty
Single(item)
Deep { prefix: Digit, mid: FingerTree(d+1), suffix: Digit }
```

A **Digit** holds 1–4 items inline (no separate allocation).

An **item** at depth *d* is:
- `d == 0` — a `uint32_t` handle (leaf element, measure = 1)
- `d  > 0` — a `SeqNode *` whose `size` field caches the total number of
  leaves beneath it (measure = `node->size`)

A `SeqNode` is a 2-ary or 3-ary heap-allocated node whose children are items
at depth *d − 1*.

### In-memory layout (`FTree`)

The prefix and suffix digits are embedded **inline** inside the `FTree`
struct to avoid extra heap allocations.  Changing tag (Empty → Single →
Deep) repurposes the same struct in place; the only additional allocation
is a fresh empty `FTree` for the middle sub-tree when transitioning
Single → Deep.

## Invariants

1. **Digit bounds:** prefix and suffix each hold 1–4 items when the tree is
   Deep.  A non-leaf tree is never Deep with an empty digit.
2. **Node arity:** internal `SeqNode` objects have arity 2 or 3.
3. **Measure cache:** `FTree::size`, `FTree::deep.pr_size`,
   `FTree::deep.sf_size`, and `SeqNode::size` are kept in sync with the
   actual leaf count at all times.
4. **Single ownership:** every `FTree *` and `SeqNode *` has exactly one
   owner.  There is no structural sharing.  `seq_free` performs a clean
   recursive free without reference counting.

## Push / Pop

Push to the front of a Deep tree:
- If prefix has < 4 items: prepend in place (O(1)).
- If prefix has 4 items: pack items 1–3 into a `Node3`, retain item 0 and
  the new item in a 2-item prefix, then recursively push the node into the
  middle tree.  Amortised O(1) because cascades are rare.

Pop is the exact reverse, replenishing an emptied prefix by popping a node
from the middle (whose children become the new prefix) or, if the middle is
empty, by stealing the first element of the suffix.

## Concatenation

`seq_concat(dest, src)` uses the standard `app3` algorithm:

```
app3(t1, middle_spine, t2, depth):
  if t1 is Empty:  push all spine items to front of t2; return t2
  if t2 is Empty:  push all spine items to back of t1; return t1
  if t1 is Single: prepend spine + t1.elem to t2; return t2
  if t2 is Single: append spine + t2.elem to t1; return t1
  Deep case:
    combined = t1.suffix ++ middle_spine ++ t2.prefix   (2–12 items)
    nodes    = pack_nodes(combined)                       (1–4 nodes)
    new_mid  = app3(t1.mid, nodes, t2.mid, depth+1)
    return Deep(t1.prefix, new_mid, t2.suffix)
```

`pack_nodes` greedily emits `Node3` objects while the count exceeds 4, then
handles the 2/3/4-item tail without leaving a remainder of 1.

Both input trees are **consumed** (one shell is reused for the result, the
other is freed).

API note: `seq_concat` requires distinct sequence objects (`dest != src`);
self-concat is rejected with `SEQ_INVALID`.  Concat also requires allocator
compatibility (`alloc_fn`, `free_fn`, and `ctx` must match exactly) so the
merged tree can be safely freed by the destination sequence.

## Split

`seq_split_at(seq, idx, &left, &right)` produces `left = [0, idx)` and
`right = [idx, n)`.

Internally `ftree_split_exact(tree, idx, depth)` returns a triple
`(left_tree, elem, right_tree)` where `elem` is the item at `depth` whose
leaf range contains `idx`:

- **Prefix case:** locate `elem` in the digit by cumulative size; build
  `left` from items before it, `right` from `deep_l(items_after, mid, sf)`.
- **Middle case:** recurse on the middle tree to find the `SeqNode` covering
  `idx`, then split that node's children to extract the exact leaf.
- **Suffix case:** symmetric to prefix case.

`deep_l` and `deep_r` handle the invariant that digits must have ≥ 1 item
by borrowing from the middle tree (or falling back to `small_items_to_tree`)
when the rebuilt prefix/suffix would otherwise be empty.

After the split, `elem` is pushed back to the front of `right` to satisfy
the `[idx, n)` contract.

## Memory Management

- Each `Seq` carries a `SeqAllocator` (`alloc_fn`, `free_fn`, `ctx`).
  `seq_new()` uses the default `malloc/free` allocator, while
  `seq_new_with_allocator()` allows runtime/Wasm-specific allocation policy.
- All `FTree` and `SeqNode` objects are allocated via the sequence allocator.
- `seq_free` recursively frees every internal node.  Elements are `uint32_t`
  values, so no per-element payload is freed.
- Operations that "consume" a tree (concat, split) either reuse the shell
  in place or free it immediately; nodes are transferred, not copied.
- There is no structural sharing, so `seq_free` never double-frees.
- `seq_split_at` creates `left`/`right` results with the same allocator as the
  source sequence, so split/concat round-trips remain allocator-compatible.
- On allocation failure `seq_push_front` / `seq_push_back` return `SEQ_OOM`.
  `seq_concat` / `seq_split_at` return `SEQ_OOM` instead of aborting; the
  involved sequence object(s) may become invalid and subsequently return
  `SEQ_INVALID`.
- `seq_is_valid()` reports whether a sequence is still usable.
- `seq_reset()` reinitializes a sequence to an empty valid state.
- For invalid sequences, cleanup/reset prioritizes safety over full reclaim;
  some internal allocations may be unrecoverable.

### Test fault injection

When compiled with `SAPLING_SEQ_TESTING`, the implementation exposes test hooks
to fail allocations deterministically (`seq_test_fail_alloc_after`,
`seq_test_clear_alloc_fail`).

## Thread Safety

None.  Do not share a `Seq *` across threads without external locking.

## Files

| File                     | Purpose                          |
|--------------------------|----------------------------------|
| `include/sapling/seq.h`  | Public API                       |
| `src/sapling/seq.c`      | Implementation                   |
| `tests/unit/test_seq.c`  | Unit tests                       |
| `docs/SEQ_DESIGN.md`     | This document                    |
