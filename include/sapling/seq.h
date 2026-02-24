/*
 * seq.h — public API for the Sapling finger-tree sequence
 *
 * A finger tree parameterised by size provides:
 *   - Amortised O(1) push/pop at both ends
 *   - O(log n) concatenation
 *   - O(log n) split at an index
 *   - O(log n) random access by index
 *
 * Elements are opaque uint32_t handles.
 * seq_free() releases all internal tree nodes.
 *
 * Thread safety: none — do not share a Seq* across threads without
 * external synchronisation.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_SEQ_H
#define SAPLING_SEQ_H

#include <stddef.h>
#include <stdint.h>

/* ------------------------------------------------------------------ */
/* Return codes                                                         */
/* ------------------------------------------------------------------ */
#define SEQ_OK    0 /* success                                         */
#define SEQ_EMPTY 1 /* operation on empty sequence                     */
#define SEQ_OOM   2 /* allocation failure                              */
#define SEQ_RANGE 3 /* index out of range                              */
#define SEQ_INVALID 4 /* invalid argument / unsupported aliasing       */
/* After SEQ_OOM, a sequence may become invalid and return SEQ_INVALID. */

/* ------------------------------------------------------------------ */
/* Opaque sequence handle                                               */
/* ------------------------------------------------------------------ */
typedef struct Seq Seq;

/* ------------------------------------------------------------------ */
/* Allocator abstraction                                                */
/* ------------------------------------------------------------------ */

typedef void *(*SeqAllocFn)(void *ctx, size_t bytes);
typedef void (*SeqFreeFn)(void *ctx, void *ptr);

/*
 * Allocator contract:
 * - alloc_fn must return storage suitably aligned for any object type.
 * - alloc_fn returns NULL on allocation failure.
 * - free_fn must accept pointers previously returned by alloc_fn.
 * - ctx and allocator function pointers must remain valid for the lifetime
 *   of the sequence (including outputs produced by seq_split_at).
 */
typedef struct SeqAllocator
{
    SeqAllocFn alloc_fn;
    SeqFreeFn  free_fn;
    void      *ctx;
} SeqAllocator;

/* ------------------------------------------------------------------ */
/* Lifecycle                                                            */
/* ------------------------------------------------------------------ */

/*
 * seq_new — allocate an empty sequence.
 * Returns NULL on allocation failure.
 */
Seq *seq_new(void);

/*
 * seq_new_with_allocator — allocate an empty sequence using allocator.
 * If allocator is NULL, the default malloc/free allocator is used.
 * Sequences can only be concatenated when their allocator triples
 * (alloc_fn, free_fn, ctx) are identical.
 * Returns NULL on allocation failure or invalid allocator function pointers.
 */
Seq *seq_new_with_allocator(const SeqAllocator *allocator);

/*
 * seq_is_valid — returns 1 when seq is usable, 0 otherwise.
 * A sequence may become invalid after SEQ_OOM from mutating operations.
 */
int seq_is_valid(const Seq *seq);

/*
 * seq_free — release all internal nodes of seq and seq itself.
 * Element handles are value types; no per-element payload is freed.
 * Safe to call with NULL.
 * If seq is invalid, internal node reclamation is best-effort.
 */
void seq_free(Seq *seq);

/*
 * seq_reset — reinitialize seq to an empty valid state.
 * Returns SEQ_OK, SEQ_OOM, or SEQ_INVALID.
 * If seq is invalid, prior internal storage may not be reclaimable.
 */
int seq_reset(Seq *seq);

/* ------------------------------------------------------------------ */
/* Size                                                                 */
/* ------------------------------------------------------------------ */

/* seq_length — total number of elements; O(1). */
size_t seq_length(const Seq *seq);

/* ------------------------------------------------------------------ */
/* Push / pop — amortised O(1)                                          */
/* ------------------------------------------------------------------ */

/* seq_push_front — prepend elem to seq. Returns SEQ_OK, SEQ_OOM, or SEQ_INVALID. */
int seq_push_front(Seq *seq, uint32_t elem);

/* seq_push_back — append elem to seq. Returns SEQ_OK, SEQ_OOM, or SEQ_INVALID. */
int seq_push_back(Seq *seq, uint32_t elem);

/*
 * seq_pop_front — remove and return the first element via *out.
 * Returns SEQ_OK, SEQ_EMPTY, or SEQ_INVALID.
 */
int seq_pop_front(Seq *seq, uint32_t *out);

/*
 * seq_pop_back — remove and return the last element via *out.
 * Returns SEQ_OK, SEQ_EMPTY, or SEQ_INVALID.
 */
int seq_pop_back(Seq *seq, uint32_t *out);

/* ------------------------------------------------------------------ */
/* Concatenation — O(log n)                                             */
/* ------------------------------------------------------------------ */

/*
 * seq_concat — append all elements of *src to *dest.
 * *src is cleared (becomes empty) after this call.
 * dest and src must be distinct objects.
 * dest and src must use the same allocator (function pointers and ctx).
 * Returns SEQ_OK, SEQ_OOM, or SEQ_INVALID.
 * On SEQ_OOM either sequence may become invalid.
 */
int seq_concat(Seq *dest, Seq *src);

/* ------------------------------------------------------------------ */
/* Split — O(log n)                                                     */
/* ------------------------------------------------------------------ */

/*
 * seq_split_at — split seq at position idx.
 *
 * On success:
 *   *left_out  contains elements [0, idx)
 *   *right_out contains elements [idx, n)
 *   *seq is left empty.
 *
 * Returns SEQ_OK, SEQ_OOM, SEQ_RANGE (idx > seq_length(seq)), or SEQ_INVALID.
 * On error neither *left_out nor *right_out is modified.
 * On SEQ_OOM, seq may become invalid.
 */
int seq_split_at(Seq *seq, size_t idx, Seq **left_out, Seq **right_out);

/* ------------------------------------------------------------------ */
/* Indexing — O(log n)                                                  */
/* ------------------------------------------------------------------ */

/*
 * seq_get — store the element at position idx in *out.
 * Returns SEQ_OK, SEQ_RANGE, or SEQ_INVALID.
 */
int seq_get(const Seq *seq, size_t idx, uint32_t *out);

#ifdef SAPLING_SEQ_TESTING
/*
 * Test hooks: fail allocations deterministically.
 * n == 0 fails the next allocation, n == 1 fails the one after that, etc.
 * n < 0 disables fault injection.
 */
void seq_test_fail_alloc_after(int64_t n);
void seq_test_clear_alloc_fail(void);
#endif

#endif /* SAPLING_SEQ_H */
