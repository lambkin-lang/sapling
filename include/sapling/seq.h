/*
 * seq.h — public API for the Sapling finger-tree sequence
 *
 * A finger tree parameterised by size provides:
 *   - Amortised O(1) push/pop at both ends
 *   - O(log n) concatenation
 *   - O(log n) split at an index
 *   - O(log n) random access by index
 *
 * Elements are opaque void* pointers; the caller owns the pointed-to data.
 * seq_free() releases all internal tree nodes but never touches user data.
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

/* ------------------------------------------------------------------ */
/* Return codes                                                         */
/* ------------------------------------------------------------------ */
#define SEQ_OK    0 /* success                                         */
#define SEQ_EMPTY 1 /* operation on empty sequence                     */
#define SEQ_OOM   2 /* allocation failure                              */
#define SEQ_RANGE 3 /* index out of range                              */

/* ------------------------------------------------------------------ */
/* Opaque sequence handle                                               */
/* ------------------------------------------------------------------ */
typedef struct Seq Seq;

/* ------------------------------------------------------------------ */
/* Lifecycle                                                            */
/* ------------------------------------------------------------------ */

/*
 * seq_new — allocate an empty sequence.
 * Returns NULL on allocation failure.
 */
Seq *seq_new(void);

/*
 * seq_free — release all internal nodes of seq and seq itself.
 * User data pointers stored inside are NOT freed.
 * Safe to call with NULL.
 */
void seq_free(Seq *seq);

/* ------------------------------------------------------------------ */
/* Size                                                                 */
/* ------------------------------------------------------------------ */

/* seq_length — total number of elements; O(1). */
size_t seq_length(const Seq *seq);

/* ------------------------------------------------------------------ */
/* Push / pop — amortised O(1)                                          */
/* ------------------------------------------------------------------ */

/* seq_push_front — prepend elem to seq. Returns SEQ_OK or SEQ_OOM. */
int seq_push_front(Seq *seq, void *elem);

/* seq_push_back — append elem to seq. Returns SEQ_OK or SEQ_OOM. */
int seq_push_back(Seq *seq, void *elem);

/*
 * seq_pop_front — remove and return the first element via *out.
 * Returns SEQ_OK or SEQ_EMPTY.
 */
int seq_pop_front(Seq *seq, void **out);

/*
 * seq_pop_back — remove and return the last element via *out.
 * Returns SEQ_OK or SEQ_EMPTY.
 */
int seq_pop_back(Seq *seq, void **out);

/* ------------------------------------------------------------------ */
/* Concatenation — O(log n)                                             */
/* ------------------------------------------------------------------ */

/*
 * seq_concat — append all elements of *src to *dest.
 * *src is cleared (becomes empty) after this call.
 * Returns SEQ_OK or SEQ_OOM.
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
 * Returns SEQ_OK, SEQ_OOM, or SEQ_RANGE (idx > seq_length(seq)).
 * On error neither *left_out nor *right_out is modified.
 */
int seq_split_at(Seq *seq, size_t idx, Seq **left_out, Seq **right_out);

/* ------------------------------------------------------------------ */
/* Indexing — O(log n)                                                  */
/* ------------------------------------------------------------------ */

/*
 * seq_get — store the element at position idx in *out.
 * Returns SEQ_OK or SEQ_RANGE.
 */
int seq_get(const Seq *seq, size_t idx, void **out);

#endif /* SAPLING_SEQ_H */
