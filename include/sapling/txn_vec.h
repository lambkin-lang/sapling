/*
 * txn_vec.h — Arena-backed growable array for transaction-scoped data
 *
 * SapTxnVec replaces malloc/realloc/free patterns for arrays whose
 * lifetime is bounded by a transaction (or environment).  On growth it
 * allocates a new arena node, copies existing data, and frees the old
 * node.  All memory flows through SapMemArena, making it Wasm
 * linear-memory compatible.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_TXN_VEC_H
#define SAPLING_TXN_VEC_H

#include "sapling/arena.h"
#include "sapling/err.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    SapMemArena *arena;
    void        *data;       /* Current backing buffer (arena node) */
    uint32_t     nodeno;     /* Arena node number for freeing */
    uint32_t     elem_size;  /* Size of each element in bytes */
    uint32_t     len;        /* Number of elements currently stored */
    uint32_t     cap;        /* Capacity in number of elements */
} SapTxnVec;

/*
 * Initialize a vector with the given initial capacity.
 * If initial_cap is 0 the first push will allocate lazily.
 * Returns ERR_OK or ERR_OOM.
 */
int sap_txn_vec_init(SapTxnVec *vec, SapMemArena *arena,
                     uint32_t elem_size, uint32_t initial_cap);

/*
 * Destroy the vector, freeing its arena node.
 * The SapTxnVec struct itself is NOT freed (it is typically
 * embedded or scratch-allocated).
 */
void sap_txn_vec_destroy(SapTxnVec *vec);

/*
 * Ensure capacity for at least `needed` total elements.
 * Grows by 2x if current capacity is insufficient.
 * On ERR_OOM existing data is preserved.
 */
int sap_txn_vec_reserve(SapTxnVec *vec, uint32_t needed);

/*
 * Append one element.  `elem` points to elem_size bytes that are
 * copied into the vector.  Grows if needed.
 * Returns ERR_OK or ERR_OOM.
 */
int sap_txn_vec_push(SapTxnVec *vec, const void *elem);

/*
 * Access element at index.  Returns pointer into backing buffer,
 * or NULL if idx >= len.
 */
void *sap_txn_vec_at(const SapTxnVec *vec, uint32_t idx);

/*
 * Remove the last element (O(1)).
 * Returns ERR_OK, ERR_INVALID (NULL vec), or ERR_EMPTY.
 */
int sap_txn_vec_pop(SapTxnVec *vec);

/*
 * Remove element at index by swapping with the last element (O(1)).
 * Returns ERR_OK, ERR_INVALID (NULL vec), or ERR_RANGE.
 */
int sap_txn_vec_swap_remove(SapTxnVec *vec, uint32_t idx);

/* Inline accessors */
static inline void    *sap_txn_vec_data(const SapTxnVec *vec) { return vec ? vec->data : NULL; }
static inline uint32_t sap_txn_vec_len(const SapTxnVec *vec)  { return vec ? vec->len  : 0; }

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_TXN_VEC_H */
