/*
 * hamt.h -- public API for the HAMT (Hash Array Mapped Trie) subsystem
 *
 * A persistent, copy-on-write hash trie mapping arbitrary byte-string
 * keys to byte-string values.  Uses FNV-1a 32-bit hashing and 5-bit
 * radix branching (6 full levels + 2 bits at level 7).  Branch indexing
 * relies on __builtin_popcount which maps to Wasm i32.popcnt.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_HAMT_H
#define SAPLING_HAMT_H

#include "sapling/txn.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    /* Initialize the HAMT subsystem within the given environment. */
    int sap_hamt_subsystem_init(SapEnv *env);

    /*
     * Insert or replace a key/value pair.
     *
     * key:     Arbitrary byte string (NULL allowed only when key_len == 0).
     * key_len: Length in bytes.
     * val:     Value bytes (NULL allowed only when val_len == 0).
     * val_len: Value length in bytes.
     * flags:   SAP_NOOVERWRITE to fail with ERR_EXISTS if key already present.
     *          All other flag bits are rejected with ERR_INVALID.
     *
     * Returns ERR_OK, ERR_EXISTS, ERR_READONLY, ERR_FULL, or ERR_INVALID.
     */
    int sap_hamt_put(SapTxnCtx *txn, const void *key, uint32_t key_len, const void *val,
                     uint32_t val_len, unsigned flags);

    /*
     * Retrieve the value for a given key.
     *
     * Returns ERR_OK on success, ERR_NOT_FOUND if not present, or ERR_INVALID.
     */
    int sap_hamt_get(SapTxnCtx *txn, const void *key, uint32_t key_len, const void **val_out,
                     uint32_t *val_len_out);

    /*
     * Delete a key.
     *
     * Returns ERR_OK, ERR_NOT_FOUND, ERR_READONLY, or ERR_INVALID.
     */
    int sap_hamt_del(SapTxnCtx *txn, const void *key, uint32_t key_len);

/* ===== Test-only hash override seam ===== */
#ifdef SAPLING_HAMT_TESTING
    typedef uint32_t (*hamt_hash_fn_t)(const void *key, uint32_t len);
    void hamt_test_set_hash_fn(hamt_hash_fn_t fn);
    void hamt_test_reset_hash_fn(void);
#endif

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_HAMT_H */
