#ifndef SAPLING_BEPT_H
#define SAPLING_BEPT_H

#include "sapling/txn.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Initialize the generic BEPT subsystems (64 and 128) within the given environment. */
int sap_bept_subsystem_init(SapEnv *env);

/* ------------------------------------------------------------------ */
/* Generic BEPT Interface (Word-based Keys)                           */
/* ------------------------------------------------------------------ */

/*
 * Insert a key/value pair.
 * key: Array of 32-bit words, Big Endian order (most significant word first).
 * key_len_words: Number of 32-bit words in the key (e.g., 2 for 64-bit, 4 for 128-bit).
 */
int sap_bept_put(SapTxnCtx *txn, const uint32_t *key, uint32_t key_len_words, 
                 const void *val, uint32_t val_len, unsigned flags, void **reserved_out);

/*
 * Retrieve a value.
 */
int sap_bept_get(SapTxnCtx *txn, const uint32_t *key, uint32_t key_len_words, 
                 const void **val_out, uint32_t *val_len_out);

/*
 * Delete a key.
 */
int sap_bept_del(SapTxnCtx *txn, const uint32_t *key, uint32_t key_len_words);

/*
 * Get the minimum (left-most) key in the tree.
 * key_out_buf: Buffer to write the found key into. Must be at least key_len_words * 4 bytes.
 */
int sap_bept_min(SapTxnCtx *txn, uint32_t *key_out_buf, uint32_t key_len_words, 
                 const void **val_out, uint32_t *val_len_out);

#ifdef __cplusplus
}
#endif

#endif
