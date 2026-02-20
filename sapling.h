/*
 * sapling.h — public API for the Sapling copy-on-write B+ tree
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_H
#define SAPLING_H

#include <stdint.h>

/* ------------------------------------------------------------------ */
/* Error codes                                                          */
/* ------------------------------------------------------------------ */
#define SAP_OK       0   /* success                                    */
#define SAP_NOTFOUND 1   /* key not found                              */
#define SAP_ERROR    2   /* general / allocation error                 */
#define SAP_FULL     3   /* key+value too large for a single page      */
#define SAP_READONLY 4   /* write attempted on a read-only transaction */

/* ------------------------------------------------------------------ */
/* Compile-time page size (override with -DSAPLING_PAGE_SIZE=N)        */
/* ------------------------------------------------------------------ */
#ifndef SAPLING_PAGE_SIZE
#define SAPLING_PAGE_SIZE 4096u
#endif

/* ------------------------------------------------------------------ */
/* Page allocator interface                                             */
/*                                                                      */
/* The caller supplies these two callbacks so that sapling never calls  */
/* malloc/free directly.  On native builds use a thin malloc wrapper;   */
/* on Wasm use an arena/bump allocator from the runtime.                */
/* ------------------------------------------------------------------ */
typedef struct {
    void *(*alloc_page)(void *ctx, uint32_t page_size);
    void  (*free_page) (void *ctx, void *page, uint32_t page_size);
    void  *ctx;
} PageAllocator;

/* ------------------------------------------------------------------ */
/* Transaction flags                                                    */
/* ------------------------------------------------------------------ */
#define TXN_RDONLY 0x01u   /* read-only snapshot transaction           */

/* ------------------------------------------------------------------ */
/* Opaque types                                                         */
/* ------------------------------------------------------------------ */
typedef struct DB     DB;
typedef struct Txn    Txn;
typedef struct Cursor Cursor;

/* ------------------------------------------------------------------ */
/* Database lifecycle                                                   */
/* ------------------------------------------------------------------ */
DB       *db_open     (PageAllocator *alloc, uint32_t page_size);
void      db_close    (DB *db);
uint32_t  db_num_pages(DB *db);   /* total allocated pages (for diagnostics) */

/* ------------------------------------------------------------------ */
/* Transaction lifecycle                                                */
/*                                                                      */
/* Pass parent=NULL for a top-level transaction.                        */
/* Nested write transactions share the parent's working set.            */
/* A child commit makes changes visible to the parent only; nothing is  */
/* durable until the outermost transaction commits.                     */
/* A child abort discards child changes without affecting the parent.   */
/* ------------------------------------------------------------------ */
Txn *txn_begin (DB *db, Txn *parent, unsigned int flags);
int  txn_commit (Txn *txn);
void txn_abort  (Txn *txn);

/* ------------------------------------------------------------------ */
/* Key/value operations                                                 */
/* ------------------------------------------------------------------ */
int txn_get(Txn *txn,
            const void *key,  uint32_t key_len,
            const void **val_out, uint32_t *val_len_out);

int txn_put(Txn *txn,
            const void *key,  uint32_t key_len,
            const void *val,  uint32_t val_len);

int txn_del(Txn *txn,
            const void *key,  uint32_t key_len);

/* ------------------------------------------------------------------ */
/* Cursor — bidirectional ordered iteration                             */
/* ------------------------------------------------------------------ */
Cursor *cursor_open (Txn *txn);
void    cursor_close(Cursor *cursor);

/* Position cursor at first key >= key.  Returns SAP_NOTFOUND if the   */
/* tree is empty or if key is past the last entry (cursor is invalid).  */
int cursor_seek (Cursor *cursor, const void *key, uint32_t key_len);

/* Position at first / last entry. */
int cursor_first(Cursor *cursor);
int cursor_last (Cursor *cursor);

/* Advance forward / backward.  Return SAP_NOTFOUND at either end. */
int cursor_next(Cursor *cursor);
int cursor_prev(Cursor *cursor);

/* Read current entry.  Returns SAP_NOTFOUND if cursor is invalid. */
int cursor_get(Cursor *cursor,
               const void **key_out, uint32_t *key_len_out,
               const void **val_out, uint32_t *val_len_out);

#endif /* SAPLING_H */
