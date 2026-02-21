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
#define SAP_OK 0       /* success                                    */
#define SAP_NOTFOUND 1 /* key not found                              */
#define SAP_ERROR 2    /* general / allocation error                 */
#define SAP_FULL 3     /* key+value too large for a single page      */
#define SAP_READONLY 4 /* write attempted on a read-only transaction */
#define SAP_BUSY 5     /* write txn active or metadata change blocked */
#define SAP_EXISTS 6   /* key already exists (with SAP_NOOVERWRITE)  */
#define SAP_CONFLICT 7 /* compare-and-swap value mismatch            */

/* ------------------------------------------------------------------ */
/* Compile-time tunables                                                */
/* ------------------------------------------------------------------ */
#ifndef SAPLING_PAGE_SIZE
#define SAPLING_PAGE_SIZE 4096u
#endif

#ifndef SAP_MAX_DBI
#define SAP_MAX_DBI 32u
#endif

/* ------------------------------------------------------------------ */
/* Page allocator interface                                             */
/*                                                                      */
/* The caller supplies callbacks for page storage (tree/meta pages).     */
/* Sapling may still use malloc/free internally for temporary buffers.   */
/* On Wasm, wire page callbacks to a linear-memory allocator.            */
/* ------------------------------------------------------------------ */
typedef struct
{
    void *(*alloc_page)(void *ctx, uint32_t page_size);
    void (*free_page)(void *ctx, void *page, uint32_t page_size);
    void *ctx;
} PageAllocator;

/* ------------------------------------------------------------------ */
/* Key comparator interface                                             */
/*                                                                      */
/* If non-NULL, this comparator replaces the default memcmp ordering.   */
/* Return < 0 if a < b, 0 if a == b, > 0 if a > b.                     */
/* The comparator MUST define a total order.                             */
/* ------------------------------------------------------------------ */
typedef int (*keycmp_fn)(const void *a, uint32_t a_len, const void *b, uint32_t b_len, void *ctx);

/* ------------------------------------------------------------------ */
/* Transaction flags                                                    */
/* ------------------------------------------------------------------ */
#define TXN_RDONLY 0x01u /* read-only snapshot transaction           */

/* ------------------------------------------------------------------ */
/* Put flags (for txn_put_flags / txn_put_flags_dbi)                    */
/* ------------------------------------------------------------------ */
#define SAP_NOOVERWRITE 0x01u /* fail with SAP_EXISTS if key present */
#define SAP_RESERVE 0x02u     /* allocate space, return pointer      */

/* ------------------------------------------------------------------ */
/* Opaque types                                                         */
/* ------------------------------------------------------------------ */
typedef struct DB DB;
typedef struct Txn Txn;
typedef struct Cursor Cursor;
typedef uint32_t DBI;

typedef int (*sap_write_fn)(const void *buf, uint32_t len, void *ctx);
typedef int (*sap_read_fn)(void *buf, uint32_t len, void *ctx);
typedef void (*sap_watch_fn)(const void *key, uint32_t key_len, const void *val, uint32_t val_len,
                             void *ctx);
typedef void (*sap_merge_fn)(const void *old_val, uint32_t old_len, const void *operand,
                             uint32_t op_len, void *new_val, uint32_t *new_len, void *ctx);

/* ------------------------------------------------------------------ */
/* Database lifecycle                                                   */
/*                                                                      */
/* db_open page_size must be in [256, 65535].                           */
/* ------------------------------------------------------------------ */
DB *db_open(PageAllocator *alloc, uint32_t page_size, keycmp_fn cmp, void *cmp_ctx);
void db_close(DB *db);
uint32_t db_num_pages(DB *db); /* total allocated pages (for diagnostics) */
int db_checkpoint(DB *db, sap_write_fn writer, void *ctx);
int db_restore(DB *db, sap_read_fn reader, void *ctx);

/* ------------------------------------------------------------------ */
/* Multiple database support (integer-indexed)                          */
/*                                                                      */
/* DBI 0 is the default database (used by non-dbi functions).           */
/* dbi_open/dbi_set_dupsort require no active read or write txns;        */
/* otherwise they return SAP_BUSY.                                       */
/* ------------------------------------------------------------------ */
#define DBI_DUPSORT 0x01u /* sorted duplicate keys                    */

int dbi_open(DB *db, uint32_t dbi, keycmp_fn cmp, void *cmp_ctx, unsigned flags);
int dbi_set_dupsort(DB *db, uint32_t dbi, keycmp_fn vcmp, void *vcmp_ctx);

/* ------------------------------------------------------------------ */
/* Statistics                                                           */
/* ------------------------------------------------------------------ */
typedef struct
{
    uint64_t num_entries; /* total key-value pairs                     */
    uint64_t txnid;       /* current transaction ID                    */
    uint32_t tree_depth;  /* levels from root to leaf (0 = empty)      */
    uint32_t num_pages;   /* total allocated pages                     */
    uint32_t page_size;   /* page size in bytes                        */
    int has_write_txn;    /* 1 if a write transaction is active        */
} SapStat;

int db_stat(DB *db, SapStat *stat);    /* reports DBI 0             */
int txn_stat(Txn *txn, SapStat *stat); /* reports DBI 0             */
int dbi_stat(Txn *txn, uint32_t dbi, SapStat *stat);

/* Watch notifications (prefix match, commit-time delivery)
 * db_watch/db_unwatch target DBI 0.
 * DBI-scoped variants reject DUPSORT DBIs and return SAP_BUSY if a
 * write transaction is active.
 * Duplicate registrations return SAP_EXISTS.
 */
int db_watch(DB *db, const void *prefix, uint32_t prefix_len, sap_watch_fn cb, void *ctx);
int db_unwatch(DB *db, const void *prefix, uint32_t prefix_len, sap_watch_fn cb, void *ctx);
int db_watch_dbi(DB *db, uint32_t dbi, const void *prefix, uint32_t prefix_len, sap_watch_fn cb,
                 void *ctx);
int db_unwatch_dbi(DB *db, uint32_t dbi, const void *prefix, uint32_t prefix_len, sap_watch_fn cb,
                   void *ctx);

/* ------------------------------------------------------------------ */
/* Transaction lifecycle                                                */
/*                                                                      */
/* Pass parent=NULL for a top-level transaction.                        */
/* Nested write transactions share the parent's working set.            */
/* A child commit makes changes visible to the parent only; nothing is  */
/* durable until the outermost transaction commits.                     */
/* A child abort discards child changes without affecting the parent.   */
/* Returns NULL on OOM or if a non-nested write txn is requested while  */
/* another write txn is already active (SAP_BUSY condition).            */
/* ------------------------------------------------------------------ */
Txn *txn_begin(DB *db, Txn *parent, unsigned int flags);
int txn_commit(Txn *txn);
void txn_abort(Txn *txn);

/* ------------------------------------------------------------------ */
/* Key/value operations (default DBI 0)                                 */
/* ------------------------------------------------------------------ */
int txn_get(Txn *txn, const void *key, uint32_t key_len, const void **val_out,
            uint32_t *val_len_out);

int txn_put(Txn *txn, const void *key, uint32_t key_len, const void *val, uint32_t val_len);

int txn_put_flags(Txn *txn, const void *key, uint32_t key_len, const void *val, uint32_t val_len,
                  unsigned flags, void **reserved_out);

int txn_del(Txn *txn, const void *key, uint32_t key_len);

/* ------------------------------------------------------------------ */
/* Key/value operations (explicit DBI)                                  */
/* ------------------------------------------------------------------ */
int txn_get_dbi(Txn *txn, uint32_t dbi, const void *key, uint32_t key_len, const void **val_out,
                uint32_t *val_len_out);

int txn_put_dbi(Txn *txn, uint32_t dbi, const void *key, uint32_t key_len, const void *val,
                uint32_t val_len);

int txn_put_flags_dbi(Txn *txn, uint32_t dbi, const void *key, uint32_t key_len, const void *val,
                      uint32_t val_len, unsigned flags, void **reserved_out);

/* Compare-and-swap put: replace only if current value matches expected.
 * Returns SAP_OK, SAP_NOTFOUND, SAP_CONFLICT, SAP_READONLY, or SAP_ERROR.
 * Not supported for DUPSORT DBIs (returns SAP_ERROR).
 */
int txn_put_if(Txn *txn, uint32_t dbi, const void *key, uint32_t key_len, const void *val,
               uint32_t val_len, const void *expected_val, uint32_t expected_len);

int txn_del_dbi(Txn *txn, uint32_t dbi, const void *key, uint32_t key_len);

/* Delete a specific (key, value) pair in a DUPSORT DBI */
int txn_del_dup_dbi(Txn *txn, uint32_t dbi, const void *key, uint32_t key_len, const void *val,
                    uint32_t val_len);

/* Bulk-load sorted entries.
 * keys/vals are arrays of pointers with matching lens arrays.
 * Returns SAP_EXISTS if duplicate keys are present for a non-DUPSORT DBI.
 */
int txn_load_sorted(Txn *txn, uint32_t dbi, const void *const *keys, const uint32_t *key_lens,
                    const void *const *vals, const uint32_t *val_lens, uint32_t count);

/* Count entries in half-open key range [lo, hi) for a DBI.
 * Pass lo=NULL for unbounded lower bound, hi=NULL for unbounded upper bound.
 * In DUPSORT DBIs, duplicate values are counted as separate entries.
 */
int txn_count_range(Txn *txn, uint32_t dbi, const void *lo, uint32_t lo_len, const void *hi,
                    uint32_t hi_len, uint64_t *count_out);

/* Delete entries in half-open key range [lo, hi) for a DBI.
 * Pass lo=NULL for unbounded lower bound, hi=NULL for unbounded upper bound.
 * In DUPSORT DBIs, duplicate values are deleted as separate entries.
 */
int txn_del_range(Txn *txn, uint32_t dbi, const void *lo, uint32_t lo_len, const void *hi,
                  uint32_t hi_len, uint64_t *deleted_count_out);

/* Merge a value with callback-defined semantics.
 * Not supported for DUPSORT DBIs.
 * old_val is NULL with old_len=0 when key is missing.
 * On entry, *new_len is output capacity; callback sets produced byte count.
 * If callback reports more than inline capacity, Sapling retries once with the
 * reported size (up to UINT16_MAX), enabling overflow-value merge results.
 * Returns SAP_FULL if the callback keeps requesting larger output or exceeds
 * UINT16_MAX.
 */
int txn_merge(Txn *txn, uint32_t dbi, const void *key, uint32_t key_len, const void *operand,
              uint32_t op_len, sap_merge_fn merge, void *ctx);

/* ------------------------------------------------------------------ */
/* Cursor — bidirectional ordered iteration                             */
/* ------------------------------------------------------------------ */
Cursor *cursor_open(Txn *txn);                   /* DBI 0                */
Cursor *cursor_open_dbi(Txn *txn, uint32_t dbi); /* NULL on invalid DBI */
void cursor_close(Cursor *cursor);
int cursor_renew(Cursor *cursor, Txn *txn); /* reuse cursor object */

int cursor_seek(Cursor *cursor, const void *key, uint32_t key_len);
int cursor_first(Cursor *cursor);
int cursor_last(Cursor *cursor);
int cursor_next(Cursor *cursor);
int cursor_prev(Cursor *cursor);

int cursor_get(Cursor *cursor, const void **key_out, uint32_t *key_len_out, const void **val_out,
               uint32_t *val_len_out);

int cursor_get_key(Cursor *cursor, const void **key_out, uint32_t *key_len_out);

/* ------------------------------------------------------------------ */
/* Cursor mutations                                                     */
/* ------------------------------------------------------------------ */
int cursor_put(Cursor *cursor, const void *val, uint32_t val_len, unsigned flags);
int cursor_del(Cursor *cursor);

/* ------------------------------------------------------------------ */
/* Cursor DupSort navigation (DUPSORT DBIs only)                        */
/* ------------------------------------------------------------------ */
int cursor_next_dup(Cursor *cursor);
int cursor_prev_dup(Cursor *cursor);
int cursor_first_dup(Cursor *cursor);
int cursor_last_dup(Cursor *cursor);
int cursor_count_dup(Cursor *cursor, uint64_t *count);

/* ------------------------------------------------------------------ */
/* Prefix / range helpers                                               */
/* ------------------------------------------------------------------ */
int cursor_seek_prefix(Cursor *cursor, const void *prefix, uint32_t prefix_len);
int cursor_in_prefix(Cursor *cursor, const void *prefix, uint32_t prefix_len);

#endif /* SAPLING_H */
