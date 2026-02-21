/*
 * sapling.c - copy-on-write B+ tree with MVCC and nested transactions
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/sapling.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================== */
/* Threading abstraction                                                */
/*                                                                      */
/* When SAPLING_THREADED is defined, use pthreads for synchronisation.  */
/* Otherwise all locking primitives compile to no-ops, suitable for     */
/* single-threaded / Wasm builds.                                       */
/* ================================================================== */

#ifdef SAPLING_THREADED

#include <pthread.h>

typedef pthread_mutex_t sap_mutex_t;

#define SAP_MUTEX_INIT(m) pthread_mutex_init(&(m), NULL)
#define SAP_MUTEX_DESTROY(m) pthread_mutex_destroy(&(m))
#define SAP_MUTEX_LOCK(m) pthread_mutex_lock(&(m))
#define SAP_MUTEX_UNLOCK(m) pthread_mutex_unlock(&(m))

#else /* !SAPLING_THREADED */

typedef int sap_mutex_t;

#define SAP_MUTEX_INIT(m) ((void)(m))
#define SAP_MUTEX_DESTROY(m) ((void)(m))
#define SAP_MUTEX_LOCK(m) ((void)(m))
#define SAP_MUTEX_UNLOCK(m) ((void)(m))

#endif /* SAPLING_THREADED */

/* ================================================================== */
/* Constants                                                            */
/* ================================================================== */

#define SAP_MAGIC 0x53415054U
#define SAP_VERSION 2U
#define INVALID_PGNO ((uint32_t)0xFFFFFFFFU)

#define PAGE_META 0
#define PAGE_INTERNAL 1
#define PAGE_LEAF 2
#define PAGE_OVERFLOW 3
#define SNAP_MAGIC 0x53434B50U
#define SNAP_VERSION 1U

#define INT_HDR 16U
#define LEAF_HDR 10U
#define OVERFLOW_HDR 14U
#define SLOT_SZ 2U
#define ICELL_HDR 6U
#define LCELL_HDR 4U
#define OVERFLOW_VALUE_SENTINEL UINT16_MAX
#define OVERFLOW_VALUE_REF_SIZE 8U
#define MAX_DEPTH 32

/* Meta-page layout (variable-length due to per-DBI records):
 *   magic(4) version(4) txnid(8) free(4) npages(4) num_dbs(4)
 *   [root(4) nentries(8)] × num_dbs
 *   cksum(4)                                                        */
#define META_MAGIC 0
#define META_VERSION 4
#define META_TXNID 8
#define META_FREE 16
#define META_NPAGES 20
#define META_NUMDBS 24
#define META_DBS 28

static uint32_t meta_max_dbs(uint32_t page_size)
{
    uint32_t fixed = META_DBS + 4U; /* header + checksum */
    if (page_size < fixed)
        return 0;
    return (page_size - fixed) / 12U;
}

/* ================================================================== */
/* Unaligned helpers                                                    */
/* ================================================================== */

static inline uint16_t rd16(const void *p)
{
    uint16_t v;
    memcpy(&v, p, 2);
    return v;
}
static inline uint32_t rd32(const void *p)
{
    uint32_t v;
    memcpy(&v, p, 4);
    return v;
}
static inline uint64_t rd64(const void *p)
{
    uint64_t v;
    memcpy(&v, p, 8);
    return v;
}
static inline uint64_t rd64be(const void *p)
{
    const uint8_t *b = (const uint8_t *)p;
    return ((uint64_t)b[0] << 56) | ((uint64_t)b[1] << 48) | ((uint64_t)b[2] << 40) |
           ((uint64_t)b[3] << 32) | ((uint64_t)b[4] << 24) | ((uint64_t)b[5] << 16) |
           ((uint64_t)b[6] << 8) | (uint64_t)b[7];
}
static inline void wr16(void *p, uint16_t v) { memcpy(p, &v, 2); }
static inline void wr32(void *p, uint32_t v) { memcpy(p, &v, 4); }
static inline void wr64(void *p, uint64_t v) { memcpy(p, &v, 8); }
static inline void wr64be(void *p, uint64_t v)
{
    uint8_t *b = (uint8_t *)p;
    b[0] = (uint8_t)(v >> 56);
    b[1] = (uint8_t)(v >> 48);
    b[2] = (uint8_t)(v >> 40);
    b[3] = (uint8_t)(v >> 32);
    b[4] = (uint8_t)(v >> 24);
    b[5] = (uint8_t)(v >> 16);
    b[6] = (uint8_t)(v >> 8);
    b[7] = (uint8_t)v;
}

/* ================================================================== */
/* Page field macros                                                    */
/* ================================================================== */

#define PB(pg, off) ((uint8_t *)(pg) + (uint32_t)(off))

#define PG_TYPE(pg) (*(uint8_t *)PB(pg, 0))
#define PG_NUM(pg) rd16(PB(pg, 2))
#define PG_PGNO(pg) rd32(PB(pg, 4))
#define SET_PG_TYPE(pg, v) (*(uint8_t *)PB(pg, 0) = (uint8_t)(v))
#define SET_PG_NUM(pg, v) wr16(PB(pg, 2), (uint16_t)(v))
#define SET_PG_PGNO(pg, v) wr32(PB(pg, 4), (v))

#define I_LEFT(pg) rd32(PB(pg, 8))
#define I_DEND(pg) rd16(PB(pg, 12))
#define SET_I_LEFT(pg, v) wr32(PB(pg, 8), (v))
#define SET_I_DEND(pg, v) wr16(PB(pg, 12), (uint16_t)(v))
#define I_SLOT(pg, i) rd16(PB(pg, INT_HDR + (uint32_t)(i) * SLOT_SZ))
#define SET_I_SLOT(pg, i, v) wr16(PB(pg, INT_HDR + (uint32_t)(i) * SLOT_SZ), (uint16_t)(v))
#define I_CKLEN(pg, off) rd16(PB(pg, off))
#define I_CRIGHT(pg, off) rd32(PB(pg, (uint32_t)(off) + 2))
#define I_CKEY(pg, off) PB(pg, (uint32_t)(off) + ICELL_HDR)
#define SET_I_CRIGHT(pg, off, v) wr32(PB(pg, (uint32_t)(off) + 2), (v))
#define ICELL_SZ(klen) (ICELL_HDR + (uint32_t)(klen))
#define I_FREE(pg) ((uint32_t)(I_DEND(pg)) - INT_HDR - (uint32_t)PG_NUM(pg) * SLOT_SZ)

#define L_DEND(pg) rd16(PB(pg, 8))
#define SET_L_DEND(pg, v) wr16(PB(pg, 8), (uint16_t)(v))
#define L_SLOT(pg, i) rd16(PB(pg, LEAF_HDR + (uint32_t)(i) * SLOT_SZ))
#define SET_L_SLOT(pg, i, v) wr16(PB(pg, LEAF_HDR + (uint32_t)(i) * SLOT_SZ), (uint16_t)(v))
#define L_CKLEN(pg, off) rd16(PB(pg, off))
#define L_CVLEN(pg, off) rd16(PB(pg, (uint32_t)(off) + 2))
#define L_CKEY(pg, off) PB(pg, (uint32_t)(off) + LCELL_HDR)
#define L_CVAL(pg, off, kl) PB(pg, (uint32_t)(off) + LCELL_HDR + (uint32_t)(kl))
#define LCELL_SZ(kl, vl) (LCELL_HDR + (uint32_t)(kl) + (uint32_t)(vl))
#define L_FREE(pg) ((uint32_t)(L_DEND(pg)) - LEAF_HDR - (uint32_t)PG_NUM(pg) * SLOT_SZ)

#define OV_NEXT(pg) rd32(PB(pg, 8))
#define OV_DLEN(pg) rd16(PB(pg, 12))
#define SET_OV_NEXT(pg, v) wr32(PB(pg, 8), (v))
#define SET_OV_DLEN(pg, v) wr16(PB(pg, 12), (uint16_t)(v))
#define OV_DATA(pg) PB(pg, OVERFLOW_HDR)

/* ================================================================== */
/* Structures                                                           */
/* ================================================================== */

struct SubDB
{
    uint32_t root_pgno;
    uint64_t num_entries;
    keycmp_fn cmp;
    void *cmp_ctx;
    keycmp_fn vcmp;
    void *vcmp_ctx;
    unsigned flags;
};

struct WatchRec
{
    uint32_t dbi;
    uint8_t *prefix;
    uint32_t prefix_len;
    sap_watch_fn cb;
    void *ctx;
};

struct DB
{
    PageAllocator *alloc;
    uint32_t page_size;
    struct SubDB dbs[SAP_MAX_DBI];
    uint32_t num_dbs;
    void **pages;
    uint32_t pages_cap;
    void ***old_page_arrays;
    uint32_t num_old_arrays;
    uint32_t cap_old_arrays;
    uint64_t txnid;
    uint32_t free_pgno;
    uint32_t num_pages;
    struct Txn *write_txn;
    sap_mutex_t write_mutex;
    sap_mutex_t reader_mutex;
    uint64_t *active_readers;
    uint32_t num_readers;
    uint32_t cap_readers;
    struct
    {
        uint64_t freed_at;
        uint32_t pgno;
    } *deferred;
    uint32_t num_deferred;
    uint32_t cap_deferred;
    struct WatchRec *watches;
    uint32_t num_watches;
    uint32_t cap_watches;
};

struct TxnDB
{
    uint32_t root_pgno;
    uint64_t num_entries;
    uint32_t saved_root;
    uint64_t saved_entries;
};

struct ScratchSeg
{
    uint8_t *buf;
    uint32_t cap;
    uint32_t used;
    struct ScratchSeg *prev;
};

struct TxnChange
{
    uint32_t dbi;
    uint32_t key_len;
    uint32_t val_len;
    uint8_t *key;
    uint8_t *val;
};

struct TxnReadBuf
{
    uint8_t *buf;
    uint32_t len;
    uint32_t first_pgno;
    struct TxnReadBuf *next;
};

struct Txn
{
    struct DB *db;
    struct Txn *parent;
    uint64_t txnid;
    unsigned int flags;
    struct TxnDB dbs[SAP_MAX_DBI];
    uint32_t free_pgno;
    uint32_t num_pages;
    uint32_t saved_free;
    uint32_t saved_npages;
    uint32_t *new_pages;
    uint32_t new_cnt;
    uint32_t new_cap;
    uint32_t *old_pages;
    uint32_t old_cnt;
    uint32_t old_cap;
    struct TxnChange *changes;
    uint32_t change_cnt;
    uint32_t change_cap;
    struct TxnReadBuf *read_bufs;
    uint8_t track_changes;
    struct ScratchSeg *scratch_top;
};

struct Cursor
{
    struct Txn *txn;
    uint32_t dbi;
    uint32_t stack[MAX_DEPTH];
    int idx[MAX_DEPTH];
    int depth;
};

static int db_has_watch_locked(const struct DB *db, uint32_t dbi);
static void txn_free_page(struct Txn *txn, uint32_t pgno);

/* ================================================================== */
/* Sorted uint32 array helpers                                          */
/* ================================================================== */

static int u32_find(const uint32_t *a, uint32_t n, uint32_t v, uint32_t *pos)
{
    uint32_t lo = 0, hi = n;
    while (lo < hi)
    {
        uint32_t mid = lo + (hi - lo) / 2;
        if (a[mid] == v)
        {
            if (pos)
                *pos = mid;
            return 1;
        }
        if (a[mid] < v)
            lo = mid + 1;
        else
            hi = mid;
    }
    if (pos)
        *pos = lo;
    return 0;
}

static int u32_push(uint32_t **a, uint32_t *cnt, uint32_t *cap, uint32_t v)
{
    uint32_t pos;
    if (u32_find(*a, *cnt, v, &pos))
        return 0;
    if (*cnt >= *cap)
    {
        uint32_t nc = *cap ? *cap * 2 : 16;
        uint32_t *na = (uint32_t *)realloc(*a, nc * sizeof(uint32_t));
        if (!na)
            return -1;
        *a = na;
        *cap = nc;
    }
    if (*cnt > pos)
        memmove(*a + pos + 1, *a + pos, (*cnt - pos) * sizeof(uint32_t));
    (*a)[pos] = v;
    (*cnt)++;
    return 0;
}

static int u32_remove(uint32_t *a, uint32_t *cnt, uint32_t v)
{
    uint32_t pos;
    if (!u32_find(a, *cnt, v, &pos))
        return 0;
    if (*cnt - pos - 1 > 0)
        memmove(a + pos, a + pos + 1, (*cnt - pos - 1) * sizeof(uint32_t));
    (*cnt)--;
    return 1;
}

/* ================================================================== */
/* Transaction scratch allocator                                         */
/* ================================================================== */

struct ScratchMark
{
    struct ScratchSeg *seg;
    uint32_t used;
};

static void txn_scratch_pop_one(struct Txn *txn)
{
    struct ScratchSeg *seg;
    if (!txn || !txn->scratch_top)
        return;
    seg = txn->scratch_top;
    txn->scratch_top = seg->prev;
    free(seg->buf);
    free(seg);
}

static void txn_scratch_clear(struct Txn *txn)
{
    while (txn && txn->scratch_top)
        txn_scratch_pop_one(txn);
}

static struct ScratchMark txn_scratch_mark(struct Txn *txn)
{
    struct ScratchMark mark;
    mark.seg = NULL;
    mark.used = 0;
    if (!txn)
        return mark;
    mark.seg = txn->scratch_top;
    if (mark.seg)
        mark.used = mark.seg->used;
    return mark;
}

static void txn_scratch_release(struct Txn *txn, struct ScratchMark mark)
{
    if (!txn)
        return;
    while (txn->scratch_top && txn->scratch_top != mark.seg)
        txn_scratch_pop_one(txn);
    if (!mark.seg)
    {
        txn_scratch_clear(txn);
        return;
    }
    if (txn->scratch_top == mark.seg && mark.used <= txn->scratch_top->cap)
    {
        txn->scratch_top->used = mark.used;
    }
    else
    {
        txn_scratch_clear(txn);
    }
}

static void *txn_scratch_alloc(struct Txn *txn, uint32_t len)
{
    struct ScratchSeg *seg;
    uint32_t n = len ? len : 1u;
    uint32_t off;
    if (!txn)
        return NULL;

    seg = txn->scratch_top;
    if (seg)
    {
        off = (seg->used + 7u) & ~7u;
        if (off <= seg->cap && n <= seg->cap - off)
        {
            seg->used = off + n;
            return seg->buf + off;
        }
    }

    {
        uint32_t cap = (seg && seg->cap) ? seg->cap * 2u : 256u;
        if (cap < n)
            cap = n;
        seg = (struct ScratchSeg *)malloc(sizeof(*seg));
        if (!seg)
            return NULL;
        seg->buf = (uint8_t *)malloc(cap);
        if (!seg->buf)
        {
            free(seg);
            return NULL;
        }
        seg->cap = cap;
        seg->used = n;
        seg->prev = txn->scratch_top;
        txn->scratch_top = seg;
    }
    return seg->buf;
}

static uint8_t *txn_scratch_copy(struct Txn *txn, const void *src, uint32_t len)
{
    uint8_t *dst = (uint8_t *)txn_scratch_alloc(txn, len);
    if (!dst)
        return NULL;
    if (len)
        memcpy(dst, src, len);
    return dst;
}

static void txn_readbuf_clear(struct Txn *txn)
{
    struct TxnReadBuf *cur;
    if (!txn)
        return;
    cur = txn->read_bufs;
    while (cur)
    {
        struct TxnReadBuf *next = cur->next;
        free(cur->buf);
        free(cur);
        cur = next;
    }
    txn->read_bufs = NULL;
}

static int txn_readbuf_hold(struct Txn *txn, uint8_t *buf, uint32_t len, uint32_t first_pgno)
{
    struct TxnReadBuf *node;
    if (!txn || !buf)
        return -1;
    node = (struct TxnReadBuf *)malloc(sizeof(*node));
    if (!node)
        return -1;
    node->buf = buf;
    node->len = len;
    node->first_pgno = first_pgno;
    node->next = txn->read_bufs;
    txn->read_bufs = node;
    return 0;
}

static const uint8_t *txn_readbuf_find(const struct Txn *txn, uint32_t len, uint32_t first_pgno)
{
    const struct TxnReadBuf *cur;
    if (!txn)
        return NULL;
    cur = txn->read_bufs;
    while (cur)
    {
        if (cur->len == len && cur->first_pgno == first_pgno)
            return cur->buf;
        cur = cur->next;
    }
    return NULL;
}

/* DUPSORT key-only seek helper used by key-prefix operations. */
static int cursor_seek_dupsort_key(Cursor *cp, const void *key, uint32_t key_len);

/* ================================================================== */
/* Watch / change tracking helpers                                     */
/* ================================================================== */

static int key_has_prefix(const void *key, uint32_t key_len, const void *prefix,
                          uint32_t prefix_len)
{
    if (prefix_len > key_len)
        return 0;
    if (prefix_len == 0)
        return 1;
    return memcmp(key, prefix, prefix_len) == 0;
}

static void txn_changes_clear(struct Txn *txn)
{
    if (!txn || !txn->changes)
        return;
    for (uint32_t i = 0; i < txn->change_cnt; i++)
    {
        free(txn->changes[i].key);
        free(txn->changes[i].val);
    }
    free(txn->changes);
    txn->changes = NULL;
    txn->change_cnt = 0;
    txn->change_cap = 0;
}

static int txn_track_change(struct Txn *txn, uint32_t dbi, const void *key, uint32_t key_len)
{
    struct TxnChange *chg;
    uint8_t *kcopy = NULL;

    if (!txn)
        return -1;
    if (!txn->track_changes)
        return 0;
    if (dbi >= txn->db->num_dbs)
        return -1;
    if (txn->db->dbs[dbi].flags & DBI_DUPSORT)
        return 0;
    if (!key && key_len > 0)
        return -1;

    for (uint32_t i = 0; i < txn->change_cnt; i++)
    {
        const struct TxnChange *cur = &txn->changes[i];
        if (cur->dbi != dbi || cur->key_len != key_len)
            continue;
        if (key_len == 0 || memcmp(cur->key, key, key_len) == 0)
            return 0;
    }

    if (txn->change_cnt >= txn->change_cap)
    {
        uint32_t nc = txn->change_cap ? txn->change_cap * 2u : 16u;
        struct TxnChange *na = (struct TxnChange *)realloc(txn->changes, (size_t)nc * sizeof(*na));
        if (!na)
            return -1;
        txn->changes = na;
        txn->change_cap = nc;
    }

    if (key_len > 0)
    {
        kcopy = (uint8_t *)malloc(key_len);
        if (!kcopy)
            return -1;
        memcpy(kcopy, key, key_len);
    }

    chg = &txn->changes[txn->change_cnt++];
    chg->dbi = dbi;
    chg->key_len = key_len;
    chg->val_len = 0;
    chg->key = kcopy;
    chg->val = NULL;
    return 0;
}

static int txn_merge_changes(struct Txn *dst, const struct Txn *src)
{
    if (!dst || !src)
        return -1;
    for (uint32_t i = 0; i < src->change_cnt; i++)
    {
        const struct TxnChange *chg = &src->changes[i];
        if (txn_track_change(dst, chg->dbi, chg->key, chg->key_len) < 0)
            return -1;
    }
    return 0;
}

static struct WatchRec *watch_snapshot_locked(const struct DB *db, uint32_t *count_out)
{
    struct WatchRec *snap = NULL;
    uint32_t n;
    if (count_out)
        *count_out = 0;
    if (!db || !db->num_watches)
        return NULL;

    n = db->num_watches;
    snap = (struct WatchRec *)calloc(n, sizeof(*snap));
    if (!snap)
        return NULL;

    for (uint32_t i = 0; i < n; i++)
    {
        snap[i].dbi = db->watches[i].dbi;
        snap[i].prefix_len = db->watches[i].prefix_len;
        snap[i].cb = db->watches[i].cb;
        snap[i].ctx = db->watches[i].ctx;
        if (snap[i].prefix_len > 0)
        {
            snap[i].prefix = (uint8_t *)malloc(snap[i].prefix_len);
            if (!snap[i].prefix)
            {
                for (uint32_t j = 0; j < i; j++)
                    free(snap[j].prefix);
                free(snap);
                return NULL;
            }
            memcpy(snap[i].prefix, db->watches[i].prefix, snap[i].prefix_len);
        }
    }

    if (count_out)
        *count_out = n;
    return snap;
}

static void watch_snapshot_free(struct WatchRec *snap, uint32_t count)
{
    if (!snap)
        return;
    for (uint32_t i = 0; i < count; i++)
        free(snap[i].prefix);
    free(snap);
}

static void txn_notify_watchers(struct Txn *txn, const struct WatchRec *watch_snap,
                                uint32_t watch_count)
{
    Txn *rtxn;

    if (!txn || !watch_snap || watch_count == 0 || txn->change_cnt == 0)
        return;

    rtxn = txn_begin((DB *)txn->db, NULL, TXN_RDONLY);
    if (!rtxn)
        return;

    for (uint32_t i = 0; i < txn->change_cnt; i++)
    {
        const struct TxnChange *chg = &txn->changes[i];
        const void *val = NULL;
        uint32_t val_len = 0;
        int rc = txn_get_dbi(rtxn, chg->dbi, chg->key, chg->key_len, &val, &val_len);
        if (rc != SAP_OK)
        {
            val = NULL;
            val_len = 0;
        }
        for (uint32_t w = 0; w < watch_count; w++)
        {
            if (!watch_snap[w].cb)
                continue;
            if (watch_snap[w].dbi != chg->dbi)
                continue;
            if (!key_has_prefix(chg->key, chg->key_len, watch_snap[w].prefix,
                                watch_snap[w].prefix_len))
                continue;
            watch_snap[w].cb(chg->key, chg->key_len, val, val_len, watch_snap[w].ctx);
        }
    }

    txn_abort(rtxn);
}

/* ================================================================== */
/* Key comparison                                                       */
/* ================================================================== */

static int default_cmp(const void *a, uint32_t al, const void *b, uint32_t bl)
{
    uint32_t m = al < bl ? al : bl;
    int c = memcmp(a, b, (size_t)m);
    if (c)
        return c;
    return al < bl ? -1 : al > bl ? 1 : 0;
}

static int user_keycmp(const struct DB *db, uint32_t dbi, const void *a, uint32_t al, const void *b,
                       uint32_t bl)
{
    if (db->dbs[dbi].cmp)
        return db->dbs[dbi].cmp(a, al, b, bl, db->dbs[dbi].cmp_ctx);
    return default_cmp(a, al, b, bl);
}

static int user_valcmp(const struct DB *db, uint32_t dbi, const void *a, uint32_t al, const void *b,
                       uint32_t bl)
{
    if (db->dbs[dbi].vcmp)
        return db->dbs[dbi].vcmp(a, al, b, bl, db->dbs[dbi].vcmp_ctx);
    return default_cmp(a, al, b, bl);
}

static int keycmp(const struct DB *db, uint32_t dbi, const void *a, uint32_t al, const void *b,
                  uint32_t bl)
{
    if (db->dbs[dbi].flags & DBI_DUPSORT)
    {
        /* Composite keys: [key_len:4][key_data][val_data] */
        uint32_t a_kl = rd32(a), b_kl = rd32(b);
        const uint8_t *a_key = (const uint8_t *)a + 4, *b_key = (const uint8_t *)b + 4;
        int c = user_keycmp(db, dbi, a_key, a_kl, b_key, b_kl);
        if (c)
            return c;
        const uint8_t *a_val = a_key + a_kl, *b_val = b_key + b_kl;
        uint32_t a_vl = al - 4 - a_kl, b_vl = bl - 4 - b_kl;
        return user_valcmp(db, dbi, a_val, a_vl, b_val, b_vl);
    }
    return user_keycmp(db, dbi, a, al, b, bl);
}

/* ================================================================== */
/* Meta-page checksum (variable-length)                                 */
/* ================================================================== */

static uint32_t meta_cksum(const void *pg, uint32_t data_len)
{
    uint32_t s = 0;
    for (uint32_t i = 0; i < data_len; i += 4)
        s ^= rd32(PB(pg, i));
    return s ^ 0xDEADBEEFU;
}

/* ================================================================== */
/* Page initialisation                                                  */
/* ================================================================== */

static void pg_init_internal(void *pg, uint32_t pgno, uint32_t pgsz)
{
    memset(pg, 0, pgsz);
    SET_PG_TYPE(pg, PAGE_INTERNAL);
    SET_PG_PGNO(pg, pgno);
    SET_PG_NUM(pg, 0);
    SET_I_LEFT(pg, INVALID_PGNO);
    SET_I_DEND(pg, (uint16_t)pgsz);
}

static void pg_init_leaf(void *pg, uint32_t pgno, uint32_t pgsz)
{
    memset(pg, 0, pgsz);
    SET_PG_TYPE(pg, PAGE_LEAF);
    SET_PG_PGNO(pg, pgno);
    SET_PG_NUM(pg, 0);
    SET_L_DEND(pg, (uint16_t)pgsz);
}

static void pg_init_overflow(void *pg, uint32_t pgno, uint32_t pgsz)
{
    (void)pgsz;
    memset(pg, 0, pgsz);
    SET_PG_TYPE(pg, PAGE_OVERFLOW);
    SET_PG_PGNO(pg, pgno);
    SET_PG_NUM(pg, 0);
    SET_OV_NEXT(pg, INVALID_PGNO);
    SET_OV_DLEN(pg, 0);
}

/* ================================================================== */
/* Raw page allocation (no tracking)                                    */
/* ================================================================== */

static uint32_t raw_alloc(struct Txn *txn)
{
    struct DB *db = txn->db;
    if (txn->free_pgno != INVALID_PGNO)
    {
        uint32_t pgno = txn->free_pgno;
        txn->free_pgno = rd32(db->pages[pgno]);
        memset(db->pages[pgno], 0, db->page_size);
        return pgno;
    }
    uint32_t pgno = txn->num_pages;
    if (pgno >= db->pages_cap)
    {
        uint32_t nc = db->pages_cap ? db->pages_cap * 2 : 64;
        void **np = (void **)calloc(nc, sizeof(void *));
        if (!np)
            return INVALID_PGNO;
        SAP_MUTEX_LOCK(db->write_mutex);
        memcpy(np, db->pages, db->pages_cap * sizeof(void *));
        if (db->num_old_arrays >= db->cap_old_arrays)
        {
            uint32_t oc = db->cap_old_arrays ? db->cap_old_arrays * 2 : 4;
            void ***oa = (void ***)realloc(db->old_page_arrays, oc * sizeof(void **));
            if (oa)
            {
                db->old_page_arrays = oa;
                db->cap_old_arrays = oc;
            }
        }
        if (db->num_old_arrays < db->cap_old_arrays)
            db->old_page_arrays[db->num_old_arrays++] = db->pages;
        db->pages = np;
        db->pages_cap = nc;
        SAP_MUTEX_UNLOCK(db->write_mutex);
    }
    void *pg = db->alloc->alloc_page(db->alloc->ctx, db->page_size);
    if (!pg)
        return INVALID_PGNO;
    memset(pg, 0, db->page_size);
    db->pages[pgno] = pg;
    txn->num_pages++;
    return pgno;
}

static uint32_t txn_alloc(struct Txn *txn)
{
    uint32_t pgno = raw_alloc(txn);
    if (pgno == INVALID_PGNO)
        return INVALID_PGNO;
    if (u32_push(&txn->new_pages, &txn->new_cnt, &txn->new_cap, pgno) < 0)
        return INVALID_PGNO;
    return pgno;
}

/* ================================================================== */
/* Copy-on-write                                                        */
/* ================================================================== */

static uint32_t txn_cow(struct Txn *txn, uint32_t pgno)
{
    if (pgno == INVALID_PGNO)
        return INVALID_PGNO;
    if (u32_find(txn->new_pages, txn->new_cnt, pgno, NULL))
        return pgno;
    struct DB *db = txn->db;
    uint32_t np = raw_alloc(txn);
    if (np == INVALID_PGNO)
        return INVALID_PGNO;
    memcpy(db->pages[np], db->pages[pgno], db->page_size);
    SET_PG_PGNO(db->pages[np], np);
    if (u32_push(&txn->new_pages, &txn->new_cnt, &txn->new_cap, np) < 0)
        return INVALID_PGNO;
    if (u32_push(&txn->old_pages, &txn->old_cnt, &txn->old_cap, pgno) < 0)
        return INVALID_PGNO;
    return np;
}

static uint32_t leaf_value_store_len(uint16_t vlen)
{
    return (vlen == OVERFLOW_VALUE_SENTINEL) ? OVERFLOW_VALUE_REF_SIZE : (uint32_t)vlen;
}

static uint32_t leaf_cell_size(uint16_t klen, uint16_t vlen)
{
    return LCELL_HDR + (uint32_t)klen + leaf_value_store_len(vlen);
}

static int overflow_mark_chain_old(struct Txn *txn, uint32_t first_pgno)
{
    struct DB *db;
    uint32_t pgno;
    uint32_t steps = 0;

    if (!txn || first_pgno == INVALID_PGNO)
        return 0;
    db = txn->db;
    pgno = first_pgno;
    while (pgno != INVALID_PGNO)
    {
        void *pg;
        uint32_t next;
        if (pgno >= txn->num_pages)
            return -1;
        pg = db->pages[pgno];
        if (!pg || PG_TYPE(pg) != PAGE_OVERFLOW)
            return -1;
        if (u32_push(&txn->old_pages, &txn->old_cnt, &txn->old_cap, pgno) < 0)
            return -1;
        next = OV_NEXT(pg);
        pgno = next;
        steps++;
        if (steps > txn->num_pages)
            return -1;
    }
    return 0;
}

static void overflow_free_new_chain(struct Txn *txn, uint32_t first_pgno)
{
    struct DB *db;
    uint32_t pgno;
    uint32_t steps = 0;

    if (!txn || first_pgno == INVALID_PGNO)
        return;
    db = txn->db;
    pgno = first_pgno;
    while (pgno != INVALID_PGNO)
    {
        void *pg;
        uint32_t next = INVALID_PGNO;
        if (pgno >= txn->num_pages)
            break;
        pg = db->pages[pgno];
        if (!pg || PG_TYPE(pg) != PAGE_OVERFLOW)
            break;
        next = OV_NEXT(pg);
        txn_free_page(txn, pgno);
        pgno = next;
        steps++;
        if (steps > txn->num_pages)
            break;
    }
}

static int overflow_store_value(struct Txn *txn, const void *val, uint32_t val_len,
                                uint32_t *first_pgno_out)
{
    struct DB *db;
    uint32_t payload_cap;
    uint32_t first = INVALID_PGNO;
    uint32_t prev = INVALID_PGNO;
    uint32_t off = 0;

    if (!txn || !first_pgno_out)
        return -1;
    *first_pgno_out = INVALID_PGNO;
    if (val_len == 0)
        return 0;
    if (!val)
        return -1;

    db = txn->db;
    if (db->page_size <= OVERFLOW_HDR)
        return -1;
    payload_cap = db->page_size - OVERFLOW_HDR;
    if (payload_cap == 0 || payload_cap > UINT16_MAX)
        return -1;

    while (off < val_len)
    {
        uint32_t pgno = txn_alloc(txn);
        uint32_t chunk = payload_cap;
        void *pg;
        if (pgno == INVALID_PGNO)
        {
            overflow_free_new_chain(txn, first);
            return -1;
        }
        pg = db->pages[pgno];
        pg_init_overflow(pg, pgno, db->page_size);
        if (chunk > val_len - off)
            chunk = val_len - off;
        memcpy(OV_DATA(pg), (const uint8_t *)val + off, chunk);
        SET_OV_DLEN(pg, chunk);

        if (first == INVALID_PGNO)
            first = pgno;
        if (prev != INVALID_PGNO)
            SET_OV_NEXT(db->pages[prev], pgno);
        prev = pgno;
        off += chunk;
    }

    *first_pgno_out = first;
    return 0;
}

static int overflow_read_value(struct Txn *txn, const void *meta, const void **val_out,
                               uint32_t *val_len_out)
{
    struct DB *db;
    uint32_t val_len;
    uint32_t first_pgno;
    uint32_t pgno;
    const uint8_t *cached;
    uint8_t *buf;
    uint32_t copied = 0;
    uint32_t steps = 0;

    if (!txn || !meta || !val_out || !val_len_out)
        return SAP_ERROR;

    val_len = rd32(meta);
    first_pgno = rd32((const uint8_t *)meta + 4);
    pgno = first_pgno;
    if (val_len > UINT16_MAX)
        return SAP_ERROR;
    if (val_len == 0)
    {
        *val_out = "";
        *val_len_out = 0;
        return SAP_OK;
    }
    if (pgno == INVALID_PGNO)
        return SAP_ERROR;

    cached = txn_readbuf_find(txn, val_len, first_pgno);
    if (cached)
    {
        *val_out = cached;
        *val_len_out = val_len;
        return SAP_OK;
    }

    buf = (uint8_t *)malloc(val_len);
    if (!buf)
        return SAP_ERROR;
    db = txn->db;

    while (copied < val_len)
    {
        void *pg;
        uint32_t chunk;
        if (pgno == INVALID_PGNO || pgno >= txn->num_pages)
        {
            free(buf);
            return SAP_ERROR;
        }
        pg = db->pages[pgno];
        if (!pg || PG_TYPE(pg) != PAGE_OVERFLOW)
        {
            free(buf);
            return SAP_ERROR;
        }
        chunk = OV_DLEN(pg);
        if (chunk == 0 || chunk > val_len - copied)
        {
            free(buf);
            return SAP_ERROR;
        }
        memcpy(buf + copied, OV_DATA(pg), chunk);
        copied += chunk;
        pgno = OV_NEXT(pg);
        steps++;
        if (steps > txn->num_pages)
        {
            free(buf);
            return SAP_ERROR;
        }
    }
    if (pgno != INVALID_PGNO)
    {
        free(buf);
        return SAP_ERROR;
    }
    if (txn_readbuf_hold(txn, buf, val_len, first_pgno) < 0)
    {
        free(buf);
        return SAP_ERROR;
    }
    *val_out = buf;
    *val_len_out = val_len;
    return SAP_OK;
}

static int leaf_cell_mark_overflow_old(struct Txn *txn, const void *leaf_pg, uint16_t off)
{
    uint16_t vlen;
    uint16_t klen;
    const uint8_t *val_ptr;
    uint32_t logical_len;
    uint32_t first_pgno;

    if (!txn || !leaf_pg)
        return -1;
    vlen = L_CVLEN(leaf_pg, off);
    if (vlen != OVERFLOW_VALUE_SENTINEL)
        return 0;
    klen = L_CKLEN(leaf_pg, off);
    val_ptr = (const uint8_t *)L_CVAL(leaf_pg, off, klen);
    logical_len = rd32(val_ptr + 0);
    first_pgno = rd32(val_ptr + 4);
    if (logical_len == 0)
        return 0;
    if (first_pgno == INVALID_PGNO)
        return -1;
    return overflow_mark_chain_old(txn, first_pgno);
}

/* ================================================================== */
/* Deferred free-list management (MVCC GC)                              */
/* ================================================================== */

static void db_process_deferred(struct DB *db)
{
    uint64_t min_reader = UINT64_MAX;
    for (uint32_t i = 0; i < db->num_readers; i++)
        if (db->active_readers[i] < min_reader)
            min_reader = db->active_readers[i];

    uint32_t keep = 0;
    for (uint32_t i = 0; i < db->num_deferred; i++)
    {
        if (db->num_readers == 0 || db->deferred[i].freed_at < min_reader)
        {
            uint32_t p = db->deferred[i].pgno;
            wr32(db->pages[p], db->free_pgno);
            db->free_pgno = p;
        }
        else
        {
            db->deferred[keep++] = db->deferred[i];
        }
    }
    db->num_deferred = keep;
}

static int db_defer_page(struct DB *db, uint64_t freed_at, uint32_t pgno)
{
    if (db->num_deferred >= db->cap_deferred)
    {
        uint32_t nc = db->cap_deferred ? db->cap_deferred * 2 : 16;
        void *nd = realloc(db->deferred, nc * sizeof(db->deferred[0]));
        if (!nd)
            return -1;
        db->deferred = nd;
        db->cap_deferred = nc;
    }
    db->deferred[db->num_deferred].freed_at = freed_at;
    db->deferred[db->num_deferred].pgno = pgno;
    db->num_deferred++;
    return 0;
}

static void db_remove_reader(struct DB *db, uint64_t snap_txnid)
{
    SAP_MUTEX_LOCK(db->reader_mutex);
    for (uint32_t i = 0; i < db->num_readers; i++)
    {
        if (db->active_readers[i] == snap_txnid)
        {
            db->active_readers[i] = db->active_readers[--db->num_readers];
            break;
        }
    }
    SAP_MUTEX_UNLOCK(db->reader_mutex);
}

/* ================================================================== */
/* Meta-page management                                                 */
/* ================================================================== */

static void meta_write(struct DB *db)
{
    void *m0 = db->pages[0], *m1 = db->pages[1];
    uint64_t t0 = rd64(PB(m0, META_TXNID)), t1 = rd64(PB(m1, META_TXNID));
    void *dst = (t1 > t0) ? m0 : m1;
    memset(dst, 0, db->page_size);
    wr32(PB(dst, META_MAGIC), SAP_MAGIC);
    wr32(PB(dst, META_VERSION), SAP_VERSION);
    wr64(PB(dst, META_TXNID), db->txnid);
    wr32(PB(dst, META_FREE), db->free_pgno);
    wr32(PB(dst, META_NPAGES), db->num_pages);
    wr32(PB(dst, META_NUMDBS), db->num_dbs);
    uint32_t off = META_DBS;
    for (uint32_t i = 0; i < db->num_dbs; i++)
    {
        wr32(PB(dst, off), db->dbs[i].root_pgno);
        wr64(PB(dst, off + 4), db->dbs[i].num_entries);
        off += 12;
    }
    wr32(PB(dst, off), meta_cksum(dst, off));
}

static int meta_load(struct DB *db)
{
    void *m0 = db->pages[0], *m1 = db->pages[1];
    uint32_t max_dbs = meta_max_dbs(db->page_size);
    if (max_dbs > SAP_MAX_DBI)
        max_dbs = SAP_MAX_DBI;
    int ok0 = 0, ok1 = 0;
    if (rd32(PB(m0, META_MAGIC)) == SAP_MAGIC)
    {
        uint32_t nd = rd32(PB(m0, META_NUMDBS));
        uint32_t cpos = META_DBS + nd * 12;
        if (nd <= max_dbs && cpos + 4 <= db->page_size)
            ok0 = (rd32(PB(m0, cpos)) == meta_cksum(m0, cpos));
    }
    if (rd32(PB(m1, META_MAGIC)) == SAP_MAGIC)
    {
        uint32_t nd = rd32(PB(m1, META_NUMDBS));
        uint32_t cpos = META_DBS + nd * 12;
        if (nd <= max_dbs && cpos + 4 <= db->page_size)
            ok1 = (rd32(PB(m1, cpos)) == meta_cksum(m1, cpos));
    }
    void *best = NULL;
    if (ok0 && ok1)
        best = (rd64(PB(m0, META_TXNID)) >= rd64(PB(m1, META_TXNID))) ? m0 : m1;
    else if (ok0)
        best = m0;
    else if (ok1)
        best = m1;
    else
        return -1;
    db->txnid = rd64(PB(best, META_TXNID));
    db->free_pgno = rd32(PB(best, META_FREE));
    db->num_pages = rd32(PB(best, META_NPAGES));
    db->num_dbs = rd32(PB(best, META_NUMDBS));
    if (db->num_dbs == 0 || db->num_dbs > max_dbs)
        return -1;
    uint32_t off = META_DBS;
    for (uint32_t i = 0; i < db->num_dbs; i++)
    {
        db->dbs[i].root_pgno = rd32(PB(best, off));
        db->dbs[i].num_entries = rd64(PB(best, off + 4));
        off += 12;
    }
    return 0;
}

/* ================================================================== */
/* Leaf operations                                                      */
/* ================================================================== */

static int leaf_find(const struct DB *db, uint32_t dbi, const void *pg, const void *key,
                     uint32_t klen, int *found)
{
    int n = (int)PG_NUM(pg), lo = 0, hi = n - 1, pos = n;
    *found = 0;
    while (lo <= hi)
    {
        int mid = lo + (hi - lo) / 2;
        uint16_t off = (uint16_t)L_SLOT(pg, mid);
        int cmp = keycmp(db, dbi, L_CKEY(pg, off), L_CKLEN(pg, off), key, klen);
        if (cmp == 0)
        {
            *found = 1;
            return mid;
        }
        if (cmp > 0)
        {
            pos = mid;
            hi = mid - 1;
        }
        else
            lo = mid + 1;
    }
    return pos;
}

static int leaf_insert(void *pg, int pos, const void *key, uint16_t klen, const void *val,
                       uint16_t vlen, void **val_out)
{
    uint32_t store_vlen = leaf_value_store_len(vlen);
    uint32_t need = SLOT_SZ + leaf_cell_size(klen, vlen);
    if (need > L_FREE(pg))
        return -1;
    uint16_t dend = (uint16_t)L_DEND(pg);
    uint16_t coff = (uint16_t)(dend - leaf_cell_size(klen, vlen));
    wr16(PB(pg, coff), klen);
    wr16(PB(pg, coff + 2), vlen);
    memcpy(PB(pg, coff + LCELL_HDR), key, klen);
    if (val_out)
    {
        memset(PB(pg, coff + LCELL_HDR + klen), 0, store_vlen);
        *val_out = PB(pg, coff + LCELL_HDR + klen);
    }
    else
    {
        memcpy(PB(pg, coff + LCELL_HDR + klen), val, store_vlen);
    }
    SET_L_DEND(pg, coff);
    int n = (int)PG_NUM(pg);
    if (n > pos)
        memmove(PB(pg, LEAF_HDR + (uint32_t)(pos + 1) * SLOT_SZ),
                PB(pg, LEAF_HDR + (uint32_t)pos * SLOT_SZ), (uint32_t)(n - pos) * SLOT_SZ);
    SET_L_SLOT(pg, pos, coff);
    SET_PG_NUM(pg, (uint16_t)(n + 1));
    return 0;
}

static void leaf_remove(void *pg, int pos)
{
    int n = (int)PG_NUM(pg);
    uint16_t off = (uint16_t)L_SLOT(pg, pos);
    uint32_t csz = leaf_cell_size(L_CKLEN(pg, off), L_CVLEN(pg, off));
    uint16_t dend = (uint16_t)L_DEND(pg);
    if (off > dend)
        memmove(PB(pg, dend + csz), PB(pg, dend), (uint32_t)(off - dend));
    for (int i = 0; i < n; i++)
    {
        if (i == pos)
            continue;
        uint16_t s = (uint16_t)L_SLOT(pg, i);
        if (s >= dend && s < off)
            SET_L_SLOT(pg, i, (uint16_t)(s + csz));
    }
    SET_L_DEND(pg, (uint16_t)(dend + csz));
    memmove(PB(pg, LEAF_HDR + (uint32_t)pos * SLOT_SZ),
            PB(pg, LEAF_HDR + (uint32_t)(pos + 1) * SLOT_SZ), (uint32_t)(n - pos - 1) * SLOT_SZ);
    SET_PG_NUM(pg, (uint16_t)(n - 1));
}

/* ================================================================== */
/* Internal node operations                                             */
/* ================================================================== */

static int int_find_child(const struct DB *db, uint32_t dbi, const void *pg, const void *key,
                          uint32_t klen)
{
    int n = (int)PG_NUM(pg), lo = 0, hi = n - 1, idx = n;
    while (lo <= hi)
    {
        int mid = lo + (hi - lo) / 2;
        uint16_t off = (uint16_t)I_SLOT(pg, mid);
        int cmp = keycmp(db, dbi, I_CKEY(pg, off), I_CKLEN(pg, off), key, klen);
        if (cmp > 0)
        {
            idx = mid;
            hi = mid - 1;
        }
        else
            lo = mid + 1;
    }
    return idx;
}

static uint32_t int_child(const void *pg, int idx)
{
    if (idx == 0)
        return I_LEFT(pg);
    return I_CRIGHT(pg, I_SLOT(pg, idx - 1));
}

static int int_insert(void *pg, int pos, const void *key, uint16_t klen, uint32_t right_child)
{
    uint32_t need = SLOT_SZ + ICELL_SZ(klen);
    if (need > I_FREE(pg))
        return -1;
    uint16_t dend = (uint16_t)I_DEND(pg);
    uint16_t coff = (uint16_t)(dend - ICELL_SZ(klen));
    wr16(PB(pg, coff), klen);
    wr32(PB(pg, coff + 2), right_child);
    memcpy(PB(pg, coff + ICELL_HDR), key, klen);
    SET_I_DEND(pg, coff);
    int n = (int)PG_NUM(pg);
    if (n > pos)
        memmove(PB(pg, INT_HDR + (uint32_t)(pos + 1) * SLOT_SZ),
                PB(pg, INT_HDR + (uint32_t)pos * SLOT_SZ), (uint32_t)(n - pos) * SLOT_SZ);
    SET_I_SLOT(pg, pos, coff);
    SET_PG_NUM(pg, (uint16_t)(n + 1));
    return 0;
}

static void int_remove_child(void *pg, int child_idx)
{
    int slot_idx = (child_idx == 0) ? 0 : child_idx - 1;
    int n = (int)PG_NUM(pg);
    if (child_idx == 0)
    {
        uint16_t off = (uint16_t)I_SLOT(pg, 0);
        SET_I_LEFT(pg, I_CRIGHT(pg, off));
    }
    uint16_t off = (uint16_t)I_SLOT(pg, slot_idx);
    uint32_t csz = ICELL_SZ(I_CKLEN(pg, off));
    uint16_t dend = (uint16_t)I_DEND(pg);
    if (off > dend)
        memmove(PB(pg, dend + csz), PB(pg, dend), (uint32_t)(off - dend));
    for (int i = 0; i < n; i++)
    {
        if (i == slot_idx)
            continue;
        uint16_t s = (uint16_t)I_SLOT(pg, i);
        if (s >= dend && s < off)
            SET_I_SLOT(pg, i, (uint16_t)(s + csz));
    }
    SET_I_DEND(pg, (uint16_t)(dend + csz));
    memmove(PB(pg, INT_HDR + (uint32_t)slot_idx * SLOT_SZ),
            PB(pg, INT_HDR + (uint32_t)(slot_idx + 1) * SLOT_SZ),
            (uint32_t)(n - slot_idx - 1) * SLOT_SZ);
    SET_PG_NUM(pg, (uint16_t)(n - 1));
}

/* ================================================================== */
/* Leaf split                                                           */
/* ================================================================== */

static uint32_t leaf_split(struct Txn *txn, uint32_t dbi, uint32_t lpgno, void *lpg,
                           const void *key, uint16_t klen, const void *val, uint16_t vlen,
                           uint8_t *sep_buf, uint16_t *sep_klen_out)
{
    struct DB *db = txn->db;
    int n = (int)PG_NUM(lpg);
    int total = n + 1;
    struct ScratchMark scratch_mark = txn_scratch_mark(txn);

    typedef struct
    {
        const uint8_t *k;
        uint16_t kl;
        const uint8_t *v;
        uint16_t vl;
    } KV;
    KV *all = (KV *)txn_scratch_alloc(txn, (uint32_t)total * sizeof(KV));
    if (!all)
        return INVALID_PGNO;

    int found, ins = leaf_find(db, dbi, lpg, key, klen, &found);
    for (int j = 0, i = 0; j < total; j++)
    {
        if (j == ins)
        {
            all[j].k = (const uint8_t *)key;
            all[j].kl = klen;
            all[j].v = (const uint8_t *)val;
            all[j].vl = vlen;
        }
        else
        {
            uint16_t off = (uint16_t)L_SLOT(lpg, i);
            all[j].k = L_CKEY(lpg, off);
            all[j].kl = L_CKLEN(lpg, off);
            all[j].v = L_CVAL(lpg, off, all[j].kl);
            all[j].vl = L_CVLEN(lpg, off);
            i++;
        }
    }

    uint8_t *kbuf = (uint8_t *)txn_scratch_alloc(txn, (uint32_t)total * db->page_size);
    uint8_t *vbuf = (uint8_t *)txn_scratch_alloc(txn, (uint32_t)total * db->page_size);
    uint32_t *koff = (uint32_t *)txn_scratch_alloc(txn, (uint32_t)total * sizeof(uint32_t));
    uint32_t *voff = (uint32_t *)txn_scratch_alloc(txn, (uint32_t)total * sizeof(uint32_t));
    uint32_t *kl2 = (uint32_t *)txn_scratch_alloc(txn, (uint32_t)total * sizeof(uint32_t));
    uint32_t *vl2 = (uint32_t *)txn_scratch_alloc(txn, (uint32_t)total * sizeof(uint32_t));
    if (!kbuf || !vbuf || !koff || !voff || !kl2 || !vl2)
    {
        txn_scratch_release(txn, scratch_mark);
        return INVALID_PGNO;
    }
    uint32_t ko = 0, vo = 0;
    for (int j = 0; j < total; j++)
    {
        uint32_t store_vlen = leaf_value_store_len(all[j].vl);
        kl2[j] = all[j].kl;
        vl2[j] = all[j].vl;
        koff[j] = ko;
        voff[j] = vo;
        memcpy(kbuf + ko, all[j].k, all[j].kl);
        ko += all[j].kl;
        memcpy(vbuf + vo, all[j].v, store_vlen);
        vo += store_vlen;
    }
    int left_n = total / 2;

    uint32_t rpgno = txn_alloc(txn);
    if (rpgno == INVALID_PGNO)
    {
        txn_scratch_release(txn, scratch_mark);
        return INVALID_PGNO;
    }
    void *rpg = db->pages[rpgno];

    pg_init_leaf(rpg, rpgno, db->page_size);
    pg_init_leaf(lpg, lpgno, db->page_size);

    for (int j = 0; j < total; j++)
    {
        void *dst = (j < left_n) ? lpg : rpg;
        int dpos = (j < left_n) ? j : j - left_n;
        leaf_insert(dst, dpos, kbuf + koff[j], (uint16_t)kl2[j], vbuf + voff[j], (uint16_t)vl2[j],
                    NULL);
    }

    uint16_t sep_off = (uint16_t)L_SLOT(rpg, 0);
    uint16_t sk = L_CKLEN(rpg, sep_off);
    memcpy(sep_buf, L_CKEY(rpg, sep_off), sk);
    *sep_klen_out = sk;

    txn_scratch_release(txn, scratch_mark);
    return rpgno;
}

/* ================================================================== */
/* Internal node split                                                  */
/* ================================================================== */

static uint32_t int_split(struct Txn *txn, uint32_t lpgno, void *lpg, int ins_pos, const void *key,
                          uint16_t klen, uint32_t right_child, uint8_t *sep_buf,
                          uint16_t *sep_klen_out)
{
    struct DB *db = txn->db;
    int n = (int)PG_NUM(lpg);
    int total = n + 1;
    struct ScratchMark scratch_mark = txn_scratch_mark(txn);

    uint16_t *ckl = (uint16_t *)txn_scratch_alloc(txn, (uint32_t)total * sizeof(uint16_t));
    uint32_t *crc = (uint32_t *)txn_scratch_alloc(txn, (uint32_t)total * sizeof(uint32_t));
    uint8_t *kb = (uint8_t *)txn_scratch_alloc(txn, (uint32_t)total * db->page_size);
    uint32_t *ko = (uint32_t *)txn_scratch_alloc(txn, (uint32_t)total * sizeof(uint32_t));
    if (!ckl || !crc || !kb || !ko)
    {
        txn_scratch_release(txn, scratch_mark);
        return INVALID_PGNO;
    }

    uint32_t kboff = 0;
    for (int j = 0, i = 0; j < total; j++)
    {
        if (j == ins_pos)
        {
            ckl[j] = klen;
            crc[j] = right_child;
            memcpy(kb + kboff, key, klen);
            ko[j] = kboff;
            kboff += klen;
        }
        else
        {
            uint16_t off = (uint16_t)I_SLOT(lpg, i);
            ckl[j] = I_CKLEN(lpg, off);
            crc[j] = I_CRIGHT(lpg, off);
            memcpy(kb + kboff, I_CKEY(lpg, off), ckl[j]);
            ko[j] = kboff;
            kboff += ckl[j];
            i++;
        }
    }

    int mid = total / 2;

    *sep_klen_out = ckl[mid];
    memcpy(sep_buf, kb + ko[mid], ckl[mid]);

    uint32_t right_lc;
    int rpos = mid + 1;
    if (rpos == 0)
        right_lc = I_LEFT(lpg);
    else
    {
        int ri = rpos - 1;
        if (ri < ins_pos)
            right_lc = I_CRIGHT(lpg, I_SLOT(lpg, ri));
        else if (ri == ins_pos)
            right_lc = right_child;
        else
            right_lc = I_CRIGHT(lpg, I_SLOT(lpg, ri - 1));
    }

    uint32_t rpgno = txn_alloc(txn);
    if (rpgno == INVALID_PGNO)
    {
        txn_scratch_release(txn, scratch_mark);
        return INVALID_PGNO;
    }
    void *rpg = db->pages[rpgno];
    uint32_t old_left = I_LEFT(lpg);
    pg_init_internal(rpg, rpgno, db->page_size);
    pg_init_internal(lpg, lpgno, db->page_size);
    SET_I_LEFT(lpg, old_left);
    SET_I_LEFT(rpg, right_lc);

    for (int j = 0; j < total; j++)
    {
        if (j == mid)
            continue;
        if (j < mid)
            int_insert(lpg, j, kb + ko[j], ckl[j], crc[j]);
        else
            int_insert(rpg, j - mid - 1, kb + ko[j], ckl[j], crc[j]);
    }

    txn_scratch_release(txn, scratch_mark);
    return rpgno;
}

/* ================================================================== */
/* txn_put                                                              */
/* ================================================================== */

int txn_put_flags_dbi(Txn *txn_pub, uint32_t dbi, const void *key, uint32_t key_len,
                      const void *val, uint32_t val_len, unsigned flags, void **reserved_out)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    struct ScratchMark scratch_mark = txn_scratch_mark(txn);
    const void *watch_key = key;
    uint32_t watch_key_len = key_len;
    const void *store_val = val;
    uint16_t store_vlen = 0;
    int changed = 0;
    int new_overflow_linked = 0;
    uint32_t new_overflow_head = INVALID_PGNO;
    uint8_t overflow_ref[OVERFLOW_VALUE_REF_SIZE];
    if (txn->flags & TXN_RDONLY)
        return SAP_READONLY;
    struct DB *db = txn->db;
    if (dbi >= db->num_dbs)
        return SAP_ERROR;

    /* DUPSORT: encode composite key [key_len:4][key][val], empty value */
    uint8_t *comp_buf = NULL;
    uint8_t *sep_buf = NULL;
    uint8_t *psep_buf = NULL;
    int is_dupsort = (db->dbs[dbi].flags & DBI_DUPSORT) != 0;
    if (is_dupsort)
    {
        if (flags & SAP_RESERVE)
            return SAP_ERROR; /* incompatible */
        uint32_t comp_len = 4 + key_len + val_len;
        if (comp_len > UINT16_MAX)
            return SAP_FULL;
        comp_buf = (uint8_t *)txn_scratch_alloc(txn, comp_len);
        if (!comp_buf)
            return SAP_ERROR;
        wr32(comp_buf, key_len);
        memcpy(comp_buf + 4, key, key_len);
        memcpy(comp_buf + 4 + key_len, val, val_len);
        key = comp_buf;
        key_len = comp_len;
        val = "";
        val_len = 0;
        flags |= SAP_NOOVERWRITE; /* exact dup rejection */
    }

    if (key_len > UINT16_MAX || val_len > UINT16_MAX)
    {
        txn_scratch_release(txn, scratch_mark);
        return SAP_FULL;
    }
    store_vlen = (uint16_t)val_len;
    void **rout = (flags & SAP_RESERVE) ? reserved_out : NULL;

    if (!is_dupsort)
    {
        uint32_t inline_need = SLOT_SZ + leaf_cell_size((uint16_t)key_len, store_vlen) + LEAF_HDR;
        if (inline_need > db->page_size)
        {
            if (rout)
            {
                txn_scratch_release(txn, scratch_mark);
                return SAP_ERROR;
            }
            if (SLOT_SZ + leaf_cell_size((uint16_t)key_len, OVERFLOW_VALUE_SENTINEL) + LEAF_HDR >
                db->page_size)
            {
                txn_scratch_release(txn, scratch_mark);
                return SAP_FULL;
            }
            if (overflow_store_value(txn, val, val_len, &new_overflow_head) < 0)
            {
                txn_scratch_release(txn, scratch_mark);
                return SAP_ERROR;
            }
            wr32(overflow_ref + 0, val_len);
            wr32(overflow_ref + 4, new_overflow_head);
            store_val = overflow_ref;
            store_vlen = OVERFLOW_VALUE_SENTINEL;
        }
    }
    else if (SLOT_SZ + leaf_cell_size((uint16_t)key_len, store_vlen) + LEAF_HDR > db->page_size)
    {
        txn_scratch_release(txn, scratch_mark);
        return SAP_FULL;
    }

    int rc;

    if (txn->dbs[dbi].root_pgno == INVALID_PGNO)
    {
        uint32_t pgno = txn_alloc(txn);
        if (pgno == INVALID_PGNO)
        {
            rc = SAP_ERROR;
            goto cleanup;
        }
        pg_init_leaf(db->pages[pgno], pgno, db->page_size);
        if (leaf_insert(db->pages[pgno], 0, key, (uint16_t)key_len, store_val, store_vlen, rout) <
            0)
        {
            rc = SAP_ERROR;
            goto cleanup;
        }
        new_overflow_linked = 1;
        txn->dbs[dbi].root_pgno = pgno;
        txn->dbs[dbi].num_entries++;
        changed = 1;
        rc = SAP_OK;
        goto cleanup;
    }

    /* Collect path */
    {
        uint32_t path[MAX_DEPTH];
        int path_idx[MAX_DEPTH];
        int depth = 0;
        uint32_t pgno = txn->dbs[dbi].root_pgno;
        while (PG_TYPE(db->pages[pgno]) == PAGE_INTERNAL)
        {
            void *pg = db->pages[pgno];
            int idx = int_find_child(db, dbi, pg, key, key_len);
            if (depth >= MAX_DEPTH - 1)
            {
                rc = SAP_ERROR;
                goto cleanup;
            }
            path[depth] = pgno;
            path_idx[depth] = idx;
            depth++;
            pgno = int_child(pg, idx);
        }

        /* NOOVERWRITE: check on pre-COW leaf */
        {
            void *pre_lpg = db->pages[pgno];
            int pre_found;
            leaf_find(db, dbi, pre_lpg, key, key_len, &pre_found);
            if (pre_found && (flags & SAP_NOOVERWRITE))
            {
                rc = SAP_EXISTS;
                goto cleanup;
            }
        }

        /* COW leaf */
        uint32_t leaf_pgno = txn_cow(txn, pgno);
        if (leaf_pgno == INVALID_PGNO)
        {
            rc = SAP_ERROR;
            goto cleanup;
        }

        /* COW ancestors and update child references bottom-up */
        if (depth > 0)
        {
            uint32_t child = leaf_pgno;
            for (int d = depth - 1; d >= 0; d--)
            {
                uint32_t pp = txn_cow(txn, path[d]);
                if (pp == INVALID_PGNO)
                {
                    rc = SAP_ERROR;
                    goto cleanup;
                }
                path[d] = pp;
                void *par = db->pages[pp];
                int ci = path_idx[d];
                if (ci == 0)
                    SET_I_LEFT(par, child);
                else
                    SET_I_CRIGHT(par, I_SLOT(par, ci - 1), child);
                child = pp;
            }
            txn->dbs[dbi].root_pgno = path[0];
        }
        else
        {
            txn->dbs[dbi].root_pgno = leaf_pgno;
        }

        void *lpg = db->pages[leaf_pgno];

        /* Update existing key by remove+reinsert */
        int found;
        int pos = leaf_find(db, dbi, lpg, key, key_len, &found);
        int is_update = found;
        if (found)
        {
            uint16_t old_off = (uint16_t)L_SLOT(lpg, pos);
            if (leaf_cell_mark_overflow_old(txn, lpg, old_off) < 0)
            {
                rc = SAP_ERROR;
                goto cleanup;
            }
            leaf_remove(lpg, pos);
            pos = leaf_find(db, dbi, lpg, key, key_len, &found);
        }

        if (leaf_insert(lpg, pos, key, (uint16_t)key_len, store_val, store_vlen, rout) == 0)
        {
            if (!is_update)
                txn->dbs[dbi].num_entries++;
            new_overflow_linked = 1;
            changed = 1;
            rc = SAP_OK;
            goto cleanup;
        }

        /* Leaf full: split */
        sep_buf = (uint8_t *)txn_scratch_alloc(txn, db->page_size);
        if (!sep_buf)
        {
            rc = SAP_ERROR;
            goto cleanup;
        }
        uint16_t sep_klen;
        uint32_t rpgno = leaf_split(txn, dbi, leaf_pgno, lpg, key, (uint16_t)key_len, store_val,
                                    store_vlen, sep_buf, &sep_klen);
        if (rpgno == INVALID_PGNO)
        {
            rc = SAP_ERROR;
            goto cleanup;
        }
        if (!is_update)
            txn->dbs[dbi].num_entries++;
        new_overflow_linked = 1;

        /* For RESERVE: locate the entry in the result pages */
        if (rout)
        {
            int f;
            int p = leaf_find(db, dbi, db->pages[leaf_pgno], key, key_len, &f);
            if (!f)
            {
                p = leaf_find(db, dbi, db->pages[rpgno], key, key_len, &f);
                lpg = db->pages[rpgno];
            }
            else
            {
                lpg = db->pages[leaf_pgno];
            }
            if (f)
            {
                uint16_t off = (uint16_t)L_SLOT(lpg, p);
                *rout = L_CVAL(lpg, off, L_CKLEN(lpg, off));
            }
        }

        uint32_t left_pgno = leaf_pgno, right_pgno = rpgno;
        const void *sep_key = sep_buf;

        /* Propagate split up the path */
        for (int d = depth - 1; d >= 0; d--)
        {
            uint32_t par_pgno = path[d];
            void *par = db->pages[par_pgno];
            int ins_pos = path_idx[d];
            if (int_insert(par, ins_pos, sep_key, sep_klen, right_pgno) == 0)
            {
                rc = SAP_OK;
                goto cleanup;
            }
            if (!psep_buf)
            {
                psep_buf = (uint8_t *)txn_scratch_alloc(txn, db->page_size);
                if (!psep_buf)
                {
                    rc = SAP_ERROR;
                    goto cleanup;
                }
            }
            uint16_t psep_klen;
            uint32_t nr = int_split(txn, par_pgno, par, ins_pos, sep_key, sep_klen, right_pgno,
                                    psep_buf, &psep_klen);
            if (nr == INVALID_PGNO)
            {
                rc = SAP_ERROR;
                goto cleanup;
            }
            memcpy(sep_buf, psep_buf, psep_klen);
            sep_klen = psep_klen;
            sep_key = sep_buf;
            left_pgno = par_pgno;
            right_pgno = nr;
        }

        /* Root split: new root */
        uint32_t new_root = txn_alloc(txn);
        if (new_root == INVALID_PGNO)
        {
            rc = SAP_ERROR;
            goto cleanup;
        }
        void *nrp = db->pages[new_root];
        pg_init_internal(nrp, new_root, db->page_size);
        SET_I_LEFT(nrp, left_pgno);
        int_insert(nrp, 0, sep_key, sep_klen, right_pgno);
        txn->dbs[dbi].root_pgno = new_root;
        changed = 1;
        rc = SAP_OK;
    } /* end of path block */

cleanup:
    if (!new_overflow_linked && new_overflow_head != INVALID_PGNO)
        overflow_free_new_chain(txn, new_overflow_head);
    txn_scratch_release(txn, scratch_mark);
    if (changed)
        (void)txn_track_change(txn, dbi, watch_key, watch_key_len);
    /* For DUPSORT: SAP_EXISTS means exact dup already present → success */
    if (is_dupsort && rc == SAP_EXISTS)
        rc = SAP_OK;
    return rc;
}

int txn_put_flags(Txn *txn, const void *key, uint32_t key_len, const void *val, uint32_t val_len,
                  unsigned flags, void **reserved_out)
{
    return txn_put_flags_dbi(txn, 0, key, key_len, val, val_len, flags, reserved_out);
}

int txn_put(Txn *txn, const void *key, uint32_t key_len, const void *val, uint32_t val_len)
{
    return txn_put_flags_dbi(txn, 0, key, key_len, val, val_len, 0, NULL);
}

int txn_put_dbi(Txn *txn, uint32_t dbi, const void *key, uint32_t key_len, const void *val,
                uint32_t val_len)
{
    return txn_put_flags_dbi(txn, dbi, key, key_len, val, val_len, 0, NULL);
}

int txn_put_if(Txn *txn_pub, uint32_t dbi, const void *key, uint32_t key_len, const void *val,
               uint32_t val_len, const void *expected_val, uint32_t expected_len)
{
    const void *cur_val;
    uint32_t cur_len;
    int rc;
    struct Txn *txn = (struct Txn *)txn_pub;
    if (!txn)
        return SAP_ERROR;
    if (txn->flags & TXN_RDONLY)
        return SAP_READONLY;
    if (dbi >= txn->db->num_dbs)
        return SAP_ERROR;
    if (txn->db->dbs[dbi].flags & DBI_DUPSORT)
        return SAP_ERROR;
    if (!expected_val && expected_len > 0)
        return SAP_ERROR;

    rc = txn_get_dbi((Txn *)txn, dbi, key, key_len, &cur_val, &cur_len);
    if (rc != SAP_OK)
        return rc;
    if (cur_len != expected_len)
        return SAP_CONFLICT;
    if (expected_len && memcmp(cur_val, expected_val, expected_len) != 0)
        return SAP_CONFLICT;
    return txn_put_dbi((Txn *)txn, dbi, key, key_len, val, val_len);
}

/* ================================================================== */
/* txn_get                                                              */
/* ================================================================== */

int txn_get_dbi(Txn *txn_pub, uint32_t dbi, const void *key, uint32_t key_len, const void **val_out,
                uint32_t *val_len_out)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    struct DB *db = txn->db;
    if (dbi >= db->num_dbs)
        return SAP_NOTFOUND;

    /* DUPSORT: seek by key (independent of value comparator ordering). */
    if (db->dbs[dbi].flags & DBI_DUPSORT)
    {
        Cursor *cur = cursor_open_dbi((Txn *)txn, dbi);
        int rc;
        const void *cur_key, *cur_val;
        uint32_t cur_key_len, cur_val_len;
        if (!cur)
            return SAP_ERROR;
        rc = cursor_seek_dupsort_key(cur, key, key_len);
        if (rc != SAP_OK)
        {
            cursor_close(cur);
            return rc;
        }
        rc = cursor_get(cur, &cur_key, &cur_key_len, &cur_val, &cur_val_len);
        if (rc == SAP_OK && user_keycmp(db, dbi, cur_key, cur_key_len, key, key_len) == 0)
        {
            *val_out = cur_val;
            *val_len_out = cur_val_len;
            cursor_close(cur);
            return SAP_OK;
        }
        cursor_close(cur);
        return SAP_NOTFOUND;
    }

    if (key_len > UINT16_MAX)
        return SAP_NOTFOUND;
    if (txn->dbs[dbi].root_pgno == INVALID_PGNO)
        return SAP_NOTFOUND;
    uint32_t pgno = txn->dbs[dbi].root_pgno;
    while (PG_TYPE(db->pages[pgno]) == PAGE_INTERNAL)
    {
        void *pg = db->pages[pgno];
        pgno = int_child(pg, int_find_child(db, dbi, pg, key, key_len));
    }
    void *lpg = db->pages[pgno];
    int found;
    int pos = leaf_find(db, dbi, lpg, key, key_len, &found);
    if (!found)
        return SAP_NOTFOUND;
    uint16_t off = (uint16_t)L_SLOT(lpg, pos);
    uint16_t klen = L_CKLEN(lpg, off);
    uint16_t vlen = L_CVLEN(lpg, off);
    const void *val_ptr = L_CVAL(lpg, off, klen);
    if (vlen == OVERFLOW_VALUE_SENTINEL)
        return overflow_read_value(txn, val_ptr, val_out, val_len_out);
    *val_out = val_ptr;
    *val_len_out = vlen;
    return SAP_OK;
}

int txn_get(Txn *txn, const void *key, uint32_t key_len, const void **val_out,
            uint32_t *val_len_out)
{
    return txn_get_dbi(txn, 0, key, key_len, val_out, val_len_out);
}

/* ================================================================== */
/* txn_del                                                              */
/* ================================================================== */

static void txn_free_page(struct Txn *txn, uint32_t pgno)
{
    struct DB *db = txn->db;
    wr32(db->pages[pgno], txn->free_pgno);
    txn->free_pgno = pgno;
    u32_remove(txn->new_pages, &txn->new_cnt, pgno);
}

int txn_del_dbi(Txn *txn_pub, uint32_t dbi, const void *key, uint32_t key_len)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    if (txn->flags & TXN_RDONLY)
        return SAP_READONLY;
    if (key_len > UINT16_MAX)
        return SAP_NOTFOUND;
    struct DB *db = txn->db;
    if (dbi >= db->num_dbs)
        return SAP_NOTFOUND;
    if (txn->dbs[dbi].root_pgno == INVALID_PGNO)
        return SAP_NOTFOUND;

    uint32_t path[MAX_DEPTH];
    int path_idx[MAX_DEPTH];
    int depth = 0;
    uint32_t pgno = txn->dbs[dbi].root_pgno;
    while (PG_TYPE(db->pages[pgno]) == PAGE_INTERNAL)
    {
        void *pg = db->pages[pgno];
        int idx = int_find_child(db, dbi, pg, key, key_len);
        if (depth >= MAX_DEPTH - 1)
            return SAP_ERROR;
        path[depth] = pgno;
        path_idx[depth] = idx;
        depth++;
        pgno = int_child(pg, idx);
    }

    uint32_t leaf_pgno = txn_cow(txn, pgno);
    if (leaf_pgno == INVALID_PGNO)
        return SAP_ERROR;
    void *lpg = db->pages[leaf_pgno];
    int found;
    int pos = leaf_find(db, dbi, lpg, key, key_len, &found);
    if (!found)
        return SAP_NOTFOUND;
    {
        uint16_t off = (uint16_t)L_SLOT(lpg, pos);
        if (leaf_cell_mark_overflow_old(txn, lpg, off) < 0)
            return SAP_ERROR;
    }
    leaf_remove(lpg, pos);
    txn->dbs[dbi].num_entries--;
    (void)txn_track_change(txn, dbi, key, key_len);

    uint32_t child = leaf_pgno;
    for (int d = depth - 1; d >= 0; d--)
    {
        uint32_t pp = txn_cow(txn, path[d]);
        if (pp == INVALID_PGNO)
            return SAP_ERROR;
        path[d] = pp;
        void *par = db->pages[pp];
        int ci = path_idx[d];
        if (ci == 0)
            SET_I_LEFT(par, child);
        else
            SET_I_CRIGHT(par, I_SLOT(par, ci - 1), child);
        child = pp;
    }
    if (depth > 0)
        txn->dbs[dbi].root_pgno = path[0];
    else
        txn->dbs[dbi].root_pgno = leaf_pgno;

    if (PG_NUM(lpg) > 0)
        return SAP_OK;

    txn_free_page(txn, leaf_pgno);

    if (depth == 0)
    {
        txn->dbs[dbi].root_pgno = INVALID_PGNO;
        return SAP_OK;
    }

    for (int d = depth - 1; d >= 0; d--)
    {
        uint32_t par_pgno = path[d];
        void *par = db->pages[par_pgno];
        int_remove_child(par, path_idx[d]);
        if (PG_NUM(par) > 0)
            break;
        uint32_t sole = I_LEFT(par);
        txn_free_page(txn, par_pgno);
        if (d == 0)
        {
            txn->dbs[dbi].root_pgno = sole;
            break;
        }
        uint32_t gp = path[d - 1];
        void *gpg = db->pages[gp];
        int gc = path_idx[d - 1];
        if (gc == 0)
            SET_I_LEFT(gpg, sole);
        else
            SET_I_CRIGHT(gpg, I_SLOT(gpg, gc - 1), sole);
        break;
    }
    return SAP_OK;
}

int txn_del(Txn *txn, const void *key, uint32_t key_len)
{
    return txn_del_dbi(txn, 0, key, key_len);
}

/* ================================================================== */
/* Bulk/range helpers                                                   */
/* ================================================================== */

struct BuildNode
{
    uint32_t pgno;
    const uint8_t *min_key;
    uint16_t min_len;
};

static int txn_mark_tree_old(struct Txn *txn, uint32_t root_pgno)
{
    uint32_t *stack = NULL;
    uint32_t top = 0, cap = 0;
    int rc = 0;

    if (!txn || root_pgno == INVALID_PGNO)
        return 0;

    cap = 64;
    stack = (uint32_t *)malloc((size_t)cap * sizeof(*stack));
    if (!stack)
        return -1;
    stack[top++] = root_pgno;

    while (top)
    {
        uint32_t pgno = stack[--top];
        void *pg;
        if (pgno == INVALID_PGNO)
            continue;
        pg = txn->db->pages[pgno];
        if (!pg)
        {
            rc = -1;
            break;
        }
        if (u32_push(&txn->old_pages, &txn->old_cnt, &txn->old_cap, pgno) < 0)
        {
            rc = -1;
            break;
        }
        if (PG_TYPE(pg) == PAGE_INTERNAL)
        {
            uint32_t n = (uint32_t)PG_NUM(pg);
            uint32_t need = n + 1;
            if (top > UINT32_MAX - need)
            {
                rc = -1;
                break;
            }
            if (top + need > cap)
            {
                uint32_t nc = cap;
                while (nc < top + need)
                {
                    if (nc > UINT32_MAX / 2)
                    {
                        rc = -1;
                        break;
                    }
                    nc *= 2;
                }
                if (rc)
                    break;
                uint32_t *ns = (uint32_t *)realloc(stack, (size_t)nc * sizeof(*stack));
                if (!ns)
                {
                    rc = -1;
                    break;
                }
                stack = ns;
                cap = nc;
            }
            stack[top++] = I_LEFT(pg);
            for (uint32_t i = 0; i < n; i++)
                stack[top++] = I_CRIGHT(pg, I_SLOT(pg, i));
        }
        else if (PG_TYPE(pg) == PAGE_LEAF)
        {
            uint32_t n = (uint32_t)PG_NUM(pg);
            for (uint32_t i = 0; i < n; i++)
            {
                uint16_t off = (uint16_t)L_SLOT(pg, i);
                if (leaf_cell_mark_overflow_old(txn, pg, off) < 0)
                {
                    rc = -1;
                    break;
                }
            }
            if (rc)
                break;
        }
    }

    free(stack);
    return rc;
}

static int txn_tree_has_overflow(struct Txn *txn, uint32_t root_pgno)
{
    uint32_t *stack = NULL;
    uint32_t top = 0, cap = 0;
    int rc = 0;

    if (!txn || root_pgno == INVALID_PGNO)
        return 0;

    cap = 64;
    stack = (uint32_t *)malloc((size_t)cap * sizeof(*stack));
    if (!stack)
        return -1;
    stack[top++] = root_pgno;

    while (top)
    {
        uint32_t pgno = stack[--top];
        void *pg;
        if (pgno == INVALID_PGNO)
            continue;
        pg = txn->db->pages[pgno];
        if (!pg)
        {
            rc = -1;
            break;
        }
        if (PG_TYPE(pg) == PAGE_INTERNAL)
        {
            uint32_t n = (uint32_t)PG_NUM(pg);
            uint32_t need = n + 1;
            if (top > UINT32_MAX - need)
            {
                rc = -1;
                break;
            }
            if (top + need > cap)
            {
                uint32_t nc = cap;
                while (nc < top + need)
                {
                    if (nc > UINT32_MAX / 2)
                    {
                        rc = -1;
                        break;
                    }
                    nc *= 2;
                }
                if (rc)
                    break;
                uint32_t *ns = (uint32_t *)realloc(stack, (size_t)nc * sizeof(*stack));
                if (!ns)
                {
                    rc = -1;
                    break;
                }
                stack = ns;
                cap = nc;
            }
            stack[top++] = I_LEFT(pg);
            for (uint32_t i = 0; i < n; i++)
                stack[top++] = I_CRIGHT(pg, I_SLOT(pg, i));
        }
        else if (PG_TYPE(pg) == PAGE_LEAF)
        {
            uint32_t n = (uint32_t)PG_NUM(pg);
            for (uint32_t i = 0; i < n; i++)
            {
                uint16_t off = (uint16_t)L_SLOT(pg, i);
                if (L_CVLEN(pg, off) == OVERFLOW_VALUE_SENTINEL)
                {
                    free(stack);
                    return 1;
                }
            }
        }
    }

    free(stack);
    return rc;
}

static int txn_load_sorted_empty_fast(struct Txn *txn, uint32_t dbi, const void *const *keys,
                                      const uint32_t *key_lens, const void *const *vals,
                                      const uint32_t *val_lens, uint32_t count, int is_dupsort)
{
    static const uint8_t zero = 0;
    struct DB *db = txn->db;
    struct BuildNode *cur = NULL, *next = NULL, *tmp;
    uint32_t cur_count = 0;
    void *leaf = NULL;
    uint32_t leaf_pgno = INVALID_PGNO;
    int rc = SAP_ERROR;

    if (count == 0)
        return SAP_OK;
    if ((size_t)count > SIZE_MAX / sizeof(*cur))
        return SAP_ERROR;

    cur = (struct BuildNode *)malloc((size_t)count * sizeof(*cur));
    next = (struct BuildNode *)malloc((size_t)count * sizeof(*next));
    if (!cur || !next)
        goto cleanup;

    for (uint32_t i = 0; i < count; i++)
    {
        for (;;)
        {
            struct ScratchMark scratch_mark = txn_scratch_mark(txn);
            const void *k = keys[i] ? keys[i] : &zero;
            const void *v = vals[i] ? vals[i] : &zero;
            const void *store_key;
            const void *store_val;
            uint16_t store_klen;
            uint16_t store_vlen;

            if (!leaf)
            {
                leaf_pgno = txn_alloc(txn);
                if (leaf_pgno == INVALID_PGNO)
                {
                    txn_scratch_release(txn, scratch_mark);
                    rc = SAP_ERROR;
                    goto cleanup;
                }
                pg_init_leaf(db->pages[leaf_pgno], leaf_pgno, db->page_size);
                leaf = db->pages[leaf_pgno];
            }

            if (is_dupsort)
            {
                uint32_t comp_len = 4u + key_lens[i] + val_lens[i];
                uint8_t *comp;
                if (comp_len > UINT16_MAX)
                {
                    txn_scratch_release(txn, scratch_mark);
                    rc = SAP_FULL;
                    goto cleanup;
                }
                comp = (uint8_t *)txn_scratch_alloc(txn, comp_len);
                if (!comp)
                {
                    txn_scratch_release(txn, scratch_mark);
                    rc = SAP_ERROR;
                    goto cleanup;
                }
                wr32(comp, key_lens[i]);
                memcpy(comp + 4, k, key_lens[i]);
                memcpy(comp + 4 + key_lens[i], v, val_lens[i]);
                store_key = comp;
                store_val = &zero;
                store_klen = (uint16_t)comp_len;
                store_vlen = 0;
            }
            else
            {
                store_key = k;
                store_val = v;
                store_klen = (uint16_t)key_lens[i];
                store_vlen = (uint16_t)val_lens[i];
            }

            if (SLOT_SZ + LCELL_SZ(store_klen, store_vlen) + LEAF_HDR > db->page_size)
            {
                txn_scratch_release(txn, scratch_mark);
                rc = SAP_FULL;
                goto cleanup;
            }

            if (leaf_insert(leaf, (int)PG_NUM(leaf), store_key, store_klen, store_val, store_vlen,
                            NULL) == 0)
            {
                if (PG_NUM(leaf) == 1)
                {
                    uint16_t off = (uint16_t)L_SLOT(leaf, 0);
                    cur[cur_count].pgno = leaf_pgno;
                    cur[cur_count].min_key = L_CKEY(leaf, off);
                    cur[cur_count].min_len = L_CKLEN(leaf, off);
                    cur_count++;
                }
                txn_scratch_release(txn, scratch_mark);
                break;
            }

            txn_scratch_release(txn, scratch_mark);
            if (PG_NUM(leaf) == 0)
            {
                rc = SAP_FULL;
                goto cleanup;
            }
            leaf = NULL; /* retry same entry in a fresh leaf */
        }
    }

    while (cur_count > 1)
    {
        uint8_t *feasible = NULL;
        uint32_t *choice = NULL;
        uint32_t *cap = NULL;
        uint32_t next_count = 0;
        uint32_t idx = 0;

        if ((size_t)cur_count > SIZE_MAX / sizeof(*cap))
        {
            rc = SAP_ERROR;
            goto cleanup;
        }

        cap = (uint32_t *)malloc((size_t)cur_count * sizeof(*cap));
        choice = (uint32_t *)calloc((size_t)cur_count + 1, sizeof(*choice));
        feasible = (uint8_t *)calloc((size_t)cur_count + 1, 1);
        if (!cap || !choice || !feasible)
        {
            free(cap);
            free(choice);
            free(feasible);
            rc = SAP_ERROR;
            goto cleanup;
        }

        for (uint32_t i = 0; i < cur_count; i++)
        {
            uint32_t free_bytes = db->page_size - INT_HDR;
            uint32_t max_children = 1;
            for (uint32_t j = i + 1; j < cur_count; j++)
            {
                uint32_t need = SLOT_SZ + ICELL_SZ(cur[j].min_len);
                if (need > free_bytes)
                    break;
                free_bytes -= need;
                max_children++;
            }
            cap[i] = max_children;
        }

        feasible[cur_count] = 1;
        for (uint32_t i = cur_count; i-- > 0;)
        {
            uint32_t max_group = cap[i];
            uint32_t remaining = cur_count - i;
            if (max_group > remaining)
                max_group = remaining;
            for (uint32_t group = 2; group <= max_group; group++)
            {
                if (feasible[i + group])
                {
                    feasible[i] = 1;
                    choice[i] = group;
                    break;
                }
            }
        }

        if (!feasible[0])
        {
            free(cap);
            free(choice);
            free(feasible);
            rc = SAP_FULL;
            goto cleanup;
        }

        while (idx < cur_count)
        {
            uint32_t group = choice[idx];
            uint32_t pgno = txn_alloc(txn);
            void *ipg;
            if (pgno == INVALID_PGNO)
            {
                free(cap);
                free(choice);
                free(feasible);
                rc = SAP_ERROR;
                goto cleanup;
            }
            ipg = db->pages[pgno];
            pg_init_internal(ipg, pgno, db->page_size);
            SET_I_LEFT(ipg, cur[idx].pgno);
            for (uint32_t j = 1; j < group; j++)
            {
                if (int_insert(ipg, (int)PG_NUM(ipg), cur[idx + j].min_key, cur[idx + j].min_len,
                               cur[idx + j].pgno) < 0)
                {
                    free(cap);
                    free(choice);
                    free(feasible);
                    rc = SAP_ERROR;
                    goto cleanup;
                }
            }
            next[next_count].pgno = pgno;
            next[next_count].min_key = cur[idx].min_key;
            next[next_count].min_len = cur[idx].min_len;
            next_count++;
            idx += group;
        }

        free(cap);
        free(choice);
        free(feasible);
        tmp = cur;
        cur = next;
        next = tmp;
        cur_count = next_count;
    }

    txn->dbs[dbi].root_pgno = cur[0].pgno;
    txn->dbs[dbi].num_entries += count;
    rc = SAP_OK;

cleanup:
    free(cur);
    free(next);
    return rc;
}

static int txn_load_sorted_nonempty_merge_fast(struct Txn *txn, uint32_t dbi,
                                               const void *const *keys, const uint32_t *key_lens,
                                               const void *const *vals, const uint32_t *val_lens,
                                               uint32_t count)
{
    static const uint8_t zero = 0;
    struct DB *db = txn->db;
    Cursor *cur = NULL;
    const void **mkeys = NULL, **mvals = NULL;
    uint32_t *mklen = NULL, *mvlen = NULL;
    uint32_t in_i = 0, out_i = 0;
    uint32_t max_total;
    uint64_t existing64 = txn->dbs[dbi].num_entries;
    uint64_t max_total64;
    int has_old = 0;
    int rc = SAP_ERROR;

    if (existing64 > UINT32_MAX)
        return SAP_ERROR;
    max_total64 = existing64 + (uint64_t)count;
    if (max_total64 > UINT32_MAX)
        return SAP_ERROR;
    max_total = (uint32_t)max_total64;

    if (max_total == 0)
        return SAP_OK;
    if ((size_t)max_total > SIZE_MAX / sizeof(*mkeys))
        return SAP_ERROR;

    mkeys = (const void **)malloc((size_t)max_total * sizeof(*mkeys));
    mvals = (const void **)malloc((size_t)max_total * sizeof(*mvals));
    mklen = (uint32_t *)malloc((size_t)max_total * sizeof(*mklen));
    mvlen = (uint32_t *)malloc((size_t)max_total * sizeof(*mvlen));
    if (!mkeys || !mvals || !mklen || !mvlen)
        goto cleanup;

    cur = cursor_open_dbi((Txn *)txn, dbi);
    if (!cur)
        goto cleanup;
    rc = cursor_first(cur);
    if (rc == SAP_OK)
        has_old = 1;
    else if (rc == SAP_NOTFOUND)
        has_old = 0;
    else
        goto cleanup;

    while (has_old && in_i < count)
    {
        const void *ok, *ov;
        uint32_t okl, ovl;
        const void *nk = keys[in_i] ? keys[in_i] : &zero;
        const void *nv = vals[in_i] ? vals[in_i] : &zero;
        int c;

        rc = cursor_get(cur, &ok, &okl, &ov, &ovl);
        if (rc != SAP_OK)
            goto cleanup;

        c = user_keycmp(db, dbi, ok, okl, nk, key_lens[in_i]);
        if (c < 0)
        {
            mkeys[out_i] = ok;
            mklen[out_i] = okl;
            mvals[out_i] = ov;
            mvlen[out_i] = ovl;
            out_i++;
            rc = cursor_next(cur);
            if (rc == SAP_OK)
                has_old = 1;
            else if (rc == SAP_NOTFOUND)
                has_old = 0;
            else
                goto cleanup;
        }
        else if (c > 0)
        {
            mkeys[out_i] = nk;
            mklen[out_i] = key_lens[in_i];
            mvals[out_i] = nv;
            mvlen[out_i] = val_lens[in_i];
            out_i++;
            in_i++;
        }
        else
        {
            mkeys[out_i] = nk;
            mklen[out_i] = key_lens[in_i];
            mvals[out_i] = nv;
            mvlen[out_i] = val_lens[in_i];
            out_i++;
            in_i++;
            rc = cursor_next(cur);
            if (rc == SAP_OK)
                has_old = 1;
            else if (rc == SAP_NOTFOUND)
                has_old = 0;
            else
                goto cleanup;
        }
    }

    while (has_old)
    {
        const void *ok, *ov;
        uint32_t okl, ovl;
        rc = cursor_get(cur, &ok, &okl, &ov, &ovl);
        if (rc != SAP_OK)
            goto cleanup;
        mkeys[out_i] = ok;
        mklen[out_i] = okl;
        mvals[out_i] = ov;
        mvlen[out_i] = ovl;
        out_i++;
        rc = cursor_next(cur);
        if (rc == SAP_OK)
            has_old = 1;
        else if (rc == SAP_NOTFOUND)
            has_old = 0;
        else
            goto cleanup;
    }

    while (in_i < count)
    {
        mkeys[out_i] = keys[in_i] ? keys[in_i] : &zero;
        mklen[out_i] = key_lens[in_i];
        mvals[out_i] = vals[in_i] ? vals[in_i] : &zero;
        mvlen[out_i] = val_lens[in_i];
        out_i++;
        in_i++;
    }

    if (txn_mark_tree_old(txn, txn->dbs[dbi].root_pgno) < 0)
    {
        rc = SAP_ERROR;
        goto cleanup;
    }
    txn->dbs[dbi].root_pgno = INVALID_PGNO;
    txn->dbs[dbi].num_entries = 0;

    rc = txn_load_sorted_empty_fast(txn, dbi, mkeys, mklen, mvals, mvlen, out_i, 0);

cleanup:
    if (cur)
        cursor_close(cur);
    free(mkeys);
    free(mvals);
    free(mklen);
    free(mvlen);
    return rc;
}

int txn_load_sorted(Txn *txn_pub, uint32_t dbi, const void *const *keys, const uint32_t *key_lens,
                    const void *const *vals, const uint32_t *val_lens, uint32_t count)
{
    static const uint8_t zero = 0;
    struct Txn *txn = (struct Txn *)txn_pub;
    struct DB *db;
    int is_dupsort;
    int requires_overflow = 0;

    if (!txn)
        return SAP_ERROR;
    if (txn->flags & TXN_RDONLY)
        return SAP_READONLY;
    db = txn->db;
    if (dbi >= db->num_dbs)
        return SAP_ERROR;
    if (count == 0)
        return SAP_OK;
    if (!key_lens || !val_lens)
        return SAP_ERROR;
    if (!keys || !vals)
        return SAP_ERROR;

    is_dupsort = (db->dbs[dbi].flags & DBI_DUPSORT) != 0;

    for (uint32_t i = 0; i < count; i++)
    {
        const void *kcur = keys[i];
        const void *vcur = vals[i];
        if (key_lens[i] > UINT16_MAX || val_lens[i] > UINT16_MAX)
            return SAP_FULL;
        if (!kcur && key_lens[i] > 0)
            return SAP_ERROR;
        if (!vcur && val_lens[i] > 0)
            return SAP_ERROR;
        if (!is_dupsort &&
            SLOT_SZ + leaf_cell_size((uint16_t)key_lens[i], (uint16_t)val_lens[i]) + LEAF_HDR >
                db->page_size)
            requires_overflow = 1;
        if (!kcur)
            kcur = &zero;
        if (!vcur)
            vcur = &zero;
        if (i == 0)
            continue;

        const void *kprev = keys[i - 1];
        const void *vprev = vals[i - 1];
        if (!kprev)
            kprev = &zero;
        if (!vprev)
            vprev = &zero;

        int kc = user_keycmp(db, dbi, kprev, key_lens[i - 1], kcur, key_lens[i]);
        if (kc > 0)
            return SAP_ERROR;
        if (kc == 0 && !is_dupsort)
            return SAP_EXISTS;
        if (kc == 0 && user_valcmp(db, dbi, vprev, val_lens[i - 1], vcur, val_lens[i]) > 0)
            return SAP_ERROR;
    }

    if (!requires_overflow && txn->dbs[dbi].root_pgno == INVALID_PGNO)
    {
        Txn *child = txn_begin((DB *)db, (Txn *)txn, 0);
        int rc;
        if (!child)
            return SAP_ERROR;
        rc = txn_load_sorted_empty_fast((struct Txn *)child, dbi, keys, key_lens, vals, val_lens,
                                        count, is_dupsort);
        if (rc == SAP_OK)
        {
            for (uint32_t i = 0; i < count; i++)
            {
                const void *k = keys[i] ? keys[i] : &zero;
                (void)txn_track_change((struct Txn *)child, dbi, k, key_lens[i]);
            }
            return txn_commit(child);
        }
        txn_abort(child);
        return rc;
    }

    if (!is_dupsort && !requires_overflow && txn->new_cnt == 0 && txn->old_cnt == 0)
    {
        int has_overflow = txn_tree_has_overflow(txn, txn->dbs[dbi].root_pgno);
        if (has_overflow < 0)
            return SAP_ERROR;
        if (!has_overflow)
        {
            Txn *child = txn_begin((DB *)db, (Txn *)txn, 0);
            int rc;
            if (!child)
                return SAP_ERROR;
            rc = txn_load_sorted_nonempty_merge_fast((struct Txn *)child, dbi, keys, key_lens, vals,
                                                     val_lens, count);
            if (rc == SAP_OK)
            {
                for (uint32_t i = 0; i < count; i++)
                {
                    const void *k = keys[i] ? keys[i] : &zero;
                    (void)txn_track_change((struct Txn *)child, dbi, k, key_lens[i]);
                }
                return txn_commit(child);
            }
            txn_abort(child);
            return rc;
        }
    }

    for (uint32_t i = 0; i < count; i++)
    {
        const void *k = keys[i] ? keys[i] : &zero;
        const void *v = vals[i] ? vals[i] : &zero;
        int rc = txn_put_dbi((Txn *)txn, dbi, k, key_lens[i], v, val_lens[i]);
        if (rc != SAP_OK)
            return rc;
    }
    return SAP_OK;
}

int txn_count_range(Txn *txn_pub, uint32_t dbi, const void *lo, uint32_t lo_len, const void *hi,
                    uint32_t hi_len, uint64_t *count_out)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    struct DB *db;
    Cursor *cur;
    int has_lo, has_hi;
    int is_dupsort;
    int rc;

    if (!txn || !count_out)
        return SAP_ERROR;
    db = txn->db;
    if (dbi >= db->num_dbs)
        return SAP_ERROR;
    if (!lo && lo_len > 0)
        return SAP_ERROR;
    if (!hi && hi_len > 0)
        return SAP_ERROR;
    *count_out = 0;

    has_lo = (lo != NULL);
    has_hi = (hi != NULL);
    is_dupsort = (db->dbs[dbi].flags & DBI_DUPSORT) != 0;
    if (has_lo && has_hi)
    {
        int cmp = user_keycmp(db, dbi, lo, lo_len, hi, hi_len);
        if (cmp >= 0)
            return SAP_OK;
    }

    cur = cursor_open_dbi((Txn *)txn, dbi);
    if (!cur)
        return SAP_ERROR;

    if (!has_lo)
    {
        rc = cursor_first(cur);
    }
    else if (is_dupsort)
    {
        rc = cursor_seek_dupsort_key(cur, lo, lo_len);
    }
    else
    {
        rc = cursor_seek(cur, lo, lo_len);
    }
    if (rc == SAP_NOTFOUND)
    {
        cursor_close(cur);
        return SAP_OK;
    }
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        return rc;
    }

    for (;;)
    {
        const void *k;
        uint32_t kl;
        rc = cursor_get_key(cur, &k, &kl);
        if (rc == SAP_NOTFOUND)
        {
            rc = SAP_OK;
            break;
        }
        if (rc != SAP_OK)
            break;

        if (has_hi && user_keycmp(db, dbi, k, kl, hi, hi_len) >= 0)
        {
            rc = SAP_OK;
            break;
        }

        (*count_out)++;
        rc = cursor_next(cur);
        if (rc == SAP_NOTFOUND)
        {
            rc = SAP_OK;
            break;
        }
        if (rc != SAP_OK)
            break;
    }

    cursor_close(cur);
    return rc;
}

int txn_del_range(Txn *txn_pub, uint32_t dbi, const void *lo, uint32_t lo_len, const void *hi,
                  uint32_t hi_len, uint64_t *deleted_count_out)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    struct DB *db;
    Cursor *cur;
    int has_lo, has_hi;
    int is_dupsort;
    int rc;
    uint64_t deleted = 0;

    if (!txn || !deleted_count_out)
        return SAP_ERROR;
    if (txn->flags & TXN_RDONLY)
        return SAP_READONLY;
    db = txn->db;
    if (dbi >= db->num_dbs)
        return SAP_ERROR;
    if (!lo && lo_len > 0)
        return SAP_ERROR;
    if (!hi && hi_len > 0)
        return SAP_ERROR;
    *deleted_count_out = 0;

    has_lo = (lo != NULL);
    has_hi = (hi != NULL);
    is_dupsort = (db->dbs[dbi].flags & DBI_DUPSORT) != 0;

    if (has_lo && has_hi)
    {
        int cmp = user_keycmp(db, dbi, lo, lo_len, hi, hi_len);
        if (cmp >= 0)
            return SAP_OK;
    }

    cur = cursor_open_dbi((Txn *)txn, dbi);
    if (!cur)
        return SAP_ERROR;

    if (!has_lo)
    {
        rc = cursor_first(cur);
    }
    else if (is_dupsort)
    {
        rc = cursor_seek_dupsort_key(cur, lo, lo_len);
    }
    else
    {
        rc = cursor_seek(cur, lo, lo_len);
    }

    if (rc == SAP_NOTFOUND)
    {
        cursor_close(cur);
        return SAP_OK;
    }
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        return rc;
    }

    for (;;)
    {
        const void *k;
        uint32_t kl;

        rc = cursor_get_key(cur, &k, &kl);
        if (rc == SAP_NOTFOUND)
        {
            rc = SAP_OK;
            break;
        }
        if (rc != SAP_OK)
            break;

        if (has_hi && user_keycmp(db, dbi, k, kl, hi, hi_len) >= 0)
        {
            rc = SAP_OK;
            break;
        }

        rc = cursor_del(cur);
        if (rc == SAP_NOTFOUND)
        {
            rc = SAP_OK;
            break;
        }
        if (rc != SAP_OK)
            break;
        deleted++;
    }

    cursor_close(cur);
    if (rc == SAP_OK)
        *deleted_count_out = deleted;
    return rc;
}

int txn_merge(Txn *txn_pub, uint32_t dbi, const void *key, uint32_t key_len, const void *operand,
              uint32_t op_len, sap_merge_fn merge, void *ctx)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    struct DB *db;
    struct ScratchMark scratch_mark;
    const void *old_val;
    uint32_t old_len;
    uint32_t inline_cap;
    uint8_t *old_copy;
    uint32_t cap;
    int rc;

    if (!txn || !merge)
        return SAP_ERROR;
    if (txn->flags & TXN_RDONLY)
        return SAP_READONLY;
    db = txn->db;
    if (dbi >= db->num_dbs)
        return SAP_ERROR;
    if (db->dbs[dbi].flags & DBI_DUPSORT)
        return SAP_ERROR;
    if (!key && key_len > 0)
        return SAP_ERROR;
    if (!operand && op_len > 0)
        return SAP_ERROR;
    if (key_len > UINT16_MAX)
        return SAP_FULL;
    if (SLOT_SZ + LCELL_SZ(key_len, 0) + LEAF_HDR > db->page_size)
        return SAP_FULL;

    inline_cap = db->page_size - (SLOT_SZ + LCELL_SZ(key_len, 0) + LEAF_HDR);
    old_val = NULL;
    old_len = 0;

    rc = txn_get_dbi((Txn *)txn, dbi, key, key_len, &old_val, &old_len);
    if (rc == SAP_NOTFOUND)
    {
        old_val = NULL;
        old_len = 0;
    }
    else if (rc != SAP_OK)
    {
        return rc;
    }

    scratch_mark = txn_scratch_mark(txn);
    old_copy = NULL;
    if (old_len > 0)
    {
        old_copy = txn_scratch_copy(txn, old_val, old_len);
        if (!old_copy)
        {
            txn_scratch_release(txn, scratch_mark);
            return SAP_ERROR;
        }
    }

    cap = inline_cap;
    if (cap > UINT16_MAX)
        cap = UINT16_MAX;

    for (int pass = 0; pass < 2; pass++)
    {
        uint8_t *out_buf = (uint8_t *)txn_scratch_alloc(txn, cap);
        uint32_t out_len = cap;
        if (!out_buf)
        {
            txn_scratch_release(txn, scratch_mark);
            return SAP_ERROR;
        }

        merge(old_copy, old_len, operand, op_len, out_buf, &out_len, ctx);
        if (out_len <= cap)
        {
            rc = txn_put_dbi((Txn *)txn, dbi, key, key_len, out_buf, out_len);
            txn_scratch_release(txn, scratch_mark);
            return rc;
        }

        if (out_len > UINT16_MAX)
        {
            txn_scratch_release(txn, scratch_mark);
            return SAP_FULL;
        }
        cap = out_len;
    }

    txn_scratch_release(txn, scratch_mark);
    return SAP_FULL;
}

struct TTLKeyList
{
    uint8_t **keys;
    uint32_t *lens;
    uint64_t *expiries;
    uint32_t count;
    uint32_t cap;
};

#define TTL_META_LOOKUP_TAG 0x00u
#define TTL_META_INDEX_TAG 0x01u
#define TTL_META_LOOKUP_OVERHEAD 1u
#define TTL_META_INDEX_OVERHEAD 9u

static void ttl_key_list_clear(struct TTLKeyList *list)
{
    if (!list)
        return;
    for (uint32_t i = 0; i < list->count; i++)
        free(list->keys[i]);
    free(list->keys);
    free(list->lens);
    free(list->expiries);
    list->keys = NULL;
    list->lens = NULL;
    list->expiries = NULL;
    list->count = 0;
    list->cap = 0;
}

static int ttl_key_list_push(struct TTLKeyList *list, const void *key, uint32_t key_len,
                             uint64_t expiry)
{
    uint8_t *copy;
    if (!list)
        return SAP_ERROR;
    if (list->count >= list->cap)
    {
        uint32_t nc = list->cap ? list->cap * 2u : 16u;
        uint8_t **nkeys = (uint8_t **)malloc(nc * sizeof(uint8_t *));
        uint32_t *nlens = (uint32_t *)malloc(nc * sizeof(uint32_t));
        uint64_t *nexp = (uint64_t *)malloc(nc * sizeof(uint64_t));
        if (!nkeys || !nlens || !nexp)
        {
            free(nkeys);
            free(nlens);
            free(nexp);
            return SAP_ERROR;
        }
        if (list->count > 0)
        {
            memcpy(nkeys, list->keys, list->count * sizeof(uint8_t *));
            memcpy(nlens, list->lens, list->count * sizeof(uint32_t));
            memcpy(nexp, list->expiries, list->count * sizeof(uint64_t));
        }
        free(list->keys);
        free(list->lens);
        free(list->expiries);
        list->keys = nkeys;
        list->lens = nlens;
        list->expiries = nexp;
        list->cap = nc;
    }
    copy = (uint8_t *)malloc(key_len ? key_len : 1u);
    if (!copy)
        return SAP_ERROR;
    if (key_len > 0)
        memcpy(copy, key, key_len);
    list->keys[list->count] = copy;
    list->lens[list->count] = key_len;
    list->expiries[list->count] = expiry;
    list->count++;
    return SAP_OK;
}

static int ttl_encode_lookup_key(const void *key, uint32_t key_len, uint8_t **out_key,
                                 uint32_t *out_len)
{
    uint8_t *buf;
    uint32_t len;
    if (!out_key || !out_len)
        return SAP_ERROR;
    if (key_len > UINT16_MAX - TTL_META_LOOKUP_OVERHEAD)
        return SAP_FULL;
    len = key_len + TTL_META_LOOKUP_OVERHEAD;
    buf = (uint8_t *)malloc(len ? len : 1u);
    if (!buf)
        return SAP_ERROR;
    buf[0] = TTL_META_LOOKUP_TAG;
    if (key_len > 0)
        memcpy(buf + TTL_META_LOOKUP_OVERHEAD, key, key_len);
    *out_key = buf;
    *out_len = len;
    return SAP_OK;
}

static int ttl_encode_index_key(const void *key, uint32_t key_len, uint64_t expiry, uint8_t **out_key,
                                uint32_t *out_len)
{
    uint8_t *buf;
    uint32_t len;
    if (!out_key || !out_len)
        return SAP_ERROR;
    if (key_len > UINT16_MAX - TTL_META_INDEX_OVERHEAD)
        return SAP_FULL;
    len = key_len + TTL_META_INDEX_OVERHEAD;
    buf = (uint8_t *)malloc(len ? len : 1u);
    if (!buf)
        return SAP_ERROR;
    buf[0] = TTL_META_INDEX_TAG;
    wr64be(buf + 1, expiry);
    if (key_len > 0)
        memcpy(buf + TTL_META_INDEX_OVERHEAD, key, key_len);
    *out_key = buf;
    *out_len = len;
    return SAP_OK;
}

static int ttl_validate_dbis(const struct Txn *txn, uint32_t data_dbi, uint32_t ttl_dbi,
                             int require_write)
{
    const struct DB *db;
    if (!txn)
        return SAP_ERROR;
    if (require_write && (txn->flags & TXN_RDONLY))
        return SAP_READONLY;
    db = txn->db;
    if (data_dbi >= db->num_dbs || ttl_dbi >= db->num_dbs || data_dbi == ttl_dbi)
        return SAP_ERROR;
    if ((db->dbs[data_dbi].flags & DBI_DUPSORT) || (db->dbs[ttl_dbi].flags & DBI_DUPSORT))
        return SAP_ERROR;
    return SAP_OK;
}

int txn_put_ttl_dbi(Txn *txn_pub, uint32_t data_dbi, uint32_t ttl_dbi, const void *key,
                    uint32_t key_len, const void *val, uint32_t val_len, uint64_t expires_at_ms)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    Txn *child;
    const void *old_exp_raw = NULL;
    uint32_t old_exp_len = 0;
    uint8_t expiry_buf[8];
    uint8_t *lookup_key = NULL;
    uint8_t *index_key = NULL;
    uint8_t *old_index_key = NULL;
    uint32_t lookup_len = 0;
    uint32_t index_len = 0;
    uint32_t old_index_len = 0;
    int rc;

    if (!key && key_len > 0)
        return SAP_ERROR;
    if (!val && val_len > 0)
        return SAP_ERROR;
    rc = ttl_validate_dbis(txn, data_dbi, ttl_dbi, 1);
    if (rc != SAP_OK)
        return rc;
    rc = ttl_encode_lookup_key(key, key_len, &lookup_key, &lookup_len);
    if (rc != SAP_OK)
        goto done;
    rc = ttl_encode_index_key(key, key_len, expires_at_ms, &index_key, &index_len);
    if (rc != SAP_OK)
        goto done;

    child = txn_begin((DB *)txn->db, (Txn *)txn, 0);
    if (!child)
    {
        rc = SAP_ERROR;
        goto done;
    }

    rc = txn_get_dbi(child, ttl_dbi, lookup_key, lookup_len, &old_exp_raw, &old_exp_len);
    if (rc == SAP_OK)
    {
        rc = (old_exp_len == 8) ? SAP_OK : SAP_ERROR;
        if (rc != SAP_OK)
            goto abort_child;
        rc = ttl_encode_index_key(key, key_len, rd64(old_exp_raw), &old_index_key, &old_index_len);
        if (rc != SAP_OK)
            goto abort_child;
        rc = txn_del_dbi(child, ttl_dbi, old_index_key, old_index_len);
        if (rc != SAP_OK && rc != SAP_NOTFOUND)
            goto abort_child;
        rc = SAP_OK;
    }
    else if (rc == SAP_NOTFOUND)
    {
        rc = SAP_OK;
    }
    else
    {
        goto abort_child;
    }

    rc = txn_put_dbi(child, data_dbi, key, key_len, val, val_len);
    if (rc != SAP_OK)
        goto abort_child;

    wr64(expiry_buf, expires_at_ms);
    rc = txn_put_dbi(child, ttl_dbi, lookup_key, lookup_len, expiry_buf, (uint32_t)sizeof(expiry_buf));
    if (rc != SAP_OK)
        goto abort_child;

    rc = txn_put_dbi(child, ttl_dbi, index_key, index_len, NULL, 0);
    if (rc != SAP_OK)
        goto abort_child;

    rc = txn_commit(child);
    goto done;

abort_child:
    txn_abort(child);
done:
    free(lookup_key);
    free(index_key);
    free(old_index_key);
    return rc;
}

int txn_get_ttl_dbi(Txn *txn_pub, uint32_t data_dbi, uint32_t ttl_dbi, const void *key,
                    uint32_t key_len, uint64_t now_ms, const void **val_out, uint32_t *val_len_out)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    const void *exp_raw;
    uint32_t exp_len;
    uint8_t *lookup_key = NULL;
    uint32_t lookup_len = 0;
    int rc;

    if (!val_out || !val_len_out)
        return SAP_ERROR;
    if (!key && key_len > 0)
        return SAP_ERROR;

    rc = ttl_validate_dbis(txn, data_dbi, ttl_dbi, 0);
    if (rc != SAP_OK)
        return rc;

    rc = ttl_encode_lookup_key(key, key_len, &lookup_key, &lookup_len);
    if (rc != SAP_OK)
        return rc;

    rc = txn_get_dbi((Txn *)txn, ttl_dbi, lookup_key, lookup_len, &exp_raw, &exp_len);
    free(lookup_key);
    if (rc != SAP_OK)
        return rc;
    if (exp_len != 8)
        return SAP_ERROR;
    if (rd64(exp_raw) <= now_ms)
        return SAP_NOTFOUND;

    return txn_get_dbi((Txn *)txn, data_dbi, key, key_len, val_out, val_len_out);
}

static int txn_sweep_ttl_inner(struct Txn *txn, uint32_t data_dbi, uint32_t ttl_dbi, uint64_t now_ms,
                               uint64_t *deleted_count_out)
{
    struct TTLKeyList expired = {0};
    Cursor *cur;
    const uint8_t seek_key = TTL_META_INDEX_TAG;
    int rc;
    uint64_t deleted = 0;

    cur = cursor_open_dbi((Txn *)txn, ttl_dbi);
    if (!cur)
        return SAP_ERROR;

    rc = cursor_seek(cur, &seek_key, 1);
    if (rc == SAP_NOTFOUND)
    {
        cursor_close(cur);
        *deleted_count_out = 0;
        return SAP_OK;
    }
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        return rc;
    }

    for (;;)
    {
        const void *k;
        const void *v;
        const uint8_t *kb;
        uint32_t kl;
        uint32_t vl;
        uint64_t expiry;

        rc = cursor_get(cur, &k, &kl, &v, &vl);
        if (rc == SAP_NOTFOUND)
        {
            rc = SAP_OK;
            break;
        }
        if (rc != SAP_OK)
            break;
        kb = (const uint8_t *)k;
        if (kl < TTL_META_INDEX_OVERHEAD || kb[0] != TTL_META_INDEX_TAG)
        {
            rc = SAP_OK;
            break;
        }
        if (vl != 0)
        {
            rc = SAP_ERROR;
            break;
        }
        expiry = rd64be(kb + 1);
        if (expiry <= now_ms)
        {
            rc = ttl_key_list_push(&expired, kb + TTL_META_INDEX_OVERHEAD,
                                   kl - TTL_META_INDEX_OVERHEAD, expiry);
            if (rc != SAP_OK)
                break;
        }
        else
        {
            rc = SAP_OK;
            break;
        }

        rc = cursor_next(cur);
        if (rc == SAP_NOTFOUND)
        {
            rc = SAP_OK;
            break;
        }
        if (rc != SAP_OK)
            break;
    }
    cursor_close(cur);
    if (rc != SAP_OK)
    {
        ttl_key_list_clear(&expired);
        return rc;
    }

    for (uint32_t i = 0; i < expired.count; i++)
    {
        uint8_t *lookup_key = NULL;
        uint8_t *index_key = NULL;
        uint32_t lookup_len = 0;
        uint32_t index_len = 0;
        const void *lookup_val = NULL;
        uint32_t lookup_vlen = 0;
        int md_deleted = 0;
        int drc = txn_del_dbi((Txn *)txn, data_dbi, expired.keys[i], expired.lens[i]);
        if (drc != SAP_OK && drc != SAP_NOTFOUND)
        {
            rc = drc;
            break;
        }
        rc = ttl_encode_lookup_key(expired.keys[i], expired.lens[i], &lookup_key, &lookup_len);
        if (rc != SAP_OK)
            break;
        rc = ttl_encode_index_key(expired.keys[i], expired.lens[i], expired.expiries[i], &index_key,
                                  &index_len);
        if (rc != SAP_OK)
        {
            free(lookup_key);
            break;
        }

        drc = txn_get_dbi((Txn *)txn, ttl_dbi, lookup_key, lookup_len, &lookup_val, &lookup_vlen);
        if (drc != SAP_OK && drc != SAP_NOTFOUND)
        {
            rc = drc;
            free(lookup_key);
            free(index_key);
            break;
        }
        if (drc == SAP_OK && lookup_vlen != 8)
        {
            rc = SAP_ERROR;
            free(lookup_key);
            free(index_key);
            break;
        }

        drc = txn_del_dbi((Txn *)txn, ttl_dbi, lookup_key, lookup_len);
        if (drc != SAP_OK && drc != SAP_NOTFOUND)
        {
            rc = drc;
            free(lookup_key);
            free(index_key);
            break;
        }
        if (drc == SAP_OK)
            md_deleted = 1;

        drc = txn_del_dbi((Txn *)txn, ttl_dbi, index_key, index_len);
        if (drc != SAP_OK && drc != SAP_NOTFOUND)
        {
            rc = drc;
            free(lookup_key);
            free(index_key);
            break;
        }
        if (drc == SAP_OK)
            md_deleted = 1;

        if (md_deleted)
            deleted++;
        free(lookup_key);
        free(index_key);
    }

    ttl_key_list_clear(&expired);
    if (rc == SAP_OK)
        *deleted_count_out = deleted;
    return rc;
}

int txn_sweep_ttl_dbi(Txn *txn_pub, uint32_t data_dbi, uint32_t ttl_dbi, uint64_t now_ms,
                      uint64_t *deleted_count_out)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    Txn *child;
    uint64_t deleted = 0;
    int rc;

    if (!deleted_count_out)
        return SAP_ERROR;
    *deleted_count_out = 0;

    rc = ttl_validate_dbis(txn, data_dbi, ttl_dbi, 1);
    if (rc != SAP_OK)
        return rc;

    child = txn_begin((DB *)txn->db, (Txn *)txn, 0);
    if (!child)
        return SAP_ERROR;

    rc = txn_sweep_ttl_inner((struct Txn *)child, data_dbi, ttl_dbi, now_ms, &deleted);
    if (rc != SAP_OK)
    {
        txn_abort(child);
        return rc;
    }
    rc = txn_commit(child);
    if (rc == SAP_OK)
        *deleted_count_out = deleted;
    return rc;
}

/* ================================================================== */
/* Transaction management                                               */
/* ================================================================== */

static void txn_free_mem(struct Txn *t)
{
    free(t->new_pages);
    free(t->old_pages);
    txn_changes_clear(t);
    txn_readbuf_clear(t);
    txn_scratch_clear(t);
    free(t);
}

Txn *txn_begin(DB *db_pub, Txn *par_pub, unsigned int flags)
{
    struct DB *db = (struct DB *)db_pub;
    struct Txn *par = (struct Txn *)par_pub;

    if (!par)
        SAP_MUTEX_LOCK(db->write_mutex);

    if (!(flags & TXN_RDONLY) && !par && db->write_txn)
    {
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return NULL;
    }

    struct Txn *txn = (struct Txn *)calloc(1, sizeof(*txn));
    if (!txn)
    {
        if (!par)
            SAP_MUTEX_UNLOCK(db->write_mutex);
        return NULL;
    }
    txn->db = db;
    txn->parent = par;
    txn->flags = flags;

    uint32_t nd = db->num_dbs;
    if (par)
    {
        txn->txnid = par->txnid;
        txn->track_changes = par->track_changes;
        for (uint32_t i = 0; i < nd; i++)
        {
            txn->dbs[i].root_pgno = par->dbs[i].root_pgno;
            txn->dbs[i].num_entries = par->dbs[i].num_entries;
            txn->dbs[i].saved_root = par->dbs[i].root_pgno;
            txn->dbs[i].saved_entries = par->dbs[i].num_entries;
        }
        txn->free_pgno = par->free_pgno;
        txn->num_pages = par->num_pages;
        txn->saved_free = par->free_pgno;
        txn->saved_npages = par->num_pages;
    }
    else
    {
        txn->txnid = db->txnid;
        for (uint32_t i = 0; i < nd; i++)
        {
            txn->dbs[i].root_pgno = db->dbs[i].root_pgno;
            txn->dbs[i].num_entries = db->dbs[i].num_entries;
            txn->dbs[i].saved_root = db->dbs[i].root_pgno;
            txn->dbs[i].saved_entries = db->dbs[i].num_entries;
        }
        txn->free_pgno = db->free_pgno;
        txn->num_pages = db->num_pages;
        txn->saved_free = db->free_pgno;
        txn->saved_npages = db->num_pages;
        if (!(flags & TXN_RDONLY))
        {
            db->write_txn = txn;
            txn->track_changes = (db->num_watches > 0) ? 1u : 0u;
            SAP_MUTEX_LOCK(db->reader_mutex);
            db_process_deferred(db);
            SAP_MUTEX_UNLOCK(db->reader_mutex);
            txn->free_pgno = db->free_pgno;
        }
        else
        {
            txn->track_changes = 0;
            SAP_MUTEX_LOCK(db->reader_mutex);
            if (db->num_readers >= db->cap_readers)
            {
                uint32_t nc = db->cap_readers ? db->cap_readers * 2 : 8;
                uint64_t *na = (uint64_t *)realloc(db->active_readers, nc * sizeof(uint64_t));
                if (na)
                {
                    db->active_readers = na;
                    db->cap_readers = nc;
                }
            }
            if (db->num_readers < db->cap_readers)
                db->active_readers[db->num_readers++] = txn->txnid;
            SAP_MUTEX_UNLOCK(db->reader_mutex);
        }
        SAP_MUTEX_UNLOCK(db->write_mutex);
    }
    return (Txn *)txn;
}

int txn_commit(Txn *txn_pub)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    struct DB *db = txn->db;
    struct WatchRec *watch_snap = NULL;
    uint32_t watch_count = 0;
    if (txn->flags & TXN_RDONLY)
    {
        db_remove_reader(db, txn->txnid);
        txn_free_mem(txn);
        return SAP_OK;
    }
    if (txn->parent)
    {
        struct Txn *par = txn->parent;
        uint32_t nd = db->num_dbs;
        for (uint32_t i = 0; i < nd; i++)
        {
            par->dbs[i].root_pgno = txn->dbs[i].root_pgno;
            par->dbs[i].num_entries = txn->dbs[i].num_entries;
        }
        par->free_pgno = txn->free_pgno;
        par->num_pages = txn->num_pages;
        for (uint32_t i = 0; i < txn->new_cnt; i++)
            u32_push(&par->new_pages, &par->new_cnt, &par->new_cap, txn->new_pages[i]);
        for (uint32_t i = 0; i < txn->old_cnt; i++)
        {
            uint32_t p = txn->old_pages[i];
            u32_remove(par->new_pages, &par->new_cnt, p);
            u32_push(&par->old_pages, &par->old_cnt, &par->old_cap, p);
        }
        (void)txn_merge_changes(par, txn);
        txn_free_mem(txn);
        return SAP_OK;
    }
    uint64_t freed_at = txn->txnid;
    txn->txnid++;
    for (uint32_t i = 0; i < txn->old_cnt; i++)
        db_defer_page(db, freed_at, txn->old_pages[i]);
    SAP_MUTEX_LOCK(db->write_mutex);
    db->txnid = txn->txnid;
    {
        uint32_t nd = db->num_dbs;
        for (uint32_t i = 0; i < nd; i++)
        {
            db->dbs[i].root_pgno = txn->dbs[i].root_pgno;
            db->dbs[i].num_entries = txn->dbs[i].num_entries;
        }
    }
    db->free_pgno = txn->free_pgno;
    db->num_pages = txn->num_pages;
    meta_write(db);
    db->write_txn = NULL;
    watch_snap = watch_snapshot_locked(db, &watch_count);
    SAP_MUTEX_UNLOCK(db->write_mutex);
    txn_notify_watchers(txn, watch_snap, watch_count);
    watch_snapshot_free(watch_snap, watch_count);
    txn_free_mem(txn);
    return SAP_OK;
}

static void txn_abort_free_untracked_new_pages(struct Txn *txn)
{
    struct DB *db;
    uint32_t pgno;
    uint32_t steps = 0;
    uint32_t max_steps;

    if (!txn)
        return;
    db = txn->db;
    pgno = txn->free_pgno;
    max_steps = txn->num_pages ? txn->num_pages : 1u;

    while (pgno != INVALID_PGNO && steps <= max_steps)
    {
        void *pg;
        uint32_t next;
        if (pgno >= txn->saved_npages && !u32_find(txn->new_pages, txn->new_cnt, pgno, NULL))
        {
            if (pgno >= db->pages_cap || !db->pages[pgno])
                break;
            pg = db->pages[pgno];
            next = rd32(pg);
            db->alloc->free_page(db->alloc->ctx, pg, db->page_size);
            db->pages[pgno] = NULL;
            pgno = next;
        }
        else
        {
            if (pgno >= db->pages_cap || !db->pages[pgno])
                break;
            pgno = rd32(db->pages[pgno]);
        }
        steps++;
    }
}

void txn_abort(Txn *txn_pub)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    struct DB *db = txn->db;
    if (txn->flags & TXN_RDONLY)
    {
        db_remove_reader(db, txn->txnid);
        txn_free_mem(txn);
        return;
    }

    txn_abort_free_untracked_new_pages(txn);

    for (uint32_t i = 0; i < txn->new_cnt; i++)
    {
        uint32_t pgno = txn->new_pages[i];
        if (pgno >= db->num_pages)
        {
            db->alloc->free_page(db->alloc->ctx, db->pages[pgno], db->page_size);
            db->pages[pgno] = NULL;
        }
        else
        {
            uint32_t *fh = txn->parent ? &txn->parent->free_pgno : &db->free_pgno;
            wr32(db->pages[pgno], *fh);
            *fh = pgno;
        }
    }
    if (txn->parent)
    {
        uint32_t nd = db->num_dbs;
        for (uint32_t i = 0; i < nd; i++)
        {
            txn->parent->dbs[i].root_pgno = txn->dbs[i].saved_root;
            txn->parent->dbs[i].num_entries = txn->dbs[i].saved_entries;
        }
        txn->parent->free_pgno = txn->saved_free;
        txn->parent->num_pages = txn->saved_npages;
    }
    else
    {
        SAP_MUTEX_LOCK(db->write_mutex);
        db->write_txn = NULL;
        SAP_MUTEX_UNLOCK(db->write_mutex);
    }
    txn_free_mem(txn);
}

/* ================================================================== */
/* Database lifecycle                                                   */
/* ================================================================== */

DB *db_open(PageAllocator *alloc, uint32_t page_size, keycmp_fn cmp, void *cmp_ctx)
{
    uint32_t max_dbs;
    if (!alloc || !alloc->alloc_page || !alloc->free_page)
        return NULL;
    if (page_size < 256 || page_size > UINT16_MAX)
        return NULL;
    max_dbs = meta_max_dbs(page_size);
    if (max_dbs == 0)
        return NULL;
    struct DB *db = (struct DB *)calloc(1, sizeof(*db));
    if (!db)
        return NULL;
    db->alloc = alloc;
    db->page_size = page_size;
    db->num_dbs = 1;
    db->dbs[0].root_pgno = INVALID_PGNO;
    db->dbs[0].num_entries = 0;
    db->dbs[0].cmp = cmp;
    db->dbs[0].cmp_ctx = cmp_ctx;
    SAP_MUTEX_INIT(db->write_mutex);
    SAP_MUTEX_INIT(db->reader_mutex);
    db->pages_cap = 64;
    db->pages = (void **)calloc(db->pages_cap, sizeof(void *));
    if (!db->pages)
    {
        free(db);
        return NULL;
    }
    for (int i = 0; i < 2; i++)
    {
        void *pg = alloc->alloc_page(alloc->ctx, page_size);
        if (!pg)
        {
            db_close((DB *)db);
            return NULL;
        }
        memset(pg, 0, page_size);
        db->pages[i] = pg;
    }
    db->num_pages = 2;
    if (meta_load(db) < 0)
    {
        db->txnid = 0;
        db->num_dbs = 1;
        db->dbs[0].root_pgno = INVALID_PGNO;
        db->dbs[0].num_entries = 0;
        db->free_pgno = INVALID_PGNO;
        meta_write(db);
        db->txnid = 1;
        meta_write(db);
        db->txnid = 0;
    }
    return (DB *)db;
}

int dbi_open(DB *db_pub, uint32_t dbi, keycmp_fn cmp, void *cmp_ctx, unsigned flags)
{
    struct DB *db = (struct DB *)db_pub;
    uint32_t max_dbs = meta_max_dbs(db->page_size);
    if (max_dbs > SAP_MAX_DBI)
        max_dbs = SAP_MAX_DBI;
    if (dbi >= max_dbs)
        return SAP_ERROR;

    SAP_MUTEX_LOCK(db->write_mutex);
    SAP_MUTEX_LOCK(db->reader_mutex);
    if (db->write_txn || db->num_readers)
    {
        SAP_MUTEX_UNLOCK(db->reader_mutex);
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_BUSY;
    }
    SAP_MUTEX_UNLOCK(db->reader_mutex);

    if (dbi >= db->num_dbs)
    {
        for (uint32_t i = db->num_dbs; i <= dbi; i++)
        {
            db->dbs[i].root_pgno = INVALID_PGNO;
            db->dbs[i].num_entries = 0;
            db->dbs[i].cmp = NULL;
            db->dbs[i].cmp_ctx = NULL;
            db->dbs[i].vcmp = NULL;
            db->dbs[i].vcmp_ctx = NULL;
            db->dbs[i].flags = 0;
        }
        db->num_dbs = dbi + 1;
    }
    db->dbs[dbi].cmp = cmp;
    db->dbs[dbi].cmp_ctx = cmp_ctx;
    db->dbs[dbi].flags = flags;
    SAP_MUTEX_UNLOCK(db->write_mutex);
    return SAP_OK;
}

int dbi_set_dupsort(DB *db_pub, uint32_t dbi, keycmp_fn vcmp, void *vcmp_ctx)
{
    struct DB *db = (struct DB *)db_pub;
    SAP_MUTEX_LOCK(db->write_mutex);
    SAP_MUTEX_LOCK(db->reader_mutex);
    if (db->write_txn || db->num_readers)
    {
        SAP_MUTEX_UNLOCK(db->reader_mutex);
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_BUSY;
    }
    SAP_MUTEX_UNLOCK(db->reader_mutex);
    if (dbi >= db->num_dbs)
    {
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_ERROR;
    }
    if (db_has_watch_locked(db, dbi))
    {
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_BUSY;
    }
    db->dbs[dbi].vcmp = vcmp;
    db->dbs[dbi].vcmp_ctx = vcmp_ctx;
    SAP_MUTEX_UNLOCK(db->write_mutex);
    return SAP_OK;
}

uint32_t db_num_pages(DB *db_pub)
{
    struct DB *db = (struct DB *)db_pub;
    return db ? db->num_pages : 0;
}

int db_checkpoint(DB *db_pub, sap_write_fn writer, void *ctx)
{
    struct DB *db = (struct DB *)db_pub;
    uint8_t hdr[16];
    if (!db || !writer)
        return SAP_ERROR;

    SAP_MUTEX_LOCK(db->write_mutex);
    SAP_MUTEX_LOCK(db->reader_mutex);
    if (db->write_txn || db->num_readers)
    {
        SAP_MUTEX_UNLOCK(db->reader_mutex);
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_BUSY;
    }

    wr32(hdr + 0, SNAP_MAGIC);
    wr32(hdr + 4, SNAP_VERSION);
    wr32(hdr + 8, db->page_size);
    wr32(hdr + 12, db->num_pages);
    if (writer(hdr, (uint32_t)sizeof(hdr), ctx) != 0)
    {
        SAP_MUTEX_UNLOCK(db->reader_mutex);
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_ERROR;
    }

    for (uint32_t i = 0; i < db->num_pages; i++)
    {
        if (!db->pages[i] || writer(db->pages[i], db->page_size, ctx) != 0)
        {
            SAP_MUTEX_UNLOCK(db->reader_mutex);
            SAP_MUTEX_UNLOCK(db->write_mutex);
            return SAP_ERROR;
        }
    }

    SAP_MUTEX_UNLOCK(db->reader_mutex);
    SAP_MUTEX_UNLOCK(db->write_mutex);
    return SAP_OK;
}

int db_restore(DB *db_pub, sap_read_fn reader, void *ctx)
{
    struct DB *db = (struct DB *)db_pub;
    uint8_t hdr[16];
    uint32_t snap_magic, snap_version, snap_psz, snap_npages;
    void **new_pages = NULL;
    uint32_t new_cap = 0;
    uint32_t loaded = 0;

    if (!db || !reader)
        return SAP_ERROR;

    SAP_MUTEX_LOCK(db->write_mutex);
    SAP_MUTEX_LOCK(db->reader_mutex);
    if (db->write_txn || db->num_readers)
    {
        SAP_MUTEX_UNLOCK(db->reader_mutex);
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_BUSY;
    }

    if (reader(hdr, (uint32_t)sizeof(hdr), ctx) != 0)
    {
        SAP_MUTEX_UNLOCK(db->reader_mutex);
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_ERROR;
    }
    snap_magic = rd32(hdr + 0);
    snap_version = rd32(hdr + 4);
    snap_psz = rd32(hdr + 8);
    snap_npages = rd32(hdr + 12);
    if (snap_magic != SNAP_MAGIC || snap_version != SNAP_VERSION || snap_psz != db->page_size ||
        snap_npages < 2)
    {
        SAP_MUTEX_UNLOCK(db->reader_mutex);
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_ERROR;
    }

    new_cap = 64;
    while (new_cap < snap_npages)
    {
        if (new_cap > UINT32_MAX / 2)
        {
            SAP_MUTEX_UNLOCK(db->reader_mutex);
            SAP_MUTEX_UNLOCK(db->write_mutex);
            return SAP_ERROR;
        }
        new_cap *= 2;
    }
    new_pages = (void **)calloc(new_cap, sizeof(void *));
    if (!new_pages)
    {
        SAP_MUTEX_UNLOCK(db->reader_mutex);
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_ERROR;
    }

    for (loaded = 0; loaded < snap_npages; loaded++)
    {
        void *pg = db->alloc->alloc_page(db->alloc->ctx, db->page_size);
        if (!pg || reader(pg, db->page_size, ctx) != 0)
        {
            if (pg)
                db->alloc->free_page(db->alloc->ctx, pg, db->page_size);
            for (uint32_t i = 0; i < loaded; i++)
                db->alloc->free_page(db->alloc->ctx, new_pages[i], db->page_size);
            free(new_pages);
            SAP_MUTEX_UNLOCK(db->reader_mutex);
            SAP_MUTEX_UNLOCK(db->write_mutex);
            return SAP_ERROR;
        }
        new_pages[loaded] = pg;
    }

    {
        void **old_pages = db->pages;
        uint32_t old_num_pages = db->num_pages;
        uint32_t old_pages_cap = db->pages_cap;
        void ***old_page_arrays = db->old_page_arrays;
        uint32_t old_num_old_arrays = db->num_old_arrays;
        uint32_t old_cap_old_arrays = db->cap_old_arrays;
        void *old_deferred = db->deferred;
        uint32_t old_num_deferred = db->num_deferred;
        uint32_t old_cap_deferred = db->cap_deferred;

        db->pages = new_pages;
        db->num_pages = snap_npages;
        db->pages_cap = new_cap;
        db->old_page_arrays = NULL;
        db->num_old_arrays = 0;
        db->cap_old_arrays = 0;
        db->deferred = NULL;
        db->num_deferred = 0;
        db->cap_deferred = 0;

        if (meta_load(db) < 0)
        {
            db->pages = old_pages;
            db->num_pages = old_num_pages;
            db->pages_cap = old_pages_cap;
            db->old_page_arrays = old_page_arrays;
            db->num_old_arrays = old_num_old_arrays;
            db->cap_old_arrays = old_cap_old_arrays;
            db->deferred = old_deferred;
            db->num_deferred = old_num_deferred;
            db->cap_deferred = old_cap_deferred;

            for (uint32_t i = 0; i < snap_npages; i++)
                db->alloc->free_page(db->alloc->ctx, new_pages[i], db->page_size);
            free(new_pages);
            SAP_MUTEX_UNLOCK(db->reader_mutex);
            SAP_MUTEX_UNLOCK(db->write_mutex);
            return SAP_ERROR;
        }

        if (old_pages)
        {
            uint32_t lim = old_num_pages < old_pages_cap ? old_num_pages : old_pages_cap;
            for (uint32_t i = 0; i < lim; i++)
                if (old_pages[i])
                    db->alloc->free_page(db->alloc->ctx, old_pages[i], db->page_size);
            free(old_pages);
        }
        for (uint32_t i = 0; i < old_num_old_arrays; i++)
            free(old_page_arrays[i]);
        free(old_page_arrays);
        free(old_deferred);
    }

    SAP_MUTEX_UNLOCK(db->reader_mutex);
    SAP_MUTEX_UNLOCK(db->write_mutex);
    return SAP_OK;
}

void db_close(DB *db_pub)
{
    struct DB *db = (struct DB *)db_pub;
    if (!db)
        return;
    if (db->write_txn)
        txn_abort((Txn *)db->write_txn);
    if (db->pages)
    {
        uint32_t lim = db->num_pages < db->pages_cap ? db->num_pages : db->pages_cap;
        for (uint32_t i = 0; i < lim; i++)
            if (db->pages[i])
                db->alloc->free_page(db->alloc->ctx, db->pages[i], db->page_size);
        free(db->pages);
    }
    free(db->active_readers);
    free(db->deferred);
    if (db->watches)
    {
        for (uint32_t i = 0; i < db->num_watches; i++)
            free(db->watches[i].prefix);
        free(db->watches);
    }
    for (uint32_t i = 0; i < db->num_old_arrays; i++)
        free(db->old_page_arrays[i]);
    free(db->old_page_arrays);
    SAP_MUTEX_DESTROY(db->write_mutex);
    SAP_MUTEX_DESTROY(db->reader_mutex);
    free(db);
}

/* ================================================================== */
/* Watch registration                                                   */
/* ================================================================== */

static int watch_same(const struct WatchRec *wr, uint32_t dbi, const void *prefix,
                      uint32_t prefix_len, sap_watch_fn cb, void *ctx)
{
    if (!wr)
        return 0;
    if (wr->dbi != dbi || wr->cb != cb || wr->ctx != ctx || wr->prefix_len != prefix_len)
        return 0;
    if (prefix_len == 0)
        return 1;
    return memcmp(wr->prefix, prefix, prefix_len) == 0;
}

static int db_has_watch_locked(const struct DB *db, uint32_t dbi)
{
    if (!db)
        return 0;
    for (uint32_t i = 0; i < db->num_watches; i++)
        if (db->watches[i].dbi == dbi)
            return 1;
    return 0;
}

int db_watch_dbi(DB *db_pub, uint32_t dbi, const void *prefix, uint32_t prefix_len, sap_watch_fn cb,
                 void *ctx)
{
    struct DB *db = (struct DB *)db_pub;
    struct WatchRec *wr;
    if (!db || !cb)
        return SAP_ERROR;
    if (!prefix && prefix_len > 0)
        return SAP_ERROR;

    SAP_MUTEX_LOCK(db->write_mutex);
    if (db->write_txn)
    {
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_BUSY;
    }
    if (dbi >= db->num_dbs)
    {
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_ERROR;
    }
    if (db->dbs[dbi].flags & DBI_DUPSORT)
    {
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_ERROR;
    }
    for (uint32_t i = 0; i < db->num_watches; i++)
    {
        if (watch_same(&db->watches[i], dbi, prefix, prefix_len, cb, ctx))
        {
            SAP_MUTEX_UNLOCK(db->write_mutex);
            return SAP_EXISTS;
        }
    }
    if (db->num_watches >= db->cap_watches)
    {
        uint32_t nc = db->cap_watches ? db->cap_watches * 2u : 8u;
        struct WatchRec *nw = (struct WatchRec *)realloc(db->watches, (size_t)nc * sizeof(*nw));
        if (!nw)
        {
            SAP_MUTEX_UNLOCK(db->write_mutex);
            return SAP_ERROR;
        }
        db->watches = nw;
        db->cap_watches = nc;
    }

    wr = &db->watches[db->num_watches];
    memset(wr, 0, sizeof(*wr));
    wr->dbi = dbi;
    wr->prefix_len = prefix_len;
    wr->cb = cb;
    wr->ctx = ctx;
    if (prefix_len > 0)
    {
        wr->prefix = (uint8_t *)malloc(prefix_len);
        if (!wr->prefix)
        {
            SAP_MUTEX_UNLOCK(db->write_mutex);
            return SAP_ERROR;
        }
        memcpy(wr->prefix, prefix, prefix_len);
    }
    db->num_watches++;
    SAP_MUTEX_UNLOCK(db->write_mutex);
    return SAP_OK;
}

int db_unwatch_dbi(DB *db_pub, uint32_t dbi, const void *prefix, uint32_t prefix_len,
                   sap_watch_fn cb, void *ctx)
{
    struct DB *db = (struct DB *)db_pub;
    if (!db || !cb)
        return SAP_ERROR;
    if (!prefix && prefix_len > 0)
        return SAP_ERROR;

    SAP_MUTEX_LOCK(db->write_mutex);
    if (db->write_txn)
    {
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_BUSY;
    }
    if (dbi >= db->num_dbs)
    {
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_ERROR;
    }
    for (uint32_t i = 0; i < db->num_watches; i++)
    {
        struct WatchRec *wr = &db->watches[i];
        if (!watch_same(wr, dbi, prefix, prefix_len, cb, ctx))
            continue;

        free(wr->prefix);
        if (i + 1 < db->num_watches)
            memmove(&db->watches[i], &db->watches[i + 1],
                    (size_t)(db->num_watches - i - 1) * sizeof(*db->watches));
        db->num_watches--;
        SAP_MUTEX_UNLOCK(db->write_mutex);
        return SAP_OK;
    }
    SAP_MUTEX_UNLOCK(db->write_mutex);
    return SAP_NOTFOUND;
}

int db_watch(DB *db_pub, const void *prefix, uint32_t prefix_len, sap_watch_fn cb, void *ctx)
{
    return db_watch_dbi(db_pub, 0, prefix, prefix_len, cb, ctx);
}

int db_unwatch(DB *db_pub, const void *prefix, uint32_t prefix_len, sap_watch_fn cb, void *ctx)
{
    return db_unwatch_dbi(db_pub, 0, prefix, prefix_len, cb, ctx);
}

/* ================================================================== */
/* Statistics                                                           */
/* ================================================================== */

static uint32_t tree_depth(struct DB *db, uint32_t root_pgno)
{
    if (root_pgno == INVALID_PGNO)
        return 0;
    uint32_t d = 1, pgno = root_pgno;
    while (PG_TYPE(db->pages[pgno]) == PAGE_INTERNAL)
    {
        pgno = I_LEFT(db->pages[pgno]);
        d++;
    }
    return d;
}

int db_stat(DB *db_pub, SapStat *stat)
{
    if (!db_pub || !stat)
        return SAP_ERROR;
    struct DB *db = (struct DB *)db_pub;
    SAP_MUTEX_LOCK(db->write_mutex);
    stat->num_entries = db->dbs[0].num_entries;
    stat->txnid = db->txnid;
    stat->tree_depth = tree_depth(db, db->dbs[0].root_pgno);
    stat->num_pages = db->num_pages;
    stat->page_size = db->page_size;
    stat->has_write_txn = (db->write_txn != NULL) ? 1 : 0;
    SAP_MUTEX_UNLOCK(db->write_mutex);
    return SAP_OK;
}

int txn_stat(Txn *txn_pub, SapStat *stat)
{
    if (!txn_pub || !stat)
        return SAP_ERROR;
    struct Txn *txn = (struct Txn *)txn_pub;
    struct DB *db = txn->db;
    stat->num_entries = txn->dbs[0].num_entries;
    stat->txnid = txn->txnid;
    stat->tree_depth = tree_depth(db, txn->dbs[0].root_pgno);
    stat->num_pages = txn->num_pages;
    stat->page_size = db->page_size;
    stat->has_write_txn = (db->write_txn != NULL) ? 1 : 0;
    return SAP_OK;
}

int dbi_stat(Txn *txn_pub, uint32_t dbi, SapStat *stat)
{
    if (!txn_pub || !stat)
        return SAP_ERROR;
    struct Txn *txn = (struct Txn *)txn_pub;
    struct DB *db = txn->db;
    if (dbi >= db->num_dbs)
        return SAP_ERROR;
    stat->num_entries = txn->dbs[dbi].num_entries;
    stat->txnid = txn->txnid;
    stat->tree_depth = tree_depth(db, txn->dbs[dbi].root_pgno);
    stat->num_pages = txn->num_pages;
    stat->page_size = db->page_size;
    stat->has_write_txn = (db->write_txn != NULL) ? 1 : 0;
    return SAP_OK;
}

/* ================================================================== */
/* Cursor                                                               */
/* ================================================================== */

Cursor *cursor_open_dbi(Txn *txn_pub, uint32_t dbi)
{
    struct Txn *txn = (struct Txn *)txn_pub;
    if (!txn || dbi >= txn->db->num_dbs)
        return NULL;
    struct Cursor *c = (struct Cursor *)calloc(1, sizeof(*c));
    if (!c)
        return NULL;
    c->txn = txn;
    c->dbi = dbi;
    c->depth = -1;
    return (Cursor *)c;
}

Cursor *cursor_open(Txn *txn_pub) { return cursor_open_dbi(txn_pub, 0); }

void cursor_close(Cursor *c) { free(c); }

int cursor_renew(Cursor *cp, Txn *txn_pub)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct Txn *txn = (struct Txn *)txn_pub;
    if (!c || !txn)
        return SAP_ERROR;
    if (c->txn && c->txn->db != txn->db)
        return SAP_ERROR;
    if (c->dbi >= txn->db->num_dbs)
        return SAP_ERROR;
    c->txn = txn;
    c->depth = -1;
    memset(c->stack, 0, sizeof(c->stack));
    memset(c->idx, 0, sizeof(c->idx));
    return SAP_OK;
}

static void cursor_go_leftmost(struct Cursor *c, uint32_t pgno)
{
    struct DB *db = c->txn->db;
    while (PG_TYPE(db->pages[pgno]) == PAGE_INTERNAL)
    {
        void *pg = db->pages[pgno];
        c->stack[c->depth] = pgno;
        c->idx[c->depth] = 0;
        c->depth++;
        pgno = I_LEFT(pg);
    }
    c->stack[c->depth] = pgno;
    c->idx[c->depth] = 0;
}

static void cursor_go_rightmost(struct Cursor *c, uint32_t pgno)
{
    struct DB *db = c->txn->db;
    while (PG_TYPE(db->pages[pgno]) == PAGE_INTERNAL)
    {
        void *pg = db->pages[pgno];
        int n = (int)PG_NUM(pg);
        c->stack[c->depth] = pgno;
        c->idx[c->depth] = n;
        c->depth++;
        pgno = int_child(pg, n);
    }
    void *lpg = db->pages[pgno];
    int n = (int)PG_NUM(lpg);
    c->stack[c->depth] = pgno;
    c->idx[c->depth] = (n > 0) ? n - 1 : 0;
}

int cursor_first(Cursor *cp)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct Txn *txn = c->txn;
    uint32_t dbi = c->dbi;
    if (txn->dbs[dbi].root_pgno == INVALID_PGNO)
    {
        c->depth = -1;
        return SAP_NOTFOUND;
    }
    c->depth = 0;
    cursor_go_leftmost(c, txn->dbs[dbi].root_pgno);
    if (PG_NUM(txn->db->pages[c->stack[c->depth]]) == 0)
    {
        c->depth = -1;
        return SAP_NOTFOUND;
    }
    return SAP_OK;
}

int cursor_last(Cursor *cp)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct Txn *txn = c->txn;
    uint32_t dbi = c->dbi;
    if (txn->dbs[dbi].root_pgno == INVALID_PGNO)
    {
        c->depth = -1;
        return SAP_NOTFOUND;
    }
    c->depth = 0;
    cursor_go_rightmost(c, txn->dbs[dbi].root_pgno);
    if (PG_NUM(txn->db->pages[c->stack[c->depth]]) == 0)
    {
        c->depth = -1;
        return SAP_NOTFOUND;
    }
    return SAP_OK;
}

int cursor_seek(Cursor *cp, const void *key, uint32_t key_len)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct Txn *txn = c->txn;
    struct DB *db = txn->db;
    uint32_t dbi = c->dbi;
    c->depth = -1;
    if (txn->dbs[dbi].root_pgno == INVALID_PGNO)
        return SAP_NOTFOUND;
    uint32_t pgno = txn->dbs[dbi].root_pgno;
    c->depth = 0;
    while (PG_TYPE(db->pages[pgno]) == PAGE_INTERNAL)
    {
        void *pg = db->pages[pgno];
        int idx = int_find_child(db, dbi, pg, key, key_len);
        c->stack[c->depth] = pgno;
        c->idx[c->depth] = idx;
        c->depth++;
        pgno = int_child(pg, idx);
    }
    void *lpg = db->pages[pgno];
    int found;
    int pos = leaf_find(db, dbi, lpg, key, key_len, &found);
    c->stack[c->depth] = pgno;
    c->idx[c->depth] = pos;
    if (pos >= (int)PG_NUM(lpg))
        return cursor_next(cp);
    return SAP_OK;
}

int cursor_next(Cursor *cp)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct DB *db = c->txn->db;
    if (c->depth < 0)
        return SAP_NOTFOUND;

    void *lpg = db->pages[c->stack[c->depth]];
    c->idx[c->depth]++;
    if (c->idx[c->depth] < (int)PG_NUM(lpg))
        return SAP_OK;

    for (int d = c->depth - 1; d >= 0; d--)
    {
        void *pg = db->pages[c->stack[d]];
        if (c->idx[d] < (int)PG_NUM(pg))
        {
            c->idx[d]++;
            uint32_t child = int_child(pg, c->idx[d]);
            c->depth = d + 1;
            while (PG_TYPE(db->pages[child]) == PAGE_INTERNAL)
            {
                void *cpg = db->pages[child];
                c->stack[c->depth] = child;
                c->idx[c->depth] = 0;
                c->depth++;
                child = I_LEFT(cpg);
            }
            c->stack[c->depth] = child;
            c->idx[c->depth] = 0;
            if (PG_NUM(db->pages[child]) == 0)
            {
                c->depth = -1;
                return SAP_NOTFOUND;
            }
            return SAP_OK;
        }
    }
    c->depth = -1;
    return SAP_NOTFOUND;
}

int cursor_prev(Cursor *cp)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct DB *db = c->txn->db;
    if (c->depth < 0)
        return SAP_NOTFOUND;

    if (c->idx[c->depth] > 0)
    {
        c->idx[c->depth]--;
        return SAP_OK;
    }

    for (int d = c->depth - 1; d >= 0; d--)
    {
        if (c->idx[d] > 0)
        {
            c->idx[d]--;
            void *pg = db->pages[c->stack[d]];
            uint32_t child = int_child(pg, c->idx[d]);
            c->depth = d + 1;
            while (PG_TYPE(db->pages[child]) == PAGE_INTERNAL)
            {
                void *cpg = db->pages[child];
                int cn = (int)PG_NUM(cpg);
                c->stack[c->depth] = child;
                c->idx[c->depth] = cn;
                c->depth++;
                child = int_child(cpg, cn);
            }
            void *leaf = db->pages[child];
            int ln = (int)PG_NUM(leaf);
            c->stack[c->depth] = child;
            c->idx[c->depth] = (ln > 0) ? ln - 1 : 0;
            if (ln == 0)
            {
                c->depth = -1;
                return SAP_NOTFOUND;
            }
            return SAP_OK;
        }
    }
    c->depth = -1;
    return SAP_NOTFOUND;
}

int cursor_get(Cursor *cp, const void **key_out, uint32_t *key_len_out, const void **val_out,
               uint32_t *val_len_out)
{
    struct Cursor *c = (struct Cursor *)cp;
    if (c->depth < 0)
        return SAP_NOTFOUND;
    struct DB *db = c->txn->db;
    void *lpg = db->pages[c->stack[c->depth]];
    int pos = c->idx[c->depth];
    if (pos < 0 || pos >= (int)PG_NUM(lpg))
        return SAP_NOTFOUND;
    uint16_t off = (uint16_t)L_SLOT(lpg, pos);
    uint16_t klen = L_CKLEN(lpg, off);
    if (db->dbs[c->dbi].flags & DBI_DUPSORT)
    {
        /* Decode composite: [key_len:4][key_data][val_data] stored as B+ key */
        const uint8_t *comp = L_CKEY(lpg, off);
        if (klen < 4)
            return SAP_ERROR;
        uint32_t orig_kl = rd32(comp);
        *key_out = comp + 4;
        *key_len_out = orig_kl;
        *val_out = comp + 4 + orig_kl;
        *val_len_out = klen - 4 - orig_kl;
    }
    else
    {
        uint16_t vlen;
        const void *val_ptr;
        *key_out = L_CKEY(lpg, off);
        *key_len_out = klen;
        vlen = L_CVLEN(lpg, off);
        val_ptr = L_CVAL(lpg, off, klen);
        if (vlen == OVERFLOW_VALUE_SENTINEL)
            return overflow_read_value(c->txn, val_ptr, val_out, val_len_out);
        *val_out = val_ptr;
        *val_len_out = vlen;
    }
    return SAP_OK;
}

int cursor_get_key(Cursor *cp, const void **key_out, uint32_t *key_len_out)
{
    struct Cursor *c = (struct Cursor *)cp;
    if (!c || !key_out || !key_len_out)
        return SAP_ERROR;
    if (c->depth < 0)
        return SAP_NOTFOUND;
    struct DB *db = c->txn->db;
    void *lpg = db->pages[c->stack[c->depth]];
    int pos = c->idx[c->depth];
    if (pos < 0 || pos >= (int)PG_NUM(lpg))
        return SAP_NOTFOUND;
    uint16_t off = (uint16_t)L_SLOT(lpg, pos);
    uint16_t klen = L_CKLEN(lpg, off);

    if (db->dbs[c->dbi].flags & DBI_DUPSORT)
    {
        const uint8_t *comp = L_CKEY(lpg, off);
        if (klen < 4)
            return SAP_ERROR;
        uint32_t orig_kl = rd32(comp);
        if (orig_kl > (uint32_t)(klen - 4))
            return SAP_ERROR;
        *key_out = comp + 4;
        *key_len_out = orig_kl;
    }
    else
    {
        *key_out = L_CKEY(lpg, off);
        *key_len_out = klen;
    }
    return SAP_OK;
}

/* ================================================================== */
/* Cursor mutations                                                     */
/* ================================================================== */

static uint32_t cow_path(struct Cursor *c)
{
    struct Txn *txn = c->txn;
    struct DB *db = txn->db;
    int depth = c->depth;
    uint32_t dbi = c->dbi;

    uint32_t leaf_pgno = txn_cow(txn, c->stack[depth]);
    if (leaf_pgno == INVALID_PGNO)
        return INVALID_PGNO;
    c->stack[depth] = leaf_pgno;

    uint32_t child = leaf_pgno;
    for (int d = depth - 1; d >= 0; d--)
    {
        uint32_t pp = txn_cow(txn, c->stack[d]);
        if (pp == INVALID_PGNO)
            return INVALID_PGNO;
        c->stack[d] = pp;
        void *par = db->pages[pp];
        int ci = c->idx[d];
        if (ci == 0)
            SET_I_LEFT(par, child);
        else
            SET_I_CRIGHT(par, I_SLOT(par, ci - 1), child);
        child = pp;
    }

    if (depth > 0)
        txn->dbs[dbi].root_pgno = c->stack[0];
    else
        txn->dbs[dbi].root_pgno = leaf_pgno;
    return leaf_pgno;
}

int cursor_put(Cursor *cp, const void *val, uint32_t val_len, unsigned flags)
{
    struct Cursor *c = (struct Cursor *)cp;
    if (c->depth < 0)
        return SAP_NOTFOUND;
    struct Txn *txn = c->txn;
    struct ScratchMark scratch_mark = txn_scratch_mark(txn);
    if (txn->flags & TXN_RDONLY)
        return SAP_READONLY;
    if (val_len > UINT16_MAX)
        return SAP_FULL;
    struct DB *db = txn->db;
    uint32_t dbi = c->dbi;
    if (flags != 0)
        return SAP_ERROR;
    if (db->dbs[dbi].flags & DBI_DUPSORT)
        return SAP_ERROR;

    void *orig_lpg = db->pages[c->stack[c->depth]];
    int pos = c->idx[c->depth];
    if (pos < 0 || pos >= (int)PG_NUM(orig_lpg))
        return SAP_NOTFOUND;
    uint16_t off = (uint16_t)L_SLOT(orig_lpg, pos);
    uint16_t klen = L_CKLEN(orig_lpg, off);
    uint16_t old_vlen = L_CVLEN(orig_lpg, off);
    uint16_t store_vlen = (uint16_t)val_len;
    uint32_t free_after_remove;
    uint32_t need_after_insert;
    uint8_t *key_buf = txn_scratch_copy(txn, L_CKEY(orig_lpg, off), klen);
    int rc;
    if (!key_buf)
        return SAP_ERROR;

    if (SLOT_SZ + leaf_cell_size(klen, store_vlen) + LEAF_HDR > db->page_size)
    {
        if (SLOT_SZ + leaf_cell_size(klen, OVERFLOW_VALUE_SENTINEL) + LEAF_HDR > db->page_size)
        {
            txn_scratch_release(txn, scratch_mark);
            return SAP_FULL;
        }
        store_vlen = OVERFLOW_VALUE_SENTINEL;
    }

    free_after_remove = L_FREE(orig_lpg) + SLOT_SZ + leaf_cell_size(klen, old_vlen);
    need_after_insert = SLOT_SZ + leaf_cell_size(klen, store_vlen);
    if (store_vlen == OVERFLOW_VALUE_SENTINEL || need_after_insert > free_after_remove)
    {
        rc = txn_put_flags_dbi((Txn *)txn, dbi, key_buf, klen, val, val_len, 0, NULL);
        if (rc == SAP_OK)
            rc = cursor_seek(cp, key_buf, klen);
        txn_scratch_release(txn, scratch_mark);
        return rc;
    }

    uint32_t leaf_pgno = cow_path(c);
    if (leaf_pgno == INVALID_PGNO)
        return SAP_ERROR;
    void *lpg = db->pages[leaf_pgno];

    off = (uint16_t)L_SLOT(lpg, pos);
    if (leaf_cell_mark_overflow_old(txn, lpg, off) < 0)
        return SAP_ERROR;

    leaf_remove(lpg, pos);

    if (leaf_insert(lpg, pos, key_buf, klen, val, (uint16_t)val_len, NULL) == 0)
    {
        (void)txn_track_change(txn, dbi, key_buf, klen);
        rc = SAP_OK;
        goto cleanup;
    }

    /* Unexpected leaf-fit miss: fall back to full txn_put */
    txn->dbs[dbi].num_entries--;
    rc = txn_put_flags_dbi((Txn *)txn, dbi, key_buf, klen, val, val_len, flags, NULL);
    if (rc != SAP_OK)
        goto cleanup;
    rc = cursor_seek(cp, key_buf, klen);

cleanup:
    txn_scratch_release(txn, scratch_mark);
    return rc;
}

int cursor_del(Cursor *cp)
{
    struct Cursor *c = (struct Cursor *)cp;
    if (c->depth < 0)
        return SAP_NOTFOUND;
    struct Txn *txn = c->txn;
    if (txn->flags & TXN_RDONLY)
        return SAP_READONLY;
    struct DB *db = txn->db;
    uint32_t dbi = c->dbi;

    void *orig_lpg = db->pages[c->stack[c->depth]];
    int pos = c->idx[c->depth];
    uint16_t off;
    uint16_t klen;
    const void *key_ptr;
    if (pos < 0 || pos >= (int)PG_NUM(orig_lpg))
        return SAP_NOTFOUND;
    off = (uint16_t)L_SLOT(orig_lpg, pos);
    klen = L_CKLEN(orig_lpg, off);
    key_ptr = L_CKEY(orig_lpg, off);

    uint32_t leaf_pgno = cow_path(c);
    if (leaf_pgno == INVALID_PGNO)
        return SAP_ERROR;
    void *lpg = db->pages[leaf_pgno];

    off = (uint16_t)L_SLOT(lpg, pos);
    if (leaf_cell_mark_overflow_old(txn, lpg, off) < 0)
        return SAP_ERROR;

    leaf_remove(lpg, pos);
    txn->dbs[dbi].num_entries--;
    (void)txn_track_change(txn, dbi, key_ptr, klen);

    if (PG_NUM(lpg) == 0)
    {
        txn_free_page(txn, leaf_pgno);
        int depth = c->depth;
        if (depth == 0)
        {
            txn->dbs[dbi].root_pgno = INVALID_PGNO;
            c->depth = -1;
            return SAP_OK;
        }
        for (int d = depth - 1; d >= 0; d--)
        {
            uint32_t par_pgno = c->stack[d];
            void *par = db->pages[par_pgno];
            int_remove_child(par, c->idx[d]);
            if (PG_NUM(par) > 0)
                break;
            uint32_t sole = I_LEFT(par);
            txn_free_page(txn, par_pgno);
            if (d == 0)
            {
                txn->dbs[dbi].root_pgno = sole;
                break;
            }
            uint32_t gp = c->stack[d - 1];
            void *gpg = db->pages[gp];
            int gc = c->idx[d - 1];
            if (gc == 0)
                SET_I_LEFT(gpg, sole);
            else
                SET_I_CRIGHT(gpg, I_SLOT(gpg, gc - 1), sole);
            break;
        }
        c->depth = -1;
        return SAP_OK;
    }

    /* Auto-advance: if pos < count, cursor now points to next entry */
    if (pos >= (int)PG_NUM(lpg))
    {
        c->idx[c->depth] = pos;
        if (cursor_next(cp) == SAP_NOTFOUND)
            c->depth = -1;
    }
    return SAP_OK;
}

/* ================================================================== */
/* txn_del_dup_dbi — delete specific (key, value) pair in DUPSORT DBI   */
/* ================================================================== */

int txn_del_dup_dbi(Txn *txn, uint32_t dbi, const void *key, uint32_t key_len, const void *val,
                    uint32_t val_len)
{
    struct Txn *tt = (struct Txn *)txn;
    struct ScratchMark scratch_mark;
    if (!tt)
        return SAP_ERROR;
    struct DB *db = tt->db;
    if (dbi >= db->num_dbs)
        return SAP_ERROR;
    if (!(db->dbs[dbi].flags & DBI_DUPSORT))
        return SAP_ERROR;
    /* Encode composite and delete as a normal key */
    uint64_t comp_len64 = 4ull + (uint64_t)key_len + (uint64_t)val_len;
    if (comp_len64 > UINT16_MAX)
        return SAP_NOTFOUND;
    scratch_mark = txn_scratch_mark(tt);
    uint8_t *comp = (uint8_t *)txn_scratch_alloc(tt, (uint32_t)comp_len64);
    if (!comp)
        return SAP_ERROR;
    wr32(comp, key_len);
    memcpy(comp + 4, key, key_len);
    memcpy(comp + 4 + key_len, val, val_len);
    int rc = txn_del_dbi(txn, dbi, comp, (uint32_t)comp_len64);
    txn_scratch_release(tt, scratch_mark);
    return rc;
}

/* ================================================================== */
/* Cursor DupSort helpers                                               */
/* ================================================================== */

/* Get the key portion of the current DUPSORT composite entry.
 * Returns key pointer and length, or NULL if invalid. */
static const uint8_t *dup_cur_key(struct Cursor *c, uint32_t *kl_out)
{
    struct DB *db = c->txn->db;
    if (c->depth < 0)
        return NULL;
    void *lpg = db->pages[c->stack[c->depth]];
    int pos = c->idx[c->depth];
    if (pos < 0 || pos >= (int)PG_NUM(lpg))
        return NULL;
    uint16_t off = (uint16_t)L_SLOT(lpg, pos);
    uint16_t comp_klen = L_CKLEN(lpg, off);
    if (comp_klen < 4)
        return NULL;
    const uint8_t *comp = L_CKEY(lpg, off);
    *kl_out = rd32(comp);
    return comp + 4; /* pointer to user key */
}

/* Check if current entry has the same user key as saved_key/saved_kl */
static int dup_same_key(struct Cursor *c, const void *saved_key, uint32_t saved_kl)
{
    uint32_t cur_kl;
    const uint8_t *cur_key = dup_cur_key(c, &cur_kl);
    if (!cur_key || cur_kl != saved_kl)
        return 0;
    struct DB *db = c->txn->db;
    if (db->dbs[c->dbi].cmp)
        return db->dbs[c->dbi].cmp(cur_key, cur_kl, saved_key, saved_kl, db->dbs[c->dbi].cmp_ctx) ==
               0;
    return default_cmp(cur_key, cur_kl, saved_key, saved_kl) == 0;
}

int cursor_next_dup(Cursor *cp)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct DB *db = c->txn->db;
    struct ScratchMark scratch_mark = txn_scratch_mark(c->txn);
    if (!(db->dbs[c->dbi].flags & DBI_DUPSORT))
        return SAP_ERROR;
    uint32_t saved_kl;
    const uint8_t *saved_key = dup_cur_key(c, &saved_kl);
    if (!saved_key)
        return SAP_NOTFOUND;
    /* Copy key since cursor_next may change the page */
    uint8_t *kbuf = txn_scratch_copy(c->txn, saved_key, saved_kl);
    if (!kbuf)
        return SAP_ERROR;
    if (cursor_next(cp) != SAP_OK)
    {
        txn_scratch_release(c->txn, scratch_mark);
        return SAP_NOTFOUND;
    }
    if (!dup_same_key(c, kbuf, saved_kl))
    {
        cursor_prev(cp); /* restore position */
        txn_scratch_release(c->txn, scratch_mark);
        return SAP_NOTFOUND;
    }
    txn_scratch_release(c->txn, scratch_mark);
    return SAP_OK;
}

int cursor_prev_dup(Cursor *cp)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct DB *db = c->txn->db;
    struct ScratchMark scratch_mark = txn_scratch_mark(c->txn);
    if (!(db->dbs[c->dbi].flags & DBI_DUPSORT))
        return SAP_ERROR;
    uint32_t saved_kl;
    const uint8_t *saved_key = dup_cur_key(c, &saved_kl);
    if (!saved_key)
        return SAP_NOTFOUND;
    uint8_t *kbuf = txn_scratch_copy(c->txn, saved_key, saved_kl);
    if (!kbuf)
        return SAP_ERROR;
    if (cursor_prev(cp) != SAP_OK)
    {
        txn_scratch_release(c->txn, scratch_mark);
        return SAP_NOTFOUND;
    }
    if (!dup_same_key(c, kbuf, saved_kl))
    {
        cursor_next(cp);
        txn_scratch_release(c->txn, scratch_mark);
        return SAP_NOTFOUND;
    }
    txn_scratch_release(c->txn, scratch_mark);
    return SAP_OK;
}

int cursor_first_dup(Cursor *cp)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct DB *db = c->txn->db;
    struct ScratchMark scratch_mark = txn_scratch_mark(c->txn);
    if (!(db->dbs[c->dbi].flags & DBI_DUPSORT))
        return SAP_ERROR;
    uint32_t saved_kl;
    const uint8_t *saved_key = dup_cur_key(c, &saved_kl);
    if (!saved_key)
        return SAP_NOTFOUND;
    uint8_t *kbuf = txn_scratch_copy(c->txn, saved_key, saved_kl);
    if (!kbuf)
        return SAP_ERROR;
    while (cursor_prev(cp) == SAP_OK)
    {
        if (!dup_same_key(c, kbuf, saved_kl))
        {
            cursor_next(cp);
            txn_scratch_release(c->txn, scratch_mark);
            return SAP_OK;
        }
    }
    /* Reached beginning of tree — current entry is first dup */
    cursor_first(cp);
    txn_scratch_release(c->txn, scratch_mark);
    return SAP_OK;
}

int cursor_last_dup(Cursor *cp)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct DB *db = c->txn->db;
    struct ScratchMark scratch_mark = txn_scratch_mark(c->txn);
    if (!(db->dbs[c->dbi].flags & DBI_DUPSORT))
        return SAP_ERROR;
    uint32_t saved_kl;
    const uint8_t *saved_key = dup_cur_key(c, &saved_kl);
    if (!saved_key)
        return SAP_NOTFOUND;
    uint8_t *kbuf = txn_scratch_copy(c->txn, saved_key, saved_kl);
    if (!kbuf)
        return SAP_ERROR;
    while (cursor_next(cp) == SAP_OK)
    {
        if (!dup_same_key(c, kbuf, saved_kl))
        {
            cursor_prev(cp);
            txn_scratch_release(c->txn, scratch_mark);
            return SAP_OK;
        }
    }
    cursor_last(cp);
    txn_scratch_release(c->txn, scratch_mark);
    return SAP_OK;
}

int cursor_count_dup(Cursor *cp, uint64_t *count)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct DB *db = c->txn->db;
    struct ScratchMark scratch_mark = txn_scratch_mark(c->txn);
    if (!(db->dbs[c->dbi].flags & DBI_DUPSORT))
        return SAP_ERROR;
    if (!count)
        return SAP_ERROR;
    /* Save position, go to first_dup, count forward */
    uint32_t saved_kl;
    const uint8_t *saved_key = dup_cur_key(c, &saved_kl);
    if (!saved_key)
    {
        *count = 0;
        return SAP_OK;
    }
    uint8_t *kbuf = txn_scratch_copy(c->txn, saved_key, saved_kl);
    if (!kbuf)
        return SAP_ERROR;

    /* Go to first dup */
    cursor_first_dup(cp);
    uint64_t cnt = 1;
    while (cursor_next(cp) == SAP_OK)
    {
        if (!dup_same_key(c, kbuf, saved_kl))
            break;
        cnt++;
    }
    *count = cnt;
    /* Re-seek to first dup with this key (position is approximate) */
    /* Use a composite prefix to re-seek */
    uint32_t comp_len = 4 + saved_kl;
    uint8_t *comp = (uint8_t *)txn_scratch_alloc(c->txn, comp_len);
    if (comp)
    {
        wr32(comp, saved_kl);
        memcpy(comp + 4, kbuf, saved_kl);
        cursor_seek(cp, comp, comp_len);
    }
    txn_scratch_release(c->txn, scratch_mark);
    return SAP_OK;
}

/* ================================================================== */
/* Prefix / range helpers                                               */
/* ================================================================== */

static int cursor_dupsort_key_cmp(struct Cursor *c, const void *key, uint32_t key_len, int *cmp_out)
{
    const void *cur_key;
    uint32_t cur_key_len;
    int rc;
    if (!c || !cmp_out)
        return SAP_ERROR;
    rc = cursor_get_key((Cursor *)c, &cur_key, &cur_key_len);
    if (rc != SAP_OK)
        return rc;
    *cmp_out = user_keycmp(c->txn->db, c->dbi, cur_key, cur_key_len, key, key_len);
    return SAP_OK;
}

static int cursor_seek_dupsort_key(Cursor *cp, const void *key, uint32_t key_len)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct Txn *txn;
    struct ScratchMark scratch_mark;
    uint32_t comp_len;
    uint8_t *comp;
    int rc, cmp;

    if (!c)
        return SAP_ERROR;
    txn = c->txn;
    scratch_mark = txn_scratch_mark(txn);
    comp_len = 4u + key_len;
    if (comp_len > UINT16_MAX)
    {
        txn_scratch_release(txn, scratch_mark);
        return SAP_NOTFOUND;
    }
    comp = (uint8_t *)txn_scratch_alloc(txn, comp_len);
    if (!comp)
    {
        txn_scratch_release(txn, scratch_mark);
        return SAP_ERROR;
    }
    wr32(comp, key_len);
    memcpy(comp + 4, key, key_len);
    rc = cursor_seek(cp, comp, comp_len);
    txn_scratch_release(txn, scratch_mark);

    if (rc == SAP_NOTFOUND)
    {
        rc = cursor_last(cp);
        if (rc != SAP_OK)
            return SAP_NOTFOUND;
        rc = cursor_dupsort_key_cmp(c, key, key_len, &cmp);
        if (rc != SAP_OK)
            return rc;
        if (cmp < 0)
            return SAP_NOTFOUND;
        if (cmp == 0)
            return cursor_first_dup(cp);
        /* cmp > 0: recover to first key >= target below. */
    }
    else if (rc != SAP_OK)
    {
        return rc;
    }
    else
    {
        rc = cursor_dupsort_key_cmp(c, key, key_len, &cmp);
        if (rc != SAP_OK)
            return rc;
        if (cmp == 0)
            return cursor_first_dup(cp);
        if (cmp < 0)
        {
            /* Defensive recovery: seek should not land below target. */
            do
            {
                rc = cursor_next(cp);
                if (rc != SAP_OK)
                    return rc;
                rc = cursor_dupsort_key_cmp(c, key, key_len, &cmp);
                if (rc != SAP_OK)
                    return rc;
            } while (cmp < 0);
            if (cmp == 0)
                return cursor_first_dup(cp);
            return SAP_OK;
        }
        /* cmp > 0: key may still exist immediately to the left. */
    }

    for (;;)
    {
        rc = cursor_prev(cp);
        if (rc == SAP_NOTFOUND)
            return cursor_first(cp);
        if (rc != SAP_OK)
            return rc;

        rc = cursor_dupsort_key_cmp(c, key, key_len, &cmp);
        if (rc != SAP_OK)
            return rc;
        if (cmp > 0)
            continue;
        if (cmp == 0)
            return cursor_first_dup(cp);
        return cursor_next(cp);
    }
}

int cursor_seek_prefix(Cursor *cp, const void *prefix, uint32_t prefix_len)
{
    struct Cursor *c = (struct Cursor *)cp;
    struct DB *db = c->txn->db;

    if (db->dbs[c->dbi].flags & DBI_DUPSORT)
    {
        int rc = cursor_seek_dupsort_key(cp, prefix, prefix_len);
        if (rc != SAP_OK)
            return rc;
        return cursor_in_prefix(cp, prefix, prefix_len) ? SAP_OK : SAP_NOTFOUND;
    }

    int rc = cursor_seek(cp, prefix, prefix_len);
    if (rc != SAP_OK)
        return rc;
    /* Check if the found key starts with prefix */
    const void *k, *v;
    uint32_t kl, vl;
    if (cursor_get(cp, &k, &kl, &v, &vl) != SAP_OK)
        return SAP_NOTFOUND;
    if (kl < prefix_len || memcmp(k, prefix, prefix_len) != 0)
    {
        c->depth = -1;
        return SAP_NOTFOUND;
    }
    return SAP_OK;
}

int cursor_in_prefix(Cursor *cp, const void *prefix, uint32_t prefix_len)
{
    const void *k, *v;
    uint32_t kl, vl;
    if (cursor_get(cp, &k, &kl, &v, &vl) != SAP_OK)
        return 0;
    if (kl < prefix_len)
        return 0;
    return memcmp(k, prefix, prefix_len) == 0;
}
