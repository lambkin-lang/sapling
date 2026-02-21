/*
 * test_sapling.c - comprehensive test suite for the Sapling B+ tree
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================== */
/* Minimal test allocator (thin wrapper over malloc)                    */
/* ================================================================== */

static void *test_alloc(void *ctx, uint32_t sz)
{
    (void)ctx;
    return malloc((size_t)sz);
}

static void test_free(void *ctx, void *p, uint32_t sz)
{
    (void)ctx;
    (void)sz;
    free(p);
}

static PageAllocator g_alloc = {test_alloc, test_free, NULL};

/* ================================================================== */
/* Simple test framework                                                */
/* ================================================================== */

static int g_pass = 0, g_fail = 0;

#define CHECK(expr)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (expr)                                                                                  \
        {                                                                                          \
            g_pass++;                                                                              \
        }                                                                                          \
        else                                                                                       \
        {                                                                                          \
            fprintf(stderr, "FAIL: %s  (%s:%d)\n", #expr, __FILE__, __LINE__);                     \
            g_fail++;                                                                              \
        }                                                                                          \
    } while (0)

#define SECTION(name) printf("--- %s ---\n", name)

static void print_summary(void) { printf("\nResults: %d passed, %d failed\n", g_pass, g_fail); }

/* ================================================================== */
/* Helpers                                                              */
/* ================================================================== */

static DB *new_db(void) { return db_open(&g_alloc, SAPLING_PAGE_SIZE, NULL, NULL); }

static int str_put(Txn *txn, const char *key, const char *val)
{
    return txn_put(txn, key, (uint32_t)strlen(key), val, (uint32_t)strlen(val));
}

static int str_get(Txn *txn, const char *key, const void **val_out, uint32_t *val_len_out)
{
    return txn_get(txn, key, (uint32_t)strlen(key), val_out, val_len_out);
}

static int str_del(Txn *txn, const char *key) { return txn_del(txn, key, (uint32_t)strlen(key)); }

/* Check that key maps to the given string value */
static int check_str(Txn *txn, const char *key, const char *expected)
{
    const void *v;
    uint32_t vl;
    int rc = str_get(txn, key, &v, &vl);
    if (rc != SAP_OK)
        return 0;
    if (vl != (uint32_t)strlen(expected))
        return 0;
    return memcmp(v, expected, vl) == 0;
}

static void fill_pattern(uint8_t *buf, uint32_t len, uint8_t seed)
{
    if (!buf)
        return;
    for (uint32_t i = 0; i < len; i++)
        buf[i] = (uint8_t)(seed + (uint8_t)(i * 17u));
}

struct MemBuf
{
    uint8_t *data;
    uint32_t len;
    uint32_t cap;
    uint32_t pos;
};

static int membuf_write(const void *buf, uint32_t len, void *ctx)
{
    struct MemBuf *mb = (struct MemBuf *)ctx;
    if (!mb || (!buf && len > 0))
        return -1;
    if (len == 0)
        return 0;

    if (mb->len > UINT32_MAX - len)
        return -1;
    if (mb->len + len > mb->cap)
    {
        uint32_t nc = mb->cap ? mb->cap : 256;
        while (nc < mb->len + len)
        {
            if (nc > UINT32_MAX / 2)
                return -1;
            nc *= 2;
        }
        uint8_t *nd = (uint8_t *)realloc(mb->data, nc);
        if (!nd)
            return -1;
        mb->data = nd;
        mb->cap = nc;
    }

    memcpy(mb->data + mb->len, buf, len);
    mb->len += len;
    return 0;
}

static int membuf_read(void *buf, uint32_t len, void *ctx)
{
    struct MemBuf *mb = (struct MemBuf *)ctx;
    if (!mb || (!buf && len > 0))
        return -1;
    if (mb->pos > mb->len || len > mb->len - mb->pos)
        return -1;
    if (len > 0)
        memcpy(buf, mb->data + mb->pos, len);
    mb->pos += len;
    return 0;
}

static uint32_t read_u32_le(const uint8_t *p)
{
    uint32_t v = 0;
    if (p)
        memcpy(&v, p, sizeof(v));
    return v;
}

static void write_u16_le(uint8_t *p, uint16_t v)
{
    if (p)
        memcpy(p, &v, sizeof(v));
}

static void write_u32_le(uint8_t *p, uint32_t v)
{
    if (p)
        memcpy(p, &v, sizeof(v));
}

static int snapshot_find_first_page_type(const struct MemBuf *mb, uint8_t page_type,
                                         uint32_t *page_size_out, uint32_t *page_index_out)
{
    uint32_t page_size;
    uint32_t num_pages;
    uint64_t total;
    if (!mb || !mb->data || mb->len < 16)
        return 0;
    page_size = read_u32_le(mb->data + 8);
    num_pages = read_u32_le(mb->data + 12);
    if (page_size == 0 || num_pages < 2)
        return 0;
    total = 16ull + (uint64_t)page_size * (uint64_t)num_pages;
    if (total > mb->len)
        return 0;
    for (uint32_t i = 0; i < num_pages; i++)
    {
        uint64_t base = 16ull + (uint64_t)i * (uint64_t)page_size;
        if (mb->data[base] == page_type)
        {
            if (page_size_out)
                *page_size_out = page_size;
            if (page_index_out)
                *page_index_out = i;
            return 1;
        }
    }
    return 0;
}

struct WatchEvent
{
    uint8_t key[64];
    uint32_t key_len;
    uint8_t val[64];
    uint32_t val_len;
    int has_val;
};

struct WatchLog
{
    struct WatchEvent events[32];
    uint32_t count;
};

static void watch_collect(const void *key, uint32_t key_len, const void *val, uint32_t val_len,
                          void *ctx)
{
    struct WatchLog *log = (struct WatchLog *)ctx;
    struct WatchEvent *evt;
    if (!log || log->count >= 32)
        return;
    evt = &log->events[log->count++];

    if (key_len > sizeof(evt->key))
        key_len = (uint32_t)sizeof(evt->key);
    evt->key_len = key_len;
    if (key_len > 0)
        memcpy(evt->key, key, key_len);

    evt->has_val = (val != NULL);
    if (!evt->has_val)
    {
        evt->val_len = 0;
        return;
    }

    if (val_len > sizeof(evt->val))
        val_len = (uint32_t)sizeof(evt->val);
    evt->val_len = val_len;
    if (val_len > 0)
        memcpy(evt->val, val, val_len);
}

static void merge_concat(const void *old_val, uint32_t old_len, const void *operand,
                         uint32_t op_len, void *new_val, uint32_t *new_len, void *ctx)
{
    uint32_t cap;
    uint64_t need64;
    (void)ctx;
    if (!new_len)
        return;
    cap = *new_len;
    need64 = (uint64_t)old_len + (uint64_t)op_len;
    if (need64 > UINT32_MAX)
    {
        *new_len = UINT32_MAX;
        return;
    }
    if (need64 > cap)
    {
        *new_len = (uint32_t)need64;
        return;
    }
    if (old_len > 0)
        memcpy(new_val, old_val, old_len);
    if (op_len > 0)
        memcpy((uint8_t *)new_val + old_len, operand, op_len);
    *new_len = (uint32_t)need64;
}

static void merge_clear(const void *old_val, uint32_t old_len, const void *operand, uint32_t op_len,
                        void *new_val, uint32_t *new_len, void *ctx)
{
    (void)old_val;
    (void)old_len;
    (void)operand;
    (void)op_len;
    (void)new_val;
    (void)ctx;
    if (new_len)
        *new_len = 0;
}

static void merge_overflow(const void *old_val, uint32_t old_len, const void *operand,
                           uint32_t op_len, void *new_val, uint32_t *new_len, void *ctx)
{
    (void)old_val;
    (void)old_len;
    (void)operand;
    (void)op_len;
    (void)new_val;
    (void)ctx;
    if (new_len && *new_len < UINT32_MAX)
        *new_len = *new_len + 1;
}

static void merge_too_large(const void *old_val, uint32_t old_len, const void *operand,
                            uint32_t op_len, void *new_val, uint32_t *new_len, void *ctx)
{
    (void)old_val;
    (void)old_len;
    (void)operand;
    (void)op_len;
    (void)new_val;
    (void)ctx;
    if (new_len)
        *new_len = (uint32_t)UINT16_MAX + 1u;
}

/* ================================================================== */
/* Test: basic CRUD                                                     */
/* ================================================================== */

static void test_basic_crud(void)
{
    SECTION("basic CRUD");
    DB *db = new_db();
    CHECK(db != NULL);

    Txn *txn = txn_begin(db, NULL, 0);
    CHECK(txn != NULL);

    /* Insert */
    CHECK(str_put(txn, "hello", "world") == SAP_OK);
    CHECK(str_put(txn, "foo", "bar") == SAP_OK);
    CHECK(str_put(txn, "abc", "123") == SAP_OK);

    /* Get */
    CHECK(check_str(txn, "hello", "world"));
    CHECK(check_str(txn, "foo", "bar"));
    CHECK(check_str(txn, "abc", "123"));

    /* Not found */
    const void *v;
    uint32_t vl;
    CHECK(str_get(txn, "missing", &v, &vl) == SAP_NOTFOUND);

    /* Update */
    CHECK(str_put(txn, "hello", "WORLD") == SAP_OK);
    CHECK(check_str(txn, "hello", "WORLD"));

    /* Delete */
    CHECK(str_del(txn, "foo") == SAP_OK);
    CHECK(str_get(txn, "foo", &v, &vl) == SAP_NOTFOUND);

    CHECK(txn_commit(txn) == SAP_OK);

    /* Verify after commit */
    txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(check_str(txn, "hello", "WORLD"));
    CHECK(check_str(txn, "abc", "123"));
    CHECK(str_get(txn, "foo", &v, &vl) == SAP_NOTFOUND);
    txn_abort(txn);

    db_close(db);
}

/* ================================================================== */
/* Test: empty tree edge cases                                          */
/* ================================================================== */

static void test_empty_tree(void)
{
    SECTION("empty tree");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    const void *v;
    uint32_t vl;
    CHECK(txn_get(txn, "k", 1, &v, &vl) == SAP_NOTFOUND);
    CHECK(txn_del(txn, "k", 1) == SAP_NOTFOUND);

    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_NOTFOUND);
    CHECK(cursor_last(cur) == SAP_NOTFOUND);
    CHECK(cursor_next(cur) == SAP_NOTFOUND);
    CHECK(cursor_prev(cur) == SAP_NOTFOUND);
    cursor_close(cur);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: single element                                                 */
/* ================================================================== */

static void test_single_element(void)
{
    SECTION("single element");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);
    CHECK(str_put(txn, "only", "one") == SAP_OK);

    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    const void *k, *v;
    uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 4 && memcmp(k, "only", 4) == 0);
    CHECK(vl == 3 && memcmp(v, "one", 3) == 0);
    CHECK(cursor_next(cur) == SAP_NOTFOUND);
    cursor_close(cur);

    cur = cursor_open(txn);
    CHECK(cursor_last(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 4 && memcmp(k, "only", 4) == 0);
    CHECK(cursor_prev(cur) == SAP_NOTFOUND);
    cursor_close(cur);

    /* Delete the only element */
    CHECK(str_del(txn, "only") == SAP_OK);
    const void *v2;
    uint32_t vl2;
    CHECK(str_get(txn, "only", &v2, &vl2) == SAP_NOTFOUND);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: ordered keys and range scans                                  */
/* ================================================================== */

static void test_range_scan(void)
{
    SECTION("range scan");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    /* Insert keys out of order */
    const char *keys[] = {"d", "b", "f", "a", "c", "e", "g"};
    for (int i = 0; i < 7; i++)
        CHECK(str_put(txn, keys[i], keys[i]) == SAP_OK);

    /* Forward scan should give alphabetical order */
    const char *expected[] = {"a", "b", "c", "d", "e", "f", "g"};
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    for (int i = 0; i < 7; i++)
    {
        const void *k, *v;
        uint32_t kl, vl;
        CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
        CHECK(kl == 1 && *(char *)k == expected[i][0]);
        if (i < 6)
            CHECK(cursor_next(cur) == SAP_OK);
    }
    CHECK(cursor_next(cur) == SAP_NOTFOUND);
    cursor_close(cur);

    /* Backward scan */
    cur = cursor_open(txn);
    CHECK(cursor_last(cur) == SAP_OK);
    for (int i = 6; i >= 0; i--)
    {
        const void *k, *v;
        uint32_t kl, vl;
        CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
        CHECK(kl == 1 && *(char *)k == expected[i][0]);
        if (i > 0)
            CHECK(cursor_prev(cur) == SAP_OK);
    }
    CHECK(cursor_prev(cur) == SAP_NOTFOUND);
    cursor_close(cur);

    /* Seek */
    cur = cursor_open(txn);
    CHECK(cursor_seek(cur, "c", 1) == SAP_OK);
    const void *k, *v;
    uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && *(char *)k == 'c');
    cursor_close(cur);

    /* Seek to non-existent key between entries */
    cur = cursor_open(txn);
    CHECK(cursor_seek(cur, "bb", 2) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && *(char *)k == 'c');
    cursor_close(cur);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: large dataset (forces multiple splits)                         */
/* ================================================================== */

static void test_large_dataset(void)
{
    SECTION("large dataset (10000 keys)");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    int N = 10000;
    char kbuf[32], vbuf[32];

    /* Insert in a pseudo-random order using a simple LCG */
    /* We insert keys "000000".."009999" in shuffled order */
    uint32_t state = 12345;
    int *order = (int *)malloc((size_t)N * sizeof(int));
    for (int i = 0; i < N; i++)
        order[i] = i;
    for (int i = N - 1; i > 0; i--)
    {
        state = state * 1664525u + 1013904223u;
        int j = (int)(state % (uint32_t)(i + 1));
        int tmp = order[i];
        order[i] = order[j];
        order[j] = tmp;
    }

    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "%06d", order[i]);
        snprintf(vbuf, sizeof(vbuf), "val%06d", order[i]);
        CHECK(txn_put(txn, kbuf, 6, vbuf, (uint32_t)strlen(vbuf)) == SAP_OK);
    }
    free(order);

    /* Verify all keys */
    int errors = 0;
    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "%06d", i);
        snprintf(vbuf, sizeof(vbuf), "val%06d", i);
        const void *v;
        uint32_t vl;
        int rc = txn_get(txn, kbuf, 6, &v, &vl);
        if (rc != SAP_OK || vl != strlen(vbuf) || memcmp(v, vbuf, vl) != 0)
            errors++;
    }
    CHECK(errors == 0);

    /* Scan: should visit all N keys in order */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    int count = 0;
    int scan_err = 0;
    do
    {
        const void *k, *v;
        uint32_t kl, vl;
        if (cursor_get(cur, &k, &kl, &v, &vl) != SAP_OK)
        {
            scan_err++;
            break;
        }
        snprintf(kbuf, sizeof(kbuf), "%06d", count);
        if (kl != 6 || memcmp(k, kbuf, 6) != 0)
            scan_err++;
        count++;
    } while (cursor_next(cur) == SAP_OK);
    cursor_close(cur);
    CHECK(count == N);
    CHECK(scan_err == 0);

    CHECK(txn_commit(txn) == SAP_OK);
    db_close(db);
}

/* ================================================================== */
/* Test: MVCC snapshot isolation                                        */
/* ================================================================== */

static void test_snapshot_isolation(void)
{
    SECTION("MVCC snapshot isolation");
    DB *db = new_db();

    /* Committed baseline */
    Txn *w = txn_begin(db, NULL, 0);
    CHECK(str_put(w, "x", "original") == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    /* Read-only snapshot */
    Txn *r = txn_begin(db, NULL, TXN_RDONLY);

    /* Write transaction modifies x */
    w = txn_begin(db, NULL, 0);
    CHECK(str_put(w, "x", "modified") == SAP_OK);
    CHECK(str_put(w, "y", "new") == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    /* Read snapshot still sees original */
    CHECK(check_str(r, "x", "original"));
    const void *v;
    uint32_t vl;
    CHECK(str_get(r, "y", &v, &vl) == SAP_NOTFOUND);
    txn_abort(r);

    /* New read sees updated values */
    r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(check_str(r, "x", "modified"));
    CHECK(check_str(r, "y", "new"));
    txn_abort(r);

    db_close(db);
}

/* ================================================================== */
/* Test: nested transaction commit                                      */
/* ================================================================== */

static void test_nested_commit(void)
{
    SECTION("nested transaction commit");
    DB *db = new_db();

    Txn *outer = txn_begin(db, NULL, 0);
    CHECK(str_put(outer, "a", "1") == SAP_OK);

    Txn *inner = txn_begin(db, outer, 0);
    CHECK(str_put(inner, "b", "2") == SAP_OK);
    /* Inner sees outer's write */
    CHECK(check_str(inner, "a", "1"));

    CHECK(txn_commit(inner) == SAP_OK); /* child commit: visible to outer */

    /* Outer now sees inner's write */
    CHECK(check_str(outer, "b", "2"));
    CHECK(check_str(outer, "a", "1"));

    CHECK(txn_commit(outer) == SAP_OK);

    /* DB has both */
    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(check_str(r, "a", "1"));
    CHECK(check_str(r, "b", "2"));
    txn_abort(r);

    db_close(db);
}

/* ================================================================== */
/* Test: nested transaction abort                                       */
/* ================================================================== */

static void test_nested_abort(void)
{
    SECTION("nested transaction abort");
    DB *db = new_db();

    Txn *outer = txn_begin(db, NULL, 0);
    CHECK(str_put(outer, "stable", "yes") == SAP_OK);

    Txn *inner = txn_begin(db, outer, 0);
    CHECK(str_put(inner, "volatile", "no") == SAP_OK);
    CHECK(str_put(inner, "stable", "overwrite") == SAP_OK);
    txn_abort(inner); /* discard all inner changes */

    /* Outer still sees its original writes */
    CHECK(check_str(outer, "stable", "yes"));
    const void *v;
    uint32_t vl;
    CHECK(str_get(outer, "volatile", &v, &vl) == SAP_NOTFOUND);

    CHECK(txn_commit(outer) == SAP_OK);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(check_str(r, "stable", "yes"));
    CHECK(str_get(r, "volatile", &v, &vl) == SAP_NOTFOUND);
    txn_abort(r);

    db_close(db);
}

/* ================================================================== */
/* Test: deeply nested transactions                                     */
/* ================================================================== */

static void test_deep_nested(void)
{
    SECTION("deeply nested transactions");
    DB *db = new_db();

    /*
     * Interleave begins and writes so each child inherits the parent's
     * current working set (including the parent's own write):
     *   t0 writes k0, then t1 begins (sees k0), t1 writes k1, ...
     * t6 and t7 are then aborted; t0..t5 are committed inner→outer.
     */
    Txn *t[8];
    t[0] = txn_begin(db, NULL, 0);
    for (int i = 0; i < 8; i++)
    {
        char key[4] = {'k', (char)('0' + i), 0};
        char val[4] = {'v', (char)('0' + i), 0};
        CHECK(str_put(t[i], key, val) == SAP_OK);
        if (i < 7)
            t[i + 1] = txn_begin(db, t[i], 0);
    }

    /* Each child can read its own and all ancestors' writes */
    CHECK(check_str(t[5], "k0", "v0"));
    CHECK(check_str(t[5], "k5", "v5"));

    txn_abort(t[7]);
    txn_abort(t[6]);

    /* Commit 5 → 4 → 3 → 2 → 1 → 0 (inner-first) */
    for (int i = 5; i >= 0; i--)
        CHECK(txn_commit(t[i]) == SAP_OK);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    for (int i = 0; i <= 5; i++)
    {
        char key[4] = {'k', (char)('0' + i), 0};
        char val[4] = {'v', (char)('0' + i), 0};
        CHECK(check_str(r, key, val));
    }
    /* k6 and k7 were aborted */
    const void *v;
    uint32_t vl;
    CHECK(str_get(r, "k6", &v, &vl) == SAP_NOTFOUND);
    CHECK(str_get(r, "k7", &v, &vl) == SAP_NOTFOUND);
    txn_abort(r);

    db_close(db);
}

/* ================================================================== */
/* Test: free-list page recycling                                       */
/* ================================================================== */

static void test_freelist_recycling(void)
{
    SECTION("free-list page recycling");
    DB *db = new_db();

    int N = 2000;
    char kbuf[32], vbuf[32];

    /* Phase 1: insert N keys */
    Txn *txn = txn_begin(db, NULL, 0);
    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "recycle%04d", i);
        snprintf(vbuf, sizeof(vbuf), "val%04d", i);
        CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf), vbuf, (uint32_t)strlen(vbuf)) == SAP_OK);
    }
    CHECK(txn_commit(txn) == SAP_OK);
    uint32_t pages_after_insert = db_num_pages(db);

    /* Phase 2: delete all N keys */
    txn = txn_begin(db, NULL, 0);
    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "recycle%04d", i);
        txn_del(txn, kbuf, (uint32_t)strlen(kbuf));
    }
    CHECK(txn_commit(txn) == SAP_OK);

    /* Phase 3: insert N keys again */
    txn = txn_begin(db, NULL, 0);
    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "recycle%04d", i);
        snprintf(vbuf, sizeof(vbuf), "new%04d", i);
        CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf), vbuf, (uint32_t)strlen(vbuf)) == SAP_OK);
    }
    CHECK(txn_commit(txn) == SAP_OK);

    /* Page count should not have grown significantly beyond first-insert peak */
    uint32_t pages_after_reinsertion = db_num_pages(db);
    /* Allow up to 20% growth over baseline to account for meta/overhead variation */
    CHECK(pages_after_reinsertion <= pages_after_insert * 12 / 10);

    /* Verify data */
    txn = txn_begin(db, NULL, TXN_RDONLY);
    int errors = 0;
    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "recycle%04d", i);
        snprintf(vbuf, sizeof(vbuf), "new%04d", i);
        const void *v;
        uint32_t vl;
        int rc = txn_get(txn, kbuf, (uint32_t)strlen(kbuf), &v, &vl);
        if (rc != SAP_OK || vl != strlen(vbuf) || memcmp(v, vbuf, vl) != 0)
            errors++;
    }
    CHECK(errors == 0);
    txn_abort(txn);

    db_close(db);
}

/* ================================================================== */
/* Test: txn abort discards writes                                      */
/* ================================================================== */

static void test_txn_abort(void)
{
    SECTION("transaction abort");
    DB *db = new_db();

    /* Commit a baseline */
    Txn *t = txn_begin(db, NULL, 0);
    CHECK(str_put(t, "base", "value") == SAP_OK);
    CHECK(txn_commit(t) == SAP_OK);

    /* Abort a write txn */
    t = txn_begin(db, NULL, 0);
    CHECK(str_put(t, "aborted", "gone") == SAP_OK);
    CHECK(str_del(t, "base") == SAP_OK);
    txn_abort(t);

    /* Baseline is intact; aborted key does not exist */
    t = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(check_str(t, "base", "value"));
    const void *v;
    uint32_t vl;
    CHECK(str_get(t, "aborted", &v, &vl) == SAP_NOTFOUND);
    txn_abort(t);

    db_close(db);
}

/* ================================================================== */
/* Test: read-only flag prevents writes                                 */
/* ================================================================== */

static void test_readonly_flag(void)
{
    SECTION("read-only transaction");
    DB *db = new_db();

    Txn *w = txn_begin(db, NULL, 0);
    str_put(w, "k", "v");
    txn_commit(w);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(str_put(r, "x", "y") == SAP_READONLY);
    CHECK(str_del(r, "k") == SAP_READONLY);
    CHECK(check_str(r, "k", "v"));
    txn_abort(r);

    db_close(db);
}

/* ================================================================== */
/* Test: binary keys (non-string data)                                  */
/* ================================================================== */

static void test_binary_keys(void)
{
    SECTION("binary / non-string keys");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    /* Keys with embedded NUL bytes */
    uint8_t k1[] = {0x00, 0x01, 0x02};
    uint8_t k2[] = {0x00, 0x01, 0x03};
    uint8_t v1[] = {0xDE, 0xAD};
    uint8_t v2[] = {0xBE, 0xEF};

    CHECK(txn_put(txn, k1, 3, v1, 2) == SAP_OK);
    CHECK(txn_put(txn, k2, 3, v2, 2) == SAP_OK);

    const void *v;
    uint32_t vl;
    CHECK(txn_get(txn, k1, 3, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, v1, 2) == 0);
    CHECK(txn_get(txn, k2, 3, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, v2, 2) == 0);

    /* Prefix ordering: k1 < k2 */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    const void *k;
    uint32_t kl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 3 && memcmp(k, k1, 3) == 0);
    cursor_close(cur);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: seek past end / before start                                   */
/* ================================================================== */

static void test_seek_boundaries(void)
{
    SECTION("seek boundaries");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    str_put(txn, "b", "B");
    str_put(txn, "d", "D");
    str_put(txn, "f", "F");

    Cursor *cur = cursor_open(txn);

    /* Seek to key before all entries */
    CHECK(cursor_seek(cur, "a", 1) == SAP_OK);
    const void *k, *v;
    uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && *(char *)k == 'b');

    /* Seek to exact key */
    CHECK(cursor_seek(cur, "d", 1) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && *(char *)k == 'd');

    /* Seek past all entries */
    CHECK(cursor_seek(cur, "z", 1) == SAP_NOTFOUND);
    cursor_close(cur);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: delete-then-reinsert same key                                  */
/* ================================================================== */

static void test_delete_reinsert(void)
{
    SECTION("delete and reinsert");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    CHECK(str_put(txn, "k", "v1") == SAP_OK);
    CHECK(str_del(txn, "k") == SAP_OK);
    const void *v;
    uint32_t vl;
    CHECK(str_get(txn, "k", &v, &vl) == SAP_NOTFOUND);
    CHECK(str_put(txn, "k", "v2") == SAP_OK);
    CHECK(check_str(txn, "k", "v2"));

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: multiple commits, growing and shrinking tree                   */
/* ================================================================== */

static void test_multi_commit(void)
{
    SECTION("multiple commits");
    DB *db = new_db();
    char kbuf[32], vbuf[32];
    int N = 500;

    /* Insert in batches, verifying after each commit */
    for (int batch = 0; batch < 5; batch++)
    {
        Txn *txn = txn_begin(db, NULL, 0);
        for (int i = 0; i < N; i++)
        {
            snprintf(kbuf, sizeof(kbuf), "b%d_%04d", batch, i);
            snprintf(vbuf, sizeof(vbuf), "v%d_%04d", batch, i);
            CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf), vbuf, (uint32_t)strlen(vbuf)) ==
                  SAP_OK);
        }
        CHECK(txn_commit(txn) == SAP_OK);

        /* Verify this batch */
        txn = txn_begin(db, NULL, TXN_RDONLY);
        int errs = 0;
        for (int i = 0; i < N; i++)
        {
            snprintf(kbuf, sizeof(kbuf), "b%d_%04d", batch, i);
            snprintf(vbuf, sizeof(vbuf), "v%d_%04d", batch, i);
            if (!check_str(txn, kbuf, vbuf))
                errs++;
        }
        CHECK(errs == 0);
        txn_abort(txn);
    }

    /* Now delete all */
    for (int batch = 0; batch < 5; batch++)
    {
        Txn *txn = txn_begin(db, NULL, 0);
        for (int i = 0; i < N; i++)
        {
            snprintf(kbuf, sizeof(kbuf), "b%d_%04d", batch, i);
            txn_del(txn, kbuf, (uint32_t)strlen(kbuf));
        }
        CHECK(txn_commit(txn) == SAP_OK);
    }

    /* Tree should be empty */
    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    Cursor *cur = cursor_open(r);
    CHECK(cursor_first(cur) == SAP_NOTFOUND);
    cursor_close(cur);
    txn_abort(r);

    db_close(db);
}

/* ================================================================== */
/* Test: input validation for key/value lengths                         */
/* ================================================================== */

static void test_input_validation(void)
{
    SECTION("input validation");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    /* Key too large for uint16_t storage */
    CHECK(txn_put(txn, "k", 1, "v", (uint32_t)70000) == SAP_FULL);
    CHECK(txn_put(txn, "k", (uint32_t)70000, "v", 1) == SAP_FULL);

    /* Get/Del with oversized key_len */
    const void *v;
    uint32_t vl;
    CHECK(txn_get(txn, "k", (uint32_t)70000, &v, &vl) == SAP_NOTFOUND);
    CHECK(txn_del(txn, "k", (uint32_t)70000) == SAP_NOTFOUND);

    /* Normal operations still work */
    CHECK(str_put(txn, "normal", "value") == SAP_OK);
    CHECK(check_str(txn, "normal", "value"));

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: SAP_FULL for oversized entry                                   */
/* ================================================================== */

static void test_sap_full(void)
{
    SECTION("SAP_FULL for oversized entry");
    DB *db = db_open(&g_alloc, 256, NULL, NULL); /* small pages */
    CHECK(db != NULL);
    Txn *txn = txn_begin(db, NULL, 0);

    /* Key too large to fit even with overflow value indirection. */
    char big_key[260];
    memset(big_key, 'A', sizeof(big_key));
    CHECK(txn_put(txn, big_key, 250, "v", 1) == SAP_FULL);

    /* Value length still bounded by uint16_t API encoding. */
    CHECK(txn_put(txn, "k", 1, big_key, (uint32_t)70000) == SAP_FULL);

    /* Smaller entry should still work */
    CHECK(txn_put(txn, "k", 1, "v", 1) == SAP_OK);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: overflow values                                                */
/* ================================================================== */

static void test_overflow_values(void)
{
    SECTION("overflow values");
    DB *db = db_open(&g_alloc, 256, NULL, NULL);
    CHECK(db != NULL);

    uint8_t v1[700];
    uint8_t v2[900];
    fill_pattern(v1, (uint32_t)sizeof(v1), 7);
    fill_pattern(v2, (uint32_t)sizeof(v2), 29);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_put(w, "k1", 2, v1, (uint32_t)sizeof(v1)) == SAP_OK);
    CHECK(txn_put(w, "k2", 2, "x", 1) == SAP_OK);
    CHECK(txn_put_flags(w, "k3", 2, NULL, (uint32_t)sizeof(v1), SAP_RESERVE, NULL) == SAP_ERROR);
    CHECK(txn_commit(w) == SAP_OK);

    {
        const void *v;
        const void *v_again;
        uint32_t vl;
        uint32_t vl_again;
        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get(r, "k1", 2, &v, &vl) == SAP_OK);
        CHECK(vl == (uint32_t)sizeof(v1));
        CHECK(memcmp(v, v1, sizeof(v1)) == 0);
        CHECK(txn_get(r, "k1", 2, &v_again, &vl_again) == SAP_OK);
        CHECK(vl_again == vl);
        CHECK(v_again == v);

        Cursor *cur = cursor_open(r);
        const void *k;
        uint32_t kl;
        CHECK(cur != NULL);
        CHECK(cursor_seek(cur, "k1", 2) == SAP_OK);
        CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
        CHECK(kl == 2 && memcmp(k, "k1", 2) == 0);
        CHECK(vl == (uint32_t)sizeof(v1));
        CHECK(memcmp(v, v1, sizeof(v1)) == 0);
        CHECK(v == v_again);
        cursor_close(cur);
        txn_abort(r);
    }

    w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    {
        Cursor *cur = cursor_open(w);
        CHECK(cur != NULL);
        CHECK(cursor_seek(cur, "k1", 2) == SAP_OK);
        CHECK(cursor_put(cur, v2, (uint32_t)sizeof(v2), 0) == SAP_OK);
        cursor_close(cur);
    }
    CHECK(txn_commit(w) == SAP_OK);

    {
        const void *v;
        uint32_t vl;
        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get(r, "k1", 2, &v, &vl) == SAP_OK);
        CHECK(vl == (uint32_t)sizeof(v2));
        CHECK(memcmp(v, v2, sizeof(v2)) == 0);
        txn_abort(r);
    }

    w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    {
        uint64_t deleted = 0;
        CHECK(txn_del_range(w, 0, "k1", 2, "k3", 2, &deleted) == SAP_OK);
        CHECK(deleted == 2);
    }
    CHECK(txn_commit(w) == SAP_OK);

    {
        const void *v;
        uint32_t vl;
        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get(r, "k1", 2, &v, &vl) == SAP_NOTFOUND);
        CHECK(txn_get(r, "k2", 2, &v, &vl) == SAP_NOTFOUND);
        txn_abort(r);
    }

    {
        struct MemBuf snap = {0};
        const void *v;
        uint32_t vl;

        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_put(w, "kp", 2, v1, (uint32_t)sizeof(v1)) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);
        CHECK(db_checkpoint(db, membuf_write, &snap) == SAP_OK);

        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_put(w, "kp", 2, "short", 5) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);

        snap.pos = 0;
        CHECK(db_restore(db, membuf_read, &snap) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get(r, "kp", 2, &v, &vl) == SAP_OK);
        CHECK(vl == (uint32_t)sizeof(v1));
        CHECK(memcmp(v, v1, sizeof(v1)) == 0);
        txn_abort(r);
        free(snap.data);
    }

    {
        DB *db2 = db_open(&g_alloc, 256, NULL, NULL);
        CHECK(db2 != NULL);
        Txn *t = txn_begin(db2, NULL, 0);
        CHECK(t != NULL);
        const void *keys[] = {"a", "b"};
        const uint32_t key_lens[] = {1, 1};
        const void *vals[] = {v1, "ok"};
        const uint32_t val_lens[] = {(uint32_t)sizeof(v1), 2};
        const void *v;
        uint32_t vl;

        CHECK(txn_load_sorted(t, 0, keys, key_lens, vals, val_lens, 2) == SAP_OK);
        CHECK(txn_commit(t) == SAP_OK);

        Txn *r = txn_begin(db2, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get(r, "a", 1, &v, &vl) == SAP_OK);
        CHECK(vl == (uint32_t)sizeof(v1));
        CHECK(memcmp(v, v1, sizeof(v1)) == 0);
        txn_abort(r);
        db_close(db2);
    }

    {
        DB *db3 = db_open(&g_alloc, 256, NULL, NULL);
        struct MemBuf snap = {0};
        uint32_t page_size = 0;
        uint32_t page_index = 0;
        const void *v;
        uint32_t vl;
        CHECK(db3 != NULL);
        w = txn_begin(db3, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_put(w, "kc", 2, v2, (uint32_t)sizeof(v2)) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);
        CHECK(db_checkpoint(db3, membuf_write, &snap) == SAP_OK);
        CHECK(snapshot_find_first_page_type(&snap, 3u, &page_size, &page_index) == 1);
        if (page_size > 0)
        {
            uint8_t *pg = snap.data + 16u + page_index * page_size;
            /* Truncate chain early while logical length remains large. */
            write_u32_le(pg + 8, 0xFFFFFFFFu);
        }
        snap.pos = 0;
        CHECK(db_restore(db3, membuf_read, &snap) == SAP_OK);
        Txn *r = txn_begin(db3, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get(r, "kc", 2, &v, &vl) == SAP_ERROR);
        txn_abort(r);
        free(snap.data);
        db_close(db3);
    }

    {
        DB *db4 = db_open(&g_alloc, 256, NULL, NULL);
        struct MemBuf snap = {0};
        uint32_t page_size = 0;
        uint32_t page_index = 0;
        const void *v;
        uint32_t vl;
        CHECK(db4 != NULL);
        w = txn_begin(db4, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_put(w, "kd", 2, v2, (uint32_t)sizeof(v2)) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);
        CHECK(db_checkpoint(db4, membuf_write, &snap) == SAP_OK);
        CHECK(snapshot_find_first_page_type(&snap, 3u, &page_size, &page_index) == 1);
        if (page_size > 0)
        {
            uint8_t *pg = snap.data + 16u + page_index * page_size;
            /* Corrupt chunk length to an invalid zero-length segment. */
            write_u16_le(pg + 12, 0);
        }
        snap.pos = 0;
        CHECK(db_restore(db4, membuf_read, &snap) == SAP_OK);
        Txn *r = txn_begin(db4, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get(r, "kd", 2, &v, &vl) == SAP_ERROR);
        txn_abort(r);
        free(snap.data);
        db_close(db4);
    }

    db_close(db);
}

/* ================================================================== */
/* Test: runtime page-size safety invariants                            */
/* ================================================================== */

static void test_runtime_page_size_safety(void)
{
    SECTION("runtime page-size safety");

    /* Offsets are 16-bit; larger pages are invalid. */
    CHECK(db_open(&g_alloc, 65536u, NULL, NULL) == NULL);

    DB *db = db_open(&g_alloc, 16384u, NULL, NULL);
    CHECK(db != NULL);
    Txn *txn = txn_begin(db, NULL, 0);
    CHECK(txn != NULL);

    const uint32_t klen = 5000;
    char *keys[4] = {NULL, NULL, NULL, NULL};
    int alloc_ok = 1;
    for (int i = 0; i < 4; i++)
    {
        keys[i] = (char *)malloc(klen);
        CHECK(keys[i] != NULL);
        if (!keys[i])
            alloc_ok = 0;
        else
            memset(keys[i], 'a' + i, klen);
    }

    /* 4 inserts with 5KB keys force a split and large separator copies. */
    if (alloc_ok)
    {
        for (int i = 0; i < 4; i++)
            CHECK(txn_put(txn, keys[i], klen, "v", 1) == SAP_OK);

        Cursor *cur = cursor_open(txn);
        CHECK(cur != NULL);
        if (cur)
        {
            CHECK(cursor_seek(cur, keys[0], klen) == SAP_OK);
            CHECK(cursor_put(cur, "w", 1, 0) == SAP_OK);
            cursor_close(cur);
        }

        const void *v;
        uint32_t vl;
        CHECK(txn_get(txn, keys[0], klen, &v, &vl) == SAP_OK);
        CHECK(vl == 1 && memcmp(v, "w", 1) == 0);
    }

    txn_abort(txn);
    db_close(db);
    for (int i = 0; i < 4; i++)
        free(keys[i]);
}

/* ================================================================== */
/* Test: write transaction contention                                   */
/* ================================================================== */

static void test_write_contention(void)
{
    SECTION("write transaction contention");
    DB *db = new_db();
    Txn *w1 = txn_begin(db, NULL, 0);
    CHECK(w1 != NULL);

    /* Second write txn should fail */
    Txn *w2 = txn_begin(db, NULL, 0);
    CHECK(w2 == NULL);

    /* Read txn still works alongside writer */
    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r != NULL);
    txn_abort(r);

    /* After aborting first writer, new writer should succeed */
    txn_abort(w1);
    Txn *w3 = txn_begin(db, NULL, 0);
    CHECK(w3 != NULL);
    txn_abort(w3);

    db_close(db);
}

/* ================================================================== */
/* Test: leaf capacity with smaller headers                             */
/* ================================================================== */

static void test_leaf_capacity(void)
{
    SECTION("leaf capacity (small pages)");
    DB *db = db_open(&g_alloc, 256, NULL, NULL); /* small page to make splits visible */
    CHECK(db != NULL);
    Txn *txn = txn_begin(db, NULL, 0);
    char kbuf[8], vbuf[8];
    int i;
    for (i = 0; i < 200; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "k%04d", i);
        snprintf(vbuf, sizeof(vbuf), "v%04d", i);
        CHECK(txn_put(txn, kbuf, 5, vbuf, 5) == SAP_OK);
    }
    /* Verify all entries are retrievable */
    int errors = 0;
    for (int j = 0; j < i; j++)
    {
        snprintf(kbuf, sizeof(kbuf), "k%04d", j);
        snprintf(vbuf, sizeof(vbuf), "v%04d", j);
        if (!check_str(txn, kbuf, vbuf))
            errors++;
    }
    CHECK(errors == 0);

    /* Verify ordered scan */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    int count = 0;
    do
    {
        count++;
    } while (cursor_next(cur) == SAP_OK);
    cursor_close(cur);
    CHECK(count == 200);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: custom key comparator (reverse order)                          */
/* ================================================================== */

static int reverse_cmp(const void *a, uint32_t al, const void *b, uint32_t bl, void *ctx)
{
    (void)ctx;
    uint32_t m = al < bl ? al : bl;
    int c = memcmp(b, a, (size_t)m); /* reversed */
    if (c)
        return c;
    return bl < al ? -1 : bl > al ? 1 : 0;
}

static void test_custom_comparator(void)
{
    SECTION("custom key comparator (reverse)");
    DB *db = db_open(&g_alloc, SAPLING_PAGE_SIZE, reverse_cmp, NULL);
    CHECK(db != NULL);
    Txn *txn = txn_begin(db, NULL, 0);

    CHECK(str_put(txn, "a", "1") == SAP_OK);
    CHECK(str_put(txn, "b", "2") == SAP_OK);
    CHECK(str_put(txn, "c", "3") == SAP_OK);

    /* With reverse comparator, cursor_first should return "c" */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    const void *k, *v;
    uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && *(char *)k == 'c');

    CHECK(cursor_next(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && *(char *)k == 'b');

    CHECK(cursor_next(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && *(char *)k == 'a');

    CHECK(cursor_next(cur) == SAP_NOTFOUND);
    cursor_close(cur);

    /* Get/del still work */
    CHECK(check_str(txn, "b", "2"));
    CHECK(str_del(txn, "b") == SAP_OK);
    const void *v2;
    uint32_t vl2;
    CHECK(str_get(txn, "b", &v2, &vl2) == SAP_NOTFOUND);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: entry count tracking                                           */
/* ================================================================== */

static void test_entry_count(void)
{
    SECTION("entry count tracking");
    DB *db = new_db();
    SapStat stat;

    CHECK(db_stat(db, &stat) == SAP_OK);
    CHECK(stat.num_entries == 0);
    CHECK(stat.tree_depth == 0);

    Txn *txn = txn_begin(db, NULL, 0);
    for (int i = 0; i < 100; i++)
    {
        char kbuf[16];
        snprintf(kbuf, sizeof(kbuf), "k%04d", i);
        CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf), "v", 1) == SAP_OK);
    }
    CHECK(txn_stat(txn, &stat) == SAP_OK);
    CHECK(stat.num_entries == 100);

    /* Update should not change count */
    CHECK(str_put(txn, "k0000", "new") == SAP_OK);
    CHECK(txn_stat(txn, &stat) == SAP_OK);
    CHECK(stat.num_entries == 100);

    /* Delete decrements */
    CHECK(str_del(txn, "k0000") == SAP_OK);
    CHECK(txn_stat(txn, &stat) == SAP_OK);
    CHECK(stat.num_entries == 99);

    CHECK(txn_commit(txn) == SAP_OK);
    CHECK(db_stat(db, &stat) == SAP_OK);
    CHECK(stat.num_entries == 99);
    CHECK(stat.tree_depth > 0);

    db_close(db);
}

/* ================================================================== */
/* Test: entry count with nested transactions                           */
/* ================================================================== */

static void test_entry_count_nested(void)
{
    SECTION("entry count with nested transactions");
    DB *db = new_db();

    Txn *outer = txn_begin(db, NULL, 0);
    CHECK(str_put(outer, "a", "1") == SAP_OK);

    Txn *inner = txn_begin(db, outer, 0);
    CHECK(str_put(inner, "b", "2") == SAP_OK);
    CHECK(str_put(inner, "c", "3") == SAP_OK);

    SapStat stat;
    CHECK(txn_stat(inner, &stat) == SAP_OK);
    CHECK(stat.num_entries == 3);

    txn_abort(inner);

    /* After abort, outer should be back to 1 */
    CHECK(txn_stat(outer, &stat) == SAP_OK);
    CHECK(stat.num_entries == 1);

    CHECK(txn_commit(outer) == SAP_OK);
    CHECK(db_stat(db, &stat) == SAP_OK);
    CHECK(stat.num_entries == 1);

    db_close(db);
}

/* ================================================================== */
/* Test: statistics API                                                 */
/* ================================================================== */

static void test_statistics_api(void)
{
    SECTION("statistics API");
    DB *db = new_db();
    SapStat stat;

    CHECK(db_stat(db, &stat) == SAP_OK);
    CHECK(stat.page_size == SAPLING_PAGE_SIZE);
    CHECK(stat.num_pages >= 2); /* at least 2 meta pages */
    CHECK(stat.has_write_txn == 0);

    Txn *txn = txn_begin(db, NULL, 0);
    CHECK(db_stat(db, &stat) == SAP_OK);
    CHECK(stat.has_write_txn == 1);

    txn_abort(txn);
    CHECK(db_stat(db, &stat) == SAP_OK);
    CHECK(stat.has_write_txn == 0);

    /* NULL checks */
    CHECK(db_stat(NULL, &stat) == SAP_ERROR);
    CHECK(db_stat(db, NULL) == SAP_ERROR);

    db_close(db);
}

/* ================================================================== */
/* Test: integer key comparator                                         */
/* ================================================================== */

static int int_cmp(const void *a, uint32_t al, const void *b, uint32_t bl, void *ctx)
{
    (void)ctx;
    if (al != 4 || bl != 4)
        return 0;
    int32_t ia, ib;
    memcpy(&ia, a, 4);
    memcpy(&ib, b, 4);
    return (ia > ib) - (ia < ib);
}

static void test_integer_key_comparator(void)
{
    SECTION("integer key comparator");
    DB *db = db_open(&g_alloc, SAPLING_PAGE_SIZE, int_cmp, NULL);
    CHECK(db != NULL);
    Txn *txn = txn_begin(db, NULL, 0);

    /* Insert integers in random order */
    int32_t keys[] = {300, 100, 200, 50, 400};
    for (int i = 0; i < 5; i++)
        CHECK(txn_put(txn, &keys[i], 4, "v", 1) == SAP_OK);

    /* Scan should yield sorted integer order: 50, 100, 200, 300, 400 */
    int32_t expected[] = {50, 100, 200, 300, 400};
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    for (int i = 0; i < 5; i++)
    {
        const void *k, *v;
        uint32_t kl, vl;
        CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
        int32_t got;
        memcpy(&got, k, 4);
        CHECK(got == expected[i]);
        if (i < 4)
            CHECK(cursor_next(cur) == SAP_OK);
    }
    CHECK(cursor_next(cur) == SAP_NOTFOUND);
    cursor_close(cur);

    /* Update works with integer keys */
    int32_t key200 = 200;
    CHECK(txn_put(txn, &key200, 4, "updated", 7) == SAP_OK);
    const void *v;
    uint32_t vl;
    CHECK(txn_get(txn, &key200, 4, &v, &vl) == SAP_OK);
    CHECK(vl == 7 && memcmp(v, "updated", 7) == 0);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Thread-safety tests (only when SAPLING_THREADED is defined)          */
/* ================================================================== */

/* ================================================================== */
/* Test: large dataset (100K keys)                                      */
/* ================================================================== */

static void test_large_dataset_100k(void)
{
    SECTION("large dataset (100000 keys)");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);
    int N = 100000;
    char kbuf[16];

    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "%08d", i);
        CHECK(txn_put(txn, kbuf, 8, kbuf, 8) == SAP_OK);
    }

    /* Verify count */
    SapStat stat;
    CHECK(txn_stat(txn, &stat) == SAP_OK);
    CHECK(stat.num_entries == (uint64_t)N);

    /* Spot-check entries */
    for (int i = 0; i < N; i += 10000)
    {
        snprintf(kbuf, sizeof(kbuf), "%08d", i);
        const void *v;
        uint32_t vl;
        CHECK(txn_get(txn, kbuf, 8, &v, &vl) == SAP_OK);
        CHECK(vl == 8 && memcmp(v, kbuf, 8) == 0);
    }

    CHECK(txn_commit(txn) == SAP_OK);
    db_close(db);
}

/* ================================================================== */
/* Test: ascending insertion pattern                                    */
/* ================================================================== */

static void test_ascending_insert(void)
{
    SECTION("ascending insertion pattern");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);
    char kbuf[16];
    int N = 5000;
    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "%06d", i);
        CHECK(txn_put(txn, kbuf, 6, "v", 1) == SAP_OK);
    }
    /* Verify order */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    int count = 0;
    do
    {
        count++;
    } while (cursor_next(cur) == SAP_OK);
    cursor_close(cur);
    CHECK(count == N);
    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: descending insertion pattern                                   */
/* ================================================================== */

static void test_descending_insert(void)
{
    SECTION("descending insertion pattern");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);
    char kbuf[16];
    int N = 5000;
    for (int i = N - 1; i >= 0; i--)
    {
        snprintf(kbuf, sizeof(kbuf), "%06d", i);
        CHECK(txn_put(txn, kbuf, 6, "v", 1) == SAP_OK);
    }
    /* First entry should be "000000" */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    const void *k, *v;
    uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 6 && memcmp(k, "000000", 6) == 0);
    int count = 1;
    while (cursor_next(cur) == SAP_OK)
        count++;
    cursor_close(cur);
    CHECK(count == N);
    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: multiple reader snapshots at different versions                 */
/* ================================================================== */

static void test_multiple_reader_snapshots(void)
{
    SECTION("multiple reader snapshots");
    DB *db = new_db();

    /* Commit v1 */
    Txn *w = txn_begin(db, NULL, 0);
    str_put(w, "key", "v1");
    txn_commit(w);

    /* Reader at v1 */
    Txn *r1 = txn_begin(db, NULL, TXN_RDONLY);

    /* Commit v2 */
    w = txn_begin(db, NULL, 0);
    str_put(w, "key", "v2");
    txn_commit(w);

    /* Reader at v2 */
    Txn *r2 = txn_begin(db, NULL, TXN_RDONLY);

    /* Commit v3 */
    w = txn_begin(db, NULL, 0);
    str_put(w, "key", "v3");
    txn_commit(w);

    /* r1 still sees v1, r2 sees v2 */
    CHECK(check_str(r1, "key", "v1"));
    CHECK(check_str(r2, "key", "v2"));

    /* New reader sees v3 */
    Txn *r3 = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(check_str(r3, "key", "v3"));

    txn_abort(r1);
    txn_abort(r2);
    txn_abort(r3);

    db_close(db);
}

/* ================================================================== */
/* Test: interleaved put/delete stress                                  */
/* ================================================================== */

static void test_interleaved_put_delete(void)
{
    SECTION("interleaved put/delete stress");
    DB *db = new_db();
    char kbuf[16], vbuf[16];
    int N = 3000;

    Txn *txn = txn_begin(db, NULL, 0);
    /* Insert N keys */
    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "s%06d", i);
        snprintf(vbuf, sizeof(vbuf), "v%06d", i);
        CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf), vbuf, (uint32_t)strlen(vbuf)) == SAP_OK);
    }
    /* Delete even keys, update odd keys */
    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "s%06d", i);
        if (i % 2 == 0)
        {
            CHECK(txn_del(txn, kbuf, (uint32_t)strlen(kbuf)) == SAP_OK);
        }
        else
        {
            CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf), "updated", 7) == SAP_OK);
        }
    }

    /* Verify: even keys gone, odd keys updated */
    int errors = 0;
    for (int i = 0; i < N; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "s%06d", i);
        const void *v;
        uint32_t vl;
        int rc = txn_get(txn, kbuf, (uint32_t)strlen(kbuf), &v, &vl);
        if (i % 2 == 0)
        {
            if (rc != SAP_NOTFOUND)
                errors++;
        }
        else
        {
            if (rc != SAP_OK || vl != 7 || memcmp(v, "updated", 7) != 0)
                errors++;
        }
    }
    CHECK(errors == 0);

    SapStat stat;
    CHECK(txn_stat(txn, &stat) == SAP_OK);
    CHECK(stat.num_entries == (uint64_t)(N / 2));

    CHECK(txn_commit(txn) == SAP_OK);
    db_close(db);
}

/* ================================================================== */
/* Test: cursor stability across seek/next/prev                         */
/* ================================================================== */

static void test_cursor_stability(void)
{
    SECTION("cursor stability");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    /* Insert 100 keys */
    char kbuf[16];
    for (int i = 0; i < 100; i++)
    {
        snprintf(kbuf, sizeof(kbuf), "cs%04d", i);
        CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf), "v", 1) == SAP_OK);
    }

    /* Seek to middle, walk forward, then backward */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_seek(cur, "cs0050", 6) == SAP_OK);
    const void *k, *v;
    uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 6 && memcmp(k, "cs0050", 6) == 0);

    /* Walk forward 10 steps */
    for (int i = 0; i < 10; i++)
        CHECK(cursor_next(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 6 && memcmp(k, "cs0060", 6) == 0);

    /* Walk backward 20 steps */
    for (int i = 0; i < 20; i++)
        CHECK(cursor_prev(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 6 && memcmp(k, "cs0040", 6) == 0);

    cursor_close(cur);
    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: cursor_renew                                                   */
/* ================================================================== */

static void test_cursor_renew(void)
{
    SECTION("cursor_renew");
    DB *db = new_db();

    Txn *w1 = txn_begin(db, NULL, 0);
    CHECK(txn_put(w1, "k1", 2, "v1", 2) == SAP_OK);
    CHECK(txn_commit(w1) == SAP_OK);

    Txn *r1 = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r1 != NULL);
    Cursor *cur = cursor_open(r1);
    CHECK(cur != NULL);
    CHECK(cursor_first(cur) == SAP_OK);

    const void *k;
    uint32_t kl;
    CHECK(cursor_get_key(cur, &k, &kl) == SAP_OK);
    CHECK(kl == 2 && memcmp(k, "k1", 2) == 0);

    Txn *w2 = txn_begin(db, NULL, 0);
    CHECK(w2 != NULL);
    CHECK(txn_put(w2, "k2", 2, "v2", 2) == SAP_OK);
    CHECK(txn_commit(w2) == SAP_OK);

    CHECK(cursor_seek(cur, "k2", 2) == SAP_NOTFOUND);

    Txn *r2 = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r2 != NULL);
    CHECK(cursor_renew(cur, r2) == SAP_OK);
    CHECK(cursor_seek(cur, "k2", 2) == SAP_OK);
    CHECK(cursor_get_key(cur, &k, &kl) == SAP_OK);
    CHECK(kl == 2 && memcmp(k, "k2", 2) == 0);

    DB *db2 = new_db();
    Txn *r_other = txn_begin(db2, NULL, TXN_RDONLY);
    CHECK(r_other != NULL);
    CHECK(cursor_renew(cur, r_other) == SAP_ERROR);

    txn_abort(r_other);
    db_close(db2);
    cursor_close(cur);
    txn_abort(r1);
    txn_abort(r2);
    db_close(db);
}

/* ================================================================== */
/* Test: cursor_get_key                                                 */
/* ================================================================== */

static void test_cursor_get_key(void)
{
    SECTION("cursor_get_key");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, DBI_DUPSORT) == SAP_OK);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_put(w, "a", 1, "va", 2) == SAP_OK);
    CHECK(txn_put(w, "b", 1, "vb", 2) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "dup", 3, "a", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "dup", 3, "b", 1) == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r != NULL);

    Cursor *cur = cursor_open(r);
    CHECK(cur != NULL);
    CHECK(cursor_seek(cur, "b", 1) == SAP_OK);
    const void *k;
    uint32_t kl;
    CHECK(cursor_get_key(cur, &k, &kl) == SAP_OK);
    CHECK(kl == 1 && memcmp(k, "b", 1) == 0);
    CHECK(cursor_get_key(cur, NULL, &kl) == SAP_ERROR);
    CHECK(cursor_get_key(cur, &k, NULL) == SAP_ERROR);
    CHECK(cursor_next(cur) == SAP_NOTFOUND);
    CHECK(cursor_get_key(cur, &k, &kl) == SAP_NOTFOUND);
    cursor_close(cur);

    Cursor *dcur = cursor_open_dbi(r, 1);
    CHECK(dcur != NULL);
    CHECK(cursor_seek_prefix(dcur, "dup", 3) == SAP_OK);
    CHECK(cursor_get_key(dcur, &k, &kl) == SAP_OK);
    CHECK(kl == 3 && memcmp(k, "dup", 3) == 0);
    CHECK(cursor_next_dup(dcur) == SAP_OK);
    CHECK(cursor_get_key(dcur, &k, &kl) == SAP_OK);
    CHECK(kl == 3 && memcmp(k, "dup", 3) == 0);
    cursor_close(dcur);

    txn_abort(r);
    db_close(db);
}

/* ================================================================== */
/* Test: NOOVERWRITE flag                                               */
/* ================================================================== */

static void test_nooverwrite(void)
{
    SECTION("NOOVERWRITE flag");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    /* First insert succeeds */
    CHECK(txn_put_flags(txn, "key", 3, "val1", 4, SAP_NOOVERWRITE, NULL) == SAP_OK);

    /* Duplicate insert fails with SAP_EXISTS */
    CHECK(txn_put_flags(txn, "key", 3, "val2", 4, SAP_NOOVERWRITE, NULL) == SAP_EXISTS);

    /* Original value unchanged */
    CHECK(check_str(txn, "key", "val1"));

    /* Without flag, upsert still works */
    CHECK(txn_put_flags(txn, "key", 3, "val2", 4, 0, NULL) == SAP_OK);
    CHECK(check_str(txn, "key", "val2"));

    /* NOOVERWRITE on a new key works */
    CHECK(txn_put_flags(txn, "new", 3, "yes", 3, SAP_NOOVERWRITE, NULL) == SAP_OK);
    CHECK(check_str(txn, "new", "yes"));

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: RESERVE flag                                                   */
/* ================================================================== */

static void test_reserve(void)
{
    SECTION("RESERVE flag");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    void *reserved = NULL;
    CHECK(txn_put_flags(txn, "rkey", 4, NULL, 8, SAP_RESERVE, &reserved) == SAP_OK);
    CHECK(reserved != NULL);

    /* Write directly into reserved space */
    memcpy(reserved, "reserved", 8);

    /* Read it back */
    const void *v;
    uint32_t vl;
    CHECK(txn_get(txn, "rkey", 4, &v, &vl) == SAP_OK);
    CHECK(vl == 8 && memcmp(v, "reserved", 8) == 0);

    /* Reserve + NOOVERWRITE combo: should fail on existing key */
    void *r2 = NULL;
    CHECK(txn_put_flags(txn, "rkey", 4, NULL, 8, SAP_RESERVE | SAP_NOOVERWRITE, &r2) == SAP_EXISTS);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: compare-and-swap put                                           */
/* ================================================================== */

static void test_put_if(void)
{
    SECTION("txn_put_if");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, 0) == SAP_OK);
    CHECK(dbi_open(db, 2, NULL, NULL, DBI_DUPSORT) == SAP_OK);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_put_dbi(w, 0, "k", 1, "v1", 2) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "db1", 3) == SAP_OK);
    CHECK(txn_put_dbi(w, 2, "dup", 3, "a", 1) == SAP_OK);

    CHECK(txn_put_if(w, 0, "k", 1, "v2", 2, "v1", 2) == SAP_OK);
    CHECK(check_str(w, "k", "v2"));

    CHECK(txn_put_if(w, 0, "k", 1, "v3", 2, "v1", 2) == SAP_CONFLICT);
    CHECK(check_str(w, "k", "v2"));

    CHECK(txn_put_if(w, 0, "missing", 7, "x", 1, "", 0) == SAP_NOTFOUND);
    CHECK(txn_put_if(w, 1, "k", 1, "db1x", 4, "db1", 3) == SAP_OK);
    CHECK(txn_put_if(w, 99, "k", 1, "x", 1, "v2", 2) == SAP_ERROR);
    CHECK(txn_put_if(w, 0, "k", 1, "x", 1, NULL, 1) == SAP_ERROR);
    CHECK(txn_put_if(w, 2, "dup", 3, "b", 1, "a", 1) == SAP_ERROR);
    CHECK(txn_commit(w) == SAP_OK);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r != NULL);
    CHECK(txn_put_if(r, 0, "k", 1, "v4", 2, "v2", 2) == SAP_READONLY);
    txn_abort(r);

    db_close(db);

    {
        uint8_t v1[700];
        uint8_t v2[900];
        const void *v;
        uint32_t vl;
        DB *odb = db_open(&g_alloc, 256, NULL, NULL);
        CHECK(odb != NULL);
        fill_pattern(v1, (uint32_t)sizeof(v1), 5);
        fill_pattern(v2, (uint32_t)sizeof(v2), 11);

        w = txn_begin(odb, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_put_if(w, 0, "ov", 2, v2, (uint32_t)sizeof(v2), v1, (uint32_t)sizeof(v1)) ==
              SAP_NOTFOUND);
        CHECK(txn_put(w, "ov", 2, v1, (uint32_t)sizeof(v1)) == SAP_OK);
        CHECK(txn_put_if(w, 0, "ov", 2, v2, (uint32_t)sizeof(v2), v1, (uint32_t)sizeof(v1)) ==
              SAP_OK);
        CHECK(txn_put_if(w, 0, "ov", 2, v1, (uint32_t)sizeof(v1), v1, (uint32_t)sizeof(v1)) ==
              SAP_CONFLICT);
        CHECK(txn_commit(w) == SAP_OK);

        r = txn_begin(odb, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get(r, "ov", 2, &v, &vl) == SAP_OK);
        CHECK(vl == (uint32_t)sizeof(v2));
        CHECK(memcmp(v, v2, sizeof(v2)) == 0);
        txn_abort(r);
        db_close(odb);
    }
}

/* ================================================================== */
/* Test: multiple named databases                                       */
/* ================================================================== */

static void test_multi_dbi(void)
{
    SECTION("multiple named databases");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, 0) == SAP_OK);
    CHECK(dbi_open(db, 2, NULL, NULL, 0) == SAP_OK);

    Txn *txn = txn_begin(db, NULL, 0);

    /* Same key in different DBIs — isolation */
    CHECK(txn_put_dbi(txn, 0, "shared", 6, "db0", 3) == SAP_OK);
    CHECK(txn_put_dbi(txn, 1, "shared", 6, "db1", 3) == SAP_OK);
    CHECK(txn_put_dbi(txn, 2, "shared", 6, "db2", 3) == SAP_OK);

    const void *v;
    uint32_t vl;
    CHECK(txn_get_dbi(txn, 0, "shared", 6, &v, &vl) == SAP_OK);
    CHECK(vl == 3 && memcmp(v, "db0", 3) == 0);
    CHECK(txn_get_dbi(txn, 1, "shared", 6, &v, &vl) == SAP_OK);
    CHECK(vl == 3 && memcmp(v, "db1", 3) == 0);
    CHECK(txn_get_dbi(txn, 2, "shared", 6, &v, &vl) == SAP_OK);
    CHECK(vl == 3 && memcmp(v, "db2", 3) == 0);

    /* Default API (DBI 0) still works */
    CHECK(txn_get(txn, "shared", 6, &v, &vl) == SAP_OK);
    CHECK(vl == 3 && memcmp(v, "db0", 3) == 0);

    /* Per-DBI entry count */
    SapStat stat;
    CHECK(dbi_stat(txn, 0, &stat) == SAP_OK);
    CHECK(stat.num_entries == 1);
    CHECK(dbi_stat(txn, 1, &stat) == SAP_OK);
    CHECK(stat.num_entries == 1);

    CHECK(txn_commit(txn) == SAP_OK);

    /* Verify after commit */
    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(txn_get_dbi(r, 0, "shared", 6, &v, &vl) == SAP_OK);
    CHECK(memcmp(v, "db0", 3) == 0);
    CHECK(txn_get_dbi(r, 1, "shared", 6, &v, &vl) == SAP_OK);
    CHECK(memcmp(v, "db1", 3) == 0);
    txn_abort(r);

    db_close(db);
}

/* ================================================================== */
/* Test: multi-DBI atomic commit / abort                                */
/* ================================================================== */

static void test_multi_dbi_txn(void)
{
    SECTION("multi-DBI atomic commit/abort");
    DB *db = new_db();
    dbi_open(db, 1, NULL, NULL, 0);

    /* Abort: neither DBI affected */
    Txn *txn = txn_begin(db, NULL, 0);
    txn_put_dbi(txn, 0, "a", 1, "1", 1);
    txn_put_dbi(txn, 1, "b", 1, "2", 1);
    txn_abort(txn);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    const void *v;
    uint32_t vl;
    CHECK(txn_get_dbi(r, 0, "a", 1, &v, &vl) == SAP_NOTFOUND);
    CHECK(txn_get_dbi(r, 1, "b", 1, &v, &vl) == SAP_NOTFOUND);
    txn_abort(r);

    /* Commit: both DBIs updated atomically */
    txn = txn_begin(db, NULL, 0);
    txn_put_dbi(txn, 0, "a", 1, "1", 1);
    txn_put_dbi(txn, 1, "b", 1, "2", 1);
    CHECK(txn_commit(txn) == SAP_OK);

    r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(txn_get_dbi(r, 0, "a", 1, &v, &vl) == SAP_OK);
    CHECK(txn_get_dbi(r, 1, "b", 1, &v, &vl) == SAP_OK);
    txn_abort(r);

    db_close(db);
}

/* ================================================================== */
/* Test: checkpoint / restore                                           */
/* ================================================================== */

static void test_checkpoint_restore(void)
{
    SECTION("checkpoint/restore");
    struct MemBuf snap = {0};
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, 0) == SAP_OK);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_put_dbi(w, 0, "a", 1, "one", 3) == SAP_OK);
    CHECK(txn_put_dbi(w, 0, "b", 1, "two", 3) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "x", 1, "db1v", 4) == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    CHECK(db_checkpoint(db, membuf_write, &snap) == SAP_OK);
    CHECK(snap.len > 0);

    Txn *wb = txn_begin(db, NULL, 0);
    CHECK(wb != NULL);
    CHECK(db_checkpoint(db, membuf_write, &snap) == SAP_BUSY);
    CHECK(db_restore(db, membuf_read, &snap) == SAP_BUSY);
    txn_abort(wb);

    Txn *rb = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(rb != NULL);
    CHECK(db_checkpoint(db, membuf_write, &snap) == SAP_BUSY);
    CHECK(db_restore(db, membuf_read, &snap) == SAP_BUSY);
    txn_abort(rb);

    w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_put_dbi(w, 0, "a", 1, "ONE!", 4) == SAP_OK);
    CHECK(txn_del_dbi(w, 1, "x", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "y", 1, "later", 5) == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    snap.pos = 0;
    CHECK(db_restore(db, membuf_read, &snap) == SAP_OK);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r != NULL);
    const void *v;
    uint32_t vl;
    CHECK(txn_get_dbi(r, 0, "a", 1, &v, &vl) == SAP_OK);
    CHECK(vl == 3 && memcmp(v, "one", 3) == 0);
    CHECK(txn_get_dbi(r, 0, "b", 1, &v, &vl) == SAP_OK);
    CHECK(vl == 3 && memcmp(v, "two", 3) == 0);
    CHECK(txn_get_dbi(r, 1, "x", 1, &v, &vl) == SAP_OK);
    CHECK(vl == 4 && memcmp(v, "db1v", 4) == 0);
    CHECK(txn_get_dbi(r, 1, "y", 1, &v, &vl) == SAP_NOTFOUND);
    txn_abort(r);

    {
        struct MemBuf bad = {0};
        bad.data = (uint8_t *)"bad";
        bad.len = 3;
        bad.cap = 3;
        bad.pos = 0;
        CHECK(db_restore(db, membuf_read, &bad) == SAP_ERROR);
    }

    free(snap.data);
    db_close(db);
}

/* ================================================================== */
/* Test: txn_count_range                                                */
/* ================================================================== */

static void test_count_range(void)
{
    SECTION("txn_count_range");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, DBI_DUPSORT) == SAP_OK);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    for (int i = 0; i < 10; i++)
    {
        char k[3], v[3];
        snprintf(k, sizeof(k), "k%d", i);
        snprintf(v, sizeof(v), "v%d", i);
        CHECK(txn_put_dbi(w, 0, k, 2, v, 2) == SAP_OK);
    }
    CHECK(txn_put_dbi(w, 1, "k", 1, "a", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "b", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "c", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "m", 1, "z", 1) == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r != NULL);
    uint64_t count = 999;

    CHECK(txn_count_range(r, 0, NULL, 0, NULL, 0, &count) == SAP_OK);
    CHECK(count == 10);

    CHECK(txn_count_range(r, 0, "k3", 2, "k7", 2, &count) == SAP_OK);
    CHECK(count == 4);

    CHECK(txn_count_range(r, 0, "k7", 2, "k3", 2, &count) == SAP_OK);
    CHECK(count == 0);

    CHECK(txn_count_range(r, 0, "zz", 2, NULL, 0, &count) == SAP_OK);
    CHECK(count == 0);

    CHECK(txn_count_range(r, 1, "k", 1, "m", 1, &count) == SAP_OK);
    CHECK(count == 3);

    CHECK(txn_count_range(r, 1, "k", 1, "n", 1, &count) == SAP_OK);
    CHECK(count == 4);

    CHECK(txn_count_range(r, 0, NULL, 1, NULL, 0, &count) == SAP_ERROR);
    CHECK(txn_count_range(r, 0, NULL, 0, NULL, 1, &count) == SAP_ERROR);
    CHECK(txn_count_range(r, 99, NULL, 0, NULL, 0, &count) == SAP_ERROR);
    CHECK(txn_count_range(r, 0, NULL, 0, NULL, 0, NULL) == SAP_ERROR);

    txn_abort(r);
    db_close(db);
}

/* ================================================================== */
/* Test: txn_del_range                                                  */
/* ================================================================== */

static void test_del_range(void)
{
    SECTION("txn_del_range");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, DBI_DUPSORT) == SAP_OK);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    for (int i = 0; i < 10; i++)
    {
        char k[3], v[3];
        snprintf(k, sizeof(k), "k%d", i);
        snprintf(v, sizeof(v), "v%d", i);
        CHECK(txn_put_dbi(w, 0, k, 2, v, 2) == SAP_OK);
    }
    CHECK(txn_put_dbi(w, 1, "k", 1, "a", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "b", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "c", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "m", 1, "z", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "n", 1, "y", 1) == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    {
        uint64_t deleted = 999;
        uint64_t count = 999;
        const void *v;
        uint32_t vl;

        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_del_range(w, 0, "k3", 2, "k7", 2, &deleted) == SAP_OK);
        CHECK(deleted == 4);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_count_range(r, 0, NULL, 0, NULL, 0, &count) == SAP_OK);
        CHECK(count == 6);
        CHECK(txn_get_dbi(r, 0, "k2", 2, &v, &vl) == SAP_OK);
        CHECK(txn_get_dbi(r, 0, "k3", 2, &v, &vl) == SAP_NOTFOUND);
        CHECK(txn_get_dbi(r, 0, "k6", 2, &v, &vl) == SAP_NOTFOUND);
        CHECK(txn_get_dbi(r, 0, "k7", 2, &v, &vl) == SAP_OK);
        txn_abort(r);
    }

    {
        uint64_t deleted = 999;
        uint64_t count = 999;

        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_del_range(w, 0, NULL, 0, "k2", 2, &deleted) == SAP_OK);
        CHECK(deleted == 2);
        CHECK(txn_del_range(w, 0, "k8", 2, NULL, 0, &deleted) == SAP_OK);
        CHECK(deleted == 2);
        CHECK(txn_del_range(w, 0, "k7", 2, "k7", 2, &deleted) == SAP_OK);
        CHECK(deleted == 0);
        CHECK(txn_del_range(w, 0, "k9", 2, "k8", 2, &deleted) == SAP_OK);
        CHECK(deleted == 0);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_count_range(r, 0, NULL, 0, NULL, 0, &count) == SAP_OK);
        CHECK(count == 2);
        txn_abort(r);
    }

    {
        uint64_t deleted = 999;
        uint64_t count = 999;

        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_del_range(w, 1, "k", 1, "m", 1, &deleted) == SAP_OK);
        CHECK(deleted == 3);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_count_range(r, 1, NULL, 0, NULL, 0, &count) == SAP_OK);
        CHECK(count == 2);
        txn_abort(r);
    }

    {
        uint64_t deleted = 999;
        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_del_range(r, 0, NULL, 0, NULL, 0, &deleted) == SAP_READONLY);
        txn_abort(r);
    }

    {
        uint64_t deleted = 999;
        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_del_range(w, 99, NULL, 0, NULL, 0, &deleted) == SAP_ERROR);
        CHECK(txn_del_range(w, 0, NULL, 1, NULL, 0, &deleted) == SAP_ERROR);
        CHECK(txn_del_range(w, 0, NULL, 0, NULL, 1, &deleted) == SAP_ERROR);
        CHECK(txn_del_range(w, 0, NULL, 0, NULL, 0, NULL) == SAP_ERROR);
        txn_abort(w);
    }

    db_close(db);
}

/* ================================================================== */
/* Test: txn_merge                                                      */
/* ================================================================== */

static void test_merge(void)
{
    SECTION("txn_merge");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, DBI_DUPSORT) == SAP_OK);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_put_dbi(w, 0, "k", 1, "a", 1) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "dup", 3) == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    {
        const void *v;
        uint32_t vl;
        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_merge(w, 0, "k", 1, "b", 1, merge_concat, NULL) == SAP_OK);
        CHECK(txn_merge(w, 0, "new", 3, "xy", 2, merge_concat, NULL) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get_dbi(r, 0, "k", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 2 && memcmp(v, "ab", 2) == 0);
        CHECK(txn_get_dbi(r, 0, "new", 3, &v, &vl) == SAP_OK);
        CHECK(vl == 2 && memcmp(v, "xy", 2) == 0);
        txn_abort(r);
    }

    {
        const void *v;
        uint32_t vl;
        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_merge(w, 0, "k", 1, NULL, 0, merge_clear, NULL) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get_dbi(r, 0, "k", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 0);
        txn_abort(r);
    }

    {
        uint8_t big[5000];
        const void *v;
        uint32_t vl;
        fill_pattern(big, (uint32_t)sizeof(big), 23);

        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_merge(w, 0, "blob", 4, big, (uint32_t)sizeof(big), merge_concat, NULL) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_get_dbi(r, 0, "blob", 4, &v, &vl) == SAP_OK);
        CHECK(vl == (uint32_t)sizeof(big));
        CHECK(memcmp(v, big, sizeof(big)) == 0);
        txn_abort(r);
    }

    {
        const void *v;
        uint32_t vl;
        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_merge(w, 0, "new", 3, "!", 1, merge_overflow, NULL) == SAP_FULL);
        CHECK(txn_get_dbi(w, 0, "new", 3, &v, &vl) == SAP_OK);
        CHECK(vl == 2 && memcmp(v, "xy", 2) == 0);
        txn_abort(w);
    }

    {
        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_merge(r, 0, "k", 1, "x", 1, merge_concat, NULL) == SAP_READONLY);
        txn_abort(r);
    }

    {
        char z = 'z';
        w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_merge(w, 99, "k", 1, "x", 1, merge_concat, NULL) == SAP_ERROR);
        CHECK(txn_merge(w, 0, NULL, 1, "x", 1, merge_concat, NULL) == SAP_ERROR);
        CHECK(txn_merge(w, 0, "k", 1, NULL, 1, merge_concat, NULL) == SAP_ERROR);
        CHECK(txn_merge(w, 0, "k", 1, "x", 1, NULL, NULL) == SAP_ERROR);
        CHECK(txn_merge(w, 1, "k", 1, "x", 1, merge_concat, NULL) == SAP_ERROR);
        CHECK(txn_merge(w, 0, "k", 1, "x", 1, merge_too_large, NULL) == SAP_FULL);
        CHECK(txn_merge(w, 0, &z, (uint32_t)UINT16_MAX + 1u, "x", 1, merge_concat, NULL) ==
              SAP_FULL);
        txn_abort(w);
    }

    db_close(db);
}

/* ================================================================== */
/* Test: txn_load_sorted                                                */
/* ================================================================== */

static void test_load_sorted(void)
{
    SECTION("txn_load_sorted");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, DBI_DUPSORT) == SAP_OK);

    {
        const void *keys[] = {"a", "b", "c"};
        const uint32_t key_lens[] = {1, 1, 1};
        const void *vals[] = {"1", "2", "3"};
        const uint32_t val_lens[] = {1, 1, 1};

        Txn *w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_load_sorted(w, 0, keys, key_lens, vals, val_lens, 3) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        const void *v;
        uint32_t vl;
        CHECK(txn_get_dbi(r, 0, "a", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 1 && memcmp(v, "1", 1) == 0);
        CHECK(txn_get_dbi(r, 0, "b", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 1 && memcmp(v, "2", 1) == 0);
        CHECK(txn_get_dbi(r, 0, "c", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 1 && memcmp(v, "3", 1) == 0);
        txn_abort(r);
    }

    {
        const void *keys[] = {"b", "d"};
        const uint32_t key_lens[] = {1, 1};
        const void *vals[] = {"22", "4"};
        const uint32_t val_lens[] = {2, 1};

        Txn *w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_load_sorted(w, 0, keys, key_lens, vals, val_lens, 2) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        const void *v;
        uint32_t vl;
        CHECK(txn_get_dbi(r, 0, "b", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 2 && memcmp(v, "22", 2) == 0);
        CHECK(txn_get_dbi(r, 0, "d", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 1 && memcmp(v, "4", 1) == 0);
        txn_abort(r);
    }

    {
        const void *keys[] = {"e", "f"};
        const uint32_t key_lens[] = {1, 1};
        const void *vals[] = {"5", "6"};
        const uint32_t val_lens[] = {1, 1};

        Txn *w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_put_dbi(w, 0, "z", 1, "99", 2) == SAP_OK); /* force existing txn deltas */
        CHECK(txn_load_sorted(w, 0, keys, key_lens, vals, val_lens, 2) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        const void *v;
        uint32_t vl;
        CHECK(txn_get_dbi(r, 0, "z", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 2 && memcmp(v, "99", 2) == 0);
        CHECK(txn_get_dbi(r, 0, "e", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 1 && memcmp(v, "5", 1) == 0);
        CHECK(txn_get_dbi(r, 0, "f", 1, &v, &vl) == SAP_OK);
        CHECK(vl == 1 && memcmp(v, "6", 1) == 0);
        txn_abort(r);
    }

    {
        const void *keys[] = {"b", "a"};
        const uint32_t key_lens[] = {1, 1};
        const void *vals[] = {"1", "2"};
        const uint32_t val_lens[] = {1, 1};
        Txn *w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_load_sorted(w, 0, keys, key_lens, vals, val_lens, 2) == SAP_ERROR);
        txn_abort(w);
    }

    {
        const void *keys[] = {"d", "d"};
        const uint32_t key_lens[] = {1, 1};
        const void *vals[] = {"1", "2"};
        const uint32_t val_lens[] = {1, 1};
        Txn *w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_load_sorted(w, 0, keys, key_lens, vals, val_lens, 2) == SAP_EXISTS);
        txn_abort(w);
    }

    {
        const void *keys[] = {"x", "x", "x", "y"};
        const uint32_t key_lens[] = {1, 1, 1, 1};
        const void *vals[] = {"a", "b", "c", "z"};
        const uint32_t val_lens[] = {1, 1, 1, 1};
        Txn *w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_load_sorted(w, 1, keys, key_lens, vals, val_lens, 4) == SAP_OK);
        CHECK(txn_commit(w) == SAP_OK);

        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        Cursor *cur = cursor_open_dbi(r, 1);
        CHECK(cur != NULL);
        CHECK(cursor_seek_prefix(cur, "x", 1) == SAP_OK);
        uint64_t dup_count = 0;
        CHECK(cursor_count_dup(cur, &dup_count) == SAP_OK);
        CHECK(dup_count == 3);
        cursor_close(cur);
        txn_abort(r);
    }

    {
        const void *keys[] = {"x", "x"};
        const uint32_t key_lens[] = {1, 1};
        const void *vals[] = {"b", "a"};
        const uint32_t val_lens[] = {1, 1};
        Txn *w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_load_sorted(w, 1, keys, key_lens, vals, val_lens, 2) == SAP_ERROR);
        txn_abort(w);
    }

    {
        Txn *w = txn_begin(db, NULL, 0);
        CHECK(w != NULL);
        CHECK(txn_load_sorted(w, 0, NULL, NULL, NULL, NULL, 0) == SAP_OK);
        CHECK(txn_load_sorted(w, 0, NULL, NULL, NULL, NULL, 1) == SAP_ERROR);
        txn_abort(w);
    }

    {
        const void *keys[] = {"q"};
        const uint32_t key_lens[] = {1};
        const void *vals[] = {"v"};
        const uint32_t val_lens[] = {1};
        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        CHECK(r != NULL);
        CHECK(txn_load_sorted(r, 0, keys, key_lens, vals, val_lens, 1) == SAP_READONLY);
        txn_abort(r);
    }

    db_close(db);
}

/* ================================================================== */
/* Test: prefix helper APIs                                             */
/* ================================================================== */

static void test_prefix_helpers(void)
{
    SECTION("prefix helpers");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    CHECK(txn_put(txn, "ab0", 3, "v", 1) == SAP_OK);
    CHECK(txn_put(txn, "ab1", 3, "v", 1) == SAP_OK);
    CHECK(txn_put(txn, "ab2", 3, "v", 1) == SAP_OK);
    CHECK(txn_put(txn, "ac0", 3, "v", 1) == SAP_OK);
    CHECK(txn_put(txn, "b00", 3, "v", 1) == SAP_OK);

    Cursor *cur = cursor_open(txn);
    CHECK(cur != NULL);
    CHECK(cursor_seek_prefix(cur, "ab", 2) == SAP_OK);

    const void *k, *v;
    uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 3 && memcmp(k, "ab0", 3) == 0);
    CHECK(cursor_in_prefix(cur, "ab", 2) == 1);

    uint32_t count = 0;
    do
    {
        CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
        CHECK(kl == 3 && memcmp(k, "ab", 2) == 0);
        count++;
    } while (cursor_next(cur) == SAP_OK && cursor_in_prefix(cur, "ab", 2));
    CHECK(count == 3);

    CHECK(cursor_in_prefix(cur, "ab", 2) == 0);
    CHECK(cursor_seek_prefix(cur, "zz", 2) == SAP_NOTFOUND);

    cursor_close(cur);
    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: DUPSORT APIs and duplicate navigation                          */
/* ================================================================== */

static void test_dupsort_apis(void)
{
    SECTION("DUPSORT APIs");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, DBI_DUPSORT) == SAP_OK);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);

    void *reserved = NULL;
    CHECK(txn_put_flags_dbi(w, 1, "k", 1, NULL, 4, SAP_RESERVE, &reserved) == SAP_ERROR);

    CHECK(txn_put_dbi(w, 1, "k", 1, "v2", 2) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "v1", 2) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "v3", 2) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "v2", 2) == SAP_OK); /* exact duplicate is a no-op */
    CHECK(txn_put_dbi(w, 1, "m", 1, "x1", 2) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "m", 1, "x0", 2) == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r != NULL);
    const void *k;
    const void *v;
    uint32_t kl;
    uint32_t vl;
    CHECK(txn_get_dbi(r, 1, "k", 1, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v1", 2) == 0);

    Cursor *cur = cursor_open_dbi(r, 1);
    CHECK(cur != NULL);
    CHECK(cursor_seek_prefix(cur, "k", 1) == SAP_OK);

    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && memcmp(k, "k", 1) == 0);
    CHECK(vl == 2 && memcmp(v, "v1", 2) == 0);
    CHECK(cursor_in_prefix(cur, "k", 1) == 1);

    uint64_t dup_count = 0;
    CHECK(cursor_count_dup(cur, &dup_count) == SAP_OK);
    CHECK(dup_count == 3);

    CHECK(cursor_next_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v2", 2) == 0);

    CHECK(cursor_next_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v3", 2) == 0);

    CHECK(cursor_next_dup(cur) == SAP_NOTFOUND);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v3", 2) == 0);

    CHECK(cursor_prev_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v2", 2) == 0);

    CHECK(cursor_first_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v1", 2) == 0);

    CHECK(cursor_last_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v3", 2) == 0);

    CHECK(cursor_seek_prefix(cur, "m", 1) == SAP_OK);
    CHECK(cursor_count_dup(cur, &dup_count) == SAP_OK);
    CHECK(dup_count == 2);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "x0", 2) == 0);
    CHECK(cursor_next_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "x1", 2) == 0);
    CHECK(cursor_next_dup(cur) == SAP_NOTFOUND);

    CHECK(cursor_seek_prefix(cur, "z", 1) == SAP_NOTFOUND);
    cursor_close(cur);
    txn_abort(r);

    Txn *w2 = txn_begin(db, NULL, 0);
    CHECK(w2 != NULL);
    CHECK(txn_del_dup_dbi(w2, 1, "k", 1, "v2", 2) == SAP_OK);
    CHECK(txn_del_dup_dbi(w2, 1, "k", 1, "qq", 2) == SAP_NOTFOUND);
    CHECK(txn_del_dup_dbi(w2, 0, "k", 1, "v1", 2) == SAP_ERROR);
    CHECK(txn_commit(w2) == SAP_OK);

    Txn *r2 = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r2 != NULL);
    Cursor *cur2 = cursor_open_dbi(r2, 1);
    CHECK(cur2 != NULL);
    CHECK(cursor_seek_prefix(cur2, "k", 1) == SAP_OK);
    CHECK(cursor_count_dup(cur2, &dup_count) == SAP_OK);
    CHECK(dup_count == 2);
    CHECK(cursor_get(cur2, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v1", 2) == 0);
    CHECK(cursor_next_dup(cur2) == SAP_OK);
    CHECK(cursor_get(cur2, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v3", 2) == 0);
    cursor_close(cur2);
    txn_abort(r2);

    Txn *r3 = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r3 != NULL);
    Cursor *nondup = cursor_open(r3);
    CHECK(nondup != NULL);
    CHECK(cursor_next_dup(nondup) == SAP_ERROR);
    CHECK(cursor_prev_dup(nondup) == SAP_ERROR);
    CHECK(cursor_first_dup(nondup) == SAP_ERROR);
    CHECK(cursor_last_dup(nondup) == SAP_ERROR);
    CHECK(cursor_count_dup(nondup, &dup_count) == SAP_ERROR);
    cursor_close(nondup);
    txn_abort(r3);

    db_close(db);
}

/* ================================================================== */
/* Test: DUPSORT value comparator                                      */
/* ================================================================== */

static void test_dupsort_value_comparator(void)
{
    SECTION("DUPSORT value comparator");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, DBI_DUPSORT) == SAP_OK);
    CHECK(dbi_set_dupsort(db, 1, reverse_cmp, NULL) == SAP_OK);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_put_dbi(w, 1, "k", 1, "v1", 2) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "v3", 2) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "k", 1, "v2", 2) == SAP_OK);

    {
        const void *keys[] = {"q", "q", "q"};
        const uint32_t key_lens[] = {1, 1, 1};
        const void *vals_desc[] = {"c", "b", "a"};
        const void *vals_asc[] = {"a", "b", "c"};
        const uint32_t val_lens[] = {1, 1, 1};

        CHECK(txn_load_sorted(w, 1, keys, key_lens, vals_desc, val_lens, 3) == SAP_OK);
        CHECK(txn_load_sorted(w, 1, keys, key_lens, vals_asc, val_lens, 3) == SAP_ERROR);
    }
    CHECK(txn_commit(w) == SAP_OK);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r != NULL);
    const void *k;
    const void *v;
    uint32_t kl;
    uint32_t vl;

    CHECK(txn_get_dbi(r, 1, "k", 1, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, "v3", 2) == 0);

    Cursor *cur = cursor_open_dbi(r, 1);
    CHECK(cur != NULL);
    CHECK(cursor_seek_prefix(cur, "k", 1) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && memcmp(k, "k", 1) == 0);
    CHECK(vl == 2 && memcmp(v, "v3", 2) == 0);
    CHECK(cursor_next_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && memcmp(k, "k", 1) == 0);
    CHECK(vl == 2 && memcmp(v, "v2", 2) == 0);
    CHECK(cursor_next_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && memcmp(k, "k", 1) == 0);
    CHECK(vl == 2 && memcmp(v, "v1", 2) == 0);

    CHECK(cursor_seek_prefix(cur, "q", 1) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && memcmp(k, "q", 1) == 0);
    CHECK(vl == 1 && memcmp(v, "c", 1) == 0);
    CHECK(cursor_next_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && memcmp(k, "q", 1) == 0);
    CHECK(vl == 1 && memcmp(v, "b", 1) == 0);
    CHECK(cursor_next_dup(cur) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && memcmp(k, "q", 1) == 0);
    CHECK(vl == 1 && memcmp(v, "a", 1) == 0);

    uint64_t count = 0;
    CHECK(txn_count_range(r, 1, "k", 1, "l", 1, &count) == SAP_OK);
    CHECK(count == 3);
    CHECK(txn_count_range(r, 1, "k", 1, "r", 1, &count) == SAP_OK);
    CHECK(count == 6);

    cursor_close(cur);
    txn_abort(r);

    uint64_t deleted = 0;
    w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_del_range(w, 1, "k", 1, "l", 1, &deleted) == SAP_OK);
    CHECK(deleted == 3);
    CHECK(txn_commit(w) == SAP_OK);

    r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r != NULL);
    CHECK(txn_get_dbi(r, 1, "k", 1, &v, &vl) == SAP_NOTFOUND);
    CHECK(txn_get_dbi(r, 1, "q", 1, &v, &vl) == SAP_OK);
    CHECK(vl == 1 && memcmp(v, "c", 1) == 0);
    txn_abort(r);

    db_close(db);
}

/* ================================================================== */
/* Test: DBI lifecycle and validation guards                            */
/* ================================================================== */

static void test_dbi_guards(void)
{
    SECTION("DBI lifecycle guards");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, 0) == SAP_OK);

    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(dbi_open(db, 2, NULL, NULL, 0) == SAP_BUSY);
    CHECK(dbi_set_dupsort(db, 1, NULL, NULL) == SAP_BUSY);
    txn_abort(w);

    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(r != NULL);
    CHECK(dbi_open(db, 2, NULL, NULL, 0) == SAP_BUSY);
    CHECK(dbi_set_dupsort(db, 1, NULL, NULL) == SAP_BUSY);

    /* Invalid DBI should not produce an unsafe cursor. */
    Cursor *bad = cursor_open_dbi(r, 999u);
    CHECK(bad == NULL);
    CHECK(txn_del_dup_dbi(r, 999u, "k", 1, "v", 1) == SAP_ERROR);
    txn_abort(r);

    /* Once no txns are active, DBI metadata changes are allowed. */
    CHECK(dbi_open(db, 2, NULL, NULL, 0) == SAP_OK);
    CHECK(dbi_set_dupsort(db, 1, NULL, NULL) == SAP_OK);

    db_close(db);
}

/* ================================================================== */
/* Test: cursor_put                                                     */
/* ================================================================== */

static void test_cursor_put(void)
{
    SECTION("cursor_put");
    DB *db = new_db();
    CHECK(dbi_open(db, 1, NULL, NULL, DBI_DUPSORT) == SAP_OK);
    Txn *txn = txn_begin(db, NULL, 0);

    for (int i = 0; i < 100; i++)
    {
        char kbuf[8], vbuf[8];
        snprintf(kbuf, sizeof(kbuf), "k%04d", i);
        snprintf(vbuf, sizeof(vbuf), "v%04d", i);
        txn_put(txn, kbuf, 5, vbuf, 5);
    }

    /* Scan and update all values via cursor */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    do
    {
        CHECK(cursor_put(cur, "UPDATED", 7, 0) == SAP_OK);
    } while (cursor_next(cur) == SAP_OK);
    cursor_close(cur);

    {
        const void *k;
        const void *v;
        uint32_t kl;
        uint32_t vl;

        cur = cursor_open(txn);
        CHECK(cur != NULL);
        CHECK(cursor_first(cur) == SAP_OK);
        CHECK(cursor_put(cur, "IGNORED", 7, SAP_RESERVE) == SAP_ERROR);
        CHECK(cursor_put(cur, "IGNORED", 7, SAP_NOOVERWRITE) == SAP_ERROR);
        CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
        CHECK(vl == 7 && memcmp(v, "UPDATED", 7) == 0);
        cursor_close(cur);
    }

    {
        const void *v;
        uint32_t vl;
        CHECK(txn_put_dbi(txn, 1, "dup", 3, "a", 1) == SAP_OK);
        cur = cursor_open_dbi(txn, 1);
        CHECK(cur != NULL);
        CHECK(cursor_first(cur) == SAP_OK);
        CHECK(cursor_put(cur, "b", 1, 0) == SAP_ERROR);
        cursor_close(cur);
        CHECK(txn_get_dbi(txn, 1, "dup", 3, &v, &vl) == SAP_OK);
        CHECK(vl == 1 && memcmp(v, "a", 1) == 0);
    }

    /* Verify all values updated */
    int errors = 0;
    for (int i = 0; i < 100; i++)
    {
        char kbuf[8];
        snprintf(kbuf, sizeof(kbuf), "k%04d", i);
        const void *v;
        uint32_t vl;
        if (txn_get(txn, kbuf, 5, &v, &vl) != SAP_OK || vl != 7 || memcmp(v, "UPDATED", 7) != 0)
            errors++;
    }
    CHECK(errors == 0);

    /* Entry count unchanged */
    SapStat stat;
    txn_stat(txn, &stat);
    CHECK(stat.num_entries == 100);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: cursor_del                                                     */
/* ================================================================== */

static void test_cursor_del(void)
{
    SECTION("cursor_del");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    for (int i = 0; i < 50; i++)
    {
        char kbuf[8];
        snprintf(kbuf, sizeof(kbuf), "d%04d", i);
        txn_put(txn, kbuf, 5, "val", 3);
    }

    /* Delete every other entry via cursor */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    int deleted = 0;
    int idx = 0;
    do
    {
        if (idx % 2 == 0)
        {
            CHECK(cursor_del(cur) == SAP_OK);
            deleted++;
            /* After del, cursor auto-advances to next entry (or becomes invalid) */
            const void *k, *v;
            uint32_t kl, vl;
            if (cursor_get(cur, &k, &kl, &v, &vl) != SAP_OK)
                break;
            idx++;
        }
        idx++;
    } while (cursor_next(cur) == SAP_OK);
    cursor_close(cur);

    SapStat stat;
    txn_stat(txn, &stat);
    CHECK(stat.num_entries == 50 - (uint64_t)deleted);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: cursor_del all entries                                         */
/* ================================================================== */

static void test_cursor_del_all(void)
{
    SECTION("cursor_del all entries");
    DB *db = new_db();
    Txn *txn = txn_begin(db, NULL, 0);

    for (int i = 0; i < 200; i++)
    {
        char kbuf[8];
        snprintf(kbuf, sizeof(kbuf), "x%04d", i);
        txn_put(txn, kbuf, 5, "v", 1);
    }

    /* Delete all via cursor — re-seek when cursor invalidated by collapse */
    Cursor *cur = cursor_open(txn);
    int count = 0;
    while (cursor_first(cur) == SAP_OK)
    {
        CHECK(cursor_del(cur) == SAP_OK);
        count++;
    }
    cursor_close(cur);

    CHECK(count == 200);
    SapStat stat;
    txn_stat(txn, &stat);
    CHECK(stat.num_entries == 0);

    txn_abort(txn);
    db_close(db);
}

/* ================================================================== */
/* Test: watch notifications                                            */
/* ================================================================== */

static void test_watch_notifications(void)
{
    SECTION("watch notifications");
    DB *db = new_db();
    struct WatchLog log;
    memset(&log, 0, sizeof(log));

    CHECK(db_watch(db, "a", 1, watch_collect, &log) == SAP_OK);

    Txn *txn = txn_begin(db, NULL, 0);
    CHECK(str_put(txn, "apple", "1") == SAP_OK);
    CHECK(str_put(txn, "banana", "2") == SAP_OK);
    CHECK(str_put(txn, "apricot", "3") == SAP_OK);
    CHECK(str_del(txn, "apricot") == SAP_OK);
    CHECK(txn_commit(txn) == SAP_OK);

    CHECK(log.count == 2);
    CHECK(log.events[0].key_len == 5);
    CHECK(memcmp(log.events[0].key, "apple", 5) == 0);
    CHECK(log.events[0].has_val == 1);
    CHECK(log.events[0].val_len == 1);
    CHECK(memcmp(log.events[0].val, "1", 1) == 0);

    CHECK(log.events[1].key_len == 7);
    CHECK(memcmp(log.events[1].key, "apricot", 7) == 0);
    CHECK(log.events[1].has_val == 0);
    CHECK(log.events[1].val_len == 0);

    CHECK(db_unwatch(db, "a", 1, watch_collect, &log) == SAP_OK);
    CHECK(db_unwatch(db, "a", 1, watch_collect, &log) == SAP_NOTFOUND);

    txn = txn_begin(db, NULL, 0);
    CHECK(str_put(txn, "apple", "9") == SAP_OK);
    CHECK(txn_commit(txn) == SAP_OK);
    CHECK(log.count == 2);

    db_close(db);
}

static void test_watch_nested_commit(void)
{
    SECTION("watch nested commit");
    DB *db = new_db();
    struct WatchLog log;
    memset(&log, 0, sizeof(log));

    CHECK(db_watch(db, "k", 1, watch_collect, &log) == SAP_OK);

    Txn *outer = txn_begin(db, NULL, 0);
    Txn *inner = txn_begin(db, outer, 0);
    CHECK(str_put(inner, "k1", "v1") == SAP_OK);
    CHECK(txn_commit(inner) == SAP_OK);
    CHECK(log.count == 0); /* child commit does not notify */

    CHECK(str_put(outer, "k2", "v2") == SAP_OK);
    CHECK(txn_commit(outer) == SAP_OK);
    CHECK(log.count == 2);

    CHECK(log.events[0].key_len == 2);
    CHECK(memcmp(log.events[0].key, "k1", 2) == 0);
    CHECK(log.events[0].has_val == 1);
    CHECK(log.events[0].val_len == 2);
    CHECK(memcmp(log.events[0].val, "v1", 2) == 0);

    CHECK(log.events[1].key_len == 2);
    CHECK(memcmp(log.events[1].key, "k2", 2) == 0);
    CHECK(log.events[1].has_val == 1);
    CHECK(log.events[1].val_len == 2);
    CHECK(memcmp(log.events[1].val, "v2", 2) == 0);

    outer = txn_begin(db, NULL, 0);
    inner = txn_begin(db, outer, 0);
    CHECK(str_put(inner, "k3", "v3") == SAP_OK);
    CHECK(txn_commit(inner) == SAP_OK);
    txn_abort(outer);
    CHECK(log.count == 2); /* abort discards pending notifications */

    CHECK(db_unwatch(db, "k", 1, watch_collect, &log) == SAP_OK);
    db_close(db);
}

static void test_watch_api_hardening(void)
{
    SECTION("watch API hardening");
    DB *db = new_db();
    struct WatchLog log0, log1;
    Txn *w;
    memset(&log0, 0, sizeof(log0));
    memset(&log1, 0, sizeof(log1));

    CHECK(dbi_open(db, 1, NULL, NULL, 0) == SAP_OK);
    CHECK(dbi_open(db, 2, NULL, NULL, DBI_DUPSORT) == SAP_OK);

    CHECK(db_watch(db, "a", 1, watch_collect, &log0) == SAP_OK);
    CHECK(db_watch(db, "a", 1, watch_collect, &log0) == SAP_EXISTS);
    CHECK(db_watch_dbi(db, 1, "a", 1, watch_collect, &log1) == SAP_OK);
    CHECK(db_watch_dbi(db, 1, "a", 1, watch_collect, &log1) == SAP_EXISTS);
    CHECK(db_watch_dbi(db, 2, "a", 1, watch_collect, &log1) == SAP_ERROR);
    CHECK(db_watch_dbi(db, 99, "a", 1, watch_collect, &log1) == SAP_ERROR);

    /* DBI metadata change is blocked while watchers are installed. */
    CHECK(dbi_set_dupsort(db, 1, NULL, NULL) == SAP_BUSY);

    w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(db_watch(db, "b", 1, watch_collect, &log0) == SAP_BUSY);
    CHECK(db_unwatch(db, "a", 1, watch_collect, &log0) == SAP_BUSY);
    CHECK(db_watch_dbi(db, 1, "b", 1, watch_collect, &log1) == SAP_BUSY);
    CHECK(db_unwatch_dbi(db, 1, "a", 1, watch_collect, &log1) == SAP_BUSY);
    txn_abort(w);

    w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_put_dbi(w, 0, "a0", 2, "v0", 2) == SAP_OK);
    CHECK(txn_put_dbi(w, 1, "a1", 2, "v1", 2) == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    CHECK(log0.count == 1);
    CHECK(log0.events[0].key_len == 2);
    CHECK(memcmp(log0.events[0].key, "a0", 2) == 0);
    CHECK(log0.events[0].has_val == 1);
    CHECK(log0.events[0].val_len == 2);
    CHECK(memcmp(log0.events[0].val, "v0", 2) == 0);

    CHECK(log1.count == 1);
    CHECK(log1.events[0].key_len == 2);
    CHECK(memcmp(log1.events[0].key, "a1", 2) == 0);
    CHECK(log1.events[0].has_val == 1);
    CHECK(log1.events[0].val_len == 2);
    CHECK(memcmp(log1.events[0].val, "v1", 2) == 0);

    CHECK(db_unwatch(db, "a", 1, watch_collect, &log1) == SAP_NOTFOUND);
    CHECK(db_unwatch(db, "a", 1, watch_collect, &log0) == SAP_OK);
    CHECK(db_unwatch_dbi(db, 1, "a", 1, watch_collect, &log1) == SAP_OK);
    CHECK(dbi_set_dupsort(db, 1, NULL, NULL) == SAP_OK);

    db_close(db);
}

/* ================================================================== */
/* Thread-safety tests (only when SAPLING_THREADED is defined)          */
/* ================================================================== */

#ifdef SAPLING_THREADED
#include <pthread.h>

static void *reader_thread(void *arg)
{
    DB *db = (DB *)arg;
    for (int i = 0; i < 1000; i++)
    {
        Txn *r = txn_begin(db, NULL, TXN_RDONLY);
        if (!r)
            continue;
        Cursor *cur = cursor_open(r);
        if (cur)
        {
            if (cursor_first(cur) == SAP_OK)
            {
                const void *k, *v;
                uint32_t kl, vl;
                while (cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK)
                {
                    if (cursor_next(cur) != SAP_OK)
                        break;
                }
            }
            cursor_close(cur);
        }
        txn_abort(r);
    }
    return NULL;
}

static void *writer_thread(void *arg)
{
    DB *db = (DB *)arg;
    char kbuf[16], vbuf[16];
    for (int i = 0; i < 500; i++)
    {
        Txn *w = txn_begin(db, NULL, 0);
        if (!w)
            continue; /* another writer active, retry */
        snprintf(kbuf, sizeof(kbuf), "tw%06d", i);
        snprintf(vbuf, sizeof(vbuf), "val%06d", i);
        txn_put(w, kbuf, (uint32_t)strlen(kbuf), vbuf, (uint32_t)strlen(vbuf));
        txn_commit(w);
    }
    return NULL;
}

static void test_concurrent_readers(void)
{
    SECTION("concurrent readers (threaded)");
    DB *db = new_db();

    /* Populate data */
    Txn *txn = txn_begin(db, NULL, 0);
    for (int i = 0; i < 1000; i++)
    {
        char kbuf[16];
        snprintf(kbuf, sizeof(kbuf), "cr%06d", i);
        txn_put(txn, kbuf, (uint32_t)strlen(kbuf), "v", 1);
    }
    txn_commit(txn);

    /* Spawn 4 reader threads */
    pthread_t threads[4];
    for (int i = 0; i < 4; i++)
        pthread_create(&threads[i], NULL, reader_thread, db);
    for (int i = 0; i < 4; i++)
        pthread_join(threads[i], NULL);

    CHECK(1); /* if we get here without crash, success */
    db_close(db);
}

static void test_writer_reader_concurrent(void)
{
    SECTION("writer + reader concurrent (threaded)");
    DB *db = new_db();

    pthread_t readers[3], writer;
    pthread_create(&writer, NULL, writer_thread, db);
    for (int i = 0; i < 3; i++)
        pthread_create(&readers[i], NULL, reader_thread, db);

    pthread_join(writer, NULL);
    for (int i = 0; i < 3; i++)
        pthread_join(readers[i], NULL);

    /* Verify data written by writer thread */
    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    int found = 0;
    for (int i = 0; i < 500; i++)
    {
        char kbuf[16];
        snprintf(kbuf, sizeof(kbuf), "tw%06d", i);
        const void *v;
        uint32_t vl;
        if (txn_get(r, kbuf, (uint32_t)strlen(kbuf), &v, &vl) == SAP_OK)
            found++;
    }
    CHECK(found == 500);
    txn_abort(r);

    db_close(db);
}

#endif /* SAPLING_THREADED */

/* ================================================================== */
/* main                                                                 */
/* ================================================================== */

int main(void)
{
    test_empty_tree();
    test_single_element();
    test_basic_crud();
    test_range_scan();
    test_large_dataset();
    test_snapshot_isolation();
    test_nested_commit();
    test_nested_abort();
    test_deep_nested();
    test_freelist_recycling();
    test_txn_abort();
    test_readonly_flag();
    test_binary_keys();
    test_seek_boundaries();
    test_delete_reinsert();
    test_multi_commit();
    test_input_validation();
    test_sap_full();
    test_overflow_values();
    test_runtime_page_size_safety();
    test_write_contention();
    test_leaf_capacity();
    test_custom_comparator();
    test_entry_count();
    test_entry_count_nested();
    test_statistics_api();
    test_integer_key_comparator();
    test_large_dataset_100k();
    test_ascending_insert();
    test_descending_insert();
    test_multiple_reader_snapshots();
    test_interleaved_put_delete();
    test_cursor_stability();
    test_cursor_renew();
    test_cursor_get_key();
    test_nooverwrite();
    test_reserve();
    test_put_if();
    test_multi_dbi();
    test_multi_dbi_txn();
    test_checkpoint_restore();
    test_count_range();
    test_del_range();
    test_merge();
    test_load_sorted();
    test_prefix_helpers();
    test_dupsort_apis();
    test_dupsort_value_comparator();
    test_dbi_guards();
    test_cursor_put();
    test_cursor_del();
    test_cursor_del_all();
    test_watch_notifications();
    test_watch_nested_commit();
    test_watch_api_hardening();

#ifdef SAPLING_THREADED
    test_concurrent_readers();
    test_writer_reader_concurrent();
#endif

    print_summary();
    return (g_fail > 0) ? 1 : 0;
}
