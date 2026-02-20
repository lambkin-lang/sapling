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
{ (void)ctx; return malloc((size_t)sz); }

static void test_free(void *ctx, void *p, uint32_t sz)
{ (void)ctx; (void)sz; free(p); }

static PageAllocator g_alloc = { test_alloc, test_free, NULL };

/* ================================================================== */
/* Simple test framework                                                */
/* ================================================================== */

static int g_pass = 0, g_fail = 0;

#define CHECK(expr) do { \
    if (expr) { g_pass++; } \
    else { fprintf(stderr, "FAIL: %s  (%s:%d)\n", #expr, __FILE__, __LINE__); g_fail++; } \
} while(0)

#define SECTION(name) printf("--- %s ---\n", name)

static void print_summary(void)
{
    printf("\nResults: %d passed, %d failed\n", g_pass, g_fail);
}

/* ================================================================== */
/* Helpers                                                              */
/* ================================================================== */

static DB *new_db(void)
{
    return db_open(&g_alloc, SAPLING_PAGE_SIZE);
}

static int str_put(Txn *txn, const char *key, const char *val)
{
    return txn_put(txn, key, (uint32_t)strlen(key),
                       val, (uint32_t)strlen(val));
}

static int str_get(Txn *txn, const char *key,
                   const void **val_out, uint32_t *val_len_out)
{
    return txn_get(txn, key, (uint32_t)strlen(key), val_out, val_len_out);
}

static int str_del(Txn *txn, const char *key)
{
    return txn_del(txn, key, (uint32_t)strlen(key));
}

/* Check that key maps to the given string value */
static int check_str(Txn *txn, const char *key, const char *expected)
{
    const void *v; uint32_t vl;
    int rc = str_get(txn, key, &v, &vl);
    if (rc != SAP_OK) return 0;
    if (vl != (uint32_t)strlen(expected)) return 0;
    return memcmp(v, expected, vl) == 0;
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
    CHECK(str_put(txn, "foo",   "bar")   == SAP_OK);
    CHECK(str_put(txn, "abc",   "123")   == SAP_OK);

    /* Get */
    CHECK(check_str(txn, "hello", "world"));
    CHECK(check_str(txn, "foo",   "bar"));
    CHECK(check_str(txn, "abc",   "123"));

    /* Not found */
    const void *v; uint32_t vl;
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
    CHECK(check_str(txn, "abc",   "123"));
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

    const void *v; uint32_t vl;
    CHECK(txn_get(txn, "k", 1, &v, &vl) == SAP_NOTFOUND);
    CHECK(txn_del(txn, "k", 1) == SAP_NOTFOUND);

    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_NOTFOUND);
    CHECK(cursor_last(cur)  == SAP_NOTFOUND);
    CHECK(cursor_next(cur)  == SAP_NOTFOUND);
    CHECK(cursor_prev(cur)  == SAP_NOTFOUND);
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
    const void *k, *v; uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 4 && memcmp(k, "only", 4) == 0);
    CHECK(vl == 3 && memcmp(v, "one",  3) == 0);
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
    const void *v2; uint32_t vl2;
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
    const char *keys[] = {"d","b","f","a","c","e","g"};
    for (int i = 0; i < 7; i++)
        CHECK(str_put(txn, keys[i], keys[i]) == SAP_OK);

    /* Forward scan should give alphabetical order */
    const char *expected[] = {"a","b","c","d","e","f","g"};
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    for (int i = 0; i < 7; i++) {
        const void *k, *v; uint32_t kl, vl;
        CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
        CHECK(kl == 1 && *(char*)k == expected[i][0]);
        if (i < 6) CHECK(cursor_next(cur) == SAP_OK);
    }
    CHECK(cursor_next(cur) == SAP_NOTFOUND);
    cursor_close(cur);

    /* Backward scan */
    cur = cursor_open(txn);
    CHECK(cursor_last(cur) == SAP_OK);
    for (int i = 6; i >= 0; i--) {
        const void *k, *v; uint32_t kl, vl;
        CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
        CHECK(kl == 1 && *(char*)k == expected[i][0]);
        if (i > 0) CHECK(cursor_prev(cur) == SAP_OK);
    }
    CHECK(cursor_prev(cur) == SAP_NOTFOUND);
    cursor_close(cur);

    /* Seek */
    cur = cursor_open(txn);
    CHECK(cursor_seek(cur, "c", 1) == SAP_OK);
    const void *k, *v; uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && *(char*)k == 'c');
    cursor_close(cur);

    /* Seek to non-existent key between entries */
    cur = cursor_open(txn);
    CHECK(cursor_seek(cur, "bb", 2) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl == 1 && *(char*)k == 'c');
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
    int *order = (int*)malloc((size_t)N * sizeof(int));
    for (int i = 0; i < N; i++) order[i] = i;
    for (int i = N - 1; i > 0; i--) {
        state = state * 1664525u + 1013904223u;
        int j = (int)(state % (uint32_t)(i + 1));
        int tmp = order[i]; order[i] = order[j]; order[j] = tmp;
    }

    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "%06d", order[i]);
        snprintf(vbuf, sizeof(vbuf), "val%06d", order[i]);
        CHECK(txn_put(txn, kbuf, 6, vbuf, (uint32_t)strlen(vbuf)) == SAP_OK);
    }
    free(order);

    /* Verify all keys */
    int errors = 0;
    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "%06d", i);
        snprintf(vbuf, sizeof(vbuf), "val%06d", i);
        const void *v; uint32_t vl;
        int rc = txn_get(txn, kbuf, 6, &v, &vl);
        if (rc != SAP_OK || vl != strlen(vbuf) ||
            memcmp(v, vbuf, vl) != 0) errors++;
    }
    CHECK(errors == 0);

    /* Scan: should visit all N keys in order */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    int count = 0;
    int scan_err = 0;
    do {
        const void *k, *v; uint32_t kl, vl;
        if (cursor_get(cur, &k, &kl, &v, &vl) != SAP_OK) { scan_err++; break; }
        snprintf(kbuf, sizeof(kbuf), "%06d", count);
        if (kl != 6 || memcmp(k, kbuf, 6) != 0) scan_err++;
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
    CHECK(str_put(w, "y", "new")      == SAP_OK);
    CHECK(txn_commit(w) == SAP_OK);

    /* Read snapshot still sees original */
    CHECK(check_str(r, "x", "original"));
    const void *v; uint32_t vl;
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

    CHECK(txn_commit(inner) == SAP_OK);   /* child commit: visible to outer */

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
    CHECK(str_put(inner, "stable",   "overwrite") == SAP_OK);
    txn_abort(inner);   /* discard all inner changes */

    /* Outer still sees its original writes */
    CHECK(check_str(outer, "stable", "yes"));
    const void *v; uint32_t vl;
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
    for (int i = 0; i < 8; i++) {
        char key[4] = {'k', (char)('0'+i), 0};
        char val[4] = {'v', (char)('0'+i), 0};
        CHECK(str_put(t[i], key, val) == SAP_OK);
        if (i < 7)
            t[i+1] = txn_begin(db, t[i], 0);
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
    for (int i = 0; i <= 5; i++) {
        char key[4] = {'k', (char)('0'+i), 0};
        char val[4] = {'v', (char)('0'+i), 0};
        CHECK(check_str(r, key, val));
    }
    /* k6 and k7 were aborted */
    const void *v; uint32_t vl;
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
    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "recycle%04d", i);
        snprintf(vbuf, sizeof(vbuf), "val%04d", i);
        CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf),
                          vbuf, (uint32_t)strlen(vbuf)) == SAP_OK);
    }
    CHECK(txn_commit(txn) == SAP_OK);
    uint32_t pages_after_insert = db_num_pages(db);

    /* Phase 2: delete all N keys */
    txn = txn_begin(db, NULL, 0);
    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "recycle%04d", i);
        txn_del(txn, kbuf, (uint32_t)strlen(kbuf));
    }
    CHECK(txn_commit(txn) == SAP_OK);

    /* Phase 3: insert N keys again */
    txn = txn_begin(db, NULL, 0);
    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "recycle%04d", i);
        snprintf(vbuf, sizeof(vbuf), "new%04d", i);
        CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf),
                          vbuf, (uint32_t)strlen(vbuf)) == SAP_OK);
    }
    CHECK(txn_commit(txn) == SAP_OK);

    /* Page count should not have grown significantly beyond first-insert peak */
    uint32_t pages_after_reinsertion = db_num_pages(db);
    /* Allow up to 20% growth over baseline to account for meta/overhead variation */
    CHECK(pages_after_reinsertion <= pages_after_insert * 12 / 10);

    /* Verify data */
    txn = txn_begin(db, NULL, TXN_RDONLY);
    int errors = 0;
    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "recycle%04d", i);
        snprintf(vbuf, sizeof(vbuf), "new%04d", i);
        const void *v; uint32_t vl;
        int rc = txn_get(txn, kbuf, (uint32_t)strlen(kbuf), &v, &vl);
        if (rc != SAP_OK || vl != strlen(vbuf) ||
            memcmp(v, vbuf, vl) != 0) errors++;
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
    const void *v; uint32_t vl;
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
    CHECK(str_del(r, "k")      == SAP_READONLY);
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

    const void *v; uint32_t vl;
    CHECK(txn_get(txn, k1, 3, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, v1, 2) == 0);
    CHECK(txn_get(txn, k2, 3, &v, &vl) == SAP_OK);
    CHECK(vl == 2 && memcmp(v, v2, 2) == 0);

    /* Prefix ordering: k1 < k2 */
    Cursor *cur = cursor_open(txn);
    CHECK(cursor_first(cur) == SAP_OK);
    const void *k; uint32_t kl;
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
    const void *k, *v; uint32_t kl, vl;
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl==1 && *(char*)k=='b');

    /* Seek to exact key */
    CHECK(cursor_seek(cur, "d", 1) == SAP_OK);
    CHECK(cursor_get(cur, &k, &kl, &v, &vl) == SAP_OK);
    CHECK(kl==1 && *(char*)k=='d');

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
    CHECK(str_del(txn, "k")       == SAP_OK);
    const void *v; uint32_t vl;
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
    for (int batch = 0; batch < 5; batch++) {
        Txn *txn = txn_begin(db, NULL, 0);
        for (int i = 0; i < N; i++) {
            snprintf(kbuf, sizeof(kbuf), "b%d_%04d", batch, i);
            snprintf(vbuf, sizeof(vbuf), "v%d_%04d", batch, i);
            CHECK(txn_put(txn, kbuf, (uint32_t)strlen(kbuf),
                              vbuf, (uint32_t)strlen(vbuf)) == SAP_OK);
        }
        CHECK(txn_commit(txn) == SAP_OK);

        /* Verify this batch */
        txn = txn_begin(db, NULL, TXN_RDONLY);
        int errs = 0;
        for (int i = 0; i < N; i++) {
            snprintf(kbuf, sizeof(kbuf), "b%d_%04d", batch, i);
            snprintf(vbuf, sizeof(vbuf), "v%d_%04d", batch, i);
            if (!check_str(txn, kbuf, vbuf)) errs++;
        }
        CHECK(errs == 0);
        txn_abort(txn);
    }

    /* Now delete all */
    for (int batch = 0; batch < 5; batch++) {
        Txn *txn = txn_begin(db, NULL, 0);
        for (int i = 0; i < N; i++) {
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

    print_summary();
    return (g_fail > 0) ? 1 : 0;
}
