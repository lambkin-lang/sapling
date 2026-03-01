/*
 * test_hamt.c -- Unit tests for the HAMT subsystem
 *
 * Follows test_bept.c pattern: CHECK macro, env/arena setup, txn lifecycle.
 * Covers CRUD, API guards, transactions, structural cases, and collision
 * paths via the SAPLING_HAMT_TESTING hash override seam.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/sapling.h"
#include "sapling/txn.h"
#include "sapling/hamt.h"
#include "sapling/arena.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expr)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (!(expr))                                                                               \
        {                                                                                          \
            fprintf(stderr, "FAIL: %s (%s:%d)\n", #expr, __FILE__, __LINE__);                      \
            exit(1);                                                                               \
        }                                                                                          \
    } while (0)

/* ===== Helpers ===== */

static SapEnv *g_env;
static SapMemArena *g_arena;

static void setup(void)
{
    SapArenaOptions opts = {0};
    opts.page_size = 4096;
    opts.type = SAP_ARENA_BACKING_MALLOC;

    int rc = sap_arena_init(&g_arena, &opts);
    CHECK(rc == ERR_OK);
    CHECK(g_arena != NULL);

    g_env = sap_env_create(g_arena, 4096);
    CHECK(g_env != NULL);

    rc = sap_hamt_subsystem_init(g_env);
    CHECK(rc == ERR_OK);
}

static void teardown(void)
{
    sap_env_destroy(g_env);
    sap_arena_destroy(g_arena);
    g_env = NULL;
    g_arena = NULL;
}

/* ===== CRUD Tests ===== */

static void test_single_insert_retrieve(void)
{
    printf("  test_single_insert_retrieve\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    const char *key = "hello";
    const char *val = "world";
    int rc = sap_hamt_put(txn, key, 5, val, 5, 0);
    CHECK(rc == ERR_OK);

    const void *val_out;
    uint32_t val_len;
    rc = sap_hamt_get(txn, key, 5, &val_out, &val_len);
    CHECK(rc == ERR_OK);
    CHECK(val_len == 5);
    CHECK(memcmp(val_out, "world", 5) == 0);

    rc = sap_txn_commit(txn);
    CHECK(rc == ERR_OK);
}

static void test_two_inserts(void)
{
    printf("  test_two_inserts\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "aaa", 3, "v1", 2, 0);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_put(txn, "bbb", 3, "v2", 2, 0);
    CHECK(rc == ERR_OK);

    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "aaa", 3, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 2);
    CHECK(memcmp(out, "v1", 2) == 0);

    rc = sap_hamt_get(txn, "bbb", 3, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 2);
    CHECK(memcmp(out, "v2", 2) == 0);

    sap_txn_abort(txn);
}

static void test_replace_value(void)
{
    printf("  test_replace_value\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "key", 3, "old", 3, 0);
    CHECK(rc == ERR_OK);

    rc = sap_hamt_put(txn, "key", 3, "new", 3, 0);
    CHECK(rc == ERR_OK);

    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "key", 3, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 3);
    CHECK(memcmp(out, "new", 3) == 0);

    sap_txn_abort(txn);
}

static void test_nooverwrite(void)
{
    printf("  test_nooverwrite\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "key", 3, "val", 3, 0);
    CHECK(rc == ERR_OK);

    rc = sap_hamt_put(txn, "key", 3, "new", 3, SAP_NOOVERWRITE);
    CHECK(rc == ERR_EXISTS);

    /* Original value preserved */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "key", 3, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 3);
    CHECK(memcmp(out, "val", 3) == 0);

    sap_txn_abort(txn);
}

static void test_missing_key(void)
{
    printf("  test_missing_key\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    const void *out;
    uint32_t len;
    int rc = sap_hamt_get(txn, "nope", 4, &out, &len);
    CHECK(rc == ERR_NOT_FOUND);

    sap_txn_abort(txn);
}

static void test_delete(void)
{
    printf("  test_delete\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "abc", 3, "123", 3, 0);
    CHECK(rc == ERR_OK);

    rc = sap_hamt_del(txn, "abc", 3);
    CHECK(rc == ERR_OK);

    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "abc", 3, &out, &len);
    CHECK(rc == ERR_NOT_FOUND);

    /* Double delete */
    rc = sap_hamt_del(txn, "abc", 3);
    CHECK(rc == ERR_NOT_FOUND);

    sap_txn_abort(txn);
}

static void test_delete_with_committed_root(void)
{
    printf("  test_delete_with_committed_root\n");

    /* Fresh env: commit exactly 1 key, then mass insert+delete in new txn */
    teardown();
    setup();

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);
    int rc = sap_hamt_put(txn, "hello", 5, "world", 5, 0);
    CHECK(rc == ERR_OK);
    rc = sap_txn_commit(txn);
    CHECK(rc == ERR_OK);

    txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int i;
    char keybuf[16];
    for (i = 0; i < 1000; i++)
    {
        int klen = snprintf(keybuf, sizeof(keybuf), "k%d", i);
        rc = sap_hamt_put(txn, keybuf, (uint32_t)klen, "v", 1, 0);
        CHECK(rc == ERR_OK);
    }
    for (i = 0; i < 1000; i++)
    {
        int klen = snprintf(keybuf, sizeof(keybuf), "k%d", i);
        rc = sap_hamt_del(txn, keybuf, (uint32_t)klen);
        CHECK(rc == ERR_OK);
    }

    /* hello should still be there */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "hello", 5, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 5);
    CHECK(memcmp(out, "world", 5) == 0);

    sap_txn_abort(txn);
}

static void test_mass_insert_retrieve_delete(void)
{
    printf("  test_mass_insert_retrieve_delete\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int count = 1000;
    int i;
    char keybuf[16];
    char valbuf[16];

    /* Insert */
    for (i = 0; i < count; i++)
    {
        int klen = snprintf(keybuf, sizeof(keybuf), "k%d", i);
        int vlen = snprintf(valbuf, sizeof(valbuf), "v%d", i);
        int rc = sap_hamt_put(txn, keybuf, (uint32_t)klen, valbuf, (uint32_t)vlen, 0);
        CHECK(rc == ERR_OK);
    }

    /* Retrieve */
    for (i = 0; i < count; i++)
    {
        int klen = snprintf(keybuf, sizeof(keybuf), "k%d", i);
        int vlen = snprintf(valbuf, sizeof(valbuf), "v%d", i);
        const void *out;
        uint32_t len;
        int rc = sap_hamt_get(txn, keybuf, (uint32_t)klen, &out, &len);
        CHECK(rc == ERR_OK);
        CHECK(len == (uint32_t)vlen);
        CHECK(memcmp(out, valbuf, len) == 0);
    }

    /* Delete */
    for (i = 0; i < count; i++)
    {
        int klen = snprintf(keybuf, sizeof(keybuf), "k%d", i);
        int rc = sap_hamt_del(txn, keybuf, (uint32_t)klen);
        CHECK(rc == ERR_OK);
    }

    /* Verify empty */
    for (i = 0; i < count; i++)
    {
        int klen = snprintf(keybuf, sizeof(keybuf), "k%d", i);
        const void *out;
        uint32_t len;
        int rc = sap_hamt_get(txn, keybuf, (uint32_t)klen, &out, &len);
        CHECK(rc == ERR_NOT_FOUND);
    }

    sap_txn_abort(txn);
}

static void test_zero_length_key_value(void)
{
    printf("  test_zero_length_key_value\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    /* Zero-length key with zero-length value */
    int rc = sap_hamt_put(txn, NULL, 0, NULL, 0, 0);
    CHECK(rc == ERR_OK);

    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, NULL, 0, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 0);

    /* Delete zero-length key */
    rc = sap_hamt_del(txn, NULL, 0);
    CHECK(rc == ERR_OK);

    rc = sap_hamt_get(txn, NULL, 0, &out, &len);
    CHECK(rc == ERR_NOT_FOUND);

    sap_txn_abort(txn);
}

/* ===== API Guard Tests ===== */

static void test_readonly_txn(void)
{
    printf("  test_readonly_txn\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, TXN_RDONLY);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "key", 3, "val", 3, 0);
    CHECK(rc == ERR_READONLY);

    rc = sap_hamt_del(txn, "key", 3);
    CHECK(rc == ERR_READONLY);

    /* Get on empty is fine */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "key", 3, &out, &len);
    CHECK(rc == ERR_NOT_FOUND);

    sap_txn_abort(txn);
}

static void test_unsupported_flags(void)
{
    printf("  test_unsupported_flags\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "key", 3, "val", 3, 0xFF);
    CHECK(rc == ERR_INVALID);

    sap_txn_abort(txn);
}

static void test_null_key_nonzero_len(void)
{
    printf("  test_null_key_nonzero_len\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, NULL, 5, "val", 3, 0);
    CHECK(rc == ERR_INVALID);

    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, NULL, 5, &out, &len);
    CHECK(rc == ERR_INVALID);

    sap_txn_abort(txn);
}

/* ===== Transaction Tests ===== */

static void test_nested_txn_commit(void)
{
    printf("  test_nested_txn_commit\n");

    /* Parent txn: insert a key */
    SapTxnCtx *parent = sap_txn_begin(g_env, NULL, 0);
    CHECK(parent != NULL);

    int rc = sap_hamt_put(parent, "p", 1, "pv", 2, 0);
    CHECK(rc == ERR_OK);

    /* Child txn: insert another key */
    SapTxnCtx *child = sap_txn_begin(g_env, parent, 0);
    CHECK(child != NULL);

    rc = sap_hamt_put(child, "c", 1, "cv", 2, 0);
    CHECK(rc == ERR_OK);

    /* Child can see parent's key */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(child, "p", 1, &out, &len);
    CHECK(rc == ERR_OK);

    /* Commit child */
    rc = sap_txn_commit(child);
    CHECK(rc == ERR_OK);

    /* Parent should see child's key */
    rc = sap_hamt_get(parent, "c", 1, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 2);
    CHECK(memcmp(out, "cv", 2) == 0);

    sap_txn_abort(parent);
}

static void test_nested_txn_abort(void)
{
    printf("  test_nested_txn_abort\n");

    SapTxnCtx *parent = sap_txn_begin(g_env, NULL, 0);
    CHECK(parent != NULL);

    int rc = sap_hamt_put(parent, "p", 1, "pv", 2, 0);
    CHECK(rc == ERR_OK);

    /* Child txn */
    SapTxnCtx *child = sap_txn_begin(g_env, parent, 0);
    CHECK(child != NULL);

    rc = sap_hamt_put(child, "c", 1, "cv", 2, 0);
    CHECK(rc == ERR_OK);

    /* Abort child */
    sap_txn_abort(child);

    /* Parent should NOT see child's key */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(parent, "c", 1, &out, &len);
    CHECK(rc == ERR_NOT_FOUND);

    /* Parent's own key still visible */
    rc = sap_hamt_get(parent, "p", 1, &out, &len);
    CHECK(rc == ERR_OK);

    sap_txn_abort(parent);
}

/* ===== Structural Tests ===== */

static void test_branch_collapse_after_delete(void)
{
    printf("  test_branch_collapse_after_delete\n");
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    /* Insert two keys that will share a branch node */
    int rc = sap_hamt_put(txn, "alpha", 5, "A", 1, 0);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_put(txn, "beta", 4, "B", 1, 0);
    CHECK(rc == ERR_OK);

    /* Delete one */
    rc = sap_hamt_del(txn, "alpha", 5);
    CHECK(rc == ERR_OK);

    /* Other still works */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "beta", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 1);
    CHECK(*(const char *)out == 'B');

    /* Deleted one is gone */
    rc = sap_hamt_get(txn, "alpha", 5, &out, &len);
    CHECK(rc == ERR_NOT_FOUND);

    sap_txn_abort(txn);
}

/* ===== Collision Tests (via hash override seam) ===== */

/* Force all keys to hash to 0xDEADBEEF */
static uint32_t forced_collision_hash(const void *key, uint32_t len)
{
    (void)key;
    (void)len;
    return 0xDEADBEEFu;
}

static void test_collision_insert_and_retrieve(void)
{
    printf("  test_collision_insert_and_retrieve\n");

    hamt_test_set_hash_fn(forced_collision_hash);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    /* Two different keys forced to same hash → collision node */
    int rc = sap_hamt_put(txn, "keyA", 4, "valA", 4, 0);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_put(txn, "keyB", 4, "valB", 4, 0);
    CHECK(rc == ERR_OK);

    /* Both retrievable */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "keyA", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 4);
    CHECK(memcmp(out, "valA", 4) == 0);

    rc = sap_hamt_get(txn, "keyB", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 4);
    CHECK(memcmp(out, "valB", 4) == 0);

    sap_txn_abort(txn);
    hamt_test_reset_hash_fn();
}

static void test_collision_delete_collapse(void)
{
    printf("  test_collision_delete_collapse\n");

    hamt_test_set_hash_fn(forced_collision_hash);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "keyA", 4, "valA", 4, 0);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_put(txn, "keyB", 4, "valB", 4, 0);
    CHECK(rc == ERR_OK);

    /* Delete A → collision node collapses to single leaf */
    rc = sap_hamt_del(txn, "keyA", 4);
    CHECK(rc == ERR_OK);

    /* A is gone */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "keyA", 4, &out, &len);
    CHECK(rc == ERR_NOT_FOUND);

    /* B still readable */
    rc = sap_hamt_get(txn, "keyB", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 4);
    CHECK(memcmp(out, "valB", 4) == 0);

    sap_txn_abort(txn);
    hamt_test_reset_hash_fn();
}

static void test_collision_replace(void)
{
    printf("  test_collision_replace\n");

    hamt_test_set_hash_fn(forced_collision_hash);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "keyA", 4, "valA", 4, 0);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_put(txn, "keyB", 4, "valB", 4, 0);
    CHECK(rc == ERR_OK);

    /* Replace A's value within collision */
    rc = sap_hamt_put(txn, "keyA", 4, "newA", 4, 0);
    CHECK(rc == ERR_OK);

    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "keyA", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(len == 4);
    CHECK(memcmp(out, "newA", 4) == 0);

    /* B unchanged */
    rc = sap_hamt_get(txn, "keyB", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(out, "valB", 4) == 0);

    sap_txn_abort(txn);
    hamt_test_reset_hash_fn();
}

static void test_collision_nooverwrite(void)
{
    printf("  test_collision_nooverwrite\n");

    hamt_test_set_hash_fn(forced_collision_hash);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "keyA", 4, "valA", 4, 0);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_put(txn, "keyB", 4, "valB", 4, 0);
    CHECK(rc == ERR_OK);

    /* NOOVERWRITE on existing collision entry */
    rc = sap_hamt_put(txn, "keyA", 4, "newA", 4, SAP_NOOVERWRITE);
    CHECK(rc == ERR_EXISTS);

    /* Original preserved */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "keyA", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(out, "valA", 4) == 0);

    sap_txn_abort(txn);
    hamt_test_reset_hash_fn();
}

static void test_collision_three_entries(void)
{
    printf("  test_collision_three_entries\n");

    hamt_test_set_hash_fn(forced_collision_hash);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "keyA", 4, "vA", 2, 0);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_put(txn, "keyB", 4, "vB", 2, 0);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_put(txn, "keyC", 4, "vC", 2, 0);
    CHECK(rc == ERR_OK);

    /* All three retrievable */
    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "keyA", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(out, "vA", 2) == 0);
    rc = sap_hamt_get(txn, "keyB", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(out, "vB", 2) == 0);
    rc = sap_hamt_get(txn, "keyC", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(out, "vC", 2) == 0);

    /* Delete B → shrink collision (3→2), not collapse */
    rc = sap_hamt_del(txn, "keyB", 4);
    CHECK(rc == ERR_OK);

    rc = sap_hamt_get(txn, "keyB", 4, &out, &len);
    CHECK(rc == ERR_NOT_FOUND);
    rc = sap_hamt_get(txn, "keyA", 4, &out, &len);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_get(txn, "keyC", 4, &out, &len);
    CHECK(rc == ERR_OK);

    /* Delete A → collapse collision (2→1) to leaf */
    rc = sap_hamt_del(txn, "keyA", 4);
    CHECK(rc == ERR_OK);

    rc = sap_hamt_get(txn, "keyC", 4, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(out, "vC", 2) == 0);

    sap_txn_abort(txn);
    hamt_test_reset_hash_fn();
}

/* Normal hash test: confirm default FNV path still works */
static void test_normal_hash_still_works(void)
{
    printf("  test_normal_hash_still_works\n");

    /* Hash should be reset already, but be explicit */
    hamt_test_reset_hash_fn();

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    int rc = sap_hamt_put(txn, "foo", 3, "bar", 3, 0);
    CHECK(rc == ERR_OK);
    rc = sap_hamt_put(txn, "baz", 3, "qux", 3, 0);
    CHECK(rc == ERR_OK);

    const void *out;
    uint32_t len;
    rc = sap_hamt_get(txn, "foo", 3, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(out, "bar", 3) == 0);

    rc = sap_hamt_get(txn, "baz", 3, &out, &len);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(out, "qux", 3) == 0);

    sap_txn_abort(txn);
}

/* ===== Arena Sanity Tests ===== */

static void test_arena_pages_after_abort(void)
{
    printf("  test_arena_pages_after_abort\n");

    uint32_t baseline = sap_arena_active_pages(g_arena);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);

    /* Insert several entries to allocate arena nodes */
    int i;
    char keybuf[16];
    for (i = 0; i < 50; i++)
    {
        int klen = snprintf(keybuf, sizeof(keybuf), "arenakey%d", i);
        int rc = sap_hamt_put(txn, keybuf, (uint32_t)klen, "v", 1, 0);
        CHECK(rc == ERR_OK);
    }

    /* Active pages should have grown */
    CHECK(sap_arena_active_pages(g_arena) > baseline);

    /* Abort should free the allocated nodes */
    sap_txn_abort(txn);

    CHECK(sap_arena_active_pages(g_arena) == baseline);
}

static void test_arena_no_growth_across_aborts(void)
{
    printf("  test_arena_no_growth_across_aborts\n");

    uint32_t baseline = sap_arena_active_pages(g_arena);

    /* Repeated put/abort cycles should not grow arena */
    int round;
    for (round = 0; round < 10; round++)
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(txn != NULL);

        int i;
        char keybuf[16];
        for (i = 0; i < 20; i++)
        {
            int klen = snprintf(keybuf, sizeof(keybuf), "rk%d", i);
            int rc = sap_hamt_put(txn, keybuf, (uint32_t)klen, "v", 1, 0);
            CHECK(rc == ERR_OK);
        }

        sap_txn_abort(txn);
    }

    CHECK(sap_arena_active_pages(g_arena) == baseline);
}

/* ===== Main ===== */

int main(void)
{
    printf("Running test_hamt...\n");

    setup();

    /* CRUD */
    printf("CRUD tests:\n");
    test_single_insert_retrieve();
    test_two_inserts();
    test_replace_value();
    test_nooverwrite();
    test_missing_key();
    test_delete();
    test_delete_with_committed_root();
    test_mass_insert_retrieve_delete();
    test_zero_length_key_value();

    /* API guards */
    printf("API guard tests:\n");
    test_readonly_txn();
    test_unsupported_flags();
    test_null_key_nonzero_len();

    /* Transaction */
    printf("Transaction tests:\n");
    test_nested_txn_commit();
    test_nested_txn_abort();

    /* Structural */
    printf("Structural tests:\n");
    test_branch_collapse_after_delete();

    /* Collision (via hash override) */
    printf("Collision tests:\n");
    test_collision_insert_and_retrieve();
    test_collision_delete_collapse();
    test_collision_replace();
    test_collision_nooverwrite();
    test_collision_three_entries();
    test_normal_hash_still_works();

    /* Arena sanity */
    printf("Arena sanity tests:\n");
    test_arena_pages_after_abort();
    test_arena_no_growth_across_aborts();

    teardown();

    printf("test_hamt passed!\n");
    return 0;
}
