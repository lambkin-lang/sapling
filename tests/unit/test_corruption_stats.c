/*
 * test_corruption_stats.c - unit tests for corruption telemetry counters
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#include "sapling/sapling.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================== */
/* Minimal test allocator                                               */
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

static int passed = 0;

#define ASSERT(cond)                                                          \
    do                                                                        \
    {                                                                         \
        if (!(cond))                                                          \
        {                                                                     \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
            return 1;                                                         \
        }                                                                     \
    } while (0)

#define RUN(fn)                                     \
    do                                              \
    {                                               \
        printf("  %-50s", #fn "...");               \
        if (fn() == 0)                              \
        {                                           \
            printf("ok\n");                         \
            passed++;                               \
        }                                           \
        else                                        \
        {                                           \
            printf("FAILED\n");                     \
            return 1;                               \
        }                                           \
    } while (0)

/* ================================================================== */
/* Tests                                                                */
/* ================================================================== */

static int test_fresh_db_has_zero_stats(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL};
    sap_arena_init(&arena, &opts);

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    ASSERT(db != NULL);

    SapCorruptionStats stats;
    int rc = sap_db_corruption_stats((struct SapEnv *)db, &stats);
    ASSERT(rc == ERR_OK);
    ASSERT(stats.free_list_head_reset == 0);
    ASSERT(stats.free_list_next_dropped == 0);
    ASSERT(stats.leaf_insert_bounds_reject == 0);
    ASSERT(stats.abort_loop_limit_hit == 0);
    ASSERT(stats.abort_bounds_break == 0);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

static int test_normal_ops_keep_stats_zero(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL};
    sap_arena_init(&arena, &opts);

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    ASSERT(db != NULL);

    /* Insert and delete many keys to exercise allocator and leaf paths */
    for (int round = 0; round < 3; round++)
    {
        Txn *txn = txn_begin(db, NULL, 0);
        ASSERT(txn != NULL);
        for (int i = 0; i < 500; i++)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "key-%04d", i);
            uint32_t val = (uint32_t)i;
            ASSERT(txn_put(txn, key, (uint32_t)klen, &val, sizeof(val)) == ERR_OK);
        }
        ASSERT(txn_commit(txn) == ERR_OK);

        /* Delete half the keys to exercise free-list */
        txn = txn_begin(db, NULL, 0);
        ASSERT(txn != NULL);
        for (int i = 0; i < 500; i += 2)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "key-%04d", i);
            ASSERT(txn_del(txn, key, (uint32_t)klen) == ERR_OK);
        }
        ASSERT(txn_commit(txn) == ERR_OK);
    }

    SapCorruptionStats stats;
    int rc = sap_db_corruption_stats((struct SapEnv *)db, &stats);
    ASSERT(rc == ERR_OK);
    ASSERT(stats.free_list_head_reset == 0);
    ASSERT(stats.free_list_next_dropped == 0);
    ASSERT(stats.leaf_insert_bounds_reject == 0);
    ASSERT(stats.abort_loop_limit_hit == 0);
    ASSERT(stats.abort_bounds_break == 0);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

static int test_stats_reset(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL};
    sap_arena_init(&arena, &opts);

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    ASSERT(db != NULL);

    /* Do some ops to get past initialization */
    Txn *txn = txn_begin(db, NULL, 0);
    ASSERT(txn != NULL);
    ASSERT(txn_put(txn, "a", 1, "b", 1) == ERR_OK);
    ASSERT(txn_commit(txn) == ERR_OK);

    /* Reset and verify */
    int rc = sap_db_corruption_stats_reset((struct SapEnv *)db);
    ASSERT(rc == ERR_OK);

    SapCorruptionStats stats;
    rc = sap_db_corruption_stats((struct SapEnv *)db, &stats);
    ASSERT(rc == ERR_OK);
    ASSERT(stats.free_list_head_reset == 0);
    ASSERT(stats.free_list_next_dropped == 0);
    ASSERT(stats.leaf_insert_bounds_reject == 0);
    ASSERT(stats.abort_loop_limit_hit == 0);
    ASSERT(stats.abort_bounds_break == 0);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

static int test_null_args(void)
{
    SapCorruptionStats stats;
    ASSERT(sap_db_corruption_stats(NULL, &stats) == ERR_INVALID);
    ASSERT(sap_db_corruption_stats_reset(NULL) == ERR_INVALID);

    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL};
    sap_arena_init(&arena, &opts);

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    ASSERT(db != NULL);

    ASSERT(sap_db_corruption_stats((struct SapEnv *)db, NULL) == ERR_INVALID);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

static int test_deferred_count_fresh(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL};
    sap_arena_init(&arena, &opts);

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    ASSERT(db != NULL);

    uint32_t count = 999;
    int rc = sap_db_deferred_count((struct SapEnv *)db, &count);
    ASSERT(rc == ERR_OK);
    ASSERT(count == 0);

    ASSERT(sap_db_deferred_count(NULL, &count) == ERR_INVALID);
    ASSERT(sap_db_deferred_count((struct SapEnv *)db, NULL) == ERR_INVALID);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

static int test_deferred_count_with_reader(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL};
    sap_arena_init(&arena, &opts);

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    ASSERT(db != NULL);

    /* Insert some data */
    Txn *txn = txn_begin(db, NULL, 0);
    ASSERT(txn != NULL);
    for (int i = 0; i < 100; i++)
    {
        char key[16];
        int klen = snprintf(key, sizeof(key), "k%04d", i);
        ASSERT(txn_put(txn, key, (uint32_t)klen, "v", 1) == ERR_OK);
    }
    ASSERT(txn_commit(txn) == ERR_OK);

    /* Hold a reader snapshot open */
    Txn *reader = txn_begin(db, NULL, TXN_RDONLY);
    ASSERT(reader != NULL);

    /* Delete data — pages go to deferred since reader holds snapshot */
    txn = txn_begin(db, NULL, 0);
    ASSERT(txn != NULL);
    for (int i = 0; i < 100; i++)
    {
        char key[16];
        int klen = snprintf(key, sizeof(key), "k%04d", i);
        ASSERT(txn_del(txn, key, (uint32_t)klen) == ERR_OK);
    }
    ASSERT(txn_commit(txn) == ERR_OK);

    /* Deferred count should be nonzero since reader is still active */
    uint32_t count = 0;
    ASSERT(sap_db_deferred_count((struct SapEnv *)db, &count) == ERR_OK);
    ASSERT(count > 0);

    /* Release reader and trigger reclamation with a new write */
    txn_abort(reader);

    txn = txn_begin(db, NULL, 0);
    ASSERT(txn != NULL);
    ASSERT(txn_put(txn, "reclaim", 7, "x", 1) == ERR_OK);
    ASSERT(txn_commit(txn) == ERR_OK);

    /* Deferred count should be zero now */
    ASSERT(sap_db_deferred_count((struct SapEnv *)db, &count) == ERR_OK);
    ASSERT(count == 0);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

/* ================================================================== */
/* Main                                                                 */
/* ================================================================== */

int main(void)
{
    printf("--- corruption stats tests ---\n");
    RUN(test_fresh_db_has_zero_stats);
    RUN(test_normal_ops_keep_stats_zero);
    RUN(test_stats_reset);
    RUN(test_null_args);

    printf("\n--- deferred count tests ---\n");
    RUN(test_deferred_count_fresh);
    RUN(test_deferred_count_with_reader);

    printf("\nResults: %d passed, 0 failed\n", passed);
    return 0;
}
