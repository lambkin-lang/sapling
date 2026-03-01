/*
 * test_freelist_check.c - unit tests for free-list integrity checker
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

static int test_fresh_db_clean(void)
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

    SapFreelistCheckResult r;
    int rc = sap_db_freelist_check((struct SapEnv *)db, &r);
    ASSERT(rc == ERR_OK);
    ASSERT(r.walk_length == 0);
    ASSERT(r.out_of_bounds == 0);
    ASSERT(r.null_backing == 0);
    ASSERT(r.cycle_detected == 0);
    ASSERT(r.deferred_count == 0);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

static int test_after_put_del_cycles(void)
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

    /* Populate then delete to build up a free-list */
    for (int round = 0; round < 5; round++)
    {
        Txn *txn = txn_begin(db, NULL, 0);
        ASSERT(txn != NULL);
        for (int i = 0; i < 200; i++)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "key-%04d", i);
            uint32_t val = (uint32_t)(round * 1000 + i);
            ASSERT(txn_put(txn, key, (uint32_t)klen, &val, sizeof(val)) == ERR_OK);
        }
        ASSERT(txn_commit(txn) == ERR_OK);

        txn = txn_begin(db, NULL, 0);
        ASSERT(txn != NULL);
        for (int i = 0; i < 200; i += 3)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "key-%04d", i);
            (void)txn_del(txn, key, (uint32_t)klen);
        }
        ASSERT(txn_commit(txn) == ERR_OK);

        /* Check integrity after each round */
        SapFreelistCheckResult r;
        int rc = sap_db_freelist_check((struct SapEnv *)db, &r);
        ASSERT(rc == ERR_OK);
        ASSERT(r.out_of_bounds == 0);
        ASSERT(r.null_backing == 0);
        ASSERT(r.cycle_detected == 0);
    }

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

static int test_busy_with_active_writer(void)
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

    Txn *txn = txn_begin(db, NULL, 0);
    ASSERT(txn != NULL);

    /* Should return ERR_BUSY while write txn is active */
    SapFreelistCheckResult r;
    int rc = sap_db_freelist_check((struct SapEnv *)db, &r);
    ASSERT(rc == ERR_BUSY);

    txn_abort(txn);

    /* Now it should work */
    rc = sap_db_freelist_check((struct SapEnv *)db, &r);
    ASSERT(rc == ERR_OK);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

static int test_null_args(void)
{
    SapFreelistCheckResult r;
    ASSERT(sap_db_freelist_check(NULL, &r) == ERR_INVALID);

    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL};
    sap_arena_init(&arena, &opts);

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    ASSERT(db != NULL);

    ASSERT(sap_db_freelist_check((struct SapEnv *)db, NULL) == ERR_INVALID);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

static int test_heavy_churn_integrity(void)
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

    /* Rapid insert-delete-reinsert churn to stress the free-list */
    for (int round = 0; round < 20; round++)
    {
        Txn *txn = txn_begin(db, NULL, 0);
        ASSERT(txn != NULL);
        for (int i = 0; i < 100; i++)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "churn-%d-%d", round, i);
            ASSERT(txn_put(txn, key, (uint32_t)klen, "v", 1) == ERR_OK);
        }
        ASSERT(txn_commit(txn) == ERR_OK);

        txn = txn_begin(db, NULL, 0);
        ASSERT(txn != NULL);
        for (int i = 0; i < 100; i++)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "churn-%d-%d", round, i);
            (void)txn_del(txn, key, (uint32_t)klen);
        }
        ASSERT(txn_commit(txn) == ERR_OK);
    }

    SapFreelistCheckResult r;
    int rc = sap_db_freelist_check((struct SapEnv *)db, &r);
    ASSERT(rc == ERR_OK);
    ASSERT(r.out_of_bounds == 0);
    ASSERT(r.null_backing == 0);
    ASSERT(r.cycle_detected == 0);
    /* After heavy churn, free-list should have nodes */
    ASSERT(r.walk_length > 0);

    /* Also verify corruption stats are clean */
    SapCorruptionStats cs;
    ASSERT(sap_db_corruption_stats((struct SapEnv *)db, &cs) == ERR_OK);
    ASSERT(cs.free_list_head_reset == 0);
    ASSERT(cs.free_list_next_dropped == 0);

    db_close(db);
    sap_arena_destroy(arena);
    return 0;
}

/* ================================================================== */
/* Main                                                                 */
/* ================================================================== */

int main(void)
{
    printf("--- free-list integrity checker tests ---\n");
    RUN(test_fresh_db_clean);
    RUN(test_after_put_del_cycles);
    RUN(test_busy_with_active_writer);
    RUN(test_null_args);
    RUN(test_heavy_churn_integrity);

    printf("\nResults: %d passed, 0 failed\n", passed);
    return 0;
}
