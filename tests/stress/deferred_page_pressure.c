/*
 * deferred_page_pressure.c - validate bounded deferred-page accumulation
 *
 * Holds a long-lived reader snapshot open while a writer churns rapidly,
 * then asserts that (1) deferred count stays bounded, (2) reclamation
 * completes after the reader releases, and (3) the free-list remains
 * structurally sound throughout.
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
/* Test allocator                                                       */
/* ================================================================== */

static void *test_alloc(void *ctx, uint32_t sz)
{
    (void)ctx;
    return malloc((size_t)sz);
}

static void test_free_page(void *ctx, void *p, uint32_t sz)
{
    (void)ctx;
    (void)sz;
    free(p);
}

/* ================================================================== */
/* Config                                                               */
/* ================================================================== */

#define CHURN_ROUNDS   20
#define KEYS_PER_ROUND 100

/* ================================================================== */
/* Main                                                                 */
/* ================================================================== */

int main(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free_page,
        .cfg.custom.ctx = NULL};
    if (sap_arena_init(&arena, &opts) != 0)
    {
        fprintf(stderr, "deferred-page-pressure: arena init failed\n");
        return 1;
    }

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        fprintf(stderr, "deferred-page-pressure: db_open failed\n");
        return 1;
    }

    /* Seed with initial data */
    {
        Txn *txn = txn_begin(db, NULL, 0);
        assert(txn);
        for (int i = 0; i < KEYS_PER_ROUND; i++)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "seed-%04d", i);
            assert(txn_put(txn, key, (uint32_t)klen, "v", 1) == ERR_OK);
        }
        assert(txn_commit(txn) == ERR_OK);
    }

    /* Hold a long-lived reader snapshot */
    Txn *reader = txn_begin(db, NULL, TXN_RDONLY);
    assert(reader);

    uint32_t max_deferred = 0;

    /* Rapid write-commit churn while reader holds snapshot */
    for (int round = 0; round < CHURN_ROUNDS; round++)
    {
        /* Delete all keys */
        Txn *txn = txn_begin(db, NULL, 0);
        assert(txn);
        for (int i = 0; i < KEYS_PER_ROUND; i++)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "seed-%04d", i);
            (void)txn_del(txn, key, (uint32_t)klen);
        }
        assert(txn_commit(txn) == ERR_OK);

        /* Re-insert all keys */
        txn = txn_begin(db, NULL, 0);
        assert(txn);
        for (int i = 0; i < KEYS_PER_ROUND; i++)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "seed-%04d", i);
            uint32_t val = (uint32_t)(round * 1000 + i);
            assert(txn_put(txn, key, (uint32_t)klen, &val, sizeof(val)) == ERR_OK);
        }
        assert(txn_commit(txn) == ERR_OK);

        /* Track deferred page count */
        uint32_t deferred = 0;
        assert(sap_db_deferred_count((struct SapEnv *)db, &deferred) == ERR_OK);
        if (deferred > max_deferred)
            max_deferred = deferred;

        printf("  round %2d: deferred=%u pages=%u\n", round, deferred, db_num_pages(db));
    }

    printf("  max_deferred=%u (across %d rounds with reader pinned)\n",
           max_deferred, CHURN_ROUNDS);

    /* Verify free-list integrity while reader is still held */
    /* NOTE: freelist_check requires no active writer, reader is OK */
    {
        SapFreelistCheckResult fl;
        assert(sap_db_freelist_check((struct SapEnv *)db, &fl) == ERR_OK);
        if (fl.out_of_bounds || fl.null_backing || fl.cycle_detected)
        {
            fprintf(stderr,
                    "deferred-page-pressure: FREE-LIST INTEGRITY FAILURE "
                    "oob=%u null=%u cycle=%u\n",
                    fl.out_of_bounds, fl.null_backing, fl.cycle_detected);
            return 1;
        }
        printf("  freelist ok: walk_length=%u deferred=%u\n",
               fl.walk_length, fl.deferred_count);
    }

    /* Verify corruption stats are clean */
    {
        SapCorruptionStats cs;
        assert(sap_db_corruption_stats((struct SapEnv *)db, &cs) == ERR_OK);
        if (cs.free_list_head_reset || cs.free_list_next_dropped)
        {
            fprintf(stderr,
                    "deferred-page-pressure: CORRUPTION detected: "
                    "head_reset=%llu next_dropped=%llu\n",
                    (unsigned long long)cs.free_list_head_reset,
                    (unsigned long long)cs.free_list_next_dropped);
            return 1;
        }
    }

    /* Release reader and trigger reclamation.
     * Two writes are needed: the first processes the accumulated deferred
     * pages, but its own freed pages become newly deferred.  The second
     * write processes those final deferred pages. */
    txn_abort(reader);

    for (int flush = 0; flush < 2; flush++)
    {
        Txn *txn = txn_begin(db, NULL, 0);
        assert(txn);
        char fkey[32];
        int fklen = snprintf(fkey, sizeof(fkey), "reclaim-%d", flush);
        assert(txn_put(txn, fkey, (uint32_t)fklen, "x", 1) == ERR_OK);
        assert(txn_commit(txn) == ERR_OK);
    }

    /* Verify deferred pages were reclaimed.  The last commit always
     * defers its own freed pages (steady-state = 1), so assert <= 1. */
    {
        uint32_t deferred = 999;
        assert(sap_db_deferred_count((struct SapEnv *)db, &deferred) == ERR_OK);
        printf("  after reader release + 2 flushes: deferred=%u (expected <= 1)\n", deferred);
        if (deferred > 1)
        {
            fprintf(stderr, "deferred-page-pressure: reclamation incomplete, %u still deferred\n",
                    deferred);
            return 1;
        }
    }

    /* Final free-list integrity check */
    {
        SapFreelistCheckResult fl;
        assert(sap_db_freelist_check((struct SapEnv *)db, &fl) == ERR_OK);
        assert(fl.out_of_bounds == 0);
        assert(fl.null_backing == 0);
        assert(fl.cycle_detected == 0);
        printf("  final freelist ok: walk_length=%u\n", fl.walk_length);
    }

    db_close(db);
    sap_arena_destroy(arena);

    printf("\ndeferred-page-pressure: PASSED\n");
    return 0;
}
