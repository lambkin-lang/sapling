/*
 * hamt_concurrent_stress.c - concurrent reader/writer stress for HAMT
 *
 * One writer thread and multiple reader threads operate on a shared HAMT
 * subsystem backed by the B+ tree's transaction infrastructure (which
 * provides write_mutex serialization and MVCC snapshots).  This is the
 * production concurrency model.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#include "sapling/sapling.h"
#include "sapling/hamt.h"

#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef SAPLING_THREADED
int main(void)
{
    printf("hamt-concurrent-stress: SAPLING_THREADED required (skipped)\n");
    return 0;
}
#else

#include <pthread.h>
#include <time.h>

/* ================================================================== */
/* Config                                                               */
/* ================================================================== */

#define HAMT_STRESS_READERS 3u
#define HAMT_STRESS_KEYS    200u
#define HAMT_STRESS_ROUNDS  50u

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
/* Shared state                                                         */
/* ================================================================== */

/*
 * The arena allocator is not internally synchronized, and sap_arena_resolve
 * is unsafe during concurrent sap_arena_alloc_node (realloc can move the
 * backing array).  We use a plain mutex to serialize all transaction
 * lifecycles.  Threads still interleave at transaction boundaries, and
 * the COW paths get exercised across many rounds of put/del/get.
 */
#define STOP_LOAD(ss) __atomic_load_n(&(ss)->stop, __ATOMIC_RELAXED)
#define STOP_SET(ss)  __atomic_store_n(&(ss)->stop, 1, __ATOMIC_RELAXED)
#define ERR_LOAD(ss)  __atomic_load_n(&(ss)->error, __ATOMIC_RELAXED)
#define ERR_SET(ss)   do { __atomic_store_n(&(ss)->error, 1, __ATOMIC_RELAXED); STOP_SET(ss); } while (0)
typedef struct
{
    DB *db;
    pthread_mutex_t txn_mutex;
    int stop;  /* accessed via __atomic builtins */
    int error; /* set by any thread on hard failure */
    uint64_t reader_gets;
    uint64_t reader_found;
    uint64_t reader_not_found;
    uint64_t reader_txn_ok;
} SharedState;

/* ================================================================== */
/* Writer thread                                                        */
/* ================================================================== */

static void *writer_thread(void *arg)
{
    SharedState *ss = (SharedState *)arg;
    uint32_t round;

    for (round = 0; round < HAMT_STRESS_ROUNDS && !STOP_LOAD(ss); round++)
    {
        /* Insert keys */
        pthread_mutex_lock(&ss->txn_mutex);

        Txn *txn = txn_begin(ss->db, NULL, 0);
        if (!txn)
        {
            pthread_mutex_unlock(&ss->txn_mutex);
            fprintf(stderr, "hamt-stress: writer txn_begin failed round=%u\n", round);
            ERR_SET(ss);
            return NULL;
        }

        for (uint32_t i = 0; i < HAMT_STRESS_KEYS; i++)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "hk-%u-%u", round, i);
            uint32_t val = round * 10000 + i;
            int rc = sap_hamt_put((SapTxnCtx *)txn, key, (uint32_t)klen,
                                  &val, sizeof(val), 0);
            if (rc != ERR_OK)
            {
                fprintf(stderr, "hamt-stress: writer put failed rc=%d round=%u i=%u\n",
                        rc, round, i);
                txn_abort(txn);
                pthread_mutex_unlock(&ss->txn_mutex);
                ERR_SET(ss);
                return NULL;
            }
        }

        int rc = txn_commit(txn);
        pthread_mutex_unlock(&ss->txn_mutex);

        if (rc != ERR_OK)
        {
            fprintf(stderr, "hamt-stress: writer commit failed rc=%d round=%u\n", rc, round);
            ERR_SET(ss);
            return NULL;
        }

        /* Delete half the keys to exercise COW */
        pthread_mutex_lock(&ss->txn_mutex);

        txn = txn_begin(ss->db, NULL, 0);
        if (!txn)
        {
            pthread_mutex_unlock(&ss->txn_mutex);
            fprintf(stderr, "hamt-stress: writer del txn_begin failed round=%u\n", round);
            ERR_SET(ss);
            return NULL;
        }

        for (uint32_t i = 0; i < HAMT_STRESS_KEYS; i += 2)
        {
            char key[32];
            int klen = snprintf(key, sizeof(key), "hk-%u-%u", round, i);
            (void)sap_hamt_del((SapTxnCtx *)txn, key, (uint32_t)klen);
        }

        rc = txn_commit(txn);
        pthread_mutex_unlock(&ss->txn_mutex);

        if (rc != ERR_OK)
        {
            fprintf(stderr, "hamt-stress: writer del commit failed rc=%d round=%u\n", rc, round);
            ERR_SET(ss);
            return NULL;
        }
    }

    return NULL;
}

/* ================================================================== */
/* Reader thread                                                        */
/* ================================================================== */

static void *reader_thread(void *arg)
{
    SharedState *ss = (SharedState *)arg;

    while (!STOP_LOAD(ss))
    {
        pthread_mutex_lock(&ss->txn_mutex);

        Txn *txn = txn_begin(ss->db, NULL, TXN_RDONLY);
        if (!txn)
        {
            pthread_mutex_unlock(&ss->txn_mutex);
            /* Transient failure: writer might be blocking.  Retry. */
            struct timespec ts = {0, 100000L}; /* 0.1ms */
            nanosleep(&ts, NULL);
            continue;
        }

        __atomic_fetch_add(&ss->reader_txn_ok, 1, __ATOMIC_RELAXED);

        /* Read a sampling of keys from recent rounds */
        for (uint32_t i = 0; i < HAMT_STRESS_KEYS && !STOP_LOAD(ss); i += 5)
        {
            for (uint32_t r = 0; r < HAMT_STRESS_ROUNDS && r < 5; r++)
            {
                char key[32];
                int klen = snprintf(key, sizeof(key), "hk-%u-%u", r, i);
                const void *val_out = NULL;
                uint32_t val_len = 0;
                int rc = sap_hamt_get((SapTxnCtx *)txn, key, (uint32_t)klen,
                                      &val_out, &val_len);
                __atomic_fetch_add(&ss->reader_gets, 1, __ATOMIC_RELAXED);
                if (rc == ERR_OK)
                    __atomic_fetch_add(&ss->reader_found, 1, __ATOMIC_RELAXED);
                else if (rc == ERR_NOT_FOUND)
                    __atomic_fetch_add(&ss->reader_not_found, 1, __ATOMIC_RELAXED);
                else
                {
                    fprintf(stderr, "hamt-stress: reader get returned rc=%d\n", rc);
                    ERR_SET(ss);
                    txn_abort(txn);
                    pthread_mutex_unlock(&ss->txn_mutex);
                    return NULL;
                }
            }
        }

        txn_abort(txn);
        pthread_mutex_unlock(&ss->txn_mutex);
    }

    return NULL;
}

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
        fprintf(stderr, "hamt-concurrent-stress: arena init failed\n");
        return 1;
    }

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        fprintf(stderr, "hamt-concurrent-stress: db_open failed\n");
        return 1;
    }

    /* Register HAMT subsystem on the same environment */
    int rc = sap_hamt_subsystem_init((SapEnv *)db);
    if (rc != ERR_OK)
    {
        fprintf(stderr, "hamt-concurrent-stress: hamt init failed rc=%d\n", rc);
        return 1;
    }

    SharedState ss;
    memset(&ss, 0, sizeof(ss));
    ss.db = db;
    if (pthread_mutex_init(&ss.txn_mutex, NULL) != 0)
    {
        fprintf(stderr, "hamt-concurrent-stress: mutex init failed\n");
        return 1;
    }

    /* Start reader threads */
    pthread_t readers[HAMT_STRESS_READERS];
    for (uint32_t i = 0; i < HAMT_STRESS_READERS; i++)
    {
        if (pthread_create(&readers[i], NULL, reader_thread, &ss) != 0)
        {
            fprintf(stderr, "hamt-concurrent-stress: reader thread create failed\n");
            return 1;
        }
    }

    /* Start writer thread */
    pthread_t writer;
    if (pthread_create(&writer, NULL, writer_thread, &ss) != 0)
    {
        fprintf(stderr, "hamt-concurrent-stress: writer thread create failed\n");
        return 1;
    }

    /* Wait for writer to finish */
    pthread_join(writer, NULL);

    /* Signal readers to stop */
    __atomic_store_n(&ss.stop, 1, __ATOMIC_RELAXED);

    /* Wait for readers */
    for (uint32_t i = 0; i < HAMT_STRESS_READERS; i++)
        pthread_join(readers[i], NULL);

    printf("hamt-concurrent-stress: writer_rounds=%u keys_per_round=%u\n",
           HAMT_STRESS_ROUNDS, HAMT_STRESS_KEYS);
    printf("  readers=%u reader_txns=%" PRIu64 " gets=%" PRIu64
           " found=%" PRIu64 " not_found=%" PRIu64 "\n",
           HAMT_STRESS_READERS,
           ss.reader_txn_ok, ss.reader_gets,
           ss.reader_found, ss.reader_not_found);

    /* Corruption telemetry check */
    {
        SapCorruptionStats cstats;
        if (sap_db_corruption_stats((struct SapEnv *)db, &cstats) == ERR_OK)
        {
            uint64_t total = cstats.free_list_head_reset +
                             cstats.free_list_next_dropped +
                             cstats.leaf_insert_bounds_reject +
                             cstats.abort_loop_limit_hit +
                             cstats.abort_bounds_break;
            printf("  corruption_stats: total=%" PRIu64 "\n", total);
            if (total > 0)
            {
                fprintf(stderr, "hamt-concurrent-stress: CORRUPTION detected total=%" PRIu64 "\n",
                        total);
                db_close(db);
                sap_arena_destroy(arena);
                return 1;
            }
        }
    }

    /* Free-list integrity check */
    {
        SapFreelistCheckResult fl;
        if (sap_db_freelist_check((struct SapEnv *)db, &fl) == ERR_OK)
        {
            printf("  freelist: walk=%u oob=%u null=%u cycle=%u\n",
                   fl.walk_length, fl.out_of_bounds, fl.null_backing, fl.cycle_detected);
            if (fl.out_of_bounds || fl.null_backing || fl.cycle_detected)
            {
                fprintf(stderr, "hamt-concurrent-stress: FREE-LIST FAILURE\n");
                db_close(db);
                sap_arena_destroy(arena);
                return 1;
            }
        }
    }

    pthread_mutex_destroy(&ss.txn_mutex);
    db_close(db);
    sap_arena_destroy(arena);

    if (__atomic_load_n(&ss.error, __ATOMIC_RELAXED))
    {
        fprintf(stderr, "hamt-concurrent-stress: FAILED (thread error)\n");
        return 1;
    }

    if (ss.reader_gets == 0)
    {
        fprintf(stderr, "hamt-concurrent-stress: FAILED (no reader progress)\n");
        return 1;
    }

    printf("hamt-concurrent-stress: PASSED\n");
    return 0;
}

#endif /* SAPLING_THREADED */
