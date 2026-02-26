/*
 * test_runner_ttl_sweep.c - tests for background TTL sweeping via the runner worker
 *
 * SPDX-License-Identifier: MIT
 */
#include "sapling/sapling.h"
#include "runner/runner_v0.h"
#include "generated/wit_schema_dbis.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Minimal memory allocator context */
static void *test_alloc(void *ctx, uint32_t sz)
{
    (void)ctx;
    return malloc((size_t)sz);
}

static void test_free(void *ctx, void *ptr, uint32_t sz)
{
    (void)ctx;
    (void)sz;
    free(ptr);
}

static SapMemArena *g_alloc = NULL;

static DB *new_db(void)
{
    DB *db = db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    assert(db != NULL);
    return db;
}

static int g_fail = 0;
#define CHECK(cond)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (!(cond))                                                                               \
        {                                                                                          \
            fprintf(stderr, "FAIL: %s at %s:%d\n", #cond, __FILE__, __LINE__);                     \
            g_fail++;                                                                              \
        }                                                                                          \
    } while (0)

#define SECTION(msg)                                                                               \
    do                                                                                             \
    {                                                                                              \
        printf("--- %s ---\n", msg);                                                               \
    } while (0)

static int64_t g_mock_time_ms = 10000;

static int64_t mock_now_ms(void *ctx)
{
    (void)ctx;
    return g_mock_time_ms;
}

static int mock_handler(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg, void *ctx)
{
    (void)runner;
    (void)msg;
    (void)ctx;
    return SAP_OK;
}

static void test_runner_ttl_sweep(void)
{
    SECTION("runner background TTL sweeping");

    DB *db = new_db();
    /* DB 0-9 = runner logic, DB 10 = data, DB 11 = ttl */
    CHECK(sap_runner_v0_bootstrap_dbis(db) == SAP_OK);
    CHECK(dbi_open(db, 10, NULL, NULL, 0) == SAP_OK);
    CHECK(dbi_open(db, 11, NULL, NULL, DBI_TTL_META) == SAP_OK);

    SapRunnerV0Config cfg = {0};
    cfg.db = db;
    cfg.worker_id = 99;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    SapRunnerV0Worker worker;
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, mock_handler, NULL, 10) == SAP_OK);

    SapRunnerV0Policy policy;
    sap_runner_v0_policy_default(&policy);
    policy.ttl_sweep_cadence_ms = 5000; /* Sweep every 5 seconds */
    policy.ttl_sweep_max_batch = 100;
    sap_runner_v0_worker_set_policy(&worker, &policy);
    sap_runner_v0_worker_set_time_hooks(&worker, mock_now_ms, NULL, NULL, NULL);

    /* Register the TTL pair */
    CHECK(sap_runner_v0_worker_register_ttl_pair(&worker, 10, 11) == SAP_OK);

    /* Insert some keys that will expire at t=12000 and t=16000 */
    Txn *w = txn_begin(db, NULL, 0);
    CHECK(w != NULL);
    CHECK(txn_put_ttl_dbi(w, 10, 11, "A", 1, "VA", 2, 12000) == SAP_OK);
    CHECK(txn_put_ttl_dbi(w, 10, 11, "B", 1, "VB", 2, 16000) == SAP_OK);
    CHECK(txn_put_ttl_dbi(w, 10, 11, "C", 1, "VC", 2, 20000) ==
          SAP_OK); /* Doesn't expire until much later */
    CHECK(txn_commit(w) == SAP_OK);

    /* Tick the worker. Time is 10000. It will record the initial sweep time. */
    uint32_t processed = 0;
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(worker.runner.metrics.ttl_sweeps_run == 0);

    /* Advance time to 12000. 2000ms elapsed. policy cadence is 5000ms so no sweep. */
    g_mock_time_ms = 12000;
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(worker.runner.metrics.ttl_sweeps_run == 0);

    /* Advance time to 15000. 5000ms elapsed. A sweep happens. */
    g_mock_time_ms = 15000;
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(worker.runner.metrics.ttl_sweeps_run == 1);
    CHECK(worker.runner.metrics.ttl_expired_entries_deleted == 1); /* "A" expired */

    /* Verify A is gone, B and C remain */
    Txn *r = txn_begin(db, NULL, TXN_RDONLY);
    const void *v;
    uint32_t vl;
    CHECK(txn_get_dbi(r, 10, "A", 1, &v, &vl) == SAP_NOTFOUND);
    CHECK(txn_get_dbi(r, 10, "B", 1, &v, &vl) == SAP_OK);
    CHECK(txn_get_dbi(r, 10, "C", 1, &v, &vl) == SAP_OK);
    txn_abort(r);

    /* Advance time to 19000. No sweep. */
    g_mock_time_ms = 19000;
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(worker.runner.metrics.ttl_sweeps_run == 1);

    /* Advance time to 21000. Sweep happens. B and C expire. */
    g_mock_time_ms = 21000;
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(worker.runner.metrics.ttl_sweeps_run == 2);
    CHECK(worker.runner.metrics.ttl_expired_entries_deleted ==
          3); /* 1 from before + 2 now = 3 total */

    r = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(txn_get_dbi(r, 10, "A", 1, &v, &vl) == SAP_NOTFOUND);
    CHECK(txn_get_dbi(r, 10, "B", 1, &v, &vl) == SAP_NOTFOUND);
    CHECK(txn_get_dbi(r, 10, "C", 1, &v, &vl) == SAP_NOTFOUND);
    txn_abort(r);

    sap_runner_v0_worker_shutdown(&worker);
    db_close(db);
}

int main(void)
{
    SapArenaOptions g_alloc_opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL
    };
    sap_arena_init(&g_alloc, &g_alloc_opts);

    test_runner_ttl_sweep();
    if (g_fail > 0)
    {
        printf("Tests failed: %d\n", g_fail);
        return 1;
    }
    printf("All sweeping tests passed.\n");
    return 0;
}
