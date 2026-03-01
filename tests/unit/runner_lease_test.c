/*
 * runner_lease_test.c - tests for general lease management (DBI 3)
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/lease_v0.h"
#include "generated/wit_schema_dbis.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define CHECK(cond)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (!(cond))                                                                               \
        {                                                                                          \
            fprintf(stderr, "CHECK failed at %s:%d\n", __FILE__, __LINE__);                        \
            return __LINE__;                                                                       \
        }                                                                                          \
    } while (0)

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

static SapMemArena *g_alloc = NULL;

static DB *new_db(void) { return db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL); }

static int test_lease_logic(void)
{
    DB *db = new_db();
    SapRunnerTxStackV0 stack;
    Txn *read_txn;
    SapRunnerLeaseV0 lease = {0};
    const char *key = "res-1";
    uint32_t klen = (uint32_t)strlen(key);
    int rc;

    CHECK(db != NULL);
    CHECK(dbi_open(db, SAP_WIT_DBI_LEASES, NULL, NULL, 0u) == ERR_OK);

    sap_runner_txstack_v0_init(&stack);
    read_txn = txn_begin(db, NULL, TXN_RDONLY);

    // 1. Acquire new lease
    CHECK(sap_runner_txstack_v0_push(&stack) == ERR_OK);
    rc = sap_runner_lease_v0_stage_acquire(&stack, read_txn, key, klen, 123u, 1000, 5000, &lease);
    CHECK(rc == ERR_OK);
    CHECK(lease.owner_worker == 123u);
    CHECK(lease.deadline_ts == 6000);

    // Commit the lease
    {
        Txn *wtxn = txn_begin(db, NULL, 0u);
        CHECK(sap_runner_txstack_v0_apply_root_writes(&stack, wtxn) == ERR_OK);
        CHECK(txn_commit(wtxn) == ERR_OK);
    }
    sap_runner_txstack_v0_reset(&stack);
    txn_abort(read_txn);

    // 2. Try to acquire by different worker (should fail)
    read_txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(sap_runner_txstack_v0_push(&stack) == ERR_OK);
    rc = sap_runner_lease_v0_stage_acquire(&stack, read_txn, key, klen, 456u, 2000, 5000, &lease);
    CHECK(rc == ERR_BUSY);
    sap_runner_txstack_v0_reset(&stack);
    txn_abort(read_txn);

    // 3. Acquire after expiration
    read_txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(sap_runner_txstack_v0_push(&stack) == ERR_OK);
    rc = sap_runner_lease_v0_stage_acquire(&stack, read_txn, key, klen, 456u, 7000, 5000, &lease);
    CHECK(rc == ERR_OK);
    CHECK(lease.owner_worker == 456u);
    CHECK(lease.deadline_ts == 12000);
    CHECK(lease.attempts == 2u);

    sap_runner_txstack_v0_dispose(&stack);
    db_close(db);
    return 0;
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

    if (test_lease_logic() != 0)
    {
        return 1;
    }
    printf("runner_lease_test PASS\n");
    return 0;
}
