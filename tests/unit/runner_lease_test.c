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
#include <limits.h>
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

static uint64_t rd64le(const uint8_t *p)
{
    uint64_t v = 0u;
    uint32_t i;
    for (i = 0u; i < 8u; i++)
    {
        v |= ((uint64_t)p[i]) << (8u * i);
    }
    return v;
}

static void wr64le(uint8_t *p, uint64_t v)
{
    uint32_t i;
    for (i = 0u; i < 8u; i++)
    {
        p[i] = (uint8_t)((v >> (8u * i)) & 0xffu);
    }
}

static int apply_root_writes(DB *db, SapRunnerTxStackV0 *stack)
{
    Txn *wtxn = txn_begin(db, NULL, 0u);
    int rc;

    CHECK(wtxn != NULL);
    rc = sap_runner_txstack_v0_apply_root_writes(stack, wtxn);
    if (rc != ERR_OK)
    {
        txn_abort(wtxn);
        return rc;
    }
    return txn_commit(wtxn);
}

static int test_lease_acquire_busy_and_expire(void)
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

    CHECK(apply_root_writes(db, &stack) == ERR_OK);
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

static int test_lease_same_owner_renew_and_release(void)
{
    DB *db = new_db();
    SapRunnerTxStackV0 stack;
    Txn *read_txn;
    SapRunnerLeaseV0 lease = {0};
    const char *key = "res-2";
    uint32_t klen = (uint32_t)strlen(key);
    int rc;
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(dbi_open(db, SAP_WIT_DBI_LEASES, NULL, NULL, 0u) == ERR_OK);

    sap_runner_txstack_v0_init(&stack);

    /* Acquire as owner 10. */
    read_txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(read_txn != NULL);
    CHECK(sap_runner_txstack_v0_push(&stack) == ERR_OK);
    rc = sap_runner_lease_v0_stage_acquire(&stack, read_txn, key, klen, 10u, 1000, 5000, &lease);
    CHECK(rc == ERR_OK);
    CHECK(lease.owner_worker == 10u);
    CHECK(lease.deadline_ts == 6000);
    CHECK(lease.attempts == 1u);
    CHECK(apply_root_writes(db, &stack) == ERR_OK);
    sap_runner_txstack_v0_reset(&stack);
    txn_abort(read_txn);

    /* Re-acquire by same owner before expiry should succeed and bump attempts. */
    read_txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(read_txn != NULL);
    CHECK(sap_runner_txstack_v0_push(&stack) == ERR_OK);
    rc = sap_runner_lease_v0_stage_acquire(&stack, read_txn, key, klen, 10u, 2000, 3000, &lease);
    CHECK(rc == ERR_OK);
    CHECK(lease.owner_worker == 10u);
    CHECK(lease.deadline_ts == 5000);
    CHECK(lease.attempts == 2u);
    CHECK(apply_root_writes(db, &stack) == ERR_OK);
    sap_runner_txstack_v0_reset(&stack);
    txn_abort(read_txn);

    /* Release by wrong owner should fail with conflict. */
    read_txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(read_txn != NULL);
    CHECK(sap_runner_txstack_v0_push(&stack) == ERR_OK);
    rc = sap_runner_lease_v0_stage_release(&stack, read_txn, key, klen, 99u);
    CHECK(rc == ERR_CONFLICT);
    sap_runner_txstack_v0_reset(&stack);
    txn_abort(read_txn);

    /* Release by correct owner should delete row. */
    read_txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(read_txn != NULL);
    CHECK(sap_runner_txstack_v0_push(&stack) == ERR_OK);
    rc = sap_runner_lease_v0_stage_release(&stack, read_txn, key, klen, 10u);
    CHECK(rc == ERR_OK);
    CHECK(apply_root_writes(db, &stack) == ERR_OK);
    sap_runner_txstack_v0_reset(&stack);
    txn_abort(read_txn);

    read_txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(read_txn != NULL);
    rc = txn_get_dbi(read_txn, SAP_WIT_DBI_LEASES, key, klen, &val, &val_len);
    CHECK(rc == ERR_NOT_FOUND);
    txn_abort(read_txn);

    sap_runner_txstack_v0_dispose(&stack);
    db_close(db);
    return 0;
}

static int test_lease_decode_guardrails(void)
{
    SapRunnerLeaseV0 in = {0};
    SapRunnerLeaseV0 out = {0};
    uint8_t raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
    uint64_t attempts;

    in.owner_worker = 7u;
    in.deadline_ts = 1234;
    in.attempts = 4u;
    sap_runner_lease_v0_encode(&in, raw);
    CHECK(sap_runner_lease_v0_decode(raw, sizeof(raw), &out) == ERR_OK);
    CHECK(out.owner_worker == in.owner_worker);
    CHECK(out.deadline_ts == in.deadline_ts);
    CHECK(out.attempts == in.attempts);

    raw[10] = SAP_WIT_LEASE_STATE_DONE;
    CHECK(sap_runner_lease_v0_decode(raw, sizeof(raw), &out) == ERR_CORRUPT);
    raw[10] = SAP_WIT_LEASE_STATE_LEASED;

    raw[0] = SAP_WIT_TAG_VARIANT;
    CHECK(sap_runner_lease_v0_decode(raw, sizeof(raw), &out) == ERR_CORRUPT);
    raw[0] = SAP_WIT_TAG_RECORD;

    attempts = (uint64_t)UINT32_MAX + 1u;
    wr64le(raw + 35, attempts);
    CHECK(sap_runner_lease_v0_decode(raw, sizeof(raw), &out) == ERR_CORRUPT);
    wr64le(raw + 35, 4u);

    CHECK(sap_runner_lease_v0_decode(raw, sizeof(raw) - 1u, &out) == ERR_INVALID);
    CHECK(rd64le(raw + 35) == 4u);
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

    if (test_lease_acquire_busy_and_expire() != 0)
    {
        return 1;
    }
    if (test_lease_same_owner_renew_and_release() != 0)
    {
        return 1;
    }
    if (test_lease_decode_guardrails() != 0)
    {
        return 1;
    }
    printf("runner_lease_test PASS\n");
    return 0;
}
