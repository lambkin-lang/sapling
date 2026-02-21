/*
 * runner_txstack_test.c - tests for nested tx stack scaffolding
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/txstack_v0.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(cond)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (!(cond))                                                                               \
        {                                                                                          \
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

static PageAllocator g_alloc = {test_alloc, test_free, NULL};

static DB *new_db(void) { return db_open(&g_alloc, SAPLING_PAGE_SIZE, NULL, NULL); }

static int db_put(DB *db, const void *key, uint32_t key_len, const void *val, uint32_t val_len)
{
    Txn *txn;
    int rc;

    if (!db)
    {
        return SAP_ERROR;
    }
    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_ERROR;
    }
    rc = txn_put_dbi(txn, 0u, key, key_len, val, val_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    return txn_commit(txn);
}

static int db_get(DB *db, const void *key, uint32_t key_len, const void **val_out,
                  uint32_t *val_len_out)
{
    Txn *txn;
    int rc;

    if (!db || !val_out || !val_len_out)
    {
        return SAP_ERROR;
    }
    *val_out = NULL;
    *val_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    rc = txn_get_dbi(txn, 0u, key, key_len, val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

static int test_nested_commit_merges_into_parent(void)
{
    DB *db = new_db();
    SapRunnerTxStackV0 stack;
    Txn *rtxn;
    Txn *wtxn;
    SapRunnerIntentV0 intent = {0};
    const uint8_t intent_msg[] = {'e', 'v', 't'};
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(db_put(db, "a", 1u, "db", 2u) == SAP_OK);
    CHECK(sap_runner_txstack_v0_init(&stack) == SAP_OK);
    CHECK(sap_runner_txstack_v0_push(&stack) == SAP_OK);

    rtxn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(rtxn != NULL);
    CHECK(sap_runner_txstack_v0_read_dbi(&stack, rtxn, 0u, "a", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 2u);
    CHECK(memcmp(val, "db", 2u) == 0);

    CHECK(sap_runner_txstack_v0_stage_put_dbi(&stack, 0u, "x", 1u, "outer", 5u) == SAP_OK);
    CHECK(sap_runner_txstack_v0_push(&stack) == SAP_OK);
    CHECK(sap_runner_txstack_v0_depth(&stack) == 2u);

    CHECK(sap_runner_txstack_v0_read_dbi(&stack, rtxn, 0u, "x", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 5u);
    CHECK(memcmp(val, "outer", 5u) == 0);
    CHECK(sap_runner_txctx_v0_read_count(sap_runner_txstack_v0_current(&stack)) == 0u);

    CHECK(sap_runner_txstack_v0_stage_put_dbi(&stack, 0u, "y", 1u, "child", 5u) == SAP_OK);
    intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
    intent.flags = 0u;
    intent.due_ts = 0;
    intent.message = intent_msg;
    intent.message_len = sizeof(intent_msg);
    CHECK(sap_runner_txstack_v0_push_intent(&stack, &intent) == SAP_OK);

    CHECK(sap_runner_txstack_v0_commit_top(&stack) == SAP_OK);
    CHECK(sap_runner_txstack_v0_depth(&stack) == 1u);
    CHECK(sap_runner_txctx_v0_write_count(sap_runner_txstack_v0_current(&stack)) == 2u);
    CHECK(sap_runner_txctx_v0_intent_count(sap_runner_txstack_v0_current(&stack)) == 1u);
    txn_abort(rtxn);

    wtxn = txn_begin(db, NULL, 0u);
    CHECK(wtxn != NULL);
    CHECK(sap_runner_txstack_v0_validate_root_reads(&stack, wtxn) == SAP_OK);
    CHECK(sap_runner_txstack_v0_apply_root_writes(&stack, wtxn) == SAP_OK);
    CHECK(txn_commit(wtxn) == SAP_OK);

    CHECK(db_get(db, "x", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 5u);
    CHECK(memcmp(val, "outer", 5u) == 0);
    CHECK(db_get(db, "y", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 5u);
    CHECK(memcmp(val, "child", 5u) == 0);

    sap_runner_txstack_v0_dispose(&stack);
    db_close(db);
    return 0;
}

static int test_nested_abort_discards_child_state(void)
{
    DB *db = new_db();
    SapRunnerTxStackV0 stack;
    Txn *rtxn;
    Txn *wtxn;
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(sap_runner_txstack_v0_init(&stack) == SAP_OK);
    CHECK(sap_runner_txstack_v0_push(&stack) == SAP_OK);
    CHECK(sap_runner_txstack_v0_stage_put_dbi(&stack, 0u, "x", 1u, "outer", 5u) == SAP_OK);

    CHECK(sap_runner_txstack_v0_push(&stack) == SAP_OK);
    CHECK(sap_runner_txstack_v0_stage_put_dbi(&stack, 0u, "x", 1u, "child", 5u) == SAP_OK);
    CHECK(sap_runner_txstack_v0_stage_put_dbi(&stack, 0u, "z", 1u, "tmp", 3u) == SAP_OK);
    CHECK(sap_runner_txstack_v0_abort_top(&stack) == SAP_OK);
    CHECK(sap_runner_txstack_v0_depth(&stack) == 1u);

    rtxn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(rtxn != NULL);
    CHECK(sap_runner_txstack_v0_read_dbi(&stack, rtxn, 0u, "x", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 5u);
    CHECK(memcmp(val, "outer", 5u) == 0);
    CHECK(sap_runner_txstack_v0_read_dbi(&stack, rtxn, 0u, "z", 1u, &val, &val_len) ==
          SAP_NOTFOUND);
    txn_abort(rtxn);

    wtxn = txn_begin(db, NULL, 0u);
    CHECK(wtxn != NULL);
    CHECK(sap_runner_txstack_v0_validate_root_reads(&stack, wtxn) == SAP_OK);
    CHECK(sap_runner_txstack_v0_apply_root_writes(&stack, wtxn) == SAP_OK);
    CHECK(txn_commit(wtxn) == SAP_OK);

    CHECK(db_get(db, "x", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 5u);
    CHECK(memcmp(val, "outer", 5u) == 0);
    CHECK(db_get(db, "z", 1u, &val, &val_len) == SAP_NOTFOUND);

    sap_runner_txstack_v0_dispose(&stack);
    db_close(db);
    return 0;
}

static int test_stack_state_guards(void)
{
    SapRunnerTxStackV0 stack;
    DB *db = new_db();
    Txn *wtxn;

    CHECK(db != NULL);
    CHECK(sap_runner_txstack_v0_init(&stack) == SAP_OK);
    CHECK(sap_runner_txstack_v0_commit_top(&stack) == SAP_ERROR);
    CHECK(sap_runner_txstack_v0_abort_top(&stack) == SAP_ERROR);

    CHECK(sap_runner_txstack_v0_push(&stack) == SAP_OK);
    CHECK(sap_runner_txstack_v0_push(&stack) == SAP_OK);
    wtxn = txn_begin(db, NULL, 0u);
    CHECK(wtxn != NULL);
    CHECK(sap_runner_txstack_v0_validate_root_reads(&stack, wtxn) == SAP_BUSY);
    CHECK(sap_runner_txstack_v0_apply_root_writes(&stack, wtxn) == SAP_BUSY);
    txn_abort(wtxn);

    sap_runner_txstack_v0_dispose(&stack);
    db_close(db);
    return 0;
}

int main(void)
{
    int rc;

    rc = test_nested_commit_merges_into_parent();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_nested_abort_discards_child_state();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_stack_state_guards();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
