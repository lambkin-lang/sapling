/*
 * runner_txctx_test.c - tests for phase-B tx context scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/txctx_v0.h"

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

static SapMemArena *g_alloc = NULL;

static DB *new_db(void)
{
    DB *db = db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    if (db)
    {
        dbi_open(db, 10u, NULL, NULL, 0u);
    }
    return db;
}

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
    rc = txn_put_dbi(txn, 10u, key, key_len, val, val_len);
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
    rc = txn_get_dbi(txn, 10u, key, key_len, val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

static int test_read_set_validation_and_conflict(void)
{
    DB *db = new_db();
    SapRunnerTxCtxV0 ctx;
    Txn *rtxn;
    Txn *wtxn;
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(db_put(db, "k", 1u, "v1", 2u) == SAP_OK);
    CHECK(sap_runner_txctx_v0_init(&ctx) == SAP_OK);

    rtxn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(rtxn != NULL);
    CHECK(sap_runner_txctx_v0_read_dbi(&ctx, rtxn, 10u, "k", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 2u);
    CHECK(memcmp(val, "v1", 2u) == 0);
    CHECK(sap_runner_txctx_v0_read_dbi(&ctx, rtxn, 10u, "k", 1u, &val, &val_len) == SAP_OK);
    CHECK(sap_runner_txctx_v0_read_count(&ctx) == 1u);
    CHECK(sap_runner_txctx_v0_read_dbi(&ctx, rtxn, 10u, "missing", 7u, &val, &val_len) ==
          SAP_NOTFOUND);
    txn_abort(rtxn);

    CHECK(sap_runner_txctx_v0_read_count(&ctx) == 2u);

    wtxn = txn_begin(db, NULL, 0u);
    CHECK(wtxn != NULL);
    CHECK(sap_runner_txctx_v0_validate_reads(&ctx, wtxn) == SAP_OK);
    txn_abort(wtxn);

    CHECK(db_put(db, "k", 1u, "v2", 2u) == SAP_OK);

    wtxn = txn_begin(db, NULL, 0u);
    CHECK(wtxn != NULL);
    CHECK(sap_runner_txctx_v0_validate_reads(&ctx, wtxn) == SAP_CONFLICT);
    txn_abort(wtxn);

    sap_runner_txctx_v0_dispose(&ctx);
    db_close(db);
    return 0;
}

static int test_write_set_apply_and_coalesce(void)
{
    DB *db = new_db();
    SapRunnerTxCtxV0 ctx;
    Txn *wtxn;
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(db_put(db, "b", 1u, "old", 3u) == SAP_OK);
    CHECK(sap_runner_txctx_v0_init(&ctx) == SAP_OK);

    CHECK(sap_runner_txctx_v0_stage_put_dbi(&ctx, 10u, "a", 1u, "v1", 2u) == SAP_OK);
    CHECK(sap_runner_txctx_v0_stage_put_dbi(&ctx, 10u, "a", 1u, "v2", 2u) == SAP_OK);
    CHECK(sap_runner_txctx_v0_stage_del_dbi(&ctx, 10u, "b", 1u) == SAP_OK);
    CHECK(sap_runner_txctx_v0_write_count(&ctx) == 2u);

    wtxn = txn_begin(db, NULL, 0u);
    CHECK(wtxn != NULL);
    CHECK(sap_runner_txctx_v0_apply_writes(&ctx, wtxn) == SAP_OK);
    CHECK(txn_commit(wtxn) == SAP_OK);

    CHECK(db_get(db, "a", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 2u);
    CHECK(memcmp(val, "v2", 2u) == 0);
    CHECK(db_get(db, "b", 1u, &val, &val_len) == SAP_NOTFOUND);

    sap_runner_txctx_v0_dispose(&ctx);
    db_close(db);
    return 0;
}

static int test_read_your_write_semantics(void)
{
    DB *db = new_db();
    SapRunnerTxCtxV0 ctx;
    Txn *rtxn;
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(db_put(db, "k", 1u, "db", 2u) == SAP_OK);
    CHECK(sap_runner_txctx_v0_init(&ctx) == SAP_OK);

    CHECK(sap_runner_txctx_v0_stage_put_dbi(&ctx, 10u, "k", 1u, "local", 5u) == SAP_OK);
    rtxn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(rtxn != NULL);
    CHECK(sap_runner_txctx_v0_read_dbi(&ctx, rtxn, 10u, "k", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 5u);
    CHECK(memcmp(val, "local", 5u) == 0);
    CHECK(sap_runner_txctx_v0_read_count(&ctx) == 0u);

    CHECK(sap_runner_txctx_v0_stage_del_dbi(&ctx, 10u, "k", 1u) == SAP_OK);
    CHECK(sap_runner_txctx_v0_read_dbi(&ctx, rtxn, 10u, "k", 1u, &val, &val_len) == SAP_NOTFOUND);
    txn_abort(rtxn);

    sap_runner_txctx_v0_dispose(&ctx);
    db_close(db);
    return 0;
}

static int test_intent_buffer_roundtrip(void)
{
    SapRunnerTxCtxV0 ctx;
    SapRunnerIntentV0 outbox = {0};
    SapRunnerIntentV0 timer = {0};
    SapRunnerIntentV0 decoded = {0};
    const uint8_t msg_a[] = {'m', 's', 'g'};
    const uint8_t msg_b[] = {'t'};
    const uint8_t *frame;
    uint32_t frame_len;

    CHECK(sap_runner_txctx_v0_init(&ctx) == SAP_OK);

    outbox.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
    outbox.flags = 0u;
    outbox.due_ts = 0;
    outbox.message = msg_a;
    outbox.message_len = sizeof(msg_a);

    timer.kind = SAP_RUNNER_INTENT_KIND_TIMER_ARM;
    timer.flags = SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS;
    timer.due_ts = 1234;
    timer.message = msg_b;
    timer.message_len = sizeof(msg_b);

    CHECK(sap_runner_txctx_v0_push_intent(&ctx, &outbox) == SAP_OK);
    CHECK(sap_runner_txctx_v0_push_intent(&ctx, &timer) == SAP_OK);
    CHECK(sap_runner_txctx_v0_intent_count(&ctx) == 2u);

    frame = sap_runner_txctx_v0_intent_frame(&ctx, 0u, &frame_len);
    CHECK(frame != NULL);
    CHECK(sap_runner_intent_v0_decode(frame, frame_len, &decoded) == SAP_RUNNER_WIRE_OK);
    CHECK(decoded.kind == SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT);
    CHECK(decoded.flags == 0u);
    CHECK(decoded.message_len == sizeof(msg_a));
    CHECK(memcmp(decoded.message, msg_a, sizeof(msg_a)) == 0);

    frame = sap_runner_txctx_v0_intent_frame(&ctx, 1u, &frame_len);
    CHECK(frame != NULL);
    CHECK(sap_runner_intent_v0_decode(frame, frame_len, &decoded) == SAP_RUNNER_WIRE_OK);
    CHECK(decoded.kind == SAP_RUNNER_INTENT_KIND_TIMER_ARM);
    CHECK(decoded.flags == SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS);
    CHECK(decoded.due_ts == 1234);
    CHECK(decoded.message_len == sizeof(msg_b));
    CHECK(memcmp(decoded.message, msg_b, sizeof(msg_b)) == 0);

    CHECK(sap_runner_txctx_v0_intent_frame(&ctx, 99u, &frame_len) == NULL);
    CHECK(frame_len == 0u);

    sap_runner_txctx_v0_dispose(&ctx);
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

    int rc;

    rc = test_read_set_validation_and_conflict();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_write_set_apply_and_coalesce();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_read_your_write_semantics();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_intent_buffer_roundtrip();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
