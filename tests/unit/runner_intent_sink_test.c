/*
 * runner_intent_sink_test.c - tests for composed intent sink scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/intent_sink_v0.h"
#include "runner/runner_v0.h"

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

static DB *new_db(void)
{
    DB *db = db_open(&g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        return NULL;
    }
    if (sap_runner_v0_bootstrap_dbis(db) != SAP_OK)
    {
        db_close(db);
        return NULL;
    }
    return db;
}

static int outbox_get(DB *db, uint64_t seq, const void **val_out, uint32_t *val_len_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_OUTBOX_KEY_V0_SIZE];
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
    sap_runner_outbox_v0_key_encode(seq, key);
    rc = txn_get_dbi(txn, 2u, key, sizeof(key), val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

static int timer_get(DB *db, int64_t due_ts, uint64_t seq, const void **val_out,
                     uint32_t *val_len_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_TIMER_KEY_V0_SIZE];
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
    sap_runner_timer_v0_key_encode(due_ts, seq, key);
    rc = txn_get_dbi(txn, 4u, key, sizeof(key), val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

static int test_sink_routes_outbox_and_timer(void)
{
    DB *db = new_db();
    SapRunnerIntentSinkV0 sink = {0};
    SapRunnerIntentV0 outbox = {0};
    SapRunnerIntentV0 timer = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;
    const uint8_t outbox_payload[] = {'e', 'v'};
    const uint8_t timer_payload[] = {'t', 'm'};

    CHECK(db != NULL);
    CHECK(sap_runner_intent_sink_v0_init(&sink, db, 100u, 200u) == SAP_OK);

    outbox.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
    outbox.flags = 0u;
    outbox.due_ts = 0;
    outbox.message = outbox_payload;
    outbox.message_len = sizeof(outbox_payload);
    CHECK(sap_runner_intent_v0_encode(&outbox, frame, sizeof(frame), &frame_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_intent_sink_v0_publish(frame, frame_len, &sink) == SAP_OK);

    timer.kind = SAP_RUNNER_INTENT_KIND_TIMER_ARM;
    timer.flags = SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS;
    timer.due_ts = 777;
    timer.message = timer_payload;
    timer.message_len = sizeof(timer_payload);
    CHECK(sap_runner_intent_v0_encode(&timer, frame, sizeof(frame), &frame_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_intent_sink_v0_publish(frame, frame_len, &sink) == SAP_OK);

    CHECK(sink.outbox.next_seq == 101u);
    CHECK(sink.timers.next_seq == 201u);
    CHECK(outbox_get(db, 100u, &val, &val_len) == SAP_OK);
    CHECK(val_len == sizeof(outbox_payload));
    CHECK(memcmp(val, outbox_payload, sizeof(outbox_payload)) == 0);
    CHECK(timer_get(db, 777, 200u, &val, &val_len) == SAP_OK);
    CHECK(val_len == sizeof(timer_payload));
    CHECK(memcmp(val, timer_payload, sizeof(timer_payload)) == 0);

    db_close(db);
    return 0;
}

static int test_sink_rejects_invalid_frame(void)
{
    DB *db = new_db();
    SapRunnerIntentSinkV0 sink = {0};
    uint8_t bogus[4] = {0, 1, 2, 3};

    CHECK(db != NULL);
    CHECK(sap_runner_intent_sink_v0_init(&sink, db, 1u, 1u) == SAP_OK);
    CHECK(sap_runner_intent_sink_v0_publish(bogus, sizeof(bogus), &sink) == SAP_ERROR);

    db_close(db);
    return 0;
}

int main(void)
{
    int rc;

    rc = test_sink_routes_outbox_and_timer();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_sink_rejects_invalid_frame();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
