/*
 * runner_outbox_test.c - tests for phase-C outbox append/drain scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/outbox_v0.h"
#include "runner/runner_v0.h"
#include "runner/wit_wire_bridge_v0.h"

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
    if (!db)
    {
        return NULL;
    }
    if (sap_runner_v0_bootstrap_dbis(db) != ERR_OK)
    {
        db_close(db);
        return NULL;
    }
    return db;
}

static int encode_message(uint32_t to_worker, const uint8_t *payload, uint32_t payload_len,
                          uint8_t *buf, uint32_t buf_cap, uint32_t *len_out)
{
    uint8_t msg_id[] = {'o', 'b', 'x', (uint8_t)payload_len};
    SapRunnerMessageV0 msg = {0};

    if (!payload || payload_len == 0u)
    {
        return ERR_INVALID;
    }
    msg.kind = SAP_RUNNER_MESSAGE_KIND_EVENT;
    msg.flags = 0u;
    msg.to_worker = (int64_t)to_worker;
    msg.route_worker = (int64_t)to_worker;
    msg.route_timestamp = 123;
    msg.from_worker = 0;
    msg.message_id = msg_id;
    msg.message_id_len = sizeof(msg_id);
    msg.trace_id = NULL;
    msg.trace_id_len = 0u;
    msg.payload = payload;
    msg.payload_len = payload_len;
    return sap_runner_message_v0_encode(&msg, buf, buf_cap, len_out);
}

static int outbox_get(DB *db, uint64_t seq, const void **val_out, uint32_t *val_len_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_OUTBOX_KEY_V0_SIZE];
    const void *val = NULL;
    uint32_t val_len = 0u;
    uint8_t *copy = NULL;
    int rc;

    if (!db || !val_out || !val_len_out)
    {
        return ERR_INVALID;
    }
    *val_out = NULL;
    *val_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_INVALID;
    }
    sap_runner_outbox_v0_key_encode(seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_OUTBOX, key, sizeof(key), &val, &val_len);
    if (rc == ERR_NOT_FOUND)
    {
        txn_abort(txn);
        return ERR_NOT_FOUND;
    }
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (!val || val_len == 0u)
    {
        txn_abort(txn);
        return ERR_CORRUPT;
    }
    if (!sap_runner_wit_wire_v0_value_is_dbi2_outbox(val, val_len))
    {
        txn_abort(txn);
        return ERR_CORRUPT;
    }
    rc = sap_runner_wit_wire_v0_decode_dbi2_outbox_value_to_wire((const uint8_t *)val, val_len,
                                                                  &copy, val_len_out, NULL);
    txn_abort(txn);
    if (rc != ERR_OK)
    {
        return rc;
    }
    *val_out = copy;
    return ERR_OK;
}

typedef struct
{
    uint32_t calls;
    uint8_t frames[8][256];
    uint32_t frame_lens[8];
} DrainCtx;

static int collect_frame(const uint8_t *frame, uint32_t frame_len, void *ctx)
{
    DrainCtx *drain = (DrainCtx *)ctx;
    if (!drain || !frame || frame_len == 0u || frame_len > 256u || drain->calls >= 8u)
    {
        return ERR_INVALID;
    }
    memcpy(drain->frames[drain->calls], frame, frame_len);
    drain->frame_lens[drain->calls] = frame_len;
    drain->calls++;
    return ERR_OK;
}

static int test_outbox_append_and_drain(void)
{
    DB *db = new_db();
    DrainCtx drain = {0};
    const uint8_t payload_a[] = {'a'};
    const uint8_t payload_b[] = {'b', 'b'};
    uint8_t a[128];
    uint8_t b[128];
    uint32_t a_len = 0u;
    uint32_t b_len = 0u;
    uint32_t processed = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(encode_message(10u, payload_a, sizeof(payload_a), a, sizeof(a), &a_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(encode_message(11u, payload_b, sizeof(payload_b), b, sizeof(b), &b_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_outbox_v0_append_frame(db, 10u, a, a_len) == ERR_OK);
    CHECK(sap_runner_outbox_v0_append_frame(db, 11u, b, b_len) == ERR_OK);
    CHECK(sap_runner_outbox_v0_drain(db, 8u, collect_frame, &drain, &processed) == ERR_OK);
    CHECK(processed == 2u);
    CHECK(drain.calls == 2u);
    CHECK(drain.frame_lens[0] == a_len);
    CHECK(memcmp(drain.frames[0], a, a_len) == 0);
    CHECK(drain.frame_lens[1] == b_len);
    CHECK(memcmp(drain.frames[1], b, b_len) == 0);

    CHECK(outbox_get(db, 10u, &val, &val_len) == ERR_NOT_FOUND);
    CHECK(outbox_get(db, 11u, &val, &val_len) == ERR_NOT_FOUND);

    db_close(db);
    return 0;
}

typedef struct
{
    uint32_t calls;
    int timer_only;
} AtomicCtx;

static int atomic_emit_intent(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx)
{
    AtomicCtx *atomic = (AtomicCtx *)ctx;
    SapRunnerIntentV0 intent = {0};
    const uint8_t outbox_payload[] = {'e', 'v', 't'};
    const uint8_t timer_payload[] = {'t'};
    uint8_t frame[128];
    uint32_t frame_len = 0u;

    (void)read_txn;
    if (!stack || !atomic)
    {
        return ERR_INVALID;
    }
    atomic->calls++;

    if (atomic->timer_only)
    {
        intent.kind = SAP_RUNNER_INTENT_KIND_TIMER_ARM;
        intent.flags = SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS;
        intent.due_ts = 123;
        if (encode_message(7u, timer_payload, sizeof(timer_payload), frame, sizeof(frame),
                           &frame_len) != SAP_RUNNER_WIRE_OK)
        {
            return ERR_CORRUPT;
        }
        intent.message = frame;
        intent.message_len = frame_len;
    }
    else
    {
        intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
        intent.flags = 0u;
        intent.due_ts = 0;
        if (encode_message(7u, outbox_payload, sizeof(outbox_payload), frame, sizeof(frame),
                           &frame_len) != SAP_RUNNER_WIRE_OK)
        {
            return ERR_CORRUPT;
        }
        intent.message = frame;
        intent.message_len = frame_len;
    }
    return sap_runner_txstack_v0_push_intent(stack, &intent);
}

static int test_outbox_publisher_with_attempt_engine(void)
{
    DB *db = new_db();
    SapRunnerOutboxV0Publisher publisher = {0};
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats stats = {0};
    AtomicCtx atomic = {0};
    const void *val = NULL;
    uint32_t val_len = 0u;
    SapRunnerMessageV0 out_msg = {0};

    CHECK(db != NULL);
    CHECK(sap_runner_outbox_v0_publisher_init(&publisher, db, 100u) == ERR_OK);
    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 0u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;

    atomic.calls = 0u;
    atomic.timer_only = 0;
    CHECK(sap_runner_attempt_v0_run(db, &policy, atomic_emit_intent, &atomic,
                                    sap_runner_outbox_v0_publish_intent, &publisher,
                                    &stats) == ERR_OK);
    CHECK(stats.attempts == 1u);
    CHECK(stats.last_rc == ERR_OK);
    CHECK(atomic.calls == 1u);
    CHECK(publisher.next_seq == 101u);
    CHECK(outbox_get(db, 100u, &val, &val_len) == ERR_OK);
    CHECK(sap_runner_message_v0_decode((const uint8_t *)val, val_len, &out_msg) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(out_msg.payload_len == 3u);
    CHECK(memcmp(out_msg.payload, "evt", 3u) == 0);
    free((void *)val);

    db_close(db);
    return 0;
}

static int test_outbox_publisher_rejects_timer_intent(void)
{
    DB *db = new_db();
    SapRunnerOutboxV0Publisher publisher = {0};
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats stats = {0};
    AtomicCtx atomic = {0};
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(sap_runner_outbox_v0_publisher_init(&publisher, db, 200u) == ERR_OK);
    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 0u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;

    atomic.calls = 0u;
    atomic.timer_only = 1;
    CHECK(sap_runner_attempt_v0_run(db, &policy, atomic_emit_intent, &atomic,
                                    sap_runner_outbox_v0_publish_intent, &publisher,
                                    &stats) == ERR_INVALID);
    CHECK(stats.attempts == 1u);
    CHECK(stats.last_rc == ERR_INVALID);
    CHECK(outbox_get(db, 200u, &val, &val_len) == ERR_NOT_FOUND);

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

    int rc;

    rc = test_outbox_append_and_drain();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_outbox_publisher_with_attempt_engine();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_outbox_publisher_rejects_timer_intent();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
