/*
 * runner_attempt_handler_test.c - tests for generic attempt-backed handler adapter
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/attempt_handler_v0.h"

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

typedef struct
{
    uint32_t calls;
    uint32_t fail_conflicts_remaining;
    int emit_intent;
} AtomicCtx;

typedef struct
{
    uint32_t calls;
    uint8_t frame[128];
    uint32_t frame_len;
} SinkCtx;

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
    if (db)
    {
        dbi_open(db, 10u, NULL, NULL, 0u);
    }
    return db;
}

static int ensure_runner_schema(DB *db)
{
    if (!db)
    {
        return SAP_ERROR;
    }
    if (sap_runner_v0_bootstrap_dbis(db) != SAP_OK)
    {
        return SAP_ERROR;
    }
    return sap_runner_v0_ensure_schema_version(db, 0u, 0u, 1);
}

static int app_state_get(DB *db, const void *key, uint32_t key_len, const void **val_out,
                         uint32_t *val_len_out)
{
    Txn *txn;
    int rc;

    if (!db || !key || key_len == 0u || !val_out || !val_len_out)
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

static int capture_sink(const uint8_t *frame, uint32_t frame_len, void *ctx)
{
    SinkCtx *sink = (SinkCtx *)ctx;
    if (!sink || !frame || frame_len == 0u || frame_len > sizeof(sink->frame))
    {
        return SAP_ERROR;
    }
    memcpy(sink->frame, frame, frame_len);
    sink->frame_len = frame_len;
    sink->calls++;
    return SAP_OK;
}

static int atomic_apply(SapRunnerTxStackV0 *stack, Txn *read_txn, SapRunnerV0 *runner,
                        const SapRunnerMessageV0 *msg, void *ctx)
{
    AtomicCtx *atomic = (AtomicCtx *)ctx;
    const uint8_t key[] = {'k'};
    int rc;

    (void)read_txn;
    (void)runner;
    if (!stack || !msg || !atomic)
    {
        return SAP_ERROR;
    }

    atomic->calls++;
    if (atomic->fail_conflicts_remaining > 0u)
    {
        atomic->fail_conflicts_remaining--;
        return SAP_CONFLICT;
    }

    rc = sap_runner_txstack_v0_stage_put_dbi(stack, 10u, key, sizeof(key), msg->payload,
                                             msg->payload_len);
    if (rc != SAP_OK)
    {
        return rc;
    }

    if (atomic->emit_intent)
    {
        SapRunnerIntentV0 intent = {0};
        intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
        intent.flags = 0u;
        intent.due_ts = 0;
        intent.message = msg->payload;
        intent.message_len = msg->payload_len;
        rc = sap_runner_txstack_v0_push_intent(stack, &intent);
        if (rc != SAP_OK)
        {
            return rc;
        }
    }

    return SAP_OK;
}

static void make_message(SapRunnerMessageV0 *msg, const uint8_t *payload, uint32_t payload_len)
{
    static const uint8_t msg_id[] = {'i', 'd'};

    memset(msg, 0, sizeof(*msg));
    msg->kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg->flags = 0u;
    msg->to_worker = 7;
    msg->route_worker = 7;
    msg->route_timestamp = 0;
    msg->from_worker = 0;
    msg->message_id = msg_id;
    msg->message_id_len = sizeof(msg_id);
    msg->trace_id = NULL;
    msg->trace_id_len = 0u;
    msg->payload = payload;
    msg->payload_len = payload_len;
}

static int encode_frame_for_worker(uint32_t to_worker, const uint8_t *payload, uint32_t payload_len,
                                   uint8_t *dst, uint32_t dst_cap, uint32_t *written_out)
{
    SapRunnerMessageV0 msg = {0};
    if (!payload || payload_len == 0u || !dst || !written_out)
    {
        return SAP_ERROR;
    }
    make_message(&msg, payload, payload_len);
    msg.to_worker = (int64_t)to_worker;
    msg.route_worker = (int64_t)to_worker;
    return sap_runner_message_v0_encode(&msg, dst, dst_cap, written_out);
}

static int test_attempt_handler_commits_and_emits_intent(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    SapRunnerAttemptHandlerV0 handler = {0};
    AtomicCtx atomic = {0};
    SinkCtx sink = {0};
    SapRunnerMessageV0 msg = {0};
    const uint8_t payload[] = {'o', 'k'};
    const void *stored = NULL;
    uint32_t stored_len = 0u;
    SapRunnerIntentV0 decoded_intent = {0};
    const uint8_t key[] = {'k'};

    CHECK(db != NULL);
    CHECK(ensure_runner_schema(db) == SAP_OK);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    atomic.calls = 0u;
    atomic.fail_conflicts_remaining = 0u;
    atomic.emit_intent = 1;
    CHECK(sap_runner_attempt_handler_v0_init(&handler, db, atomic_apply, &atomic, capture_sink,
                                             &sink) == SAP_OK);

    make_message(&msg, payload, sizeof(payload));
    CHECK(sap_runner_attempt_handler_v0_runner_handler(&runner, &msg, &handler) == SAP_OK);
    CHECK(atomic.calls == 1u);
    CHECK(handler.last_stats.attempts == 1u);
    CHECK(handler.last_stats.retries == 0u);
    CHECK(handler.last_stats.last_rc == SAP_OK);
    CHECK(sink.calls == 1u);

    CHECK(app_state_get(db, key, sizeof(key), &stored, &stored_len) == SAP_OK);
    CHECK(stored_len == sizeof(payload));
    CHECK(memcmp(stored, payload, sizeof(payload)) == 0);

    CHECK(sap_runner_intent_v0_decode(sink.frame, sink.frame_len, &decoded_intent) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(decoded_intent.kind == SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT);
    CHECK(decoded_intent.message_len == sizeof(payload));
    CHECK(memcmp(decoded_intent.message, payload, sizeof(payload)) == 0);

    db_close(db);
    return 0;
}

static int test_attempt_handler_retries_conflicts(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    SapRunnerAttemptHandlerV0 handler = {0};
    SapRunnerAttemptV0Policy policy;
    AtomicCtx atomic = {0};
    SapRunnerMessageV0 msg = {0};
    const uint8_t payload[] = {'v'};
    const uint8_t key[] = {'k'};
    const void *stored = NULL;
    uint32_t stored_len = 0u;

    CHECK(db != NULL);
    CHECK(ensure_runner_schema(db) == SAP_OK);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    atomic.calls = 0u;
    atomic.fail_conflicts_remaining = 1u;
    atomic.emit_intent = 0;
    CHECK(sap_runner_attempt_handler_v0_init(&handler, db, atomic_apply, &atomic, NULL, NULL) ==
          SAP_OK);

    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 2u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;
    policy.sleep_fn = NULL;
    policy.sleep_ctx = NULL;
    sap_runner_attempt_handler_v0_set_policy(&handler, &policy);

    make_message(&msg, payload, sizeof(payload));
    CHECK(sap_runner_attempt_handler_v0_runner_handler(&runner, &msg, &handler) == SAP_OK);
    CHECK(atomic.calls == 2u);
    CHECK(handler.last_stats.attempts == 2u);
    CHECK(handler.last_stats.retries == 1u);
    CHECK(handler.last_stats.conflict_retries == 1u);
    CHECK(handler.last_stats.last_rc == SAP_OK);

    CHECK(app_state_get(db, key, sizeof(key), &stored, &stored_len) == SAP_OK);
    CHECK(stored_len == sizeof(payload));
    CHECK(memcmp(stored, payload, sizeof(payload)) == 0);

    db_close(db);
    return 0;
}

static int test_attempt_handler_worker_tick_path(void)
{
    DB *db = new_db();
    SapRunnerV0Config cfg;
    SapRunnerV0Worker worker;
    SapRunnerAttemptHandlerV0 handler = {0};
    AtomicCtx atomic = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    const uint8_t payload[] = {'t', 'i', 'c', 'k'};
    const uint8_t key[] = {'k'};
    const void *stored = NULL;
    uint32_t stored_len = 0u;

    CHECK(db != NULL);
    CHECK(ensure_runner_schema(db) == SAP_OK);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    atomic.calls = 0u;
    atomic.fail_conflicts_remaining = 0u;
    atomic.emit_intent = 0;
    CHECK(sap_runner_attempt_handler_v0_init(&handler, db, atomic_apply, &atomic, NULL, NULL) ==
          SAP_OK);
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, sap_runner_attempt_handler_v0_runner_handler,
                                    &handler, 1u) == SAP_OK);

    CHECK(encode_frame_for_worker(7u, payload, sizeof(payload), frame, sizeof(frame), &frame_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len) == SAP_OK);
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(atomic.calls == 1u);
    CHECK(handler.last_stats.attempts == 1u);
    CHECK(handler.last_stats.last_rc == SAP_OK);

    CHECK(app_state_get(db, key, sizeof(key), &stored, &stored_len) == SAP_OK);
    CHECK(stored_len == sizeof(payload));
    CHECK(memcmp(stored, payload, sizeof(payload)) == 0);

    db_close(db);
    return 0;
}

int main(void)
{
    if (test_attempt_handler_commits_and_emits_intent() != 0)
    {
        return 1;
    }
    if (test_attempt_handler_retries_conflicts() != 0)
    {
        return 2;
    }
    if (test_attempt_handler_worker_tick_path() != 0)
    {
        return 3;
    }
    return 0;
}
