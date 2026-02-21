/*
 * runner_native_example.c - non-WASI runner path via attempt_handler_v0
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/attempt_handler_v0.h"
#include "runner/intent_sink_v0.h"
#include "runner/outbox_v0.h"
#include "runner/runner_v0.h"
#include "runner/timer_v0.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const uint8_t k_counter_key[] = {'c', 'o', 'u', 'n', 't', 'e', 'r'};
static const uint8_t k_last_key[] = {'l', 'a', 's', 't'};

typedef struct
{
    uint32_t calls;
    int64_t due_ts;
} ExampleAtomicCtx;

typedef struct
{
    uint32_t calls;
    uint8_t frames[4][64];
    uint32_t frame_lens[4];
} OutboxCollectCtx;

typedef struct
{
    uint32_t calls;
    int64_t due_ts[4];
    uint64_t seq[4];
    uint8_t payloads[4][256];
    uint32_t payload_lens[4];
} TimerCollectCtx;

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

static void wr64be(uint8_t out[8], uint64_t v)
{
    int i;
    for (i = 7; i >= 0; i--)
    {
        out[i] = (uint8_t)(v & 0xffu);
        v >>= 8;
    }
}

static uint64_t rd64be(const uint8_t in[8])
{
    uint64_t v = 0u;
    uint32_t i;

    for (i = 0u; i < 8u; i++)
    {
        v = (v << 8) | (uint64_t)in[i];
    }
    return v;
}

static int app_state_read_counter(DB *db, uint64_t *counter_out)
{
    Txn *txn = NULL;
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !counter_out)
    {
        return SAP_ERROR;
    }
    *counter_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_APP_STATE, k_counter_key, sizeof(k_counter_key), &val,
                     &val_len);
    if (rc == SAP_NOTFOUND)
    {
        txn_abort(txn);
        return SAP_OK;
    }
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (!val || val_len != 8u)
    {
        txn_abort(txn);
        return SAP_CONFLICT;
    }
    *counter_out = rd64be((const uint8_t *)val);
    txn_abort(txn);
    return SAP_OK;
}

static int app_state_read_blob(DB *db, const uint8_t *key, uint32_t key_len, uint8_t *dst,
                               uint32_t dst_cap, uint32_t *dst_len_out)
{
    Txn *txn = NULL;
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !key || key_len == 0u || !dst || dst_cap == 0u || !dst_len_out)
    {
        return SAP_ERROR;
    }
    *dst_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    rc = txn_get_dbi(txn, SAP_WIT_DBI_APP_STATE, key, key_len, &val, &val_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (!val || val_len > dst_cap)
    {
        txn_abort(txn);
        return SAP_FULL;
    }
    memcpy(dst, val, val_len);
    *dst_len_out = val_len;
    txn_abort(txn);
    return SAP_OK;
}

static int inbox_read_frame(DB *db, uint64_t worker_id, uint64_t seq, uint8_t *dst,
                            uint32_t dst_cap, uint32_t *dst_len_out)
{
    Txn *txn = NULL;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !dst || dst_cap == 0u || !dst_len_out)
    {
        return SAP_ERROR;
    }
    *dst_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), &val, &val_len);
    if (rc != SAP_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (!val || val_len > dst_cap)
    {
        txn_abort(txn);
        return SAP_FULL;
    }
    memcpy(dst, val, val_len);
    *dst_len_out = val_len;
    txn_abort(txn);
    return SAP_OK;
}

static int collect_outbox_frame(const uint8_t *frame, uint32_t frame_len, void *ctx)
{
    OutboxCollectCtx *outbox = (OutboxCollectCtx *)ctx;

    if (!outbox || !frame || frame_len == 0u || frame_len > sizeof(outbox->frames[0]) ||
        outbox->calls >= 4u)
    {
        return SAP_ERROR;
    }
    memcpy(outbox->frames[outbox->calls], frame, frame_len);
    outbox->frame_lens[outbox->calls] = frame_len;
    outbox->calls++;
    return SAP_OK;
}

static int collect_timer_due(int64_t due_ts, uint64_t seq, const uint8_t *payload,
                             uint32_t payload_len, void *ctx)
{
    TimerCollectCtx *timers = (TimerCollectCtx *)ctx;

    if (!timers || !payload || payload_len == 0u || payload_len > sizeof(timers->payloads[0]) ||
        timers->calls >= 4u)
    {
        return SAP_ERROR;
    }
    timers->due_ts[timers->calls] = due_ts;
    timers->seq[timers->calls] = seq;
    memcpy(timers->payloads[timers->calls], payload, payload_len);
    timers->payload_lens[timers->calls] = payload_len;
    timers->calls++;
    return SAP_OK;
}

static int native_atomic_apply(SapRunnerTxStackV0 *stack, Txn *read_txn, SapRunnerV0 *runner,
                               const SapRunnerMessageV0 *msg, void *ctx)
{
    ExampleAtomicCtx *atomic = (ExampleAtomicCtx *)ctx;
    SapRunnerIntentV0 outbox_intent = {0};
    SapRunnerIntentV0 timer_intent = {0};
    SapRunnerMessageV0 timer_msg = {0};
    uint8_t timer_frame[256];
    uint32_t timer_frame_len = 0u;
    const void *cur = NULL;
    uint32_t cur_len = 0u;
    uint64_t count = 0u;
    uint8_t raw_count[8];
    int rc;

    (void)runner;
    if (!stack || !read_txn || !msg || !atomic || !msg->payload || msg->payload_len == 0u)
    {
        return SAP_ERROR;
    }
    atomic->calls++;

    rc = sap_runner_txstack_v0_read_dbi(stack, read_txn, SAP_WIT_DBI_APP_STATE, k_counter_key,
                                        sizeof(k_counter_key), &cur, &cur_len);
    if (rc == SAP_OK)
    {
        if (!cur || cur_len != 8u)
        {
            return SAP_CONFLICT;
        }
        count = rd64be((const uint8_t *)cur);
    }
    else if (rc != SAP_NOTFOUND)
    {
        return rc;
    }

    count++;
    wr64be(raw_count, count);
    rc = sap_runner_txstack_v0_stage_put_dbi(stack, SAP_WIT_DBI_APP_STATE, k_counter_key,
                                             sizeof(k_counter_key), raw_count, sizeof(raw_count));
    if (rc != SAP_OK)
    {
        return rc;
    }

    /* Demonstrate a closed-nested child frame inside the atomic handler. */
    rc = sap_runner_txstack_v0_push(stack);
    if (rc != SAP_OK)
    {
        return rc;
    }
    rc = sap_runner_txstack_v0_stage_put_dbi(stack, SAP_WIT_DBI_APP_STATE, k_last_key,
                                             sizeof(k_last_key), msg->payload, msg->payload_len);
    if (rc != SAP_OK)
    {
        (void)sap_runner_txstack_v0_abort_top(stack);
        return rc;
    }
    rc = sap_runner_txstack_v0_commit_top(stack);
    if (rc != SAP_OK)
    {
        (void)sap_runner_txstack_v0_abort_top(stack);
        return rc;
    }

    outbox_intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
    outbox_intent.flags = 0u;
    outbox_intent.due_ts = 0;
    outbox_intent.message = msg->payload;
    outbox_intent.message_len = msg->payload_len;
    rc = sap_runner_txstack_v0_push_intent(stack, &outbox_intent);
    if (rc != SAP_OK)
    {
        return rc;
    }

    timer_intent.kind = SAP_RUNNER_INTENT_KIND_TIMER_ARM;
    timer_intent.flags = SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS;
    timer_intent.due_ts = atomic->due_ts;
    timer_msg = *msg;
    timer_msg.kind = SAP_RUNNER_MESSAGE_KIND_TIMER;
    timer_msg.route_timestamp = atomic->due_ts;
    if (sap_runner_message_v0_size(&timer_msg) > sizeof(timer_frame))
    {
        return SAP_FULL;
    }
    if (sap_runner_message_v0_encode(&timer_msg, timer_frame, sizeof(timer_frame),
                                     &timer_frame_len) != SAP_RUNNER_WIRE_OK)
    {
        return SAP_ERROR;
    }
    timer_intent.message = timer_frame;
    timer_intent.message_len = timer_frame_len;
    return sap_runner_txstack_v0_push_intent(stack, &timer_intent);
}

int main(void)
{
    static const uint8_t payload[] = {'n', 'a', 't', 'i', 'v', 'e', '-', 'v', '0'};
    static const uint8_t msg_id[] = {'e', 'x', 'a', 'm', 'p', 'l', 'e', '-', '1'};
    SapRunnerMessageV0 msg = {0};
    SapRunnerV0Config cfg = {0};
    SapRunnerV0Worker worker = {0};
    SapRunnerIntentSinkV0 intent_sink = {0};
    SapRunnerAttemptHandlerV0 handler = {0};
    SapRunnerAttemptV0Policy policy;
    ExampleAtomicCtx atomic = {0};
    OutboxCollectCtx outbox = {0};
    TimerCollectCtx timers = {0};
    uint8_t frame[256];
    uint8_t inbox_frame[256];
    uint8_t last_blob[64];
    uint32_t frame_len = 0u;
    uint32_t inbox_frame_len = 0u;
    uint32_t processed = 0u;
    uint32_t last_blob_len = 0u;
    uint64_t counter = 0u;
    int rc = 1;
    int worker_inited = 0;
    DB *db = NULL;

    db = db_open(&g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        fprintf(stderr, "runner-native-example: db_open failed\n");
        goto done;
    }

    cfg.db = db;
    cfg.worker_id = 42u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    atomic.calls = 0u;
    atomic.due_ts = 4102444800000LL;

    if (sap_runner_intent_sink_v0_init(&intent_sink, db, 1u, 1u) != SAP_OK)
    {
        fprintf(stderr, "runner-native-example: intent sink init failed\n");
        goto done;
    }
    if (sap_runner_attempt_handler_v0_init(&handler, db, native_atomic_apply, &atomic,
                                           sap_runner_intent_sink_v0_publish,
                                           &intent_sink) != SAP_OK)
    {
        fprintf(stderr, "runner-native-example: attempt handler init failed\n");
        goto done;
    }

    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 2u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;
    policy.sleep_fn = NULL;
    policy.sleep_ctx = NULL;
    sap_runner_attempt_handler_v0_set_policy(&handler, &policy);

    if (sap_runner_v0_worker_init(&worker, &cfg, sap_runner_attempt_handler_v0_runner_handler,
                                  &handler, 4u) != SAP_OK)
    {
        fprintf(stderr, "runner-native-example: worker init failed\n");
        goto done;
    }
    worker_inited = 1;

    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg.flags = 0u;
    msg.to_worker = 42;
    msg.route_worker = 42;
    msg.route_timestamp = 123;
    msg.from_worker = 0;
    msg.message_id = msg_id;
    msg.message_id_len = sizeof(msg_id);
    msg.trace_id = NULL;
    msg.trace_id_len = 0u;
    msg.payload = payload;
    msg.payload_len = sizeof(payload);

    if (sap_runner_message_v0_encode(&msg, frame, sizeof(frame), &frame_len) != SAP_RUNNER_WIRE_OK)
    {
        fprintf(stderr, "runner-native-example: encode failed\n");
        goto done;
    }
    if (sap_runner_message_v0_decode(frame, frame_len, &msg) != SAP_RUNNER_WIRE_OK)
    {
        fprintf(stderr, "runner-native-example: immediate decode check failed\n");
        goto done;
    }
    if (sap_runner_v0_inbox_put(db, 42u, 1u, frame, frame_len) != SAP_OK)
    {
        fprintf(stderr, "runner-native-example: inbox_put failed\n");
        goto done;
    }
    if (inbox_read_frame(db, 42u, 1u, inbox_frame, sizeof(inbox_frame), &inbox_frame_len) !=
            SAP_OK ||
        inbox_frame_len != frame_len ||
        sap_runner_message_v0_decode(inbox_frame, inbox_frame_len, &msg) != SAP_RUNNER_WIRE_OK)
    {
        fprintf(stderr, "runner-native-example: inbox frame decode check failed\n");
        goto done;
    }
    {
        int tick_rc = sap_runner_v0_worker_tick(&worker, &processed);
        if (tick_rc != SAP_OK || processed != 1u)
        {
            fprintf(
                stderr,
                "runner-native-example: worker_tick failed (rc=%d last_error=%d processed=%u)\n",
                tick_rc, worker.last_error, processed);
            goto done;
        }
    }

    if (app_state_read_counter(db, &counter) != SAP_OK || counter != 1u)
    {
        fprintf(stderr, "runner-native-example: counter check failed\n");
        goto done;
    }
    if (app_state_read_blob(db, k_last_key, sizeof(k_last_key), last_blob, sizeof(last_blob),
                            &last_blob_len) != SAP_OK ||
        last_blob_len != sizeof(payload) || memcmp(last_blob, payload, sizeof(payload)) != 0)
    {
        fprintf(stderr, "runner-native-example: last payload check failed\n");
        goto done;
    }

    processed = 0u;
    if (sap_runner_outbox_v0_drain(db, 4u, collect_outbox_frame, &outbox, &processed) != SAP_OK ||
        processed != 1u || outbox.calls != 1u || outbox.frame_lens[0] != sizeof(payload) ||
        memcmp(outbox.frames[0], payload, sizeof(payload)) != 0)
    {
        fprintf(stderr, "runner-native-example: outbox drain check failed\n");
        goto done;
    }

    processed = 0u;
    if (sap_runner_timer_v0_drain_due(db, atomic.due_ts, 4u, collect_timer_due, &timers,
                                      &processed) != SAP_OK ||
        processed != 1u || timers.calls != 1u || timers.due_ts[0] != atomic.due_ts ||
        timers.seq[0] != 1u)
    {
        fprintf(stderr, "runner-native-example: timer drain check failed\n");
        goto done;
    }
    {
        SapRunnerMessageV0 timer_msg = {0};
        if (sap_runner_message_v0_decode(timers.payloads[0], timers.payload_lens[0], &timer_msg) !=
                SAP_RUNNER_WIRE_OK ||
            timer_msg.kind != SAP_RUNNER_MESSAGE_KIND_TIMER ||
            timer_msg.payload_len != sizeof(payload) ||
            memcmp(timer_msg.payload, payload, sizeof(payload)) != 0)
        {
            fprintf(stderr, "runner-native-example: timer payload decode check failed\n");
            goto done;
        }
    }

    if (handler.last_stats.attempts != 1u || handler.last_stats.retries != 0u ||
        handler.last_stats.last_rc != SAP_OK || atomic.calls != 1u)
    {
        fprintf(stderr, "runner-native-example: attempt stats check failed\n");
        goto done;
    }

    printf("runner-native-example: OK worker=%u attempts=%u outbox=%u timers=%u\n",
           worker.runner.worker_id, handler.last_stats.attempts, outbox.calls, timers.calls);
    rc = 0;

done:
    if (worker_inited)
    {
        sap_runner_v0_worker_shutdown(&worker);
    }
    if (db)
    {
        db_close(db);
    }
    return rc;
}
