/*
 * runner_multiwriter_stress.c - threaded runner-style multi-writer stress harness
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/attempt_handler_v0.h"
#include "runner/outbox_v0.h"
#include "runner/runner_v0.h"

#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef SAPLING_THREADED
int main(void)
{
    SapArenaOptions g_alloc_opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL
    };
    sap_arena_init(&g_alloc, &g_alloc_opts);

    printf("runner-multiwriter-stress: SAPLING_THREADED required (skipped)\n");
    return 0;
}
#else

#define STRESS_WORKER_COUNT 4u
#define STRESS_MAX_BATCH 32u
#define STRESS_DISPATCH_BATCH 128u
#define STRESS_IDLE_SLEEP_MS 1u
#define STRESS_FRAME_CAP 256u
#define STRESS_OUTBOX_SEQ_STRIDE 1000000000ULL

#define STRESS_DEFAULT_ROUNDS 8u
#define STRESS_DEFAULT_ORDERS 64u
#define STRESS_DEFAULT_TIMEOUT_MS 5000u

#define WORKER_STAGE1 101u
#define WORKER_STAGE2 102u
#define WORKER_STAGE3 103u
#define WORKER_STAGE4 104u

static const uint8_t k_counter_stage1[] = {'s', 't', 'a', 'g', 'e', '.', '1'};
static const uint8_t k_counter_stage2[] = {'s', 't', 'a', 'g', 'e', '.', '2'};
static const uint8_t k_counter_stage3[] = {'s', 't', 'a', 'g', 'e', '.', '3'};
static const uint8_t k_counter_stage4[] = {'s', 't', 'a', 'g', 'e', '.', '4'};

typedef struct
{
    uint32_t worker_id;
    uint32_t next_worker_id;
    const uint8_t *counter_key;
    uint32_t counter_key_len;
} StageAtomicCtx;

typedef struct
{
    SapRunnerV0Worker worker;
    SapRunnerAttemptHandlerV0 handler;
    SapRunnerOutboxV0Publisher outbox;
    StageAtomicCtx atomic;
    int inited;
    int started;
} StageWorkerCtx;

typedef struct
{
    DB *db;
    SapRunnerV0DbGate *db_gate;
    uint32_t worker_ids[STRESS_WORKER_COUNT];
    uint64_t next_seq[STRESS_WORKER_COUNT];
    uint64_t forwarded;
    int stop_requested;
    int last_error;
} DispatcherCtx;

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

static void wr32be(uint8_t out[4], uint32_t v)
{
    out[0] = (uint8_t)((v >> 24) & 0xffu);
    out[1] = (uint8_t)((v >> 16) & 0xffu);
    out[2] = (uint8_t)((v >> 8) & 0xffu);
    out[3] = (uint8_t)(v & 0xffu);
}

static int64_t wall_now_ms(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
    {
        return 0;
    }
    return (int64_t)ts.tv_sec * 1000 + (int64_t)(ts.tv_nsec / 1000000L);
}

static void sleep_ms(uint32_t ms)
{
    struct timespec ts;

    ts.tv_sec = (time_t)(ms / 1000u);
    ts.tv_nsec = (long)((ms % 1000u) * 1000000L);
    nanosleep(&ts, NULL);
}

static uint32_t env_u32(const char *name, uint32_t default_value)
{
    const char *raw;
    char *end = NULL;
    unsigned long v;

    if (!name)
    {
        return default_value;
    }
    raw = getenv(name);
    if (!raw || *raw == '\0')
    {
        return default_value;
    }

    v = strtoul(raw, &end, 10);
    if (!end || *end != '\0' || v == 0ul || v > (unsigned long)UINT32_MAX)
    {
        return default_value;
    }

    return (uint32_t)v;
}

static int app_state_read_counter(DB *db, const uint8_t *key, uint32_t key_len, uint64_t *value_out)
{
    Txn *txn = NULL;
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !key || key_len == 0u || !value_out)
    {
        return SAP_ERROR;
    }
    *value_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }

    rc = txn_get_dbi(txn, SAP_WIT_DBI_APP_STATE, key, key_len, &val, &val_len);
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

    *value_out = rd64be((const uint8_t *)val);
    txn_abort(txn);
    return SAP_OK;
}

static int txstack_read_counter(SapRunnerTxStackV0 *stack, Txn *read_txn, const uint8_t *key,
                                uint32_t key_len, uint64_t *value_out)
{
    const void *cur = NULL;
    uint32_t cur_len = 0u;
    int rc;

    if (!stack || !read_txn || !key || key_len == 0u || !value_out)
    {
        return SAP_ERROR;
    }

    rc = sap_runner_txstack_v0_read_dbi(stack, read_txn, SAP_WIT_DBI_APP_STATE, key, key_len, &cur,
                                        &cur_len);
    if (rc == SAP_NOTFOUND)
    {
        *value_out = 0u;
        return SAP_OK;
    }
    if (rc != SAP_OK)
    {
        return rc;
    }
    if (!cur || cur_len != 8u)
    {
        return SAP_CONFLICT;
    }

    *value_out = rd64be((const uint8_t *)cur);
    return SAP_OK;
}

static int txstack_key_exists(SapRunnerTxStackV0 *stack, Txn *read_txn, uint32_t dbi,
                              const uint8_t *key, uint32_t key_len, int *exists_out)
{
    const void *cur = NULL;
    uint32_t cur_len = 0u;
    int rc;

    if (!stack || !read_txn || !key || key_len == 0u || !exists_out)
    {
        return SAP_ERROR;
    }
    *exists_out = 0;

    rc = sap_runner_txstack_v0_read_dbi(stack, read_txn, dbi, key, key_len, &cur, &cur_len);
    if (rc == SAP_NOTFOUND)
    {
        *exists_out = 0;
        return SAP_OK;
    }
    if (rc == SAP_OK)
    {
        *exists_out = 1;
        return SAP_OK;
    }
    return rc;
}

static int txstack_stage_counter(SapRunnerTxStackV0 *stack, const uint8_t *key, uint32_t key_len,
                                 uint64_t value)
{
    uint8_t raw[8];

    if (!stack || !key || key_len == 0u)
    {
        return SAP_ERROR;
    }

    wr64be(raw, value);
    return sap_runner_txstack_v0_stage_put_dbi(stack, SAP_WIT_DBI_APP_STATE, key, key_len, raw,
                                               sizeof(raw));
}

static int encode_forward_frame(const SapRunnerMessageV0 *msg, uint32_t from_worker,
                                uint32_t to_worker, uint8_t *frame_out, uint32_t frame_cap,
                                uint32_t *frame_len_out)
{
    SapRunnerMessageV0 next = {0};
    uint8_t flags;
    int wire_rc;

    if (!msg || !msg->payload || msg->payload_len != 8u || !msg->message_id ||
        msg->message_id_len == 0u || !frame_out || frame_cap == 0u || !frame_len_out)
    {
        return SAP_ERROR;
    }

    flags = (uint8_t)((msg->flags | SAP_RUNNER_MESSAGE_FLAG_HAS_FROM_WORKER) &
                      SAP_RUNNER_MESSAGE_FLAG_ALLOWED_MASK);

    next.kind = SAP_RUNNER_MESSAGE_KIND_EVENT;
    next.flags = flags;
    next.to_worker = (int64_t)to_worker;
    next.route_worker = (int64_t)to_worker;
    next.route_timestamp = msg->route_timestamp + 1;
    next.from_worker = (int64_t)from_worker;
    next.message_id = msg->message_id;
    next.message_id_len = msg->message_id_len;
    next.trace_id = msg->trace_id;
    next.trace_id_len = msg->trace_id_len;
    next.payload = msg->payload;
    next.payload_len = msg->payload_len;

    wire_rc = sap_runner_message_v0_encode(&next, frame_out, frame_cap, frame_len_out);
    if (wire_rc == SAP_RUNNER_WIRE_OK)
    {
        return SAP_OK;
    }
    if (wire_rc == SAP_RUNNER_WIRE_E2BIG)
    {
        return SAP_FULL;
    }
    return SAP_ERROR;
}

static int stress_atomic_apply(SapRunnerTxStackV0 *stack, Txn *read_txn, SapRunnerV0 *runner,
                               const SapRunnerMessageV0 *msg, void *ctx)
{
    StageAtomicCtx *stage = (StageAtomicCtx *)ctx;
    static const uint8_t k_done[] = {1u};
    uint64_t order_id;
    uint8_t dedupe_key[12];
    int seen = 0;
    uint64_t counter = 0u;
    int rc;

    (void)runner;
    if (!stack || !read_txn || !msg || !stage || !msg->payload || msg->payload_len != 8u)
    {
        return SAP_ERROR;
    }

    order_id = rd64be(msg->payload);
    wr32be(dedupe_key, stage->worker_id);
    wr64be(dedupe_key + 4u, order_id);

    rc = txstack_key_exists(stack, read_txn, SAP_WIT_DBI_DEDUPE, dedupe_key, sizeof(dedupe_key),
                            &seen);
    if (rc != SAP_OK)
    {
        return rc;
    }
    if (seen)
    {
        return SAP_OK;
    }

    rc =
        txstack_read_counter(stack, read_txn, stage->counter_key, stage->counter_key_len, &counter);
    if (rc != SAP_OK)
    {
        return rc;
    }
    if (counter == UINT64_MAX)
    {
        return SAP_FULL;
    }
    rc = txstack_stage_counter(stack, stage->counter_key, stage->counter_key_len, counter + 1u);
    if (rc != SAP_OK)
    {
        return rc;
    }

    if (stage->next_worker_id != 0u)
    {
        uint8_t frame[STRESS_FRAME_CAP];
        uint32_t frame_len = 0u;
        SapRunnerIntentV0 intent = {0};

        rc = encode_forward_frame(msg, stage->worker_id, stage->next_worker_id, frame,
                                  sizeof(frame), &frame_len);
        if (rc != SAP_OK)
        {
            return rc;
        }

        intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
        intent.flags = 0u;
        intent.due_ts = 0;
        intent.message = frame;
        intent.message_len = frame_len;
        rc = sap_runner_txstack_v0_push_intent(stack, &intent);
        if (rc != SAP_OK)
        {
            return rc;
        }
    }

    rc = sap_runner_txstack_v0_stage_put_dbi(stack, SAP_WIT_DBI_DEDUPE, dedupe_key,
                                             sizeof(dedupe_key), k_done, sizeof(k_done));
    if (rc != SAP_OK)
    {
        return rc;
    }

    return SAP_OK;
}

static int dispatcher_stop_requested(const DispatcherCtx *ctx)
{
    if (!ctx)
    {
        return 1;
    }
    return __atomic_load_n(&ctx->stop_requested, __ATOMIC_ACQUIRE);
}

static void dispatcher_request_stop(DispatcherCtx *ctx)
{
    if (!ctx)
    {
        return;
    }
    __atomic_store_n(&ctx->stop_requested, 1, __ATOMIC_RELEASE);
}

static int find_worker_slot(const DispatcherCtx *ctx, uint32_t worker_id, uint32_t *slot_out)
{
    uint32_t i;

    if (!ctx || !slot_out)
    {
        return SAP_ERROR;
    }
    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        if (ctx->worker_ids[i] == worker_id)
        {
            *slot_out = i;
            return SAP_OK;
        }
    }
    return SAP_NOTFOUND;
}

static int dispatch_outbox_frame(const uint8_t *frame, uint32_t frame_len, void *ctx)
{
    DispatcherCtx *dispatch = (DispatcherCtx *)ctx;
    SapRunnerMessageV0 msg = {0};
    uint32_t to_worker;
    uint32_t slot = 0u;
    uint64_t seq;
    int rc;

    if (!dispatch || !frame || frame_len == 0u)
    {
        return SAP_ERROR;
    }
    if (sap_runner_message_v0_decode(frame, frame_len, &msg) != SAP_RUNNER_WIRE_OK)
    {
        return SAP_ERROR;
    }
    if (msg.to_worker < 0 || msg.to_worker > INT32_MAX)
    {
        return SAP_CONFLICT;
    }

    to_worker = (uint32_t)msg.to_worker;
    rc = find_worker_slot(dispatch, to_worker, &slot);
    if (rc != SAP_OK)
    {
        return rc;
    }

    seq = dispatch->next_seq[slot];
    dispatch->next_seq[slot] = seq + 1u;
    rc = sap_runner_v0_inbox_put(dispatch->db, to_worker, seq, frame, frame_len);
    if (rc == SAP_OK)
    {
        dispatch->forwarded++;
    }
    return rc;
}

static void *dispatcher_thread_main(void *arg)
{
    DispatcherCtx *dispatch = (DispatcherCtx *)arg;

    if (!dispatch)
    {
        return NULL;
    }

    while (!dispatcher_stop_requested(dispatch))
    {
        uint32_t drained = 0u;
        int rc;

        if (dispatch->db_gate)
        {
            (void)pthread_mutex_lock(&dispatch->db_gate->mutex);
        }
        rc = sap_runner_outbox_v0_drain(dispatch->db, STRESS_DISPATCH_BATCH, dispatch_outbox_frame,
                                        dispatch, &drained);
        if (dispatch->db_gate)
        {
            (void)pthread_mutex_unlock(&dispatch->db_gate->mutex);
        }

        if (rc == SAP_BUSY || rc == SAP_CONFLICT)
        {
            sleep_ms(STRESS_IDLE_SLEEP_MS);
            continue;
        }
        if (rc != SAP_OK)
        {
            fprintf(stderr, "runner-multiwriter-stress: dispatcher drain rc=%d drained=%u\n", rc,
                    drained);
            dispatch->last_error = rc;
            break;
        }
        if (drained == 0u)
        {
            sleep_ms(STRESS_IDLE_SLEEP_MS);
        }
    }

    return NULL;
}

static int seed_stage1_inbox(DB *db, uint32_t worker_id, uint32_t order_count)
{
    uint32_t i;

    if (!db || worker_id == 0u || order_count == 0u)
    {
        return SAP_ERROR;
    }

    for (i = 0u; i < order_count; i++)
    {
        uint64_t order_id = (uint64_t)i + 1u;
        uint8_t payload[8];
        uint8_t message_id[8];
        uint8_t frame[STRESS_FRAME_CAP];
        uint32_t frame_len = 0u;
        SapRunnerMessageV0 msg = {0};

        wr64be(payload, order_id);
        wr64be(message_id, order_id);

        msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
        msg.flags = SAP_RUNNER_MESSAGE_FLAG_DURABLE;
        msg.to_worker = (int64_t)worker_id;
        msg.route_worker = (int64_t)worker_id;
        msg.route_timestamp = (int64_t)order_id;
        msg.from_worker = 0;
        msg.message_id = message_id;
        msg.message_id_len = sizeof(message_id);
        msg.trace_id = NULL;
        msg.trace_id_len = 0u;
        msg.payload = payload;
        msg.payload_len = sizeof(payload);

        if (sap_runner_message_v0_encode(&msg, frame, sizeof(frame), &frame_len) !=
            SAP_RUNNER_WIRE_OK)
        {
            return SAP_ERROR;
        }
        if (sap_runner_v0_inbox_put(db, worker_id, order_id, frame, frame_len) != SAP_OK)
        {
            return SAP_ERROR;
        }
    }

    return SAP_OK;
}

static int run_round(uint32_t round_index, uint32_t order_count, uint32_t timeout_ms)
{
    DB *db = NULL;
    SapRunnerV0DbGate db_gate;
    StageWorkerCtx workers[STRESS_WORKER_COUNT];
    DispatcherCtx dispatch;
    const uint32_t worker_ids[STRESS_WORKER_COUNT] = {WORKER_STAGE1, WORKER_STAGE2, WORKER_STAGE3,
                                                      WORKER_STAGE4};
    pthread_t dispatch_thread;
    int dispatch_started = 0;
    int db_gate_inited = 0;
    int rc = SAP_ERROR;
    uint32_t i;

    memset(workers, 0, sizeof(workers));
    memset(&dispatch, 0, sizeof(dispatch));

    db = db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        fprintf(stderr, "runner-multiwriter-stress: round=%u db_open failed\n", round_index);
        return SAP_ERROR;
    }
    if (sap_runner_v0_db_gate_init(&db_gate) != SAP_OK)
    {
        fprintf(stderr, "runner-multiwriter-stress: round=%u db gate init failed\n", round_index);
        db_close(db);
        return SAP_ERROR;
    }
    db_gate_inited = 1;

    workers[0].atomic.worker_id = WORKER_STAGE1;
    workers[0].atomic.next_worker_id = WORKER_STAGE2;
    workers[0].atomic.counter_key = k_counter_stage1;
    workers[0].atomic.counter_key_len = sizeof(k_counter_stage1);

    workers[1].atomic.worker_id = WORKER_STAGE2;
    workers[1].atomic.next_worker_id = WORKER_STAGE3;
    workers[1].atomic.counter_key = k_counter_stage2;
    workers[1].atomic.counter_key_len = sizeof(k_counter_stage2);

    workers[2].atomic.worker_id = WORKER_STAGE3;
    workers[2].atomic.next_worker_id = WORKER_STAGE4;
    workers[2].atomic.counter_key = k_counter_stage3;
    workers[2].atomic.counter_key_len = sizeof(k_counter_stage3);

    workers[3].atomic.worker_id = WORKER_STAGE4;
    workers[3].atomic.next_worker_id = 0u;
    workers[3].atomic.counter_key = k_counter_stage4;
    workers[3].atomic.counter_key_len = sizeof(k_counter_stage4);

    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        SapRunnerV0Config cfg = {0};
        SapRunnerAttemptV0Policy policy;
        uint64_t outbox_initial_seq =
            1u + (uint64_t)i * STRESS_OUTBOX_SEQ_STRIDE + (uint64_t)round_index;

        cfg.db = db;
        cfg.worker_id = worker_ids[i];
        cfg.schema_major = 0u;
        cfg.schema_minor = 0u;
        cfg.bootstrap_schema_if_missing = 1;

        if (sap_runner_outbox_v0_publisher_init(&workers[i].outbox, db, outbox_initial_seq) !=
            SAP_OK)
        {
            fprintf(stderr, "runner-multiwriter-stress: round=%u worker[%u] outbox init failed\n",
                    round_index, i);
            goto done;
        }

        if (sap_runner_attempt_handler_v0_init(
                &workers[i].handler, db, stress_atomic_apply, &workers[i].atomic,
                sap_runner_outbox_v0_publish_intent, &workers[i].outbox) != SAP_OK)
        {
            fprintf(stderr, "runner-multiwriter-stress: round=%u worker[%u] handler init failed\n",
                    round_index, i);
            goto done;
        }

        sap_runner_attempt_v0_policy_default(&policy);
        policy.max_retries = 12u;
        policy.initial_backoff_us = 0u;
        policy.max_backoff_us = 0u;
        sap_runner_attempt_handler_v0_set_policy(&workers[i].handler, &policy);

        if (sap_runner_v0_worker_init(&workers[i].worker, &cfg,
                                      sap_runner_attempt_handler_v0_runner_handler,
                                      &workers[i].handler, STRESS_MAX_BATCH) != SAP_OK)
        {
            fprintf(stderr, "runner-multiwriter-stress: round=%u worker[%u] worker init failed\n",
                    round_index, i);
            goto done;
        }
        sap_runner_v0_worker_set_idle_policy(&workers[i].worker, STRESS_IDLE_SLEEP_MS);
        sap_runner_v0_worker_set_db_gate(&workers[i].worker, &db_gate);
        workers[i].inited = 1;
    }

    dispatch.db = db;
    dispatch.db_gate = &db_gate;
    dispatch.forwarded = 0u;
    dispatch.stop_requested = 0;
    dispatch.last_error = SAP_OK;
    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        dispatch.worker_ids[i] = worker_ids[i];
        dispatch.next_seq[i] = 1u;
    }

    if (seed_stage1_inbox(db, WORKER_STAGE1, order_count) != SAP_OK)
    {
        fprintf(stderr, "runner-multiwriter-stress: round=%u seed failed\n", round_index);
        goto done;
    }

    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        if (sap_runner_v0_worker_start(&workers[i].worker) != SAP_OK)
        {
            fprintf(stderr, "runner-multiwriter-stress: round=%u worker[%u] start failed\n",
                    round_index, i);
            goto done;
        }
        workers[i].started = 1;
    }

    if (pthread_create(&dispatch_thread, NULL, dispatcher_thread_main, &dispatch) != 0)
    {
        fprintf(stderr, "runner-multiwriter-stress: round=%u dispatcher start failed\n",
                round_index);
        goto done;
    }
    dispatch_started = 1;

    {
        int64_t deadline_ms = wall_now_ms() + (int64_t)timeout_ms;
        for (;;)
        {
            uint64_t delivered = 0u;

            if (app_state_read_counter(db, k_counter_stage4, sizeof(k_counter_stage4),
                                       &delivered) != SAP_OK)
            {
                fprintf(stderr,
                        "runner-multiwriter-stress: round=%u failed to read stage4 counter\n",
                        round_index);
                goto done;
            }
            if (delivered >= (uint64_t)order_count)
            {
                break;
            }
            if (dispatch.last_error != SAP_OK)
            {
                fprintf(
                    stderr,
                    "runner-multiwriter-stress: round=%u dispatcher error while waiting rc=%d\n",
                    round_index, dispatch.last_error);
                goto done;
            }
            if (wall_now_ms() > deadline_ms)
            {
                uint64_t c1 = 0u;
                uint64_t c2 = 0u;
                uint64_t c3 = 0u;
                uint64_t c4 = 0u;
                uint32_t j;

                for (j = 0u; j < STRESS_WORKER_COUNT; j++)
                {
                    if (workers[j].worker.last_error != SAP_OK)
                    {
                        fprintf(stderr,
                                "runner-multiwriter-stress: round=%u worker[%u] died with "
                                "last_error=%d\n",
                                round_index, j, workers[j].worker.last_error);
                    }
                }

                (void)app_state_read_counter(db, k_counter_stage1, sizeof(k_counter_stage1), &c1);
                (void)app_state_read_counter(db, k_counter_stage2, sizeof(k_counter_stage2), &c2);
                (void)app_state_read_counter(db, k_counter_stage3, sizeof(k_counter_stage3), &c3);
                (void)app_state_read_counter(db, k_counter_stage4, sizeof(k_counter_stage4), &c4);
                fprintf(stderr,
                        "runner-multiwriter-stress: round=%u timeout waiting for stage4=%u"
                        " counters=%" PRIu64 "/%" PRIu64 "/%" PRIu64 "/%" PRIu64 "\n",
                        round_index, order_count, c1, c2, c3, c4);
                goto done;
            }
            sleep_ms(2u);
        }
    }

    rc = SAP_OK;

done:
    dispatcher_request_stop(&dispatch);
    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        if (workers[i].inited)
        {
            sap_runner_v0_worker_request_stop(&workers[i].worker);
        }
    }

    if (dispatch_started)
    {
        if (pthread_join(dispatch_thread, NULL) != 0 && rc == SAP_OK)
        {
            fprintf(stderr, "runner-multiwriter-stress: round=%u dispatcher join failed\n",
                    round_index);
            rc = SAP_ERROR;
        }
        dispatch_started = 0;
    }

    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        if (workers[i].started)
        {
            if (sap_runner_v0_worker_join(&workers[i].worker) != SAP_OK && rc == SAP_OK)
            {
                fprintf(stderr, "runner-multiwriter-stress: round=%u worker[%u] join failed\n",
                        round_index, i);
                rc = SAP_ERROR;
            }
            if (workers[i].worker.last_error != SAP_OK)
            {
                rc = workers[i].worker.last_error;
            }
            workers[i].started = 0;
        }
    }

    if (rc != SAP_OK)
    {
        for (i = 0u; i < STRESS_WORKER_COUNT; i++)
        {
            if (workers[i].inited)
            {
                fprintf(stderr,
                        "runner-multiwriter-stress: round=%u worker[%u] id=%u last_error=%d"
                        " attempts=%u retries=%u last_rc=%d\n",
                        round_index, i, workers[i].atomic.worker_id, workers[i].worker.last_error,
                        workers[i].handler.last_stats.attempts,
                        workers[i].handler.last_stats.retries,
                        workers[i].handler.last_stats.last_rc);
            }
        }
    }

    if (rc == SAP_OK && dispatch.last_error != SAP_OK)
    {
        fprintf(stderr, "runner-multiwriter-stress: round=%u dispatcher last_error=%d\n",
                round_index, dispatch.last_error);
        rc = dispatch.last_error;
    }

    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        if (rc == SAP_OK && workers[i].inited && workers[i].worker.last_error != SAP_OK)
        {
            fprintf(stderr,
                    "runner-multiwriter-stress: round=%u worker[%u] last_error=%d (worker_id=%u)\n",
                    round_index, i, workers[i].worker.last_error, workers[i].atomic.worker_id);
            rc = workers[i].worker.last_error;
        }
    }

    if (rc == SAP_OK)
    {
        uint64_t c1 = 0u;
        uint64_t c2 = 0u;
        uint64_t c3 = 0u;
        uint64_t c4 = 0u;
        if (app_state_read_counter(db, k_counter_stage1, sizeof(k_counter_stage1), &c1) != SAP_OK ||
            app_state_read_counter(db, k_counter_stage2, sizeof(k_counter_stage2), &c2) != SAP_OK ||
            app_state_read_counter(db, k_counter_stage3, sizeof(k_counter_stage3), &c3) != SAP_OK ||
            app_state_read_counter(db, k_counter_stage4, sizeof(k_counter_stage4), &c4) != SAP_OK ||
            c1 != (uint64_t)order_count || c2 != (uint64_t)order_count ||
            c3 != (uint64_t)order_count || c4 != (uint64_t)order_count)
        {
            fprintf(stderr,
                    "runner-multiwriter-stress: round=%u counter mismatch c1=%" PRIu64
                    " c2=%" PRIu64 " c3=%" PRIu64 " c4=%" PRIu64 " expected=%u\n",
                    round_index, c1, c2, c3, c4, order_count);
            rc = SAP_CONFLICT;
        }
    }

    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        if (workers[i].inited)
        {
            sap_runner_v0_worker_shutdown(&workers[i].worker);
        }
    }
    if (db)
    {
        db_close(db);
    }
    if (db_gate_inited)
    {
        sap_runner_v0_db_gate_shutdown(&db_gate);
    }

    return rc;
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

    uint32_t rounds = env_u32("RUNNER_MULTIWRITER_STRESS_ROUNDS", STRESS_DEFAULT_ROUNDS);
    uint32_t orders = env_u32("RUNNER_MULTIWRITER_STRESS_ORDERS", STRESS_DEFAULT_ORDERS);
    uint32_t timeout_ms =
        env_u32("RUNNER_MULTIWRITER_STRESS_TIMEOUT_MS", STRESS_DEFAULT_TIMEOUT_MS);
    uint32_t round;

    for (round = 1u; round <= rounds; round++)
    {
        int rc = run_round(round, orders, timeout_ms);
        if (rc != SAP_OK)
        {
            fprintf(stderr,
                    "runner-multiwriter-stress: FAILED round=%u/%u rc=%d orders=%u timeout_ms=%u\n",
                    round, rounds, rc, orders, timeout_ms);
            return 1;
        }
    }

    printf("runner-multiwriter-stress: OK rounds=%u orders=%u workers=%u\n", rounds, orders,
           STRESS_WORKER_COUNT);
    return 0;
}

#endif /* SAPLING_THREADED */
