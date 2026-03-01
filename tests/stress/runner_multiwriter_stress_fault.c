/*
 * runner_multiwriter_stress_fault.c - fault-injected multi-writer stress test
 *
 * Verifies graceful degradation of the 4-stage runner pipeline under
 * page-alloc failures.  Reuses struct definitions and utility functions
 * from runner_multiwriter_stress.c (keep in sync).
 *
 * SPDX-License-Identifier: MIT
 */
#include "common/fault_inject.h"
#include "generated/wit_schema_dbis.h"
#include "runner/attempt_handler_v0.h"
#include "runner/dedupe_v0.h"
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
    printf("runner-multiwriter-stress-fault: SAPLING_THREADED required (skipped)\n");
    return 0;
}
#else

/* ------------------------------------------------------------------ */
/* Constants — keep in sync with runner_multiwriter_stress.c           */
/* ------------------------------------------------------------------ */

#define STRESS_WORKER_COUNT 4u
#define STRESS_MAX_BATCH 32u
#define STRESS_DISPATCH_BATCH 128u
#define STRESS_IDLE_SLEEP_MS 1u
#define STRESS_FRAME_CAP 256u
#define STRESS_OUTBOX_SEQ_STRIDE 1000000000ULL
#define STRESS_DBI_COUNTERS 7u

#define FAULT_DEFAULT_ROUNDS 4u
#define FAULT_DEFAULT_ORDERS 32u
#define FAULT_DEFAULT_TIMEOUT_MS 8000u
#define FAULT_DEFAULT_FAIL_PCT 25u
#define FAULT_DEFAULT_CORRUPTION_THRESHOLD 0u

#define WORKER_STAGE1 101u
#define WORKER_STAGE2 102u
#define WORKER_STAGE3 103u
#define WORKER_STAGE4 104u

static const uint8_t k_counter_stage1[] = {'s', 't', 'a', 'g', 'e', '.', '1'};
static const uint8_t k_counter_stage2[] = {'s', 't', 'a', 'g', 'e', '.', '2'};
static const uint8_t k_counter_stage3[] = {'s', 't', 'a', 'g', 'e', '.', '3'};
static const uint8_t k_counter_stage4[] = {'s', 't', 'a', 'g', 'e', '.', '4'};

/* ------------------------------------------------------------------ */
/* Struct definitions — keep in sync with runner_multiwriter_stress.c  */
/* ------------------------------------------------------------------ */

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

/* ------------------------------------------------------------------ */
/* Utility functions — keep in sync with runner_multiwriter_stress.c   */
/* ------------------------------------------------------------------ */

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

/* ------------------------------------------------------------------ */
/* Counter helpers — keep in sync with runner_multiwriter_stress.c     */
/* ------------------------------------------------------------------ */

static int app_state_read_counter(DB *db, const uint8_t *key, uint32_t key_len, uint64_t *value_out)
{
    Txn *txn = NULL;
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !key || key_len == 0u || !value_out)
    {
        return ERR_CORRUPT;
    }
    *value_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_CORRUPT;
    }

    rc = txn_get_dbi(txn, STRESS_DBI_COUNTERS, key, key_len, &val, &val_len);
    if (rc == ERR_NOT_FOUND)
    {
        txn_abort(txn);
        return ERR_OK;
    }
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (!val || val_len != 8u)
    {
        txn_abort(txn);
        return ERR_CONFLICT;
    }

    *value_out = rd64be((const uint8_t *)val);
    txn_abort(txn);
    return ERR_OK;
}

static int txstack_read_counter(SapRunnerTxStackV0 *stack, Txn *read_txn, const uint8_t *key,
                                uint32_t key_len, uint64_t *value_out)
{
    const void *cur = NULL;
    uint32_t cur_len = 0u;
    int rc;

    if (!stack || !read_txn || !key || key_len == 0u || !value_out)
    {
        return ERR_CORRUPT;
    }

    rc = sap_runner_txstack_v0_read_dbi(stack, read_txn, STRESS_DBI_COUNTERS, key, key_len, &cur,
                                        &cur_len);
    if (rc == ERR_NOT_FOUND)
    {
        *value_out = 0u;
        return ERR_OK;
    }
    if (rc != ERR_OK)
    {
        return rc;
    }
    if (!cur || cur_len != 8u)
    {
        return ERR_CONFLICT;
    }

    *value_out = rd64be((const uint8_t *)cur);
    return ERR_OK;
}

static int txstack_key_exists(SapRunnerTxStackV0 *stack, Txn *read_txn, uint32_t dbi,
                              const uint8_t *key, uint32_t key_len, int *exists_out)
{
    const void *cur = NULL;
    uint32_t cur_len = 0u;
    int rc;

    if (!stack || !read_txn || !key || key_len == 0u || !exists_out)
    {
        return ERR_CORRUPT;
    }
    *exists_out = 0;

    rc = sap_runner_txstack_v0_read_dbi(stack, read_txn, dbi, key, key_len, &cur, &cur_len);
    if (rc == ERR_NOT_FOUND)
    {
        *exists_out = 0;
        return ERR_OK;
    }
    if (rc == ERR_OK)
    {
        *exists_out = 1;
        return ERR_OK;
    }
    return rc;
}

static int txstack_stage_counter(SapRunnerTxStackV0 *stack, const uint8_t *key, uint32_t key_len,
                                 uint64_t value)
{
    uint8_t raw[8];

    if (!stack || !key || key_len == 0u)
    {
        return ERR_CORRUPT;
    }

    wr64be(raw, value);
    return sap_runner_txstack_v0_stage_put_dbi(stack, STRESS_DBI_COUNTERS, key, key_len, raw,
                                               sizeof(raw));
}

/* ------------------------------------------------------------------ */
/* Message encoding — keep in sync with runner_multiwriter_stress.c    */
/* ------------------------------------------------------------------ */

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
        return ERR_CORRUPT;
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
        return ERR_OK;
    }
    if (wire_rc == SAP_RUNNER_WIRE_E2BIG)
    {
        return ERR_FULL;
    }
    return ERR_CORRUPT;
}

/* ------------------------------------------------------------------ */
/* Atomic apply — keep in sync with runner_multiwriter_stress.c        */
/* ------------------------------------------------------------------ */

static int stress_atomic_apply(SapRunnerTxStackV0 *stack, Txn *read_txn, SapRunnerV0 *runner,
                               const SapRunnerMessageV0 *msg, void *ctx)
{
    StageAtomicCtx *stage = (StageAtomicCtx *)ctx;
    uint64_t order_id;
    uint8_t dedupe_key[12];
    int seen = 0;
    uint64_t counter = 0u;
    int rc;

    (void)runner;
    if (!stack || !read_txn || !msg || !stage || !msg->payload || msg->payload_len != 8u)
    {
        return ERR_CORRUPT;
    }

    order_id = rd64be(msg->payload);
    wr32be(dedupe_key, stage->worker_id);
    wr64be(dedupe_key + 4u, order_id);

    rc = txstack_key_exists(stack, read_txn, SAP_WIT_DBI_DEDUPE, dedupe_key, sizeof(dedupe_key),
                            &seen);
    if (rc != ERR_OK)
    {
        return rc;
    }
    if (seen)
    {
        return ERR_OK;
    }

    rc =
        txstack_read_counter(stack, read_txn, stage->counter_key, stage->counter_key_len, &counter);
    if (rc != ERR_OK)
    {
        return rc;
    }
    if (counter == UINT64_MAX)
    {
        return ERR_FULL;
    }
    rc = txstack_stage_counter(stack, stage->counter_key, stage->counter_key_len, counter + 1u);
    if (rc != ERR_OK)
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
        if (rc != ERR_OK)
        {
            return rc;
        }

        intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
        intent.flags = 0u;
        intent.due_ts = 0;
        intent.message = frame;
        intent.message_len = frame_len;
        rc = sap_runner_txstack_v0_push_intent(stack, &intent);
        if (rc != ERR_OK)
        {
            return rc;
        }
    }

    {
        SapRunnerDedupeV0 dd = {0};
        dd.accepted = 1;
        rc = sap_runner_dedupe_v0_stage_put(stack, dedupe_key, sizeof(dedupe_key), &dd);
        if (rc != ERR_OK)
        {
            return rc;
        }
    }

    return ERR_OK;
}

/* ------------------------------------------------------------------ */
/* Dispatcher — keep in sync with runner_multiwriter_stress.c          */
/* ------------------------------------------------------------------ */

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
        return ERR_CORRUPT;
    }
    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        if (ctx->worker_ids[i] == worker_id)
        {
            *slot_out = i;
            return ERR_OK;
        }
    }
    return ERR_NOT_FOUND;
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
        return ERR_CORRUPT;
    }
    if (sap_runner_message_v0_decode(frame, frame_len, &msg) != SAP_RUNNER_WIRE_OK)
    {
        return ERR_CORRUPT;
    }
    if (msg.to_worker < 0 || msg.to_worker > INT32_MAX)
    {
        return ERR_CONFLICT;
    }

    to_worker = (uint32_t)msg.to_worker;
    rc = find_worker_slot(dispatch, to_worker, &slot);
    if (rc != ERR_OK)
    {
        return rc;
    }

    seq = dispatch->next_seq[slot];
    dispatch->next_seq[slot] = seq + 1u;
    rc = sap_runner_v0_inbox_put(dispatch->db, to_worker, seq, frame, frame_len);
    if (rc == ERR_OK)
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

        if (rc == ERR_BUSY || rc == ERR_CONFLICT)
        {
            sleep_ms(STRESS_IDLE_SLEEP_MS);
            continue;
        }
        if (rc != ERR_OK)
        {
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
        return ERR_CORRUPT;
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
            return ERR_CORRUPT;
        }
        if (sap_runner_v0_inbox_put(db, worker_id, order_id, frame, frame_len) != ERR_OK)
        {
            return ERR_CORRUPT;
        }
    }

    return ERR_OK;
}

/* ------------------------------------------------------------------ */
/* Fault-injected round                                                */
/* ------------------------------------------------------------------ */

static int run_round_fault(uint32_t round_index, uint32_t order_count, uint32_t timeout_ms,
                           uint32_t fail_pct, uint32_t corruption_threshold)
{
    DB *db = NULL;
    SapRunnerV0DbGate db_gate;
    SapFaultInjector fi;
    StageWorkerCtx workers[STRESS_WORKER_COUNT];
    DispatcherCtx dispatch;
    const uint32_t worker_ids[STRESS_WORKER_COUNT] = {WORKER_STAGE1, WORKER_STAGE2, WORKER_STAGE3,
                                                      WORKER_STAGE4};
    pthread_t dispatch_thread;
    int dispatch_started = 0;
    int db_gate_inited = 0;
    int fi_attached = 0;
    int rc = ERR_CORRUPT;
    uint32_t i;

    memset(workers, 0, sizeof(workers));
    memset(&dispatch, 0, sizeof(dispatch));
    sap_fi_reset(&fi);

    db = db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        fprintf(stderr, "fault: round=%u db_open failed\n", round_index);
        return ERR_CORRUPT;
    }
    if (sap_runner_v0_db_gate_init(&db_gate) != ERR_OK)
    {
        fprintf(stderr, "fault: round=%u db gate init failed\n", round_index);
        db_close(db);
        return ERR_CORRUPT;
    }
    db_gate_inited = 1;

    if (dbi_open(db, STRESS_DBI_COUNTERS, NULL, NULL, 0u) != ERR_OK)
    {
        fprintf(stderr, "fault: round=%u dbi_open(%u) failed\n", round_index, STRESS_DBI_COUNTERS);
        sap_runner_v0_db_gate_shutdown(&db_gate);
        db_close(db);
        return ERR_CORRUPT;
    }

    /* --- Worker configuration --- */

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
            ERR_OK)
        {
            fprintf(stderr, "fault: round=%u worker[%u] outbox init failed\n", round_index, i);
            goto done;
        }

        if (sap_runner_attempt_handler_v0_init(
                &workers[i].handler, db, stress_atomic_apply, &workers[i].atomic,
                sap_runner_outbox_v0_publish_intent, &workers[i].outbox) != ERR_OK)
        {
            fprintf(stderr, "fault: round=%u worker[%u] handler init failed\n", round_index, i);
            goto done;
        }

        sap_runner_attempt_v0_policy_default(&policy);
        policy.max_retries = 12u;
        policy.initial_backoff_us = 0u;
        policy.max_backoff_us = 0u;
        sap_runner_attempt_handler_v0_set_policy(&workers[i].handler, &policy);

        if (sap_runner_v0_worker_init(&workers[i].worker, &cfg,
                                      sap_runner_attempt_handler_v0_runner_handler,
                                      &workers[i].handler, STRESS_MAX_BATCH) != ERR_OK)
        {
            fprintf(stderr, "fault: round=%u worker[%u] worker init failed\n", round_index, i);
            goto done;
        }
        sap_runner_v0_worker_set_idle_policy(&workers[i].worker, STRESS_IDLE_SLEEP_MS);
        sap_runner_v0_worker_set_db_gate(&workers[i].worker, &db_gate);
        workers[i].inited = 1;
    }

    /* --- Dispatcher setup --- */

    dispatch.db = db;
    dispatch.db_gate = &db_gate;
    dispatch.forwarded = 0u;
    dispatch.stop_requested = 0;
    dispatch.last_error = ERR_OK;
    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        dispatch.worker_ids[i] = worker_ids[i];
        dispatch.next_seq[i] = 1u;
    }

    /* --- Seed inbox (fault injection OFF) --- */

    if (seed_stage1_inbox(db, WORKER_STAGE1, order_count) != ERR_OK)
    {
        fprintf(stderr, "fault: round=%u seed failed\n", round_index);
        goto done;
    }

    /* --- Attach fault injector --- */

    if (sap_fi_add_rate_rule(&fi, "alloc.page", fail_pct) != 0)
    {
        fprintf(stderr, "fault: round=%u fi add_rate_rule failed\n", round_index);
        goto done;
    }
    if (sap_db_set_fault_injector((struct SapEnv *)db, &fi) != ERR_OK)
    {
        fprintf(stderr, "fault: round=%u set_fault_injector failed\n", round_index);
        goto done;
    }
    fi_attached = 1;

    /* --- Start workers and dispatcher --- */

    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        if (sap_runner_v0_worker_start(&workers[i].worker) != ERR_OK)
        {
            fprintf(stderr, "fault: round=%u worker[%u] start failed\n", round_index, i);
            goto done;
        }
        workers[i].started = 1;
    }

    if (pthread_create(&dispatch_thread, NULL, dispatcher_thread_main, &dispatch) != 0)
    {
        fprintf(stderr, "fault: round=%u dispatcher start failed\n", round_index);
        goto done;
    }
    dispatch_started = 1;

    /* --- Poll loop --- */

    {
        int64_t deadline_ms = wall_now_ms() + (int64_t)timeout_ms;
        int all_dead;

        for (;;)
        {
            uint64_t delivered = 0u;

            (void)app_state_read_counter(db, k_counter_stage4, sizeof(k_counter_stage4),
                                         &delivered);
            if (delivered >= (uint64_t)order_count)
            {
                break;
            }

            /* Early exit if all workers have died */
            all_dead = 1;
            for (i = 0u; i < STRESS_WORKER_COUNT; i++)
            {
                if (workers[i].started && workers[i].worker.last_error == ERR_OK)
                {
                    all_dead = 0;
                    break;
                }
            }
            if (all_dead)
            {
                fprintf(stderr, "fault: round=%u all workers died, stage4=%" PRIu64 "/%u\n",
                        round_index, delivered, order_count);
                break;
            }

            if (wall_now_ms() > deadline_ms)
            {
                uint64_t c1 = 0u, c2 = 0u, c3 = 0u, c4 = 0u;
                (void)app_state_read_counter(db, k_counter_stage1, sizeof(k_counter_stage1), &c1);
                (void)app_state_read_counter(db, k_counter_stage2, sizeof(k_counter_stage2), &c2);
                (void)app_state_read_counter(db, k_counter_stage3, sizeof(k_counter_stage3), &c3);
                (void)app_state_read_counter(db, k_counter_stage4, sizeof(k_counter_stage4), &c4);
                fprintf(stderr,
                        "fault: round=%u timeout counters=%" PRIu64 "/%" PRIu64 "/%" PRIu64
                        "/%" PRIu64 " expected=%u\n",
                        round_index, c1, c2, c3, c4, order_count);
                break;
            }
            sleep_ms(2u);
        }
    }

    rc = ERR_OK;

done:
    /* --- Detach fault injector before teardown writes --- */
    if (fi_attached)
    {
        sap_db_set_fault_injector((struct SapEnv *)db, NULL);
        fi_attached = 0;
    }

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
        (void)pthread_join(dispatch_thread, NULL);
        dispatch_started = 0;
    }

    for (i = 0u; i < STRESS_WORKER_COUNT; i++)
    {
        if (workers[i].started)
        {
            (void)sap_runner_v0_worker_join(&workers[i].worker);
            workers[i].started = 0;
        }
    }

    /* --- Verification --- */

    /* 1. Require stage4 > 0: the pipeline must make forward progress */
    if (rc == ERR_OK)
    {
        uint64_t c4 = 0u;
        (void)app_state_read_counter(db, k_counter_stage4, sizeof(k_counter_stage4), &c4);
        if (c4 == 0u)
        {
            fprintf(stderr, "fault: round=%u FAILED: stage4=0 (no forward progress)\n",
                    round_index);
            rc = ERR_CORRUPT;
        }
        else
        {
            uint64_t c1 = 0u, c2 = 0u, c3 = 0u;
            (void)app_state_read_counter(db, k_counter_stage1, sizeof(k_counter_stage1), &c1);
            (void)app_state_read_counter(db, k_counter_stage2, sizeof(k_counter_stage2), &c2);
            (void)app_state_read_counter(db, k_counter_stage3, sizeof(k_counter_stage3), &c3);
            printf("  round=%u counters=%" PRIu64 "/%" PRIu64 "/%" PRIu64 "/%" PRIu64
                   " expected=%u\n",
                   round_index, c1, c2, c3, c4, order_count);
        }
    }

    /* 2. Worker error classification: ERR_OOM and ERR_BUSY are expected,
     *    anything else is a hard failure */
    if (rc == ERR_OK)
    {
        for (i = 0u; i < STRESS_WORKER_COUNT; i++)
        {
            int err = workers[i].worker.last_error;
            if (err != ERR_OK && err != ERR_OOM && err != ERR_BUSY)
            {
                fprintf(stderr,
                        "fault: round=%u worker[%u] unexpected last_error=%d (id=%u)\n",
                        round_index, i, err, workers[i].atomic.worker_id);
                rc = err;
            }
            else if (err != ERR_OK)
            {
                printf("  round=%u worker[%u] expected error=%d (id=%u)\n",
                       round_index, i, err, workers[i].atomic.worker_id);
            }
        }
    }

    /* 3. Corruption stats — thresholded enforcement */
    if (rc == ERR_OK)
    {
        SapCorruptionStats cstats;
        if (sap_db_corruption_stats((struct SapEnv *)db, &cstats) == ERR_OK)
        {
            uint64_t total = cstats.free_list_head_reset + cstats.free_list_next_dropped +
                             cstats.leaf_insert_bounds_reject + cstats.abort_loop_limit_hit +
                             cstats.abort_bounds_break;
            printf("  round=%u corruption_stats: total=%" PRIu64
                   " head_reset=%" PRIu64 " next_dropped=%" PRIu64 " leaf_reject=%" PRIu64
                   " abort_limit=%" PRIu64 " abort_bounds=%" PRIu64 "\n",
                   round_index, total, cstats.free_list_head_reset,
                   cstats.free_list_next_dropped, cstats.leaf_insert_bounds_reject,
                   cstats.abort_loop_limit_hit, cstats.abort_bounds_break);

            if (total > (uint64_t)corruption_threshold)
            {
                fprintf(stderr,
                        "fault: round=%u CORRUPTION total=%" PRIu64 " > threshold=%u\n",
                        round_index, total, corruption_threshold);
                rc = ERR_CORRUPT;
            }
        }
    }

    /* 4. Free-list integrity — hard failure */
    if (rc == ERR_OK)
    {
        SapFreelistCheckResult fl;
        if (sap_db_freelist_check((struct SapEnv *)db, &fl) == ERR_OK)
        {
            if (fl.out_of_bounds || fl.null_backing || fl.cycle_detected)
            {
                fprintf(stderr,
                        "fault: round=%u FREE-LIST FAILURE oob=%u null=%u cycle=%u\n",
                        round_index, fl.out_of_bounds, fl.null_backing, fl.cycle_detected);
                rc = ERR_CORRUPT;
            }
        }
    }

    /* 5. Faults must have actually fired */
    if (rc == ERR_OK)
    {
        printf("  round=%u fi: hits=%u fails=%u\n",
               round_index, fi.rules[0].hit_count, fi.rules[0].fail_count);
        if (fi.rules[0].fail_count == 0u)
        {
            fprintf(stderr, "fault: round=%u FAILED: no faults injected (fail_count=0)\n",
                    round_index);
            rc = ERR_CORRUPT;
        }
    }

    /* --- Cleanup --- */

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

/* ------------------------------------------------------------------ */
/* Main                                                                */
/* ------------------------------------------------------------------ */

int main(void)
{
    SapArenaOptions g_alloc_opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free,
        .cfg.custom.ctx = NULL
    };
    sap_arena_init(&g_alloc, &g_alloc_opts);

    uint32_t rounds = env_u32("RUNNER_MULTIWRITER_STRESS_FAULT_ROUNDS", FAULT_DEFAULT_ROUNDS);
    uint32_t orders = env_u32("RUNNER_MULTIWRITER_STRESS_FAULT_ORDERS", FAULT_DEFAULT_ORDERS);
    uint32_t timeout_ms =
        env_u32("RUNNER_MULTIWRITER_STRESS_FAULT_TIMEOUT_MS", FAULT_DEFAULT_TIMEOUT_MS);
    uint32_t fail_pct =
        env_u32("RUNNER_MULTIWRITER_STRESS_FAULT_FAIL_PCT", FAULT_DEFAULT_FAIL_PCT);
    uint32_t corruption_threshold = env_u32("RUNNER_MULTIWRITER_STRESS_FAULT_CORRUPTION_THRESHOLD",
                                            FAULT_DEFAULT_CORRUPTION_THRESHOLD);
    uint32_t round;

    printf("runner-multiwriter-stress-fault: rounds=%u orders=%u timeout=%u fail_pct=%u "
           "corruption_threshold=%u\n",
           rounds, orders, timeout_ms, fail_pct, corruption_threshold);

    for (round = 1u; round <= rounds; round++)
    {
        int rc = run_round_fault(round, orders, timeout_ms, fail_pct, corruption_threshold);
        if (rc != ERR_OK)
        {
            fprintf(stderr,
                    "runner-multiwriter-stress-fault: FAILED round=%u/%u rc=%d\n",
                    round, rounds, rc);
            return 1;
        }
    }

    printf("runner-multiwriter-stress-fault: PASSED rounds=%u orders=%u fail_pct=%u\n",
           rounds, orders, fail_pct);
    return 0;
}

#endif /* SAPLING_THREADED */
