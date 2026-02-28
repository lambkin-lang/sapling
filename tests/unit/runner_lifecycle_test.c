/*
 * runner_lifecycle_test.c - tests for phase-A runner lifecycle scaffolding
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/runner_v0.h"
#include "runner/mailbox_v0.h"
#include "runner/timer_v0.h"
#ifndef SAP_WIT_SCHEMA_DBIS_H
#include "generated/wit_schema_dbis.h"
#endif

#include "sapling/bept.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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
    uint32_t invocations;
    uint32_t calls;
    int64_t last_to_worker;
    uint32_t fail_calls_remaining;
    int fail_rc;
} TestDispatchCtx;

typedef struct
{
    uint8_t kind;
    uint64_t seq;
    int32_t rc;
    uint32_t frame_len;
    uint8_t frame[128];
} ReplayEventLogEntry;

typedef struct
{
    uint32_t count;
    ReplayEventLogEntry events[32];
} ReplayHookCtx;

typedef struct
{
    uint32_t count;
    SapRunnerV0Metrics last;
} MetricsSinkCtx;

typedef struct
{
    uint32_t count;
    SapRunnerV0LogEvent events[32];
} LogSinkCtx;

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

static void timer_to_bept_key(int64_t due_ts, uint64_t seq, uint32_t out_key[4]) {
    /* Flip sign bit of signed int64 to sort correctly as unsigned */
    uint64_t ts_encoded = (uint64_t)due_ts ^ 0x8000000000000000ULL;
    
    out_key[0] = (uint32_t)(ts_encoded >> 32);
    out_key[1] = (uint32_t)(ts_encoded & 0xFFFFFFFF);
    out_key[2] = (uint32_t)(seq >> 32);
    out_key[3] = (uint32_t)(seq & 0xFFFFFFFF);
}

static DB *new_db(void) { 
    DB *db = db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL); 
    if (db) {
        (void)sap_bept_subsystem_init((SapEnv *)db);
    }
    return db;
}

static int on_message(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg, void *ctx)
{
    TestDispatchCtx *state = (TestDispatchCtx *)ctx;
    (void)runner;
    if (!state || !msg)
    {
        return SAP_ERROR;
    }
    state->invocations++;
    if (state->fail_calls_remaining > 0u)
    {
        state->fail_calls_remaining--;
        return state->fail_rc;
    }
    state->calls++;
    state->last_to_worker = msg->to_worker;
    return SAP_OK;
}

static void on_replay_event(const SapRunnerV0ReplayEvent *event, void *ctx)
{
    ReplayHookCtx *log = (ReplayHookCtx *)ctx;
    ReplayEventLogEntry *dst;

    if (!event || !log || log->count >= 32u)
    {
        return;
    }
    dst = &log->events[log->count];
    dst->kind = event->kind;
    dst->seq = event->seq;
    dst->rc = event->rc;
    dst->frame_len = 0u;
    if (event->frame && event->frame_len > 0u && event->frame_len <= sizeof(dst->frame))
    {
        memcpy(dst->frame, event->frame, event->frame_len);
        dst->frame_len = event->frame_len;
    }
    log->count++;
}

static void on_metrics_event(const SapRunnerV0Metrics *metrics, void *ctx)
{
    MetricsSinkCtx *sink = (MetricsSinkCtx *)ctx;

    if (!metrics || !sink)
    {
        return;
    }
    sink->last = *metrics;
    sink->count++;
}

static void on_log_event(const SapRunnerV0LogEvent *event, void *ctx)
{
    LogSinkCtx *sink = (LogSinkCtx *)ctx;

    if (!event || !sink || sink->count >= 32u)
    {
        return;
    }
    sink->events[sink->count] = *event;
    sink->count++;
}

static int encode_test_message(uint32_t to_worker, uint8_t *buf, uint32_t buf_len,
                               uint32_t *out_len)
{
    const uint8_t msg_id[] = {'m', '1'};
    const uint8_t payload[] = {'o', 'k'};
    SapRunnerMessageV0 msg = {0};

    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
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
    msg.payload_len = sizeof(payload);
    return sap_runner_message_v0_encode(&msg, buf, buf_len, out_len);
}

static int inbox_entry_exists(DB *db, uint64_t worker_id, uint64_t seq, int *exists_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !exists_out)
    {
        return SAP_ERROR;
    }

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), &val, &val_len);
    txn_abort(txn);

    if (rc == SAP_OK)
    {
        *exists_out = 1;
        return SAP_OK;
    }
    if (rc == SAP_NOTFOUND)
    {
        *exists_out = 0;
        return SAP_OK;
    }
    return rc;
}

static int timer_entry_exists(DB *db, int64_t due_ts, uint64_t seq, int *exists_out)
{
    Txn *txn;
    uint32_t bept_key[4];
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !exists_out)
    {
        return SAP_ERROR;
    }

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    timer_to_bept_key(due_ts, seq, bept_key);
    rc = sap_bept_get((Txn *)txn, bept_key, 4, &val, &val_len);
    txn_abort(txn);

    if (rc == SAP_OK)
    {
        *exists_out = 1;
        return SAP_OK;
    }
    if (rc == SAP_NOTFOUND)
    {
        *exists_out = 0;
        return SAP_OK;
    }
    return rc;
}

static int lease_entry_exists(DB *db, uint64_t worker_id, uint64_t seq, int *exists_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !exists_out)
    {
        return SAP_ERROR;
    }

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), &val, &val_len);
    txn_abort(txn);

    if (rc == SAP_OK)
    {
        *exists_out = 1;
        return SAP_OK;
    }
    if (rc == SAP_NOTFOUND)
    {
        *exists_out = 0;
        return SAP_OK;
    }
    return rc;
}

static int count_worker_entries(DB *db, uint32_t dbi, uint64_t worker_id, uint32_t *count_out)
{
    Txn *txn;
    Cursor *cur;
    uint8_t prefix[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint32_t count = 0u;
    int rc;

    if (!db || !count_out)
    {
        return SAP_ERROR;
    }
    *count_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    cur = cursor_open_dbi(txn, dbi);
    if (!cur)
    {
        txn_abort(txn);
        return SAP_ERROR;
    }

    sap_runner_v0_inbox_key_encode(worker_id, 0u, prefix);
    rc = cursor_seek_prefix(cur, prefix, 8u);
    if (rc == SAP_NOTFOUND)
    {
        cursor_close(cur);
        txn_abort(txn);
        return SAP_OK;
    }
    if (rc != SAP_OK)
    {
        cursor_close(cur);
        txn_abort(txn);
        return rc;
    }

    for (;;)
    {
        const void *key = NULL;
        const void *val = NULL;
        uint32_t key_len = 0u;
        uint32_t val_len = 0u;
        uint64_t found_worker = 0u;
        uint64_t found_seq = 0u;

        rc = cursor_get(cur, &key, &key_len, &val, &val_len);
        (void)val;
        (void)val_len;
        if (rc != SAP_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }

        rc = sap_runner_v0_inbox_key_decode((const uint8_t *)key, key_len, &found_worker,
                                            &found_seq);
        (void)found_seq;
        if (rc != SAP_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }
        if (found_worker != worker_id)
        {
            break;
        }
        count++;

        rc = cursor_next(cur);
        if (rc == SAP_NOTFOUND)
        {
            break;
        }
        if (rc != SAP_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }
    }

    cursor_close(cur);
    txn_abort(txn);
    *count_out = count;
    return SAP_OK;
}

static int test_inbox_key_codec(void)
{
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint64_t worker = 0u;
    uint64_t seq = 0u;

    sap_runner_v0_inbox_key_encode(7u, 99u, key);
    CHECK(sap_runner_v0_inbox_key_decode(key, sizeof(key), &worker, &seq) == SAP_OK);
    CHECK(worker == 7u);
    CHECK(seq == 99u);
    CHECK(sap_runner_v0_inbox_key_decode(key, sizeof(key) - 1u, &worker, &seq) == SAP_ERROR);
    return 0;
}

static int test_schema_bootstrap_and_guard(void)
{
    DB *db = new_db();
    CHECK(db != NULL);

    CHECK(sap_runner_v0_bootstrap_dbis(db) == SAP_OK);
    CHECK(sap_runner_v0_ensure_schema_version(db, 0u, 0u, 1) == SAP_OK);
    CHECK(sap_runner_v0_ensure_schema_version(db, 0u, 0u, 0) == SAP_OK);
    CHECK(sap_runner_v0_ensure_schema_version(db, 0u, 1u, 0) == SAP_CONFLICT);

    db_close(db);
    return 0;
}

static int test_runner_init_and_step(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);
    CHECK(runner.state == SAP_RUNNER_V0_STATE_RUNNING);
    CHECK(runner.steps_completed == 0u);

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_run_step(&runner, frame, frame_len, on_message, &dispatch_state) == SAP_OK);
    CHECK(dispatch_state.calls == 1u);
    CHECK(dispatch_state.last_to_worker == 7);
    CHECK(runner.steps_completed == 1u);

    CHECK(encode_test_message(8u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_run_step(&runner, frame, frame_len, on_message, &dispatch_state) ==
          SAP_NOTFOUND);
    CHECK(dispatch_state.calls == 1u);
    CHECK(runner.steps_completed == 1u);

    sap_runner_v0_shutdown(&runner);
    CHECK(sap_runner_v0_run_step(&runner, frame, frame_len, on_message, &dispatch_state) ==
          SAP_BUSY);

    db_close(db);
    return 0;
}

static int test_runner_poll_inbox(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 10u, frame, frame_len) == SAP_OK);
    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 11u, frame, frame_len) == SAP_OK);
    CHECK(encode_test_message(8u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 8u, 1u, frame, frame_len) == SAP_OK);

    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch_state, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch_state.calls == 1u);
    CHECK(runner.steps_completed == 1u);

    CHECK(inbox_entry_exists(db, 7u, 10u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(inbox_entry_exists(db, 7u, 11u, &exists) == SAP_OK);
    CHECK(exists == 1);

    CHECK(sap_runner_v0_poll_inbox(&runner, 10u, on_message, &dispatch_state, &processed) ==
          SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch_state.calls == 2u);
    CHECK(runner.steps_completed == 2u);
    CHECK(inbox_entry_exists(db, 7u, 11u, &exists) == SAP_OK);
    CHECK(exists == 0);

    CHECK(inbox_entry_exists(db, 8u, 1u, &exists) == SAP_OK);
    CHECK(exists == 1);

    CHECK(sap_runner_v0_poll_inbox(&runner, 10u, on_message, &dispatch_state, &processed) ==
          SAP_OK);
    CHECK(processed == 0u);

    db_close(db);
    return 0;
}

static int test_poll_inbox_retryable_requeues_and_recovers(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    dispatch_state.fail_calls_remaining = 1u;
    dispatch_state.fail_rc = SAP_CONFLICT;

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len) == SAP_OK);
    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 2u, frame, frame_len) == SAP_OK);

    CHECK(sap_runner_v0_poll_inbox(&runner, 4u, on_message, &dispatch_state, &processed) == SAP_OK);
    CHECK(processed == 2u);
    CHECK(dispatch_state.invocations == 3u);
    CHECK(dispatch_state.calls == 2u);
    CHECK(runner.steps_completed == 2u);

    CHECK(inbox_entry_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(inbox_entry_exists(db, 7u, 2u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(inbox_entry_exists(db, 7u, 3u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(lease_entry_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(lease_entry_exists(db, 7u, 2u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(lease_entry_exists(db, 7u, 3u, &exists) == SAP_OK);
    CHECK(exists == 0);

    db_close(db);
    return 0;
}

static int test_poll_inbox_non_retryable_requeues_and_returns_error(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    dispatch_state.fail_calls_remaining = 1u;
    dispatch_state.fail_rc = SAP_ERROR;

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 10u, frame, frame_len) == SAP_OK);

    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch_state, &processed) ==
          SAP_ERROR);
    CHECK(processed == 0u);
    CHECK(dispatch_state.invocations == 1u);
    CHECK(dispatch_state.calls == 0u);
    CHECK(runner.steps_completed == 0u);

    CHECK(inbox_entry_exists(db, 7u, 10u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(inbox_entry_exists(db, 7u, 11u, &exists) == SAP_OK);
    CHECK(exists == 1);
    CHECK(lease_entry_exists(db, 7u, 10u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(lease_entry_exists(db, 7u, 11u, &exists) == SAP_OK);
    CHECK(exists == 0);

    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch_state, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch_state.invocations == 2u);
    CHECK(dispatch_state.calls == 1u);
    CHECK(runner.steps_completed == 1u);

    db_close(db);
    return 0;
}

static int test_retry_budget_moves_to_dead_letter(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    uint32_t inbox_count = 0u;
    uint32_t dead_letter_count = 0u;
    uint32_t rounds;
    int rc = SAP_OK;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    dispatch_state.fail_calls_remaining = 32u;
    dispatch_state.fail_rc = SAP_CONFLICT;

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 50u, frame, frame_len) == SAP_OK);

    for (rounds = 0u; rounds < 16u; rounds++)
    {
        rc = sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch_state, &processed);
        CHECK(rc == SAP_OK);
        CHECK(processed == 0u);

        CHECK(count_worker_entries(db, SAP_WIT_DBI_DEAD_LETTER, 7u, &dead_letter_count) == SAP_OK);
        if (dead_letter_count > 0u)
        {
            break;
        }
    }

    CHECK(dead_letter_count == 1u);
    CHECK(count_worker_entries(db, SAP_WIT_DBI_INBOX, 7u, &inbox_count) == SAP_OK);
    CHECK(inbox_count == 0u);

    db_close(db);
    return 0;
}

static int test_runner_policy_override_retry_budget(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    SapRunnerV0Policy policy;
    SapRunnerV0Metrics metrics = {0};
    TestDispatchCtx dispatch_state = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    uint32_t dead_letter_count = 0u;
    uint32_t inbox_count = 0u;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    sap_runner_v0_policy_default(&policy);
    policy.retry_budget_max = 1u;
    sap_runner_v0_set_policy(&runner, &policy);

    dispatch_state.fail_calls_remaining = 8u;
    dispatch_state.fail_rc = SAP_CONFLICT;
    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 60u, frame, frame_len) == SAP_OK);

    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch_state, &processed) == SAP_OK);
    CHECK(processed == 0u);
    CHECK(count_worker_entries(db, SAP_WIT_DBI_DEAD_LETTER, 7u, &dead_letter_count) == SAP_OK);
    CHECK(dead_letter_count == 1u);
    CHECK(count_worker_entries(db, SAP_WIT_DBI_INBOX, 7u, &inbox_count) == SAP_OK);
    CHECK(inbox_count == 0u);

    sap_runner_v0_metrics_snapshot(&runner, &metrics);
    CHECK(metrics.retryable_failures == 1u);
    CHECK(metrics.requeues == 0u);
    CHECK(metrics.dead_letter_moves == 1u);

    db_close(db);
    return 0;
}

static int test_runner_metrics_non_retryable_and_reset(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    SapRunnerV0Metrics metrics = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    dispatch_state.fail_calls_remaining = 1u;
    dispatch_state.fail_rc = SAP_ERROR;

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 70u, frame, frame_len) == SAP_OK);
    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch_state, &processed) ==
          SAP_ERROR);
    CHECK(processed == 0u);

    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch_state, &processed) == SAP_OK);
    CHECK(processed == 1u);

    sap_runner_v0_metrics_snapshot(&runner, &metrics);
    CHECK(metrics.step_attempts == 2u);
    CHECK(metrics.step_successes == 1u);
    CHECK(metrics.retryable_failures == 0u);
    CHECK(metrics.conflict_failures == 0u);
    CHECK(metrics.busy_failures == 0u);
    CHECK(metrics.non_retryable_failures == 1u);
    CHECK(metrics.requeues == 1u);
    CHECK(metrics.dead_letter_moves == 0u);
    CHECK(metrics.step_latency_samples == 2u);

    sap_runner_v0_metrics_reset(&runner);
    sap_runner_v0_metrics_snapshot(&runner, &metrics);
    CHECK(metrics.step_attempts == 0u);
    CHECK(metrics.step_successes == 0u);
    CHECK(metrics.retryable_failures == 0u);
    CHECK(metrics.non_retryable_failures == 0u);
    CHECK(metrics.requeues == 0u);
    CHECK(metrics.dead_letter_moves == 0u);
    CHECK(metrics.step_latency_samples == 0u);
    CHECK(metrics.step_latency_total_ms == 0u);
    CHECK(metrics.step_latency_max_ms == 0u);

    db_close(db);
    return 0;
}

static int test_runner_metrics_retryable_dead_letter_path(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    SapRunnerV0Metrics metrics = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    uint32_t dead_letter_count = 0u;
    uint32_t rounds;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    dispatch_state.fail_calls_remaining = 32u;
    dispatch_state.fail_rc = SAP_CONFLICT;
    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 90u, frame, frame_len) == SAP_OK);

    for (rounds = 0u; rounds < 16u; rounds++)
    {
        CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch_state, &processed) ==
              SAP_OK);
        CHECK(processed == 0u);
        CHECK(count_worker_entries(db, SAP_WIT_DBI_DEAD_LETTER, 7u, &dead_letter_count) == SAP_OK);
        if (dead_letter_count == 1u)
        {
            break;
        }
    }
    CHECK(dead_letter_count == 1u);

    sap_runner_v0_metrics_snapshot(&runner, &metrics);
    CHECK(metrics.step_attempts >= 1u);
    CHECK(metrics.step_successes == 0u);
    CHECK(metrics.retryable_failures == metrics.step_attempts);
    CHECK(metrics.conflict_failures == metrics.retryable_failures);
    CHECK(metrics.busy_failures == 0u);
    CHECK(metrics.non_retryable_failures == 0u);
    CHECK(metrics.dead_letter_moves == 1u);
    CHECK(metrics.requeues + metrics.dead_letter_moves == metrics.retryable_failures);
    CHECK(metrics.step_latency_samples == metrics.step_attempts);

    db_close(db);
    return 0;
}

static int test_runner_replay_hook_inbox_requeue_flow(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    ReplayHookCtx replay = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    sap_runner_v0_set_replay_hook(&runner, on_replay_event, &replay);
    dispatch_state.fail_calls_remaining = 1u;
    dispatch_state.fail_rc = SAP_CONFLICT;

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len) == SAP_OK);

    CHECK(sap_runner_v0_poll_inbox(&runner, 2u, on_message, &dispatch_state, &processed) == SAP_OK);
    CHECK(processed == 1u);

    CHECK(replay.count >= 5u);
    CHECK(replay.events[0].kind == SAP_RUNNER_V0_REPLAY_EVENT_INBOX_ATTEMPT);
    CHECK(replay.events[0].seq == 1u);
    CHECK(replay.events[0].frame_len > 0u);
    CHECK(replay.events[1].kind == SAP_RUNNER_V0_REPLAY_EVENT_INBOX_RESULT);
    CHECK(replay.events[1].seq == 1u);
    CHECK(replay.events[1].rc == SAP_CONFLICT);
    CHECK(replay.events[2].kind == SAP_RUNNER_V0_REPLAY_EVENT_DISPOSITION_REQUEUE);
    CHECK(replay.events[2].seq == 1u);
    CHECK(replay.events[2].rc == SAP_CONFLICT);
    CHECK(replay.events[3].kind == SAP_RUNNER_V0_REPLAY_EVENT_INBOX_ATTEMPT);
    CHECK(replay.events[3].seq == 2u);
    CHECK(replay.events[4].kind == SAP_RUNNER_V0_REPLAY_EVENT_INBOX_RESULT);
    CHECK(replay.events[4].seq == 2u);
    CHECK(replay.events[4].rc == SAP_OK);

    db_close(db);
    return 0;
}

static int test_runner_observability_sinks_emit_updates(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    MetricsSinkCtx metrics_sink = {0};
    LogSinkCtx log_sink = {0};
    SapRunnerV0Metrics metrics = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    sap_runner_v0_set_metrics_sink(&runner, on_metrics_event, &metrics_sink);
    sap_runner_v0_set_log_sink(&runner, on_log_event, &log_sink);
    dispatch_state.fail_calls_remaining = 1u;
    dispatch_state.fail_rc = SAP_CONFLICT;

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len) == SAP_OK);
    CHECK(sap_runner_v0_poll_inbox(&runner, 2u, on_message, &dispatch_state, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch_state.invocations == 2u);
    CHECK(dispatch_state.calls == 1u);

    sap_runner_v0_metrics_snapshot(&runner, &metrics);
    CHECK(metrics.step_attempts == 2u);
    CHECK(metrics.step_successes == 1u);
    CHECK(metrics.retryable_failures == 1u);
    CHECK(metrics.conflict_failures == 1u);
    CHECK(metrics.busy_failures == 0u);
    CHECK(metrics.non_retryable_failures == 0u);
    CHECK(metrics.requeues == 1u);
    CHECK(metrics.dead_letter_moves == 0u);

    CHECK(metrics_sink.count > 1u);
    CHECK(metrics_sink.last.step_attempts == metrics.step_attempts);
    CHECK(metrics_sink.last.step_successes == metrics.step_successes);
    CHECK(metrics_sink.last.retryable_failures == metrics.retryable_failures);
    CHECK(metrics_sink.last.requeues == metrics.requeues);

    CHECK(log_sink.count == 2u);
    CHECK(log_sink.events[0].kind == SAP_RUNNER_V0_LOG_EVENT_STEP_RETRYABLE_FAILURE);
    CHECK(log_sink.events[0].seq == 1u);
    CHECK(log_sink.events[0].rc == SAP_CONFLICT);
    CHECK(log_sink.events[0].detail == 0u);
    CHECK(log_sink.events[1].kind == SAP_RUNNER_V0_LOG_EVENT_DISPOSITION_REQUEUE);
    CHECK(log_sink.events[1].seq == 1u);
    CHECK(log_sink.events[1].rc == SAP_CONFLICT);
    CHECK(log_sink.events[1].detail == 1u);

    db_close(db);
    return 0;
}

static int test_worker_tick_emits_worker_error_log_event(void)
{
    DB *db = new_db();
    SapRunnerV0Worker worker;
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    LogSinkCtx log_sink = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, on_message, &dispatch_state, 2u) == SAP_OK);
    sap_runner_v0_set_log_sink(&worker.runner, on_log_event, &log_sink);

    dispatch_state.fail_calls_remaining = 1u;
    dispatch_state.fail_rc = SAP_ERROR;
    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 10u, frame, frame_len) == SAP_OK);

    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_ERROR);
    CHECK(processed == 0u);
    CHECK(worker.last_error == SAP_ERROR);

    CHECK(log_sink.count == 3u);
    CHECK(log_sink.events[0].kind == SAP_RUNNER_V0_LOG_EVENT_STEP_NON_RETRYABLE_FAILURE);
    CHECK(log_sink.events[0].seq == 10u);
    CHECK(log_sink.events[0].rc == SAP_ERROR);
    CHECK(log_sink.events[1].kind == SAP_RUNNER_V0_LOG_EVENT_DISPOSITION_REQUEUE);
    CHECK(log_sink.events[1].seq == 10u);
    CHECK(log_sink.events[1].rc == SAP_ERROR);
    CHECK(log_sink.events[2].kind == SAP_RUNNER_V0_LOG_EVENT_WORKER_ERROR);
    CHECK(log_sink.events[2].seq == 0u);
    CHECK(log_sink.events[2].rc == SAP_ERROR);
    CHECK(log_sink.events[2].detail == 0u);

    db_close(db);
    return 0;
}

static int test_worker_shell_tick(void)
{
    DB *db = new_db();
    SapRunnerV0Worker worker;
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, on_message, &dispatch_state, 4u) == SAP_OK);

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len) == SAP_OK);

    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(worker.ticks == 1u);
    CHECK(worker.last_error == SAP_OK);
    CHECK(dispatch_state.calls == 1u);
    CHECK(inbox_entry_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(lease_entry_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);

#ifdef SAPLING_THREADED
    CHECK(sap_runner_v0_worker_start(&worker) == SAP_OK);
    sap_runner_v0_worker_request_stop(&worker);
    CHECK(sap_runner_v0_worker_join(&worker) == SAP_OK);
#else
    CHECK(sap_runner_v0_worker_start(&worker) == SAP_ERROR);
    CHECK(sap_runner_v0_worker_join(&worker) == SAP_ERROR);
    sap_runner_v0_worker_request_stop(&worker);
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_BUSY);
#endif

    sap_runner_v0_worker_shutdown(&worker);
    db_close(db);
    return 0;
}

typedef struct
{
    int64_t now_ms;
} NowCtx;

static int64_t fixed_now_ms(void *ctx)
{
    NowCtx *clock = (NowCtx *)ctx;
    if (!clock)
    {
        return 0;
    }
    return clock->now_ms;
}

typedef struct
{
    const int64_t *values;
    uint32_t len;
    uint32_t idx;
} SequenceNowCtx;

static int64_t sequence_now_ms(void *ctx)
{
    SequenceNowCtx *clock = (SequenceNowCtx *)ctx;
    uint32_t idx;

    if (!clock || !clock->values || clock->len == 0u)
    {
        return 0;
    }
    idx = (clock->idx < clock->len) ? clock->idx : (clock->len - 1u);
    if (clock->idx < UINT32_MAX)
    {
        clock->idx++;
    }
    return clock->values[idx];
}

static int64_t realtime_now_ms(void)
{
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0)
    {
        return 0;
    }
    return (int64_t)ts.tv_sec * 1000 + (int64_t)(ts.tv_nsec / 1000000L);
}

#ifdef SAPLING_THREADED
static void sleep_for_ms(uint32_t ms)
{
    struct timespec ts;

    ts.tv_sec = (time_t)(ms / 1000u);
    ts.tv_nsec = (long)((ms % 1000u) * 1000000L);
    nanosleep(&ts, NULL);
}

static int test_worker_thread_survives_transient_busy(void)
{
    DB *db = new_db();
    SapRunnerV0Worker worker;
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    Txn *hold_wtxn = NULL;
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    int exists = 1;
    uint32_t i;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, on_message, &dispatch_state, 2u) == SAP_OK);
    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_timer_v0_append(db, 0, 11u, frame, frame_len) == SAP_OK);

    hold_wtxn = txn_begin(db, NULL, 0u);
    CHECK(hold_wtxn != NULL);

    CHECK(sap_runner_v0_worker_start(&worker) == SAP_OK);
    sleep_for_ms(10u);

    txn_abort(hold_wtxn);
    hold_wtxn = NULL;

    for (i = 0u; i < 200u; i++)
    {
        CHECK(timer_entry_exists(db, 0, 11u, &exists) == SAP_OK);
        if (!exists)
        {
            break;
        }
        sleep_for_ms(2u);
    }

    sap_runner_v0_worker_request_stop(&worker);
    CHECK(sap_runner_v0_worker_join(&worker) == SAP_OK);

    CHECK(exists == 0);
    CHECK(worker.last_error == SAP_OK);

    sap_runner_v0_worker_shutdown(&worker);
    db_close(db);
    return 0;
}
#endif

static int test_worker_tick_uses_time_hook_for_inbox_lease_reclaim(void)
{
    DB *db = new_db();
    SapRunnerV0Worker worker;
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    NowCtx clock = {0};
    SapRunnerLeaseV0 lease = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;
    int64_t wall_now;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, on_message, &dispatch_state, 4u) == SAP_OK);

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len) == SAP_OK);

    wall_now = realtime_now_ms();
    CHECK(wall_now > 0);
    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 1u, 99u, wall_now, wall_now + 60000, &lease) ==
          SAP_OK);

    clock.now_ms = wall_now + 120000;
    sap_runner_v0_worker_set_time_hooks(&worker, fixed_now_ms, &clock, NULL, NULL);

    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch_state.calls == 1u);
    CHECK(inbox_entry_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(lease_entry_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);

    db_close(db);
    return 0;
}

static int test_worker_tick_uses_time_hook_for_timer_latency(void)
{
    DB *db = new_db();
    SapRunnerV0Worker worker;
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    SapRunnerV0Metrics metrics = {0};
    const int64_t now_values[] = {5000, 10000, 11234};
    SequenceNowCtx clock = {now_values, 3u, 0u};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, on_message, &dispatch_state, 4u) == SAP_OK);
    sap_runner_v0_worker_set_time_hooks(&worker, sequence_now_ms, &clock, NULL, NULL);

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_timer_v0_append(db, 5000, 1u, frame, frame_len) == SAP_OK);

    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch_state.calls == 1u);

    sap_runner_v0_metrics_snapshot(&worker.runner, &metrics);
    CHECK(metrics.step_attempts == 1u);
    CHECK(metrics.step_successes == 1u);
    CHECK(metrics.step_latency_samples == 1u);
    CHECK(metrics.step_latency_total_ms == 1234u);
    CHECK(metrics.step_latency_max_ms == 1234u);

    db_close(db);
    return 0;
}

static int test_worker_tick_drains_due_timers(void)
{
    DB *db = new_db();
    SapRunnerV0Worker worker;
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, on_message, &dispatch_state, 4u) == SAP_OK);

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_timer_v0_append(db, 0, 1u, frame, frame_len) == SAP_OK);

    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch_state.calls == 1u);
    CHECK(timer_entry_exists(db, 0, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);

    db_close(db);
    return 0;
}

static int test_worker_tick_timer_replay_preserves_seq(void)
{
    DB *db = new_db();
    SapRunnerV0Worker worker;
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    ReplayHookCtx replay = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, on_message, &dispatch_state, 4u) == SAP_OK);
    sap_runner_v0_set_replay_hook(&worker.runner, on_replay_event, &replay);

    CHECK(encode_test_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_timer_v0_append(db, 0, 41u, frame, frame_len) == SAP_OK);
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch_state.calls == 1u);

    CHECK(replay.count >= 2u);
    CHECK(replay.events[0].kind == SAP_RUNNER_V0_REPLAY_EVENT_TIMER_ATTEMPT);
    CHECK(replay.events[0].seq == 41u);
    CHECK(replay.events[1].kind == SAP_RUNNER_V0_REPLAY_EVENT_TIMER_RESULT);
    CHECK(replay.events[1].seq == 41u);
    CHECK(replay.events[1].rc == SAP_OK);

    db_close(db);
    return 0;
}

static int test_worker_idle_sleep_budget(void)
{
    DB *db = new_db();
    SapRunnerV0Worker worker;
    SapRunnerV0Config cfg;
    TestDispatchCtx dispatch_state = {0};
    NowCtx clock = {0};
    uint32_t sleep_ms = 0u;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_worker_init(&worker, &cfg, on_message, &dispatch_state, 4u) == SAP_OK);

    sap_runner_v0_worker_set_idle_policy(&worker, 25u);
    clock.now_ms = 100;
    sap_runner_v0_worker_set_time_hooks(&worker, fixed_now_ms, &clock, NULL, NULL);

    CHECK(sap_runner_v0_worker_compute_idle_sleep_ms(&worker, &sleep_ms) == SAP_OK);
    CHECK(sleep_ms == 25u);

    CHECK(sap_runner_timer_v0_append(db, 150, 1u, (const uint8_t *)"a", 1u) == SAP_OK);
    CHECK(sap_runner_v0_worker_compute_idle_sleep_ms(&worker, &sleep_ms) == SAP_OK);
    CHECK(sleep_ms == 25u);

    CHECK(sap_runner_timer_v0_append(db, 105, 1u, (const uint8_t *)"b", 1u) == SAP_OK);
    CHECK(sap_runner_v0_worker_compute_idle_sleep_ms(&worker, &sleep_ms) == SAP_OK);
    CHECK(sleep_ms == 5u);

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

    if (test_inbox_key_codec() != 0)
    {
        return 1;
    }
    if (test_schema_bootstrap_and_guard() != 0)
    {
        return 2;
    }
    if (test_runner_init_and_step() != 0)
    {
        return 3;
    }
    if (test_runner_poll_inbox() != 0)
    {
        return 4;
    }
    if (test_poll_inbox_retryable_requeues_and_recovers() != 0)
    {
        return 5;
    }
    if (test_poll_inbox_non_retryable_requeues_and_returns_error() != 0)
    {
        return 6;
    }
    if (test_retry_budget_moves_to_dead_letter() != 0)
    {
        return 7;
    }
    if (test_runner_policy_override_retry_budget() != 0)
    {
        return 8;
    }
    if (test_runner_metrics_non_retryable_and_reset() != 0)
    {
        return 9;
    }
    if (test_runner_metrics_retryable_dead_letter_path() != 0)
    {
        return 10;
    }
    if (test_runner_replay_hook_inbox_requeue_flow() != 0)
    {
        return 11;
    }
    if (test_runner_observability_sinks_emit_updates() != 0)
    {
        return 12;
    }
    if (test_worker_tick_emits_worker_error_log_event() != 0)
    {
        return 13;
    }
    if (test_worker_shell_tick() != 0)
    {
        return 14;
    }
#ifdef SAPLING_THREADED
    if (test_worker_thread_survives_transient_busy() != 0)
    {
        return 15;
    }
#endif
    if (test_worker_tick_drains_due_timers() != 0)
    {
        return 16;
    }
    if (test_worker_tick_timer_replay_preserves_seq() != 0)
    {
        return 17;
    }
    if (test_worker_tick_uses_time_hook_for_inbox_lease_reclaim() != 0)
    {
        return 18;
    }
    if (test_worker_tick_uses_time_hook_for_timer_latency() != 0)
    {
        return 19;
    }
    if (test_worker_idle_sleep_budget() != 0)
    {
        return 20;
    }
    return 0;
}
