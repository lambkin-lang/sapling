/*
 * runner_lifecycle_test.c - tests for phase-A runner lifecycle scaffolding
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/runner_v0.h"
#include "runner/timer_v0.h"
#include "generated/wit_schema_dbis.h"

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
    uint32_t invocations;
    uint32_t calls;
    int64_t last_to_worker;
    uint32_t fail_calls_remaining;
    int fail_rc;
} TestDispatchCtx;

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
    uint8_t key[SAP_RUNNER_TIMER_KEY_V0_SIZE];
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
    sap_runner_timer_v0_key_encode(due_ts, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_TIMERS, key, sizeof(key), &val, &val_len);
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
    if (test_worker_shell_tick() != 0)
    {
        return 8;
    }
    if (test_worker_tick_drains_due_timers() != 0)
    {
        return 9;
    }
    if (test_worker_idle_sleep_budget() != 0)
    {
        return 10;
    }
    return 0;
}
