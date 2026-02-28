/*
 * runner_attempt_test.c - tests for phase-B retry attempt engine
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/attempt_v0.h"

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

typedef struct
{
    uint32_t calls;
    uint8_t frames[8][128];
    uint32_t frame_lens[8];
} SinkCtx;

static int capture_sink(const uint8_t *frame, uint32_t frame_len, void *ctx)
{
    SinkCtx *sink = (SinkCtx *)ctx;

    if (!sink || !frame || frame_len == 0u || sink->calls >= 8u || frame_len > 128u)
    {
        return SAP_ERROR;
    }
    memcpy(sink->frames[sink->calls], frame, frame_len);
    sink->frame_lens[sink->calls] = frame_len;
    sink->calls++;
    return SAP_OK;
}

typedef struct
{
    uint32_t calls;
    uint32_t count;
} SleepCtx;

static void fake_sleep(uint32_t backoff_us, void *ctx)
{
    SleepCtx *sleep = (SleepCtx *)ctx;
    if (!sleep)
    {
        return;
    }
    (void)backoff_us;
    sleep->calls++;
}

typedef struct
{
    uint32_t calls;
} HappyCtx;

static int happy_atomic(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx)
{
    HappyCtx *happy = (HappyCtx *)ctx;
    const void *val = NULL;
    uint32_t val_len = 0u;
    SapRunnerIntentV0 intent = {0};
    const uint8_t payload[] = {'o', 'k'};

    if (!stack || !read_txn || !happy)
    {
        return SAP_ERROR;
    }
    happy->calls++;

    if (sap_runner_txstack_v0_read_dbi(stack, read_txn, 10u, "k", 1u, &val, &val_len) !=
        SAP_NOTFOUND)
    {
        return SAP_ERROR;
    }
    if (sap_runner_txstack_v0_stage_put_dbi(stack, 10u, "k", 1u, "v", 1u) != SAP_OK)
    {
        return SAP_ERROR;
    }

    intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
    intent.flags = 0u;
    intent.due_ts = 0;
    intent.message = payload;
    intent.message_len = sizeof(payload);
    if (sap_runner_txstack_v0_push_intent(stack, &intent) != SAP_OK)
    {
        return SAP_ERROR;
    }
    return SAP_OK;
}

typedef struct
{
    DB *db;
    uint32_t calls;
    int injected;
} ConflictCtx;

static int conflict_once_atomic(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx)
{
    ConflictCtx *cc = (ConflictCtx *)ctx;
    const void *val = NULL;
    uint32_t val_len = 0u;

    if (!stack || !read_txn || !cc || !cc->db)
    {
        return SAP_ERROR;
    }
    cc->calls++;

    if (sap_runner_txstack_v0_read_dbi(stack, read_txn, 10u, "k", 1u, &val, &val_len) != SAP_OK)
    {
        return SAP_ERROR;
    }
    if (sap_runner_txstack_v0_stage_put_dbi(stack, 10u, "k", 1u, "final", 5u) != SAP_OK)
    {
        return SAP_ERROR;
    }
    if (!cc->injected)
    {
        cc->injected = 1;
        if (db_put(cc->db, "k", 1u, "other", 5u) != SAP_OK)
        {
            return SAP_ERROR;
        }
    }
    return SAP_OK;
}

typedef struct
{
    uint32_t calls;
} NestedCtx;

static int nested_atomic(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx)
{
    NestedCtx *nested = (NestedCtx *)ctx;
    (void)read_txn;
    if (!stack || !nested)
    {
        return SAP_ERROR;
    }
    nested->calls++;

    if (sap_runner_txstack_v0_push(stack) != SAP_OK)
    {
        return SAP_ERROR;
    }
    if (sap_runner_txstack_v0_stage_put_dbi(stack, 10u, "x", 1u, "1", 1u) != SAP_OK)
    {
        return SAP_ERROR;
    }
    if (sap_runner_txstack_v0_commit_top(stack) != SAP_OK)
    {
        return SAP_ERROR;
    }

    if (sap_runner_txstack_v0_push(stack) != SAP_OK)
    {
        return SAP_ERROR;
    }
    if (sap_runner_txstack_v0_stage_put_dbi(stack, 10u, "y", 1u, "tmp", 3u) != SAP_OK)
    {
        return SAP_ERROR;
    }
    if (sap_runner_txstack_v0_abort_top(stack) != SAP_OK)
    {
        return SAP_ERROR;
    }
    return SAP_OK;
}

static int always_conflict_atomic(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx)
{
    (void)stack;
    (void)read_txn;
    (void)ctx;
    return SAP_CONFLICT;
}

static int test_attempt_success_and_intent_sink(void)
{
    DB *db = new_db();
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats stats = {0};
    HappyCtx atomic = {0};
    SinkCtx sink = {0};
    const void *val = NULL;
    uint32_t val_len = 0u;
    SapRunnerIntentV0 decoded = {0};

    CHECK(db != NULL);
    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 0u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;

    CHECK(sap_runner_attempt_v0_run(db, &policy, happy_atomic, &atomic, capture_sink, &sink,
                                    &stats) == SAP_OK);
    CHECK(stats.attempts == 1u);
    CHECK(stats.retries == 0u);
    CHECK(stats.last_rc == SAP_OK);
    CHECK(atomic.calls == 1u);
    CHECK(sink.calls == 1u);
    CHECK(sap_runner_intent_v0_decode(sink.frames[0], sink.frame_lens[0], &decoded) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(decoded.kind == SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT);
    CHECK(decoded.message_len == 2u);
    CHECK(memcmp(decoded.message, "ok", 2u) == 0);

    CHECK(db_get(db, "k", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 1u);
    CHECK(memcmp(val, "v", 1u) == 0);

    db_close(db);
    return 0;
}

static int test_attempt_retries_on_conflict(void)
{
    DB *db = new_db();
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats stats = {0};
    ConflictCtx cc = {0};
    SleepCtx sleep = {0};
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(db_put(db, "k", 1u, "init", 4u) == SAP_OK);

    cc.db = db;
    cc.calls = 0u;
    cc.injected = 0;

    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 2u;
    policy.initial_backoff_us = 10u;
    policy.max_backoff_us = 40u;
    policy.sleep_fn = fake_sleep;
    policy.sleep_ctx = &sleep;

    CHECK(sap_runner_attempt_v0_run(db, &policy, conflict_once_atomic, &cc, NULL, NULL, &stats) ==
          SAP_OK);
    CHECK(cc.calls == 2u);
    CHECK(stats.attempts == 2u);
    CHECK(stats.retries == 1u);
    CHECK(stats.conflict_retries == 1u);
    CHECK(stats.busy_retries == 0u);
    CHECK(stats.last_rc == SAP_OK);
    CHECK(sleep.calls == 1u);

    CHECK(db_get(db, "k", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 5u);
    CHECK(memcmp(val, "final", 5u) == 0);

    db_close(db);
    return 0;
}

static int test_attempt_nested_stack_in_atomic_fn(void)
{
    DB *db = new_db();
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats stats = {0};
    NestedCtx nested = {0};
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 0u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;

    CHECK(sap_runner_attempt_v0_run(db, &policy, nested_atomic, &nested, NULL, NULL, &stats) ==
          SAP_OK);
    CHECK(nested.calls == 1u);
    CHECK(stats.attempts == 1u);
    CHECK(stats.last_rc == SAP_OK);

    CHECK(db_get(db, "x", 1u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 1u);
    CHECK(memcmp(val, "1", 1u) == 0);
    CHECK(db_get(db, "y", 1u, &val, &val_len) == SAP_NOTFOUND);

    db_close(db);
    return 0;
}

static int test_attempt_stops_at_retry_budget(void)
{
    DB *db = new_db();
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats stats = {0};
    SleepCtx sleep = {0};

    CHECK(db != NULL);
    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 2u;
    policy.initial_backoff_us = 10u;
    policy.max_backoff_us = 40u;
    policy.sleep_fn = fake_sleep;
    policy.sleep_ctx = &sleep;

    CHECK(sap_runner_attempt_v0_run(db, &policy, always_conflict_atomic, NULL, NULL, NULL,
                                    &stats) == SAP_CONFLICT);
    CHECK(stats.attempts == 3u);
    CHECK(stats.retries == 2u);
    CHECK(stats.conflict_retries == 2u);
    CHECK(stats.last_rc == SAP_CONFLICT);
    CHECK(sleep.calls == 2u);

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

    rc = test_attempt_success_and_intent_sink();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_attempt_retries_on_conflict();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_attempt_nested_stack_in_atomic_fn();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_attempt_stops_at_retry_budget();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
