/*
 * runner_atomic_integration_test.c - deterministic retry + nested atomic integration
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
        return ERR_INVALID;
    }
    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_INVALID;
    }
    rc = txn_put_dbi(txn, 10u, key, key_len, val, val_len);
    if (rc != ERR_OK)
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
        return ERR_INVALID;
    }
    *val_out = NULL;
    *val_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_INVALID;
    }
    rc = txn_get_dbi(txn, 10u, key, key_len, val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

typedef struct
{
    uint32_t calls;
    uint8_t frames[4][128];
    uint32_t frame_lens[4];
} SinkCtx;

static int capture_sink(const uint8_t *frame, uint32_t frame_len, void *ctx)
{
    SinkCtx *sink = (SinkCtx *)ctx;
    if (!sink || !frame || frame_len == 0u || sink->calls >= 4u || frame_len > 128u)
    {
        return ERR_INVALID;
    }
    memcpy(sink->frames[sink->calls], frame, frame_len);
    sink->frame_lens[sink->calls] = frame_len;
    sink->calls++;
    return ERR_OK;
}

typedef struct
{
    uint32_t calls;
} SleepCtx;

static void fake_sleep(uint32_t backoff_us, void *ctx)
{
    SleepCtx *sleep = (SleepCtx *)ctx;
    (void)backoff_us;
    if (!sleep)
    {
        return;
    }
    sleep->calls++;
}

typedef struct
{
    DB *db;
    uint32_t calls;
    int injected_conflict;
} AtomicCtx;

static int nested_retry_atomic(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx)
{
    AtomicCtx *atomic = (AtomicCtx *)ctx;
    const void *state_val = NULL;
    uint32_t state_len = 0u;
    SapRunnerIntentV0 intent = {0};
    const uint8_t intent_payload[] = {'d', 'o', 'n', 'e'};

    if (!stack || !read_txn || !atomic || !atomic->db)
    {
        return ERR_INVALID;
    }
    atomic->calls++;

    if (sap_runner_txstack_v0_read_dbi(stack, read_txn, 10u, "state", 5u, &state_val, &state_len) !=
        ERR_OK)
    {
        return ERR_INVALID;
    }

    if (sap_runner_txstack_v0_push(stack) != ERR_OK)
    {
        return ERR_INVALID;
    }
    if (sap_runner_txstack_v0_stage_put_dbi(stack, 10u, "nested.commit", 13u, "yes", 3u) != ERR_OK)
    {
        return ERR_INVALID;
    }
    if (sap_runner_txstack_v0_commit_top(stack) != ERR_OK)
    {
        return ERR_INVALID;
    }

    if (sap_runner_txstack_v0_push(stack) != ERR_OK)
    {
        return ERR_INVALID;
    }
    if (sap_runner_txstack_v0_stage_put_dbi(stack, 10u, "nested.abort", 12u, "no", 2u) != ERR_OK)
    {
        return ERR_INVALID;
    }
    if (sap_runner_txstack_v0_abort_top(stack) != ERR_OK)
    {
        return ERR_INVALID;
    }

    if (sap_runner_txstack_v0_stage_put_dbi(stack, 10u, "state", 5u, "done", 4u) != ERR_OK)
    {
        return ERR_INVALID;
    }

    intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
    intent.flags = 0u;
    intent.due_ts = 0;
    intent.message = intent_payload;
    intent.message_len = sizeof(intent_payload);
    if (sap_runner_txstack_v0_push_intent(stack, &intent) != ERR_OK)
    {
        return ERR_INVALID;
    }

    if (!atomic->injected_conflict)
    {
        atomic->injected_conflict = 1;
        if (db_put(atomic->db, "state", 5u, "other", 5u) != ERR_OK)
        {
            return ERR_INVALID;
        }
    }

    return ERR_OK;
}

static int test_retry_and_nested_closed_nesting(void)
{
    DB *db = new_db();
    AtomicCtx atomic = {0};
    SinkCtx sink = {0};
    SleepCtx sleep = {0};
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats stats = {0};
    const void *val = NULL;
    uint32_t val_len = 0u;
    SapRunnerIntentV0 decoded = {0};

    CHECK(db != NULL);
    CHECK(db_put(db, "state", 5u, "seed", 4u) == ERR_OK);

    atomic.db = db;
    atomic.calls = 0u;
    atomic.injected_conflict = 0;

    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 3u;
    policy.initial_backoff_us = 10u;
    policy.max_backoff_us = 40u;
    policy.sleep_fn = fake_sleep;
    policy.sleep_ctx = &sleep;

    CHECK(sap_runner_attempt_v0_run(db, &policy, nested_retry_atomic, &atomic, capture_sink, &sink,
                                    &stats) == ERR_OK);
    CHECK(atomic.calls == 2u);
    CHECK(stats.attempts == 2u);
    CHECK(stats.retries == 1u);
    CHECK(stats.conflict_retries == 1u);
    CHECK(stats.busy_retries == 0u);
    CHECK(stats.last_rc == ERR_OK);
    CHECK(sleep.calls == 1u);
    CHECK(sink.calls == 1u);

    CHECK(db_get(db, "state", 5u, &val, &val_len) == ERR_OK);
    CHECK(val_len == 4u);
    CHECK(memcmp(val, "done", 4u) == 0);
    CHECK(db_get(db, "nested.commit", 13u, &val, &val_len) == ERR_OK);
    CHECK(val_len == 3u);
    CHECK(memcmp(val, "yes", 3u) == 0);
    CHECK(db_get(db, "nested.abort", 12u, &val, &val_len) == ERR_NOT_FOUND);

    CHECK(sap_runner_intent_v0_decode(sink.frames[0], sink.frame_lens[0], &decoded) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(decoded.kind == SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT);
    CHECK(decoded.message_len == 4u);
    CHECK(memcmp(decoded.message, "done", 4u) == 0);

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

    int rc = test_retry_and_nested_closed_nesting();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
