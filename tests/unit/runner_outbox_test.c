/*
 * runner_outbox_test.c - tests for phase-C outbox append/drain scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/outbox_v0.h"
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
    rc = txn_get_dbi(txn, SAP_WIT_DBI_OUTBOX, key, sizeof(key), val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

typedef struct
{
    uint32_t calls;
    uint8_t frames[8][32];
    uint32_t frame_lens[8];
} DrainCtx;

static int collect_frame(const uint8_t *frame, uint32_t frame_len, void *ctx)
{
    DrainCtx *drain = (DrainCtx *)ctx;
    if (!drain || !frame || frame_len == 0u || frame_len > 32u || drain->calls >= 8u)
    {
        return SAP_ERROR;
    }
    memcpy(drain->frames[drain->calls], frame, frame_len);
    drain->frame_lens[drain->calls] = frame_len;
    drain->calls++;
    return SAP_OK;
}

static int test_outbox_append_and_drain(void)
{
    DB *db = new_db();
    DrainCtx drain = {0};
    uint8_t a[] = {'a'};
    uint8_t b[] = {'b', 'b'};
    uint32_t processed = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(sap_runner_outbox_v0_append_frame(db, 10u, a, sizeof(a)) == SAP_OK);
    CHECK(sap_runner_outbox_v0_append_frame(db, 11u, b, sizeof(b)) == SAP_OK);
    CHECK(sap_runner_outbox_v0_drain(db, 8u, collect_frame, &drain, &processed) == SAP_OK);
    CHECK(processed == 2u);
    CHECK(drain.calls == 2u);
    CHECK(drain.frame_lens[0] == sizeof(a));
    CHECK(memcmp(drain.frames[0], a, sizeof(a)) == 0);
    CHECK(drain.frame_lens[1] == sizeof(b));
    CHECK(memcmp(drain.frames[1], b, sizeof(b)) == 0);

    CHECK(outbox_get(db, 10u, &val, &val_len) == SAP_NOTFOUND);
    CHECK(outbox_get(db, 11u, &val, &val_len) == SAP_NOTFOUND);

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

    (void)read_txn;
    if (!stack || !atomic)
    {
        return SAP_ERROR;
    }
    atomic->calls++;

    if (atomic->timer_only)
    {
        intent.kind = SAP_RUNNER_INTENT_KIND_TIMER_ARM;
        intent.flags = SAP_RUNNER_INTENT_FLAG_HAS_DUE_TS;
        intent.due_ts = 123;
        intent.message = timer_payload;
        intent.message_len = sizeof(timer_payload);
    }
    else
    {
        intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
        intent.flags = 0u;
        intent.due_ts = 0;
        intent.message = outbox_payload;
        intent.message_len = sizeof(outbox_payload);
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

    CHECK(db != NULL);
    CHECK(sap_runner_outbox_v0_publisher_init(&publisher, db, 100u) == SAP_OK);
    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 0u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;

    atomic.calls = 0u;
    atomic.timer_only = 0;
    CHECK(sap_runner_attempt_v0_run(db, &policy, atomic_emit_intent, &atomic,
                                    sap_runner_outbox_v0_publish_intent, &publisher,
                                    &stats) == SAP_OK);
    CHECK(stats.attempts == 1u);
    CHECK(stats.last_rc == SAP_OK);
    CHECK(atomic.calls == 1u);
    CHECK(publisher.next_seq == 101u);
    CHECK(outbox_get(db, 100u, &val, &val_len) == SAP_OK);
    CHECK(val_len == 3u);
    CHECK(memcmp(val, "evt", 3u) == 0);

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
    CHECK(sap_runner_outbox_v0_publisher_init(&publisher, db, 200u) == SAP_OK);
    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 0u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;

    atomic.calls = 0u;
    atomic.timer_only = 1;
    CHECK(sap_runner_attempt_v0_run(db, &policy, atomic_emit_intent, &atomic,
                                    sap_runner_outbox_v0_publish_intent, &publisher,
                                    &stats) == SAP_ERROR);
    CHECK(stats.attempts == 1u);
    CHECK(stats.last_rc == SAP_ERROR);
    CHECK(outbox_get(db, 200u, &val, &val_len) == SAP_NOTFOUND);

    db_close(db);
    return 0;
}

int main(void)
{
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
