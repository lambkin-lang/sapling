/*
 * runner_timer_test.c - tests for phase-C timer ingestion/drain scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/runner_v0.h"
#include "runner/timer_v0.h"
#include "sapling/bept.h"

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

static SapMemArena *g_alloc = NULL;

static DB *new_db(void)
{
    DB *db = db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        return NULL;
    }
    if (sap_bept_subsystem_init((SapEnv *)db) != ERR_OK)
    {
        /* db_close destroys env, which is enough */
        db_close(db);
        return NULL;
    }
    if (sap_runner_v0_bootstrap_dbis(db) != ERR_OK)
    {
        db_close(db);
        return NULL;
    }
    return db;
}

static void timer_to_bept_key(int64_t due_ts, uint64_t seq, uint32_t key_out[4])
{
    /* Flip sign bit of signed int64 to sort correctly as unsigned */
    uint64_t ts_encoded = (uint64_t)due_ts ^ 0x8000000000000000ULL;
    key_out[0] = (uint32_t)(ts_encoded >> 32);
    key_out[1] = (uint32_t)(ts_encoded & 0xFFFFFFFF);
    key_out[2] = (uint32_t)(seq >> 32);
    key_out[3] = (uint32_t)(seq & 0xFFFFFFFF);
}

static int timer_get(DB *db, int64_t due_ts, uint64_t seq, const void **val_out,
                     uint32_t *val_len_out)
{
    Txn *txn;
    int rc;
    uint32_t key[4];

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
    timer_to_bept_key(due_ts, seq, key);
    rc = sap_bept_get(txn, key, 4, val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

typedef struct
{
    uint32_t calls;
    int64_t due_ts[8];
    uint64_t seq[8];
    uint8_t payloads[8][16];
    uint32_t payload_lens[8];
} DueCtx;

static int collect_due(int64_t due_ts, uint64_t seq, const uint8_t *payload, uint32_t payload_len,
                       void *ctx)
{
    DueCtx *due = (DueCtx *)ctx;
    if (!due || !payload || payload_len == 0u || payload_len > 16u || due->calls >= 8u)
    {
        return ERR_INVALID;
    }
    due->due_ts[due->calls] = due_ts;
    due->seq[due->calls] = seq;
    memcpy(due->payloads[due->calls], payload, payload_len);
    due->payload_lens[due->calls] = payload_len;
    due->calls++;
    return ERR_OK;
}

static int test_timer_append_and_drain_due(void)
{
    DB *db = new_db();
    DueCtx due = {0};
    uint32_t processed = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;
    const uint8_t a[] = {'a'};
    const uint8_t b[] = {'b'};
    const uint8_t c[] = {'c'};

    CHECK(db != NULL);
    CHECK(sap_runner_timer_v0_append(db, 100, 2u, a, sizeof(a)) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 90, 1u, b, sizeof(b)) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 110, 1u, c, sizeof(c)) == ERR_OK);
    CHECK(sap_runner_timer_v0_drain_due(db, 100, 8u, collect_due, &due, &processed) == ERR_OK);
    CHECK(processed == 2u);
    CHECK(due.calls == 2u);
    CHECK(due.due_ts[0] == 90);
    CHECK(due.seq[0] == 1u);
    CHECK(due.payload_lens[0] == 1u);
    CHECK(due.payloads[0][0] == 'b');
    CHECK(due.due_ts[1] == 100);
    CHECK(due.seq[1] == 2u);
    CHECK(due.payload_lens[1] == 1u);
    CHECK(due.payloads[1][0] == 'a');

    CHECK(timer_get(db, 90, 1u, &val, &val_len) == ERR_NOT_FOUND);
    CHECK(timer_get(db, 100, 2u, &val, &val_len) == ERR_NOT_FOUND);
    CHECK(timer_get(db, 110, 1u, &val, &val_len) == ERR_OK);
    CHECK(val_len == 1u);
    CHECK(((const uint8_t *)val)[0] == 'c');

    db_close(db);
    return 0;
}

typedef struct
{
    uint32_t calls;
    int timer_only;
} AtomicCtx;

static int atomic_emit_timer(SapRunnerTxStackV0 *stack, Txn *read_txn, void *ctx)
{
    AtomicCtx *atomic = (AtomicCtx *)ctx;
    SapRunnerIntentV0 intent = {0};
    const uint8_t timer_payload[] = {'t', 'm'};
    const uint8_t outbox_payload[] = {'o'};

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

static int test_timer_publisher_with_attempt_engine(void)
{
    DB *db = new_db();
    SapRunnerTimerV0Publisher publisher = {0};
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats stats = {0};
    AtomicCtx atomic = {0};
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(sap_runner_timer_v0_publisher_init(&publisher, db, 50u) == ERR_OK);
    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 0u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;

    atomic.calls = 0u;
    atomic.timer_only = 1;
    CHECK(sap_runner_attempt_v0_run(db, &policy, atomic_emit_timer, &atomic,
                                    sap_runner_timer_v0_publish_intent, &publisher,
                                    &stats) == ERR_OK);
    CHECK(stats.attempts == 1u);
    CHECK(stats.last_rc == ERR_OK);
    CHECK(publisher.next_seq == 51u);
    CHECK(timer_get(db, 123, 50u, &val, &val_len) == ERR_OK);
    CHECK(val_len == 2u);
    CHECK(memcmp(val, "tm", 2u) == 0);

    db_close(db);
    return 0;
}

static int test_timer_publisher_rejects_outbox_intent(void)
{
    DB *db = new_db();
    SapRunnerTimerV0Publisher publisher = {0};
    SapRunnerAttemptV0Policy policy;
    SapRunnerAttemptV0Stats stats = {0};
    AtomicCtx atomic = {0};
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(sap_runner_timer_v0_publisher_init(&publisher, db, 80u) == ERR_OK);
    sap_runner_attempt_v0_policy_default(&policy);
    policy.max_retries = 0u;
    policy.initial_backoff_us = 0u;
    policy.max_backoff_us = 0u;

    atomic.calls = 0u;
    atomic.timer_only = 0;
    CHECK(sap_runner_attempt_v0_run(db, &policy, atomic_emit_timer, &atomic,
                                    sap_runner_timer_v0_publish_intent, &publisher,
                                    &stats) == ERR_INVALID);
    CHECK(stats.attempts == 1u);
    CHECK(stats.last_rc == ERR_INVALID);
    CHECK(timer_get(db, 123, 80u, &val, &val_len) == ERR_NOT_FOUND);

    db_close(db);
    return 0;
}

int main(void)
{
    SapArenaOptions g_alloc_opts = {
        .type = SAP_ARENA_BACKING_MALLOC
    };
    sap_arena_init(&g_alloc, &g_alloc_opts);

    int rc;

    rc = test_timer_append_and_drain_due();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_timer_publisher_with_attempt_engine();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_timer_publisher_rejects_outbox_intent();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
