/*
 * runner_timer_test.c - tests for phase-C timer ingestion/drain scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/runner_v0.h"
#include "runner/scheduler_v0.h"
#include "runner/timer_v0.h"
#include "runner/wit_wire_bridge_v0.h"
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
    if (sap_runner_v0_bootstrap_dbis(db) != ERR_OK)
    {
        db_close(db);
        return NULL;
    }
    return db;
}

static int encode_message(uint32_t to_worker, const uint8_t *payload, uint32_t payload_len,
                          uint8_t *buf, uint32_t buf_cap, uint32_t *len_out)
{
    uint8_t msg_id[] = {'t', 'm', 'r', (uint8_t)payload_len};
    SapRunnerMessageV0 msg = {0};

    if (!payload || payload_len == 0u)
    {
        return ERR_INVALID;
    }
    msg.kind = SAP_RUNNER_MESSAGE_KIND_TIMER;
    msg.flags = 0u;
    msg.to_worker = (int64_t)to_worker;
    msg.route_worker = (int64_t)to_worker;
    msg.route_timestamp = 456;
    msg.from_worker = 0;
    msg.message_id = msg_id;
    msg.message_id_len = sizeof(msg_id);
    msg.trace_id = NULL;
    msg.trace_id_len = 0u;
    msg.payload = payload;
    msg.payload_len = payload_len;
    return sap_runner_message_v0_encode(&msg, buf, buf_cap, len_out);
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
    const void *stored = NULL;
    uint32_t stored_len = 0u;
    uint8_t *copy = NULL;

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
    rc = sap_bept_get(txn, key, 4, &stored, &stored_len);
    if (rc == ERR_NOT_FOUND)
    {
        txn_abort(txn);
        return ERR_NOT_FOUND;
    }
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (!stored || stored_len == 0u)
    {
        txn_abort(txn);
        return ERR_CORRUPT;
    }
    if (!sap_runner_wit_wire_v0_value_is_dbi4_timers(stored, stored_len))
    {
        txn_abort(txn);
        return ERR_CORRUPT;
    }
    rc = sap_runner_wit_wire_v0_decode_dbi4_timers_value_to_wire((const uint8_t *)stored, stored_len,
                                                                  &copy, val_len_out);
    txn_abort(txn);
    if (rc != ERR_OK)
    {
        return rc;
    }
    *val_out = copy;
    return ERR_OK;
}

static int force_bept_min_only(DB *db, int64_t due_ts, uint64_t seq)
{
    Txn *txn;
    uint8_t timer_key[SAP_RUNNER_TIMER_KEY_V0_SIZE];
    uint32_t key[4];
    const void *payload = NULL;
    uint32_t payload_len = 0u;
    int rc;

    if (!db)
    {
        return ERR_INVALID;
    }
    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return ERR_BUSY;
    }

    rc = sap_bept_clear(txn);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    sap_runner_timer_v0_key_encode(due_ts, seq, timer_key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_TIMERS, timer_key, sizeof(timer_key), &payload, &payload_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (!payload || payload_len == 0u)
    {
        txn_abort(txn);
        return ERR_CORRUPT;
    }

    timer_to_bept_key(due_ts, seq, key);
    rc = sap_bept_put(txn, key, 4u, payload, payload_len, 0u, NULL);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    return txn_commit(txn);
}

typedef struct
{
    uint32_t calls;
    int64_t due_ts[8];
    uint64_t seq[8];
    uint8_t payloads[8][256];
    uint32_t payload_lens[8];
} DueCtx;

static int collect_due(int64_t due_ts, uint64_t seq, const uint8_t *payload, uint32_t payload_len,
                       void *ctx)
{
    DueCtx *due = (DueCtx *)ctx;
    if (!due || !payload || payload_len == 0u || payload_len > 256u || due->calls >= 8u)
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
    uint8_t frame_a[128];
    uint8_t frame_b[128];
    uint8_t frame_c[128];
    uint32_t frame_a_len = 0u;
    uint32_t frame_b_len = 0u;
    uint32_t frame_c_len = 0u;
    SapRunnerMessageV0 msg = {0};

    CHECK(db != NULL);
    CHECK(encode_message(1u, a, sizeof(a), frame_a, sizeof(frame_a), &frame_a_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(encode_message(1u, b, sizeof(b), frame_b, sizeof(frame_b), &frame_b_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(encode_message(1u, c, sizeof(c), frame_c, sizeof(frame_c), &frame_c_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_timer_v0_append(db, 100, 2u, frame_a, frame_a_len) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 90, 1u, frame_b, frame_b_len) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 110, 1u, frame_c, frame_c_len) == ERR_OK);
    CHECK(sap_runner_timer_v0_drain_due(db, 100, 8u, collect_due, &due, &processed) == ERR_OK);
    CHECK(processed == 2u);
    CHECK(due.calls == 2u);
    CHECK(due.due_ts[0] == 90);
    CHECK(due.seq[0] == 1u);
    CHECK(sap_runner_message_v0_decode(due.payloads[0], due.payload_lens[0], &msg) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(msg.payload_len == 1u);
    CHECK(msg.payload[0] == 'b');
    CHECK(due.due_ts[1] == 100);
    CHECK(due.seq[1] == 2u);
    CHECK(sap_runner_message_v0_decode(due.payloads[1], due.payload_lens[1], &msg) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(msg.payload_len == 1u);
    CHECK(msg.payload[0] == 'a');

    CHECK(timer_get(db, 90, 1u, &val, &val_len) == ERR_NOT_FOUND);
    CHECK(timer_get(db, 100, 2u, &val, &val_len) == ERR_NOT_FOUND);
    CHECK(timer_get(db, 110, 1u, &val, &val_len) == ERR_OK);
    CHECK(sap_runner_message_v0_decode((const uint8_t *)val, val_len, &msg) == SAP_RUNNER_WIRE_OK);
    CHECK(msg.payload_len == 1u);
    CHECK(msg.payload[0] == 'c');
    free((void *)val);

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
    uint8_t frame[128];
    uint32_t frame_len = 0u;

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
        if (encode_message(7u, timer_payload, sizeof(timer_payload), frame, sizeof(frame),
                           &frame_len) != SAP_RUNNER_WIRE_OK)
        {
            return ERR_CORRUPT;
        }
        intent.message = frame;
        intent.message_len = frame_len;
    }
    else
    {
        intent.kind = SAP_RUNNER_INTENT_KIND_OUTBOX_EMIT;
        intent.flags = 0u;
        intent.due_ts = 0;
        if (encode_message(7u, outbox_payload, sizeof(outbox_payload), frame, sizeof(frame),
                           &frame_len) != SAP_RUNNER_WIRE_OK)
        {
            return ERR_CORRUPT;
        }
        intent.message = frame;
        intent.message_len = frame_len;
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
    SapRunnerMessageV0 msg = {0};

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
    CHECK(sap_runner_message_v0_decode((const uint8_t *)val, val_len, &msg) == SAP_RUNNER_WIRE_OK);
    CHECK(msg.payload_len == 2u);
    CHECK(memcmp(msg.payload, "tm", 2u) == 0);
    free((void *)val);

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

static int test_scheduler_self_heals_missing_earliest_timer(void)
{
    DB *db = new_db();
    int64_t due = 0;
    uint32_t processed = 0u;
    DueCtx out = {0};
    const uint8_t a[] = {'a'};
    const uint8_t b[] = {'b'};
    uint8_t frame_a[128];
    uint8_t frame_b[128];
    uint32_t frame_a_len = 0u;
    uint32_t frame_b_len = 0u;
    SapRunnerMessageV0 msg = {0};

    CHECK(db != NULL);
    CHECK(encode_message(1u, a, sizeof(a), frame_a, sizeof(frame_a), &frame_a_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(encode_message(1u, b, sizeof(b), frame_b, sizeof(frame_b), &frame_b_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_timer_v0_append(db, 100, 1u, frame_a, frame_a_len) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 200, 1u, frame_b, frame_b_len) == ERR_OK);

    /* Simulate restore drift: BEPT is missing the earliest DBI timer row. */
    CHECK(force_bept_min_only(db, 200, 1u) == ERR_OK);

    CHECK(sap_runner_scheduler_v0_next_due(db, &due) == ERR_OK);
    CHECK(due == 100);

    CHECK(sap_runner_timer_v0_drain_due(db, 100, 1u, collect_due, &out, &processed) == ERR_OK);
    CHECK(processed == 1u);
    CHECK(out.calls == 1u);
    CHECK(out.due_ts[0] == 100);
    CHECK(out.seq[0] == 1u);
    CHECK(sap_runner_message_v0_decode(out.payloads[0], out.payload_lens[0], &msg) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(msg.payload_len == 1u);
    CHECK(msg.payload[0] == 'a');

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
    rc = test_scheduler_self_heals_missing_earliest_timer();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
