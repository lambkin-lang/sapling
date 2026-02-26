/*
 * runner_dead_letter_test.c - tests for phase-C dead-letter helpers
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/dead_letter_v0.h"
#include "runner/runner_v0.h"

#include <stdint.h>
#include <stdio.h>
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

static DB *new_db(void) { return db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL); }

static int ensure_runner_schema(DB *db)
{
    if (!db)
    {
        return SAP_ERROR;
    }
    if (sap_runner_v0_bootstrap_dbis(db) != SAP_OK)
    {
        return SAP_ERROR;
    }
    return sap_runner_v0_ensure_schema_version(db, 0u, 0u, 1);
}

static int encode_message(uint32_t to_worker, uint8_t payload_tag, uint8_t *buf, uint32_t buf_cap,
                          uint32_t *len_out)
{
    uint8_t msg_id[] = {'m', 'i', payload_tag};
    uint8_t payload[] = {'o', payload_tag};
    SapRunnerMessageV0 msg = {0};

    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg.flags = 0u;
    msg.to_worker = (int64_t)to_worker;
    msg.route_worker = (int64_t)to_worker;
    msg.route_timestamp = 11;
    msg.from_worker = 0;
    msg.message_id = msg_id;
    msg.message_id_len = sizeof(msg_id);
    msg.trace_id = NULL;
    msg.trace_id_len = 0u;
    msg.payload = payload;
    msg.payload_len = sizeof(payload);
    return sap_runner_message_v0_encode(&msg, buf, buf_cap, len_out);
}

static int inbox_exists(DB *db, uint64_t worker_id, uint64_t seq, int *exists_out)
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

    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
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

static int inbox_get_copy(DB *db, uint64_t worker_id, uint64_t seq, uint8_t *dst, uint32_t dst_cap,
                          uint32_t *len_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !dst || dst_cap == 0u || !len_out)
    {
        return SAP_ERROR;
    }
    *len_out = 0u;

    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
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
    *len_out = val_len;
    txn_abort(txn);
    return SAP_OK;
}

static int lease_exists(DB *db, uint64_t worker_id, uint64_t seq, int *exists_out)
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

    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
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

static int dead_letter_get_copy(DB *db, uint64_t worker_id, uint64_t seq, uint8_t *dst,
                                uint32_t dst_cap, uint32_t *len_out, int *exists_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !dst || dst_cap == 0u || !len_out || !exists_out)
    {
        return SAP_ERROR;
    }

    *len_out = 0u;
    *exists_out = 0;
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, sizeof(key), &val, &val_len);
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
    if (!val || val_len > dst_cap)
    {
        txn_abort(txn);
        return SAP_FULL;
    }
    memcpy(dst, val, val_len);
    *len_out = val_len;
    *exists_out = 1;
    txn_abort(txn);
    return SAP_OK;
}

static int dead_letter_exists(DB *db, uint64_t worker_id, uint64_t seq, int *exists_out)
{
    uint8_t tmp[512];
    uint32_t len = 0u;
    return dead_letter_get_copy(db, worker_id, seq, tmp, sizeof(tmp), &len, exists_out);
}

static int move_one_to_dead_letter(DB *db, uint64_t worker_id, uint64_t seq, uint8_t payload_tag,
                                   int32_t failure_rc, uint32_t attempts)
{
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    SapRunnerLeaseV0 lease = {0};

    if (!db)
    {
        return SAP_ERROR;
    }

    if (encode_message((uint32_t)worker_id, payload_tag, frame, sizeof(frame), &frame_len) !=
        SAP_RUNNER_WIRE_OK)
    {
        return SAP_ERROR;
    }
    if (sap_runner_v0_inbox_put(db, worker_id, seq, frame, frame_len) != SAP_OK)
    {
        return SAP_ERROR;
    }
    if (sap_runner_mailbox_v0_claim(db, worker_id, seq, worker_id, 10, 20, &lease) != SAP_OK)
    {
        return SAP_ERROR;
    }
    return sap_runner_dead_letter_v0_move(db, worker_id, seq, &lease, failure_rc, attempts);
}

static int test_move_to_dead_letter(void)
{
    DB *db = new_db();
    uint8_t dlq_raw[256];
    uint32_t dlq_len = 0u;
    int exists = 0;
    SapRunnerDeadLetterV0Record rec = {0};
    SapRunnerMessageV0 decoded = {0};

    CHECK(db != NULL);
    CHECK(ensure_runner_schema(db) == SAP_OK);
    CHECK(move_one_to_dead_letter(db, 7u, 1u, (uint8_t)'a', SAP_CONFLICT, 3u) == SAP_OK);
    CHECK(inbox_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(lease_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);

    CHECK(dead_letter_get_copy(db, 7u, 1u, dlq_raw, sizeof(dlq_raw), &dlq_len, &exists) == SAP_OK);
    CHECK(exists == 1);
    CHECK(sap_runner_dead_letter_v0_decode(dlq_raw, dlq_len, &rec) == SAP_OK);
    CHECK(rec.failure_rc == SAP_CONFLICT);
    CHECK(rec.attempts == 3u);
    CHECK(sap_runner_message_v0_decode(rec.frame, rec.frame_len, &decoded) == SAP_RUNNER_WIRE_OK);
    CHECK(decoded.to_worker == 7);
    CHECK(decoded.payload_len == 2u);
    CHECK(decoded.payload[1] == (uint8_t)'a');

    db_close(db);
    return 0;
}

static int test_move_rejects_stale_lease(void)
{
    DB *db = new_db();
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    SapRunnerLeaseV0 lease1 = {0};
    SapRunnerLeaseV0 lease2 = {0};
    int exists = 0;

    CHECK(db != NULL);
    CHECK(ensure_runner_schema(db) == SAP_OK);
    CHECK(encode_message(9u, (uint8_t)'b', frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 9u, 2u, frame, frame_len) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 9u, 2u, 9u, 10, 20, &lease1) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 9u, 2u, 10u, 30, 40, &lease2) == SAP_OK);

    CHECK(sap_runner_dead_letter_v0_move(db, 9u, 2u, &lease1, SAP_BUSY, 2u) == SAP_CONFLICT);
    CHECK(inbox_exists(db, 9u, 2u, &exists) == SAP_OK);
    CHECK(exists == 1);
    CHECK(dead_letter_exists(db, 9u, 2u, &exists) == SAP_OK);
    CHECK(exists == 0);

    CHECK(sap_runner_dead_letter_v0_move(db, 9u, 2u, &lease2, SAP_BUSY, 2u) == SAP_OK);
    CHECK(inbox_exists(db, 9u, 2u, &exists) == SAP_OK);
    CHECK(exists == 0);

    db_close(db);
    return 0;
}

typedef struct
{
    uint32_t calls;
    uint64_t worker_id[4];
    uint64_t seq[4];
    int32_t failure_rc[4];
    uint32_t attempts[4];
    uint8_t payload_tag[4];
} DrainCtx;

static int collect_dead_letter(uint64_t worker_id, uint64_t seq,
                               const SapRunnerDeadLetterV0Record *record, void *ctx)
{
    DrainCtx *drain = (DrainCtx *)ctx;
    SapRunnerMessageV0 msg = {0};
    int rc;

    if (!drain || !record || !record->frame || record->frame_len == 0u || drain->calls >= 4u)
    {
        return SAP_ERROR;
    }
    rc = sap_runner_message_v0_decode(record->frame, record->frame_len, &msg);
    if (rc != SAP_RUNNER_WIRE_OK || msg.payload_len < 2u)
    {
        return SAP_ERROR;
    }

    drain->worker_id[drain->calls] = worker_id;
    drain->seq[drain->calls] = seq;
    drain->failure_rc[drain->calls] = record->failure_rc;
    drain->attempts[drain->calls] = record->attempts;
    drain->payload_tag[drain->calls] = msg.payload[1];
    drain->calls++;
    return SAP_OK;
}

static int test_drain_dead_letter_records(void)
{
    DB *db = new_db();
    DrainCtx drain = {0};
    uint32_t processed = 0u;
    int exists = 0;

    CHECK(db != NULL);
    CHECK(ensure_runner_schema(db) == SAP_OK);
    CHECK(move_one_to_dead_letter(db, 3u, 10u, (uint8_t)'x', SAP_CONFLICT, 4u) == SAP_OK);
    CHECK(move_one_to_dead_letter(db, 4u, 11u, (uint8_t)'y', SAP_BUSY, 2u) == SAP_OK);

    CHECK(sap_runner_dead_letter_v0_drain(db, 8u, collect_dead_letter, &drain, &processed) ==
          SAP_OK);
    CHECK(processed == 2u);
    CHECK(drain.calls == 2u);
    CHECK(drain.worker_id[0] == 3u);
    CHECK(drain.seq[0] == 10u);
    CHECK(drain.failure_rc[0] == SAP_CONFLICT);
    CHECK(drain.attempts[0] == 4u);
    CHECK(drain.payload_tag[0] == (uint8_t)'x');
    CHECK(drain.worker_id[1] == 4u);
    CHECK(drain.seq[1] == 11u);
    CHECK(drain.failure_rc[1] == SAP_BUSY);
    CHECK(drain.attempts[1] == 2u);
    CHECK(drain.payload_tag[1] == (uint8_t)'y');

    CHECK(dead_letter_exists(db, 3u, 10u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(dead_letter_exists(db, 4u, 11u, &exists) == SAP_OK);
    CHECK(exists == 0);

    db_close(db);
    return 0;
}

static int test_replay_dead_letter_record(void)
{
    DB *db = new_db();
    uint8_t inbox_frame[128];
    uint32_t inbox_frame_len = 0u;
    SapRunnerMessageV0 msg = {0};
    int exists = 0;

    CHECK(db != NULL);
    CHECK(ensure_runner_schema(db) == SAP_OK);
    CHECK(move_one_to_dead_letter(db, 11u, 3u, (uint8_t)'r', SAP_BUSY, 5u) == SAP_OK);

    CHECK(sap_runner_dead_letter_v0_replay(db, 11u, 3u, 30u) == SAP_OK);
    CHECK(dead_letter_exists(db, 11u, 3u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(inbox_exists(db, 11u, 30u, &exists) == SAP_OK);
    CHECK(exists == 1);
    CHECK(inbox_get_copy(db, 11u, 30u, inbox_frame, sizeof(inbox_frame), &inbox_frame_len) ==
          SAP_OK);
    CHECK(sap_runner_message_v0_decode(inbox_frame, inbox_frame_len, &msg) == SAP_RUNNER_WIRE_OK);
    CHECK(msg.to_worker == 11);
    CHECK(msg.payload_len == 2u);
    CHECK(msg.payload[1] == (uint8_t)'r');

    CHECK(move_one_to_dead_letter(db, 11u, 4u, (uint8_t)'s', SAP_BUSY, 1u) == SAP_OK);
    CHECK(sap_runner_v0_inbox_put(db, 11u, 31u, inbox_frame, inbox_frame_len) == SAP_OK);
    CHECK(sap_runner_dead_letter_v0_replay(db, 11u, 4u, 31u) == SAP_EXISTS);
    CHECK(dead_letter_exists(db, 11u, 4u, &exists) == SAP_OK);
    CHECK(exists == 1);

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

    rc = test_move_to_dead_letter();
    if (rc != 0)
    {
        fprintf(stderr, "runner_dead_letter_test: failure line=%d\n", rc);
        return 1;
    }
    rc = test_move_rejects_stale_lease();
    if (rc != 0)
    {
        fprintf(stderr, "runner_dead_letter_test: failure line=%d\n", rc);
        return 1;
    }
    rc = test_drain_dead_letter_records();
    if (rc != 0)
    {
        fprintf(stderr, "runner_dead_letter_test: failure line=%d\n", rc);
        return 1;
    }
    rc = test_replay_dead_letter_record();
    if (rc != 0)
    {
        fprintf(stderr, "runner_dead_letter_test: failure line=%d\n", rc);
        return 1;
    }
    return 0;
}
