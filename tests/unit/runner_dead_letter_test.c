/*
 * runner_dead_letter_test.c - tests for phase-C dead-letter move helper
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/dead_letter_v0.h"
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

static DB *new_db(void) { return db_open(&g_alloc, SAPLING_PAGE_SIZE, NULL, NULL); }

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

static int encode_message(uint32_t to_worker, uint8_t *buf, uint32_t buf_cap, uint32_t *len_out)
{
    const uint8_t msg_id[] = {'m', 'i', 'd'};
    const uint8_t payload[] = {'o', 'k'};
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

static int dead_letter_get(DB *db, uint64_t worker_id, uint64_t seq, const void **val_out,
                           uint32_t *val_len_out, int *exists_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    int rc;

    if (!db || !val_out || !val_len_out || !exists_out)
    {
        return SAP_ERROR;
    }

    *val_out = NULL;
    *val_len_out = 0u;
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, sizeof(key), val_out, val_len_out);
    if (rc == SAP_OK)
    {
        *exists_out = 1;
        txn_abort(txn);
        return SAP_OK;
    }
    if (rc == SAP_NOTFOUND)
    {
        *exists_out = 0;
        txn_abort(txn);
        return SAP_OK;
    }
    txn_abort(txn);
    return rc;
}

static int dead_letter_exists(DB *db, uint64_t worker_id, uint64_t seq, int *exists_out)
{
    const void *val = NULL;
    uint32_t val_len = 0u;
    return dead_letter_get(db, worker_id, seq, &val, &val_len, exists_out);
}

static int test_move_to_dead_letter(void)
{
    DB *db = new_db();
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    SapRunnerLeaseV0 lease = {0};
    const void *dlq_val = NULL;
    uint32_t dlq_len = 0u;
    SapRunnerDeadLetterV0Record rec = {0};
    SapRunnerMessageV0 decoded = {0};
    int exists = 0;

    CHECK(db != NULL);
    CHECK(ensure_runner_schema(db) == SAP_OK);
    CHECK(encode_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 1u, 7u, 10, 20, &lease) == SAP_OK);

    CHECK(sap_runner_dead_letter_v0_move(db, 7u, 1u, &lease, SAP_CONFLICT, 3u) == SAP_OK);
    CHECK(inbox_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(lease_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 0);

    CHECK(dead_letter_get(db, 7u, 1u, &dlq_val, &dlq_len, &exists) == SAP_OK);
    CHECK(exists == 1);
    CHECK(sap_runner_dead_letter_v0_decode((const uint8_t *)dlq_val, dlq_len, &rec) == SAP_OK);
    CHECK(rec.failure_rc == SAP_CONFLICT);
    CHECK(rec.attempts == 3u);
    CHECK(sap_runner_message_v0_decode(rec.frame, rec.frame_len, &decoded) == SAP_RUNNER_WIRE_OK);
    CHECK(decoded.to_worker == 7);

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
    CHECK(encode_message(9u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
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

int main(void)
{
    if (test_move_to_dead_letter() != 0)
    {
        return 1;
    }
    if (test_move_rejects_stale_lease() != 0)
    {
        return 2;
    }
    return 0;
}
