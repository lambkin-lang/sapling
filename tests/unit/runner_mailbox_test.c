/*
 * runner_mailbox_test.c - tests for phase-C mailbox lease scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/mailbox_v0.h"
#include "runner/runner_v0.h"
#include "runner/wit_wire_bridge_v0.h"

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

static int encode_message(uint32_t to_worker, uint8_t payload_tag, uint8_t *buf, uint32_t buf_cap,
                          uint32_t *len_out)
{
    uint8_t msg_id[] = {'m', 'b', payload_tag};
    uint8_t payload[] = {'p', payload_tag};
    SapRunnerMessageV0 msg = {0};

    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg.flags = 0u;
    msg.to_worker = (int64_t)to_worker;
    msg.route_worker = (int64_t)to_worker;
    msg.route_timestamp = 1;
    msg.from_worker = 0;
    msg.message_id = msg_id;
    msg.message_id_len = sizeof(msg_id);
    msg.trace_id = NULL;
    msg.trace_id_len = 0u;
    msg.payload = payload;
    msg.payload_len = sizeof(payload);
    return sap_runner_message_v0_encode(&msg, buf, buf_cap, len_out);
}

static int inbox_put(DB *db, uint64_t worker_id, uint64_t seq, const uint8_t *frame,
                     uint32_t frame_len)
{
    return sap_runner_v0_inbox_put(db, worker_id, seq, frame, frame_len);
}

static int inbox_get(DB *db, uint64_t worker_id, uint64_t seq, const void **val_out,
                     uint32_t *val_len_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
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
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

static int inbox_get_copy_wire(DB *db, uint64_t worker_id, uint64_t seq, uint8_t *dst,
                               uint32_t dst_cap, uint32_t *len_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    const void *val = NULL;
    uint32_t val_len = 0u;
    int rc;

    if (!db || !dst || dst_cap == 0u || !len_out)
    {
        return ERR_INVALID;
    }
    *len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_INVALID;
    }
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), &val, &val_len);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }
    if (!val || val_len == 0u)
    {
        txn_abort(txn);
        return ERR_CORRUPT;
    }
    if (sap_runner_wit_wire_v0_value_is_dbi1_inbox(val, val_len))
    {
        uint8_t *wire = NULL;
        uint32_t wire_len = 0u;

        rc = sap_runner_wit_wire_v0_decode_dbi1_inbox_value_to_wire((const uint8_t *)val, val_len,
                                                                     &wire, &wire_len);
        txn_abort(txn);
        if (rc != ERR_OK)
        {
            return rc;
        }
        if (wire_len > dst_cap)
        {
            free(wire);
            return ERR_FULL;
        }
        memcpy(dst, wire, wire_len);
        *len_out = wire_len;
        free(wire);
        return ERR_OK;
    }
    txn_abort(txn);
    return ERR_CORRUPT;
}

static int lease_get(DB *db, uint64_t worker_id, uint64_t seq, const void **val_out,
                     uint32_t *val_len_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
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
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

static int test_claim_busy_and_takeover(void)
{
    DB *db = new_db();
    SapRunnerLeaseV0 lease = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;

    CHECK(db != NULL);
    CHECK(encode_message(7u, (uint8_t)'a', frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(inbox_put(db, 7u, 1u, frame, frame_len) == ERR_OK);

    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 1u, 7u, 100, 150, &lease) == ERR_OK);
    CHECK(lease.owner_worker == 7u);
    CHECK(lease.deadline_ts == 150);
    CHECK(lease.attempts == 1u);

    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 1u, 8u, 120, 220, &lease) == ERR_BUSY);
    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 1u, 8u, 200, 260, &lease) == ERR_OK);
    CHECK(lease.owner_worker == 8u);
    CHECK(lease.deadline_ts == 260);
    CHECK(lease.attempts == 2u);

    db_close(db);
    return 0;
}

static int test_ack_removes_inbox_and_lease(void)
{
    DB *db = new_db();
    SapRunnerLeaseV0 lease = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(encode_message(9u, (uint8_t)'b', frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(inbox_put(db, 9u, 5u, frame, frame_len) == ERR_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 9u, 5u, 9u, 10, 20, &lease) == ERR_OK);
    CHECK(sap_runner_mailbox_v0_ack(db, 9u, 5u, &lease) == ERR_OK);

    CHECK(inbox_get(db, 9u, 5u, &val, &val_len) == ERR_NOT_FOUND);
    CHECK(lease_get(db, 9u, 5u, &val, &val_len) == ERR_NOT_FOUND);

    db_close(db);
    return 0;
}

static int test_ack_rejects_stale_lease_token(void)
{
    DB *db = new_db();
    SapRunnerLeaseV0 lease1 = {0};
    SapRunnerLeaseV0 lease2 = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;

    CHECK(db != NULL);
    CHECK(encode_message(3u, (uint8_t)'c', frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(inbox_put(db, 3u, 11u, frame, frame_len) == ERR_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 3u, 11u, 3u, 0, 5, &lease1) == ERR_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 3u, 11u, 4u, 10, 20, &lease2) == ERR_OK);
    CHECK(sap_runner_mailbox_v0_ack(db, 3u, 11u, &lease1) == ERR_CONFLICT);
    CHECK(sap_runner_mailbox_v0_ack(db, 3u, 11u, &lease2) == ERR_OK);

    db_close(db);
    return 0;
}

static int test_requeue_moves_message_and_clears_lease(void)
{
    DB *db = new_db();
    SapRunnerLeaseV0 lease = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    const void *old_val = NULL;
    uint32_t old_len = 0u;
    uint8_t new_wire[128];
    uint32_t new_wire_len = 0u;

    CHECK(db != NULL);
    CHECK(encode_message(12u, (uint8_t)'d', frame, sizeof(frame), &frame_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(inbox_put(db, 12u, 50u, frame, frame_len) == ERR_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 12u, 50u, 12u, 100, 150, &lease) == ERR_OK);
    CHECK(sap_runner_mailbox_v0_requeue(db, 12u, 50u, &lease, 60u) == ERR_OK);

    CHECK(inbox_get(db, 12u, 50u, &old_val, &old_len) == ERR_NOT_FOUND);
    CHECK(lease_get(db, 12u, 50u, &old_val, &old_len) == ERR_NOT_FOUND);
    CHECK(inbox_get_copy_wire(db, 12u, 60u, new_wire, sizeof(new_wire), &new_wire_len) == ERR_OK);
    CHECK(new_wire_len == frame_len);
    CHECK(memcmp(new_wire, frame, frame_len) == 0);

    db_close(db);
    return 0;
}

static int test_inbox_put_rejects_non_wire_frame(void)
{
    DB *db = new_db();
    const uint8_t bad[] = {'n', 'o', 'p', 'e'};

    CHECK(db != NULL);
    CHECK(sap_runner_v0_inbox_put(db, 1u, 1u, bad, sizeof(bad)) == ERR_INVALID);

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

    rc = test_claim_busy_and_takeover();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_ack_removes_inbox_and_lease();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_ack_rejects_stale_lease_token();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_requeue_moves_message_and_clears_lease();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_inbox_put_rejects_non_wire_frame();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
