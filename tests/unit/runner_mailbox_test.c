/*
 * runner_mailbox_test.c - tests for phase-C mailbox lease scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/mailbox_v0.h"
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

static SapMemArena *g_alloc = NULL;

static DB *new_db(void)
{
    DB *db = db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
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
        return SAP_ERROR;
    }
    *val_out = NULL;
    *val_len_out = 0u;

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

static int lease_get(DB *db, uint64_t worker_id, uint64_t seq, const void **val_out,
                     uint32_t *val_len_out)
{
    Txn *txn;
    uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
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
    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_LEASES, key, sizeof(key), val_out, val_len_out);
    txn_abort(txn);
    return rc;
}

static int test_claim_busy_and_takeover(void)
{
    DB *db = new_db();
    SapRunnerLeaseV0 lease = {0};
    const uint8_t frame[] = {'m', 's', 'g'};

    CHECK(db != NULL);
    CHECK(inbox_put(db, 7u, 1u, frame, sizeof(frame)) == SAP_OK);

    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 1u, 7u, 100, 150, &lease) == SAP_OK);
    CHECK(lease.owner_worker == 7u);
    CHECK(lease.deadline_ts == 150);
    CHECK(lease.attempts == 1u);

    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 1u, 8u, 120, 220, &lease) == SAP_BUSY);
    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 1u, 8u, 200, 260, &lease) == SAP_OK);
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
    const uint8_t frame[] = {'a', 'c', 'k'};
    const void *val = NULL;
    uint32_t val_len = 0u;

    CHECK(db != NULL);
    CHECK(inbox_put(db, 9u, 5u, frame, sizeof(frame)) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 9u, 5u, 9u, 10, 20, &lease) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_ack(db, 9u, 5u, &lease) == SAP_OK);

    CHECK(inbox_get(db, 9u, 5u, &val, &val_len) == SAP_NOTFOUND);
    CHECK(lease_get(db, 9u, 5u, &val, &val_len) == SAP_NOTFOUND);

    db_close(db);
    return 0;
}

static int test_ack_rejects_stale_lease_token(void)
{
    DB *db = new_db();
    SapRunnerLeaseV0 lease1 = {0};
    SapRunnerLeaseV0 lease2 = {0};
    const uint8_t frame[] = {'s', 't', 'a', 'l', 'e'};

    CHECK(db != NULL);
    CHECK(inbox_put(db, 3u, 11u, frame, sizeof(frame)) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 3u, 11u, 3u, 0, 5, &lease1) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 3u, 11u, 4u, 10, 20, &lease2) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_ack(db, 3u, 11u, &lease1) == SAP_CONFLICT);
    CHECK(sap_runner_mailbox_v0_ack(db, 3u, 11u, &lease2) == SAP_OK);

    db_close(db);
    return 0;
}

static int test_requeue_moves_message_and_clears_lease(void)
{
    DB *db = new_db();
    SapRunnerLeaseV0 lease = {0};
    const uint8_t frame[] = {'r', 'e', 'q'};
    const void *old_val = NULL;
    const void *new_val = NULL;
    uint32_t old_len = 0u;
    uint32_t new_len = 0u;

    CHECK(db != NULL);
    CHECK(inbox_put(db, 12u, 50u, frame, sizeof(frame)) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_claim(db, 12u, 50u, 12u, 100, 150, &lease) == SAP_OK);
    CHECK(sap_runner_mailbox_v0_requeue(db, 12u, 50u, &lease, 60u) == SAP_OK);

    CHECK(inbox_get(db, 12u, 50u, &old_val, &old_len) == SAP_NOTFOUND);
    CHECK(lease_get(db, 12u, 50u, &old_val, &old_len) == SAP_NOTFOUND);
    CHECK(inbox_get(db, 12u, 60u, &new_val, &new_len) == SAP_OK);
    CHECK(new_len == sizeof(frame));
    CHECK(memcmp(new_val, frame, sizeof(frame)) == 0);

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
    return 0;
}
