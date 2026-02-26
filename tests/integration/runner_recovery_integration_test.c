/*
 * runner_recovery_integration_test.c - runner checkpoint/restore recovery checks
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/dead_letter_v0.h"
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

typedef struct
{
    uint8_t *data;
    uint32_t len;
    uint32_t cap;
    uint32_t pos;
} MemBuf;

typedef struct
{
    uint32_t calls;
    uint8_t last_payload_tag;
} DispatchCtx;

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

static int membuf_write(const void *buf, uint32_t len, void *ctx)
{
    MemBuf *mb = (MemBuf *)ctx;
    uint8_t *new_data;
    uint32_t new_cap;

    if (!mb || (!buf && len > 0u))
    {
        return -1;
    }
    if (len == 0u)
    {
        return 0;
    }
    if (mb->len > (UINT32_MAX - len))
    {
        return -1;
    }
    if (mb->len + len > mb->cap)
    {
        new_cap = (mb->cap == 0u) ? 256u : mb->cap;
        while (new_cap < (mb->len + len))
        {
            if (new_cap > (UINT32_MAX / 2u))
            {
                return -1;
            }
            new_cap *= 2u;
        }
        new_data = (uint8_t *)realloc(mb->data, (size_t)new_cap);
        if (!new_data)
        {
            return -1;
        }
        mb->data = new_data;
        mb->cap = new_cap;
    }

    memcpy(mb->data + mb->len, buf, len);
    mb->len += len;
    return 0;
}

static int membuf_read(void *buf, uint32_t len, void *ctx)
{
    MemBuf *mb = (MemBuf *)ctx;

    if (!mb || (!buf && len > 0u))
    {
        return -1;
    }
    if (mb->pos > mb->len || len > (mb->len - mb->pos))
    {
        return -1;
    }
    if (len > 0u)
    {
        memcpy(buf, mb->data + mb->pos, len);
    }
    mb->pos += len;
    return 0;
}

static int encode_message(uint32_t to_worker, uint8_t payload_tag, uint8_t *buf, uint32_t buf_cap,
                          uint32_t *written_out)
{
    uint8_t msg_id[] = {'r', 'c', payload_tag};
    uint8_t payload[] = {'p', payload_tag};
    SapRunnerMessageV0 msg = {0};

    if (!buf || !written_out)
    {
        return SAP_ERROR;
    }

    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg.flags = 0u;
    msg.to_worker = (int64_t)to_worker;
    msg.route_worker = (int64_t)to_worker;
    msg.route_timestamp = 0;
    msg.from_worker = 0;
    msg.message_id = msg_id;
    msg.message_id_len = sizeof(msg_id);
    msg.trace_id = NULL;
    msg.trace_id_len = 0u;
    msg.payload = payload;
    msg.payload_len = sizeof(payload);
    return sap_runner_message_v0_encode(&msg, buf, buf_cap, written_out);
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
    *exists_out = 0;

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
        return SAP_OK;
    }
    return rc;
}

static int dead_letter_exists(DB *db, uint64_t worker_id, uint64_t seq, int *exists_out)
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
    *exists_out = 0;

    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return SAP_ERROR;
    }
    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, sizeof(key), &val, &val_len);
    txn_abort(txn);
    if (rc == SAP_OK)
    {
        *exists_out = 1;
        return SAP_OK;
    }
    if (rc == SAP_NOTFOUND)
    {
        return SAP_OK;
    }
    return rc;
}

static int on_message(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg, void *ctx)
{
    DispatchCtx *dispatch = (DispatchCtx *)ctx;
    (void)runner;

    if (!dispatch || !msg || !msg->payload || msg->payload_len < 2u)
    {
        return SAP_ERROR;
    }
    dispatch->calls++;
    dispatch->last_payload_tag = msg->payload[1];
    return SAP_OK;
}

static int test_runner_recovery_checkpoint_restore(void)
{
    DB *db = new_db();
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg = {0};
    DispatchCtx dispatch = {0};
    uint8_t frame_a[128];
    uint8_t frame_b[128];
    uint32_t frame_a_len = 0u;
    uint32_t frame_b_len = 0u;
    uint32_t processed = 0u;
    SapRunnerLeaseV0 lease = {0};
    int exists = 0;
    MemBuf snap = {0};

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    CHECK(sap_runner_v0_init(&runner, &cfg) == SAP_OK);

    CHECK(encode_message(7u, (uint8_t)'a', frame_a, sizeof(frame_a), &frame_a_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(encode_message(7u, (uint8_t)'b', frame_b, sizeof(frame_b), &frame_b_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame_a, frame_a_len) == SAP_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 2u, frame_b, frame_b_len) == SAP_OK);

    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 2u, 7u, 10, 20, &lease) == SAP_OK);
    CHECK(sap_runner_dead_letter_v0_move(db, 7u, 2u, &lease, SAP_ERROR, 1u) == SAP_OK);
    CHECK(inbox_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 1);
    CHECK(dead_letter_exists(db, 7u, 2u, &exists) == SAP_OK);
    CHECK(exists == 1);

    CHECK(db_checkpoint(db, membuf_write, &snap) == SAP_OK);
    CHECK(snap.len > 0u);

    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch.calls == 1u);
    CHECK(dispatch.last_payload_tag == (uint8_t)'a');

    CHECK(sap_runner_dead_letter_v0_replay(db, 7u, 2u, 3u) == SAP_OK);
    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch.calls == 2u);
    CHECK(dispatch.last_payload_tag == (uint8_t)'b');

    snap.pos = 0u;
    CHECK(db_restore(db, membuf_read, &snap) == SAP_OK);

    CHECK(inbox_exists(db, 7u, 1u, &exists) == SAP_OK);
    CHECK(exists == 1);
    CHECK(inbox_exists(db, 7u, 3u, &exists) == SAP_OK);
    CHECK(exists == 0);
    CHECK(dead_letter_exists(db, 7u, 2u, &exists) == SAP_OK);
    CHECK(exists == 1);

    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch, &processed) == SAP_OK);
    CHECK(processed == 1u);
    CHECK(dispatch.calls == 3u);
    CHECK(dispatch.last_payload_tag == (uint8_t)'a');

    free(snap.data);
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

    if (test_runner_recovery_checkpoint_restore() != 0)
    {
        return 1;
    }
    return 0;
}
