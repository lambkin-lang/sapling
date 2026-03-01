/*
 * runner_recovery_integration_test.c - runner checkpoint/restore recovery checks
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/dead_letter_v0.h"
#include "runner/mailbox_v0.h"
#include "runner/runner_v0.h"
#include "runner/scheduler_v0.h"
#include "runner/timer_v0.h"

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

typedef struct
{
    uint32_t calls;
    int64_t due_ts[4];
    uint64_t seq[4];
    uint8_t payloads[4][8];
    uint32_t payload_lens[4];
} TimerDrainCtx;

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
        return ERR_INVALID;
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
        return ERR_INVALID;
    }
    *exists_out = 0;

    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_INVALID;
    }
    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), &val, &val_len);
    txn_abort(txn);
    if (rc == ERR_OK)
    {
        *exists_out = 1;
        return ERR_OK;
    }
    if (rc == ERR_NOT_FOUND)
    {
        return ERR_OK;
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
        return ERR_INVALID;
    }
    *exists_out = 0;

    sap_runner_v0_inbox_key_encode(worker_id, seq, key);
    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_INVALID;
    }
    rc = txn_get_dbi(txn, SAP_WIT_DBI_DEAD_LETTER, key, sizeof(key), &val, &val_len);
    txn_abort(txn);
    if (rc == ERR_OK)
    {
        *exists_out = 1;
        return ERR_OK;
    }
    if (rc == ERR_NOT_FOUND)
    {
        return ERR_OK;
    }
    return rc;
}

static int on_message(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg, void *ctx)
{
    DispatchCtx *dispatch = (DispatchCtx *)ctx;
    (void)runner;

    if (!dispatch || !msg || !msg->payload || msg->payload_len < 2u)
    {
        return ERR_INVALID;
    }
    dispatch->calls++;
    dispatch->last_payload_tag = msg->payload[1];
    return ERR_OK;
}

static int collect_timer_due(int64_t due_ts, uint64_t seq, const uint8_t *payload,
                             uint32_t payload_len, void *ctx)
{
    TimerDrainCtx *drain = (TimerDrainCtx *)ctx;

    if (!drain || !payload || payload_len == 0u || payload_len > 8u || drain->calls >= 4u)
    {
        return ERR_INVALID;
    }
    drain->due_ts[drain->calls] = due_ts;
    drain->seq[drain->calls] = seq;
    memcpy(drain->payloads[drain->calls], payload, payload_len);
    drain->payload_lens[drain->calls] = payload_len;
    drain->calls++;
    return ERR_OK;
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
    CHECK(sap_runner_v0_init(&runner, &cfg) == ERR_OK);

    CHECK(encode_message(7u, (uint8_t)'a', frame_a, sizeof(frame_a), &frame_a_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(encode_message(7u, (uint8_t)'b', frame_b, sizeof(frame_b), &frame_b_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame_a, frame_a_len) == ERR_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 2u, frame_b, frame_b_len) == ERR_OK);

    CHECK(sap_runner_mailbox_v0_claim(db, 7u, 2u, 7u, 10, 20, &lease) == ERR_OK);
    CHECK(sap_runner_dead_letter_v0_move(db, 7u, 2u, &lease, ERR_INVALID, 1u) == ERR_OK);
    CHECK(inbox_exists(db, 7u, 1u, &exists) == ERR_OK);
    CHECK(exists == 1);
    CHECK(dead_letter_exists(db, 7u, 2u, &exists) == ERR_OK);
    CHECK(exists == 1);

    CHECK(db_checkpoint(db, membuf_write, &snap) == ERR_OK);
    CHECK(snap.len > 0u);

    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch, &processed) == ERR_OK);
    CHECK(processed == 1u);
    CHECK(dispatch.calls == 1u);
    CHECK(dispatch.last_payload_tag == (uint8_t)'a');

    CHECK(sap_runner_dead_letter_v0_replay(db, 7u, 2u, 3u) == ERR_OK);
    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch, &processed) == ERR_OK);
    CHECK(processed == 1u);
    CHECK(dispatch.calls == 2u);
    CHECK(dispatch.last_payload_tag == (uint8_t)'b');

    snap.pos = 0u;
    CHECK(db_restore(db, membuf_read, &snap) == ERR_OK);

    CHECK(inbox_exists(db, 7u, 1u, &exists) == ERR_OK);
    CHECK(exists == 1);
    CHECK(inbox_exists(db, 7u, 3u, &exists) == ERR_OK);
    CHECK(exists == 0);
    CHECK(dead_letter_exists(db, 7u, 2u, &exists) == ERR_OK);
    CHECK(exists == 1);

    CHECK(sap_runner_v0_poll_inbox(&runner, 1u, on_message, &dispatch, &processed) == ERR_OK);
    CHECK(processed == 1u);
    CHECK(dispatch.calls == 3u);
    CHECK(dispatch.last_payload_tag == (uint8_t)'a');

    free(snap.data);
    db_close(db);
    return 0;
}

static int test_timer_index_recovers_after_restore(void)
{
    DB *db = new_db();
    MemBuf snap = {0};
    TimerDrainCtx drain = {0};
    uint32_t processed = 0u;
    int64_t next_due = 0;
    const uint8_t a[] = {'a'};
    const uint8_t b[] = {'b'};

    CHECK(db != NULL);
    CHECK(sap_runner_v0_bootstrap_dbis(db) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 10, 1u, a, sizeof(a)) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 20, 1u, b, sizeof(b)) == ERR_OK);
    CHECK(db_checkpoint(db, membuf_write, &snap) == ERR_OK);
    CHECK(snap.len > 0u);

    CHECK(sap_runner_timer_v0_drain_due(db, 10, 1u, collect_timer_due, &drain, &processed) ==
          ERR_OK);
    CHECK(processed == 1u);
    CHECK(drain.calls == 1u);
    CHECK(drain.due_ts[0] == 10);
    CHECK(drain.seq[0] == 1u);
    CHECK(drain.payload_lens[0] == 1u);
    CHECK(drain.payloads[0][0] == 'a');

    snap.pos = 0u;
    CHECK(db_restore(db, membuf_read, &snap) == ERR_OK);

    CHECK(sap_runner_scheduler_v0_next_due(db, &next_due) == ERR_OK);
    CHECK(next_due == 10);

    memset(&drain, 0, sizeof(drain));
    processed = 0u;
    CHECK(sap_runner_timer_v0_drain_due(db, 20, 2u, collect_timer_due, &drain, &processed) ==
          ERR_OK);
    CHECK(processed == 2u);
    CHECK(drain.calls == 2u);
    CHECK(drain.due_ts[0] == 10);
    CHECK(drain.seq[0] == 1u);
    CHECK(drain.payload_lens[0] == 1u);
    CHECK(drain.payloads[0][0] == 'a');
    CHECK(drain.due_ts[1] == 20);
    CHECK(drain.seq[1] == 1u);
    CHECK(drain.payload_lens[1] == 1u);
    CHECK(drain.payloads[1][0] == 'b');

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
    if (test_timer_index_recovers_after_restore() != 0)
    {
        return 1;
    }
    return 0;
}
