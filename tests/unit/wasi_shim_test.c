/*
 * wasi_shim_test.c - tests for phase-A runner <-> wasi shim wiring
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/runner_v0.h"
#include "wasi/runtime_v0.h"
#include "wasi/shim_v0.h"

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
    uint32_t calls;
    int rc;
    uint8_t reply[16];
    uint32_t reply_len;
} GuestCtx;

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

static int encode_message(uint32_t to_worker, uint8_t *buf, uint32_t buf_len, uint32_t *out_len)
{
    const uint8_t msg_id[] = {'m', 'i', 'd'};
    const uint8_t payload[] = {'i', 'n'};
    SapRunnerMessageV0 msg = {0};

    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg.flags = 0u;
    msg.to_worker = (int64_t)to_worker;
    msg.route_worker = 99;
    msg.route_timestamp = 1234;
    msg.from_worker = 0;
    msg.message_id = msg_id;
    msg.message_id_len = sizeof(msg_id);
    msg.trace_id = NULL;
    msg.trace_id_len = 0u;
    msg.payload = payload;
    msg.payload_len = sizeof(payload);
    return sap_runner_message_v0_encode(&msg, buf, buf_len, out_len);
}

static int guest_call(void *ctx, SapHostV0 *host, const uint8_t *request, uint32_t request_len,
                      uint8_t *reply_buf, uint32_t reply_buf_cap, uint32_t *reply_len_out)
{
    GuestCtx *g = (GuestCtx *)ctx;
    (void)host;
    (void)request;
    (void)request_len;
    if (!g || !reply_buf || !reply_len_out)
    {
        return ERR_INVALID;
    }
    g->calls++;
    if (g->rc != ERR_OK)
    {
        return g->rc;
    }
    if (g->reply_len > reply_buf_cap)
    {
        return ERR_INVALID;
    }
    memcpy(reply_buf, g->reply, g->reply_len);
    *reply_len_out = g->reply_len;
    return ERR_OK;
}

static int outbox_get(DB *db, uint64_t seq, const void **val_out, uint32_t *val_len_out,
                      int *exists_out)
{
    Txn *txn;
    uint8_t key[SAP_WASI_SHIM_V0_OUTBOX_KEY_SIZE];
    int rc;

    if (!db || !val_out || !val_len_out || !exists_out)
    {
        return ERR_INVALID;
    }

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_INVALID;
    }
    sap_wasi_shim_v0_outbox_key_encode(seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_OUTBOX, key, sizeof(key), val_out, val_len_out);
    if (rc == ERR_OK)
    {
        *exists_out = 1;
        txn_abort(txn);
        return ERR_OK;
    }
    if (rc == ERR_NOT_FOUND)
    {
        *exists_out = 0;
        txn_abort(txn);
        return ERR_OK;
    }
    txn_abort(txn);
    return rc;
}

static int inbox_exists(DB *db, uint64_t worker, uint64_t seq, int *exists_out)
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

    txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
    {
        return ERR_INVALID;
    }
    sap_runner_v0_inbox_key_encode(worker, seq, key);
    rc = txn_get_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), &val, &val_len);
    txn_abort(txn);

    if (rc == ERR_OK)
    {
        *exists_out = 1;
        return ERR_OK;
    }
    if (rc == ERR_NOT_FOUND)
    {
        *exists_out = 0;
        return ERR_OK;
    }
    return rc;
}

static int test_worker_shim_outbox_path(void)
{
    DB *db = new_db();
    SapRunnerV0Config cfg;
    SapRunnerV0Worker worker;
    SapWasiShimV0 shim;
    SapWasiRuntimeV0 runtime;
    GuestCtx guest = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;
    const void *out_val = NULL;
    uint32_t out_len = 0u;
    SapRunnerMessageV0 out_msg = {0};

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    guest.rc = ERR_OK;
    guest.reply[0] = 'o';
    guest.reply[1] = 'k';
    guest.reply_len = 2u;

    CHECK(sap_wasi_runtime_v0_init(&runtime, "guest.main", guest_call, &guest) == ERR_OK);
    CHECK(sap_wasi_shim_v0_init(&shim, db, &runtime, 100u, 1) == ERR_OK);
    CHECK(sap_wasi_shim_v0_worker_init(&worker, &cfg, &shim, 4u) == ERR_OK);

    CHECK(encode_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len) == ERR_OK);

    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == ERR_OK);
    CHECK(processed == 1u);
    CHECK(guest.calls == 1u);
    CHECK(runtime.calls == 1u);
    CHECK(runtime.last_rc == ERR_OK);
    CHECK(shim.last_attempt_stats.attempts == 1u);
    CHECK(shim.last_attempt_stats.retries == 0u);
    CHECK(shim.last_attempt_stats.last_rc == ERR_OK);
    CHECK(shim.next_outbox_seq == 101u);

    CHECK(outbox_get(db, 100u, &out_val, &out_len, &exists) == ERR_OK);
    CHECK(exists == 1);
    CHECK(sap_runner_message_v0_decode((const uint8_t *)out_val, out_len, &out_msg) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(out_msg.kind == SAP_RUNNER_MESSAGE_KIND_EVENT);
    CHECK(out_msg.to_worker == 99);
    CHECK(out_msg.payload_len == 2u);
    CHECK(memcmp(out_msg.payload, "ok", 2u) == 0);

    CHECK(inbox_exists(db, 7u, 1u, &exists) == ERR_OK);
    CHECK(exists == 0);

    db_close(db);
    return 0;
}

static int test_worker_shim_retryable_error_requeues_inbox(void)
{
    DB *db = new_db();
    SapRunnerV0Config cfg;
    SapRunnerV0Worker worker;
    SapWasiShimV0 shim;
    SapWasiRuntimeV0 runtime;
    GuestCtx guest = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    guest.rc = ERR_CONFLICT;
    guest.reply_len = 0u;

    CHECK(sap_wasi_runtime_v0_init(&runtime, "guest.main", guest_call, &guest) == ERR_OK);
    CHECK(sap_wasi_shim_v0_init(&shim, db, &runtime, 0u, 1) == ERR_OK);
    CHECK(sap_wasi_shim_v0_worker_init(&worker, &cfg, &shim, 1u) == ERR_OK);
    CHECK(encode_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 55u, frame, frame_len) == ERR_OK);

    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == ERR_OK);
    CHECK(processed == 0u);
    CHECK(worker.last_error == ERR_OK);
    CHECK(guest.calls == shim.attempt_policy.max_retries + 1u);
    CHECK(runtime.calls == shim.attempt_policy.max_retries + 1u);
    CHECK(runtime.last_rc == ERR_CONFLICT);
    CHECK(shim.last_attempt_stats.attempts == shim.attempt_policy.max_retries + 1u);
    CHECK(shim.last_attempt_stats.retries == shim.attempt_policy.max_retries);
    CHECK(shim.last_attempt_stats.conflict_retries == shim.attempt_policy.max_retries);
    CHECK(shim.last_attempt_stats.last_rc == ERR_CONFLICT);

    CHECK(inbox_exists(db, 7u, 55u, &exists) == ERR_OK);
    CHECK(exists == 0);
    CHECK(inbox_exists(db, 7u, 56u, &exists) == ERR_OK);
    CHECK(exists == 1);

    db_close(db);
    return 0;
}

static int test_worker_shim_fatal_error_requeues_and_returns_error(void)
{
    DB *db = new_db();
    SapRunnerV0Config cfg;
    SapRunnerV0Worker worker;
    SapWasiShimV0 shim;
    SapWasiRuntimeV0 runtime;
    GuestCtx guest = {0};
    uint8_t frame[128];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    guest.rc = ERR_INVALID;
    guest.reply_len = 0u;

    CHECK(sap_wasi_runtime_v0_init(&runtime, "guest.main", guest_call, &guest) == ERR_OK);
    CHECK(sap_wasi_shim_v0_init(&shim, db, &runtime, 0u, 1) == ERR_OK);
    CHECK(sap_wasi_shim_v0_worker_init(&worker, &cfg, &shim, 1u) == ERR_OK);
    CHECK(encode_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 77u, frame, frame_len) == ERR_OK);

    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == ERR_INVALID);
    CHECK(processed == 0u);
    CHECK(worker.last_error == ERR_INVALID);
    CHECK(guest.calls == 1u);
    CHECK(runtime.calls == 1u);
    CHECK(runtime.last_rc == ERR_INVALID);
    CHECK(shim.last_attempt_stats.attempts == 1u);
    CHECK(shim.last_attempt_stats.retries == 0u);
    CHECK(shim.last_attempt_stats.last_rc == ERR_INVALID);

    CHECK(inbox_exists(db, 7u, 77u, &exists) == ERR_OK);
    CHECK(exists == 0);
    CHECK(inbox_exists(db, 7u, 78u, &exists) == ERR_OK);
    CHECK(exists == 1);

    db_close(db);
    return 0;
}

static int test_worker_shim_custom_reply_cap(void)
{
    DB *db = new_db();
    SapRunnerV0Config cfg;
    SapRunnerV0Worker worker;
    SapWasiShimV0 shim;
    SapWasiRuntimeV0 runtime;
    SapWasiShimV0Options options = {0};
    GuestCtx guest = {0};
    uint8_t frame[128];
    uint8_t reply_buf[2];
    const void *out_val = NULL;
    uint32_t out_len = 0u;
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int exists = 0;

    CHECK(db != NULL);
    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    guest.rc = ERR_OK;
    guest.reply[0] = 'o';
    guest.reply[1] = 'v';
    guest.reply[2] = 'r';
    guest.reply_len = 3u;

    sap_wasi_shim_v0_options_default(&options);
    options.initial_outbox_seq = 0u;
    options.emit_outbox_events = 1;
    options.reply_buf = reply_buf;
    options.reply_buf_cap = (uint32_t)sizeof(reply_buf);

    CHECK(sap_wasi_runtime_v0_init(&runtime, "guest.main", guest_call, &guest) == ERR_OK);
    CHECK(sap_wasi_shim_v0_init_with_options(&shim, db, &runtime, &options) == ERR_OK);
    CHECK(shim.reply_buf == reply_buf);
    CHECK(shim.reply_buf_cap == (uint32_t)sizeof(reply_buf));
    CHECK(sap_wasi_shim_v0_worker_init(&worker, &cfg, &shim, 1u) == ERR_OK);
    CHECK(encode_message(7u, frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_v0_inbox_put(db, 7u, 91u, frame, frame_len) == ERR_OK);

    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == ERR_INVALID);
    CHECK(processed == 0u);
    CHECK(guest.calls == 1u);
    CHECK(runtime.calls == 1u);
    CHECK(runtime.last_rc == ERR_INVALID);
    CHECK(shim.last_attempt_stats.attempts == 1u);
    CHECK(shim.last_attempt_stats.last_rc == ERR_INVALID);

    CHECK(inbox_exists(db, 7u, 91u, &exists) == ERR_OK);
    CHECK(exists == 0);
    CHECK(inbox_exists(db, 7u, 92u, &exists) == ERR_OK);
    CHECK(exists == 1);
    CHECK(outbox_get(db, 0u, &out_val, &out_len, &exists) == ERR_OK);
    CHECK(exists == 0);

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

    if (test_worker_shim_outbox_path() != 0)
    {
        return 1;
    }
    if (test_worker_shim_retryable_error_requeues_inbox() != 0)
    {
        return 2;
    }
    if (test_worker_shim_fatal_error_requeues_and_returns_error() != 0)
    {
        return 3;
    }
    if (test_worker_shim_custom_reply_cap() != 0)
    {
        return 4;
    }
    return 0;
}
