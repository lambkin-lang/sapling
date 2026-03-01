/*
 * wasi_dedupe_test.c - tests for WASI shim exactly-once integrity (DBI 5)
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
#include <stdio.h>

#define CHECK(cond)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (!(cond))                                                                               \
        {                                                                                          \
            fprintf(stderr, "CHECK failed at %s:%d\n", __FILE__, __LINE__);                        \
            return __LINE__;                                                                       \
        }                                                                                          \
    } while (0)

typedef struct
{
    uint32_t calls;
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

static int guest_call(void *ctx, SapHostV0 *host, const uint8_t *request, uint32_t request_len,
                      uint8_t *reply_buf, uint32_t reply_buf_cap, uint32_t *reply_len_out)
{
    GuestCtx *g = (GuestCtx *)ctx;
    (void)host;
    (void)request;
    (void)request_len;
    (void)reply_buf;
    (void)reply_buf_cap;
    (void)reply_len_out;

    g->calls++;
    return ERR_OK;
}

static int encode_message(uint8_t *buf, uint32_t buf_len, uint32_t *out_len)
{
    const uint8_t msg_id[] = {'m', '1'};
    const uint8_t payload[] = {'i', 'n'};
    SapRunnerMessageV0 msg = {0};

    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg.flags = SAP_RUNNER_MESSAGE_FLAG_DEDUPE_REQUIRED;
    msg.to_worker = 7u;
    msg.message_id = msg_id;
    msg.message_id_len = sizeof(msg_id);
    msg.payload = payload;
    msg.payload_len = sizeof(payload);
    return sap_runner_message_v0_encode(&msg, buf, buf_len, out_len);
}

static int test_shim_dedupe_skips_invoke(void)
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

    CHECK(db != NULL);
    CHECK(dbi_open(db, SAP_WIT_DBI_INBOX, NULL, NULL, 0u) == ERR_OK);
    CHECK(dbi_open(db, SAP_WIT_DBI_DEDUPE, NULL, NULL, 0u) == ERR_OK);

    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    CHECK(sap_wasi_runtime_v0_init(&runtime, "guest.main", guest_call, &guest) == ERR_OK);
    CHECK(sap_wasi_shim_v0_init(&shim, db, &runtime, 0u, 0) == ERR_OK);
    CHECK(sap_wasi_shim_v0_worker_init(&worker, &cfg, &shim, 1u) == ERR_OK);

    CHECK(encode_message(frame, sizeof(frame), &frame_len) == SAP_RUNNER_WIRE_OK);

    // Attempt 1: New message
    CHECK(sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len) == ERR_OK);
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == ERR_OK);
    CHECK(processed == 1u);
    CHECK(guest.calls == 1u);

    // Attempt 2: Duplicate message (with same ID and dedupe flag)
    CHECK(sap_runner_v0_inbox_put(db, 7u, 2u, frame, frame_len) == ERR_OK);
    CHECK(sap_runner_v0_worker_tick(&worker, &processed) == ERR_OK);
    CHECK(processed == 1u);
    CHECK(guest.calls == 1u); // Should still be 1!

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

    if (test_shim_dedupe_skips_invoke() != 0)
    {
        return 1;
    }
    printf("wasi_dedupe_test PASS\n");
    return 0;
}
