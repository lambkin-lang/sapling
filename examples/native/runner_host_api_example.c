/*
 * runner_host_api_example.c - Host API usage example via attempt_handler_v0
 *
 * SPDX-License-Identifier: MIT
 */
#include "generated/wit_schema_dbis.h"
#include "runner/attempt_handler_v0.h"
#include "runner/host_v0.h"
#include "runner/intent_sink_v0.h"
#include "runner/runner_v0.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const uint8_t k_counter_key[] = {'h', 'o', 's', 't', '-', 'c', 'o', 'u', 'n', 't', 'e', 'r'};

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

static void wr64be(uint8_t out[8], uint64_t v)
{
    int i;
    for (i = 7; i >= 0; i--)
    {
        out[i] = (uint8_t)(v & 0xffu);
        v >>= 8;
    }
}

static uint64_t rd64be(const uint8_t in[8])
{
    uint64_t v = 0u;
    uint32_t i;

    for (i = 0u; i < 8u; i++)
    {
        v = (v << 8) | (uint64_t)in[i];
    }
    return v;
}

/*
 * Simulated guest entry point using the new Host API.
 * This function models the Lambkin 'atomic' block logic.
 */
static int guest_atomic_logic(void *ctx, SapHostV0 *host, const uint8_t *request,
                              uint32_t request_len, uint8_t *reply_buf, uint32_t reply_buf_cap,
                              uint32_t *reply_len_out)
{
    const void *cur = NULL;
    uint32_t cur_len = 0u;
    uint64_t count = 0u;
    uint8_t raw_count[8];
    int rc;

    (void)ctx;
    (void)reply_buf;
    (void)reply_buf_cap;
    (void)reply_len_out;

    printf("Guest logic: processing request '%.*s'\n", (int)request_len, (const char *)request);

    /* 1. Read from application state (DBI 10) */
    rc = sap_host_v0_get(host, 10u, k_counter_key, sizeof(k_counter_key), &cur, &cur_len);
    if (rc == SAP_OK)
    {
        if (cur_len == 8u)
        {
            count = rd64be((const uint8_t *)cur);
        }
    }
    else if (rc != SAP_NOTFOUND)
    {
        return rc;
    }

    /* 2. Update application state */
    count++;
    wr64be(raw_count, count);
    rc = sap_host_v0_put(host, 10u, k_counter_key, sizeof(k_counter_key), raw_count,
                         sizeof(raw_count));
    if (rc != SAP_OK)
    {
        return rc;
    }

    /* 3. Emit a message in the same atomic block */
    rc = sap_host_v0_emit(host, request, request_len);
    if (rc != SAP_OK)
    {
        return rc;
    }

    printf("Guest logic: counter incremented to %" PRIu64 "\n", count);
    return SAP_OK;
}

/* Host-side adapter that initializes SapHostV0 and calls the guest logic. */
static int host_atomic_adapter(SapRunnerTxStackV0 *stack, Txn *read_txn, SapRunnerV0 *runner,
                               const SapRunnerMessageV0 *msg, void *ctx)
{
    SapHostV0 host;
    (void)runner;

    sap_host_v0_init(&host, stack, read_txn, 123u, 0); // Using dummy worker_id and time for example

    return guest_atomic_logic(ctx, &host, msg->payload, msg->payload_len, NULL, 0u, NULL);
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

    static const uint8_t payload[] = {'h', 'e', 'l', 'l', 'o', '-', 'h',
                                      'o', 's', 't', '-', 'a', 'p', 'i'};
    static const uint8_t msg_id[] = {'m', 's', 'g', '-', '4', '2'};
    SapRunnerMessageV0 msg = {0};
    SapRunnerV0Config cfg = {0};
    SapRunnerV0Worker worker = {0};
    SapRunnerIntentSinkV0 intent_sink = {0};
    SapRunnerAttemptHandlerV0 handler = {0};
    uint8_t frame[256];
    uint32_t frame_len = 0u;
    uint32_t processed = 0u;
    int rc = 1;
    DB *db = NULL;

    db = db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        fprintf(stderr, "runner-host-api-example: db_open failed\n");
        goto done;
    }
    /* Application state DBI */
    dbi_open(db, 10u, NULL, NULL, 0u);

    cfg.db = db;
    cfg.worker_id = 7u;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;

    if (sap_runner_intent_sink_v0_init(&intent_sink, db, 1u, 1u) != SAP_OK)
    {
        fprintf(stderr, "runner-host-api-example: intent sink init failed\n");
        goto done;
    }

    if (sap_runner_attempt_handler_v0_init(&handler, db, host_atomic_adapter, NULL,
                                           sap_runner_intent_sink_v0_publish,
                                           &intent_sink) != SAP_OK)
    {
        fprintf(stderr, "runner-host-api-example: attempt handler init failed\n");
        goto done;
    }

    if (sap_runner_v0_worker_init(&worker, &cfg, sap_runner_attempt_handler_v0_runner_handler,
                                  &handler, 4u) != SAP_OK)
    {
        fprintf(stderr, "runner-host-api-example: worker init failed\n");
        goto done;
    }

    /* Prepare a message */
    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg.to_worker = 7;
    msg.route_worker = 7;
    msg.message_id = msg_id;
    msg.message_id_len = sizeof(msg_id);
    msg.payload = payload;
    msg.payload_len = sizeof(payload);

    sap_runner_message_v0_encode(&msg, frame, sizeof(frame), &frame_len);
    sap_runner_v0_inbox_put(db, 7u, 1u, frame, frame_len);

    /* Run the worker */
    if (sap_runner_v0_worker_tick(&worker, &processed) != SAP_OK || processed != 1u)
    {
        fprintf(stderr, "runner-host-api-example: worker_tick failed\n");
        goto done;
    }

    printf("runner-host-api-example: OK\n");
    rc = 0;

done:
    sap_runner_v0_worker_shutdown(&worker);
    if (db)
    {
        db_close(db);
    }
    return rc;
}
