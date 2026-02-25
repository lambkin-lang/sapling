/*
 * wasm_runner_test.c - end-to-end verification of WASI shim + Host API
 *
 * SPDX-License-Identifier: MIT
 */
#include "wasi/shim_v0.h"
#include "runner/host_v0.h"
#include "generated/wit_schema_dbis.h"

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

/*
 * Mock Guest Logic (C version of wasm_guest_example.c)
 */
static int mock_guest_logic(void *ctx, SapHostV0 *host, const uint8_t *request,
                            uint32_t request_len, uint8_t *reply_buf, uint32_t reply_buf_cap,
                            uint32_t *reply_len_out)
{
    const char *lease_key = "lock-1";
    const char *counter_key = "counter";
    const void *val = NULL;
    uint32_t val_len = 0;
    uint32_t counter = 0;
    int rc;

    (void)ctx;
    (void)request;
    (void)request_len;
    (void)reply_buf;
    (void)reply_buf_cap;

    /* 1. Acquire lease */
    rc = sap_host_v0_lease_acquire(host, lease_key, (uint32_t)strlen(lease_key), 5000);
    if (rc != SAP_OK)
    {
        return rc;
    }

    /* 2. Read counter (DBI 10) */
    rc = sap_host_v0_get(host, 10, counter_key, (uint32_t)strlen(counter_key), &val, &val_len);
    if (rc == SAP_OK && val_len == 4)
    {
        memcpy(&counter, val, 4);
    }
    else if (rc != SAP_OK && rc != SAP_NOTFOUND)
    {
        return rc;
    }

    /* 3. Increment and Put */
    counter++;
    rc = sap_host_v0_put(host, 10, counter_key, (uint32_t)strlen(counter_key), &counter, 4);
    if (rc != SAP_OK)
    {
        return rc;
    }

    /* 4. Release lease */
    rc = sap_host_v0_lease_release(host, lease_key, (uint32_t)strlen(lease_key));
    if (rc != SAP_OK)
    {
        return rc;
    }

    *reply_len_out = 0;
    return SAP_OK;
}

static int test_wasm_runner_end_to_end(void)
{
    DB *db = new_db();
    SapWasiRuntimeV0 runtime = {0};
    SapWasiShimV0 shim = {0};
    SapRunnerV0Worker worker = {0};
    SapRunnerV0Config cfg = {0};
    uint32_t processed = 0;
    uint8_t msg_key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
    uint8_t msg_frame[128];
    SapRunnerMessageV0 msg = {0};
    int rc;

    CHECK(db != NULL);
    rc = sap_runner_v0_bootstrap_dbis(db);
    if (rc != SAP_OK)
    {
        fprintf(stderr, "sap_runner_v0_bootstrap_dbis failed with rc=%d\n", rc);
        return 1;
    }
    /* Open a custom DBI for the counter to bypass WIT validation */
    CHECK(dbi_open(db, 10, NULL, NULL, 0) == SAP_OK);

    /* Initialize Mock Wasm Runtime */
    CHECK(sap_wasi_runtime_v0_init(&runtime, "mock_guest", mock_guest_logic, NULL) == SAP_OK);

    /* Initialize WASI Shim */
    CHECK(sap_wasi_shim_v0_init(&shim, db, &runtime, 1000u, 0) == SAP_OK);

    /* Initialize Worker */
    cfg.db = db;
    cfg.worker_id = 1u;
    cfg.bootstrap_schema_if_missing = 1;
    rc = sap_wasi_shim_v0_worker_init(&worker, &cfg, &shim, 10u);
    if (rc != SAP_OK)
    {
        fprintf(stderr, "sap_wasi_shim_v0_worker_init failed with rc=%d\n", rc);
        return 1;
    }

    /* 1. Queue a message */
    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg.to_worker = 1;
    msg.payload = (const uint8_t *)"hello";
    msg.payload_len = 5u;
    msg.message_id = (const uint8_t *)"msg-1";
    msg.message_id_len = 5u;

    sap_runner_v0_inbox_key_encode(1u, 100u, msg_key);
    uint32_t written = 0;
    rc = sap_runner_message_v0_encode(&msg, msg_frame, sizeof(msg_frame), &written);
    CHECK(rc == SAP_OK);
    CHECK(sap_runner_v0_inbox_put(db, 1u, 100u, msg_frame, written) == SAP_OK);

    /* 2. Run one step */
    rc = sap_runner_v0_worker_tick(&worker, &processed);
    CHECK(rc == SAP_OK);
    CHECK(processed == 1u);

    /* 3. Verify counter in DB */
    {
        Txn *txn = txn_begin(db, NULL, TXN_RDONLY);
        const void *val = NULL;
        uint32_t val_len = 0;
        uint32_t counter = 0;
        CHECK(txn_get_dbi(txn, 10, "counter", 7, &val, &val_len) == SAP_OK);
        CHECK(val_len == 4);
        memcpy(&counter, val, 4);
        CHECK(counter == 1);
        txn_abort(txn);
    }

    sap_runner_v0_worker_shutdown(&worker);
    db_close(db);
    return 0;
}

int main(void)
{
    if (test_wasm_runner_end_to_end() != 0)
    {
        return 1;
    }
    printf("wasm_runner_test PASS\n");
    return 0;
}
