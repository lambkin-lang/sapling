/*
 * runner_dedupe_test.c - tests for exactly-once integrity (DBI 5)
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/dedupe_v0.h"
#include "runner/txstack_v0.h"
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

static SapMemArena *g_alloc = NULL;

static DB *new_db(void) { return db_open(g_alloc, SAPLING_PAGE_SIZE, NULL, NULL); }

static int test_dedupe_encode_decode(void)
{
    SapRunnerDedupeV0 in = {0};
    SapRunnerDedupeV0 out = {0};
    uint8_t raw[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE];
    uint32_t raw_len = 0u;
    uint8_t checksum[] = {0x01, 0x02, 0x03, 0x04};

    in.accepted = 1;
    in.last_seen_ts = 123456789LL;
    in.checksum_len = sizeof(checksum);
    memcpy(in.checksum, checksum, sizeof(checksum));

    raw_len = sap_runner_dedupe_v0_encoded_len(&in);
    CHECK(raw_len <= sizeof(raw));
    sap_runner_dedupe_v0_encode(&in, raw);
    CHECK(sap_runner_dedupe_v0_decode(raw, raw_len, &out) == ERR_OK);

    CHECK(out.accepted == 1);
    CHECK(out.last_seen_ts == 123456789LL);
    CHECK(out.checksum_len == 4u);
    CHECK(memcmp(out.checksum, checksum, 4u) == 0);

    return 0;
}

static int test_dedupe_storage(void)
{
    DB *db = new_db();
    Txn *txn;
    SapRunnerDedupeV0 in = {0};
    SapRunnerDedupeV0 out = {0};
    const char *mid = "msg-1";
    int rc;

    CHECK(db != NULL);
    CHECK(dbi_open(db, SAP_WIT_DBI_DEDUPE, NULL, NULL, 0u) == ERR_OK);

    in.accepted = 1;
    in.last_seen_ts = 999;
    in.checksum_len = 0u;

    txn = txn_begin(db, NULL, 0u);
    CHECK(txn != NULL);
    CHECK(sap_runner_dedupe_v0_put(txn, mid, (uint32_t)strlen(mid), &in) == ERR_OK);
    CHECK(txn_commit(txn) == ERR_OK);

    txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(txn != NULL);
    rc = sap_runner_dedupe_v0_get(txn, mid, (uint32_t)strlen(mid), &out);
    CHECK(rc == ERR_OK);
    CHECK(out.accepted == 1);
    CHECK(out.last_seen_ts == 999);
    txn_abort(txn);

    db_close(db);
    return 0;
}

static int test_dedupe_decode_guardrails(void)
{
    SapRunnerDedupeV0 in = {0};
    SapRunnerDedupeV0 out = {0};
    uint8_t raw[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE];
    uint32_t raw_len;

    in.accepted = 0;
    in.last_seen_ts = 42;
    in.checksum_len = 3u;
    in.checksum[0] = 0xa1u;
    in.checksum[1] = 0xb2u;
    in.checksum[2] = 0xc3u;
    raw_len = sap_runner_dedupe_v0_encoded_len(&in);
    sap_runner_dedupe_v0_encode(&in, raw);

    CHECK(sap_runner_dedupe_v0_decode(raw, raw_len, &out) == ERR_OK);
    CHECK(out.accepted == 0);
    CHECK(out.last_seen_ts == 42);
    CHECK(out.checksum_len == 3u);

    raw[0] = SAP_WIT_TAG_VARIANT;
    CHECK(sap_runner_dedupe_v0_decode(raw, raw_len, &out) == ERR_CORRUPT);
    raw[0] = SAP_WIT_TAG_RECORD;

    raw[1] = 0u;
    raw[2] = 0u;
    raw[3] = 0u;
    raw[4] = 0u;
    CHECK(sap_runner_dedupe_v0_decode(raw, raw_len, &out) == ERR_CORRUPT);
    raw[1] = (uint8_t)(raw_len - 5u);

    raw[5] = 0xffu;
    CHECK(sap_runner_dedupe_v0_decode(raw, raw_len, &out) == ERR_CORRUPT);
    raw[5] = SAP_WIT_TAG_BOOL_FALSE;

    raw[15] = SAP_WIT_TAG_S64;
    CHECK(sap_runner_dedupe_v0_decode(raw, raw_len, &out) == ERR_CORRUPT);
    raw[15] = SAP_WIT_TAG_BYTES;

    CHECK(sap_runner_dedupe_v0_decode(raw, 19u, &out) == ERR_INVALID);
    return 0;
}

static int test_dedupe_checksum_clamp_and_stage_put(void)
{
    DB *db = new_db();
    Txn *txn;
    Txn *guard_txn;
    Txn *read_txn;
    SapRunnerTxStackV0 stack;
    SapRunnerDedupeV0 in = {0};
    SapRunnerDedupeV0 out = {0};
    SapRunnerDedupeV0 nested = {0};
    uint32_t i;

    CHECK(db != NULL);
    CHECK(dbi_open(db, SAP_WIT_DBI_DEDUPE, NULL, NULL, 0u) == ERR_OK);

    in.accepted = 1;
    in.last_seen_ts = 777;
    in.checksum_len = SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE + 10u;
    for (i = 0u; i < SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE; i++)
    {
        in.checksum[i] = (uint8_t)(i + 1u);
    }
    CHECK(sap_runner_dedupe_v0_encoded_len(&in) == SAP_RUNNER_DEDUPE_V0_VALUE_SIZE);

    txn = txn_begin(db, NULL, 0u);
    CHECK(txn != NULL);
    CHECK(sap_runner_dedupe_v0_put(txn, "clamp", 5u, &in) == ERR_OK);
    CHECK(txn_commit(txn) == ERR_OK);

    read_txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(read_txn != NULL);
    CHECK(sap_runner_dedupe_v0_get(read_txn, "clamp", 5u, &out) == ERR_OK);
    CHECK(out.checksum_len == SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE);
    CHECK(out.checksum[0] == 1u);
    CHECK(out.checksum[SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE - 1u] ==
          SAP_RUNNER_DEDUPE_V0_CHECKSUM_SIZE);
    txn_abort(read_txn);

    sap_runner_txstack_v0_init(&stack);
    CHECK(sap_runner_txstack_v0_push(&stack) == ERR_OK);
    CHECK(sap_runner_txstack_v0_push(&stack) == ERR_OK);

    nested.accepted = 0;
    nested.last_seen_ts = 1234;
    nested.checksum_len = 0u;
    CHECK(sap_runner_dedupe_v0_stage_put(&stack, "nested", 6u, &nested) == ERR_OK);
    CHECK(sap_runner_txstack_v0_commit_top(&stack) == ERR_OK);

    txn = txn_begin(db, NULL, 0u);
    CHECK(txn != NULL);
    CHECK(sap_runner_txstack_v0_apply_root_writes(&stack, txn) == ERR_OK);
    CHECK(txn_commit(txn) == ERR_OK);
    sap_runner_txstack_v0_dispose(&stack);

    read_txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(read_txn != NULL);
    CHECK(sap_runner_dedupe_v0_get(read_txn, "nested", 6u, &out) == ERR_OK);
    CHECK(out.accepted == 0);
    CHECK(out.last_seen_ts == 1234);
    txn_abort(read_txn);

    guard_txn = txn_begin(db, NULL, 0u);
    CHECK(guard_txn != NULL);
    CHECK(sap_runner_dedupe_v0_put(NULL, "x", 1u, &in) == ERR_INVALID);
    CHECK(sap_runner_dedupe_v0_put(guard_txn, NULL, 1u, &in) == ERR_INVALID);
    CHECK(sap_runner_dedupe_v0_put(guard_txn, "x", 0u, &in) == ERR_INVALID);
    CHECK(sap_runner_dedupe_v0_put(guard_txn, "x", 1u, NULL) == ERR_INVALID);
    txn_abort(guard_txn);

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

    if (test_dedupe_encode_decode() != 0)
    {
        return 1;
    }
    if (test_dedupe_storage() != 0)
    {
        return 2;
    }
    if (test_dedupe_decode_guardrails() != 0)
    {
        return 3;
    }
    if (test_dedupe_checksum_clamp_and_stage_put() != 0)
    {
        return 4;
    }
    return 0;
}
