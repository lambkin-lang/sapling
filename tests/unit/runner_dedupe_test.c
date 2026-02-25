/*
 * runner_dedupe_test.c - tests for exactly-once integrity (DBI 5)
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/dedupe_v0.h"
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

static int test_dedupe_encode_decode(void)
{
    SapRunnerDedupeV0 in = {0};
    SapRunnerDedupeV0 out = {0};
    uint8_t raw[SAP_RUNNER_DEDUPE_V0_VALUE_SIZE];
    uint8_t checksum[] = {0x01, 0x02, 0x03, 0x04};

    in.accepted = 1;
    in.last_seen_ts = 123456789LL;
    in.checksum_len = sizeof(checksum);
    memcpy(in.checksum, checksum, sizeof(checksum));

    sap_runner_dedupe_v0_encode(&in, raw);
    CHECK(sap_runner_dedupe_v0_decode(raw, sizeof(raw), &out) == SAP_OK);

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
    CHECK(dbi_open(db, SAP_WIT_DBI_DEDUPE, NULL, NULL, 0u) == SAP_OK);

    in.accepted = 1;
    in.last_seen_ts = 999;
    in.checksum_len = 0u;

    txn = txn_begin(db, NULL, 0u);
    CHECK(txn != NULL);
    CHECK(sap_runner_dedupe_v0_put(txn, mid, (uint32_t)strlen(mid), &in) == SAP_OK);
    CHECK(txn_commit(txn) == SAP_OK);

    txn = txn_begin(db, NULL, TXN_RDONLY);
    CHECK(txn != NULL);
    rc = sap_runner_dedupe_v0_get(txn, mid, (uint32_t)strlen(mid), &out);
    CHECK(rc == SAP_OK);
    CHECK(out.accepted == 1);
    CHECK(out.last_seen_ts == 999);
    txn_abort(txn);

    db_close(db);
    return 0;
}

int main(void)
{
    if (test_dedupe_encode_decode() != 0)
    {
        return 1;
    }
    if (test_dedupe_storage() != 0)
    {
        return 2;
    }
    return 0;
}
