/*
 * bench_runner_phasee.c - phase-E runner coupling study benchmark
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "generated/wit_schema_dbis.h"
#include "runner/mailbox_v0.h"
#include "runner/runner_v0.h"
#include "runner/wire_v0.h"
#include "sapling/sapling.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#define BENCH_WORKER_ID 7u
#define BENCH_MAX_FRAME_SIZE 256u

static void *bench_alloc(void *ctx, uint32_t sz)
{
    (void)ctx;
    return malloc((size_t)sz);
}

static void bench_free(void *ctx, void *p, uint32_t sz)
{
    (void)ctx;
    (void)sz;
    free(p);
}

static PageAllocator g_alloc = {bench_alloc, bench_free, NULL};

static double now_seconds(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static int parse_u32(const char *s, uint32_t *out)
{
    unsigned long v;
    char *end = NULL;

    if (!s || !*s || !out)
    {
        return 0;
    }
    v = strtoul(s, &end, 10);
    if (!end || *end != '\0' || v > 0xfffffffful)
    {
        return 0;
    }
    *out = (uint32_t)v;
    return 1;
}

static int encode_message_frame(uint32_t worker_id, uint64_t seq, uint8_t *out, uint32_t out_len,
                                uint32_t *frame_len_out)
{
    uint8_t message_id[8];
    uint8_t payload[8];
    SapRunnerMessageV0 msg = {0};
    uint32_t i;

    if (!out || !frame_len_out)
    {
        return SAP_ERROR;
    }

    for (i = 0u; i < 8u; i++)
    {
        message_id[i] = (uint8_t)((seq >> (i * 8u)) & 0xffu);
        payload[i] = (uint8_t)((seq >> (i * 8u)) & 0xffu);
    }

    msg.kind = SAP_RUNNER_MESSAGE_KIND_COMMAND;
    msg.flags = 0u;
    msg.to_worker = (int64_t)worker_id;
    msg.route_worker = (int64_t)worker_id;
    msg.route_timestamp = (int64_t)seq;
    msg.from_worker = 0;
    msg.message_id = message_id;
    msg.message_id_len = sizeof(message_id);
    msg.trace_id = NULL;
    msg.trace_id_len = 0u;
    msg.payload = payload;
    msg.payload_len = sizeof(payload);

    if (sap_runner_message_v0_encode(&msg, out, out_len, frame_len_out) != SAP_RUNNER_WIRE_OK)
    {
        return SAP_ERROR;
    }
    return SAP_OK;
}

static DB *open_bench_db(void)
{
    DB *db = db_open(&g_alloc, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        return NULL;
    }
    if (sap_runner_v0_bootstrap_dbis(db) != SAP_OK)
    {
        db_close(db);
        return NULL;
    }
    if (sap_runner_v0_ensure_schema_version(db, 0u, 0u, 1) != SAP_OK)
    {
        db_close(db);
        return NULL;
    }
    return db;
}

static int populate_inbox(DB *db, uint32_t worker_id, uint32_t count)
{
    Txn *txn;
    uint32_t i;

    if (!db)
    {
        return SAP_ERROR;
    }

    txn = txn_begin(db, NULL, 0u);
    if (!txn)
    {
        return SAP_BUSY;
    }

    for (i = 0u; i < count; i++)
    {
        uint64_t seq = (uint64_t)i + 1u;
        uint8_t key[SAP_RUNNER_INBOX_KEY_V0_SIZE];
        uint8_t frame[BENCH_MAX_FRAME_SIZE];
        uint32_t frame_len = 0u;
        int rc;

        sap_runner_v0_inbox_key_encode((uint64_t)worker_id, seq, key);
        rc = encode_message_frame(worker_id, seq, frame, sizeof(frame), &frame_len);
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            return rc;
        }
        rc = txn_put_dbi(txn, SAP_WIT_DBI_INBOX, key, sizeof(key), frame, frame_len);
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            return rc;
        }
    }

    return txn_commit(txn);
}

typedef struct
{
    uint32_t calls;
} BenchDispatchCtx;

static int bench_noop_handler(SapRunnerV0 *runner, const SapRunnerMessageV0 *msg, void *ctx)
{
    BenchDispatchCtx *state = (BenchDispatchCtx *)ctx;

    if (!runner || !msg || !state)
    {
        return SAP_ERROR;
    }
    if (msg->to_worker != (int64_t)runner->worker_id)
    {
        return SAP_NOTFOUND;
    }
    state->calls++;
    return SAP_OK;
}

static int run_baseline_round(uint32_t count, uint32_t batch, double *seconds_out)
{
    DB *db = NULL;
    SapRunnerV0 runner = {0};
    SapRunnerV0Config cfg = {0};
    BenchDispatchCtx dispatch = {0};
    uint32_t total = 0u;
    double t0;
    double t1;

    if (!seconds_out || batch == 0u)
    {
        return SAP_ERROR;
    }
    *seconds_out = 0.0;

    db = open_bench_db();
    if (!db)
    {
        return SAP_ERROR;
    }
    if (populate_inbox(db, BENCH_WORKER_ID, count) != SAP_OK)
    {
        db_close(db);
        return SAP_ERROR;
    }

    cfg.db = db;
    cfg.worker_id = BENCH_WORKER_ID;
    cfg.schema_major = 0u;
    cfg.schema_minor = 0u;
    cfg.bootstrap_schema_if_missing = 1;
    if (sap_runner_v0_init(&runner, &cfg) != SAP_OK)
    {
        db_close(db);
        return SAP_ERROR;
    }

    t0 = now_seconds();
    for (;;)
    {
        uint32_t processed = 0u;
        int rc =
            sap_runner_v0_poll_inbox(&runner, batch, bench_noop_handler, &dispatch, &processed);
        if (rc != SAP_OK)
        {
            db_close(db);
            return rc;
        }
        total += processed;
        if (processed == 0u)
        {
            break;
        }
    }
    t1 = now_seconds();

    db_close(db);
    if (total != count || dispatch.calls != count)
    {
        return SAP_ERROR;
    }
    *seconds_out = t1 - t0;
    return SAP_OK;
}

static int drain_fused_storage_candidate(DB *db, uint32_t worker_id, uint32_t *processed_out)
{
    uint32_t processed = 0u;
    uint8_t prefix[SAP_RUNNER_INBOX_KEY_V0_SIZE];

    if (!db || !processed_out)
    {
        return SAP_ERROR;
    }
    *processed_out = 0u;

    sap_runner_v0_inbox_key_encode((uint64_t)worker_id, 0u, prefix);

    for (;;)
    {
        Txn *txn = txn_begin(db, NULL, 0u);
        Cursor *cur = NULL;
        const void *key = NULL;
        const void *val = NULL;
        uint32_t key_len = 0u;
        uint32_t val_len = 0u;
        uint8_t key_copy[SAP_RUNNER_INBOX_KEY_V0_SIZE];
        uint8_t frame_copy[BENCH_MAX_FRAME_SIZE];
        uint64_t key_worker = 0u;
        uint64_t key_seq = 0u;
        SapRunnerMessageV0 msg = {0};
        SapRunnerLeaseV0 lease = {0};
        uint8_t lease_raw[SAP_RUNNER_LEASE_V0_VALUE_SIZE];
        int rc;

        if (!txn)
        {
            return SAP_BUSY;
        }
        cur = cursor_open_dbi(txn, SAP_WIT_DBI_INBOX);
        if (!cur)
        {
            txn_abort(txn);
            return SAP_ERROR;
        }

        rc = cursor_seek_prefix(cur, prefix, 8u);
        if (rc == SAP_NOTFOUND)
        {
            cursor_close(cur);
            txn_abort(txn);
            break;
        }
        if (rc != SAP_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }

        rc = cursor_get(cur, &key, &key_len, &val, &val_len);
        if (rc != SAP_OK)
        {
            cursor_close(cur);
            txn_abort(txn);
            return rc;
        }
        if (!key || !val || key_len != sizeof(key_copy) || val_len == 0u ||
            val_len > sizeof(frame_copy))
        {
            cursor_close(cur);
            txn_abort(txn);
            return SAP_ERROR;
        }
        memcpy(key_copy, key, sizeof(key_copy));
        memcpy(frame_copy, val, val_len);
        cursor_close(cur);

        rc = sap_runner_v0_inbox_key_decode(key_copy, sizeof(key_copy), &key_worker, &key_seq);
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            return rc;
        }
        if (key_worker != worker_id)
        {
            txn_abort(txn);
            break;
        }

        if (sap_runner_message_v0_decode(frame_copy, val_len, &msg) != SAP_RUNNER_WIRE_OK)
        {
            txn_abort(txn);
            return SAP_ERROR;
        }
        if (msg.to_worker != (int64_t)worker_id)
        {
            txn_abort(txn);
            return SAP_ERROR;
        }

        lease.owner_worker = (uint64_t)worker_id;
        lease.deadline_ts = (int64_t)key_seq + 1;
        lease.attempts = 1u;
        sap_runner_lease_v0_encode(&lease, lease_raw);

        rc = txn_put_dbi(txn, SAP_WIT_DBI_LEASES, key_copy, sizeof(key_copy), lease_raw,
                         sizeof(lease_raw));
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            return rc;
        }

        rc = txn_del_dbi(txn, SAP_WIT_DBI_INBOX, key_copy, sizeof(key_copy));
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            return rc;
        }

        rc = txn_del_dbi(txn, SAP_WIT_DBI_LEASES, key_copy, sizeof(key_copy));
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            return rc;
        }

        rc = txn_commit(txn);
        if (rc != SAP_OK)
        {
            return rc;
        }
        processed++;
    }

    *processed_out = processed;
    return SAP_OK;
}

static int run_candidate_round(uint32_t count, double *seconds_out)
{
    DB *db = NULL;
    uint32_t processed = 0u;
    double t0;
    double t1;
    int rc;

    if (!seconds_out)
    {
        return SAP_ERROR;
    }
    *seconds_out = 0.0;

    db = open_bench_db();
    if (!db)
    {
        return SAP_ERROR;
    }
    rc = populate_inbox(db, BENCH_WORKER_ID, count);
    if (rc != SAP_OK)
    {
        db_close(db);
        return rc;
    }

    t0 = now_seconds();
    rc = drain_fused_storage_candidate(db, BENCH_WORKER_ID, &processed);
    t1 = now_seconds();
    db_close(db);
    if (rc != SAP_OK)
    {
        return rc;
    }
    if (processed != count)
    {
        return SAP_ERROR;
    }
    *seconds_out = t1 - t0;
    return SAP_OK;
}

int main(int argc, char **argv)
{
    uint32_t count = 5000u;
    uint32_t rounds = 5u;
    uint32_t batch = 64u;
    double baseline_total = 0.0;
    double candidate_total = 0.0;
    uint32_t r;

    for (r = 1u; r < (uint32_t)argc; r++)
    {
        if (strcmp(argv[r], "--count") == 0 && (r + 1u) < (uint32_t)argc)
        {
            if (!parse_u32(argv[++r], &count) || count == 0u)
            {
                fprintf(stderr, "invalid --count\n");
                return 2;
            }
        }
        else if (strcmp(argv[r], "--rounds") == 0 && (r + 1u) < (uint32_t)argc)
        {
            if (!parse_u32(argv[++r], &rounds) || rounds == 0u)
            {
                fprintf(stderr, "invalid --rounds\n");
                return 2;
            }
        }
        else if (strcmp(argv[r], "--batch") == 0 && (r + 1u) < (uint32_t)argc)
        {
            if (!parse_u32(argv[++r], &batch) || batch == 0u)
            {
                fprintf(stderr, "invalid --batch\n");
                return 2;
            }
        }
        else
        {
            fprintf(stderr, "usage: %s [--count N] [--rounds R] [--batch B]\n", argv[0]);
            return 2;
        }
    }

    for (r = 0u; r < rounds; r++)
    {
        double baseline_sec = 0.0;
        double candidate_sec = 0.0;
        int rc;

        rc = run_baseline_round(count, batch, &baseline_sec);
        if (rc != SAP_OK)
        {
            fprintf(stderr, "baseline round failed (round=%u rc=%d)\n", r + 1u, rc);
            return 1;
        }
        rc = run_candidate_round(count, &candidate_sec);
        if (rc != SAP_OK)
        {
            fprintf(stderr, "candidate round failed (round=%u rc=%d)\n", r + 1u, rc);
            return 1;
        }

        baseline_total += baseline_sec;
        candidate_total += candidate_sec;
    }

    {
        double baseline_avg = baseline_total / (double)rounds;
        double candidate_avg = candidate_total / (double)rounds;
        double baseline_mps = (double)count / baseline_avg;
        double candidate_mps = (double)count / candidate_avg;
        double speedup = baseline_avg / candidate_avg;

        printf("Runner Phase-E coupling study benchmark\n");
        printf("count=%u rounds=%u batch=%u page_size=%u worker=%u\n", count, rounds, batch,
               (unsigned)SAPLING_PAGE_SIZE, (unsigned)BENCH_WORKER_ID);
        printf("baseline_poll_public_api:   %.6f s avg  (%.2f msg/s)\n", baseline_avg,
               baseline_mps);
        printf("candidate_fused_storage:    %.6f s avg  (%.2f msg/s)\n", candidate_avg,
               candidate_mps);
        printf("speedup(candidate/baseline): %.2fx\n", speedup);
        printf("note: candidate path is study-only and not used by runner_v0\n");
    }

    return 0;
}
