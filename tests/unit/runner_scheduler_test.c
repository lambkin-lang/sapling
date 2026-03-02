/*
 * runner_scheduler_test.c - tests for phase-C scheduler helper scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/runner_v0.h"
#include "runner/scheduler_v0.h"
#include "runner/wire_v0.h"

#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

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
    if (sap_runner_v0_bootstrap_dbis(db) != ERR_OK)
    {
        db_close(db);
        return NULL;
    }
    return db;
}

static int encode_message(uint32_t to_worker, uint8_t payload_tag, uint8_t *buf, uint32_t buf_cap,
                          uint32_t *len_out)
{
    uint8_t msg_id[] = {'s', 'c', payload_tag};
    uint8_t payload[] = {'p', payload_tag};
    SapRunnerMessageV0 msg = {0};

    if (!buf || !len_out)
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
    return sap_runner_message_v0_encode(&msg, buf, buf_cap, len_out);
}

static int test_next_due_empty_and_present(void)
{
    DB *db = new_db();
    int64_t due = 0;
    uint8_t frame_a[128];
    uint8_t frame_b[128];
    uint32_t frame_a_len = 0u;
    uint32_t frame_b_len = 0u;

    CHECK(db != NULL);
    CHECK(encode_message(1u, (uint8_t)'a', frame_a, sizeof(frame_a), &frame_a_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(encode_message(1u, (uint8_t)'b', frame_b, sizeof(frame_b), &frame_b_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_scheduler_v0_next_due(db, &due) == ERR_NOT_FOUND);
    CHECK(sap_runner_timer_v0_append(db, 200, 1u, frame_a, frame_a_len) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 100, 1u, frame_b, frame_b_len) == ERR_OK);
    CHECK(sap_runner_scheduler_v0_next_due(db, &due) == ERR_OK);
    CHECK(due == 100);

    db_close(db);
    return 0;
}

static int test_compute_sleep_ms(void)
{
    uint32_t sleep_ms = 0u;

    CHECK(sap_runner_scheduler_v0_compute_sleep_ms(100, 150, 1000u, &sleep_ms) == ERR_OK);
    CHECK(sleep_ms == 50u);
    CHECK(sap_runner_scheduler_v0_compute_sleep_ms(100, 90, 1000u, &sleep_ms) == ERR_OK);
    CHECK(sleep_ms == 0u);
    CHECK(sap_runner_scheduler_v0_compute_sleep_ms(100, 5000, 200u, &sleep_ms) == ERR_OK);
    CHECK(sleep_ms == 200u);
    return 0;
}

static int due_counter_handler(int64_t due_ts, uint64_t seq, const uint8_t *payload,
                               uint32_t payload_len, void *ctx)
{
    uint32_t *count = (uint32_t *)ctx;
    (void)due_ts;
    (void)seq;
    (void)payload;
    (void)payload_len;
    (*count)++;
    return ERR_OK;
}

static int test_next_due_progress_after_drain(void)
{
    DB *db = new_db();
    int64_t due = 0;
    uint32_t handled = 0u;
    uint32_t processed = 0u;
    uint8_t frame_a[128];
    uint8_t frame_b[128];
    uint8_t frame_c[128];
    uint32_t frame_a_len = 0u;
    uint32_t frame_b_len = 0u;
    uint32_t frame_c_len = 0u;

    CHECK(db != NULL);
    CHECK(encode_message(1u, (uint8_t)'a', frame_a, sizeof(frame_a), &frame_a_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(encode_message(1u, (uint8_t)'b', frame_b, sizeof(frame_b), &frame_b_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(encode_message(1u, (uint8_t)'c', frame_c, sizeof(frame_c), &frame_c_len) ==
          SAP_RUNNER_WIRE_OK);
    CHECK(sap_runner_timer_v0_append(db, 200, 1u, frame_a, frame_a_len) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 100, 2u, frame_b, frame_b_len) == ERR_OK);
    CHECK(sap_runner_timer_v0_append(db, 150, 3u, frame_c, frame_c_len) == ERR_OK);

    CHECK(sap_runner_scheduler_v0_next_due(db, &due) == ERR_OK);
    CHECK(due == 100);

    CHECK(sap_runner_timer_v0_drain_due(db, 120, 1u, due_counter_handler, &handled, &processed) ==
          ERR_OK);
    CHECK(handled == 1u);
    CHECK(processed == 1u);

    CHECK(sap_runner_scheduler_v0_next_due(db, &due) == ERR_OK);
    CHECK(due == 150);

    CHECK(sap_runner_timer_v0_drain_due(db, 1000, 10u, due_counter_handler, &handled, &processed) ==
          ERR_OK);
    CHECK(processed == 2u);
    CHECK(handled == 3u);
    CHECK(sap_runner_scheduler_v0_next_due(db, &due) == ERR_NOT_FOUND);

    db_close(db);
    return 0;
}

static int test_scheduler_invalid_args_and_caps(void)
{
    DB *db = new_db();
    int64_t due = 0;
    uint32_t sleep_ms = 0u;

    CHECK(db != NULL);
    CHECK(sap_runner_scheduler_v0_next_due(NULL, &due) == ERR_INVALID);
    CHECK(sap_runner_scheduler_v0_next_due(db, NULL) == ERR_INVALID);
    CHECK(sap_runner_scheduler_v0_compute_sleep_ms(0, 1, 1u, NULL) == ERR_INVALID);

    CHECK(sap_runner_scheduler_v0_compute_sleep_ms(100, 5000, 0u, &sleep_ms) == ERR_OK);
    CHECK(sleep_ms == 4900u);
    CHECK(sap_runner_scheduler_v0_compute_sleep_ms(0, 5000000000LL, 0u, &sleep_ms) == ERR_OK);
    CHECK(sleep_ms == UINT32_MAX);

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

    rc = test_next_due_empty_and_present();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_compute_sleep_ms();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_next_due_progress_after_drain();
    if (rc != 0)
    {
        return rc;
    }
    rc = test_scheduler_invalid_args_and_caps();
    if (rc != 0)
    {
        return rc;
    }
    return 0;
}
