/*
 * runner_scheduler_test.c - tests for phase-C scheduler helper scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/runner_v0.h"
#include "runner/scheduler_v0.h"

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

static PageAllocator g_alloc = {test_alloc, test_free, NULL};

static DB *new_db(void)
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
    return db;
}

static int test_next_due_empty_and_present(void)
{
    DB *db = new_db();
    int64_t due = 0;

    CHECK(db != NULL);
    CHECK(sap_runner_scheduler_v0_next_due(db, &due) == SAP_NOTFOUND);
    CHECK(sap_runner_timer_v0_append(db, 200, 1u, (const uint8_t *)"a", 1u) == SAP_OK);
    CHECK(sap_runner_timer_v0_append(db, 100, 1u, (const uint8_t *)"b", 1u) == SAP_OK);
    CHECK(sap_runner_scheduler_v0_next_due(db, &due) == SAP_OK);
    CHECK(due == 100);

    db_close(db);
    return 0;
}

static int test_compute_sleep_ms(void)
{
    uint32_t sleep_ms = 0u;

    CHECK(sap_runner_scheduler_v0_compute_sleep_ms(100, 150, 1000u, &sleep_ms) == SAP_OK);
    CHECK(sleep_ms == 50u);
    CHECK(sap_runner_scheduler_v0_compute_sleep_ms(100, 90, 1000u, &sleep_ms) == SAP_OK);
    CHECK(sleep_ms == 0u);
    CHECK(sap_runner_scheduler_v0_compute_sleep_ms(100, 5000, 200u, &sleep_ms) == SAP_OK);
    CHECK(sleep_ms == 200u);
    return 0;
}

int main(void)
{
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
    return 0;
}
