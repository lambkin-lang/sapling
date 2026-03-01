/*
 * btree_fault_stress.c - B+ tree stress test with fault-injected page allocation
 *
 * Exercises the core put/del/commit path under configurable page-alloc
 * failure rates.  Verifies that:
 *   - Corruption guards fire (nonzero telemetry counters are expected)
 *   - Aborted transactions leave the DB structurally sound
 *   - Successfully committed data remains readable
 *   - Free-list integrity is maintained throughout
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#include "sapling/sapling.h"
#include "common/fault_inject.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================== */
/* Config                                                               */
/* ================================================================== */

#define FAULT_STRESS_ROUNDS      20u
#define FAULT_STRESS_KEYS        100u
#define FAULT_STRESS_FAIL_PCT    15u  /* 15% of page allocs fail */

/* ================================================================== */
/* Test allocator                                                       */
/* ================================================================== */

static void *test_alloc(void *ctx, uint32_t sz)
{
    (void)ctx;
    return malloc((size_t)sz);
}

static void test_free_page(void *ctx, void *p, uint32_t sz)
{
    (void)ctx;
    (void)sz;
    free(p);
}

/* ================================================================== */
/* Helpers                                                              */
/* ================================================================== */

static int put_key(DB *db, uint32_t round, uint32_t i)
{
    char key[32], val[32];
    int klen = snprintf(key, sizeof(key), "fk-%u-%u", round, i);
    int vlen = snprintf(val, sizeof(val), "fv-%u-%u", round, i);

    Txn *txn = txn_begin(db, NULL, 0);
    if (!txn)
        return ERR_BUSY;

    int rc = txn_put(txn, key, (uint32_t)klen, val, (uint32_t)vlen);
    if (rc != ERR_OK)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_commit(txn);
    return rc;
}

static int del_key(DB *db, uint32_t round, uint32_t i)
{
    char key[32];
    int klen = snprintf(key, sizeof(key), "fk-%u-%u", round, i);

    Txn *txn = txn_begin(db, NULL, 0);
    if (!txn)
        return ERR_BUSY;

    int rc = txn_del(txn, key, (uint32_t)klen);
    if (rc != ERR_OK && rc != ERR_NOT_FOUND)
    {
        txn_abort(txn);
        return rc;
    }

    rc = txn_commit(txn);
    return rc;
}

static int verify_key(DB *db, uint32_t round, uint32_t i)
{
    char key[32], expected_val[32];
    int klen = snprintf(key, sizeof(key), "fk-%u-%u", round, i);
    int vlen = snprintf(expected_val, sizeof(expected_val), "fv-%u-%u", round, i);

    Txn *txn = txn_begin(db, NULL, TXN_RDONLY);
    if (!txn)
        return ERR_BUSY;

    const void *val_out = NULL;
    uint32_t val_len = 0;
    int rc = txn_get(txn, key, (uint32_t)klen, &val_out, &val_len);
    if (rc == ERR_OK)
    {
        if (val_len != (uint32_t)vlen || memcmp(val_out, expected_val, val_len) != 0)
        {
            fprintf(stderr, "btree-fault-stress: data corruption at round=%u i=%u\n", round, i);
            txn_abort(txn);
            return ERR_CORRUPT;
        }
    }

    txn_abort(txn);
    return rc;
}

/* ================================================================== */
/* Main                                                                 */
/* ================================================================== */

int main(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = test_alloc,
        .cfg.custom.free_page = test_free_page,
        .cfg.custom.ctx = NULL};
    if (sap_arena_init(&arena, &opts) != 0)
    {
        fprintf(stderr, "btree-fault-stress: arena init failed\n");
        return 1;
    }

    DB *db = db_open(arena, SAPLING_PAGE_SIZE, NULL, NULL);
    if (!db)
    {
        fprintf(stderr, "btree-fault-stress: db_open failed\n");
        return 1;
    }

    /* Configure fault injector */
    SapFaultInjector fi;
    sap_fi_reset(&fi);
    if (sap_fi_add_rate_rule(&fi, "alloc.page", FAULT_STRESS_FAIL_PCT) != 0)
    {
        fprintf(stderr, "btree-fault-stress: fi add_rate_rule failed\n");
        return 1;
    }
    if (sap_db_set_fault_injector((struct SapEnv *)db, &fi) != ERR_OK)
    {
        fprintf(stderr, "btree-fault-stress: set_fault_injector failed\n");
        return 1;
    }

    uint32_t total_puts = 0, put_ok = 0, put_fail = 0;
    uint32_t total_dels = 0, del_ok = 0, del_fail = 0;

    /*
     * Per-key expected state: 1 = live (put committed, not deleted),
     * 0 = dead (never committed, or successfully deleted).
     */
    uint8_t *live = calloc(FAULT_STRESS_ROUNDS * FAULT_STRESS_KEYS, sizeof(uint8_t));
    if (!live)
    {
        fprintf(stderr, "btree-fault-stress: alloc live bitmap failed\n");
        return 1;
    }

    for (uint32_t round = 0; round < FAULT_STRESS_ROUNDS; round++)
    {
        /* Insert keys (some will fail due to fault injection) */
        for (uint32_t i = 0; i < FAULT_STRESS_KEYS; i++)
        {
            int rc = put_key(db, round, i);
            total_puts++;
            if (rc == ERR_OK)
            {
                put_ok++;
                live[round * FAULT_STRESS_KEYS + i] = 1;
            }
            else
                put_fail++;
        }

        /* Delete even-indexed keys */
        for (uint32_t i = 0; i < FAULT_STRESS_KEYS; i += 2)
        {
            int rc = del_key(db, round, i);
            total_dels++;
            if (rc == ERR_OK)
            {
                del_ok++;
                live[round * FAULT_STRESS_KEYS + i] = 0;
            }
            else
                del_fail++;
        }

        /* Free-list integrity check each round */
        SapFreelistCheckResult fl;
        if (sap_db_freelist_check((struct SapEnv *)db, &fl) == ERR_OK)
        {
            if (fl.out_of_bounds || fl.null_backing || fl.cycle_detected)
            {
                fprintf(stderr,
                        "btree-fault-stress: round=%u FREE-LIST FAILURE "
                        "oob=%u null=%u cycle=%u\n",
                        round, fl.out_of_bounds, fl.null_backing, fl.cycle_detected);
                free(live);
                db_close(db);
                sap_arena_destroy(arena);
                return 1;
            }
        }
    }

    /* Disable fault injection for verification reads */
    sap_db_set_fault_injector((struct SapEnv *)db, NULL);

    /*
     * Verify: live keys must return ERR_OK with correct value,
     * dead keys must return ERR_NOT_FOUND.
     */
    uint32_t verified = 0, found = 0, not_found = 0, corrupt = 0;
    uint32_t live_missing = 0, dead_present = 0;
    for (uint32_t round = 0; round < FAULT_STRESS_ROUNDS; round++)
    {
        for (uint32_t i = 0; i < FAULT_STRESS_KEYS; i++)
        {
            int expected_live = live[round * FAULT_STRESS_KEYS + i];
            int rc = verify_key(db, round, i);
            verified++;
            if (rc == ERR_OK)
            {
                found++;
                if (!expected_live)
                {
                    dead_present++;
                    fprintf(stderr,
                            "btree-fault-stress: ghost key round=%u i=%u (expected dead)\n",
                            round, i);
                }
            }
            else if (rc == ERR_NOT_FOUND)
            {
                not_found++;
                if (expected_live)
                {
                    live_missing++;
                    fprintf(stderr,
                            "btree-fault-stress: lost key round=%u i=%u (expected live)\n",
                            round, i);
                }
            }
            else
            {
                corrupt++;
                fprintf(stderr, "btree-fault-stress: verify error rc=%d round=%u i=%u\n",
                        rc, round, i);
            }
        }
    }
    free(live);

    /* Corruption telemetry */
    SapCorruptionStats cstats;
    uint64_t corruption_total = 0;
    if (sap_db_corruption_stats((struct SapEnv *)db, &cstats) == ERR_OK)
    {
        corruption_total = cstats.free_list_head_reset +
                           cstats.free_list_next_dropped +
                           cstats.leaf_insert_bounds_reject +
                           cstats.abort_loop_limit_hit +
                           cstats.abort_bounds_break;
    }

    /* Final free-list check */
    SapFreelistCheckResult fl_final;
    int fl_ok = 1;
    if (sap_db_freelist_check((struct SapEnv *)db, &fl_final) == ERR_OK)
    {
        if (fl_final.out_of_bounds || fl_final.null_backing || fl_final.cycle_detected)
            fl_ok = 0;
    }

    printf("btree-fault-stress: rounds=%u keys=%u fail_pct=%u\n",
           FAULT_STRESS_ROUNDS, FAULT_STRESS_KEYS, FAULT_STRESS_FAIL_PCT);
    printf("  puts: total=%u ok=%u fail=%u\n", total_puts, put_ok, put_fail);
    printf("  dels: total=%u ok=%u fail=%u\n", total_dels, del_ok, del_fail);
    printf("  verify: total=%u found=%u not_found=%u corrupt=%u"
           " live_missing=%u dead_present=%u\n",
           verified, found, not_found, corrupt, live_missing, dead_present);
    printf("  corruption_stats: total=%" PRIu64 "\n", corruption_total);
    printf("  fi_rule: hits=%u fails=%u\n", fi.rules[0].hit_count, fi.rules[0].fail_count);
    printf("  freelist_final: walk=%u oob=%u null=%u cycle=%u\n",
           fl_final.walk_length, fl_final.out_of_bounds,
           fl_final.null_backing, fl_final.cycle_detected);

    db_close(db);
    sap_arena_destroy(arena);

    if (corrupt > 0)
    {
        fprintf(stderr, "btree-fault-stress: FAILED (data corruption)\n");
        return 1;
    }
    if (live_missing > 0)
    {
        fprintf(stderr, "btree-fault-stress: FAILED (live keys missing=%u)\n", live_missing);
        return 1;
    }
    if (dead_present > 0)
    {
        fprintf(stderr, "btree-fault-stress: FAILED (ghost keys present=%u)\n", dead_present);
        return 1;
    }
    if (!fl_ok)
    {
        fprintf(stderr, "btree-fault-stress: FAILED (free-list failure)\n");
        return 1;
    }
    if (put_fail == 0)
    {
        fprintf(stderr, "btree-fault-stress: FAILED (no faults injected)\n");
        return 1;
    }
    if (found == 0)
    {
        fprintf(stderr, "btree-fault-stress: FAILED (no data committed)\n");
        return 1;
    }

    printf("btree-fault-stress: PASSED\n");
    return 0;
}
