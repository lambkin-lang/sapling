/*
 * corruption_stats.h — observable counters for storage hardening guards
 *
 * Every time a hardening guard fires (e.g. free-list head reset, leaf
 * bounds reject), the corresponding counter increments.  The host or
 * test harness queries these to distinguish "no corruption occurred"
 * from "corruption was caught and handled."
 *
 * Thread safety: counters use relaxed atomics under SAPLING_THREADED.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_CORRUPTION_STATS_H
#define SAPLING_CORRUPTION_STATS_H

#include <stdint.h>

typedef struct
{
    uint64_t free_list_head_reset;      /* raw_alloc: head pgno invalid         */
    uint64_t free_list_next_dropped;    /* raw_alloc: next-pointer invalid      */
    uint64_t leaf_insert_bounds_reject; /* leaf_insert: bounds check failure    */
    uint64_t abort_loop_limit_hit;      /* txn_abort_free_untracked: loop cap   */
    uint64_t abort_bounds_break;        /* txn_abort_free_untracked: bounds brk */
} SapCorruptionStats;

struct SapEnv;

/* Query the current corruption counters for a database.
 * Thread-safe: counters are read atomically under SAPLING_THREADED.
 * Returns ERR_OK on success, ERR_INVALID if db or out is NULL. */
int sap_db_corruption_stats(struct SapEnv *db, SapCorruptionStats *out);

/* Reset all counters to zero.  Useful at the start of a stress round.
 * Returns ERR_OK on success, ERR_INVALID if db is NULL. */
int sap_db_corruption_stats_reset(struct SapEnv *db);

#endif /* SAPLING_CORRUPTION_STATS_H */
