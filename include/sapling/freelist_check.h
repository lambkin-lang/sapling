/*
 * freelist_check.h — proactive free-list structural integrity validation
 *
 * Walks the free-list and checks for cycles, out-of-bounds page numbers,
 * and NULL backing pointers.  Intended for use in test harnesses between
 * operations to assert invariants.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_FREELIST_CHECK_H
#define SAPLING_FREELIST_CHECK_H

#include <stdint.h>

struct SapEnv;

typedef struct
{
    uint32_t walk_length;   /* nodes visited in the free-list           */
    uint32_t out_of_bounds; /* nodes with pgno >= pages_cap             */
    uint32_t null_backing;  /* nodes where db->pages[pgno] == NULL      */
    uint32_t cycle_detected;/* 1 if tortoise-and-hare found a cycle     */
    uint32_t deferred_count;/* current size of deferred page array      */
} SapFreelistCheckResult;

/* Validate free-list structural integrity.
 * Must be called with no active write transaction (returns ERR_BUSY).
 * Returns ERR_OK even if issues are found — check result fields.
 * Returns ERR_INVALID on NULL args, ERR_BUSY if write txn active. */
int sap_db_freelist_check(struct SapEnv *db, SapFreelistCheckResult *result);

#endif /* SAPLING_FREELIST_CHECK_H */
