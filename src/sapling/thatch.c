/*
 * thatch.c — implementation of the Thatch packed data subsystem
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/thatch.h"
#include "sapling/arena.h"
#include "sapling/txn.h"
#include <string.h>

/* ------------------------------------------------------------------ */
/* Internal State Structures                                          */
/* ------------------------------------------------------------------ */

struct ThatchRegion {
    SapMemArena    *arena;
    void           *page_ptr;   /* The raw Wasm linear memory chunk */
    uint32_t        pgno;       /* The page number for O(1) freeing */
    uint32_t        capacity;   /* e.g., SAPLING_PAGE_SIZE */
    uint32_t        head;       /* The bump allocator cursor */
    int             sealed;     /* 1 if immutable/read-only, 0 if mutable */
    ThatchRegion   *next;       /* Linked list for transaction tracking */
};

/*
 * ThatchTxnState tracks all regions allocated during a transaction.
 * This is the crucial link for zero-overhead GC.
 */
typedef struct {
    ThatchRegion *active_regions;
} ThatchTxnState;

/* ------------------------------------------------------------------ */
/* Subsystem Callbacks                                                */
/* ------------------------------------------------------------------ */

static int thatch_on_begin(SapTxnCtx *txn, void *parent_state, void **state_out) {
    (void)parent_state;
    ThatchTxnState *state = sap_txn_scratch_alloc(txn, sizeof(ThatchTxnState));
    if (!state) return THATCH_OOM;

    state->active_regions = NULL;
    *state_out = state;
    return THATCH_OK;
}

static int thatch_on_commit(SapTxnCtx *txn, void *state_ptr) {
    (void)txn;
    ThatchTxnState *state = (ThatchTxnState *)state_ptr;

    /*
     * On commit, regions should theoretically be merged into the parent
     * transaction or sealed as immutable read-only buffers for threads.
     * For this draft, we simply seal them all.
     */
    ThatchRegion *curr = state->active_regions;
    while (curr) {
        curr->sealed = 1;
        curr = curr->next;
    }
    return THATCH_OK;
}

static void thatch_on_abort(SapTxnCtx *txn, void *state_ptr) {
    ThatchTxnState *state = (ThatchTxnState *)state_ptr;
    if (!state) return;
    SapMemArena *arena = sap_txn_arena(txn);

    /*
     * Instantaneous GC: Drop all regions allocated in this failed transaction.
     * No walking ASTs, no tracing pointers.
     */
    ThatchRegion *curr = state->active_regions;
    while (curr) {
        ThatchRegion *next = curr->next;
        sap_arena_free_page(arena, curr->pgno);
        /* Note: 'curr' struct itself is freed when txn scratch memory is wiped */
        curr = next;
    }
}

static void thatch_on_env_destroy(void *env_state) {
    (void)env_state;
}

static const SapTxnSubsystemCallbacks thatch_cbs = {
    .on_begin      = thatch_on_begin,
    .on_commit     = thatch_on_commit,
    .on_abort      = thatch_on_abort,
    .on_env_destroy = thatch_on_env_destroy
};

int sap_thatch_subsystem_init(SapEnv *env) {
    return sap_env_register_subsystem(env, SAP_SUBSYSTEM_THATCH, &thatch_cbs);
}

/* ------------------------------------------------------------------ */
/* Writing / Bump Allocation API                                      */
/* ------------------------------------------------------------------ */

int thatch_region_new(SapTxnCtx *txn, ThatchRegion **region_out) {
    if (!txn || !region_out) return THATCH_INVALID;

    ThatchTxnState *state = sap_txn_subsystem_state(txn, SAP_SUBSYSTEM_THATCH);
    if (!state) return THATCH_INVALID;

    SapMemArena *arena = sap_txn_arena(txn);

    ThatchRegion *region = sap_txn_scratch_alloc(txn, sizeof(ThatchRegion));
    if (!region) return THATCH_OOM;

    int rc = sap_arena_alloc_page(arena, &region->page_ptr, &region->pgno);
    if (rc != 0) return THATCH_OOM;

    region->arena    = arena;
    region->capacity = sap_env_get_page_size(sap_txn_env(txn));
    region->head     = 0;
    region->sealed   = 0;

    /* Track for transaction lifecycle */
    region->next            = state->active_regions;
    state->active_regions   = region;

    *region_out = region;
    return THATCH_OK;
}

int thatch_write_tag(ThatchRegion *region, uint8_t tag) {
    if (!region || region->sealed) return THATCH_INVALID;
    if (region->head + 1 > region->capacity) return THATCH_OOM;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    mem[region->head] = tag;
    region->head += 1;
    return THATCH_OK;
}

int thatch_write_data(ThatchRegion *region, const void *data, uint32_t len) {
    if (!region || region->sealed) return THATCH_INVALID;
    if (!data || len == 0) return len == 0 ? THATCH_OK : THATCH_INVALID;
    if (region->head + len > region->capacity) return THATCH_OOM;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    memcpy(mem + region->head, data, len);
    region->head += len;
    return THATCH_OK;
}

int thatch_reserve_skip(ThatchRegion *region, ThatchCursor *skip_loc_out) {
    if (!region || region->sealed || !skip_loc_out) return THATCH_INVALID;
    if (region->head + sizeof(uint32_t) > region->capacity) return THATCH_OOM;

    *skip_loc_out = region->head;
    /* Advance the head to leave a 4-byte gap for the lookahead marker */
    region->head += sizeof(uint32_t);
    return THATCH_OK;
}

int thatch_commit_skip(ThatchRegion *region, ThatchCursor skip_loc) {
    if (!region || region->sealed) return THATCH_INVALID;

    /* Calculate the total bytes written since the reservation */
    uint32_t skip_len = region->head - skip_loc - sizeof(uint32_t);

    uint8_t *mem = (uint8_t *)region->page_ptr;
    /* Backpatch the 4-byte integer into the reserved slot */
    memcpy(mem + skip_loc, &skip_len, sizeof(uint32_t));

    return THATCH_OK;
}

int thatch_seal(SapTxnCtx *txn, ThatchRegion *region) {
    (void)txn;
    if (!region) return THATCH_INVALID;
    region->sealed = 1;
    return THATCH_OK;
}

/* ------------------------------------------------------------------ */
/* Reading / Cursor Traversal API                                     */
/* ------------------------------------------------------------------ */

int thatch_read_tag(const ThatchRegion *region, ThatchCursor *cursor, uint8_t *tag_out) {
    if (!region || !cursor || !tag_out) return THATCH_INVALID;
    if (*cursor + 1 > region->head) return THATCH_BOUNDS;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    *tag_out = mem[*cursor];
    *cursor += 1;
    return THATCH_OK;
}

int thatch_read_data(const ThatchRegion *region, ThatchCursor *cursor, uint32_t len, void *data_out) {
    if (!region || !cursor || !data_out) return THATCH_INVALID;
    if (*cursor + len > region->head) return THATCH_BOUNDS;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    memcpy(data_out, mem + *cursor, len);
    *cursor += len;
    return THATCH_OK;
}

int thatch_read_skip_len(const ThatchRegion *region, ThatchCursor *cursor, uint32_t *skip_len_out) {
    if (!region || !cursor || !skip_len_out) return THATCH_INVALID;
    if (*cursor + sizeof(uint32_t) > region->head) return THATCH_BOUNDS;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    memcpy(skip_len_out, mem + *cursor, sizeof(uint32_t));
    *cursor += sizeof(uint32_t);
    return THATCH_OK;
}

int thatch_advance_cursor(const ThatchRegion *region, ThatchCursor *cursor, uint32_t skip_len) {
    if (!region || !cursor) return THATCH_INVALID;
    if (*cursor + skip_len > region->head) return THATCH_BOUNDS;

    /* The core of the O(1) jq-style bypass mechanism */
    *cursor += skip_len;
    return THATCH_OK;
}
