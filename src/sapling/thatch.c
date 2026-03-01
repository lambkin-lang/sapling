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
    uint32_t        nodeno;     /* Arena node ID for this struct itself */
    uint32_t        capacity;   /* e.g., SAPLING_PAGE_SIZE */
    uint32_t        head;       /* The bump allocator cursor */
    int             sealed;     /* 1 if immutable/read-only, 0 if mutable */
    ThatchRegion   *next;       /* Linked list for transaction tracking */
};

/*
 * ThatchTxnState tracks all regions allocated during a transaction.
 * This is the crucial link for zero-overhead GC.
 *
 * For nested transactions, parent_state links to the parent txn's
 * ThatchTxnState so that on commit, child regions are merged into the
 * parent's tracking list (making parent-abort clean them up correctly).
 */
typedef struct ThatchTxnState {
    ThatchRegion *active_regions;
    struct ThatchTxnState *parent_state;
} ThatchTxnState;

/* ------------------------------------------------------------------ */
/* Subsystem Callbacks                                                */
/* ------------------------------------------------------------------ */

static int thatch_on_begin(SapTxnCtx *txn, void *parent_state, void **state_out) {
    ThatchTxnState *state = sap_txn_scratch_alloc(txn, sizeof(ThatchTxnState));
    if (!state) return ERR_OOM;

    state->active_regions = NULL;
    state->parent_state = (ThatchTxnState *)parent_state;
    *state_out = state;
    return ERR_OK;
}

static int thatch_on_commit(SapTxnCtx *txn, void *state_ptr) {
    (void)txn;
    ThatchTxnState *state = (ThatchTxnState *)state_ptr;

    /* Seal all regions created in this transaction */
    ThatchRegion *curr = state->active_regions;
    ThatchRegion *tail = NULL;
    while (curr) {
        curr->sealed = 1;
        tail = curr;
        curr = curr->next;
    }

    if (state->parent_state && state->active_regions) {
        /*
         * Nested transaction: transfer ownership to parent so that a
         * subsequent parent abort will free these regions correctly.
         * Prepend child's list to parent's list via the child's tail.
         */
        tail->next = state->parent_state->active_regions;
        state->parent_state->active_regions = state->active_regions;
    }

    state->active_regions = NULL;
    return ERR_OK;
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
        sap_arena_free_node(arena, curr->nodeno, sizeof(ThatchRegion));
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
    if (!txn || !region_out) return ERR_INVALID;

    ThatchTxnState *state = sap_txn_subsystem_state(txn, SAP_SUBSYSTEM_THATCH);
    if (!state) return ERR_INVALID;

    SapMemArena *arena = sap_txn_arena(txn);

    /*
     * Allocate the ThatchRegion struct from the arena (not txn scratch)
     * so it survives transaction commit and remains valid for readers.
     */
    ThatchRegion *region = NULL;
    uint32_t region_nodeno = 0;
    int rc = sap_arena_alloc_node(arena, sizeof(ThatchRegion),
                                  (void **)&region, &region_nodeno);
    if (rc != 0) return ERR_OOM;

    rc = sap_arena_alloc_page(arena, &region->page_ptr, &region->pgno);
    if (rc != 0) {
        sap_arena_free_node(arena, region_nodeno, sizeof(ThatchRegion));
        return ERR_OOM;
    }

    region->arena    = arena;
    region->nodeno   = region_nodeno;
    region->capacity = sap_env_get_page_size(sap_txn_env(txn));
    region->head     = 0;
    region->sealed   = 0;

    /* Track for transaction lifecycle */
    region->next            = state->active_regions;
    state->active_regions   = region;

    *region_out = region;
    return ERR_OK;
}

int thatch_write_tag(ThatchRegion *region, uint8_t tag) {
    if (!region || region->sealed) return ERR_INVALID;
    if (region->head + 1 > region->capacity) return ERR_OOM;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    mem[region->head] = tag;
    region->head += 1;
    return ERR_OK;
}

int thatch_write_data(ThatchRegion *region, const void *data, uint32_t len) {
    if (!region || region->sealed) return ERR_INVALID;
    if (!data || len == 0) return len == 0 ? ERR_OK : ERR_INVALID;
    if (region->head + len > region->capacity) return ERR_OOM;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    memcpy(mem + region->head, data, len);
    region->head += len;
    return ERR_OK;
}

int thatch_reserve_skip(ThatchRegion *region, ThatchCursor *skip_loc_out) {
    if (!region || region->sealed || !skip_loc_out) return ERR_INVALID;
    if (region->head + sizeof(uint32_t) > region->capacity) return ERR_OOM;

    *skip_loc_out = region->head;
    /* Advance the head to leave a 4-byte gap for the lookahead marker */
    region->head += sizeof(uint32_t);
    return ERR_OK;
}

int thatch_commit_skip(ThatchRegion *region, ThatchCursor skip_loc) {
    if (!region || region->sealed) return ERR_INVALID;

    /* Validate that skip_loc is within bounds and the reserved 4-byte
     * slot fits within the data written so far. */
    if (skip_loc > region->head ||
        region->head - skip_loc < sizeof(uint32_t))
        return ERR_RANGE;

    /* Calculate the total bytes written since the reservation */
    uint32_t skip_len = region->head - skip_loc - sizeof(uint32_t);

    uint8_t *mem = (uint8_t *)region->page_ptr;
    /* Backpatch the 4-byte integer into the reserved slot */
    memcpy(mem + skip_loc, &skip_len, sizeof(uint32_t));

    return ERR_OK;
}

int thatch_seal(SapTxnCtx *txn, ThatchRegion *region) {
    (void)txn;
    if (!region) return ERR_INVALID;
    region->sealed = 1;
    return ERR_OK;
}

int thatch_region_release(SapTxnCtx *txn, ThatchRegion *region) {
    if (!txn || !region) return ERR_INVALID;

    ThatchTxnState *state = sap_txn_subsystem_state(txn, SAP_SUBSYSTEM_THATCH);
    if (!state) return ERR_INVALID;

    /* Unlink from the active region list — only free if actually found */
    int found = 0;
    ThatchRegion **pp = &state->active_regions;
    while (*pp) {
        if (*pp == region) {
            *pp = region->next;
            found = 1;
            break;
        }
        pp = &(*pp)->next;
    }

    if (!found) return ERR_INVALID;

    SapMemArena *arena = region->arena;
    sap_arena_free_page(arena, region->pgno);
    sap_arena_free_node(arena, region->nodeno, sizeof(ThatchRegion));
    return ERR_OK;
}

/* ------------------------------------------------------------------ */
/* Reading / Cursor Traversal API                                     */
/* ------------------------------------------------------------------ */

int thatch_read_tag(const ThatchRegion *region, ThatchCursor *cursor, uint8_t *tag_out) {
    if (!region || !cursor || !tag_out) return ERR_INVALID;
    if (*cursor + 1 > region->head) return ERR_RANGE;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    *tag_out = mem[*cursor];
    *cursor += 1;
    return ERR_OK;
}

int thatch_read_data(const ThatchRegion *region, ThatchCursor *cursor, uint32_t len, void *data_out) {
    if (!region || !cursor || !data_out) return ERR_INVALID;
    if (*cursor + len > region->head) return ERR_RANGE;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    memcpy(data_out, mem + *cursor, len);
    *cursor += len;
    return ERR_OK;
}

int thatch_read_skip_len(const ThatchRegion *region, ThatchCursor *cursor, uint32_t *skip_len_out) {
    if (!region || !cursor || !skip_len_out) return ERR_INVALID;
    if (*cursor + sizeof(uint32_t) > region->head) return ERR_RANGE;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    memcpy(skip_len_out, mem + *cursor, sizeof(uint32_t));
    *cursor += sizeof(uint32_t);
    return ERR_OK;
}

int thatch_advance_cursor(const ThatchRegion *region, ThatchCursor *cursor, uint32_t skip_len) {
    if (!region || !cursor) return ERR_INVALID;
    if (*cursor + skip_len > region->head) return ERR_RANGE;

    /* The core of the O(1) jq-style bypass mechanism */
    *cursor += skip_len;
    return ERR_OK;
}

int thatch_read_ptr(const ThatchRegion *region, ThatchCursor *cursor,
                    uint32_t len, const void **ptr_out) {
    if (!region || !cursor || !ptr_out) return ERR_INVALID;
    if (*cursor + len > region->head) return ERR_RANGE;

    uint8_t *mem = (uint8_t *)region->page_ptr;
    *ptr_out = mem + *cursor;
    *cursor += len;
    return ERR_OK;
}

uint32_t thatch_region_used(const ThatchRegion *region) {
    return region ? region->head : 0;
}
