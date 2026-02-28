/*
 * thatch.h — public API for the Thatch packed data subsystem
 *
 * Thatch implements a cursor-passing, mostly-serialized memory model
 * optimized for bulk-processed immutable trees (e.g., JSONL parsing,
 * precompiled web templates). It operates directly on linear memory
 * to minimize allocations and enable zero-deserialization traversals.
 *
 * Concepts adapted from:
 * - "Compiling Tree Transforms to Operate on Packed Representations" (ECOOP 2017)
 * - "Efficient Tree-Traversals" (ICFP 2021) - Lookahead markers (skip pointers)
 * - "Garbage Collection for Mostly Serialized Heaps" (ISMM 2024) - Region allocation
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_THATCH_H
#define SAPLING_THATCH_H

#include <stddef.h>
#include <stdint.h>
#include "sapling/arena.h"
#include "sapling/txn.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/* Return codes                                                       */
/* ------------------------------------------------------------------ */
#define THATCH_OK      0 /* success                                   */
#define THATCH_OOM     1 /* arena page allocation failure             */
#define THATCH_BOUNDS  2 /* cursor attempted to read past region      */
#define THATCH_INVALID 3 /* invalid argument / bad subsystem state    */

/* ------------------------------------------------------------------ */
/* Types and Handles                                                  */
/* ------------------------------------------------------------------ */

/*
 * ThatchCursor represents a byte offset within a specific ThatchRegion.
 * In a Wasm environment, this natively compiles down to simple i32 math.
 */
typedef uint32_t ThatchCursor;

/*
 * Opaque handle representing a contiguous memory region (typically
 * backed by one or more SapMemArena pages).
 */
typedef struct ThatchRegion ThatchRegion;

/* ------------------------------------------------------------------ */
/* Subsystem Lifecycle                                                */
/* ------------------------------------------------------------------ */

/*
 * Initialize the Thatch subsystem on the environment.
 * Registers the SAP_SUBSYSTEM_THATCH callbacks with sap_env_register_subsystem.
 */
int sap_thatch_subsystem_init(SapEnv *env);

/* ------------------------------------------------------------------ */
/* Writing / Bump Allocation API                                      */
/* ------------------------------------------------------------------ */

/*
 * thatch_region_new — allocate a new, mutable region for bump-allocation.
 * Acquires pages from the transaction's arena.
 * Returns THATCH_OK or THATCH_OOM.
 */
int thatch_region_new(SapTxnCtx *txn, ThatchRegion **region_out);

/*
 * thatch_write_tag — bump-allocate 1 byte for a variant tag.
 */
int thatch_write_tag(ThatchRegion *region, uint8_t tag);

/*
 * thatch_write_data — bump-allocate and copy arbitrary data (e.g., primitives).
 */
int thatch_write_data(ThatchRegion *region, const void *data, uint32_t len);

/*
 * thatch_reserve_skip — reserve 4 bytes for a length-prefix (lookahead marker).
 * Saves the location to *skip_loc_out so it can be backpatched later.
 * Used when starting to parse a complex node (like a JSON array) whose
 * total serialized size is not yet known.
 */
int thatch_reserve_skip(ThatchRegion *region, ThatchCursor *skip_loc_out);

/*
 * thatch_commit_skip — backpatch a previously reserved skip pointer.
 * Calculates the difference between the current bump allocator head and
 * the skip_loc, writing the total byte-length into the reserved 4 bytes.
 */
int thatch_commit_skip(ThatchRegion *region, ThatchCursor skip_loc);

/*
 * thatch_seal — freeze the region. It can no longer be written to.
 * Prepares the region for zero-overhead concurrent reads.
 */
int thatch_seal(SapTxnCtx *txn, ThatchRegion *region);

/* ------------------------------------------------------------------ */
/* Reading / Cursor Traversal API                                     */
/* ------------------------------------------------------------------ */

/*
 * thatch_read_tag — read a 1-byte variant tag and advance the cursor.
 */
int thatch_read_tag(const ThatchRegion *region, ThatchCursor *cursor, uint8_t *tag_out);

/*
 * thatch_read_data — read arbitrary data and advance the cursor by len.
 */
int thatch_read_data(const ThatchRegion *region, ThatchCursor *cursor, uint32_t len, void *data_out);

/*
 * thatch_read_skip_len — read a 4-byte length-prefix and advance the cursor by 4.
 */
int thatch_read_skip_len(const ThatchRegion *region, ThatchCursor *cursor, uint32_t *skip_len_out);

/*
 * thatch_advance_cursor — manually advance the cursor by a calculated amount.
 * Often used immediately after thatch_read_skip_len to bypass a complex node
 * in O(1) time without reading its contents.
 */
int thatch_advance_cursor(const ThatchRegion *region, ThatchCursor *cursor, uint32_t skip_len);

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_THATCH_H */
