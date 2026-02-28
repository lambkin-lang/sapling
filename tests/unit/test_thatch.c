/*
 * test_thatch.c — unit tests for the Thatch packed data subsystem
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sapling/arena.h"
#include "sapling/txn.h"
#include "sapling/thatch.h"

static int passed = 0;
static int failed = 0;

#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            failed++; \
            return; \
        } \
        passed++; \
    } while (0)

/* Helper: create a test arena + env with Thatch registered */
static void make_env(SapMemArena **arena_out, SapEnv **env_out) {
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_MALLOC,
        .cfg.mmap.max_size = 1024 * 1024
    };
    CHECK(sap_arena_init(arena_out, &opts) == 0);
    *env_out = sap_env_create(*arena_out, SAPLING_PAGE_SIZE);
    CHECK(*env_out != NULL);
    CHECK(sap_thatch_subsystem_init(*env_out) == SAP_OK);
}

/* ------------------------------------------------------------------ */
/* Test: subsystem init + region alloc                                */
/* ------------------------------------------------------------------ */
static void test_subsystem_init_and_region_alloc(void) {
    printf("--- subsystem init and region alloc ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);
    CHECK(region != NULL);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: write_tag / read_tag round-trip                              */
/* ------------------------------------------------------------------ */
static void test_write_read_tag(void) {
    printf("--- write_tag / read_tag round-trip ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);

    CHECK(thatch_write_tag(region, 0x42) == THATCH_OK);
    CHECK(thatch_write_tag(region, 0xFF) == THATCH_OK);

    ThatchCursor cursor = 0;
    uint8_t tag = 0;
    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_OK);
    CHECK(tag == 0x42);
    CHECK(cursor == 1);

    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_OK);
    CHECK(tag == 0xFF);
    CHECK(cursor == 2);

    /* Reading past end should return BOUNDS */
    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_BOUNDS);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: write_data / read_data round-trip                            */
/* ------------------------------------------------------------------ */
static void test_write_read_data(void) {
    printf("--- write_data / read_data round-trip ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);

    const char *msg = "hello thatch";
    uint32_t len = (uint32_t)strlen(msg);
    CHECK(thatch_write_data(region, msg, len) == THATCH_OK);

    char buf[32] = {0};
    ThatchCursor cursor = 0;
    CHECK(thatch_read_data(region, &cursor, len, buf) == THATCH_OK);
    CHECK(memcmp(buf, msg, len) == 0);
    CHECK(cursor == len);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: skip pointer backpatching (the jq bypass mechanism)          */
/* ------------------------------------------------------------------ */
static void test_skip_pointer_backpatch(void) {
    printf("--- skip pointer backpatching ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);

    /* Simulate serializing: { "key": 42 }
     * Layout: [tag:OBJ][skip:4bytes][tag:KEY][data:"key"][tag:NUM][data:42]
     */
    #define TAG_OBJ 1
    #define TAG_KEY 2
    #define TAG_NUM 3

    /* Write the object tag */
    CHECK(thatch_write_tag(region, TAG_OBJ) == THATCH_OK);

    /* Reserve the skip pointer */
    ThatchCursor skip_loc = 0;
    CHECK(thatch_reserve_skip(region, &skip_loc) == THATCH_OK);

    /* Write the contents of the object */
    CHECK(thatch_write_tag(region, TAG_KEY) == THATCH_OK);
    const char *key = "key";
    CHECK(thatch_write_data(region, key, 3) == THATCH_OK);
    CHECK(thatch_write_tag(region, TAG_NUM) == THATCH_OK);
    uint32_t val = 42;
    CHECK(thatch_write_data(region, &val, sizeof(val)) == THATCH_OK);

    /* Backpatch the skip pointer */
    CHECK(thatch_commit_skip(region, skip_loc) == THATCH_OK);

    /* --- Read it back --- */
    ThatchCursor cursor = 0;
    uint8_t tag = 0;

    /* Read the object tag */
    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_OK);
    CHECK(tag == TAG_OBJ);

    /* Read the skip length */
    uint32_t skip_len = 0;
    CHECK(thatch_read_skip_len(region, &cursor, &skip_len) == THATCH_OK);
    /* skip_len should cover: tag(1) + "key"(3) + tag(1) + uint32(4) = 9 */
    CHECK(skip_len == 9);

    /* Use O(1) bypass to skip past the entire object contents */
    ThatchCursor after_skip = cursor + skip_len;
    CHECK(thatch_advance_cursor(region, &cursor, skip_len) == THATCH_OK);
    CHECK(cursor == after_skip);

    /* Also verify we can read the contents sequentially */
    cursor = skip_loc + sizeof(uint32_t); /* reset to after skip slot */
    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_OK);
    CHECK(tag == TAG_KEY);
    char keybuf[4] = {0};
    CHECK(thatch_read_data(region, &cursor, 3, keybuf) == THATCH_OK);
    CHECK(memcmp(keybuf, "key", 3) == 0);
    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_OK);
    CHECK(tag == TAG_NUM);
    uint32_t readval = 0;
    CHECK(thatch_read_data(region, &cursor, sizeof(uint32_t), &readval) == THATCH_OK);
    CHECK(readval == 42);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: seal prevents writes                                         */
/* ------------------------------------------------------------------ */
static void test_seal_prevents_writes(void) {
    printf("--- seal prevents writes ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);

    CHECK(thatch_write_tag(region, 0x01) == THATCH_OK);
    CHECK(thatch_seal(txn, region) == THATCH_OK);

    /* All writes should now fail with THATCH_INVALID */
    CHECK(thatch_write_tag(region, 0x02) == THATCH_INVALID);
    CHECK(thatch_write_data(region, "x", 1) == THATCH_INVALID);

    ThatchCursor skip_loc;
    CHECK(thatch_reserve_skip(region, &skip_loc) == THATCH_INVALID);
    CHECK(thatch_commit_skip(region, 0) == THATCH_INVALID);

    /* But reads should still work */
    ThatchCursor cursor = 0;
    uint8_t tag = 0;
    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_OK);
    CHECK(tag == 0x01);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: commit seals all active regions                              */
/* ------------------------------------------------------------------ */
static void test_commit_seals_regions(void) {
    printf("--- commit seals all active regions ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *r1 = NULL, *r2 = NULL;
    CHECK(thatch_region_new(txn, &r1) == THATCH_OK);
    CHECK(thatch_region_new(txn, &r2) == THATCH_OK);

    /* Write something to both */
    CHECK(thatch_write_tag(r1, 0xAA) == THATCH_OK);
    CHECK(thatch_write_tag(r2, 0xBB) == THATCH_OK);

    /* Commit should seal both */
    CHECK(sap_txn_commit(txn) == SAP_OK);

    /* After commit, regions are sealed (writes should fail) */
    CHECK(thatch_write_tag(r1, 0x01) == THATCH_INVALID);
    CHECK(thatch_write_tag(r2, 0x01) == THATCH_INVALID);

    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: abort frees region pages (checks active_pages count)         */
/* ------------------------------------------------------------------ */
static void test_abort_frees_regions(void) {
    printf("--- abort frees region pages ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    /* Warm up the arena so active_pages baseline is non-zero
     * (avoids unsigned underflow in sap_arena_active_pages when chunk_count=0) */
    {
        void *warmup = NULL;
        uint32_t warmup_pgno = 0;
        CHECK(sap_arena_alloc_page(arena, &warmup, &warmup_pgno) == 0);
        CHECK(sap_arena_free_page(arena, warmup_pgno) == 0);
    }

    uint32_t baseline = sap_arena_active_pages(arena);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);

    /* We should have allocated pages (at least the region page + scratch page) */
    CHECK(sap_arena_active_pages(arena) > baseline);

    sap_txn_abort(txn);

    /* After abort, arena pages should be freed back */
    CHECK(sap_arena_active_pages(arena) == baseline);

    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: multiple regions in one transaction                          */
/* ------------------------------------------------------------------ */
static void test_multiple_regions(void) {
    printf("--- multiple regions in one transaction ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *regions[4];
    for (int i = 0; i < 4; i++) {
        CHECK(thatch_region_new(txn, &regions[i]) == THATCH_OK);
        uint8_t tag = (uint8_t)(0x10 + i);
        CHECK(thatch_write_tag(regions[i], tag) == THATCH_OK);
    }

    /* Verify each region has independent data */
    for (int i = 0; i < 4; i++) {
        ThatchCursor cursor = 0;
        uint8_t tag = 0;
        CHECK(thatch_read_tag(regions[i], &cursor, &tag) == THATCH_OK);
        CHECK(tag == (uint8_t)(0x10 + i));
    }

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: bounds checking for cursor advances                          */
/* ------------------------------------------------------------------ */
static void test_bounds_checking(void) {
    printf("--- bounds checking ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);

    /* Write 2 bytes total */
    CHECK(thatch_write_tag(region, 0xAA) == THATCH_OK);
    CHECK(thatch_write_tag(region, 0xBB) == THATCH_OK);

    /* Try to read 4 bytes (should fail) */
    ThatchCursor cursor = 0;
    char buf[4];
    CHECK(thatch_read_data(region, &cursor, 4, buf) == THATCH_BOUNDS);

    /* Try to advance past end */
    cursor = 0;
    CHECK(thatch_advance_cursor(region, &cursor, 10) == THATCH_BOUNDS);

    /* Try to read skip len (needs 4 bytes but only 2 available) */
    cursor = 0;
    uint32_t skip = 0;
    CHECK(thatch_read_skip_len(region, &cursor, &skip) == THATCH_BOUNDS);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: invalid argument handling                                    */
/* ------------------------------------------------------------------ */
static void test_invalid_args(void) {
    printf("--- invalid argument handling ---\n");

    /* NULL region */
    CHECK(thatch_write_tag(NULL, 0) == THATCH_INVALID);
    CHECK(thatch_write_data(NULL, "x", 1) == THATCH_INVALID);
    CHECK(thatch_read_tag(NULL, NULL, NULL) == THATCH_INVALID);

    ThatchCursor cursor = 0;
    CHECK(thatch_reserve_skip(NULL, &cursor) == THATCH_INVALID);
    CHECK(thatch_commit_skip(NULL, 0) == THATCH_INVALID);
    CHECK(thatch_seal(NULL, NULL) == THATCH_INVALID);

    /* NULL txn for region_new */
    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(NULL, &region) == THATCH_INVALID);
}

/* ------------------------------------------------------------------ */
/* Test: nested skip pointers (object within object)                  */
/* ------------------------------------------------------------------ */
static void test_nested_skip_pointers(void) {
    printf("--- nested skip pointers ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);

    /* Serialize: { inner: { val: 99 } }
     * Outer: [tag:OBJ][skip_outer][tag:OBJ][skip_inner][tag:NUM][data:99]
     */
    CHECK(thatch_write_tag(region, TAG_OBJ) == THATCH_OK);
    ThatchCursor skip_outer = 0;
    CHECK(thatch_reserve_skip(region, &skip_outer) == THATCH_OK);

    CHECK(thatch_write_tag(region, TAG_OBJ) == THATCH_OK);
    ThatchCursor skip_inner = 0;
    CHECK(thatch_reserve_skip(region, &skip_inner) == THATCH_OK);

    CHECK(thatch_write_tag(region, TAG_NUM) == THATCH_OK);
    uint32_t val = 99;
    CHECK(thatch_write_data(region, &val, sizeof(val)) == THATCH_OK);

    /* Backpatch inner first, then outer */
    CHECK(thatch_commit_skip(region, skip_inner) == THATCH_OK);
    CHECK(thatch_commit_skip(region, skip_outer) == THATCH_OK);

    /* Read back: skip the outer object entirely */
    ThatchCursor cursor = 0;
    uint8_t tag = 0;
    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_OK);
    CHECK(tag == TAG_OBJ);

    uint32_t outer_skip = 0;
    CHECK(thatch_read_skip_len(region, &cursor, &outer_skip) == THATCH_OK);
    /* Inner: tag(1) + skip(4) + tag(1) + uint32(4) = 10 bytes */
    CHECK(outer_skip == 10);

    /* Skip the entire outer contents */
    ThatchCursor end = cursor + outer_skip;
    CHECK(thatch_advance_cursor(region, &cursor, outer_skip) == THATCH_OK);
    CHECK(cursor == end);

    /* Or: read the inner skip pointer and bypass just the inner */
    cursor = skip_outer + sizeof(uint32_t); /* after outer skip slot */
    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_OK);
    CHECK(tag == TAG_OBJ);

    uint32_t inner_skip = 0;
    CHECK(thatch_read_skip_len(region, &cursor, &inner_skip) == THATCH_OK);
    /* tag(1) + uint32(4) = 5 bytes */
    CHECK(inner_skip == 5);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: [P0] region survives commit and is readable in next txn      */
/* ------------------------------------------------------------------ */
static void test_region_valid_after_commit(void) {
    printf("--- region valid after commit ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    /* Txn1: write data, commit */
    SapTxnCtx *txn1 = sap_txn_begin(env, NULL, 0);
    CHECK(txn1 != NULL);

    ThatchRegion *r1 = NULL;
    CHECK(thatch_region_new(txn1, &r1) == THATCH_OK);
    CHECK(thatch_write_tag(r1, 0xAB) == THATCH_OK);
    CHECK(thatch_write_data(r1, "hello", 5) == THATCH_OK);
    CHECK(sap_txn_commit(txn1) == SAP_OK);

    /* Txn2: start a new txn that allocates its own region.
     * If r1's metadata was on scratch memory, txn2's scratch
     * would alias the freed memory, corrupting r1. */
    SapTxnCtx *txn2 = sap_txn_begin(env, NULL, 0);
    CHECK(txn2 != NULL);

    ThatchRegion *r2 = NULL;
    CHECK(thatch_region_new(txn2, &r2) == THATCH_OK);
    CHECK(thatch_write_tag(r2, 0xCD) == THATCH_OK);

    /* r1 must still be readable and contain original data */
    ThatchCursor cursor = 0;
    uint8_t tag = 0;
    CHECK(thatch_read_tag(r1, &cursor, &tag) == THATCH_OK);
    CHECK(tag == 0xAB);  /* must NOT be 0xCD from txn2 */

    char buf[8] = {0};
    CHECK(thatch_read_data(r1, &cursor, 5, buf) == THATCH_OK);
    CHECK(memcmp(buf, "hello", 5) == 0);

    sap_txn_abort(txn2);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: [P1] commit_skip rejects invalid skip_loc                    */
/* ------------------------------------------------------------------ */
static void test_commit_skip_bounds_check(void) {
    printf("--- commit_skip bounds check ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);

    /* Write a single tag byte so head == 1 */
    CHECK(thatch_write_tag(region, 0x01) == THATCH_OK);

    /* skip_loc pointing past end — must fail */
    CHECK(thatch_commit_skip(region, 100) == THATCH_BOUNDS);

    /* skip_loc at head (no room for 4-byte slot) — must fail */
    CHECK(thatch_commit_skip(region, 1) == THATCH_BOUNDS);

    /* skip_loc at 0 but only 1 byte written (need 4) — must fail */
    CHECK(thatch_commit_skip(region, 0) == THATCH_BOUNDS);

    /* Now write enough data so a valid skip_loc works */
    ThatchCursor skip_loc;
    CHECK(thatch_reserve_skip(region, &skip_loc) == THATCH_OK);
    CHECK(thatch_write_tag(region, 0x42) == THATCH_OK);
    CHECK(thatch_commit_skip(region, skip_loc) == THATCH_OK);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: [P1] thatch_region_release frees pages immediately           */
/* ------------------------------------------------------------------ */
static void test_region_release(void) {
    printf("--- region release ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    /* Warm up arena */
    {
        void *warmup = NULL;
        uint32_t warmup_pgno = 0;
        CHECK(sap_arena_alloc_page(arena, &warmup, &warmup_pgno) == 0);
        CHECK(sap_arena_free_page(arena, warmup_pgno) == 0);
    }

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    uint32_t baseline = sap_arena_active_pages(arena);

    /* Allocate 5 regions, then release them all within the same txn */
    ThatchRegion *regions[5];
    for (int i = 0; i < 5; i++) {
        CHECK(thatch_region_new(txn, &regions[i]) == THATCH_OK);
    }
    CHECK(sap_arena_active_pages(arena) > baseline);

    for (int i = 0; i < 5; i++) {
        CHECK(thatch_region_release(txn, regions[i]) == THATCH_OK);
    }
    CHECK(sap_arena_active_pages(arena) == baseline);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: [P0] double-release returns THATCH_INVALID, no crash         */
/* ------------------------------------------------------------------ */
static void test_double_release(void) {
    printf("--- double release ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn, &region) == THATCH_OK);

    /* First release succeeds */
    CHECK(thatch_region_release(txn, region) == THATCH_OK);

    /* Second release must fail — region is no longer in the txn's list */
    CHECK(thatch_region_release(txn, region) == THATCH_INVALID);

    sap_txn_abort(txn);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: [P0] wrong-owner release returns THATCH_INVALID              */
/* ------------------------------------------------------------------ */
static void test_wrong_owner_release(void) {
    printf("--- wrong-owner release ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    SapTxnCtx *txn1 = sap_txn_begin(env, NULL, 0);
    CHECK(txn1 != NULL);
    SapTxnCtx *txn2 = sap_txn_begin(env, NULL, 0);
    CHECK(txn2 != NULL);

    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(txn1, &region) == THATCH_OK);

    /* Releasing from wrong txn must fail */
    CHECK(thatch_region_release(txn2, region) == THATCH_INVALID);

    /* Original owner can still release */
    CHECK(thatch_region_release(txn1, region) == THATCH_OK);

    sap_txn_abort(txn2);
    sap_txn_abort(txn1);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: [P1] nested txn: child commit + parent abort frees child     */
/* ------------------------------------------------------------------ */
static void test_nested_child_commit_parent_abort(void) {
    printf("--- nested: child commit + parent abort ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    /* Warm up arena */
    {
        void *warmup = NULL;
        uint32_t warmup_pgno = 0;
        CHECK(sap_arena_alloc_page(arena, &warmup, &warmup_pgno) == 0);
        CHECK(sap_arena_free_page(arena, warmup_pgno) == 0);
    }

    uint32_t baseline = sap_arena_active_pages(arena);

    /* Parent txn */
    SapTxnCtx *parent = sap_txn_begin(env, NULL, 0);
    CHECK(parent != NULL);

    /* Child txn */
    SapTxnCtx *child = sap_txn_begin(env, parent, 0);
    CHECK(child != NULL);

    /* Allocate regions in child */
    ThatchRegion *r1 = NULL;
    CHECK(thatch_region_new(child, &r1) == THATCH_OK);
    ThatchRegion *r2 = NULL;
    CHECK(thatch_region_new(child, &r2) == THATCH_OK);
    CHECK(sap_arena_active_pages(arena) > baseline);

    /* Child commits — regions should transfer to parent */
    CHECK(sap_txn_commit(child) == 0);

    /* Parent aborts — child-committed regions must be freed */
    sap_txn_abort(parent);
    CHECK(sap_arena_active_pages(arena) == baseline);

    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Test: [P1] nested txn: child commit + parent commit finalizes      */
/* ------------------------------------------------------------------ */
static void test_nested_child_commit_parent_commit(void) {
    printf("--- nested: child commit + parent commit ---\n");
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    make_env(&arena, &env);

    /* Parent txn */
    SapTxnCtx *parent = sap_txn_begin(env, NULL, 0);
    CHECK(parent != NULL);

    /* Child txn */
    SapTxnCtx *child = sap_txn_begin(env, parent, 0);
    CHECK(child != NULL);

    /* Write data in child */
    ThatchRegion *region = NULL;
    CHECK(thatch_region_new(child, &region) == THATCH_OK);
    CHECK(thatch_write_tag(region, 0xAA) == THATCH_OK);
    CHECK(thatch_write_data(region, "nested", 6) == THATCH_OK);

    /* Child commits */
    CHECK(sap_txn_commit(child) == 0);

    /* Parent commits */
    CHECK(sap_txn_commit(parent) == 0);

    /* Data must still be readable */
    ThatchCursor cursor = 0;
    uint8_t tag = 0;
    CHECK(thatch_read_tag(region, &cursor, &tag) == THATCH_OK);
    CHECK(tag == 0xAA);
    char buf[8] = {0};
    CHECK(thatch_read_data(region, &cursor, 6, buf) == THATCH_OK);
    CHECK(memcmp(buf, "nested", 6) == 0);

    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

/* ------------------------------------------------------------------ */
/* Entry point                                                        */
/* ------------------------------------------------------------------ */
int main(void) {
    test_subsystem_init_and_region_alloc();
    test_write_read_tag();
    test_write_read_data();
    test_skip_pointer_backpatch();
    test_seal_prevents_writes();
    test_commit_seals_regions();
    test_abort_frees_regions();
    test_multiple_regions();
    test_bounds_checking();
    test_invalid_args();
    test_nested_skip_pointers();
    test_region_valid_after_commit();
    test_commit_skip_bounds_check();
    test_region_release();
    test_double_release();
    test_wrong_owner_release();
    test_nested_child_commit_parent_abort();
    test_nested_child_commit_parent_commit();

    printf("\nResults: %d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
}
