/*
 * test_txn_vec.c — Unit tests for arena-backed growable array
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "sapling/arena.h"
#include "sapling/txn_vec.h"

#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            exit(1); \
        } \
    } while (0)

static SapMemArena *make_arena(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_MALLOC,
        .cfg.mmap.max_size = 1024 * 1024
    };
    CHECK(sap_arena_init(&arena, &opts) == ERR_OK);
    return arena;
}

/* ---- Test: init with zero capacity (lazy) ---- */
static void test_init_zero_cap(void)
{
    printf("Test: init with zero capacity\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 0) == ERR_OK);
    CHECK(vec.data == NULL);
    CHECK(vec.len == 0);
    CHECK(vec.cap == 0);

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: init with initial capacity ---- */
static void test_init_with_cap(void)
{
    printf("Test: init with initial capacity\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 16) == ERR_OK);
    CHECK(vec.data != NULL);
    CHECK(vec.len == 0);
    CHECK(vec.cap == 16);

    sap_txn_vec_destroy(&vec);
    CHECK(vec.data == NULL);
    CHECK(vec.len == 0);
    CHECK(vec.cap == 0);

    sap_arena_destroy(arena);
}

/* ---- Test: push within capacity ---- */
static void test_push_within_cap(void)
{
    printf("Test: push within capacity\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 8) == ERR_OK);

    for (uint32_t i = 0; i < 8; i++)
    {
        CHECK(sap_txn_vec_push(&vec, &i) == ERR_OK);
    }
    CHECK(vec.len == 8);
    CHECK(vec.cap == 8); /* no growth needed */

    for (uint32_t i = 0; i < 8; i++)
    {
        uint32_t *val = (uint32_t *)sap_txn_vec_at(&vec, i);
        CHECK(val != NULL);
        CHECK(*val == i);
    }

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: push triggers growth ---- */
static void test_push_triggers_growth(void)
{
    printf("Test: push triggers growth\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 4) == ERR_OK);
    CHECK(vec.cap == 4);

    /* Push 5 elements to trigger growth */
    for (uint32_t i = 0; i < 5; i++)
    {
        CHECK(sap_txn_vec_push(&vec, &i) == ERR_OK);
    }
    CHECK(vec.len == 5);
    CHECK(vec.cap == 8); /* doubled from 4 */

    /* Verify all data survived the copy */
    for (uint32_t i = 0; i < 5; i++)
    {
        uint32_t *val = (uint32_t *)sap_txn_vec_at(&vec, i);
        CHECK(val != NULL);
        CHECK(*val == i);
    }

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: lazy allocation on first push ---- */
static void test_lazy_alloc(void)
{
    printf("Test: lazy allocation on first push\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 0) == ERR_OK);
    CHECK(vec.data == NULL);

    uint32_t val = 42;
    CHECK(sap_txn_vec_push(&vec, &val) == ERR_OK);
    CHECK(vec.data != NULL);
    CHECK(vec.len == 1);
    CHECK(vec.cap >= 1);

    uint32_t *out = (uint32_t *)sap_txn_vec_at(&vec, 0);
    CHECK(out != NULL);
    CHECK(*out == 42);

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: swap_remove ---- */
static void test_swap_remove(void)
{
    printf("Test: swap_remove\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 8) == ERR_OK);

    /* Push 0,1,2,3,4 */
    for (uint32_t i = 0; i < 5; i++)
    {
        CHECK(sap_txn_vec_push(&vec, &i) == ERR_OK);
    }

    /* Remove index 1: swap with last (4), len becomes 4 */
    CHECK(sap_txn_vec_swap_remove(&vec, 1) == ERR_OK);
    CHECK(vec.len == 4);

    /* Expected: [0, 4, 2, 3] */
    CHECK(*(uint32_t *)sap_txn_vec_at(&vec, 0) == 0);
    CHECK(*(uint32_t *)sap_txn_vec_at(&vec, 1) == 4);
    CHECK(*(uint32_t *)sap_txn_vec_at(&vec, 2) == 2);
    CHECK(*(uint32_t *)sap_txn_vec_at(&vec, 3) == 3);

    /* Remove last element */
    CHECK(sap_txn_vec_swap_remove(&vec, 3) == ERR_OK);
    CHECK(vec.len == 3);

    /* Out of range */
    CHECK(sap_txn_vec_swap_remove(&vec, 5) == ERR_RANGE);
    CHECK(sap_txn_vec_swap_remove(&vec, 3) == ERR_RANGE);

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: at returns NULL for out-of-bounds ---- */
static void test_at_bounds(void)
{
    printf("Test: at boundary checks\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 8) == ERR_OK);

    CHECK(sap_txn_vec_at(&vec, 0) == NULL);

    uint32_t val = 99;
    CHECK(sap_txn_vec_push(&vec, &val) == ERR_OK);
    CHECK(sap_txn_vec_at(&vec, 0) != NULL);
    CHECK(sap_txn_vec_at(&vec, 1) == NULL);

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: reserve explicit ---- */
static void test_reserve(void)
{
    printf("Test: explicit reserve\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 0) == ERR_OK);

    CHECK(sap_txn_vec_reserve(&vec, 100) == ERR_OK);
    CHECK(vec.cap >= 100);
    CHECK(vec.len == 0);
    CHECK(vec.data != NULL);

    /* Reserve less than current cap is a no-op */
    uint32_t old_cap = vec.cap;
    CHECK(sap_txn_vec_reserve(&vec, 50) == ERR_OK);
    CHECK(vec.cap == old_cap);

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: pointer-sized elements ---- */
static void test_pointer_elements(void)
{
    printf("Test: pointer-sized elements\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(void *), 4) == ERR_OK);

    /* Push addresses of stack variables */
    int a = 1, b = 2, c = 3;
    void *pa = &a, *pb = &b, *pc = &c;
    CHECK(sap_txn_vec_push(&vec, &pa) == ERR_OK);
    CHECK(sap_txn_vec_push(&vec, &pb) == ERR_OK);
    CHECK(sap_txn_vec_push(&vec, &pc) == ERR_OK);
    CHECK(vec.len == 3);

    void **out0 = (void **)sap_txn_vec_at(&vec, 0);
    void **out1 = (void **)sap_txn_vec_at(&vec, 1);
    void **out2 = (void **)sap_txn_vec_at(&vec, 2);
    CHECK(*out0 == &a);
    CHECK(*out1 == &b);
    CHECK(*out2 == &c);

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: large struct elements ---- */
typedef struct {
    uint32_t x;
    uint32_t y;
    uint64_t z;
} TestStruct;

static void test_large_elements(void)
{
    printf("Test: large struct elements\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(TestStruct), 4) == ERR_OK);

    for (uint32_t i = 0; i < 10; i++)
    {
        TestStruct s = { .x = i, .y = i * 10, .z = i * 100 };
        CHECK(sap_txn_vec_push(&vec, &s) == ERR_OK);
    }
    CHECK(vec.len == 10);
    CHECK(vec.cap >= 10); /* grew from initial 4 */

    for (uint32_t i = 0; i < 10; i++)
    {
        TestStruct *s = (TestStruct *)sap_txn_vec_at(&vec, i);
        CHECK(s->x == i);
        CHECK(s->y == i * 10);
        CHECK(s->z == i * 100);
    }

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: invalid arguments ---- */
static void test_invalid_args(void)
{
    printf("Test: invalid arguments\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(NULL, arena, sizeof(uint32_t), 8) == ERR_INVALID);
    CHECK(sap_txn_vec_init(&vec, NULL, sizeof(uint32_t), 8) == ERR_INVALID);
    CHECK(sap_txn_vec_init(&vec, arena, 0, 8) == ERR_INVALID);

    CHECK(sap_txn_vec_push(NULL, &vec) == ERR_INVALID);
    CHECK(sap_txn_vec_at(NULL, 0) == NULL);
    CHECK(sap_txn_vec_swap_remove(NULL, 0) == ERR_INVALID);
    CHECK(sap_txn_vec_pop(NULL) == ERR_INVALID);

    sap_arena_destroy(arena);
}

/* ---- Test: pop ---- */
static void test_pop(void)
{
    printf("Test: pop\n");
    SapMemArena *arena = make_arena();

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 8) == ERR_OK);

    /* Pop from empty vec */
    CHECK(sap_txn_vec_pop(&vec) == ERR_EMPTY);

    /* Push 0,1,2 then pop back to empty */
    for (uint32_t i = 0; i < 3; i++)
        CHECK(sap_txn_vec_push(&vec, &i) == ERR_OK);
    CHECK(vec.len == 3);

    CHECK(sap_txn_vec_pop(&vec) == ERR_OK);
    CHECK(vec.len == 2);
    CHECK(*(uint32_t *)sap_txn_vec_at(&vec, 0) == 0);
    CHECK(*(uint32_t *)sap_txn_vec_at(&vec, 1) == 1);

    CHECK(sap_txn_vec_pop(&vec) == ERR_OK);
    CHECK(sap_txn_vec_pop(&vec) == ERR_OK);
    CHECK(vec.len == 0);

    CHECK(sap_txn_vec_pop(&vec) == ERR_EMPTY);

    /* Can push again after popping to empty */
    uint32_t val = 42;
    CHECK(sap_txn_vec_push(&vec, &val) == ERR_OK);
    CHECK(vec.len == 1);
    CHECK(*(uint32_t *)sap_txn_vec_at(&vec, 0) == 42);

    sap_txn_vec_destroy(&vec);
    sap_arena_destroy(arena);
}

/* ---- Test: multiple vectors on same arena ---- */
static void test_multiple_vecs(void)
{
    printf("Test: multiple vectors on same arena\n");
    SapMemArena *arena = make_arena();

    SapTxnVec v1, v2;
    CHECK(sap_txn_vec_init(&v1, arena, sizeof(uint32_t), 8) == ERR_OK);
    CHECK(sap_txn_vec_init(&v2, arena, sizeof(uint64_t), 8) == ERR_OK);

    for (uint32_t i = 0; i < 20; i++)
    {
        CHECK(sap_txn_vec_push(&v1, &i) == ERR_OK);
        uint64_t big = (uint64_t)i * 1000000;
        CHECK(sap_txn_vec_push(&v2, &big) == ERR_OK);
    }

    /* Verify no cross-contamination */
    for (uint32_t i = 0; i < 20; i++)
    {
        CHECK(*(uint32_t *)sap_txn_vec_at(&v1, i) == i);
        CHECK(*(uint64_t *)sap_txn_vec_at(&v2, i) == (uint64_t)i * 1000000);
    }

    sap_txn_vec_destroy(&v1);
    sap_txn_vec_destroy(&v2);
    sap_arena_destroy(arena);
}

/* ---- Test: custom arena tracking ---- */
static uint32_t g_alloc_count = 0;
static uint32_t g_free_count = 0;

static void *tracking_alloc(void *ctx, uint32_t size)
{
    (void)ctx;
    g_alloc_count++;
    return calloc(1, size);
}

static void tracking_free(void *ctx, void *ptr, uint32_t size)
{
    (void)ctx;
    (void)size;
    g_free_count++;
    free(ptr);
}

static void test_custom_arena_tracking(void)
{
    printf("Test: custom arena allocation tracking\n");

    g_alloc_count = 0;
    g_free_count = 0;

    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom = {
            .alloc_page = tracking_alloc,
            .free_page = tracking_free,
            .ctx = NULL
        }
    };
    CHECK(sap_arena_init(&arena, &opts) == ERR_OK);

    SapTxnVec vec;
    CHECK(sap_txn_vec_init(&vec, arena, sizeof(uint32_t), 4) == ERR_OK);
    uint32_t allocs_after_init = g_alloc_count;
    CHECK(allocs_after_init >= 1); /* at least the initial node */

    /* Force a growth */
    for (uint32_t i = 0; i < 5; i++)
    {
        CHECK(sap_txn_vec_push(&vec, &i) == ERR_OK);
    }

    /* Growth allocated a new node and freed the old one */
    CHECK(g_alloc_count > allocs_after_init);
    uint32_t frees_before_destroy = g_free_count;
    CHECK(frees_before_destroy >= 1); /* old node freed on growth */

    sap_txn_vec_destroy(&vec);
    CHECK(g_free_count == frees_before_destroy + 1); /* final node freed */

    sap_arena_destroy(arena);
}

int main(void)
{
    test_init_zero_cap();
    test_init_with_cap();
    test_push_within_cap();
    test_push_triggers_growth();
    test_lazy_alloc();
    test_swap_remove();
    test_at_bounds();
    test_reserve();
    test_pointer_elements();
    test_large_elements();
    test_invalid_args();
    test_multiple_vecs();
    test_pop();
    test_custom_arena_tracking();

    printf("All test_txn_vec tests passed.\n");
    return 0;
}
