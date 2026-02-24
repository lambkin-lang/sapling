/*
 * test_text.c - unit tests for mutable text built on seq
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text.h"

#include <stdio.h>
#include <stdlib.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(expr)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (expr)                                                                                  \
        {                                                                                          \
            g_pass++;                                                                              \
        }                                                                                          \
        else                                                                                       \
        {                                                                                          \
            fprintf(stderr, "FAIL: %s (%s:%d)\n", #expr, __FILE__, __LINE__);                     \
            g_fail++;                                                                              \
        }                                                                                          \
    } while (0)

#define SECTION(name) printf("--- %s ---\n", name)

static void print_summary(void) { printf("\nResults: %d passed, %d failed\n", g_pass, g_fail); }

static Text *text_from_array(const uint32_t *vals, size_t n)
{
    Text *text = text_new();
    if (!text)
        return NULL;
    for (size_t i = 0; i < n; i++)
    {
        if (text_push_back(text, vals[i]) != SEQ_OK)
        {
            text_free(text);
            return NULL;
        }
    }
    return text;
}

static int text_equals_array(Text *text, const uint32_t *vals, size_t n)
{
    if (text_length(text) != n)
        return 0;
    for (size_t i = 0; i < n; i++)
    {
        uint32_t got = 0;
        if (text_get(text, i, &got) != SEQ_OK)
            return 0;
        if (got != vals[i])
            return 0;
    }
    return 1;
}

typedef struct
{
    size_t alloc_calls;
    size_t free_calls;
} CountingAllocatorStats;

static void *counting_alloc(void *ctx, size_t bytes)
{
    CountingAllocatorStats *stats = (CountingAllocatorStats *)ctx;
    if (stats)
        stats->alloc_calls++;
    return malloc(bytes);
}

static void counting_free(void *ctx, void *ptr)
{
    CountingAllocatorStats *stats = (CountingAllocatorStats *)ctx;
    if (stats && ptr)
        stats->free_calls++;
    free(ptr);
}

static void test_empty(void)
{
    SECTION("empty");
    Text *text = text_new();
    uint32_t out = 0;

    CHECK(text != NULL);
    CHECK(text_is_valid(text) == 1);
    CHECK(text_length(text) == 0);
    CHECK(text_get(text, 0, &out) == SEQ_RANGE);
    CHECK(text_pop_front(text, &out) == SEQ_EMPTY);
    CHECK(text_pop_back(text, &out) == SEQ_EMPTY);

    text_free(text);
}

static void test_push_pop_get(void)
{
    SECTION("push/pop/get");
    Text *text = text_new();
    uint32_t out = 0;

    CHECK(text != NULL);
    CHECK(text_push_back(text, 0x61u) == SEQ_OK);
    CHECK(text_push_back(text, 0x1F600u) == SEQ_OK);
    CHECK(text_push_front(text, 0x40u) == SEQ_OK);
    CHECK(text_length(text) == 3);
    CHECK(text_get(text, 0, &out) == SEQ_OK && out == 0x40u);
    CHECK(text_get(text, 1, &out) == SEQ_OK && out == 0x61u);
    CHECK(text_get(text, 2, &out) == SEQ_OK && out == 0x1F600u);
    CHECK(text_pop_front(text, &out) == SEQ_OK && out == 0x40u);
    CHECK(text_pop_back(text, &out) == SEQ_OK && out == 0x1F600u);
    CHECK(text_pop_back(text, &out) == SEQ_OK && out == 0x61u);
    CHECK(text_pop_back(text, &out) == SEQ_EMPTY);

    text_free(text);
}

static void test_insert_set_delete(void)
{
    SECTION("insert/set/delete");
    uint32_t init[] = {1u, 2u, 3u};
    Text    *text = text_from_array(init, 3);
    uint32_t out = 0;

    CHECK(text != NULL);

    CHECK(text_insert(text, 0, 9u) == SEQ_OK);
    CHECK(text_insert(text, 2, 8u) == SEQ_OK);
    CHECK(text_insert(text, text_length(text), 7u) == SEQ_OK);
    {
        uint32_t expect[] = {9u, 1u, 8u, 2u, 3u, 7u};
        CHECK(text_equals_array(text, expect, 6));
    }
    CHECK(text_insert(text, text_length(text) + 1u, 5u) == SEQ_RANGE);

    CHECK(text_set(text, 3, 99u) == SEQ_OK);
    CHECK(text_set(text, text_length(text), 42u) == SEQ_RANGE);
    {
        uint32_t expect[] = {9u, 1u, 8u, 99u, 3u, 7u};
        CHECK(text_equals_array(text, expect, 6));
    }

    CHECK(text_delete(text, 1, &out) == SEQ_OK && out == 1u);
    CHECK(text_delete(text, 4, NULL) == SEQ_OK);
    CHECK(text_delete(text, text_length(text), NULL) == SEQ_RANGE);
    {
        uint32_t expect[] = {9u, 8u, 99u, 3u};
        CHECK(text_equals_array(text, expect, 4));
    }

    text_free(text);
}

static void test_concat_split(void)
{
    SECTION("concat/split");
    uint32_t av[] = {10u, 11u};
    uint32_t bv[] = {12u, 13u, 14u};
    Text    *a = text_from_array(av, 2);
    Text    *b = text_from_array(bv, 3);
    Text    *l = NULL;
    Text    *r = NULL;
    uint32_t expect[] = {10u, 11u, 12u, 13u, 14u};

    CHECK(a != NULL && b != NULL);
    CHECK(text_concat(a, b) == SEQ_OK);
    CHECK(text_length(a) == 5);
    CHECK(text_length(b) == 0);
    CHECK(text_equals_array(a, expect, 5));
    CHECK(text_concat(a, a) == SEQ_INVALID);

    CHECK(text_split_at(a, 2, &l, &r) == SEQ_OK);
    CHECK(text_length(a) == 0);
    {
        uint32_t left_expect[] = {10u, 11u};
        uint32_t right_expect[] = {12u, 13u, 14u};
        CHECK(text_equals_array(l, left_expect, 2));
        CHECK(text_equals_array(r, right_expect, 3));
    }

    CHECK(text_concat(a, l) == SEQ_OK);
    CHECK(text_concat(a, r) == SEQ_OK);
    CHECK(text_equals_array(a, expect, 5));

    text_free(a);
    text_free(b);
    text_free(l);
    text_free(r);
}

static void test_split_range_contract(void)
{
    SECTION("split range contract");
    uint32_t vals[] = {1u};
    Text    *text = text_from_array(vals, 1);
    Text    *l = (Text *)(uintptr_t)1;
    Text    *r = (Text *)(uintptr_t)2;

    CHECK(text != NULL);
    CHECK(text_split_at(text, 2, &l, &r) == SEQ_RANGE);
    CHECK(l == (Text *)(uintptr_t)1);
    CHECK(r == (Text *)(uintptr_t)2);

    text_free(text);
}

static void test_custom_allocator(void)
{
    SECTION("custom allocator and mismatch");
    CountingAllocatorStats stats_a = {0};
    CountingAllocatorStats stats_b = {0};
    SeqAllocator allocator_a = {counting_alloc, counting_free, &stats_a};
    SeqAllocator allocator_b = {counting_alloc, counting_free, &stats_b};
    Text *a = text_new_with_allocator(&allocator_a);
    Text *b = text_new_with_allocator(&allocator_b);
    Text *l = NULL;
    Text *r = NULL;
    size_t before_alloc_calls = 0;
    SeqAllocator bad_alloc = {NULL, counting_free, NULL};

    CHECK(a != NULL && b != NULL);
    CHECK(text_push_back(a, 0x41u) == SEQ_OK);
    CHECK(text_push_back(b, 0x42u) == SEQ_OK);
    CHECK(text_concat(a, b) == SEQ_INVALID);

    before_alloc_calls = stats_a.alloc_calls;
    CHECK(text_split_at(a, 1, &l, &r) == SEQ_OK);
    CHECK(stats_a.alloc_calls >= before_alloc_calls + 4u);
    text_free(l);
    text_free(r);

    CHECK(text_new_with_allocator(&bad_alloc) == NULL);

    text_free(a);
    text_free(b);
    CHECK(stats_a.free_calls > 0);
    CHECK(stats_b.free_calls > 0);
}

static void test_invalid_args(void)
{
    SECTION("invalid args");
    Text *text = text_new();
    Text *l = NULL;
    Text *r = NULL;
    uint32_t out = 0;

    CHECK(text != NULL);
    CHECK(text_is_valid(NULL) == 0);
    CHECK(text_reset(NULL) == SEQ_INVALID);
    CHECK(text_push_front(NULL, 1u) == SEQ_INVALID);
    CHECK(text_push_back(NULL, 1u) == SEQ_INVALID);
    CHECK(text_pop_front(NULL, &out) == SEQ_INVALID);
    CHECK(text_pop_back(NULL, &out) == SEQ_INVALID);
    CHECK(text_get(NULL, 0, &out) == SEQ_INVALID);
    CHECK(text_get(text, 0, NULL) == SEQ_INVALID);
    CHECK(text_set(NULL, 0, 1u) == SEQ_INVALID);
    CHECK(text_insert(text, 1, 1u) == SEQ_RANGE);
    CHECK(text_delete(text, 0, &out) == SEQ_RANGE);
    CHECK(text_concat(text, NULL) == SEQ_INVALID);
    CHECK(text_concat(NULL, text) == SEQ_INVALID);
    CHECK(text_split_at(NULL, 0, &l, &r) == SEQ_INVALID);
    CHECK(text_split_at(text, 0, NULL, &r) == SEQ_INVALID);
    CHECK(text_split_at(text, 0, &l, NULL) == SEQ_INVALID);

    text_free(text);
}

int main(void)
{
    printf("=== text unit tests ===\n");

    test_empty();
    test_push_pop_get();
    test_insert_set_delete();
    test_concat_split();
    test_split_range_contract();
    test_custom_allocator();
    test_invalid_args();

    print_summary();
    return g_fail ? 1 : 0;
}
