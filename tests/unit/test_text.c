/*
 * test_text.c - unit tests for mutable text built on seq
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

typedef struct
{
    uint8_t *buf;
    size_t   cap;
    size_t   off;
} ArenaAllocator;

static size_t align_up(size_t v, size_t a)
{
    return (v + (a - 1u)) & ~(a - 1u);
}

static void *arena_alloc(void *ctx, size_t bytes)
{
    ArenaAllocator *arena = (ArenaAllocator *)ctx;
    size_t          aligned = 0;
    const size_t    max_align = sizeof(max_align_t);

    if (!arena || bytes == 0)
        return NULL;
    aligned = align_up(arena->off, max_align);
    if (aligned > arena->cap || bytes > arena->cap - aligned)
        return NULL;
    arena->off = aligned + bytes;
    return arena->buf + aligned;
}

static void arena_free_noop(void *ctx, void *ptr)
{
    (void)ctx;
    (void)ptr;
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
    TextHandle handle = 0;
    uint32_t out = 0;
    size_t utf8_len = 0;

    CHECK(text != NULL);
    CHECK(text_is_valid(NULL) == 0);
    CHECK(text_reset(NULL) == SEQ_INVALID);
    CHECK(text_push_front(NULL, 1u) == SEQ_INVALID);
    CHECK(text_push_back(NULL, 1u) == SEQ_INVALID);
    CHECK(text_push_front_handle(NULL, 1u) == SEQ_INVALID);
    CHECK(text_push_back_handle(NULL, 1u) == SEQ_INVALID);
    CHECK(text_pop_front(NULL, &out) == SEQ_INVALID);
    CHECK(text_pop_back(NULL, &out) == SEQ_INVALID);
    CHECK(text_pop_front_handle(NULL, &handle) == SEQ_INVALID);
    CHECK(text_pop_back_handle(NULL, &handle) == SEQ_INVALID);
    CHECK(text_get_handle(NULL, 0, &handle) == SEQ_INVALID);
    CHECK(text_get(NULL, 0, &out) == SEQ_INVALID);
    CHECK(text_get(text, 0, NULL) == SEQ_INVALID);
    CHECK(text_get_handle(text, 0, NULL) == SEQ_INVALID);
    CHECK(text_set(NULL, 0, 1u) == SEQ_INVALID);
    CHECK(text_set_handle(NULL, 0, 1u) == SEQ_INVALID);
    CHECK(text_insert(text, 1, 1u) == SEQ_RANGE);
    CHECK(text_insert_handle(NULL, 0, 1u) == SEQ_INVALID);
    CHECK(text_delete(text, 0, &out) == SEQ_RANGE);
    CHECK(text_delete_handle(NULL, 0, &handle) == SEQ_INVALID);
    CHECK(text_concat(text, NULL) == SEQ_INVALID);
    CHECK(text_concat(NULL, text) == SEQ_INVALID);
    CHECK(text_split_at(NULL, 0, &l, &r) == SEQ_INVALID);
    CHECK(text_split_at(text, 0, NULL, &r) == SEQ_INVALID);
    CHECK(text_split_at(text, 0, &l, NULL) == SEQ_INVALID);
    CHECK(text_handle_from_codepoint(0x41u, NULL) == SEQ_INVALID);
    CHECK(text_handle_to_codepoint(0u, NULL) == SEQ_INVALID);
    CHECK(text_from_utf8(NULL, (const uint8_t *)"a", 1) == SEQ_INVALID);
    CHECK(text_from_utf8(text, NULL, 1) == SEQ_INVALID);
    CHECK(text_utf8_length(NULL, &utf8_len) == SEQ_INVALID);
    CHECK(text_utf8_length(text, NULL) == SEQ_INVALID);
    CHECK(text_to_utf8(NULL, (uint8_t *)&out, 1, &utf8_len) == SEQ_INVALID);
    CHECK(text_to_utf8(text, NULL, 1, &utf8_len) == SEQ_INVALID);
    CHECK(text_to_utf8(text, (uint8_t *)&out, 1, NULL) == SEQ_INVALID);

    text_free(text);
}

static void test_utf8_round_trip(void)
{
    SECTION("utf8 round trip");
    Text *text = text_new();
    const uint8_t utf8[] = {
        0x41u,                   /* A */
        0xC3u, 0xA9u,            /* e-acute */
        0xE2u, 0x82u, 0xACu,     /* euro */
        0xF0u, 0x9Fu, 0x99u, 0x82u /* 🙂 */
    };
    uint8_t out[16];
    size_t need = 0;
    size_t wrote = 0;
    uint32_t cp = 0;

    CHECK(text != NULL);
    CHECK(text_from_utf8(text, utf8, sizeof(utf8)) == SEQ_OK);
    CHECK(text_length(text) == 4);
    CHECK(text_get(text, 0, &cp) == SEQ_OK && cp == 0x41u);
    CHECK(text_get(text, 1, &cp) == SEQ_OK && cp == 0xE9u);
    CHECK(text_get(text, 2, &cp) == SEQ_OK && cp == 0x20ACu);
    CHECK(text_get(text, 3, &cp) == SEQ_OK && cp == 0x1F642u);

    CHECK(text_utf8_length(text, &need) == SEQ_OK);
    CHECK(need == sizeof(utf8));
    CHECK(text_to_utf8(text, out, sizeof(out), &wrote) == SEQ_OK);
    CHECK(wrote == sizeof(utf8));
    CHECK(memcmp(out, utf8, sizeof(utf8)) == 0);

    text_free(text);
}

static void test_utf8_decode_rejects_invalid(void)
{
    SECTION("utf8 decode rejects invalid");
    Text *text = text_new();
    uint32_t before[] = {0x61u, 0x62u};
    const uint8_t overlong[] = {0xC0u, 0xAFu};
    const uint8_t truncated[] = {0xE2u, 0x82u};
    const uint8_t surrogate[] = {0xEDu, 0xA0u, 0x80u};
    const uint8_t bad_cont[] = {0xE2u, 0x28u, 0xA1u};

    CHECK(text != NULL);
    CHECK(text_push_back(text, before[0]) == SEQ_OK);
    CHECK(text_push_back(text, before[1]) == SEQ_OK);

    CHECK(text_from_utf8(text, overlong, sizeof(overlong)) == SEQ_INVALID);
    CHECK(text_equals_array(text, before, 2));

    CHECK(text_from_utf8(text, truncated, sizeof(truncated)) == SEQ_INVALID);
    CHECK(text_equals_array(text, before, 2));

    CHECK(text_from_utf8(text, surrogate, sizeof(surrogate)) == SEQ_INVALID);
    CHECK(text_equals_array(text, before, 2));

    CHECK(text_from_utf8(text, bad_cont, sizeof(bad_cont)) == SEQ_INVALID);
    CHECK(text_equals_array(text, before, 2));

    text_free(text);
}

static void test_utf8_buffer_contract(void)
{
    SECTION("utf8 output buffer contract");
    uint32_t vals[] = {0x41u, 0x20ACu};
    Text    *text = text_from_array(vals, 2);
    uint8_t  out[4];
    size_t   need = 0;
    size_t   wrote = 0;

    CHECK(text != NULL);
    CHECK(text_utf8_length(text, &need) == SEQ_OK);
    CHECK(need == 4u);

    CHECK(text_to_utf8(text, out, 3u, &wrote) == SEQ_RANGE);
    CHECK(wrote == 4u);

    CHECK(text_to_utf8(text, NULL, 0u, &wrote) == SEQ_RANGE);
    CHECK(wrote == 4u);

    CHECK(text_to_utf8(text, out, sizeof(out), &wrote) == SEQ_OK);
    CHECK(wrote == 4u);
    CHECK(out[0] == 0x41u);
    CHECK(out[1] == 0xE2u && out[2] == 0x82u && out[3] == 0xACu);

    text_free(text);
}

static void test_codepoint_validation(void)
{
    SECTION("codepoint validation");
    uint32_t base[] = {0x61u, 0x62u};
    Text    *text = text_from_array(base, 2);

    CHECK(text != NULL);
    CHECK(text_push_back(text, 0x110000u) == SEQ_INVALID);
    CHECK(text_push_front(text, 0xD800u) == SEQ_INVALID);
    CHECK(text_set(text, 0, 0x110000u) == SEQ_INVALID);
    CHECK(text_insert(text, 1, 0xDFFFu) == SEQ_INVALID);
    CHECK(text_equals_array(text, base, 2));

    text_free(text);
}

static void test_handle_codec(void)
{
    SECTION("handle codec");
    TextHandle cp_handle = 0;
    TextHandle lit_handle = 0;
    uint32_t   cp = 0;

    CHECK(text_handle_from_codepoint(0x1F642u, &cp_handle) == SEQ_OK);
    CHECK(text_handle_kind(cp_handle) == TEXT_HANDLE_CODEPOINT);
    CHECK(text_handle_payload(cp_handle) == 0x1F642u);
    CHECK(text_handle_is_codepoint(cp_handle) == 1);
    CHECK(text_handle_to_codepoint(cp_handle, &cp) == SEQ_OK && cp == 0x1F642u);

    lit_handle = text_handle_make(TEXT_HANDLE_LITERAL, 77u);
    CHECK(text_handle_kind(lit_handle) == TEXT_HANDLE_LITERAL);
    CHECK(text_handle_payload(lit_handle) == 77u);
    CHECK(text_handle_is_codepoint(lit_handle) == 0);
    CHECK(text_handle_to_codepoint(lit_handle, &cp) == SEQ_INVALID);

    CHECK(text_handle_from_codepoint(0x110000u, &cp_handle) == SEQ_INVALID);
    CHECK(text_handle_from_codepoint(0xD800u, &cp_handle) == SEQ_INVALID);
    CHECK(text_handle_to_codepoint(text_handle_make(TEXT_HANDLE_CODEPOINT, 0xD800u), &cp) ==
          SEQ_INVALID);
}

static void test_handle_apis_and_strict_codepoint_wrappers(void)
{
    SECTION("handle apis + strict codepoint wrappers");
    Text      *text = text_new();
    TextHandle cp_handle = 0;
    TextHandle lit_handle = text_handle_make(TEXT_HANDLE_LITERAL, 21u);
    TextHandle tree_handle = text_handle_make(TEXT_HANDLE_TREE, 42u);
    TextHandle out_h = 0;
    uint32_t   cp = 0;
    size_t     need = 0;

    CHECK(text != NULL);
    CHECK(text_handle_from_codepoint(0x41u, &cp_handle) == SEQ_OK);
    CHECK(text_push_back_handle(text, cp_handle) == SEQ_OK);
    CHECK(text_push_back_handle(text, lit_handle) == SEQ_OK);
    CHECK(text_push_back_handle(text, tree_handle) == SEQ_OK);
    CHECK(text_push_back_handle(text, text_handle_make(TEXT_HANDLE_RESERVED, 1u)) ==
          SEQ_INVALID);
    CHECK(text_length(text) == 3);

    CHECK(text_get_handle(text, 1, &out_h) == SEQ_OK && out_h == lit_handle);
    CHECK(text_get(text, 0, &cp) == SEQ_OK && cp == 0x41u);
    CHECK(text_get(text, 1, &cp) == SEQ_INVALID);
    CHECK(text_utf8_length(text, &need) == SEQ_INVALID);

    CHECK(text_pop_front(text, &cp) == SEQ_OK && cp == 0x41u);
    CHECK(text_pop_front(text, &cp) == SEQ_INVALID);
    CHECK(text_length(text) == 2);
    CHECK(text_pop_front_handle(text, &out_h) == SEQ_OK && out_h == lit_handle);
    CHECK(text_pop_front_handle(text, &out_h) == SEQ_OK && out_h == tree_handle);
    CHECK(text_pop_front_handle(text, &out_h) == SEQ_EMPTY);

    CHECK(text_insert_handle(text, 0, lit_handle) == SEQ_OK);
    CHECK(text_delete(text, 0, &cp) == SEQ_INVALID);
    CHECK(text_length(text) == 1);
    CHECK(text_delete_handle(text, 0, &out_h) == SEQ_OK && out_h == lit_handle);
    CHECK(text_length(text) == 0);

    text_free(text);
}

static void test_arena_allocator_exhaustion(void)
{
    SECTION("arena allocator exhaustion");
    uint8_t      storage[512];
    ArenaAllocator arena = {storage, sizeof(storage), 0};
    SeqAllocator allocator = {arena_alloc, arena_free_noop, &arena};
    Text        *text = text_new_with_allocator(&allocator);
    int          saw_oom = 0;
    uint32_t     out = 0;

    CHECK(text != NULL);
    for (size_t i = 0; i < 4096; i++)
    {
        int rc = text_push_back(text, (uint32_t)(i & 0x7Fu));
        if (rc == SEQ_OK)
            continue;
        CHECK(rc == SEQ_OOM);
        saw_oom = 1;
        break;
    }
    CHECK(saw_oom == 1);
    CHECK(text_is_valid(text) == 0);
    CHECK(text_push_back(text, 0x41u) == SEQ_INVALID);
    CHECK(text_pop_front(text, &out) == SEQ_INVALID);
    CHECK(text_pop_back(text, &out) == SEQ_INVALID);
    CHECK(text_reset(text) != SEQ_OK);

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
    test_utf8_round_trip();
    test_utf8_decode_rejects_invalid();
    test_utf8_buffer_contract();
    test_codepoint_validation();
    test_handle_codec();
    test_handle_apis_and_strict_codepoint_wrappers();
    test_arena_allocator_exhaustion();

    print_summary();
    return g_fail ? 1 : 0;
}
