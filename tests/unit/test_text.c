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
            fprintf(stderr, "FAIL: %s (%s:%d)\n", #expr, __FILE__, __LINE__);                      \
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
    size_t cap;
    size_t off;
} ArenaAllocator;

static size_t align_up(size_t v, size_t a) { return (v + (a - 1u)) & ~(a - 1u); }

static void *arena_alloc(void *ctx, size_t bytes)
{
    ArenaAllocator *arena = (ArenaAllocator *)ctx;
    size_t aligned = 0;
    const size_t max_align = sizeof(max_align_t);

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

typedef struct
{
    TextHandle handle;
    const uint32_t *codepoints;
    size_t len;
    int rc;
} ResolveEntry;

typedef struct
{
    const ResolveEntry *entries;
    size_t count;
    size_t calls;
} ResolveCtx;

typedef struct
{
    uint32_t id;
    const uint8_t *utf8;
    size_t utf8_len;
    int rc;
} RuntimeLiteralEntry;

typedef struct
{
    uint32_t id;
    const Text *text;
    int rc;
} RuntimeTreeEntry;

typedef struct
{
    const RuntimeLiteralEntry *literals;
    size_t literal_count;
    const RuntimeTreeEntry *trees;
    size_t tree_count;
    size_t literal_calls;
    size_t tree_calls;
} RuntimeResolverCtx;

static int test_expand_handle(TextHandle handle, TextEmitCodepointFn emit_fn, void *emit_ctx,
                              void *resolver_ctx)
{
    ResolveCtx *ctx = (ResolveCtx *)resolver_ctx;

    if (!ctx || !emit_fn)
        return SEQ_INVALID;
    ctx->calls++;

    for (size_t i = 0; i < ctx->count; i++)
    {
        const ResolveEntry *entry = &ctx->entries[i];
        if (entry->handle != handle)
            continue;
        if (entry->rc != SEQ_OK)
            return entry->rc;
        for (size_t j = 0; j < entry->len; j++)
        {
            int rc = emit_fn(entry->codepoints[j], emit_ctx);
            if (rc != SEQ_OK)
                return rc;
        }
        return SEQ_OK;
    }

    return SEQ_INVALID;
}

static int runtime_resolve_literal_utf8(uint32_t literal_id, const uint8_t **utf8_out,
                                        size_t *utf8_len_out, void *ctx)
{
    RuntimeResolverCtx *resolver = (RuntimeResolverCtx *)ctx;
    if (!resolver || !utf8_out || !utf8_len_out)
        return SEQ_INVALID;

    resolver->literal_calls++;
    for (size_t i = 0; i < resolver->literal_count; i++)
    {
        const RuntimeLiteralEntry *entry = &resolver->literals[i];
        if (entry->id != literal_id)
            continue;
        if (entry->rc != SEQ_OK)
            return entry->rc;
        *utf8_out = entry->utf8;
        *utf8_len_out = entry->utf8_len;
        return SEQ_OK;
    }

    return SEQ_INVALID;
}

static int runtime_resolve_tree_text(uint32_t tree_id, const Text **tree_out, void *ctx)
{
    RuntimeResolverCtx *resolver = (RuntimeResolverCtx *)ctx;
    if (!resolver || !tree_out)
        return SEQ_INVALID;

    resolver->tree_calls++;
    for (size_t i = 0; i < resolver->tree_count; i++)
    {
        const RuntimeTreeEntry *entry = &resolver->trees[i];
        if (entry->id != tree_id)
            continue;
        if (entry->rc != SEQ_OK)
            return entry->rc;
        *tree_out = entry->text;
        return SEQ_OK;
    }

    return SEQ_INVALID;
}

static void test_empty(void)
{
    SECTION("empty");
    Text *text = text_new();
    Text *clone_null = text_clone(NULL);
    uint32_t out = 0;

    CHECK(text != NULL);
    CHECK(clone_null == NULL);
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
    Text *text = text_from_array(init, 3);
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
    Text *a = text_from_array(av, 2);
    Text *b = text_from_array(bv, 3);
    Text *l = NULL;
    Text *r = NULL;
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

static void test_clone_copy_on_write(void)
{
    SECTION("clone copy-on-write");
    uint32_t vals[] = {0x41u, 0x42u, 0x43u};
    Text *a = text_from_array(vals, 3);
    Text *b = text_clone(a);

    CHECK(a != NULL && b != NULL);
    CHECK(text_equals_array(a, vals, 3));
    CHECK(text_equals_array(b, vals, 3));

    CHECK(text_push_back(b, 0x44u) == SEQ_OK);
    {
        uint32_t expect_a[] = {0x41u, 0x42u, 0x43u};
        uint32_t expect_b[] = {0x41u, 0x42u, 0x43u, 0x44u};
        CHECK(text_equals_array(a, expect_a, 3));
        CHECK(text_equals_array(b, expect_b, 4));
    }

    CHECK(text_set(a, 0, 0x5Au) == SEQ_OK);
    {
        uint32_t expect_a[] = {0x5Au, 0x42u, 0x43u};
        uint32_t expect_b[] = {0x41u, 0x42u, 0x43u, 0x44u};
        CHECK(text_equals_array(a, expect_a, 3));
        CHECK(text_equals_array(b, expect_b, 4));
    }

    text_free(a);
    text_free(b);
}

static void test_clone_structural_detach(void)
{
    SECTION("clone structural detach");
    uint32_t vals[] = {1u, 2u, 3u};
    Text *a = text_from_array(vals, 3);
    Text *b = text_clone(a);
    Text *l = NULL;
    Text *r = NULL;
    const uint8_t utf8[] = {'x', 'y'};

    CHECK(a != NULL && b != NULL);
    CHECK(text_split_at(b, 1u, &l, &r) == SEQ_OK);
    CHECK(text_length(b) == 0u);
    {
        uint32_t expect_a[] = {1u, 2u, 3u};
        uint32_t expect_l[] = {1u};
        uint32_t expect_r[] = {2u, 3u};
        CHECK(text_equals_array(a, expect_a, 3));
        CHECK(text_equals_array(l, expect_l, 1));
        CHECK(text_equals_array(r, expect_r, 2));
    }

    CHECK(text_concat(b, l) == SEQ_OK);
    CHECK(text_concat(b, r) == SEQ_OK);
    {
        uint32_t expect_b[] = {1u, 2u, 3u};
        CHECK(text_equals_array(b, expect_b, 3));
    }

    CHECK(text_from_utf8(b, utf8, sizeof(utf8)) == SEQ_OK);
    {
        uint32_t expect_a[] = {1u, 2u, 3u};
        uint32_t expect_b[] = {0x78u, 0x79u};
        CHECK(text_equals_array(a, expect_a, 3));
        CHECK(text_equals_array(b, expect_b, 2));
    }

    text_free(a);
    text_free(b);
    text_free(l);
    text_free(r);
}

static void test_split_range_contract(void)
{
    SECTION("split range contract");
    uint32_t vals[] = {1u};
    Text *text = text_from_array(vals, 1);
    Text *l = (Text *)(uintptr_t)1;
    Text *r = (Text *)(uintptr_t)2;

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
    CHECK(text_codepoint_length_resolved(NULL, NULL, NULL, &utf8_len) == SEQ_INVALID);
    CHECK(text_codepoint_length_resolved(text, NULL, NULL, NULL) == SEQ_INVALID);
    CHECK(text_get_codepoint_resolved(NULL, 0, NULL, NULL, &out) == SEQ_INVALID);
    CHECK(text_get_codepoint_resolved(text, 0, NULL, NULL, NULL) == SEQ_INVALID);
    CHECK(text_from_utf8(NULL, (const uint8_t *)"a", 1) == SEQ_INVALID);
    CHECK(text_from_utf8(text, NULL, 1) == SEQ_INVALID);
    CHECK(text_utf8_length(NULL, &utf8_len) == SEQ_INVALID);
    CHECK(text_utf8_length(text, NULL) == SEQ_INVALID);
    CHECK(text_utf8_length_resolved(NULL, NULL, NULL, &utf8_len) == SEQ_INVALID);
    CHECK(text_utf8_length_resolved(text, NULL, NULL, NULL) == SEQ_INVALID);
    CHECK(text_to_utf8(NULL, (uint8_t *)&out, 1, &utf8_len) == SEQ_INVALID);
    CHECK(text_to_utf8(text, NULL, 1, &utf8_len) == SEQ_INVALID);
    CHECK(text_to_utf8(text, (uint8_t *)&out, 1, NULL) == SEQ_INVALID);
    CHECK(text_to_utf8_resolved(NULL, NULL, NULL, (uint8_t *)&out, 1, &utf8_len) == SEQ_INVALID);
    CHECK(text_to_utf8_resolved(text, NULL, NULL, NULL, 1, &utf8_len) == SEQ_INVALID);
    CHECK(text_to_utf8_resolved(text, NULL, NULL, (uint8_t *)&out, 1, NULL) == SEQ_INVALID);

    text_free(text);
}

static void test_utf8_round_trip(void)
{
    SECTION("utf8 round trip");
    Text *text = text_new();
    const uint8_t utf8[] = {
        0x41u,                     /* A */
        0xC3u, 0xA9u,              /* e-acute */
        0xE2u, 0x82u, 0xACu,       /* euro */
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
    Text *text = text_from_array(vals, 2);
    uint8_t out[4];
    size_t need = 0;
    size_t wrote = 0;

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
    Text *text = text_from_array(base, 2);

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
    uint32_t cp = 0;

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
    Text *text = text_new();
    TextHandle cp_handle = 0;
    TextHandle lit_handle = text_handle_make(TEXT_HANDLE_LITERAL, 21u);
    TextHandle tree_handle = text_handle_make(TEXT_HANDLE_TREE, 42u);
    TextHandle out_h = 0;
    uint32_t cp = 0;
    size_t need = 0;

    CHECK(text != NULL);
    CHECK(text_handle_from_codepoint(0x41u, &cp_handle) == SEQ_OK);
    CHECK(text_push_back_handle(text, cp_handle) == SEQ_OK);
    CHECK(text_push_back_handle(text, lit_handle) == SEQ_OK);
    CHECK(text_push_back_handle(text, tree_handle) == SEQ_OK);
    CHECK(text_push_back_handle(text, text_handle_make(TEXT_HANDLE_RESERVED, 1u)) == SEQ_INVALID);
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

static void test_resolved_codepoint_view(void)
{
    SECTION("resolved codepoint view");
    Text *text = text_new();
    TextHandle h_a = 0;
    TextHandle h_d = 0;
    TextHandle h_literal = text_handle_make(TEXT_HANDLE_LITERAL, 7u);
    TextHandle h_tree = text_handle_make(TEXT_HANDLE_TREE, 9u);
    const uint32_t literal_cps[] = {0x42u, 0x43u};
    const uint32_t tree_cps[] = {0x1F642u};
    const ResolveEntry entries[] = {
        {h_literal, literal_cps, 2u, SEQ_OK},
        {h_tree, tree_cps, 1u, SEQ_OK},
    };
    ResolveCtx resolver = {entries, 2u, 0u};
    size_t cp_len = 0;
    size_t utf8_need = 0;
    size_t utf8_wrote = 0;
    uint8_t utf8_out[16];
    const uint8_t expect_utf8[] = {0x41u, 0x42u, 0x43u, 0xF0u, 0x9Fu, 0x99u, 0x82u, 0x44u};
    const uint32_t expect_cps[] = {0x41u, 0x42u, 0x43u, 0x1F642u, 0x44u};
    uint32_t cp = 0;

    CHECK(text != NULL);
    CHECK(text_handle_from_codepoint(0x41u, &h_a) == SEQ_OK);
    CHECK(text_handle_from_codepoint(0x44u, &h_d) == SEQ_OK);
    CHECK(text_push_back_handle(text, h_a) == SEQ_OK);
    CHECK(text_push_back_handle(text, h_literal) == SEQ_OK);
    CHECK(text_push_back_handle(text, h_tree) == SEQ_OK);
    CHECK(text_push_back_handle(text, h_d) == SEQ_OK);
    CHECK(text_length(text) == 4u);

    CHECK(text_codepoint_length_resolved(text, test_expand_handle, &resolver, &cp_len) == SEQ_OK);
    CHECK(cp_len == 5u);

    for (size_t i = 0; i < 5u; i++)
        CHECK(text_get_codepoint_resolved(text, i, test_expand_handle, &resolver, &cp) == SEQ_OK &&
              cp == expect_cps[i]);
    CHECK(text_get_codepoint_resolved(text, 5u, test_expand_handle, &resolver, &cp) == SEQ_RANGE);

    CHECK(text_utf8_length(text, &utf8_need) == SEQ_INVALID);
    CHECK(text_utf8_length_resolved(text, test_expand_handle, &resolver, &utf8_need) == SEQ_OK);
    CHECK(utf8_need == sizeof(expect_utf8));
    CHECK(text_to_utf8_resolved(text, test_expand_handle, &resolver, utf8_out, sizeof(utf8_out),
                                &utf8_wrote) == SEQ_OK);
    CHECK(utf8_wrote == sizeof(expect_utf8));
    CHECK(memcmp(utf8_out, expect_utf8, sizeof(expect_utf8)) == 0);
    CHECK(text_to_utf8_resolved(text, test_expand_handle, &resolver, utf8_out, 7u, &utf8_wrote) ==
          SEQ_RANGE);
    CHECK(utf8_wrote == sizeof(expect_utf8));
    CHECK(text_to_utf8_resolved(text, test_expand_handle, &resolver, NULL, 0u, &utf8_wrote) ==
          SEQ_RANGE);
    CHECK(utf8_wrote == sizeof(expect_utf8));

    CHECK(resolver.calls > 0u);
    text_free(text);
}

static void test_resolver_error_paths(void)
{
    SECTION("resolved error paths");
    Text *text = text_new();
    TextHandle h_literal = text_handle_make(TEXT_HANDLE_LITERAL, 99u);
    const uint32_t bad_cps[] = {0xD800u};
    const ResolveEntry bad_entries[] = {
        {h_literal, bad_cps, 1u, SEQ_OK},
    };
    const ResolveEntry oom_entries[] = {
        {h_literal, NULL, 0u, SEQ_OOM},
    };
    ResolveCtx no_entries = {NULL, 0u, 0u};
    ResolveCtx bad_resolver = {bad_entries, 1u, 0u};
    ResolveCtx oom_resolver = {oom_entries, 1u, 0u};
    uint32_t cp = 0;
    size_t len = 0;

    CHECK(text != NULL);
    CHECK(text_push_back_handle(text, h_literal) == SEQ_OK);
    CHECK(text_codepoint_length_resolved(text, NULL, NULL, &len) == SEQ_INVALID);
    CHECK(text_codepoint_length_resolved(text, test_expand_handle, &no_entries, &len) ==
          SEQ_INVALID);
    CHECK(text_codepoint_length_resolved(text, test_expand_handle, &bad_resolver, &len) ==
          SEQ_INVALID);
    CHECK(text_codepoint_length_resolved(text, test_expand_handle, &oom_resolver, &len) == SEQ_OOM);
    CHECK(text_get_codepoint_resolved(text, 0u, test_expand_handle, &oom_resolver, &cp) == SEQ_OOM);
    CHECK(text_utf8_length_resolved(text, test_expand_handle, &oom_resolver, &len) == SEQ_OOM);
    CHECK(text_to_utf8_resolved(text, test_expand_handle, &oom_resolver, NULL, 0u, &len) ==
          SEQ_OOM);

    text_free(text);
}

static void test_runtime_resolver_adapter(void)
{
    SECTION("runtime resolver adapter");
    Text *root = text_new();
    Text *tree_outer = text_new();
    Text *tree_inner = text_new();
    TextHandle h_cp_d = 0;
    TextHandle h_cp_e = 0;
    const uint8_t lit_a[] = {'A'};
    const uint8_t lit_bc[] = {'B', 'C'};
    const uint8_t lit_smile[] = {0xF0u, 0x9Fu, 0x99u, 0x82u};
    const RuntimeLiteralEntry literals[] = {
        {1u, lit_a, sizeof(lit_a), SEQ_OK},
        {2u, lit_bc, sizeof(lit_bc), SEQ_OK},
        {3u, lit_smile, sizeof(lit_smile), SEQ_OK},
    };
    RuntimeTreeEntry trees[] = {
        {10u, tree_outer, SEQ_OK},
        {11u, tree_inner, SEQ_OK},
    };
    RuntimeResolverCtx resolver_ctx = {literals, 3u, trees, 2u, 0u, 0u};
    TextRuntimeResolver resolver = {runtime_resolve_literal_utf8, runtime_resolve_tree_text,
                                    &resolver_ctx, 8u, 32u};
    const uint32_t expect_cps[] = {0x41u, 0x42u, 0x43u, 0x44u, 0x1F642u, 0x45u};
    const uint8_t expect_utf8[] = {0x41u, 0x42u, 0x43u, 0x44u, 0xF0u, 0x9Fu, 0x99u, 0x82u, 0x45u};
    uint8_t utf8_out[32];
    size_t cp_len = 0;
    size_t utf8_len = 0;
    size_t utf8_wrote = 0;
    uint32_t cp = 0;

    CHECK(root != NULL && tree_outer != NULL && tree_inner != NULL);
    CHECK(text_handle_from_codepoint(0x44u, &h_cp_d) == SEQ_OK);
    CHECK(text_handle_from_codepoint(0x45u, &h_cp_e) == SEQ_OK);

    CHECK(text_push_back_handle(tree_inner, text_handle_make(TEXT_HANDLE_LITERAL, 2u)) == SEQ_OK);
    CHECK(text_push_back_handle(tree_inner, h_cp_d) == SEQ_OK);
    CHECK(text_push_back_handle(tree_outer, text_handle_make(TEXT_HANDLE_TREE, 11u)) == SEQ_OK);
    CHECK(text_push_back_handle(tree_outer, text_handle_make(TEXT_HANDLE_LITERAL, 3u)) == SEQ_OK);
    CHECK(text_push_back_handle(root, text_handle_make(TEXT_HANDLE_LITERAL, 1u)) == SEQ_OK);
    CHECK(text_push_back_handle(root, text_handle_make(TEXT_HANDLE_TREE, 10u)) == SEQ_OK);
    CHECK(text_push_back_handle(root, h_cp_e) == SEQ_OK);

    CHECK(text_utf8_length(root, &utf8_len) == SEQ_INVALID);
    CHECK(text_codepoint_length_resolved(root, text_expand_runtime_handle, &resolver, &cp_len) ==
          SEQ_OK);
    CHECK(cp_len == 6u);
    for (size_t i = 0; i < 6u; i++)
        CHECK(text_get_codepoint_resolved(root, i, text_expand_runtime_handle, &resolver, &cp) ==
                  SEQ_OK &&
              cp == expect_cps[i]);
    CHECK(text_get_codepoint_resolved(root, 6u, text_expand_runtime_handle, &resolver, &cp) ==
          SEQ_RANGE);

    CHECK(text_utf8_length_resolved(root, text_expand_runtime_handle, &resolver, &utf8_len) ==
          SEQ_OK);
    CHECK(utf8_len == sizeof(expect_utf8));
    CHECK(text_to_utf8_resolved(root, text_expand_runtime_handle, &resolver, utf8_out,
                                sizeof(utf8_out), &utf8_wrote) == SEQ_OK);
    CHECK(utf8_wrote == sizeof(expect_utf8));
    CHECK(memcmp(utf8_out, expect_utf8, sizeof(expect_utf8)) == 0);
    CHECK(text_to_utf8_resolved(root, text_expand_runtime_handle, &resolver, utf8_out, 8u,
                                &utf8_wrote) == SEQ_RANGE);
    CHECK(utf8_wrote == sizeof(expect_utf8));

    CHECK(resolver_ctx.literal_calls > 0u);
    CHECK(resolver_ctx.tree_calls > 0u);

    text_free(root);
    text_free(tree_outer);
    text_free(tree_inner);
}

static void test_runtime_resolver_guards_and_errors(void)
{
    SECTION("runtime resolver guards/errors");
    Text *root_cycle = text_new();
    Text *tree_a = text_new();
    Text *tree_b = text_new();
    Text *root_depth = text_new();
    Text *tree_c = text_new();
    Text *tree_d = text_new();
    Text *root_visits = text_new();
    Text *tree_e = text_new();
    Text *tree_f = text_new();
    Text *tree_g = text_new();
    Text *root_literal = text_new();
    const uint8_t bad_utf8[] = {0xC0u, 0xAFu};
    const RuntimeLiteralEntry literals_bad[] = {
        {5u, bad_utf8, sizeof(bad_utf8), SEQ_OK},
    };
    RuntimeTreeEntry cycle_trees[] = {
        {20u, tree_a, SEQ_OK},
        {21u, tree_b, SEQ_OK},
    };
    RuntimeTreeEntry depth_trees[] = {
        {30u, tree_c, SEQ_OK},
        {31u, tree_d, SEQ_OK},
    };
    RuntimeTreeEntry visits_trees[] = {
        {40u, tree_e, SEQ_OK},
        {41u, tree_f, SEQ_OK},
        {42u, tree_g, SEQ_OK},
    };
    RuntimeResolverCtx cycle_ctx = {NULL, 0u, cycle_trees, 2u, 0u, 0u};
    RuntimeResolverCtx depth_ctx = {NULL, 0u, depth_trees, 2u, 0u, 0u};
    RuntimeResolverCtx visits_ctx = {NULL, 0u, visits_trees, 3u, 0u, 0u};
    RuntimeResolverCtx bad_lit_ctx = {literals_bad, 1u, NULL, 0u, 0u, 0u};
    TextRuntimeResolver cycle_resolver = {runtime_resolve_literal_utf8, runtime_resolve_tree_text,
                                          &cycle_ctx, 8u, 32u};
    TextRuntimeResolver depth_resolver_bad = {runtime_resolve_literal_utf8,
                                              runtime_resolve_tree_text, &depth_ctx, 1u, 32u};
    TextRuntimeResolver depth_resolver_ok = {runtime_resolve_literal_utf8,
                                             runtime_resolve_tree_text, &depth_ctx, 2u, 32u};
    TextRuntimeResolver visits_resolver = {runtime_resolve_literal_utf8, runtime_resolve_tree_text,
                                           &visits_ctx, 8u, 2u};
    TextRuntimeResolver visits_resolver_ok = {runtime_resolve_literal_utf8,
                                              runtime_resolve_tree_text, &visits_ctx, 8u, 4u};
    TextRuntimeResolver bad_lit_resolver = {runtime_resolve_literal_utf8, runtime_resolve_tree_text,
                                            &bad_lit_ctx, 8u, 32u};
    TextRuntimeResolver missing_lit_cb = {NULL, runtime_resolve_tree_text, &bad_lit_ctx, 8u, 32u};
    TextRuntimeResolver missing_tree_cb = {runtime_resolve_literal_utf8, NULL, &cycle_ctx, 8u, 32u};
    size_t len = 0;

    CHECK(root_cycle && tree_a && tree_b && root_depth && tree_c && tree_d);
    CHECK(root_visits && tree_e && tree_f && tree_g && root_literal);

    CHECK(text_push_back_handle(tree_a, text_handle_make(TEXT_HANDLE_TREE, 21u)) == SEQ_OK);
    CHECK(text_push_back_handle(tree_b, text_handle_make(TEXT_HANDLE_TREE, 20u)) == SEQ_OK);
    CHECK(text_push_back_handle(root_cycle, text_handle_make(TEXT_HANDLE_TREE, 20u)) == SEQ_OK);
    CHECK(text_codepoint_length_resolved(root_cycle, text_expand_runtime_handle, &cycle_resolver,
                                         &len) == SEQ_INVALID);

    CHECK(text_push_back_handle(tree_d, text_handle_make(TEXT_HANDLE_CODEPOINT, 0x51u)) == SEQ_OK);
    CHECK(text_push_back_handle(tree_c, text_handle_make(TEXT_HANDLE_TREE, 31u)) == SEQ_OK);
    CHECK(text_push_back_handle(root_depth, text_handle_make(TEXT_HANDLE_TREE, 30u)) == SEQ_OK);
    CHECK(text_codepoint_length_resolved(root_depth, text_expand_runtime_handle,
                                         &depth_resolver_bad, &len) == SEQ_INVALID);
    CHECK(text_codepoint_length_resolved(root_depth, text_expand_runtime_handle, &depth_resolver_ok,
                                         &len) == SEQ_OK);
    CHECK(len == 1u);

    CHECK(text_push_back_handle(tree_f, text_handle_make(TEXT_HANDLE_CODEPOINT, 0x61u)) == SEQ_OK);
    CHECK(text_push_back_handle(tree_g, text_handle_make(TEXT_HANDLE_CODEPOINT, 0x62u)) == SEQ_OK);
    CHECK(text_push_back_handle(tree_e, text_handle_make(TEXT_HANDLE_TREE, 41u)) == SEQ_OK);
    CHECK(text_push_back_handle(tree_e, text_handle_make(TEXT_HANDLE_TREE, 42u)) == SEQ_OK);
    CHECK(text_push_back_handle(root_visits, text_handle_make(TEXT_HANDLE_TREE, 40u)) == SEQ_OK);
    CHECK(text_codepoint_length_resolved(root_visits, text_expand_runtime_handle, &visits_resolver,
                                         &len) == SEQ_INVALID);
    CHECK(text_codepoint_length_resolved(root_visits, text_expand_runtime_handle,
                                         &visits_resolver_ok, &len) == SEQ_OK);
    CHECK(len == 2u);

    CHECK(text_push_back_handle(root_literal, text_handle_make(TEXT_HANDLE_LITERAL, 5u)) == SEQ_OK);
    CHECK(text_codepoint_length_resolved(root_literal, text_expand_runtime_handle,
                                         &bad_lit_resolver, &len) == SEQ_INVALID);
    CHECK(text_codepoint_length_resolved(root_literal, text_expand_runtime_handle, &missing_lit_cb,
                                         &len) == SEQ_INVALID);
    CHECK(text_codepoint_length_resolved(root_cycle, text_expand_runtime_handle, &missing_tree_cb,
                                         &len) == SEQ_INVALID);

    text_free(root_cycle);
    text_free(tree_a);
    text_free(tree_b);
    text_free(root_depth);
    text_free(tree_c);
    text_free(tree_d);
    text_free(root_visits);
    text_free(tree_e);
    text_free(tree_f);
    text_free(tree_g);
    text_free(root_literal);
}

static void test_arena_allocator_exhaustion(void)
{
    SECTION("arena allocator exhaustion");
    uint8_t storage[512];
    ArenaAllocator arena = {storage, sizeof(storage), 0};
    SeqAllocator allocator = {arena_alloc, arena_free_noop, &arena};
    Text *text = text_new_with_allocator(&allocator);
    int saw_oom = 0;
    uint32_t out = 0;

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
    test_clone_copy_on_write();
    test_clone_structural_detach();
    test_split_range_contract();
    test_custom_allocator();
    test_invalid_args();
    test_utf8_round_trip();
    test_utf8_decode_rejects_invalid();
    test_utf8_buffer_contract();
    test_codepoint_validation();
    test_handle_codec();
    test_handle_apis_and_strict_codepoint_wrappers();
    test_resolved_codepoint_view();
    test_resolver_error_paths();
    test_runtime_resolver_adapter();
    test_runtime_resolver_guards_and_errors();
    test_arena_allocator_exhaustion();

    print_summary();
    return g_fail ? 1 : 0;
}
