/*
 * test_text.c - unit tests for mutable text built on seq
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text.h"
#include "sapling/text_literal.h"
#include "sapling/text_tree_registry.h"
#include "sapling/txn.h"
#include "sapling/arena.h"
#include <sapling/sapling.h>

/* Forward declare subsystem init */
int sap_seq_subsystem_init(SapEnv *env);

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_pass = 0;
static int g_fail = 0;

/* Global test environment */
static SapMemArena *g_arena = NULL;
static SapEnv *g_env = NULL;

static void setup_env(void)
{
    SapArenaOptions opts = {0};
    opts.type = SAP_ARENA_BACKING_MALLOC;
    opts.page_size = 4096;
    if (sap_arena_init(&g_arena, &opts) != ERR_OK)
    {
        fprintf(stderr, "Failed to init arena\n");
        exit(1);
    }
    g_env = sap_env_create(g_arena, 4096);
    if (!g_env)
    {
        fprintf(stderr, "Failed to create env\n");
        exit(1);
    }
    sap_seq_subsystem_init(g_env);
}

static void teardown_env(void)
{
    if (g_env)
        sap_env_destroy(g_env);
    if (g_arena)
        sap_arena_destroy(g_arena);
}

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

static int text_push_back_w(Text *text, uint32_t val)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_push_back(txn, text, val);
    sap_txn_commit(txn);
    return rc;
}

static Text *text_from_array(const uint32_t *vals, size_t n)
{
    Text *text = text_new(g_env);
    if (!text)
        return NULL;
    for (size_t i = 0; i < n; i++)
    {
        if (text_push_back_w(text, vals[i]) != ERR_OK)
        {
            text_free(g_env, text);
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
        if (text_get(text, i, &got) != ERR_OK)
            return 0;
        if (got != vals[i])
            return 0;
    }
    return 1;
}

static int text_push_front_w(Text *text, uint32_t val)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_push_front(txn, text, val);
    sap_txn_commit(txn);
    return rc;
}

static int text_pop_back_w(Text *text, uint32_t *val)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_pop_back(txn, text, val);
    sap_txn_commit(txn);
    return rc;
}

static int text_pop_front_w(Text *text, uint32_t *val)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_pop_front(txn, text, val);
    sap_txn_commit(txn);
    return rc;
}

static int text_set_w(Text *text, size_t idx, uint32_t val)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_set(txn, text, idx, val);
    sap_txn_commit(txn);
    return rc;
}

static int text_insert_w(Text *text, size_t idx, uint32_t val)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_insert(txn, text, idx, val);
    sap_txn_commit(txn);
    return rc;
}

static int text_delete_w(Text *text, size_t idx, uint32_t *out)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_delete(txn, text, idx, out);
    sap_txn_commit(txn);
    return rc;
}

static int text_concat_w(Text *dest, Text *src)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_concat(txn, dest, src);
    sap_txn_commit(txn);
    return rc;
}

static int text_split_at_w(Text *text, size_t idx, Text **l, Text **r)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_split_at(txn, text, idx, l, r);
    sap_txn_commit(txn);
    return rc;
}

static int text_from_utf8_w(Text *text, const uint8_t *utf8, size_t len)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_from_utf8(txn, text, utf8, len);
    sap_txn_commit(txn);
    return rc;
}

static int text_reset_w(Text *text)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_reset(txn, text);
    sap_txn_commit(txn);
    return rc;
}

static int text_push_back_handle_w(Text *text, TextHandle handle)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_push_back_handle(txn, text, handle);
    sap_txn_commit(txn);
    return rc;
}

static int text_pop_front_handle_w(Text *text, TextHandle *out)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_pop_front_handle(txn, text, out);
    sap_txn_commit(txn);
    return rc;
}

static int text_insert_handle_w(Text *text, size_t idx, TextHandle handle)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_insert_handle(txn, text, idx, handle);
    sap_txn_commit(txn);
    return rc;
}

static int text_delete_handle_w(Text *text, size_t idx, TextHandle *out)
{
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    int rc = text_delete_handle(txn, text, idx, out);
    sap_txn_commit(txn);
    return rc;
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
        return ERR_INVALID;
    ctx->calls++;

    for (size_t i = 0; i < ctx->count; i++)
    {
        const ResolveEntry *entry = &ctx->entries[i];
        if (entry->handle != handle)
            continue;
        if (entry->rc != ERR_OK)
            return entry->rc;
        for (size_t j = 0; j < entry->len; j++)
        {
            int rc = emit_fn(entry->codepoints[j], emit_ctx);
            if (rc != ERR_OK)
                return rc;
        }
        return ERR_OK;
    }

    return ERR_INVALID;
}

static int runtime_resolve_literal_utf8(uint32_t literal_id, const uint8_t **utf8_out,
                                        size_t *utf8_len_out, void *ctx)
{
    RuntimeResolverCtx *resolver = (RuntimeResolverCtx *)ctx;
    if (!resolver || !utf8_out || !utf8_len_out)
        return ERR_INVALID;

    resolver->literal_calls++;
    for (size_t i = 0; i < resolver->literal_count; i++)
    {
        const RuntimeLiteralEntry *entry = &resolver->literals[i];
        if (entry->id != literal_id)
            continue;
        if (entry->rc != ERR_OK)
            return entry->rc;
        *utf8_out = entry->utf8;
        *utf8_len_out = entry->utf8_len;
        return ERR_OK;
    }

    return ERR_INVALID;
}

static int runtime_resolve_tree_text(uint32_t tree_id, const Text **tree_out, void *ctx)
{
    RuntimeResolverCtx *resolver = (RuntimeResolverCtx *)ctx;
    if (!resolver || !tree_out)
        return ERR_INVALID;

    resolver->tree_calls++;
    for (size_t i = 0; i < resolver->tree_count; i++)
    {
        const RuntimeTreeEntry *entry = &resolver->trees[i];
        if (entry->id != tree_id)
            continue;
        if (entry->rc != ERR_OK)
            return entry->rc;
        *tree_out = entry->text;
        return ERR_OK;
    }

    return ERR_INVALID;
}

static void test_empty(void)
{
    SECTION("empty");
    Text *text = text_new(g_env);
    Text *clone_null = text_clone(g_env, NULL);
    uint32_t out = 0;

    CHECK(text != NULL);
    CHECK(clone_null == NULL);
    CHECK(text_is_valid(text) == 1);
    CHECK(text_length(text) == 0);
    CHECK(text_get(text, 0, &out) == ERR_RANGE);
    CHECK(text_pop_front_w(text, &out) == ERR_EMPTY);
    CHECK(text_pop_back_w(text, &out) == ERR_EMPTY);

    text_free(g_env, text);
}

static void test_push_pop_get(void)
{
    SECTION("push/pop/get");
    Text *text = text_new(g_env);
    uint32_t out = 0;

    CHECK(text != NULL);
    CHECK(text_push_back_w(text, 0x61u) == ERR_OK);
    CHECK(text_push_back_w(text, 0x1F600u) == ERR_OK);
    CHECK(text_push_front_w(text, 0x40u) == ERR_OK);
    CHECK(text_length(text) == 3);
    CHECK(text_get(text, 0, &out) == ERR_OK && out == 0x40u);
    CHECK(text_get(text, 1, &out) == ERR_OK && out == 0x61u);
    CHECK(text_get(text, 2, &out) == ERR_OK && out == 0x1F600u);
    CHECK(text_pop_front_w(text, &out) == ERR_OK && out == 0x40u);
    CHECK(text_pop_back_w(text, &out) == ERR_OK && out == 0x1F600u);
    CHECK(text_pop_back_w(text, &out) == ERR_OK && out == 0x61u);
    CHECK(text_pop_back_w(text, &out) == ERR_EMPTY);

    text_free(g_env, text);
}

static void test_insert_set_delete(void)
{
    SECTION("insert/set/delete");
    uint32_t init[] = {1u, 2u, 3u};
    Text *text = text_from_array(init, 3);
    uint32_t out = 0;

    CHECK(text != NULL);

    CHECK(text_insert_w(text, 0, 9u) == ERR_OK);
    CHECK(text_insert_w(text, 2, 8u) == ERR_OK);
    CHECK(text_insert_w(text, text_length(text), 7u) == ERR_OK);
    {
        uint32_t expect[] = {9u, 1u, 8u, 2u, 3u, 7u};
        CHECK(text_equals_array(text, expect, 6));
    }
    CHECK(text_insert_w(text, text_length(text) + 1u, 5u) == ERR_RANGE);

    CHECK(text_set_w(text, 3, 99u) == ERR_OK);
    CHECK(text_set_w(text, text_length(text), 42u) == ERR_RANGE);
    {
        uint32_t expect[] = {9u, 1u, 8u, 99u, 3u, 7u};
        CHECK(text_equals_array(text, expect, 6));
    }

    CHECK(text_delete_w(text, 1, &out) == ERR_OK && out == 1u);
    CHECK(text_delete_w(text, 4, NULL) == ERR_OK);
    CHECK(text_delete_w(text, text_length(text), NULL) == ERR_RANGE);
    {
        uint32_t expect[] = {9u, 8u, 99u, 3u};
        CHECK(text_equals_array(text, expect, 4));
    }

    text_free(g_env, text);
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
    CHECK(text_concat_w(a, b) == ERR_OK);
    CHECK(text_length(a) == 5);
    CHECK(text_length(b) == 0);
    CHECK(text_equals_array(a, expect, 5));
    CHECK(text_concat_w(a, a) == ERR_INVALID);

    CHECK(text_split_at_w(a, 2, &l, &r) == ERR_OK);
    CHECK(text_length(a) == 0);
    {
        uint32_t left_expect[] = {10u, 11u};
        uint32_t right_expect[] = {12u, 13u, 14u};
        CHECK(text_equals_array(l, left_expect, 2));
        CHECK(text_equals_array(r, right_expect, 3));
    }

    CHECK(text_concat_w(a, l) == ERR_OK);
    CHECK(text_concat_w(a, r) == ERR_OK);
    CHECK(text_equals_array(a, expect, 5));

    text_free(g_env, a);
    text_free(g_env, b);
    text_free(g_env, l);
    text_free(g_env, r);
}

static void test_clone_copy_on_write(void)
{
    SECTION("clone copy-on-write");
    uint32_t vals[] = {0x41u, 0x42u, 0x43u};
    Text *a = text_from_array(vals, 3);
    Text *b = text_clone(g_env, a);

    CHECK(a != NULL && b != NULL);
    CHECK(text_equals_array(a, vals, 3));
    CHECK(text_equals_array(b, vals, 3));

    CHECK(text_push_back_w(b, 0x44u) == ERR_OK);
    {
        uint32_t expect_a[] = {0x41u, 0x42u, 0x43u};
        uint32_t expect_b[] = {0x41u, 0x42u, 0x43u, 0x44u};
        CHECK(text_equals_array(a, expect_a, 3));
        CHECK(text_equals_array(b, expect_b, 4));
    }

    CHECK(text_set_w(a, 0, 0x5Au) == ERR_OK);
    {
        uint32_t expect_a[] = {0x5Au, 0x42u, 0x43u};
        uint32_t expect_b[] = {0x41u, 0x42u, 0x43u, 0x44u};
        CHECK(text_equals_array(a, expect_a, 3));
        CHECK(text_equals_array(b, expect_b, 4));
    }

    text_free(g_env, a);
    text_free(g_env, b);
}

static void test_clone_structural_detach(void)
{
    SECTION("clone structural detach");
    uint32_t vals[] = {1u, 2u, 3u};
    Text *a = text_from_array(vals, 3);
    Text *b = text_clone(g_env, a);
    Text *l = NULL;
    Text *r = NULL;
    const uint8_t utf8[] = {'x', 'y'};

    CHECK(a != NULL && b != NULL);
    CHECK(text_split_at_w(b, 1u, &l, &r) == ERR_OK);
    CHECK(text_length(b) == 0u);
    {
        uint32_t expect_a[] = {1u, 2u, 3u};
        uint32_t expect_l[] = {1u};
        uint32_t expect_r[] = {2u, 3u};
        CHECK(text_equals_array(a, expect_a, 3));
        CHECK(text_equals_array(l, expect_l, 1));
        CHECK(text_equals_array(r, expect_r, 2));
    }

    CHECK(text_concat_w(b, l) == ERR_OK);
    CHECK(text_concat_w(b, r) == ERR_OK);
    {
        uint32_t expect_b[] = {1u, 2u, 3u};
        CHECK(text_equals_array(b, expect_b, 3));
    }

    CHECK(text_from_utf8_w(b, utf8, sizeof(utf8)) == ERR_OK);
    {
        uint32_t expect_a[] = {1u, 2u, 3u};
        uint32_t expect_b[] = {0x78u, 0x79u};
        CHECK(text_equals_array(a, expect_a, 3));
        CHECK(text_equals_array(b, expect_b, 2));
    }

    text_free(g_env, a);
    text_free(g_env, b);
    text_free(g_env, l);
    text_free(g_env, r);
}

static void test_split_range_contract(void)
{
    SECTION("split range contract");
    uint32_t vals[] = {1u};
    Text *text = text_from_array(vals, 1);
    Text *l = (Text *)(uintptr_t)1;
    Text *r = (Text *)(uintptr_t)2;

    CHECK(text != NULL);
    CHECK(text_split_at_w(text, 2, &l, &r) == ERR_RANGE);
    CHECK(l == (Text *)(uintptr_t)1);
    CHECK(r == (Text *)(uintptr_t)2);

    text_free(g_env, text);
}

static void test_invalid_args(void)
{
    SECTION("invalid args");
    Text *text = text_new(g_env);
    Text *l = NULL;
    Text *r = NULL;
    TextHandle handle = 0;
    uint32_t out = 0;
    size_t utf8_len = 0;
    SapTxnCtx *txn = NULL; // Invalid txn

    CHECK(text != NULL);
    CHECK(text_is_valid(NULL) == 0);
    CHECK(text_reset_w(NULL) == ERR_INVALID);
    CHECK(text_push_front_w(NULL, 1u) == ERR_INVALID);
    CHECK(text_push_back_w(NULL, 1u) == ERR_INVALID);

    // Raw handle calls need explicit txn arg
    CHECK(text_push_front_handle(txn, NULL, 1u) == ERR_INVALID);
    CHECK(text_push_back_handle(txn, NULL, 1u) == ERR_INVALID);
    CHECK(text_pop_front_handle(txn, NULL, &out) == ERR_INVALID);
    CHECK(text_pop_back_handle(txn, NULL, &out) == ERR_INVALID);
    CHECK(text_pop_front_handle(txn, NULL, &handle) == ERR_INVALID);
    CHECK(text_pop_back_handle(txn, NULL, &handle) == ERR_INVALID);
    CHECK(text_get_handle(NULL, 0, &handle) == ERR_INVALID);
    CHECK(text_get(NULL, 0, &out) == ERR_INVALID);
    CHECK(text_get(text, 0, NULL) == ERR_INVALID);
    CHECK(text_get_handle(text, 0, NULL) == ERR_INVALID);
    CHECK(text_set_w(NULL, 0, 1u) == ERR_INVALID);
    CHECK(text_set_handle(txn, NULL, 0, 1u) == ERR_INVALID);
    CHECK(text_insert_w(text, 1, 1u) == ERR_RANGE);
    CHECK(text_insert_handle_w(NULL, 0, 1u) == ERR_INVALID);
    CHECK(text_delete_w(text, 0, &out) == ERR_RANGE);
    CHECK(text_delete_handle_w(NULL, 0, &handle) == ERR_INVALID);
    CHECK(text_concat_w(text, NULL) == ERR_INVALID);
    CHECK(text_concat_w(NULL, text) == ERR_INVALID);
    CHECK(text_split_at_w(NULL, 0, &l, &r) == ERR_INVALID);
    CHECK(text_split_at_w(text, 0, NULL, &r) == ERR_INVALID);
    CHECK(text_split_at_w(text, 0, &l, NULL) == ERR_INVALID);
    CHECK(text_handle_from_codepoint(0x41u, NULL) == ERR_INVALID);
    CHECK(text_handle_to_codepoint(0u, NULL) == ERR_INVALID);
    CHECK(text_codepoint_length_resolved(NULL, NULL, NULL, &utf8_len) == ERR_INVALID);
    CHECK(text_codepoint_length_resolved(text, NULL, NULL, NULL) == ERR_INVALID);
    CHECK(text_get_codepoint_resolved(NULL, 0, NULL, NULL, &out) == ERR_INVALID);
    CHECK(text_get_codepoint_resolved(text, 0, NULL, NULL, NULL) == ERR_INVALID);
    CHECK(text_from_utf8_w(NULL, (const uint8_t *)"a", 1) == ERR_INVALID);
    CHECK(text_from_utf8_w(text, NULL, 1) == ERR_INVALID);
    CHECK(text_utf8_length(NULL, &utf8_len) == ERR_INVALID);
    CHECK(text_utf8_length(text, NULL) == ERR_INVALID);
    CHECK(text_utf8_length_resolved(NULL, NULL, NULL, &utf8_len) == ERR_INVALID);
    CHECK(text_utf8_length_resolved(text, NULL, NULL, NULL) == ERR_INVALID);
    CHECK(text_to_utf8(NULL, (uint8_t *)&out, 1, &utf8_len) == ERR_INVALID);
    CHECK(text_to_utf8(text, NULL, 1, &utf8_len) == ERR_INVALID);
    CHECK(text_to_utf8(text, (uint8_t *)&out, 1, NULL) == ERR_INVALID);
    CHECK(text_to_utf8_resolved(NULL, NULL, NULL, (uint8_t *)&out, 1, &utf8_len) == ERR_INVALID);
    CHECK(text_to_utf8_resolved(text, NULL, NULL, NULL, 1, &utf8_len) == ERR_INVALID);
    CHECK(text_to_utf8_resolved(text, NULL, NULL, (uint8_t *)&out, 1, NULL) == ERR_INVALID);

    text_free(g_env, text);
}

static void test_utf8_round_trip(void)
{
    SECTION("utf8 round trip");
    Text *text = text_new(g_env);
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
    CHECK(text_from_utf8_w(text, utf8, sizeof(utf8)) == ERR_OK);
    CHECK(text_length(text) == 4);
    CHECK(text_get(text, 0, &cp) == ERR_OK && cp == 0x41u);
    CHECK(text_get(text, 1, &cp) == ERR_OK && cp == 0xE9u);
    CHECK(text_get(text, 2, &cp) == ERR_OK && cp == 0x20ACu);
    CHECK(text_get(text, 3, &cp) == ERR_OK && cp == 0x1F642u);

    CHECK(text_utf8_length(text, &need) == ERR_OK);
    CHECK(need == sizeof(utf8));
    CHECK(text_to_utf8(text, out, sizeof(out), &wrote) == ERR_OK);
    CHECK(wrote == sizeof(utf8));
    CHECK(memcmp(out, utf8, sizeof(utf8)) == 0);

    text_free(g_env, text);
}

static void test_utf8_decode_rejects_invalid(void)
{
    SECTION("utf8 decode rejects invalid");
    Text *text = text_new(g_env);
    uint32_t before[] = {0x61u, 0x62u};
    const uint8_t overlong[] = {0xC0u, 0xAFu};
    const uint8_t truncated[] = {0xE2u, 0x82u};
    const uint8_t surrogate[] = {0xEDu, 0xA0u, 0x80u};
    const uint8_t bad_cont[] = {0xE2u, 0x28u, 0xA1u};

    CHECK(text != NULL);
    CHECK(text_push_back_w(text, before[0]) == ERR_OK);
    CHECK(text_push_back_w(text, before[1]) == ERR_OK);

    CHECK(text_from_utf8_w(text, overlong, sizeof(overlong)) == ERR_INVALID);
    CHECK(text_equals_array(text, before, 2));

    CHECK(text_from_utf8_w(text, truncated, sizeof(truncated)) == ERR_INVALID);
    CHECK(text_equals_array(text, before, 2));

    CHECK(text_from_utf8_w(text, surrogate, sizeof(surrogate)) == ERR_INVALID);
    CHECK(text_equals_array(text, before, 2));

    CHECK(text_from_utf8_w(text, bad_cont, sizeof(bad_cont)) == ERR_INVALID);
    CHECK(text_equals_array(text, before, 2));

    text_free(g_env, text);
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
    CHECK(text_utf8_length(text, &need) == ERR_OK);
    CHECK(need == 4u);

    CHECK(text_to_utf8(text, out, 3u, &wrote) == ERR_RANGE);
    CHECK(wrote == 4u);

    CHECK(text_to_utf8(text, NULL, 0u, &wrote) == ERR_RANGE);
    CHECK(wrote == 4u);

    CHECK(text_to_utf8(text, out, sizeof(out), &wrote) == ERR_OK);
    CHECK(wrote == 4u);
    CHECK(out[0] == 0x41u);
    CHECK(out[1] == 0xE2u && out[2] == 0x82u && out[3] == 0xACu);

    text_free(g_env, text);
}

static void test_codepoint_validation(void)
{
    SECTION("codepoint validation");
    uint32_t base[] = {0x61u, 0x62u};
    Text *text = text_from_array(base, 2);

    CHECK(text != NULL);
    CHECK(text_push_back_w(text, 0x110000u) == ERR_INVALID);
    CHECK(text_push_front_w(text, 0xD800u) == ERR_INVALID);
    CHECK(text_set_w(text, 0, 0x110000u) == ERR_INVALID);
    CHECK(text_insert_w(text, 1, 0xDFFFu) == ERR_INVALID);
    CHECK(text_equals_array(text, base, 2));

    text_free(g_env, text);
}

static void test_handle_codec(void)
{
    SECTION("handle codec");
    TextHandle cp_handle = 0;
    TextHandle lit_handle = 0;
    uint32_t cp = 0;

    CHECK(text_handle_from_codepoint(0x1F642u, &cp_handle) == ERR_OK);
    CHECK(text_handle_kind(cp_handle) == TEXT_HANDLE_CODEPOINT);
    CHECK(text_handle_payload(cp_handle) == 0x1F642u);
    CHECK(text_handle_is_codepoint(cp_handle) == 1);
    CHECK(text_handle_to_codepoint(cp_handle, &cp) == ERR_OK && cp == 0x1F642u);

    lit_handle = text_handle_make(TEXT_HANDLE_LITERAL, 77u);
    CHECK(text_handle_kind(lit_handle) == TEXT_HANDLE_LITERAL);
    CHECK(text_handle_payload(lit_handle) == 77u);
    CHECK(text_handle_is_codepoint(lit_handle) == 0);
    CHECK(text_handle_to_codepoint(lit_handle, &cp) == ERR_INVALID);

    CHECK(text_handle_from_codepoint(0x110000u, &cp_handle) == ERR_INVALID);
    CHECK(text_handle_from_codepoint(0xD800u, &cp_handle) == ERR_INVALID);
    CHECK(text_handle_to_codepoint(text_handle_make(TEXT_HANDLE_CODEPOINT, 0xD800u), &cp) ==
          ERR_INVALID);
}

static void test_handle_apis_and_strict_codepoint_wrappers(void)
{
    SECTION("handle apis + strict codepoint wrappers");
    Text *text = text_new(g_env);
    TextHandle cp_handle = 0;
    TextHandle lit_handle = text_handle_make(TEXT_HANDLE_LITERAL, 21u);
    TextHandle tree_handle = text_handle_make(TEXT_HANDLE_TREE, 42u);
    TextHandle out_h = 0;
    uint32_t cp = 0;
    size_t need = 0;

    CHECK(text != NULL);
    CHECK(text_handle_from_codepoint(0x41u, &cp_handle) == ERR_OK);
    CHECK(text_push_back_handle_w(text, cp_handle) == ERR_OK);
    CHECK(text_push_back_handle_w(text, lit_handle) == ERR_OK);
    CHECK(text_push_back_handle_w(text, tree_handle) == ERR_OK);
    CHECK(text_push_back_handle_w(text, text_handle_make(TEXT_HANDLE_RESERVED, 1u)) == ERR_INVALID);
    CHECK(text_length(text) == 3);

    CHECK(text_get_handle(text, 1, &out_h) == ERR_OK && out_h == lit_handle);
    CHECK(text_get(text, 0, &cp) == ERR_OK && cp == 0x41u);
    CHECK(text_get(text, 1, &cp) == ERR_INVALID);
    CHECK(text_utf8_length(text, &need) == ERR_INVALID);

    CHECK(text_pop_front_w(text, &cp) == ERR_OK && cp == 0x41u);
    CHECK(text_pop_front_w(text, &cp) == ERR_INVALID);
    CHECK(text_length(text) == 2);
    CHECK(text_pop_front_handle_w(text, &out_h) == ERR_OK && out_h == lit_handle);
    CHECK(text_pop_front_handle_w(text, &out_h) == ERR_OK && out_h == tree_handle);
    CHECK(text_pop_front_handle_w(text, &out_h) == ERR_EMPTY);

    CHECK(text_insert_handle_w(text, 0, lit_handle) == ERR_OK);
    CHECK(text_delete_w(text, 0, &cp) == ERR_INVALID);
    CHECK(text_length(text) == 1);
    CHECK(text_delete_handle_w(text, 0, &out_h) == ERR_OK && out_h == lit_handle);
    CHECK(text_length(text) == 0);

    text_free(g_env, text);
}

static void test_resolved_codepoint_view(void)
{
    SECTION("resolved codepoint view");
    Text *text = text_new(g_env);
    TextHandle h_a = 0;
    TextHandle h_d = 0;
    TextHandle h_literal = text_handle_make(TEXT_HANDLE_LITERAL, 7u);
    TextHandle h_tree = text_handle_make(TEXT_HANDLE_TREE, 9u);
    const uint32_t literal_cps[] = {0x42u, 0x43u};
    const uint32_t tree_cps[] = {0x1F642u};
    const ResolveEntry entries[] = {
        {h_literal, literal_cps, 2u, ERR_OK},
        {h_tree, tree_cps, 1u, ERR_OK},
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
    CHECK(text_handle_from_codepoint(0x41u, &h_a) == ERR_OK);
    CHECK(text_handle_from_codepoint(0x44u, &h_d) == ERR_OK);
    CHECK(text_push_back_handle_w(text, h_a) == ERR_OK);
    CHECK(text_push_back_handle_w(text, h_literal) == ERR_OK);
    CHECK(text_push_back_handle_w(text, h_tree) == ERR_OK);
    CHECK(text_push_back_handle_w(text, h_d) == ERR_OK);
    CHECK(text_length(text) == 4u);

    CHECK(text_codepoint_length_resolved(text, test_expand_handle, &resolver, &cp_len) == ERR_OK);
    CHECK(cp_len == 5u);

    for (size_t i = 0; i < 5u; i++)
        CHECK(text_get_codepoint_resolved(text, i, test_expand_handle, &resolver, &cp) == ERR_OK &&
              cp == expect_cps[i]);
    CHECK(text_get_codepoint_resolved(text, 5u, test_expand_handle, &resolver, &cp) == ERR_RANGE);

    CHECK(text_utf8_length(text, &utf8_need) == ERR_INVALID);
    CHECK(text_utf8_length_resolved(text, test_expand_handle, &resolver, &utf8_need) == ERR_OK);
    CHECK(utf8_need == sizeof(expect_utf8));
    CHECK(text_to_utf8_resolved(text, test_expand_handle, &resolver, utf8_out, sizeof(utf8_out),
                                &utf8_wrote) == ERR_OK);
    CHECK(utf8_wrote == sizeof(expect_utf8));
    CHECK(memcmp(utf8_out, expect_utf8, sizeof(expect_utf8)) == 0);
    CHECK(text_to_utf8_resolved(text, test_expand_handle, &resolver, utf8_out, 7u, &utf8_wrote) ==
          ERR_RANGE);
    CHECK(utf8_wrote == sizeof(expect_utf8));
    CHECK(text_to_utf8_resolved(text, test_expand_handle, &resolver, NULL, 0u, &utf8_wrote) ==
          ERR_RANGE);
    CHECK(utf8_wrote == sizeof(expect_utf8));

    CHECK(resolver.calls > 0u);
    text_free(g_env, text);
}

static void test_resolver_error_paths(void)
{
    SECTION("resolved error paths");
    Text *text = text_new(g_env);
    TextHandle h_literal = text_handle_make(TEXT_HANDLE_LITERAL, 99u);
    const uint32_t bad_cps[] = {0xD800u};
    const ResolveEntry bad_entries[] = {
        {h_literal, bad_cps, 1u, ERR_OK},
    };
    const ResolveEntry oom_entries[] = {
        {h_literal, NULL, 0u, ERR_OOM},
    };
    ResolveCtx no_entries = {NULL, 0u, 0u};
    ResolveCtx bad_resolver = {bad_entries, 1u, 0u};
    ResolveCtx oom_resolver = {oom_entries, 1u, 0u};
    uint32_t cp = 0;
    size_t len = 0;

    CHECK(text != NULL);
    CHECK(text_push_back_handle_w(text, h_literal) == ERR_OK);
    CHECK(text_codepoint_length_resolved(text, NULL, NULL, &len) == ERR_INVALID);
    CHECK(text_codepoint_length_resolved(text, test_expand_handle, &no_entries, &len) ==
          ERR_INVALID);
    CHECK(text_codepoint_length_resolved(text, test_expand_handle, &bad_resolver, &len) ==
          ERR_INVALID);
    CHECK(text_codepoint_length_resolved(text, test_expand_handle, &oom_resolver, &len) == ERR_OOM);
    CHECK(text_get_codepoint_resolved(text, 0u, test_expand_handle, &oom_resolver, &cp) == ERR_OOM);
    CHECK(text_utf8_length_resolved(text, test_expand_handle, &oom_resolver, &len) == ERR_OOM);
    CHECK(text_to_utf8_resolved(text, test_expand_handle, &oom_resolver, NULL, 0u, &len) ==
          ERR_OOM);

    text_free(g_env, text);
}

static void test_runtime_resolver_adapter(void)
{
    SECTION("runtime resolver adapter");
    Text *root = text_new(g_env);
    Text *tree_outer = text_new(g_env);
    Text *tree_inner = text_new(g_env);
    TextHandle h_cp_d = 0;
    TextHandle h_cp_e = 0;
    const uint8_t lit_a[] = {'A'};
    const uint8_t lit_bc[] = {'B', 'C'};
    const uint8_t lit_smile[] = {0xF0u, 0x9Fu, 0x99u, 0x82u};
    const RuntimeLiteralEntry literals[] = {
        {1u, lit_a, sizeof(lit_a), ERR_OK},
        {2u, lit_bc, sizeof(lit_bc), ERR_OK},
        {3u, lit_smile, sizeof(lit_smile), ERR_OK},
    };
    RuntimeTreeEntry trees[] = {
        {10u, tree_outer, ERR_OK},
        {11u, tree_inner, ERR_OK},
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
    CHECK(text_handle_from_codepoint(0x44u, &h_cp_d) == ERR_OK);
    CHECK(text_handle_from_codepoint(0x45u, &h_cp_e) == ERR_OK);

    CHECK(text_push_back_handle_w(tree_inner, text_handle_make(TEXT_HANDLE_LITERAL, 2u)) == ERR_OK);
    CHECK(text_push_back_handle_w(tree_inner, h_cp_d) == ERR_OK);
    CHECK(text_push_back_handle_w(tree_outer, text_handle_make(TEXT_HANDLE_TREE, 11u)) == ERR_OK);
    CHECK(text_push_back_handle_w(tree_outer, text_handle_make(TEXT_HANDLE_LITERAL, 3u)) == ERR_OK);
    CHECK(text_push_back_handle_w(root, text_handle_make(TEXT_HANDLE_LITERAL, 1u)) == ERR_OK);
    CHECK(text_push_back_handle_w(root, text_handle_make(TEXT_HANDLE_TREE, 10u)) == ERR_OK);
    CHECK(text_push_back_handle_w(root, h_cp_e) == ERR_OK);

    CHECK(text_utf8_length(root, &utf8_len) == ERR_INVALID);
    CHECK(text_codepoint_length_resolved(root, text_expand_runtime_handle, &resolver, &cp_len) ==
          ERR_OK);
    CHECK(cp_len == 6u);
    for (size_t i = 0; i < 6u; i++)
        CHECK(text_get_codepoint_resolved(root, i, text_expand_runtime_handle, &resolver, &cp) ==
                  ERR_OK &&
              cp == expect_cps[i]);
    CHECK(text_get_codepoint_resolved(root, 6u, text_expand_runtime_handle, &resolver, &cp) ==
          ERR_RANGE);

    CHECK(text_utf8_length_resolved(root, text_expand_runtime_handle, &resolver, &utf8_len) ==
          ERR_OK);
    CHECK(utf8_len == sizeof(expect_utf8));
    CHECK(text_to_utf8_resolved(root, text_expand_runtime_handle, &resolver, utf8_out,
                                sizeof(utf8_out), &utf8_wrote) == ERR_OK);
    CHECK(utf8_wrote == sizeof(expect_utf8));
    CHECK(memcmp(utf8_out, expect_utf8, sizeof(expect_utf8)) == 0);
    CHECK(text_to_utf8_resolved(root, text_expand_runtime_handle, &resolver, utf8_out, 8u,
                                &utf8_wrote) == ERR_RANGE);
    CHECK(utf8_wrote == sizeof(expect_utf8));

    CHECK(resolver_ctx.literal_calls > 0u);
    CHECK(resolver_ctx.tree_calls > 0u);

    text_free(g_env, root);
    text_free(g_env, tree_outer);
    text_free(g_env, tree_inner);
}

static void test_runtime_resolver_guards_and_errors(void)
{
    SECTION("runtime resolver guards/errors");
    Text *root_cycle = text_new(g_env);
    Text *tree_a = text_new(g_env);
    Text *tree_b = text_new(g_env);
    Text *root_depth = text_new(g_env);
    Text *tree_c = text_new(g_env);
    Text *tree_d = text_new(g_env);
    Text *root_visits = text_new(g_env);
    Text *tree_e = text_new(g_env);
    Text *tree_f = text_new(g_env);
    Text *tree_g = text_new(g_env);
    Text *root_literal = text_new(g_env);
    const uint8_t bad_utf8[] = {0xC0u, 0xAFu};
    const RuntimeLiteralEntry literals_bad[] = {
        {5u, bad_utf8, sizeof(bad_utf8), ERR_OK},
    };
    RuntimeTreeEntry cycle_trees[] = {
        {20u, tree_a, ERR_OK},
        {21u, tree_b, ERR_OK},
    };
    RuntimeTreeEntry depth_trees[] = {
        {30u, tree_c, ERR_OK},
        {31u, tree_d, ERR_OK},
    };
    RuntimeTreeEntry visits_trees[] = {
        {40u, tree_e, ERR_OK},
        {41u, tree_f, ERR_OK},
        {42u, tree_g, ERR_OK},
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

    CHECK(text_push_back_handle_w(tree_a, text_handle_make(TEXT_HANDLE_TREE, 21u)) == ERR_OK);
    CHECK(text_push_back_handle_w(tree_b, text_handle_make(TEXT_HANDLE_TREE, 20u)) == ERR_OK);
    CHECK(text_push_back_handle_w(root_cycle, text_handle_make(TEXT_HANDLE_TREE, 20u)) == ERR_OK);
    CHECK(text_codepoint_length_resolved(root_cycle, text_expand_runtime_handle, &cycle_resolver,
                                         &len) == ERR_INVALID);

    CHECK(text_push_back_handle_w(tree_d, text_handle_make(TEXT_HANDLE_CODEPOINT, 0x51u)) ==
          ERR_OK);
    CHECK(text_push_back_handle_w(tree_c, text_handle_make(TEXT_HANDLE_TREE, 31u)) == ERR_OK);
    CHECK(text_push_back_handle_w(root_depth, text_handle_make(TEXT_HANDLE_TREE, 30u)) == ERR_OK);
    CHECK(text_codepoint_length_resolved(root_depth, text_expand_runtime_handle,
                                         &depth_resolver_bad, &len) == ERR_INVALID);
    CHECK(text_codepoint_length_resolved(root_depth, text_expand_runtime_handle, &depth_resolver_ok,
                                         &len) == ERR_OK);
    CHECK(len == 1u);

    CHECK(text_push_back_handle_w(tree_f, text_handle_make(TEXT_HANDLE_CODEPOINT, 0x61u)) ==
          ERR_OK);
    CHECK(text_push_back_handle_w(tree_g, text_handle_make(TEXT_HANDLE_CODEPOINT, 0x62u)) ==
          ERR_OK);
    CHECK(text_push_back_handle_w(tree_e, text_handle_make(TEXT_HANDLE_TREE, 41u)) == ERR_OK);
    CHECK(text_push_back_handle_w(tree_e, text_handle_make(TEXT_HANDLE_TREE, 42u)) == ERR_OK);
    CHECK(text_push_back_handle_w(root_visits, text_handle_make(TEXT_HANDLE_TREE, 40u)) == ERR_OK);
    CHECK(text_codepoint_length_resolved(root_visits, text_expand_runtime_handle, &visits_resolver,
                                         &len) == ERR_INVALID);
    CHECK(text_codepoint_length_resolved(root_visits, text_expand_runtime_handle,
                                         &visits_resolver_ok, &len) == ERR_OK);
    CHECK(len == 2u);

    CHECK(text_push_back_handle_w(root_literal, text_handle_make(TEXT_HANDLE_LITERAL, 5u)) ==
          ERR_OK);
    CHECK(text_codepoint_length_resolved(root_literal, text_expand_runtime_handle,
                                         &bad_lit_resolver, &len) == ERR_INVALID);
    CHECK(text_codepoint_length_resolved(root_literal, text_expand_runtime_handle, &missing_lit_cb,
                                         &len) == ERR_INVALID);
    CHECK(text_codepoint_length_resolved(root_cycle, text_expand_runtime_handle, &missing_tree_cb,
                                         &len) == ERR_INVALID);

    text_free(g_env, root_cycle);
    text_free(g_env, tree_a);
    text_free(g_env, tree_b);
    text_free(g_env, root_depth);
    text_free(g_env, tree_c);
    text_free(g_env, tree_d);
    text_free(g_env, root_visits);
    text_free(g_env, tree_e);
    text_free(g_env, tree_f);
    text_free(g_env, tree_g);
    text_free(g_env, root_literal);
}

static void test_abort_cleanup(void)
{
    SECTION("abort-during-construction cleanup");
    uint32_t pages_before = sap_arena_active_pages(g_arena);
    for (int i = 0; i < 200; i++)
    {
        Text *t = text_new(g_env);
        CHECK(t != NULL);
        text_free(g_env, t);
    }
    uint32_t pages_after = sap_arena_active_pages(g_arena);
    /* Arena pages should not grow unboundedly from create/free cycles.
     * Allow a small margin for fragmentation. */
    CHECK(pages_after <= pages_before + 2);
}

static void test_split_at_boundary_cases(void)
{
    SECTION("split at boundary cases");
    uint32_t vals[] = {'A', 'B', 'C', 'D', 'E'};
    Text *text = NULL;
    Text *l = NULL;
    Text *r = NULL;

    /* Split at 0 — left is empty, right has all elements */
    text = text_from_array(vals, 5);
    CHECK(text != NULL);
    CHECK(text_split_at_w(text, 0, &l, &r) == ERR_OK);
    CHECK(text_length(l) == 0);
    CHECK(text_length(r) == 5);
    {
        uint32_t cp = 0;
        CHECK(text_get(r, 0, &cp) == ERR_OK);
        CHECK(cp == 'A');
        CHECK(text_get(r, 4, &cp) == ERR_OK);
        CHECK(cp == 'E');
    }
    text_free(g_env, l);
    text_free(g_env, r);
    text_free(g_env, text);

    /* Split at length — left has all elements, right is empty */
    text = text_from_array(vals, 5);
    CHECK(text != NULL);
    CHECK(text_split_at_w(text, 5, &l, &r) == ERR_OK);
    CHECK(text_length(l) == 5);
    CHECK(text_length(r) == 0);
    {
        uint32_t cp = 0;
        CHECK(text_get(l, 0, &cp) == ERR_OK);
        CHECK(cp == 'A');
        CHECK(text_get(l, 4, &cp) == ERR_OK);
        CHECK(cp == 'E');
    }
    text_free(g_env, l);
    text_free(g_env, r);
    text_free(g_env, text);

    /* Split single-element text at 0 and at 1 */
    text = text_from_array(vals, 1);
    CHECK(text != NULL);
    CHECK(text_split_at_w(text, 0, &l, &r) == ERR_OK);
    CHECK(text_length(l) == 0);
    CHECK(text_length(r) == 1);
    text_free(g_env, l);
    text_free(g_env, r);
    text_free(g_env, text);

    text = text_from_array(vals, 1);
    CHECK(text != NULL);
    CHECK(text_split_at_w(text, 1, &l, &r) == ERR_OK);
    CHECK(text_length(l) == 1);
    CHECK(text_length(r) == 0);
    text_free(g_env, l);
    text_free(g_env, r);
    text_free(g_env, text);
}

static void test_cross_phase_integration(void)
{
    SECTION("cross-phase integration");

    /*
     * Full pipeline:
     * 1. Create a literal table, bulk-load a UTF-8 string (Phase 1 + 2)
     * 2. Register the resulting Text into the tree registry (Phase 3)
     * 3. Create a new Text containing that TREE handle
     * 4. Split the outer text to exercise mixed content
     * 5. Expand a LITERAL handle at a boundary (Phase 2)
     * 6. Resolve everything to UTF-8 via text_to_utf8_full
     */

    TextLiteralTable *lt = text_literal_table_new(g_env);
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(lt != NULL);
    CHECK(reg != NULL);

    /* Step 1: Bulk load "HELLO" into a Text via the literal table */
    Text *bulk_text = text_new(g_env);
    CHECK(bulk_text != NULL);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(txn != NULL);
        CHECK(text_from_utf8_bulk(txn, bulk_text, (const uint8_t *)"HELLO", 5, lt) == ERR_OK);
        sap_txn_commit(txn);
    }
    /* bulk_text has 1 LITERAL handle */
    CHECK(text_length(bulk_text) == 1);

    /* Step 2: Register bulk_text as a tree */
    uint32_t tree_id = 0;
    CHECK(text_tree_registry_register(reg, bulk_text, &tree_id) == ERR_OK);
    text_free(g_env, bulk_text);

    /* Step 3: Build outer text: "[" + TREE(tree_id) + "]" */
    Text *outer = text_new(g_env);
    CHECK(outer != NULL);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(txn != NULL);
        CHECK(text_push_back(txn, outer, '[') == ERR_OK);
        TextHandle tree_h = text_handle_make(TEXT_HANDLE_TREE, tree_id);
        CHECK(text_push_back_handle(txn, outer, tree_h) == ERR_OK);
        CHECK(text_push_back(txn, outer, ']') == ERR_OK);
        sap_txn_commit(txn);
    }
    /* outer has 3 handles: '[', TREE, ']' */
    CHECK(text_length(outer) == 3);

    /* Step 6 (early check): Resolve entire outer to UTF-8 */
    {
        uint8_t *utf8 = NULL;
        size_t utf8_len = 0;
        CHECK(text_to_utf8_full(outer, lt, reg, &utf8, &utf8_len) == ERR_OK);
        CHECK(utf8_len == 7); /* "[HELLO]" */
        CHECK(memcmp(utf8, "[HELLO]", 7) == 0);
        free(utf8);
    }

    /* Step 4: Split outer at index 2 (between TREE and ']') */
    Text *left = NULL;
    Text *right = NULL;
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(txn != NULL);
        CHECK(text_split_at(txn, outer, 2, &left, &right) == ERR_OK);
        sap_txn_commit(txn);
    }
    /* left has "[", TREE; right has "]" */
    CHECK(text_length(left) == 2);
    CHECK(text_length(right) == 1);

    /* Verify left resolves to "[HELLO" */
    {
        uint8_t *utf8 = NULL;
        size_t utf8_len = 0;
        CHECK(text_to_utf8_full(left, lt, reg, &utf8, &utf8_len) == ERR_OK);
        CHECK(utf8_len == 6);
        CHECK(memcmp(utf8, "[HELLO", 6) == 0);
        free(utf8);
    }

    /* Verify right resolves to "]" */
    {
        uint8_t *utf8 = NULL;
        size_t utf8_len = 0;
        CHECK(text_to_utf8_full(right, lt, reg, &utf8, &utf8_len) == ERR_OK);
        CHECK(utf8_len == 1);
        CHECK(utf8[0] == ']');
        free(utf8);
    }

    /* Step 5: Now create a fresh text with a LITERAL handle and expand it.
     * Build "AB" as a bulk literal, then expand. */
    Text *edit_text = text_new(g_env);
    CHECK(edit_text != NULL);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(txn != NULL);
        CHECK(text_from_utf8_bulk(txn, edit_text, (const uint8_t *)"AB", 2, lt) == ERR_OK);
        sap_txn_commit(txn);
    }
    CHECK(text_length(edit_text) == 1); /* single LITERAL handle */

    /* Expand the LITERAL at index 0 */
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(txn != NULL);
        CHECK(text_expand_handle_at(txn, edit_text, 0,
                                    text_literal_table_resolve_fn, lt) == ERR_OK);
        sap_txn_commit(txn);
    }
    CHECK(text_length(edit_text) == 2); /* 'A', 'B' as CODEPOINTs */

    /* Verify expanded content */
    {
        uint32_t cp = 0;
        CHECK(text_get(edit_text, 0, &cp) == ERR_OK);
        CHECK(cp == 'A');
        CHECK(text_get(edit_text, 1, &cp) == ERR_OK);
        CHECK(cp == 'B');
    }

    /* Encode expanded back to UTF-8 */
    {
        uint8_t *utf8 = NULL;
        size_t utf8_len = 0;
        CHECK(text_to_utf8_full(edit_text, lt, reg, &utf8, &utf8_len) == ERR_OK);
        CHECK(utf8_len == 2);
        CHECK(memcmp(utf8, "AB", 2) == 0);
        free(utf8);
    }

    text_free(g_env, edit_text);
    text_free(g_env, left);
    text_free(g_env, right);
    text_free(g_env, outer);
    text_tree_registry_free(reg);
    text_literal_table_free(lt);
}

static void print_summary(void) { printf("Passed: %d, Failed: %d\n", g_pass, g_fail); }

int main(void)
{
    setup_env();
    printf("=== text unit tests ===\n");

    test_empty();
    test_push_pop_get();
    test_insert_set_delete();
    test_concat_split();
    test_clone_copy_on_write();
    test_clone_structural_detach();
    test_split_range_contract();
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
    test_abort_cleanup();
    test_split_at_boundary_cases();
    test_cross_phase_integration();

    print_summary();
    teardown_env();
    return g_fail ? 1 : 0;
}
