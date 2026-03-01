/*
 * test_text_tree_registry.c - unit tests for the tree registry and convenience API
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text_tree_registry.h"
#include "sapling/text_literal.h"
#include "sapling/text.h"
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

/* Helper: create a Text from a C string */
static Text *text_from_cstr(const char *s)
{
    Text *t = text_new(g_env);
    if (!t) return NULL;
    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    if (!txn) { text_free(g_env, t); return NULL; }
    int rc = text_from_utf8(txn, t, (const uint8_t *)s, strlen(s));
    if (rc != ERR_OK) { sap_txn_abort(txn); text_free(g_env, t); return NULL; }
    sap_txn_commit(txn);
    return t;
}

/* Helper: convert Text to a malloc'd C string */
static char *text_to_cstr(const Text *t)
{
    size_t utf8_len = 0;
    if (text_utf8_length(t, &utf8_len) != ERR_OK) return NULL;
    char *buf = (char *)malloc(utf8_len + 1);
    if (!buf) return NULL;
    size_t written = 0;
    if (text_to_utf8(t, (uint8_t *)buf, utf8_len, &written) != ERR_OK)
    {
        free(buf);
        return NULL;
    }
    buf[written] = '\0';
    return buf;
}

/* ================================================================== */
/* Tests                                                               */
/* ================================================================== */

static void test_register_and_get(void)
{
    SECTION("register and get");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    Text *t = text_from_cstr("hello tree");
    CHECK(t != NULL);

    uint32_t id = 0;
    CHECK(text_tree_registry_register(reg, t, &id) == ERR_OK);
    CHECK(id == 0);
    CHECK(text_tree_registry_count(reg) == 1);

    const Text *got = NULL;
    CHECK(text_tree_registry_get(reg, id, &got) == ERR_OK);
    CHECK(got != NULL);

    /* Verify content matches */
    char *s = text_to_cstr(got);
    CHECK(s != NULL);
    CHECK(strcmp(s, "hello tree") == 0);
    free(s);

    /* Original text still valid after registration */
    char *orig = text_to_cstr(t);
    CHECK(orig != NULL);
    CHECK(strcmp(orig, "hello tree") == 0);
    free(orig);

    text_free(g_env, t);
    text_tree_registry_free(reg);
}

static void test_multiple_registrations(void)
{
    SECTION("multiple registrations");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    Text *t1 = text_from_cstr("alpha");
    Text *t2 = text_from_cstr("beta");
    Text *t3 = text_from_cstr("gamma");
    CHECK(t1 != NULL);
    CHECK(t2 != NULL);
    CHECK(t3 != NULL);

    uint32_t id1 = 0, id2 = 0, id3 = 0;
    CHECK(text_tree_registry_register(reg, t1, &id1) == ERR_OK);
    CHECK(text_tree_registry_register(reg, t2, &id2) == ERR_OK);
    CHECK(text_tree_registry_register(reg, t3, &id3) == ERR_OK);

    CHECK(id1 == 0);
    CHECK(id2 == 1);
    CHECK(id3 == 2);
    CHECK(text_tree_registry_count(reg) == 3);

    /* Verify each */
    const Text *got = NULL;
    CHECK(text_tree_registry_get(reg, id1, &got) == ERR_OK);
    char *s = text_to_cstr(got);
    CHECK(strcmp(s, "alpha") == 0);
    free(s);

    CHECK(text_tree_registry_get(reg, id2, &got) == ERR_OK);
    s = text_to_cstr(got);
    CHECK(strcmp(s, "beta") == 0);
    free(s);

    CHECK(text_tree_registry_get(reg, id3, &got) == ERR_OK);
    s = text_to_cstr(got);
    CHECK(strcmp(s, "gamma") == 0);
    free(s);

    text_free(g_env, t1);
    text_free(g_env, t2);
    text_free(g_env, t3);
    text_tree_registry_free(reg);
}

static void test_retain_release(void)
{
    SECTION("retain and release");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    Text *t = text_from_cstr("refcounted");
    CHECK(t != NULL);

    uint32_t id = 0;
    CHECK(text_tree_registry_register(reg, t, &id) == ERR_OK);
    text_free(g_env, t); /* original freed, registry still holds clone */

    /* Get still works (refs=1) */
    const Text *got = NULL;
    CHECK(text_tree_registry_get(reg, id, &got) == ERR_OK);
    CHECK(got != NULL);

    /* Retain bumps to refs=2 */
    CHECK(text_tree_registry_retain(reg, id) == ERR_OK);

    /* First release: refs=1 */
    CHECK(text_tree_registry_release(reg, id) == ERR_OK);
    CHECK(text_tree_registry_get(reg, id, &got) == ERR_OK);

    /* Second release: refs=0, entry freed */
    CHECK(text_tree_registry_release(reg, id) == ERR_OK);

    /* Get on freed entry returns error */
    CHECK(text_tree_registry_get(reg, id, &got) == ERR_INVALID);

    /* Retain on freed entry returns error */
    CHECK(text_tree_registry_retain(reg, id) == ERR_INVALID);

    /* Release on freed entry returns error (underflow guard) */
    CHECK(text_tree_registry_release(reg, id) == ERR_INVALID);

    text_tree_registry_free(reg);
}

static void test_resolver_fn(void)
{
    SECTION("resolver function");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    Text *t = text_from_cstr("resolved tree");
    uint32_t id = 0;
    CHECK(text_tree_registry_register(reg, t, &id) == ERR_OK);
    text_free(g_env, t);

    /* Use resolver adapter */
    const Text *got = NULL;
    CHECK(text_tree_registry_resolve_fn(id, &got, reg) == ERR_OK);
    CHECK(got != NULL);

    char *s = text_to_cstr(got);
    CHECK(strcmp(s, "resolved tree") == 0);
    free(s);

    /* Out of range */
    CHECK(text_tree_registry_resolve_fn(999, &got, reg) == ERR_RANGE);

    text_tree_registry_free(reg);
}

static void test_tree_handle_resolved(void)
{
    SECTION("TREE handle resolution via text_to_utf8_resolved");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    /* Register a source text */
    Text *src = text_from_cstr("inner content");
    uint32_t tree_id = 0;
    CHECK(text_tree_registry_register(reg, src, &tree_id) == ERR_OK);
    text_free(g_env, src);

    /* Create a new text with a single TREE handle */
    Text *outer = text_new(g_env);
    CHECK(outer != NULL);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(txn != NULL);
        TextHandle h = text_handle_make(TEXT_HANDLE_TREE, tree_id);
        CHECK(text_push_back_handle(txn, outer, h) == ERR_OK);
        sap_txn_commit(txn);
    }

    /* Resolve via the runtime resolver */
    TextRuntimeResolver resolver;
    resolver.resolve_literal_utf8_fn = NULL;
    resolver.resolve_tree_text_fn = text_tree_registry_resolve_fn;
    resolver.ctx = reg;
    resolver.max_tree_depth = 0;
    resolver.max_tree_visits = 0;

    size_t utf8_len = 0;
    CHECK(text_utf8_length_resolved(outer, text_expand_runtime_handle,
                                    &resolver, &utf8_len) == ERR_OK);
    CHECK(utf8_len == strlen("inner content"));

    uint8_t buf[64];
    size_t written = 0;
    CHECK(text_to_utf8_resolved(outer, text_expand_runtime_handle,
                                &resolver, buf, sizeof(buf), &written) == ERR_OK);
    CHECK(written == strlen("inner content"));
    CHECK(memcmp(buf, "inner content", written) == 0);

    text_free(g_env, outer);
    text_tree_registry_free(reg);
}

static void test_nested_trees(void)
{
    SECTION("nested trees");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    /* Tree B: "world" */
    Text *tb = text_from_cstr("world");
    uint32_t id_b = 0;
    CHECK(text_tree_registry_register(reg, tb, &id_b) == ERR_OK);
    text_free(g_env, tb);

    /* Tree A: "hello " + TREE(B) */
    Text *ta = text_new(g_env);
    CHECK(ta != NULL);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(txn != NULL);
        CHECK(text_from_utf8(txn, ta, (const uint8_t *)"hello ", 6) == ERR_OK);
        TextHandle tree_h = text_handle_make(TEXT_HANDLE_TREE, id_b);
        CHECK(text_push_back_handle(txn, ta, tree_h) == ERR_OK);
        sap_txn_commit(txn);
    }

    /* Resolve nested structure */
    TextRuntimeResolver resolver;
    resolver.resolve_literal_utf8_fn = NULL;
    resolver.resolve_tree_text_fn = text_tree_registry_resolve_fn;
    resolver.ctx = reg;
    resolver.max_tree_depth = 0;
    resolver.max_tree_visits = 0;

    size_t utf8_len = 0;
    CHECK(text_utf8_length_resolved(ta, text_expand_runtime_handle,
                                    &resolver, &utf8_len) == ERR_OK);
    CHECK(utf8_len == strlen("hello world"));

    uint8_t buf[64];
    size_t written = 0;
    CHECK(text_to_utf8_resolved(ta, text_expand_runtime_handle,
                                &resolver, buf, sizeof(buf), &written) == ERR_OK);
    CHECK(written == strlen("hello world"));
    CHECK(memcmp(buf, "hello world", written) == 0);

    text_free(g_env, ta);
    text_tree_registry_free(reg);
}

static void test_cycle_detection(void)
{
    SECTION("cycle detection");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    /*
     * Create a cycle: A contains TREE(B), B contains TREE(A).
     * We have to register A first (with some placeholder content),
     * then create B referencing A, register B, then build A referencing B.
     *
     * Actually, since we can't modify a registered entry, we build:
     * - Register a text containing TREE handle pointing to id=1 (B's future ID)
     * - Register a text containing TREE handle pointing to id=0 (A's ID)
     */

    /* Tree A: contains TREE handle pointing to id=1 */
    Text *ta = text_new(g_env);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        TextHandle h = text_handle_make(TEXT_HANDLE_TREE, 1); /* future B */
        text_push_back_handle(txn, ta, h);
        sap_txn_commit(txn);
    }
    uint32_t id_a = 0;
    CHECK(text_tree_registry_register(reg, ta, &id_a) == ERR_OK);
    CHECK(id_a == 0);
    text_free(g_env, ta);

    /* Tree B: contains TREE handle pointing to id=0 (A) */
    Text *tb = text_new(g_env);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        TextHandle h = text_handle_make(TEXT_HANDLE_TREE, 0); /* A */
        text_push_back_handle(txn, tb, h);
        sap_txn_commit(txn);
    }
    uint32_t id_b = 0;
    CHECK(text_tree_registry_register(reg, tb, &id_b) == ERR_OK);
    CHECK(id_b == 1);
    text_free(g_env, tb);

    /* Create outer text with TREE(A) and try to resolve */
    Text *outer = text_new(g_env);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        TextHandle h = text_handle_make(TEXT_HANDLE_TREE, id_a);
        text_push_back_handle(txn, outer, h);
        sap_txn_commit(txn);
    }

    TextRuntimeResolver resolver;
    resolver.resolve_literal_utf8_fn = NULL;
    resolver.resolve_tree_text_fn = text_tree_registry_resolve_fn;
    resolver.ctx = reg;
    resolver.max_tree_depth = 8; /* small depth to catch cycle quickly */
    resolver.max_tree_visits = 64;

    /* Should fail due to depth/visit guard */
    size_t utf8_len = 0;
    int rc = text_utf8_length_resolved(outer, text_expand_runtime_handle,
                                       &resolver, &utf8_len);
    CHECK(rc != ERR_OK);

    text_free(g_env, outer);
    text_tree_registry_free(reg);
}

static void test_invalid_args(void)
{
    SECTION("invalid arguments");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    /* NULL args */
    CHECK(text_tree_registry_register(NULL, NULL, NULL) == ERR_INVALID);
    CHECK(text_tree_registry_get(NULL, 0, NULL) == ERR_INVALID);
    CHECK(text_tree_registry_retain(NULL, 0) == ERR_INVALID);
    CHECK(text_tree_registry_release(NULL, 0) == ERR_INVALID);
    CHECK(text_tree_registry_count(NULL) == 0);

    /* Out of range */
    const Text *got = NULL;
    CHECK(text_tree_registry_get(reg, 0, &got) == ERR_RANGE);
    CHECK(text_tree_registry_get(reg, 999, &got) == ERR_RANGE);
    CHECK(text_tree_registry_retain(reg, 0) == ERR_RANGE);
    CHECK(text_tree_registry_release(reg, 0) == ERR_RANGE);

    text_tree_registry_free(reg);
}

static void test_to_utf8_full_codepoints_only(void)
{
    SECTION("text_to_utf8_full with codepoints only");
    Text *t = text_from_cstr("simple text");
    CHECK(t != NULL);

    uint8_t *utf8 = NULL;
    size_t utf8_len = 0;
    CHECK(text_to_utf8_full(t, NULL, NULL, &utf8, &utf8_len) == ERR_OK);
    CHECK(utf8_len == strlen("simple text"));
    CHECK(memcmp(utf8, "simple text", utf8_len) == 0);
    CHECK(utf8[utf8_len] == '\0'); /* NUL terminated */
    free(utf8);

    text_free(g_env, t);
}

static void test_to_utf8_full_with_literal(void)
{
    SECTION("text_to_utf8_full with literal");
    TextLiteralTable *lt = text_literal_table_new(g_env);
    CHECK(lt != NULL);

    Text *t = text_new(g_env);
    CHECK(t != NULL);

    /* Bulk load */
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(text_from_utf8_bulk(txn, t, (const uint8_t *)"bulk loaded", 11, lt) == ERR_OK);
        sap_txn_commit(txn);
    }

    uint8_t *utf8 = NULL;
    size_t utf8_len = 0;
    CHECK(text_to_utf8_full(t, lt, NULL, &utf8, &utf8_len) == ERR_OK);
    CHECK(utf8_len == 11);
    CHECK(memcmp(utf8, "bulk loaded", 11) == 0);
    free(utf8);

    text_free(g_env, t);
    text_literal_table_free(lt);
}

static void test_to_utf8_full_with_tree(void)
{
    SECTION("text_to_utf8_full with tree");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    Text *src = text_from_cstr("tree data");
    uint32_t tree_id = 0;
    CHECK(text_tree_registry_register(reg, src, &tree_id) == ERR_OK);
    text_free(g_env, src);

    Text *outer = text_new(g_env);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        TextHandle h = text_handle_make(TEXT_HANDLE_TREE, tree_id);
        text_push_back_handle(txn, outer, h);
        sap_txn_commit(txn);
    }

    uint8_t *utf8 = NULL;
    size_t utf8_len = 0;
    CHECK(text_to_utf8_full(outer, NULL, reg, &utf8, &utf8_len) == ERR_OK);
    CHECK(utf8_len == strlen("tree data"));
    CHECK(memcmp(utf8, "tree data", utf8_len) == 0);
    free(utf8);

    text_free(g_env, outer);
    text_tree_registry_free(reg);
}

static void test_to_utf8_full_mixed(void)
{
    SECTION("text_to_utf8_full with literals and trees");
    TextLiteralTable *lt = text_literal_table_new(g_env);
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(lt != NULL);
    CHECK(reg != NULL);

    /* Register a literal via bulk loading */
    Text *lit_text = text_new(g_env);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(text_from_utf8_bulk(txn, lit_text, (const uint8_t *)"LIT", 3, lt) == ERR_OK);
        sap_txn_commit(txn);
    }

    /* Register it as a tree */
    uint32_t tree_id = 0;
    CHECK(text_tree_registry_register(reg, lit_text, &tree_id) == ERR_OK);
    text_free(g_env, lit_text);

    /* Create outer: codepoints + TREE handle */
    Text *outer = text_new(g_env);
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(text_from_utf8(txn, outer, (const uint8_t *)"[", 1) == ERR_OK);
        TextHandle tree_h = text_handle_make(TEXT_HANDLE_TREE, tree_id);
        CHECK(text_push_back_handle(txn, outer, tree_h) == ERR_OK);
        /* push ']' */
        TextHandle close_h;
        CHECK(text_handle_from_codepoint(']', &close_h) == ERR_OK);
        CHECK(text_push_back_handle(txn, outer, close_h) == ERR_OK);
        sap_txn_commit(txn);
    }

    /* Resolve: should produce "[LIT]" */
    uint8_t *utf8 = NULL;
    size_t utf8_len = 0;
    CHECK(text_to_utf8_full(outer, lt, reg, &utf8, &utf8_len) == ERR_OK);
    CHECK(utf8_len == 5);
    CHECK(memcmp(utf8, "[LIT]", 5) == 0);
    free(utf8);

    text_free(g_env, outer);
    text_tree_registry_free(reg);
    text_literal_table_free(lt);
}

static void test_to_utf8_full_invalid(void)
{
    SECTION("text_to_utf8_full invalid args");
    CHECK(text_to_utf8_full(NULL, NULL, NULL, NULL, NULL) == ERR_INVALID);

    Text *t = text_from_cstr("x");
    uint8_t *utf8 = NULL;
    size_t len = 0;
    CHECK(text_to_utf8_full(t, NULL, NULL, NULL, &len) == ERR_INVALID);
    CHECK(text_to_utf8_full(t, NULL, NULL, &utf8, NULL) == ERR_INVALID);
    text_free(g_env, t);
}

static void test_cow_sharing(void)
{
    SECTION("COW sharing across registration");
    TextTreeRegistry *reg = text_tree_registry_new(g_env);
    CHECK(reg != NULL);

    /* Create a text and register it — the registry holds a COW clone */
    Text *original = text_from_cstr("shared data");
    CHECK(original != NULL);

    uint32_t id = 0;
    CHECK(text_tree_registry_register(reg, original, &id) == ERR_OK);

    /* Modify the original — COW ensures registry's copy is unaffected */
    {
        SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
        CHECK(text_push_back(txn, original, '!') == ERR_OK);
        sap_txn_commit(txn);
    }

    /* Original should now be "shared data!" */
    char *orig_str = text_to_cstr(original);
    CHECK(strcmp(orig_str, "shared data!") == 0);
    free(orig_str);

    /* Registry's copy should still be "shared data" */
    const Text *got = NULL;
    CHECK(text_tree_registry_get(reg, id, &got) == ERR_OK);
    char *reg_str = text_to_cstr(got);
    CHECK(strcmp(reg_str, "shared data") == 0);
    free(reg_str);

    text_free(g_env, original);
    text_tree_registry_free(reg);
}

/* ================================================================== */
/* Main                                                                */
/* ================================================================== */

int main(void)
{
    printf("=== text tree registry unit tests ===\n");
    setup_env();

    test_register_and_get();
    test_multiple_registrations();
    test_retain_release();
    test_resolver_fn();
    test_tree_handle_resolved();
    test_nested_trees();
    test_cycle_detection();
    test_invalid_args();
    test_to_utf8_full_codepoints_only();
    test_to_utf8_full_with_literal();
    test_to_utf8_full_with_tree();
    test_to_utf8_full_mixed();
    test_to_utf8_full_invalid();
    test_cow_sharing();

    teardown_env();
    printf("Passed: %d, Failed: %d\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
