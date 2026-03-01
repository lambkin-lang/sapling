/*
 * test_text_literal.c - unit tests for the literal table
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

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

static void test_basic_add_and_get(void)
{
    SECTION("basic add and get");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    const uint8_t *hello = (const uint8_t *)"hello";
    const uint8_t *world = (const uint8_t *)"world";
    uint32_t id0 = 0, id1 = 0;

    CHECK(text_literal_table_add(t, hello, 5, &id0) == ERR_OK);
    CHECK(id0 == 0);
    CHECK(text_literal_table_add(t, world, 5, &id1) == ERR_OK);
    CHECK(id1 == 1);
    CHECK(text_literal_table_count(t) == 2);

    const uint8_t *out = NULL;
    size_t out_len = 0;
    CHECK(text_literal_table_get(t, 0, &out, &out_len) == ERR_OK);
    CHECK(out_len == 5);
    CHECK(memcmp(out, "hello", 5) == 0);

    CHECK(text_literal_table_get(t, 1, &out, &out_len) == ERR_OK);
    CHECK(out_len == 5);
    CHECK(memcmp(out, "world", 5) == 0);

    /* Out of range */
    CHECK(text_literal_table_get(t, 2, &out, &out_len) == ERR_RANGE);

    text_literal_table_free(t);
}

static void test_seal(void)
{
    SECTION("seal");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    const uint8_t *data = (const uint8_t *)"test";
    uint32_t id = 0;
    CHECK(text_literal_table_add(t, data, 4, &id) == ERR_OK);
    CHECK(text_literal_table_is_sealed(t) == 0);

    text_literal_table_seal(t);
    CHECK(text_literal_table_is_sealed(t) == 1);

    /* Add after seal should fail */
    uint32_t id2 = 0;
    CHECK(text_literal_table_add(t, data, 4, &id2) == ERR_INVALID);

    /* Get still works */
    const uint8_t *out = NULL;
    size_t out_len = 0;
    CHECK(text_literal_table_get(t, 0, &out, &out_len) == ERR_OK);
    CHECK(out_len == 4);
    CHECK(memcmp(out, "test", 4) == 0);

    text_literal_table_free(t);
}

static void test_empty_string(void)
{
    SECTION("empty string literal");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    uint32_t id = 0;
    CHECK(text_literal_table_add(t, NULL, 0, &id) == ERR_OK);
    CHECK(id == 0);

    const uint8_t *out = NULL;
    size_t out_len = 0;
    CHECK(text_literal_table_get(t, 0, &out, &out_len) == ERR_OK);
    CHECK(out_len == 0);
    CHECK(out != NULL); /* pointer is stable, even for empty */

    text_literal_table_free(t);
}

static void test_dedup(void)
{
    SECTION("deduplication");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    const uint8_t *abc = (const uint8_t *)"abc";
    const uint8_t *def = (const uint8_t *)"def";
    uint32_t id0 = 0, id1 = 0, id2 = 0, id3 = 0;

    CHECK(text_literal_table_add(t, abc, 3, &id0) == ERR_OK);
    CHECK(id0 == 0);
    CHECK(text_literal_table_add(t, def, 3, &id1) == ERR_OK);
    CHECK(id1 == 1);

    /* Adding same content again returns existing ID */
    CHECK(text_literal_table_add(t, abc, 3, &id2) == ERR_OK);
    CHECK(id2 == 0); /* same as id0 */
    CHECK(text_literal_table_add(t, def, 3, &id3) == ERR_OK);
    CHECK(id3 == 1); /* same as id1 */

    /* Only 2 entries in the table */
    CHECK(text_literal_table_count(t) == 2);

    text_literal_table_free(t);
}

static void test_page_boundary(void)
{
    SECTION("page boundary crossing");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    /* Fill up a page with many small strings, then verify all are accessible.
     * Page size is 4096 bytes. With strings of ~100 bytes each,
     * we need ~41+ to cross a page boundary. */
    uint32_t ids[60];
    char buf[128];
    for (int i = 0; i < 60; i++)
    {
        int n = snprintf(buf, sizeof(buf), "literal-entry-number-%04d-padding-to-fill-page", i);
        CHECK(text_literal_table_add(t, (const uint8_t *)buf, (size_t)n, &ids[i]) == ERR_OK);
        CHECK(ids[i] == (uint32_t)i);
    }

    /* Verify all entries */
    for (int i = 0; i < 60; i++)
    {
        const uint8_t *out = NULL;
        size_t out_len = 0;
        int n = snprintf(buf, sizeof(buf), "literal-entry-number-%04d-padding-to-fill-page", i);
        CHECK(text_literal_table_get(t, ids[i], &out, &out_len) == ERR_OK);
        CHECK(out_len == (size_t)n);
        CHECK(memcmp(out, buf, (size_t)n) == 0);
    }

    text_literal_table_free(t);
}

static void test_invalid_args(void)
{
    SECTION("invalid arguments");
    uint32_t id = 0;
    const uint8_t *out = NULL;
    size_t out_len = 0;

    CHECK(text_literal_table_new(NULL) == NULL);
    CHECK(text_literal_table_add(NULL, (const uint8_t *)"x", 1, &id) == ERR_INVALID);
    CHECK(text_literal_table_get(NULL, 0, &out, &out_len) == ERR_INVALID);
    CHECK(text_literal_table_count(NULL) == 0);

    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);
    CHECK(text_literal_table_add(t, (const uint8_t *)"x", 1, NULL) == ERR_INVALID);
    CHECK(text_literal_table_add(t, NULL, 5, &id) == ERR_INVALID); /* non-zero len with NULL ptr */
    text_literal_table_free(t);
}

static void test_resolver_integration(void)
{
    SECTION("resolver integration");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    /* Add a UTF-8 string: "Héllo" */
    const uint8_t hello_utf8[] = {'H', 0xC3, 0xA9, 'l', 'l', 'o'};
    uint32_t id = 0;
    CHECK(text_literal_table_add(t, hello_utf8, sizeof(hello_utf8), &id) == ERR_OK);
    text_literal_table_seal(t);

    /* Create a Text with a LITERAL handle pointing to this ID */
    Text *text = text_new(g_env);
    CHECK(text != NULL);
    TextHandle handle = text_handle_make(TEXT_HANDLE_LITERAL, id);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);
    CHECK(text_push_back_handle(txn, text, handle) == ERR_OK);
    sap_txn_commit(txn);

    /* Resolve via text_to_utf8_resolved using the literal table */
    TextRuntimeResolver resolver = {0};
    resolver.resolve_literal_utf8_fn = text_literal_table_resolve_fn;
    resolver.ctx = t;

    size_t utf8_need = 0;
    CHECK(text_utf8_length_resolved(text, text_expand_runtime_handle, &resolver, &utf8_need) ==
          ERR_OK);
    CHECK(utf8_need == sizeof(hello_utf8));

    uint8_t buf[32];
    size_t wrote = 0;
    CHECK(text_to_utf8_resolved(text, text_expand_runtime_handle, &resolver, buf, sizeof(buf),
                                &wrote) == ERR_OK);
    CHECK(wrote == sizeof(hello_utf8));
    CHECK(memcmp(buf, hello_utf8, sizeof(hello_utf8)) == 0);

    text_free(g_env, text);
    text_literal_table_free(t);
}

static void test_unicode_literals(void)
{
    SECTION("unicode literals");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    /* Emoji: 🙂 = F0 9F 99 82 */
    const uint8_t emoji[] = {0xF0, 0x9F, 0x99, 0x82};
    uint32_t id = 0;
    CHECK(text_literal_table_add(t, emoji, sizeof(emoji), &id) == ERR_OK);

    const uint8_t *out = NULL;
    size_t out_len = 0;
    CHECK(text_literal_table_get(t, id, &out, &out_len) == ERR_OK);
    CHECK(out_len == 4);
    CHECK(memcmp(out, emoji, 4) == 0);

    text_literal_table_free(t);
}

static void test_bulk_load(void)
{
    SECTION("text_from_utf8_bulk");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    const char *ascii = "Hello, World!";
    size_t ascii_len = strlen(ascii);

    Text *text = text_new(g_env);
    CHECK(text != NULL);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);
    CHECK(text_from_utf8_bulk(txn, text, (const uint8_t *)ascii, ascii_len, t) == ERR_OK);
    sap_txn_commit(txn);

    /* Should have exactly 1 handle (the LITERAL) */
    CHECK(text_length(text) == 1);

    /* The handle should be a LITERAL */
    TextHandle handle = 0;
    CHECK(text_get_handle(text, 0, &handle) == ERR_OK);
    CHECK(text_handle_kind(handle) == TEXT_HANDLE_LITERAL);

    /* Resolve to UTF-8 */
    TextRuntimeResolver resolver = {0};
    resolver.resolve_literal_utf8_fn = text_literal_table_resolve_fn;
    resolver.ctx = t;

    size_t utf8_need = 0;
    CHECK(text_utf8_length_resolved(text, text_expand_runtime_handle, &resolver, &utf8_need) ==
          ERR_OK);
    CHECK(utf8_need == ascii_len);

    uint8_t buf[64];
    size_t wrote = 0;
    CHECK(text_to_utf8_resolved(text, text_expand_runtime_handle, &resolver, buf, sizeof(buf),
                                &wrote) == ERR_OK);
    CHECK(wrote == ascii_len);
    CHECK(memcmp(buf, ascii, ascii_len) == 0);

    text_free(g_env, text);
    text_literal_table_free(t);
}

static void test_bulk_load_multibyte(void)
{
    SECTION("text_from_utf8_bulk multibyte");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    /* "Héllo 🙂" in UTF-8 */
    const uint8_t utf8[] = {'H', 0xC3, 0xA9, 'l', 'l', 'o', ' ', 0xF0, 0x9F, 0x99, 0x82};

    Text *text = text_new(g_env);
    CHECK(text != NULL);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);
    CHECK(text_from_utf8_bulk(txn, text, utf8, sizeof(utf8), t) == ERR_OK);
    sap_txn_commit(txn);

    CHECK(text_length(text) == 1);

    /* Resolve and verify */
    TextRuntimeResolver resolver = {0};
    resolver.resolve_literal_utf8_fn = text_literal_table_resolve_fn;
    resolver.ctx = t;

    uint8_t buf[32];
    size_t wrote = 0;
    CHECK(text_to_utf8_resolved(text, text_expand_runtime_handle, &resolver, buf, sizeof(buf),
                                &wrote) == ERR_OK);
    CHECK(wrote == sizeof(utf8));
    CHECK(memcmp(buf, utf8, sizeof(utf8)) == 0);

    text_free(g_env, text);
    text_literal_table_free(t);
}

static void test_bulk_load_invalid_utf8(void)
{
    SECTION("text_from_utf8_bulk rejects invalid UTF-8");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    /* Overlong sequence */
    const uint8_t bad[] = {0xC0, 0xAF};

    Text *text = text_new(g_env);
    CHECK(text != NULL);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);
    CHECK(text_from_utf8_bulk(txn, text, bad, sizeof(bad), t) == ERR_INVALID);
    sap_txn_abort(txn);

    /* Table should not have any entries (validation rejects before adding) */
    CHECK(text_literal_table_count(t) == 0);

    text_free(g_env, text);
    text_literal_table_free(t);
}

static void test_bulk_load_dedup(void)
{
    SECTION("text_from_utf8_bulk dedup");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    const char *data = "same content";
    size_t len = strlen(data);

    Text *text1 = text_new(g_env);
    Text *text2 = text_new(g_env);
    CHECK(text1 != NULL);
    CHECK(text2 != NULL);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(text_from_utf8_bulk(txn, text1, (const uint8_t *)data, len, t) == ERR_OK);
    CHECK(text_from_utf8_bulk(txn, text2, (const uint8_t *)data, len, t) == ERR_OK);
    sap_txn_commit(txn);

    /* Both should have same literal ID */
    TextHandle h1 = 0, h2 = 0;
    CHECK(text_get_handle(text1, 0, &h1) == ERR_OK);
    CHECK(text_get_handle(text2, 0, &h2) == ERR_OK);
    CHECK(h1 == h2); /* same handle = same literal ID */

    /* Only 1 entry in table */
    CHECK(text_literal_table_count(t) == 1);

    text_free(g_env, text1);
    text_free(g_env, text2);
    text_literal_table_free(t);
}

static void test_expand_handle_at(void)
{
    SECTION("text_expand_handle_at");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    const char *data = "ABC";
    size_t len = strlen(data);

    Text *text = text_new(g_env);
    CHECK(text != NULL);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(text_from_utf8_bulk(txn, text, (const uint8_t *)data, len, t) == ERR_OK);
    sap_txn_commit(txn);

    /* Before expansion: 1 LITERAL handle */
    CHECK(text_length(text) == 1);

    /* Expand */
    txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(txn != NULL);
    CHECK(text_expand_handle_at(txn, text, 0, text_literal_table_resolve_fn, t) == ERR_OK);
    sap_txn_commit(txn);

    /* After expansion: 3 CODEPOINT handles */
    CHECK(text_length(text) == 3);

    uint32_t cp = 0;
    CHECK(text_get(text, 0, &cp) == ERR_OK);
    CHECK(cp == 'A');
    CHECK(text_get(text, 1, &cp) == ERR_OK);
    CHECK(cp == 'B');
    CHECK(text_get(text, 2, &cp) == ERR_OK);
    CHECK(cp == 'C');

    text_free(g_env, text);
    text_literal_table_free(t);
}

static void test_expand_round_trip(void)
{
    SECTION("bulk load + expand round trip");
    TextLiteralTable *t = text_literal_table_new(g_env);
    CHECK(t != NULL);

    /* UTF-8 with multi-byte chars: "Aé€🙂" */
    const uint8_t utf8[] = {'A', 0xC3, 0xA9, 0xE2, 0x82, 0xAC, 0xF0, 0x9F, 0x99, 0x82};

    Text *text = text_new(g_env);
    CHECK(text != NULL);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(text_from_utf8_bulk(txn, text, utf8, sizeof(utf8), t) == ERR_OK);
    sap_txn_commit(txn);

    /* Expand the literal */
    txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(text_expand_handle_at(txn, text, 0, text_literal_table_resolve_fn, t) == ERR_OK);
    sap_txn_commit(txn);

    /* Should have 4 CODEPOINT handles: A, é, €, 🙂 */
    CHECK(text_length(text) == 4);

    /* Encode back to UTF-8 (no resolver needed — all CODEPOINTs) */
    uint8_t buf[32];
    size_t wrote = 0;
    CHECK(text_to_utf8(text, buf, sizeof(buf), &wrote) == ERR_OK);
    CHECK(wrote == sizeof(utf8));
    CHECK(memcmp(buf, utf8, sizeof(utf8)) == 0);

    text_free(g_env, text);
    text_literal_table_free(t);
}

static void test_expand_noop_on_codepoint(void)
{
    SECTION("expand no-op on CODEPOINT handle");
    Text *text = text_new(g_env);
    CHECK(text != NULL);

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(text_push_back(txn, text, 'X') == ERR_OK);
    sap_txn_commit(txn);

    /* Expand on a CODEPOINT handle should be a no-op */
    txn = sap_txn_begin(g_env, NULL, 0);
    CHECK(text_expand_handle_at(txn, text, 0, text_literal_table_resolve_fn, NULL) == ERR_OK);
    sap_txn_commit(txn);

    CHECK(text_length(text) == 1);
    uint32_t cp = 0;
    CHECK(text_get(text, 0, &cp) == ERR_OK);
    CHECK(cp == 'X');

    text_free(g_env, text);
}

static void print_summary(void) { printf("Passed: %d, Failed: %d\n", g_pass, g_fail); }

int main(void)
{
    setup_env();
    printf("=== text literal table unit tests ===\n");

    test_basic_add_and_get();
    test_seal();
    test_empty_string();
    test_dedup();
    test_page_boundary();
    test_invalid_args();
    test_resolver_integration();
    test_unicode_literals();
    test_bulk_load();
    test_bulk_load_multibyte();
    test_bulk_load_invalid_utf8();
    test_bulk_load_dedup();
    test_expand_handle_at();
    test_expand_round_trip();
    test_expand_noop_on_codepoint();

    print_summary();
    teardown_env();
    return g_fail ? 1 : 0;
}
