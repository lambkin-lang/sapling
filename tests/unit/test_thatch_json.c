/*
 * test_thatch_json.c — unit tests for the Thatch JSONL parser and jq cursor
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#include "sapling/arena.h"
#include "sapling/txn.h"
#include "sapling/thatch.h"
#include "sapling/thatch_json.h"

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

/* Helper: create a test env with Thatch registered */
static SapMemArena *g_arena;
static SapEnv *g_env;
static SapTxnCtx *g_txn;

static void setup(void) {
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_MALLOC,
        .cfg.mmap.max_size = 1024 * 1024
    };
    sap_arena_init(&g_arena, &opts);
    g_env = sap_env_create(g_arena, SAPLING_PAGE_SIZE);
    sap_thatch_subsystem_init(g_env);
    g_txn = sap_txn_begin(g_env, NULL, 0);
}

static void teardown(void) {
    if (g_txn) sap_txn_abort(g_txn);
    g_txn = NULL;
    sap_env_destroy(g_env);
    g_env = NULL;
    sap_arena_destroy(g_arena);
    g_arena = NULL;
}

/* Helper: parse JSON string, assert success */
static int parse_ok(const char *json, ThatchRegion **r, ThatchVal *v) {
    uint32_t err_pos = 0;
    int rc = tj_parse(g_txn, json, (uint32_t)strlen(json), r, v, &err_pos);
    if (rc != ERR_OK) {
        fprintf(stderr, "  parse failed at pos %u for: %s\n", err_pos, json);
    }
    return rc;
}

/* ================================================================== */
/* Parsing Tests                                                      */
/* ================================================================== */

static void test_parse_null(void) {
    printf("--- parse null ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;
    CHECK(parse_ok("null", &r, &v) == ERR_OK);
    CHECK(tj_type(v) == TJ_TYPE_NULL);
    CHECK(tj_is_null(v));
    teardown();
}

static void test_parse_booleans(void) {
    printf("--- parse booleans ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("true", &r, &v) == ERR_OK);
    CHECK(tj_type(v) == TJ_TYPE_TRUE);
    CHECK(tj_is_bool(v));
    int b;
    CHECK(tj_bool(v, &b) == ERR_OK);
    CHECK(b == 1);

    CHECK(parse_ok("false", &r, &v) == ERR_OK);
    CHECK(tj_type(v) == TJ_TYPE_FALSE);
    CHECK(tj_bool(v, &b) == ERR_OK);
    CHECK(b == 0);
    teardown();
}

static void test_parse_integers(void) {
    printf("--- parse integers ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;
    int64_t iv;

    CHECK(parse_ok("0", &r, &v) == ERR_OK);
    CHECK(tj_type(v) == TJ_TYPE_INT);
    CHECK(tj_int(v, &iv) == ERR_OK);
    CHECK(iv == 0);

    CHECK(parse_ok("42", &r, &v) == ERR_OK);
    CHECK(tj_int(v, &iv) == ERR_OK);
    CHECK(iv == 42);

    CHECK(parse_ok("-1", &r, &v) == ERR_OK);
    CHECK(tj_int(v, &iv) == ERR_OK);
    CHECK(iv == -1);

    CHECK(parse_ok("9223372036854775807", &r, &v) == ERR_OK); /* INT64_MAX */
    CHECK(tj_int(v, &iv) == ERR_OK);
    CHECK(iv == 9223372036854775807LL);

    CHECK(parse_ok("-9223372036854775808", &r, &v) == ERR_OK); /* INT64_MIN */
    CHECK(tj_int(v, &iv) == ERR_OK);
    CHECK(iv == (-9223372036854775807LL - 1));

    /* int→double promotion */
    double dv;
    CHECK(parse_ok("42", &r, &v) == ERR_OK);
    CHECK(tj_double(v, &dv) == ERR_OK);
    CHECK(dv == 42.0);
    teardown();
}

static void test_parse_doubles(void) {
    printf("--- parse doubles ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;
    double dv;

    CHECK(parse_ok("3.14", &r, &v) == ERR_OK);
    CHECK(tj_type(v) == TJ_TYPE_DOUBLE);
    CHECK(tj_double(v, &dv) == ERR_OK);
    CHECK(fabs(dv - 3.14) < 1e-12);

    CHECK(parse_ok("-0.5", &r, &v) == ERR_OK);
    CHECK(tj_double(v, &dv) == ERR_OK);
    CHECK(fabs(dv - (-0.5)) < 1e-12);

    CHECK(parse_ok("1e10", &r, &v) == ERR_OK);
    CHECK(tj_double(v, &dv) == ERR_OK);
    CHECK(fabs(dv - 1e10) < 1e3);

    CHECK(parse_ok("2.5E-3", &r, &v) == ERR_OK);
    CHECK(tj_double(v, &dv) == ERR_OK);
    CHECK(fabs(dv - 0.0025) < 1e-12);

    /* Integer overflow → double */
    CHECK(parse_ok("99999999999999999999", &r, &v) == ERR_OK);
    CHECK(tj_type(v) == TJ_TYPE_DOUBLE);
    teardown();
}

static void test_parse_strings(void) {
    printf("--- parse strings ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;
    const char *s; uint32_t slen;

    CHECK(parse_ok("\"hello\"", &r, &v) == ERR_OK);
    CHECK(tj_type(v) == TJ_TYPE_STRING);
    CHECK(tj_string(v, &s, &slen) == ERR_OK);
    CHECK(slen == 5);
    CHECK(memcmp(s, "hello", 5) == 0);

    /* Empty string */
    CHECK(parse_ok("\"\"", &r, &v) == ERR_OK);
    CHECK(tj_string(v, &s, &slen) == ERR_OK);
    CHECK(slen == 0);

    /* Escape sequences */
    CHECK(parse_ok("\"a\\nb\\tc\"", &r, &v) == ERR_OK);
    CHECK(tj_string(v, &s, &slen) == ERR_OK);
    CHECK(slen == 5);
    CHECK(s[0] == 'a' && s[1] == '\n' && s[2] == 'b' && s[3] == '\t' && s[4] == 'c');

    CHECK(parse_ok("\"\\\"quoted\\\"\"", &r, &v) == ERR_OK);
    CHECK(tj_string(v, &s, &slen) == ERR_OK);
    CHECK(slen == 8);
    CHECK(memcmp(s, "\"quoted\"", 8) == 0);

    /* Unicode escape: \u0041 = 'A' */
    CHECK(parse_ok("\"\\u0041\"", &r, &v) == ERR_OK);
    CHECK(tj_string(v, &s, &slen) == ERR_OK);
    CHECK(slen == 1);
    CHECK(s[0] == 'A');

    /* 2-byte UTF-8: \u00E9 = é (0xC3 0xA9) */
    CHECK(parse_ok("\"\\u00e9\"", &r, &v) == ERR_OK);
    CHECK(tj_string(v, &s, &slen) == ERR_OK);
    CHECK(slen == 2);
    CHECK((uint8_t)s[0] == 0xC3 && (uint8_t)s[1] == 0xA9);

    /* Surrogate pair: \uD83D\uDE00 = 😀 (U+1F600, 4-byte UTF-8) */
    CHECK(parse_ok("\"\\uD83D\\uDE00\"", &r, &v) == ERR_OK);
    CHECK(tj_string(v, &s, &slen) == ERR_OK);
    CHECK(slen == 4);
    CHECK((uint8_t)s[0] == 0xF0 && (uint8_t)s[1] == 0x9F &&
          (uint8_t)s[2] == 0x98 && (uint8_t)s[3] == 0x80);
    teardown();
}

static void test_parse_arrays(void) {
    printf("--- parse arrays ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    /* Empty array */
    CHECK(parse_ok("[]", &r, &v) == ERR_OK);
    CHECK(tj_type(v) == TJ_TYPE_ARRAY);
    uint32_t len;
    CHECK(tj_length(v, &len) == ERR_OK);
    CHECK(len == 0);

    /* Simple array */
    CHECK(parse_ok("[1, 2, 3]", &r, &v) == ERR_OK);
    CHECK(tj_length(v, &len) == ERR_OK);
    CHECK(len == 3);

    /* Mixed types */
    CHECK(parse_ok("[null, true, 42, \"hi\"]", &r, &v) == ERR_OK);
    CHECK(tj_length(v, &len) == ERR_OK);
    CHECK(len == 4);

    ThatchVal elem;
    CHECK(tj_index(v, 0, &elem) == ERR_OK);
    CHECK(tj_is_null(elem));
    CHECK(tj_index(v, 1, &elem) == ERR_OK);
    int b; CHECK(tj_bool(elem, &b) == ERR_OK); CHECK(b == 1);
    CHECK(tj_index(v, 2, &elem) == ERR_OK);
    int64_t iv; CHECK(tj_int(elem, &iv) == ERR_OK); CHECK(iv == 42);
    CHECK(tj_index(v, 3, &elem) == ERR_OK);
    const char *s; uint32_t slen;
    CHECK(tj_string(elem, &s, &slen) == ERR_OK);
    CHECK(slen == 2 && memcmp(s, "hi", 2) == 0);

    /* Out of bounds */
    CHECK(tj_index(v, 4, &elem) == ERR_NOT_FOUND);
    teardown();
}

static void test_parse_objects(void) {
    printf("--- parse objects ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    /* Empty object */
    CHECK(parse_ok("{}", &r, &v) == ERR_OK);
    CHECK(tj_type(v) == TJ_TYPE_OBJECT);
    uint32_t len;
    CHECK(tj_length(v, &len) == ERR_OK);
    CHECK(len == 0);

    /* Simple object */
    CHECK(parse_ok("{\"name\": \"Alice\", \"age\": 30}", &r, &v) == ERR_OK);
    CHECK(tj_length(v, &len) == ERR_OK);
    CHECK(len == 2);

    ThatchVal name;
    CHECK(tj_get_str(v, "name", &name) == ERR_OK);
    const char *s; uint32_t slen;
    CHECK(tj_string(name, &s, &slen) == ERR_OK);
    CHECK(slen == 5 && memcmp(s, "Alice", 5) == 0);

    ThatchVal age;
    CHECK(tj_get_str(v, "age", &age) == ERR_OK);
    int64_t iv;
    CHECK(tj_int(age, &iv) == ERR_OK);
    CHECK(iv == 30);

    /* Not found */
    ThatchVal missing;
    CHECK(tj_get_str(v, "nope", &missing) == ERR_NOT_FOUND);
    teardown();
}

static void test_parse_nested(void) {
    printf("--- parse nested structures ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    const char *json =
        "{\"users\": [{\"name\": \"Alice\", \"score\": 95},"
        " {\"name\": \"Bob\", \"score\": 87}],"
        " \"count\": 2}";
    CHECK(parse_ok(json, &r, &v) == ERR_OK);

    /* Navigate to users[1].name */
    ThatchVal users, user1, name;
    CHECK(tj_get_str(v, "users", &users) == ERR_OK);
    CHECK(tj_is_array(users));
    CHECK(tj_index(users, 1, &user1) == ERR_OK);
    CHECK(tj_is_object(user1));
    CHECK(tj_get_str(user1, "name", &name) == ERR_OK);
    const char *s; uint32_t slen;
    CHECK(tj_string(name, &s, &slen) == ERR_OK);
    CHECK(slen == 3 && memcmp(s, "Bob", 3) == 0);

    /* Navigate to count */
    ThatchVal count;
    CHECK(tj_get_str(v, "count", &count) == ERR_OK);
    int64_t iv;
    CHECK(tj_int(count, &iv) == ERR_OK);
    CHECK(iv == 2);
    teardown();
}

/* ================================================================== */
/* Whitespace and edge cases                                          */
/* ================================================================== */

static void test_parse_whitespace(void) {
    printf("--- parse with whitespace ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("  \t\n  42  \n  ", &r, &v) == ERR_OK);
    int64_t iv;
    CHECK(tj_int(v, &iv) == ERR_OK);
    CHECK(iv == 42);

    CHECK(parse_ok("{ \"a\" : [ 1 , 2 ] }", &r, &v) == ERR_OK);
    CHECK(tj_is_object(v));
    teardown();
}

static void test_parse_errors(void) {
    printf("--- parse errors ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;
    uint32_t err_pos;

    CHECK(tj_parse(g_txn, "", 0, &r, &v, &err_pos) == ERR_PARSE);
    CHECK(tj_parse(g_txn, "[", 1, &r, &v, &err_pos) == ERR_PARSE);
    CHECK(tj_parse(g_txn, "{\"a\"}", 5, &r, &v, &err_pos) == ERR_PARSE);
    CHECK(tj_parse(g_txn, "nul", 3, &r, &v, &err_pos) == ERR_PARSE);
    CHECK(tj_parse(g_txn, "tru", 3, &r, &v, &err_pos) == ERR_PARSE);
    CHECK(tj_parse(g_txn, "42 99", 5, &r, &v, &err_pos) == ERR_PARSE);
    CHECK(err_pos == 3); /* trailing 99 starts at pos 3 */
    teardown();
}

/* ================================================================== */
/* Type error tests                                                   */
/* ================================================================== */

static void test_type_errors(void) {
    printf("--- type errors ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("42", &r, &v) == ERR_OK);

    /* Can't use string ops on int */
    const char *s; uint32_t slen;
    CHECK(tj_string(v, &s, &slen) == ERR_TYPE);

    /* Can't use bool ops on int */
    int b;
    CHECK(tj_bool(v, &b) == ERR_TYPE);

    /* Can't index into int */
    ThatchVal elem;
    CHECK(tj_index(v, 0, &elem) == ERR_TYPE);

    /* Can't get field from int */
    CHECK(tj_get_str(v, "x", &elem) == ERR_TYPE);

    /* Can't iterate int */
    TjIter iter;
    CHECK(tj_iter_array(v, &iter) == ERR_TYPE);
    CHECK(tj_iter_object(v, &iter) == ERR_TYPE);

    /* Can't get int from string */
    CHECK(parse_ok("\"hello\"", &r, &v) == ERR_OK);
    int64_t iv;
    CHECK(tj_int(v, &iv) == ERR_TYPE);
    teardown();
}

/* ================================================================== */
/* Iteration tests                                                    */
/* ================================================================== */

static void test_iter_array(void) {
    printf("--- iterate array ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("[10, 20, 30]", &r, &v) == ERR_OK);

    TjIter iter;
    CHECK(tj_iter_array(v, &iter) == ERR_OK);

    ThatchVal elem;
    int64_t iv;
    CHECK(tj_iter_next(&iter, &elem) == ERR_OK);
    CHECK(tj_int(elem, &iv) == ERR_OK); CHECK(iv == 10);
    CHECK(tj_iter_next(&iter, &elem) == ERR_OK);
    CHECK(tj_int(elem, &iv) == ERR_OK); CHECK(iv == 20);
    CHECK(tj_iter_next(&iter, &elem) == ERR_OK);
    CHECK(tj_int(elem, &iv) == ERR_OK); CHECK(iv == 30);
    CHECK(tj_iter_next(&iter, &elem) == ERR_NOT_FOUND);

    /* Iterate empty array */
    CHECK(parse_ok("[]", &r, &v) == ERR_OK);
    CHECK(tj_iter_array(v, &iter) == ERR_OK);
    CHECK(tj_iter_next(&iter, &elem) == ERR_NOT_FOUND);
    teardown();
}

static void test_iter_object(void) {
    printf("--- iterate object ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("{\"x\": 1, \"y\": 2}", &r, &v) == ERR_OK);

    TjIter iter;
    CHECK(tj_iter_object(v, &iter) == ERR_OK);

    const char *key; uint32_t klen;
    ThatchVal val;
    int64_t iv;

    CHECK(tj_iter_next_kv(&iter, &key, &klen, &val) == ERR_OK);
    CHECK(klen == 1 && key[0] == 'x');
    CHECK(tj_int(val, &iv) == ERR_OK); CHECK(iv == 1);

    CHECK(tj_iter_next_kv(&iter, &key, &klen, &val) == ERR_OK);
    CHECK(klen == 1 && key[0] == 'y');
    CHECK(tj_int(val, &iv) == ERR_OK); CHECK(iv == 2);

    CHECK(tj_iter_next_kv(&iter, &key, &klen, &val) == ERR_NOT_FOUND);
    teardown();
}

/* ================================================================== */
/* Path expression tests                                              */
/* ================================================================== */

static void test_path_identity(void) {
    printf("--- path: identity ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("42", &r, &v) == ERR_OK);
    ThatchVal out;
    CHECK(tj_path(v, ".", &out) == ERR_OK);
    int64_t iv;
    CHECK(tj_int(out, &iv) == ERR_OK); CHECK(iv == 42);
    teardown();
}

static void test_path_field(void) {
    printf("--- path: field access ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("{\"name\": \"Alice\"}", &r, &v) == ERR_OK);
    ThatchVal out;
    CHECK(tj_path(v, ".name", &out) == ERR_OK);
    const char *s; uint32_t slen;
    CHECK(tj_string(out, &s, &slen) == ERR_OK);
    CHECK(slen == 5 && memcmp(s, "Alice", 5) == 0);
    teardown();
}

static void test_path_index(void) {
    printf("--- path: array index ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("[10, 20, 30]", &r, &v) == ERR_OK);
    ThatchVal out;
    CHECK(tj_path(v, ".[1]", &out) == ERR_OK);
    int64_t iv;
    CHECK(tj_int(out, &iv) == ERR_OK); CHECK(iv == 20);
    teardown();
}

static void test_path_chained(void) {
    printf("--- path: chained navigation ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    const char *json =
        "{\"users\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}";
    CHECK(parse_ok(json, &r, &v) == ERR_OK);

    ThatchVal out;
    CHECK(tj_path(v, ".users[1].name", &out) == ERR_OK);
    const char *s; uint32_t slen;
    CHECK(tj_string(out, &s, &slen) == ERR_OK);
    CHECK(slen == 3 && memcmp(s, "Bob", 3) == 0);

    /* Also test the .[N] form after a dot */
    CHECK(tj_path(v, ".users.[0].name", &out) == ERR_OK);
    CHECK(tj_string(out, &s, &slen) == ERR_OK);
    CHECK(slen == 5 && memcmp(s, "Alice", 5) == 0);
    teardown();
}

static void test_path_quoted_key(void) {
    printf("--- path: quoted key ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("{\"odd key\": 99}", &r, &v) == ERR_OK);
    ThatchVal out;
    CHECK(tj_path(v, ".[\"odd key\"]", &out) == ERR_OK);
    int64_t iv;
    CHECK(tj_int(out, &iv) == ERR_OK); CHECK(iv == 99);
    teardown();
}

static void test_path_not_found(void) {
    printf("--- path: not found ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("{\"a\": 1}", &r, &v) == ERR_OK);
    ThatchVal out;
    CHECK(tj_path(v, ".b", &out) == ERR_NOT_FOUND);
    CHECK(tj_path(v, ".a[0]", &out) == ERR_TYPE); /* a is int, not array */
    teardown();
}

static void test_path_errors(void) {
    printf("--- path: syntax errors ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("42", &r, &v) == ERR_OK);
    ThatchVal out;
    CHECK(tj_path(v, "", &out) == ERR_PARSE);   /* no leading dot */
    CHECK(tj_path(v, "x", &out) == ERR_PARSE);  /* no leading dot */
    CHECK(tj_path(v, "..", &out) == ERR_PARSE);  /* trailing dot */
    teardown();
}

/* ================================================================== */
/* JSONL tests                                                        */
/* ================================================================== */

typedef struct {
    int count;
    int64_t sum;
} JsonlCtx;

static int jsonl_summer(ThatchVal val, ThatchRegion *region, uint32_t line_no, void *ctx) {
    (void)region; (void)line_no;
    JsonlCtx *jc = (JsonlCtx *)ctx;
    jc->count++;
    int64_t iv;
    if (tj_int(val, &iv) == ERR_OK) jc->sum += iv;
    return ERR_OK;
}

static void test_jsonl_basic(void) {
    printf("--- JSONL basic ---\n");
    setup();

    const char *jsonl = "1\n2\n3\n";
    JsonlCtx ctx = {0, 0};
    CHECK(tj_parse_jsonl(g_txn, jsonl, (uint32_t)strlen(jsonl),
                         jsonl_summer, &ctx) == ERR_OK);
    CHECK(ctx.count == 3);
    CHECK(ctx.sum == 6);
    teardown();
}

static void test_jsonl_blank_lines(void) {
    printf("--- JSONL blank lines ---\n");
    setup();

    const char *jsonl = "\n10\n\n\n20\n\n";
    JsonlCtx ctx = {0, 0};
    CHECK(tj_parse_jsonl(g_txn, jsonl, (uint32_t)strlen(jsonl),
                         jsonl_summer, &ctx) == ERR_OK);
    CHECK(ctx.count == 2);
    CHECK(ctx.sum == 30);
    teardown();
}

static int jsonl_field_reader(ThatchVal val, ThatchRegion *region, uint32_t line_no, void *ctx) {
    (void)region; (void)line_no;
    int64_t *sum = (int64_t *)ctx;
    ThatchVal score;
    if (tj_get_str(val, "score", &score) == ERR_OK) {
        int64_t iv;
        if (tj_int(score, &iv) == ERR_OK) *sum += iv;
    }
    return ERR_OK;
}

static void test_jsonl_objects(void) {
    printf("--- JSONL objects with jq navigation ---\n");
    setup();

    const char *jsonl =
        "{\"name\": \"Alice\", \"score\": 95}\n"
        "{\"name\": \"Bob\", \"score\": 87}\n"
        "{\"name\": \"Carol\", \"score\": 91}\n";

    int64_t sum = 0;
    CHECK(tj_parse_jsonl(g_txn, jsonl, (uint32_t)strlen(jsonl),
                         jsonl_field_reader, &sum) == ERR_OK);
    CHECK(sum == 95 + 87 + 91);
    teardown();
}

/* ================================================================== */
/* val_byte_size tests                                                */
/* ================================================================== */

static void test_val_byte_size(void) {
    printf("--- val_byte_size ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;
    uint32_t sz;

    CHECK(parse_ok("null", &r, &v) == ERR_OK);
    CHECK(tj_val_byte_size(r, 0, &sz) == ERR_OK);
    CHECK(sz == 1);

    CHECK(parse_ok("42", &r, &v) == ERR_OK);
    CHECK(tj_val_byte_size(r, 0, &sz) == ERR_OK);
    CHECK(sz == 9); /* tag(1) + int64(8) */

    CHECK(parse_ok("\"hi\"", &r, &v) == ERR_OK);
    CHECK(tj_val_byte_size(r, 0, &sz) == ERR_OK);
    CHECK(sz == 1 + 4 + 2); /* tag + len + "hi" */

    CHECK(parse_ok("[1,2]", &r, &v) == ERR_OK);
    CHECK(tj_val_byte_size(r, 0, &sz) == ERR_OK);
    CHECK(sz == thatch_region_used(r)); /* whole region is one array */
    teardown();
}

/* ================================================================== */
/* Deep nesting stress test                                           */
/* ================================================================== */

static void test_deep_nesting(void) {
    printf("--- deep nesting ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    /* Build: {"a":{"a":{"a":42}}} via string concat */
    char json[256];
    int off = 0;
    int depth = 5;
    for (int i = 0; i < depth; i++) off += snprintf(json + off, sizeof(json) - (size_t)off, "{\"a\":");
    off += snprintf(json + off, sizeof(json) - (size_t)off, "42");
    for (int i = 0; i < depth; i++) off += snprintf(json + off, sizeof(json) - (size_t)off, "}");

    CHECK(parse_ok(json, &r, &v) == ERR_OK);

    /* Navigate with path */
    ThatchVal out;
    CHECK(tj_path(v, ".a.a.a.a.a", &out) == ERR_OK);
    int64_t iv;
    CHECK(tj_int(out, &iv) == ERR_OK);
    CHECK(iv == 42);
    teardown();
}

/* ================================================================== */
/* Zero-copy string pointer stability test                            */
/* ================================================================== */

static void test_zero_copy_strings(void) {
    printf("--- zero-copy string pointers ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("{\"a\": \"hello\", \"b\": \"world\"}", &r, &v) == ERR_OK);

    ThatchVal va, vb;
    CHECK(tj_get_str(v, "a", &va) == ERR_OK);
    CHECK(tj_get_str(v, "b", &vb) == ERR_OK);

    const char *sa, *sb;
    uint32_t la, lb;
    CHECK(tj_string(va, &sa, &la) == ERR_OK);
    CHECK(tj_string(vb, &sb, &lb) == ERR_OK);

    /* Both pointers should be into the same region page */
    CHECK(la == 5 && lb == 5);
    CHECK(memcmp(sa, "hello", 5) == 0);
    CHECK(memcmp(sb, "world", 5) == 0);

    /* The pointers should be stable — reading again gives the same address */
    const char *sa2;
    uint32_t la2;
    CHECK(tj_string(va, &sa2, &la2) == ERR_OK);
    CHECK(sa == sa2);
    teardown();
}

/* ================================================================== */
/* Regression: [P1] path index overflow wraps to 0                    */
/* ================================================================== */

static void test_path_index_overflow(void) {
    printf("--- path: index overflow ---\n");
    setup();
    ThatchRegion *r; ThatchVal v;

    CHECK(parse_ok("[10, 20, 30]", &r, &v) == ERR_OK);

    /* UINT32_MAX+1 = 4294967296 should be rejected, not wrap to 0 */
    ThatchVal out;
    CHECK(tj_path(v, ".[4294967296]", &out) == ERR_PARSE);

    /* Just below the overflow boundary should work (as NOT_FOUND) */
    CHECK(tj_path(v, ".[4294967295]", &out) == ERR_NOT_FOUND);

    /* Normal index still works */
    CHECK(tj_path(v, ".[0]", &out) == ERR_OK);
    int64_t iv;
    CHECK(tj_int(out, &iv) == ERR_OK); CHECK(iv == 10);
    teardown();
}

/* ================================================================== */
/* Regression: [P1] parse failures release region pages               */
/* ================================================================== */

static void test_parse_failure_no_leak(void) {
    printf("--- parse failure releases region ---\n");
    setup();

    /* Warm up arena */
    {
        void *warmup = NULL;
        uint32_t warmup_pgno = 0;
        sap_arena_alloc_page(g_arena, &warmup, &warmup_pgno);
        sap_arena_free_page(g_arena, warmup_pgno);
    }

    uint32_t baseline = sap_arena_active_pages(g_arena);

    /* Deliberately parse bad JSON many times in the same txn */
    ThatchRegion *r; ThatchVal v; uint32_t err_pos;
    for (int i = 0; i < 20; i++) {
        int rc = tj_parse(g_txn, "{bad", 4, &r, &v, &err_pos);
        CHECK(rc != ERR_OK);
    }

    /* Pages should be released, not accumulated */
    uint32_t after = sap_arena_active_pages(g_arena);
    CHECK(after == baseline);

    teardown();
}

/* ================================================================== */
/* Entry point                                                        */
/* ================================================================== */

int main(void) {
    /* Parsing */
    test_parse_null();
    test_parse_booleans();
    test_parse_integers();
    test_parse_doubles();
    test_parse_strings();
    test_parse_arrays();
    test_parse_objects();
    test_parse_nested();
    test_parse_whitespace();
    test_parse_errors();

    /* Type errors */
    test_type_errors();

    /* Iteration */
    test_iter_array();
    test_iter_object();

    /* Path expressions */
    test_path_identity();
    test_path_field();
    test_path_index();
    test_path_chained();
    test_path_quoted_key();
    test_path_not_found();
    test_path_errors();

    /* JSONL */
    test_jsonl_basic();
    test_jsonl_blank_lines();
    test_jsonl_objects();

    /* Internals */
    test_val_byte_size();

    /* Stress */
    test_deep_nesting();
    test_zero_copy_strings();

    /* Regression */
    test_path_index_overflow();
    test_parse_failure_no_leak();

    printf("\nResults: %d passed, %d failed\n", passed, failed);
    return failed ? 1 : 0;
}
