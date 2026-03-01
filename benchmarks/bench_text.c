/*
 * bench_text.c - throughput benchmark for text operations
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text.h"
#include "sapling/txn.h"
#include "sapling/arena.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static SapMemArena *g_arena = NULL;
static SapEnv *g_env = NULL;

static void setup_env(void) {
    SapArenaOptions opts = {0};
    opts.type = SAP_ARENA_BACKING_MALLOC;
    opts.page_size = 4096;
    sap_arena_init(&g_arena, &opts);
    g_env = sap_env_create(g_arena, 4096);
}

static void teardown_env(void) {
    if (g_env) sap_env_destroy(g_env);
    if (g_arena) sap_arena_destroy(g_arena);
}

static double now_seconds(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static int parse_u32(const char *s, uint32_t *out)
{
    unsigned long v;
    char *end = NULL;
    if (!s || !*s)
        return 0;
    v = strtoul(s, &end, 10);
    if (!end || *end != '\0')
        return 0;
    if (v > 0xFFFFFFFFul)
        return 0;
    *out = (uint32_t)v;
    return 1;
}

static uint32_t pattern_ascii(uint32_t i) { return 0x41u + (i % 26u); }

static uint32_t pattern_multibyte(uint32_t i)
{
    switch (i % 4u)
    {
    case 0:
        return pattern_ascii(i);
    case 1:
        return 0x00E9u;
    case 2:
        return 0x20ACu;
    default:
        return 0x1F642u;
    }
}

typedef struct
{
    uint32_t id;
    const uint8_t *utf8;
    size_t utf8_len;
} BenchLiteralEntry;

typedef struct
{
    uint32_t id;
    const Text *text;
} BenchTreeEntry;

typedef struct
{
    const BenchLiteralEntry *literals;
    size_t literal_count;
    const BenchTreeEntry *trees;
    size_t tree_count;
} BenchResolverCtx;

static int bench_resolve_literal_utf8(uint32_t literal_id, const uint8_t **utf8_out,
                                      size_t *utf8_len_out, void *ctx)
{
    BenchResolverCtx *resolver = (BenchResolverCtx *)ctx;
    if (!resolver || !utf8_out || !utf8_len_out)
        return ERR_INVALID;
    for (size_t i = 0; i < resolver->literal_count; i++)
    {
        if (resolver->literals[i].id != literal_id)
            continue;
        *utf8_out = resolver->literals[i].utf8;
        *utf8_len_out = resolver->literals[i].utf8_len;
        return ERR_OK;
    }
    return ERR_INVALID;
}

static int bench_resolve_tree_text(uint32_t tree_id, const Text **tree_out, void *ctx)
{
    BenchResolverCtx *resolver = (BenchResolverCtx *)ctx;
    if (!resolver || !tree_out)
        return ERR_INVALID;
    for (size_t i = 0; i < resolver->tree_count; i++)
    {
        if (resolver->trees[i].id != tree_id)
            continue;
        *tree_out = resolver->trees[i].text;
        return ERR_OK;
    }
    return ERR_INVALID;
}

static int run_append_pop(uint32_t count)
{
    Text *text = text_new(g_env);
    if (!text)
        return 0;

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    if (!txn) { text_free(g_env, text); return 0; }

    for (uint32_t i = 0; i < count; i++)
    {
        if (text_push_back(txn, text, pattern_ascii(i)) != ERR_OK)
            goto fail;
    }
    for (uint32_t i = 0; i < count; i++)
    {
        uint32_t out = 0;
        if (text_pop_front(txn, text, &out) != ERR_OK || out != pattern_ascii(i))
            goto fail;
    }
    sap_txn_commit(txn);
    text_free(g_env, text);
    return 1;

fail:
    sap_txn_abort(txn);
    text_free(g_env, text);
    return 0;
}

static int run_mid_edits(uint32_t count)
{
    Text *text = text_new(g_env);
    if (!text)
        return 0;

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    if (!txn) { text_free(g_env, text); return 0; }

    for (uint32_t i = 0; i < count; i++)
    {
        if (text_push_back(txn, text, pattern_multibyte(i)) != ERR_OK)
            goto fail;
    }

    if (count > 0)
    {
        for (uint32_t i = 0; i < count; i++)
        {
            size_t idx = text_length(text) / 2u;
            if (text_set(txn, text, idx, pattern_multibyte(i + 11u)) != ERR_OK)
                goto fail;
            if (text_insert(txn, text, idx, pattern_multibyte(i + 29u)) != ERR_OK)
                goto fail;
            if (text_delete(txn, text, idx + 1u, NULL) != ERR_OK)
                goto fail;
        }
    }

    sap_txn_commit(txn);
    text_free(g_env, text);
    return 1;

fail:
    sap_txn_abort(txn);
    text_free(g_env, text);
    return 0;
}

static int run_utf8_roundtrip(uint32_t count)
{
    Text *text = NULL;
    Text *roundtrip = NULL;
    uint8_t *buf = NULL;
    size_t need = 0;
    size_t wrote = 0;

    text = text_new(g_env);
    roundtrip = text_new(g_env);
    if (!text || !roundtrip)
        goto fail;

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    if (!txn) goto fail;

    for (uint32_t i = 0; i < count; i++)
    {
        if (text_push_back(txn, text, pattern_multibyte(i)) != ERR_OK)
            goto abort;
    }

    if (text_utf8_length(text, &need) != ERR_OK)
        goto abort;
    buf = (uint8_t *)malloc(need > 0 ? need : 1u);
    if (!buf)
        goto abort;
    if (text_to_utf8(text, buf, need, &wrote) != ERR_OK || wrote != need)
        goto abort;
    if (text_from_utf8(txn, roundtrip, buf, wrote) != ERR_OK)
        goto abort;
    // NOTE: text_length is read-only, no txn required? The API says const Text*
    if (text_length(roundtrip) != text_length(text))
        goto abort;

    sap_txn_commit(txn);
    free(buf);
    text_free(g_env, text);
    text_free(g_env, roundtrip);
    return 1;

abort:
    sap_txn_abort(txn);
fail:
    free(buf);
    if (text) text_free(g_env, text);
    if (roundtrip) text_free(g_env, roundtrip);
    return 0;
}

static int run_clone_detach(uint32_t count)
{
    const uint32_t seed_len = 256u;
    Text *base = NULL;

    base = text_new(g_env);
    if (!base)
        return 0;

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    if (!txn) { text_free(g_env, base); return 0; }

    for (uint32_t i = 0; i < seed_len; i++)
    {
        if (text_push_back(txn, base, pattern_multibyte(i)) != ERR_OK)
        {
            sap_txn_abort(txn);
            text_free(g_env, base);
            return 0;
        }
    }
    sap_txn_commit(txn);

    for (uint32_t i = 0; i < count; i++)
    {
        Text *clone = text_clone(g_env, base);
        size_t idx = (size_t)(i % seed_len);
        if (!clone)
        {
            text_free(g_env, base);
            return 0;
        }

        txn = sap_txn_begin(g_env, NULL, 0);
        if (!txn) { text_free(g_env, clone); text_free(g_env, base); return 0; }

        if (text_set(txn, clone, idx, pattern_multibyte(i + 17u)) != ERR_OK)
        {
            sap_txn_abort(txn);
            text_free(g_env, clone);
            text_free(g_env, base);
            return 0;
        }
        sap_txn_commit(txn);
        text_free(g_env, clone);
    }

    if (text_length(base) != seed_len)
    {
        text_free(g_env, base);
        return 0;
    }
    text_free(g_env, base);
    return 1;
}

static int run_utf8_resolved(uint32_t count)
{
    Text *root = NULL;
    Text *tree = NULL;
    uint8_t *buf = NULL;
    size_t need = 0;
    size_t wrote = 0;
    size_t cp_len = 0;
    TextHandle cp_handle = 0;
    const uint8_t literal_word[] = {'h', 'e', 'l', 'l', 'o'};
    const uint8_t literal_smile[] = {0xF0u, 0x9Fu, 0x99u, 0x82u};
    const BenchLiteralEntry literals[] = {
        {1u, literal_word, sizeof(literal_word)},
        {2u, literal_smile, sizeof(literal_smile)},
    };
    BenchTreeEntry trees[] = {
        {7u, NULL},
    };
    BenchResolverCtx resolver_ctx = {literals, 2u, trees, 1u};
    TextRuntimeResolver resolver = {bench_resolve_literal_utf8, bench_resolve_tree_text,
                                    &resolver_ctx, 8u, 16384u};

    root = text_new(g_env);
    tree = text_new(g_env);
    if (!root || !tree)
        goto fail;

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    if (!txn) goto fail;

    if (text_push_back_handle(txn, tree, text_handle_make(TEXT_HANDLE_LITERAL, 2u)) != ERR_OK)
        goto abort;
    if (text_handle_from_codepoint((uint32_t)'!', &cp_handle) != ERR_OK)
        goto abort;
    if (text_push_back_handle(txn, tree, cp_handle) != ERR_OK)
        goto abort;
    trees[0].text = tree;

    for (uint32_t i = 0; i < count; i++)
    {
        if (text_handle_from_codepoint(pattern_ascii(i), &cp_handle) != ERR_OK)
            goto abort;
        if (text_push_back_handle(txn, root, cp_handle) != ERR_OK)
            goto abort;
        if (text_push_back_handle(txn, root, text_handle_make(TEXT_HANDLE_LITERAL, 1u)) != ERR_OK)
            goto abort;
        if (text_push_back_handle(txn, root, text_handle_make(TEXT_HANDLE_TREE, 7u)) != ERR_OK)
            goto abort;
    }

    // Resolved APIs do not need txn
    if (text_codepoint_length_resolved(root, text_expand_runtime_handle, &resolver, &cp_len) !=
        ERR_OK)
        goto abort;
    if (cp_len == 0u)
        goto abort;

    if (text_utf8_length_resolved(root, text_expand_runtime_handle, &resolver, &need) != ERR_OK)
        goto abort;
    buf = (uint8_t *)malloc(need > 0u ? need : 1u);
    if (!buf)
        goto abort;
    if (text_to_utf8_resolved(root, text_expand_runtime_handle, &resolver, buf, need, &wrote) !=
            ERR_OK ||
        wrote != need)
        goto abort;

    sap_txn_commit(txn);
    free(buf);
    text_free(g_env, root);
    text_free(g_env, tree);
    return 1;

abort:
    sap_txn_abort(txn);
fail:
    free(buf);
    if (root) text_free(g_env, root);
    if (tree) text_free(g_env, tree);
    return 0;
}

static void print_metric(const char *name, double total_secs, uint32_t rounds, double ops_per_round)
{
    double avg = total_secs / (double)rounds;
    double mops = (ops_per_round / avg) / 1000000.0;
    printf("%-22s  avg=%8.6f s  throughput=%8.2f Mops/s\n", name, avg, mops);
}

int main(int argc, char **argv)
{
    uint32_t count = 100000u;
    uint32_t rounds = 3u;
    double t_append_pop = 0.0;
    double t_mid_edits = 0.0;
    double t_utf8 = 0.0;
    double t_clone_detach = 0.0;
    double t_utf8_resolved = 0.0;

    for (int i = 1; i < argc; i++)
    {
        if (strcmp(argv[i], "--count") == 0 && i + 1 < argc)
        {
            if (!parse_u32(argv[++i], &count))
            {
                fprintf(stderr, "invalid --count value\n");
                return 2;
            }
        }
        else if (strcmp(argv[i], "--rounds") == 0 && i + 1 < argc)
        {
            if (!parse_u32(argv[++i], &rounds) || rounds == 0)
            {
                fprintf(stderr, "invalid --rounds value\n");
                return 2;
            }
        }
        else
        {
            fprintf(stderr, "usage: %s [--count N] [--rounds R]\n", argv[0]);
            return 2;
        }
    }

    setup_env();

    for (uint32_t r = 0; r < rounds; r++)
    {
        double start = now_seconds();
        if (!run_append_pop(count))
        {
            fprintf(stderr, "append/pop benchmark failed on round %u\n", r + 1u);
            return 1;
        }
        t_append_pop += (now_seconds() - start);

        start = now_seconds();
        if (!run_mid_edits(count))
        {
            fprintf(stderr, "mid-edits benchmark failed on round %u\n", r + 1u);
            return 1;
        }
        t_mid_edits += (now_seconds() - start);

        start = now_seconds();
        if (!run_utf8_roundtrip(count))
        {
            fprintf(stderr, "utf8 benchmark failed on round %u\n", r + 1u);
            return 1;
        }
        t_utf8 += (now_seconds() - start);

        start = now_seconds();
        if (!run_clone_detach(count))
        {
            fprintf(stderr, "clone/detach benchmark failed on round %u\n", r + 1u);
            return 1;
        }
        t_clone_detach += (now_seconds() - start);

        start = now_seconds();
        if (!run_utf8_resolved(count))
        {
            fprintf(stderr, "resolved utf8 benchmark failed on round %u\n", r + 1u);
            return 1;
        }
        t_utf8_resolved += (now_seconds() - start);
    }

    printf("Text benchmark\n");
    printf("count=%u rounds=%u\n", count, rounds);
    print_metric("append+pop_front", t_append_pop, rounds, (double)count * 2.0);
    print_metric("mid set/ins/del", t_mid_edits, rounds, (double)count * 4.0);
    print_metric("utf8 roundtrip", t_utf8, rounds, (double)count * 3.0);
    print_metric("clone+detach(set)", t_clone_detach, rounds, (double)count * 2.0);
    print_metric("utf8 resolved", t_utf8_resolved, rounds, (double)count * 3.0);

    teardown_env();
    return 0;
}
