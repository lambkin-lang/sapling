/*
 * bench_text.c - throughput benchmark for text operations
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static double now_seconds(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}

static int parse_u32(const char *s, uint32_t *out)
{
    unsigned long v;
    char         *end = NULL;
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

static uint32_t pattern_ascii(uint32_t i)
{
    return 0x41u + (i % 26u);
}

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
    uint32_t       id;
    const uint8_t *utf8;
    size_t         utf8_len;
} BenchLiteralEntry;

typedef struct
{
    uint32_t    id;
    const Text *text;
} BenchTreeEntry;

typedef struct
{
    const BenchLiteralEntry *literals;
    size_t                   literal_count;
    const BenchTreeEntry    *trees;
    size_t                   tree_count;
} BenchResolverCtx;

static int bench_resolve_literal_utf8(uint32_t literal_id, const uint8_t **utf8_out,
                                      size_t *utf8_len_out, void *ctx)
{
    BenchResolverCtx *resolver = (BenchResolverCtx *)ctx;
    if (!resolver || !utf8_out || !utf8_len_out)
        return SEQ_INVALID;
    for (size_t i = 0; i < resolver->literal_count; i++)
    {
        if (resolver->literals[i].id != literal_id)
            continue;
        *utf8_out = resolver->literals[i].utf8;
        *utf8_len_out = resolver->literals[i].utf8_len;
        return SEQ_OK;
    }
    return SEQ_INVALID;
}

static int bench_resolve_tree_text(uint32_t tree_id, const Text **tree_out, void *ctx)
{
    BenchResolverCtx *resolver = (BenchResolverCtx *)ctx;
    if (!resolver || !tree_out)
        return SEQ_INVALID;
    for (size_t i = 0; i < resolver->tree_count; i++)
    {
        if (resolver->trees[i].id != tree_id)
            continue;
        *tree_out = resolver->trees[i].text;
        return SEQ_OK;
    }
    return SEQ_INVALID;
}

static int run_append_pop(uint32_t count)
{
    Text *text = text_new();
    if (!text)
        return 0;
    for (uint32_t i = 0; i < count; i++)
    {
        if (text_push_back(text, pattern_ascii(i)) != SEQ_OK)
            goto fail;
    }
    for (uint32_t i = 0; i < count; i++)
    {
        uint32_t out = 0;
        if (text_pop_front(text, &out) != SEQ_OK || out != pattern_ascii(i))
            goto fail;
    }
    text_free(text);
    return 1;

fail:
    text_free(text);
    return 0;
}

static int run_mid_edits(uint32_t count)
{
    Text *text = text_new();
    if (!text)
        return 0;

    for (uint32_t i = 0; i < count; i++)
    {
        if (text_push_back(text, pattern_multibyte(i)) != SEQ_OK)
            goto fail;
    }

    if (count > 0)
    {
        for (uint32_t i = 0; i < count; i++)
        {
            size_t idx = text_length(text) / 2u;
            if (text_set(text, idx, pattern_multibyte(i + 11u)) != SEQ_OK)
                goto fail;
            if (text_insert(text, idx, pattern_multibyte(i + 29u)) != SEQ_OK)
                goto fail;
            if (text_delete(text, idx + 1u, NULL) != SEQ_OK)
                goto fail;
        }
    }

    text_free(text);
    return 1;

fail:
    text_free(text);
    return 0;
}

static int run_utf8_roundtrip(uint32_t count)
{
    Text    *text = NULL;
    Text    *roundtrip = NULL;
    uint8_t *buf = NULL;
    size_t   need = 0;
    size_t   wrote = 0;

    text = text_new();
    roundtrip = text_new();
    if (!text || !roundtrip)
        goto fail;

    for (uint32_t i = 0; i < count; i++)
    {
        if (text_push_back(text, pattern_multibyte(i)) != SEQ_OK)
            goto fail;
    }

    if (text_utf8_length(text, &need) != SEQ_OK)
        goto fail;
    buf = (uint8_t *)malloc(need > 0 ? need : 1u);
    if (!buf)
        goto fail;
    if (text_to_utf8(text, buf, need, &wrote) != SEQ_OK || wrote != need)
        goto fail;
    if (text_from_utf8(roundtrip, buf, wrote) != SEQ_OK)
        goto fail;
    if (text_length(roundtrip) != text_length(text))
        goto fail;

    free(buf);
    text_free(text);
    text_free(roundtrip);
    return 1;

fail:
    free(buf);
    text_free(text);
    text_free(roundtrip);
    return 0;
}

static int run_clone_detach(uint32_t count)
{
    const uint32_t seed_len = 256u;
    Text          *base = NULL;

    base = text_new();
    if (!base)
        return 0;
    for (uint32_t i = 0; i < seed_len; i++)
    {
        if (text_push_back(base, pattern_multibyte(i)) != SEQ_OK)
            goto fail;
    }

    for (uint32_t i = 0; i < count; i++)
    {
        Text  *clone = text_clone(base);
        size_t idx = (size_t)(i % seed_len);
        if (!clone)
            goto fail;
        if (text_set(clone, idx, pattern_multibyte(i + 17u)) != SEQ_OK)
        {
            text_free(clone);
            goto fail;
        }
        text_free(clone);
    }

    if (text_length(base) != seed_len)
        goto fail;
    text_free(base);
    return 1;

fail:
    text_free(base);
    return 0;
}

static int run_utf8_resolved(uint32_t count)
{
    Text *root = NULL;
    Text *tree = NULL;
    uint8_t *buf = NULL;
    size_t   need = 0;
    size_t   wrote = 0;
    size_t   cp_len = 0;
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

    root = text_new();
    tree = text_new();
    if (!root || !tree)
        goto fail;

    if (text_push_back_handle(tree, text_handle_make(TEXT_HANDLE_LITERAL, 2u)) != SEQ_OK)
        goto fail;
    if (text_handle_from_codepoint((uint32_t)'!', &cp_handle) != SEQ_OK)
        goto fail;
    if (text_push_back_handle(tree, cp_handle) != SEQ_OK)
        goto fail;
    trees[0].text = tree;

    for (uint32_t i = 0; i < count; i++)
    {
        if (text_handle_from_codepoint(pattern_ascii(i), &cp_handle) != SEQ_OK)
            goto fail;
        if (text_push_back_handle(root, cp_handle) != SEQ_OK)
            goto fail;
        if (text_push_back_handle(root, text_handle_make(TEXT_HANDLE_LITERAL, 1u)) != SEQ_OK)
            goto fail;
        if (text_push_back_handle(root, text_handle_make(TEXT_HANDLE_TREE, 7u)) != SEQ_OK)
            goto fail;
    }

    if (text_codepoint_length_resolved(root, text_expand_runtime_handle, &resolver, &cp_len) !=
        SEQ_OK)
        goto fail;
    if (cp_len == 0u)
        goto fail;

    if (text_utf8_length_resolved(root, text_expand_runtime_handle, &resolver, &need) != SEQ_OK)
        goto fail;
    buf = (uint8_t *)malloc(need > 0u ? need : 1u);
    if (!buf)
        goto fail;
    if (text_to_utf8_resolved(root, text_expand_runtime_handle, &resolver, buf, need, &wrote) !=
            SEQ_OK ||
        wrote != need)
        goto fail;

    free(buf);
    text_free(root);
    text_free(tree);
    return 1;

fail:
    free(buf);
    text_free(root);
    text_free(tree);
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
    double   t_append_pop = 0.0;
    double   t_mid_edits = 0.0;
    double   t_utf8 = 0.0;
    double   t_clone_detach = 0.0;
    double   t_utf8_resolved = 0.0;

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
    return 0;
}
