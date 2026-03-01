/*
 * bench_seq.c - throughput benchmark for seq operations
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/seq.h"
#include "sapling/sapling.h"
#include "sapling/txn.h"
#include "sapling/arena.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static SapMemArena *g_arena = NULL;
static SapEnv *g_env = NULL;

static void setup_env(void)
{
    SapArenaOptions opts = {0};
    opts.type = SAP_ARENA_BACKING_MALLOC;
    opts.page_size = 4096;
    if (sap_arena_init(&g_arena, &opts) != ERR_OK)
    {
        fprintf(stderr, "failed to init arena\n");
        exit(1);
    }
    g_env = sap_env_create(g_arena, 4096);
    if (!g_env)
    {
        fprintf(stderr, "failed to create env\n");
        exit(1);
    }
}

static void teardown_env(void)
{
    if (g_env) sap_env_destroy(g_env);
    if (g_arena) sap_arena_destroy(g_arena);
    g_env = NULL;
    g_arena = NULL;
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

static uint32_t pattern_u32(uint32_t i) { return (i * 2654435761u) ^ 0x9E3779B9u; }

static int run_push_pop(uint32_t count)
{
    Seq *s = seq_new(g_env);
    if (!s)
        return 0;

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    if (!txn) { seq_free(g_env, s); return 0; }

    for (uint32_t i = 0; i < count; i++)
    {
        if (seq_push_back(txn, s, i) != ERR_OK)
            goto fail;
    }
    for (uint32_t i = 0; i < count; i++)
    {
        uint32_t out = 0;
        if (seq_pop_front(txn, s, &out) != ERR_OK || out != i)
            goto fail;
    }
    sap_txn_commit(txn);
    seq_free(g_env, s);
    return 1;

fail:
    sap_txn_abort(txn);
    seq_free(g_env, s);
    return 0;
}

static int run_mixed(uint32_t count)
{
    Seq *s = seq_new(g_env);
    if (!s)
        return 0;

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    if (!txn) { seq_free(g_env, s); return 0; }

    for (uint32_t i = 0; i < count; i++)
    {
        uint32_t v = pattern_u32(i);
        if ((i & 1u) == 0)
        {
            if (seq_push_front(txn, s, v) != ERR_OK)
                goto fail;
        }
        else
        {
            if (seq_push_back(txn, s, v) != ERR_OK)
                goto fail;
        }
    }

    for (uint32_t i = 0; i < count; i++)
    {
        uint32_t out = 0;
        if (seq_get(s, i, &out) != ERR_OK)
            goto fail;
    }

    while (seq_length(s) > 0)
    {
        uint32_t out = 0;
        if (seq_pop_back(txn, s, &out) != ERR_OK)
            goto fail;
    }

    sap_txn_commit(txn);
    seq_free(g_env, s);
    return 1;

fail:
    sap_txn_abort(txn);
    seq_free(g_env, s);
    return 0;
}

static int run_concat_split(uint32_t count)
{
    Seq *left = NULL;
    Seq *right = NULL;
    Seq *a = NULL;
    Seq *b = NULL;
    uint32_t left_count = count / 2u;
    uint32_t right_count = count - left_count;

    left = seq_new(g_env);
    right = seq_new(g_env);
    if (!left || !right)
        goto fail_alloc;

    SapTxnCtx *txn = sap_txn_begin(g_env, NULL, 0);
    if (!txn)
        goto fail_alloc;

    for (uint32_t i = 0; i < left_count; i++)
    {
        if (seq_push_back(txn, left, pattern_u32(i)) != ERR_OK)
            goto fail;
    }
    for (uint32_t i = 0; i < right_count; i++)
    {
        if (seq_push_back(txn, right, pattern_u32(left_count + i)) != ERR_OK)
            goto fail;
    }

    if (seq_concat(txn, left, right) != ERR_OK)
        goto fail;
    if (seq_length(left) != count || seq_length(right) != 0)
        goto fail;

    if (seq_split_at(txn, left, left_count, &a, &b) != ERR_OK)
        goto fail;
    if (seq_length(a) != left_count || seq_length(b) != right_count)
        goto fail;

    if (count > 0)
    {
        uint32_t out = 0;
        if (left_count > 0)
        {
            if (seq_get(a, 0, &out) != ERR_OK || out != pattern_u32(0))
                goto fail;
            if (seq_get(a, left_count - 1u, &out) != ERR_OK || out != pattern_u32(left_count - 1u))
                goto fail;
        }
        if (right_count > 0)
        {
            if (seq_get(b, 0, &out) != ERR_OK || out != pattern_u32(left_count))
                goto fail;
            if (seq_get(b, right_count - 1u, &out) != ERR_OK || out != pattern_u32(count - 1u))
                goto fail;
        }
    }

    sap_txn_commit(txn);
    seq_free(g_env, a);
    seq_free(g_env, b);
    seq_free(g_env, left);
    seq_free(g_env, right);
    return 1;

fail:
    sap_txn_abort(txn);
fail_alloc:
    if (a) seq_free(g_env, a);
    if (b) seq_free(g_env, b);
    if (left) seq_free(g_env, left);
    if (right) seq_free(g_env, right);
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
    double t_push_pop = 0.0;
    double t_mixed = 0.0;
    double t_concat_split = 0.0;

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
        if (!run_push_pop(count))
        {
            fprintf(stderr, "push/pop benchmark failed on round %u\n", r + 1u);
            return 1;
        }
        t_push_pop += (now_seconds() - start);

        start = now_seconds();
        if (!run_mixed(count))
        {
            fprintf(stderr, "mixed benchmark failed on round %u\n", r + 1u);
            return 1;
        }
        t_mixed += (now_seconds() - start);

        start = now_seconds();
        if (!run_concat_split(count))
        {
            fprintf(stderr, "concat/split benchmark failed on round %u\n", r + 1u);
            return 1;
        }
        t_concat_split += (now_seconds() - start);
    }

    printf("Seq benchmark\n");
    printf("count=%u rounds=%u\n", count, rounds);
    print_metric("push_back+pop_front", t_push_pop, rounds, (double)count * 2.0);
    print_metric("mixed push/get/pop", t_mixed, rounds, (double)count * 3.0);
    print_metric("concat+split", t_concat_split, rounds, (double)count * 2.0);
    
    teardown_env();
    return 0;
}
