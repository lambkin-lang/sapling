/*
 * bench_sapling.c - simple throughput benchmark for sorted loading paths
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/sapling.h"
#include "sapling/txn.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

static void *bench_alloc(void *ctx, uint32_t sz)
{
    (void)ctx;
    return malloc((size_t)sz);
}

static void bench_free(void *ctx, void *p, uint32_t sz)
{
    (void)ctx;
    (void)sz;
    free(p);
}

static SapMemArena *g_alloc = NULL;

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

static int make_dataset(uint32_t start, uint32_t count, const void ***keys_out,
                        uint32_t **key_lens_out, const void ***vals_out, uint32_t **val_lens_out,
                        char **key_buf_out, char **val_buf_out)
{
    const uint32_t key_len = 11; /* k + 10 digits */
    const uint32_t val_len = 11; /* v + 10 digits */
    const size_t key_stride = (size_t)key_len + 1u;
    const size_t val_stride = (size_t)val_len + 1u;
    const size_t key_bytes = (size_t)count * key_stride;
    const size_t val_bytes = (size_t)count * val_stride;

    const void **keys = (const void **)malloc((size_t)count * sizeof(*keys));
    const void **vals = (const void **)malloc((size_t)count * sizeof(*vals));
    uint32_t *key_lens = (uint32_t *)malloc((size_t)count * sizeof(*key_lens));
    uint32_t *val_lens = (uint32_t *)malloc((size_t)count * sizeof(*val_lens));
    char *key_buf = (char *)malloc(key_bytes);
    char *val_buf = (char *)malloc(val_bytes);

    if (!keys || !vals || !key_lens || !val_lens || !key_buf || !val_buf)
    {
        free(keys);
        free(vals);
        free(key_lens);
        free(val_lens);
        free(key_buf);
        free(val_buf);
        return 0;
    }

    for (uint32_t i = 0; i < count; i++)
    {
        uint32_t id = start + i;
        char *k = key_buf + (size_t)i * key_stride;
        char *v = val_buf + (size_t)i * val_stride;
        (void)snprintf(k, key_len + 1u, "k%010u", id);
        (void)snprintf(v, val_len + 1u, "v%010u", id);
        keys[i] = k;
        vals[i] = v;
        key_lens[i] = key_len;
        val_lens[i] = val_len;
    }

    *keys_out = keys;
    *vals_out = vals;
    *key_lens_out = key_lens;
    *val_lens_out = val_lens;
    *key_buf_out = key_buf;
    *val_buf_out = val_buf;
    return 1;
}

static void free_dataset(const void *const *keys, uint32_t *key_lens, const void *const *vals,
                         uint32_t *val_lens, char *key_buf, char *val_buf)
{
    free((void *)keys);
    free(key_lens);
    free((void *)vals);
    free(val_lens);
    free(key_buf);
    free(val_buf);
}

static DB *create_bench_db(void)
{
    SapEnv *env = sap_env_create(g_alloc, SAPLING_PAGE_SIZE);
    if (!env) return NULL;
    /* Initialize B-Tree subsystem */
    if (sap_btree_subsystem_init(env, NULL, NULL) != SAP_OK) {
        sap_env_destroy(env);
        return NULL;
    }
    return (DB *)env;
}

static int run_put_sorted(uint32_t count, const void *const *keys, const uint32_t *key_lens,
                          const void *const *vals, const uint32_t *val_lens)
{
    DB *db = create_bench_db();
    if (!db)
        return 0;

    Txn *txn = txn_begin(db, NULL, 0);
    if (!txn)
    {
        db_close(db);
        return 0;
    }

    for (uint32_t i = 0; i < count; i++)
    {
        int rc = txn_put_dbi(txn, 0, keys[i], key_lens[i], vals[i], val_lens[i]);
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            db_close(db);
            return 0;
        }
    }

    if (txn_commit(txn) != SAP_OK)
    {
        db_close(db);
        return 0;
    }

    db_close(db);
    return 1;
}

static int run_load_sorted(uint32_t count, const void *const *keys, const uint32_t *key_lens,
                           const void *const *vals, const uint32_t *val_lens)
{
    DB *db = create_bench_db();
    if (!db)
        return 0;

    Txn *txn = txn_begin(db, NULL, 0);
    if (!txn)
    {
        db_close(db);
        return 0;
    }

    if (txn_load_sorted(txn, 0, keys, key_lens, vals, val_lens, count) != SAP_OK)
    {
        txn_abort(txn);
        db_close(db);
        return 0;
    }

    if (txn_commit(txn) != SAP_OK)
    {
        db_close(db);
        return 0;
    }

    db_close(db);
    return 1;
}

static int preload_sorted(DB *db, uint32_t count, const void *const *keys, const uint32_t *key_lens,
                          const void *const *vals, const uint32_t *val_lens)
{
    Txn *txn = txn_begin(db, NULL, 0);
    if (!txn)
        return 0;
    if (txn_load_sorted(txn, 0, keys, key_lens, vals, val_lens, count) != SAP_OK)
    {
        txn_abort(txn);
        return 0;
    }
    return txn_commit(txn) == SAP_OK;
}

static int run_put_sorted_nonempty(uint32_t base_count, uint32_t delta_count,
                                   const void *const *base_keys, const uint32_t *base_key_lens,
                                   const void *const *base_vals, const uint32_t *base_val_lens,
                                   const void *const *delta_keys, const uint32_t *delta_key_lens,
                                   const void *const *delta_vals, const uint32_t *delta_val_lens)
{
    DB *db = create_bench_db();
    if (!db)
        return 0;
    if (!preload_sorted(db, base_count, base_keys, base_key_lens, base_vals, base_val_lens))
    {
        db_close(db);
        return 0;
    }

    Txn *txn = txn_begin(db, NULL, 0);
    if (!txn)
    {
        db_close(db);
        return 0;
    }
    for (uint32_t i = 0; i < delta_count; i++)
    {
        int rc =
            txn_put_dbi(txn, 0, delta_keys[i], delta_key_lens[i], delta_vals[i], delta_val_lens[i]);
        if (rc != SAP_OK)
        {
            txn_abort(txn);
            db_close(db);
            return 0;
        }
    }
    if (txn_commit(txn) != SAP_OK)
    {
        db_close(db);
        return 0;
    }
    db_close(db);
    return 1;
}

static int run_load_sorted_nonempty(uint32_t base_count, uint32_t delta_count,
                                    const void *const *base_keys, const uint32_t *base_key_lens,
                                    const void *const *base_vals, const uint32_t *base_val_lens,
                                    const void *const *delta_keys, const uint32_t *delta_key_lens,
                                    const void *const *delta_vals, const uint32_t *delta_val_lens)
{
    DB *db = create_bench_db();
    if (!db)
        return 0;
    if (!preload_sorted(db, base_count, base_keys, base_key_lens, base_vals, base_val_lens))
    {
        db_close(db);
        return 0;
    }

    Txn *txn = txn_begin(db, NULL, 0);
    if (!txn)
    {
        db_close(db);
        return 0;
    }
    if (txn_load_sorted(txn, 0, delta_keys, delta_key_lens, delta_vals, delta_val_lens,
                        delta_count) != SAP_OK)
    {
        txn_abort(txn);
        db_close(db);
        return 0;
    }
    if (txn_commit(txn) != SAP_OK)
    {
        db_close(db);
        return 0;
    }
    db_close(db);
    return 1;
}

int main(int argc, char **argv)
{
    SapArenaOptions g_alloc_opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .cfg.custom.alloc_page = bench_alloc,
        .cfg.custom.free_page = bench_free,
        .cfg.custom.ctx = NULL
    };
    sap_arena_init(&g_alloc, &g_alloc_opts);

    uint32_t count = 100000;
    uint32_t rounds = 3;
    uint32_t delta_start;
    const void **keys = NULL, **vals = NULL;
    const void **delta_keys = NULL, **delta_vals = NULL;
    uint32_t *key_lens = NULL, *val_lens = NULL;
    uint32_t *delta_key_lens = NULL, *delta_val_lens = NULL;
    char *key_buf = NULL, *val_buf = NULL;
    char *delta_key_buf = NULL, *delta_val_buf = NULL;
    double put_total = 0.0;
    double load_total = 0.0;
    double put_nonempty_total = 0.0;
    double load_nonempty_total = 0.0;

    for (int i = 1; i < argc; i++)
    {
        if (strcmp(argv[i], "--count") == 0 && i + 1 < argc)
        {
            if (!parse_u32(argv[++i], &count) || count == 0)
            {
                fprintf(stderr, "invalid --count\n");
                return 2;
            }
        }
        else if (strcmp(argv[i], "--rounds") == 0 && i + 1 < argc)
        {
            if (!parse_u32(argv[++i], &rounds) || rounds == 0)
            {
                fprintf(stderr, "invalid --rounds\n");
                return 2;
            }
        }
        else
        {
            fprintf(stderr, "usage: %s [--count N] [--rounds R]\n", argv[0]);
            return 2;
        }
    }

    delta_start = count / 2u;
    if (!make_dataset(0, count, &keys, &key_lens, &vals, &val_lens, &key_buf, &val_buf))
    {
        fprintf(stderr, "failed to allocate benchmark dataset\n");
        return 1;
    }
    if (!make_dataset(delta_start, count, &delta_keys, &delta_key_lens, &delta_vals,
                      &delta_val_lens, &delta_key_buf, &delta_val_buf))
    {
        fprintf(stderr, "failed to allocate delta benchmark dataset\n");
        free_dataset(keys, key_lens, vals, val_lens, key_buf, val_buf);
        return 1;
    }

    for (uint32_t r = 0; r < rounds; r++)
    {
        double t0 = now_seconds();
        if (!run_put_sorted(count, keys, key_lens, vals, val_lens))
        {
            fprintf(stderr, "txn_put_dbi benchmark failed on round %u\n", r + 1);
            free_dataset(delta_keys, delta_key_lens, delta_vals, delta_val_lens, delta_key_buf,
                         delta_val_buf);
            free_dataset(keys, key_lens, vals, val_lens, key_buf, val_buf);
            return 1;
        }
        put_total += now_seconds() - t0;
    }

    for (uint32_t r = 0; r < rounds; r++)
    {
        double t0 = now_seconds();
        if (!run_load_sorted(count, keys, key_lens, vals, val_lens))
        {
            fprintf(stderr, "txn_load_sorted benchmark failed on round %u\n", r + 1);
            free_dataset(delta_keys, delta_key_lens, delta_vals, delta_val_lens, delta_key_buf,
                         delta_val_buf);
            free_dataset(keys, key_lens, vals, val_lens, key_buf, val_buf);
            return 1;
        }
        load_total += now_seconds() - t0;
    }

    for (uint32_t r = 0; r < rounds; r++)
    {
        double t0 = now_seconds();
        if (!run_put_sorted_nonempty(count, count, keys, key_lens, vals, val_lens, delta_keys,
                                     delta_key_lens, delta_vals, delta_val_lens))
        {
            fprintf(stderr, "txn_put_dbi nonempty benchmark failed on round %u\n", r + 1);
            free_dataset(delta_keys, delta_key_lens, delta_vals, delta_val_lens, delta_key_buf,
                         delta_val_buf);
            free_dataset(keys, key_lens, vals, val_lens, key_buf, val_buf);
            return 1;
        }
        put_nonempty_total += now_seconds() - t0;
    }

    for (uint32_t r = 0; r < rounds; r++)
    {
        double t0 = now_seconds();
        if (!run_load_sorted_nonempty(count, count, keys, key_lens, vals, val_lens, delta_keys,
                                      delta_key_lens, delta_vals, delta_val_lens))
        {
            fprintf(stderr, "txn_load_sorted nonempty benchmark failed on round %u\n", r + 1);
            free_dataset(delta_keys, delta_key_lens, delta_vals, delta_val_lens, delta_key_buf,
                         delta_val_buf);
            free_dataset(keys, key_lens, vals, val_lens, key_buf, val_buf);
            return 1;
        }
        load_nonempty_total += now_seconds() - t0;
    }

    free_dataset(delta_keys, delta_key_lens, delta_vals, delta_val_lens, delta_key_buf,
                 delta_val_buf);
    free_dataset(keys, key_lens, vals, val_lens, key_buf, val_buf);

    {
        double put_avg = put_total / (double)rounds;
        double load_avg = load_total / (double)rounds;
        double put_ops = (double)count / put_avg;
        double load_ops = (double)count / load_avg;
        double speedup = put_avg / load_avg;
        double put_nonempty_avg = put_nonempty_total / (double)rounds;
        double load_nonempty_avg = load_nonempty_total / (double)rounds;
        double put_nonempty_ops = (double)count / put_nonempty_avg;
        double load_nonempty_ops = (double)count / load_nonempty_avg;
        double nonempty_speedup = put_nonempty_avg / load_nonempty_avg;

        printf("Sapling sorted-load benchmark\n");
        printf("count=%u rounds=%u page_size=%u\n", count, rounds, (unsigned)SAPLING_PAGE_SIZE);
        printf("txn_put_dbi(sorted):   %.6f s avg  (%.2f ops/s)\n", put_avg, put_ops);
        printf("txn_load_sorted:       %.6f s avg  (%.2f ops/s)\n", load_avg, load_ops);
        printf("speedup(load/put):     %.2fx\n", speedup);
        printf("txn_put_dbi(nonempty): %.6f s avg  (%.2f ops/s)\n", put_nonempty_avg,
               put_nonempty_ops);
        printf("txn_load_nonempty:     %.6f s avg  (%.2f ops/s)\n", load_nonempty_avg,
               load_nonempty_ops);
        printf("speedup(nonempty):     %.2fx\n", nonempty_speedup);
    }

    return 0;
}
