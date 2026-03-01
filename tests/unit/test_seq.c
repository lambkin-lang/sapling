/*
 * test_seq.c — unit tests for the Sapling finger-tree sequence
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/seq.h"

#include "sapling/txn.h"
SapEnv *g_env = NULL;
SapTxnCtx *g_txn = NULL;

#define seq_new() seq_new(g_env)
#define seq_free(s) seq_free(g_env, s)
#define seq_push_back(s, v) seq_push_back(g_txn, s, v)
#define seq_push_front(s, v) seq_push_front(g_txn, s, v)
#define seq_pop_back(s, val) seq_pop_back(g_txn, s, val)
#define seq_pop_front(s, val) seq_pop_front(g_txn, s, val)
#define seq_concat(d, src) seq_concat(g_txn, d, src)
#define seq_split_at(s, i, l, r) seq_split_at(g_txn, s, i, l, r)
#define seq_reset(s) seq_reset(g_txn, s)

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================== */
/* Minimal test framework (matches style in test_sapling.c)            */
/* ================================================================== */

static int g_pass = 0, g_fail = 0;

#define CHECK(expr)                                                                                \
    do                                                                                             \
    {                                                                                              \
        if (expr)                                                                                  \
        {                                                                                          \
            g_pass++;                                                                              \
        }                                                                                          \
        else                                                                                       \
        {                                                                                          \
            fprintf(stderr, "FAIL: %s  (%s:%d)\n", #expr, __FILE__, __LINE__);                     \
            g_fail++;                                                                              \
        }                                                                                          \
    } while (0)

#define SECTION(name) printf("--- %s ---\n", name)

static void print_summary(void) { printf("\nResults: %d passed, %d failed\n", g_pass, g_fail); }

/* ================================================================== */
/* Helpers                                                              */
/* ================================================================== */

/*
 * Verify that seq contains exactly the values val[0..n-1] by indexed
 * lookup and by iterative pop-front.
 */
static int seq_equals_array(Seq *seq, uint32_t *val, size_t n)
{
    if (seq_length(seq) != n)
        return 0;
    for (size_t i = 0; i < n; i++)
    {
        uint32_t out = 0;
        if (seq_get(seq, i, &out) != ERR_OK)
            return 0;
        if (out != val[i])
            return 0;
    }
    return 1;
}

/* Build a seq from val[0..n-1] by push_back. */
static Seq *seq_from_array(uint32_t *val, size_t n)
{
    Seq *s = seq_new();
    assert(s);
    for (size_t i = 0; i < n; i++)
        assert(seq_push_back(s, val[i]) == ERR_OK);
    return s;
}

/* Convenience: cast int index to uint32_t handle */
static inline uint32_t ip(size_t i) { return (uint32_t)i; }

typedef struct
{
    uint32_t *data;
    size_t len;
    size_t cap;
} ModelVec;

static void model_init(ModelVec *m)
{
    m->data = NULL;
    m->len = 0;
    m->cap = 0;
}

static void model_free(ModelVec *m)
{
    free(m->data);
    m->data = NULL;
    m->len = 0;
    m->cap = 0;
}

static int model_reserve(ModelVec *m, size_t need)
{
    if (need <= m->cap)
        return 1;

    size_t new_cap = (m->cap > 0) ? m->cap : 16;
    while (new_cap < need)
    {
        if (new_cap > SIZE_MAX / 2)
            new_cap = need;
        else
            new_cap *= 2;
    }
    if (new_cap > SIZE_MAX / sizeof(uint32_t))
        return 0;

    uint32_t *next = (uint32_t *)realloc(m->data, new_cap * sizeof(uint32_t));
    if (!next)
        return 0;
    m->data = next;
    m->cap = new_cap;
    return 1;
}

static int model_push_back(ModelVec *m, uint32_t v)
{
    if (!model_reserve(m, m->len + 1))
        return 0;
    m->data[m->len++] = v;
    return 1;
}

static int model_push_front(ModelVec *m, uint32_t v)
{
    if (!model_reserve(m, m->len + 1))
        return 0;
    memmove(&m->data[1], &m->data[0], m->len * sizeof(uint32_t));
    m->data[0] = v;
    m->len++;
    return 1;
}

static int model_pop_back(ModelVec *m, uint32_t *out)
{
    if (m->len == 0)
        return 0;
    *out = m->data[m->len - 1];
    m->len--;
    return 1;
}

static int model_pop_front(ModelVec *m, uint32_t *out)
{
    if (m->len == 0)
        return 0;
    *out = m->data[0];
    memmove(&m->data[0], &m->data[1], (m->len - 1) * sizeof(uint32_t));
    m->len--;
    return 1;
}

static int model_concat(ModelVec *dst, const ModelVec *src)
{
    if (!model_reserve(dst, dst->len + src->len))
        return 0;
    if (src->len > 0)
        memcpy(&dst->data[dst->len], src->data, src->len * sizeof(uint32_t));
    dst->len += src->len;
    return 1;
}

static uint32_t prng_u32(uint64_t *state)
{
    uint64_t x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    return (uint32_t)((x * 2685821657736338717ULL) >> 32);
}

static int seq_matches_model(Seq *seq, const ModelVec *model)
{
    if (seq_length(seq) != model->len)
        return 0;
    for (size_t i = 0; i < model->len; i++)
    {
        uint32_t out = 0;
        if (seq_get(seq, i, &out) != ERR_OK)
            return 0;
        if (out != model->data[i])
            return 0;
    }
    return 1;
}

static int seq_matches_model_slice(Seq *seq, const ModelVec *model, size_t off, size_t n)
{
    if (seq_length(seq) != n)
        return 0;
    for (size_t i = 0; i < n; i++)
    {
        uint32_t out = 0;
        if (seq_get(seq, i, &out) != ERR_OK)
            return 0;
        if (out != model->data[off + i])
            return 0;
    }
    return 1;
}

/* ================================================================== */
/* Tests: empty / single                                                */
/* ================================================================== */

static void test_empty(void)
{
    SECTION("empty");
    Seq *s = seq_new();
    CHECK(s != NULL);
    CHECK(seq_length(s) == 0);

    uint32_t out = 0;
    CHECK(seq_pop_front(s, &out) == ERR_EMPTY);
    CHECK(seq_pop_back(s, &out) == ERR_EMPTY);
    CHECK(seq_get(s, 0, &out) == ERR_RANGE);

    seq_free(s);
}

static void test_single(void)
{
    SECTION("single element");
    Seq *s = seq_new();
    uint32_t ptr = ip(42);

    CHECK(seq_push_back(s, ptr) == ERR_OK);
    CHECK(seq_length(s) == 1);

    uint32_t out = 0;
    CHECK(seq_get(s, 0, &out) == ERR_OK);
    CHECK(out == ptr);

    CHECK(seq_get(s, 1, &out) == ERR_RANGE);

    seq_free(s);
}

/* ================================================================== */
/* Tests: push/pop invariants                                           */
/* ================================================================== */

static void test_push_pop_front(void)
{
    SECTION("push_front / pop_front");
    enum
    {
        N = 64
    };
    Seq *s = seq_new();

    /* Push 0..N-1 to the front; sequence should be N-1 .. 0 */
    for (size_t i = 0; i < N; i++)
        CHECK(seq_push_front(s, ip(i)) == ERR_OK);

    CHECK(seq_length(s) == N);

    for (size_t i = 0; i < N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_get(s, i, &out) == ERR_OK);
        CHECK(out == ip(N - 1 - i));
    }

    /* Pop from front; should come out N-1 down to 0 */
    for (size_t i = N; i > 0; i--)
    {
        uint32_t out = 0;
        CHECK(seq_pop_front(s, &out) == ERR_OK);
        CHECK(out == ip(i - 1));
    }
    CHECK(seq_length(s) == 0);
    seq_free(s);
}

static void test_push_pop_back(void)
{
    SECTION("push_back / pop_back");
    enum
    {
        N = 64
    };
    Seq *s = seq_new();

    /* Push 0..N-1 to the back */
    for (size_t i = 0; i < N; i++)
        CHECK(seq_push_back(s, ip(i)) == ERR_OK);

    CHECK(seq_length(s) == N);

    /* Pop from back; should come out N-1 down to 0 */
    for (size_t i = N; i > 0; i--)
    {
        uint32_t out = 0;
        CHECK(seq_pop_back(s, &out) == ERR_OK);
        CHECK(out == ip(i - 1));
    }
    CHECK(seq_length(s) == 0);
    seq_free(s);
}

static void test_alternating_push(void)
{
    SECTION("alternating push_front and push_back");
    /* Build [99,97,...,1,0,2,4,...,98] */
    enum
    {
        N = 100
    };
    Seq *s = seq_new();
    for (int i = 0; i < N; i++)
    {
        if (i % 2 == 0)
            seq_push_back(s, ip((size_t)i));
        else
            seq_push_front(s, ip((size_t)i));
    }
    CHECK(seq_length(s) == N);

    /* Reconstruct via pop_front to verify ordering */
    uint32_t popped[N];
    for (int i = 0; i < N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_pop_front(s, &out) == ERR_OK);
        popped[i] = out;
    }

    /* Front half (odd numbers pushed front in reverse order) */
    /* odd pushes: 1,3,5,...,99 pushed front → front has 99,97,...,1 */
    /* even pushes: 0,2,4,...,98 pushed back → back has 0,2,4,...,98 */
    /* full order: 99,97,...,3,1,0,2,4,...,98 */
    size_t j = 0;
    for (int k = N - 1; k >= 1; k -= 2)
        CHECK(popped[j++] == ip((size_t)k));
    for (int k = 0; k < N; k += 2)
        CHECK(popped[j++] == ip((size_t)k));

    CHECK(seq_length(s) == 0);
    seq_free(s);
}

/* ================================================================== */
/* Tests: get (indexing)                                                */
/* ================================================================== */

static void test_get(void)
{
    SECTION("get (indexing)");
    enum
    {
        N = 200
    };
    Seq *s = seq_new();
    for (size_t i = 0; i < N; i++)
        seq_push_back(s, ip(i));

    for (size_t i = 0; i < N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_get(s, i, &out) == ERR_OK);
        CHECK(out == ip(i));
    }
    CHECK(seq_get(s, N, &(uint32_t){0}) == ERR_RANGE);

    seq_free(s);
}

/* ================================================================== */
/* Tests: concat                                                        */
/* ================================================================== */

static void test_concat_basic(void)
{
    SECTION("concat basic");
    uint32_t a[] = {ip(0), ip(1), ip(2)};
    uint32_t b[] = {ip(3), ip(4), ip(5)};
    Seq *sa = seq_from_array(a, 3);
    Seq *sb = seq_from_array(b, 3);

    CHECK(seq_concat(sa, sb) == ERR_OK);
    CHECK(seq_length(sa) == 6);
    CHECK(seq_length(sb) == 0);

    uint32_t expect[] = {ip(0), ip(1), ip(2), ip(3), ip(4), ip(5)};
    CHECK(seq_equals_array(sa, expect, 6));

    seq_free(sa);
    seq_free(sb);
}

static void test_concat_empty(void)
{
    SECTION("concat with empty");
    uint32_t a[] = {ip(1), ip(2)};
    Seq *sa = seq_from_array(a, 2);
    Seq *empty = seq_new();

    /* concat(non_empty, empty) */
    CHECK(seq_concat(sa, empty) == ERR_OK);
    CHECK(seq_length(sa) == 2);

    /* concat(empty, non_empty) */
    Seq *sa2 = seq_from_array(a, 2);
    Seq *empty2 = seq_new();
    CHECK(seq_concat(empty2, sa2) == ERR_OK);
    CHECK(seq_length(empty2) == 2);
    uint32_t out = 0;
    CHECK(seq_get(empty2, 0, &out) == ERR_OK);
    CHECK(out == ip(1));

    seq_free(sa);
    seq_free(empty);
    seq_free(sa2);
    seq_free(empty2);
}

static void test_concat_self_invalid(void)
{
    SECTION("concat self invalid");
    uint32_t a[] = {ip(0), ip(1), ip(2), ip(3)};
    Seq *s = seq_from_array(a, 4);

    CHECK(seq_concat(s, s) == ERR_INVALID);
    CHECK(seq_length(s) == 4);
    CHECK(seq_equals_array(s, a, 4));

    seq_free(s);
}

static void test_concat_large(void)
{
    SECTION("concat large sequences");
    enum
    {
        N = 500
    };
    Seq *left = seq_new();
    Seq *right = seq_new();
    for (size_t i = 0; i < N; i++)
        seq_push_back(left, ip(i));
    for (size_t i = N; i < 2 * N; i++)
        seq_push_back(right, ip(i));

    CHECK(seq_concat(left, right) == ERR_OK);
    CHECK(seq_length(left) == 2 * N);

    for (size_t i = 0; i < 2 * N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_get(left, i, &out) == ERR_OK);
        CHECK(out == ip(i));
    }

    seq_free(left);
    seq_free(right);
}



/* ================================================================== */
/* Tests: split_at                                                      */
/* ================================================================== */

static void test_split_at_basic(void)
{
    SECTION("split_at basic");
    enum
    {
        N = 10
    };
    uint32_t vals[N];
    for (int i = 0; i < N; i++)
        vals[i] = ip((size_t)i);

    /* Split at every possible position */
    for (size_t split = 0; split <= N; split++)
    {
        Seq *s = seq_from_array(vals, N);
        Seq *l = NULL, *r = NULL;
        CHECK(seq_split_at(s, split, &l, &r) == ERR_OK);
        CHECK(seq_length(l) == split);
        CHECK(seq_length(r) == N - split);

        for (size_t i = 0; i < split; i++)
        {
            uint32_t out = 0;
            seq_get(l, i, &out);
            CHECK(out == ip(i));
        }
        for (size_t i = 0; i < N - split; i++)
        {
            uint32_t out = 0;
            seq_get(r, i, &out);
            CHECK(out == ip(split + i));
        }

        seq_free(s);
        seq_free(l);
        seq_free(r);
    }
}

static void test_split_at_large(void)
{
    SECTION("split_at large sequence");
    enum
    {
        N = 1000
    };
    Seq *s = seq_new();
    for (size_t i = 0; i < N; i++)
        seq_push_back(s, ip(i));

    size_t split = N / 3;
    Seq *l = NULL, *r = NULL;
    CHECK(seq_split_at(s, split, &l, &r) == ERR_OK);
    CHECK(seq_length(l) == split);
    CHECK(seq_length(r) == N - split);

    for (size_t i = 0; i < split; i++)
    {
        uint32_t out = 0;
        seq_get(l, i, &out);
        CHECK(out == ip(i));
    }
    for (size_t i = 0; i < N - split; i++)
    {
        uint32_t out = 0;
        seq_get(r, i, &out);
        CHECK(out == ip(split + i));
    }

    seq_free(s);
    seq_free(l);
    seq_free(r);
}

static void test_split_at_range(void)
{
    SECTION("split_at out-of-range");
    uint32_t a[] = {ip(1), ip(2)};
    Seq *s = seq_from_array(a, 2);
    Seq *l = NULL, *r = NULL;

    /* idx == length is valid (right side is empty) */
    CHECK(seq_split_at(s, 2, &l, &r) == ERR_OK);
    CHECK(seq_length(l) == 2);
    CHECK(seq_length(r) == 0);
    seq_free(l);
    seq_free(r);

    /* idx > length is invalid */
    Seq *s2 = seq_from_array(a, 2);
    CHECK(seq_split_at(s2, 3, &l, &r) == ERR_RANGE);
    seq_free(s2);
    seq_free(s);
}

/* ================================================================== */
/* Tests: large push/pop stress (exercises internal node cascade)      */
/* ================================================================== */

static void test_large_push_pop(void)
{
    SECTION("large push/pop stress");
    enum
    {
        N = 2000
    };
    Seq *s = seq_new();

    /* Push all to back */
    for (size_t i = 0; i < N; i++)
        seq_push_back(s, ip(i));
    CHECK(seq_length(s) == N);

    /* Verify get */
    for (size_t i = 0; i < N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_get(s, i, &out) == ERR_OK);
        CHECK(out == ip(i));
    }

    /* Pop all from front */
    for (size_t i = 0; i < N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_pop_front(s, &out) == ERR_OK);
        CHECK(out == ip(i));
    }
    CHECK(seq_length(s) == 0);
    seq_free(s);
}

static void test_large_push_front_pop_back(void)
{
    SECTION("push_front pop_back stress");
    enum
    {
        N = 1500
    };
    Seq *s = seq_new();

    /* Push 0..N-1 to front → sequence is N-1..0 */
    for (size_t i = 0; i < N; i++)
        seq_push_front(s, ip(i));
    CHECK(seq_length(s) == N);

    /* Pop from back → should produce 0, 1, 2, ... */
    for (size_t i = 0; i < N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_pop_back(s, &out) == ERR_OK);
        CHECK(out == ip(i));
    }
    CHECK(seq_length(s) == 0);
    seq_free(s);
}

/* ================================================================== */
/* Tests: concat then split round-trip                                  */
/* ================================================================== */

static void test_concat_split_roundtrip(void)
{
    SECTION("concat then split round-trip");
    enum
    {
        A = 37,
        B = 53
    };
    Seq *left = seq_new();
    Seq *right = seq_new();
    for (size_t i = 0; i < A; i++)
        seq_push_back(left, ip(i));
    for (size_t i = A; i < A + B; i++)
        seq_push_back(right, ip(i));

    /* Concat into one big sequence */
    CHECK(seq_concat(left, right) == ERR_OK);
    CHECK(seq_length(left) == A + B);

    /* Split it back at A */
    Seq *l2 = NULL, *r2 = NULL;
    CHECK(seq_split_at(left, A, &l2, &r2) == ERR_OK);
    CHECK(seq_length(l2) == A);
    CHECK(seq_length(r2) == B);

    for (size_t i = 0; i < A; i++)
    {
        uint32_t out = 0;
        seq_get(l2, i, &out);
        CHECK(out == ip(i));
    }
    for (size_t i = 0; i < B; i++)
    {
        uint32_t out = 0;
        seq_get(r2, i, &out);
        CHECK(out == ip(A + i));
    }

    seq_free(left);
    seq_free(right);
    seq_free(l2);
    seq_free(r2);
}

/* ================================================================== */
/* Tests: seq_free on non-empty (memory safety only — no assertion)    */
/* ================================================================== */

static void test_free_non_empty(void)
{
    SECTION("free non-empty sequence");
    enum
    {
        N = 300
    };
    Seq *s = seq_new();
    for (size_t i = 0; i < N; i++)
        seq_push_back(s, ip(i));
    /* Should not crash or leak (verified by Address Sanitizer) */
    seq_free(s);
    g_pass++; /* reaching here counts as passing */
}

/* ================================================================== */
/* Tests: mixed ops                                                     */
/* ================================================================== */

static void test_mixed_ops(void)
{
    SECTION("mixed push/pop/get");
    Seq *s = seq_new();

    /* Build [10,20,30,40,50] using mixed pushes */
    seq_push_back(s, ip(30));
    seq_push_front(s, ip(20));
    seq_push_front(s, ip(10));
    seq_push_back(s, ip(40));
    seq_push_back(s, ip(50));

    CHECK(seq_length(s) == 5);

    uint32_t out = 0;
    seq_get(s, 0, &out);
    CHECK(out == ip(10));
    seq_get(s, 2, &out);
    CHECK(out == ip(30));
    seq_get(s, 4, &out);
    CHECK(out == ip(50));

    /* Pop from both ends */
    seq_pop_front(s, &out);
    CHECK(out == ip(10));
    seq_pop_back(s, &out);
    CHECK(out == ip(50));
    CHECK(seq_length(s) == 3);

    seq_free(s);
}

/* ================================================================== */
/* Tests: repeated concat of many small seqs                           */
/* ================================================================== */

static void test_concat_many(void)
{
    SECTION("concat many small sequences");
    enum
    {
        SEQS = 20,
        PER = 10
    };
    Seq *acc = seq_new();
    for (int s = 0; s < SEQS; s++)
    {
        Seq *chunk = seq_new();
        for (int j = 0; j < PER; j++)
            seq_push_back(chunk, ip((size_t)(s * PER + j)));
        seq_concat(acc, chunk);
        seq_free(chunk);
    }
    CHECK(seq_length(acc) == SEQS * PER);
    for (size_t i = 0; i < SEQS * PER; i++)
    {
        uint32_t out = 0;
        seq_get(acc, i, &out);
        CHECK(out == ip(i));
    }
    seq_free(acc);
}

/* ================================================================== */
/* Tests: split_at then concat restores original                       */
/* ================================================================== */

static void test_split_concat_identity(void)
{
    SECTION("split then re-concat is identity");
    enum
    {
        N = 300
    };
    Seq *s = seq_new();
    for (size_t i = 0; i < N; i++)
        seq_push_back(s, ip(i));

    /* Pick an arbitrary split point */
    size_t mid = N * 2 / 3;
    Seq *l = NULL, *r = NULL;
    CHECK(seq_split_at(s, mid, &l, &r) == ERR_OK);

    /* Re-concat */
    CHECK(seq_concat(l, r) == ERR_OK);
    CHECK(seq_length(l) == N);

    for (size_t i = 0; i < N; i++)
    {
        uint32_t out = 0;
        seq_get(l, i, &out);
        CHECK(out == ip(i));
    }

    seq_free(s);
    seq_free(l);
    seq_free(r);
}

static void test_model_randomized(void)
{
    SECTION("model-based randomized operations");
    enum
    {
        RUNS = 6,
        OPS_PER_RUN = 12000,
        MAX_MODEL_LEN = 1024
    };

    for (int run = 0; run < RUNS; run++)
    {
        uint64_t seed = 0x9E3779B97F4A7C15ULL ^ ((uint64_t)(run + 1) * 0xD1B54A32D192ED03ULL);
        Seq *seq = seq_new();
        ModelVec model;

        model_init(&model);
        CHECK(seq != NULL);
        if (!seq)
        {
            model_free(&model);
            continue;
        }

        for (int step = 0; step < OPS_PER_RUN; step++)
        {
            uint32_t choice = prng_u32(&seed) % 12;
            if (model.len > MAX_MODEL_LEN)
                choice = 2 + (prng_u32(&seed) % 2); /* pop to keep size bounded */

            switch (choice)
            {
            case 0: /* push_front */
            {
                uint32_t v = prng_u32(&seed);
                CHECK(seq_push_front(seq, v) == ERR_OK);
                CHECK(model_push_front(&model, v));
                break;
            }
            case 1: /* push_back */
            {
                uint32_t v = prng_u32(&seed);
                CHECK(seq_push_back(seq, v) == ERR_OK);
                CHECK(model_push_back(&model, v));
                break;
            }
            case 2: /* pop_front */
            {
                uint32_t got = 0;
                uint32_t exp = 0;
                if (model.len == 0)
                {
                    CHECK(seq_pop_front(seq, &got) == ERR_EMPTY);
                }
                else
                {
                    CHECK(seq_pop_front(seq, &got) == ERR_OK);
                    CHECK(model_pop_front(&model, &exp));
                    CHECK(got == exp);
                }
                break;
            }
            case 3: /* pop_back */
            {
                uint32_t got = 0;
                uint32_t exp = 0;
                if (model.len == 0)
                {
                    CHECK(seq_pop_back(seq, &got) == ERR_EMPTY);
                }
                else
                {
                    CHECK(seq_pop_back(seq, &got) == ERR_OK);
                    CHECK(model_pop_back(&model, &exp));
                    CHECK(got == exp);
                }
                break;
            }
            case 4: /* get (in-range/out-of-range mix) */
            {
                uint32_t out = 0;
                if (model.len > 0 && (prng_u32(&seed) & 1u))
                {
                    size_t idx = (size_t)(prng_u32(&seed) % model.len);
                    CHECK(seq_get(seq, idx, &out) == ERR_OK);
                    CHECK(out == model.data[idx]);
                }
                else
                {
                    size_t idx = model.len + (size_t)(prng_u32(&seed) % 4u);
                    CHECK(seq_get(seq, idx, &out) == ERR_RANGE);
                }
                break;
            }
            case 5: /* split and re-concat into original seq */
            {
                size_t idx = (model.len == 0) ? 0 : (size_t)(prng_u32(&seed) % (model.len + 1));
                Seq *l = NULL;
                Seq *r = NULL;
                CHECK(seq_split_at(seq, idx, &l, &r) == ERR_OK);
                CHECK(l != NULL && r != NULL);
                if (l && r)
                {
                    CHECK(seq_length(seq) == 0);
                    CHECK(seq_matches_model_slice(l, &model, 0, idx));
                    CHECK(seq_matches_model_slice(r, &model, idx, model.len - idx));
                    CHECK(seq_concat(seq, l) == ERR_OK);
                    CHECK(seq_concat(seq, r) == ERR_OK);
                }
                seq_free(l);
                seq_free(r);
                break;
            }
            case 6: /* concat with random chunk */
            {
                Seq *chunk = seq_new();
                ModelVec chunk_model;
                size_t n = (size_t)(prng_u32(&seed) % 9u);

                model_init(&chunk_model);
                CHECK(chunk != NULL);
                if (!chunk)
                {
                    model_free(&chunk_model);
                    break;
                }

                for (size_t i = 0; i < n; i++)
                {
                    uint32_t v = prng_u32(&seed);
                    if (prng_u32(&seed) & 1u)
                    {
                        CHECK(seq_push_front(chunk, v) == ERR_OK);
                        CHECK(model_push_front(&chunk_model, v));
                    }
                    else
                    {
                        CHECK(seq_push_back(chunk, v) == ERR_OK);
                        CHECK(model_push_back(&chunk_model, v));
                    }
                }

                CHECK(seq_concat(seq, chunk) == ERR_OK);
                CHECK(model_concat(&model, &chunk_model));
                seq_free(chunk);
                model_free(&chunk_model);
                break;
            }
            case 7: /* reset */
                CHECK(seq_reset(seq) == ERR_OK);
                model.len = 0;
                break;
            case 8: /* split out-of-range */
            {
                Seq *l = (Seq *)(uintptr_t)1;
                Seq *r = (Seq *)(uintptr_t)2;
                CHECK(seq_split_at(seq, model.len + 1, &l, &r) == ERR_RANGE);
                CHECK(l == (Seq *)(uintptr_t)1);
                CHECK(r == (Seq *)(uintptr_t)2);
                break;
            }
            default: /* periodic full model check trigger */
                break;
            }

            if ((step % 64) == 0)
            {
                CHECK(seq_is_valid(seq) == 1);
                CHECK(seq_matches_model(seq, &model));
            }
        }

        CHECK(seq_is_valid(seq) == 1);
        CHECK(seq_matches_model(seq, &model));
        seq_free(seq);
        model_free(&model);
    }
}

static void test_invalid_args(void)
{
    SECTION("invalid argument handling");
    Seq *s = seq_new();
    uint32_t out = 0;
    Seq *l = NULL;
    Seq *r = NULL;

    CHECK(s != NULL);
    CHECK(seq_push_front(NULL, ip(1)) == ERR_INVALID);
    CHECK(seq_push_back(NULL, ip(1)) == ERR_INVALID);
    CHECK(seq_pop_front(NULL, &out) == ERR_INVALID);
    CHECK(seq_pop_back(NULL, &out) == ERR_INVALID);
    CHECK(seq_get(NULL, 0, &out) == ERR_INVALID);
    CHECK(seq_concat(NULL, s) == ERR_INVALID);
    CHECK(seq_concat(s, NULL) == ERR_INVALID);
    CHECK(seq_split_at(NULL, 0, &l, &r) == ERR_INVALID);
    CHECK(seq_split_at(s, 0, NULL, &r) == ERR_INVALID);
    CHECK(seq_split_at(s, 0, &l, NULL) == ERR_INVALID);
    CHECK(seq_pop_front(s, NULL) == ERR_INVALID);
    CHECK(seq_pop_back(s, NULL) == ERR_INVALID);
    CHECK(seq_get(s, 0, NULL) == ERR_INVALID);
    CHECK(seq_reset(NULL) == ERR_INVALID);
    CHECK(seq_is_valid(NULL) == 0);
    CHECK(seq_is_valid(s) == 1);

    seq_free(s);
}

#ifdef SAPLING_SEQ_TESTING
static void test_fault_injection_push(void)
{
    SECTION("fault injection: push oom marks invalid");
    Seq *s = seq_new();
    CHECK(s != NULL);
    CHECK(seq_is_valid(s) == 1);

    CHECK(seq_push_back(s, ip(1)) == ERR_OK);
    seq_test_fail_alloc_after(0);
    CHECK(seq_push_back(s, ip(2)) == ERR_OOM);
    seq_test_clear_alloc_fail();

    CHECK(seq_is_valid(s) == 0);
    CHECK(seq_push_back(s, ip(3)) == ERR_INVALID);
    CHECK(seq_reset(s) == ERR_OK);
    CHECK(seq_is_valid(s) == 1);
    CHECK(seq_length(s) == 0);

    seq_free(s);
}

static void test_fault_injection_concat(void)
{
    SECTION("fault injection: concat oom marks invalid");
    Seq *a = seq_new();
    Seq *b = seq_new();
    CHECK(a != NULL && b != NULL);
    CHECK(seq_push_back(a, ip(10)) == ERR_OK);
    CHECK(seq_push_back(b, ip(20)) == ERR_OK);

    seq_test_fail_alloc_after(0);
    CHECK(seq_concat(a, b) == ERR_OOM);
    seq_test_clear_alloc_fail();

    CHECK(seq_is_valid(a) == 0);
    CHECK(seq_is_valid(b) == 0);
    CHECK(seq_concat(a, b) == ERR_INVALID);

    CHECK(seq_reset(a) == ERR_OK);
    CHECK(seq_reset(b) == ERR_OK);
    CHECK(seq_is_valid(a) == 1);
    CHECK(seq_is_valid(b) == 1);

    seq_free(a);
    seq_free(b);
}

static void test_fault_injection_split(void)
{
    SECTION("fault injection: split oom marks invalid");
    Seq *s = seq_new();
    CHECK(s != NULL);
    CHECK(seq_push_back(s, ip(0)) == ERR_OK);
    CHECK(seq_push_back(s, ip(1)) == ERR_OK);
    CHECK(seq_push_back(s, ip(2)) == ERR_OK);

    Seq *l = (Seq *)(uintptr_t)1;
    Seq *r = (Seq *)(uintptr_t)2;
    /*
     * Fail after split has allocated temporary Seq wrappers so we exercise
     * internal split-tree allocation failure (which poisons seq).
     */
    seq_test_fail_alloc_after(4);
    CHECK(seq_split_at(s, 1, &l, &r) == ERR_OOM);
    seq_test_clear_alloc_fail();

    CHECK(l == (Seq *)(uintptr_t)1);
    CHECK(r == (Seq *)(uintptr_t)2);
    CHECK(seq_is_valid(s) == 0);
    CHECK(seq_split_at(s, 0, &l, &r) == ERR_INVALID);

    CHECK(seq_reset(s) == ERR_OK);
    CHECK(seq_is_valid(s) == 1);
    CHECK(seq_length(s) == 0);

    seq_free(s);
}

static void test_fault_injection_push_sweep(void)
{
    SECTION("fault injection: push sweep");
    int saw_oom = 0;
    int saw_ok = 0;
    for (int64_t fail_after = 0; fail_after <= 8; fail_after++)
    {
        Seq *s = seq_new();
        CHECK(s != NULL);
        CHECK(seq_push_back(s, ip(1)) == ERR_OK);

        seq_test_fail_alloc_after(fail_after);
        int rc = seq_push_back(s, ip(2));
        seq_test_clear_alloc_fail();

        if (rc == ERR_OOM)
        {
            saw_oom = 1;
            CHECK(seq_is_valid(s) == 0);
            CHECK(seq_reset(s) == ERR_OK);
            CHECK(seq_is_valid(s) == 1);
            CHECK(seq_length(s) == 0);
        }
        else if (rc == ERR_OK)
        {
            uint32_t out = 0;
            saw_ok = 1;
            CHECK(seq_is_valid(s) == 1);
            CHECK(seq_length(s) == 2);
            CHECK(seq_get(s, 0, &out) == ERR_OK);
            CHECK(out == ip(1));
            CHECK(seq_get(s, 1, &out) == ERR_OK);
            CHECK(out == ip(2));
        }
        else
        {
            CHECK(0);
        }
        seq_free(s);
    }
    CHECK(saw_oom == 1);
    CHECK(saw_ok == 1);
}

static void test_fault_injection_concat_sweep(void)
{
    SECTION("fault injection: concat sweep");
    int saw_oom = 0;
    int saw_ok = 0;
    for (int64_t fail_after = 0; fail_after <= 64; fail_after++)
    {
        Seq *a = seq_new();
        Seq *b = seq_new();
        CHECK(a != NULL && b != NULL);
        for (size_t i = 0; i < 32; i++)
            CHECK(seq_push_back(a, ip(i)) == ERR_OK);
        for (size_t i = 32; i < 64; i++)
            CHECK(seq_push_back(b, ip(i)) == ERR_OK);

        seq_test_fail_alloc_after(fail_after);
        int rc = seq_concat(a, b);
        seq_test_clear_alloc_fail();

        if (rc == ERR_OOM)
        {
            saw_oom = 1;
            CHECK(seq_is_valid(a) == 0 || seq_is_valid(b) == 0);
            CHECK(seq_reset(a) == ERR_OK);
            CHECK(seq_reset(b) == ERR_OK);
            CHECK(seq_is_valid(a) == 1);
            CHECK(seq_is_valid(b) == 1);
        }
        else if (rc == ERR_OK)
        {
            uint32_t out = 0;
            saw_ok = 1;
            CHECK(seq_length(a) == 64);
            CHECK(seq_length(b) == 0);
            CHECK(seq_get(a, 0, &out) == ERR_OK);
            CHECK(out == ip(0));
            CHECK(seq_get(a, 63, &out) == ERR_OK);
            CHECK(out == ip(63));
        }
        else
        {
            CHECK(0);
        }

        seq_free(a);
        seq_free(b);
    }
    CHECK(saw_oom == 1);
    CHECK(saw_ok == 1);
}

static void test_fault_injection_push_front_sweep(void)
{
    SECTION("fault injection: push_front sweep");
    int saw_oom = 0;
    int saw_ok = 0;
    for (int64_t fail_after = 0; fail_after <= 8; fail_after++)
    {
        Seq *s = seq_new();
        CHECK(s != NULL);
        CHECK(seq_push_front(s, ip(1)) == ERR_OK);

        seq_test_fail_alloc_after(fail_after);
        int rc = seq_push_front(s, ip(2));
        seq_test_clear_alloc_fail();

        if (rc == ERR_OOM)
        {
            saw_oom = 1;
            CHECK(seq_is_valid(s) == 0);
            CHECK(seq_reset(s) == ERR_OK);
            CHECK(seq_is_valid(s) == 1);
            CHECK(seq_length(s) == 0);
        }
        else if (rc == ERR_OK)
        {
            uint32_t out = 0;
            saw_ok = 1;
            CHECK(seq_is_valid(s) == 1);
            CHECK(seq_length(s) == 2);
            CHECK(seq_get(s, 0, &out) == ERR_OK);
            CHECK(out == ip(2));
            CHECK(seq_get(s, 1, &out) == ERR_OK);
            CHECK(out == ip(1));
        }
        else
        {
            CHECK(0);
        }
        seq_free(s);
    }
    CHECK(saw_oom == 1);
    CHECK(saw_ok == 1);
}

static void test_fault_injection_split_sweep(void)
{
    SECTION("fault injection: split sweep");
    int saw_oom = 0;
    int saw_ok = 0;
    for (int64_t fail_after = 0; fail_after <= 64; fail_after++)
    {
        Seq *s = seq_new();
        CHECK(s != NULL);
        for (size_t i = 0; i < 24; i++)
            CHECK(seq_push_back(s, ip(i)) == ERR_OK);

        Seq *l = (Seq *)(uintptr_t)11;
        Seq *r = (Seq *)(uintptr_t)22;
        seq_test_fail_alloc_after(fail_after);
        int rc = seq_split_at(s, 11, &l, &r);
        seq_test_clear_alloc_fail();

        if (rc == ERR_OOM)
        {
            uint32_t out = 0;
            saw_oom = 1;
            CHECK(l == (Seq *)(uintptr_t)11);
            CHECK(r == (Seq *)(uintptr_t)22);
            if (seq_is_valid(s) == 0)
            {
                CHECK(seq_reset(s) == ERR_OK);
                CHECK(seq_length(s) == 0);
            }
            else
            {
                CHECK(seq_length(s) == 24);
                CHECK(seq_get(s, 0, &out) == ERR_OK);
                CHECK(out == ip(0));
                CHECK(seq_get(s, 23, &out) == ERR_OK);
                CHECK(out == ip(23));
            }
        }
        else if (rc == ERR_OK)
        {
            uint32_t out = 0;
            saw_ok = 1;
            CHECK(seq_is_valid(s) == 1);
            CHECK(seq_length(s) == 0);
            CHECK(seq_length(l) == 11);
            CHECK(seq_length(r) == 13);
            CHECK(seq_get(l, 0, &out) == ERR_OK);
            CHECK(out == ip(0));
            CHECK(seq_get(l, 10, &out) == ERR_OK);
            CHECK(out == ip(10));
            CHECK(seq_get(r, 0, &out) == ERR_OK);
            CHECK(out == ip(11));
            CHECK(seq_get(r, 12, &out) == ERR_OK);
            CHECK(out == ip(23));
            seq_free(l);
            seq_free(r);
        }
        else
        {
            CHECK(0);
        }

        seq_free(s);
    }
    CHECK(saw_oom == 1);
    CHECK(saw_ok == 1);
}

static void test_fault_injection_reset_sweep(void)
{
    SECTION("fault injection: reset sweep");
    int saw_oom = 0;
    int saw_ok = 0;
    for (int64_t fail_after = 0; fail_after <= 4; fail_after++)
    {
        Seq *s = seq_new();
        CHECK(s != NULL);
        CHECK(seq_push_back(s, ip(7)) == ERR_OK);

        seq_test_fail_alloc_after(fail_after);
        int rc = seq_reset(s);
        seq_test_clear_alloc_fail();

        if (rc == ERR_OOM)
        {
            saw_oom = 1;
            CHECK(seq_is_valid(s) == 0);
            CHECK(seq_reset(s) == ERR_OK);
            CHECK(seq_is_valid(s) == 1);
            CHECK(seq_length(s) == 0);
        }
        else if (rc == ERR_OK)
        {
            saw_ok = 1;
            CHECK(seq_is_valid(s) == 1);
            CHECK(seq_length(s) == 0);
        }
        else
        {
            CHECK(0);
        }

        seq_free(s);
    }
    CHECK(saw_oom == 1);
    CHECK(saw_ok == 1);
}
#endif

/* ================================================================== */
/* main                                                                 */
/* ================================================================== */

int main(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions arena_opts = { .type = SAP_ARENA_BACKING_MALLOC, .page_size = 4096 };
    sap_arena_init(&arena, &arena_opts);
    g_env = sap_env_create(arena, 4096);
    sap_seq_subsystem_init(g_env);
    g_txn = sap_txn_begin(g_env, NULL, 0);

    printf("=== seq unit tests ===\n");

    test_empty();
    test_single();
    test_push_pop_front();
    test_push_pop_back();
    test_alternating_push();
    test_get();
    test_concat_basic();
    test_concat_empty();
    test_concat_self_invalid();
    test_concat_large();





    test_split_at_basic();
    test_split_at_large();
    test_split_at_range();
    test_large_push_pop();
    test_large_push_front_pop_back();
    test_concat_split_roundtrip();
    test_free_non_empty();
    test_mixed_ops();
    test_concat_many();
    test_split_concat_identity();
    test_model_randomized();
    test_invalid_args();
#ifdef SAPLING_SEQ_TESTING
    test_fault_injection_push();
    test_fault_injection_concat();
    test_fault_injection_split();
    test_fault_injection_push_sweep();
    test_fault_injection_push_front_sweep();
    test_fault_injection_concat_sweep();
    test_fault_injection_split_sweep();
    test_fault_injection_reset_sweep();
#endif

    print_summary();
    return g_fail ? 1 : 0;
}
