/*
 * test_seq.c — unit tests for the Sapling finger-tree sequence
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/seq.h"

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

static void print_summary(void)
{
    printf("\nResults: %d passed, %d failed\n", g_pass, g_fail);
}

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
        if (seq_get(seq, i, &out) != SEQ_OK)
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
        assert(seq_push_back(s, val[i]) == SEQ_OK);
    return s;
}

/* Convenience: cast int index to uint32_t handle */
static inline uint32_t ip(size_t i)
{
    return (uint32_t)i;
}

typedef struct
{
    size_t alloc_calls;
    size_t free_calls;
} CountingAllocatorStats;

static void *counting_alloc(void *ctx, size_t bytes)
{
    CountingAllocatorStats *stats = (CountingAllocatorStats *)ctx;
    stats->alloc_calls++;
    return malloc(bytes);
}

static void counting_free(void *ctx, void *ptr)
{
    CountingAllocatorStats *stats = (CountingAllocatorStats *)ctx;
    if (ptr)
    {
        stats->free_calls++;
        free(ptr);
    }
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
    CHECK(seq_pop_front(s, &out) == SEQ_EMPTY);
    CHECK(seq_pop_back(s, &out) == SEQ_EMPTY);
    CHECK(seq_get(s, 0, &out) == SEQ_RANGE);

    seq_free(s);
}

static void test_single(void)
{
    SECTION("single element");
    Seq  *s   = seq_new();
    uint32_t ptr = ip(42);

    CHECK(seq_push_back(s, ptr) == SEQ_OK);
    CHECK(seq_length(s) == 1);

    uint32_t out = 0;
    CHECK(seq_get(s, 0, &out) == SEQ_OK);
    CHECK(out == ptr);

    CHECK(seq_get(s, 1, &out) == SEQ_RANGE);

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
        CHECK(seq_push_front(s, ip(i)) == SEQ_OK);

    CHECK(seq_length(s) == N);

    for (size_t i = 0; i < N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_get(s, i, &out) == SEQ_OK);
        CHECK(out == ip(N - 1 - i));
    }

    /* Pop from front; should come out N-1 down to 0 */
    for (size_t i = N; i > 0; i--)
    {
        uint32_t out = 0;
        CHECK(seq_pop_front(s, &out) == SEQ_OK);
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
        CHECK(seq_push_back(s, ip(i)) == SEQ_OK);

    CHECK(seq_length(s) == N);

    /* Pop from back; should come out N-1 down to 0 */
    for (size_t i = N; i > 0; i--)
    {
        uint32_t out = 0;
        CHECK(seq_pop_back(s, &out) == SEQ_OK);
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
        CHECK(seq_pop_front(s, &out) == SEQ_OK);
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
        CHECK(seq_get(s, i, &out) == SEQ_OK);
        CHECK(out == ip(i));
    }
    CHECK(seq_get(s, N, &(uint32_t){0}) == SEQ_RANGE);

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
    Seq  *sa  = seq_from_array(a, 3);
    Seq  *sb  = seq_from_array(b, 3);

    CHECK(seq_concat(sa, sb) == SEQ_OK);
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
    Seq  *sa  = seq_from_array(a, 2);
    Seq  *empty = seq_new();

    /* concat(non_empty, empty) */
    CHECK(seq_concat(sa, empty) == SEQ_OK);
    CHECK(seq_length(sa) == 2);

    /* concat(empty, non_empty) */
    Seq  *sa2 = seq_from_array(a, 2);
    Seq  *empty2 = seq_new();
    CHECK(seq_concat(empty2, sa2) == SEQ_OK);
    CHECK(seq_length(empty2) == 2);
    uint32_t out = 0;
    CHECK(seq_get(empty2, 0, &out) == SEQ_OK);
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
    Seq     *s   = seq_from_array(a, 4);

    CHECK(seq_concat(s, s) == SEQ_INVALID);
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
    Seq *left  = seq_new();
    Seq *right = seq_new();
    for (size_t i = 0; i < N; i++)
        seq_push_back(left, ip(i));
    for (size_t i = N; i < 2 * N; i++)
        seq_push_back(right, ip(i));

    CHECK(seq_concat(left, right) == SEQ_OK);
    CHECK(seq_length(left) == 2 * N);

    for (size_t i = 0; i < 2 * N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_get(left, i, &out) == SEQ_OK);
        CHECK(out == ip(i));
    }

    seq_free(left);
    seq_free(right);
}

static void test_custom_allocator_lifecycle(void)
{
    SECTION("custom allocator lifecycle");
    CountingAllocatorStats stats     = {0};
    SeqAllocator          allocator = {counting_alloc, counting_free, &stats};

    Seq *s = seq_new_with_allocator(&allocator);
    CHECK(s != NULL);
    CHECK(seq_push_back(s, ip(1)) == SEQ_OK);
    CHECK(seq_push_front(s, ip(0)) == SEQ_OK);
    CHECK(seq_push_back(s, ip(2)) == SEQ_OK);
    CHECK(seq_reset(s) == SEQ_OK);
    CHECK(seq_push_back(s, ip(9)) == SEQ_OK);
    seq_free(s);

    CHECK(stats.alloc_calls > 0);
    CHECK(stats.free_calls == stats.alloc_calls);
}

static void test_concat_allocator_mismatch(void)
{
    SECTION("concat allocator mismatch invalid");
    CountingAllocatorStats stats_a   = {0};
    CountingAllocatorStats stats_b   = {0};
    SeqAllocator          alloc_a   = {counting_alloc, counting_free, &stats_a};
    SeqAllocator          alloc_b   = {counting_alloc, counting_free, &stats_b};

    Seq *a = seq_new_with_allocator(&alloc_a);
    Seq *b = seq_new_with_allocator(&alloc_b);
    CHECK(a != NULL && b != NULL);
    CHECK(seq_push_back(a, ip(10)) == SEQ_OK);
    CHECK(seq_push_back(b, ip(20)) == SEQ_OK);

    CHECK(seq_concat(a, b) == SEQ_INVALID);
    CHECK(seq_length(a) == 1);
    CHECK(seq_length(b) == 1);

    uint32_t out = 0;
    CHECK(seq_get(a, 0, &out) == SEQ_OK);
    CHECK(out == ip(10));
    CHECK(seq_get(b, 0, &out) == SEQ_OK);
    CHECK(out == ip(20));

    seq_free(a);
    seq_free(b);
    CHECK(stats_a.free_calls == stats_a.alloc_calls);
    CHECK(stats_b.free_calls == stats_b.alloc_calls);
}

static void test_split_preserves_allocator(void)
{
    SECTION("split preserves allocator");
    CountingAllocatorStats stats     = {0};
    SeqAllocator          allocator = {counting_alloc, counting_free, &stats};
    Seq                  *s         = seq_new_with_allocator(&allocator);
    CHECK(s != NULL);

    for (size_t i = 0; i < 8; i++)
        CHECK(seq_push_back(s, ip(i)) == SEQ_OK);

    Seq *l = NULL;
    Seq *r = NULL;
    CHECK(seq_split_at(s, 3, &l, &r) == SEQ_OK);
    CHECK(seq_concat(l, r) == SEQ_OK);
    CHECK(seq_length(l) == 8);

    for (size_t i = 0; i < 8; i++)
    {
        uint32_t out = 0;
        CHECK(seq_get(l, i, &out) == SEQ_OK);
        CHECK(out == ip(i));
    }

    seq_free(s);
    seq_free(l);
    seq_free(r);
    CHECK(stats.free_calls == stats.alloc_calls);
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
        CHECK(seq_split_at(s, split, &l, &r) == SEQ_OK);
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
    Seq   *l     = NULL, *r = NULL;
    CHECK(seq_split_at(s, split, &l, &r) == SEQ_OK);
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
    Seq  *s   = seq_from_array(a, 2);
    Seq  *l   = NULL, *r = NULL;

    /* idx == length is valid (right side is empty) */
    CHECK(seq_split_at(s, 2, &l, &r) == SEQ_OK);
    CHECK(seq_length(l) == 2);
    CHECK(seq_length(r) == 0);
    seq_free(l);
    seq_free(r);

    /* idx > length is invalid */
    Seq *s2 = seq_from_array(a, 2);
    CHECK(seq_split_at(s2, 3, &l, &r) == SEQ_RANGE);
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
        CHECK(seq_get(s, i, &out) == SEQ_OK);
        CHECK(out == ip(i));
    }

    /* Pop all from front */
    for (size_t i = 0; i < N; i++)
    {
        uint32_t out = 0;
        CHECK(seq_pop_front(s, &out) == SEQ_OK);
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
        CHECK(seq_pop_back(s, &out) == SEQ_OK);
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
    Seq *left  = seq_new();
    Seq *right = seq_new();
    for (size_t i = 0; i < A; i++)
        seq_push_back(left, ip(i));
    for (size_t i = A; i < A + B; i++)
        seq_push_back(right, ip(i));

    /* Concat into one big sequence */
    CHECK(seq_concat(left, right) == SEQ_OK);
    CHECK(seq_length(left) == A + B);

    /* Split it back at A */
    Seq *l2 = NULL, *r2 = NULL;
    CHECK(seq_split_at(left, A, &l2, &r2) == SEQ_OK);
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
        PER  = 10
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
    Seq   *l   = NULL, *r = NULL;
    CHECK(seq_split_at(s, mid, &l, &r) == SEQ_OK);

    /* Re-concat */
    CHECK(seq_concat(l, r) == SEQ_OK);
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

static void test_invalid_args(void)
{
    SECTION("invalid argument handling");
    Seq      *s   = seq_new();
    Seq      *s2  = seq_new_with_allocator(NULL);
    uint32_t  out = 0;
    Seq      *l   = NULL;
    Seq      *r   = NULL;
    SeqAllocator bad_alloc_a = {NULL, counting_free, NULL};
    SeqAllocator bad_alloc_b = {counting_alloc, NULL, NULL};

    CHECK(s != NULL);
    CHECK(s2 != NULL);
    CHECK(seq_push_front(NULL, ip(1)) == SEQ_INVALID);
    CHECK(seq_push_back(NULL, ip(1)) == SEQ_INVALID);
    CHECK(seq_pop_front(NULL, &out) == SEQ_INVALID);
    CHECK(seq_pop_back(NULL, &out) == SEQ_INVALID);
    CHECK(seq_get(NULL, 0, &out) == SEQ_INVALID);
    CHECK(seq_concat(NULL, s) == SEQ_INVALID);
    CHECK(seq_concat(s, NULL) == SEQ_INVALID);
    CHECK(seq_split_at(NULL, 0, &l, &r) == SEQ_INVALID);
    CHECK(seq_split_at(s, 0, NULL, &r) == SEQ_INVALID);
    CHECK(seq_split_at(s, 0, &l, NULL) == SEQ_INVALID);
    CHECK(seq_pop_front(s, NULL) == SEQ_INVALID);
    CHECK(seq_pop_back(s, NULL) == SEQ_INVALID);
    CHECK(seq_get(s, 0, NULL) == SEQ_INVALID);
    CHECK(seq_reset(NULL) == SEQ_INVALID);
    CHECK(seq_is_valid(NULL) == 0);
    CHECK(seq_is_valid(s) == 1);
    CHECK(seq_new_with_allocator(&bad_alloc_a) == NULL);
    CHECK(seq_new_with_allocator(&bad_alloc_b) == NULL);

    seq_free(s);
    seq_free(s2);
}

#ifdef SAPLING_SEQ_TESTING
static void test_fault_injection_push(void)
{
    SECTION("fault injection: push oom marks invalid");
    Seq *s = seq_new();
    CHECK(s != NULL);
    CHECK(seq_is_valid(s) == 1);

    CHECK(seq_push_back(s, ip(1)) == SEQ_OK);
    seq_test_fail_alloc_after(0);
    CHECK(seq_push_back(s, ip(2)) == SEQ_OOM);
    seq_test_clear_alloc_fail();

    CHECK(seq_is_valid(s) == 0);
    CHECK(seq_push_back(s, ip(3)) == SEQ_INVALID);
    CHECK(seq_reset(s) == SEQ_OK);
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
    CHECK(seq_push_back(a, ip(10)) == SEQ_OK);
    CHECK(seq_push_back(b, ip(20)) == SEQ_OK);

    seq_test_fail_alloc_after(0);
    CHECK(seq_concat(a, b) == SEQ_OOM);
    seq_test_clear_alloc_fail();

    CHECK(seq_is_valid(a) == 0);
    CHECK(seq_is_valid(b) == 0);
    CHECK(seq_concat(a, b) == SEQ_INVALID);

    CHECK(seq_reset(a) == SEQ_OK);
    CHECK(seq_reset(b) == SEQ_OK);
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
    CHECK(seq_push_back(s, ip(0)) == SEQ_OK);
    CHECK(seq_push_back(s, ip(1)) == SEQ_OK);
    CHECK(seq_push_back(s, ip(2)) == SEQ_OK);

    Seq *l = (Seq *)(uintptr_t)1;
    Seq *r = (Seq *)(uintptr_t)2;
    /*
     * Fail after split has allocated temporary Seq wrappers so we exercise
     * internal split-tree allocation failure (which poisons seq).
     */
    seq_test_fail_alloc_after(4);
    CHECK(seq_split_at(s, 1, &l, &r) == SEQ_OOM);
    seq_test_clear_alloc_fail();

    CHECK(l == (Seq *)(uintptr_t)1);
    CHECK(r == (Seq *)(uintptr_t)2);
    CHECK(seq_is_valid(s) == 0);
    CHECK(seq_split_at(s, 0, &l, &r) == SEQ_INVALID);

    CHECK(seq_reset(s) == SEQ_OK);
    CHECK(seq_is_valid(s) == 1);
    CHECK(seq_length(s) == 0);

    seq_free(s);
}
#endif

/* ================================================================== */
/* main                                                                 */
/* ================================================================== */

int main(void)
{
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
    test_custom_allocator_lifecycle();
    test_concat_allocator_mismatch();
    test_split_preserves_allocator();
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
    test_invalid_args();
#ifdef SAPLING_SEQ_TESTING
    test_fault_injection_push();
    test_fault_injection_concat();
    test_fault_injection_split();
#endif

    print_summary();
    return g_fail ? 1 : 0;
}
