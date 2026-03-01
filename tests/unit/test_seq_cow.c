#include "sapling/seq.h"
#include "sapling/txn.h"
#include "sapling/sapling.h"
#include "sapling/arena.h"
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Subsystem registration function from seq.c */
int sap_seq_subsystem_init(SapEnv *env);

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

static void test_seq_cow_rollback(void)
{
    SECTION("Sequence COW rollback");

    SapMemArena *arena = NULL;
    SapArenaOptions opts = {.type = SAP_ARENA_BACKING_MALLOC, .page_size = 4096};
    int rc = sap_arena_init(&arena, &opts);
    CHECK(rc == 0);

    SapEnv *env = sap_env_create(arena, 4096);
    CHECK(env != NULL);

    sap_seq_subsystem_init(env);

    Seq *s = seq_new(env);
    CHECK(s != NULL);

    /* 1. Basic mutation and rollback */
    SapTxnCtx *txn1 = sap_txn_begin(env, NULL, 0);
    CHECK(seq_push_back(txn1, s, 10) == ERR_OK);
    CHECK(seq_push_back(txn1, s, 20) == ERR_OK);
    CHECK(sap_txn_commit(txn1) == ERR_OK);

    CHECK(seq_length(s) == 2);

    SapTxnCtx *txn2 = sap_txn_begin(env, NULL, 0);
    CHECK(seq_push_back(txn2, s, 30) == ERR_OK);
    CHECK(seq_length(s) == 3);
    uint32_t val = 0;
    seq_get(s, 2, &val);
    CHECK(val == 30);

    sap_txn_abort(txn2);

    /* Should be rolled back to [10, 20] */
    CHECK(seq_length(s) == 2);
    val = 0;
    seq_get(s, 0, &val);
    CHECK(val == 10);
    val = 0;
    seq_get(s, 1, &val);
    CHECK(val == 20);

    /* 2. Concat and rollback */
    Seq *s2 = seq_new(env);
    SapTxnCtx *txn3 = sap_txn_begin(env, NULL, 0);
    CHECK(seq_push_back(txn3, s2, 40) == ERR_OK);
    CHECK(sap_txn_commit(txn3) == ERR_OK);

    SapTxnCtx *txn4 = sap_txn_begin(env, NULL, 0);
    CHECK(seq_concat(txn4, s, s2) == ERR_OK);
    CHECK(seq_length(s) == 3);
    CHECK(seq_length(s2) == 0);
    sap_txn_abort(txn4);

    CHECK(seq_length(s) == 2);  /* s restored to [10, 20] */
    CHECK(seq_length(s2) == 1); /* s2 restored to [40] */
    val = 0;
    seq_get(s2, 0, &val);
    CHECK(val == 40);

    /* 3. Nested Transaction Rollback */
    SapTxnCtx *txn5 = sap_txn_begin(env, NULL, 0);
    CHECK(seq_push_back(txn5, s, 50) == ERR_OK);

    /* Nested txn */
    SapTxnCtx *txn6 = sap_txn_begin(env, txn5, 0);
    CHECK(seq_push_back(txn6, s, 60) == ERR_OK);
    CHECK(seq_length(s) == 4); /* [10, 20, 50, 60] */
    sap_txn_abort(txn6);

    CHECK(seq_length(s) == 3); /* [10, 20, 50] */
    sap_txn_commit(txn5);
    CHECK(seq_length(s) == 3);
    seq_get(s, 2, &val);
    CHECK(val == 50);

    seq_free(env, s);
    seq_free(env, s2);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

int main()
{
    test_seq_cow_rollback();
    printf("\nResults: %d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0;
}
