#include "sapling/sapling.h"
#include "sapling/txn.h"
#include "sapling/bept.h"
#include "sapling/arena.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define CHECK(expr) \
    do { \
        if (!(expr)) { \
            fprintf(stderr, "FAIL: %s (%s:%d)\n", #expr, __FILE__, __LINE__); \
            exit(1); \
        } \
    } while (0)

static void u64_to_key(uint64_t val, uint32_t key[2]) {
    key[0] = (uint32_t)(val >> 32);
    key[1] = (uint32_t)(val & 0xFFFFFFFF);
}

int main(void) {
    SapEnv *env;
    SapMemArena *arena;
    SapTxnCtx *txn;
    void *bept_state;
    int rc;

    printf("Running test_bept...\n");

    /* Create environment */
    SapArenaOptions opts = {0};
    opts.page_size = 4096;
    opts.type = SAP_ARENA_BACKING_MALLOC;
    
    rc = sap_arena_init(&arena, &opts);
    CHECK(rc == ERR_OK);
    CHECK(arena != NULL);
    
    env = sap_env_create(arena, 4096);
    CHECK(env != NULL);

    /* Initialize BEPT subsystem */
    rc = sap_bept_subsystem_init(env);
    CHECK(rc == ERR_OK);

    /* Verify subsystem state is set */
    bept_state = sap_env_subsystem_state(env, SAP_SUBSYSTEM_BEPT);
    CHECK(bept_state != NULL);

    /* Start a transaction */
    txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);

    /* Test: Single Insert */
    uint64_t k1_val = 0xDEADBEEF;
    uint32_t k1[2];
    u64_to_key(k1_val, k1);
    
    const char *v1 = "value1";
    
    rc = sap_bept_put(txn, k1, 2, v1, 6, 0, NULL);
    CHECK(rc == ERR_OK);
    
    /* Test: Retrieve */
    const void *v_out;
    uint32_t len_out;
    rc = sap_bept_get(txn, k1, 2, &v_out, &len_out);
    CHECK(rc == ERR_OK);
    CHECK(len_out == 6);
    CHECK(memcmp(v1, v_out, 6) == 0);
    
    /* Test: Second Insert */
    uint64_t k2_val = 0xFEADBEEF;
    uint32_t k2[2];
    u64_to_key(k2_val, k2);
    
    const char *v2 = "value2";
    rc = sap_bept_put(txn, k2, 2, v2, 6, 0, NULL);
    CHECK(rc == ERR_OK);
    
    /* Verify both exist */
    rc = sap_bept_get(txn, k1, 2, &v_out, &len_out);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(v1, v_out, 6) == 0);
    
    rc = sap_bept_get(txn, k2, 2, &v_out, &len_out);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(v2, v_out, 6) == 0);

    /* Test: Replace Value */
    const char *v1_new = "newval";
    rc = sap_bept_put(txn, k1, 2, v1_new, 6, 0, NULL);
    CHECK(rc == ERR_OK);
    
    rc = sap_bept_get(txn, k1, 2, &v_out, &len_out);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(v1_new, v_out, 6) == 0);
    
    /* Test: Missing Key */
    uint32_t k_missing[2];
    u64_to_key(0x12345678, k_missing);
    rc = sap_bept_get(txn, k_missing, 2, &v_out, &len_out);
    CHECK(rc == ERR_NOT_FOUND);

    /* Test: Delete */
    /* Delete k1 (should leave k2) */
    rc = sap_bept_del(txn, k1, 2);
    CHECK(rc == ERR_OK);
    
    /* Verify k1 gone */
    rc = sap_bept_get(txn, k1, 2, &v_out, &len_out);
    CHECK(rc == ERR_NOT_FOUND);
    
    /* Verify k2 still there */
    rc = sap_bept_get(txn, k2, 2, &v_out, &len_out);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(v2, v_out, 6) == 0);
    
    /* Delete k2 */
    rc = sap_bept_del(txn, k2, 2);
    CHECK(rc == ERR_OK);
    
    rc = sap_bept_get(txn, k2, 2, &v_out, &len_out);
    CHECK(rc == ERR_NOT_FOUND);

    /* Insert again for subsequent tests */
    rc = sap_bept_put(txn, k1, 2, v1_new, 6, 0, NULL);
    CHECK(rc == ERR_OK);

    /* Verify k1 is back */
    rc = sap_bept_get(txn, k1, 2, &v_out, &len_out);
    CHECK(rc == ERR_OK);

    /* Massive Insert/Delete Test */
    printf("Running massive insert/delete test...\n");
    int i;
    int count = 1000;
    
    for (i = 0; i < count; i++) {
        uint32_t k[2];
        u64_to_key(i * 1234567, k); // Pseudo-random distribution
        rc = sap_bept_put(txn, k, 2, &i, sizeof(i), 0, NULL);
        CHECK(rc == ERR_OK);
    }
    
    for (i = 0; i < count; i++) {
        uint32_t k[2];
        u64_to_key(i * 1234567, k);
        int val = 0;
        uint32_t len = 0;
        const void *vptr;
        rc = sap_bept_get(txn, k, 2, &vptr, &len);
        CHECK(rc == ERR_OK);
        CHECK(len == sizeof(int));
        memcpy(&val, vptr, sizeof(int));
        CHECK(val == i);
    }

    for (i = 0; i < count; i++) {
        uint32_t k[2];
        u64_to_key(i * 1234567, k);
        rc = sap_bept_del(txn, k, 2);
        CHECK(rc == ERR_OK);
    }
    
    /* Verify empty */
    for (i = 0; i < count; i++) {
        uint32_t k[2];
        u64_to_key(i * 1234567, k);
        rc = sap_bept_get(txn, k, 2, &v_out, &len_out);
        CHECK(rc == ERR_NOT_FOUND);
    }
    
    /* Test: Min (First key) */
    uint32_t k_min[2] = {0, 1};
    rc = sap_bept_put(txn, k_min, 2, "min", 3, 0, NULL);
    CHECK(rc == ERR_OK);
    
    uint32_t k_min_out[2];
    const void *v_min_out;
    uint32_t v_min_len_out;
    
    rc = sap_bept_min(txn, k_min_out, 2, &v_min_out, &v_min_len_out);
    CHECK(rc == ERR_OK);
    CHECK(k_min_out[0] == 0 && k_min_out[1] == 1); // Should match
    
    // Add a smaller key
    uint32_t k_smaller[2] = {0, 0};
    rc = sap_bept_put(txn, k_smaller, 2, "zero", 4, 0, NULL);
    CHECK(rc == ERR_OK);
    
    rc = sap_bept_min(txn, k_min_out, 2, &v_min_out, &v_min_len_out);
    CHECK(rc == ERR_OK);
    CHECK(k_min_out[0] == 0 && k_min_out[1] == 0); // Should match smaller

    /* Commit baseline and verify abort rollback semantics in a fresh txn. */
    rc = sap_txn_commit(txn);
    CHECK(rc == ERR_OK);

    txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);
    {
        uint32_t k_abort[2];
        const char *abort_val = "abortv";
        u64_to_key(0xABCDEF01u, k_abort);
        rc = sap_bept_put(txn, k_abort, 2, abort_val, 6, 0, NULL);
        CHECK(rc == ERR_OK);
    }
    sap_txn_abort(txn);

    txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);
    {
        uint32_t k_abort[2];
        const void *vptr = NULL;
        uint32_t vlen = 0u;
        u64_to_key(0xABCDEF01u, k_abort);
        rc = sap_bept_get(txn, k_abort, 2, &vptr, &vlen);
        CHECK(rc == ERR_NOT_FOUND);
    }
    sap_txn_abort(txn);

    /* 128-bit key support: min ordering and point lookups across 4-word keys. */
    txn = sap_txn_begin(env, NULL, 0);
    CHECK(txn != NULL);
    {
        static const uint32_t k128_a[4] = {0x7fffffffu, 0xffffffffu, 0xffffffffu, 0xffffffffu};
        static const uint32_t k128_b[4] = {0x80000000u, 0x00000000u, 0x00000000u, 0x00000000u};
        static const uint32_t k128_c[4] = {0x80000000u, 0x00000000u, 0x00000000u, 0x00000001u};
        static const char va[] = "k128a";
        static const char vb[] = "k128b";
        static const char vc[] = "k128c";
        uint32_t kmin128[4] = {0};
        const void *vptr = NULL;
        uint32_t vlen = 0u;

        rc = sap_bept_clear(txn);
        CHECK(rc == ERR_OK);

        rc = sap_bept_put(txn, k128_c, 4, vc, (uint32_t)(sizeof(vc) - 1u), 0, NULL);
        CHECK(rc == ERR_OK);
        rc = sap_bept_put(txn, k128_a, 4, va, (uint32_t)(sizeof(va) - 1u), 0, NULL);
        CHECK(rc == ERR_OK);
        rc = sap_bept_put(txn, k128_b, 4, vb, (uint32_t)(sizeof(vb) - 1u), 0, NULL);
        CHECK(rc == ERR_OK);

        rc = sap_bept_min(txn, kmin128, 4, &vptr, &vlen);
        CHECK(rc == ERR_OK);
        CHECK(memcmp(kmin128, k128_a, sizeof(kmin128)) == 0);
        CHECK(vlen == (uint32_t)(sizeof(va) - 1u));
        CHECK(memcmp(vptr, va, vlen) == 0);

        rc = sap_bept_get(txn, k128_b, 4, &vptr, &vlen);
        CHECK(rc == ERR_OK);
        CHECK(vlen == (uint32_t)(sizeof(vb) - 1u));
        CHECK(memcmp(vptr, vb, vlen) == 0);
    }
    rc = sap_txn_commit(txn);
    CHECK(rc == ERR_OK);
    
    printf("test_bept passed!\n");

    sap_env_destroy(env);
    sap_arena_destroy(arena);

    return 0;
}
