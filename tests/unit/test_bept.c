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

typedef struct {
    uint32_t alloc_calls;
    uint32_t fail_after;
} FailingAllocCtx;

static void *failing_alloc(void *ctx, uint32_t sz)
{
    FailingAllocCtx *f = (FailingAllocCtx *)ctx;
    void *p;

    if (!f) return NULL;
    f->alloc_calls++;
    if (f->alloc_calls > f->fail_after) return NULL;
    p = malloc((size_t)sz);
    return p;
}

static void failing_free(void *ctx, void *p, uint32_t sz)
{
    (void)ctx;
    (void)sz;
    free(p);
}

static void test_key_length_boundary_stress(void)
{
    SapEnv *env = NULL;
    SapMemArena *arena = NULL;
    SapTxnCtx *txn = NULL;
    SapArenaOptions opts = {0};
    int rc;
    const void *vptr = NULL;
    uint32_t vlen = 0u;

    static const uint32_t k1[1] = {0x01020304u};
    static const uint32_t k2_boundary[2] = {0x01020304u, 0x80000000u};
    static const uint32_t k2_lsb[2] = {0x01020305u, 0x00000000u};
    static const uint32_t k8[8] = {
        0x89abcdefu, 0x01234567u, 0xfedcba98u, 0x76543210u,
        0x0f0f0f0fu, 0xf0f0f0f0u, 0x13579bdfu, 0x2468ace0u
    };
    static const uint32_t k1_missing[1] = {0x01020306u};
    static const uint32_t k1_as_2[2] = {0x01020304u, 0x00000000u};
    static const char v0[] = "k0";
    static const char v1[] = "k1";
    static const char v2a[] = "k2a";
    static const char v2b[] = "k2b";
    static const char v8[] = "k8";
    uint32_t min_key_buf[8] = {0};

    printf("Running key-length boundary stress test...\n");

    opts.page_size = 4096u;
    opts.type = SAP_ARENA_BACKING_MALLOC;
    rc = sap_arena_init(&arena, &opts);
    CHECK(rc == ERR_OK);
    CHECK(arena != NULL);

    env = sap_env_create(arena, 4096u);
    CHECK(env != NULL);
    rc = sap_bept_subsystem_init(env);
    CHECK(rc == ERR_OK);

    txn = sap_txn_begin(env, NULL, 0u);
    CHECK(txn != NULL);

    /* Zero-length key should round-trip and behave as the minimum key. */
    rc = sap_bept_put(txn, NULL, 0u, v0, (uint32_t)(sizeof(v0) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);

    /* Mixed key lengths and bit-boundary differences. */
    rc = sap_bept_put(txn, k1, 1u, v1, (uint32_t)(sizeof(v1) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k2_boundary, 2u, v2a, (uint32_t)(sizeof(v2a) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k2_lsb, 2u, v2b, (uint32_t)(sizeof(v2b) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k8, 8u, v8, (uint32_t)(sizeof(v8) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);

    rc = sap_bept_get(txn, NULL, 0u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v0) - 1u));
    CHECK(memcmp(vptr, v0, vlen) == 0);

    rc = sap_bept_get(txn, k1, 1u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v1) - 1u));
    CHECK(memcmp(vptr, v1, vlen) == 0);

    rc = sap_bept_get(txn, k2_boundary, 2u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v2a) - 1u));
    CHECK(memcmp(vptr, v2a, vlen) == 0);

    rc = sap_bept_get(txn, k2_lsb, 2u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v2b) - 1u));
    CHECK(memcmp(vptr, v2b, vlen) == 0);

    rc = sap_bept_get(txn, k8, 8u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v8) - 1u));
    CHECK(memcmp(vptr, v8, vlen) == 0);

    /* Wrong-length lookup must not match. */
    rc = sap_bept_get(txn, k1_as_2, 2u, &vptr, &vlen);
    CHECK(rc == ERR_NOT_FOUND);
    rc = sap_bept_get(txn, k1_missing, 1u, &vptr, &vlen);
    CHECK(rc == ERR_NOT_FOUND);

    rc = sap_bept_min(txn, min_key_buf, 8u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v0) - 1u));
    CHECK(memcmp(vptr, v0, vlen) == 0);

    /* Delete zero-length key and ensure next minimum is the 1-word key. */
    rc = sap_bept_del(txn, NULL, 0u);
    CHECK(rc == ERR_OK);
    memset(min_key_buf, 0, sizeof(min_key_buf));
    rc = sap_bept_min(txn, min_key_buf, 8u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(min_key_buf[0] == k1[0]);

    rc = sap_txn_commit(txn);
    CHECK(rc == ERR_OK);

    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

static void test_word_boundary_patterns(void)
{
    SapEnv *env = NULL;
    SapMemArena *arena = NULL;
    SapTxnCtx *txn = NULL;
    SapArenaOptions opts = {0};
    int rc;
    const void *vptr = NULL;
    uint32_t vlen = 0u;
    uint32_t min_key_buf[3] = {0};

    static const uint32_t k_bit31[1] = {0x00000001u};
    static const uint32_t k_bit32[2] = {0x00000000u, 0x80000000u};
    static const uint32_t k_bit63[2] = {0x00000000u, 0x00000001u};
    static const uint32_t k_bit64[3] = {0x00000000u, 0x00000000u, 0x80000000u};
    static const uint32_t k_bit65[3] = {0x00000000u, 0x00000000u, 0x40000000u};
    static const uint32_t k_missing[3] = {0x00000000u, 0x00000000u, 0x20000000u};

    static const char v31[] = "b31";
    static const char v32[] = "b32";
    static const char v63[] = "b63";
    static const char v64[] = "b64";
    static const char v65[] = "b65";

    printf("Running word-boundary key stress test...\n");

    opts.page_size = 4096u;
    opts.type = SAP_ARENA_BACKING_MALLOC;
    rc = sap_arena_init(&arena, &opts);
    CHECK(rc == ERR_OK);
    CHECK(arena != NULL);

    env = sap_env_create(arena, 4096u);
    CHECK(env != NULL);
    rc = sap_bept_subsystem_init(env);
    CHECK(rc == ERR_OK);

    txn = sap_txn_begin(env, NULL, 0u);
    CHECK(txn != NULL);

    /* Insert in mixed order so trie branches are built across 31/32/63/64/65 boundaries. */
    rc = sap_bept_put(txn, k_bit31, 1u, v31, (uint32_t)(sizeof(v31) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k_bit64, 3u, v64, (uint32_t)(sizeof(v64) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k_bit32, 2u, v32, (uint32_t)(sizeof(v32) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k_bit65, 3u, v65, (uint32_t)(sizeof(v65) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k_bit63, 2u, v63, (uint32_t)(sizeof(v63) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);

    rc = sap_bept_get(txn, k_bit31, 1u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v31) - 1u));
    CHECK(memcmp(vptr, v31, vlen) == 0);

    rc = sap_bept_get(txn, k_bit32, 2u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v32) - 1u));
    CHECK(memcmp(vptr, v32, vlen) == 0);

    rc = sap_bept_get(txn, k_bit63, 2u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v63) - 1u));
    CHECK(memcmp(vptr, v63, vlen) == 0);

    rc = sap_bept_get(txn, k_bit64, 3u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v64) - 1u));
    CHECK(memcmp(vptr, v64, vlen) == 0);

    rc = sap_bept_get(txn, k_bit65, 3u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v65) - 1u));
    CHECK(memcmp(vptr, v65, vlen) == 0);

    memset(min_key_buf, 0, sizeof(min_key_buf));
    rc = sap_bept_min(txn, min_key_buf, 3u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(min_key_buf, k_bit65, sizeof(k_bit65)) == 0);

    rc = sap_bept_del(txn, k_bit65, 3u);
    CHECK(rc == ERR_OK);
    memset(min_key_buf, 0, sizeof(min_key_buf));
    rc = sap_bept_min(txn, min_key_buf, 3u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(min_key_buf, k_bit64, sizeof(k_bit64)) == 0);

    rc = sap_bept_del(txn, k_bit64, 3u);
    CHECK(rc == ERR_OK);
    memset(min_key_buf, 0, sizeof(min_key_buf));
    rc = sap_bept_min(txn, min_key_buf, 3u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(min_key_buf[0] == k_bit63[0]);
    CHECK(min_key_buf[1] == k_bit63[1]);

    rc = sap_bept_get(txn, k_missing, 3u, &vptr, &vlen);
    CHECK(rc == ERR_NOT_FOUND);

    rc = sap_txn_commit(txn);
    CHECK(rc == ERR_OK);

    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

static void test_delete_collapse_edge_cases(void)
{
    SapEnv *env = NULL;
    SapMemArena *arena = NULL;
    SapTxnCtx *txn = NULL;
    SapArenaOptions opts = {0};
    int rc;
    const void *vptr = NULL;
    uint32_t vlen = 0u;
    uint32_t min_key_buf[2] = {0u, 0u};

    static const uint32_t k0[2] = {0x00000000u, 0x00000000u};
    static const uint32_t k1[2] = {0x00000000u, 0x80000000u};
    static const uint32_t k2[2] = {0x00000000u, 0x80000001u};
    static const uint32_t k3[2] = {0xffffffffu, 0xffffffffu};
    static const uint32_t km[2] = {0x12345678u, 0x9abcdef0u};

    static const char v0[] = "d0";
    static const char v1[] = "d1";
    static const char v2[] = "d2";
    static const char v3[] = "d3";

    printf("Running deletion collapse edge-case test...\n");

    opts.page_size = 4096u;
    opts.type = SAP_ARENA_BACKING_MALLOC;
    rc = sap_arena_init(&arena, &opts);
    CHECK(rc == ERR_OK);
    CHECK(arena != NULL);

    env = sap_env_create(arena, 4096u);
    CHECK(env != NULL);
    rc = sap_bept_subsystem_init(env);
    CHECK(rc == ERR_OK);

    txn = sap_txn_begin(env, NULL, 0u);
    CHECK(txn != NULL);

    rc = sap_bept_put(txn, k0, 2u, v0, (uint32_t)(sizeof(v0) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k1, 2u, v1, (uint32_t)(sizeof(v1) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k2, 2u, v2, (uint32_t)(sizeof(v2) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k3, 2u, v3, (uint32_t)(sizeof(v3) - 1u), 0u, NULL);
    CHECK(rc == ERR_OK);

    rc = sap_bept_del(txn, km, 2u);
    CHECK(rc == ERR_NOT_FOUND);

    rc = sap_bept_del(txn, k2, 2u);
    CHECK(rc == ERR_OK);
    rc = sap_bept_get(txn, k2, 2u, &vptr, &vlen);
    CHECK(rc == ERR_NOT_FOUND);
    rc = sap_bept_get(txn, k1, 2u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v1) - 1u));
    CHECK(memcmp(vptr, v1, vlen) == 0);

    rc = sap_bept_del(txn, k1, 2u);
    CHECK(rc == ERR_OK);
    rc = sap_bept_get(txn, k1, 2u, &vptr, &vlen);
    CHECK(rc == ERR_NOT_FOUND);
    memset(min_key_buf, 0, sizeof(min_key_buf));
    rc = sap_bept_min(txn, min_key_buf, 2u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(min_key_buf, k0, sizeof(k0)) == 0);

    rc = sap_bept_del(txn, k0, 2u);
    CHECK(rc == ERR_OK);
    memset(min_key_buf, 0, sizeof(min_key_buf));
    rc = sap_bept_min(txn, min_key_buf, 2u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(memcmp(min_key_buf, k3, sizeof(k3)) == 0);

    rc = sap_bept_del(txn, k3, 2u);
    CHECK(rc == ERR_OK);
    rc = sap_bept_min(txn, min_key_buf, 2u, &vptr, &vlen);
    CHECK(rc == ERR_NOT_FOUND);
    rc = sap_bept_del(txn, k3, 2u);
    CHECK(rc == ERR_NOT_FOUND);

    rc = sap_txn_commit(txn);
    CHECK(rc == ERR_OK);

    sap_env_destroy(env);
    sap_arena_destroy(arena);
}

static void test_arena_exhaustion_behavior(void)
{
    SapArenaOptions opts = {0};
    SapMemArena *arena = NULL;
    SapEnv *env = NULL;
    SapTxnCtx *txn = NULL;
    FailingAllocCtx fail = {0};
    int rc;
    static const uint32_t k1[1] = {0x11111111u};
    static const uint32_t k2[1] = {0x22222222u};
    static const char v1[] = "v1";
    static const char v2[] = "v2";
    const void *vptr = NULL;
    uint32_t vlen = 0u;

    printf("Running arena-exhaustion behavior test...\n");

    /* Case 1: subsystem init should fail cleanly on immediate OOM. */
    fail.alloc_calls = 0u;
    fail.fail_after = 0u;
    opts.page_size = 4096u;
    opts.type = SAP_ARENA_BACKING_CUSTOM;
    opts.cfg.custom.alloc_page = failing_alloc;
    opts.cfg.custom.free_page = failing_free;
    opts.cfg.custom.ctx = &fail;

    rc = sap_arena_init(&arena, &opts);
    CHECK(rc == ERR_OK);
    env = sap_env_create(arena, 4096u);
    CHECK(env != NULL);
    rc = sap_bept_subsystem_init(env);
    CHECK(rc == ERR_OOM);
    sap_env_destroy(env);
    sap_arena_destroy(arena);

    /* Case 2: insert path OOM must not corrupt existing trie state. */
    fail.alloc_calls = 0u;
    fail.fail_after = 3u;
    rc = sap_arena_init(&arena, &opts);
    CHECK(rc == ERR_OK);
    env = sap_env_create(arena, 4096u);
    CHECK(env != NULL);
    rc = sap_bept_subsystem_init(env);
    CHECK(rc == ERR_OK); /* alloc call #1 */

    txn = sap_txn_begin(env, NULL, 0u);
    CHECK(txn != NULL);

    rc = sap_bept_put(txn, k1, 1u, v1, (uint32_t)(sizeof(v1) - 1u), 0u, NULL); /* alloc call #2 */
    CHECK(rc == ERR_OK);

    /* Second insert requires both a leaf and an internal node.
     * With fail_after=3, leaf alloc succeeds (#3) and internal alloc fails (#4). */
    rc = sap_bept_put(txn, k2, 1u, v2, (uint32_t)(sizeof(v2) - 1u), 0u, NULL);
    CHECK(rc == ERR_OOM);

    rc = sap_bept_get(txn, k1, 1u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v1) - 1u));
    CHECK(memcmp(vptr, v1, vlen) == 0);

    rc = sap_bept_get(txn, k2, 1u, &vptr, &vlen);
    CHECK(rc == ERR_NOT_FOUND);

    rc = sap_txn_commit(txn);
    CHECK(rc == ERR_OK);

    txn = sap_txn_begin(env, NULL, TXN_RDONLY);
    CHECK(txn != NULL);
    rc = sap_bept_get(txn, k1, 1u, &vptr, &vlen);
    CHECK(rc == ERR_OK);
    CHECK(vlen == (uint32_t)(sizeof(v1) - 1u));
    CHECK(memcmp(vptr, v1, vlen) == 0);
    sap_txn_abort(txn);

    sap_env_destroy(env);
    sap_arena_destroy(arena);

    /* Case 3: failed insert followed by abort should leave environment unchanged. */
    fail.alloc_calls = 0u;
    fail.fail_after = 4u;
    rc = sap_arena_init(&arena, &opts);
    CHECK(rc == ERR_OK);
    env = sap_env_create(arena, 4096u);
    CHECK(env != NULL);
    rc = sap_bept_subsystem_init(env);
    CHECK(rc == ERR_OK); /* alloc call #1 */

    txn = sap_txn_begin(env, NULL, 0u);
    CHECK(txn != NULL);
    rc = sap_bept_put(txn, k1, 1u, v1, (uint32_t)(sizeof(v1) - 1u), 0u, NULL); /* alloc call #2 */
    CHECK(rc == ERR_OK);
    rc = sap_bept_put(txn, k2, 1u, v2, (uint32_t)(sizeof(v2) - 1u), 0u, NULL); /* alloc call #3 fails */
    CHECK(rc == ERR_OOM);
    sap_txn_abort(txn);

    txn = sap_txn_begin(env, NULL, TXN_RDONLY);
    CHECK(txn != NULL);
    rc = sap_bept_get(txn, k1, 1u, &vptr, &vlen);
    CHECK(rc == ERR_NOT_FOUND);
    rc = sap_bept_get(txn, k2, 1u, &vptr, &vlen);
    CHECK(rc == ERR_NOT_FOUND);
    sap_txn_abort(txn);

    sap_env_destroy(env);
    sap_arena_destroy(arena);
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
    
    sap_env_destroy(env);
    sap_arena_destroy(arena);

    test_key_length_boundary_stress();
    test_word_boundary_patterns();
    test_delete_collapse_edge_cases();
    test_arena_exhaustion_behavior();

    printf("test_bept passed!\n");

    return 0;
}
