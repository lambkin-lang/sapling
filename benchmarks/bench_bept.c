/*
 * bench_bept.c - benchmark comparing BEPT operations
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/sapling.h"
#include "sapling/txn.h"
#include "sapling/bept.h"
#include "sapling/arena.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <windows.h>
static double now_seconds(void) {
    LARGE_INTEGER freq, counter;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&counter);
    return (double)counter.QuadPart / (double)freq.QuadPart;
}
#else
#include <sys/time.h>
static double now_seconds(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (double)tv.tv_sec + (double)tv.tv_usec / 1000000.0;
}
#endif

/* Xorshift RNG for deterministic pseudo-random sequences */
static uint64_t xorshift64(uint64_t *state) {
    uint64_t x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return *state = x;
}

int main(int argc, char **argv) {
    uint32_t count = 100000;
    uint32_t rounds = 3;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--count") == 0 && i + 1 < argc) {
            count = atoi(argv[i+1]);
            i++;
        } else if (strcmp(argv[i], "--rounds") == 0 && i + 1 < argc) {
            rounds = atoi(argv[i+1]);
            i++;
        }
    }

    printf("Benchmarking BEPT (count=%u, rounds=%u)...\n", count, rounds);

    /* Setup Environment */
    SapMemArena *arena;
    SapArenaOptions opts = {0};
    opts.page_size = 4096;
    opts.type = SAP_ARENA_BACKING_MALLOC;
    sap_arena_init(&arena, &opts);
    SapEnv *env = sap_env_create(arena, 4096);
    sap_bept_subsystem_init(env);

    /* Generate Keys */
    uint32_t *keys = malloc(count * 2 * sizeof(uint32_t));
    if (!keys) {
        fprintf(stderr, "Failed to allocate memory for keys\n");
        return 1;
    }
    
    uint64_t rng = 12345;
    for (uint32_t i = 0; i < count; i++) {
        uint64_t k = xorshift64(&rng);
        keys[2*i] = (uint32_t)(k >> 32);
        keys[2*i+1] = (uint32_t)(k & 0xFFFFFFFF);
    }

    uint64_t val = 0xCAFEBABE;

    /* --- BEPT Benchmarks --- */
    printf("\n--- BEPT Performance ---\n");
    
    for (uint32_t r = 0; r < rounds; r++) {
        printf("Round %u:\n", r+1);
        
        SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);

        /* Insert */
        double t0 = now_seconds();
        for (uint32_t i = 0; i < count; i++) {
            sap_bept_put(txn, &keys[2*i], 2, &val, sizeof(val), 0, NULL);
        }
        double t1 = now_seconds();
        printf("  Put: %.6f sec (%.0f ops/sec)\n", t1 - t0, count / (t1 - t0));

        /* Get (Random Access) - before commit to test uncommitted visibility */
        t0 = now_seconds();
        for (uint32_t i = 0; i < count; i++) {
            const void *v;
            uint32_t len;
            sap_bept_get(txn, &keys[2*i], 2, &v, &len);
        }
        t1 = now_seconds();
        printf("  Get (uncommitted): %.6f sec (%.0f ops/sec)\n", t1 - t0, count / (t1 - t0));
        
        /* Commit */
        sap_txn_commit(txn);
        
        txn = sap_txn_begin(env, NULL, 0);
        
        /* Get (Committed) */
        t0 = now_seconds();
        for (uint32_t i = 0; i < count; i++) {
            const void *v;
            uint32_t len;
            sap_bept_get(txn, &keys[2*i], 2, &v, &len);
        }
        t1 = now_seconds();
        printf("  Get (committed):   %.6f sec (%.0f ops/sec)\n", t1 - t0, count / (t1 - t0));
        
        /* Delete */
        t0 = now_seconds();
        for (uint32_t i = 0; i < count; i++) {
            sap_bept_del(txn, &keys[2*i], 2);
        }
        t1 = now_seconds();
        printf("  Del: %.6f sec (%.0f ops/sec)\n", t1 - t0, count / (t1 - t0));
        
        sap_txn_commit(txn);
    }
    
    free(keys);
    sap_env_destroy(env);
    sap_arena_destroy(arena);
    
    return 0;
}
