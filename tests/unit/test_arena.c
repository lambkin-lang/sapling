#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "sapling/arena.h"

#define CHECK(cond) \
    do { \
        if (!(cond)) { \
            fprintf(stderr, "FAIL: %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            exit(1); \
        } \
    } while (0)

static void test_arena_init_destroy(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_MALLOC,
        .cfg.mmap.max_size = 1024 * 1024
    };

    printf("Test: init/destroy\n");
    CHECK(sap_arena_init(&arena, &opts) == 0);
    CHECK(arena != NULL);
    sap_arena_destroy(arena);
}

static void test_arena_alloc_page(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_MALLOC,
        .cfg.mmap.max_size = 1024 * 1024
    };

    printf("Test: alloc/free page\n");
    CHECK(sap_arena_init(&arena, &opts) == 0);

    void *pg1 = NULL, *pg2 = NULL;
    uint32_t pgno1 = 0, pgno2 = 0;

    CHECK(sap_arena_alloc_page(arena, &pg1, &pgno1) == 0);
    CHECK(pgno1 == 1);
    CHECK(pg1 != NULL);

    CHECK(sap_arena_alloc_page(arena, &pg2, &pgno2) == 0);
    CHECK(pgno2 == 2);
    CHECK(pg2 != NULL);
    CHECK(pg1 != pg2);

    /* Test resolve mapping */
    CHECK(sap_arena_resolve(arena, pgno1) == pg1);
    CHECK(sap_arena_resolve(arena, pgno2) == pg2);
    CHECK(sap_arena_resolve(arena, 999) == NULL);

    /* Test free list reuse */
    CHECK(sap_arena_free_page(arena, pgno1) == 0);
    
    void *pg3 = NULL;
    uint32_t pgno3 = 0;
    CHECK(sap_arena_alloc_page(arena, &pg3, &pgno3) == 0);
    CHECK(pgno3 == pgno1); /* Should reuse pgno1 from free list */
    CHECK(pg3 != NULL);

    sap_arena_destroy(arena);
}

static void test_arena_alloc_node(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_MALLOC,
        .cfg.mmap.max_size = 1024 * 1024
    };

    printf("Test: alloc/free node\n");
    CHECK(sap_arena_init(&arena, &opts) == 0);

    void *nd1 = NULL, *nd2 = NULL;
    uint32_t id1 = 0, id2 = 0;

    /* Allocate small objects (e.g., finger tree node sizing) */
    CHECK(sap_arena_alloc_node(arena, 48, &nd1, &id1) == 0);
    CHECK(nd1 != NULL);
    CHECK(id1 != 0);

    CHECK(sap_arena_alloc_node(arena, 128, &nd2, &id2) == 0);
    CHECK(nd2 != NULL);
    CHECK(id2 != id1);

    CHECK(sap_arena_resolve(arena, id1) == nd1);
    CHECK(sap_arena_resolve(arena, id2) == nd2);

    /* Free them */
    CHECK(sap_arena_free_node(arena, id1, 48) == 0);
    CHECK(sap_arena_free_node(arena, id2, 128) == 0);

    sap_arena_destroy(arena);
}

typedef struct {
    uint32_t alloc_attempts;
    uint32_t alloc_successes;
    uint32_t free_calls;
    uint32_t fail_after;
} TestAllocCtx;

static void *test_custom_alloc(void *ctx, uint32_t size)
{
    TestAllocCtx *tctx = (TestAllocCtx *)ctx;
    void *p;

    if (!tctx) {
        return NULL;
    }
    tctx->alloc_attempts++;
    if (tctx->alloc_attempts > tctx->fail_after) {
        return NULL;
    }
    p = malloc((size_t)size);
    if (p) {
        tctx->alloc_successes++;
    }
    return p;
}

static void test_custom_free(void *ctx, void *ptr, uint32_t size)
{
    TestAllocCtx *tctx = (TestAllocCtx *)ctx;
    (void)size;
    if (tctx) {
        tctx->free_calls++;
    }
    free(ptr);
}

static void test_arena_backing_strategy_switch(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions malloc_opts = {
        .type = SAP_ARENA_BACKING_MALLOC,
        .page_size = 4096u
    };
    SapArenaOptions custom_opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .page_size = 4096u
    };
    TestAllocCtx custom_ctx = {
        .alloc_attempts = 0u,
        .alloc_successes = 0u,
        .free_calls = 0u,
        .fail_after = UINT32_MAX
    };
    void *page = NULL;
    uint32_t pgno = 0u;
    void *node = NULL;
    uint32_t nodeno = 0u;

    printf("Test: backing strategy switch (malloc -> custom)\n");

    CHECK(sap_arena_init(&arena, &malloc_opts) == ERR_OK);
    CHECK(sap_arena_alloc_page(arena, &page, &pgno) == ERR_OK);
    CHECK(page != NULL);
    CHECK(pgno == 1u);
    sap_arena_destroy(arena);
    arena = NULL;

    custom_opts.cfg.custom.alloc_page = test_custom_alloc;
    custom_opts.cfg.custom.free_page = test_custom_free;
    custom_opts.cfg.custom.ctx = &custom_ctx;
    CHECK(sap_arena_init(&arena, &custom_opts) == ERR_OK);

    CHECK(sap_arena_alloc_page(arena, &page, &pgno) == ERR_OK);
    CHECK(page != NULL);
    CHECK(pgno == 1u);

    CHECK(sap_arena_alloc_node(arena, 80u, &node, &nodeno) == ERR_OK);
    CHECK(node != NULL);
    CHECK(nodeno != 0u);

    CHECK(sap_arena_free_node(arena, nodeno, 80u) == ERR_OK);
    CHECK(sap_arena_free_page(arena, pgno) == ERR_OK);

    sap_arena_destroy(arena);

    CHECK(custom_ctx.alloc_successes >= 2u);
    CHECK(custom_ctx.free_calls == custom_ctx.alloc_successes);
}

static void test_arena_exhaustion_behavior(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_CUSTOM,
        .page_size = 4096u
    };
    TestAllocCtx ctx = {
        .alloc_attempts = 0u,
        .alloc_successes = 0u,
        .free_calls = 0u,
        .fail_after = 1u
    };
    void *page = NULL;
    uint32_t pgno = 0u;
    void *node = NULL;
    uint32_t nodeno = 0u;

    printf("Test: exhaustion behavior (custom backing)\n");

    opts.cfg.custom.alloc_page = test_custom_alloc;
    opts.cfg.custom.free_page = test_custom_free;
    opts.cfg.custom.ctx = &ctx;
    CHECK(sap_arena_init(&arena, &opts) == ERR_OK);

    CHECK(sap_arena_alloc_page(arena, &page, &pgno) == ERR_OK);
    CHECK(pgno == 1u);
    CHECK(sap_arena_alloc_page(arena, &page, &pgno) == ERR_OOM);

    /* Failure should roll back pgno progression; next success should be pgno 2. */
    ctx.fail_after = UINT32_MAX;
    CHECK(sap_arena_alloc_page(arena, &page, &pgno) == ERR_OK);
    CHECK(pgno == 2u);

    /* Exhaustion on node allocations should fail cleanly without corrupting state. */
    ctx.fail_after = ctx.alloc_attempts;
    CHECK(sap_arena_alloc_node(arena, 32u, &node, &nodeno) == ERR_OOM);
    ctx.fail_after = UINT32_MAX;
    CHECK(sap_arena_alloc_node(arena, 32u, &node, &nodeno) == ERR_OK);
    CHECK(node != NULL);
    CHECK(nodeno != 0u);
    CHECK(sap_arena_free_node(arena, nodeno, 32u) == ERR_OK);

    sap_arena_destroy(arena);
    CHECK(ctx.free_calls == ctx.alloc_successes);
}

static void test_arena_fragmentation_reuse(void)
{
    SapMemArena *arena = NULL;
    SapArenaOptions opts = {
        .type = SAP_ARENA_BACKING_MALLOC,
        .page_size = 4096u
    };
    void *page = NULL;
    uint32_t p1 = 0u, p2 = 0u, p3 = 0u;
    void *n1 = NULL, *n2 = NULL, *n3 = NULL;
    uint32_t n1_id = 0u, n2_id = 0u, n3_id = 0u;
    uint32_t r1 = 0u, r2 = 0u, r3 = 0u, r4 = 0u;

    printf("Test: multi-region fragmentation and slot reuse\n");

    CHECK(sap_arena_init(&arena, &opts) == ERR_OK);

    CHECK(sap_arena_alloc_page(arena, &page, &p1) == ERR_OK);
    CHECK(sap_arena_alloc_page(arena, &page, &p2) == ERR_OK);
    CHECK(sap_arena_alloc_page(arena, &page, &p3) == ERR_OK);
    CHECK(p1 == 1u && p2 == 2u && p3 == 3u);

    CHECK(sap_arena_alloc_node(arena, 48u, &n1, &n1_id) == ERR_OK);
    CHECK(sap_arena_alloc_node(arena, 64u, &n2, &n2_id) == ERR_OK);
    CHECK(sap_arena_alloc_node(arena, 96u, &n3, &n3_id) == ERR_OK);
    CHECK(n1 != NULL && n2 != NULL && n3 != NULL);
    CHECK(n1_id == 4u && n2_id == 5u && n3_id == 6u);
    CHECK(sap_arena_active_pages(arena) == 6u);

    /* Free a mix of node/page slots to simulate fragmented reuse pressure. */
    CHECK(sap_arena_free_node(arena, n2_id, 64u) == ERR_OK);
    CHECK(sap_arena_free_page(arena, p2) == ERR_OK);
    CHECK(sap_arena_free_node(arena, n3_id, 96u) == ERR_OK);
    CHECK(sap_arena_active_pages(arena) == 3u);

    /* Page allocations should consume the free-slot stack in LIFO order. */
    CHECK(sap_arena_alloc_page(arena, &page, &r1) == ERR_OK);
    CHECK(sap_arena_alloc_page(arena, &page, &r2) == ERR_OK);
    CHECK(sap_arena_alloc_page(arena, &page, &r3) == ERR_OK);
    CHECK(r1 == n3_id);
    CHECK(r2 == p2);
    CHECK(r3 == n2_id);
    CHECK(sap_arena_alloc_page(arena, &page, &r4) == ERR_OK);
    CHECK(r4 == 7u);
    CHECK(sap_arena_active_pages(arena) == 7u);

    CHECK(sap_arena_resolve(arena, r1) != NULL);
    CHECK(sap_arena_resolve(arena, r2) != NULL);
    CHECK(sap_arena_resolve(arena, r3) != NULL);
    CHECK(sap_arena_resolve(arena, r4) != NULL);

    sap_arena_destroy(arena);
}

int main(void)
{
    test_arena_init_destroy();
    test_arena_alloc_page();
    test_arena_alloc_node();
    test_arena_backing_strategy_switch();
    test_arena_exhaustion_behavior();
    test_arena_fragmentation_reuse();
    
    printf("All test_arena tests passed.\n");
    return 0;
}
