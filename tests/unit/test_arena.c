#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

int main(void)
{
    test_arena_init_destroy();
    test_arena_alloc_page();
    test_arena_alloc_node();
    
    printf("All test_arena tests passed.\n");
    return 0;
}
