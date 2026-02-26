#include "sapling/arena.h"
#include <stdlib.h>
#include <string.h>

#define SAP_OK 0
#define SAP_ERROR -1
#define SAP_FULL -8

struct SapMemArena {
    SapArenaOptions opts;
    
    /* For SAP_ARENA_BACKING_MALLOC: track malloc'd chunks to avoid leaks */
    void **malloc_chunks;
    uint32_t chunk_count;
    uint32_t chunk_capacity;
    
    /* Simple free stack for page numbers */
    uint32_t *free_pgnos;
    uint32_t free_pgno_count;
    uint32_t free_pgno_capacity;

    /* Next available pgno if free list is empty */
    uint32_t next_pgno;
};

int sap_arena_init(SapMemArena **arena_out, const SapArenaOptions *opts)
{
    if (!arena_out || !opts)
        return SAP_ERROR;

    SapMemArena *a = malloc(sizeof(SapMemArena));
    if (!a)
        return SAP_FULL;

    memcpy(&a->opts, opts, sizeof(*opts));
    a->malloc_chunks = NULL;
    a->chunk_count = 0;
    a->chunk_capacity = 0;

    a->free_pgnos = NULL;
    a->free_pgno_count = 0;
    a->free_pgno_capacity = 0;

    a->next_pgno = 1; /* pgno 0 is typically reserved or root */

    *arena_out = a;

    return SAP_OK;
}

void sap_arena_destroy(SapMemArena *arena)
{
    if (arena) {
        if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) {
            for (uint32_t i = 0; i < arena->chunk_count; i++) {
                free(arena->malloc_chunks[i]);
            }
            free(arena->malloc_chunks);
        } else if (arena->opts.type == SAP_ARENA_BACKING_CUSTOM) {
            uint32_t eff_page_size = arena->opts.page_size ? arena->opts.page_size : SAPLING_PAGE_SIZE;
            for (uint32_t i = 0; i < arena->chunk_count; i++) {
                if (arena->malloc_chunks[i] && arena->opts.cfg.custom.free_page) {
                    arena->opts.cfg.custom.free_page(arena->opts.cfg.custom.ctx, arena->malloc_chunks[i], eff_page_size);
                }
            }
            free(arena->malloc_chunks);
        }
        
        free(arena->free_pgnos);
        free(arena);
    }
}

#ifndef SAPLING_PAGE_SIZE
#define SAPLING_PAGE_SIZE 4096
#endif

int sap_arena_alloc_page(SapMemArena *arena, void **page_out, uint32_t *pgno_out)
{
    if (!arena || !page_out || !pgno_out) return SAP_ERROR;

    uint32_t pgno;
    int from_free_list = 0;
    if (arena->free_pgno_count > 0) {
        pgno = arena->free_pgnos[--arena->free_pgno_count];
        from_free_list = 1;
    } else {
        pgno = arena->next_pgno++;
    }

    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC || arena->opts.type == SAP_ARENA_BACKING_CUSTOM) {
        void *page = NULL;
        uint32_t eff_page_size = arena->opts.page_size ? arena->opts.page_size : SAPLING_PAGE_SIZE;
        
        if (from_free_list && pgno < arena->chunk_count && arena->malloc_chunks[pgno]) {
            page = arena->malloc_chunks[pgno];
            if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) {
                memset(page, 0, eff_page_size); /* calloc behavior */
            }
        } else {
            if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) {
                page = calloc(1, eff_page_size);
            } else {
                if (!arena->opts.cfg.custom.alloc_page) return SAP_ERROR;
                page = arena->opts.cfg.custom.alloc_page(arena->opts.cfg.custom.ctx, eff_page_size);
            }
            
            if (!page) {
                if (!from_free_list && pgno == arena->next_pgno - 1) {
                    arena->next_pgno--;
                } else if (arena->free_pgno_count < arena->free_pgno_capacity) {
                    arena->free_pgnos[arena->free_pgno_count++] = pgno;
                }
                return SAP_FULL;
            }

            if (arena->chunk_count == arena->chunk_capacity) {
                uint32_t old_cap = arena->chunk_capacity;
                uint32_t new_cap = old_cap == 0 ? 16 : old_cap * 2;
                void **new_chunks = realloc(arena->malloc_chunks, new_cap * sizeof(void*));
                if (!new_chunks) {
                    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) free(page);
                    else if (arena->opts.cfg.custom.free_page) arena->opts.cfg.custom.free_page(arena->opts.cfg.custom.ctx, page, eff_page_size);
                    if (!from_free_list && pgno == arena->next_pgno - 1) arena->next_pgno--;
                    else if (arena->free_pgno_count < arena->free_pgno_capacity) arena->free_pgnos[arena->free_pgno_count++] = pgno;
                    return SAP_FULL;
                }
                memset((uint8_t *)new_chunks + old_cap * sizeof(void*), 0, (new_cap - old_cap) * sizeof(void*));
                arena->malloc_chunks = new_chunks;
                arena->chunk_capacity = new_cap;
            }

            if (pgno >= arena->chunk_capacity) {
                 uint32_t old_cap = arena->chunk_capacity;
                 uint32_t new_cap = pgno + 16;
                 void **new_chunks = realloc(arena->malloc_chunks, new_cap * sizeof(void*));
                 if (!new_chunks) {
                     if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) free(page);
                     else if (arena->opts.cfg.custom.free_page) arena->opts.cfg.custom.free_page(arena->opts.cfg.custom.ctx, page, eff_page_size);
                     if (!from_free_list && pgno == arena->next_pgno - 1) arena->next_pgno--;
                     else if (arena->free_pgno_count < arena->free_pgno_capacity) arena->free_pgnos[arena->free_pgno_count++] = pgno;
                     return SAP_FULL;
                 }
                 memset((uint8_t *)new_chunks + old_cap * sizeof(void*), 0, (new_cap - old_cap) * sizeof(void*));
                 arena->malloc_chunks = new_chunks;
                 arena->chunk_capacity = new_cap;
            }
            
            arena->malloc_chunks[pgno] = page;
            if (pgno >= arena->chunk_count) {
                arena->chunk_count = pgno + 1;
            }
        }
        
        *page_out = page;
        *pgno_out = pgno;
        return SAP_OK;
    }

    return SAP_ERROR; /* STUB for other backends */
}

int sap_arena_free_page(SapMemArena *arena, uint32_t pgno)
{
    if (!arena) return SAP_ERROR;

    if (arena->free_pgno_count == arena->free_pgno_capacity) {
        uint32_t new_cap = arena->free_pgno_capacity == 0 ? 16 : arena->free_pgno_capacity * 2;
        uint32_t *new_free = realloc(arena->free_pgnos, new_cap * sizeof(uint32_t));
        if (!new_free) return SAP_FULL; /* Cannot track free page, leak it */
        arena->free_pgnos = new_free;
        arena->free_pgno_capacity = new_cap;
    }

    arena->free_pgnos[arena->free_pgno_count++] = pgno;
    return SAP_OK;
}

int sap_arena_free_page_ptr(SapMemArena *arena, void *page)
{
    if (!arena || !page) return SAP_ERROR;

    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC || arena->opts.type == SAP_ARENA_BACKING_CUSTOM) {
        for (uint32_t i = 1; i < arena->chunk_count; i++) {
            if (arena->malloc_chunks[i] == page) {
                return sap_arena_free_page(arena, i);
            }
        }
    }
    return SAP_ERROR;
}

int sap_arena_alloc_node(SapMemArena *arena, uint32_t size, void **node_out, uint32_t *nodeno_out)
{
    if (!arena || !node_out || !nodeno_out) return SAP_ERROR;

    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) {
        /*
         * For the native MALLOC backend, we just allocate the requested node
         * using calloc, matching `SeqAllocator`'s behavior, but mapping it
         * into our tracked chunks list (or resolving it as a direct ptr index).
         */
        void *node = calloc(1, size);
        if (!node) return SAP_FULL;

        if (arena->chunk_count == arena->chunk_capacity) {
            uint32_t new_cap = arena->chunk_capacity == 0 ? 16 : arena->chunk_capacity * 2;
            void **new_chunks = realloc(arena->malloc_chunks, new_cap * sizeof(void*));
            if (!new_chunks) {
                free(node);
                return SAP_FULL;
            }
            arena->malloc_chunks = new_chunks;
            arena->chunk_capacity = new_cap;
        }

        uint32_t nodeno = arena->next_pgno++;
        if (nodeno >= arena->chunk_capacity) {
            uint32_t old_cap = arena->chunk_capacity;
            uint32_t new_cap = nodeno + 16;
            void **new_chunks = realloc(arena->malloc_chunks, new_cap * sizeof(void*));
            if (!new_chunks) {
                free(node);
                return SAP_FULL;
            }
            memset((uint8_t *)new_chunks + old_cap * sizeof(void*), 0, (new_cap - old_cap) * sizeof(void*));
            arena->malloc_chunks = new_chunks;
            arena->chunk_capacity = new_cap;
        }

        arena->malloc_chunks[nodeno] = node;
        if (nodeno >= arena->chunk_count) {
            arena->chunk_count = nodeno + 1;
        }

        *node_out = node;
        *nodeno_out = nodeno;
        return SAP_OK;
    }

    return SAP_ERROR;
}

int sap_arena_free_node(SapMemArena *arena, uint32_t nodeno, uint32_t size)
{
    /* 
     * In MALLOC backend, nodes occupy a chunk slot but we don't immediately
     * drop the ID into the free_pgnos list since we aren't pooling non-page
     * sized pieces. We just release the memory. Node-level pooling per-size
     * is deferred to WASI blocks. 
     */
    if (!arena) return SAP_ERROR;
    
    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) {
        if (nodeno < arena->chunk_count && arena->malloc_chunks[nodeno]) {
            free(arena->malloc_chunks[nodeno]);
            arena->malloc_chunks[nodeno] = NULL;
            /* In MALLOC we don't actively reuse this ID to avoid size-class
             * complexity in this simple backend loop. */
            return SAP_OK;
        }
    }

    (void)size;
    return SAP_ERROR;
}

void *sap_arena_resolve(SapMemArena *arena, uint32_t p_or_n_no)
{
    if (!arena) return NULL;
    
    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC || arena->opts.type == SAP_ARENA_BACKING_CUSTOM) {
        if (p_or_n_no < arena->chunk_count) {
            return arena->malloc_chunks[p_or_n_no];
        }
    }
    return NULL;
}

uint32_t sap_arena_active_pages(const SapMemArena *arena)
{
    if (!arena) return 0;
    return arena->chunk_count - (arena->opts.type == SAP_ARENA_BACKING_MALLOC || arena->opts.type == SAP_ARENA_BACKING_CUSTOM ? 1 : 0) - arena->free_pgno_count;
    /* note: pgno 0 is reserved so chunk_count includes 1 extra unused slot for MALLOC/CUSTOM backends */
}
