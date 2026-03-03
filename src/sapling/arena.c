#include "sapling/arena.h"
#include "common/arena_alloc_internal.h"
#include <stdlib.h>
#include <string.h>

struct SapMemArena {
    SapArenaOptions opts;
    SapArenaAllocStats stats;
    SapArenaAllocBudget budget;
    
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

static int arena_is_slot_backed(const SapMemArena *arena)
{
    return arena && (arena->opts.type == SAP_ARENA_BACKING_MALLOC ||
                     arena->opts.type == SAP_ARENA_BACKING_CUSTOM);
}

static uint64_t arena_active_slots_raw(const SapMemArena *arena)
{
    uint64_t reserved_slots = 0;
    uint64_t live_slots = 0;

    if (!arena)
        return 0;

    if (arena_is_slot_backed(arena))
        reserved_slots = 1; /* slot 0 is reserved */
    if (arena->chunk_count <= reserved_slots)
        return 0;

    live_slots = (uint64_t)arena->chunk_count - reserved_slots;
    if (arena->free_pgno_count >= live_slots)
        return 0;
    return live_slots - arena->free_pgno_count;
}

static void arena_refresh_active_stats(SapMemArena *arena)
{
    uint64_t active = 0;
    if (!arena)
        return;
    active = arena_active_slots_raw(arena);
    arena->stats.active_slots_current = active;
    if (active > arena->stats.active_slots_high_water)
        arena->stats.active_slots_high_water = active;
}

static int arena_budget_reject_active_slots(SapMemArena *arena)
{
    uint64_t max_slots = 0;
    if (!arena)
        return 0;
    max_slots = arena->budget.max_active_slots;
    if (max_slots == 0)
        return 0;
    if (arena_active_slots_raw(arena) + 1 > max_slots) {
        arena->stats.budget_reject_active_slots++;
        return 1;
    }
    return 0;
}

int sap_arena_init(SapMemArena **arena_out, const SapArenaOptions *opts)
{
    if (!arena_out || !opts)
        return ERR_INVALID;

    SapMemArena *a = malloc(sizeof(SapMemArena));
    if (!a)
        return ERR_OOM;

    memcpy(&a->opts, opts, sizeof(*opts));
    memset(&a->stats, 0, sizeof(a->stats));
    memset(&a->budget, 0, sizeof(a->budget));
    a->malloc_chunks = NULL;
    a->chunk_count = 0;
    a->chunk_capacity = 0;

    a->free_pgnos = NULL;
    a->free_pgno_count = 0;
    a->free_pgno_capacity = 0;

    a->next_pgno = 1; /* pgno 0 is typically reserved or root */
    arena_refresh_active_stats(a);

    *arena_out = a;

    return ERR_OK;
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
    if (!arena || !page_out || !pgno_out)
        return ERR_INVALID;

    arena->stats.page_alloc_calls++;
    if (arena_budget_reject_active_slots(arena)) {
        arena->stats.page_alloc_oom++;
        return ERR_OOM;
    }

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
                if (!arena->opts.cfg.custom.alloc_page) return ERR_INVALID;
                page = arena->opts.cfg.custom.alloc_page(arena->opts.cfg.custom.ctx, eff_page_size);
            }
            
            if (!page) {
                if (!from_free_list && pgno == arena->next_pgno - 1) {
                    arena->next_pgno--;
                } else if (arena->free_pgno_count < arena->free_pgno_capacity) {
                    arena->free_pgnos[arena->free_pgno_count++] = pgno;
                }
                arena->stats.page_alloc_oom++;
                return ERR_OOM;
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
                    arena->stats.page_alloc_oom++;
                    return ERR_OOM;
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
                     arena->stats.page_alloc_oom++;
                     return ERR_OOM;
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
        arena->stats.page_alloc_ok++;
        arena_refresh_active_stats(arena);
        return ERR_OK;
    }

    return ERR_INVALID; /* STUB for other backends */
}

int sap_arena_free_page(SapMemArena *arena, uint32_t pgno)
{
    if (!arena)
        return ERR_INVALID;
    arena->stats.page_free_calls++;

    if (arena->free_pgno_count == arena->free_pgno_capacity) {
        uint32_t new_cap = arena->free_pgno_capacity == 0 ? 16 : arena->free_pgno_capacity * 2;
        uint32_t *new_free = realloc(arena->free_pgnos, new_cap * sizeof(uint32_t));
        if (!new_free)
            return ERR_OOM; /* Cannot track free page, leak it */
        arena->free_pgnos = new_free;
        arena->free_pgno_capacity = new_cap;
    }

    arena->free_pgnos[arena->free_pgno_count++] = pgno;
    arena->stats.page_free_ok++;
    arena_refresh_active_stats(arena);
    return ERR_OK;
}

int sap_arena_free_page_ptr(SapMemArena *arena, void *page)
{
    if (!arena || !page) return ERR_INVALID;

    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC || arena->opts.type == SAP_ARENA_BACKING_CUSTOM) {
        for (uint32_t i = 1; i < arena->chunk_count; i++) {
            if (arena->malloc_chunks[i] == page) {
                return sap_arena_free_page(arena, i);
            }
        }
    }
    return ERR_NOT_FOUND;
}

int sap_arena_alloc_node(SapMemArena *arena, uint32_t size, void **node_out, uint32_t *nodeno_out)
{
    if (!arena || !node_out || !nodeno_out)
        return ERR_INVALID;

    arena->stats.node_alloc_calls++;
    if (arena_budget_reject_active_slots(arena)) {
        arena->stats.node_alloc_oom++;
        return ERR_OOM;
    }

    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC || arena->opts.type == SAP_ARENA_BACKING_CUSTOM) {
        void *node;
        if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) {
            node = calloc(1, size);
        } else {
            if (!arena->opts.cfg.custom.alloc_page) return ERR_INVALID;
            node = arena->opts.cfg.custom.alloc_page(arena->opts.cfg.custom.ctx, size);
            if (node) memset(node, 0, size);
        }
        if (!node) {
            arena->stats.node_alloc_oom++;
            return ERR_OOM;
        }

        if (arena->chunk_count == arena->chunk_capacity) {
            uint32_t old_cap = arena->chunk_capacity;
            uint32_t new_cap = old_cap == 0 ? 16 : old_cap * 2;
            void **new_chunks = realloc(arena->malloc_chunks, new_cap * sizeof(void*));
            if (!new_chunks) {
                if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) free(node);
                else if (arena->opts.cfg.custom.free_page) arena->opts.cfg.custom.free_page(arena->opts.cfg.custom.ctx, node, size);
                arena->stats.node_alloc_oom++;
                return ERR_OOM;
            }
            memset((uint8_t *)new_chunks + old_cap * sizeof(void*), 0, (new_cap - old_cap) * sizeof(void*));
            arena->malloc_chunks = new_chunks;
            arena->chunk_capacity = new_cap;
        }

        uint32_t nodeno = arena->next_pgno++;
        if (nodeno >= arena->chunk_capacity) {
            uint32_t old_cap = arena->chunk_capacity;
            uint32_t new_cap = nodeno + 16;
            void **new_chunks = realloc(arena->malloc_chunks, new_cap * sizeof(void*));
            if (!new_chunks) {
                if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) free(node);
                else if (arena->opts.cfg.custom.free_page) arena->opts.cfg.custom.free_page(arena->opts.cfg.custom.ctx, node, size);
                arena->next_pgno--;
                arena->stats.node_alloc_oom++;
                return ERR_OOM;
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
        arena->stats.node_alloc_ok++;
        arena_refresh_active_stats(arena);
        return ERR_OK;
    }

    return ERR_INVALID;
}

int sap_arena_free_node(SapMemArena *arena, uint32_t nodeno, uint32_t size)
{
    (void)size;
    /*
     * Free a node's memory and return its slot ID to the free_pgnos list
     * so that sap_arena_active_pages stays accurate and the slot can be
     * reused by future allocations.
     */
    if (!arena)
        return ERR_INVALID;
    arena->stats.node_free_calls++;

    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC || arena->opts.type == SAP_ARENA_BACKING_CUSTOM) {
        if (nodeno < arena->chunk_count && arena->malloc_chunks[nodeno]) {
            void *p = arena->malloc_chunks[nodeno];
            arena->malloc_chunks[nodeno] = NULL;
            if (arena->opts.type == SAP_ARENA_BACKING_MALLOC) {
                free(p);
            } else if (arena->opts.cfg.custom.free_page) {
                arena->opts.cfg.custom.free_page(arena->opts.cfg.custom.ctx, p, size);
            }
            /* Track the freed slot so sap_arena_active_pages is accurate
             * and the slot can be reused by future allocations. */
            if (arena->free_pgno_count == arena->free_pgno_capacity) {
                uint32_t new_cap = arena->free_pgno_capacity == 0 ? 16 : arena->free_pgno_capacity * 2;
                uint32_t *new_free = realloc(arena->free_pgnos, new_cap * sizeof(uint32_t));
                if (new_free) {
                    arena->free_pgnos = new_free;
                    arena->free_pgno_capacity = new_cap;
                }
            }
            if (arena->free_pgno_count < arena->free_pgno_capacity) {
                arena->free_pgnos[arena->free_pgno_count++] = nodeno;
            }
            arena->stats.node_free_ok++;
            arena_refresh_active_stats(arena);
            return ERR_OK;
        }
    }

    return ERR_NOT_FOUND;
}

int sap_arena_free_node_ptr(SapMemArena *arena, void *node, uint32_t size)
{
    if (!arena || !node) return ERR_INVALID;

    if (arena->opts.type == SAP_ARENA_BACKING_MALLOC || arena->opts.type == SAP_ARENA_BACKING_CUSTOM) {
        for (uint32_t i = 1; i < arena->chunk_count; i++) {
            if (arena->malloc_chunks[i] == node) {
                return sap_arena_free_node(arena, i, size);
            }
        }
        return ERR_NOT_FOUND;
    }

    (void)size;
    return ERR_INVALID;
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
    return (uint32_t)arena_active_slots_raw(arena);
}

int sap_arena_alloc_stats(const SapMemArena *arena, SapArenaAllocStats *out)
{
    if (!arena || !out)
        return ERR_INVALID;
    *out = arena->stats;
    return ERR_OK;
}

int sap_arena_alloc_stats_reset(SapMemArena *arena)
{
    if (!arena)
        return ERR_INVALID;
    memset(&arena->stats, 0, sizeof(arena->stats));
    arena_refresh_active_stats(arena);
    return ERR_OK;
}

#define DIFF_FIELD(name)                                                                           \
    do {                                                                                           \
        if (end->name >= start->name)                                                              \
            delta_out->name = end->name - start->name;                                             \
        else                                                                                       \
            delta_out->name = 0;                                                                   \
    } while (0)

int sap_arena_alloc_stats_diff(const SapArenaAllocStats *start, const SapArenaAllocStats *end,
                               SapArenaAllocStats *delta_out)
{
    if (!start || !end || !delta_out)
        return ERR_INVALID;
    memset(delta_out, 0, sizeof(*delta_out));

    DIFF_FIELD(page_alloc_calls);
    DIFF_FIELD(page_alloc_ok);
    DIFF_FIELD(page_alloc_oom);
    DIFF_FIELD(page_free_calls);
    DIFF_FIELD(page_free_ok);

    DIFF_FIELD(node_alloc_calls);
    DIFF_FIELD(node_alloc_ok);
    DIFF_FIELD(node_alloc_oom);
    DIFF_FIELD(node_free_calls);
    DIFF_FIELD(node_free_ok);

    DIFF_FIELD(scratch_alloc_calls);
    DIFF_FIELD(scratch_alloc_ok);
    DIFF_FIELD(scratch_alloc_fail);
    DIFF_FIELD(scratch_bytes_requested);
    DIFF_FIELD(scratch_bytes_granted);

    DIFF_FIELD(txn_vec_reserve_calls);
    DIFF_FIELD(txn_vec_reserve_ok);
    DIFF_FIELD(txn_vec_reserve_oom);
    DIFF_FIELD(txn_vec_bytes_requested);
    DIFF_FIELD(txn_vec_bytes_allocated);

    DIFF_FIELD(budget_reject_active_slots);
    DIFF_FIELD(budget_reject_scratch_bytes);
    DIFF_FIELD(budget_reject_txn_vec_bytes);

    /* snapshots for current/high-water are absolute and monotonic on end */
    delta_out->active_slots_current = end->active_slots_current;
    delta_out->active_slots_high_water = end->active_slots_high_water;
    return ERR_OK;
}

#undef DIFF_FIELD

int sap_arena_set_alloc_budget(SapMemArena *arena, const SapArenaAllocBudget *budget)
{
    if (!arena || !budget)
        return ERR_INVALID;
    arena->budget = *budget;
    return ERR_OK;
}

int sap_arena_get_alloc_budget(const SapMemArena *arena, SapArenaAllocBudget *budget_out)
{
    if (!arena || !budget_out)
        return ERR_INVALID;
    *budget_out = arena->budget;
    return ERR_OK;
}

int sap_arena_alloc_budget_check_scratch(SapMemArena *arena, uint64_t requested_bytes)
{
    if (!arena)
        return ERR_INVALID;
    if (arena->budget.max_scratch_request_bytes != 0 &&
        requested_bytes > arena->budget.max_scratch_request_bytes) {
        arena->stats.budget_reject_scratch_bytes++;
        return ERR_OOM;
    }
    return ERR_OK;
}

int sap_arena_alloc_budget_check_txn_vec(SapMemArena *arena, uint64_t requested_bytes)
{
    if (!arena)
        return ERR_INVALID;
    if (arena->budget.max_txn_vec_reserve_bytes != 0 &&
        requested_bytes > arena->budget.max_txn_vec_reserve_bytes) {
        arena->stats.budget_reject_txn_vec_bytes++;
        return ERR_OOM;
    }
    return ERR_OK;
}

void sap_arena_alloc_note_scratch(SapMemArena *arena, uint64_t requested_bytes,
                                  uint64_t granted_bytes, int ok)
{
    if (!arena)
        return;
    arena->stats.scratch_alloc_calls++;
    arena->stats.scratch_bytes_requested += requested_bytes;
    if (ok) {
        arena->stats.scratch_alloc_ok++;
        arena->stats.scratch_bytes_granted += granted_bytes;
    } else {
        arena->stats.scratch_alloc_fail++;
    }
}

void sap_arena_alloc_note_txn_vec(SapMemArena *arena, uint64_t requested_bytes,
                                  uint64_t granted_bytes, int ok)
{
    if (!arena)
        return;
    arena->stats.txn_vec_reserve_calls++;
    arena->stats.txn_vec_bytes_requested += requested_bytes;
    if (ok) {
        arena->stats.txn_vec_reserve_ok++;
        arena->stats.txn_vec_bytes_allocated += granted_bytes;
    } else {
        arena->stats.txn_vec_reserve_oom++;
    }
}
