#ifndef SAPLING_ARENA_H
#define SAPLING_ARENA_H

#include <stdint.h>
#include <stddef.h>
#include <sapling/err.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * SapMemArena: Universal Wasm-friendly linear memory allocator.
 *
 * This arena manages both 4KB pages (for B+ Trees) and smaller nodes (for
 * Finger Trees, Tries, text ropes). It is designed to sit on top of diverse
 * backing strategies (mmap for native, WASI filesystem, or simple contiguous
 * array growth for Web Workers/Universal Wasm).
 */
typedef struct SapMemArena SapMemArena;

/* Configuration for the underlying backing strategy */
typedef enum {
    SAP_ARENA_BACKING_MALLOC = 0, /* standard native chunking */
    SAP_ARENA_BACKING_MMAP,       /* native file-backed mmap */
    SAP_ARENA_BACKING_WASI_FD,    /* WASI filesystem fd */
    SAP_ARENA_BACKING_LINEAR,     /* simple array expansion (browser/Workers) */
    SAP_ARENA_BACKING_CUSTOM      /* custom callbacks (used for test tracking) */
} SapArenaBackingType;

typedef struct {
    SapArenaBackingType type;
    uint32_t page_size;
    union {
        struct {
            int fd;
            uint64_t max_size;
        } mmap;
        struct {
            uint64_t initial_bytes;
            uint64_t max_bytes;
        } linear;
        struct {
            void *(*alloc_page)(void *ctx, uint32_t page_size);
            void (*free_page)(void *ctx, void *page, uint32_t page_size);
            void *ctx;
        } custom;
    } cfg;
} SapArenaOptions;

/*
 * Unified allocation telemetry across arena page/node allocators, transaction
 * scratch allocation, and SapTxnVec reserve growth.
 */
typedef struct {
    uint64_t page_alloc_calls;
    uint64_t page_alloc_ok;
    uint64_t page_alloc_oom;
    uint64_t page_free_calls;
    uint64_t page_free_ok;

    uint64_t node_alloc_calls;
    uint64_t node_alloc_ok;
    uint64_t node_alloc_oom;
    uint64_t node_free_calls;
    uint64_t node_free_ok;

    uint64_t scratch_alloc_calls;
    uint64_t scratch_alloc_ok;
    uint64_t scratch_alloc_fail;
    uint64_t scratch_bytes_requested;
    uint64_t scratch_bytes_granted;

    uint64_t txn_vec_reserve_calls;
    uint64_t txn_vec_reserve_ok;
    uint64_t txn_vec_reserve_oom;
    uint64_t txn_vec_bytes_requested;
    uint64_t txn_vec_bytes_allocated;

    uint64_t budget_reject_active_slots;
    uint64_t budget_reject_scratch_bytes;
    uint64_t budget_reject_txn_vec_bytes;

    uint64_t active_slots_current;
    uint64_t active_slots_high_water;
} SapArenaAllocStats;

/*
 * Optional allocation budgets. Zero means "unlimited".
 */
typedef struct {
    uint64_t max_active_slots;
    uint64_t max_scratch_request_bytes;
    uint64_t max_txn_vec_reserve_bytes;
} SapArenaAllocBudget;

/* 
 * Initialize a new arena.
 */
int sap_arena_init(SapMemArena **arena_out, const SapArenaOptions *opts);

/* 
 * Clean up the arena and sync any backing stores.
 */
void sap_arena_destroy(SapMemArena *arena);

/*
 * Page allocation: Retrieves an entire contiguous chunk (typically SAPLING_PAGE_SIZE)
 * Returns ERR_OK on success, ERR_OOM on capacity exhaustion.
 */
int sap_arena_alloc_page(SapMemArena *arena, void **page_out, uint32_t *pgno_out);

/*
 * Free a page back to the arena's free list.
 */
int sap_arena_free_page(SapMemArena *arena, uint32_t pgno);

/*
 * Free a page by its process pointer (helper for systems not tracking IDs).
 */
int sap_arena_free_page_ptr(SapMemArena *arena, void *page);

/*
 * Node allocation: Retrieves a precise, smaller-than-page byte boundary.
 * Suitable for Finger Tree node/leaf variants or Trie nodes.
 */
int sap_arena_alloc_node(SapMemArena *arena, uint32_t size, void **node_out, uint32_t *nodeno_out);

/*
 * Free a sub-page node.
 */
int sap_arena_free_node(SapMemArena *arena, uint32_t nodeno, uint32_t size);

/*
 * Free a sub-page node by pointer.
 */
int sap_arena_free_node_ptr(SapMemArena *arena, void *node, uint32_t size);

/*
 * Memory inspection: Returns the number of currently active/held pages not in the free list.
 */
uint32_t sap_arena_active_pages(const SapMemArena *arena);

/*
 * Pointer resolution: Map a page/node integer reference back to a process pointer.
 */
void *sap_arena_resolve(SapMemArena *arena, uint32_t p_or_n_no);

/*
 * Read/reset aggregate allocation telemetry.
 */
int sap_arena_alloc_stats(const SapMemArena *arena, SapArenaAllocStats *out);
int sap_arena_alloc_stats_reset(SapMemArena *arena);
int sap_arena_alloc_stats_diff(const SapArenaAllocStats *start, const SapArenaAllocStats *end,
                               SapArenaAllocStats *delta_out);

/*
 * Configure/query optional allocation budgets.
 */
int sap_arena_set_alloc_budget(SapMemArena *arena, const SapArenaAllocBudget *budget);
int sap_arena_get_alloc_budget(const SapMemArena *arena, SapArenaAllocBudget *budget_out);

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_ARENA_H */
