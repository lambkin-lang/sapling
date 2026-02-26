#ifndef SAPLING_ARENA_H
#define SAPLING_ARENA_H

#include <stdint.h>
#include <stddef.h>

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
    SAP_ARENA_BACKING_LINEAR      /* simple array expansion (browser/Workers) */
} SapArenaBackingType;

typedef struct {
    SapArenaBackingType type;
    union {
        struct {
            int fd;
            uint64_t max_size;
        } mmap;
        struct {
            uint64_t initial_bytes;
            uint64_t max_bytes;
        } linear;
    } cfg;
} SapArenaOptions;

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
 * Returns SAP_OK on success, SAP_FULL on capacity exhaustion.
 */
int sap_arena_alloc_page(SapMemArena *arena, void **page_out, uint32_t *pgno_out);

/*
 * Free a page back to the arena's free list.
 */
int sap_arena_free_page(SapMemArena *arena, uint32_t pgno);

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
 * Pointer resolution: Map a page/node integer reference back to a process pointer.
 */
void *sap_arena_resolve(SapMemArena *arena, uint32_t p_or_n_no);

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_ARENA_H */
