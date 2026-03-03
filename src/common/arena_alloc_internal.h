#ifndef SAPLING_ARENA_ALLOC_INTERNAL_H
#define SAPLING_ARENA_ALLOC_INTERNAL_H

#include "sapling/arena.h"

#ifdef __cplusplus
extern "C" {
#endif

int sap_arena_alloc_budget_check_scratch(SapMemArena *arena, uint64_t requested_bytes);
int sap_arena_alloc_budget_check_txn_vec(SapMemArena *arena, uint64_t requested_bytes);
void sap_arena_alloc_note_scratch(SapMemArena *arena, uint64_t requested_bytes,
                                  uint64_t granted_bytes, int ok);
void sap_arena_alloc_note_txn_vec(SapMemArena *arena, uint64_t requested_bytes,
                                  uint64_t granted_bytes, int ok);

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_ARENA_ALLOC_INTERNAL_H */
