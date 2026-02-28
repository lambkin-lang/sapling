#ifndef SAPLING_TXN_H
#define SAPLING_TXN_H

#include "sapling/sapling.h"
#include "sapling/arena.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Maximum number of supported subsystems (e.g., DB, Seq, Trie, Thatch) */
#define SAP_MAX_SUBSYSTEMS 8

/* Supported subsystem IDs */
#define SAP_SUBSYSTEM_DB      0
#define SAP_SUBSYSTEM_SEQ     1
#define SAP_SUBSYSTEM_BEPT    2
#define SAP_SUBSYSTEM_BEPT128 3
#define SAP_SUBSYSTEM_THATCH  4

typedef struct SapTxnCtx SapTxnCtx;

/*
 * Subsystem callback definitions for transaction events.
 * subsystem_ctx is the per-subsystem state (like TxnDBs array).
 */
typedef struct {
    /* Called when a transaction starts. Returns SAP_OK or error. Sets *state_out. */
    int (*on_begin)(SapTxnCtx *txn, void *parent_state, void **state_out);

    /* Called when the transaction commits. The subsystem should merge its state
       into the parent transaction (if nested) or persist it to the snapshot root. */
    int (*on_commit)(SapTxnCtx *txn, void *state);
    
    /* Called when the transaction aborts. The subsystem should free temporarily
       allocated resources specific to this subsystem. */
    void (*on_abort)(SapTxnCtx *txn, void *state);
    
    /* Called when the environment is destroyed. The subsystem should free
       all resources associated with its environment state. */
    void (*on_env_destroy)(void *env_state);
} SapTxnSubsystemCallbacks;

/* Initialize a transaction context tree manager *for the environment*.
 * This differs from sapling.c where DB was the overarching manager.
 */
typedef struct SapEnv SapEnv;

SapEnv *sap_env_create(SapMemArena *arena, uint32_t page_size);
void sap_env_destroy(SapEnv *env);

/* Accessors for environment properties */
SapMemArena *sap_env_get_arena(SapEnv *env);
uint32_t sap_env_get_page_size(SapEnv *env);

/* Thread-safe registry for subsystem callbacks across the environment */
int sap_env_register_subsystem(SapEnv *env, uint32_t sys_id, const SapTxnSubsystemCallbacks *cbs);

/* Centralized transaction lifecycle */
void *sap_env_subsystem_state(SapEnv *env, uint32_t sys_id);
int sap_env_set_subsystem_state(SapEnv *env, uint32_t sys_id, void *state);

SapTxnCtx *sap_txn_begin(SapEnv *env, SapTxnCtx *parent, unsigned int flags);
int sap_txn_commit(SapTxnCtx *txn);
void sap_txn_abort(SapTxnCtx *txn);

/* Access underlying subsystem state within the ongoing transaction */
void *sap_txn_subsystem_state(SapTxnCtx *txn, uint32_t sys_id);
int sap_txn_set_subsystem_state(SapTxnCtx *txn, uint32_t sys_id, void *state);

/* Expose central arena and environment from the context */
SapMemArena *sap_txn_arena(SapTxnCtx *txn);
SapEnv *sap_txn_env(SapTxnCtx *txn);
unsigned int sap_txn_flags(SapTxnCtx *txn);

/* Nested scratch memory allocation logic */
void *sap_txn_scratch_alloc(SapTxnCtx *txn, uint32_t len);

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_TXN_H */
