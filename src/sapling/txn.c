#include "sapling/txn.h"
#include <stdlib.h>
#include <string.h>

struct SapEnv {
    SapMemArena *arena;
    SapTxnSubsystemCallbacks subsystems[SAP_MAX_SUBSYSTEMS];
    void *subsystem_env_states[SAP_MAX_SUBSYSTEMS];
    uint32_t active_subs;
    uint32_t page_size;
};

struct SapTxnCtx {
    SapEnv *env;
    struct SapTxnCtx *parent;
    uint64_t txnid;
    unsigned int flags;
    void *subsystem_states[SAP_MAX_SUBSYSTEMS];
    /* Scratch bump allocator for small per-transaction allocations */
    uint8_t *scratch_page;
    uint32_t scratch_pgno;
    uint32_t scratch_head;
    uint32_t scratch_cap;
};

SapEnv *sap_env_create(SapMemArena *arena, uint32_t page_size) {
    SapEnv *env = calloc(1, sizeof(SapEnv));
    if (env) {
        env->arena = arena;
        env->page_size = page_size ? page_size : 4096;
    }
    return env;
}

void sap_env_destroy(SapEnv *env) {
    if (env) {
        for (uint32_t i = 0; i < env->active_subs; i++) {
            if (env->subsystems[i].on_env_destroy) {
                env->subsystems[i].on_env_destroy(env->subsystem_env_states[i]);
            }
        }
        free(env);
    }
}

SapMemArena *sap_env_get_arena(SapEnv *env) {
    return env ? env->arena : NULL;
}

uint32_t sap_env_get_page_size(SapEnv *env) {
    return env ? env->page_size : 0;
}

void *sap_env_subsystem_state(SapEnv *env, uint32_t sys_id) {
    if (!env || sys_id >= SAP_MAX_SUBSYSTEMS) return NULL;
    return env->subsystem_env_states[sys_id];
}

int sap_env_set_subsystem_state(SapEnv *env, uint32_t sys_id, void *state) {
    if (!env || sys_id >= SAP_MAX_SUBSYSTEMS) return ERR_INVALID;
    env->subsystem_env_states[sys_id] = state;
    return ERR_OK;
}

int sap_env_register_subsystem(SapEnv *env, uint32_t sys_id, const SapTxnSubsystemCallbacks *cbs) {
    if (!env || !cbs || sys_id >= SAP_MAX_SUBSYSTEMS) return ERR_INVALID;
    env->subsystems[sys_id] = *cbs;
    if (sys_id >= env->active_subs) {
        env->active_subs = sys_id + 1;
    }
    return ERR_OK;
}

SapMemArena *sap_txn_arena(SapTxnCtx *txn) {
    return txn ? txn->env->arena : NULL;
}

SapEnv *sap_txn_env(SapTxnCtx *txn) {
    return txn ? txn->env : NULL;
}

unsigned int sap_txn_flags(SapTxnCtx *txn) {
    return txn ? txn->flags : 0;
}

void *sap_txn_subsystem_state(SapTxnCtx *txn, uint32_t sys_id) {
    if (!txn || sys_id >= SAP_MAX_SUBSYSTEMS) return NULL;
    return txn->subsystem_states[sys_id];
}

int sap_txn_set_subsystem_state(SapTxnCtx *txn, uint32_t sys_id, void *state) {
    if (!txn || sys_id >= SAP_MAX_SUBSYSTEMS) return ERR_INVALID;
    txn->subsystem_states[sys_id] = state;
    return ERR_OK;
}

SapTxnCtx *sap_txn_begin(SapEnv *env, SapTxnCtx *parent, unsigned int flags)
{
    if (!env) return NULL;
    
    SapTxnCtx *txn = calloc(1, sizeof(SapTxnCtx));
    if (!txn) return NULL;
    
    txn->env = env;
    txn->parent = parent;
    txn->flags = flags;
    
    /* Initialize subsystem states */
    for (uint32_t i = 0; i < env->active_subs; i++) {
        if (env->subsystems[i].on_begin) {
            void *state = NULL;
            int rc = env->subsystems[i].on_begin(txn, 
                parent ? parent->subsystem_states[i] : NULL, &state);
            txn->subsystem_states[i] = state;
            if (rc != ERR_OK) {
                sap_txn_abort(txn);
                return NULL;
            }
        }
    }
    
    return txn;
}

int sap_txn_commit(SapTxnCtx *txn)
{
    if (!txn) return ERR_INVALID;
    SapEnv *env = txn->env;

    for (uint32_t i = 0; i < env->active_subs; i++) {
        if (env->subsystems[i].on_commit) {
            int rc = env->subsystems[i].on_commit(txn, txn->subsystem_states[i]);
            if (rc != ERR_OK) {
                sap_txn_abort(txn);
                return rc;
            }
        }
    }

    if (txn->scratch_page) {
        sap_arena_free_page(env->arena, txn->scratch_pgno);
    }
    free(txn);
    return ERR_OK;
}

void sap_txn_abort(SapTxnCtx *txn)
{
    if (!txn) return;
    SapEnv *env = txn->env;

    for (uint32_t i = 0; i < env->active_subs; i++) {
        if (env->subsystems[i].on_abort) {
            env->subsystems[i].on_abort(txn, txn->subsystem_states[i]);
        }
    }

    if (txn->scratch_page) {
        sap_arena_free_page(env->arena, txn->scratch_pgno);
    }
    free(txn);
}

void *sap_txn_scratch_alloc(SapTxnCtx *txn, uint32_t len)
{
    if (!txn || len == 0) return NULL;

    /* Lazy-allocate the scratch page on first use */
    if (!txn->scratch_page) {
        void *page = NULL;
        uint32_t pgno = 0;
        if (sap_arena_alloc_page(txn->env->arena, &page, &pgno) != ERR_OK) {
            return NULL;
        }
        txn->scratch_page = (uint8_t *)page;
        txn->scratch_pgno = pgno;
        txn->scratch_head = 0;
        txn->scratch_cap = txn->env->page_size;
    }

    /* Align to pointer size for safe struct placement */
    uint32_t align = (uint32_t)sizeof(void *);
    uint32_t aligned = (txn->scratch_head + align - 1) & ~(align - 1);
    if (aligned + len > txn->scratch_cap) return NULL;

    void *ptr = txn->scratch_page + aligned;
    txn->scratch_head = aligned + len;
    return ptr;
}
