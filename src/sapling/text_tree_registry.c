/*
 * text_tree_registry.c - COW tree sharing registry for cross-worker text transfer
 *
 * Follows the Thatch lifecycle pattern: entries are owned during a
 * transaction (single-writer, no lock needed), sealed on commit
 * (become immutable), and lock-free readable after seal.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text_tree_registry.h"
#include "sapling/seq.h"
#include "sapling/txn_vec.h"

#include <stdatomic.h>
#include <string.h>

#include "sapling/nomalloc.h"

/* ===== Internal Types ===== */

typedef struct {
    Text *text;                /* retained via text_clone; freed on last release */
    _Atomic uint32_t refs;     /* TREE handle refcount; atomic for cross-worker release */
} TextTreeEntry;

struct TextTreeRegistry {
    SapEnv *env;
    SapTxnVec entries;         /* arena-backed; elem_size = sizeof(TextTreeEntry) */
    uint32_t nodeno;           /* arena node number of this struct */
};

/* ===== Constants ===== */

#define TREE_INITIAL_CAP 16u
#define TREE_MAX_ID      0x3FFFFFFFu /* 30-bit payload limit */

/* ===== Lifecycle ===== */

TextTreeRegistry *text_tree_registry_new(SapEnv *env)
{
    if (!env) return NULL;

    SapMemArena *arena = sap_env_get_arena(env);
    TextTreeRegistry *reg = NULL;
    uint32_t nodeno = 0;
    if (sap_arena_alloc_node(arena, (uint32_t)sizeof(TextTreeRegistry),
                             (void **)&reg, &nodeno) != ERR_OK)
        return NULL;
    memset(reg, 0, sizeof(*reg));

    reg->env = env;
    reg->nodeno = nodeno;

    if (sap_txn_vec_init(&reg->entries, arena, sizeof(TextTreeEntry),
                         TREE_INITIAL_CAP) != ERR_OK)
    {
        sap_arena_free_node(arena, nodeno, (uint32_t)sizeof(TextTreeRegistry));
        return NULL;
    }

    return reg;
}

void text_tree_registry_free(TextTreeRegistry *reg)
{
    if (!reg) return;

    /* Release every live Text before tearing down the backing storage */
    for (uint32_t i = 0; i < sap_txn_vec_len(&reg->entries); i++)
    {
        TextTreeEntry *e = (TextTreeEntry *)sap_txn_vec_at(&reg->entries, i);
        if (e && e->text)
        {
            text_free(reg->env, e->text);
            e->text = NULL;
        }
    }

    sap_txn_vec_destroy(&reg->entries);
    /* arena-allocated; free the struct node */
    sap_arena_free_node(sap_env_get_arena(reg->env), reg->nodeno,
                        (uint32_t)sizeof(TextTreeRegistry));
}

/* ===== Registration ===== */

int text_tree_registry_register(TextTreeRegistry *reg, const Text *text,
                                uint32_t *id_out)
{
    if (!reg || !text || !id_out)
        return ERR_INVALID;

    uint32_t cur_len = sap_txn_vec_len(&reg->entries);
    if (cur_len > TREE_MAX_ID)
        return ERR_INVALID;

    /* COW clone — text_clone bumps the shared refcount */
    Text *clone = text_clone(reg->env, text);
    if (!clone)
        return ERR_OOM;

    TextTreeEntry entry;
    entry.text = clone;
    atomic_init(&entry.refs, 1u);

    int rc = sap_txn_vec_push(&reg->entries, &entry);
    if (rc != ERR_OK)
    {
        text_free(reg->env, clone);
        return rc;
    }

    *id_out = cur_len;
    return ERR_OK;
}

/* ===== Lookup ===== */

int text_tree_registry_get(const TextTreeRegistry *reg, uint32_t id,
                           const Text **text_out)
{
    if (!reg || !text_out)
        return ERR_INVALID;
    if (id >= sap_txn_vec_len(&reg->entries))
        return ERR_RANGE;

    const TextTreeEntry *entry =
        (const TextTreeEntry *)sap_txn_vec_at(&reg->entries, id);

    /* Entry was already released (refs reached 0) */
    if (atomic_load(&entry->refs) == 0u)
        return ERR_INVALID;

    *text_out = entry->text;
    return ERR_OK;
}

/* ===== Refcount Management ===== */

int text_tree_registry_retain(TextTreeRegistry *reg, uint32_t id)
{
    if (!reg)
        return ERR_INVALID;
    if (id >= sap_txn_vec_len(&reg->entries))
        return ERR_RANGE;

    TextTreeEntry *entry = (TextTreeEntry *)sap_txn_vec_at(&reg->entries, id);
    uint32_t old = atomic_fetch_add(&entry->refs, 1u);

    /* Catch retain on an already-freed entry */
    if (old == 0u)
    {
        atomic_fetch_sub(&entry->refs, 1u);
        return ERR_INVALID;
    }

    return ERR_OK;
}

int text_tree_registry_release(TextTreeRegistry *reg, uint32_t id)
{
    if (!reg)
        return ERR_INVALID;
    if (id >= sap_txn_vec_len(&reg->entries))
        return ERR_RANGE;

    TextTreeEntry *entry = (TextTreeEntry *)sap_txn_vec_at(&reg->entries, id);
    uint32_t old = atomic_fetch_sub(&entry->refs, 1u);

    if (old == 0u)
    {
        /* Underflow — restore and report error */
        atomic_fetch_add(&entry->refs, 1u);
        return ERR_INVALID;
    }

    if (old == 1u)
    {
        /* Last reference — free the Text */
        text_free(reg->env, entry->text);
        entry->text = NULL;
    }

    return ERR_OK;
}

/* ===== Query ===== */

uint32_t text_tree_registry_count(const TextTreeRegistry *reg)
{
    if (!reg) return 0;
    return sap_txn_vec_len(&reg->entries);
}

/* ===== Resolver Adapter ===== */

int text_tree_registry_resolve_fn(uint32_t tree_id, const Text **text_out,
                                  void *ctx)
{
    TextTreeRegistry *reg = (TextTreeRegistry *)ctx;
    return text_tree_registry_get(reg, tree_id, text_out);
}
