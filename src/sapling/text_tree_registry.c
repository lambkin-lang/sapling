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

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

/* ===== Internal Types ===== */

typedef struct {
    Text *text;                /* retained via text_clone; freed on last release */
    _Atomic uint32_t refs;     /* TREE handle refcount; atomic for cross-worker release */
} TextTreeEntry;

struct TextTreeRegistry {
    SapEnv *env;
    TextTreeEntry *entries;    /* malloc'd growable array */
    uint32_t count;
    uint32_t capacity;
};

/* ===== Constants ===== */

#define TREE_INITIAL_CAP 16u
#define TREE_MAX_ID      0x3FFFFFFFu /* 30-bit payload limit */

/* ===== Lifecycle ===== */

TextTreeRegistry *text_tree_registry_new(SapEnv *env)
{
    if (!env) return NULL;

    TextTreeRegistry *reg = (TextTreeRegistry *)malloc(sizeof(TextTreeRegistry));
    if (!reg) return NULL;

    reg->entries = (TextTreeEntry *)malloc(TREE_INITIAL_CAP * sizeof(TextTreeEntry));
    if (!reg->entries)
    {
        free(reg);
        return NULL;
    }

    reg->env = env;
    reg->count = 0;
    reg->capacity = TREE_INITIAL_CAP;
    return reg;
}

void text_tree_registry_free(TextTreeRegistry *reg)
{
    if (!reg) return;

    for (uint32_t i = 0; i < reg->count; i++)
    {
        if (reg->entries[i].text)
        {
            text_free(reg->env, reg->entries[i].text);
            reg->entries[i].text = NULL;
        }
    }

    free(reg->entries);
    free(reg);
}

/* ===== Registration ===== */

int text_tree_registry_register(TextTreeRegistry *reg, const Text *text,
                                uint32_t *id_out)
{
    if (!reg || !text || !id_out)
        return ERR_INVALID;

    if (reg->count > TREE_MAX_ID)
        return ERR_INVALID;

    /* Grow the entries array if needed */
    if (reg->count >= reg->capacity)
    {
        uint32_t new_cap = reg->capacity * 2u;
        if (new_cap < reg->capacity) /* overflow */
            return ERR_OOM;
        TextTreeEntry *new_entries =
            (TextTreeEntry *)realloc(reg->entries, new_cap * sizeof(TextTreeEntry));
        if (!new_entries)
            return ERR_OOM;
        reg->entries = new_entries;
        reg->capacity = new_cap;
    }

    /* COW clone — text_clone bumps the shared refcount */
    Text *clone = text_clone(reg->env, text);
    if (!clone)
        return ERR_OOM;

    uint32_t id = reg->count;
    reg->entries[id].text = clone;
    atomic_init(&reg->entries[id].refs, 1u);
    reg->count++;

    *id_out = id;
    return ERR_OK;
}

/* ===== Lookup ===== */

int text_tree_registry_get(const TextTreeRegistry *reg, uint32_t id,
                           const Text **text_out)
{
    if (!reg || !text_out)
        return ERR_INVALID;
    if (id >= reg->count)
        return ERR_RANGE;

    const TextTreeEntry *entry = &reg->entries[id];

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
    if (id >= reg->count)
        return ERR_RANGE;

    TextTreeEntry *entry = &reg->entries[id];
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
    if (id >= reg->count)
        return ERR_RANGE;

    TextTreeEntry *entry = &reg->entries[id];
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
    return reg->count;
}

/* ===== Resolver Adapter ===== */

int text_tree_registry_resolve_fn(uint32_t tree_id, const Text **text_out,
                                  void *ctx)
{
    TextTreeRegistry *reg = (TextTreeRegistry *)ctx;
    return text_tree_registry_get(reg, tree_id, text_out);
}
