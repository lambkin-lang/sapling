/*
 * txn_vec.c — Arena-backed growable array implementation
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/txn_vec.h"

#include <string.h>

int sap_txn_vec_init(SapTxnVec *vec, SapMemArena *arena,
                     uint32_t elem_size, uint32_t initial_cap)
{
    if (!vec || !arena || elem_size == 0)
        return ERR_INVALID;

    vec->arena = arena;
    vec->data = NULL;
    vec->nodeno = 0;
    vec->elem_size = elem_size;
    vec->len = 0;
    vec->cap = 0;

    if (initial_cap == 0)
        return ERR_OK;

    return sap_txn_vec_reserve(vec, initial_cap);
}

void sap_txn_vec_destroy(SapTxnVec *vec)
{
    if (!vec)
        return;
    if (vec->data && vec->arena)
        sap_arena_free_node(vec->arena, vec->nodeno, vec->cap * vec->elem_size);
    vec->data = NULL;
    vec->nodeno = 0;
    vec->len = 0;
    vec->cap = 0;
}

int sap_txn_vec_reserve(SapTxnVec *vec, uint32_t needed)
{
    if (!vec || !vec->arena)
        return ERR_INVALID;
    if (needed <= vec->cap)
        return ERR_OK;

    uint32_t new_cap = vec->cap;
    if (new_cap == 0)
        new_cap = needed;
    while (new_cap < needed)
    {
        if (new_cap > UINT32_MAX / 2)
            return ERR_OOM;
        new_cap *= 2;
    }

    /* Overflow check: new_cap * elem_size */
    if (new_cap > UINT32_MAX / vec->elem_size)
        return ERR_OOM;

    uint32_t new_size = new_cap * vec->elem_size;

    void *new_data = NULL;
    uint32_t new_nodeno = 0;
    int rc = sap_arena_alloc_node(vec->arena, new_size, &new_data, &new_nodeno);
    if (rc != ERR_OK)
        return rc;

    if (vec->data && vec->len > 0)
        memcpy(new_data, vec->data, (size_t)vec->len * vec->elem_size);

    /* Free old backing node */
    if (vec->data)
        sap_arena_free_node(vec->arena, vec->nodeno, vec->cap * vec->elem_size);

    vec->data = new_data;
    vec->nodeno = new_nodeno;
    vec->cap = new_cap;
    return ERR_OK;
}

int sap_txn_vec_push(SapTxnVec *vec, const void *elem)
{
    if (!vec || !elem)
        return ERR_INVALID;

    if (vec->len == vec->cap)
    {
        int rc = sap_txn_vec_reserve(vec, vec->len + 1);
        if (rc != ERR_OK)
            return rc;
    }

    memcpy((uint8_t *)vec->data + (size_t)vec->len * vec->elem_size,
           elem, vec->elem_size);
    vec->len++;
    return ERR_OK;
}

void *sap_txn_vec_at(const SapTxnVec *vec, uint32_t idx)
{
    if (!vec || idx >= vec->len)
        return NULL;
    return (uint8_t *)vec->data + (size_t)idx * vec->elem_size;
}

int sap_txn_vec_pop(SapTxnVec *vec)
{
    if (!vec)
        return ERR_INVALID;
    if (vec->len == 0)
        return ERR_EMPTY;
    vec->len--;
    return ERR_OK;
}

int sap_txn_vec_swap_remove(SapTxnVec *vec, uint32_t idx)
{
    if (!vec)
        return ERR_INVALID;
    if (idx >= vec->len)
        return ERR_RANGE;

    vec->len--;
    if (idx < vec->len)
    {
        void *dst = (uint8_t *)vec->data + (size_t)idx * vec->elem_size;
        void *src = (uint8_t *)vec->data + (size_t)vec->len * vec->elem_size;
        memcpy(dst, src, vec->elem_size);
    }
    return ERR_OK;
}
