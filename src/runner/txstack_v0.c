/*
 * txstack_v0.c - phase-B nested atomic context stack scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/txstack_v0.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

static int key_eq(const uint8_t *a, uint32_t a_len, const void *b, uint32_t b_len)
{
    if (a_len != b_len)
    {
        return 0;
    }
    if (a_len == 0u)
    {
        return 1;
    }
    return memcmp(a, b, a_len) == 0;
}

static int find_write(const SapRunnerTxCtxV0 *ctx, uint32_t dbi, const void *key, uint32_t key_len,
                      uint32_t *index_out)
{
    uint32_t i;

    if (!ctx || !key || !index_out)
    {
        return ERR_INVALID;
    }
    for (i = 0u; i < ctx->write_count; i++)
    {
        const SapRunnerTxWriteV0 *w = &ctx->writes[i];
        if (w->dbi == dbi && key_eq(w->key, w->key_len, key, key_len))
        {
            *index_out = i;
            return ERR_OK;
        }
    }
    return ERR_NOT_FOUND;
}

static int find_read(const SapRunnerTxCtxV0 *ctx, uint32_t dbi, const void *key, uint32_t key_len,
                     uint32_t *index_out)
{
    uint32_t i;

    if (!ctx || !key || !index_out)
    {
        return ERR_INVALID;
    }
    for (i = 0u; i < ctx->read_count; i++)
    {
        const SapRunnerTxReadV0 *r = &ctx->reads[i];
        if (r->dbi == dbi && key_eq(r->key, r->key_len, key, key_len))
        {
            *index_out = i;
            return ERR_OK;
        }
    }
    return ERR_NOT_FOUND;
}

static int ensure_cap(SapRunnerTxStackV0 *stack)
{
    uint32_t new_cap;
    size_t bytes;
    void *next;

    if (!stack)
    {
        return ERR_INVALID;
    }
    if (stack->depth < stack->cap)
    {
        return ERR_OK;
    }

    if (stack->cap == 0u)
    {
        new_cap = 4u;
    }
    else
    {
        if (stack->cap > (UINT32_MAX / 2u))
        {
            return ERR_INVALID;
        }
        new_cap = stack->cap * 2u;
    }

    if (new_cap > (UINT32_MAX / (uint32_t)sizeof(SapRunnerTxCtxV0)))
    {
        return ERR_INVALID;
    }
    bytes = (size_t)new_cap * sizeof(SapRunnerTxCtxV0);
    next = realloc(stack->frames, bytes);
    if (!next)
    {
        return ERR_OOM;
    }
    stack->frames = (SapRunnerTxCtxV0 *)next;
    stack->cap = new_cap;
    return ERR_OK;
}

int sap_runner_txstack_v0_init(SapRunnerTxStackV0 *stack)
{
    if (!stack)
    {
        return ERR_INVALID;
    }
    memset(stack, 0, sizeof(*stack));
    return ERR_OK;
}

void sap_runner_txstack_v0_reset(SapRunnerTxStackV0 *stack)
{
    uint32_t i;

    if (!stack)
    {
        return;
    }
    for (i = 0u; i < stack->depth; i++)
    {
        sap_runner_txctx_v0_dispose(&stack->frames[i]);
    }
    stack->depth = 0u;
}

void sap_runner_txstack_v0_dispose(SapRunnerTxStackV0 *stack)
{
    if (!stack)
    {
        return;
    }
    sap_runner_txstack_v0_reset(stack);
    free(stack->frames);
    memset(stack, 0, sizeof(*stack));
}

uint32_t sap_runner_txstack_v0_depth(const SapRunnerTxStackV0 *stack)
{
    return stack ? stack->depth : 0u;
}

SapRunnerTxCtxV0 *sap_runner_txstack_v0_current(SapRunnerTxStackV0 *stack)
{
    if (!stack || stack->depth == 0u)
    {
        return NULL;
    }
    return &stack->frames[stack->depth - 1u];
}

const SapRunnerTxCtxV0 *sap_runner_txstack_v0_root(const SapRunnerTxStackV0 *stack)
{
    if (!stack || stack->depth == 0u)
    {
        return NULL;
    }
    return &stack->frames[0];
}

int sap_runner_txstack_v0_push(SapRunnerTxStackV0 *stack)
{
    int rc;
    SapRunnerTxCtxV0 *frame;

    if (!stack)
    {
        return ERR_INVALID;
    }

    rc = ensure_cap(stack);
    if (rc != ERR_OK)
    {
        return rc;
    }

    frame = &stack->frames[stack->depth];
    rc = sap_runner_txctx_v0_init(frame);
    if (rc != ERR_OK)
    {
        return rc;
    }
    stack->depth++;
    return ERR_OK;
}

int sap_runner_txstack_v0_commit_top(SapRunnerTxStackV0 *stack)
{
    SapRunnerTxCtxV0 *parent;
    SapRunnerTxCtxV0 *child;
    int rc;

    if (!stack || stack->depth < 2u)
    {
        return ERR_INVALID;
    }

    parent = &stack->frames[stack->depth - 2u];
    child = &stack->frames[stack->depth - 1u];
    rc = sap_runner_txctx_v0_merge_child(parent, child);
    if (rc != ERR_OK)
    {
        return rc;
    }

    sap_runner_txctx_v0_dispose(child);
    stack->depth--;
    return ERR_OK;
}

int sap_runner_txstack_v0_abort_top(SapRunnerTxStackV0 *stack)
{
    SapRunnerTxCtxV0 *top;

    if (!stack || stack->depth == 0u)
    {
        return ERR_INVALID;
    }

    top = &stack->frames[stack->depth - 1u];
    sap_runner_txctx_v0_dispose(top);
    stack->depth--;
    return ERR_OK;
}

int sap_runner_txstack_v0_read_dbi(SapRunnerTxStackV0 *stack, Txn *txn, uint32_t dbi,
                                   const void *key, uint32_t key_len, const void **val_out,
                                   uint32_t *val_len_out)
{
    uint32_t d;

    if (val_out)
    {
        *val_out = NULL;
    }
    if (val_len_out)
    {
        *val_len_out = 0u;
    }
    if (!stack || !txn || !key || key_len == 0u || stack->depth == 0u)
    {
        return ERR_INVALID;
    }

    for (d = stack->depth; d > 0u; d--)
    {
        const SapRunnerTxCtxV0 *frame = &stack->frames[d - 1u];
        uint32_t idx;
        int rc = find_write(frame, dbi, key, key_len, &idx);

        if (rc == ERR_OK)
        {
            const SapRunnerTxWriteV0 *w = &frame->writes[idx];
            if (w->kind == SAP_RUNNER_TX_WRITE_KIND_DEL)
            {
                return ERR_NOT_FOUND;
            }
            if (w->kind != SAP_RUNNER_TX_WRITE_KIND_PUT)
            {
                return ERR_CORRUPT;
            }
            if (val_out)
            {
                *val_out = w->val;
            }
            if (val_len_out)
            {
                *val_len_out = w->val_len;
            }
            return ERR_OK;
        }
        if (rc != ERR_NOT_FOUND)
        {
            return rc;
        }
    }

    for (d = stack->depth; d > 0u; d--)
    {
        const SapRunnerTxCtxV0 *frame = &stack->frames[d - 1u];
        uint32_t idx;
        int rc = find_read(frame, dbi, key, key_len, &idx);

        if (rc == ERR_OK)
        {
            const SapRunnerTxReadV0 *r = &frame->reads[idx];
            if (!r->exists)
            {
                return ERR_NOT_FOUND;
            }
            if (val_out)
            {
                *val_out = r->val;
            }
            if (val_len_out)
            {
                *val_len_out = r->val_len;
            }
            return ERR_OK;
        }
        if (rc != ERR_NOT_FOUND)
        {
            return rc;
        }
    }

    return sap_runner_txctx_v0_read_dbi(&stack->frames[stack->depth - 1u], txn, dbi, key, key_len,
                                        val_out, val_len_out);
}

int sap_runner_txstack_v0_stage_put_dbi(SapRunnerTxStackV0 *stack, uint32_t dbi, const void *key,
                                        uint32_t key_len, const void *val, uint32_t val_len)
{
    SapRunnerTxCtxV0 *cur = sap_runner_txstack_v0_current(stack);
    if (!cur)
    {
        return ERR_INVALID;
    }
    return sap_runner_txctx_v0_stage_put_dbi(cur, dbi, key, key_len, val, val_len);
}

int sap_runner_txstack_v0_stage_del_dbi(SapRunnerTxStackV0 *stack, uint32_t dbi, const void *key,
                                        uint32_t key_len)
{
    SapRunnerTxCtxV0 *cur = sap_runner_txstack_v0_current(stack);
    if (!cur)
    {
        return ERR_INVALID;
    }
    return sap_runner_txctx_v0_stage_del_dbi(cur, dbi, key, key_len);
}

int sap_runner_txstack_v0_push_intent(SapRunnerTxStackV0 *stack, const SapRunnerIntentV0 *intent)
{
    SapRunnerTxCtxV0 *cur = sap_runner_txstack_v0_current(stack);
    if (!cur)
    {
        return ERR_INVALID;
    }
    return sap_runner_txctx_v0_push_intent(cur, intent);
}

int sap_runner_txstack_v0_validate_root_reads(const SapRunnerTxStackV0 *stack, Txn *txn)
{
    if (!stack || stack->depth != 1u || !txn)
    {
        return ERR_BUSY;
    }
    return sap_runner_txctx_v0_validate_reads(&stack->frames[0], txn);
}

int sap_runner_txstack_v0_apply_root_writes(const SapRunnerTxStackV0 *stack, Txn *txn)
{
    if (!stack || stack->depth != 1u || !txn)
    {
        return ERR_BUSY;
    }
    return sap_runner_txctx_v0_apply_writes(&stack->frames[0], txn);
}
