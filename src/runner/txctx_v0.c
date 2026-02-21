/*
 * txctx_v0.c - phase-B host transaction context scaffold
 *
 * SPDX-License-Identifier: MIT
 */
#include "runner/txctx_v0.h"

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

static int copy_blob(const void *src, uint32_t len, uint8_t **dst_out)
{
    uint8_t *dst;

    if (!dst_out)
    {
        return SAP_ERROR;
    }
    *dst_out = NULL;
    if (len == 0u)
    {
        return SAP_OK;
    }
    if (!src)
    {
        return SAP_ERROR;
    }

    dst = (uint8_t *)malloc((size_t)len);
    if (!dst)
    {
        return SAP_ERROR;
    }
    memcpy(dst, src, len);
    *dst_out = dst;
    return SAP_OK;
}

static int grow_array(void **arr, uint32_t *cap, size_t elem_size)
{
    uint32_t new_cap;
    size_t bytes;
    void *next;

    if (!arr || !cap || elem_size == 0u)
    {
        return SAP_ERROR;
    }

    if (*cap == 0u)
    {
        new_cap = 4u;
    }
    else
    {
        if (*cap > (UINT32_MAX / 2u))
        {
            return SAP_ERROR;
        }
        new_cap = (*cap) * 2u;
    }

    if (((size_t)new_cap) > (SIZE_MAX / elem_size))
    {
        return SAP_ERROR;
    }
    bytes = (size_t)new_cap * elem_size;
    next = realloc(*arr, bytes);
    if (!next)
    {
        return SAP_ERROR;
    }

    *arr = next;
    *cap = new_cap;
    return SAP_OK;
}

static int ensure_read_cap(SapRunnerTxCtxV0 *ctx)
{
    if (ctx->read_count < ctx->read_cap)
    {
        return SAP_OK;
    }
    return grow_array((void **)&ctx->reads, &ctx->read_cap, sizeof(ctx->reads[0]));
}

static int ensure_write_cap(SapRunnerTxCtxV0 *ctx)
{
    if (ctx->write_count < ctx->write_cap)
    {
        return SAP_OK;
    }
    return grow_array((void **)&ctx->writes, &ctx->write_cap, sizeof(ctx->writes[0]));
}

static int ensure_intent_cap(SapRunnerTxCtxV0 *ctx)
{
    if (ctx->intent_count < ctx->intent_cap)
    {
        return SAP_OK;
    }
    return grow_array((void **)&ctx->intents, &ctx->intent_cap, sizeof(ctx->intents[0]));
}

static int find_read_index(const SapRunnerTxCtxV0 *ctx, uint32_t dbi, const void *key,
                           uint32_t key_len, uint32_t *index_out)
{
    uint32_t i;

    if (!ctx || !key || !index_out)
    {
        return SAP_ERROR;
    }
    for (i = 0u; i < ctx->read_count; i++)
    {
        const SapRunnerTxReadV0 *entry = &ctx->reads[i];
        if (entry->dbi == dbi && key_eq(entry->key, entry->key_len, key, key_len))
        {
            *index_out = i;
            return SAP_OK;
        }
    }
    return SAP_NOTFOUND;
}

static int find_write_index(const SapRunnerTxCtxV0 *ctx, uint32_t dbi, const void *key,
                            uint32_t key_len, uint32_t *index_out)
{
    uint32_t i;

    if (!ctx || !key || !index_out)
    {
        return SAP_ERROR;
    }
    for (i = 0u; i < ctx->write_count; i++)
    {
        const SapRunnerTxWriteV0 *entry = &ctx->writes[i];
        if (entry->dbi == dbi && key_eq(entry->key, entry->key_len, key, key_len))
        {
            *index_out = i;
            return SAP_OK;
        }
    }
    return SAP_NOTFOUND;
}

static void free_read_entry(SapRunnerTxReadV0 *entry)
{
    if (!entry)
    {
        return;
    }
    free(entry->key);
    free(entry->val);
    memset(entry, 0, sizeof(*entry));
}

static void free_write_entry(SapRunnerTxWriteV0 *entry)
{
    if (!entry)
    {
        return;
    }
    free(entry->key);
    free(entry->val);
    memset(entry, 0, sizeof(*entry));
}

static void free_intent_entry(SapRunnerTxIntentV0 *entry)
{
    if (!entry)
    {
        return;
    }
    free(entry->frame);
    memset(entry, 0, sizeof(*entry));
}

int sap_runner_txctx_v0_init(SapRunnerTxCtxV0 *ctx)
{
    if (!ctx)
    {
        return SAP_ERROR;
    }
    memset(ctx, 0, sizeof(*ctx));
    return SAP_OK;
}

void sap_runner_txctx_v0_reset(SapRunnerTxCtxV0 *ctx)
{
    uint32_t i;

    if (!ctx)
    {
        return;
    }
    for (i = 0u; i < ctx->read_count; i++)
    {
        free_read_entry(&ctx->reads[i]);
    }
    for (i = 0u; i < ctx->write_count; i++)
    {
        free_write_entry(&ctx->writes[i]);
    }
    for (i = 0u; i < ctx->intent_count; i++)
    {
        free_intent_entry(&ctx->intents[i]);
    }
    ctx->read_count = 0u;
    ctx->write_count = 0u;
    ctx->intent_count = 0u;
}

void sap_runner_txctx_v0_dispose(SapRunnerTxCtxV0 *ctx)
{
    if (!ctx)
    {
        return;
    }
    sap_runner_txctx_v0_reset(ctx);
    free(ctx->reads);
    free(ctx->writes);
    free(ctx->intents);
    memset(ctx, 0, sizeof(*ctx));
}

int sap_runner_txctx_v0_read_dbi(SapRunnerTxCtxV0 *ctx, Txn *txn, uint32_t dbi, const void *key,
                                 uint32_t key_len, const void **val_out, uint32_t *val_len_out)
{
    uint32_t idx;
    int rc;

    if (val_out)
    {
        *val_out = NULL;
    }
    if (val_len_out)
    {
        *val_len_out = 0u;
    }
    if (!ctx || !txn || !key || key_len == 0u)
    {
        return SAP_ERROR;
    }

    rc = find_write_index(ctx, dbi, key, key_len, &idx);
    if (rc == SAP_OK)
    {
        const SapRunnerTxWriteV0 *w = &ctx->writes[idx];
        if (w->kind == SAP_RUNNER_TX_WRITE_KIND_DEL)
        {
            return SAP_NOTFOUND;
        }
        if (w->kind != SAP_RUNNER_TX_WRITE_KIND_PUT)
        {
            return SAP_ERROR;
        }
        if (val_out)
        {
            *val_out = w->val;
        }
        if (val_len_out)
        {
            *val_len_out = w->val_len;
        }
        return SAP_OK;
    }
    if (rc != SAP_NOTFOUND)
    {
        return rc;
    }

    rc = find_read_index(ctx, dbi, key, key_len, &idx);
    if (rc == SAP_OK)
    {
        const SapRunnerTxReadV0 *r = &ctx->reads[idx];
        if (!r->exists)
        {
            return SAP_NOTFOUND;
        }
        if (val_out)
        {
            *val_out = r->val;
        }
        if (val_len_out)
        {
            *val_len_out = r->val_len;
        }
        return SAP_OK;
    }
    if (rc != SAP_NOTFOUND)
    {
        return rc;
    }

    {
        const void *val = NULL;
        uint32_t val_len = 0u;
        int lookup_rc;
        SapRunnerTxReadV0 *entry;

        lookup_rc = txn_get_dbi(txn, dbi, key, key_len, &val, &val_len);
        if (lookup_rc != SAP_OK && lookup_rc != SAP_NOTFOUND)
        {
            return lookup_rc;
        }

        rc = ensure_read_cap(ctx);
        if (rc != SAP_OK)
        {
            return rc;
        }

        entry = &ctx->reads[ctx->read_count];
        memset(entry, 0, sizeof(*entry));
        entry->dbi = dbi;
        entry->exists = (lookup_rc == SAP_OK) ? 1 : 0;
        entry->key_len = key_len;

        rc = copy_blob(key, key_len, &entry->key);
        if (rc != SAP_OK)
        {
            return rc;
        }

        if (entry->exists)
        {
            entry->val_len = val_len;
            rc = copy_blob(val, val_len, &entry->val);
            if (rc != SAP_OK)
            {
                free(entry->key);
                entry->key = NULL;
                return rc;
            }
        }

        ctx->read_count++;
        if (!entry->exists)
        {
            return SAP_NOTFOUND;
        }
        if (val_out)
        {
            *val_out = entry->val;
        }
        if (val_len_out)
        {
            *val_len_out = entry->val_len;
        }
        return SAP_OK;
    }
}

int sap_runner_txctx_v0_stage_put_dbi(SapRunnerTxCtxV0 *ctx, uint32_t dbi, const void *key,
                                      uint32_t key_len, const void *val, uint32_t val_len)
{
    uint32_t idx;
    int rc;

    if (!ctx || !key || key_len == 0u)
    {
        return SAP_ERROR;
    }
    if (val_len > 0u && !val)
    {
        return SAP_ERROR;
    }

    rc = find_write_index(ctx, dbi, key, key_len, &idx);
    if (rc == SAP_OK)
    {
        SapRunnerTxWriteV0 *entry = &ctx->writes[idx];
        uint8_t *new_val = NULL;

        rc = copy_blob(val, val_len, &new_val);
        if (rc != SAP_OK)
        {
            return rc;
        }
        free(entry->val);
        entry->val = new_val;
        entry->val_len = val_len;
        entry->kind = SAP_RUNNER_TX_WRITE_KIND_PUT;
        return SAP_OK;
    }
    if (rc != SAP_NOTFOUND)
    {
        return rc;
    }

    rc = ensure_write_cap(ctx);
    if (rc != SAP_OK)
    {
        return rc;
    }

    {
        SapRunnerTxWriteV0 *entry = &ctx->writes[ctx->write_count];
        memset(entry, 0, sizeof(*entry));
        entry->dbi = dbi;
        entry->kind = SAP_RUNNER_TX_WRITE_KIND_PUT;
        entry->key_len = key_len;
        entry->val_len = val_len;

        rc = copy_blob(key, key_len, &entry->key);
        if (rc != SAP_OK)
        {
            return rc;
        }
        rc = copy_blob(val, val_len, &entry->val);
        if (rc != SAP_OK)
        {
            free(entry->key);
            entry->key = NULL;
            return rc;
        }
    }

    ctx->write_count++;
    return SAP_OK;
}

int sap_runner_txctx_v0_stage_del_dbi(SapRunnerTxCtxV0 *ctx, uint32_t dbi, const void *key,
                                      uint32_t key_len)
{
    uint32_t idx;
    int rc;

    if (!ctx || !key || key_len == 0u)
    {
        return SAP_ERROR;
    }

    rc = find_write_index(ctx, dbi, key, key_len, &idx);
    if (rc == SAP_OK)
    {
        SapRunnerTxWriteV0 *entry = &ctx->writes[idx];
        free(entry->val);
        entry->val = NULL;
        entry->val_len = 0u;
        entry->kind = SAP_RUNNER_TX_WRITE_KIND_DEL;
        return SAP_OK;
    }
    if (rc != SAP_NOTFOUND)
    {
        return rc;
    }

    rc = ensure_write_cap(ctx);
    if (rc != SAP_OK)
    {
        return rc;
    }

    {
        SapRunnerTxWriteV0 *entry = &ctx->writes[ctx->write_count];
        memset(entry, 0, sizeof(*entry));
        entry->dbi = dbi;
        entry->kind = SAP_RUNNER_TX_WRITE_KIND_DEL;
        entry->key_len = key_len;

        rc = copy_blob(key, key_len, &entry->key);
        if (rc != SAP_OK)
        {
            return rc;
        }
    }

    ctx->write_count++;
    return SAP_OK;
}

int sap_runner_txctx_v0_push_intent(SapRunnerTxCtxV0 *ctx, const SapRunnerIntentV0 *intent)
{
    uint8_t *frame;
    uint32_t frame_len;
    int rc;

    if (!ctx || !intent)
    {
        return SAP_ERROR;
    }

    frame_len = sap_runner_intent_v0_size(intent);
    if (frame_len == 0u)
    {
        return SAP_ERROR;
    }

    frame = (uint8_t *)malloc((size_t)frame_len);
    if (!frame)
    {
        return SAP_ERROR;
    }

    rc = sap_runner_intent_v0_encode(intent, frame, frame_len, &frame_len);
    if (rc != SAP_RUNNER_WIRE_OK)
    {
        free(frame);
        return SAP_ERROR;
    }

    rc = ensure_intent_cap(ctx);
    if (rc != SAP_OK)
    {
        free(frame);
        return rc;
    }

    ctx->intents[ctx->intent_count].frame = frame;
    ctx->intents[ctx->intent_count].frame_len = frame_len;
    ctx->intent_count++;
    return SAP_OK;
}

int sap_runner_txctx_v0_validate_reads(const SapRunnerTxCtxV0 *ctx, Txn *txn)
{
    uint32_t i;

    if (!ctx || !txn)
    {
        return SAP_ERROR;
    }

    for (i = 0u; i < ctx->read_count; i++)
    {
        const SapRunnerTxReadV0 *entry = &ctx->reads[i];
        const void *cur_val = NULL;
        uint32_t cur_len = 0u;
        int rc;

        rc = txn_get_dbi(txn, entry->dbi, entry->key, entry->key_len, &cur_val, &cur_len);
        if (entry->exists)
        {
            if (rc == SAP_NOTFOUND)
            {
                return SAP_CONFLICT;
            }
            if (rc != SAP_OK)
            {
                return rc;
            }
            if (cur_len != entry->val_len)
            {
                return SAP_CONFLICT;
            }
            if (cur_len > 0u && memcmp(cur_val, entry->val, cur_len) != 0)
            {
                return SAP_CONFLICT;
            }
        }
        else
        {
            if (rc == SAP_NOTFOUND)
            {
                continue;
            }
            if (rc == SAP_OK)
            {
                return SAP_CONFLICT;
            }
            return rc;
        }
    }

    return SAP_OK;
}

int sap_runner_txctx_v0_apply_writes(const SapRunnerTxCtxV0 *ctx, Txn *txn)
{
    uint32_t i;

    if (!ctx || !txn)
    {
        return SAP_ERROR;
    }

    for (i = 0u; i < ctx->write_count; i++)
    {
        const SapRunnerTxWriteV0 *entry = &ctx->writes[i];
        int rc;

        if (entry->kind == SAP_RUNNER_TX_WRITE_KIND_PUT)
        {
            rc = txn_put_dbi(txn, entry->dbi, entry->key, entry->key_len, entry->val,
                             entry->val_len);
        }
        else if (entry->kind == SAP_RUNNER_TX_WRITE_KIND_DEL)
        {
            rc = txn_del_dbi(txn, entry->dbi, entry->key, entry->key_len);
        }
        else
        {
            return SAP_ERROR;
        }

        if (rc != SAP_OK)
        {
            return rc;
        }
    }
    return SAP_OK;
}

uint32_t sap_runner_txctx_v0_read_count(const SapRunnerTxCtxV0 *ctx)
{
    return ctx ? ctx->read_count : 0u;
}

uint32_t sap_runner_txctx_v0_write_count(const SapRunnerTxCtxV0 *ctx)
{
    return ctx ? ctx->write_count : 0u;
}

uint32_t sap_runner_txctx_v0_intent_count(const SapRunnerTxCtxV0 *ctx)
{
    return ctx ? ctx->intent_count : 0u;
}

const uint8_t *sap_runner_txctx_v0_intent_frame(const SapRunnerTxCtxV0 *ctx, uint32_t index,
                                                uint32_t *frame_len_out)
{
    if (frame_len_out)
    {
        *frame_len_out = 0u;
    }
    if (!ctx || index >= ctx->intent_count)
    {
        return NULL;
    }
    if (frame_len_out)
    {
        *frame_len_out = ctx->intents[index].frame_len;
    }
    return ctx->intents[index].frame;
}
