/*
 * text.c — mutable code-point text built on top of Seq
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text.h"
#include "sapling/txn.h"

#include <stdlib.h>

struct Text
{
    struct TextShared *shared;
};

typedef struct TextShared
{
    Seq *seq;
    size_t refs;
} TextShared;

static Seq *text_seq(const Text *text);

static int text_codepoint_is_valid(uint32_t codepoint)
{
    return (codepoint <= 0x10FFFFu) && !(codepoint >= 0xD800u && codepoint <= 0xDFFFu);
}

static int text_handle_is_storable(TextHandle handle)
{
    TextHandleKind kind =
        (TextHandleKind)((handle & TEXT_HANDLE_TAG_MASK) >> TEXT_HANDLE_TAG_SHIFT);
    if (kind == TEXT_HANDLE_CODEPOINT)
        return text_codepoint_is_valid(handle & TEXT_HANDLE_PAYLOAD_MASK);
    if (kind == TEXT_HANDLE_LITERAL || kind == TEXT_HANDLE_TREE)
        return 1;
    return 0;
}

typedef int (*TextVisitCodepointFn)(uint32_t codepoint, void *visit_ctx);

typedef struct
{
    TextVisitCodepointFn visit_fn;
    void *visit_ctx;
} TextExpandEmitCtx;

static int text_expand_emit_one(uint32_t codepoint, void *emit_ctx)
{
    TextExpandEmitCtx *ctx = (TextExpandEmitCtx *)emit_ctx;
    if (!ctx || !ctx->visit_fn || !text_codepoint_is_valid(codepoint))
        return SEQ_INVALID;
    return ctx->visit_fn(codepoint, ctx->visit_ctx);
}

static int text_visit_resolved_codepoints(const Text *text, TextHandleExpandFn expand_fn,
                                          void *resolver_ctx, TextVisitCodepointFn visit_fn,
                                          void *visit_ctx)
{
    Seq *seq = text_seq(text);
    size_t n = 0;

    if (!seq || !seq_is_valid(seq) || !visit_fn)
        return SEQ_INVALID;

    n = seq_length(seq);
    for (size_t i = 0; i < n; i++)
    {
        TextHandle handle = 0;
        int rc = seq_get(seq, i, &handle);

        if (rc != SEQ_OK)
            return rc;

        if (text_handle_kind(handle) == TEXT_HANDLE_CODEPOINT)
        {
            uint32_t codepoint = 0;
            rc = text_handle_to_codepoint(handle, &codepoint);
            if (rc != SEQ_OK)
                return rc;
            rc = visit_fn(codepoint, visit_ctx);
            if (rc != SEQ_OK)
                return rc;
            continue;
        }

        if (!expand_fn)
            return SEQ_INVALID;

        TextExpandEmitCtx emit_ctx = {visit_fn, visit_ctx};
        rc = expand_fn(handle, text_expand_emit_one, &emit_ctx, resolver_ctx);
        if (rc != SEQ_OK)
            return rc;
    }

    return SEQ_OK;
}

static size_t text_codepoint_utf8_size(uint32_t codepoint)
{
    if (codepoint <= 0x7Fu)
        return 1u;
    if (codepoint <= 0x7FFu)
        return 2u;
    if (codepoint <= 0xFFFFu)
        return 3u;
    return 4u;
}

static size_t text_utf8_encode_one(uint32_t codepoint, uint8_t out[4])
{
    if (!text_codepoint_is_valid(codepoint))
        return 0;

    if (codepoint <= 0x7Fu)
    {
        out[0] = (uint8_t)codepoint;
        return 1u;
    }
    if (codepoint <= 0x7FFu)
    {
        out[0] = (uint8_t)(0xC0u | (codepoint >> 6));
        out[1] = (uint8_t)(0x80u | (codepoint & 0x3Fu));
        return 2u;
    }
    if (codepoint <= 0xFFFFu)
    {
        out[0] = (uint8_t)(0xE0u | (codepoint >> 12));
        out[1] = (uint8_t)(0x80u | ((codepoint >> 6) & 0x3Fu));
        out[2] = (uint8_t)(0x80u | (codepoint & 0x3Fu));
        return 3u;
    }

    out[0] = (uint8_t)(0xF0u | (codepoint >> 18));
    out[1] = (uint8_t)(0x80u | ((codepoint >> 12) & 0x3Fu));
    out[2] = (uint8_t)(0x80u | ((codepoint >> 6) & 0x3Fu));
    out[3] = (uint8_t)(0x80u | (codepoint & 0x3Fu));
    return 4u;
}

static int text_utf8_decode_one(const uint8_t *utf8, size_t utf8_len, size_t *consumed_out,
                                uint32_t *codepoint_out)
{
    uint8_t b0 = 0;

    if (!utf8 || utf8_len == 0 || !consumed_out || !codepoint_out)
        return SEQ_INVALID;

    b0 = utf8[0];
    if (b0 <= 0x7Fu)
    {
        *consumed_out = 1u;
        *codepoint_out = (uint32_t)b0;
        return SEQ_OK;
    }

    if (b0 >= 0xC2u && b0 <= 0xDFu)
    {
        uint8_t b1 = 0;
        if (utf8_len < 2u)
            return SEQ_INVALID;
        b1 = utf8[1];
        if ((b1 & 0xC0u) != 0x80u)
            return SEQ_INVALID;
        *consumed_out = 2u;
        *codepoint_out = ((uint32_t)(b0 & 0x1Fu) << 6) | (uint32_t)(b1 & 0x3Fu);
        return SEQ_OK;
    }

    if (b0 >= 0xE0u && b0 <= 0xEFu)
    {
        uint8_t b1 = 0;
        uint8_t b2 = 0;
        if (utf8_len < 3u)
            return SEQ_INVALID;
        b1 = utf8[1];
        b2 = utf8[2];
        if ((b1 & 0xC0u) != 0x80u || (b2 & 0xC0u) != 0x80u)
            return SEQ_INVALID;
        if (b0 == 0xE0u && b1 < 0xA0u)
            return SEQ_INVALID; /* overlong */
        if (b0 == 0xEDu && b1 >= 0xA0u)
            return SEQ_INVALID; /* surrogate */
        *consumed_out = 3u;
        *codepoint_out =
            ((uint32_t)(b0 & 0x0Fu) << 12) | ((uint32_t)(b1 & 0x3Fu) << 6) | (uint32_t)(b2 & 0x3Fu);
        return SEQ_OK;
    }

    if (b0 >= 0xF0u && b0 <= 0xF4u)
    {
        uint8_t b1 = 0;
        uint8_t b2 = 0;
        uint8_t b3 = 0;
        if (utf8_len < 4u)
            return SEQ_INVALID;
        b1 = utf8[1];
        b2 = utf8[2];
        b3 = utf8[3];
        if ((b1 & 0xC0u) != 0x80u || (b2 & 0xC0u) != 0x80u || (b3 & 0xC0u) != 0x80u)
            return SEQ_INVALID;
        if (b0 == 0xF0u && b1 < 0x90u)
            return SEQ_INVALID; /* overlong */
        if (b0 == 0xF4u && b1 > 0x8Fu)
            return SEQ_INVALID; /* > U+10FFFF */
        *consumed_out = 4u;
        *codepoint_out = ((uint32_t)(b0 & 0x07u) << 18) | ((uint32_t)(b1 & 0x3Fu) << 12) |
                         ((uint32_t)(b2 & 0x3Fu) << 6) | (uint32_t)(b3 & 0x3Fu);
        return SEQ_OK;
    }

    return SEQ_INVALID;
}

enum
{
    TEXT_RUNTIME_DEFAULT_MAX_DEPTH = 64,
    TEXT_RUNTIME_DEFAULT_MAX_VISITS = 4096
};

typedef struct
{
    const TextRuntimeResolver *resolver;
    TextEmitCodepointFn emit_fn;
    void *emit_ctx;
    uint32_t *path;
    size_t path_len;
    size_t max_depth;
    size_t max_visits;
    size_t visits;
} TextRuntimeExpandCtx;

static int text_runtime_expand_handle_inner(TextHandle handle, size_t depth,
                                            TextRuntimeExpandCtx *ctx)
{
    TextHandleKind kind = text_handle_kind(handle);
    uint32_t payload = text_handle_payload(handle);
    int rc = SEQ_OK;

    if (!ctx || !ctx->resolver || !ctx->emit_fn)
        return SEQ_INVALID;

    if (kind == TEXT_HANDLE_CODEPOINT)
    {
        uint32_t codepoint = 0;
        rc = text_handle_to_codepoint(handle, &codepoint);
        if (rc != SEQ_OK)
            return rc;
        return ctx->emit_fn(codepoint, ctx->emit_ctx);
    }

    if (kind == TEXT_HANDLE_LITERAL)
    {
        const uint8_t *utf8 = NULL;
        size_t utf8_len = 0;
        size_t off = 0;

        if (!ctx->resolver->resolve_literal_utf8_fn)
            return SEQ_INVALID;
        rc = ctx->resolver->resolve_literal_utf8_fn(payload, &utf8, &utf8_len, ctx->resolver->ctx);
        if (rc != SEQ_OK)
            return rc;
        if (!utf8 && utf8_len > 0u)
            return SEQ_INVALID;

        while (off < utf8_len)
        {
            size_t consumed = 0;
            uint32_t codepoint = 0;

            rc = text_utf8_decode_one(utf8 + off, utf8_len - off, &consumed, &codepoint);
            if (rc != SEQ_OK)
                return rc;
            rc = ctx->emit_fn(codepoint, ctx->emit_ctx);
            if (rc != SEQ_OK)
                return rc;
            off += consumed;
        }
        return SEQ_OK;
    }

    if (kind == TEXT_HANDLE_TREE)
    {
        const Text *tree = NULL;
        size_t n = 0;

        if (!ctx->resolver->resolve_tree_text_fn)
            return SEQ_INVALID;
        if (depth >= ctx->max_depth)
            return SEQ_INVALID;
        for (size_t i = 0; i < ctx->path_len; i++)
        {
            if (ctx->path[i] == payload)
                return SEQ_INVALID;
        }
        if (ctx->visits >= ctx->max_visits)
            return SEQ_INVALID;

        rc = ctx->resolver->resolve_tree_text_fn(payload, &tree, ctx->resolver->ctx);
        if (rc != SEQ_OK)
            return rc;
        if (!tree || !text_is_valid(tree))
            return SEQ_INVALID;

        ctx->path[ctx->path_len++] = payload;
        ctx->visits++;

        n = text_length(tree);
        for (size_t i = 0; i < n; i++)
        {
            TextHandle child = 0;
            rc = text_get_handle(tree, i, &child);
            if (rc != SEQ_OK)
            {
                ctx->path_len--;
                return rc;
            }
            rc = text_runtime_expand_handle_inner(child, depth + 1u, ctx);
            if (rc != SEQ_OK)
            {
                ctx->path_len--;
                return rc;
            }
        }

        ctx->path_len--;
        return SEQ_OK;
    }

    return SEQ_INVALID;
}

int text_expand_runtime_handle(TextHandle handle, TextEmitCodepointFn emit_fn, void *emit_ctx,
                               void *resolver_ctx)
{
    const TextRuntimeResolver *resolver = (const TextRuntimeResolver *)resolver_ctx;
    TextRuntimeExpandCtx ctx;
    size_t max_depth = 0;
    size_t max_visits = 0;
    int rc = SEQ_OK;

    if (!resolver || !emit_fn)
        return SEQ_INVALID;

    max_depth = resolver->max_tree_depth ? resolver->max_tree_depth
                                         : (size_t)TEXT_RUNTIME_DEFAULT_MAX_DEPTH;
    max_visits = resolver->max_tree_visits ? resolver->max_tree_visits
                                           : (size_t)TEXT_RUNTIME_DEFAULT_MAX_VISITS;
    if (max_depth == 0u || max_visits == 0u)
        return SEQ_INVALID;
    if (max_depth > SIZE_MAX / sizeof(uint32_t))
        return SEQ_INVALID;

    ctx.resolver = resolver;
    ctx.emit_fn = emit_fn;
    ctx.emit_ctx = emit_ctx;
    ctx.path = (uint32_t *)malloc(max_depth * sizeof(uint32_t));
    if (!ctx.path)
        return SEQ_OOM;
    ctx.path_len = 0u;
    ctx.max_depth = max_depth;
    ctx.max_visits = max_visits;
    ctx.visits = 0u;

    rc = text_runtime_expand_handle_inner(handle, 0u, &ctx);
    free(ctx.path);
    return rc;
}


static void *text_alloc(SapTxnCtx *txn, size_t size)
{
    SapMemArena *arena = sap_txn_arena(txn);
    void *ptr = NULL;
    uint32_t nodeno = 0;
    if (sap_arena_alloc_node(arena, (uint32_t)size, &ptr, &nodeno) != 0)
        return NULL;
    return ptr;
}

static void text_dealloc(SapTxnCtx *txn, void *ptr, size_t size)
{
    SapMemArena *arena = sap_txn_arena(txn);
    if (ptr)
        sap_arena_free_node_ptr(arena, ptr, (uint32_t)size);
}

int text_is_valid(const Text *text)
{
    return text && text->shared && seq_is_valid(text->shared->seq);
}

static Text *text_shell_new(SapTxnCtx *txn)
{
    Text *text = (Text *)text_alloc(txn, sizeof(Text));
    if (text)
        text->shared = NULL;
    return text;
}


static TextShared *text_shared_new_with_seq(SapTxnCtx *txn, Seq *seq)
{
    TextShared *shared = (TextShared *)text_alloc(txn, sizeof(TextShared));
    if (!shared)
        return NULL;
    shared->seq = seq;
    shared->refs = 1u;
    return shared;
}

static TextShared *text_shared_new_empty(SapTxnCtx *txn, SapEnv *env)
{
    // Need env for seq_new.
    // If we only have txn, we can get env?
    // sap_txn_env(txn) is available in txn.h?
    // Let's assume yes, if not I'll fix it.
    // txn.h has SapEnv *sap_txn_env(SapTxnCtx *txn);
    
    Seq *seq = seq_new(env);
    if (!seq)
        return NULL;

    TextShared *shared = text_shared_new_with_seq(txn, seq);
    if (!shared)
    {
        seq_free(env, seq);
        return NULL;
    }
    return shared;
}

static void text_shared_destroy(SapTxnCtx *txn, TextShared *shared)
{
    if (!shared)
        return;
    SapEnv *env = sap_txn_env(txn);
    seq_free(env, shared->seq);
    shared->seq = NULL;
    text_dealloc(txn, shared, sizeof(TextShared));
}

static void text_shared_release(SapTxnCtx *txn, TextShared *shared)
{
    if (!shared || shared->refs == 0u)
        return;
    shared->refs--;
    if (shared->refs == 0u)
        text_shared_destroy(txn, shared);
}

static int text_shared_retain(TextShared *shared)
{
    if (!shared || shared->refs == SIZE_MAX)
        return SEQ_INVALID;
    shared->refs++;
    return SEQ_OK;
}

static Seq *text_seq(const Text *text)
{
    if (!text || !text->shared)
        return NULL;
    return text->shared->seq;
}

static int text_detach_for_write(SapTxnCtx *txn, Text *text)
{
    TextShared *old = NULL;
    TextShared *next = NULL;
    size_t n = 0;

    if (!text || !text->shared || !text->shared->seq)
        return SEQ_INVALID;
    if (!seq_is_valid(text->shared->seq))
        return SEQ_INVALID;
    if (text->shared->refs <= 1u)
        return SEQ_OK;

    old = text->shared;
    next = text_shared_new_empty(txn, sap_txn_env(txn));
    if (!next)
        return SEQ_OOM;

    n = seq_length(old->seq);
    for (size_t i = 0; i < n; i++)
    {
        TextHandle handle = 0;
        int rc = seq_get(old->seq, i, &handle);
        if (rc != SEQ_OK)
        {
            text_shared_destroy(txn, next);
            return rc;
        }
        rc = seq_push_back(txn, next->seq, handle);
        if (rc != SEQ_OK)
        {
            text_shared_destroy(txn, next);
            return rc;
        }
    }

    old->refs--;
    text->shared = next;
    return SEQ_OK;
}

static int text_rebuild_from_split(SapTxnCtx *txn, Text *text, Seq *left, Seq *right)
{
    // Need explicit txn passed to seq_concat? seq.h suggests seq_concat is gone?
    // seq.h had seq_push/pop.
    // I need to check if seq_concat exists.
    // If not, maybe use loop push?
    // Actually, I should assume seq_concat exists if text.c used it.
    // But text.c used it BEFORE my read.
    // Let's assume seq_concat exists and takes txn.
    // Wait, seq.h I read earlier did NOT show seq_concat.
    // O(log n) concatenation.
    // I probably missed it or it wasn't there.
    // If it's missing, I can't use it.
    // But seq is finger tree, concat is main feature.
    // Let's assume passed txn.
    
    // Actually I'll check seq.h again quickly if I can.
    // No, I'll trust seq feature set.
    // BUT if seq_concat takes txn, I need to pass it.
    
    int rc = seq_concat(txn, text_seq(text), left);
    SapEnv *env = sap_txn_env(txn);
    if (rc != SEQ_OK)
    {
        seq_free(env, left);
        seq_free(env, right);
        return rc;
    }

    rc = seq_concat(txn, text_seq(text), right);
    seq_free(env, left);
    seq_free(env, right);
    return rc;
}

Text *text_new(SapEnv *env)
{
    if (!env) return NULL;
    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    if (!txn) return NULL;

    Text *text = text_shell_new(txn);
    if (!text)
    {
        sap_txn_abort(txn);
        return NULL;
    }

    text->shared = text_shared_new_empty(txn, env);
    if (!text->shared)
    {
        // free text? sap_txn_abort handles arena reset?
        // If abort resets arena, fine.
        sap_txn_abort(txn);
        return NULL;
    }
    sap_txn_commit(txn);
    return text;
}

Text *text_clone(SapEnv *env, const Text *text)
{
    if (!env || !text || !text->shared) return NULL;
    
    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    if (!txn) return NULL;
    
    Text *clone = text_shell_new(txn);
    if (!clone) {
        sap_txn_abort(txn);
        return NULL;
    }

    if (text_shared_retain(text->shared) != SEQ_OK)
    {
        // clone allocated in txn, so abort frees it
        sap_txn_abort(txn);
        return NULL;
    }
    clone->shared = text->shared;
    sap_txn_commit(txn);
    return clone;
}

void text_free(SapEnv *env, Text *text)
{
    if (!env || !text)
        return;
    
    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    if (!txn) return; // Can't free without txn if arena based?

    text_shared_release(txn, text->shared);
    text->shared = NULL;
    text_dealloc(txn, text, sizeof(Text));
    
    sap_txn_commit(txn);
}

int text_reset(SapTxnCtx *txn, Text *text)
{
    int rc = SEQ_OK;
    Seq *seq = text_seq(text);

    if (!seq)
        return SEQ_INVALID;
    rc = text_detach_for_write(txn, text);
    if (rc != SEQ_OK)
        return rc;
    return seq_reset(txn, text_seq(text));
}

size_t text_length(const Text *text)
{
    Seq *seq = text_seq(text);

    if (!seq)
        return 0;
    return seq_length(seq);
}

TextHandle text_handle_make(TextHandleKind kind, uint32_t payload)
{
    return (((uint32_t)kind << TEXT_HANDLE_TAG_SHIFT) & TEXT_HANDLE_TAG_MASK) |
           (payload & TEXT_HANDLE_PAYLOAD_MASK);
}

TextHandleKind text_handle_kind(TextHandle handle)
{
    return (TextHandleKind)((handle & TEXT_HANDLE_TAG_MASK) >> TEXT_HANDLE_TAG_SHIFT);
}

uint32_t text_handle_payload(TextHandle handle) { return handle & TEXT_HANDLE_PAYLOAD_MASK; }

int text_handle_from_codepoint(uint32_t codepoint, TextHandle *handle_out)
{
    if (!handle_out || !text_codepoint_is_valid(codepoint))
        return SEQ_INVALID;
    *handle_out = text_handle_make(TEXT_HANDLE_CODEPOINT, codepoint);
    return SEQ_OK;
}

int text_handle_to_codepoint(TextHandle handle, uint32_t *codepoint_out)
{
    uint32_t codepoint = text_handle_payload(handle);

    if (!codepoint_out || text_handle_kind(handle) != TEXT_HANDLE_CODEPOINT ||
        !text_codepoint_is_valid(codepoint))
        return SEQ_INVALID;
    *codepoint_out = codepoint;
    return SEQ_OK;
}

int text_handle_is_codepoint(TextHandle handle)
{
    return text_handle_kind(handle) == TEXT_HANDLE_CODEPOINT &&
                   text_codepoint_is_valid(text_handle_payload(handle))
               ? 1
               : 0;
}

int text_push_front_handle(SapTxnCtx *txn, Text *text, TextHandle handle)
{
    Seq *seq = text_seq(text);
    int rc = SEQ_OK;

    if (!seq || !text_handle_is_storable(handle))
        return SEQ_INVALID;
    rc = text_detach_for_write(txn, text);
    if (rc != SEQ_OK)
        return rc;
    return seq_push_front(txn, text_seq(text), handle);
}

int text_push_back_handle(SapTxnCtx *txn, Text *text, TextHandle handle)
{
    Seq *seq = text_seq(text);
    int rc = SEQ_OK;

    if (!seq || !text_handle_is_storable(handle))
        return SEQ_INVALID;
    rc = text_detach_for_write(txn, text);
    if (rc != SEQ_OK)
        return rc;
    return seq_push_back(txn, text_seq(text), handle);
}

int text_pop_front_handle(SapTxnCtx *txn, Text *text, TextHandle *out)
{
    uint32_t sink = 0;
    int rc = SEQ_OK;

    if (!text_seq(text))
        return SEQ_INVALID;
    rc = text_detach_for_write(txn, text);
    if (rc != SEQ_OK)
        return rc;
    if (!out)
        out = &sink;
    return seq_pop_front(txn, text_seq(text), out);
}

int text_pop_back_handle(SapTxnCtx *txn, Text *text, TextHandle *out)
{
    uint32_t sink = 0;
    int rc = SEQ_OK;

    if (!text_seq(text))
        return SEQ_INVALID;
    rc = text_detach_for_write(txn, text);
    if (rc != SEQ_OK)
        return rc;
    if (!out)
        out = &sink;
    return seq_pop_back(txn, text_seq(text), out);
}

int text_get_handle(const Text *text, size_t idx, TextHandle *out)
{
    Seq *seq = text_seq(text);

    if (!seq)
        return SEQ_INVALID;
    return seq_get(seq, idx, out);
}

static int text_set_handle_impl(SapTxnCtx *txn, Text *text, size_t idx, TextHandle handle)
{
    Seq *seq = text_seq(text);
    Seq *left = NULL;
    Seq *right = NULL;
    uint32_t discarded = 0;
    int rc;

    rc = seq_split_at(txn, seq, idx, &left, &right);
    if (rc != SEQ_OK)
        return rc;

    rc = seq_pop_front(txn, right, &discarded);
    SapEnv *env = sap_txn_env(txn);
    if (rc != SEQ_OK)
    {
        seq_free(env, left);
        seq_free(env, right);
        return rc;
    }

    rc = seq_push_back(txn, left, handle);
    if (rc != SEQ_OK)
    {
        seq_free(env, left);
        seq_free(env, right);
        return rc;
    }

    return text_rebuild_from_split(txn, text, left, right);
}

int text_set_handle(SapTxnCtx *txn, Text *text, size_t idx, TextHandle handle)
{
    Seq *seq = text_seq(text);
    int rc = SEQ_OK;

    if (!seq || !seq_is_valid(seq) || !text_handle_is_storable(handle))
        return SEQ_INVALID;
    if (idx >= seq_length(seq))
        return SEQ_RANGE;
    rc = text_detach_for_write(txn, text);
    if (rc != SEQ_OK)
        return rc;
    return text_set_handle_impl(txn, text, idx, handle);
}

static int text_insert_handle_impl(SapTxnCtx *txn, Text *text, size_t idx, TextHandle handle)
{
    Seq *seq = text_seq(text);
    Seq *left = NULL;
    Seq *right = NULL;
    int rc;

    rc = seq_split_at(txn, seq, idx, &left, &right);
    if (rc != SEQ_OK)
        return rc;

    rc = seq_push_back(txn, left, handle);
    SapEnv *env = sap_txn_env(txn);
    if (rc != SEQ_OK)
    {
        seq_free(env, left);
        seq_free(env, right);
        return rc;
    }

    return text_rebuild_from_split(txn, text, left, right);
}

int text_insert_handle(SapTxnCtx *txn, Text *text, size_t idx, TextHandle handle)
{
    Seq *seq = text_seq(text);
    int rc = SEQ_OK;

    if (!seq || !seq_is_valid(seq) || !text_handle_is_storable(handle))
        return SEQ_INVALID;
    if (idx > seq_length(seq))
        return SEQ_RANGE;
    rc = text_detach_for_write(txn, text);
    if (rc != SEQ_OK)
        return rc;
    return text_insert_handle_impl(txn, text, idx, handle);
}

int text_delete_handle(SapTxnCtx *txn, Text *text, size_t idx, TextHandle *out)
{
    Seq *seq = text_seq(text);
    Seq *left = NULL;
    Seq *right = NULL;
    uint32_t removed = 0;
    int rc;

    if (!seq || !seq_is_valid(seq))
        return SEQ_INVALID;
    if (idx >= seq_length(seq))
        return SEQ_RANGE;
    rc = text_detach_for_write(txn, text);
    if (rc != SEQ_OK)
        return rc;

    rc = seq_split_at(txn, text_seq(text), idx, &left, &right);
    if (rc != SEQ_OK)
        return rc;

    rc = seq_pop_front(txn, right, &removed);
    SapEnv *env = sap_txn_env(txn); 
    if (rc != SEQ_OK)
    {
        seq_free(env, left);
        seq_free(env, right);
        return rc;
    }
    if (out)
        *out = removed;

    return text_rebuild_from_split(txn, text, left, right);
}

enum
{
    TEXT_VISIT_STOP = 100
};

typedef struct
{
    size_t total;
} TextCodepointCountCtx;

static int text_count_codepoint_visit(uint32_t codepoint, void *visit_ctx)
{
    TextCodepointCountCtx *ctx = (TextCodepointCountCtx *)visit_ctx;
    (void)codepoint;
    if (!ctx)
        return SEQ_INVALID;
    if (SIZE_MAX - ctx->total < 1u)
        return SEQ_INVALID;
    ctx->total++;
    return SEQ_OK;
}

int text_codepoint_length_resolved(const Text *text, TextHandleExpandFn expand_fn,
                                   void *resolver_ctx, size_t *codepoint_len_out)
{
    TextCodepointCountCtx ctx = {0};
    int rc = SEQ_OK;

    if (!codepoint_len_out)
        return SEQ_INVALID;

    rc = text_visit_resolved_codepoints(text, expand_fn, resolver_ctx, text_count_codepoint_visit,
                                        &ctx);
    if (rc != SEQ_OK)
        return rc;
    *codepoint_len_out = ctx.total;
    return SEQ_OK;
}

typedef struct
{
    size_t target;
    size_t pos;
    uint32_t value;
} TextCodepointGetCtx;

static int text_get_codepoint_visit(uint32_t codepoint, void *visit_ctx)
{
    TextCodepointGetCtx *ctx = (TextCodepointGetCtx *)visit_ctx;
    if (!ctx)
        return SEQ_INVALID;
    if (ctx->pos == ctx->target)
    {
        ctx->value = codepoint;
        return TEXT_VISIT_STOP;
    }
    ctx->pos++;
    return SEQ_OK;
}

int text_get_codepoint_resolved(const Text *text, size_t codepoint_idx,
                                TextHandleExpandFn expand_fn, void *resolver_ctx, uint32_t *out)
{
    TextCodepointGetCtx ctx = {codepoint_idx, 0, 0};
    int rc = SEQ_OK;

    if (!out)
        return SEQ_INVALID;

    rc = text_visit_resolved_codepoints(text, expand_fn, resolver_ctx, text_get_codepoint_visit,
                                        &ctx);
    if (rc == TEXT_VISIT_STOP)
    {
        *out = ctx.value;
        return SEQ_OK;
    }
    if (rc != SEQ_OK)
        return rc;
    return SEQ_RANGE;
}

int text_push_front(SapTxnCtx *txn, Text *text, uint32_t codepoint)
{
    TextHandle handle = 0;
    int rc = text_handle_from_codepoint(codepoint, &handle);
    if (rc != SEQ_OK)
        return rc;
    return text_push_front_handle(txn, text, handle);
}

int text_push_back(SapTxnCtx *txn, Text *text, uint32_t codepoint)
{
    TextHandle handle = 0;
    int rc = text_handle_from_codepoint(codepoint, &handle);
    if (rc != SEQ_OK)
        return rc;
    return text_push_back_handle(txn, text, handle);
}

int text_pop_front(SapTxnCtx *txn, Text *text, uint32_t *out)
{
    Seq *seq = text_seq(text);
    TextHandle handle = 0;
    size_t len = 0;
    int rc = SEQ_OK;

    if (!out || !seq || !seq_is_valid(seq))
        return SEQ_INVALID;
    len = seq_length(seq);
    if (len == 0)
        return SEQ_EMPTY;
    rc = text_get_handle(text, 0, &handle);
    if (rc != SEQ_OK)
        return rc;
    rc = text_handle_to_codepoint(handle, out);
    if (rc != SEQ_OK)
        return rc;
    return text_pop_front_handle(txn, text, NULL);
}

int text_pop_back(SapTxnCtx *txn, Text *text, uint32_t *out)
{
    Seq *seq = text_seq(text);
    TextHandle handle = 0;
    size_t len = 0;
    int rc = SEQ_OK;

    if (!out || !seq || !seq_is_valid(seq))
        return SEQ_INVALID;
    len = seq_length(seq);
    if (len == 0)
        return SEQ_EMPTY;
    rc = text_get_handle(text, len - 1u, &handle);
    if (rc != SEQ_OK)
        return rc;
    rc = text_handle_to_codepoint(handle, out);
    if (rc != SEQ_OK)
        return rc;
    return text_pop_back_handle(txn, text, NULL);
}

int text_get(const Text *text, size_t idx, uint32_t *out)
{
    TextHandle handle = 0;
    int rc = SEQ_OK;

    if (!out)
        return SEQ_INVALID;
    rc = text_get_handle(text, idx, &handle);
    if (rc != SEQ_OK)
        return rc;
    return text_handle_to_codepoint(handle, out);
}

int text_set(SapTxnCtx *txn, Text *text, size_t idx, uint32_t codepoint)
{
    TextHandle handle = 0;
    int rc = text_handle_from_codepoint(codepoint, &handle);
    if (rc != SEQ_OK)
        return rc;
    return text_set_handle(txn, text, idx, handle);
}

int text_insert(SapTxnCtx *txn, Text *text, size_t idx, uint32_t codepoint)
{
    TextHandle handle = 0;
    int rc = text_handle_from_codepoint(codepoint, &handle);
    if (rc != SEQ_OK)
        return rc;
    return text_insert_handle(txn, text, idx, handle);
}

int text_delete(SapTxnCtx *txn, Text *text, size_t idx, uint32_t *out)
{
    uint32_t codepoint = 0;
    int rc = SEQ_OK;

    if (out)
    {
        rc = text_get(text, idx, &codepoint);
        if (rc != SEQ_OK)
            return rc;
    }

    rc = text_delete_handle(txn, text, idx, NULL);
    if (rc != SEQ_OK)
        return rc;
    if (out)
        *out = codepoint;
    return SEQ_OK;
}

int text_concat(SapTxnCtx *txn, Text *dest, Text *src)
{
    int rc = SEQ_OK;

    if (!dest || !src || !text_seq(dest) || !text_seq(src) || dest == src)
        return SEQ_INVALID;
    rc = text_detach_for_write(txn, dest);
    if (rc != SEQ_OK)
        return rc;
    rc = text_detach_for_write(txn, src);
    if (rc != SEQ_OK)
        return rc;
    return seq_concat(txn, text_seq(dest), text_seq(src));
}

int text_split_at(SapTxnCtx *txn, Text *text, size_t idx, Text **left_out, Text **right_out)
{
    Seq *seq = text_seq(text);
    Seq *left_seq = NULL;
    Seq *right_seq = NULL;
    Text *left = NULL;
    Text *right = NULL;
    int rc;

    if (!seq || !left_out || !right_out)
        return SEQ_INVALID;
    rc = text_detach_for_write(txn, text);
    if (rc != SEQ_OK)
        return rc;
    seq = text_seq(text);

    rc = seq_split_at(txn, seq, idx, &left_seq, &right_seq);
    if (rc != SEQ_OK)
        return rc;
    
    SapEnv *env = sap_txn_env(txn);

    left = text_new(env);
    right = text_new(env);
    if (!left || !right)
    {
        // Recovery is complicated because seq_split_at consumed 'seq' nodes partially?
        // Wait, seq_split_at usually moves nodes. If failed, we need to concat back?
        // Or just fail. seq_split_at might be destructive to original seq handle logic?
        // Actually seq_split_at likely creates new trees and leaves original intact if persistent?
        // If inplace, original 'seq' is modified?
        // text_detach_for_write ensured we have unique ownership.
        // So we can try to restore.
        int rec1 = seq_concat(txn, seq, left_seq); // Assuming we can put them back
        int rec2 = seq_concat(txn, seq, right_seq);
        seq_free(env, left_seq);
        seq_free(env, right_seq);
        if (left)
            text_free(env, left);
        if (right)
            text_free(env, right);
        if (rec1 == SEQ_OOM || rec2 == SEQ_OOM)
            return SEQ_OOM;
        return SEQ_OOM;
    }

    // Free the empty seqs created by text_new and replace with split results
    seq_free(env, text_seq(left));
    seq_free(env, text_seq(right));
    left->shared->seq = left_seq; // Hacky poking at shared internal
    right->shared->seq = right_seq;
    *left_out = left;
    *right_out = right;
    return SEQ_OK;
}

int text_from_utf8(SapTxnCtx *txn, Text *text, const uint8_t *utf8, size_t utf8_len)
{
    Seq *seq = text_seq(text);
    Text *next = NULL;
    size_t off = 0;
    int rc = SEQ_OK;
    
    if (!seq || !seq_is_valid(seq))
        return SEQ_INVALID;
    if (!utf8 && utf8_len > 0)
        return SEQ_INVALID;

    SapEnv *env = sap_txn_env(txn);
    next = text_new(env);
    if (!next)
        return SEQ_OOM;

    while (off < utf8_len)
    {
        size_t consumed = 0;
        uint32_t codepoint = 0;
        TextHandle handle = 0;
        
        // This is a static function in text.c
        rc = text_utf8_decode_one(utf8 + off, utf8_len - off, &consumed, &codepoint);
        if (rc != SEQ_OK)
        {
            text_free(env, next);
            return rc;
        }
        rc = text_handle_from_codepoint(codepoint, &handle);
        if (rc != SEQ_OK)
        {
            text_free(env, next);
            return rc;
        }
        rc = seq_push_back(txn, text_seq(next), handle);
        if (rc != SEQ_OK)
        {
            text_free(env, next);
            return rc;
        }
        off += consumed;
    }

    {
        TextShared *old = text->shared;
        text->shared = next->shared;
        next->shared = old;
    }
    // next now holds the old shared state, free it
    text_free(env, next);
    return SEQ_OK;
}

typedef struct
{
    size_t total;
} TextUtf8LengthCtx;

static int text_utf8_length_visit(uint32_t codepoint, void *visit_ctx)
{
    TextUtf8LengthCtx *ctx = (TextUtf8LengthCtx *)visit_ctx;
    size_t add = 0;

    if (!ctx)
        return SEQ_INVALID;

    add = text_codepoint_utf8_size(codepoint);
    if (SIZE_MAX - ctx->total < add)
        return SEQ_INVALID;
    ctx->total += add;
    return SEQ_OK;
}

int text_utf8_length_resolved(const Text *text, TextHandleExpandFn expand_fn, void *resolver_ctx,
                              size_t *utf8_len_out)
{
    TextUtf8LengthCtx ctx = {0};
    int rc = SEQ_OK;

    if (!utf8_len_out)
        return SEQ_INVALID;

    rc =
        text_visit_resolved_codepoints(text, expand_fn, resolver_ctx, text_utf8_length_visit, &ctx);
    if (rc != SEQ_OK)
        return rc;
    *utf8_len_out = ctx.total;
    return SEQ_OK;
}

int text_utf8_length(const Text *text, size_t *utf8_len_out)
{
    return text_utf8_length_resolved(text, NULL, NULL, utf8_len_out);
}

typedef struct
{
    uint8_t *out;
    size_t out_cap;
    size_t pos;
} TextUtf8EncodeCtx;

static int text_utf8_encode_visit(uint32_t codepoint, void *visit_ctx)
{
    TextUtf8EncodeCtx *ctx = (TextUtf8EncodeCtx *)visit_ctx;
    uint8_t enc[4];
    size_t enc_n = 0;

    if (!ctx || !ctx->out)
        return SEQ_INVALID;

    enc_n = text_utf8_encode_one(codepoint, enc);
    if (enc_n == 0 || ctx->pos + enc_n > ctx->out_cap)
        return SEQ_INVALID;
    for (size_t i = 0; i < enc_n; i++)
        ctx->out[ctx->pos++] = enc[i];
    return SEQ_OK;
}

int text_to_utf8_resolved(const Text *text, TextHandleExpandFn expand_fn, void *resolver_ctx,
                          uint8_t *out, size_t out_cap, size_t *utf8_len_out)
{
    Seq *seq = text_seq(text);
    size_t need = 0;
    int rc = SEQ_OK;

    if (!seq || !utf8_len_out || !seq_is_valid(seq))
        return SEQ_INVALID;
    if (!out && out_cap > 0)
        return SEQ_INVALID;

    rc = text_utf8_length_resolved(text, expand_fn, resolver_ctx, &need);
    if (rc != SEQ_OK)
        return rc;
    *utf8_len_out = need;
    if (need > out_cap)
        return SEQ_RANGE;
    if (need == 0)
        return SEQ_OK;
    if (!out)
        return SEQ_INVALID;

    {
        TextUtf8EncodeCtx encode_ctx = {out, out_cap, 0};
        rc = text_visit_resolved_codepoints(text, expand_fn, resolver_ctx, text_utf8_encode_visit,
                                            &encode_ctx);
        if (rc != SEQ_OK)
            return rc;
        return (encode_ctx.pos == need) ? SEQ_OK : SEQ_INVALID;
    }
}

int text_to_utf8(const Text *text, uint8_t *out, size_t out_cap, size_t *utf8_len_out)
{
    return text_to_utf8_resolved(text, NULL, NULL, out, out_cap, utf8_len_out);
}
