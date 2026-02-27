/*
 * text.h — mutable code-point text built on top of Seq
 *
 * Text stores Unicode code points as uint32_t values.
 * Operations return SEQ_* status codes from seq.h.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_TEXT_H
#define SAPLING_TEXT_H

#include "sapling/seq.h"
#include "sapling/txn.h"

#include <stddef.h>
#include <stdint.h>

/* Opaque text handle. */
typedef struct Text Text;

/* ------------------------------------------------------------------ */
/* Tagged text element handles                                         */
/* ------------------------------------------------------------------ */

/*
 * Text stores leaves as uint32_t handles:
 * - CODEPOINT: payload is a Unicode scalar value
 * - LITERAL: payload is runtime-defined literal-table id
 * - TREE: payload is runtime-defined subtree/COW id
 */
typedef uint32_t TextHandle;

typedef enum TextHandleKind
{
    TEXT_HANDLE_CODEPOINT = 0u,
    TEXT_HANDLE_LITERAL = 1u,
    TEXT_HANDLE_TREE = 2u,
    TEXT_HANDLE_RESERVED = 3u
} TextHandleKind;

#define TEXT_HANDLE_TAG_SHIFT 30u
#define TEXT_HANDLE_TAG_MASK 0xC0000000u
#define TEXT_HANDLE_PAYLOAD_MASK 0x3FFFFFFFu

TextHandle    text_handle_make(TextHandleKind kind, uint32_t payload);
TextHandleKind text_handle_kind(TextHandle handle);
uint32_t      text_handle_payload(TextHandle handle);
int           text_handle_from_codepoint(uint32_t codepoint, TextHandle *handle_out);
int           text_handle_to_codepoint(TextHandle handle, uint32_t *codepoint_out);
int           text_handle_is_codepoint(TextHandle handle);

/*
 * Expand a non-codepoint handle into zero or more Unicode scalar values.
 * Implementations call emit_fn for each expanded codepoint.
 */
typedef int (*TextEmitCodepointFn)(uint32_t codepoint, void *emit_ctx);
typedef int (*TextHandleExpandFn)(TextHandle handle, TextEmitCodepointFn emit_fn, void *emit_ctx,
                                  void *resolver_ctx);

/*
 * Runtime resolver adapter for mixed-handle trees:
 * - LITERAL payloads map to UTF-8 bytes.
 * - TREE payloads map to nested Text values.
 * - Expansion enforces depth and node-visit guards and rejects cycles.
 */
typedef int (*TextResolveLiteralUtf8Fn)(uint32_t literal_id, const uint8_t **utf8_out,
                                        size_t *utf8_len_out, void *ctx);
typedef int (*TextResolveTreeTextFn)(uint32_t tree_id, const Text **tree_out, void *ctx);

typedef struct TextRuntimeResolver
{
    TextResolveLiteralUtf8Fn resolve_literal_utf8_fn;
    TextResolveTreeTextFn    resolve_tree_text_fn;
    void                    *ctx;
    /* 0 => default 64 */
    size_t max_tree_depth;
    /* 0 => default 4096 */
    size_t max_tree_visits;
} TextRuntimeResolver;

/*
 * Adapter callback suitable for *_resolved APIs.
 * resolver_ctx must point to TextRuntimeResolver.
 */
int text_expand_runtime_handle(TextHandle handle, TextEmitCodepointFn emit_fn, void *emit_ctx,
                               void *resolver_ctx);

/* Lifecycle */
Text *text_new(SapEnv *env);
/* O(1) clone with copy-on-write sharing. */
Text *text_clone(SapEnv *env, const Text *text);

void  text_free(SapEnv *env, Text *text);
int   text_reset(SapTxnCtx *txn, Text *text);
int   text_is_valid(const Text *text);

/* Length in stored leaves (code points when only CODEPOINT handles are present). */
size_t text_length(const Text *text);

/* Raw tagged-handle operations (element-count semantics). */
int text_push_front_handle(SapTxnCtx *txn, Text *text, TextHandle handle);
int text_push_back_handle(SapTxnCtx *txn, Text *text, TextHandle handle);
int text_pop_front_handle(SapTxnCtx *txn, Text *text, TextHandle *out);
int text_pop_back_handle(SapTxnCtx *txn, Text *text, TextHandle *out);
int text_get_handle(const Text *text, size_t idx, TextHandle *out);
int text_set_handle(SapTxnCtx *txn, Text *text, size_t idx, TextHandle handle);
int text_insert_handle(SapTxnCtx *txn, Text *text, size_t idx, TextHandle handle);
int text_delete_handle(SapTxnCtx *txn, Text *text, size_t idx, TextHandle *out);

/*
 * Resolved code-point view:
 * - codepoint handles contribute one code point.
 * - non-codepoint handles are expanded through expand_fn.
 * - when non-codepoint handles exist and expand_fn is NULL, returns SEQ_INVALID.
 */
int text_codepoint_length_resolved(const Text *text, TextHandleExpandFn expand_fn,
                                   void *resolver_ctx, size_t *codepoint_len_out);
int text_get_codepoint_resolved(const Text *text, size_t codepoint_idx,
                                TextHandleExpandFn expand_fn, void *resolver_ctx, uint32_t *out);

/* End operations */
int text_push_front(SapTxnCtx *txn, Text *text, uint32_t codepoint);
int text_push_back(SapTxnCtx *txn, Text *text, uint32_t codepoint);
int text_pop_front(SapTxnCtx *txn, Text *text, uint32_t *out);
int text_pop_back(SapTxnCtx *txn, Text *text, uint32_t *out);

/* Indexed access/update */
int text_get(const Text *text, size_t idx, uint32_t *out);
int text_set(SapTxnCtx *txn, Text *text, size_t idx, uint32_t codepoint);
int text_insert(SapTxnCtx *txn, Text *text, size_t idx, uint32_t codepoint);

/*
 * text_delete — remove one code point at idx.
 * If out is non-NULL, the removed code point is stored in *out.
 */
int text_delete(SapTxnCtx *txn, Text *text, size_t idx, uint32_t *out);

/* Structural operations */
int text_concat(SapTxnCtx *txn, Text *dest, Text *src);
int text_split_at(SapTxnCtx *txn, Text *text, size_t idx, Text **left_out, Text **right_out);

/*
 * UTF-8 bridge (strict):
 * - Decoding rejects invalid UTF-8 (including overlong forms, surrogates,
 *   and code points > U+10FFFF).
 * - Encoding rejects non-codepoint handles and invalid code-point payloads.
 * - text_to_utf8 writes into caller-provided storage; it never allocates.
 */
int text_from_utf8(SapTxnCtx *txn, Text *text, const uint8_t *utf8, size_t utf8_len);
int text_utf8_length(const Text *text, size_t *utf8_len_out);
int text_to_utf8(const Text *text, uint8_t *out, size_t out_cap, size_t *utf8_len_out);
int text_utf8_length_resolved(const Text *text, TextHandleExpandFn expand_fn, void *resolver_ctx,
                              size_t *utf8_len_out);
int text_to_utf8_resolved(const Text *text, TextHandleExpandFn expand_fn, void *resolver_ctx,
                          uint8_t *out, size_t out_cap, size_t *utf8_len_out);

#endif /* SAPLING_TEXT_H */
