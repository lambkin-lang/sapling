/*
 * text.c — mutable code-point text built on top of Seq
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text.h"

#include <stdlib.h>

struct Text
{
    Seq         *seq;
    SeqAllocator allocator;
};

static int text_codepoint_is_valid(uint32_t codepoint)
{
    return (codepoint <= 0x10FFFFu) &&
           !(codepoint >= 0xD800u && codepoint <= 0xDFFFu);
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
        *codepoint_out = ((uint32_t)(b0 & 0x0Fu) << 12) |
                         ((uint32_t)(b1 & 0x3Fu) << 6) | (uint32_t)(b2 & 0x3Fu);
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
        *codepoint_out = ((uint32_t)(b0 & 0x07u) << 18) |
                         ((uint32_t)(b1 & 0x3Fu) << 12) |
                         ((uint32_t)(b2 & 0x3Fu) << 6) | (uint32_t)(b3 & 0x3Fu);
        return SEQ_OK;
    }

    return SEQ_INVALID;
}

static void *text_allocator_malloc(void *ctx, size_t bytes)
{
    (void)ctx;
    return malloc(bytes);
}

static void text_allocator_free(void *ctx, void *ptr)
{
    (void)ctx;
    free(ptr);
}

static SeqAllocator text_allocator_default(void)
{
    SeqAllocator allocator = {text_allocator_malloc, text_allocator_free, NULL};
    return allocator;
}

static int text_allocator_is_valid(const SeqAllocator *allocator)
{
    return allocator && allocator->alloc_fn && allocator->free_fn;
}

static void *text_alloc(const SeqAllocator *allocator, size_t bytes)
{
    return allocator->alloc_fn(allocator->ctx, bytes);
}

static void text_dealloc(const SeqAllocator *allocator, void *ptr)
{
    allocator->free_fn(allocator->ctx, ptr);
}

static Text *text_shell_new(const SeqAllocator *allocator)
{
    Text *text = (Text *)text_alloc(allocator, sizeof(Text));
    if (!text)
        return NULL;
    text->seq = NULL;
    text->allocator = *allocator;
    return text;
}

static int text_rebuild_from_split(Text *text, Seq *left, Seq *right)
{
    int rc = seq_concat(text->seq, left);
    if (rc != SEQ_OK)
    {
        seq_free(left);
        seq_free(right);
        return rc;
    }

    rc = seq_concat(text->seq, right);
    seq_free(left);
    seq_free(right);
    return rc;
}

Text *text_new_with_allocator(const SeqAllocator *allocator)
{
    SeqAllocator resolved = allocator ? *allocator : text_allocator_default();
    if (!text_allocator_is_valid(&resolved))
        return NULL;

    Text *text = text_shell_new(&resolved);
    if (!text)
        return NULL;

    text->seq = seq_new_with_allocator(&resolved);
    if (!text->seq)
    {
        text_dealloc(&resolved, text);
        return NULL;
    }
    return text;
}

Text *text_new(void)
{
    return text_new_with_allocator(NULL);
}

void text_free(Text *text)
{
    if (!text)
        return;
    seq_free(text->seq);
    text->seq = NULL;
    text_dealloc(&text->allocator, text);
}

int text_is_valid(const Text *text)
{
    return (text && text->seq && seq_is_valid(text->seq)) ? 1 : 0;
}

int text_reset(Text *text)
{
    if (!text || !text->seq)
        return SEQ_INVALID;
    return seq_reset(text->seq);
}

size_t text_length(const Text *text)
{
    if (!text || !text->seq)
        return 0;
    return seq_length(text->seq);
}

int text_push_front(Text *text, uint32_t codepoint)
{
    if (!text || !text->seq || !text_codepoint_is_valid(codepoint))
        return SEQ_INVALID;
    return seq_push_front(text->seq, codepoint);
}

int text_push_back(Text *text, uint32_t codepoint)
{
    if (!text || !text->seq || !text_codepoint_is_valid(codepoint))
        return SEQ_INVALID;
    return seq_push_back(text->seq, codepoint);
}

int text_pop_front(Text *text, uint32_t *out)
{
    if (!text || !text->seq)
        return SEQ_INVALID;
    return seq_pop_front(text->seq, out);
}

int text_pop_back(Text *text, uint32_t *out)
{
    if (!text || !text->seq)
        return SEQ_INVALID;
    return seq_pop_back(text->seq, out);
}

int text_get(const Text *text, size_t idx, uint32_t *out)
{
    if (!text || !text->seq)
        return SEQ_INVALID;
    return seq_get(text->seq, idx, out);
}

int text_set(Text *text, size_t idx, uint32_t codepoint)
{
    Seq      *left = NULL;
    Seq      *right = NULL;
    uint32_t  discarded = 0;
    int       rc;

    if (!text || !text->seq || !seq_is_valid(text->seq) ||
        !text_codepoint_is_valid(codepoint))
        return SEQ_INVALID;
    if (idx >= seq_length(text->seq))
        return SEQ_RANGE;

    rc = seq_split_at(text->seq, idx, &left, &right);
    if (rc != SEQ_OK)
        return rc;

    rc = seq_pop_front(right, &discarded);
    if (rc != SEQ_OK)
    {
        seq_free(left);
        seq_free(right);
        return rc;
    }

    rc = seq_push_back(left, codepoint);
    if (rc != SEQ_OK)
    {
        seq_free(left);
        seq_free(right);
        return rc;
    }

    return text_rebuild_from_split(text, left, right);
}

int text_insert(Text *text, size_t idx, uint32_t codepoint)
{
    Seq *left = NULL;
    Seq *right = NULL;
    int  rc;

    if (!text || !text->seq || !seq_is_valid(text->seq) ||
        !text_codepoint_is_valid(codepoint))
        return SEQ_INVALID;
    if (idx > seq_length(text->seq))
        return SEQ_RANGE;

    rc = seq_split_at(text->seq, idx, &left, &right);
    if (rc != SEQ_OK)
        return rc;

    rc = seq_push_back(left, codepoint);
    if (rc != SEQ_OK)
    {
        seq_free(left);
        seq_free(right);
        return rc;
    }

    return text_rebuild_from_split(text, left, right);
}

int text_delete(Text *text, size_t idx, uint32_t *out)
{
    Seq      *left = NULL;
    Seq      *right = NULL;
    uint32_t  removed = 0;
    int       rc;

    if (!text || !text->seq || !seq_is_valid(text->seq))
        return SEQ_INVALID;
    if (idx >= seq_length(text->seq))
        return SEQ_RANGE;

    rc = seq_split_at(text->seq, idx, &left, &right);
    if (rc != SEQ_OK)
        return rc;

    rc = seq_pop_front(right, &removed);
    if (rc != SEQ_OK)
    {
        seq_free(left);
        seq_free(right);
        return rc;
    }
    if (out)
        *out = removed;

    return text_rebuild_from_split(text, left, right);
}

int text_concat(Text *dest, Text *src)
{
    if (!dest || !src || !dest->seq || !src->seq || dest == src)
        return SEQ_INVALID;
    return seq_concat(dest->seq, src->seq);
}

int text_split_at(Text *text, size_t idx, Text **left_out, Text **right_out)
{
    Seq  *left_seq = NULL;
    Seq  *right_seq = NULL;
    Text *left = NULL;
    Text *right = NULL;
    int   rc;

    if (!text || !text->seq || !left_out || !right_out)
        return SEQ_INVALID;

    rc = seq_split_at(text->seq, idx, &left_seq, &right_seq);
    if (rc != SEQ_OK)
        return rc;

    left = text_shell_new(&text->allocator);
    right = text_shell_new(&text->allocator);
    if (!left || !right)
    {
        int rec1 = seq_concat(text->seq, left_seq);
        int rec2 = seq_concat(text->seq, right_seq);
        seq_free(left_seq);
        seq_free(right_seq);
        if (left)
            text_dealloc(&text->allocator, left);
        if (right)
            text_dealloc(&text->allocator, right);
        if (rec1 == SEQ_OOM || rec2 == SEQ_OOM)
            return SEQ_OOM;
        return SEQ_OOM;
    }

    left->seq = left_seq;
    right->seq = right_seq;
    *left_out = left;
    *right_out = right;
    return SEQ_OK;
}

int text_from_utf8(Text *text, const uint8_t *utf8, size_t utf8_len)
{
    Text  *next = NULL;
    size_t off = 0;
    int    rc = SEQ_OK;

    if (!text || !text->seq || !seq_is_valid(text->seq))
        return SEQ_INVALID;
    if (!utf8 && utf8_len > 0)
        return SEQ_INVALID;

    next = text_new_with_allocator(&text->allocator);
    if (!next)
        return SEQ_OOM;

    while (off < utf8_len)
    {
        size_t consumed = 0;
        uint32_t codepoint = 0;
        rc = text_utf8_decode_one(utf8 + off, utf8_len - off, &consumed, &codepoint);
        if (rc != SEQ_OK)
        {
            text_free(next);
            return rc;
        }
        rc = seq_push_back(next->seq, codepoint);
        if (rc != SEQ_OK)
        {
            text_free(next);
            return rc;
        }
        off += consumed;
    }

    {
        Seq *old = text->seq;
        text->seq = next->seq;
        next->seq = old;
    }
    text_free(next);
    return SEQ_OK;
}

int text_utf8_length(const Text *text, size_t *utf8_len_out)
{
    size_t total = 0;
    size_t n = 0;

    if (!text || !text->seq || !utf8_len_out || !seq_is_valid(text->seq))
        return SEQ_INVALID;

    n = seq_length(text->seq);
    for (size_t i = 0; i < n; i++)
    {
        uint32_t codepoint = 0;
        size_t   add = 0;

        if (seq_get(text->seq, i, &codepoint) != SEQ_OK)
            return SEQ_INVALID;
        if (!text_codepoint_is_valid(codepoint))
            return SEQ_INVALID;
        add = text_codepoint_utf8_size(codepoint);
        if (SIZE_MAX - total < add)
            return SEQ_INVALID;
        total += add;
    }

    *utf8_len_out = total;
    return SEQ_OK;
}

int text_to_utf8(const Text *text, uint8_t *out, size_t out_cap, size_t *utf8_len_out)
{
    size_t need = 0;
    size_t n = 0;
    size_t pos = 0;
    int    rc = SEQ_OK;

    if (!text || !text->seq || !utf8_len_out || !seq_is_valid(text->seq))
        return SEQ_INVALID;
    if (!out && out_cap > 0)
        return SEQ_INVALID;

    rc = text_utf8_length(text, &need);
    if (rc != SEQ_OK)
        return rc;
    *utf8_len_out = need;
    if (need > out_cap)
        return SEQ_RANGE;
    if (need == 0)
        return SEQ_OK;
    if (!out)
        return SEQ_INVALID;

    n = seq_length(text->seq);
    for (size_t i = 0; i < n; i++)
    {
        uint32_t codepoint = 0;
        uint8_t  enc[4];
        size_t   enc_n = 0;

        if (seq_get(text->seq, i, &codepoint) != SEQ_OK)
            return SEQ_INVALID;
        enc_n = text_utf8_encode_one(codepoint, enc);
        if (enc_n == 0 || pos + enc_n > out_cap)
            return SEQ_INVALID;
        for (size_t j = 0; j < enc_n; j++)
            out[pos++] = enc[j];
    }

    return (pos == need) ? SEQ_OK : SEQ_INVALID;
}
