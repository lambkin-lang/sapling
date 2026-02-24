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
    if (!text || !text->seq)
        return SEQ_INVALID;
    return seq_push_front(text->seq, codepoint);
}

int text_push_back(Text *text, uint32_t codepoint)
{
    if (!text || !text->seq)
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

    if (!text || !text->seq || !seq_is_valid(text->seq))
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

    if (!text || !text->seq || !seq_is_valid(text->seq))
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
