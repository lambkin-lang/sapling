/*
 * fuzz_seq.c - libFuzzer harness for seq against a simple vector model
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/seq.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct
{
    uint32_t *data;
    size_t len;
    size_t cap;
} ModelVec;

static void model_init(ModelVec *m)
{
    m->data = NULL;
    m->len = 0;
    m->cap = 0;
}

static void model_free(ModelVec *m)
{
    free(m->data);
    m->data = NULL;
    m->len = 0;
    m->cap = 0;
}

static int model_reserve(ModelVec *m, size_t need)
{
    if (need <= m->cap)
        return 1;

    size_t new_cap = (m->cap > 0) ? m->cap : 16;
    while (new_cap < need)
    {
        if (new_cap > SIZE_MAX / 2)
            new_cap = need;
        else
            new_cap *= 2;
    }
    if (new_cap > SIZE_MAX / sizeof(uint32_t))
        return 0;

    uint32_t *next = (uint32_t *)realloc(m->data, new_cap * sizeof(uint32_t));
    if (!next)
        return 0;
    m->data = next;
    m->cap = new_cap;
    return 1;
}

static int model_push_back(ModelVec *m, uint32_t v)
{
    if (!model_reserve(m, m->len + 1))
        return 0;
    m->data[m->len++] = v;
    return 1;
}

static int model_push_front(ModelVec *m, uint32_t v)
{
    if (!model_reserve(m, m->len + 1))
        return 0;
    memmove(&m->data[1], &m->data[0], m->len * sizeof(uint32_t));
    m->data[0] = v;
    m->len++;
    return 1;
}

static int model_pop_back(ModelVec *m, uint32_t *out)
{
    if (m->len == 0)
        return 0;
    *out = m->data[m->len - 1];
    m->len--;
    return 1;
}

static int model_pop_front(ModelVec *m, uint32_t *out)
{
    if (m->len == 0)
        return 0;
    *out = m->data[0];
    memmove(&m->data[0], &m->data[1], (m->len - 1) * sizeof(uint32_t));
    m->len--;
    return 1;
}

static int model_concat(ModelVec *dst, const ModelVec *src)
{
    if (!model_reserve(dst, dst->len + src->len))
        return 0;
    memcpy(&dst->data[dst->len], src->data, src->len * sizeof(uint32_t));
    dst->len += src->len;
    return 1;
}

static int seq_matches_model(Seq *seq, const ModelVec *model)
{
    if (!seq_is_valid(seq))
        return 0;
    if (seq_length(seq) != model->len)
        return 0;
    for (size_t i = 0; i < model->len; i++)
    {
        uint32_t got = 0;
        if (seq_get(seq, i, &got) != SEQ_OK)
            return 0;
        if (got != model->data[i])
            return 0;
    }
    return 1;
}

static int seq_matches_model_slice(Seq *seq, const ModelVec *model, size_t off, size_t n)
{
    if (!seq_is_valid(seq))
        return 0;
    if (seq_length(seq) != n)
        return 0;
    for (size_t i = 0; i < n; i++)
    {
        uint32_t got = 0;
        if (seq_get(seq, i, &got) != SEQ_OK)
            return 0;
        if (got != model->data[off + i])
            return 0;
    }
    return 1;
}

static int model_sync_from_seq(ModelVec *model, Seq *seq)
{
    size_t len = 0;
    if (!seq_is_valid(seq))
        return 0;
    len = seq_length(seq);
    if (!model_reserve(model, len))
        return 0;
    for (size_t i = 0; i < len; i++)
    {
        uint32_t got = 0;
        if (seq_get(seq, i, &got) != SEQ_OK)
            return 0;
        model->data[i] = got;
    }
    model->len = len;
    return 1;
}

#ifdef SAPLING_SEQ_TESTING
static int recover_after_oom(Seq *seq, ModelVec *model)
{
    if (seq_is_valid(seq))
        return model_sync_from_seq(model, seq);
    if (seq_reset(seq) != SEQ_OK)
        return 0;
    model->len = 0;
    return 1;
}
#endif

static uint32_t read_u32(const uint8_t *p)
{
    return ((uint32_t)p[0]) | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    Seq *seq = seq_new();
    ModelVec model;
    size_t i = 0;

    model_init(&model);
    if (!seq)
    {
        model_free(&model);
        return 0;
    }

    while (i < size)
    {
        uint8_t op = data[i++] % 12u;
        switch (op)
        {
        case 0: /* push_front */
            if (i + 4 > size)
                i = size;
            else
            {
                uint32_t v = read_u32(&data[i]);
                i += 4;
                if (seq_push_front(seq, v) != SEQ_OK || !model_push_front(&model, v))
                    goto out;
            }
            break;
        case 1: /* push_back */
            if (i + 4 > size)
                i = size;
            else
            {
                uint32_t v = read_u32(&data[i]);
                i += 4;
                if (seq_push_back(seq, v) != SEQ_OK || !model_push_back(&model, v))
                    goto out;
            }
            break;
        case 2: /* pop_front */
        {
            uint32_t got = 0;
            uint32_t exp = 0;
            if (model.len == 0)
            {
                if (seq_pop_front(seq, &got) != SEQ_EMPTY)
                    __builtin_trap();
            }
            else
            {
                if (seq_pop_front(seq, &got) != SEQ_OK)
                    __builtin_trap();
                if (!model_pop_front(&model, &exp))
                    __builtin_trap();
                if (got != exp)
                    __builtin_trap();
            }
            break;
        }
        case 3: /* pop_back */
        {
            uint32_t got = 0;
            uint32_t exp = 0;
            if (model.len == 0)
            {
                if (seq_pop_back(seq, &got) != SEQ_EMPTY)
                    __builtin_trap();
            }
            else
            {
                if (seq_pop_back(seq, &got) != SEQ_OK)
                    __builtin_trap();
                if (!model_pop_back(&model, &exp))
                    __builtin_trap();
                if (got != exp)
                    __builtin_trap();
            }
            break;
        }
        case 4: /* get in-range / out-of-range */
        {
            uint32_t got = 0;
            if (model.len > 0 && i < size && (data[i++] & 1u))
            {
                size_t idx = 0;
                if (i < size)
                    idx = (size_t)data[i++] % model.len;
                if (seq_get(seq, idx, &got) != SEQ_OK || got != model.data[idx])
                    __builtin_trap();
            }
            else
            {
                size_t idx = model.len + ((i < size) ? (size_t)(data[i++] % 4u) : 1u);
                if (seq_get(seq, idx, &got) != SEQ_RANGE)
                    __builtin_trap();
            }
            break;
        }
        case 5: /* split and re-concat */
        {
            size_t idx = 0;
            Seq *l = NULL;
            Seq *r = NULL;
            if (model.len > 0 && i < size)
                idx = (size_t)data[i++] % (model.len + 1u);
            if (seq_split_at(seq, idx, &l, &r) != SEQ_OK)
                __builtin_trap();
            if (!seq_matches_model_slice(l, &model, 0, idx))
                __builtin_trap();
            if (!seq_matches_model_slice(r, &model, idx, model.len - idx))
                __builtin_trap();
            if (seq_concat(seq, l) != SEQ_OK || seq_concat(seq, r) != SEQ_OK)
                __builtin_trap();
            seq_free(l);
            seq_free(r);
            break;
        }
        case 6: /* concat random chunk */
        {
            Seq *chunk = seq_new();
            ModelVec chunk_model;
            size_t count = 0;

            model_init(&chunk_model);
            if (!chunk)
            {
                model_free(&chunk_model);
                goto out;
            }
            if (i < size)
                count = (size_t)(data[i++] % 8u);
            for (size_t n = 0; n < count; n++)
            {
                uint32_t v = 0;
                if (i + 4 > size)
                    break;
                v = read_u32(&data[i]);
                i += 4;
                if (i < size && (data[i++] & 1u))
                {
                    if (seq_push_front(chunk, v) != SEQ_OK || !model_push_front(&chunk_model, v))
                        __builtin_trap();
                }
                else
                {
                    if (seq_push_back(chunk, v) != SEQ_OK || !model_push_back(&chunk_model, v))
                        __builtin_trap();
                }
            }
            if (seq_concat(seq, chunk) != SEQ_OK || !model_concat(&model, &chunk_model))
                __builtin_trap();
            seq_free(chunk);
            model_free(&chunk_model);
            break;
        }
        case 7: /* reset */
            if (seq_reset(seq) != SEQ_OK)
                __builtin_trap();
            model.len = 0;
            break;
        case 8: /* split out-of-range contract */
        {
            Seq *l = (Seq *)(uintptr_t)1;
            Seq *r = (Seq *)(uintptr_t)2;
            if (seq_split_at(seq, model.len + 1u, &l, &r) != SEQ_RANGE)
                __builtin_trap();
            if (l != (Seq *)(uintptr_t)1 || r != (Seq *)(uintptr_t)2)
                __builtin_trap();
            break;
        }
        case 9: /* fault-injected mutators */
        {
#ifdef SAPLING_SEQ_TESTING
            uint8_t selector = (i < size) ? (uint8_t)(data[i++] % 4u) : 0u;
            int64_t fail_after = (i < size) ? (int64_t)(data[i++] % 24u) : 0;

            switch (selector)
            {
            case 0: /* push_back under deterministic alloc fault */
            {
                uint32_t v = 0;
                int rc;
                if (i + 4 > size)
                    break;
                v = read_u32(&data[i]);
                i += 4;

                seq_test_fail_alloc_after(fail_after);
                rc = seq_push_back(seq, v);
                seq_test_clear_alloc_fail();
                if (rc == SEQ_OK)
                {
                    if (!model_push_back(&model, v))
                        goto out;
                }
                else if (rc == SEQ_OOM)
                {
                    if (!recover_after_oom(seq, &model))
                        goto out;
                }
                else
                {
                    __builtin_trap();
                }
                break;
            }
            case 1: /* concat under deterministic alloc fault */
            {
                Seq *chunk = seq_new();
                ModelVec chunk_model;
                size_t count = 0;
                int rc;

                model_init(&chunk_model);
                if (!chunk)
                {
                    model_free(&chunk_model);
                    goto out;
                }

                if (i < size)
                    count = (size_t)(data[i++] % 6u);
                for (size_t n = 0; n < count; n++)
                {
                    uint32_t v = 0;
                    if (i + 4 > size)
                        break;
                    v = read_u32(&data[i]);
                    i += 4;
                    if (seq_push_back(chunk, v) != SEQ_OK || !model_push_back(&chunk_model, v))
                    {
                        seq_free(chunk);
                        model_free(&chunk_model);
                        goto out;
                    }
                }

                seq_test_fail_alloc_after(fail_after);
                rc = seq_concat(seq, chunk);
                seq_test_clear_alloc_fail();
                if (rc == SEQ_OK)
                {
                    if (!model_concat(&model, &chunk_model))
                    {
                        seq_free(chunk);
                        model_free(&chunk_model);
                        goto out;
                    }
                }
                else if (rc == SEQ_OOM)
                {
                    if (!recover_after_oom(seq, &model))
                    {
                        seq_free(chunk);
                        model_free(&chunk_model);
                        goto out;
                    }
                }
                else
                {
                    seq_free(chunk);
                    model_free(&chunk_model);
                    __builtin_trap();
                }

                seq_free(chunk);
                model_free(&chunk_model);
                break;
            }
            case 2: /* split under deterministic alloc fault */
            {
                size_t idx = 0;
                Seq *l = (Seq *)(uintptr_t)11;
                Seq *r = (Seq *)(uintptr_t)22;
                int rc;

                if (model.len > 0 && i < size)
                    idx = (size_t)(data[i++] % (model.len + 1u));

                seq_test_fail_alloc_after(fail_after);
                rc = seq_split_at(seq, idx, &l, &r);
                seq_test_clear_alloc_fail();
                if (rc == SEQ_OK)
                {
                    if (!seq_matches_model_slice(l, &model, 0, idx) ||
                        !seq_matches_model_slice(r, &model, idx, model.len - idx))
                        __builtin_trap();
                    if (seq_concat(seq, l) != SEQ_OK || seq_concat(seq, r) != SEQ_OK)
                        __builtin_trap();
                    seq_free(l);
                    seq_free(r);
                }
                else if (rc == SEQ_OOM)
                {
                    if (l != (Seq *)(uintptr_t)11 || r != (Seq *)(uintptr_t)22)
                        __builtin_trap();
                    if (!recover_after_oom(seq, &model))
                        goto out;
                }
                else
                {
                    __builtin_trap();
                }
                break;
            }
            default: /* reset under deterministic alloc fault */
            {
                int rc;
                seq_test_fail_alloc_after(fail_after);
                rc = seq_reset(seq);
                seq_test_clear_alloc_fail();
                if (rc == SEQ_OK)
                {
                    model.len = 0;
                }
                else if (rc == SEQ_OOM)
                {
                    if (!recover_after_oom(seq, &model))
                        goto out;
                }
                else
                {
                    __builtin_trap();
                }
                break;
            }
            }
#endif
            break;
        }
        default: /* periodic consistency check */
            break;
        }

        if (!seq_matches_model(seq, &model))
            __builtin_trap();
    }

out:
    seq_free(seq);
    model_free(&model);
    return 0;
}
