/*
 * fuzz_text.c - libFuzzer harness for text against a vector model
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct
{
    uint32_t *data;
    size_t    len;
    size_t    cap;
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

static int model_clone(ModelVec *dst, const ModelVec *src)
{
    if (!model_reserve(dst, src->len))
        return 0;
    memcpy(dst->data, src->data, src->len * sizeof(uint32_t));
    dst->len = src->len;
    return 1;
}

static int model_equal(const ModelVec *a, const ModelVec *b)
{
    if (a->len != b->len)
        return 0;
    return memcmp(a->data, b->data, a->len * sizeof(uint32_t)) == 0;
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

static int model_insert(ModelVec *m, size_t idx, uint32_t v)
{
    if (idx > m->len)
        return 0;
    if (!model_reserve(m, m->len + 1))
        return 0;
    memmove(&m->data[idx + 1], &m->data[idx], (m->len - idx) * sizeof(uint32_t));
    m->data[idx] = v;
    m->len++;
    return 1;
}

static int model_delete(ModelVec *m, size_t idx, uint32_t *out)
{
    if (idx >= m->len)
        return 0;
    *out = m->data[idx];
    memmove(&m->data[idx], &m->data[idx + 1], (m->len - idx - 1) * sizeof(uint32_t));
    m->len--;
    return 1;
}

static int model_set(ModelVec *m, size_t idx, uint32_t v)
{
    if (idx >= m->len)
        return 0;
    m->data[idx] = v;
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

static int model_sync_from_text(ModelVec *model, Text *text)
{
    size_t len = 0;
    if (!text_is_valid(text))
        return 0;
    len = text_length(text);
    if (!model_reserve(model, len))
        return 0;
    for (size_t i = 0; i < len; i++)
    {
        uint32_t got = 0;
        if (text_get(text, i, &got) != SEQ_OK)
            return 0;
        model->data[i] = got;
    }
    model->len = len;
    return 1;
}

static int text_matches_model(Text *text, const ModelVec *model)
{
    if (!text_is_valid(text))
        return 0;
    if (text_length(text) != model->len)
        return 0;
    for (size_t i = 0; i < model->len; i++)
    {
        uint32_t got = 0;
        if (text_get(text, i, &got) != SEQ_OK)
            return 0;
        if (got != model->data[i])
            return 0;
    }
    return 1;
}

static uint32_t normalize_codepoint(uint32_t raw)
{
    uint32_t cp = raw % 0x110000u;
    if (cp >= 0xD800u && cp <= 0xDFFFu)
        cp = 0xFFFDu;
    return cp;
}

static uint32_t read_u32(const uint8_t *p)
{
    return ((uint32_t)p[0]) | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static int recover_after_oom(Text *text, ModelVec *model)
{
    if (text_is_valid(text))
        return model_sync_from_text(model, text);
    if (text_reset(text) != SEQ_OK)
        return 0;
    model->len = 0;
    return 1;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    Text    *text = text_new();
    ModelVec model;
    size_t   i = 0;

    model_init(&model);
    if (!text)
    {
        model_free(&model);
        return 0;
    }

    while (i < size)
    {
        uint8_t op = data[i++] % 14u;

        switch (op)
        {
        case 0: /* push_front */
        case 1: /* push_back */
            if (i + 4 <= size)
            {
                uint32_t cp = normalize_codepoint(read_u32(&data[i]));
                int rc = (op == 0) ? text_push_front(text, cp) : text_push_back(text, cp);
                i += 4;
                if (rc == SEQ_OK)
                {
                    if ((op == 0 && !model_push_front(&model, cp)) ||
                        (op == 1 && !model_push_back(&model, cp)))
                        goto out;
                }
                else if (rc == SEQ_OOM)
                {
                    if (!recover_after_oom(text, &model))
                        goto out;
                }
                else
                {
                    __builtin_trap();
                }
            }
            else
            {
                i = size;
            }
            break;
        case 2: /* pop_front */
        case 3: /* pop_back */
        {
            uint32_t got = 0;
            uint32_t exp = 0;
            int rc = (op == 2) ? text_pop_front(text, &got) : text_pop_back(text, &got);
            if (model.len == 0)
            {
                if (rc != SEQ_EMPTY)
                    __builtin_trap();
            }
            else
            {
                int ok = (op == 2) ? model_pop_front(&model, &exp) : model_pop_back(&model, &exp);
                if (rc != SEQ_OK || !ok || got != exp)
                    __builtin_trap();
            }
            break;
        }
        case 4: /* get */
        {
            uint32_t got = 0;
            if (model.len > 0 && i < size && (data[i++] & 1u))
            {
                size_t idx = (size_t)((i < size) ? data[i++] : 0) % model.len;
                if (text_get(text, idx, &got) != SEQ_OK || got != model.data[idx])
                    __builtin_trap();
            }
            else
            {
                size_t idx = model.len + ((i < size) ? (size_t)(data[i++] % 4u) : 1u);
                if (text_get(text, idx, &got) != SEQ_RANGE)
                    __builtin_trap();
            }
            break;
        }
        case 5: /* set */
            if (model.len > 0 && i + 5 <= size)
            {
                size_t idx = (size_t)data[i++] % model.len;
                uint32_t cp = normalize_codepoint(read_u32(&data[i]));
                int rc;
                i += 4;
                rc = text_set(text, idx, cp);
                if (rc == SEQ_OK)
                {
                    if (!model_set(&model, idx, cp))
                        __builtin_trap();
                }
                else if (rc == SEQ_OOM)
                {
                    if (!recover_after_oom(text, &model))
                        goto out;
                }
                else
                {
                    __builtin_trap();
                }
            }
            break;
        case 6: /* insert */
            if (i + 5 <= size)
            {
                size_t idx = (model.len == 0) ? 0u : (size_t)data[i++] % (model.len + 1u);
                uint32_t cp = normalize_codepoint(read_u32(&data[i]));
                int rc;
                i += 4;
                rc = text_insert(text, idx, cp);
                if (rc == SEQ_OK)
                {
                    if (!model_insert(&model, idx, cp))
                        __builtin_trap();
                }
                else if (rc == SEQ_OOM)
                {
                    if (!recover_after_oom(text, &model))
                        goto out;
                }
                else
                {
                    __builtin_trap();
                }
            }
            else
            {
                i = size;
            }
            break;
        case 7: /* delete */
            if (model.len > 0)
            {
                size_t idx = (size_t)((i < size) ? data[i++] : 0u) % model.len;
                uint32_t got = 0;
                uint32_t exp = 0;
                int rc = text_delete(text, idx, &got);
                if (rc == SEQ_OK)
                {
                    if (!model_delete(&model, idx, &exp) || got != exp)
                        __builtin_trap();
                }
                else if (rc == SEQ_OOM)
                {
                    if (!recover_after_oom(text, &model))
                        goto out;
                }
                else
                {
                    __builtin_trap();
                }
            }
            break;
        case 8: /* split + re-concat identity */
        {
            size_t idx = (model.len == 0) ? 0u : (size_t)((i < size) ? data[i++] : 0u) % (model.len + 1u);
            Text  *l = NULL;
            Text  *r = NULL;
            int rc = text_split_at(text, idx, &l, &r);
            if (rc == SEQ_OK)
            {
                if (!l || !r)
                    __builtin_trap();
                if (text_length(text) != 0)
                    __builtin_trap();
                if (text_concat(text, l) != SEQ_OK || text_concat(text, r) != SEQ_OK)
                    __builtin_trap();
                text_free(l);
                text_free(r);
            }
            else if (rc == SEQ_OOM)
            {
                if (!recover_after_oom(text, &model))
                    goto out;
            }
            else
            {
                __builtin_trap();
            }
            break;
        }
        case 9: /* concat random chunk */
        {
            Text    *chunk = text_new();
            ModelVec chunk_model;
            size_t count = 0;
            int rc = SEQ_OK;

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
                uint32_t cp = 0;
                if (i + 4 > size)
                    break;
                cp = normalize_codepoint(read_u32(&data[i]));
                i += 4;
                if (text_push_back(chunk, cp) != SEQ_OK || !model_push_back(&chunk_model, cp))
                {
                    text_free(chunk);
                    model_free(&chunk_model);
                    goto out;
                }
            }

            rc = text_concat(text, chunk);
            if (rc == SEQ_OK)
            {
                if (!model_concat(&model, &chunk_model))
                {
                    text_free(chunk);
                    model_free(&chunk_model);
                    goto out;
                }
            }
            else if (rc == SEQ_OOM)
            {
                if (!recover_after_oom(text, &model))
                {
                    text_free(chunk);
                    model_free(&chunk_model);
                    goto out;
                }
            }
            else
            {
                text_free(chunk);
                model_free(&chunk_model);
                __builtin_trap();
            }

            text_free(chunk);
            model_free(&chunk_model);
            break;
        }
        case 10: /* reset */
        {
            int rc = text_reset(text);
            if (rc == SEQ_OK)
            {
                model.len = 0;
            }
            else if (rc == SEQ_OOM)
            {
                if (!recover_after_oom(text, &model))
                    goto out;
            }
            else
            {
                __builtin_trap();
            }
            break;
        }
        case 11: /* UTF-8 round-trip */
        {
            size_t need = 0;
            size_t wrote = 0;
            int rc = text_utf8_length(text, &need);
            if (rc != SEQ_OK)
                __builtin_trap();
            if (need > 0)
            {
                uint8_t *buf = (uint8_t *)malloc(need);
                Text    *tmp = text_new();
                if (!buf || !tmp)
                {
                    free(buf);
                    text_free(tmp);
                    goto out;
                }
                if (text_to_utf8(text, buf, need, &wrote) != SEQ_OK || wrote != need)
                    __builtin_trap();
                if (need > 1 && text_to_utf8(text, buf, need - 1, &wrote) != SEQ_RANGE)
                    __builtin_trap();
                if (text_from_utf8(tmp, buf, need) != SEQ_OK)
                    __builtin_trap();
                if (!text_matches_model(tmp, &model))
                    __builtin_trap();
                free(buf);
                text_free(tmp);
            }
            else
            {
                if (text_to_utf8(text, NULL, 0, &wrote) != SEQ_OK || wrote != 0)
                    __builtin_trap();
            }
            break;
        }
        case 12: /* text_from_utf8 from random bytes; on invalid, preserve state */
        {
            size_t n = 0;
            const uint8_t *blob = NULL;
            ModelVec before;
            int rc;

            model_init(&before);
            if (!model_clone(&before, &model))
            {
                model_free(&before);
                goto out;
            }

            if (i < size)
                n = (size_t)(data[i++] % 16u);
            if (i + n > size)
                n = size - i;
            blob = &data[i];
            i += n;

            rc = text_from_utf8(text, blob, n);
            if (rc == SEQ_OK)
            {
                if (!model_sync_from_text(&model, text))
                {
                    model_free(&before);
                    goto out;
                }
            }
            else if (rc == SEQ_INVALID)
            {
                if (!model_equal(&model, &before))
                    __builtin_trap();
            }
            else if (rc == SEQ_OOM)
            {
                if (!model_equal(&model, &before))
                    __builtin_trap();
                if (!recover_after_oom(text, &model))
                {
                    model_free(&before);
                    goto out;
                }
            }
            else
            {
                __builtin_trap();
            }
            model_free(&before);
            break;
        }
        default: /* invalid code points are rejected */
        {
            uint32_t bad = (i < size && (data[i++] & 1u)) ? 0x110000u : 0xD800u;
            size_t before = model.len;
            int rc = text_push_back(text, bad);
            if (rc != SEQ_INVALID || model.len != before)
                __builtin_trap();
            break;
        }
        }

        if (!text_matches_model(text, &model))
            __builtin_trap();
    }

out:
    text_free(text);
    model_free(&model);
    return 0;
}
