/*
 * text_literal.c - immutable literal table for text handle resolution
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/text_literal.h"
#include "sapling/arena.h"
#include "sapling/seq.h"
#include "sapling/txn.h"

#include <stdlib.h>
#include <string.h>

/* ===== Internal Types ===== */

typedef struct {
    const uint8_t *ptr; /* pointer into arena page */
    size_t len;
} TextLiteralEntry;

/* Arena page tracking for cleanup */
typedef struct TextLiteralPage {
    struct TextLiteralPage *next;
    uint32_t pgno;        /* arena page number, or UINT32_MAX for malloc'd buffer */
    void *malloc_ptr;     /* non-NULL for malloc'd oversized buffers */
} TextLiteralPage;

struct TextLiteralTable {
    SapEnv *env;
    TextLiteralEntry *entries;     /* malloc'd growable index array */
    uint32_t count;
    uint32_t capacity;
    int sealed;

    /* Arena-backed bump allocator for UTF-8 data */
    uint8_t *page_ptr;            /* current arena page */
    uint32_t page_pgno;           /* for freeing */
    uint32_t page_head;           /* bump offset into current page */
    uint32_t page_cap;            /* page capacity (bytes) */
    TextLiteralPage *pages;       /* linked list of all allocated pages */

    /* Dedup hash table (open addressing) */
    uint32_t *hash_buckets;       /* value = entry index + 1 (0 = empty) */
    uint32_t hash_capacity;       /* power-of-two bucket count */
};

/* ===== Hash Function ===== */

/* FNV-1a 32-bit — same algorithm as hamt.c */
static uint32_t fnv1a(const uint8_t *data, size_t len)
{
    uint32_t h = 0x811C9DC5u;
    for (size_t i = 0; i < len; i++)
    {
        h ^= (uint32_t)data[i];
        h *= 0x01000193u;
    }
    return h;
}

/* ===== Hash Table Helpers ===== */

#define HASH_EMPTY 0u
#define HASH_INITIAL_CAP 16u
#define HASH_LOAD_NUM 3u    /* grow when count * 4 > capacity * 3 (75% load) */
#define HASH_LOAD_DEN 4u

static int hash_grow(TextLiteralTable *t);

static uint32_t hash_probe(const TextLiteralTable *t, const uint8_t *utf8, size_t len)
{
    if (!t->hash_buckets || t->hash_capacity == 0)
        return UINT32_MAX;

    uint32_t h = fnv1a(utf8, len);
    uint32_t mask = t->hash_capacity - 1u;
    uint32_t slot = h & mask;

    for (uint32_t i = 0; i < t->hash_capacity; i++)
    {
        uint32_t val = t->hash_buckets[slot];
        if (val == HASH_EMPTY)
            return UINT32_MAX; /* not found */

        uint32_t idx = val - 1u;
        if (idx < t->count)
        {
            const TextLiteralEntry *e = &t->entries[idx];
            if (e->len == len && (len == 0 || memcmp(e->ptr, utf8, len) == 0))
                return idx; /* found */
        }

        slot = (slot + 1u) & mask;
    }
    return UINT32_MAX;
}

static int hash_insert(TextLiteralTable *t, uint32_t entry_idx,
                       const uint8_t *utf8, size_t len)
{
    /* Grow if needed */
    if (t->hash_capacity == 0 ||
        (uint64_t)(t->count + 1u) * HASH_LOAD_DEN > (uint64_t)t->hash_capacity * HASH_LOAD_NUM)
    {
        if (hash_grow(t) != 0)
            return ERR_OOM;
    }

    uint32_t h = fnv1a(utf8, len);
    uint32_t mask = t->hash_capacity - 1u;
    uint32_t slot = h & mask;

    for (uint32_t i = 0; i < t->hash_capacity; i++)
    {
        if (t->hash_buckets[slot] == HASH_EMPTY)
        {
            t->hash_buckets[slot] = entry_idx + 1u;
            return ERR_OK;
        }
        slot = (slot + 1u) & mask;
    }
    return ERR_OOM; /* should not happen if load factor is maintained */
}

static int hash_grow(TextLiteralTable *t)
{
    uint32_t new_cap = (t->hash_capacity == 0) ? HASH_INITIAL_CAP : t->hash_capacity * 2u;
    uint32_t *new_buckets = (uint32_t *)calloc(new_cap, sizeof(uint32_t));
    if (!new_buckets)
        return ERR_OOM;

    /* Rehash all existing entries */
    uint32_t old_cap = t->hash_capacity;
    uint32_t *old_buckets = t->hash_buckets;

    t->hash_buckets = new_buckets;
    t->hash_capacity = new_cap;

    if (old_buckets)
    {
        for (uint32_t i = 0; i < old_cap; i++)
        {
            uint32_t val = old_buckets[i];
            if (val == HASH_EMPTY)
                continue;
            uint32_t idx = val - 1u;
            const TextLiteralEntry *e = &t->entries[idx];
            uint32_t h = fnv1a(e->ptr, e->len);
            uint32_t mask = new_cap - 1u;
            uint32_t slot = h & mask;
            while (new_buckets[slot] != HASH_EMPTY)
                slot = (slot + 1u) & mask;
            new_buckets[slot] = val;
        }
        free(old_buckets);
    }
    return ERR_OK;
}

/* ===== Arena Page Management ===== */

static int alloc_new_page(TextLiteralTable *t)
{
    SapMemArena *arena = sap_env_get_arena(t->env);
    void *ptr = NULL;
    uint32_t pgno = 0;

    if (sap_arena_alloc_page(arena, &ptr, &pgno) != 0)
        return ERR_OOM;

    /* Track the page for cleanup */
    TextLiteralPage *pg = (TextLiteralPage *)malloc(sizeof(TextLiteralPage));
    if (!pg)
    {
        sap_arena_free_page(arena, pgno);
        return ERR_OOM;
    }
    pg->pgno = pgno;
    pg->malloc_ptr = NULL;
    pg->next = t->pages;
    t->pages = pg;

    t->page_ptr = (uint8_t *)ptr;
    t->page_pgno = pgno;
    t->page_head = 0;
    /* page_cap is set from the env's page_size at table creation */
    return ERR_OK;
}

/* Copy bytes into arena, potentially spanning a page boundary by
 * allocating a new page if the current one doesn't have enough room.
 * If the data doesn't fit in a single page, it gets its own dedicated page
 * (the data must be contiguous for the pointer to be valid). */
static int arena_copy(TextLiteralTable *t, const uint8_t *data, size_t len,
                      const uint8_t **out)
{
    if (len == 0)
    {
        /* Empty string — use any stable non-NULL pointer */
        static const uint8_t empty = 0;
        *out = &empty;
        return ERR_OK;
    }

    /* If data is larger than a page, we need a dedicated allocation.
     * For simplicity, if len > page_cap we allocate using malloc.
     * This is an edge case — most literals are small. */
    if (len > t->page_cap)
    {
        uint8_t *buf = (uint8_t *)malloc(len);
        if (!buf)
            return ERR_OOM;
        memcpy(buf, data, len);
        *out = buf;
        /* Track the malloc'd buffer for cleanup */
        TextLiteralPage *pg = (TextLiteralPage *)malloc(sizeof(TextLiteralPage));
        if (!pg)
        {
            free(buf);
            return ERR_OOM;
        }
        pg->pgno = UINT32_MAX;
        pg->malloc_ptr = buf;
        pg->next = t->pages;
        t->pages = pg;
        return ERR_OK;
    }

    /* Check if current page has room */
    if (!t->page_ptr || t->page_head + (uint32_t)len > t->page_cap)
    {
        int rc = alloc_new_page(t);
        if (rc != ERR_OK)
            return rc;
    }

    memcpy(t->page_ptr + t->page_head, data, len);
    *out = t->page_ptr + t->page_head;
    t->page_head += (uint32_t)len;
    return ERR_OK;
}

/* ===== Public API ===== */

TextLiteralTable *text_literal_table_new(SapEnv *env)
{
    if (!env)
        return NULL;

    TextLiteralTable *t = (TextLiteralTable *)calloc(1, sizeof(TextLiteralTable));
    if (!t)
        return NULL;

    t->env = env;
    t->page_cap = sap_env_get_page_size(env);

    return t;
}

int text_literal_table_add(TextLiteralTable *table,
                           const uint8_t *utf8, size_t utf8_len,
                           uint32_t *id_out)
{
    if (!table || !id_out)
        return ERR_INVALID;
    if (!utf8 && utf8_len > 0)
        return ERR_INVALID;
    if (table->sealed)
        return ERR_INVALID;
    if (table->count > TEXT_HANDLE_PAYLOAD_MASK)
        return ERR_INVALID; /* 30-bit ID space exhausted */

    /* Dedup: check if this exact content already exists */
    uint32_t existing = hash_probe(table, utf8, utf8_len);
    if (existing != UINT32_MAX)
    {
        *id_out = existing;
        return ERR_OK;
    }

    /* Ensure entries array has capacity */
    if (table->count >= table->capacity)
    {
        uint32_t new_cap = (table->capacity == 0) ? 16u : table->capacity * 2u;
        TextLiteralEntry *new_entries = (TextLiteralEntry *)realloc(
            table->entries, new_cap * sizeof(TextLiteralEntry));
        if (!new_entries)
            return ERR_OOM;
        table->entries = new_entries;
        table->capacity = new_cap;
    }

    /* Copy data into arena */
    const uint8_t *stored = NULL;
    int rc = arena_copy(table, utf8, utf8_len, &stored);
    if (rc != ERR_OK)
        return rc;

    /* Add entry */
    uint32_t idx = table->count;
    table->entries[idx].ptr = stored;
    table->entries[idx].len = utf8_len;
    table->count++;

    /* Insert into hash table */
    rc = hash_insert(table, idx, utf8, utf8_len);
    if (rc != ERR_OK)
    {
        /* Rollback: decrement count, but the arena bytes are not reclaimed
         * (bump allocator doesn't support individual free). This is acceptable
         * since OOM is a rare/fatal condition. */
        table->count--;
        return rc;
    }

    *id_out = idx;
    return ERR_OK;
}

void text_literal_table_seal(TextLiteralTable *table)
{
    if (table)
        table->sealed = 1;
}

int text_literal_table_is_sealed(const TextLiteralTable *table)
{
    return table ? table->sealed : 0;
}

int text_literal_table_get(const TextLiteralTable *table,
                           uint32_t id,
                           const uint8_t **utf8_out,
                           size_t *utf8_len_out)
{
    if (!table || !utf8_out || !utf8_len_out)
        return ERR_INVALID;
    if (id >= table->count)
        return ERR_RANGE;

    *utf8_out = table->entries[id].ptr;
    *utf8_len_out = table->entries[id].len;
    return ERR_OK;
}

uint32_t text_literal_table_count(const TextLiteralTable *table)
{
    return table ? table->count : 0;
}

void text_literal_table_free(TextLiteralTable *table)
{
    if (!table)
        return;

    /* Free arena pages and malloc'd oversized buffers */
    SapMemArena *arena = sap_env_get_arena(table->env);
    TextLiteralPage *pg = table->pages;
    while (pg)
    {
        TextLiteralPage *next = pg->next;
        if (pg->pgno == UINT32_MAX)
        {
            free(pg->malloc_ptr);
        }
        else
        {
            sap_arena_free_page(arena, pg->pgno);
        }
        free(pg);
        pg = next;
    }

    free(table->entries);
    free(table->hash_buckets);
    free(table);
}

int text_literal_table_resolve_fn(uint32_t literal_id,
                                  const uint8_t **utf8_out,
                                  size_t *utf8_len_out,
                                  void *ctx)
{
    return text_literal_table_get((const TextLiteralTable *)ctx,
                                 literal_id, utf8_out, utf8_len_out);
}
