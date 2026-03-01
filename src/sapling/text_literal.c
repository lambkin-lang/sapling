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
#include "sapling/txn_vec.h"

#include <string.h>

#include "sapling/nomalloc.h"

/* ===== Internal Types ===== */

typedef struct {
    const uint8_t *ptr; /* pointer into arena page */
    size_t len;
} TextLiteralEntry;

/* Arena page tracking for cleanup */
typedef struct TextLiteralPage {
    struct TextLiteralPage *next;
    uint32_t pgno;              /* arena page number, or UINT32_MAX for oversized */
    uint32_t self_nodeno;       /* arena node for this tracking struct */
    void *oversized_ptr;        /* non-NULL for arena-allocated oversized buffers */
    uint32_t oversized_nodeno;  /* arena node for the oversized buffer */
    uint32_t oversized_size;    /* byte size of oversized buffer (for arena free) */
} TextLiteralPage;

struct TextLiteralTable {
    SapEnv *env;
    SapTxnVec entries;             /* arena-backed; elem_size = sizeof(TextLiteralEntry) */
    int sealed;

    /* Arena-backed bump allocator for UTF-8 data */
    uint8_t *page_ptr;            /* current arena page */
    uint32_t page_pgno;           /* for freeing */
    uint32_t page_head;           /* bump offset into current page */
    uint32_t page_cap;            /* page capacity (bytes) */
    TextLiteralPage *pages;       /* linked list of all allocated pages */

    /* Dedup hash table (open addressing, arena-allocated) */
    uint32_t *hash_buckets;       /* value = entry index + 1 (0 = empty) */
    uint32_t hash_capacity;       /* power-of-two bucket count */
    uint32_t hash_nodeno;         /* arena node for hash_buckets */

    uint32_t nodeno;              /* arena node number of this struct */
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
    uint32_t entry_count = sap_txn_vec_len(&t->entries);

    for (uint32_t i = 0; i < t->hash_capacity; i++)
    {
        uint32_t val = t->hash_buckets[slot];
        if (val == HASH_EMPTY)
            return UINT32_MAX; /* not found */

        uint32_t idx = val - 1u;
        if (idx < entry_count)
        {
            const TextLiteralEntry *e =
                (const TextLiteralEntry *)sap_txn_vec_at(&t->entries, idx);
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
    uint32_t entry_count = sap_txn_vec_len(&t->entries);
    if (t->hash_capacity == 0 ||
        (uint64_t)(entry_count + 1u) * HASH_LOAD_DEN > (uint64_t)t->hash_capacity * HASH_LOAD_NUM)
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
    uint32_t alloc_size = new_cap * (uint32_t)sizeof(uint32_t);

    SapMemArena *arena = sap_env_get_arena(t->env);
    void *new_data = NULL;
    uint32_t new_nodeno = 0;
    if (sap_arena_alloc_node(arena, alloc_size, &new_data, &new_nodeno) != ERR_OK)
        return ERR_OOM;

    uint32_t *new_buckets = (uint32_t *)new_data;
    memset(new_buckets, 0, alloc_size); /* zero = HASH_EMPTY */

    /* Rehash all existing entries */
    uint32_t old_cap = t->hash_capacity;
    uint32_t *old_buckets = t->hash_buckets;
    uint32_t old_nodeno = t->hash_nodeno;

    t->hash_buckets = new_buckets;
    t->hash_capacity = new_cap;
    t->hash_nodeno = new_nodeno;

    if (old_buckets)
    {
        for (uint32_t i = 0; i < old_cap; i++)
        {
            uint32_t val = old_buckets[i];
            if (val == HASH_EMPTY)
                continue;
            uint32_t idx = val - 1u;
            const TextLiteralEntry *e =
                (const TextLiteralEntry *)sap_txn_vec_at(&t->entries, idx);
            uint32_t h = fnv1a(e->ptr, e->len);
            uint32_t mask = new_cap - 1u;
            uint32_t slot = h & mask;
            while (new_buckets[slot] != HASH_EMPTY)
                slot = (slot + 1u) & mask;
            new_buckets[slot] = val;
        }
        sap_arena_free_node(arena, old_nodeno,
                            old_cap * (uint32_t)sizeof(uint32_t));
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

    /* Track the page for cleanup — arena-allocated tracking node */
    void *pg_mem = NULL;
    uint32_t pg_nodeno = 0;
    if (sap_arena_alloc_node(arena, (uint32_t)sizeof(TextLiteralPage),
                             &pg_mem, &pg_nodeno) != ERR_OK)
    {
        sap_arena_free_page(arena, pgno);
        return ERR_OOM;
    }
    TextLiteralPage *pg = (TextLiteralPage *)pg_mem;
    memset(pg, 0, sizeof(*pg));
    pg->pgno = pgno;
    pg->self_nodeno = pg_nodeno;
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

    /* If data is larger than a page, we need a dedicated arena node.
     * This is an edge case — most literals are small. */
    if (len > t->page_cap)
    {
        SapMemArena *arena = sap_env_get_arena(t->env);
        void *buf_mem = NULL;
        uint32_t buf_nodeno = 0;
        if (sap_arena_alloc_node(arena, (uint32_t)len, &buf_mem, &buf_nodeno) != ERR_OK)
            return ERR_OOM;
        memcpy(buf_mem, data, len);
        *out = (const uint8_t *)buf_mem;

        /* Track the oversized buffer for cleanup — arena-allocated tracking node */
        void *pg_mem = NULL;
        uint32_t pg_nodeno = 0;
        if (sap_arena_alloc_node(arena, (uint32_t)sizeof(TextLiteralPage),
                                 &pg_mem, &pg_nodeno) != ERR_OK)
        {
            sap_arena_free_node(arena, buf_nodeno, (uint32_t)len);
            return ERR_OOM;
        }
        TextLiteralPage *pg = (TextLiteralPage *)pg_mem;
        memset(pg, 0, sizeof(*pg));
        pg->pgno = UINT32_MAX;
        pg->self_nodeno = pg_nodeno;
        pg->oversized_ptr = buf_mem;
        pg->oversized_nodeno = buf_nodeno;
        pg->oversized_size = (uint32_t)len;
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

    SapMemArena *arena = sap_env_get_arena(env);
    TextLiteralTable *t = NULL;
    uint32_t nodeno = 0;
    if (sap_arena_alloc_node(arena, (uint32_t)sizeof(TextLiteralTable),
                             (void **)&t, &nodeno) != ERR_OK)
        return NULL;
    memset(t, 0, sizeof(*t));

    t->env = env;
    t->nodeno = nodeno;
    t->page_cap = sap_env_get_page_size(env);

    /* Lazy init — entries vec will allocate on first push */
    if (sap_txn_vec_init(&t->entries, arena, sizeof(TextLiteralEntry), 0) != ERR_OK)
    {
        sap_arena_free_node(arena, nodeno, (uint32_t)sizeof(TextLiteralTable));
        return NULL;
    }

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
    if (sap_txn_vec_len(&table->entries) > TEXT_HANDLE_PAYLOAD_MASK)
        return ERR_INVALID; /* 30-bit ID space exhausted */

    /* Dedup: check if this exact content already exists */
    uint32_t existing = hash_probe(table, utf8, utf8_len);
    if (existing != UINT32_MAX)
    {
        *id_out = existing;
        return ERR_OK;
    }

    /* Copy data into arena */
    const uint8_t *stored = NULL;
    int rc = arena_copy(table, utf8, utf8_len, &stored);
    if (rc != ERR_OK)
        return rc;

    /* Add entry via SapTxnVec */
    uint32_t idx = sap_txn_vec_len(&table->entries);
    TextLiteralEntry entry;
    entry.ptr = stored;
    entry.len = utf8_len;

    rc = sap_txn_vec_push(&table->entries, &entry);
    if (rc != ERR_OK)
        return rc;

    /* Insert into hash table */
    rc = hash_insert(table, idx, utf8, utf8_len);
    if (rc != ERR_OK)
    {
        /* Rollback: remove the entry we just pushed. The arena bytes are not
         * reclaimed (bump allocator doesn't support individual free). This is
         * acceptable since OOM is a rare/fatal condition. */
        sap_txn_vec_pop(&table->entries);
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
    if (id >= sap_txn_vec_len(&table->entries))
        return ERR_RANGE;

    const TextLiteralEntry *e =
        (const TextLiteralEntry *)sap_txn_vec_at(&table->entries, id);
    *utf8_out = e->ptr;
    *utf8_len_out = e->len;
    return ERR_OK;
}

uint32_t text_literal_table_count(const TextLiteralTable *table)
{
    return table ? sap_txn_vec_len(&table->entries) : 0;
}

void text_literal_table_free(TextLiteralTable *table)
{
    if (!table)
        return;

    SapMemArena *arena = sap_env_get_arena(table->env);

    /* Free arena pages and oversized buffers */
    TextLiteralPage *pg = table->pages;
    while (pg)
    {
        TextLiteralPage *next = pg->next;
        if (pg->pgno == UINT32_MAX)
        {
            /* Oversized buffer — arena node */
            sap_arena_free_node(arena, pg->oversized_nodeno, pg->oversized_size);
        }
        else
        {
            sap_arena_free_page(arena, pg->pgno);
        }
        /* Free the tracking struct itself — arena node */
        sap_arena_free_node(arena, pg->self_nodeno,
                            (uint32_t)sizeof(TextLiteralPage));
        pg = next;
    }

    /* Free entries vector, hash buckets, and table struct */
    sap_txn_vec_destroy(&table->entries);
    if (table->hash_buckets)
        sap_arena_free_node(arena, table->hash_nodeno,
                            table->hash_capacity * (uint32_t)sizeof(uint32_t));
    sap_arena_free_node(arena, table->nodeno,
                        (uint32_t)sizeof(TextLiteralTable));
}

int text_literal_table_resolve_fn(uint32_t literal_id,
                                  const uint8_t **utf8_out,
                                  size_t *utf8_len_out,
                                  void *ctx)
{
    return text_literal_table_get((const TextLiteralTable *)ctx,
                                 literal_id, utf8_out, utf8_len_out);
}
