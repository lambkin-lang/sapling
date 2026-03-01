/*
 * hamt.c -- Hash Array Mapped Trie implementation
 *
 * Persistent, copy-on-write HAMT with ref-based node addressing,
 * abort-safe allocation tracking, and iterative COW path-stack rebuilds.
 *
 * Node links are uint32_t arena nodenums (0 = null).  Branch child arrays
 * are dense, indexed via __builtin_popcount (Wasm i32.popcnt).  Memory is
 * allocated through sap_arena_alloc_node with exact per-node sizing.
 *
 * Old-node reclamation: v1 does NOT reclaim replaced nodes on commit.
 * Nodes replaced by COW remain in the arena until sap_arena_destroy.
 * This is safe for correctness but means memory grows monotonically
 * under sustained write workloads.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/hamt.h"
#include "sapling/arena.h"
#include <stdlib.h>
#include <string.h>

/* ===== Constants ===== */

#define HAMT_SUBSYSTEM_ID SAP_SUBSYSTEM_HAMT
#define HAMT_BITS_PER_LEVEL 5
#define HAMT_MASK 0x1Fu
#define HAMT_MAX_DEPTH 7 /* depths 0..6: 5*6+2 = 32 bits consumed */
#define HAMT_REF_NULL 0  /* arena pgno 0 is reserved */

enum
{
    HAMT_TAG_BRANCH = 0,
    HAMT_TAG_LEAF = 1,
    HAMT_TAG_COLLISION = 2
};

/* ===== Node Structures ===== */

typedef struct
{
    uint32_t tag;
    uint32_t bitmap;
    uint32_t child_refs[]; /* dense, length = popcount(bitmap) */
} HamtBranch;

typedef struct
{
    uint32_t tag;
    uint32_t hash;
    uint32_t key_len;
    uint32_t val_len;
    uint8_t data[]; /* key bytes then value bytes */
} HamtLeaf;

typedef struct
{
    uint32_t tag;
    uint32_t hash;
    uint32_t count;
    uint32_t leaf_refs[]; /* each is arena nodeno to a HamtLeaf */
} HamtCollision;

/* ===== Size Computation ===== */

static uint32_t branch_size(uint32_t popcount)
{
    return (uint32_t)(sizeof(HamtBranch) + popcount * sizeof(uint32_t));
}

static uint32_t leaf_size(uint32_t key_len, uint32_t val_len)
{
    return (uint32_t)(sizeof(HamtLeaf) + key_len + val_len);
}

static uint32_t collision_size(uint32_t count)
{
    return (uint32_t)(sizeof(HamtCollision) + count * sizeof(uint32_t));
}

static uint32_t node_alloc_size(const void *node)
{
    uint32_t tag = *(const uint32_t *)node;
    if (tag == HAMT_TAG_BRANCH)
    {
        const HamtBranch *br = (const HamtBranch *)node;
        return branch_size((uint32_t)__builtin_popcount(br->bitmap));
    }
    else if (tag == HAMT_TAG_LEAF)
    {
        const HamtLeaf *lf = (const HamtLeaf *)node;
        return leaf_size(lf->key_len, lf->val_len);
    }
    else
    {
        const HamtCollision *col = (const HamtCollision *)node;
        return collision_size(col->count);
    }
}

/* ===== State Structures ===== */

typedef struct
{
    SapEnv *env;
    uint32_t root_ref;
} HamtEnvState;

typedef struct HamtTxnState
{
    uint32_t root_ref;
    uint32_t saved_root;
    struct HamtTxnState *parent;
    uint32_t *new_refs;
    uint32_t new_cnt;
    uint32_t new_cap;
} HamtTxnState;

/* ===== Resolve Helper ===== */

static inline void *hamt_resolve(SapMemArena *arena, uint32_t ref)
{
    return sap_arena_resolve(arena, ref);
}

/* ===== Hash Function ===== */

/*
 * FNV-1a 32-bit.  XOR + multiply per byte, no lookup tables.
 * Compiles to i32.xor + i32.mul on Wasm.
 */
static uint32_t hamt_hash_fnv1a(const void *data, uint32_t len)
{
    const uint8_t *p = (const uint8_t *)data;
    uint32_t h = 0x811C9DC5u;
    uint32_t i;
    for (i = 0; i < len; i++)
    {
        h ^= p[i];
        h *= 0x01000193u;
    }
    return h;
}

typedef uint32_t (*hamt_hash_fn_t)(const void *key, uint32_t len);
static hamt_hash_fn_t g_hamt_hash = hamt_hash_fnv1a;

static inline uint32_t hamt_hash(const void *k, uint32_t n) { return g_hamt_hash(k, n); }

#ifdef SAPLING_HAMT_TESTING
void hamt_test_set_hash_fn(hamt_hash_fn_t fn) { g_hamt_hash = fn ? fn : hamt_hash_fnv1a; }
void hamt_test_reset_hash_fn(void) { g_hamt_hash = hamt_hash_fnv1a; }
#endif

/* ===== Bit Helpers ===== */

static inline uint32_t hash_fragment(uint32_t hash, uint32_t depth)
{
    return (hash >> (depth * HAMT_BITS_PER_LEVEL)) & HAMT_MASK;
}

static inline uint32_t bitmap_index(uint32_t bitmap, uint32_t bit_pos)
{
    return (uint32_t)__builtin_popcount(bitmap & ((1u << bit_pos) - 1));
}

/* ===== Key Comparison ===== */

static inline int leaf_key_eq(const HamtLeaf *leaf, const void *key, uint32_t key_len)
{
    if (leaf->key_len != key_len)
        return 0;
    if (key_len == 0)
        return 1;
    return memcmp(leaf->data, key, key_len) == 0;
}

/* ===== Tracked Allocation ===== */

static int hamt_track_ref(HamtTxnState *st, uint32_t ref)
{
    if (st->new_cnt == st->new_cap)
    {
        uint32_t cap = st->new_cap == 0 ? 64 : st->new_cap * 2;
        uint32_t *arr = realloc(st->new_refs, cap * sizeof(uint32_t));
        if (!arr)
            return ERR_OOM;
        st->new_refs = arr;
        st->new_cap = cap;
    }
    st->new_refs[st->new_cnt++] = ref;
    return ERR_OK;
}

static int hamt_alloc_tracked(SapMemArena *arena, HamtTxnState *st, uint32_t size, void **out,
                              uint32_t *ref_out)
{
    int rc = sap_arena_alloc_node(arena, size, out, ref_out);
    if (rc != ERR_OK)
        return rc;

    rc = hamt_track_ref(st, *ref_out);
    if (rc != ERR_OK)
    {
        sap_arena_free_node(arena, *ref_out, size);
        *out = NULL;
        *ref_out = HAMT_REF_NULL;
        return ERR_OOM;
    }
    return ERR_OK;
}

/* ===== Allocation Helpers (with overflow checks) ===== */

static int alloc_leaf(SapMemArena *arena, HamtTxnState *st, uint32_t hash, const void *key,
                      uint32_t key_len, const void *val, uint32_t val_len, uint32_t *ref_out)
{
    if (key_len > UINT32_MAX - val_len)
        return ERR_INVALID;
    uint32_t data_len = key_len + val_len;
    if (data_len > UINT32_MAX - (uint32_t)sizeof(HamtLeaf))
        return ERR_INVALID;
    uint32_t total = (uint32_t)sizeof(HamtLeaf) + data_len;

    HamtLeaf *leaf;
    int rc = hamt_alloc_tracked(arena, st, total, (void **)&leaf, ref_out);
    if (rc != ERR_OK)
        return rc;

    leaf->tag = HAMT_TAG_LEAF;
    leaf->hash = hash;
    leaf->key_len = key_len;
    leaf->val_len = val_len;
    if (key_len > 0)
        memcpy(leaf->data, key, key_len);
    if (val && val_len > 0)
        memcpy(leaf->data + key_len, val, val_len);
    return ERR_OK;
}

static int alloc_branch_raw(SapMemArena *arena, HamtTxnState *st, uint32_t bitmap,
                            const uint32_t *children, uint32_t child_count, uint32_t *ref_out)
{
    if (child_count > UINT32_MAX / (uint32_t)sizeof(uint32_t))
        return ERR_INVALID;
    uint32_t payload = child_count * (uint32_t)sizeof(uint32_t);
    if (payload > UINT32_MAX - (uint32_t)sizeof(HamtBranch))
        return ERR_INVALID;
    uint32_t total = (uint32_t)sizeof(HamtBranch) + payload;

    HamtBranch *br;
    int rc = hamt_alloc_tracked(arena, st, total, (void **)&br, ref_out);
    if (rc != ERR_OK)
        return rc;

    br->tag = HAMT_TAG_BRANCH;
    br->bitmap = bitmap;
    if (child_count > 0 && children)
        memcpy(br->child_refs, children, payload);
    return ERR_OK;
}

static int alloc_collision(SapMemArena *arena, HamtTxnState *st, uint32_t hash,
                           const uint32_t *leaf_refs, uint32_t count, uint32_t *ref_out)
{
    if (count > UINT32_MAX / (uint32_t)sizeof(uint32_t))
        return ERR_INVALID;
    uint32_t payload = count * (uint32_t)sizeof(uint32_t);
    if (payload > UINT32_MAX - (uint32_t)sizeof(HamtCollision))
        return ERR_INVALID;
    uint32_t total = (uint32_t)sizeof(HamtCollision) + payload;

    HamtCollision *col;
    int rc = hamt_alloc_tracked(arena, st, total, (void **)&col, ref_out);
    if (rc != ERR_OK)
        return rc;

    col->tag = HAMT_TAG_COLLISION;
    col->hash = hash;
    col->count = count;
    if (count > 0 && leaf_refs)
        memcpy(col->leaf_refs, leaf_refs, payload);
    return ERR_OK;
}

/* ===== Branch Primitives ===== */

/*
 * Insert child_ref at bit_pos into branch, producing a new branch.
 * bit_pos must NOT already be set in old->bitmap.
 */
static int branch_with_inserted(SapMemArena *arena, HamtTxnState *st, const HamtBranch *old,
                                uint32_t bit_pos, uint32_t child_ref, uint32_t *out_ref)
{
    uint32_t bit = 1u << bit_pos;
    uint32_t new_bitmap = old->bitmap | bit;
    uint32_t old_pop = (uint32_t)__builtin_popcount(old->bitmap);
    uint32_t new_pop = old_pop + 1;
    uint32_t idx = bitmap_index(new_bitmap, bit_pos);

    uint32_t payload = new_pop * (uint32_t)sizeof(uint32_t);
    if (payload > UINT32_MAX - (uint32_t)sizeof(HamtBranch))
        return ERR_INVALID;
    uint32_t total = (uint32_t)sizeof(HamtBranch) + payload;

    HamtBranch *br;
    int rc = hamt_alloc_tracked(arena, st, total, (void **)&br, out_ref);
    if (rc != ERR_OK)
        return rc;

    br->tag = HAMT_TAG_BRANCH;
    br->bitmap = new_bitmap;

    uint32_t i;
    for (i = 0; i < idx; i++)
        br->child_refs[i] = old->child_refs[i];
    br->child_refs[idx] = child_ref;
    for (i = idx; i < old_pop; i++)
        br->child_refs[i + 1] = old->child_refs[i];

    return ERR_OK;
}

/*
 * Replace child at bit_pos in branch, producing a new branch.
 * bit_pos must already be set in old->bitmap.
 */
static int branch_with_replaced(SapMemArena *arena, HamtTxnState *st, const HamtBranch *old,
                                uint32_t bit_pos, uint32_t child_ref, uint32_t *out_ref)
{
    uint32_t pop = (uint32_t)__builtin_popcount(old->bitmap);
    uint32_t idx = bitmap_index(old->bitmap, bit_pos);

    uint32_t total = branch_size(pop);
    HamtBranch *br;
    int rc = hamt_alloc_tracked(arena, st, total, (void **)&br, out_ref);
    if (rc != ERR_OK)
        return rc;

    br->tag = HAMT_TAG_BRANCH;
    br->bitmap = old->bitmap;
    memcpy(br->child_refs, old->child_refs, pop * sizeof(uint32_t));
    br->child_refs[idx] = child_ref;

    return ERR_OK;
}

/*
 * Remove child at bit_pos from branch.
 * If pop == 1, sets *collapsed = 1 and out_ref = HAMT_REF_NULL.
 * If pop == 2 and the survivor is a LEAF or COLLISION, collapses:
 *   sets *collapsed = 1 and out_ref = the surviving child ref.
 * If pop == 2 but the survivor is a BRANCH, does NOT collapse
 *   (collapsing would remove a hash-fragment level, breaking depth
 *   alignment for descendants).  Allocates a 1-child branch instead.
 * Otherwise allocates a new shrunken branch.
 */
static int branch_with_removed(SapMemArena *arena, HamtTxnState *st, const HamtBranch *old,
                               uint32_t bit_pos, uint32_t *out_ref, int *collapsed)
{
    uint32_t bit = 1u << bit_pos;
    uint32_t pop = (uint32_t)__builtin_popcount(old->bitmap);
    uint32_t idx = bitmap_index(old->bitmap, bit_pos);

    *collapsed = 0;

    if (pop == 1)
    {
        *collapsed = 1;
        *out_ref = HAMT_REF_NULL;
        return ERR_OK;
    }

    if (pop == 2)
    {
        uint32_t survivor_ref = old->child_refs[1 - idx];
        void *survivor = hamt_resolve(arena, survivor_ref);
        if (!survivor)
            return ERR_CORRUPT;
        uint32_t survivor_tag = *(uint32_t *)survivor;

        if (survivor_tag != HAMT_TAG_BRANCH)
        {
            /* Safe to collapse: survivor is a leaf or collision */
            *collapsed = 1;
            *out_ref = survivor_ref;
            return ERR_OK;
        }

        /* Survivor is a branch: cannot collapse without breaking depth
         * alignment.  Create a 1-child branch instead. */
        uint32_t new_bitmap = old->bitmap & ~bit;
        return alloc_branch_raw(arena, st, new_bitmap, &survivor_ref, 1, out_ref);
    }

    uint32_t new_bitmap = old->bitmap & ~bit;
    uint32_t new_pop = pop - 1;
    uint32_t total = branch_size(new_pop);

    HamtBranch *br;
    int rc = hamt_alloc_tracked(arena, st, total, (void **)&br, out_ref);
    if (rc != ERR_OK)
        return rc;

    br->tag = HAMT_TAG_BRANCH;
    br->bitmap = new_bitmap;

    uint32_t i, j = 0;
    for (i = 0; i < pop; i++)
    {
        if (i != idx)
            br->child_refs[j++] = old->child_refs[i];
    }

    return ERR_OK;
}

/* ===== Transaction Callbacks ===== */

static int on_begin(SapTxnCtx *txn, void *parent_state, void **state_out)
{
    HamtTxnState *st = (HamtTxnState *)sap_txn_scratch_alloc(txn, (uint32_t)sizeof(HamtTxnState));
    if (!st)
        return ERR_OOM;
    memset(st, 0, sizeof(*st));

    HamtEnvState *env_st =
        (HamtEnvState *)sap_env_subsystem_state(sap_txn_env(txn), HAMT_SUBSYSTEM_ID);

    if (!parent_state)
    {
        st->parent = NULL;
        st->root_ref = env_st->root_ref;
    }
    else
    {
        st->parent = (HamtTxnState *)parent_state;
        st->root_ref = st->parent->root_ref;
    }
    st->saved_root = st->root_ref;

    *state_out = st;
    return ERR_OK;
}

static int on_commit(SapTxnCtx *txn, void *state)
{
    HamtTxnState *st = (HamtTxnState *)state;
    if (!st)
        return ERR_OK;

    if (st->parent)
    {
        /* Nested commit: atomically merge child allocations into parent.
         * Pre-reserve capacity, then memcpy, to avoid partial merge. */
        if (st->new_cnt > 0)
        {
            uint32_t needed = st->parent->new_cnt + st->new_cnt;
            if (needed < st->parent->new_cnt)
                return ERR_OOM; /* overflow */

            if (needed > st->parent->new_cap)
            {
                uint32_t cap = st->parent->new_cap;
                while (cap < needed)
                    cap = cap == 0 ? 64 : cap * 2;
                uint32_t *arr = realloc(st->parent->new_refs, cap * sizeof(uint32_t));
                if (!arr)
                    return ERR_OOM;
                st->parent->new_refs = arr;
                st->parent->new_cap = cap;
            }
            memcpy(st->parent->new_refs + st->parent->new_cnt, st->new_refs,
                   st->new_cnt * sizeof(uint32_t));
            st->parent->new_cnt = needed;
        }
        st->parent->root_ref = st->root_ref;
    }
    else
    {
        HamtEnvState *env_st =
            (HamtEnvState *)sap_env_subsystem_state(sap_txn_env(txn), HAMT_SUBSYSTEM_ID);
        env_st->root_ref = st->root_ref;
    }

    free(st->new_refs);
    return ERR_OK;
}

static void on_abort(SapTxnCtx *txn, void *state)
{
    HamtTxnState *st = (HamtTxnState *)state;
    if (!st)
        return;
    SapMemArena *arena = sap_txn_arena(txn);

    for (uint32_t i = 0; i < st->new_cnt; i++)
    {
        void *node = sap_arena_resolve(arena, st->new_refs[i]);
        if (node)
        {
            uint32_t sz = node_alloc_size(node);
            sap_arena_free_node(arena, st->new_refs[i], sz);
        }
    }
    free(st->new_refs);
}

static void on_env_destroy(void *env_state) { free(env_state); }

/* ===== Subsystem Init ===== */

int sap_hamt_subsystem_init(SapEnv *env)
{
    SapTxnSubsystemCallbacks callbacks = {
        .on_begin = on_begin,
        .on_commit = on_commit,
        .on_abort = on_abort,
        .on_env_destroy = on_env_destroy,
    };

    if (!env)
        return ERR_INVALID;

    HamtEnvState *state = (HamtEnvState *)malloc(sizeof(HamtEnvState));
    if (!state)
        return ERR_OOM;

    state->env = env;
    state->root_ref = HAMT_REF_NULL;

    int rc = sap_env_register_subsystem(env, HAMT_SUBSYSTEM_ID, &callbacks);
    if (rc != ERR_OK)
    {
        free(state);
        return rc;
    }

    rc = sap_env_set_subsystem_state(env, HAMT_SUBSYSTEM_ID, state);
    if (rc != ERR_OK)
    {
        free(state);
        return rc;
    }

    return ERR_OK;
}

/* ===== Build Branch Chain ===== */

/*
 * Build a chain of branches from depth downward until hash_a and hash_b
 * fragments diverge, then create a two-child branch.
 * Both child_a and child_b are already-allocated node refs.
 */
static int make_branch_pair(SapMemArena *arena, HamtTxnState *st, uint32_t depth, uint32_t hash_a,
                            uint32_t ref_a, uint32_t hash_b, uint32_t ref_b, uint32_t *out_ref)
{
    uint32_t frag_a = hash_fragment(hash_a, depth);
    uint32_t frag_b = hash_fragment(hash_b, depth);

    if (frag_a != frag_b)
    {
        /* Fragments differ: create two-child branch */
        uint32_t bit_a = 1u << frag_a;
        uint32_t bit_b = 1u << frag_b;
        uint32_t bitmap = bit_a | bit_b;
        uint32_t children[2];

        if (frag_a < frag_b)
        {
            children[0] = ref_a;
            children[1] = ref_b;
        }
        else
        {
            children[0] = ref_b;
            children[1] = ref_a;
        }
        return alloc_branch_raw(arena, st, bitmap, children, 2, out_ref);
    }

    /* Fragments match: recurse deeper */
    if (depth + 1 >= HAMT_MAX_DEPTH)
    {
        /* All 32 bits consumed but hashes differ — impossible for truly
         * different hashes.  Defensive: treat as corruption. */
        return ERR_CORRUPT;
    }

    uint32_t sub_ref;
    int rc = make_branch_pair(arena, st, depth + 1, hash_a, ref_a, hash_b, ref_b, &sub_ref);
    if (rc != ERR_OK)
        return rc;

    /* Wrap in single-child branch at current depth */
    uint32_t bit = 1u << frag_a;
    return alloc_branch_raw(arena, st, bit, &sub_ref, 1, out_ref);
}

/* ===== Put ===== */

int sap_hamt_put(SapTxnCtx *txn, const void *key, uint32_t key_len, const void *val,
                 uint32_t val_len, unsigned flags)
{
    if (!txn)
        return ERR_INVALID;
    if (sap_txn_flags(txn) & TXN_RDONLY)
        return ERR_READONLY;
    if (flags & ~(unsigned)SAP_NOOVERWRITE)
        return ERR_INVALID;
    if (key_len > 0 && !key)
        return ERR_INVALID;
    if (val_len > 0 && !val)
        return ERR_INVALID;

    HamtTxnState *st = (HamtTxnState *)sap_txn_subsystem_state(txn, HAMT_SUBSYSTEM_ID);
    if (!st)
        return ERR_INVALID;

    SapMemArena *arena = sap_txn_arena(txn);
    if (!arena)
        return ERR_INVALID;

    uint32_t hash = hamt_hash(key, key_len);

    /* Path stack for iterative descent */
    uint32_t path_refs[HAMT_MAX_DEPTH];
    uint32_t path_frags[HAMT_MAX_DEPTH];
    int depth = 0;
    int rc;

    /* Descend through branches */
    uint32_t cur = st->root_ref;
    while (cur != HAMT_REF_NULL)
    {
        void *node = hamt_resolve(arena, cur);
        uint32_t tag = *(uint32_t *)node;

        if (tag != HAMT_TAG_BRANCH)
            break;
        if (depth >= HAMT_MAX_DEPTH)
            break;

        HamtBranch *br = (HamtBranch *)node;
        uint32_t frag = hash_fragment(hash, (uint32_t)depth);
        uint32_t bit = 1u << frag;

        if (!(br->bitmap & bit))
        {
            /* Empty slot: record this branch so bottom-up can insert */
            path_refs[depth] = cur;
            path_frags[depth] = frag;
            depth++;
            cur = HAMT_REF_NULL;
            break;
        }

        path_refs[depth] = cur;
        path_frags[depth] = frag;
        uint32_t idx = bitmap_index(br->bitmap, frag);
        cur = br->child_refs[idx];
        depth++;
    }

    uint32_t new_child_ref;

    if (cur == HAMT_REF_NULL)
    {
        /* Empty tree or empty branch slot: insert leaf */
        rc = alloc_leaf(arena, st, hash, key, key_len, val, val_len, &new_child_ref);
        if (rc != ERR_OK)
            return rc;

        /* If we stopped at a branch with empty slot, insert into it */
        if (depth > 0)
        {
            HamtBranch *br = (HamtBranch *)hamt_resolve(arena, path_refs[depth - 1]);
            uint32_t frag = hash_fragment(hash, (uint32_t)(depth - 1));

            /* Check if we stopped because bitmap didn't have this bit */
            uint32_t bit = 1u << frag;
            if (!(br->bitmap & bit))
            {
                /* Insert into this branch instead of replacing */
                uint32_t new_br_ref;
                rc = branch_with_inserted(arena, st, br, frag, new_child_ref, &new_br_ref);
                if (rc != ERR_OK)
                    return rc;
                new_child_ref = new_br_ref;
                depth--; /* consumed this path entry */
            }
        }
    }
    else
    {
        void *node = hamt_resolve(arena, cur);
        uint32_t tag = *(uint32_t *)node;

        if (tag == HAMT_TAG_LEAF)
        {
            HamtLeaf *existing = (HamtLeaf *)node;

            if (existing->hash == hash && leaf_key_eq(existing, key, key_len))
            {
                /* Same key: replace or reject */
                if (flags & SAP_NOOVERWRITE)
                    return ERR_EXISTS;
                rc = alloc_leaf(arena, st, hash, key, key_len, val, val_len, &new_child_ref);
                if (rc != ERR_OK)
                    return rc;
            }
            else if (existing->hash != hash)
            {
                /* Different hash: build branch chain */
                uint32_t new_leaf_ref;
                rc = alloc_leaf(arena, st, hash, key, key_len, val, val_len, &new_leaf_ref);
                if (rc != ERR_OK)
                    return rc;
                rc = make_branch_pair(arena, st, (uint32_t)depth, existing->hash, cur, hash,
                                      new_leaf_ref, &new_child_ref);
                if (rc != ERR_OK)
                    return rc;
            }
            else
            {
                /* Same hash, different key: create collision */
                uint32_t new_leaf_ref;
                rc = alloc_leaf(arena, st, hash, key, key_len, val, val_len, &new_leaf_ref);
                if (rc != ERR_OK)
                    return rc;
                uint32_t entries[2] = {cur, new_leaf_ref};
                rc = alloc_collision(arena, st, hash, entries, 2, &new_child_ref);
                if (rc != ERR_OK)
                    return rc;
            }
        }
        else if (tag == HAMT_TAG_COLLISION)
        {
            HamtCollision *col = (HamtCollision *)node;
            uint32_t i;

            /* Check for existing key in collision entries */
            for (i = 0; i < col->count; i++)
            {
                HamtLeaf *entry = (HamtLeaf *)hamt_resolve(arena, col->leaf_refs[i]);
                if (leaf_key_eq(entry, key, key_len))
                {
                    if (flags & SAP_NOOVERWRITE)
                        return ERR_EXISTS;
                    /* Replace this entry */
                    uint32_t new_leaf_ref;
                    rc = alloc_leaf(arena, st, hash, key, key_len, val, val_len, &new_leaf_ref);
                    if (rc != ERR_OK)
                        return rc;
                    rc = alloc_collision(arena, st, hash, col->leaf_refs, col->count,
                                         &new_child_ref);
                    if (rc != ERR_OK)
                        return rc;
                    HamtCollision *new_col = (HamtCollision *)hamt_resolve(arena, new_child_ref);
                    new_col->leaf_refs[i] = new_leaf_ref;
                    goto bottom_up;
                }
            }

            /* Append new entry to collision */
            uint32_t new_leaf_ref;
            rc = alloc_leaf(arena, st, hash, key, key_len, val, val_len, &new_leaf_ref);
            if (rc != ERR_OK)
                return rc;
            rc = alloc_collision(arena, st, hash, NULL, col->count + 1, &new_child_ref);
            if (rc != ERR_OK)
                return rc;
            HamtCollision *new_col = (HamtCollision *)hamt_resolve(arena, new_child_ref);
            memcpy(new_col->leaf_refs, col->leaf_refs, col->count * sizeof(uint32_t));
            new_col->leaf_refs[col->count] = new_leaf_ref;
        }
        else
        {
            /* Branch reached at terminal position — shouldn't happen
             * since the descent loop would have continued */
            return ERR_CORRUPT;
        }
    }

bottom_up:
    /* Bottom-up rebuild: walk path from deepest to root */
    for (int d = depth - 1; d >= 0; d--)
    {
        HamtBranch *br = (HamtBranch *)hamt_resolve(arena, path_refs[d]);
        uint32_t new_br_ref;
        rc = branch_with_replaced(arena, st, br, path_frags[d], new_child_ref, &new_br_ref);
        if (rc != ERR_OK)
            return rc;
        new_child_ref = new_br_ref;
    }

    st->root_ref = new_child_ref;
    return ERR_OK;
}

/* ===== Get ===== */

int sap_hamt_get(SapTxnCtx *txn, const void *key, uint32_t key_len, const void **val_out,
                 uint32_t *val_len_out)
{
    if (!txn)
        return ERR_INVALID;
    if (key_len > 0 && !key)
        return ERR_INVALID;

    HamtTxnState *st = (HamtTxnState *)sap_txn_subsystem_state(txn, HAMT_SUBSYSTEM_ID);
    if (!st || st->root_ref == HAMT_REF_NULL)
        return ERR_NOT_FOUND;

    SapMemArena *arena = sap_txn_arena(txn);
    if (!arena)
        return ERR_INVALID;

    uint32_t hash = hamt_hash(key, key_len);
    uint32_t cur = st->root_ref;
    uint32_t depth = 0;

    while (cur != HAMT_REF_NULL)
    {
        void *node = hamt_resolve(arena, cur);
        uint32_t tag = *(uint32_t *)node;

        if (tag == HAMT_TAG_LEAF)
        {
            HamtLeaf *leaf = (HamtLeaf *)node;
            /* Short-circuit: hash first, then length, then memcmp */
            if (leaf->hash != hash)
                return ERR_NOT_FOUND;
            if (!leaf_key_eq(leaf, key, key_len))
                return ERR_NOT_FOUND;
            if (val_out)
                *val_out = leaf->data + leaf->key_len;
            if (val_len_out)
                *val_len_out = leaf->val_len;
            return ERR_OK;
        }

        if (tag == HAMT_TAG_BRANCH)
        {
            if (depth >= HAMT_MAX_DEPTH)
                return ERR_CORRUPT;
            HamtBranch *br = (HamtBranch *)node;
            uint32_t frag = hash_fragment(hash, depth);
            uint32_t bit = 1u << frag;
            if (!(br->bitmap & bit))
                return ERR_NOT_FOUND;
            uint32_t idx = bitmap_index(br->bitmap, frag);
            cur = br->child_refs[idx];
            depth++;
            continue;
        }

        if (tag == HAMT_TAG_COLLISION)
        {
            HamtCollision *col = (HamtCollision *)node;
            uint32_t i;
            for (i = 0; i < col->count; i++)
            {
                HamtLeaf *entry = (HamtLeaf *)hamt_resolve(arena, col->leaf_refs[i]);
                if (leaf_key_eq(entry, key, key_len))
                {
                    if (val_out)
                        *val_out = entry->data + entry->key_len;
                    if (val_len_out)
                        *val_len_out = entry->val_len;
                    return ERR_OK;
                }
            }
            return ERR_NOT_FOUND;
        }

        return ERR_CORRUPT;
    }

    return ERR_NOT_FOUND;
}

/* ===== Delete ===== */

int sap_hamt_del(SapTxnCtx *txn, const void *key, uint32_t key_len)
{
    if (!txn)
        return ERR_INVALID;
    if (sap_txn_flags(txn) & TXN_RDONLY)
        return ERR_READONLY;
    if (key_len > 0 && !key)
        return ERR_INVALID;

    HamtTxnState *st = (HamtTxnState *)sap_txn_subsystem_state(txn, HAMT_SUBSYSTEM_ID);
    if (!st)
        return ERR_INVALID;
    if (st->root_ref == HAMT_REF_NULL)
        return ERR_NOT_FOUND;

    SapMemArena *arena = sap_txn_arena(txn);
    if (!arena)
        return ERR_INVALID;

    uint32_t hash = hamt_hash(key, key_len);

    /* Path stack for iterative descent */
    uint32_t path_refs[HAMT_MAX_DEPTH];
    uint32_t path_frags[HAMT_MAX_DEPTH];
    int depth = 0;
    int rc;

    /* Descend through branches */
    uint32_t cur = st->root_ref;
    while (cur != HAMT_REF_NULL)
    {
        void *node = hamt_resolve(arena, cur);
        uint32_t tag = *(uint32_t *)node;

        if (tag != HAMT_TAG_BRANCH)
            break;
        if (depth >= HAMT_MAX_DEPTH)
            break;

        HamtBranch *br = (HamtBranch *)node;
        uint32_t frag = hash_fragment(hash, (uint32_t)depth);
        uint32_t bit = 1u << frag;

        if (!(br->bitmap & bit))
            return ERR_NOT_FOUND;

        path_refs[depth] = cur;
        path_frags[depth] = frag;
        uint32_t idx = bitmap_index(br->bitmap, frag);
        cur = br->child_refs[idx];
        depth++;
    }

    if (cur == HAMT_REF_NULL)
        return ERR_NOT_FOUND;

    uint32_t new_child_ref;

    void *node = hamt_resolve(arena, cur);
    uint32_t tag = *(uint32_t *)node;

    if (tag == HAMT_TAG_LEAF)
    {
        HamtLeaf *leaf = (HamtLeaf *)node;
        if (leaf->hash != hash || !leaf_key_eq(leaf, key, key_len))
            return ERR_NOT_FOUND;
        new_child_ref = HAMT_REF_NULL;
    }
    else if (tag == HAMT_TAG_COLLISION)
    {
        HamtCollision *col = (HamtCollision *)node;
        uint32_t found_idx = UINT32_MAX;
        uint32_t i;

        for (i = 0; i < col->count; i++)
        {
            HamtLeaf *entry = (HamtLeaf *)hamt_resolve(arena, col->leaf_refs[i]);
            if (leaf_key_eq(entry, key, key_len))
            {
                found_idx = i;
                break;
            }
        }

        if (found_idx == UINT32_MAX)
            return ERR_NOT_FOUND;

        if (col->count == 2)
        {
            /* Collapse collision to single leaf */
            new_child_ref = col->leaf_refs[1 - found_idx];
        }
        else
        {
            /* Shrink collision */
            rc = alloc_collision(arena, st, hash, NULL, col->count - 1, &new_child_ref);
            if (rc != ERR_OK)
                return rc;
            HamtCollision *new_col = (HamtCollision *)hamt_resolve(arena, new_child_ref);
            uint32_t j = 0;
            for (i = 0; i < col->count; i++)
            {
                if (i != found_idx)
                    new_col->leaf_refs[j++] = col->leaf_refs[i];
            }
        }
    }
    else
    {
        return ERR_CORRUPT;
    }

    /* Bottom-up rebuild.
     * Use new_child_ref == HAMT_REF_NULL to decide remove-vs-replace:
     * NULL means the subtree is empty and the parent slot should be removed;
     * non-NULL means the child was replaced (possibly by a collapse survivor)
     * and the parent slot should be updated to point to it. */
    for (int d = depth - 1; d >= 0; d--)
    {
        HamtBranch *br = (HamtBranch *)hamt_resolve(arena, path_refs[d]);

        if (new_child_ref == HAMT_REF_NULL)
        {
            int collapsed = 0;
            rc = branch_with_removed(arena, st, br, path_frags[d], &new_child_ref, &collapsed);
            if (rc != ERR_OK)
                return rc;
            /* If collapsed with a non-null survivor, next iteration
             * will take the replace path automatically. */
        }
        else
        {
            uint32_t new_br_ref;
            rc = branch_with_replaced(arena, st, br, path_frags[d], new_child_ref, &new_br_ref);
            if (rc != ERR_OK)
                return rc;
            new_child_ref = new_br_ref;
        }
    }

    st->root_ref = new_child_ref;
    return ERR_OK;
}
