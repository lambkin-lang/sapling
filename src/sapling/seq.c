/*
 * seq.c — finger-tree sequence implementation
 *
 * Design
 * ------
 * A finger tree is a recursive, balanced data structure parameterised by a
 * "measure."  For a sequence the measure is size (leaf-element count).
 *
 * Three node shapes at each level:
 *   Empty   – no elements
 *   Single  – exactly one element
 *   Deep    – a prefix digit (1–4 items), a recursive middle finger tree
 *             whose items are 2–3-ary internal nodes, and a suffix digit
 *             (1–4 items).
 *
 * "Depth" parameter
 * -----------------
 * Every internal function carries an `item_depth` (or `child_depth`) value:
 *   item_depth == 0  →  items stored here are user uint32_t handles
 *                       (measure = 1 each)
 *   item_depth >  0  →  items are SeqNode* whose measure is node->size
 *
 * The middle tree of a depth-d tree has item_depth d+1.
 *
 * Memory ownership
 * ----------------
 * Every FTree* and SeqNode* has exactly one owner (no shared structure).
 * Operations that "consume" a tree transfer ownership of all contained nodes
 * to the result; the consumed FTree shell is freed immediately.
 * Handle payloads are values; no per-element payload is freed by this library.
 * On OOM from concat/split, sequence handles may become invalid.
 *
 * Thread safety: none.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/seq.h"

#include "sapling/txn.h"
#include "sapling/arena.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/* ================================================================== */
/* Internal types                                                       */
/* ================================================================== */

typedef enum
{
    FTREE_EMPTY = 0,
    FTREE_SINGLE,
    FTREE_DEEP
} FTreeTag;

typedef uintptr_t SeqItem;

struct SeqNode;
typedef struct SeqNode SeqNode;

static inline SeqItem seq_item_from_node(SeqNode *node) { return (SeqItem)(uintptr_t)node; }

static inline SeqNode *seq_item_as_node(SeqItem item) { return (SeqNode *)(uintptr_t)item; }

static inline SeqItem seq_item_from_handle(uint32_t handle) { return (SeqItem)handle; }

static inline uint32_t seq_item_to_handle(SeqItem item) { return (uint32_t)item; }

/*
 * FTree — a finger tree node.
 *
 * When tag == FTREE_DEEP the prefix and suffix arrays are stored inline,
 * avoiding separate heap allocations for digit objects.
 */
typedef struct FTree
{
    FTreeTag tag;
    size_t size; /* total leaf elements in this subtree */
    union
    {
        SeqItem single; /* FTREE_SINGLE: one item (u32 handle or SeqNode*) */
        struct
        {
            int pr_count; /* 1–4 */
            SeqItem pr[4];
            size_t pr_size;
            struct FTree *mid; /* middle tree, item_depth + 1 */
            int sf_count;      /* 1–4 */
            SeqItem sf[4];
            size_t sf_size;
        } deep;
    };
} FTree;

/*
 * SeqNode — a 2-ary or 3-ary internal node.
 * child[i] are items at depth (node_depth - 1): u32 handles or SeqNode*.
 */
struct SeqNode
{
    size_t size;      /* total leaf elements beneath */
    int arity;        /* 2 or 3 */
    SeqItem child[3]; /* children */
};

/* Public handle */
struct Seq
{
    FTree *root;
    int valid;
};

/*
 * SmallItems — a temporary array of up to 4 items used during split.
 */
typedef struct
{
    SeqItem elems[4];
    int count;
    size_t size;
} SmallItems;

/*
 * SplitResult — returned by ftree_split_exact.
 * On SEQ_OK, elem is the item at item_depth that contains the target leaf.
 */
typedef struct
{
    int rc; /* SEQ_OK / SEQ_OOM */
    FTree *left;
    SeqItem elem;
    FTree *right;
} SplitResult;

/* ================================================================== */
/* Forward declarations                                                 */
/* ================================================================== */

static int ftree_push_front(FTree **tree, SeqItem item, int item_depth,
                            SapTxnCtx *txn);
static int ftree_push_back(FTree **tree, SeqItem item, int item_depth,
                           SapTxnCtx *txn);
static SeqItem ftree_pop_front(FTree **tree, int item_depth, SapTxnCtx *txn);
static SeqItem ftree_pop_back(FTree **tree, int item_depth, SapTxnCtx *txn);
static SeqItem ftree_get(const FTree *t, size_t idx, int item_depth);
static FTree *app3(FTree *t1, SeqItem *ts, int ts_count, FTree *t2, int item_depth,
                   SapTxnCtx *txn);
static void seq_node_free(SeqNode *node, int child_depth, SapTxnCtx *txn);
static SplitResult ftree_split_exact(FTree *tree, size_t idx, int item_depth,
                                     SapTxnCtx *txn);

/* ================================================================== */
/* Helpers                                                              */
/* ================================================================== */

typedef struct {
    Seq *seq;
    FTree *old_root;
} ShadowedSeq;

struct SeqTxnState {
    SapTxnCtx *sap_txn;
    void **new_nodes;
    uint32_t new_cnt;
    uint32_t new_cap;
    void **old_nodes;
    uint32_t old_cnt;
    uint32_t old_cap;
    ShadowedSeq *shadows;
    uint32_t shadow_cnt;
    uint32_t shadow_cap;
};

static int seq_on_begin(SapTxnCtx *txn, void *parent_state, void **state_out)
{
    (void)parent_state;
    struct SeqTxnState *st = calloc(1, sizeof(struct SeqTxnState));
    if (!st) return SAP_ERROR;
    st->sap_txn = txn;
    /* Pre-size arrays to reduce allocations */
    st->new_cap = 64;
    st->new_nodes = malloc(st->new_cap * sizeof(void *));
    st->old_cap = 64;
    st->old_nodes = malloc(st->old_cap * sizeof(void *));
    st->shadow_cap = 16;
    st->shadows = malloc(st->shadow_cap * sizeof(ShadowedSeq));
    if (!st->new_nodes || !st->old_nodes || !st->shadows) {
        free(st->new_nodes); free(st->old_nodes); free(st->shadows); free(st);
        return SAP_ERROR;
    }
    *state_out = st;
    return SAP_OK;
}

static int seq_on_commit(SapTxnCtx *txn, void *state)
{
    struct SeqTxnState *st = state;
    if (st) {
        SapMemArena *arena = sap_txn_arena(txn);
        for (uint32_t i = 0; i < st->old_cnt; i++) {
            sap_arena_free_node_ptr(arena, st->old_nodes[i], 0);
        }
        free(st->new_nodes);
        free(st->old_nodes);
        free(st->shadows);
        free(st);
    }
    return SAP_OK;
}

static void seq_on_abort(SapTxnCtx *txn, void *state)
{
    struct SeqTxnState *st = state;
    if (st) {
        SapMemArena *arena = sap_txn_arena(txn);
        for (uint32_t i = 0; i < st->new_cnt; i++) {
            sap_arena_free_node_ptr(arena, st->new_nodes[i], 0);
        }
        for (uint32_t i = 0; i < st->shadow_cnt; i++) {
            st->shadows[i].seq->root = st->shadows[i].old_root;
        }
        free(st->new_nodes);
        free(st->old_nodes);
        free(st->shadows);
        free(st);
    }
}

static const SapTxnSubsystemCallbacks seq_subsystem_cbs = {
    .on_begin = seq_on_begin,
    .on_commit = seq_on_commit,
    .on_abort = seq_on_abort
};

int sap_seq_subsystem_init(SapEnv *env)
{
    return sap_env_register_subsystem(env, SAP_SUBSYSTEM_SEQ, &seq_subsystem_cbs);
}

#ifdef SAPLING_SEQ_TESTING
static int64_t g_alloc_fail_after = -1;

void seq_test_fail_alloc_after(int64_t n) { g_alloc_fail_after = n; }

void seq_test_clear_alloc_fail(void) { g_alloc_fail_after = -1; }
#endif

static void *seq_alloc_node(SapTxnCtx *txn, size_t bytes)
{
    if (!txn) return NULL;
    SapMemArena *arena = sap_txn_arena(txn);
    void *node_out = NULL;
    uint32_t nodeno = 0;
#ifdef SAPLING_SEQ_TESTING
    if (g_alloc_fail_after >= 0)
    {
        if (g_alloc_fail_after == 0)
            return NULL;
        g_alloc_fail_after--;
    }
#endif
    if (sap_arena_alloc_node(arena, (uint32_t)bytes, &node_out, &nodeno) != SAP_OK)
        return NULL;
        
    struct SeqTxnState *st = sap_txn_subsystem_state(txn, SAP_SUBSYSTEM_SEQ);
    if (st) {
        if (st->new_cnt == st->new_cap) {
            uint32_t cap = st->new_cap * 2;
            void **arr = realloc(st->new_nodes, cap * sizeof(void *));
            if (!arr) { sap_arena_free_node_ptr(arena, node_out, 0); return NULL; }
            st->new_nodes = arr;
            st->new_cap = cap;
        }
        st->new_nodes[st->new_cnt++] = node_out;
    }
    return node_out;
}

static void seq_dealloc_node(SapTxnCtx *txn, void *ptr)
{
    if (!txn || !ptr) return;
    struct SeqTxnState *st = sap_txn_subsystem_state(txn, SAP_SUBSYSTEM_SEQ);
    if (st) {
        for (uint32_t i = 0; i < st->new_cnt; i++) {
            if (st->new_nodes[i] == ptr) {
                st->new_nodes[i] = st->new_nodes[st->new_cnt - 1];
                st->new_cnt--;
                sap_arena_free_node_ptr(sap_txn_arena(txn), ptr, 0);
                return;
            }
        }
        if (st->old_cnt == st->old_cap) {
            uint32_t cap = st->old_cap * 2;
            void **arr = realloc(st->old_nodes, cap * sizeof(void *));
            if (arr) {
                st->old_nodes = arr;
                st->old_cap = cap;
            } else {
                /* Exceeded arrays, warn and drop it inline: not COW safe */
                sap_arena_free_node_ptr(sap_txn_arena(txn), ptr, 0);
                return;
            }
        }
        st->old_nodes[st->old_cnt++] = ptr;
    } else {
        sap_arena_free_node_ptr(sap_txn_arena(txn), ptr, 0);
    }
}

static int seq_prepare_root(SapTxnCtx *txn, Seq *s) {
    if (!txn || !s) return SEQ_INVALID;
    struct SeqTxnState *st = sap_txn_subsystem_state(txn, SAP_SUBSYSTEM_SEQ);
    if (!st) return SEQ_INVALID;

    /* Check if already shadowed in this txn */
    for (uint32_t i = 0; i < st->shadow_cnt; i++) {
        if (st->shadows[i].seq == s) return SEQ_OK;
    }

    if (st->shadow_cnt == st->shadow_cap) {
        uint32_t cap = st->shadow_cap * 2;
        ShadowedSeq *arr = realloc(st->shadows, cap * sizeof(ShadowedSeq));
        if (!arr) return SEQ_OOM;
        st->shadows = arr;
        st->shadow_cap = cap;
    }

    st->shadows[st->shadow_cnt].seq = s;
    st->shadows[st->shadow_cnt].old_root = s->root;
    st->shadow_cnt++;
    return SEQ_OK;
}

static int is_node_new(SapTxnCtx *txn, void *ptr) {
    if (!txn || !ptr) return 0;
    struct SeqTxnState *st = sap_txn_subsystem_state(txn, SAP_SUBSYSTEM_SEQ);
    if (!st) return 0;
    for (uint32_t i = 0; i < st->new_cnt; i++) {
        if (st->new_nodes[i] == ptr) return 1;
    }
    return 0;
}

static int ftree_ensure_writable(SapTxnCtx *txn, FTree **tp) {
    if (!txn || !tp || !*tp) return SEQ_INVALID;
    if (is_node_new(txn, *tp)) return SEQ_OK;

    FTree *new_t = seq_alloc_node(txn, sizeof(FTree));
    if (!new_t) return SEQ_OOM;
    memcpy(new_t, *tp, sizeof(FTree));
    *tp = new_t;
    return SEQ_OK;
}


/* Checked size_t additions for measure accumulation. */
static int size_add_checked(size_t a, size_t b, size_t *out)
{
    if (SIZE_MAX - a < b)
        return 0;
    *out = a + b;
    return 1;
}

static int size_add3_checked(size_t a, size_t b, size_t c, size_t *out)
{
    size_t tmp = 0;
    if (!size_add_checked(a, b, &tmp))
        return 0;
    return size_add_checked(tmp, c, out);
}

/* Return the leaf measure of one item at the given depth. */
static inline size_t item_measure(SeqItem item, int depth)
{
    if (depth == 0)
        return 1;
    return seq_item_as_node(item)->size;
}

/*
 * Release an item that was never linked into a tree due to OOM.
 * Leaf handles (depth 0) are value types and require no cleanup.
 */
static void item_release_unconsumed(SeqItem item, int item_depth, SapTxnCtx *txn)
{
    if (item_depth > 0)
        seq_node_free(seq_item_as_node(item), item_depth - 1, txn);
}

/* Allocate an empty FTree shell. */
static FTree *ftree_new(SapTxnCtx *txn)
{
    FTree *t = seq_alloc_node(txn, sizeof(FTree));
    if (!t)
        return NULL;
    t->tag = FTREE_EMPTY;
    t->size = 0;
    return t;
}

/* Allocate a 2-ary internal node whose children are at child_depth. */
static SeqNode *node_new2(SeqItem a, SeqItem b, int child_depth, SapTxnCtx *txn)
{
    SeqNode *n = seq_alloc_node(txn, sizeof(SeqNode));
    if (!n)
        return NULL;
    size_t asz = item_measure(a, child_depth);
    size_t bsz = item_measure(b, child_depth);
    if (!size_add_checked(asz, bsz, &n->size))
    {
        seq_dealloc_node(txn, n);
        return NULL;
    }
    n->arity = 2;
    n->child[0] = a;
    n->child[1] = b;
    n->child[2] = 0;
    return n;
}

/* Allocate a 3-ary internal node whose children are at child_depth. */
static SeqNode *node_new3(SeqItem a, SeqItem b, SeqItem c, int child_depth,
                          SapTxnCtx *txn)
{
    SeqNode *n = seq_alloc_node(txn, sizeof(SeqNode));
    if (!n)
        return NULL;
    size_t asz = item_measure(a, child_depth);
    size_t bsz = item_measure(b, child_depth);
    size_t csz = item_measure(c, child_depth);
    if (!size_add3_checked(asz, bsz, csz, &n->size))
    {
        seq_dealloc_node(txn, n);
        return NULL;
    }
    n->arity = 3;
    n->child[0] = a;
    n->child[1] = b;
    n->child[2] = c;
    return n;
}

/*
 * pack_nodes — pack items[0..count-1] (at child_depth) into 2-ary/3-ary
 * nodes.  count must be in [2, 12].  Fills out[]; returns node count.
 *
 * Packing strategy: greedily emit Node3 while count > 4, then handle the
 * tail (2, 3, or 4 items) as one or two nodes, ensuring no remainder of 1.
 */
static int pack_nodes(SeqItem *items, int count, int child_depth, SeqItem *out,
                      SapTxnCtx *txn)
{
    int n = 0;
    while (count > 4)
    {
        SeqNode *node = node_new3(items[0], items[1], items[2], child_depth, txn);
        if (!node)
            goto oom;
        out[n++] = seq_item_from_node(node);
        items += 3;
        count -= 3;
    }
    switch (count)
    {
    case 2:
    {
        SeqNode *node = node_new2(items[0], items[1], child_depth, txn);
        if (!node)
            goto oom;
        out[n++] = seq_item_from_node(node);
        break;
    }
    case 3:
    {
        SeqNode *node = node_new3(items[0], items[1], items[2], child_depth, txn);
        if (!node)
            goto oom;
        out[n++] = seq_item_from_node(node);
        break;
    }
    case 4:
    {
        SeqNode *a = node_new2(items[0], items[1], child_depth, txn);
        SeqNode *b = node_new2(items[2], items[3], child_depth, txn);
        if (!a || !b)
        {
            if (a)
                seq_node_free(a, child_depth, txn);
            if (b)
                seq_node_free(b, child_depth, txn);
            goto oom;
        }
        out[n++] = seq_item_from_node(a);
        out[n++] = seq_item_from_node(b);
        break;
    }
    default:
        break; /* unreachable */
    }
    return n;

oom:
    for (int i = 0; i < n; i++)
        seq_node_free(seq_item_as_node(out[i]), child_depth, txn);
    return -1;
}

/* ================================================================== */
/* Free                                                                 */
/* ================================================================== */

/*
 * seq_node_free — recursively free a SeqNode and all descendant SeqNodes.
 * child_depth: depth of this node's children (0 = u32 handle, do not free).
 */
static void seq_node_free(SeqNode *node, int child_depth, SapTxnCtx *txn)
{
    if (child_depth > 0)
    {
        for (int i = 0; i < node->arity; i++)
            seq_node_free(seq_item_as_node(node->child[i]), child_depth - 1, txn);
    }
    seq_dealloc_node(txn, node);
}

/*
 * ftree_free — recursively free a FTree and all contained SeqNodes.
 * item_depth: depth of items stored in this tree.
 */
static void ftree_free(FTree *t, int item_depth, SapTxnCtx *txn)
{
    if (!t)
        return;
    switch (t->tag)
    {
    case FTREE_EMPTY:
        break;
    case FTREE_SINGLE:
        if (item_depth > 0)
            seq_node_free(seq_item_as_node(t->single), item_depth - 1, txn);
        break;
    case FTREE_DEEP:
        if (item_depth > 0)
        {
            for (int i = 0; i < t->deep.pr_count; i++)
                seq_node_free(seq_item_as_node(t->deep.pr[i]), item_depth - 1, txn);
            for (int i = 0; i < t->deep.sf_count; i++)
                seq_node_free(seq_item_as_node(t->deep.sf[i]), item_depth - 1, txn);
        }
        ftree_free(t->deep.mid, item_depth + 1, txn);
        break;
    }
    seq_dealloc_node(txn, t);
}

/* ================================================================== */
/* Push front / back                                                    */
/* ================================================================== */

/*
 * ftree_push_front — prepend item (at item_depth) to tree, in place.
 * Returns SEQ_OK or SEQ_OOM.  On SEQ_OOM the tree may be partially
 * modified; callers should treat the Seq as invalid.
 */
static int ftree_push_front(FTree **tp, SeqItem item, int item_depth,
                            SapTxnCtx *txn)
{
    int rc = ftree_ensure_writable(txn, tp);
    if (rc != SEQ_OK) {
        item_release_unconsumed(item, item_depth, txn);
        return rc;
    }
    FTree *tree = *tp;
    size_t sz = item_measure(item, item_depth);
    switch (tree->tag)
    {
    case FTREE_EMPTY:
        tree->tag = FTREE_SINGLE;
        tree->size = sz;
        tree->single = item;
        return SEQ_OK;

    case FTREE_SINGLE:
    {
        SeqItem b = tree->single;
        size_t bsz = item_measure(b, item_depth);
        size_t total_size = 0;
        if (!size_add_checked(bsz, sz, &total_size))
        {
            item_release_unconsumed(item, item_depth, txn);
            return SEQ_OOM;
        }
        FTree *mid = ftree_new(txn);
        if (!mid)
        {
            item_release_unconsumed(item, item_depth, txn);
            return SEQ_OOM;
        }
        tree->tag = FTREE_DEEP;
        tree->size = total_size;
        tree->deep.pr_count = 1;
        tree->deep.pr[0] = item;
        tree->deep.pr_size = sz;
        tree->deep.mid = mid;
        tree->deep.sf_count = 1;
        tree->deep.sf[0] = b;
        tree->deep.sf_size = bsz;
        return SEQ_OK;
    }

    case FTREE_DEEP:
    {
        size_t new_tree_size = 0;
        if (!size_add_checked(tree->size, sz, &new_tree_size))
        {
            item_release_unconsumed(item, item_depth, txn);
            return SEQ_OOM;
        }
        if (tree->deep.pr_count < 4)
        {
            size_t new_pr_size = 0;
            if (!size_add_checked(tree->deep.pr_size, sz, &new_pr_size))
            {
                item_release_unconsumed(item, item_depth, txn);
                return SEQ_OOM;
            }
            /* Shift prefix right and insert at front */
            int n = tree->deep.pr_count;
            for (int i = n; i > 0; i--)
                tree->deep.pr[i] = tree->deep.pr[i - 1];
            tree->deep.pr[0] = item;
            tree->deep.pr_count++;
            tree->deep.pr_size = new_pr_size;
            tree->size = new_tree_size;
            return SEQ_OK;
        }
        /*
         * Prefix is full (4 items).  Pack pr[1..3] into a Node3, retain
         * pr[0] in the prefix alongside the new item.
         */
        {
            SeqNode *node = node_new3(tree->deep.pr[1], tree->deep.pr[2], tree->deep.pr[3],
                                      item_depth, txn);
            if (!node)
            {
                item_release_unconsumed(item, item_depth, txn);
                return SEQ_OOM;
            }
            SeqItem old_front = tree->deep.pr[0];
            size_t old_front_sz = item_measure(old_front, item_depth);
            size_t new_pr_size = 0;
            if (!size_add_checked(sz, old_front_sz, &new_pr_size))
            {
                seq_node_free(node, item_depth, txn);
                item_release_unconsumed(item, item_depth, txn);
                return SEQ_OOM;
            }
            tree->deep.pr[0] = item;
            tree->deep.pr[1] = old_front;
            tree->deep.pr_count = 2;
            tree->deep.pr_size = new_pr_size;
            tree->size = new_tree_size;
            return ftree_push_front(&tree->deep.mid, seq_item_from_node(node), item_depth + 1,
                                    txn);
        }
    }
    }
    return SEQ_OOM; /* unreachable */
}

/*
 * ftree_push_back — append item (at item_depth) to tree, in place.
 */
static int ftree_push_back(FTree **tp, SeqItem item, int item_depth, SapTxnCtx *txn)
{
    int rc = ftree_ensure_writable(txn, tp);
    if (rc != SEQ_OK) {
        item_release_unconsumed(item, item_depth, txn);
        return rc;
    }
    FTree *tree = *tp;
    size_t sz = item_measure(item, item_depth);
    switch (tree->tag)
    {
    case FTREE_EMPTY:
        tree->tag = FTREE_SINGLE;
        tree->size = sz;
        tree->single = item;
        return SEQ_OK;

    case FTREE_SINGLE:
    {
        SeqItem b = tree->single;
        size_t bsz = item_measure(b, item_depth);
        size_t total_size = 0;
        if (!size_add_checked(bsz, sz, &total_size))
        {
            item_release_unconsumed(item, item_depth, txn);
            return SEQ_OOM;
        }
        FTree *mid = ftree_new(txn);
        if (!mid)
        {
            item_release_unconsumed(item, item_depth, txn);
            return SEQ_OOM;
        }
        tree->tag = FTREE_DEEP;
        tree->size = total_size;
        tree->deep.pr_count = 1;
        tree->deep.pr[0] = b;
        tree->deep.pr_size = bsz;
        tree->deep.mid = mid;
        tree->deep.sf_count = 1;
        tree->deep.sf[0] = item;
        tree->deep.sf_size = sz;
        return SEQ_OK;
    }

    case FTREE_DEEP:
    {
        size_t new_tree_size = 0;
        if (!size_add_checked(tree->size, sz, &new_tree_size))
        {
            item_release_unconsumed(item, item_depth, txn);
            return SEQ_OOM;
        }
        if (tree->deep.sf_count < 4)
        {
            size_t new_sf_size = 0;
            if (!size_add_checked(tree->deep.sf_size, sz, &new_sf_size))
            {
                item_release_unconsumed(item, item_depth, txn);
                return SEQ_OOM;
            }
            tree->deep.sf[tree->deep.sf_count] = item;
            tree->deep.sf_count++;
            tree->deep.sf_size = new_sf_size;
            tree->size = new_tree_size;
            return SEQ_OK;
        }
        /*
         * Suffix is full.  Pack sf[0..2] into a Node3, retain sf[3] in
         * the suffix alongside the new item.
         */
        {
            SeqNode *node = node_new3(tree->deep.sf[0], tree->deep.sf[1], tree->deep.sf[2],
                                      item_depth, txn);
            if (!node)
            {
                item_release_unconsumed(item, item_depth, txn);
                return SEQ_OOM;
            }
            SeqItem old_last = tree->deep.sf[3];
            size_t old_last_sz = item_measure(old_last, item_depth);
            size_t new_sf_size = 0;
            if (!size_add_checked(old_last_sz, sz, &new_sf_size))
            {
                seq_node_free(node, item_depth, txn);
                item_release_unconsumed(item, item_depth, txn);
                return SEQ_OOM;
            }
            tree->deep.sf[0] = old_last;
            tree->deep.sf[1] = item;
            tree->deep.sf_count = 2;
            tree->deep.sf_size = new_sf_size;
            tree->size = new_tree_size;
            return ftree_push_back(&tree->deep.mid, seq_item_from_node(node), item_depth + 1,
                                   txn);
        }
    }
    }
    return SEQ_OOM; /* unreachable */
}

/* ================================================================== */
/* Pop front / back                                                     */
/* ================================================================== */

/*
 * ftree_pop_front — remove and return the first item (at item_depth).
 * Pre: tree is non-empty.  Modifies tree in place.
 */
static SeqItem ftree_pop_front(FTree **tp, int item_depth, SapTxnCtx *txn)
{
    if (ftree_ensure_writable(txn, tp) != SEQ_OK) return 0;
    FTree *tree = *tp;
    assert(tree->tag != FTREE_EMPTY);

    if (tree->tag == FTREE_SINGLE)
    {
        SeqItem item = tree->single;
        tree->tag = FTREE_EMPTY;
        tree->size = 0;
        return item;
    }

    /* FTREE_DEEP */
    assert(tree->deep.pr_count >= 1 && tree->deep.pr_count <= 4);
    assert(tree->deep.sf_count >= 1 && tree->deep.sf_count <= 4);
    SeqItem item = tree->deep.pr[0];
    size_t item_sz = item_measure(item, item_depth);
    tree->size -= item_sz;

    if (tree->deep.pr_count > 1)
    {
        /* Just shift prefix left */
        tree->deep.pr_count--;
        tree->deep.pr_size -= item_sz;
        for (int i = 0; i < tree->deep.pr_count; i++)
            tree->deep.pr[i] = tree->deep.pr[i + 1];
        return item;
    }

    /* Prefix is now empty — replenish from middle or suffix */
    if (tree->deep.mid->tag == FTREE_EMPTY)
    {
        /* No middle: promote from suffix */
        if (tree->deep.sf_count == 1)
        {
            SeqItem sf0 = tree->deep.sf[0];
            seq_dealloc_node(txn, tree->deep.mid);
            tree->tag = FTREE_SINGLE;
            tree->single = sf0;
        }
        else
        {
            /* Move sf[0] to become the new single-item prefix */
            SeqItem sf0 = tree->deep.sf[0];
            size_t sf0_sz = item_measure(sf0, item_depth);
            tree->deep.pr[0] = sf0;
            tree->deep.pr_count = 1;
            tree->deep.pr_size = sf0_sz;
            tree->deep.sf_count--;
            tree->deep.sf_size -= sf0_sz;
            for (int i = 0; i < tree->deep.sf_count; i++)
                tree->deep.sf[i] = tree->deep.sf[i + 1];
        }
    }
    else
    {
        /* Pop a node from middle; its children become the new prefix */
        SeqNode *node =
            seq_item_as_node(ftree_pop_front(&tree->deep.mid, item_depth + 1, txn));
        if (!node) return 0; /* OOM in recursion */
        tree->deep.pr_count = node->arity;
        tree->deep.pr_size = node->size;
        for (int i = 0; i < node->arity; i++)
            tree->deep.pr[i] = node->child[i];
        seq_dealloc_node(txn, node);
    }

    return item;
}

/*
 * ftree_pop_back — remove and return the last item (at item_depth).
 * Pre: tree is non-empty.  Modifies tree in place.
 */
static SeqItem ftree_pop_back(FTree **tp, int item_depth, SapTxnCtx *txn)
{
    if (ftree_ensure_writable(txn, tp) != SEQ_OK) return 0;
    FTree *tree = *tp;
    assert(tree->tag != FTREE_EMPTY);

    if (tree->tag == FTREE_SINGLE)
    {
        SeqItem item = tree->single;
        tree->tag = FTREE_EMPTY;
        tree->size = 0;
        return item;
    }

    /* FTREE_DEEP */
    assert(tree->deep.pr_count >= 1 && tree->deep.pr_count <= 4);
    assert(tree->deep.sf_count >= 1 && tree->deep.sf_count <= 4);
    SeqItem item = tree->deep.sf[tree->deep.sf_count - 1];
    size_t item_sz = item_measure(item, item_depth);
    tree->size -= item_sz;

    if (tree->deep.sf_count > 1)
    {
        tree->deep.sf_count--;
        tree->deep.sf_size -= item_sz;
        return item;
    }

    /* Suffix is now empty — replenish from middle or prefix */
    if (tree->deep.mid->tag == FTREE_EMPTY)
    {
        if (tree->deep.pr_count == 1)
        {
            SeqItem pr0 = tree->deep.pr[0];
            seq_dealloc_node(txn, tree->deep.mid);
            tree->tag = FTREE_SINGLE;
            tree->single = pr0;
        }
        else
        {
            /* Move pr[last] to become the new single-item suffix */
            int last = tree->deep.pr_count - 1;
            SeqItem pr_last = tree->deep.pr[last];
            size_t pr_last_sz = item_measure(pr_last, item_depth);
            tree->deep.sf[0] = pr_last;
            tree->deep.sf_count = 1;
            tree->deep.sf_size = pr_last_sz;
            tree->deep.pr_count--;
            tree->deep.pr_size -= pr_last_sz;
        }
    }
    else
    {
        /* Pop a node from back of middle; its children become the new suffix */
        SeqNode *node = seq_item_as_node(ftree_pop_back(&tree->deep.mid, item_depth + 1, txn));
        if (!node) return 0; /* OOM in recursion */
        tree->deep.sf_count = node->arity;
        tree->deep.sf_size = node->size;
        for (int i = 0; i < node->arity; i++)
            tree->deep.sf[i] = node->child[i];
        seq_dealloc_node(txn, node);
    }

    return item;
}

/* ================================================================== */
/* Indexing (get)                                                       */
/* ================================================================== */

/* Drill into item (at item_depth) to find the leaf at offset idx. */
static SeqItem item_get(SeqItem item, size_t idx, int item_depth)
{
    if (item_depth == 0)
        return item; /* idx must be 0; item is the leaf */

    SeqNode *node = seq_item_as_node(item);
    size_t off = 0;
    for (int i = 0; i < node->arity; i++)
    {
        size_t sz = item_measure(node->child[i], item_depth - 1);
        size_t next_off = 0;
        if (!size_add_checked(off, sz, &next_off))
            return 0;
        if (idx < next_off)
            return item_get(node->child[i], idx - off, item_depth - 1);
        off = next_off;
    }
    return 0; /* unreachable for valid idx */
}

/* Return the leaf at index idx within a digit (array of items at item_depth). */
static SeqItem digit_get(const SeqItem *elems, int count, size_t idx, int item_depth)
{
    size_t off = 0;
    for (int i = 0; i < count; i++)
    {
        size_t sz = item_measure(elems[i], item_depth);
        size_t next_off = 0;
        if (!size_add_checked(off, sz, &next_off))
            return 0;
        if (idx < next_off)
            return item_get(elems[i], idx - off, item_depth);
        off = next_off;
    }
    return 0; /* unreachable for valid idx */
}

static SeqItem ftree_get(const FTree *t, size_t idx, int item_depth)
{
    switch (t->tag)
    {
    case FTREE_EMPTY:
        return 0;
    case FTREE_SINGLE:
        return item_get(t->single, idx, item_depth);
    case FTREE_DEEP:
        if (idx < t->deep.pr_size)
            return digit_get(t->deep.pr, t->deep.pr_count, idx, item_depth);
        idx -= t->deep.pr_size;
        if (idx < t->deep.mid->size)
            return ftree_get(t->deep.mid, idx, item_depth + 1);
        idx -= t->deep.mid->size;
        return digit_get(t->deep.sf, t->deep.sf_count, idx, item_depth);
    }
    return 0; /* unreachable */
}

/* ================================================================== */
/* Concatenation                                                        */
/* ================================================================== */

/*
 * small_items_to_tree — build a FTree at item_depth from 0–4 items.
 * Ownership of the items transfers to the new tree.
 */
static FTree *small_items_to_tree(SeqItem *items, int count, int item_depth,
                                  SapTxnCtx *txn)
{
    FTree *t = ftree_new(txn);
    if (!t)
        return NULL;
    for (int i = 0; i < count; i++)
    {
        int rc = ftree_push_back(&t, items[i], item_depth, txn);
        if (rc != SEQ_OK)
        {
            ftree_free(t, item_depth, txn);
            return NULL;
        }
    }
    return t;
}

/*
 * deep_l_items — build a FTree at item_depth from a (possibly empty)
 * prefix array, an existing middle tree, and a suffix array.
 * When pr is empty, borrow the first node from mid (or use sf directly
 * if mid is also empty).
 * Ownership of mid, the prefix items, and the suffix items transfers in.
 */
static FTree *deep_l_items(SeqItem *pr, int pr_count, FTree *mid, SeqItem *sf, int sf_count,
                           size_t sf_size, int item_depth, SapTxnCtx *txn)
{
    if (pr_count > 0)
    {
        FTree *t = ftree_new(txn);
        if (!t)
            return NULL;
        t->tag = FTREE_DEEP;
        t->deep.pr_count = pr_count;
        t->deep.pr_size = 0;
        for (int i = 0; i < pr_count; i++)
        {
            t->deep.pr[i] = pr[i];
            size_t next_pr_size = 0;
            if (!size_add_checked(t->deep.pr_size, item_measure(pr[i], item_depth), &next_pr_size))
            {
                seq_dealloc_node(txn, t);
                return NULL;
            }
            t->deep.pr_size = next_pr_size;
        }
        t->deep.mid = mid;
        t->deep.sf_count = sf_count;
        for (int i = 0; i < sf_count; i++)
            t->deep.sf[i] = sf[i];
        t->deep.sf_size = sf_size;
        if (!size_add3_checked(t->deep.pr_size, mid->size, sf_size, &t->size))
        {
            seq_dealloc_node(txn, t);
            return NULL;
        }
        return t;
    }

    /* Empty prefix: borrow from mid or fall back to suffix */
    if (mid->tag == FTREE_EMPTY)
    {
        seq_dealloc_node(txn, mid);
        return small_items_to_tree(sf, sf_count, item_depth, txn);
    }

    /* Pop a node from front of mid; expand it into the new prefix */
    SeqNode *node = seq_item_as_node(ftree_pop_front(&mid, item_depth + 1, txn));
    FTree *t = ftree_new(txn);
    if (!t)
    {
        seq_node_free(node, item_depth, txn);
        return NULL;
    }
    size_t total_size = 0;
    if (!size_add3_checked(node->size, mid->size, sf_size, &total_size))
    {
        seq_dealloc_node(txn, t);
        seq_node_free(node, item_depth, txn);
        return NULL;
    }
    t->tag = FTREE_DEEP;
    t->deep.pr_count = node->arity;
    t->deep.pr_size = node->size;
    for (int i = 0; i < node->arity; i++)
        t->deep.pr[i] = node->child[i];
    t->deep.mid = mid;
    t->deep.sf_count = sf_count;
    for (int i = 0; i < sf_count; i++)
        t->deep.sf[i] = sf[i];
    t->deep.sf_size = sf_size;
    t->size = total_size;
    seq_dealloc_node(txn, node);
    return t;
}

/*
 * deep_r_items — symmetric to deep_l_items: build from prefix + mid +
 * (possibly empty) suffix.  Borrows from back of mid if suffix is empty.
 */
static FTree *deep_r_items(SeqItem *pr, int pr_count, size_t pr_size, FTree *mid, SeqItem *sf,
                           int sf_count, int item_depth, SapTxnCtx *txn)
{
    if (sf_count > 0)
    {
        FTree *t = ftree_new(txn);
        if (!t)
            return NULL;
        t->tag = FTREE_DEEP;
        t->deep.pr_count = pr_count;
        for (int i = 0; i < pr_count; i++)
            t->deep.pr[i] = pr[i];
        t->deep.pr_size = pr_size;
        t->deep.mid = mid;
        t->deep.sf_count = sf_count;
        t->deep.sf_size = 0;
        for (int i = 0; i < sf_count; i++)
        {
            t->deep.sf[i] = sf[i];
            size_t next_sf_size = 0;
            if (!size_add_checked(t->deep.sf_size, item_measure(sf[i], item_depth), &next_sf_size))
            {
                seq_dealloc_node(txn, t);
                return NULL;
            }
            t->deep.sf_size = next_sf_size;
        }
        if (!size_add3_checked(pr_size, mid->size, t->deep.sf_size, &t->size))
        {
            seq_dealloc_node(txn, t);
            return NULL;
        }
        return t;
    }

    /* Empty suffix: borrow from mid or fall back to prefix */
    if (mid->tag == FTREE_EMPTY)
    {
        seq_dealloc_node(txn, mid);
        return small_items_to_tree(pr, pr_count, item_depth, txn);
    }

    SeqNode *node = seq_item_as_node(ftree_pop_back(&mid, item_depth + 1, txn));
    FTree *t = ftree_new(txn);
    if (!t)
    {
        seq_node_free(node, item_depth, txn);
        return NULL;
    }
    size_t total_size = 0;
    if (!size_add3_checked(pr_size, mid->size, node->size, &total_size))
    {
        seq_dealloc_node(txn, t);
        seq_node_free(node, item_depth, txn);
        return NULL;
    }
    t->tag = FTREE_DEEP;
    t->deep.pr_count = pr_count;
    for (int i = 0; i < pr_count; i++)
        t->deep.pr[i] = pr[i];
    t->deep.pr_size = pr_size;
    t->deep.mid = mid;
    t->deep.sf_count = node->arity;
    t->deep.sf_size = node->size;
    for (int i = 0; i < node->arity; i++)
        t->deep.sf[i] = node->child[i];
    t->size = total_size;
    seq_dealloc_node(txn, node);
    return t;
}

/*
 * app3 — destructive three-way merge used for O(log n) concatenation.
 *
 * Merges t1 ++ ts ++ t2 at item_depth, consuming both t1 and t2.
 * ts (ts_count items at item_depth) is the "spine accumulator" that
 * carries the middle elements across recursive calls; it is always NULL /
 * zero at the top-level call from seq_concat.
 *
 * Returns the merged tree (either t1's shell reused, or t2's shell),
 * or NULL on OOM.
 */
static FTree *app3(FTree *t1, SeqItem *ts, int ts_count, FTree *t2, int item_depth,
                   SapTxnCtx *txn)
{
    int rc;

    if (t1->tag == FTREE_EMPTY)
    {
        seq_dealloc_node(txn, t1);
        for (int i = ts_count - 1; i >= 0; i--)
        {
            rc = ftree_push_front(&t2, ts[i], item_depth, txn);
            if (rc != SEQ_OK)
                return NULL;
        }
        return t2;
    }

    if (t2->tag == FTREE_EMPTY)
    {
        seq_dealloc_node(txn, t2);
        for (int i = 0; i < ts_count; i++)
        {
            rc = ftree_push_back(&t1, ts[i], item_depth, txn);
            if (rc != SEQ_OK)
                return NULL;
        }
        return t1;
    }

    if (t1->tag == FTREE_SINGLE)
    {
        SeqItem x = t1->single;
        seq_dealloc_node(txn, t1);
        for (int i = ts_count - 1; i >= 0; i--)
        {
            rc = ftree_push_front(&t2, ts[i], item_depth, txn);
            if (rc != SEQ_OK)
                return NULL;
        }
        rc = ftree_push_front(&t2, x, item_depth, txn);
        if (rc != SEQ_OK)
            return NULL;
        return t2;
    }

    if (t2->tag == FTREE_SINGLE)
    {
        SeqItem y = t2->single;
        seq_dealloc_node(txn, t2);
        for (int i = 0; i < ts_count; i++)
        {
            rc = ftree_push_back(&t1, ts[i], item_depth, txn);
            if (rc != SEQ_OK)
                return NULL;
        }
        rc = ftree_push_back(&t1, y, item_depth, txn);
        if (rc != SEQ_OK)
            return NULL;
        return t1;
    }

    /* Both DEEP: combine sf1 ++ ts ++ pr2, pack into nodes, recurse */
    SeqItem combined[12]; /* max: sf(4) + ts(4) + pr(4) = 12 */
    int cc = 0;
    for (int i = 0; i < t1->deep.sf_count; i++)
        combined[cc++] = t1->deep.sf[i];
    for (int i = 0; i < ts_count; i++)
        combined[cc++] = ts[i];
    for (int i = 0; i < t2->deep.pr_count; i++)
        combined[cc++] = t2->deep.pr[i];

    SeqItem nodes[6]; /* 12 items → at most 4 nodes (3+3+3+3), use 6 for safety */
    int nc = pack_nodes(combined, cc, item_depth, nodes, txn);
    if (nc < 0)
        return NULL;

    /* Recursively merge the two middle trees with the new node spine */
    FTree *new_mid = app3(t1->deep.mid, nodes, nc, t2->deep.mid, item_depth + 1, txn);
    if (!new_mid)
        return NULL;

    /* To modify t1, ensure it is writable */
    if (ftree_ensure_writable(txn, &t1) != SEQ_OK) {
        ftree_free(new_mid, item_depth + 1, txn);
        return NULL;
    }

    size_t merged_size = 0;
    if (!size_add3_checked(t1->deep.pr_size, new_mid->size, t2->deep.sf_size, &merged_size))
    {
        ftree_free(new_mid, item_depth + 1, txn);
        return NULL;
    }

    /* Reuse t1's shell; update middle and steal t2's suffix */
    t1->deep.mid = new_mid;
    t1->deep.sf_count = t2->deep.sf_count;
    for (int i = 0; i < t2->deep.sf_count; i++)
        t1->deep.sf[i] = t2->deep.sf[i];
    t1->deep.sf_size = t2->deep.sf_size;
    t1->size = merged_size;

    seq_dealloc_node(txn, t2);
    return t1;
}

/* ================================================================== */
/* Split                                                                */
/* ================================================================== */

/*
 * split_digit_at — find the item in items[0..count-1] (at item_depth)
 * whose leaf range contains leaf index idx.
 *
 * Fills left_out with items before the found item, right_out with items
 * after it.  Returns the found item.
 */
static SeqItem split_digit_at(const SeqItem *items, int count, int capacity, size_t idx,
                              int item_depth, SmallItems *left_out, SmallItems *right_out)
{
    assert(items);
    assert(left_out);
    assert(right_out);
    assert(count >= 1);
    assert(count <= capacity);
    assert(capacity >= 1 && capacity <= 4);

    size_t off = 0;
    for (int i = 0; i < count; i++)
    {
        size_t sz = item_measure(items[i], item_depth);
        size_t next_off = 0;
        if (!size_add_checked(off, sz, &next_off))
            return 0;
        if (idx < next_off)
        {
            left_out->count = i;
            left_out->size = off;
            for (int j = 0; j < i; j++)
                left_out->elems[j] = items[j];

            right_out->count = count - i - 1;
            right_out->size = 0;
            for (int j = i + 1; j < count; j++)
            {
                size_t next_right_size = 0;
                right_out->elems[j - i - 1] = items[j];
                if (!size_add_checked(right_out->size, item_measure(items[j], item_depth),
                                      &next_right_size))
                    return 0;
                right_out->size = next_right_size;
            }
            return items[i];
        }
        off = next_off;
    }
    return 0; /* unreachable for valid idx */
}

/*
 * ftree_split_exact — destructively split tree at leaf index idx.
 *
 * Returns SplitResult { left, elem, right } where:
 *   left  — FTree at item_depth containing all items before the found one
 *   elem  — the item at item_depth whose leaf range contains idx
 *   right — FTree at item_depth containing all items after elem
 *   rc    — SEQ_OK on success, SEQ_OOM on allocation failure
 *
 * On SEQ_OK, the original tree is consumed (its shell is freed).
 * On SEQ_OOM, the tree may be partially consumed.
 * Pre: tree is non-empty and idx < tree->size.
 */
static SplitResult ftree_split_exact(FTree *tree, size_t idx, int item_depth,
                                     SapTxnCtx *txn)
{
    SplitResult res = {SEQ_OOM, NULL, 0, NULL};
    assert(tree->tag != FTREE_EMPTY);

    if (tree->tag == FTREE_SINGLE)
    {
        res.left = ftree_new(txn);
        res.right = ftree_new(txn);
        if (!res.left || !res.right)
        {
            ftree_free(res.left, item_depth, txn);
            ftree_free(res.right, item_depth, txn);
            res.left = NULL;
            res.right = NULL;
            return res;
        }
        res.rc = SEQ_OK;
        res.elem = tree->single;
        seq_dealloc_node(txn, tree);
        return res;
    }

    /* FTREE_DEEP */

    if (idx < tree->deep.pr_size)
    {
        /* Target is in the prefix */
        SmallItems left_si = {0};
        SmallItems right_si = {0};
        SeqItem found = split_digit_at(tree->deep.pr, tree->deep.pr_count, 4, idx, item_depth,
                                       &left_si, &right_si);

        res.left = small_items_to_tree(left_si.elems, left_si.count, item_depth, txn);
        res.right = deep_l_items(right_si.elems, right_si.count, tree->deep.mid, tree->deep.sf,
                                 tree->deep.sf_count, tree->deep.sf_size, item_depth, txn);
        if (!res.left || !res.right)
        {
            ftree_free(res.left, item_depth, txn);
            ftree_free(res.right, item_depth, txn);
            res.left = NULL;
            res.right = NULL;
            return res;
        }
        res.rc = SEQ_OK;
        res.elem = found;
        seq_dealloc_node(txn, tree); /* mid was transferred to res.right */
        return res;
    }

    size_t mid_idx = idx - tree->deep.pr_size;

    if (mid_idx < tree->deep.mid->size)
    {
        /*
         * Target is in the middle tree.  Split the middle to find the
         * SeqNode containing our leaf, then split within that node.
         */
        SplitResult mid_sr = ftree_split_exact(tree->deep.mid, mid_idx, item_depth + 1, txn);
        if (mid_sr.rc != SEQ_OK)
            return res;
        SeqNode *node = seq_item_as_node(mid_sr.elem);

        /* Leaf offset within the found node */
        size_t node_idx = mid_idx - mid_sr.left->size;

        SmallItems node_left = {0};
        SmallItems node_right = {0};
        SeqItem found = split_digit_at(node->child, node->arity, 3, node_idx, item_depth,
                                       &node_left, &node_right);

        res.left = deep_r_items(tree->deep.pr, tree->deep.pr_count, tree->deep.pr_size, mid_sr.left,
                                node_left.elems, node_left.count, item_depth, txn);
        res.right = deep_l_items(node_right.elems, node_right.count, mid_sr.right, tree->deep.sf,
                                 tree->deep.sf_count, tree->deep.sf_size, item_depth, txn);
        if (!res.left || !res.right)
        {
            ftree_free(res.left, item_depth, txn);
            ftree_free(res.right, item_depth, txn);
            res.left = NULL;
            res.right = NULL;
            seq_dealloc_node(txn, node);
            return res;
        }
        res.rc = SEQ_OK;
        res.elem = found;
        seq_dealloc_node(txn, node); /* node was consumed */
        seq_dealloc_node(txn, tree); /* shell consumed; pr/sf moved to left/right */
        return res;
    }

    /* Target is in the suffix */
    size_t sf_idx = mid_idx - tree->deep.mid->size;
    SmallItems left_si = {0};
    SmallItems right_si = {0};
    SeqItem found = split_digit_at(tree->deep.sf, tree->deep.sf_count, 4, sf_idx, item_depth,
                                   &left_si, &right_si);

    res.left = deep_r_items(tree->deep.pr, tree->deep.pr_count, tree->deep.pr_size, tree->deep.mid,
                            left_si.elems, left_si.count, item_depth, txn);
    res.right = small_items_to_tree(right_si.elems, right_si.count, item_depth, txn);
    if (!res.left || !res.right)
    {
        ftree_free(res.left, item_depth, txn);
        ftree_free(res.right, item_depth, txn);
        res.left = NULL;
        res.right = NULL;
        return res;
    }
    res.rc = SEQ_OK;
    res.elem = found;
    seq_dealloc_node(txn, tree); /* mid was transferred to res.left */
    return res;
}

/* ================================================================== */
/* Public API                                                           */
/* ================================================================== */

Seq *seq_new(SapEnv *env) {
    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    if (!txn) return NULL;
    Seq *s = seq_alloc_node(txn, sizeof(Seq));
    if (!s) { sap_txn_abort(txn); return NULL; }
    s->valid = 1;
    s->root = ftree_new(txn);
    if (!s->root) { sap_txn_abort(txn); return NULL; }
    sap_txn_commit(txn);
    return s;
}



int seq_is_valid(const Seq *seq) { return (seq && seq->valid && seq->root) ? 1 : 0; }

void seq_free(SapEnv *env, Seq *seq)
{
    if (!seq)
        return;
    SapTxnCtx *txn = sap_txn_begin(env, NULL, 0);
    if (!txn) return;
    if (seq->root)
        ftree_free(seq->root, 0, txn);
    seq->root = NULL;
    seq->valid = 0;
    seq_dealloc_node(txn, seq);
    sap_txn_commit(txn);
}

int seq_reset(SapTxnCtx *txn, Seq *seq)
{
    if (!seq)
        return SEQ_INVALID;

    if (seq_prepare_root(txn, seq) != SEQ_OK) return SEQ_OOM;

    if (seq->root)
        ftree_free(seq->root, 0, txn);
    seq->root = NULL;
    seq->root = ftree_new(txn);
    if (!seq->root)
    {
        seq->valid = 0;
        return SEQ_OOM;
    }
    seq->valid = 1;
    return SEQ_OK;
}

size_t seq_length(const Seq *seq) { return (seq && seq->valid && seq->root) ? seq->root->size : 0; }

int seq_push_front(SapTxnCtx *txn, Seq *seq, uint32_t elem)
{
    if (!seq || !seq->valid || !seq->root)
        return SEQ_INVALID;
    if (seq_prepare_root(txn, seq) != SEQ_OK) return SEQ_OOM;
    int rc = ftree_push_front(&seq->root, seq_item_from_handle(elem), 0, txn);
    if (rc == SEQ_OOM)
        seq->valid = 0;
    return rc;
}

int seq_push_back(SapTxnCtx *txn, Seq *seq, uint32_t elem)
{
    if (!seq || !seq->valid || !seq->root)
        return SEQ_INVALID;
    if (seq_prepare_root(txn, seq) != SEQ_OK) return SEQ_OOM;
    int rc = ftree_push_back(&seq->root, seq_item_from_handle(elem), 0, txn);
    if (rc == SEQ_OOM)
        seq->valid = 0;
    return rc;
}

int seq_pop_front(SapTxnCtx *txn, Seq *seq, uint32_t *out)
{
    if (!seq || !seq->valid || !seq->root || !out)
        return SEQ_INVALID;
    if (seq->root->tag == FTREE_EMPTY)
        return SEQ_EMPTY;
    if (seq_prepare_root(txn, seq) != SEQ_OK) return SEQ_OOM;
    *out = seq_item_to_handle(ftree_pop_front(&seq->root, 0, txn));
    return SEQ_OK;
}

int seq_pop_back(SapTxnCtx *txn, Seq *seq, uint32_t *out)
{
    if (!seq || !seq->valid || !seq->root || !out)
        return SEQ_INVALID;
    if (seq->root->tag == FTREE_EMPTY)
        return SEQ_EMPTY;
    if (seq_prepare_root(txn, seq) != SEQ_OK) return SEQ_OOM;
    *out = seq_item_to_handle(ftree_pop_back(&seq->root, 0, txn));
    return SEQ_OK;
}

int seq_concat(SapTxnCtx *txn, Seq *dest, Seq *src)
{
    if (!dest || !src || !dest->valid || !src->valid || !dest->root || !src->root || dest == src)
        return SEQ_INVALID;

    if (seq_prepare_root(txn, dest) != SEQ_OK) return SEQ_OOM;
    if (seq_prepare_root(txn, src) != SEQ_OK) return SEQ_OOM;

    FTree *dest_root = dest->root;
    FTree *src_root = src->root;
    dest->root = NULL;
    src->root = NULL;

    FTree *new_root = app3(dest_root, NULL, 0, src_root, 0, txn);
    if (!new_root)
    {
        dest->valid = 0;
        src->valid = 0;
        return SEQ_OOM;
    }

    dest->root = new_root;
    dest->valid = 1;
    /* src's tree was consumed; give it a fresh empty root */
    src->root = ftree_new(txn);
    if (!src->root)
    {
        /* src becomes invalid on OOM while reinitializing. */
        src->valid = 0;
        return SEQ_OOM;
    }
    src->valid = 1;
    return SEQ_OK;
}

int seq_split_at(SapTxnCtx *txn, Seq *seq, size_t idx, Seq **left_out, Seq **right_out)
{
    if (!seq || !seq->valid || !seq->root || !left_out || !right_out)
        return SEQ_INVALID;

    size_t n = seq->root->size;
    if (idx > n)
        return SEQ_RANGE;

    if (seq_prepare_root(txn, seq) != SEQ_OK) return SEQ_OOM;

    Seq *left = seq_alloc_node(txn, sizeof(Seq)); if (left) { left->root = ftree_new(txn); left->valid = 1; }
    if (!left)
        return SEQ_OOM;
    Seq *right = seq_alloc_node(txn, sizeof(Seq)); if (right) { right->root = ftree_new(txn); right->valid = 1; }
    if (!right)
    {
        if (left) { ftree_free(left->root, 0, txn); seq_dealloc_node(txn, left); }
        return SEQ_OOM;
    }

    if (idx == 0)
    {
        FTree *replacement = ftree_new(txn);
        if (!replacement)
        {
            if (left) { ftree_free(left->root, 0, txn); seq_dealloc_node(txn, left); }
            if (right) { ftree_free(right->root, 0, txn); seq_dealloc_node(txn, right); }
            return SEQ_OOM;
        }

        /* Transfer the whole tree to right; left stays empty */
        ftree_free(right->root, 0, txn);
        right->root = seq->root;
        seq->root = replacement;
        *left_out = left;
        *right_out = right;
        return SEQ_OK;
    }

    if (idx == n)
    {
        FTree *replacement = ftree_new(txn);
        if (!replacement)
        {
            if (left) { ftree_free(left->root, 0, txn); seq_dealloc_node(txn, left); }
            if (right) { ftree_free(right->root, 0, txn); seq_dealloc_node(txn, right); }
            return SEQ_OOM;
        }

        /* Transfer the whole tree to left; right stays empty */
        ftree_free(left->root, 0, txn);
        left->root = seq->root;
        seq->root = replacement;
        *left_out = left;
        *right_out = right;
        return SEQ_OK;
    }

    /*
     * General case: split so that left = [0, idx), right = [idx, n).
     * ftree_split_exact extracts element at idx, leaving [0, idx-1] on
     * the left; we push the element back to the front of right.
     */
    FTree *root = seq->root;
    seq->root = NULL;
    SplitResult sr = ftree_split_exact(root, idx, 0, txn);
    if (sr.rc != SEQ_OK)
    {
        /*
         * Split may consume/free interior structure on OOM. Discard this
         * detached shell to avoid leaking it; sequence remains invalid.
         */
        seq_dealloc_node(txn, root);
        seq->root = NULL;
        seq->valid = 0;
        if (left) { ftree_free(left->root, 0, txn); seq_dealloc_node(txn, left); }
        if (right) { ftree_free(right->root, 0, txn); seq_dealloc_node(txn, right); }
        return SEQ_OOM;
    }

    ftree_free(left->root, 0, txn);
    ftree_free(right->root, 0, txn);
    left->root = sr.left;
    right->root = sr.right;

    int rc = ftree_push_front(&right->root, sr.elem, 0, txn);
    if (rc != SEQ_OK)
    {
        seq->valid = 0;
        if (rc == SEQ_OOM)
            right->valid = 0;
        if (left) { ftree_free(left->root, 0, txn); seq_dealloc_node(txn, left); }
        if (right) { ftree_free(right->root, 0, txn); seq_dealloc_node(txn, right); }
        return SEQ_OOM;
    }

    /* seq is now empty */
    seq->root = ftree_new(txn);
    if (!seq->root)
    {
        seq->valid = 0;
        if (left) { ftree_free(left->root, 0, txn); seq_dealloc_node(txn, left); }
        if (right) { ftree_free(right->root, 0, txn); seq_dealloc_node(txn, right); }
        return SEQ_OOM;
    }
    seq->valid = 1;

    *left_out = left;
    *right_out = right;
    return SEQ_OK;
}

int seq_get(const Seq *seq, size_t idx, uint32_t *out)
{
    if (!seq || !seq->valid || !seq->root || !out)
        return SEQ_INVALID;

    if (idx >= seq->root->size)
        return SEQ_RANGE;
    *out = seq_item_to_handle(ftree_get(seq->root, idx, 0));
    return SEQ_OK;
}
