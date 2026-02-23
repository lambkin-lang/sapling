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
 *   item_depth == 0  →  items stored here are user void* (measure = 1 each)
 *   item_depth >  0  →  items are SeqNode* whose measure is node->size
 *
 * The middle tree of a depth-d tree has item_depth d+1.
 *
 * Memory ownership
 * ----------------
 * Every FTree* and SeqNode* has exactly one owner (no shared structure).
 * Operations that "consume" a tree transfer ownership of all contained nodes
 * to the result; the consumed FTree shell is freed immediately.
 * User data pointers are never freed by this library.
 *
 * Thread safety: none.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/seq.h"

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

/*
 * FTree — a finger tree node.
 *
 * When tag == FTREE_DEEP the prefix and suffix arrays are stored inline,
 * avoiding separate heap allocations for digit objects.
 */
typedef struct FTree
{
    FTreeTag tag;
    size_t   size; /* total leaf elements in this subtree */
    union
    {
        void *single; /* FTREE_SINGLE: the one item (user ptr or SeqNode*) */
        struct
        {
            int          pr_count; /* 1–4 */
            void        *pr[4];
            size_t       pr_size;
            struct FTree *mid;     /* middle tree, item_depth + 1 */
            int          sf_count; /* 1–4 */
            void        *sf[4];
            size_t       sf_size;
        } deep;
    };
} FTree;

/*
 * SeqNode — a 2-ary or 3-ary internal node.
 * child[i] are items at depth (node_depth - 1): user void* or SeqNode*.
 */
typedef struct SeqNode
{
    size_t size;     /* total leaf elements beneath */
    int    arity;    /* 2 or 3 */
    void  *child[3]; /* children */
} SeqNode;

/* Public handle */
struct Seq
{
    FTree *root;
};

/*
 * SmallItems — a temporary array of up to 4 items used during split.
 */
typedef struct
{
    void  *elems[4];
    int    count;
    size_t size;
} SmallItems;

/*
 * SplitResult — returned by ftree_split_exact.
 * elem is the item at item_depth that contains the target leaf.
 */
typedef struct
{
    FTree *left;
    void  *elem;
    FTree *right;
} SplitResult;

/* ================================================================== */
/* Forward declarations                                                 */
/* ================================================================== */

static int    ftree_push_front(FTree *tree, void *item, int item_depth);
static int    ftree_push_back(FTree *tree, void *item, int item_depth);
static void  *ftree_pop_front(FTree *tree, int item_depth);
static void  *ftree_pop_back(FTree *tree, int item_depth);
static void  *ftree_get(const FTree *t, size_t idx, int item_depth);
static FTree *app3(FTree *t1, void **ts, int ts_count, FTree *t2, int item_depth);
static SplitResult ftree_split_exact(FTree *tree, size_t idx, int item_depth);

/* ================================================================== */
/* Helpers                                                              */
/* ================================================================== */

/* Return the leaf measure of one item at the given depth. */
static inline size_t item_measure(const void *item, int depth)
{
    if (depth == 0)
        return 1;
    return ((const SeqNode *)item)->size;
}

/* Allocate an empty FTree shell. */
static FTree *ftree_new(void)
{
    FTree *t = malloc(sizeof(FTree));
    if (!t)
        return NULL;
    t->tag  = FTREE_EMPTY;
    t->size = 0;
    return t;
}

/* Allocate a 2-ary internal node whose children are at child_depth. */
static SeqNode *node_new2(void *a, void *b, int child_depth)
{
    SeqNode *n = malloc(sizeof(SeqNode));
    assert(n != NULL);
    n->arity    = 2;
    n->child[0] = a;
    n->child[1] = b;
    n->child[2] = NULL;
    n->size     = item_measure(a, child_depth) + item_measure(b, child_depth);
    return n;
}

/* Allocate a 3-ary internal node whose children are at child_depth. */
static SeqNode *node_new3(void *a, void *b, void *c, int child_depth)
{
    SeqNode *n = malloc(sizeof(SeqNode));
    assert(n != NULL);
    n->arity    = 3;
    n->child[0] = a;
    n->child[1] = b;
    n->child[2] = c;
    n->size     = item_measure(a, child_depth) + item_measure(b, child_depth) +
                  item_measure(c, child_depth);
    return n;
}

/*
 * pack_nodes — pack items[0..count-1] (at child_depth) into 2-ary/3-ary
 * nodes.  count must be in [2, 12].  Fills out[]; returns node count.
 *
 * Packing strategy: greedily emit Node3 while count > 4, then handle the
 * tail (2, 3, or 4 items) as one or two nodes, ensuring no remainder of 1.
 */
static int pack_nodes(void **items, int count, int child_depth, SeqNode **out)
{
    int n = 0;
    while (count > 4)
    {
        out[n++] = node_new3(items[0], items[1], items[2], child_depth);
        items += 3;
        count -= 3;
    }
    switch (count)
    {
    case 2:
        out[n++] = node_new2(items[0], items[1], child_depth);
        break;
    case 3:
        out[n++] = node_new3(items[0], items[1], items[2], child_depth);
        break;
    case 4:
        out[n++] = node_new2(items[0], items[1], child_depth);
        out[n++] = node_new2(items[2], items[3], child_depth);
        break;
    default:
        break; /* unreachable */
    }
    return n;
}

/* ================================================================== */
/* Free                                                                 */
/* ================================================================== */

/*
 * seq_node_free — recursively free a SeqNode and all descendant SeqNodes.
 * child_depth: depth of this node's children (0 = user void*, do not free).
 */
static void seq_node_free(SeqNode *node, int child_depth)
{
    if (child_depth > 0)
    {
        for (int i = 0; i < node->arity; i++)
            seq_node_free((SeqNode *)node->child[i], child_depth - 1);
    }
    free(node);
}

/*
 * ftree_free — recursively free a FTree and all contained SeqNodes.
 * item_depth: depth of items stored in this tree.
 */
static void ftree_free(FTree *t, int item_depth)
{
    if (!t)
        return;
    switch (t->tag)
    {
    case FTREE_EMPTY:
        break;
    case FTREE_SINGLE:
        if (item_depth > 0)
            seq_node_free((SeqNode *)t->single, item_depth - 1);
        break;
    case FTREE_DEEP:
        if (item_depth > 0)
        {
            for (int i = 0; i < t->deep.pr_count; i++)
                seq_node_free((SeqNode *)t->deep.pr[i], item_depth - 1);
            for (int i = 0; i < t->deep.sf_count; i++)
                seq_node_free((SeqNode *)t->deep.sf[i], item_depth - 1);
        }
        ftree_free(t->deep.mid, item_depth + 1);
        break;
    }
    free(t);
}

/* ================================================================== */
/* Push front / back                                                    */
/* ================================================================== */

/*
 * ftree_push_front — prepend item (at item_depth) to tree, in place.
 * Returns SEQ_OK or SEQ_OOM.  On SEQ_OOM the tree may be partially
 * modified; callers should treat the Seq as invalid.
 */
static int ftree_push_front(FTree *tree, void *item, int item_depth)
{
    size_t sz = item_measure(item, item_depth);
    switch (tree->tag)
    {
    case FTREE_EMPTY:
        tree->tag    = FTREE_SINGLE;
        tree->size   = sz;
        tree->single = item;
        return SEQ_OK;

    case FTREE_SINGLE:
    {
        void  *b   = tree->single;
        FTree *mid = ftree_new();
        if (!mid)
            return SEQ_OOM;
        tree->tag           = FTREE_DEEP;
        tree->size         += sz;
        tree->deep.pr_count = 1;
        tree->deep.pr[0]    = item;
        tree->deep.pr_size  = sz;
        tree->deep.mid      = mid;
        tree->deep.sf_count = 1;
        tree->deep.sf[0]    = b;
        tree->deep.sf_size  = item_measure(b, item_depth);
        return SEQ_OK;
    }

    case FTREE_DEEP:
        tree->size += sz;
        if (tree->deep.pr_count < 4)
        {
            /* Shift prefix right and insert at front */
            int n = tree->deep.pr_count;
            for (int i = n; i > 0; i--)
                tree->deep.pr[i] = tree->deep.pr[i - 1];
            tree->deep.pr[0] = item;
            tree->deep.pr_count++;
            tree->deep.pr_size += sz;
            return SEQ_OK;
        }
        /*
         * Prefix is full (4 items).  Pack pr[1..3] into a Node3, retain
         * pr[0] in the prefix alongside the new item.
         */
        {
            SeqNode *node =
                node_new3(tree->deep.pr[1], tree->deep.pr[2], tree->deep.pr[3], item_depth);
            void  *old_front    = tree->deep.pr[0];
            tree->deep.pr[0]    = item;
            tree->deep.pr[1]    = old_front;
            tree->deep.pr_count = 2;
            tree->deep.pr_size  = sz + item_measure(old_front, item_depth);
            return ftree_push_front(tree->deep.mid, (void *)node, item_depth + 1);
        }
    }
    return SEQ_OOM; /* unreachable */
}

/*
 * ftree_push_back — append item (at item_depth) to tree, in place.
 */
static int ftree_push_back(FTree *tree, void *item, int item_depth)
{
    size_t sz = item_measure(item, item_depth);
    switch (tree->tag)
    {
    case FTREE_EMPTY:
        tree->tag    = FTREE_SINGLE;
        tree->size   = sz;
        tree->single = item;
        return SEQ_OK;

    case FTREE_SINGLE:
    {
        void  *b   = tree->single;
        FTree *mid = ftree_new();
        if (!mid)
            return SEQ_OOM;
        tree->tag           = FTREE_DEEP;
        tree->size         += sz;
        tree->deep.pr_count = 1;
        tree->deep.pr[0]    = b;
        tree->deep.pr_size  = item_measure(b, item_depth);
        tree->deep.mid      = mid;
        tree->deep.sf_count = 1;
        tree->deep.sf[0]    = item;
        tree->deep.sf_size  = sz;
        return SEQ_OK;
    }

    case FTREE_DEEP:
        tree->size += sz;
        if (tree->deep.sf_count < 4)
        {
            tree->deep.sf[tree->deep.sf_count] = item;
            tree->deep.sf_count++;
            tree->deep.sf_size += sz;
            return SEQ_OK;
        }
        /*
         * Suffix is full.  Pack sf[0..2] into a Node3, retain sf[3] in
         * the suffix alongside the new item.
         */
        {
            SeqNode *node =
                node_new3(tree->deep.sf[0], tree->deep.sf[1], tree->deep.sf[2], item_depth);
            void  *old_last     = tree->deep.sf[3];
            tree->deep.sf[0]    = old_last;
            tree->deep.sf[1]    = item;
            tree->deep.sf_count = 2;
            tree->deep.sf_size  = item_measure(old_last, item_depth) + sz;
            return ftree_push_back(tree->deep.mid, (void *)node, item_depth + 1);
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
static void *ftree_pop_front(FTree *tree, int item_depth)
{
    assert(tree->tag != FTREE_EMPTY);

    if (tree->tag == FTREE_SINGLE)
    {
        void *item   = tree->single;
        tree->tag    = FTREE_EMPTY;
        tree->size   = 0;
        return item;
    }

    /* FTREE_DEEP */
    void  *item    = tree->deep.pr[0];
    size_t item_sz = item_measure(item, item_depth);
    tree->size    -= item_sz;

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
            void *sf0 = tree->deep.sf[0];
            free(tree->deep.mid);
            tree->tag    = FTREE_SINGLE;
            tree->single = sf0;
        }
        else
        {
            /* Move sf[0] to become the new single-item prefix */
            void  *sf0    = tree->deep.sf[0];
            size_t sf0_sz = item_measure(sf0, item_depth);
            tree->deep.pr[0]    = sf0;
            tree->deep.pr_count = 1;
            tree->deep.pr_size  = sf0_sz;
            tree->deep.sf_count--;
            tree->deep.sf_size -= sf0_sz;
            for (int i = 0; i < tree->deep.sf_count; i++)
                tree->deep.sf[i] = tree->deep.sf[i + 1];
        }
    }
    else
    {
        /* Pop a node from middle; its children become the new prefix */
        SeqNode *node = (SeqNode *)ftree_pop_front(tree->deep.mid, item_depth + 1);
        tree->deep.pr_count = node->arity;
        tree->deep.pr_size  = node->size;
        for (int i = 0; i < node->arity; i++)
            tree->deep.pr[i] = node->child[i];
        free(node);
    }

    return item;
}

/*
 * ftree_pop_back — remove and return the last item (at item_depth).
 * Pre: tree is non-empty.  Modifies tree in place.
 */
static void *ftree_pop_back(FTree *tree, int item_depth)
{
    assert(tree->tag != FTREE_EMPTY);

    if (tree->tag == FTREE_SINGLE)
    {
        void *item   = tree->single;
        tree->tag    = FTREE_EMPTY;
        tree->size   = 0;
        return item;
    }

    /* FTREE_DEEP */
    void  *item    = tree->deep.sf[tree->deep.sf_count - 1];
    size_t item_sz = item_measure(item, item_depth);
    tree->size    -= item_sz;

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
            void *pr0 = tree->deep.pr[0];
            free(tree->deep.mid);
            tree->tag    = FTREE_SINGLE;
            tree->single = pr0;
        }
        else
        {
            /* Move pr[last] to become the new single-item suffix */
            int    last       = tree->deep.pr_count - 1;
            void  *pr_last    = tree->deep.pr[last];
            size_t pr_last_sz = item_measure(pr_last, item_depth);
            tree->deep.sf[0]    = pr_last;
            tree->deep.sf_count = 1;
            tree->deep.sf_size  = pr_last_sz;
            tree->deep.pr_count--;
            tree->deep.pr_size -= pr_last_sz;
        }
    }
    else
    {
        /* Pop a node from back of middle; its children become the new suffix */
        SeqNode *node = (SeqNode *)ftree_pop_back(tree->deep.mid, item_depth + 1);
        tree->deep.sf_count = node->arity;
        tree->deep.sf_size  = node->size;
        for (int i = 0; i < node->arity; i++)
            tree->deep.sf[i] = node->child[i];
        free(node);
    }

    return item;
}

/* ================================================================== */
/* Indexing (get)                                                       */
/* ================================================================== */

/* Drill into item (at item_depth) to find the leaf at offset idx. */
static void *item_get(void *item, size_t idx, int item_depth)
{
    if (item_depth == 0)
        return item; /* idx must be 0; item is the leaf */

    SeqNode *node = (SeqNode *)item;
    size_t   off  = 0;
    for (int i = 0; i < node->arity; i++)
    {
        size_t sz = item_measure(node->child[i], item_depth - 1);
        if (idx < off + sz)
            return item_get(node->child[i], idx - off, item_depth - 1);
        off += sz;
    }
    return NULL; /* unreachable for valid idx */
}

/* Return the leaf at index idx within a digit (array of items at item_depth). */
static void *digit_get(void *const *elems, int count, size_t idx, int item_depth)
{
    size_t off = 0;
    for (int i = 0; i < count; i++)
    {
        size_t sz = item_measure(elems[i], item_depth);
        if (idx < off + sz)
            return item_get(elems[i], idx - off, item_depth);
        off += sz;
    }
    return NULL; /* unreachable for valid idx */
}

static void *ftree_get(const FTree *t, size_t idx, int item_depth)
{
    switch (t->tag)
    {
    case FTREE_EMPTY:
        return NULL;
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
    return NULL; /* unreachable */
}

/* ================================================================== */
/* Concatenation                                                        */
/* ================================================================== */

/*
 * small_items_to_tree — build a FTree at item_depth from 0–4 items.
 * Ownership of the items transfers to the new tree.
 */
static FTree *small_items_to_tree(void **items, int count, int item_depth)
{
    FTree *t = ftree_new();
    assert(t != NULL);
    for (int i = 0; i < count; i++)
    {
        int rc = ftree_push_back(t, items[i], item_depth);
        assert(rc == SEQ_OK);
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
static FTree *deep_l_items(void **pr, int pr_count, FTree *mid, void **sf, int sf_count,
                            size_t sf_size, int item_depth)
{
    if (pr_count > 0)
    {
        FTree *t = ftree_new();
        assert(t);
        t->tag           = FTREE_DEEP;
        t->deep.pr_count = pr_count;
        t->deep.pr_size  = 0;
        for (int i = 0; i < pr_count; i++)
        {
            t->deep.pr[i] = pr[i];
            t->deep.pr_size += item_measure(pr[i], item_depth);
        }
        t->deep.mid      = mid;
        t->deep.sf_count = sf_count;
        for (int i = 0; i < sf_count; i++)
            t->deep.sf[i] = sf[i];
        t->deep.sf_size = sf_size;
        t->size         = t->deep.pr_size + mid->size + sf_size;
        return t;
    }

    /* Empty prefix: borrow from mid or fall back to suffix */
    if (mid->tag == FTREE_EMPTY)
    {
        free(mid);
        return small_items_to_tree(sf, sf_count, item_depth);
    }

    /* Pop a node from front of mid; expand it into the new prefix */
    SeqNode *node = (SeqNode *)ftree_pop_front(mid, item_depth + 1);
    FTree   *t    = ftree_new();
    assert(t);
    t->tag           = FTREE_DEEP;
    t->deep.pr_count = node->arity;
    t->deep.pr_size  = node->size;
    for (int i = 0; i < node->arity; i++)
        t->deep.pr[i] = node->child[i];
    t->deep.mid      = mid;
    t->deep.sf_count = sf_count;
    for (int i = 0; i < sf_count; i++)
        t->deep.sf[i] = sf[i];
    t->deep.sf_size = sf_size;
    t->size         = node->size + mid->size + sf_size;
    free(node);
    return t;
}

/*
 * deep_r_items — symmetric to deep_l_items: build from prefix + mid +
 * (possibly empty) suffix.  Borrows from back of mid if suffix is empty.
 */
static FTree *deep_r_items(void **pr, int pr_count, size_t pr_size, FTree *mid, void **sf,
                            int sf_count, int item_depth)
{
    if (sf_count > 0)
    {
        FTree *t = ftree_new();
        assert(t);
        t->tag           = FTREE_DEEP;
        t->deep.pr_count = pr_count;
        for (int i = 0; i < pr_count; i++)
            t->deep.pr[i] = pr[i];
        t->deep.pr_size  = pr_size;
        t->deep.mid      = mid;
        t->deep.sf_count = sf_count;
        t->deep.sf_size  = 0;
        for (int i = 0; i < sf_count; i++)
        {
            t->deep.sf[i] = sf[i];
            t->deep.sf_size += item_measure(sf[i], item_depth);
        }
        t->size = pr_size + mid->size + t->deep.sf_size;
        return t;
    }

    /* Empty suffix: borrow from mid or fall back to prefix */
    if (mid->tag == FTREE_EMPTY)
    {
        free(mid);
        return small_items_to_tree(pr, pr_count, item_depth);
    }

    SeqNode *node = (SeqNode *)ftree_pop_back(mid, item_depth + 1);
    FTree   *t    = ftree_new();
    assert(t);
    t->tag           = FTREE_DEEP;
    t->deep.pr_count = pr_count;
    for (int i = 0; i < pr_count; i++)
        t->deep.pr[i] = pr[i];
    t->deep.pr_size  = pr_size;
    t->deep.mid      = mid;
    t->deep.sf_count = node->arity;
    t->deep.sf_size  = node->size;
    for (int i = 0; i < node->arity; i++)
        t->deep.sf[i] = node->child[i];
    t->size = pr_size + mid->size + node->size;
    free(node);
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
 * Returns the merged tree (either t1's shell reused, or t2's shell).
 */
static FTree *app3(FTree *t1, void **ts, int ts_count, FTree *t2, int item_depth)
{
    int rc;

    if (t1->tag == FTREE_EMPTY)
    {
        free(t1);
        for (int i = ts_count - 1; i >= 0; i--)
        {
            rc = ftree_push_front(t2, ts[i], item_depth);
            assert(rc == SEQ_OK);
        }
        return t2;
    }

    if (t2->tag == FTREE_EMPTY)
    {
        free(t2);
        for (int i = 0; i < ts_count; i++)
        {
            rc = ftree_push_back(t1, ts[i], item_depth);
            assert(rc == SEQ_OK);
        }
        return t1;
    }

    if (t1->tag == FTREE_SINGLE)
    {
        void *x = t1->single;
        free(t1);
        for (int i = ts_count - 1; i >= 0; i--)
        {
            rc = ftree_push_front(t2, ts[i], item_depth);
            assert(rc == SEQ_OK);
        }
        rc = ftree_push_front(t2, x, item_depth);
        assert(rc == SEQ_OK);
        return t2;
    }

    if (t2->tag == FTREE_SINGLE)
    {
        void *y = t2->single;
        free(t2);
        for (int i = 0; i < ts_count; i++)
        {
            rc = ftree_push_back(t1, ts[i], item_depth);
            assert(rc == SEQ_OK);
        }
        rc = ftree_push_back(t1, y, item_depth);
        assert(rc == SEQ_OK);
        return t1;
    }

    /* Both DEEP: combine sf1 ++ ts ++ pr2, pack into nodes, recurse */
    void *combined[12]; /* max: sf(4) + ts(4) + pr(4) = 12 */
    int   cc = 0;
    for (int i = 0; i < t1->deep.sf_count; i++)
        combined[cc++] = t1->deep.sf[i];
    for (int i = 0; i < ts_count; i++)
        combined[cc++] = ts[i];
    for (int i = 0; i < t2->deep.pr_count; i++)
        combined[cc++] = t2->deep.pr[i];

    SeqNode *nodes[6]; /* 12 items → at most 4 nodes (3+3+3+3), use 6 for safety */
    int      nc = pack_nodes(combined, cc, item_depth, nodes);

    /* Recursively merge the two middle trees with the new node spine */
    FTree *new_mid = app3(t1->deep.mid, (void **)nodes, nc, t2->deep.mid, item_depth + 1);

    /* Reuse t1's shell; update middle and steal t2's suffix */
    t1->deep.mid      = new_mid;
    t1->deep.sf_count = t2->deep.sf_count;
    for (int i = 0; i < t2->deep.sf_count; i++)
        t1->deep.sf[i] = t2->deep.sf[i];
    t1->deep.sf_size = t2->deep.sf_size;
    t1->size         = t1->deep.pr_size + new_mid->size + t1->deep.sf_size;

    free(t2);
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
static void *split_digit_at(void **items, int count, size_t idx, int item_depth,
                             SmallItems *left_out, SmallItems *right_out)
{
    size_t off = 0;
    for (int i = 0; i < count; i++)
    {
        size_t sz = item_measure(items[i], item_depth);
        if (idx < off + sz)
        {
            left_out->count = i;
            left_out->size  = off;
            for (int j = 0; j < i; j++)
                left_out->elems[j] = items[j];

            right_out->count = count - i - 1;
            right_out->size  = 0;
            for (int j = i + 1; j < count; j++)
            {
                right_out->elems[j - i - 1]  = items[j];
                right_out->size             += item_measure(items[j], item_depth);
            }
            return items[i];
        }
        off += sz;
    }
    return NULL; /* unreachable for valid idx */
}

/*
 * ftree_split_exact — destructively split tree at leaf index idx.
 *
 * Returns SplitResult { left, elem, right } where:
 *   left  — FTree at item_depth containing all items before the found one
 *   elem  — the item at item_depth whose leaf range contains idx
 *   right — FTree at item_depth containing all items after elem
 *
 * The original tree is consumed (its shell is freed).
 * Pre: tree is non-empty and idx < tree->size.
 */
static SplitResult ftree_split_exact(FTree *tree, size_t idx, int item_depth)
{
    SplitResult res = {NULL, NULL, NULL};
    assert(tree->tag != FTREE_EMPTY);

    if (tree->tag == FTREE_SINGLE)
    {
        res.left  = ftree_new();
        res.right = ftree_new();
        assert(res.left && res.right);
        res.elem = tree->single;
        free(tree);
        return res;
    }

    /* FTREE_DEEP */

    if (idx < tree->deep.pr_size)
    {
        /* Target is in the prefix */
        SmallItems left_si  = {0};
        SmallItems right_si = {0};
        void      *found =
            split_digit_at(tree->deep.pr, tree->deep.pr_count, idx, item_depth, &left_si,
                           &right_si);

        res.left  = small_items_to_tree(left_si.elems, left_si.count, item_depth);
        res.right = deep_l_items(right_si.elems, right_si.count, tree->deep.mid, tree->deep.sf,
                                 tree->deep.sf_count, tree->deep.sf_size, item_depth);
        res.elem  = found;
        free(tree); /* mid was transferred to res.right */
        return res;
    }

    size_t mid_idx = idx - tree->deep.pr_size;

    if (mid_idx < tree->deep.mid->size)
    {
        /*
         * Target is in the middle tree.  Split the middle to find the
         * SeqNode containing our leaf, then split within that node.
         */
        SplitResult mid_sr = ftree_split_exact(tree->deep.mid, mid_idx, item_depth + 1);
        SeqNode    *node   = (SeqNode *)mid_sr.elem;

        /* Leaf offset within the found node */
        size_t node_idx = mid_idx - mid_sr.left->size;

        SmallItems node_left  = {0};
        SmallItems node_right = {0};
        void      *found =
            split_digit_at(node->child, node->arity, node_idx, item_depth, &node_left,
                           &node_right);

        res.left = deep_r_items(tree->deep.pr, tree->deep.pr_count, tree->deep.pr_size,
                                mid_sr.left, node_left.elems, node_left.count, item_depth);
        res.right =
            deep_l_items(node_right.elems, node_right.count, mid_sr.right, tree->deep.sf,
                         tree->deep.sf_count, tree->deep.sf_size, item_depth);
        res.elem = found;
        free(node);  /* node was consumed */
        free(tree);  /* shell consumed; pr/sf moved to left/right */
        return res;
    }

    /* Target is in the suffix */
    size_t     sf_idx   = mid_idx - tree->deep.mid->size;
    SmallItems left_si  = {0};
    SmallItems right_si = {0};
    void      *found =
        split_digit_at(tree->deep.sf, tree->deep.sf_count, sf_idx, item_depth, &left_si,
                       &right_si);

    res.left = deep_r_items(tree->deep.pr, tree->deep.pr_count, tree->deep.pr_size,
                            tree->deep.mid, left_si.elems, left_si.count, item_depth);
    res.right = small_items_to_tree(right_si.elems, right_si.count, item_depth);
    res.elem  = found;
    free(tree); /* mid was transferred to res.left */
    return res;
}

/* ================================================================== */
/* Public API                                                           */
/* ================================================================== */

Seq *seq_new(void)
{
    Seq *s = malloc(sizeof(Seq));
    if (!s)
        return NULL;
    s->root = ftree_new();
    if (!s->root)
    {
        free(s);
        return NULL;
    }
    return s;
}

void seq_free(Seq *seq)
{
    if (!seq)
        return;
    ftree_free(seq->root, 0);
    free(seq);
}

size_t seq_length(const Seq *seq)
{
    return seq ? seq->root->size : 0;
}

int seq_push_front(Seq *seq, void *elem)
{
    return ftree_push_front(seq->root, elem, 0);
}

int seq_push_back(Seq *seq, void *elem)
{
    return ftree_push_back(seq->root, elem, 0);
}

int seq_pop_front(Seq *seq, void **out)
{
    if (seq->root->tag == FTREE_EMPTY)
        return SEQ_EMPTY;
    *out = ftree_pop_front(seq->root, 0);
    return SEQ_OK;
}

int seq_pop_back(Seq *seq, void **out)
{
    if (seq->root->tag == FTREE_EMPTY)
        return SEQ_EMPTY;
    *out = ftree_pop_back(seq->root, 0);
    return SEQ_OK;
}

int seq_concat(Seq *dest, Seq *src)
{
    FTree *new_root = app3(dest->root, NULL, 0, src->root, 0);
    dest->root      = new_root;
    /* src's tree was consumed; give it a fresh empty root */
    src->root = ftree_new();
    assert(src->root);
    return SEQ_OK;
}

int seq_split_at(Seq *seq, size_t idx, Seq **left_out, Seq **right_out)
{
    size_t n = seq->root->size;
    if (idx > n)
        return SEQ_RANGE;

    Seq *left = seq_new();
    if (!left)
        return SEQ_OOM;
    Seq *right = seq_new();
    if (!right)
    {
        seq_free(left);
        return SEQ_OOM;
    }

    if (idx == 0)
    {
        /* Transfer the whole tree to right; left stays empty */
        ftree_free(right->root, 0);
        right->root = seq->root;
        seq->root   = ftree_new();
        assert(seq->root);
        *left_out  = left;
        *right_out = right;
        return SEQ_OK;
    }

    if (idx == n)
    {
        /* Transfer the whole tree to left; right stays empty */
        ftree_free(left->root, 0);
        left->root = seq->root;
        seq->root  = ftree_new();
        assert(seq->root);
        *left_out  = left;
        *right_out = right;
        return SEQ_OK;
    }

    /*
     * General case: split so that left = [0, idx), right = [idx, n).
     * ftree_split_exact extracts element at idx, leaving [0, idx-1] on
     * the left; we push the element back to the front of right.
     */
    SplitResult sr = ftree_split_exact(seq->root, idx, 0);

    ftree_free(left->root, 0);
    ftree_free(right->root, 0);
    left->root  = sr.left;
    right->root = sr.right;

    int rc = ftree_push_front(right->root, sr.elem, 0);
    assert(rc == SEQ_OK);

    /* seq is now empty */
    seq->root = ftree_new();
    assert(seq->root);

    *left_out  = left;
    *right_out = right;
    return SEQ_OK;
}

int seq_get(const Seq *seq, size_t idx, void **out)
{
    if (idx >= seq->root->size)
        return SEQ_RANGE;
    *out = ftree_get(seq->root, idx, 0);
    return SEQ_OK;
}
