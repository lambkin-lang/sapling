#include "sapling/bept.h"
#include "sapling/arena.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Constants */
enum {
    BEPT_NODE_INTERNAL = 0,
    BEPT_NODE_LEAF = 1
};

/* We use a single subsystem ID now */
#define BEPT_SUBSYSTEM_ID SAP_SUBSYSTEM_BEPT

/* 
 * Node structs
 */
typedef struct Node {
    uint8_t type;
} Node;

typedef struct {
    Node header;
    uint32_t bit;           /* Critical bit index (0 = MSB of word 0) */
    Node *left;             /* Bit 0 branch */
    Node *right;            /* Bit 1 branch */
} Internal;

typedef struct {
    Node header;
    uint32_t key_len_words;
    uint32_t val_len;
    /* Layout: key words then value bytes */
    uint32_t data[]; 
} Leaf;

typedef struct EnvState {
    SapEnv *env;
    Node *root;
} EnvState;

typedef struct TxnState {
    SapTxnCtx *txn;
    EnvState *env_state;
    struct TxnState *parent;
    Node *root;
} TxnState;

/* ------------------------------------------------------------------ */
/* Bit Helpers                                                        */
/* ------------------------------------------------------------------ */

/* Check if bit 'b' is set in key. 
 * b=0 is MSB of key[0]. b=31 is LSB of key[0].
 */
static inline int check_bit(const uint32_t *key, uint32_t key_len, uint32_t bit) {
    uint32_t word_idx = bit / 32;
    if (word_idx >= key_len) return 0; /* Treat out of bounds as 0 */
    
    uint32_t bit_in_word = 31 - (bit % 32);
    return (key[word_idx] >> bit_in_word) & 1;
}

/* Find first differing bit between two keys. 
 * Returns bit index, or -1 if equal.
 */
static int find_diff_bit(const uint32_t *k1, uint32_t k1_len, 
                         const uint32_t *k2, uint32_t k2_len) 
{
    uint32_t min_len = (k1_len < k2_len) ? k1_len : k2_len;
    uint32_t i;
    
    for (i = 0; i < min_len; i++) {
        if (k1[i] != k2[i]) {
            uint32_t diff = k1[i] ^ k2[i];
            /* __builtin_clz returns undefined for 0, but we know diff != 0 */
            return (i * 32) + __builtin_clz(diff);
        }
    }
    
    if (k1_len != k2_len) {
        /* Difference is in the extension */
        /* If k1 is shorter, k1 effectively has 0s. 
           We need to find the first set bit in k2's extension. */
        const uint32_t *longer = (k1_len > k2_len) ? k1 : k2;
        uint32_t max_len = (k1_len > k2_len) ? k1_len : k2_len;
        
        for (; i < max_len; i++) {
            if (longer[i] != 0) {
                return (i * 32) + __builtin_clz(longer[i]);
            }
        }
    }
    
    return -1; /* Keys are identical */
}

/* ------------------------------------------------------------------ */
/* Transaction Callbacks                                              */
/* ------------------------------------------------------------------ */

static int on_begin(SapTxnCtx *txn, void *parent_state, void **state_out) {
    TxnState *state = malloc(sizeof(TxnState));
    if (!state) return ERR_OOM;

    state->txn = txn;
    state->env_state = (EnvState *)sap_env_subsystem_state(sap_txn_env(txn), BEPT_SUBSYSTEM_ID);

    if (parent_state == NULL) {
        state->parent = NULL;
        state->root = state->env_state->root;
    } else {
        state->parent = (TxnState *)parent_state;
        state->root = state->parent->root;
    }

    *state_out = state;
    return ERR_OK;
}

static int on_commit(SapTxnCtx *txn, void *state) {
    (void)txn;
    TxnState *s = (TxnState *)state;
    
    if (s->parent) {
        s->parent->root = s->root;
    } else {
        s->env_state->root = s->root;
    }
    
    free(s);
    return ERR_OK;
}

static void on_abort(SapTxnCtx *txn, void *state) {
    (void)txn;
    TxnState *s = (TxnState *)state;
    free(s);
}

static void on_env_destroy(void *env_state) {
    EnvState *state = (EnvState *)env_state;
    free(state);
}

int sap_bept_subsystem_init(SapEnv *env) {
    SapTxnSubsystemCallbacks callbacks = {
        .on_begin = on_begin,
        .on_commit = on_commit,
        .on_abort = on_abort,
        .on_env_destroy = on_env_destroy,
    };

    if (!env) return ERR_INVALID;

    EnvState *state = malloc(sizeof(EnvState));
    if (!state) return ERR_OOM;

    state->env = env;
    state->root = NULL;

    /* Using a single ID for the generic subsystem */
    int rc = sap_env_register_subsystem(env, BEPT_SUBSYSTEM_ID, &callbacks);
    if (rc != ERR_OK) {
        free(state);
        return rc;
    }

    rc = sap_env_set_subsystem_state(env, BEPT_SUBSYSTEM_ID, state);
    if (rc != ERR_OK) {
        free(state);
        return rc;
    }

    return ERR_OK;
}

/* ------------------------------------------------------------------ */
/* Allocation Helpers                                                 */
/* ------------------------------------------------------------------ */

static int alloc_leaf(SapMemArena *arena, const uint32_t *key, uint32_t key_len, 
                      const void *val, uint32_t val_len, Node **out) 
{
    Leaf *node;
    uint32_t data_size = (key_len * sizeof(uint32_t)) + val_len;
    /* Ensure padding for uint32 alignment if needed, though malloc usually aligns */
    uint32_t total_size = sizeof(Leaf) + data_size;
    uint32_t nodeno;
    
    int rc = sap_arena_alloc_node(arena, total_size, (void**)&node, &nodeno);
    if (rc != ERR_OK) return rc;
    
    node->header.type = BEPT_NODE_LEAF;
    node->key_len_words = key_len;
    node->val_len = val_len;
    
    /* Copy key */
    if (key_len > 0) {
        memcpy(node->data, key, key_len * sizeof(uint32_t));
    }
    
    /* Copy value (after key) */
    if (val && val_len > 0) {
        uint8_t *val_ptr = (uint8_t *)(node->data + key_len);
        memcpy(val_ptr, val, val_len);
    }
    
    *out = (Node*)node;
    return ERR_OK;
}

static int alloc_internal(SapMemArena *arena, uint32_t bit, Node *left, Node *right, Node **out) {
    Internal *node;
    uint32_t nodeno;
    
    int rc = sap_arena_alloc_node(arena, sizeof(Internal), (void**)&node, &nodeno);
    if (rc != ERR_OK) return rc;
    
    node->header.type = BEPT_NODE_INTERNAL;
    node->bit = bit;
    node->left = left;
    node->right = right;
    
    *out = (Node*)node;
    return ERR_OK;
}

/* ------------------------------------------------------------------ */
/* Core Recursive Logic                                               */
/* ------------------------------------------------------------------ */

static int insert_recursive(SapMemArena *arena, Node *node, const uint32_t *key, uint32_t key_len,
                           Node *new_leaf, uint32_t diff_bit, Node **out) 
{
    /* Sentinel for "replace existing leaf" */
    /* diff_bit is -1 if keys match exactly */
    
    if (diff_bit == (uint32_t)-1) {
        /* Replace */
        if (node->type == BEPT_NODE_INTERNAL) {
            Internal *internal = (Internal *)node;
            Node *new_internal;
            int rc = alloc_internal(arena, internal->bit, internal->left, internal->right, &new_internal);
            if (rc != ERR_OK) return rc;
            
            if (check_bit(key, key_len, internal->bit)) {
                rc = insert_recursive(arena, internal->right, key, key_len, new_leaf, (uint32_t)-1, &((Internal*)new_internal)->right);
            } else {
                rc = insert_recursive(arena, internal->left, key, key_len, new_leaf, (uint32_t)-1, &((Internal*)new_internal)->left);
            }
            if (rc != ERR_OK) return rc;
            *out = new_internal;
            return ERR_OK;
        } else {
            /* Leaf case: replaced by new_leaf */
            *out = new_leaf;
            return ERR_OK;
        }
    } else {
        /* Insert branching */
        if (node->type == BEPT_NODE_INTERNAL) {
            Internal *internal = (Internal *)node;
            if (internal->bit < diff_bit) {
                /* We are strictly above the diff point, recurse down */
                Node *new_internal;
                int rc = alloc_internal(arena, internal->bit, internal->left, internal->right, &new_internal);
                if (rc != ERR_OK) return rc;
                
                if (check_bit(key, key_len, internal->bit)) {
                    rc = insert_recursive(arena, internal->right, key, key_len, new_leaf, diff_bit, &((Internal*)new_internal)->right);
                } else {
                    rc = insert_recursive(arena, internal->left, key, key_len, new_leaf, diff_bit, &((Internal*)new_internal)->left);
                }
                if (rc != ERR_OK) return rc;
                *out = new_internal;
                return ERR_OK;
            }
        }
        
        /* Insert new internal node here */
        Node *new_branch;
        /* Determine direction at diff_bit */
        int rc;
        if (check_bit(key, key_len, diff_bit)) {
            rc = alloc_internal(arena, diff_bit, node, new_leaf, &new_branch);
        } else {
            rc = alloc_internal(arena, diff_bit, new_leaf, node, &new_branch);
        }
        
        if (rc != ERR_OK) return rc;
        *out = new_branch;
        return ERR_OK;
    }
}

/* ------------------------------------------------------------------ */
/* Public Operations                                                  */
/* ------------------------------------------------------------------ */

int sap_bept_put(SapTxnCtx *txn, const uint32_t *key, uint32_t key_len_words, 
                 const void *val, uint32_t val_len, unsigned flags, void **reserved_out)
{
    if (!txn) return ERR_INVALID;
    
    TxnState *state = (TxnState *)sap_txn_subsystem_state(txn, BEPT_SUBSYSTEM_ID);
    if (!state) return ERR_INVALID;
    
    SapMemArena *arena = sap_txn_arena(txn);
    if (!arena) return ERR_INVALID;
    
    Node *new_leaf_node;
    int rc = alloc_leaf(arena, key, key_len_words, val, val_len, &new_leaf_node);
    if (rc != ERR_OK) return rc;
    
    Leaf *new_leaf = (Leaf *)new_leaf_node;

    if (!state->root) {
        state->root = new_leaf_node;
        if (reserved_out) {
             uint8_t *val_ptr = (uint8_t *)(new_leaf->data + new_leaf->key_len_words);
             *reserved_out = val_ptr;
        }
        return ERR_OK;
    }
    
    /* Find best match to calculate diff bit */
    Node *node = state->root;
    while (node->type == BEPT_NODE_INTERNAL) {
        Internal *internal = (Internal *)node;
        if (check_bit(key, key_len_words, internal->bit)) {
            node = internal->right;
        } else {
            node = internal->left;
        }
    }
    
    Leaf *leaf = (Leaf *)node;
    uint32_t diff_bit_val; // Renamed to avoid confusion
    
    /* Calculate diff bit between query key and found leaf key */
    int diff_idx = find_diff_bit(key, key_len_words, leaf->data, leaf->key_len_words);
    
    if (diff_idx == -1) {
        /* Exact match */
        if (flags & SAP_NOOVERWRITE) return ERR_EXISTS;
        diff_bit_val = (uint32_t)-1;
    } else {
        diff_bit_val = (uint32_t)diff_idx;
    }
    
    if (reserved_out) {
        uint8_t *val_ptr = (uint8_t *)(new_leaf->data + new_leaf->key_len_words);
        *reserved_out = val_ptr;
    }

    return insert_recursive(arena, state->root, key, key_len_words, new_leaf_node, diff_bit_val, &state->root);
}

int sap_bept_get(SapTxnCtx *txn, const uint32_t *key, uint32_t key_len_words, 
                 const void **val_out, uint32_t *val_len_out)
{
    if (!txn) return ERR_INVALID;
    
    TxnState *state = (TxnState *)sap_txn_subsystem_state(txn, BEPT_SUBSYSTEM_ID);
    if (!state || !state->root) return ERR_NOT_FOUND;
    
    Node *node = state->root;
    while (node->type == BEPT_NODE_INTERNAL) {
        Internal *internal = (Internal *)node;
        if (check_bit(key, key_len_words, internal->bit)) {
            node = internal->right;
        } else {
            node = internal->left;
        }
    }
    
    Leaf *leaf = (Leaf *)node;
    
    /* Check exact match */
    if (key_len_words != leaf->key_len_words) return ERR_NOT_FOUND;
    if (memcmp(key, leaf->data, key_len_words * 4) != 0) return ERR_NOT_FOUND;
    
    if (val_out) {
        *val_out = leaf->data + leaf->key_len_words;
    }
    if (val_len_out) *val_len_out = leaf->val_len;
    
    return ERR_OK;
}

static int delete_recursive(SapMemArena *arena, Node *node, const uint32_t *key, uint32_t key_len, Node **out) {
    if (node->type == BEPT_NODE_LEAF) {
        Leaf *leaf = (Leaf *)node;
        /* Check match */
        if (key_len == leaf->key_len_words && memcmp(key, leaf->data, key_len * 4) == 0) {
            *out = NULL;
            return ERR_OK;
        } else {
            return ERR_NOT_FOUND;
        }
    } else {
        Internal *internal = (Internal *)node;
        int rc;
        Node *new_child;
        
        if (check_bit(key, key_len, internal->bit)) {
            rc = delete_recursive(arena, internal->right, key, key_len, &new_child);
            if (rc != ERR_OK) return rc;
            
            if (new_child == NULL) {
                *out = internal->left;
            } else {
                rc = alloc_internal(arena, internal->bit, internal->left, new_child, out);
                if (rc != ERR_OK) return rc;
            }
        } else {
            rc = delete_recursive(arena, internal->left, key, key_len, &new_child);
            if (rc != ERR_OK) return rc;
            
            if (new_child == NULL) {
                *out = internal->right;
            } else {
                rc = alloc_internal(arena, internal->bit, new_child, internal->right, out);
                if (rc != ERR_OK) return rc;
            }
        }
        return ERR_OK;
    }
}

int sap_bept_del(SapTxnCtx *txn, const uint32_t *key, uint32_t key_len_words) {
    if (!txn) return ERR_INVALID;
    
    TxnState *state = (TxnState *)sap_txn_subsystem_state(txn, BEPT_SUBSYSTEM_ID);
    if (!state) return ERR_INVALID;
    
    if (!state->root) return ERR_NOT_FOUND;

    SapMemArena *arena = sap_txn_arena(txn);
    if (!arena) return ERR_INVALID;

    Node *new_root;
    int rc = delete_recursive(arena, state->root, key, key_len_words, &new_root);
    
    if (rc == ERR_OK) {
        state->root = new_root;
    }
    
    return rc;
}

int sap_bept_min(SapTxnCtx *txn, uint32_t *key_out_buf, uint32_t key_len_words, 
                 const void **val_out, uint32_t *val_len_out)
{
    if (!txn) return ERR_INVALID;
    
    TxnState *state = (TxnState *)sap_txn_subsystem_state(txn, BEPT_SUBSYSTEM_ID);
    if (!state || !state->root) return ERR_NOT_FOUND;
    
    Node *node = state->root;
    while (node->type == BEPT_NODE_INTERNAL) {
        Internal *internal = (Internal *)node;
        node = internal->left;
    }
    
    Leaf *leaf = (Leaf *)node;
    if (key_out_buf) {
        uint32_t copy_words = (leaf->key_len_words < key_len_words) ? leaf->key_len_words : key_len_words;
        memcpy(key_out_buf, leaf->data, copy_words * 4);
    }
    if (val_out) *val_out = leaf->data + leaf->key_len_words;
    if (val_len_out) *val_len_out = leaf->val_len;
    
    return ERR_OK;
}
