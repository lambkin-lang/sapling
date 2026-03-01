/*
 * text_tree_registry.h - COW tree sharing registry for cross-worker text transfer
 *
 * Follows the Thatch lifecycle pattern: entries are owned during a
 * transaction (single-writer, no lock needed), sealed on commit
 * (become immutable), and lock-free readable after seal.
 *
 * For cross-thread refcount management, uses atomic operations.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#ifndef SAPLING_TEXT_TREE_REGISTRY_H
#define SAPLING_TEXT_TREE_REGISTRY_H

#include <stdint.h>
#include "sapling/text.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TextTreeRegistry TextTreeRegistry;

struct SapEnv;

/*
 * Create a new tree registry.
 * Returns NULL on allocation failure.
 */
TextTreeRegistry *text_tree_registry_new(struct SapEnv *env);

/*
 * Destroy the registry and release all held Text values.
 */
void text_tree_registry_free(TextTreeRegistry *reg);

/*
 * Register a Text under a new tree ID. The registry takes a COW
 * clone of the text (via text_clone). The original text remains valid.
 * Returns ERR_OK on success, assigns *id_out.
 */
int text_tree_registry_register(TextTreeRegistry *reg, const Text *text,
                                uint32_t *id_out);

/*
 * Look up a tree by ID. Returns a borrowed pointer valid as long as
 * the registry entry exists. Thread-safe (immutable data after seal).
 */
int text_tree_registry_get(const TextTreeRegistry *reg, uint32_t id,
                           const Text **text_out);

/*
 * Bump the refcount for a tree ID. Used on the message-receive path
 * when a worker receives a TREE handle. Thread-safe (atomic).
 */
int text_tree_registry_retain(TextTreeRegistry *reg, uint32_t id);

/*
 * Decrement the refcount for a tree ID. When refs reach 0, the entry's
 * Text is freed. Thread-safe (atomic).
 */
int text_tree_registry_release(TextTreeRegistry *reg, uint32_t id);

/* Return the number of registered entries. */
uint32_t text_tree_registry_count(const TextTreeRegistry *reg);

/*
 * Resolver adapter matching TextResolveTreeTextFn.
 * Pass the TextTreeRegistry pointer as ctx.
 */
int text_tree_registry_resolve_fn(uint32_t tree_id, const Text **text_out,
                                  void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_TEXT_TREE_REGISTRY_H */
