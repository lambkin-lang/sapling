/*
 * text_literal.h - immutable literal table for text handle resolution
 *
 * Maps uint32_t literal IDs to (const uint8_t *, size_t) UTF-8 byte pairs.
 * Arena-backed bump allocation for data storage. Once sealed, reads are
 * safe without synchronization.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#ifndef SAPLING_TEXT_LITERAL_H
#define SAPLING_TEXT_LITERAL_H

#include <stddef.h>
#include <stdint.h>
#include "sapling/text.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TextLiteralTable TextLiteralTable;

/* Forward declare SapEnv */
struct SapEnv;

/*
 * Create an empty, mutable literal table backed by the arena from env.
 * Returns NULL on allocation failure.
 */
TextLiteralTable *text_literal_table_new(struct SapEnv *env);

/*
 * Add a UTF-8 string to the table, receiving its literal ID.
 *
 * The table copies the bytes into arena-backed pages (owns the data).
 * If an identical byte sequence was previously added, the existing ID
 * is returned (deduplication via FNV-1a hash).
 *
 * Returns ERR_OK on success, ERR_OOM on allocation failure, ERR_INVALID
 * if the table is sealed or the 30-bit ID space is exhausted.
 */
int text_literal_table_add(TextLiteralTable *table,
                           const uint8_t *utf8, size_t utf8_len,
                           uint32_t *id_out);

/*
 * Seal the table. No further additions are allowed.
 * After sealing, the table is safe for concurrent reads
 * without synchronization.
 */
void text_literal_table_seal(TextLiteralTable *table);

/* Query whether the table is sealed. */
int text_literal_table_is_sealed(const TextLiteralTable *table);

/*
 * Lookup a literal by ID. Returns ERR_OK on success, ERR_RANGE if ID
 * is out of range. utf8_out and utf8_len_out point into arena-owned
 * memory that is valid for the lifetime of the table.
 */
int text_literal_table_get(const TextLiteralTable *table,
                           uint32_t id,
                           const uint8_t **utf8_out,
                           size_t *utf8_len_out);

/* Return the number of entries in the table. */
uint32_t text_literal_table_count(const TextLiteralTable *table);

/* Destroy the table and free all owned memory (arena pages + index). */
void text_literal_table_free(TextLiteralTable *table);

/*
 * Resolver adapter matching TextResolveLiteralUtf8Fn.
 * Pass the TextLiteralTable pointer as ctx.
 */
int text_literal_table_resolve_fn(uint32_t literal_id,
                                  const uint8_t **utf8_out,
                                  size_t *utf8_len_out,
                                  void *ctx);

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_TEXT_LITERAL_H */
