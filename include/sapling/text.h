/*
 * text.h — mutable code-point text built on top of Seq
 *
 * Text stores Unicode code points as uint32_t values.
 * Operations return SEQ_* status codes from seq.h.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_TEXT_H
#define SAPLING_TEXT_H

#include "sapling/seq.h"

#include <stddef.h>
#include <stdint.h>

/* Opaque text handle. */
typedef struct Text Text;

/* Lifecycle */
Text *text_new(void);
Text *text_new_with_allocator(const SeqAllocator *allocator);
void  text_free(Text *text);
int   text_is_valid(const Text *text);
int   text_reset(Text *text);

/* Length in code points. */
size_t text_length(const Text *text);

/* End operations */
int text_push_front(Text *text, uint32_t codepoint);
int text_push_back(Text *text, uint32_t codepoint);
int text_pop_front(Text *text, uint32_t *out);
int text_pop_back(Text *text, uint32_t *out);

/* Indexed access/update */
int text_get(const Text *text, size_t idx, uint32_t *out);
int text_set(Text *text, size_t idx, uint32_t codepoint);
int text_insert(Text *text, size_t idx, uint32_t codepoint);

/*
 * text_delete — remove one code point at idx.
 * If out is non-NULL, the removed code point is stored in *out.
 */
int text_delete(Text *text, size_t idx, uint32_t *out);

/* Structural operations */
int text_concat(Text *dest, Text *src);
int text_split_at(Text *text, size_t idx, Text **left_out, Text **right_out);

#endif /* SAPLING_TEXT_H */
