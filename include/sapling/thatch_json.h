/*
 * thatch_json.h — JSONL parser and jq-style cursor for Thatch packed regions
 *
 * Parses JSON text into the Thatch packed binary format, then provides
 * an ergonomic cursor API for navigating the result without any
 * deserialization or per-node allocation.
 *
 * Designed for portable C11 and universal Wasm compilation:
 * - No malloc per node — all data lives in ThatchRegion arena pages
 * - ThatchVal is a small value type (region pointer + cursor offset)
 * - Zero-copy string access via thatch_read_ptr
 * - O(1) subtree bypass via skip pointers (jq .[0] on a 10M-element
 *   array skips the first element in constant time, not linear)
 *
 * jq compatibility note: We reference jq (MIT license) for the path
 * expression syntax (.field, .[N], chaining). The implementation is
 * original — we operate on packed regions, not jq's internal AST.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */
#ifndef SAPLING_THATCH_JSON_H
#define SAPLING_THATCH_JSON_H

#include <stdint.h>
#include "sapling/thatch.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------ */
/* Packed Binary Tags (wire format)                                   */
/* ------------------------------------------------------------------ */
/* Layout per tag:
 *   TAG_NULL                                      1 byte
 *   TAG_TRUE                                      1 byte
 *   TAG_FALSE                                     1 byte
 *   TAG_INT      + int64_t (native byte order)     9 bytes
 *   TAG_DOUBLE   + double  (IEEE 754, native)     9 bytes
 *   TAG_STRING   + uint32_t(len) + UTF-8 bytes    5 + N bytes
 *   TAG_ARRAY    + uint32_t(skip) + elements...   5 + skip bytes
 *   TAG_OBJECT   + uint32_t(skip) + entries...    5 + skip bytes
 *   TAG_KEY      + uint32_t(len)  + UTF-8 bytes   5 + N bytes
 *                  (only inside objects, before each value)
 */
#define TJ_TAG_NULL    0x01
#define TJ_TAG_TRUE    0x02
#define TJ_TAG_FALSE   0x03
#define TJ_TAG_INT     0x04
#define TJ_TAG_DOUBLE  0x05
#define TJ_TAG_STRING  0x06
#define TJ_TAG_ARRAY   0x07
#define TJ_TAG_OBJECT  0x08
#define TJ_TAG_KEY     0x09

/* ------------------------------------------------------------------ */
/* Return codes (extends THATCH_OK/OOM/BOUNDS/INVALID)                */
/* ------------------------------------------------------------------ */
#define TJ_OK          THATCH_OK
#define TJ_OOM         THATCH_OOM
#define TJ_BOUNDS      THATCH_BOUNDS
#define TJ_INVALID     THATCH_INVALID
#define TJ_PARSE_ERROR 4   /* JSON syntax error                      */
#define TJ_NOT_FOUND   5   /* field / index does not exist           */
#define TJ_TYPE_ERROR  6   /* wrong JSON type for the operation      */

/* ------------------------------------------------------------------ */
/* JSON type enumeration                                              */
/* ------------------------------------------------------------------ */
typedef enum {
    TJ_TYPE_NULL    = TJ_TAG_NULL,
    TJ_TYPE_TRUE    = TJ_TAG_TRUE,
    TJ_TYPE_FALSE   = TJ_TAG_FALSE,
    TJ_TYPE_INT     = TJ_TAG_INT,
    TJ_TYPE_DOUBLE  = TJ_TAG_DOUBLE,
    TJ_TYPE_STRING  = TJ_TAG_STRING,
    TJ_TYPE_ARRAY   = TJ_TAG_ARRAY,
    TJ_TYPE_OBJECT  = TJ_TAG_OBJECT,
    TJ_TYPE_INVALID = 0
} TjType;

/* ------------------------------------------------------------------ */
/* ThatchVal — zero-allocation handle into a packed region            */
/* ------------------------------------------------------------------ */
/*
 * A ThatchVal is just a (region, offset) pair. It costs 12 bytes,
 * requires no allocation, and can be freely copied/passed by value.
 * Multiple vals can coexist pointing into the same region.
 */
typedef struct {
    const ThatchRegion *region;
    ThatchCursor        pos;   /* byte offset of this value's tag */
} ThatchVal;

/* ------------------------------------------------------------------ */
/* Parsing: JSON text → ThatchRegion                                  */
/* ------------------------------------------------------------------ */

/*
 * Parse a single JSON value from text and serialize it into a new
 * ThatchRegion.  On success, *val_out points to the root value.
 *
 * The region is allocated inside the transaction and tracked by the
 * Thatch subsystem for automatic cleanup on abort.
 *
 * On parse error, *err_pos (if non-NULL) receives the byte offset
 * of the first unexpected character.
 */
int tj_parse(SapTxnCtx *txn, const char *json, uint32_t len,
             ThatchRegion **region_out, ThatchVal *val_out,
             uint32_t *err_pos);

/*
 * Parse newline-delimited JSONL.  Calls on_value for each successfully
 * parsed line.  Each line gets its own ThatchRegion.  Blank lines are
 * silently skipped.  Returns TJ_OK if all lines parsed, or the first
 * error code encountered (remaining lines are not parsed).
 */
typedef int (*TjOnValue)(ThatchVal val, ThatchRegion *region,
                         uint32_t line_no, void *ctx);

int tj_parse_jsonl(SapTxnCtx *txn, const char *jsonl, uint32_t len,
                   TjOnValue on_value, void *ctx);

/* ------------------------------------------------------------------ */
/* Type inspection                                                    */
/* ------------------------------------------------------------------ */

TjType      tj_type(ThatchVal val);
int         tj_is_null(ThatchVal val);
int         tj_is_bool(ThatchVal val);
int         tj_is_number(ThatchVal val);
int         tj_is_string(ThatchVal val);
int         tj_is_array(ThatchVal val);
int         tj_is_object(ThatchVal val);

/* ------------------------------------------------------------------ */
/* Value extraction                                                   */
/* ------------------------------------------------------------------ */

/* Boolean: returns 0 or 1 in *out.  TJ_TYPE_ERROR if not bool. */
int tj_bool(ThatchVal val, int *out);

/* Integer: TJ_TYPE_ERROR if not TJ_TAG_INT. */
int tj_int(ThatchVal val, int64_t *out);

/* Double: accepts both INT and DOUBLE tags (int→double promotion). */
int tj_double(ThatchVal val, double *out);

/*
 * String: zero-copy.  *out points directly into the ThatchRegion's
 * arena page.  The pointer is valid for the lifetime of the region.
 * *len_out receives the byte length (not null-terminated).
 */
int tj_string(ThatchVal val, const char **out, uint32_t *len_out);

/* ------------------------------------------------------------------ */
/* Navigation (jq-style)                                              */
/* ------------------------------------------------------------------ */

/*
 * Object field access:  .field
 * Scans the object's packed keys, comparing against the given key.
 * O(n) in the number of keys but each non-matching value is skipped
 * in O(1) via its skip pointer.
 */
int tj_get(ThatchVal val, const char *key, uint32_t key_len, ThatchVal *out);

/*
 * Convenience: key is a C string (strlen computed internally).
 */
int tj_get_str(ThatchVal val, const char *key, ThatchVal *out);

/*
 * Array index:  .[N]
 * Skips over elements 0..N-1 in O(N) using skip pointers (each skip
 * is O(1) regardless of element complexity).
 */
int tj_index(ThatchVal val, uint32_t index, ThatchVal *out);

/*
 * Length of an array or object (number of elements or key-value pairs).
 */
int tj_length(ThatchVal val, uint32_t *len_out);

/* ------------------------------------------------------------------ */
/* Iteration                                                          */
/* ------------------------------------------------------------------ */

typedef struct {
    const ThatchRegion *region;
    ThatchCursor pos;      /* current position within container */
    ThatchCursor end;      /* one past last byte of container */
    uint32_t     index;    /* 0-based element counter */
} TjIter;

/*
 * Initialize an iterator over an array's elements.
 */
int tj_iter_array(ThatchVal val, TjIter *iter);

/*
 * Initialize an iterator over an object's key-value pairs.
 */
int tj_iter_object(ThatchVal val, TjIter *iter);

/*
 * Advance to the next array element.  Returns TJ_OK + sets *val_out,
 * or TJ_NOT_FOUND when iteration is exhausted.
 */
int tj_iter_next(TjIter *iter, ThatchVal *val_out);

/*
 * Advance to the next object entry.  Returns the key (zero-copy
 * pointer into the region) and the value.
 * Returns TJ_NOT_FOUND when iteration is exhausted.
 */
int tj_iter_next_kv(TjIter *iter,
                    const char **key_out, uint32_t *key_len_out,
                    ThatchVal *val_out);

/* ------------------------------------------------------------------ */
/* Path expressions (jq mini)                                         */
/* ------------------------------------------------------------------ */

/*
 * Evaluate a jq-style path expression against a value.
 *
 * Supported syntax:
 *   .              identity
 *   .field         object field (bare identifier)
 *   .["field"]     object field (quoted, for keys with special chars)
 *   .[N]           array index (unsigned integer)
 *   chaining:      .users[0].name   .data.items[2].id
 *
 * Returns TJ_OK on success, TJ_NOT_FOUND if any step fails,
 * TJ_TYPE_ERROR on type mismatch, or TJ_PARSE_ERROR for bad syntax.
 */
int tj_path(ThatchVal val, const char *path, ThatchVal *out);

/* ------------------------------------------------------------------ */
/* Utility: skip past a value                                         */
/* ------------------------------------------------------------------ */

/*
 * Compute the byte size of the value at cursor position `pos`.
 * Does not advance any cursor — purely a measurement.
 */
int tj_val_byte_size(const ThatchRegion *region, ThatchCursor pos,
                     uint32_t *size_out);

#ifdef __cplusplus
}
#endif

#endif /* SAPLING_THATCH_JSON_H */
