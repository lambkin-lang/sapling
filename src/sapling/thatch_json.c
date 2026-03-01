/*
 * thatch_json.c — JSONL parser and jq-style cursor for Thatch packed regions
 *
 * Architecture:
 *   1. Recursive-descent parser writes into a ThatchRegion via the
 *      bump-allocator write API.  Arrays/objects use reserve_skip +
 *      commit_skip for backpatched lookahead markers.
 *   2. ThatchVal is a (region, cursor) pair — zero allocation.
 *   3. Navigation (tj_get, tj_index) reads tags/skips to locate
 *      children without deserializing siblings.
 *   4. Path expressions (".users[0].name") are interpreted left-to-right
 *      using tj_get/tj_index under the hood.
 *
 * Portability:
 *   - Pure C11, no POSIX/platform dependencies
 *   - No malloc in the hot path (all allocation via ThatchRegion)
 *   - Integer encoding is native byte-order (same machine reads and
 *     writes).  Cross-endian serialization is out of scope for v0.
 *   - strtod is used for floating-point parsing; Wasm runtimes provide
 *     a locale-free C runtime so this is safe.
 *
 * SPDX-License-Identifier: MIT
 * Copyright (c) 2026 lambkin-lang
 */

#include "sapling/thatch_json.h"
#include <string.h>
#include <stdlib.h>  /* strtod */
#include <limits.h>

/* ================================================================== */
/* Internal: JSON Parser                                              */
/* ================================================================== */

typedef struct {
    const char    *src;
    uint32_t       pos;
    uint32_t       len;
    ThatchRegion  *region;
} JParser;

/* ---------- helpers ---------- */

static void jp_skip_ws(JParser *p) {
    while (p->pos < p->len) {
        char c = p->src[p->pos];
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n')
            p->pos++;
        else
            break;
    }
}

static int jp_match(JParser *p, const char *lit, uint32_t lit_len) {
    if (p->pos + lit_len > p->len) return 0;
    if (memcmp(p->src + p->pos, lit, lit_len) != 0) return 0;
    p->pos += lit_len;
    return 1;
}

static int jp_peek(JParser *p) {
    return (p->pos < p->len) ? (unsigned char)p->src[p->pos] : -1;
}

/* ---------- forward declarations ---------- */

static int jp_parse_value(JParser *p);
static int jp_parse_string_impl(JParser *p, uint8_t tag);
static int jp_parse_number(JParser *p);
static int jp_parse_array(JParser *p);
static int jp_parse_object(JParser *p);

/* ---------- Unicode helpers ---------- */

static int hex_digit(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static int jp_read_hex4(JParser *p, uint32_t *out) {
    if (p->pos + 4 > p->len) return ERR_PARSE;
    *out = 0;
    for (int i = 0; i < 4; i++) {
        int d = hex_digit(p->src[p->pos++]);
        if (d < 0) return ERR_PARSE;
        *out = (*out << 4) | (uint32_t)d;
    }
    return ERR_OK;
}

/* Encode a Unicode codepoint as UTF-8 into buf[].  Returns bytes written. */
static int utf8_encode(uint32_t cp, uint8_t buf[4]) {
    if (cp <= 0x7F) {
        buf[0] = (uint8_t)cp;
        return 1;
    }
    if (cp <= 0x7FF) {
        buf[0] = (uint8_t)(0xC0 | (cp >> 6));
        buf[1] = (uint8_t)(0x80 | (cp & 0x3F));
        return 2;
    }
    if (cp <= 0xFFFF) {
        buf[0] = (uint8_t)(0xE0 | (cp >> 12));
        buf[1] = (uint8_t)(0x80 | ((cp >> 6) & 0x3F));
        buf[2] = (uint8_t)(0x80 | (cp & 0x3F));
        return 3;
    }
    if (cp <= 0x10FFFF) {
        buf[0] = (uint8_t)(0xF0 | (cp >> 18));
        buf[1] = (uint8_t)(0x80 | ((cp >> 12) & 0x3F));
        buf[2] = (uint8_t)(0x80 | ((cp >> 6) & 0x3F));
        buf[3] = (uint8_t)(0x80 | (cp & 0x3F));
        return 4;
    }
    return 0; /* invalid codepoint */
}

/* ---------- string parser ---------- */

/*
 * Parse a JSON string (opening " already expected at p->pos).
 * Writes TAG + uint32_t(decoded_len) + decoded UTF-8 bytes.
 *
 * tag is TJ_TAG_STRING for values or TJ_TAG_KEY for object keys.
 *
 * Strategy: write the tag and reserve 4 bytes for the length, then
 * decode escape sequences byte-by-byte into the region, then
 * backpatch the length.
 */
static int jp_parse_string_impl(JParser *p, uint8_t tag) {
    if (jp_peek(p) != '"') return ERR_PARSE;
    p->pos++; /* consume opening quote */

    int rc = thatch_write_tag(p->region, tag);
    if (rc) return rc;

    /* Reserve space for the decoded byte length */
    ThatchCursor len_loc;
    rc = thatch_reserve_skip(p->region, &len_loc);
    if (rc) return rc;

    ThatchCursor data_start = len_loc + sizeof(uint32_t);
    (void)data_start; /* used implicitly — thatch_commit_skip measures from len_loc */

    while (p->pos < p->len) {
        char c = p->src[p->pos];

        if (c == '"') {
            p->pos++; /* consume closing quote */
            /* Backpatch the decoded length */
            return thatch_commit_skip(p->region, len_loc);
        }

        if (c == '\\') {
            p->pos++;
            if (p->pos >= p->len) return ERR_PARSE;
            char esc = p->src[p->pos++];
            uint8_t byte = 0;
            switch (esc) {
                case '"':  byte = '"';  break;
                case '\\': byte = '\\'; break;
                case '/':  byte = '/';  break;
                case 'b':  byte = '\b'; break;
                case 'f':  byte = '\f'; break;
                case 'n':  byte = '\n'; break;
                case 'r':  byte = '\r'; break;
                case 't':  byte = '\t'; break;
                case 'u': {
                    uint32_t cp = 0;
                    rc = jp_read_hex4(p, &cp);
                    if (rc) return rc;
                    /* Handle surrogate pairs for U+10000..U+10FFFF */
                    if (cp >= 0xD800 && cp <= 0xDBFF) {
                        if (p->pos + 1 >= p->len ||
                            p->src[p->pos] != '\\' ||
                            p->src[p->pos + 1] != 'u')
                            return ERR_PARSE;
                        p->pos += 2; /* skip \u */
                        uint32_t lo = 0;
                        rc = jp_read_hex4(p, &lo);
                        if (rc) return rc;
                        if (lo < 0xDC00 || lo > 0xDFFF)
                            return ERR_PARSE;
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                    } else if (cp >= 0xDC00 && cp <= 0xDFFF) {
                        return ERR_PARSE; /* lone low surrogate */
                    }
                    uint8_t utf8[4];
                    int n = utf8_encode(cp, utf8);
                    if (n <= 0) return ERR_PARSE;
                    rc = thatch_write_data(p->region, utf8, (uint32_t)n);
                    if (rc) return rc;
                    continue;
                }
                default: return ERR_PARSE;
            }
            rc = thatch_write_data(p->region, &byte, 1);
            if (rc) return rc;
        } else if ((unsigned char)c < 0x20) {
            /* Control characters are not allowed unescaped in JSON */
            return ERR_PARSE;
        } else {
            rc = thatch_write_data(p->region, &c, 1);
            if (rc) return rc;
            p->pos++;
        }
    }
    return ERR_PARSE; /* unterminated string */
}

/* ---------- number parser ---------- */

static int jp_parse_number(JParser *p) {
    uint32_t start = p->pos;
    int is_float = 0;

    /* Optional minus */
    if (p->pos < p->len && p->src[p->pos] == '-') p->pos++;

    /* Integer part */
    if (p->pos >= p->len) return ERR_PARSE;
    if (p->src[p->pos] == '0') {
        p->pos++;
    } else if (p->src[p->pos] >= '1' && p->src[p->pos] <= '9') {
        while (p->pos < p->len && p->src[p->pos] >= '0' && p->src[p->pos] <= '9')
            p->pos++;
    } else {
        return ERR_PARSE;
    }

    /* Fractional part */
    if (p->pos < p->len && p->src[p->pos] == '.') {
        is_float = 1;
        p->pos++;
        if (p->pos >= p->len || p->src[p->pos] < '0' || p->src[p->pos] > '9')
            return ERR_PARSE;
        while (p->pos < p->len && p->src[p->pos] >= '0' && p->src[p->pos] <= '9')
            p->pos++;
    }

    /* Exponent */
    if (p->pos < p->len && (p->src[p->pos] == 'e' || p->src[p->pos] == 'E')) {
        is_float = 1;
        p->pos++;
        if (p->pos < p->len && (p->src[p->pos] == '+' || p->src[p->pos] == '-'))
            p->pos++;
        if (p->pos >= p->len || p->src[p->pos] < '0' || p->src[p->pos] > '9')
            return ERR_PARSE;
        while (p->pos < p->len && p->src[p->pos] >= '0' && p->src[p->pos] <= '9')
            p->pos++;
    }

    uint32_t num_len = p->pos - start;

    if (!is_float) {
        /* Try int64_t with overflow detection */
        int neg = (p->src[start] == '-');
        uint32_t i = neg ? start + 1 : start;
        int64_t val = 0;
        int overflow = 0;
        while (i < p->pos) {
            int d = p->src[i] - '0';
            if (neg) {
                /* Check: val * 10 - d >= INT64_MIN */
                if (val < (INT64_MIN + d) / 10) { overflow = 1; break; }
                val = val * 10 - d;
            } else {
                if (val > (INT64_MAX - d) / 10) { overflow = 1; break; }
                val = val * 10 + d;
            }
            i++;
        }
        if (!overflow) {
            int rc = thatch_write_tag(p->region, TJ_TAG_INT);
            if (rc) return rc;
            return thatch_write_data(p->region, &val, sizeof(val));
        }
        is_float = 1; /* fall through to double */
    }

    if (is_float) {
        /* Copy to NUL-terminated buffer for strtod */
        char buf[64];
        if (num_len >= sizeof(buf)) return ERR_PARSE;
        memcpy(buf, p->src + start, num_len);
        buf[num_len] = '\0';
        char *end;
        double dval = strtod(buf, &end);
        if ((uint32_t)(end - buf) != num_len) return ERR_PARSE;
        int rc = thatch_write_tag(p->region, TJ_TAG_DOUBLE);
        if (rc) return rc;
        return thatch_write_data(p->region, &dval, sizeof(dval));
    }
    return ERR_PARSE; /* unreachable */
}

/* ---------- array parser ---------- */

static int jp_parse_array(JParser *p) {
    p->pos++; /* consume '[' */
    int rc = thatch_write_tag(p->region, TJ_TAG_ARRAY);
    if (rc) return rc;

    ThatchCursor skip_loc;
    rc = thatch_reserve_skip(p->region, &skip_loc);
    if (rc) return rc;

    jp_skip_ws(p);
    if (jp_peek(p) == ']') {
        p->pos++;
        return thatch_commit_skip(p->region, skip_loc);
    }

    for (;;) {
        jp_skip_ws(p);
        rc = jp_parse_value(p);
        if (rc) return rc;

        jp_skip_ws(p);
        int c = jp_peek(p);
        if (c == ',') { p->pos++; continue; }
        if (c == ']') { p->pos++; return thatch_commit_skip(p->region, skip_loc); }
        return ERR_PARSE;
    }
}

/* ---------- object parser ---------- */

static int jp_parse_object(JParser *p) {
    p->pos++; /* consume '{' */
    int rc = thatch_write_tag(p->region, TJ_TAG_OBJECT);
    if (rc) return rc;

    ThatchCursor skip_loc;
    rc = thatch_reserve_skip(p->region, &skip_loc);
    if (rc) return rc;

    jp_skip_ws(p);
    if (jp_peek(p) == '}') {
        p->pos++;
        return thatch_commit_skip(p->region, skip_loc);
    }

    for (;;) {
        jp_skip_ws(p);
        /* Key must be a string — serialize with TJ_TAG_KEY */
        rc = jp_parse_string_impl(p, TJ_TAG_KEY);
        if (rc) return rc;

        jp_skip_ws(p);
        if (jp_peek(p) != ':') return ERR_PARSE;
        p->pos++;

        jp_skip_ws(p);
        rc = jp_parse_value(p);
        if (rc) return rc;

        jp_skip_ws(p);
        int c = jp_peek(p);
        if (c == ',') { p->pos++; continue; }
        if (c == '}') { p->pos++; return thatch_commit_skip(p->region, skip_loc); }
        return ERR_PARSE;
    }
}

/* ---------- top-level value dispatch ---------- */

static int jp_parse_value(JParser *p) {
    jp_skip_ws(p);
    int c = jp_peek(p);
    if (c < 0) return ERR_PARSE;

    switch (c) {
        case 'n':
            if (!jp_match(p, "null", 4)) return ERR_PARSE;
            return thatch_write_tag(p->region, TJ_TAG_NULL);
        case 't':
            if (!jp_match(p, "true", 4)) return ERR_PARSE;
            return thatch_write_tag(p->region, TJ_TAG_TRUE);
        case 'f':
            if (!jp_match(p, "false", 5)) return ERR_PARSE;
            return thatch_write_tag(p->region, TJ_TAG_FALSE);
        case '"':
            return jp_parse_string_impl(p, TJ_TAG_STRING);
        case '[':
            return jp_parse_array(p);
        case '{':
            return jp_parse_object(p);
        default:
            if (c == '-' || (c >= '0' && c <= '9'))
                return jp_parse_number(p);
            return ERR_PARSE;
    }
}

/* ================================================================== */
/* Public: Parsing API                                                */
/* ================================================================== */

int tj_parse(SapTxnCtx *txn, const char *json, uint32_t len,
             ThatchRegion **region_out, ThatchVal *val_out,
             uint32_t *err_pos) {
    if (!txn || !json || !region_out || !val_out) return ERR_INVALID;

    ThatchRegion *region = NULL;
    int rc = thatch_region_new(txn, &region);
    if (rc) return rc;

    JParser p = { .src = json, .pos = 0, .len = len, .region = region };
    rc = jp_parse_value(&p);
    if (rc) {
        if (err_pos) *err_pos = p.pos;
        thatch_region_release(txn, region);
        return rc;
    }

    /* Ensure no trailing non-whitespace */
    jp_skip_ws(&p);
    if (p.pos != p.len) {
        if (err_pos) *err_pos = p.pos;
        thatch_region_release(txn, region);
        return ERR_PARSE;
    }

    *region_out = region;
    val_out->region = region;
    val_out->pos = 0;
    return ERR_OK;
}

int tj_parse_jsonl(SapTxnCtx *txn, const char *jsonl, uint32_t len,
                   TjOnValue on_value, void *ctx) {
    if (!txn || !jsonl || !on_value) return ERR_INVALID;

    uint32_t line_no = 0;
    uint32_t pos = 0;

    while (pos < len) {
        /* Find end of line */
        uint32_t line_start = pos;
        while (pos < len && jsonl[pos] != '\n') pos++;
        uint32_t line_end = pos;
        if (pos < len) pos++; /* skip newline */

        /* Trim trailing CR for CRLF */
        if (line_end > line_start && jsonl[line_end - 1] == '\r')
            line_end--;

        /* Skip blank lines */
        uint32_t trimmed = line_start;
        while (trimmed < line_end &&
               (jsonl[trimmed] == ' ' || jsonl[trimmed] == '\t'))
            trimmed++;
        if (trimmed == line_end) { line_no++; continue; }

        ThatchRegion *region = NULL;
        ThatchVal val;
        uint32_t err_pos;
        int rc = tj_parse(txn, jsonl + line_start, line_end - line_start,
                          &region, &val, &err_pos);
        if (rc) return rc;

        rc = on_value(val, region, line_no, ctx);
        if (rc) return rc;
        line_no++;
    }
    return ERR_OK;
}

/* ================================================================== */
/* Public: Type Inspection                                            */
/* ================================================================== */

TjType tj_type(ThatchVal val) {
    uint8_t tag = 0;
    ThatchCursor c = val.pos;
    if (thatch_read_tag(val.region, &c, &tag) != ERR_OK) return TJ_TYPE_INVALID;
    switch (tag) {
        case TJ_TAG_NULL:   return TJ_TYPE_NULL;
        case TJ_TAG_TRUE:   return TJ_TYPE_TRUE;
        case TJ_TAG_FALSE:  return TJ_TYPE_FALSE;
        case TJ_TAG_INT:    return TJ_TYPE_INT;
        case TJ_TAG_DOUBLE: return TJ_TYPE_DOUBLE;
        case TJ_TAG_STRING: return TJ_TYPE_STRING;
        case TJ_TAG_ARRAY:  return TJ_TYPE_ARRAY;
        case TJ_TAG_OBJECT: return TJ_TYPE_OBJECT;
        default:            return TJ_TYPE_INVALID;
    }
}

int tj_is_null(ThatchVal val)   { return tj_type(val) == TJ_TYPE_NULL; }
int tj_is_bool(ThatchVal val)   { TjType t = tj_type(val); return t == TJ_TYPE_TRUE || t == TJ_TYPE_FALSE; }
int tj_is_number(ThatchVal val) { TjType t = tj_type(val); return t == TJ_TYPE_INT || t == TJ_TYPE_DOUBLE; }
int tj_is_string(ThatchVal val) { return tj_type(val) == TJ_TYPE_STRING; }
int tj_is_array(ThatchVal val)  { return tj_type(val) == TJ_TYPE_ARRAY; }
int tj_is_object(ThatchVal val) { return tj_type(val) == TJ_TYPE_OBJECT; }

/* ================================================================== */
/* Public: Value Extraction                                           */
/* ================================================================== */

int tj_bool(ThatchVal val, int *out) {
    if (!out) return ERR_INVALID;
    TjType t = tj_type(val);
    if (t == TJ_TYPE_TRUE)  { *out = 1; return ERR_OK; }
    if (t == TJ_TYPE_FALSE) { *out = 0; return ERR_OK; }
    return ERR_TYPE;
}

int tj_int(ThatchVal val, int64_t *out) {
    if (!out) return ERR_INVALID;
    uint8_t tag;
    ThatchCursor c = val.pos;
    if (thatch_read_tag(val.region, &c, &tag) != ERR_OK) return ERR_INVALID;
    if (tag != TJ_TAG_INT) return ERR_TYPE;
    return thatch_read_data(val.region, &c, sizeof(int64_t), out);
}

int tj_double(ThatchVal val, double *out) {
    if (!out) return ERR_INVALID;
    uint8_t tag;
    ThatchCursor c = val.pos;
    if (thatch_read_tag(val.region, &c, &tag) != ERR_OK) return ERR_INVALID;
    if (tag == TJ_TAG_DOUBLE) {
        return thatch_read_data(val.region, &c, sizeof(double), out);
    }
    if (tag == TJ_TAG_INT) {
        int64_t iv;
        int rc = thatch_read_data(val.region, &c, sizeof(int64_t), &iv);
        if (rc) return rc;
        *out = (double)iv;
        return ERR_OK;
    }
    return ERR_TYPE;
}

int tj_string(ThatchVal val, const char **out, uint32_t *len_out) {
    if (!out || !len_out) return ERR_INVALID;
    uint8_t tag;
    ThatchCursor c = val.pos;
    if (thatch_read_tag(val.region, &c, &tag) != ERR_OK) return ERR_INVALID;
    if (tag != TJ_TAG_STRING) return ERR_TYPE;

    uint32_t slen;
    int rc = thatch_read_data(val.region, &c, sizeof(uint32_t), &slen);
    if (rc) return rc;

    const void *ptr;
    rc = thatch_read_ptr(val.region, &c, slen, &ptr);
    if (rc) return rc;

    *out = (const char *)ptr;
    *len_out = slen;
    return ERR_OK;
}

/* ================================================================== */
/* Public: tj_val_byte_size                                           */
/* ================================================================== */

int tj_val_byte_size(const ThatchRegion *region, ThatchCursor pos,
                     uint32_t *size_out) {
    if (!region || !size_out) return ERR_INVALID;
    uint8_t tag;
    ThatchCursor c = pos;
    int rc = thatch_read_tag(region, &c, &tag);
    if (rc) return rc;

    switch (tag) {
        case TJ_TAG_NULL:
        case TJ_TAG_TRUE:
        case TJ_TAG_FALSE:
            *size_out = 1;
            return ERR_OK;
        case TJ_TAG_INT:
        case TJ_TAG_DOUBLE:
            *size_out = 1 + 8;
            return ERR_OK;
        case TJ_TAG_STRING:
        case TJ_TAG_KEY: {
            uint32_t slen;
            rc = thatch_read_data(region, &c, sizeof(uint32_t), &slen);
            if (rc) return rc;
            *size_out = 1 + 4 + slen;
            return ERR_OK;
        }
        case TJ_TAG_ARRAY:
        case TJ_TAG_OBJECT: {
            uint32_t skip;
            rc = thatch_read_data(region, &c, sizeof(uint32_t), &skip);
            if (rc) return rc;
            *size_out = 1 + 4 + skip;
            return ERR_OK;
        }
        default:
            return ERR_INVALID;
    }
}

/* Internal: advance cursor past one value */
static int skip_value(const ThatchRegion *region, ThatchCursor *cursor) {
    uint32_t sz;
    int rc = tj_val_byte_size(region, *cursor, &sz);
    if (rc) return rc;
    *cursor += sz;
    return ERR_OK;
}

/* Internal: advance cursor past one KEY tag (tag + len + bytes) */
static int skip_key(const ThatchRegion *region, ThatchCursor *cursor) {
    uint8_t tag;
    ThatchCursor c = *cursor;
    int rc = thatch_read_tag(region, &c, &tag);
    if (rc) return rc;
    if (tag != TJ_TAG_KEY) return ERR_TYPE;
    uint32_t klen;
    rc = thatch_read_data(region, &c, sizeof(uint32_t), &klen);
    if (rc) return rc;
    c += klen;
    *cursor = c;
    return ERR_OK;
}

/* ================================================================== */
/* Public: Navigation                                                 */
/* ================================================================== */

int tj_get(ThatchVal val, const char *key, uint32_t key_len, ThatchVal *out) {
    if (!key || !out) return ERR_INVALID;
    uint8_t tag;
    ThatchCursor c = val.pos;
    int rc = thatch_read_tag(val.region, &c, &tag);
    if (rc) return rc;
    if (tag != TJ_TAG_OBJECT) return ERR_TYPE;

    uint32_t skip;
    rc = thatch_read_data(val.region, &c, sizeof(uint32_t), &skip);
    if (rc) return rc;

    ThatchCursor end = c + skip;

    while (c < end) {
        /* Read the key */
        uint8_t ktag;
        ThatchCursor key_start = c;
        rc = thatch_read_tag(val.region, &c, &ktag);
        if (rc) return rc;
        if (ktag != TJ_TAG_KEY) return ERR_INVALID;

        uint32_t klen;
        rc = thatch_read_data(val.region, &c, sizeof(uint32_t), &klen);
        if (rc) return rc;

        /* Zero-copy key comparison */
        const void *kptr;
        rc = thatch_read_ptr(val.region, &c, klen, &kptr);
        if (rc) return rc;

        /* c now points at the value */
        if (klen == key_len && memcmp(kptr, key, key_len) == 0) {
            out->region = val.region;
            out->pos = c;
            return ERR_OK;
        }

        /* Skip the value */
        rc = skip_value(val.region, &c);
        if (rc) return rc;
        (void)key_start;
    }

    return ERR_NOT_FOUND;
}

int tj_get_str(ThatchVal val, const char *key, ThatchVal *out) {
    if (!key) return ERR_INVALID;
    return tj_get(val, key, (uint32_t)strlen(key), out);
}

int tj_index(ThatchVal val, uint32_t index, ThatchVal *out) {
    if (!out) return ERR_INVALID;
    uint8_t tag;
    ThatchCursor c = val.pos;
    int rc = thatch_read_tag(val.region, &c, &tag);
    if (rc) return rc;
    if (tag != TJ_TAG_ARRAY) return ERR_TYPE;

    uint32_t skip;
    rc = thatch_read_data(val.region, &c, sizeof(uint32_t), &skip);
    if (rc) return rc;

    ThatchCursor end = c + skip;

    for (uint32_t i = 0; c < end; i++) {
        if (i == index) {
            out->region = val.region;
            out->pos = c;
            return ERR_OK;
        }
        rc = skip_value(val.region, &c);
        if (rc) return rc;
    }

    return ERR_NOT_FOUND;
}

int tj_length(ThatchVal val, uint32_t *len_out) {
    if (!len_out) return ERR_INVALID;
    uint8_t tag;
    ThatchCursor c = val.pos;
    int rc = thatch_read_tag(val.region, &c, &tag);
    if (rc) return rc;

    if (tag != TJ_TAG_ARRAY && tag != TJ_TAG_OBJECT) return ERR_TYPE;

    uint32_t skip;
    rc = thatch_read_data(val.region, &c, sizeof(uint32_t), &skip);
    if (rc) return rc;

    ThatchCursor end = c + skip;
    uint32_t count = 0;

    while (c < end) {
        if (tag == TJ_TAG_OBJECT) {
            rc = skip_key(val.region, &c);
            if (rc) return rc;
        }
        rc = skip_value(val.region, &c);
        if (rc) return rc;
        count++;
    }

    *len_out = count;
    return ERR_OK;
}

/* ================================================================== */
/* Public: Iteration                                                  */
/* ================================================================== */

int tj_iter_array(ThatchVal val, TjIter *iter) {
    if (!iter) return ERR_INVALID;
    uint8_t tag;
    ThatchCursor c = val.pos;
    int rc = thatch_read_tag(val.region, &c, &tag);
    if (rc) return rc;
    if (tag != TJ_TAG_ARRAY) return ERR_TYPE;

    uint32_t skip;
    rc = thatch_read_data(val.region, &c, sizeof(uint32_t), &skip);
    if (rc) return rc;

    iter->region = val.region;
    iter->pos = c;
    iter->end = c + skip;
    iter->index = 0;
    return ERR_OK;
}

int tj_iter_object(ThatchVal val, TjIter *iter) {
    if (!iter) return ERR_INVALID;
    uint8_t tag;
    ThatchCursor c = val.pos;
    int rc = thatch_read_tag(val.region, &c, &tag);
    if (rc) return rc;
    if (tag != TJ_TAG_OBJECT) return ERR_TYPE;

    uint32_t skip;
    rc = thatch_read_data(val.region, &c, sizeof(uint32_t), &skip);
    if (rc) return rc;

    iter->region = val.region;
    iter->pos = c;
    iter->end = c + skip;
    iter->index = 0;
    return ERR_OK;
}

int tj_iter_next(TjIter *iter, ThatchVal *val_out) {
    if (!iter || !val_out) return ERR_INVALID;
    if (iter->pos >= iter->end) return ERR_NOT_FOUND;

    val_out->region = iter->region;
    val_out->pos = iter->pos;

    int rc = skip_value(iter->region, &iter->pos);
    if (rc) return rc;

    iter->index++;
    return ERR_OK;
}

int tj_iter_next_kv(TjIter *iter,
                    const char **key_out, uint32_t *key_len_out,
                    ThatchVal *val_out) {
    if (!iter || !key_out || !key_len_out || !val_out) return ERR_INVALID;
    if (iter->pos >= iter->end) return ERR_NOT_FOUND;

    /* Read key tag */
    uint8_t tag;
    ThatchCursor c = iter->pos;
    int rc = thatch_read_tag(iter->region, &c, &tag);
    if (rc) return rc;
    if (tag != TJ_TAG_KEY) return ERR_INVALID;

    /* Read key length + zero-copy pointer */
    uint32_t klen;
    rc = thatch_read_data(iter->region, &c, sizeof(uint32_t), &klen);
    if (rc) return rc;

    const void *kptr;
    rc = thatch_read_ptr(iter->region, &c, klen, &kptr);
    if (rc) return rc;

    *key_out = (const char *)kptr;
    *key_len_out = klen;

    /* Value starts at c */
    val_out->region = iter->region;
    val_out->pos = c;

    /* Advance past value */
    rc = skip_value(iter->region, &c);
    if (rc) return rc;

    iter->pos = c;
    iter->index++;
    return ERR_OK;
}

/* ================================================================== */
/* Public: Path Expressions                                           */
/* ================================================================== */

/*
 * Mini jq path interpreter.  Grammar:
 *
 *   path  := '.' rest
 *   rest  := ident index_part rest
 *          | '[' expr ']' rest
 *          | ε
 *   ident := [a-zA-Z_][a-zA-Z0-9_]*
 *   expr  := integer | '"' chars '"'
 *   index_part := '[' expr ']' | ε
 *
 * Examples:
 *   .                   → identity
 *   .name               → tj_get_str(val, "name")
 *   .users[0]           → tj_get_str(val, "users"), then tj_index(_, 0)
 *   .users[0].name      → chained
 *   .["odd key"]        → tj_get(val, "odd key", 7)
 */
int tj_path(ThatchVal val, const char *path, ThatchVal *out) {
    if (!path || !out) return ERR_INVALID;
    ThatchVal cur = val;
    const char *p = path;

    if (*p != '.') return ERR_PARSE;
    p++;

    /* '.' alone is identity */
    if (*p == '\0') { *out = cur; return ERR_OK; }

    while (*p != '\0') {
        if (*p == '.') {
            p++;
            if (*p == '\0') return ERR_PARSE; /* trailing dot */
        }

        if (*p == '[') {
            p++; /* consume '[' */
            if (*p == '"') {
                /* Quoted key: .["field"] */
                p++; /* consume opening quote */
                const char *key_start = p;
                while (*p && *p != '"') p++;
                if (*p != '"') return ERR_PARSE;
                uint32_t klen = (uint32_t)(p - key_start);
                p++; /* consume closing quote */
                if (*p != ']') return ERR_PARSE;
                p++; /* consume ']' */
                int rc = tj_get(cur, key_start, klen, &cur);
                if (rc) return rc;
            } else if (*p >= '0' && *p <= '9') {
                /* Array index: .[N] */
                uint32_t idx = 0;
                while (*p >= '0' && *p <= '9') {
                    uint32_t digit = (uint32_t)(*p - '0');
                    if (idx > (UINT32_MAX - digit) / 10)
                        return ERR_PARSE; /* overflow */
                    idx = idx * 10 + digit;
                    p++;
                }
                if (*p != ']') return ERR_PARSE;
                p++; /* consume ']' */
                int rc = tj_index(cur, idx, &cur);
                if (rc) return rc;
            } else {
                return ERR_PARSE;
            }
        } else if ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || *p == '_') {
            /* Bare identifier: .field */
            const char *id_start = p;
            while ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                   (*p >= '0' && *p <= '9') || *p == '_')
                p++;
            uint32_t id_len = (uint32_t)(p - id_start);
            int rc = tj_get(cur, id_start, id_len, &cur);
            if (rc) return rc;

            /* Optional immediate index: .field[N] */
            while (*p == '[') {
                p++; /* consume '[' */
                if (*p >= '0' && *p <= '9') {
                    uint32_t idx = 0;
                    while (*p >= '0' && *p <= '9') {
                        uint32_t digit = (uint32_t)(*p - '0');
                        if (idx > (UINT32_MAX - digit) / 10)
                            return ERR_PARSE; /* overflow */
                        idx = idx * 10 + digit;
                        p++;
                    }
                    if (*p != ']') return ERR_PARSE;
                    p++;
                    rc = tj_index(cur, idx, &cur);
                    if (rc) return rc;
                } else if (*p == '"') {
                    p++;
                    const char *ks = p;
                    while (*p && *p != '"') p++;
                    if (*p != '"') return ERR_PARSE;
                    uint32_t kl = (uint32_t)(p - ks);
                    p++;
                    if (*p != ']') return ERR_PARSE;
                    p++;
                    rc = tj_get(cur, ks, kl, &cur);
                    if (rc) return rc;
                } else {
                    return ERR_PARSE;
                }
            }
        } else {
            return ERR_PARSE;
        }
    }

    *out = cur;
    return ERR_OK;
}
