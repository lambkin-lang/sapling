# Thatch JSON Wire Format & Path Grammar

## Wire Format

JSON values are serialized into ThatchRegion pages using a tag-length-value
encoding.  Each value starts with a 1-byte tag, optionally followed by
fixed-size or length-prefixed payload data.

### Tag Layout

| Tag | Name | Payload | Total Size |
|-----|------|---------|------------|
| `0x01` | NULL | none | 1 byte |
| `0x02` | TRUE | none | 1 byte |
| `0x03` | FALSE | none | 1 byte |
| `0x04` | INT | `int64_t` (native byte order) | 9 bytes |
| `0x05` | DOUBLE | `double` (IEEE 754, native byte order) | 9 bytes |
| `0x06` | STRING | `uint32_t` length + UTF-8 bytes | 5 + N bytes |
| `0x07` | ARRAY | `uint32_t` skip + elements... | 5 + skip bytes |
| `0x08` | OBJECT | `uint32_t` skip + entries... | 5 + skip bytes |
| `0x09` | KEY | `uint32_t` length + UTF-8 bytes | 5 + N bytes |

### Byte Order

All multi-byte integers (`int64_t`, `double`, `uint32_t` lengths and skips)
are stored in **native byte order**.  The format is designed for same-machine
read/write within a single arena lifetime.  Cross-endian serialization is
explicitly out of scope for v0.

### Skip Pointers

Arrays and objects store a 4-byte skip value immediately after the tag.  The
skip value is the number of bytes of child content following the skip slot
itself.  This enables O(1) bypass of entire subtrees during navigation.

Skip values are backpatched: `thatch_reserve_skip` allocates the 4-byte slot,
the children are written, then `thatch_commit_skip` writes the final length.

### Object Entries

Inside an object, entries are stored as alternating KEY-VALUE pairs:
```
TAG_OBJECT | skip:u32 | TAG_KEY | len:u32 | key_bytes | value | TAG_KEY | ...
```

### Strings

Strings are NOT null-terminated in the wire format.  The `uint32_t` length
prefix gives the byte count.  `tj_string` returns a zero-copy pointer directly
into the region page.

## Path Expression Grammar

```
path     = "." rest*
rest     = field | index | bracket
field    = IDENT ( "[" index_body "]" )*
index    = "[" index_body "]"
bracket  = "[" ( quoted_key | integer ) "]"

IDENT    = [a-zA-Z_][a-zA-Z0-9_]*
integer  = [0-9]+                   (must fit uint32_t without overflow)
quoted_key = '"' chars '"'
```

### Examples

| Expression | Meaning |
|-----------|---------|
| `.` | Identity (root value) |
| `.name` | Object field "name" |
| `.[0]` | Array element 0 |
| `.users[0].name` | Chained: field → index → field |
| `.["special-key"]` | Quoted key for non-identifier field names |

### Overflow Protection

Array indices must fit in `uint32_t`.  Parsing rejects indices that would
overflow (e.g., `.[4294967296]`) with `TJ_PARSE_ERROR` rather than silently
wrapping.

## Failure Semantics

### Parse Errors

When `tj_parse` encounters a syntax error:
1. `*err_pos` (if non-NULL) receives the byte offset of the error.
2. The partially-written region is released via `thatch_region_release`.
3. No region or value is returned to the caller.
4. The arena page count returns to baseline (no leak on repeated failures).

### JSONL

`tj_parse_jsonl` parses newline-delimited JSON.  Each line gets its own
ThatchRegion.  On first error, parsing stops and the error code is returned.
Blank lines are silently skipped.

## Versioning

This is the v0 wire format.  There are no stability guarantees across Sapling
versions.  The format is intended for in-process, same-session use within a
single arena lifetime.
