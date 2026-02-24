# Text Design (Mutable Unicode over Seq)

`text.h` / `text.c` provide a mutable text container over `Seq`.

## Storage model

`Text` stores `uint32_t` tagged handles:

- `TEXT_HANDLE_CODEPOINT` (tag `0`): payload is a Unicode scalar value.
- `TEXT_HANDLE_LITERAL` (tag `1`): payload is a runtime literal-table id.
- `TEXT_HANDLE_TREE` (tag `2`): payload is a runtime subtree/COW id.
- `TEXT_HANDLE_RESERVED` (tag `3`): currently rejected by mutating APIs.

Handle layout:

- top 2 bits: tag
- low 30 bits: payload

## API split

- Code-point APIs (`text_push_back`, `text_get`, `text_to_utf8`, etc.) are
  strict wrappers that only operate on code-point leaves.
- Raw handle APIs (`text_*_handle`) allow mixed leaf kinds.

## Semantics

- `text_length` reports leaf count, not expanded grapheme/UTF-8 byte length.
- With only `TEXT_HANDLE_CODEPOINT` leaves, `text_length` equals code-point
  length.
- UTF-8 encoding/length routines return `SEQ_INVALID` if any non-codepoint
  handle is present.

## Runtime integration notes

For Wasm linear memory and native runtimes, this split allows the runtime to
use compact `u32` ids for literals/subtrees while preserving strict Unicode
paths where required.

If the runtime wants a single handle to represent multiple code points, index
and length by code point will require either:

- flattening to one leaf per code point before code-point APIs, or
- a future weighted-measure sequence variant where each handle carries an
  explicit code-point measure.
