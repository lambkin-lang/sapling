# Text Design (Mutable Unicode over Seq)

`text.h` / `text.c` provide a mutable text container over `Seq`.

Runtime ABI details for literal/tree handle resolution live in
`docs/TEXT_RUNTIME_HANDLE_CONTRACT.md`.

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
- Resolved APIs (`*_resolved`) accept a runtime resolver callback for
  expanding non-codepoint handles into code points on demand.
- `text_expand_runtime_handle` is a built-in adapter over
  `TextRuntimeResolver` callbacks (`literal -> UTF-8`, `tree -> Text*`).
- `text_clone` provides shell-level copy-on-write sharing; writes detach.

## Semantics

- `text_length` reports leaf count, not expanded grapheme/UTF-8 byte length.
- With only `TEXT_HANDLE_CODEPOINT` leaves, `text_length` equals code-point
  length.
- UTF-8 encoding/length routines return `ERR_INVALID` if any non-codepoint
  handle is present.
- Resolved code-point/UTF-8 routines can operate on mixed-handle text without
  flattening:
  - `text_codepoint_length_resolved`
  - `text_get_codepoint_resolved`
  - `text_utf8_length_resolved`
  - `text_to_utf8_resolved`
- Runtime adapter expansion rejects:
  - UTF-8-invalid literal bytes
  - tree cycles (same tree id re-entered on the active path)
  - configured depth/visit guard exhaustion

## Runtime integration notes

For Wasm linear memory and native runtimes, this split allows the runtime to
use compact `u32` ids for literals/subtrees while preserving strict Unicode
paths where required.

If the runtime wants a single handle to represent multiple code points, index
and length by code point will require either:

- flattening to one leaf per code point before code-point APIs, or
- a future weighted-measure sequence variant where each handle carries an
  explicit code-point measure.

## Host-Only Allocation Policy

`text.c` intentionally keeps two host-convenience allocation paths:

1. `text_expand_runtime_handle(...)`:
   - default depth (`max_depth == 0` => 64) uses stack storage only
   - custom depths above 64 use a host-side malloc fallback
   - `SAP_NO_MALLOC` builds reject custom depths > 64 (`ERR_OOM`)

2. `text_to_utf8_full(...)`:
   - allocates output with malloc and returns it via `utf8_out`
   - caller frees with `free()`
   - `SAP_NO_MALLOC` builds return `ERR_INVALID` for this helper

Decision (current): keep these as host-only convenience paths. For no-malloc
runtimes, use `text_utf8_length_resolved(...)` plus
`text_to_utf8_resolved(...)` with caller-owned buffers and default-depth
resolver limits unless a dedicated arena-backed export path is introduced.
