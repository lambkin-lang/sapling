# Text Runtime Handle Contract

This document defines the runtime-facing contract for `TextHandle` leaves used
by `text.h` / `text.c`.

## Handle Encoding

`TextHandle` is a `uint32_t` value:

- high 2 bits: tag
- low 30 bits: payload

Tags:

- `TEXT_HANDLE_CODEPOINT` (`0`): payload is Unicode scalar value.
- `TEXT_HANDLE_LITERAL` (`1`): payload is runtime literal-table id.
- `TEXT_HANDLE_TREE` (`2`): payload is runtime subtree/COW id.
- `TEXT_HANDLE_RESERVED` (`3`): currently invalid for storage.

## ID Domains

Literal and tree payload ids are opaque to the `text` module.

Recommended runtime policy:

- keep ids stable for the lifetime of any text that may reference them
- avoid id reuse while live references may still exist
- treat `0` as a valid id only if runtime tables define it explicitly

## Resolver Contract

`text_expand_runtime_handle(...)` uses a `TextRuntimeResolver` with two
callbacks:

- `resolve_literal_utf8_fn(literal_id, &bytes, &len, ctx)`
- `resolve_tree_text_fn(tree_id, &text_ptr, ctx)`

Callback requirements:

- return `SEQ_OK` on success, `SEQ_*` error otherwise
- returned literal bytes must be valid UTF-8
- returned `Text*` must be valid (`text_is_valid(...) == 1`)
- callback-owned data must remain valid until callback returns
- returned tree text is read-only from the adapter’s perspective

## Expansion Guards

`text_expand_runtime_handle(...)` enforces:

- cycle rejection: repeated tree id on active recursion path => `SEQ_INVALID`
- depth bound: `max_tree_depth` (`0` uses default 64)
- visit bound: `max_tree_visits` (`0` uses default 4096)

Guard violations return `SEQ_INVALID`.

## Ownership and COW

- `Text` now supports shell-level COW via `text_clone`.
- resolver-returned tree pointers are borrowed, not consumed.
- mutations on cloned texts detach before write; resolver trees are unaffected.

## Wasm Linear Memory Notes

For Universal Wasm targets, this contract avoids embedding raw pointers in
leaves:

- leaves carry stable `u32` ids
- literals can reference data-section UTF-8
- tree ids can reference runtime-managed text objects/tables

This keeps the element ABI portable between native and linear-memory runtimes.
