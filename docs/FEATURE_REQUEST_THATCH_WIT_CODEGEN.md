# Feature Request: Thatch-Based WIT Schema Codegen

## Summary

Extend `tools/wit_schema_codegen.py` to produce Thatch-native cursor accessors
for all WIT types, replacing the current `unknown_layout` placeholders with
generated encode/write and decode/read functions that operate directly on
ThatchRegion pages. This eliminates the gap between the WIT schema and the
runtime, proves out the Thatch serialization model as a general-purpose internal
representation, and acts as a cross-validation point for the Lambkin compiler's
own code generation.

## Motivation

### The current gap

The codegen today handles flat primitive fields (`s64`, `bool`, `utf8` as
offset+length) but emits `uint64_t foo_unknown_layout` for any compound WIT
type — records, variants, enums, flags, options, and tuples. This affects
DBIs 1–6 (inbox, outbox, leases, timers, dedupe, dead_letter). The generated
C structs cannot be used to read or write these DBI records, and the validation
functions check against wrong sizes.

At runtime, `wire_v0.c` works around this with 400+ lines of hand-written
encode/decode that duplicates the schema knowledge embedded in the WIT file.
Every time the schema changes, both the WIT and the wire code must be updated
in lockstep — a maintenance and correctness risk.

### Why Thatch, not the Canonical ABI

The Component Model's Canonical ABI is designed for module boundaries — it
flattens types into function parameter lists and linear-memory layouts suitable
for cross-module calls. That flattening is the right choice at strict
interoperability boundaries, but it is the wrong choice for internal
representations:

- It forces fixed-offset struct layouts, which means every optional field
  (`option<T>`) must reserve space for the discriminant and the full payload
  even when absent.
- Variants are flattened into the union of all case payloads, wasting space on
  the smaller cases.
- Variable-length fields (`list<u8>`, `utf8`) become offset+length pairs that
  point elsewhere in linear memory, requiring a second indirection to reach the
  actual data.
- The flattened layout is optimized for ABI stability across modules, not for
  traversal speed within a single process.

Thatch is designed for exactly the internal case:

- Data is serialized sequentially with tag bytes, so optional fields cost 1 byte
  when absent (tag only) versus 1 + payload bytes when present.
- Variants are naturally tag-dispatched — read the tag, branch, interpret the
  payload for that case. No wasted space for other cases.
- Variable-length data is inline (length-prefixed bytes follow the tag), so
  traversal is a single forward cursor walk with no pointer chasing.
- Skip pointers on compound types (records, variants with large payloads) enable
  O(1) bypass of subtrees the reader doesn't care about.
- The whole representation lives in a single arena-backed page, so it can be
  written into a DBI value directly and read back with zero allocation.

### Why this proves out the Lambkin compiler approach

Lambkin will need to generate exactly this kind of code: given a type definition,
emit serialization into Thatch regions and cursor-based accessors for
deserialization. The WIT codegen is a simpler, constrained version of the same
problem — the type universe is small (WIT primitives, records, variants, enums,
flags, options, tuples, lists), the target is C, and the correctness can be
validated against the existing hand-written `wire_v0` encode/decode.

If the codegen can produce correct, efficient Thatch accessors from WIT
declarations, it validates three things simultaneously:

1. The Thatch tag/skip layout is expressive enough for arbitrary algebraic types.
2. The cursor-accessor pattern generates code that is competitive with
   hand-written encode/decode.
3. The approach generalizes to the Lambkin compiler, where the type universe is
   richer but the serialization target is the same.

## Design

### Phase 1: Type registry

The codegen must parse the full WIT type namespace, not just `dbiN-*-key/value`
records. Today `RECORD_BLOCK_RE` extracts record bodies and `FIELD_RE` extracts
fields, but field types are matched only against a hardcoded primitive list in
`map_wit_type`. Compound type references like `message-envelope` fall through
to the `unknown_layout` fallback.

Add a type registry that maps every named type to its definition:

```python
@dataclass
class WitTypeDef:
    kind: str          # "record", "variant", "enum", "flags", "type-alias", "tuple"
    name: str
    fields: list       # record fields, variant cases, enum values, flag bits, etc.

class WitTypeRegistry:
    def __init__(self, wit_text: str): ...
    def resolve(self, type_name: str) -> WitTypeDef: ...
    def is_primitive(self, type_name: str) -> bool: ...
    def is_fixed_size(self, type_name: str) -> bool: ...
```

Parsing requirements (new, beyond current capability):

| WIT construct | Example | What to parse |
|---|---|---|
| `record` | `record message-envelope { ... }` | Already parsed; needs recursive field resolution |
| `variant` | `variant lease-state { queued, leased(lease-info), ... }` | Case names + optional payload types |
| `enum` | `enum message-kind { command, event, timer }` | Case names (no payloads) |
| `flags` | `flags message-flags { durable, high-priority, ... }` | Bit names |
| `type` alias | `type worker-id = s64` | Target type |
| `tuple` | `type msg-route = tuple<worker-id, timestamp>` | Element types |
| `option<T>` | `from-worker: option<worker-id>` | Inner type |
| `list<T>` | `type bytes = list<u8>` | Element type |
| `result<T, E>` | `type apply-result = result<message-ack, tx-error>` | Ok/Err types |

### Phase 2: Thatch tag assignment

Define a tag byte namespace for WIT types, analogous to the JSON tag layout in
`THATCH_JSON_FORMAT.md` but for the WIT type universe. This is the internal
representation — not the Canonical ABI, not the wire format. It is used only
within a single arena lifetime, same as Thatch JSON.

Proposed tag layout:

| Tag | Name | Payload | Notes |
|---|---|---|---|
| `0x10` | RECORD | skip:u32 + fields... | Skip pointer for O(1) bypass |
| `0x11` | VARIANT | case:u8 + payload | Discriminant then case-specific data |
| `0x12` | ENUM | value:u8 | Single byte, up to 255 cases |
| `0x13` | FLAGS | value:u32 | Bitfield, up to 32 flags |
| `0x14` | OPTION_NONE | (none) | 1 byte total |
| `0x15` | OPTION_SOME | payload | Tag then inner value |
| `0x16` | TUPLE | skip:u32 + elements... | Skip pointer for bypass |
| `0x17` | LIST | count:u32 + skip:u32 + elements... | Element count + skip |
| `0x18` | RESULT_OK | payload | Tag then Ok value |
| `0x19` | RESULT_ERR | payload | Tag then Err value |
| `0x20` | S8 | int8_t | 2 bytes total |
| `0x21` | U8 | uint8_t | 2 bytes total |
| `0x22` | S16 | int16_t | 3 bytes total |
| `0x23` | U16 | uint16_t | 3 bytes total |
| `0x24` | S32 | int32_t | 5 bytes total |
| `0x25` | U32 | uint32_t | 5 bytes total |
| `0x26` | S64 | int64_t | 9 bytes total |
| `0x27` | U64 | uint64_t | 9 bytes total |
| `0x28` | F32 | float | 5 bytes total |
| `0x29` | F64 | double | 9 bytes total |
| `0x2A` | BOOL_FALSE | (none) | 1 byte total |
| `0x2B` | BOOL_TRUE | (none) | 1 byte total |
| `0x2C` | BYTES | len:u32 + data | Length-prefixed |
| `0x2D` | STRING | len:u32 + UTF-8 data | Length-prefixed |

The `0x01`–`0x09` range is reserved for the existing Thatch JSON tags.
The `0x10`+ range is the WIT type namespace. Both coexist in the same
ThatchRegion — a record field could contain a Thatch JSON value (e.g., an
application-state body), and the cursor machinery is the same either way.

Byte order: native, same as Thatch JSON v0. Cross-endian is out of scope.

### Phase 3: Writer codegen

For each WIT type, generate a C function that writes a value into a
ThatchRegion using the Thatch bump-allocation API.

**Record example** (for `message-envelope`):

```c
/*
 * Auto-generated writer for WIT record message-envelope.
 * Writes: TAG_RECORD | skip:u32 | field_0 | field_1 | ... | field_N
 */
int sap_wit_write_message_envelope(
    ThatchRegion *region,
    const SapWitMessageEnvelope *val)
{
    int rc;
    ThatchCursor skip_loc;

    rc = thatch_write_tag(region, SAP_WIT_TAG_RECORD);
    if (rc != THATCH_OK) return rc;

    rc = thatch_reserve_skip(region, &skip_loc);
    if (rc != THATCH_OK) return rc;

    /* field: message-id (utf8 → STRING) */
    rc = thatch_write_tag(region, SAP_WIT_TAG_STRING);
    if (rc != THATCH_OK) return rc;
    rc = thatch_write_data(region, &val->message_id_len, sizeof(uint32_t));
    if (rc != THATCH_OK) return rc;
    rc = thatch_write_data(region, val->message_id, val->message_id_len);
    if (rc != THATCH_OK) return rc;

    /* field: from-worker (option<worker-id> → OPTION_NONE or OPTION_SOME + S64) */
    if (!val->has_from_worker) {
        rc = thatch_write_tag(region, SAP_WIT_TAG_OPTION_NONE);
        if (rc != THATCH_OK) return rc;
    } else {
        rc = thatch_write_tag(region, SAP_WIT_TAG_OPTION_SOME);
        if (rc != THATCH_OK) return rc;
        rc = thatch_write_tag(region, SAP_WIT_TAG_S64);
        if (rc != THATCH_OK) return rc;
        rc = thatch_write_data(region, &val->from_worker, sizeof(int64_t));
        if (rc != THATCH_OK) return rc;
    }

    /* ... remaining fields ... */

    rc = thatch_commit_skip(region, skip_loc);
    if (rc != THATCH_OK) return rc;

    return THATCH_OK;
}
```

**Variant example** (for `lease-state`):

```c
/*
 * Auto-generated writer for WIT variant lease-state.
 * Writes: TAG_VARIANT | case:u8 | case-specific payload
 */
int sap_wit_write_lease_state(
    ThatchRegion *region,
    const SapWitLeaseState *val)
{
    int rc;

    rc = thatch_write_tag(region, SAP_WIT_TAG_VARIANT);
    if (rc != THATCH_OK) return rc;

    uint8_t case_tag = val->case_tag;
    rc = thatch_write_data(region, &case_tag, 1);
    if (rc != THATCH_OK) return rc;

    switch (val->case_tag) {
    case SAP_WIT_LEASE_STATE_QUEUED:   /* no payload */
        break;
    case SAP_WIT_LEASE_STATE_LEASED:   /* payload: lease-info record */
        rc = sap_wit_write_lease_info(region, &val->leased);
        if (rc != THATCH_OK) return rc;
        break;
    case SAP_WIT_LEASE_STATE_DONE:     /* no payload */
        break;
    case SAP_WIT_LEASE_STATE_FAILED:   /* payload: utf8 */
        rc = thatch_write_tag(region, SAP_WIT_TAG_STRING);
        if (rc != THATCH_OK) return rc;
        rc = thatch_write_data(region, &val->failed_len, sizeof(uint32_t));
        if (rc != THATCH_OK) return rc;
        rc = thatch_write_data(region, val->failed, val->failed_len);
        if (rc != THATCH_OK) return rc;
        break;
    default:
        return THATCH_INVALID;
    }

    return THATCH_OK;
}
```

The writer functions compose recursively — `sap_wit_write_message_envelope`
calls `sap_wit_write_lease_info` for nested records, and so on. The codegen
walks the type registry and emits writers in dependency order.

### Phase 4: Reader codegen

For each WIT type, generate cursor-based reader functions. Readers operate on
`(const ThatchRegion *, ThatchCursor *)` pairs and advance the cursor as they
consume data. They never allocate.

**Two reader patterns:**

**Selective field access** — read a specific field by name, skipping preceding
fields via cursor advancement. This is the common case: the caller wants
`envelope.payload` without decoding the full record.

```c
/*
 * Read the 'payload' field of a message-envelope starting at *cursor.
 * Skips over preceding fields (message-id, from-worker, to, route, kind,
 * message-flags, requires-ack, trace-id) by advancing the cursor past each.
 * Returns a zero-copy pointer into the region for the payload bytes.
 */
int sap_wit_read_message_envelope_payload(
    const ThatchRegion *region,
    ThatchCursor *cursor,
    const void **payload_out,
    uint32_t *payload_len_out);
```

**Full decode** — read all fields into a C struct. Used when the caller needs
every field (e.g., for re-encoding into wire format or for debug logging).

```c
int sap_wit_read_message_envelope(
    const ThatchRegion *region,
    ThatchCursor *cursor,
    SapWitMessageEnvelope *out);
```

**Variant readers** return the case tag and provide per-case accessor functions:

```c
int sap_wit_read_lease_state_tag(
    const ThatchRegion *region,
    ThatchCursor *cursor,
    uint8_t *case_tag_out);

/* Called after tag read, when case_tag == SAP_WIT_LEASE_STATE_LEASED */
int sap_wit_read_lease_state_leased(
    const ThatchRegion *region,
    ThatchCursor *cursor,
    SapWitLeaseInfo *out);
```

**Skip-based bypass** — for compound fields the caller doesn't need:

```c
/*
 * Skip over a message-envelope value starting at *cursor without
 * decoding any fields. Uses the record's skip pointer for O(1) bypass.
 */
int sap_wit_skip_message_envelope(
    const ThatchRegion *region,
    ThatchCursor *cursor);
```

### Phase 5: Validation codegen

Replace the current `sizeof`-based validators with Thatch-aware validators that
walk the cursor and check tag correctness, field count, variant case bounds, and
length consistency. These are generated alongside the readers.

```c
/*
 * Validate a message-envelope value starting at *cursor.
 * Walks all fields, checks tags, verifies lengths.
 * Advances cursor past the validated record on success.
 * Returns THATCH_OK or THATCH_INVALID with cursor at error position.
 */
int sap_wit_validate_message_envelope(
    const ThatchRegion *region,
    ThatchCursor *cursor);
```

Refinement annotations (`/// @refine(value >= 0)`) from the WIT source are
emitted as additional checks within the validation walk, just as they are today
but now operating on the actual decoded values rather than the placeholder struct.

### Phase 6: DBI integration

Replace the generated DBI structs and validators in `wit_schema_dbis.h` with:

1. **Tag constants** and **case constants** for the WIT type namespace.
2. **Writer functions** for each DBI key and value type.
3. **Reader functions** (both selective and full-decode) for each DBI key and
   value type.
4. **Skip functions** for each compound type.
5. **Validator functions** for each DBI key and value type.

The generated header becomes the single source of truth for how DBI records
are encoded and decoded. `wire_v0.c` can either be retired (if the Thatch
encoding replaces the wire format) or kept as the external boundary encoding
with generated conversion functions between wire and Thatch representations.

### Phase 7: Wire format convergence (deferred, optional)

Evaluate whether the Thatch WIT encoding can replace `wire_v0` entirely. The
key question is whether the tag overhead (1 byte per field) and native byte
order are acceptable at the wire boundary, or whether the compact fixed-offset
layout of `wire_v0` is worth keeping for messages that cross process or network
boundaries.

If the wire format is kept, the codegen can still generate conversion functions:

```c
/* Decode wire_v0 message bytes into a Thatch region for internal use */
int sap_wit_wire_to_thatch_message_envelope(
    const uint8_t *wire_src, uint32_t wire_len,
    ThatchRegion *region,
    ThatchCursor *cursor_out);

/* Encode a Thatch message-envelope into wire_v0 bytes for external use */
int sap_wit_thatch_to_wire_message_envelope(
    const ThatchRegion *region,
    ThatchCursor cursor,
    uint8_t *dst, uint32_t dst_len,
    uint32_t *written_out);
```

This keeps the Canonical ABI–style compact encoding at the strict boundary
(network, cross-module calls) while using Thatch internally.

## Input/Output contract

**Input:** `schemas/wit/runtime-schema.wit` (unchanged)

**Output** (all generated, replacing current output):

| File | Contents |
|---|---|
| `generated/wit_schema_dbis.h` | Tag constants, case constants, input C structs for writers, reader/writer/skip/validate function declarations |
| `generated/wit_schema_dbis.c` | DBI schema metadata table (unchanged), plus all generated function bodies |
| `schemas/dbi_manifest.csv` | DBI manifest (unchanged) |

**Unchanged:** The Makefile targets (`make schema-check`,
`make wit-schema-generate`, etc.) continue to work as before. The generated
header is still `#include`-able with the same guard. Downstream code that uses
`SAP_WIT_DBI_*` constants and the `sap_wit_dbi_schema` table sees no breaking
change.

## Testing strategy

### Unit tests for the codegen

1. **Type registry round-trip.** Parse the current `runtime-schema.wit`, verify
   that every named type resolves correctly, and that recursive references
   (e.g., `message-envelope` → `msg-route` → `worker-id`) terminate.

2. **Tag assignment determinism.** Verify that re-running the codegen produces
   identical tag assignments.

3. **Generated code compiles.** `make wit-schema-cc-check` already does this;
   it must continue to pass with the new output.

### Integration tests for generated accessors

4. **Write-then-read round-trip.** For each DBI key/value type, construct a
   value, write it to a ThatchRegion, read it back, and verify field equality.

5. **Selective field access.** For `message-envelope`, write a full record and
   then read only the `payload` field, verifying that preceding fields are
   correctly skipped.

6. **Variant round-trip.** For `lease-state`, write each case (queued, leased,
   done, failed), read back the case tag, and verify the case-specific payload.

7. **Option round-trip.** For fields like `from-worker: option<worker-id>`,
   verify both the `NONE` and `SOME` paths.

8. **Skip correctness.** Write a compound value, call the skip function, and
   verify the cursor lands at the correct position.

9. **Validation.** Feed truncated, wrong-tag, and out-of-bounds data to the
   generated validators and verify they return `THATCH_INVALID`.

### Cross-validation against wire_v0

10. **Wire-to-Thatch equivalence.** For each message kind (`COMMAND`, `EVENT`,
    `TIMER`), encode via `wire_v0`, convert to Thatch, read back fields, and
    verify they match the original `SapRunnerMessageV0` struct.

11. **Thatch-to-wire equivalence.** Write a message-envelope into Thatch, convert
    to wire, decode via `wire_v0`, and verify field equality.

These tests validate that the generated code produces the same semantic content
as the hand-written wire format, which is the cross-validation point.

## Lambkin compiler alignment

The codegen extension exercises a subset of what the Lambkin compiler will need:

| Codegen capability | Lambkin compiler equivalent |
|---|---|
| WIT type registry (records, variants, enums, flags, options, tuples, lists) | Lambkin type system (algebraic types, refinements, generics) |
| Thatch tag assignment | Lambkin binary format tag assignment |
| Writer generation (type → bump-allocated bytes) | Lambkin serialization (value → linear memory) |
| Reader generation (cursor → typed value) | Lambkin deserialization (linear memory → value) |
| Skip generation (cursor bypass) | Lambkin partial access / projection optimization |
| Validation generation (tag + bounds checking) | Lambkin runtime contract checking (or elision under trusted mode) |
| Refinement annotations → inline checks | Lambkin refinement types → Z3-backed static checks + runtime guards |

The key difference is that the Lambkin compiler will also need to handle:
generics (parameterized types), recursive types (e.g., trees), structural
sharing (COW references between regions), and escape-analysis-driven elision
of runtime checks. But the serialization target — Thatch regions with
tag/skip/cursor traversal — is the same.

Successfully generating correct accessors from WIT proves that the Thatch
encoding is expressive enough and that the cursor-based access pattern
composes cleanly across nested types. If the codegen hits a limitation in
the Thatch model (e.g., a type that doesn't fit the tag/skip layout), that
surfaces early and informs the Lambkin compiler design before it's locked in.

## Implementation plan

This work can be done incrementally, with each phase producing testable output.

1. **Type registry** — parse all WIT constructs, build resolution graph, add
   unit tests for resolution. No change to generated output yet.

2. **Tag constants** — emit the tag byte `#define`s into the generated header.
   Existing code unaffected.

3. **Primitive writers/readers** — generate write/read functions for scalar
   types (`s64`, `bool`, `utf8`, `bytes`). Replace current `map_wit_type`
   primitive handling.

4. **Compound writers/readers** — generate for records, variants, enums, flags,
   options, tuples. The `unknown_layout` placeholders disappear.

5. **Skip functions** — generate for all compound types. Add skip-correctness
   tests.

6. **Validators** — replace `sizeof`-based validators with cursor-walking
   validators. Refinement annotation support carries forward.

7. **DBI integration** — replace the `wit_schema_dbis.h` structs and validators
   with the new generated code. Update any downstream code that accessed the
   old placeholder structs.

8. **Wire cross-validation** — add wire-to-Thatch and Thatch-to-wire tests.
   Decide whether to generate conversion functions or retire `wire_v0` for
   internal use.

## Open questions

1. **Should compound types always carry skip pointers?** Skip pointers add 4
   bytes per record/tuple/list. For small records (e.g., `lease-info` with 3
   fixed-size fields totaling 24 bytes), the skip pointer is proportionally
   expensive. One option: only emit skip pointers for types whose serialized
   size is variable or exceeds a threshold. The codegen can determine this
   statically from the type registry.

2. **Should the generated C input structs use tagged unions for variants?** The
   writer examples above use a `case_tag` + union pattern. An alternative is
   to generate a separate writer function per case, avoiding the union entirely.
   The reader side already does this naturally (tag dispatch → per-case reader).

3. **Byte order.** Thatch v0 uses native byte order. The wire format uses
   little-endian. If the Thatch encoding ever needs to cross an endian boundary
   (unlikely for in-process use, but possible for checkpoint/restore to a
   different architecture), the codegen would need a byte-order flag. For now,
   native is correct.

4. **Tag namespace partitioning.** The proposed `0x10`+ range leaves room for
   future expansion but the exact assignments are arbitrary. Should the tag
   values be considered part of the frozen contract (like wire_v0), or are they
   internal and free to change between codegen versions?
