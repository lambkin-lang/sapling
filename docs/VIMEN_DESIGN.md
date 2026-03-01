# Vimen — Document Overlay System Design

*From Latin vīmen ("flexible twig, withe"), from PIE \*wei- "to bend."*

## Overview

Vimen is an architectural pattern for working with structured documents in
Sapling's Wasm linear-memory environment. The core idea: a sealed Thatch
region serves as an immutable base layer, a lightweight overlay captures
edits as cursor references into the base plus inline new content, and a
commit operation collapses the overlay into a new sealed region. Interval
snapshots of the overlay provide crash recovery and deep undo history.

Vimen is not a single library or data structure. It is a contract between
existing Sapling subsystems — Thatch, Seq, Arena, Text — and a set of
conventions for how they compose when a worker needs to load, edit, snapshot,
and persist structured data.

## Motivation

### The problem

Applications that work with user documents face a fundamental tension between
two representations: the durable format (on disk, in a B+ tree row, on the
wire) and the in-memory working model (the thing the user edits). Every
document-oriented system must decide how to bridge them.

Common approaches include:

- **Load-transform-save**: Deserialize the entire document into an in-memory
  object model, edit the model, serialize back on save. Simple but memory-
  hungry, and the save operation must rewrite the whole document even for a
  one-character edit.

- **Memory-mapped / page-faulted**: The file is the working representation;
  the OS pages data in and out. Efficient for page-structured data (SQLite)
  but poor for formats that require large-region rewrites on interior edits.

- **Piece table / rope**: The original content stays immutable; edits are
  recorded as a sequence of references to either the original or new content.
  Efficient for text editors (VS Code, Vim) but traditionally limited to
  flat character sequences.

### Where Sapling sits

In Lambkin's runtime model, "documents" are structured data that workers read
from and write to B+ tree rows. The B+ tree handles row-level storage — it is
the durable layer. But when a worker loads a row, transforms it, and writes
it back, the working representation matters.

Thatch already provides the "load" half: a sealed Thatch region is a compact,
cursor-traversable snapshot of structured data. But Thatch regions are
immutable after commit — there is no mechanism for incremental edits. The
text subsystem has independently evolved the piece-table pattern (LITERAL
handles point into immutable backing content, CODEPOINT handles are inline
edits, the Seq of handles is the edit sequence). Vimen generalizes this
pattern from text strings to arbitrary structured data.

### What Vimen adds

1. A sealed Thatch region as the immutable base layer for any document type.
2. A cursor-indexed overlay that captures edits without copying or mutating
   the base.
3. Interval snapshots of the overlay for crash recovery and undo history.
4. A commit operation that collapses overlay + base into a new sealed region.
5. A contract for how the Lambkin compiler generates the overlay indices and
   accessors for a given type.

## Prior Art

The base-plus-overlay pattern appears across real-world document systems.
Vimen draws on the structural ideas while fitting them to Sapling's
arena-backed, cursor-oriented, Wasm-targeted environment.

### Git object model

A commit is a sealed snapshot (tree of blobs). The working directory is the
overlay. The index/staging area is a second overlay between them. Recovery
after a crash uses the reflog — sealed objects are self-consistent, and what
was in flight is reconstructed from the log. The parallel: sealed Thatch
regions are content-addressed immutable snapshots, the worker-local overlay
is the working tree, interval-saved overlay snapshots are the reflog.

### SQLite WAL

The database file is the base. The write-ahead log is the overlay — it
contains pages modified but not yet checkpointed. Readers consult the WAL
index to decide whether to read from the main file or the WAL. Crash
recovery replays or discards the WAL. Vimen's overlay serves the same role
at document granularity rather than page granularity.

### PDF incremental save

A PDF can be updated by appending new objects and a new cross-reference table
to the end of the file, without rewriting the original content. Each
incremental save is an overlay. Readers walk the chain from newest to oldest.
Undo is "discard the last append." Recovery is "truncate at the last valid
cross-reference." The base-plus-overlay structure is baked into the format.

### Gibbon packed representations

The Gibbon compiler (Vollmer et al., ECOOP 2017; ICFP 2021) serializes
algebraic data types into packed byte arrays and generates cursor-passing
code that traverses them without deserialization. Thatch's design explicitly
draws on this work (see thatch.h header). Vimen extends the analogy: where
Gibbon produces immutable packed buffers traversed by generated cursors,
Vimen adds a mutable overlay that references cursors into the immutable base,
and a commit operation that re-packs the result into a new immutable buffer.
Unlike Gibbon, Vimen does not require garbage collection — the seal/retain/
release lifecycle provides deterministic resource management.

## Architecture

### Layers

```
┌──────────────────────────────────────────────────┐
│  Application / Worker Code                       │
│  (edit operations, queries, undo)                │
├──────────────────────────────────────────────────┤
│  Overlay                                         │
│  ┌──────────────┐  ┌──────────────────────────┐  │
│  │ Edit Sequence │  │ Index Structures         │  │
│  │ (Seq of refs) │  │ (back-ptrs, search, ...) │  │
│  └──────┬───────┘  └──────────┬───────────────┘  │
│         │ cursor refs         │ cursor refs       │
├─────────┼─────────────────────┼──────────────────┤
│  Base Layer (sealed Thatch region)               │
│  ┌───────────────────────────────────────────┐   │
│  │ tag | data | skip | tag | data | ...      │   │
│  │ (immutable, cursor-traversable)           │   │
│  └───────────────────────────────────────────┘   │
├──────────────────────────────────────────────────┤
│  Arena (backing memory for both layers)          │
└──────────────────────────────────────────────────┘
```

### Base layer

A sealed Thatch region containing the document's last committed state. The
region is immutable, cursor-traversable, and owned by the arena. Any number
of readers can traverse it concurrently without synchronization.

The base layer is populated in one of two ways:

1. **From a B+ tree row**: The worker reads a row value, interprets it as a
   Thatch region, and seals it. This is the "load" path.
2. **From a commit**: The overlay is collapsed into a new Thatch region,
   sealed, and becomes the new base. The old base is released when no
   readers remain.

### Overlay

The overlay is worker-local and ephemeral. It consists of:

**Edit sequence**: A Seq (or similar ordered sequence) of *references*, where
each reference is either:

- A **base reference**: a `(cursor_start, cursor_end)` pair into the sealed
  base region, meaning "this span of the document is unchanged."
- An **inline edit**: new content, stored in a separate append buffer (a
  mutable Thatch region or arena allocation).

This is the piece-table pattern generalized from characters to Thatch
cursor ranges. A freshly loaded document has a single base reference
spanning the entire region. Each edit splits a base reference and inserts
inline content at the split point.

**Index structures**: Optional auxiliary data structures built by traversing
the base region. These provide capabilities that Thatch's forward-only
cursor model does not natively support:

- Back pointers (parent cursor for each child node)
- Field-name lookup tables (name → cursor offset)
- Search indices over content
- Type-specific computed views

Index structures are arrays of cursors into the base region, organized
however the application (or generated code) requires. They contain no
serialized content — only structural references. They are cheap to build
(single forward pass over the base) and cheap to discard.

### Lifecycle

```
  Load from B+ tree row
         │
         ▼
  ┌─────────────┐
  │  Base only   │  (sealed Thatch region, no overlay)
  │  (read-only) │
  └──────┬──────┘
         │  first edit
         ▼
  ┌──────────────┐     interval snapshot
  │  Base +      │ ──────────────────────► recovery log
  │  Overlay     │
  │  (editing)   │
  └──────┬──────┘
         │  commit
         ▼
  ┌─────────────┐
  │  New base   │  (overlay collapsed into new sealed region)
  │  (sealed)   │
  └──────┬──────┘
         │  write to B+ tree row
         ▼
      Persisted
```

**Load**: Read a B+ tree row value into an arena page, wrap as a sealed
Thatch region. Build any needed index structures. Cost: one read + one
forward pass.

**Edit**: Split a base reference, insert inline content. Update affected
index entries. Cost: proportional to the edit, not the document size.

**Snapshot**: Serialize the overlay (edit sequence + append buffer) to a
recovery log. The base region is identified by its B+ tree row key and
version, so it does not need to be copied. Cost: proportional to the
accumulated edits, not the document size.

**Recover**: On crash or abnormal exit, load the base from the B+ tree
(the last committed version is always intact), then replay the overlay
from the most recent snapshot. Present the recovered state to the user for
approval before committing.

**Commit**: Walk the edit sequence. For each base reference, copy the
referenced span from the old base region into a new Thatch region. For each
inline edit, write the new content. The result is a single contiguous sealed
region — the new base. Write it to the B+ tree row. Release the old base
when all readers have detached.

**Undo**: Interval snapshots form a stack of overlay states. Undo pops to
the previous snapshot. This provides coarse-grained undo "for free" on top
of whatever fine-grained undo the application implements.

## Relationship to Existing Subsystems

### Thatch

Vimen's base layer IS a Thatch region. No new serialization format is
introduced. The write API (thatch_write_tag, thatch_write_data,
thatch_reserve_skip, thatch_commit_skip) is used during commit to produce
the new base. The read API (thatch_read_tag, thatch_read_data,
thatch_read_skip_len, thatch_advance_cursor) is used to traverse both the
base during index construction and the base spans during commit.

The Thatch lifecycle contract (owned during transaction → sealed on commit →
lock-free reads → freed on abort) governs the base layer directly.

### Text

The text subsystem is the first concrete instance of the Vimen pattern,
though it predates the formalization:

| Vimen concept    | Text equivalent                             |
|------------------|---------------------------------------------|
| Base layer       | Literal table (sealed, arena-backed UTF-8)  |
| Base reference   | LITERAL handle (30-bit id into the table)   |
| Inline edit      | CODEPOINT handle (inline Unicode scalar)    |
| Edit sequence    | Seq of TextHandles                          |
| Index structure  | (not yet implemented)                       |
| Cross-worker ref | TREE handle + TextTreeRegistry              |
| Commit           | (not yet formalized — would flatten to new literal) |

The text type's evolution toward literal tables, bulk loading, and COW tree
sharing is a specific case of the general Vimen lifecycle. Formalizing Vimen
means the text code can eventually be expressed in terms of the general
pattern rather than reimplementing it ad hoc.

### Seq

The overlay's edit sequence is naturally a Seq — an ordered, index-
addressable, COW-friendly sequence of uint32_t-sized elements. Each element
encodes a reference (base span or inline edit locator) using a tagged-handle
scheme analogous to TextHandle. The exact encoding is type-specific and
determined by the generated code.

### Arena

Both the base region and the overlay's append buffer are arena-backed.
The arena's page-level allocation and deallocation governs the memory
lifecycle. Snapshot and recovery operate at arena-page granularity where
possible, avoiding per-element serialization overhead.

### B+ tree

The B+ tree is the durability layer. Vimen documents are stored as row
values — the committed (sealed) base region is the row's value bytes.
Vimen does not replace or compete with the B+ tree; it governs what happens
between reading a row and writing it back.

### Wire format

The wire format (wire_v0) handles cross-worker message encoding. When a
Vimen document needs to be sent to another worker (e.g., via the intent/
outbox path), the committed base region can be transmitted directly — it is
already a self-contained byte sequence. The receiving worker wraps it as a
sealed Thatch region and proceeds. No re-serialization is needed.

## Compiler Integration

### Generated overlay code

For each user-defined document type, the Lambkin compiler generates:

1. **Base traversal**: Cursor-advancing code that walks a sealed Thatch
   region and produces a type-safe view. This is the same code that the
   WIT schema codegen feature request describes for runtime types.

2. **Index builders**: Functions that perform a single forward pass over
   the base and populate the index structures needed by the application.
   The compiler determines which indices are needed through static analysis
   of field access patterns. For example, if the program accesses a record
   field by name, the compiler emits a field-lookup index builder.

3. **Edit operations**: Type-safe edit functions that manipulate the overlay.
   An "update field X" operation locates the field's cursor range in the
   base (via the index), splits the edit sequence at that range, and inserts
   inline content for the new value.

4. **Commit writer**: A function that walks the edit sequence and produces
   a new sealed Thatch region. Base references are copied via
   thatch_write_data from the old region; inline edits are written via the
   standard Thatch write API.

### Monomorphization

Like BEPT's key-width specialization, Vimen operations are monomorphized
per document type. The compiler emits concrete functions for each type
rather than generic traversal code. This aligns with Lambkin's "big
analysis, small binaries" philosophy: the compiler does the structural
analysis, and the emitted code is a minimal, type-specific state machine.

In Wasm linear memory, this means each document type gets its own set of
i32-offset cursor operations with no dynamic dispatch overhead.

### Relationship to WIT codegen

The WIT schema codegen feature request (docs/FEATURE_REQUEST_THATCH_WIT_CODEGEN.md)
proposes extending the Python codegen to produce Thatch-native cursor
accessors for runtime types. This is a constrained preview of Vimen's
compiler integration: the WIT codegen handles a small type universe
(primitives, records, variants, enums, flags) and produces C accessors.

The Lambkin compiler handles a richer type universe (user-defined algebraic
types, refinement types, generics) and produces Wasm. But the target is the
same — Thatch cursor accessors — and the WIT codegen serves as a proving
ground and cross-validation point.

## Overlay Representation

### Reference encoding

Each entry in the overlay's edit sequence encodes one of:

```
Base span reference:
  ┌──────────────────┬──────────────────┐
  │ start_cursor:u32 │ end_cursor:u32   │
  └──────────────────┴──────────────────┘
  Meaning: "copy bytes [start, end) from the sealed base region."

Inline edit reference:
  ┌───────────────────┬──────────────────┐
  │ buffer_offset:u32 │ length:u32       │
  └───────────────────┴──────────────────┘
  Meaning: "copy `length` bytes starting at `buffer_offset` in the
            append buffer."
```

A tag bit distinguishes the two kinds. The exact encoding depends on whether
a single Seq of uint32_t pairs suffices or whether a more compact scheme is
needed. For most document sizes, a flat array of 8-byte entries (tagged
start + length) is sufficient and cache-friendly.

### Append buffer

Inline edits are written into a mutable Thatch region (the append buffer)
using the standard bump-allocation write API. The append buffer is NOT sealed
until commit — it remains writable for the duration of the editing session.

On commit, the append buffer's contents are interspersed with base spans to
produce the new sealed region. After commit, the append buffer is freed.

### Snapshot format

An interval snapshot consists of:

1. The base region's identity (B+ tree DBI + key + version/sequence number).
2. The edit sequence (serialized Seq contents).
3. The append buffer contents (raw bytes).

This is sufficient to reconstruct the full overlay given the base. The
snapshot does not include index structures — these are rebuilt from the
base + overlay on recovery, since they are derived data.

## Recovery Protocol

### On crash

1. Open the B+ tree. The last committed state is intact (B+ tree
   transactions are atomic).
2. Check for a recovery log (the most recent interval snapshot).
3. If present:
   a. Load the base region identified in the snapshot.
   b. Reconstruct the overlay from the snapshot's edit sequence and
      append buffer.
   c. Present the recovered document to the user with a clear indication
      that it contains uncommitted edits from the interrupted session.
   d. The user may approve (triggering a commit) or discard (dropping
      the overlay and reverting to the last committed base).
4. If no recovery log: the last committed base is the current state.
   No data loss beyond any edits that occurred after the last snapshot.

### Snapshot interval

The interval between snapshots is a policy decision, not a structural one.
Reasonable defaults: snapshot on every N-th edit, every M seconds of
activity, or on explicit user save. The cost of a snapshot is proportional
to the overlay size (accumulated edits), not the document size.

## Wasm Linear Memory Considerations

### Cursor arithmetic

All Vimen cursor operations reduce to uint32_t byte offsets — the same
representation Thatch already uses. In Wasm, these are i32 values with
native add/compare/load/store operations. No pointer indirection beyond
what Wasm linear memory already provides.

### No shared mutable state

The base layer is immutable (sealed). The overlay is worker-local. No
shared mutable memory is needed. Cross-worker document sharing uses the
same mechanism as cross-worker text sharing: register the committed base
in a registry with atomic refcounting, reference it by ID, resolve on the
receiving side.

### Arena backing

Both layers are arena-backed. In Wasm, arenas can be backed by linear
memory segments (ARENA_LINEAR strategy), avoiding any reliance on system
allocators. The arena's page-level lifecycle maps directly to Wasm memory
grow/shrink semantics.

## Open Questions

### Overlay granularity

Should the edit sequence track byte-level cursor ranges (maximum
flexibility, highest overhead per edit) or node-level Thatch subtree
boundaries (coarser, but edits snap to structural boundaries)? For text,
byte-level is natural. For structured records, subtree-level may be more
appropriate. The compiler may need to choose per type.

### Index persistence

The design describes indices as ephemeral and worker-local. Should there
be an opt-in path for serializing indices into their own Thatch region so
they can be cached alongside the base? This would amortize the index-build
cost across transactions but adds complexity to the snapshot and recovery
protocol. The compiler could decide this based on the cost of rebuilding
versus the cost of storing.

### Nested documents

When a document contains a field whose value is itself a Vimen document
(e.g., a record with a text field), should the inner document share the
outer document's base region, or have its own? Sharing is more compact
but complicates the edit sequence. Separate regions are simpler but may
fragment arena pages.

### Overlay size limits

At what point should an overlay be forced to commit (collapse into a new
base) rather than continuing to accumulate edits? A very large overlay
degrades read performance (every read must consult the edit sequence) and
increases snapshot size. A policy like "commit when overlay exceeds N% of
base size" may be appropriate.

### Recovery log location

Where do interval snapshots live? Options include: a dedicated B+ tree DBI
for recovery logs, a separate file outside the B+ tree, or a ring buffer
in arena pages. The choice affects durability guarantees and cleanup
complexity.

### Diff and merge

If two workers independently edit overlays against the same base, can their
overlays be merged? The edit sequences are structurally similar to
operational transforms — each is a sequence of "retain base span / insert
new content" operations. Three-way merge (base, overlay A, overlay B) is
theoretically possible but may not be worth the complexity for the initial
design.

## Naming Conventions

Following Sapling's pastoral naming tradition:

| Name      | Role |
|-----------|------|
| Sapling   | B+ tree storage engine (the whole project) |
| Thatch    | Cursor-oriented packed data subsystem |
| Seq       | Copy-on-write sequence of uint32_t |
| Vimen     | Document overlay pattern (this design) |

The Latin *vīmen* means "flexible twig" — a branch that bends without
breaking, woven into useful structures. The name reflects the pattern's
flexibility (any structured document type), its relationship to the tree
family (Sapling, Thatch), and its weaving of base and overlay into a
coherent working document.
