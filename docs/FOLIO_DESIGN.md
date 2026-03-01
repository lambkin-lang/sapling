# Folio (Formerly Vimen) - Document Session Architecture

## Status

This document replaces the prior "Vimen overlay" concept with a concrete,
implementable architecture for file-backed document editing.

- Working name: `Folio`
- Previous name: `Vimen`
- Intent: the system must feel like a real document workflow: open from storage,
  perform many edits, and save back safely.

## Why Rename

`Vimen` described an internal data-structure pattern. The product need is broader:
a document session model with persistence, recovery, and conflict handling.

`Folio` is a better fit because it describes the user-facing unit of work: a
versioned document that can be opened, edited, and saved.

## Problem Statement

We need a single architecture that supports all of the following:

1. Load a document from filesystem or Sapling DB row.
2. Edit incrementally without rewriting the entire document on each change.
3. Save atomically (no partial corruption on crash/power loss).
4. Recover unsaved edits after crash.
5. Support undo/redo and external-change detection.
6. Reuse Sapling primitives (`Thatch`, `Seq`, `Text`, arena lifetimes).

The old design over-focused on overlay mechanics and under-specified operational
behavior (open/save/recover/conflict).

## Scope

### In scope

- Session lifecycle: open, mutate, checkpoint, save, close, recover.
- Persistence semantics for both file paths and DB-backed values.
- Recovery journal and crash-safe save protocol.
- Edit model and undo/redo model.
- Integration points with existing Sapling subsystems.

### Out of scope

- Multi-user collaborative OT/CRDT protocol.
- Binary diff format standardization.
- UI/editor-specific command bindings.

## Core Model

`Folio` is a document session with explicit source, state, and persistence
contracts.

### 1) Document source

A document originates from exactly one source adapter:

- `FS_PATH`: bytes read from file path.
- `DB_VALUE`: bytes read from `(dbi, key)`.
- `MEMORY`: ephemeral bytes (tests, generated docs, transient workers).

### 2) Session state

Each open document has:

- `base_bytes`: immutable last-committed bytes from source.
- `working_state`: mutable structured representation used by edit ops.
- `edit_log`: ordered operations since `base_bytes`.
- `checkpoints`: undo/redo boundaries.
- `dirty`: whether `working_state` diverged from `base_bytes`.
- `base_fingerprint`: source revision marker used for conflict detection.

### 3) Codec

A codec converts `bytes <-> working_state`.

- Decode on open.
- Encode on save.
- Validate structural invariants before commit.

Examples:

- Thatch-native structured codec.
- UTF-8 text codec (possibly using `Text` handles internally).
- JSON codec backed by Thatch traversal.

## Data Representation Strategy

`Folio` keeps the overlay idea, but as an implementation detail, not the API.

- Immutable base snapshot: prior committed bytes.
- Mutable overlay/edit state: records changes since base.
- Materialization on save: produce a full new byte payload.

This preserves incremental edit efficiency while still delivering robust,
atomic save semantics.

## Lifecycle

### Open

1. Read source bytes and capture `base_fingerprint`.
2. Decode bytes into `working_state`.
3. Initialize empty `edit_log`, checkpoints, and `dirty = false`.
4. Check for recovery journal entries tied to this source fingerprint.
5. If recovery exists, replay journal into `working_state` and mark dirty.

### Edit

1. Apply typed edit operation to `working_state`.
2. Append operation to `edit_log`.
3. Update incremental indices/caches.
4. Mark dirty.

### Checkpoint

- Record current `edit_log` position as undo boundary.
- Optionally persist journal snapshot.

### Save

1. Re-read source fingerprint when source is external (`FS_PATH`, `DB_VALUE`).
2. If fingerprint differs from `base_fingerprint`, trigger conflict policy.
3. Encode current `working_state` to `new_bytes`.
4. Validate encode/decode round-trip invariants (configurable strict mode).
5. Atomically persist `new_bytes`.
6. Set `base_bytes = new_bytes`, clear `edit_log`, `dirty = false`.
7. Advance `base_fingerprint` and prune obsolete recovery journal.

### Close

- If clean: close with no extra work.
- If dirty: keep journal (autosave) or require explicit policy (discard/force save).

## Atomic Persistence Contracts

### Filesystem save contract

Required algorithm:

1. Write to sibling temp file (`<name>.tmp.<pid>.<nonce>`).
2. `fsync` temp file.
3. Atomic rename temp -> target.
4. `fsync` parent directory.

This prevents partial-file corruption and guarantees crash consistency under
POSIX-style semantics.

### DB value save contract

Required algorithm:

1. Begin short write transaction.
2. Re-check expected fingerprint/version.
3. Compare-and-swap write new value.
4. Commit transaction.

If compare step fails, return conflict and keep session dirty.

## Recovery and Autosave

Each session may maintain a journal entry keyed by source identity:

- For files: canonical path + base fingerprint.
- For DB values: `(dbi, key)` + base fingerprint.

Journal contents:

- minimal session metadata
- checkpoint map
- edit log payload

Recovery rule:

- replay journal only if its base fingerprint matches an accessible source
  snapshot.
- otherwise keep journal as orphaned recovery candidate and require explicit
  operator decision.

## Undo/Redo Semantics

- Undo rewinds to previous checkpoint in `edit_log`.
- Redo replays forward until next checkpoint.
- Save does not erase undo history by default inside the same session instance,
  but a new session starts from committed base with empty redo stack.

This mirrors standard document editor behavior.

## External Change and Conflict Policy

At save time, if source changed externally:

- `FAIL_FAST` (default): return conflict, no write.
- `RELOAD_THEN_REAPPLY`: reload new base and replay local edits.
- `FORCE_OVERWRITE`: bypass comparison (opt-in only).

The default must be `FAIL_FAST`.

## Integration with Existing Sapling Components

### Thatch

- Preferred structured backing for `working_state` fragments and serialization.
- Skip-pointer traversal remains the fast path for typed readers.

### Seq

- Natural storage for `edit_log` and checkpoint index vectors.

### Text

- Can be used as an internal representation for text-heavy fields.
- `Folio` does not require all documents to be text-first.

### Arena

- Session-local arena for transient decode/edit buffers.
- Clear ownership rules: source bytes are immutable input; working buffers are
  session-owned.

## API Sketch (C-level)

```c
typedef enum {
    SAP_FOLIO_SRC_FS_PATH,
    SAP_FOLIO_SRC_DB_VALUE,
    SAP_FOLIO_SRC_MEMORY,
} SapFolioSourceKind;

typedef struct SapFolioSession SapFolioSession;

typedef struct {
    SapFolioSourceKind kind;
    /* tagged payload: path or db key/value identity */
} SapFolioSource;

int sap_folio_open(const SapFolioSource *src, SapFolioSession **out);
int sap_folio_apply(SapFolioSession *s, const SapFolioOp *op);
int sap_folio_checkpoint(SapFolioSession *s);
int sap_folio_undo(SapFolioSession *s);
int sap_folio_redo(SapFolioSession *s);
int sap_folio_save(SapFolioSession *s, SapFolioSavePolicy policy);
int sap_folio_close(SapFolioSession *s);
```

This API is intentionally operational (session actions), not storage-mechanism
oriented.

## Invariants

These invariants must hold for all codecs and storage adapters:

1. `base_bytes` is immutable once session opens.
2. `dirty == false` implies encoded `working_state` is byte-equivalent to
   `base_bytes` (or canonically equivalent when codec canonicalizes).
3. Save is atomic: target is always either old committed bytes or new committed
   bytes, never a torn middle.
4. Recovery replay is deterministic and idempotent for a fixed
   `(base_fingerprint, journal)` pair.
5. Undo/redo never mutates `base_bytes`; only `working_state` and log cursor.

## Roadmap

### M1 - Core session and file save correctness

- `sap_folio_open/apply/save/close`
- file atomic-save implementation
- dirty tracking and fail-fast external conflict detection

### M2 - Recovery and undo/redo

- journal format + replay
- checkpointed undo/redo
- crash recovery bootstrap path

### M3 - DB adapter and typed codecs

- `(dbi,key)` source adapter with CAS semantics
- stricter structural codec validation hooks
- performance profiling on large docs

### M4 - Compiler/runtime integration

- generated typed edit ops for selected schemas
- optional index generation for hot access paths

## Migration Note

`Vimen` should be treated as a retired codename. New code, docs, and APIs should
use `Folio` terminology.
