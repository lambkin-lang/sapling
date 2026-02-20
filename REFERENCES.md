# REFERENCES.md — Works consulted during development of Sapling

All consulted works are listed below in accordance with the cleanroom
procedure described in the problem statement.

---

## Academic papers and textbooks

### Comer (1979), "The Ubiquitous B-Tree"
- **Type:** journal paper (ACM Computing Surveys, Vol. 11 No. 2)
- **License:** not applicable (academic paper)
- **What was learned:** core B-tree and B+ tree algorithms — node
  structure, search, insert with split, delete with merge/redistribution,
  and the distinction between B-tree (push-up) and B+ tree (copy-up)
  separator semantics.
- **Source code read:** no

### Knuth, *The Art of Computer Programming*, Vol. 3 (Sorting and Searching)
- **Type:** textbook
- **License:** not applicable (book)
- **What was learned:** formal treatment of B-tree invariants, fill
  factors, and the relationship between page size and tree height.
- **Source code read:** no

---

## Documentation / architecture descriptions

### LMDB technical documentation
- **URL:** http://www.lmdb.tech/doc/
- **License:** documentation is freely readable; LMDB source is OpenLDAP
  Public License 2.8 — **source code was NOT consulted**
- **What was learned:**
  - Dual meta-page strategy: two meta pages alternate writes so that at
    least one is always valid; the one with the higher transaction ID is
    current.  Crash recovery reads both and picks the valid higher-txnid
    page.
  - Single-writer / multiple-reader model.
  - Copy-on-write for MVCC: read transactions receive a root page number
    at `txn_begin` and see an immutable snapshot; write transactions copy
    pages before modifying them.
  - Free-page recycling: freed pages are added to a free list and reused
    by subsequent write transactions.
- **Source code read:** no

### libmdbx documentation
- **URL:** https://libmdbx.dqdkfa.ru/
- **License:** Apache 2.0 — **source code was NOT consulted**
- **What was learned:**
  - Nested write transactions (savepoint semantics): a child transaction
    sees the parent's uncommitted writes; child commit makes changes
    visible to the parent only; child abort discards child changes without
    affecting the parent.  Nothing is durable until the outermost
    transaction commits.
- **Source code read:** no

### Wikipedia — "B+ tree"
- **URL:** https://en.wikipedia.org/wiki/B%2B_tree
- **License:** CC BY-SA (article content, not applicable to
  implementations)
- **What was learned:** page/node layout diagrams; leaf-sibling linked
  list for O(1) range-scan traversal; copy-up vs push-up split rules.
- **Source code read:** no

### Wikipedia — "Multiversion concurrency control"
- **URL:** https://en.wikipedia.org/wiki/Multiversion_concurrency_control
- **License:** CC BY-SA
- **What was learned:** snapshot isolation model; the notion that a
  read transaction's snapshot is determined at `txn_begin` time.
- **Source code read:** no

---

## MIT-licensed code repositories (source read)

### tidwall/btree.c
- **URL:** https://github.com/tidwall/btree.c
- **License:** MIT
- **What was learned:** custom allocator interface pattern — passing a
  `void *ctx` alongside function pointers rather than relying on global
  state; the general shape of a copy-on-write B-tree in C.
- **Source code read:** yes — API design and allocator interface pattern
  were referenced.  No algorithmic code was copied.

### habedi/bptree
- **URL:** https://github.com/habedi/bptree
- **License:** MIT
- **What was learned:** single-header B+ tree layout; slot-array +
  backward-growing cell-data page format as an educational starting
  point.
- **Source code read:** yes — page layout concept referenced for the
  slot-array design.  No code was copied verbatim.

### embedded2016/bplus-tree
- **URL:** https://github.com/embedded2016/bplus-tree
- **License:** MIT
- **What was learned:** embedded-focused B+ tree; confirmed that a
  fixed-page-size design with 32-bit page numbers is practical at
  embedded scale.
- **Source code read:** yes — scanned for layout ideas; no code was
  copied.

---

## Works NOT consulted (source code)

The following repositories were explicitly excluded from source
consultation per the cleanroom requirement:

- **LMDB** (mdb.c) — OpenLDAP Public License 2.8
- **libmdbx** — Apache 2.0
- **ubco-db/btree** — BSD-3-Clause
- **BerkeleyDB** — AGPL
