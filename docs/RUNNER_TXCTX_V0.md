# Runner Tx Context v0

This document defines the initial Phase-B host transaction context module:
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/txctx_v0.h`
- `/Users/mshonle/Projects/pubic-lambkin-lang/sapling/src/runner/txctx_v0.c`

The module captures optimistic-attempt state outside Sapling write transactions:
- read set (`dbi+key -> observed value or not-found`)
- write set (`put` / `del`, coalesced by `dbi+key`)
- buffered intent frames (`LINT` encoded bytes)

## Current behavior

1. `sap_runner_txctx_v0_read_dbi`:
- returns staged write values first ("read-your-write")
- otherwise reads from a snapshot `Txn*` and records first observation
- repeated reads of the same key are served from the recorded read-set entry

2. `sap_runner_txctx_v0_stage_put_dbi` / `sap_runner_txctx_v0_stage_del_dbi`:
- stage mutations in-memory
- coalesce updates by key so later operations replace earlier staged state

3. `sap_runner_txctx_v0_push_intent`:
- validates and encodes a `SapRunnerIntentV0`
- stores the encoded frame for post-commit publication

4. `sap_runner_txctx_v0_validate_reads`:
- re-checks each observed key in a short write txn
- returns `SAP_CONFLICT` if any observed value/not-found state has changed

5. `sap_runner_txctx_v0_apply_writes`:
- applies staged writes in the caller-provided write txn
- does not commit; caller controls transaction lifecycle and retry policy

## Why this exists now

This is the first concrete building block for the Phase-B OCC execution model:
1. run guest logic against an in-memory tx context using snapshot reads
2. open short write txn
3. validate read-set, apply write-set, commit
4. publish buffered intents only after commit

Nested atomic context stacks and retry engines are intentionally separate next
steps.
