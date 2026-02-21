# WIT Schema Pipeline

This project uses WIT as the canonical schema definition language for:
- DBI key/value record shapes
- message envelopes and coordination records

The component model is intentionally not required for runtime execution; WIT is
used as schema input and code-generation source.

## Files

- `schemas/wit/runtime-schema.wit`: canonical WIT schema package
- `tools/wit_schema_codegen.py`: generates DBI manifest + C metadata
- `generated/wit_schema_dbis.h`: generated C declarations
- `generated/wit_schema_dbis.c`: generated C table
- `schemas/dbi_manifest.csv`: generated DBI manifest

## Naming convention for DBI records

In WIT:
- `record dbi<index>-<name>-key`
- `record dbi<index>-<name>-value`

Example:
- `record dbi1-inbox-key { ... }`
- `record dbi1-inbox-value { ... }`

The codegen tool derives DBI index and logical name from this convention.

## Commands

- `make wit-schema-check`
  - validates WIT package parse using `wasm-tools component wit`
- `make wit-schema-generate`
  - regenerates `schemas/dbi_manifest.csv` and generated C metadata
- `make wit-schema-cc-check`
  - compile-checks generated C metadata (`generated/wit_schema_dbis.c`)
- `make schema-check`
  - runs WIT check, codegen, generated-C compile check, and DBI manifest validation

## Type notes

- WIT `resources` are intentionally not used.
- String-like values intended for storage are modeled as `list<u8>` (`utf8`)
  instead of component-lowering offset/length conventions.
- Heavy usage is expected for `record`, with common primitives such as `bool`,
  `s64`, and `f64`.
