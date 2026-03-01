#!/usr/bin/env python3
"""
Generate DBI manifest and C metadata from WIT schema record declarations.

Convention:
  record dbi<index>-<name>-key { ... }
  record dbi<index>-<name>-value { ... }
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path

RECORD_BLOCK_RE = re.compile(
    r"(?:^[ \t]*///\s*@refine\(([^)]+)\)\s*\n)?[ \t]*record\s+([a-z0-9][a-z0-9-]*)\s*\{([^}]*)\}",
    re.MULTILINE
)
DBI_REC_RE = re.compile(r"^dbi([0-9]+)-([a-z0-9][a-z0-9-]*)-(key|value)$")
FIELD_RE = re.compile(r"^\s*([a-z0-9-]+)\s*:\s*([a-z0-9-]+(?:<[^>]+>)?)\s*,", re.MULTILINE)

@dataclass
class WitField:
    name: str
    wit_type: str

    @property
    def c_name(self) -> str:
        return self.name.replace("-", "_")

@dataclass
class WitRecord:
    name: str
    refine_rule: str | None
    fields: list[WitField]

    @property
    def c_name(self) -> str:
        return self.name.replace("-", "_")

@dataclass
class DbiEntry:
    dbi: int
    name: str
    key_record: str
    value_record: str
    key_ast: WitRecord
    value_ast: WitRecord

    @property
    def c_name(self) -> str:
        return self.name.replace("-", "_").upper()


def parse_dbi_entries(wit_text: str) -> list[DbiEntry]:
    per_dbi: dict[int, dict[str, WitRecord]] = {}
    names_by_dbi: dict[int, str] = {}

    for match in RECORD_BLOCK_RE.finditer(wit_text):
        refine_rule = match.group(1)
        rec_name = match.group(2)
        body = match.group(3)

        fields = []
        for fmatch in FIELD_RE.finditer(body):
            fields.append(WitField(name=fmatch.group(1), wit_type=fmatch.group(2)))
            
        record = WitRecord(name=rec_name, refine_rule=refine_rule, fields=fields)

        m = DBI_REC_RE.match(rec_name)
        if not m:
            continue
        dbi = int(m.group(1), 10)
        name = m.group(2)
        kind = m.group(3)

        if dbi in names_by_dbi and names_by_dbi[dbi] != name:
            raise ValueError(
                f"dbi {dbi} has multiple names ({names_by_dbi[dbi]!r} vs {name!r})"
            )
        names_by_dbi[dbi] = name

        slot = per_dbi.setdefault(dbi, {})
        if kind in slot:
            raise ValueError(f"duplicate {kind} record for dbi {dbi}: {rec_name}")
        slot[kind] = record

    if not per_dbi:
        raise ValueError("no dbi records found (expected dbiN-*-key/value records)")

    dbis = sorted(per_dbi.keys())
    if dbis[0] != 0:
        raise ValueError("dbi sequence must start at 0")
    for prev, cur in zip(dbis, dbis[1:]):
        if cur != prev + 1:
            raise ValueError(f"dbi sequence gap between {prev} and {cur}")

    entries: list[DbiEntry] = []
    for dbi in dbis:
        slot = per_dbi[dbi]
        if "key" not in slot or "value" not in slot:
            raise ValueError(f"dbi {dbi} missing key/value pair: {slot}")
        entries.append(
            DbiEntry(
                dbi=dbi,
                name=names_by_dbi[dbi],
                key_record=slot["key"].name,
                value_record=slot["value"].name,
                key_ast=slot["key"],
                value_ast=slot["value"],
            )
        )
    return entries


def write_manifest(entries: list[DbiEntry], out_path: Path) -> None:
    existing_meta: dict[int, tuple[str, str, str]] = {}

    if out_path.exists():
        with out_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    dbi = int((row.get("dbi") or "").strip(), 10)
                except ValueError:
                    continue
                name = (row.get("name") or "").strip()
                owner = (row.get("owner") or "").strip()
                status = (row.get("status") or "").strip()
                if not name:
                    continue
                existing_meta[dbi] = (name, owner or "runtime", status or "planned")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["dbi", "name", "key_format", "value_format", "owner", "status"])
        for e in entries:
            norm_name = e.name.replace("-", "_")
            owner = "runtime"
            status = "active" if e.dbi == 0 else "planned"
            if e.dbi in existing_meta:
                old_name, old_owner, old_status = existing_meta[e.dbi]
                if old_name == norm_name:
                    owner = old_owner
                    status = old_status
            writer.writerow(
                [
                    str(e.dbi),
                    norm_name,
                    f"wit:{e.key_record}",
                    f"wit:{e.value_record}",
                    owner,
                    status,
                ]
            )


def map_wit_type(wit_type: str, c_name: str) -> list[str]:
    """Map a WIT type to a C struct field declaration (Canonical ABI layout)."""
    # Simple primitive mapping.
    if wit_type == "s8" or wit_type == "u8" or wit_type == "bool":
        return [f"    uint8_t {c_name};"]
    if wit_type == "s16" or wit_type == "u16":
        return [f"    uint16_t {c_name};"]
    if wit_type == "s32" or wit_type == "u32":
        return [f"    uint32_t {c_name};"]
    if wit_type == "s64" or wit_type == "u64" or wit_type == "timestamp":
        return [f"    uint64_t {c_name};"]
    if wit_type == "f32":
        return [f"    float {c_name};"]
    if wit_type == "f64" or wit_type == "score":
        return [f"    double {c_name};"]
    if wit_type == "utf8" or wit_type == "bytes" or wit_type == "string":
        return [
            f"    uint32_t {c_name}_offset;",
            f"    uint32_t {c_name}_len;"
        ]
    
    # Fallback to byte blobs if we hit arrays or complex component variants
    return [f"    uint64_t {c_name}_unknown_layout;"]

def generate_record_struct(record: WitRecord) -> list[str]:
    lines = [f"typedef struct __attribute__((packed)) {{"]
    for field in record.fields:
        lines.extend(map_wit_type(field.wit_type, field.c_name))
    lines.append(f"}} SapWit_{record.c_name};")
    lines.append("")
    return lines

def generate_validator(record: WitRecord) -> list[str]:
    lines = [
        f"static inline int sap_wit_validate_{record.c_name}(const void *data, uint32_t len) {{",
        f"    if (data == NULL || len == 0) return 0; /* Deletion or empty payload bypass */",
        f"    if (len < sizeof(SapWit_{record.c_name})) return -1; /* ERR_CORRUPT */",
    ]
    if record.refine_rule:
        lines.append(f"    const SapWit_{record.c_name} *rec = (const SapWit_{record.c_name} *)data;")
        # Replace variable accesses with rec->field
        rule = record.refine_rule
        for field in record.fields:
            if field.name in rule:
                # Naive regex replace for standalone word boundaries
                rule = re.sub(rf'\b{field.name}\b', f"rec->{field.c_name}", rule)
        lines.append(f"    if (!({rule})) return -1; /* Refinement violation! */")
    else:
        lines.append("    (void)data; /* No refinement constraints */")

    lines.append("    return 0;")
    lines.append("}")
    lines.append("")
    return lines

def write_c_header(entries: list[DbiEntry], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    guard = "SAPLING_WIT_SCHEMA_DBIS_H"
    lines = [
        "/* Auto-generated by tools/wit_schema_codegen.py; DO NOT EDIT. */",
        f"#ifndef {guard}",
        f"#define {guard}",
        "",
        "#include <stdint.h>",
        "#include <stddef.h>",
        "",
        "typedef struct {",
        "    uint32_t dbi;",
        "    const char *name;",
        "    const char *key_wit_record;",
        "    const char *value_wit_record;",
        "} SapWitDbiSchema;",
        "",
    ]
    seen_records = set()
    for e in entries:
        lines.append(f"#define SAP_WIT_DBI_{e.c_name} {e.dbi}u")
    lines.append("")

    for e in entries:
        if e.key_ast.name not in seen_records:
            lines.extend(generate_record_struct(e.key_ast))
            lines.extend(generate_validator(e.key_ast))
            seen_records.add(e.key_ast.name)
        if e.value_ast.name not in seen_records:
            lines.extend(generate_record_struct(e.value_ast))
            lines.extend(generate_validator(e.value_ast))
            seen_records.add(e.value_ast.name)
    lines.extend(
        [
            "",
            "extern const SapWitDbiSchema sap_wit_dbi_schema[];",
            "extern const uint32_t sap_wit_dbi_schema_count;",
            "",
            f"#endif /* {guard} */",
            "",
        ]
    )
    out_path.write_text("\n".join(lines), encoding="utf-8")


def write_c_source(entries: list[DbiEntry], header_path: Path, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    rel_header = header_path.as_posix()
    lines = [
        "/* Auto-generated by tools/wit_schema_codegen.py; DO NOT EDIT. */",
        f'#include "{rel_header}"',
        "",
        "const SapWitDbiSchema sap_wit_dbi_schema[] = {",
    ]
    for e in entries:
        lines.append(
            f'    {{{e.dbi}u, "{e.name.replace("-", "_")}", "{e.key_record}", "{e.value_record}"}},'
        )
    lines.extend(
        [
            "};",
            "",
            "const uint32_t sap_wit_dbi_schema_count = "
            "(uint32_t)(sizeof(sap_wit_dbi_schema) / sizeof(sap_wit_dbi_schema[0]));",
            "",
        ]
    )
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--wit", required=True, help="Input WIT file containing dbi records")
    p.add_argument("--manifest", required=True, help="Output CSV manifest path")
    p.add_argument("--header", required=True, help="Output generated C header path")
    p.add_argument("--source", required=True, help="Output generated C source path")
    args = p.parse_args(argv)

    wit_path = Path(args.wit)
    if not wit_path.exists():
        print(f"wit-schema: FAIL: file not found: {wit_path}", file=sys.stderr)
        return 1

    try:
        entries = parse_dbi_entries(wit_path.read_text(encoding="utf-8"))
        write_manifest(entries, Path(args.manifest))
        write_c_header(entries, Path(args.header))
        write_c_source(entries, Path(args.header), Path(args.source))
    except Exception as exc:  # noqa: BLE001
        print(f"wit-schema: FAIL: {exc}", file=sys.stderr)
        return 1

    print(
        "wit-schema: PASS "
        f"(entries={len(entries)} wit={wit_path} manifest={args.manifest} header={args.header})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
