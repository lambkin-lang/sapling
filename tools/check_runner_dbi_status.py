#!/usr/bin/env python3
"""
check_runner_dbi_status.py - validate runner DBI status drift

Ensures DBIs referenced by runtime code/docs are marked active in the
manifest and remain aligned with generated SAP_WIT_DBI_* constants.
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path


def fail(msg: str) -> int:
    print(f"runner-dbi-status: FAIL: {msg}", file=sys.stderr)
    return 1


def load_manifest(path: Path):
    rows = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    by_dbi = {}
    for idx, row in enumerate(rows, start=2):
        try:
            dbi = int(row["dbi"], 10)
        except (TypeError, ValueError):
            raise ValueError(f"line {idx}: invalid dbi value {row.get('dbi')!r}")
        by_dbi[dbi] = row
    return by_dbi


def load_wit_macros(path: Path):
    macros = {}
    pat = re.compile(r"^\s*#define\s+(SAP_WIT_DBI_[A-Z0-9_]+)\s+([0-9]+)u?\s*$")
    for line in path.read_text(encoding="utf-8").splitlines():
        m = pat.match(line)
        if not m:
            continue
        macros[m.group(1)] = int(m.group(2), 10)
    return macros


def collect_runtime_dbi_usage(runner_dir: Path, macros: dict[str, int]):
    used = set()
    macro_pat = re.compile(r"\bSAP_WIT_DBI_[A-Z0-9_]+\b")
    for path in sorted(runner_dir.glob("*.[ch]")):
        text = path.read_text(encoding="utf-8")
        for token in set(macro_pat.findall(text)):
            if token in macros:
                used.add(macros[token])
    return used


def collect_runner_doc_dbi_usage(docs_dir: Path):
    used = set()
    pat = re.compile(r"\bDBI\s*([0-9]+)\b")
    for path in sorted(docs_dir.glob("RUNNER_*.md")):
        text = path.read_text(encoding="utf-8")
        for m in pat.finditer(text):
            used.add(int(m.group(1), 10))
    return used


def main() -> int:
    if len(sys.argv) != 4:
        return fail(
            "usage: tools/check_runner_dbi_status.py "
            "<manifest.csv> <generated_wit_schema_dbis.h> <repo_root>"
        )

    manifest = Path(sys.argv[1])
    wit_header = Path(sys.argv[2])
    repo_root = Path(sys.argv[3])
    runner_dir = repo_root / "src" / "runner"
    docs_dir = repo_root / "docs"

    for path in (manifest, wit_header, runner_dir, docs_dir):
        if not path.exists():
            return fail(f"path not found: {path}")

    try:
        by_dbi = load_manifest(manifest)
        macros = load_wit_macros(wit_header)
    except ValueError as e:
        return fail(str(e))

    if not macros:
        return fail(f"no SAP_WIT_DBI_* macros found in {wit_header}")

    runtime_used = collect_runtime_dbi_usage(runner_dir, macros)
    doc_used = collect_runner_doc_dbi_usage(docs_dir)
    required_active = sorted(runtime_used | doc_used)

    for dbi in required_active:
        row = by_dbi.get(dbi)
        if row is None:
            return fail(f"DBI {dbi} is referenced by runner code/docs but missing in manifest")
        status = row.get("status", "").strip().lower()
        if status != "active":
            return fail(
                f"DBI {dbi} ({row.get('name')}) is referenced by runner code/docs "
                f"but has status={row.get('status')!r}; expected 'active'"
            )

    for macro, dbi in sorted(macros.items()):
        row = by_dbi.get(dbi)
        if row is None:
            return fail(f"{macro}={dbi} missing from manifest")
        macro_name = macro[len("SAP_WIT_DBI_") :].lower()
        manifest_name = row.get("name", "")
        if macro_name != manifest_name:
            return fail(
                f"{macro} name mismatch: macro implies {macro_name!r}, "
                f"manifest has {manifest_name!r}"
            )

    print(
        "runner-dbi-status: PASS "
        f"(runtime_dbis={len(runtime_used)} doc_dbis={len(doc_used)} "
        f"required_active={len(required_active)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
