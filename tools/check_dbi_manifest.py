#!/usr/bin/env python3
"""
check_dbi_manifest.py - basic DBI schema manifest validation
"""

import csv
import sys
from pathlib import Path


def fail(msg: str) -> int:
    print(f"dbi-manifest: FAIL: {msg}", file=sys.stderr)
    return 1


def main() -> int:
    if len(sys.argv) != 2:
        return fail("usage: tools/check_dbi_manifest.py <manifest.csv>")

    manifest = Path(sys.argv[1])
    if not manifest.exists():
        return fail(f"file not found: {manifest}")

    rows = []
    with manifest.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = ["dbi", "name", "key_format", "value_format", "owner", "status"]
        if reader.fieldnames != required:
            return fail(f"expected header {required}, got {reader.fieldnames}")
        for row in reader:
            rows.append(row)

    if not rows:
        return fail("manifest has no entries")

    seen = set()
    for idx, row in enumerate(rows, start=2):
        for k, v in row.items():
            if not (v or "").strip():
                return fail(f"line {idx}: empty {k}")
        try:
            dbi = int(row["dbi"], 10)
        except ValueError:
            return fail(f"line {idx}: dbi is not an integer: {row['dbi']}")
        if dbi < 0:
            return fail(f"line {idx}: dbi must be >= 0")
        if dbi in seen:
            return fail(f"line {idx}: duplicate dbi {dbi}")
        seen.add(dbi)

    sorted_dbis = sorted(seen)
    if sorted_dbis[0] != 0:
        return fail("dbi 0 entry is required")
    for prev, cur in zip(sorted_dbis, sorted_dbis[1:]):
        if cur != prev + 1:
            return fail(f"dbi sequence has a gap between {prev} and {cur}")

    print(
        "dbi-manifest: PASS "
        f"(entries={len(rows)} max_dbi={sorted_dbis[-1]} file={manifest})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
