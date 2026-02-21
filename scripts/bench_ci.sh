#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE_FILE="${1:-benchmarks/baseline.env}"

cd "$ROOT_DIR"

if [[ ! -f "$BASELINE_FILE" ]]; then
    echo "bench-ci: baseline file not found: $BASELINE_FILE" >&2
    exit 2
fi

# shellcheck disable=SC1090
source "$BASELINE_FILE"

baseline_speedup="${BENCH_BASELINE_SPEEDUP:-${BASELINE_SPEEDUP:-}}"
if [[ -z "$baseline_speedup" ]]; then
    echo "bench-ci: BASELINE_SPEEDUP is required in $BASELINE_FILE" >&2
    exit 2
fi

count="${BENCH_COUNT:-${BASELINE_COUNT:-100000}}"
rounds="${BENCH_ROUNDS:-${BASELINE_ROUNDS:-3}}"
allowed_reg_pct="${BENCH_ALLOWED_REGRESSION_PCT:-${ALLOWED_REGRESSION_PCT:-45}}"

echo "bench-ci: running benchmark (count=$count rounds=$rounds)"
output="$(make -s bench-run BENCH_COUNT="$count" BENCH_ROUNDS="$rounds")"
printf "%s\n" "$output"

put_avg="$(printf "%s\n" "$output" | awk '/^txn_put_dbi\(sorted\):/ {print $2; exit}')"
load_avg="$(printf "%s\n" "$output" | awk '/^txn_load_sorted:/ {print $2; exit}')"
speedup="$(printf "%s\n" "$output" | awk '/^speedup\(load\/put\):/ {gsub(/x/, "", $2); print $2; exit}')"

if [[ -z "$put_avg" || -z "$load_avg" || -z "$speedup" ]]; then
    echo "bench-ci: failed to parse benchmark output" >&2
    exit 2
fi

min_speedup="$(awk -v base="$baseline_speedup" -v pct="$allowed_reg_pct" 'BEGIN { printf "%.6f", base * (1.0 - pct / 100.0) }')"

if ! awk -v s="$speedup" 'BEGIN { exit !(s >= 1.0) }'; then
    echo "bench-ci: FAIL speedup is < 1.0 (current=$speedup)" >&2
    exit 1
fi

if ! awk -v cur="$speedup" -v min="$min_speedup" 'BEGIN { exit !(cur >= min) }'; then
    echo "bench-ci: FAIL speedup regression detected" >&2
    echo "bench-ci: baseline_speedup=$baseline_speedup allowed_regression_pct=$allowed_reg_pct min_speedup=$min_speedup current_speedup=$speedup" >&2
    exit 1
fi

if [[ -n "${BASELINE_PUT_SEC:-}" ]]; then
    max_put="$(awk -v base="$BASELINE_PUT_SEC" -v pct="$allowed_reg_pct" 'BEGIN { printf "%.6f", base * (1.0 + pct / 100.0) }')"
    if ! awk -v cur="$put_avg" -v max="$max_put" 'BEGIN { exit !(cur <= max) }'; then
        echo "bench-ci: FAIL txn_put_dbi(sorted) time regressed (baseline=$BASELINE_PUT_SEC max=$max_put current=$put_avg)" >&2
        exit 1
    fi
fi

if [[ -n "${BASELINE_LOAD_SEC:-}" ]]; then
    max_load="$(awk -v base="$BASELINE_LOAD_SEC" -v pct="$allowed_reg_pct" 'BEGIN { printf "%.6f", base * (1.0 + pct / 100.0) }')"
    if ! awk -v cur="$load_avg" -v max="$max_load" 'BEGIN { exit !(cur <= max) }'; then
        echo "bench-ci: FAIL txn_load_sorted time regressed (baseline=$BASELINE_LOAD_SEC max=$max_load current=$load_avg)" >&2
        exit 1
    fi
fi

echo "bench-ci: PASS (current_speedup=$speedup, min_speedup=$min_speedup)"
