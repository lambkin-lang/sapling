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

to_lower() {
    printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

profile_to_suffix() {
    printf '%s' "$1" | tr '[:lower:]-' '[:upper:]_'
}

is_truthy() {
    case "$(to_lower "${1:-}")" in
        1|true|yes|on)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

detect_profile() {
    local os
    local arch

    os="$(to_lower "$(uname -s)")"
    arch="$(to_lower "$(uname -m)")"

    if [[ "$os" == "darwin" && "$arch" == "arm64" ]]; then
        echo "apple_silicon"
        return
    fi
    if [[ "$os" == "linux" ]]; then
        echo "linux"
        return
    fi
    echo "generic"
}

baseline_value() {
    local key="$1"
    local suffix="$2"
    local profile_key="${key}_${suffix}"
    local value="${!profile_key:-}"

    if [[ -z "$value" ]]; then
        value="${!key:-}"
    fi

    printf '%s' "$value"
}

baseline_profile="${BENCH_BASELINE_PROFILE:-$(detect_profile)}"
baseline_profile_suffix="$(profile_to_suffix "$baseline_profile")"
emit_baseline_update="${BENCH_EMIT_BASELINE_UPDATE:-0}"
baseline_update_file="${BENCH_BASELINE_UPDATE_FILE:-}"

baseline_speedup="${BENCH_BASELINE_SPEEDUP:-$(baseline_value BASELINE_SPEEDUP "$baseline_profile_suffix")}"
if [[ -z "$baseline_speedup" ]]; then
    echo "bench-ci: BASELINE_SPEEDUP is required for profile '$baseline_profile' in $BASELINE_FILE" >&2
    exit 2
fi

count="${BENCH_COUNT:-$(baseline_value BASELINE_COUNT "$baseline_profile_suffix")}"
rounds="${BENCH_ROUNDS:-$(baseline_value BASELINE_ROUNDS "$baseline_profile_suffix")}"
allowed_reg_pct="${BENCH_ALLOWED_REGRESSION_PCT:-$(baseline_value ALLOWED_REGRESSION_PCT "$baseline_profile_suffix")}"
baseline_captured_at="$(baseline_value BASELINE_CAPTURED_AT_UTC "$baseline_profile_suffix")"
baseline_commit="$(baseline_value BASELINE_CAPTURED_COMMIT "$baseline_profile_suffix")"
baseline_host="$(baseline_value BASELINE_CAPTURED_HOSTNAME "$baseline_profile_suffix")"
baseline_machine="$(baseline_value BASELINE_CAPTURED_MACHINE "$baseline_profile_suffix")"
baseline_put_sec="$(baseline_value BASELINE_PUT_SEC "$baseline_profile_suffix")"
baseline_load_sec="$(baseline_value BASELINE_LOAD_SEC "$baseline_profile_suffix")"

if [[ -z "$count" ]]; then
    count="100000"
fi
if [[ -z "$rounds" ]]; then
    rounds="3"
fi
if [[ -z "$allowed_reg_pct" ]]; then
    allowed_reg_pct="45"
fi

if [[ -z "$baseline_captured_at" || -z "$baseline_commit" || -z "$baseline_host" || -z "$baseline_machine" ]]; then
    echo "bench-ci: warning: baseline metadata is incomplete for profile '$baseline_profile' in $BASELINE_FILE" >&2
fi

echo "bench-ci: system diagnostics"
echo "  profile: $baseline_profile"
if [[ -f /proc/loadavg ]]; then
    echo "  loadavg: $(cat /proc/loadavg)"
fi
uptime
if [[ -n "$baseline_captured_at" || -n "$baseline_commit" || -n "$baseline_host" || -n "$baseline_machine" ]]; then
    echo "bench-ci: baseline provenance"
    [[ -n "$baseline_captured_at" ]] && echo "  captured_at_utc: $baseline_captured_at"
    [[ -n "$baseline_commit" ]] && echo "  commit: $baseline_commit"
    [[ -n "$baseline_host" ]] && echo "  host: $baseline_host"
    [[ -n "$baseline_machine" ]] && echo "  machine: $baseline_machine"
fi

echo "bench-ci: running warm-up (10k items)..."
make -s bench-run BENCH_COUNT=10000 BENCH_ROUNDS=1 > /dev/null

echo "bench-ci: running benchmark (count=$count rounds=$rounds)..."
# Use temporary file for more robust parsing
output_log=$(mktemp)
make -s bench-run BENCH_COUNT="$count" BENCH_ROUNDS="$rounds" | tee "$output_log"
output=$(cat "$output_log")
rm -f "$output_log"

# More robust parsing using 'last' match if multiple exist
put_avg="$(printf "%s\n" "$output" | awk '/^txn_put_dbi\(sorted\):/ {val=$2} END {print val}')"
load_avg="$(printf "%s\n" "$output" | awk '/^txn_load_sorted:/ {val=$2} END {print val}')"
speedup="$(printf "%s\n" "$output" | awk '/^speedup\(load\/put\):/ {gsub(/x/, "", $2); val=$2} END {print val}')"

if [[ -z "$put_avg" || -z "$load_avg" || -z "$speedup" ]]; then
    echo "bench-ci: error: failed to parse benchmark output" >&2
    echo "--- output start ---" >&2
    printf "%s\n" "$output" >&2
    echo "--- output end ---" >&2
    exit 2
fi

min_speedup="$(awk -v base="$baseline_speedup" -v pct="$allowed_reg_pct" 'BEGIN { printf "%.6f", base * (1.0 - pct / 100.0) }')"

if ! awk -v s="$speedup" 'BEGIN { exit !(s >= 1.0) }'; then
    echo "bench-ci: FAIL speedup is < 1.0 (current=$speedup)" >&2
    exit 1
fi

if is_truthy "$emit_baseline_update"; then
    now_utc="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    now_commit="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
    now_host="$(hostname 2>/dev/null || echo unknown)"
    now_machine="$(uname -srm 2>/dev/null || echo unknown)"
    speedup_fmt="$(awk -v s="$speedup" 'BEGIN { printf "%.2f", s }')"
    put_fmt="$(awk -v s="$put_avg" 'BEGIN { printf "%.6f", s }')"
    load_fmt="$(awk -v s="$load_avg" 'BEGIN { printf "%.6f", s }')"

    update_text="$(cat <<EOF
# Baseline update suggestion for profile ${baseline_profile} (${baseline_profile_suffix})
BASELINE_CAPTURED_AT_UTC_${baseline_profile_suffix}=${now_utc}
BASELINE_CAPTURED_COMMIT_${baseline_profile_suffix}=${now_commit}
BASELINE_CAPTURED_HOSTNAME_${baseline_profile_suffix}=${now_host}
BASELINE_CAPTURED_MACHINE_${baseline_profile_suffix}="${now_machine}"
BASELINE_COUNT_${baseline_profile_suffix}=${count}
BASELINE_ROUNDS_${baseline_profile_suffix}=${rounds}
BASELINE_SPEEDUP_${baseline_profile_suffix}=${speedup_fmt}
ALLOWED_REGRESSION_PCT_${baseline_profile_suffix}=${allowed_reg_pct}
# Optional absolute thresholds:
BASELINE_PUT_SEC_${baseline_profile_suffix}=${put_fmt}
BASELINE_LOAD_SEC_${baseline_profile_suffix}=${load_fmt}
EOF
)"

    echo "bench-ci: baseline update suggestion"
    printf '%s\n' "$update_text"

    if [[ -n "$baseline_update_file" ]]; then
        printf '%s\n' "$update_text" > "$baseline_update_file"
        echo "bench-ci: wrote baseline update suggestion to $baseline_update_file"
    fi

    echo "bench-ci: PASS (record mode current_speedup=$speedup profile=$baseline_profile)"
    exit 0
fi

if ! awk -v cur="$speedup" -v min="$min_speedup" 'BEGIN { exit !(cur >= min) }'; then
    echo "bench-ci: FAIL speedup regression detected" >&2
    echo "bench-ci: profile=$baseline_profile baseline_speedup=$baseline_speedup allowed_regression_pct=$allowed_reg_pct min_speedup=$min_speedup current_speedup=$speedup" >&2
    exit 1
fi

if [[ -n "$baseline_put_sec" ]]; then
    max_put="$(awk -v base="$baseline_put_sec" -v pct="$allowed_reg_pct" 'BEGIN { printf "%.6f", base * (1.0 + pct / 100.0) }')"
    if ! awk -v cur="$put_avg" -v max="$max_put" 'BEGIN { exit !(cur <= max) }'; then
        echo "bench-ci: FAIL txn_put_dbi(sorted) time regressed (profile=$baseline_profile baseline=$baseline_put_sec max=$max_put current=$put_avg)" >&2
        exit 1
    fi
fi

if [[ -n "$baseline_load_sec" ]]; then
    max_load="$(awk -v base="$baseline_load_sec" -v pct="$allowed_reg_pct" 'BEGIN { printf "%.6f", base * (1.0 + pct / 100.0) }')"
    if ! awk -v cur="$load_avg" -v max="$max_load" 'BEGIN { exit !(cur <= max) }'; then
        echo "bench-ci: FAIL txn_load_sorted time regressed (profile=$baseline_profile baseline=$baseline_load_sec max=$max_load current=$load_avg)" >&2
        exit 1
    fi
fi

echo "bench-ci: PASS (profile=$baseline_profile current_speedup=$speedup min_speedup=$min_speedup)"
