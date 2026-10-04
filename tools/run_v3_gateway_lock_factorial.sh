#!/usr/bin/env bash
set -euo pipefail

# Runs the four lock/disk cells.  The underlying per-case runner owns service
# startup/teardown and enforces pidstat/iostat/sar observability gates.
: "${MINIKV_V2_CLUSTER_SECRET:?MINIKV_V2_CLUSTER_SECRET is required}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
root="${MINIKV_R7_FACTORIAL_ROOT:-/data/minikv-v2/r7-factorial-$(date +%Y%m%d-%H%M%S)}"
duration="${MINIKV_R7_FACTORIAL_DURATION_SECONDS:-300}"
warmup="${MINIKV_R7_FACTORIAL_WARMUP_SECONDS:-30}"
repeats="${MINIKV_R7_FACTORIAL_REPEATS:-3}"
lock_modes="${MINIKV_R7_FACTORIAL_LOCK_MODES:-global sharded}"
topologies="${MINIKV_R7_FACTORIAL_TOPOLOGIES:-shared split-metadata}"
concurrencies="${MINIKV_R7_FACTORIAL_CONCURRENCIES:-8 16}"
baseline_snapshot="${MINIKV_R7_FACTORIAL_BASELINE_SNAPSHOT:-}"
require_snapshot="${MINIKV_R7_FACTORIAL_REQUIRE_SNAPSHOT:-1}"

for tool in pidstat iostat sar; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "required observability tool missing: $tool" >&2
    echo "factorial run is not eligible for before/after comparison" >&2
    exit 3
  }
done

if [[ "${require_snapshot}" == 1 && -z "${baseline_snapshot}" ]]; then
  echo "MINIKV_R7_FACTORIAL_BASELINE_SNAPSHOT is required for formal runs" >&2
  exit 2
fi
if [[ -n "${baseline_snapshot}" && ! -f "${baseline_snapshot}/snapshot-manifest.json" ]]; then
  echo "baseline snapshot is missing snapshot-manifest.json: ${baseline_snapshot}" >&2
  exit 2
fi

mkdir -p "$root"
overall_failed=0
for repeat in $(seq 1 "$repeats"); do
  if (( repeat == 2 )); then
    mode_order="sharded global"
  else
    mode_order="global sharded"
  fi
  if (( repeat >= 3 )); then
    c_order=""
    for c in ${concurrencies}; do c_order="$c $c_order"; done
  else
    c_order="$concurrencies"
  fi
  for concurrency in ${c_order}; do
    for lock_mode in ${mode_order}; do
      [[ " ${lock_modes} " == *" ${lock_mode} "* ]] || continue
      for topology in ${topologies}; do
        [[ "$topology" == shared || "$topology" == split-metadata ]] || continue
        cell="$root/repeat-${repeat}/${lock_mode}-${topology}-c${concurrency}"
        mkdir -p "$cell"
        echo "=== repeat=${repeat} lock=${lock_mode} topology=${topology} concurrency=${concurrency} ==="
        if ! MINIKV_GATEWAY_LOCK_MODE="$lock_mode" \
          MINIKV_R7_WINDOW_ROOT="$cell" \
          MINIKV_R7_WINDOW_TOPOLOGY="$topology" \
          MINIKV_R7_WINDOW_METADATA_ROOT="$cell/metadata" \
          MINIKV_R7_WINDOW_CONCURRENCIES="$concurrency" \
          MINIKV_R7_WINDOW_BASELINE_SNAPSHOT="$baseline_snapshot" \
          MINIKV_R7_WINDOW_CLEAN_CLUSTER=1 \
          MINIKV_R7_WINDOW_CLEAN_METADATA=1 \
          MINIKV_R7_WINDOW_DURATION_SECONDS="$duration" \
          MINIKV_R7_WINDOW_WARMUP_SECONDS="$warmup" \
          bash "$repo_dir/tools/run_v3_r7_put_gateway_window_diagnostic.sh"; then
          overall_failed=1
          echo "case incomplete; preserving evidence and continuing schedule" >&2
        fi
      done
    done
  done
done
printf 'factorial complete root=%s repeats=%s duration=%s warmup=%s\n' \
  "$root" "$repeats" "$duration" "$warmup"
exit "$overall_failed"
