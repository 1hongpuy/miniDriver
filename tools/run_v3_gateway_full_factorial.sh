#!/usr/bin/env bash
set -euo pipefail

# Full Gateway lock/disk/workload factorial. Reuses the per-case runner so
# every cell gets an isolated snapshot clone and complete observability.
: "${MINIKV_V2_CLUSTER_SECRET:?MINIKV_V2_CLUSTER_SECRET is required}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
root="${MINIKV_R7_FULL_FACTORIAL_ROOT:-/data/minikv-v2/r7-full-factorial-$(date +%Y%m%d-%H%M%S)}"
baseline="${MINIKV_R7_FULL_FACTORIAL_BASELINE_SNAPSHOT:-}"
duration="${MINIKV_R7_FULL_FACTORIAL_DURATION_SECONDS:-300}"
warmup="${MINIKV_R7_FULL_FACTORIAL_WARMUP_SECONDS:-30}"
repeats="${MINIKV_R7_FULL_FACTORIAL_REPEATS:-3}"
lock_modes="${MINIKV_R7_FULL_FACTORIAL_LOCK_MODES:-global sharded}"
topologies="${MINIKV_R7_FULL_FACTORIAL_TOPOLOGIES:-shared split-metadata}"
workloads="${MINIKV_R7_FULL_FACTORIAL_WORKLOADS:-upload download mixed}"
upload_concurrencies="${MINIKV_R7_FULL_FACTORIAL_UPLOAD_CONCURRENCIES:-4 8 16}"
download_concurrencies="${MINIKV_R7_FULL_FACTORIAL_DOWNLOAD_CONCURRENCIES:-8 16}"
mixed_concurrencies="${MINIKV_R7_FULL_FACTORIAL_MIXED_CONCURRENCIES:-16}"

for tool in pidstat iostat sar curl; do
    command -v "${tool}" >/dev/null 2>&1 || {
        echo "required observability tool missing: ${tool}" >&2; exit 3;
    }
done
[[ -n "${baseline}" && -f "${baseline}/snapshot-manifest.json" ]] || {
    echo "MINIKV_R7_FULL_FACTORIAL_BASELINE_SNAPSHOT must point to a snapshot" >&2; exit 2;
}
if [[ -e "${root}" && -n "$(find "${root}" -mindepth 1 -print -quit 2>/dev/null)" &&
      ! -f "${root}/run-config.txt" ]]; then
    echo "factorial root is not empty: ${root}" >&2; exit 2
fi
mkdir -p "${root}"
printf 'status=ready\nbaseline=%s\nduration_seconds=%s\nwarmup_seconds=%s\nrepeats=%s\n' \
    "${baseline}" "${duration}" "${warmup}" "${repeats}" > "${root}/run-config.txt"

run_cell() {
    local repeat="$1" lock="$2" topology="$3" workload="$4" concurrency="$5"
    local cell="${root}/repeat-${repeat}/${lock}-${topology}-${workload}-c${concurrency}"
    mkdir -p "${root}/repeat-${repeat}"
    if [[ -f "${cell}/c${concurrency}/case-status.txt" ]] &&
       grep -q '^status=completed$' "${cell}/c${concurrency}/case-status.txt"; then
        echo "skip completed ${cell}"; return 0
    fi
    echo "=== repeat=${repeat} lock=${lock} topology=${topology} workload=${workload} c=${concurrency} ==="
    MINIKV_GATEWAY_LOCK_MODE="${lock}" \
    MINIKV_R7_WINDOW_ROOT="${cell}" \
    MINIKV_R7_WINDOW_TOPOLOGY="${topology}" \
    MINIKV_R7_WINDOW_WORKLOAD="${workload}" \
    MINIKV_R7_WINDOW_METADATA_ROOT="${cell}/metadata" \
    MINIKV_R7_WINDOW_CONCURRENCIES="${concurrency}" \
    MINIKV_R7_WINDOW_BASELINE_SNAPSHOT="${baseline}" \
    MINIKV_R7_WINDOW_CLEAN_CLUSTER=1 \
    MINIKV_R7_WINDOW_CLEAN_METADATA=1 \
    MINIKV_R7_WINDOW_ALLOW_EXISTING_ROOT=1 \
    MINIKV_R7_WINDOW_DURATION_SECONDS="${duration}" \
    MINIKV_R7_WINDOW_WARMUP_SECONDS="${warmup}" \
        bash "${repo_dir}/tools/run_v3_r7_put_gateway_window_diagnostic.sh"
}

overall_failed=0
for repeat in $(seq 1 "${repeats}"); do
    if (( repeat % 2 == 1 )); then mode_order="${lock_modes}"; else
        mode_order="${lock_modes}"
        if [[ " ${lock_modes} " == *" global "* && " ${lock_modes} " == *" sharded "* ]]; then
            mode_order="sharded global"
        fi
    fi
    topo_order="${topologies}"
    for topology in ${topo_order}; do
        for workload in ${workloads}; do
            case "${workload}" in
                upload) concurrencies="${upload_concurrencies}" ;;
                download) concurrencies="${download_concurrencies}" ;;
                mixed) concurrencies="${mixed_concurrencies}" ;;
                *) echo "unknown workload: ${workload}" >&2; exit 2 ;;
            esac
            for concurrency in ${concurrencies}; do
                # Keep global/sharded adjacent for the same disk/workload/
                # concurrency so device drift is not mistaken for lock gain.
                for lock in ${mode_order}; do
                    if ! run_cell "${repeat}" "${lock}" "${topology}" "${workload}" "${concurrency}"; then
                        overall_failed=1
                        echo "cell incomplete; preserving evidence" >&2
                    fi
                done
            done
        done
    done
done
printf 'factorial complete root=%s repeats=%s duration=%s warmup=%s\n' \
    "${root}" "${repeats}" "${duration}" "${warmup}"
exit "${overall_failed}"
