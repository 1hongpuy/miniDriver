#!/usr/bin/env bash
set -euo pipefail

# Per-case Gateway diagnostics for 64 KiB workloads.  Each case has its own
# Gateway/DataNode process lifetime, but the diagnostic counters are reset
# immediately before the 300 s measured window so warmup is excluded.
# This script never deletes an existing directory.

: "${MINIKV_V2_CLUSTER_SECRET:?MINIKV_V2_CLUSTER_SECRET is required}"
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin="${MINIKV_R7_WINDOW_BIN:-${repo_dir}/build/bin/minikv_v2_bench}"
cluster_bin="${MINIKV_V2_BIN_DIR:-${repo_dir}/build/bin}"
root="${MINIKV_R7_WINDOW_ROOT:-/data/minikv-v2/r7-put-gateway-window-$(date +%Y%m%d-%H%M%S)}"
duration="${MINIKV_R7_WINDOW_DURATION_SECONDS:-300}"
warmup="${MINIKV_R7_WINDOW_WARMUP_SECONDS:-30}"
topology="${MINIKV_R7_WINDOW_TOPOLOGY:-shared}"
meta_root="${MINIKV_R7_WINDOW_METADATA_ROOT:-/data/minikv-v2/r7-gateway-metadata-$(date +%Y%m%d-%H%M%S)}"
concurrencies="${MINIKV_R7_WINDOW_CONCURRENCIES:-8 16}"
workload="${MINIKV_R7_WINDOW_WORKLOAD:-upload}"
baseline_snapshot="${MINIKV_R7_WINDOW_BASELINE_SNAPSHOT:-}"
clean_cluster="${MINIKV_R7_WINDOW_CLEAN_CLUSTER:-0}"
clean_metadata="${MINIKV_R7_WINDOW_CLEAN_METADATA:-0}"

[[ -x "${bin}" ]] || { echo "missing benchmark: ${bin}" >&2; exit 2; }
[[ -x "${cluster_bin}/minikv_v2_gateway" && -x "${cluster_bin}/minikv_v2_datanode" ]] || {
    echo "missing Gateway/DataNode binaries under ${cluster_bin}" >&2; exit 2;
}
for tool in pidstat iostat sar curl; do
    command -v "${tool}" >/dev/null 2>&1 || { echo "required tool missing: ${tool}" >&2; exit 3; }
done
if [[ "${topology}" != shared && "${topology}" != split-metadata ]]; then
    echo "MINIKV_R7_WINDOW_TOPOLOGY must be shared or split-metadata" >&2; exit 2
fi
if [[ "${workload}" != upload && "${workload}" != download && "${workload}" != mixed ]]; then
    echo "MINIKV_R7_WINDOW_WORKLOAD must be upload, download, or mixed" >&2; exit 2
fi
if [[ -n "${baseline_snapshot}" ]]; then
    [[ -d "${baseline_snapshot}" ]] || {
        echo "baseline snapshot does not exist: ${baseline_snapshot}" >&2; exit 2;
    }
    [[ -f "${baseline_snapshot}/snapshot-manifest.json" ]] || {
        echo "baseline snapshot is missing snapshot-manifest.json: ${baseline_snapshot}" >&2; exit 2;
    }
fi
if [[ -e "${root}" ]] && [[ -n "$(find "${root}" -mindepth 1 -print -quit 2>/dev/null)" ]] &&
   [[ "${MINIKV_R7_WINDOW_ALLOW_EXISTING_ROOT:-0}" != 1 ]]; then
    echo "diagnostic root is not empty: ${root}" >&2; exit 2
fi
mkdir -p "${root}"
printf 'status=complete\ntopology=%s\nworkload=%s\nduration_seconds=%s\nwarmup_seconds=%s\n' \
    "${topology}" "${workload}" "${duration}" "${warmup}" > "${root}/observability-status.txt"

run_case() {
    local c="$1"
    local case_root="${root}/c${c}"
    local cluster="${case_root}/cluster"
    local result="${case_root}/result"
    local fixture_manifest="${cluster}/fixture-manifest.csv"
    local gateway_port="$((19080 + c))"
    local node_a_port="$((20080 + c))"
    local node_c_port="$((21080 + c))"
    local node_b_port="$((22080 + c))"
    mkdir -p "${case_root}" "${result}"
    if [[ -e "${case_root}/case-status.txt" ]] &&
       grep -q '^status=completed$' "${case_root}/case-status.txt"; then
        echo "skipping completed case c${c}" >&2
        return 0
    fi
    if [[ -e "${cluster}" ]] && [[ -n "$(find "${cluster}" -mindepth 1 -print -quit 2>/dev/null)" ]]; then
        echo "case cluster is not empty; use a new case root for a clean rerun: ${cluster}" >&2
        printf 'status=incomplete\nreason=nonempty_case_cluster\nchecked_at=%s\n' "$(date -Is)" > "${case_root}/case-status.txt"
        return 1
    fi
    printf 'status=running\nlock_mode=%s\ntopology=%s\nworkload=%s\nconcurrency=%s\nstarted_at=%s\n' \
        "${MINIKV_GATEWAY_LOCK_MODE:-global}" "${topology}" "${workload}" "${c}" "$(date -Is)" > "${case_root}/case-status.txt"
    mark_incomplete() {
        printf 'status=incomplete\nlock_mode=%s\ntopology=%s\nworkload=%s\nconcurrency=%s\nfailed_at=%s\n' \
            "${MINIKV_GATEWAY_LOCK_MODE:-global}" "${topology}" "${workload}" "${c}" "$(date -Is)" > "${case_root}/case-status.txt"
    }
    trap mark_incomplete ERR
    mkdir -p "${cluster}"
    if [[ -n "${baseline_snapshot}" ]]; then
        cp -a "${baseline_snapshot}/." "${cluster}/"
    fi
    # A stopped baseline may contain logs from fixture prefill/recovery.  They
    # are not part of this case's steady window and would make slow-lock and
    # timestamp correlation ambiguous after the clone is started.  Remove only
    # copied Gateway log files; the immutable snapshot and all data/metadata
    # files remain untouched.
    if [[ -d "${cluster}/gateway/logs" ]]; then
        find "${cluster}/gateway/logs" -maxdepth 1 -type f -name '*.log' -delete
    fi
    if [[ "${topology}" == split-metadata ]]; then
        # The snapshot contains the shared metadata directory.  Replace the
        # clone's link target only; the immutable baseline is never touched.
        rm -rf "${cluster}/gateway/metadata"
        mkdir -p "${meta_root}/c${c}"
        # Read and mixed workloads need the committed-object metadata from
        # the immutable snapshot.  Pure upload starts with a fresh metadata
        # directory; GET/mixed seed the split target with the same DB state
        # as shared mode so the disk A/B changes only placement, not data.
        if [[ "${workload}" != upload ]]; then
            cp -a "${baseline_snapshot}/gateway/metadata/." "${meta_root}/c${c}/"
        fi
        mkdir -p "${cluster}/gateway"
        ln -s "${meta_root}/c${c}" "${cluster}/gateway/metadata"
    fi

    export MINIKV_V2_BENCH_CLUSTER_DIR="${cluster}"
    export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
    export MINIKV_V2_BENCH_NODE_A_PORT="${node_a_port}"
    export MINIKV_V2_BENCH_NODE_C_PORT="${node_c_port}"
    export MINIKV_V2_BENCH_NODE_B_PORT="${node_b_port}"
    export MINIKV_V2_BENCH_NODE_COUNT=3
    export MINIKV_V4_MAX_ACTIVE_UPLOADS=32
    export MINIKV_V4_MAX_UPLOADS_PER_CLIENT=32
    export MINIKV_V4_MAX_ACTIVE_DOWNLOADS=32
    export MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT=32
    export MINIKV_GATEWAY_MUTEX_DIAGNOSTICS=1
    export MINIKV_HTTP_DIAGNOSTICS=1

    bash "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh" > "${case_root}/cluster-start.log" 2>&1
    cleanup_case() {
        bash "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh" >/dev/null 2>&1 || true
    }
    trap cleanup_case RETURN
    trap 'cleanup_case; printf "status=incomplete\ninterrupted_at=%s\n" "$(date -Is)" > "${case_root}/case-status.txt"; exit 130' INT TERM

    # Warmup is intentionally outside the measured result directory.
    benchmark_args=(--concurrency "${c}" --chunk-window 2 --global-chunk-budget 64
        --connection-mode keep-alive --remote-dir "/r7-window-${workload}/c${c}")
    case "${workload}" in
        upload)
            benchmark_args+=(--mode upload --upload-checksum crc32c)
            ;;
        download)
            [[ -f "${fixture_manifest}" ]] || {
                echo "download workload requires ${fixture_manifest}" >&2; return 1;
            }
            if ! grep -q ',"download",' "${fixture_manifest}"; then
                fixture_manifest="${cluster}/fixture-manifest-download.csv"
                sed 's/,"upload",/,"download",/' "${cluster}/fixture-manifest.csv" > "${fixture_manifest}"
            fi
            benchmark_args+=(--mode download --download-fixture-manifest "${fixture_manifest}"
                --download-verification strict --sdk-read-plan true
                --cluster-internal-token "${MINIKV_V2_CLUSTER_SECRET}"
                --service-principal "r7-window-diagnostic")
            ;;
        mixed)
            [[ -f "${fixture_manifest}" ]] || {
                echo "mixed workload requires ${fixture_manifest}" >&2; return 1;
            }
            if ! grep -q ',"download",' "${fixture_manifest}"; then
                fixture_manifest="${cluster}/fixture-manifest-download.csv"
                sed 's/,"upload",/,"download",/' "${cluster}/fixture-manifest.csv" > "${fixture_manifest}"
            fi
            benchmark_args+=(--mode mixed --read-share 50
                --download-fixture-manifest "${fixture_manifest}"
                --download-verification strict --sdk-read-plan true
                --cluster-internal-token "${MINIKV_V2_CLUSTER_SECRET}"
                --service-principal "r7-window-diagnostic")
            ;;
    esac
    "${bin}" local --gateway "127.0.0.1:${gateway_port}" \
        --work-dir "${case_root}/warmup" --sizes 64KiB \
        --duration-seconds "${warmup}" "${benchmark_args[@]}" > "${case_root}/warmup.log" 2>&1

    curl --noproxy '*' --silent --fail -X POST "http://127.0.0.1:${gateway_port}/internal/v3/admin/diagnostics/reset" \
        -H "X-Cluster-Internal-Token: ${MINIKV_V2_CLUSTER_SECRET}" \
        -H "X-Service-Principal: r7-window-diagnostic" > "${result}/diagnostics-reset.json"

    pidstat -dur 1 > "${result}/pidstat.log" 2>&1 & local p1=$!
    iostat -xt 1 > "${result}/iostat.log" 2>&1 & local p2=$!
    vmstat -t 1 > "${result}/vmstat.log" 2>&1 & local p3=$!
    sar -n DEV 1 > "${result}/sar-net.log" 2>&1 & local p4=$!
    # A non-zero benchmark exit is evidence that must still get diagnostics,
    # monitor logs, and service logs.  Keep it out of the ERR trap so the
    # flush/cleanup path runs before the case is marked incomplete.
    local benchmark_status=0
    benchmark_args+=(--duration-seconds "${duration}")
    if "${bin}" local --gateway "127.0.0.1:${gateway_port}" \
        --work-dir "${result}" --sizes 64KiB "${benchmark_args[@]}" | tee "${result}/console.log"; then
        benchmark_status=0
    else
        benchmark_status=${PIPESTATUS[0]}
    fi
    kill "${p1}" "${p2}" "${p3}" "${p4}" 2>/dev/null || true
    wait "${p1}" "${p2}" "${p3}" "${p4}" 2>/dev/null || true

    local flush_status=0
    if curl --noproxy '*' --silent --fail -X POST "http://127.0.0.1:${gateway_port}/internal/v3/admin/diagnostics/flush" \
        -H "X-Cluster-Internal-Token: ${MINIKV_V2_CLUSTER_SECRET}" \
        -H "X-Service-Principal: r7-window-diagnostic" > "${result}/diagnostics-flush.json"; then
        flush_status=0
    else
        flush_status=$?
    fi
    mkdir -p "${result}/service-logs"
    cp "${cluster}/gateway/logs/gateway.log" "${result}/service-logs/" 2>/dev/null || true
    cp "${cluster}"/gateway/logs/gateway.*.log "${result}/service-logs/" 2>/dev/null || true
    cp "${cluster}"/node-*/logs/datanode-*.log "${result}/service-logs/" 2>/dev/null || true
    cp "${cluster}/logs/gateway.out.log" "${result}/service-logs/" 2>/dev/null || true
    cp "${cluster}/logs/gateway.err.log" "${result}/service-logs/" 2>/dev/null || true
    cp "${cluster}"/logs/node-*.out.log "${result}/service-logs/" 2>/dev/null || true
    cp "${cluster}"/logs/node-*.err.log "${result}/service-logs/" 2>/dev/null || true
    cleanup_case
    trap - RETURN
    trap - ERR
    trap - INT TERM
    if [[ "${benchmark_status}" -ne 0 || "${flush_status}" -ne 0 ]]; then
            printf 'status=incomplete\nlock_mode=%s\ntopology=%s\nworkload=%s\nconcurrency=%s\nbenchmark_status=%s\nflush_status=%s\nfailed_at=%s\n' \
            "${MINIKV_GATEWAY_LOCK_MODE:-global}" "${topology}" "${workload}" "${c}" \
            "${benchmark_status}" "${flush_status}" "$(date -Is)" > "${case_root}/case-status.txt"
        return 1
    fi
    printf 'status=completed\nlock_mode=%s\ntopology=%s\nworkload=%s\nconcurrency=%s\ncompleted_at=%s\n' \
        "${MINIKV_GATEWAY_LOCK_MODE:-global}" "${topology}" "${workload}" "${c}" "$(date -Is)" > "${case_root}/case-status.txt"
    if [[ "${clean_cluster}" == 1 ]]; then
        # The immutable snapshot is the source of truth.  A completed case
        # retains result CSVs, diagnostics, host metrics and copied service
        # logs; the cloned payload is disposable and may be removed to keep
        # the finite test volume usable for the remaining matrix cells.
        rm -rf "${cluster}"
        printf 'cluster_clone=removed\n' >> "${case_root}/case-status.txt"
        if [[ "${clean_metadata}" == 1 && "${topology}" == split-metadata ]]; then
            rm -rf "${meta_root}"
            printf 'metadata_clone=removed\n' >> "${case_root}/case-status.txt"
        fi
    fi
}

for c in ${concurrencies}; do run_case "${c}"; done
printf 'completed root=%s topology=%s\n' "${root}" "${topology}"
