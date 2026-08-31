#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root="${MINIKV_V3_P5_SMOKE_ROOT:-}"
identity_scheme="${MINIKV_V3_IDENTITY_SCHEME:-opaque-chunk-id}"
checksum_type="${MINIKV_V3_CHECKSUM_TYPE:-crc32c}"
service_principal="${MINIKV_V3_P5_SERVICE_PRINCIPAL:-benchmark-p5-protocol}"

if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2
    exit 2
fi
case "${result_root}" in
    /tmp/minidriver-v3-p5-*) ;;
    *)
        echo "MINIKV_V3_P5_SMOKE_ROOT must match /tmp/minidriver-v3-p5-*" >&2
        exit 2
        ;;
esac
if [[ -e "${result_root}" ]]; then
    echo "P5 smoke root already exists; choose a fresh directory" >&2
    exit 2
fi
if [[ "${identity_scheme}:${checksum_type}" != "cas-sha256:sha256" &&
      "${identity_scheme}:${checksum_type}" != "opaque-chunk-id:sha256" &&
      "${identity_scheme}:${checksum_type}" != "opaque-chunk-id:crc32c" ]]; then
    echo "unsupported identity/checksum pair" >&2
    exit 2
fi

gateway_port="${MINIKV_V3_P5_GATEWAY_PORT:-58381}"
node_a_port="${MINIKV_V3_P5_NODE_A_PORT:-59301}"
node_c_port="${MINIKV_V3_P5_NODE_C_PORT:-59302}"
work_dir="${result_root}/work"
cluster_running=false

stop_cluster()
{
    if [[ "${cluster_running}" == true ]]; then
        "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh"
        cluster_running=false
    fi
}
trap stop_cluster EXIT INT TERM

export MINIKV_V2_BENCH_CLUSTER_DIR="${result_root}/cluster"
export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
export MINIKV_V2_BENCH_NODE_A_PORT="${node_a_port}"
export MINIKV_V2_BENCH_NODE_C_PORT="${node_c_port}"
export MINIKV_V2_BENCH_NODE_COUNT=2
export MINIKV_V3_DURABILITY_MODE=group_commit
export MINIKV_V3_WRITE_BATCH_MODE=pwritev
export MINIKV_V3_WRITE_BATCH_BYTES=262144
export MINIKV_V3_WRITE_BATCH_DELAY_US=1000
export MINIKV_V3_GROUP_COMMIT_BYTES=8388608
export MINIKV_V3_GROUP_COMMIT_ITEMS=8
export MINIKV_V3_GROUP_COMMIT_DELAY_US=2000
export MINIKV_V4_IO_THREADS=2
export MINIKV_V4_MAX_ACTIVE_UPLOADS=8
export MINIKV_V4_MAX_UPLOADS_PER_CLIENT=8
export MINIKV_V4_MAX_ACTIVE_DOWNLOADS=8
export MINIKV_V4_MAX_DOWNLOADS_PER_CLIENT=8
export MINIKV_V4_DISK_WRITE_WORKERS=2
export MINIKV_V4_DISK_WRITE_BLOCKS=128

mkdir -p "${result_root}/results" "${work_dir}"
"${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
cluster_running=true

"${repo_dir}/build/bin/minikv_v2_bench" local \
    --gateway "127.0.0.1:${gateway_port}" \
    --work-dir "${work_dir}" \
    --sizes 64KiB,4MiB,16MiB --runs 2 --concurrency 2 \
    --chunk-window 2 --global-chunk-budget 4 --mode end-to-end \
    --upload-checksum "${checksum_type}" \
    --sdk-read-plan true --cluster-internal-token "${MINIKV_V2_CLUSTER_SECRET}" \
    --service-principal "${service_principal}" \
    --remote-dir "/perf/v3-p5/${identity_scheme}/${checksum_type}" \
    | tee "${result_root}/results/console.log"

cp "${work_dir}/runs.csv" "${work_dir}/summary.csv" "${work_dir}/summary.json" \
    "${result_root}/results/"

for log in "${result_root}"/cluster/node-*/logs/datanode-*.log; do
    if ! rg -q "identity_scheme=${identity_scheme} checksum_type=${checksum_type}" "${log}"; then
        echo "expected protocol fields not found in ${log}" >&2
        exit 1
    fi
done

echo "P5 protocol smoke completed: ${result_root} (${identity_scheme}/${checksum_type})"
