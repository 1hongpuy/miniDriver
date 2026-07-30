#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${MINIKV_V2_CLUSTER_SECRET:-}" ]]; then
    echo "MINIKV_V2_CLUSTER_SECRET is required" >&2
    exit 2
fi

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin_dir="${MINIKV_V2_BIN_DIR:-${repo_dir}/build/bin}"
root_dir="${MINIKV_V2_BENCH_CLUSTER_DIR:-/tmp/minikv-v2-bench-cluster}"
gateway_port="${MINIKV_V2_BENCH_GATEWAY_PORT:-18181}"
node_a_port="${MINIKV_V2_BENCH_NODE_A_PORT:-19102}"
node_c_port="${MINIKV_V2_BENCH_NODE_C_PORT:-19103}"
pid_file="${root_dir}/pids"

for executable in minikv_v2_gateway minikv_v2_datanode; do
    if [[ ! -x "${bin_dir}/${executable}" ]]; then
        echo "missing executable: ${bin_dir}/${executable}; build the project first" >&2
        exit 2
    fi
done

if [[ -f "${pid_file}" ]]; then
    echo "existing benchmark cluster PID file: ${pid_file}" >&2
    echo "run tools/stop_v2_local_benchmark_cluster.sh first" >&2
    exit 2
fi

mkdir -p "${root_dir}/gateway" "${root_dir}/node-a" "${root_dir}/node-c" "${root_dir}/logs"

nohup "${bin_dir}/minikv_v2_gateway" "${gateway_port}" "${root_dir}/gateway" \
    >"${root_dir}/logs/gateway.out.log" 2>"${root_dir}/logs/gateway.err.log" &
gateway_pid=$!

nohup "${bin_dir}/minikv_v2_datanode" "bench-a" "127.0.0.1" "${node_a_port}" \
    "${root_dir}/node-a" "127.0.0.1" "${gateway_port}" \
    >"${root_dir}/logs/node-a.out.log" 2>"${root_dir}/logs/node-a.err.log" &
node_a_pid=$!

nohup "${bin_dir}/minikv_v2_datanode" "bench-c" "127.0.0.1" "${node_c_port}" \
    "${root_dir}/node-c" "127.0.0.1" "${gateway_port}" \
    >"${root_dir}/logs/node-c.out.log" 2>"${root_dir}/logs/node-c.err.log" &
node_c_pid=$!

printf '%s\n' "${gateway_pid}" "${node_a_pid}" "${node_c_pid}" >"${pid_file}"

for _ in $(seq 1 30); do
    if nodes="$(curl --noproxy '*' --silent --fail "http://127.0.0.1:${gateway_port}/api/v2/admin/nodes" 2>/dev/null)"; then
        node_a="${nodes#*\"nodeId\":\"bench-a\"}"
        node_a="${node_a%%\}*}"
        node_c="${nodes#*\"nodeId\":\"bench-c\"}"
        node_c="${node_c%%\}*}"
        if [[ "${node_a}" == *'"state":1'* ]] && [[ "${node_c}" == *'"state":1'* ]]; then
        echo "isolated benchmark cluster ready"
        echo "  Gateway: 127.0.0.1:${gateway_port}"
        echo "  Nodes:   127.0.0.1:${node_a_port}, 127.0.0.1:${node_c_port}"
        echo "  State:   ${root_dir}"
        exit 0
        fi
    fi
    sleep 1
done

echo "cluster did not become ready; inspect ${root_dir}/logs" >&2
exit 1
