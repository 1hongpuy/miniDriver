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
node_count="${MINIKV_V2_BENCH_NODE_COUNT:-2}"
pid_file="${root_dir}/pids"

if [[ ! "${node_count}" =~ ^[2-4]$ ]]; then
    echo "MINIKV_V2_BENCH_NODE_COUNT must be 2, 3, or 4" >&2
    exit 2
fi

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

mkdir -p "${root_dir}/gateway" "${root_dir}/logs"

nohup "${bin_dir}/minikv_v2_gateway" "${gateway_port}" "${root_dir}/gateway" \
    >"${root_dir}/logs/gateway.out.log" 2>"${root_dir}/logs/gateway.err.log" &
gateway_pid=$!

node_ids=(a c b d)
node_ports=("${node_a_port}" "${node_c_port}" "${MINIKV_V2_BENCH_NODE_B_PORT:-19104}" "${MINIKV_V2_BENCH_NODE_D_PORT:-19105}")
pids=("${gateway_pid}")
for ((index = 0; index < node_count; ++index)); do
    node_id="${node_ids[index]}"
    node_port="${node_ports[index]}"
    mkdir -p "${root_dir}/node-${node_id}"
    nohup "${bin_dir}/minikv_v2_datanode" "bench-${node_id}" "127.0.0.1" "${node_port}" \
        "${root_dir}/node-${node_id}" "127.0.0.1" "${gateway_port}" \
        >"${root_dir}/logs/node-${node_id}.out.log" 2>"${root_dir}/logs/node-${node_id}.err.log" &
    pids+=("$!")
done
printf '%s\n' "${pids[@]}" >"${pid_file}"

for _ in $(seq 1 30); do
    if nodes="$(curl --noproxy '*' --silent --fail "http://127.0.0.1:${gateway_port}/api/v2/admin/nodes" 2>/dev/null)"; then
        ready=true
        for ((index = 0; index < node_count; ++index)); do
            node="${nodes#*\"nodeId\":\"bench-${node_ids[index]}\"}"
            node="${node%%\}*}"
            if [[ "${node}" != *'"state":1'* ]]; then ready=false; break; fi
        done
        if [[ "${ready}" == true ]]; then
        echo "isolated benchmark cluster ready"
        echo "  Gateway: 127.0.0.1:${gateway_port}"
        echo "  Nodes:   ${node_count}"
        echo "  State:   ${root_dir}"
        exit 0
        fi
    fi
    sleep 1
done

echo "cluster did not become ready; inspect ${root_dir}/logs" >&2
exit 1
