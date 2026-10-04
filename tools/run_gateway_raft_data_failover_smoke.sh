#!/usr/bin/env bash
set -euo pipefail

# Upload through Gateway-1, stop it, then strict ReadPlan-read through
# Gateway-2 while the same metadata quorum and DataNodes stay alive.
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
metadata_bin="${1:-${repo_dir}/build-ha/bin/minikv_metadata_raft_service}"
bin_dir="${repo_dir}/build-ha/bin"
run_dir="$(mktemp -d /tmp/minidriver-gateway-raft-failover.XXXXXX)"
secret="gateway-data-failover-secret"
meta_pids=()
gateway2_pid=""
cluster_started=0
cleanup() {
  if [[ -n "${gateway2_pid}" ]]; then kill -TERM "${gateway2_pid}" 2>/dev/null || true; wait "${gateway2_pid}" 2>/dev/null || true; fi
  if [[ "${cluster_started}" == 1 ]]; then
    MINIKV_V2_BENCH_CLUSTER_DIR="${run_dir}/cluster" \
    MINIKV_V2_BIN_DIR="${bin_dir}" \
    bash "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh" >/dev/null 2>&1 || true
  fi
  for pid in "${meta_pids[@]:-}"; do kill -TERM "${pid}" 2>/dev/null || true; done
  for pid in "${meta_pids[@]:-}"; do wait "${pid}" 2>/dev/null || true; done
}
trap cleanup EXIT

members="1@127.0.0.1:29801,2@127.0.0.1:29802,3@127.0.0.1:29803"
for member in 1 2 3; do
  MINIKV_METADATA_MEMBER_ID="${member}" \
  MINIKV_METADATA_RAFT_PORT="$((29800 + member))" \
  MINIKV_METADATA_RAFT_MEMBERS="${members}" \
  MINIKV_METADATA_PORT="$((29900 + member))" \
  MINIKV_METADATA_DIR="${run_dir}/meta-${member}" \
  MINIKV_V2_CLUSTER_SECRET="${secret}" \
  "${metadata_bin}" >"${run_dir}/meta-${member}.log" 2>&1 &
  meta_pids+=("$!")
done

leader_ready=0
for attempt in $(seq 1 100); do
  for member in 1 2 3; do
    if curl --silent --max-time 1 "http://127.0.0.1:$((29900 + member))/readyz" |
        grep -q '"role":"leader"'; then leader_ready=1; break 2; fi
  done
  sleep 0.1
done
[[ "${leader_ready}" == 1 ]] || { echo "metadata election failed: ${run_dir}" >&2; exit 1; }

export MINIKV_V2_CLUSTER_SECRET="${secret}"
export MINIKV_METADATA_MODE=raft
export MINIKV_METADATA_ENDPOINTS=127.0.0.1:29901,127.0.0.1:29902,127.0.0.1:29903
export MINIKV_V2_BIN_DIR="${bin_dir}"
export MINIKV_V2_BENCH_CLUSTER_DIR="${run_dir}/cluster"
export MINIKV_V2_BENCH_GATEWAY_PORT=29781
export MINIKV_V2_BENCH_NODE_A_PORT=30701
export MINIKV_V2_BENCH_NODE_C_PORT=30702
export MINIKV_V2_BENCH_NODE_COUNT=2
export MINIKV_V2_BENCH_ADVERTISE_ADDRESS=127.0.0.1
export MINIKV_V3_REPLICATION_FACTOR=2
export MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id
export MINIKV_V3_CHECKSUM_TYPE=crc32c
export MINIKV_V3_DURABILITY_MODE=group_commit

bash "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh"
cluster_started=1

upload_dir="${run_dir}/upload"
mkdir -p "${upload_dir}"
"${bin_dir}/minikv_v2_bench" local \
  --gateway 127.0.0.1:29781 --work-dir "${upload_dir}" \
  --sizes 64KiB --runs 1 --mode end-to-end --concurrency 1 \
  --requests-per-worker 1 --chunk-window 1 --global-chunk-budget 2 \
  --upload-checksum crc32c --connection-mode keep-alive \
  --download-verification strict --sdk-read-plan true \
  --cluster-internal-token "${secret}" --service-principal failover-smoke \
  --remote-dir /phase3-failover

object_id="$(awk -F',' 'NR == 2 { gsub(/"/, "", $4); print $4 }' "${upload_dir}/runs.csv")"
object_version="$(awk -F',' 'NR == 2 { gsub(/"/, "", $5); print $5 }' "${upload_dir}/runs.csv")"
input_hash="$(awk -F',' 'NR == 2 { gsub(/"/, "", $8); print $8 }' "${upload_dir}/runs.csv")"
[[ -n "${object_id}" && -n "${object_version}" && -n "${input_hash}" ]] || {
  echo "upload fixture was not produced: ${run_dir}" >&2; exit 1;
}

gateway1_pid="$(head -n 1 "${run_dir}/cluster/pids")"
kill -TERM "${gateway1_pid}"
wait "${gateway1_pid}" 2>/dev/null || true

MINIKV_V2_CLUSTER_SECRET="${secret}" \
MINIKV_METADATA_MODE=raft \
MINIKV_METADATA_ENDPOINTS=127.0.0.1:29901,127.0.0.1:29902,127.0.0.1:29903 \
MINIKV_V3_REPLICATION_FACTOR=2 \
MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id \
MINIKV_V3_CHECKSUM_TYPE=crc32c \
MINIKV_V2_LOG_FILE="${run_dir}/gateway-2.log" \
"${bin_dir}/minikv_v2_gateway" 29782 "${run_dir}/gateway-2" "${secret}" >"${run_dir}/gateway-2.out" 2>&1 &
gateway2_pid="$!"

for attempt in $(seq 1 50); do
  if curl --silent --fail "http://127.0.0.1:29782/readyz" >/dev/null 2>&1; then break; fi
  sleep 0.1
done
curl --silent --fail "http://127.0.0.1:29782/readyz" >/dev/null

download_path="${run_dir}/failover.download"
"${bin_dir}/minidriver_client_worker" read \
  --gateway 127.0.0.1:29782 --cluster-token "${secret}" \
  --service-principal failover-smoke --object-id "${object_id}" \
  --object-version "${object_version}" --output "${download_path}"
download_hash="$(sha256sum "${download_path}" | awk '{print $1}')"
[[ "${download_hash}" == "${input_hash}" ]] || {
  echo "strict failover read hash mismatch: expected=${input_hash} actual=${download_hash}" >&2; exit 1;
}

# Phase 4 lifecycle checks use the same surviving Gateway and Raft state.  The
# delete command intentionally leaves physical replica cleanup to the normal
# DataNode heartbeat/ack path; the metadata assertion here verifies that the
# catalog entry is removed and the object enters the replicated deleting state.
directory_response="$(curl --fail --silent -X POST \
  -H 'Content-Type: application/json' \
  -d '{"parentPath":"/","name":"lifecycle"}' \
  "http://127.0.0.1:29782/api/v2/directories")"
[[ "${directory_response}" == *'"path":"/lifecycle"'* ]] || {
  echo "remote CreateDirectory failed: ${directory_response}" >&2; exit 1;
}
root_catalog="$(curl --fail --silent "http://127.0.0.1:29782/api/v2/catalog?path=/")"
[[ "${root_catalog}" == *'/lifecycle'* ]] || {
  echo "remote catalog did not contain /lifecycle: ${root_catalog}" >&2; exit 1;
}

delete_response="$(curl --fail --silent -X DELETE \
  "http://127.0.0.1:29782/api/v2/objects/${object_id}")"
[[ "${delete_response}" == *'"status":"deleted"'* ]] || {
  echo "remote DeleteObject failed: ${delete_response}" >&2; exit 1;
}
object_after_delete="${run_dir}/object-after-delete.json"
object_status="$(curl --silent --output "${object_after_delete}" --write-out '%{http_code}' \
  "http://127.0.0.1:29782/api/v2/objects/${object_id}")"
[[ "${object_status}" == "404" ]] || {
  echo "deleted object remained readable: status=${object_status} body=$(cat "${object_after_delete}")" >&2; exit 1;
}
deleted_catalog="$(curl --fail --silent "http://127.0.0.1:29782/api/v2/catalog?path=/")"
[[ "${deleted_catalog}" != *"${object_id}"* ]] || {
  echo "deleted object remained in catalog: ${deleted_catalog}" >&2; exit 1;
}

echo "Gateway Raft data/lifecycle failover smoke passed: upload=gw-1 read/delete=gw-2 RF2 strict artifacts=${run_dir}"
