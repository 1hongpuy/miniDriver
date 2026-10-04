#!/usr/bin/env bash
set -euo pipefail

# Control-plane smoke only: no DataNode body is sent.  It verifies that two
# independent Gateway frontends use the same three-member Metadata Raft state
# for node registration, ONLINE transition, session creation and failover.
metadata_bin="${1:-build-ha/bin/minikv_metadata_raft_service}"
gateway_bin="${2:-build-ha/bin/minikv_v2_gateway}"
run_dir="$(mktemp -d /tmp/minidriver-gateway-raft-smoke.XXXXXX)"
secret="gateway-smoke-secret"
meta_pids=()
gw_pids=()
cleanup() {
  for pid in "${gw_pids[@]:-}"; do kill -TERM "${pid}" 2>/dev/null || true; done
  for pid in "${meta_pids[@]:-}"; do kill -TERM "${pid}" 2>/dev/null || true; done
  for pid in "${gw_pids[@]:-}"; do wait "${pid}" 2>/dev/null || true; done
  for pid in "${meta_pids[@]:-}"; do wait "${pid}" 2>/dev/null || true; done
}
trap cleanup EXIT

members="1@127.0.0.1:28401,2@127.0.0.1:28402,3@127.0.0.1:28403"
for member in 1 2 3; do
  MINIKV_METADATA_MEMBER_ID="${member}" \
  MINIKV_METADATA_RAFT_PORT="$((28400 + member))" \
  MINIKV_METADATA_RAFT_MEMBERS="${members}" \
  MINIKV_METADATA_PORT="$((28500 + member))" \
  MINIKV_METADATA_DIR="${run_dir}/meta-${member}" \
  MINIKV_V2_CLUSTER_SECRET="${secret}" \
  "${metadata_bin}" >"${run_dir}/meta-${member}.log" 2>&1 &
  meta_pids+=("$!")
done

for attempt in $(seq 1 80); do
  ready=0
  for member in 1 2 3; do
    if curl --silent --max-time 1 "http://127.0.0.1:$((28500 + member))/readyz" |
        grep -q '"role":"leader"'; then ready=1; break; fi
  done
  [[ "${ready}" == 1 ]] && break
  sleep 0.1
done
[[ "${ready:-0}" == 1 ]] || { echo "metadata leader election failed: ${run_dir}" >&2; exit 1; }

metadata_endpoints="127.0.0.1:28501,127.0.0.1:28502,127.0.0.1:28503"
for gateway in 1 2; do
  MINIKV_METADATA_MODE=raft \
  MINIKV_METADATA_ENDPOINTS="${metadata_endpoints}" \
  MINIKV_V2_CLUSTER_SECRET="${secret}" \
  MINIKV_GATEWAY_ID="gw-${gateway}" \
  MINIKV_V3_REPLICATION_FACTOR=2 \
  MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id \
  MINIKV_V3_CHECKSUM_TYPE=crc32c \
  "${gateway_bin}" "$((28600 + gateway))" "${run_dir}/gw-${gateway}" \
    >"${run_dir}/gw-${gateway}.log" 2>&1 &
  gw_pids+=("$!")
done

for port in 28601 28602; do
  for attempt in $(seq 1 80); do
    if curl --silent --max-time 1 "http://127.0.0.1:${port}/healthz" | grep -q '"status":"ok"'; then break; fi
    sleep 0.1
  done
  curl --fail --silent --max-time 2 "http://127.0.0.1:${port}/healthz" >/dev/null
done

register_node() {
  local gateway_port="$1" node_id="$2" data_port="$3"
  local register_response
  register_response="$(curl --silent --show-error --max-time 3 -X POST \
    -H "X-Cluster-Internal-Token: ${secret}" -H 'Content-Type: application/json' \
    -d "{\"nodeId\":\"${node_id}\",\"address\":\"127.0.0.1\",\"httpPort\":${data_port},\"maxStorageBytes\":1073741824,\"capabilities\":\"storage,rf2\"}" \
    "http://127.0.0.1:${gateway_port}/api/v2/nodes/register")"
  if [[ "${register_response}" != *'"status":"registered"'* ]]; then
    echo "registration failed for ${node_id}: ${register_response}" >&2
    return 1
  fi
  local heartbeat_response
  heartbeat_response="$(curl --silent --show-error --max-time 3 -X POST \
    -H "X-Cluster-Internal-Token: ${secret}" -H 'Content-Type: application/json' \
    -d '{"freeBytes":1073741824,"activeUploads":0,"usedBytes":0,"cpuPermille":10,"memoryPermille":10,"diskIoPermille":10,"netOutMbps":0}' \
    "http://127.0.0.1:${gateway_port}/api/v2/nodes/${node_id}/heartbeat")"
  if [[ "${heartbeat_response}" != *'"status":"ok"'* ]]; then
    echo "heartbeat failed for ${node_id}: ${heartbeat_response}" >&2
    return 1
  fi
}
register_node 28601 dn-smoke-1 29601
register_node 28601 dn-smoke-2 29602

preflight_body='{"commandId":"upload-command-1","fileName":"cat.jpg","dirPath":"/photo","fileSize":65536,"chunkSize":65536,"manifestHash":"manifest-1","chunks":[{"index":0,"hash":"chunk-1","size":65536,"checksumDigest":"deadbeef"}]}'
preflight="$(curl --silent --show-error --max-time 3 -X POST \
  -H "X-Cluster-Internal-Token: ${secret}" -H 'Content-Type: application/json' \
  -H 'X-Minidriver-Command-Id: upload-command-1' -d "${preflight_body}" \
  http://127.0.0.1:28601/api/v2/upload/preflight)"
if [[ "${preflight}" != *'"sessionId"'* ]]; then
  echo "preflight failed: ${preflight}" >&2; exit 1
fi
session_id="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["sessionId"])' <<<"${preflight}")"
[[ -n "${session_id}" ]] || { echo "preflight returned no session: ${preflight}" >&2; exit 1; }

route_body='{"chunks":[{"index":0,"hash":"chunk-1","size":65536,"checksumDigest":"deadbeef"}]}'
route="$(curl --silent --show-error --max-time 3 -X POST \
  -H "X-Cluster-Internal-Token: ${secret}" -H 'Content-Type: application/json' \
  -d "${route_body}" "http://127.0.0.1:28601/api/v2/upload/sessions/${session_id}/routes")"
if [[ "${route}" != *'"routes"'* ]]; then echo "route planning failed: ${route}" >&2; exit 1; fi

kill -KILL "${gw_pids[0]}" 2>/dev/null || true
wait "${gw_pids[0]}" 2>/dev/null || true
session_after_failover="$(curl --fail --silent --max-time 3 \
  -H "X-Cluster-Internal-Token: ${secret}" \
  "http://127.0.0.1:28602/api/v2/upload/sessions/${session_id}")"
[[ "${session_after_failover}" == *"${session_id}"* ]] || {
  echo "session not readable after Gateway failover: ${session_after_failover}" >&2; exit 1;
}

echo "Gateway Raft smoke passed: shared_session=${session_id} route=OK gateway_failover=OK artifacts=${run_dir}"
