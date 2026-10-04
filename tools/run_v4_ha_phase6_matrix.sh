#!/usr/bin/env bash
set -euo pipefail

# Phase 6 A/B/C benchmark harness.  The default values match the release
# matrix in the 4.0-HA plan (30 s warmup, 300 s steady, three interleaved
# repeats).  For local smoke use, override PHASE6_* explicitly; the script
# records the exact values in manifest.tsv and never labels a short run as a
# release result.

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
bin_dir="${MINIKV_V2_BIN_DIR:-${repo_dir}/build-ha/bin}"
output_root="${PHASE6_OUTPUT_DIR:-$(mktemp -d /tmp/minidriver-v4-phase6.XXXXXX)}"
warmup="${PHASE6_WARMUP_SECONDS:-30}"
steady="${PHASE6_STEADY_SECONDS:-300}"
repeats="${PHASE6_REPEATS:-3}"
sizes="${PHASE6_SIZES:-64KiB,16MiB}"
concurrencies="${PHASE6_CONCURRENCIES:-1,4,8,16}"
topologies="${PHASE6_TOPOLOGIES:-legacy,raft,ha}"
operations="${PHASE6_OPERATIONS:-upload,download}"
chunk_window="${PHASE6_CHUNK_WINDOW:-1}"
global_chunk_budget="${PHASE6_GLOBAL_CHUNK_BUDGET:-2}"
replication_factor="${PHASE6_REPLICATION_FACTOR:-2}"
haproxy_bin="${HAPROXY_BIN:-$(command -v haproxy || true)}"
secret="${MINIKV_V2_CLUSTER_SECRET:-phase6-local-secret}"

case "${operations}" in
    upload|download|upload,download|download,upload) ;;
    *) echo "invalid PHASE6_OPERATIONS=${operations}; use upload,download, upload, or download" >&2; exit 2 ;;
esac

if ! [[ "${warmup}" =~ ^[0-9]+$ && "${steady}" =~ ^[0-9]+$ &&
        "${repeats}" =~ ^[1-9][0-9]*$ && "${warmup}" -lt "${steady}" ]]; then
    echo "invalid Phase 6 timing: warmup=${warmup}s steady=${steady}s repeats=${repeats}; require 0 <= warmup < steady and repeats >= 1" >&2
    exit 2
fi
if ! [[ "${chunk_window}" =~ ^[1-9][0-9]*$ && "${global_chunk_budget}" =~ ^[1-9][0-9]*$ ]]; then
    echo "invalid Phase 6 chunk limits: chunk_window=${chunk_window} global_chunk_budget=${global_chunk_budget}; require positive integers" >&2
    exit 2
fi
if ! [[ "${replication_factor}" =~ ^[1-9][0-9]*$ && "${replication_factor}" -le 3 ]]; then
    echo "invalid Phase 6 replication factor: ${replication_factor}; require 1..3" >&2
    exit 2
fi
if [[ -z "${PHASE6_PORT_BASE:-}" ]]; then
    # MiniDriver's test HTTP listener does not rely on SO_REUSEADDR, so a
    # previous short run can leave a port in TIME_WAIT. Pick one contiguous
    # block whose ports are currently bindable and derive all topology ports
    # from it. Explicit PHASE6_*_PORT overrides remain available for CI.
    phase6_port_base="$(python3 - <<'PY'
import secrets
import socket

# Keep the derived service ports below Linux's usual ephemeral client-port
# range (32768+).  Otherwise a legacy/raft benchmark connection can pick a
# future HA listener port as its local ephemeral port and leave it in
# TIME_WAIT before the HA topology starts.
candidates = list(range(20000, 28000, 37))
secrets.SystemRandom().shuffle(candidates)
occupied = set()
for proc_path in ("/proc/net/tcp", "/proc/net/tcp6"):
    try:
        with open(proc_path, "r", encoding="ascii") as proc:
            next(proc)
            for line in proc:
                fields = line.split()
                if len(fields) < 4:
                    continue
                try:
                    port = int(fields[1].rsplit(":", 1)[1], 16)
                except (ValueError, IndexError):
                    continue
                if fields[3] in {"01", "04", "05", "06", "0A"}:
                    occupied.add(port)
    except OSError:
        pass
for base in candidates:
    offsets = [0, 1, 2, 3, 100, 101, 102, 103, 200, 201, 202, 203,
               300, 301, 302, 303, 400, 401, 402, 403, 500, 501, 502, 503,
               600, 601, 602, 603, 700, 701, 702, 703, 800,
               1000, 1001, 1002, 1003, 1300, 1301, 1302, 1303,
               1700, 1701, 1702, 1703]
    sockets = []
    if any(base + offset in occupied for offset in offsets):
        continue
    try:
        for offset in offsets:
            sock = socket.socket()
            sock.bind(("127.0.0.1", base + offset))
            sockets.append(sock)
        print(base)
        break
    except OSError:
        for sock in sockets:
            sock.close()
else:
    raise SystemExit("no free Phase 6 port block")
for sock in sockets:
    sock.close()
PY
)"
else
    phase6_port_base="${PHASE6_PORT_BASE}"
fi
legacy_gateway_port="${PHASE6_LEGACY_GATEWAY_PORT:-$((phase6_port_base + 0))}"
raft_meta_base="${PHASE6_RAFT_META_BASE:-$((phase6_port_base + 100))}"
raft_gateway_port="${PHASE6_RAFT_GATEWAY_PORT:-$((phase6_port_base + 300))}"
ha_meta_base="${PHASE6_HA_META_BASE:-$((phase6_port_base + 500))}"
ha_gateway_port="${PHASE6_HA_GATEWAY_PORT:-$((phase6_port_base + 700))}"
ha_proxy_port="${PHASE6_HA_PROXY_PORT:-$((phase6_port_base + 800))}"

mkdir -p "${output_root}"
manifest="${output_root}/manifest.tsv"
phase6_resume="${PHASE6_RESUME:-0}"
if [[ "${phase6_resume}" == 1 && -s "${manifest}" ]]; then
    # Keep an existing manifest so an interrupted long matrix can continue.
    # Cases with two rows (upload + download) are skipped, including cases
    # whose measured status is `failed`: a failed 300-second case is still a
    # completed experiment and must not be silently rerun on every resume.
    # Partially written cases (fewer than two rows) are rerun and remain
    # auditable.
    :
else
    printf 'topology\trepeat\tsize\tconcurrency\toperation\twarmup_s\tsteady_s\tstatus\twork_dir\n' >"${manifest}"
fi

declare -A phase6_done_cases=()
if [[ "${phase6_resume}" == 1 && -s "${manifest}" ]]; then
    while IFS=$'\t' read -r topo rep sz conc op _warm _steady status _work; do
        [[ "${topo}" == "topology" || -z "${topo}" ]] && continue
        key="${topo}|${rep}|${sz}|${conc}"
        phase6_done_cases["${key}"]=$(( ${phase6_done_cases["${key}"]:-0} + 1 ))
    done <"${manifest}"
fi

# Record the storage identity used by the benchmark.  A directory name is not
# enough to establish a device A/B: WAL and DataNode files may still share the
# same underlying block device.  The report is diagnostic metadata only; the
# evaluator still decides the throughput gate.
storage_report="${output_root}/storage.tsv"
printf 'path\tdevice_id\tfstype\tmount_source\n' >"${storage_report}"
record_storage_identity() {
    local path="$1"
    local device_id="unknown" fstype="unknown" mount_source="unknown"
    if [[ -d "${path}" ]]; then
        device_id="$(stat -c '%d' "${path}" 2>/dev/null || echo unknown)"
        fstype="$(stat -f -c '%T' "${path}" 2>/dev/null || echo unknown)"
        if command -v findmnt >/dev/null 2>&1; then
            mount_source="$(findmnt -T "${path}" -no SOURCE 2>/dev/null || echo unknown)"
        fi
    fi
    printf '%s\t%s\t%s\t%s\n' "${path}" "${device_id}" "${fstype}" "${mount_source}" >>"${storage_report}"
}
# By default the benchmark cluster lives under the output root and therefore
# shares the repository device.  Long 16 MiB cases can be much larger than the
# report files, so callers may place the runner-owned DataNode cluster on a
# separate test volume.  Record that data volume as the benchmark device (not
# merely the directory containing the script) so the WAL A/B remains auditable.
if [[ -n "${PHASE6_CLUSTER_DATA_ROOT:-}" ]]; then
    mkdir -p "${PHASE6_CLUSTER_DATA_ROOT}"
    record_storage_identity "${PHASE6_CLUSTER_DATA_ROOT}"
else
    record_storage_identity "${repo_dir}"
fi
if [[ -n "${PHASE6_METADATA_WAL_DIR_ROOT:-}" ]]; then
    mkdir -p "${PHASE6_METADATA_WAL_DIR_ROOT}"
    record_storage_identity "${PHASE6_METADATA_WAL_DIR_ROOT}"
fi

declare -a meta_pids=()
declare -a case_monitor_pids=()
gateway2_pid=""
gateway3_pid=""
haproxy_pid=""
cluster_started=0
active_cluster_dir=""
active_meta_dir=""
matrix_failed=0
phase6_case_number=0

stop_pid() { local pid="${1:-}"; [[ -n "${pid}" ]] && kill -TERM "${pid}" 2>/dev/null || true; }
wait_pid() { local pid="${1:-}"; [[ -n "${pid}" ]] && wait "${pid}" 2>/dev/null || true; }

stop_case_monitors() {
    local pid=""
    for pid in "${case_monitor_pids[@]:-}"; do stop_pid "${pid}"; done
    for pid in "${case_monitor_pids[@]:-}"; do wait_pid "${pid}"; done
    case_monitor_pids=()
}

start_case_monitors() {
    local work="$1"
    [[ "${PHASE6_CAPTURE_HOST_METRICS:-0}" == 1 ]] || return 0
    stop_case_monitors

    local metrics_dir="${work}/host-metrics"
    local pid="" pid_csv=""
    local -a server_pids=()
    declare -A seen_pids=()
    mkdir -p "${metrics_dir}"

    for pid in "${meta_pids[@]:-}"; do
        [[ "${pid}" =~ ^[0-9]+$ && -z "${seen_pids[${pid}]:-}" ]] || continue
        kill -0 "${pid}" 2>/dev/null || continue
        seen_pids["${pid}"]=1
        server_pids+=("${pid}")
    done
    if [[ -n "${active_cluster_dir}" && -f "${active_cluster_dir}/pids" ]]; then
        while IFS= read -r pid; do
            [[ "${pid}" =~ ^[0-9]+$ && -z "${seen_pids[${pid}]:-}" ]] || continue
            kill -0 "${pid}" 2>/dev/null || continue
            seen_pids["${pid}"]=1
            server_pids+=("${pid}")
        done <"${active_cluster_dir}/pids"
    fi
    for pid in "${gateway2_pid}" "${gateway3_pid}" "${haproxy_pid}"; do
        [[ "${pid}" =~ ^[0-9]+$ && -z "${seen_pids[${pid}]:-}" ]] || continue
        kill -0 "${pid}" 2>/dev/null || continue
        seen_pids["${pid}"]=1
        server_pids+=("${pid}")
    done

    printf 'pid\tcomm\targs\n' >"${metrics_dir}/process-pids.tsv"
    for pid in "${server_pids[@]:-}"; do
        ps -p "${pid}" -o pid=,comm=,args= 2>/dev/null |
            awk '{$1=$1; pid=$1; comm=$2; $1=""; $2=""; sub(/^  */, "", $0); printf "%s\t%s\t%s\n", pid, comm, $0}' \
            >>"${metrics_dir}/process-pids.tsv" || true
        if [[ -z "${pid_csv}" ]]; then pid_csv="${pid}"; else pid_csv="${pid_csv},${pid}"; fi
    done

    if command -v iostat >/dev/null 2>&1; then
        iostat -x -t -y 1 >"${metrics_dir}/iostat.log" 2>&1 &
        case_monitor_pids+=("$!")
    fi
    if command -v vmstat >/dev/null 2>&1; then
        vmstat -t 1 >"${metrics_dir}/vmstat.log" 2>&1 &
        case_monitor_pids+=("$!")
    fi
    if command -v sar >/dev/null 2>&1; then
        sar -n DEV 1 >"${metrics_dir}/sar-network.log" 2>&1 &
        case_monitor_pids+=("$!")
    fi
    if [[ -n "${pid_csv}" ]] && command -v pidstat >/dev/null 2>&1; then
        pidstat -dru -h -p "${pid_csv}" 1 >"${metrics_dir}/pidstat.log" 2>&1 &
        case_monitor_pids+=("$!")
    fi
}

cleanup_topology() {
    stop_case_monitors
    stop_pid "${haproxy_pid}"; wait_pid "${haproxy_pid}"; haproxy_pid=""
    stop_pid "${gateway2_pid}"; wait_pid "${gateway2_pid}"; gateway2_pid=""
    stop_pid "${gateway3_pid}"; wait_pid "${gateway3_pid}"; gateway3_pid=""
    if [[ "${cluster_started}" == 1 ]]; then
        MINIKV_V2_BENCH_CLUSTER_DIR="${active_cluster_dir}" MINIKV_V2_BIN_DIR="${bin_dir}" \
            bash "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh" >/dev/null 2>&1 || true
        cluster_started=0
    fi
    for pid in "${meta_pids[@]:-}"; do stop_pid "${pid}"; done
    for pid in "${meta_pids[@]:-}"; do wait_pid "${pid}"; done
    meta_pids=()
}
trap cleanup_topology EXIT

require_binary() { [[ -x "$1" ]] || { echo "missing executable: $1" >&2; exit 2; }; }
require_binary "${bin_dir}/minikv_v2_gateway"
require_binary "${bin_dir}/minikv_v2_datanode"
require_binary "${bin_dir}/minikv_v2_bench"
require_binary "${bin_dir}/minikv_metadata_raft_service"

wait_http() {
    local url="$1"; local attempts="${2:-100}"
    for _ in $(seq 1 "${attempts}"); do
        if curl --noproxy '*' --silent --fail --max-time 1 "${url}" >/dev/null 2>&1; then return 0; fi
        sleep 0.1
    done
    return 1
}

start_metadata() {
    active_meta_dir="$1"
    local base="$2"
    local members="1@127.0.0.1:$((base + 1)),2@127.0.0.1:$((base + 2)),3@127.0.0.1:$((base + 3))"
    local wal_root="${PHASE6_METADATA_WAL_DIR_ROOT:-}"
    local topology_name="$(basename "$(dirname "${active_meta_dir}")")"
    mkdir -p "${active_meta_dir}"
    for member in 1 2 3; do
        local wal_dir=""
        if [[ -n "${wal_root}" ]]; then
            wal_dir="${wal_root}/${topology_name}/meta-${member}"
            mkdir -p "${wal_dir}"
        fi
        MINIKV_METADATA_MEMBER_ID="${member}" \
        MINIKV_METADATA_RAFT_PORT="$((base + member))" \
        MINIKV_METADATA_RAFT_MEMBERS="${members}" \
        MINIKV_METADATA_PORT="$((base + 100 + member))" \
        MINIKV_METADATA_DIR="${active_meta_dir}/meta-${member}" \
        MINIKV_METADATA_WAL_DIR="${wal_dir}" \
        MINIKV_V2_CLUSTER_SECRET="${secret}" \
            "${bin_dir}/minikv_metadata_raft_service" >"${active_meta_dir}/meta-${member}.log" 2>&1 &
        meta_pids+=("$!")
    done
    # Do not silently continue with a two-member cluster.  The election probe
    # below can otherwise find a leader while the intended three-member
    # topology was never created (for example after a bind or storage error).
    sleep 0.2
    for pid in "${meta_pids[@]}"; do
        kill -0 "${pid}" 2>/dev/null || {
            echo "metadata member exited during startup; inspect ${active_meta_dir}" >&2
            return 1
        }
    done
    local leader=""
    for _ in $(seq 1 120); do
        for member in 1 2 3; do
            if curl --noproxy '*' --silent --max-time 1 "http://127.0.0.1:$((base + 100 + member))/readyz" |
                grep -q '"role":"leader"'; then leader="${member}"; break 2; fi
        done
        sleep 0.1
    done
    [[ -n "${leader}" ]] || { echo "metadata election failed; inspect ${active_meta_dir}" >&2; return 1; }
}

capture_metadata_status() {
    local root="$1"; local base="$2"; local label="$3"
    mkdir -p "${root}/metadata-status"
    for member in 1 2 3; do
        curl --noproxy '*' --silent --max-time 2 \
            "http://127.0.0.1:$((base + 100 + member))/internal/v4/metadata/status" \
            >"${root}/metadata-status/${label}-meta-${member}.json" 2>/dev/null || \
            printf '{"status":"unavailable"}\n' >"${root}/metadata-status/${label}-meta-${member}.json"
    done
}

start_cluster() {
    local topology="$1"; local root="$2"; local gateway_port="$3"; local meta_endpoints="${4:-}"
    if [[ -n "${PHASE6_CLUSTER_DATA_ROOT:-}" ]]; then
        active_cluster_dir="${PHASE6_CLUSTER_DATA_ROOT}/${topology}/cluster"
    else
        active_cluster_dir="${root}/cluster"
    fi
    mkdir -p "${root}"
    export MINIKV_V2_CLUSTER_SECRET="${secret}"
    export MINIKV_METADATA_MODE="${meta_endpoints:+raft}"
    export MINIKV_METADATA_ENDPOINTS="${meta_endpoints}"
    # In Raft topologies DataNodes should exercise the same direct metadata
    # control path as the v4-ha Compose deployment.  Keep the variable empty
    # for legacy so the compatibility GatewayControlClient remains the
    # baseline.  Object bytes never use this control-plane endpoint.
    export MINIKV_DATANODE_METADATA_ENDPOINTS="${meta_endpoints}"
    export MINIKV_V2_BIN_DIR="${bin_dir}"
    export MINIKV_V2_BENCH_CLUSTER_DIR="${active_cluster_dir}"
    export MINIKV_V2_BENCH_GATEWAY_PORT="${gateway_port}"
    export MINIKV_V2_BENCH_NODE_A_PORT="$((gateway_port + 1000))"
    export MINIKV_V2_BENCH_NODE_C_PORT="$((gateway_port + 1001))"
    export MINIKV_V2_BENCH_NODE_B_PORT="$((gateway_port + 1002))"
    export MINIKV_V2_BENCH_NODE_D_PORT="$((gateway_port + 1003))"
    export MINIKV_V2_BENCH_NODE_COUNT=3
    export MINIKV_V2_BENCH_ADVERTISE_ADDRESS=127.0.0.1
    export MINIKV_V3_REPLICATION_FACTOR="${replication_factor}"
    export MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id
    export MINIKV_V3_CHECKSUM_TYPE=crc32c
    export MINIKV_V3_DURABILITY_MODE=group_commit
    bash "${repo_dir}/tools/start_v2_local_benchmark_cluster.sh" >"${root}/cluster-start.log" 2>&1
    if [[ ! -f "${active_cluster_dir}/pids" ]]; then
        echo "benchmark cluster did not leave a pid file; inspect ${root}/cluster-start.log" >&2
        return 1
    fi
    while IFS= read -r pid; do
        [[ -z "${pid}" ]] && continue
        if ! kill -0 "${pid}" 2>/dev/null; then
            echo "benchmark cluster process ${pid} exited during startup; inspect ${active_cluster_dir}/logs" >&2
            return 1
        fi
    done <"${active_cluster_dir}/pids"
    cluster_started=1
}

start_extra_gateways() {
    local root="$1"; local base_port="$2"; local meta_endpoints="$3"
    for slot in 2 3; do
        local port="$((base_port + slot - 1))"
        local pid_var="gateway${slot}_pid"
        MINIKV_V2_CLUSTER_SECRET="${secret}" MINIKV_METADATA_MODE=raft \
        MINIKV_METADATA_ENDPOINTS="${meta_endpoints}" MINIKV_GATEWAY_ID="gw-${slot}" \
        MINIKV_V3_REPLICATION_FACTOR="${replication_factor}" MINIKV_V3_IDENTITY_SCHEME=opaque-chunk-id \
        MINIKV_V3_CHECKSUM_TYPE=crc32c MINIKV_V3_DURABILITY_MODE=group_commit \
        "${bin_dir}/minikv_v2_gateway" "${port}" "${root}/gateway-${slot}" "${secret}" \
            >"${root}/gateway-${slot}.out" 2>&1 &
        if [[ "${slot}" == 2 ]]; then gateway2_pid="$!"; else gateway3_pid="$!"; fi
        if ! wait_http "http://127.0.0.1:${port}/healthz" 100; then
            echo "gateway-${slot} did not become healthy; inspect ${root}/gateway-${slot}.out" >&2
            return 1
        fi
        if ! wait_http "http://127.0.0.1:${port}/readyz" 100; then
            echo "gateway-${slot} did not become metadata-ready; response follows:" >&2
            curl --noproxy '*' --silent --show-error --max-time 2 \
                "http://127.0.0.1:${port}/readyz" >&2 || true
            echo >&2
            echo "inspect ${root}/gateway-${slot}.out and ${root}/gateway-${slot}/logs" >&2
            return 1
        fi
    done
}

start_haproxy() {
    local root="$1"; local listen_port="$2"; local base_port="$3"
    [[ -n "${haproxy_bin}" ]] || return 2
    cat >"${root}/haproxy.cfg" <<EOF
global
    maxconn 4096
    daemon
defaults
    mode http
    timeout connect 1s
    timeout client 30s
    timeout server 30s
frontend minidriver_http
    bind 127.0.0.1:${listen_port}
    default_backend gateways
backend gateways
    balance roundrobin
    option httpchk GET /healthz
    http-check expect status 200
    server gw-1 127.0.0.1:${base_port} check inter 1s fall 2 rise 2
    server gw-2 127.0.0.1:$((base_port + 1)) check inter 1s fall 2 rise 2
    server gw-3 127.0.0.1:$((base_port + 2)) check inter 1s fall 2 rise 2
EOF
    "${haproxy_bin}" -db -f "${root}/haproxy.cfg" >"${root}/haproxy.log" 2>&1 & haproxy_pid="$!"
    if ! wait_http "http://127.0.0.1:${listen_port}/healthz" 100; then
        echo "HAProxy did not become healthy; inspect ${root}/haproxy.log" >&2
        return 1
    fi
}

restart_case_services() {
    local topology="$1"; local root="$2"; local gateway_port="$3"; local meta_endpoints="${4:-}"
    local cluster_topology="${topology}"
    # The public matrix label is `ha`, while the isolated benchmark cluster
    # uses the more explicit `raft-ha` storage namespace.  Keep the reset
    # path check aligned with start_cluster() so HA cases can be restarted
    # without refusing their own derived directory.
    if [[ "${topology}" == "ha" ]]; then
        cluster_topology="raft-ha"
    fi
    # This mode is opt-in for long matrices.  It bounds DataNode object bytes
    # by one case while keeping the metadata service and its Raft state alive.
    # Only the runner-owned, explicitly derived cluster directory is removed.
    stop_pid "${haproxy_pid}"; wait_pid "${haproxy_pid}"; haproxy_pid=""
    stop_pid "${gateway2_pid}"; wait_pid "${gateway2_pid}"; gateway2_pid=""
    stop_pid "${gateway3_pid}"; wait_pid "${gateway3_pid}"; gateway3_pid=""
    if [[ "${cluster_started}" == 1 ]]; then
        MINIKV_V2_BENCH_CLUSTER_DIR="${active_cluster_dir}" MINIKV_V2_BIN_DIR="${bin_dir}" \
            bash "${repo_dir}/tools/stop_v2_local_benchmark_cluster.sh" >/dev/null 2>&1 || true
        cluster_started=0
    fi
    local expected_cluster="${root}/cluster"
    if [[ -n "${PHASE6_CLUSTER_DATA_ROOT:-}" ]]; then
        expected_cluster="${PHASE6_CLUSTER_DATA_ROOT}/${cluster_topology}/cluster"
    fi
    [[ "${active_cluster_dir}" == "${expected_cluster}" ]] || {
        echo "refusing to reset unexpected benchmark cluster path: ${active_cluster_dir}" >&2
        return 1
    }
    rm -rf -- "${active_cluster_dir}"
    start_cluster "${cluster_topology}" "${root}" "${gateway_port}" "${meta_endpoints}"
    # Metadata may still contain the previous incarnation's ONLINE record.
    # Give the freshly booted DataNodes one heartbeat interval to publish the
    # new bootId/nodeEpoch before preparing the next fixture.
    sleep "${PHASE6_RESET_CLUSTER_SETTLE_SECONDS:-3}"
    if [[ "${topology}" == "raft-ha" || "${topology}" == "ha" ]]; then
        start_extra_gateways "${root}" "${gateway_port}" "${meta_endpoints}"
        start_haproxy "${root}" "${ha_proxy_port}" "${gateway_port}"
    fi
}

run_case() {
    local topology="$1"; local repeat="$2"; local size="$3"; local concurrency="$4"; local gateway="$5"; local root="$6"
    local work="${root}/${topology}/repeat-${repeat}/${size}-c${concurrency}"
    local endpoint="127.0.0.1:${gateway}"
    local common=(--gateway "${endpoint}" --sizes "${size}" --runs 1
        --concurrency "${concurrency}" --chunk-window "${chunk_window}" --global-chunk-budget "${global_chunk_budget}"
        --upload-checksum crc32c --connection-mode keep-alive --download-verification strict
        --sdk-read-plan true --cluster-internal-token "${secret}" --service-principal "phase6-${topology}")
    local fixture_dir="${work}/fixtures"
    local upload_dir="${work}/upload"
    local download_dir="${work}/download"
    local run_upload=0 run_download=0
    [[ ",${operations}," == *,upload,* ]] && run_upload=1
    [[ ",${operations}," == *,download,* ]] && run_download=1
    mkdir -p "${fixture_dir}" "${upload_dir}" "${download_dir}"

    start_case_monitors "${work}"
    if [[ "${PHASE6_CAPTURE_CASE_METADATA_STATUS:-${PHASE6_CAPTURE_HOST_METRICS:-0}}" == 1 ]]; then
        if [[ "${topology}" == "raft" ]]; then
            capture_metadata_status "${work}" "${raft_meta_base}" "before"
        elif [[ "${topology}" == "ha" ]]; then
            capture_metadata_status "${work}" "${ha_meta_base}" "before"
        fi
    fi

    # The duration runner intentionally rejects end-to-end mode.  Prepare a
    # stable set of committed download fixtures only when download is enabled,
    # then measure upload and strict download as independent workloads.  The
    # fixture manifest contains operation=download rows, which is the contract
    # used by --download-fixture-manifest.  Upload is independent of fixture
    # preparation: a fixture failure must not hide an upload result.
    set +e
    local fixture_code=0 upload_code=0 download_code=0
    if [[ "${run_download}" == 1 ]]; then
        "${bin_dir}/minikv_v2_bench" local \
            "${common[@]}" --work-dir "${fixture_dir}" --mode download \
            --requests-per-worker 1 --remote-dir "/phase6/${topology}/r${repeat}/fixtures" \
            >"${work}/fixture.log" 2>&1
        fixture_code=$?
    fi
    if [[ "${run_upload}" == 1 ]]; then
        "${bin_dir}/minikv_v2_bench" local \
            "${common[@]}" --work-dir "${upload_dir}" --mode upload \
            --duration-seconds "${steady}" --warmup-seconds "${warmup}" \
            --remote-dir "/phase6/${topology}/r${repeat}/upload" \
            >"${upload_dir}/runner.log" 2>&1
        upload_code=$?
    fi
    if [[ "${run_download}" == 1 && "${fixture_code}" == 0 ]]; then
        "${bin_dir}/minikv_v2_bench" local \
            "${common[@]}" --work-dir "${download_dir}" --mode download \
            --duration-seconds "${steady}" --warmup-seconds "${warmup}" \
            --download-fixture-manifest "${fixture_dir}/runs.csv" \
            --download-fixtures "${concurrency}" \
            --remote-dir "/phase6/${topology}/r${repeat}/download" \
            >"${download_dir}/runner.log" 2>&1
        download_code=$?
    elif [[ "${run_download}" == 1 ]]; then
        download_code=2
    fi
    set -e

    if [[ "${PHASE6_CAPTURE_CASE_METADATA_STATUS:-${PHASE6_CAPTURE_HOST_METRICS:-0}}" == 1 ]]; then
        if [[ "${topology}" == "raft" ]]; then
            capture_metadata_status "${work}" "${raft_meta_base}" "after"
        elif [[ "${topology}" == "ha" ]]; then
            capture_metadata_status "${work}" "${ha_meta_base}" "after"
        fi
    fi
    stop_case_monitors

    local fixture_status="skipped" upload_status="skipped" download_status="skipped" case_status="passed"
    if [[ "${run_download}" == 1 ]]; then
        fixture_status="passed"
        [[ "${fixture_code}" == 0 ]] || fixture_status="failed"
        if [[ "${fixture_code}" != 0 ]]; then case_status="failed"; fi
    fi
    if [[ "${run_upload}" == 1 ]]; then
        upload_status="passed"
        [[ "${upload_code}" == 0 ]] || { upload_status="failed"; case_status="failed"; }
    fi
    if [[ "${run_download}" == 1 ]]; then
        if [[ "${fixture_code}" != 0 ]]; then
            download_status="skipped"
        else
            download_status="passed"
            [[ "${download_code}" == 0 ]] || { download_status="failed"; case_status="failed"; }
        fi
    fi
    printf '%s\t%s\t%s\t%s\tupload\t%s\t%s\t%s\t%s\n' "${topology}" "${repeat}" "${size}" "${concurrency}" \
        "${warmup}" "${steady}" "${upload_status}" "${upload_dir}" >>"${manifest}"
    printf '%s\t%s\t%s\t%s\tdownload\t%s\t%s\t%s\t%s\n' "${topology}" "${repeat}" "${size}" "${concurrency}" \
        "${warmup}" "${steady}" "${download_status}" "${download_dir}" >>"${manifest}"

    # A 300-second duration run can produce hundreds of megabytes of
    # per-request CSV at c8/c16.  The release evaluator only consumes the
    # summaries, per-second samples, stage summary and manifest; retaining
    # every row is opt-in so a long matrix cannot fail because its evidence
    # directory exhausts the workspace.  The fixture manifest is no longer
    # needed after the download phase has completed.
    if [[ "${PHASE6_KEEP_RAW_RUNS:-1}" != 1 ]]; then
        rm -f -- "${fixture_dir}/runs.csv" "${upload_dir}/runs.csv" "${download_dir}/runs.csv"
    fi
    [[ "${case_status}" == passed ]]
}

run_topology() {
    local topology="$1"; local root="${output_root}/${topology}"; local gateway=""
    local base_gateway=""; local meta_endpoints=""
    mkdir -p "${root}"
    if [[ "${topology}" == "legacy" ]]; then
        gateway="${legacy_gateway_port}"
        base_gateway="${gateway}"
        start_cluster "legacy" "${root}" "${gateway}" ""
    elif [[ "${topology}" == "raft" ]]; then
        start_metadata "${root}/metadata" "${raft_meta_base}"
        capture_metadata_status "${root}" "${raft_meta_base}" "before"
        gateway="${raft_gateway_port}"
        base_gateway="${gateway}"
        meta_endpoints="127.0.0.1:$((raft_meta_base + 101)),127.0.0.1:$((raft_meta_base + 102)),127.0.0.1:$((raft_meta_base + 103))"
        start_cluster "raft" "${root}" "${gateway}" "${meta_endpoints}"
    else
        [[ -n "${haproxy_bin}" ]] || { echo "SKIP ${topology}: haproxy binary not found" | tee "${root}/SKIPPED"; return 0; }
        start_metadata "${root}/metadata" "${ha_meta_base}"
        capture_metadata_status "${root}" "${ha_meta_base}" "before"
        gateway="${ha_gateway_port}"
        base_gateway="${gateway}"
        meta_endpoints="127.0.0.1:$((ha_meta_base + 101)),127.0.0.1:$((ha_meta_base + 102)),127.0.0.1:$((ha_meta_base + 103))"
        start_cluster "raft-ha" "${root}" "${gateway}" "${meta_endpoints}"
        start_extra_gateways "${root}" "${gateway}" "${meta_endpoints}"
        start_haproxy "${root}" "${ha_proxy_port}" "${gateway}"
        if [[ "${PHASE6_PAUSE_AFTER_START_SECONDS:-0}" =~ ^[1-9][0-9]*$ ]]; then
            echo "phase6_debug_pause_after_start=${PHASE6_PAUSE_AFTER_START_SECONDS}s" >&2
            sleep "${PHASE6_PAUSE_AFTER_START_SECONDS}"
        fi
        gateway="${ha_proxy_port}"
    fi
    phase6_case_number=0
    for repeat in $(seq 1 "${repeats}"); do
        IFS=',' read -ra size_list <<<"${sizes}"
        IFS=',' read -ra concurrency_list <<<"${concurrencies}"
        for size in "${size_list[@]}"; do
            for concurrency in "${concurrency_list[@]}"; do
                if [[ "${PHASE6_RESET_DATA_CLUSTER_PER_CASE:-0}" == 1 && "${phase6_case_number}" -gt 0 ]]; then
                    restart_case_services "${topology}" "${root}" "${base_gateway}" "${meta_endpoints}"
                    if [[ "${topology}" == "raft-ha" ]]; then gateway="${ha_proxy_port}"; else gateway="${base_gateway}"; fi
                fi
                phase6_case_number=$((phase6_case_number + 1))
                case_key="${topology}|${repeat}|${size}|${concurrency}"
                if [[ "${phase6_done_cases["${case_key}"]:-0}" -ge 2 ]]; then
                    echo "phase6_resume_skip=${case_key}" >&2
                    continue
                fi
                if ! run_case "${topology}" "${repeat}" "${size}" "${concurrency}" "${gateway}" "${output_root}"; then
                    matrix_failed=1
                fi
            done
        done
    done
    if [[ "${topology}" == "raft" ]]; then
        capture_metadata_status "${root}" "${raft_meta_base}" "after"
    elif [[ "${topology}" == "ha" ]]; then
        capture_metadata_status "${root}" "${ha_meta_base}" "after"
    fi
    cleanup_topology
}

echo "phase6_output=${output_root}"
echo "phase6_matrix warmup=${warmup}s steady=${steady}s repeats=${repeats} sizes=${sizes} concurrencies=${concurrencies} topologies=${topologies}"
echo "phase6_ports legacy_gw=${legacy_gateway_port} raft_gw=${raft_gateway_port} ha_gw=${ha_gateway_port} ha_proxy=${ha_proxy_port}"
echo "phase6_metadata_wal_dir_root=${PHASE6_METADATA_WAL_DIR_ROOT:-<default-metadata-dir>}"
echo "phase6_capture_host_metrics=${PHASE6_CAPTURE_HOST_METRICS:-0}"
echo "phase6_capture_case_metadata_status=${PHASE6_CAPTURE_CASE_METADATA_STATUS:-${PHASE6_CAPTURE_HOST_METRICS:-0}}"
echo "phase6_operations=${operations}"
echo "phase6_chunk_window=${chunk_window} global_chunk_budget=${global_chunk_budget}"
echo "phase6_replication_factor=${replication_factor}"
echo "phase6_storage_report=${storage_report}"
IFS=',' read -ra topology_list <<<"${topologies}"
for topology in "${topology_list[@]}"; do run_topology "${topology}"; done
echo "phase6_manifest=${manifest}"
exit "${matrix_failed}"
