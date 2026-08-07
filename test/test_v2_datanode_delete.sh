#!/usr/bin/env bash
set -euo pipefail

binary="$1"
port="${MINIKV_TEST_DATANODE_DELETE_PORT:-19027}"
io_threads="${MINIKV_TEST_IO_THREADS:-2}"
secret="test-delete-secret"
root="$(mktemp -d)"
pid=""

cleanup() {
    if [[ -n "$pid" ]]; then
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    rm -rf "$root"
}
trap cleanup EXIT

MINIKV_V4_IO_THREADS="$io_threads" MINIKV_V2_CLUSTER_SECRET="$secret" \
    "$binary" node-delete 127.0.0.1 "$port" \
    "$root/data" 127.0.0.1 6553 >"$root/datanode.out" 2>"$root/datanode.err" &
pid=$!

for _ in $(seq 1 50); do
    if curl --noproxy '*' -sS --max-time 1 -o /dev/null \
        "http://127.0.0.1:${port}/v2/chunks/missing"; then
        break
    fi
    sleep 0.1
done

log="$root/data/logs/datanode-node-delete.log"
for _ in $(seq 1 50); do
    if [[ -f "$log" ]] && grep -q "event=datanode_started.*io_threads=${io_threads}" "$log"; then
        break
    fi
    sleep 0.1
done
grep -q "event=datanode_started.*io_threads=${io_threads}" "$log"

status="$(curl --noproxy '*' -sS --max-time 3 -o /dev/null -w '%{http_code}' \
    -X DELETE "http://127.0.0.1:${port}/internal/v2/chunks/missing")"
[[ "$status" == "403" ]]

for _ in 1 2; do
    status="$(curl --noproxy '*' -sS --max-time 3 -o /dev/null -w '%{http_code}' \
        -X DELETE -H "X-Cluster-Internal-Token: ${secret}" \
        "http://127.0.0.1:${port}/internal/v2/chunks/missing")"
    [[ "$status" == "200" ]]
done

echo "PASS: DataNode internal delete rejects unauthenticated requests and is idempotent"
