#!/usr/bin/env bash
set -euo pipefail

server=${1:?usage: test_search_node_http.sh <minikv_search_node>}
root=$(mktemp -d /tmp/minikv-search-node-http.XXXXXX)
port=$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)
pid=""
cleanup() {
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
    rm -rf "$root"
}
trap cleanup EXIT

"$server" "$port" "$root/index" 2 >"$root/server.log" 2>&1 &
pid=$!
base="http://127.0.0.1:${port}"
for _ in $(seq 1 80); do
    if curl -fsS "$base/healthz" >/dev/null 2>&1; then break; fi
    sleep 0.05
done
curl -fsS "$base/healthz" | grep -q '"status":"ok"'

document='{"doc_id":"asset-1","object_id":"A123","object_version":3,"tenant_id":"tenant-a","media_type":"image","metadata":{"camera":"D610"},"thumbnail_ref":{"object_id":"T123","object_version":1},"embedding":{"model_id":"test-v1","dimension":2,"vector":[1.0,0.0]},"processor_version":"image-index-v1","index_generation":1,"state":"ready"}'
curl -fsS -X POST "$base/v1/index/documents" -H 'Content-Type: application/json' \
    --data "$document" | grep -q '"result":"applied"'
curl -fsS -X POST "$base/v1/index/documents" -H 'Content-Type: application/json' \
    --data "$document" | grep -q '"result":"idempotent"'

without_thumbnail='{"doc_id":"asset-2","object_id":"B456","object_version":1,"tenant_id":"tenant-a","media_type":"image","thumbnail_ref":null,"embedding":null,"processor_version":"image-index-v1","state":"ready"}'
curl -fsS -X POST "$base/v1/index/documents" -H 'Content-Type: application/json' \
    --data "$without_thumbnail" | grep -q '"result":"applied"'

stale='{"doc_id":"asset-1-old","object_id":"A123","object_version":2,"tenant_id":"tenant-a","media_type":"image","embedding":{"model_id":"test-v1","vector":[1.0,0.0]},"processor_version":"image-index-v1","state":"ready"}'
status=$(curl -sS -o "$root/stale.json" -w '%{http_code}' -X POST "$base/v1/index/documents" \
    -H 'Content-Type: application/json' --data "$stale")
[[ "$status" == 409 ]]

curl -fsS -X POST "$base/v1/index/flush" -H 'Content-Type: application/json' \
    --data '{"generation":7}' | grep -q '"generation":7'
curl -fsS "$base/healthz" | grep -q '"documents":2'
curl -fsS -X POST "$base/v1/index/query" -H 'Content-Type: application/json' \
    --data '{"embedding":[1.0,0.0],"limit":3}' | grep -q '"doc_id":"asset-1"'

kill -TERM "$pid"
wait "$pid" 2>/dev/null || true
pid=""

"$server" "$port" "$root/index" 2 >"$root/restart.log" 2>&1 &
pid=$!
for _ in $(seq 1 80); do
    if curl -fsS "$base/healthz" >/dev/null 2>&1; then break; fi
    sleep 0.05
done
curl -fsS "$base/healthz" | grep -q '"documents":2'
curl -fsS -X POST "$base/v1/index/reload" | grep -q '"status":"reloaded"'

echo "search node http tests passed"
