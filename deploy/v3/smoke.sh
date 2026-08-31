#!/usr/bin/env bash
set -euo pipefail

gateway="${MINIKV_V3_GATEWAY_URL:-http://127.0.0.1:18280}"
for endpoint in "${gateway}/healthz" "${gateway}/readyz"; do
    curl --noproxy '*' --fail --silent --show-error "${endpoint}" >/dev/null
done
nodes="$(curl --noproxy '*' --fail --silent --show-error "${gateway}/api/v2/admin/nodes")"
for node in dn-1 dn-2 dn-3; do
    if [[ "${nodes}" != *"\"nodeId\":\"${node}\""* ]]; then
        echo "missing registered node: ${node}" >&2
        exit 1
    fi
done
echo "MiniDriver 3.0 Compose control-plane smoke passed"
