#!/usr/bin/env bash
set -euo pipefail

kubectl_cmd=(sudo k3s kubectl)
metadata_nodes=(192.168.137.10 192.168.137.148 192.168.137.151)
datanode_nodes=(192.168.137.10 192.168.137.148 192.168.137.151)

"${kubectl_cmd[@]}" get nodes -o wide
"${kubectl_cmd[@]}" -n minidriver get pods -o wide

echo "Metadata readiness:"
for node in "${metadata_nodes[@]}"; do
  printf '%s ' "${node}"
  curl --fail --silent --show-error --max-time 3 \
    "http://${node}:18101/readyz"
  echo
done

echo "DataNode readiness:"
for node in "${datanode_nodes[@]}"; do
  printf '%s ' "${node}"
  curl --fail --silent --show-error --max-time 3 \
    "http://${node}:19201/readyz"
  echo
done

echo "Gateway readiness through NodePort:"
curl --fail --silent --show-error --max-time 3 \
  http://192.168.137.10:30280/storage-readyz
echo

echo "Registered DataNodes from each Metadata endpoint:"
response_file="$(mktemp)"
trap 'rm -f "${response_file}"' EXIT
metadata_read_successes=0
metadata_read_unexpected=0
for node in "${metadata_nodes[@]}"; do
  if http_status="$(curl --silent --show-error --max-time 3 \
      --output "${response_file}" --write-out '%{http_code}' \
      "http://${node}:18101/internal/v4/metadata/nodes")"; then
    printf '%s status=%s ' "${node}" "${http_status}"
    cat "${response_file}"
    if [[ "${http_status}" == "200" ]]; then
      metadata_read_successes=$((metadata_read_successes + 1))
    elif [[ "${http_status}" == "503" ]]; then
      printf ' (expected from a follower or during leader transition)'
    else
      metadata_read_unexpected=$((metadata_read_unexpected + 1))
    fi
    echo
  else
    curl_status=$?
    printf '%s transport_error=%s ' "${node}" "${curl_status}"
    cat "${response_file}"
    echo
    metadata_read_unexpected=$((metadata_read_unexpected + 1))
  fi
done

if (( metadata_read_successes == 0 )); then
  echo "no Metadata endpoint served the authoritative node read" >&2
  exit 1
fi
if (( metadata_read_unexpected != 0 )); then
  echo "one or more Metadata endpoints returned an unexpected result" >&2
  exit 1
fi

echo "Metadata authoritative read succeeded on ${metadata_read_successes} endpoint(s)."
