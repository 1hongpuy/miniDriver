#!/usr/bin/env bash
set -euo pipefail

archive="${1:-${MINIDRIVER_K3S_IMAGE_ARCHIVE:-/tmp/minidriver-4.0-ha-k3s.tar}}"
remote_user="${MINIDRIVER_K3S_SSH_USER:-hpy}"
remote_nodes=(192.168.137.148 192.168.137.151)
remote_archive="/tmp/$(basename "${archive}")"

if [[ ! -r "${archive}" ]]; then
  echo "image archive is not readable: ${archive}" >&2
  exit 1
fi

echo "Importing image on minidriver-1"
sudo k3s ctr -n k8s.io images import "${archive}"

for node in "${remote_nodes[@]}"; do
  echo "Copying image to ${node}"
  scp "${archive}" "${remote_user}@${node}:${remote_archive}"
  echo "Importing image on ${node}"
  ssh -t "${remote_user}@${node}" \
    "sudo k3s ctr -n k8s.io images import '${remote_archive}'"
done

echo "Local image inventory:"
sudo k3s crictl images | grep -F minidriver || true
for node in "${remote_nodes[@]}"; do
  echo "${node} image inventory:"
  ssh -t "${remote_user}@${node}" "sudo k3s crictl images | grep -F minidriver"
done

