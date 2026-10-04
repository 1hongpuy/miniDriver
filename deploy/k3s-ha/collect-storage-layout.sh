#!/usr/bin/env bash
set -euo pipefail

# Collect only guest-visible storage facts. Mapping virtual disks to Windows
# host volumes remains a manual evidence step and is recorded in the report
# template generated below.
report_root="${MINIDRIVER_STORAGE_REPORT_ROOT:-/var/lib/minidriver-bench/storage-diagnostics}"
ssh_user="${MINIDRIVER_STORAGE_SSH_USER:-$USER}"
read -r -a nodes <<< "${MINIDRIVER_STORAGE_NODES:-192.168.137.10 192.168.137.148 192.168.137.151}"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
report_dir="${report_root}/layout-${stamp}"

sudo mkdir -p "${report_dir}"
sudo chown "$(id -u):$(id -g)" "${report_dir}"

for node in "${nodes[@]}"; do
  safe_node="${node//[^A-Za-z0-9._-]/_}"
  output="${report_dir}/${safe_node}.txt"
  echo "Collecting guest storage layout from ${node}"
  ssh -o BatchMode=no -o ConnectTimeout=10 "${ssh_user}@${node}" 'bash -s' >"${output}" <<'REMOTE'
set -u
echo "# collected_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "## identity"
hostname
hostname -I || true
ip -br addr || true
echo "## required paths"
for path in \
  /var/lib/minidriver-data \
  /data/minidriver-data \
  /var/lib/minidriver-index \
  /var/lib/minidriver-metadata \
  /; do
  printf '\n### %s\n' "$path"
  if test -e "$path"; then
    findmnt -T "$path" || true
    df -hT "$path" || true
  else
    echo "missing"
  fi
done
echo "## block devices"
lsblk -o NAME,MAJ:MIN,TYPE,SIZE,FSTYPE,MOUNTPOINTS,MODEL,ROTA || true
echo "## root mount options"
findmnt -no SOURCE,TARGET,FSTYPE,OPTIONS / || true
REMOTE
done

cat >"${report_dir}/WINDOWS_HOST_MAPPING.md" <<'TEMPLATE'
# Windows host storage mapping (manual evidence)

Fill this from the hypervisor UI before interpreting a guest-visible “split disk” as physical isolation.

| VM / node | Linux path | Guest block device | Virtual disk file | Windows volume | Physical disk | VHD/VMDK type | Checkpoint / differencing chain | Notes |
|---|---|---|---|---|---|---|---|---|
| minidriver-1 | data | | | | | | | |
| minidriver-1 | index | | | | | | | |
| minidriver-1 | metadata WAL | | | | | | | |
| minidriver-2 | data | | | | | | | |
| minidriver-2 | index | | | | | | | |
| minidriver-2 | metadata WAL | | | | | | | |
| minidriver-3 | data | | | | | | | |
| minidriver-3 | index | | | | | | | |
| minidriver-3 | metadata WAL | | | | | | | |

During each probe window also archive Windows physical disk latency, queue length, IOPS, throughput, CPU, available memory, paging, and any competing host workload.
TEMPLATE

echo "storage layout report: ${report_dir}"
