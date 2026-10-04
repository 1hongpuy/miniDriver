#!/usr/bin/env python3
"""Capture Metadata status snapshots and calculate count/total deltas."""

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path


ENDPOINTS = ("192.168.137.10:18101", "192.168.137.148:18101", "192.168.137.151:18101")
DELTA_FIELDS = (
    "proposalCount", "proposalFailureCount", "proposalTotalUs", "raftAppendTotalUs", "raftAppendCount",
    "raftLogSyncCount", "raftLogSyncFailureCount", "raftLogSyncTotalUs", "raftLogWritevCount",
    "raftLogWritevRecords", "raftLogWritevBytes", "raftLogWritevTotalUs", "quorumWaitCount",
    "quorumWaitTotalUs", "stateMachineDecodeTotalUs", "placementCount", "placementTotalUs",
)


def capture(endpoint: str) -> dict:
    with urllib.request.urlopen(f"http://{endpoint}/internal/v4/metadata/status", timeout=5) as response:
        return json.load(response)


def snapshot(path: Path, endpoints: list[str]) -> None:
    data = {"captured_unix_ms": int(time.time() * 1000), "members": {}}
    for endpoint in endpoints:
        try:
            value = capture(endpoint)
            data["members"][value.get("memberId", endpoint)] = {"endpoint": endpoint, "status": value}
        except Exception as error:  # retain failed endpoint evidence
            data["members"][endpoint] = {"endpoint": endpoint, "error": str(error)}
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def delta(before: Path, after: Path) -> dict:
    left = json.loads(before.read_text(encoding="utf-8"))
    right = json.loads(after.read_text(encoding="utf-8"))
    output = {"before": str(before), "after": str(after), "members": {}}
    for member, after_member in right.get("members", {}).items():
        before_status = left.get("members", {}).get(member, {}).get("status")
        after_status = after_member.get("status")
        if not before_status or not after_status:
            continue
        values = {field: int(after_status.get(field, 0)) - int(before_status.get(field, 0)) for field in DELTA_FIELDS}
        output["members"][member] = {"endpoint": after_member.get("endpoint"), "delta": values}
    return output


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--delta", nargs=2, type=Path, metavar=("BEFORE", "AFTER"))
    parser.add_argument("--endpoints", default=",".join(ENDPOINTS))
    args = parser.parse_args()
    if bool(args.output) == bool(args.delta):
        parser.error("choose exactly one of --output or --delta")
    if args.output:
        snapshot(args.output, [item for item in args.endpoints.split(",") if item])
        print(args.output)
    else:
        print(json.dumps(delta(*args.delta), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
